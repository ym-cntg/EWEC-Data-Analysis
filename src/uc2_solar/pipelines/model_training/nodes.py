"""Model training nodes: one model per clock slot (forecast time × step) × plant, logged to MLflow.

- Label: target ÷ capacity (both plants: actual MW).
- Daytime only: models exist for slots in `model_training.daytime` (fixed Gulf hours or sun up); forecast = 0 outside.
- Test windows (`model_training.splits`): each split trains its own models on origins BEFORE its test window
  (e.g. Phase 1's Sep 1 – Nov 19 2025, and the last 3 months).
- Feature groups can be switched off with `exclude_feature_prefixes` (for blending / ablation runs).
- Optional recency weighting: sample weight = 0.5 ** (age_days / recency_half_life_days).
"""
import pickle
import tempfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ...metrics import score

KEY_COLS = ["site", "origin", "period_end", "origin_hour_gulf", "step", "lead_min"]
LABEL_COLS = ["label_target_mw", "label_actual_mw", "label_target_norm", "label_ok"]
NON_FEATURES = set(KEY_COLS[:3] + LABEL_COLS + ["split", "capacity_mw", "is_day", "product"])


def build_model_input(feat_table: DataFrame, mt: dict) -> DataFrame:
    """Rows with a usable label (splits are applied in training)."""
    return feat_table.where("label_ok")


def feature_columns(df: DataFrame, exclude_prefixes=()):
    """All numeric columns except keys, labels, run timestamps and excluded feature groups."""
    numeric = {"double", "float", "integer", "long", "short", "byte", "decimal"}
    ex = tuple(exclude_prefixes or ())
    return [f.name for f in df.schema.fields
            if f.name not in NON_FEATURES and not f.name.endswith("_run_time") and f.dataType.typeName() in numeric
            and not (ex and f.name.startswith(ex))]


def _make_model(model_cfg: dict):
    if model_cfg["type"] == "lightgbm":
        try:
            import lightgbm as lgb
            return lgb.LGBMRegressor(**model_cfg["params"])
        except ImportError:
            print("    LightGBM not installed -> using sklearn HistGradientBoostingRegressor")
    from sklearn.ensemble import HistGradientBoostingRegressor
    return HistGradientBoostingRegressor(max_iter=model_cfg["params"].get("n_estimators", 300),
                                         learning_rate=model_cfg["params"].get("learning_rate", 0.05))


def in_daytime_window(pdf: pd.DataFrame, daytime: dict) -> pd.Series:
    """True for target slots that get a model. clock: period end in (start, end] Gulf hours; sun: sun up."""
    if daytime["mode"] == "sun":
        return pdf["is_day"].astype(bool)
    g = pd.to_datetime(pdf["period_end"]) + pd.Timedelta(hours=4)
    hour = g.dt.hour + g.dt.minute / 60
    hour = hour.where(hour > 0, 24.0)  # a period ending at midnight belongs to the previous day
    return (hour > daytime["start_hour_gulf"]) & (hour <= daytime["end_hour_gulf"])


def split_masks(pdf: pd.DataFrame, split_cfg: dict):
    """(train_mask, test_mask, test_start, test_end) for one split; training = origins before the test window."""
    od = pd.to_datetime(pdf["origin"]).dt.date
    if "test_start" in split_cfg:
        start, end = date.fromisoformat(split_cfg["test_start"]), date.fromisoformat(split_cfg["test_end"])
    else:
        end = od.max()
        start = (pd.Timestamp(end) - pd.DateOffset(months=int(split_cfg["holdout_months"]))).date() + timedelta(days=1)
    test = (od >= start) & (od <= end)
    train = od < start
    return train, test, start, end


def _experiment_path(spark, name: str) -> str:
    user = spark.sql("SELECT current_user()").first()[0]
    return f"/Users/{user}/{name}"


def train_models(spark, model_input: DataFrame, mt: dict, run: dict, product_cfg: dict, ev: dict) -> DataFrame:
    """For each split × site × candidate: train the slot models, log one MLflow run, return test predictions (+ Reuniwatt baseline)."""
    import mlflow

    feats = feature_columns(model_input, mt.get("exclude_feature_prefixes"))
    group_cols = (["origin_hour_gulf"] if mt["split_by_origin_hour"] else []) + ["step"]
    label, thr = mt["label"], ev["mape_min_actual_mw"]
    half_life = mt.get("recency_half_life_days")
    print(f"    {len(feats)} features (excluded prefixes: {mt.get('exclude_feature_prefixes') or 'none'}); groups by {group_cols}; "
          f"recency half-life: {half_life or 'off'}")
    # Databricks autologging would create one MLflow run per fitted model; we log one run per split × site × candidate.
    mlflow.autolog(disable=True)
    mlflow.set_experiment(_experiment_path(spark, mt["mlflow"]["experiment"]))
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    out = []
    for site in [r[0] for r in model_input.select("site").distinct().orderBy("site").collect()]:
        cols = list(dict.fromkeys(KEY_COLS + feats + LABEL_COLS + ["capacity_mw", "is_day", "f_power_mw"]))
        pdf = model_input.where(F.col("site") == site).select(*cols).toPandas()
        pdf[feats] = pdf[feats].astype("float32")
        pdf["in_window"] = in_daytime_window(pdf, mt["daytime"])

        for split, scfg in mt["splits"].items():
            tr_mask, te_mask, t0, t1 = split_masks(pdf, scfg)
            train_all, test_all = pdf[tr_mask & pdf["in_window"]], pdf[te_mask]
            if test_all.empty or train_all.empty:
                print(f"    {site} {split}: no train or test rows (test {t0} → {t1}); skipped")
                continue
            base = test_all[KEY_COLS + ["is_day", "in_window", "capacity_mw", "label_target_mw", "label_ok"]].copy().assign(split=split)
            out.append(base.assign(model="reuniwatt_baseline", pred_mw=test_all["f_power_mw"].values))
            if half_life:
                age = (pd.Timestamp(t0) - pd.to_datetime(train_all["origin"])).dt.days.clip(lower=0)
                weights = np.power(0.5, age / float(half_life)).values
            else:
                weights = np.ones(len(train_all))
            train_all = train_all.assign(_w=weights)

            for cand in mt["candidates"]:
                models, preds, importances = {}, [], []
                for key, g in train_all.groupby(group_cols):
                    if len(g) < mt["min_train_rows"]:
                        continue
                    m = _make_model(mt["model"])
                    m.fit(g[feats], g[label], sample_weight=g["_w"].values)
                    models[key if isinstance(key, tuple) else (key,)] = m
                    if hasattr(m, "feature_importances_"):
                        importances.append(pd.Series(m.feature_importances_, index=feats))
                for key, g in test_all.groupby(group_cols):
                    k = key if isinstance(key, tuple) else (key,)
                    p = models[k].predict(g[feats]) if k in models else np.zeros(len(g))
                    p = np.where(g["in_window"].values, np.clip(p, 0, 1.05), 0.0)  # no model outside the daytime window -> 0
                    preds.append(pd.Series(p * g["capacity_mw"].values, index=g.index))
                pred = pd.concat(preds) if preds else pd.Series(dtype=float)
                res = base.assign(model=cand, pred_mw=pred.reindex(base.index).values)
                out.append(res)

                day = res[res["in_window"] & res["label_ok"]]
                m_all = score(day, "pred_mw", "label_target_mw", "capacity_mw", thr)
                b_all = score(day.assign(pred_mw=test_all.loc[day.index, "f_power_mw"].values), "pred_mw", "label_target_mw", "capacity_mw", thr)
                with mlflow.start_run(run_name=f"{run['product']}_{split}_{site}_{cand}"):
                    mlflow.log_params({
                        "product": run["product"], "split": split, "test_start": str(t0), "test_end": str(t1), "site": site,
                        "candidate": cand, "n_models": len(models), "n_features": len(feats), "group_by": ",".join(group_cols),
                        "excluded_prefixes": ",".join(mt.get("exclude_feature_prefixes") or []) or "none",
                        "recency_half_life_days": half_life or "off", "model_type": mt["model"]["type"],
                        "daytime": f"{mt['daytime']['mode']} {mt['daytime'].get('start_hour_gulf')}-{mt['daytime'].get('end_hour_gulf')}",
                        "mape_min_actual_mw": thr, "train_rows": len(train_all), "test_rows": len(test_all),
                        **{f"m_{k}": v for k, v in mt["model"]["params"].items()}})
                    mlflow.log_metrics({f"test_{k}": v for k, v in m_all.items() if v == v} | {f"baseline_{k}": v for k, v in b_all.items() if v == v})
                    for s_val, gs in day.groupby("step"):
                        sm = score(gs, "pred_mw", "label_target_mw", "capacity_mw", thr)
                        if sm["mape_pct"] == sm["mape_pct"]:
                            mlflow.log_metric("test_mape_pct_by_step", sm["mape_pct"], step=int(s_val))
                    with tempfile.TemporaryDirectory() as d:
                        if importances:
                            imp = pd.concat(importances, axis=1).mean(axis=1).sort_values(ascending=False)
                            imp.rename("mean_importance").to_csv(Path(d) / "feature_importance.csv")
                            mlflow.log_artifact(str(Path(d) / "feature_importance.csv"))
                        if mt["mlflow"]["log_models"]:
                            with open(Path(d) / "models.pkl", "wb") as fh:
                                pickle.dump({"models": models, "features": feats, "group_cols": group_cols, "label": label}, fh)
                            mlflow.log_artifact(str(Path(d) / "models.pkl"))
                print(f"    {split} {site} {cand}: {len(models)} models | MAPE {m_all['mape_pct']:.2f}% (Reuniwatt {b_all['mape_pct']:.2f}%) "
                      f"| nMAE {m_all['nmae_pct']:.2f}% (Reuniwatt {b_all['nmae_pct']:.2f}%) | test {t0} → {t1}")

    preds = pd.concat(out, ignore_index=True)
    for c in ("is_day", "label_ok", "in_window"):
        preds[c] = preds[c].astype(bool)
    return spark.createDataFrame(preds)
