"""Model training nodes: one model per (origin hour ×) forecast step × plant × candidate, logged to MLflow.

Label: target ÷ capacity. Only daytime slots get models (`model_training.daytime`: fixed Gulf hours or sun up);
outside that window the forecast is 0. Split: the last `holdout_months` of origins are the test set.
"""
import pickle
import tempfile
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
    """Usable labels only, with a train / test split on origin date."""
    max_day = feat_table.agg(F.max(F.to_date("origin"))).first()[0]
    cutoff = F.add_months(F.lit(max_day), -int(mt["holdout_months"]))
    print(f"    last origin {max_day}; test = origins after {max_day} minus {mt['holdout_months']} months")
    return (feat_table.where("label_ok")
            .withColumn("split", F.when(F.to_date("origin") > cutoff, F.lit("test")).otherwise(F.lit("train"))))


def feature_columns(df: DataFrame):
    """All numeric columns except keys, labels and run timestamps (origin hour, step and lead are kept as features)."""
    numeric = {"double", "float", "integer", "long", "short", "byte", "decimal"}
    return [f.name for f in df.schema.fields
            if f.name not in NON_FEATURES and not f.name.endswith("_run_time") and f.dataType.typeName() in numeric]


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


def _experiment_path(spark, name: str) -> str:
    user = spark.sql("SELECT current_user()").first()[0]
    return f"/Users/{user}/{name}"


def train_models(spark, model_input: DataFrame, mt: dict, run: dict, product_cfg: dict, ev: dict) -> DataFrame:
    """Train every group model, log one MLflow run per site × candidate, return test predictions (incl. Reuniwatt baseline)."""
    import mlflow

    feats = feature_columns(model_input)
    group_cols = (["origin_hour_gulf"] if mt["split_by_origin_hour"] else []) + ["step"]
    label = mt["label"]
    print(f"    {len(feats)} features; groups by {group_cols}")
    # Databricks turns MLflow autologging on by default: it would create one run per fitted model (~1,500 runs).
    # We log one run per site × candidate ourselves, so switch it off.
    mlflow.autolog(disable=True)
    mlflow.set_experiment(_experiment_path(spark, mt["mlflow"]["experiment"]))
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    out = []
    for site in [r[0] for r in model_input.select("site").distinct().orderBy("site").collect()]:
        pdf = model_input.where(F.col("site") == site).select(*set(KEY_COLS + feats + LABEL_COLS + ["split", "capacity_mw", "is_day", "f_power_mw"])).toPandas()
        pdf[feats] = pdf[feats].astype("float32")
        pdf["in_window"] = in_daytime_window(pdf, mt["daytime"])
        train_all, test_all = pdf[pdf["split"] == "train"], pdf[pdf["split"] == "test"]
        base = test_all[KEY_COLS + ["is_day", "in_window", "capacity_mw", "label_target_mw", "label_ok"]].copy()
        out.append(base.assign(model="reuniwatt_baseline", pred_mw=test_all["f_power_mw"].values))

        for cand, ccfg in mt["candidates"].items():
            tr = train_all[train_all["in_window"]]
            models, preds, importances = {}, [], []
            for key, g in tr.groupby(group_cols):
                if len(g) < mt["min_train_rows"]:
                    continue
                m = _make_model(mt["model"])
                m.fit(g[feats], g[label])
                models[key if isinstance(key, tuple) else (key,)] = m
                if hasattr(m, "feature_importances_"):
                    importances.append(pd.Series(m.feature_importances_, index=feats))
            for key, g in test_all.groupby(group_cols):
                k = key if isinstance(key, tuple) else (key,)
                p = models[k].predict(g[feats]) if k in models else np.zeros(len(g))
                p = np.clip(p, 0, 1.05)
                p = np.where(g["in_window"].values, p, 0.0)  # no model outside the daytime window -> 0
                preds.append(pd.Series(p * g["capacity_mw"].values, index=g.index))
            pred = pd.concat(preds) if preds else pd.Series(dtype=float)
            res = base.assign(model=cand, pred_mw=pred.reindex(base.index).values)
            out.append(res)

            day = res[res["in_window"] & res["label_ok"]]
            m_all = score(day, "pred_mw", "label_target_mw", "capacity_mw", ev["mape_min_target_share"])
            b_all = score(day.assign(pred_mw=test_all.loc[day.index, "f_power_mw"].values), "pred_mw", "label_target_mw", "capacity_mw", ev["mape_min_target_share"])
            with mlflow.start_run(run_name=f"{run['product']}_{site}_{cand}"):
                mlflow.log_params({"product": run["product"], "site": site, "candidate": cand, "n_models": len(models),
                                   "n_features": len(feats), "group_by": ",".join(group_cols), "model_type": mt["model"]["type"],
                                   "daytime_mode": mt["daytime"]["mode"],
                                   "daytime_hours": f"{mt['daytime'].get('start_hour_gulf')}-{mt['daytime'].get('end_hour_gulf')}",
                                   "train_rows": len(tr), "test_rows": len(test_all), **{f"m_{k}": v for k, v in mt["model"]["params"].items()}})
                mlflow.log_metrics({f"test_{k}": v for k, v in m_all.items() if v == v} | {f"baseline_{k}": v for k, v in b_all.items() if v == v})
                for s_val, gs in day.groupby("step"):
                    sm = score(gs, "pred_mw", "label_target_mw", "capacity_mw", ev["mape_min_target_share"])
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
            print(f"    {site} {cand}: {len(models)} models | test MAPE {m_all['mape_pct']:.2f}% (baseline {b_all['mape_pct']:.2f}%) "
                  f"| nMAE {m_all['nmae_pct']:.2f}% (baseline {b_all['nmae_pct']:.2f}%)")

    preds = pd.concat(out, ignore_index=True)
    preds["is_day"] = preds["is_day"].astype(bool)
    preds["label_ok"] = preds["label_ok"].astype(bool)
    preds["in_window"] = preds["in_window"].astype(bool)
    return spark.createDataFrame(preds)
