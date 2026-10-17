"""Evaluation nodes: test-set metrics per split × site × model (overall, by forecast time, by clock hour, by step) and a text report.
Scored on daytime-window slots (where models exist) with usable labels. MAPE only where actual > `mape_min_actual_mw`."""
from datetime import datetime

import pandas as pd
from pyspark.sql import DataFrame

from ...metrics import score


def evaluate(spark, predictions: DataFrame, ev: dict, run: dict):
    pdf = predictions.toPandas()
    thr = ev["mape_min_actual_mw"]
    day = pdf[pdf["in_window"] & pdf["label_ok"]].copy()
    day["gulf_hour"] = (pd.to_datetime(day["period_end"]) + pd.Timedelta(hours=4) - pd.Timedelta(minutes=1)).dt.hour
    outside = pdf[pdf["is_day"] & ~pdf["in_window"] & pdf["label_ok"] & (pdf["model"] == "reuniwatt_baseline")]

    def table(df, by):
        rows = []
        for key, g in df.groupby(by):
            key = key if isinstance(key, tuple) else (key,)
            rows.append({**dict(zip(by, key)), **score(g, "pred_mw", "label_target_mw", "capacity_mw", thr)})
        return pd.DataFrame(rows)

    summary = table(day, ["split", "site", "model"]).sort_values(["split", "site", "mape_pct"])
    by_step = table(day, ["split", "site", "model", "origin_hour_gulf", "step"])
    parts = [f"# UC2 Solar — {run['product']} — evaluation ({datetime.utcnow():%Y-%m-%d %H:%M} UTC)",
             f"Scored on daytime-window slots with usable labels. MAPE only where actual > {thr:g} MW (Phase 1 rule). "
             f"nMAE / RMSE / bias as % of capacity.",
             "## Overall (lower is better)\n" + summary.round(2).to_string(index=False)]
    for split, d in day.groupby("split"):
        t0, t1 = pd.to_datetime(d["origin"]).min().date(), pd.to_datetime(d["origin"]).max().date()
        parts += [f"## Split {split} (test origins {t0} → {t1})",
                  "### By forecast time (Gulf hour)\n" + table(d, ["site", "model", "origin_hour_gulf"]).sort_values(["site", "origin_hour_gulf", "mape_pct"]).round(2).to_string(index=False),
                  "### By clock hour of the target (Gulf)\n" + table(d, ["site", "gulf_hour", "model"]).sort_values(["site", "gulf_hour", "model"]).round(2).to_string(index=False)]
    if len(outside):
        parts.append(f"Sun-up slots OUTSIDE the daytime window (forecast forced to 0): {len(outside)} rows, average actual "
                     f"{outside['label_target_mw'].mean():.1f} MW, max {outside['label_target_mw'].max():.0f} MW. "
                     "Large values mean the window cuts off real production (consider daytime.mode: sun).")
    report = "\n\n".join(parts)
    print(report)
    return spark.createDataFrame(summary), spark.createDataFrame(by_step), report
