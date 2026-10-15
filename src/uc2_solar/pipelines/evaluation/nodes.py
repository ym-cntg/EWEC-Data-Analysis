"""Evaluation nodes: test-set metrics per site × model (overall, by forecast time, by lead hour) and a text report."""
from datetime import datetime

import pandas as pd
from pyspark.sql import DataFrame

from ...metrics import score


def evaluate(spark, predictions: DataFrame, ev: dict, run: dict):
    pdf = predictions.toPandas()
    day = pdf[pdf["is_day"] & pdf["label_ok"]].copy()
    day["lead_hour"] = ((day["lead_min"] - 1) // 60 + 1).astype(int)
    share = ev["mape_min_target_share"]

    def table(by):
        rows = []
        for key, g in day.groupby(by):
            key = key if isinstance(key, tuple) else (key,)
            rows.append({**dict(zip(by, key)), **score(g, "pred_mw", "label_target_mw", "capacity_mw", share)})
        return pd.DataFrame(rows)

    summary = table(["site", "model"]).sort_values(["site", "mape_pct"])
    by_origin = table(["site", "model", "origin_hour_gulf"])
    by_step = table(["site", "model", "origin_hour_gulf", "step"])
    by_lead_hour = table(["site", "model", "lead_hour"])

    def fmt(df):
        return df.round(2).to_string(index=False)

    test_from, test_to = pdf["origin"].min(), pdf["origin"].max()
    report = "\n\n".join([
        f"# UC2 Solar — {run['product']} — evaluation ({datetime.utcnow():%Y-%m-%d %H:%M} UTC)",
        f"Test origins: {test_from} → {test_to}. Daytime rows with usable labels. MAPE only where target > {int(share * 100)}% of capacity.",
        "## Overall (lower is better)\n" + fmt(summary),
        "## By forecast time (Gulf hour)\n" + fmt(by_origin.sort_values(["site", "origin_hour_gulf", "mape_pct"])),
        "## By lead hour\n" + fmt(by_lead_hour.sort_values(["site", "lead_hour", "model"])),
    ])
    print(report)
    return spark.createDataFrame(summary), spark.createDataFrame(by_step), report
