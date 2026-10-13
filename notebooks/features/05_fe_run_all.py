# Databricks notebook source
# MAGIC %md
# MAGIC # FE 05 · Run the feature pipeline
# MAGIC Runs 02 → 03 → 04 (intraday, day-ahead, week-ahead) with the same settings, then writes a short summary to `results/fe_run_summary.txt` next to this notebook so it can be pushed to Git.
# MAGIC
# MAGIC **First run:** keep the defaults (Bronze, from 2026-09-01) as a quick end-to-end test. **Then:** set `start_date = 2024-01-23` for the full history.

# COMMAND ----------

dbutils.widgets.text("output_schema", "ewec_dev_powerops.scratch_yash", "Output schema (catalog.schema)")
dbutils.widgets.dropdown("source", "bronze", ["bronze", "silver"], "Reuniwatt source")
dbutils.widgets.text("start_date", "2026-09-01", "First date (test: 2026-09-01, full: 2024-01-23)")
dbutils.widgets.text("swap_date", "auto", "Bronze PV column swap date")

# COMMAND ----------

import os
import time

args = {k: dbutils.widgets.get(k) for k in ["output_schema", "source", "start_date", "swap_date"]}
schema = args["output_schema"]
log = []


def run(nb, **extra):
    params = {**args, **extra}
    t0 = time.time()
    try:
        dbutils.notebook.run(f"./{nb}", 0, params)
        status = "ok"
    except Exception as e:
        status = f"FAILED: {str(e)[:1500]}"
    log.append(f"{nb} {extra or ''}: {status} ({(time.time() - t0) / 60:.1f} min)")
    print(log[-1])
    return status == "ok"


if run("02_fe_reuniwatt_forecasts") and run("03_fe_pv_target"):
    for product in ["intraday", "dayahead", "weekahead"]:
        run("04_fe_build_features", product=product)

# COMMAND ----------

from pyspark.sql import functions as F

lines = ["# Feature pipeline run", f"settings: {args}", "", "## Steps", *log, "", "## Tables"]
for t in ["fe_reuniwatt_forecasts", "fe_pv_target_5min", "fe_features_intraday_5min", "fe_features_dayahead_5min", "fe_features_weekahead_5min"]:
    try:
        df = spark.table(f"{schema}.{t}")
        lines.append(f"{t}: {df.count():,} rows, {len(df.columns)} columns")
        if t.startswith("fe_features"):
            s = df.agg(
                F.min("origin"), F.max("origin"),
                F.round(100 * F.avg(F.col("f_power_mw").isNotNull().cast("int")), 1),
                F.round(100 * F.avg(F.col("label_ok").cast("int")), 1),
                F.round(F.corr("power_norm", "label_pv_norm"), 3),
            ).first()
            lines.append(f"   origins {s[0]} → {s[1]}, with forecast {s[2]}%, label_ok {s[3]}%, corr(Reuniwatt, actual) {s[4]}")
            lines.append("   columns: " + ", ".join(df.columns))
        if t == "fe_pv_target_5min":
            for r in df.groupBy("site", "status").count().orderBy("site", "status").collect():
                lines.append(f"   {r['site']} {r['status']}: {r['count']:,}")
    except Exception as e:
        lines.append(f"{t}: not available ({str(e)[:200]})")

text = "\n".join(lines)
os.makedirs("results", exist_ok=True)
with open("results/fe_run_summary.txt", "w") as f:
    f.write(text)
print(text)
