# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Run all DQ checks
# MAGIC 1. Set `scratch_schema` (default `ewec_dev_powerops.scratch_yash`), then **Run all**.
# MAGIC 2. When it finishes, open `07_dq_export`, **Run all**, and copy its single output.
# MAGIC
# MAGIC What it does:
# MAGIC - Creates the scratch schema if it's missing (needs `CREATE SCHEMA` on the catalog; otherwise ask Harish).
# MAGIC - Runs 01 → 02 (PV1, PV2) → 04 → 03 → 05 → 06 and records status, duration and errors in `dq_run_log`. A failing notebook doesn't stop the others.
# MAGIC - **Auto re-runs** 05 and 06 with `timestamps_tz = Asia/Dubai` if their timezone test says so, and 06 with `pv_shift_h` if PV is offset.
# MAGIC
# MAGIC Expect it to take a while (tens of minutes on a small warehouse). Bronze PV is 1-minute data.

# COMMAND ----------

dbutils.widgets.text("scratch_schema", "ewec_dev_powerops.scratch_yash", "Scratch schema (catalog.schema)")

# COMMAND ----------

import time
from datetime import datetime

from pyspark.sql import functions as F

SCHEMA = dbutils.widgets.get("scratch_schema").strip()
assert SCHEMA and SCHEMA.count(".") == 1, "Set scratch_schema as catalog.schema"
assert SCHEMA.split(".")[-1].lower() not in {"bronze", "silver", "gold"}, "Use a personal scratch schema"

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} COMMENT 'Data-quality check outputs'")
    print(f"Schema ready: {SCHEMA}")
except Exception as e:
    raise RuntimeError(f"Cannot create or use {SCHEMA}. Ask Harish for a schema you can write to, then set the widget. ({e})")

LOG = []


def run(nb: str, label: str, **params) -> bool:
    args = {"scratch_schema": SCHEMA, **{k: str(v) for k, v in params.items()}}
    t0 = time.time()
    print(f"▶ {label} {params or ''}")
    try:
        dbutils.notebook.run(f"./{nb}", 0, args)
        status, err = "ok", ""
    except Exception as e:
        status, err = "FAILED", str(e)[:2000]
    mins = round((time.time() - t0) / 60, 1)
    print(f"  {status} in {mins} min {('— ' + err[:300]) if err else ''}")
    LOG.append((label, nb, str(params), status, mins, err, datetime.utcnow().isoformat(timespec="seconds")))
    spark.createDataFrame(LOG, "label string, notebook string, params string, status string, minutes double, error string, finished_utc string") \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.dq_run_log")
    return status == "ok"


def table_or_none(name: str):
    try:
        return spark.table(f"{SCHEMA}.dq_{name}")
    except Exception:
        return None

# COMMAND ----------

run("01_dq_bronze_pv_telemetry", "01 PV telemetry")
run("02_dq_bronze_solcast", "02 Solcast PV1", site="PV1")
run("02_dq_bronze_solcast", "02 Solcast PV2", site="PV2")
run("04_dq_source_alignment", "04 Source alignment")
run("03_dq_silver_masking", "03 Silver masking")

# COMMAND ----------

# 05: run with UTC; re-run with Asia/Dubai if that assumption has fewer night-irradiance rows
if run("05_dq_reuniwatt_irradiance", "05 Reuniwatt irradiance (UTC)", timestamps_tz="UTC"):
    t = table_or_none("reuniwatt_irradiance_timezone_test")
    if t is not None:
        best = t.orderBy("night_rows_with_ghi", F.col("assumed_tz") != "UTC").first()["assumed_tz"]  # ties -> UTC
        print("Irradiance timezone test winner:", best)
        if best != "UTC":
            run("05_dq_reuniwatt_irradiance", f"05 Reuniwatt irradiance ({best})", timestamps_tz=best)

# COMMAND ----------

# 06: same timezone logic, then align PV if the lag scan shows one consistent offset
tz = "UTC"
if run("06_dq_reuniwatt_power", "06 Reuniwatt power (UTC)", timestamps_tz=tz, pv_shift_h=0):
    t = table_or_none("reuniwatt_power_timezone_test")
    if t is not None:
        tz = t.orderBy("night_rows_with_power", F.col("assumed_tz") != "UTC").first()["assumed_tz"]  # ties -> UTC
    mapping = table_or_none("reuniwatt_power_site_mapping")
    best_lag = table_or_none("reuniwatt_power_lag_best")
    shift = 0
    if mapping is not None and best_lag is not None:
        mapped = best_lag.join(mapping.select("site", "pv_col"), ["site", "pv_col"])
        offsets = {r["offset_h"] for r in mapped.collect()}
        if len(offsets) == 1:
            shift = -offsets.pop()
    print(f"Power: timezone winner {tz}, PV shift {shift} h")
    if tz != "UTC" or shift != 0:
        run("06_dq_reuniwatt_power", f"06 Reuniwatt power ({tz}, pv_shift_h={shift})", timestamps_tz=tz, pv_shift_h=shift)

# COMMAND ----------

display(spark.table(f"{SCHEMA}.dq_run_log"))
print("Done. Now run 07_dq_export with the same scratch_schema and copy its output.")
