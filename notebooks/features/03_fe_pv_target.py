# Databricks notebook source
# MAGIC %md
# MAGIC # FE 03 · PV target → `fe_pv_target_5min`
# MAGIC Bronze 1-minute PV telemetry, cleaned and turned into a 5-minute target in UTC with Reuniwatt site labels.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | Corrupt values | Values with absolute size above 5,000 MW (the ±9.2e18 sentinels) → null |
# MAGIC | Small negatives | From −10 to 0 MW (night self-consumption) → 0; below −10 → null |
# MAGIC | Duplicates | Same minute logged more than once → averaged |
# MAGIC | Site mapping | `pv2` = larger plant, `pv1` = smaller. The Bronze columns swapped around March 2026; the swap date is detected from daily peaks (`swap_date = auto`) or set by hand |
# MAGIC | Timezone | Telemetry is Gulf time → converted to UTC |
# MAGIC | 5-minute periods | Labelled by period end, like Reuniwatt; `n_valid_min` = minutes with data (1 when the logger ran at 5-minute steps) |
# MAGIC | Status | `night`: sun down, missing → 0 · `missing`: daytime with no data (kept null, **not** zero-filled) · `suspected_outage`: sunny but output under 1% of capacity · `ok` |
# MAGIC
# MAGIC Train and score only on rows with `label_ok = true`.

# COMMAND ----------

dbutils.widgets.text("output_schema", "ewec_dev_powerops.scratch_yash", "Output schema (catalog.schema)")
dbutils.widgets.text("swap_date", "auto", "Bronze column swap date (YYYY-MM-DD, 'auto' or 'none')")

# COMMAND ----------

# MAGIC %run ./00_fe_common

# COMMAND ----------

from datetime import date, timedelta


def clean(c: str):
    v = F.col(c).cast("double")
    return (
        F.when(F.abs(v) > PV_SENTINEL_ABS_MW, F.lit(None).cast("double"))
        .when(v < NIGHT_NEG_FLOOR_MW, F.lit(None).cast("double"))
        .when(v < 0, F.lit(0.0))
        .otherwise(v)
    )


pv = (
    spark.table(PV_BRONZE)
    .select(F.col("DateTime").cast("timestamp").alias("ts_gulf"), clean("PV1_MW").alias("PV1_MW"), clean("PV2_MW").alias("PV2_MW"))
    .where("ts_gulf IS NOT NULL")
    .groupBy("ts_gulf").agg(F.avg("PV1_MW").alias("PV1_MW"), F.avg("PV2_MW").alias("PV2_MW"))
)

# COMMAND ----------

# MAGIC %md ## Bronze column swap
# MAGIC The larger plant peaks around 1,650 MW and the smaller around 950 MW. On each day, compare the two columns' peaks (7-day majority vote). The swap date is the day after the last day on which `PV1_MW` was still the larger plant.

# COMMAND ----------

daily = (
    pv.groupBy(F.to_date("ts_gulf").alias("day")).agg(F.max("PV1_MW").alias("pv1_peak"), F.max("PV2_MW").alias("pv2_peak"))
    .withColumn("pv2_bigger", (F.col("pv2_peak") > F.col("pv1_peak")).cast("int"))
    .withColumn("pv2_bigger_7d", F.avg("pv2_bigger").over(Window.orderBy("day").rowsBetween(-3, 3)))
)

SWAP = dbutils.widgets.get("swap_date").strip().lower()
if SWAP == "auto":
    last_pv1_big = daily.where("pv2_bigger_7d < 0.5").agg(F.max("day")).first()[0]
    last_day = daily.agg(F.max("day")).first()[0]
    swap_day = (last_pv1_big + timedelta(days=1)) if last_pv1_big and last_pv1_big < last_day else None
elif SWAP == "none":
    swap_day = None
else:
    swap_day = date.fromisoformat(SWAP)
print(f"Swap date used: {swap_day or 'no swap'}")

if swap_day:
    display(daily.where(F.col("day").between(F.lit(swap_day - timedelta(days=20)), F.lit(swap_day + timedelta(days=20)))).orderBy("day"))

# COMMAND ----------

# MAGIC %md ## 5-minute target with status

# COMMAND ----------

swap_lit = F.lit(swap_day or date(9999, 12, 31))
long = reduce(DataFrame.unionByName, [
    pv.select(
        F.lit(s).alias("site"),
        "ts_gulf",
        F.when(F.to_date("ts_gulf") < swap_lit, F.col(cfg["bronze_col_before_swap"])).otherwise(F.col(cfg["bronze_col_after_swap"])).alias("pv_mw"),
    )
    for s, cfg in SITES.items()
])
agg = (
    long.withColumn("period_end", ceil_to(gulf_to_utc("ts_gulf"), STEP_MIN))
    .groupBy("site", "period_end")
    .agg(F.avg("pv_mw").alias("pv_mw"), F.count("pv_mw").alias("n_valid_min"))
)

# Full 5-minute spine so periods with no rows at all show up as missing
b = agg.agg(F.min("period_end").alias("lo"), F.max("period_end").alias("hi")).first()
spine = (
    spark.range(1).select(F.explode(F.sequence(F.lit(b.lo), F.lit(b.hi), F.expr(f"INTERVAL {STEP_MIN} MINUTES"))).alias("period_end"))
    .crossJoin(spark.createDataFrame([(s,) for s in SITES], "site string"))
)

cap = site_value("capacity_mw")
t = add_solar(spine.join(agg, ["site", "period_end"], "left").fillna(0, ["n_valid_min"]), "period_end")
status = (
    F.when(F.col("sun_zenith") > 95, "night")
    .when(F.col("n_valid_min") == 0, "missing")
    .when((F.col("clearsky_ghi") > 200) & (F.col("pv_mw") < 0.01 * cap), "suspected_outage")
    .otherwise("ok")
)
target = (
    t.withColumn("status", status)
    .withColumn("pv_mw", F.when(F.col("status") == "night", F.coalesce("pv_mw", F.lit(0.0))).otherwise(F.col("pv_mw")))
    .withColumn("capacity_mw", cap)
    .withColumn("pv_norm", F.col("pv_mw") / F.col("capacity_mw"))
    .withColumn("label_ok", F.col("status").isin("ok", "night"))
    .withColumn("period_end_gulf", utc_to_gulf("period_end"))
    .select("site", "period_end", "period_end_gulf", "pv_mw", "pv_norm", "capacity_mw", "n_valid_min", "status", "label_ok",
            "sun_zenith", "clearsky_ghi", "is_day")
)
target = write(target, "fe_pv_target_5min", partition_by=("site",))

# COMMAND ----------

display(target.groupBy("site", "status").count().orderBy("site", "status"))
display(
    target.where("is_day").groupBy("site", F.date_trunc("month", "period_end_gulf").alias("month"))
    .agg(F.round(100 * F.avg(F.col("label_ok").cast("int")), 2).alias("daytime_label_ok_pct"), F.round(F.max("pv_mw")).alias("peak_mw"))
    .orderBy("site", "month")
)
