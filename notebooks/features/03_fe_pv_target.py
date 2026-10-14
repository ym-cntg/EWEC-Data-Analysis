# Databricks notebook source
# MAGIC %md
# MAGIC # FE 03 · PV target → `fe_pv_target_5min`
# MAGIC Bronze 1-minute PV telemetry, cleaned and turned into a 5-minute target in UTC.
# MAGIC
# MAGIC **Targets (agreed 2026-10-14):** `pv1` (small plant) = actual MW · `pv2` (large plant) = **`Pv2_AVL`** (available power, from 31 May 2023).
# MAGIC In the output, `pv_mw` is the **target**; `actual_mw` is the actual output, kept for comparison.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | Corrupt values | Absolute MW above 5,000 (the ±9.2e18 sentinels) → null; `Pv2_AVL` below 0 or above 2,000 → null |
# MAGIC | Small negatives | MW from −10 to 0 (night self-consumption) → 0; below −10 → null |
# MAGIC | Column swap | Bronze `PV1_MW` / `PV2_MW` were mislabelled before ~March 2026. Swap date detected from daily peaks (`swap_date = auto`) or set by hand. `Pv2_AVL` was never swapped |
# MAGIC | Timezone | Telemetry is Gulf time → converted to UTC |
# MAGIC | 5-minute periods | Labelled by period end, like Reuniwatt; `n_valid_min` = minutes with a target value |
# MAGIC | Status | `night`: sun down, missing → 0 · `missing`: daytime with no data (kept null, **not** zero-filled) · `suspected_outage`: sunny but target under 1% of capacity · `ok` |
# MAGIC
# MAGIC Train and score only on rows with `label_ok = true`.

# COMMAND ----------

dbutils.widgets.text("output_schema", "ewec_dev_powerops.scratch_yash", "Output schema (catalog.schema)")
dbutils.widgets.text("swap_date", "auto", "Bronze MW column swap date (YYYY-MM-DD, 'auto' or 'none')")

# COMMAND ----------

# MAGIC %run ./00_fe_common

# COMMAND ----------

SWAP = dbutils.widgets.get("swap_date").strip().lower()
swap_day = detect_pv_swap_date() if SWAP == "auto" else None if SWAP == "none" else date.fromisoformat(SWAP)
print(f"Swap date used: {swap_day or 'no swap'}")

avl = F.col("Pv2_AVL").cast("double")
avl_clean = F.when((avl < 0) | (avl > 2000), F.lit(None).cast("double")).otherwise(avl)
TARGET_SOURCE = {"pv1": "actual MW", "pv2": "Pv2_AVL"}

src = spark.table(PV_BRONZE).withColumn("ts_gulf", F.col("DateTime").cast("timestamp")).where("ts_gulf IS NOT NULL")
long = reduce(DataFrame.unionByName, [
    src.select(
        F.lit(s).alias("site"), "ts_gulf",
        site_mw(s, swap_day).alias("actual_mw"),
        (avl_clean if s == "pv2" else site_mw(s, swap_day)).alias("target"),
    )
    for s in SITES
])

# COMMAND ----------

# MAGIC %md ## 5-minute target with status

# COMMAND ----------

agg = (
    long.withColumn("period_end", ceil_to(gulf_to_utc("ts_gulf"), STEP_MIN))
    .groupBy("site", "period_end")
    .agg(F.avg("target").alias("pv_mw"), F.count("target").alias("n_valid_min"), F.avg("actual_mw").alias("actual_mw"))
)

# Full 5-minute spine so periods with no rows at all show up as missing
b = agg.agg(F.min("period_end").alias("lo"), F.max("period_end").alias("hi")).first()
spine = (
    spark.range(1).select(F.explode(F.sequence(F.lit(b.lo), F.lit(b.hi), F.expr(f"INTERVAL {STEP_MIN} MINUTES"))).alias("period_end"))
    .crossJoin(spark.createDataFrame([(s,) for s in SITES], "site string"))
)

cap = site_value("capacity_mw")
target_source = F.when(F.col("site") == "pv2", F.lit(TARGET_SOURCE["pv2"])).otherwise(F.lit(TARGET_SOURCE["pv1"]))
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
    .withColumn("target_source", target_source)
    .withColumn("period_end_gulf", utc_to_gulf("period_end"))
    .select("site", "period_end", "period_end_gulf", "pv_mw", "pv_norm", "target_source", "actual_mw", "capacity_mw",
            "n_valid_min", "status", "label_ok", "sun_zenith", "clearsky_ghi", "is_day")
)
target = write(target, "fe_pv_target_5min", partition_by=("site",))

# COMMAND ----------

# MAGIC %md ## Checks
# MAGIC - **Status counts per site.** PV2 has many `missing` periods before 31 May 2023, when `Pv2_AVL` starts.
# MAGIC - **Monthly view.** Daytime share of usable labels; peak target (~950 pv1, ~1,640 pv2); average gap between target and actual (pv2: available − actual ≈ curtailment / outages, 0 for pv1).

# COMMAND ----------

display(target.groupBy("site", "target_source", "status").count().orderBy("site", "status"))
display(
    target.where("is_day").groupBy("site", F.date_trunc("month", "period_end_gulf").alias("month"))
    .agg(F.round(100 * F.avg(F.col("label_ok").cast("int")), 2).alias("daytime_label_ok_pct"),
         F.round(F.max("pv_mw")).alias("peak_target_mw"),
         F.round(F.avg(F.col("pv_mw") - F.col("actual_mw")), 1).alias("avg_target_minus_actual_mw"))
    .orderBy("site", "month")
)
