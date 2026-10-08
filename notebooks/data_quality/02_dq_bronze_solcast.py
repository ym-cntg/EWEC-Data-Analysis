# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · DQ — Bronze Solcast history
# MAGIC `ewec_dev_powerops.bronze.pv{1,2}_history_solcast`, keyed on `period_end`.
# MAGIC
# MAGIC Covers completeness, validity, timezone, and whether rows are **forecasts (with an issue time)** or **estimated actuals**. Estimated actuals would mean the backtest has perfect-foresight leakage.
# MAGIC
# MAGIC Read-only. Set `scratch_schema` to persist summaries.

# COMMAND ----------

dbutils.widgets.text("scratch_schema", "", "Scratch schema (optional, catalog.schema)")
dbutils.widgets.dropdown("site", "PV1", ["PV1", "PV2"], "Solcast table")
dbutils.widgets.text("stuck_min_rows", "12", "Stuck-value min run (rows)")

# COMMAND ----------

# MAGIC %run ./00_dq_utils

# COMMAND ----------

SITE = dbutils.widgets.get("site")
TABLE = SOLCAST_TABLES[SITE]
TS = "period_end"
tag = f"solcast_{SITE.lower()}"

raw = spark.table(TABLE)
raw.printSchema()
sc = raw.withColumn(TS, F.col(TS).cast("timestamp"))

print("Expected Solcast columns missing:", [c for c in SOLCAST_COLS if c not in sc.columns])
print("Extra columns:", [c for c in sc.columns if c not in SOLCAST_COLS + [TS]])

day_flag = (
    F.when(F.col("zenith").isNull(), "unknown").when(F.col("zenith") < 90, "day").otherwise("night")
    if "zenith" in sc.columns
    else F.when(F.col("clearsky_ghi") > DAYTIME_CLEARSKY_GHI, "day").otherwise("night")
)

# COMMAND ----------

# MAGIC %md ## 1. Forecast or actuals?
# MAGIC Look for an issue/creation-time column, more than one row per `period_end`, rows in the future, and the table's write pattern.

# COMMAND ----------

issue_like = [c for c in sc.columns if any(k in c.lower() for k in ("issue", "forecast", "created", "ingest", "run", "load", "update", "asof", "as_of"))]
print("Issue-time-like columns:", issue_like or "none")

now = spark.sql("SELECT current_timestamp() AS now").first()["now"]
r = sc.agg(F.max(TS).alias("max_ts"), F.sum((F.col(TS) > F.current_timestamp()).cast("int")).alias("future_rows")).first()
print(f"now={now} | max period_end={r.max_ts} | rows in the future={r.future_rows}")
print("If future_rows == 0, live inference has no target_* features and writes nothing (bug C1).")

# COMMAND ----------

dups = duplicate_timestamps(sc, [TS], present(sc, ["ghi", "air_temp", "cloud_opacity"]))
print("period_end values with >1 row:", dups.count(), "| conflicting:", dups.where("distinct_value_sets > 1").count())
display(dups.limit(50))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABLE}").select("version", "timestamp", "operation", "operationParameters", "operationMetrics").limit(30))

# COMMAND ----------

# MAGIC %md ## 2. Range, interval, completeness

# COMMAND ----------

display(time_range(sc, TS))
display(interval_profile(sc, TS).limit(20))

INTERVAL = infer_interval_min(sc, TS)
print("Native interval (min):", INTERVAL)

# COMMAND ----------

display(save(completeness_summary(sc, TS, INTERVAL), f"{tag}_completeness"))

sc_gaps = gaps(sc, TS, INTERVAL)
print(f"Gaps > {INTERVAL} min:", sc_gaps.count())
display(save(sc_gaps, f"{tag}_gaps").limit(100))

# COMMAND ----------

key_cols = present(sc, ["ghi", "dni", "dhi", "gti", "clearsky_ghi", "air_temp", "cloud_opacity"])
per_day = save(completeness_per_day(sc, TS, INTERVAL, key_cols), f"{tag}_completeness_per_day")
display(per_day.where(" OR ".join(f"{c}_pct < 99" for c in key_cols) + " OR timestamp_pct < 99"))

# COMMAND ----------

# MAGIC %md ## 3. Null rates (day vs night)
# MAGIC Silver forward-fills every Solcast column, so it hides these nulls.

# COMMAND ----------

display(save(null_rates(sc, present(sc, SOLCAST_COLS), day_flag), f"{tag}_null_rates"))

# COMMAND ----------

# MAGIC %md ## 4. Validity

# COMMAND ----------

display(column_stats(sc, present(sc, SOLCAST_COLS)))

# COMMAND ----------

irr = present(sc, IRRADIANCE_COLS)
rules = {
    "negative_irradiance": (" OR ".join(f"{c} < 0" for c in irr) or "false", irr),
    "irradiance_at_night": ("zenith > 90 AND (ghi > 5 OR dni > 5 OR dhi > 5)", ["zenith", "ghi", "dni", "dhi"]),
    "clearsky_at_night": ("zenith > 90 AND clearsky_ghi > 5", ["zenith", "clearsky_ghi"]),
    "ghi_far_above_clearsky": ("ghi > clearsky_ghi * 1.2 AND ghi - clearsky_ghi > 50", ["ghi", "clearsky_ghi"]),
    "dni_far_above_clearsky": ("dni > clearsky_dni * 1.2 AND dni - clearsky_dni > 50", ["dni", "clearsky_dni"]),
    "ghi_closure_error": (
        "zenith < 85 AND abs(ghi - (dni * cos(radians(zenith)) + dhi)) > greatest(50, 0.1 * ghi)",
        ["ghi", "dni", "dhi", "zenith"],
    ),
    "cloud_opacity_out_of_0_100": ("cloud_opacity < 0 OR cloud_opacity > 100", ["cloud_opacity"]),
    "rh_out_of_0_100": ("relative_humidity < 0 OR relative_humidity > 100", ["relative_humidity"]),
    "zenith_out_of_0_180": ("zenith < 0 OR zenith > 180", ["zenith"]),
    "azimuth_out_of_range": ("azimuth < -180 OR azimuth > 360", ["azimuth"]),
    "albedo_out_of_0_1": ("albedo < 0 OR albedo > 1", ["albedo"]),
    "air_temp_implausible": ("air_temp < -5 OR air_temp > 60", ["air_temp"]),
    "dewpoint_above_air_temp": ("dewpoint_temp > air_temp + 0.5", ["dewpoint_temp", "air_temp"]),
    "wind_out_of_range": ("wind_speed_10m < 0 OR wind_speed_10m > 50", ["wind_speed_10m"]),
    "precip_negative": ("precipitation_rate < 0", ["precipitation_rate"]),
}
display(save(rule_counts(sc, rules), f"{tag}_rules"))

# COMMAND ----------

# Sample rows for any rule: change the condition
display(sc.where("zenith > 90 AND (ghi > 5 OR dni > 5 OR dhi > 5)").orderBy(TS).limit(100))

# COMMAND ----------

# MAGIC %md ## 5. Stuck values
# MAGIC Identical consecutive non-zero values. Default 12 rows (1 h at 5 min, 6 h at 30 min); adjust `stuck_min_rows` to the native interval.

# COMMAND ----------

min_rows = int(dbutils.widgets.get("stuck_min_rows"))
stuck_cols = present(sc, ["ghi", "dni", "dhi", "gti", "air_temp", "cloud_opacity", "relative_humidity", "wind_speed_10m"])
stuck = reduce(DataFrame.unionByName, [stuck_runs(sc, TS, c, min_rows) for c in stuck_cols])
display(stuck.groupBy("column").agg(F.count("*").alias("runs"), F.sum("rows").alias("rows_affected"), F.max("rows").alias("longest")))
display(save(stuck, f"{tag}_stuck_runs").limit(200))

# COMMAND ----------

# MAGIC %md ## 6. Timezone and period convention
# MAGIC The hour with the lowest mean zenith (or highest clear-sky GHI) is about 08 UTC or about 12 Asia/Dubai. Because `period_end` labels the end of an interval, the peak shifts later by up to one interval.

# COMMAND ----------

prof = hourly_profile(sc, TS, present(sc, ["zenith", "clearsky_ghi", "ghi"]))
display(prof)
peak = prof.orderBy(F.desc("avg_clearsky_ghi")).first()["hour"] if "clearsky_ghi" in sc.columns else prof.orderBy("avg_zenith").first()["hour"]
print(f"{SITE} Solcast: peak hour {peak:02d}:00 -> looks like {guess_timezone(peak)}")
