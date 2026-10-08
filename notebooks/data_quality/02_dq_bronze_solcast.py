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
notes_init(tag)

raw = spark.table(TABLE)
raw.printSchema()
sc = raw.withColumn(TS, F.col(TS).cast("timestamp"))

note("missing_solcast_columns", [c for c in SOLCAST_COLS if c not in sc.columns])
note("extra_columns", [c for c in sc.columns if c not in SOLCAST_COLS + [TS]])

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
note("issue_time_like_columns", issue_like or "none")

now = spark.sql("SELECT current_timestamp() AS now").first()["now"]
r = sc.agg(F.max(TS).alias("max_ts"), F.sum((F.col(TS) > F.current_timestamp()).cast("int")).alias("future_rows")).first()
note("now", now)
note("max_period_end", r.max_ts)
note("future_rows", r.future_rows)
print("If future_rows == 0, live inference has no target_* features and writes nothing (bug C1).")

# COMMAND ----------

dups = duplicate_timestamps(sc, [TS], present(sc, ["ghi", "air_temp", "cloud_opacity"]))
note("period_end_with_multiple_rows", dup_note(dups))
display(dups.limit(50))

# COMMAND ----------

display(save(spark.sql(f"DESCRIBE HISTORY {TABLE}").select("version", "timestamp", "operation", "operationParameters").limit(30), f"{tag}_history"))

# COMMAND ----------

# MAGIC %md ## 2. Range, interval, completeness

# COMMAND ----------

display(save(time_range(sc, TS), f"{tag}_range"))
display(save(interval_profile(sc, TS).limit(20), f"{tag}_interval_profile"))

INTERVAL = infer_interval_min(sc, TS)
note("native_interval_min", INTERVAL)

# COMMAND ----------

display(save(completeness_summary(sc, TS, INTERVAL), f"{tag}_completeness"))

sc_gaps = gaps(sc, TS, INTERVAL)
note("gaps", sc_gaps.count())
display(save(sc_gaps, f"{tag}_gaps").limit(100))

# COMMAND ----------

key_cols = present(sc, ["ghi", "dni", "dhi", "gti", "clearsky_ghi", "air_temp", "cloud_opacity"])
per_day = save(completeness_per_day(sc, TS, INTERVAL, key_cols), f"{tag}_completeness_per_day")
low_days = save(per_day.where(" OR ".join(f"{c}_pct < 99" for c in key_cols) + " OR timestamp_pct < 99"), f"{tag}_low_days")
note("days_below_99pct", f"{low_days.count()} of {per_day.count()}")
display(low_days)

# COMMAND ----------

# MAGIC %md ## 3. Null rates (day vs night)
# MAGIC Silver forward-fills every Solcast column, so it hides these nulls.

# COMMAND ----------

display(save(null_rates(sc, present(sc, SOLCAST_COLS), day_flag), f"{tag}_null_rates"))

# COMMAND ----------

# MAGIC %md ## 4. Validity

# COMMAND ----------

display(save(column_stats(sc, present(sc, SOLCAST_COLS)), f"{tag}_column_stats"))

# COMMAND ----------

display(save(rule_counts(sc, weather_rules(sc)), f"{tag}_rules"))

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
display(save(stuck.groupBy("column").agg(F.count("*").alias("runs"), F.sum("rows").alias("rows_affected"), F.max("rows").alias("longest")), f"{tag}_stuck_summary"))
display(save(stuck, f"{tag}_stuck_runs").limit(200))

# COMMAND ----------

# MAGIC %md ## 6. Timezone and period convention
# MAGIC The hour with the lowest mean zenith (or highest clear-sky GHI) is about 08 UTC or about 12 Asia/Dubai. Because `period_end` labels the end of an interval, the peak shifts later by up to one interval.

# COMMAND ----------

prof = hourly_profile(sc, TS, present(sc, ["zenith", "clearsky_ghi", "ghi"]))
display(save(prof, f"{tag}_hourly_profile"))
peak = prof.orderBy(F.desc("avg_clearsky_ghi")).first()["hour"] if "clearsky_ghi" in sc.columns else prof.orderBy("avg_zenith").first()["hour"]
note("peak_hour", f"{peak:02d}:00 -> looks like {guess_timezone(peak)}")
