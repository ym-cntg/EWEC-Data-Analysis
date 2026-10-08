# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · DQ — Bronze PV telemetry
# MAGIC `ewec_dev_powerops.bronze.power_contango_minute_pv`: 1-min `DateTime`, `PV1_MW`, `PV2_MW`.
# MAGIC
# MAGIC Read-only. Results are displayed; set `scratch_schema` to also persist summaries (never bronze/silver/gold).
# MAGIC
# MAGIC ⚠️ Columns here use **upstream** labels. In Silver, `site='PV1'` holds `PV2_MW` and vice versa.

# COMMAND ----------

dbutils.widgets.text("scratch_schema", "", "Scratch schema (optional, catalog.schema)")
dbutils.widgets.text("stuck_min_minutes", "60", "Stuck-value min run (minutes)")

# COMMAND ----------

# MAGIC %run ./00_dq_utils

# COMMAND ----------

from pyspark.sql import functions as F
notes_init("pv")
TS = "DateTime"
raw = spark.table("ewec_dev_powerops.bronze.power_contango_minute_pv")
raw.printSchema()

pv = raw.select(F.col(TS).alias("_raw_ts"), F.col(TS).cast("timestamp").alias(TS), *PV_COLS)
note("unparseable_timestamps", pv.where(F.col("_raw_ts").isNotNull() & F.col(TS).isNull()).count())
pv = pv.drop("_raw_ts")

# COMMAND ----------

# MAGIC %md ## 1. Range and duplicates

# COMMAND ----------

display(save(time_range(pv, TS), "pv_range"))

dups = duplicate_timestamps(pv, [TS], PV_COLS)
note("duplicate_timestamps", dup_note(dups))
display(dups.limit(50))

# COMMAND ----------

# MAGIC %md ## 2. Interval regularity
# MAGIC Expect almost all gaps = 1 min. Anything else is missing data or irregular sampling.

# COMMAND ----------

display(save(interval_profile(pv, TS).limit(20), "pv_interval_profile"))

pv_gaps = gaps(pv, TS, 1)
note("gaps_over_1min", pv_gaps.count())
note("missing_minutes_in_gaps", pv_gaps.agg(F.sum("missing_minutes")).first()[0])
display(save(pv_gaps, "pv_gaps").limit(100))

# COMMAND ----------

# MAGIC %md ## 3. Completeness
# MAGIC Timestamp coverage (row present) and per-column coverage (row present **and** value non-null), per day.

# COMMAND ----------

display(save(completeness_summary(pv, TS, 1), "pv_completeness"))

per_day = save(completeness_per_day(pv, TS, 1, PV_COLS), "pv_completeness_per_day")
display(per_day)

# COMMAND ----------

low_days = save(per_day.where(" OR ".join(f"{c}_pct < 99" for c in PV_COLS) + " OR timestamp_pct < 99"), "pv_low_days")
note("days_below_99pct", f"{low_days.count()} of {per_day.count()}")
display(low_days)

# COMMAND ----------

# MAGIC %md ## 4. Nulls by hour of day
# MAGIC Day/night split without assuming a timezone. Daytime nulls matter most because Silver turns them into 0.

# COMMAND ----------

display(save(null_by_hour(pv, TS, PV_COLS), "pv_null_by_hour"))

# COMMAND ----------

# MAGIC %md ## 5. Validity
# MAGIC The capacity bound is the pipeline's 1700 MW plausibility limit. Use the monthly maxima to estimate nameplate capacity (an open question).

# COMMAND ----------

display(save(column_stats(pv, PV_COLS), "pv_column_stats"))

pv_rules = {}
for c in PV_COLS:
    pv_rules[f"{c}_negative"] = (f"{c} < 0", [c])
    pv_rules[f"{c}_above_{PV_PLAUSIBLE_MAX_MW}MW"] = (f"{c} > {PV_PLAUSIBLE_MAX_MW}", [c])
display(save(rule_counts(pv, pv_rules), "pv_rules"))

# COMMAND ----------

display(pv.where(" OR ".join(f"{c} < 0 OR {c} > {PV_PLAUSIBLE_MAX_MW}" for c in PV_COLS)).orderBy(TS).limit(200))

# COMMAND ----------

monthly = pv.groupBy(F.date_trunc("month", TS).alias("month")).agg(
    *[F.max(c).alias(f"{c}_max") for c in PV_COLS],
    *[F.percentile_approx(c, 0.995).alias(f"{c}_p995") for c in PV_COLS],
).orderBy("month")
display(save(monthly, "pv_monthly_max"))

# COMMAND ----------

# MAGIC %md ## 6. Stuck / flat-lined values
# MAGIC Runs of identical non-zero values (zeros are expected at night).

# COMMAND ----------

min_rows = int(dbutils.widgets.get("stuck_min_minutes"))
stuck = reduce(DataFrame.unionByName, [stuck_runs(pv, TS, c, min_rows) for c in PV_COLS])
note(f"stuck_runs_ge_{min_rows}min", stuck.count())
display(save(stuck, "pv_stuck_runs").limit(200))

# COMMAND ----------

# MAGIC %md ## 7. Timezone check
# MAGIC Abu Dhabi solar noon is about 08:20 UTC, or 12:20 Asia/Dubai. The hour with peak average PV shows which convention `DateTime` uses.
# MAGIC Preprocessing assumes UTC and adds 4 h.

# COMMAND ----------

prof = hourly_profile(pv, TS, PV_COLS)
display(save(prof, "pv_hourly_profile"))
for c in PV_COLS:
    peak = prof.orderBy(F.desc(f"avg_{c}")).first()["hour"]
    note(f"{c}_peak_hour", f"{peak:02d}:00 -> looks like {guess_timezone(peak)}")