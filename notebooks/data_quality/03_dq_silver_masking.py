# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · DQ — Silver masking cross-check
# MAGIC Bronze→Silver runs `coalesce(PV_MW, 0)` and forward-fills Solcast, so Silver looks complete even where Bronze has holes (bug C8).
# MAGIC This notebook measures how many **daytime `PV_MW = 0` rows** in `silver.uc2_solar_5min` are really missing Bronze telemetry.
# MAGIC
# MAGIC ⚠️ Site swap: Silver `PV1` ↔ Bronze `PV2_MW`, Silver `PV2` ↔ Bronze `PV1_MW`. Step 1 tests this, along with the 5-min bucket convention.
# MAGIC
# MAGIC Check the Silver column names in the widgets against the printed schema first.

# COMMAND ----------

dbutils.widgets.text("scratch_schema", "", "Scratch schema (optional, catalog.schema)")
dbutils.widgets.text("silver_ts_col", "DateTime", "Silver timestamp column")
dbutils.widgets.text("silver_site_col", "site", "Silver site column")
dbutils.widgets.text("silver_pv_col", "PV_MW", "Silver PV column")

# COMMAND ----------

# MAGIC %run ./00_dq_utils

# COMMAND ----------

notes_init("silver")
TS_S = dbutils.widgets.get("silver_ts_col")
SITE_S = dbutils.widgets.get("silver_site_col")
PV_S = dbutils.widgets.get("silver_pv_col")

silver_raw = spark.table(SILVER_5MIN)
silver_raw.printSchema()

silver = silver_raw.select(
    F.col(SITE_S).alias("site"),
    F.col(TS_S).cast("timestamp").alias("ts"),
    F.col(PV_S).alias("silver_pv"),
    F.col("clearsky_ghi"),
).withColumn("is_day", F.col("clearsky_ghi") > DAYTIME_CLEARSKY_GHI)

display(save(time_range(silver, "ts", ["site"]), "silver_range"))
note("duplicate_site_ts", duplicate_timestamps(silver, ["site", "ts"]).count())
note("null_pv_in_silver", silver.where("silver_pv IS NULL").count())

# COMMAND ----------

# MAGIC %md ## Bronze aggregated to 5-min buckets

# COMMAND ----------

pv = spark.table(PV_TABLE).select(F.col("DateTime").cast("timestamp").alias("ts"), *PV_COLS)
bucket_start = F.timestamp_seconds(F.floor(F.unix_timestamp("ts") / 300) * 300)

def bronze_5min(mapping: dict) -> DataFrame:
    """mapping: silver site label -> bronze column."""
    pairs = F.explode(F.array(*[F.struct(F.lit(s).alias("site"), F.col(c).alias("pv")) for s, c in mapping.items()]))
    return (
        pv.withColumn("bucket_start", bucket_start)
        .select("bucket_start", pairs.alias("x")).select("bucket_start", "x.site", "x.pv")
        .groupBy("bucket_start", "site")
        .agg(F.count("*").alias("bronze_rows"), F.count("pv").alias("bronze_nonnull"), F.avg("pv").alias("bronze_avg_mw"))
    )

SWAPPED = SILVER_SITE_TO_BRONZE_COL
UNSWAPPED = {"PV1": "PV1_MW", "PV2": "PV2_MW"}

# COMMAND ----------

# MAGIC %md ## 1. Confirm the site swap and bucket convention
# MAGIC Is the Silver timestamp the bucket start or the bucket end? Correlation with Bronze should be close to 1 for the right combination.

# COMMAND ----------

results = []
for map_name, mapping in [("swapped", SWAPPED), ("unswapped", UNSWAPPED)]:
    b = bronze_5min(mapping)
    for conv, shift in [("bucket_start", 0), ("bucket_end", 300)]:
        bb = b.withColumn("ts", F.timestamp_seconds(F.unix_timestamp("bucket_start") + shift))
        r = silver.join(bb, ["site", "ts"], "left").groupBy("site").agg(
            F.round(100 * F.avg(F.col("bronze_rows").isNotNull().cast("int")), 2).alias("key_match_pct"),
            F.round(F.corr("silver_pv", "bronze_avg_mw"), 4).alias("corr"),
        )
        results.append(r.withColumn("mapping", F.lit(map_name)).withColumn("convention", F.lit(conv)))
alignment = reduce(DataFrame.unionByName, results).orderBy("site", F.desc("corr"))
display(save(alignment, "silver_bronze_alignment"))

# COMMAND ----------

best = alignment.where("mapping = 'swapped'").orderBy(F.desc("corr")).first()
CONVENTION = best["convention"]
note("convention_used", f"swapped mapping, {CONVENTION} (corr={best['corr']})")
if alignment.orderBy(F.desc("corr")).first()["mapping"] != "swapped":
    note("WARNING", "unswapped mapping correlates better than swapped")

shift = 0 if CONVENTION == "bucket_start" else 300
bronze = bronze_5min(SWAPPED).withColumn("ts", F.timestamp_seconds(F.unix_timestamp("bucket_start") + shift))

# COMMAND ----------

# MAGIC %md ## 2. Classify daytime Silver zeros

# COMMAND ----------

j = silver.join(bronze, ["site", "ts"], "left").withColumn(
    "bronze_state",
    F.when(F.col("bronze_rows").isNull(), "no_bronze_rows")
    .when(F.col("bronze_nonnull") == 0, "bronze_all_null")
    .when(F.col("bronze_avg_mw") < DROPOUT_PV_MW, "bronze_near_zero")
    .otherwise("bronze_has_power"),
)

day_zero = j.where("is_day AND silver_pv = 0")
breakdown = day_zero.groupBy("site", "bronze_state").count().orderBy("site", "bronze_state")
display(save(breakdown, "silver_day_zero_breakdown"))

# COMMAND ----------

summary = j.where("is_day").groupBy("site").agg(
    F.count("*").alias("daytime_rows"),
    F.sum((F.col("silver_pv") == 0).cast("int")).alias("daytime_zero_rows"),
    F.sum(((F.col("silver_pv") == 0) & F.col("bronze_state").isin("no_bronze_rows", "bronze_all_null")).cast("int")).alias("masked_missing"),
    F.sum(((F.col("silver_pv") == 0) & (F.col("bronze_state") == "bronze_has_power")).cast("int")).alias("zero_but_bronze_has_power"),
).withColumn("masked_pct_of_daytime", F.round(100 * F.col("masked_missing") / F.col("daytime_rows"), 3))
display(save(summary, "silver_masking_summary"))
print("masked_missing = daytime Silver rows that are 0 only because Bronze had no data.")
print("zero_but_bronze_has_power should be ~0. If not, suspect timezone or bucketing misalignment.")

# COMMAND ----------

monthly = day_zero.groupBy("site", F.date_trunc("month", "ts").alias("month")).pivot(
    "bronze_state", ["no_bronze_rows", "bronze_all_null", "bronze_near_zero", "bronze_has_power"]
).count().fillna(0).orderBy("site", "month")
display(save(monthly, "silver_day_zero_monthly"))

# COMMAND ----------

# MAGIC %md ## 3. Longest masked outages
# MAGIC Contiguous runs of masked daytime rows. Preprocessing interpolates only 6 steps (30 min); anything longer becomes a fake zero (bug C9).

# COMMAND ----------

w = Window.partitionBy("site").orderBy("ts")
masked = j.where("is_day AND silver_pv = 0 AND bronze_state IN ('no_bronze_rows', 'bronze_all_null')").select("site", "ts")
runs = (
    masked.withColumn("new_run", F.coalesce((F.unix_timestamp("ts") - F.unix_timestamp(F.lag("ts").over(w)) > 300).cast("int"), F.lit(1)))
    .withColumn("run_id", F.sum("new_run").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    .groupBy("site", "run_id").agg(F.min("ts").alias("start"), F.max("ts").alias("end"), F.count("*").alias("steps_5min"))
    .drop("run_id")
)
display(save(runs.groupBy("site").agg(
    F.count("*").alias("runs"),
    F.sum((F.col("steps_5min") > 6).cast("int")).alias("runs_longer_than_30min"),
    F.sum(F.when(F.col("steps_5min") > 6, F.col("steps_5min"))).alias("steps_in_long_runs"),
), "silver_masked_runs_summary"))
display(save(runs.orderBy(F.desc("steps_5min")), "silver_masked_runs").limit(100))

# COMMAND ----------

# MAGIC %md ## 4. Preprocessed table: daytime zeros (counts only)
# MAGIC Preprocessing shifts timestamps by +4 h, so this compares totals rather than joining rows.

# COMMAND ----------

try:
    pre = spark.table(PREPROCESSED_5MIN)
    pre.printSchema()
    pre_counts = pre.where(F.col("clearsky_ghi") > DAYTIME_CLEARSKY_GHI).groupBy(F.col(SITE_S).alias("site")).agg(
        F.count("*").alias("pre_daytime_rows"), F.sum((F.col(PV_S) == 0).cast("int")).alias("pre_daytime_zero_rows")
    )
    display(save(summary.select("site", "daytime_rows", "daytime_zero_rows").join(pre_counts, "site", "full"), "silver_vs_preprocessed_zeros"))
except Exception as e:
    note("preprocessed_check_skipped", e)
