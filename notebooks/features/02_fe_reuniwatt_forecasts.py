# Databricks notebook source
# MAGIC %md
# MAGIC # FE 02 · Reuniwatt forecasts → `fe_reuniwatt_forecasts`
# MAGIC One clean row per **site × horizon × run (`reference_time`) × target (`period_end`)**, irradiance, weather and power side by side.
# MAGIC
# MAGIC | `source` | Reads | History |
# MAGIC |---|---|---|
# MAGIC | `bronze` (default) | `bronze.solar_forecast_raw` (raw JSON) | Jan 2024 → now, all 6 products |
# MAGIC | `silver` | `silver.fact_solar_irradiance` + `fact_solar_power_forecast` | Only what Silver has processed |
# MAGIC
# MAGIC **Bronze JSON layout:** `{"PV1": {"ghi": {"2026-10-02T02:20:00+00:00": 0.39, …}, "dni": {…}, …}}`, i.e. site → variable → (UTC target time → value).
# MAGIC Day-ahead has 18 variables (irradiance, weather, power); intraday and week-ahead have irradiance and power only. PV1 has `gti_90` / `gti_270` (east / west panels), PV2 has `gti`.
# MAGIC
# MAGIC **Run thinning (Bronze):** runs arrive every 15 min. Unpacking every week-ahead run would mean billions of rows, so we keep:
# MAGIC every intraday run (15 min), one day-ahead run per hour and one week-ahead run every 3 hours. That is enough for the feature schedules in 04 (max run ages 60 / 360 / 1440 min).
# MAGIC
# MAGIC **Cleaning:**
# MAGIC - `migration_legacy` rows are dropped (flat JSON with no real issue time).
# MAGIC - Latest ingestion per run.
# MAGIC - Lead ≤ 0 dropped.
# MAGIC - Power converted to MW (Bronze sends kW, Silver GW; detected from the values).
# MAGIC - `gti_poa` = sunlight on the panels (PV2 `gti`; PV1 mean of east and west).

# COMMAND ----------

dbutils.widgets.text("output_schema", "ewec_dev_powerops.scratch_yash", "Output schema (catalog.schema)")
dbutils.widgets.dropdown("source", "bronze", ["bronze", "silver"], "Reuniwatt source")
dbutils.widgets.text("start_date", "2024-01-01", "First run date (UTC)")
dbutils.widgets.text("end_date", "", "Last run date (blank = latest)")

# COMMAND ----------

# MAGIC %run ./00_fe_common

# COMMAND ----------

KEY = ["site", "horizon", "reference_time", "period_end"]
SOURCE = dbutils.widgets.get("source")
START = dbutils.widgets.get("start_date").strip()
END = dbutils.widgets.get("end_date").strip()


def in_range(df: DataFrame) -> DataFrame:
    df = df.where(F.to_date("reference_time") >= F.lit(START))
    return df.where(F.to_date("reference_time") <= F.lit(END)) if END else df


def prep(df: DataFrame) -> DataFrame:
    for c in ("reference_time", "period_end", "_ingested_at"):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("timestamp"))
    return df.withColumn("site", F.lower(F.trim("site"))).withColumn("horizon", F.lower(F.trim("horizon")))


def latest_per(df: DataFrame, keys) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.desc("_ingested_at"))
    return df.withColumn("_rk", F.row_number().over(w)).where("_rk = 1").drop("_rk")

# COMMAND ----------

# MAGIC %md ## Bronze reader

# COMMAND ----------

POWER_COLS = REUNI_POWER_COLS


def from_bronze() -> DataFrame:
    # Shared reader in 00_fe_common (same code as used by the comparison notebook)
    return reuniwatt_from_bronze(START, END)

# COMMAND ----------

# MAGIC %md ## Silver reader

# COMMAND ----------

def from_silver() -> DataFrame:
    irr = in_range(latest_per(prep(spark.table(REUNI_SILVER_IRR)), KEY)).select(
        *KEY,
        F.col("ghi_wm2").alias("ghi"), F.col("dni_wm2").alias("dni"), F.col("dhi_wm2").alias("dhi"), F.col("bhi_wm2").alias("bhi"),
        F.col("gti_wm2").alias("gti"), F.col("gti_east_wm2").alias("gti_east"), F.col("gti_west_wm2").alias("gti_west"),
        F.col("ambient_temp_c").alias("temp_c"), "dewpoint_c", "humidity_pct", "wind_speed", "wind_dir",
        F.col("cloud_opacity_pct").alias("cloud_opacity"), "pressure_hpa", "precip_rate_mmh",
        F.col("clearness_idx_pct").alias("clearness_index"), F.col("_source").alias("raw_source"),
    )
    pwr = in_range(latest_per(prep(spark.table(REUNI_SILVER_PWR)), KEY))
    max_power = pwr.agg(F.max("power_mw")).first()[0] or 0
    scale = power_scale_to_mw(max_power)
    pwr = pwr.select(*KEY, *[(F.col(c) * scale).alias(c) for c in POWER_COLS])
    return irr.join(pwr, KEY, "full")

# COMMAND ----------

# MAGIC %md ## Build and write

# COMMAND ----------

fc = from_bronze() if SOURCE == "bronze" else from_silver()
fc = (
    fc.withColumn("gti_poa", F.when(F.col("site") == "pv1", (F.col("gti_east") + F.col("gti_west")) / 2).otherwise(F.col("gti")))
    .withColumn("lead_min", (F.unix_timestamp("period_end") - F.unix_timestamp("reference_time")) / 60)
    .where("lead_min > 0")
    .withColumn("source", F.lit(SOURCE))
)
fc = write(fc, "fe_reuniwatt_forecasts", partition_by=("horizon",))

# COMMAND ----------

# MAGIC %md ## Checks
# MAGIC **Coverage:** days and runs per month for each site × horizon. Expect full months from Jan 2024, and about 96 / 24 / 8 runs a day for intraday / day-ahead / week-ahead.

# COMMAND ----------

display(
    fc.groupBy(F.date_trunc("month", "reference_time").alias("month"), "site", "horizon")
    .agg(F.countDistinct(F.to_date("reference_time")).alias("days"), F.countDistinct("reference_time").alias("runs"),
         F.round(F.max("power_mw")).alias("max_power_mw"), F.round(F.max("ghi")).alias("max_ghi"))
    .orderBy("month", "site", "horizon")
)

# COMMAND ----------

# MAGIC %md **Is the backfill the forecast as originally issued?** Backfill and live runs overlap on 1 Oct 2026. For runs issued in the same 15 minutes, the values for the same target should be (almost) identical.
# MAGIC A large difference means the backfill was re-computed later, which would leak information into training.

# COMMAND ----------

if SOURCE == "bronze":
    slot = F.timestamp_seconds(F.floor(F.unix_timestamp("reference_time") / 900) * 900)
    x = fc.withColumn("slot", slot)
    k = ["site", "horizon", "slot", "period_end"]
    bf = x.where("raw_source = 'api_historical_backfill'").select(*k, F.col("ghi").alias("ghi_bf"), F.col("power_mw").alias("pw_bf"))
    lv = x.where("raw_source = 'api_incremental'").select(*k, F.col("ghi").alias("ghi_live"), F.col("power_mw").alias("pw_live"))
    display(
        bf.join(lv, k).groupBy("site", "horizon").agg(
            F.countDistinct("slot").alias("overlapping_runs"),
            F.count("*").alias("matched_values"),
            F.round(F.corr("ghi_bf", "ghi_live"), 4).alias("ghi_corr"),
            F.round(F.avg(F.abs(F.col("ghi_bf") - F.col("ghi_live"))), 2).alias("ghi_mean_abs_diff"),
            F.round(F.avg(F.abs(F.col("pw_bf") - F.col("pw_live"))), 2).alias("power_mean_abs_diff_mw"),
        ).orderBy("site", "horizon")
    )
