# Databricks notebook source
# MAGIC %md
# MAGIC # FE 02 · Reuniwatt forecasts → `fe_reuniwatt_forecasts`
# MAGIC One clean row per **site × horizon × run (`reference_time`) × target (`period_end`)**, irradiance and power side by side.
# MAGIC
# MAGIC | `source` | Reads | History |
# MAGIC |---|---|---|
# MAGIC | `silver` (works now) | `silver.fact_solar_irradiance` + `fact_solar_power_forecast` | Patchy: mainly 21 Sep 2026 onwards |
# MAGIC | `bronze` (pending) | `bronze.solar_forecast_raw` (raw JSON) | 23 Jan 2024 onwards, all products. Parser to be written once `01_fe_explore_bronze_json` output is shared |
# MAGIC
# MAGIC Cleaning applied:
# MAGIC - Latest ingestion per key (re-fetched runs overwrite older copies).
# MAGIC - `granularity_min` is ignored: all products are 5-minute, and some Silver rows carry a wrong label.
# MAGIC - Rows with lead ≤ 0 are dropped, which removes the legacy power rows whose issue time was lost.
# MAGIC - Power is converted from GW to MW if needed. `gti_poa` is the sunlight on the panels: PV2 `gti`, PV1 the mean of the east and west GTI.

# COMMAND ----------

dbutils.widgets.text("output_schema", "ewec_dev_powerops.scratch_yash", "Output schema (catalog.schema)")
dbutils.widgets.dropdown("source", "silver", ["silver", "bronze"], "Reuniwatt source")

# COMMAND ----------

# MAGIC %run ./00_fe_common

# COMMAND ----------

KEY = ["site", "horizon", "reference_time", "period_end"]


def prep(df: DataFrame) -> DataFrame:
    for c in ("reference_time", "period_end", "_ingested_at"):
        df = df.withColumn(c, F.col(c).cast("timestamp"))
    return df.withColumn("site", F.lower(F.trim("site"))).withColumn("horizon", F.lower(F.trim("horizon")))


def latest_per_key(df: DataFrame) -> DataFrame:
    w = Window.partitionBy(*KEY).orderBy(F.desc("_ingested_at"))
    return df.withColumn("_rk", F.row_number().over(w)).where("_rk = 1").drop("_rk")


def from_silver() -> DataFrame:
    irr = latest_per_key(prep(spark.table(REUNI_SILVER_IRR))).select(
        *KEY,
        F.col("ghi_wm2").alias("ghi"), F.col("dni_wm2").alias("dni"), F.col("dhi_wm2").alias("dhi"), F.col("bhi_wm2").alias("bhi"),
        F.col("gti_wm2").alias("gti"), F.col("gti_east_wm2").alias("gti_east"), F.col("gti_west_wm2").alias("gti_west"),
        F.col("ambient_temp_c").alias("temp_c"), "dewpoint_c", "humidity_pct", "wind_speed", "wind_dir",
        F.col("cloud_opacity_pct").alias("cloud_opacity"), "pressure_hpa", "precip_rate_mmh",
        F.col("_source").alias("source_irr"),
    )
    pwr = latest_per_key(prep(spark.table(REUNI_SILVER_PWR)))
    max_power = pwr.agg(F.max("power_mw")).first()[0] or 0
    scale = 1000.0 if max_power < 10 else 1.0
    print(f"Max power_mw in Silver = {max_power} -> scaling by {scale} to get MW")
    pwr = pwr.select(
        *KEY,
        (F.col("power_mw") * scale).alias("power_mw"),
        (F.col("power_p10_mw") * scale).alias("power_p10_mw"),
        (F.col("power_p90_mw") * scale).alias("power_p90_mw"),
        F.col("_source").alias("source_power"),
    )
    return irr.join(pwr, KEY, "full")


def from_bronze() -> DataFrame:
    raise NotImplementedError(
        "Bronze parser not written yet: run 01_fe_explore_bronze_json, push results/bronze_json_layout.txt, and ask Claude to add it."
    )


SOURCE = dbutils.widgets.get("source")
fc = from_silver() if SOURCE == "silver" else from_bronze()

fc = (
    fc.withColumn("gti_poa", F.when(F.col("site") == "pv1", (F.col("gti_east") + F.col("gti_west")) / 2).otherwise(F.col("gti")))
    .withColumn("lead_min", (F.unix_timestamp("period_end") - F.unix_timestamp("reference_time")) / 60)
    .where("lead_min > 0")
    .withColumn("source", F.lit(SOURCE))
)

# COMMAND ----------

fc = write(fc, "fe_reuniwatt_forecasts", partition_by=("horizon",))

# COMMAND ----------

# MAGIC %md ## Coverage check
# MAGIC Days with at least one run, per month. Expect near-full months for every site × horizon once the Bronze source is used.

# COMMAND ----------

display(
    fc.groupBy(F.date_trunc("month", "reference_time").alias("month"), "site", "horizon")
    .agg(F.countDistinct(F.to_date("reference_time")).alias("days"), F.countDistinct("reference_time").alias("runs"),
         F.round(F.avg(F.col("power_mw").isNotNull().cast("int")) * 100, 1).alias("pct_with_power"))
    .orderBy("month", "site", "horizon")
)
