# Databricks notebook source
# MAGIC %md
# MAGIC # FE 00 · Common config and helpers
# MAGIC Loaded by the other feature notebooks with `%run ./00_fe_common`. Each notebook defines an `output_schema` widget before loading this.
# MAGIC
# MAGIC Conventions used everywhere:
# MAGIC - **All timestamps are UTC** and label the **end** of a 5-minute period (`period_end`), the same as Reuniwatt. Gulf-time columns are added only for calendar features.
# MAGIC - **Sites use Reuniwatt labels:** `pv1` = smaller plant (~950 MW), `pv2` = larger plant (~1,650 MW). They do **not** follow the Bronze column names, which swapped around March 2026, nor the Silver labels, which are swapped on purpose.
# MAGIC - Shared tables are only read; outputs go to `output_schema`.

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# All time arithmetic below assumes the session clock is UTC (Databricks default); make it explicit.
spark.conf.set("spark.sql.session.timeZone", "UTC")

PV_BRONZE = "ewec_dev_powerops.bronze.power_contango_minute_pv"
REUNI_BRONZE = "ewec_dev_reuniwatt.bronze.solar_forecast_raw"
REUNI_SILVER_IRR = "ewec_dev_reuniwatt.silver.fact_solar_irradiance"
REUNI_SILVER_PWR = "ewec_dev_reuniwatt.silver.fact_solar_power_forecast"

STEP_MIN = 5
GULF_OFFSET_H = 4  # Gulf Standard Time = UTC+4, no daylight saving

# Capacities are estimated from telemetry peaks (DQ run 2026-10-08). Replace with nameplate / export limits once confirmed.
# Coordinates are approximate (Abu Dhabi emirate). Replace with plant coordinates once confirmed (dim_pv_site is empty).
SITES = {
    "pv1": {"capacity_mw": 950.0, "lat": 24.45, "lon": 54.6, "bronze_col_before_swap": "PV2_MW", "bronze_col_after_swap": "PV1_MW"},
    "pv2": {"capacity_mw": 1650.0, "lat": 24.45, "lon": 54.6, "bronze_col_before_swap": "PV1_MW", "bronze_col_after_swap": "PV2_MW"},
}

PV_SENTINEL_ABS_MW = 5000  # |value| above this is corrupt (e.g. the ±9.2e18 values in PV1, Apr–May 2023)
NIGHT_NEG_FLOOR_MW = -10   # small negatives are plant self-consumption at night -> 0; below this -> invalid

# COMMAND ----------

def out_table(name: str) -> str:
    schema = dbutils.widgets.get("output_schema").strip()
    if schema.count(".") != 1:
        raise ValueError("Set output_schema as catalog.schema")
    if schema.split(".")[-1].lower() in {"bronze", "silver", "gold"}:
        raise ValueError(f"Refusing to write into shared schema '{schema}'")
    return f"{schema}.{name}"


def write(df: DataFrame, name: str, partition_by=()) -> DataFrame:
    full = out_table(name)
    w = df.write.mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.saveAsTable(full)
    t = spark.table(full)
    print(f"wrote {full}: {t.count():,} rows")
    return t


def shift_minutes(col, minutes):
    c = F.col(col) if isinstance(col, str) else col
    return F.timestamp_seconds(F.unix_timestamp(c) + F.lit(minutes) * 60)


def utc_to_gulf(col):
    return shift_minutes(col, GULF_OFFSET_H * 60)


def gulf_to_utc(col):
    return shift_minutes(col, -GULF_OFFSET_H * 60)


def ceil_to(col, minutes: int):
    """Label a timestamp with the end of its `minutes`-wide period."""
    c = F.col(col) if isinstance(col, str) else col
    return F.timestamp_seconds(F.ceil(F.unix_timestamp(c) / (minutes * 60)) * (minutes * 60))


def site_value(key: str):
    """Column with SITES[<row's site>][key]."""
    expr = None
    for s, cfg in SITES.items():
        cond, val = F.col("site") == s, F.lit(cfg[key])
        expr = F.when(cond, val) if expr is None else expr.when(cond, val)
    return expr

# COMMAND ----------

# MAGIC %md ## Sun position and clear-sky irradiance (pure Spark)
# MAGIC NOAA solar-position approximation (well under 1° error), Kasten–Young air mass, Haurwitz clear-sky GHI.
# MAGIC Haurwitz needs no aerosol input. It's a reasonable baseline for a desert site but tends to over-estimate on dusty days; the clear-sky *index* and the model absorb that.

# COMMAND ----------

def add_solar(df: DataFrame, ts_utc: str = "period_end", prefix: str = "", period_min: int = STEP_MIN) -> DataFrame:
    """Sun position / clear-sky at the middle of the period ending at `ts_utc`. Needs a `site` column."""
    pi = 3.141592653589793
    t = F.timestamp_seconds(F.unix_timestamp(ts_utc) - period_min * 30)
    doy = F.dayofyear(t)
    hr = F.hour(t) + F.minute(t) / 60 + F.second(t) / 3600
    g = 2 * pi / 365 * (doy - 1 + (hr - 12) / 24)
    decl = (0.006918 - 0.399912 * F.cos(g) + 0.070257 * F.sin(g) - 0.006758 * F.cos(2 * g)
            + 0.000907 * F.sin(2 * g) - 0.002697 * F.cos(3 * g) + 0.00148 * F.sin(3 * g))
    eqtime = 229.18 * (0.000075 + 0.001868 * F.cos(g) - 0.032077 * F.sin(g) - 0.014615 * F.cos(2 * g) - 0.040849 * F.sin(2 * g))
    lat = F.radians(site_value("lat"))
    ha = F.radians((hr * 60 + eqtime + 4 * site_value("lon")) / 4 - 180)
    cosz = F.least(F.greatest(F.sin(lat) * F.sin(decl) + F.cos(lat) * F.cos(decl) * F.cos(ha), F.lit(-1.0)), F.lit(1.0))
    zen = F.degrees(F.acos(cosz))
    p = prefix
    return (
        df.withColumn(f"{p}sun_zenith", zen)
        .withColumn(f"{p}sun_elevation", 90 - zen)
        .withColumn(f"{p}cos_zenith", F.greatest(cosz, F.lit(0.0)))
        .withColumn(f"{p}air_mass", F.when(zen < 89, 1 / (cosz + 0.50572 * F.pow(96.07995 - zen, -1.6364))))
        .withColumn(f"{p}etr_horizontal", 1361.0 * (1 + 0.033 * F.cos(2 * pi * doy / 365)) * F.greatest(cosz, F.lit(0.0)))
        .withColumn(f"{p}clearsky_ghi", F.when(cosz > 0.01, 1098.0 * cosz * F.exp(-0.057 / cosz)).otherwise(F.lit(0.0)))
        .withColumn(f"{p}is_day", zen < 90)
    )


def add_calendar(df: DataFrame, ts_utc: str, prefix: str) -> DataFrame:
    """Cyclical calendar features in Gulf time."""
    pi = 3.141592653589793
    g = utc_to_gulf(ts_utc)
    hr = F.hour(g) + F.minute(g) / 60
    doy = F.dayofyear(g)
    return (
        df.withColumn(f"{prefix}hour_sin", F.sin(2 * pi * hr / 24)).withColumn(f"{prefix}hour_cos", F.cos(2 * pi * hr / 24))
        .withColumn(f"{prefix}doy_sin", F.sin(2 * pi * doy / 365.25)).withColumn(f"{prefix}doy_cos", F.cos(2 * pi * doy / 365.25))
    )

# COMMAND ----------

# MAGIC %md ## As-of join
# MAGIC For each `(site, origin)`, find the latest forecast run that was **available** at the origin: `reference_time + latency ≤ origin`.
# MAGIC Using only such runs is what prevents look-ahead leakage in training and backtests.

# COMMAND ----------

def asof_runs(origins: DataFrame, runs: DataFrame, latency_min: int) -> DataFrame:
    """origins: (site, origin). runs: distinct (site, reference_time). Returns (site, origin, run_time, run_age_min)."""
    r = runs.select(
        "site", shift_minutes("reference_time", latency_min).alias("t"), F.col("reference_time").alias("run_time"), F.lit(1).alias("is_run")
    )
    o = origins.select("site", F.col("origin").alias("t"), F.lit(None).cast("timestamp").alias("run_time"), F.lit(0).alias("is_run"))
    # Runs sort before origins at the same instant, so a run available exactly at the origin is used.
    w = Window.partitionBy("site").orderBy("t", F.desc("is_run")).rowsBetween(Window.unboundedPreceding, 0)
    return (
        r.unionByName(o)
        .withColumn("run_time", F.last("run_time", ignorenulls=True).over(w))
        .where("is_run = 0")
        .select("site", F.col("t").alias("origin"), "run_time")
        .withColumn("run_age_min", (F.unix_timestamp("origin") - F.unix_timestamp("run_time")) / 60)
    )
