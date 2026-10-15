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


def power_scale_to_mw(max_value) -> float:
    """Factor that converts Reuniwatt power to MW, judged from the largest value seen.
    Plant peaks are ~950 / ~1,650 MW, so: > 10,000 means kW (Bronze), < 10 means GW (Silver), otherwise MW."""
    mx = float(max_value or 0)
    factor, unit = (0.001, "kW") if mx > 10_000 else (1000.0, "GW") if mx < 10 else (1.0, "MW")
    print(f"Max raw power = {mx:,.3f} -> looks like {unit}; multiplying by {factor} to get MW")
    return factor


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

# COMMAND ----------

# MAGIC %md ## PV telemetry: cleaning and column swap
# MAGIC Labels follow the plant sensors and `Pv2_AVL`: **PV1 = small plant** (east/west panels), **PV2 = large plant**.
# MAGIC Before ~March 2026 the Bronze MW columns were mislabelled (`PV1_MW` held the large plant); afterwards they are correct. Sensor columns were never swapped.
# MAGIC **Targets (agreed 2026-10-14):** PV1 = actual MW; PV2 = `Pv2_AVL` (available power).

# COMMAND ----------

from datetime import date, timedelta


def clean_pv_mw(c: str):
    """Corrupt (|v| > 5,000) and strongly negative values -> null; small negatives (night self-consumption) -> 0."""
    v = F.col(c).cast("double")
    return (F.when(F.abs(v) > PV_SENTINEL_ABS_MW, F.lit(None).cast("double"))
            .when(v < NIGHT_NEG_FLOOR_MW, F.lit(None).cast("double"))
            .when(v < 0, F.lit(0.0)).otherwise(v))


def detect_pv_swap_date():
    """Day the Bronze MW columns switched (7-day majority of which column has the larger daily peak). None if never."""
    daily = (
        spark.table(PV_BRONZE)
        .select(F.to_date(F.col("DateTime").cast("timestamp")).alias("day"), clean_pv_mw("PV1_MW").alias("a"), clean_pv_mw("PV2_MW").alias("b"))
        .groupBy("day").agg(F.max("a").alias("pv1_peak"), F.max("b").alias("pv2_peak"))
        .withColumn("pv2_bigger", (F.col("pv2_peak") > F.col("pv1_peak")).cast("int"))
        .withColumn("pv2_bigger_7d", F.avg("pv2_bigger").over(Window.orderBy("day").rowsBetween(-3, 3)))
    )
    last_pv1_big = daily.where("pv2_bigger_7d < 0.5").agg(F.max("day")).first()[0]
    last_day = daily.agg(F.max("day")).first()[0]
    swap = (last_pv1_big + timedelta(days=1)) if last_pv1_big and last_pv1_big < last_day else None
    print("Bronze MW column swap date:", swap)
    return swap


def site_mw(site: str, swap_day, ts_col: str = "ts_gulf"):
    """Cleaned actual MW for a site, taking the Bronze column swap into account."""
    cfg = SITES[site]
    swap_lit = F.lit(swap_day or date(9999, 12, 31))
    return F.when(F.to_date(ts_col) < swap_lit, clean_pv_mw(cfg["bronze_col_before_swap"])).otherwise(clean_pv_mw(cfg["bronze_col_after_swap"]))

# COMMAND ----------

# MAGIC %md ## Reuniwatt Bronze reader
# MAGIC `_raw` JSON = site → variable → (UTC target time → value). Unpacked by sorting each variable's (time, value) pairs and zipping them (no slow map look-ups).
# MAGIC Keeps every intraday run, hourly day-ahead runs and 3-hourly week-ahead runs; drops `migration_legacy`; power converted to MW.

# COMMAND ----------

REUNI_JSON_SCHEMA = "MAP<STRING, MAP<STRING, MAP<STRING, DOUBLE>>>"
REUNI_VARS = {
    "ghi": "ghi", "dni": "dni", "dhi": "dhi", "bhi": "bhi",
    "gti": "gti", "gti_90": "gti_east", "gti_270": "gti_west",
    "temperature": "temp_c", "dewpoint": "dewpoint_c", "relative_humidity": "humidity_pct",
    "wind_speed": "wind_speed", "wind_direction": "wind_dir", "cloud_opacity": "cloud_opacity",
    "pressure": "pressure_hpa", "precip": "precip_rate_mmh", "clearness_index": "clearness_index",
    "power": "power_mw", "power10": "power_p10_mw", "power90": "power_p90_mw",
}
REUNI_POWER_COLS = ["power_mw", "power_p10_mw", "power_p90_mw"]
REUNI_KEEP_RUN_EVERY_MIN = {"intraday": 15, "dayahead": 60, "weekahead": 180}


def reuniwatt_from_bronze(start: str, end: str = "", horizons=None, max_lead_min=None) -> DataFrame:
    """One row per site × horizon × run × 5-min target, runs issued between start and end (UTC dates).
    horizons: optional list to keep (e.g. ["dayahead", "weekahead"]); max_lead_min: optional cap on lead time."""
    b = (
        spark.table(REUNI_BRONZE).where(F.col("_source") != "migration_legacy")
        .withColumn("reference_time", F.col("reference_time").cast("timestamp"))
        .withColumn("_ingested_at", F.col("_ingested_at").cast("timestamp"))
        .withColumn("site", F.lower(F.trim("site"))).withColumn("horizon", F.lower(F.trim("horizon")))
        .where(F.to_date("reference_time") >= F.lit(start))
    )
    if end:
        b = b.where(F.to_date("reference_time") <= F.lit(end))
    if horizons:
        b = b.where(F.col("horizon").isin(*horizons))
    every = None
    for h, mins in REUNI_KEEP_RUN_EVERY_MIN.items():
        every = F.when(F.col("horizon") == h, F.lit(mins)) if every is None else every.when(F.col("horizon") == h, F.lit(mins))
    b = b.where(((F.hour("reference_time") * 60 + F.minute("reference_time")) % every) < 15)
    b = b.withColumn("_rk", F.row_number().over(Window.partitionBy("site", "horizon", "reference_time").orderBy(F.desc("_ingested_at")))) \
        .where("_rk = 1").drop("_rk")

    m = F.try_element_at(F.from_json("_raw", REUNI_JSON_SCHEMA), F.upper("site"))
    runs = b.select("site", "horizon", "reference_time", F.col("_source").alias("raw_source"), m.alias("m"))

    def sorted_entries(var):
        return F.array_sort(F.map_entries(F.try_element_at("m", F.lit(var))))

    runs = runs.withColumn("_times", F.transform(sorted_entries("ghi"), lambda e: e["key"])).withColumn("_n", F.size("_times")).where("_n > 0")
    for k, v in REUNI_VARS.items():
        ent = sorted_entries(k)
        runs = runs.withColumn(v, F.when(F.size(ent) == F.col("_n"), F.transform(ent, lambda e: e["value"]))
                               .otherwise(F.array_repeat(F.lit(None).cast("double"), F.col("_n"))))
    z = runs.select("site", "horizon", "reference_time", "raw_source", F.explode(F.arrays_zip("_times", *REUNI_VARS.values())).alias("z"))
    out = z.select("site", "horizon", "reference_time", "raw_source",
                   F.to_timestamp(F.col("z._times"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("period_end"),
                   *[F.col(f"z.{v}").alias(v) for v in REUNI_VARS.values()])

    # Power unit, judged on one recent day-ahead run (Bronze sends kW)
    sample = b.where("horizon = 'dayahead'").orderBy(F.desc("reference_time")).limit(1)
    sample_max = sample.select(F.explode(F.map_values(F.try_element_at(
        F.try_element_at(F.from_json("_raw", REUNI_JSON_SCHEMA), F.upper("site")), F.lit("power")))).alias("v")).agg(F.max("v")).first()[0]
    scale = power_scale_to_mw(sample_max)
    for c in REUNI_POWER_COLS:
        out = out.withColumn(c, F.col(c) * scale)
    return (
        out.withColumn("gti_poa", F.when(F.col("site") == "pv1", (F.col("gti_east") + F.col("gti_west")) / 2).otherwise(F.col("gti")))
        .withColumn("lead_min", (F.unix_timestamp("period_end") - F.unix_timestamp("reference_time")) / 60)
        .where((F.col("lead_min") > 0) & (F.col("lead_min") <= F.lit(max_lead_min if max_lead_min else 10 ** 9)))
    )


def flag_broken_runs(fc: DataFrame) -> DataFrame:
    """Per run (site, horizon, reference_time): broken if, when the sun is high (clear-sky GHI > 600),
    more than half of the forecast GHI values are below 20% of clear-sky. Catches feed glitches like 3 Jul 2026."""
    x = add_solar(fc.select("site", "horizon", "reference_time", "period_end", "ghi"), "period_end")
    high = F.col("clearsky_ghi") > 600
    return (
        x.groupBy("site", "horizon", "reference_time").agg(
            F.sum(high.cast("int")).alias("n_high_sun"),
            F.sum((high & (F.col("ghi") < 0.2 * F.col("clearsky_ghi"))).cast("int")).alias("n_dark"),
        )
        .withColumn("run_broken", (F.col("n_high_sun") >= 6) & (F.col("n_dark") > 0.5 * F.col("n_high_sun")))
    )

# COMMAND ----------

# MAGIC %md ## Plant sensors + target, 5-minute
# MAGIC One row per site × 5-min period (UTC, period end): cleaned plant sensors (`m_*`), target (`target_mw`: PV1 actual MW, PV2 `Pv2_AVL`), actual MW, sun position and label status.

# COMMAND ----------

_NULL = F.lit(None).cast("double")
SENSOR_CLEAN_RULES = {
    "irradiance": (["PV1_GHI", "PV1_ArrayRadiance_east", "PV1_ArrayRadiance_west", "PV2_GHI", "PV2_ArrayRadiance"],
                   lambda v: F.when(v < -20, _NULL).when(v < 0, F.lit(0.0)).when(v > 1500, _NULL).otherwise(v)),
    "temperature": (["PV1_Temp", "PV2_Outdoor_Temp"], lambda v: F.when((v <= 0) | (v > 60), _NULL).otherwise(v)),
    "humidity": (["PV1_HumidityPer", "PV2_HumidityPer"], lambda v: F.when((v <= 0) | (v > 100), _NULL).otherwise(v)),
    "available": (["Pv2_AVL"], lambda v: F.when((v < 0) | (v > 2000), _NULL).otherwise(v)),
}


def build_sensor_target_5min(start: str, end: str = "") -> DataFrame:
    swap_day = detect_pv_swap_date()
    raw = spark.table(PV_BRONZE).withColumn("ts_gulf", F.col("DateTime").cast("timestamp")).where("ts_gulf IS NOT NULL")
    raw = raw.where(F.to_date("ts_gulf") >= F.date_sub(F.lit(start), 1))
    if end:
        raw = raw.where(F.to_date("ts_gulf") <= F.date_add(F.lit(end), 1))
    for _, (cols, rule) in SENSOR_CLEAN_RULES.items():
        for c in cols:
            raw = raw.withColumn(f"{c}__c", rule(F.col(c).cast("double")))

    def c(name):
        return F.col(f"{name}__c")

    per_site = {
        "pv1": dict(m_ghi=c("PV1_GHI"), m_gti_poa=(c("PV1_ArrayRadiance_east") + c("PV1_ArrayRadiance_west")) / 2,
                    m_temp_c=c("PV1_Temp"), m_humidity_pct=c("PV1_HumidityPer"),
                    actual_mw=site_mw("pv1", swap_day), target_mw=site_mw("pv1", swap_day)),
        "pv2": dict(m_ghi=c("PV2_GHI"), m_gti_poa=c("PV2_ArrayRadiance"),
                    m_temp_c=c("PV2_Outdoor_Temp"), m_humidity_pct=c("PV2_HumidityPer"),
                    actual_mw=site_mw("pv2", swap_day), target_mw=c("Pv2_AVL")),
    }
    cols = list(per_site["pv1"].keys())
    long = reduce(DataFrame.unionByName, [raw.select(F.lit(s).alias("site"), "ts_gulf", *[e.alias(k) for k, e in ex.items()])
                                          for s, ex in per_site.items()])
    agg = (long.withColumn("period_end", ceil_to(gulf_to_utc("ts_gulf"), STEP_MIN))
           .groupBy("site", "period_end").agg(*[F.avg(k).alias(k) for k in cols], F.count("target_mw").alias("n_target_min")))
    agg = agg.where(F.to_date("period_end") >= F.lit(start))
    if end:
        agg = agg.where(F.to_date("period_end") <= F.lit(end))
    cap = site_value("capacity_mw")
    t = add_solar(agg, "period_end")
    status = (F.when(F.col("sun_zenith") > 95, "night").when(F.col("n_target_min") == 0, "missing")
              .when((F.col("clearsky_ghi") > 200) & (F.col("target_mw") < 0.01 * cap), "suspected_outage").otherwise("ok"))
    return (
        t.withColumn("status", status)
        .withColumn("target_mw", F.when(F.col("status") == "night", F.coalesce("target_mw", F.lit(0.0))).otherwise(F.col("target_mw")))
        .withColumn("capacity_mw", cap)
        .withColumn("label_ok", F.col("status").isin("ok", "night"))
    )
