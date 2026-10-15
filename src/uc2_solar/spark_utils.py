"""Spark helpers: time arithmetic, sun position / clear-sky, calendar features, as-of joins.

All timestamps are UTC and label the END of a period (`period_end`), like Reuniwatt.
These functions assume the Spark session time zone is UTC (set by the runner).
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

PI = 3.141592653589793


def _c(col) -> Column:
    return F.col(col) if isinstance(col, str) else col


def shift_minutes(col, minutes) -> Column:
    return F.timestamp_seconds(F.unix_timestamp(_c(col)) + F.lit(minutes) * 60)


def ceil_to(col, minutes: int) -> Column:
    """Label a timestamp with the end of its `minutes`-wide period."""
    return F.timestamp_seconds(F.ceil(F.unix_timestamp(_c(col)) / (minutes * 60)) * (minutes * 60))


def utc_to_gulf(col, offset_h: int = 4) -> Column:
    return shift_minutes(col, offset_h * 60)


def gulf_to_utc(col, offset_h: int = 4) -> Column:
    return shift_minutes(col, -offset_h * 60)


def site_value(sites: dict, key: str) -> Column:
    """Column holding sites[<row's site>][key]."""
    expr = None
    for s, cfg in sites.items():
        cond, val = F.col("site") == s, F.lit(cfg[key])
        expr = F.when(cond, val) if expr is None else expr.when(cond, val)
    return expr


def add_solar(df: DataFrame, sites: dict, ts: str = "period_end", period_min: int = 5, prefix: str = "") -> DataFrame:
    """Sun position and clear-sky irradiance at the middle of the period ending at `ts` (needs a `site` column).
    NOAA solar position, Kasten–Young air mass, Haurwitz clear-sky GHI."""
    t = F.timestamp_seconds(F.unix_timestamp(ts) - period_min * 30)
    doy = F.dayofyear(t)
    hr = F.hour(t) + F.minute(t) / 60 + F.second(t) / 3600
    g = 2 * PI / 365 * (doy - 1 + (hr - 12) / 24)
    decl = (0.006918 - 0.399912 * F.cos(g) + 0.070257 * F.sin(g) - 0.006758 * F.cos(2 * g)
            + 0.000907 * F.sin(2 * g) - 0.002697 * F.cos(3 * g) + 0.00148 * F.sin(3 * g))
    eqtime = 229.18 * (0.000075 + 0.001868 * F.cos(g) - 0.032077 * F.sin(g) - 0.014615 * F.cos(2 * g) - 0.040849 * F.sin(2 * g))
    lat = F.radians(site_value(sites, "lat"))
    ha = F.radians((hr * 60 + eqtime + 4 * site_value(sites, "lon")) / 4 - 180)
    cosz = F.least(F.greatest(F.sin(lat) * F.sin(decl) + F.cos(lat) * F.cos(decl) * F.cos(ha), F.lit(-1.0)), F.lit(1.0))
    zen = F.degrees(F.acos(cosz))
    p = prefix
    return (
        df.withColumn(f"{p}sun_zenith", zen)
        .withColumn(f"{p}sun_elevation", 90 - zen)
        .withColumn(f"{p}cos_zenith", F.greatest(cosz, F.lit(0.0)))
        .withColumn(f"{p}air_mass", F.when(zen < 89, 1 / (cosz + 0.50572 * F.pow(96.07995 - zen, -1.6364))))
        .withColumn(f"{p}etr_horizontal", 1361.0 * (1 + 0.033 * F.cos(2 * PI * doy / 365)) * F.greatest(cosz, F.lit(0.0)))
        .withColumn(f"{p}clearsky_ghi", F.when(cosz > 0.01, 1098.0 * cosz * F.exp(-0.057 / cosz)).otherwise(F.lit(0.0)))
        .withColumn(f"{p}is_day", zen < 90)
    )


def add_calendar(df: DataFrame, ts: str, prefix: str, offset_h: int = 4, with_doy: bool = True) -> DataFrame:
    """Cyclical hour-of-day (and day-of-year) in Gulf time."""
    g = utc_to_gulf(ts, offset_h)
    hr = F.hour(g) + F.minute(g) / 60
    out = df.withColumn(f"{prefix}hour_sin", F.sin(2 * PI * hr / 24)).withColumn(f"{prefix}hour_cos", F.cos(2 * PI * hr / 24))
    if with_doy:
        doy = F.dayofyear(g)
        out = out.withColumn(f"{prefix}doy_sin", F.sin(2 * PI * doy / 365.25)).withColumn(f"{prefix}doy_cos", F.cos(2 * PI * doy / 365.25))
    return out


def asof_runs(points: DataFrame, runs: DataFrame, latency_min: int, time_col: str = "t") -> DataFrame:
    """Latest run available at each point: reference_time + latency <= point time.

    points: (site, origin, <time_col>)   runs: distinct (site, reference_time)
    returns: (site, origin, run_time, run_age_min) with run_age_min measured from <time_col>.
    """
    r = runs.select(
        "site", shift_minutes("reference_time", latency_min).alias("t"),
        F.lit(None).cast("timestamp").alias("origin"), F.col("reference_time").alias("run_time"), F.lit(1).alias("is_run"),
    )
    o = points.select("site", F.col(time_col).alias("t"), "origin", F.lit(None).cast("timestamp").alias("run_time"), F.lit(0).alias("is_run"))
    w = Window.partitionBy("site").orderBy("t", F.desc("is_run")).rowsBetween(Window.unboundedPreceding, 0)
    return (
        r.unionByName(o)
        .withColumn("run_time", F.last("run_time", ignorenulls=True).over(w))
        .where("is_run = 0")
        .select("site", "origin", "run_time", ((F.unix_timestamp("t") - F.unix_timestamp("run_time")) / 60).alias("run_age_min"))
    )
