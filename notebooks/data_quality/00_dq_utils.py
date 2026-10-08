# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · DQ utilities
# MAGIC Shared constants and helpers for the data-quality notebooks. Load with `%run ./00_dq_utils`.
# MAGIC
# MAGIC Every helper is read-only. `save()` is the only writer and only targets the optional `scratch_schema` widget.

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CATALOG = "ewec_dev_powerops"
PV_TABLE = f"{CATALOG}.bronze.power_contango_minute_pv"
SOLCAST_TABLES = {
    "PV1": f"{CATALOG}.bronze.pv1_history_solcast",
    "PV2": f"{CATALOG}.bronze.pv2_history_solcast",
}
SILVER_5MIN = f"{CATALOG}.silver.uc2_solar_5min"
PREPROCESSED_5MIN = f"{CATALOG}.silver.uc2_solar_preprocessed_5min"
REUNIWATT_CATALOG = "ewec_dev_reuniwatt"
REUNIWATT_SILVER = f"{REUNIWATT_CATALOG}.silver"

PV_COLS = ["PV1_MW", "PV2_MW"]
SOLCAST_COLS = [
    "air_temp", "albedo", "azimuth", "clearsky_dhi", "clearsky_dni", "clearsky_ghi", "clearsky_gti",
    "cloud_opacity", "dewpoint_temp", "dhi", "dni", "ghi", "gti", "precipitation_rate",
    "relative_humidity", "wind_speed_10m", "zenith",
]
IRRADIANCE_COLS = ["ghi", "dni", "dhi", "gti", "clearsky_ghi", "clearsky_dni", "clearsky_dhi", "clearsky_gti"]

# Pipeline magic numbers (see CLAUDE.md)
DAYTIME_CLEARSKY_GHI = 50
DROPOUT_PV_MW = 1.0
PV_PLAUSIBLE_MAX_MW = 1700

# Silver/downstream site label -> upstream Bronze column (labels are swapped at Bronze->Silver)
SILVER_SITE_TO_BRONZE_COL = {"PV1": "PV2_MW", "PV2": "PV1_MW"}

# Abu Dhabi solar noon is ~08:20 UTC / ~12:20 Gulf Standard Time (UTC+4)
SOLAR_NOON_HOUR = {"UTC": 8, "Asia/Dubai": 12}

# COMMAND ----------

def save(df: DataFrame, name: str) -> DataFrame:
    """Persist a result as <scratch_schema>.dq_<name> if the `scratch_schema` widget is set; otherwise return df unchanged."""
    try:
        schema = dbutils.widgets.get("scratch_schema").strip()
    except Exception:
        schema = ""
    if not schema:
        return df
    if schema.split(".")[-1].lower() in {"bronze", "silver", "gold"}:
        raise ValueError(f"Refusing to write DQ output into shared schema '{schema}'")
    full = f"{schema}.dq_{name}"
    # _row keeps the display order (e.g. largest gaps first) so 07_dq_export can print rows in the same order
    df.withColumn("_row", F.monotonically_increasing_id()).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    print(f"saved -> {full}")
    return spark.table(full).orderBy("_row").drop("_row")  # later cells read the saved result instead of recomputing


def present(df: DataFrame, cols):
    return [c for c in cols if c in df.columns]


def time_range(df: DataFrame, ts: str, by=()) -> DataFrame:
    aggs = [
        F.min(ts).alias("min_ts"),
        F.max(ts).alias("max_ts"),
        F.count("*").alias("rows"),
        F.countDistinct(ts).alias("distinct_ts"),
        F.sum(F.col(ts).isNull().cast("int")).alias("null_ts"),
    ]
    return df.groupBy(*by).agg(*aggs) if by else df.agg(*aggs)


def duplicate_timestamps(df: DataFrame, keys, value_cols=()) -> DataFrame:
    """Keys appearing more than once. `distinct_value_sets` > 1 means the duplicates conflict."""
    aggs = [F.count("*").alias("count")]
    if value_cols:
        aggs.append(F.countDistinct(*value_cols).alias("distinct_value_sets"))
    return df.groupBy(*keys).agg(*aggs).where("count > 1").orderBy(F.desc("count"))


def _with_gap(df: DataFrame, ts: str, by=()) -> DataFrame:
    w = Window.partitionBy(*by).orderBy(ts) if by else Window.orderBy(ts)
    return (
        df.select(*by, ts).where(F.col(ts).isNotNull()).distinct()
        .withColumn("prev_ts", F.lag(ts).over(w))
        .withColumn("gap_min", (F.unix_timestamp(ts) - F.unix_timestamp("prev_ts")) / 60)
        .where(F.col("prev_ts").isNotNull())
    )


def interval_profile(df: DataFrame, ts: str, by=()) -> DataFrame:
    """Distribution of minutes between consecutive distinct timestamps."""
    return _with_gap(df, ts, by).groupBy(*by, "gap_min").count().orderBy(*by, F.desc("count"))


def infer_interval_min(df: DataFrame, ts: str) -> int:
    """Native interval = most common gap between consecutive timestamps."""
    return int(interval_profile(df, ts).first()["gap_min"])


def gaps(df: DataFrame, ts: str, interval_min: int, by=()) -> DataFrame:
    """Every break in the series longer than the native interval, longest first."""
    return (
        _with_gap(df, ts, by)
        .where(F.col("gap_min") > interval_min)
        .select(*by, F.col("prev_ts").alias("last_before_gap"), F.col(ts).alias("first_after_gap"),
                (F.col("gap_min") - interval_min).alias("missing_minutes"))
        .orderBy(F.desc("missing_minutes"))
    )


def completeness_summary(df: DataFrame, ts: str, interval_min: int) -> DataFrame:
    r = df.agg(F.min(ts).alias("lo"), F.max(ts).alias("hi"), F.countDistinct(ts).alias("actual")).first()
    expected = int((r.hi - r.lo).total_seconds() // (interval_min * 60)) + 1
    return spark.createDataFrame(
        [(r.lo, r.hi, interval_min, expected, r.actual, expected - r.actual, round(100 * r.actual / expected, 3))],
        "min_ts timestamp, max_ts timestamp, interval_min int, expected_rows long, actual_distinct_ts long, "
        "missing_rows long, completeness_pct double",
    )


def completeness_per_day(df: DataFrame, ts: str, interval_min: int, value_cols=()) -> DataFrame:
    """One row per calendar day (including days with no data at all): timestamp and per-column coverage."""
    expected = 1440 // interval_min
    bounds = df.agg(F.min(F.to_date(ts)).alias("a"), F.max(F.to_date(ts)).alias("b"))
    cal = bounds.select(F.explode(F.sequence("a", "b")).alias("date"))
    counts = df.groupBy(F.to_date(ts).alias("date")).agg(
        F.countDistinct(ts).alias("timestamps"), *[F.count(c).alias(f"{c}_valid") for c in value_cols]
    )
    out = cal.join(counts, "date", "left").fillna(0).withColumn("expected", F.lit(expected))
    out = out.withColumn("timestamp_pct", F.round(100 * F.col("timestamps") / F.col("expected"), 2))
    for c in value_cols:
        out = out.withColumn(f"{c}_pct", F.round(100 * F.col(f"{c}_valid") / F.col("expected"), 2)).drop(f"{c}_valid")
    return out.orderBy("date")


def null_rates(df: DataFrame, cols, period=None) -> DataFrame:
    """Long-format null % per column, optionally split by a period expression (e.g. day/night)."""
    grp = [period.alias("period")] if period is not None else [F.lit("all").alias("period")]
    wide = df.groupBy(*grp).agg(
        F.count("*").alias("rows"),
        *[F.round(100 * F.avg(F.col(c).isNull().cast("int")), 3).alias(c) for c in cols],
    )
    pairs = F.explode(F.array(*[F.struct(F.lit(c).alias("column"), F.col(c).alias("null_pct")) for c in cols]))
    return wide.select("period", "rows", pairs.alias("x")).select("period", "rows", "x.*").orderBy("column", "period")


def null_by_hour(df: DataFrame, ts: str, cols) -> DataFrame:
    return df.groupBy(F.hour(ts).alias("hour")).agg(
        F.count("*").alias("rows"),
        *[F.round(100 * F.avg(F.col(c).isNull().cast("int")), 3).alias(f"{c}_null_pct") for c in cols],
    ).orderBy("hour")


def column_stats(df: DataFrame, cols) -> DataFrame:
    rows = [
        df.agg(
            F.lit(c).alias("column"),
            F.count("*").alias("rows"),
            F.sum(F.col(c).isNull().cast("int")).alias("nulls"),
            F.sum((F.col(c) == 0).cast("int")).alias("zeros"),
            F.min(c).alias("min"),
            F.percentile_approx(c, [0.01, 0.5, 0.99, 0.999]).alias("p01_p50_p99_p999"),
            F.max(c).alias("max"),
        )
        for c in cols
    ]
    return reduce(DataFrame.unionByName, rows)


def rule_counts(df: DataFrame, rules: dict) -> DataFrame:
    """rules: {name: (sql_condition, [required_cols])}. Rules whose columns are missing are skipped."""
    active = {n: e for n, (e, req) in rules.items() if set(req) <= set(df.columns)}
    skipped = [n for n in rules if n not in active]
    if skipped:
        print("Skipped (columns missing):", skipped)
    total = df.count()
    agg = df.agg(*[F.sum(F.expr(e).cast("int")).alias(n) for n, e in active.items()]).first()
    out = [(n, e, int(agg[n] or 0), round(100 * (agg[n] or 0) / total, 4) if total else None) for n, e in active.items()]
    return spark.createDataFrame(out, "rule string, condition string, violations long, pct double")


def stuck_runs(df: DataFrame, ts: str, col: str, min_rows: int, ignore_values=(0,)) -> DataFrame:
    """Runs of identical consecutive non-null values at least `min_rows` long (nulls are skipped, not run-breaking)."""
    w = Window.orderBy(ts)
    d = (
        df.select(ts, col).where(F.col(col).isNotNull())
        .withColumn("chg", F.coalesce((F.col(col) != F.lag(col).over(w)).cast("int"), F.lit(1)))
        .withColumn("run_id", F.sum("chg").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    )
    runs = d.groupBy("run_id").agg(
        F.first(col).alias("value"), F.min(ts).alias("start"), F.max(ts).alias("end"), F.count("*").alias("rows")
    ).where(F.col("rows") >= min_rows)
    if ignore_values:
        runs = runs.where(~F.col("value").isin(*ignore_values))
    return runs.select(F.lit(col).alias("column"), "value", "start", "end", "rows").orderBy(F.desc("rows"))


def hourly_profile(df: DataFrame, ts: str, cols) -> DataFrame:
    return df.groupBy(F.hour(ts).alias("hour")).agg(*[F.avg(c).alias(f"avg_{c}") for c in cols]).orderBy("hour")


def guess_timezone(peak_hour: int) -> str:
    for tz, h in SOLAR_NOON_HOUR.items():
        if abs(peak_hour - h) <= 1:
            return tz
    return "unclear"


def numeric_cols(df: DataFrame, exclude=()):
    num = ("double", "float", "integer", "long", "decimal", "short")
    return [f.name for f in df.schema.fields if f.dataType.typeName() in num and f.name not in exclude]


def weather_rules(df: DataFrame) -> dict:
    """Physical sanity rules for irradiance/weather columns, for use with rule_counts()."""
    irr = present(df, IRRADIANCE_COLS)
    return {
        "negative_irradiance": (" OR ".join(f"{c} < 0" for c in irr) or "false", irr),
        "irradiance_at_night": ("zenith > 90 AND (ghi > 5 OR dni > 5 OR dhi > 5)", ["zenith", "ghi", "dni", "dhi"]),
        "clearsky_at_night": ("zenith > 90 AND clearsky_ghi > 5", ["zenith", "clearsky_ghi"]),
        "ghi_far_above_clearsky": ("ghi > clearsky_ghi * 1.2 AND ghi - clearsky_ghi > 50", ["ghi", "clearsky_ghi"]),
        "dni_far_above_clearsky": ("dni > clearsky_dni * 1.2 AND dni - clearsky_dni > 50", ["dni", "clearsky_dni"]),
        "ghi_above_1400": ("ghi > 1400", ["ghi"]),
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


def bucket_end(df: DataFrame, ts: str, cols, minutes: int, out_ts: str = "period_end") -> DataFrame:
    """Average `cols` into `minutes`-wide buckets labelled by bucket END (period_end convention)."""
    end = F.timestamp_seconds(F.ceil(F.unix_timestamp(ts) / (minutes * 60)) * minutes * 60)
    return df.groupBy(end.alias(out_ts)).agg(*[F.avg(c).alias(c) for c in cols])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solar geometry
# MAGIC Computed sun position, so day/night can be decided for sources without a `zenith` column (e.g. Reuniwatt).
# MAGIC Both plants are in Abu Dhabi emirate; a 1° longitude difference shifts solar time by only ~4 min, so one reference point is enough for day/night and clearness checks.

# COMMAND ----------

SITE_LAT, SITE_LON = 24.45, 54.6
SOLAR_CONSTANT = 1361.0
TZ_OFFSET_H = {"UTC": 0, "Asia/Dubai": 4}


def add_solar_geometry(df: DataFrame, ts: str = "period_end", tz_offset_h: int = 0, granularity_col: str = "granularity_min",
                       lat: float = SITE_LAT, lon: float = SITE_LON) -> DataFrame:
    """Adds sun_zenith (deg), etr_horizontal (W/m2, top-of-atmosphere on a horizontal plane) and daypart.

    Evaluated at the middle of each period (period_end − granularity/2). `tz_offset_h` is the offset of the
    timestamps from UTC (0 if they are UTC, 4 if Asia/Dubai). NOAA approximation, accurate to well under a degree.
    """
    half = F.col(granularity_col) * 30 if granularity_col in df.columns else F.lit(0)
    t = F.timestamp_seconds(F.unix_timestamp(ts) - tz_offset_h * 3600 - half)
    doy = F.dayofyear(t)
    hr = F.hour(t) + F.minute(t) / 60 + F.second(t) / 3600
    g = 2 * 3.141592653589793 / 365 * (doy - 1 + (hr - 12) / 24)
    decl = (0.006918 - 0.399912 * F.cos(g) + 0.070257 * F.sin(g) - 0.006758 * F.cos(2 * g)
            + 0.000907 * F.sin(2 * g) - 0.002697 * F.cos(3 * g) + 0.00148 * F.sin(3 * g))
    eqtime = 229.18 * (0.000075 + 0.001868 * F.cos(g) - 0.032077 * F.sin(g) - 0.014615 * F.cos(2 * g) - 0.040849 * F.sin(2 * g))
    ha = F.radians((hr * 60 + eqtime + 4 * lon) / 4 - 180)
    latr = F.radians(F.lit(lat))
    cosz = F.sin(latr) * F.sin(decl) + F.cos(latr) * F.cos(decl) * F.cos(ha)
    cosz = F.least(F.greatest(cosz, F.lit(-1.0)), F.lit(1.0))
    etr = SOLAR_CONSTANT * (1 + 0.033 * F.cos(2 * 3.141592653589793 * doy / 365)) * F.greatest(cosz, F.lit(0.0))
    return (
        df.withColumn("sun_zenith", F.degrees(F.acos(cosz)))
        .withColumn("etr_horizontal", etr)
        .withColumn("daypart", F.when(F.col("sun_zenith") < 85, "day").when(F.col("sun_zenith") > 95, "night").otherwise("twilight"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecast-table helpers (Reuniwatt fact tables)
# MAGIC A forecast table has one row per run (`reference_time`, issue time) × target period (`period_end`), per segment
# MAGIC (`site`, `provider`, `horizon`, `granularity_min`).

# COMMAND ----------

FC_KEY = ["site", "provider", "horizon", "granularity_min", "reference_time", "period_end"]
FC_SEG = ["site", "provider", "horizon", "granularity_min"]
FC_META = ["_source", "_ingested_at", "forecast_day_offset", "_backup_at", "_backup_run_id"]


def prep_forecast(df: DataFrame) -> DataFrame:
    for c in ("period_end", "reference_time", "_ingested_at", "_backup_at"):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("timestamp"))
    for c in ("site", "horizon", "provider"):
        if c in df.columns:
            df = df.withColumn(c, F.lower(F.trim(c)))
    return df.withColumn("lead_min", (F.unix_timestamp("period_end") - F.unix_timestamp("reference_time")) / 60)


def fc_overview(df: DataFrame) -> DataFrame:
    extra = [F.max("_ingested_at").alias("last_ingested")] if "_ingested_at" in df.columns else []
    return df.groupBy(*FC_SEG).agg(
        F.count("*").alias("rows"),
        F.countDistinct("reference_time").alias("runs"),
        F.min("reference_time").alias("first_issue"),
        F.max("reference_time").alias("last_issue"),
        F.min("period_end").alias("first_period"),
        F.max("period_end").alias("last_period"),
        *extra,
    ).orderBy(*FC_SEG)


def fc_lead(df: DataFrame) -> DataFrame:
    return df.groupBy(*FC_SEG).agg(
        F.min("lead_min").alias("min_lead_min"),
        F.percentile_approx("lead_min", 0.5).alias("median_lead_min"),
        F.max("lead_min").alias("max_lead_min"),
        F.round(F.max("lead_min") / 1440, 2).alias("max_lead_days"),
        F.sum((F.col("lead_min") <= 0).cast("int")).alias("rows_lead_le_0"),
        F.sum((F.col("lead_min") < 0).cast("int")).alias("rows_lead_lt_0"),
    ).orderBy(*FC_SEG)


def fc_day_offset_check(df: DataFrame):
    """forecast_day_offset should equal the calendar-day difference between period_end and reference_time."""
    actual = F.datediff(F.to_date("period_end"), F.to_date("reference_time"))
    dist = df.groupBy("horizon", "forecast_day_offset").agg(F.count("*").alias("rows")).orderBy("horizon", "forecast_day_offset")
    mismatch = df.withColumn("computed_day_offset", actual).groupBy("horizon").agg(
        F.count("*").alias("rows"),
        F.sum((F.col("forecast_day_offset") != F.col("computed_day_offset")).cast("int")).alias("mismatched"),
        F.sum(F.col("forecast_day_offset").isNull().cast("int")).alias("null_offset"),
    ).orderBy("horizon")
    return dist, mismatch


def fc_ingestion(df: DataFrame) -> DataFrame:
    """Delay between issue (reference_time) and ingestion; negative = ingested before it was issued."""
    lag_min = (F.unix_timestamp("_ingested_at") - F.unix_timestamp("reference_time")) / 60
    return df.withColumn("ingest_lag_min", lag_min).groupBy(*FC_SEG).agg(
        F.min("ingest_lag_min").alias("min_ingest_lag_min"),
        F.percentile_approx("ingest_lag_min", 0.5).alias("median_ingest_lag_min"),
        F.max("ingest_lag_min").alias("max_ingest_lag_min"),
        F.sum((F.col("ingest_lag_min") < 0).cast("int")).alias("rows_ingested_before_issue"),
        F.countDistinct("_ingested_at").alias("distinct_ingest_times"),
    ).orderBy(*FC_SEG)


def fc_cadence(df: DataFrame):
    """Returns (summary per segment, list of gaps between runs > 1.5x the usual cadence)."""
    g = _with_gap(df, "reference_time", FC_SEG)
    cadence = (
        g.groupBy(*FC_SEG, "gap_min").count()
        .withColumn("rk", F.row_number().over(Window.partitionBy(*FC_SEG).orderBy(F.desc("count"))))
        .where("rk = 1").select(*FC_SEG, F.col("gap_min").alias("cadence_min"))
    )
    gaps_df = g.join(cadence, FC_SEG).where("gap_min > 1.5 * cadence_min").select(
        *FC_SEG, "cadence_min", F.col("prev_ts").alias("last_run_before_gap"), F.col("reference_time").alias("first_run_after_gap"),
        F.round(F.col("gap_min") / F.col("cadence_min") - 1).alias("approx_missing_runs"),
    )
    summary = cadence.join(
        gaps_df.groupBy(*FC_SEG).agg(F.count("*").alias("gaps"), F.sum("approx_missing_runs").alias("approx_missing_runs")), FC_SEG, "left"
    ).fillna(0, ["gaps", "approx_missing_runs"]).orderBy(*FC_SEG)
    return summary, gaps_df.orderBy(F.desc("approx_missing_runs"))


def fc_run_completeness(df: DataFrame):
    """Returns (summary per segment, runs with fewer steps than the segment median)."""
    runs = df.groupBy(*FC_SEG, "reference_time").agg(
        F.countDistinct("period_end").alias("steps"), F.min("lead_min").alias("min_lead_min"), F.max("lead_min").alias("max_lead_min")
    )
    runs = runs.join(runs.groupBy(*FC_SEG).agg(F.percentile_approx("steps", 0.5).alias("expected_steps")), FC_SEG)
    w = Window.partitionBy(*FC_SEG, "reference_time").orderBy("period_end")
    irregular = (
        df.select(*FC_SEG, "reference_time", "period_end").distinct()
        .withColumn("step_min", (F.unix_timestamp("period_end") - F.unix_timestamp(F.lag("period_end").over(w))) / 60)
        .where("step_min IS NOT NULL AND step_min != granularity_min")
        .groupBy(*FC_SEG).agg(F.count("*").alias("irregular_steps"))
    )
    summary = runs.groupBy(*FC_SEG, "expected_steps").agg(
        F.count("*").alias("runs"),
        F.sum((F.col("steps") < F.col("expected_steps")).cast("int")).alias("short_runs"),
        F.sum((F.col("steps") > F.col("expected_steps")).cast("int")).alias("long_runs"),
        F.min("steps").alias("min_steps"),
    ).withColumn("short_pct", F.round(100 * F.col("short_runs") / F.col("runs"), 2))
    summary = summary.join(irregular, FC_SEG, "left").fillna(0, ["irregular_steps"]).orderBy(*FC_SEG)
    return summary, runs.where("steps < expected_steps").orderBy(*FC_SEG, "reference_time")


def fc_coverage(df: DataFrame):
    """Returns (target-time coverage per segment, days below 99%)."""
    cov = df.groupBy(*FC_SEG).agg(
        F.min("period_end").alias("first_period"), F.max("period_end").alias("last_period"), F.countDistinct("period_end").alias("periods")
    )
    cov = cov.withColumn(
        "expected_periods", ((F.unix_timestamp("last_period") - F.unix_timestamp("first_period")) / 60 / F.col("granularity_min") + 1).cast("long")
    ).withColumn("coverage_pct", F.round(100 * F.col("periods") / F.col("expected_periods"), 2))
    per_day = (
        df.groupBy(*FC_SEG, F.to_date("period_end").alias("date")).agg(F.countDistinct("period_end").alias("periods"))
        .withColumn("expected", (1440 / F.col("granularity_min")).cast("int"))
        .withColumn("pct", F.round(100 * F.col("periods") / F.col("expected"), 1))
    )
    return cov.orderBy(*FC_SEG), per_day.where("pct < 99").orderBy(*FC_SEG, "date")


def fc_stale_runs(df: DataFrame, cols, main: str):
    """A run is stale if, for >=3 overlapping non-zero targets, every value equals the previous run's."""
    w = Window.partitionBy(*FC_SEG, "period_end").orderBy("reference_time")
    same = reduce(lambda a, b: a & b, [F.col(c).eqNullSafe(F.lag(c).over(w)) for c in cols])
    pairs = (
        df.select(*FC_SEG, "reference_time", "period_end", *cols)
        .withColumn("has_prev", F.lag("reference_time").over(w).isNotNull())
        .withColumn("same", same)
        .where(F.col("has_prev") & (F.col(main) != 0))
    )
    per_run = pairs.groupBy(*FC_SEG, "reference_time").agg(F.count("*").alias("overlap"), F.sum(F.col("same").cast("int")).alias("identical"))
    stale = per_run.where("overlap >= 3 AND identical = overlap")
    summary = (
        per_run.groupBy(*FC_SEG).agg(F.count("*").alias("runs_compared"))
        .join(stale.groupBy(*FC_SEG).agg(F.count("*").alias("stale_runs")), FC_SEG, "left").fillna(0, ["stale_runs"])
        .withColumn("stale_pct", F.round(100 * F.col("stale_runs") / F.col("runs_compared"), 2)).orderBy(*FC_SEG)
    )
    return summary, stale.orderBy(*FC_SEG, "reference_time")


def fc_latest(df: DataFrame) -> DataFrame:
    """Most recent run per segment and target period (the shortest-lead forecast)."""
    w = Window.partitionBy(*FC_SEG, "period_end").orderBy(F.desc("reference_time"))
    return df.withColumn("_rk", F.row_number().over(w)).where("_rk = 1").drop("_rk")


def fc_backup_compare(df: DataFrame, backup_table: str):
    """Returns (summary of main vs backup keys, backup snapshots). Raises if the backup is missing."""
    bk = prep_forecast(spark.table(backup_table))
    km, kb = df.select(*FC_KEY).distinct(), bk.select(*FC_KEY).distinct()
    summary = spark.createDataFrame([(
        df.count(), bk.count(), km.join(kb, FC_KEY, "left_anti").count(), kb.join(km, FC_KEY, "left_anti").count(),
    )], "rows_main long, rows_backup long, keys_only_in_main long, keys_only_in_backup long")
    snaps = bk.groupBy("_backup_run_id").agg(
        F.min("_backup_at").alias("backup_at"), F.count("*").alias("rows"), F.max("reference_time").alias("latest_issue_in_snapshot")
    ).orderBy("backup_at") if "_backup_run_id" in bk.columns else None
    return summary, snaps


def peak_hour_by_site(df: DataFrame, col: str, ts: str = "period_end") -> DataFrame:
    prof = df.groupBy("site", F.hour(ts).alias("hour")).agg(F.avg(col).alias(f"avg_{col}"))
    best = prof.withColumn("rk", F.row_number().over(Window.partitionBy("site").orderBy(F.desc(f"avg_{col}")))).where("rk = 1")
    guess = F.when(F.abs(F.col("hour") - SOLAR_NOON_HOUR["UTC"]) <= 1, "UTC") \
        .when(F.abs(F.col("hour") - SOLAR_NOON_HOUR["Asia/Dubai"]) <= 1, "Asia/Dubai").otherwise("unclear")
    return best.select("site", F.col("hour").alias("peak_hour"), guess.alias("looks_like"))


def pv_observed(minutes: int, shift_h: int = 0) -> DataFrame:
    """Bronze PV averaged into `minutes` buckets labelled by bucket end, optionally shifted by `shift_h` hours."""
    pv = spark.table(PV_TABLE).select(
        F.timestamp_seconds(F.unix_timestamp(F.col("DateTime").cast("timestamp")) + shift_h * 3600).alias("ts"), *PV_COLS
    )
    return bucket_end(pv, "ts", PV_COLS, minutes)


def lag_scan_vs_pv(latest: DataFrame, col: str, hours: int = 6) -> DataFrame:
    """Hourly correlation of `col` with each Bronze PV column for offsets -hours..+hours.
    Best offset k means PV time ≈ forecast period_end + k."""
    hour_end = F.timestamp_seconds(F.ceil(F.unix_timestamp("period_end") / 3600) * 3600)
    fc_h = latest.groupBy("site", hour_end.alias("period_end")).agg(F.avg(col).alias(col))
    pv_h = pv_observed(60)
    rows = []
    for off in range(-hours, hours + 1):
        j = fc_h.withColumn("period_end", F.timestamp_seconds(F.unix_timestamp("period_end") + off * 3600)).join(pv_h, "period_end")
        for r in j.groupBy("site").agg(*[F.corr(col, c).alias(c) for c in PV_COLS]).collect():
            rows += [(r["site"], c, off, r[c]) for c in PV_COLS]
    return spark.createDataFrame(rows, "site string, pv_col string, offset_h int, corr double")


def best_rows(df: DataFrame, by, order_col: str) -> DataFrame:
    return df.withColumn("_rk", F.row_number().over(Window.partitionBy(*by).orderBy(F.desc(order_col)))).where("_rk = 1").drop("_rk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes: single findings (counts, detected settings) saved as `dq_<tag>_notes` for 07_dq_export

# COMMAND ----------

_NOTES = {"tag": None, "rows": []}


def notes_init(tag: str):
    _NOTES["tag"], _NOTES["rows"] = tag, []


def note(key: str, value):
    """Print a single finding and persist all notes so far (no-op persist without scratch_schema)."""
    print(f"{key}: {value}")
    _NOTES["rows"].append((key, str(value)))
    if _NOTES["tag"]:
        save(spark.createDataFrame(_NOTES["rows"], "key string, value string"), f"{_NOTES['tag']}_notes")


def dup_note(dups: DataFrame) -> str:
    return f"{dups.count()} (conflicting: {dups.where('distinct_value_sets > 1').count()})"
