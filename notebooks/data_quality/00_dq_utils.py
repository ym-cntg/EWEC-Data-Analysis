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
    """Persist a result to the scratch schema if the `scratch_schema` widget is set; otherwise no-op."""
    try:
        schema = dbutils.widgets.get("scratch_schema").strip()
    except Exception:
        schema = ""
    if not schema:
        return df
    if schema.split(".")[-1].lower() in {"bronze", "silver", "gold"}:
        raise ValueError(f"Refusing to write DQ output into shared schema '{schema}'")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{schema}.dq_{name}")
    print(f"saved -> {schema}.dq_{name}")
    return df


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
