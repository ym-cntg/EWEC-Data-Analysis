"""Data engineering nodes: Bronze -> cleaned 5-min intermediate tables -> product-resolution primary tables.

Raw data:
- PV telemetry: 1-min, Gulf time. PV1 = small plant (actual MW), PV2 = large plant (target Pv2_AVL).
  Bronze PV1_MW / PV2_MW were mislabelled before ~Mar 2026 (detected automatically).
- Reuniwatt: one Bronze row per forecast run; `_raw` JSON = site -> variable -> (UTC time -> value); power in kW.
"""
from datetime import date, timedelta
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ...spark_utils import add_solar, ceil_to, gulf_to_utc, site_value


def _null():
    return F.lit(None).cast("double")

REUNI_JSON_SCHEMA = "MAP<STRING, MAP<STRING, MAP<STRING, DOUBLE>>>"
REUNI_VARS = {
    "ghi": "ghi", "dni": "dni", "dhi": "dhi", "bhi": "bhi",
    "gti": "gti", "gti_90": "gti_east", "gti_270": "gti_west",
    "temperature": "temp_c", "dewpoint": "dewpoint_c", "relative_humidity": "humidity_pct",
    "wind_speed": "wind_speed", "wind_direction": "wind_dir", "cloud_opacity": "cloud_opacity",
    "pressure": "pressure_hpa", "precip": "precip_rate_mmh", "clearness_index": "clearness_index",
    "power": "power_mw", "power10": "power_p10_mw", "power90": "power_p90_mw",
}
POWER_COLS = ["power_mw", "power_p10_mw", "power_p90_mw"]


def _window(run: dict, de: dict):
    start = date.fromisoformat(run["start_date"]) - timedelta(days=de["history_days_before_start"])
    end = date.fromisoformat(run["end_date"])
    return start, end


def _clean_mw(col: str, de: dict):
    v = F.col(col).cast("double")
    return (F.when(F.abs(v) > de["pv_sentinel_abs_mw"], _null())
            .when(v < de["night_neg_floor_mw"], _null())
            .when(v < 0, F.lit(0.0)).otherwise(v))


def _detect_swap_date(raw_pv: DataFrame, de: dict):
    setting = str(de.get("swap_date", "auto")).strip().lower()
    if setting == "none":
        return None
    if setting != "auto":
        return date.fromisoformat(setting)
    daily = (
        raw_pv.select(F.to_date(F.col("DateTime").cast("timestamp")).alias("day"), _clean_mw("PV1_MW", de).alias("a"), _clean_mw("PV2_MW", de).alias("b"))
        .groupBy("day").agg(F.max("a").alias("pv1_peak"), F.max("b").alias("pv2_peak"))
        .withColumn("pv2_bigger", (F.col("pv2_peak") > F.col("pv1_peak")).cast("int"))
        .withColumn("pv2_bigger_7d", F.avg("pv2_bigger").over(Window.orderBy("day").rowsBetween(-3, 3)))
    )
    last_pv1_big = daily.where("pv2_bigger_7d < 0.5").agg(F.max("day")).first()[0]
    last_day = daily.agg(F.max("day")).first()[0]
    return (last_pv1_big + timedelta(days=1)) if last_pv1_big and last_pv1_big < last_day else None


def clean_target(raw_pv: DataFrame, sites: dict, de: dict, run: dict) -> DataFrame:
    """5-min target per site (UTC, period end): PV1 actual MW, PV2 Pv2_AVL; actual MW kept; status and label_ok."""
    start, end = _window(run, de)
    swap = _detect_swap_date(raw_pv, de)
    print(f"    Bronze MW column swap date: {swap}")
    swap_lit = F.lit(swap or date(9999, 12, 31))
    off, step = de["gulf_offset_h"], de["base_step_min"]

    raw = (raw_pv.withColumn("ts_gulf", F.col("DateTime").cast("timestamp"))
           .where(F.col("ts_gulf").isNotNull())
           .where(F.to_date("ts_gulf").between(F.lit(start - timedelta(days=1)), F.lit(end + timedelta(days=3)))))
    avl = F.col("Pv2_AVL").cast("double")
    avl_clean = F.when((avl < 0) | (avl > de["avl_max_mw"]), _null()).otherwise(avl)

    def actual(cfg):
        return F.when(F.to_date("ts_gulf") < swap_lit, _clean_mw(cfg["bronze_col_before_swap"], de)) \
            .otherwise(_clean_mw(cfg["bronze_col_after_swap"], de))

    long = reduce(DataFrame.unionByName, [
        raw.select(F.lit(s).alias("site"), "ts_gulf", actual(cfg).alias("actual_mw"),
                   (avl_clean if cfg["target"] == "available" else actual(cfg)).alias("target_mw"))
        for s, cfg in sites.items()
    ])
    agg = (long.withColumn("period_end", ceil_to(gulf_to_utc("ts_gulf", off), step))
           .groupBy("site", "period_end")
           .agg(F.avg("target_mw").alias("target_mw"), F.avg("actual_mw").alias("actual_mw"), F.count("target_mw").alias("n_target_min"))
           .where(F.to_date("period_end").between(F.lit(start), F.lit(end + timedelta(days=2)))))
    cap = site_value(sites, "capacity_mw")
    t = add_solar(agg, sites, "period_end", step)
    status = (F.when(F.col("sun_zenith") > 95, "night")
              .when(F.col("n_target_min") == 0, "missing")
              .when((F.col("clearsky_ghi") > de["outage_clearsky_ghi"]) & (F.col("target_mw") < de["outage_share_of_capacity"] * cap), "suspected_outage")
              .otherwise("ok"))
    return (t.withColumn("status", status)
            .withColumn("target_mw", F.when(F.col("status") == "night", F.coalesce("target_mw", F.lit(0.0))).otherwise(F.col("target_mw")))
            .withColumn("capacity_mw", cap)
            .withColumn("label_ok", F.col("status").isin("ok", "night"))
            .select("site", "period_end", "target_mw", "actual_mw", "n_target_min", "capacity_mw", "status", "label_ok", "sun_zenith", "is_day"))


def unpack_reuniwatt(raw_reuni: DataFrame, de: dict, run: dict) -> DataFrame:
    """One row per site × horizon × run × 5-min target. Keeps the configured horizons and run thinning; power in MW."""
    start, end = _window(run, de)
    cfg = de["reuniwatt"]
    b = (raw_reuni.where(F.col("_source") != "migration_legacy")
         .withColumn("reference_time", F.col("reference_time").cast("timestamp"))
         .withColumn("_ingested_at", F.col("_ingested_at").cast("timestamp"))
         .withColumn("site", F.lower(F.trim("site"))).withColumn("horizon", F.lower(F.trim("horizon")))
         .where(F.col("horizon").isin(*cfg["horizons"]))
         .where(F.to_date("reference_time").between(F.lit(start), F.lit(end))))
    every = None
    for h, mins in cfg["keep_run_every_min"].items():
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
                               .otherwise(F.array_repeat(_null(), F.col("_n"))))
    z = runs.select("site", "horizon", "reference_time", "raw_source", F.explode(F.arrays_zip("_times", *REUNI_VARS.values())).alias("z"))
    out = z.select("site", "horizon", "reference_time", "raw_source",
                   F.to_timestamp(F.col("z._times"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("period_end"),
                   *[F.col(f"z.{v}").alias(v) for v in REUNI_VARS.values()])

    sample = b.where("horizon = 'dayahead'").orderBy(F.desc("reference_time")).limit(1)
    mx = sample.select(F.explode(F.map_values(F.try_element_at(m, F.lit("power")))).alias("v")).agg(F.max("v")).first()[0] or 0
    factor, unit = (0.001, "kW") if mx > 10_000 else (1000.0, "GW") if mx < 10 else (1.0, "MW")
    print(f"    Reuniwatt power max {mx:,.1f} -> {unit}; × {factor} to MW")
    for c in POWER_COLS:
        out = out.withColumn(c, F.col(c) * factor)
    lead = (F.unix_timestamp("period_end") - F.unix_timestamp("reference_time")) / 60
    return (out.withColumn("gti_poa", F.when(F.col("site") == "pv1", (F.col("gti_east") + F.col("gti_west")) / 2).otherwise(F.col("gti")))
            .withColumn("lead_min", lead)
            .where((F.col("lead_min") > 0) & (F.col("lead_min") <= cfg["max_lead_min"])))


def flag_broken_runs(int_reuni: DataFrame, sites: dict, de: dict) -> DataFrame:
    """A run is broken if, with the sun high, most of its GHI values are near zero (e.g. 3 Jul 2026)."""
    c = de["broken_run"]
    x = add_solar(int_reuni.select("site", "horizon", "reference_time", "period_end", "ghi"), sites, "period_end", de["base_step_min"])
    high = F.col("clearsky_ghi") > c["high_sun_clearsky_ghi"]
    flags = (x.groupBy("site", "horizon", "reference_time").agg(
        F.sum(high.cast("int")).alias("n_high_sun"),
        F.sum((high & (F.col("ghi") < c["dark_share_of_clearsky"] * F.col("clearsky_ghi"))).cast("int")).alias("n_dark"))
        .withColumn("run_broken", (F.col("n_high_sun") >= c["min_high_sun_steps"]) & (F.col("n_dark") > c["max_dark_share"] * F.col("n_high_sun"))))
    return flags


def resample_reuniwatt(int_reuni: DataFrame, flags: DataFrame, product_cfg: dict, fe: dict) -> DataFrame:
    """Average 5-min forecasts into the product resolution, dropping broken runs."""
    key = ["site", "horizon", "reference_time"]
    res = product_cfg["resolution_min"]
    cols = fe["forecast_vars"] + fe["weather_vars"]
    return (int_reuni.join(flags.select(*key, "run_broken"), key).where(~F.col("run_broken"))
            .withColumn("period_end", ceil_to("period_end", res))
            .groupBy(*key, "period_end").agg(*[F.avg(c).alias(c) for c in cols]))


def resample_target(int_target: DataFrame, product_cfg: dict, de: dict) -> DataFrame:
    """Average the 5-min target into the product resolution; label_ok only if every 5-min part is usable."""
    res = product_cfg["resolution_min"]
    parts = res // de["base_step_min"]
    return (int_target.withColumn("period_end", ceil_to("period_end", res))
            .groupBy("site", "period_end").agg(
                F.avg("target_mw").alias("label_target_mw"), F.avg("actual_mw").alias("label_actual_mw"),
                F.first("capacity_mw").alias("capacity_mw"),
                F.count("*").alias("_n"), F.min(F.col("label_ok").cast("int")).alias("_ok"))
            .withColumn("label_ok", (F.col("_n") == parts) & (F.col("_ok") == 1))
            .withColumn("label_target_norm", F.col("label_target_mw") / F.col("capacity_mw"))
            .drop("_n", "_ok"))
