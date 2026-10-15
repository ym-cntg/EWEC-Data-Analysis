"""Feature engineering nodes (Reuniwatt + computed features only).

Row grain of the feature table: site × origin (forecast time) × period_end (target), at the product resolution.
Leakage rule: every Reuniwatt value comes from a run issued at least `latency_min` before the origin
(or before the shifted point for "across runs" features).

Lag families:
- around the target:  same run, neighbouring steps (earlier / later values, change, centred rolling mean / std)
- across runs:        same target from main-horizon runs issued 1 h / 3 h / 6 h earlier, and the revision since
- across horizons:    main horizon minus other horizon for the same target
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ...spark_utils import add_calendar, add_solar, asof_runs, shift_minutes, site_value

RUN_KEY = ["site", "horizon", "reference_time", "period_end"]


def reuniwatt_run_features(pri_reuni: DataFrame, sites: dict, fe: dict, product_cfg: dict) -> DataFrame:
    """Per run and target: forecast values, clear-sky ratios and the 'around the target' lag features."""
    res = product_cfg["resolution_min"]
    x = add_solar(pri_reuni, sites, "period_end", res)
    x = (x.withColumn("csi_ghi", F.when(F.col("clearsky_ghi") > 50, F.least(F.col("ghi") / F.col("clearsky_ghi"), F.lit(1.5))))
         .withColumn("kt", F.when(F.col("etr_horizontal") > 50, F.col("ghi") / F.col("etr_horizontal")))
         .withColumn("diffuse_frac", F.when(F.col("ghi") > 20, F.least(F.col("dhi") / F.col("ghi"), F.lit(1.0))))
         .withColumn("beam_frac", F.when(F.col("ghi") > 20, F.least(F.col("bhi") / F.col("ghi"), F.lit(1.0))))
         .drop("sun_zenith", "sun_elevation", "cos_zenith", "air_mass", "etr_horizontal", "clearsky_ghi", "is_day"))

    at = fe["around_target"]
    w = Window.partitionBy("site", "horizon", "reference_time").orderBy("period_end")
    for c in at["columns"]:
        for k in at["lag_steps"]:
            x = x.withColumn(f"{c}_m{k * res}", F.lag(c, k).over(w))
        for k in at["lead_steps"]:
            x = x.withColumn(f"{c}_p{k * res}", F.lead(c, k).over(w))
        x = x.withColumn(f"{c}_d1", F.col(c) - F.lag(c, 1).over(w))
        for h in at["rolling_half_steps"]:
            frame = w.rowsBetween(-h, h)
            x = x.withColumn(f"{c}_mean_pm{h * res}", F.avg(c).over(frame)).withColumn(f"{c}_std_pm{h * res}", F.stddev(c).over(frame))
    return x


def build_origins(spark, product_cfg: dict, run: dict, sites: dict, de: dict) -> DataFrame:
    """Forecast origins: fixed Gulf clock hours each day, for every site (UTC)."""
    off = de["gulf_offset_h"]
    days = spark.range(1).select(F.explode(F.sequence(F.to_date(F.lit(run["start_date"])), F.to_date(F.lit(run["end_date"])))).alias("d"))
    origins = days.select(F.explode(F.array(*[F.lit(h) for h in product_cfg["origin_hours_gulf"]])).alias("origin_hour_gulf"), "d").select(
        "origin_hour_gulf",
        F.timestamp_seconds(F.unix_timestamp(F.col("d").cast("timestamp")) + (F.col("origin_hour_gulf") - off) * 3600).alias("origin"))
    return origins.crossJoin(spark.createDataFrame([(s,) for s in sites], "site string"))


def _attach_run(rows: DataFrame, origins: DataFrame, feat_runs: DataFrame, horizon: str, latency: int, max_age: float,
                cols, prefix: str, shift_min: int = 0) -> DataFrame:
    """Attach values of the latest run (issued `shift_min` before the origin, minus latency) for each target."""
    fh = feat_runs.where(F.col("horizon") == horizon)
    points = origins.select("site", "origin", shift_minutes("origin", -shift_min).alias("t"))
    a = asof_runs(points, fh.select("site", "reference_time").distinct(), latency)
    a = a.select("site", "origin", F.when(F.col("run_age_min") <= max_age, F.col("run_time")).alias(f"{prefix}_run_time"),
                 F.col("run_age_min").alias(f"{prefix}_run_age_min"))
    vals = fh.select("site", F.col("reference_time").alias(f"{prefix}_run_time"), "period_end", *[F.col(c).alias(f"{prefix}_{c}") for c in cols])
    return rows.join(a, ["site", "origin"], "left").join(vals, ["site", f"{prefix}_run_time", "period_end"], "left")


def assemble_features(origins: DataFrame, feat_runs: DataFrame, product_cfg: dict, fe: dict, sites: dict, de: dict) -> DataFrame:
    """Feature table without labels: Reuniwatt values per horizon, across-runs and across-horizons lags, computed features."""
    res, window, latency = product_cfg["resolution_min"], product_cfg["window_min"], product_cfg["latency_min"]
    main = product_cfg["main_horizon"]
    hz = product_cfg["horizons"]
    mp = hz[main]["prefix"]
    others = [h for h in hz if h != main]

    rows = origins.select("site", "origin", "origin_hour_gulf", F.explode(F.sequence(
        shift_minutes("origin", res), shift_minutes("origin", window), F.expr(f"INTERVAL {res} MINUTES"))).alias("period_end"))
    rows = rows.withColumn("lead_min", (F.unix_timestamp("period_end") - F.unix_timestamp("origin")) / 60) \
        .withColumn("step", (F.col("lead_min") / res).cast("int"))

    run_cols = [c for c in feat_runs.columns if c not in RUN_KEY]
    base_cols = fe["forecast_vars"] + ["csi_ghi", "kt", "diffuse_frac", "beam_frac"]

    # Main horizon: everything (incl. weather and around-target lags). Other horizons: base values only.
    rows = _attach_run(rows, origins, feat_runs, main, latency, hz[main]["max_run_age_min"], run_cols, mp)
    for h in others:
        rows = _attach_run(rows, origins, feat_runs, h, latency, hz[h]["max_run_age_min"], base_cols, hz[h]["prefix"])

    # Across runs: same target from main-horizon runs issued earlier, and the revision since then
    ar = fe["across_runs"]
    for o in ar["offsets_min"]:
        rows = _attach_run(rows, origins, feat_runs, main, latency, hz[main]["max_run_age_min"], ar["columns"], f"{mp}_r{o}", shift_min=o)
        for c in ar["columns"]:
            rows = rows.withColumn(f"{mp}_{c}_rev{o}", F.col(f"{mp}_{c}") - F.col(f"{mp}_r{o}_{c}"))

    # Across horizons: main minus each other horizon
    for h in others:
        op = hz[h]["prefix"]
        for c in fe["across_horizons"]["columns"]:
            rows = rows.withColumn(f"xh_{c}_{mp}_minus_{op}", F.col(f"{mp}_{c}") - F.col(f"{op}_{c}"))

    # Best available value (main horizon first)
    for c in base_cols:
        rows = rows.withColumn(f"f_{c}", F.coalesce(*[F.col(f"{hz[h]['prefix']}_{c}") for h in [main] + others]))

    # Computed: sun, calendar, capacity-normalised power, uncertainty, plant limit, panel heat
    cap = site_value(sites, "capacity_mw")
    off = de["gulf_offset_h"]
    rows = add_solar(rows, sites, "period_end", res)
    rows = add_calendar(rows, "period_end", "t_", off)
    rows = add_calendar(rows, "origin", "o_", off, with_doy=False)
    rows = (rows.withColumn("capacity_mw", cap)
            .withColumn("power_norm", F.col("f_power_mw") / cap)
            .withColumn("p10_norm", F.col("f_power_p10_mw") / cap)
            .withColumn("p90_norm", F.col("f_power_p90_mw") / cap)
            .withColumn("band_width_norm", (F.col("f_power_p90_mw") - F.col("f_power_p10_mw")) / cap)
            .withColumn("clip_flag", (F.col("f_power_mw") >= 0.97 * cap).cast("int"))
            .withColumn("t_cell_c", F.col(f"{mp}_temp_c") + 0.031 * F.col("f_gti_poa"))
            .withColumn("temp_derate", 1 - 0.004 * (F.col("t_cell_c") - 25))
            .withColumn("is_day_int", F.col("is_day").cast("int")))
    return rows


def add_labels(features: DataFrame, pri_target: DataFrame, product_cfg: dict) -> DataFrame:
    """Attach the label (target at the target time) and keep origins that had at least one usable run."""
    main_p = product_cfg["horizons"][product_cfg["main_horizon"]]["prefix"]
    lab = pri_target.select("site", "period_end", "label_target_mw", "label_actual_mw", "label_target_norm", "label_ok")
    out = features.join(lab, ["site", "period_end"], "left").withColumn("label_ok", F.coalesce("label_ok", F.lit(False)))
    run_cols = [f"{h['prefix']}_run_time" for h in product_cfg["horizons"].values()]
    any_run = F.coalesce(*[F.col(c) for c in run_cols]).isNotNull()
    print(f"    main-horizon prefix: {main_p}")
    return out.where(any_run)
