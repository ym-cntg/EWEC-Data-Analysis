# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · DQ — Reuniwatt irradiance forecasts
# MAGIC `ewec_dev_reuniwatt.silver.fact_solar_irradiance` (and its `_backup`). This is the new Reuniwatt weather source, called "renewal" in meetings.
# MAGIC
# MAGIC Each row is one forecast **run** (`reference_time`, the issue time) for one **target period** (`period_end`), per **segment** = `site` × `provider` × `horizon` × `granularity_min`.
# MAGIC
# MAGIC | Group | Columns |
# MAGIC |---|---|
# MAGIC | Keys | `period_end`, `site`, `provider`, `horizon`, `reference_time`, `granularity_min` |
# MAGIC | Irradiance (W/m²) | `ghi_wm2`, `dni_wm2`, `dhi_wm2`, `bhi_wm2`, `gti_wm2`, `gti_east_wm2`, `gti_west_wm2` |
# MAGIC | Weather | `ambient_temp_c`, `dewpoint_c`, `humidity_pct`, `wind_speed`, `wind_dir`, `cloud_opacity_pct`, `pressure_hpa`, `precip_rate_mmh`, `clearness_idx_pct` |
# MAGIC | Metadata | `forecast_day_offset`, `_source`, `_ingested_at` |
# MAGIC
# MAGIC **Widgets.** `timestamps_tz` is the timezone the timestamps are assumed to be in when computing sun position (day/night). Section 10 tests both options; set this to whichever wins and re-run.
# MAGIC `solcast_shift_h` shifts Solcast before the comparison in section 12, if notebook 04 found an offset. `scratch_schema` is optional.

# COMMAND ----------

dbutils.widgets.text("scratch_schema", "", "Scratch schema (optional, catalog.schema)")
dbutils.widgets.dropdown("timestamps_tz", "UTC", ["UTC", "Asia/Dubai"], "Reuniwatt timestamps are in")
dbutils.widgets.text("solcast_shift_h", "0", "Shift Solcast by (hours)")

# COMMAND ----------

# MAGIC %run ./00_dq_utils

# COMMAND ----------

TABLE = f"{REUNIWATT_SILVER}.fact_solar_irradiance"
tag = "reuniwatt_irradiance"
TZ = dbutils.widgets.get("timestamps_tz")
SOLCAST_SHIFT_H = int(dbutils.widgets.get("solcast_shift_h"))

IRR = ["ghi_wm2", "dni_wm2", "dhi_wm2", "bhi_wm2", "gti_wm2", "gti_east_wm2", "gti_west_wm2"]
MET = ["ambient_temp_c", "dewpoint_c", "humidity_pct", "wind_speed", "wind_dir", "cloud_opacity_pct", "pressure_hpa", "precip_rate_mmh", "clearness_idx_pct"]
VALUES = IRR + MET

raw = spark.table(TABLE)
raw.printSchema()
print("Expected columns missing:", [c for c in FC_KEY + VALUES + ["forecast_day_offset", "_source", "_ingested_at"] if c not in raw.columns])
print("Unexpected columns:", [c for c in raw.columns if c not in FC_KEY + VALUES + FC_META])

df = add_solar_geometry(prep_forecast(raw), tz_offset_h=TZ_OFFSET_H[TZ])

# COMMAND ----------

# MAGIC %md ## 0. Context: dimensions, load history, backup

# COMMAND ----------

for d in ["dim_forecast_product", "dim_pv_site"]:
    print(d)
    display(spark.table(f"{REUNIWATT_SILVER}.{d}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABLE}").select("version", "timestamp", "operation", "operationParameters", "operationMetrics").limit(30))

# COMMAND ----------

bk_summary, bk_snaps = fc_backup_compare(df, f"{TABLE}_backup")
display(bk_summary)
display(bk_snaps)

# COMMAND ----------

# MAGIC %md ## 1. Overview: segments, sources, key nulls

# COMMAND ----------

display(save(fc_overview(df), f"{tag}_segments"))
display(df.groupBy("_source").agg(F.count("*").alias("rows"), F.min("reference_time").alias("first_issue"), F.max("reference_time").alias("last_issue"), F.collect_set("horizon").alias("horizons")))
display(rule_counts(df, {f"null_{c}": (f"{c} IS NULL", [c]) for c in FC_KEY}))

# COMMAND ----------

# MAGIC %md ## 2. Duplicate keys

# COMMAND ----------

dups = duplicate_timestamps(df, FC_KEY, ["ghi_wm2", "dni_wm2", "dhi_wm2", "ambient_temp_c"])
print("Duplicate keys:", dups.count(), "| conflicting values:", dups.where("distinct_value_sets > 1").count())
display(dups.limit(50))

# COMMAND ----------

# MAGIC %md ## 3. Lead time and `forecast_day_offset`

# COMMAND ----------

display(save(fc_lead(df), f"{tag}_lead_time"))
dist, mismatch = fc_day_offset_check(df)
display(dist)
display(mismatch)

# COMMAND ----------

# MAGIC %md ## 4. Issue cadence and ingestion delay

# COMMAND ----------

cad_summary, cad_gaps = fc_cadence(df)
display(save(cad_summary, f"{tag}_cadence"))
display(save(cad_gaps, f"{tag}_missing_runs").limit(200))
display(save(fc_ingestion(df), f"{tag}_ingestion"))

# COMMAND ----------

# MAGIC %md ## 5. Run completeness

# COMMAND ----------

run_summary, short_runs = fc_run_completeness(df)
display(save(run_summary, f"{tag}_run_completeness"))
display(short_runs.limit(200))

# COMMAND ----------

# MAGIC %md ## 6. Target-time coverage

# COMMAND ----------

cov, low_days = fc_coverage(df)
display(save(cov, f"{tag}_coverage"))
display(save(low_days, f"{tag}_low_coverage_days"))

# COMMAND ----------

# MAGIC %md ## 7. Null rates by horizon and day/night (computed sun position)

# COMMAND ----------

display(save(null_rates(df, VALUES, F.concat_ws(" / ", "horizon", "daypart")), f"{tag}_null_rates"))

# COMMAND ----------

# MAGIC %md ## 8. Value ranges and validity rules

# COMMAND ----------

display(save(column_stats(df, VALUES), f"{tag}_column_stats"))

# COMMAND ----------

IRR_MAX = {"ghi_wm2": 1400, "dni_wm2": 1200, "dhi_wm2": 800, "bhi_wm2": 1200, "gti_wm2": 1500, "gti_east_wm2": 1500, "gti_west_wm2": 1500}
rules = {}
for c, mx in IRR_MAX.items():
    rules[f"{c}_negative"] = (f"{c} < 0", [c])
    rules[f"{c}_above_{mx}"] = (f"{c} > {mx}", [c])
    rules[f"{c}_at_night"] = (f"daypart = 'night' AND {c} > 5", [c])
rules.update({
    "ghi_above_extraterrestrial": ("daypart <> 'night' AND ghi_wm2 > etr_horizontal * 1.05 + 20", ["ghi_wm2"]),
    "ambient_temp_out_of_-5_60": ("ambient_temp_c < -5 OR ambient_temp_c > 60", ["ambient_temp_c"]),
    "dewpoint_out_of_-40_40": ("dewpoint_c < -40 OR dewpoint_c > 40", ["dewpoint_c"]),
    "dewpoint_above_ambient_temp": ("dewpoint_c > ambient_temp_c + 0.5", ["dewpoint_c", "ambient_temp_c"]),
    "humidity_out_of_0_100": ("humidity_pct < 0 OR humidity_pct > 100", ["humidity_pct"]),
    "cloud_opacity_out_of_0_100": ("cloud_opacity_pct < 0 OR cloud_opacity_pct > 100", ["cloud_opacity_pct"]),
    "clearness_out_of_0_130": ("clearness_idx_pct < 0 OR clearness_idx_pct > 130", ["clearness_idx_pct"]),
    "wind_speed_out_of_0_50": ("wind_speed < 0 OR wind_speed > 50", ["wind_speed"]),
    "wind_dir_out_of_0_360": ("wind_dir < 0 OR wind_dir > 360", ["wind_dir"]),
    "pressure_out_of_900_1100": ("pressure_hpa < 900 OR pressure_hpa > 1100", ["pressure_hpa"]),
    "precip_out_of_0_200": ("precip_rate_mmh < 0 OR precip_rate_mmh > 200", ["precip_rate_mmh"]),
    "lead_time_negative": ("lead_min < 0", ["lead_min"]),
})
display(save(rule_counts(df, rules), f"{tag}_rules"))

# COMMAND ----------

# Sample violating rows: edit the condition
display(df.where("daypart = 'night' AND ghi_wm2 > 5").select(*FC_KEY, "sun_zenith", *IRR).orderBy("period_end").limit(100))

# COMMAND ----------

# MAGIC %md ## 9. Physical consistency between columns (daytime)
# MAGIC - **Closure:** `ghi ≈ bhi + dhi`. `bhi` is beam on the horizontal plane, so this should hold almost exactly.
# MAGIC - **Beam geometry:** `bhi ≈ dni · cos(sun_zenith)`, using the computed sun position.
# MAGIC - **Clearness:** `clearness_idx_pct ≈ 100 · ghi / extraterrestrial horizontal`.
# MAGIC - **Humidity:** `humidity_pct` should agree with `ambient_temp_c` and `dewpoint_c` (Magnus formula).
# MAGIC
# MAGIC Large residuals mean the columns were derived inconsistently, or are mislabelled or misaligned in time.

# COMMAND ----------

def kt_calc():
    """Clearness index (%) = GHI / extraterrestrial horizontal; null when the sun is down."""
    return 100 * F.col("ghi_wm2") / F.when(F.col("etr_horizontal") > 0, F.col("etr_horizontal"))


def magnus(t):
    return F.exp(17.625 * t / (243.04 + t))

day = df.where("daypart = 'day'")
consistency = day.groupBy("horizon").agg(
    F.count("*").alias("day_rows"),
    F.percentile_approx(F.abs(F.col("ghi_wm2") - F.col("bhi_wm2") - F.col("dhi_wm2")), [0.5, 0.95]).alias("ghi_closure_abs_p50_p95"),
    F.sum((F.abs(F.col("ghi_wm2") - F.col("bhi_wm2") - F.col("dhi_wm2")) > F.greatest(F.lit(20), 0.05 * F.col("ghi_wm2"))).cast("int")).alias("ghi_closure_violations"),
    F.percentile_approx(F.abs(F.col("bhi_wm2") - F.col("dni_wm2") * F.cos(F.radians("sun_zenith"))), [0.5, 0.95]).alias("bhi_vs_dni_cosz_abs_p50_p95"),
    F.percentile_approx(F.abs(F.col("clearness_idx_pct") - kt_calc()), [0.5, 0.95]).alias("clearness_abs_p50_p95"),
).orderBy("horizon")
display(save(consistency, f"{tag}_consistency"))

rh_calc = 100 * magnus(F.col("dewpoint_c")) / magnus(F.col("ambient_temp_c"))
display(df.groupBy("horizon").agg(
    F.percentile_approx(F.abs(F.col("humidity_pct") - rh_calc), [0.5, 0.95]).alias("rh_vs_dewpoint_abs_p50_p95"),
    F.sum((F.abs(F.col("humidity_pct") - rh_calc) > 10).cast("int")).alias("rh_off_by_more_than_10pts"),
).orderBy("horizon"))

# COMMAND ----------

# MAGIC %md ### East/west tilted irradiance
# MAGIC `gti_east_wm2` should peak in the morning and `gti_west_wm2` in the afternoon. If they're the other way round, the columns are swapped.

# COMMAND ----------

gti_prof = df.groupBy("site", F.hour("period_end").alias("hour")).agg(*[F.avg(c).alias(c) for c in ["ghi_wm2", "gti_wm2", "gti_east_wm2", "gti_west_wm2"]]).orderBy("site", "hour")
display(gti_prof)
for c in ["ghi_wm2", "gti_east_wm2", "gti_west_wm2"]:
    display(peak_hour_by_site(df, c).withColumn("column", F.lit(c)))

# COMMAND ----------

# MAGIC %md ## 10. Timezone test
# MAGIC Recompute sun position assuming the timestamps are UTC, then Asia/Dubai. The right timezone gives **far fewer night-irradiance rows** and **smaller beam/clearness residuals**.
# MAGIC If `timestamps_tz` doesn't match the winner, change it and re-run the notebook. Section 9 also rules out the clear-sky index if neither option fits `clearness_idx_pct`.

# COMMAND ----------

tz_rows = []
base = prep_forecast(raw)
for tz, off in TZ_OFFSET_H.items():
    g = add_solar_geometry(base, tz_offset_h=off)
    d = g.where("sun_zenith < 80 AND ghi_wm2 > 20").agg(
        F.percentile_approx(F.abs(F.col("bhi_wm2") - F.col("dni_wm2") * F.cos(F.radians("sun_zenith"))), 0.5).alias("bhi"),
        F.percentile_approx(F.abs(F.col("clearness_idx_pct") - kt_calc()), 0.5).alias("kt"),
    ).first()
    night = g.where("daypart = 'night' AND ghi_wm2 > 5").count()
    tz_rows.append((tz, night, d["bhi"], d["kt"]))
tz_test = spark.createDataFrame(tz_rows, "assumed_tz string, night_rows_with_ghi long, median_abs_bhi_vs_dni_cosz double, median_abs_clearness_diff double")
display(save(tz_test, f"{tag}_timezone_test"))
print("Peak-hour check (about 08 = UTC, about 12 = Asia/Dubai):")
display(peak_hour_by_site(df, "ghi_wm2"))

# COMMAND ----------

# MAGIC %md ## 11. Stale runs

# COMMAND ----------

stale_summary, stale = fc_stale_runs(df, IRR + ["ambient_temp_c", "cloud_opacity_pct"], "ghi_wm2")
display(save(stale_summary, f"{tag}_stale_runs"))
display(stale.limit(100))

# COMMAND ----------

# MAGIC %md ## 12. Against Solcast and PV (latest run per target)
# MAGIC Each Reuniwatt variable is compared with its Solcast equivalent, for **both** Solcast sites, averaged to the same granularity (bucket-end labels).
# MAGIC Irradiance pairs use daytime rows only. Expect high `corr`. `bias` shows systematic over- or under-forecasting against Solcast; Solcast is not ground truth.
# MAGIC Reuniwatt `pv1` should match one Solcast site clearly better than the other; that gives the site mapping.

# COMMAND ----------

PAIRS = [  # (reuniwatt, solcast, daytime only)
    ("ghi_wm2", "ghi", True), ("dni_wm2", "dni", True), ("dhi_wm2", "dhi", True), ("gti_wm2", "gti", True),
    ("ambient_temp_c", "air_temp", False), ("dewpoint_c", "dewpoint_temp", False), ("humidity_pct", "relative_humidity", False),
    ("wind_speed", "wind_speed_10m", False), ("cloud_opacity_pct", "cloud_opacity", False), ("precip_rate_mmh", "precipitation_rate", False),
]
latest = fc_latest(df)
grans = [int(r[0]) for r in df.select("granularity_min").distinct().collect()]
parts = []
for gm in grans:
    fc = latest.where(F.col("granularity_min") == gm)
    for s, t in SOLCAST_TABLES.items():
        sc_raw = spark.table(t)
        pairs = [(rc, scc, day_only) for rc, scc, day_only in PAIRS if scc in sc_raw.columns]
        sc = sc_raw.select(
            F.timestamp_seconds(F.unix_timestamp(F.col("period_end").cast("timestamp")) + SOLCAST_SHIFT_H * 3600).alias("ts"),
            *[F.col(scc).alias(f"sc_{scc}") for _, scc, _ in pairs],
        )
        j = fc.join(bucket_end(sc, "ts", [f"sc_{scc}" for _, scc, _ in pairs], gm), "period_end")
        aggs = []
        for rc, scc, day_only in pairs:
            x = F.when(F.col("daypart") == "day", F.col(rc)) if day_only else F.col(rc)
            y = F.col(f"sc_{scc}")
            aggs += [F.count(F.when(x.isNotNull() & y.isNotNull(), 1)).alias(f"{rc}__n"), F.corr(x, y).alias(f"{rc}__corr"),
                     F.avg(x - y).alias(f"{rc}__bias"), F.avg(F.abs(x - y)).alias(f"{rc}__mae")]
        wide = j.groupBy(*FC_SEG).agg(*aggs)
        long = wide.select(*FC_SEG, F.explode(F.array(*[F.struct(
            F.lit(rc).alias("reuniwatt_col"), F.lit(scc).alias("solcast_col"), F.col(f"{rc}__n").alias("n"),
            F.round(F.col(f"{rc}__corr"), 4).alias("corr"), F.round(F.col(f"{rc}__bias"), 2).alias("bias"), F.round(F.col(f"{rc}__mae"), 2).alias("mae"),
        ) for rc, scc, _ in pairs])).alias("x")).select(*FC_SEG, "x.*")
        parts.append(long.withColumn("solcast_site", F.lit(s)))
vs_solcast = reduce(DataFrame.unionByName, parts)
display(save(vs_solcast.orderBy("reuniwatt_col", *FC_SEG, "solcast_site"), f"{tag}_vs_solcast"))

# COMMAND ----------

print("Site mapping: GHI correlation of each Reuniwatt site with each Solcast site (best first)")
display(vs_solcast.where("reuniwatt_col = 'ghi_wm2'").groupBy("site", "solcast_site").agg(F.round(F.avg("corr"), 4).alias("avg_corr")).orderBy("site", F.desc("avg_corr")))

# COMMAND ----------

# MAGIC %md ### Time offset of GHI vs. Bronze PV (hourly, ±6 h)
# MAGIC Best `offset_h` should be 0. ±4 h means a UTC vs. Asia/Dubai mismatch between Reuniwatt and the PV telemetry. The best `pv_col` per site hints at the site mapping.

# COMMAND ----------

lag = lag_scan_vs_pv(latest, "ghi_wm2")
display(save(lag.orderBy("site", "pv_col", "offset_h"), f"{tag}_lag_scan_vs_pv"))
display(best_rows(lag, ["site", "pv_col"], "corr"))
