# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · DQ — Reuniwatt power forecasts
# MAGIC `ewec_dev_reuniwatt.silver.fact_solar_power_forecast` (and its `_backup`).
# MAGIC
# MAGIC Each row is one forecast **run** (`reference_time`, the issue time) for one **target period** (`period_end`), per **segment** = `site` × `provider` × `horizon` × `granularity_min`.
# MAGIC
# MAGIC | Group | Columns |
# MAGIC |---|---|
# MAGIC | Keys | `period_end`, `site`, `provider`, `horizon`, `reference_time`, `granularity_min` |
# MAGIC | Power (MW) | `power_mw` (point forecast), `power_p10_mw`, `power_p90_mw` (uncertainty band), `power_mw_original` (before adjustment) |
# MAGIC | Flags / metadata | `quality_flag`, `forecast_day_offset`, `_source`, `_ingested_at` |
# MAGIC
# MAGIC **Widgets.** `timestamps_tz` is the timezone used for the sun-position day/night split; section 12 tests both options. `pv_shift_h` shifts Bronze PV before the comparison in section 13, if a time offset is found. `scratch_schema` is optional.

# COMMAND ----------

dbutils.widgets.text("scratch_schema", "", "Scratch schema (optional, catalog.schema)")
dbutils.widgets.dropdown("timestamps_tz", "UTC", ["UTC", "Asia/Dubai"], "Reuniwatt timestamps are in")
dbutils.widgets.text("pv_shift_h", "0", "Shift Bronze PV by (hours)")

# COMMAND ----------

# MAGIC %run ./00_dq_utils

# COMMAND ----------

TABLE = f"{REUNIWATT_SILVER}.fact_solar_power_forecast"
IRR_TABLE = f"{REUNIWATT_SILVER}.fact_solar_irradiance"
tag = "reuniwatt_power"
notes_init(tag)
TZ = dbutils.widgets.get("timestamps_tz")
PV_SHIFT_H = int(dbutils.widgets.get("pv_shift_h"))

P = ["power_mw", "power_p10_mw", "power_p90_mw", "power_mw_original"]

raw = spark.table(TABLE)
raw.printSchema()
note("missing_columns", [c for c in FC_KEY + P + ["quality_flag", "forecast_day_offset", "_source", "_ingested_at"] if c not in raw.columns])
note("unexpected_columns", [c for c in raw.columns if c not in FC_KEY + P + ["quality_flag"] + FC_META])

df = add_solar_geometry(prep_forecast(raw), tz_offset_h=TZ_OFFSET_H[TZ])

# COMMAND ----------

# MAGIC %md ## 0. Context: dimensions, load history, backup

# COMMAND ----------

for d in ["dim_forecast_product", "dim_pv_site"]:
    display(save(spark.table(f"{REUNIWATT_SILVER}.{d}"), f"reuniwatt_{d}"))

# COMMAND ----------

display(save(spark.sql(f"DESCRIBE HISTORY {TABLE}").select("version", "timestamp", "operation", "operationParameters").limit(30), f"{tag}_history"))

# COMMAND ----------

bk_summary, bk_snaps = fc_backup_compare(df, f"{TABLE}_backup")
display(save(bk_summary, f"{tag}_backup_compare"))
if bk_snaps is not None:
    display(save(bk_snaps, f"{tag}_backup_snapshots"))

# COMMAND ----------

# MAGIC %md ## 1. Overview: segments, sources, quality flags, key nulls

# COMMAND ----------

display(save(fc_overview(df), f"{tag}_segments"))
display(save(df.groupBy("_source").agg(F.count("*").alias("rows"), F.min("reference_time").alias("first_issue"), F.max("reference_time").alias("last_issue"), F.collect_set("horizon").alias("horizons")), f"{tag}_sources"))
display(save(rule_counts(df, {f"null_{c}": (f"{c} IS NULL", [c]) for c in FC_KEY}), f"{tag}_key_nulls"))

# COMMAND ----------

qf = df.groupBy("horizon", "quality_flag").agg(F.count("*").alias("rows"))
qf = qf.withColumn("pct_of_horizon", F.round(100 * F.col("rows") / F.sum("rows").over(Window.partitionBy("horizon")), 2))
display(save(qf.orderBy("horizon", F.desc("rows")), f"{tag}_quality_flags"))

# COMMAND ----------

# MAGIC %md ## 2. Duplicate keys

# COMMAND ----------

dups = duplicate_timestamps(df, FC_KEY, P)
note("duplicate_keys", dup_note(dups))
display(dups.limit(50))

# COMMAND ----------

# MAGIC %md ## 3. Lead time and `forecast_day_offset`

# COMMAND ----------

display(save(fc_lead(df), f"{tag}_lead_time"))
dist, mismatch = fc_day_offset_check(df)
display(save(dist, f"{tag}_day_offset_dist"))
display(save(mismatch, f"{tag}_day_offset_mismatch"))

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

# MAGIC %md ## 7. Null rates by horizon and day/night

# COMMAND ----------

display(save(null_rates(df, P + ["quality_flag"], F.concat_ws(" / ", "horizon", "daypart")), f"{tag}_null_rates"))

# COMMAND ----------

# MAGIC %md ## 8. Value ranges and validity rules
# MAGIC The band must be ordered: `p10 ≤ power_mw ≤ p90`. Power must be ≥ 0, below the 1700 MW plausibility bound, and about 0 at night.

# COMMAND ----------

display(save(column_stats(df, P), f"{tag}_column_stats"))
display(save(df.groupBy("site").agg(*[F.percentile_approx(c, 0.999).alias(f"{c}_p999") for c in P], *[F.max(c).alias(f"{c}_max") for c in P]), f"{tag}_capacity_hint"))

# COMMAND ----------

rules = {}
for c in P:
    rules[f"{c}_negative"] = (f"{c} < 0", [c])
    rules[f"{c}_above_{PV_PLAUSIBLE_MAX_MW}MW"] = (f"{c} > {PV_PLAUSIBLE_MAX_MW}", [c])
    rules[f"{c}_at_night"] = (f"daypart = 'night' AND {c} > {DROPOUT_PV_MW}", [c])
rules.update({
    "p10_above_point": ("power_p10_mw > power_mw + 0.001", ["power_p10_mw", "power_mw"]),
    "point_above_p90": ("power_mw > power_p90_mw + 0.001", ["power_mw", "power_p90_mw"]),
    "p10_above_p90": ("power_p10_mw > power_p90_mw + 0.001", ["power_p10_mw", "power_p90_mw"]),
    "zero_width_band_daytime": (f"daypart = 'day' AND power_mw > {DROPOUT_PV_MW} AND power_p90_mw = power_p10_mw", ["power_p10_mw", "power_p90_mw"]),
    "band_null_but_point_present": ("power_mw IS NOT NULL AND (power_p10_mw IS NULL OR power_p90_mw IS NULL)", ["power_mw"]),
    "lead_time_negative": ("lead_min < 0", ["lead_min"]),
})
display(save(rule_counts(df, rules), f"{tag}_rules"))

# COMMAND ----------

# Sample violating rows: edit the condition
display(df.where("power_p10_mw > power_mw + 0.001 OR power_mw > power_p90_mw + 0.001").select(*FC_KEY, *P, "quality_flag").orderBy("period_end").limit(100))

# COMMAND ----------

# MAGIC %md ## 9. `power_mw` vs. `power_mw_original`, and `quality_flag`
# MAGIC `power_mw_original` appears to be the forecast before some adjustment (clipping, scaling or a fix). This section shows how often and by how much the two differ, and whether that lines up with `quality_flag`.
# MAGIC Unexplained differences, or differences with no flag, should be raised with Harish or Nitheesh.

# COMMAND ----------

diff = F.col("power_mw") - F.col("power_mw_original")
adj = df.groupBy("horizon", "quality_flag").agg(
    F.count("*").alias("rows"),
    F.sum((F.abs(diff) > 0.001).cast("int")).alias("rows_adjusted"),
    F.round(F.avg(diff), 3).alias("mean_diff_mw"),
    F.round(F.max(F.abs(diff)), 3).alias("max_abs_diff_mw"),
    F.round(F.percentile_approx(F.when(F.col("power_mw_original") > 1, F.col("power_mw") / F.col("power_mw_original")), 0.5), 4).alias("median_ratio"),
    F.sum((F.col("power_mw_original").isNull() & F.col("power_mw").isNotNull()).cast("int")).alias("original_null_only"),
    F.sum((F.col("power_mw").isNull() & F.col("power_mw_original").isNotNull()).cast("int")).alias("adjusted_null_only"),
).withColumn("adjusted_pct", F.round(100 * F.col("rows_adjusted") / F.col("rows"), 2)).orderBy("horizon", "quality_flag")
display(save(adj, f"{tag}_adjustments"))

# COMMAND ----------

display(df.where(F.abs(diff) > 0.001).select(*FC_KEY, "power_mw_original", "power_mw", "power_p10_mw", "power_p90_mw", "quality_flag", "daypart").orderBy(F.desc(F.abs(diff))).limit(100))

# COMMAND ----------

# MAGIC %md ## 10. Uncertainty band width
# MAGIC Daytime `p90 − p10`, by horizon and day offset. The band should **widen as lead time grows** (dayahead wider than intraday, weekahead widest). A band that doesn't grow, or one that is constant, suggests a placeholder.

# COMMAND ----------

width = F.col("power_p90_mw") - F.col("power_p10_mw")
bands = df.where(f"daypart = 'day' AND power_mw > {DROPOUT_PV_MW}").groupBy("site", "horizon", "forecast_day_offset").agg(
    F.count("*").alias("rows"),
    F.round(F.percentile_approx(width, 0.5), 2).alias("median_width_mw"),
    F.round(F.percentile_approx(width / F.when(F.col("power_mw") > 0, F.col("power_mw")), 0.5), 3).alias("median_width_rel_to_point"),
    F.countDistinct(F.round(width, 2)).alias("distinct_widths"),
).orderBy("site", "horizon", "forecast_day_offset")
display(save(bands, f"{tag}_band_width"))

# COMMAND ----------

# MAGIC %md ## 11. Stale runs

# COMMAND ----------

stale_summary, stale = fc_stale_runs(df, P, "power_mw")
display(save(stale_summary, f"{tag}_stale_runs"))
display(stale.limit(100))

# COMMAND ----------

# MAGIC %md ## 12. Timezone test
# MAGIC Count forecasts of > 1 MW at night when the timestamps are assumed to be UTC, then Asia/Dubai. The right timezone gives far fewer. The peak hour should be about 08 for UTC or about 12 for Asia/Dubai.

# COMMAND ----------

base = prep_forecast(raw)
tz_rows = [(tz, add_solar_geometry(base, tz_offset_h=off).where(f"daypart = 'night' AND power_mw > {DROPOUT_PV_MW}").count()) for tz, off in TZ_OFFSET_H.items()]
display(save(spark.createDataFrame(tz_rows, "assumed_tz string, night_rows_with_power long"), f"{tag}_timezone_test"))
display(save(peak_hour_by_site(df, "power_mw"), f"{tag}_peak_hours"))
note("timestamps_tz_used", TZ)
note("pv_shift_h_used", PV_SHIFT_H)

# COMMAND ----------

# MAGIC %md ## 13. Against actual PV (Bronze telemetry)
# MAGIC **Time offset first.** If the best offset is not 0, set `pv_shift_h` to minus that offset and re-run this section.
# MAGIC
# MAGIC **Site mapping.** Reuniwatt `pv1` should correlate clearly better with one Bronze column. The Bronze→Silver swap means Silver `PV1` = Bronze `PV2_MW`.
# MAGIC
# MAGIC **Accuracy (daytime, all runs).** Results are split by horizon and `forecast_day_offset`, so you can see error grow with lead time.
# MAGIC - `nmae_pct` is MAE as a % of the observed max PV, a stand-in for capacity.
# MAGIC - `band_coverage_pct` is how often the actual falls inside p10–p90. It should be about **80%**: much lower means the band is too narrow (overconfident), much higher means too wide.
# MAGIC - `mae_original` vs `mae` shows whether the adjustment helped.
# MAGIC
# MAGIC This is a data sanity check, not a model evaluation. Actuals include outages and curtailment, which notebook 01 identifies.

# COMMAND ----------

latest = fc_latest(df)
lag = lag_scan_vs_pv(latest, "power_mw")
display(save(lag.orderBy("site", "pv_col", "offset_h"), f"{tag}_lag_scan_vs_pv"))
display(save(best_rows(lag, ["site", "pv_col"], "corr"), f"{tag}_lag_best"))

# COMMAND ----------

mapping = best_rows(lag.where("offset_h = 0"), ["site"], "corr").select("site", "pv_col", "corr")
display(save(mapping, f"{tag}_site_mapping"))
SITE_TO_PV = {r["site"]: r["pv_col"] for r in mapping.collect()}

# COMMAND ----------

grans = [int(r[0]) for r in df.select("granularity_min").distinct().collect()]
parts = []
for gm in grans:
    obs = pv_observed(gm, PV_SHIFT_H)
    cap = {c: (obs.agg(F.max(c)).first()[0] or None) for c in PV_COLS}  # None (not 0) so ANSI mode never divides by zero
    for site, pv_col in SITE_TO_PV.items():
        j = df.where((F.col("granularity_min") == gm) & (F.col("site") == site) & (F.col("daypart") == "day")) \
            .join(obs.select("period_end", F.col(pv_col).alias("actual")), "period_end").where("actual IS NOT NULL")
        err = F.col("power_mw") - F.col("actual")
        parts.append(j.groupBy(*FC_SEG, "forecast_day_offset").agg(
            F.count("*").alias("n"),
            F.round(F.corr("power_mw", "actual"), 4).alias("corr"),
            F.round(F.avg(err), 2).alias("bias_mw"),
            F.round(F.avg(F.abs(err)), 2).alias("mae"),
            F.round(F.avg(F.abs(F.col("power_mw_original") - F.col("actual"))), 2).alias("mae_original"),
            F.round(100 * F.avg(((F.col("actual") >= F.col("power_p10_mw")) & (F.col("actual") <= F.col("power_p90_mw"))).cast("int")), 1).alias("band_coverage_pct"),
        ).withColumn("pv_col", F.lit(pv_col)).withColumn("nmae_pct", F.round(100 * F.col("mae") / F.lit(cap[pv_col]), 2)))
accuracy = reduce(DataFrame.unionByName, parts).orderBy("site", "horizon", "granularity_min", "forecast_day_offset")
display(save(accuracy, f"{tag}_accuracy_vs_pv"))

# COMMAND ----------

# MAGIC %md ## 14. Consistency with the irradiance table
# MAGIC Power and irradiance forecasts should come from the **same runs**. Runs found in only one table mean incomplete loads.
# MAGIC Daytime correlation of power with GTI and GHI on matching rows should be high (≥ 0.9). A low value means the tables are misaligned, or the sites are labelled differently.

# COMMAND ----------

irr = prep_forecast(spark.table(IRR_TABLE))
run_key = FC_SEG + ["reference_time"]
rp, ri = df.select(*run_key).distinct(), irr.select(*run_key).distinct()
display(save(rp.join(ri, run_key, "left_anti").groupBy(*FC_SEG).agg(F.count("*").alias("runs_only_in_power"))
        .join(ri.join(rp, run_key, "left_anti").groupBy(*FC_SEG).agg(F.count("*").alias("runs_only_in_irradiance")), FC_SEG, "full")
        .fillna(0).orderBy(*FC_SEG), f"{tag}_run_overlap_with_irradiance"))

j = df.where("daypart = 'day'").join(irr.select(*FC_KEY, "ghi_wm2", "gti_wm2"), FC_KEY)
display(save(j.groupBy("site", "horizon").agg(
    F.count("*").alias("matched_rows"),
    F.round(F.corr("power_mw", "gti_wm2"), 4).alias("corr_power_gti"),
    F.round(F.corr("power_mw", "ghi_wm2"), 4).alias("corr_power_ghi"),
).orderBy("site", "horizon"), f"{tag}_vs_irradiance"))
