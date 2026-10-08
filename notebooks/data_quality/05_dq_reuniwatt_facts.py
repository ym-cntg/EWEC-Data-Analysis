# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · DQ — Reuniwatt forecast fact tables
# MAGIC `ewec_dev_reuniwatt.silver.fact_solar_irradiance` and `fact_solar_power_forecast`: the new Reuniwatt source ("renewal" in meetings).
# MAGIC
# MAGIC These are **forecast** tables. Each row is one forecast **run** (`reference_time` = issue time) for one **target period** (`period_end`), per `site` × `provider` × `horizon` (intraday / dayahead / weekahead) × `granularity_min`.
# MAGIC That means quality is judged **per run** (cadence, steps, lead times, staleness), not per timestamp.
# MAGIC
# MAGIC Pick the table with the `fact_table` widget and run once for each. Read-only; set `scratch_schema` to persist summaries.

# COMMAND ----------

dbutils.widgets.text("scratch_schema", "", "Scratch schema (optional, catalog.schema)")
dbutils.widgets.dropdown("fact_table", "fact_solar_irradiance", ["fact_solar_irradiance", "fact_solar_power_forecast"], "Fact table")
dbutils.widgets.text("main_col", "", "Main value column (blank = auto)")

# COMMAND ----------

# MAGIC %run ./00_dq_utils

# COMMAND ----------

import re

FACT = dbutils.widgets.get("fact_table")
TABLE = f"{REUNIWATT_SILVER}.{FACT}"
tag = f"reuniwatt_{FACT}"


def prep(d: DataFrame) -> DataFrame:
    for c in ("period_end", "reference_time"):
        if c in d.columns:
            d = d.withColumn(c, F.col(c).cast("timestamp"))
    if "site" in d.columns:
        d = d.withColumn("site", F.lower(F.trim("site")))
    return d


raw = spark.table(TABLE)
raw.printSchema()
df = prep(raw)

KEY = present(df, ["site", "provider", "horizon", "granularity_min", "reference_time", "period_end"])
SEG = present(df, ["site", "provider", "horizon", "granularity_min"])
VALUE_COLS = [c for c in numeric_cols(df, KEY) if not c.lower().endswith(("_id", "_key"))]
HAS_RUNS = {"reference_time", "period_end"} <= set(df.columns)

power_like = [c for c in VALUE_COLS if re.search(r"power|_mw$|^mw|^pv", c.lower())]
MAIN = dbutils.widgets.get("main_col").strip() or ("ghi" if "ghi" in VALUE_COLS else (power_like or VALUE_COLS)[0])
IS_POWER = MAIN in power_like

print("Key columns:", KEY, "| missing:", [c for c in ["site", "provider", "horizon", "granularity_min", "reference_time", "period_end"] if c not in KEY])
print("Segment columns:", SEG)
print("Value columns:", VALUE_COLS)
print(f"Main column: {MAIN} ({'power' if IS_POWER else 'irradiance/other'})")

if HAS_RUNS:
    df = df.withColumn("lead_min", (F.unix_timestamp("period_end") - F.unix_timestamp("reference_time")) / 60)

# COMMAND ----------

# MAGIC %md ## 0. Context: dimensions, load history, backup table

# COMMAND ----------

for d in ["dim_forecast_product", "dim_pv_site"]:
    try:
        print(d)
        display(spark.table(f"{REUNIWATT_SILVER}.{d}"))
    except Exception as e:
        print(f"{d}: {e}")

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {TABLE}"))
display(spark.sql(f"DESCRIBE HISTORY {TABLE}").select("version", "timestamp", "operation", "operationParameters", "operationMetrics").limit(30))

# COMMAND ----------

# Compare with the _backup copy: same keys? Rows that disappeared or appeared?
try:
    bk = prep(spark.table(f"{TABLE}_backup"))
    keys_main, keys_bk = df.select(*KEY).distinct(), bk.select(*present(bk, KEY)).distinct()
    display(spark.createDataFrame([(
        df.count(), bk.count(),
        keys_main.join(keys_bk, KEY, "left_anti").count(),
        keys_bk.join(keys_main, KEY, "left_anti").count(),
    )], "rows_main long, rows_backup long, keys_only_in_main long, keys_only_in_backup long"))
    print("Columns only in main:", sorted(set(raw.columns) - set(bk.columns)), "| only in backup:", sorted(set(bk.columns) - set(raw.columns)))
except Exception as e:
    print("Backup comparison skipped:", e)

# COMMAND ----------

# MAGIC %md ## 1. Segments overview
# MAGIC One row per site × provider × horizon × granularity: how many runs, and the issue- and target-time ranges.

# COMMAND ----------

overview = df.groupBy(*SEG).agg(
    F.count("*").alias("rows"),
    *([F.countDistinct("reference_time").alias("runs"), F.min("reference_time").alias("first_issue"), F.max("reference_time").alias("last_issue")] if HAS_RUNS else []),
    F.min("period_end").alias("first_period"),
    F.max("period_end").alias("last_period"),
).orderBy(*SEG)
display(save(overview, f"{tag}_segments"))

key_nulls = {f"null_{c}": (f"{c} IS NULL", [c]) for c in KEY}
display(rule_counts(df, key_nulls))

# COMMAND ----------

# MAGIC %md ## 2. Duplicate keys
# MAGIC The key is `site, provider, horizon, granularity_min, reference_time, period_end`. Each combination should appear once.

# COMMAND ----------

dups = duplicate_timestamps(df, KEY, VALUE_COLS[:5])
print("Duplicate keys:", dups.count(), "| conflicting values:", dups.where("distinct_value_sets > 1").count())
display(dups.limit(50))

# COMMAND ----------

# MAGIC %md ## 3. Lead time (`period_end − reference_time`)
# MAGIC Lead time should be **positive** and fit the horizon: intraday up to hours, dayahead about 1–2 days, weekahead up to about 7 days.
# MAGIC Lead ≤ 0 means the row targets the past (a hindcast or nowcast). If lead is always 0, `period_end` is really the issue time; its column comment says so.

# COMMAND ----------

if HAS_RUNS:
    lead = df.groupBy(*SEG).agg(
        F.min("lead_min").alias("min_lead_min"),
        F.percentile_approx("lead_min", 0.5).alias("median_lead_min"),
        F.max("lead_min").alias("max_lead_min"),
        F.round(F.max("lead_min") / 1440, 2).alias("max_lead_days"),
        F.sum((F.col("lead_min") <= 0).cast("int")).alias("rows_lead_le_0"),
        F.sum((F.col("lead_min") < 0).cast("int")).alias("rows_lead_lt_0"),
    ).orderBy(*SEG)
    display(save(lead, f"{tag}_lead_time"))

# COMMAND ----------

# MAGIC %md ## 4. Issue cadence: are runs arriving on schedule?
# MAGIC `cadence_min` is the usual time between runs. Gaps longer than 1.5× cadence are **missing runs**.

# COMMAND ----------

if HAS_RUNS:
    g = _with_gap(df, "reference_time", SEG)
    cadence = (
        g.groupBy(*SEG, "gap_min").count()
        .withColumn("rk", F.row_number().over(Window.partitionBy(*SEG).orderBy(F.desc("count"))))
        .where("rk = 1").select(*SEG, F.col("gap_min").alias("cadence_min"))
    )
    issue_gaps = g.join(cadence, SEG).where("gap_min > 1.5 * cadence_min").select(
        *SEG, "cadence_min", F.col("prev_ts").alias("last_run_before_gap"), F.col("reference_time").alias("first_run_after_gap"),
        F.round(F.col("gap_min") / F.col("cadence_min") - 1).alias("approx_missing_runs"),
    )
    display(save(
        cadence.join(issue_gaps.groupBy(*SEG).agg(F.count("*").alias("gaps"), F.sum("approx_missing_runs").alias("approx_missing_runs")), SEG, "left")
        .fillna(0, ["gaps", "approx_missing_runs"]).orderBy(*SEG),
        f"{tag}_cadence",
    ))
    display(save(issue_gaps.orderBy(F.desc("approx_missing_runs")), f"{tag}_missing_runs").limit(200))

# COMMAND ----------

# MAGIC %md ## 5. Run completeness: does every run have all its steps?
# MAGIC `expected_steps` is the median number of target periods per run in that segment. Short runs are truncated deliveries.
# MAGIC `irregular_steps` counts consecutive `period_end` values inside a run that are not exactly `granularity_min` apart.

# COMMAND ----------

if HAS_RUNS:
    runs = df.groupBy(*SEG, "reference_time").agg(
        F.countDistinct("period_end").alias("steps"),
        F.min("lead_min").alias("min_lead_min"),
        F.max("lead_min").alias("max_lead_min"),
    )
    expected = runs.groupBy(*SEG).agg(F.percentile_approx("steps", 0.5).alias("expected_steps"))
    runs = runs.join(expected, SEG)
    run_summary = runs.groupBy(*SEG, "expected_steps").agg(
        F.count("*").alias("runs"),
        F.sum((F.col("steps") < F.col("expected_steps")).cast("int")).alias("short_runs"),
        F.sum((F.col("steps") > F.col("expected_steps")).cast("int")).alias("long_runs"),
        F.min("steps").alias("min_steps"),
    ).withColumn("short_pct", F.round(100 * F.col("short_runs") / F.col("runs"), 2))

    if "granularity_min" in df.columns:
        w = Window.partitionBy(*SEG, "reference_time").orderBy("period_end")
        irregular = (
            df.select(*SEG, "reference_time", "period_end").distinct()
            .withColumn("step_min", (F.unix_timestamp("period_end") - F.unix_timestamp(F.lag("period_end").over(w))) / 60)
            .where("step_min IS NOT NULL AND step_min != granularity_min")
            .groupBy(*SEG).agg(F.count("*").alias("irregular_steps"))
        )
        run_summary = run_summary.join(irregular, SEG, "left").fillna(0, ["irregular_steps"])
    display(save(run_summary.orderBy(*SEG), f"{tag}_run_completeness"))

# COMMAND ----------

if HAS_RUNS:
    display(runs.where("steps < expected_steps").orderBy(*SEG, "reference_time").limit(200))

# COMMAND ----------

# MAGIC %md ## 6. Target-time coverage
# MAGIC Is every `period_end` between the first and last target covered by at least one run? Intraday products may cover **daylight only**; check whether the low days are night-only gaps.

# COMMAND ----------

cov = df.groupBy(*SEG).agg(F.min("period_end").alias("first_period"), F.max("period_end").alias("last_period"), F.countDistinct("period_end").alias("periods"))
if "granularity_min" in df.columns:
    cov = cov.withColumn(
        "expected_periods", ((F.unix_timestamp("last_period") - F.unix_timestamp("first_period")) / 60 / F.col("granularity_min") + 1).cast("long")
    ).withColumn("coverage_pct", F.round(100 * F.col("periods") / F.col("expected_periods"), 2))
display(save(cov.orderBy(*SEG), f"{tag}_coverage"))

# COMMAND ----------

if "granularity_min" in df.columns:
    per_day = df.groupBy(*SEG, F.to_date("period_end").alias("date")).agg(F.countDistinct("period_end").alias("periods")) \
        .withColumn("expected", (1440 / F.col("granularity_min")).cast("int")) \
        .withColumn("pct", F.round(100 * F.col("periods") / F.col("expected"), 1))
    print("Target days below 99% coverage (first and last day of each segment are naturally partial):")
    display(save(per_day.where("pct < 99").orderBy(*SEG, "date"), f"{tag}_low_coverage_days"))

# COMMAND ----------

# MAGIC %md ## 7. Null rates by horizon and day/night

# COMMAND ----------

if "zenith" in df.columns:
    dn = F.when(F.col("zenith").isNull(), "unknown").when(F.col("zenith") < 90, "day").otherwise("night")
elif "clearsky_ghi" in df.columns:
    dn = F.when(F.col("clearsky_ghi") > DAYTIME_CLEARSKY_GHI, "day").otherwise("night")
else:
    dn = F.lit("all")
period = F.concat_ws(" / ", F.col("horizon"), dn) if "horizon" in df.columns else dn
display(save(null_rates(df, VALUE_COLS, period), f"{tag}_null_rates"))

# COMMAND ----------

# MAGIC %md ## 8. Validity
# MAGIC Value ranges, physical rules for irradiance, bounds for power, and quantile ordering (for example p10 ≤ p50 ≤ p90) if the table has quantile columns.

# COMMAND ----------

display(save(column_stats(df, VALUE_COLS), f"{tag}_column_stats"))

# COMMAND ----------

rules = weather_rules(df)
for c in power_like:
    rules[f"{c}_negative"] = (f"{c} < 0", [c])
    rules[f"{c}_above_{PV_PLAUSIBLE_MAX_MW}MW"] = (f"{c} > {PV_PLAUSIBLE_MAX_MW}", [c])
    rules[f"{c}_at_night"] = (f"zenith > 90 AND {c} > {DROPOUT_PV_MW}", [c, "zenith"])

quantiles = {}
for c in VALUE_COLS:
    m = re.match(r"^(.*?)_?(?:p|q|quantile_?)(\d{1,2})$", c.lower())
    if m:
        quantiles.setdefault(m.group(1), []).append((int(m.group(2)), c))
for base, cols in quantiles.items():
    cols.sort()
    for (_, lo), (_, hi) in zip(cols, cols[1:]):
        rules[f"quantile_crossing_{lo}_gt_{hi}"] = (f"{lo} > {hi}", [lo, hi])

if HAS_RUNS:
    rules["lead_time_negative"] = ("lead_min < 0", ["lead_min"])

display(save(rule_counts(df, rules), f"{tag}_rules"))

# COMMAND ----------

# Sample violating rows: edit the condition
display(df.where(f"{MAIN} < 0").orderBy(*SEG, "period_end").limit(100))

# COMMAND ----------

# MAGIC %md ## 9. Stale runs: a new run that repeats the previous one
# MAGIC For each target period, compare a run's values with the previous run's. A run whose non-zero values are **all** identical to its predecessor's across ≥3 overlapping periods suggests the feed re-published old data.

# COMMAND ----------

if HAS_RUNS:
    cmp_cols = VALUE_COLS[:10]
    w = Window.partitionBy(*SEG, "period_end").orderBy("reference_time")
    same = reduce(lambda a, b: a & b, [F.col(c).eqNullSafe(F.lag(c).over(w)) for c in cmp_cols])
    pairs = (
        df.select(*SEG, "reference_time", "period_end", *cmp_cols)
        .withColumn("has_prev", F.lag("reference_time").over(w).isNotNull())
        .withColumn("same", same)
        .where(F.col("has_prev") & (F.col(MAIN) != 0))
    )
    per_run = pairs.groupBy(*SEG, "reference_time").agg(F.count("*").alias("overlap"), F.sum(F.col("same").cast("int")).alias("identical"))
    stale = per_run.where("overlap >= 3 AND identical = overlap")
    display(save(
        per_run.groupBy(*SEG).agg(F.count("*").alias("runs_compared"))
        .join(stale.groupBy(*SEG).agg(F.count("*").alias("stale_runs")), SEG, "left").fillna(0, ["stale_runs"])
        .withColumn("stale_pct", F.round(100 * F.col("stale_runs") / F.col("runs_compared"), 2)).orderBy(*SEG),
        f"{tag}_stale_runs",
    ))
    display(stale.orderBy(*SEG, "reference_time").limit(100))

# COMMAND ----------

# MAGIC %md ## 10. Timezone of `period_end`
# MAGIC Peak hour of the main value: about 08 means UTC, about 12 means Asia/Dubai. It must match PV telemetry and Solcast (notebooks 01, 02).

# COMMAND ----------

by_site = present(df, ["site"])
prof = df.groupBy(*by_site, F.hour("period_end").alias("hour")).agg(F.avg(MAIN).alias(f"avg_{MAIN}")).orderBy(*by_site, "hour")
display(prof)
for r in prof.withColumn("rk", F.row_number().over(Window.partitionBy(*by_site).orderBy(F.desc(f"avg_{MAIN}")))).where("rk = 1").collect():
    print(f"site={r.asDict().get('site', 'all')}: peak hour {r['hour']:02d}:00 -> looks like {guess_timezone(r['hour'])}")

# COMMAND ----------

# MAGIC %md ## 11. Against observations: PV telemetry and Solcast
# MAGIC For each target period, take the **latest available run** (shortest lead). Compare it with Bronze PV (both columns) and Solcast GHI (both sites), each averaged into `granularity_min` buckets labelled by bucket end.
# MAGIC - **Site mapping:** Reuniwatt `pv1` should correlate best with one Bronze PV column. Record which; the Bronze→Silver swap still applies downstream.
# MAGIC - **Accuracy:** `bias` and `mae` use the same units as the main column. They only mean something against the matching reference (PV for power, Solcast GHI for irradiance).
# MAGIC - All rows are included, nights too, so correlations are inflated. Compare them with each other rather than reading them as absolute values.

# COMMAND ----------

if HAS_RUNS:
    latest = df.withColumn("rk", F.row_number().over(Window.partitionBy(*SEG, "period_end").orderBy(F.desc("reference_time")))).where("rk = 1")
    pv = spark.table(PV_TABLE).select(F.col("DateTime").cast("timestamp").alias("ts"), *PV_COLS)
    sc = {s: spark.table(t).select(F.col("period_end").cast("timestamp").alias("ts"), F.col("ghi").alias(f"solcast_{s}_ghi")) for s, t in SOLCAST_TABLES.items()}
    refs = PV_COLS + [f"solcast_{s}_ghi" for s in SOLCAST_TABLES]
    grans = [r[0] for r in df.select("granularity_min").distinct().collect()] if "granularity_min" in df.columns else [60]

    parts = []
    for gm in grans:
        gm = int(gm)
        fc = latest.where(F.col("granularity_min") == gm) if "granularity_min" in df.columns else latest
        obs = bucket_end(pv, "ts", PV_COLS, gm)
        for s, d in sc.items():
            obs = obs.join(bucket_end(d, "ts", [f"solcast_{s}_ghi"], gm), "period_end", "full")
        j = fc.join(obs, "period_end")
        for ref in refs:
            parts.append(j.groupBy(*SEG).agg(
                F.count(F.when(F.col(MAIN).isNotNull() & F.col(ref).isNotNull(), 1)).alias("n"),
                F.round(F.corr(MAIN, ref), 4).alias("corr"),
                F.round(F.avg(F.col(MAIN) - F.col(ref)), 2).alias("bias"),
                F.round(F.avg(F.abs(F.col(MAIN) - F.col(ref))), 2).alias("mae"),
            ).withColumn("reference", F.lit(ref)))
    vs_obs = reduce(DataFrame.unionByName, parts).orderBy(*SEG, F.desc("corr"))
    display(save(vs_obs, f"{tag}_vs_observations"))

# COMMAND ----------

# MAGIC %md ### Time offset vs. PV (hourly, ±6 h)
# MAGIC Best offset `k` means PV time ≈ Reuniwatt `period_end` + k. A non-zero offset means a timezone or labelling mismatch.

# COMMAND ----------

if HAS_RUNS:
    by_site = present(df, ["site"])
    hour_end = F.timestamp_seconds(F.ceil(F.unix_timestamp("period_end") / 3600) * 3600)
    fc_h = latest.groupBy(*by_site, hour_end.alias("ts")).agg(F.avg(MAIN).alias(MAIN))
    pv_h = bucket_end(pv, "ts", PV_COLS, 60, "ts")
    rows = []
    for off in range(-6, 7):
        j = fc_h.withColumn("ts", F.timestamp_seconds(F.unix_timestamp("ts") + off * 3600)).join(pv_h, "ts")
        for r in j.groupBy(*by_site).agg(*[F.corr(MAIN, c).alias(c) for c in PV_COLS]).collect():
            for c in PV_COLS:
                rows.append((r.asDict().get("site", "all"), c, off, r[c]))
    lag_df = spark.createDataFrame(rows, "site string, pv_col string, offset_h int, corr double")
    display(save(lag_df.orderBy("site", "pv_col", "offset_h"), f"{tag}_lag_scan"))
    display(lag_df.withColumn("rk", F.row_number().over(Window.partitionBy("site", "pv_col").orderBy(F.desc("corr")))).where("rk = 1").drop("rk"))
