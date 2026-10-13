# Databricks notebook source
# MAGIC %md
# MAGIC # FE 10 · Step-by-step walkthrough of the feature pipeline
# MAGIC Run this **one cell at a time**. Each step explains what it does, runs it on a **small window** (default: 30 Sep – 2 Oct 2026), and ends with a **✅ What you should see** check.
# MAGIC
# MAGIC It follows the same logic as notebooks 02 (Reuniwatt) → 03 (PV target) → 04 (features), but keeps every intermediate result visible.
# MAGIC **Nothing is written** to any table, unless you set `save_output = yes` and run the very last cell.
# MAGIC
# MAGIC | Part | Steps |
# MAGIC |---|---|
# MAGIC | A · Reuniwatt forecasts (Bronze JSON) | 1–6 |
# MAGIC | B · PV target (Bronze telemetry) | 7–10 |
# MAGIC | C · Features for one product | 11–16 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 · Settings
# MAGIC - `start_date` / `end_date`: the test window (UTC dates). Keep it to a few days. The default covers 1 Oct 2026, when backfill and live runs overlap (used in step 6).
# MAGIC - `product`: which feature set to build in part C (`intraday`, `dayahead`, `weekahead`).
# MAGIC - `output_schema`: only used if you save at the end.

# COMMAND ----------

dbutils.widgets.text("output_schema", "ewec_dev_powerops.scratch_yash", "Output schema (only if saving)")
dbutils.widgets.text("start_date", "2026-09-30", "Window start (UTC)")
dbutils.widgets.text("end_date", "2026-10-02", "Window end (UTC)")
dbutils.widgets.dropdown("product", "dayahead", ["intraday", "dayahead", "weekahead"], "Product for part C")
dbutils.widgets.dropdown("save_output", "no", ["no", "yes"], "Save features at the end")

# COMMAND ----------

# MAGIC %run ./00_fe_common

# COMMAND ----------

START = dbutils.widgets.get("start_date").strip()
END = dbutils.widgets.get("end_date").strip()
PRODUCT = dbutils.widgets.get("product")
print(f"Window {START} → {END} (UTC), product = {PRODUCT}")
print("Session timezone:", spark.conf.get("spark.sql.session.timeZone"))
print("Sites:", SITES)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** the window and product you chose, session timezone `UTC`, and two sites (`pv1` ~950 MW, `pv2` ~1,650 MW).

# COMMAND ----------

# MAGIC %md
# MAGIC # Part A · Reuniwatt forecasts from Bronze
# MAGIC ## Step 1 · Look at the raw Bronze rows
# MAGIC Each Bronze row is **one forecast run**: a product (site × horizon), the time it was issued (`reference_time`) and the whole forecast as JSON text in `_raw`.

# COMMAND ----------

bronze = (
    spark.table(REUNI_BRONZE)
    .withColumn("reference_time", F.col("reference_time").cast("timestamp"))
    .withColumn("site", F.lower(F.trim("site")))
    .withColumn("horizon", F.lower(F.trim("horizon")))
    .where(F.to_date("reference_time").between(F.lit(START), F.lit(END)))
)
display(
    bronze.groupBy("_source", "site", "horizon").agg(
        F.count("*").alias("runs"),
        F.min("reference_time").alias("first_run"),
        F.max("reference_time").alias("last_run"),
        F.round(F.avg(F.length("_raw"))).alias("avg_json_chars"),
    ).orderBy("site", "horizon", "_source")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:**
# MAGIC - All 6 products (pv1/pv2 × intraday/day-ahead/week-ahead), about **96 runs per day each** (one every 15 min).
# MAGIC - Rows from `api_historical_backfill` up to 1 Oct, `api_incremental` from 1 Oct.
# MAGIC - JSON size: intraday smallest (~20k chars), day-ahead ~190k, week-ahead largest.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · Open one run's JSON
# MAGIC The JSON is a nested map: **site → variable → (target time → value)**. Here we read one day-ahead PV1 run and list its variables.

# COMMAND ----------

JSON_SCHEMA = "MAP<STRING, MAP<STRING, MAP<STRING, DOUBLE>>>"
one = (
    bronze.where("horizon = 'dayahead' AND site = 'pv1' AND _source != 'migration_legacy'")
    .orderBy("reference_time").limit(1)
    .withColumn("m", F.try_element_at(F.from_json("_raw", JSON_SCHEMA), F.upper("site")))
)
r = one.select("product_id", "reference_time", "_source", F.map_keys("m").alias("variables"),
               F.size(F.try_element_at("m", F.lit("ghi"))).alias("time_steps")).first()
print("Run:", r["product_id"], "issued", r["reference_time"], "from", r["_source"])
print("Variables:", sorted(r["variables"]))
print("Time steps per variable:", r["time_steps"])

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** 18 variables for PV1 day-ahead: `bhi, clearness_index, cloud_opacity, dewpoint, dhi, dni, ghi, gti_270, gti_90, power, power10, power90, precip, pressure, relative_humidity, temperature, wind_direction, wind_speed`, and **288 time steps** (24 h × 12 per hour).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · Unpack that one run into rows
# MAGIC Turn the nested map into a table: **one row per target time, one column per variable**.
# MAGIC Method: sort each variable's (time, value) pairs by time and zip them side by side. Every variable in a run has the same times, so they line up. (This avoids slow map look-ups.)

# COMMAND ----------

BRONZE_VARS = {
    "ghi": "ghi", "dni": "dni", "dhi": "dhi", "bhi": "bhi",
    "gti": "gti", "gti_90": "gti_east", "gti_270": "gti_west",
    "temperature": "temp_c", "dewpoint": "dewpoint_c", "relative_humidity": "humidity_pct",
    "wind_speed": "wind_speed", "wind_direction": "wind_dir", "cloud_opacity": "cloud_opacity",
    "pressure": "pressure_hpa", "precip": "precip_rate_mmh", "clearness_index": "clearness_index",
    "power": "power_mw", "power10": "power_p10_mw", "power90": "power_p90_mw",
}


def unpack(runs_with_m: DataFrame) -> DataFrame:
    """runs: site, horizon, reference_time, raw_source, m (variable -> time -> value). Returns one row per run × target."""
    def sorted_entries(var):
        return F.array_sort(F.map_entries(F.try_element_at("m", F.lit(var))))

    r = runs_with_m.withColumn("_times", F.transform(sorted_entries("ghi"), lambda e: e["key"])).withColumn("_n", F.size("_times")).where("_n > 0")
    for k, v in BRONZE_VARS.items():
        ent = sorted_entries(k)
        r = r.withColumn(v, F.when(F.size(ent) == F.col("_n"), F.transform(ent, lambda e: e["value"]))
                         .otherwise(F.array_repeat(F.lit(None).cast("double"), F.col("_n"))))
    z = r.select("site", "horizon", "reference_time", "raw_source", F.explode(F.arrays_zip("_times", *BRONZE_VARS.values())).alias("z"))
    return z.select("site", "horizon", "reference_time", "raw_source",
                    F.to_timestamp(F.col("z._times"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("period_end"),
                    *[F.col(f"z.{v}").alias(v) for v in BRONZE_VARS.values()])


one_rows = unpack(one.select("site", "horizon", "reference_time", F.col("_source").alias("raw_source"), "m"))
print("Rows:", one_rows.count())
display(one_rows.orderBy("period_end"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:**
# MAGIC - **288 rows**; the first `period_end` is 5 min after `reference_time`.
# MAGIC - `ghi` is 0 at night (UTC ~14:00–02:00, i.e. Gulf ~18:00–06:00) and peaks around **08:00 UTC** (12:00 Gulf time).
# MAGIC - `gti_east` higher in the morning, `gti_west` in the afternoon. `gti` is empty (that's PV2's column).
# MAGIC - `power_mw` is **below 1** here: Reuniwatt sends **GW**. Step 5 converts it.
# MAGIC
# MAGIC Tip: in the result, click **+ → Visualization → Line**, x = `period_end`, y = `ghi`, `power_mw`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · Keep only the runs we need ("thinning")
# MAGIC Runs arrive every 15 min, but the models don't need every one. Unpacking every week-ahead run would mean billions of rows. We keep:
# MAGIC every **intraday** run, one **day-ahead** run per hour, and one **week-ahead** run every 3 hours. We also drop `migration_legacy` (no real issue time) and keep the latest copy of each run.

# COMMAND ----------

KEEP_RUN_EVERY_MIN = {"intraday": 15, "dayahead": 60, "weekahead": 180}
every = None
for h, mins in KEEP_RUN_EVERY_MIN.items():
    every = F.when(F.col("horizon") == h, F.lit(mins)) if every is None else every.when(F.col("horizon") == h, F.lit(mins))

kept = (
    bronze.where(F.col("_source") != "migration_legacy")
    .withColumn("_ingested_at", F.col("_ingested_at").cast("timestamp"))
    .where(((F.hour("reference_time") * 60 + F.minute("reference_time")) % every) < 15)
    .withColumn("_rk", F.row_number().over(Window.partitionBy("site", "horizon", "reference_time").orderBy(F.desc("_ingested_at"))))
    .where("_rk = 1").drop("_rk")
)
display(kept.groupBy("site", "horizon", F.to_date("reference_time").alias("day")).count().orderBy("site", "horizon", "day"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** per full day, about **96** intraday, **24** day-ahead and **8** week-ahead runs per site. 1 Oct may show extra runs where the backfill and live feeds overlap; that's expected and used in step 6.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 · Unpack all kept runs and clean them
# MAGIC Same unpacking as step 3, for every kept run. Then:
# MAGIC - Power **GW → MW** (× 1000).
# MAGIC - `gti_poa` = sunlight on the panels: PV2 `gti`, PV1 the average of the east and west panels.
# MAGIC - `lead_min` = minutes between issue and target; anything ≤ 0 is dropped.

# COMMAND ----------

fc = unpack(kept.select("site", "horizon", "reference_time", F.col("_source").alias("raw_source"),
                        F.try_element_at(F.from_json("_raw", JSON_SCHEMA), F.upper("site")).alias("m")))
for c in ["power_mw", "power_p10_mw", "power_p90_mw"]:
    fc = fc.withColumn(c, F.col(c) * 1000.0)
fc = (
    fc.withColumn("gti_poa", F.when(F.col("site") == "pv1", (F.col("gti_east") + F.col("gti_west")) / 2).otherwise(F.col("gti")))
    .withColumn("lead_min", (F.unix_timestamp("period_end") - F.unix_timestamp("reference_time")) / 60)
    .where("lead_min > 0")
)
display(
    fc.groupBy("site", "horizon").agg(
        F.countDistinct("reference_time").alias("runs"), F.count("*").alias("rows"),
        F.round(F.max("lead_min") / 60, 1).alias("max_lead_h"),
        F.round(F.max("ghi")).alias("max_ghi"), F.round(F.max("power_mw")).alias("max_power_mw"),
        F.round(100 * F.avg(F.col("temp_c").isNotNull().cast("int")), 1).alias("pct_with_temperature"),
    ).orderBy("site", "horizon")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:**
# MAGIC - `max_lead_h`: intraday **6**, day-ahead **24**, week-ahead **168**.
# MAGIC - `max_ghi` ~900–1,000 W/m².
# MAGIC - `max_power_mw`: pv1 up to ~**950**, pv2 up to ~**1,650** (now in MW).
# MAGIC - `pct_with_temperature` 100 for day-ahead, 0 for intraday and week-ahead (weather comes only with day-ahead).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 · Is the backfill the forecast as originally issued?
# MAGIC Around 1 Oct, the same forecast exists twice: once from the **historical backfill** and once from the **live feed**. If the backfill holds the forecasts as Reuniwatt issued them at the time, both copies should match.
# MAGIC If they differ a lot, the backfill was re-computed later with information from after the issue time, and training on it would "cheat".

# COMMAND ----------

slot = F.timestamp_seconds(F.floor(F.unix_timestamp("reference_time") / 900) * 900)
x = fc.withColumn("slot", slot)
k = ["site", "horizon", "slot", "period_end"]
bf = x.where("raw_source = 'api_historical_backfill'").select(*k, F.col("ghi").alias("ghi_backfill"), F.col("power_mw").alias("power_backfill"))
lv = x.where("raw_source = 'api_incremental'").select(*k, F.col("ghi").alias("ghi_live"), F.col("power_mw").alias("power_live"))
overlap = bf.join(lv, k)
display(
    overlap.groupBy("site", "horizon").agg(
        F.countDistinct("slot").alias("overlapping_runs"),
        F.round(F.corr("ghi_backfill", "ghi_live"), 4).alias("ghi_corr"),
        F.round(F.avg(F.abs(F.col("ghi_backfill") - F.col("ghi_live"))), 2).alias("ghi_mean_abs_diff"),
        F.round(F.avg(F.abs(F.col("power_backfill") - F.col("power_live"))), 2).alias("power_mean_abs_diff_mw"),
    ).orderBy("site", "horizon")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** `ghi_corr` ≈ **1.0** and small differences (a few W/m², a few MW). That means the backfill is safe to train on.
# MAGIC ⚠️ No rows → your window doesn't include 1 Oct 2026. Large differences → **stop and flag it**: the backfill isn't the as-issued forecast.

# COMMAND ----------

# MAGIC %md
# MAGIC # Part B · PV target from Bronze telemetry
# MAGIC ## Step 7 · Raw PV minutes and cleaning
# MAGIC Bronze PV is one row per minute in **Gulf time**, with columns `PV1_MW` and `PV2_MW`. Cleaning:
# MAGIC - Absolute value above 5,000 MW → **corrupt**, set to null.
# MAGIC - Between −10 and 0 MW → night self-consumption, set to **0**. Below −10 → null.
# MAGIC - The same minute logged twice → averaged.

# COMMAND ----------

def clean(c: str):
    v = F.col(c).cast("double")
    return (F.when(F.abs(v) > PV_SENTINEL_ABS_MW, F.lit(None).cast("double"))
            .when(v < NIGHT_NEG_FLOOR_MW, F.lit(None).cast("double"))
            .when(v < 0, F.lit(0.0)).otherwise(v))


pv_raw = spark.table(PV_BRONZE).select(F.col("DateTime").cast("timestamp").alias("ts_gulf"), "PV1_MW", "PV2_MW")
pv_win = pv_raw.where(F.to_date("ts_gulf").between(F.date_sub(F.lit(START), 1), F.date_add(F.lit(END), 1)))
pv = pv_win.select("ts_gulf", clean("PV1_MW").alias("PV1_MW"), clean("PV2_MW").alias("PV2_MW")) \
    .groupBy("ts_gulf").agg(F.avg("PV1_MW").alias("PV1_MW"), F.avg("PV2_MW").alias("PV2_MW"))

display(pv_win.agg(
    F.count("*").alias("raw_rows"), F.countDistinct("ts_gulf").alias("distinct_minutes"),
    F.sum((F.abs("PV1_MW") > PV_SENTINEL_ABS_MW).cast("int")).alias("pv1_corrupt"),
    F.sum((F.col("PV1_MW") < 0).cast("int")).alias("pv1_negative"),
    F.sum((F.col("PV2_MW") < 0).cast("int")).alias("pv2_negative"),
    F.round(F.max("PV1_MW")).alias("pv1_max"), F.round(F.max("PV2_MW")).alias("pv2_max"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** about 1,440 minutes per day; no corrupt values in Oct 2026 (they're only in Apr–May 2023); some small negatives at night. In Oct 2026, `pv2_max` (~1,600) > `pv1_max` (~900) because the columns swapped in March 2026 (next step).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 · Which Bronze column is which plant?
# MAGIC The Bronze columns **swapped around March 2026**: before, `PV1_MW` was the large plant; after, `PV2_MW` is. We detect the date from daily peaks over the full history (7-day majority vote), then map:
# MAGIC `pv2` (large) = `PV1_MW` before the swap, `PV2_MW` after. `pv1` (small) is the opposite.

# COMMAND ----------

from datetime import timedelta

full = spark.table(PV_BRONZE).select(F.col("DateTime").cast("timestamp").alias("ts_gulf"), clean("PV1_MW").alias("PV1_MW"), clean("PV2_MW").alias("PV2_MW"))
daily = (
    full.groupBy(F.to_date("ts_gulf").alias("day")).agg(F.max("PV1_MW").alias("pv1_peak"), F.max("PV2_MW").alias("pv2_peak"))
    .withColumn("pv2_bigger", (F.col("pv2_peak") > F.col("pv1_peak")).cast("int"))
    .withColumn("pv2_bigger_7d", F.avg("pv2_bigger").over(Window.orderBy("day").rowsBetween(-3, 3)))
)
last_pv1_big = daily.where("pv2_bigger_7d < 0.5").agg(F.max("day")).first()[0]
last_day = daily.agg(F.max("day")).first()[0]
swap_day = (last_pv1_big + timedelta(days=1)) if last_pv1_big and last_pv1_big < last_day else None
print("Detected swap date:", swap_day)
display(daily.where(F.col("day").between(F.lit(swap_day - timedelta(days=10)), F.lit(swap_day + timedelta(days=10)))).orderBy("day")
        if swap_day else daily.orderBy(F.desc("day")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** a date in **March 2026**. In the table, `pv1_peak` ~1,600 and `pv2_peak` ~900 before it, then the reverse after it. If the change is messy across several days, note the clean date: you can set it by hand in notebook 03 (`swap_date`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 · 5-minute target in UTC, with a status per period
# MAGIC - Pick the right column per plant (step 8), convert Gulf → **UTC**, and average into 5-min periods labelled by their **end** (same as Reuniwatt).
# MAGIC - Add sun position, then a status:
# MAGIC   - `night`: sun down; missing values → 0.
# MAGIC   - `missing`: daytime with no data, kept **blank** (not 0).
# MAGIC   - `suspected_outage`: sunny but output under 1% of capacity.
# MAGIC   - `ok`.
# MAGIC - `label_ok` = usable for training and scoring (`ok` or `night`).

# COMMAND ----------

from datetime import date

swap_lit = F.lit(swap_day or date(9999, 12, 31))
long = reduce(DataFrame.unionByName, [
    pv.select(F.lit(s).alias("site"), "ts_gulf",
              F.when(F.to_date("ts_gulf") < swap_lit, F.col(cfg["bronze_col_before_swap"])).otherwise(F.col(cfg["bronze_col_after_swap"])).alias("pv_mw"))
    for s, cfg in SITES.items()
])
agg = long.withColumn("period_end", ceil_to(gulf_to_utc("ts_gulf"), STEP_MIN)).groupBy("site", "period_end") \
    .agg(F.avg("pv_mw").alias("pv_mw"), F.count("pv_mw").alias("n_valid_min"))
spine = spark.range(1).select(F.explode(F.sequence(F.to_timestamp(F.lit(START)), F.to_timestamp(F.lit(END + " 23:55:00")),
                                                   F.expr("INTERVAL 5 MINUTES"))).alias("period_end")) \
    .crossJoin(spark.createDataFrame([(s,) for s in SITES], "site string"))
cap = site_value("capacity_mw")
t = add_solar(spine.join(agg, ["site", "period_end"], "left").fillna(0, ["n_valid_min"]), "period_end")
status = (F.when(F.col("sun_zenith") > 95, "night").when(F.col("n_valid_min") == 0, "missing")
          .when((F.col("clearsky_ghi") > 200) & (F.col("pv_mw") < 0.01 * cap), "suspected_outage").otherwise("ok"))
target = (
    t.withColumn("status", status)
    .withColumn("pv_mw", F.when(F.col("status") == "night", F.coalesce("pv_mw", F.lit(0.0))).otherwise(F.col("pv_mw")))
    .withColumn("capacity_mw", cap).withColumn("pv_norm", F.col("pv_mw") / F.col("capacity_mw"))
    .withColumn("label_ok", F.col("status").isin("ok", "night"))
)
display(target.groupBy("site", "status").count().orderBy("site", "status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** per site and day, 288 periods, roughly half `night` and half `ok`, with few or no `missing` / `suspected_outage`. Many `missing` periods mean telemetry gaps in your window.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 · Sanity-check the target against the sun
# MAGIC Actual PV should rise and fall with clear-sky sunlight (computed), peaking around **08:00 UTC**.

# COMMAND ----------

display(target.where(F.col("period_end").between(F.to_timestamp(F.lit(START)), F.to_timestamp(F.lit(START + " 23:55:00"))))
        .select("site", "period_end", "pv_mw", "clearsky_ghi", "sun_elevation", "status").orderBy("site", "period_end"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** for each site, `pv_mw` and `clearsky_ghi` both start rising around 02:00–02:30 UTC, peak around 08:00 UTC and reach 0 around 14:00 UTC. pv1 peaks near ~900 MW and pv2 near ~1,600 MW on a clear day.
# MAGIC If PV is shifted by about 4 hours relative to `clearsky_ghi`, the Gulf → UTC conversion is wrong; tell Claude.

# COMMAND ----------

# MAGIC %md
# MAGIC # Part C · Features for one product
# MAGIC ## Step 11 · Forecast origins and target times
# MAGIC An **origin** is when we make a forecast. For each origin we predict every 5-min **target** in the product's window.
# MAGIC
# MAGIC | Product | Origins | Targets |
# MAGIC |---|---|---|
# MAGIC | intraday | every 15 min | next 6 h |
# MAGIC | dayahead | 09:00 and 15:00 Gulf time | next 24 h |
# MAGIC | weekahead | 09:00 Gulf time | next 7 days |

# COMMAND ----------

PRODUCTS = {
    "intraday": {"window_min": 360, "origins": ("every_min", 15), "uses": ["intraday", "dayahead", "weekahead"]},
    "dayahead": {"window_min": 1440, "origins": ("gulf_hours", [9, 15]), "uses": ["dayahead", "weekahead"]},
    "weekahead": {"window_min": 7 * 1440, "origins": ("gulf_hours", [9]), "uses": ["weekahead", "dayahead"]},
}
PREFIX = {"intraday": "id", "dayahead": "da", "weekahead": "wa"}
MAX_RUN_AGE_MIN = {"intraday": 60, "dayahead": 360, "weekahead": 1440}
CFG = PRODUCTS[PRODUCT]
LATENCY = 15

sites = spark.createDataFrame([(s,) for s in SITES], "site string")
kind, arg = CFG["origins"]
if kind == "every_min":
    origins = spark.range(1).select(F.explode(F.sequence(F.to_timestamp(F.lit(START)), F.to_timestamp(F.lit(END + " 23:59:59")),
                                                         F.expr(f"INTERVAL {arg} MINUTES"))).alias("origin"))
else:
    days = spark.range(1).select(F.explode(F.sequence(F.to_date(F.lit(START)), F.to_date(F.lit(END)))).alias("d"))
    origins = days.select(F.explode(F.array(*[F.lit(h) for h in arg])).alias("h"), "d").select(
        F.timestamp_seconds(F.unix_timestamp(F.col("d").cast("timestamp")) + (F.col("h") - GULF_OFFSET_H) * 3600).alias("origin"))
origins = origins.crossJoin(sites)
rows = origins.select("site", "origin", F.explode(F.sequence(shift_minutes("origin", STEP_MIN), shift_minutes("origin", CFG["window_min"]),
                                                             F.expr("INTERVAL 5 MINUTES"))).alias("period_end"))
display(origins.withColumn("origin_gulf", utc_to_gulf("origin")).orderBy("site", "origin"))
print("Rows (site × origin × target):", rows.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** for `dayahead`, two origins per day per site at **05:00 and 11:00 UTC** (= 09:00 and 15:00 Gulf), and 288 targets per origin.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 · Pick the latest forecast run available at each origin
# MAGIC **The key no-cheating rule.** For each origin, take the latest run issued at least 15 min before it (`asof_runs`). Never a later one. Runs older than the max age count as missing.

# COMMAND ----------

asof_tables = {}
for h in CFG["uses"]:
    runs_h = fc.where(F.col("horizon") == h).select("site", "reference_time").distinct()
    asof_tables[h] = asof_runs(origins, runs_h, LATENCY).withColumn(
        "run_time", F.when(F.col("run_age_min") <= MAX_RUN_AGE_MIN[h], F.col("run_time")))
    print(f"--- {h}")
    display(asof_tables[h].orderBy("site", "origin"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** every `run_time` is **before** its `origin`, with `run_age_min` ≥ 15. Day-ahead runs are typically 60–75 min old (hourly thinning), week-ahead up to ~3 h. A blank `run_time` on the first origin is normal: the window starts before any run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 · Attach that run's forecast for each target
# MAGIC For each horizon, join the chosen run's values onto every target time. Columns are prefixed `id_` (intraday), `da_` (day-ahead), `wa_` (week-ahead).
# MAGIC `f_*` = best available: the product's own horizon first, others as fallback.

# COMMAND ----------

IRR_POWER = ["ghi", "dni", "dhi", "gti_poa", "power_mw", "power_p10_mw", "power_p90_mw"]
WEATHER = ["temp_c", "dewpoint_c", "humidity_pct", "wind_speed", "cloud_opacity"]
feat = rows
for h in CFG["uses"]:
    p = PREFIX[h]
    a = asof_tables[h].select("site", "origin", F.col("run_time").alias(f"{p}_run_time"), F.col("run_age_min").alias(f"{p}_run_age_min"))
    cols = IRR_POWER + (WEATHER if h == "dayahead" else [])
    vals = fc.where(F.col("horizon") == h).select("site", F.col("reference_time").alias(f"{p}_run_time"), "period_end",
                                                 *[F.col(c).alias(f"{p}_{c}") for c in cols])
    feat = feat.join(a, ["site", "origin"], "left").join(vals, ["site", f"{p}_run_time", "period_end"], "left")
prefixes = [PREFIX[h] for h in CFG["uses"]]
for c in IRR_POWER:
    feat = feat.withColumn(f"f_{c}", F.coalesce(*[F.col(f"{p}_{c}") for p in prefixes]))

one_origin = feat.select("origin").orderBy("origin").limit(2).collect()[-1][0]
display(feat.where((F.col("site") == "pv2") & (F.col("origin") == F.lit(one_origin))).orderBy("period_end"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** one origin for pv2 with all its targets. `f_ghi` and `f_power_mw` follow a daily curve; `da_temp_c` etc. are filled; `wa_*` values are close to `da_*` for the first day.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 · Derived features
# MAGIC - **Sun at the target** (computed): angle, air mass, clear-sky sunlight.
# MAGIC - **Calendar** (Gulf time): hour and day-of-year as sin/cos; `lead_min`.
# MAGIC - **Weather vs clear sky:** `csi_ghi` = forecast GHI ÷ clear-sky GHI (1 = clear, low = cloudy or dusty); `diffuse_frac` = DHI ÷ GHI (high = haze/dust).
# MAGIC - **Reuniwatt power ÷ capacity:** `power_norm`, uncertainty `band_width_norm`, `clip_flag` when near the plant limit.
# MAGIC - **Heat:** panel temperature and derate.
# MAGIC - **Disagreement** between horizons.

# COMMAND ----------

feat = add_solar(feat, "period_end")
feat = add_calendar(feat, "period_end", "t_")
feat = add_calendar(feat, "origin", "o_").drop("o_doy_sin", "o_doy_cos")
feat = (
    feat.withColumn("capacity_mw", cap)
    .withColumn("lead_min", (F.unix_timestamp("period_end") - F.unix_timestamp("origin")) / 60)
    .withColumn("csi_ghi", F.when(F.col("clearsky_ghi") > 50, F.least(F.col("f_ghi") / F.col("clearsky_ghi"), F.lit(1.5))))
    .withColumn("kt", F.when(F.col("etr_horizontal") > 50, F.col("f_ghi") / F.col("etr_horizontal")))
    .withColumn("diffuse_frac", F.when(F.col("f_ghi") > 20, F.least(F.col("f_dhi") / F.col("f_ghi"), F.lit(1.0))))
    .withColumn("power_norm", F.col("f_power_mw") / cap)
    .withColumn("band_width_norm", (F.col("f_power_p90_mw") - F.col("f_power_p10_mw")) / cap)
    .withColumn("clip_flag", (F.col("f_power_mw") >= 0.97 * cap).cast("int"))
    .withColumn("t_cell_c", F.col("da_temp_c") + 0.031 * F.col("f_gti_poa"))
    .withColumn("temp_derate", 1 - 0.004 * (F.col("t_cell_c") - 25))
)
if "da" in prefixes and "wa" in prefixes:
    feat = feat.withColumn("disagree_da_wa", F.abs(F.col("da_power_mw") - F.col("wa_power_mw")) / cap)
if "id" in prefixes:
    feat = feat.withColumn("disagree_id_da", F.abs(F.col("id_power_mw") - F.col("da_power_mw")) / cap)

display(feat.where("is_day").select("site", "lead_min", "sun_elevation", "clearsky_ghi", "f_ghi", "csi_ghi", "diffuse_frac",
                                     "power_norm", "band_width_norm", "clip_flag", "t_cell_c", "temp_derate").summary("min", "50%", "max"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see (daytime):**
# MAGIC - `csi_ghi` mostly **0.7–1.1** (clear desert days).
# MAGIC - `diffuse_frac` ~**0.1–0.3**.
# MAGIC - `power_norm` 0 to ~**1**.
# MAGIC - `t_cell_c` ~**30–70 °C**.
# MAGIC - `temp_derate` ~**0.8–1.0**.
# MAGIC
# MAGIC Values far outside these ranges mean a unit or join problem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 · What we knew at the origin (recent actual PV)
# MAGIC Using **only PV data up to the origin**:
# MAGIC - output now
# MAGIC - average over the last 30 and 60 min
# MAGIC - 30-min trend
# MAGIC - output vs clear sky
# MAGIC - `o_reuni_ratio_60m`: how far Reuniwatt has been off over the last hour (actual ÷ Reuniwatt's latest forecast for those periods)

# COMMAND ----------

nowcast = (fc.where("power_mw IS NOT NULL")
           .withColumn("_rk", F.row_number().over(Window.partitionBy("site", "period_end").orderBy(F.desc("reference_time"))))
           .where("_rk = 1").select("site", "period_end", F.col("power_mw").alias("nc_power_mw")))
series = target.select("site", "period_end", F.when(F.col("label_ok"), F.col("pv_mw")).alias("pv"), "clearsky_ghi", "capacity_mw") \
    .join(nowcast, ["site", "period_end"], "left")
ts = F.unix_timestamp("period_end")
w = Window.partitionBy("site").orderBy(ts)
w30, w60 = w.rangeBetween(-25 * 60, 0), w.rangeBetween(-55 * 60, 0)
both = F.col("pv").isNotNull() & F.col("nc_power_mw").isNotNull()
state = (
    series.withColumn("o_pv_norm", F.col("pv") / F.col("capacity_mw"))
    .withColumn("o_pv_mean_30m_norm", F.avg("pv").over(w30) / F.col("capacity_mw"))
    .withColumn("o_pv_mean_60m_norm", F.avg("pv").over(w60) / F.col("capacity_mw"))
    .withColumn("o_pv_trend_30m_norm", (F.col("pv") - F.lag("pv", 6).over(w)) / F.col("capacity_mw"))
    .withColumn("o_pv_vs_clearsky", F.when(F.col("clearsky_ghi") > 50, F.col("o_pv_norm") / (F.col("clearsky_ghi") / 1000)))
    .withColumn("_pv60", F.sum(F.when(both, F.col("pv"))).over(w60))
    .withColumn("_nc60", F.sum(F.when(both, F.col("nc_power_mw"))).over(w60))
    .withColumn("o_reuni_ratio_60m", F.when(F.col("_nc60") > 1, F.least(F.col("_pv60") / F.col("_nc60"), F.lit(2.0))))
    .select("site", F.col("period_end").alias("origin"), "o_pv_norm", "o_pv_mean_30m_norm", "o_pv_mean_60m_norm",
            "o_pv_trend_30m_norm", "o_pv_vs_clearsky", "o_reuni_ratio_60m")
)
feat = feat.join(state, ["site", "origin"], "left")
display(feat.select("site", "origin", "o_pv_norm", "o_pv_mean_60m_norm", "o_pv_trend_30m_norm", "o_reuni_ratio_60m").distinct().orderBy("site", "origin"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:** at day-ahead origins (09:00 and 15:00 Gulf), `o_pv_norm` > 0 (sun is up). `o_reuni_ratio_60m` near **1** means Reuniwatt was right over the last hour; 0.8 means actual was 20% below its forecast.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16 · Add the answer (label) and do a final check
# MAGIC The label is the actual PV at each target time, from step 9. The last check: how well Reuniwatt's own forecast already matches actual output. That's the baseline our model must beat.

# COMMAND ----------

label = target.select("site", "period_end", F.col("pv_mw").alias("label_pv_mw"), F.col("pv_norm").alias("label_pv_norm"),
                      F.col("status").alias("label_status"), "label_ok")
feat = feat.join(label, ["site", "period_end"], "left").withColumn("product", F.lit(PRODUCT))
main = PREFIX[CFG["uses"][0]]
feat = feat.where(F.col(f"{main}_run_time").isNotNull())

day = feat.where("is_day AND label_ok")
display(day.groupBy("site").agg(
    F.count("*").alias("daytime_rows"),
    F.round(F.corr("power_norm", "label_pv_norm"), 3).alias("corr_reuniwatt_vs_actual"),
    F.round(100 * F.avg(F.abs(F.col("f_power_mw") - F.col("label_pv_mw"))) / F.first("capacity_mw"), 2).alias("reuniwatt_nmae_pct"),
    F.round(100 * F.avg(F.col("f_power_mw").isNull().cast("int")), 1).alias("pct_missing_forecast"),
))
print("Total columns:", len(feat.columns))
print(sorted(feat.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **What you should see:**
# MAGIC - `corr_reuniwatt_vs_actual` around **0.9+**.
# MAGIC - `reuniwatt_nmae_pct`: Reuniwatt's average error as % of capacity. This is the number to beat.
# MAGIC - `pct_missing_forecast` near 0.
# MAGIC - Around 100 columns.
# MAGIC
# MAGIC If all of that holds, the pipeline logic works and you can run `05_fe_run_all` over the full history.

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Save this test output
# MAGIC Writes the features for this window to `<output_schema>.fe_walkthrough_<product>_5min`. It only runs if `save_output = yes`.

# COMMAND ----------

if dbutils.widgets.get("save_output") == "yes":
    write(feat, f"fe_walkthrough_{PRODUCT}_5min")
else:
    print("save_output = no → nothing written.")
