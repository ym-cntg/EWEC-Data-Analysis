# Databricks notebook source
# MAGIC %md
# MAGIC # FE 04 · Build features → `fe_features_<product>_5min`
# MAGIC One row per **site × forecast origin × 5-minute target time**: the features known at the origin, plus the label (actual PV at the target).
# MAGIC
# MAGIC | `product` | Origins (when the forecast is made) | Targets | Reuniwatt runs used |
# MAGIC |---|---|---|---|
# MAGIC | `intraday` | every 15 min | next 6 h | intraday (main), day-ahead, week-ahead |
# MAGIC | `dayahead` | 09:00 and 15:00 Gulf time | next 24 h | day-ahead (main), week-ahead |
# MAGIC | `weekahead` | 09:00 Gulf time | next 7 days | week-ahead (main), day-ahead |
# MAGIC
# MAGIC **No look-ahead:** each origin uses only the latest run issued at least `latency_min` before it (`asof_runs`), and only PV actuals up to the origin.
# MAGIC 15- and 60-minute products are built later by averaging these 5-minute rows.
# MAGIC
# MAGIC Needs `fe_reuniwatt_forecasts` (02) and `fe_pv_target_5min` (03) in `output_schema`.

# COMMAND ----------

dbutils.widgets.text("output_schema", "ewec_dev_powerops.scratch_yash", "Output schema (catalog.schema)")
dbutils.widgets.dropdown("product", "dayahead", ["intraday", "dayahead", "weekahead"], "Forecast product")
dbutils.widgets.text("start_date", "2026-09-21", "First origin date (use 2024-01-23 once Bronze is parsed)")
dbutils.widgets.text("end_date", "", "Last origin date (blank = latest forecast)")
dbutils.widgets.text("latency_min", "15", "Minutes before a run counts as available")

# COMMAND ----------

# MAGIC %run ./00_fe_common

# COMMAND ----------

PRODUCTS = {
    "intraday": {"window_min": 360, "origins": ("every_min", 15), "uses": ["intraday", "dayahead", "weekahead"]},
    "dayahead": {"window_min": 1440, "origins": ("gulf_hours", [9, 15]), "uses": ["dayahead", "weekahead"]},
    "weekahead": {"window_min": 7 * 1440, "origins": ("gulf_hours", [9]), "uses": ["weekahead", "dayahead"]},
}
PREFIX = {"intraday": "id", "dayahead": "da", "weekahead": "wa"}
MAX_RUN_AGE_MIN = {"intraday": 60, "dayahead": 360, "weekahead": 1440}  # older runs count as missing
IRR_POWER = ["ghi", "dni", "dhi", "gti_poa", "power_mw", "power_p10_mw", "power_p90_mw"]
WEATHER = ["temp_c", "dewpoint_c", "humidity_pct", "wind_speed", "cloud_opacity"]  # day-ahead only

PRODUCT = dbutils.widgets.get("product")
CFG = PRODUCTS[PRODUCT]
LATENCY = int(dbutils.widgets.get("latency_min"))

fc = spark.table(out_table("fe_reuniwatt_forecasts"))
pv = spark.table(out_table("fe_pv_target_5min"))

start = dbutils.widgets.get("start_date").strip()
end = dbutils.widgets.get("end_date").strip() or str(fc.agg(F.max(F.to_date("reference_time"))).first()[0])
print(f"{PRODUCT}: origins {start} → {end}, window {CFG['window_min']} min, runs from {CFG['uses']}")

# COMMAND ----------

# MAGIC %md ## Origins and targets

# COMMAND ----------

sites = spark.createDataFrame([(s,) for s in SITES], "site string")
kind, arg = CFG["origins"]
if kind == "every_min":
    origins = spark.range(1).select(F.explode(F.sequence(
        F.to_timestamp(F.lit(start)), F.to_timestamp(F.lit(end + " 23:59:59")), F.expr(f"INTERVAL {arg} MINUTES"))).alias("origin"))
else:  # fixed Gulf clock hours each day
    days = spark.range(1).select(F.explode(F.sequence(F.to_date(F.lit(start)), F.to_date(F.lit(end)))).alias("d"))
    origins = days.select(F.explode(F.array(*[F.lit(h) for h in arg])).alias("h"), "d").select(
        F.timestamp_seconds(F.unix_timestamp(F.col("d").cast("timestamp")) + (F.col("h") - GULF_OFFSET_H) * 3600).alias("origin"))
origins = origins.crossJoin(sites)

rows = origins.select("site", "origin", F.explode(F.sequence(
    shift_minutes("origin", STEP_MIN), shift_minutes("origin", CFG["window_min"]), F.expr(f"INTERVAL {STEP_MIN} MINUTES"))).alias("period_end"))

# COMMAND ----------

# MAGIC %md ## Reuniwatt values from the latest available run of each horizon

# COMMAND ----------

for h in CFG["uses"]:
    p = PREFIX[h]
    fh = fc.where(F.col("horizon") == h)
    a = asof_runs(origins, fh.select("site", "reference_time").distinct(), LATENCY)
    a = a.select(
        "site", "origin",
        F.when(F.col("run_age_min") <= MAX_RUN_AGE_MIN[h], F.col("run_time")).alias(f"{p}_run_time"),
        F.col("run_age_min").alias(f"{p}_run_age_min"),
    )
    cols = IRR_POWER + (WEATHER if h == "dayahead" else [])
    vals = fh.select("site", F.col("reference_time").alias(f"{p}_run_time"), "period_end", *[F.col(c).alias(f"{p}_{c}") for c in cols])
    rows = rows.join(a, ["site", "origin"], "left").join(vals, ["site", f"{p}_run_time", "period_end"], "left")

prefixes = [PREFIX[h] for h in CFG["uses"]]  # main horizon first
for c in IRR_POWER:
    rows = rows.withColumn(f"f_{c}", F.coalesce(*[F.col(f"{p}_{c}") for p in prefixes]))

# COMMAND ----------

# MAGIC %md ## Derived features

# COMMAND ----------

cap = site_value("capacity_mw")
rows = add_solar(rows, "period_end")
rows = add_calendar(rows, "period_end", "t_")
rows = add_calendar(rows, "origin", "o_").drop("o_doy_sin", "o_doy_cos")

rows = (
    rows.withColumn("capacity_mw", cap)
    .withColumn("lead_min", (F.unix_timestamp("period_end") - F.unix_timestamp("origin")) / 60)
    # Weather vs clear sky
    .withColumn("csi_ghi", F.when(F.col("clearsky_ghi") > 50, F.least(F.col("f_ghi") / F.col("clearsky_ghi"), F.lit(1.5))))
    .withColumn("kt", F.when(F.col("etr_horizontal") > 50, F.col("f_ghi") / F.col("etr_horizontal")))
    .withColumn("diffuse_frac", F.when(F.col("f_ghi") > 20, F.least(F.col("f_dhi") / F.col("f_ghi"), F.lit(1.0))))
    # Reuniwatt power, normalised to capacity
    .withColumn("power_norm", F.col("f_power_mw") / cap)
    .withColumn("band_width_norm", (F.col("f_power_p90_mw") - F.col("f_power_p10_mw")) / cap)
    .withColumn("clip_flag", (F.col("f_power_mw") >= 0.97 * cap).cast("int"))
    # Panel temperature (day-ahead weather, used for every product)
    .withColumn("t_cell_c", F.col("da_temp_c") + 0.031 * F.col("f_gti_poa"))
    .withColumn("temp_derate", 1 - 0.004 * (F.col("t_cell_c") - 25))
)
if "da" in prefixes and "wa" in prefixes:
    rows = rows.withColumn("disagree_da_wa", F.abs(F.col("da_power_mw") - F.col("wa_power_mw")) / cap)
if "id" in prefixes:
    rows = rows.withColumn("disagree_id_da", F.abs(F.col("id_power_mw") - F.col("da_power_mw")) / cap)

# COMMAND ----------

# MAGIC %md ## State at the origin (actuals up to the origin)
# MAGIC Recent PV output, its trend, and how far Reuniwatt has been off in the last hour (actual ÷ Reuniwatt's most recent forecast for those periods).

# COMMAND ----------

nowcast = (
    fc.where("power_mw IS NOT NULL")
    .withColumn("_rk", F.row_number().over(Window.partitionBy("site", "period_end").orderBy(F.desc("reference_time"))))
    .where("_rk = 1").select("site", "period_end", F.col("power_mw").alias("nc_power_mw"))
)
series = pv.select("site", "period_end", F.when(F.col("label_ok"), F.col("pv_mw")).alias("pv"), "clearsky_ghi", "capacity_mw") \
    .join(nowcast, ["site", "period_end"], "left")

ts = F.unix_timestamp("period_end")
w = Window.partitionBy("site").orderBy(ts)
w30 = w.rangeBetween(-25 * 60, 0)
w60 = w.rangeBetween(-55 * 60, 0)
both = F.col("pv").isNotNull() & F.col("nc_power_mw").isNotNull()
state = (
    series
    .withColumn("o_pv_norm", F.col("pv") / F.col("capacity_mw"))
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
rows = rows.join(state, ["site", "origin"], "left")

# COMMAND ----------

# MAGIC %md ## Label and write

# COMMAND ----------

label = pv.select("site", "period_end", F.col("pv_mw").alias("label_pv_mw"), F.col("pv_norm").alias("label_pv_norm"),
                  F.col("status").alias("label_status"), "label_ok")
rows = rows.join(label, ["site", "period_end"], "left").withColumn("product", F.lit(PRODUCT))

main = PREFIX[CFG["uses"][0]]
out = write(rows.where(F.col(f"{main}_run_time").isNotNull()), f"fe_features_{PRODUCT}_5min", partition_by=("site",))

# COMMAND ----------

display(
    out.groupBy("site", F.date_trunc("month", "origin").alias("month")).agg(
        F.countDistinct("origin").alias("origins"),
        F.count("*").alias("rows"),
        F.round(100 * F.avg(F.col("f_power_mw").isNotNull().cast("int")), 1).alias("pct_with_forecast"),
        F.round(100 * F.avg(F.col("label_ok").cast("int")), 1).alias("pct_label_ok"),
        F.round(F.corr("power_norm", "label_pv_norm"), 3).alias("corr_reuni_vs_actual"),
    ).orderBy("site", "month")
)
