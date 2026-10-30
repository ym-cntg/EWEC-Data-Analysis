# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Data availability, full history
# MAGIC From when to when each source has **values** (not just rows), per variable and site:
# MAGIC - `ewec_dev_powerops.bronze.power_contango_minute_pv`: PV power and plant sensors (1-min, Gulf time)
# MAGIC - `ewec_dev_reuniwatt.bronze.satellite_weather_sts`: Reuniwatt observed satellite weather (UTC)
# MAGIC - `ewec_dev_reuniwatt.bronze.solar_forecast_raw`: Reuniwatt forecasts (JSON per run, UTC)
# MAGIC
# MAGIC Row counts are not enough: the Reuniwatt backfill has a run every 15 min with every value null. Every check here counts values.
# MAGIC
# MAGIC **Run all.** Takes 5–10 min. Writes `results/data_availability.txt` next to this notebook (commit and push it).
# MAGIC Findings as of 2026-10-29: `documentation/data_availability.md`.

# COMMAND ----------

dbutils.widgets.text("pv_table", "ewec_dev_powerops.bronze.power_contango_minute_pv", "PV table (dev / uat / sbx copy)")

# COMMAND ----------

import os
import pandas as pd
from pyspark.sql import functions as F

PV_TABLE = dbutils.widgets.get("pv_table").strip()
SAT_TABLE = "ewec_dev_reuniwatt.bronze.satellite_weather_sts"
FC_TABLE = "ewec_dev_reuniwatt.bronze.solar_forecast_raw"
GOOD = 95          # a month counts as complete at >= 95 % of expected timestamps with a value


def num(c):
    return F.expr(f"try_cast(`{c}` AS double)")


def with_ts(df, c):
    """Parse the timestamp and drop junk dates (before 2000 or in the future)."""
    return (df.withColumn("_ts", F.expr(f"try_cast(`{c}` AS timestamp)"))
              .where((F.col("_ts") >= F.lit("2000-01-01")) & (F.col("_ts") < F.date_add(F.current_date(), 2))))


def ranges(v, keep):
    """Months where keep(v) is true, collapsed into ranges: '2014-01→2024-12, 2026-07'."""
    out, start, prev = [], None, None
    for m, x in v.items():
        if keep(x) and start is None:
            start = m
        if not keep(x) and start is not None:
            out.append((start, prev)); start = None
        prev = m
    if start is not None:
        out.append((start, prev))
    return ", ".join(a if a == b else f"{a}→{b}" for a, b in out) or "none"


def monthly(d, cols, per_day, site=None):
    """% of expected timestamps with a numeric value, per month, from each site's first to last timestamp."""
    keys = [F.date_format("_ts", "yyyy-MM").alias("month")] + ([F.lower(F.col(site)).alias("site")] if site else [])
    aggs = [F.min("_ts").alias("_first"), F.max("_ts").alias("_last"), F.countDistinct("_ts").alias("timestamps")]
    aggs += [F.countDistinct(F.when(num(c).isNotNull(), F.col("_ts"))).alias(c) for c in cols]
    if "cloud_opacity" in cols:   # blank at night by design: score it on daylight timestamps only
        aggs += [F.countDistinct(F.when(num("ghi") > 0, F.col("_ts"))).alias("_day"),
                 F.countDistinct(F.when((num("ghi") > 0) & num("cloud_opacity").isNotNull(), F.col("_ts"))).alias("_cloud_day")]
    m = d.groupBy(*keys).agg(*aggs).toPandas()
    if not site:
        m["site"] = "-"
    out = []
    for s, g in m.groupby("site"):
        first = pd.Timestamp(g["_first"].min()).normalize()
        last = pd.Timestamp(g["_last"].max()).normalize() + pd.Timedelta(days=1)
        months = pd.period_range(first, last - pd.Timedelta(days=1), freq="M").astype(str)
        g = g.set_index("month").reindex(months)
        days = [(min(pd.Period(x, "M").end_time.ceil("D"), last) - max(pd.Period(x, "M").start_time, first)).days for x in months]
        exp = pd.Series(days, index=months, dtype=float) * per_day
        w = pd.DataFrame({"site": s}, index=months)
        for c in ["timestamps"] + cols:
            w[c] = (g[c].fillna(0) / exp * 100).round(1)
        if "cloud_opacity" in cols:
            w["cloud_opacity"] = (g["_cloud_day"] / g["_day"].where(g["_day"] > 0) * 100).round(1).fillna(0)
        out.append(w.rename_axis("month").reset_index())
    return pd.concat(out, ignore_index=True)


def first_last(d, cols, site=None):
    keys = [F.lower(F.col(site)).alias("site")] if site else []
    aggs = [F.min("_ts").alias("timestamps|first"), F.max("_ts").alias("timestamps|last")]
    for c in cols:
        aggs += [F.min(F.when(num(c).isNotNull(), F.col("_ts"))).alias(f"{c}|first"),
                 F.max(F.when(num(c).isNotNull(), F.col("_ts"))).alias(f"{c}|last")]
    return {(r["site"] if site else "-"): r for r in (x.asDict() for x in d.groupBy(*keys).agg(*aggs).collect())}


def summary(source, wide, fl):
    rows = []
    for s, g in wide.groupby("site"):
        g = g.set_index("month")
        for c in [c for c in g.columns if c != "site"]:
            v = g[c]
            rows.append({"source": source, "site": s, "variable": c,
                         "first_value": fl[s].get(f"{c}|first"), "last_value": fl[s].get(f"{c}|last"),
                         f"complete_periods (>={GOOD}%)": ranges(v, lambda x: x >= GOOD),
                         "partial_months": ranges(v, lambda x: 0 < x < GOOD),
                         "empty_months": ranges(v, lambda x: x == 0)})
    return pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · PV telemetry: power and plant sensors (all hours)
# MAGIC Blanks are counted before any forward-fill. SCADA leaves a blank when a value has not changed, so power and irradiance look partial over all hours (night = constant 0). Section 4 checks daytime only.

# COMMAND ----------

pv = spark.table(PV_TABLE)
low = {c.lower(): c for c in pv.columns}
PV_COLS = ["PV1_MW", "PV2_MW", "Pv2_AVL", "PV1_GHI", "PV1_ArrayRadiance_east", "PV1_ArrayRadiance_west", "PV1_Temp",
           "PV1_HumidityPer", "PV2_GHI", "PV2_ArrayRadiance", "PV2_Outdoor_Temp", "PV2_HumidityPer", "PV2_WindSpeed", "PV2_WindDirection"]
pv_cols = [low[c.lower()] for c in PV_COLS if c.lower() in low]
pv_d = with_ts(pv, low["datetime"])
pv_m = monthly(pv_d, pv_cols, 1440)
pv_s = summary("PV telemetry", pv_m, first_last(pv_d, pv_cols))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Reuniwatt satellite (observed weather)

# COMMAND ----------

sat = spark.table(SAT_TABLE)
sat_d = with_ts(sat, "datetime")
r = sat_d.select(F.countDistinct("_ts").alias("t"), F.countDistinct(F.to_date("_ts")).alias("d")).collect()[0]
sat_per_day = min([24, 48, 96, 144, 288], key=lambda k: abs(k - r["t"] / max(r["d"], 1)))
sat_cols = [c for c in ["ghi", "dni", "dhi", "temperature", "relative_humidity", "wind_speed", "wind_direction", "dewpoint_2m",
                        "pressure", "precip", "clearness_index", "cloud_opacity"] if c in sat.columns]
sat_m = monthly(sat_d, sat_cols, sat_per_day, site="site")
sat_s = summary("satellite (observed)", sat_m, first_last(sat_d, sat_cols, site="site"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Reuniwatt forecasts
# MAGIC Runs: all runs. Values: % filled inside the latest copy of each run, sampled one run every 3 h. Skips `migration_legacy` (different JSON format; see section 5).

# COMMAND ----------

fc_runs = spark.sql(f"""
  SELECT date_format(rt, 'yyyy-MM') AS month, site, horizon, MIN(rt) AS first_run, MAX(rt) AS last_run, COUNT(DISTINCT rt) AS runs,
         COUNT(DISTINCT to_date(rt)) AS days_with_runs
  FROM (SELECT try_cast(reference_time AS TIMESTAMP) AS rt, lower(site) AS site, lower(horizon) AS horizon
        FROM {FC_TABLE} WHERE _source <> 'migration_legacy')
  WHERE rt IS NOT NULL GROUP BY ALL""").toPandas()
fc_vals = spark.sql(f"""
  WITH runs AS (
    SELECT lower(site) AS site, lower(horizon) AS horizon, try_cast(reference_time AS TIMESTAMP) AS rt, _raw,
           row_number() OVER (PARTITION BY lower(site), lower(horizon), reference_time ORDER BY _ingested_at DESC) AS rk
    FROM {FC_TABLE} WHERE _source <> 'migration_legacy'
  ),
  vals AS (
    SELECT site, horizon, month, variable,
           size(map_values(series)) AS n_values,
           size(filter(map_values(series), x -> x IS NOT NULL)) AS n_filled
    FROM (SELECT site, horizon, date_format(rt, 'yyyy-MM') AS month,
                 try_element_at(from_json(_raw, 'MAP<STRING, MAP<STRING, MAP<STRING, DOUBLE>>>'), upper(site)) AS m
          FROM runs WHERE rk = 1 AND minute(rt) = 0 AND hour(rt) % 3 = 0)
    LATERAL VIEW explode(m) t AS variable, series
  )
  SELECT month, site, horizon, variable, SUM(n_filled) * 100.0 / NULLIF(SUM(n_values), 0) AS pct
  FROM vals GROUP BY ALL""").toPandas()

fc_rows = []
for (s, h), g in fc_runs.groupby(["site", "horizon"]):
    months = pd.period_range(g["month"].min(), g["month"].max(), freq="M").astype(str)
    days = g.set_index("month")["days_with_runs"].reindex(months).fillna(0)
    fc_rows.append({"source": f"forecast · {h}", "site": s, "variable": "runs",
                    "first_value": g["first_run"].min(), "last_value": g["last_run"].max(),
                    f"complete_periods (>={GOOD}%)": ranges(days, lambda x: x >= 27),
                    "partial_months": ranges(days, lambda x: 0 < x < 27), "empty_months": ranges(days, lambda x: x == 0)})
    vals = fc_vals[(fc_vals.site == s) & (fc_vals.horizon == h)]
    for v, gv in vals.groupby("variable"):
        p = gv.set_index("month")["pct"].reindex(months).fillna(0)
        has = p[p > 0]
        fc_rows.append({"source": f"forecast · {h}", "site": s, "variable": v,
                        "first_value": has.index.min() if len(has) else "none", "last_value": has.index.max() if len(has) else "none",
                        f"complete_periods (>={GOOD}%)": ranges(p, lambda x: x >= GOOD),
                        "partial_months": ranges(p, lambda x: 0 < x < GOOD), "empty_months": ranges(p, lambda x: x == 0)})
fc_s = pd.DataFrame(fc_rows)
fc_key = (fc_vals[(fc_vals.horizon == "dayahead") & fc_vals.variable.isin(["ghi", "temperature", "power"])]
          .assign(k=lambda x: x.site + "_" + x.variable).pivot(index="month", columns="k", values="pct").sort_index().round(1).reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · PV power and irradiance, daytime only (06:00–18:00 Gulf)
# MAGIC High % = the all-hours blanks are night / unchanged values. Low % = real gaps.

# COMMAND ----------

PV_DAY = ["PV1_MW", "PV2_MW", "Pv2_AVL", "PV1_GHI", "PV2_GHI", "PV1_ArrayRadiance_east", "PV1_ArrayRadiance_west", "PV2_ArrayRadiance"]
pv_day_cols = [low[c.lower()] for c in PV_DAY if c.lower() in low]
dd = pv_d.where(F.hour("_ts").between(6, 17))
pv_day = (dd.groupBy(F.date_format("_ts", "yyyy-MM").alias("month"))
            .agg(F.countDistinct("_ts").alias("rows"),
                 *[F.countDistinct(F.when(num(c).isNotNull(), F.col("_ts"))).alias(c) for c in pv_day_cols])
            .toPandas().sort_values("month"))
exp = pv_day["month"].map(lambda m: pd.Period(m, "M").days_in_month * 720)
for c in ["rows"] + pv_day_cols:
    pv_day[c] = (pv_day[c] / exp * 100).round(1)
display(pv_day)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Forecast values by ingestion source (one day-ahead run per day, 06:00 UTC)
# MAGIC Shows which ingestion holds values. As of 2026-10-29: `api_historical_backfill` 0 % for 2023–2025; live feeds 100 %; `migration_legacy` null (different JSON format).

# COMMAND ----------

by_source = spark.sql(f"""
  WITH r AS (
    SELECT _source, lower(site) AS site, year(try_cast(reference_time AS TIMESTAMP)) AS yr,
           try_element_at(try_element_at(from_json(_raw, 'MAP<STRING, MAP<STRING, MAP<STRING, DOUBLE>>>'), upper(site)), 'ghi') AS g,
           _ingested_at
    FROM {FC_TABLE}
    WHERE lower(horizon) = 'dayahead' AND hour(reference_time) = 6 AND minute(reference_time) = 0
  )
  SELECT _source, site, yr, COUNT(*) AS runs, MIN(_ingested_at) AS first_ingested, MAX(_ingested_at) AS last_ingested,
         ROUND(SUM(size(filter(map_values(g), x -> x IS NOT NULL))) * 100.0 / NULLIF(SUM(size(map_values(g))), 0), 1) AS ghi_filled_pct
  FROM r GROUP BY ALL ORDER BY _source, site, yr""").toPandas()
display(by_source)

# COMMAND ----------

# Sample of the legacy rows, to see their JSON format
display(spark.sql(f"""
  SELECT site, horizon, product_id, reference_time, length(_raw) AS raw_len, substr(_raw, 1, 1200) AS raw_start
  FROM {FC_TABLE} WHERE _source = 'migration_legacy' ORDER BY reference_time DESC LIMIT 5"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Report

# COMMAND ----------

answer = pd.concat([pv_s, sat_s, fc_s], ignore_index=True)
pd.set_option("display.max_colwidth", 200)
parts = [f"# Data availability, full history · run {pd.Timestamp.utcnow():%Y-%m-%d %H:%M} UTC",
         f"PV table: {PV_TABLE}. Monthly % = share of expected timestamps with a numeric value (blanks counted before forward-fill). "
         f"Complete = >= {GOOD} %. Satellite resolution detected: {1440 // sat_per_day} min. Cloud opacity scored on daylight only. "
         "Forecast 'runs' complete = runs on >= 27 days of the month; forecast variables = % of values filled inside the runs.",
         "## A · Availability per source and variable\n" + answer.to_string(index=False),
         "## B · PV telemetry, % filled by month (all hours)\n" + pv_m.to_string(index=False),
         "## C · PV power and irradiance, % filled by month (06:00–18:00 Gulf)\n" + pv_day.to_string(index=False),
         "## D · Satellite, % filled by month\n" + sat_m.to_string(index=False),
         "## E · Forecast day-ahead values filled %, by month\n" + fc_key.to_string(index=False),
         "## F · Forecast values by ingestion source\n" + by_source.to_string(index=False)]
report = "\n\n".join(parts)
print(report)
os.makedirs("results", exist_ok=True)
open("results/data_availability.txt", "w").write(report)
display(answer)
