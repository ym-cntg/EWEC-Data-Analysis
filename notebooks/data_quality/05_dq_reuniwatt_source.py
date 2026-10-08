# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · DQ — Reuniwatt historical weather source
# MAGIC The new historical weather/irradiance data Harish ingested, in catalog **`ewec_dev_reuniwatt`**. Mentioned in meetings as "renewal".
# MAGIC
# MAGIC - Section 0 lists every table and its columns in `ewec_dev_reuniwatt`. Use it to pick `reuniwatt_table` and `ts_col`.
# MAGIC - With those widgets set, it runs the generic profile and compares coverage with Solcast for the same site.
# MAGIC
# MAGIC Run `04_dq_source_alignment` with the same table to check time offset and site mapping against PV.

# COMMAND ----------

dbutils.widgets.text("scratch_schema", "", "Scratch schema (optional, catalog.schema)")
dbutils.widgets.text("reuniwatt_table", "", "Reuniwatt table (ewec_dev_reuniwatt.<schema>.<table>)")
dbutils.widgets.text("ts_col", "", "Timestamp column")
dbutils.widgets.text("site_col", "", "Site column (optional)")
dbutils.widgets.dropdown("compare_site", "PV1", ["PV1", "PV2"], "Solcast site to compare")

# COMMAND ----------

# MAGIC %run ./00_dq_utils

# COMMAND ----------

TABLE = dbutils.widgets.get("reuniwatt_table").strip()
TS = dbutils.widgets.get("ts_col").strip()
SITE_COL = dbutils.widgets.get("site_col").strip()

# COMMAND ----------

# MAGIC %md ## 0. Inventory of `ewec_dev_reuniwatt`
# MAGIC Every table, then its columns. Look for a timestamp column, a site/location column, and irradiance columns (`ghi`, `dni`, `dhi`, …).

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_schema, table_name, table_type, created, last_altered, comment
    FROM {REUNIWATT_CATALOG}.information_schema.tables
    WHERE table_schema <> 'information_schema'
    ORDER BY table_schema, table_name
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_schema, table_name,
           count(*) AS n_columns,
           concat_ws(', ', collect_list(concat(column_name, ' ', data_type))) AS columns
    FROM (SELECT * FROM {REUNIWATT_CATALOG}.information_schema.columns
          WHERE table_schema <> 'information_schema' ORDER BY ordinal_position)
    GROUP BY table_schema, table_name
    ORDER BY table_schema, table_name
"""))

# COMMAND ----------

if not TABLE or not TS:
    dbutils.notebook.exit("Set reuniwatt_table and ts_col, then re-run.")

raw = spark.table(TABLE)
raw.printSchema()
display(spark.sql(f"DESCRIBE DETAIL {TABLE}"))

ren = raw.withColumn(TS, F.col(TS).cast("timestamp"))
by = [SITE_COL] if SITE_COL else []
num_cols = [f.name for f in ren.schema.fields if f.dataType.typeName() in ("double", "float", "integer", "long", "decimal", "short") and f.name not in by]

print("Solcast columns present:", [c for c in SOLCAST_COLS if c in ren.columns])
print("Solcast columns missing:", [c for c in SOLCAST_COLS if c not in ren.columns])
print("Numeric columns:", num_cols)

# COMMAND ----------

# MAGIC %md ## 1. Range, duplicates, interval

# COMMAND ----------

display(time_range(ren, TS, by))
dups = duplicate_timestamps(ren, by + [TS], num_cols[:5])
print("Duplicate keys:", dups.count(), "| conflicting:", dups.where("distinct_value_sets > 1").count())
display(interval_profile(ren, TS, by).limit(30))

# COMMAND ----------

# MAGIC %md ## 2. Completeness
# MAGIC Run per site when there is a site column.

# COMMAND ----------

sites = [r[0] for r in ren.select(SITE_COL).distinct().collect()] if SITE_COL else [None]
for s in sites:
    sub = ren.where(F.col(SITE_COL) == s) if s is not None else ren
    interval = infer_interval_min(sub, TS)
    label = f"site={s}" if s is not None else "all"
    print(f"--- {label}: native interval {interval} min")
    display(completeness_summary(sub, TS, interval))
    display(gaps(sub, TS, interval).limit(50))
    key = present(sub, ["ghi", "dni", "dhi", "gti", "air_temp"]) or num_cols[:5]
    per_day = completeness_per_day(sub, TS, interval, key)
    display(per_day.where(" OR ".join(f"{c}_pct < 99" for c in key) + " OR timestamp_pct < 99"))

# COMMAND ----------

# MAGIC %md ## 3. Nulls and value ranges

# COMMAND ----------

period = (
    F.when(F.col("zenith") < 90, "day").otherwise("night") if "zenith" in ren.columns
    else F.when(F.col("ghi") > 5, "day").otherwise("night") if "ghi" in ren.columns
    else None
)
display(save(null_rates(ren, num_cols, period), "reuniwatt_null_rates"))
display(save(column_stats(ren, num_cols), "reuniwatt_column_stats"))

# COMMAND ----------

irr = present(ren, IRRADIANCE_COLS)
rules = {
    "negative_irradiance": (" OR ".join(f"{c} < 0" for c in irr) or "false", irr),
    "irradiance_at_night": ("zenith > 90 AND ghi > 5", ["zenith", "ghi"]),
    "ghi_far_above_clearsky": ("ghi > clearsky_ghi * 1.2 AND ghi - clearsky_ghi > 50", ["ghi", "clearsky_ghi"]),
    "ghi_above_1400": ("ghi > 1400", ["ghi"]),
    "cloud_opacity_out_of_0_100": ("cloud_opacity < 0 OR cloud_opacity > 100", ["cloud_opacity"]),
    "rh_out_of_0_100": ("relative_humidity < 0 OR relative_humidity > 100", ["relative_humidity"]),
    "air_temp_implausible": ("air_temp < -5 OR air_temp > 60", ["air_temp"]),
}
display(save(rule_counts(ren, rules), "reuniwatt_rules"))

# COMMAND ----------

# MAGIC %md ## 4. Timezone hint

# COMMAND ----------

if "ghi" in ren.columns:
    prof = hourly_profile(ren, TS, ["ghi"])
    display(prof)
    peak = prof.orderBy(F.desc("avg_ghi")).first()["hour"]
    print(f"Peak GHI hour {peak:02d}:00 -> looks like {guess_timezone(peak)}")

# COMMAND ----------

# MAGIC %md ## 5. Compare with Solcast on overlapping timestamps
# MAGIC Uses an exact timestamp join. If the intervals or conventions differ, run 04 first to find the offset.

# COMMAND ----------

site = dbutils.widgets.get("compare_site")
sc = spark.table(SOLCAST_TABLES[site]).select(F.col("period_end").cast("timestamp").alias(TS), *[F.col(c).alias(f"solcast_{c}") for c in ["ghi", "dni", "dhi", "air_temp"]])
ren_site = ren.where(F.col(SITE_COL) == site) if SITE_COL else ren
common = [c for c in ["ghi", "dni", "dhi", "air_temp"] if c in ren_site.columns]
j = ren_site.select(TS, *common).join(sc, TS)
display(j.agg(
    F.count("*").alias("matched_ts"),
    *[F.corr(c, f"solcast_{c}").alias(f"{c}_corr") for c in common],
    *[F.avg(F.col(c) - F.col(f"solcast_{c}")).alias(f"{c}_mean_bias") for c in common],
    *[F.avg(F.abs(F.col(c) - F.col(f"solcast_{c}"))).alias(f"{c}_mae") for c in common],
))
