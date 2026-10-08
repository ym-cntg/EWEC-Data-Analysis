# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · DQ — Source alignment
# MAGIC Checks across PV telemetry, Solcast PV1/PV2 and (optionally) the renewal source:
# MAGIC 1. Date-range overlap and monthly coverage per source
# MAGIC 2. **Time offset** between PV and irradiance, found by lag correlation. This exposes timezone or `period_end` mismatches.
# MAGIC 3. **Site mapping**: which Solcast table tracks which Bronze PV column
# MAGIC
# MAGIC All in Bronze (upstream) labels.

# COMMAND ----------

dbutils.widgets.text("scratch_schema", "", "Scratch schema (optional, catalog.schema)")
dbutils.widgets.text("renewal_table", "", "Renewal table (optional)")
dbutils.widgets.text("renewal_ts_col", "", "Renewal timestamp column")
dbutils.widgets.text("renewal_ghi_col", "ghi", "Renewal GHI column")

# COMMAND ----------

# MAGIC %run ./00_dq_utils

# COMMAND ----------

pv = spark.table(PV_TABLE).select(F.col("DateTime").cast("timestamp").alias("ts"), *PV_COLS)
solcast = {s: spark.table(t).select(F.col("period_end").cast("timestamp").alias("ts"), "ghi", *present(spark.table(t), ["gti", "clearsky_ghi"])) for s, t in SOLCAST_TABLES.items()}

sources = {"pv_telemetry": pv, **{f"solcast_{s.lower()}": df for s, df in solcast.items()}}

REN = dbutils.widgets.get("renewal_table").strip()
if REN:
    ren_ts, ren_ghi = dbutils.widgets.get("renewal_ts_col"), dbutils.widgets.get("renewal_ghi_col")
    sources["renewal"] = spark.table(REN).select(F.col(ren_ts).cast("timestamp").alias("ts"), F.col(ren_ghi).alias("ghi"))

# COMMAND ----------

# MAGIC %md ## 1. Date ranges and overlap

# COMMAND ----------

ranges = reduce(DataFrame.unionByName, [time_range(df, "ts").withColumn("source", F.lit(n)) for n, df in sources.items()])
display(save(ranges.select("source", "min_ts", "max_ts", "rows", "distinct_ts"), "source_ranges"))

r = ranges.agg(F.max("min_ts").alias("start"), F.min("max_ts").alias("end")).first()
print(f"Common overlap window: {r.start} -> {r.end}")

# COMMAND ----------

monthly = reduce(DataFrame.unionByName, [
    df.groupBy(F.date_trunc("month", "ts").alias("month")).agg(F.countDistinct("ts").alias("timestamps")).withColumn("source", F.lit(n))
    for n, df in sources.items()
]).groupBy("month").pivot("source").sum("timestamps").orderBy("month")
display(save(monthly, "source_monthly_timestamps"))

# COMMAND ----------

# MAGIC %md ## 2. Time offset via lag correlation
# MAGIC Solcast is shifted by `offset` and correlated with PV. If the best offset is `k`, then PV time ≈ Solcast time + k.
# MAGIC - Coarse (hourly means, ±8 h): a 4 h offset means one source is UTC and the other Asia/Dubai.
# MAGIC - Fine (5-min buckets, ±60 min): exposes period-start vs. period-end differences.

# COMMAND ----------

def bucketed(df, cols, minutes):
    b = F.timestamp_seconds(F.floor(F.unix_timestamp("ts") / (minutes * 60)) * minutes * 60)
    return df.groupBy(b.alias("ts")).agg(*[F.avg(c).alias(c) for c in cols])

def lag_scan(minutes, offsets_min):
    pv_b = bucketed(pv, PV_COLS, minutes)
    rows = []
    for name, df in [(n, d) for n, d in sources.items() if n != "pv_telemetry"]:
        irr = bucketed(df, ["ghi"], minutes)
        for off in offsets_min:
            shifted = irr.withColumn("ts", F.timestamp_seconds(F.unix_timestamp("ts") + off * 60))
            j = pv_b.join(shifted, "ts")
            agg = j.agg(*[F.corr(c, "ghi").alias(c) for c in PV_COLS], F.count("*").alias("n")).first()
            rows += [(name, c, off, agg[c], agg["n"]) for c in PV_COLS]
    return spark.createDataFrame(rows, "source string, pv_col string, offset_min int, corr double, n long")

coarse = lag_scan(60, [h * 60 for h in range(-8, 9)])
display(save(coarse.orderBy("source", "pv_col", "offset_min"), "lag_scan_hourly"))

best_coarse = coarse.withColumn("rk", F.row_number().over(Window.partitionBy("source", "pv_col").orderBy(F.desc("corr")))).where("rk = 1").drop("rk")
display(best_coarse)

# COMMAND ----------

center = int(best_coarse.agg(F.expr("percentile_approx(offset_min, 0.5)")).first()[0])
fine = lag_scan(5, list(range(center - 60, center + 61, 5)))
display(save(fine.orderBy("source", "pv_col", "offset_min"), "lag_scan_5min"))
display(fine.withColumn("rk", F.row_number().over(Window.partitionBy("source", "pv_col").orderBy(F.desc("corr")))).where("rk = 1").drop("rk"))

# COMMAND ----------

# MAGIC %md ## 3. Site mapping
# MAGIC Correlation of each Bronze PV column against each irradiance source, at the best coarse offset. Clear-sky days look alike at both sites, so differences may be small. Compare against cloudy days as well.

# COMMAND ----------

pv_h = bucketed(pv, PV_COLS, 60)
mat = []
for name, df in [(n, d) for n, d in sources.items() if n != "pv_telemetry"]:
    irr = bucketed(df, ["ghi"], 60).withColumn("ts", F.timestamp_seconds(F.unix_timestamp("ts") + center * 60))
    j = pv_h.join(irr, "ts")
    agg = j.agg(*[F.corr(c, "ghi").alias(c) for c in PV_COLS]).first()
    mat.append((name, *[agg[c] for c in PV_COLS]))
display(spark.createDataFrame(mat, "source string, " + ", ".join(f"{c} double" for c in PV_COLS)))

print("PV1_MW vs PV2_MW corr:", pv.agg(F.corr("PV1_MW", "PV2_MW")).first()[0])

# COMMAND ----------

# MAGIC %md ## 4. Are the two Solcast tables actually different locations?

# COMMAND ----------

s1, s2 = (solcast["PV1"].select("ts", F.col("ghi").alias("ghi_pv1")), solcast["PV2"].select("ts", F.col("ghi").alias("ghi_pv2")))
display(s1.join(s2, "ts").agg(
    F.count("*").alias("common_ts"),
    F.round(100 * F.avg((F.col("ghi_pv1") == F.col("ghi_pv2")).cast("int")), 2).alias("identical_ghi_pct"),
    F.corr("ghi_pv1", "ghi_pv2").alias("corr"),
    F.avg(F.abs(F.col("ghi_pv1") - F.col("ghi_pv2"))).alias("mean_abs_diff"),
))
