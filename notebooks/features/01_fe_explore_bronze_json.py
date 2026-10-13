# Databricks notebook source
# MAGIC %md
# MAGIC # FE 01 · Explore the Bronze JSON layout
# MAGIC The Bronze table keeps each Reuniwatt API response as raw JSON in `_raw`. To write the Bronze parser (notebook 02, `source = bronze`), we need the JSON structure.
# MAGIC
# MAGIC **Run all.** The output is saved to `results/bronze_json_layout.txt` next to this notebook. Commit and push that file (Git dialog) so Claude can read it.

# COMMAND ----------

dbutils.widgets.text("output_schema", "ewec_dev_powerops.scratch_yash", "Output schema (unused here)")

# COMMAND ----------

# MAGIC %run ./00_fe_common

# COMMAND ----------

import os

raw = spark.table(REUNI_BRONZE)
lines = ["# Bronze JSON layout", "", "## Columns"] + [f"{f.name}: {f.dataType.simpleString()}" for f in raw.schema.fields] + [""]

overview = raw.groupBy("_source", "site", "horizon", "product_id").agg(
    F.count("*").alias("rows"),
    F.min("reference_time").alias("first_issue"),
    F.max("reference_time").alias("last_issue"),
    F.round(F.avg(F.length("_raw"))).alias("avg_json_chars"),
).orderBy("_source", "site", "horizon")
display(overview)
lines += ["## Rows per source / site / horizon", " | ".join(overview.columns)]
lines += [" | ".join(str(v) for v in r) for r in overview.collect()]

# COMMAND ----------

# One sample per source (day-ahead PV1 where available), with its inferred JSON schema and the first 4,000 characters.
for src in [r[0] for r in raw.select("_source").distinct().collect()]:
    sub = raw.where(F.col("_source") == src)
    pick = sub.where(F.col("product_id").contains("dayahead-1"))
    row = (pick if pick.limit(1).count() else sub).orderBy(F.desc("reference_time")).select("product_id", "reference_time", "_raw").first()
    schema = spark.range(1).select(F.schema_of_json(F.lit(row["_raw"]))).first()[0]
    lines += ["", f"## _source = {src}", f"product_id = {row['product_id']}, reference_time = {row['reference_time']}",
              "", "### Inferred schema", schema, "", "### First 4000 characters", row["_raw"][:4000]]

text = "\n".join(lines)
os.makedirs("results", exist_ok=True)
with open("results/bronze_json_layout.txt", "w") as f:
    f.write(text)
print(f"Saved {os.path.abspath('results/bronze_json_layout.txt')} ({len(text):,} chars). Commit and push it.\n")
print(text[:20000])
