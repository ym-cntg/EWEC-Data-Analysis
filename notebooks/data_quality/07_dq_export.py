# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Export all DQ results as text
# MAGIC Reads every `dq_*` table in `scratch_schema` and prints them as **one block of text**. Copy the whole output of the last cell and paste it to Claude.
# MAGIC
# MAGIC - Summary tables are printed in full, up to `max_rows`. Long detail lists (gaps, low days, stuck runs …) print their total row count and the first `detail_rows` rows.
# MAGIC - The same text is written to `results/dq_export.txt` in this Git folder. Commit and push it (Git dialog), and Claude can pull and read it.

# COMMAND ----------

dbutils.widgets.text("scratch_schema", "ewec_dev_powerops.scratch_yash", "Scratch schema (catalog.schema)")
dbutils.widgets.text("max_rows", "60", "Max rows per summary table")
dbutils.widgets.text("detail_rows", "12", "Rows per long detail list")

# COMMAND ----------

from datetime import datetime

SCHEMA = dbutils.widgets.get("scratch_schema").strip()
MAX_ROWS = int(dbutils.widgets.get("max_rows"))
DETAIL_ROWS = int(dbutils.widgets.get("detail_rows"))

# Long row-level lists: print the count and the first rows only
DETAIL_SUFFIXES = (
    "_gaps", "_low_days", "_low_coverage_days", "_stuck_runs", "_missing_runs", "_masked_runs",
    "_completeness_per_day", "_history", "_hourly_profile", "_lag_scan", "_lag_scan_hourly", "_lag_scan_5min",
    "_lag_scan_vs_pv", "_monthly_timestamps", "_day_zero_monthly", "_backup_snapshots", "_accuracy_vs_pv",
)
# Print first so the most important context comes at the top
FIRST = ["dq_run_log"]
ORDER = ["pv_", "solcast_pv1_", "solcast_pv2_", "source_", "lag_", "site_mapping", "alignment_", "silver_", "reuniwatt_dim", "reuniwatt_irradiance_", "reuniwatt_power_"]


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.4g}" if abs(v) < 1e6 else f"{v:.0f}"
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(fmt(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"{k}={fmt(x)}" for k, x in v.items()) + "}"
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M")
    return str(v).replace("\n", " ")[:200]


def render(name: str) -> str:
    df = spark.table(f"{SCHEMA}.{name}")
    if "_row" in df.columns:
        df = df.orderBy("_row").drop("_row")
    total = df.count()
    is_detail = name.endswith(DETAIL_SUFFIXES)
    limit = DETAIL_ROWS if is_detail else MAX_ROWS
    rows = df.limit(limit).collect()
    shown = f"{len(rows)} of {total} rows" if total > len(rows) else f"{total} rows"
    if name.endswith("_notes"):  # key/value notes read better as lines
        body = "\n".join(f"  {r['key']}: {r['value']}" for r in rows)
    else:
        body = "\n".join(" | ".join(fmt(v) for v in r) for r in rows)
        body = " | ".join(df.columns) + "\n" + body
    return f"### {name.removeprefix('dq_')} ({shown})\n{body}\n"


tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {SCHEMA} LIKE 'dq_*'").collect()]


def sort_key(t):
    if t in FIRST:
        return (0, 0, t)
    base = t.removeprefix("dq_")
    group = next((i for i, p in enumerate(ORDER) if base.startswith(p)), len(ORDER))
    return (1, group, "" if base.endswith("_notes") else base)


parts = [f"# DQ EXPORT · {SCHEMA} · {datetime.utcnow():%Y-%m-%d %H:%M} UTC · {len(tables)} tables\n"]
for t in sorted(tables, key=sort_key):
    try:
        parts.append(render(t))
    except Exception as e:
        parts.append(f"### {t}\n  (could not read: {e})\n")
text = "\n".join(parts)

import os

# Write into the Git folder (<repo>/results/dq_export.txt) so it can be committed and pulled.
try:
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    nb_dir = os.path.dirname(nb_path if nb_path.startswith("/Workspace") else "/Workspace" + nb_path)
    repo_root = os.path.normpath(os.path.join(nb_dir, "..", ".."))
    os.makedirs(os.path.join(repo_root, "results"), exist_ok=True)
    path = os.path.join(repo_root, "results", "dq_export.txt")
    with open(path, "w") as f:
        f.write(text)
    print(f"(saved to {path}, {len(text):,} characters: commit and push it from the Git dialog)\n")
except Exception as e:
    print(f"(could not save to the repo: {e})\n")

# COMMAND ----------

print(text)
