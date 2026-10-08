# EWEC-Data-Analysis

Data-quality and completeness checks for the UC2 EWEC solar forecasting source tables (Databricks). See `CLAUDE.md` for project context.

## Data-quality notebooks (`notebooks/data_quality/`)

Databricks source-format notebooks. Import them via Repos, then run each one on a cluster with read access to `ewec_dev_powerops`.

| Notebook | Checks |
|---|---|
| `00_dq_utils` | Shared constants and helpers (loaded by the others via `%run`) |
| `01_dq_bronze_pv_telemetry` | `bronze.power_contango_minute_pv`: range, gaps, per-day completeness, nulls by hour, negative/over-capacity values, stuck values, timezone |
| `02_dq_bronze_solcast` | `bronze.pv{1,2}_history_solcast`: forecast vs. actuals, future rows, completeness, day/night nulls, physical validity rules, stuck values, timezone |
| `03_dq_silver_masking` | `silver.uc2_solar_5min` vs. Bronze: confirms the PV1/PV2 swap and bucket convention, and quantifies daytime zeros that are really missing data |
| `04_dq_source_alignment` | Date overlap, PV↔irradiance lag correlation (timezone / `period_end` offset), site mapping |
| `05_dq_renewal_source` | Finds and profiles the new "renewal" historical weather tables, and compares them with Solcast |

All notebooks are read-only. To persist summary tables, set the `scratch_schema` widget to a personal schema. Writes to `bronze`/`silver`/`gold` are refused.
