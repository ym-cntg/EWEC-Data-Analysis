# CLAUDE.md: guide for AI agents working on UC2 EWEC Solar

Read this first. Then read `documentation/HANDOVER.md`, which has the full state of the work, results and open items, and `documentation/data_availability.md`.

## Project

- **What:** solar PV power forecasting for EWEC's two plants, run by Contango on Databricks ("UC2 solar"). This repo is Phase 2, started Oct 2026.
- **Product:** day-ahead 15-min first.
- **Inputs:** Reuniwatt weather and power forecasts (new) and Solcast (Phase 1).

**People:**

| Person | Role |
|---|---|
| Yash Mahadeshwar | Does the work |
| Zaynab Habibi | Lead; asks the questions; audience for findings |
| Harish Naidu | Director |
| Nitheesh Panicker | Owns the Reuniwatt ingestion and tables |

## Rules for agents (learned the hard way)

1. **Check values, not rows.**
   - Before any correlation, model or claim that data is "complete", profile null % **by month**, distributions and time ranges for every variable.
   - The Reuniwatt backfill has a run every 15 min with every value null, so row counts looked complete for weeks.
   - Use `notebooks/data_quality/08_dq_data_availability.py`, or the same logic.
2. **Ask before coding.** Confirm the plan and the feature set with Yash before implementing anything new. Bug fixes to code you wrote are fine; say so.
3. **MLflow autologging always off** unless Yash says otherwise: call `mlflow.autolog(disable=True)` before any `.fit`. Databricks enables it by default; it logged hundreds of runs and made one cell take 45 min.
4. **Plant naming flips between sources.** Compare by plant name. See below.
5. **Never write to bronze, silver or gold.** Write to the scratch schema `ewec_dev_powerops.scratch_yash` only. The pipeline's `DataCatalog` refuses other writes.
6. **Push only when Yash says "push".** Work on `main`.
   - Results flow: Yash runs in Databricks → commits `results/` → you pull and read.
   - Don't commit `.DS_Store`.
7. **Writing style for Yash and Zaynab:**
   - concise and numbers-first;
   - tables for per-column results;
   - no emojis in messages to send;
   - Gulf time when talking about the plants.
   - State the table, period and % affected for every finding.
   - If a result was wrong, say so plainly and correct it.

## Plants and naming

| Ours (pipeline, Reuniwatt, plant sensors) | Plant | Capacity | Phase 1 doc and Solcast tables |
|---|---|---|---|
| **pv1** | Sweihan / Noor, small, east/west panels | 935 MW | PV2 (`pv2_history_solcast`) |
| **pv2** | Al Dhafrah, large, tracker-like | 1,584 MW | PV1 (`pv1_history_solcast`) |

**Bronze MW column swap:** before 2026-03-17, `PV1_MW` held the large plant. It is handled by `_detect_swap_date` and the `bronze_col_before_swap` / `after_swap` settings in `conf/parameters.yml`. Sensor columns were never swapped.

**Phase 1 baseline** (day-ahead 15-min, 1 Sep – 19 Nov 2025, MAPE where actual > 150 MW):
- Al Dhafrah (our pv2): 8.73 %
- Sweihan (our pv1): 5.23 %

## Data (see `documentation/data_availability.md`)

| Table | Content | Key facts |
|---|---|---|
| `ewec_dev_powerops.bronze.power_contango_minute_pv` | 1-min telemetry, **Gulf time** | From 2023-03-18; see below |
| `ewec_dev_reuniwatt.bronze.solar_forecast_raw` | Reuniwatt forecasts, one row per run | See below |
| `ewec_dev_reuniwatt.bronze.satellite_weather_sts` | Reuniwatt **observed** satellite weather, 15-min, UTC | 2014-01 → 2024-12, complete; 4 sites (pv1–pv4; pv3/pv4 unknown); `cloud_opacity` is 0–1 and daytime only |
| `ewec_dev_powerops.bronze.pv1_history_solcast` / `pv2_history_solcast` | Solcast, 15-min, `period_end` (UTC, 15-min average) | Jan 2023 → 2025-11-19; pv2 table has no `relative_humidity`; `cloud_opacity` is 0–100 |
| `ewec_dev_reuniwatt.silver.fact_solar_irradiance` / `fact_solar_power_forecast` | Partly processed Reuniwatt | We read Bronze instead |

**PV telemetry table:**
- **Columns:**
  - power: `PV1_MW`, `PV2_MW`, `Pv2_AVL`; ignore `PV_Misc_MW`;
  - sensors: `PV1_GHI`, `PV1_ArrayRadiance_east/west`, `PV1_Temp`, `PV1_HumidityPer`, `PV2_GHI`, `PV2_ArrayRadiance` (2026+), `PV2_Outdoor_Temp`, `PV2_HumidityPer`, `PV2_WindSpeed/Direction`.
- **`Load_Datetime`:** UTC ISO; data arrives 5–10 min after measurement.
- **Blanks:** SCADA leaves a blank when a value hasn't changed. Zaynab's rule is to forward-fill.
- **Copies:** also in `ewec_uat_powerops` and `ewec_sbx_powerops`.

**Reuniwatt forecasts (`solar_forecast_raw`):**
- **Format:** `_raw` JSON = `MAP<site upper-case, MAP<variable, MAP<UTC time, value>>>`.
- **Timing:** a run every 15 min, values every 5 min.
- **Power is in kW.**
- **Horizons:** `intraday`, `dayahead`, `weekahead`.
- **Sources:** `api_historical_backfill`, `api_incremental` (live), `migration_new_pipeline`, `migration_legacy` (different format; skipped).
- **⚠️ All values are null before ~3 Jul 2026, in every copy.** Usable forecast history is about 3 months.

## Decisions (agreed with Zaynab)

- **Targets:** actual MW for both plants. `Pv2_AVL` was dropped: noisy and 22 % blank.
- **Metrics:** MAPE only where actual > 150 MW; nMAE, RMSE and bias as % of capacity (`src/uc2_solar/metrics.py`).
- **Model granularity:** one LightGBM per clock slot (forecast time 09:00 / 15:00 Gulf × 15-min step × plant). Label = target ÷ capacity.
- **Daytime only:** 06:00–18:00 Gulf.
- **No look-ahead:** as-of joins on runs issued at least 15 min before the forecast time.
- **No plant sensors as model features,** because they are not available at forecast time.
- **Test windows:** `phase1_window` (2025-09-01 → 2025-11-19) and `recent_3m`.
- **Solcast vs Reuniwatt:** not interchangeable one-for-one. Only temperature matches; GHI is about 5 % low in the satellite; cloud, DNI/DHI and wind differ.

## Code

- **Kedro-style pipeline** (plain Python, no Kedro install):
  - `conf/parameters.yml` and `conf/catalog.yml` hold settings and datasets;
  - `src/uc2_solar/`: `framework.py`, `runner.py`, `pipelines/{data_engineering, feature_engineering, model_training, evaluation}/nodes.py`;
  - details in `documentation/pipeline.md`.
- **Runners:** `notebooks/pipeline/00–04`. They need `%pip install -q pyyaml lightgbm`, widgets, and `sys.path` pointing to `src`.
- **Analysis notebooks:** `notebooks/pipeline/05–07`. **DQ notebooks:** `notebooks/data_quality/` (Databricks `.py` source format; `08` = availability).
- **Databricks constraints:**
  - serverless with **ANSI mode**: guard divisions with `NULLIF`, and use `try_cast`, `try_to_timestamp` and `try_element_at`;
  - Spark Connect: no `.cache()`, no RDD;
  - set `spark.sql.session.timeZone = UTC`.
- **Solar geometry is pure Spark:** NOAA sun position, Haurwitz clear-sky, Kasten–Young air mass (`spark_utils.add_solar`).

## Legacy Phase 1 code (not in this repo)

The Phase 1 pipeline (`databricks_ewec_solar_inference/`) wrote:
- `ewec_dev_powerops.silver.uc2_solar_*`
- `ewec_forecast_models.gold.uc2_solar_forecast_*`
- `ewec_dev_powerops.gold.uc2_solar_*`

**Known issues:**
- Silver forward-fills Solcast and turns missing PV into 0.
- Preprocessing zero-fills outages.
- The "ensemble" is LightGBM only.
- Gold is split across two catalogs.
- Live inference no-ops without future Solcast rows.

Check Bronze, never Silver, for data quality.
