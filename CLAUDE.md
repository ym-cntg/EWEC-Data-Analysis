# CLAUDE.md — UC2 EWEC Solar Forecasting

## Project context

Short-term solar PV power generation forecasting for EWEC, run by Contango on Databricks. Code lives in `databricks_ewec_solar_inference/`. The README calls it "UC2 solar"; UC1 is the separate power/load forecasting pipeline.

- **Owner / lead:** Zaynab Habibi. Daily check-ins with Yash start the week of 2026-10-11.
- **Catalog access / ingestion:** Harish Naidu
- **Team:** Shourya, Nitheesh, Mohan
- **Yash's current task:** data quality and data completeness checks on the source tables. That covers the existing Solcast and PV telemetry inputs, plus the new historical **Reuniwatt** weather/irradiance data Harish ingested into catalog `ewec_dev_reuniwatt` (heard as "renewal" in meetings).

Weather inputs come from two possible sources: **Solcast** (current) and the new **Reuniwatt** source (`ewec_dev_reuniwatt`). Part of the goal is to assess whether the new source can replace or supplement Solcast.

## Sites and target

- Two sites: `PV1`, `PV2`. Single target column: `PV_MW` (instantaneous MW, a rate, not energy).
- ⚠️ **PV1/PV2 labels are intentionally swapped at Bronze→Silver.** In Silver and downstream, `site='PV1'` rows contain upstream PV2 telemetry, and the reverse. Always account for this when reconciling Bronze vs. Silver.
- Granularities: 5min, 15min, 60min. Code uses `hourly` and `60min` interchangeably.

## Catalogs and tables

### Bronze (raw inputs) — primary data-quality targets
| Table | Contents |
|---|---|
| `ewec_dev_powerops.bronze.power_contango_minute_pv` | 1-min PV telemetry: `DateTime`, `PV1_MW`, `PV2_MW` |
| `ewec_dev_powerops.bronze.pv1_history_solcast` | Solcast irradiance/weather for PV1, keyed on `period_end` |
| `ewec_dev_powerops.bronze.pv2_history_solcast` | Same for PV2 (`relative_humidity` effectively unused) |
| `ewec_dev_reuniwatt.silver.fact_solar_irradiance` | Reuniwatt irradiance forecasts: `site` (pv1/pv2), `provider`, `horizon` (intraday/dayahead/weekahead), `reference_time` (issue), `period_end`, `granularity_min` |
| `ewec_dev_reuniwatt.silver.fact_solar_power_forecast` | Reuniwatt power forecasts (same keys). `_backup` copies, `dim_forecast_product` and `dim_pv_site` also exist |

### Silver
- `ewec_dev_powerops.silver.uc2_solar_5min` / `_15min` / `_hourly` are Bronze→Silver outputs. Only `_5min` is actually consumed downstream.
- `ewec_dev_powerops.silver.uc2_solar_preprocessed_{5min,15min,hourly}`. The 15/60min versions are rebuilt from `uc2_solar_5min`.
- `ewec_dev_powerops.silver.uc2_solar_features_{5min,15min,hourly}`

### Gold (split across two catalogs; this is a known bug)
- Forecasts: `ewec_forecast_models.gold.uc2_solar_forecast_{5min,15min,hourly}_horizon`
- Dashboard (incremental): `ewec_forecast_models.gold.uc2_solar_dashboard_horizon`, `..._daily_summary`
- Dashboard (legacy, read by BI): `ewec_dev_powerops.gold.uc2_solar_dashboard_horizon`, `..._daily_summary`
- MAPE: `ewec_dev_powerops.gold.uc2_solar_mape_{5min,15min,hourly}`
- Backtest: `ewec_dev_powerops.gold.uc2_solar_intraday_backtest` (append-only, no dedup)
- Models (UC): `ewec_dev_powerops.gold.uc2_solar_intraday_forecast_{N}m_{pv1|pv2}`

### Solcast columns carried into Silver
`air_temp, albedo, azimuth, clearsky_dhi, clearsky_dni, clearsky_ghi, clearsky_gti, cloud_opacity, dewpoint_temp, dhi, dni, ghi, gti, precipitation_rate, relative_humidity, wind_speed_10m, zenith`

## Pipeline

```
Bronze → Silver (data_ingestion_silver_*.py, pure SQL, full overwrite)
  → Preprocessing (uc2_preprocessing_*.py; always from 5min raw)
  → Feature Generation (uc2_feature_generation_*.py)
  → Training (model_training_*.py; per-site, per-horizon, 8 sub-models)
  → Inference: live / historical / live_aligned
  → Dashboard (uc2_dashboard_incremental_*.py) + BackTest
```

Model: per (site × horizon), RidgeCV ×5 + LightGBM + XGBoost + HistGBR. The written `forecast_ensemble` is **LightGBM only** (`pred_lgb`); the seasonal blend weights in `config.json` are never applied.

## Data-quality gotchas (why to check at Bronze, not Silver)

- **Silver hides gaps.** Bronze→Silver forward-fills every Solcast column (`last_value(..., true)`), and `coalesce(PV_MW, 0)` turns missing PV into 0. Silver looks complete even when Bronze isn't.
- **Preprocessing zero-fills outages.** Daytime dropouts (`clearsky_ghi > 50 AND PV_MW < 1.0`) are set to NaN, interpolated for up to 6 steps (30 min at 5min), then `fillna(0)`. Longer outages become fake zeros, with no imputation flag.
- **Timezone.** Preprocessing assumes Bronze is UTC and adds +4h naively. Bronze→Silver bucketing uses session timezone. Verify the actual Bronze timezone.
- **Hard-coded floor:** Silver only includes `DateTime > '2023-11-01'`.
- **Live inference needs future Solcast rows.** `target_*` features use `shift(-h)`. Without Solcast rows past "now", live inference silently writes zero rows.
- **Forecast vs. actual Solcast.** If the Solcast history tables hold estimated actuals rather than forecasts at issue time, the backtest has perfect-foresight leakage. Check for an issue-time column.
- **Magic numbers:** clearsky_ghi 50 (daytime), PV 1.0 MW (dropout), 2000 MW (clip), 1700 MW (plausibility bound). MAPE thresholds differ by pipeline: 150 MW (dashboard), 100 MW (legacy SQL), 200 MW / >0 (backtest).

## Data-quality checklist (current task)

For each Bronze table, and for the Reuniwatt tables:
- **Completeness:** min/max timestamp; expected vs. actual row count at the native interval; missing intervals per day; null rate per column, split by daytime and night.
- **Validity:** negative irradiance or PV; irradiance > 0 when `zenith > 90`; `ghi` far above `clearsky_ghi`; PV above plausible capacity; flat-lined or stuck values; duplicate timestamps.
- **Consistency:** timezone and timestamp convention (period start vs. `period_end`); interval regularity; unit consistency.
- **Alignment:** date-range overlap between PV telemetry, Solcast, and Reuniwatt data; site mapping (remember the swap).
- **Solcast-specific:** does data extend into the future? Forecast or actuals?
- **Silver cross-check:** count daytime rows with `PV_MW = 0` in `uc2_solar_5min` vs. Bronze nulls to quantify masking.

## Known critical bugs (from the 2026-05-12 code review)

- C1: live inference no-ops without future Solcast rows
- C2: 5min/60min historical inference produces nothing (feature window stops at origin)
- C3: dashboard writes to `ewec_forecast_models.gold`, BI reads `ewec_dev_powerops.gold`
- C4: `live_aligned` writes `generation_mode='historical'` and overwrites historical rows
- C5: capacity-blind, inconsistent MAPE thresholds
- C6: out-of-bounds forecasts (>1700 MW) still written to gold
- C7: MLflow picks max version number, with no Champion/Production alias
- C8/C9: missing data collapsed to zero at Bronze→Silver and Preprocessing
- C10: "ensemble" is LightGBM only

## Working conventions

- Prefer Spark SQL / PySpark for checks over `toPandas()` on full tables, since the 5min history is large.
- Treat Bronze/Silver/Gold tables as read-only during the data-quality work. Never `CREATE OR REPLACE` or overwrite shared tables.
- Write check outputs to a personal scratch schema or notebook results, not to `gold`.
- When reporting findings, state the table, time range, count/percentage affected, and a sample query.
- Keep answers concise; use tables for per-column or per-table results.

## Open questions

- [x] Reuniwatt tables: `ewec_dev_reuniwatt.silver.fact_solar_irradiance`, `fact_solar_power_forecast`
- [ ] Reuniwatt `site` pv1/pv2: which Bronze PV column does each match? (notebooks 05 and 06)
- [ ] Bronze timestamp timezone (UTC vs. Asia/Dubai)
- [ ] Are Solcast history rows forecasts (with issue time) or estimated actuals?
- [ ] Site nameplate capacities for PV1 and PV2
- [ ] Which plant is physically PV1 vs. PV2 upstream
