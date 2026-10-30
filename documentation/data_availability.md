# Data availability: PV telemetry and Reuniwatt Bronze tables

**As of:** 2026-10-29.

**Question:** over the full history of each table, from when to when does each source have values?

**How it was checked:** `notebooks/data_quality/08_dq_data_availability.py`.
- Every check counts **values**, not rows.
- The Reuniwatt forecast backfill has a run every 15 min but every value is null, so row counts alone look complete.

## Summary

| Source | Table | Rows from → to | Values usable | Gaps |
|---|---|---|---|---|
| PV telemetry (power + plant sensors), 1-min, Gulf time | `ewec_dev_powerops.bronze.power_contango_minute_pv` | 2023-03-18 → today | 2023-03 → today | Mar–Apr 2026 partial rows; sensor gaps below |
| Reuniwatt satellite (observed weather), 15-min, UTC | `ewec_dev_reuniwatt.bronze.satellite_weather_sts` | 2014-01-01 → 2024-12-31 | 2014-01 → 2024-12, all variables | none; nothing after 2024 |
| Reuniwatt forecasts (irradiance, weather, power), UTC | `ewec_dev_reuniwatt.bronze.solar_forecast_raw` | 2023-01-01 → today, every month, all 6 products | **2026-07 → today only** (complete from 2026-08) | all values null 2023-01 → 2026-06 |

## 1. PV telemetry

Other copies of this table exist in `ewec_uat_powerops` and `ewec_sbx_powerops`. The dev copy is current: its last value is 2026-10-29.

### Coverage over all hours

| Variable | Complete months (≥95 %) | Partial months |
|---|---|---|
| Rows (timestamps) | 2023-03 → 2026-02, 2026-05 → 2026-10 | 2026-03 → 2026-04 |
| PV1_Temp | 2023-05 → 2023-09, 2023-11 → 2024-11, 2025-01 → 2025-12, 2026-02, 2026-07 → 2026-10 | 2023-03 → 04, 2023-10, 2024-12, 2026-01, 2026-03 → 06 |
| PV1_HumidityPer | 2023-04 → 2026-02, 2026-07 → 2026-10 | 2023-03, 2026-03 → 06 |
| PV2_Outdoor_Temp | 2023-04 → 2023-08, 2023-10 → 2026-02, 2026-05 → 2026-10 | 2023-03, 2023-09, 2026-03 → 04 |
| PV2_HumidityPer | 2023-03 → 2023-08, 2023-10 → 2026-02, 2026-05 → 2026-10 | 2023-09, 2026-03 → 04 |
| PV2_WindSpeed | 2023-04 → 2024-11, 2025-01 → 2025-12, 2026-02, 2026-05 → 2026-10 | 2023-03, 2024-12, 2026-01, 2026-03 → 04 |
| PV2_WindDirection | 2023-04 → 2024-11, 2025-01 → 2026-02, 2026-05 → 2026-07 | 2023-03, 2024-12, 2026-03 → 04, 2026-08 → 10 |
| PV1_MW, PV2_MW, Pv2_AVL, PV1_GHI, PV2_GHI, PV1_ArrayRadiance_east / west | none | every month |
| PV2_ArrayRadiance | none | 2026-03 → 2026-10 (column starts 2026-01) |

**Why power and irradiance are never complete over all hours:** SCADA leaves a blank when a value has not changed. That happens all night at 0. Zaynab's rule is to use the previous non-blank value.

**Still to check:** section 4 of notebook 08 measures daytime-only completeness (06:00–18:00 Gulf) for these columns, to tell real gaps apart from night blanks.

**Other notes:**
- **Start dates:** Pv2_AVL values start on 2023-05-31 and PV1_MW on 2023-04-12.
- **Column swap:** the PV1_MW / PV2_MW columns are swapped before 2026-03-17. The pipeline detects the swap.
- **PV1 GHI sensor is unreliable:** it gets stuck on a single value from the afternoon through the night on some days, and it shows physically impossible values in Mar 2023. See `solcast_vs_reuniwatt_findings.md`.

## 2. Reuniwatt satellite

- **Coverage:** 2014-01-01 00:00 → 2024-12-31, 15-min.
- **Variables:** GHI, DNI, DHI, temperature, humidity, wind speed and direction, dewpoint, pressure, precipitation, clearness index and cloud opacity are all ≥95 % in every month. Cloud opacity is daytime only, by design.
- **Sites:** **4 sites, pv1–pv4.** pv1 and pv2 are our plants; pv3 and pv4 are unknown (ask Nitheesh).
- **No data after 2024:** the table was last altered on 2026-10-22, but nothing beyond 2024 has been added.
- **What it is:** observed weather, not forecasts. It is suitable for training and for analysis, but not for live inference.

## 3. Reuniwatt forecasts

### By ingestion source
Sampled one day-ahead run per day at 06:00 UTC. The figure is the % of GHI values filled.

| `_source` | Years | Ingested | GHI filled |
|---|---|---|---|
| `api_historical_backfill` | 2023 | 2026-10-13 | 0 % |
| | 2024 | 2026-10-06 → 13 | 0 % |
| | 2025 | 2026-10-01 → 06 | 0 % |
| | 2026 | 2026-10-01 → 02 | 24.8 % (values from Jul) |
| `api_incremental` (live) | 2026 | from 2026-10-02 | 100 % |
| `migration_new_pipeline` (live, earlier pipeline) | 2026 | 2026-09-23 → 29 | 100 % |
| `migration_legacy` | 2025–2026 | 2026-09-29 | null (different JSON format) |

### What this means
- **Every product is affected.** This holds for every variable (irradiance, weather, power, p10/p90), both sites and all horizons (intraday, day-ahead, week-ahead): values are null from 2023-01 to 2026-06, partial in 2026-07 and complete from 2026-08.
- **The cause is the API, not our pipeline.** The re-ingestion of 13 Oct added 2023 runs that are also empty. Reuniwatt's historical API appears to return values only for recent months, so re-running the backfill will not fill the gap.
- **`migration_legacy` is an open lead.** It holds 2025–2026 runs in a different JSON format and might contain older forecasts. The pipeline currently skips it. Section 5 of notebook 08 prints a sample.
- **Cloud opacity:** it never reaches 95 %, because it is blank at night.
- **Duplicate copies:** some 2026 runs were ingested more than once. This is harmless, because the pipeline keeps the latest copy.
- **PV2 live feed:** `api_incremental` shows fewer sampled PV2 runs than PV1 since 2026-10-12. Check PV2 runs per day.

## Consequences

- **Usable Reuniwatt forecast history is about 3 months** (Jul/Aug → Oct 2026). Models trained on Reuniwatt forecasts before Jul 2026 had no weather input.
- **The long Reuniwatt history is the satellite table:** observed weather, 2014–2024. It overlaps PV actuals from 2023-03 to 2024-12.
- **Asked of Nitheesh / Reuniwatt:**
  - an archive export of past forecasts, at least 2025;
  - what is in `migration_legacy`;
  - what the satellite sites pv3 and pv4 are.
