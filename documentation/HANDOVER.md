# UC2 Solar: handover

**Written:** 2026-10-30, by Yash Mahadeshwar (Contango).

**Scope:** what has been done on EWEC UC2 solar PV forecasting, Phase 2, from Oct 2026. It covers where everything is, what we found, and what is still open.

**For AI agents:** read [`CLAUDE.md`](../CLAUDE.md) in the repo root.

## 1. The project in one paragraph

**The task:** forecast power output for EWEC's two solar plants.
- **Products:** day-ahead, 15-min first; intraday and week-ahead later.
- **Platform:** Databricks.

**Inputs:**
- **New source:** the Reuniwatt weather and power forecasts.
- **Phase 1 source:** Solcast.

**Phase 1 baseline to beat** (Solcast + LightGBM, day-ahead 15-min, 1 Sep – 19 Nov 2025, MAPE where actual > 150 MW):

| Plant (our name) | MAPE |
|---|---|
| Al Dhafrah (pv2) | 8.73 % |
| Sweihan (pv1) | 5.23 % |

**People:**

| Person | Role |
|---|---|
| Zaynab Habibi | Lead; sets the questions and daily check-ins |
| Harish Naidu | Director |
| Nitheesh Panicker | Owns the Reuniwatt ingestion and the `ewec_dev_reuniwatt` tables |

## 2. Plants and naming

| Our name | Plant | Capacity | Panels | Phase 1 doc calls it |
|---|---|---|---|---|
| **pv1** | Sweihan / Noor (small) | 935 MW | east/west fixed | PV2 |
| **pv2** | Al Dhafrah (large) | 1,584 MW | tracker-like | PV1 |

**Naming used by each source:**
- **Same as ours:** Reuniwatt, the plant sensors and our pipeline.
- **Opposite (Phase 1 naming):** the Phase 1 design doc and the Solcast Bronze tables. The Solcast `pv2_history_solcast` table is our pv1.

**Always compare by plant name, never by "PV1"/"PV2" alone.**

## 3. Data sources

| Source | Table | Notes |
|---|---|---|
| PV telemetry + plant sensors | `ewec_dev_powerops.bronze.power_contango_minute_pv` | See below |
| Solcast | `ewec_dev_powerops.bronze.pv1_history_solcast` / `pv2_history_solcast` | 15-min, `period_end`, Jan 2023 → 19 Nov 2025. The pv2 table has no humidity. |
| Reuniwatt forecasts | `ewec_dev_reuniwatt.bronze.solar_forecast_raw` | See below |
| Reuniwatt forecasts, Silver | `ewec_dev_reuniwatt.silver.fact_solar_irradiance`, `fact_solar_power_forecast` (+ `_backup`) | Partly processed; we read Bronze |
| Reuniwatt satellite (observed) | `ewec_dev_reuniwatt.bronze.satellite_weather_sts` | 15-min, 2014-01 → 2024-12, complete, 4 sites (pv1–pv4) |

**PV telemetry table:**
- 1-min, Gulf time (UTC+4), from 2023-03-18.
- **Columns:**
  - power: `PV1_MW`, `PV2_MW`, `Pv2_AVL`;
  - sensors: GHI, array radiance, temperature, humidity, wind.
- **Column swap:** `PV1_MW` / `PV2_MW` are swapped before 2026-03-17.
- **Blanks:** SCADA leaves a blank when a value hasn't changed. Zaynab's rule is to forward-fill.
- **Copies:** also exists in `ewec_uat_powerops` and `ewec_sbx_powerops`.

**Reuniwatt forecasts (`solar_forecast_raw`):**
- **Format:** one row per forecast run. The `_raw` JSON is site → variable → (UTC time → value).
- **Timing:** a run every 15 min, values every 5 min.
- **Units:** power is in kW.
- **⚠️ Values are null before ~3 Jul 2026.**

Full availability: [data_availability.md](data_availability.md).

**Most important data fact:** usable Reuniwatt *forecast* history is only **~3 months** (Jul/Aug → Oct 2026).
- Runs exist back to Jan 2023, but every value is null. This holds for every ingestion copy, including the 2023 re-ingestion of 13 Oct.
- Row counts look complete. That hid the problem until 2026-10-18.

## 4. Repo map

```
CLAUDE.md                       guide for AI agents (conventions, gotchas, current state)
conf/                           pipeline settings (parameters.yml) and dataset catalog (catalog.yml)
src/uc2_solar/                  Kedro-style pipeline code (nodes, pipelines, runner, metrics)
notebooks/
  data_quality/                 DQ checks, Databricks .py notebooks (00_run_all, 01–07, 08_dq_data_availability)
  features/                     first feature-engineering notebooks (superseded by the pipeline)
  pipeline/                     pipeline runners 00–04 and analysis notebooks 05–07
    results/                    text reports and plots, committed after each run
documentation/                  write-ups (this file, findings, pipeline, features, DQ)
```

| Document | Content |
|---|---|
| [pipeline.md](pipeline.md) | How the pipeline is built and run |
| [features.md](features.md), [feature_engineering.md](feature_engineering.md) | Feature definitions, time-window / lag families |
| [data_quality_checks.md](data_quality_checks.md) | First DQ notebooks (01–07) |
| [sensor_vs_reuniwatt.md](sensor_vs_reuniwatt.md) | Plant sensors vs Reuniwatt (Jul–Oct 2026) |
| [solcast_vs_reuniwatt_findings.md](solcast_vs_reuniwatt_findings.md) | Solcast vs Reuniwatt satellite feature comparison (2023–2024) |
| [data_availability.md](data_availability.md) | From when to when every source has values |

## 5. How to run

- **Workspace:** Databricks, repo cloned as a Git folder.
- **Compute:** serverless, with ANSI mode on.
- **Output schema:** `ewec_dev_powerops.scratch_yash`. Never write to bronze, silver or gold; the pipeline refuses to.

| What | Where | Notes |
|---|---|---|
| Full pipeline | `notebooks/pipeline/00_run_all.ipynb` | Widgets override `conf/parameters.yml`; report → `results/report_dayahead_15min.txt` |
| One pipeline stage | `notebooks/pipeline/01–04` | `from_node` / `to_node` to resume; errors → `results/last_error.txt` |
| Input analysis | `05_analysis_inputs.ipynb` | Fake zeros, telemetry delay, source agreement |
| Solcast → Reuniwatt transfer | `06_analysis_solcast_reuniwatt_transfer.ipynb` | Writes `uc2_analysis_transfer_15min`, needed by 07 |
| Solcast vs Reuniwatt features | `07_analysis_solcast_vs_reuniwatt_features.ipynb` | Report + daily plots in `results/` |
| Data availability | `notebooks/data_quality/08_dq_data_availability.py` | Report → `notebooks/data_quality/results/data_availability.txt` |

**Workflow:** run in Databricks → commit and push `results/` → pull locally to review.

**MLflow autologging must be off before any training:** `mlflow.autolog(disable=True)`. With it on, a training cell logged hundreds of runs and took about 45 min instead of a few.

## 6. Modelling approach (agreed with Zaynab)

- **Targets:** actual MW for both plants. `Pv2_AVL` was dropped: it is noisier in 39 of 42 months and 22 % blank.
- **Model granularity:** one LightGBM per clock slot: forecast time (09:00 / 15:00 Gulf) × 15-min step × plant. Each model answers "what happens at 09:15".
- **Label:** target ÷ capacity.
- **Daytime only:** 06:00–18:00 Gulf; the forecast is 0 outside that window.
- **No look-ahead:** features use only runs issued at least 15 min before the forecast time, joined as-of.
- **Feature families:**
  - Reuniwatt values: around-target lags, across-runs lags, across-horizons (day-ahead − week-ahead);
  - computed: sun position, clear-sky, calendar;
  - actual-output history (`act_`), interactions (`ix_`) and daily summaries (`day_`).
- **Plant sensors are not used as features,** because they are not available for the forecast period.
- **Metrics:** MAPE where actual > 150 MW (the Phase 1 rule); nMAE, RMSE and bias as % of capacity.
- **Test windows:** `phase1_window` (1 Sep – 19 Nov 2025) and `recent_3m`.

## 7. Results so far

### Pipeline v2 (2026-10-18)
Source: `results/report_dayahead_15min.txt`. Figures are MAPE.

| Split | Plant | Our model | Reuniwatt power forecast |
|---|---|---|---|
| phase1_window | pv1 | 5.72 % | no values |
| phase1_window | pv2 | 16.54 % | no values |
| recent_3m (Jul–Oct 2026) | pv1 | 6.67 % | 7.61 % |
| recent_3m | pv2 | 10.20 % | 9.04 % |

⚠️ **These results are a lower bound.** Reuniwatt values are null before Jul 2026, so these models learned almost entirely without weather inputs (sun position and actual history only). They need re-running once real weather history exists.

### Solcast → Reuniwatt transfer (notebook 06, test Sep–Oct 2026)
Train on Solcast, infer with Reuniwatt forecasts. Figures are MAPE.

| Plant | Solcast-trained | Reuniwatt-trained | Reuniwatt power forecast |
|---|---|---|---|
| pv1 | 3.42 % | 4.17 % | 6.15 % |
| pv2 | 7.33 % | 6.21 % | 7.68 % |

Calibrating the inputs made results worse.

### Solcast vs Reuniwatt satellite, features (notebook 07, 2023–2024)
- **Temperature:** interchangeable (corr 0.98).
- **GHI:** close (corr 0.92–0.93), but the satellite reads about 5 % low, and −50 to −90 W/m² at midday in winter/spring.
- **Cloud opacity:** attenuation in both sources, but on different scales (0–1 vs 0–100) that don't map simply.
- **DNI/DHI and wind:** don't match.
- **Alignment:** both UTC, 15-min aligned; Solcast is labelled at the period end.
- **Answer:** the sources can't be swapped one-for-one. A reduced model (GHI + temperature + computed features) is untested.

## 8. Data-quality findings

1. **Reuniwatt forecast values are null before Jul 2026** for every product and every ingestion copy. The cause is the Reuniwatt historical API; re-pulling won't help.
2. **Bronze MW column swap** before 2026-03-17. Handled by `_detect_swap_date` in `src/uc2_solar/pipelines/data_engineering/nodes.py`.
3. **Pv2_AVL is unreliable:** 22 % blank on average, 53 % in Apr–Jun, and noisier than PV2_MW.
4. **PV1 GHI sensor faults:** it gets stuck from the afternoon through the night on some days, and shows impossible values in Mar–Apr 2023.
5. **Telemetry delay:** data arrives 5–10 min after measurement (`Load_Datetime`, UTC).
6. **Mar–Apr 2026:** partial telemetry rows.
7. **Reuniwatt power units:** Bronze is in **kW**. It was first assumed to be GW; the pipeline now detects the unit.

## 9. Open items

| Item | Owner / waiting on |
|---|---|
| Older Reuniwatt forecasts: archive export (at least 2025)? What is in `migration_legacy`? What are satellite sites pv3/pv4? | Nitheesh / Reuniwatt |
| Exact plant coordinates used by Solcast and Reuniwatt | Zaynab |
| Does forward-fill apply to PV1/PV2 MW and the sensors, not only Pv2_AVL? | Zaynab |
| Daytime-only completeness of PV power and irradiance (notebook 08 section 4) | Run and review |
| PV2 live forecast runs since 2026-10-12: check runs per day | Run query |
| Transfer test on the satellite data (train Solcast, infer satellite, 2023–2024; covers winter) | Proposed; waiting for approval |
| Re-test transfer on Reuniwatt forecasts in Dec 2026 – Mar 2027 (winter) | When data exists |
| Automatic value-coverage gate in data engineering (fail when training months are mostly null) | Proposed; waiting for approval |
| Next model iterations: feature groups, training from Jul 2026 onwards | After the data decisions above |

## 10. Lessons

- **Profile every input before correlating or modelling:** null % by month, distributions, time-series plots. Correlations silently drop nulls, and row counts hide empty values.
- **Plant naming flips between sources.** Check which plant a number refers to before comparing it with Phase 1.
- **Agree the plan before coding.** Ask clarifying questions first; Zaynab and Yash review the approach before implementation.
