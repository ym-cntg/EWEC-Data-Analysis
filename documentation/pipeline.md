# UC2 Solar — Pipeline (Kedro-style)

Structured pipeline from raw Bronze data to trained models and evaluation. Plain Python on Databricks, organised like Kedro: **config → catalog → nodes → pipelines → runner**.

## Layout

```
conf/
  parameters.yml       all settings (dates, sites, products, lags, model, MLflow)
  catalog.yml          every dataset: Delta table name / file path, by layer
src/uc2_solar/
  framework.py         Node, Pipeline, DataCatalog, run_pipeline (mini-Kedro)
  config.py            loads conf/, applies notebook overrides
  spark_utils.py       time, sun position, clear-sky, calendar, as-of join
  metrics.py           nMAE / MAPE / RMSE / bias
  pipelines/
    data_engineering/    nodes.py + pipeline.py
    feature_engineering/ nodes.py + pipeline.py
    model_training/      nodes.py + pipeline.py
    evaluation/          nodes.py + pipeline.py
  registry.py          pipeline names (+ __default__ = all)
  runner.py            run("<pipeline>", spark, repo_root, overrides)
notebooks/pipeline/
  00_run_all  01_data_engineering  02_feature_engineering  03_model_training  04_evaluation
```

Nodes are pure functions; all wiring (inputs → outputs) lives in each `pipeline.py`. To change behaviour, edit `conf/parameters.yml`, not code.

## Data layers

| Layer | Dataset | Content |
|---|---|---|
| 01 raw | `raw_pv_telemetry`, `raw_reuniwatt` | Bronze tables (read only) |
| 02 intermediate | `int_target_5min` | Target 5-min UTC: PV1 actual MW, PV2 `Pv2_AVL`; status, `label_ok` |
| | `int_reuniwatt_5min` | Reuniwatt runs unpacked from JSON, power in MW (day-ahead hourly, week-ahead 3-hourly ≤ 2 days) |
| | `int_reuniwatt_run_flags` | Broken runs (darkness forecast at midday) |
| 03 primary | `pri_target`, `pri_reuniwatt` | Resampled to the product resolution (15 min); broken runs removed |
| 04 feature | `feat_reuniwatt_runs` | Per run × target: values, clear-sky ratios, around-target lags |
| | `feat_origins` | Forecast times (09:00, 15:00 Gulf) × sites |
| | `feat_table` | Site × origin × target: all features + label |
| 05 model input | `model_input` | Usable labels, train / test split (last 3 months = test) |
| 07 model output | `predictions` | Test predictions: `daytime` model and `reuniwatt_baseline` |
| 08 reporting | `metrics_summary`, `metrics_by_step`, `report` | Metrics tables; text report in `notebooks/pipeline/results/` |

All tables are written to `run.output_schema` with the prefix `uc2_` (writes to bronze / silver / gold are refused).

## Pipelines and nodes

| Pipeline | Node | Does |
|---|---|---|
| data_engineering | `clean_target` | Clean PV, fix the Mar 2026 column swap, PV2 = `Pv2_AVL`, Gulf → UTC, 5-min, status |
| | `unpack_reuniwatt` | Unpack Bronze JSON, thin runs, kW → MW, `gti_poa` |
| | `flag_broken_runs` | Mark runs forecasting darkness when the sun is high |
| | `resample_reuniwatt` / `resample_target` | Average to 15 min; label usable only if all 5-min parts are |
| feature_engineering | `reuniwatt_run_features` | Clear-sky ratios + **around-target** lags per run |
| | `build_origins` | 09:00 and 15:00 Gulf each day |
| | `assemble_features` | As-of joins (no look-ahead), **across-runs** and **across-horizons** lags, computed features |
| | `add_labels` | Attach target at each target time |
| model_training | `build_model_input` | Keep usable labels; train / test split |
| | `train_models` | One LightGBM per origin hour × daytime step × site; MLflow logging; test predictions |
| evaluation | `evaluate` | Metrics overall, by forecast time, by lead hour, by step; report |

## Features (Reuniwatt + computed only)

| Group | Columns (examples) |
|---|---|
| Main horizon (`da_`) | `da_ghi`, `da_dni`, `da_dhi`, `da_bhi`, `da_gti_poa`, `da_power_mw`, `da_power_p10_mw`, `da_power_p90_mw`, weather (`da_temp_c`, `da_humidity_pct`, `da_dewpoint_c`, `da_cloud_opacity`, `da_clearness_index`, `da_pressure_hpa`, `da_precip_rate_mmh`), `da_csi_ghi`, `da_kt`, `da_diffuse_frac`, `da_beam_frac`, run age |
| Other horizon (`wa_`) | Base values and clear-sky ratios from the week-ahead run |
| Best available (`f_`) | Day-ahead, falling back to week-ahead |
| **Around the target** | For `ghi`, `gti_poa`, `power_mw`, `csi_ghi`: `_m15/_m30/_m45` (earlier), `_p15/_p30/_p45` (later), `_d1` (change), `_mean_pm30/_pm60`, `_std_pm30/_pm60` (e.g. `da_gti_poa_m15`) |
| **Across runs** | Same target from the day-ahead run issued 60 / 180 / 360 min earlier (`da_r60_ghi`, …) and revisions (`da_ghi_rev60`, …) |
| **Across horizons** | `xh_ghi_da_minus_wa`, `xh_gti_poa_da_minus_wa`, `xh_power_mw_da_minus_wa` |
| Computed | Sun position, clear-sky GHI, `t_hour/doy_sin/cos`, `o_hour_sin/cos`, `lead_min`, `step`, `origin_hour_gulf`, `power_norm`, `p10_norm`, `p90_norm`, `band_width_norm`, `clip_flag`, `t_cell_c`, `temp_derate`, `is_day_int` |

No plant-sensor or past-target features in this version.

## Models

- **Daytime only:** models exist only for target slots inside the daytime window (`model_training.daytime`). Default: **Gulf 06:00–18:00** (`mode: clock`); `mode: sun` uses sun-up instead (covers summer evenings until ~19:10). Outside the window the forecast is 0. The report shows how much real production falls outside the window.
- **Grain:** one model per **forecast time (09:00 / 15:00) × daytime step × plant**, i.e. ~48 steps per forecast time (≈ 192 models per plant).
- **Label:** target ÷ capacity. Predictions are clipped to 0–1.05 and converted back to MW.
- **Scoring:** daytime-window slots with usable labels; Reuniwatt baseline scored on the same rows.
- **Split:** last 3 months of origins = test.
- **MLflow:** experiment `/Users/<you>/uc2_solar`, one run per product × site × candidate.
  - Logged: parameters, test and baseline metrics, MAPE by step (as a metric series), feature importance, and all models as `models.pkl`.

## How to run

1. Pull the Git folder.
2. Open `notebooks/pipeline/00_run_all` (or 01 → 04 one at a time).
3. Check the widgets (`output_schema`, `start_date`, `end_date`). For a quick test use `start_date = 2026-04-01`.
4. Run all. The first cell installs `pyyaml` and `lightgbm` and restarts Python.
5. If a node fails: fix it, set `from_node` to that node's name, and run again.
6. Commit and push `notebooks/pipeline/results/report_dayahead_15min.txt` to share results.

## Adding a product

Add an entry under `products` in `conf/parameters.yml` (resolution, origin hours, window, horizons) and set `run.product` to it. The same nodes build its features and models.
