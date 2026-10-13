# UC2 Solar — Feature Engineering with Reuniwatt

How the existing UC2 features map to the new Reuniwatt source, and what to build to reach the accuracy targets.
Sources: `UC2_ANALYSIS.md` (pipeline review, 2026-05-12) and the DQ run of 2026-10-08 (`documentation/data_quality_checks.md`).

## 1. Accuracy targets (solar)

| Look-ahead | Granularity | MAPE window | Target MAPE |
|---|---|---|---|
| Intraday | 5 / 15 / 60 min | 6 h / 24 h / 24 h | **2%** |
| Day-ahead | 5 / 15 / 60 min | 24 h | **3%** |
| Week-ahead | 15 / 60 min | 7 days | **4%** |
| Month-ahead | 60 min | 1 month | **4%** |
| Annual | 60 min | 1 year | **10%** |

These are very aggressive for solar. Public benchmarks for utility PV are typically ~3–8% nMAE for intraday and ~5–15% for day-ahead. Reaching them depends on two things as much as on the model:

1. **The MAPE definition.** The pipeline uses three different near-zero thresholds today (150 / 100 / 200 MW). Agree on one definition (daytime mask, capacity threshold) before measuring against targets.
2. **Clean labels.** Outages, curtailment and corrupt telemetry inflate MAPE. They must be flagged and excluded from training and scoring (see §5).

Month-ahead and annual go beyond any weather forecast (Reuniwatt week-ahead ends at 7 days). They need a climatology / statistical approach, not NWP features (see §4.4).

## 2. Data available from Reuniwatt

Bronze `ewec_dev_reuniwatt.bronze.solar_forecast_raw` stores each forecast run as raw JSON. History: **23 Jan 2024 → today**, all 6 products (PV1/PV2 × intraday / day-ahead / week-ahead), 5-min resolution. Silver currently has only part of it processed.

| Product | Lead time | Variables (as unpacked in Silver) |
|---|---|---|
| Intraday | 0–6 h | GHI, DNI, DHI, BHI, GTI* |
| Day-ahead | 0–24 h | GHI, DNI, DHI, BHI, GTI*, temperature, dewpoint, humidity, wind speed/dir, cloud opacity, pressure, precipitation, clearness |
| Week-ahead | 0–7 days | GHI, DNI, DHI, BHI, GTI* |
| Power (all horizons) | as above | `power_mw` (**in GW**), `power_p10_mw`, `power_p90_mw` |

\* GTI: PV2 has `gti_wm2`; PV1 has `gti_east_wm2` + `gti_west_wm2` (different panel layouts).
Timestamps are UTC; PV telemetry is Gulf time (UTC+4). To be confirmed: whether the raw JSON contains more variables than Silver extracts.

## 3. Existing features → Reuniwatt

| Existing feature | Reuniwatt equivalent | Action |
|---|---|---|
| Calendar: hour / month / day-of-year (sin, cos) | — | Keep; compute in Gulf time |
| PV lags, rolling mean/std/min/max, EMA, MACD | — (from PV target) | Keep; **fix leaky `change_1`, `change_1h`, `ghi_change_1`** (S1) |
| `ghi`, `ghi_lag_*`, `ghi_mean_1h` | `ghi_wm2` | Direct |
| `gti`, `target_gti` | PV2 `gti_wm2`; PV1 `gti_east_wm2`, `gti_west_wm2` | Per-site |
| `dni`, `dhi` | `dni_wm2`, `dhi_wm2` | Direct |
| `zenith`, `azimuth`, `target_zenith` | not provided | **Compute** (pvlib) |
| `clearsky_ghi`, `clearsky_gti` | not provided | **Compute** (pvlib Ineichen); GTI needs panel tilt/orientation (`dim_pv_site` is empty) |
| `clear_sky_ratio`, `gti_ratio`, `csr_*` | — | Recompute from computed clear-sky |
| `cloud_opacity`, `cloud_*`, `csr_x_cloud` | `cloud_opacity_pct` (day-ahead only, fraction) | Borrow day-ahead value for other horizons |
| `air_temp`, `ghi_x_temp`, `target_air_temp` | `ambient_temp_c` (day-ahead only) | Same |
| `albedo` | not provided | Drop |
| `target_*` (Solcast `shift(-h)`) | **Value from the latest run issued before the origin** | Fixes C1 (live no-op) and S2 (backtest leakage) |

## 4. Features to build

### 4.1 Common to all horizons

| Feature | Definition | Why |
|---|---|---|
| Sun geometry | zenith, elevation, azimuth, air mass, hour angle, day length, minutes since sunrise / to sunset | Physical driver; closes S13 |
| Clear-sky irradiance & power | pvlib clear-sky GHI/GTI; expected clear-sky power = f(GTI_clear, capacity) | Baseline the model corrects |
| Clear-sky index | Reuniwatt GHI ÷ clear-sky GHI (and GTI version) | Separates weather from sun position |
| Capacity-normalised target | PV ÷ site capacity (~1,650 / ~950 MW) | Closes S14; one model shape for both plants |
| Clipping indicator | expected power ≥ export limit (~1,645 MW plant) | Large plant clips in spring/summer; flat tops are easy wins |
| Daytime mask | zenith < 90° (or elevation > ~3°) | Train/score daytime only; closes S3 |
| Panel temperature / derating | T_cell ≈ T_amb + k·GTI − wind cooling | Heat losses are material in UAE summer |

### 4.2 Intraday (target 2%): recent actuals dominate

| Feature | Definition |
|---|---|
| PV at origin, last 15/30/60 min trend | From Bronze PV (cleaned), up to the origin only |
| Clear-sky index of actual PV at origin | PV ÷ expected clear-sky power ("smart persistence") |
| Reuniwatt error at origin | actual ÷ Reuniwatt forecast for the last 30–60 min, used to correct the next hours |
| Reuniwatt intraday irradiance & power | Latest run issued before the origin, first 6 h |
| Forecast revision | Latest run − previous run for the same target time |
| Plant availability proxy | Recent max PV ÷ expected clear-sky power (detects partial outages / curtailment) |

### 4.3 Day-ahead and week-ahead (targets 3% / 4%)

| Feature | Definition |
|---|---|
| Reuniwatt power forecast (×1000 → MW) | Strongest input; also the benchmark |
| Reuniwatt GTI, GHI, DNI/DHI, clear-sky index | From the run issued before the origin (e.g. 09:00 / 15:00 for day-ahead) |
| Uncertainty | p90 − p10 width; intraday vs day-ahead vs week-ahead disagreement |
| Dust / haze proxies | Diffuse share DHI ÷ GHI; humidity; wind speed (dust lifting); day-ahead only |
| Lead time | Target − issue time |
| Reuniwatt recent bias | Rolling 7-day error of Reuniwatt per site and hour of day |
| Climatology blend (week-ahead) | Mean clear-sky index for that day-of-year/hour from history; weight it more as lead time grows |

### 4.4 Month-ahead and annual (targets 4% / 10%)

No weather forecast reaches this far. Features:

| Feature | Definition |
|---|---|
| Expected clear-sky energy | pvlib clear-sky power per hour for the period |
| Climatological clear-sky index | Typical ratio by month / hour from 2.5+ years of PV history |
| Plant capacity / availability trend | Recent availability, planned outages if known |
| Seasonal dust and soiling | Month-level effects; days since rain/cleaning if available |

## 5. Rules (leakage and data)

1. **As-of joins only.** For each origin, use the latest Reuniwatt run with `reference_time` ≤ origin. Never a later run.
2. **Actuals up to the origin only** for lag / persistence features.
3. **Timezones:** convert Reuniwatt UTC → Gulf time before joining with PV.
4. **Exclude unusable rows:** legacy power rows with lost issue time (`reference_time = period_end`), corrupt PV1 values (Apr–May 2023), outages/curtailment (flag, don't zero-fill).
5. **Fix before use:** power unit (GW → MW); PV column swap around March 2026; process the full Bronze backfill into Silver.

## 6. Suggested starting set

GTI (per site), GHI, Reuniwatt power forecast, clear-sky index, sun elevation, hour / day-of-year, panel temperature, p10–p90 width, lead time, clipping indicator; plus PV at origin, actual clear-sky index at origin and Reuniwatt error at origin for intraday. Train on a capacity-normalised, daytime-only, outage-free target. Add the rest only where they improve validation MAPE.

## 7. Pipeline (`notebooks/features/`)

| Notebook | Output (in `output_schema`) | Notes |
|---|---|---|
| `00_fe_common` | — | Sites, capacities, sun position, clear-sky (Haurwitz), calendar, as-of join |
| `01_fe_explore_bronze_json` | `results/bronze_json_layout.txt` | JSON layout of Bronze `_raw`, needed to write the Bronze parser |
| `02_fe_reuniwatt_forecasts` | `fe_reuniwatt_forecasts` | One row per site × horizon × run × target; power in MW; `gti_poa`; lead ≤ 0 dropped. `source = silver` now, `bronze` once parsed |
| `03_fe_pv_target` | `fe_pv_target_5min` | Cleaned PV in UTC, Reuniwatt site labels, swap auto-detected, `status` / `label_ok` |
| `04_fe_build_features` | `fe_features_{intraday,dayahead,weekahead}_5min` | Features at each origin + label at each target |
| `05_fe_run_all` | `results/fe_run_summary.txt` | Runs 02 → 03 → 04 ×3 |

Feature columns in `fe_features_*`:
- **Reuniwatt, per horizon** (`id_` intraday, `da_` day-ahead, `wa_` week-ahead): `ghi`, `dni`, `dhi`, `gti_poa`, `power_mw`, `power_p10_mw`, `power_p90_mw`, run time and age; `da_` also `temp_c`, `dewpoint_c`, `humidity_pct`, `wind_speed`, `cloud_opacity`.
- **Best available** (`f_*`): main horizon first, others as fallback.
- **Sun and calendar:** `sun_zenith`, `sun_elevation`, `cos_zenith`, `air_mass`, `etr_horizontal`, `clearsky_ghi`, `is_day`, `t_hour/doy_sin/cos`, `o_hour_sin/cos`, `lead_min`.
- **Derived:** `csi_ghi`, `kt`, `diffuse_frac`, `power_norm`, `band_width_norm`, `clip_flag`, `t_cell_c`, `temp_derate`, `disagree_da_wa`, `disagree_id_da`.
- **State at origin:** `o_pv_norm`, `o_pv_mean_30m_norm`, `o_pv_mean_60m_norm`, `o_pv_trend_30m_norm`, `o_pv_vs_clearsky`, `o_reuni_ratio_60m`.
- **Label:** `label_pv_mw`, `label_pv_norm`, `label_status`, `label_ok`.

Not yet built: forecast revision between runs, Reuniwatt rolling bias, climatology blend, 15/60-minute aggregation, month-ahead/annual features.
