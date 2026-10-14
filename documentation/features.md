# UC2 Solar — Features

What the feature pipeline builds today, what we have validated, and how we plan to extend it with time-window features.
Code: `notebooks/features/` (walkthrough: `10_fe_walkthrough`). Status as of 13 Oct 2026.

## 1. The setup in one picture

```
Forecast made at ORIGIN (e.g. today 09:00 Gulf)  ──predicts──▶  PV output at TARGET (e.g. tomorrow 12:00)
                                                                 lead = target − origin

Inputs (features): only what is known at the origin       Answer (label): actual PV at the target
  · Reuniwatt forecast run issued before the origin          · Bronze PV telemetry, cleaned
  · Sun position at the target (computed)
  · Actual PV up to the origin
```

| | Source | Notes |
|---|---|---|
| **Label** | `ewec_dev_powerops.bronze.power_contango_minute_pv` | **PV1: actual MW; PV2: `Pv2_AVL` (available power)**. 1-min, cleaned (corrupt values removed, Mar 2026 column swap fixed), averaged to 5 min, UTC |
| **Features** | `ewec_dev_reuniwatt.bronze.solar_forecast_raw` | Reuniwatt forecasts: new run every 15 min, values every 5 min, Jan 2024 → now, PV1/PV2 × intraday / day-ahead / week-ahead |

One feature table per product (intraday, day-ahead, week-ahead); one row per **plant × origin × 5-min target**.

## 2. Features built today

### 2.1 From the Reuniwatt forecast (at the target time)

| Feature | Meaning | Products |
|---|---|---|
| `f_ghi` | Forecast sunlight on flat ground (W/m²) | all |
| `f_dni`, `f_dhi` | Direct beam / scattered sunlight | all |
| `f_gti_poa` | Forecast sunlight on the panels (PV2 `gti`; PV1 average of east- and west-facing panels) | all |
| `f_power_mw` | Reuniwatt's own power forecast (MW). Also the **baseline to beat** | all |
| `f_power_p10_mw`, `f_power_p90_mw` | Reuniwatt's low / high estimate | all |
| `id_*`, `da_*`, `wa_*` | The same values from the intraday / day-ahead / week-ahead run separately, plus each run's age | per product |
| `da_temp_c`, `da_dewpoint_c`, `da_humidity_pct`, `da_wind_speed`, `da_cloud_opacity` | Forecast weather | day-ahead run only (Reuniwatt sends weather only there) |

`f_*` = best available: the product's own horizon first, the other horizons as fallback.

### 2.2 Computed by us

| Feature | Meaning |
|---|---|
| `sun_zenith`, `sun_elevation`, `cos_zenith`, `air_mass` | Sun position at the target |
| `etr_horizontal` | Sunlight at the top of the atmosphere |
| `clearsky_ghi` | Sunlight expected on a perfectly clear day |
| `is_day` | Sun above the horizon |
| `t_hour_sin/cos`, `t_doy_sin/cos` | Time of day and season of the target (Gulf time) |
| `o_hour_sin/cos` | Time of day of the origin |
| `lead_min` | Minutes between origin and target |

### 2.3 Derived ratios

| Feature | Meaning | Why |
|---|---|---|
| `csi_ghi` | Forecast GHI ÷ clear-sky GHI (1 = clear) | Separates weather from sun angle |
| `kt` | Forecast GHI ÷ top-of-atmosphere sunlight | Same idea, different reference |
| `diffuse_frac` | DHI ÷ GHI | High = haze or dust |
| `power_norm` | Reuniwatt power ÷ plant capacity | Same scale for both plants |
| `band_width_norm` | (p90 − p10) ÷ capacity | How unsure Reuniwatt is |
| `clip_flag` | Reuniwatt power ≥ 97% of capacity | Plant at its export limit |
| `t_cell_c`, `temp_derate` | Estimated panel temperature and heat loss | Panels lose output when hot |
| `disagree_da_wa`, `disagree_id_da` | Gap between horizons' power forecasts | Unsettled weather |

### 2.4 Recent actual PV (up to the origin)

| Feature | Meaning |
|---|---|
| `o_pv_norm` | Actual output at the origin ÷ capacity |
| `o_pv_mean_30m_norm`, `o_pv_mean_60m_norm` | Average output over the last 30 / 60 min |
| `o_pv_trend_30m_norm` | Change over the last 30 min |
| `o_pv_vs_clearsky` | Actual output vs clear-sky expectation |
| `o_reuni_ratio_60m` | Actual ÷ Reuniwatt forecast over the last hour (1 = Reuniwatt spot on) |

About 60 features in total.

## 3. Rules that keep results honest

1. **Forecast runs:** only runs issued at least 15 min **before** the origin.
2. **Actual PV:** only data **up to** the origin.
3. **Timezones:** everything in UTC internally. Reuniwatt is UTC, PV telemetry is Gulf time and is converted.
4. **Labels:** training and scoring use only rows with a clean label (`label_ok`): no missing data or suspected outages.

## 4. What we have validated (test window 30 Sep – 2 Oct 2026)

| Check | Result |
|---|---|
| Runs, lead times, variables | ✅ ~96 / 24 / 8 runs a day (intraday / day-ahead / week-ahead), leads 6 h / 24 h / 7 d, weather only in day-ahead |
| **Is the historical backfill the forecast as originally issued?** | ✅ Backfill vs live copies: GHI correlation 0.999–1.0, differences 0–4 W/m². **Safe to train on** |
| Reuniwatt vs actual PV | ✅ Correlation **0.996** (PV1) and **0.985** (PV2) |
| Power unit | ⚠️ Bronze sends kW (not MW / GW). Fixed; the pipeline now detects the unit |
| Reuniwatt baseline error | ⏳ Re-run after the unit fix; this is the number our models must beat |

## 5. Plan: expanding the features with time windows

We add features **one group at a time** and keep a group only if it improves accuracy over the previous step in a walk-forward test (train on earlier months, test on the next).

```
Baseline: Reuniwatt power forecast  →  + current features (§2)  →  + window group A  →  + B  →  …
```

### A · Forecast windows around the target (same run)
For `f_ghi`, `f_gti_poa`, `f_power_mw`, `csi_ghi`:

| Feature | Meaning | Why |
|---|---|---|
| `_m5`, `_m10`, `_m15` | Value 5 / 10 / 15 min **earlier** in the forecast | A cloud forecast a few minutes early or late is still captured |
| `_p5`, `_p10` | Value 5 / 10 min **later** | Same, in the other direction |
| `_ramp_5` | Change from 5 min earlier | Sudden drops or rises |
| `_mean_30m`, `_mean_60m` | Average over ±15 / ±30 min | Smooths noisy 5-min values |
| `_std_30m`, `_std_60m` | Variability over ±15 / ±30 min | How unsettled the sky is |

All values come from the same run, which is known at the origin, so there is no look-ahead. Most useful for **day-ahead and week-ahead**.

### B · Actual PV windows before the origin
| Feature | Why |
|---|---|
| Averages over 15 / 30 / 60 / 120 min, variability, ramps | Current conditions and momentum |
| Same time yesterday | Strong reference in stable desert weather |
| Actual ÷ Reuniwatt over the last 3 h and the last day | Reuniwatt's current bias, to correct it |

Most useful for **intraday** (the first hours after the origin).

### C · Forecast revisions between runs
| Feature | Why |
|---|---|
| Latest run − run issued 15 / 60 min earlier, for the same target | Clouds arriving or clearing; the direction of change |

### D · Daily summaries
| Feature | Why |
|---|---|
| Forecast daily energy, share of the day near clear-sky, max ramp of the day | Day type (clear, hazy, cloudy) for day-ahead and week-ahead |

### Order of work
1. Re-run the walkthrough with the unit fix → **baseline error** of Reuniwatt.
2. Train the first models on the §2 features and compare with the baseline.
3. Add groups A → B → C → D one by one, keeping what helps, per product × resolution × plant (20 models).

## 6. Open items

- Plant capacities (~950 / ~1,650 MW) and coordinates are estimates: confirm nameplate, export limit, panel orientation.
- Agree one MAPE definition (daytime rule, threshold) before comparing against the targets.
- Oct 2023 – Jan 2024 forecast history is missing (requested).
