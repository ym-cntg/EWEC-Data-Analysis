# Plant Sensors vs Reuniwatt Forecasts

How close Reuniwatt's forecasts are to what the plants' own sensors measured, and which inputs best explain the target.
Notebook: `notebooks/features/11_sensor_vs_reuniwatt`. Raw output: `notebooks/features/results/sensor_vs_reuniwatt.txt`.

**Window:** 1 Jul – 13 Oct 2026 (~30,000 five-minute periods per plant). **Run:** 14 Oct 2026.
**Targets:** PV1 = actual MW · PV2 = `Pv2_AVL` (available power).
**Plants:** PV1 = small plant (~950 MW, east/west panels) · PV2 = large plant (~1,640 MW).

## Summary

| # | Finding | Impact |
|---|---|---|
| 1 | Reuniwatt sunlight is very accurate: **~4% error** intraday/day-ahead, rising to **~7%** at week-ahead day 7 | Strong base for all products |
| 2 | **PV2 panel sunlight (GTI) is overestimated** by ~40–50 W/m² (~8% error), worst in early morning and late afternoon | Likely a panel-setup mismatch at Reuniwatt; correctable by the model |
| 3 | For **PV2, panel sunlight explains output far better than flat sunlight** (0.998 vs 0.941) | PV2 features must be built on GTI |
| 4 | **Reuniwatt GTI tracks the target better than Reuniwatt's own power forecast** | A model on GTI can beat their power forecast |
| 5 | **Reuniwatt misses dust/haze**: too optimistic in August; worst days are hazy days | Haze features are the biggest opportunity |
| 6 | **3 Jul 2026 forecast is broken** (near-zero sun forecast on a sunny day) | Needs a data check to flag such runs |
| 7 | Temperature forecast good; humidity fair; wind poor | Keep temperature, drop wind |

## 1. Sunlight: Reuniwatt vs measured (daytime)

Error = average absolute error as % of the measured value.

| Horizon (lead) | PV1 GHI corr / error | PV2 GHI corr / error | PV1 GTI error | PV2 GTI error |
|---|---|---|---|---|
| Intraday 0–1 h | 0.984 / 5.0% | 0.990 / 4.7% | 5.1% | 7.8% |
| Intraday 3–6 h | 0.989 / 4.0% | 0.992 / 4.0% | 4.3% | 7.4% |
| Day-ahead 12–24 h | 0.989 / 4.0% | 0.992 / 4.3% | 4.3% | 8.0% |
| Week-ahead day 1 | 0.988 / 4.1% | 0.992 / 4.3% | 4.3% | 7.8% |
| Week-ahead day 4 | 0.987 / 4.6% | 0.989 / 5.2% | 4.9% | 8.8% |
| Week-ahead day 7 | 0.980 / 6.9% | 0.986 / 6.9% | 7.4% | 10.4% |

- Accuracy holds well up to ~4 days ahead, then degrades.
- **Intraday adds no accuracy over day-ahead.** Its first hour is slightly *worse*, with a negative bias (PV1 −11 W/m²). Worth asking Reuniwatt why.
- Reuniwatt is on average **too high** (positive bias), more so for PV2 (+11 to +25 W/m² GHI).

## 2. PV2 panel sunlight is overestimated

Day-ahead (12–24 h) bias, forecast − measured, by Gulf hour:

| Gulf hour | 07 | 08 | 10 | 12 | 14 | 16 | 17 |
|---|---|---|---|---|---|---|---|
| PV2 GTI bias (W/m²) | +29 | **+86** | +47 | +30 | +54 | **+84** | +48 |
| PV2 GHI bias (W/m²) | +8 | +20 | +22 | +23 | +22 | +23 | +20 |
| PV1 GTI bias (W/m²) | −6 | +1 | +10 | +12 | +11 | +18 | +13 |

The PV2 GTI error is largest when the sun is low (morning and afternoon) and smallest at midday, while GHI bias is flat. That pattern is typical of a **mismatch in panel orientation or tracking** between Reuniwatt's model and the plant (e.g. Reuniwatt assumes trackers but the sensor is fixed, or the tilt/azimuth differs).
**Action:** confirm PV2's panel configuration (fixed / single-axis tracker, tilt, azimuth) with the plant and Reuniwatt. Because the error is systematic, a model with time-of-day features can correct it.

## 3. Which input best explains the target (daytime correlation)

| Input | PV1 | PV2 |
|---|---|---|
| Measured panel sunlight (GTI) | **0.997** | **0.998** |
| Measured flat sunlight (GHI) | **0.997** | 0.941 |
| Reuniwatt GTI, day-ahead 12–24 h | 0.988 | 0.980 |
| Reuniwatt GTI, intraday latest | 0.985 | 0.982 |
| Reuniwatt power, day-ahead 12–24 h | 0.986 | 0.977 |
| Reuniwatt power, week-ahead latest | 0.987 | 0.980 |
| Reuniwatt GHI, day-ahead 12–24 h | 0.987 | 0.926 |

- **Measured values (~0.998) are the ceiling:** what a perfect weather forecast could reach. The gap to Reuniwatt (~0.98) is the room for improvement.
- **PV2 behaves like a sun-tracking plant:** flat GHI explains it poorly (0.94), panel GTI almost perfectly (0.998). PV1 (fixed east/west) is explained equally well by both.
- **Reuniwatt's GTI ≥ Reuniwatt's power forecast** for both plants, so our own model on GTI can outperform their power forecast.
- PV2 available power vs actual MW: correlations are very close (e.g. 0.998 vs 0.997), so curtailment and outages are small in this window.

## 4. Weather: Reuniwatt day-ahead vs measured (all hours)

| Variable | PV1 corr / error | PV2 corr / error | Bias |
|---|---|---|---|
| Temperature | 0.96 / 1.7 °C | 0.93 / 2.7 °C | ~0 |
| Humidity | 0.85–0.87 / 9–10 pts | 0.76–0.80 / 12–13 pts | Reuniwatt 6–9 pts too low |
| Wind speed (PV2 only) | — | 0.47 / 40% | Reuniwatt 1.4 too low |

Temperature is reliable (useful for panel heat-loss features). Humidity is usable but biased. Wind forecast is weak: low value as a feature (it may also be measured at a different height).

## 5. Dust and haze: where Reuniwatt goes wrong

Monthly day-ahead GHI bias:

| Month | PV1 bias | PV2 bias | Corr |
|---|---|---|---|
| Jul 2026 | +4 | +19 | 0.99 |
| **Aug 2026** | **+17** | **+28** | 0.98 |
| Sep 2026 | −1 | +11 | 0.995+ |
| Oct 2026 | +4 | +17 | 0.99 |

Worst days (excluding 3 Jul) are **hazy August days**: measured clear-sky index 0.62–0.74 against a forecast of 0.84–0.94, i.e. 10–32% too much sunlight forecast. Examples: 31 Aug (PV2 +32%), 2 Aug (+22%), 11 Aug (+21%), 4 Aug (PV1 +17%), 16 Aug (both ~+13%).
These days drive MAPE. Reuniwatt under-predicts haze; our features should catch it.

## 6. Data issue: 3 July 2026

Reuniwatt's day-ahead forecast for 3 Jul shows a clear-sky index of **0.08** (almost no sun) while the sensors measured **0.86–0.90** (a normal sunny day): −91% error for both plants. This is a broken forecast, not a weather miss.
**Action:** add a check that flags runs with implausible values (e.g. near-zero GHI at midday when clear-sky GHI is high) and excludes them from training and evaluation. Check Bronze for that day's runs.

## 7. Sensor data quality (window)

| | PV1 | PV2 |
|---|---|---|
| Irradiance coverage (daytime-equivalent) | ~71% of all periods | ~68–69% |
| Temperature / humidity | 100% | ~99.5% |
| Wind speed / direction | — | 99% / **41%** (many directions > 360° removed) |
| Target coverage | actual MW 67% | `Pv2_AVL` 64% |

Irradiance and target are blank at night by design, so ~65–70% coverage is close to full daytime coverage.

## 8. Implications for features

| Priority | Feature idea | Based on |
|---|---|---|
| High | **PV2 on GTI**, not GHI; time-of-day terms so the model learns Reuniwatt's PV2 GTI bias | §2, §3 |
| High | **Haze/dust signals:** measured clear-sky index at the origin (intraday), yesterday's measured clear-sky index, humidity, diffuse share (DHI/GHI) | §5 |
| High | **Forecast sanity flag** to drop broken runs (e.g. 3 Jul) | §6 |
| Medium | **Reuniwatt recent bias** vs sensors (last hours / last days, per hour of day) | §1, §2, §5 |
| Medium | Panel temperature from forecast temperature | §4 |
| Low | Wind features | §4 |

Reminder: sensor values are measurements. In models they can only be used **up to the forecast origin**, never for future targets.

## 9. Open questions

1. **PV2 panel setup** (fixed or tracker, tilt, azimuth): does Reuniwatt use the right configuration?
2. **Intraday first hour** is less accurate than longer leads: is that expected from Reuniwatt's product?
3. **3 Jul 2026** broken forecast: known incident at Reuniwatt or in our ingestion?
4. **Wind sensor:** height and units (forecast is 40% off).

## 10. Next steps

- Re-run over 6 months (Apr–Oct 2026) to cover more seasons.
- Add the forecast sanity flag and the haze features to the feature pipeline, one group at a time, and measure the gain against the Reuniwatt baseline.
