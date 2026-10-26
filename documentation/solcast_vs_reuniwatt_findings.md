# Solcast vs Reuniwatt: feature comparison

**Question:** are Solcast and Reuniwatt features similar enough to train on Solcast and run inference with Reuniwatt?

**Short answer, at feature level:**
- **Usable across the two sources:** temperature and GHI. GHI is usable only if we account for the satellite reading about 5% low at midday, in winter and spring, and on dusty days.
- **Not usable across the two sources:** cloud opacity, DNI, DHI and wind. They are defined or scaled differently, or agree too weakly.
- **Still to do:** a model-level transfer test, covered in Next steps.

Source notebook: `notebooks/pipeline/07_analysis_solcast_vs_reuniwatt_features.ipynb` (parts 1–4). Full output: `notebooks/pipeline/results/feature_comparison_report.txt`.

## Data used

| Source | Table | Period used |
|---|---|---|
| Reuniwatt satellite (observed) | `ewec_dev_reuniwatt.bronze.satellite_weather_sts` | 2023-01-01 → 2024-12-31 |
| Solcast | `ewec_dev_powerops.bronze.pv1_history_solcast` / `pv2_history_solcast` | 2023-01-01 → 2024-12-31 |
| Plant sensors | `ewec_dev_powerops.bronze.power_contango_minute_pv` | 2023-01-01 → 2024-12-31 |

- **Plant naming:** PV1 is the small plant (935 MW) and PV2 the large plant (1,584 MW). The Solcast tables follow the Phase 1 naming, which is swapped: the Solcast "pv2" table is our PV1.
- **Time handling:** all data is resampled to 15 min, labelled at the period end, in UTC.
- **Daytime filter:** irradiance metrics use daytime rows only (clear-sky GHI > 50 W/m²).
- **Bias sign:** bias is satellite − Solcast unless stated otherwise.
- **Reuniwatt data used:** throughout, "satellite" means the Reuniwatt observed satellite table, which is fully filled for 2023–2024.

## 1. Mean, bias, MAE and RMSE per feature and plant

| Feature | Plant | n | Mean satellite | Mean Solcast | Bias | MAE | RMSE | Corr |
|---|---|---|---|---|---|---|---|---|
| GHI (W/m²) | PV1 | 32,901 | 503.3 | 532.1 | −28.8 | 62.7 | 116.3 | 0.93 |
| GHI (W/m²) | PV2 | 32,929 | 505.9 | 534.5 | −28.6 | 64.2 | 121.7 | 0.92 |
| DNI (W/m²) | PV1 | 32,901 | 505.2 | 467.1 | +38.1 | 187.5 | 239.7 | 0.70 |
| DNI (W/m²) | PV2 | 32,929 | 508.8 | 485.8 | +23.0 | 186.0 | 247.6 | 0.67 |
| DHI (W/m²) | PV1 | 32,901 | 160.2 | 210.5 | −50.3 | 76.4 | 98.1 | 0.64 |
| DHI (W/m²) | PV2 | 32,929 | 159.3 | 203.1 | −43.8 | 73.5 | 96.5 | 0.61 |
| Temperature (°C) | PV1 | 70,176 | 28.3 | 29.3 | −1.05 | 1.66 | 2.05 | 0.98 |
| Temperature (°C) | PV2 | 70,176 | 28.8 | 29.1 | −0.30 | 1.33 | 1.76 | 0.98 |
| Humidity (%) | PV2 | 70,176 | 53.7 | 49.0 | +4.7 | 9.5 | 13.3 | 0.87 |
| Wind (m/s) | PV1 | 70,176 | 3.42 | 3.42 | 0.00 | 0.93 | 1.26 | 0.75 |
| Wind (m/s) | PV2 | 70,176 | 3.49 | 3.37 | +0.12 | 1.04 | 1.41 | 0.69 |

Cloud opacity is in section 3, because its units differ between the sources.

### Humidity
The Solcast table for PV1 has no humidity column, so PV1 can only be compared against the plant sensor.

| Plant | Comparison | Bias | MAE | RMSE | Corr |
|---|---|---|---|---|---|
| PV1 | satellite vs sensor | −8.9 | 11.7 | 15.5 | 0.85 |
| PV2 | satellite vs Solcast | +4.7 | 9.5 | 13.3 | 0.87 |
| PV2 | satellite vs sensor | +3.4 | 9.3 | 13.2 | 0.86 |
| PV2 | Solcast vs sensor | −1.5 | 7.3 | 9.4 | 0.92 |

### Where the −28 W/m² GHI bias comes from
The bias is almost identical at both plants (−28.8 and −28.6 W/m²).

| Split | PV1 | PV2 |
|---|---|---|
| Midday (11:00–12:00 Gulf) | −64 to −68 | −64 to −71 |
| Early morning / late afternoon | −14 to +18 | −14 to +15 |
| Winter and spring months (Nov–Apr) | −20 to −91 | −24 to −82 |
| May–Jun | −7 to +5 | −23 to +19 |
| Clear days | −37 | −24 |
| Cloudy/dusty days | −66 | −68 |

**Which source is off:** at PV2, compared with the plant GHI sensor, Solcast is almost unbiased (−9 W/m²) while the satellite reads −35 W/m². So the satellite reads about 5% low. The error is largest at midday in winter and spring and on hazy days, which suggests it comes from how the satellite handles aerosols and dust.

**PV1 sensor is unreliable as a reference:**
- From mid-afternoon onwards on several days, the PV1 GHI sensor gets stuck on a single value and holds it through the night (for example 2024-05-17 and 2024-10-10).
- Its average for Mar 2023 is 1,926 W/m², which is physically impossible.
- So the PV1 "vs sensor" results (GHI corr 0.59–0.65) reflect the sensor, not the sources, and PV2 is used as the sensor reference throughout.

## 2. Alignment: timezone, 15-min intervals, start vs end, coordinates

**Timezone and 15-min alignment:** both sources are UTC and aligned. Shifting each source against the plant GHI sensor, the best match is at a 0 shift for both sources at both plants.

| Plant | Source | Corr at −15 min | Corr at 0 | Corr at +15 min |
|---|---|---|---|---|
| PV2 | satellite | 0.895 | **0.898** | 0.885 |
| PV2 | Solcast | 0.954 | **0.965** | 0.953 |
| PV1 | satellite | 0.584 | **0.594** | 0.593 |
| PV1 | Solcast | 0.637 | **0.645** | 0.640 |

**Start vs end of interval:**
- **Solcast:** its own zenith angle matches the *middle* of each interval (mean error 0.26–0.44°, against 1.75° if it were taken at the period end). So Solcast values are 15-min averages labelled at the period end, which is how we use them.
- **Satellite:** on clear days its morning ramp starts one 15-min step later than computed clear-sky, while its evening ramp is on time. A clock offset would shift both ends, so this looks like a low-sun cutoff in the satellite product rather than a timestamp error. Reuniwatt should confirm their timestamp convention.

**Coordinates:**
- The data cannot pin them down to better than about 0.25–0.5°. Solcast's zenith fits a latitude of about 24.25–24.5°N for both sites.
- Longitude cannot be separated from the time convention, because 1° of longitude equals 4 minutes.
- **Needed:** the exact site coordinates that Solcast and Reuniwatt use.

## 3. Feature definitions and units

### Cloud opacity
Both measure sunlight attenuation; neither measures cloud cover. However, they use different units and scales.

| | Satellite | Solcast |
|---|---|---|
| Unit | fraction 0–1 | percent 0–100 |
| p99 / max | 0.85 / 0.93 | 81–84 / 96.5 |
| Corr with its own clear-sky deficit (1 − GHI / clear-sky GHI) | 0.90 | 1.00 |
| Mean on clear days | 0.07–0.11 | 0.3–1.4 % |
| Mean on cloudy/dusty days | 0.32–0.37 | 19–22 % |
| Available at night | no (daytime only) | yes |

- **Not a simple ×100:** the satellite reports ~10% opacity on clear days, where Solcast reports ~1%. On cloudy days the satellite reports less than Solcast.
- **Agreement:** the two correlate only 0.46–0.52.
- **Conclusion:** cloud opacity is not interchangeable even after rescaling, so it should not be a shared feature.

### Wind speed

| | Mean (m/s) | Ratio to PV2 sensor | Corr with PV2 sensor |
|---|---|---|---|
| Satellite | 3.45 | 0.76 | 0.69 |
| Solcast (10 m) | 3.35 | 0.74 | 0.56 |
| PV2 plant sensor | 4.52 | — | — |

- **Units:** both sources are in m/s and their means match.
- **Timing:** hour-to-hour agreement is moderate (corr 0.69–0.75).
- **Plant sensor:** it reads about 30% higher than both sources, probably because of its mounting height or local exposure. It also has spikes up to 37 m/s.

### DNI and DHI
The two providers split the same GHI differently between direct and diffuse light. On clear days the satellite gives more direct (DNI) and less diffuse (DHI) light. For example, at PV1 on 2024-05-17 the satellite gives DNI 840 / DHI 175 W/m², where Solcast gives 700 / 280. DHI bias is negative in every month at both plants.

## 4. Monthly breakdown and daily plots

Monthly satellite vs Solcast, daytime, 24 months per plant:

| Feature | Plant | Monthly corr | Monthly bias (W/m²) |
|---|---|---|---|
| GHI | PV1 | 0.80 – 0.99 | −91 to +17 |
| GHI | PV2 | 0.72 – 0.99 | −82 to +19 |
| DNI | PV1 | 0.56 – 0.88 | −97 to +155 |
| DNI | PV2 | 0.35 – 0.84 | −121 to +159 |
| DHI | PV1 | 0.46 – 0.78 | −84 to −13 |
| DHI | PV2 | 0.41 – 0.70 | −88 to −4 |
| Cloud opacity | both | −0.03 – 0.71 | (units differ) |

The full month-by-month table is in section 4 of the report.

**Daily plots:** three clear and three cloudy/dusty days per plant, showing GHI, DNI, DHI and cloud opacity.
- Files: `results/feature_plots/pv1_daily_comparison.png` and `pv2_daily_comparison.png`.
- **Clear days:** GHI from both sources tracks the sensor and the computed clear-sky closely. DNI and DHI levels differ, as described in section 3.
- **Cloudy/dusty days:** GHI differs strongly from one 15-min step to the next; each source shows cloud that the other misses.
- **Reading the cloud panel:** the satellite and Solcast cloud opacity are drawn on the same axis. The satellite uses a 0–1 scale, so its line looks flat next to Solcast's 0–100.

## Conclusion and next steps

1. **Usable across the two sources:**
   - Temperature: corr 0.98, bias ≤ 1 °C.
   - GHI: corr 0.92–0.93, bias −28 W/m², with the satellite reading about 5% low.
2. **Do not share between the sources:**
   - cloud opacity (different scale and behaviour);
   - DNI and DHI (different direct/diffuse split);
   - wind (weak agreement).
3. **Next, model-level transfer test:** train on Solcast and run the model on the Reuniwatt satellite data for 2023–2024, scored against actual MW. This covers winter and the dusty months. It answers whether a Solcast-trained model works on Reuniwatt inputs, but not how accurate the day-ahead forecast will be, because satellite data is observed weather, not a forecast.
4. **Later, with Reuniwatt forecasts:** repeat the test with the Reuniwatt forecasts once enough history has built up. They only have values from ~3 Jul 2026, so winter coverage arrives in Dec 2026 – Mar 2027.
5. **Open input:** the exact plant coordinates used by Solcast and Reuniwatt.
