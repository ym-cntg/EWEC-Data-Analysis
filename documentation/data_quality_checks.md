# Data-Quality Checks — UC2 Solar Source Tables

This guide covers what each notebook in `notebooks/data_quality/` checks, how to run it, and how to read the results.

## Why we check Bronze, not Silver

Silver hides data problems:

- **Missing PV becomes 0.** Bronze→Silver runs `coalesce(PV_MW, 0)`, so an outage looks like a real zero-generation period.
- **Missing weather is forward-filled.** Every Solcast column is copied forward from the last known value, so Silver looks 100% complete even when Bronze is not.
- **Preprocessing adds more zeros.** Daytime dropouts are interpolated for up to 30 minutes, then filled with 0. There is no flag marking imputed values.

The checks therefore run on the raw Bronze tables. Notebook 03 then measures how much Silver is hiding.

## How to run

### Quick path: everything, then one export

1. Pull the Git folder in Databricks.
2. Open **`00_run_all`**. Check the `scratch_schema` widget (default `ewec_dev_powerops.scratch_yash`), then **Run all**.
   - It creates the schema if needed. If you lack permission, ask Harish for a schema you can write to.
   - It runs 01 → 02 (PV1, PV2) → 04 → 03 → 05 → 06. A failing notebook is logged in `dq_run_log` and the rest continue.
   - It re-runs 05 and 06 with `timestamps_tz = Asia/Dubai` if their timezone test says so, and 06 with `pv_shift_h` if PV is offset.
3. Open **`07_dq_export`** with the same `scratch_schema`, **Run all**, and copy the output of the last cell. It is every result as one block of text, also saved to `/Workspace/Users/<you>/dq_export.txt`.

Each result is saved as `<scratch_schema>.dq_<name>`, and single findings (counts, detected settings) as `dq_<notebook>_notes`. Re-running replaces the tables.

### Running a single notebook

Open it and **Run all**. Every notebook first runs `%run ./00_dq_utils`, which defines the shared table names, thresholds (`PV_COLS`, `PV_PLAUSIBLE_MAX_MW`, …) and helpers. If you see `NameError: PV_COLS is not defined`, that cell has not run. Fill in `scratch_schema` to save results; leave it empty to only display them. Writes to `bronze`, `silver` or `gold` are refused.

All notebooks are **read-only** against the shared tables.

### Suggested order

| Order | Notebook | Answers open question |
|---|---|---|
| 1 | `01_dq_bronze_pv_telemetry` | Bronze timezone; site capacities |
| 2 | `02_dq_bronze_solcast` (run once per site) | Forecasts or actuals?; Solcast timezone |
| 3 | `04_dq_source_alignment` | Timezone offset between sources; which plant is PV1/PV2 |
| 4 | `03_dq_silver_masking` | How much missing data Silver turns into zeros |
| 5 | `05_dq_reuniwatt_irradiance` | Is the Reuniwatt weather forecast complete, physically consistent, and in line with Solcast? |
| 6 | `06_dq_reuniwatt_power` | Is the Reuniwatt power forecast complete and sane, and how does it compare with actual PV? |

### Thresholds used (from the pipeline code)

| Constant | Value | Meaning |
|---|---|---|
| `DAYTIME_CLEARSKY_GHI` | 50 W/m² | Daytime if `clearsky_ghi > 50` |
| `DROPOUT_PV_MW` | 1.0 MW | PV below this in daytime counts as a dropout |
| `PV_PLAUSIBLE_MAX_MW` | 1700 MW | Upper plausibility bound for PV |
| `SOLAR_NOON_HOUR` | 8 (UTC) / 12 (Asia/Dubai) | Expected peak-sun hour in Abu Dhabi |

---

## 00 · `00_dq_utils`

No checks. It defines constants and helpers used by the other notebooks:

| Helper | Returns |
|---|---|
| `time_range` | Min/max timestamp, row count, distinct timestamps, null timestamps |
| `duplicate_timestamps` | Keys with more than one row; `distinct_value_sets > 1` means the duplicates disagree |
| `interval_profile` | How often each gap size (minutes between consecutive rows) occurs |
| `gaps` | Every break in the series longer than the native interval |
| `completeness_summary` | Expected vs. actual rows over the full range |
| `completeness_per_day` | Per-day % of timestamps present and % of non-null values per column. Includes days with no data at all. |
| `null_rates` | Null % per column, optionally split day/night |
| `column_stats` | Nulls, zeros, min, percentiles, max |
| `rule_counts` | Number of rows violating each validity rule |
| `stuck_runs` | Runs of the same non-zero value repeated many times |
| `hourly_profile` / `guess_timezone` | Average by hour of day, and the timezone the peak hour points to |

---

## 01 · Bronze PV telemetry

**Table:** `ewec_dev_powerops.bronze.power_contango_minute_pv` (1-min `DateTime`, `PV1_MW`, `PV2_MW`)
**Widgets:** `stuck_min_minutes` (default 60)

> These are **upstream** labels. In Silver, `PV1` holds Bronze `PV2_MW`, and the reverse.

| Section | What it checks | How to read it | Red flags |
|---|---|---|---|
| Setup | Schema and timestamp parsing | `Unparseable timestamps` should be 0 | Any value above 0 means `DateTime` is a string in an unexpected format |
| 1. Range and duplicates | First/last timestamp; repeated timestamps | `rows` should equal `distinct_ts` | Duplicates, and especially **conflicting** ones (same minute, different values) |
| 2. Interval regularity | Distribution of minutes between rows, plus a list of gaps | Almost all gaps should be `1.0`. The gaps table lists outages, longest first. | Many gaps of 2–5 min (irregular logging); long gaps of hours or days (outages) |
| 3. Completeness | Overall %, then per day | `timestamp_pct` = rows present; `PV1_MW_pct` = rows present **and** non-null. The second table lists days below 99%. | Days well below 99%, whole days at 0%, and periods where one site is complete but the other is not |
| 4. Nulls by hour | Null % for each hour of the day | Night nulls matter less, because the value is 0 anyway | High null % during daylight hours. Silver turns these into 0 MW and the model learns fake zero output. |
| 5. Validity | Stats; negative values; values above 1700 MW; monthly max/p99.5 | Rule table shows violation counts and %. Sample rows show examples. | Negative PV beyond small sensor noise; any value above 1700 MW. The monthly max/p99.5 plateau gives an **estimate of nameplate capacity**. |
| 6. Stuck values | Same non-zero value repeated ≥ `stuck_min_minutes` | Each row is one run: value, start, end, length | Long daytime runs of an identical value point to a frozen sensor or SCADA link |
| 7. Timezone | Average PV by hour of day; peak hour | Peak near **08:00** means `DateTime` is UTC. Peak near **12:00** means Asia/Dubai. | Peak at 12:00. Preprocessing assumes UTC and adds +4h, which would shift everything 4 hours late. |

---

## 02 · Bronze Solcast history

**Tables:** `ewec_dev_powerops.bronze.pv1_history_solcast`, `pv2_history_solcast` (keyed on `period_end`)
**Widgets:** `site` (PV1/PV2; run once for each), `stuck_min_rows` (default 12)

| Section | What it checks | How to read it | Red flags |
|---|---|---|---|
| Setup | Schema vs. the 17 expected Solcast columns | Lists missing and extra columns | Missing columns that Silver expects |
| 1. Forecast or actuals? | Issue-time-like columns; rows after "now"; several rows per `period_end`; table write history | **Forecasts:** an issue-time column exists, or each `period_end` has several rows, or rows extend into the future. **Estimated actuals:** one row per `period_end`, nothing in the future, no issue time. | **Actuals only** means the backtest uses observed weather as "forecasts", which is perfect-foresight leakage. **`future_rows = 0`** means live inference has no inputs and writes nothing (bug C1). |
| 1. Write history | `DESCRIBE HISTORY` | Shows how the table is loaded (append/overwrite/merge) and how often | No writes recently means ingestion has stopped |
| 2. Range, interval, completeness | Native interval (most common gap), overall %, gaps, days below 99% | Note the native interval (5, 15, 30 or 60 min); later sections use it | Gaps longer than one interval; days below 99% for `ghi` or `clearsky_ghi` |
| 3. Null rates (day vs. night) | Null % per column, split by `zenith < 90` | Compare day vs. night per column | Daytime nulls in `ghi`, `dni`, `dhi`, `gti`, `clearsky_ghi`. Silver forward-fills these, so the model sees stale irradiance. |
| 4. Validity | Physical sanity rules (below) | `violations` and `pct` per rule. Edit the sample cell's condition to see rows. | See the rules table below |
| 5. Stuck values | Repeated identical non-zero values in key columns | Summary per column, then individual runs | Long runs in `ghi` or `air_temp` usually mean a feed problem |
| 6. Timezone | Hour with the highest average `clearsky_ghi` | ~08:00 means UTC; ~12:00 means Asia/Dubai | Different convention from the PV table (compare with notebook 01) |

### Solcast validity rules

| Rule | Meaning | Acceptable level |
|---|---|---|
| `negative_irradiance` | Any irradiance below 0 | 0 |
| `irradiance_at_night` | `ghi`/`dni`/`dhi` > 5 W/m² when the sun is below the horizon | ~0. A large count suggests a timestamp shift. |
| `clearsky_at_night` | Clear-sky GHI > 5 at night | 0. Otherwise the time or zenith is misaligned. |
| `ghi_far_above_clearsky` | GHI more than 20% **and** 50 W/m² above clear-sky | Rare. Brief cloud-edge spikes are physical; many violations are not. |
| `dni_far_above_clearsky` | Same, for DNI | Rare |
| `ghi_closure_error` | `ghi ≠ dni·cos(zenith) + dhi` by more than max(50, 10%) | Low %. High values mean the components are inconsistent. |
| `cloud_opacity_out_of_0_100`, `rh_out_of_0_100`, `albedo_out_of_0_1`, `zenith_out_of_0_180`, `azimuth_out_of_range` | Out-of-range values | 0 |
| `air_temp_implausible` | Below −5 °C or above 60 °C | 0 |
| `dewpoint_above_air_temp` | Dewpoint above air temperature | ~0 |
| `wind_out_of_range`, `precip_negative` | Physically impossible values | 0 |

---

## 03 · Silver masking cross-check

**Tables:** `silver.uc2_solar_5min` vs. Bronze PV; also `silver.uc2_solar_preprocessed_5min`
**Widgets:** `silver_ts_col`, `silver_site_col`, `silver_pv_col`. Check them against the printed schema first.

This measures how much of the "zero generation" in Silver is really missing data.

| Section | What it checks | How to read it | Red flags |
|---|---|---|---|
| Setup | Silver range per site; duplicate keys; nulls | Null PV should be 0, because Silver coalesces to 0 | Silver starting before 2023-11-01 (the hard-coded floor) |
| 1. Swap and bucket convention | Joins Silver to Bronze 5-min averages in 4 ways: {swapped, unswapped} × {bucket start, bucket end}. Reports key-match % and correlation. | The row with **corr ≈ 1** and match ≈ 100% shows the true mapping and convention. It should be **swapped**. | The unswapped mapping correlating better (the swap assumption is wrong). Low correlation everywhere (timezone or bucketing mismatch). |
| 2. Daytime-zero breakdown | Every daytime Silver row with `PV_MW = 0`, labelled by what Bronze had | `no_bronze_rows` / `bronze_all_null` = **masked missing data**. `bronze_near_zero` = genuine dropout or curtailment. `bronze_has_power` = misalignment. | A high `masked_pct_of_daytime`. Any meaningful `zero_but_bronze_has_power`, which points to timezone or bucketing errors. |
| 2. Monthly view | The same breakdown per month | Shows when the masking happens | Months dominated by masked rows. These distort training and MAPE. |
| 3. Longest masked outages | Contiguous masked runs, with a count of those longer than 30 min | Preprocessing interpolates only 6 steps (30 min), so longer runs become **fake zeros** (bug C9) | `runs_longer_than_30min` and `steps_in_long_runs` give the size of the training-data damage |
| 4. Preprocessed zeros | Daytime zero counts, Silver vs. Preprocessed | Counts only. Preprocessing shifts time by +4h, so rows can't be joined directly. | More zeros in Preprocessed than in Silver means preprocessing adds zero-fills on top |

---

## 04 · Source alignment

**Sources:** Bronze PV, Solcast PV1, Solcast PV2, and optionally a Reuniwatt table (`reuniwatt_table`, `reuniwatt_ts_col`, `reuniwatt_ghi_col`). For the Reuniwatt forecast fact tables, use notebooks 05 (section 12) and 06 (section 13) instead: those tables hold several runs per timestamp.

All results use Bronze (upstream) site labels.

| Section | What it checks | How to read it | Red flags |
|---|---|---|---|
| 1. Ranges and overlap | Min/max per source; the window covered by all of them; distinct timestamps per month | The **common overlap window** is the usable period for training and backtesting | Months where one source is empty or sparse |
| 2. Coarse lag scan (hourly, ±8h) | Correlation of PV with GHI after shifting the irradiance by each offset | Best offset **0 h**: same time convention. **±4 h**: one source is UTC and the other is Asia/Dubai. | Any non-zero best offset |
| 2. Fine lag scan (5-min, ±60 min) | The same at 5-min resolution around the coarse best | A best offset of a few minutes or one interval reflects `period_end` vs. period-start labelling | An offset of 30 min or more not explained by the period convention |
| 3. Site mapping | Correlation of each PV column with each irradiance source | Each PV column should correlate best with its own site's Solcast. Clear-sky days look alike everywhere, so differences may be small. | `PV1_MW` matching PV2 Solcast better than PV1 Solcast (the mapping is reversed) |
| 4. Solcast PV1 vs. PV2 | Share of identical GHI values; correlation; mean absolute difference | Two real sites: high correlation but not identical | `identical_ghi_pct` ≈ 100% means both tables were pulled for the same location |

---

### How the Reuniwatt tables are structured

These are **forecast** tables. Each row is one forecast **run** (`reference_time`, the issue time) for one **target period** (`period_end`). Rows are split by `site` (pv1/pv2), `provider`, `horizon` (intraday/dayahead/weekahead) and `granularity_min`; each such combination is a **segment**, and most results have one row per segment. Neither table has a sun-position column, so the notebooks **compute** sun zenith and extraterrestrial irradiance (for Abu Dhabi, at the middle of each period) to split day from night. That depends on the `timestamps_tz` widget, and each notebook tests which setting is right.

> The column comment on `period_end` says "Forecast reference/issue time", the same as `reference_time`. Section 3 (lead time) checks which one is really the target time.

### Sections shared by 05 and 06

| Section | What it checks | How to read it | Red flags |
|---|---|---|---|
| Setup | Expected vs. actual columns | Lists missing and unexpected columns | Schema drift |
| 0. Context | `dim_forecast_product`, `dim_pv_site`; load history; main vs. `_backup` keys and snapshots | What products and sites mean; how often the table reloads | `keys_only_in_backup` > 0 (rows lost on reload) |
| 1. Overview | Rows, runs, first/last issue and target per segment; `_source` breakdown; null keys | How much history each product has; `last_issue` tells you whether the feed is live | Null keys; few runs; feed stopped |
| 2. Duplicates | Repeated full keys | Should be 0 | Duplicates, especially conflicting ones |
| 3. Lead time / day offset | `period_end − reference_time` per segment; `forecast_day_offset` vs. the computed day difference | Lead > 0 and fits the horizon (intraday = hours, dayahead ≈ 1–2 d, weekahead ≤ 7 d). `mismatched` should be 0. | **Lead always 0:** `period_end` is the issue time. **Lead < 0:** hindcasts that would leak into a backtest. Day-offset mismatches mean a timezone problem in the day calculation. |
| 4. Cadence / ingestion | Usual gap between runs, missing runs; delay from issue to `_ingested_at` | `cadence_min` = schedule; `approx_missing_runs` = runs that never arrived; median ingestion delay | Many missing runs; **`rows_ingested_before_issue` > 0** (impossible: clock or timezone error); very long ingestion delays (the forecast arrives too late to use) |
| 5. Run completeness | Steps per run vs. the segment median; irregular spacing | `short_pct` = truncated runs | High `short_pct`; `irregular_steps` > 0 |
| 6. Coverage | % of target slots covered; days below 99% | First and last days are naturally partial | Low coverage. Intraday may skip night hours by design, so check that before flagging it. |
| 7. Null rates | Null % per column by `horizon / daypart` | `daypart` = day / twilight / night from the computed sun position | Daytime nulls in the main columns |
| Stale runs | Runs whose values all repeat the previous run's | `stale_pct` per segment | The provider re-sent an old forecast as new |

---

## 05 · Reuniwatt irradiance forecasts

**Table:** `ewec_dev_reuniwatt.silver.fact_solar_irradiance`
**Widgets:** `timestamps_tz` (UTC / Asia/Dubai), `solcast_shift_h`, `scratch_schema`

This decides whether Reuniwatt can replace or supplement Solcast as the weather input.

| Section | What it checks | How to read it | Red flags |
|---|---|---|---|
| 8. Ranges and rules | Stats for all 16 value columns. Irradiance: < 0, above maximum (GHI 1400, DNI 1200, DHI 800, GTI 1500), > 5 W/m² at night, GHI above extraterrestrial. Weather: temperature −5…60 °C, dewpoint ≤ temperature, humidity/cloud 0–100, clearness 0–130, wind speed 0–50 and direction 0–360, pressure 900–1100 hPa, precipitation 0–200. | `violations` / `pct` per rule; sample cell to inspect rows | Night irradiance (usually a timezone problem); pressure around 100 (the unit is kPa, not hPa); any impossible values |
| 9. Physical consistency | Daytime residuals: `ghi − (bhi + dhi)`; `bhi − dni·cos(zenith)`; `clearness_idx_pct − 100·ghi/ETR`; humidity vs. temperature and dewpoint (Magnus) | p50/p95 of the absolute residuals. Closure should be within a few W/m²; humidity within a few points. | Large closure errors (columns derived inconsistently); a large `bhi` vs. `dni·cos z` gap (time misalignment); humidity off by more than 10 points |
| 9. East/west GTI | Hourly profile and peak hour of `gti_east_wm2` and `gti_west_wm2` | East should peak before solar noon, west after | Reversed (columns swapped) |
| 10. Timezone test | Night-irradiance rows and beam/clearness residuals, assuming UTC and then Asia/Dubai; peak GHI hour | The right timezone has far fewer night rows and smaller residuals. Set `timestamps_tz` to it and re-run. | Neither fits: timestamps may be period-start, or `clearness_idx_pct` is a clear-sky index |
| 11. Stale runs | See the shared table | | |
| 12. vs. Solcast | Latest run per target, against both Solcast sites at the same granularity: GHI, DNI, DHI, GTI (daytime only), temperature, dewpoint, humidity, wind, cloud, precipitation. `n`, `corr`, `bias`, `mae`. | High `corr` for irradiance and temperature. `bias` = Reuniwatt minus Solcast (Solcast isn't ground truth). The site-mapping table shows which Solcast site each Reuniwatt site matches. | Low GHI correlation; a large constant bias; `pv1` matching the PV2 Solcast site (record the mapping) |
| 12. Lag vs. PV | Hourly GHI vs. Bronze PV for offsets ±6 h | Best `offset_h` should be 0 | ±4 h = UTC vs. Dubai mismatch with telemetry |

---

## 06 · Reuniwatt power forecasts

**Table:** `ewec_dev_reuniwatt.silver.fact_solar_power_forecast`
**Widgets:** `timestamps_tz`, `pv_shift_h`, `scratch_schema`

| Section | What it checks | How to read it | Red flags |
|---|---|---|---|
| 1. Quality flags | `quality_flag` counts and % per horizon | Which flags exist and how common they are | A large share flagged; unknown flag values |
| 8. Ranges and rules | Stats and p99.9/max per site; each power column < 0, > 1700 MW, or > 1 MW at night; band ordering (`p10 ≤ power_mw ≤ p90`); zero-width daytime band; band missing while the point forecast is present | The p99.9/max per site approximates the plant capacity Reuniwatt assumes | Any ordering violations; power at night; values above capacity |
| 9. Adjustments | `power_mw` vs. `power_mw_original` by horizon × `quality_flag`: % adjusted, mean/max difference, median ratio, null mismatches | Shows what the adjustment does (for example clipping gives ratio < 1 near the top; a scale factor gives a constant ratio) and whether flags explain it | Adjustments with no flag; large or unexplained changes; the adjusted value null where the original isn't |
| 10. Band width | Median daytime `p90 − p10` (absolute and relative) by site × horizon × day offset | Should widen with lead time | Constant width or few distinct widths (a placeholder band); the band narrowing with lead |
| 11. Stale runs | See the shared table | | |
| 12. Timezone test | Night rows with power > 1 MW, assuming UTC and then Asia/Dubai; peak hour | The right timezone has far fewer night rows | Many night rows under both (period labelling problem) |
| 13. Lag and mapping vs. PV | Hourly ±6 h lag scan; best Bronze PV column per Reuniwatt site | Best offset should be 0; otherwise set `pv_shift_h` = −offset and re-run. Mapping is picked automatically and displayed. | Non-zero offset; an ambiguous mapping (similar correlations) |
| 13. Accuracy vs. PV | Daytime, all runs, by site × horizon × granularity × day offset: `corr`, `bias_mw`, `mae`, `nmae_pct` (% of observed max), `mae_original`, `band_coverage_pct` | MAE should grow with day offset. `band_coverage_pct` ≈ 80% for a calibrated p10–p90. `mae < mae_original` means the adjustment helps. | Large bias; error not growing with lead; coverage well below 80% (overconfident) or near 100% (band too wide). Remember actuals include outages (notebook 01). |
| 14. vs. irradiance table | Runs present in only one of the two tables; daytime correlation of power with GTI and GHI on matching keys | Both tables should share runs, and power should correlate ≥ 0.9 with GTI | Runs missing from one table; low correlation (misaligned or mislabelled) |

---

## Reporting findings

For each issue, record:

| Field | Example |
|---|---|
| Table | `bronze.power_contango_minute_pv` |
| Time range | 2024-03-02 → 2024-03-05 |
| Affected | 4,310 rows / 2.1% of daytime minutes |
| Impact | Becomes 0 MW in Silver; trains the model on fake outages |
| Sample query | `SELECT * FROM … WHERE DateTime BETWEEN … AND PV1_MW IS NULL` |

When a result answers an **open question** in `CLAUDE.md` (timezone, forecasts vs. actuals, capacities, PV1/PV2 mapping, Reuniwatt tables), update that file.
