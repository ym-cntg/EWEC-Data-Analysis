# Data-Quality Checks — UC2 Solar Source Tables

This guide covers what each notebook in `notebooks/data_quality/` checks, how to run it, and how to read the results.

## Why we check Bronze, not Silver

Silver hides data problems:

- **Missing PV becomes 0.** Bronze→Silver runs `coalesce(PV_MW, 0)`, so an outage looks like a real zero-generation period.
- **Missing weather is forward-filled.** Every Solcast column is copied forward from the last known value, so Silver looks 100% complete even when Bronze is not.
- **Preprocessing adds more zeros.** Daytime dropouts are interpolated for up to 30 minutes, then filled with 0. There is no flag marking imputed values.

The checks therefore run on the raw Bronze tables. Notebook 03 then measures how much Silver is hiding.

## How to run

1. Open the notebooks in a Databricks Git folder. Keep all of them in the same directory.
2. Use **Run all**. Every notebook first runs `%run ./00_dq_utils`, which defines the shared table names, thresholds (`PV_COLS`, `PV_PLAUSIBLE_MAX_MW`, …) and helper functions. If you see `NameError: PV_COLS is not defined`, that cell has not run.
3. Optional: set the `scratch_schema` widget (for example `ewec_dev_powerops.scratch_yash`) to save summary tables as `dq_*`. Leave it empty to display results only. Writes to `bronze`, `silver` or `gold` are refused.

All notebooks are **read-only** against the shared tables.

### Suggested order

| Order | Notebook | Answers open question |
|---|---|---|
| 1 | `01_dq_bronze_pv_telemetry` | Bronze timezone; site capacities |
| 2 | `02_dq_bronze_solcast` (run once per site) | Forecasts or actuals?; Solcast timezone |
| 3 | `04_dq_source_alignment` | Timezone offset between sources; which plant is PV1/PV2 |
| 4 | `03_dq_silver_masking` | How much missing data Silver turns into zeros |
| 5 | `05_dq_reuniwatt_facts` (run once per fact table) | Is the Reuniwatt forecast data complete, on time and consistent with PV and Solcast? |

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

**Sources:** Bronze PV, Solcast PV1, Solcast PV2, and optionally a Reuniwatt table (`reuniwatt_table`, `reuniwatt_ts_col`, `reuniwatt_ghi_col`) For the Reuniwatt forecast fact tables, use notebook 05 section 11 instead: those tables hold several runs per timestamp.

All results use Bronze (upstream) site labels.

| Section | What it checks | How to read it | Red flags |
|---|---|---|---|
| 1. Ranges and overlap | Min/max per source; the window covered by all of them; distinct timestamps per month | The **common overlap window** is the usable period for training and backtesting | Months where one source is empty or sparse |
| 2. Coarse lag scan (hourly, ±8h) | Correlation of PV with GHI after shifting the irradiance by each offset | Best offset **0 h**: same time convention. **±4 h**: one source is UTC and the other is Asia/Dubai. | Any non-zero best offset |
| 2. Fine lag scan (5-min, ±60 min) | The same at 5-min resolution around the coarse best | A best offset of a few minutes or one interval reflects `period_end` vs. period-start labelling | An offset of 30 min or more not explained by the period convention |
| 3. Site mapping | Correlation of each PV column with each irradiance source | Each PV column should correlate best with its own site's Solcast. Clear-sky days look alike everywhere, so differences may be small. | `PV1_MW` matching PV2 Solcast better than PV1 Solcast (the mapping is reversed) |
| 4. Solcast PV1 vs. PV2 | Share of identical GHI values; correlation; mean absolute difference | Two real sites: high correlation but not identical | `identical_ghi_pct` ≈ 100% means both tables were pulled for the same location |

---

## 05 · Reuniwatt forecast fact tables

**Tables:** `ewec_dev_reuniwatt.silver.fact_solar_irradiance`, `ewec_dev_reuniwatt.silver.fact_solar_power_forecast` (plus `_backup` copies and the `dim_forecast_product` / `dim_pv_site` dimensions)
**Widgets:** `fact_table` (run once for each table), `main_col` (blank = `ghi` for irradiance, the power column for power)

This decides whether Reuniwatt ("renewal" in meetings) can replace or supplement Solcast.

These are **forecast** tables, not a single time series. Each row is one forecast **run**, identified by `reference_time` (issue time), for one target period, `period_end`. Rows are split by `site` (pv1/pv2), `provider`, `horizon` (intraday/dayahead/weekahead) and `granularity_min`. Each of those combinations is called a **segment** below, and most checks report one row per segment.

> The column comment on `period_end` says "Forecast reference/issue time", the same as `reference_time`. Section 3 checks which one is really the target time.

| Section | What it checks | How to read it | Red flags |
|---|---|---|---|
| Setup | Key columns, value columns, main column | Prints what the notebook detected | Key columns missing; wrong main column (set `main_col`) |
| 0. Context | Dimension tables; load history; main vs. `_backup` row and key differences | Shows the products and sites; how and when the table is loaded | Keys only in the backup (rows lost on reload); no recent loads |
| 1. Segments overview | Rows, runs, first/last issue time, first/last target time per segment; null key columns | Which products exist and how much history each has | Null keys; segments with very few runs or short history; last issue long ago (feed stopped) |
| 2. Duplicate keys | Repeated (site, provider, horizon, granularity, reference_time, period_end) | Should be 0 | Duplicates, especially conflicting ones; they double-count in any join |
| 3. Lead time | `period_end − reference_time`: min/median/max per segment; rows with lead ≤ 0 | Lead should be positive and fit the horizon: intraday = hours, dayahead ≈ 1–2 days, weekahead ≤ ~7 days | **Lead always 0:** `period_end` is the issue time, not the target. **Lead < 0:** past targets (hindcasts) mixed in; these must be excluded from backtests or they leak. |
| 4. Issue cadence | Usual gap between runs per segment; gaps > 1.5× cadence | `cadence_min` = schedule (e.g. 15/60/1440). `approx_missing_runs` = runs that never arrived. | Many missing runs; long outages (listed longest first) |
| 5. Run completeness | Target periods per run vs. the segment median; irregular steps within a run | `short_runs` = truncated deliveries; `irregular_steps` = gaps inside a run | High `short_pct`; non-zero `irregular_steps` |
| 6. Target-time coverage | % of `period_end` slots covered by at least one run; days below 99% | First and last days are naturally partial | Low coverage. If **intraday** misses only night hours, that may be by design. Check the times of day in the low days. |
| 7. Null rates | Null % per value column, by horizon and day/night | Day/night uses `zenith` or `clearsky_ghi` if present, otherwise "all" | Daytime nulls in the main column |
| 8. Validity | Stats; irradiance rules (as in 02); power < 0, > 1700 MW, or > 1 MW at night; quantile crossing (p10 > p50 …); negative lead | `violations` and `pct` per rule. Edit the sample cell to see rows. | Any power or quantile violations; irradiance at night |
| 9. Stale runs | Runs whose non-zero values all equal the previous run's for the same targets | `stale_pct` per segment | Stale runs mean the provider re-sent an old forecast as new |
| 10. Timezone | Peak hour of the main value per site | ~08 = UTC, ~12 = Asia/Dubai | A different convention from PV (01) or Solcast (02) |
| 11. Against observations | Latest run per target vs. Bronze PV (both columns) and Solcast GHI (both sites), averaged to the same granularity: `n`, `corr`, `bias`, `mae` | **Site mapping:** which reference correlates best with `pv1`/`pv2`. **Accuracy:** bias/MAE against the matching reference (PV for power, Solcast GHI for irradiance). Nights are included, so compare correlations relative to each other. | `pv1` matching the "wrong" PV column (record it; the Bronze→Silver swap also applies); large bias; low correlation |
| 11. Time offset | Hourly lag scan ±6 h vs. Bronze PV | Best `offset_h` should be 0 | ±4 h = UTC vs. Asia/Dubai mismatch; ±1 h = period-start vs. period-end labelling |

---|---|---|---|
| 0. Inventory | Lists every table in `ewec_dev_reuniwatt` with its columns and types | Pick the table holding historical irradiance, and its timestamp and site columns, for the widgets | Empty or permission error: ask Harish for access
| Setup | Schema; table details; which Solcast columns it has | Shows which features the new source can provide | Key columns missing (`ghi`, `dni`, `dhi`, `clearsky_ghi`, `zenith`) |
| 1. Range, duplicates, interval | As for Solcast | Note the native interval and history length | Shorter history than Solcast; duplicates |
| 2. Completeness | Per site if `site_col` is set: overall %, gaps, days below 99% | Compare directly with notebook 02 | Lower completeness than Solcast |
| 3. Nulls, ranges, rules | Day/night null %, stats, a subset of the physical rules | Same interpretation as notebook 02 | Same red flags |
| 4. Timezone | Peak-GHI hour | ~08 UTC / ~12 Dubai | A different convention from Solcast or PV, which will need converting |
| 5. Compare with Solcast | Correlation, mean bias, MAE on matching timestamps | High correlation with low bias means the sources agree. Bias shows systematic over- or under-estimation. | `matched_ts` = 0 (interval or timezone mismatch; run 04 first); low correlation |

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
