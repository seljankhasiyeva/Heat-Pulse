# Heat-Pulse — `src/` Folder: Full Technical Documentation

> **Project:** Heat-Pulse · **Scope:** `src/` directory and all its contents  
> **Date:** May 2, 2026

---

## Table of Contents

1. [Overview](#1-overview)
2. [Folder Structure](#2-folder-structure)
3. [Python Modules](#3-python-modules)
   - [\_\_init\_\_.py](#31-__init__py)
   - [config.py](#32-configpy)
   - [ingestion.py](#33-ingestionpy)
   - [database.py](#34-databasepy)
   - [cleaning.py](#35-cleaningpy)
   - [features.py](#36-featurespy)
   - [quality_checks.py](#37-quality_checkspy)
   - [pipeline.py](#38-pipelinepy)
4. [Data Files — Historical CSVs](#4-data-files--historical-csvs)
5. [Sub-folders](#5-sub-folders)
   - [data/](#51-data)
   - [logs/](#52-logs)
6. [Pipeline Execution Flow](#6-pipeline-execution-flow)
7. [Known Issues — pipeline.log Analysis](#7-known-issues--pipelinelog-analysis)
8. [Module Dependency Map](#8-module-dependency-map)

---

## 1. Overview

The `src/` directory is the **data engineering core** of the Heat-Pulse project. It contains the complete ETL (Extract, Transform, Load) pipeline that:

1. **Ingests** daily weather data from the Open-Meteo API or local CSV files for 94+ cities across Azerbaijan and the surrounding Caspian region.
2. **Stores** raw data in a DuckDB analytical database.
3. **Cleans** the raw data (missing values, outlier flagging, date continuity checks).
4. **Engineers features** (rolling averages, seasonal indicators, degree days, anomaly scores, lag features).
5. **Validates** data quality at each stage with automated checks.
6. **Logs** every pipeline run to an audit table and a log file.

The pipeline supports three run modes: **full** (complete historical reload), **incremental** (only new days), and **forecast** (7-day forward-looking data).

---

## 2. Folder Structure

```
src/
├── data/
│   └── weather.duckdb                  (268 KB) — Main analytical database
├── logs/
│   └── pipeline.log                    (3 KB)  — Pipeline execution log
├── __init__.py                         (0 KB)  — Package marker
├── baku_historical.csv                 (178 KB) — 2,310 rows of historical data
├── ganja_historical.csv                (180 KB) — 2,310 rows of historical data
├── lankaran_historical.csv             (188 KB) — 2,310 rows of historical data
├── mingachevir_historical.csv          (194 KB) — 2,310 rows of historical data
├── sumgayit_historical.csv             (187 KB) — 2,310 rows of historical data
├── config.py                           (11 KB)  — API URLs, city registry, date range
├── ingestion.py                        (13 KB)  — Open-Meteo API fetch + CSV loading
├── database.py                         (15 KB)  — DuckDB interface + schema + audit log
├── cleaning.py                         (12 KB)  — Missing values, outliers, date gaps
├── features.py                         (11 KB)  — Feature engineering
├── quality_checks.py                   (18 KB)  — Automated quality gates
└── pipeline.py                         (29 KB)  — Master orchestrator (CLI entrypoint)
```

---

## 3. Python Modules

### 3.1 `__init__.py` — 0 KB

**Purpose:** Empty file that marks the `src/` directory as a Python package. This allows other modules (e.g. `pipeline.py`) to import from sibling files using `import ingestion`, `import database`, etc., after adding `src/` to `sys.path`.

**Why it matters:** Without this file, Python would not recognise `src/` as a package in some import contexts. It is a standard Python convention and costs nothing to maintain.

---

### 3.2 `config.py` — 11 KB

**Purpose:** The **centralised configuration file** for the entire pipeline. Defines API endpoints, the list of weather variables to fetch, the full city registry (94+ cities), and the data date range.

#### API Endpoints

| Constant | Value |
|---|---|
| `HISTORICAL_URL` | `https://archive-api.open-meteo.com/v1/archive` |
| `FORECAST_URL` | `https://api.open-meteo.com/v1/forecast` |

#### `VARIABLES` — 15 weather parameters fetched from the API:

| Variable | Unit | Description |
|---|---|---|
| `temperature_2m_max` | °C | Maximum temperature at 2 m |
| `temperature_2m_min` | °C | Minimum temperature at 2 m |
| `temperature_2m_mean` | °C | Mean temperature at 2 m |
| `precipitation_sum` | mm | Total precipitation |
| `rain_sum` | mm | Rainfall component |
| `snowfall_sum` | cm | Snowfall component |
| `wind_speed_10m_max` | km/h | Maximum wind speed at 10 m |
| `wind_gusts_10m_max` | km/h | Maximum wind gusts at 10 m |
| `relative_humidity_2m_mean` | % | Mean relative humidity |
| `pressure_msl_mean` | hPa | Mean sea-level pressure |
| `cloud_cover_mean` | % | Mean cloud cover |
| `shortwave_radiation_sum` | MJ/m² | Daily solar radiation total |
| `apparent_temperature_max` | °C | Feels-like maximum temperature |
| `weather_code` | WMO code | WMO weather interpretation code |

#### `CITIES` — 94+ city registry

The list spans 7 countries in the Caspian region. Each entry has `name`, `lat`, `lon`, and a `districts` sub-list.

| Country | Cities included |
|---|---|
| Azerbaijan | Baku, Ganja, Sumqayit, Lankaran, Mingachevir, Nakhchivan, Sheki, Shirvan, Yevlakh, Absheron, and ~75 more districts |
| Iran | Rasht, Sari, Gorgan, Bandar-e Anzali |
| Russia (Dagestan) | Makhachkala, Derbent |
| Armenia | Yerevan, Gyumri |
| Turkey (East Anatolia) | Erzurum, Van, Malatya |
| Georgia | Tbilisi, Batumi, Kutaisi |
| Kazakhstan (West) | Atyrau, Aktau, Oral |
| Turkmenistan (West) | Turkmenbashi, Balkanabat |

#### Date Range

| Constant | Value |
|---|---|
| `START_DATE` | `"2020-01-01"` |
| `END_DATE` | `"2026-04-19"` |

**Why it matters:** `config.py` is the single source of truth for what the pipeline fetches and from where. Any change to the city list, date range, or variable set is made here and automatically propagates to all other modules.

---

### 3.3 `ingestion.py` — 13 KB

**Purpose:** **Handles all data acquisition** — fetching from the Open-Meteo REST API and loading from pre-downloaded CSV files. Sits at Stage 1 of the pipeline.

#### Key constants

| Constant | Value |
|---|---|
| `HISTORICAL_URL` | Open-Meteo archive API endpoint |
| `FORECAST_URL` | Open-Meteo forecast API endpoint |
| `DAILY_VARIABLES` | 12-element list matching `database.py` column names exactly |
| `RAW_DIR` | `data/raw/` |
| `HIST_CSV` | `data/raw/all_94_cities_historical_combined.csv` |
| `FORE_CSV` | `data/raw/all_94_cities_forecast_combined.csv` |
| `DEFAULT_START_DATE` | `"2020-01-01"` |

#### Functions

**`load_cities_from_csv(csv_path)`**
- Reads the combined historical CSV and extracts unique `(city, latitude, longitude)` rows.
- Returns a dict: `{city_name: {"latitude": float, "longitude": float}}`.
- Falls back to a 6-city hardcoded Azerbaijan list if the CSV is not found.
- Used by `pipeline.py` to discover which cities are available.

**`fetch_city_weather(city, latitude, longitude, start_date, end_date, retries=3, backoff=2.0, is_forecast=False)`**
- The **core API call function**. Sends a GET request to Open-Meteo with the city's coordinates and the `DAILY_VARIABLES` list.
- Handles retries with exponential backoff (waits `backoff × attempt` seconds between retries).
- For forecast mode (`is_forecast=True`), hits `FORECAST_URL` without date parameters to get the default 7-day lookahead.
- Returns a `pd.DataFrame` with columns: `time, city, latitude, longitude, <12 variables>`, or `None` on total failure.
- Sets `timezone=UTC` in all API requests.

**`ingest_all_cities_full(cities, start_date, end_date)`**
- Loops over all cities and calls `fetch_city_weather()` for each.
- Concatenates all successful results into one combined DataFrame.
- Used in `--mode full` pipeline runs.

**`ingest_incremental(latest_dates, cities, end_date)`**
- Takes a `{city: last_date_in_db}` dict and fetches only data **after** each city's last stored date.
- Cities already up-to-date are skipped with an INFO log message.
- Used in `--mode incremental` pipeline runs — the most common daily operation.

**`ingest_all_forecasts(cities)`**
- Fetches the 7-day forecast for every city.
- Used in `--mode forecast` pipeline runs.

**`load_historical_from_csv(path)` / `load_forecast_from_csv(path)`**
- Loads pre-downloaded CSVs from disk.
- Normalises the `time` column to `YYYY-MM-DD` string format.
- Provides a fast offline-first path for `--mode full` when the combined CSV already exists.

**Why it matters:** `ingestion.py` is the data source layer. It abstracts away whether data comes from a live API call or a local CSV, presenting the same DataFrame structure to the rest of the pipeline regardless of source.

---

### 3.4 `database.py` — 15 KB

**Purpose:** The **DuckDB interface layer**. Manages connections, schema creation, data loading, parquet export, incremental helpers, and the audit log. All other modules interact with DuckDB through functions defined here.

#### Database Schema

The pipeline maintains 7 tables in `weather.duckdb`:

| Table | Created by | Purpose |
|---|---|---|
| `raw_historical` | `database.py` | Raw historical weather from API/CSV |
| `raw_forecast` | `database.py` | Raw 7-day forecast from API |
| `staging_historical` | `cleaning.py` | Cleaned + outlier-flagged historical data |
| `staging_forecast` | `cleaning.py` | Cleaned + outlier-flagged forecast data |
| `analytics_historical` | `features.py` | Feature-engineered historical data |
| `analytics_forecast` | `features.py` | Feature-engineered forecast data |
| `pipeline_runs` | `database.py` | Audit log of every pipeline execution |

**`raw_historical` and `raw_forecast` schema** (16 columns):

```
time VARCHAR, city VARCHAR,
latitude DOUBLE, longitude DOUBLE,
temperature_2m_max DOUBLE, temperature_2m_min DOUBLE, temperature_2m_mean DOUBLE,
precipitation_sum DOUBLE, rain_sum DOUBLE, snowfall_sum DOUBLE,
wind_speed_10m_max DOUBLE, wind_gusts_10m_max DOUBLE,
pressure_msl_mean DOUBLE, shortwave_radiation_sum DOUBLE,
apparent_temperature_max DOUBLE, weather_code DOUBLE,
PRIMARY KEY (time, city)
```

**`pipeline_runs` schema:**

```
run_id INTEGER PRIMARY KEY, run_at TIMESTAMP,
mode VARCHAR, cities_count INTEGER,
rows_raw INTEGER, rows_staging INTEGER, rows_analytics INTEGER,
duration_sec DOUBLE, status VARCHAR, notes VARCHAR
```

#### Key functions

**`get_connection(db_path)`**
- Opens (or creates) the DuckDB file and returns the connection object.
- Creates the parent directory automatically if it does not exist.

**`create_schema(conn)`**
- Runs `CREATE TABLE IF NOT EXISTS` for `raw_historical`, `raw_forecast`, and `pipeline_runs`.
- Safe to call multiple times (idempotent).

**`_normalise_raw(df)`**
- Private helper: normalises the `time` column to `YYYY-MM-DD` string format.
- Ensures all 16 expected columns exist, adding `None` for missing ones.
- Called by both `load_raw_historical` and `load_raw_forecast` before any INSERT.

**`load_raw_historical(conn, df, mode='append')`**
- `mode='append'` → `INSERT OR REPLACE` (upsert — deduplicates by primary key `time + city`).
- `mode='replace'` → deletes the entire table first, then inserts.
- Returns the number of rows loaded.

**`load_raw_forecast(conn, df)`**
- Always replaces the forecast table entirely (`DELETE` then `INSERT`).
- Forecasts are point-in-time snapshots, so full replacement is correct.

**`save_raw_as_parquet(conn, data_dir)`**
- Exports `raw_historical` to three files: `raw.parquet`, `raw_historical.parquet` (both are the same data — the first is what `cleaning.py` looks for by default).
- Also exports `raw_forecast.parquet`.
- Keeps the `data/` folder consistent for tools that prefer Parquet over DuckDB.

**`get_latest_dates(conn)`**
- Queries `MAX(time)` per city from `raw_historical`.
- Returns `{city: last_date_string}`.
- Used by `pipeline.py` to determine the start date for incremental ingestion.

**`get_row_count(conn, table)` / `table_exists(conn, table_name)`**
- Utility functions for safely checking table sizes and existence.
- Return `0` / `False` if the table does not exist (avoids exceptions).

**`log_pipeline_run(conn, mode, cities_count, rows_raw, ...)`**
- Inserts one row into `pipeline_runs` after each pipeline execution.
- Auto-increments `run_id` using `MAX(run_id) + 1`.
- Records mode, row counts, duration, status, and error notes.

**`get_table_summary(conn)` / `print_row_counts(conn)`**
- Returns a DataFrame with row counts for all 7 pipeline tables.
- `print_row_counts` renders a visual bar chart using `█` characters — useful for quick inspection in notebooks.

**Why it matters:** `database.py` is the persistence contract of the pipeline. It defines exactly what gets stored, how it gets deduplicated, and how every pipeline run is audited. All other modules are stateless — they pass DataFrames through functions. Only `database.py` touches the DuckDB file directly.

---

### 3.5 `cleaning.py` — 12 KB

**Purpose:** **Task 2 of the pipeline** — data cleaning. Takes raw data from DuckDB or Parquet, applies three cleaning operations, and writes to `staging_*` tables.

#### Functions

**`handle_missing_values(df, strategy=None)`**

Imputes missing values using a column-type-aware strategy:

| Column pattern | Default strategy | Rationale |
|---|---|---|
| `temperature`, `apparent` | Forward-fill, then backward-fill | Temperature rarely jumps; adjacent values are the best estimate |
| `precipitation_sum`, `rain_sum`, `snowfall_sum` | Fill with `0` | No data for precipitation = no precipitation |
| All other numeric | Linear interpolation (bidirectional) | Smooth interpolation is appropriate for gradual-change variables |
| Any remaining nulls | Drop row | Last resort; logged with row count |

Imputation is applied **per city group** (`df.groupby('city')`) to avoid cross-city contamination. The function accepts a custom `strategy` dict for non-default overrides.

**`flag_outliers(df, columns, method='iqr', threshold=1.5)`**

Adds a boolean `<column>_outlier_flag` column for each specified column. Does **not** remove outliers — flags them for downstream use.

Two detection methods supported:

| Method | Logic |
|---|---|
| `'iqr'` | Flags values outside `[Q1 - 1.5×IQR, Q3 + 1.5×IQR]` (Tukey fences). Applied per city group. |
| `'zscore'` | Flags values with `|z| > threshold`. Skips columns with zero standard deviation. |

Flagged columns targeted by default: `temperature_2m_max/min/mean`, `precipitation_sum`, `wind_speed_10m_max`, `wind_gusts_10m_max`, `pressure_msl_mean`, `shortwave_radiation_sum`, `apparent_temperature_max`.

**`validate_date_continuity(df, city)`**

For a single city, builds the full expected date range and checks actual dates against it. Returns a summary DataFrame with:

| Field | Description |
|---|---|
| `start_date` / `end_date` | Actual date range in the data |
| `expected_days` | Total calendar days in the range |
| `actual_days` | Unique dates actually present |
| `missing_count` | Number of missing dates |
| `missing_dates` | List of the specific missing date values |

**`validate_all_cities(df)`** — calls `validate_date_continuity` for every city and concatenates the results.

**`clean_raw_to_staging(conn, data_dir)`**

The **master cleaning function**. Runs the complete cleaning pipeline for both `historical` and `forecast` data:

1. **Source detection** — looks for Parquet files first (`raw.parquet`, `raw_historical.parquet`, `forecast.parquet`), falls back to DuckDB tables (`raw_historical`, `raw_forecast`).
2. Calls `handle_missing_values()`.
3. Calls `flag_outliers()` on all 9 targeted columns.
4. Calls `validate_all_cities()` and writes the date-gap summary to a DuckDB table (`staging_historical_date_gaps` or `staging_forecast_date_gaps`).
5. Writes the cleaned DataFrame as a DuckDB table (`staging_historical` / `staging_forecast`).
6. Also saves the cleaned data as a Parquet file (`staging_historical.parquet` / `staging_forecast.parquet`).

**Why it matters:** `cleaning.py` is the data quality foundation. Raw API data frequently has gaps, sensor anomalies, and missing values. This module ensures that only clean, flagged, validated data proceeds to feature engineering.

---

### 3.6 `features.py` — 11 KB

**Purpose:** **Task 3 of the pipeline** — feature engineering. Reads cleaned `staging_*` tables, computes 6 categories of derived features, and writes to `analytics_*` tables.

All feature functions call `_prepare(df)` first, which converts `time` to `datetime` and sorts by `['city', 'time']` to ensure correct temporal ordering.

#### Feature Functions

**`add_rolling_averages(df)`**

Adds 7-day and 30-day rolling means for temperature and precipitation. Uses `min_periods=1` so early rows are not lost.

New columns: `temperature_2m_mean_7d`, `temperature_2m_mean_30d`, `precipitation_sum_7d`, `precipitation_sum_30d`.

**`add_seasonal_indicators(df)`**

Adds calendar and meteorological season columns based on the Northern Hemisphere meteorological calendar:

| Column | Type | Values |
|---|---|---|
| `month` | int | 1–12 |
| `quarter` | int | 1–4 |
| `day_of_year` | int | 1–366 |
| `season` | string | `'winter'`, `'spring'`, `'summer'`, `'autumn'` |

**`add_temperature_range(df)`**

Adds `temperature_range = temperature_2m_max − temperature_2m_min`. Quantifies daily thermal volatility — a high range indicates a large day-night temperature swing.

**`add_degree_days(df, base=18.0)`**

Computes energy demand proxies relative to a base temperature of 18°C (industry standard):

- `HDD` (Heating Degree Days) = `max(0, 18 − T_mean)` — energy needed to heat a building.
- `CDD` (Cooling Degree Days) = `max(0, T_mean − 18)` — energy needed to cool a building.

These columns are the primary link between weather data and the energy forecasting models used by the `web/` layer.

**`add_anomaly_score(df)`**

Calculates how far today's temperature deviates from the historical mean for the same calendar day and city:

```
anomaly_score = T_mean − mean(T_mean | same city, same day_of_year)
```

Positive = warmer than normal. Negative = cooler than normal. Useful for detecting climate anomalies and unusual weather events.

**`add_lag_features(df)`**

Adds yesterday's (lag-1) and the day-before-yesterday's (lag-2) values for temperature and precipitation:

New columns: `temperature_2m_mean_lag1`, `temperature_2m_mean_lag2`, `precipitation_sum_lag1`, `precipitation_sum_lag2`.

These are standard inputs for time-series prediction models (LSTM, ARIMA, XGBoost, etc.).

#### Master function

**`compute_all_features(df)`** — runs all 6 feature functions in sequence and returns the fully-featured DataFrame.

**`populate_analytics_tables(conn)`** — reads both `staging_historical` and `staging_forecast` from DuckDB, calls `compute_all_features()` on each, and writes the result to `analytics_historical` and `analytics_forecast`.

**Why it matters:** `features.py` transforms raw measurements into model-ready inputs. Without rolling averages, lag features, and seasonal indicators, the downstream ML models in the `notebooks/` layer would have no temporal context to learn from.

---

### 3.7 `quality_checks.py` — 18 KB

**Purpose:** **Automated data quality gate** for all pipeline stages. Runs 6 types of checks and returns structured pass/warn/fail results. Can be called both by `pipeline.py` (with a DuckDB connection) and directly from Jupyter notebooks (with DataFrames).

#### Check result format

Every check returns a dict:
```python
{
    "check_name": str,   # e.g. "null_ratio"
    "stage":      str,   # e.g. "staging_historical"
    "status":     str,   # "PASS", "WARN", or "FAIL"
    "details":    str,   # human-readable description
}
```

#### The 6 quality checks

**`check_row_count(conn_or_df, table_or_stage)`**
- **FAIL** if the table or DataFrame has 0 rows.
- This is a pipeline abort condition — zero rows means the ingest stage failed.

**`check_null_ratio(conn_or_df, table_or_stage, threshold=0.05)`**
- **WARN** if any numeric column has more than 5% null values after cleaning.
- Identifies columns where imputation did not fully succeed.

**`check_date_continuity(conn_or_df, table_or_stage, max_gap_days=3)`**
- **WARN** if there are date gaps longer than 3 consecutive days in any city's time series.
- Uses the same logic as `cleaning.validate_date_continuity()` but formatted as a quality gate result.

**`check_value_ranges(conn_or_df, table_or_stage)`**
- **WARN** if any temperature column contains values outside `[-50°C, +60°C]`.
- Catches physically implausible values that survived outlier flagging.
- `TEMP_COLS` = `temperature_2m_max`, `temperature_2m_min`, `temperature_2m_mean`, `apparent_temperature_max`.

**`check_feature_completeness(conn_or_df, table_or_stage)`**
- **WARN** if any of the 16 required feature columns are missing or entirely null in the analytics table.
- `REQUIRED_FEATURE_COLS` = all columns added by `features.py` (rolling means, season, degree days, anomaly score, lags).

**`check_freshness(conn_or_df, table_or_stage, max_lag_days=2)`**
- **WARN** if the most recent date in the `time` column is more than 2 days behind today.
- Detects stale data — an indication that the incremental ingest has not run recently.

#### Key design: dual calling convention

The `_to_df()` helper normalises two different calling patterns:
- `(conn, "raw_historical")` → reads the table from DuckDB, returns `(df, "raw_historical")`.
- `(df, "stage_label")` → uses the DataFrame directly.

This allows the same check functions to be used in both the automated pipeline and interactive notebook exploration.

#### `run_all_checks(conn, raw_df, staging_df, analytics_df)`

Batch runner that applies checks to all three pipeline stages:

| Stage | Checks applied |
|---|---|
| `raw_historical` | `row_count`, `freshness` |
| `staging_historical` | `row_count`, `null_ratio`, `date_continuity`, `value_ranges` |
| `analytics_historical` | `row_count`, `feature_completeness` |

#### `print_quality_report(results)` / `print_check_summary(results)`

Renders a formatted table with ✅ / ⚠️ / ❌ icons per check, plus a summary line (`N PASS | N WARN | N FAIL`). Logs `ERROR` if any FAIL, `WARNING` if any WARN.

**Why it matters:** `quality_checks.py` is the safety net of the entire pipeline. It prevents bad data from silently propagating into analytics tables and the web API. Without it, null-heavy or stale data would reach the dashboard without any alert.

---

### 3.8 `pipeline.py` — 29 KB

**Purpose:** The **master orchestrator** and the **CLI entrypoint** of the entire Heat-Pulse data pipeline. It imports all other `src/` modules and runs them in the correct order. It is the only file a user needs to call directly.

#### CLI usage

```bash
python src/pipeline.py --mode full                              # Full historical reload
python src/pipeline.py --mode incremental                       # Only new days (default)
python src/pipeline.py --mode forecast                          # Update 7-day forecast
python src/pipeline.py --mode incremental --cities Baku Ganja   # Specific cities only
python src/pipeline.py --mode incremental --force-update        # Ignore last stored date
python src/pipeline.py --mode full --start-date 2023-01-01      # Custom start date
python src/pipeline.py --mode incremental --log-level DEBUG      # Verbose logging
```

#### CLI arguments

| Argument | Default | Description |
|---|---|---|
| `--mode` | `incremental` | Run mode: `full`, `incremental`, or `forecast` |
| `--data-dir` | `data` | Path to the data folder |
| `--db-path` | `data/weather.duckdb` | DuckDB file path |
| `--log-dir` | `logs` | Directory for `pipeline.log` |
| `--start-date` | None (→ 2020-01-01) | Custom history start (full mode only) |
| `--cities` | None (all cities) | Whitelist specific cities |
| `--force-update` | False | Re-fetch regardless of last stored date |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

#### `setup_logging(level, log_dir)`

Configures `logging` with two handlers: a rotating file handler writing to `logs/pipeline.log` and a `stdout` stream handler. Clears existing handlers first to prevent duplicate output when called from a Jupyter notebook.

#### `run_pipeline(mode, data_dir, db_path, log_dir, start_date, cities, force_update)`

The **primary public function**. Runs all 5 pipeline stages:

**Stage 1 — INGEST (`_resolve_source`)**
- Determines the data source based on `mode`:
  - `incremental`: calls `ingestion.ingest_incremental()` with `get_latest_dates()` as the start point.
  - `full`: uses the combined CSV if present (fast), otherwise calls `ingestion.ingest_all_cities_full()` (slow).
  - `forecast`: calls `ingestion.ingest_all_forecasts()`.
- `--force-update` overrides the last date check by setting fake `latest_dates` to yesterday.
- `--cities` filters the city dict before fetching.
- If the result is empty (no new data), pipeline exits early with `status="UP_TO_DATE"`.

**Stage 2 — LOAD RAW (`_stage_load_raw`)**
- Calls `database.load_raw_historical()` or `database.load_raw_forecast()` depending on mode.
- Also calls `database.save_raw_as_parquet()` to keep Parquet files in sync.
- Runs `quality_checks.check_row_count()` on `raw_historical` — aborts if 0 rows.

**Stage 3 — CLEAN (`_stage_clean`)**
- Calls `cleaning.clean_raw_to_staging(conn, data_dir)`.
- Returns the number of rows written to `staging_historical`.

**Stage 4 — FEATURE ENGINEERING (`_stage_features`)**
- Calls `features.populate_analytics_tables(conn)`.
- Returns the number of rows written to `analytics_historical`.

**Stage 5 — QUALITY CHECKS**
- Calls `quality_checks.run_all_checks(conn)` across all three stages.
- Prints the formatted report via `quality_checks.print_check_summary()`.

**Audit logging (finally block)**
- Always calls `database.log_pipeline_run()` with the final status (`SUCCESS`, `ABORTED`, `ERROR`, or `UP_TO_DATE`) and all row counts.
- Always closes the DuckDB connection.

#### Return value

`run_pipeline()` returns a summary dict:

```python
{
    "status":         "SUCCESS" | "ABORTED" | "ERROR" | "UP_TO_DATE",
    "mode":           "full" | "incremental" | "forecast",
    "rows_raw":       int,
    "rows_staging":   int,
    "rows_analytics": int,
    "duration_sec":   float,
    "quality_checks": pd.DataFrame,
    "cities_skipped": int,
    "rows_ingested":  int,
}
```

This dict is consumed by the notebook layer for post-run analysis.

**Why it matters:** `pipeline.py` is the operational control surface of the project. It is what a scheduled cron job or Airflow task would call. At 29 KB, it is the largest file in `src/` because it handles all error branching, logging, mode switching, optional module loading, and graceful shutdown logic.

---

## 4. Data Files — Historical CSVs

Five pre-downloaded CSV files are stored directly in `src/`. Each contains 2,310 rows (one row per day) of historical weather data from 2020-01-01 to approximately 2026-04-19.

| File | Size | City | Rows |
|---|---|---|---|
| `baku_historical.csv` | 178 KB | Baku | 2,310 |
| `ganja_historical.csv` | 180 KB | Ganja | 2,310 |
| `lankaran_historical.csv` | 188 KB | Lankaran | 2,310 |
| `mingachevir_historical.csv` | 194 KB | Mingachevir | 2,310 |
| `sumgayit_historical.csv` | 187 KB | Sumqayit | 2,310 |

**Column schema** (16 columns, same as the DuckDB `raw_historical` table):

```
city, date, temperature_2m_max, temperature_2m_min, temperature_2m_mean,
precipitation_sum, rain_sum, snowfall_sum,
wind_speed_10m_max, wind_gusts_10m_max, relative_humidity_2m_mean,
pressure_msl_mean, cloud_cover_mean, shortwave_radiation_sum,
apparent_temperature_max, weather_code
```

**Sample row (Baku, 2020-01-01):**

```
Baku, 2020-01-01, 11.4, 3.6, 6.5, 0.0, 0.0, 0.0,
11.4, 21.6, 90, 1013.4, 30, 8.23, 8.8, 3
```

**Note:** The `date` column in these CSVs is named `date`, whereas the DuckDB schema uses `time`. The `_normalise_raw()` function in `database.py` handles this discrepancy.

**Why they exist:** These files serve as a fast offline seed for the pipeline. When running `--mode full` for the first time, loading from these CSVs is orders of magnitude faster than making 5 separate API calls. They also function as a backup if the Open-Meteo API is unavailable.

---

## 5. Sub-folders

### 5.1 `data/`

Contains the single DuckDB database file:

**`weather.duckdb` — 268 KB**

This is the analytical database that stores all pipeline data across 7 tables (see `database.py` schema section). At 268 KB, it currently holds data for the 5 cities seeded from the historical CSVs. The file size will grow significantly as more cities and years are added via incremental runs.

DuckDB is used instead of PostgreSQL or SQLite because:
- It reads Parquet files natively with SQL.
- It supports window functions (`ROW_NUMBER() OVER PARTITION BY`) needed for deduplication.
- It runs in-process — no server required.
- It is optimised for analytical (column-oriented) workloads over the time-series data used here.

### 5.2 `logs/`

Contains one log file:

**`pipeline.log` — 3 KB**

A structured text log of pipeline executions. Each line follows the format:

```
YYYY-MM-DD HH:MM:SS [LEVEL   ] logger_name — message
```

The most recent run recorded in this file is the April 28, 2026 incremental run — see Section 7 for a detailed analysis of what went wrong.

---

## 6. Pipeline Execution Flow

The following diagram shows the complete data flow from source to DuckDB:

```
                    ┌──────────────────────────────────────────┐
                    │         OPEN-METEO API / LOCAL CSV        │
                    │  (archive-api.open-meteo.com / data/raw/) │
                    └──────────────────┬───────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────┐
                    │  STAGE 1: INGEST (ingestion.py)           │
                    │  fetch_city_weather() for each city       │
                    │  → returns pd.DataFrame                   │
                    └──────────────────┬───────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────┐
                    │  STAGE 2: LOAD RAW (database.py)          │
                    │  load_raw_historical() → raw_historical   │
                    │  save_raw_as_parquet() → data/*.parquet   │
                    │  check_row_count() → FAIL if 0 rows       │
                    └──────────────────┬───────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────┐
                    │  STAGE 3: CLEAN (cleaning.py)             │
                    │  handle_missing_values()                  │
                    │  flag_outliers()                          │
                    │  validate_all_cities()                    │
                    │  → staging_historical (DuckDB + Parquet)  │
                    └──────────────────┬───────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────┐
                    │  STAGE 4: FEATURES (features.py)          │
                    │  compute_all_features()                   │
                    │  → analytics_historical (DuckDB)          │
                    └──────────────────┬───────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────┐
                    │  STAGE 5: QUALITY CHECKS (quality_checks) │
                    │  run_all_checks() across all 3 stages     │
                    │  print_check_summary()                    │
                    └──────────────────┬───────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────┐
                    │  AUDIT LOG (database.py)                   │
                    │  log_pipeline_run() → pipeline_runs table │
                    │  logs/pipeline.log (text file)             │
                    └──────────────────────────────────────────┘
```

---

## 7. Known Issues — `pipeline.log` Analysis

The most recent pipeline run recorded in `logs/pipeline.log` (April 28, 2026, INCREMENTAL mode) **aborted after 613 seconds** with the following errors:

### Error 1: `'time'` KeyError for every city

```
Fetching Baku [2020-01-01 → 2026-04-28] → 2,310 rows fetched.
Baku: FAILED — 'time'
```

**Root cause:** The Open-Meteo API returns the date column as `"time"`. However, when `ingestion.py` builds the DataFrame and the pipeline then tries to write it to DuckDB via `database._normalise_raw()`, there is a key mismatch — the individual city CSVs use `"date"` as the column name, but the API response uses `"time"`. When `ingest_incremental()` constructs the DataFrame from the API response, the column arrives as `"time"`, but `_normalise_raw()` attempts to parse `df["time"]` using `pd.to_datetime()`, which works — meaning the error is likely happening in `pipeline.py`'s `_stage_load_raw()` where a different code path expects a `"time"` column that may be absent or named differently in the returned DataFrame from `_resolve_source()`.

**Fix:** Add a column rename step in `_resolve_source()`:
```python
if "date" in df.columns and "time" not in df.columns:
    df = df.rename(columns={"date": "time"})
```

### Error 2: `unsupported operand type(s) for -: 'datetime.date' and 'NaTType'`

```
Pipeline aborted: unsupported operand type(s) for -: 'datetime.date' and 'NaTType'
```

**Root cause:** In `database.get_latest_dates()`, if a city exists in the database but has `NULL` in the `MAX(time)` column (e.g., all its dates failed to load), the result is `NaT`. Then in `ingestion.ingest_incremental()`, the code tries to compute `last + timedelta(days=1)` where `last` is a `datetime.date` for some cities and `NaT` for others. Subtracting `timedelta` from `NaT` raises this arithmetic error.

**Fix:** Add a null guard in `ingest_incremental()`:
```python
if last is not None and pd.isnull(last):
    last = None  # Treat NaT as "no data" and fetch from DEFAULT_START_DATE
```

**Impact of both errors:** 0 rows were ingested, 0 were staged, and 0 analytics rows were produced. The pipeline was fully aborted and the run was logged with status `ABORTED`.

---

## 8. Module Dependency Map

```
pipeline.py  (orchestrator — imports everything)
    │
    ├── ingestion.py       (fetch from API / CSV)
    │       └── config.py  (API URLs, city list)
    │
    ├── database.py        (DuckDB interface)
    │
    ├── cleaning.py        (missing values, outliers, date gaps)
    │       └── database.py (connection)
    │
    ├── features.py        (feature engineering)
    │       └── database.py (connection)
    │
    └── quality_checks.py  (automated quality gates)
            └── database.py (optional connection)

__init__.py  (no imports — package marker only)
config.py    (no imports from this project)
```

All modules import standard libraries (`pandas`, `numpy`, `duckdb`, `requests`, `logging`, `pathlib`) and are otherwise self-contained. `pipeline.py` uses `try/except ImportError` to handle missing `cleaning` or `features` modules gracefully, so the pipeline can run in partial mode if either module is unavailable.

---

*This document covers all files and folders within `Heat-Pulse/src/` as of May 2, 2026.*
