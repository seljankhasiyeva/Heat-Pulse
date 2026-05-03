# 📓 Notebooks Folder — Full Report

**Project:** Heat-Pulse  
**Folder:** `notebooks/`  
**Total Files:** 8 Jupyter notebooks + 1 `.gitkeep` placeholder  
**Date Modified:** May 2, 2026  
**Purpose:** Day-by-day working notebooks covering API ingestion, database design, data cleaning, pipeline automation, EDA, statistical analysis, and predictive modeling

---

## Table of Contents

1. [Folder Overview](#1-folder-overview)
2. [File Inventory](#2-file-inventory)
3. [Notebook-by-Notebook Breakdown](#3-notebook-by-notebook-breakdown)
4. [Key Results & Outputs](#4-key-results--outputs)
5. [Libraries & Dependencies Used](#5-libraries--dependencies-used)
6. [Visualisations Produced](#6-visualisations-produced)
7. [Figures Saved to `reports/figures/`](#7-figures-saved-to-reportsfigures)

---

## 1. Folder Overview

The `notebooks/` folder is the **live working environment** of the Heat-Pulse project. Each notebook corresponds directly to one project day and serves as the documented, executed record of that day's work — combining code cells, outputs, inline findings, and Markdown commentary.

Unlike the `src/` modules which contain reusable, importable Python code, the notebooks are **exploratory and demonstrative**: they import from `src/`, run the pipeline stages, display outputs, and produce the analysis that informs subsequent days.

---

## 2. File Inventory

| File | Size | Role |
|------|------|------|
| `.gitkeep` | 0 KB | Git placeholder to track empty folder |
| `day_01_exploration.ipynb` | 277 KB | API exploration, city/variable selection |
| `day_02_ingestion.ipynb` | 7 KB | Bulk ingestion run + data audit |
| `day_03_database.ipynb` | 23 KB | DuckDB setup, loading, validation, SQL queries |
| `day_04_cleaning.ipynb` | 4,092 KB | Data quality assessment + feature engineering |
| `day_05_pipeline.ipynb` | 59 KB | End-to-end pipeline demo (full + incremental) |
| `day_06_eda.ipynb` | 4,534 KB | Exploratory data analysis — 13 visualisations |
| `day_07_statistical_analysis.ipynb` | 991 KB | Hypothesis testing + correlation + feature selection |
| `day_08_modeling_fixed.ipynb` | 81 KB | Full ML pipeline — regression + classification |

> The large size of `day_04_cleaning.ipynb` (4 MB) and `day_06_eda.ipynb` (4.5 MB) is due to embedded chart images in the notebook outputs.

---

## 3. Notebook-by-Notebook Breakdown

---

### `day_01_exploration.ipynb` — API Exploration & Project Setup

**Total cells:** 8 (6 code, 2 Markdown)

This notebook is the project's entry point. It establishes what data will be collected and why.

**What it does:**

**Task 2 — API Exploration:** Calls the Open-Meteo Historical Archive endpoint for Baku (latitude 40.41, longitude 49.87) over one year. Inspects the full response structure — confirming the `daily` key contains a dictionary of date + variable arrays, with metadata fields for coordinates, timezone, elevation, and `generationtime_ms`. Verifies that `daily_units` correctly maps variable names to their units (e.g., `temperature_2m_max` → `°C`, `windspeed_10m_max` → `km/h`).

A temperature visualisation is produced with `fill_between` for the min/max band and a second line for the mean — confirming no gaps and a max recorded temperature of **38.8°C on 2025-07-14** for Baku.

The 7-day forecast endpoint is also called and compared structurally — confirmed to share the same JSON schema as the historical endpoint.

**Task 3 — City & Variable Selection:** Documents the final city list across 8 countries — including Baku, Ganja, Nakhchivan, Sumqayit, Lankaran, and 89 others. A city metadata table is produced confirming names, coordinates, and rationale. Six core weather variables are selected and documented with their units and analytical relevance:

| Variable | Unit | Relevance |
|---|---|---|
| `temperature_2m_max` | °C | Primary regression target |
| `precipitation_sum` | mm | Rain/drought indicator |
| `windspeed_10m_max` | km/h | Wind stress |
| `relative_humidity_2m_mean` | % | Comfort index |
| `rain_sum` | mm | Subset of precipitation |
| `snowfall_sum` | cm | Winter severity |

---

### `day_02_ingestion.ipynb` — Bulk Ingestion & Data Audit

**Total cells:** 4 (3 code, 0 Markdown)

This notebook runs the full 94-city historical ingestion and produces the raw data audit.

**What it does:**

Imports `ingestion` and `config` modules from `src/`. Implements a **batch processing** approach — splits the 94-city list into batches of 10, with a 30-second API cooldown between batches to avoid HTTP 429 (Too Many Requests) errors. Each batch's result is appended to a growing Parquet file (`raw_for_day_2.parquet`).

A separate cell fetches the 7-day forecast for each city, saving individual CSVs (`{city_name}_forecast.csv`), skipping cities where the file already exists.

**Data Audit Results (Task 4):**

| Metric | Result |
|---|---|
| Total rows ingested | **215,928** |
| Date gaps (missing days) | **0 cities affected** |
| Null values across all columns | **0** |
| Requested date range | 2020-01-01 to 2026-04-19 |
| Actual date range delivered | 2020-01-01 to 2026-04-19 ✅ |

**Verdict:** Clean ingestion — 100% date coverage, zero nulls.

---

### `day_03_database.ipynb` — Database Design & Loading

**Total cells:** 7 (7 code, 0 Markdown)

This notebook sets up the DuckDB analytical database and validates the loaded data with SQL.

**What it does:**

Calls `get_connection()`, `create_schemas()`, and `load_raw_data()` from `src/database.py`. Connects to the DuckDB file at `data/weather.duckdb`. Loads two CSV files directly into DuckDB tables:

- `all_94_cities_historical_combined.csv` → `raw.raw_historical` (215,928 rows)
- `all_94_cities_forecast_combined.csv` → `raw.raw_forecast` (658 rows)

**Validation Results:**

| Check | Status |
|---|---|
| No duplicate city-date combinations | ✅ PASS |
| No gaps in date range | ✅ PASS |
| Latitude/longitude present for all rows | ✅ PASS |

**Analytical SQL Queries (Task 4):**

Four queries are run as proof of functionality:

1. **Average max temperature per city per year** — shows Absheron ranging from 18.91°C (2020) to 20.26°C (2023)
2. **Top 10 hottest days across all cities** — Balkanabat (Turkmenistan) tops the list with **45.9°C on July 5, 2021**
3. **Highest variance in daily precipitation** — computed city by city
4. **Days with zero precipitation per city per year** — used as a drought-day proxy

---

### `day_04_cleaning.ipynb` — Data Cleaning & Feature Engineering

**Total cells:** 18 (12 code, 6 Markdown)

The largest and most detailed notebook. Systematically examines data quality across five dimensions and engineers 30+ new features.

**What it does:**

First merges all 94 individual `*_historical.csv` files from `data/raw/` into one master DataFrame of **215,928 rows** (confirmed with file count check).

**Task 1 — Data Quality Assessment:**

**1.1 Missing Values:** Per-city, per-column null percentage is computed. Result: **0% nulls across all columns for all 94 cities** — confirmed clean.

**1.2 Outliers (IQR method):** Box plots generated for all numerical variables by city. Key finding: `temperature_2m_max` returns **no outliers detected**, suggesting genuine extreme values fit within the IQR bounds for this diverse regional dataset.

**1.3 Temporal Gaps:** Each city shows **2,301 "missing" dates** — this is the expected number of future dates from the current end-of-dataset date to some far future reference point. Within the actual 2020–2026 coverage window, **no gaps found**.

**1.4 Consistency:** 94 forecast CSVs merged into `all_94_cities_forecast_combined.csv` (658 rows). Overlap check with historical: **no overlapping date ranges found** — the forecast starts after the latest historical date, which is the correct behaviour.

**1.5 Sensor Artefacts:** Two anomaly types detected:
- **Stuck sensor records** (3+ consecutive days with constant mean temperature): **511 records** flagged — mostly occurring in summer months when temperatures genuinely plateau
- **Sudden jumps** (>15°C change in 24 hours): **2 events** detected across all 94 cities

**Feature Engineering (`src/features.py`):** The notebook populates the `analytics` layer with:
- Rolling 7-day and 30-day means for temperature
- Seasonal labels (Winter/Spring/Summer/Autumn)
- Temperature range (max − min)
- Heating Degree Days (HDD) and Cooling Degree Days (CDD)
- Anomaly score vs. historical mean for the calendar day
- Lag-1 and lag-2 features for temperature and precipitation
- Weather category labels, heat stress index, wind×temperature interaction

---

### `day_05_pipeline.ipynb` — Pipeline Automation & Quality Gates

**Total cells:** 16 (6 code, 10 Markdown)

Demonstrates the fully automated, end-to-end pipeline with both full and incremental modes.

**What it does:**

Sets up project root paths and imports `run_pipeline` from `src/pipeline.py`.

**Full Mode Run:**  
Calls `run_pipeline(mode='full', ...)` which drops and recreates the raw table, then re-fetches all cities from 2020-01-01. During the notebook run, an API rate-limit (HTTP 429) is encountered for some cities — the pipeline retries with exponential backoff and logs the failures. Despite API throttling during the notebook demo, the pipeline handles errors gracefully without crashing.

**Database Inspection After Full Run:**

```
=============================================
  VERİLƏNLƏR BAZASI — CƏDVƏLLƏRİN SƏTIR SAYI
=============================================
  raw_historical        9,248
  raw_forecast              0
  staging_historical    9,248
  analytics_historical  9,248
```

*(The 9,248 rows reflect a partial run during the notebook demo; the full production run yielded 217,234 rows)*

**Incremental Mode Run:**  
The pipeline correctly detects that all cities are already up-to-date and skips the API fetch stage — completing in **1.7 seconds** instead of minutes.

**Quality Gate Results (standalone demo):**

| Check | Stage | Status |
|---|---|---|
| `row_count` | raw_historical | ✅ PASS — 9,248 rows |
| `freshness` | raw_historical | ✅ PASS — data is fresh |

**Pipeline Architecture Diagram:** A full ASCII art diagram is embedded in cell [13] showing all stages: `Open-Meteo API → INGEST → raw tables → CLEAN → staging tables → FEATURES → analytics tables → Quality Gates at each stage`.

**CLI Equivalents** are documented in the final cell:
```bash
python src/pipeline.py --mode full
python src/pipeline.py --mode incremental
```

---

### `day_06_eda.ipynb` — Exploratory Data Analysis

**Total cells:** 30 (20 code, 10 Markdown)  
**Figures produced:** 13 (saved to `reports/figures/`)

The richest visualisation notebook in the project.

**What it does:**

Loads the full 215,928-row raw dataset. Focuses analysis on **10 representative cities**: Baku, Ganja, Lankaran, Nakhchivan, Sheki, Sumqayit, Sabirabad, Mingachevir, Saatli, Imishli.

**Task 1 — Descriptive Statistics:**
- Summary table with count, mean, std, min, Q1, median, Q3, max, skewness, kurtosis per city per variable
- **Yearly summary** (saved as plot_1.png): Linear trend slopes for average temperature per city — all cities show slight negative trends in the 2020–2026 window (e.g., Baku: −0.78°C/yr, R²=0.32, p=0.187 — not statistically significant)
- **Monthly variability** (saved as plot_2.png): Most variable months identified — March for temperature, September for precipitation and humidity, February for wind
- **Top 10 Extreme Days**: Hottest — Balkanabat at **45.9°C (July 5, 2021)**; detailed tables produced for coldest and wettest days

**Task 2 — Distribution Analysis:**
- **Temperature histograms** (plot_3.png): Most cities exhibit **bimodal distributions** — two peaks reflecting hot summers and cold winters, violating normality. Normal distribution curves overlaid for comparison.
- **Seasonal box plots** (plot_4.png): Summer temperatures tightly clustered (high and consistent); Spring/Autumn show widest variance — consistent with continental climate patterns.
- **Precipitation violin plots** (plot_5.png): Most days show zero precipitation ("fat bottom" violins). Lankaran and Sheki identified as notably wetter cities.
- **QQ-plots** (plot_6.png): Confirm non-normality for all cities — S-shaped curves deviating from the theoretical normal line, supporting use of non-parametric tests.

**Task 3 — Time Series Exploration:**
- **Full 5-year time series with 30-day rolling mean** (plot_7.png): Eight-panel chart showing daily max temperature 2020–2026 per city
- **Seasonal decomposition for Baku** (plot_8.png): Additive model separates clear annual seasonal cycle (~15°C amplitude), a slight upward trend component, and random residuals
- **Year-over-year overlay** (plot_9.png): All years plotted on a day-of-year axis for Baku — 2024 visually appears as a warm year across summer months
- **Calendar heatmap** (plot_10.png): Year × day-of-year grid coloured by temperature — immediately reveals the seasonal pattern and year-to-year variation

**Task 4 — Cross-City Comparison:**
- **Paired time series 2023** (plot_11.png): All 10 cities on one chart — reveals high synchrony with Nakhchivan as consistently coldest in winter
- **Pearson cross-city correlation**: Cities are highly correlated (typically >0.90) — confirming shared regional climate signal
- **Baku scatter matrix** (plot_12.png): Pairplot of 5 key variables coloured by season
- **Baku correlation heatmap** (plot_13.png): Strongly correlated pairs (>0.7) identified — notably: `apparent_temperature_max` ↔ `temperature_2m_max` (r=0.992), `rain_sum` ↔ `precipitation_sum` (r=0.990)

**Task 5 — Key Findings Summary:**

Five main observations documented:
1. Bimodal temperature distributions confirm strong continental seasonality
2. Significant seasonal temperature variance — summer tightly clustered, autumn/spring wide
3. Azerbaijan is predominantly dry — most days zero precipitation; Lankaran and Sheki are exceptions
4. Cities are highly correlated, with Nakhchivan as the coldest outlier (continental interior)
5. Multi-year temperature trends are slightly negative in this dataset but not statistically significant at p<0.05

---

### `day_07_statistical_analysis.ipynb` — Statistical Analysis & Feature Selection

**Total cells:** 18 (9 code, 9 Markdown)

Formalises EDA observations into statistical tests and prepares the feature set for modeling.

**What it does:**

**Task 1 — Three Hypothesis Tests:**

**Hypothesis 1 — Heat Stress Impact:**
- H₀: No effect of `heat_stress_index` on `impact_score`
- H₁: High heat stress positively associates with higher impact score
- **Test:** Welch's two-sample t-test (Levene's test confirmed unequal variances, p=1.68e-7)
- **Sample sizes:** High stress N=107,939; Low stress N=107,989
- **Result:** T-statistic=14.14, p=2.26e-45 → **Reject H₀**
- **Interpretation:** High heat stress index days produce significantly higher impact scores

**Hypothesis 2 — Persistence Effect:**
- H₀: Duration of extreme event (persistence level) has no effect on impact score
- H₁: Longer persistence → higher impact score
- **Test:** One-way ANOVA across 4 persistence levels (0, 1, 2, 3)
- **Result:** F=17,872.20, p=0.0000 → **Reject H₀**
- **Interpretation:** Impact score rises significantly with each persistence level — sustained heat events are disproportionately more impactful

**Hypothesis 3 — Cooling Interaction:**
- H₀: No correlation between `wind_temp_interaction` and `impact_score`
- H₁: Wind × temperature interaction significantly affects impact score
- **Test:** Welch's t-test
- **Result:** T-statistic=92.08, p=0.0000 → **Reject H₀**
- **Interpretation:** The combined wind-temperature signal has a highly significant effect on impact scores

All three null hypotheses are rejected at p<0.0001.

**Task 2 — Correlation Analysis:**
Side-by-side Pearson and Spearman heatmaps produced. Redundant feature pairs (>0.85 correlation) flagged:
- `temperature_2m_max` ↔ `temperature_2m_min`: 0.94
- `temperature_2m_max` ↔ `temperature_2m_mean`: 0.98
- `precipitation_sum` ↔ `rain_sum`: 0.99
- `wind_speed_10m_max` ↔ `wind_gusts_10m_max`: 0.98
- `temperature_2m_max` ↔ `apparent_temperature_max`: 0.99

**Task 3 — Feature Selection:**
Prediction targets defined:
- **Daily regression:** `temperature_2m_max`, `temperature_2m_min`, `relative_humidity_2m_mean`, `impact_score`
- **Classification:** `weather_category`, rain probability

Redundant features dropped from model input (e.g., `temperature_2m_min` and `temperature_2m_mean` dropped in favour of `temperature_2m_max` as primary temperature representation).

**Task 4 — Advanced Analyses:**
- **ANOVA across 94 cities:** F=162.51, p=0.0000 — temperatures differ significantly across cities
- **Tukey HSD post-hoc analysis:** Full pairwise city comparison plotted (12 × 30 inch figure)
- **Effect sizes computed:**
  - Eta-squared (city vs temperature): 0.065
  - Cohen's d (wind vs impact): 0.741 (large effect)
  - Cramér's V (season vs weather category): 0.257 (moderate association)
- **Autocorrelation discussed:** Weather on day t is correlated with day t-1, which technically violates the independence assumption of standard t-tests — addressed in Day 8's model design through lag features

---

### `day_08_modeling_fixed.ipynb` — Predictive Modeling & Evaluation

**Total cells:** 39 (19 code, 20 Markdown)  
**Models built:** 6 (4 regression + 2 classification)

The most technically advanced notebook. Implements a complete ML pipeline with temporal splits, multiple models, confidence intervals, and residual diagnostics.

**What it does:**

**Configuration:**
- **Regression target:** `temperature_2m_max` (next-day maximum temperature)
- **Classification target:** `will_rain` (precipitation > 0 mm, binary)
- **Linear model features:** 15
- **XGBoost features:** 28

**Data Loading:** Uses `final_weather_data_encoded.parquet` — 363,298 rows × 54 columns across 94 cities (2020–2026).

**Task 1 — Temporal Train/Test Split:**
```
TRAIN: 2020–2024  →  288,591 rows  (83.3%)
TEST:  2025        →   57,649 rows  (16.7%)
```
After lag feature engineering: 80 total features per row.

**Baseline Models:**
| Baseline | RMSE | MAE | R² |
|---|---|---|---|
| Persistence (yesterday = tomorrow) | 2.225 | 1.514 | 0.9511 |
| Seasonal Naive (historical mean for that calendar day) | 4.483 | 3.484 | 0.8015 |

The persistence baseline is very strong (R²=0.951), correctly setting a high bar.

**Task 2 — Model Building:**

| Model | RMSE | MAE | R² | CI Width | Notes |
|---|---|---|---|---|---|
| Baseline — Persistence | 2.225 | 1.514 | 0.9511 | — | Hard to beat |
| Linear Regression | 2.246 | 1.664 | 0.9502 | 8.74°C | Top feature: lag-1 temp (coef +7.11) |
| OLS (statsmodels) | 2.246 | 1.664 | 0.9502 | 0.065°C | Adj.R²=0.9513, AIC=1,280,448 |
| Ridge (α=100) | 2.246 | 1.664 | 0.9502 | 8.74°C | Best CV R²=0.9505 |
| Lasso (α=0.001) | 2.244 | 1.662 | 0.9503 | 8.74°C | 1/15 features zeroed |
| **XGBoost** | **2.220** | **1.647** | **0.9513** | **8.26°C** | Best performer |

**Classification Results (predicting rain yes/no):**

| Model | Accuracy | F1 | Precision | Recall |
|---|---|---|---|---|
| Logistic Regression | 0.675 | 0.605 | 0.529 | 0.706 |
| **XGBoost Classifier** | **0.819** | **0.745** | **0.742** | **0.748** |

**Task 4 — Residual Diagnostics (XGBoost & OLS):**
- **Shapiro-Wilk tests:** Both models return non-normal residuals (XGBoost W=0.981, p=1.3e-25; OLS W=0.977, p=6.2e-28) — expected for large n; confirmed by visual QQ-plots
- **Residual vs. fitted plots:** Residuals are randomly scattered around zero for XGBoost; OLS shows slight heteroscedasticity at temperature extremes
- **ACF of residuals:** Checked for temporal autocorrelation — lag features partially address this
- Diagnostic plots saved to `reports/figures/` for all models

**Final Model Selection:**
**XGBoost is selected** as the best regression model (RMSE=2.220, R²=0.9513, narrowest CI width of 8.26°C). It slightly but consistently outperforms all linear models.

For classification, **XGBoost Classifier** is selected (F1=0.745 vs. Logistic Regression F1=0.605) — a meaningful improvement, especially in precision.

---

## 4. Key Results & Outputs

| Metric | Value |
|---|---|
| Total rows in raw dataset | 215,928 |
| Total rows in encoded ML dataset | 363,298 |
| Cities covered | 94 |
| Zero data gaps | ✅ Confirmed |
| Zero null values in raw data | ✅ Confirmed |
| Sensor artefacts flagged | 511 stuck-sensor records, 2 sudden jumps |
| All 3 hypotheses | Rejected at p < 0.0001 |
| Best regression model | XGBoost (RMSE=2.220, R²=0.9513) |
| Best classification model | XGBoost Classifier (F1=0.745, Accuracy=0.819) |
| Figures saved to reports/figures/ | 13 EDA + 8 modelling plots |

---

## 5. Libraries & Dependencies Used

| Library | Used In |
|---|---|
| `pandas`, `numpy` | All notebooks |
| `requests` | day_01, day_02 |
| `matplotlib`, `seaborn` | day_01, day_06, day_07, day_08 |
| `duckdb` | day_03, day_05 |
| `scipy.stats` | day_04, day_06, day_07, day_08 |
| `statsmodels` | day_06 (decompose), day_07, day_08 (OLS) |
| `sklearn` | day_08 (LinearRegression, Ridge, Lasso, LogisticRegression, cross_val_score, TimeSeriesSplit) |
| `xgboost` | day_08 |
| `pathlib` | day_05, day_08 |
| `logging` | day_05 (via pipeline module) |

---

## 6. Visualisations Produced

**Day 6 EDA (13 figures):**

| Plot | Description |
|---|---|
| plot_1 | Yearly average temperature, total precipitation, max wind — 3-panel |
| plot_2 | Monthly mean ± std for 6 variables — 6-panel |
| plot_3 | Temperature histograms with normal fit — 8-panel (one per city) |
| plot_4 | Seasonal box plots — 8-panel |
| plot_5 | Precipitation violin plots (raw + log-scale) — 2-panel |
| plot_6 | QQ-plots for temperature normality check — 8-panel |
| plot_7 | Full 5-year time series with 30-day rolling mean — 8-panel |
| plot_8 | Seasonal decomposition (trend/seasonal/residual) for Baku |
| plot_9 | Year-over-year temperature overlay — day-of-year axis |
| plot_10 | Calendar heatmap: Baku temperatures (year × day-of-year) |
| plot_11 | Cross-city temperature comparison — 2023 |
| plot_12 | Scatter matrix — Baku (5 variables, season-coloured) |
| plot_13 | Correlation heatmap — Baku numerical features |

**Day 8 Modeling (8+ figures):**

| Plot | Description |
|---|---|
| `Linear_Regression_ci_plot.png` | Prediction vs actual with 95% CI band |
| `OLS_statsmodels_ci_plot.png` | OLS predictions with CI |
| `Ridge_Regression_ci_plot.png` | Ridge predictions with CI |
| `Lasso_Regression_ci_plot.png` | Lasso predictions with CI |
| `XGBoost_Regressor_ci_plot.png` | XGBoost predictions with CI |
| `Logistic_Regression_clf_diagnostics.png` | Confusion matrix + probability distribution |
| `XGBoost_Classifier_clf_diagnostics.png` | Confusion matrix + probability distribution |
| `model_comparison_summary.png` | RMSE bar chart across all regression + classification models |
| `XGBoost_Regressor_resid_*.png` | 4 residual diagnostic panels for XGBoost |
| `OLS_statsmodels_resid_*.png` | 4 residual diagnostic panels for OLS |

---

## 7. Figures Saved to `reports/figures/`

All figures are saved with `dpi=300` using `bbox_inches='tight'` for print quality. The total figure count saved across both EDA and modeling days is **21+** PNG files.

---

*Report generated: May 2, 2026 | Folder: `Heat-Pulse/notebooks/`*
