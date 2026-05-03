# 🌡️ Heat-Pulse

**A regional weather data pipeline, feature engineering system, and analytics platform for 94 cities across the Caspian–Caucasus region.**

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Geographic Scope](#2-geographic-scope)
3. [Project Structure](#3-project-structure)
4. [Data Pipeline Architecture](#4-data-pipeline-architecture)
5. [Data Layers (Files)](#5-data-layers-files)
6. [Feature Engineering](#6-feature-engineering)
7. [Alert & Classification System](#7-alert--classification-system)
8. [ML-Ready Output](#8-ml-ready-output)
9. [Key Statistics](#9-key-statistics)
10. [Tech Stack](#10-tech-stack)

---

## 1. Project Overview

**Heat-Pulse** is a data engineering and analytics project that collects, processes, and enriches daily historical weather data for 94 cities spanning 8 countries in the Caspian Sea–Caucasus–Central Asia region. The project covers the period **January 1, 2020 – April 2026**, and includes a **7-day rolling forecast layer**.

The system transforms raw Open-Meteo API data through a structured multi-stage pipeline into a fully feature-engineered, alert-tagged, ML-ready dataset. The final output is designed for use in dashboards, visualisations, predictive models, and daily weather briefs.

---

## 2. Geographic Scope

The project covers **94 cities** across **8 countries**, grouped into **6 regions**:

| Country | Cities | Region(s) |
|---|---|---|
| **Azerbaijan** | 75 | Caspian, Caucasus |
| **Iran** | 4 | Caspian |
| **Georgia** | 3 | Caucasus |
| **Kazakhstan** | 3 | Western Kazakhstan |
| **Turkey** | 3 | Eastern Anatolia |
| **Armenia** | 2 | Caucasus |
| **Russia** | 2 | North Caucasus |
| **Turkmenistan** | 2 | Western Turkmenistan |

**Continents covered:** Asia, Eurasia

The dataset has a deliberate focus on **Azerbaijan** (75 of 94 cities), covering every rayon and major settlement — making it one of the most granular sub-national weather datasets for the country.

---

## 3. Project Structure

```
Heat-Pulse/
│
├── README.md
├── .gitignore
├── requirements.txt
│
├── data/                          ← All data files (see section 5)
│   ├── raw/                       ← Raw per-city CSVs from API
│   ├── all_94_cities_historical_combined.csv
│   ├── raw.parquet
│   ├── staging_historical.parquet
│   ├── staging_forecast.parquet
│   ├── staging.parquet
│   ├── for_vis.parquet
│   ├── final_weather_data_encoded.parquet
│   ├── weather.duckdb
│   └── weather.duckdb.wal
│
├── daily-briefs/                  ← Auto-generated daily weather summaries
├── logs/                          ← Pipeline run logs
├── notebooks/                     ← Jupyter notebooks for exploration
├── reports/                       ← Analysis reports
├── src/                           ← Source code (ETL, feature eng., alerts)
└── web/                           ← Web/dashboard front-end
```

---

## 4. Data Pipeline Architecture

The pipeline follows a classic **Medallion / Layered** architecture:

```
Open-Meteo API
      │
      ▼
[RAW LAYER]
  raw.parquet  /  all_94_cities_historical_combined.csv
  • 18 columns: raw meteorological variables only
  • No transformations
      │
      ▼
[STAGING LAYER]
  staging_historical.parquet  +  staging_forecast.parquet
  • Outlier flags added for each numeric variable
  • Historical: 217,234 rows | Forecast: 658 rows (7-day ahead)
      │
      ▼
[FEATURE ENGINEERING LAYER]
  staging.parquet  →  for_vis.parquet
  • 45+ engineered columns added
  • City metadata joined (district, region, continent, elevation)
  • Alert system applied
      │
      ▼
[ML-READY LAYER]
  final_weather_data_encoded.parquet
  • Categorical variables one-hot encoded
  • Ready for model training
```

The pipeline also maintains a **DuckDB** database (`weather.duckdb`) as a queryable store for all layers.

---

## 5. Data Layers (Files)

### `raw.parquet` — Raw Layer
- **Rows:** 217,234 | **Columns:** 18
- Contains only the original API variables: temperatures (max/min/mean), precipitation, rain, snowfall, wind speed & gusts, humidity, pressure, cloud cover, solar radiation, apparent temperature, weather code, and coordinates.
- One row per city per day, from 2020-01-01 onwards.

### `all_94_cities_historical_combined.csv` — Raw CSV Archive
- Same schema as `raw.parquet` in CSV format (~19.7 MB).
- Combines individual per-city CSV files from the `data/raw/` folder.

### `staging_historical.parquet` — Cleaned Historical
- **Rows:** 217,234 | **Columns:** 27
- Adds **outlier detection flags** for 9 key variables (`temperature_2m_max_outlier_flag`, `precipitation_sum_outlier_flag`, etc.)
- Each flag is a boolean marking statistically anomalous values.

### `staging_forecast.parquet` — Cleaned Forecast
- **Rows:** 658 | **Columns:** 27
- Same structure as staging_historical but for the **next 7 days** (covers 94 cities × 7 days).
- Date range: April 21–27, 2026 at time of last run.
- Includes the same outlier flags for quality control on forecasted values.

### `for_vis.parquet` — Visualisation & Analysis Layer ⭐
- **Rows:** 503,700 | **Columns:** 45
- The primary analysis-ready file. Combines historical and forecast data with all engineered features, city metadata, alert classifications, and human-readable labels.
- Covers **2020-01-01 to 2026-04-18** across 94 cities.

### `final_weather_data_encoded.parquet` — ML-Ready Layer
- **Rows:** 363,298 | **Columns:** 48
- Same as the staging/feature layer but with categorical variables one-hot encoded:
  - `weather_category` → 4 binary columns (Cloudy, Drizzle, Rain, Snowfall)
  - `region` → 5 binary columns
  - `continent` → 1 binary column
- Ready for use as input to machine learning models.

---

## 6. Feature Engineering

The pipeline adds the following engineered features on top of the raw meteorological variables:

### Temporal Features
| Feature | Description |
|---|---|
| `year`, `month`, `day` | Extracted from date |
| `day_of_year` | Integer 1–366 |
| `season` | Winter / Spring / Summer / Autumn |
| `is_weekend` | Binary flag (Saturday or Sunday) |

### Temperature-Derived Features
| Feature | Description |
|---|---|
| `temp_range` | Daily max − min temperature (°C) |
| `temp_anomaly` | Deviation from long-term seasonal mean for that location |
| `feels_like_diff` | Apparent temperature − actual mean temperature |
| `heat_stress_index` | Composite index combining temperature and humidity |
| `CDD` | Cooling Degree Days — accumulated heat load above a baseline |
| `regional_temp_mean` | Mean temperature for the city's region on that day |

### Precipitation Features
| Feature | Description |
|---|---|
| `snow_ratio` | Fraction of total precipitation that fell as snow |
| `rain_ratio` | Fraction of total precipitation that fell as rain |
| `is_rainy` | Binary flag: rain sum > threshold |
| `is_snowy` | Binary flag: snowfall sum > threshold |

### Wind & Interaction Features
| Feature | Description |
|---|---|
| `wind_temp_interaction` | Wind speed × temperature interaction term |

### Geographic/Metadata Features
| Feature | Description |
|---|---|
| `city`, `district` | City name and administrative district |
| `region`, `continent` | Regional and continental classification |
| `country` | Country name |
| `lat`, `lon` | Coordinates |
| `elevation` | Elevation in meters above sea level |
| `is_clear` | Binary flag for clear sky conditions |

---

## 7. Alert & Classification System

Heat-Pulse includes a multi-level alert engine that classifies each city-day observation into actionable weather risk categories.

### Weather Category
Each day is assigned one of 5 human-readable categories based on WMO weather codes and precipitation:

| Category | Condition |
|---|---|
| `Clear Sky` | No precipitation, low cloud cover |
| `Cloudy` | High cloud cover, no significant precipitation |
| `Drizzle` | Light precipitation |
| `Rain` | Moderate to heavy rainfall |
| `Snowfall` | Snow present |

Distribution across the dataset (503,700 rows):
- Cloudy: 263,360 (52%)
- Drizzle: 117,239 (23%)
- Clear Sky: 48,965 (10%)
- Rain: 43,486 (9%)
- Snowfall: 30,650 (6%)

### Temperature Alert Level
A signed integer scale centred on 0 (normal):

| Level | Meaning |
|---|---|
| `+3` | Extreme heat anomaly |
| `+2` | Significant heat |
| `+1` | Mild warmth above normal |
| `0` | Normal / no alert |
| `-1` | Mild cold below normal |
| `-2` | Significant cold |
| `-3` | Extreme cold anomaly |

### Persistence Alert Level
Flags sustained multi-day anomaly streaks — i.e., when a heat or cold event persists over consecutive days, escalating the alert level.

### Alert Color
A colour-coded summary for dashboard display:

| Color | Meaning |
|---|---|
| 🟢 Green | Normal conditions (500,082 days — 99.3%) |
| 🟡 Yellow | Mild temperature or precipitation alert |
| 🔵 Deep Blue | Cold snap |
| 🩵 Light Blue | Cool anomaly |
| 🔴 Red | Heat alert |
| 🔵 Blue | Moderate cold |
| 🟠 Orange | Moderate heat |

---

## 8. ML-Ready Output

`final_weather_data_encoded.parquet` is structured for supervised learning tasks. Key characteristics:

- **363,298 rows × 48 columns**
- All categorical variables converted to binary (one-hot) columns
- No text/string columns remain — fully numeric
- Includes engineered interaction terms, anomaly scores, and degree days
- Suitable for regression (e.g., temperature forecasting), classification (e.g., weather category prediction), or anomaly detection tasks

Encoded columns added:
- `continent_Eurasia`
- `region_Caucasus`, `region_Eastern Anatolia`, `region_North Caucasus`, `region_Western Kazakhstan`, `region_Western Turkmenistan`
- `weather_category_Cloudy`, `weather_category_Drizzle`, `weather_category_Rain`, `weather_category_Snowfall`

---

## 9. Key Statistics

| Metric | Value |
|---|---|
| Total city-day records (vis layer) | 503,700 |
| Cities covered | 94 |
| Countries | 8 |
| Regions | 6 |
| Date range | 2020-01-01 → 2026-04-18 |
| Forecast horizon | 7 days rolling |
| Raw variables per record | 18 |
| Engineered features (final) | 45 |
| ML-encoded features | 48 |
| Max temperature anomaly | +27°C above baseline |
| Min temperature anomaly | −36°C below baseline |
| Max CDD in a single day | 22.8 |
| Heat stress index range | −20.9 to +25.8 |

---

## 10. Tech Stack

| Component | Technology |
|---|---|
| Data source | [Open-Meteo API](https://open-meteo.com/) (free, no key required) |
| Data storage | Apache Parquet, DuckDB, CSV |
| Data processing | Python (pandas, pyarrow) |
| Query engine | DuckDB |
| Notebooks | Jupyter |
| Visualisation output | `for_vis.parquet` (Tableau / Power BI / custom web) |
| Web layer | `web/` folder (dashboard front-end) |
| Daily reports | `daily-briefs/` auto-generated summaries |

---

*Last pipeline run: May 2, 2026 | Data coverage through: April 18–27, 2026*
