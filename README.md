# Heat-Pulse
### Team sPySci (Spicy) 


## 1. Problem Statement
**Objective:** How can we predict "Extreme Heat Events" (EHE) to prevent power generator burn-outs and optimize grid load management?

This project develops an end-to-end weather intelligence pipeline to identify days exceeding the 90th percentile of historical summer temperatures. Our goal is to provide a 30-day risk assessment tool for **Azerenerji** and industrial facility managers in Azerbaijan to prevent technical failures similar to those seen in 2018 and 2024.

---

## 2. Data Sources & Variables
We use the **Open-Meteo API** to bridge historical weather patterns with real-time forecasts.

* **Locations (94 Cities):** Baku (AZ), Ganja (AZ), Nakhchivan (AZ), Sumqayit (AZ), Lankaran (AZ), Mingachevir (AZ), Naftalan (AZ), Khankendi (AZ), Sheki (AZ), Shirvan (AZ), Yevlakh (AZ), Khirdalan (AZ), Agjabadi (AZ), Agdam (AZ), Agdash (AZ), Agdere (AZ), Agstafa (AZ), Agsu (AZ), Astara (AZ), Babek (AZ), Balakan (AZ), Beylagan (AZ), Barda (AZ), Bilesuvar (AZ), Jabrayil (AZ), Jalilabad (AZ), Julfa (AZ), Dashkasan (AZ), Fuzuli (AZ), Gadabay (AZ), Goranboy (AZ), Goychay (AZ), Goygol (AZ), Hajigabul (AZ), Khachmaz (AZ), Khizi (AZ), Khojaly (AZ), Khojavend (AZ), Imishli (AZ), Ismayilli (AZ), Kalbajar (AZ), Kangarli (AZ), Kurdamir (AZ), Kakh (AZ), Gazakh (AZ), Gabala (AZ), Gobustan (AZ), Guba (AZ), Gubadli (AZ), Gusar (AZ), Lachin (AZ), Lerik (AZ), Masalli (AZ), Neftchala (AZ), Oghuz (AZ), Ordubad (AZ), Saatli (AZ), Sabirabad (AZ), Salyan (AZ), Samukh (AZ), Sadarak (AZ), Siyazan (AZ), Shabran (AZ), Shahbuz (AZ), Shamakhi (AZ), Shamkir (AZ), Sharur (AZ), Shusha (AZ), Tartar (AZ), Tovuz (AZ), Ujar (AZ), Yardimli (AZ), Zagatala (AZ), Zangilan (AZ), Zardab (AZ), Rasht (IR), Sari (IR), Gorgan (IR), Bandar-e Anzali (IR), Makhachkala (RU), Derbent (RU), Yerevan (AM), Gyumri (AM), Erzurum (TR), Van (TR), Malatya (TR), Tbilisi (GE), Batumi (GE), Kutaisi (GE), Atyrau (KZ), Aktau (KZ), Oral (KZ), Turkmenbashi (TM), Balkanabat (TM).
* **Data Scope:** 5+ years of historical daily data and 7-day real-time forecasts.
* **Key Variables:** Temperature (Max/Min/Mean/Apparent), Humidity, Solar Radiation, Wind Speed, Pressure, and Weathercode.

---

## 3. Features Table

### Weather Features (Open-Meteo — Daily)

| Source     | Feature Name              | Unit     | Aggregation | Description                    |
| ---------- | ------------------------- | -------- | ----------- | ------------------------------ |
| Open-Meteo | temperature_2m_max        | °C       | daily max   | Maximum daily air temperature  |
| Open-Meteo | temperature_2m_min        | °C       | daily min   | Minimum daily air temperature  |
| Open-Meteo | temperature_2m_mean       | °C       | daily mean  | Average daily temperature      |
| Open-Meteo | apparent_temperature_max  | °C       | daily max   | Feels-like maximum temperature |
| Open-Meteo | precipitation_sum         | mm       | daily sum   | Total precipitation            |
| Open-Meteo | rain_sum                  | mm       | daily sum   | Total rainfall                 |
| Open-Meteo | snowfall_sum              | mm       | daily sum   | Total snowfall                 |
| Open-Meteo | windspeed_10m_max         | m/s      | daily max   | Max wind speed at 10m          |
| Open-Meteo | windgusts_10m_max         | m/s      | daily max   | Max wind gust speed            |
| Open-Meteo | relative_humidity_2m_mean | %        | daily mean  | Average humidity               |
| Open-Meteo | pressure_msl_mean         | hPa      | daily mean  | Mean sea level pressure        |
| Open-Meteo | cloudcover_mean           | %        | daily mean  | Average cloud cover            |
| Open-Meteo | shortwave_radiation_sum   | MJ/m²    | daily sum   | Solar radiation                |
| Open-Meteo | weathercode               | category | daily       | Weather condition code         |

---

### Derived Weather Features

| Source  | Feature Name              | Unit              | Aggregation | Description                             |
| ------- | ------------------------- | ----------------- | ----------- | --------------------------------------- |
| Derived | temp_range                | °C                | daily       | temperature_2m_max − temperature_2m_min |
| Derived | rain_ratio                | ratio             | daily       | rain_sum / precipitation_sum            |
| Derived | snow_ratio                | ratio             | daily       | snowfall_sum / precipitation_sum        |
| Derived | is_rainy                  | binary            | daily       | 1 if precipitation > 0                  |
| Derived | is_snowy                  | binary            | daily       | 1 if snowfall > 0                       |
| Derived | is_clear                  | binary            | daily       | 1 if weathercode == clear               |
| Derived | heat_stress_index         | index             | daily       | temperature × humidity interaction      |
| Derived | extreme_heat_impact       | index             | daily       | excess temperature above threshold      |
| Derived | wind_temp_interaction     | index             | daily       | windspeed × temperature                 |
| Derived | CDD (Cooling Degree Days) | °C                | daily       | cooling demand proxy                    |
| Derived | temp_anomaly              | °C                | daily       | deviation from city mean                |
| Derived | persistence_alert_level   | ordinal (-3 to 3) | daily       | duration of extreme events              |

---

### Temporal Features

| Source  | Feature Name | Unit     | Aggregation | Description                 |
| ------- | ------------ | -------- | ----------- | --------------------------- |
| Derived | day_of_year  | int      | daily       | Day index in year           |
| Derived | is_weekend   | binary   | daily       | Weekend indicator           |
| Derived | season       | category | daily       | Season label                |
| Derived | doy_sin      | cyclic   | daily       | Sin encoding of day_of_year |
| Derived | doy_cos      | cyclic   | daily       | Cos encoding of day_of_year |

---

### Target Variable

| Source  | Feature Name | Unit  | Aggregation | Description                              |
| ------- | ------------ | ----- | ----------- | ---------------------------------------- |
| Derived | impact_score | 0–100 | daily       | Weighted risk score for generator stress |

---

### Energy Prediction Inputs (Hourly)

| Source     | Feature Name        | Unit   | Aggregation | Description        |
| ---------- | ------------------- | ------ | ----------- | ------------------ |
| Open-Meteo | wind_speed_10m      | m/s    | hourly      | Wind speed         |
| Open-Meteo | temperature_2m      | °C     | hourly      | Air temperature    |
| Open-Meteo | shortwave_radiation | W/m²   | hourly      | Solar radiation    |
| Derived    | is_day              | binary | hourly      | Daylight indicator |

---

### Energy Output Features (Engineered)

| Source  | Feature Name          | Unit | Aggregation | Description                      |
| ------- | --------------------- | ---- | ----------- | -------------------------------- |
| Derived | Envision_wind_kWh     | kWh  | hourly      | Wind energy (Envision turbine)   |
| Derived | Fuhrlander_wind_kWh   | kWh  | hourly      | Wind energy (Fuhrlander turbine) |
| Derived | Jinko_Solar_solar_kWh | kWh  | hourly      | Solar energy (Jinko panels)      |
| Derived | Trina_Solar_solar_kWh | kWh  | hourly      | Solar energy (Trina panels)      |

---

### Lag & Rolling Features (Hourly Modeling)

| Source  | Feature Name                              | Unit | Aggregation  | Description                    |
| ------- | ----------------------------------------- | ---- | ------------ | ------------------------------ |
| Derived | wind_speed_10m_lag{1,2,3,6,12,24,48}      | m/s  | lag          | Previous wind values           |
| Derived | temperature_2m_lag{1,2,3,6,12,24,48}      | °C   | lag          | Previous temperature           |
| Derived | shortwave_radiation_lag{1,2,3,6,12,24,48} | W/m² | lag          | Previous solar radiation       |
| Derived | wind_speed_10m_roll{6,12,24}              | m/s  | rolling mean | Moving average wind            |
| Derived | temperature_2m_roll{6,12,24}              | °C   | rolling mean | Moving average temperature     |
| Derived | shortwave_radiation_roll{6,12,24}         | W/m² | rolling mean | Moving average solar radiation |

---

## 4. Success Criteria
* **Model Recall:** Achieving **>80% Recall** for "Critical" heat days to minimize missed alerts.
* **Reliability:** A robust pipeline using **DuckDB** for efficient large-scale data processing.
* **Impact:** Aligning model alerts with historical failure dates (backtesting).
* **Automation:** End-to-end flow from API call to risk classification in <10 seconds.

---

## Tech Stack
* **Data Engineering:** DuckDB, Apache Arrow, Python.
* **ML & Stats:** Scikit-learn, Scipy, XGBoost.
* **Visualization:** Seaborn, Plotly.

## Daily Activities

| Days | **Work Done** |
| --- | --- |
| 20.04.2026 | As a team, we conducted feature engineering, gathered information from official sources, and worked with NASA space weather data by collecting and analyzing it. We actively managed our workflow, ensuring tasks were organized and prepared for the following days. On Day 1, we completed Tasks 2 and 3 and began developing visualizations. In parallel, we set up the project repository along with all its related components, documented our progress by writing the Day 1 README, initiated the website development process, and established a structured Notion workspace to support collaboration and project management. |
| 21.04.2026 | As a team, we completed the Open-Meteo prediction process and prepared the data for integration into the website in JSON format. We reviewed the available variables and began exploring additional data such as wind and radiation, investigating whether these could also be sourced from Open-Meteo. In parallel, we evaluated different machine learning models, including XGBoost and Linear Regression, to determine the best fit for our dataset, concluding that XGBoost delivered significantly better performance. On Day 2, we collected five years of historical weather data along with future forecasts using an API. We cleaned and structured this data, ensuring it was properly stored for further use. Additionally, we developed the initial prototype of the website, demonstrated its functionality using mock data, and continued preparing the project for the next phase (Day 3). |
| 22.04.2026 | As a team, we researched the types of solar panels and wind turbines used in Azerbaijan to better align our project with real-world applications. We also improved the performance of our machine learning model, increasing the F1 score from 0.7 to 0.9 and reducing the RMSE, resulting in more accurate predictions. We completed Day 2 tasks and continued enhancing our visualizations. On the development side, we implemented a feature that allows cities to be added through the admin panel of the website, and we worked with DuckDB to efficiently manage and query our data. |
| 23.04.2026 | As a team, we conducted additional research on space weather data to further strengthen our dataset, although we were unable to identify a suitable new dataset. During this process, we also encountered technical issues, including a system error where the kernel repeatedly crashed, which required troubleshooting. We successfully completed all planned visualizations, ensuring that the data insights are clearly and effectively presented. With these tasks finalized, we completed all Day 3 objectives and brought this phase of the project to a close. |
| 24.04.2026 | As a team, we began preparing the modelling process for Azerbaijani green energy production, aligning our approach with the project’s overall objectives. We reviewed upcoming tasks, including those for Day 6 and Day 8, and made initial progress by completing approximately 25% of the planned work for Day 8. In parallel, we explored additional datasets to enhance our visualizations and improve data coverage. On the development side, we connected the website to DuckDB and researched effective ways to integrate visualizations into the platform, ensuring a smoother and more interactive user experience. |
| 27.04.2026 |  |
| 28.04.2026 |  |
| 29.04.2026 |  |
| 30.04.2026 |  |

---
