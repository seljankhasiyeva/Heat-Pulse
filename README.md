# Heat-Pulse
### Team sPySci (Spicy) | Weather Intelligence Pipeline


## 1. Problem Statement
**Objective:** How can we predict "Extreme Heat Events" (EHE) to prevent power generator burn-outs and optimize grid load management?

This project develops an end-to-end weather intelligence pipeline to identify days exceeding the 90th percentile of historical summer temperatures. Our goal is to provide a 10-day risk assessment tool for **Azerenerji** and industrial facility managers in Azerbaijan to prevent technical failures similar to those seen in 2018 and 2024.

---

## 2. Data Sources & Variables
We use the **Open-Meteo API** to bridge historical weather patterns with real-time forecasts.

* **Locations (10 Cities):** Baku, Ganja, Sumqayit, Nakhchivan, Lankaran, Tabriz (IR), Tbilisi (GE), Yerevan (AM), Makhachkala (RU), Turkmenbashi (TM).
* **Data Scope:** 5+ years of historical daily data and 7-day real-time forecasts.
* **Key Variables:** Temperature (Max/Min/Mean/Apparent), Humidity, Solar Radiation, Wind Speed, Pressure, and Weathercode.

---

## 3. Methodology (8-Day Sprint)

| Day | Date | Focus | Key Deliverables |
| :--- | :--- | :--- | :--- |
| 1 | 20 Apr | **Kick-Off** | Repo setup, city/variable selection, project plan. |
| 2 | 21 Apr | **Data Ingestion** | API ingestion module, full historical fetch (5+ years). |
| 3 | 22 Apr | **Database Design** | **DuckDB** schema setup and data loading functions. |
| 4 | 23 Apr | **Cleaning & FE** | Quality audit, outlier handling, and Feature Engineering. |
| 5 | 24 Apr | **Automation** | Orchestrator, incremental loading, and quality gates. |
| 6 | 27 Apr | **EDA** | Statistical distributions and cross-city correlations. |
| 7 | 28 Apr | **Stats & Selection** | Hypothesis testing and final feature selection. |
| 8 | 29 Apr | **Modeling** | Training 2+ models (XGBoost/RF) and evaluation. |
| — | 30 Apr | **Presentation** | Live pipeline demo and final project submission. |

---

## 4. Success Criteria
* **Model Recall:** Achieving **>80% Recall** for "Critical" heat days to minimize missed alerts.
* **Reliability:** A robust pipeline using **DuckDB** for efficient large-scale data processing.
* **Impact:** Aligning model alerts with historical failure dates (backtesting).
* **Automation:** End-to-end flow from API call to risk classification in <10 seconds.

---

## 🛠 Tech Stack
* **Data Engineering:** DuckDB, Apache Arrow, Python.
* **ML & Stats:** Scikit-learn, Scipy, XGBoost.
* **Visualization:** Seaborn, Plotly.

**Team name:** sPySci🌶️

**Team members:** Jabrail Atakishiyev, Firuddin Rzayev, Zarifa Musayeva, Seljan Khasiyeva

18.04.2026-https://us05web.zoom.us/j/82189687654?pwd=cIbCgnQV6JBB5xSXuhd56NaP2ViEHL.1

19.04.2026-https://meet.google.com/rnv-yukg-nyf

23.04.2026  10:00 -https://meet.google.com/rss-stmv-qxw

25.04.2026  20:00 -[meet.google.com/gqt-jjoy-vup](http://meet.google.com/gqt-jjoy-vup)

| Roles | Data Engineer | Data Scientist | Visualizator | Web Developer |
| --- | --- | --- | --- | --- |
| Days | **Jabrail Atakishiyev** | **Firuddin Rzayev** | **Zarifa Musayeva** | **Seljan Khasiyeva** |
| 1 | I did feature engineering, collected information on the topic from official sources, and took and analyzed NASA space weather data. | i was controlling processes and doing tasks for the next day | Day 1 – I wrote Tasks 2 and 3, and started working on the visualizations. | I set up the repository and all its related details, wrote the README for Day 1, started working on the website, and prepared the Notion workspace. |
| 2 | The prediction from Open-Meteo is finished. The data is ready. We can upload it to the website in JSON format. Then I will look at the data for wind and radiation, it would be great if Open Meteo has it, I will investigate. | i was looking which model(XGBoost, Linear Regression and so on) is better for our dataset. XGboost was much better | day 2 - I collect five years of weather data and future forecasts using an API. I clean this data and save it | I prepared the initial website prototype and demonstrated how it looks using mock data, and also made preparations for Day 3. |
| 3 | I researched what solar panels and wind turbines are used in Azerbaijan. | i improved the F1 scores of our model from 0.7 to 0.9 and i made RMSE lower | I've completed Day 2 and continued working on visualizations. | I implemented the feature that allows adding cities from the admin side of the website and worked with DuckDB. |
| 4 | Data searching for the space weather. | i was looking for new dataset but didn’t find one. and my pc return new error, that kernel appears to die | I've finished all the visualizations. | finalized day 3 |
| 5 | Preporations of the modelling process for Azerbaijani green energy production | i checked the task for day 8 and tried to do something. 25% is done for today. | I checked the tasks for Day 6. I also looked for an additional dataset for visualizations. | I connected the website to DuckDB and researched how to integrate visualizations into the website. |
| 6 |  |  |  |  |
| 7 |  |  |  |  |
| 8 |  |  |  |  |

Dediyiniz impact score formulunu əlavə edərsiz, əgər githubda varsa o da kifayət edir

---
