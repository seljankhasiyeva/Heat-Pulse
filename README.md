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

---
**Team:** sPySci (Spicy) 🌶️  