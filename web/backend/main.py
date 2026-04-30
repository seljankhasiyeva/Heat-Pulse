import sys
import os
import duckdb
import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DB_PATH      = os.path.join(BASE_DIR, "data", "weather.duckdb")
WEATHER_JSON = os.path.join(CURRENT_DIR, "data_web", "weather_predictions.json")
ENERGY_JSON  = os.path.join(CURRENT_DIR, "data_web", "energy_forecast_30days.json")


@app.get("/api/weather")
def get_weather():
    if not os.path.exists(DB_PATH):
        return {"status": "error", "message": f"Database not found: {DB_PATH}"}
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df   = conn.execute("""
            SELECT city,
                   temperature_2m_max AS temp,
                   latitude            AS lat,
                   longitude           AS lon
            FROM   raw.raw_forecast
            QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY time DESC) = 1
        """).df()
        conn.close()
        return {"status": "success", "count": len(df), "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/details/{city_name}")
async def get_detailed_analytics(city_name: str):
    try:
        with open(WEATHER_JSON, "r", encoding="utf-8") as f:
            weather_data = json.load(f)
        with open(ENERGY_JSON, "r", encoding="utf-8") as f:
            energy_data = json.load(f)

        target_raw = city_name.strip()
        target = target_raw.capitalize()
        target_lc = target_raw.lower()

        # Resolve JSON keys case-insensitively (DB/API cities are lowercase, JSON keys are usually Title case)
        weather_cities = weather_data.get("cities", {})
        energy_locations = energy_data.get("locations", {})
        weather_key = next((k for k in weather_cities.keys() if k.lower() == target_lc), target)
        energy_key = next((k for k in energy_locations.keys() if k.lower() == target_lc), target)

        # 1. HAVA: { "cities": { "Baku": [{date, temperature_2m_max, ...}] } }
        city_weather_list = weather_cities.get(weather_key, [])
        if not city_weather_list:
            return {"status": "error", "message": f"City '{target}' not found in weather data"}

        forecast_30 = [
            {
                "date":      w.get("date"),
                "temp_max":  w.get("temperature_2m_max"),
                "temp_min":  w.get("temperature_2m_min"),
                "humidity":  w.get("relative_humidity_2m_mean"),
                "condition": w.get("weather_category"),
                "impact":    w.get("impact_score"),
                "alert":     w.get("persistence_alert_level"),
            }
            for w in city_weather_list
        ]
        latest = forecast_30[0] if forecast_30 else {}

        # 2. ENERJİ saatliq: { "locations": { "Baku": [{year,month,day,hour,...}] } }
        hourly_rows = energy_locations.get(energy_key, [])

        # Model metrics
        metrics = {}
        for m in energy_data.get("model_metrics", []):
            t = m.get("target", "")
            if t == "temperature_2m":
                metrics["temp_r2"] = m.get("test_r2", 0)
                metrics["temp_rmse"] = m.get("test_rmse", 0)
            elif t == "wind_speed_10m":
                metrics["wind_r2"] = m.get("test_r2", 0)
                metrics["wind_rmse"] = m.get("test_rmse", 0)
            elif t == "shortwave_radiation":
                metrics["solar_r2"] = m.get("test_r2", 0)

        # Saatligi gunluk topla
        from collections import defaultdict
        daily_energy = defaultdict(lambda: {"wind": 0.0, "solar": 0.0})
        for row in hourly_rows:
            yr, mo, dy = row.get("year"), row.get("month"), row.get("day")
            if yr is None or mo is None or dy is None:
                continue
            date_str = f"{int(yr):04d}-{int(mo):02d}-{int(dy):02d}"
            daily_energy[date_str]["wind"]  += float(row.get("Envision_wind_kWh",   0) or 0)
            daily_energy[date_str]["wind"]  += float(row.get("Fuhrlander_wind_kWh", 0) or 0)
            daily_energy[date_str]["solar"] += float(row.get("Jinko_Solar_kWh",     0) or 0)
            daily_energy[date_str]["solar"] += float(row.get("Trina_Solar_kWh",     0) or 0)

        energy_30 = [
            {"date": ds, "wind": round(v["wind"], 2), "solar": round(v["solar"], 2),
             "total": round(v["wind"] + v["solar"], 2)}
            for ds, v in sorted(daily_energy.items())
        ]
        total_wind  = round(sum(e["wind"]  for e in energy_30), 2)
        total_solar = round(sum(e["solar"] for e in energy_30), 2)

        # 3. DuckDB tarixi data
        hist_temps, hist_full = [], []
        if os.path.exists(DB_PATH):
            try:
                conn = duckdb.connect(DB_PATH, read_only=True)
                hist_temps = [r[0] for r in conn.execute("""
                    WITH merged AS (
                        SELECT time, temperature_2m_max, 1 AS src
                        FROM raw.raw_historical
                        WHERE lower(city)=lower(?) AND temperature_2m_max IS NOT NULL
                        UNION ALL
                        SELECT time, temperature_2m_max, 2 AS src
                        FROM raw.raw_forecast
                        WHERE lower(city)=lower(?) AND temperature_2m_max IS NOT NULL
                    ),
                    dedup AS (
                        SELECT time, temperature_2m_max,
                               ROW_NUMBER() OVER (PARTITION BY time ORDER BY src) AS rn
                        FROM merged
                    )
                    SELECT temperature_2m_max
                    FROM dedup
                    WHERE rn=1
                    ORDER BY time
                """, [target_raw, target_raw]).fetchall()]

                full_rows = conn.execute("""
                    WITH merged AS (
                        SELECT time,
                               temperature_2m_max,
                               temperature_2m_min,
                               temperature_2m_mean,
                               relative_humidity_2m_mean,
                               wind_speed_10m_max,
                               shortwave_radiation_sum,
                               1 AS src
                        FROM raw.raw_historical
                        WHERE lower(city)=lower(?) AND temperature_2m_max IS NOT NULL
                        UNION ALL
                        SELECT time,
                               temperature_2m_max,
                               temperature_2m_min,
                               temperature_2m_mean,
                               relative_humidity_2m_mean,
                               wind_speed_10m_max,
                               shortwave_radiation_sum,
                               2 AS src
                        FROM raw.raw_forecast
                        WHERE lower(city)=lower(?) AND temperature_2m_max IS NOT NULL
                    ),
                    dedup AS (
                        SELECT *,
                               ROW_NUMBER() OVER (PARTITION BY time ORDER BY src) AS rn
                        FROM merged
                    )
                    SELECT CAST(time AS VARCHAR),
                           temperature_2m_max,
                           temperature_2m_min,
                           COALESCE(temperature_2m_mean,
                               (temperature_2m_max+temperature_2m_min)/2.0),
                           relative_humidity_2m_mean,
                           wind_speed_10m_max,
                           shortwave_radiation_sum
                    FROM dedup
                    WHERE rn=1
                    ORDER BY time
                """, [target_raw, target_raw]).fetchall()
                cols = ["date","temp_max","temp_min","temp_mean","humidity","wind","solar"]
                hist_full = [{cols[i]: (float(v) if isinstance(v,(int,float)) and v is not None else str(v) if i==0 else None) for i,v in enumerate(row)} for row in full_rows]
                conn.close()
            except Exception as db_err:
                print(f"DuckDB error: {db_err}")

        if not hist_temps:
            hist_temps = [d["temp_max"] for d in forecast_30 if d["temp_max"] is not None]
        if not hist_full:
            hist_full = [{"date":d["date"],"temp_max":d["temp_max"],"temp_min":d["temp_min"],
                "temp_mean": round((d["temp_max"]+d["temp_min"])/2,1) if d["temp_max"] and d["temp_min"] else None,
                "humidity":d["humidity"],"wind":None,"solar":None} for d in forecast_30]

        return {
            "status": "success", "city": target,
            "weather": latest,
            "forecast": forecast_30,
            "energy_forecast": energy_30,
            "energy": {"wind": total_wind, "solar": total_solar, "total": round(total_wind+total_solar,2)},
            "accuracy_metrics": metrics,
            "hist_temps": hist_temps,
            "hist_full": hist_full,
        }

    except Exception as e:
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}