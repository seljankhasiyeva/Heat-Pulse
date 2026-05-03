import os, json
from collections import defaultdict
from datetime import datetime

import duckdb

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DB_PATH = os.path.join(BASE_DIR, "data", "weather.duckdb")
HIST_WINDOW_YEARS = 6


def _trim_hist_last_n_years(hist_full, n_years=HIST_WINDOW_YEARS):
    if not hist_full or n_years < 1:
        return hist_full or []

    def _parse_d(s):
        try:
            return datetime.fromisoformat(str(s).replace("Z", "+00:00")[:10])
        except Exception:
            return None

    times = [_parse_d(r["date"]) for r in hist_full]
    times = [t for t in times if t]
    if not times:
        return hist_full
    end = max(times)
    start = datetime(end.year - (n_years - 1), 1, 1)
    return [r for r in hist_full if (d := _parse_d(r["date"])) and d >= start]


def _hist_from_duckdb(city_raw: str):
    """main.py ilə eyni məntiqlə tarix + proqnozu birləşdirir; DB yoxdursa None."""
    if not os.path.exists(DB_PATH):
        return None
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        raw_cols = [
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='raw' AND table_name='raw_historical'"
            ).fetchall()
        ]
        has_humidity = "relative_humidity_2m_mean" in raw_cols
        humidity_expr = "relative_humidity_2m_mean" if has_humidity else "NULL"

        full_rows = conn.execute(
            f"""
            WITH merged AS (
                SELECT time,
                       temperature_2m_max,
                       temperature_2m_min,
                       temperature_2m_mean,
                       {humidity_expr} AS relative_humidity_2m_mean,
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
                       {humidity_expr} AS relative_humidity_2m_mean,
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
            FROM dedup WHERE rn=1
            ORDER BY time
            """,
            [city_raw, city_raw],
        ).fetchall()
        conn.close()

        cols = ["date", "temp_max", "temp_min", "temp_mean", "humidity", "wind", "solar"]
        hist_full = [
            {
                cols[i]: (
                    float(v)
                    if isinstance(v, (int, float)) and v is not None
                    else (str(v) if i == 0 else None)
                )
                for i, v in enumerate(row)
            }
            for row in full_rows
        ]
        if not hist_full:
            return None
        hist_full = _trim_hist_last_n_years(hist_full)
        hist_temps = [r["temp_max"] for r in hist_full if r.get("temp_max") is not None]
        return hist_full, hist_temps
    except Exception as e:
        print(f"[DuckDB] {city_raw}: {e}")
        return None

WEATHER_JSON = os.path.join(SCRIPT_DIR, "data_web", "weather_predictions.json")
ENERGY_JSON  = os.path.join(SCRIPT_DIR, "data_web", "energy_forecast_30days.json")
COORDS_JSON  = os.path.join(SCRIPT_DIR, "data_web", "city_coords.json")
OUTPUT_JSON  = os.path.join(SCRIPT_DIR, "data_web", "static_cities.json")

with open(WEATHER_JSON, "r", encoding="utf-8") as f:
    weather_data = json.load(f)
with open(ENERGY_JSON, "r", encoding="utf-8") as f:
    energy_data = json.load(f)
with open(COORDS_JSON, "r", encoding="utf-8") as f:
    city_coords = json.load(f)

weather_cities   = weather_data.get("cities", {})
energy_locations = energy_data.get("locations", {})

metrics_global = {}
for m in energy_data.get("model_metrics", []):
    t = m.get("target", "")
    if   t == "temperature_2m":      metrics_global["temp_r2"]  = m.get("test_r2", 0); metrics_global["temp_rmse"]  = m.get("test_rmse", 0)
    elif t == "wind_speed_10m":      metrics_global["wind_r2"]  = m.get("test_r2", 0); metrics_global["wind_rmse"]  = m.get("test_rmse", 0)
    elif t == "shortwave_radiation": metrics_global["solar_r2"] = m.get("test_r2", 0)

result = []

for city_name, city_weather_list in weather_cities.items():
    city_lc = city_name.lower()

    coords = next((v for k, v in city_coords.items() if k.lower() == city_lc), None)
    if not coords:
        print(f"[SKIP] {city_name} — koordinat yoxdur")
        continue

    lat = coords.get("lat")
    lon = coords.get("lon")

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

    energy_key  = next((k for k in energy_locations if k.lower() == city_lc), city_name)
    hourly_rows = energy_locations.get(energy_key, [])
    daily_energy = defaultdict(lambda: {"wind": 0.0, "solar": 0.0})
    for row in hourly_rows:
        yr, mo, dy = row.get("year"), row.get("month"), row.get("day")
        if yr is None or mo is None or dy is None:
            continue
        ds = f"{int(yr):04d}-{int(mo):02d}-{int(dy):02d}"
        daily_energy[ds]["wind"]  += float(row.get("Envision_wind_kWh",   0) or 0)
        daily_energy[ds]["wind"]  += float(row.get("Fuhrlander_wind_kWh", 0) or 0)
        daily_energy[ds]["solar"] += float(row.get("Jinko_Solar_kWh",     0) or 0)
        daily_energy[ds]["solar"] += float(row.get("Trina_Solar_kWh",     0) or 0)

    energy_30 = [
        {"date": ds, "wind": round(v["wind"], 2), "solar": round(v["solar"], 2),
         "total": round(v["wind"] + v["solar"], 2)}
        for ds, v in sorted(daily_energy.items())
    ]
    total_wind  = round(sum(e["wind"]  for e in energy_30), 2)
    total_solar = round(sum(e["solar"] for e in energy_30), 2)

    hist_full = [
        {
            "date":      d["date"],
            "temp_max":  d["temp_max"],
            "temp_min":  d["temp_min"],
            "temp_mean": round((d["temp_max"] + d["temp_min"]) / 2, 1)
                         if d["temp_max"] is not None and d["temp_min"] is not None else None,
            "humidity":  d["humidity"],
            "wind":      None,
            "solar":     None,
        }
        for d in forecast_30
    ]
    hist_temps = [r["temp_max"] for r in hist_full if r["temp_max"] is not None]

    db_hist = _hist_from_duckdb(city_name.strip())
    if db_hist:
        hist_full, hist_temps = db_hist

    result.append({
        "city":             city_name,
        "lat":              lat,
        "lon":              lon,
        "weather":          {
            "temp_max":  latest.get("temp_max"),
            "temp_min":  latest.get("temp_min"),
            "humidity":  latest.get("humidity"),
            "condition": latest.get("condition"),
            "impact":    latest.get("impact"),
            "alert":     latest.get("alert", 0),
            "date":      latest.get("date"),
        },
        "forecast":         forecast_30,
        "energy_forecast":  energy_30,
        "energy":           {"wind": total_wind, "solar": total_solar,
                             "total": round(total_wind + total_solar, 2)},
        "accuracy_metrics": metrics_global,
        "hist_temps":       hist_temps,
        "hist_full":        hist_full,
    })

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False)

print(f"OK {len(result)} cities -> {OUTPUT_JSON}")