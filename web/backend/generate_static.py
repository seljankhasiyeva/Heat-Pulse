import os, json
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

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

print(f"✅ {len(result)} şəhər → {OUTPUT_JSON}")