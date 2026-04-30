"""
src/ingestion.py
Weather data ingestion from Open-Meteo API.

Column names EXACTLY match cleaning.py expectations:
    time, city, latitude, longitude,
    temperature_2m_max, temperature_2m_min, temperature_2m_mean,
    precipitation_sum, rain_sum, snowfall_sum,
    wind_speed_10m_max, wind_gusts_10m_max,
    pressure_msl_mean, shortwave_radiation_sum,
    apparent_temperature_max, weather_code
"""

import requests
import pandas as pd
import logging
import time as _time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── API endpoints ─────────────────────────────────────────────────────────────
HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL   = "https://api.open-meteo.com/v1/forecast"

# ── API variable names (match cleaning.py column names exactly) ───────────────
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "pressure_msl_mean",
    "shortwave_radiation_sum",
    "apparent_temperature_max",
    "weather_code",
]

# ── Paths ─────────────────────────────────────────────────────────────────────
RAW_DIR      = Path("data/raw")
HIST_CSV     = RAW_DIR / "all_94_cities_historical_combined.csv"
FORE_CSV     = RAW_DIR / "all_94_cities_forecast_combined.csv"

DEFAULT_START_DATE = "2020-01-01"


# ─────────────────────────────────────────────────────────────────────────────
# City registry — loaded from the existing CSV
# ─────────────────────────────────────────────────────────────────────────────

def load_cities_from_csv(csv_path: Path = HIST_CSV) -> dict:
    """
    Read unique (city, latitude, longitude) from the raw historical CSV.
    Returns {city_name: {"latitude": ..., "longitude": ...}}.
    Falls back to a small set of key Azerbaijani cities if file not found.
    """
    if csv_path.exists():
        try:
            cols_needed = ["city", "latitude", "longitude"]
            df = pd.read_csv(csv_path, usecols=cols_needed)
            cities = (
                df.drop_duplicates("city")
                  .set_index("city")[["latitude", "longitude"]]
                  .to_dict("index")
            )
            logger.info(f"Loaded {len(cities)} cities from {csv_path}")
            return cities
        except Exception as e:
            logger.warning(f"Could not read cities from CSV: {e}")

    logger.warning("CSV not found — using fallback city list.")
    return {
        "Baku":        {"latitude": 40.4093, "longitude": 49.8671},
        "Ganja":       {"latitude": 40.6828, "longitude": 46.3606},
        "Sumqayit":    {"latitude": 40.5897, "longitude": 49.6686},
        "Mingachevir": {"latitude": 40.7706, "longitude": 47.0495},
        "Nakhchivan":  {"latitude": 39.2090, "longitude": 45.4122},
        "Lankaran":    {"latitude": 38.7529, "longitude": 48.8511},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Core API fetch
# ─────────────────────────────────────────────────────────────────────────────

def fetch_city_weather(
    city: str,
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    retries: int = 3,
    backoff: float = 2.0,
    is_forecast: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Fetch daily weather for one city from Open-Meteo.
    Returns a DataFrame with columns: time, city, latitude, longitude, <vars>.
    Returns None on failure.
    """
    url = FORECAST_URL if is_forecast else HISTORICAL_URL
    params = {
        "latitude":  latitude,
        "longitude": longitude,
        "daily":     ",".join(DAILY_VARIABLES),
        "timezone":  "UTC",
    }
    if not is_forecast:
        params["start_date"] = start_date
        params["end_date"]   = end_date

    for attempt in range(1, retries + 1):
        try:
            label = "7-day forecast" if is_forecast else f"{start_date} → {end_date}"
            logger.info(f"[{city}] Fetching {label} (attempt {attempt})")
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            if "daily" not in data:
                logger.warning(f"[{city}] No 'daily' key in response.")
                return None

            df = pd.DataFrame(data["daily"])
            # API returns date column as 'time' — keep it as-is to match cleaning.py
            df["city"]      = city
            df["latitude"]  = latitude
            df["longitude"] = longitude

            # Ensure all expected columns present
            for col in DAILY_VARIABLES:
                if col not in df.columns:
                    df[col] = None

            col_order = ["time", "city", "latitude", "longitude"] + DAILY_VARIABLES
            df = df[[c for c in col_order if c in df.columns]]

            logger.info(f"[{city}] Fetched {len(df)} rows.")
            return df

        except requests.exceptions.RequestException as e:
            logger.warning(f"[{city}] Attempt {attempt} failed: {e}")
            if attempt < retries:
                _time.sleep(backoff * attempt)

    logger.error(f"[{city}] All {retries} attempts failed.")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Full ingestion (all cities, all history)
# ─────────────────────────────────────────────────────────────────────────────

def ingest_all_cities_full(
    cities: dict = None,
    start_date: str = DEFAULT_START_DATE,
    end_date:   str = None,
) -> pd.DataFrame:
    """Fetch complete historical data for every city. Returns combined DataFrame."""
    if cities is None:
        cities = load_cities_from_csv()
    if end_date is None:
        end_date = str(date.today())

    frames = []
    for city, coords in cities.items():
        df = fetch_city_weather(
            city, coords["latitude"], coords["longitude"],
            start_date, end_date,
        )
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        logger.error("No data fetched for any city.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"Full ingest complete: {len(combined):,} rows from {len(frames)} cities.")
    return combined


# ─────────────────────────────────────────────────────────────────────────────
# Incremental ingestion (only new days)
# ─────────────────────────────────────────────────────────────────────────────

def ingest_incremental(
    latest_dates: dict,
    cities: dict = None,
    end_date: str = None,
) -> pd.DataFrame:
    """
    Fetch only data after the latest date already stored for each city.

    Parameters
    ----------
    latest_dates : {city_name: last_time_in_db}
                   Values can be datetime, date, or "YYYY-MM-DD" string.
    cities       : {city_name: {latitude, longitude}}
    end_date     : upper bound (defaults to today)
    """
    if cities is None:
        cities = load_cities_from_csv()
    if end_date is None:
        end_date = str(date.today())

    frames = []
    for city, coords in cities.items():
        last = latest_dates.get(city)

        if last is not None:
            if hasattr(last, "date"):           # datetime → date
                last = last.date()
            elif isinstance(last, str):
                last = pd.to_datetime(last).date()
            start = str(last + timedelta(days=1))
        else:
            start = DEFAULT_START_DATE

        if start > end_date:
            logger.info(f"[{city}] Already up to date (last={last}). Skipping.")
            continue

        df = fetch_city_weather(
            city, coords["latitude"], coords["longitude"],
            start, end_date,
        )
        if df is not None and not df.empty:
            frames.append(df)
            logger.info(f"[{city}] {len(df)} new rows ({start} → {end_date}).")
        else:
            logger.info(f"[{city}] No new rows available.")

    if not frames:
        logger.info("Incremental ingest: no new data for any city.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"Incremental ingest: {len(combined):,} new rows total.")
    return combined


# ─────────────────────────────────────────────────────────────────────────────
# Forecast ingestion (7-day)
# ─────────────────────────────────────────────────────────────────────────────

def ingest_all_forecasts(cities: dict = None) -> pd.DataFrame:
    """Fetch 7-day forecast for all cities."""
    if cities is None:
        cities = load_cities_from_csv()

    frames = []
    for city, coords in cities.items():
        df = fetch_city_weather(
            city, coords["latitude"], coords["longitude"],
            start_date="", end_date="",
            is_forecast=True,
        )
        if df is not None and not df.empty:
            frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# Load from local CSV (offline / first-run mode)
# ─────────────────────────────────────────────────────────────────────────────

def load_historical_from_csv(path: Path = HIST_CSV) -> pd.DataFrame:
    """
    Load the pre-downloaded historical CSV.
    'time' column is standardised to YYYY-MM-DD string format.
    """
    df = pd.read_csv(path)
    df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%d")
    logger.info(f"Loaded {len(df):,} historical rows from {path}")
    return df


def load_forecast_from_csv(path: Path = FORE_CSV) -> pd.DataFrame:
    """Load the pre-downloaded forecast CSV."""
    df = pd.read_csv(path)
    df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%d")
    logger.info(f"Loaded {len(df):,} forecast rows from {path}")
    return df