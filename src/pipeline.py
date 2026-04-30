"""
src/pipeline.py
Task 1 & 2 — Pipeline Orchestrator with Full and Incremental modes

Usage
-----
# Full historical re-ingest (drops and recreates raw tables)
python src/pipeline.py --mode full

# Incremental: only fetch new days since last record in DB
python src/pipeline.py --mode incremental

# Incremental for a specific city subset
python src/pipeline.py --mode incremental --cities Baku Ganja

Optional overrides
------------------
  --db-path   PATH     Path to weather.duckdb   (default: data/weather.duckdb)
  --data-dir  PATH     Path to data/ folder     (default: data)
  --log-dir   PATH     Path for log files       (default: logs)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

# ── Make sure src/ is on the path when running as a script ──────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd

# ── Project modules ──────────────────────────────────────────────────────────
try:
    import duckdb
except ImportError:
    sys.exit("❌  duckdb not installed.  Run:  pip install duckdb")

try:
    from cleaning import clean_raw_to_staging
except ImportError:
    from src.cleaning import clean_raw_to_staging   # type: ignore

try:
    from features import populate_analytics_tables
except ImportError:
    from src.features import populate_analytics_tables   # type: ignore

try:
    from quality_checks import run_all_checks, print_quality_report
except ImportError:
    from src.quality_checks import run_all_checks, print_quality_report   # type: ignore

# ─────────────────────────────────────────────────────────────────────────────
# Defaults — adjust these to match your project layout
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_DB_PATH   = Path("data") / "weather.duckdb"
DEFAULT_DATA_DIR  = Path("data")
DEFAULT_LOG_DIR   = Path("logs")
DEFAULT_START     = "2020-01-01"
DEFAULT_END       = str(date.today())

# ── Cities configuration (mirrors your config.py / config.yaml) ──────────────
CITIES_CONFIG = [
    {"name": "Baku",        "latitude": 40.3754, "longitude": 49.8327},
    {"name": "Ganja",       "latitude": 40.6850, "longitude": 46.3500},
    {"name": "Nakhchivan",  "latitude": 39.2092, "longitude": 45.4122},
    {"name": "Sumqayit",    "latitude": 40.5800, "longitude": 49.6300},
    {"name": "Lankaran",    "latitude": 38.7540, "longitude": 48.8511},
    {"name": "Mingachevir", "latitude": 40.7667, "longitude": 47.0500},
    {"name": "Naftalan",    "latitude": 40.5072, "longitude": 46.8242},
    {"name": "Khankendi",   "latitude": 39.8219, "longitude": 46.7725},
    {"name": "Sheki",       "latitude": 41.1923, "longitude": 47.1705},
    {"name": "Shirvan",     "latitude": 39.9323, "longitude": 48.9203},
    {"name": "Yevlakh",     "latitude": 40.6133, "longitude": 47.1492},
    {"name": "Khirdalan",   "latitude": 40.4500, "longitude": 49.7333},
    {"name": "Agjabadi",    "latitude": 40.0542, "longitude": 47.4578},
    {"name": "Agdam",       "latitude": 39.9917, "longitude": 46.9250},
    {"name": "Agdash",      "latitude": 40.6453, "longitude": 47.4686},
    {"name": "Agdere",      "latitude": 40.2033, "longitude": 46.8094},
    {"name": "Agstafa",     "latitude": 41.1208, "longitude": 45.4517},
    {"name": "Agsu",        "latitude": 40.5739, "longitude": 48.4053},
    {"name": "Astara",      "latitude": 38.4553, "longitude": 48.8775},
    {"name": "Babek",       "latitude": 39.1833, "longitude": 45.4000},
    {"name": "Balakan",     "latitude": 41.7228, "longitude": 46.4056},
    {"name": "Beylagan",    "latitude": 39.7736, "longitude": 47.6069},
    {"name": "Barda",       "latitude": 40.3753, "longitude": 47.1336},
    {"name": "Bilesuvar",   "latitude": 39.4533, "longitude": 48.5536},
    {"name": "Jabrayil",    "latitude": 39.3969, "longitude": 47.0297},
    {"name": "Jalilabad",   "latitude": 39.2039, "longitude": 48.4836},
    {"name": "Julfa",       "latitude": 38.9606, "longitude": 45.6214},
    {"name": "Dashkasan",   "latitude": 40.5167, "longitude": 46.0833},
    {"name": "Fuzuli",      "latitude": 39.5967, "longitude": 47.2394},
    {"name": "Gadabay",     "latitude": 40.5706, "longitude": 45.8114},
    {"name": "Goranboy",    "latitude": 40.6128, "longitude": 46.7861},
    {"name": "Goychay",     "latitude": 40.6556, "longitude": 47.7375},
    {"name": "Goygol",      "latitude": 40.5914, "longitude": 46.3267},
    {"name": "Hajigabul",   "latitude": 40.0356, "longitude": 48.9328},
    {"name": "Khachmaz",    "latitude": 41.4633, "longitude": 48.8025},
    {"name": "Khizi",       "latitude": 40.8525, "longitude": 49.0717},
    {"name": "Khojaly",     "latitude": 39.8972, "longitude": 46.7900},
    {"name": "Khojavend",   "latitude": 39.7833, "longitude": 47.1000},
    {"name": "Imishli",     "latitude": 39.8667, "longitude": 48.0667},
    {"name": "Ismayilli",   "latitude": 40.7833, "longitude": 48.1500},
    {"name": "Kalbajar",    "latitude": 40.1014, "longitude": 46.0333},
    {"name": "Kangarli",    "latitude": 39.3167, "longitude": 45.1667},
    {"name": "Kurdamir",    "latitude": 40.3325, "longitude": 48.1578},
    {"name": "Kakh",        "latitude": 41.4172, "longitude": 46.9258},
    {"name": "Gazakh",      "latitude": 41.0928, "longitude": 45.3614},
    {"name": "Gabala",      "latitude": 40.9933, "longitude": 47.8467},
    {"name": "Gobustan",    "latitude": 40.5333, "longitude": 48.9333},
    {"name": "Guba",        "latitude": 41.3625, "longitude": 48.5111},
    {"name": "Gubadli",     "latitude": 39.3242, "longitude": 46.5753},
    {"name": "Gusar",       "latitude": 41.4286, "longitude": 48.4300},
    {"name": "Lachin",      "latitude": 39.6397, "longitude": 46.5458},
    {"name": "Lerik",       "latitude": 38.7736, "longitude": 48.4150},
    {"name": "Masalli",     "latitude": 39.0306, "longitude": 48.6675},
    {"name": "Neftchala",   "latitude": 39.3667, "longitude": 49.2500},
    {"name": "Oghuz",       "latitude": 41.0667, "longitude": 47.4500},
    {"name": "Ordubad",     "latitude": 38.9039, "longitude": 46.0236},
    {"name": "Saatli",      "latitude": 39.9333, "longitude": 48.3667},
    {"name": "Sabirabad",   "latitude": 39.9958, "longitude": 48.4775},
    {"name": "Salyan",      "latitude": 39.5936, "longitude": 48.9750},
    {"name": "Samukh",      "latitude": 40.7833, "longitude": 46.4500},
    {"name": "Sadarak",     "latitude": 39.6975, "longitude": 44.8142},
    {"name": "Siyazan",     "latitude": 41.0736, "longitude": 49.1133},
    {"name": "Shabran",     "latitude": 41.2167, "longitude": 48.9833},
    {"name": "Shahbuz",     "latitude": 39.4167, "longitude": 45.5667},
    {"name": "Shamakhi",    "latitude": 40.6306, "longitude": 48.6347},
    {"name": "Shamkir",     "latitude": 40.8333, "longitude": 46.0167},
    {"name": "Sharur",      "latitude": 39.5542, "longitude": 44.9825},
    {"name": "Shusha",      "latitude": 39.7611, "longitude": 46.7497},
    {"name": "Tartar",      "latitude": 40.3444, "longitude": 46.9358},
    {"name": "Tovuz",       "latitude": 40.9917, "longitude": 45.6250},
    {"name": "Ujar",        "latitude": 40.5056, "longitude": 47.6533},
    {"name": "Yardimli",    "latitude": 38.8667, "longitude": 48.2333},
    {"name": "Zagatala",    "latitude": 41.6333, "longitude": 46.6333},
    {"name": "Zangilan",    "latitude": 39.0769, "longitude": 46.6636},
    {"name": "Zardab",      "latitude": 40.2167, "longitude": 47.7167},
    {"name": "Rasht",       "latitude": 37.2808, "longitude": 49.5832},
    {"name": "Sari",        "latitude": 36.5633, "longitude": 53.0601},
    {"name": "Gorgan",      "latitude": 36.8416, "longitude": 54.4436},
    {"name": "Bandar-e Anzali", "latitude": 37.4716, "longitude": 49.4622},
    {"name": "Makhachkala", "latitude": 42.9764, "longitude": 47.5024},
    {"name": "Derbent",     "latitude": 42.0678, "longitude": 48.2899},
    {"name": "Yerevan",     "latitude": 40.1811, "longitude": 44.5136},
    {"name": "Gyumri",      "latitude": 40.7942, "longitude": 43.8453},
    {"name": "Erzurum",     "latitude": 39.9086, "longitude": 41.2769},
    {"name": "Van",         "latitude": 38.4946, "longitude": 43.3832},
    {"name": "Malatya",     "latitude": 38.3502, "longitude": 38.3167},
    {"name": "Tbilisi",     "latitude": 41.7151, "longitude": 44.8271},
    {"name": "Batumi",      "latitude": 41.6423, "longitude": 41.6339},
    {"name": "Kutaisi",     "latitude": 42.2679, "longitude": 42.6946},
    {"name": "Atyrau",      "latitude": 47.1167, "longitude": 51.8833},
    {"name": "Aktau",       "latitude": 43.6481, "longitude": 51.1722},
    {"name": "Oral",        "latitude": 51.2333, "longitude": 51.3667},
    {"name": "Turkmenbashi", "latitude": 40.0222, "longitude": 52.9552},
    {"name": "Balkanabat",  "latitude": 39.5108, "longitude": 54.3671},
]

WEATHER_VARIABLES = [
    "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
    "precipitation_sum", "rain_sum", "snowfall_sum",
    "wind_speed_10m_max", "wind_gusts_10m_max",
    "relative_humidity_2m_mean", "pressure_msl_mean",
    "cloud_cover_mean", "shortwave_radiation_sum",
    "apparent_temperature_max", "weather_code",
]

# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"

    fmt = "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=date_fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

logger = logging.getLogger("pipeline")

# ─────────────────────────────────────────────────────────────────────────────
# Ingestion helpers (self-contained so pipeline.py has no hard dep on Day-2
# ingestion.py — swap in your real ingestion module below if available)
# ─────────────────────────────────────────────────────────────────────────────

def _try_import_ingestion():
    """Return the fetch_historical function from ingestion.py if available."""
    for module_path in ("ingestion", "src.ingestion"):
        try:
            mod = __import__(module_path, fromlist=["fetch_historical"])
            return mod.fetch_historical
        except ImportError:
            continue
    return None


def _fetch_from_open_meteo(
    city_name: str,
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    variables: list[str],
) -> pd.DataFrame:
    """
    Thin wrapper around the Open-Meteo archive API.
    Falls back to this when ingestion.py is not available.
    """
    import requests
    from time import sleep

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":  latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date":   end_date,
        "daily":      ",".join(variables),
        "timezone":   "auto",
    }

    for attempt in range(1, 4):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            daily = data.get("daily", {})
            df = pd.DataFrame(daily)
            df.rename(columns={"time": "time"}, inplace=True)
            df.insert(0, "city", city_name.lower())
            return df
        except Exception as exc:
            logger.warning(f"Attempt {attempt} failed for {city_name}: {exc}")
            sleep(2 ** attempt)

    raise RuntimeError(f"Could not fetch data for {city_name} after 3 attempts.")


def _ingest_city(
    city: dict,
    start_date: str,
    end_date: str,
    variables: list[str],
    fetch_fn,
) -> pd.DataFrame:
    """Fetch data for a single city, using whichever fetch function is available."""
    name = city["name"]
    lat  = city["latitude"]
    lon  = city["longitude"]

    logger.info(f"  Fetching {name} [{start_date} → {end_date}]")

    if fetch_fn is not None:
        try:
            df = fetch_fn(
                city_name=name,
                latitude=lat,
                longitude=lon,
                start_date=start_date,
                end_date=end_date,
                variables=variables,
            )
        except TypeError:
            # Some versions use positional args
            df = fetch_fn(name, lat, lon, start_date, end_date, variables)
    else:
        df = _fetch_from_open_meteo(name, lat, lon, start_date, end_date, variables)

    logger.info(f"    → {len(df):,} rows fetched.")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Database helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_connection(db_path: Path) -> "duckdb.DuckDBPyConnection":
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def ensure_raw_table(conn) -> None:
    # Create schemas if they don't exist
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging") 
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_historical (
            city                      VARCHAR,
            time                      DATE,
            temperature_2m_max        DOUBLE,
            temperature_2m_min        DOUBLE,
            temperature_2m_mean       DOUBLE,
            precipitation_sum         DOUBLE,
            rain_sum                  DOUBLE,
            snowfall_sum              DOUBLE,
            wind_speed_10m_max        DOUBLE,
            wind_gusts_10m_max        DOUBLE,
            relative_humidity_2m_mean DOUBLE,
            pressure_msl_mean         DOUBLE,
            cloud_cover_mean          DOUBLE,
            shortwave_radiation_sum   DOUBLE,
            apparent_temperature_max  DOUBLE,
            weather_code              INTEGER,
            latitude                  DOUBLE,
            longitude                 DOUBLE
        )
    """)


def get_latest_date_per_city(conn) -> dict[str, date]:
    """Return a dict: city_name (lower) → latest date in raw_historical."""
    try:
        rows = conn.execute(
            "SELECT city, MAX(time) AS latest FROM raw.raw_historical GROUP BY city"
        ).fetchall()
        return {row[0].lower(): row[1] for row in rows}
    except Exception:
        return {}

def should_run_weekly_update(conn, city_name: str, force_update: bool = False) -> bool:
    """
    Check if weekly update should run for a city.
    Returns True if:
    - No data exists for the city, OR
    - Force update is requested, OR  
    - Latest data is more than 7 days old
    """
    if force_update:
        return True
        
    latest_dates = get_latest_date_per_city(conn)
    latest_date = latest_dates.get(city_name.lower())
    
    if latest_date is None:
        logger.info(f"  {city_name}: No existing data found - will fetch from 2020-01-01")
        return True
    
    today = date.today()
    days_since_update = (today - latest_date).days
    
    if days_since_update >= 7:
        logger.info(f"  {city_name}: Data is {days_since_update} days old - weekly update needed")
        return True
    else:
        logger.info(f"  {city_name}: Data is only {days_since_update} days old - skipping weekly update")
        return False


def should_fetch_missing_days(conn, city_name: str, force_update: bool = False) -> bool:
    """
    Check if daily update should run for a city to fetch missing days.
    Returns True if:
    - No data exists for city, OR
    - Force update is requested, OR  
    - There are missing days between max database date and current date
    """
    if force_update:
        return True
        
    latest_dates = get_latest_date_per_city(conn)
    latest_date = latest_dates.get(city_name.lower())
    
    if latest_date is None:
        logger.info(f"  {city_name}: No existing data found - will fetch from 2020-01-01")
        return True
    
    today = date.today()
    days_since_update = (today - latest_date).days
    
    if days_since_update > 0:
        logger.info(f"  {city_name}: Data is {days_since_update} days old - fetching missing days")
        return True
    else:
        logger.info(f"  {city_name}: Data is up-to-date - no missing days")
        return False


def append_to_raw(conn, df: pd.DataFrame) -> int:
    """Append a DataFrame to raw.raw_historical. Returns the number of rows inserted."""
    if df.empty:
        return 0
    conn.register("_tmp_new", df)
    conn.execute("INSERT INTO raw.raw_historical SELECT * FROM _tmp_new")
    conn.unregister("_tmp_new")
    return len(df)


def load_raw_as_df(conn) -> pd.DataFrame:
    """Return the entire raw.raw_historical table as a DataFrame."""
    try:
        return conn.execute("SELECT * FROM raw.raw_historical").df()
    except Exception:
        return pd.DataFrame()

def load_staging_as_df(conn) -> pd.DataFrame:
    try:
        return conn.execute("SELECT * FROM staging.staging_historical").df()
    except Exception:
        return pd.DataFrame()

def load_analytics_as_df(conn) -> pd.DataFrame:
    try:
        return conn.execute("SELECT * FROM analytics.analytics_historical").df()
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline stages
# ─────────────────────────────────────────────────────────────────────────────

def stage_ingest(
    conn,
    cities: list[dict],
    start_date: str,
    end_date: str,
    variables: list[str],
    mode: str,
    data_dir: Path,
    force_update: bool = False,
) -> dict:
    """
    Ingest stage.
    - full:        recreate raw_historical and fetch all data

    - incremental: daily update only (fetch from latest date + 1 day to current date)
    """
    fetch_fn = _try_import_ingestion()
    if fetch_fn:
        logger.info("Using project ingestion.py for fetching.")
    else:
        logger.info("ingestion.py not found — using built-in Open-Meteo fetcher.")

    if mode == "full":
        logger.info("FULL mode: dropping and recreating raw.raw_historical.")
        conn.execute("DROP TABLE IF EXISTS raw.raw_historical")

    ensure_raw_table(conn)

    latest_per_city = get_latest_date_per_city(conn)
    summary = {"rows_ingested": 0, "cities_skipped": 0, "errors": []}

    all_frames: list[pd.DataFrame] = []

    for city in cities:
        name_lower = city["name"].lower()

        if mode == "incremental":

            # Check if daily update should run to fetch missing days
            if not should_fetch_missing_days(conn, city["name"], force_update):
                summary["cities_skipped"] += 1
                continue
                
            latest = latest_per_city.get(name_lower)
            if latest:
                # Fetch from day after the latest stored date
                new_start = (latest + timedelta(days=1)).strftime("%Y-%m-%d")
                if new_start > end_date:
                    logger.info(f"  {city['name']}: already up-to-date (latest={latest}).")
                    summary["cities_skipped"] += 1
                    continue
            else:
                # No existing data, start from default start date
                new_start = start_date
        else:
            new_start = start_date

        try:
            df = _ingest_city(city, new_start, end_date, variables, fetch_fn)
            if df.empty:
                logger.warning(f"  {city['name']}: empty response, skipping.")
                continue
            df["time"] = pd.to_datetime(df["time"]).dt.date
            # Add coordinates from city config
            df["latitude"] = city["latitude"]
            df["longitude"] = city["longitude"]
            # Align dtypes to schema
            for col in variables:
                if col in df.columns and col != "weather_code":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            if "weather_code" in df.columns:
                df["weather_code"] = pd.to_numeric(df["weather_code"], errors="coerce").astype("Int64")

            n = append_to_raw(conn, df)
            summary["rows_ingested"] += n
            all_frames.append(df)
        except Exception as exc:
            logger.error(f"  {city['name']}: FAILED — {exc}")
            summary["errors"].append(f"{city['name']}: {exc}")

    # Also save combined raw parquet for cleaning.py to pick up
    if all_frames or mode == "full":
        try:
            full_df = load_raw_as_df(conn)
            if not full_df.empty:
                raw_parquet = data_dir / "raw.parquet"
                full_df.to_parquet(str(raw_parquet), index=False)
                logger.info(f"Raw parquet saved → {raw_parquet}")
        except Exception as exc:
            logger.warning(f"Could not save raw parquet: {exc}")

    return summary


def stage_clean(conn, data_dir: Path) -> dict:
    """Cleaning stage: raw → staging."""
    summary = {"errors": []}
    try:
        clean_raw_to_staging(conn, data_dir=str(data_dir))
        staging_df = load_staging_as_df(conn)
        summary["rows_staging"] = len(staging_df)
    except Exception as exc:
        logger.error(f"Cleaning stage failed: {exc}")
        summary["errors"].append(str(exc))
        summary["rows_staging"] = 0
    return summary


def stage_features(conn) -> dict:
    """Feature engineering stage: staging → analytics."""
    summary = {"errors": []}
    try:
        populate_analytics_tables(conn)
        analytics_df = load_analytics_as_df(conn)
        summary["rows_analytics"] = len(analytics_df)
    except Exception as exc:
        logger.error(f"Feature engineering stage failed: {exc}")
        summary["errors"].append(str(exc))
        summary["rows_analytics"] = 0
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline runner
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(
    mode: str       = "incremental",
    db_path: Path   = DEFAULT_DB_PATH,
    data_dir: Path  = DEFAULT_DATA_DIR,
    log_dir: Path   = DEFAULT_LOG_DIR,
    cities: Optional[list[str]] = None,
    start_date: str = DEFAULT_START,
    end_date: str   = DEFAULT_END,
    force_update: bool = False,
) -> dict:
    """
    Execute the full pipeline.

    Parameters
    ----------
    mode         : "full" or "incremental"
    db_path      : path to the DuckDB file
    data_dir     : path to the data directory
    log_dir      : where to write pipeline.log
    cities       : optional list of city names to process (None = all)
    start_date   : ISO date string for historical start
    end_date     : ISO date string for historical end
    force_update : force weekly update regardless of 7-day rule

    Returns
    -------
    dict with summary information
    """
    setup_logging(log_dir)
    t0 = time.time()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 70)
    logger.info(f"Heat-Pulse Pipeline  |  run_id={run_id}  |  mode={mode.upper()}")
    logger.info("=" * 70)

    # Filter cities if a subset was requested
    selected_cities = CITIES_CONFIG
    if cities:
        lower_names = [c.lower() for c in cities]
        selected_cities = [c for c in CITIES_CONFIG if c["name"].lower() in lower_names]
        if not selected_cities:
            logger.warning(f"No matching cities found for filter: {cities}")

    summary: dict = {
        "run_id":     run_id,
        "mode":       mode,
        "start_time": datetime.now().isoformat(),
        "errors":     [],
        "warnings":   [],
    }

    conn = get_connection(db_path)
    data_dir.mkdir(parents=True, exist_ok=True)

    try:
        # ── Stage 1: Ingest ──────────────────────────────────────────────────
        logger.info("\n── Stage 1: INGEST ─────────────────────────────────────────")
        ingest_summary = stage_ingest(
            conn, selected_cities, start_date, end_date,
            WEATHER_VARIABLES, mode, data_dir, force_update
        )
        summary["rows_ingested"]    = ingest_summary["rows_ingested"]
        summary["cities_skipped"]   = ingest_summary.get("cities_skipped", 0)
        summary["errors"]          += ingest_summary.get("errors", [])
        logger.info(f"  Rows ingested: {ingest_summary['rows_ingested']:,}")

        # Quality check after raw load
        raw_df = load_raw_as_df(conn)
        qc_raw = run_all_checks(raw_df=raw_df)
        print_quality_report(qc_raw)

        # Abort if row count check failed
        if any(r["status"] == "FAIL" for r in qc_raw):
            raise RuntimeError("Quality gate FAILED after ingest stage — aborting.")

        # ── Stage 2: Clean ───────────────────────────────────────────
        # Only run cleaning if new data was ingested
        if ingest_summary.get("rows_ingested", 0) > 0:
            logger.info("\n── Stage 2: CLEAN ──────────────────────────────────────────")
            clean_summary = stage_clean(conn, data_dir)
            summary["rows_staging"]  = clean_summary.get("rows_staging", 0)
            summary["errors"]       += clean_summary.get("errors", [])
            logger.info(f"  Rows in staging: {clean_summary.get('rows_staging', 0):,}")

            # Quality check after staging
            staging_df = load_staging_as_df(conn)
            qc_staging = run_all_checks(staging_df=staging_df)
            print_quality_report(qc_staging)
        else:
            logger.info("\n── Stage 2: CLEAN ──────────────────────────────────────────")
            logger.info("  SKIPPED: No new data to clean")
            summary["rows_staging"] = 0

        # ── Stage 3: Features ────────────────────────────────────────
        # Only run features if new data was ingested or cleaned
        if ingest_summary.get("rows_ingested", 0) > 0 or summary["rows_staging"] > 0:
            logger.info("\n── Stage 3: FEATURES ───────────────────────────────")
            feat_summary = stage_features(conn)
            summary["rows_analytics"] = feat_summary.get("rows_analytics", 0)
            summary["errors"]        += feat_summary.get("errors", [])
            logger.info(f"  Rows in analytics: {feat_summary.get('rows_analytics', 0):,}")

            # Quality check after analytics
            analytics_df = load_analytics_as_df(conn)
            qc_analytics = run_all_checks(analytics_df=analytics_df)
            print_quality_report(qc_analytics)
        else:
            logger.info("\n── Stage 3: FEATURES ───────────────────────────────")
            logger.info("  SKIPPED: No new data to process")
            summary["rows_analytics"] = 0

    except Exception as exc:
        logger.error(f"Pipeline aborted: {exc}")
        summary["errors"].append(str(exc))
        summary["status"] = "ABORTED"
    else:
        summary["status"] = "COMPLETED" if not summary["errors"] else "COMPLETED_WITH_WARNINGS"
    finally:
        conn.close()

    elapsed = time.time() - t0
    summary["duration_seconds"] = round(elapsed, 1)
    summary["end_time"] = datetime.now().isoformat()

    logger.info("\n" + "=" * 70)
    logger.info(f"Pipeline {summary['status']} in {elapsed:.1f}s")
    logger.info(f"  Ingested:  {summary.get('rows_ingested', 0):,} rows")
    logger.info(f"  Staging:   {summary.get('rows_staging', 0):,} rows")
    logger.info(f"  Analytics: {summary.get('rows_analytics', 0):,} rows")
    if summary["errors"]:
        logger.error(f"  Errors: {summary['errors']}")
    logger.info("=" * 70)

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry-point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Heat-Pulse Weather Intelligence Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/pipeline.py --mode full
  python src/pipeline.py --mode incremental
  python src/pipeline.py --mode incremental --cities Baku Ganja
  python src/pipeline.py --mode incremental --force-update
        """,
    )
    parser.add_argument(
        "--mode", choices=["full", "incremental"], default="incremental",

        help="full = re-ingest everything; incremental = daily update only (default: incremental)",
    )
    parser.add_argument("--db-path",  default=str(DEFAULT_DB_PATH),  help="Path to weather.duckdb")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="Path to data/ folder")
    parser.add_argument("--log-dir",  default=str(DEFAULT_LOG_DIR),  help="Path for log files")
    parser.add_argument("--cities",   nargs="*", default=None,        help="Specific city names to process")
    parser.add_argument("--start",    default=DEFAULT_START,          help=f"Start date (default: {DEFAULT_START})")
    parser.add_argument("--end",      default=DEFAULT_END,            help=f"End date (default: today)")

    parser.add_argument("--force-update", action="store_true",       help="Force daily update regardless of data freshness")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    summary = run_pipeline(
        mode         = args.mode,
        db_path      = Path(args.db_path),
        data_dir     = Path(args.data_dir),
        log_dir      = Path(args.log_dir),
        cities       = args.cities,
        start_date   = args.start,
        end_date     = args.end,
        force_update = args.force_update,
    )
    sys.exit(0 if summary["status"] != "ABORTED" else 1)
