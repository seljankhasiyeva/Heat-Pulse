"""
src/database.py
Task 2 (Day 3) — Database setup + incremental loading support (Day 5 update)

Public API
----------
get_connection(db_path)           → DuckDB connection
create_schemas(conn)              → ensures raw / staging / analytics schemas exist
create_raw_tables(conn)           → creates raw_historical + raw_forecast if not present
load_raw_data(conn, data_dir)     → FULL load from CSV/Parquet → raw tables
load_incremental(conn, df, city)  → INCREMENTAL append of new rows only
get_latest_date(conn, city)       → last date stored for a city
row_counts(conn)                  → summary dict per table
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent

try:
    import duckdb
except ImportError:
    raise ImportError("duckdb is required.  Run: pip install duckdb")

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Column schema for raw tables
# ─────────────────────────────────────────────────────────────────────────────
RAW_COLUMNS_DDL = """
    city                         VARCHAR,
    time                         DATE,
    temperature_2m_max          DOUBLE,
    temperature_2m_min          DOUBLE,
    temperature_2m_mean         DOUBLE,
    precipitation_sum           DOUBLE,
    rain_sum                    DOUBLE,
    snowfall_sum                DOUBLE,
    wind_speed_10m_max          DOUBLE,
    wind_gusts_10m_max          DOUBLE,
    relative_humidity_2m_mean   DOUBLE,
    pressure_msl_mean           DOUBLE,
    cloud_cover_mean            DOUBLE,
    shortwave_radiation_sum     DOUBLE,
    apparent_temperature_max    DOUBLE,
    weather_code                INTEGER,
    latitude                    DOUBLE,
    longitude                   DOUBLE
"""

# ─────────────────────────────────────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────────────────────────────────────

def get_connection(db_path: str | Path = None) -> "duckdb.DuckDBPyConnection":
    """
    Əgər db_path verilməyibsə, avtomatik olaraq layihənin ana qovluğundakı
    data/weather.duckdb faylına qoşulur.
    """
    if db_path is None:
        db_path = BASE_DIR / "data" / "weather.duckdb"
    
    path = Path(db_path)
    # Lazımi qovluqları (data/) yaradır
    path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(str(path))
    logger.info(f"Connected to DuckDB: {path.resolve()}")
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# Schema creation
# ─────────────────────────────────────────────────────────────────────────────

def create_schemas(conn: "duckdb.DuckDBPyConnection") -> None:
    """
    Create the raw, staging, and analytics schemas if they do not exist.
    DuckDB uses schemas to logically separate pipeline layers.
    """
    for schema in ("raw", "staging", "analytics"):
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    logger.info("Schemas ensured: raw, staging, analytics")


def create_raw_tables(conn: "duckdb.DuckDBPyConnection") -> None:
    """
    Cədvəlləri 'raw' sxeminin daxilində yaradırıq.
    """
    conn.execute(f"CREATE TABLE IF NOT EXISTS raw.raw_historical ({RAW_COLUMNS_DDL})")
    conn.execute(f"CREATE TABLE IF NOT EXISTS raw.raw_forecast ({RAW_COLUMNS_DDL})")
    logger.info("Raw tables ensured inside 'raw' schema.")

# ─────────────────────────────────────────────────────────────────────────────
# Full load (Day 3 original)
# ─────────────────────────────────────────────────────────────────────────────

def load_raw_data(conn, data_dir=None) -> dict:
    if data_dir is None:
        data_dir = BASE_DIR / "data" / "raw"
    
    data_dir = Path(data_dir)
    # Sxem daxilindəki cədvəlləri silirik
    conn.execute("DROP TABLE IF EXISTS raw.raw_historical")
    conn.execute("DROP TABLE IF EXISTS raw.raw_forecast")
    create_raw_tables(conn)
    
    summary = {}
    files = {
        "raw.raw_historical": data_dir / "all_94_cities_historical_combined.csv",
        "raw.raw_forecast": data_dir / "all_94_cities_forecast_combined.csv"
    }

    for table, path in files.items():
        if path.exists():
            logger.info(f" Loading {path.name} → {table}")
            conn.execute(f"INSERT INTO {table} SELECT * FROM read_csv_auto('{path}')")
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            summary[table] = n
        else:
            logger.warning(f" File not found: {path}")
            summary[table] = 0
    return summary
# ─────────────────────────────────────────────────────────────────────────────
# Incremental loading (Day 5 addition)
# ─────────────────────────────────────────────────────────────────────────────

def get_latest_date(
    conn: "duckdb.DuckDBPyConnection",
    city: str,
    table: str = "raw_historical",
) -> Optional[date]:
    """
    Return the most recent date stored for *city* in *table*.
    Returns None if the table is empty or the city has no rows yet.
    """
    try:
        row = conn.execute(
            f"SELECT MAX(time) FROM {table} WHERE city = ?",
            [city.lower()],
        ).fetchone()
        return row[0] if row and row[0] is not None else None
    except Exception:
        return None


def load_incremental(
    conn:  "duckdb.DuckDBPyConnection",
    df:    pd.DataFrame,
    table: str = "raw_historical",
) -> int:
    """
    Append only the rows in *df* that are NOT already in *table*.

    Deduplication is done on (city, time).  This avoids primary-key
    constraint issues while still being safe to call repeatedly.

    Returns the number of rows actually inserted.
    """
    if df.empty:
        return 0

    # Normalise
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"]).dt.date
    if "city" in df.columns:
        df["city"] = df["city"].str.lower()

    # Find what is already stored for each city in this batch
    cities_in_batch = df["city"].unique().tolist()
    placeholders    = ", ".join(["?" for _ in cities_in_batch])
    try:
        existing = conn.execute(
            f"SELECT city, time FROM {table} WHERE city IN ({placeholders})",
            cities_in_batch,
        ).df()
        existing["time"] = pd.to_datetime(existing["time"]).dt.date
        existing_keys = set(zip(existing["city"], existing["time"]))
    except Exception:
        existing_keys = set()

    # Keep only new rows
    mask    = ~df.apply(lambda row: (row["city"], row["time"]) in existing_keys, axis=1)
    new_df  = df[mask]

    if new_df.empty:
        logger.info(f"  No new rows to insert into {table}.")
        return 0

    conn.register("_tmp_incremental", new_df)
    conn.execute(f"INSERT INTO {table} SELECT * FROM _tmp_incremental")
    conn.unregister("_tmp_incremental")

    logger.info(f"  Incremental insert: {len(new_df):,} new rows → {table}")
    return len(new_df)


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def row_counts(conn: "duckdb.DuckDBPyConnection") -> dict:
    """
    Return a summary dict: table_name → row_count for all pipeline tables.
    """
    tables = [
        "raw.raw_historical", "raw.raw_forecast",
        "staging.staging_historical", "staging.staging_forecast",
        "analytics.analytics_historical", "analytics.analytics_forecast",
    ]
    counts = {}
    for t in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            counts[t] = n
        except Exception:
            counts[t] = None   # table doesn't exist yet
    return counts


def print_row_counts(conn: "duckdb.DuckDBPyConnection") -> None:
    counts = row_counts(conn)
    print("\n── Table row counts ─────────────────────────────────")
    for table, n in counts.items():
        status = f"{n:>12,}" if n is not None else "  (not created)"
        print(f"  {table:<30} {status}")
    print("─────────────────────────────────────────────────────\n")


# ─────────────────────────────────────────────────────────────────────────────
# Quick standalone test
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    DB_PATH  = os.environ.get("DB_PATH",  "data/weather.duckdb")
    DATA_DIR = os.environ.get("DATA_DIR", "data")

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    conn = get_connection(DB_PATH)
    create_schemas(conn)
    create_raw_tables(conn)

    print("Loading raw data …")
    summary = load_raw_data(conn, data_dir=DATA_DIR)
    print(f"Loaded: {summary}")

    print_row_counts(conn)
    conn.close()
