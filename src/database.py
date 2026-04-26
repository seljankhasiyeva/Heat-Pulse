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
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    import duckdb
except ImportError:
    raise ImportError("duckdb is required.  Run: pip install duckdb")

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Column schema for raw_historical
# ─────────────────────────────────────────────────────────────────────────────
RAW_HISTORICAL_DDL = """
CREATE TABLE IF NOT EXISTS raw_historical (
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
    PRIMARY KEY (city, time)
)
"""

RAW_FORECAST_DDL = """
CREATE TABLE IF NOT EXISTS raw_forecast (
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
    PRIMARY KEY (city, time)
)
"""

# ─────────────────────────────────────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────────────────────────────────────

def get_connection(db_path: str | Path = "data/weather.duckdb") -> "duckdb.DuckDBPyConnection":
    """
    Return a DuckDB connection, creating the database file (and parent dirs)
    if they do not yet exist.
    """
    path = Path(db_path)
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
    Create raw_historical and raw_forecast tables if not present.
    Safe to call multiple times (IF NOT EXISTS).
    """
    # DuckDB (< 0.10) does not support composite PRIMARY KEY in CREATE TABLE
    # when the table already has data; we use a simpler CREATE TABLE IF NOT EXISTS
    # and enforce uniqueness via UPSERT logic in load_incremental.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_historical (
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
            weather_code              INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_forecast (
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
            weather_code              INTEGER
        )
    """)
    logger.info("Raw tables ensured: raw_historical, raw_forecast")


# ─────────────────────────────────────────────────────────────────────────────
# Full load (Day 3 original)
# ─────────────────────────────────────────────────────────────────────────────

def load_raw_data(
    conn: "duckdb.DuckDBPyConnection",
    data_dir: str | Path = "data",
) -> dict:
    """
    FULL load: read CSV/Parquet files from data_dir and insert into raw tables.
    Existing data is replaced (DROP + recreate + INSERT).

    Looks for (in order):
        historical → raw.parquet > raw_historical.parquet > all_*historical*.csv
        forecast   → forecast.parquet > raw_forecast.parquet > all_*forecast*.csv

    Returns a summary dict with row counts.
    """
    data_dir = Path(data_dir)
    create_raw_tables(conn)
    summary = {}

    def _load_one(table: str, candidates: list[Path]) -> int:
        for p in candidates:
            if p.exists():
                logger.info(f"  Loading {p.name} → {table}")
                conn.execute(f"DELETE FROM {table}")
                if p.suffix == ".parquet":
                    conn.execute(
                        f"INSERT INTO {table} SELECT * FROM read_parquet('{p}')"
                    )
                else:
                    conn.execute(
                        f"INSERT INTO {table} SELECT * FROM read_csv_auto('{p}')"
                    )
                n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                logger.info(f"  {table}: {n:,} rows loaded.")
                return n
        logger.warning(f"  No source file found for {table}. Looked in: {[str(c) for c in candidates]}")
        return 0

    hist_candidates = [
        data_dir / "raw.parquet",
        data_dir / "raw_historical.parquet",
        *sorted(data_dir.glob("*historical*.csv")),
        *sorted(data_dir.glob("all_*historical*.csv")),
    ]
    fc_candidates = [
        data_dir / "forecast.parquet",
        data_dir / "raw_forecast.parquet",
        *sorted(data_dir.glob("*forecast*.csv")),
    ]

    summary["raw_historical"] = _load_one("raw_historical", hist_candidates)
    summary["raw_forecast"]   = _load_one("raw_forecast",   fc_candidates)
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
        "raw_historical", "raw_forecast",
        "staging_historical", "staging_forecast",
        "analytics_historical", "analytics_forecast",
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
