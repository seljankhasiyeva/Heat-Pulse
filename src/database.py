"""
src/database.py
===============
DuckDB interfeysi — Heat-Pulse Weather Intelligence Pipeline.

Cədvəl strukturu (cleaning.py / features.py ilə uyğun):
  raw_historical       → API / CSV-dən gələn xam məlumat
  raw_forecast         → Xam 7 günlük proqnoz
  staging_historical   → cleaning.py tərəfindən yaradılır
  staging_forecast     → cleaning.py tərəfindən yaradılır
  analytics_historical → features.py tərəfindən yaradılır
  analytics_forecast   → features.py tərəfindən yaradılır
  pipeline_runs        → Pipeline audit loqu
"""

import duckdb
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH  = Path("data/weather.duckdb")
DATA_DIR = Path("data")


# ─────────────────────────────────────────────────────────────────────────────
# Əlaqə
# ─────────────────────────────────────────────────────────────────────────────

def get_connection(db_path=DB_PATH) -> duckdb.DuckDBPyConnection:
    """DuckDB faylını açır (yoxdursa yaradır) və əlaqəni qaytarır."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    logger.debug(f"DuckDB-yə qoşuldu: {db_path}")
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# Sxem yaratma
# ─────────────────────────────────────────────────────────────────────────────

def create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    raw_historical, raw_forecast və pipeline_runs cədvəllərini yaradır.
    staging_* və analytics_* cədvəlləri cleaning.py / features.py tərəfindən
    CREATE OR REPLACE ilə yaradılır.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_historical (
            time                       VARCHAR,
            city                       VARCHAR,
            latitude                   DOUBLE,
            longitude                  DOUBLE,
            temperature_2m_max         DOUBLE,
            temperature_2m_min         DOUBLE,
            temperature_2m_mean        DOUBLE,
            precipitation_sum          DOUBLE,
            rain_sum                   DOUBLE,
            snowfall_sum               DOUBLE,
            wind_speed_10m_max         DOUBLE,
            wind_gusts_10m_max         DOUBLE,
            pressure_msl_mean          DOUBLE,
            shortwave_radiation_sum    DOUBLE,
            apparent_temperature_max   DOUBLE,
            weather_code               DOUBLE,
            PRIMARY KEY (time, city)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_forecast (
            time                       VARCHAR,
            city                       VARCHAR,
            latitude                   DOUBLE,
            longitude                  DOUBLE,
            temperature_2m_max         DOUBLE,
            temperature_2m_min         DOUBLE,
            temperature_2m_mean        DOUBLE,
            precipitation_sum          DOUBLE,
            rain_sum                   DOUBLE,
            snowfall_sum               DOUBLE,
            wind_speed_10m_max         DOUBLE,
            wind_gusts_10m_max         DOUBLE,
            pressure_msl_mean          DOUBLE,
            shortwave_radiation_sum    DOUBLE,
            apparent_temperature_max   DOUBLE,
            weather_code               DOUBLE,
            PRIMARY KEY (time, city)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id          INTEGER PRIMARY KEY,
            run_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            mode            VARCHAR,
            cities_count    INTEGER,
            rows_raw        INTEGER,
            rows_staging    INTEGER,
            rows_analytics  INTEGER,
            duration_sec    DOUBLE,
            status          VARCHAR,
            notes           VARCHAR
        )
    """)

    logger.info("Sxem yoxlandı / yaradıldı.")


# ─────────────────────────────────────────────────────────────────────────────
# Xam məlumat normallaşdırma
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    time sütununu YYYY-MM-DD formatına gətirir.
    Çatışmayan sütunları None ilə əlavə edir.
    """
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%d")

    raw_cols = [
        "time", "city", "latitude", "longitude",
        "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
        "precipitation_sum", "rain_sum", "snowfall_sum",
        "wind_speed_10m_max", "wind_gusts_10m_max",
        "pressure_msl_mean", "shortwave_radiation_sum",
        "apparent_temperature_max", "weather_code",
    ]
    for col in raw_cols:
        if col not in df.columns:
            df[col] = None

    return df[raw_cols]


# ─────────────────────────────────────────────────────────────────────────────
# Xam məlumat yükləmə
# ─────────────────────────────────────────────────────────────────────────────

def load_raw_historical(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    mode: str = "append",
) -> int:
    """
    DataFrame-i raw_historical cədvəlinə yükləyir.
    mode='append'  → INSERT OR REPLACE (upsert)
    mode='replace' → Əvvəlcə bütün cədvəli sil, sonra yüklə
    """
    if df is None or df.empty:
        logger.warning("load_raw_historical: boş DataFrame — yüklənmir.")
        return 0

    df = _normalise_raw(df)

    if mode == "replace":
        conn.execute("DELETE FROM raw_historical")
        logger.info("raw_historical tam yenilənmə üçün təmizləndi.")

    conn.register("_rh", df)
    conn.execute("""
        INSERT OR REPLACE INTO raw_historical
        SELECT
            time, city, latitude, longitude,
            temperature_2m_max, temperature_2m_min, temperature_2m_mean,
            precipitation_sum, rain_sum, snowfall_sum,
            wind_speed_10m_max, wind_gusts_10m_max,
            pressure_msl_mean, shortwave_radiation_sum,
            apparent_temperature_max, weather_code
        FROM _rh
    """)
    conn.unregister("_rh")
    logger.info(f"raw_historical-a {len(df):,} sətir yükləndi.")
    return len(df)


def load_raw_forecast(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
) -> int:
    """raw_forecast cədvəlinə proqnozu yükləyir (həmişə əvəzlənir)."""
    if df is None or df.empty:
        return 0
    df = _normalise_raw(df)
    conn.execute("DELETE FROM raw_forecast")
    conn.register("_rf", df)
    conn.execute("""
        INSERT INTO raw_forecast
        SELECT
            time, city, latitude, longitude,
            temperature_2m_max, temperature_2m_min, temperature_2m_mean,
            precipitation_sum, rain_sum, snowfall_sum,
            wind_speed_10m_max, wind_gusts_10m_max,
            pressure_msl_mean, shortwave_radiation_sum,
            apparent_temperature_max, weather_code
        FROM _rf
    """)
    conn.unregister("_rf")
    logger.info(f"raw_forecast-a {len(df):,} sətir yükləndi.")
    return len(df)


# ─────────────────────────────────────────────────────────────────────────────
# Parquet ixracı (cleaning.py üçün lazımdır)
# ─────────────────────────────────────────────────────────────────────────────

def save_raw_as_parquet(
    conn: duckdb.DuckDBPyConnection,
    data_dir=DATA_DIR,
) -> None:
    """
    raw_historical → data/raw.parquet         (cleaning.py bunu axtarır)
    raw_historical → data/raw_historical.parquet
    raw_forecast   → data/raw_forecast.parquet
    """
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    exports = [
        ("raw_historical", "raw.parquet"),
        ("raw_historical", "raw_historical.parquet"),
        ("raw_forecast",   "raw_forecast.parquet"),
    ]
    for table, fname in exports:
        try:
            df = conn.execute(f"SELECT * FROM {table}").df()
            out = data_dir / fname
            df.to_parquet(out, index=False)
            logger.info(f"{len(df):,} sətir saxlandı → {out}")
        except Exception as e:
            logger.warning(f"{table} → {fname} saxlanıla bilmədi: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# İnkremental yardımçılar
# ─────────────────────────────────────────────────────────────────────────────

def get_latest_dates(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    raw_historical-dan {city: son_tarix_str} lüğəti qaytarır.
    İnkremental yükləmədə başlanğıc tarixi müəyyən etmək üçün istifadə olunur.
    """
    try:
        rows = conn.execute(
            "SELECT city, MAX(time) AS max_time FROM raw_historical GROUP BY city"
        ).fetchall()
        result = {row[0]: row[1] for row in rows}
        logger.info(f"{len(result)} şəhər üçün son tarix alındı.")
        return result
    except Exception as e:
        logger.warning(f"Son tarixlər oxuna bilmədi: {e}")
        return {}


def get_row_count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    """Cədvəldəki sətir sayını qaytarır. Cədvəl yoxdursa 0 qaytarır."""
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return 0


def table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Cədvəlin mövcudluğunu yoxlayır."""
    try:
        conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Audit loqu
# ─────────────────────────────────────────────────────────────────────────────

def log_pipeline_run(
    conn: duckdb.DuckDBPyConnection,
    mode: str,
    cities_count: int,
    rows_raw: int,
    rows_staging: int,
    rows_analytics: int,
    duration_sec: float,
    status: str,
    notes: str = "",
) -> None:
    """Pipeline icrasının qeydini pipeline_runs cədvəlinə yazır."""
    try:
        next_id = conn.execute(
            "SELECT COALESCE(MAX(run_id), 0) + 1 FROM pipeline_runs"
        ).fetchone()[0]
        conn.execute("""
            INSERT INTO pipeline_runs
                (run_id, mode, cities_count, rows_raw, rows_staging,
                 rows_analytics, duration_sec, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            next_id, mode, cities_count,
            rows_raw, rows_staging, rows_analytics,
            round(duration_sec, 2), status, notes,
        ])
        logger.info(f"Pipeline icra #{next_id} qeyd edildi: {status}")
    except Exception as e:
        logger.warning(f"Audit loqu yazıla bilmədi: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Verilənlər bazası icmalı
# ─────────────────────────────────────────────────────────────────────────────

def get_table_summary(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Bütün pipeline cədvəllərinin sətir sayını qaytarır."""
    tables = [
        "raw_historical", "raw_forecast",
        "staging_historical", "staging_forecast",
        "analytics_historical", "analytics_forecast",
        "pipeline_runs",
    ]
    return pd.DataFrame([
        {"table": t, "row_count": get_row_count(conn, t)}
        for t in tables
    ])


def print_row_counts(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Bütün cədvəllərin sətir sayını ekrana çıxarır.
    Notebook-dan rahat çağırmaq üçündür.
    """
    summary = get_table_summary(conn)
    print("\n" + "=" * 45)
    print("  VERİLƏNLƏR BAZASI — CƏDVƏLLƏRİN SƏTIR SAYI")
    print("=" * 45)
    for _, row in summary.iterrows():
        bar = "█" * min(int(row["row_count"] / 5000), 30)
        print(f"  {row['table']:<25} {row['row_count']:>10,}  {bar}")
    print("=" * 45 + "\n")