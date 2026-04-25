"""
src/cleaning.py
Task 2 — Cleaning Pipeline
"""

import pandas as pd
import numpy as np
import duckdb
from pathlib import Path


# ─────────────────────────────────────────────
# 1. handle_missing_values
# ─────────────────────────────────────────────

def handle_missing_values(df: pd.DataFrame, strategy: dict = None) -> pd.DataFrame:
    """
    Imputes or drops missing values based on a strategy dictionary.

    Default strategy:
        - temperature columns  → forward-fill (then backward-fill as fallback)
        - precipitation / rain / snowfall → fill with 0  (no rain = 0)
        - all other numeric    → linear interpolation
        - any remaining nulls  → drop row
    """
    df = df.copy()
    df = df.sort_values(['city', 'time']).reset_index(drop=True)

    if strategy is None:
        strategy = {}
        for col in df.columns:
            if col in ('city', 'time', 'weather_code'):
                continue
            if 'temperature' in col or 'apparent' in col:
                strategy[col] = 'ffill'
            elif col in ('precipitation_sum', 'rain_sum', 'snowfall_sum'):
                strategy[col] = 'zero'
            else:
                strategy[col] = 'interpolate'

    for col, method in strategy.items():
        if col not in df.columns:
            continue

        if method == 'ffill':
            df[col] = (
                df.groupby('city')[col]
                  .transform(lambda x: x.ffill().bfill())
            )
        elif method == 'zero':
            df[col] = df[col].fillna(0)
        elif method == 'interpolate':
            df[col] = (
                df.groupby('city')[col]
                  .transform(lambda x: x.interpolate(method='linear',
                                                       limit_direction='both'))
            )
        elif method == 'drop':
            df = df.dropna(subset=[col])

    before = len(df)
    df = df.dropna()
    dropped = before - len(df)
    if dropped > 0:
        print(f"[handle_missing_values] Dropped {dropped} rows with remaining nulls.")

    return df


# ─────────────────────────────────────────────
# 2. flag_outliers
# ─────────────────────────────────────────────

def flag_outliers(
    df: pd.DataFrame,
    columns: list,
    method: str = 'iqr',
    threshold: float = 1.5
) -> pd.DataFrame:
    """
    Adds boolean flag columns for outliers WITHOUT removing them.
    New columns: <column>_outlier_flag  (True = outlier).
    """
    df = df.copy()

    for col in columns:
        if col not in df.columns:
            print(f"[flag_outliers] Column '{col}' not found, skipping.")
            continue

        flag_col = f"{col}_outlier_flag"

        if method == 'iqr':
            def _iqr_flag(group):
                q1 = group.quantile(0.25)
                q3 = group.quantile(0.75)
                iqr = q3 - q1
                lower = q1 - threshold * iqr
                upper = q3 + threshold * iqr
                return (group < lower) | (group > upper)

            df[flag_col] = df.groupby('city')[col].transform(_iqr_flag)

        elif method == 'zscore':
            def _z_flag(group):
                mean = group.mean()
                std  = group.std()
                if std == 0:
                    return pd.Series(False, index=group.index)
                z = (group - mean).abs() / std
                return z > threshold

            df[flag_col] = df.groupby('city')[col].transform(_z_flag)

        else:
            raise ValueError(f"Unknown method '{method}'. Use 'iqr' or 'zscore'.")

        n_flagged = df[flag_col].sum()
        print(f"[flag_outliers] {col}: {n_flagged} outliers flagged ({n_flagged/len(df)*100:.2f}%)")

    return df


# ─────────────────────────────────────────────
# 3. validate_date_continuity
# ─────────────────────────────────────────────

def validate_date_continuity(df: pd.DataFrame, city: str) -> pd.DataFrame:
    """
    Checks for date gaps in a single city's time series.
    Returns a summary DataFrame.
    """
    city_df = df[df['city'] == city].copy()
    city_df['time'] = pd.to_datetime(city_df['time'])
    city_df = city_df.sort_values('time')

    start_date    = city_df['time'].min()
    end_date      = city_df['time'].max()
    full_range    = pd.date_range(start=start_date, end=end_date, freq='D')
    actual_dates  = pd.DatetimeIndex(city_df['time'].dt.normalize().unique())
    missing_dates = full_range.difference(actual_dates)

    summary = pd.DataFrame([{
        'city'          : city,
        'start_date'    : start_date.date(),
        'end_date'      : end_date.date(),
        'expected_days' : len(full_range),
        'actual_days'   : len(actual_dates),
        'missing_count' : len(missing_dates),
        'missing_dates' : list(missing_dates.date) if len(missing_dates) > 0 else []
    }])

    if len(missing_dates) == 0:
        print(f"[validate_date_continuity] {city}: Timeline is complete ✓")
    else:
        print(f"[validate_date_continuity] {city}: {len(missing_dates)} missing dates found!")

    return summary


def validate_all_cities(df: pd.DataFrame) -> pd.DataFrame:
    summaries = [validate_date_continuity(df, city) for city in df['city'].unique()]
    return pd.concat(summaries, ignore_index=True)


# ─────────────────────────────────────────────
# 4. clean_raw_to_staging  (FIXED)
# ─────────────────────────────────────────────

def clean_raw_to_staging(
    conn: duckdb.DuckDBPyConnection,
    data_dir: str = None
) -> None:
    """
    Reads raw data from parquet files (your project stores data as .parquet),
    applies cleaning, and writes:
        • DuckDB tables  staging_historical / staging_forecast
        • Parquet files  staging_historical.parquet / staging_forecast.parquet

    Source detection order:
        1. <data_dir>/raw.parquet          → treated as 'historical'
        2. <data_dir>/raw_historical.parquet
        3. DuckDB table raw_historical     (legacy fallback)

        1. <data_dir>/forecast.parquet     → treated as 'forecast'
        2. <data_dir>/raw_forecast.parquet
        3. DuckDB table raw_forecast       (legacy fallback)

    Parameters
    ----------
    conn     : active DuckDB connection
    data_dir : path to the data folder
               e.g. r'C:\\Users\\Vito\\Desktop\\Heat-Pulse\\data'
               If None, defaults to the same folder as weather.duckdb.
    """

    OUTLIER_COLS = [
        'temperature_2m_max', 'temperature_2m_min', 'temperature_2m_mean',
        'precipitation_sum', 'wind_speed_10m_max', 'wind_gusts_10m_max',
        'pressure_msl_mean', 'shortwave_radiation_sum', 'apparent_temperature_max'
    ]

    # ── Resolve data_dir ────────────────────────────────────────────────────
    if data_dir is None:
        # Try to get the DB file path from DuckDB
        try:
            row = conn.execute("SELECT current_setting('database_file')").fetchone()
            data_dir = str(Path(row[0]).parent)
        except Exception:
            data_dir = 'data'

    data_dir = Path(data_dir)
    print(f"[clean_raw_to_staging] data_dir = {data_dir}")

    # ── Source map: kind → list of candidate parquet names ──────────────────
    source_map = {
        'historical': ['raw.parquet', 'raw_historical.parquet'],
        'forecast'  : ['forecast.parquet', 'raw_forecast.parquet'],
    }

    def _try_parquet(candidates):
        for name in candidates:
            p = data_dir / name
            if p.exists():
                print(f"  Found parquet: {p}")
                return pd.read_parquet(p)
        return None

    def _try_duckdb(table_name):
        try:
            df = conn.execute(f"SELECT * FROM {table_name}").df()
            print(f"  Found DuckDB table: {table_name}")
            return df
        except Exception:
            return None

    # ── Main loop ───────────────────────────────────────────────────────────
    any_processed = False

    for kind, candidates in source_map.items():
        staging_table = f'staging_{kind}'

        print(f"\n{'='*55}")
        print(f"  {kind.upper()}  →  {staging_table}")
        print('='*55)

        # Load: parquet first, DuckDB table as fallback
        df = _try_parquet(candidates)
        if df is None:
            df = _try_duckdb(f'raw_{kind}')

        if df is None:
            print(f"  ⚠  No source found for '{kind}'. Skipping.\n"
                  f"     Looked for: {[str(data_dir/c) for c in candidates]}")
            continue

        df['time'] = pd.to_datetime(df['time'])
        print(f"  Rows: {len(df):,}  |  Columns: {list(df.columns)}")

        # Step 1 — missing values
        df = handle_missing_values(df)
        print(f"  After cleaning: {len(df):,} rows")

        # Step 2 — flag outliers (don't remove)
        existing = [c for c in OUTLIER_COLS if c in df.columns]
        df = flag_outliers(df, columns=existing, method='iqr', threshold=1.5)

        # Step 3 — date continuity report → DuckDB
        continuity = validate_all_cities(df)
        gaps_table = f'{staging_table}_date_gaps'
        conn.register('_tmp_gaps', continuity)
        conn.execute(f"CREATE OR REPLACE TABLE {gaps_table} AS SELECT * FROM _tmp_gaps")
        print(f"  Date-gap summary → DuckDB table: {gaps_table}")

        # Step 4 — write staging to DuckDB
        conn.register('_tmp_staging', df)
        conn.execute(
            f"CREATE OR REPLACE TABLE {staging_table} AS SELECT * FROM _tmp_staging"
        )
        print(f"  DuckDB table '{staging_table}' written ({len(df):,} rows)")

        # Step 5 — also save as parquet (keeps your data/ folder consistent)
        out_path = data_dir / f'{staging_table}.parquet'
        df.to_parquet(out_path, index=False)
        print(f"  Parquet saved → {out_path}")

        any_processed = True

    if not any_processed:
        print("\n❌ Nothing was processed.")
        print(f"   data_dir checked: {data_dir}")
        print("   Expected files:  raw.parquet  OR  raw_historical.parquet / forecast.parquet")
    else:
        print("\n[clean_raw_to_staging] Done ✓")


# ─────────────────────────────────────────────
# Run directly
# ─────────────────────────────────────────────
if __name__ == '__main__':
    DATA_DIR = r'data'
    DB_PATH  = r'data\weather.duckdb'

    conn = duckdb.connect(DB_PATH)
    clean_raw_to_staging(conn, data_dir=DATA_DIR)
    conn.close()