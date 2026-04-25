"""
src/features.py
Task 3 — Feature Engineering
"""

import pandas as pd
import numpy as np
import duckdb
import os


# ─────────────────────────────────────────────
# Helper: ensure the df is sorted correctly
# ─────────────────────────────────────────────

def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values(['city', 'time']).reset_index(drop=True)
    return df


# ─────────────────────────────────────────────
# 1. Rolling averages (7-day and 30-day)
# ─────────────────────────────────────────────

def add_rolling_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 7-day and 30-day rolling means for temperature and precipitation.

    New columns:
        temperature_2m_mean_7d, temperature_2m_mean_30d
        precipitation_sum_7d,   precipitation_sum_30d
    """
    df = _prepare(df)
    roll_targets = {
        'temperature_2m_mean': [7, 30],
        'precipitation_sum'  : [7, 30],
    }

    for col, windows in roll_targets.items():
        if col not in df.columns:
            print(f"[add_rolling_averages] Column '{col}' missing, skipping.")
            continue
        for w in windows:
            new_col = f"{col}_{w}d"
            df[new_col] = (
                df.groupby('city')[col]
                  .transform(lambda x, w=w: x.rolling(window=w, min_periods=1).mean())
            )

    print("[add_rolling_averages] Done ✓")
    return df


# ─────────────────────────────────────────────
# 2. Seasonal indicators
# ─────────────────────────────────────────────

def add_seasonal_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds calendar / seasonal columns:
        month, quarter, day_of_year, season
    Season is defined by meteorological convention (Northern Hemisphere):
        Winter: Dec-Jan-Feb | Spring: Mar-Apr-May
        Summer: Jun-Jul-Aug | Autumn: Sep-Oct-Nov
    """
    df = _prepare(df)

    df['month']      = df['time'].dt.month
    df['quarter']    = df['time'].dt.quarter
    df['day_of_year'] = df['time'].dt.day_of_year

    season_map = {
        12: 'winter', 1: 'winter', 2: 'winter',
        3 : 'spring', 4: 'spring', 5: 'spring',
        6 : 'summer', 7: 'summer', 8: 'summer',
        9 : 'autumn', 10:'autumn', 11:'autumn',
    }
    df['season'] = df['month'].map(season_map)

    print("[add_seasonal_indicators] Done ✓")
    return df


# ─────────────────────────────────────────────
# 3. Temperature range (daily volatility)
# ─────────────────────────────────────────────

def add_temperature_range(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds temperature_range = temperature_2m_max − temperature_2m_min
    as a daily volatility indicator.
    """
    df = _prepare(df)

    if 'temperature_2m_max' in df.columns and 'temperature_2m_min' in df.columns:
        df['temperature_range'] = df['temperature_2m_max'] - df['temperature_2m_min']
        print("[add_temperature_range] Done ✓")
    else:
        print("[add_temperature_range] Max or Min temperature columns missing.")

    return df


# ─────────────────────────────────────────────
# 4. Heating / Cooling Degree Days
# ─────────────────────────────────────────────

def add_degree_days(df: pd.DataFrame, base: float = 18.0) -> pd.DataFrame:
    """
    Proxy for energy demand.
        HDD = max(0, base − T_mean)   heating degree days
        CDD = max(0, T_mean − base)   cooling degree days

    Default base temperature = 18 °C (industry standard).
    """
    df = _prepare(df)

    if 'temperature_2m_mean' not in df.columns:
        print("[add_degree_days] 'temperature_2m_mean' missing.")
        return df

    df['HDD'] = (base - df['temperature_2m_mean']).clip(lower=0)
    df['CDD'] = (df['temperature_2m_mean'] - base).clip(lower=0)

    print("[add_degree_days] Done ✓")
    return df


# ─────────────────────────────────────────────
# 5. Anomaly score
# ─────────────────────────────────────────────

def add_anomaly_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    How far is today's temperature from the historical mean for that
    calendar day (day-of-year) and city?

        anomaly_score = T_mean − mean(T_mean for same day-of-year, same city)

    Positive → warmer than normal, Negative → cooler than normal.
    """
    df = _prepare(df)

    if 'temperature_2m_mean' not in df.columns:
        print("[add_anomaly_score] 'temperature_2m_mean' missing.")
        return df

    df['day_of_year'] = df['time'].dt.day_of_year

    # Historical mean per (city, day_of_year)
    doy_means = (
        df.groupby(['city', 'day_of_year'])['temperature_2m_mean']
          .mean()
          .rename('doy_mean_temp')
          .reset_index()
    )

    df = df.merge(doy_means, on=['city', 'day_of_year'], how='left')
    df['anomaly_score'] = df['temperature_2m_mean'] - df['doy_mean_temp']
    df.drop(columns=['doy_mean_temp'], inplace=True)

    print("[add_anomaly_score] Done ✓")
    return df


# ─────────────────────────────────────────────
# 6. Lag features
# ─────────────────────────────────────────────

def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Yesterday's (lag-1) and the day-before-yesterday's (lag-2)
    temperature and precipitation — useful for prediction models.

    New columns:
        temperature_2m_mean_lag1, temperature_2m_mean_lag2
        precipitation_sum_lag1,   precipitation_sum_lag2
    """
    df = _prepare(df)

    lag_targets = ['temperature_2m_mean', 'precipitation_sum']

    for col in lag_targets:
        if col not in df.columns:
            print(f"[add_lag_features] Column '{col}' missing, skipping.")
            continue
        for lag in (1, 2):
            new_col = f"{col}_lag{lag}"
            df[new_col] = (
                df.groupby('city')[col]
                  .transform(lambda x, l=lag: x.shift(l))
            )

    print("[add_lag_features] Done ✓")
    return df


# ─────────────────────────────────────────────
# Master function: compute ALL features
# ─────────────────────────────────────────────

def compute_all_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Runs all feature engineering steps in order and returns
    a fully-featured DataFrame.
    """
    df = add_rolling_averages(df)
    df = add_seasonal_indicators(df)
    df = add_temperature_range(df)
    df = add_degree_days(df)
    df = add_anomaly_score(df)
    df = add_lag_features(df)
    return df


# ─────────────────────────────────────────────
# Populate analytics tables in DuckDB
# ─────────────────────────────────────────────

def populate_analytics_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Reads from staging tables, computes features, writes to analytics tables.

    Expects: staging_historical, staging_forecast
    Creates: analytics_historical, analytics_forecast
    """
    for staging_table in ('staging_historical', 'staging_forecast'):
        analytics_table = staging_table.replace('staging_', 'analytics_')
        print(f"\n{'='*50}")
        print(f"Building features: {staging_table}  →  {analytics_table}")
        print('='*50)

        # Load from staging
        try:
            df = conn.execute(f"SELECT * FROM {staging_table}").df()
        except Exception as e:
            print(f"  [SKIP] Could not load {staging_table}: {e}")
            continue

        print(f"  Loaded {len(df):,} rows from {staging_table}")

        # Compute all features
        df = compute_all_features(df)
        print(f"  Feature engineering complete. Shape: {df.shape}")

        # Write to analytics
        conn.register('_tmp_analytics', df)
        conn.execute(
            f"CREATE OR REPLACE TABLE {analytics_table} AS SELECT * FROM _tmp_analytics"
        )
        print(f"  Analytics table '{analytics_table}' written ✓")

    print("\n[populate_analytics_tables] Done ✓")


# ─────────────────────────────────────────────
# Quick test (run this file directly)
# ─────────────────────────────────────────────
if __name__ == '__main__':
    DB_PATH = os.environ.get('DB_PATH', 'data/weather.duckdb')
    conn = duckdb.connect(DB_PATH)
    populate_analytics_tables(conn)
    conn.close()