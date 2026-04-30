"""
src/quality_checks.py
Task 3 — Automated Quality Gates

Each check returns a structured dict:
    {
        "check_name": str,
        "stage":      str,
        "status":     "PASS" | "WARN" | "FAIL",
        "details":    str,
    }
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ── Temperature validity window ──────────────────────────────────────────────
TEMP_MIN = -50.0   # °C
TEMP_MAX =  60.0   # °C
TEMP_COLS = [
    "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
    "apparent_temperature_max",
]

# ── Required feature columns produced by features.py ────────────────────────
REQUIRED_FEATURE_COLS = [
    "temperature_2m_mean_7d",  "temperature_2m_mean_30d",
    "precipitation_sum_7d",    "precipitation_sum_30d",
    "month", "quarter", "day_of_year", "season",
    "temperature_range",
    "HDD", "CDD",
    "anomaly_score",
    "temperature_2m_mean_lag1", "temperature_2m_mean_lag2",
    "precipitation_sum_lag1",   "precipitation_sum_lag2",
]

# ─────────────────────────────────────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────────────────────────────────────

def check_row_count(df: pd.DataFrame, stage: str = "raw_load") -> dict:
    """
    FAIL if the DataFrame is empty (0 rows).
    Triggers pipeline abort upstream.
    """
    n = len(df)
    if n == 0:
        status = "FAIL"
        details = "Zero rows loaded — aborting pipeline."
    else:
        status = "PASS"
        details = f"{n:,} rows present."

    result = {"check_name": "row_count", "stage": stage, "status": status, "details": details}
    _log(result)
    return result


def check_null_ratio(df: pd.DataFrame, stage: str = "staging",
                     threshold: float = 0.05) -> dict:
    """
    WARN if any numeric column has > threshold (default 5 %) nulls.
    """
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    bad: list[str] = []
    for col in numeric_cols:
        ratio = df[col].isna().mean()
        if ratio > threshold:
            bad.append(f"{col} ({ratio*100:.1f}%)")

    if bad:
        status  = "WARN"
        details = f"Columns exceeding {threshold*100:.0f}% nulls: {', '.join(bad)}"
    else:
        status  = "PASS"
        details = f"All numeric columns have ≤ {threshold*100:.0f}% nulls."

    result = {"check_name": "null_ratio", "stage": stage, "status": status, "details": details}
    _log(result)
    return result


def check_date_continuity(df: pd.DataFrame, stage: str = "staging",
                           max_gap_days: int = 3) -> dict:
    """
    WARN if any city has a gap > max_gap_days in its date sequence.
    """
    if "time" not in df.columns or "city" not in df.columns:
        result = {"check_name": "date_continuity", "stage": stage,
                  "status": "WARN",
                  "details": "Columns 'time' or 'city' not found; check skipped."}
        _log(result)
        return result

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    problems: list[str] = []

    for city, group in df.groupby("city"):
        dates = group["time"].dt.normalize().sort_values().unique()
        if len(dates) < 2:
            continue
        diffs = pd.Series(dates).diff().dropna().dt.days
        max_gap = diffs.max()
        if max_gap > max_gap_days:
            problems.append(f"{city} (max gap {max_gap:.0f} days)")

    if problems:
        status  = "WARN"
        details = f"Cities with gaps > {max_gap_days} days: {', '.join(problems)}"
    else:
        status  = "PASS"
        details = f"No gaps > {max_gap_days} days found in any city."

    result = {"check_name": "date_continuity", "stage": stage, "status": status, "details": details}
    _log(result)
    return result


def check_value_ranges(df: pd.DataFrame, stage: str = "staging") -> dict:
    """
    FLAG rows where temperature columns are outside [TEMP_MIN, TEMP_MAX].
    Returns WARN + count of flagged rows; does NOT drop them.
    """
    present = [c for c in TEMP_COLS if c in df.columns]
    if not present:
        result = {"check_name": "value_ranges", "stage": stage,
                  "status": "WARN",
                  "details": "No temperature columns found; check skipped."}
        _log(result)
        return result

    mask = (
        df[present].lt(TEMP_MIN) | df[present].gt(TEMP_MAX)
    ).any(axis=1)
    n_bad = mask.sum()

    if n_bad > 0:
        status  = "WARN"
        details = (
            f"{n_bad:,} rows have temperature values outside "
            f"[{TEMP_MIN}°C, {TEMP_MAX}°C]. Rows flagged, not removed."
        )
    else:
        status  = "PASS"
        details = f"All temperature values within [{TEMP_MIN}°C, {TEMP_MAX}°C]."

    result = {"check_name": "value_ranges", "stage": stage, "status": status, "details": details}
    _log(result)
    return result


def check_feature_completeness(df: pd.DataFrame, stage: str = "analytics") -> dict:
    """
    WARN if any required feature column is missing or entirely null.
    """
    missing_cols = [c for c in REQUIRED_FEATURE_COLS if c not in df.columns]
    all_null_cols = [
        c for c in REQUIRED_FEATURE_COLS
        if c in df.columns and df[c].isna().all()
    ]

    issues: list[str] = []
    if missing_cols:
        issues.append(f"Missing columns: {missing_cols}")
    if all_null_cols:
        issues.append(f"All-null columns: {all_null_cols}")

    if issues:
        status  = "WARN"
        details = " | ".join(issues)
    else:
        status  = "PASS"
        details = f"All {len(REQUIRED_FEATURE_COLS)} required feature columns present and non-null."

    result = {"check_name": "feature_completeness", "stage": stage, "status": status, "details": details}
    _log(result)
    return result


def check_freshness(df: pd.DataFrame, stage: str = "raw_load",
                    max_lag_days: int = 2) -> dict:
    """
    WARN if the latest date in the data is more than max_lag_days behind today.
    """
    if "time" not in df.columns:
        result = {"check_name": "freshness", "stage": stage,
                  "status": "WARN",
                  "details": "Column 'time' not found; freshness check skipped."}
        _log(result)
        return result

    latest: date = pd.to_datetime(df["time"]).max().date()
    today:  date = date.today()
    lag    = (today - latest).days

    if lag > max_lag_days:
        status  = "WARN"
        details = (
            f"Latest date in data: {latest}. Today: {today}. "
            f"Lag = {lag} days (threshold = {max_lag_days} days)."
        )
    else:
        status  = "PASS"
        details = f"Data is fresh. Latest date: {latest} (lag {lag} days)."

    result = {"check_name": "freshness", "stage": stage, "status": status, "details": details}
    _log(result)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Batch runner
# ─────────────────────────────────────────────────────────────────────────────

def run_all_checks(
    raw_df:      pd.DataFrame | None = None,
    staging_df:  pd.DataFrame | None = None,
    analytics_df: pd.DataFrame | None = None,
) -> list[dict]:
    """
    Run the appropriate checks for each available DataFrame.
    Returns a list of result dicts.
    """
    results: list[dict] = []

    if raw_df is not None:
        results.append(check_row_count(raw_df,  stage="raw_load"))
        results.append(check_freshness(raw_df,  stage="raw_load"))

    if staging_df is not None:
        results.append(check_row_count(staging_df,       stage="staging"))
        results.append(check_null_ratio(staging_df,      stage="staging"))
        results.append(check_date_continuity(staging_df, stage="staging"))
        results.append(check_value_ranges(staging_df,    stage="staging"))

    if analytics_df is not None:
        results.append(check_row_count(analytics_df,          stage="analytics"))
        results.append(check_feature_completeness(analytics_df, stage="analytics"))

    return results


def print_quality_report(results: list[dict]) -> None:
    """
    Pretty-print the quality gate results as a table.
    """
    icons = {"PASS": "✅", "WARN": "⚠️ ", "FAIL": "❌"}
    sep   = "─" * 90

    print(f"\n{sep}")
    print(f"  {'CHECK':<25}  {'STAGE':<15}  {'STATUS':<6}  DETAILS")
    print(sep)
    for r in results:
        icon = icons.get(r["status"], "?")
        print(
            f"  {r['check_name']:<25}  {r['stage']:<15}  "
            f"{icon} {r['status']:<5}  {r['details']}"
        )
    print(sep)

    n_fail = sum(1 for r in results if r["status"] == "FAIL")
    n_warn = sum(1 for r in results if r["status"] == "WARN")
    n_pass = sum(1 for r in results if r["status"] == "PASS")
    print(f"  TOTAL: {n_pass} PASS  |  {n_warn} WARN  |  {n_fail} FAIL")
    print(f"{sep}\n")

    if n_fail > 0:
        logger.error("Quality gate FAILED — pipeline should be aborted.")
    elif n_warn > 0:
        logger.warning("Quality gate passed with warnings.")
    else:
        logger.info("All quality checks passed.")


# ─────────────────────────────────────────────────────────────────────────────
# Internal helper
# ─────────────────────────────────────────────────────────────────────────────

def _log(result: dict) -> None:
    msg = f"[{result['check_name']}] {result['status']} — {result['details']}"
    if result["status"] == "FAIL":
        logger.error(msg)
    elif result["status"] == "WARN":
        logger.warning(msg)
    else:
        logger.info(msg)
