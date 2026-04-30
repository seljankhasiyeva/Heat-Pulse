"""
src/quality_checks.py
=====================
Automated Quality Gates — Heat-Pulse Weather Intelligence Pipeline

Hər check aşağıdakı dict qaytarır:
    {
        "check_name": str,
        "stage":      str,
        "status":     "PASS" | "WARN" | "FAIL",
        "details":    str,
    }

PUBLIC API
──────────
Individual checks  — hər biri HƏM (conn, "table_name") HƏM (df, "stage") qəbul edir:
    check_row_count(conn_or_df, table_or_stage)
    check_null_ratio(conn_or_df, table_or_stage, threshold=0.05)
    check_date_continuity(conn_or_df, table_or_stage, max_gap_days=3)
    check_value_ranges(conn_or_df, table_or_stage)
    check_feature_completeness(conn_or_df, table_or_stage)
    check_freshness(conn_or_df, table_or_stage, max_lag_days=2)

Batch runner — iki müxtəlif çağırış şəkli:
    # pipeline.py stili — yalnız conn göndər, cədvəlləri özü oxuyur
    results_df = run_all_checks(conn)

    # notebook/test stili — DataFramelər birbaşa ötürülür
    results_df = run_all_checks(
        conn,                        # DuckDB conn (None da ola bilər)
        raw_df=raw_df,
        staging_df=staging_df,
        analytics_df=analytics_df,
    )

Pretty-print:
    print_quality_report(results_df)   # DataFrame və ya list[dict] qəbul edir
    print_check_summary(results_df)    # eyni funksiya, alias
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Union

import pandas as pd

# duckdb import-u könüllü: modul mövcud deyilsə da işləyir
try:
    import duckdb
    _HAS_DUCKDB = True
except ImportError:
    duckdb = None            # type: ignore[assignment]
    _HAS_DUCKDB = False

logger = logging.getLogger(__name__)

# ── Temperatur etibarlılıq hədləri ──────────────────────────────────────────
TEMP_MIN  = -50.0   # °C
TEMP_MAX  =  60.0   # °C
TEMP_COLS = [
    "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
    "apparent_temperature_max",
]

# ── features.py-ın yaratmalı olduğu sütunlar ────────────────────────────────
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
# Daxili köməkçi: conn+cədvəl adı YA DA df+label → (df, label)
# ─────────────────────────────────────────────────────────────────────────────

def _to_df(
    conn_or_df,
    table_or_stage: str,
) -> tuple[pd.DataFrame, str]:
    """
    İki çağırış şəklini normallaşdırır:
      • (conn, "raw_historical")  → cədvəli oxuyur, ("raw_historical", df) qaytarır
      • (df,   "stage_label")     → df-i birbaşa istifadə edir

    conn None olarsa boş DataFrame qaytarır (cədvəl mövcud deyil kimi davranır).
    """
    if isinstance(conn_or_df, pd.DataFrame):
        return conn_or_df, table_or_stage

    # DuckDB connection (və ya None)
    conn  = conn_or_df
    table = table_or_stage

    if conn is None:
        logger.warning(f"_to_df: conn=None, '{table}' oxuna bilmir.")
        return pd.DataFrame(), table

    try:
        df = conn.execute(f"SELECT * FROM {table}").df()
    except Exception as exc:
        logger.warning(f"_to_df: '{table}' oxunarkən xəta: {exc}")
        df = pd.DataFrame()

    return df, table


def _safe_read(conn, table: str) -> pd.DataFrame | None:
    """
    Cədvəli oxuyur.
    Cədvəl mövcud deyilsə və ya boşdursa None qaytarır (check keçilir).
    """
    if conn is None:
        return None
    try:
        df = conn.execute(f"SELECT * FROM {table}").df()
        return df if not df.empty else None
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────────────────────────────────────

def check_row_count(
    conn_or_df,
    table_or_stage: str = "raw_load",
) -> dict:
    """
    FAIL — cədvəl / DataFrame boşdursa (0 sətir).
    Həm (conn, "table_name") həm (df, "stage") qəbul edir.
    """
    df, stage = _to_df(conn_or_df, table_or_stage)
    n = len(df)
    if n == 0:
        status  = "FAIL"
        details = "Zero rows — pipeline aborted."
    else:
        status  = "PASS"
        details = f"{n:,} rows present."

    result = {"check_name": "row_count", "stage": stage,
              "status": status, "details": details}
    _log(result)
    return result


def check_null_ratio(
    conn_or_df,
    table_or_stage: str = "staging",
    threshold: float = 0.05,
) -> dict:
    """WARN — hər hansı ədədi sütunda > threshold (default 5 %) null varsa."""
    df, stage = _to_df(conn_or_df, table_or_stage)
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    bad: list[str] = []
    for col in numeric_cols:
        ratio = df[col].isna().mean()
        if ratio > threshold:
            bad.append(f"{col} ({ratio * 100:.1f}%)")

    if bad:
        status  = "WARN"
        details = f"Columns exceeding {threshold * 100:.0f}% nulls: {', '.join(bad)}"
    else:
        status  = "PASS"
        details = f"All numeric columns have ≤ {threshold * 100:.0f}% nulls."

    result = {"check_name": "null_ratio", "stage": stage,
              "status": status, "details": details}
    _log(result)
    return result


def check_date_continuity(
    conn_or_df,
    table_or_stage: str = "staging",
    max_gap_days: int = 3,
) -> dict:
    """WARN — hər hansı şəhərdə max_gap_days-dən böyük boşluq varsa."""
    df, stage = _to_df(conn_or_df, table_or_stage)

    if df.empty or "time" not in df.columns or "city" not in df.columns:
        result = {
            "check_name": "date_continuity", "stage": stage,
            "status": "WARN",
            "details": "Columns 'time' or 'city' not found; check skipped.",
        }
        _log(result)
        return result

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    problems: list[str] = []

    for city, group in df.groupby("city"):
        dates   = group["time"].dt.normalize().sort_values().unique()
        if len(dates) < 2:
            continue
        diffs   = pd.Series(dates).diff().dropna().dt.days
        max_gap = diffs.max()
        if max_gap > max_gap_days:
            problems.append(f"{city} (max gap {max_gap:.0f} days)")

    if problems:
        status  = "WARN"
        details = f"Cities with gaps > {max_gap_days} days: {', '.join(problems)}"
    else:
        status  = "PASS"
        details = f"No gaps > {max_gap_days} days found in any city."

    result = {"check_name": "date_continuity", "stage": stage,
              "status": status, "details": details}
    _log(result)
    return result


def check_value_ranges(
    conn_or_df,
    table_or_stage: str = "staging",
) -> dict:
    """Temperatur sütunları [TEMP_MIN, TEMP_MAX] xaricindəki sətirləri işarələ."""
    df, stage = _to_df(conn_or_df, table_or_stage)
    present   = [c for c in TEMP_COLS if c in df.columns]

    if not present or df.empty:
        result = {
            "check_name": "value_ranges", "stage": stage,
            "status": "WARN",
            "details": "No temperature columns found; check skipped.",
        }
        _log(result)
        return result

    mask  = (df[present].lt(TEMP_MIN) | df[present].gt(TEMP_MAX)).any(axis=1)
    n_bad = int(mask.sum())

    if n_bad > 0:
        status  = "WARN"
        details = (
            f"{n_bad:,} rows have temperature values outside "
            f"[{TEMP_MIN}°C, {TEMP_MAX}°C]. Flagged, not removed."
        )
    else:
        status  = "PASS"
        details = f"All temperature values within [{TEMP_MIN}°C, {TEMP_MAX}°C]."

    result = {"check_name": "value_ranges", "stage": stage,
              "status": status, "details": details}
    _log(result)
    return result


def check_feature_completeness(
    conn_or_df,
    table_or_stage: str = "analytics",
) -> dict:
    """WARN — tələb olunan xüsusiyyət sütunları yoxdursa və ya tam null-dursa."""
    df, stage = _to_df(conn_or_df, table_or_stage)

    missing_cols  = [c for c in REQUIRED_FEATURE_COLS if c not in df.columns]
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
        details = (
            f"All {len(REQUIRED_FEATURE_COLS)} required feature columns "
            "present and non-null."
        )

    result = {"check_name": "feature_completeness", "stage": stage,
              "status": status, "details": details}
    _log(result)
    return result


def check_freshness(
    conn_or_df,
    table_or_stage: str = "raw_load",
    max_lag_days: int = 2,
) -> dict:
    """WARN — datanın son tarixi bugündən max_lag_days gündən çox geridədirsə."""
    df, stage = _to_df(conn_or_df, table_or_stage)

    if df.empty or "time" not in df.columns:
        result = {
            "check_name": "freshness", "stage": stage,
            "status": "WARN",
            "details": "Column 'time' not found or table empty; freshness check skipped.",
        }
        _log(result)
        return result

    latest: date = pd.to_datetime(df["time"]).max().date()
    today:  date = date.today()
    lag           = (today - latest).days

    if lag > max_lag_days:
        status  = "WARN"
        details = (
            f"Latest date in data: {latest}. Today: {today}. "
            f"Lag = {lag} days (threshold = {max_lag_days} days)."
        )
    else:
        status  = "PASS"
        details = f"Data is fresh. Latest date: {latest} (lag {lag} days)."

    result = {"check_name": "freshness", "stage": stage,
              "status": status, "details": details}
    _log(result)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Batch runner
# ─────────────────────────────────────────────────────────────────────────────

def run_all_checks(
    conn=None,
    raw_df:       pd.DataFrame | None = None,
    staging_df:   pd.DataFrame | None = None,
    analytics_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Bütün pipeline mərhələləri üçün keyfiyyət yoxlamalarını icra edir.

    İki çağırış şəkli:

    1. pipeline.py stili (conn ilə, DuckDB-dən özü oxuyur):
        results_df = run_all_checks(conn)

    2. notebook/test stili (DataFrame-lər birbaşa ötürülür):
        results_df = run_all_checks(
            conn=None,           # və ya conn (lazım olsa)
            raw_df=raw_df,
            staging_df=staging_df,
            analytics_df=analytics_df,
        )

    Hər iki şəkil eyni vaxtda da istifadə oluna bilər:
        results_df = run_all_checks(conn, raw_df=my_df)

    Qaytarır: check_name, stage, status, details sütunları olan DataFrame.
    """
    results: list[dict] = []

    # ── raw_historical ────────────────────────────────────────────────────────
    _raw = raw_df if raw_df is not None else _safe_read(conn, "raw_historical")
    if _raw is not None and not _raw.empty:
        results.append(check_row_count(_raw,  table_or_stage="raw_historical"))
        results.append(check_freshness(_raw,  table_or_stage="raw_historical"))
    elif _raw is not None and _raw.empty:
        results.append(check_row_count(_raw,  table_or_stage="raw_historical"))

    # ── staging_historical ────────────────────────────────────────────────────
    _stg = staging_df if staging_df is not None else _safe_read(conn, "staging_historical")
    if _stg is not None and not _stg.empty:
        results.append(check_row_count(_stg,          table_or_stage="staging_historical"))
        results.append(check_null_ratio(_stg,          table_or_stage="staging_historical"))
        results.append(check_date_continuity(_stg,     table_or_stage="staging_historical"))
        results.append(check_value_ranges(_stg,        table_or_stage="staging_historical"))

    # ── analytics_historical ──────────────────────────────────────────────────
    _ana = analytics_df if analytics_df is not None else _safe_read(conn, "analytics_historical")
    if _ana is not None and not _ana.empty:
        results.append(check_row_count(_ana,              table_or_stage="analytics_historical"))
        results.append(check_feature_completeness(_ana,   table_or_stage="analytics_historical"))

    if not results:
        logger.warning("run_all_checks: heç bir cədvəl/DataFrame tapılmadı.")

    return pd.DataFrame(results) if results else pd.DataFrame(
        columns=["check_name", "stage", "status", "details"]
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pretty-print
# ─────────────────────────────────────────────────────────────────────────────

def print_quality_report(results: list[dict] | pd.DataFrame) -> None:
    """Keyfiyyət yoxlaması nəticələrini cədvəl şəklində ekrana çıxarır."""
    if isinstance(results, pd.DataFrame):
        if results.empty:
            print("\n  [Keyfiyyət yoxlaması: heç bir nəticə yoxdur]\n")
            return
        results = results.to_dict("records")

    if not results:
        print("\n  [Keyfiyyət yoxlaması: heç bir nəticə yoxdur]\n")
        return

    icons = {"PASS": "✅", "WARN": "⚠️ ", "FAIL": "❌"}
    sep   = "─" * 95

    print(f"\n{sep}")
    print(f"  {'CHECK':<28}  {'STAGE':<25}  {'STATUS':<6}  DETAILS")
    print(sep)
    for r in results:
        icon = icons.get(r.get("status", "?"), "?")
        print(
            f"  {r.get('check_name',''):<28}  {r.get('stage',''):<25}  "
            f"{icon} {r.get('status',''):<5}  {r.get('details','')}"
        )
    print(sep)

    n_fail = sum(1 for r in results if r.get("status") == "FAIL")
    n_warn = sum(1 for r in results if r.get("status") == "WARN")
    n_pass = sum(1 for r in results if r.get("status") == "PASS")
    print(f"  TOTAL: {n_pass} PASS  |  {n_warn} WARN  |  {n_fail} FAIL")
    print(f"{sep}\n")

    if n_fail > 0:
        logger.error("Quality gate FAILED — pipeline should be aborted.")
    elif n_warn > 0:
        logger.warning("Quality gate passed with warnings.")
    else:
        logger.info("All quality checks passed.")


# Alias — pipeline.py bu adla çağırır
print_check_summary = print_quality_report


# ─────────────────────────────────────────────────────────────────────────────
# Daxili köməkçi
# ─────────────────────────────────────────────────────────────────────────────

def _log(result: dict) -> None:
    msg = f"[{result['check_name']}] {result['status']} — {result['details']}"
    if result["status"] == "FAIL":
        logger.error(msg)
    elif result["status"] == "WARN":
        logger.warning(msg)
    else:
        logger.info(msg)