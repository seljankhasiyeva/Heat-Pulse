"""
pipeline.py
===========
Weather Intelligence Pipeline — Əsas Orkestrator

İstifadə:
    python src/pipeline.py --mode full          # Tam tarixi yenidən yüklə
    python src/pipeline.py --mode incremental   # Yalnız yeni günləri əlavə et
    python src/pipeline.py --mode forecast      # 7 günlük proqnozu yenilə

Axın:
    API/CSV → raw_historical (DuckDB) → staging_historical (cleaning.py)
            → analytics_historical (features.py) → Keyfiyyət yoxlamaları
            → logs/pipeline.log + pipeline_runs (audit)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from datetime import date, timedelta

import pandas as pd

# ── src/ qovluğunu Python yoluna əlavə et ────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

import ingestion      as ingest_mod
import database       as db_mod
import quality_checks as qc

# ── cleaning.py-ı yüklə ─────────────────────────────────────────────────────
try:
    import cleaning as clean_mod
    HAS_CLEANING = True
except ImportError:
    HAS_CLEANING = False

# ── features.py-ı yüklə ─────────────────────────────────────────────────────
try:
    import features as feat_mod
    HAS_FEATURES = True
except ImportError:
    HAS_FEATURES = False


# ─────────────────────────────────────────────────────────────────────────────
# Loglama quraşdırması
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(
    level: int = logging.INFO,
    log_dir: str | Path = "logs",
) -> None:
    """
    Logging-i quraşdırır.
    Notebook-da dəfələrlə çağırıldıqda handler-ləri sıfırlayır.
    """
    log_dir  = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

    # Mövcud handler-ləri təmizlə (notebook-da ikiqat log olmasın)
    root = logging.getLogger()
    root.handlers.clear()

    logging.basicConfig(
        level=level,
        format=fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


logger = logging.getLogger("pipeline")


# ─────────────────────────────────────────────────────────────────────────────
# Mərhələ 1: Məlumatın haradan gəldiyini müəyyən et
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_source(
    mode: str,
    conn,
    start_date: str | None = None,
    cities: list[str] | None = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    İnkremental rejimdə: API-dan yalnız çatışmayan günlər çəkilir.
    Tam rejimdə: əvvəlcə mövcud CSV istifadə edilir (sürətli),
                 CSV yoxdursa API-a müraciət edilir.
    start_date : yalnız 'full' rejimdə API-dan çəkiləndə istifadə olunur.
    cities     : yalnız bu şəhərləri update et (None = hamısı)
    force_update : True olarsa, son tarixdən asılı olmayaraq yenidən çək
    """
    all_cities = ingest_mod.load_cities_from_csv()

    # Filter cities if requested
    if cities:
        cities_lower = [c.lower() for c in cities]
        filtered = {k: v for k, v in all_cities.items() if k.lower() in cities_lower}
        if not filtered:
            logger.warning(f"Heç bir şəhər tapılmadı: {cities}. Bütün şəhərlər istifadə olunur.")
            filtered = all_cities
        cities_dict = filtered
        logger.info(f"Seçilmiş şəhərlər: {list(cities_dict.keys())[:5]}... (cəmi {len(cities_dict)})")
    else:
        cities_dict = all_cities

    logger.info(f"Şəhər sayı: {len(cities_dict)}")

    if mode == "incremental":
        latest = db_mod.get_latest_dates(conn)
        if latest:
            sample = list(latest.items())[:3]
            logger.info(f"Bazadakı son tarixlər (nümunə): {sample}")
        else:
            logger.info("Bazada heç bir tarix yoxdur — ilk yükləmə başlayır.")

        if force_update:
            logger.info("FORCE UPDATE: Son tarixdən asılı olmayaraq yenidən çəkilir...")
            # Reset latest dates to force re-fetch from a recent date
            # Use start_date if provided, otherwise use a date that forces update
            force_start = start_date or "2026-04-01"
            fake_latest = {city: pd.to_datetime(force_start).date() - timedelta(days=1)
                          for city in cities_dict.keys()}
            return ingest_mod.ingest_incremental(latest_dates=fake_latest, cities=cities_dict)

        return ingest_mod.ingest_incremental(latest_dates=latest, cities=cities_dict)

    else:  # full
        hist_csv = ingest_mod.HIST_CSV
        if hist_csv.exists() and not force_update:
            logger.info(f"CSV tapıldı: {hist_csv} — CSV-dən yüklənir.")
            df = ingest_mod.load_historical_from_csv(hist_csv)
            # Filter cities if requested
            if cities:
                df = df[df["city"].str.lower().isin([c.lower() for c in cities])]
            return df
        else:
            sd = start_date or ingest_mod.DEFAULT_START_DATE
            logger.info(f"CSV tapılmadı — API-dan tam yüklənir (start={sd}).")
            return ingest_mod.ingest_all_cities_full(cities=cities_dict, start_date=sd)


# ─────────────────────────────────────────────────────────────────────────────
# Mərhələ 2: DuckDB-yə yazma
# ─────────────────────────────────────────────────────────────────────────────

def _stage_load_raw(conn, raw_df: pd.DataFrame, mode: str, data_dir: Path) -> int:
    logger.info("=== MƏRHƏLƏ 2: XAM MƏLUMAT YÜKLƏNIR ===")
    load_mode = "replace" if mode == "full" else "append"
    n = db_mod.load_raw_historical(conn, raw_df, mode=load_mode)
    # cleaning.py parquet fayllarından oxuduğu üçün parqueti də yenilə
    db_mod.save_raw_as_parquet(conn, data_dir=data_dir)
    return n


# ─────────────────────────────────────────────────────────────────────────────
# Mərhələ 3: Təmizləmə (cleaning.py)
# ─────────────────────────────────────────────────────────────────────────────

def _stage_clean(conn, data_dir: str = "data") -> int:
    logger.info("=== MƏRHƏLƏ 3: TƏMİZLƏMƏ ===")

    if HAS_CLEANING:
        try:
            clean_mod.clean_raw_to_staging(conn, data_dir=data_dir)
            n = db_mod.get_row_count(conn, "staging_historical")
            logger.info(f"staging_historical: {n:,} sətir")
            return n
        except Exception as e:
            logger.error(f"cleaning.py xəta verdi: {e}")
            logger.info("Ehtiyat təmizləmə işlədilir...")

    return _fallback_clean(conn)


def _fallback_clean(conn) -> int:
    """cleaning.py olmadıqda sadə pandas əməliyyatları ilə təmizlə."""
    try:
        raw_df = conn.execute("SELECT * FROM raw_historical").df()
    except Exception as e:
        logger.error(f"raw_historical oxuna bilmədi: {e}")
        return 0

    if raw_df.empty:
        return 0

    raw_df = raw_df.copy()
    raw_df["time"] = pd.to_datetime(raw_df["time"])
    raw_df = raw_df.sort_values(["city", "time"]).drop_duplicates(["city", "time"])

    numeric = [c for c in raw_df.columns
               if c not in ("time", "city", "latitude", "longitude")]
    for col in numeric:
        raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce")
        raw_df[col] = (
            raw_df.groupby("city")[col]
                  .transform(lambda s: s.ffill().bfill())
        )

    conn.register("_staging_fb", raw_df)
    conn.execute("CREATE OR REPLACE TABLE staging_historical AS SELECT * FROM _staging_fb")
    conn.unregister("_staging_fb")

    n = len(raw_df)
    logger.info(f"Ehtiyat təmizləmə: {n:,} sətir → staging_historical")
    return n


# ─────────────────────────────────────────────────────────────────────────────
# Mərhələ 4: Xüsusiyyət mühəndisliyi (features.py)
# ─────────────────────────────────────────────────────────────────────────────

def _stage_features(conn) -> int:
    logger.info("=== MƏRHƏLƏ 4: XÜSUSİYYƏT MÜHƏNDİSLİYİ ===")

    if HAS_FEATURES:
        try:
            feat_mod.populate_analytics_tables(conn)
            n = db_mod.get_row_count(conn, "analytics_historical")
            logger.info(f"analytics_historical: {n:,} sətir")
            return n
        except Exception as e:
            logger.error(f"features.py xəta verdi: {e}")
            logger.info("Ehtiyat xüsusiyyət mühəndisliyi işlədilir...")

    return _fallback_features(conn)


def _fallback_features(conn) -> int:
    """features.py olmadıqda bütün lazımi xüsusiyyətləri hesabla."""
    if not db_mod.table_exists(conn, "staging_historical"):
        return 0
    try:
        df = conn.execute("SELECT * FROM staging_historical").df()
    except Exception as e:
        logger.error(f"staging_historical oxuna bilmədi: {e}")
        return 0

    if df.empty:
        return 0

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values(["city", "time"])
    grp = df.groupby("city")

    # Rolling temperatur
    if "temperature_2m_mean" in df.columns:
        df["temperature_2m_mean_7d"]  = grp["temperature_2m_mean"].transform(
            lambda s: s.rolling(7,  min_periods=1).mean())
        df["temperature_2m_mean_30d"] = grp["temperature_2m_mean"].transform(
            lambda s: s.rolling(30, min_periods=1).mean())
        df["anomaly_score"] = df["temperature_2m_mean"] - df["temperature_2m_mean_30d"]
        df["temperature_2m_mean_lag1"] = grp["temperature_2m_mean"].transform(
            lambda s: s.shift(1))
        df["temperature_2m_mean_lag2"] = grp["temperature_2m_mean"].transform(
            lambda s: s.shift(2))
        df["HDD"] = (18 - df["temperature_2m_mean"]).clip(lower=0)
        df["CDD"] = (df["temperature_2m_mean"] - 18).clip(lower=0)

    # Rolling yağış
    if "precipitation_sum" in df.columns:
        df["precipitation_sum_7d"]  = grp["precipitation_sum"].transform(
            lambda s: s.rolling(7,  min_periods=1).sum())
        df["precipitation_sum_30d"] = grp["precipitation_sum"].transform(
            lambda s: s.rolling(30, min_periods=1).sum())
        df["precipitation_sum_lag1"] = grp["precipitation_sum"].transform(
            lambda s: s.shift(1))
        df["precipitation_sum_lag2"] = grp["precipitation_sum"].transform(
            lambda s: s.shift(2))

    # Temperatur aralığı
    if "temperature_2m_max" in df.columns and "temperature_2m_min" in df.columns:
        df["temperature_range"] = df["temperature_2m_max"] - df["temperature_2m_min"]

    # Təqvim xüsusiyyətləri
    df["month"]       = df["time"].dt.month
    df["quarter"]     = df["time"].dt.quarter
    df["day_of_year"] = df["time"].dt.dayofyear
    df["season"]      = df["time"].dt.month.map({
        12: "winter", 1: "winter",  2: "winter",
        3:  "spring", 4: "spring",  5: "spring",
        6:  "summer", 7: "summer",  8: "summer",
        9:  "autumn", 10: "autumn", 11: "autumn",
    })

    conn.register("_analytics_fb", df)
    conn.execute("CREATE OR REPLACE TABLE analytics_historical AS SELECT * FROM _analytics_fb")
    conn.unregister("_analytics_fb")

    logger.info(f"Ehtiyat xüsusiyyətlər: {len(df):,} sətir → analytics_historical")
    return len(df)


# ─────────────────────────────────────────────────────────────────────────────
# Güncəlləmə hesabatı
# ─────────────────────────────────────────────────────────────────────────────

def _print_update_report(conn, raw_df: pd.DataFrame) -> None:
    """Hər şəhər üçün neçə yeni gün əlavə edildi — cədvəl şəklində göstər."""
    if raw_df.empty:
        print("\n  Yeni məlumat yoxdur — baza artıq aktual idi.")
        return

    raw_df = raw_df.copy()
    raw_df["time"] = pd.to_datetime(raw_df["time"])

    summary = (
        raw_df.groupby("city")
              .agg(
                  yeni_gunler=("time", "count"),
                  ilk_yeni=("time", "min"),
                  son_yeni=("time", "max"),
              )
              .reset_index()
    )
    summary["ilk_yeni"] = summary["ilk_yeni"].dt.strftime("%Y-%m-%d")
    summary["son_yeni"] = summary["son_yeni"].dt.strftime("%Y-%m-%d")
    summary = summary.sort_values("yeni_gunler", ascending=False)

    print("\n" + "=" * 65)
    print("  YENİLƏNƏN MƏLUMAT HESABATI")
    print("=" * 65)
    print(f"{'Şəhər':<20} {'Yeni Gün':>9} {'İlk Tarix':<14} {'Son Tarix'}")
    print("-" * 65)
    for _, row in summary.iterrows():
        print(f"{row['city']:<20} {row['yeni_gunler']:>9,}    "
              f"{row['ilk_yeni']:<14} {row['son_yeni']}")
    print("=" * 65)
    print(f"  Cəmi: {summary['yeni_gunler'].sum():,} yeni sətir, "
          f"{len(summary)} şəhər\n")

    try:
        db_dates = conn.execute(
            "SELECT city, MAX(time) as son_tarix FROM raw_historical GROUP BY city"
        ).df()
        print("  Bazadakı son tarixlər (ilk 10 şəhər):")
        today = date.today()
        db_dates["bosluq_gun"] = (
            today - pd.to_datetime(db_dates["son_tarix"]).dt.date
        ).apply(lambda x: x.days if hasattr(x, "days") else x)
        print(db_dates.head(10).to_string(index=False))
        print()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Əsas pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(
    mode: str = "incremental",
    data_dir: str | Path = "data",
    db_path: str | Path = "data/weather.duckdb",
    log_dir: str | Path = "logs",
    start_date: str | None = None,
    cities: list[str] | None = None,
    force_update: bool = False,
) -> dict:
    """
    Pipeline-ı icra edir.

    Parameters
    ----------
    mode       : "full" | "incremental" | "forecast"
    data_dir   : data qovluğunun yolu (cleaning.py parquetləri buradan oxuyur)
    db_path    : DuckDB fayl yolu
    log_dir    : log qovluğunun yolu
    start_date : 'full' rejim üçün başlanğıc tarixi YYYY-MM-DD
                 (default: "2020-01-01", yalnız API-dan çəkiləndə aktiv)

    cities     : yalnız bu şəhərləri update et (None = hamısı)
    force_update : True olarsa, son tarixdən asılı olmayaraq yenidən çək

    Returns
    -------
    dict ilə açarlar:
        status, mode, rows_raw, rows_staging, rows_analytics,
        duration_sec, quality_checks, cities_skipped, rows_ingested
    """
    start_time = time.time()
    data_dir   = Path(data_dir)
    db_path    = Path(db_path)

    logger.info("=" * 65)
    city_info = f" | şəhərlər={cities}" if cities else ""
    force_info = " | FORCE" if force_update else ""
    logger.info(f"PİPELİNE BAŞLADI  |  rejim={mode}{city_info}{force_info}  |  tarix={date.today()}")
    logger.info("=" * 65)

    conn = db_mod.get_connection(db_path)
    db_mod.create_schema(conn)

    rows_raw  = 0
    rows_stg  = 0
    rows_ana  = 0
    status    = "SUCCESS"
    notes     = []
    raw_df    = pd.DataFrame()
    checks_df = pd.DataFrame()

    try:
        # ── Proqnoz rejimi ─────────────────────────────────────────────────────
        if mode == "forecast":
            logger.info("=== MƏRHƏLƏ 1: PROQNOZ ÇƏKİLİR ===")
            all_cities = ingest_mod.load_cities_from_csv()
            if cities:
                cities_lower = [c.lower() for c in cities]
                cities_dict = {k: v for k, v in all_cities.items() if k.lower() in cities_lower}
            else:
                cities_dict = all_cities
            fore_df  = ingest_mod.ingest_all_forecasts(cities=cities_dict)
            rows_raw = db_mod.load_raw_forecast(conn, fore_df)
            db_mod.save_raw_as_parquet(conn, data_dir=data_dir)
            checks_df = qc.run_all_checks(conn)
            qc.print_check_summary(checks_df)
            duration = time.time() - start_time
            db_mod.log_pipeline_run(
                conn, mode=mode, cities_count=len(cities),
                rows_raw=rows_raw, rows_staging=0, rows_analytics=0,
                duration_sec=duration, status="SUCCESS",
            )
            conn.close()
            return {
                "status": "SUCCESS", "mode": mode,
                "rows_raw": rows_raw, "rows_staging": 0, "rows_analytics": 0,
                "duration_sec": round(duration, 2),
                "quality_checks": checks_df,
                "cities_skipped": 0, "rows_ingested": rows_raw,
            }

        # ── Mərhələ 1: Məlumatı tap / çək ────────────────────────────────────
        logger.info("=== MƏRHƏLƏ 1: MƏLUMAT MƏNBƏYI MÜƏYYƏNLƏŞDİRİLİR ===")
        raw_df = _resolve_source(mode, conn, start_date=start_date, cities=cities, force_update=force_update)

        if raw_df.empty and mode == "incremental":
            logger.info("İnkremental rejim: yeni məlumat yoxdur — baza aktual!")
            _print_update_report(conn, raw_df)
            checks_df = qc.run_all_checks(conn)
            qc.print_check_summary(checks_df)
            duration = time.time() - start_time
            cities   = ingest_mod.load_cities_from_csv()
            db_mod.log_pipeline_run(
                conn, mode=mode, cities_count=len(cities),
                rows_raw=0, rows_staging=0, rows_analytics=0,
                duration_sec=duration, status="UP_TO_DATE",
                notes="Yeni məlumat yoxdur.",
            )
            conn.close()
            return {
                "status": "UP_TO_DATE", "mode": mode,
                "rows_raw": 0, "rows_staging": 0, "rows_analytics": 0,
                "duration_sec": round(duration, 2),
                "quality_checks": checks_df,
                "cities_skipped": len(cities), "rows_ingested": 0,
            }

        logger.info(f"Məlumat alındı: {len(raw_df):,} sətir")

        # ── Mərhələ 2: DuckDB-yə yüklə ───────────────────────────────────────
        rows_raw = _stage_load_raw(conn, raw_df, mode, data_dir=data_dir)

        # ── Keyfiyyət yoxlaması: sətir sayı ──────────────────────────────────
        # check_row_count(conn, "table_name") → _to_df() daxildə conn-dan oxuyur
        rc = qc.check_row_count(conn, "raw_historical")
        if rc["status"] == "FAIL":
            raise RuntimeError("Sətir sayı sıfırdır — pipeline dayandırıldı.")

        # ── Mərhələ 3: Təmizlə ────────────────────────────────────────────────
        rows_stg = _stage_clean(conn, data_dir=str(data_dir))

        # ── Mərhələ 4: Xüsusiyyətlər ─────────────────────────────────────────
        rows_ana = _stage_features(conn)

        # ── Mərhələ 5: Keyfiyyət yoxlamaları ─────────────────────────────────
        logger.info("=== MƏRHƏLƏ 5: KEYFİYYƏT YOXLAMALARI ===")
        checks_df = qc.run_all_checks(conn)
        qc.print_check_summary(checks_df)

        # ── Güncəlləmə hesabatı ───────────────────────────────────────────────
        _print_update_report(conn, raw_df)

        # ── Cədvəl icmalı ─────────────────────────────────────────────────────
        summary_df = db_mod.get_table_summary(conn)
        logger.info(f"\n{summary_df.to_string(index=False)}")
        print("\n" + summary_df.to_string(index=False))

    except RuntimeError as e:
        status = "ABORTED"
        notes.append(str(e))
        logger.error(f"Pipeline DAYANDIRIDI: {e}")

    except Exception as e:
        status = "ERROR"
        notes.append(str(e))
        logger.exception(f"Pipeline XƏTƏSİ: {e}")

    finally:
        duration = time.time() - start_time
        cities   = ingest_mod.load_cities_from_csv()

        try:
            db_mod.log_pipeline_run(
                conn,
                mode=mode,
                cities_count=len(cities),
                rows_raw=rows_raw,
                rows_staging=rows_stg,
                rows_analytics=rows_ana,
                duration_sec=duration,
                status=status,
                notes="; ".join(notes),
            )
        except Exception as log_err:
            logger.warning(f"Audit loqu yazıla bilmədi: {log_err}")

        try:
            conn.close()
        except Exception:
            pass

        logger.info(f"Pipeline bitdi: {duration:.1f} saniyə | status={status}")

    # cities_skipped hesabla
    cities_all      = ingest_mod.load_cities_from_csv()
    cities_updated  = raw_df["city"].nunique() if ("city" in raw_df.columns and not raw_df.empty) else 0
    cities_skipped  = max(0, len(cities_all) - cities_updated)

    return {
        "status":          status,
        "mode":            mode,
        "rows_raw":        rows_raw,
        "rows_staging":    rows_stg,
        "rows_analytics":  rows_ana,
        "duration_sec":    round(duration, 2),
        "quality_checks":  checks_df,
        "cities_skipped":  cities_skipped,
        "rows_ingested":   rows_raw,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Komanda sətri interfeysi
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Weather Intelligence Pipeline — Heat-Pulse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Nümunələr:
  python src/pipeline.py --mode full                           # Hamısını yenidən yüklə
  python src/pipeline.py --mode incremental                    # Yalnız yeni günlər
  python src/pipeline.py --mode incremental --cities Baku Ganja  # Yalnız bu şəhərlər
  python src/pipeline.py --mode incremental --force-update       # Son tarixdən asılı olmayaraq yenidən çək
  python src/pipeline.py --mode forecast                       # 7 günlük proqnozu yenilə
  python src/pipeline.py --mode full --start-date 2023-01-01
  python src/pipeline.py --mode incremental --log-level DEBUG
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental", "forecast"],
        default="incremental",
        help="Pipeline rejimi (default: incremental)",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Data qovluğunun yolu (default: data)",
    )
    parser.add_argument(
        "--db-path",
        default="data/weather.duckdb",
        help="DuckDB fayl yolu (default: data/weather.duckdb)",
    )
    parser.add_argument(
        "--log-dir",
        default="logs",
        help="Log qovluğunun yolu (default: logs)",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Tam yükləmə üçün başlanğıc tarixi YYYY-MM-DD (default: 2020-01-01)",
    )
    parser.add_argument(
        "--cities",
        nargs="+",
        default=None,
        help="Yalnız bu şəhərləri update et (məsələn: --cities Baku Ganja)",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Son tarixdən asılı olmayaraq yenidən çək (force)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Log səviyyəsi (default: INFO)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    setup_logging(
        level=getattr(logging, args.log_level),
        log_dir=args.log_dir,
    )

    summary = run_pipeline(
        mode=args.mode,
        data_dir=args.data_dir,
        db_path=args.db_path,
        log_dir=args.log_dir,
        start_date=args.start_date,
        cities=args.cities,
        force_update=args.force_update,
    )

    print("\n" + "=" * 65)
    print("  PİPELİNE NƏTİCƏSİ")
    print("=" * 65)
    print(f"  Rejim           : {summary['mode']}")
    print(f"  Status          : {summary['status']}")
    print(f"  Xam sətir       : {summary.get('rows_raw', 0):,}")
    print(f"  Staging sətir   : {summary.get('rows_staging', 0):,}")
    print(f"  Analytics sətir : {summary.get('rows_analytics', 0):,}")
    print(f"  Yeni sətir      : {summary.get('rows_ingested', 0):,}")
    print(f"  Atlanmış şəhər  : {summary.get('cities_skipped', 0)}")
    print(f"  Müddət          : {summary['duration_sec']} saniyə")
    print("=" * 65)

    if summary["status"] in ("ERROR", "ABORTED"):
        sys.exit(1)