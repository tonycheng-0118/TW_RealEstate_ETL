"""
run_etl.py — Main orchestrator (唯一進入點)

Chains download → transform → load → backup. Both cron/LaunchAgent
and Claude Code call this single script.

CLI usage:
    python scripts/run_etl.py                              # 當前季度
    python scripts/run_etl.py --start 113S4 --end 113S4    # 單季
    python scripts/run_etl.py --start 112S1 --end 114S1    # 區間
    python scripts/run_etl.py --start 113S1                # 113S1 到當前
    python scripts/run_etl.py --start 114S1 --end 114S1 --city J      # 指定新竹縣
    python scripts/run_etl.py --start 111S1 --end 112S4 --city J,H    # 多縣市
    python scripts/run_etl.py --backup-only
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Dual-mode path setup: works in both repo layout (scripts/ under project root)
# and skill layout (config.py is a sibling in the same scripts/ folder).
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_script_dir))

import config  # noqa: E402 — found in project root (repo) or scripts/ (skill)

# Repo mode: package-qualified imports (from scripts.xxx).
# Skill mode: direct imports (from xxx) when scripts/ is not a subpackage.
try:
    from scripts.download import download_season, download_current
    from scripts.transform import process_season, process_current
    from scripts.season_utils import get_current_season, resolve_params, season_range
    from scripts.load import (
        get_connection,
        check_already_loaded,
        upsert_transactions,
        upsert_rentals,
        log_etl,
    )
    from scripts.backup import backup_database
except ImportError:
    from download import download_season, download_current
    from transform import process_season, process_current
    from season_utils import get_current_season, resolve_params, season_range
    from load import (
        get_connection,
        check_already_loaded,
        upsert_transactions,
        upsert_rentals,
        log_etl,
    )
    from backup import backup_database

logger = logging.getLogger(__name__)


def setup_logging():
    """Configure logging to both console and log file."""
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOG_DIR / "etl.log"

    # Root logger: DEBUG to file, INFO to console.
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # File handler — captures everything.
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(fh)

    # Console handler — INFO and above.
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(ch)


def _load_dataframes(dataframes: dict, season: str, conn) -> dict:
    """Load all DataFrames for a season into the database.

    Returns a summary dict: {file_name: {"rows": int, "status": str}}.
    Each file is processed independently — a failure in one does not block others.
    """
    summary = {}
    for key, df in dataframes.items():
        # key format: '{city}_{type}', e.g. 'd_a'.
        parts = key.split("_")
        city_code = parts[0]
        file_type = parts[1]
        file_name = f"{city_code}_lvr_land_{file_type}.csv"

        # Check if this file was already successfully loaded for this season.
        if check_already_loaded(season, file_name, conn):
            logger.info("Already loaded %s/%s, skipping", season, file_name)
            summary[file_name] = {"rows": 0, "status": "skipped"}
            continue

        started_at = datetime.now()
        try:
            # Route to the correct upsert function based on file type.
            if file_type in ("a", "b"):
                count = upsert_transactions(df, conn)
            else:
                count = upsert_rentals(df, conn)

            log_etl(season, file_name, count, "success", started_at, conn)
            summary[file_name] = {"rows": count, "status": "success"}

        except Exception as e:
            logger.error("Failed to load %s/%s: %s", season, file_name, e, exc_info=True)
            # Rollback the failed transaction so the connection stays usable.
            conn.rollback()
            log_etl(season, file_name, 0, "failed", started_at, conn)
            summary[file_name] = {"rows": 0, "status": "failed"}

    return summary


def run_etl(seasons: list[str], is_current: bool = False) -> dict:
    """Run the full ETL pipeline for a list of seasons.

    Args:
        seasons: List of season strings (e.g. ['113S1', '113S2']).
                 Ignored if is_current=True.
        is_current: If True, download and process the current period instead.

    Returns:
        Overall summary dict keyed by season.
    """
    overall = {}
    conn = get_connection()

    try:
        if is_current:
            current_season = get_current_season()
            logger.info("=== Processing current period (tagged as %s) ===", current_season)
            try:
                download_current()
                dataframes = process_current()
                summary = _load_dataframes(dataframes, current_season, conn)
                overall[current_season] = summary
            except Exception as e:
                logger.error("Failed to process current period: %s", e, exc_info=True)
                overall[current_season] = {"error": str(e)}
        else:
            for i, season in enumerate(seasons):
                logger.info("=== Processing season %s (%d/%d) ===", season, i + 1, len(seasons))
                try:
                    download_season(season)
                    dataframes = process_season(season)
                    summary = _load_dataframes(dataframes, season, conn)
                    overall[season] = summary
                except Exception as e:
                    logger.error("Failed to process season %s: %s", season, e, exc_info=True)
                    overall[season] = {"error": str(e)}

                # Delay between downloads (skip after last season).
                if i < len(seasons) - 1:
                    logger.info("Waiting %d seconds...", config.DOWNLOAD_DELAY_SEC)
                    time.sleep(config.DOWNLOAD_DELAY_SEC)
    finally:
        conn.close()

    return overall


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="TW RealEstate ETL — Download, transform, load, and backup"
    )
    parser.add_argument("--start", help="Start season, e.g. 113S1. Empty = current.")
    parser.add_argument("--end", help="End season, e.g. 114S4. Empty = start through current.")
    parser.add_argument("--backup-only", action="store_true", help="Only run backup, skip ETL")
    parser.add_argument("--skip-backup", action="store_true", help="Skip backup after ETL")
    parser.add_argument(
        "--city",
        help="Override TARGET_CITY_CODES. Comma-separated city codes, e.g. J or A,J,H. "
             "Use 'all' to download every city. Defaults to config.TARGET_CITY_CODES.",
    )
    args = parser.parse_args()

    setup_logging()

    # Override TARGET_CITY_CODES if --city is provided.
    if args.city:
        if args.city.lower() == "all":
            config.TARGET_CITY_CODES = None
            logger.info("City override: ALL cities")
        else:
            config.TARGET_CITY_CODES = [c.strip().upper() for c in args.city.split(",")]
            logger.info("City override: %s", config.TARGET_CITY_CODES)

    logger.info("TW_RealEstate_ETL started at %s", datetime.now().isoformat())

    # --- ETL ---
    if not args.backup_only:
        # Resolve season parameters
        try:
            start, end, is_current = resolve_params(args.start, args.end)
        except ValueError as e:
            parser.error(str(e))

        logger.info("Resolved: start=%s end=%s is_current=%s", start, end, is_current)

        # Build season list and run
        if is_current:
            overall = run_etl([], is_current=True)
        elif start == end:
            overall = run_etl([start])
        else:
            seasons = season_range(start, end)
            # If end == current season, also download current period for latest data
            current = get_current_season()
            if end == current:
                historical = [s for s in seasons if s != current]
                overall = run_etl(historical) if historical else {}
                current_result = run_etl([], is_current=True)
                overall.update(current_result)
            else:
                overall = run_etl(seasons)

        # Print summary.
        logger.info("=== ETL Summary ===")
        for season, info in overall.items():
            if isinstance(info, dict) and "error" in info:
                logger.error("  %s: ERROR — %s", season, info["error"])
            else:
                for fname, detail in info.items():
                    logger.info("  %s/%s: %s (%d rows)", season, fname, detail["status"], detail["rows"])

    # --- Backup ---
    if not args.skip_backup:
        logger.info("=== Running backup ===")
        try:
            backup_path = backup_database()
            logger.info("Backup saved to: %s", backup_path)
        except Exception as e:
            logger.error("Backup failed: %s", e, exc_info=True)

    logger.info("TW_RealEstate_ETL finished at %s", datetime.now().isoformat())


if __name__ == "__main__":
    main()

