"""
delete.py — Delete real estate data for specific season(s) and optional city.

Removes matching records from transactions, rentals, and etl_log.
This is a destructive operation — use with care.

CLI usage:
    python scripts/delete.py --start 112S1 --end 112S1
    python scripts/delete.py --start 112S1 --end 113S4
    python scripts/delete.py --start 112S1 --end 112S1 --city A
    python scripts/delete.py --start 112S1 --end 112S1 --city A,F,H
    python scripts/delete.py                          # delete current season
"""

import argparse
import logging
import sys
from pathlib import Path

# Path setup: add project root and scripts/ so imports work
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_script_dir))

import config  # noqa: E402

try:
    from scripts.load import get_connection
    from scripts.season_utils import resolve_params, season_range
except ImportError:
    from load import get_connection  # noqa: E402
    from season_utils import resolve_params, season_range  # noqa: E402

logger = logging.getLogger(__name__)


def count_records(season: str, city_codes: list[str] | None, conn) -> dict:
    """Count records that will be deleted (dry-run preview)."""
    counts = {}
    for table in ["transactions", "rentals"]:
        sql = f"SELECT COUNT(*) FROM {table} WHERE source_season = %s"
        params: list = [season]
        if city_codes:
            placeholders = ",".join(["%s"] * len(city_codes))
            sql += f" AND city_code IN ({placeholders})"
            params.extend(city_codes)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            counts[table] = cur.fetchone()[0]

    # etl_log: filter by season and optionally by file_name prefix (city code)
    sql = "SELECT COUNT(*) FROM etl_log WHERE season = %s"
    params = [season]
    if city_codes:
        # etl_log file_name format: '{city_code}_lvr_land_{type}.csv'
        conditions = " OR ".join(["file_name LIKE %s"] * len(city_codes))
        sql += f" AND ({conditions})"
        params.extend([f"{c}_%%" for c in city_codes])
    with conn.cursor() as cur:
        cur.execute(sql, params)
        counts["etl_log"] = cur.fetchone()[0]

    return counts


def delete_records(season: str, city_codes: list[str] | None, conn) -> dict:
    """Delete records for the given season and optional city codes.

    Returns dict with deleted counts per table.
    """
    deleted = {}
    for table in ["transactions", "rentals"]:
        sql = f"DELETE FROM {table} WHERE source_season = %s"
        params: list = [season]
        if city_codes:
            placeholders = ",".join(["%s"] * len(city_codes))
            sql += f" AND city_code IN ({placeholders})"
            params.extend(city_codes)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            deleted[table] = cur.rowcount

    # etl_log
    sql = "DELETE FROM etl_log WHERE season = %s"
    params = [season]
    if city_codes:
        conditions = " OR ".join(["file_name LIKE %s"] * len(city_codes))
        sql += f" AND ({conditions})"
        params.extend([f"{c}_%%" for c in city_codes])
    with conn.cursor() as cur:
        cur.execute(sql, params)
        deleted["etl_log"] = cur.rowcount

    conn.commit()
    return deleted


def _delete_single_season(season: str, city_codes: list[str] | None, conn) -> None:
    """Delete records for a single season. Logs preview and result."""
    city_label = ",".join(city_codes) if city_codes else "ALL"
    logger.info("=== Deleting season=%s city=%s ===", season, city_label)

    counts = count_records(season, city_codes, conn)
    logger.info("Records to delete:")
    for table, count in counts.items():
        logger.info("  %s: %d rows", table, count)

    total = sum(counts.values())
    if total == 0:
        logger.info("No records found. Skipping.")
        return

    deleted = delete_records(season, city_codes, conn)
    logger.info("Deleted:")
    for table, count in deleted.items():
        logger.info("  %s: %d rows", table, count)


def main():
    parser = argparse.ArgumentParser(
        description="Delete real estate data for specific season(s)"
    )
    parser.add_argument("--start", help="Start season, e.g. 113S1. Empty = current.")
    parser.add_argument("--end", help="End season, e.g. 114S4. Empty = start through current.")
    parser.add_argument(
        "--city",
        help="City codes to delete (comma-separated, e.g. A,F,H). Omit for all cities.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Resolve season parameters
    try:
        start, end, _ = resolve_params(args.start, args.end)
    except ValueError as e:
        parser.error(str(e))

    city_codes = None
    if args.city:
        city_codes = [c.strip().upper() for c in args.city.split(",")]

    # Build season list
    if start == end:
        seasons = [start]
    else:
        seasons = season_range(start, end)

    logger.info("Delete target: %d season(s) [%s ~ %s], city=%s",
                len(seasons), start, end,
                ",".join(city_codes) if city_codes else "ALL")

    conn = get_connection()
    try:
        for season in seasons:
            try:
                _delete_single_season(season, city_codes, conn)
            except Exception as e:
                # Partial delete accepted — log error, continue to next season
                logger.error("Failed to delete season %s: %s", season, e, exc_info=True)
                conn.rollback()
    finally:
        conn.close()

    logger.info("Delete complete.")


if __name__ == "__main__":
    main()
