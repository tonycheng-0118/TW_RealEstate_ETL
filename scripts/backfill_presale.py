"""
backfill_presale.py — Fill project_name / building_unit / cancel_status on existing pre-sale rows

Reads the b CSVs already on disk (no download) and UPDATEs the three columns of
rows that are already in transactions, matched by serial_no. Never inserts rows
and never touches other columns. Columns a file does not have (建案名稱/棟及號
before 107S1, 解約情形 before 112S3) are left as they are.

Files run oldest season first, then current/ releases by mtime, so the newest
file wins when the same serial_no appears more than once.

CLI usage:
    python scripts/backfill_presale.py --city D,J,O
    python scripts/backfill_presale.py --city D,J,O --data-dir data --data-dir ~/.claude/skills/tw-realestate-etl/data
    python scripts/backfill_presale.py --city all --dry-run
    python scripts/backfill_presale.py --city D --season 115S3 --since 1763800000

--season reads only <data_dir>/<season>/ and fails (exit 4) unless every target file
comes from a <season>.zip modified at or after --since (epoch seconds) and matched at
least one row. Use it after re-downloading a season to prove the new file was applied.

Exit codes: 0 ok, 1 error (files before the failing one stay committed), 2 bad arguments,
4 --season check failed.
"""

import argparse
import logging
import re
import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_script_dir.parent))
sys.path.insert(0, str(_script_dir))

import config  # noqa: E402

try:
    from scripts.load import TRANSACTION_ADDED_COLUMNS, ensure_schema, get_connection
    from scripts.transform import CSV_PATTERN, _apply_column_map, _skip_english_header, read_csv_with_encoding
except ImportError:
    from load import TRANSACTION_ADDED_COLUMNS, ensure_schema, get_connection
    from transform import CSV_PATTERN, _apply_column_map, _skip_english_header, read_csv_with_encoding

logger = logging.getLogger(__name__)

SEASON_DIR = re.compile(r"^(\d{3})S([1-4])$")


def _order_key(path: Path) -> tuple:
    """Seasons ascending, then current/ releases by file mtime."""
    m = SEASON_DIR.match(path.parent.name)
    if m:
        return (0, int(m.group(1)), int(m.group(2)), 0.0)
    return (1, 0, 0, path.stat().st_mtime)


def find_b_files(data_dirs: list[Path], cities, season: str | None = None) -> list[Path]:
    """List <city>_lvr_land_b.csv in each data dir's season folders and current/.

    Other folders (e.g. a renamed '114S1.old' backup) are skipped: they would sort
    after the seasons and overwrite newer values. cities: codes (any case) or None for all.
    season: only that season's folder (no current/).
    """
    wanted = None if cities is None else {c.upper() for c in cities}
    found = []
    for data_dir in data_dirs:
        if not data_dir.is_dir():
            logger.warning("Data dir not found, skipping: %s", data_dir)
            continue
        for path in data_dir.glob("*/*"):
            folder = path.parent.name
            if season is not None:
                if folder != season:
                    continue
            elif not (SEASON_DIR.match(folder) or folder == "current"):
                continue
            m = CSV_PATTERN.match(path.name)
            if not m or m.group(2).lower() != "b":
                continue
            if wanted is not None and m.group(1).upper() not in wanted:
                continue
            found.append(path)
    return sorted(found, key=_order_key)


def stale_files(files: list[Path], since: float) -> list[Path]:
    """Files whose <data_dir>/<season>.zip is missing or older than since (epoch seconds).

    Judged by the zip, not the CSV: run_etl re-extracts the zip on every run, so the
    CSV mtime is fresh even when the zip is the old download.
    """
    stale = []
    for path in files:
        zip_path = path.parent.parent / f"{path.parent.name}.zip"
        if not zip_path.is_file() or zip_path.stat().st_mtime < since:
            stale.append(path)
    return stale


def read_updates(csv_path: Path) -> tuple[list[str], list[tuple]]:
    """Return (columns, rows) to update; each row is (serial_no, *column values).

    columns is the subset of TRANSACTION_ADDED_COLUMNS the file has. Empty cells → None.
    """
    df = _skip_english_header(read_csv_with_encoding(csv_path))
    df = _apply_column_map(df, config.COLUMN_MAP_B)
    cols = [c for c in TRANSACTION_ADDED_COLUMNS if c in df.columns]
    if not cols or "serial_no" not in df.columns:
        return [], []

    df = df[df["serial_no"].notna() & (df["serial_no"].str.strip() != "")]
    df = df[["serial_no", *cols]]
    df = df.astype(object).where(df.notna(), None)
    return cols, list(df.itertuples(index=False, name=None))


def _update_sql(cols: list[str]) -> str:
    set_clause = ", ".join(f"{c} = v.{c}" for c in cols)
    names = ", ".join(["serial_no", *cols])
    return (
        f"UPDATE transactions AS t SET {set_clause}, updated_at = NOW() "
        f"FROM (VALUES %s) AS v ({names}) WHERE t.serial_no = v.serial_no"
    )


def backfill(files: list[Path], conn, dry_run: bool = False) -> dict:
    """UPDATE the new columns from each file in order; commit per file.

    rows_updated counts matched rows per file, so a serial_no present in two
    files counts twice.
    """
    from psycopg2.extras import execute_values

    stats = {"files": 0, "rows_read": 0, "rows_updated": 0}
    for path in files:
        cols, rows = read_updates(path)
        if not cols:
            logger.info("%s: no pre-sale columns, skipped", path)
            continue
        stats["files"] += 1
        stats["rows_read"] += len(rows)
        if dry_run or not rows:
            logger.info("%s: %d rows, columns %s%s", path, len(rows), cols, " (dry run)" if dry_run else "")
            continue
        updated = 0
        with conn.cursor() as cur:
            # One statement per page so rowcount is exact (execute_values keeps only the last page's).
            for i in range(0, len(rows), config.BATCH_SIZE):
                execute_values(cur, _update_sql(cols), rows[i : i + config.BATCH_SIZE],
                               page_size=config.BATCH_SIZE)
                updated += cur.rowcount
        conn.commit()
        stats["rows_updated"] += updated
        logger.info("%s: %d rows read, %d matched, columns %s", path, len(rows), updated, cols)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill pre-sale project name/unit/cancel status")
    parser.add_argument("--city", help="Comma-separated city codes, or 'all'. Defaults to config.TARGET_CITY_CODES.")
    parser.add_argument("--data-dir", action="append", type=Path,
                        help="Folder holding <season>/ and current/ CSVs. Repeatable. Defaults to config.DATA_DIR.")
    parser.add_argument("--season", help="Only this season (e.g. 115S3); needs --since. Exit 4 if not proven updated.")
    parser.add_argument("--since", type=int,
                        help="Epoch seconds taken before re-downloading --season; its zip must be at least this new")
    parser.add_argument("--dry-run", action="store_true", help="Read files and report counts, no DB writes")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    season = args.season.upper() if args.season is not None else None
    if (season is None) != (args.since is None):
        parser.error("--season and --since go together")
    if season is not None and not SEASON_DIR.match(season):
        parser.error(f"--season must look like 115S3, got {args.season!r}")

    if args.city:
        cities = None if args.city.lower() == "all" else [c.strip() for c in args.city.split(",") if c.strip()]
    else:
        cities = config.TARGET_CITY_CODES
    data_dirs = [d.expanduser() for d in (args.data_dir or [config.DATA_DIR])]

    files = find_b_files(data_dirs, cities, season)
    logger.info("Found %d b file(s) for cities %s in %s", len(files), cities or "ALL", data_dirs)
    if season is not None:
        if not files:
            logger.error("Season check failed: no %s b file for cities %s", season, cities or "ALL")
            return 4
        stale = stale_files(files, args.since)
        for path in stale:
            logger.error("Season check failed: %s.zip is missing or older than --since; "
                         "%s is still the old download", path.parent.parent / season, path)
        if stale:
            return 4

    if args.dry_run:  # reads files only; works without a database
        logger.info("Done (dry run): %s", backfill(files, None, dry_run=True))
        return 0

    conn = get_connection()
    unmatched = []
    try:
        ensure_schema(conn)
        if season is None:
            stats = backfill(files, conn)
        else:
            stats = {"files": 0, "rows_read": 0, "rows_updated": 0}
            for path in files:  # one at a time so each target file's match count is known
                one = backfill([path], conn)
                stats = {k: stats[k] + one[k] for k in stats}
                if one["rows_read"] and one["rows_updated"] == 0:  # header-only file: nothing to prove
                    unmatched.append(path)
    except Exception as e:
        logger.error("Backfill failed: %s", e, exc_info=True)
        conn.rollback()
        return 1
    finally:
        conn.close()

    logger.info("Done: %s", stats)
    for path in unmatched:
        logger.error("Season check failed: %s matched 0 rows (season not imported yet?)", path)
    return 4 if unmatched else 0


if __name__ == "__main__":
    sys.exit(main())
