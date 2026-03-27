"""
backup.py — Step 4: PostgreSQL backup via pg_dump + gzip

Creates compressed database backups in BACKUP_DIR. If a backup for today
already exists, it is overwritten and logged.

BACKUP_KEEP_COUNT controls retention:
    0 = keep all backups (default)
    N > 0 = keep only the most recent N backups, delete older ones

Public API:
    backup_database() → Path (path to the created backup file)
"""

import gzip
import logging
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

logger = logging.getLogger(__name__)


def _find_pg_dump() -> str:
    """Locate the pg_dump binary.

    Checks the Homebrew PostgreSQL 17 path first, then falls back to
    whatever is on $PATH via shutil.which.
    """
    # Homebrew arm64 path (Apple Silicon).
    homebrew_path = Path("/opt/homebrew/opt/postgresql@17/bin/pg_dump")
    if homebrew_path.exists():
        return str(homebrew_path)

    # Fallback: search $PATH.
    found = shutil.which("pg_dump")
    if found:
        return found

    raise FileNotFoundError(
        "pg_dump not found. Install PostgreSQL or add it to PATH."
    )


def backup_database() -> Path:
    """Run pg_dump and compress the output with gzip.

    Returns the path to the created .sql.gz backup file.
    If a file with today's date already exists, it is overwritten.
    """
    config.BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    # Build output filename with today's date.
    today = datetime.now().strftime("%Y%m%d")
    backup_file = config.BACKUP_DIR / f"tw_realestate_{today}.sql.gz"

    # Log if overwriting an existing backup.
    if backup_file.exists():
        logger.warning("Backup file already exists, overwriting: %s", backup_file)

    pg_dump = _find_pg_dump()

    # Build pg_dump command with connection parameters from config.
    cmd = [
        pg_dump,
        "-h", config.DB_CONFIG["host"],
        "-p", str(config.DB_CONFIG["port"]),
        "-U", config.DB_CONFIG["user"],
        "-d", config.DB_CONFIG["dbname"],
        "--no-password",  # Rely on peer/trust auth for local connections.
    ]

    logger.info("Running pg_dump → %s", backup_file.name)

    # Stream pg_dump stdout through gzip to the output file.
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    with gzip.open(backup_file, "wb") as gz:
        while True:
            chunk = proc.stdout.read(65536)
            if not chunk:
                break
            gz.write(chunk)

    proc.wait()
    if proc.returncode != 0:
        stderr_output = proc.stderr.read().decode("utf-8", errors="replace")
        backup_file.unlink(missing_ok=True)
        raise RuntimeError(f"pg_dump failed (rc={proc.returncode}): {stderr_output}")

    size_kb = backup_file.stat().st_size / 1024
    logger.info("Backup complete: %s (%.1f KB)", backup_file.name, size_kb)

    # Clean up old backups if BACKUP_KEEP_COUNT > 0.
    cleanup_old_backups()

    return backup_file


def cleanup_old_backups() -> None:
    """Remove old backup files beyond BACKUP_KEEP_COUNT.

    If BACKUP_KEEP_COUNT is 0 (default), all backups are kept.
    Otherwise, only the most recent N files matching the naming pattern
    are retained; older ones are deleted.
    """
    keep = config.BACKUP_KEEP_COUNT
    if keep <= 0:
        logger.debug("BACKUP_KEEP_COUNT=%d, keeping all backups", keep)
        return

    # List all backup files sorted by name (date-based naming = chronological order).
    pattern = "tw_realestate_*.sql.gz"
    backups = sorted(config.BACKUP_DIR.glob(pattern))

    if len(backups) <= keep:
        return

    # Delete the oldest files.
    to_delete = backups[: len(backups) - keep]
    for f in to_delete:
        logger.info("Deleting old backup: %s", f.name)
        f.unlink()


if __name__ == "__main__":
    # Allow standalone execution: `python scripts/backup.py`
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    path = backup_database()
    print(f"Backup saved to: {path}")
