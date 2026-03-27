"""
download.py — Step 1: Download ZIP archives from plvr.land.moi.gov.tw

Downloads seasonal or current real estate transaction data as ZIP files.
Validates each download by checking ZIP magic bytes (PK\x03\x04).
Skips files that already exist and are valid ZIPs.

CLI usage:
    python scripts/download.py --season 113S4
    python scripts/download.py --from 112S1 --to 114S1
    python scripts/download.py --current
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import requests

# Allow running as `python scripts/download.py` from project root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

logger = logging.getLogger(__name__)

# ZIP file magic bytes — first 4 bytes of any valid ZIP archive.
ZIP_MAGIC = b"PK\x03\x04"


def parse_season(s: str) -> tuple[int, int]:
    """Parse a season string like '113S4' into (year=113, quarter=4).

    Raises ValueError if the format is invalid or quarter is out of range.
    """
    s = s.strip().upper()
    if "S" not in s:
        raise ValueError(f"Invalid season format: {s!r}. Expected e.g. '113S4'.")
    parts = s.split("S")
    year, quarter = int(parts[0]), int(parts[1])
    if quarter < 1 or quarter > 4:
        raise ValueError(f"Quarter must be 1–4, got {quarter}")
    return year, quarter


def parse_season_range(from_s: str, to_s: str) -> list[str]:
    """Generate a list of season strings from from_s to to_s (inclusive).

    Example: parse_season_range('112S1', '113S2')
             → ['112S1', '112S2', '112S3', '112S4', '113S1', '113S2']
    """
    y1, q1 = parse_season(from_s)
    y2, q2 = parse_season(to_s)
    seasons = []
    y, q = y1, q1
    while (y, q) <= (y2, q2):
        seasons.append(f"{y}S{q}")
        q += 1
        if q > 4:
            q = 1
            y += 1
    return seasons


def is_valid_zip(filepath: Path) -> bool:
    """Check if a file starts with ZIP magic bytes."""
    if not filepath.exists() or filepath.stat().st_size < 4:
        return False
    with open(filepath, "rb") as f:
        return f.read(4) == ZIP_MAGIC


def download_season(season: str) -> Path:
    """Download a single seasonal ZIP file.

    Returns the path to the saved ZIP. Skips download if the file
    already exists and is a valid ZIP.

    Raises RuntimeError if the download fails or response is not a valid ZIP.
    """
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = config.DATA_DIR / f"{season}.zip"

    # Skip if already downloaded and valid.
    if is_valid_zip(target):
        logger.info("ZIP already exists and is valid, skipping: %s", target)
        return target

    url = config.SEASON_URL_TEMPLATE.format(season=season)
    logger.info("Downloading season %s from %s", season, url)

    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()

    # Write to a temp file first, then rename (atomic-ish on same filesystem).
    tmp = target.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    # Validate the downloaded content is actually a ZIP.
    if not is_valid_zip(tmp):
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"Downloaded file for {season} is not a valid ZIP. "
            "The server may have returned an HTML error page."
        )

    tmp.rename(target)
    logger.info("Saved %s (%.1f KB)", target.name, target.stat().st_size / 1024)
    return target


def download_current() -> Path:
    """Download the current (latest) period ZIP file.

    Returns the path to the saved ZIP. Always re-downloads since
    current data changes frequently.
    """
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = config.DATA_DIR / "current.zip"

    logger.info("Downloading current period from %s", config.CURRENT_URL)
    resp = requests.get(config.CURRENT_URL, timeout=120, stream=True)
    resp.raise_for_status()

    tmp = target.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    if not is_valid_zip(tmp):
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            "Downloaded current-period file is not a valid ZIP. "
            "The endpoint may require a browser session."
        )

    tmp.rename(target)
    logger.info("Saved %s (%.1f KB)", target.name, target.stat().st_size / 1024)
    return target


def main():
    """CLI entry point for download.py."""
    parser = argparse.ArgumentParser(description="Download real estate ZIP data")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--season", help="Single season, e.g. 113S4")
    group.add_argument("--current", action="store_true", help="Download current period")
    parser.add_argument("--from", dest="from_s", help="Range start, e.g. 112S1")
    parser.add_argument("--to", dest="to_s", help="Range end, e.g. 114S1")
    args = parser.parse_args()

    # Configure logging to console.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.current:
        download_current()
    elif args.from_s and args.to_s:
        seasons = parse_season_range(args.from_s, args.to_s)
        logger.info("Downloading %d seasons: %s → %s", len(seasons), seasons[0], seasons[-1])
        for i, s in enumerate(seasons):
            download_season(s)
            # Delay between downloads to avoid rate limiting (skip after last).
            if i < len(seasons) - 1:
                logger.info("Waiting %d seconds before next download...", config.DOWNLOAD_DELAY_SEC)
                time.sleep(config.DOWNLOAD_DELAY_SEC)
    elif args.season:
        # Could be a single season or a range specified with --from/--to.
        if args.from_s or args.to_s:
            parser.error("--season cannot be used with --from/--to")
        download_season(args.season)
    else:
        parser.error("Specify --season, --from/--to, or --current")


if __name__ == "__main__":
    main()
