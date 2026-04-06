"""
transform.py — Step 2: Extract ZIPs, read CSVs, clean & transform data

Handles Big5/UTF-8 encoding detection, Chinese→English column renaming,
ROC→AD date conversion, and numeric type casting.

Public API:
    process_season(season) → dict[str, pd.DataFrame]
    process_current()      → dict[str, pd.DataFrame]
"""

import logging
import re
import zipfile
from datetime import date
from pathlib import Path
from typing import Optional
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

logger = logging.getLogger(__name__)

# Regex to match CSV filenames like "d_lvr_land_a.csv" → city='d', type='a'.
CSV_PATTERN = re.compile(r"^([a-z])_lvr_land_([abc])\.csv$", re.IGNORECASE)


# ------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------

def roc_date_to_ad(roc_date) -> Optional[date]:
    """Convert a ROC (民國) date string to a Python date object.

    Input format: "1130715" → year=113+1911=2024, month=07, day=15.
    Returns None for empty, malformed, or out-of-range values.
    Rejects ROC years outside 90–120 (AD 2001–2031) as likely parse errors.
    """
    if pd.isna(roc_date):
        return None
    s = str(roc_date).strip()
    if not s or not s.isdigit() or len(s) < 5:
        return None
    try:
        # The year part is everything except the last 4 digits (MMDD).
        roc_year = int(s[:-4])
        # Validate ROC year range: real estate data spans ~民國90年 (2001) to
        # ~民國120年 (2031). Values outside this are almost certainly parse
        # errors (e.g. "1041231" misread as year=1, month=04, day=12).
        if roc_year < 90 or roc_year > 120:
            return None
        year = roc_year + 1911
        month = int(s[-4:-2])
        day = int(s[-2:])
        return date(year, month, day)
    except (ValueError, OverflowError):
        return None


def safe_numeric(val) -> Optional[float]:
    """Convert a value to float, returning None for non-numeric inputs."""
    if pd.isna(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_int(val) -> Optional[int]:
    """Convert a value to int, returning None for non-numeric inputs."""
    if pd.isna(val):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def read_csv_with_encoding(filepath: Path) -> pd.DataFrame:
    """Read a CSV file by trying encodings in config.ENCODING_ORDER.

    All columns are read as strings initially (dtype=str) to prevent
    pandas from silently converting values.

    Raises UnicodeDecodeError if none of the encodings work.
    """
    last_error = None
    for enc in config.ENCODING_ORDER:
        try:
            df = pd.read_csv(filepath, dtype=str, encoding=enc, on_bad_lines="skip")
            logger.debug("Read %s with encoding %s (%d rows)", filepath.name, enc, len(df))
            return df
        except (UnicodeDecodeError, UnicodeError) as e:
            last_error = e
            continue
    raise UnicodeDecodeError(
        "all",
        b"",
        0,
        0,
        f"Failed to read {filepath.name} with any of {config.ENCODING_ORDER}. "
        f"Last error: {last_error}",
    )


# ------------------------------------------------------------------
# Core transform logic
# ------------------------------------------------------------------

def _skip_english_header(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the first data row if it looks like an English header.

    Some CSV versions have row 0 as English column names (e.g. 'The villages...',
    'land shifting total area'). We detect this by checking if the first cell
    contains only ASCII characters.
    """
    if df.empty:
        return df
    first_val = str(df.iloc[0, 0]).strip()
    # If the first cell is all ASCII (English header), drop it.
    if first_val and all(ord(c) < 128 for c in first_val):
        logger.debug("Dropping English header row: %s", first_val[:50])
        return df.iloc[1:].reset_index(drop=True)
    return df


def _apply_column_map(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """Rename Chinese columns to English using column_map.

    Only keeps columns present in column_map. Unknown columns are silently
    discarded for forward-compatibility with schema changes.
    """
    # Find which mapped columns exist in the CSV.
    existing = {ch: en for ch, en in column_map.items() if ch in df.columns}
    if not existing:
        logger.warning("No matching columns found. CSV columns: %s", list(df.columns[:5]))
        return pd.DataFrame()

    missing = set(column_map.keys()) - set(existing.keys())
    if missing:
        logger.debug("Missing expected columns (will be NULL): %s", missing)

    # Select only mapped columns and rename.
    df = df[list(existing.keys())].rename(columns=existing)
    return df


def _convert_types_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Apply type conversions for transaction/pre-sale DataFrames."""
    # Date conversion: ROC → AD.
    if "transaction_date" in df.columns:
        df["transaction_date_ad"] = df["transaction_date"].apply(roc_date_to_ad)

    # Numeric area columns.
    for col in ["land_area", "building_area", "main_area", "sub_area",
                 "balcony_area", "parking_area", "unit_price"]:
        if col in df.columns:
            df[col] = df[col].apply(safe_numeric)

    # Integer price columns.
    for col in ["total_price", "parking_price"]:
        if col in df.columns:
            df[col] = df[col].apply(safe_int)

    # Integer layout columns.
    for col in ["rooms", "halls", "bathrooms"]:
        if col in df.columns:
            df[col] = df[col].apply(safe_int)

    return df


def _convert_types_rentals(df: pd.DataFrame) -> pd.DataFrame:
    """Apply type conversions for rental DataFrames."""
    # Date conversion.
    if "transaction_date" in df.columns:
        df["transaction_date_ad"] = df["transaction_date"].apply(roc_date_to_ad)

    # Numeric area columns.
    for col in ["land_area", "building_area", "main_area", "sub_area",
                 "balcony_area", "unit_rent"]:
        if col in df.columns:
            df[col] = df[col].apply(safe_numeric)

    # Integer rent column.
    if "total_rent" in df.columns:
        df["total_rent"] = df["total_rent"].apply(safe_int)

    # Integer layout columns.
    for col in ["rooms", "halls", "bathrooms"]:
        if col in df.columns:
            df[col] = df[col].apply(safe_int)

    return df


def _extract_zip(zip_path: Path, extract_dir: Path) -> None:
    """Extract a ZIP archive to extract_dir, creating it if needed."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    logger.info("Extracted %s → %s", zip_path.name, extract_dir)


def _process_csv(csv_path: Path, file_type: str, season: str, city_code: str) -> pd.DataFrame:
    """Process a single CSV file: read, rename, convert types, add metadata.

    Args:
        csv_path: Path to the CSV file.
        file_type: 'a' (sale), 'b' (pre-sale), or 'c' (rental).
        season: Season string, e.g. '113S4'.
        city_code: Single-char city code, e.g. 'D'.

    Returns:
        Cleaned DataFrame ready for DB upsert. May be empty if no valid rows.
    """
    # Read CSV with encoding auto-detection.
    df = read_csv_with_encoding(csv_path)

    # Skip English header row if present.
    df = _skip_english_header(df)

    # Apply column mapping (Chinese → English).
    column_map = config.COLUMN_MAP_BY_TYPE.get(file_type, config.COLUMN_MAP_A)
    df = _apply_column_map(df, column_map)
    if df.empty:
        return df

    # Type conversions based on file type.
    if file_type in ("a", "b"):
        df = _convert_types_transactions(df)
    else:
        df = _convert_types_rentals(df)

    # Filter out rows with no serial_no (cannot upsert without unique key).
    if "serial_no" in df.columns:
        before = len(df)
        df = df[df["serial_no"].notna() & (df["serial_no"].str.strip() != "")]
        dropped = before - len(df)
        if dropped > 0:
            logger.info("Dropped %d rows with empty serial_no from %s", dropped, csv_path.name)

    # Add ETL metadata columns.
    df["source_file"] = csv_path.name
    df["source_season"] = season
    df["city_code"] = city_code.upper()

    logger.info("Processed %s: %d rows, %d columns", csv_path.name, len(df), len(df.columns))
    return df


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def process_season(season: str) -> dict[str, pd.DataFrame]:
    """Process all CSVs for a given season.

    Extracts the season ZIP, filters by TARGET_CITY_CODES, and returns
    a dict keyed by '{city}_{type}' (e.g. 'd_a') mapping to DataFrames.
    """
    zip_path = config.DATA_DIR / f"{season}.zip"
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}")

    extract_dir = config.DATA_DIR / season
    _extract_zip(zip_path, extract_dir)

    # Discover CSV files matching the expected pattern.
    results = {}
    for csv_path in sorted(extract_dir.glob("*.csv")):
        m = CSV_PATTERN.match(csv_path.name)
        if not m:
            logger.debug("Skipping non-matching file: %s", csv_path.name)
            continue

        city_code = m.group(1).upper()
        file_type = m.group(2).lower()

        # Filter by target city codes if configured.
        if config.TARGET_CITY_CODES and city_code not in config.TARGET_CITY_CODES:
            continue

        key = f"{city_code.lower()}_{file_type}"
        df = _process_csv(csv_path, file_type, season, city_code)
        if not df.empty:
            results[key] = df

    logger.info("Season %s: processed %d file(s)", season, len(results))
    return results


def process_current() -> dict[str, pd.DataFrame]:
    """Process the current-period ZIP (data/current.zip).

    Uses the computed current season (e.g. '115S2') as the season identifier.
    """
    from season_utils import get_current_season

    zip_path = config.DATA_DIR / "current.zip"
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}")

    extract_dir = config.DATA_DIR / "current"
    _extract_zip(zip_path, extract_dir)

    current_season = get_current_season()

    results = {}
    for csv_path in sorted(extract_dir.glob("*.csv")):
        m = CSV_PATTERN.match(csv_path.name)
        if not m:
            continue

        city_code = m.group(1).upper()
        file_type = m.group(2).lower()

        if config.TARGET_CITY_CODES and city_code not in config.TARGET_CITY_CODES:
            continue

        key = f"{city_code.lower()}_{file_type}"
        df = _process_csv(csv_path, file_type, current_season, city_code)
        if not df.empty:
            results[key] = df

    logger.info("Current period (tagged as %s): processed %d file(s)", current_season, len(results))
    return results
