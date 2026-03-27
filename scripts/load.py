"""
load.py — Step 3: Upsert DataFrames into PostgreSQL

Handles batch INSERT ... ON CONFLICT (serial_no) DO UPDATE for both
transactions and rentals tables. Logs each import to etl_log to prevent
duplicate processing.

Public API:
    get_connection()  → psycopg2 connection
    check_already_loaded(season, file_name, conn) → bool
    upsert_transactions(df, conn) → int
    upsert_rentals(df, conn) → int
    log_etl(season, file_name, row_count, status, started_at, conn)
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

logger = logging.getLogger(__name__)

# ---------- Column lists for each table (must match schema.sql) ----------
# These define the INSERT column order. Excludes id, created_at (auto-generated).

TRANSACTION_COLUMNS = [
    "district", "address", "land_section", "land_number",
    "target_type", "transaction_date", "transaction_date_ad", "transaction_note",
    "land_area", "zoning_urban", "zoning_non_urban", "zoning_non_urban_cd",
    "floor_transferred", "total_floors", "building_type", "main_purpose",
    "main_material", "build_complete_date", "building_area",
    "main_area", "sub_area", "balcony_area",
    "rooms", "halls", "bathrooms", "has_partition",
    "has_management", "has_elevator",
    "total_price", "unit_price",
    "parking_type", "parking_area", "parking_price",
    "serial_no", "transfer_no", "note",
    "source_file", "source_season", "city_code",
]

RENTAL_COLUMNS = [
    "district", "address", "land_section", "land_number",
    "target_type", "transaction_date", "transaction_date_ad", "transaction_note",
    "land_area", "zoning_urban", "zoning_non_urban", "zoning_non_urban_cd",
    "floor_transferred", "total_floors", "building_type", "main_purpose",
    "main_material", "build_complete_date", "building_area",
    "main_area", "sub_area", "balcony_area",
    "rooms", "halls", "bathrooms", "has_partition",
    "has_management", "has_elevator",
    "total_rent", "unit_rent",
    "serial_no", "transfer_no", "note",
    "source_file", "source_season", "city_code",
]


def get_connection():
    """Create and return a new psycopg2 connection using config.DB_CONFIG."""
    conn = psycopg2.connect(**config.DB_CONFIG)
    conn.autocommit = False
    return conn


def check_already_loaded(season: str, file_name: str, conn) -> bool:
    """Check if a season/file combination has been successfully loaded before.

    Returns True if etl_log shows status='success' for this combination.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM etl_log WHERE season = %s AND file_name = %s AND status = 'success'",
            (season, file_name),
        )
        return cur.fetchone() is not None


def _build_upsert_sql(table: str, columns: list[str]) -> str:
    """Build an INSERT ... ON CONFLICT (serial_no) DO UPDATE SQL statement.

    The UPDATE SET clause updates all columns except serial_no and adds
    updated_at = NOW().
    """
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    # Update all columns except serial_no on conflict.
    update_cols = [c for c in columns if c != "serial_no"]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    update_set += ", updated_at = NOW()"

    return (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (serial_no) DO UPDATE SET {update_set}"
    )


def _df_to_tuples(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    """Convert a DataFrame to a list of tuples for executemany.

    Missing columns are filled with None. NaN/NaT values are converted
    to None (PostgreSQL NULL).
    """
    # Ensure all required columns exist (fill missing with None).
    for col in columns:
        if col not in df.columns:
            df[col] = None

    rows = []
    for _, row in df.iterrows():
        vals = []
        for col in columns:
            v = row[col]
            # Convert pandas NaN/NaT/None to Python None for psycopg2.
            if pd.isna(v):
                vals.append(None)
            # Convert numpy int/float to native Python types.
            elif hasattr(v, "item"):
                vals.append(v.item())
            else:
                vals.append(v)
        rows.append(tuple(vals))
    return rows


def upsert_transactions(df: pd.DataFrame, conn) -> int:
    """Upsert a DataFrame into the transactions table.

    Uses batch executemany with config.BATCH_SIZE. Returns total upserted count.
    """
    sql = _build_upsert_sql("transactions", TRANSACTION_COLUMNS)
    rows = _df_to_tuples(df, TRANSACTION_COLUMNS)
    total = len(rows)

    with conn.cursor() as cur:
        for i in range(0, total, config.BATCH_SIZE):
            batch = rows[i : i + config.BATCH_SIZE]
            cur.executemany(sql, batch)
            logger.debug("Upserted transactions batch %d–%d", i, i + len(batch))
    conn.commit()

    logger.info("Upserted %d rows into transactions", total)
    return total


def upsert_rentals(df: pd.DataFrame, conn) -> int:
    """Upsert a DataFrame into the rentals table.

    Uses batch executemany with config.BATCH_SIZE. Returns total upserted count.
    """
    sql = _build_upsert_sql("rentals", RENTAL_COLUMNS)
    rows = _df_to_tuples(df, RENTAL_COLUMNS)
    total = len(rows)

    with conn.cursor() as cur:
        for i in range(0, total, config.BATCH_SIZE):
            batch = rows[i : i + config.BATCH_SIZE]
            cur.executemany(sql, batch)
            logger.debug("Upserted rentals batch %d–%d", i, i + len(batch))
    conn.commit()

    logger.info("Upserted %d rows into rentals", total)
    return total


def log_etl(
    season: str,
    file_name: str,
    row_count: int,
    status: str,
    started_at: datetime,
    conn,
) -> None:
    """Write or update an etl_log entry for this season/file combination.

    Uses ON CONFLICT to update existing records (e.g. re-running a failed import).
    """
    sql = """
        INSERT INTO etl_log (season, file_name, row_count, status, started_at, finished_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (season, file_name) DO UPDATE SET
            row_count = EXCLUDED.row_count,
            status = EXCLUDED.status,
            started_at = EXCLUDED.started_at,
            finished_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(sql, (season, file_name, row_count, status, started_at))
    conn.commit()
    logger.info("Logged ETL: season=%s file=%s rows=%d status=%s", season, file_name, row_count, status)
