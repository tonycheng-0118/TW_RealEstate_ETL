"""
test_presale.py — Pre-sale (b file) project name / unit / cancellation columns (TW_RealEstate_ETL#6).

Fixtures are rows copied verbatim from the official CSVs on 2026-11-22:
  j_lvr_land_b_115.csv  data/current + 113S1 j b file (both headers, a non-Big5 '?' name,
                        a cancelled row, one malformed line with an extra field)
  j_lvr_land_b_108.csv  108S1 j b file (has 建案名稱/棟及號, no 解約情形)
  d_lvr_land_b_105.csv  105S1 d b file (none of the three columns)
  j_lvr_land_a_115.csv  data/current j a file
Expected values are read off those rows by hand, not from the implementation.

DB tests run against TEMP tables (they shadow public.transactions for the session only)
and are skipped when PostgreSQL is unreachable.
"""

import logging
import os
import re
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config
from scripts import backfill_presale, load
from scripts.transform import _process_csv

FIXTURES = Path(__file__).resolve().parent / "fixtures"
SCHEMA_SQL = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
NEW_COLUMNS = ["project_name", "building_unit", "cancel_status"]


def _fixture_as(tmp_path, fixture: str, name: str) -> Path:
    """Copy a fixture under its real file name (the city/type come from the name)."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    dest = tmp_path / name
    dest.write_bytes((FIXTURES / fixture).read_bytes())
    return dest


# ------------------------------------------------------------------
# transform: b files keep the three columns, other files leave them out
# ------------------------------------------------------------------

def test_b_file_keeps_project_name_unit_and_cancel_status(tmp_path):
    csv = _fixture_as(tmp_path, "j_lvr_land_b_115.csv", "j_lvr_land_b.csv")
    df = _process_csv(csv, "b", "115S4", "J").set_index("serial_no")

    assert len(df) == 2  # English header dropped, malformed line skipped
    assert df.loc["RPPNMLMKIILGFCJ97CB", ["project_name", "building_unit"]].tolist() == ["?安·一珀", "D-9F號"]
    assert pd.isna(df.loc["RPPNMLMKIILGFCJ97CB", "cancel_status"])  # empty cell = not cancelled
    assert df.loc["RPPNMLSKKIIGFDJ58CB", NEW_COLUMNS].tolist() == ["廣僑樹No.3", "B棟2號", "1130116全部解約"]


@pytest.mark.parametrize("fixture, name, city, present", [
    ("j_lvr_land_b_108.csv", "j_lvr_land_b.csv", "J", ["project_name", "building_unit"]),
    ("d_lvr_land_b_105.csv", "d_lvr_land_b.csv", "D", []),
    ("j_lvr_land_a_115.csv", "j_lvr_land_a.csv", "J", []),
])
def test_older_b_files_and_a_files_do_not_fail(tmp_path, fixture, name, city, present):
    file_type = name[-5]
    df = _process_csv(_fixture_as(tmp_path, fixture, name), file_type, "108S1", city)

    assert not df.empty
    assert [c for c in NEW_COLUMNS if c in df.columns] == present


def test_a_file_rows_get_null_new_columns_when_loaded(tmp_path):
    df = _process_csv(_fixture_as(tmp_path, "j_lvr_land_a_115.csv", "j_lvr_land_a.csv"), "a", "115S4", "J")
    rows = load._df_to_tuples(df, load.TRANSACTION_COLUMNS)
    idx = [load.TRANSACTION_COLUMNS.index(c) for c in NEW_COLUMNS]

    assert rows and all(row[i] is None for row in rows for i in idx)


# ------------------------------------------------------------------
# schema: the new columns are written in schema.sql, load.py and ensure_schema
# ------------------------------------------------------------------

def _transactions_ddl() -> str:
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    return re.search(r"CREATE TABLE IF NOT EXISTS transactions \((.*?)\n\);", sql, re.S).group(1)


def test_every_loaded_transaction_column_exists_in_schema_sql():
    ddl_cols = {m.group(1) for m in re.finditer(r"^\s{4}(\w+)\s", _transactions_ddl(), re.M)}
    assert ddl_cols  # an empty match would make the subset check pass
    assert set(load.TRANSACTION_COLUMNS) <= ddl_cols


@pytest.mark.parametrize("col", NEW_COLUMNS)
def test_schema_sql_migrates_existing_tables(col):
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    assert re.search(rf"ALTER TABLE transactions ADD COLUMN IF NOT EXISTS {col} TEXT;", sql)
    assert col in load.TRANSACTION_ADDED_COLUMNS
    assert col in load.TRANSACTION_COLUMNS


def test_upsert_updates_new_columns_on_conflict():
    sql = load._build_upsert_sql("transactions", load.TRANSACTION_COLUMNS)
    for col in NEW_COLUMNS:
        assert f"{col} = EXCLUDED.{col}" in sql


# ------------------------------------------------------------------
# backfill: file discovery and ordering
# ------------------------------------------------------------------

def _touch(path: Path, mtime: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")
    os.utime(path, (mtime, mtime))


def test_find_b_files_orders_seasons_then_current_and_filters_cities(tmp_path):
    repo, skill = tmp_path / "repo", tmp_path / "skill"
    _touch(repo / "current" / "j_lvr_land_b.csv", 300)
    _touch(skill / "115S3" / "j_lvr_land_b.csv", 200)
    _touch(repo / "114S4" / "j_lvr_land_b.csv", 100)
    _touch(repo / "114S4" / "J_LVR_LAND_B.CSV.bak", 100)
    _touch(repo / "114S4" / "j_lvr_land_a.csv", 100)
    _touch(repo / "114S4" / "a_lvr_land_b.csv", 100)
    _touch(skill / "101S1" / "o_lvr_land_b.csv", 50)
    _touch(skill / "current" / "o_lvr_land_b.csv", 150)  # older current release goes first
    _touch(repo / "114S4.old" / "j_lvr_land_b.csv", 999)  # renamed backup must never be read
    _touch(repo / "tmp" / "j_lvr_land_b.csv", 999)
    (repo / "114S4.zip").write_text("zip")

    found = backfill_presale.find_b_files([repo, skill], ["J", "O"])

    assert found == [
        skill / "101S1" / "o_lvr_land_b.csv",
        repo / "114S4" / "j_lvr_land_b.csv",
        skill / "115S3" / "j_lvr_land_b.csv",
        skill / "current" / "o_lvr_land_b.csv",
        repo / "current" / "j_lvr_land_b.csv",
    ]


@pytest.mark.parametrize("cities", [None, ["j", "o"]])
def test_find_b_files_city_filter_none_means_all_and_ignores_case(tmp_path, cities):
    _touch(tmp_path / "114S4" / "j_lvr_land_b.csv", 1)
    _touch(tmp_path / "114S4" / "o_lvr_land_b.csv", 1)
    assert len(backfill_presale.find_b_files([tmp_path], cities)) == 2


def test_find_b_files_missing_dir_is_empty_and_warns(tmp_path, caplog):
    assert backfill_presale.find_b_files([tmp_path / "nope"], ["J"]) == []
    assert "Data dir not found" in caplog.text  # a typo'd --data-dir must not pass silently


@pytest.mark.parametrize("fixture, name, expected_cols, n_rows", [
    ("j_lvr_land_b_115.csv", "j_lvr_land_b.csv", NEW_COLUMNS, 2),
    ("j_lvr_land_b_108.csv", "j_lvr_land_b.csv", ["project_name", "building_unit"], 2),
    ("d_lvr_land_b_105.csv", "d_lvr_land_b.csv", [], 0),
])
def test_read_updates_only_returns_columns_the_file_has(tmp_path, fixture, name, expected_cols, n_rows):
    cols, rows = backfill_presale.read_updates(_fixture_as(tmp_path, fixture, name))
    assert cols == expected_cols
    assert len(rows) == n_rows


# ------------------------------------------------------------------
# DB: ensure_schema + backfill UPDATE + upsert, against TEMP tables
# ------------------------------------------------------------------

@pytest.fixture
def temp_db():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = load.get_connection()
    except psycopg2.OperationalError as e:
        pytest.skip(f"PostgreSQL unreachable: {e}")
    with conn.cursor() as cur:
        # A pre-#6 table: no new columns yet.
        old_cols = [c for c in load.TRANSACTION_COLUMNS if c not in NEW_COLUMNS]
        cols_sql = ", ".join(f"{c} TEXT" for c in old_cols)
        cur.execute(f"CREATE TEMP TABLE transactions ({cols_sql}, updated_at TIMESTAMPTZ, "
                    f"CONSTRAINT tmp_uq UNIQUE (serial_no))")
        # Guard: every unqualified 'transactions' in these tests must hit the TEMP table.
        cur.execute("SELECT n.nspname LIKE 'pg_temp%' FROM pg_class c JOIN pg_namespace n "
                    "ON n.oid = c.relnamespace WHERE c.oid = 'transactions'::regclass")
        assert cur.fetchone()[0]
    yield conn
    conn.rollback()
    conn.close()


def _row(conn, serial):
    with conn.cursor() as cur:
        cur.execute("SELECT district, total_price, project_name, building_unit, cancel_status "
                    "FROM transactions WHERE serial_no = %s", (serial,))
        return cur.fetchone()


def test_ensure_schema_is_rerunnable(temp_db):
    load.ensure_schema(temp_db)
    load.ensure_schema(temp_db)  # second run must not fail
    assert _row(temp_db, "nope") is None


class _RecordingConn:
    """Wrap a connection and record every SQL statement its cursors execute."""

    def __init__(self, conn):
        self.conn, self.sql = conn, []

    def commit(self):
        self.conn.commit()

    def cursor(self):
        outer, cur = self, self.conn.cursor()

        class Cur:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                cur.close()

            def execute(self, sql, params=None):
                outer.sql.append(sql)
                cur.execute(sql, params)

            def fetchone(self):
                return cur.fetchone()

            def fetchall(self):
                return cur.fetchall()

        return Cur()


def test_ensure_schema_skips_alter_when_columns_exist(temp_db):
    load.ensure_schema(temp_db)
    rec = _RecordingConn(temp_db)
    load.ensure_schema(rec)
    assert rec.sql and not any("ALTER" in s for s in rec.sql)  # no ACCESS EXCLUSIVE lock on every run


def test_ensure_schema_skips_missing_table(temp_db):
    with temp_db.cursor() as cur:
        cur.execute("DROP TABLE pg_temp.transactions")
        cur.execute("SET search_path = pg_temp")  # hide public.transactions: a fresh DB
        cur.execute("SELECT to_regclass('transactions')")
        assert cur.fetchone()[0] is None  # guard: never ALTER the real table here
    load.ensure_schema(temp_db)  # must not raise


def test_backfill_fills_new_columns_without_touching_others(temp_db, tmp_path):
    with temp_db.cursor() as cur:
        cur.execute("INSERT INTO transactions (serial_no, district, total_price, source_file) VALUES "
                    "('RPPNMLMKIILGFCJ97CB', '芎林鄉', '123', 'j_lvr_land_b.csv'), "
                    "('RPPNMLSKKIIGFDJ58CB', '新豐鄉', '456', 'j_lvr_land_b.csv')")
    load.ensure_schema(temp_db)
    old = _fixture_as(tmp_path / "108S1", "j_lvr_land_b_108.csv", "j_lvr_land_b.csv")
    new = _fixture_as(tmp_path / "115S1", "j_lvr_land_b_115.csv", "j_lvr_land_b.csv")

    stats = backfill_presale.backfill([old, new], temp_db)

    assert _row(temp_db, "RPPNMLMKIILGFCJ97CB") == ("芎林鄉", "123", "?安·一珀", "D-9F號", None)
    assert _row(temp_db, "RPPNMLSKKIIGFDJ58CB") == ("新豐鄉", "456", "廣僑樹No.3", "B棟2號", "1130116全部解約")
    with temp_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM transactions")
        assert cur.fetchone()[0] == 2  # UPDATE only, never inserts
    assert stats == {"files": 2, "rows_read": 4, "rows_updated": 2}


def test_backfill_counts_rows_across_pages(temp_db, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "BATCH_SIZE", 1)
    load.ensure_schema(temp_db)
    with temp_db.cursor() as cur:
        cur.execute("INSERT INTO transactions (serial_no) VALUES "
                    "('RPPNMLMKIILGFCJ97CB'), ('RPPNMLSKKIIGFDJ58CB')")
    f = _fixture_as(tmp_path / "115S1", "j_lvr_land_b_115.csv", "j_lvr_land_b.csv")

    assert backfill_presale.backfill([f], temp_db)["rows_updated"] == 2


def test_dry_run_never_connects(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    _fixture_as(tmp_path / "115S1", "j_lvr_land_b_115.csv", "j_lvr_land_b.csv")

    def no_db():
        raise AssertionError("dry run must not connect")

    monkeypatch.setattr(backfill_presale, "get_connection", no_db)
    monkeypatch.setattr(sys, "argv", ["backfill_presale.py", "--city", "J", "--data-dir", str(tmp_path), "--dry-run"])

    assert backfill_presale.main() == 0
    assert "'rows_read': 2" in caplog.text


def test_backfill_older_file_does_not_clear_cancel_status(temp_db, tmp_path):
    load.ensure_schema(temp_db)
    with temp_db.cursor() as cur:
        cur.execute("INSERT INTO transactions (serial_no, project_name, cancel_status) VALUES "
                    "('RPSQMLKKJHOFFBJ48CB', 'old', '1130116全部解約')")
    f = _fixture_as(tmp_path / "108S1", "j_lvr_land_b_108.csv", "j_lvr_land_b.csv")

    backfill_presale.backfill([f], temp_db)

    assert _row(temp_db, "RPSQMLKKJHOFFBJ48CB")[2:] == ("V8", None, "1130116全部解約")


def test_etl_upsert_writes_new_columns(temp_db, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "BATCH_SIZE", 1)
    load.ensure_schema(temp_db)
    df = _process_csv(_fixture_as(tmp_path, "j_lvr_land_b_115.csv", "j_lvr_land_b.csv"), "b", "115S4", "J")

    assert load.upsert_transactions(df, temp_db) == 2
    assert _row(temp_db, "RPPNMLSKKIIGFDJ58CB")[2:] == ("廣僑樹No.3", "B棟2號", "1130116全部解約")


# ------------------------------------------------------------------
# --season / --since: verify one season after re-downloading it (TW_RealEstate_ETL#8)
# Expected behaviour comes from Issue #8: the check must fail when the target season
# is still the old download. run_etl re-extracts the zip every run, so the CSV mtime
# is always fresh; only the <season>.zip mtime tells an old download from a new one.
# ------------------------------------------------------------------

def test_find_b_files_season_only_reads_that_season_in_every_dir(tmp_path):
    repo, skill = tmp_path / "repo", tmp_path / "skill"
    for d in (repo, skill):
        _touch(d / "115S3" / "j_lvr_land_b.csv", 100)
        _touch(d / "115S3.old" / "j_lvr_land_b.csv", 100)
        _touch(d / "115S2" / "j_lvr_land_b.csv", 100)
        _touch(d / "current" / "j_lvr_land_b.csv", 100)
    _touch(skill / "115S3" / "o_lvr_land_b.csv", 100)

    assert backfill_presale.find_b_files([repo, skill], ["J"], season="115S3") == [
        repo / "115S3" / "j_lvr_land_b.csv",
        skill / "115S3" / "j_lvr_land_b.csv",
    ]


@pytest.mark.parametrize("zip_mtime, since, stale", [
    (2000, 1000, False),  # re-downloaded after the procedure started
    (1000, 1000, False),  # same second counts as fresh
    (999, 1000, True),    # old download kept (step 1 skipped)
    (None, 1000, True),   # no zip: nothing proves the CSV is a new download
])
def test_stale_files_judges_by_season_zip_mtime_not_csv(tmp_path, zip_mtime, since, stale):
    csv = tmp_path / "115S3" / "j_lvr_land_b.csv"
    _touch(csv, 5000)  # CSV always looks fresh: run_etl re-extracts every run
    if zip_mtime is not None:
        _touch(tmp_path / "115S3.zip", zip_mtime)

    assert backfill_presale.stale_files([csv], since) == ([csv] if stale else [])


def _season_argv(data_dir, *extra):
    return ["backfill_presale.py", "--city", "J", "--data-dir", str(data_dir), *extra]


def _no_db():
    raise AssertionError("must fail before touching the database")


@pytest.mark.parametrize("extra", [
    ["--season", "115S3"],                       # --since is what makes the check fail on old files
    ["--since", "1000"],                         # --since alone means nothing
    ["--season", "115S5", "--since", "1000"],    # no fifth quarter
    ["--season", "", "--since", "1000"],
])
def test_season_arguments_are_validated(tmp_path, monkeypatch, extra):
    monkeypatch.setattr(backfill_presale, "get_connection", _no_db)
    monkeypatch.setattr(sys, "argv", _season_argv(tmp_path, *extra))
    with pytest.raises(SystemExit) as e:
        backfill_presale.main()
    assert e.value.code == 2


def test_season_lowercase_s_is_accepted(tmp_path, monkeypatch, caplog):
    _touch(tmp_path / "115S3" / "j_lvr_land_b.csv", 100)
    _touch(tmp_path / "115S3.zip", 100)
    monkeypatch.setattr(backfill_presale, "get_connection", _no_db)
    monkeypatch.setattr(sys, "argv", _season_argv(tmp_path, "--season", "115s3", "--since", "100", "--dry-run"))
    assert backfill_presale.main() == 0


def test_season_with_old_zip_exits_4_before_db(tmp_path, monkeypatch, caplog):
    _touch(tmp_path / "115S3" / "j_lvr_land_b.csv", 5000)
    _touch(tmp_path / "115S3.zip", 100)
    monkeypatch.setattr(backfill_presale, "get_connection", _no_db)
    monkeypatch.setattr(sys, "argv", _season_argv(tmp_path, "--season", "115S3", "--since", "1000"))

    assert backfill_presale.main() == 4
    assert "115S3.zip" in caplog.text


def test_season_with_no_files_exits_4(tmp_path, monkeypatch):
    _touch(tmp_path / "115S2" / "j_lvr_land_b.csv", 5000)
    monkeypatch.setattr(backfill_presale, "get_connection", _no_db)
    monkeypatch.setattr(sys, "argv", _season_argv(tmp_path, "--season", "115S3", "--since", "1000"))
    assert backfill_presale.main() == 4


class _KeepOpen:
    """Hand temp_db to main() without letting it close the connection the fixture owns."""

    def __init__(self, conn):
        self.conn = conn

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def close(self):
        pass


@pytest.mark.parametrize("serials, expected_exit", [
    (["RPPNMLSKKIIGFDJ58CB"], 0),  # target season rows are in the DB → updated
    ([], 4),                       # fresh file but nothing matched: not proof of an update
])
def test_season_exit_depends_on_target_file_matching(temp_db, tmp_path, monkeypatch, serials, expected_exit):
    load.ensure_schema(temp_db)
    with temp_db.cursor() as cur:
        for s in serials:
            cur.execute("INSERT INTO transactions (serial_no) VALUES (%s)", (s,))
    _fixture_as(tmp_path / "115S3", "j_lvr_land_b_115.csv", "j_lvr_land_b.csv")
    _touch(tmp_path / "115S3.zip", 2000)
    monkeypatch.setattr(backfill_presale, "get_connection", lambda: _KeepOpen(temp_db))
    monkeypatch.setattr(sys, "argv", _season_argv(tmp_path, "--season", "115S3", "--since", "1000"))

    assert backfill_presale.main() == expected_exit
    if serials:
        assert _row(temp_db, "RPPNMLSKKIIGFDJ58CB")[4] == "1130116全部解約"
