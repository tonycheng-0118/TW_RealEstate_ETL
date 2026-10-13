"""
test_run_etl.py — Exit codes, backup skipping, current-period loads, and curl downloads.

Expected values come from TW_RealEstate_ETL#4 (any failed season → non-zero exit;
all failed → no backup), not from the implementation's output.

Run with: python -m pytest tests/ -v
Live download test: RUN_NETWORK_TESTS=1 python -m pytest tests/test_run_etl.py -k live
"""

import logging
import os
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config
from scripts import delete, download, run_etl

FIXTURES = Path(__file__).resolve().parent / "fixtures"


OK = {"d_lvr_land_a.csv": {"rows": 10, "status": "success"}}
SKIPPED = {"d_lvr_land_a.csv": {"rows": 0, "status": "skipped"}}
FILE_FAILED = {
    "d_lvr_land_a.csv": {"rows": 10, "status": "success"},
    "d_lvr_land_c.csv": {"rows": 0, "status": "failed"},
}
# Error text copied from the 2026-10-13 run on Python 3.14 (TW_RealEstate_ETL#4).
SSL_ERROR = {"error": "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: "
                      "Missing Subject Key Identifier (_ssl.c:1081)"}


# ------------------------------------------------------------------
# etl_exit_code
# ------------------------------------------------------------------

@pytest.mark.parametrize("overall, expected", [
    ({}, run_etl.EXIT_OK),
    ({"114S1": OK}, run_etl.EXIT_OK),
    ({"114S1": SKIPPED, "114S2": OK}, run_etl.EXIT_OK),
    ({"114S1": {}}, run_etl.EXIT_OK),  # season had no target-city CSVs
    ({"114S1": SSL_ERROR}, run_etl.EXIT_ALL_FAILED),
    ({"114S1": SSL_ERROR, "114S2": SSL_ERROR}, run_etl.EXIT_ALL_FAILED),
    ({"114S1": FILE_FAILED}, run_etl.EXIT_ALL_FAILED),
    ({"114S1": OK, "114S2": SSL_ERROR}, run_etl.EXIT_PARTIAL_FAILURE),
    ({"114S1": OK, "114S2": FILE_FAILED}, run_etl.EXIT_PARTIAL_FAILURE),
])
def test_etl_exit_code(overall, expected):
    assert run_etl.etl_exit_code(overall) == expected


def test_exit_codes_are_nonzero_and_distinct_from_argparse():
    assert run_etl.EXIT_PARTIAL_FAILURE != 0
    assert run_etl.EXIT_ALL_FAILED not in (0, 2)


# ------------------------------------------------------------------
# main(): download failures → exit code and backup
# ------------------------------------------------------------------

class FakeConn:
    def close(self):
        pass

    def rollback(self):
        pass


@pytest.fixture
def etl_env(tmp_path, monkeypatch):
    """Point every path at tmp_path and stub the DB; return the backup call list."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(config, "BACKUP_DIR", tmp_path / "backups")
    monkeypatch.setattr(config, "DOWNLOAD_DELAY_SEC", 0)
    monkeypatch.setattr(config, "TARGET_CITY_CODES", config.TARGET_CITY_CODES)  # main() --city overrides it
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", list(root.handlers))  # setup_logging() adds handlers
    monkeypatch.setattr(run_etl, "get_connection", lambda: FakeConn())

    backups = []

    def fake_backup():
        config.BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        path = config.BACKUP_DIR / "tw_realestate_test.sql.gz"
        path.write_bytes(b"backup")
        backups.append(path)
        return path

    monkeypatch.setattr(run_etl, "backup_database", fake_backup)
    return backups


def _fail_fetch(url, dest):
    raise RuntimeError(f"curl failed (rc=60) for {url}: SSL certificate problem")


def test_main_all_downloads_fail_exits_nonzero_without_backup(etl_env, monkeypatch):
    # Patch the curl call itself so the failure travels download_season → run_etl → main.
    monkeypatch.setattr(download, "fetch_to_file", _fail_fetch)
    monkeypatch.setattr(sys, "argv", ["run_etl.py", "--start", "114S1", "--end", "114S2", "--city", "D,J,O"])

    assert run_etl.main() == run_etl.EXIT_ALL_FAILED
    assert etl_env == []
    assert not config.BACKUP_DIR.exists() or list(config.BACKUP_DIR.iterdir()) == []


def test_main_partial_failure_exits_nonzero_and_still_backs_up(etl_env, monkeypatch):
    def fake_download(season):
        if season == "114S2":
            raise RuntimeError("Downloaded file for 114S2 is not a valid ZIP.")
        return config.DATA_DIR / f"{season}.zip"

    monkeypatch.setattr(run_etl, "download_season", fake_download)
    monkeypatch.setattr(run_etl, "process_season", lambda season: {})
    monkeypatch.setattr(sys, "argv", ["run_etl.py", "--start", "114S1", "--end", "114S2"])

    assert run_etl.main() == run_etl.EXIT_PARTIAL_FAILURE
    assert len(etl_env) == 1


def test_main_all_succeed_exits_zero_and_backs_up(etl_env, monkeypatch):
    monkeypatch.setattr(run_etl, "download_season", lambda season: None)
    monkeypatch.setattr(run_etl, "process_season", lambda season: {})
    monkeypatch.setattr(sys, "argv", ["run_etl.py", "--start", "114S1", "--end", "114S2"])

    assert run_etl.main() == run_etl.EXIT_OK
    assert len(etl_env) == 1


def test_main_backup_failure_exits_nonzero(etl_env, monkeypatch):
    def broken_backup():
        raise RuntimeError("pg_dump failed (rc=1)")

    monkeypatch.setattr(run_etl, "download_season", lambda season: None)
    monkeypatch.setattr(run_etl, "process_season", lambda season: {})
    monkeypatch.setattr(run_etl, "backup_database", broken_backup)
    monkeypatch.setattr(sys, "argv", ["run_etl.py", "--start", "114S1", "--end", "114S1"])

    assert run_etl.main() == run_etl.EXIT_PARTIAL_FAILURE


def test_main_current_mode_download_failure_skips_backup(etl_env, monkeypatch):
    # The LaunchAgent runs with no --start/--end (current period only).
    monkeypatch.setattr(download, "fetch_to_file", _fail_fetch)
    monkeypatch.setattr(sys, "argv", ["run_etl.py", "--city", "D,J,O"])

    assert run_etl.main() == run_etl.EXIT_ALL_FAILED
    assert etl_env == []


# ------------------------------------------------------------------
# _load_dataframes: current-period loads
# ------------------------------------------------------------------

@pytest.fixture
def load_spy(monkeypatch):
    calls = {"checked": [], "logged": [], "upserted": 0}

    def fake_check(season, file_name, conn):
        calls["checked"].append(file_name)
        return True  # pretend everything was loaded before

    def fake_upsert(df, conn):
        calls["upserted"] += 1
        return 5

    def fake_log(season, file_name, count, status, started_at, conn):
        calls["logged"].append((season, file_name, status))

    monkeypatch.setattr(run_etl, "check_already_loaded", fake_check)
    monkeypatch.setattr(run_etl, "upsert_transactions", fake_upsert)
    monkeypatch.setattr(run_etl, "upsert_rentals", fake_upsert)
    monkeypatch.setattr(run_etl, "log_etl", fake_log)
    return calls


def test_season_load_skips_already_loaded_files(load_spy):
    summary = run_etl._load_dataframes({"d_a": object()}, "114S1", FakeConn())

    assert summary == {"d_lvr_land_a.csv": {"rows": 0, "status": "skipped"}}
    assert load_spy["upserted"] == 0


def test_current_load_never_skips_and_logs_under_current_prefix(load_spy):
    summary = run_etl._load_dataframes({"d_a": object(), "d_c": object()}, "115S4", FakeConn(), is_current=True)

    assert load_spy["upserted"] == 2
    assert load_spy["checked"] == []
    assert summary == {
        "current/d_lvr_land_a.csv": {"rows": 5, "status": "success"},
        "current/d_lvr_land_c.csv": {"rows": 5, "status": "success"},
    }
    assert load_spy["logged"] == [
        ("115S4", "current/d_lvr_land_a.csv", "success"),
        ("115S4", "current/d_lvr_land_c.csv", "success"),
    ]


# ------------------------------------------------------------------
# download: curl
# ------------------------------------------------------------------

def test_fetch_to_file_never_disables_certificate_checks(monkeypatch, tmp_path):
    seen = {}

    class Result:
        returncode = 0
        stderr = ""

    def fake_run(cmd, **kwargs):
        seen["cmd"] = cmd
        return Result()

    monkeypatch.setattr(download.subprocess, "run", fake_run)
    download.fetch_to_file("https://plvr.land.moi.gov.tw/x", tmp_path / "x.zip")

    assert seen["cmd"][0] == "curl"
    assert "--fail" in seen["cmd"]
    for flag in ("-k", "--insecure", "--proxy-insecure", "--cacert", "--capath"):
        assert flag not in seen["cmd"]


def test_fetch_to_file_failure_raises_and_removes_partial_file(tmp_path):
    # Real curl against a missing file:// URL — no network needed.
    dest = tmp_path / "out.tmp"
    dest.write_bytes(b"partial")

    with pytest.raises(RuntimeError, match="curl failed"):
        download.fetch_to_file((tmp_path / "missing.zip").as_uri(), dest)
    assert not dest.exists()


def test_download_season_via_curl_saves_valid_zip(tmp_path, monkeypatch):
    src = tmp_path / "src" / "114S1.zip"
    src.parent.mkdir()
    with zipfile.ZipFile(src, "w") as zf:
        zf.writestr("d_lvr_land_a.csv", "鄉鎮市區\n東區\n")

    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "SEASON_URL_TEMPLATE", src.parent.as_uri() + "/{season}.zip")

    target = download.download_season("114S1")
    assert target == tmp_path / "data" / "114S1.zip"
    assert target.read_bytes() == src.read_bytes()
    assert not target.with_suffix(".tmp").exists()


def test_download_season_rejects_html_response(tmp_path, monkeypatch):
    # Unpublished seasons return HTTP 200 with an HTML page; fixture is the real 115S4 response from 2026-10-13.
    src = tmp_path / "src" / "115S4.zip"
    src.parent.mkdir()
    src.write_bytes((FIXTURES / "unpublished_season_115S4.html").read_bytes())

    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "SEASON_URL_TEMPLATE", src.parent.as_uri() + "/{season}.zip")

    with pytest.raises(RuntimeError, match="not a valid ZIP"):
        download.download_season("115S4")
    assert not (tmp_path / "data" / "115S4.zip").exists()


@pytest.mark.skipif(os.environ.get("RUN_NETWORK_TESTS") != "1", reason="set RUN_NETWORK_TESTS=1")
def test_live_download_season(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    target = download.download_season("115S2")
    assert download.is_valid_zip(target)
    assert target.stat().st_size > 1_000_000  # 115S2 was 14.7 MB on 2026-10-13
    with zipfile.ZipFile(target) as zf:
        assert any(n.lower().endswith("_lvr_land_a.csv") for n in zf.namelist())


# ------------------------------------------------------------------
# delete.py: etl_log city filter must match both file_name formats
# ------------------------------------------------------------------

def _like(pattern: str, value: str) -> bool:
    """Minimal SQL LIKE (% and _) for checking patterns without a DB."""
    import re
    regex = "".join(".*" if ch == "%" else "." if ch == "_" else re.escape(ch) for ch in pattern)
    return re.fullmatch(regex, value) is not None


@pytest.mark.parametrize("city_codes, file_name, expected", [
    (["D"], "d_lvr_land_a.csv", True),            # CLI upper-cases --city
    (["d"], "d_lvr_land_a.csv", True),
    (["D"], "current/d_lvr_land_c.csv", True),
    (["D"], "j_lvr_land_a.csv", False),
    (["D"], "current/j_lvr_land_a.csv", False),
    (["D", "J"], "current/j_lvr_land_b.csv", True),
])
def test_etl_log_city_filter(city_codes, file_name, expected):
    sql, params = delete.etl_log_city_filter(city_codes)

    assert sql.count("%s") == len(params)
    assert any(_like(p, file_name) for p in params) == expected
