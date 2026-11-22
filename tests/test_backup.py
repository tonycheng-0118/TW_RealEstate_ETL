"""
test_backup.py — backup.py's command line (TW_RealEstate_ETL#10).

`backup.py --help` used to run a real backup and rotate old files out (the script ignored argv).
Expected behaviour comes from the ticket decision: --help only prints usage, an unknown argument
is an argparse error (exit 2), and no arguments runs exactly one backup.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import backup


@pytest.fixture
def calls(monkeypatch, tmp_path):
    made = []

    def fake_backup():
        made.append(1)
        return tmp_path / "tw_realestate_20261122.sql.gz"

    monkeypatch.setattr(backup, "backup_database", fake_backup)
    return made


@pytest.mark.parametrize("argv, code", [(["--help"], 0), (["-h"], 0), (["--bogus"], 2), (["x"], 2)])
def test_help_and_bad_args_never_back_up(calls, argv, code, capsys):
    with pytest.raises(SystemExit) as e:
        backup.main(argv)
    assert e.value.code == code
    assert calls == []
    if code == 0:
        assert "usage:" in capsys.readouterr().out


def test_no_args_backs_up_once(calls, capsys):
    assert backup.main([]) == 0
    assert calls == [1]
    assert "Backup saved to:" in capsys.readouterr().out
