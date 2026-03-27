"""
test_transform.py — Unit tests for transform utilities and download helpers.

Run with: python -m pytest tests/ -v
"""

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

# Ensure project root is on the path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.transform import roc_date_to_ad, safe_numeric, safe_int, read_csv_with_encoding
from scripts.download import parse_season, parse_season_range
import config


# ------------------------------------------------------------------
# roc_date_to_ad
# ------------------------------------------------------------------

class TestRocDateToAd:
    """Test ROC (民國) date → AD date conversion."""

    def test_normal_7digit(self):
        # 民國113年7月15日 → 2024-07-15
        assert roc_date_to_ad("1130715") == date(2024, 7, 15)

    def test_normal_6digit(self):
        # 民國99年1月1日 → 2010-01-01 (6 digits: year=99, month=01, day=01)
        assert roc_date_to_ad("990101") == date(2010, 1, 1)

    def test_empty_string(self):
        assert roc_date_to_ad("") is None

    def test_none(self):
        assert roc_date_to_ad(None) is None

    def test_nan(self):
        assert roc_date_to_ad(float("nan")) is None

    def test_non_numeric(self):
        assert roc_date_to_ad("abc") is None

    def test_short_string(self):
        assert roc_date_to_ad("123") is None

    def test_invalid_date(self):
        # Month 13 is invalid.
        assert roc_date_to_ad("1131301") is None

    def test_leap_year(self):
        # 民國113年 = 2024 (leap year), Feb 29 is valid.
        assert roc_date_to_ad("1130229") == date(2024, 2, 29)


# ------------------------------------------------------------------
# safe_numeric
# ------------------------------------------------------------------

class TestSafeNumeric:
    def test_normal(self):
        assert safe_numeric("123.45") == 123.45

    def test_integer_string(self):
        assert safe_numeric("100") == 100.0

    def test_empty(self):
        assert safe_numeric("") is None

    def test_none(self):
        assert safe_numeric(None) is None

    def test_nan(self):
        assert safe_numeric(float("nan")) is None

    def test_non_numeric(self):
        assert safe_numeric("N/A") is None

    def test_zero(self):
        assert safe_numeric("0") == 0.0


# ------------------------------------------------------------------
# safe_int
# ------------------------------------------------------------------

class TestSafeInt:
    def test_normal(self):
        assert safe_int("3") == 3

    def test_zero(self):
        assert safe_int("0") == 0

    def test_float_string(self):
        # "1.5" → int(float("1.5")) → 1
        assert safe_int("1.5") == 1

    def test_empty(self):
        assert safe_int("") is None

    def test_none(self):
        assert safe_int(None) is None

    def test_nan(self):
        assert safe_int(float("nan")) is None

    def test_non_numeric(self):
        assert safe_int("abc") is None


# ------------------------------------------------------------------
# parse_season / parse_season_range
# ------------------------------------------------------------------

class TestParseSeason:
    def test_normal(self):
        assert parse_season("113S4") == (113, 4)

    def test_lowercase(self):
        assert parse_season("112s2") == (112, 2)

    def test_invalid_format(self):
        with pytest.raises(ValueError):
            parse_season("113-4")

    def test_quarter_out_of_range(self):
        with pytest.raises(ValueError):
            parse_season("113S5")


class TestParseSeasonRange:
    def test_multi_year(self):
        result = parse_season_range("112S1", "113S2")
        assert result == ["112S1", "112S2", "112S3", "112S4", "113S1", "113S2"]

    def test_single_season(self):
        result = parse_season_range("113S3", "113S3")
        assert result == ["113S3"]

    def test_same_year(self):
        result = parse_season_range("113S1", "113S4")
        assert result == ["113S1", "113S2", "113S3", "113S4"]

    def test_single_quarter(self):
        result = parse_season_range("114S1", "114S1")
        assert result == ["114S1"]


# ------------------------------------------------------------------
# read_csv_with_encoding (with temp file)
# ------------------------------------------------------------------

class TestReadCsvWithEncoding:
    def test_utf8_csv(self, tmp_path):
        """Create a UTF-8 CSV and verify it reads correctly."""
        csv_content = "鄉鎮市區,總價元\n大安區,10000000\n信義區,8000000\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        df = read_csv_with_encoding(csv_file)
        assert len(df) == 2
        assert "鄉鎮市區" in df.columns
        assert df.iloc[0]["鄉鎮市區"] == "大安區"

    def test_cp950_csv(self, tmp_path):
        """Create a CP950 (Big5) CSV and verify it reads correctly."""
        csv_content = "鄉鎮市區,總價元\n大安區,10000000\n"
        csv_file = tmp_path / "test_big5.csv"
        csv_file.write_bytes(csv_content.encode("cp950"))

        df = read_csv_with_encoding(csv_file)
        assert len(df) == 1
        assert df.iloc[0]["鄉鎮市區"] == "大安區"


# ------------------------------------------------------------------
# Column mapping validation
# ------------------------------------------------------------------

class TestColumnMapping:
    """Verify that COLUMN_MAP values don't contain typos by checking
    they exist as known DB column names."""

    # Known transaction columns from schema.sql.
    KNOWN_TXN_COLS = {
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
    }

    KNOWN_RENTAL_COLS = {
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
    }

    def test_column_map_a_values(self):
        for en_col in config.COLUMN_MAP_A.values():
            assert en_col in self.KNOWN_TXN_COLS, f"Unknown column in COLUMN_MAP_A: {en_col}"

    def test_column_map_c_values(self):
        for en_col in config.COLUMN_MAP_C.values():
            assert en_col in self.KNOWN_RENTAL_COLS, f"Unknown column in COLUMN_MAP_C: {en_col}"
