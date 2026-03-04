# tests/test_utils.py
"""
Tests for src/common/utils.py

Covers: normalize_gtfs_time, clean_str, safe_int, safe_float
"""

import pytest
from src.common.utils import normalize_gtfs_time, clean_str, safe_int, safe_float


# ── normalize_gtfs_time ───────────────────────────────────────────────────────

class TestNormalizeGtfsTime:
    def test_standard_time(self):
        assert normalize_gtfs_time("08:30:00") == 30600  # 8*3600 + 30*60

    def test_midnight_exactly(self):
        assert normalize_gtfs_time("00:00:00") == 0

    def test_past_midnight_gtfs_convention(self):
        # GTFS allows >24h for trips running after midnight
        assert normalize_gtfs_time("25:30:00") == 91800  # 25*3600 + 30*60

    def test_none_returns_none(self):
        assert normalize_gtfs_time(None) is None

    def test_empty_string_returns_none(self):
        assert normalize_gtfs_time("") is None

    def test_nan_string_returns_none(self):
        assert normalize_gtfs_time("nan") is None

    def test_end_of_day(self):
        assert normalize_gtfs_time("23:59:59") == 86399


# ── clean_str ─────────────────────────────────────────────────────────────────

class TestCleanStr:
    def test_strips_whitespace(self):
        assert clean_str("  hello  ") == "hello"

    def test_none_returns_none(self):
        assert clean_str(None) is None

    def test_empty_string_returns_none(self):
        assert clean_str("") is None

    def test_whitespace_only_returns_none(self):
        assert clean_str("   ") is None

    def test_nan_string_returns_none(self):
        assert clean_str("nan") is None

    def test_normal_string_unchanged(self):
        assert clean_str("STN_A01") == "STN_A01"

    def test_numeric_value_becomes_string(self):
        assert clean_str(42) == "42"


# ── safe_int ──────────────────────────────────────────────────────────────────

class TestSafeInt:
    def test_integer_passthrough(self):
        assert safe_int(5) == 5

    def test_string_integer(self):
        assert safe_int("7") == 7

    def test_float_truncates(self):
        assert safe_int(3.9) == 3

    def test_none_returns_none(self):
        assert safe_int(None) is None

    def test_non_numeric_string_returns_none(self):
        assert safe_int("abc") is None

    def test_empty_string_returns_none(self):
        assert safe_int("") is None


# ── safe_float ────────────────────────────────────────────────────────────────

class TestSafeFloat:
    def test_float_passthrough(self):
        assert safe_float(2.25) == 2.25

    def test_string_float(self):
        assert safe_float("3.14") == pytest.approx(3.14)

    def test_integer_converts(self):
        assert safe_float(5) == 5.0

    def test_none_returns_none(self):
        assert safe_float(None) is None

    def test_non_numeric_string_returns_none(self):
        assert safe_float("abc") is None

    def test_empty_string_returns_none(self):
        assert safe_float("") is None
