# tests/test_accessibility_extract_transform.py
"""
Tests for src/layers/accessibility/extract.py, transform.py, and pathway_joiner.py

Extract tests:   wraps list→DataFrame, empty API response, returns "outages" key
Transform tests: column rename + exclusion, severity derivation, composite_key
                 snapshot model, projected_duration_days, status, dedup, empty input
Pathway joiner:  Tier 1 programmatic join (station/zone/type/segment filters),
                 Tier 2 static lookup, unmatched logging
"""

import pandas as pd
import pytest

from src.layers.accessibility.extract import _OUTAGE_COLUMNS
from src.layers.accessibility.extract import run as extract_run
from src.layers.accessibility.transform import _parse_epoch_ms
from src.layers.accessibility.transform import run as transform_run
from src.layers.accessibility import pathway_joiner

# ═══════════════════════════════════════════════════════════════
# EXTRACT
# ═══════════════════════════════════════════════════════════════


class TestAccessibilityExtract:
    def test_returns_outages_key(self, raw_outage_rows):
        client = _MockAPIClient(raw_outage_rows)
        result = extract_run(client)
        assert "outages" in result

    def test_wraps_list_into_dataframe(self, raw_outage_rows):
        client = _MockAPIClient(raw_outage_rows)
        result = extract_run(client)
        assert isinstance(result["outages"], pd.DataFrame)
        assert len(result["outages"]) == len(raw_outage_rows)

    def test_empty_api_response_returns_empty_frame(self):
        client = _MockAPIClient([])
        result = extract_run(client)
        df = result["outages"]
        assert isinstance(df, pd.DataFrame)
        assert df.empty
        # Guaranteed columns even when empty
        for col in _OUTAGE_COLUMNS:
            assert col in df.columns

    def test_raw_columns_preserved(self, raw_outage_rows):
        client = _MockAPIClient(raw_outage_rows)
        df = extract_run(client)["outages"]
        assert "UnitName" in df.columns
        assert "DateOutOfService" in df.columns


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — _parse_epoch_ms helper
# ═══════════════════════════════════════════════════════════════


class TestParseEpochMs:
    def test_valid_iso_date_returns_integer(self):
        s = pd.Series(["2024-01-15T08:00:00"])
        result = _parse_epoch_ms(s)
        assert pd.notna(result.iloc[0])
        assert result.iloc[0] > 0

    def test_known_epoch_value_with_z_suffix(self):
        # 2024-01-15 00:00:00 UTC = 1_705_276_800_000 ms since Unix epoch
        s = pd.Series(["2024-01-15T00:00:00Z"])
        result = _parse_epoch_ms(s)
        assert result.iloc[0] == 1_705_276_800_000

    def test_timezone_naive_string_treated_as_utc(self):
        # WMATA API returns naive ISO strings (no Z) — they must be treated as UTC,
        # not local time. Pins the utc=True behaviour in _parse_epoch_ms.
        s = pd.Series(["2024-01-15T00:00:00"])
        result = _parse_epoch_ms(s)
        assert result.iloc[0] == 1_705_276_800_000

    def test_unknown_string_returns_na(self):
        s = pd.Series(["UNKNOWN"])
        result = _parse_epoch_ms(s)
        assert pd.isna(result.iloc[0])

    def test_none_returns_na(self):
        s = pd.Series([None])
        result = _parse_epoch_ms(s)
        assert pd.isna(result.iloc[0])

    def test_returns_nullable_int64_dtype(self):
        s = pd.Series(["2024-01-15T00:00:00", None])
        result = _parse_epoch_ms(s)
        assert str(result.dtype) == "Int64"


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — column handling
# ═══════════════════════════════════════════════════════════════


class TestAccessibilityTransformColumns:
    def test_renames_camel_case_to_snake_case(self, raw_outages_dict):
        result = transform_run(raw_outages_dict)
        cols = result.outages.columns
        assert "unit_name" in cols
        assert "station_code" in cols
        assert "location_description" in cols
        assert "symptom_description" in cols
        assert "date_out_of_service" in cols
        assert "date_updated" in cols
        assert "estimated_return" in cols

    def test_excluded_fields_not_present(self, raw_outages_dict):
        result = transform_run(raw_outages_dict)
        cols = result.outages.columns
        for excluded in (
            "UnitStatus", "StationName", "SymptomCode",
            "TimeOutOfService", "TimeUpdated", "DisplayOrder",
        ):
            assert excluded not in cols, f"{excluded!r} should be excluded"

    def test_status_is_active_lowercase(self, raw_outages_dict):
        result = transform_run(raw_outages_dict)
        assert (result.outages["status"] == "active").all()

    def test_poll_timestamp_column_present(self, raw_outages_dict):
        result = transform_run(raw_outages_dict)
        assert "poll_timestamp" in result.outages.columns
        assert result.poll_timestamp  # non-empty string

    def test_empty_input_returns_empty_result(self):
        raw = {"outages": pd.DataFrame()}
        result = transform_run(raw)
        assert result.outages.empty
        assert result.stats["outages"] == 0


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — severity derivation
# ═══════════════════════════════════════════════════════════════


class TestSeverityDerivation:
    @pytest.mark.parametrize("symptom,expected_severity", [
        ("Minor Repair",      2),
        ("Service Call",      2),
        ("Inspection Repair", 2),
        ("Other",             2),
        ("Major Repair",      3),
        ("Modernization",     4),
    ])
    def test_severity_from_symptom(self, symptom, expected_severity):
        raw = {"outages": pd.DataFrame([_make_row(symptom_description=symptom)])}
        result = transform_run(raw)
        assert result.outages.iloc[0]["severity"] == expected_severity

    def test_unknown_symptom_defaults_to_2(self):
        raw = {"outages": pd.DataFrame([_make_row(symptom_description="Totally Unknown")])}
        result = transform_run(raw)
        assert result.outages.iloc[0]["severity"] == 2

    def test_severity_is_integer_dtype(self, raw_outages_dict):
        result = transform_run(raw_outages_dict)
        assert pd.api.types.is_integer_dtype(result.outages["severity"])

    def test_fixture_contains_all_severity_levels(self, raw_outages_dict):
        result = transform_run(raw_outages_dict)
        sevs = set(result.outages["severity"].tolist())
        # fixture has: Minor Repair (2), Major Repair (3), Modernization (4)
        assert sevs == {2, 3, 4}


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — composite_key (snapshot model)
# ═══════════════════════════════════════════════════════════════


class TestCompositeKey:
    def test_composite_key_present(self, raw_outages_dict):
        result = transform_run(raw_outages_dict)
        assert "composite_key" in result.outages.columns

    def test_composite_key_format(self, raw_outages_dict):
        result = transform_run(raw_outages_dict)
        key = result.outages.iloc[0]["composite_key"]
        # format: unit_name|dos_epoch|du_epoch — two pipe separators
        parts = key.split("|")
        assert len(parts) == 3
        assert parts[0] == "A02W03"

    def test_different_date_updated_creates_different_key(self):
        """Two rows with same unit+dos but different date_updated → different composite_key."""
        base = _make_row(
            unit_name="A02W03",
            date_out_of_service="2024-01-15T08:00:00",
        )
        row_v1 = {**base, "DateUpdated": "2024-01-15T09:00:00"}
        row_v2 = {**base, "DateUpdated": "2024-01-15T12:00:00"}
        result = transform_run({"outages": pd.DataFrame([row_v1, row_v2])})
        keys = result.outages["composite_key"].tolist()
        assert keys[0] != keys[1]

    def test_same_snapshot_deduplicates(self):
        """Identical rows → single node after dedup on composite_key."""
        row = _make_row()
        result = transform_run({"outages": pd.DataFrame([row, row])})
        assert len(result.outages) == 1

    def test_all_keys_unique_in_fixture(self, raw_outages_dict):
        result = transform_run(raw_outages_dict)
        keys = result.outages["composite_key"]
        assert keys.nunique() == len(result.outages)


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — projected_duration_days
# ═══════════════════════════════════════════════════════════════


class TestProjectedDurationDays:
    def test_computed_when_estimated_return_present(self):
        # dos = 2024-01-15, return = 2024-01-16 → 1 day
        row = _make_row(
            date_out_of_service="2024-01-15T00:00:00",
            estimated_return="2024-01-16T00:00:00",
        )
        result = transform_run({"outages": pd.DataFrame([row])})
        assert result.outages.iloc[0]["projected_duration_days"] == 1

    def test_null_when_estimated_return_absent(self):
        row = _make_row(estimated_return=None)
        result = transform_run({"outages": pd.DataFrame([row])})
        val = result.outages.iloc[0]["projected_duration_days"]
        assert pd.isna(val) or val is None

    def test_7_day_outage(self):
        row = _make_row(
            date_out_of_service="2024-01-01T00:00:00",
            estimated_return="2024-01-08T00:00:00",
        )
        result = transform_run({"outages": pd.DataFrame([row])})
        assert result.outages.iloc[0]["projected_duration_days"] == 7


# ═══════════════════════════════════════════════════════════════
# PATHWAY JOINER — Tier 1
# ═══════════════════════════════════════════════════════════════


class TestPathwayJoinerTier1:
    def test_standard_station_match(self, pathway_candidates_df):
        outage = _make_outage_series(
            unit_name="A02W03",
            station_code="A02",
            unit_type="ESCALATOR",
            location_description="Escalator between street and mezzanine",
        )
        result = pathway_joiner._tier1_match(outage, pathway_candidates_df)
        assert result == "A02_ESC_W_BT"

    def test_elevator_match(self, pathway_candidates_df):
        # Elevator at A02 W-zone: only one mode-5 candidate after zone filter,
        # so it resolves even without a segment position keyword in the description.
        outage = _make_outage_series(
            unit_name="A02W10",
            station_code="A02",
            unit_type="ELEVATOR",
            location_description="Elevator general outage",  # no street/platform keyword
        )
        result = pathway_joiner._tier1_match(outage, pathway_candidates_df)
        assert result == "A02_ELE_W"

    def test_zone_letter_filters_correctly(self, pathway_candidates_df):
        # C03 S-zone escalator exists; N-zone does not
        outage_s = _make_outage_series(
            unit_name="C03S01",
            station_code="C03",
            unit_type="ESCALATOR",
            location_description="Escalator between street and mezzanine",
        )
        result = pathway_joiner._tier1_match(outage_s, pathway_candidates_df)
        assert result == "C03_ESC_S_BT"

    def test_wrong_zone_returns_none(self, pathway_candidates_df):
        # No N-zone escalator at C03
        outage = _make_outage_series(
            unit_name="C03N01",
            station_code="C03",
            unit_type="ESCALATOR",
            location_description="Escalator between street and mezzanine",
        )
        result = pathway_joiner._tier1_match(outage, pathway_candidates_df)
        assert result is None

    def test_segment_keyword_platform_maps_to_tp(self, pathway_candidates_df):
        # "platform" keyword → _TP; description must not contain other keywords first
        outage = _make_outage_series(
            unit_name="A02W05",
            station_code="A02",
            unit_type="ESCALATOR",
            location_description="Escalator serving platform level",
        )
        result = pathway_joiner._tier1_match(outage, pathway_candidates_df)
        assert result == "A02_ESC_W_TP"

    def test_missing_station_code_returns_none(self, pathway_candidates_df):
        outage = _make_outage_series(
            unit_name="A02W03",
            station_code="",
            unit_type="ESCALATOR",
            location_description="street",
        )
        result = pathway_joiner._tier1_match(outage, pathway_candidates_df)
        assert result is None

    def test_unknown_unit_type_returns_none(self, pathway_candidates_df):
        outage = _make_outage_series(
            unit_name="A02W03",
            station_code="A02",
            unit_type="STAIRS",
            location_description="street",
        )
        result = pathway_joiner._tier1_match(outage, pathway_candidates_df)
        assert result is None

    def test_ambiguous_returns_none(self, pathway_candidates_df):
        """When no position keyword matches both _BT and _TP candidates remain → None.

        Requires: fixture has ≥2 A02 W-zone ESC rows (A02_ESC_W_BT and A02_ESC_W_TP).
        Without a keyword, position_suffix is None and both rows survive Filter 4.
        """
        outage = _make_outage_series(
            unit_name="A02W03",
            station_code="A02",
            unit_type="ESCALATOR",
            location_description="general outage",  # no street/platform/mezzanine keyword
        )
        result = pathway_joiner._tier1_match(outage, pathway_candidates_df)
        assert result is None


# ═══════════════════════════════════════════════════════════════
# PATHWAY JOINER — Tier 2 (static lookup)
# ═══════════════════════════════════════════════════════════════


class TestPathwayJoinerTier2:
    def test_hit_returns_pathway_id(self, monkeypatch):
        monkeypatch.setattr(
            pathway_joiner,
            "_STATIC_LOOKUP",
            {"A01E04": "A01_C01_104115"},
        )
        outage = _make_outage_series(unit_name="A01E04", station_code="A01")
        result = pathway_joiner._tier2_match(outage)
        assert result == "A01_C01_104115"

    def test_unknown_unit_returns_none(self):
        outage = _make_outage_series(unit_name="X99Z99", station_code="X99")
        result = pathway_joiner._tier2_match(outage)
        assert result is None


# ═══════════════════════════════════════════════════════════════
# PATHWAY JOINER — resolve() integration
# ═══════════════════════════════════════════════════════════════


class TestPathwayJoinerResolve:
    def test_returns_matched_pairs(self, pathway_candidates_df):
        outages = pd.DataFrame([
            dict(
                composite_key="A02W03|1705276800000|1705309200000",
                unit_name="A02W03",
                station_code="A02",
                unit_type="ESCALATOR",
                location_description="Escalator between street and mezzanine",
            )
        ])
        neo4j = _MockNeo4jForJoiner(pathway_candidates_df)
        result = pathway_joiner.resolve(outages, neo4j)
        assert not result.empty
        assert list(result.columns) == ["composite_key", "pathway_id"]
        assert result.iloc[0]["pathway_id"] == "A02_ESC_W_BT"

    def test_empty_outages_returns_empty_frame(self, pathway_candidates_df):
        result = pathway_joiner.resolve(pd.DataFrame(), _MockNeo4jForJoiner(pathway_candidates_df))
        assert result.empty
        assert "composite_key" in result.columns
        assert "pathway_id" in result.columns

    def test_unmatched_logged_not_raised(self, pathway_candidates_df):
        outages = pd.DataFrame([
            dict(
                composite_key="ZZZ1|0|0",
                unit_name="ZZZ1",
                station_code="ZZZ",
                unit_type="ESCALATOR",
                location_description="street",
            )
        ])
        neo4j = _MockNeo4jForJoiner(pathway_candidates_df)
        # Should not raise — just return empty frame and log warning
        result = pathway_joiner.resolve(outages, neo4j)
        assert result.empty


# ═══════════════════════════════════════════════════════════════
# Real GTFS integration (slow — skipped when files absent)
# ═══════════════════════════════════════════════════════════════


@pytest.mark.slow
class TestRealGTFSPathwayJoiner:
    def test_standard_station_resolves(self, real_pathways_df):
        """Farragut North A02W03 should resolve to a pathway_id via Tier 1."""
        outages = pd.DataFrame([
            dict(
                composite_key="A02W03|0|0",
                unit_name="A02W03",
                station_code="A02",
                unit_type="ESCALATOR",
                location_description="Escalator between street and mezzanine",
            )
        ])
        neo4j = _MockNeo4jForJoiner(real_pathways_df)
        result = pathway_joiner.resolve(outages, neo4j)
        # Tier 1 should find exactly one match at a standard station
        assert len(result) == 1
        assert result.iloc[0]["composite_key"] == "A02W03|0|0"


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════


class _MockAPIClient:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def get_elevator_outages(self) -> list[dict]:
        return self._rows


class _MockNeo4jForJoiner:
    """Minimal Neo4j stub that returns a fixed pathway candidates DataFrame."""

    def __init__(self, candidates: pd.DataFrame):
        self._candidates = candidates

    def query(self, cypher: str, **kwargs):
        # Return rows in the format Neo4jManager.query() produces: list[dict]
        return self._candidates.to_dict(orient="records")


def _make_row(
    *,
    unit_name: str = "A02W03",
    unit_type: str = "ESCALATOR",
    station_code: str = "A02",
    location_description: str = "Escalator between street and mezzanine",
    symptom_description: str = "Minor Repair",
    date_out_of_service: str = "2024-01-15T08:00:00",
    date_updated: str = "2024-01-15T09:00:00",
    estimated_return: str | None = "2024-01-16T08:00:00",
) -> dict:
    """Build a minimal raw API row in CamelCase (as extract.run() returns)."""
    return dict(
        UnitName=unit_name,
        UnitType=unit_type,
        UnitStatus=None,
        StationCode=station_code,
        StationName="Test Station",
        LocationDescription=location_description,
        SymptomCode=None,
        SymptomDescription=symptom_description,
        TimeOutOfService="08:00",
        TimeUpdated="09:00",
        DisplayOrder=1,
        DateOutOfService=date_out_of_service,
        DateUpdated=date_updated,
        EstimatedReturnToService=estimated_return,
    )


def _make_outage_series(
    *,
    unit_name: str = "A02W03",
    station_code: str = "A02",
    unit_type: str = "ESCALATOR",
    location_description: str = "Escalator between street and mezzanine",
) -> pd.Series:
    """Build a post-transform outage row for direct Tier 1/2 tests."""
    return pd.Series({
        "composite_key": f"{unit_name}|0|0",
        "unit_name": unit_name,
        "station_code": station_code,
        "unit_type": unit_type,
        "location_description": location_description,
    })
