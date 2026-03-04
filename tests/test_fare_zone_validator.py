# tests/test_fare_zone_validator.py
"""
Tests for src/common/validators/fare_zones.py — validate_pre_load

Covers:
  - All area_ids resolve to a zone (happy path)
  - Unresolved area_id triggers failure
  - Multi-zone area_id triggers failure
  - Gate/station zone mismatch triggers failure
  - Zone 53 no-gate warning
  - No faregates at all
  - Real GTFS data (skipped if files absent)
"""

import pandas as pd

from src.common.validators.fare_zones import validate_pre_load

# ── Happy path ────────────────────────────────────────────────────────────────


def test_all_checks_pass_with_clean_data(stops_df, fare_leg_rules_df):
    result = validate_pre_load(stops_df, fare_leg_rules_df)
    assert result.passed


def test_info_messages_populated_on_success(stops_df, fare_leg_rules_df):
    result = validate_pre_load(stops_df, fare_leg_rules_df)
    assert len(result.info) > 0


# ── Check 1: unresolved area_ids ──────────────────────────────────────────────


def test_unresolved_area_id_fails(stops_df, fare_leg_rules_df):
    bad_rules = fare_leg_rules_df.copy()
    bad_rules.loc[len(bad_rules)] = {
        "leg_group_id": "leg_metrorail",
        "network_id": "metrorail",
        "from_area_id": "STN_DOESNOTEXIST",
        "to_area_id": "STN_A02",
        "fare_product_id": "metrorail_one_way_full_fare_225",
        "from_timeframe_group_id": "weekday_regular",
    }
    result = validate_pre_load(stops_df, bad_rules)
    assert not result.passed
    assert any("area_id" in e.lower() for e in result.errors)


# ── Check 2: multi-zone area_ids ──────────────────────────────────────────────


def test_area_id_in_multiple_zones_fails(fare_leg_rules_df):
    """Same stop_id appearing with two different zone_ids should flag as ambiguous."""
    stops_ambiguous = pd.DataFrame(
        [
            # STN_A02 appears twice with different zones
            dict(stop_id="STN_A02", zone_id="3", parent_station="", location_type="1"),
            dict(stop_id="STN_A02", zone_id="99", parent_station="", location_type="1"),
            dict(stop_id="STN_C03", zone_id="3", parent_station="", location_type="1"),
            dict(
                stop_id="STN_A01_C01",
                zone_id="10",
                parent_station="",
                location_type="1",
            ),
            dict(stop_id="STN_X99", zone_id="53", parent_station="", location_type="1"),
        ]
    )
    result = validate_pre_load(stops_ambiguous, fare_leg_rules_df)
    assert not result.passed
    assert any("multiple zone" in e.lower() for e in result.errors)


# ── Check 3: gate/station zone mismatch ──────────────────────────────────────


def test_gate_zone_mismatch_fails(fare_leg_rules_df):
    stops_mismatch = pd.DataFrame(
        [
            dict(stop_id="STN_A02", zone_id="3", parent_station="", location_type="1"),
            dict(stop_id="STN_C03", zone_id="3", parent_station="", location_type="1"),
            dict(
                stop_id="STN_A01_C01",
                zone_id="10",
                parent_station="",
                location_type="1",
            ),
            dict(stop_id="STN_X99", zone_id="53", parent_station="", location_type="1"),
            # Gate has zone 99 but parent station STN_A02 has zone 3 — mismatch
            dict(
                stop_id="NODE_A02_N_FG_PAID",
                zone_id="99",
                parent_station="STN_A02",
                location_type="3",
            ),
        ]
    )
    result = validate_pre_load(stops_mismatch, fare_leg_rules_df)
    assert not result.passed
    assert any("mismatch" in e.lower() for e in result.errors)


# ── Zone 53 warning ───────────────────────────────────────────────────────────


def test_zone_53_no_gates_produces_warning(stops_df, fare_leg_rules_df):
    """Zone 53 station exists but has no FG nodes — should warn, not fail."""
    result = validate_pre_load(stops_df, fare_leg_rules_df)
    assert result.passed  # non-blocking
    assert any("53" in w for w in result.warnings)


def test_zone_53_with_gate_suppresses_warning(fare_leg_rules_df):
    stops_with_53_gate = pd.DataFrame(
        [
            dict(stop_id="STN_A02", zone_id="3", parent_station="", location_type="1"),
            dict(stop_id="STN_C03", zone_id="3", parent_station="", location_type="1"),
            dict(
                stop_id="STN_A01_C01",
                zone_id="10",
                parent_station="",
                location_type="1",
            ),
            dict(stop_id="STN_X99", zone_id="53", parent_station="", location_type="1"),
            dict(
                stop_id="NODE_X99_FG_PAID",
                zone_id="53",
                parent_station="STN_X99",
                location_type="3",
            ),
            dict(
                stop_id="NODE_A02_N_FG_PAID",
                zone_id="3",
                parent_station="STN_A02",
                location_type="3",
            ),
        ]
    )
    result = validate_pre_load(stops_with_53_gate, fare_leg_rules_df)
    assert result.passed
    assert not any("53" in w for w in result.warnings)


# ── Edge cases ────────────────────────────────────────────────────────────────


def test_bus_rows_with_no_area_pass(stops_df):
    """Bus rules have empty from/to area — should be ignored, not flagged."""
    bus_only = pd.DataFrame(
        [
            dict(
                leg_group_id="leg_metrobus_regular",
                network_id="metrobus_regular",
                from_area_id="",
                to_area_id="",
                fare_product_id="metrobus_one_way_regular_fare",
                from_timeframe_group_id="",
            ),
        ]
    )
    result = validate_pre_load(stops_df, bus_only)
    assert result.passed


def test_no_faregates_in_stops_passes(fare_leg_rules_df):
    """Stops with no FG nodes — gates checks should pass with nothing to check."""
    stops_no_gates = pd.DataFrame(
        [
            dict(stop_id="STN_A02", zone_id="3", parent_station="", location_type="1"),
            dict(stop_id="STN_C03", zone_id="3", parent_station="", location_type="1"),
            dict(
                stop_id="STN_A01_C01",
                zone_id="10",
                parent_station="",
                location_type="1",
            ),
            dict(stop_id="STN_X99", zone_id="53", parent_station="", location_type="1"),
        ]
    )
    result = validate_pre_load(stops_no_gates, fare_leg_rules_df)
    assert result.passed


# ── Real GTFS data (integration tests) ───────────────────────────────────────


def test_pre_load_passes_on_real_gtfs(real_stops_df, real_fare_leg_df):
    """
    Full integration check against actual WMATA GTFS files.
    Skipped automatically if files are not present.
    """
    result = validate_pre_load(real_stops_df, real_fare_leg_df)
    assert result.passed, (
        f"Pre-load validation failed on real data:\n{result.summary()}"
    )


def test_zone_counts_on_real_gtfs(real_stops_df, real_fare_leg_df):
    """Verify known counts from our earlier analysis hold in the real feed."""
    result = validate_pre_load(real_stops_df, real_fare_leg_df)
    station_zones = real_stops_df[
        real_stops_df["stop_id"].str.startswith("STN_", na=False)
        & real_stops_df["zone_id"].notna()
    ]
    gate_nodes = real_stops_df[
        real_stops_df["stop_id"].str.contains("_FG_", na=False)
        & real_stops_df["zone_id"].notna()
    ]
    assert len(station_zones) == 98, f"Expected 98 stations, got {len(station_zones)}"
    assert len(gate_nodes) == 240, f"Expected 240 gate nodes, got {len(gate_nodes)}"
    assert result.passed
