# tests/test_physical_validator.py
"""
Tests for src/common/validators/physical.py

Covers:
  validate_pre_transform:
    - Happy path (clean data passes)
    - Duplicate stop_id triggers failure
    - Orphaned child (parent not in stops) triggers failure
    - Child with no parent_station produces warning (does not block)
    - Unknown pathway endpoint produces warning (does not block)
    - pathway_mode outside 1–7 produces warning (does not block)
    - Missing Station/Platform/FareGate partition triggers failure

  validate_post_load:
    - All checks pass with clean mock (count=0)
    - Duplicate id on Station/Platform/FareGate triggers failure
    - Dangling Pathway (no LINKS) produces warning (does not block)
    - Station with no CONTAINS→Platform produces warning (does not block)
    - Escalator/Elevator label migration failure triggers failure
    - Soft counts are recorded as info
"""

import pandas as pd
import pytest

from src.common.validators.physical import validate_pre_transform, validate_post_load


# ── validate_pre_transform — happy path ──────────────────────────────────────


def test_clean_data_passes(physical_stops_df, physical_pathways_df):
    result = validate_pre_transform(physical_stops_df, physical_pathways_df)
    assert result.passed


def test_info_messages_populated_on_success(physical_stops_df, physical_pathways_df):
    result = validate_pre_transform(physical_stops_df, physical_pathways_df)
    assert len(result.info) > 0


# ── Check 1: duplicate stop_id ────────────────────────────────────────────────


def test_duplicate_stop_id_fails(physical_pathways_df):
    stops_with_dup = pd.DataFrame([
        dict(stop_id="STN_A01", location_type=1, parent_station="", zone_id="10"),
        dict(stop_id="STN_A01", location_type=1, parent_station="", zone_id="10"),  # dup
        dict(stop_id="STN_B01", location_type=1, parent_station="", zone_id="5"),
        dict(stop_id="PF_A01_1", location_type=0, parent_station="STN_A01", zone_id=""),
        dict(stop_id="NODE_A01_FG_PAID", location_type=3, parent_station="STN_A01", zone_id="10"),
    ])
    result = validate_pre_transform(stops_with_dup, physical_pathways_df)
    assert not result.passed
    assert any("stop_id" in e.lower() for e in result.errors)


# ── Check 2: orphaned children ────────────────────────────────────────────────


def test_orphaned_platform_fails(physical_pathways_df):
    """Platform references a parent_station not in stops.txt."""
    stops_orphaned = pd.DataFrame([
        dict(stop_id="STN_A01", location_type=1, parent_station="", zone_id="10"),
        dict(stop_id="STN_B01", location_type=1, parent_station="", zone_id="5"),
        dict(stop_id="PF_A01_1", location_type=0, parent_station="STN_GHOST", zone_id=""),  # orphan
        dict(stop_id="PF_B01_1", location_type=0, parent_station="STN_B01", zone_id=""),
        dict(stop_id="NODE_A01_FG_PAID", location_type=3, parent_station="STN_A01", zone_id="10"),
    ])
    result = validate_pre_transform(stops_orphaned, physical_pathways_df)
    assert not result.passed
    assert any("parent_station" in e.lower() for e in result.errors)


def test_orphaned_entrance_fails(physical_pathways_df):
    """Entrance (location_type=2) references a parent_station not in stops.txt."""
    stops_orphaned = pd.DataFrame([
        dict(stop_id="STN_A01", location_type=1, parent_station="", zone_id="10"),
        dict(stop_id="STN_B01", location_type=1, parent_station="", zone_id="5"),
        dict(stop_id="PF_A01_1", location_type=0, parent_station="STN_A01", zone_id=""),
        dict(stop_id="PF_B01_1", location_type=0, parent_station="STN_B01", zone_id=""),
        dict(stop_id="ENT_A01_N", location_type=2, parent_station="STN_MISSING", zone_id=""),  # orphan
        dict(stop_id="NODE_A01_FG_PAID", location_type=3, parent_station="STN_A01", zone_id="10"),
    ])
    result = validate_pre_transform(stops_orphaned, physical_pathways_df)
    assert not result.passed
    assert any("parent_station" in e.lower() for e in result.errors)


def test_child_with_no_parent_station_warns(physical_pathways_df):
    """Child stop missing parent_station entirely — warns but does not block."""
    stops_no_parent = pd.DataFrame([
        dict(stop_id="STN_A01", location_type=1, parent_station="", zone_id="10"),
        dict(stop_id="STN_B01", location_type=1, parent_station="", zone_id="5"),
        dict(stop_id="PF_A01_1", location_type=0, parent_station="STN_A01", zone_id=""),
        dict(stop_id="PF_B01_1", location_type=0, parent_station="STN_B01", zone_id=""),
        dict(stop_id="NODE_A01_FG_PAID",  location_type=3, parent_station="STN_A01", zone_id="10"),
        dict(stop_id="ENT_X_N", location_type=2, parent_station=None, zone_id=""),  # no parent
    ])
    result = validate_pre_transform(stops_no_parent, physical_pathways_df)
    assert result.passed  # non-blocking
    assert any("parent_station" in w.lower() for w in result.warnings)


# ── Check 3: unknown pathway endpoints ───────────────────────────────────────


def test_unknown_pathway_endpoint_warns(physical_stops_df):
    """Pathway referencing a stop_id not in stops.txt — warns but does not block."""
    pathways_unknown = pd.DataFrame([
        dict(pathway_id="PW_001", from_stop_id="ENT_A01_N",   to_stop_id="NODE_A01_FG_UNPAID", pathway_mode=1, is_bidirectional=1),
        dict(pathway_id="PW_999", from_stop_id="GHOST_STOP",  to_stop_id="PF_A01_1",           pathway_mode=1, is_bidirectional=1),
    ])
    result = validate_pre_transform(physical_stops_df, pathways_unknown)
    assert result.passed  # non-blocking
    assert any("links" in w.lower() or "stop_id" in w.lower() for w in result.warnings)


def test_all_pathway_endpoints_known_passes(physical_stops_df, physical_pathways_df):
    result = validate_pre_transform(physical_stops_df, physical_pathways_df)
    assert result.passed
    assert not any("ghost" in w.lower() for w in result.warnings)


# ── Check 4: pathway_mode range ──────────────────────────────────────────────


def test_invalid_pathway_mode_warns(physical_stops_df):
    """pathway_mode=99 is outside GTFS 1–7 range — warns but does not block."""
    pathways_bad_mode = pd.DataFrame([
        dict(pathway_id="PW_001", from_stop_id="ENT_A01_N", to_stop_id="NODE_A01_FG_UNPAID", pathway_mode=1,  is_bidirectional=1),
        dict(pathway_id="PW_BAD", from_stop_id="PF_A01_1",  to_stop_id="PF_A01_2",           pathway_mode=99, is_bidirectional=1),
    ])
    result = validate_pre_transform(physical_stops_df, pathways_bad_mode)
    assert result.passed  # non-blocking
    assert any("pathway_mode" in w.lower() or "1" in w for w in result.warnings)


def test_valid_pathway_modes_pass(physical_stops_df, physical_pathways_df):
    result = validate_pre_transform(physical_stops_df, physical_pathways_df)
    assert result.passed
    assert not any("outside" in w.lower() for w in result.warnings)


# ── Check 5: minimum partition counts ────────────────────────────────────────


def test_no_stations_fails(physical_pathways_df):
    stops_no_station = pd.DataFrame([
        dict(stop_id="PF_A01_1", location_type=0, parent_station="STN_A01", zone_id=""),
        dict(stop_id="NODE_A01_FG_PAID", location_type=3, parent_station="STN_A01", zone_id="10"),
    ])
    result = validate_pre_transform(stops_no_station, physical_pathways_df)
    assert not result.passed
    assert any("station" in e.lower() for e in result.errors)


def test_no_platforms_fails(physical_pathways_df):
    stops_no_platform = pd.DataFrame([
        dict(stop_id="STN_A01", location_type=1, parent_station="", zone_id="10"),
        dict(stop_id="NODE_A01_FG_PAID", location_type=3, parent_station="STN_A01", zone_id="10"),
    ])
    result = validate_pre_transform(stops_no_platform, physical_pathways_df)
    assert not result.passed
    assert any("platform" in e.lower() for e in result.errors)


def test_no_faregates_fails(physical_pathways_df):
    stops_no_faregate = pd.DataFrame([
        dict(stop_id="STN_A01", location_type=1, parent_station="", zone_id="10"),
        dict(stop_id="PF_A01_1", location_type=0, parent_station="STN_A01", zone_id=""),
    ])
    result = validate_pre_transform(stops_no_faregate, physical_pathways_df)
    assert not result.passed
    assert any("faregate" in e.lower() for e in result.errors)


# ── validate_post_load — happy path ──────────────────────────────────────────


def test_post_load_passes_with_clean_graph(neo4j_clean):
    result = validate_post_load(neo4j_clean)
    assert result.passed


def test_post_load_info_messages_populated(neo4j_clean):
    result = validate_post_load(neo4j_clean)
    assert len(result.info) > 0


# ── Checks 6–8: duplicate id on nodes ────────────────────────────────────────


def test_post_load_duplicate_station_fails(neo4j_with_duplicates):
    result = validate_post_load(neo4j_with_duplicates)
    assert not result.passed
    assert any("station" in e.lower() for e in result.errors)


def test_post_load_duplicate_platform_fails(neo4j_with_duplicates):
    result = validate_post_load(neo4j_with_duplicates)
    assert not result.passed
    assert any("platform" in e.lower() for e in result.errors)


def test_post_load_duplicate_faregate_fails(neo4j_with_duplicates):
    result = validate_post_load(neo4j_with_duplicates)
    assert not result.passed
    assert any("faregate" in e.lower() for e in result.errors)


# ── Check 9: dangling Pathway ─────────────────────────────────────────────────


def test_dangling_pathway_warns_not_fails(neo4j_with_duplicates):
    """Pathway with no LINKS rel should warn but not block the pipeline."""
    result = validate_post_load(neo4j_with_duplicates)
    # neo4j_with_duplicates returns count=1 for all queries including the
    # dangling pathway check — verify a warning is emitted
    assert any("pathway" in w.lower() or "links" in w.lower() for w in result.warnings)


def test_no_dangling_pathways_passes(neo4j_clean):
    result = validate_post_load(neo4j_clean)
    assert result.passed
    assert not any("dangling" in w.lower() for w in result.warnings)


# ── Check 10: Station with no CONTAINS→Platform ───────────────────────────────


def test_station_no_platform_warns_not_fails(neo4j_with_duplicates):
    """Station missing CONTAINS→Platform should warn but not block."""
    result = validate_post_load(neo4j_with_duplicates)
    assert any("contains" in w.lower() or "platform" in w.lower() for w in result.warnings)


# ── Checks 11a/b: Escalator/Elevator label migration ─────────────────────────


def test_escalator_label_missing_fails(neo4j_with_duplicates):
    result = validate_post_load(neo4j_with_duplicates)
    assert not result.passed
    assert any("escalator" in e.lower() for e in result.errors)


def test_elevator_label_missing_fails(neo4j_with_duplicates):
    result = validate_post_load(neo4j_with_duplicates)
    assert not result.passed
    assert any("elevator" in e.lower() for e in result.errors)


def test_label_migration_passes_when_clean(neo4j_clean):
    result = validate_post_load(neo4j_clean)
    assert result.passed
    assert any("escalator" in m.lower() for m in result.info)
    assert any("elevator" in m.lower() for m in result.info)


# ── Check 12: soft counts recorded as info ────────────────────────────────────


def test_soft_counts_are_info_not_errors(neo4j_clean):
    result = validate_post_load(neo4j_clean)
    assert result.passed
    labels = {"Station", "Platform", "FareGate", "StationEntrance", "Pathway", "Level", "BusStop"}
    found = {label for label in labels if any(label in m for m in result.info)}
    assert found == labels, f"Missing soft count labels: {labels - found}"
