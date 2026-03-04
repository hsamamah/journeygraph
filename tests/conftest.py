# tests/conftest.py
"""
Shared pytest fixtures available to all test modules.

Fixtures:
  stops_df          — minimal stops DataFrame covering all node types
  fare_leg_rules_df — minimal fare_leg_rules DataFrame (bus + rail rows)
  gtfs_data         — full dict[str, DataFrame] as extract.run() receives it
  real_stops_df     — loaded from actual stops.csv if present (optional)
  real_fare_leg_df  — loaded from actual fare_leg_rules.csv if present (optional)
"""

from pathlib import Path

import pandas as pd
import pytest

# ── Path to real GTFS files if available ─────────────────────────────────────
_UPLOADS = Path(__file__).parents[1] / "data" / "gtfs"


# ── Minimal synthetic fixtures ────────────────────────────────────────────────

@pytest.fixture
def stops_df() -> pd.DataFrame:
    """
    Minimal stops covering: Station, FareGate (PAID + UNPAID), BusStop.
    Zone 3 = Farragut North/West cluster.
    Zone 10 = Metro Center cluster.
    Zone 53 = known no-gate station.
    """
    return pd.DataFrame([
        # Stations
        dict(stop_id="STN_A02",     stop_name="Farragut North", zone_id="3",  parent_station="",         location_type="1"),
        dict(stop_id="STN_C03",     stop_name="Farragut West",  zone_id="3",  parent_station="",         location_type="1"),
        dict(stop_id="STN_A01_C01", stop_name="Metro Center",   zone_id="10", parent_station="",         location_type="1"),
        dict(stop_id="STN_X99",     stop_name="Zone53 Station", zone_id="53", parent_station="",         location_type="1"),
        # FareGates — zone matches parent station
        dict(stop_id="NODE_A02_N_FG_PAID",   stop_name="Farragut North", zone_id="3",  parent_station="STN_A02",     location_type="3"),
        dict(stop_id="NODE_A02_N_FG_UNPAID", stop_name="Farragut North", zone_id="3",  parent_station="STN_A02",     location_type="3"),
        dict(stop_id="NODE_A02_S_FG_PAID",   stop_name="Farragut North", zone_id="3",  parent_station="STN_A02",     location_type="3"),
        dict(stop_id="NODE_A02_S_FG_UNPAID", stop_name="Farragut North", zone_id="3",  parent_station="STN_A02",     location_type="3"),
        dict(stop_id="NODE_A01_C01_E_FG_PAID",   stop_name="Metro Center", zone_id="10", parent_station="STN_A01_C01", location_type="3"),
        dict(stop_id="NODE_A01_C01_E_FG_UNPAID", stop_name="Metro Center", zone_id="10", parent_station="STN_A01_C01", location_type="3"),
        # BusStop — no zone_id
        dict(stop_id="10000", stop_name="Some Bus Stop", zone_id="", parent_station="", location_type=""),
    ])


@pytest.fixture
def fare_leg_rules_df() -> pd.DataFrame:
    """Minimal fare_leg_rules covering bus (no area) and rail (with area) rows."""
    return pd.DataFrame([
        # Bus rows — no from/to area
        dict(leg_group_id="leg_metrobus_regular",  network_id="metrobus_regular",  from_area_id="",        to_area_id="",        fare_product_id="metrobus_one_way_regular_fare",    from_timeframe_group_id=""),
        dict(leg_group_id="leg_metrobus_express",  network_id="metrobus_express",  from_area_id="",        to_area_id="",        fare_product_id="metrobus_one_way_express_fare",    from_timeframe_group_id=""),
        # Rail rows — from/to area are STN_ stop_ids
        dict(leg_group_id="leg_metrorail",         network_id="metrorail",         from_area_id="STN_A02", to_area_id="STN_C03", fare_product_id="metrorail_free_fare_000",           from_timeframe_group_id="weekday_regular"),
        dict(leg_group_id="leg_metrorail",         network_id="metrorail",         from_area_id="STN_A02", to_area_id="STN_A01_C01", fare_product_id="metrorail_one_way_full_fare_225", from_timeframe_group_id="weekday_regular"),
        dict(leg_group_id="leg_metrorail",         network_id="metrorail",         from_area_id="STN_A02", to_area_id="STN_A01_C01", fare_product_id="metrorail_one_way_full_fare_200", from_timeframe_group_id="weekend"),
    ])


@pytest.fixture
def fare_media_df() -> pd.DataFrame:
    return pd.DataFrame([
        dict(fare_media_id="smartrip_card", fare_media_name="SmarTrip Card",          fare_media_type=2),
        dict(fare_media_id="tap_ride_go",   fare_media_name="Tap & Ride Go",           fare_media_type=3),
    ])


@pytest.fixture
def fare_products_df() -> pd.DataFrame:
    return pd.DataFrame([
        dict(fare_product_id="metrobus_one_way_regular_fare",  fare_product_name="Metrobus Regular", fare_media_id="smartrip_card"),
        dict(fare_product_id="metrobus_one_way_express_fare",  fare_product_name="Metrobus Express", fare_media_id="smartrip_card"),
        dict(fare_product_id="metrorail_free_fare_000",        fare_product_name="Metrorail Free",   fare_media_id="smartrip_card"),
        dict(fare_product_id="metrorail_one_way_full_fare_225",fare_product_name="Metrorail One-Way",fare_media_id="smartrip_card"),
        dict(fare_product_id="metrorail_one_way_full_fare_200",fare_product_name="Metrorail One-Way",fare_media_id="tap_ride_go"),
    ])


@pytest.fixture
def gtfs_data(stops_df, fare_leg_rules_df, fare_media_df, fare_products_df) -> dict:
    return {
        "stops":           stops_df,
        "fare_leg_rules":  fare_leg_rules_df,
        "fare_media":      fare_media_df,
        "fare_products":   fare_products_df,
    }


# ── Real GTFS fixtures (skip if files not present) ────────────────────────────

def _gtfs_path(name: str) -> Path:
    # Also look in uploads dir for convenience during development
    uploads = Path(__file__).parents[1].parent / "uploads"
    for candidate in [_UPLOADS / f"{name}.txt", _UPLOADS / f"{name}.csv", uploads / f"{name}.csv"]:
        if candidate.exists():
            return candidate
    return _UPLOADS / f"{name}.txt"  # will trigger skip if missing


@pytest.fixture
def real_stops_df():
    path = _gtfs_path("stops")
    if not path.exists():
        pytest.skip(f"Real stops file not found at {path}")
    return pd.read_csv(path, dtype={"stop_id": str, "parent_station": str})


@pytest.fixture
def real_fare_leg_df():
    path = _gtfs_path("fare_leg_rules")
    if not path.exists():
        pytest.skip(f"Real fare_leg_rules file not found at {path}")
    return pd.read_csv(path, dtype={"leg_group_id": str, "network_id": str,
                                     "from_area_id": str, "to_area_id": str})
