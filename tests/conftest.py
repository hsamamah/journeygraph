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
        dict(fare_product_id="metrobus_one_way_regular_fare",  fare_product_name="Metrobus Regular", fare_media_id="smartrip_card", amount=2.25, currency="USD"),
        dict(fare_product_id="metrobus_one_way_express_fare",  fare_product_name="Metrobus Express", fare_media_id="smartrip_card", amount=4.25, currency="USD"),
        dict(fare_product_id="metrorail_free_fare_000",        fare_product_name="Metrorail Free",   fare_media_id="smartrip_card", amount=0.00, currency="USD"),
        dict(fare_product_id="metrorail_one_way_full_fare_225",fare_product_name="Metrorail One-Way",fare_media_id="smartrip_card", amount=2.25, currency="USD"),
        dict(fare_product_id="metrorail_one_way_full_fare_200",fare_product_name="Metrorail One-Way",fare_media_id="tap_ride_go",   amount=2.00, currency="USD"),
    ])


@pytest.fixture
def gtfs_data(stops_df, fare_leg_rules_df, fare_media_df, fare_products_df, feed_info_df) -> dict:
    return {
        "stops":           stops_df,
        "fare_leg_rules":  fare_leg_rules_df,
        "fare_media":      fare_media_df,
        "fare_products":   fare_products_df,
        "feed_info":       feed_info_df,
    }


# ── Service & Schedule layer fixtures ─────────────────────────────────────────


@pytest.fixture
def agency_df() -> pd.DataFrame:
    return pd.DataFrame([
        dict(agency_id="1", agency_name="WMATA", agency_url="https://wmata.com",
             agency_timezone="America/New_York", agency_lang="en",
             agency_phone="", agency_fare_url="", agency_email=""),
    ])


@pytest.fixture
def routes_df() -> pd.DataFrame:
    """Two rail lines + two bus routes."""
    return pd.DataFrame([
        dict(route_id="RED", route_short_name="Red", route_long_name="Red Line",
             route_type=1, route_color="BF0D3E", route_text_color="",
             route_desc="", route_url="", route_sort_order=""),
        dict(route_id="BLUE", route_short_name="Blue", route_long_name="Blue Line",
             route_type=1, route_color="009CDE", route_text_color="",
             route_desc="", route_url="", route_sort_order=""),
        dict(route_id="70", route_short_name="70", route_long_name="Georgia Ave",
             route_type=3, route_color="", route_text_color="",
             route_desc="", route_url="", route_sort_order=""),
        dict(route_id="S2", route_short_name="S2", route_long_name="16th Street",
             route_type=3, route_color="", route_text_color="",
             route_desc="", route_url="", route_sort_order=""),
    ])


@pytest.fixture
def trips_df() -> pd.DataFrame:
    """
    Trips covering:
      - Rail weekday trip
      - Bus weekday trip
      - Rail Saturday trip
      - Rail maintenance trip (_R suffix service)
    """
    return pd.DataFrame([
        dict(trip_id="T_RED_1", route_id="RED", service_id="WK_RAIL",
             shape_id="SH_RED_0", direction_id="0", trip_headsign="Glenmont",
             trip_short_name="", block_id="B1"),
        dict(trip_id="T_70_1", route_id="70", service_id="WK_BUS",
             shape_id="SH_70_0", direction_id="0", trip_headsign="Silver Spring",
             trip_short_name="", block_id="B2"),
        dict(trip_id="T_BLUE_SAT", route_id="BLUE", service_id="SAT",
             shape_id="SH_BLUE_0", direction_id="0", trip_headsign="Franconia",
             trip_short_name="", block_id="B3"),
        dict(trip_id="T_RED_MAINT", route_id="RED", service_id="WK_R",
             shape_id="SH_RED_0", direction_id="1", trip_headsign="Shady Grove",
             trip_short_name="", block_id="B4"),
    ])


@pytest.fixture
def stop_times_df() -> pd.DataFrame:
    """Minimal stop_times — 2 stops per trip, rail + bus."""
    return pd.DataFrame([
        # Rail trip T_RED_1
        dict(trip_id="T_RED_1", stop_id="PF_A01_1", arrival_time="06:00:00",
             departure_time="06:00:00", stop_sequence=1, shape_dist_traveled=0.0, timepoint=None),
        dict(trip_id="T_RED_1", stop_id="PF_A02_1", arrival_time="06:05:00",
             departure_time="06:05:00", stop_sequence=2, shape_dist_traveled=1.2, timepoint=None),
        # Bus trip T_70_1
        dict(trip_id="T_70_1", stop_id="10001", arrival_time="07:00:00",
             departure_time="07:00:00", stop_sequence=1, shape_dist_traveled=None, timepoint=1),
        dict(trip_id="T_70_1", stop_id="10002", arrival_time="07:10:00",
             departure_time="07:10:00", stop_sequence=2, shape_dist_traveled=None, timepoint=1),
        # Rail Saturday trip
        dict(trip_id="T_BLUE_SAT", stop_id="PF_C01_1", arrival_time="09:00:00",
             departure_time="09:00:00", stop_sequence=1, shape_dist_traveled=0.0, timepoint=None),
        dict(trip_id="T_BLUE_SAT", stop_id="PF_C02_1", arrival_time="09:08:00",
             departure_time="09:08:00", stop_sequence=2, shape_dist_traveled=2.5, timepoint=None),
        # Rail maintenance trip
        dict(trip_id="T_RED_MAINT", stop_id="PF_A02_1", arrival_time="23:00:00",
             departure_time="23:00:00", stop_sequence=1, shape_dist_traveled=0.0, timepoint=None),
        dict(trip_id="T_RED_MAINT", stop_id="PF_A01_1", arrival_time="23:05:00",
             departure_time="23:05:00", stop_sequence=2, shape_dist_traveled=1.2, timepoint=None),
    ])


@pytest.fixture
def calendar_df() -> pd.DataFrame:
    """
    Three service patterns:
      WK_RAIL  — Mon-Fri rail
      WK_BUS   — Mon-Fri bus
      SAT      — Saturday only
    Note: WK_R (maintenance) runs via calendar_dates only, not in calendar.txt.
    """
    return pd.DataFrame([
        dict(service_id="WK_RAIL", monday=1, tuesday=1, wednesday=1, thursday=1,
             friday=1, saturday=0, sunday=0, start_date="20260101", end_date="20260110"),
        dict(service_id="WK_BUS", monday=1, tuesday=1, wednesday=1, thursday=1,
             friday=1, saturday=0, sunday=0, start_date="20260101", end_date="20260110"),
        dict(service_id="SAT", monday=0, tuesday=0, wednesday=0, thursday=0,
             friday=0, saturday=1, sunday=0, start_date="20260101", end_date="20260110"),
    ])


@pytest.fixture
def calendar_dates_df() -> pd.DataFrame:
    """
    Exceptions:
      - Remove New Year's Day (2026-01-01) from WK_RAIL  (type=2)
      - Add maintenance window WK_R on 2026-01-04 (type=1, calendar_dates-only)
    """
    return pd.DataFrame([
        dict(service_id="WK_RAIL", date="20260101", exception_type=2),
        dict(service_id="WK_R", date="20260104", exception_type=1),
    ])


@pytest.fixture
def service_stops_df() -> pd.DataFrame:
    """Stops for service layer tests — stations, platforms, bus stops."""
    return pd.DataFrame([
        dict(stop_id="STN_A01", stop_name="Metro Center", parent_station="", zone_id="10"),
        dict(stop_id="STN_A02", stop_name="Farragut North", parent_station="", zone_id="3"),
        dict(stop_id="STN_C01", stop_name="Arlington Cemetery", parent_station="", zone_id="15"),
        dict(stop_id="STN_C02", stop_name="Addison Road", parent_station="", zone_id="20"),
        dict(stop_id="PF_A01_1", stop_name="MC Platform 1", parent_station="STN_A01", zone_id=""),
        dict(stop_id="PF_A02_1", stop_name="FN Platform 1", parent_station="STN_A02", zone_id=""),
        dict(stop_id="PF_C01_1", stop_name="AC Platform 1", parent_station="STN_C01", zone_id=""),
        dict(stop_id="PF_C02_1", stop_name="AR Platform 1", parent_station="STN_C02", zone_id=""),
        dict(stop_id="10001", stop_name="Bus Stop 1", parent_station="", zone_id=""),
        dict(stop_id="10002", stop_name="Bus Stop 2", parent_station="", zone_id=""),
    ])


@pytest.fixture
def feed_info_df() -> pd.DataFrame:
    return pd.DataFrame([dict(
        feed_publisher_name="WMATA", feed_publisher_url="https://wmata.com",
        feed_lang="en", feed_start_date="20251214", feed_end_date="20260613",
        feed_version="S1000246", feed_contact_email="", feed_contact_url="",
    )])


@pytest.fixture
def service_gtfs_data(
    agency_df, routes_df, trips_df, stop_times_df,
    calendar_df, calendar_dates_df, service_stops_df, feed_info_df,
) -> dict:
    """Full gtfs_data dict for service layer tests."""
    return {
        "agency": agency_df,
        "routes": routes_df,
        "trips": trips_df,
        "stop_times": stop_times_df,
        "calendar": calendar_df,
        "calendar_dates": calendar_dates_df,
        "stops": service_stops_df,
        "feed_info": feed_info_df,
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
