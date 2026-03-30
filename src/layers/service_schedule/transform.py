# src/layers/service_schedule/transform.py
"""
Service & Schedule layer — Transform

Converts raw GTFS DataFrames into Neo4j-ready DataFrames.

Key transforms:
  - Calendar resolution: merges calendar.txt + calendar_dates.txt into a
    final ServicePattern → Date mapping. No intermediate exception node.
  - Route classification: adds mode ('bus'|'rail') and multi-label.
  - RoutePattern derivation: groups trips by shape_id.
  - SCHEDULED_AT: normalises stop_times with mode-specific properties.
  - STOPS_AT: derives ordered stop sequence per RoutePattern.
  - Route SERVES: derives unique route → station/busstop mappings.

Design decisions (v3 schema):
  - :ServiceDay renamed :ServicePattern. :HolidayException eliminated.
  - ETL resolves calendar + calendar_dates into ServicePattern→ACTIVE_ON→Date.
  - holiday_name carried on ACTIVE_ON relationship, not on the node.
  - Route gets :Bus/:Rail multi-labels. Trip does NOT (mode is derived).
  - FeedInfo versioned; all source nodes get FROM_FEED.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.common.logger import get_logger
from src.common.utils import safe_int

log = get_logger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

DAY_COLS = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]

# WMATA-specific: only route_type 1 (rail) and 3 (bus) are used.
# GTFS defines additional types (0=tram, 2=rail, 4=ferry, etc.)
# which are not mapped. Unknown types warn and default to "bus".
# See CONVENTIONS.md → "Route Type Mapping"
ROUTE_TYPE_MODE = {1: "rail", 3: "bus"}


# ── Result container ─────────────────────────────────────────────────────────


@dataclass
class ServiceTransformResult:
    """Clean DataFrames ready for Neo4j ingestion."""

    # Nodes
    feed_info: pd.DataFrame  # single row: feed_version, publisher, dates, etc.
    agency: pd.DataFrame  # agency_id, agency_name, ...
    routes_bus: pd.DataFrame  # Route:Bus rows
    routes_rail: pd.DataFrame  # Route:Rail rows
    route_patterns: pd.DataFrame  # shape_id, headsign, direction_id, route_id
    trips: (
        pd.DataFrame
    )  # trip_id, direction_id, headsign, block_id, shape_id, service_id
    service_patterns: pd.DataFrame  # service_id, label (for multi-label split)
    dates: pd.DataFrame  # date (YYYYMMDD), day_of_week

    # Relationship data
    active_on: pd.DataFrame  # service_id, date, holiday_name (nullable)
    pattern_stops_at: pd.DataFrame  # shape_id, stop_id, stop_sequence, is_terminus
    scheduled_at_rail: (
        pd.DataFrame
    )  # trip_id, stop_id, arrival_time, departure_time, ...
    scheduled_at_bus: (
        pd.DataFrame
    )  # trip_id, stop_id, arrival_time, departure_time, ...
    route_serves_station: pd.DataFrame  # route_id, stop_id (STN_)
    route_serves_busstop: pd.DataFrame  # route_id, stop_id (numeric)

    # Metadata
    feed_version: str
    stats: dict[str, int] = field(default_factory=dict)


# ── US Federal Holiday lookup ────────────────────────────────────────────────
# WMATA-specific: 11 US federal holidays with observed-date rules.
# Does not include DC-specific holidays (Emancipation Day, Inauguration Day).
# holiday_name is attached to the ACTIVE_ON relationship, not the node.
# See CONVENTIONS.md → "US Federal Holiday Detection"


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Return nth occurrence of weekday (0=Mon) in month."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Return last occurrence of weekday in month."""
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _compute_us_holidays(year: int) -> dict[str, str]:
    """Return {YYYYMMDD: holiday_name} for US federal holidays in a year."""
    holidays: dict[str, str] = {}

    def _add(d: date, name: str) -> None:
        holidays[d.strftime("%Y%m%d")] = name
        # Observed-date rules (federal)
        if d.weekday() == 5:  # Saturday → observed Friday
            holidays[(d - timedelta(days=1)).strftime("%Y%m%d")] = name
        elif d.weekday() == 6:  # Sunday → observed Monday
            holidays[(d + timedelta(days=1)).strftime("%Y%m%d")] = name

    _add(date(year, 1, 1), "New Year's Day")
    _add(_nth_weekday(year, 1, 0, 3), "MLK Day")
    _add(_nth_weekday(year, 2, 0, 3), "Presidents' Day")
    _add(_last_weekday(year, 5, 0), "Memorial Day")
    _add(date(year, 6, 19), "Juneteenth")
    _add(date(year, 7, 4), "Independence Day")
    _add(_nth_weekday(year, 9, 0, 1), "Labor Day")
    _add(_nth_weekday(year, 10, 0, 2), "Columbus Day")
    _add(date(year, 11, 11), "Veterans Day")
    _add(_nth_weekday(year, 11, 3, 4), "Thanksgiving")
    _add(date(year, 12, 25), "Christmas Day")
    return holidays


def _build_holiday_lookup(start_year: int, end_year: int) -> dict[str, str]:
    """Build holiday lookup spanning the feed date range."""
    lookup: dict[str, str] = {}
    for y in range(start_year, end_year + 1):
        lookup.update(_compute_us_holidays(y))
    return lookup


# ── Helpers ──────────────────────────────────────────────────────────────────


def _parse_gtfs_date(d) -> date | None:
    """Parse YYYYMMDD int or string to date."""
    s = str(d).strip()
    if len(s) != 8:
        return None
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def _day_of_week_name(d: date) -> str:
    return [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ][d.weekday()]


def _classify_service(row: dict, service_id: str) -> str:
    """
    Assign a ServicePattern label based on calendar day flags and service_id.
    Returns one of: Weekday, Saturday, Sunday, Holiday, Maintenance.
    """
    sid = str(service_id)
    # WMATA-specific: service_ids ending with _R denote maintenance windows
    # (e.g. WK_R, SAT_R). Not a GTFS standard — WMATA naming convention.
    # See CONVENTIONS.md → "Maintenance Service Detection"
    if sid.endswith("_R"):
        return "Maintenance"

    flags = {c: int(row.get(c, 0) or 0) for c in DAY_COLS}
    weekday_count = sum(flags[d] for d in DAY_COLS[:5])
    sat = flags["saturday"]
    sun = flags["sunday"]

    # All flags zero → calendar_dates-only service in calendar.txt row
    if weekday_count == 0 and not sat and not sun:
        return "Holiday"

    if weekday_count >= 4 and not sat and not sun:
        return "Weekday"
    if sat and weekday_count == 0 and not sun:
        return "Saturday"
    if sun and weekday_count == 0 and not sat:
        return "Sunday"

    # Mixed patterns (e.g. Mon-Sat) — default to Weekday with warning
    log.warning(
        "service transform: service_id '%s' has mixed day flags "
        "(weekday=%d, sat=%d, sun=%d) — defaulting to 'Weekday'",
        sid,
        weekday_count,
        sat,
        sun,
    )
    return "Weekday"


# ── Transform functions ──────────────────────────────────────────────────────


def _transform_feed_info(feed_info_raw: pd.DataFrame) -> pd.DataFrame:
    """Extract single-row FeedInfo DataFrame."""
    cols = [
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "feed_start_date",
        "feed_end_date",
        "feed_version",
        "feed_contact_email",
        "feed_contact_url",
    ]
    present = [c for c in cols if c in feed_info_raw.columns]
    df = feed_info_raw[present].head(1).copy()
    # Normalise date columns to string
    for c in ("feed_start_date", "feed_end_date"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df


def _transform_agency(agency_raw: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
        "agency_phone",
        "agency_fare_url",
        "agency_email",
    ]
    present = [c for c in cols if c in agency_raw.columns]
    return agency_raw[present].copy()


def _transform_routes(routes_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (routes_bus, routes_rail) with mode column added.
    Split by route_type so load.py can apply distinct multi-labels.
    """
    cols = [
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_color",
        "route_text_color",
        "route_type",
        "route_desc",
        "route_url",
        "route_sort_order",
    ]
    present = [c for c in cols if c in routes_raw.columns]
    df = routes_raw[present].copy()

    # Vectorised safe_int — replaces apply(safe_int) on route_type
    df["route_type"] = pd.to_numeric(df["route_type"], errors="coerce").astype(
        "float64"
    )
    df["mode"] = df["route_type"].map(ROUTE_TYPE_MODE)

    # Warn on unknown route_types before defaulting to "bus"
    unknown_types = df[df["mode"].isna()]["route_type"].unique()
    if len(unknown_types) > 0:
        log.warning(
            "service transform: unknown route_type(s) %s not in %s — "
            "defaulting to mode='bus'. If these are not bus routes, "
            "add them to ROUTE_TYPE_MODE.",
            unknown_types.tolist(),
            ROUTE_TYPE_MODE,
        )
    df["mode"] = df["mode"].fillna("bus")

    bus = df[df["mode"] == "bus"].reset_index(drop=True)
    rail = df[df["mode"] == "rail"].reset_index(drop=True)
    return bus, rail


def _derive_route_patterns(
    trips_raw: pd.DataFrame,
) -> pd.DataFrame:
    """
    One RoutePattern per unique shape_id.
    Headsign and direction_id taken from the first trip in each group.
    """
    df = trips_raw.dropna(subset=["shape_id"]).copy()
    if df.empty:
        log.warning("service transform: no trips with shape_id — empty RoutePatterns")
        return pd.DataFrame(
            columns=["shape_id", "headsign", "direction_id", "route_id"]
        )

    # First trip per shape_id for headsign / direction
    first = df.sort_values("trip_id").groupby("shape_id", as_index=False).first()
    patterns = first[["shape_id", "route_id"]].copy()
    patterns["headsign"] = first.get("trip_headsign", pd.Series(dtype=str))
    patterns["direction_id"] = first.get("direction_id", pd.Series(dtype=str))
    return patterns.reset_index(drop=True)


def _transform_trips(trips_raw: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "trip_id",
        "direction_id",
        "trip_headsign",
        "trip_short_name",
        "block_id",
        "shape_id",
        "service_id",
        "route_id",
    ]
    present = [c for c in cols if c in trips_raw.columns]
    return trips_raw[present].copy()


def _resolve_calendar(
    calendar_raw: pd.DataFrame,
    calendar_dates_raw: pd.DataFrame | None,
    feed_start: date,
    feed_end: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Resolve calendar + calendar_dates into:
      service_patterns — service_id, label
      active_on        — service_id, date (YYYYMMDD), holiday_name (nullable)
    """
    holiday_lookup = _build_holiday_lookup(feed_start.year, feed_end.year)

    service_dates: dict[str, set[str]] = {}  # service_id → set of YYYYMMDD
    service_labels: dict[str, str] = {}

    # Step 1: Base date sets from calendar.txt
    for _, row in calendar_raw.iterrows():
        sid = str(row["service_id"]).strip()
        start = _parse_gtfs_date(row.get("start_date"))
        end = _parse_gtfs_date(row.get("end_date"))
        if not start or not end:
            log.warning(
                "service transform: bad dates for service_id=%s — skipping", sid
            )
            continue

        service_labels[sid] = _classify_service(row.to_dict(), sid)

        # Clip to feed window — prevents _R maintenance patterns with
        # multi-year ranges (e.g. 20240101–20301231) from generating thousands
        # of Date nodes outside the feed's validity period.
        # See CONVENTIONS.md → "Maintenance Service Detection"
        effective_start = max(start, feed_start)
        effective_end = min(end, feed_end)

        dates: set[str] = set()
        if effective_start > effective_end:
            service_dates[sid] = dates
            continue
        current = effective_start
        while current <= effective_end:
            day_col = DAY_COLS[current.weekday()]
            if int(row.get(day_col, 0) or 0) == 1:
                dates.add(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
        service_dates[sid] = dates

    # Step 2: Apply calendar_dates exceptions
    if calendar_dates_raw is not None and not calendar_dates_raw.empty:
        for _, row in calendar_dates_raw.iterrows():
            sid = str(row["service_id"]).strip()
            date_str = str(row["date"]).strip()
            exc_type = safe_int(row.get("exception_type")) or 0

            if sid not in service_dates:
                service_dates[sid] = set()
                # calendar_dates-only service — classify as Holiday
                service_labels[sid] = "Holiday"

            if exc_type == 1:
                service_dates[sid].add(date_str)
            elif exc_type == 2:
                service_dates[sid].discard(date_str)

    # Step 3: Build output DataFrames
    sp_rows = [
        {"service_id": sid, "label": label} for sid, label in service_labels.items()
    ]
    service_patterns = pd.DataFrame(sp_rows)

    active_rows: list[dict] = []
    for sid, dates in service_dates.items():
        for d in sorted(dates):
            active_rows.append(
                {
                    "service_id": sid,
                    "date": d,
                    "holiday_name": holiday_lookup.get(d),
                }
            )
    active_on = pd.DataFrame(active_rows)

    return service_patterns, active_on


def _transform_dates(active_on: pd.DataFrame) -> pd.DataFrame:
    """Generate unique Date nodes from all dates appearing in active_on."""
    if active_on.empty:
        return pd.DataFrame(columns=["date", "day_of_week"])

    unique_dates = sorted(active_on["date"].unique())
    rows = []
    for d_str in unique_dates:
        d = _parse_gtfs_date(d_str)
        rows.append(
            {
                "date": d_str,
                "day_of_week": _day_of_week_name(d) if d else "Unknown",
            }
        )
    return pd.DataFrame(rows)


def _normalize_gtfs_time_vec(series: pd.Series) -> pd.Series:
    """
    Vectorised GTFS HH:MM:SS → total seconds from service-day start.

    GTFS allows times past 24:00:00 for trips that run after midnight
    (e.g. '25:30:00' = 90930s). Null / malformed values → NaN (float64).

    Replaces per-row apply(normalize_gtfs_time) on 4.4M rows.
    Strategy: GTFS times are always exactly 8 characters (HH:MM:SS).
    Exploit numpy's fixed-width unicode representation — each character
    maps to a 4-byte int32 at a fixed offset. Extract H/M/S digit pairs
    directly as int32 without string splitting or Python loops.
    ~6x faster than apply() on 4.4M rows (0.58s vs 3.4s).
    """
    null_mask = series.isna() | series.isin(["nan", "None", ""])
    result = np.full(len(series), np.nan, dtype="float64")

    valid_mask = ~null_mask
    if not valid_mask.any():
        return pd.Series(result, index=series.index)

    # to_numpy(dtype="U8") pads to exactly 8 chars — guaranteed by GTFS format
    sv = series[valid_mask].to_numpy(dtype="U8")
    flat = sv.view(np.int32)  # each U1 char → int32 value

    # Fixed digit positions: HH=0,1 | MM=3,4 | SS=6,7
    h = (flat[0::8] - 48) * 10 + (flat[1::8] - 48)
    m = (flat[3::8] - 48) * 10 + (flat[4::8] - 48)
    sec = (flat[6::8] - 48) * 10 + (flat[7::8] - 48)
    result[valid_mask.to_numpy()] = h * 3600 + m * 60 + sec

    return pd.Series(result, index=series.index)


def _transform_scheduled_at(
    stop_times_raw: pd.DataFrame,
    trips_raw: pd.DataFrame,
    routes_raw: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (scheduled_at_rail, scheduled_at_bus).

    Joins stop_times with trips→routes to add mode and mode-specific
    properties. Times normalised to seconds from service-day start.

    Performance: all five per-row apply() calls on 4.4M rows replaced
    with vectorised pandas operations:
      normalize_gtfs_time → str.split(":") + arithmetic (Int64)
      safe_int            → pd.to_numeric(errors="coerce") cast to Int64
      safe_float          → pd.to_numeric(errors="coerce")
    Reduces ~22M Python-level function calls to C-speed pandas ops.
    """
    # Build trip→mode lookup via route_type — vectorised map
    trip_route = trips_raw[["trip_id", "route_id"]].drop_duplicates()
    route_mode = routes_raw[["route_id", "route_type"]].drop_duplicates().copy()
    # Vectorised route_type → mode mapping
    route_mode["route_type_int"] = pd.to_numeric(
        route_mode["route_type"], errors="coerce"
    ).astype("float64")
    route_mode["mode"] = route_mode["route_type_int"].map(ROUTE_TYPE_MODE).fillna("bus")

    trip_mode = trip_route.merge(
        route_mode[["route_id", "mode"]], on="route_id", how="left"
    )
    trip_mode_map = trip_mode.set_index("trip_id")["mode"].to_dict()

    # Process stop_times — all vectorised
    st = stop_times_raw.copy()
    st["mode"] = st["trip_id"].map(trip_mode_map).fillna("bus")

    # Times: vectorised HH:MM:SS → seconds (replaces two apply() calls)
    st["arrival_time"] = _normalize_gtfs_time_vec(st["arrival_time"])
    st["departure_time"] = _normalize_gtfs_time_vec(st["departure_time"])

    # stop_sequence: safe_int → pd.to_numeric (replaces one apply() call)
    st["stop_sequence"] = pd.to_numeric(st["stop_sequence"], errors="coerce").astype(
        "float64"
    )

    # Rail carries shape_dist_traveled; bus carries timepoint
    if "shape_dist_traveled" not in st.columns:
        st["shape_dist_traveled"] = pd.NA
    if "timepoint" not in st.columns:
        st["timepoint"] = pd.NA

    # safe_float / safe_int → pd.to_numeric (replaces two apply() calls)
    st["shape_dist_traveled"] = pd.to_numeric(
        st["shape_dist_traveled"], errors="coerce"
    )
    st["timepoint"] = pd.to_numeric(st["timepoint"], errors="coerce").astype("float64")

    rail_cols = [
        "trip_id",
        "stop_id",
        "arrival_time",
        "departure_time",
        "stop_sequence",
        "mode",
        "shape_dist_traveled",
    ]
    bus_cols = [
        "trip_id",
        "stop_id",
        "arrival_time",
        "departure_time",
        "stop_sequence",
        "mode",
        "timepoint",
    ]

    rail = st[st["mode"] == "rail"][rail_cols].reset_index(drop=True)
    bus = st[st["mode"] == "bus"][bus_cols].reset_index(drop=True)

    return rail, bus


def _derive_pattern_stops(
    stop_times_raw: pd.DataFrame,
    trips_raw: pd.DataFrame,
) -> pd.DataFrame:
    """
    Derive STOPS_AT: for each RoutePattern (shape_id), the ordered stop sequence
    from one representative trip. First and last stops marked is_terminus=True.

    Performance: apply(safe_int) replaced with pd.to_numeric; terminus
    marking loop replaced with idxmin()/idxmax() per shape_id group.
    """
    trip_shapes = trips_raw[["trip_id", "shape_id"]].dropna(subset=["shape_id"])
    if trip_shapes.empty:
        return pd.DataFrame(
            columns=["shape_id", "stop_id", "stop_sequence", "is_terminus"]
        )

    # Pick one representative trip per shape_id
    rep = trip_shapes.groupby("shape_id", as_index=False)["trip_id"].first()

    # Get stop_times for those representative trips
    st = stop_times_raw[stop_times_raw["trip_id"].isin(rep["trip_id"])].copy()
    st = st.merge(rep, on="trip_id", how="inner")

    # Vectorised safe_int — replaces apply(safe_int) on stop_sequence
    st["stop_sequence"] = pd.to_numeric(st["stop_sequence"], errors="coerce").astype(
        "float64"
    )
    st = st.sort_values(["shape_id", "stop_sequence"])

    # Mark terminus: idxmin/idxmax per group replaces per-shape_id for loop
    # Each shape_id contributes exactly one first and one last stop.
    st["is_terminus"] = False
    first_idx = st.groupby("shape_id")["stop_sequence"].idxmin()
    last_idx = st.groupby("shape_id")["stop_sequence"].idxmax()
    st.loc[first_idx.dropna(), "is_terminus"] = True
    st.loc[last_idx.dropna(), "is_terminus"] = True

    return st[["shape_id", "stop_id", "stop_sequence", "is_terminus"]].reset_index(
        drop=True
    )


def _derive_route_serves(
    stop_times_raw: pd.DataFrame,
    trips_raw: pd.DataFrame,
    routes_raw: pd.DataFrame,
    stops_raw: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Derive Route -[:SERVES]-> Station/BusStop.
    Rail routes: stop_times references PF_ (Platform), map to parent STN_ (Station).
    Bus routes: stop_times references bus stop_id directly.

    Returns (route_serves_station, route_serves_busstop).
    """
    # Build trip→route and route→mode
    trip_route = trips_raw[["trip_id", "route_id"]].drop_duplicates()
    route_mode = routes_raw[["route_id", "route_type"]].drop_duplicates()
    # Vectorised route_type → mode — replaces apply(lambda + safe_int)
    route_mode["_rt_int"] = pd.to_numeric(
        route_mode["route_type"], errors="coerce"
    ).astype("Int64")
    route_mode["mode"] = route_mode["_rt_int"].map(ROUTE_TYPE_MODE).fillna("bus")

    # Get unique (trip_id, stop_id) from stop_times, then join to route
    st_unique = stop_times_raw[["trip_id", "stop_id"]].drop_duplicates()
    st_route = st_unique.merge(trip_route, on="trip_id", how="left")
    st_route = st_route.merge(
        route_mode[["route_id", "mode"]], on="route_id", how="left"
    )

    # Unique (route_id, stop_id, mode)
    serves = st_route[["route_id", "stop_id", "mode"]].drop_duplicates()

    # WMATA-specific: rail stop_times reference PF_ (Platform) stop_ids,
    # which map to STN_ (Station) via parent_station. Bus stop_ids are numeric.
    # See CONVENTIONS.md → "Stop ID Prefix Conventions"

    # Rail: PF_ stops → parent station (STN_)
    parent_map = (
        stops_raw[
            stops_raw["parent_station"].notna() & (stops_raw["parent_station"] != "")
        ]
        .set_index("stop_id")["parent_station"]
        .to_dict()
    )

    rail = serves[serves["mode"] == "rail"].copy()
    rail["station_id"] = rail["stop_id"].map(parent_map)
    rail = rail.dropna(subset=["station_id"])
    route_serves_station = (
        rail[["route_id", "station_id"]]
        .rename(columns={"station_id": "stop_id"})
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Bus: stop_id is the BusStop directly
    bus = serves[serves["mode"] == "bus"].copy()
    route_serves_busstop = (
        bus[["route_id", "stop_id"]].drop_duplicates().reset_index(drop=True)
    )

    return route_serves_station, route_serves_busstop


# ── Main entry point ─────────────────────────────────────────────────────────


def run(raw: dict[str, pd.DataFrame]) -> ServiceTransformResult:
    """
    Transform raw GTFS DataFrames into a ServiceTransformResult.
    """
    log.info("service transform: starting")

    # Unpack
    agency_raw = raw["agency"]
    routes_raw = raw["routes"]
    trips_raw = raw["trips"]
    stop_times_raw = raw["stop_times"]
    calendar_raw = raw["calendar"]
    stops_raw = raw["stops"]
    feed_info_raw = raw["feed_info"]
    calendar_dates_raw = raw.get("calendar_dates")

    # ── Feed info ────────────────────────────────────────────────────────────
    feed_info = _transform_feed_info(feed_info_raw)
    feed_version = str(feed_info.iloc[0].get("feed_version", "unknown")).strip()
    log.info("service transform: feed_version = %s", feed_version)

    # Feed date range for calendar generation
    feed_start = _parse_gtfs_date(feed_info.iloc[0].get("feed_start_date"))
    feed_end = _parse_gtfs_date(feed_info.iloc[0].get("feed_end_date"))
    if not feed_start or not feed_end:
        raise ValueError(
            f"feed_info.txt missing feed_start_date or feed_end_date. "
            f"Got start={feed_info.iloc[0].get('feed_start_date')}, "
            f"end={feed_info.iloc[0].get('feed_end_date')}. "
            f"Cannot resolve calendar without feed date range."
        )

    # ── Nodes ────────────────────────────────────────────────────────────────
    agency = _transform_agency(agency_raw)
    routes_bus, routes_rail = _transform_routes(routes_raw)
    route_patterns = _derive_route_patterns(trips_raw)
    trips = _transform_trips(trips_raw)
    service_patterns, active_on = _resolve_calendar(
        calendar_raw, calendar_dates_raw, feed_start, feed_end
    )
    dates = _transform_dates(active_on)

    # ── Relationships ────────────────────────────────────────────────────────
    scheduled_at_rail, scheduled_at_bus = _transform_scheduled_at(
        stop_times_raw, trips_raw, routes_raw
    )
    pattern_stops = _derive_pattern_stops(stop_times_raw, trips_raw)
    route_serves_station, route_serves_busstop = _derive_route_serves(
        stop_times_raw, trips_raw, routes_raw, stops_raw
    )

    # ── Stats ────────────────────────────────────────────────────────────────
    stats = {
        "agency": len(agency),
        "routes_bus": len(routes_bus),
        "routes_rail": len(routes_rail),
        "route_patterns": len(route_patterns),
        "trips": len(trips),
        "service_patterns": len(service_patterns),
        "dates": len(dates),
        "active_on": len(active_on),
        "scheduled_at_rail": len(scheduled_at_rail),
        "scheduled_at_bus": len(scheduled_at_bus),
        "pattern_stops_at": len(pattern_stops),
        "route_serves_station": len(route_serves_station),
        "route_serves_busstop": len(route_serves_busstop),
    }
    for k, v in stats.items():
        log.info("service transform: %-25s %8d rows", k, v)

    log.info("service transform: complete")

    return ServiceTransformResult(
        feed_info=feed_info,
        agency=agency,
        routes_bus=routes_bus,
        routes_rail=routes_rail,
        route_patterns=route_patterns,
        trips=trips,
        service_patterns=service_patterns,
        dates=dates,
        active_on=active_on,
        pattern_stops_at=pattern_stops,
        scheduled_at_rail=scheduled_at_rail,
        scheduled_at_bus=scheduled_at_bus,
        route_serves_station=route_serves_station,
        route_serves_busstop=route_serves_busstop,
        feed_version=feed_version,
        stats=stats,
    )
