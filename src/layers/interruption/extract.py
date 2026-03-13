# src/layers/interruption/extract.py
"""
Interruption layer — Extract

Fetches current GTFS-RT feeds (trip updates + service alerts) from WMATA
via WMATAClient and flattens protobuf messages into pandas DataFrames.

Produces:
  trip_updates      — one row per TripUpdate entity
  stop_time_updates — one row per StopTimeUpdate within a TripUpdate
  service_alerts    — one row per ServiceAlert entity
  entity_selectors  — one row per informed_entity within a ServiceAlert

Protobuf enum values are mapped to human-readable strings matching the
v3 schema (e.g. CANCELED, SIGNIFICANT_DELAYS, CONSTRUCTION).
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.common.logger import get_logger

log = get_logger(__name__)

# ── GTFS-RT enum mappings ────────────────────────────────────────────────────
# Protobuf uses integer enum values. Map to v3 schema string names.

SCHEDULE_RELATIONSHIP_TRIP = {
    0: "SCHEDULED",
    1: "ADDED",
    2: "UNSCHEDULED",
    3: "CANCELED",
}

SCHEDULE_RELATIONSHIP_STOP = {
    0: "SCHEDULED",
    1: "SKIPPED",
    2: "NO_DATA",
}

ALERT_CAUSE = {
    1: "UNKNOWN_CAUSE",
    2: "OTHER_CAUSE",
    3: "TECHNICAL_PROBLEM",
    4: "STRIKE",
    5: "DEMONSTRATION",
    6: "ACCIDENT",
    7: "HOLIDAY",
    8: "WEATHER",
    9: "MAINTENANCE",
    10: "CONSTRUCTION",
    11: "POLICE_ACTIVITY",
    12: "MEDICAL_EMERGENCY",
}

ALERT_EFFECT = {
    1: "UNKNOWN_EFFECT",
    2: "NO_SERVICE",
    3: "REDUCED_SERVICE",
    4: "SIGNIFICANT_DELAYS",
    5: "DETOUR",
    6: "ADDITIONAL_SERVICE",
    7: "MODIFIED_SERVICE",
    8: "OTHER_EFFECT",
    9: "STOP_MOVED",
    10: "NO_EFFECT",
    11: "ACCESSIBILITY_ISSUE",
}

SEVERITY_LEVEL = {
    1: "UNKNOWN_SEVERITY",
    2: "INFO",
    3: "WARNING",
    4: "SEVERE",
}


# ── Protobuf text helper ────────────────────────────────────────────────────


def _translated_text(field) -> str | None:
    """Extract the first English translation from a TranslatedString field."""
    if not field or not field.translation:
        return None
    for t in field.translation:
        if t.language in ("en", ""):
            return t.text
    return field.translation[0].text if field.translation else None


# ── Trip Update flattening ───────────────────────────────────────────────────


def _flatten_trip_updates(
    feeds: list[tuple],  # [(FeedMessage, source_str), ...]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Flatten TripUpdate entities into two DataFrames:
      trip_updates      — one row per TripUpdate
      stop_time_updates — one row per StopTimeUpdate (linked by feed_entity_id)
    """
    tu_rows: list[dict] = []
    stu_rows: list[dict] = []

    for feed, source in feeds:
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            tu = entity.trip_update
            trip = tu.trip

            # Compute aggregate delay from stop updates, or use TripUpdate.delay
            delay = None
            if tu.HasField("delay"):
                delay = tu.delay
            elif tu.stop_time_update:
                delays = []
                for stu in tu.stop_time_update:
                    if stu.arrival and stu.arrival.delay:
                        delays.append(stu.arrival.delay)
                    if stu.departure and stu.departure.delay:
                        delays.append(stu.departure.delay)
                if delays:
                    delay = max(delays)

            tu_rows.append({
                "feed_entity_id": entity.id,
                "trip_id": trip.trip_id or None,
                "route_id": trip.route_id or None,
                "start_date": trip.start_date or None,
                "start_time": trip.start_time or None,
                "schedule_relationship": SCHEDULE_RELATIONSHIP_TRIP.get(
                    trip.schedule_relationship, "SCHEDULED"
                ),
                "delay": delay,
                "timestamp": tu.timestamp or None,
                "source": source,
            })

            # StopTimeUpdates within this TripUpdate
            for stu in tu.stop_time_update:
                stu_rows.append({
                    "parent_entity_id": entity.id,
                    "stop_sequence": stu.stop_sequence or None,
                    "stop_id": stu.stop_id or None,
                    "arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
                    "arrival_time": stu.arrival.time if stu.HasField("arrival") else None,
                    "departure_delay": stu.departure.delay if stu.HasField("departure") else None,
                    "departure_time": stu.departure.time if stu.HasField("departure") else None,
                    "schedule_relationship": SCHEDULE_RELATIONSHIP_STOP.get(
                        stu.schedule_relationship, "SCHEDULED"
                    ),
                })

    trip_updates = pd.DataFrame(tu_rows) if tu_rows else pd.DataFrame(
        columns=["feed_entity_id", "trip_id", "route_id", "start_date",
                 "start_time", "schedule_relationship", "delay", "timestamp", "source"]
    )
    stop_time_updates = pd.DataFrame(stu_rows) if stu_rows else pd.DataFrame(
        columns=["parent_entity_id", "stop_sequence", "stop_id", "arrival_delay",
                 "arrival_time", "departure_delay", "departure_time", "schedule_relationship"]
    )

    return trip_updates, stop_time_updates


# ── Service Alert flattening ─────────────────────────────────────────────────


def _flatten_alerts(
    feeds: list[tuple],  # [(FeedMessage, source_str), ...]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Flatten Alert entities into two DataFrames:
      service_alerts    — one row per ServiceAlert
      entity_selectors  — one row per informed_entity (linked by feed_entity_id)
    """
    alert_rows: list[dict] = []
    selector_rows: list[dict] = []

    for feed, source in feeds:
        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue

            alert = entity.alert

            # Active period — take first period if multiple
            active_start = None
            active_end = None
            if alert.active_period:
                active_start = alert.active_period[0].start or None
                active_end = alert.active_period[0].end or None

            alert_rows.append({
                "feed_entity_id": entity.id,
                "cause": ALERT_CAUSE.get(alert.cause, "UNKNOWN_CAUSE"),
                "effect": ALERT_EFFECT.get(alert.effect, "UNKNOWN_EFFECT"),
                "severity_level": SEVERITY_LEVEL.get(alert.severity_level, "UNKNOWN_SEVERITY"),
                "header_text": _translated_text(alert.header_text),
                "description_text": _translated_text(alert.description_text),
                "url": _translated_text(alert.url),
                "active_period_start": active_start,
                "active_period_end": active_end,
                "source": source,
            })

            # EntitySelectors (informed_entity)
            for i, ie in enumerate(alert.informed_entity):
                selector_rows.append({
                    "parent_entity_id": entity.id,
                    "selector_group_id": f"{entity.id}_sel_{i}",
                    "agency_id": ie.agency_id or None,
                    "route_id": ie.route_id or None,
                    "stop_id": ie.stop_id or None,
                    "trip_id": ie.trip.trip_id if ie.HasField("trip") else None,
                })

    service_alerts = pd.DataFrame(alert_rows) if alert_rows else pd.DataFrame(
        columns=["feed_entity_id", "cause", "effect", "severity_level",
                 "header_text", "description_text", "url",
                 "active_period_start", "active_period_end", "source"]
    )
    entity_selectors = pd.DataFrame(selector_rows) if selector_rows else pd.DataFrame(
        columns=["parent_entity_id", "selector_group_id", "agency_id",
                 "route_id", "stop_id", "trip_id"]
    )

    return service_alerts, entity_selectors


# ── Main entry point ─────────────────────────────────────────────────────────


def run(
    api_client,
    gtfs_data: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Fetch GTFS-RT feeds and flatten to DataFrames.

    Returns dict with keys:
      trip_updates, stop_time_updates, service_alerts, entity_selectors,
      feed_info (passed through from gtfs_data)
    """
    log.info("interruption extract: fetching GTFS-RT feeds")

    # Fetch both rail and bus feeds
    tu_feeds = api_client.get_all_trip_updates()
    alert_feeds = api_client.get_all_alerts()

    log.info(
        "interruption extract: fetched %d trip update feed(s), %d alert feed(s)",
        len(tu_feeds), len(alert_feeds),
    )

    # Flatten protobuf → DataFrames
    trip_updates, stop_time_updates = _flatten_trip_updates(tu_feeds)
    service_alerts, entity_selectors = _flatten_alerts(alert_feeds)

    log.info("interruption extract: trip_updates          %6d rows", len(trip_updates))
    log.info("interruption extract: stop_time_updates     %6d rows", len(stop_time_updates))
    log.info("interruption extract: service_alerts        %6d rows", len(service_alerts))
    log.info("interruption extract: entity_selectors      %6d rows", len(entity_selectors))

    result = {
        "trip_updates": trip_updates,
        "stop_time_updates": stop_time_updates,
        "service_alerts": service_alerts,
        "entity_selectors": entity_selectors,
    }

    # Pass through feed_info for FeedInfo node
    if "feed_info" in gtfs_data:
        result["feed_info"] = gtfs_data["feed_info"].copy()

    return result
