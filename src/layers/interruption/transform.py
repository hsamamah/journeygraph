# src/layers/interruption/transform.py
"""
Interruption layer — Transform

Applies deduplication and transform rules to produce the three-tier
disruption model from raw GTFS-RT DataFrames.

Deduplication:
  TripUpdates — hash of (trip_id, start_date, schedule_relationship,
                delay, stop-level states). MERGE on hash in Neo4j ensures
                repeated polls with same state don't create duplicates.
  ServiceAlerts — deduplicate on feed_entity_id.

Transform rules (v3 schema Section 3):
  Rule 1: CANCELED TripUpdate → :Interruption:Cancellation
  Rule 2: delay >= 300s       → :Interruption:Delay
  Rule 3: SKIPPED stop        → :Interruption:Skip
  Rule 4: ServiceAlert effect → mapped Interruption type
  Rule 5: Correlation rollup  — deferred to post-load enrichment
  Rule 6: Maintenance overlap — deferred to post-load enrichment

Severity:
  CANCELED → SEVERE
  delay 300–899s → WARNING, 900+ → SEVERE
  SKIPPED → WARNING
  ServiceAlert → from alert severity_level

Performance notes:
  All inner-loop lookups against stop_time_updates, trip_updates, and
  entity_selectors use pre-grouped dicts (O(1) per lookup) rather than
  per-iteration DataFrame filters (O(m) per lookup). With n=2,205
  TripUpdates and m=27,652 StopTimeUpdates the naive approach produced
  ~60M row comparisons; the grouped approach reduces this to ~30K.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from src.common.logger import get_logger

log = get_logger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

DELAY_THRESHOLD = 300   # seconds (5 minutes) — Rule 2
SEVERE_DELAY = 900      # seconds (15 minutes)

# ServiceAlert effect → Interruption type mapping (Rule 4)
# OTHER_EFFECT mapped to service_change — WMATA uses it for miscellaneous
# service modifications that don't fit named categories.
# See CONVENTIONS.md → "ServiceAlert Effect Mapping"
EFFECT_TYPE_MAP = {
    "NO_SERVICE":          "cancellation",
    "REDUCED_SERVICE":     "service_change",
    "SIGNIFICANT_DELAYS":  "delay",
    "DETOUR":              "detour",
    "MODIFIED_SERVICE":    "service_change",
    "STOP_MOVED":          "service_change",
    "ACCESSIBILITY_ISSUE": "accessibility",
    "OTHER_EFFECT":        "service_change",
}

# Interruption type → Neo4j multi-label
TYPE_LABEL_MAP = {
    "cancellation":  "Cancellation",
    "delay":         "Delay",
    "skip":          "Skip",
    "detour":        "Detour",
    "service_change": "ServiceChange",
    "accessibility": "Accessibility",
}


# ── Result container ─────────────────────────────────────────────────────────


@dataclass
class InterruptionTransformResult:
    """Clean DataFrames ready for Neo4j ingestion."""

    # Tier 1 — Raw source nodes
    trip_updates: pd.DataFrame        # with dedup_hash added
    stop_time_updates: pd.DataFrame
    service_alerts: pd.DataFrame
    entity_selectors: pd.DataFrame

    # Tier 2 — Normalized Interruption nodes
    interruptions: pd.DataFrame       # interruption_id, type, label, cause, effect,
                                      # severity, start_time, description, date

    # Tier 2→1 links
    interruption_sources: pd.DataFrame  # interruption_id, source_entity_id, source_type

    # Tier 2→Service layer links (AFFECTS_*)
    affects_trip: pd.DataFrame        # interruption_id, trip_id
    affects_route: pd.DataFrame       # interruption_id, route_id
    affects_stop: pd.DataFrame        # interruption_id, stop_id

    # Feed info (passed through)
    feed_info: pd.DataFrame | None

    # Metadata
    stats: dict[str, int] = field(default_factory=dict)


# ── Dedup hash ───────────────────────────────────────────────────────────────


def _build_stu_index(
    stop_time_updates: pd.DataFrame,
) -> dict[str, list[tuple]]:
    """
    Pre-group StopTimeUpdates by parent_entity_id for O(1) hash lookups.

    Returns dict: entity_id → list of (stop_sequence, schedule_relationship,
    arrival_delay, departure_delay) tuples, sorted by stop_sequence.

    Building this index once costs O(m). Each subsequent lookup is O(1)
    instead of O(m), reducing total hash computation from O(n×m) to O(n+m).
    With n=2,205 and m=27,652 this is ~60M → ~30K operations.
    """
    index: dict[str, list[tuple]] = {}
    if stop_time_updates.empty:
        return index

    for _, stu in stop_time_updates.iterrows():
        eid = stu["parent_entity_id"]
        if eid not in index:
            index[eid] = []
        index[eid].append((
            stu.get("stop_sequence"),
            stu.get("schedule_relationship"),
            stu.get("arrival_delay"),
            stu.get("departure_delay"),
        ))

    # Sort each group by stop_sequence once — None sorts last
    for eid in index:
        index[eid].sort(key=lambda t: (t[0] is None, t[0]))

    return index


def _compute_tu_hash(row: dict, stu_index: dict[str, list[tuple]]) -> str:
    """
    Compute a deduplication hash for a TripUpdate.
    Hash includes trip-level state + all stop-level states.

    Uses pre-grouped stu_index for O(1) stop lookup instead of O(m)
    DataFrame filter on each call.
    """
    parts = [
        str(row.get("trip_id", "")),
        str(row.get("start_date", "")),
        str(row.get("schedule_relationship", "")),
        str(row.get("delay", "")),
    ]

    entity_id = row.get("feed_entity_id")
    for stop_seq, sched_rel, arr_delay, dep_delay in stu_index.get(entity_id, []):
        parts.append(f"{stop_seq}:{sched_rel}:{arr_delay}:{dep_delay}")

    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ── Transform rules ──────────────────────────────────────────────────────────


def _apply_trip_update_rules(
    trip_updates: pd.DataFrame,
    stop_time_updates: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Apply Rules 1, 2, 3 to TripUpdates and StopTimeUpdates.

    Returns:
      interruptions   — Interruption rows from Rules 1, 2, 3
      sources         — interruption_id → source_entity_id mappings
      affects_trip    — interruption_id → trip_id
      affects_route   — interruption_id → route_id

    Performance: pre-indexes trip_updates by feed_entity_id so Rule 3's
    parent lookup is O(1) instead of O(n) per skipped stop.
    """
    int_rows: list[dict] = []
    src_rows: list[dict] = []
    trip_rows: list[dict] = []
    route_rows: list[dict] = []

    # ── Rules 1 + 2: iterate TripUpdates ─────────────────────────────────────

    for _, tu in trip_updates.iterrows():
        entity_id  = tu["feed_entity_id"]
        trip_id    = tu.get("trip_id")
        route_id   = tu.get("route_id")
        start_date = tu.get("start_date")
        timestamp  = tu.get("timestamp")

        # Rule 1: CANCELED → Cancellation
        if tu["schedule_relationship"] == "CANCELED":
            int_id = f"int_tu_{entity_id}"
            int_rows.append({
                "interruption_id":   int_id,
                "interruption_type": "cancellation",
                "label":             "Cancellation",
                "cause":             None,
                "effect":            "NO_SERVICE",
                "severity":          "SEVERE",
                "start_time":        timestamp,
                "end_time":          None,
                "description":       f"Trip {trip_id} cancelled",
                "date":              start_date,
            })
            src_rows.append({
                "interruption_id":  int_id,
                "source_entity_id": entity_id,
                "source_type":      "TripUpdate",
            })
            if trip_id:
                trip_rows.append({"interruption_id": int_id, "trip_id": trip_id})
            if route_id:
                route_rows.append({"interruption_id": int_id, "route_id": route_id})

        # Rule 2: delay >= threshold → Delay
        elif tu.get("delay") is not None and tu["delay"] >= DELAY_THRESHOLD:
            delay    = tu["delay"]
            int_id   = f"int_tu_{entity_id}"
            severity = "SEVERE" if delay >= SEVERE_DELAY else "WARNING"
            int_rows.append({
                "interruption_id":   int_id,
                "interruption_type": "delay",
                "label":             "Delay",
                "cause":             None,
                "effect":            "SIGNIFICANT_DELAYS",
                "severity":          severity,
                "start_time":        timestamp,
                "end_time":          None,
                "description":       f"Trip {trip_id} delayed {delay}s",
                "date":              start_date,
            })
            src_rows.append({
                "interruption_id":  int_id,
                "source_entity_id": entity_id,
                "source_type":      "TripUpdate",
            })
            if trip_id:
                trip_rows.append({"interruption_id": int_id, "trip_id": trip_id})
            if route_id:
                route_rows.append({"interruption_id": int_id, "route_id": route_id})

    # ── Rule 3: SKIPPED stops → Skip ─────────────────────────────────────────
    #
    # Pre-index trip_updates by feed_entity_id — O(n) once.
    # Without this, each skipped stop scans all trip_update rows: O(n×skipped).

    if not stop_time_updates.empty:
        tu_by_entity: dict[str, dict] = {
            row["feed_entity_id"]: row
            for row in trip_updates.to_dict(orient="records")
        }

        skipped = stop_time_updates[
            stop_time_updates["schedule_relationship"] == "SKIPPED"
        ]
        for _, stu in skipped.iterrows():
            parent_id  = stu["parent_entity_id"]
            stop_seq   = stu.get("stop_sequence", 0)
            stop_id    = stu.get("stop_id")
            int_id     = f"int_skip_{parent_id}_{stop_seq}"

            # O(1) dict lookup replaces O(n) DataFrame filter
            parent       = tu_by_entity.get(parent_id, {})
            parent_date  = parent.get("start_date")
            parent_time  = parent.get("timestamp")
            parent_trip  = parent.get("trip_id")
            parent_route = parent.get("route_id")

            int_rows.append({
                "interruption_id":   int_id,
                "interruption_type": "skip",
                "label":             "Skip",
                "cause":             None,
                "effect":            "STOP_MOVED",
                "severity":          "WARNING",
                "start_time":        parent_time,
                "end_time":          None,
                "description":       f"Stop {stop_id} skipped (seq {stop_seq})",
                "date":              parent_date,
            })
            src_rows.append({
                "interruption_id":  int_id,
                "source_entity_id": parent_id,
                "source_type":      "TripUpdate",
            })
            if parent_trip:
                trip_rows.append({"interruption_id": int_id, "trip_id": parent_trip})
            if parent_route:
                route_rows.append({"interruption_id": int_id, "route_id": parent_route})

    return (
        pd.DataFrame(int_rows)   if int_rows   else _empty_interruptions(),
        pd.DataFrame(src_rows)   if src_rows   else _empty_sources(),
        pd.DataFrame(trip_rows)  if trip_rows  else pd.DataFrame(columns=["interruption_id", "trip_id"]),
        pd.DataFrame(route_rows) if route_rows else pd.DataFrame(columns=["interruption_id", "route_id"]),
    )


def _apply_alert_rules(
    service_alerts: pd.DataFrame,
    entity_selectors: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Apply Rule 4 to ServiceAlerts.

    Returns:
      interruptions   — Interruption rows
      sources         — interruption_id → source_entity_id
      affects_trip    — interruption_id → trip_id
      affects_route   — interruption_id → route_id
      affects_stop    — interruption_id → stop_id

    Performance: pre-indexes entity_selectors by parent_entity_id so the
    selector lookup per alert is O(1) instead of O(k) where k is the total
    number of EntitySelector rows.
    """
    int_rows: list[dict] = []
    src_rows: list[dict] = []
    trip_rows: list[dict] = []
    route_rows: list[dict] = []
    stop_rows: list[dict] = []

    # Pre-index EntitySelectors by parent_entity_id — O(k) once
    sel_by_parent: dict[str, list[dict]] = {}
    if not entity_selectors.empty:
        for row in entity_selectors.to_dict(orient="records"):
            eid = row["parent_entity_id"]
            if eid not in sel_by_parent:
                sel_by_parent[eid] = []
            sel_by_parent[eid].append(row)

    for _, alert in service_alerts.iterrows():
        effect = alert.get("effect", "")
        if effect not in EFFECT_TYPE_MAP:
            continue

        entity_id = alert["feed_entity_id"]
        int_id    = f"int_sa_{entity_id}"
        int_type  = EFFECT_TYPE_MAP[effect]

        # Derive date from active_period_start (epoch → YYYYMMDD)
        alert_date = None
        if alert.get("active_period_start"):
            try:
                dt = datetime.fromtimestamp(int(alert["active_period_start"]))
                alert_date = dt.strftime("%Y%m%d")
            except (ValueError, TypeError, OSError):
                pass

        int_rows.append({
            "interruption_id":   int_id,
            "interruption_type": int_type,
            "label":             TYPE_LABEL_MAP.get(int_type, "ServiceChange"),
            "cause":             alert.get("cause"),
            "effect":            effect,
            "severity":          alert.get("severity_level", "UNKNOWN_SEVERITY"),
            "start_time":        alert.get("active_period_start"),
            "end_time":          alert.get("active_period_end"),
            "description":       alert.get("header_text"),
            "date":              alert_date,
        })
        src_rows.append({
            "interruption_id":  int_id,
            "source_entity_id": entity_id,
            "source_type":      "ServiceAlert",
        })

        # O(1) selector lookup replaces O(k) DataFrame filter per alert
        for sel in sel_by_parent.get(entity_id, []):
            if sel.get("trip_id") and pd.notna(sel["trip_id"]):
                trip_rows.append({"interruption_id": int_id, "trip_id": sel["trip_id"]})
            if sel.get("route_id") and pd.notna(sel["route_id"]):
                route_rows.append({"interruption_id": int_id, "route_id": sel["route_id"]})
            if sel.get("stop_id") and pd.notna(sel["stop_id"]):
                stop_rows.append({"interruption_id": int_id, "stop_id": sel["stop_id"]})

    return (
        pd.DataFrame(int_rows)   if int_rows   else _empty_interruptions(),
        pd.DataFrame(src_rows)   if src_rows   else _empty_sources(),
        pd.DataFrame(trip_rows)  if trip_rows  else pd.DataFrame(columns=["interruption_id", "trip_id"]),
        pd.DataFrame(route_rows) if route_rows else pd.DataFrame(columns=["interruption_id", "route_id"]),
        pd.DataFrame(stop_rows)  if stop_rows  else pd.DataFrame(columns=["interruption_id", "stop_id"]),
    )


def _empty_interruptions() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "interruption_id", "interruption_type", "label", "cause", "effect",
        "severity", "start_time", "end_time", "description", "date",
    ])


def _empty_sources() -> pd.DataFrame:
    return pd.DataFrame(columns=["interruption_id", "source_entity_id", "source_type"])


# ── Main entry point ─────────────────────────────────────────────────────────


def run(raw: dict[str, pd.DataFrame]) -> InterruptionTransformResult:
    """
    Transform raw GTFS-RT DataFrames into the three-tier disruption model.
    """
    log.info("interruption transform: starting")

    trip_updates      = raw["trip_updates"]
    stop_time_updates = raw["stop_time_updates"]
    service_alerts    = raw["service_alerts"]
    entity_selectors  = raw["entity_selectors"]
    feed_info         = raw.get("feed_info")

    # ── Dedup hashes on TripUpdates ──────────────────────────────────────────
    #
    # Build STU index once — O(m). Hash computation drops from O(n×m) to
    # O(n+m). With n=2,205 and m=27,652: ~60M → ~30K operations.

    if not trip_updates.empty:
        trip_updates = trip_updates.copy()
        stu_index = _build_stu_index(stop_time_updates)
        trip_updates["dedup_hash"] = trip_updates.apply(
            lambda row: _compute_tu_hash(row.to_dict(), stu_index),
            axis=1,
        )
        before = len(trip_updates)
        trip_updates = trip_updates.drop_duplicates(subset=["dedup_hash"])
        log.info(
            "interruption transform: TripUpdate dedup %d → %d",
            before, len(trip_updates),
        )

    # ── ServiceAlert dedup on feed_entity_id ─────────────────────────────────

    if not service_alerts.empty:
        before = len(service_alerts)
        service_alerts = service_alerts.drop_duplicates(subset=["feed_entity_id"])
        log.info(
            "interruption transform: ServiceAlert dedup %d → %d",
            before, len(service_alerts),
        )

    # ── Apply transform rules ────────────────────────────────────────────────

    tu_ints, tu_srcs, tu_trips, tu_routes = _apply_trip_update_rules(
        trip_updates, stop_time_updates
    )
    sa_ints, sa_srcs, sa_trips, sa_routes, sa_stops = _apply_alert_rules(
        service_alerts, entity_selectors
    )

    # Combine Tier 2 results
    interruptions = pd.concat([tu_ints, sa_ints], ignore_index=True)
    sources       = pd.concat([tu_srcs, sa_srcs], ignore_index=True)
    affects_trip  = pd.concat([tu_trips, sa_trips], ignore_index=True).drop_duplicates()
    affects_route = pd.concat([tu_routes, sa_routes], ignore_index=True).drop_duplicates()
    affects_stop  = sa_stops.drop_duplicates() if not sa_stops.empty else sa_stops

    # ── Stats ────────────────────────────────────────────────────────────────

    stats = {
        "trip_updates":         len(trip_updates),
        "stop_time_updates":    len(stop_time_updates),
        "service_alerts":       len(service_alerts),
        "entity_selectors":     len(entity_selectors),
        "interruptions":        len(interruptions),
        "interruption_sources": len(sources),
        "affects_trip":         len(affects_trip),
        "affects_route":        len(affects_route),
        "affects_stop":         len(affects_stop),
    }
    for k, v in stats.items():
        log.info("interruption transform: %-25s %6d rows", k, v)

    if not interruptions.empty:
        for itype, count in interruptions["interruption_type"].value_counts().items():
            log.info("interruption transform:   type=%-20s %d", itype, count)

    # ── Pre-load validation ───────────────────────────────────────────────────

    log.info("interruption transform: running pre-load validation")
    from src.common.validators.interruption import validate_pre_load
    validation = validate_pre_load(
        trip_updates=trip_updates,
        stop_time_updates=stop_time_updates,
        service_alerts=service_alerts,
        entity_selectors=entity_selectors,
        interruptions=interruptions,
        interruption_sources=sources,
    )
    log.info(
        "interruption transform: pre-load validation result:\n%s",
        validation.summary(),
    )
    if not validation.passed:
        raise ValueError(
            f"Interruption layer pre-load validation failed — aborting pipeline:\n"
            f"{validation.summary()}"
        )

    log.info("interruption transform: complete")

    return InterruptionTransformResult(
        trip_updates=trip_updates,
        stop_time_updates=stop_time_updates,
        service_alerts=service_alerts,
        entity_selectors=entity_selectors,
        interruptions=interruptions,
        interruption_sources=sources,
        affects_trip=affects_trip,
        affects_route=affects_route,
        affects_stop=affects_stop,
        feed_info=feed_info,
        stats=stats,
    )
