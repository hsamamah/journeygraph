# src/layers/service_schedule/load.py
"""
Service & Schedule layer — Load

Writes all service nodes and relationships to Neo4j in dependency order.
Uses batch_write for large datasets (trips, stop_times).

Load order:
  Phase 1 — Constraints
  Phase 2 — Nodes: FeedInfo, Agency, Route (:Bus/:Rail), RoutePattern,
            ServicePattern (by label), Date, Trip
  Phase 3 — Internal relationships: OPERATES, OPERATED_BY, HAS_PATTERN,
            BELONGS_TO, HAS_TRIP, FOLLOWS, OPERATED_ON, ACTIVE_ON, FROM_FEED
  Phase 4 — Cross-layer relationships (require Physical layer):
            SERVES, STOPS_AT, SCHEDULED_AT (batched)

Prerequisite: Physical layer must have committed Station, Platform, and
BusStop nodes before Phase 4 runs.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from src.common.feed_info import ensure_feed_info
from src.common.logger import get_logger
from src.common.neo4j_tools import Neo4jManager
from src.layers.service_schedule.transform import ServiceTransformResult

log = get_logger(__name__)

# Resolve Cypher query files relative to repo root
_QUERY_DIR = Path(__file__).parents[3] / "queries" / "service_schedule"

SCHEDULED_AT_BATCH_SIZE = 5_000


# ── Cypher helpers ───────────────────────────────────────────────────────────


def _load_query(filename: str) -> str:
    path = _QUERY_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Cypher file not found: {path}")
    return path.read_text(encoding="utf-8")


def _extract_statement(cypher: str, label_hint: str) -> str:
    """
    Extract a single UNWIND statement from a multi-statement Cypher file
    by matching the comment line containing label_hint.
    """
    blocks = re.split(r"\n(?=//)", cypher)
    for block in blocks:
        if label_hint in block:
            lines = [ln for ln in block.splitlines() if not ln.strip().startswith("//")]
            stmt = "\n".join(lines).strip().rstrip(";")
            if stmt:
                return stmt
    raise ValueError(
        f"Could not find Cypher statement with hint '{label_hint}' "
        f"in service_schedule queries"
    )


def _split_statements(cypher: str) -> list[str]:
    statements = []
    for raw in cypher.split(";"):
        lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("//")]
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)
    return statements


def _df_to_rows(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of dicts, replacing NaN/NaT with None."""
    return [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]


# ── Phase 1: Constraints ─────────────────────────────────────────────────────


def _load_constraints(neo4j: Neo4jManager) -> None:
    log.info("service load: applying constraints")
    cypher = _load_query("constraints.cypher")
    for stmt in _split_statements(cypher):
        neo4j.execute_write(stmt)


# ── Phase 2: Nodes ───────────────────────────────────────────────────────────


def _load_agency(neo4j: Neo4jManager, result: ServiceTransformResult) -> None:
    log.info("service load: Agency (%d nodes)", len(result.agency))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Agency")
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(result.agency)})


def _load_routes(neo4j: Neo4jManager, result: ServiceTransformResult) -> None:
    nodes_cypher = _load_query("nodes.cypher")

    if not result.routes_bus.empty:
        log.info("service load: Route:Bus (%d nodes)", len(result.routes_bus))
        cypher = _extract_statement(nodes_cypher, ":Route:Bus")
        neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(result.routes_bus)})

    if not result.routes_rail.empty:
        log.info("service load: Route:Rail (%d nodes)", len(result.routes_rail))
        cypher = _extract_statement(nodes_cypher, ":Route:Rail")
        neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(result.routes_rail)})


def _load_route_patterns(neo4j: Neo4jManager, result: ServiceTransformResult) -> None:
    log.info("service load: RoutePattern (%d nodes)", len(result.route_patterns))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":RoutePattern")
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(result.route_patterns)})


def _load_service_patterns(neo4j: Neo4jManager, result: ServiceTransformResult) -> None:
    """Load ServicePattern nodes split by label for multi-label MERGE."""
    nodes_cypher = _load_query("nodes.cypher")

    label_map = {
        "Weekday": ":ServicePattern:Weekday",
        "Saturday": ":ServicePattern:Saturday",
        "Sunday": ":ServicePattern:Sunday",
        "Holiday": ":ServicePattern:Holiday",
        "Maintenance": ":ServicePattern:Maintenance",
    }

    for label, hint in label_map.items():
        subset = result.service_patterns[result.service_patterns["label"] == label]
        if subset.empty:
            continue
        log.info("service load: ServicePattern:%s (%d nodes)", label, len(subset))
        cypher = _extract_statement(nodes_cypher, hint)
        neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(subset)})


def _load_dates(neo4j: Neo4jManager, result: ServiceTransformResult) -> None:
    log.info("service load: Date (%d nodes)", len(result.dates))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Date")
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(result.dates)})


def _load_trips(neo4j: Neo4jManager, result: ServiceTransformResult) -> None:
    log.info("service load: Trip (%d nodes)", len(result.trips))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Trip")
    neo4j.batch_write(
        cypher,
        _df_to_rows(result.trips),
        batch_size=SCHEDULED_AT_BATCH_SIZE,
        label="Trip nodes",
    )


# ── Phase 3: Internal relationships ──────────────────────────────────────────


def _load_internal_rels(neo4j: Neo4jManager, result: ServiceTransformResult) -> None:
    rel_cypher = _load_query("relationships.cypher")

    # Agency ↔ Route (both directions from same data)
    all_routes = pd.concat([result.routes_bus, result.routes_rail], ignore_index=True)
    if "agency_id" not in all_routes.columns:
        # WMATA has a single agency — pull from agency DataFrame
        agency_id = result.agency.iloc[0]["agency_id"] if not result.agency.empty else "1"
        all_routes["agency_id"] = agency_id

    agency_route_rows = _df_to_rows(all_routes[["agency_id", "route_id"]].drop_duplicates())

    log.info("service load: Agency -[:OPERATES]-> Route (%d rels)", len(agency_route_rows))
    neo4j.execute_write(
        _extract_statement(rel_cypher, "Agency -[:OPERATES]"),
        parameters={"rows": agency_route_rows},
    )

    log.info("service load: Route -[:OPERATED_BY]-> Agency (%d rels)", len(agency_route_rows))
    neo4j.execute_write(
        _extract_statement(rel_cypher, "Route -[:OPERATED_BY]"),
        parameters={"rows": agency_route_rows},
    )

    # Route ↔ RoutePattern
    rp_rows = _df_to_rows(result.route_patterns[["route_id", "shape_id"]].drop_duplicates())
    log.info("service load: Route -[:HAS_PATTERN]-> RoutePattern (%d rels)", len(rp_rows))
    neo4j.execute_write(
        _extract_statement(rel_cypher, "Route -[:HAS_PATTERN]"),
        parameters={"rows": rp_rows},
    )
    log.info("service load: RoutePattern -[:BELONGS_TO]-> Route (%d rels)", len(rp_rows))
    neo4j.execute_write(
        _extract_statement(rel_cypher, "RoutePattern -[:BELONGS_TO]"),
        parameters={"rows": rp_rows},
    )

    # RoutePattern ↔ Trip
    trip_pattern = result.trips[["trip_id", "shape_id"]].dropna(subset=["shape_id"])
    tp_rows = _df_to_rows(trip_pattern)

    log.info("service load: RoutePattern -[:HAS_TRIP]-> Trip (%d rels)", len(tp_rows))
    neo4j.batch_write(
        _extract_statement(rel_cypher, "RoutePattern -[:HAS_TRIP]"),
        tp_rows,
        batch_size=SCHEDULED_AT_BATCH_SIZE,
        label="HAS_TRIP",
    )

    log.info("service load: Trip -[:FOLLOWS]-> RoutePattern (%d rels)", len(tp_rows))
    neo4j.batch_write(
        _extract_statement(rel_cypher, "Trip -[:FOLLOWS]"),
        tp_rows,
        batch_size=SCHEDULED_AT_BATCH_SIZE,
        label="FOLLOWS",
    )

    # Trip -[:OPERATED_ON]-> ServicePattern
    trip_svc = result.trips[["trip_id", "service_id"]].dropna(subset=["service_id"])
    ts_rows = _df_to_rows(trip_svc)
    log.info("service load: Trip -[:OPERATED_ON]-> ServicePattern (%d rels)", len(ts_rows))
    neo4j.batch_write(
        _extract_statement(rel_cypher, "Trip -[:OPERATED_ON]"),
        ts_rows,
        batch_size=SCHEDULED_AT_BATCH_SIZE,
        label="OPERATED_ON",
    )

    # ServicePattern -[:ACTIVE_ON]-> Date
    ao_rows = _df_to_rows(result.active_on)
    log.info("service load: ServicePattern -[:ACTIVE_ON]-> Date (%d rels)", len(ao_rows))
    neo4j.batch_write(
        _extract_statement(rel_cypher, "ServicePattern -[:ACTIVE_ON]"),
        ao_rows,
        batch_size=SCHEDULED_AT_BATCH_SIZE,
        label="ACTIVE_ON",
    )


def _load_from_feed_rels(neo4j: Neo4jManager, result: ServiceTransformResult) -> None:
    """Wire FROM_FEED relationships from all service nodes to FeedInfo."""
    rel_cypher = _load_query("relationships.cypher")
    fv = [{"feed_version": result.feed_version}]

    # Small node sets — single query per type
    for hint in [
        "Agency -[:FROM_FEED]",
        "Route -[:FROM_FEED]",
        "RoutePattern -[:FROM_FEED]",
        "ServicePattern -[:FROM_FEED]",
        "Date -[:FROM_FEED]",
    ]:
        log.info("service load: %s-> FeedInfo", hint.split("-")[0].strip())
        neo4j.execute_write(
            _extract_statement(rel_cypher, hint),
            parameters={"rows": fv},
        )

    # Trip FROM_FEED — large set, needs per-trip rows
    trip_feed = result.trips[["trip_id"]].copy()
    trip_feed["feed_version"] = result.feed_version
    log.info("service load: Trip -[:FROM_FEED]-> FeedInfo (%d rels)", len(trip_feed))
    neo4j.batch_write(
        _extract_statement(rel_cypher, "Trip -[:FROM_FEED]"),
        _df_to_rows(trip_feed),
        batch_size=SCHEDULED_AT_BATCH_SIZE,
        label="Trip FROM_FEED",
    )


# ── Phase 4: Cross-layer relationships ───────────────────────────────────────


def _load_cross_layer_rels(neo4j: Neo4jManager, result: ServiceTransformResult) -> None:
    """
    Load relationships that target Physical layer nodes.
    Silently skips rows where the target node doesn't exist (MATCH fails).
    """
    rel_cypher = _load_query("relationships.cypher")

    # Route -[:SERVES]-> Station
    if not result.route_serves_station.empty:
        rows = _df_to_rows(result.route_serves_station)
        log.info("service load: Route -[:SERVES]-> Station (%d rels)", len(rows))
        neo4j.batch_write(
            _extract_statement(rel_cypher, "Route -[:SERVES]-> Station"),
            rows,
            batch_size=SCHEDULED_AT_BATCH_SIZE,
            label="SERVES Station",
        )

    # Route -[:SERVES]-> BusStop
    if not result.route_serves_busstop.empty:
        rows = _df_to_rows(result.route_serves_busstop)
        log.info("service load: Route -[:SERVES]-> BusStop (%d rels)", len(rows))
        neo4j.batch_write(
            _extract_statement(rel_cypher, "Route -[:SERVES]-> BusStop"),
            rows,
            batch_size=SCHEDULED_AT_BATCH_SIZE,
            label="SERVES BusStop",
        )

    # RoutePattern -[:STOPS_AT]-> Platform
    rail_stops = result.pattern_stops_at[
        result.pattern_stops_at["stop_id"].str.startswith("PF_", na=False)
    ]
    if not rail_stops.empty:
        rows = _df_to_rows(rail_stops)
        log.info("service load: RoutePattern -[:STOPS_AT]-> Platform (%d rels)", len(rows))
        neo4j.batch_write(
            _extract_statement(rel_cypher, "RoutePattern -[:STOPS_AT]-> Platform"),
            rows,
            batch_size=SCHEDULED_AT_BATCH_SIZE,
            label="STOPS_AT Platform",
        )

    # RoutePattern -[:STOPS_AT]-> BusStop
    bus_stops = result.pattern_stops_at[
        ~result.pattern_stops_at["stop_id"].str.startswith("PF_", na=False)
    ]
    if not bus_stops.empty:
        rows = _df_to_rows(bus_stops)
        log.info("service load: RoutePattern -[:STOPS_AT]-> BusStop (%d rels)", len(rows))
        neo4j.batch_write(
            _extract_statement(rel_cypher, "RoutePattern -[:STOPS_AT]-> BusStop"),
            rows,
            batch_size=SCHEDULED_AT_BATCH_SIZE,
            label="STOPS_AT BusStop",
        )

    # Trip -[:SCHEDULED_AT]-> Platform (rail) — batched
    if not result.scheduled_at_rail.empty:
        rows = _df_to_rows(result.scheduled_at_rail)
        log.info("service load: Trip -[:SCHEDULED_AT]-> Platform (%d rels)", len(rows))
        neo4j.batch_write(
            _extract_statement(rel_cypher, "Trip -[:SCHEDULED_AT]-> Platform"),
            rows,
            batch_size=SCHEDULED_AT_BATCH_SIZE,
            label="SCHEDULED_AT rail",
        )

    # Trip -[:SCHEDULED_AT]-> BusStop (bus) — batched
    if not result.scheduled_at_bus.empty:
        rows = _df_to_rows(result.scheduled_at_bus)
        log.info("service load: Trip -[:SCHEDULED_AT]-> BusStop (%d rels)", len(rows))
        neo4j.batch_write(
            _extract_statement(rel_cypher, "Trip -[:SCHEDULED_AT]-> BusStop"),
            rows,
            batch_size=SCHEDULED_AT_BATCH_SIZE,
            label="SCHEDULED_AT bus",
        )


# ── Main entry point ─────────────────────────────────────────────────────────


def run(result: ServiceTransformResult, neo4j: Neo4jManager) -> None:
    """
    Load all service layer nodes and relationships into Neo4j.
    """
    log.info("service load: starting")

    # Phase 1
    _load_constraints(neo4j)

    # Phase 2 — Nodes (FeedInfo via shared utility)
    feed_version = ensure_feed_info(neo4j, result.feed_info)
    _load_agency(neo4j, result)
    _load_routes(neo4j, result)
    _load_route_patterns(neo4j, result)
    _load_service_patterns(neo4j, result)
    _load_dates(neo4j, result)
    _load_trips(neo4j, result)

    # Phase 3 — Internal relationships
    _load_internal_rels(neo4j, result)
    _load_from_feed_rels(neo4j, result)

    # Phase 4 — Cross-layer relationships
    _load_cross_layer_rels(neo4j, result)

    log.info("service load: complete — stats: %s", result.stats)
