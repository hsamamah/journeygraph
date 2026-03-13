# src/layers/interruption/load.py
"""
Interruption layer — Load

Writes all three tiers of the disruption model to Neo4j:

  Phase 1 — Constraints
  Phase 2 — FeedInfo (shared utility)
  Phase 3 — Tier 1 nodes: TripUpdate, StopTimeUpdate, ServiceAlert, EntitySelector
  Phase 4 — Tier 2 nodes: Interruption (by multi-label variant)
  Phase 5 — Tier 1 relationships: UPDATES, ON_DATE, HAS_STOP_UPDATE, AT_STOP,
            HAS_SELECTOR, FROM_FEED, TARGETS_*
  Phase 6 — Tier 2 relationships: SOURCED_FROM
  Phase 7 — Tier 3 relationships: AFFECTS_TRIP, AFFECTS_ROUTE, AFFECTS_STOP, ON_DATE
  Phase 8 — Post-load enrichment: Rule 6 (DURING_PLANNED_SERVICE)

Prerequisites:
  Service layer must be loaded (Trip, Route, Date, ServicePattern nodes).
  Physical layer should be loaded for AT_STOP and AFFECTS_STOP connections.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from src.common.feed_info import ensure_feed_info
from src.common.logger import get_logger
from src.common.neo4j_tools import Neo4jManager
from src.layers.interruption.transform import InterruptionTransformResult

log = get_logger(__name__)

_QUERY_DIR = Path(__file__).parents[3] / "queries" / "interruption"

BATCH_SIZE = 5_000


# ── Cypher helpers ───────────────────────────────────────────────────────────


def _load_query(filename: str) -> str:
    path = _QUERY_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Cypher file not found: {path}")
    return path.read_text(encoding="utf-8")


def _extract_statement(cypher: str, label_hint: str) -> str:
    """Extract a single UNWIND statement by matching the '// ── ' block header."""
    blocks = re.split(r"\n(?=// ── )", cypher)
    for block in blocks:
        if label_hint in block:
            lines = [ln for ln in block.splitlines() if not ln.strip().startswith("//")]
            stmt = "\n".join(lines).strip().rstrip(";")
            if stmt:
                return stmt
    raise ValueError(
        f"Could not find Cypher statement with hint '{label_hint}' "
        f"in interruption queries"
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
    return [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]


# ── Phase 3: Tier 1 nodes ───────────────────────────────────────────────────


def _load_tier1_nodes(neo4j: Neo4jManager, result: InterruptionTransformResult) -> None:
    nodes_cypher = _load_query("nodes.cypher")

    if not result.trip_updates.empty:
        log.info("interruption load: TripUpdate (%d nodes)", len(result.trip_updates))
        cypher = _extract_statement(nodes_cypher, ":TripUpdate")
        neo4j.batch_write(cypher, _df_to_rows(result.trip_updates),
                          batch_size=BATCH_SIZE, label="TripUpdate")

    if not result.stop_time_updates.empty:
        log.info("interruption load: StopTimeUpdate (%d nodes)", len(result.stop_time_updates))
        cypher = _extract_statement(nodes_cypher, ":StopTimeUpdate")
        neo4j.batch_write(cypher, _df_to_rows(result.stop_time_updates),
                          batch_size=BATCH_SIZE, label="StopTimeUpdate")

    if not result.service_alerts.empty:
        log.info("interruption load: ServiceAlert (%d nodes)", len(result.service_alerts))
        cypher = _extract_statement(nodes_cypher, ":ServiceAlert")
        neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(result.service_alerts)})

    if not result.entity_selectors.empty:
        log.info("interruption load: EntitySelector (%d nodes)", len(result.entity_selectors))
        cypher = _extract_statement(nodes_cypher, ":EntitySelector")
        neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(result.entity_selectors)})


# ── Phase 4: Tier 2 nodes (Interruption by label) ───────────────────────────


def _load_interruption_nodes(neo4j: Neo4jManager, result: InterruptionTransformResult) -> None:
    if result.interruptions.empty:
        log.warning("interruption load: no Interruption nodes to create")
        return

    nodes_cypher = _load_query("nodes.cypher")

    label_map = {
        "Cancellation": ":Interruption:Cancellation",
        "Delay": ":Interruption:Delay",
        "Skip": ":Interruption:Skip",
        "Detour": ":Interruption:Detour",
        "ServiceChange": ":Interruption:ServiceChange",
        "Accessibility": ":Interruption:Accessibility",
    }

    for label, hint in label_map.items():
        subset = result.interruptions[result.interruptions["label"] == label]
        if subset.empty:
            continue
        log.info("interruption load: Interruption:%s (%d nodes)", label, len(subset))
        cypher = _extract_statement(nodes_cypher, hint)
        neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(subset)})


# ── Phase 5: Tier 1 relationships ───────────────────────────────────────────


def _load_tier1_rels(
    neo4j: Neo4jManager,
    result: InterruptionTransformResult,
    feed_version: str,
) -> None:
    rel_cypher = _load_query("relationships.cypher")

    # TripUpdate -[:UPDATES]-> Trip
    if not result.trip_updates.empty:
        rows = result.trip_updates[result.trip_updates["trip_id"].notna()][["dedup_hash", "trip_id"]]
        if not rows.empty:
            log.info("interruption load: TripUpdate -[:UPDATES]-> Trip (%d)", len(rows))
            neo4j.batch_write(
                _extract_statement(rel_cypher, "TripUpdate -[:UPDATES]-> Trip"),
                _df_to_rows(rows), batch_size=BATCH_SIZE, label="UPDATES",
            )

    # TripUpdate -[:ON_DATE]-> Date
    if not result.trip_updates.empty:
        rows = result.trip_updates[result.trip_updates["start_date"].notna()][["dedup_hash", "start_date"]]
        rows = rows.rename(columns={"start_date": "date"})
        if not rows.empty:
            log.info("interruption load: TripUpdate -[:ON_DATE]-> Date (%d)", len(rows))
            neo4j.batch_write(
                _extract_statement(rel_cypher, "TripUpdate -[:ON_DATE]-> Date"),
                _df_to_rows(rows), batch_size=BATCH_SIZE, label="TU ON_DATE",
            )

    # TripUpdate -[:HAS_STOP_UPDATE]-> StopTimeUpdate
    if not result.stop_time_updates.empty and not result.trip_updates.empty:
        # Need dedup_hash from parent TripUpdate
        hash_map = result.trip_updates.set_index("feed_entity_id")["dedup_hash"].to_dict()
        stu = result.stop_time_updates.copy()
        stu["dedup_hash"] = stu["parent_entity_id"].map(hash_map)
        rows = stu[stu["dedup_hash"].notna()][["dedup_hash", "parent_entity_id", "stop_sequence"]]
        if not rows.empty:
            log.info("interruption load: TripUpdate -[:HAS_STOP_UPDATE]-> STU (%d)", len(rows))
            neo4j.batch_write(
                _extract_statement(rel_cypher, "TripUpdate -[:HAS_STOP_UPDATE]"),
                _df_to_rows(rows), batch_size=BATCH_SIZE, label="HAS_STOP_UPDATE",
            )

    # TripUpdate -[:FROM_FEED]-> FeedInfo
    if not result.trip_updates.empty:
        rows = result.trip_updates[["dedup_hash"]].copy()
        rows["feed_version"] = feed_version
        log.info("interruption load: TripUpdate -[:FROM_FEED]-> FeedInfo (%d)", len(rows))
        neo4j.batch_write(
            _extract_statement(rel_cypher, "TripUpdate -[:FROM_FEED]"),
            _df_to_rows(rows), batch_size=BATCH_SIZE, label="TU FROM_FEED",
        )

    # StopTimeUpdate -[:AT_STOP]-> Platform|BusStop
    if not result.stop_time_updates.empty:
        rows = result.stop_time_updates[result.stop_time_updates["stop_id"].notna()][
            ["parent_entity_id", "stop_sequence", "stop_id"]
        ]
        if not rows.empty:
            log.info("interruption load: StopTimeUpdate -[:AT_STOP] (%d)", len(rows))
            neo4j.batch_write(
                _extract_statement(rel_cypher, "StopTimeUpdate -[:AT_STOP]"),
                _df_to_rows(rows), batch_size=BATCH_SIZE, label="AT_STOP",
            )

    # ServiceAlert -[:HAS_SELECTOR]-> EntitySelector
    if not result.entity_selectors.empty:
        rows = result.entity_selectors[["parent_entity_id", "selector_group_id"]].copy()
        rows = rows.rename(columns={"parent_entity_id": "feed_entity_id"})
        log.info("interruption load: ServiceAlert -[:HAS_SELECTOR] (%d)", len(rows))
        neo4j.execute_write(
            _extract_statement(rel_cypher, "ServiceAlert -[:HAS_SELECTOR]"),
            parameters={"rows": _df_to_rows(rows)},
        )

    # ServiceAlert -[:ACTIVE_ON]-> Date (derive dates from active_period)
    if not result.service_alerts.empty:
        sa_dates = result.interruptions[
            result.interruptions["interruption_id"].str.startswith("int_sa_")
        ][["date"]].dropna()
        if not sa_dates.empty:
            # Map back to feed_entity_id
            sa_with_dates = result.service_alerts[["feed_entity_id"]].copy()
            # Use the date from the interruption that was derived from this alert
            int_sa = result.interruptions[
                result.interruptions["interruption_id"].str.startswith("int_sa_")
            ].copy()
            int_sa["feed_entity_id"] = int_sa["interruption_id"].str.replace("int_sa_", "", regex=False)
            date_rows = int_sa[int_sa["date"].notna()][["feed_entity_id", "date"]]
            if not date_rows.empty:
                log.info("interruption load: ServiceAlert -[:ACTIVE_ON]-> Date (%d)", len(date_rows))
                neo4j.execute_write(
                    _extract_statement(rel_cypher, "ServiceAlert -[:ACTIVE_ON]-> Date"),
                    parameters={"rows": _df_to_rows(date_rows)},
                )

    # ServiceAlert -[:FROM_FEED]-> FeedInfo
    if not result.service_alerts.empty:
        rows = result.service_alerts[["feed_entity_id"]].copy()
        rows["feed_version"] = feed_version
        log.info("interruption load: ServiceAlert -[:FROM_FEED]-> FeedInfo (%d)", len(rows))
        neo4j.execute_write(
            _extract_statement(rel_cypher, "ServiceAlert -[:FROM_FEED]"),
            parameters={"rows": _df_to_rows(rows)},
        )

    # EntitySelector -[:TARGETS_*] relationships
    if not result.entity_selectors.empty:
        es = result.entity_selectors

        route_rows = es[es["route_id"].notna()][["selector_group_id", "route_id"]]
        if not route_rows.empty:
            log.info("interruption load: EntitySelector -[:TARGETS_ROUTE] (%d)", len(route_rows))
            neo4j.execute_write(
                _extract_statement(rel_cypher, "EntitySelector -[:TARGETS_ROUTE]"),
                parameters={"rows": _df_to_rows(route_rows)},
            )

        trip_rows = es[es["trip_id"].notna()][["selector_group_id", "trip_id"]]
        if not trip_rows.empty:
            log.info("interruption load: EntitySelector -[:TARGETS_TRIP] (%d)", len(trip_rows))
            neo4j.execute_write(
                _extract_statement(rel_cypher, "EntitySelector -[:TARGETS_TRIP]"),
                parameters={"rows": _df_to_rows(trip_rows)},
            )

        stop_rows = es[es["stop_id"].notna()][["selector_group_id", "stop_id"]]
        if not stop_rows.empty:
            log.info("interruption load: EntitySelector -[:TARGETS_STOP] (%d)", len(stop_rows))
            neo4j.execute_write(
                _extract_statement(rel_cypher, "EntitySelector -[:TARGETS_STOP]"),
                parameters={"rows": _df_to_rows(stop_rows)},
            )

        agency_rows = es[es["agency_id"].notna()][["selector_group_id", "agency_id"]]
        if not agency_rows.empty:
            log.info("interruption load: EntitySelector -[:TARGETS_AGENCY] (%d)", len(agency_rows))
            neo4j.execute_write(
                _extract_statement(rel_cypher, "EntitySelector -[:TARGETS_AGENCY]"),
                parameters={"rows": _df_to_rows(agency_rows)},
            )


# ── Phase 6 + 7: Tier 2/3 relationships ─────────────────────────────────────


def _load_tier2_rels(neo4j: Neo4jManager, result: InterruptionTransformResult) -> None:
    rel_cypher = _load_query("relationships.cypher")

    # SOURCED_FROM — split by source type
    if not result.interruption_sources.empty:
        tu_sources = result.interruption_sources[
            result.interruption_sources["source_type"] == "TripUpdate"
        ]
        if not tu_sources.empty:
            log.info("interruption load: Interruption -[:SOURCED_FROM]-> TripUpdate (%d)", len(tu_sources))
            neo4j.execute_write(
                _extract_statement(rel_cypher, "Interruption -[:SOURCED_FROM]-> TripUpdate"),
                parameters={"rows": _df_to_rows(tu_sources[["interruption_id", "source_entity_id"]])},
            )

        sa_sources = result.interruption_sources[
            result.interruption_sources["source_type"] == "ServiceAlert"
        ]
        if not sa_sources.empty:
            log.info("interruption load: Interruption -[:SOURCED_FROM]-> ServiceAlert (%d)", len(sa_sources))
            neo4j.execute_write(
                _extract_statement(rel_cypher, "Interruption -[:SOURCED_FROM]-> ServiceAlert"),
                parameters={"rows": _df_to_rows(sa_sources[["interruption_id", "source_entity_id"]])},
            )


def _load_tier3_rels(neo4j: Neo4jManager, result: InterruptionTransformResult) -> None:
    rel_cypher = _load_query("relationships.cypher")

    # AFFECTS_TRIP
    if not result.affects_trip.empty:
        log.info("interruption load: Interruption -[:AFFECTS_TRIP] (%d)", len(result.affects_trip))
        neo4j.batch_write(
            _extract_statement(rel_cypher, "Interruption -[:AFFECTS_TRIP]"),
            _df_to_rows(result.affects_trip),
            batch_size=BATCH_SIZE, label="AFFECTS_TRIP",
        )

    # AFFECTS_ROUTE
    if not result.affects_route.empty:
        log.info("interruption load: Interruption -[:AFFECTS_ROUTE] (%d)", len(result.affects_route))
        neo4j.execute_write(
            _extract_statement(rel_cypher, "Interruption -[:AFFECTS_ROUTE]"),
            parameters={"rows": _df_to_rows(result.affects_route)},
        )

    # AFFECTS_STOP
    if not result.affects_stop.empty:
        log.info("interruption load: Interruption -[:AFFECTS_STOP] (%d)", len(result.affects_stop))
        neo4j.execute_write(
            _extract_statement(rel_cypher, "Interruption -[:AFFECTS_STOP]"),
            parameters={"rows": _df_to_rows(result.affects_stop)},
        )

    # ON_DATE
    int_dates = result.interruptions[result.interruptions["date"].notna()][
        ["interruption_id", "date"]
    ]
    if not int_dates.empty:
        log.info("interruption load: Interruption -[:ON_DATE] (%d)", len(int_dates))
        neo4j.execute_write(
            _extract_statement(rel_cypher, "Interruption -[:ON_DATE]-> Date"),
            parameters={"rows": _df_to_rows(int_dates)},
        )


# ── Phase 8: Post-load enrichment ───────────────────────────────────────────


def _run_enrichment(neo4j: Neo4jManager) -> None:
    """
    Rule 6: Link Interruptions to ServicePattern:Maintenance via shared Date.
    """
    rel_cypher = _load_query("relationships.cypher")
    enrichment = _extract_statement(rel_cypher, "Rule 6: DURING_PLANNED_SERVICE")
    log.info("interruption load: running Rule 6 enrichment (DURING_PLANNED_SERVICE)")
    neo4j.execute_write(enrichment)


# ── Main entry point ─────────────────────────────────────────────────────────


def run(
    result: InterruptionTransformResult,
    neo4j: Neo4jManager,
    gtfs_data: dict[str, pd.DataFrame],
) -> None:
    """Load all interruption layer nodes and relationships into Neo4j."""
    log.info("interruption load: starting")

    # Phase 1 — Constraints
    log.info("interruption load: applying constraints")
    cypher = _load_query("constraints.cypher")
    for stmt in _split_statements(cypher):
        neo4j.execute_write(stmt)

    # Phase 2 — FeedInfo
    feed_info_df = result.feed_info if result.feed_info is not None else gtfs_data.get("feed_info")
    feed_version = "unknown"
    if feed_info_df is not None and not feed_info_df.empty:
        feed_version = ensure_feed_info(neo4j, feed_info_df)

    # Phase 3 — Tier 1 nodes
    _load_tier1_nodes(neo4j, result)

    # Phase 4 — Tier 2 nodes
    _load_interruption_nodes(neo4j, result)

    # Phase 5 — Tier 1 relationships
    _load_tier1_rels(neo4j, result, feed_version)

    # Phase 6 + 7 — Tier 2/3 relationships
    _load_tier2_rels(neo4j, result)
    _load_tier3_rels(neo4j, result)

    # Phase 8 — Post-load enrichment
    _run_enrichment(neo4j)

    log.info("interruption load: complete — stats: %s", result.stats)
