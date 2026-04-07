# src/layers/physical/load.py
"""
Physical Infrastructure Layer — Load

Writes all physical nodes and relationships to Neo4j in dependency order,
then runs post-load validation (validate_post_load from validators/physical.py).

Load order (respects foreign key dependencies):
  1. Constraints        (idempotent — safe to re-run)
  2. Station            (no dependencies)
  3. StationEntrance    (no dependencies)
  4. Platform           (no dependencies)
  5. FareGate           (no dependencies)
  6. Pathway            (base node — no dependencies)
  7. Level              (no dependencies)
  8. Pathway label migrations  (requires Pathway nodes to exist)
  9. Station  -[:CONTAINS]-> StationEntrance
  10. Station -[:CONTAINS]-> Platform
  11. (stop_entity) -[:LINKS]-> Pathway   (from_stop direction, all 5 entity types)
  12. Pathway -[:LINKS]-> (stop_entity)   (to_stop direction, all 5 entity types)
  13. Pathway -[:LINKS]-> Pathway         (chain via deferred generic node pivot)
  14. (stop_entity) -[:ON_LEVEL]-> Level  (all stop types with level_id)
  15. Pathway -[:ON_LEVEL]-> Level        (elevators + same-level pathways)
  16. Pathway -[:STARTING_LEVEL]-> Level  (escalators + multi-level pathways)
  17. Pathway -[:ENDING_LEVEL]-> Level    (escalators + multi-level pathways)
  18. Station -[:CONTAINS]-> Pathway      (derived: via intermediate + direct LINKS)

Cypher is stored in queries/physical/ and loaded at runtime — no
inline Cypher strings in this module.
"""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from src.common.feed_info import ensure_feed_info
from src.common.logger import get_logger
from src.common.neo4j_tools import Neo4jManager, df_to_rows
from src.common.validators.physical import validate_post_load

log = get_logger(__name__)

# Resolve Cypher query files relative to repo root
_QUERY_DIR = Path(__file__).parents[3] / "queries" / "physical"

# ── Cypher file helpers ────────────────────────────────────────────────────────


def _load_query(filename: str) -> str:
    """Load a named Cypher file from queries/physical/."""
    path = _QUERY_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Cypher file not found: {path}")
    return path.read_text(encoding="utf-8")


def _extract_statement(cypher: str, label_hint: str) -> str:
    """
    Extract a single statement from a multi-statement Cypher file by matching
    the decorative separator comment that precedes it.

    Splits on '// ── ' blocks (same convention as fare layer). Comment lines
    are stripped; the trailing semicolon is removed so the caller can pass
    the statement directly to execute_write.
    """
    blocks = re.split(r"\n(?=// ── )", cypher)
    for block in blocks:
        if label_hint in block:
            lines = [ln for ln in block.splitlines() if not ln.strip().startswith("//")]
            stmt = "\n".join(lines).strip().rstrip(";")
            if stmt:
                return stmt
    raise ValueError(
        f"Could not find Cypher statement with hint '{label_hint}' in query file"
    )




# ── Node loaders ───────────────────────────────────────────────────────────────


def _load_constraints(neo4j: Neo4jManager) -> None:
    log.info("physical load: applying constraints")
    cypher = _load_query("constraints.cypher")
    for raw in cypher.split(";"):
        lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("//")]
        stmt = "\n".join(lines).strip()
        if stmt:
            neo4j.execute_write(stmt)


def _load_stations(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    log.info("physical load: Station (%d nodes)", len(stops))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Station")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(stops)})


def _load_entrances(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    log.info("physical load: StationEntrance (%d nodes)", len(stops))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":StationEntrance")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(stops)})


def _load_platforms(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    log.info("physical load: Platform (%d nodes)", len(stops))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Platform")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(stops)})


def _load_bus_stops(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    log.info("physical load: BusStop (%d nodes)", len(stops))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":BusStop")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(stops)})


def _load_faregates(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    log.info("physical load: FareGate (%d nodes)", len(stops))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":FareGate")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(stops)})


def _load_pathways(neo4j: Neo4jManager, pathways: pd.DataFrame) -> None:
    log.info("physical load: Pathway base nodes (%d nodes)", len(pathways))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Pathway")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(pathways)})


def _load_pathway_labels(neo4j: Neo4jManager) -> None:
    """
    Apply mode and zone multi-labels to :Pathway nodes.

    These are migration queries — they SET labels on nodes that already exist.
    They carry no $rows parameter; each runs as a plain MATCH/SET statement.
    Label hints must match exactly the '// ── Pathway :X label' sections in
    nodes.cypher.
    """
    nodes_cypher = _load_query("nodes.cypher")
    label_hints = [
        "Pathway :Elevator label",
        "Pathway :Escalator label",
        "Pathway :Stairs label",
        "Pathway :Walkway label",
        "Pathway :Paid label",
        "Pathway :Unpaid label",
    ]
    for hint in label_hints:
        stmt = _extract_statement(nodes_cypher, hint)
        log.info("physical load: applying %s", hint)
        neo4j.execute_write(stmt)


def _load_levels(neo4j: Neo4jManager, levels: pd.DataFrame) -> None:
    log.info("physical load: Level (%d nodes)", len(levels))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Level")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(levels)})


# ── Relationship loaders ───────────────────────────────────────────────────────


def _load_station_contains_entrance(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    log.info(
        "physical load: Station -[:CONTAINS]-> StationEntrance (%d rels)",
        len(stops),
    )
    cypher = _extract_statement(
        _load_query("relationships.cypher"), "Station -[:CONTAINS]-> StationEntrance"
    )
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(stops)})


def _load_station_contains_platform(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    log.info("physical load: Station -[:CONTAINS]-> Platform (%d rels)", len(stops))
    cypher = _extract_statement(
        _load_query("relationships.cypher"), "Station -[:CONTAINS]-> Platform"
    )
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(stops)})


def _load_station_contains_faregate(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    log.info("physical load: Station -[:CONTAINS]-> FareGate (%d rels)", len(stops))
    cypher = _extract_statement(
        _load_query("relationships.cypher"), "Station -[:CONTAINS]-> FareGate"
    )
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(stops)})


def _load_from_links(neo4j: Neo4jManager, from_links: dict[str, pd.DataFrame]) -> None:
    """Wire (stop_entity)-[:LINKS]->(Pathway) for all from_stop partitions."""
    rel_cypher = _load_query("relationships.cypher")
    dispatch = [
        ("StationEntrance -[:LINKS]-> Pathway (from_stop)", from_links["ENTRANCE"]),
        ("Platform -[:LINKS]-> Pathway (from_stop)", from_links["PLATFORM"]),
        ("Station -[:LINKS]-> Pathway (from_stop)", from_links["STATION"]),
        ("FareGate -[:LINKS]-> Pathway (from_stop)", from_links["FAREGATE"]),
        ("BusStop -[:LINKS]-> Pathway (from_stop)", from_links["BUS_STOP"]),
    ]
    for hint, partition in dispatch:
        if partition.empty:
            log.warning("physical load: %s — 0 rows, skipping", hint)
            continue
        log.info("physical load: %s (%d rels)", hint, len(partition))
        stmt = _extract_statement(rel_cypher, hint)
        neo4j.execute_write(
            stmt, parameters={"rows": df_to_rows(partition[["pathway_id", "stop_id"]])}
        )


def _load_to_links(neo4j: Neo4jManager, to_links: dict[str, pd.DataFrame]) -> None:
    """Wire (Pathway)-[:LINKS]->(stop_entity) for all to_stop partitions."""
    rel_cypher = _load_query("relationships.cypher")
    dispatch = [
        ("Pathway -[:LINKS]-> StationEntrance (to_stop)", to_links["ENTRANCE"]),
        ("Pathway -[:LINKS]-> Platform (to_stop)", to_links["PLATFORM"]),
        ("Pathway -[:LINKS]-> Station (to_stop)", to_links["STATION"]),
        ("Pathway -[:LINKS]-> FareGate (to_stop)", to_links["FAREGATE"]),
        ("Pathway -[:LINKS]-> BusStop (to_stop)", to_links["BUS_STOP"]),
    ]
    for hint, partition in dispatch:
        if partition.empty:
            log.warning("physical load: %s — 0 rows, skipping", hint)
            continue
        log.info("physical load: %s (%d rels)", hint, len(partition))
        stmt = _extract_statement(rel_cypher, hint)
        neo4j.execute_write(
            stmt, parameters={"rows": df_to_rows(partition[["pathway_id", "stop_id"]])}
        )


def _load_pathway_chain_links(neo4j: Neo4jManager, chains: pd.DataFrame) -> None:
    """Wire (Pathway)-[:LINKS]->(Pathway) through deferred generic node pivots."""
    if chains.empty:
        log.info("physical load: Pathway -[:LINKS]-> Pathway (chain) — 0 rows, skipping")
        return
    log.info("physical load: Pathway -[:LINKS]-> Pathway (chain) (%d rels)", len(chains))
    stmt = _extract_statement(
        _load_query("relationships.cypher"), "Pathway -[:LINKS]-> Pathway (chain)"
    )
    neo4j.execute_write(
        stmt,
        parameters={"rows": df_to_rows(chains[["from_pathway_id", "to_pathway_id"]])},
    )


def _load_stop_on_level(neo4j: Neo4jManager, stop_on_level: pd.DataFrame) -> None:
    if stop_on_level.empty:
        log.warning("physical load: (stop_entity) -[:ON_LEVEL]-> Level — 0 rows, skipping")
        return
    log.info(
        "physical load: (stop_entity) -[:ON_LEVEL]-> Level (%d rels)", len(stop_on_level)
    )
    stmt = _extract_statement(_load_query("relationships.cypher"), "(stop_entity) -[:ON_LEVEL]-> Level")
    neo4j.execute_write(stmt, parameters={"rows": df_to_rows(stop_on_level)})


def _load_pathway_on_level(
    neo4j: Neo4jManager,
    on_level: pd.DataFrame,
    starting_level: pd.DataFrame,
    ending_level: pd.DataFrame,
) -> None:
    rel_cypher = _load_query("relationships.cypher")
    for hint, df, rel_type in [
        ("Pathway -[:ON_LEVEL]-> Level", on_level, "ON_LEVEL"),
        ("Pathway -[:STARTING_LEVEL]-> Level", starting_level, "STARTING_LEVEL"),
        ("Pathway -[:ENDING_LEVEL]-> Level", ending_level, "ENDING_LEVEL"),
    ]:
        if df.empty:
            log.info("physical load: Pathway -[:%s]-> Level — 0 rows, skipping", rel_type)
            continue
        log.info("physical load: Pathway -[:%s]-> Level (%d rels)", rel_type, len(df))
        stmt = _extract_statement(rel_cypher, hint)
        neo4j.execute_write(stmt, parameters={"rows": df_to_rows(df)})


def _derive_station_contains_pathway(neo4j: Neo4jManager) -> None:
    """
    Derive Station -[:CONTAINS]-> Pathway shortcuts after all LINKS and
    CONTAINS edges are committed.

    Two passes — both are idempotent via MERGE:
      Path 1: Station → CONTAINS → intermediate → LINKS ↔ Pathway
      Path 2: Station ↔ LINKS ↔ Pathway  (DEFERRED-node pivot pathways)

    Cross-station pathways receive CONTAINS from both stations; this is
    intentional — such a pathway is physically accessible from both.
    """
    rel_cypher = _load_query("relationships.cypher")

    stmt_via_intermediate = _extract_statement(
        rel_cypher, "Station -[:CONTAINS]-> Pathway via intermediate"
    )
    stmt_direct = _extract_statement(
        rel_cypher, "Station -[:CONTAINS]-> Pathway direct"
    )

    log.info("physical load: deriving Station -[:CONTAINS]-> Pathway (via intermediate)")
    neo4j.execute_write(stmt_via_intermediate)

    log.info("physical load: deriving Station -[:CONTAINS]-> Pathway (direct LINKS)")
    neo4j.execute_write(stmt_direct)


# ── Main entry point ───────────────────────────────────────────────────────────


def run(result: dict[str, pd.DataFrame], neo4j: Neo4jManager) -> None:
    """
    Load all physical layer nodes and relationships into Neo4j.

    result must be the dict returned by physical/transform.run() — keys used:
      stops, pathways, levels, feed_info
    """
    log.info("physical load: starting")

    stations = result["stations"]
    entrances = result["entrances"]
    platforms = result["platforms"]
    bus_stops = result["bus_stops"]
    pathways = result["pathways"]
    levels = result["levels"]
    faregates = result["faregates"]
    station_contains_platform = result["station_contains_platform"]
    station_contains_entrance = result["station_contains_entrance"]
    station_contains_faregate = result["station_contains_faregate"]
    from_links = result["from_links"]
    to_links = result["to_links"]
    pathway_chain_links = result["pathway_chain_links"]
    stop_on_level = result["stop_on_level"]
    pathway_on_level = result["pathway_on_level"]
    pathway_starting_level = result["pathway_starting_level"]
    pathway_ending_level = result["pathway_ending_level"]

    # Shared FeedInfo node (idempotent — safe if already created by another layer)
    ensure_feed_info(neo4j, result["feed_info"])

    # ── Constraints ──────────────────────────────────────────────────────────
    _load_constraints(neo4j)

    # ── Nodes ─────────────────────────────────────────────────────────────────
    _load_stations(neo4j, stations)
    _load_entrances(neo4j, entrances)
    _load_platforms(neo4j, platforms)
    _load_bus_stops(neo4j, bus_stops)
    _load_faregates(neo4j, faregates)
    _load_pathways(neo4j, pathways)
    _load_levels(neo4j, levels)

    # Multi-label migrations — must run after all :Pathway nodes are committed
    _load_pathway_labels(neo4j)

    # ── Relationships ─────────────────────────────────────────────────────────
    _load_station_contains_entrance(neo4j, station_contains_entrance)
    _load_station_contains_platform(neo4j, station_contains_platform)
    _load_station_contains_faregate(neo4j, station_contains_faregate)
    _load_from_links(neo4j, from_links)
    _load_to_links(neo4j, to_links)
    _load_pathway_chain_links(neo4j, pathway_chain_links)

    # ── Level relationships ───────────────────────────────────────────────────
    # Must run after all stop nodes and Pathway nodes are committed.
    # Level nodes are written in _load_levels() above, so all MATCH targets exist.
    _load_stop_on_level(neo4j, stop_on_level)
    _load_pathway_on_level(neo4j, pathway_on_level, pathway_starting_level, pathway_ending_level)

    # Derived shortcuts — must run after all CONTAINS and LINKS edges exist
    _derive_station_contains_pathway(neo4j)

    # ── Post-load validation ───────────────────────────────────────────────────
    log.info("physical load: running post-load validation")
    validation = validate_post_load(neo4j)
    log.info("physical load: post-load validation result:\n%s", validation.summary())
    if not validation.passed:
        raise ValueError(
            f"Physical layer post-load validation failed — aborting pipeline:\n"
            f"{validation.summary()}"
        )

    log.info("physical load: complete")
