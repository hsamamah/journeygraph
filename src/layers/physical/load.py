# src/layers/physical/load.py
"""
Physical Infrastructure Layer — Load

Writes all physical nodes and relationships to Neo4j in dependency order,
then runs post-load validation.

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
  11. Pathway -[:LINKS]->    StationEntrance
  12. Pathway -[:LINKS]->    Platform
  13. Pathway -[:LINKS]->    Station
  14. Pathway -[:LINKS]->    FareGate

Cypher is stored in queries/physical/ and loaded at runtime — no
inline Cypher strings in this module.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from src.common.feed_info import ensure_feed_info
from src.common.logger import get_logger
from src.common.neo4j_tools import Neo4jManager

log = get_logger(__name__)

# Resolve Cypher query files relative to repo root
_QUERY_DIR = Path(__file__).parents[3] / "queries" / "physical"

# GTFS location_type values
_LOC_PLATFORM  = {0, 4}
_LOC_STATION   = 1
_LOC_ENTRANCE  = 2
_LOC_FAREGATE  = 3


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


def _df_to_rows(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of dicts, replacing NaN/NaT with None."""
    return [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]


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
    stations = stops[stops["location_type"] == _LOC_STATION].copy()
    log.info("physical load: Station (%d nodes)", len(stations))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Station")
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(stations)})


def _load_entrances(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    entrances = stops[stops["location_type"] == _LOC_ENTRANCE].copy()
    log.info("physical load: StationEntrance (%d nodes)", len(entrances))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":StationEntrance")
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(entrances)})


def _load_platforms(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    platforms = stops[stops["location_type"].isin(_LOC_PLATFORM)].copy()
    log.info("physical load: Platform (%d nodes)", len(platforms))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Platform")
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(platforms)})


def _load_faregates(neo4j: Neo4jManager, stops: pd.DataFrame) -> None:
    faregates = stops[stops["id"].str.contains("_FG_", na=False)].copy()
    log.info("physical load: FareGate (%d nodes)", len(faregates))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":FareGate")
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(faregates)})


def _load_pathways(neo4j: Neo4jManager, pathways: pd.DataFrame) -> None:
    log.info("physical load: Pathway base nodes (%d nodes)", len(pathways))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Pathway")
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(pathways)})


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
        "Pathway :PaidZone label",
        "Pathway :UnpaidZone label",
    ]
    for hint in label_hints:
        stmt = _extract_statement(nodes_cypher, hint)
        log.info("physical load: applying %s", hint)
        neo4j.execute_write(stmt)


def _load_levels(neo4j: Neo4jManager, levels: pd.DataFrame) -> None:
    log.info("physical load: Level (%d nodes)", len(levels))
    cypher = _extract_statement(_load_query("nodes.cypher"), ":Level")
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(levels)})


# ── Relationship loaders ───────────────────────────────────────────────────────


def _load_station_contains_entrance(
    neo4j: Neo4jManager, stops: pd.DataFrame
) -> None:
    entrances = stops[stops["location_type"] == _LOC_ENTRANCE][
        ["parent_station", "id"]
    ].rename(columns={"parent_station": "station_id", "id": "entrance_id"})
    log.info(
        "physical load: Station -[:CONTAINS]-> StationEntrance (%d rels)", len(entrances)
    )
    cypher = _extract_statement(
        _load_query("relationships.cypher"), "Station -[:CONTAINS]-> StationEntrance"
    )
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(entrances)})


def _load_station_contains_platform(
    neo4j: Neo4jManager, stops: pd.DataFrame
) -> None:
    platforms = stops[stops["location_type"].isin(_LOC_PLATFORM)][
        ["parent_station", "id"]
    ].rename(columns={"parent_station": "station_id", "id": "platform_id"})
    log.info(
        "physical load: Station -[:CONTAINS]-> Platform (%d rels)", len(platforms)
    )
    cypher = _extract_statement(
        _load_query("relationships.cypher"), "Station -[:CONTAINS]-> Platform"
    )
    neo4j.execute_write(cypher, parameters={"rows": _df_to_rows(platforms)})


def _load_pathway_links(
    neo4j: Neo4jManager, pathways: pd.DataFrame, stops: pd.DataFrame
) -> None:
    """
    Wire Pathway -[:LINKS]-> (StationEntrance | Platform | Station | FareGate).

    Replaces the previous row-by-row loop with a single vectorized partition
    pass. Each pathway contributes up to two rows (from_stop_id + to_stop_id).
    We join against stops to resolve location_type, then dispatch each
    partition to its dedicated batch UNWIND statement.

    Complexity: O(n) pandas ops instead of O(n) × individual Neo4j writes.
    """
    rel_cypher = _load_query("relationships.cypher")
    stop_types = stops[["id", "location_type"]].copy()

    # Build a single (pathway_id, stop_id) frame covering both endpoints
    from_side = pathways[["id", "from_stop_id"]].rename(
        columns={"id": "pathway_id", "from_stop_id": "stop_id"}
    )
    to_side = pathways[["id", "to_stop_id"]].rename(
        columns={"id": "pathway_id", "to_stop_id": "stop_id"}
    )
    all_links = (
        pd.concat([from_side, to_side], ignore_index=True)
        .dropna(subset=["stop_id"])
        .drop_duplicates()
        .merge(stop_types.rename(columns={"id": "stop_id"}), on="stop_id", how="inner")
    )

    dispatch = [
        ("Pathway -[:LINKS]-> StationEntrance", all_links[all_links["location_type"] == _LOC_ENTRANCE]),
        ("Pathway -[:LINKS]-> Platform",        all_links[all_links["location_type"].isin(_LOC_PLATFORM)]),
        ("Pathway -[:LINKS]-> Station",         all_links[all_links["location_type"] == _LOC_STATION]),
        ("Pathway -[:LINKS]-> FareGate",        all_links[all_links["location_type"] == _LOC_FAREGATE]),
    ]

    for hint, partition in dispatch:
        if partition.empty:
            log.warning("physical load: %s — 0 rows, skipping", hint)
            continue
        log.info("physical load: %s (%d rels)", hint, len(partition))
        stmt = _extract_statement(rel_cypher, hint)
        neo4j.execute_write(
            stmt,
            parameters={"rows": _df_to_rows(partition[["pathway_id", "stop_id"]])},
        )


# ── Main entry point ───────────────────────────────────────────────────────────


def run(result: dict[str, pd.DataFrame], neo4j: Neo4jManager) -> None:
    """
    Load all physical layer nodes and relationships into Neo4j.

    result must be the dict returned by physical/transform.run() — keys used:
      stops, pathways, levels, feed_info
    """
    log.info("physical load: starting")

    stops    = result["stops"]
    pathways = result["pathways"]
    levels   = result["levels"]

    # Shared FeedInfo node (idempotent — safe if already created by another layer)
    ensure_feed_info(neo4j, result["feed_info"])

    # ── Constraints ──────────────────────────────────────────────────────────
    _load_constraints(neo4j)

    # ── Nodes ─────────────────────────────────────────────────────────────────
    _load_stations(neo4j, stops)
    _load_entrances(neo4j, stops)
    _load_platforms(neo4j, stops)
    _load_faregates(neo4j, stops)
    _load_pathways(neo4j, pathways)
    _load_levels(neo4j, levels)

    # Multi-label migrations — must run after all :Pathway nodes are committed
    _load_pathway_labels(neo4j)

    # ── Relationships ─────────────────────────────────────────────────────────
    _load_station_contains_entrance(neo4j, stops)
    _load_station_contains_platform(neo4j, stops)
    _load_pathway_links(neo4j, pathways, stops)

    log.info("physical load: complete")
