# src/layers/fare/load.py
"""
Fare layer — Load

Writes all fare nodes and relationships to Neo4j in dependency order,
then runs post-load validation.

Load order (respects foreign key dependencies):
  1. Constraints        (idempotent — safe to re-run)
  2. FareZone           (no dependencies)
  3. FareMedia          (no dependencies)
  4. FareProduct        (no dependencies)
  5. FareLegRule        (no dependencies on fare nodes)
  6. FareTransferRule   (no dependencies on fare nodes)
  7. Station -[:IN_ZONE]-> FareZone      (requires physical layer: Station)
  8. FareGate -[:IN_ZONE]-> FareZone     (requires physical layer: FareGate)
  9. FareGate -[:BELONGS_TO]-> Station   (requires physical layer: FareGate, Station)
  10. FareMedia -[:ACCEPTS]-> FareProduct
  11. FareProduct -[:ACCEPTED_VIA]-> FareMedia
  12. FareLegRule -[:FROM_AREA]-> FareZone
  13. FareLegRule -[:TO_AREA]-> FareZone
  14. FareLegRule -[:APPLIES_PRODUCT]-> FareProduct
  15. FareTransferRule -[:FROM_LEG]-> FareLegRule
  16. FareTransferRule -[:TO_LEG]-> FareLegRule
  17. FareTransferRule -[:APPLIES_PRODUCT]-> FareProduct  (nullable rows skipped)

Prerequisite: physical layer must have committed Station and FareGate nodes
before the fare layer runs. Coordinate via pipeline.py --layers ordering.
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import TYPE_CHECKING

import pandas as pd

from src.common.cross_layer import check_target_nodes
from src.common.feed_info import ensure_feed_info
from src.common.logger import get_logger
from src.common.neo4j_tools import df_to_rows
from src.common.validators.fare_zones import validate_post_load

if TYPE_CHECKING:
    from src.common.neo4j_tools import Neo4jManager
    from src.layers.fare.transform import FareTransformResult

log = get_logger(__name__)

# Resolve Cypher query files relative to repo root
_QUERY_DIR = Path(__file__).parents[3] / "queries" / "fare"


def _load_query(filename: str) -> str:
    """Load a named Cypher file from queries/fare/."""
    path = _QUERY_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Cypher file not found: {path}")
    return path.read_text(encoding="utf-8")


def _split_statements(cypher: str) -> list[str]:
    """
    Split a multi-statement Cypher file on semicolons,
    stripping comments and blank statements.
    """
    statements = []
    for raw in cypher.split(";"):
        # Strip comment lines
        lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("//")]
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)
    return statements


def _run_statements(
    neo4j: Neo4jManager, cypher: str, rows: list[dict] | None = None
) -> None:
    """Execute all semicolon-delimited statements in a Cypher string."""
    for stmt in _split_statements(cypher):
        if rows is not None:
            neo4j.execute_write(stmt, parameters={"rows": rows})
        else:
            neo4j.execute_write(stmt)




# ── Node loaders ──────────────────────────────────────────────────────────────


def _load_constraints(neo4j: Neo4jManager) -> None:
    log.info("fare load: applying constraints")
    cypher = _load_query("constraints.cypher")
    for stmt in _split_statements(cypher):
        neo4j.execute_write(stmt)


def _load_fare_zones(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    log.info("fare load: FareZone (%d nodes)", len(result.fare_zones))
    cypher = _extract_statement(_load_query("nodes.cypher"), "FareZone")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(result.fare_zones)})


def _load_fare_media(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    log.info("fare load: FareMedia (%d nodes)", len(result.fare_media))
    cypher = _extract_statement(_load_query("nodes.cypher"), "FareMedia")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(result.fare_media)})


def _load_fare_products(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    log.info("fare load: FareProduct (%d nodes)", len(result.fare_products))
    cypher = _extract_statement(_load_query("nodes.cypher"), "FareProduct")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(result.fare_products)})


def _load_fare_leg_rules(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    log.info("fare load: FareLegRule (%d nodes)", len(result.fare_leg_rules))
    cypher = _extract_statement(_load_query("nodes.cypher"), "FareLegRule")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(result.fare_leg_rules)})


def _load_fare_transfer_rules(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    if result.fare_transfer_rules.empty:
        log.warning("fare load: FareTransferRule — no data, skipping")
        return
    # Synthesise rule_id as primary key.
    # Assumes (from_leg_group_id, to_leg_group_id) is unique — holds for
    # current feed (15 rules, 15 unique pairs). Would collide if WMATA adds
    # multiple transfer rules for the same leg pair.
    # See CONVENTIONS.md → "FareTransferRule Synthetic Key"
    df = result.fare_transfer_rules.copy()
    df["rule_id"] = df["from_leg_group_id"] + "__" + df["to_leg_group_id"]
    log.info("fare load: FareTransferRule (%d nodes)", len(df))
    cypher = _extract_statement(_load_query("nodes.cypher"), "FareTransferRule")
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(df)})


# ── Relationship loaders ───────────────────────────────────────────────────────


def _load_station_in_zone(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    log.info(
        "fare load: Station -[:IN_ZONE]-> FareZone (%d rels)", len(result.station_zones)
    )
    cypher = _extract_statement(
        _load_query("relationships.cypher"), "Station -[:IN_ZONE]"
    )
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(result.station_zones)})


def _load_gate_in_zone(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    log.info(
        "fare load: FareGate -[:IN_ZONE]-> FareZone (%d rels)", len(result.gate_zones)
    )
    cypher = _extract_statement(
        _load_query("relationships.cypher"), "FareGate -[:IN_ZONE]"
    )
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(result.gate_zones)})


def _load_gate_belongs_to(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    log.info(
        "fare load: FareGate -[:BELONGS_TO]-> Station (%d rels)", len(result.gate_zones)
    )
    cypher = _extract_statement(
        _load_query("relationships.cypher"), "FareGate -[:BELONGS_TO]"
    )
    neo4j.execute_write(cypher, parameters={"rows": df_to_rows(result.gate_zones)})


def _load_media_product_rels(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    log.info("fare load: FareMedia/FareProduct bidirectional relationships")
    rel_cypher = _load_query("relationships.cypher")

    accepts_cypher = _extract_statement(rel_cypher, "FareMedia -[:ACCEPTS]")
    accepted_cypher = _extract_statement(rel_cypher, "FareProduct -[:ACCEPTED_VIA]")

    # product_media_map has {fare_product_id, fare_media_id} rows
    if result.product_media_map.empty:
        log.warning("fare load: product_media_map is empty — skipping media rels")
        return

    rows = df_to_rows(result.product_media_map)
    log.info("fare load: FareMedia -[:ACCEPTS]-> FareProduct (%d rels)", len(rows))
    neo4j.execute_write(accepts_cypher, parameters={"rows": rows})
    log.info("fare load: FareProduct -[:ACCEPTED_VIA]-> FareMedia (%d rels)", len(rows))
    neo4j.execute_write(accepted_cypher, parameters={"rows": rows})


def _load_leg_rule_rels(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    rel_cypher = _load_query("relationships.cypher")

    log.info(
        "fare load: FareLegRule -[:FROM_AREA]-> FareZone (%d rels)",
        len(result.leg_rule_from_area),
    )
    from_cypher = _extract_statement(rel_cypher, "FareLegRule -[:FROM_AREA]")
    neo4j.execute_write(
        from_cypher, parameters={"rows": df_to_rows(result.leg_rule_from_area)}
    )

    log.info(
        "fare load: FareLegRule -[:TO_AREA]-> FareZone (%d rels)",
        len(result.leg_rule_to_area),
    )
    to_cypher = _extract_statement(rel_cypher, "FareLegRule -[:TO_AREA]")
    neo4j.execute_write(
        to_cypher, parameters={"rows": df_to_rows(result.leg_rule_to_area)}
    )

    log.info(
        "fare load: FareLegRule -[:APPLIES_PRODUCT] (%d rels)",
        len(result.leg_rule_applies_product),
    )
    applies_cypher = _extract_statement(rel_cypher, "FareLegRule -[:APPLIES_PRODUCT]")
    neo4j.execute_write(
        applies_cypher,
        parameters={"rows": df_to_rows(result.leg_rule_applies_product)},
    )


def _load_transfer_rule_rels(neo4j: Neo4jManager, result: FareTransformResult) -> None:
    if result.fare_transfer_rules.empty:
        log.warning("fare load: FareTransferRule relationships — no data, skipping")
        return

    rel_cypher = _load_query("relationships.cypher")
    df = result.fare_transfer_rules.copy()
    df["rule_id"] = df["from_leg_group_id"] + "__" + df["to_leg_group_id"]

    from_leg = _extract_statement(rel_cypher, "FareTransferRule -[:FROM_LEG]")
    to_leg = _extract_statement(rel_cypher, "FareTransferRule -[:TO_LEG]")
    applies = _extract_statement(rel_cypher, "FareTransferRule -[:APPLIES_PRODUCT]")

    neo4j.execute_write(
        from_leg, parameters={"rows": df_to_rows(df[["rule_id", "from_leg_group_id"]])}
    )
    neo4j.execute_write(
        to_leg, parameters={"rows": df_to_rows(df[["rule_id", "to_leg_group_id"]])}
    )

    # APPLIES_PRODUCT only for non-free transfer rows
    if "fare_product_id" not in df.columns:
        return
    product_rows = df[df["fare_product_id"].notna()][["rule_id", "fare_product_id"]]
    if not product_rows.empty:
        neo4j.execute_write(applies, parameters={"rows": df_to_rows(product_rows)})


# ── Statement extractor ───────────────────────────────────────────────────────


def _extract_statement(cypher: str, label_hint: str) -> str:
    """
    Extract a single UNWIND statement from a multi-statement Cypher file
    by matching the comment line containing label_hint.

    Splits on the decorative separator pattern '// ── ' which precedes
    each statement. This keeps multi-line comments (like // $rows: ...)
    together with their UNWIND block.
    """
    blocks = re.split(r"\n(?=// ── )", cypher)
    for block in blocks:
        if label_hint in block:
            lines = [ln for ln in block.splitlines() if not ln.strip().startswith("//")]
            stmt = "\n".join(lines).strip().rstrip(";")
            if stmt:
                return stmt
    raise ValueError(
        f"Could not find Cypher statement with hint '{label_hint}' in relationships.cypher"
    )


# ── Main entry point ──────────────────────────────────────────────────────────


def run(result: FareTransformResult, neo4j: Neo4jManager) -> None:
    """
    Load all fare layer nodes and relationships into Neo4j.
    Runs post-load validation; raises ValueError on failure.
    """
    log.info("fare load: starting")

    # Shared FeedInfo node (idempotent — safe if already created by another layer)
    ensure_feed_info(neo4j, result.feed_info)

    # Nodes
    _load_constraints(neo4j)
    _load_fare_zones(neo4j, result)
    _load_fare_media(neo4j, result)
    _load_fare_products(neo4j, result)
    _load_fare_leg_rules(neo4j, result)
    _load_fare_transfer_rules(neo4j, result)

    # Relationships — physical layer nodes must exist
    has_stations = check_target_nodes(neo4j, "Station", "fare → physical")
    has_faregates = check_target_nodes(neo4j, "FareGate", "fare → physical")

    if has_stations:
        _load_station_in_zone(neo4j, result)
    if has_faregates:
        _load_gate_in_zone(neo4j, result)
        _load_gate_belongs_to(neo4j, result)

    # Internal relationships (fare layer nodes only)
    _load_media_product_rels(neo4j, result)
    _load_leg_rule_rels(neo4j, result)
    _load_transfer_rule_rels(neo4j, result)

    # ── Post-load validation ──────────────────────────────────────────────────
    log.info("fare load: running post-load validation")
    validation = validate_post_load(neo4j)
    log.info("fare load: post-load validation result:\n%s", validation.summary())

    if not validation.passed:
        raise ValueError(
            f"Fare layer post-load validation failed:\n{validation.summary()}"
        )

    log.info("fare load: complete — stats: %s", result.stats)
