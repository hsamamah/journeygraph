# src/layers/accessibility/load.py
"""
Accessibility layer — Load

Writes OutageEvent nodes and relationships to Neo4j:

  Phase 1 — Constraints and indexes (constraints.cypher)
  Phase 2 — OutageEvent nodes (nodes.cypher ON CREATE / ON MATCH)
  Phase 3 — AFFECTS relationships to Pathway nodes (relationships.cypher)
             Skipped until pathway_joiner is implemented (see §5 of schema doc).
             Currently a no-op placeholder — no OUTAGE_AT→Station fallback.
  Phase 4 — Stale resolution: mark active nodes not seen this poll as resolved,
             setting resolved_at and computing actual_duration_days.

Phase 4 detects outages that were active in a previous poll but are no longer
present in the current API response (last_seen_poll < poll_timestamp).

Prerequisites:
  Physical layer must be loaded (Pathway nodes) for Phase 3 AFFECTS links.
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import TYPE_CHECKING

from src.common.cross_layer import check_target_nodes
from src.common.logger import get_logger
from src.common.neo4j_tools import df_to_rows
from src.common.validators.accessibility import validate_post_load, validate_pre_load
from src.layers.accessibility import pathway_joiner

if TYPE_CHECKING:
    from src.common.neo4j_tools import Neo4jManager
    from src.layers.accessibility.transform import AccessibilityTransformResult

log = get_logger(__name__)

_QUERY_DIR = Path(__file__).parents[3] / "queries" / "accessibility"

BATCH_SIZE = 5_000

_MS_PER_DAY = 86_400_000


# ── Cypher helpers ────────────────────────────────────────────────────────────


def _load_query(filename: str) -> str:
    path = _QUERY_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Cypher file not found: {path}")
    return path.read_text(encoding="utf-8")


def _split_statements(cypher: str) -> list[str]:
    statements = []
    for raw in cypher.split(";"):
        lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("//")]
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)
    return statements


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
        f"in accessibility queries"
    )


# ── Phase 2: OutageEvent nodes ────────────────────────────────────────────────


def _load_outage_nodes(
    neo4j: Neo4jManager,
    result: AccessibilityTransformResult,
) -> None:
    if result.outages.empty:
        log.warning("accessibility load: no OutageEvent nodes to create")
        return

    log.info("accessibility load: OutageEvent (%d nodes)", len(result.outages))
    nodes_cypher = _load_query("nodes.cypher")
    stmt = _extract_statement(nodes_cypher, ":OutageEvent")
    neo4j.batch_write(
        stmt,
        df_to_rows(result.outages),
        batch_size=BATCH_SIZE,
        label="OutageEvent",
    )


# ── Phase 3: AFFECTS → Pathway relationships ─────────────────────────────────


def _load_affects_rels(
    neo4j: Neo4jManager,
    result: AccessibilityTransformResult,
) -> None:
    """
    Create OutageEvent -[:AFFECTS]-> Pathway relationships via pathway_joiner.

    Unmatched outages (no Pathway found) are logged as warnings — they still
    have OutageEvent nodes in the graph, just without the AFFECTS link.
    Match rate is logged so the static lookup table build-out can be tracked.
    """
    if result.outages.empty:
        return

    check_target_nodes(neo4j, "Pathway", "accessibility → physical")
    matched = pathway_joiner.resolve(result.outages, neo4j)
    if matched.empty:
        log.warning("accessibility load: no AFFECTS relationships to create")
        return

    log.info(
        "accessibility load: OutageEvent -[:AFFECTS]-> Pathway (%d)", len(matched)
    )
    rel_cypher = _load_query("relationships.cypher")
    stmt = _extract_statement(rel_cypher, "OutageEvent -[:AFFECTS]-> Pathway")
    neo4j.batch_write(
        stmt,
        df_to_rows(matched),
        batch_size=BATCH_SIZE,
        label="AFFECTS",
    )


# ── Phase 4: Stale resolution ─────────────────────────────────────────────────


def _resolve_stale_outages(neo4j: Neo4jManager, poll_timestamp: str) -> None:
    """
    Resolve OutageEvent nodes that are active but absent from this poll.

    A node is stale when last_seen_poll < poll_timestamp — it was not touched
    by the Phase 2 MERGE, meaning the unit no longer appears in the API response.

    Sets:
      status             = 'resolved'
      resolved_at        = poll_timestamp  (approximation — actual resolution
                           occurred between last_seen_poll and resolved_at)
      actual_duration_days = floor((resolved_at_epoch - date_out_of_service_epoch) / ms_per_day)
    """
    rows = neo4j.query(
        """
        MATCH (o:OutageEvent {status: 'active'})
        WHERE datetime(o.last_seen_poll) < datetime($poll_timestamp)
        SET o.status               = 'resolved',
            o.resolved_at          = $poll_timestamp,
            o.actual_duration_days = CASE
              WHEN o.date_out_of_service IS NOT NULL
              THEN toInteger(
                (datetime($poll_timestamp).epochMillis - o.date_out_of_service)
                / $ms_per_day
              )
              ELSE null
            END
        RETURN count(o) AS n
        """,
        {"poll_timestamp": poll_timestamp, "ms_per_day": _MS_PER_DAY},
    )
    n = rows[0]["n"] if rows else 0
    if n > 0:
        log.info(
            "accessibility load: resolved %d stale OutageEvent(s) "
            "(last_seen_poll < current poll)",
            n,
        )
    else:
        log.info("accessibility load: no stale outages to resolve")


# ── Main entry point ──────────────────────────────────────────────────────────


def run(
    result: AccessibilityTransformResult,
    neo4j: Neo4jManager,
) -> None:
    """Load all accessibility layer nodes and relationships into Neo4j."""
    log.info("accessibility load: starting")

    # Phase 1 — Constraints and indexes
    log.info("accessibility load: applying constraints")
    for stmt in _split_statements(_load_query("constraints.cypher")):
        neo4j.execute_write(stmt)

    # Pre-load validation — check transformed DataFrames before any writes
    pre_vr = validate_pre_load(result)
    log.info("accessibility load: pre-load validation\n%s", pre_vr.summary())

    # Phase 2 — OutageEvent nodes
    _load_outage_nodes(neo4j, result)

    # Phase 3 — AFFECTS → Pathway (deferred until pathway_joiner is implemented)
    _load_affects_rels(neo4j, result)

    # Phase 4 — Stale resolution
    _resolve_stale_outages(neo4j, result.poll_timestamp)

    # Post-load validation — check graph integrity after all phases
    post_vr = validate_post_load(neo4j)
    log.info("accessibility load: post-load validation\n%s", post_vr.summary())

    log.info("accessibility load: complete — stats: %s", result.stats)
