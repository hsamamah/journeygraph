"""
subgraph_builder.py — Subgraph path orchestrator.

Wires Stage 1 (AnchorResolver), Stage 2 (HopExpander), and Stage 3
(ContextSerializer) into a single run() call that accepts PlannerOutput
and returns SubgraphOutput.

This is the only entry point into the Subgraph path. All downstream
consumers (Narration Agent) interact with SubgraphOutput only — the
internal stages are not exposed.

Failure handling:
    - Zero anchors resolved → immediate SubgraphOutput(success=False)
      via make_zero_anchor_fallback(). No expansion or serialization runs.
    - Expansion returns empty node set → SubgraphOutput(success=False)
      with failure_reason describing which domain produced no results.
    - Any unhandled exception → SubgraphOutput(success=False) with
      failure_reason carrying the exception message. Never raises.
"""

from datetime import UTC, datetime
import logging

from src.common.neo4j_tools import Neo4jManager
from src.llm.anchor_resolver import AnchorResolver
from src.llm.context_serializer import ContextSerializer
from src.llm.hop_expander import HopExpander
from src.llm.planner_output import PlannerOutput
from src.llm.subgraph_output import SubgraphOutput, make_zero_anchor_fallback

log = logging.getLogger(__name__)


class SubgraphBuilder:
    """
    Orchestrates the three-stage Subgraph path.

    Args:
        db:              Neo4jManager instance. Injected at construction —
                         caller owns the connection lifecycle.
        invocation_time: Pipeline invocation datetime passed to AnchorResolver
                         for relative date resolution. Defaults to
                         datetime.utcnow() if not provided. Should be the
                         same timestamp used across the full pipeline
                         invocation for consistency.
    """

    def __init__(
        self,
        db: Neo4jManager,
        invocation_time: datetime | None = None,
    ):
        self._resolver = AnchorResolver(
            db=db, invocation_time=invocation_time or datetime.now(UTC)
        )
        self._expander = HopExpander(db=db)
        self._serializer = ContextSerializer()

    # ── Public ────────────────────────────────────────────────────────────────

    def run(self, planner_output: PlannerOutput) -> SubgraphOutput:
        """
        Executes the full Subgraph path for a single pipeline invocation.

        Args:
            planner_output: PlannerOutput from the Planner. Consumed read-only.

        Returns:
            SubgraphOutput. Never raises — all failures produce a
            SubgraphOutput with success=False and a populated failure_reason.
        """
        domain = planner_output.domain

        try:
            return self._run(planner_output, domain)
        except Exception as exc:
            log.exception("subgraph_builder | unhandled exception | domain=%s", domain)
            return SubgraphOutput(
                context="",
                node_count=0,
                trimmed=False,
                provenance_nodes=[],
                anchor_resolutions={},
                domain=domain,
                success=False,
                failure_reason=f"Unhandled exception in Subgraph path: {exc}",
            )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _run(self, planner_output: PlannerOutput, domain: str) -> SubgraphOutput:
        # ── Stage 1: Anchor resolution ────────────────────────────────────────
        resolutions = self._resolver.resolve(planner_output.anchors)

        if not resolutions.any_resolved:
            log.warning("subgraph_builder | zero anchors resolved | domain=%s", domain)
            return make_zero_anchor_fallback(domain)

        log.debug(
            "subgraph_builder | anchors resolved | stations=%d routes=%d "
            "dates=%d pathway_nodes=%d | domain=%s",
            len(resolutions.resolved_stations),
            len(resolutions.resolved_routes),
            len(resolutions.resolved_dates),
            len(resolutions.resolved_pathway_nodes),
            domain,
        )

        # ── Stage 2: Hop expansion ────────────────────────────────────────────
        raw = self._expander.expand(resolutions=resolutions, domain=domain)

        if raw.node_count == 0:
            log.warning(
                "subgraph_builder | expansion produced no nodes | domain=%s", domain
            )
            return SubgraphOutput(
                context="",
                node_count=0,
                trimmed=False,
                provenance_nodes=[],
                anchor_resolutions=resolutions.as_flat_dict(),
                domain=domain,
                success=False,
                failure_reason=(
                    f"Hop expansion produced no nodes for domain '{domain}'. "
                    "Anchors resolved but no connected subgraph found."
                ),
            )

        log.debug(
            "subgraph_builder | expansion complete | nodes=%d rels=%d "
            "provenance=%d | domain=%s",
            raw.node_count,
            len(raw.rels),
            len(raw.provenance_nodes),
            domain,
        )

        # ── Stage 3: Serialization and budget enforcement ─────────────────────
        result = self._serializer.serialize_and_enforce(
            raw=raw,
            resolutions=resolutions,
        )

        if result.trimmed:
            log.info(
                "subgraph_builder | context trimmed | nodes_removed=%d "
                "final_tokens=%d | domain=%s",
                result.nodes_removed,
                result.token_count,
                domain,
            )

        return SubgraphOutput(
            context=self._maybe_append_trim_notice(result),
            node_count=raw.node_count,
            trimmed=result.trimmed,
            provenance_nodes=raw.provenance_nodes,
            anchor_resolutions=resolutions.as_flat_dict(),
            domain=domain,
            success=True,
            failure_reason=None,
        )

    @staticmethod
    def _maybe_append_trim_notice(result) -> str:
        """
        Appends a trim notice to the context block when budget was enforced.
        The Narration Agent reads this notice and qualifies any quantities
        it cannot source from the truncated context.
        """
        if not result.trimmed:
            return result.context
        return (
            result.context
            + f"\n[CONTEXT TRIMMED — {result.nodes_removed} node(s) removed "
            f"to meet {result.token_count}-token budget. "
            "Provenance and peripheral service nodes removed first. "
            "Anchor nodes always preserved.]"
        )
