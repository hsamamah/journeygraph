"""
subgraph_builder.py — Subgraph path orchestrator.

Wires Stage 2 (HopExpander) and Stage 3 (ContextSerializer) into a single
run() call. Anchor resolution (Stage 1) is performed upstream by the pipeline
orchestrator and passed in as AnchorResolutions — this allows the same
resolved anchors to be shared with the Text2Cypher path without a second
DB round-trip.

This is the only entry point into the Subgraph path. All downstream
consumers (Narration Agent) interact with SubgraphOutput only — the
internal stages are not exposed.

Failure handling:
    - Zero anchors in resolutions → immediate SubgraphOutput(success=False)
      via make_zero_anchor_fallback(). Treated as a defensive guard — the
      orchestrator should already have checked this before calling run().
    - Expansion returns empty node set → SubgraphOutput(success=False)
      with failure_reason describing which domain produced no results.
    - Any unhandled exception → SubgraphOutput(success=False) with
      failure_reason carrying the exception message. Never raises.
"""

import logging
from typing import TYPE_CHECKING

from src.llm.anchor_resolver import AnchorResolutions
from src.llm.context_serializer import ContextSerializer
from src.llm.hop_expander import HopExpander
from src.llm.subgraph_output import SubgraphOutput, make_zero_anchor_fallback

if TYPE_CHECKING:
    from src.common.neo4j_tools import Neo4jManager
    from src.llm.planner_output import PlannerOutput

log = logging.getLogger(__name__)


class SubgraphBuilder:
    """
    Orchestrates the two-stage Subgraph path (HopExpander + ContextSerializer).

    Anchor resolution is performed upstream by the pipeline orchestrator and
    passed into run() as AnchorResolutions. SubgraphBuilder has no DB
    dependency of its own beyond what HopExpander requires for hop queries.

    Args:
        db: Neo4jManager instance. Injected at construction —
            caller owns the connection lifecycle.
    """

    def __init__(
        self,
        db: Neo4jManager,
    ) -> None:
        self._expander = HopExpander(db=db)
        self._serializer = ContextSerializer()

    # ── Public ────────────────────────────────────────────────────────────────

    def run(
        self,
        planner_output: PlannerOutput,
        resolutions: AnchorResolutions,
        resolver_config: dict | None = None,
    ) -> SubgraphOutput:
        """
        Executes the Subgraph path (hop expansion + serialization) for a
        single pipeline invocation.

        Anchor resolution is performed upstream by the pipeline orchestrator.
        The zero-anchor check here is a defensive guard — the orchestrator
        should already have returned early before reaching this call.

        Args:
            planner_output:  PlannerOutput from the Planner. Consumed read-only
                             for domain and schema_slice_key.
            resolutions:     AnchorResolutions from the upstream resolver.
            resolver_config: AnchorResolver.config dict for pipeline trace
                             and A/B testing. Passed through to SubgraphOutput.
                             Defaults to empty dict if not provided.

        Returns:
            SubgraphOutput. Never raises — all failures produce a
            SubgraphOutput with success=False and a populated failure_reason.
        """
        domain = planner_output.domain
        cfg = resolver_config or {}

        try:
            return self._run(resolutions, domain, cfg)
        except Exception as exc:
            log.error(
                "subgraph_builder | unhandled exception | %s: %s | domain=%s",
                type(exc).__name__,
                exc,
                domain,
                exc_info=True,
            )
            return SubgraphOutput(
                context="",
                node_count=0,
                trimmed=False,
                provenance_nodes=[],
                anchor_resolutions={},
                domain=domain,
                success=False,
                failure_reason=f"Unhandled exception in Subgraph path: {exc}",
                resolver_config=cfg,
            )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _run(
        self,
        resolutions: AnchorResolutions,
        domain: str,
        resolver_config: dict,
    ) -> SubgraphOutput:
        # ── Defensive zero-anchor guard ───────────────────────────────────────
        if not resolutions.any_resolved:
            log.warning(
                "subgraph_builder | zero anchors in resolutions (unexpected) | domain=%s",
                domain,
            )
            return make_zero_anchor_fallback(domain)

        log.info(
            "subgraph_builder | anchors received | stations=%d routes=%d "
            "dates=%d pathway_nodes=%d | domain=%s",
            len(resolutions.resolved_stations),
            len(resolutions.resolved_routes),
            len(resolutions.resolved_dates),
            len(resolutions.resolved_pathway_nodes),
            domain,
        )

        # ── Stage 1: Hop expansion ────────────────────────────────────────────
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
                resolver_config=resolver_config,
            )

        log.info(
            "subgraph_builder | expansion complete | nodes=%d rels=%d "
            "provenance=%d | domain=%s",
            raw.node_count,
            len(raw.rels),
            len(raw.provenance_nodes),
            domain,
        )

        # ── Stage 2: Serialization and budget enforcement ─────────────────────
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
            resolver_config=resolver_config,
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
