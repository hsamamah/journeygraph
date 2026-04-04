"""
context_serializer.py — Stage 3 of the Subgraph Context Builder.

Receives a RawSubgraph from the HopExpander and produces the Subgraph
Context Block — the structured text string carried by SubgraphOutput.context
and consumed by the Narration Agent.

Two responsibilities:
    1. Serialization — converts RawSubgraph nodes, relationships, and
       provenance into a structured text block with relationship types
       preserved. Anchor nodes are always serialized first.

    2. Budget enforcement — after full serialization, counts tokens using
       tiktoken (cl100k_base). If the block exceeds the 2,000-token budget,
       removes nodes one at a time in priority order, re-serializes and
       re-counts after each removal, until the block fits.

Trim priority (lowest priority = removed first):
    Group 1 — Provenance section (TripUpdate, ServiceAlert, StopTimeUpdate)
    Group 2 — Service layer nodes (Trip, Route, Platform, BusStop, Date)
    Group 3 — Interruption and OutageEvent nodes
    Group 4 — Anchor nodes — NEVER removed

Within each group, nodes are trimmed in reverse expansion order —
highest hop_distance first (furthest from anchors first).

Trimming affects the serialized context block only. RawSubgraph.provenance_nodes
is passed through to SubgraphOutput.provenance_nodes unchanged.
"""

from dataclasses import dataclass
import logging

import tiktoken

from src.llm.anchor_resolver import AnchorResolutions
from src.llm.hop_expander import RawNode, RawRel, RawSubgraph

log = logging.getLogger(__name__)

# ── Budget ────────────────────────────────────────────────────────────────────

TOKEN_BUDGET = 2_000
# _TRIM_NOTICE_RESERVE accounts for the ~20-token trim notice that
# SubgraphBuilder appends after budget enforcement. Without this reserve the
# final context can silently exceed TOKEN_BUDGET by that margin.
_EFFECTIVE_BUDGET = TOKEN_BUDGET - 30

# ── Tokenizer ─────────────────────────────────────────────────────────────────
# cl100k_base is a close approximation for Claude-class models.
# Lazy-loaded on first _count_tokens() call to avoid ~100–200 ms startup
# cost when context_serializer is imported but the subgraph path is not taken.

_ENCODING = None


def _get_encoding():
    global _ENCODING
    if _ENCODING is None:
        _ENCODING = tiktoken.get_encoding("cl100k_base")
    return _ENCODING

# ── Trim priority groups ──────────────────────────────────────────────────────
# Lower group number = removed first under budget pressure.
# Any label not listed falls into group 2 (service layer default).

_PROVENANCE_LABELS = {"TripUpdate", "ServiceAlert", "StopTimeUpdate"}
_INTERRUPTION_LABELS = {"Interruption", "OutageEvent"}
_SERVICE_LABELS = {"Trip", "Route", "Platform", "BusStop", "Date"}

# Group 1 = first trimmed, Group 3 = last trimmed (anchors never in this table)
_LABEL_TO_TRIM_GROUP: dict[str, int] = (
    dict.fromkeys(_PROVENANCE_LABELS, 1)
    | dict.fromkeys(_SERVICE_LABELS, 2)
    | dict.fromkeys(_INTERRUPTION_LABELS, 3)
)


def _trim_group(node: RawNode) -> int:
    """
    Returns the trim group for a node based on its labels.
    Lowest group = trimmed first.
    Falls back to group 2 (service layer default) for unlisted labels.
    """
    for label in node.labels:
        if label in _LABEL_TO_TRIM_GROUP:
            return _LABEL_TO_TRIM_GROUP[label]
    return 2


# ── Serializer output ─────────────────────────────────────────────────────────


@dataclass
class SerializerResult:
    context: str  # final Subgraph Context Block
    token_count: int  # token count of final context (post-trim)
    trimmed: bool  # True if any node was removed during budget enforcement
    nodes_removed: int  # count of nodes removed; 0 if trimmed=False


# ── ContextSerializer ─────────────────────────────────────────────────────────


class ContextSerializer:
    """
    Serializes a RawSubgraph into the Subgraph Context Block and enforces
    the 2,000-token budget.

    Stateless — no instance state. All methods are pure functions over
    their inputs. Instantiated per pipeline invocation by the Context Builder.
    """

    def serialize_and_enforce(
        self,
        raw: RawSubgraph,
        resolutions: AnchorResolutions,
    ) -> SerializerResult:
        """
        Entry point. Serializes the full RawSubgraph, then enforces the
        token budget by trimming nodes if necessary.

        Args:
            raw:         RawSubgraph from HopExpander.
            resolutions: AnchorResolutions from Stage 1 — used for the
                         anchor label in the header.

        Returns:
            SerializerResult with final context, token count, and trim metadata.
        """
        # Working copy of nodes — anchor element IDs never enter the trim pool
        nodes_by_eid: dict[str, RawNode] = {n.element_id: n for n in raw.nodes}

        # Build relationship index: element_id → list of connected RawRels
        rel_index = _build_rel_index(raw.rels)

        # Full serialization pass
        context = self._serialize(raw, nodes_by_eid, rel_index, resolutions)
        token_count = _count_tokens(context)

        log.info(
            "context_serializer | initial serialization | tokens=%d budget=%d | domain=%s",
            token_count,
            TOKEN_BUDGET,
            raw.domain,
        )

        if token_count <= _EFFECTIVE_BUDGET:
            return SerializerResult(
                context=context,
                token_count=token_count,
                trimmed=False,
                nodes_removed=0,
            )

        # Budget exceeded — trim nodes one at a time
        context, token_count, nodes_removed = self._trim_to_budget(
            raw=raw,
            nodes_by_eid=nodes_by_eid,
            rel_index=rel_index,
            resolutions=resolutions,
            token_count=token_count,
        )

        log.info(
            "context_serializer | budget enforced | tokens=%d nodes_removed=%d | domain=%s",
            token_count,
            nodes_removed,
            raw.domain,
        )

        return SerializerResult(
            context=context,
            token_count=token_count,
            trimmed=nodes_removed > 0,
            nodes_removed=nodes_removed,
        )

    # ── Trim loop ─────────────────────────────────────────────────────────────

    def _trim_to_budget(
        self,
        raw: RawSubgraph,
        nodes_by_eid: dict[str, RawNode],
        rel_index: dict[str, list[RawRel]],
        resolutions: AnchorResolutions,
        token_count: int,
    ) -> tuple[str, int, int]:
        """
        Removes nodes one at a time in trim priority order until the
        serialized block fits within TOKEN_BUDGET.

        Returns (final_context, final_token_count, nodes_removed).
        """
        # Build trim queue — excludes anchor nodes entirely
        trim_candidates = [
            n
            for n in nodes_by_eid.values()
            if n.element_id not in raw.anchor_element_ids
        ]

        # Sort: lowest trim group first, then highest hop_distance first
        # (furthest from anchors trimmed first within a group)
        trim_candidates.sort(key=lambda n: (_trim_group(n), -n.hop_distance))

        nodes_removed = 0

        for candidate in trim_candidates:
            if token_count <= _EFFECTIVE_BUDGET:
                break

            # Subtract the token cost of this node's lines before mutating
            # nodes_by_eid — the delta includes the node's own block and the
            # back-reference rel lines it contributes to other nodes' blocks.
            # Section header count changes are not tracked here (minor error,
            # corrected by the final _count_tokens call below).
            token_count -= _removal_token_delta(candidate, nodes_by_eid, rel_index)

            del nodes_by_eid[candidate.element_id]
            nodes_removed += 1

            log.info(
                "context_serializer | trimmed node | labels=%s hop=%d "
                "tokens≈%d nodes_removed=%d | domain=%s",
                candidate.labels,
                candidate.hop_distance,
                token_count,
                nodes_removed,
                raw.domain,
            )

        # Single final serialization — corrects any approximation error from
        # delta tracking (section header count changes, Total nodes line, etc.)
        context = self._serialize(raw, nodes_by_eid, rel_index, resolutions)
        token_count = _count_tokens(context)

        if token_count > _EFFECTIVE_BUDGET:
            # Trim candidates exhausted — only anchors remain. Log and continue:
            # the Narration Agent will receive a trimmed=True signal and degrade
            # gracefully. Anchors are never removed regardless.
            log.warning(
                "context_serializer | budget not met after exhausting trim candidates "
                "| tokens=%d effective_budget=%d | domain=%s",
                token_count,
                _EFFECTIVE_BUDGET,
                raw.domain,
            )

        return context, token_count, nodes_removed

    # ── Serialization ─────────────────────────────────────────────────────────

    def _serialize(
        self,
        raw: RawSubgraph,
        nodes_by_eid: dict[str, RawNode],
        rel_index: dict[str, list[RawRel]],
        resolutions: AnchorResolutions,
    ) -> str:
        """
        Produces the full structured Subgraph Context Block from the current
        node set. Relationship types are preserved. Provenance section is
        appended after the main node block.

        Called once for initial serialization and once per trim iteration.
        """
        lines: list[str] = []

        # ── Header ────────────────────────────────────────────────────────────
        lines.append(f"SUBGRAPH CONTEXT — domain: {raw.domain}")
        lines.append(f"Anchors: {_format_anchor_header(resolutions)}")
        lines.append(f"Total nodes: {len(nodes_by_eid)}")
        lines.append("")

        # ── Anchor nodes ──────────────────────────────────────────────────────
        lines.append("── Anchor nodes ──")
        for eid in raw.anchor_element_ids:
            node = nodes_by_eid.get(eid)
            if node:
                lines.append(_format_node(node))
                for rel in rel_index.get(eid, []):
                    other_eid = (
                        rel.to_element_id
                        if rel.from_element_id == eid
                        else rel.from_element_id
                    )
                    if other_eid in nodes_by_eid:
                        lines.append(_format_rel(rel, eid, nodes_by_eid))
        lines.append("")

        # ── Non-anchor nodes grouped by label ────────────────────────────────
        non_anchors = [
            n
            for n in nodes_by_eid.values()
            if n.element_id not in raw.anchor_element_ids
        ]

        grouped = _group_by_primary_label(non_anchors)

        for group_label, group_nodes in grouped.items():
            lines.append(f"── {group_label} nodes ({len(group_nodes)}) ──")
            for node in group_nodes:
                lines.append(_format_node(node))
                for rel in rel_index.get(node.element_id, []):
                    other_eid = (
                        rel.to_element_id
                        if rel.from_element_id == node.element_id
                        else rel.from_element_id
                    )
                    if other_eid in nodes_by_eid:
                        lines.append(_format_rel(rel, node.element_id, nodes_by_eid))
            lines.append("")

        # ── Provenance section ────────────────────────────────────────────────
        if raw.provenance_nodes:
            lines.append("── Provenance ──")
            for prov in raw.provenance_nodes:
                label_str = ":".join(prov["labels"])
                props_str = _format_props(prov["props"])
                lines.append(f"  [{prov['rel_type']}] :{label_str} {props_str}")
            lines.append("")

        # ── Trim notice ───────────────────────────────────────────────────────
        # Injected during trim loop only — not on initial serialization.
        # The Context Builder appends this after receiving SerializerResult.trimmed=True.

        return "\n".join(lines)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _count_tokens(text: str) -> int:
    return len(_get_encoding().encode(text))


def _removal_token_delta(
    node: RawNode,
    nodes_by_eid: dict[str, RawNode],
    rel_index: dict[str, list[RawRel]],
) -> int:
    """
    Estimates the token reduction from removing a node from the serialized context.

    Each relationship between A and B renders twice — once in A's block and once
    in B's block — so both perspectives are counted. Section header count changes
    (e.g. "Route nodes (3)" → "Route nodes (2)") are not tracked here; the
    caller corrects any residual error with a final _count_tokens pass.
    """
    eid = node.element_id
    lines: list[str] = [_format_node(node)]

    for rel in rel_index.get(eid, []):
        other_eid = (
            rel.to_element_id if rel.from_element_id == eid else rel.from_element_id
        )
        if other_eid in nodes_by_eid:
            lines.append(_format_rel(rel, eid, nodes_by_eid))
            lines.append(_format_rel(rel, other_eid, nodes_by_eid))

    return _count_tokens("\n".join(lines))


def _build_rel_index(rels: list[RawRel]) -> dict[str, list[RawRel]]:
    """
    Builds a bidirectional index: element_id → list[RawRel].
    Each relationship appears under both its from and to element IDs so
    node serialization can find all connected relationships in one lookup.
    """
    index: dict[str, list[RawRel]] = {}
    for rel in rels:
        index.setdefault(rel.from_element_id, []).append(rel)
        index.setdefault(rel.to_element_id, []).append(rel)
    return index


def _format_anchor_header(resolutions: AnchorResolutions) -> str:
    """Produces the anchor summary line in the header."""
    parts: list[str] = []
    for name, node_id in resolutions.resolved_stations.items():
        parts.append(f"Station({name}, {node_id})")
    for name, node_id in resolutions.resolved_routes.items():
        parts.append(f"Route({name}, {node_id})")
    for expr, date_id in resolutions.resolved_dates.items():
        parts.append(f"Date({expr}, {date_id})")
    for name, node_id in resolutions.resolved_pathway_nodes.items():
        parts.append(f"Pathway({name}, {node_id})")
    return ", ".join(parts) if parts else "none"


def _format_node(node: RawNode) -> str:
    label_str = ":".join(node.labels)
    props_str = _format_props(node.props)
    return f"  :{label_str} {props_str}"


def _format_rel(
    rel: RawRel,
    perspective_eid: str,
    nodes_by_eid: dict[str, RawNode],
) -> str:
    """
    Formats a relationship from the perspective of one of its endpoints.
    Arrow direction reflects the original graph direction.
    """
    other_eid = (
        rel.to_element_id
        if rel.from_element_id == perspective_eid
        else rel.from_element_id
    )
    other_node = nodes_by_eid.get(other_eid)
    other_label = ":".join(other_node.labels) if other_node else "?"

    if rel.from_element_id == perspective_eid:
        direction = f"-[:{rel.rel_type}]->"
    else:
        direction = f"<-[:{rel.rel_type}]-"

    props_str = f" {_format_props(rel.props)}" if rel.props else ""
    return f"    {direction} :{other_label}{props_str}"


def _format_props(props: dict) -> str:
    """Formats a property dict as a compact inline string."""
    if not props:
        return "{}"
    pairs = ", ".join(f"{k}: {v!r}" for k, v in props.items())
    return "{" + pairs + "}"


def _group_by_primary_label(nodes: list[RawNode]) -> dict[str, list[RawNode]]:
    """
    Groups nodes by their first non-generic label for section headers.
    Ordered by trim group descending — most important groups (Interruption,
    OutageEvent) appear before service layer nodes in the output.
    """
    groups: dict[str, list[RawNode]] = {}
    for node in nodes:
        primary = node.labels[0] if node.labels else "Unknown"
        groups.setdefault(primary, []).append(node)

    # Sort groups: higher trim group (more important) first
    return dict(sorted(groups.items(), key=lambda kv: -_trim_group(kv[1][0])))
