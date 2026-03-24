from dataclasses import dataclass, field


@dataclass
class SubgraphOutput:
    context: str                  # serialized Subgraph Context Block after budget enforcement;
                                  # empty string on failure
    node_count: int               # total nodes after full expansion and provenance pass,
                                  # before any trimming; 0 on failure
    trimmed: bool                 # True if any node was removed during budget enforcement
    provenance_nodes: list[dict]  # complete post-expansion provenance fetch ---
                                  # TripUpdate/ServiceAlert raw properties;
                                  # never affected by trimming; empty list on failure
    anchor_resolutions: dict      # {'Metro Center': 'STN_A01'}; empty dict on failure
    domain: str
    success: bool                 # False if zero anchors resolved or expansion failed
    failure_reason: str | None    # 'No anchors resolved from query' | None


def make_zero_anchor_fallback(domain: str) -> SubgraphOutput:
    """
    Returns the canonical SubgraphOutput for the zero-anchor failure case.
    Called by the Context Builder immediately after anchor resolution
    if no anchors resolved across all types. No expansion or serialization
    is attempted. The Narration Agent's Input Assembler treats success=False
    the same as a missing SubgraphOutput --- routes to degraded or precision
    mode depending on whether Text2Cypher succeeded.
    """
    return SubgraphOutput(
        context="",
        node_count=0,
        trimmed=False,
        provenance_nodes=[],
        anchor_resolutions={},
        domain=domain,
        success=False,
        failure_reason="No anchors resolved from query",
    )
