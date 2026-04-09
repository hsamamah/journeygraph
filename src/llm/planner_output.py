# src/llm/planner_output.py
"""
PlannerOutput — the contract between the Planner and all downstream agents.

PlannerOutput is the single dataclass passed from the Planner to every
downstream pipeline stage (Query Writer, Cypher Validator, Context Builder,
Narration Agent). All downstream agents consume it read-only.

PlannerAnchors is a typed boundary point for extracted entities. Keeping
anchors typed rather than a plain dict means any change to anchor structure
is an explicit decision, and downstream consumers have a clear contract.

Design notes:
    schema_slice_key is kept separate from domain to support future
    free-form queries that may reference the nearest domain slice without
    belonging to a named domain. For current domains the two values are
    identical — the separation is an extension point, not current behaviour.

    parse_warning is set when Stage 2 JSON parsing failed after one retry
    and the Planner degraded to text2cypher-only with empty anchors. The
    presence of parse_warning signals to downstream agents that anchor
    extraction did not succeed and they should work from the raw query alone.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PlannerAnchors:
    """
    Entities extracted from the user query by the Planner Stage 2 LLM call.

    All fields default to empty lists. On Stage 2 degradation (JSON parse
    failure after retry), all fields remain empty and PlannerOutput will
    carry a parse_warning describing the failure.

    Attributes:
        stations:      Station names e.g. ['Metro Center', 'Gallery Place']
        routes:        Route names or line names e.g. ['Red Line', '70']
        dates:         Date references e.g. ['2026-03-15', 'yesterday']
        pathway_nodes: Pathway node references e.g. ['elevator at Metro Center']
        levels:        Floor/level references e.g. ['street level', 'mezzanine', 'L2']
    """

    stations: list[str] = field(default_factory=list)
    routes: list[str] = field(default_factory=list)
    dates: list[str] = field(default_factory=list)
    pathway_nodes: list[str] = field(default_factory=list)
    levels: list[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        """Return True if no anchors were extracted across all fields."""
        return not any([self.stations, self.routes, self.dates, self.pathway_nodes, self.levels])


@dataclass
class PlannerOutput:
    """
    Output contract from the Planner to all downstream agents.

    Attributes:
        domain:            Classified query domain.
                           Values: transfer_impact | accessibility |
                           delay_propagation
        path:              Selected execution path.
                           Values: text2cypher | subgraph | both
        anchors:           Typed entity anchors extracted from the query.
        schema_slice_key:  Key into SliceRegistry.get(). Separate from domain
                           to support future free-form query routing.
        rejected:          True if Stage 1 produced a zero score across all
                           domains. Downstream agents must check this first.
        rejection_message: Human-readable explanation when rejected=True.
                           None when rejected=False.
        path_reasoning:    Stage 2 LLM explanation of the path decision.
                           None when rejected=True or Stage 2 degraded.
        anchor_notes:      Stage 2 LLM notes on anchor extraction e.g.
                           'Date inferred from yesterday'. None when
                           rejected=True or Stage 2 degraded.
        parse_warning:     Set when Stage 2 JSON parsing failed after one
                           retry. Describes the failure. None on success.
    """

    domain: str
    path: str
    anchors: PlannerAnchors
    schema_slice_key: str
    rejected: bool
    rejection_message: str | None
    path_reasoning: str | None
    anchor_notes: str | None
    parse_warning: str | None
