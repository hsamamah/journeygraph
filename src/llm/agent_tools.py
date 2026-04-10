# src/llm/agent_tools.py
"""
Agent tool definitions and execute functions for the agentic pipeline.

Each tool is:
  - An Input dataclass whose fields map 1:1 to the Anthropic input_schema
  - An Output dataclass (or the existing SubgraphOutput for subgraph_expand)
  - An execute_* function that wraps the existing pipeline component

Tools are typed by retrieval method (what kind of graph access they perform),
not by pipeline stage. This makes them natural for an LLM to reason about
and aligns with the NeoConverse pattern.

TOOL_DEFINITIONS is the list of Anthropic-format tool dicts passed as
client.messages.create(tools=TOOL_DEFINITIONS).

Entry points:
    execute_full_text_search  — wraps AnchorResolver
    execute_cypher_query      — wraps QueryWriter + CypherValidator (3 retries)
    execute_subgraph_expand   — wraps SubgraphBuilder.run()
    execute_entity_clarify    — wraps AnchorClarifier.clarify()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from src.common.logger import get_logger
from src.llm.anchor_resolver import AnchorResolutions, AnchorResolver
from src.llm.cypher_validator import validate_and_log_cypher
from src.llm.planner_output import PlannerAnchors
from src.llm.query_writer import run_query_writer
from src.llm.subgraph_builder import SubgraphBuilder

if TYPE_CHECKING:
    from src.common.config import LLMConfig
    from src.common.neo4j_tools import Neo4jManager
    from src.llm.anchor_clarifier import AnchorClarifier
    from src.llm.planner_output import PlannerOutput
    from src.llm.slice_registry import SliceRegistry
    from src.llm.subgraph_output import SubgraphOutput

log = get_logger(__name__)

# ── Tool 1: full_text_search ──────────────────────────────────────────────────


@dataclass
class FullTextSearchInput:
    mention: str        # entity mention from the user query
    anchor_type: str    # "station" | "route" | "pathway_node" | "date"


@dataclass
class FullTextSearchOutput:
    mention: str
    resolved_ids: list[str]     # empty on failure
    success: bool
    failure_reason: str | None = None


def execute_full_text_search(
    inp: FullTextSearchInput,
    *,
    resolver: AnchorResolver,
) -> FullTextSearchOutput:
    """
    Resolve one entity mention to graph node IDs via full-text Lucene index.
    Wraps AnchorResolver.resolve() on a synthetic single-mention PlannerAnchors.
    """
    type_field = {
        "station": "stations",
        "route": "routes",
        "date": "dates",
        "pathway_node": "pathway_nodes",
        "level": "levels",
    }.get(inp.anchor_type)

    if type_field is None:
        return FullTextSearchOutput(
            mention=inp.mention,
            resolved_ids=[],
            success=False,
            failure_reason=f"Unknown anchor_type '{inp.anchor_type}'",
        )

    kwargs: dict[str, list[str]] = {
        "stations": [],
        "routes": [],
        "dates": [],
        "pathway_nodes": [],
        "levels": [],
    }
    kwargs[type_field] = [inp.mention]
    synthetic = PlannerAnchors(**kwargs)

    resolutions = resolver.resolve(synthetic)
    ids = resolutions.as_flat_dict().get(inp.mention, [])

    if ids:
        return FullTextSearchOutput(
            mention=inp.mention,
            resolved_ids=ids,
            success=True,
        )

    reason = resolutions.failed.get(inp.mention, "No candidates found in full-text index")
    return FullTextSearchOutput(
        mention=inp.mention,
        resolved_ids=[],
        success=False,
        failure_reason=reason,
    )


# ── Tool 2: cypher_query ──────────────────────────────────────────────────────


@dataclass
class CypherQueryInput:
    question: str                           # focused natural language sub-question
    schema_slice_key: str                   # "transfer_impact" | "accessibility" | "delay_propagation"
    resolved_anchors: dict[str, list[str]]  # mention → [id, ...]


@dataclass
class CypherQueryOutput:
    cypher: str
    results: list[dict]
    attempt_count: int
    validation_notes: list[str] = field(default_factory=list)
    success: bool = False
    failure_reason: str | None = None


def execute_cypher_query(
    inp: CypherQueryInput,
    *,
    planner_output: PlannerOutput,
    llm_config: LLMConfig,
    registry: SliceRegistry,
    db: Neo4jManager,
) -> CypherQueryOutput:
    """
    Generate and execute a validated Cypher query with up to 3 retry attempts.
    Wraps run_query_writer() + validate_and_log_cypher() — mirrors the static
    pipeline retry loop in run.py exactly.
    """
    _MAX_ATTEMPTS = 3

    try:
        schema_slice = registry.get(inp.schema_slice_key)
    except Exception as exc:
        return CypherQueryOutput(
            cypher="",
            results=[],
            attempt_count=0,
            success=False,
            failure_reason=f"Schema slice lookup failed for '{inp.schema_slice_key}': {exc}",
        )

    refinement_errors: list[str] = []
    all_validation_notes: list[str] = []

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            qw_output = run_query_writer(
                inp.question,
                planner_output,
                llm_config,
                schema_slice=schema_slice,
                resolved_anchors=inp.resolved_anchors,
                refinement_errors=refinement_errors or None,
            )
        except Exception as exc:
            log.error(
                "agent_tools | cypher_query | QueryWriter error at attempt=%d | %s: %s",
                attempt,
                type(exc).__name__,
                exc,
            )
            return CypherQueryOutput(
                cypher="",
                results=[],
                attempt_count=attempt,
                validation_notes=all_validation_notes,
                success=False,
                failure_reason=f"QueryWriter raised {type(exc).__name__}: {exc}",
            )

        val_result = validate_and_log_cypher(
            qw_output.cypher_query,
            schema_slice,
            schema_slice.property_registry,
            db.driver,
            log,
        )

        if val_result.valid:
            log.info(
                "agent_tools | cypher_query | valid at attempt=%d | rows=%d",
                attempt,
                len(val_result.results or []),
            )
            return CypherQueryOutput(
                cypher=qw_output.cypher_query,
                results=val_result.results or [],
                attempt_count=attempt,
                validation_notes=all_validation_notes,
                success=True,
            )

        log.warning(
            "agent_tools | cypher_query | validation failed | attempt=%d/%d | errors=%s",
            attempt,
            _MAX_ATTEMPTS,
            val_result.errors,
        )
        all_validation_notes.extend(val_result.errors)
        refinement_errors = val_result.errors

    return CypherQueryOutput(
        cypher="",
        results=[],
        attempt_count=_MAX_ATTEMPTS,
        validation_notes=all_validation_notes,
        success=False,
        failure_reason=f"All {_MAX_ATTEMPTS} validation attempts failed",
    )


# ── Tool 3: subgraph_expand ───────────────────────────────────────────────────


@dataclass
class SubgraphExpandInput:
    anchor_ids: dict[str, list[str]]    # {"Metro Center": ["STN_A01"]}
    anchor_type: str                     # "station" | "route" | "pathway_node"


def execute_subgraph_expand(
    inp: SubgraphExpandInput,
    *,
    db: Neo4jManager,
    planner_output: PlannerOutput,
    base_resolutions: AnchorResolutions,
    resolver_config: dict,
) -> SubgraphOutput:
    """
    Expand a subgraph neighbourhood from resolved anchor node IDs.
    Wraps SubgraphBuilder.run(). Merges the agent's anchor_ids into the
    base resolutions from the pre-step so all known anchors are included.
    """
    # Start from a copy of the pre-step resolutions so we don't mutate it
    synthetic = AnchorResolutions(
        resolved_stations=dict(base_resolutions.resolved_stations),
        resolved_routes=dict(base_resolutions.resolved_routes),
        resolved_dates=dict(base_resolutions.resolved_dates),
        resolved_pathway_nodes=dict(base_resolutions.resolved_pathway_nodes),
        resolved_levels=dict(base_resolutions.resolved_levels),
    )

    # Merge the agent-specified anchor_ids into the appropriate typed field
    if inp.anchor_type == "station":
        synthetic.resolved_stations.update(inp.anchor_ids)
    elif inp.anchor_type == "route":
        synthetic.resolved_routes.update(inp.anchor_ids)
    elif inp.anchor_type == "pathway_node":
        synthetic.resolved_pathway_nodes.update(inp.anchor_ids)

    builder = SubgraphBuilder(db=db)
    return builder.run(planner_output, synthetic, resolver_config=resolver_config)


# ── Tool 4: entity_clarify ────────────────────────────────────────────────────


@dataclass
class EntityClarifyInput:
    failed_mentions: list[str]  # names that returned nothing from full_text_search
    anchor_type: str            # "station" | "route" only


@dataclass
class EntityClarifyOutput:
    resolved_ids: dict[str, list[str]]  # mention → [id, ...] for newly resolved mentions
    success: bool
    failure_reason: str | None = None


def execute_entity_clarify(
    inp: EntityClarifyInput,
    *,
    clarifier: AnchorClarifier,
    resolver: AnchorResolver,
    base_resolutions: AnchorResolutions,
) -> EntityClarifyOutput:
    """
    LLM-assisted repair for station/route names that failed full-text lookup.
    Wraps AnchorClarifier.clarify() on a synthetic AnchorResolutions containing
    only the failed mentions. The clarifier's _partition_failures() checks for
    "Station" or "Route" in the failure reason string.
    """
    if inp.anchor_type not in {"station", "route"}:
        return EntityClarifyOutput(
            resolved_ids={},
            success=False,
            failure_reason=f"entity_clarify only supports 'station' and 'route', got '{inp.anchor_type}'",
        )

    # Build failure reasons that match the strings AnchorClarifier._partition_failures expects
    reason_label = "Station" if inp.anchor_type == "station" else "Route"
    synthetic_failed = {
        mention: f"No {reason_label} matched '{mention}'"
        for mention in inp.failed_mentions
    }

    # Build a synthetic AnchorResolutions with copies of base data + the new failures
    synthetic = AnchorResolutions(
        resolved_stations=dict(base_resolutions.resolved_stations),
        resolved_routes=dict(base_resolutions.resolved_routes),
        resolved_dates=dict(base_resolutions.resolved_dates),
        resolved_pathway_nodes=dict(base_resolutions.resolved_pathway_nodes),
        resolved_levels=dict(base_resolutions.resolved_levels),
        failed=synthetic_failed,
    )

    clarifier.clarify(synthetic, resolver)  # mutates synthetic in place

    # Collect mentions that were successfully moved out of failed
    newly_resolved: dict[str, list[str]] = {}
    for mention in inp.failed_mentions:
        if mention not in synthetic.failed:
            ids = synthetic.as_flat_dict().get(mention, [])
            if ids:
                newly_resolved[mention] = ids

    if newly_resolved:
        log.info(
            "agent_tools | entity_clarify | resolved=%s",
            list(newly_resolved.keys()),
        )
        return EntityClarifyOutput(resolved_ids=newly_resolved, success=True)

    return EntityClarifyOutput(
        resolved_ids={},
        success=False,
        failure_reason="No mentions could be matched to valid WMATA names",
    )


# ── Anthropic tool definitions ────────────────────────────────────────────────
# Passed as tools=[...] in client.messages.create().
# Descriptions are written to guide the LLM in selecting the right tool.

TOOL_DEFINITIONS: list[dict] = [
    {
        "name": "full_text_search",
        "description": (
            "Resolve a transit entity name (station, route, pathway node, or date) "
            "to its graph node ID using the full-text Lucene index. Use this when "
            "you need the canonical ID for an entity before running a Cypher query "
            "or expanding a subgraph. Returns a list of candidate IDs ranked by "
            "string match score. Check the pre-resolved anchors in the user message "
            "first — call this only for entities not already resolved. "
            "The returned IDs are NOT automatically added to the shared anchor set — "
            "you must pass them explicitly in the resolved_anchors argument of "
            "cypher_query, or in anchor_ids for subgraph_expand."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "mention": {
                    "type": "string",
                    "description": (
                        "The raw entity mention from the user query, "
                        "e.g. 'Metro Center', 'Red Line', 'A01 Elevator 1'"
                    ),
                },
                "anchor_type": {
                    "type": "string",
                    "enum": ["station", "route", "pathway_node", "date"],
                    "description": "The type of entity to look up",
                },
            },
            "required": ["mention", "anchor_type"],
        },
    },
    {
        "name": "cypher_query",
        "description": (
            "Generate and execute a validated Cypher query against the Neo4j WMATA "
            "knowledge graph. Provide a focused natural language question along with "
            "the resolved entity IDs you already know. Retries up to 3 times with "
            "validator feedback. Use this for precise aggregate queries (counts, trip IDs, "
            "booleans) where exact numeric results matter rather than contextual descriptions."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": (
                        "Focused natural language question for this Cypher query, "
                        "e.g. 'How many trips were cancelled on the Red Line on 20260409?'"
                    ),
                },
                "schema_slice_key": {
                    "type": "string",
                    "enum": ["transfer_impact", "accessibility", "delay_propagation"],
                    "description": "The schema domain slice to constrain query generation",
                },
                "resolved_anchors": {
                    "type": "object",
                    "description": (
                        "Map of entity mention to resolved graph ID list, "
                        "e.g. {\"Red Line\": [\"RED\"], \"yesterday\": [\"20260409\"]}"
                    ),
                    "additionalProperties": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
            "required": ["question", "schema_slice_key", "resolved_anchors"],
        },
    },
    {
        "name": "subgraph_expand",
        "description": (
            "Expand a neighbourhood subgraph around resolved anchor node IDs. "
            "Retrieves connected service alerts, trip updates, elevator/escalator outages, "
            "and pathway topology. Use this for contextual questions where you need the "
            "surrounding graph structure rather than a single aggregate metric. "
            "Returns a serialized context block ready for the narration agent."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "anchor_ids": {
                    "type": "object",
                    "description": (
                        "Map of entity display name to list of resolved graph node IDs, "
                        "e.g. {\"Gallery Place\": [\"STN_B01\"]}"
                    ),
                    "additionalProperties": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "anchor_type": {
                    "type": "string",
                    "enum": ["station", "route", "pathway_node"],
                    "description": "The primary anchor type driving the expansion",
                },
            },
            "required": ["anchor_ids", "anchor_type"],
        },
    },
    {
        "name": "entity_clarify",
        "description": (
            "LLM-assisted repair for station or route names that failed the "
            "full-text index lookup. Use this only after full_text_search has returned "
            "no results for a station or route mention. Fuzzy-matches the failed mention "
            "against the full WMATA name catalogue and re-resolves. "
            "Not applicable to dates or pathway nodes."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "failed_mentions": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "List of entity names that returned no results from full_text_search"
                    ),
                },
                "anchor_type": {
                    "type": "string",
                    "enum": ["station", "route"],
                    "description": (
                        "Type of the failed entities — "
                        "only station and route are eligible for clarification"
                    ),
                },
            },
            "required": ["failed_mentions", "anchor_type"],
        },
    },
]
