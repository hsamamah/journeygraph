# src/llm/agent.py
"""
AgentOrchestrator — agentic pipeline for the JourneyGraph LLM layer.

Coexists alongside the static pipeline in run.py. Both pipelines share the
Planner + AnchorResolver + AnchorClarifier pre-step and the NarrationAgent
terminal step. The agentic path replaces the fixed QueryWriter/SubgraphBuilder
fork with a Claude agent loop that selects tools dynamically.

Architecture:
    Shared pre-step (run.py)
        Planner.run(query) → PlannerOutput
        AnchorResolver.resolve(anchors) → AnchorResolutions
        AnchorClarifier.clarify() [if failed]
            ↓
    AgentOrchestrator.run(query, planner_output, resolutions, resolver)
        Builds system prompt (domain + SchemaSlice constraints + tool guidance)
        Initial user message (query + pre-resolved anchors)
        Agent loop (max 5 iterations):
            client.messages.create(tools=TOOL_DEFINITIONS, tool_choice="auto")
            → tool_use blocks dispatched to execute_* functions in agent_tools.py
            → tool_result messages appended
            → stop on end_turn or budget exhaustion
        AgentContext projected → Text2CypherOutput | None, SubgraphOutput | None
        NarrationAgent.run() called with accumulated outputs
            ↓
    Returns (t2c_output, sub_output, narration_output) — same 4-tuple shape
    as _run_query() in run.py for eval harness symmetry.

The agent_trace dict in NarrationOutput carries tool_call_history for
A/B evaluation against the static pipeline.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

import anthropic

from src.common.logger import get_logger
from src.llm.agent_tools import (
    TOOL_DEFINITIONS,
    CypherQueryInput,
    CypherQueryOutput,
    EntityClarifyInput,
    FullTextSearchInput,
    SubgraphExpandInput,
    execute_cypher_query,
    execute_entity_clarify,
    execute_full_text_search,
    execute_subgraph_expand,
)
from src.llm.anchor_resolver import AnchorResolutions
from src.llm.subgraph_output import SubgraphOutput
from src.llm.text2cypher_output import Text2CypherOutput

if TYPE_CHECKING:
    from src.common.config import LLMConfig
    from src.common.neo4j_tools import Neo4jManager
    from src.llm.anchor_clarifier import AnchorClarifier
    from src.llm.anchor_resolver import AnchorResolver
    from src.llm.narration_agent import NarrationAgent
    from src.llm.narration_output import NarrationOutput
    from src.llm.planner_output import PlannerOutput
    from src.llm.slice_registry import SchemaSlice, SliceRegistry

log = get_logger(__name__)

_MAX_ITERATIONS = 5


# ── AgentContext — mutable accumulator across the loop ────────────────────────


@dataclass
class AgentContext:
    """
    Accumulates tool outputs across all iterations of the agent loop.

    Projection methods produce the Text2CypherOutput and SubgraphOutput
    that NarrationAgent.run() expects — same types as the static pipeline.
    The tool_call_history is attached to NarrationOutput.agent_trace for
    A/B evaluation against the static pipeline.
    """

    cypher_results: list[CypherQueryOutput] = field(default_factory=list)
    subgraph_results: list[SubgraphOutput] = field(default_factory=list)
    # (tool_name, input_dict, summary_dict) — ordered record of every tool call
    tool_call_history: list[tuple[str, dict, dict]] = field(default_factory=list)

    @property
    def has_cypher_data(self) -> bool:
        return any(r.success for r in self.cypher_results)

    @property
    def has_subgraph_data(self) -> bool:
        return any(r.success for r in self.subgraph_results)

    def project_t2c(self, domain: str) -> Text2CypherOutput | None:
        """
        Project accumulated Cypher results → Text2CypherOutput.

        Returns None when cypher_query was never called.
        Returns a failed Text2CypherOutput when all calls failed (so
        NarrationAgent can route to degraded mode correctly).
        Uses the last successful result when multiple calls succeeded.
        """
        if not self.cypher_results:
            return None

        all_notes = [n for r in self.cypher_results for n in r.validation_notes]
        total_attempts = sum(r.attempt_count for r in self.cypher_results)
        successful = [r for r in self.cypher_results if r.success]

        if not successful:
            return Text2CypherOutput(
                cypher="",
                results=[],
                domain=domain,
                attempt_count=total_attempts,
                validation_notes=all_notes,
                success=False,
            )

        last = successful[-1]
        return Text2CypherOutput(
            cypher=last.cypher,
            results=last.results,
            domain=domain,
            attempt_count=total_attempts,
            validation_notes=all_notes,
            success=True,
        )

    def project_subgraph(self) -> SubgraphOutput | None:
        """
        Project accumulated subgraph results → SubgraphOutput.

        Returns None when subgraph_expand was never called or all calls failed.
        Merges multiple successful SubgraphOutputs by joining their context
        blocks — NarrationAgent treats the merged string as a single context.
        """
        successful = [r for r in self.subgraph_results if r.success]
        if not successful:
            return None

        if len(successful) == 1:
            return successful[0]

        # Merge multiple successful expansions
        merged_context = "\n\n".join(r.context for r in successful if r.context)
        merged_anchors: dict = {}
        merged_provenance: list[dict] = []
        for r in successful:
            merged_anchors.update(r.anchor_resolutions)
            merged_provenance.extend(r.provenance_nodes)
        last = successful[-1]
        return SubgraphOutput(
            context=merged_context,
            node_count=sum(r.node_count for r in successful),
            trimmed=any(r.trimmed for r in successful),
            provenance_nodes=merged_provenance,
            anchor_resolutions=merged_anchors,
            domain=last.domain,
            success=True,
            failure_reason=None,
            resolver_config=last.resolver_config,
        )

    def as_trace_dict(self) -> dict:
        """Serializable summary attached to NarrationOutput.agent_trace."""
        return {
            "mode": "agentic",
            "total_tool_calls": len(self.tool_call_history),
            "has_cypher_data": self.has_cypher_data,
            "has_subgraph_data": self.has_subgraph_data,
            "tool_call_history": [
                {"tool": name, "input": inp, "output": out}
                for name, inp, out in self.tool_call_history
            ],
        }


# ── AgentOrchestrator ─────────────────────────────────────────────────────────


class AgentOrchestrator:
    """
    Agentic pipeline orchestrator — coexists with the static pipeline.

    Wraps existing JourneyGraph pipeline components as tools and exposes
    them to a Claude agent via function calling (tool_use). The agent
    decides which tools to call based on the query and observed results.

    Constructed once at startup (same lifecycle as NarrationAgent). The
    run() method is called per query by _run_query_agentic() in run.py.

    Args:
        db:              Neo4jManager — held open across queries.
        llm_config:      LLMConfig — model, API key, token budgets.
        registry:        SliceRegistry — schema slices for domain constraints.
        clarifier:       AnchorClarifier — for the entity_clarify tool.
        narration_agent: NarrationAgent — called after the agent loop.
    """

    def __init__(
        self,
        *,
        db: Neo4jManager,
        llm_config: LLMConfig,
        registry: SliceRegistry,
        clarifier: AnchorClarifier,
        narration_agent: NarrationAgent,
    ) -> None:
        self._db = db
        self._llm_config = llm_config
        self._registry = registry
        self._clarifier = clarifier
        self._narration_agent = narration_agent
        self._client = anthropic.Anthropic(api_key=llm_config.anthropic_api_key)
        log.info(
            "AgentOrchestrator ready | model=%s max_iterations=%d",
            llm_config.llm_model,
            _MAX_ITERATIONS,
        )

    # ── Public ────────────────────────────────────────────────────────────────

    def run(
        self,
        query: str,
        planner_output: PlannerOutput,
        resolutions: AnchorResolutions,
        resolver: AnchorResolver,
        invocation_time: datetime,
    ) -> tuple[Text2CypherOutput | None, SubgraphOutput | None, NarrationOutput]:
        """
        Run the agentic pipeline for a single query.

        The Planner + AnchorResolver + AnchorClarifier pre-step must have
        already run — resolutions and resolver are injected from that shared
        step. The NarrationAgent is called after the loop with whatever data
        the agent gathered, using the same call signature as the static pipeline.

        Args:
            query:           Raw user query string.
            planner_output:  PlannerOutput from the shared Planner step.
            resolutions:     AnchorResolutions from the shared pre-step.
            resolver:        AnchorResolver instance (same invocation_time).
            invocation_time: Pipeline invocation time for date resolution.

        Returns:
            (t2c_output, sub_output, narration_output) — mirrors the return
            shape of _run_query() for eval harness symmetry.
        """
        schema_slice = self._registry.get(planner_output.schema_slice_key)
        system_prompt = self._build_system_prompt(
            planner_output, resolutions, schema_slice, invocation_time
        )
        context = AgentContext()
        resolver_config = resolver.config

        # Build initial user message — include pre-step resolution context
        flat_anchors = resolutions.as_flat_dict()
        if flat_anchors:
            anchor_lines = "\n".join(
                f"  {mention} → {ids}" for mention, ids in flat_anchors.items()
            )
        else:
            anchor_lines = "  (none resolved in pre-step)"

        initial_message = f"Query: {query}\n\nPre-resolved anchors:\n{anchor_lines}"
        messages: list[dict] = [{"role": "user", "content": initial_message}]

        log.info(
            "agent | starting loop | domain=%s anchors=%d max_iterations=%d",
            planner_output.domain,
            len(flat_anchors),
            _MAX_ITERATIONS,
        )

        # ── Agent loop ─────────────────────────────────────────────────────────
        for iteration in range(_MAX_ITERATIONS):
            try:
                response = self._client.messages.create(
                    model=self._llm_config.llm_model,
                    max_tokens=self._llm_config.llm_narration_max_tokens,
                    system=system_prompt,
                    tools=TOOL_DEFINITIONS,
                    tool_choice={"type": "auto"},
                    messages=messages,
                )
            except anthropic.APIError as exc:
                log.error(
                    "agent | API error at iteration=%d | %s: %s",
                    iteration,
                    type(exc).__name__,
                    exc,
                )
                break

            # Append the full assistant message (may contain text + tool_use blocks)
            messages.append({"role": "assistant", "content": response.content})

            log.info(
                "agent | iteration=%d stop_reason=%s",
                iteration,
                response.stop_reason,
            )

            if response.stop_reason == "end_turn":
                break

            if response.stop_reason != "tool_use":
                log.warning(
                    "agent | unexpected stop_reason=%r at iteration=%d — stopping",
                    response.stop_reason,
                    iteration,
                )
                break

            # Dispatch all tool_use blocks from this response turn
            tool_results: list[dict] = []
            any_success = False

            for block in response.content:
                if block.type != "tool_use":
                    continue

                result_content, succeeded = self._dispatch_tool(
                    block.name,
                    block.input,
                    context=context,
                    planner_output=planner_output,
                    resolutions=resolutions,
                    resolver=resolver,
                    resolver_config=resolver_config,
                )
                if succeeded:
                    any_success = True

                # All tool_use blocks from one assistant turn must be resolved
                # in a single user message per the Anthropic multi-tool pattern.
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_content,
                })

            messages.append({"role": "user", "content": tool_results})

            # Stop early if all tools failed after the first iteration —
            # the LLM has seen failure feedback and is likely stuck.
            if not any_success and iteration > 0:
                log.warning(
                    "agent | all tools failed at iteration=%d — stopping early",
                    iteration,
                )
                break

        # ── Final synthesis turn (budget exhaustion only) ─────────────────────
        # If the loop exited while the last message is a user turn (tool results),
        # the model has not yet seen those results. One additional call with
        # tool_choice=none lets it synthesise before NarrationAgent is invoked.
        if messages and messages[-1].get("role") == "user" and len(messages) > 1:
            try:
                synthesis = self._client.messages.create(
                    model=self._llm_config.llm_model,
                    max_tokens=self._llm_config.llm_narration_max_tokens,
                    system=system_prompt,
                    tools=TOOL_DEFINITIONS,
                    tool_choice={"type": "none"},
                    messages=messages,
                )
                messages.append({"role": "assistant", "content": synthesis.content})
                log.info("agent | synthesis turn complete | stop_reason=%s", synthesis.stop_reason)
            except anthropic.APIError as exc:
                log.warning("agent | synthesis turn failed — %s: %s", type(exc).__name__, exc)

        # ── Project accumulated results → NarrationAgent inputs ────────────────
        t2c_output = context.project_t2c(planner_output.domain)
        sub_output = context.project_subgraph()

        log.info(
            "agent | loop complete | tool_calls=%d has_cypher=%s has_subgraph=%s",
            len(context.tool_call_history),
            context.has_cypher_data,
            context.has_subgraph_data,
        )

        narration_output = self._narration_agent.run(
            query,
            planner_output,
            t2c_output=t2c_output,
            subgraph_output=sub_output,
            resolutions=resolutions,
        )

        # Attach agent trace — consumed by eval harness, not by NarrationAgent
        narration_output.agent_trace = context.as_trace_dict()

        return t2c_output, sub_output, narration_output

    # ── System prompt ─────────────────────────────────────────────────────────

    def _build_system_prompt(
        self,
        planner_output: PlannerOutput,
        resolutions: AnchorResolutions,
        schema_slice: SchemaSlice,
        invocation_time: datetime,
    ) -> str:
        """
        Build the agent loop system prompt.

        Three sections:
          1. Role and domain framing
          2. SchemaSlice constraints (same content as QueryWriter's system prompt)
          3. Tool selection guidance and iteration budget
        """
        domain_descriptions = {
            "transfer_impact": "cancelled, skipped, or disrupted trips affecting connections",
            "delay_propagation": "delays spreading across routes, trips, or stops",
            "accessibility": "elevator/escalator outages and accessible pathway status",
        }
        domain_desc = domain_descriptions.get(planner_output.domain, planner_output.domain)
        invocation_date = invocation_time.strftime("%Y-%m-%d")

        parts: list[str] = []

        # Section 1 — Role and domain
        parts.append(
            f"You are a WMATA transit knowledge graph query agent for the "
            f"{planner_output.domain} domain ({domain_desc}).\n"
            f"Today's date is {invocation_date}. All date node IDs use YYYYMMDD format."
        )

        # Section 2 — Schema constraints
        node_list = "\n".join(f"  {n}" for n in schema_slice.nodes)
        parts.append(f"Allowed node labels (use ONLY these in Cypher):\n{node_list}")

        rel_list = "\n".join(f"  {r}" for r in schema_slice.relationships)
        parts.append(f"Allowed relationship types:\n{rel_list}")

        if schema_slice.nodes_optional:
            opt_nodes = "\n".join(f"  {n}" for n in schema_slice.nodes_optional)
            parts.append(
                f"Optional node labels (valid schema, may have no live data):\n{opt_nodes}"
            )

        if schema_slice.patterns:
            pat_list = "\n".join(f"  {p}" for p in schema_slice.patterns)
            parts.append(f"Key traversal patterns:\n{pat_list}")

        if schema_slice.warnings:
            warn_list = "\n".join(f"  - {w}" for w in schema_slice.warnings)
            parts.append(f"IMPORTANT data quirks:\n{warn_list}")

        # Section 3 — Tool selection guidance
        parts.append(
            "Tool selection strategy:\n"
            "1. Check the pre-resolved anchors in the user message first — "
            "call full_text_search only for entities not already resolved.\n"
            "2. If full_text_search returns nothing for a station or route, "
            "call entity_clarify.\n"
            "3. For aggregate metrics (counts, trip IDs, booleans), call cypher_query "
            "with the resolved IDs.\n"
            "4. For contextual neighbourhood data (alerts, equipment status, topology), "
            "call subgraph_expand.\n"
            "5. When you have sufficient data to answer the question, stop calling tools.\n"
            f"You have at most {_MAX_ITERATIONS} tool calls total. Be decisive."
        )

        return "\n\n".join(parts)

    # ── Tool dispatch ─────────────────────────────────────────────────────────

    def _dispatch_tool(
        self,
        name: str,
        tool_input: dict,
        *,
        context: AgentContext,
        planner_output: PlannerOutput,
        resolutions: AnchorResolutions,
        resolver: AnchorResolver,
        resolver_config: dict,
    ) -> tuple[str, bool]:
        """
        Route a tool_use block to the correct execute function.

        Updates context in place. Returns (result_json_str, success_bool).
        Never raises — all errors are caught and serialised as error JSON.
        The result_json_str is placed in the tool_result content field sent
        back to the model.
        """
        log.info("agent | dispatch | tool=%s", name)

        try:
            if name == "full_text_search":
                inp = FullTextSearchInput(**tool_input)
                output = execute_full_text_search(inp, resolver=resolver)
                context.tool_call_history.append((
                    name,
                    tool_input,
                    {"success": output.success, "resolved_ids": output.resolved_ids},
                ))
                return json.dumps(dataclasses.asdict(output)), output.success

            elif name == "cypher_query":
                inp = CypherQueryInput(**tool_input)
                output = execute_cypher_query(
                    inp,
                    planner_output=planner_output,
                    llm_config=self._llm_config,
                    registry=self._registry,
                    db=self._db,
                )
                context.cypher_results.append(output)
                # Truncate results for the message — full rows kept in context
                result_dict = dataclasses.asdict(output)
                if output.results:
                    result_dict["results"] = output.results[:10]
                    result_dict["result_count"] = len(output.results)
                context.tool_call_history.append((
                    name,
                    {k: v for k, v in tool_input.items() if k != "resolved_anchors"},
                    {
                        "success": output.success,
                        "attempt_count": output.attempt_count,
                        "result_count": len(output.results),
                    },
                ))
                return json.dumps(result_dict), output.success

            elif name == "subgraph_expand":
                inp = SubgraphExpandInput(**tool_input)
                output = execute_subgraph_expand(
                    inp,
                    db=self._db,
                    planner_output=planner_output,
                    base_resolutions=resolutions,
                    resolver_config=resolver_config,
                )
                context.subgraph_results.append(output)
                # Return a compact summary — full context kept in context object
                result = {
                    "success": output.success,
                    "node_count": output.node_count,
                    "trimmed": output.trimmed,
                    "failure_reason": output.failure_reason,
                    # First 2000 chars of context so the agent can see what was retrieved
                    "context_preview": output.context[:2000] if output.context else "",
                }
                context.tool_call_history.append((
                    name,
                    tool_input,
                    {"success": output.success, "node_count": output.node_count},
                ))
                return json.dumps(result), output.success

            elif name == "entity_clarify":
                inp = EntityClarifyInput(**tool_input)
                output = execute_entity_clarify(
                    inp,
                    clarifier=self._clarifier,
                    resolver=resolver,
                    base_resolutions=resolutions,
                )
                # Merge newly resolved IDs into the live resolutions so
                # subsequent tool calls in the same loop benefit from them
                if output.success:
                    if inp.anchor_type == "station":
                        resolutions.resolved_stations.update(output.resolved_ids)
                    elif inp.anchor_type == "route":
                        resolutions.resolved_routes.update(output.resolved_ids)
                context.tool_call_history.append((
                    name,
                    tool_input,
                    {
                        "success": output.success,
                        "resolved": list(output.resolved_ids.keys()),
                    },
                ))
                return json.dumps(dataclasses.asdict(output)), output.success

            else:
                msg = f"Unknown tool '{name}'"
                log.warning("agent | dispatch | %s", msg)
                context.tool_call_history.append((name, tool_input, {"error": msg}))
                return json.dumps({"error": msg}), False

        except Exception as exc:
            msg = f"Tool '{name}' raised {type(exc).__name__}: {exc}"
            log.error("agent | dispatch | %s", msg, exc_info=True)
            context.tool_call_history.append((name, tool_input, {"error": msg}))
            return json.dumps({"error": msg}), False
