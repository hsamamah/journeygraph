# src/llm/narration_agent.py
"""
NarrationAgent — terminal node of the JourneyGraph LLM pipeline.

The NarrationAgent is the only pipeline component whose output reaches the
user. It receives PlannerOutput, Text2CypherOutput (or None), and
SubgraphOutput (or None), then:

    1. Input Assembler  — selects response mode via pure logic (no LLM call)
    2. Prompt Builder   — assembles the three-section system prompt and
                          four-section user message for the selected mode
    3. LLM call         — produces the natural language answer
    4. Output assembly  — wraps the answer in NarrationOutput with trace

Response modes (selected by Input Assembler, no LLM):
    synthesis   — Both paths succeeded. Lead with facts, explain pattern.
    precision   — Text2Cypher only. Answer directly, no speculation.
    contextual  — Subgraph only. Qualify quantities, describe topology.
    degraded    — Both failed or partial. Flag what could not be determined.

System prompt structure (~240 tokens total):
    Section 1 (~100 tokens, fixed): role, no-fabrication rule,
              no pipeline self-disclosure.
    Section 2 (varies per mode):   what data is available and how to use it.
    Section 3 (varies per domain): vocabulary framing for the domain.

User message structure:
    QUERY           — original query string
    DOMAIN / MODE   — domain and selected mode
    PRECISE RESULTS — Text2Cypher results block (if available)
    GRAPH CONTEXT   — Named Projection context block (if available)

Pipeline trace is always populated in NarrationOutput.trace for the caller
to surface. It is not injected into the LLM user message — the LLM does not
need pipeline metadata to answer the question.

Usage:
    agent = NarrationAgent(llm_config)
    output = agent.run(
        query="how many trips were cancelled on the red line yesterday",
        planner_output=planner_output,
        t2c_output=t2c_output,      # None if Text2Cypher not run
        subgraph_output=subgraph_output,  # None if Subgraph not run
    )
    print(output.answer)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.common.logger import get_logger
from src.llm.llm_factory import build_llm
from src.llm.narration_output import NarrationOutput

if TYPE_CHECKING:
    from src.common.config import LLMConfig
    from src.llm.anchor_resolver import AnchorResolutions
    from src.llm.planner_output import PlannerOutput
    from src.llm.subgraph_output import SubgraphOutput
    from src.llm.text2cypher_output import Text2CypherOutput

log = get_logger(__name__)


# ── System prompt — Section 1 (fixed) ────────────────────────────────────────
# Role definition, no-fabrication rule, no pipeline self-disclosure.
# Stable across all calls — approximately 100 tokens.

_SECTION1 = """\
You are a transit analyst for the WMATA Washington DC Metro system.
Answer the user's question using only the data provided below.
Do not fabricate trip IDs, station codes, route identifiers, node IDs, \
or any other identifiers not present in the provided data.
Do not describe how you obtained the data or reference internal system \
components unless the user explicitly asks.\
### CRITICAL VERIFICATION PROTOCOL\
1. **No Numerical Drift:** Match numbers exactly.\
2. **Strict Entity Grounding:** You are STRICTLY FORBIDDEN from mentioning any station name, route ID, or equipment ID (e.g., B01_F01_119167) that does not appear in the data blocks below. \
3. **Operational Default vs. Fabrication:** - If a specific entity (like an Elevator) is listed in the results, but its 'outage' field is empty/null, state 'Operational'.\
   - If the entity is NOT listed in the results at all, do not mention it or assume its status. \
4. **No Speculative Fill:** If the user asks about 'Elevators at Gallery Place' and the data only shows 2 elevators, do not mention a 3rd one even if you 'think' it exists."""


# ── System prompt — Section 2 (varies per mode) ───────────────────────────────
# Tells the LLM what data is available and how to use it.

_SECTION2: dict[str, str] = {
    "synthesis": (
        "You have both precise query results and topological graph context. "
        "Lead with the precise facts from the query results. "
        "Then use the graph context to explain the pattern, cause, or "
        "network effect behind those facts. "
        "Every number you state must be traceable to the precise results block."
    ),
    "precision": (
        "You have precise query results only — no topological graph context "
        "is available for this query. "
        "You MUST only report the exact numbers provided in the 'PRECISE RESULTS' table."
        "If the table has 113 rows, do not say 'over 100'; say '113'."
        "Answer directly using the counts, identifiers, and structured data "
        "provided. "
        "Do not speculate about broader network effects or causes not evident "
        "in the results."
        "DO NOT perform addition or aggregation unless the 'PRECISE RESULTS' block already did it for you."
        "If the user asks for a specific date and the data is for a different date, you must state the data date clearly."
    ),
    "contextual": (
        "You are describing a network topology based on Graph Context."
        "1. Only describe nodes and relationships explicitly listed in the 'GRAPH CONTEXT' block."
        "2. If a node exists but has no properties indicating failure, report it as 'Operational'."
        "3. If a node is missing entirely from the Graph Context, you must state that no information is available for that specific entity."
        "4. DO NOT invent 'Working' statuses for entities that are not present in your data stream."
    ),
    "degraded": (
        "Limited or no graph data is available for this query. "
        "The RESOLUTION STATUS section below lists which entities were found "
        "and which could not be matched. "
        "State explicitly what was and was not resolved. "
        "If specific entities could not be found, name them and suggest how "
        "the user might rephrase (e.g. use a specific station name, route "
        "number, or date). "
        "Do not speculate about data you do not have. "
        "Do not fabricate results."
    ),
}


# ── System prompt — Section 3 (varies per domain) ────────────────────────────
# Focuses response vocabulary toward the right kind of answer per domain.

_SECTION3: dict[str, str] = {
    "transfer_impact": (
        "Focus on broken transfer opportunities: which connections were "
        "disrupted, at which stations, and for which services. "
        "If cancellation counts are available, lead with them. "
        "If transfer partner context is available, explain the downstream "
        "impact on connecting services. "
        "If the PRECISE RESULTS section contains an empty list for Skips or Interruptions, "
        "you must inform the user that service is Normal/OK. Do not claim the data is missing."
    ),
    "accessibility": (
        "Focus on pathway accessibility loss: which elevator or escalator "
        "outages are active and at which stations. "
        "If service disruption data is also present, describe whether the "
        "outages correlate with service-level disruptions at the same "
        "stations. "
        "Keep OutageEvent (WMATA Incidents API) and service Interruptions "
        "(GTFS-RT) conceptually separate — they come from different sources "
        "and share no common key. "
        "If the PRECISE RESULTS section contains an empty list for OutageEvents, "
        "you must inform the user that all accessibility pathways are Normal/OK. "
        "Do not claim the data is missing."
    ),
    "delay_propagation": (
        "Focus on delay origin and downstream spread: where the delay "
        "originated, which stops and trips are affected, and how far it has "
        "propagated across the network. "
        "If provenance data is present (TripUpdate, StopTimeUpdate), use it "
        "to explain the cause of the delay, not just its existence. "
        "Note the 5-minute (300 s) threshold — delays below this have no "
        "Interruption node in the graph. "
        "If the PRECISE RESULTS section contains an empty list for Interruptions or Delays, "
        "you must inform the user that service is Normal/OK. Do not claim the data is missing."
    ),
}


# ── NarrationAgent ────────────────────────────────────────────────────────────


class NarrationAgent:
    """
    Terminal node of the JourneyGraph LLM pipeline.

    Receives outputs from the Planner, Text2Cypher path, and Subgraph path,
    then produces the final natural language response surfaced to the user.

    The LLM instance is built once at construction with narration-specific
    max tokens (LLM_NARRATION_MAX_TOKENS) rather than the lightweight
    Planner max tokens (LLM_MAX_TOKENS).

    Args:
        llm_config: Validated LLMConfig from get_llm_config(). The agent
                    uses llm_config.llm_narration_max_tokens for its LLM
                    call — separate from the Planner's llm_max_tokens.
    """

    def __init__(self, llm_config: LLMConfig) -> None:
        self._llm_config = llm_config
        self._llm = build_llm(
            llm_config, max_tokens=llm_config.llm_narration_max_tokens
        )
        log.debug(
            "NarrationAgent ready — provider=%s model=%s narration_max_tokens=%d",
            llm_config.llm_provider,
            llm_config.llm_model,
            llm_config.llm_narration_max_tokens,
        )

    # ── Public interface ──────────────────────────────────────────────────────

    def run(
        self,
        query: str,
        planner_output: PlannerOutput,
        *,
        t2c_output: Text2CypherOutput | None,
        subgraph_output: SubgraphOutput | None,
        resolutions: AnchorResolutions | None = None,
    ) -> NarrationOutput:
        """
        Run the Narration Agent for a single query.

        Args:
            query:          Original user query string.
            planner_output: PlannerOutput from the Planner. Must not be
                            rejected (caller is responsible for this check).
            t2c_output:     Text2CypherOutput or None if the Text2Cypher
                            path was not run or is not yet implemented.
            subgraph_output: SubgraphOutput or None if the Subgraph path
                            was not run.
            resolutions:    AnchorResolutions from the resolver, if available.
                            Used in degraded mode to surface resolved and
                            failed anchor info to the LLM.

        Returns:
            NarrationOutput. Always returns — never raises on query-level
            failures. Check output.success for LLM call failures.
        """
        domain = planner_output.domain
        mode = self._select_mode(t2c_output, subgraph_output)

        log.info(
            "NarrationAgent.run | domain=%s mode=%s t2c=%s subgraph=%s",
            domain,
            mode,
            "success" if (t2c_output and t2c_output.success) else "absent/failed",
            "success"
            if (subgraph_output and subgraph_output.success)
            else "absent/failed",
        )

        system_prompt = self._build_system_prompt(mode, domain)
        user_message = self._build_user_message(
            query, domain, mode, t2c_output, subgraph_output, resolutions
        )

        try:
            answer = self._invoke_llm(system_prompt, user_message)
        except Exception as exc:
            failure = f"NarrationAgent LLM call failed [{type(exc).__name__}]: {exc}"
            log.error(
                "NarrationAgent.run | LLM call failed | %s: %s",
                type(exc).__name__,
                exc,
                exc_info=True,
            )
            return NarrationOutput(
                answer="",
                mode=mode,
                sources_used=[],
                domain=domain,
                trace=self._build_trace(
                    planner_output, t2c_output, subgraph_output, mode
                ),
                success=False,
                failure_reason=failure,
            )

        sources_used = self._sources_used(t2c_output, subgraph_output)
        trace = self._build_trace(planner_output, t2c_output, subgraph_output, mode)

        log.info(
            "NarrationAgent.run | complete | mode=%s sources=%s answer_len=%d",
            mode,
            sources_used,
            len(answer),
        )

        return NarrationOutput(
            answer=answer,
            mode=mode,
            sources_used=sources_used,
            domain=domain,
            trace=trace,
            success=True,
            failure_reason=None,
        )

    # ── Input Assembler — mode selection (pure logic, no LLM) ────────────────

    @staticmethod
    def _select_mode(
        t2c_output: Text2CypherOutput | None,
        subgraph_output: SubgraphOutput | None,
    ) -> str:
        """
        Select the response mode from path success states.

        Pure logic — no LLM call.

        Mode table:
            synthesis   — both t2c and subgraph succeeded
            precision   — t2c succeeded, subgraph absent/failed
            contextual  — subgraph succeeded, t2c absent/failed
            degraded    — neither succeeded
        """
        t2c_ok = t2c_output is not None and t2c_output.success
        sub_ok = subgraph_output is not None and subgraph_output.success

        if t2c_ok and sub_ok:
            return "synthesis"
        if t2c_ok:
            return "precision"
        if sub_ok:
            return "contextual"
        return "degraded"

    # ── Prompt builder — system prompt ───────────────────────────────────────

    @staticmethod
    def _build_system_prompt(mode: str, domain: str) -> str:
        """
        Assemble the three-section system prompt.

        Section 1: fixed role + constraints (~100 tokens)
        Section 2: mode-specific instruction
        Section 3: domain framing
        """
        section2 = _SECTION2.get(mode, _SECTION2["degraded"])
        section3 = _SECTION3.get(domain, "")

        parts = [_SECTION1, section2]
        if section3:
            parts.append(section3)

        return "\n\n".join(parts)

    # ── Prompt builder — user message ─────────────────────────────────────────

    @staticmethod
    def _build_user_message(
        query: str,
        domain: str,
        mode: str,
        t2c_output: Text2CypherOutput | None,
        subgraph_output: SubgraphOutput | None,
        resolutions: AnchorResolutions | None = None,
    ) -> str:
        """
        Assemble the user message.

        Sections:
            QUERY              — original query string
            DOMAIN / MODE      — routing metadata
            RESOLUTION STATUS  — resolved and failed anchors (degraded mode only)
            PRECISE RESULTS    — Text2Cypher results block (if available)
            GRAPH CONTEXT      — Named Projection context block (if available)
        """
        lines: list[str] = []

        # ── QUERY ─────────────────────────────────────────────────────────────
        # XML tags prevent a crafted query from colliding with section headers
        # (e.g. "PRECISE RESULTS" or "GRAPH CONTEXT" injected mid-query).
        lines.append("QUERY")
        lines.append(f"<user_query>{query}</user_query>")
        lines.append("")

        # ── DOMAIN / MODE ──────────────────────────────────────────────────────
        lines.append(f"DOMAIN: {domain} | MODE: {mode}")
        lines.append("")

        # ── RESOLUTION STATUS (degraded mode only) ────────────────────────────
        # Surfaces what the resolver found and what it couldn't match, so the
        # LLM can tell the user specifically what failed rather than giving a
        # generic "no data" response.
        # NOTE: resolutions is intentionally only consumed here in degraded mode.
        # In other modes (contextual, precision, synthesis) the subgraph or T2C
        # output already embeds resolved anchor context — surfacing it again would
        # be redundant. Any extension that surfaces resolutions in non-degraded
        # modes should be deliberate, not accidental.
        if mode == "degraded" and resolutions is not None:
            lines.append("RESOLUTION STATUS")
            flat = resolutions.as_flat_dict()
            if flat:
                for mention, node_ids in flat.items():
                    lines.append(f"  resolved: {mention!r} → {node_ids}")
            if resolutions.failed:
                for anchor, reason in resolutions.failed.items():
                    lines.append(f"  failed:   {anchor!r} — {reason}")
            if not flat and not resolutions.failed:
                lines.append("  (no anchors extracted from query)")
            lines.append("")

        # ── PRECISE RESULTS ────────────────────────────────────────────────────
        if t2c_output is not None and t2c_output.success:
            attempt_note = f"{t2c_output.attempt_count} attempt" + (
                "s" if t2c_output.attempt_count != 1 else ""
            )
            lines.append(f"PRECISE RESULTS [from Text2Cypher — {attempt_note}]")
            if t2c_output.results:
                for row in t2c_output.results:
                    for key, value in row.items():
                        lines.append(f"{key}: {value}")
            else:
                lines.append("(no rows returned)")
        else:
            lines.append("PRECISE RESULTS")
            lines.append("[Text2Cypher not available for this query]")

        lines.append("")

        # ── GRAPH CONTEXT ──────────────────────────────────────────────────────
        if subgraph_output is not None and subgraph_output.success:
            trim_note = ", trimmed to budget" if subgraph_output.trimmed else ""
            lines.append(
                f"GRAPH CONTEXT [from Subgraph — "
                f"{subgraph_output.node_count} nodes{trim_note}]"
            )
            lines.append(subgraph_output.context)
        else:
            lines.append("GRAPH CONTEXT")
            if subgraph_output is not None and subgraph_output.failure_reason:
                lines.append(
                    f"[Subgraph not available: {subgraph_output.failure_reason}]"
                )
            else:
                lines.append("[Subgraph not available for this query]")

        return "\n".join(lines)

    # ── LLM invocation ─────────────────────────────────────────────────────────

    def _invoke_llm(self, system_prompt: str, user_message: str) -> str:
        """Invoke the LLM and return the raw response string.

        Raises:
            ValueError: if the LLM returns non-string content (e.g. a tool-call
                response with no text block, where .content is [] rather than str).
                The caller's except Exception block converts this to a clean
                NarrationOutput(success=False) with a meaningful failure_reason.
        """
        response = self._llm.invoke(
            user_message,
            system_instruction=system_prompt,
        )
        content = response.content
        if not isinstance(content, str):
            raise ValueError(
                f"LLM returned non-string content type {type(content)!r} — "
                "expected a text response, got a tool-call or structured response."
            )
        return content

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _sources_used(
        t2c_output: Text2CypherOutput | None,
        subgraph_output: SubgraphOutput | None,
    ) -> list[str]:
        """Build the sources_used list from path success states."""
        sources: list[str] = []
        if t2c_output is not None and t2c_output.success:
            sources.append("text2cypher")
        if subgraph_output is not None and subgraph_output.success:
            sources.append("subgraph")
        return sources

    @staticmethod
    def _build_trace(
        planner_output: PlannerOutput,
        t2c_output: Text2CypherOutput | None,
        subgraph_output: SubgraphOutput | None,
        mode: str,
    ) -> dict:
        """
        Build the full pipeline trace dict for NarrationOutput.trace.

        Always populated — callers (run.py) print this for team review.
        """
        planner_trace = {
            "domain": planner_output.domain,
            "path": planner_output.path,
            "schema_slice_key": planner_output.schema_slice_key,
            "path_reasoning": planner_output.path_reasoning,
            "anchor_notes": planner_output.anchor_notes,
            "parse_warning": planner_output.parse_warning,
            "anchors": {
                "stations": list(planner_output.anchors.stations),
                "routes": list(planner_output.anchors.routes),
                "dates": list(planner_output.anchors.dates),
                "pathway_nodes": list(planner_output.anchors.pathway_nodes),
                "levels": list(planner_output.anchors.levels),
            },
        }

        t2c_trace: dict | None = None
        if t2c_output is not None:
            t2c_trace = {
                "success": t2c_output.success,
                "attempt_count": t2c_output.attempt_count,
                "validation_notes": t2c_output.validation_notes,
                "error": (
                    {
                        "check": t2c_output.error.check,
                        "detail": t2c_output.error.detail,
                    }
                    if t2c_output.error
                    else None
                ),
            }

        subgraph_trace: dict | None = None
        if subgraph_output is not None:
            subgraph_trace = {
                "success": subgraph_output.success,
                "node_count": subgraph_output.node_count,
                "trimmed": subgraph_output.trimmed,
                "anchor_resolutions": subgraph_output.anchor_resolutions,
                "failure_reason": subgraph_output.failure_reason,
                "provenance_node_count": len(subgraph_output.provenance_nodes),
            }

        return {
            "planner": planner_trace,
            "text2cypher": t2c_trace,
            "subgraph": subgraph_trace,
            "narration": {"mode": mode},
        }
