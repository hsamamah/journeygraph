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
"""


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
        "Answer directly using the counts, identifiers, and structured data "
        "provided. "
        "Do not speculate about broader network effects or causes not evident "
        "in the results."
    ),
    "contextual": (
        "You have topological graph context only — no precise counts or query "
        "results are available for this query. "
        "Describe the structural relationships and patterns visible in the "
        "graph context. "
        "Qualify any quantities you mention (e.g. 'at least', 'several') — "
        "do not state exact counts unless they appear explicitly in the "
        "graph context."
    ),
    "degraded": (
        "Limited or no data is available for this query. "
        "Present only what could be determined from the partial data provided. "
        "Explicitly state what could not be determined and why, so the user "
        "understands the scope of the answer."
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
        "impact on connecting services."
    ),
    "accessibility": (
        "Focus on pathway accessibility loss: which elevator or escalator "
        "outages are active and at which stations. "
        "If service disruption data is also present, describe whether the "
        "outages correlate with service-level disruptions at the same "
        "stations. "
        "Keep OutageEvent (WMATA Incidents API) and service Interruptions "
        "(GTFS-RT) conceptually separate — they come from different sources "
        "and share no common key."
    ),
    "delay_propagation": (
        "Focus on delay origin and downstream spread: where the delay "
        "originated, which stops and trips are affected, and how far it has "
        "propagated across the network. "
        "If provenance data is present (TripUpdate, StopTimeUpdate), use it "
        "to explain the cause of the delay, not just its existence. "
        "Note the 5-minute (300 s) threshold — delays below this have no "
        "Interruption node in the graph."
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
        self._llm = build_llm(llm_config, max_tokens=llm_config.llm_narration_max_tokens)
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
            "success" if (subgraph_output and subgraph_output.success) else "absent/failed",
        )

        system_prompt = self._build_system_prompt(mode, domain)
        user_message = self._build_user_message(
            query, domain, mode, t2c_output, subgraph_output
        )

        try:
            answer = self._invoke_llm(system_prompt, user_message)
        except Exception as exc:  # noqa: BLE001
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
                trace=self._build_trace(planner_output, t2c_output, subgraph_output, mode),
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
    ) -> str:
        """
        Assemble the four-section user message.

        Sections:
            QUERY           — original query string
            DOMAIN / MODE   — routing metadata
            PRECISE RESULTS — Text2Cypher results block (if available)
            GRAPH CONTEXT   — Named Projection context block (if available)
        """
        lines: list[str] = []

        # ── QUERY ─────────────────────────────────────────────────────────────
        lines.append("QUERY")
        lines.append(f"'{query}'")
        lines.append("")

        # ── DOMAIN / MODE ──────────────────────────────────────────────────────
        lines.append(f"DOMAIN: {domain} | MODE: {mode}")
        lines.append("")

        # ── PRECISE RESULTS ────────────────────────────────────────────────────
        if t2c_output is not None and t2c_output.success:
            attempt_note = (
                f"{t2c_output.attempt_count} attempt"
                + ("s" if t2c_output.attempt_count != 1 else "")
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
                lines.append(f"[Subgraph not available: {subgraph_output.failure_reason}]")
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
                "stations": planner_output.anchors.stations,
                "routes": planner_output.anchors.routes,
                "dates": planner_output.anchors.dates,
                "pathway_nodes": planner_output.anchors.pathway_nodes,
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
