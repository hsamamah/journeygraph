# src/llm/narration_output.py
"""
NarrationOutput — output contract for the Narration Agent.

NarrationOutput is the terminal dataclass of the LLM pipeline. It carries
the final natural language answer surfaced to the user, the mode that
produced it, an audit record of which data sources were used, and the full
pipeline trace for team review.

Design notes:
    sources_used is retained even in degraded mode so the audit record is
    always complete regardless of whether verbose output is enabled.

    trace is always populated (run.py is always verbose). It carries the
    full pipeline state at narration time so the caller can print a
    structured trace without re-querying any pipeline components.

    success=False signals that the NarrationAgent LLM call itself failed
    (network error, empty response). It does not indicate that the upstream
    paths failed — those failures are encoded in the mode field (degraded).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class NarrationOutput:
    """
    Terminal output from the Narration Agent.

    Attributes:
        answer:       Natural language response to the user query.
                      Empty string when success=False.
        mode:         Response mode selected by the Input Assembler.
                      Values: synthesis | precision | contextual | degraded
        sources_used: Data sources that contributed to the answer.
                      Values: subset of ['text2cypher', 'subgraph'].
                      Empty list in degraded mode with no data.
        domain:       Query domain — mirrors PlannerOutput.domain.
        trace:        Full pipeline state at narration time. Always populated.
                      Keys: planner, text2cypher, subgraph, narration.
        success:      False only if the Narration Agent LLM call itself
                      failed. Degraded mode with partial data is still
                      success=True — the agent produced an answer.
        failure_reason: Describes the LLM call failure when success=False.
                      None on success.
    """

    answer: str
    mode: str
    sources_used: list[str]
    domain: str
    trace: dict = field(default_factory=dict)
    success: bool = True
    failure_reason: str | None = None
    agent_trace: dict | None = None
    # Populated by AgentOrchestrator after the tool loop; None in static mode.
    # Carries tool_call_history and retrieval summary for A/B eval comparison.
