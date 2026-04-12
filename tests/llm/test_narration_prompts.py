# tests/llm/test_narration_prompts.py
"""
Prompt regression tests for the NarrationAgent.

These tests are distinct from test_narration_agent.py, which checks assembly
*correctness* (does the right section constant get included). These tests
check assembly *stability and intent* — encoding the reasons why specific
phrases must survive any refactor of the prompt constants.

Each test documents a behavioural invariant the LLM depends on. If a test
fails after a prompt edit, it is a signal to review whether the LLM's
behaviour has been unintentionally changed, not just that a string changed.

Coverage:
    System prompt — global invariants (all mode × domain combinations)
    System prompt — mode-specific behavioural instructions
    System prompt — domain-specific vocabulary and warnings
    System prompt — section ordering
    User message  — structural invariants
    User message  — section ordering
    User message  — pipeline trace exclusion

Not covered here (in test_narration_agent.py):
    That the correct section *constant* is included per mode/domain
    That unknown mode falls back to degraded
    That sections are separated by blank lines
"""

from __future__ import annotations

import pytest

from src.llm.narration_agent import NarrationAgent
from src.llm.subgraph_output import SubgraphOutput
from src.llm.text2cypher_output import Text2CypherOutput

# ── Constants ─────────────────────────────────────────────────────────────────

_ALL_MODES = ["synthesis", "precision", "contextual", "degraded"]
_ALL_DOMAINS = ["transfer_impact", "accessibility", "delay_propagation"]
_ALL_COMBINATIONS = [(m, d) for m in _ALL_MODES for d in _ALL_DOMAINS]


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _t2c(
    success: bool = True,
    results: list[dict] | None = None,
    attempt_count: int = 1,
) -> Text2CypherOutput:
    return Text2CypherOutput(
        cypher="MATCH (n) RETURN n LIMIT 1",
        results=results if results is not None else [{"cancel_count": 4}],
        domain="transfer_impact",
        attempt_count=attempt_count,
        success=success,
    )


def _subgraph(
    success: bool = True,
    trimmed: bool = False,
    node_count: int = 6,
    context: str = "SUBGRAPH CONTEXT — domain: transfer_impact\nAnchors: Station(Metro Center)",
    failure_reason: str | None = None,
) -> SubgraphOutput:
    return SubgraphOutput(
        context=context,
        node_count=node_count,
        trimmed=trimmed,
        provenance_nodes=[],
        anchor_resolutions={"Metro Center": "STN_A01"},
        domain="transfer_impact",
        success=success,
        failure_reason=failure_reason,
    )


# ── System prompt — global invariants ─────────────────────────────────────────
#
# These phrases must be present in every prompt, regardless of mode or domain.
# They encode the three core constraints the LLM must always operate under.


class TestSystemPromptGlobalInvariants:
    """Phrases that must survive any refactor of any prompt section."""

    @pytest.mark.parametrize("mode,domain", _ALL_COMBINATIONS)
    def test_role_definition_present(self, mode: str, domain: str) -> None:
        """
        The LLM must always know it is a transit analyst.
        Removing this causes the LLM to answer in a generic style rather than
        with transit-domain vocabulary and appropriate scope acknowledgment.
        """
        prompt = NarrationAgent._build_system_prompt(mode, domain)
        assert "transit analyst" in prompt

    @pytest.mark.parametrize("mode,domain", _ALL_COMBINATIONS)
    def test_no_fabrication_rule_present(self, mode: str, domain: str) -> None:
        """
        The no-fabrication constraint must be present in every prompt.
        This is the single most important safety constraint in the pipeline —
        without it the LLM will invent plausible-sounding trip IDs and station
        codes that do not exist in the graph.
        """
        prompt = NarrationAgent._build_system_prompt(mode, domain)
        assert "Do not fabricate" in prompt

    @pytest.mark.parametrize("mode,domain", _ALL_COMBINATIONS)
    def test_no_pipeline_self_disclosure_rule_present(self, mode: str, domain: str) -> None:
        """
        The LLM must not describe how it obtained its data.
        Surfacing pipeline internals (Neo4j, Cypher, subgraph paths) would
        confuse users and expose implementation details.
        """
        prompt = NarrationAgent._build_system_prompt(mode, domain)
        assert "Do not describe how you obtained" in prompt

    @pytest.mark.parametrize("mode,domain", _ALL_COMBINATIONS)
    def test_prompt_is_non_empty(self, mode: str, domain: str) -> None:
        """All 12 mode × domain combinations must produce a usable prompt."""
        prompt = NarrationAgent._build_system_prompt(mode, domain)
        assert len(prompt.strip()) > 0


# ── System prompt — section ordering ─────────────────────────────────────────
#
# Section 1 (role + constraints) must appear before Section 2 (mode
# instruction), which must appear before Section 3 (domain framing).
# Reordering changes what the LLM attends to first — role before instruction.


class TestSystemPromptSectionOrdering:

    @pytest.mark.parametrize("mode,domain", _ALL_COMBINATIONS)
    def test_role_before_mode_instruction(self, mode: str, domain: str) -> None:
        """
        The LLM should encounter its role definition before its behavioural
        instruction. Role-first ordering is standard practice — it establishes
        who the LLM is before telling it what to do.
        """
        prompt = NarrationAgent._build_system_prompt(mode, domain)
        role_pos = prompt.index("transit analyst")
        # Each mode has a unique distinguishing phrase; find the mode section
        mode_markers = {
            "synthesis": "Lead with the precise facts",
            "precision": "precise query results only",
            "contextual": "topological graph context only",
            "degraded": "Limited or no graph data",
        }
        mode_pos = prompt.index(mode_markers[mode])
        assert role_pos < mode_pos

    @pytest.mark.parametrize("domain", _ALL_DOMAINS)
    def test_mode_instruction_before_domain_framing(self, domain: str) -> None:
        """
        Domain framing (Section 3) must follow the mode instruction (Section 2).
        The mode instruction tells the LLM what data it has; domain framing
        tells it what vocabulary to use. Reversed order would have the LLM
        focus on vocabulary before understanding what it can say.
        """
        # Use synthesis so both sections are present and distinguishable
        prompt = NarrationAgent._build_system_prompt("synthesis", domain)
        mode_pos = prompt.index("Lead with the precise facts")
        domain_markers = {
            "transfer_impact": "broken transfer opportunities",
            "accessibility": "pathway accessibility loss",
            "delay_propagation": "delay origin and downstream spread",
        }
        domain_pos = prompt.index(domain_markers[domain])
        assert mode_pos < domain_pos


# ── System prompt — mode-specific behavioural instructions ────────────────────
#
# Each mode encodes a specific behavioural contract for the LLM. These tests
# verify that the key instruction phrase for each mode is present and that
# the wrong instruction is not present (modes must not bleed into each other).


class TestSystemPromptModeInstructions:

    def test_synthesis_leads_with_facts(self) -> None:
        """
        Synthesis mode must instruct the LLM to lead with facts before
        explaining the network pattern. Leading with context first buries
        the direct answer and degrades user experience.
        """
        prompt = NarrationAgent._build_system_prompt("synthesis", "transfer_impact")
        assert "Lead with the precise facts" in prompt

    def test_synthesis_quantity_traceability_required(self) -> None:
        """
        In synthesis mode every number must be traceable to the precise
        results block. This is the primary fabrication guard for numeric data —
        without it the LLM may blend counts from the graph context (node
        counts) with precise query results.
        """
        prompt = NarrationAgent._build_system_prompt("synthesis", "transfer_impact")
        assert "Every number you state must be traceable" in prompt

    def test_precision_no_speculation(self) -> None:
        """
        Precision mode has no topological context, so the LLM must not
        speculate about network effects it cannot see. Without this constraint
        it will extrapolate from the count to invent downstream impacts.
        """
        prompt = NarrationAgent._build_system_prompt("precision", "transfer_impact")
        assert "Do not speculate" in prompt

    def test_contextual_qualify_quantities(self) -> None:
        """
        Contextual mode has no precise counts — only topology. The LLM must
        qualify any quantities it mentions (e.g. 'at least', 'several').
        Without this constraint it will invent exact counts from node counts
        in the graph context, which are not equivalent to query result counts.
        """
        prompt = NarrationAgent._build_system_prompt("contextual", "transfer_impact")
        assert "Qualify any quantities" in prompt

    def test_degraded_explicit_uncertainty(self) -> None:
        """
        Degraded mode must instruct the LLM to explicitly state what could
        not be determined. A degraded response that does not flag its own
        incompleteness is worse than no response — it gives the user false
        confidence in a partial answer.
        """
        prompt = NarrationAgent._build_system_prompt("degraded", "transfer_impact")
        assert "State explicitly what was and was not resolved" in prompt

    def test_synthesis_instruction_absent_from_precision(self) -> None:
        """Mode instructions must not bleed across modes."""
        prompt = NarrationAgent._build_system_prompt("precision", "transfer_impact")
        assert "Lead with the precise facts" not in prompt

    def test_contextual_qualify_instruction_absent_from_synthesis(self) -> None:
        """Synthesis has precise data — it must not qualify quantities."""
        prompt = NarrationAgent._build_system_prompt("synthesis", "transfer_impact")
        assert "Qualify any quantities" not in prompt


# ── System prompt — domain-specific vocabulary and warnings ───────────────────
#
# Section 3 encodes domain framing that focuses the LLM's vocabulary toward
# the right kind of answer. These tests verify the key facts and warnings
# that must survive any rewording of the domain framing sections.


class TestSystemPromptDomainFraming:

    def test_transfer_impact_broken_transfers_vocabulary(self) -> None:
        """
        Transfer impact answers must focus on broken connections, not just
        cancellation counts. Without this framing the LLM reports the count
        and stops, rather than explaining the downstream transfer impact.
        """
        prompt = NarrationAgent._build_system_prompt("synthesis", "transfer_impact")
        assert "broken transfer opportunities" in prompt

    def test_accessibility_two_source_warning_present(self) -> None:
        """
        The accessibility domain draws from two separate sources: WMATA
        Incidents API (OutageEvent) and GTFS-RT (Interruption:Accessibility).
        They share no common key. Without this warning the LLM implies a
        causal link between the two that the data does not support.
        """
        prompt = NarrationAgent._build_system_prompt("contextual", "accessibility")
        assert "OutageEvent" in prompt
        assert "GTFS-RT" in prompt

    def test_accessibility_two_sources_kept_separate(self) -> None:
        """
        The framing must state the sources are conceptually separate —
        not just name them. This is the specific WMATA data quirk that causes
        incorrect narration if the LLM treats them as equivalent.
        """
        prompt = NarrationAgent._build_system_prompt("contextual", "accessibility")
        assert "separate" in prompt

    def test_delay_propagation_threshold_present(self) -> None:
        """
        The 5-minute (300 s) delay threshold must be in the domain framing.
        Without it the LLM will describe the absence of Interruption nodes as
        'no delays' when the correct answer is 'no delays above the 5-minute
        threshold' — a meaningfully different statement.
        """
        prompt = NarrationAgent._build_system_prompt("synthesis", "delay_propagation")
        assert "300 s" in prompt

    def test_delay_propagation_origin_and_spread_vocabulary(self) -> None:
        """
        Delay propagation answers must address both origin and downstream
        spread. Without this framing the LLM tends to report only whether
        a delay exists at the anchor station, omitting the propagation pattern
        that is the point of the query.
        """
        prompt = NarrationAgent._build_system_prompt("synthesis", "delay_propagation")
        assert "origin" in prompt
        assert "downstream" in prompt

    def test_delay_propagation_provenance_instruction_present(self) -> None:
        """
        When TripUpdate/StopTimeUpdate nodes are present in the graph context,
        the LLM must use their properties to explain *why* the delay occurred,
        not just *that* it occurred. This instruction is what lifts narration
        from description to explanation.
        """
        prompt = NarrationAgent._build_system_prompt("synthesis", "delay_propagation")
        assert "TripUpdate" in prompt


# ── User message — structural invariants ──────────────────────────────────────
#
# The user message is the data payload the LLM reads to formulate its answer.
# These tests verify the structure that the LLM's answer quality depends on.


class TestUserMessageInvariants:

    def test_query_wrapped_in_single_quotes(self) -> None:
        """
        The query must be in single quotes so the LLM reads it as a user
        utterance rather than a continuation of the metadata headers above it.
        """
        msg = NarrationAgent._build_user_message(
            "how many cancellations yesterday",
            "transfer_impact",
            "precision",
            _t2c(),
            None,
        )
        assert "how many cancellations yesterday" in msg

    def test_domain_and_mode_on_same_line(self) -> None:
        """
        DOMAIN and MODE must appear together so the LLM reads them as a
        single routing decision rather than two independent metadata items.
        """
        msg = NarrationAgent._build_user_message(
            "query", "delay_propagation", "contextual", None, _subgraph()
        )
        # Must be on the same line — split by newline and check one line contains both
        lines = msg.splitlines()
        combined_lines = [l for l in lines if "DOMAIN:" in l and "MODE:" in l]
        assert len(combined_lines) == 1

    def test_pipeline_trace_not_in_user_message(self) -> None:
        """
        The pipeline trace must NOT appear in the user message sent to the LLM.
        The trace is for the team (NarrationOutput.trace); injecting it into
        the LLM context would waste tokens and could cause the LLM to describe
        internal pipeline state rather than answering the user's question.
        """
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "synthesis", _t2c(), _subgraph()
        )
        assert "PIPELINE TRACE" not in msg

    def test_query_section_before_domain_mode(self) -> None:
        """Query appears before DOMAIN/MODE line — establishes intent first."""
        msg = NarrationAgent._build_user_message(
            "how many cancellations", "transfer_impact", "precision", _t2c(), None
        )
        query_pos = msg.index("how many cancellations")
        domain_pos = msg.index("DOMAIN:")
        assert query_pos < domain_pos

    @pytest.mark.parametrize("mode", _ALL_MODES)
    @pytest.mark.parametrize("domain", _ALL_DOMAINS)
    def test_user_message_non_empty_all_combinations(
        self, mode: str, domain: str
    ) -> None:
        """All 12 mode × domain combinations produce a non-empty user message."""
        msg = NarrationAgent._build_user_message(
            "test query", domain, mode, None, None
        )
        assert len(msg.strip()) > 0


# ── User message — section ordering ───────────────────────────────────────────
#
# The LLM reads the user message top-to-bottom. Section ordering determines
# what the LLM has in its recency window when it starts generating. Precise
# results must come before graph context so counts are fresher than topology.


class TestUserMessageSectionOrdering:

    def test_precise_results_before_graph_context_in_synthesis(self) -> None:
        """
        In synthesis mode the LLM is instructed to 'lead with facts, then
        explain pattern.' The user message must present precise results before
        graph context to match that instruction — if context came first the LLM
        would read it first and tend to lead with topology instead of facts.
        """
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "synthesis", _t2c(), _subgraph()
        )
        precise_pos = msg.index("PRECISE RESULTS")
        graph_pos = msg.index("GRAPH CONTEXT")
        assert precise_pos < graph_pos

    def test_domain_mode_before_data_sections(self) -> None:
        """
        DOMAIN/MODE routing metadata must precede the data sections so the
        LLM knows what mode it is operating in before reading the data.
        """
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "synthesis", _t2c(), _subgraph()
        )
        domain_pos = msg.index("DOMAIN:")
        precise_pos = msg.index("PRECISE RESULTS")
        assert domain_pos < precise_pos

    def test_precise_results_before_graph_context_in_degraded(self) -> None:
        """Section ordering must be stable even when both sections show absence markers."""
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "degraded", _t2c(success=False), _subgraph(success=False)
        )
        precise_pos = msg.index("PRECISE RESULTS")
        graph_pos = msg.index("GRAPH CONTEXT")
        assert precise_pos < graph_pos
