# tests/llm/test_narration_agent.py
"""
Unit tests for the NarrationAgent — pure Python assembly logic only.

Coverage:
    Mode selection    — _select_mode (all four modes)
    System prompt     — _build_system_prompt (section assembly per mode/domain)
    User message      — _build_user_message (section presence and content)
    Pipeline trace    — _build_trace (field mapping)
    Sources used      — _sources_used
    NarrationOutput   — field population on success and LLM failure

Not covered here (require live LLM):
    NarrationAgent.run() end-to-end — needs LLM mock or live API key
    LLM invoke() call — tested via integration tests

All static methods are tested directly without instantiating NarrationAgent,
mirroring the pattern in test_planner.py for _classify and _parse_json_response.
"""

import pytest

from src.llm.narration_agent import (
    NarrationAgent,
    _SECTION1,
    _SECTION2,
    _SECTION3,
)
from src.llm.narration_output import NarrationOutput
from src.llm.planner_output import PlannerAnchors, PlannerOutput
from src.llm.subgraph_output import SubgraphOutput
from src.llm.text2cypher_output import Text2CypherOutput, ValidationError


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_planner_output(
    domain: str = "transfer_impact",
    path: str = "both",
    path_reasoning: str | None = "Query asks for a count — text2cypher",
    anchor_notes: str | None = None,
    parse_warning: str | None = None,
) -> PlannerOutput:
    return PlannerOutput(
        domain=domain,
        path=path,
        anchors=PlannerAnchors(
            stations=["Metro Center"],
            routes=["Red Line"],
            dates=["20260315"],
            pathway_nodes=[],
        ),
        schema_slice_key=domain,
        rejected=False,
        rejection_message=None,
        path_reasoning=path_reasoning,
        anchor_notes=anchor_notes,
        parse_warning=parse_warning,
    )


def _make_t2c_output(
    success: bool = True,
    results: list[dict] | None = None,
    attempt_count: int = 1,
    validation_notes: list[str] | None = None,
    error: ValidationError | None = None,
) -> Text2CypherOutput:
    return Text2CypherOutput(
        cypher="MATCH (n) RETURN n LIMIT 1",
        results=results if results is not None else [{"cancel_count": 4}],
        domain="transfer_impact",
        attempt_count=attempt_count,
        validation_notes=validation_notes or [],
        success=success,
        error=error,
    )


def _make_subgraph_output(
    success: bool = True,
    node_count: int = 6,
    trimmed: bool = False,
    context: str = "SUBGRAPH CONTEXT — domain: transfer_impact\nAnchors: Station(Metro Center)",
    failure_reason: str | None = None,
) -> SubgraphOutput:
    return SubgraphOutput(
        context=context,
        node_count=node_count,
        trimmed=trimmed,
        provenance_nodes=[{"source": "gtfs_rt_rail"}],
        anchor_resolutions={"Metro Center": "STN_A01"},
        domain="transfer_impact",
        success=success,
        failure_reason=failure_reason,
    )


# ── Mode selection ────────────────────────────────────────────────────────────


class TestSelectMode:

    def test_synthesis_both_succeeded(self) -> None:
        mode = NarrationAgent._select_mode(
            _make_t2c_output(success=True),
            _make_subgraph_output(success=True),
        )
        assert mode == "synthesis"

    def test_precision_t2c_only(self) -> None:
        mode = NarrationAgent._select_mode(
            _make_t2c_output(success=True),
            _make_subgraph_output(success=False),
        )
        assert mode == "precision"

    def test_precision_subgraph_none(self) -> None:
        mode = NarrationAgent._select_mode(
            _make_t2c_output(success=True),
            None,
        )
        assert mode == "precision"

    def test_contextual_subgraph_only(self) -> None:
        mode = NarrationAgent._select_mode(
            _make_t2c_output(success=False),
            _make_subgraph_output(success=True),
        )
        assert mode == "contextual"

    def test_contextual_t2c_none(self) -> None:
        mode = NarrationAgent._select_mode(
            None,
            _make_subgraph_output(success=True),
        )
        assert mode == "contextual"

    def test_degraded_both_failed(self) -> None:
        mode = NarrationAgent._select_mode(
            _make_t2c_output(success=False),
            _make_subgraph_output(success=False),
        )
        assert mode == "degraded"

    def test_degraded_both_none(self) -> None:
        mode = NarrationAgent._select_mode(None, None)
        assert mode == "degraded"

    def test_degraded_t2c_none_subgraph_failed(self) -> None:
        mode = NarrationAgent._select_mode(
            None,
            _make_subgraph_output(success=False, failure_reason="No anchors resolved"),
        )
        assert mode == "degraded"


# ── System prompt assembly ────────────────────────────────────────────────────


class TestBuildSystemPrompt:

    def test_section1_always_present(self) -> None:
        prompt = NarrationAgent._build_system_prompt("synthesis", "transfer_impact")
        assert _SECTION1 in prompt

    def test_synthesis_section2(self) -> None:
        prompt = NarrationAgent._build_system_prompt("synthesis", "transfer_impact")
        assert _SECTION2["synthesis"] in prompt

    def test_precision_section2(self) -> None:
        prompt = NarrationAgent._build_system_prompt("precision", "accessibility")
        assert _SECTION2["precision"] in prompt

    def test_contextual_section2(self) -> None:
        prompt = NarrationAgent._build_system_prompt("contextual", "delay_propagation")
        assert _SECTION2["contextual"] in prompt

    def test_degraded_section2(self) -> None:
        prompt = NarrationAgent._build_system_prompt("degraded", "transfer_impact")
        assert _SECTION2["degraded"] in prompt

    def test_transfer_impact_section3(self) -> None:
        prompt = NarrationAgent._build_system_prompt("synthesis", "transfer_impact")
        assert _SECTION3["transfer_impact"] in prompt

    def test_accessibility_section3(self) -> None:
        prompt = NarrationAgent._build_system_prompt("contextual", "accessibility")
        assert _SECTION3["accessibility"] in prompt

    def test_delay_propagation_section3(self) -> None:
        prompt = NarrationAgent._build_system_prompt("precision", "delay_propagation")
        assert _SECTION3["delay_propagation"] in prompt

    def test_unknown_domain_no_section3(self) -> None:
        # Unknown domain should not raise — section 3 is omitted
        prompt = NarrationAgent._build_system_prompt("degraded", "unknown_domain")
        assert _SECTION1 in prompt
        assert _SECTION2["degraded"] in prompt

    def test_sections_separated_by_blank_line(self) -> None:
        prompt = NarrationAgent._build_system_prompt("synthesis", "transfer_impact")
        # Sections are joined by "\n\n"
        assert "\n\n" in prompt

    def test_unknown_mode_falls_back_to_degraded_section2(self) -> None:
        # Unknown mode should degrade gracefully
        prompt = NarrationAgent._build_system_prompt("nonexistent_mode", "transfer_impact")
        assert _SECTION2["degraded"] in prompt


# ── User message assembly ─────────────────────────────────────────────────────


class TestBuildUserMessage:

    def test_query_appears_in_message(self) -> None:
        msg = NarrationAgent._build_user_message(
            "how many trips were cancelled",
            "transfer_impact",
            "synthesis",
            _make_t2c_output(),
            _make_subgraph_output(),
        )
        assert "how many trips were cancelled" in msg

    def test_domain_and_mode_appear(self) -> None:
        msg = NarrationAgent._build_user_message(
            "query",
            "delay_propagation",
            "contextual",
            None,
            _make_subgraph_output(),
        )
        assert "DOMAIN: delay_propagation" in msg
        assert "MODE: contextual" in msg

    def test_precise_results_present_when_t2c_succeeded(self) -> None:
        t2c = _make_t2c_output(success=True, results=[{"cancel_count": 4}])
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "synthesis", t2c, None
        )
        assert "PRECISE RESULTS" in msg
        assert "cancel_count: 4" in msg

    def test_precise_results_absent_label_when_t2c_none(self) -> None:
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "contextual", None, _make_subgraph_output()
        )
        assert "[Text2Cypher not available" in msg

    def test_precise_results_absent_label_when_t2c_failed(self) -> None:
        t2c = _make_t2c_output(success=False)
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "degraded", t2c, None
        )
        assert "[Text2Cypher not available" in msg

    def test_graph_context_present_when_subgraph_succeeded(self) -> None:
        sub = _make_subgraph_output(success=True, context="SUBGRAPH CONTEXT — domain: transfer_impact")
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "contextual", None, sub
        )
        assert "GRAPH CONTEXT" in msg
        assert "SUBGRAPH CONTEXT — domain: transfer_impact" in msg

    def test_graph_context_absent_label_when_subgraph_none(self) -> None:
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "precision", _make_t2c_output(), None
        )
        assert "[Subgraph not available" in msg

    def test_graph_context_includes_failure_reason(self) -> None:
        sub = _make_subgraph_output(
            success=False, failure_reason="No anchors resolved from query"
        )
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "degraded", None, sub
        )
        assert "No anchors resolved from query" in msg

    def test_t2c_attempt_count_singular(self) -> None:
        t2c = _make_t2c_output(success=True, attempt_count=1)
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "precision", t2c, None
        )
        assert "1 attempt]" in msg

    def test_t2c_attempt_count_plural(self) -> None:
        t2c = _make_t2c_output(success=True, attempt_count=3)
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "synthesis", t2c, _make_subgraph_output()
        )
        assert "3 attempts]" in msg

    def test_subgraph_trimmed_note_included(self) -> None:
        sub = _make_subgraph_output(success=True, trimmed=True, node_count=200)
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "synthesis", _make_t2c_output(), sub
        )
        assert "trimmed to budget" in msg

    def test_subgraph_not_trimmed_no_note(self) -> None:
        sub = _make_subgraph_output(success=True, trimmed=False, node_count=10)
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "synthesis", _make_t2c_output(), sub
        )
        assert "trimmed to budget" not in msg

    def test_empty_t2c_results_shows_no_rows(self) -> None:
        t2c = _make_t2c_output(success=True, results=[])
        msg = NarrationAgent._build_user_message(
            "query", "transfer_impact", "precision", t2c, None
        )
        assert "(no rows returned)" in msg


# ── Sources used ──────────────────────────────────────────────────────────────


class TestSourcesUsed:

    def test_both_sources(self) -> None:
        sources = NarrationAgent._sources_used(
            _make_t2c_output(success=True),
            _make_subgraph_output(success=True),
        )
        assert sources == ["text2cypher", "subgraph"]

    def test_t2c_only(self) -> None:
        sources = NarrationAgent._sources_used(
            _make_t2c_output(success=True),
            _make_subgraph_output(success=False),
        )
        assert sources == ["text2cypher"]

    def test_subgraph_only(self) -> None:
        sources = NarrationAgent._sources_used(
            None,
            _make_subgraph_output(success=True),
        )
        assert sources == ["subgraph"]

    def test_no_sources(self) -> None:
        sources = NarrationAgent._sources_used(None, None)
        assert sources == []


# ── Pipeline trace ────────────────────────────────────────────────────────────


class TestBuildTrace:

    def test_planner_fields_present(self) -> None:
        planner = _make_planner_output(domain="accessibility", path_reasoning="topology query")
        trace = NarrationAgent._build_trace(planner, None, None, "degraded")
        assert trace["planner"]["domain"] == "accessibility"
        assert trace["planner"]["path_reasoning"] == "topology query"
        assert trace["planner"]["anchors"]["stations"] == ["Metro Center"]

    def test_t2c_trace_populated(self) -> None:
        t2c = _make_t2c_output(
            success=True,
            attempt_count=2,
            validation_notes=["label_whitelist: :Foo not in slice"],
        )
        trace = NarrationAgent._build_trace(_make_planner_output(), t2c, None, "precision")
        assert trace["text2cypher"]["success"] is True
        assert trace["text2cypher"]["attempt_count"] == 2
        assert len(trace["text2cypher"]["validation_notes"]) == 1

    def test_t2c_trace_none_when_not_run(self) -> None:
        trace = NarrationAgent._build_trace(_make_planner_output(), None, None, "degraded")
        assert trace["text2cypher"] is None

    def test_t2c_error_serialized(self) -> None:
        error = ValidationError(
            check="label_whitelist",
            detail="Label :Foo not in slice",
            violated_rule=":Foo",
            cypher_excerpt="MATCH (n:Foo)",
        )
        t2c = _make_t2c_output(success=False, error=error)
        trace = NarrationAgent._build_trace(_make_planner_output(), t2c, None, "degraded")
        assert trace["text2cypher"]["error"]["check"] == "label_whitelist"
        assert trace["text2cypher"]["error"]["detail"] == "Label :Foo not in slice"

    def test_subgraph_trace_populated(self) -> None:
        sub = _make_subgraph_output(success=True, node_count=12, trimmed=True)
        trace = NarrationAgent._build_trace(_make_planner_output(), None, sub, "contextual")
        assert trace["subgraph"]["success"] is True
        assert trace["subgraph"]["node_count"] == 12
        assert trace["subgraph"]["trimmed"] is True
        assert trace["subgraph"]["anchor_resolutions"] == {"Metro Center": "STN_A01"}

    def test_subgraph_trace_none_when_not_run(self) -> None:
        trace = NarrationAgent._build_trace(_make_planner_output(), None, None, "degraded")
        assert trace["subgraph"] is None

    def test_narration_mode_in_trace(self) -> None:
        trace = NarrationAgent._build_trace(_make_planner_output(), None, None, "synthesis")
        assert trace["narration"]["mode"] == "synthesis"

    def test_provenance_node_count_in_subgraph_trace(self) -> None:
        sub = _make_subgraph_output(success=True)
        trace = NarrationAgent._build_trace(_make_planner_output(), None, sub, "contextual")
        assert trace["subgraph"]["provenance_node_count"] == 1

    def test_parse_warning_in_planner_trace(self) -> None:
        planner = _make_planner_output(parse_warning="Stage 2 JSON parse failed after retry")
        trace = NarrationAgent._build_trace(planner, None, None, "degraded")
        assert trace["planner"]["parse_warning"] == "Stage 2 JSON parse failed after retry"


# ── NarrationOutput dataclass ─────────────────────────────────────────────────


class TestNarrationOutput:

    def test_defaults(self) -> None:
        output = NarrationOutput(
            answer="Four trips were cancelled.",
            mode="precision",
            sources_used=["text2cypher"],
            domain="transfer_impact",
        )
        assert output.success is True
        assert output.failure_reason is None
        assert output.trace == {}

    def test_failure_state(self) -> None:
        output = NarrationOutput(
            answer="",
            mode="contextual",
            sources_used=[],
            domain="accessibility",
            success=False,
            failure_reason="LLM call timed out",
        )
        assert output.success is False
        assert output.answer == ""
        assert output.failure_reason == "LLM call timed out"
