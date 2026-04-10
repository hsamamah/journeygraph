# tests/test_planner.py
"""
Unit tests for the LLM Planner — pure Python components only.

Coverage:
    JSON parsing — _parse_json_response
    Anchor extraction — _extract_anchors
    PlannerAnchors — is_empty()

Not covered here (require live DB or LLM):
    SliceRegistry — needs Neo4j connection
    Planner.run() end-to-end — needs both
    Domain routing — delegated to the LLM in Stage 1 (tested via test_gds.py mocks)
"""

import pytest

from src.llm.planner import (
    _extract_anchors,
    _parse_json_response,
)
from src.llm.planner_output import PlannerAnchors


# ── JSON parsing ──────────────────────────────────────────────────────────────

class TestParseJsonResponse:

    def test_clean_json(self):
        raw = '{"path": "text2cypher", "anchors": {}}'
        parsed, err = _parse_json_response(raw)
        assert parsed == {"path": "text2cypher", "anchors": {}}
        assert err is None

    def test_fenced_json_with_language_tag(self):
        raw = '```json\n{"path": "subgraph"}\n```'
        parsed, err = _parse_json_response(raw)
        assert parsed == {"path": "subgraph"}
        assert err is None

    def test_fenced_json_without_language_tag(self):
        raw = '```\n{"path": "both"}\n```'
        parsed, err = _parse_json_response(raw)
        assert parsed == {"path": "both"}
        assert err is None

    def test_whitespace_padded_json(self):
        raw = '  {"path": "text2cypher"}  '
        parsed, err = _parse_json_response(raw)
        assert parsed == {"path": "text2cypher"}
        assert err is None

    def test_invalid_json_returns_none_and_error(self):
        parsed, err = _parse_json_response("not json at all")
        assert parsed is None
        assert err is not None
        assert isinstance(err, str)

    def test_non_dict_json_returns_none(self):
        parsed, err = _parse_json_response("[1, 2, 3]")
        assert parsed is None
        assert err is not None

    def test_empty_string_returns_none(self):
        parsed, err = _parse_json_response("")
        assert parsed is None
        assert err is not None

    def test_full_stage2_contract(self):
        raw = """{
            "path": "text2cypher",
            "anchors": {
                "stations": ["Metro Center"],
                "routes": ["Red Line"],
                "dates": ["2026-03-15"],
                "pathway_nodes": []
            },
            "path_reasoning": "Count query — text2cypher",
            "anchor_notes": "Date explicit in query"
        }"""
        parsed, err = _parse_json_response(raw)
        assert err is None
        assert parsed["path"] == "text2cypher"
        assert parsed["anchors"]["stations"] == ["Metro Center"]
        assert parsed["path_reasoning"] == "Count query — text2cypher"


# ── Anchor extraction ─────────────────────────────────────────────────────────

class TestExtractAnchors:

    def test_full_anchor_dict(self):
        anchors = _extract_anchors({
            "stations": ["Metro Center", "Gallery Place"],
            "routes": ["Red Line"],
            "dates": ["2026-03-15"],
            "pathway_nodes": [],
        })
        assert anchors.stations == ["Metro Center", "Gallery Place"]
        assert anchors.routes == ["Red Line"]
        assert anchors.dates == ["2026-03-15"]
        assert anchors.pathway_nodes == []

    def test_string_coercion(self):
        # LLM occasionally returns a string instead of a single-element list
        anchors = _extract_anchors({
            "stations": "Metro Center",
            "routes": [],
            "dates": [],
            "pathway_nodes": [],
        })
        assert anchors.stations == ["Metro Center"]

    def test_empty_dict(self):
        anchors = _extract_anchors({})
        assert anchors.stations == []
        assert anchors.routes == []
        assert anchors.dates == []
        assert anchors.pathway_nodes == []

    def test_none_values_dropped(self):
        anchors = _extract_anchors({
            "stations": [None, "Metro Center", None],
            "routes": [],
            "dates": [],
            "pathway_nodes": [],
        })
        assert anchors.stations == ["Metro Center"]

    def test_non_string_items_coerced(self):
        # Non-string list items should be coerced to str
        anchors = _extract_anchors({
            "stations": [],
            "routes": [],
            "dates": [20260315],
            "pathway_nodes": [],
        })
        assert anchors.dates == ["20260315"]


# ── PlannerAnchors ────────────────────────────────────────────────────────────

class TestPlannerAnchors:

    def test_is_empty_true_when_all_lists_empty(self):
        anchors = PlannerAnchors()
        assert anchors.is_empty()

    def test_is_empty_false_with_station(self):
        anchors = PlannerAnchors(stations=["Metro Center"])
        assert not anchors.is_empty()

    def test_is_empty_false_with_route_only(self):
        anchors = PlannerAnchors(routes=["Red Line"])
        assert not anchors.is_empty()

    def test_is_empty_false_with_date_only(self):
        anchors = PlannerAnchors(dates=["2026-03-15"])
        assert not anchors.is_empty()

    def test_is_empty_false_with_pathway_node_only(self):
        anchors = PlannerAnchors(pathway_nodes=["elevator at Metro Center"])
        assert not anchors.is_empty()

    def test_default_fields_are_empty_lists(self):
        anchors = PlannerAnchors()
        assert anchors.stations == []
        assert anchors.routes == []
        assert anchors.dates == []
        assert anchors.pathway_nodes == []
