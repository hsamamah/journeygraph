# tests/test_planner.py
"""
Unit tests for the LLM Planner — pure Python components only.

Coverage:
    Stage 1 — domain classifier (_COMPILED_SIGNALS, _DOMAIN_WEIGHTS)
    JSON parsing — _parse_json_response
    Anchor extraction — _extract_anchors
    PlannerAnchors — is_empty()

Not covered here (require live DB or LLM):
    SliceRegistry — needs Neo4j connection
    Stage 2 — needs LLM mock
    Planner.run() end-to-end — needs both

Verified manually against live output before tests were written:
    "how many trips were cancelled on the red line yesterday"
        → domain=transfer_impact, path=text2cypher,
          anchors: routes=['red line'], dates=['yesterday']
    "are there any delays propagating from Gallery Place"
        → domain=delay_propagation, path=subgraph,
          anchors: stations=['Gallery Place']
"""

import pytest

from src.llm.planner import (
    _COMPILED_SIGNALS,
    _DOMAIN_WEIGHTS,
    _Stage1Result,
    _extract_anchors,
    _parse_json_response,
)
from src.llm.planner_output import PlannerAnchors


# ── Helpers ───────────────────────────────────────────────────────────────────

def _classify(query: str) -> _Stage1Result:
    """
    Run Stage 1 classification without instantiating Planner.
    Mirrors the logic in Planner._stage1_classify exactly.
    """
    scores: dict[str, float] = {}
    for domain, patterns in _COMPILED_SIGNALS.items():
        matched = [p for p in patterns if p.search(query)]
        scores[domain] = len(matched) / len(matched) if matched else 0.0

    if all(s == 0.0 for s in scores.values()):
        return _Stage1Result(domain=None, scores=scores, rejected=True)

    winner = max(scores, key=lambda d: (scores[d], _DOMAIN_WEIGHTS[d]))
    return _Stage1Result(domain=winner, scores=scores, rejected=False)


# ── Stage 1: domain classifier ────────────────────────────────────────────────

class TestStage1Classifier:

    def test_transfer_impact_cancelled(self):
        r = _classify("how many trips were cancelled on the red line yesterday")
        assert not r.rejected
        assert r.domain == "transfer_impact"

    def test_transfer_impact_cancellations_variant(self):
        # Prefix matching — 'cancel' covers 'cancellations'
        r = _classify("are there cancellations on the blue line")
        assert not r.rejected
        assert r.domain == "transfer_impact"

    def test_transfer_impact_missed_connection(self):
        r = _classify("I missed my connection at Metro Center")
        assert not r.rejected
        assert r.domain == "transfer_impact"

    def test_delay_propagation_propagating_variant(self):
        # The reported bug — 'propagating' was not matched before prefix fix
        r = _classify("are there any delays propagating from Gallery Place")
        assert not r.rejected
        assert r.domain == "delay_propagation"

    def test_delay_propagation_delayed_variant(self):
        # Prefix matching — 'delay' covers 'delayed', 'delays', 'delaying'
        r = _classify("the train is delayed at Farragut North")
        assert not r.rejected
        assert r.domain == "delay_propagation"

    def test_delay_propagation_downstream(self):
        r = _classify("how far downstream has the delay spread")
        assert not r.rejected
        assert r.domain == "delay_propagation"

    def test_delay_propagation_behind_schedule(self):
        # Multi-word signal
        r = _classify("the red line is behind schedule")
        assert not r.rejected
        assert r.domain == "delay_propagation"

    def test_accessibility_elevator(self):
        r = _classify("is the elevator at Metro Center working")
        assert not r.rejected
        assert r.domain == "accessibility"

    def test_accessibility_escalator_variant(self):
        # Prefix matching — 'escalator' covers 'escalators'
        r = _classify("escalators out of service at Gallery Place")
        assert not r.rejected
        assert r.domain == "accessibility"

    def test_accessibility_wheelchair(self):
        r = _classify("wheelchair accessible route please")
        assert not r.rejected
        assert r.domain == "accessibility"

    def test_accessibility_outage(self):
        r = _classify("elevator outage at Dupont Circle")
        assert not r.rejected
        assert r.domain == "accessibility"

    def test_rejection_no_signals(self):
        r = _classify("what is the weather like in DC today")
        assert r.rejected
        assert r.domain is None

    def test_rejection_translate_false_positive_guard(self):
        # Word-boundary prefix: 'late' must not match 'translate'
        r = _classify("please translate this text for me")
        assert r.rejected

    def test_tiebreak_by_weight(self):
        # Equal scores — delay_propagation (weight 3) beats transfer_impact (weight 2)
        r = _classify("cancelled and delayed trips")
        assert not r.rejected
        assert r.domain == "delay_propagation"

    def test_normalization_single_strong_signal(self):
        # Single signal scores 1.0 — beats multi-weak domain
        r = _classify("wheelchair accessible route please")
        assert not r.rejected
        assert r.domain == "accessibility"
        assert r.scores["accessibility"] == 1.0

    def test_scores_present_for_all_domains(self):
        r = _classify("how many trips were cancelled yesterday")
        assert set(r.scores.keys()) == {"transfer_impact", "accessibility", "delay_propagation"}

    def test_rejected_result_has_all_zero_scores(self):
        r = _classify("what is the weather like today")
        assert r.rejected
        assert all(s == 0.0 for s in r.scores.values())

    def test_propagation_prefix_does_not_over_match(self):
        # 'propagate' prefix should not match unrelated words
        # 'properly' starts with 'prop' not 'propagate' — not a risk,
        # but verify signal vocabulary does not fire on nonsense queries
        r = _classify("the proposal was approved today")
        assert r.rejected


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
