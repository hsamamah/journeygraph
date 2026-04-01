# tests/llm/test_disambiguation_strategy.py
"""
Tests for the strategy pattern commit.

Covers:
    TopKStrategy              — always selects highest-scoring candidate
    TypeWeightedCoherenceStrategy:
        - unambiguous mentions pass through without a graph query
        - db=None falls back to TopK
        - unambiguous candidates are included in graph query as scoring anchors
          (regression test for the bug where Red Line was excluded from all_eids)
        - same-type pairs are excluded from scoring
        - correct winner selected by coherence score
        - string score used as tiebreaker on equal coherence
    AnchorResolver:
        - strategy and candidate_limit stored and reported via config
        - k=1 short-circuits strategy (strategy.select never called)
        - k>1 calls strategy.select with all candidates
    SubgraphOutput:
        - resolver_config field present and defaults to empty dict
        - make_zero_anchor_fallback includes resolver_config={}

No live Neo4j connection required — all graph queries are mocked.
"""

from unittest.mock import MagicMock, call, patch

import pytest

from src.llm.anchor_resolver import (
    AnchorResolutions,
    AnchorResolver,
    Candidate,
    TopKStrategy,
)
from src.llm.disambiguation_strategies import TypeWeightedCoherenceStrategy
from src.llm.planner_output import PlannerAnchors
from src.llm.subgraph_output import SubgraphOutput, make_zero_anchor_fallback


# ── Helpers ───────────────────────────────────────────────────────────────────


def make_candidate(
    node_id: str,
    anchor_type: str,
    score: float = 1.0,
    element_id: str | None = None,
    display_name: str | None = None,
) -> Candidate:
    return Candidate(
        node_id=node_id,
        display_name=display_name or node_id,
        score=score,
        element_id=element_id or f"eid_{node_id}",
        anchor_type=anchor_type,
    )


# ── TopKStrategy ──────────────────────────────────────────────────────────────


class TestTopKStrategy:
    def test_selects_first_candidate_per_mention(self):
        strategy = TopKStrategy()
        candidates = {
            "Farragut": [
                make_candidate("STN_A02", "station", score=1.0),
                make_candidate("STN_C03", "station", score=0.8),
            ],
            "Red Line": [
                make_candidate("RED", "route", score=1.0),
            ],
        }
        result = strategy.select(candidates, db=None)

        assert result["Farragut"] == "STN_A02"
        assert result["Red Line"] == "RED"

    def test_empty_candidates_excluded(self):
        strategy = TopKStrategy()
        result = strategy.select({}, db=None)
        assert result == {}

    def test_does_not_call_db(self):
        strategy = TopKStrategy()
        mock_db = MagicMock()
        strategy.select(
            {"station": [make_candidate("STN_X", "station")]},
            db=mock_db,
        )
        mock_db.query.assert_not_called()


# ── TypeWeightedCoherenceStrategy ─────────────────────────────────────────────


class TestTypeWeightedCoherenceStrategy:

    # ── Pass-through for unambiguous mentions ─────────────────────────────────

    def test_single_candidate_passes_through_without_graph_query(self):
        """All mentions have one candidate — no graph query should fire."""
        mock_db = MagicMock()
        strategy = TypeWeightedCoherenceStrategy()

        candidates = {
            "Metro Center": [make_candidate("STN_A01", "station")],
            "Red Line":     [make_candidate("RED", "route")],
        }
        result = strategy.select(candidates, db=mock_db)

        assert result == {"Metro Center": "STN_A01", "Red Line": "RED"}
        mock_db.query.assert_not_called()

    # ── db=None fallback ──────────────────────────────────────────────────────

    def test_db_none_falls_back_to_topk_for_ambiguous(self):
        """When db is None, ambiguous mentions fall back to highest string score."""
        strategy = TypeWeightedCoherenceStrategy()

        candidates = {
            "Farragut": [
                make_candidate("STN_A02", "station", score=1.0),
                make_candidate("STN_C03", "station", score=0.8),
            ],
        }
        result = strategy.select(candidates, db=None)

        assert result["Farragut"] == "STN_A02"

    # ── Critical bug regression: unambiguous candidates in graph query ─────────

    def test_unambiguous_candidates_included_in_graph_query(self):
        """
        Regression test for the bug where single-candidate mentions (e.g. Red Line)
        were excluded from all_eids, causing cross-type edges to be missed and
        coherence scores to stay at 0.

        The graph query MUST include element IDs from ALL mentions — both
        ambiguous and unambiguous — so that cross-type edges (e.g.
        Farragut North ↔ Red Line) are found and contribute to scoring.
        """
        mock_db = MagicMock()
        # Graph returns SERVES edge between Farragut North and Red Line
        mock_db.query.return_value = [
            {
                "from_eid": "eid_STN_A02",  # Farragut North
                "to_eid":   "eid_RED",       # Red Line
                "rel_type": "SERVES",
            },
            {
                "from_eid": "eid_RED",
                "to_eid":   "eid_STN_A02",
                "rel_type": "SERVES",
            },
        ]

        strategy = TypeWeightedCoherenceStrategy()
        candidates = {
            # Ambiguous — two station candidates
            "Farragut": [
                make_candidate("STN_A02", "station", score=1.0, element_id="eid_STN_A02"),
                make_candidate("STN_C03", "station", score=1.0, element_id="eid_STN_C03"),
            ],
            # Unambiguous — single route candidate
            "Red Line": [
                make_candidate("RED", "route", score=1.0, element_id="eid_RED"),
            ],
        }

        result = strategy.select(candidates, db=mock_db)

        # Confirm the graph query was called with all three element IDs
        call_args = mock_db.query.call_args
        eids_passed = set(call_args[0][1]["eids"])  # positional args: (cypher, params)
        assert "eid_STN_A02" in eids_passed, "Farragut North eid missing from graph query"
        assert "eid_STN_C03" in eids_passed, "Farragut West eid missing from graph query"
        assert "eid_RED" in eids_passed, "Red Line eid missing — bug: was excluded before fix"

        # Farragut North wins via coherence (SERVES Red Line)
        assert result["Farragut"] == "STN_A02"
        # Red Line passes through unchanged
        assert result["Red Line"] == "RED"

    # ── Same-type exclusion ───────────────────────────────────────────────────

    def test_same_type_edges_do_not_contribute_to_score(self):
        """
        Edges between two candidates of the same anchor type (e.g. two stations)
        must not contribute to scoring — prevents self-reinforcement.
        """
        mock_db = MagicMock()
        # Only edge returned is between two station candidates — same type
        mock_db.query.return_value = [
            {
                "from_eid": "eid_STN_A02",
                "to_eid":   "eid_STN_C03",
                "rel_type": "TRANSFER",      # hypothetical same-type edge
            },
        ]

        strategy = TypeWeightedCoherenceStrategy()
        candidates = {
            "Farragut": [
                make_candidate("STN_A02", "station", score=1.0, element_id="eid_STN_A02"),
                make_candidate("STN_C03", "station", score=0.8, element_id="eid_STN_C03"),
            ],
        }

        result = strategy.select(candidates, db=mock_db)

        # Same-type edge ignored — falls back to string score tiebreaker
        assert result["Farragut"] == "STN_A02"  # wins on string score, not coherence

    # ── Coherence winner ──────────────────────────────────────────────────────

    def test_higher_coherence_score_wins_over_string_score(self):
        """
        Candidate with lower string score but higher coherence wins.
        Confirms coherence is the primary sort key.
        """
        mock_db = MagicMock()
        # Only STN_C03 (lower string score) has a cross-type edge
        mock_db.query.return_value = [
            {"from_eid": "eid_STN_C03", "to_eid": "eid_RED", "rel_type": "SERVES"},
            {"from_eid": "eid_RED", "to_eid": "eid_STN_C03", "rel_type": "SERVES"},
        ]

        strategy = TypeWeightedCoherenceStrategy()
        candidates = {
            "Farragut": [
                make_candidate("STN_A02", "station", score=1.0, element_id="eid_STN_A02"),
                make_candidate("STN_C03", "station", score=0.5, element_id="eid_STN_C03"),
            ],
            "Red Line": [
                make_candidate("RED", "route", score=1.0, element_id="eid_RED"),
            ],
        }

        result = strategy.select(candidates, db=mock_db)

        # STN_C03 wins despite lower string score because it has coherence signal
        assert result["Farragut"] == "STN_C03"

    # ── String score tiebreaker ───────────────────────────────────────────────

    def test_string_score_breaks_equal_coherence_tie(self):
        """
        When two candidates have equal coherence scores, higher string score wins.
        """
        mock_db = MagicMock()
        # Both candidates have identical cross-type edges to the same route
        mock_db.query.return_value = [
            {"from_eid": "eid_STN_A02", "to_eid": "eid_RED", "rel_type": "SERVES"},
            {"from_eid": "eid_RED", "to_eid": "eid_STN_A02", "rel_type": "SERVES"},
            {"from_eid": "eid_STN_C03", "to_eid": "eid_RED", "rel_type": "SERVES"},
            {"from_eid": "eid_RED", "to_eid": "eid_STN_C03", "rel_type": "SERVES"},
        ]

        strategy = TypeWeightedCoherenceStrategy()
        candidates = {
            "Farragut": [
                make_candidate("STN_A02", "station", score=0.9, element_id="eid_STN_A02"),
                make_candidate("STN_C03", "station", score=0.7, element_id="eid_STN_C03"),
            ],
            "Red Line": [
                make_candidate("RED", "route", score=1.0, element_id="eid_RED"),
            ],
        }

        result = strategy.select(candidates, db=mock_db)

        # Equal coherence (both serve Red Line) — higher string score wins
        assert result["Farragut"] == "STN_A02"


# ── AnchorResolver — strategy and candidate_limit params ─────────────────────


class TestAnchorResolverStrategyParams:
    def test_config_reflects_k1_topk_defaults(self):
        mock_db = MagicMock()
        resolver = AnchorResolver(db=mock_db)

        assert resolver.config == {"candidate_limit": 1, "strategy": "TopKStrategy"}

    def test_config_reflects_coherence_strategy(self):
        mock_db = MagicMock()
        resolver = AnchorResolver(
            db=mock_db,
            strategy=TypeWeightedCoherenceStrategy(),
            candidate_limit=5,
        )

        assert resolver.config == {
            "candidate_limit": 5,
            "strategy": "TypeWeightedCoherenceStrategy",
        }

    def test_k1_short_circuits_strategy(self):
        """
        When candidate_limit=1, the strategy's select() must never be called —
        the single candidate is taken directly.
        """
        mock_db = MagicMock()
        mock_strategy = MagicMock()

        # DB returns one station candidate
        mock_db.query.return_value = [
            {
                "id": "STN_A01", "name": "Metro Center",
                "score": 1.0, "element_id": "eid_STN_A01", "degree": 10,
            }
        ]

        resolver = AnchorResolver(
            db=mock_db,
            strategy=mock_strategy,
            candidate_limit=1,
        )
        anchors = PlannerAnchors(stations=["Metro Center"])
        result = resolver.resolve(anchors)

        mock_strategy.select.assert_not_called()
        assert result.resolved_stations["Metro Center"] == ["STN_A01"]

    def test_k_greater_than_1_calls_strategy(self):
        """
        When candidate_limit>1 and multiple candidates exist, strategy.select()
        must be called with all candidates.
        """
        mock_db = MagicMock()
        mock_strategy = MagicMock()
        mock_strategy.select_with_ties.return_value = {"Metro Center": ["STN_A01"]}

        # DB returns two candidates
        mock_db.query.return_value = [
            {
                "id": "STN_A01", "name": "Metro Center",
                "score": 1.0, "element_id": "eid_STN_A01", "degree": 10,
            },
            {
                "id": "STN_B01", "name": "Metro Center Alt",
                "score": 0.8, "element_id": "eid_STN_B01", "degree": 5,
            },
        ]

        resolver = AnchorResolver(
            db=mock_db,
            strategy=mock_strategy,
            candidate_limit=2,
        )
        anchors = PlannerAnchors(stations=["Metro Center"])
        result = resolver.resolve(anchors)

        mock_strategy.select_with_ties.assert_called_once()
        assert result.resolved_stations["Metro Center"] == ["STN_A01"]


# ── SubgraphOutput — resolver_config field ───────────────────────────────────


class TestSubgraphOutputResolverConfig:
    def test_resolver_config_field_exists_with_default(self):
        output = SubgraphOutput(
            context="",
            node_count=0,
            trimmed=False,
            provenance_nodes=[],
            anchor_resolutions={},
            domain="transfer_impact",
            success=False,
            failure_reason=None,
        )
        assert hasattr(output, "resolver_config")
        assert output.resolver_config == {}

    def test_resolver_config_populated_when_provided(self):
        cfg = {"candidate_limit": 5, "strategy": "TypeWeightedCoherenceStrategy"}
        output = SubgraphOutput(
            context="test context",
            node_count=3,
            trimmed=False,
            provenance_nodes=[],
            anchor_resolutions={"Metro Center": "STN_A01"},
            domain="delay_propagation",
            success=True,
            failure_reason=None,
            resolver_config=cfg,
        )
        assert output.resolver_config == cfg

    def test_make_zero_anchor_fallback_includes_resolver_config(self):
        output = make_zero_anchor_fallback("transfer_impact")

        assert hasattr(output, "resolver_config")
        assert output.resolver_config == {}
        assert output.success is False
        assert output.failure_reason == "No anchors resolved from query"
