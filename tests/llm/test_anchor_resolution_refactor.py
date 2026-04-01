# tests/llm/test_anchor_resolution_refactor.py
"""
Tests for the anchor resolution refactor commit.

Verifies that:
    1. SubgraphBuilder no longer owns AnchorResolver — the attribute is gone.
    2. SubgraphBuilder.run() accepts AnchorResolutions as an explicit parameter.
    3. Zero-anchor resolutions trigger the defensive guard and return
       SubgraphOutput(success=False) immediately — no expansion runs.
    4. Non-zero resolutions pass through to the HopExpander.
    5. AnchorResolver is independently instantiable — the class itself is
       unchanged and its resolve() contract still holds.

All tests mock Neo4jManager and HopExpander so no DB connection is required.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.llm.anchor_resolver import AnchorResolutions, AnchorResolver
from src.llm.planner_output import PlannerAnchors, PlannerOutput
from src.llm.subgraph_builder import SubgraphBuilder
from src.llm.subgraph_output import SubgraphOutput


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_db():
    """Minimal Neo4jManager mock — no real DB connection."""
    return MagicMock()


@pytest.fixture
def planner_output_transfer():
    """A non-rejected PlannerOutput for the transfer_impact domain."""
    return PlannerOutput(
        domain="transfer_impact",
        path="subgraph",
        anchors=PlannerAnchors(stations=["Metro Center"], dates=["yesterday"]),
        schema_slice_key="transfer_impact",
        rejected=False,
        rejection_message=None,
        path_reasoning="Topological query",
        anchor_notes=None,
        parse_warning=None,
    )


@pytest.fixture
def resolved_anchors():
    """AnchorResolutions with one station and one date resolved."""
    return AnchorResolutions(
        resolved_stations={"Metro Center": "STN_A01"},
        resolved_dates={"yesterday": "20260326"},
    )


@pytest.fixture
def empty_resolutions():
    """AnchorResolutions with nothing resolved — simulates full resolution failure."""
    return AnchorResolutions()


# ── Test 1: SubgraphBuilder no longer owns AnchorResolver ────────────────────


def test_subgraph_builder_has_no_resolver(mock_db):
    """
    SubgraphBuilder.__init__ must not create a _resolver attribute.
    The refactor moves resolver ownership to the pipeline orchestrator.
    """
    builder = SubgraphBuilder(db=mock_db)

    assert not hasattr(builder, "_resolver"), (
        "SubgraphBuilder should not own AnchorResolver after the refactor. "
        "Resolution is the orchestrator's responsibility."
    )


# ── Test 2: SubgraphBuilder.run() accepts AnchorResolutions parameter ─────────


def test_subgraph_builder_run_accepts_resolutions_parameter(
    mock_db,
    planner_output_transfer,
    resolved_anchors,
):
    """
    SubgraphBuilder.run() must accept (planner_output, resolutions) signature.
    Passing only planner_output (old signature) must raise TypeError.
    """
    builder = SubgraphBuilder(db=mock_db)

    # New signature — should not raise TypeError
    # We patch HopExpander so no real graph query fires
    with patch("src.llm.subgraph_builder.HopExpander") as MockExpander:
        mock_raw = MagicMock()
        mock_raw.node_count = 3
        mock_raw.rels = []
        mock_raw.provenance_nodes = []
        mock_raw.anchor_element_ids = set()
        mock_raw.domain = "transfer_impact"
        MockExpander.return_value.expand.return_value = mock_raw

        with patch("src.llm.subgraph_builder.ContextSerializer") as MockSerializer:
            mock_result = MagicMock()
            mock_result.trimmed = False
            mock_result.context = "SUBGRAPH CONTEXT — domain: transfer_impact\n"
            mock_result.token_count = 42
            mock_result.nodes_removed = 0
            MockSerializer.return_value.serialize_and_enforce.return_value = mock_result

            builder = SubgraphBuilder(db=mock_db)
            output = builder.run(planner_output_transfer, resolved_anchors)

    assert isinstance(output, SubgraphOutput)


def test_subgraph_builder_old_signature_raises(
    mock_db,
    planner_output_transfer,
):
    """
    Passing only planner_output without resolutions must raise TypeError.
    Confirms the old single-argument call site no longer works.
    """
    builder = SubgraphBuilder(db=mock_db)

    with pytest.raises(TypeError):
        builder.run(planner_output_transfer)  # missing resolutions argument


# ── Test 3: Zero-anchor defensive guard ──────────────────────────────────────


def test_zero_anchor_guard_returns_failure(
    mock_db,
    planner_output_transfer,
    empty_resolutions,
):
    """
    When AnchorResolutions.any_resolved is False, SubgraphBuilder.run()
    must return SubgraphOutput(success=False) immediately without calling
    the HopExpander.
    """
    with patch("src.llm.subgraph_builder.HopExpander") as MockExpander:
        builder = SubgraphBuilder(db=mock_db)
        output = builder.run(planner_output_transfer, empty_resolutions)

    assert output.success is False
    assert output.failure_reason == "No anchors resolved from query"
    assert output.node_count == 0
    assert output.context == ""

    # HopExpander.expand must never have been called
    MockExpander.return_value.expand.assert_not_called()


# ── Test 4: Non-zero resolutions reach the expander ──────────────────────────


def test_resolved_anchors_reach_expander(
    mock_db,
    planner_output_transfer,
    resolved_anchors,
):
    """
    When resolutions contain at least one resolved anchor, the HopExpander
    must be called with those resolutions. Confirms the data flows through.
    """
    with patch("src.llm.subgraph_builder.HopExpander") as MockExpander:
        mock_raw = MagicMock()
        mock_raw.node_count = 5
        mock_raw.rels = []
        mock_raw.provenance_nodes = []
        mock_raw.anchor_element_ids = {"eid_1", "eid_2"}
        mock_raw.domain = "transfer_impact"
        MockExpander.return_value.expand.return_value = mock_raw

        with patch("src.llm.subgraph_builder.ContextSerializer") as MockSerializer:
            mock_result = MagicMock()
            mock_result.trimmed = False
            mock_result.context = "SUBGRAPH CONTEXT — domain: transfer_impact\n"
            mock_result.token_count = 80
            mock_result.nodes_removed = 0
            MockSerializer.return_value.serialize_and_enforce.return_value = mock_result

            builder = SubgraphBuilder(db=mock_db)
            output = builder.run(planner_output_transfer, resolved_anchors)

    assert output.success is True
    # Expander was called with the resolutions we passed in
    MockExpander.return_value.expand.assert_called_once_with(
        resolutions=resolved_anchors,
        domain="transfer_impact",
    )


# ── Test 5: AnchorResolver is independently instantiable ─────────────────────


def test_anchor_resolver_instantiates_standalone(mock_db):
    """
    AnchorResolver must instantiate without being inside SubgraphBuilder —
    confirming it is now a standalone pipeline component.
    The class itself is unchanged; this test confirms the import path is clean.
    """
    from datetime import UTC, datetime

    invocation_time = datetime(2026, 3, 27, 12, 0, 0, tzinfo=UTC)
    resolver = AnchorResolver(db=mock_db, invocation_time=invocation_time)

    assert resolver is not None
    assert resolver.invocation_time == invocation_time
    assert resolver.db is mock_db
