# tests/llm/test_query_writer_smoke.py
"""
Layer 3 — smoke tests for the Text2Cypher path.

These tests make REAL Anthropic API calls with a mocked Neo4j driver.
They verify that the LLM produces a parseable Cypher block for each
known domain query, and that the end-to-end run_query_writer call
returns non-empty output.

Run with:
    pytest -m live_api tests/llm/test_query_writer_smoke.py -v

Skipped by default (not in the normal test suite).

Prerequisites:
    ANTHROPIC_API_KEY must be set in the environment / .env file.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from src.common.config import LLMConfig
from src.llm.cypher_validator import ValidationResult, cypher_validator
from src.llm.planner_output import PlannerAnchors, PlannerOutput
from src.llm.query_writer import QueryWriter, QueryWriterInput, run_query_writer
from src.llm.slice_registry import RelationshipTriple, SchemaSlice


# ── Marker — all tests in this file require a live API key ───────────────────

pytestmark = pytest.mark.live_api


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def llm_config() -> LLMConfig:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        pytest.skip("ANTHROPIC_API_KEY not set")
    return LLMConfig(
        anthropic_api_key=api_key,
        llm_provider="anthropic",
        llm_model=os.environ.get("LLM_MODEL", "claude-haiku-4-5-20251001"),
        llm_max_tokens=int(os.environ.get("LLM_MAX_TOKENS", "1024")),
        llm_narration_max_tokens=1024,
    )


def _mock_driver(records: list[dict] | None = None) -> MagicMock:
    """Neo4j driver that accepts EXPLAIN and returns fixed rows on execute."""
    driver = MagicMock()
    session = MagicMock()
    driver.session.return_value.__enter__.return_value = session
    driver.session.return_value.__exit__.return_value = False
    # EXPLAIN succeeds (no exception)
    # Execute returns the provided records (or empty)
    mock_records = [MagicMock(data=lambda r=r: r) for r in (records or [])]
    session.run.return_value = iter(mock_records)
    return driver


def _mock_planner_output(domain: str, path: str, schema_slice_key: str) -> PlannerOutput:
    return PlannerOutput(
        domain=domain,
        path=path,
        schema_slice_key=schema_slice_key,
        rejected=False,
        rejection_message=None,
        path_reasoning="smoke test",
        anchor_notes=None,
        parse_warning=None,
        anchors=PlannerAnchors(
            stations=["Metro Center"] if domain in {"accessibility", "delay_propagation"} else [],
            routes=["Red Line"] if domain == "transfer_impact" else [],
            dates=["yesterday"] if domain == "transfer_impact" else [],
            pathway_nodes=[],
        ),
    )


# ── Layer 3: QueryWriter produces valid Cypher blocks ─────────────────────────


@pytest.mark.parametrize("domain,query,schema_slice_key", [
    (
        "accessibility",
        "is the elevator at Metro Center out of service",
        "accessibility",
    ),
    (
        "transfer_impact",
        "how many trips were cancelled on the red line yesterday",
        "transfer_impact",
    ),
    (
        "delay_propagation",
        "are there any delays propagating from Gallery Place",
        "delay_propagation",
    ),
])
def test_query_writer_returns_cypher_block(
    llm_config: LLMConfig,
    domain: str,
    query: str,
    schema_slice_key: str,
) -> None:
    """LLM must return a ```cypher ... ``` block for each standard domain query."""
    anchors = PlannerAnchors(
        stations=["Metro Center"] if domain in {"accessibility", "delay_propagation"} else [],
        routes=["Red Line"] if domain == "transfer_impact" else [],
        dates=["yesterday"] if domain == "transfer_impact" else [],
        pathway_nodes=[],
    )
    qw_input = QueryWriterInput(
        user_query=query,
        anchors=anchors,
        schema_slice=schema_slice_key,
        schema_slice_obj=None,
        patterns=[],  # no few-shot — tests raw model capability
        conventions={},
    )
    writer = QueryWriter(llm_config)
    output = writer.run(qw_input)

    assert output.cypher_query, (
        f"Expected a non-empty Cypher query for domain={domain!r}. "
        f"LLM response was: {output.cot_comments!r}"
    )
    # Basic sanity: output should look like Cypher
    assert any(kw in output.cypher_query.upper() for kw in ("MATCH", "RETURN", "WITH")), (
        f"Cypher block doesn't look like Cypher: {output.cypher_query!r}"
    )


# ── Layer 3: run_query_writer loads patterns from disk ───────────────────────


@pytest.mark.parametrize("domain,query,schema_slice_key", [
    (
        "accessibility",
        "is the elevator at Metro Center out of service",
        "accessibility",
    ),
    (
        "transfer_impact",
        "how many trips were cancelled on the red line yesterday",
        "transfer_impact",
    ),
])
def test_run_query_writer_with_disk_patterns(
    llm_config: LLMConfig,
    domain: str,
    query: str,
    schema_slice_key: str,
) -> None:
    """run_query_writer loads .cypher files from disk and produces non-empty output."""
    planner_output = _mock_planner_output(domain, "text2cypher", schema_slice_key)
    output = run_query_writer(query, planner_output, llm_config)

    assert output.cypher_query, (
        f"run_query_writer returned empty Cypher for domain={domain!r}. "
        f"cot_comments: {output.cot_comments!r}"
    )


# ── Layer 3: cypher_validator runs against LLM-generated Cypher ───────────────


def test_cypher_validator_on_live_output(llm_config: LLMConfig) -> None:
    """
    Full path: LLM generates Cypher → validator runs EXPLAIN against mock driver.

    Validates that the LLM's output is syntactically accepted by the mock EXPLAIN
    step (the mock never raises, so this confirms the validator path runs cleanly
    end-to-end without crashing on real LLM output).
    """
    anchors = PlannerAnchors(stations=["Metro Center"], routes=[], dates=[], pathway_nodes=[])
    qw_input = QueryWriterInput(
        user_query="is the elevator at Metro Center out of service",
        anchors=anchors,
        schema_slice="accessibility",
        patterns=[],
        conventions={},
    )
    writer = QueryWriter(llm_config)
    output = writer.run(qw_input)

    if not output.cypher_query:
        pytest.skip("LLM returned no Cypher block — cannot validate")

    driver = _mock_driver()
    # Permissive slice so whitelist checks don't mask syntax errors
    permissive_slice = SchemaSlice(
        domain="accessibility",
        nodes=[],
        relationships=[],
        patterns=[],
        warnings=[],
        property_registry={},
    )
    result = cypher_validator(output.cypher_query, permissive_slice, {}, driver)

    # The mock driver never raises on EXPLAIN, so we should always reach the
    # whitelist step. A ValidationResult is returned either way.
    assert isinstance(result.valid, bool)
    assert isinstance(result.errors, list)
