# tests/llm/test_query_writer.py
"""
Tests for QueryWriter and cypher_validator — Layers 1 and 2.

Layer 1 — pure logic, no DB, no API:
    QueryWriter._build_system_prompt
    QueryWriter._build_user_message
    QueryWriter._parse_llm_response
    cypher_validator label / relationship / property whitelisting
    run_query_writer pattern loading

Layer 2 — mocked Anthropic SDK + mocked Neo4j driver:
    QueryWriter.run end-to-end
    validate_and_log_cypher logger integration

Not covered here (require live API / DB):
    run_query_writer with real conventions.json (Layer 3 smoke tests)
    Full _run_query pipeline (Layer 3)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
import pytest

from src.common.config import LLMConfig
from src.llm.cypher_validator import ValidationResult, cypher_validator, validate_and_log_cypher
from src.llm.planner_output import PlannerAnchors
from src.llm.query_writer import QueryWriter, QueryWriterInput, QueryWriterOutput
from src.llm.slice_registry import RelationshipTriple, SchemaSlice


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def llm_config() -> LLMConfig:
    return LLMConfig(
        anthropic_api_key="test-key",
        llm_provider="anthropic",
        llm_model="claude-haiku-4-5-20251001",
        llm_max_tokens=512,
        llm_narration_max_tokens=1024,
    )


@pytest.fixture
def anchors() -> PlannerAnchors:
    return PlannerAnchors(
        stations=["Metro Center"],
        routes=[],
        dates=[],
        pathway_nodes=[],
    )


@pytest.fixture
def query_writer(llm_config: LLMConfig) -> QueryWriter:
    with patch("src.llm.query_writer.anthropic.Anthropic"):
        return QueryWriter(llm_config)


@pytest.fixture
def sample_input(anchors: PlannerAnchors) -> QueryWriterInput:
    return QueryWriterInput(
        user_query="is the elevator at Metro Center out of service",
        anchors=anchors,
        schema_slice="accessibility",
        schema_slice_obj=None,
        patterns=["MATCH (e:Elevator) RETURN e"],
        conventions={"stop_id_prefixes": {"STN_": "Station"}},
    )


# ── Layer 1: _build_system_prompt ─────────────────────────────────────────────


def test_build_system_prompt_includes_conventions(
    query_writer: QueryWriter, sample_input: QueryWriterInput
) -> None:
    prompt = query_writer._build_system_prompt(
        sample_input.conventions, sample_input.patterns
    )
    assert "System conventions:" in prompt
    assert "stop_id_prefixes" in prompt


def test_build_system_prompt_includes_patterns(
    query_writer: QueryWriter, sample_input: QueryWriterInput
) -> None:
    prompt = query_writer._build_system_prompt(
        sample_input.conventions, sample_input.patterns
    )
    assert "Example Cypher queries for this domain:" in prompt
    assert "MATCH (e:Elevator)" in prompt


def test_build_system_prompt_no_patterns_omits_section(
    query_writer: QueryWriter, sample_input: QueryWriterInput
) -> None:
    prompt = query_writer._build_system_prompt(sample_input.conventions, patterns=[])
    assert "Example Cypher queries for this domain:" not in prompt


def test_build_system_prompt_multiple_patterns_separated(
    query_writer: QueryWriter, sample_input: QueryWriterInput
) -> None:
    patterns = ["MATCH (a) RETURN a", "MATCH (b) RETURN b"]
    prompt = query_writer._build_system_prompt(sample_input.conventions, patterns)
    assert "---" in prompt


# ── Layer 1: _build_system_prompt — SchemaSlice injection ────────────────────


def test_build_system_prompt_injects_node_whitelist(query_writer: QueryWriter) -> None:
    slice_obj, _ = _make_slice(
        domain="accessibility",
        labels=["Pathway", "Station"],
        relationships=[("OutageEvent", "AFFECTS", "Pathway")],
    )
    prompt = query_writer._build_system_prompt({}, [], schema_slice=slice_obj)
    assert "Allowed node labels" in prompt
    assert "Pathway" in prompt
    assert "Station" in prompt


def test_build_system_prompt_injects_relationship_whitelist(query_writer: QueryWriter) -> None:
    slice_obj, _ = _make_slice(
        domain="accessibility",
        relationships=[("OutageEvent", "AFFECTS", "Pathway")],
    )
    prompt = query_writer._build_system_prompt({}, [], schema_slice=slice_obj)
    assert "Allowed relationship types" in prompt
    assert "AFFECTS" in prompt


def test_build_system_prompt_no_slice_omits_whitelist(query_writer: QueryWriter) -> None:
    prompt = query_writer._build_system_prompt({}, [], schema_slice=None)
    assert "Allowed node labels" not in prompt
    assert "Allowed relationship types" not in prompt


# ── Layer 1: _build_user_message ─────────────────────────────────────────────


def test_build_user_message_contains_query(
    query_writer: QueryWriter, anchors: PlannerAnchors
) -> None:
    msg = query_writer._build_user_message(
        "is the elevator broken", anchors, "accessibility"
    )
    assert "is the elevator broken" in msg


def test_build_user_message_contains_schema(
    query_writer: QueryWriter, anchors: PlannerAnchors
) -> None:
    msg = query_writer._build_user_message("query", anchors, "accessibility")
    assert "accessibility" in msg


def test_build_user_message_contains_anchors(
    query_writer: QueryWriter, anchors: PlannerAnchors
) -> None:
    msg = query_writer._build_user_message("query", anchors, "accessibility")
    assert "Metro Center" in msg


def test_build_user_message_injects_resolved_ids(
    query_writer: QueryWriter, anchors: PlannerAnchors
) -> None:
    resolved = {"yesterday": ["20260408"], "Metro Center": ["STN_C01_F01"]}
    msg = query_writer._build_user_message("query", anchors, "accessibility", resolved)
    assert "20260408" in msg
    assert "STN_C01_F01" in msg
    assert "do NOT use $parameters" in msg


def test_build_user_message_no_resolved_ids_omits_block(
    query_writer: QueryWriter, anchors: PlannerAnchors
) -> None:
    msg = query_writer._build_user_message("query", anchors, "accessibility", {})
    assert "do NOT use $parameters" not in msg


# ── Layer 1: _parse_llm_response ──────────────────────────────────────────────


def test_parse_llm_response_extracts_cypher_block(query_writer: QueryWriter) -> None:
    text = "Here is the query:\n```cypher\nMATCH (n) RETURN n\n```\nDone."
    cypher, cot = query_writer._parse_llm_response(text)
    assert cypher == "MATCH (n) RETURN n"
    assert "Here is the query:" in cot


def test_parse_llm_response_no_code_block_returns_empty_cypher(
    query_writer: QueryWriter,
) -> None:
    text = "I cannot generate a Cypher query for this request."
    cypher, cot = query_writer._parse_llm_response(text)
    assert cypher == ""
    assert cot == text


def test_parse_llm_response_multiline_cypher(query_writer: QueryWriter) -> None:
    text = "```cypher\nMATCH (n:Station)\nWHERE n.name = 'Metro Center'\nRETURN n\n```"
    cypher, _ = query_writer._parse_llm_response(text)
    assert "MATCH (n:Station)" in cypher
    assert "RETURN n" in cypher


def test_parse_llm_response_strips_whitespace(query_writer: QueryWriter) -> None:
    text = "```cypher\n  MATCH (n) RETURN n  \n```"
    cypher, _ = query_writer._parse_llm_response(text)
    assert cypher == "MATCH (n) RETURN n"


# ── Layer 1: cypher_validator ─────────────────────────────────────────────────


def _make_driver(explain_ok: bool = True) -> MagicMock:
    driver = MagicMock()
    session = MagicMock()
    driver.session.return_value.__enter__.return_value = session
    driver.session.return_value.__exit__.return_value = False
    if not explain_ok:
        session.run.side_effect = Exception("Syntax error near ...")
    else:
        # First call is EXPLAIN (returns None), second call is the real query
        records = [MagicMock(data=lambda: {"count": 5})]
        session.run.return_value = iter(records)
    return driver


def _make_slice(
    domain: str = "test",
    labels: list[str] | None = None,
    relationships: list[tuple[str, str, str]] | None = None,
    properties: dict[str, list[str]] | None = None,
) -> tuple[SchemaSlice, dict[str, list[str]]]:
    """Return (SchemaSlice, property_registry) matching production call convention."""
    rels = [
        RelationshipTriple(from_label=f, rel_type=t, to_label=to)
        for f, t, to in (relationships or [])
    ]
    prop_registry: dict[str, list[str]] = properties or {}
    slice_obj = SchemaSlice(
        domain=domain,
        nodes=labels or [],
        relationships=rels,
        patterns=[],
        warnings=[],
        property_registry=prop_registry,
    )
    return slice_obj, prop_registry


def test_cypher_validator_valid_query_returns_success() -> None:
    driver = _make_driver(explain_ok=True)
    slice_obj, prop_reg = _make_slice(
        labels=["Station"],
        relationships=[("Station", "CONNECTS_TO", "Station")],
        properties={"Station": ["name"]},
    )
    cypher = "MATCH (s:Station)-[:CONNECTS_TO]->(t:Station) RETURN s.name"
    result = cypher_validator(cypher, slice_obj, prop_reg, driver)
    assert result.valid is True
    assert result.errors == []


def test_cypher_validator_syntax_error_returns_invalid() -> None:
    driver = _make_driver(explain_ok=False)
    slice_obj, prop_reg = _make_slice()
    result = cypher_validator("NOT VALID CYPHER @@", slice_obj, prop_reg, driver)
    assert result.valid is False
    assert any("Syntax error" in e for e in result.errors)


def test_cypher_validator_unknown_label_flagged() -> None:
    driver = _make_driver(explain_ok=True)
    slice_obj, prop_reg = _make_slice(labels=["Station"])
    cypher = "MATCH (n:ServiceAlert) RETURN n"
    result = cypher_validator(cypher, slice_obj, prop_reg, driver)
    assert result.valid is False
    assert any("ServiceAlert" in e for e in result.errors)


def test_cypher_validator_known_label_not_flagged() -> None:
    driver = _make_driver(explain_ok=True)
    slice_obj, prop_reg = _make_slice(labels=["Station"])
    cypher = "MATCH (n:Station) RETURN n"
    result = cypher_validator(cypher, slice_obj, prop_reg, driver)
    assert not any("Station" in e and "whitelist" in e for e in result.errors)


def test_cypher_validator_directed_relationship_detected() -> None:
    """Directed rels (-[:REL]->) must be caught by the whitelist check."""
    driver = _make_driver(explain_ok=True)
    slice_obj, prop_reg = _make_slice(labels=["Station"], relationships=[])
    cypher = "MATCH (a:Station)-[:CONNECTS_TO]->(b:Station) RETURN a"
    result = cypher_validator(cypher, slice_obj, prop_reg, driver)
    assert any("CONNECTS_TO" in e for e in result.errors)


def test_cypher_validator_directed_relationship_whitelisted_passes() -> None:
    driver = _make_driver(explain_ok=True)
    slice_obj, prop_reg = _make_slice(
        labels=["Station"],
        relationships=[("Station", "CONNECTS_TO", "Station")],
    )
    cypher = "MATCH (a:Station)-[:CONNECTS_TO]->(b:Station) RETURN a"
    result = cypher_validator(cypher, slice_obj, prop_reg, driver)
    assert not any("CONNECTS_TO" in e for e in result.errors)


def test_cypher_validator_unknown_property_flagged() -> None:
    driver = _make_driver(explain_ok=True)
    slice_obj, prop_reg = _make_slice(
        labels=["Station"],
        properties={"Station": ["name"]},
    )
    cypher = "MATCH (n:Station) RETURN n.secret_field"
    result = cypher_validator(cypher, slice_obj, prop_reg, driver)
    assert any("secret_field" in e for e in result.errors)


def test_cypher_validator_multilabel_node_whitelisted() -> None:
    """Multi-label node strings like ':Interruption:Cancellation' are flattened correctly."""
    driver = _make_driver(explain_ok=True)
    slice_obj, prop_reg = _make_slice(labels=[":Interruption:Cancellation"])
    cypher = "MATCH (n:Interruption) RETURN n"
    result = cypher_validator(cypher, slice_obj, prop_reg, driver)
    assert not any("Interruption" in e and "whitelist" in e for e in result.errors)


# ── Layer 1: validate_and_log_cypher logger integration ──────────────────────


def test_validate_and_log_cypher_calls_logger_on_failure() -> None:
    driver = _make_driver(explain_ok=True)
    slice_obj, prop_reg = _make_slice(labels=[])
    cypher = "MATCH (n:UnknownLabel) RETURN n"
    logger = MagicMock()
    result = validate_and_log_cypher(cypher, slice_obj, prop_reg, driver, logger)
    assert result.valid is False
    logger.warning.assert_called_once()


def test_validate_and_log_cypher_no_log_on_success() -> None:
    driver = _make_driver(explain_ok=True)
    slice_obj, prop_reg = _make_slice(labels=["Station"])
    cypher = "MATCH (n:Station) RETURN n"
    logger = MagicMock()
    validate_and_log_cypher(cypher, slice_obj, prop_reg, driver, logger)
    logger.warning.assert_not_called()


# ── Layer 2: QueryWriter.run with mocked Anthropic ───────────────────────────


def test_query_writer_run_returns_output(
    llm_config: LLMConfig, sample_input: QueryWriterInput
) -> None:
    llm_response = MagicMock()
    llm_response.content = [MagicMock(text="Reasoning here.\n```cypher\nMATCH (n) RETURN n\n```")]

    with patch("src.llm.query_writer.anthropic.Anthropic") as mock_anthropic:
        mock_client = MagicMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create.return_value = llm_response

        writer = QueryWriter(llm_config)
        output = writer.run(sample_input)

    assert isinstance(output, QueryWriterOutput)
    assert output.cypher_query == "MATCH (n) RETURN n"
    assert "Reasoning here." in output.cot_comments


def test_query_writer_run_uses_config_model(
    llm_config: LLMConfig, sample_input: QueryWriterInput
) -> None:
    llm_response = MagicMock()
    llm_response.content = [MagicMock(text="```cypher\nRETURN 1\n```")]

    with patch("src.llm.query_writer.anthropic.Anthropic") as mock_anthropic:
        mock_client = MagicMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create.return_value = llm_response

        writer = QueryWriter(llm_config)
        writer.run(sample_input)

        call_kwargs = mock_client.messages.create.call_args
        assert call_kwargs.kwargs["model"] == llm_config.llm_model
        assert call_kwargs.kwargs["max_tokens"] == llm_config.llm_max_tokens


def test_query_writer_run_no_code_block_graceful(
    llm_config: LLMConfig, sample_input: QueryWriterInput
) -> None:
    llm_response = MagicMock()
    llm_response.content = [MagicMock(text="I cannot answer this query.")]

    with patch("src.llm.query_writer.anthropic.Anthropic") as mock_anthropic:
        mock_client = MagicMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create.return_value = llm_response

        writer = QueryWriter(llm_config)
        output = writer.run(sample_input)

    assert output.cypher_query == ""
    assert output.cot_comments == "I cannot answer this query."
