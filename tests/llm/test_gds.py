# tests/llm/test_gds.py
"""
Tests for GDS (Graph Data Science) integration in the text2cypher pipeline.

Coverage:
  SliceRegistry.gds_available — GDS detection at startup
  Planner._gds_available propagation
  Planner._stage1_llm — use_gds parsed from LLM JSON, hallucination guard
  PlannerOutput.use_gds default
  run_query_writer — GDS few-shot loading, use_gds threading
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
import json
import pytest

from src.common.config import LLMConfig
from src.llm.planner_output import PlannerAnchors, PlannerOutput
from src.llm.query_writer import QueryWriterOutput, _GDS_SYSTEM_SECTION, run_query_writer
from src.llm.slice_registry import SchemaSlice, SliceRegistry


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_llm_config() -> LLMConfig:
    return LLMConfig(
        anthropic_api_key="test-key",
        llm_provider="anthropic",
        llm_model="claude-haiku-4-5-20251001",
        llm_max_tokens=512,
        llm_narration_max_tokens=1024,
    )


def _make_planner_output(domain: str = "transfer_impact", use_gds: bool = False) -> PlannerOutput:
    return PlannerOutput(
        domain=domain,
        path="text2cypher",
        anchors=PlannerAnchors(),
        schema_slice_key=domain,
        rejected=False,
        rejection_message=None,
        path_reasoning=None,
        anchor_notes=None,
        parse_warning=None,
        use_gds=use_gds,
    )


def _make_neo4j_mock(gds_installed: bool = True) -> MagicMock:
    """Return a Neo4jManager mock that simulates GDS installed or not."""
    neo4j = MagicMock()

    def _query(cypher: str, *args, **kwargs):
        if "gds.version" in cypher:
            if gds_installed:
                return [{"gdsVersion": "2.6.0"}]
            raise Exception("There is no procedure with the name `gds.version`")
        # db.labels / db.relationshipTypes / db.schema.nodeTypeProperties
        if "db.labels" in cypher:
            return [{"label": "Station"}, {"label": "Platform"}]
        if "db.relationshipTypes" in cypher:
            return [{"relationshipType": "TRANSFER_TO"}]
        if "db.schema.nodeTypeProperties" in cypher:
            return [{"nodeType": ":`Station`", "propertyName": "id"},
                    {"nodeType": ":`Station`", "propertyName": "name"}]
        return []

    neo4j.query.side_effect = _query
    return neo4j


def _minimal_slice_yaml_dir(tmp_path):
    """Write a minimal YAML slice file to tmp_path and return the path."""
    slices_dir = tmp_path / "slices"
    slices_dir.mkdir()
    (slices_dir / "transfer_impact.yaml").write_text(
        """
nodes: ["Station"]
relationships:
  - {from: Station, type: TRANSFER_TO, to: Station}
patterns: []
warnings: []
"""
    )
    return slices_dir


# ── SliceRegistry: gds_available detection ────────────────────────────────────


def test_slice_registry_gds_available_true_when_installed(tmp_path) -> None:
    """gds_available is True when CALL gds.version() succeeds."""
    slices_dir = _minimal_slice_yaml_dir(tmp_path)
    neo4j = _make_neo4j_mock(gds_installed=True)

    with patch("src.llm.slice_registry.SLICES_DIR", slices_dir):
        registry = SliceRegistry(neo4j, strict=False)

    assert registry.gds_available is True


def test_slice_registry_gds_available_false_when_not_installed(tmp_path) -> None:
    """gds_available is False when gds.version() raises (GDS not installed)."""
    slices_dir = _minimal_slice_yaml_dir(tmp_path)
    neo4j = _make_neo4j_mock(gds_installed=False)

    with patch("src.llm.slice_registry.SLICES_DIR", slices_dir):
        registry = SliceRegistry(neo4j, strict=False)

    assert registry.gds_available is False


def test_slice_registry_gds_detection_does_not_raise_in_strict_mode(tmp_path) -> None:
    """GDS absence never triggers a strict-mode failure — it is always soft-fail."""
    slices_dir = _minimal_slice_yaml_dir(tmp_path)
    neo4j = _make_neo4j_mock(gds_installed=False)

    with patch("src.llm.slice_registry.SLICES_DIR", slices_dir):
        # strict=True should not raise because of GDS absence
        registry = SliceRegistry(neo4j, strict=True)

    assert registry.gds_available is False


# ── PlannerOutput: use_gds default ───────────────────────────────────────────


def test_planner_output_use_gds_defaults_false() -> None:
    """use_gds defaults to False on PlannerOutput so existing callers are unaffected."""
    output = PlannerOutput(
        domain="transfer_impact",
        path="text2cypher",
        anchors=PlannerAnchors(),
        schema_slice_key="transfer_impact",
        rejected=False,
        rejection_message=None,
        path_reasoning=None,
        anchor_notes=None,
        parse_warning=None,
    )
    assert output.use_gds is False


def test_planner_output_use_gds_set_true() -> None:
    output = _make_planner_output(use_gds=True)
    assert output.use_gds is True


# ── Planner: GDS prompt injection and use_gds parsing ────────────────────────


def _make_planner(gds_available: bool = True):
    """Return a Planner with a mocked LLM and a registry stub."""
    from src.llm.planner import Planner

    registry = MagicMock()
    registry.gds_available = gds_available
    registry.get.return_value = MagicMock(spec=SchemaSlice)
    registry.domains.return_value = ["transfer_impact", "delay_propagation", "accessibility"]

    llm_config = _make_llm_config()
    with patch("src.llm.planner.build_llm"):
        planner = Planner(registry, llm_config)
    return planner


def test_planner_stores_gds_available_from_registry() -> None:
    planner = _make_planner(gds_available=True)
    assert planner._gds_available is True


def test_planner_stores_gds_unavailable_from_registry() -> None:
    planner = _make_planner(gds_available=False)
    assert planner._gds_available is False


def test_planner_injects_gds_addon_when_available() -> None:
    """When GDS is available the GDS section appears in the Stage 1 system prompt."""
    from src.llm.planner import _GDS_PROMPT_ADDON

    planner = _make_planner(gds_available=True)

    captured_prompts: list[str] = []

    def _fake_invoke(system_prompt: str, user_message: str) -> str:  # noqa: ANN001
        captured_prompts.append(system_prompt)
        return json.dumps({
            "domain": "transfer_impact",
            "path": "text2cypher",
            "anchors": {"stations": [], "routes": [], "dates": [], "pathway_nodes": [], "levels": []},
            "path_reasoning": "count query",
            "anchor_notes": None,
            "rejection_reason": None,
            "use_gds": False,
        })

    planner._invoke_llm = _fake_invoke
    planner.run("how many trips were cancelled on the red line")

    assert len(captured_prompts) == 1
    assert "Graph Data Science" in captured_prompts[0]


def test_planner_omits_gds_addon_when_unavailable() -> None:
    planner = _make_planner(gds_available=False)

    captured_prompts: list[str] = []

    def _fake_invoke(system_prompt: str, user_message: str) -> str:
        captured_prompts.append(system_prompt)
        return json.dumps({
            "domain": "transfer_impact",
            "path": "text2cypher",
            "anchors": {"stations": [], "routes": [], "dates": [], "pathway_nodes": [], "levels": []},
            "path_reasoning": "count query",
            "anchor_notes": None,
            "rejection_reason": None,
            "use_gds": False,
        })

    planner._invoke_llm = _fake_invoke
    planner.run("how many trips were cancelled on the red line")

    assert "Graph Data Science" not in captured_prompts[0]


def test_planner_parses_use_gds_true_from_llm_response() -> None:
    """use_gds=True in LLM JSON propagates to PlannerOutput when GDS is available."""
    planner = _make_planner(gds_available=True)

    planner._invoke_llm = lambda sp, um: json.dumps({
        "domain": "transfer_impact",
        "path": "text2cypher",
        "anchors": {"stations": [], "routes": [], "dates": [], "pathway_nodes": [], "levels": []},
        "path_reasoning": "needs centrality",
        "anchor_notes": None,
        "rejection_reason": None,
        "use_gds": True,
    })

    output = planner.run("which stations are the most important transfer hubs")
    assert output.use_gds is True


def test_planner_hallucination_guard_use_gds_false_when_gds_unavailable() -> None:
    """LLM returning use_gds=True is overridden to False when GDS is not installed."""
    planner = _make_planner(gds_available=False)

    planner._invoke_llm = lambda sp, um: json.dumps({
        "domain": "transfer_impact",
        "path": "text2cypher",
        "anchors": {"stations": [], "routes": [], "dates": [], "pathway_nodes": [], "levels": []},
        "path_reasoning": "needs centrality",
        "anchor_notes": None,
        "rejection_reason": None,
        "use_gds": True,  # LLM hallucinated this despite GDS being unavailable
    })

    output = planner.run("which stations are the most important transfer hubs")
    assert output.use_gds is False


def test_planner_use_gds_false_in_rejected_output() -> None:
    """Rejected PlannerOutput always has use_gds=False."""
    planner = _make_planner(gds_available=True)

    planner._invoke_llm = lambda sp, um: json.dumps({
        "domain": None,
        "path": None,
        "anchors": None,
        "path_reasoning": None,
        "anchor_notes": None,
        "rejection_reason": "out of scope",
        "use_gds": False,
    })

    output = planner.run("what is the weather today")
    assert output.rejected is True
    assert output.use_gds is False


# ── run_query_writer: use_gds threading and GDS few-shot loading ──────────────


def test_run_query_writer_use_gds_loads_gds_examples(tmp_path) -> None:
    """When use_gds=True, queries/gds/analytical.cypher content is added to patterns."""
    gds_dir = tmp_path / "queries" / "gds"
    gds_dir.mkdir(parents=True)
    (gds_dir / "analytical.cypher").write_text("// GDS example\nCALL gds.pageRank.stream({}) YIELD nodeId RETURN nodeId")

    domain_dir = tmp_path / "queries" / "transfer_impact"
    domain_dir.mkdir(parents=True)
    (domain_dir / "analytical.cypher").write_text("// domain example\nMATCH (n) RETURN n")

    conventions_dir = tmp_path / "src" / "llm"
    conventions_dir.mkdir(parents=True)
    (conventions_dir / "conventions.json").write_text("{}")

    planner_output = _make_planner_output("transfer_impact", use_gds=True)
    llm_config = _make_llm_config()

    llm_response = MagicMock()
    llm_response.content = [MagicMock(text="```cypher\nRETURN 1\n```")]

    captured_prompts: list[str] = []

    def _fake_create(**kwargs):  # noqa: ANN001
        captured_prompts.append(kwargs["messages"][0]["content"])
        return llm_response

    with patch("src.llm.query_writer.anthropic.Anthropic") as mock_anthropic, \
         patch("src.llm.query_writer.PROJECT_ROOT", tmp_path):
        mock_client = MagicMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create.side_effect = _fake_create

        run_query_writer(
            "which stations have highest PageRank",
            planner_output,
            llm_config,
            use_gds=True,
        )

    assert len(captured_prompts) == 1
    prompt = captured_prompts[0]
    assert "GDS example" in prompt
    assert "gds/analytical.cypher" in prompt


def test_run_query_writer_no_gds_examples_when_use_gds_false(tmp_path) -> None:
    """When use_gds=False, GDS few-shot examples are not loaded."""
    gds_dir = tmp_path / "queries" / "gds"
    gds_dir.mkdir(parents=True)
    (gds_dir / "analytical.cypher").write_text("// GDS example\nCALL gds.pageRank.stream({})")

    domain_dir = tmp_path / "queries" / "transfer_impact"
    domain_dir.mkdir(parents=True)
    (domain_dir / "analytical.cypher").write_text("// domain example\nMATCH (n) RETURN n")

    conventions_dir = tmp_path / "src" / "llm"
    conventions_dir.mkdir(parents=True)
    (conventions_dir / "conventions.json").write_text("{}")

    planner_output = _make_planner_output("transfer_impact", use_gds=False)
    llm_config = _make_llm_config()

    llm_response = MagicMock()
    llm_response.content = [MagicMock(text="```cypher\nRETURN 1\n```")]

    captured_prompts: list[str] = []

    def _fake_create(**kwargs):
        captured_prompts.append(kwargs["messages"][0]["content"])
        return llm_response

    with patch("src.llm.query_writer.anthropic.Anthropic") as mock_anthropic, \
         patch("src.llm.query_writer.PROJECT_ROOT", tmp_path):
        mock_client = MagicMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create.side_effect = _fake_create

        run_query_writer(
            "how many trips were cancelled",
            planner_output,
            llm_config,
            use_gds=False,
        )

    assert "GDS example" not in captured_prompts[0]


def test_run_query_writer_gds_section_in_system_prompt_when_use_gds(tmp_path) -> None:
    """When use_gds=True, the GDS procedure section appears in the system prompt."""
    domain_dir = tmp_path / "queries" / "transfer_impact"
    domain_dir.mkdir(parents=True)

    conventions_dir = tmp_path / "src" / "llm"
    conventions_dir.mkdir(parents=True)
    (conventions_dir / "conventions.json").write_text("{}")

    planner_output = _make_planner_output("transfer_impact", use_gds=True)
    llm_config = _make_llm_config()

    llm_response = MagicMock()
    llm_response.content = [MagicMock(text="```cypher\nRETURN 1\n```")]

    captured_prompts: list[str] = []

    def _fake_create(**kwargs):
        captured_prompts.append(kwargs["messages"][0]["content"])
        return llm_response

    with patch("src.llm.query_writer.anthropic.Anthropic") as mock_anthropic, \
         patch("src.llm.query_writer.PROJECT_ROOT", tmp_path):
        mock_client = MagicMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create.side_effect = _fake_create

        run_query_writer(
            "which stations are most central",
            planner_output,
            llm_config,
            use_gds=True,
        )

    assert "Graph Data Science" in captured_prompts[0]
    assert "gds.pageRank.stream" in captured_prompts[0]
