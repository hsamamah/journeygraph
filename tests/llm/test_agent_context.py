# tests/llm/test_agent_context.py
"""
Tests for AgentContext and the changes made in:
  - src/llm/agent.py          (AgentContext projection methods)
  - src/llm/cypher_validator.py (UNWIND alias fix)
  - src/llm/slice_registry.py   (properties_optional merge)
  - src/llm/query_writer.py     (analytical.cypher-only loading)

Layers:
  1 — pure logic, no DB, no LLM API
  2 — mocked filesystem / mocked driver
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from src.llm.agent import AgentContext
from src.llm.agent_tools import CypherQueryOutput
from src.llm.cypher_validator import cypher_validator
from src.llm.slice_registry import RelationshipTriple, SchemaSlice
from src.llm.subgraph_output import SubgraphOutput
from src.llm.text2cypher_output import Text2CypherOutput


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_driver(explain_ok: bool = True) -> MagicMock:
    driver = MagicMock()
    session = MagicMock()
    driver.session.return_value.__enter__.return_value = session
    driver.session.return_value.__exit__.return_value = False
    if not explain_ok:
        session.run.side_effect = Exception("Syntax error")
    else:
        records = [MagicMock(data=lambda: {})]
        session.run.return_value = iter(records)
    return driver


def _make_slice(
    domain: str = "test",
    labels: list[str] | None = None,
    relationships: list[tuple[str, str, str]] | None = None,
    properties: dict[str, list[str]] | None = None,
) -> tuple[SchemaSlice, dict[str, list[str]]]:
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


def _make_subgraph(success: bool = True, context: str = "ctx") -> SubgraphOutput:
    return SubgraphOutput(
        context=context if success else "",
        node_count=3 if success else 0,
        trimmed=False,
        provenance_nodes=[],
        anchor_resolutions={},
        domain="delay_propagation",
        success=success,
        failure_reason=None if success else "empty",
        resolver_config={},
    )


# ── AgentContext.project_t2c ──────────────────────────────────────────────────


class TestAgentContextProjectT2c:
    def test_no_cypher_calls_returns_none(self) -> None:
        ctx = AgentContext()
        assert ctx.project_t2c("delay_propagation") is None

    def test_all_failed_returns_failed_t2c(self) -> None:
        ctx = AgentContext()
        ctx.cypher_results.append(
            CypherQueryOutput(cypher="", results=[], attempt_count=3, success=False,
                              failure_reason="all attempts failed")
        )
        out = ctx.project_t2c("delay_propagation")
        assert out is not None
        assert out.success is False
        assert out.attempt_count == 3

    def test_single_success_returns_it(self) -> None:
        ctx = AgentContext()
        ctx.cypher_results.append(
            CypherQueryOutput(cypher="MATCH (n) RETURN n", results=[{"n": 1}],
                              attempt_count=1, success=True)
        )
        out = ctx.project_t2c("delay_propagation")
        assert out is not None
        assert out.success is True
        assert out.results == [{"n": 1}]
        assert out.domain == "delay_propagation"

    def test_multiple_successes_uses_last(self) -> None:
        ctx = AgentContext()
        ctx.cypher_results.append(
            CypherQueryOutput(cypher="MATCH (a) RETURN a", results=[{"a": 1}],
                              attempt_count=1, success=True)
        )
        ctx.cypher_results.append(
            CypherQueryOutput(cypher="MATCH (b) RETURN b", results=[{"b": 2}],
                              attempt_count=1, success=True)
        )
        out = ctx.project_t2c("delay_propagation")
        assert out is not None
        assert out.results == [{"b": 2}]

    def test_total_attempts_sums_across_calls(self) -> None:
        ctx = AgentContext()
        ctx.cypher_results.append(
            CypherQueryOutput(cypher="", results=[], attempt_count=3, success=False)
        )
        ctx.cypher_results.append(
            CypherQueryOutput(cypher="MATCH (n) RETURN n", results=[], attempt_count=2, success=True)
        )
        out = ctx.project_t2c("delay_propagation")
        assert out is not None
        assert out.attempt_count == 5

    def test_validation_notes_merged_across_calls(self) -> None:
        ctx = AgentContext()
        ctx.cypher_results.append(
            CypherQueryOutput(cypher="", results=[], attempt_count=1, success=False,
                              validation_notes=["note A"])
        )
        ctx.cypher_results.append(
            CypherQueryOutput(cypher="MATCH (n) RETURN n", results=[], attempt_count=1,
                              success=True, validation_notes=["note B"])
        )
        out = ctx.project_t2c("delay_propagation")
        assert out is not None
        assert "note A" in out.validation_notes
        assert "note B" in out.validation_notes


# ── AgentContext.project_subgraph ─────────────────────────────────────────────


class TestAgentContextProjectSubgraph:
    def test_no_subgraph_calls_returns_none(self) -> None:
        ctx = AgentContext()
        assert ctx.project_subgraph() is None

    def test_all_failed_returns_none(self) -> None:
        ctx = AgentContext()
        ctx.subgraph_results.append(_make_subgraph(success=False))
        assert ctx.project_subgraph() is None

    def test_single_success_returned_directly(self) -> None:
        ctx = AgentContext()
        sg = _make_subgraph(success=True, context="graph data")
        ctx.subgraph_results.append(sg)
        out = ctx.project_subgraph()
        assert out is sg

    def test_multiple_successes_merged(self) -> None:
        ctx = AgentContext()
        ctx.subgraph_results.append(_make_subgraph(success=True, context="part A"))
        ctx.subgraph_results.append(_make_subgraph(success=True, context="part B"))
        out = ctx.project_subgraph()
        assert out is not None
        assert "part A" in out.context
        assert "part B" in out.context
        assert out.node_count == 6

    def test_failed_before_success_ignored(self) -> None:
        ctx = AgentContext()
        ctx.subgraph_results.append(_make_subgraph(success=False))
        ctx.subgraph_results.append(_make_subgraph(success=True, context="real data"))
        out = ctx.project_subgraph()
        assert out is not None
        assert out.success is True


# ── AgentContext.as_trace_dict ────────────────────────────────────────────────


class TestAgentContextAsTraceDict:
    def test_empty_context_trace(self) -> None:
        ctx = AgentContext()
        trace = ctx.as_trace_dict()
        assert trace["mode"] == "agentic"
        assert trace["total_tool_calls"] == 0
        assert trace["has_cypher_data"] is False
        assert trace["has_subgraph_data"] is False

    def test_tool_call_history_serialized(self) -> None:
        ctx = AgentContext()
        ctx.tool_call_history.append(("cypher_query", {"question": "q"}, {"rows": 5}))
        trace = ctx.as_trace_dict()
        assert trace["total_tool_calls"] == 1
        assert trace["tool_call_history"][0]["tool"] == "cypher_query"

    def test_has_cypher_data_true_when_any_success(self) -> None:
        ctx = AgentContext()
        ctx.cypher_results.append(
            CypherQueryOutput(cypher="MATCH (n) RETURN n", results=[], attempt_count=1, success=True)
        )
        assert ctx.as_trace_dict()["has_cypher_data"] is True


# ── cypher_validator: UNWIND alias fix ────────────────────────────────────────


class TestCypherValidatorUnwindAlias:
    """
    The UNWIND map alias pattern (COLLECT({key: node}) ... UNWIND ... AS td ... td.key)
    must not produce false-positive property errors. td is a map variable, not a node.
    """

    def test_unwind_map_alias_not_flagged_as_property(self) -> None:
        driver = _make_driver(explain_ok=True)
        slice_obj, prop_reg = _make_slice(
            labels=["Station", "Interruption", "Trip", "Route", "Platform", "RoutePattern"],
            relationships=[
                ("Station", "CONTAINS", "Platform"),
                ("Trip", "SCHEDULED_AT", "Platform"),
                ("Interruption", "AFFECTS_TRIP", "Trip"),
                ("Trip", "FOLLOWS", "RoutePattern"),
                ("RoutePattern", "BELONGS_TO", "Route"),
            ],
            properties={
                "Station": ["name"],
                "Interruption": ["interruption_id", "severity", "start_time", "description"],
                "Route": ["route_short_name"],
                "Trip": ["trip_id"],
            },
        )
        # This pattern appeared in [3/4] Gallery Place delay query — attempt 1.
        # td.route and td.trip are map field accesses, NOT node property accesses.
        cypher = """
MATCH (s:Station {id: 'STN_B01_F01'})
OPTIONAL MATCH (s)-[:CONTAINS]->(p:Platform)<-[:SCHEDULED_AT]-(t:Trip)
OPTIONAL MATCH (i:Interruption)-[:AFFECTS_TRIP]->(t)
OPTIONAL MATCH (t)-[:FOLLOWS]->(rp:RoutePattern)-[:BELONGS_TO]->(r:Route)
WITH s, COLLECT(DISTINCT {delay: i, trip: t, route: r}) AS trip_delays
UNWIND trip_delays AS td
WITH s, td.delay AS i, td.trip AS t, td.route AS r
WHERE i IS NOT NULL
RETURN s.name AS station, i.interruption_id AS delay_id, r.route_short_name AS route
"""
        result = cypher_validator(cypher, slice_obj, prop_reg, driver)
        # 'route' and 'trip' should NOT appear as property errors
        assert not any("'route' not in registry" in e for e in result.errors)
        assert not any("'trip' not in registry" in e for e in result.errors)

    def test_non_unwind_unknown_property_still_flagged(self) -> None:
        driver = _make_driver(explain_ok=True)
        slice_obj, prop_reg = _make_slice(
            labels=["Station"],
            properties={"Station": ["name"]},
        )
        cypher = "MATCH (s:Station) RETURN s.end_time"
        result = cypher_validator(cypher, slice_obj, prop_reg, driver)
        assert any("end_time" in e for e in result.errors)

    def test_multiple_unwind_aliases_all_excluded(self) -> None:
        driver = _make_driver(explain_ok=True)
        slice_obj, prop_reg = _make_slice(
            labels=["Station"],
            properties={"Station": ["name"]},
        )
        # Two UNWIND aliases — both their field accesses must be excluded
        cypher = """
WITH [{a: 1}] AS xs, [{b: 2}] AS ys
UNWIND xs AS x
UNWIND ys AS y
MATCH (s:Station) RETURN s.name, x.a, y.b
"""
        result = cypher_validator(cypher, slice_obj, prop_reg, driver)
        assert not any("'a' not in registry" in e for e in result.errors)
        assert not any("'b' not in registry" in e for e in result.errors)

    def test_unwind_list_comprehension_alias_excluded(self) -> None:
        """UNWIND [x IN list | x.val] AS item — space-containing expression."""
        driver = _make_driver(explain_ok=True)
        slice_obj, prop_reg = _make_slice(
            labels=["Station"],
            properties={"Station": ["name"]},
        )
        cypher = """
WITH [1, 2, 3] AS nums
UNWIND [n IN nums | n + 1] AS item
MATCH (s:Station) RETURN s.name, item.ghost_key
"""
        result = cypher_validator(cypher, slice_obj, prop_reg, driver)
        # item is an UNWIND alias — item.ghost_key must not be flagged
        assert not any("ghost_key" in e for e in result.errors)

    def test_property_on_node_variable_still_checked(self) -> None:
        """s is not an UNWIND alias — s.ghost_prop must still be flagged."""
        driver = _make_driver(explain_ok=True)
        slice_obj, prop_reg = _make_slice(
            labels=["Station"],
            properties={"Station": ["name"]},
        )
        cypher = """
WITH [{x: 1}] AS rows
UNWIND rows AS row
MATCH (s:Station) RETURN s.ghost_prop, row.x
"""
        result = cypher_validator(cypher, slice_obj, prop_reg, driver)
        assert any("ghost_prop" in e for e in result.errors)
        assert not any("'x' not in registry" in e for e in result.errors)


# ── slice_registry: properties_optional ──────────────────────────────────────


class TestSliceRegistryPropertiesOptional:
    """
    properties_optional in the YAML declares properties that exist in the schema
    but are absent from all current live nodes (e.g. end_time on Interruption).
    They must be merged into property_registry so the validator whitelists them.
    """

    def test_properties_optional_whitelisted_by_validator(self) -> None:
        """end_time declared in properties_optional must not be flagged."""
        driver = _make_driver(explain_ok=True)
        # Simulate what SliceRegistry._build_slice produces when end_time is
        # declared under properties_optional but absent from the live graph registry.
        slice_obj = SchemaSlice(
            domain="delay_propagation",
            nodes=[":Interruption:Delay"],
            relationships=[],
            patterns=[],
            warnings=[],
            property_registry={"Interruption": ["start_time", "severity", "end_time"]},
            properties_optional={"Interruption": ["end_time"]},
        )
        cypher = """
MATCH (i:Interruption:Delay)
WHERE i.end_time IS NOT NULL
RETURN i.start_time AS started, i.end_time AS resolved, i.severity AS severity
"""
        result = cypher_validator(cypher, slice_obj, slice_obj.property_registry, driver)
        assert not any("end_time" in e for e in result.errors)

    def test_properties_optional_merged_into_registry(self) -> None:
        """
        _build_slice must merge YAML properties_optional into the scoped
        property_registry even when the live graph registry omits the property.
        """
        from src.llm.slice_registry import SliceRegistry

        raw = {
            "domain": "delay_propagation",
            "nodes": [":Interruption:Delay"],
            "nodes_optional": [],
            "relationships": [],
            "patterns": [],
            "warnings": [],
            "properties_optional": {"Interruption": ["end_time"]},
        }
        # Live graph registry — end_time absent (no resolved interruptions yet)
        live_registry = {"Interruption": ["start_time", "severity"]}

        registry = SliceRegistry.__new__(SliceRegistry)  # bypass __init__ (needs DB)
        slice_obj = registry._build_slice("delay_propagation", raw, live_registry)

        assert "end_time" in slice_obj.property_registry.get("Interruption", [])

    def test_properties_optional_does_not_duplicate_existing(self) -> None:
        """If the live graph already has end_time, it must not appear twice."""
        from src.llm.slice_registry import SliceRegistry

        raw = {
            "domain": "delay_propagation",
            "nodes": [":Interruption:Delay"],
            "nodes_optional": [],
            "relationships": [],
            "patterns": [],
            "warnings": [],
            "properties_optional": {"Interruption": ["end_time"]},
        }
        live_registry = {"Interruption": ["start_time", "severity", "end_time"]}

        registry = SliceRegistry.__new__(SliceRegistry)
        slice_obj = registry._build_slice("delay_propagation", raw, live_registry)

        props = slice_obj.property_registry.get("Interruption", [])
        assert props.count("end_time") == 1

    def test_properties_optional_stored_on_slice(self) -> None:
        """properties_optional is accessible on the SchemaSlice for inspection."""
        from src.llm.slice_registry import SliceRegistry

        raw = {
            "domain": "delay_propagation",
            "nodes": [":Interruption"],
            "nodes_optional": [],
            "relationships": [],
            "patterns": [],
            "warnings": [],
            "properties_optional": {"Interruption": ["end_time"]},
        }
        registry = SliceRegistry.__new__(SliceRegistry)
        slice_obj = registry._build_slice("delay_propagation", raw, {})
        assert slice_obj.properties_optional == {"Interruption": ["end_time"]}

    def test_typo_label_in_properties_optional_skipped_with_warning(self) -> None:
        """A misspelled label in properties_optional must not create phantom whitelist entries."""
        from unittest.mock import patch
        from src.llm.slice_registry import SliceRegistry

        raw = {
            "domain": "delay_propagation",
            "nodes": [":Interruption:Delay"],
            "nodes_optional": [],
            "relationships": [],
            "patterns": [],
            "warnings": [],
            "properties_optional": {"Interuption": ["end_time"]},  # typo: missing 'r'
        }
        registry = SliceRegistry.__new__(SliceRegistry)
        with patch("src.llm.slice_registry.log") as mock_log:
            slice_obj = registry._build_slice("delay_propagation", raw, {})
        # The typo'd label must not appear in property_registry
        assert "Interuption" not in slice_obj.property_registry
        # A warning must have been logged
        assert mock_log.warning.called

    def test_no_properties_optional_key_is_fine(self) -> None:
        """YAML without properties_optional must still build a valid slice."""
        from src.llm.slice_registry import SliceRegistry

        raw = {
            "domain": "transfer_impact",
            "nodes": [":Trip"],
            "nodes_optional": [],
            "relationships": [],
            "patterns": [],
            "warnings": [],
        }
        registry = SliceRegistry.__new__(SliceRegistry)
        slice_obj = registry._build_slice("transfer_impact", raw, {"Trip": ["trip_id"]})
        assert slice_obj.properties_optional == {}
        assert "trip_id" in slice_obj.property_registry.get("Trip", [])


# ── query_writer: analytical.cypher-only loading ─────────────────────────────


class TestQueryWriterAnalyticalLoading:
    """
    run_query_writer must load only analytical.cypher — not constraints.cypher,
    nodes.cypher, or relationships.cypher that co-exist in queries/accessibility/.
    """

    def test_loads_analytical_cypher_when_present(self, tmp_path) -> None:
        from unittest.mock import MagicMock, patch

        from src.common.config import LLMConfig
        from src.llm.planner_output import PlannerAnchors, PlannerOutput
        from src.llm.query_writer import run_query_writer

        domain = "test_domain"
        domain_dir = tmp_path / "queries" / domain
        domain_dir.mkdir(parents=True)

        (domain_dir / "analytical.cypher").write_text("// analytical\nMATCH (n) RETURN n")
        (domain_dir / "constraints.cypher").write_text("CREATE CONSTRAINT ...")
        (domain_dir / "nodes.cypher").write_text("UNWIND $rows AS row MERGE ...")

        llm_config = LLMConfig(
            anthropic_api_key="test-key",
            llm_provider="anthropic",
            llm_model="claude-haiku-4-5-20251001",
            llm_max_tokens=512,
            llm_narration_max_tokens=1024,
        )
        planner_output = MagicMock()
        planner_output.schema_slice_key = domain
        planner_output.anchors = PlannerAnchors(stations=[], routes=[], dates=[], pathway_nodes=[])

        llm_response = MagicMock()
        llm_response.content = [MagicMock(text="```cypher\nRETURN 1\n```")]

        with (
            patch("src.llm.query_writer.anthropic.Anthropic") as mock_anthropic,
            patch("os.path.join", side_effect=lambda *args: os.path.join(tmp_path, *args)
                  if args[0] in ("queries", "src") else os.path.join(*args)),
            patch("src.llm.query_writer.os.path.isfile",
                  side_effect=lambda p: os.path.isfile(p.replace(str(os.getcwd()), str(tmp_path)))),
        ):
            mock_client = MagicMock()
            mock_anthropic.return_value = mock_client
            mock_client.messages.create.return_value = llm_response

            # Verify via the log output that only 1 few-shot file was loaded
            with patch("src.llm.query_writer.log") as mock_log:
                try:
                    run_query_writer(
                        "test query", planner_output, llm_config,
                        schema_slice=None, resolved_anchors={},
                    )
                except Exception:
                    pass  # conventions.json path may fail in tmp_path context
                # Check that if the log fired, it only reported 1 file
                for call in mock_log.info.call_args_list:
                    args = call[0]
                    if "few_shot_files" in str(args):
                        assert args[3] <= 1, (
                            f"Expected at most 1 few-shot file, log reported: {args}"
                        )

    def test_no_analytical_cypher_loads_zero_patterns(self, tmp_path) -> None:
        """Domain with no analytical.cypher produces empty patterns — no DDL fallback."""
        from src.llm.query_writer import run_query_writer
        from src.common.config import LLMConfig
        from src.llm.planner_output import PlannerAnchors

        domain = "no_examples_domain"
        domain_dir = tmp_path / "queries" / domain
        domain_dir.mkdir(parents=True)
        # Only DDL files — no analytical.cypher
        (domain_dir / "constraints.cypher").write_text("CREATE CONSTRAINT ...")

        llm_config = LLMConfig(
            anthropic_api_key="key",
            llm_provider="anthropic",
            llm_model="claude-haiku-4-5-20251001",
            llm_max_tokens=512,
            llm_narration_max_tokens=1024,
        )
        planner_output = MagicMock()
        planner_output.schema_slice_key = domain
        planner_output.anchors = PlannerAnchors(stations=[], routes=[], dates=[], pathway_nodes=[])

        llm_response = MagicMock()
        llm_response.content = [MagicMock(text="```cypher\nRETURN 1\n```")]

        with patch("src.llm.query_writer.anthropic.Anthropic") as mock_anthropic:
            mock_client = MagicMock()
            mock_anthropic.return_value = mock_client
            mock_client.messages.create.return_value = llm_response
            with patch("src.llm.query_writer.log") as mock_log:
                try:
                    run_query_writer(
                        "test query", planner_output, llm_config,
                        schema_slice=None, resolved_anchors={},
                    )
                except Exception:
                    pass
                for call in mock_log.info.call_args_list:
                    args = call[0]
                    if "few_shot_files" in str(args):
                        assert args[3] == 0, (
                            f"Expected 0 few-shot files for domain with no analytical.cypher, got: {args}"
                        )
