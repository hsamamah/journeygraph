"""
demos/pipeline_demo.py — Helper module for the LLM pipeline demo notebook.

Provides:
- run_question()      Full pipeline execution, returns QuestionResult
- run_and_display()   run_question() + all display helpers in one call (Q2+)
- display_*()         Individual step display functions used in the Q1 walkthrough
- visualize_subgraph() Matplotlib graph render from RawSubgraph

Design: Q1 in the notebook calls each display_* function individually so the
professor sees a step-by-step walkthrough. Q2+ call run_and_display() so only
one line of code is visible per question.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import networkx as nx

from src.llm.anchor_resolver import AnchorResolutions, AnchorResolver
from src.llm.cypher_validator import validate_and_log_cypher
from src.llm.hop_expander import HopExpander, RawSubgraph
from src.llm.narration_output import NarrationOutput
from src.llm.planner_output import PlannerOutput
from src.llm.query_writer import run_query_writer
from src.llm.subgraph_builder import SubgraphBuilder
from src.llm.subgraph_output import SubgraphOutput
from src.llm.text2cypher_output import Text2CypherOutput

if TYPE_CHECKING:
    from src.common.config import LLMConfig
    from src.common.neo4j_tools import Neo4jManager
    from src.llm.narration_agent import NarrationAgent
    from src.llm.planner import Planner
    from src.llm.slice_registry import SliceRegistry

log = logging.getLogger(__name__)

_MAX_T2C_ATTEMPTS = 3

# ── Node label colours for graph visualization ────────────────────────────────

_LABEL_COLORS: dict[str, str] = {
    "Station": "#4A90D9",
    "Route": "#E67E22",
    "Trip": "#2ECC71",
    "StopTime": "#9B59B6",
    "Date": "#1ABC9C",
    "Interruption": "#E74C3C",
    "Pathway": "#F39C12",
    "Level": "#16A085",
}
_DEFAULT_COLOR = "#95A5A6"


# ── Result container ──────────────────────────────────────────────────────────


@dataclass
class QuestionResult:
    """All intermediate and final outputs for a single pipeline question."""

    question: str
    planner_output: PlannerOutput
    resolutions: AnchorResolutions
    t2c_output: Text2CypherOutput | None
    t2c_cot: str | None          # chain-of-thought from first QueryWriter attempt
    subgraph_output: SubgraphOutput | None
    raw_subgraph: RawSubgraph | None   # raw nodes+edges for visualization
    narration_output: NarrationOutput  # static pipeline answer
    agent_narration: NarrationOutput   # agentic pipeline answer
    invocation_time: datetime = field(default_factory=lambda: datetime.now(UTC))


# ── Pipeline runner ───────────────────────────────────────────────────────────


def run_question(
    question: str,
    db: Neo4jManager,
    llm_config: LLMConfig,
    registry: SliceRegistry,
    planner: Planner,
    narration_agent: NarrationAgent,
) -> QuestionResult:
    """
    Execute the full LLM pipeline for *question* and return all intermediate
    outputs.  Called by run_and_display() for Q2+ compact cells, and by the
    Q1 walkthrough cells individually via the display_* helpers.
    """
    from src.common.logger import get_logger
    from src.llm.agent import AgentOrchestrator
    from src.llm.anchor_clarifier import AnchorClarifier

    _log = get_logger(__name__)
    invocation_time = datetime.now(UTC)

    # ── Step 1: Planner ───────────────────────────────────────────────────────
    planner_output = planner.run(question)

    # ── Step 2: Anchor Resolution ─────────────────────────────────────────────
    resolver = AnchorResolver(db=db, invocation_time=invocation_time)
    resolutions = resolver.resolve(planner_output.anchors)

    # ── Step 3a: Subgraph ─────────────────────────────────────────────────────
    subgraph_output: SubgraphOutput | None = None
    raw_subgraph: RawSubgraph | None = None

    if planner_output.path in {"subgraph", "both"}:
        # Run HopExpander separately so we have RawSubgraph for visualization.
        expander = HopExpander(db=db)
        raw_subgraph = expander.expand(resolutions, planner_output.domain)
        # SubgraphBuilder runs ContextSerializer on top of the same expansion.
        builder = SubgraphBuilder(db=db)
        subgraph_output = builder.run(
            planner_output, resolutions, resolver_config=resolver.config
        )

    # ── Step 3b: Text-to-Cypher ───────────────────────────────────────────────
    t2c_output: Text2CypherOutput | None = None
    t2c_cot: str | None = None

    if planner_output.path in {"text2cypher", "both"}:
        schema_slice = registry.get(planner_output.schema_slice_key)
        refinement_errors: list[str] = []
        all_validation_notes: list[str] = []

        for attempt in range(1, _MAX_T2C_ATTEMPTS + 1):
            qw_output = run_query_writer(
                question,
                planner_output,
                llm_config,
                schema_slice=schema_slice,
                resolved_anchors=resolutions.as_flat_dict(),
                refinement_errors=refinement_errors or None,
                use_gds=planner_output.use_gds,
            )
            if t2c_cot is None:
                t2c_cot = qw_output.cot_comments

            val_result = validate_and_log_cypher(
                qw_output.cypher_query,
                schema_slice,
                schema_slice.property_registry,
                db.driver,
                _log,
            )
            if val_result.valid:
                t2c_output = Text2CypherOutput(
                    cypher=qw_output.cypher_query,
                    results=val_result.results or [],
                    domain=planner_output.domain,
                    attempt_count=attempt,
                    validation_notes=all_validation_notes,
                    success=True,
                )
                break
            all_validation_notes.extend(val_result.errors)
            refinement_errors = val_result.errors
        else:
            t2c_output = Text2CypherOutput(
                cypher="",
                results=[],
                domain=planner_output.domain,
                attempt_count=_MAX_T2C_ATTEMPTS,
                validation_notes=all_validation_notes,
                success=False,
            )

    # ── Step 4: Static Narration ──────────────────────────────────────────────
    narration_output = narration_agent.run(
        question,
        planner_output,
        t2c_output=t2c_output,
        subgraph_output=subgraph_output,
        resolutions=resolutions,
    )

    # ── Step 5: Agentic Pipeline ──────────────────────────────────────────────
    clarifier = AnchorClarifier(db, llm_config)
    orchestrator = AgentOrchestrator(
        db=db,
        llm_config=llm_config,
        registry=registry,
        clarifier=clarifier,
        narration_agent=narration_agent,
    )
    try:
        _, _, agent_narration = orchestrator.run(
            question, planner_output, resolutions, resolver, invocation_time
        )
    except Exception as exc:
        log.warning("Agentic pipeline failed: %s", exc)
        agent_narration = NarrationOutput(
            answer="(agentic pipeline unavailable)",
            mode="degraded",
            sources_used=[],
            success=False,
            failure_reason=str(exc),
        )

    return QuestionResult(
        question=question,
        planner_output=planner_output,
        resolutions=resolutions,
        t2c_output=t2c_output,
        t2c_cot=t2c_cot,
        subgraph_output=subgraph_output,
        raw_subgraph=raw_subgraph,
        narration_output=narration_output,
        agent_narration=agent_narration,
        invocation_time=invocation_time,
    )


# ── Display helpers ───────────────────────────────────────────────────────────


def display_model_info(llm_config: LLMConfig) -> None:
    """Print a concise model info banner. Shown once at the top of the notebook."""
    w = 56
    print("╔" + "═" * w + "╗")
    print(f"║  {'Model':<12}: {llm_config.llm_model:<{w - 16}} ║")
    print(f"║  {'Provider':<12}: {llm_config.llm_provider:<{w - 16}} ║")
    tok = f"{llm_config.llm_max_tokens} (pipeline) / {llm_config.llm_narration_max_tokens} (narration)"
    print(f"║  {'Token budget':<12}: {tok:<{w - 16}} ║")
    print("╚" + "═" * w + "╝")


def display_question_header(question: str, n: int) -> None:
    sep = "═" * 56
    print(f"\n{sep}")
    print(f"  Q{n}: {question}")
    print(f"{sep}\n")


def display_planner_step(planner_output: PlannerOutput) -> None:
    print("── Step 1 · Planner ─────────────────────────────────────")
    print(f"  domain         : {planner_output.domain!r}")
    print(f"  path           : {planner_output.path!r}")
    print(f"  path_reasoning : {planner_output.path_reasoning!r}")
    print(f"  anchor_notes   : {planner_output.anchor_notes!r}")
    print(f"  use_gds        : {planner_output.use_gds}")
    print("  anchors:")
    print(f"    routes         : {planner_output.anchors.routes}")
    print(f"    stations       : {planner_output.anchors.stations}")
    print(f"    dates          : {planner_output.anchors.dates}")
    print(f"    pathway_nodes  : {planner_output.anchors.pathway_nodes}")
    print(f"    levels         : {planner_output.anchors.levels}")
    if planner_output.rejected:
        print(f"\n  ⚠ REJECTED: {planner_output.rejection_message}")
    if planner_output.parse_warning:
        print(f"\n  ⚠ parse_warning: {planner_output.parse_warning}")


def display_anchor_step(resolutions: AnchorResolutions) -> None:
    print("\n── Step 2 · Anchor Resolution ───────────────────────────")
    flat = resolutions.as_flat_dict()
    if flat:
        for mention, ids in flat.items():
            print(f"  {mention!r:30s} → {ids}")
    else:
        print("  (no anchors resolved)")
    if resolutions.failed:
        print(f"  failed: {resolutions.failed}")


def display_subgraph_step(subgraph_output: SubgraphOutput | None) -> None:
    print("\n── Step 3a · Subgraph ───────────────────────────────────")
    if subgraph_output is None:
        print("  (path not taken — Planner selected text2cypher)")
        return
    print(f"  success    : {subgraph_output.success}")
    if not subgraph_output.success:
        print(f"  failure    : {subgraph_output.failure_reason}")
        return
    print(f"  node_count : {subgraph_output.node_count}  trimmed: {subgraph_output.trimmed}")
    print(f"  provenance : {len(subgraph_output.provenance_nodes)} node(s)")
    if subgraph_output.context:
        excerpt = subgraph_output.context[:500]
        print(f"\n  Context (excerpt):\n{excerpt}{'...' if len(subgraph_output.context) > 500 else ''}")


def display_t2c_step(
    t2c_output: Text2CypherOutput | None, cot: str | None
) -> None:
    print("\n── Step 3b · Text-to-Cypher ─────────────────────────────")
    if t2c_output is None:
        print("  (path not taken — Planner selected subgraph)")
        return
    print(f"  attempts : {t2c_output.attempt_count}   success: {t2c_output.success}")
    if t2c_output.validation_notes:
        for note in t2c_output.validation_notes:
            print(f"  validation: {note}")
    if t2c_output.cypher:
        print(f"\n  Cypher:\n{t2c_output.cypher}")
    if cot:
        print(f"\n  Chain-of-Thought:\n{cot}")
    print(f"\n  Results: {t2c_output.results}")


def display_narration_step(
    narration_output: NarrationOutput, label: str = "Static"
) -> None:
    print(f"\n── Step 4 · Narration [{label}] ──────────────────────────")
    print(f"  mode    : {narration_output.mode}")
    print(f"  sources : {narration_output.sources_used}")
    if not narration_output.success:
        print(f"  failure : {narration_output.failure_reason}")
    print(f"\n  Answer:\n{'═' * 56}")
    print(narration_output.answer)
    print("═" * 56)


def display_agentic_step(agent_narration: NarrationOutput) -> None:
    print("\n── Agentic Pipeline ─────────────────────────────────────")
    print(f"  mode    : {agent_narration.mode}")
    print(f"  sources : {agent_narration.sources_used}")
    if agent_narration.agent_trace:
        history = agent_narration.agent_trace.get("tool_call_history", [])
        print(f"  tools   : {len(history)} call(s)")
        for i, call in enumerate(history, 1):
            if isinstance(call, dict):
                name = call.get("tool", "?")
                out = call.get("output", {})
                summary = out.get("summary", out.get("error", ""))
            else:
                name = summary = "?"
            print(f"    [{i}] {name} — {summary}")
    print(f"\n  Answer:\n{'═' * 56}")
    print(agent_narration.answer)
    print("═" * 56)


def display_comparison_todo() -> None:
    print("\n" + "─" * 56)
    print("  TODO: fill in comparison notes after running")
    print("  Static vs Agentic:")
    print("    - Mode:    static=?  agentic=?")
    print("    - Sources: static=?  agentic=?")
    print("    - Answer quality: ...")
    print("─" * 56)


# ── Graph visualization ───────────────────────────────────────────────────────


def visualize_subgraph(raw_subgraph: RawSubgraph, title: str = "Subgraph") -> None:
    """
    Render *raw_subgraph* as a matplotlib figure.

    Node colours are keyed on the primary Neo4j label.  Anchor nodes are
    drawn with a bold border.  Edge labels show the relationship type.
    """
    if not raw_subgraph or not raw_subgraph.nodes:
        print("  (no subgraph nodes to visualize)")
        return

    G: nx.DiGraph = nx.DiGraph()

    node_color_map: dict[str, str] = {}
    node_label_map: dict[str, str] = {}
    anchor_ids = raw_subgraph.anchor_element_ids

    for node in raw_subgraph.nodes:
        primary = next(
            (lbl for lbl in node.labels if lbl in _LABEL_COLORS),
            node.labels[0] if node.labels else "Node",
        )
        display_name = (
            node.props.get("stop_name")
            or node.props.get("route_short_name")
            or node.props.get("route_long_name")
            or node.props.get("trip_id", "")[:10]
            or node.props.get("date", "")
            or node.element_id[-8:]
        )
        G.add_node(node.element_id, primary_label=primary, is_anchor=node.element_id in anchor_ids)
        node_label_map[node.element_id] = f"{primary}\n{display_name}"
        node_color_map[node.element_id] = _LABEL_COLORS.get(primary, _DEFAULT_COLOR)

    for rel in raw_subgraph.rels:
        if rel.from_element_id in G and rel.to_element_id in G:
            G.add_edge(rel.from_element_id, rel.to_element_id, rel_type=rel.rel_type)

    if G.number_of_nodes() == 0:
        print("  (subgraph is empty after filtering)")
        return

    fig, ax = plt.subplots(figsize=(14, 8))
    pos = nx.spring_layout(G, seed=42, k=2.5)

    # Draw non-anchor nodes
    non_anchor = [n for n in G.nodes if not G.nodes[n].get("is_anchor")]
    anchor_nodes = [n for n in G.nodes if G.nodes[n].get("is_anchor")]
    non_anchor_colors = [node_color_map[n] for n in non_anchor]
    anchor_colors = [node_color_map[n] for n in anchor_nodes]

    nx.draw_networkx_nodes(G, pos, nodelist=non_anchor, node_color=non_anchor_colors,
                           node_size=900, ax=ax, alpha=0.85)
    nx.draw_networkx_nodes(G, pos, nodelist=anchor_nodes, node_color=anchor_colors,
                           node_size=1400, ax=ax, alpha=0.95,
                           linewidths=2.5, edgecolors="#222")

    nx.draw_networkx_labels(G, pos, labels=node_label_map, font_size=6, ax=ax)
    nx.draw_networkx_edges(
        G, pos, edge_color="#555", arrows=True, arrowsize=14,
        connectionstyle="arc3,rad=0.08", ax=ax, width=1.2,
    )
    edge_labels = {(u, v): d["rel_type"] for u, v, d in G.edges(data=True)}
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=5, ax=ax)

    # Legend — only labels that appear in this subgraph
    present_labels = {G.nodes[n]["primary_label"] for n in G.nodes}
    legend_patches = [
        mpatches.Patch(color=c, label=lbl)
        for lbl, c in _LABEL_COLORS.items()
        if lbl in present_labels
    ]
    if legend_patches:
        ax.legend(handles=legend_patches, loc="upper left", fontsize=8, framealpha=0.8)

    ax.set_title(
        f"{title}  —  {G.number_of_nodes()} nodes · {G.number_of_edges()} edges"
        + ("  (anchor nodes have bold border)" if anchor_nodes else ""),
        fontsize=10,
    )
    ax.axis("off")
    plt.tight_layout()
    plt.show()


# ── Compound helper for Q2+ cells ─────────────────────────────────────────────


def run_and_display(
    question: str,
    n: int,
    db: Neo4jManager,
    llm_config: LLMConfig,
    registry: SliceRegistry,
    planner: Planner,
    narration_agent: NarrationAgent,
) -> QuestionResult:
    """
    Run the full pipeline for *question* and display every step.

    Used for Q2+ cells where a single call keeps the notebook tidy while
    still producing all intermediate outputs.
    """
    result = run_question(question, db, llm_config, registry, planner, narration_agent)

    display_question_header(question, n)
    display_planner_step(result.planner_output)
    display_anchor_step(result.resolutions)
    display_subgraph_step(result.subgraph_output)
    if result.raw_subgraph and result.raw_subgraph.nodes:
        visualize_subgraph(result.raw_subgraph, title=f"Q{n} Subgraph")
    display_t2c_step(result.t2c_output, result.t2c_cot)
    display_narration_step(result.narration_output, label="Static")
    display_agentic_step(result.agent_narration)

    return result
