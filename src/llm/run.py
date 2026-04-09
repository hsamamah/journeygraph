# src/llm/run.py
"""
JourneyGraph LLM Query Pipeline — run script.

Entry point for natural language querying over the WMATA knowledge graph.
Runs the Planner stage, shared anchor resolution, and where the path warrants
it, the Subgraph path (HopExpander → ContextSerializer). Anchor resolution
runs once after the Planner for all non-rejected queries — resolved IDs are
shared across both the Subgraph and Text2Cypher paths. Downstream pipeline
stages (Query Writer, Cypher Validator, Narration Agent) are not yet
implemented.

Usage:
    # Single query (default)
    python -m src.llm.run "how many trips were cancelled on the red line yesterday"

    # Single query with full decision trace
    python -m src.llm.run "how many trips were cancelled"

    # Smoke test — four hardcoded queries, always runs in strict mode
    python -m src.llm.run --demo

    # Interactive REPL
    python -m src.llm.run --repl
    python -m src.llm.run --repl

    # Hard-fail on any schema validation warning
    python -m src.llm.run "..." --strict

Startup sequence (same for all modes):
    1. get_llm_config()   — hard fail if ANTHROPIC_API_KEY missing
    2. Neo4jManager()     — hard fail if DB unreachable; connection held
                           open for subgraph queries during session
    3. SliceRegistry()    — validates slices against live graph
    4. Planner()          — builds LLM instance
    5. Enter selected mode

The SliceRegistry validation (DB-touching) always completes before any
LLM call is made, so a misconfigured database never wastes API tokens.
The Neo4j connection is held open across queries and closed on exit.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import logging
import sys
from typing import TYPE_CHECKING

from src.common.config import get_llm_config
from src.common.logger import get_logger
from src.common.neo4j_tools import Neo4jManager
from src.llm.anchor_resolver import AnchorResolver
from src.llm.disambiguation_strategies import TypeWeightedCoherenceStrategy
from src.llm.planner import Planner
from src.llm.slice_registry import SliceRegistry
from src.llm.subgraph_builder import SubgraphBuilder
from src.llm.cypher_validator import validate_and_log_cypher
from src.llm.query_writer import run_query_writer

if TYPE_CHECKING:
    from src.llm.planner_output import PlannerOutput
    from src.llm.subgraph_output import SubgraphOutput

log = get_logger(__name__)

# ── Demo queries ──────────────────────────────────────────────────────────────
# One per domain plus one rejection case. --demo always runs strict mode
# so any schema validation warning surfaces immediately as a hard failure.

_DEMO_QUERIES: list[tuple[str, str]] = [
    (
        "how many trips were cancelled on the red line yesterday",
        "transfer_impact",
    ),
    (
        "is the elevator at Metro Center out of service",
        "accessibility",
    ),
    (
        "are there any delays propagating from Gallery Place",
        "delay_propagation",
    ),
    (
        "what is the weather like in DC today",
        "rejection",
    ),
]


# ── Argument parsing ──────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="src.llm.run",
        description="JourneyGraph LLM query pipeline — Planner + Subgraph stages",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "query",
        nargs="?",
        metavar="QUERY",
        help="Natural language query to run (default mode).",
    )
    mode_group.add_argument(
        "--demo",
        action="store_true",
        help=(
            "Run four hardcoded smoke test queries (one per domain + rejection). "
            "Always runs in strict mode regardless of --strict flag."
        ),
    )
    mode_group.add_argument(
        "--repl",
        action="store_true",
        help="Start an interactive query loop. Exit with 'quit', 'exit', or Ctrl+C.",
    )

    parser.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Promote SliceRegistry validation warnings to hard failures. "
            "Applies to missing node labels and relationship types. "
            "Always active in --demo mode."
        ),
    )

    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=1,
        metavar="K",
        help=(
            "Maximum candidates fetched per anchor mention from the full-text "
            "index. K=1 (default) uses the top result directly — no "
            "disambiguation runs. K>1 enables graph-assisted disambiguation "
            "via the configured strategy. Use with --strategy for A/B testing."
        ),
    )

    parser.add_argument(
        "--strategy",
        choices=["topk", "coherence"],
        default="topk",
        help=(
            "Disambiguation strategy. 'topk' (default) takes the "
            "highest-scoring candidate per mention — no graph query. "
            "'coherence' uses typed relationship weights across all anchor "
            "types to pick the most coherent candidate set. Ignored when "
            "--candidate-limit=1."
        ),
    )

    return parser.parse_args(argv)


# ── Output formatting — Planner ───────────────────────────────────────────────


def _fmt_planner_compact(output: PlannerOutput) -> str:
    if output.rejected:
        return f"rejected — {output.rejection_message}"

    anchors = output.anchors
    anchor_parts = []
    if anchors.stations:
        anchor_parts.append(f"stations={anchors.stations}")
    if anchors.routes:
        anchor_parts.append(f"routes={anchors.routes}")
    if anchors.dates:
        anchor_parts.append(f"dates={anchors.dates}")
    if anchors.pathway_nodes:
        anchor_parts.append(f"pathway_nodes={anchors.pathway_nodes}")

    anchor_str = "  ".join(anchor_parts) if anchor_parts else "(no anchors extracted)"
    if output.parse_warning:
        anchor_str += "  ⚠ parse_warning set"

    return f"domain={output.domain}  path={output.path}\nanchors: {anchor_str}"


def _fmt_planner_verbose(
    output: PlannerOutput,
    stage1_scores: dict[str, float] | None = None,
) -> str:
    lines: list[str] = []

    if stage1_scores is not None:
        lines.append("─── Stage 1 — domain classifier ────────────────────────")
        for domain, score in sorted(stage1_scores.items()):
            marker = (
                "  ← selected"
                if (not output.rejected and domain == output.domain)
                else ""
            )
            lines.append(f"  {domain:<22} {score:.4f}{marker}")

    lines.append("─── PlannerOutput ───────────────────────────────────────")
    lines.append(f"  domain           : {output.domain!r}")
    lines.append(f"  path             : {output.path!r}")
    lines.append(f"  schema_slice_key : {output.schema_slice_key!r}")
    lines.append(f"  rejected         : {output.rejected}")
    lines.append(f"  rejection_message: {output.rejection_message!r}")
    lines.append(f"  path_reasoning   : {output.path_reasoning!r}")
    lines.append(f"  anchor_notes     : {output.anchor_notes!r}")

    if output.parse_warning:
        lines.append(f"  ⚠ parse_warning  : {output.parse_warning}")
    else:
        lines.append("  parse_warning    : None")

    lines.append("  anchors:")
    lines.append(f"    stations       : {output.anchors.stations}")
    lines.append(f"    routes         : {output.anchors.routes}")
    lines.append(f"    dates          : {output.anchors.dates}")
    lines.append(f"    pathway_nodes  : {output.anchors.pathway_nodes}")

    return "\n".join(lines)


# ── Output formatting — Subgraph ──────────────────────────────────────────────


def _fmt_subgraph_compact(sub: SubgraphOutput) -> str:
    if not sub.success:
        return f"subgraph: failed — {sub.failure_reason}"

    trim_note = f"  trimmed={sub.trimmed}" if sub.trimmed else ""
    return (
        f"subgraph: domain={sub.domain}  nodes={sub.node_count}"
        f"  anchors={sub.anchor_resolutions}{trim_note}"
    )


def _fmt_subgraph_verbose(sub: SubgraphOutput) -> str:
    lines: list[str] = []
    lines.append("─── SubgraphOutput ──────────────────────────────────────")
    lines.append(f"  domain            : {sub.domain!r}")
    lines.append(f"  success           : {sub.success}")
    lines.append(f"  failure_reason    : {sub.failure_reason!r}")
    lines.append(f"  node_count        : {sub.node_count}")
    lines.append(f"  trimmed           : {sub.trimmed}")
    lines.append(f"  anchor_resolutions: {sub.anchor_resolutions}")
    lines.append(f"  resolver_config   : {sub.resolver_config}")
    lines.append(f"  provenance_nodes  : {len(sub.provenance_nodes)} node(s)")
    lines.append("─── Subgraph Context Block ───────────────────────────────")
    if sub.context:
        lines.append(sub.context)
    else:
        lines.append("  (empty)")
    return "\n".join(lines)


# ── Query execution ───────────────────────────────────────────────────────────

def _run_query(
    planner: Planner,
    db: Neo4jManager,
    query: str,
    registry: SchemaRegistry,
    *,
    candidate_limit: int = 1,
    strategy: str = "topk",
    label: str | None = None,
) -> tuple[PlannerOutput, SubgraphOutput | None]:
    """
    Execute a single query through the Planner and, where the path
    warrants it, the Subgraph path.

    invocation_time is captured once here so relative date expressions
    (yesterday, last Tuesday) resolve consistently within the same
    pipeline invocation.

    Args:
        planner:         Initialised Planner instance.
        db:              Live Neo4jManager — held open across queries.
        query:           Raw query string.
        candidate_limit: Max candidates per mention from full-text index.
                         1 = baseline, no disambiguation.
        strategy:        'topk' or 'coherence'. Ignored when
                         candidate_limit=1.
        label:           Optional prefix for --demo mode e.g. '[1/4]'.

    Returns:
        (PlannerOutput, SubgraphOutput | None)
        SubgraphOutput is None when the query is rejected, zero anchors
        resolve, or path is text2cypher only.
    """
    invocation_time = datetime.now(UTC)
    prefix = f"{label}  " if label else ""

    stage1_result = planner.classify_only(query)
    stage1_scores = stage1_result.scores

    planner_output = planner.run(query)

    # ── Planner output ────────────────────────────────────────────────────────
    header = f"\n{'═' * 56}\n{prefix}Query: {query!r}\n{'═' * 56}"
    print(header)
    print(_fmt_planner_verbose(planner_output, stage1_scores=stage1_scores))

    if planner_output.rejected:
        return planner_output, None

    # ── Anchor resolution — shared pre-fork step ──────────────────────────────
    # Runs for every non-rejected query regardless of path. Both Text2Cypher
    # and Subgraph receive the same resolved IDs. Zero-anchor failure stops
    # the pipeline here — the user is asked to restate with clearer entities.
    disambiguation_strategy = (
        TypeWeightedCoherenceStrategy() if strategy == "coherence" else None
    )
    resolver = AnchorResolver(
        db=db,
        invocation_time=invocation_time,
        strategy=disambiguation_strategy,
        candidate_limit=candidate_limit,
    )
    resolutions = resolver.resolve(planner_output.anchors)

    if not resolutions.any_resolved:
        log.warning("run | zero anchors resolved | query=%r", query)
        print(
            "\nNo entities could be resolved from your query. "
            "Please restate it with a specific station, route, or date "
            "(e.g. 'Metro Center', 'Red Line', 'yesterday')."
        )
        return planner_output, None

    log.info(
        "run | anchors resolved | config=%s | %s",
        resolver.config,
        resolutions.as_flat_dict(),
    )
    
    # ── Query Writer path ─────────────────────────────────────────────────────────
    if getattr(planner_output, "path", None) in {"text2cypher", "both"}:
        query_writer_output = run_query_writer(query, planner_output)
        print("\n[Query Writer Output]")
        print("Cypher Query:\n", query_writer_output.cypher_query)
        print("Chain-of-Thought Comments:\n", query_writer_output.cot_comments)

        # Validate Cypher
        schema_slice = registry.get(planner_output.schema_slice_key)
        property_registry = schema_slice.property_registry
        cypher_validator_result = validate_and_log_cypher(query_writer_output.cypher_query, schema_slice, schema_slice.property_registry, db.driver, log)
        if cypher_validator_result.errors:
            # print("Cypher Validator Errors:", cypher_validator_result.errors)
            print("Cypher Validator Errors - see logs for details.")
        else:
            print("Cypher Validator: Query is valid.")

    # ── Subgraph path ─────────────────────────────────────────────────────────
    # sub_output: SubgraphOutput | None = None

    # if planner_output.path in {"subgraph", "both"}:
    #     builder = SubgraphBuilder(db=db)
    #     sub_output = builder.run(
    #         planner_output,
    #         resolutions,
    #         resolver_config=resolver.config,
    #     )
    #     print(_fmt_subgraph_verbose(sub_output))

    # return planner_output, sub_output


# ── Modes ─────────────────────────────────────────────────────────────────────

def _mode_default(
    planner: Planner,
    db: Neo4jManager,
    query: str,
    registry: SchemaRegistry,
    *,
    candidate_limit: int,
    strategy: str,
) -> None:
    _run_query(planner, db, registry, query, candidate_limit=candidate_limit, strategy=strategy)


def _mode_demo(
    planner: Planner,
    db: Neo4jManager,
    registry: SchemaRegistry,
    *,
    candidate_limit: int,
    strategy: str,
) -> None:
    """
    Four hardcoded smoke test queries.
    Always runs in strict mode (enforced at registry construction by caller).
    """
    print(f"Running {len(_DEMO_QUERIES)} demo queries in strict mode...\n")
    results: list[tuple[str, PlannerOutput]] = []

    for i, (query, expected_domain) in enumerate(_DEMO_QUERIES, 1):
        label = f"[{i}/{len(_DEMO_QUERIES)}]"
        planner_output, _ = _run_query(
            planner,
            db,
            query,
            candidate_limit=candidate_limit,
            strategy=strategy,
            label=label,
        )
        results.append((expected_domain, planner_output))

    print(f"\n{'═' * 56}")
    print("Demo summary:")
    all_passed = True
    for (expected, output), (query, expected_domain) in zip(results, _DEMO_QUERIES, strict=True):
        if expected == "rejection":
            passed = output.rejected
        else:
            passed = not output.rejected and output.domain == expected
        status = "✅" if passed else "❌"
        if not passed:
            all_passed = False
        print(f"  {status}  {query[:52]!r:<54}  expected={expected}")

    print(f"\n{'All passed' if all_passed else 'Some checks failed'}")


def _mode_repl(
    planner: Planner,
    db: Neo4jManager,
    *,
    candidate_limit: int,
    strategy: str,
) -> None:
    """
    Interactive query loop.
    Exits cleanly on 'quit', 'exit', or Ctrl+C / Ctrl+D.
    """
    print("JourneyGraph query pipeline — interactive mode")
    print(f"Resolver: candidate_limit={candidate_limit}  strategy={strategy}")
    print("Type a question, or 'quit' to exit.\n")

    while True:
        try:
            query = input("query> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not query:
            continue
        if query.lower() in {"quit", "exit"}:
            print("Exiting.")
            break

        _run_query(
            planner,
            db,
            query,
            candidate_limit=candidate_limit,
            strategy=strategy,
        )
        print()


# ── Startup ───────────────────────────────────────────────────────────────────


def _startup(*, strict: bool) -> tuple[Planner, Neo4jManager]:
    """
    Initialise the full pipeline stack.

    Returns (Planner, Neo4jManager). The Neo4j connection is held open
    across queries for the Subgraph path and closed by main() on exit
    via the finally block.

    Sequencing:
      1. LLM config   — hard fail if ANTHROPIC_API_KEY missing
      2. Neo4j        — hard fail if DB unreachable
      3. SliceRegistry— validates slices against live graph
      4. Planner      — builds LLM instance
    """
    llm_config = get_llm_config()
    log.info(
        "LLM config loaded — provider=%s model=%s max_tokens=%d",
        llm_config.llm_provider,
        llm_config.llm_model,
        llm_config.llm_max_tokens,
    )

    db = Neo4jManager()
    log.info("Neo4j connected")

    registry = SliceRegistry(db, strict=strict)
    # Connection remains open — AnchorResolver and SubgraphBuilder need it
    # at query time.

    planner = Planner(registry, llm_config, strict=strict)
    return planner, db, registry


# ── Entry point ───────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    # Set up global logging configuration to show all internal module logs
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    args = _parse_args(argv)
    strict = args.strict or args.demo
    candidate_limit: int = args.candidate_limit
    strategy: str = args.strategy

    if candidate_limit > 1:
        log.info(
            "Resolver config — candidate_limit=%d strategy=%s",
            candidate_limit,
            strategy,
        )

    try:
        planner, db, registry = _startup(strict=strict)
    except (OSError, RuntimeError) as exc:
        log.error("Startup failed: %s", exc)
        sys.exit(1)

    try:
        if args.demo:
            _mode_demo(
                planner,
                db,
                registry,
                candidate_limit=candidate_limit,
                strategy=strategy,
            )
        elif args.repl:
            _mode_repl(
                planner,
                db,
                registry,
                candidate_limit=candidate_limit,
                strategy=strategy,
            )
        elif args.query:
            _mode_default(
                planner,
                db,
                registry,
                args.query,
                candidate_limit=candidate_limit,
                strategy=strategy,
            )
        else:
            _parse_args(["--help"])
    finally:
        db.close()
        log.info("Neo4j connection closed")


if __name__ == "__main__":
    main()
