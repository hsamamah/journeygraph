# src/llm/run.py
"""
JourneyGraph LLM Query Pipeline — run script.

Entry point for natural language querying over the WMATA knowledge graph.
Runs the full pipeline: Planner → Anchor Resolution → Clarification →
Query Writer + Cypher Validator (text2cypher/both paths) → Subgraph path →
Narration Agent. Anchor resolution runs once after the Planner for all
non-rejected queries — resolved IDs are shared across both the Subgraph and
Text2Cypher paths.

Usage:
    # Single query (default)
    python -m src.llm.run "how many trips were cancelled on the red line yesterday"

    # Single query with full decision trace
    python -m src.llm.run "how many trips were cancelled"

    # Smoke test — four hardcoded queries, always runs in strict mode
    python -m src.llm.run --demo

    # Interactive REPL
    python -m src.llm.run --repl

    # Hard-fail on any schema validation warning
    python -m src.llm.run "..." --strict

Startup sequence (same for all modes):
    1. get_llm_config()   — hard fail if ANTHROPIC_API_KEY missing
    2. Neo4jManager()     — hard fail if DB unreachable; connection held
                           open for subgraph queries during session
    3. SliceRegistry()    — validates slices against live graph
    4. Planner()          — builds LLM instance (Stage 2)
    5. NarrationAgent()   — builds LLM instance (narration)
    6. AnchorClarifier()  — fetches station/route catalogue from graph once
    7. Enter selected mode

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

from neo4j.exceptions import Neo4jError

from src.common.config import LLMConfig, get_llm_config
from src.common.logger import get_logger
from src.common.neo4j_tools import Neo4jManager
from src.llm.agent import AgentOrchestrator
from src.llm.anchor_clarifier import AnchorClarifier
from src.llm.anchor_resolver import AnchorResolver
from src.llm.disambiguation_strategies import TypeWeightedCoherenceStrategy
from src.llm.narration_agent import NarrationAgent
from src.llm.planner import Planner
from src.llm.slice_registry import SliceRegistry
from src.llm.subgraph_builder import SubgraphBuilder
from src.llm.cypher_validator import validate_and_log_cypher
from src.llm.query_writer import run_query_writer
from src.llm.text2cypher_output import Text2CypherOutput

if TYPE_CHECKING:
    from src.llm.narration_output import NarrationOutput
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

    parser.add_argument(
        "--mode",
        choices=["static", "agentic"],
        default="static",
        help=(
            "Pipeline execution mode. 'static' (default) runs the fixed "
            "Planner → QueryWriter/Subgraph → Narration chain. "
            "'agentic' replaces the retrieval step with a Claude agent loop "
            "that selects tools dynamically (max 5 iterations). "
            "Both modes share the Planner + AnchorResolver pre-step and the "
            "NarrationAgent terminal step. Use --demo or --repl with either mode."
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


def _fmt_planner_verbose(output: PlannerOutput) -> str:
    lines: list[str] = []

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


# ── Output formatting — Narration ────────────────────────────────────────────


def _fmt_narration(output: NarrationOutput) -> str:
    lines: list[str] = []
    lines.append("─── NarrationOutput ─────────────────────────────────────")
    lines.append(f"  mode         : {output.mode!r}")
    lines.append(f"  domain       : {output.domain!r}")
    lines.append(f"  sources_used : {output.sources_used}")
    lines.append(f"  success      : {output.success}")
    if output.failure_reason:
        lines.append(f"  ⚠ failure    : {output.failure_reason}")
    lines.append("─── Pipeline Trace ──────────────────────────────────────")
    trace = output.trace
    planner_t = trace.get("planner", {})
    lines.append(
        f"  planner      : domain={planner_t.get('domain')!r}"
        f"  path={planner_t.get('path')!r}"
    )
    lines.append(f"    path_reasoning : {planner_t.get('path_reasoning')!r}")
    lines.append(f"    anchor_notes   : {planner_t.get('anchor_notes')!r}")
    if planner_t.get("parse_warning"):
        lines.append(f"    ⚠ parse_warning: {planner_t.get('parse_warning')}")
    t2c_t = trace.get("text2cypher")
    if t2c_t:
        lines.append(
            f"  text2cypher  : success={t2c_t.get('success')}"
            f"  attempts={t2c_t.get('attempt_count')}"
        )
        if t2c_t.get("validation_notes"):
            for note in t2c_t["validation_notes"]:
                lines.append(f"    validator  : {note}")
    else:
        lines.append("  text2cypher  : not run")
    sub_t = trace.get("subgraph")
    if sub_t:
        lines.append(
            f"  subgraph     : success={sub_t.get('success')}"
            f"  nodes={sub_t.get('node_count')}"
            f"  trimmed={sub_t.get('trimmed')}"
        )
        lines.append(f"    anchors    : {sub_t.get('anchor_resolutions')}")
    else:
        lines.append("  subgraph     : not run")
    lines.append("─── Answer ──────────────────────────────────────────────")
    if output.answer:
        lines.append(output.answer)
    else:
        lines.append("  (no answer — LLM call failed)")
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
    narration_agent: NarrationAgent,
    db: Neo4jManager,
    query: str,
    *,
    registry: SliceRegistry,
    clarifier: AnchorClarifier,
    llm_config: LLMConfig,
    candidate_limit: int = 1,
    strategy: str = "topk",
    label: str | None = None,
) -> tuple[PlannerOutput, Text2CypherOutput | None, SubgraphOutput | None, NarrationOutput | None]:
    """
    Execute a single query through the full pipeline.

    Stages: Planner → Anchor Resolution → Clarification (if needed) →
    Query Writer + Cypher Validator (text2cypher/both paths) →
    Subgraph path → Narration Agent.

    invocation_time is captured once here so relative date expressions
    (yesterday, last Tuesday) resolve consistently within the same
    pipeline invocation.

    Args:
        planner:          Initialised Planner instance.
        narration_agent:  Initialised NarrationAgent instance.
        db:               Live Neo4jManager — held open across queries.
        query:            Raw query string.
        registry:         SliceRegistry — used by Query Writer path for
                          property whitelist validation.
        llm_config:       LLMConfig — passed to QueryWriter for model/key.
        clarifier:        AnchorClarifier — fires only when station/route
                          anchors fail Lucene lookup.
        candidate_limit:  Max candidates per mention from full-text index.
                          1 = baseline, no disambiguation.
        strategy:         'topk' or 'coherence'. Ignored when
                          candidate_limit=1.
        label:            Optional prefix for --demo mode e.g. '[1/4]'.

    Returns:
        (PlannerOutput, Text2CypherOutput | None, SubgraphOutput | None, NarrationOutput | None)
        Text2CypherOutput is None when the planner routes to subgraph-only path.
        NarrationOutput is None only when the query is rejected or a
        Neo4j error occurs during anchor resolution.
    """
    invocation_time = datetime.now(UTC)
    prefix = f"{label}  " if label else ""

    planner_output = planner.run(query)

    # ── Planner output ────────────────────────────────────────────────────────
    header = f"\n{'═' * 56}\n{prefix}Query: {query!r}\n{'═' * 56}"
    print(header)
    print(_fmt_planner_verbose(planner_output))

    if planner_output.rejected:
        return planner_output, None, None, None

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
    try:
        resolutions = resolver.resolve(planner_output.anchors)
    except Neo4jError as exc:
        log.error(
            "run | anchor resolution failed | %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        print(f"\nDatabase error during anchor resolution: {exc}")
        return planner_output, None, None, None

    # ── Anchor clarification — fires only on station/route failures ──────────────
    # Silent repair pass: maps failed mentions to valid WMATA names via a small
    # LLM call, then re-resolves. Skipped when no station/route failures exist.
    if resolutions.failed:
        resolutions = clarifier.clarify(resolutions, resolver)

    if resolutions.any_resolved:
        log.info(
            "run | anchors resolved | config=%s | %s",
            resolver.config,
            resolutions.as_flat_dict(),
        )
    else:
        log.warning(
            "run | zero anchors resolved — proceeding to degraded narration | query=%r",
            query,
        )

    # ── Query Writer path — up to 3 attempts ─────────────────────────────────
    # Attempt 1: plain generation. Attempts 2–3: validator errors fed back as
    # targeted correction hints. Stops as soon as validation passes.
    t2c_output: Text2CypherOutput | None = None

    if planner_output.path in {"text2cypher", "both"}:
        schema_slice = registry.get(planner_output.schema_slice_key)
        _MAX_ATTEMPTS = 3
        refinement_errors: list[str] = []
        all_validation_notes: list[str] = []

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            query_writer_output = run_query_writer(
                query,
                planner_output,
                llm_config,
                schema_slice=schema_slice,
                resolved_anchors=resolutions.as_flat_dict(),
                refinement_errors=refinement_errors or None,
            )
            print(f"\n[Query Writer — attempt {attempt}/{_MAX_ATTEMPTS}]")
            print("Cypher Query:\n", query_writer_output.cypher_query)
            if attempt == 1:
                print("Chain-of-Thought Comments:\n", query_writer_output.cot_comments)

            val_result = validate_and_log_cypher(
                query_writer_output.cypher_query,
                schema_slice,
                schema_slice.property_registry,
                db.driver,
                log,
            )

            if val_result.valid:
                print(f"Cypher Validator: valid (attempt {attempt}).")
                t2c_output = Text2CypherOutput(
                    cypher=query_writer_output.cypher_query,
                    results=val_result.results or [],
                    domain=planner_output.domain,
                    attempt_count=attempt,
                    validation_notes=all_validation_notes,
                    success=True,
                )
                break

            # Validation failed — collect errors and prepare for next attempt
            log.warning(
                "run | cypher validation failed | attempt=%d/%d | errors=%s",
                attempt,
                _MAX_ATTEMPTS,
                val_result.errors,
            )
            all_validation_notes.extend(val_result.errors)
            refinement_errors = val_result.errors

        else:
            # All attempts exhausted without a valid query
            print(f"Cypher Validator: all {_MAX_ATTEMPTS} attempts failed.")
            t2c_output = Text2CypherOutput(
                cypher="",
                results=[],
                domain=planner_output.domain,
                attempt_count=_MAX_ATTEMPTS,
                validation_notes=all_validation_notes,
                success=False,
            )

    # ── Subgraph path ─────────────────────────────────────────────────────────
    sub_output: SubgraphOutput | None = None

    if planner_output.path in {"subgraph", "both"}:
        builder = SubgraphBuilder(db=db)
        try:
            sub_output = builder.run(
                planner_output,
                resolutions,
                resolver_config=resolver.config,
            )
        except Neo4jError as exc:
            log.error(
                "run | subgraph expansion failed | %s: %s",
                type(exc).__name__,
                exc,
                exc_info=True,
            )
            print(f"\nDatabase error during subgraph expansion: {exc}")
            # Continue to narration with sub_output=None — degraded mode
        else:
            print(_fmt_subgraph_verbose(sub_output))

    # ── Narration Agent ───────────────────────────────────────────────────────
    narration_output = narration_agent.run(
        query,
        planner_output,
        t2c_output=t2c_output,
        subgraph_output=sub_output,
        resolutions=resolutions,
    )
    print(_fmt_narration(narration_output))

    return planner_output, t2c_output, sub_output, narration_output


# ── Agentic query execution ───────────────────────────────────────────────────


def _run_query_agentic(
    planner: Planner,
    orchestrator: AgentOrchestrator,
    db: Neo4jManager,
    query: str,
    *,
    clarifier: AnchorClarifier,
    candidate_limit: int = 1,
    strategy: str = "topk",
    label: str | None = None,
) -> tuple[PlannerOutput, Text2CypherOutput | None, SubgraphOutput | None, NarrationOutput | None]:
    """
    Execute a single query through the agentic pipeline.

    Shares the Planner + AnchorResolver + AnchorClarifier pre-step
    with _run_query() verbatim. After anchor resolution, delegates to
    AgentOrchestrator.run() instead of the static QueryWriter/Subgraph fork.

    Returns the same 4-tuple as _run_query() so the eval harness and mode
    functions can treat both pipelines identically.
    """
    invocation_time = datetime.now(UTC)
    prefix = f"{label}  " if label else ""

    planner_output = planner.run(query)

    header = f"\n{'═' * 56}\n{prefix}Query: {query!r}  [agentic]\n{'═' * 56}"
    print(header)
    print(_fmt_planner_verbose(planner_output))

    if planner_output.rejected:
        return planner_output, None, None, None

    # ── Shared pre-step: anchor resolution + clarification ────────────────────
    disambiguation_strategy = (
        TypeWeightedCoherenceStrategy() if strategy == "coherence" else None
    )
    resolver = AnchorResolver(
        db=db,
        invocation_time=invocation_time,
        strategy=disambiguation_strategy,
        candidate_limit=candidate_limit,
    )
    try:
        resolutions = resolver.resolve(planner_output.anchors)
    except Neo4jError as exc:
        log.error(
            "run_agentic | anchor resolution failed | %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        print(f"\nDatabase error during anchor resolution: {exc}")
        return planner_output, None, None, None

    if resolutions.failed:
        resolutions = clarifier.clarify(resolutions, resolver)

    if resolutions.any_resolved:
        log.info(
            "run_agentic | anchors resolved | config=%s | %s",
            resolver.config,
            resolutions.as_flat_dict(),
        )
    else:
        log.warning(
            "run_agentic | zero anchors resolved — proceeding to agent loop | query=%r",
            query,
        )

    # ── Agent loop ────────────────────────────────────────────────────────────
    try:
        t2c_output, sub_output, narration_output = orchestrator.run(
            query,
            planner_output,
            resolutions,
            resolver,
            invocation_time,
        )
    except Neo4jError as exc:
        log.error(
            "run_agentic | agent loop failed | %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        print(f"\nDatabase error during agent loop: {exc}")
        return planner_output, None, None, None

    print(_fmt_narration(narration_output))
    return planner_output, t2c_output, sub_output, narration_output


# ── Modes ─────────────────────────────────────────────────────────────────────

def _mode_default(
    planner: Planner,
    narration_agent: NarrationAgent,
    db: Neo4jManager,
    query: str,
    *,
    registry: SliceRegistry,
    clarifier: AnchorClarifier,
    llm_config: LLMConfig,
    candidate_limit: int,
    strategy: str,
    pipeline_mode: str = "static",
    orchestrator: AgentOrchestrator | None = None,
) -> None:
    if pipeline_mode == "agentic" and orchestrator is not None:
        _run_query_agentic(
            planner,
            orchestrator,
            db,
            query,
            clarifier=clarifier,
            candidate_limit=candidate_limit,
            strategy=strategy,
        )
    else:
        _run_query(
            planner,
            narration_agent,
            db,
            query,
            registry=registry,
            clarifier=clarifier,
            llm_config=llm_config,
            candidate_limit=candidate_limit,
            strategy=strategy,
        )


def _mode_demo(
    planner: Planner,
    narration_agent: NarrationAgent,
    db: Neo4jManager,
    *,
    registry: SliceRegistry,
    clarifier: AnchorClarifier,
    llm_config: LLMConfig,
    candidate_limit: int,
    strategy: str,
    pipeline_mode: str = "static",
    orchestrator: AgentOrchestrator | None = None,
) -> None:
    """
    Four hardcoded smoke test queries.
    Always runs in strict mode (enforced at registry construction by caller).
    Supports both static and agentic modes for side-by-side comparison.
    """
    mode_label = f"[{pipeline_mode}]"
    print(f"Running {len(_DEMO_QUERIES)} demo queries in strict mode {mode_label}...\n")

    results: list[tuple] = []

    for i, (query, expected_domain) in enumerate(_DEMO_QUERIES, 1):
        label = f"[{i}/{len(_DEMO_QUERIES)}]"
        if pipeline_mode == "agentic" and orchestrator is not None:
            planner_output, t2c_output, _, narration_output = _run_query_agentic(
                planner,
                orchestrator,
                db,
                query,
                clarifier=clarifier,
                candidate_limit=candidate_limit,
                strategy=strategy,
                label=label,
            )
        else:
            planner_output, t2c_output, _, narration_output = _run_query(
                planner,
                narration_agent,
                db,
                query,
                registry=registry,
                clarifier=clarifier,
                llm_config=llm_config,
                candidate_limit=candidate_limit,
                strategy=strategy,
                label=label,
            )
        results.append((expected_domain, planner_output, t2c_output, narration_output))

    print(f"\n{'═' * 56}")
    print("Demo summary:")
    all_passed = True
    for (expected, planner_out, t2c_out, narration_out), (query, _) in zip(
        results, _DEMO_QUERIES, strict=True
    ):
        checks: list[tuple[bool, str]] = []

        if expected == "rejection":
            checks.append((planner_out.rejected, "planner:rejected"))
        else:
            checks.append((not planner_out.rejected and planner_out.domain == expected, f"planner:domain={expected}"))
            if planner_out.path in {"text2cypher", "both"}:
                t2c_ok = t2c_out is not None and t2c_out.success
                checks.append((t2c_ok, "t2c:valid"))
                result_count = len(t2c_out.results) if t2c_out is not None else 0
                checks.append((True, f"t2c:rows={result_count}"))
            checks.append((narration_out is not None, "narration:produced"))

        row_passed = all(ok for ok, _ in checks)
        if not row_passed:
            all_passed = False
        status = "✅" if row_passed else "❌"
        detail = "  ".join(("✓" if ok else "✗") + lbl for ok, lbl in checks)
        print(f"  {status}  {query[:48]!r:<50}  {detail}")

    print(f"\n{'All passed' if all_passed else 'Some checks failed'}")


def _mode_repl(
    planner: Planner,
    narration_agent: NarrationAgent,
    db: Neo4jManager,
    *,
    registry: SliceRegistry,
    clarifier: AnchorClarifier,
    llm_config: LLMConfig,
    candidate_limit: int,
    strategy: str,
    pipeline_mode: str = "static",
    orchestrator: AgentOrchestrator | None = None,
) -> None:
    """
    Interactive query loop.
    Exits cleanly on 'quit', 'exit', or Ctrl+C / Ctrl+D.
    """
    print(f"JourneyGraph query pipeline — interactive mode [{pipeline_mode}]")
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

        if pipeline_mode == "agentic" and orchestrator is not None:
            _run_query_agentic(
                planner,
                orchestrator,
                db,
                query,
                clarifier=clarifier,
                candidate_limit=candidate_limit,
                strategy=strategy,
            )
        else:
            _run_query(
                planner,
                narration_agent,
                db,
                query,
                registry=registry,
                clarifier=clarifier,
                llm_config=llm_config,
                candidate_limit=candidate_limit,
                strategy=strategy,
            )
        print()


# ── Startup ───────────────────────────────────────────────────────────────────


def _startup(*, strict: bool) -> tuple[Planner, NarrationAgent, Neo4jManager, AnchorClarifier, SliceRegistry, LLMConfig]:
    """
    Initialise the full pipeline stack.

    Returns (Planner, NarrationAgent, Neo4jManager, AnchorClarifier,
    SliceRegistry, LLMConfig). The Neo4j connection is held open across
    queries and closed by main() on exit via the finally block.

    Sequencing:
      1. LLM config      — hard fail if ANTHROPIC_API_KEY missing
      2. Neo4j           — hard fail if DB unreachable
      3. SliceRegistry   — validates slices against live graph
      4. Planner         — builds Planner LLM instance (LLM_MAX_TOKENS)
      5. NarrationAgent  — builds Narration LLM instance
                           (LLM_NARRATION_MAX_TOKENS)
      6. AnchorClarifier — fetches station/route catalogue from graph once
      7. Registry + LLMConfig returned for Query Writer path
    """
    llm_config = get_llm_config()
    log.info(
        "LLM config loaded — provider=%s model=%s "
        "planner_max_tokens=%d narration_max_tokens=%d",
        llm_config.llm_provider,
        llm_config.llm_model,
        llm_config.llm_max_tokens,
        llm_config.llm_narration_max_tokens,
    )

    db = Neo4jManager()
    log.info("Neo4j connected")

    registry = SliceRegistry(db, strict=strict)
    # Connection remains open — AnchorResolver and SubgraphBuilder need it
    # at query time.

    planner = Planner(registry, llm_config, strict=strict)
    narration_agent = NarrationAgent(llm_config)
    clarifier = AnchorClarifier(db, llm_config)
    return planner, narration_agent, db, clarifier, registry, llm_config


# ── Agentic startup ───────────────────────────────────────────────────────────


def _startup_agentic(
    *,
    db: Neo4jManager,
    llm_config: LLMConfig,
    registry: SliceRegistry,
    clarifier: AnchorClarifier,
    narration_agent: NarrationAgent,
) -> AgentOrchestrator:
    """
    Initialise the AgentOrchestrator after the standard _startup() completes.

    All heavy deps (db, registry, clarifier, narration_agent) are reused from
    _startup() — no additional DB queries or LLM instances are created here.

    Returns:
        AgentOrchestrator ready for run() calls.
    """
    return AgentOrchestrator(
        db=db,
        llm_config=llm_config,
        registry=registry,
        clarifier=clarifier,
        narration_agent=narration_agent,
    )


# ── Entry point ───────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    # Configure root logger for third-party libraries (httpx, neo4j, etc.).
    # Project loggers use get_logger(__name__) which writes to logs/pipeline.log
    # directly (propagate=False), so this basicConfig only affects external libs.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    args = _parse_args(argv)
    strict = args.strict or args.demo
    candidate_limit: int = args.candidate_limit
    strategy: str = args.strategy
    pipeline_mode: str = args.mode

    if candidate_limit > 1:
        log.info(
            "Resolver config — candidate_limit=%d strategy=%s",
            candidate_limit,
            strategy,
        )

    if pipeline_mode == "agentic":
        log.info("Pipeline mode: agentic (agent loop, max 5 iterations)")

    try:
        planner, narration_agent, db, clarifier, registry, llm_config = _startup(strict=strict)
    except (OSError, RuntimeError) as exc:
        log.error("Startup failed: %s", exc)
        sys.exit(1)

    # Conditionally initialise the AgentOrchestrator — only when agentic mode
    # is requested. Static mode is unaffected and incurs no extra cost.
    orchestrator: AgentOrchestrator | None = None
    if pipeline_mode == "agentic":
        orchestrator = _startup_agentic(
            db=db,
            llm_config=llm_config,
            registry=registry,
            clarifier=clarifier,
            narration_agent=narration_agent,
        )

    try:
        if args.demo:
            _mode_demo(
                planner,
                narration_agent,
                db,
                registry=registry,
                clarifier=clarifier,
                llm_config=llm_config,
                candidate_limit=candidate_limit,
                strategy=strategy,
                pipeline_mode=pipeline_mode,
                orchestrator=orchestrator,
            )
        elif args.repl:
            _mode_repl(
                planner,
                narration_agent,
                db,
                registry=registry,
                clarifier=clarifier,
                llm_config=llm_config,
                candidate_limit=candidate_limit,
                strategy=strategy,
                pipeline_mode=pipeline_mode,
                orchestrator=orchestrator,
            )
        elif args.query:
            _mode_default(
                planner,
                narration_agent,
                db,
                args.query,
                registry=registry,
                clarifier=clarifier,
                llm_config=llm_config,
                candidate_limit=candidate_limit,
                strategy=strategy,
                pipeline_mode=pipeline_mode,
                orchestrator=orchestrator,
            )
        else:
            _parse_args(["--help"])
    finally:
        db.close()
        log.info("Neo4j connection closed")


if __name__ == "__main__":
    main()
