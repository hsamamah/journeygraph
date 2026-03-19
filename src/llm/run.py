# src/llm/run.py
"""
JourneyGraph LLM Query Pipeline — run script.

Entry point for natural language querying over the WMATA knowledge graph.
Runs the Planner stage and prints structured output. Downstream pipeline
stages (Query Writer, Cypher Validator, Context Builder, Narration Agent)
are not yet implemented — this script demonstrates and validates the
Planner in isolation.

Usage:
    # Single query (default)
    python -m src.llm.run "how many trips were cancelled on the red line yesterday"

    # Single query with full decision trace
    python -m src.llm.run "how many trips were cancelled" --verbose

    # Smoke test — four hardcoded queries, always runs in strict mode
    python -m src.llm.run --demo

    # Interactive REPL
    python -m src.llm.run --repl
    python -m src.llm.run --repl --verbose

    # Hard-fail on any schema validation warning
    python -m src.llm.run "..." --strict

Startup sequence (same for all modes):
    1. get_llm_config()   — hard fail if ANTHROPIC_API_KEY missing
    2. Neo4jManager()     — hard fail if DB unreachable
    3. SliceRegistry()    — validates slices against live graph
    4. Planner()          — builds LLM instance
    5. Enter selected mode

The SliceRegistry validation (DB-touching) always completes before any
LLM call is made, so a misconfigured database never wastes API tokens.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

from src.common.config import get_llm_config
from src.common.logger import get_logger
from src.common.neo4j_tools import Neo4jManager
from src.llm.planner import Planner
from src.llm.slice_registry import SliceRegistry

if TYPE_CHECKING:
    from src.llm.planner_output import PlannerOutput

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
        description="JourneyGraph LLM query pipeline — Planner stage",
    )

    # Mutually exclusive mode group — only one mode at a time
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
        "--verbose",
        action="store_true",
        help=(
            "Print Stage 1 domain scores, raw Stage 2 LLM response, "
            "and each PlannerOutput field individually. "
            "Available in all modes including --repl."
        ),
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

    return parser.parse_args(argv)


# ── Output formatting ─────────────────────────────────────────────────────────


def _fmt_compact(output: PlannerOutput) -> str:
    """
    One-line summary for default and REPL modes.

    Rejected queries print the rejection message directly.
    """
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


def _fmt_verbose(
    output: PlannerOutput,
    stage1_scores: dict[str, float] | None = None,
    raw_llm_response: str | None = None,
) -> str:
    """
    Full structured output for --verbose mode.

    stage1_scores and raw_llm_response are optional — they are populated
    by running classify_only() and capturing the LLM response before
    the full Planner.run() call. When not provided, those sections are
    omitted rather than showing placeholder text.
    """
    lines: list[str] = []

    # Stage 1 scores
    if stage1_scores is not None:
        lines.append("─── Stage 1 — domain classifier ────────────────────────")
        for domain, score in sorted(stage1_scores.items()):
            marker = (
                "  ← selected"
                if (not output.rejected and domain == output.domain)
                else ""
            )
            lines.append(f"  {domain:<22} {score:.4f}{marker}")

    # Stage 2 raw response
    if raw_llm_response is not None:
        lines.append("─── Stage 2 — LLM raw response ─────────────────────────")
        # Truncate very long responses for readability
        display = (
            raw_llm_response
            if len(raw_llm_response) <= 400
            else raw_llm_response[:400] + " …"
        )
        lines.append(f"  {display}")

    # PlannerOutput fields
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


# ── Query execution ───────────────────────────────────────────────────────────


def _run_query(
    planner: Planner,
    query: str,
    *,
    verbose: bool,
    label: str | None = None,
) -> PlannerOutput:
    """
    Execute a single query through the Planner and print results.

    In verbose mode, Stage 1 scores are obtained via classify_only()
    before the full run() call so they can be surfaced alongside the
    final output without modifying Planner internals.

    Args:
        planner: Initialised Planner instance.
        query:   Raw query string.
        verbose: Print full structured output including Stage 1 scores.
        label:   Optional prefix for --demo mode e.g. '[1/4]'.
    """
    prefix = f"{label}  " if label else ""

    if verbose:
        # Capture Stage 1 scores independently before the full pipeline run
        stage1_result = planner.classify_only(query)
        stage1_scores = stage1_result.scores
    else:
        stage1_scores = None

    output = planner.run(query)

    if verbose:
        header = f"\n{'═' * 56}\n{prefix}Query: {query!r}\n{'═' * 56}"
        print(header)
        # Raw LLM response not captured separately — surfaced via parse_warning
        # if Stage 2 degraded; otherwise shown as part of structured output.
        print(_fmt_verbose(output, stage1_scores=stage1_scores))
    else:
        if label:
            print(f"\n{prefix}{query!r}")
        print(_fmt_compact(output))

    return output


# ── Modes ─────────────────────────────────────────────────────────────────────


def _mode_default(planner: Planner, query: str, *, verbose: bool) -> None:
    """Single query, print result, exit."""
    _run_query(planner, query, verbose=verbose)


def _mode_demo(planner: Planner, *, verbose: bool) -> None:
    """
    Four hardcoded smoke test queries.
    Always runs in strict mode (enforced at registry construction by caller).
    Prints a summary line at the end showing pass/fail per query.
    """
    print(f"Running {len(_DEMO_QUERIES)} demo queries in strict mode...\n")
    results: list[tuple[str, PlannerOutput]] = []

    for i, (query, expected_domain) in enumerate(_DEMO_QUERIES, 1):
        label = f"[{i}/{len(_DEMO_QUERIES)}]"
        output = _run_query(planner, query, verbose=verbose, label=label)
        results.append((expected_domain, output))

    # Summary
    print(f"\n{'═' * 56}")
    print("Demo summary:")
    all_passed = True
    for (expected, output), (query, _) in zip(results, _DEMO_QUERIES, strict=True):
        if expected == "rejection":
            passed = output.rejected
        else:
            passed = not output.rejected and output.domain == expected
        status = "✅" if passed else "❌"
        if not passed:
            all_passed = False
        print(f"  {status}  {query[:52]!r:<54}  expected={expected}")

    print(f"\n{'All passed' if all_passed else 'Some checks failed'}")


def _mode_repl(planner: Planner, *, verbose: bool) -> None:
    """
    Interactive query loop.
    Exits cleanly on 'quit', 'exit', or Ctrl+C / Ctrl+D.
    """
    print("JourneyGraph query pipeline — interactive mode")
    print("Type a question, or 'quit' to exit.\n")

    while True:
        try:
            query = input("query> ").strip()
        except EOFError, KeyboardInterrupt:
            print("\nExiting.")
            break

        if not query:
            continue
        if query.lower() in {"quit", "exit"}:
            print("Exiting.")
            break

        _run_query(planner, query, verbose=verbose)
        print()


# ── Startup ───────────────────────────────────────────────────────────────────


def _startup(*, strict: bool) -> Planner:
    """
    Initialise the full pipeline stack.

    Sequencing:
      1. LLM config   — hard fail if ANTHROPIC_API_KEY missing
      2. Neo4j        — hard fail if DB unreachable
      3. SliceRegistry— validates slices against live graph
      4. Planner      — builds LLM instance

    The SliceRegistry validation completes before any LLM call is made.
    A misconfigured database never wastes API tokens.
    """
    llm_config = get_llm_config()
    log.info(
        "LLM config loaded — provider=%s model=%s max_tokens=%d",
        llm_config.llm_provider,
        llm_config.llm_model,
        llm_config.llm_max_tokens,
    )

    neo4j = Neo4jManager()
    log.info("Neo4j connected")

    registry = SliceRegistry(neo4j, strict=strict)
    neo4j.close()  # registry holds validated data in memory — connection released

    planner = Planner(registry, llm_config, strict=strict)
    return planner


# ── Entry point ───────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    # --demo always runs strict regardless of --strict flag
    strict = args.strict or args.demo

    try:
        planner = _startup(strict=strict)
    except (OSError, RuntimeError) as exc:
        log.error("Startup failed: %s", exc)
        sys.exit(1)

    if args.demo:
        _mode_demo(planner, verbose=args.verbose)
    elif args.repl:
        _mode_repl(planner, verbose=args.verbose)
    elif args.query:
        _mode_default(planner, args.query, verbose=args.verbose)
    else:
        # No mode selected — print help
        _parse_args(["--help"])


if __name__ == "__main__":
    main()
