#!/usr/bin/env python3
"""
tests/eval/run_harness.py

Config matrix harness for the JourneyGraph LLM eval framework.

Runs every question in questions/*.yaml against every config in configs.yaml
and writes one JSONL result row per (question, config) pair.

Each row captures:
  - Raw answer text
  - Planner-resolved domain and path (for regression detection)
  - Token counts (input + output + cache, summed across both LLM calls)
  - Estimated API cost in USD (from pricing table in configs.yaml)
  - Wall-clock latency
  - Process memory delta (RSS) and CPU user+sys time per question
  - Regression flag when planner output diverges from question metadata

Usage:
    uv run -m tests.eval.run_harness                         # all questions × all configs
    uv run -m tests.eval.run_harness --file hani.yaml        # one contributor file
    uv run -m tests.eval.run_harness --id hani_001           # one question
    uv run -m tests.eval.run_harness --config default        # one config
    uv run -m tests.eval.run_harness --output results/my.jsonl
    uv run -m tests.eval.run_harness --score                 # chain scorer after run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Generator

import psutil
import yaml

log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

EVAL_DIR = Path(__file__).parent
QUESTIONS_DIR = EVAL_DIR / "questions"
CONFIGS_FILE = EVAL_DIR / "configs.yaml"
RESULTS_DIR = EVAL_DIR / "results"


# ── Question loading ──────────────────────────────────────────────────────────


def load_questions(
    file_filter: str | None = None, id_filter: str | None = None
) -> list[dict]:
    files = sorted(QUESTIONS_DIR.glob("*.yaml"))
    if file_filter:
        files = [f for f in files if f.name == file_filter or f.stem == file_filter]
    if not files:
        sys.exit(f"No question files found matching: {file_filter!r}")

    questions = []
    for f in files:
        questions.extend(yaml.safe_load(f.read_text()))

    if id_filter:
        questions = [q for q in questions if q["id"] == id_filter]
        if not questions:
            sys.exit(f"No question found with id: {id_filter!r}")

    return questions


def load_configs(config_filter: str | None = None) -> tuple[list[dict], dict]:
    """Return (configs, pricing) parsed from a single CONFIGS_FILE read."""
    raw = yaml.safe_load(CONFIGS_FILE.read_text())
    configs = raw["configs"]
    pricing = raw.get("pricing", {})
    if config_filter:
        configs = [c for c in configs if c["name"] == config_filter]
        if not configs:
            sys.exit(f"No config named: {config_filter!r}")
    return configs, pricing


# ── Token tracking ────────────────────────────────────────────────────────────


@contextmanager
def _track_tokens(*llm_instances: Any) -> Generator[dict, None, None]:
    """
    Wrap anthropic client.messages.create on each AnthropicLLM instance to
    accumulate input/output token counts.

    Yields a dict that is populated during the context:
        {"input": int, "output": int}

    Restores original methods on exit regardless of exceptions.
    """
    totals: dict[str, int] = {
        "input": 0,
        "output": 0,
        "cache_write": 0,
        "cache_read": 0,
    }
    originals: list[tuple[Any, Any]] = []

    def _make_patched(orig_fn: Any) -> Any:
        def _patched(*args: Any, **kwargs: Any) -> Any:
            response = orig_fn(*args, **kwargs)
            usage = getattr(response, "usage", None)
            if usage:
                totals["input"] += getattr(usage, "input_tokens", 0)
                totals["output"] += getattr(usage, "output_tokens", 0)
                totals["cache_write"] += getattr(usage, "cache_creation_input_tokens", 0)
                totals["cache_read"] += getattr(usage, "cache_read_input_tokens", 0)
            return response
        return _patched

    for llm in llm_instances:
        client = getattr(llm, "client", None)
        if client is None or not hasattr(client, "messages"):
            log.warning(
                "_track_tokens: LLM instance %r has no .client.messages "
                "— tokens will not be tracked for this instance",
                llm,
            )
            continue
        orig = client.messages.create
        client.messages.create = _make_patched(orig)
        originals.append((client, orig))

    try:
        yield totals
    finally:
        for client, orig in originals:
            client.messages.create = orig


# ── Pipeline execution ────────────────────────────────────────────────────────


def _compute_cost(tokens: dict[str, int], model: str, pricing: dict) -> float:
    """Return estimated USD cost from token counts and pricing table."""
    rates = pricing.get(model)
    if not rates:
        return 0.0
    mtok = 1_000_000
    return (
        tokens["input"] * rates.get("input_per_mtok", 0) / mtok
        + tokens["output"] * rates.get("output_per_mtok", 0) / mtok
        + tokens["cache_write"] * rates.get("cache_write_per_mtok", 0) / mtok
        + tokens["cache_read"] * rates.get("cache_read_per_mtok", 0) / mtok
    )


def _execute(
    question: str,
    planner: Any,
    narration_agent: Any,
    db: Any,
    *,
    candidate_limit: int,
    disambiguation_strategy: Any,
    force_path: str | None,
    model: str,
    pricing: dict,
    registry: Any,
    llm_config: Any,
) -> dict:
    """
    Run a single question through the pipeline and return a result dict.

    Mirrors _run_query from src/llm/run.py but adds:
      - token tracking (input, output, cache) via _track_tokens context manager
      - cost estimation from pricing table
      - process memory delta (RSS MB) and CPU time (user+sys ms) via psutil
      - force_path injection (patches PlannerOutput.path after planner.run)
      - wall-clock latency measurement

    Returns a dict with keys: answer, planner_domain, planner_path,
    narration_mode, input_tokens, output_tokens, cache_write_tokens,
    cache_read_tokens, cost_usd, latency_ms, memory_delta_mb, cpu_time_ms,
    success, failure_reason.
    """
    from neo4j.exceptions import Neo4jError

    from src.llm.anchor_resolver import AnchorResolver
    from src.llm.cypher_validator import validate_and_log_cypher
    from src.llm.query_writer import run_query_writer
    from src.llm.subgraph_builder import SubgraphBuilder
    from src.llm.text2cypher_output import Text2CypherOutput

    proc = psutil.Process()
    mem_before = proc.memory_info().rss
    cpu_before = proc.cpu_times()
    t0 = time.monotonic()

    def _snapshot() -> dict:
        """Capture resource usage delta and latency since t0."""
        cpu_after = proc.cpu_times()
        mem_after = proc.memory_info().rss
        return {
            "latency_ms": int((time.monotonic() - t0) * 1000),
            "memory_delta_mb": round((mem_after - mem_before) / 1_048_576, 2),
            "cpu_time_ms": round(
                (cpu_after.user - cpu_before.user + cpu_after.system - cpu_before.system) * 1000,
                1,
            ),
        }

    with _track_tokens(planner._llm, narration_agent._llm) as tokens:
        # Stage 1 + 2: Planner
        planner_output = planner.run(question)

        if planner_output.rejected:
            snap = _snapshot()
            return {
                "answer": planner_output.rejection_message or "",
                "planner_domain": "",
                "planner_path": "",
                "narration_mode": "rejected",
                "input_tokens": tokens["input"],
                "output_tokens": tokens["output"],
                "cache_write_tokens": tokens["cache_write"],
                "cache_read_tokens": tokens["cache_read"],
                "cost_usd": _compute_cost(tokens, model, pricing),
                **snap,
                "success": True,
                "failure_reason": None,
            }

        # Inject force_path before path dispatch
        if force_path:
            planner_output.path = force_path

        # Anchor resolution
        invocation_time = datetime.now(UTC)
        resolver = AnchorResolver(
            db=db,
            invocation_time=invocation_time,
            strategy=disambiguation_strategy,
            candidate_limit=candidate_limit,
        )
        try:
            resolutions = resolver.resolve(planner_output.anchors)
        except Neo4jError as exc:
            snap = _snapshot()
            return {
                "answer": "",
                "planner_domain": planner_output.domain,
                "planner_path": planner_output.path,
                "narration_mode": "error",
                "input_tokens": tokens["input"],
                "output_tokens": tokens["output"],
                "cache_write_tokens": tokens["cache_write"],
                "cache_read_tokens": tokens["cache_read"],
                "cost_usd": _compute_cost(tokens, model, pricing),
                **snap,
                "success": False,
                "failure_reason": f"anchor resolution Neo4j error: {exc}",
            }

        if not resolutions.any_resolved:
            log.warning(
                "run_harness | zero anchors resolved — proceeding to degraded narration"
            )

        # Text2Cypher path
        t2c_output = None
        if planner_output.path in {"text2cypher", "both"}:
            schema_slice = registry.get(planner_output.schema_slice_key)
            _MAX_ATTEMPTS = 3
            refinement_errors: list[str] = []
            all_validation_notes: list[str] = []

            for attempt in range(1, _MAX_ATTEMPTS + 1):
                qw_output = run_query_writer(
                    question,
                    planner_output,
                    llm_config,
                    schema_slice=schema_slice,
                    resolved_anchors=resolutions.as_flat_dict(),
                    refinement_errors=refinement_errors or None,
                    use_gds=planner_output.use_gds,
                )
                val_result = validate_and_log_cypher(
                    qw_output.cypher_query,
                    schema_slice,
                    schema_slice.property_registry,
                    db.driver,
                    log,
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
                    attempt_count=_MAX_ATTEMPTS,
                    validation_notes=all_validation_notes,
                    success=False,
                )

        # Subgraph path
        sub_output = None
        subgraph_failure: str | None = None

        if planner_output.path in {"subgraph", "both"}:
            builder = SubgraphBuilder(db=db)
            try:
                sub_output = builder.run(
                    planner_output,
                    resolutions,
                    resolver_config=resolver.config,
                )
            except Neo4jError as exc:
                subgraph_failure = f"subgraph Neo4j error: {exc}"

        # Narration
        try:
            narration_output = narration_agent.run(
                question,
                planner_output,
                t2c_output=t2c_output,
                subgraph_output=sub_output,
                resolutions=resolutions,
            )
        except Exception as exc:
            snap = _snapshot()
            return {
                "answer": "",
                "planner_domain": planner_output.domain,
                "planner_path": planner_output.path,
                "narration_mode": "error",
                "input_tokens": tokens["input"],
                "output_tokens": tokens["output"],
                "cache_write_tokens": tokens["cache_write"],
                "cache_read_tokens": tokens["cache_read"],
                "cost_usd": _compute_cost(tokens, model, pricing),
                **snap,
                "success": False,
                "failure_reason": f"narration error: {exc}",
            }

    snap = _snapshot()
    return {
        "answer": narration_output.answer,
        "planner_domain": planner_output.domain,
        "planner_path": planner_output.path,
        "narration_mode": narration_output.mode,
        "input_tokens": tokens["input"],
        "output_tokens": tokens["output"],
        "cache_write_tokens": tokens["cache_write"],
        "cache_read_tokens": tokens["cache_read"],
        "cost_usd": _compute_cost(tokens, model, pricing),
        **snap,
        "success": narration_output.success,
        "failure_reason": "; ".join(
            r for r in [narration_output.failure_reason, subgraph_failure] if r
        ) or None,
    }


# ── Regression detection ──────────────────────────────────────────────────────


def _check_regression(question: dict, result: dict) -> tuple[bool, str | None]:
    """
    Compare question metadata expectations against actual planner output.

    Returns (regression: bool, detail: str | None).
    """
    issues = []

    expected_domain = question.get("domain")
    expected_mode = question.get("query_mode")

    if expected_domain and expected_domain != "null":
        actual_domain = result["planner_domain"]
        if not actual_domain:
            issues.append(
                f"domain: expected={expected_domain!r} but pipeline did not classify "
                f"(mode={result['narration_mode']})"
            )
        elif actual_domain != expected_domain:
            issues.append(
                f"domain: expected={expected_domain!r} actual={actual_domain!r}"
            )

    if expected_mode and expected_mode != "null":
        actual_path = result["planner_path"]
        if not actual_path:
            issues.append(
                f"query_mode: expected={expected_mode!r} but pipeline did not route "
                f"(mode={result['narration_mode']})"
            )
        elif actual_path != expected_mode:
            issues.append(
                f"query_mode: expected={expected_mode!r} actual={actual_path!r}"
            )

    if issues:
        return True, "; ".join(issues)
    return False, None


# ── Harness orchestration ─────────────────────────────────────────────────────


def run(
    questions: list[dict],
    configs: list[dict],
    pricing: dict,
    output_path: Path,
    run_id: str,
) -> None:
    """Execute all (question, config) pairs and write JSONL results."""
    from src.common.config import LLMConfig, get_llm_config
    from src.common.neo4j_tools import Neo4jManager
    from src.llm.disambiguation_strategies import TypeWeightedCoherenceStrategy
    from src.llm.narration_agent import NarrationAgent
    from src.llm.planner import Planner
    from src.llm.slice_registry import SliceRegistry

    base_llm_config = get_llm_config()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    total = len(questions) * len(configs)
    done = 0

    with Neo4jManager() as db, output_path.open("a") as f:
        # SliceRegistry queries Neo4j schema once — shared across all configs
        registry = SliceRegistry(db, strict=False)

        for cfg in configs:
            config_name = cfg["name"]
            candidate_limit = cfg.get("candidate_limit", 1)
            strategy = cfg.get("strategy", "topk")
            force_path = cfg.get("force_path")
            narration_max_tokens = cfg.get(
                "narration_max_tokens", base_llm_config.llm_narration_max_tokens
            )

            # Build per-config pipeline components (only LLM config varies between configs)
            llm_config = LLMConfig(
                anthropic_api_key=base_llm_config.anthropic_api_key,
                llm_provider=base_llm_config.llm_provider,
                llm_model=base_llm_config.llm_model,
                llm_max_tokens=base_llm_config.llm_max_tokens,
                llm_narration_max_tokens=narration_max_tokens,
            )
            planner = Planner(registry, llm_config)
            narration_agent = NarrationAgent(llm_config)
            # Stateless — instantiate once per config, reused across all questions
            disambiguation_strategy = TypeWeightedCoherenceStrategy() if strategy == "coherence" else None

            for q in questions:
                qid = q["id"]
                question_text = q["question"]
                done += 1

                # Skip adversarial questions for path-override configs —
                # they have no domain so override has no meaningful effect
                if q.get("category") == "adversarial" and force_path:
                    print(
                        f"  [{done}/{total}] {qid} × {config_name}  ⊘ skipped (adversarial + force_path)"
                    )
                    continue

                print(f"  [{done}/{total}] {qid} × {config_name} ...", end=" ", flush=True)
                timestamp = datetime.now(UTC).isoformat()

                try:
                    result = _execute(
                        question_text,
                        planner,
                        narration_agent,
                        db,
                        candidate_limit=candidate_limit,
                        disambiguation_strategy=disambiguation_strategy,
                        force_path=force_path,
                        model=llm_config.llm_model,
                        pricing=pricing,
                        registry=registry,
                        llm_config=llm_config,
                    )
                except Exception as exc:
                    result = {
                        "answer": "",
                        "planner_domain": "",
                        "planner_path": "",
                        "narration_mode": "error",
                        "input_tokens": 0,
                        "output_tokens": 0,
                        "cache_write_tokens": 0,
                        "cache_read_tokens": 0,
                        "cost_usd": 0.0,
                        "latency_ms": 0,
                        "memory_delta_mb": 0.0,
                        "cpu_time_ms": 0.0,
                        "success": False,
                        "failure_reason": f"unhandled exception: {exc}",
                    }

                regression, regression_detail = _check_regression(q, result)

                row = {
                    "run_id": run_id,
                    "config_name": config_name,
                    "question_id": qid,
                    "question": question_text,
                    "category": q.get("category"),
                    "hop_depth": q.get("hop_depth"),
                    "expected_domain": q.get("domain"),
                    "expected_query_mode": q.get("query_mode"),
                    "ground_truth": q.get("ground_truth"),
                    "answer": result["answer"],
                    "planner_domain": result["planner_domain"],
                    "planner_path": result["planner_path"],
                    "narration_mode": result["narration_mode"],
                    "regression": regression,
                    "regression_detail": regression_detail,
                    "latency_ms": result["latency_ms"],
                    "input_tokens": result["input_tokens"],
                    "output_tokens": result["output_tokens"],
                    "cache_write_tokens": result["cache_write_tokens"],
                    "cache_read_tokens": result["cache_read_tokens"],
                    "cost_usd": round(result["cost_usd"], 6),
                    "memory_delta_mb": result["memory_delta_mb"],
                    "cpu_time_ms": result["cpu_time_ms"],
                    "success": result["success"],
                    "failure_reason": result["failure_reason"],
                    "timestamp": timestamp,
                }

                f.write(json.dumps(row) + "\n")
                f.flush()

                status = "✓" if result["success"] else "✗"
                reg = " ⚠ regression" if regression else ""
                print(
                    f"{status}  {result['latency_ms']}ms  "
                    f"in={result['input_tokens']} out={result['output_tokens']}  "
                    f"${round(result['cost_usd'], 5)}  "
                    f"mem={result['memory_delta_mb']:+.1f}MB  "
                    f"cpu={result['cpu_time_ms']:.0f}ms"
                    f"{reg}"
                )

    print(f"\n  Results written to: {output_path}")


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run eval questions against the LLM pipeline config matrix"
    )
    parser.add_argument("--file", help="Limit to one question file (e.g. hani.yaml)")
    parser.add_argument("--id", help="Limit to one question ID (e.g. hani_001)")
    parser.add_argument("--config", help="Limit to one named config (e.g. default)")
    parser.add_argument(
        "--output",
        help="Output JSONL path (default: tests/eval/results/<run_id>.jsonl)",
    )
    parser.add_argument(
        "--score",
        action="store_true",
        help="Chain score_results.py after the run completes",
    )
    args = parser.parse_args()

    questions = load_questions(file_filter=args.file, id_filter=args.id)
    configs, pricing = load_configs(config_filter=args.config)

    run_id = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    output_path = Path(args.output) if args.output else RESULTS_DIR / f"{run_id}.jsonl"

    print(f"\n  Run ID   : {run_id}")
    print(f"  Questions: {len(questions)}")
    print(f"  Configs  : {len(configs)} ({', '.join(c['name'] for c in configs)})")
    print(f"  Output   : {output_path}")
    print(f"  Total    : {len(questions) * len(configs)} runs\n")

    run(questions, configs, pricing, output_path, run_id)

    if args.score:
        import subprocess

        print("\n  Chaining scorer...")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.eval.score_results",
                "--input",
                str(output_path),
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
