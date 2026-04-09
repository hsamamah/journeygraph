#!/usr/bin/env python3
"""
tests/eval/score_results.py

LLM-as-judge scorer for the JourneyGraph eval harness.

Reads a JSONL results file produced by run_harness.py, scores each
(question, answer, ground_truth) triple using the LLM, and writes a new
JSONL file with added score fields.

Each scored row adds:
    faithfulness      — float 0.0-1.0: answer is grounded in graph facts,
                        does not fabricate claims beyond what the data supports
    answer_relevance  — float 0.0-1.0: answer directly addresses the question
    passed            — bool: both scores ≥ 0.7 (configurable via --threshold)
    score_reasoning   — str: LLM explanation of the scores

Score files are written alongside the input file:
    results/20260408_143022.jsonl     ← harness output
    results/20260408_143022_scored.jsonl  ← scorer output

Usage:
    uv run -m tests.eval.score_results --input results/20260408_143022.jsonl
    uv run -m tests.eval.score_results --input results/20260408_143022.jsonl --config default
    uv run -m tests.eval.score_results --input results/20260408_143022.jsonl --threshold 0.8
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

PASS_THRESHOLD_DEFAULT = 0.7

SCORE_SYSTEM_PROMPT = """\
You are an impartial evaluator for a transit information system. You will be
given a user question, a ground truth statement describing what a correct answer
must convey, and the system's proposed answer.

Score the proposed answer on two dimensions, each from 0.0 to 1.0:

faithfulness — Does the answer stay grounded in verifiable facts? Does it avoid
fabricating claims, inventing numbers, or implying relationships that the ground
truth says do not exist? A score of 1.0 means every claim in the answer is
supported by or consistent with the ground truth. A score of 0.0 means the
answer contains significant fabrications or contradictions.

answer_relevance — Does the answer directly address the question asked? A score
of 1.0 means the answer is on-topic and complete. A score of 0.0 means the
answer is off-topic or entirely fails to address the question.

Respond with valid JSON only — no prose before or after the JSON block:
{
  "faithfulness": <float>,
  "answer_relevance": <float>,
  "reasoning": "<one or two sentences explaining both scores>"
}
"""

SCORE_USER_TEMPLATE = """\
QUESTION:
{question}

GROUND TRUTH:
{ground_truth}

PROPOSED ANSWER:
{answer}
"""


def _pct(passed: int, failed: int) -> str:
    return f"{passed / (passed + failed) * 100:.0f}%" if (passed + failed) else "—"


def _score_one(
    question: str,
    ground_truth: str,
    answer: str,
    client,
    model: str,
) -> dict:
    """Call the LLM judge and parse the score JSON. Returns score dict."""
    user_message = SCORE_USER_TEMPLATE.format(
        question=question,
        ground_truth=ground_truth,
        answer=answer if answer else "(no answer — pipeline failed or rejected query)",
    )

    try:
        response = client.messages.create(
            model=model,
            max_tokens=256,
            system=SCORE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        raw = response.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        parsed = json.loads(raw)
        return {
            "faithfulness": float(parsed.get("faithfulness", 0.0)),
            "answer_relevance": float(parsed.get("answer_relevance", 0.0)),
            "score_reasoning": str(parsed.get("reasoning", "")),
        }
    except Exception as exc:
        return {
            "faithfulness": 0.0,
            "answer_relevance": 0.0,
            "score_reasoning": f"scorer error: {exc}",
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM-as-judge scorer for harness results")
    parser.add_argument("--input", required=True, help="Path to harness JSONL output")
    parser.add_argument(
        "--output",
        help="Output path (default: <input stem>_scored.jsonl alongside input)",
    )
    parser.add_argument(
        "--config",
        help="Limit scoring to one named config (e.g. default)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=PASS_THRESHOLD_DEFAULT,
        help=f"Minimum score for both faithfulness and answer_relevance to pass (default: {PASS_THRESHOLD_DEFAULT})",
    )
    parser.add_argument(
        "--judge-model",
        help=(
            "Anthropic model to use as judge instead of the pipeline model from config "
            "(e.g. claude-haiku-4-5-20251001 for a cheaper independent judge). "
            "Cross-vendor judges (e.g. GPT-4o Mini) are not yet supported — requires OPENAI_API_KEY."
        ),
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        sys.exit(f"Input file not found: {input_path}")

    output_path = (
        Path(args.output)
        if args.output
        else input_path.parent / f"{input_path.stem}_scored.jsonl"
    )

    import anthropic

    from src.common.config import get_llm_config

    cfg = get_llm_config()
    client = anthropic.Anthropic(api_key=cfg.anthropic_api_key)
    model = args.judge_model or cfg.llm_model

    rows = [json.loads(line) for line in input_path.read_text().splitlines() if line.strip()]

    if args.config:
        rows = [r for r in rows if r.get("config_name") == args.config]
        if not rows:
            sys.exit(f"No rows found for config: {args.config!r}")

    total = len(rows)
    passed = 0
    failed = 0
    skipped = 0

    # Breakdown accumulators: {key: {"passed": int, "failed": int}}
    by_hop: dict[str, dict[str, int]] = defaultdict(lambda: {"passed": 0, "failed": 0})
    by_category: dict[str, dict[str, int]] = defaultdict(lambda: {"passed": 0, "failed": 0})

    print(f"\n  Input  : {input_path}")
    print(f"  Output : {output_path}")
    print(f"  Rows   : {total}")
    print(f"  Judge  : {model}")
    print(f"  Threshold: {args.threshold}\n")

    with output_path.open("w") as out:
        for i, row in enumerate(rows, 1):
            qid = row.get("question_id", "?")
            config_name = row.get("config_name", "?")
            ground_truth = row.get("ground_truth", "")

            # Skip rows with no ground truth (adversarial questions keep their
            # hand-written rubric — they should still be scored)
            if not ground_truth:
                print(f"  [{i}/{total}] {qid} × {config_name}  ⊘ no ground_truth — skipping")
                skipped += 1
                scored_row = {**row, "faithfulness": None, "answer_relevance": None, "passed": None, "score_reasoning": "no ground_truth"}
                out.write(json.dumps(scored_row) + "\n")
                continue

            print(f"  [{i}/{total}] {qid} × {config_name} ...", end=" ", flush=True)

            scores = _score_one(
                question=row["question"],
                ground_truth=ground_truth,
                answer=row.get("answer", ""),
                client=client,
                model=model,
            )

            passed_check = (
                scores["faithfulness"] >= args.threshold
                and scores["answer_relevance"] >= args.threshold
            )
            bucket = "passed" if passed_check else "failed"
            if passed_check:
                passed += 1
            else:
                failed += 1

            hop = row.get("hop_depth") or "unknown"
            cat = row.get("category") or "unknown"
            by_hop[hop][bucket] += 1
            by_category[cat][bucket] += 1

            scored_row = {
                **row,
                "faithfulness": scores["faithfulness"],
                "answer_relevance": scores["answer_relevance"],
                "passed": passed_check,
                "score_reasoning": scores["score_reasoning"],
            }
            out.write(json.dumps(scored_row) + "\n")

            status = "✓" if passed_check else "✗"
            print(
                f"{status}  faith={scores['faithfulness']:.2f}  "
                f"rel={scores['answer_relevance']:.2f}"
            )

    print(f"\n{'═' * 60}")
    print(
        f"  Scored: {passed} passed  |  {failed} failed  |  {skipped} skipped  |  {total} total"
    )
    print(f"  Pass rate: {_pct(passed, failed)}")

    if by_hop:
        print(f"\n  {'By hop_depth':<16} {'passed':>7}  {'failed':>7}  {'rate':>6}")
        print(f"  {'─' * 40}")
        for hop in sorted(by_hop):
            p, f = by_hop[hop]["passed"], by_hop[hop]["failed"]
            print(f"  {hop:<16} {p:>7}  {f:>7}  {_pct(p, f):>6}")

    if by_category:
        print(f"\n  {'By category':<16} {'passed':>7}  {'failed':>7}  {'rate':>6}")
        print(f"  {'─' * 40}")
        for cat in sorted(by_category):
            p, f = by_category[cat]["passed"], by_category[cat]["failed"]
            print(f"  {cat:<16} {p:>7}  {f:>7}  {_pct(p, f):>6}")

    print(f"\n  Scored results written to: {output_path}")


if __name__ == "__main__":
    main()
