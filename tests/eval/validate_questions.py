#!/usr/bin/env python3
"""
tests/eval/validate_questions.py

Validates eval question sets against the live graph.

For each question that has an oracle_cypher field, runs the Cypher directly
against Neo4j and prints the result. Questions with no data are flagged so
you can fix or remove them before committing ground_truth values.

Usage:
    uv run -m tests.eval.validate_questions                  # all question files
    uv run -m tests.eval.validate_questions --file hani.yaml # one file
    uv run -m tests.eval.validate_questions --id hani_001    # one question

After reviewing the output, use --write-ground-truth to patch the YAML files
with confirmed LLM-generated prose statements.

    uv run -m tests.eval.validate_questions --write-ground-truth
"""

import argparse
import json
import sys
import textwrap
from collections import defaultdict
from pathlib import Path

import yaml

QUESTIONS_DIR = Path(__file__).parent / "questions"

# Row caps — display truncates sooner than what is sent to the LLM
_DISPLAY_LIMIT = 10
_LLM_ROW_LIMIT = 20


def load_questions(file_filter: str | None = None) -> list[dict]:
    files = sorted(QUESTIONS_DIR.glob("*.yaml"))
    if file_filter:
        files = [f for f in files if f.name == file_filter or f.stem == file_filter]
    if not files:
        sys.exit(f"No question files found matching: {file_filter}")

    questions = []
    for f in files:
        qs = yaml.safe_load(f.read_text())
        for q in qs:
            q["_file"] = f
        questions.extend(qs)
    return questions


def is_adversarial(q: dict) -> bool:
    """Return True for questions that have no oracle (out-of-scope / rejection tests)."""
    return q.get("category") == "adversarial" or not q.get("oracle_cypher", "").strip()


def run_oracle(cypher: str, db) -> list[dict]:
    return db.query(cypher)


def format_result(rows: list[dict]) -> str:
    if not rows:
        return "  (no rows returned)"
    lines = ["  " + json.dumps(row, default=str) for row in rows[:_DISPLAY_LIMIT]]
    if len(rows) > _DISPLAY_LIMIT:
        lines.append(f"  ... ({len(rows) - _DISPLAY_LIMIT} more rows)")
    return "\n".join(lines)


def ground_truth_from_rows(rows: list[dict], question: str, client, model: str) -> str:
    """
    Use the LLM to convert oracle rows into a declarative ground truth statement.

    The statement is written so an LLM judge can compare it against a pipeline
    answer using: "Here is the question, here is the proposed answer, here is
    the ground truth — does the proposed answer satisfy the ground truth?"
    """
    rows_json = json.dumps(rows[:_LLM_ROW_LIMIT], default=str, indent=2)
    prompt = f"""You are writing a ground truth statement for an LLM evaluation dataset.

Question: {question}

Oracle query result (verified data from the graph database):
{rows_json}

Write a concise factual statement (2-5 sentences) that describes what a correct answer
to this question must convey. The statement will be used by an LLM judge that receives:
- The original question
- A proposed answer from the system under test
- This ground truth statement

Requirements:
- State the key facts a correct answer must include (specific values, counts, names)
- Note anything a correct answer must NOT do (e.g. must not imply causation between unrelated data sources)
- Do not use JSON or bullet points — write in plain prose
- If the oracle returned no relevant data (e.g. null values), say so clearly

Ground truth statement:"""

    response = client.messages.create(
        model=model,
        max_tokens=256,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


def patch_ground_truth_batch(file: Path, updates: dict[str, str]) -> None:
    """Write ground_truth updates for multiple questions in a single file write."""
    questions = yaml.safe_load(file.read_text())
    for q in questions:
        if q["id"] in updates:
            q["ground_truth"] = updates[q["id"]]
    file.write_text(yaml.dump(questions, allow_unicode=True, sort_keys=False, width=100))


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate eval questions against Neo4j")
    parser.add_argument("--file", help="Limit to a single question file (e.g. hani.yaml)")
    parser.add_argument("--id", help="Limit to a single question ID (e.g. hani_001)")
    parser.add_argument(
        "--write-ground-truth",
        action="store_true",
        help="Generate LLM prose ground truth statements and patch YAML files",
    )
    args = parser.parse_args()

    questions = load_questions(args.file)
    if args.id:
        questions = [q for q in questions if q["id"] == args.id]
        if not questions:
            sys.exit(f"No question found with id: {args.id}")

    # Initialise LLM client once — only when needed
    llm_client = None
    llm_model = None
    if args.write_ground_truth:
        import anthropic

        from src.common.config import get_llm_config

        cfg = get_llm_config()
        llm_client = anthropic.Anthropic(api_key=cfg.anthropic_api_key)
        llm_model = cfg.llm_model

    from src.common.neo4j_tools import Neo4jManager

    passed = 0
    no_data = 0
    skipped = 0

    # Accumulate ground truth patches per file — write each file once at the end
    pending_patches: dict[Path, dict[str, str]] = defaultdict(dict)

    with Neo4jManager() as db:
        for q in questions:
            oracle = q.get("oracle_cypher", "").strip()
            qid = q["id"]
            question_text = q["question"]

            print(f"\n{'─' * 70}")
            print(f"  {qid}  [{q.get('category', '?')}]  domain={q.get('domain', 'null')}")
            print(f"  Q: {question_text}")

            if is_adversarial(q):
                print("  ⊘  Adversarial/no-op — skipping oracle")
                skipped += 1
                continue

            try:
                rows = run_oracle(oracle, db)
            except Exception as e:
                print(f"  ✗  Oracle query failed: {e}")
                print("     Cypher:")
                print(textwrap.indent(oracle, "       "))
                no_data += 1
                continue

            if not rows:
                print("  ✗  No rows returned — no data in graph for this question")
                no_data += 1
            else:
                print(f"  ✓  {len(rows)} row(s) returned:")
                print(format_result(rows))
                passed += 1

                if args.write_ground_truth:
                    print("  ↳  generating ground truth statement via LLM...")
                    gt = ground_truth_from_rows(rows, question_text, llm_client, llm_model)
                    pending_patches[q["_file"]][qid] = gt
                    print(f"  ↳  queued: {gt[:80]}...")

    # Write each file once with all its patches
    for file, updates in pending_patches.items():
        patch_ground_truth_batch(file, updates)
        print(f"\n  ✎  Patched {len(updates)} question(s) in {file.name}")

    total = len(questions)
    print(f"\n{'═' * 70}")
    print(f"  Results: {passed} passed  |  {no_data} no data  |  {skipped} skipped  |  {total} total")

    if no_data:
        print(
            "\n  Questions with no data need to be revised or removed before\n"
            "  committing ground_truth values."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
