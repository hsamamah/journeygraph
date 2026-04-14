# LLM Validation — Slide Table Plan

This file tracks the target table for the final presentation slide and documents
what data exists, what is missing, and how to populate the numbers when the full
question set is ready.

---

## Target Table (Final Slide)

| Domain | Questions | Pass Rate | Faithfulness | Answer Relevance | Adversarial Rejection |
|---|---|---|---|---|---|
| transfer_impact | — | —% | — | — | n/a |
| accessibility | — | —% | — | — | n/a |
| delay_propagation | — | —% | — | — | n/a |
| **Out-of-scope (adversarial)** | — | n/a | n/a | n/a | —% |

Columns explained:
- **Pass Rate** — % of questions where both faithfulness ≥ 0.7 and answer_relevance ≥ 0.7 (LLM-as-judge)
- **Faithfulness** — mean score 0–1: answer is grounded in graph facts, no fabricated claims
- **Answer Relevance** — mean score 0–1: answer directly addresses the question asked
- **Adversarial Rejection** — % of out-of-scope questions correctly refused by the Planner

---

## Current State (Partial — as of 2026-04-14)

### Questions authored

| File | Count | Domains covered | Notes |
|---|---|---|---|
| `questions/hani.yaml` | 13 | transfer_impact (4), accessibility (3), delay_propagation (3), adversarial (1), noise (3) | Complete. Noise variants (typo, colloquial, underspecified) included. |
| `questions/hani_gds.yaml` | 10 | transfer_impact/GDS (9), accessibility (1) | GDS-focused questions (PageRank, betweenness, Dijkstra, BFS, Louvain, WCC, degree). |
| **Remaining contributors** | **0** | — | **Not yet authored. See gap table below.** |

### Gaps to fill before the slide numbers are final

| What is missing | Why it matters |
|---|---|
| Additional question authors / files | The eval README targets 10 questions per contributor. Only one contributor file exists beyond GDS. Pass rates from 13 questions are not representative enough for a slide claim. |
| More adversarial questions | Currently 1 adversarial question in `hani.yaml`. The table needs ≥ 3–5 to report a meaningful rejection rate. |
| Multi-author coverage for all three domains | `delay_propagation` and `accessibility` are each covered by only 3 happy-path questions in hani.yaml. More variety (different anchors, hop depths) is needed to avoid overfitting the reported numbers. |

---

## Numbers from Latest Scored Run (`20260409_005903_scored.jsonl`)

These are the **current partial numbers** across all configs. Do not use directly for
the slide — they are from a single contributor file and reflect known data-freshness
failures (implicit temporal anchors failing when the graph is not refreshed same-day).

| Domain | Pass Rate | Avg Faithfulness | Avg Answer Relevance | Dominant Narration Mode |
|---|---|---|---|---|
| transfer_impact | 0 / 20 (0%) | 0.10 | 0.00 | degraded |
| accessibility | 2 / 20 (10%) | 0.36 | 0.47 | contextual |
| delay_propagation | 2 / 20 (10%) | 0.16 | 0.17 | degraded |
| adversarial | 3 / 3 (100% rejected) | — | — | rejected |

Rows are counted across all 5 configs (`default`, `high_candidates`, `force_subgraph`,
`force_both`, `tight_budget`), so each question appears 5 times. To see per-question
pass counts, filter to `config_name = "default"` only.

### Root cause of low scores

The dominant failure mode is `degraded` narration — both retrieval paths returned no
data. The primary cause is **implicit temporal anchors** ("most recently", "right now")
that the AnchorResolver cannot ground when the graph's Date nodes are not current-day
fresh. This is a data-freshness issue, not a model quality issue. Running the pipeline
with fresh interruption/accessibility data before eval is expected to significantly
improve transfer_impact and delay_propagation scores.

---

## How to Populate the Final Numbers

### Step 1 — Refresh the graph (same day as the eval run)

```bash
uv run python -m src.pipeline --layers interruption accessibility
```

### Step 2 — Regenerate ground truths against fresh data

```bash
uv run -m tests.eval.validate_questions --write-ground-truth
```

### Step 3 — Run harness and score

```bash
# All questions × all configs, then immediately score
uv run -m tests.eval.run_harness --score

# Or: default config only, for a cleaner slide number
uv run -m tests.eval.run_harness --config default --score
```

### Step 4 — Extract the table numbers

```bash
python3 - << 'EOF'
import json, sys

f = "tests/eval/results/<run_id>_scored.jsonl"   # replace with actual run_id
rows = [json.loads(l) for l in open(f)]

# Filter to default config only for slide numbers
rows = [r for r in rows if r["config_name"] == "default"]

for domain in ["transfer_impact", "accessibility", "delay_propagation"]:
    dr = [r for r in rows if r.get("expected_domain") == domain]
    if not dr:
        print(f"{domain}: no data")
        continue
    passed = sum(1 for r in dr if r.get("passed") is True)
    avg_f = sum((r.get("faithfulness") or 0) for r in dr) / len(dr)
    avg_r = sum((r.get("answer_relevance") or 0) for r in dr) / len(dr)
    print(f"{domain}: pass={passed}/{len(dr)} ({100*passed//len(dr)}%) faith={avg_f:.2f} rel={avg_r:.2f}")

adv = [r for r in rows if r.get("category") == "adversarial"]
rejected = sum(1 for r in adv if r.get("narration_mode") == "rejected")
print(f"adversarial: rejected={rejected}/{len(adv)} ({100*rejected//len(adv) if adv else 0}%)")
EOF
```

---

## Question Coverage Checklist (for slide readiness)

- [ ] At least 3 contributor question files authored and validated
- [ ] Each domain has ≥ 5 unique questions (excluding noise variants)
- [ ] At least 3 adversarial questions total
- [ ] Graph refreshed same day as final eval run
- [ ] Ground truths regenerated after refresh
- [ ] Harness run completed with `--config default`
- [ ] Scored results reviewed — no `regression: true` rows in happy_path questions
