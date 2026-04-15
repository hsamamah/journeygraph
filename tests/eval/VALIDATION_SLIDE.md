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

## Current State (as of 2026-04-15)

### Questions authored

| File | Count | Domains covered | Notes |
|---|---|---|---|
| `questions/hani.yaml` | 13 | transfer_impact (4), accessibility (3), delay_propagation (3), adversarial (1), noise (3) | Complete. Noise variants (typo, colloquial, underspecified) included. |
| `questions/hani_gds.yaml` | 10 | transfer_impact/GDS (9), accessibility (1) | GDS-focused questions (PageRank, betweenness, Dijkstra, BFS, Louvain, WCC, degree). |
| `questions/lauren.yaml` | 34 | transfer_impact (8), accessibility (7), delay_propagation (7), adversarial (4), noise (4), edge_case (4) | Multi-domain coverage with noise and edge case variants. |

### Gaps to fill before the slide numbers are final

| What is missing | Why it matters |
|---|---|
| More adversarial questions | Currently 5 adversarial questions total. Ideally ≥ 3–5 per contributor for a robust rejection rate claim. |
| GDS planner routing fixes | 6/10 GDS questions fail because the planner refuses to execute rather than calling GDS tools. Fixing this will significantly move transfer_impact scores. |
| Graph refresh same day as eval | Implicit temporal anchors ("most recently", "right now") fail when graph data is stale. Numbers below reflect same-day-fresh data. |

---

## Numbers from Latest Scored Run (`20260415_150857_scored.jsonl`)

Single `default` config, 57 questions across all three contributor files.

| Domain | Pass Rate | Avg Faithfulness | Avg Answer Relevance | Dominant Narration Mode |
|---|---|---|---|---|
| transfer_impact | 15 / 28 (54%) | 0.63 | 0.62 | contextual |
| accessibility | 5 / 12 (42%) | 0.46 | 0.55 | contextual |
| delay_propagation | 7 / 12 (58%) | 0.67 | 0.68 | contextual |
| adversarial | 5 / 5 (100% rejected) | — | — | rejected |

### GDS vs Non-GDS breakdown

| Question type | Pass Rate | Notes |
|---|---|---|
| GDS questions (`hani_gds_*`, `lauren_029–034`) | 4 / 10 (40%) | Majority of failures are planner refusals, not bad answers |
| Regular questions (non-GDS) | 28 / 47 (60%) | Failures are mix of hallucination, incomplete retrieval, ambiguous anchors |

### By category

| Category | Pass Rate |
|---|---|
| happy_path | 17 / 33 (52%) |
| edge_case | 6 / 11 (55%) |
| noise | 4 / 8 (50%) |
| adversarial | 5 / 5 (100%) |

### Root cause of low scores

Two distinct failure modes:

1. **GDS planner refusals** — The planner returns "no data available" for GDS-mode questions (betweenness centrality, Louvain clusters, BFS reachability, Dijkstra shortest path). These questions have `expected_query_mode: gds` but the planner falls back to a cypher-only path and finds no matching tool. This is the dominant failure in `transfer_impact` and explains the GDS pass rate gap.

2. **Hallucination / incomplete retrieval** — Narration agent interpolates facts not present in the graph (wrong elevator status, fabricated escalator IDs, extra bus routes). Affects `accessibility` scores most heavily.

---

## Notable Failures and Proposed Fixes

| Question | ID | Score | Failure Reason | Proposed Fix |
|---|---|---|---|---|
| "Which stations are the biggest choke points…" | `hani_gds_003` | 0.00 | Planner refuses to execute — claims no data for betweenness centrality query | GDS tool routing: planner needs explicit GDS-capable intent signals; add betweenness centrality to planner's tool dispatch table |
| "Which stations can I reach from L'Enfant Plaza within 2 transfers?" | `hani_gds_006` | 0.50 | Planner acknowledges limitation rather than executing BFS | Wire BFS/reachability intent to GDS BFS tool; planner currently has no handler for hop-bounded reachability questions |
| `lauren_029`–`lauren_034` (GDS) | `lauren_029`–`034` | 0.00 | All refuse to answer — Louvain clusters, Dijkstra, centrality comparisons, BFS from Gallery Place | Same root cause as above; the entire GDS tool dispatch path is not reachable from lauren's question phrasings |
| "What disruptions are there at Washington?" | `hani_007` | 0.25 | Anchor resolves to Reagan National Airport (0 results) instead of a broader DC search | AnchorResolver: city-name anchors should broaden to all stations in the metro area, not resolve to a single station match |
| "Any bus delays downtown right now?" | `hani_013` | 0.00 | System refuses entirely — cannot ground spatial anchor "downtown" | Add fallback for unresolvable spatial anchors: return all active delays system-wide rather than refusing |
| "Are there any elevator outages at Metro Center…" | `hani_010` | 0.17 | Fabricated elevator status ("Resolved") when ground truth is "active"; hallucinated details | Narration agent must read status field directly from graph facts, not infer from context |
| "any elevator issues at galery place rn" | `lauren_017` | 0.35 | Fabricated elevator IDs not present in ground truth | Same hallucination issue — elevator ID enumeration must be grounded in retrieved nodes only |
| "What types of pathway infrastructure exist…" | `lauren_013` | 0.20 | Returns 3 of 7 pathway types; misrepresents counts | Pathway query needs `DISTINCT type` over full graph, not just the matched subgraph |
| "Which bus routes had both skipped trips and delay interruptions?" | `lauren_024` | 0.50 | Correctly identifies 20 routes but fabricates 6 extra routes | Cypher set-intersection join condition is too loose — tighten to exact match on route ID |

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

- [x] At least 3 contributor question files authored and validated
- [x] Each domain has ≥ 5 unique questions (excluding noise variants)
- [x] At least 3 adversarial questions total
- [ ] GDS planner routing fixed (currently 40% pass rate on GDS questions)
- [ ] Graph refreshed same day as final eval run
- [ ] Ground truths regenerated after refresh
- [ ] Harness run completed with `--config default`
- [ ] Scored results reviewed — no `regression: true` rows in happy_path questions
