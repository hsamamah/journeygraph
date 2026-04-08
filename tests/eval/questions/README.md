# Eval Question Sets

Each contributor adds one YAML file here with 10 questions. The eval harness loads all files and runs every question against a matrix of pipeline configurations.

See `../README.md` for the full framework overview, validator CLI, and planned harness design.

---

## File naming

`<your_name>.yaml` — e.g. `hani.yaml`

---

## Question schema

```yaml
- id: <name>_001          # unique across all files, never reuse
  question: "..."         # exact natural language string sent to the pipeline
  domain: transfer_impact | accessibility | delay_propagation | null
  anchors: []             # named entities the resolver must find
  anchor_complexity: single | multi | ambiguous
  query_mode: text2cypher | subgraph | both | null
  hop_depth: shallow | deep | null
  temporal: explicit | implicit | null
  category: happy_path | edge_case | adversarial
  oracle_cypher: |        # handwritten Cypher — run to confirm data exists in graph
    MATCH ...
    RETURN ...
  ground_truth: "..."     # LLM-generated prose statement — filled by validate_questions.py
  notes: "..."            # what this question is testing
```

**`domain`, `query_mode`, `hop_depth`, `temporal`** are the *expected* pipeline behaviour under default settings. The harness uses them to detect regressions (e.g. a question tagged `text2cypher` that now routes to `subgraph`). Set to `null` for adversarial/out-of-scope questions.

**`oracle_cypher`** is a handwritten Cypher query that fetches the correct answer directly from Neo4j. It must return rows — questions with empty oracle results are rejected by `validate_questions.py`. For adversarial questions, use `RETURN "no_data_expected" AS result`.

**`ground_truth`** is generated automatically by `validate_questions.py --write-ground-truth`. It is a 2–5 sentence prose statement written for an LLM judge describing what a correct answer must convey and what it must not do. **Do not write this by hand** — it is derived from confirmed oracle results.

---

## Validation workflow

Before committing, every question must pass oracle validation:

```bash
# 1. Check all oracles return data
uv run -m tests.eval.validate_questions --file <your_name>.yaml

# 2. Generate ground truth statements from confirmed results
uv run -m tests.eval.validate_questions --file <your_name>.yaml --write-ground-truth

# 3. Review the ground_truth fields — should be prose rubrics, not JSON
# 4. Commit
```

---

## Coverage criteria

Write questions that are distinct on at least two axes:

| Axis | Values |
|---|---|
| Domain | `transfer_impact`, `accessibility`, `delay_propagation` |
| Anchor complexity | `single`, `multi`, `ambiguous` |
| Query mode | `text2cypher`, `subgraph`, `both` |
| Hop depth | `shallow` (1–2 hops), `deep` (3–4 hops) |
| Temporal | `explicit` (date named), `implicit` (current/recent implied) |
| Category | `happy_path`, `edge_case`, `adversarial` |

Per file of 10, aim for:
- At least one question per domain
- At least one `adversarial` (out-of-scope) question
- At least one `ambiguous` anchor question
- At least one `deep` hop question
- No two questions with identical intent (same anchor + domain + query mode)

---

## Temporal framing

**Do not hardcode dates.** The interruption and accessibility layers have not run continuously, so arbitrary dates are likely to return no data. Use implicit temporal framing (`"most recently"`, `"right now"`, `"recently"`) so the pipeline resolves against whatever data is actually in the graph. The oracle confirms a date exists at validation time.

---

## Live graph schema — known gotchas

These were discovered during oracle authoring. Oracles that ignore these will return zero rows.

| Gotcha | Detail |
|---|---|
| `Station.name` | Property is `name`, not `stop_name` |
| Rail short names | `R`=Red, `B`=Blue, `G`=Green, `O`=Orange, `S`=Silver, `Y`=Yellow |
| No `:Cancellation` | Use `:Skip` for skipped/dropped trips |
| No `AFFECTS_STOP` | Not in live graph — use `AFFECTS_ROUTE` or `AFFECTS_TRIP` |
| Bus vs rail stops | Bus trips use `SCHEDULED_AT → BusStop`; rail uses `SCHEDULED_AT → Platform` |
| `Pathway.id` | Property is `id`, not `pathway_id` |
| `OutageEvent` → `Station` | No direct link — join via `(s:Station)-[:CONTAINS]->(p:Pathway)<-[:AFFECTS]-(o:OutageEvent)` |
| Rail skip coverage | Only Orange and Yellow lines have `:Skip` data in current graph |
| Rail delay coverage | No `:Delay` on rail in current data — delays are bus-only |
| `OutageEvent` ↔ `Interruption` | These are **separate data sources** (WMATA Incidents API vs GTFS-RT) with no graph link — questions that ask about both must not imply causation |

---

## How the harness uses these files

Questions are run against a config matrix — each question × each config combination produces one result. The metadata fields drive:

- **Regression detection** — if `query_mode: text2cypher` but the planner routes to `subgraph`, that run is flagged
- **LLM-as-judge scoring** — `ground_truth` is passed to a scorer that rates faithfulness and answer relevance for each (question, config, answer) triple
- **Cost/latency tracking** — token counts and wall time are logged per run so quality vs. cost trade-offs can be compared across configs
