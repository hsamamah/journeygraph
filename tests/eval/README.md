# Eval

End-to-end evaluation framework for the JourneyGraph LLM query pipeline. Tests real pipeline behaviour across question types, anchor configurations, and retrieval strategies — with LLM-as-judge scoring against graph-verified ground truth.

---

## How it works

The framework has four stages:

**1. Question authoring** — contributors write natural language questions in YAML files under `questions/`. Each question carries metadata describing the expected pipeline behaviour (domain routing, query mode, hop depth, anchor complexity) and an `oracle_cypher` — a handwritten Cypher query that fetches the ground truth directly from Neo4j.

**2. Oracle validation** — `validate_questions.py` runs every `oracle_cypher` against the live graph. Questions with no data are flagged before they can be committed. Once data is confirmed, `--write-ground-truth` calls the LLM to convert raw oracle rows into a prose statement that an LLM judge can reason against.

**3. Harness runs** — `run_harness.py` executes every question against every named config in `configs.yaml` and writes one JSONL row per (question, config) pair. Each row captures the answer, planner routing decisions, token counts, estimated cost, latency, and process resource usage. Regression detection flags questions where the planner's domain or path diverges from the metadata expectation.

**4. LLM-as-judge scoring** — `score_results.py` reads a harness JSONL output and scores each (question, answer, ground_truth) triple for faithfulness and answer relevance. Scores are broken down by `hop_depth` and `category` to surface quality vs. cost trade-offs across configs.

---

## Directory structure

```
tests/eval/
├── configs.yaml              ← named pipeline configurations + Anthropic pricing table
├── run_harness.py            ← config matrix runner — writes results/*.jsonl
├── score_results.py          ← LLM-as-judge scorer — writes results/*_scored.jsonl
├── validate_questions.py     ← oracle runner + ground truth generator
├── test_eval_framework.py    ← unit tests for harness, scorer, and validator
├── questions/
│   ├── README.md             ← question schema reference and contributor guide
│   └── hani.yaml             ← 10 validated questions (transfer_impact, accessibility, delay_propagation)
└── results/
    └── <run_id>.jsonl        ← harness output (gitignored)
    └── <run_id>_scored.jsonl ← scorer output (gitignored)
```

---

## Workflow

### Adding questions (contributors)

1. Read `questions/README.md` for the schema and coverage criteria
2. Create `questions/<your_name>.yaml` with 10 questions, each including an `oracle_cypher`
3. Run the validator to confirm data exists in the graph:
   ```bash
   uv run -m tests.eval.validate_questions --file <your_name>.yaml
   ```
4. Fix any `✗` questions (no data returned) — revise the question or the oracle
5. Generate LLM ground truth statements from confirmed oracle results:
   ```bash
   uv run -m tests.eval.validate_questions --file <your_name>.yaml --write-ground-truth
   ```
6. Review the patched `ground_truth` fields in your YAML — they should read as prose scoring rubrics, not JSON
7. Commit your file

### Data freshness

> **Run the pipeline before running eval.** Temporal expressions like `"now"`, `"most recently"`, and `"recently"` resolve against whatever Date nodes are in the graph. If the graph has not been refreshed today, questions with implicit dates will fail anchor resolution and receive a degraded (no-data) answer.
>
> ```bash
> uv run -m src.llm.pipeline   # reload GTFS-RT and Incidents data
> ```
>
> After a reload, rerun `validate_questions.py --write-ground-truth` for any questions whose oracle results may have changed (e.g. new skip/delay counts).

---

### Running the harness

```bash
# All questions × all configs
uv run -m tests.eval.run_harness

# One contributor file × all configs
uv run -m tests.eval.run_harness --file hani.yaml

# One question × all configs
uv run -m tests.eval.run_harness --id hani_001

# All questions × one config
uv run -m tests.eval.run_harness --config default

# Custom output path
uv run -m tests.eval.run_harness --output results/my_run.jsonl

# Run harness then immediately score
uv run -m tests.eval.run_harness --score
```

Requires Neo4j and `.env` configured (`NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, `ANTHROPIC_API_KEY`).

### Scoring results

```bash
# Score a harness output file
uv run -m tests.eval.score_results --input results/20260408_230109.jsonl

# Score only one config's rows
uv run -m tests.eval.score_results --input results/20260408_230109.jsonl --config default

# Use a cheaper judge model (independent of pipeline model)
uv run -m tests.eval.score_results --input results/20260408_230109.jsonl --judge-model claude-haiku-4-5-20251001

# Custom pass threshold (default 0.7)
uv run -m tests.eval.score_results --input results/20260408_230109.jsonl --threshold 0.8
```

Scorer output is written as `<run_id>_scored.jsonl` alongside the input file.

### Revalidating after a data reload

Oracle results are point-in-time snapshots. After loading new interruption or accessibility data, regenerate ground truths:

```bash
uv run -m tests.eval.validate_questions --write-ground-truth
```

---

## `validate_questions.py`

Requires Neo4j running and `.env` configured (`NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`). The `--write-ground-truth` flag additionally requires `ANTHROPIC_API_KEY`.

```bash
# Validate all question files
uv run -m tests.eval.validate_questions

# Validate a single contributor's file
uv run -m tests.eval.validate_questions --file hani.yaml

# Validate a single question by ID
uv run -m tests.eval.validate_questions --id hani_001

# Confirm data exists and generate prose ground truth statements
uv run -m tests.eval.validate_questions --file hani.yaml --write-ground-truth
```

**Output symbols:**

| Symbol | Meaning |
|---|---|
| `✓` | Oracle returned rows — data confirmed in graph |
| `✗` | No rows returned or query failed — question needs revision |
| `⊘` | Adversarial/no-op question — oracle intentionally skipped |

The script exits with code `1` if any question has no data, making it safe to wire into CI.

### Ground truth generation

When `--write-ground-truth` is passed, each confirmed question's oracle rows are sent to the LLM with a prompt that asks it to write a 2–5 sentence prose statement describing what a correct pipeline answer must convey. The statement is written for an LLM judge that will later receive:

- The original question
- A proposed answer from the pipeline under test
- The ground truth statement

Statements include specific values a correct answer must mention (counts, route names, dates) and constraints on what it must not do (e.g. must not imply causation between OutageEvent and Interruption nodes, which are separate data sources with no graph link).

---

## `run_harness.py`

Executes the full config matrix and writes one JSONL row per (question, config) pair.

Each JSONL row contains:

| Field | Type | Description |
|---|---|---|
| `run_id` | str | Timestamp-based run identifier (e.g. `20260408_230109`) |
| `config_name` | str | Named config from `configs.yaml` |
| `question_id` | str | Question ID (e.g. `hani_001`) |
| `question` | str | Natural language question text |
| `category` | str | `happy_path`, `edge_case`, or `adversarial` |
| `hop_depth` | str \| null | `shallow`, `deep`, or null |
| `expected_domain` | str \| null | Metadata expectation for regression detection |
| `expected_query_mode` | str \| null | Metadata expectation for regression detection |
| `ground_truth` | str | Prose scoring rubric from oracle validation |
| `answer` | str | Raw pipeline answer |
| `planner_domain` | str | Actual domain the planner resolved |
| `planner_path` | str | Actual path the planner chose (`subgraph`, `both`, etc.) |
| `narration_mode` | str | `subgraph`, `both`, `rejected`, `zero_anchors`, `error` |
| `regression` | bool | True if planner domain or path diverges from metadata |
| `regression_detail` | str \| null | Human-readable description of the regression |
| `latency_ms` | int | Wall-clock time for the full pipeline call |
| `input_tokens` | int | Total input tokens (planner + narration) |
| `output_tokens` | int | Total output tokens (planner + narration) |
| `cache_write_tokens` | int | Tokens written to prompt cache |
| `cache_read_tokens` | int | Tokens read from prompt cache |
| `cost_usd` | float | Estimated API cost from `configs.yaml` pricing table |
| `memory_delta_mb` | float | RSS memory delta for this question |
| `cpu_time_ms` | float | CPU user+sys time for this question |
| `success` | bool | False if pipeline errored, rejected, or returned no anchors |
| `failure_reason` | str \| null | Description when `success` is false |
| `timestamp` | str | ISO 8601 UTC timestamp |

**Adversarial questions are skipped for path-override configs** (`force_subgraph`, `force_both`) — they have no domain so the override has no meaningful effect.

---

## `score_results.py`

LLM-as-judge scorer. Reads a harness JSONL file and appends four fields to each row:

| Field | Type | Description |
|---|---|---|
| `faithfulness` | float 0.0–1.0 | Answer is grounded in graph facts; no fabricated claims |
| `answer_relevance` | float 0.0–1.0 | Answer directly addresses the question |
| `passed` | bool | Both scores ≥ threshold (default 0.7) |
| `score_reasoning` | str | LLM explanation of the scores |

Rows with no `ground_truth` are skipped (`passed: null`). The scorer prints a summary table broken down by `hop_depth` and `category`.

The judge model defaults to whatever `LLM_MODEL` is set in `.env`. Use `--judge-model` to run an independent, cheaper judge (e.g. `claude-haiku-4-5-20251001`).

---

## `configs.yaml`

Defines the named pipeline configurations and the Anthropic pricing table used to estimate `cost_usd`.

| Config | `candidate_limit` | `strategy` | `force_path` | `narration_max_tokens` |
|---|---|---|---|---|
| `default` | 1 | topk | — | env default (1024) |
| `high_candidates` | 5 | coherence | — | env default (1024) |
| `force_subgraph` | 1 | topk | subgraph | env default (1024) |
| `force_both` | 1 | topk | both | env default (1024) |
| `tight_budget` | 1 | topk | — | 512 |

**`candidate_limit: 1`** short-circuits disambiguation — the topk candidate is used directly. Values >1 activate the configured `strategy`.

**`force_path`** overrides the planner's routing decision after Stage 2. Useful for comparing subgraph vs. planner-chosen paths on the same question set.

Update the `pricing` block when Anthropic changes rates.

---

## Question design criteria

Questions must be distinct on at least two of these axes:

| Axis | Values |
|---|---|
| Domain | `transfer_impact`, `accessibility`, `delay_propagation` |
| Anchor complexity | `single`, `multi`, `ambiguous` |
| Query mode | `text2cypher`, `subgraph`, `both` |
| Hop depth | `shallow` (1–2 hops), `deep` (3–4 hops) |
| Temporal | `explicit` (date named), `implicit` (current/recent implied) |
| Category | `happy_path`, `edge_case`, `adversarial` |

Each file of 10 should include at least one adversarial (out-of-scope) question, one ambiguous anchor question, and one deep-hop question. See `questions/README.md` for the full schema.

---

## Live graph schema notes

These were discovered empirically during oracle authoring and differ from the schema slice YAMLs in some cases:

| Fact | Detail |
|---|---|
| `Station.name` | Property is `name`, not `stop_name` |
| Rail `route_short_name` | `R`=Red, `B`=Blue, `G`=Green, `O`=Orange, `S`=Silver, `Y`=Yellow |
| Interruption subtypes | `:Delay`, `:Skip`, `:ServiceChange` — no `:Cancellation` in live graph |
| `:Skip` | Covers skipped/dropped trips — uses `AFFECTS_ROUTE` or `AFFECTS_TRIP` |
| `AFFECTS_STOP` | Defined in schema slices but **not present** in live graph |
| Bus trips | Use `SCHEDULED_AT → BusStop`, not `Platform` |
| `Pathway.id` | Property is `id`, not `pathway_id` |
| `OutageEvent` | Links via `AFFECTS → Pathway` — no direct link to `Station`; join via `Station-[:CONTAINS]->Pathway` |
| Rail skip data | Only Orange (`O`) and Yellow (`Y`) lines have `:Skip` interruptions in current data |
| Rail delay data | No `:Delay` interruptions on rail lines in current data — delays are bus-only |
