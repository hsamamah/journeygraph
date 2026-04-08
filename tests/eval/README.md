# Eval

End-to-end evaluation framework for the JourneyGraph LLM query pipeline. Tests real pipeline behaviour across question types, anchor configurations, and retrieval strategies — with LLM-as-judge scoring against graph-verified ground truth.

---

## How it works

The framework has three stages, with a fourth planned:

**1. Question authoring** — contributors write natural language questions in YAML files under `questions/`. Each question carries metadata describing the expected pipeline behaviour (domain routing, query mode, hop depth, anchor complexity) and an `oracle_cypher` — a handwritten Cypher query that fetches the ground truth directly from Neo4j.

**2. Oracle validation** — `validate_questions.py` runs every `oracle_cypher` against the live graph. Questions with no data are flagged before they can be committed. Once data is confirmed, `--write-ground-truth` calls the LLM to convert raw oracle rows into a prose statement that an LLM judge can reason against.

**3. *(Planned)* Harness runs** — a config matrix runner will execute each question against multiple pipeline configurations (candidate limits, disambiguation strategies, token budgets, query mode overrides) and collect raw answers alongside token counts and wall-clock latency per run.

**4. *(Planned)* LLM-as-judge scoring** — each (question, config, answer) triple is scored automatically using the `ground_truth` statement. Scores are aggregated across the config matrix to surface quality vs. cost trade-offs and catch regressions in domain routing.

---

## Directory structure

```
tests/eval/
├── validate_questions.py   ← oracle runner + ground truth generator
└── questions/
    ├── README.md           ← question schema reference and contributor guide
    ├── hani.yaml           ← 10 validated questions (transfer_impact, accessibility, delay_propagation)
    └── <teammate>.yaml     ← each contributor adds one file here
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

---

## Planned: harness config matrix

The harness (not yet built) will run each question against a set of named configurations:

```yaml
runs:
  - name: default           # let planner decide everything
  - name: high_candidates   # candidate_limit: 5, strategy: TypeWeightedCoherence
  - name: force_text2cypher # query_mode: text2cypher
  - name: force_both        # query_mode: both
  - name: tight_budget      # token_budget: 1000
```

Each run produces one answer per question. The harness will log:
- Raw answer text
- Token counts (input + output)
- Wall-clock latency
- Planner-resolved domain and query mode (for regression detection against question metadata)

## Planned: LLM-as-judge scoring loop

After each harness run, a scorer will call the LLM for each (question, answer, ground_truth) triple and return a structured score:

```json
{
  "faithfulness": 0.9,
  "answer_relevance": 0.8,
  "passed": true,
  "reasoning": "..."
}
```

Scores aggregated across configs surface Pareto trade-offs between answer quality, token cost, and latency.
