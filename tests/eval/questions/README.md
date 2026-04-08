# Eval Question Sets

Each contributor adds one YAML file here with 10 questions. The eval harness
loads all files and runs every question against a matrix of pipeline configs.

## File naming

`<your_name>.yaml` — e.g. `hani.yaml`

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
  ground_truth: "..."     # known answer or scoring rubric for LLM-as-judge
  notes: "..."            # what this question is testing
```

`domain`, `query_mode`, `hop_depth`, `temporal` are the **expected** pipeline
behaviour under default settings — they are used to detect regressions, not to
constrain the harness. Set to `null` for adversarial/out-of-scope questions.

## Criteria — write questions that are distinct on at least two axes

| Axis | Values |
|---|---|
| Domain | transfer_impact, accessibility, delay_propagation |
| Anchor complexity | single, multi, ambiguous |
| Query mode | text2cypher, subgraph, both |
| Hop depth | shallow (1-2 hops), deep (3-4 hops) |
| Temporal | explicit date, implicit (current/recent) |
| Category | happy_path, edge_case, adversarial |

Aim for:
- At least one question per domain
- At least one adversarial (out-of-scope) question
- At least one ambiguous anchor question
- At least one deep-hop question
- No two questions with identical intent (same anchor + same domain + same query mode)

## How the harness uses these files

Questions are run against a config matrix — each question × each config
combination produces one result. The metadata fields drive:

- **Regression detection** — if `query_mode: text2cypher` but the planner routes
  to `subgraph`, that is flagged
- **LLM-as-judge scoring** — `ground_truth` is passed to a scorer that rates
  faithfulness and answer relevance
- **Cost/latency tracking** — token counts and wall time are logged per run so
  quality vs. cost trade-offs can be compared across configs
