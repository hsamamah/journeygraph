# JourneyGraph LLM Query Pipeline

Natural language querying over the WMATA knowledge graph. The pipeline classifies a query, resolves anchor entities, then routes to a Subgraph Context Builder or Text2Cypher path — producing structured context consumed by the Narration Agent.

---

## Pipeline Stages

| Stage | Component | Status |
|---|---|---|
| L3 | Planner (domain classifier + LLM call) | ✅ Done |
| L4 | Text2Cypher (`Text2CypherRetriever`) | ✅ Done |
| L5 | Anchor Resolver → Subgraph Context Builder | ✅ Done |
| L6 | Narration Agent (4 response modes) | ✅ Done |

Anchor resolution runs as a shared pre-fork step after the Planner and before the Text2Cypher / Subgraph path split. Resolved IDs are shared across both paths with no second DB round-trip.

---

## Query Domains

| Domain | Example question |
|---|---|
| `transfer_impact` | How many trips were cancelled on the Red Line yesterday? |
| `accessibility` | Is the elevator at Metro Center out of service? |
| `delay_propagation` | Are there delays propagating from Gallery Place? |

---

## Setup

Add to `.env`:
```
ANTHROPIC_API_KEY=sk-ant-...
```

Optional overrides (defaults shown):
```
LLM_PROVIDER=anthropic
LLM_MODEL=claude-haiku-4-5-20251001
LLM_MAX_TOKENS=512
LLM_NARRATION_MAX_TOKENS=1024
```

Install dependencies:
```bash
uv sync --extra llm
```

---

## CLI

```bash
# Single query
uv run python -m src.llm.run "how many trips were cancelled on the red line yesterday"

# Full decision trace
uv run python -m src.llm.run "is the elevator at Metro Center out of service" --verbose

# Smoke test — all three domains + one rejection, strict mode
uv run python -m src.llm.run --demo

# Interactive loop
uv run python -m src.llm.run --repl
uv run python -m src.llm.run --repl --verbose

# Hard-fail on any schema validation warning
uv run python -m src.llm.run "..." --strict

# Increase disambiguation candidates (default: 1)
uv run python -m src.llm.run "..." --candidate-limit 5

# Use coherence-based disambiguation strategy
uv run python -m src.llm.run "..." --candidate-limit 5 --strategy TypeWeightedCoherenceStrategy
```

**`--candidate-limit`** — number of candidates fetched per anchor mention from the full-text index. `1` = baseline, no disambiguation. Values above `1` enable graph-assisted disambiguation via `--strategy`.

**`--strategy`** — disambiguation strategy to use when `--candidate-limit > 1`. Default: `TopKStrategy`. Alternative: `TypeWeightedCoherenceStrategy` (scores candidates by typed-relationship coherence across all anchor types in the query).

---

## Anchor Resolution

Anchors extracted by the Planner (stations, routes, dates, pathway nodes) are resolved to graph node IDs in a two-phase process, followed by an optional clarification pass:

**Phase 1 — Candidate generation**: each mention is looked up via a Neo4j full-text index, returning up to `candidate_limit` candidates ranked by string match score.

**Phase 2 — Disambiguation**: a `DisambiguationStrategy` selects one candidate per mention. When `candidate_limit=1` the strategy call is short-circuited — the single candidate is selected directly.

**Clarification pass** (`AnchorClarifier`): if any station or route mentions fail Lucene lookup, a small LLM call fires with the failed mentions and the full WMATA station/route name catalogue (fetched once at startup). The LLM maps each failed mention to the closest valid name; successfully mapped mentions are re-resolved and merged back into `AnchorResolutions`. Silent — no user-facing output. Dates, pathway nodes, and levels are excluded (name fuzzing cannot fix structural failures).

`AnchorResolutions` carries typed dicts (`resolved_stations`, `resolved_routes`, `resolved_dates`, `resolved_pathway_nodes`, `resolved_levels`) plus a `failed` dict for mentions that produced zero candidates. Each value is a `list[str]` — length 1 when unambiguous, `>1` when candidates tied.

Pathway node lookup uses the `physical_pathway_name` full-text index (wildcard query) instead of a `toLower CONTAINS` scan — significantly faster on large graphs.

---

## Subgraph Context Builder

Three-stage pipeline producing a token-budgeted context block:

**Stage 1 — `AnchorResolver`**: resolves anchor mentions to graph node IDs (runs upstream as a shared pre-fork step).

**Stage 2 — `HopExpander`**: bidirectional hop expansion from anchor nodes, constrained by a `DomainExpansionConfig` (relationship types, node labels, max hops, per-hop `LIMIT`). Returns a `RawSubgraph`.

**Stage 3 — `ContextSerializer`**: serializes the `RawSubgraph` to a structured text block, then enforces a 6,000-token budget (tiktoken `cl100k_base`, lazy-loaded). Trim order when over budget: provenance nodes first → service layer nodes → interruption/outage nodes. Anchor nodes are never trimmed. After non-anchor trim candidates are exhausted, a binary search caps the provenance section (max 20 nodes) to fit within budget.

`SubgraphBuilder.run()` orchestrates Stages 2 and 3 and returns a `SubgraphOutput`. On failure (zero anchors, empty expansion, or unhandled exception) it returns `SubgraphOutput(success=False)` and never raises.

Per-domain expansion parameters live in `expansion_config.py` (`EXPANSION_CONFIG` dict keyed by domain name).

---

## Narration Agent

Terminal stage. Receives `PlannerOutput`, `Text2CypherOutput` (or `None`), and `SubgraphOutput` (or `None`), then selects a response mode and makes one LLM call.

**Response modes** (selected by pure logic, no LLM):

| Mode | Condition | Behaviour |
|---|---|---|
| `synthesis` | Both paths succeeded | Lead with facts, explain pattern using both sources |
| `precision` | Text2Cypher only | Answer directly, no speculation |
| `contextual` | Subgraph only | Qualify quantities, describe topology |
| `degraded` | Both failed or partial | Flag what could not be determined |

**Prompt structure:**
- System section 1 (~100 tokens, fixed): role, no-fabrication rule, no pipeline self-disclosure
- System section 2 (varies per mode): what data is available and how to use it
- System section 3 (varies per domain): vocabulary framing from the domain schema slice
- User message: `QUERY`, `DOMAIN / MODE`, `RESOLUTION STATUS` (degraded mode only — resolved and failed anchors), `PRECISE RESULTS` (if available), `GRAPH CONTEXT` (if available)

`NarrationOutput` always includes a `trace` dict for the caller to surface. The trace is not injected into the LLM prompt.

Token budget for the narration call is controlled separately from the planner via `LLM_NARRATION_MAX_TOKENS` (default 1024).

---

## Schema Slices

Each query domain maps to a YAML file in `src/llm/slices/`. A slice defines the node labels, relationship types, traversal patterns, and WMATA data quirks the LLM is permitted to reference for that domain.

```
src/llm/slices/
    transfer_impact.yaml
    accessibility.yaml
    delay_propagation.yaml
```

To add a new domain: add a YAML file with the four required fields (`nodes`, `relationships`, `patterns`, `warnings`) and register the domain key in `_SLICE_KEY_MAP` in `planner.py`.

---

## Future Improvements

### Vector Embedding Fallback for Anchor Resolution

When Lucene full-text search returns zero candidates for a station or route mention (e.g. "Dupont" instead of "Dupont Circle"), the anchor fails silently and the pipeline degrades. A vector similarity fallback would catch these near-miss variants.

**Intended flow:** Lucene lookup → zero results → vector ANN search → still nothing → clarification LLM call (clarification is already implemented; vector ANN search is the missing middle step).

**Architecture:**
- At ETL load time, compute embeddings for each `Station.name` and `Route.route_long_name` and store them as a `name_embedding` property using `db.create.setNodeVectorProperty`.
- Create a Neo4j vector index at schema-init time (alongside existing full-text indexes in `constraints.py`):
  ```cypher
  CREATE VECTOR INDEX station_embeddings IF NOT EXISTS
  FOR (s:Station) ON s.name_embedding
  OPTIONS { indexConfig: {
    `vector.dimensions`: 1536,
    `vector.similarity_function`: 'cosine'
  }}
  ```
- At runtime, embed the failed mention and query: `CALL db.index.vector.queryNodes('station_embeddings', 3, $queryVector) YIELD node, score WHERE score >= 0.75`.

**Blocker:** `neo4j-graphrag` has no `AnthropicEmbeddings` — Anthropic does not expose a public embeddings API. A second embedding provider (OpenAI or Cohere) would need to be added. This adds dependency and credential complexity that is not currently justified by the question set.

**Revisit when:** the question set grows to include more free-form station references, or a second LLM provider is added for another reason.

---

## Text2Cypher — Implementation Notes

The Text2Cypher path is wired in `run.py` and `narration_agent.py` but currently passes `t2c_output=None` — it is dead code. The Narration Agent's `synthesis` mode (both paths succeed) and `precision` mode (Text2Cypher only) are implemented and waiting for this path to be completed.

### What it should answer

Text2Cypher fills a gap the subgraph path cannot: **exact scalar answers** — counts, totals, booleans. Example:

- "How many trips were skipped on Orange Line yesterday?" → `7` (not "looks like several")
- "Are there any active elevator outages at Gallery Place?" → `true`

The subgraph path traverses and returns graph structure; the LLM then *estimates* from it. For precision questions this is unreliable. A direct aggregation query returns a definitive number.

### Recommended approach: templated queries, not raw Text2Cypher

Letting the LLM generate raw Cypher is unreliable — it hallucates property names, relationship types, and syntax. A safer pattern (validated in production by Neo4j's contract analysis work):

1. **The LLM fills typed parameters**, not Cypher syntax. Example struct:
   ```python
   {
       "domain": "transfer_impact",
       "metric": "skip_count",          # from an enum, not free text
       "station": "A01",                # resolved anchor ID
       "route": "O",                    # resolved anchor ID
       "date": "20260407"               # resolved anchor ID
   }
   ```
2. **Python constructs the Cypher** from validated templates keyed by `(domain, metric)`. The LLM never touches syntax.
3. The result (a count or row set) is passed to the Narration Agent as `Text2CypherOutput`.

### Why not both paths simultaneously?

They serve different query shapes and should both run in parallel for `path="both"` queries:

| Query type | Subgraph | Text2Cypher |
|---|---|---|
| "Explain why Red Line is disrupted" | ✓ topology + narrative | — |
| "How many skips on Orange Line yesterday?" | — | ✓ exact count |
| "How many and which routes are affected?" | ✓ which routes | ✓ exact count |

For `path="both"`, Narration Agent `synthesis` mode leads with the Text2Cypher number and explains using the subgraph context. The architecture already supports this — `synthesis` mode just needs both inputs to be non-None.

### Outstanding questions before implementing

- Define the full set of `metric` enum values per domain (what counts/aggregations are useful)
- Decide where template Cypher lives (inline in a new `text2cypher_writer.py`, or in the domain slice YAMLs)
- Determine whether `path` override should force Text2Cypher when the Planner routes to `subgraph`

---

## Structure

```
src/llm/
    run.py                       ← CLI entry point; shared anchor resolution + clarification pre-fork
    planner.py                   ← Single LLM call: domain + path + anchor extraction; produces PlannerOutput
    planner_output.py            ← PlannerOutput and PlannerAnchors dataclasses
    anchor_resolver.py           ← Two-phase anchor resolution; TopKStrategy (default)
    anchor_clarifier.py          ← LLM-assisted repair for failed station/route anchors
    disambiguation_strategies.py ← TypeWeightedCoherenceStrategy and strategy protocol
    subgraph_builder.py          ← Subgraph path orchestrator (HopExpander + ContextSerializer)
    hop_expander.py              ← Stage 2: bidirectional hop expansion → RawSubgraph
    context_serializer.py        ← Stage 3: serialization + 6,000-token budget enforcement
    subgraph_output.py           ← SubgraphOutput dataclass; make_zero_anchor_fallback()
    expansion_config.py          ← DomainExpansionConfig; EXPANSION_CONFIG per domain
    slice_registry.py            ← Loads and validates schema slices against live graph
    llm_factory.py               ← LLM provider abstraction (currently: Anthropic)
    narration_agent.py           ← Terminal stage; mode selection, prompt assembly, LLM call
    narration_output.py          ← NarrationOutput dataclass
    text2cypher_output.py        ← Text2CypherOutput dataclass
    slices/                      ← Domain YAML files (one per query domain)
```
