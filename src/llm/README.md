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

Each query domain maps to a YAML file in `src/llm/slices/`. A slice defines the exact node labels, relationship types, traversal patterns, and WMATA data quirks the LLM is permitted to reference for that domain. `SliceRegistry` validates every slice against the live graph at startup and injects it into the `QueryWriter` system prompt as hard constraints.

```
src/llm/slices/
    transfer_impact.yaml
    accessibility.yaml
    delay_propagation.yaml
```

**Required vs optional schema** — node labels and relationships fall into two categories:

- **`nodes:` / `relationships:`** — static structural nodes always present after base pipeline layers load (Station, Platform, Route, Trip, …). Checked against `db.labels()` / `db.relationshipTypes()` at startup; strict mode fails if any are absent.
- **`nodes_optional:`** — RT/API overlay nodes absent on a fresh DB (Interruption:\*, TripUpdate, OutageEvent, …). Included in the validator whitelist so the LLM may reference them; never checked at startup. Any relationship whose endpoint is in `nodes_optional` is automatically treated as optional too — no `relationships_optional` entry needed in the YAML.

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

## Text2Cypher — Current State

The Text2Cypher path is fully wired. `QueryWriter` calls Claude with domain conventions and few-shot `.cypher` patterns, the `CypherValidator` runs EXPLAIN + whitelist checks + execution, and the resulting `Text2CypherOutput` is passed to the Narration Agent. The `precision` mode (Text2Cypher only) and `synthesis` mode (both paths) are live.

### What it answers

Text2Cypher fills a gap the subgraph path cannot: **exact scalar answers** — counts, totals, booleans.

- "How many trips were skipped on Orange Line yesterday?" → `7` (not "looks like several")
- "Are there any active elevator outages at Gallery Place?" → `true`

### Known gaps

None. The retry loop is implemented: `_run_query` in `run.py` wraps the `run_query_writer` +
`validate_and_log_cypher` block in a `for attempt in range(1, 4)` loop. On validation failure the
validator errors are fed back to `QueryWriter._build_user_message` as a targeted correction hint
(`refinement_errors`), and the LLM is asked to fix only the flagged issues. `Text2CypherOutput.attempt_count`
and `validation_notes` record how many cycles ran.

---

## Future: Agentic Graph RAG

The current pipeline is **static RAG** — every decision is made upfront and the execution path is fixed. The Planner commits to a domain, path, and anchor list before touching any data. If the Cypher returns zero rows or the subgraph expansion is sparse, the pipeline proceeds to degraded narration with no recovery.

Moving to **agentic graph RAG** means replacing the linear DAG with a loop where an LLM decides which tools to call, observes what it finds, and iterates.

### What static RAG cannot do (today)

| Limitation | Example |
|---|---|
| No recovery from empty results | Cypher returns 0 rows → narration says "no data" with no retry |
| Single upfront path commitment | Can't try text2cypher, observe failure, then fall back to subgraph |
| No multi-domain queries | "Which stations have both elevator outages AND cancelled trips?" is classified into one domain and loses the other half |
| Predetermined hop topology | `EXPANSION_CONFIG` hard-codes which relationship types to follow per domain — the expander can't adapt to what it finds mid-traversal |
| No self-correction | If Cypher validation fails, there's no mechanism to reason about *why* and fix just the offending clause |
| Stateless across turns | No memory of previous results that could seed a follow-up query |

### Proposed agentic architecture

Replace `_run_query` in `run.py` with an agent loop. The agent has access to a fixed tool set and decides which tools to call based on intermediate results:

```
Tools:
  resolve_entities(mentions)        → AnchorResolutions
  get_schema(domain)                → SchemaSlice
  run_cypher(query)                 → rows | error
  expand_subgraph(anchor_ids)       → SubgraphOutput
  clarify_anchors(failed_mentions)  → corrected AnchorResolutions

Agent loop (max N iterations):
  1. Classify domain + extract anchor mentions (Planner, unchanged)
  2. Call tools in any order, observe results
  3. If run_cypher returns 0 rows → broaden query or switch to expand_subgraph
  4. If expand_subgraph is sparse → try different relationship types via run_cypher
  5. Stop when confident enough to narrate, or budget exhausted
  6. Call NarrationAgent with whatever was gathered
```

### Extension points already in the codebase

Every component is already structured as a callable with a clean input/output contract — converting them to tools requires no internal changes:

| Current component | As an agent tool |
|---|---|
| `AnchorResolver.resolve()` | `resolve_entities(mentions) → AnchorResolutions` |
| `cypher_validator()` | `run_cypher(query) → rows \| ValidationResult` |
| `SubgraphBuilder.run()` | `expand_subgraph(anchor_ids, domain) → SubgraphOutput` |
| `AnchorClarifier.clarify()` | `clarify_anchors(failed) → AnchorResolutions` |
| `SliceRegistry.get()` | `get_schema(domain) → SchemaSlice` |

`Text2CypherOutput.attempt_count`, `validation_notes`, and `ValidationError.cypher_excerpt` were designed specifically for a retry-aware loop — these fields are dead weight in the current static pipeline but become load-bearing in an agentic one.

### Latency and cost trade-offs

| Approach | LLM calls per query | Latency |
|---|---|---|
| Current static pipeline | 2–4 | ~5–10s |
| Level 1: retry loop only | 2–7 | ~8–15s |
| Level 3: full agent loop | 4–12 | ~15–40s |

The agent loop is best suited for the REPL and async contexts. The `--demo` batch mode should keep the static pipeline as a fast-path option.

### Recommended implementation path

1. ~~**Fix the remaining known gap** in the Text2Cypher path (retry loop)~~ — done. Retry loop, resolved IDs, and whitelist checks are all wired.
2. **Add a `path_fallback` mechanism** to `run.py`: if text2cypher returns zero rows and path was `text2cypher`, re-run as `subgraph`. No agent loop required — just a conditional in `_run_query`. This alone recovers the most common failure mode.
3. **Build the tool wrappers** as a thin adapter layer over the existing components.
4. **Wire Claude tool use** via the Anthropic SDK (`tools=[...]` in `client.messages.create`) — the `QueryWriter` already uses the SDK directly, so no new dependencies.
5. **Evaluate using `tests/eval/`** before and after each step to measure answer quality improvement.

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
    query_writer.py              ← Text2Cypher LLM call; injects SchemaSlice constraints +
    │                               resolved anchor IDs as literals into the prompt
    cypher_validator.py          ← EXPLAIN + node/rel/property whitelist + execution
    slice_registry.py            ← Loads and validates schema slices against live graph;
    │                               splits required vs optional nodes/relationships
    llm_factory.py               ← LLM provider abstraction (currently: Anthropic)
    narration_agent.py           ← Terminal stage; mode selection, prompt assembly, LLM call
    narration_output.py          ← NarrationOutput dataclass
    text2cypher_output.py        ← Text2CypherOutput dataclass
    slices/                      ← Domain YAML files (one per query domain)
```
