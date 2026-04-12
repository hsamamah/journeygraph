# JourneyGraph LLM Query Pipeline

Natural language querying over the WMATA knowledge graph. A question enters the pipeline, is classified and routed by the Planner, resolved to graph entity IDs, then answered via a Cypher-generated query, a topology subgraph, or both — with a final narration step producing a prose answer.

---

## Pipeline Stages

| Stage | Component | What it does |
|---|---|---|
| L3 | **Planner** | Single LLM call: classifies domain, selects retrieval path, extracts anchor entities. Returns `PlannerOutput`. |
| — | **AnchorResolver** | Resolves station/route/date mentions to graph node IDs via full-text Lucene index. Shared pre-fork — runs once, results used by both paths. |
| — | **AnchorClarifier** | LLM-assisted repair pass. If any station/route mention fails Lucene lookup, a small LLM call maps the failed mention to the closest valid WMATA name. Silent — no user-facing output. |
| L4 | **QueryWriter** | Builds a Cypher query using domain few-shot examples, schema slice constraints, and resolved anchor IDs. |
| — | **CypherValidator** | Validates the generated query: write-clause guard, blocked-namespace guard, GDS procedure whitelist, label/rel/property whitelist, `EXPLAIN` parse check, then executes. 3-attempt retry loop feeds validation errors back to QueryWriter as targeted correction hints. |
| L5 | **SubgraphBuilder** | Bidirectional hop expansion from anchor nodes → token-budgeted context block. Orchestrates HopExpander + ContextSerializer. |
| L6 | **NarrationAgent** | Selects a response mode from `{synthesis, precision, contextual, degraded}`, assembles a structured prompt, makes the final LLM call. Returns a prose answer. |
| — | **AgentOrchestrator** | Agentic variant. Replaces the fixed QueryWriter/SubgraphBuilder fork with a Claude tool-use loop (`full_text_search`, `cypher_query`, `subgraph_expand`, `entity_clarify`). Shares Planner and NarrationAgent with the static pipeline. |

---

## Query Domains

| Domain key | What it covers | Example question |
|---|---|---|
| `transfer_impact` | Trip cancellations, skips, and their effect on station transfer partners | "How many trips were cancelled on the Red Line yesterday?" |
| `accessibility` | Elevator and escalator outages at stations | "Is the elevator at Metro Center out of service?" |
| `delay_propagation` | Real-time delays above 5 minutes, propagation through the network | "Are there delays propagating from Gallery Place?" |

A question outside these domains is rejected by the Planner with a `rejection_reason`. Use `null` domain in eval question metadata for adversarial/out-of-scope questions.

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

# Interactive loop
uv run python -m src.llm.run --repl

# Smoke test — one question per domain + one rejection, strict mode
uv run python -m src.llm.run --demo

# Agentic mode (tool-use loop instead of static Planner fork)
uv run python -m src.llm.run "which stations are the biggest choke points" --agentic

# Hard-fail on any schema validation warning
uv run python -m src.llm.run "..." --strict

# Increase disambiguation candidates (default: 1)
uv run python -m src.llm.run "..." --candidate-limit 5

# Use coherence-based disambiguation strategy
uv run python -m src.llm.run "..." --candidate-limit 5 --strategy coherence
```

**`--candidate-limit`** — number of candidates fetched per anchor mention from the full-text index. `1` = baseline, no disambiguation. Values above `1` enable graph-assisted disambiguation via `--strategy`.

**`--strategy`** — disambiguation strategy when `--candidate-limit > 1`. `topk` (default) takes the highest-scoring candidate. `coherence` uses `TypeWeightedCoherenceStrategy` — scores candidates by typed-relationship coherence across all anchor types in the query.

**`--agentic`** — routes through `AgentOrchestrator` instead of the static fork. The agent loop makes up to 5 tool-call iterations, then hands accumulated results to NarrationAgent. Output shape is identical to the static pipeline for eval harness compatibility.

---

## Startup Sequence

On every run (single query, REPL, or demo), startup runs in this order:

1. `get_llm_config()` — hard fail if `ANTHROPIC_API_KEY` missing
2. `Neo4jManager()` — hard fail if DB unreachable; connection held open for the session
3. `SliceRegistry()` — validates schema slices against live graph (`db.labels()`, `db.relationshipTypes()`, `db.schema.nodeTypeProperties()`); detects GDS availability (`CALL gds.version()`)
4. `Planner()` — builds LLM instance for Stage 1
5. `NarrationAgent()` — builds LLM instance for narration
6. `AnchorClarifier()` — fetches station/route name catalogue from graph once at startup
7. Enter selected mode

The DB validation (step 3) always completes before any LLM call, so a misconfigured database never wastes API tokens.

---

## Planner

A single LLM call handles domain classification, path selection, and anchor entity extraction. Bundling avoids a two-round-trip approach and ensures routing decisions are made with the same context.

**Stage 1 output** (JSON, parsed into `PlannerOutput`):
```json
{
  "domain": "transfer_impact",
  "path": "text2cypher",
  "anchors": {
    "stations": ["Metro Center"],
    "routes": ["Red Line"],
    "dates": ["2026-03-15"],
    "pathway_nodes": [],
    "levels": []
  },
  "path_reasoning": "count query — needs precise scalar",
  "anchor_notes": "date explicit in question",
  "use_gds": false
}
```

`use_gds: true` signals that a GDS graph algorithm (PageRank, Dijkstra, betweenness, etc.) would answer the question. The Planner only sets this when GDS is installed (`SliceRegistry.gds_available`); if GDS is absent, any `use_gds: true` from the LLM is overridden to `false` (hallucination guard).

**Rejection:** `null` domain → `PlannerOutput(rejected=True, rejection_message=...)`. Downstream stages are skipped.

---

## Anchor Resolution

Anchor mentions extracted by the Planner (stations, routes, dates, pathway nodes, levels) are resolved to graph node IDs in a two-phase process:

**Phase 1 — Candidate generation:** each mention is looked up via a Neo4j full-text index, returning up to `candidate_limit` candidates ranked by string match score.

**Phase 2 — Disambiguation:** a `DisambiguationStrategy` selects one candidate per mention. When `candidate_limit=1` the strategy is short-circuited — the single candidate is accepted directly.

**Clarification pass (`AnchorClarifier`):** if any station or route mention returns zero candidates, a small LLM call fires with the failed mentions and the full WMATA station/route catalogue (fetched once at startup). The LLM maps each failed mention to the closest valid name; resolved mentions are merged back into `AnchorResolutions`. Dates, pathway nodes, and levels are excluded (name fuzzing cannot fix structural failures).

`AnchorResolutions` carries typed dicts (`resolved_stations`, `resolved_routes`, `resolved_dates`, `resolved_pathway_nodes`, `resolved_levels`) plus a `failed` dict. Each value is a `list[str]` — length 1 when unambiguous, `>1` if candidates tied.

---

## Text2Cypher Path

`QueryWriter` calls the LLM with:
- Domain-specific few-shot `.cypher` examples (from `queries/<domain>/analytical.cypher`)
- GDS few-shot examples (from `queries/gds/analytical.cypher`) when `use_gds=True`
- `SchemaSlice` constraints (allowed node labels, relationship types, property names)
- Resolved anchor IDs injected as literals (not `$parameters`) to prevent hallucinated IDs
- `conventions.json` — formatting rules for generated Cypher

`CypherValidator` validates the generated query in this order:
1. Write-clause guard (`CREATE`/`MERGE`/`SET`/`DELETE`/`REMOVE` rejected immediately)
2. Blocked-namespace guard (`apoc.`, `dbms.`, `db.index.`, etc.)
3. GDS procedure whitelist (when GDS calls are present)
4. `EXPLAIN` syntax check via Neo4j driver
5. Label / relationship type / property whitelist against the schema slice
6. Execute (read-only) and return results

On validation failure, the errors are fed back to `QueryWriter` as `refinement_errors`. The outer loop retries up to 3 times. `Text2CypherOutput.attempt_count` records how many cycles ran.

---

## GDS Analytical Queries

When GDS (Graph Data Science) is installed and the Planner sets `use_gds=True`, the QueryWriter injects GDS-specific few-shot examples and procedure guidance into the prompt.

**GDS version note:** GDS 2.6+ requires a named graph as the first argument to all algorithm procedures. All GDS queries use the two-step named-graph pattern:

```cypher
CALL gds.graph.project('tmpName', ['Station', 'Route'],
    {SERVES: {type: 'SERVES', orientation: 'UNDIRECTED'}})
YIELD graphName
CALL gds.pageRank.stream(graphName)
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS node, score
WHERE node:Station
RETURN node.name AS station_name, round(score, 4) AS pagerank_score
ORDER BY pagerank_score DESC LIMIT 10
```

**Graph model for GDS:** There is no direct Station-to-Station relationship in this graph. Network analysis uses the Route-Station bipartite graph: `['Station', 'Route']` nodes connected by `SERVES`. This naturally models station importance as a function of which routes serve them.

**Allowed GDS algorithms:** PageRank, betweenness, degree, closeness, Louvain, label propagation, WCC, SCC, triangle count, node similarity, kNN, BFS, DFS, Dijkstra shortest path, all-pairs Dijkstra, A*. Named graph lifecycle (`gds.graph.project`, `gds.graph.drop`) is also permitted.

---

## Subgraph Context Builder

Three-stage pipeline producing a token-budgeted context block:

**Stage 1 — AnchorResolver** (shared pre-fork, described above).

**Stage 2 — HopExpander:** bidirectional hop expansion from anchor nodes, constrained by a `DomainExpansionConfig` (relationship types, node labels, max hops, per-hop `LIMIT`). Returns a `RawSubgraph`.

**Stage 3 — ContextSerializer:** serializes the `RawSubgraph` to a structured text block, then enforces a 6,000-token budget (tiktoken `cl100k_base`, lazy-loaded). Trim order when over budget: provenance nodes → service layer nodes → interruption/outage nodes. Anchor nodes are never trimmed. After non-anchor candidates are exhausted, binary search caps the provenance section (max 20 nodes).

Per-domain expansion parameters live in `expansion_config.py` (`EXPANSION_CONFIG` dict keyed by domain name).

---

## Narration Agent

Terminal stage. Receives `PlannerOutput`, `Text2CypherOutput` (or `None`), and `SubgraphOutput` (or `None`), then selects a response mode and makes one LLM call.

**Response modes** (selected by pure logic, no LLM):

| Mode | Condition | Behaviour |
|---|---|---|
| `synthesis` | Both Text2Cypher and Subgraph succeeded | Lead with precise facts, explain network pattern using both sources |
| `precision` | Text2Cypher succeeded, Subgraph absent/failed | Answer directly from query results, no speculation |
| `contextual` | Subgraph succeeded, Text2Cypher absent/failed | Describe topology, qualify quantities (no exact counts available) |
| `degraded` | Both failed or partial | Flag what could not be determined; state what was and wasn't resolved |

**Prompt structure** (three sections, assembled per mode × domain):
- Section 1 (~100 tokens, fixed): role definition, no-fabrication rule, no pipeline self-disclosure
- Section 2 (varies per mode): what data is available and how to use it
- Section 3 (varies per domain): vocabulary framing and WMATA data quirks from the schema slice

`NarrationOutput.trace` carries the pipeline trace for the caller. The trace is never injected into the LLM prompt.

---

## Schema Slices

Each query domain maps to a YAML file in `src/llm/slices/`. A slice defines the exact node labels, relationship types, traversal patterns, and WMATA data quirks the LLM is permitted to reference. `SliceRegistry` validates every slice against the live graph at startup and injects it into the `QueryWriter` system prompt as hard constraints.

```
src/llm/slices/
    transfer_impact.yaml
    accessibility.yaml
    delay_propagation.yaml
```

**Required vs. optional schema:**
- `nodes:` / `relationships:` — static structural nodes always present after base layers load. Checked at startup; strict mode fails if any are absent.
- `nodes_optional:` — real-time overlay nodes absent on a fresh DB (`Interruption:*`, `TripUpdate`, `OutageEvent`, …). Included in the validator whitelist so the LLM may reference them; never checked at startup. Any relationship whose endpoint is in `nodes_optional:` is automatically treated as optional.

To add a new domain: add a YAML file with `nodes`, `relationships`, `patterns`, `warnings` fields and register the domain key in `_SLICE_KEY_MAP` in `planner.py`.

---

## Agentic Pipeline

`AgentOrchestrator` (`agent.py`) provides an alternative to the static pipeline. Rather than committing upfront to one retrieval path, the agent uses Claude's native tool-use API to decide which tools to call based on what it observes:

**Tools available to the agent:**

| Tool | Wraps | What it does |
|---|---|---|
| `full_text_search` | `AnchorResolver` | Resolve one entity mention to graph node IDs |
| `cypher_query` | `QueryWriter` + `CypherValidator` | Generate and execute a Cypher query with 3-attempt retry |
| `subgraph_expand` | `SubgraphBuilder` | Expand a subgraph from resolved anchor IDs |
| `entity_clarify` | `AnchorClarifier` | LLM repair pass for failed entity lookups |

The agent loop runs up to 5 iterations. On each iteration, it calls `client.messages.create(tools=TOOL_DEFINITIONS, tool_choice="auto")`, dispatches returned `tool_use` blocks to the execute functions in `agent_tools.py`, appends `tool_result` messages, and continues until `stop_reason == "end_turn"` or the iteration budget is exhausted. The accumulated `AgentContext` (Cypher results + subgraph expansions) is then projected into `Text2CypherOutput | None` and `SubgraphOutput | None` for the NarrationAgent.

**When to use agentic mode:**
- Questions that might require fallback (zero Cypher rows → expand subgraph)
- Multi-domain questions
- Exploratory REPL sessions where the question type is uncertain

**When to use static mode:**
- Batch eval runs (faster, more predictable token cost)
- Well-defined questions within a single domain
- Cost-sensitive deployments

---

## Future: Vector Embedding Fallback for Anchor Resolution

When Lucene full-text search returns zero candidates (e.g. "Dupont" instead of "Dupont Circle"), the anchor falls back to the `AnchorClarifier` LLM call. A vector similarity middle step would catch near-miss variants before reaching the LLM.

**Intended flow:** Lucene → zero results → vector ANN search → still nothing → AnchorClarifier

**Blocker:** Anthropic does not expose a public embeddings API. A second provider (OpenAI or Cohere) would be required. Not currently justified by the question set size.

---

## Structure

```
src/llm/
    run.py                       ← CLI entry point; shared pre-fork (anchor resolution + clarification)
    planner.py                   ← Stage 1 LLM call: domain + path + anchor extraction
    planner_output.py            ← PlannerOutput and PlannerAnchors dataclasses
    anchor_resolver.py           ← Two-phase anchor resolution; TopK strategy (default)
    anchor_clarifier.py          ← LLM-assisted repair for failed station/route anchors
    disambiguation_strategies.py ← TypeWeightedCoherenceStrategy and strategy protocol
    query_writer.py              ← Text2Cypher LLM call; schema slice + anchor ID injection;
    │                               GDS few-shot loading; GDS_PROCEDURE_WHITELIST
    cypher_validator.py          ← Write-clause guard + namespace guard + GDS whitelist +
    │                               label/rel/property whitelist + EXPLAIN + execution
    slice_registry.py            ← Loads + validates schema slices; GDS availability detection
    llm_factory.py               ← LLM provider abstraction (Anthropic)
    subgraph_builder.py          ← Subgraph path orchestrator (HopExpander + ContextSerializer)
    hop_expander.py              ← Bidirectional hop expansion → RawSubgraph
    context_serializer.py        ← Serialization + 6,000-token budget enforcement
    subgraph_output.py           ← SubgraphOutput dataclass
    expansion_config.py          ← DomainExpansionConfig; EXPANSION_CONFIG per domain
    agent.py                     ← AgentOrchestrator — agentic tool-use loop
    agent_tools.py               ← Tool definitions (TOOL_DEFINITIONS) and execute_* functions
    narration_agent.py           ← Terminal stage: mode selection, prompt assembly, LLM call
    narration_output.py          ← NarrationOutput dataclass
    text2cypher_output.py        ← Text2CypherOutput dataclass
    conventions.json             ← Cypher formatting rules for QueryWriter
    slices/                      ← Domain YAML schema slices (one per query domain)
```
