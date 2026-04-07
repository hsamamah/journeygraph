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

Anchors extracted by the Planner (stations, routes, dates, pathway nodes) are resolved to graph node IDs in a two-phase process:

**Phase 1 — Candidate generation**: each mention is looked up via a Neo4j full-text index, returning up to `candidate_limit` candidates ranked by string match score.

**Phase 2 — Disambiguation**: a `DisambiguationStrategy` selects one candidate per mention. When `candidate_limit=1` the strategy call is short-circuited — the single candidate is selected directly.

`AnchorResolutions` carries four typed dicts (`resolved_stations`, `resolved_routes`, `resolved_dates`, `resolved_pathway_nodes`) plus a `failed` dict for mentions that produced zero candidates. Each value is a `list[str]` — length 1 when unambiguous, `>1` when candidates tied.

Pathway node lookup uses the `physical_pathway_name` full-text index (wildcard query) instead of a `toLower CONTAINS` scan — significantly faster on large graphs.

---

## Subgraph Context Builder

Three-stage pipeline producing a token-budgeted context block:

**Stage 1 — `AnchorResolver`**: resolves anchor mentions to graph node IDs (runs upstream as a shared pre-fork step).

**Stage 2 — `HopExpander`**: bidirectional hop expansion from anchor nodes, constrained by a `DomainExpansionConfig` (relationship types, node labels, max hops, per-hop `LIMIT`). Returns a `RawSubgraph`.

**Stage 3 — `ContextSerializer`**: serializes the `RawSubgraph` to a structured text block, then enforces a 2,000-token budget (tiktoken `cl100k_base`, lazy-loaded). Trim order when over budget: provenance nodes first → service layer nodes → interruption/outage nodes. Anchor nodes are never trimmed.

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
- User message: `QUERY`, `DOMAIN / MODE`, `PRECISE RESULTS` (if available), `GRAPH CONTEXT` (if available)

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

To add a new domain: add a YAML file with the four required fields (`nodes`, `relationships`, `patterns`, `warnings`) and register the domain key in `_SLICE_KEY_MAP` and `_DOMAIN_SIGNALS` in `planner.py`.

---

## Structure

```
src/llm/
    run.py                       ← CLI entry point; shared anchor resolution pre-fork
    planner.py                   ← Domain classifier + LLM call; produces PlannerOutput
    planner_output.py            ← PlannerOutput and PlannerAnchors dataclasses
    anchor_resolver.py           ← Two-phase anchor resolution; TopKStrategy (default)
    disambiguation_strategies.py ← TypeWeightedCoherenceStrategy and strategy protocol
    subgraph_builder.py          ← Subgraph path orchestrator (HopExpander + ContextSerializer)
    hop_expander.py              ← Stage 2: bidirectional hop expansion → RawSubgraph
    context_serializer.py        ← Stage 3: serialization + tiktoken budget enforcement
    subgraph_output.py           ← SubgraphOutput dataclass; make_zero_anchor_fallback()
    expansion_config.py          ← DomainExpansionConfig; EXPANSION_CONFIG per domain
    slice_registry.py            ← Loads and validates schema slices against live graph
    llm_factory.py               ← LLM provider abstraction (currently: Anthropic)
    narration_agent.py           ← Terminal stage; mode selection, prompt assembly, LLM call
    narration_output.py          ← NarrationOutput dataclass
    text2cypher_output.py        ← Text2CypherOutput dataclass
    slices/                      ← Domain YAML files (one per query domain)
```
