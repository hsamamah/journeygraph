# JourneyGraph LLM Query Pipeline

Natural language querying over the WMATA knowledge graph. The pipeline classifies a query, selects an execution path, and extracts anchor entities — producing a structured `PlannerOutput` consumed by downstream agents.

## Pipeline Stages

| Stage | Status | Owner |
|---|---|---|
| Planner | ✅ Implemented | — |
| Query Writer | 🔲 TODO | — |
| Cypher Validator | 🔲 TODO | — |
| Context Builder | 🔲 TODO | — |
| Narration Agent | 🔲 TODO | — |

## Query Domains

| Domain | Example question |
|---|---|
| `transfer_impact` | How many trips were cancelled on the Red Line yesterday? |
| `accessibility` | Is the elevator at Metro Center out of service? |
| `delay_propagation` | Are there delays propagating from Gallery Place? |

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
```

Install dependencies:
```bash
uv sync --extra llm
```

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
```

## Schema Slices

Each query domain maps to a YAML file in `src/llm/slices/`. A slice defines the node labels, relationship types, traversal patterns, and WMATA data quirks the LLM is permitted to reference for that domain.

```
src/llm/slices/
    transfer_impact.yaml
    accessibility.yaml
    delay_propagation.yaml
```

To add a new domain: add a YAML file with the four required fields (`nodes`, `relationships`, `patterns`, `warnings`) and register the domain key in `_SLICE_KEY_MAP` and `_DOMAIN_SIGNALS` in `planner.py`.

## Structure

```
src/llm/
    run.py              Entry point — CLI modes
    planner.py          Stage 1 classifier, Stage 2 LLM call, Stage 3 assembly
    planner_output.py   PlannerOutput and PlannerAnchors dataclasses
    slice_registry.py   Loads and validates schema slices at startup
    llm_factory.py      LLM provider abstraction (currently: Anthropic)
    slices/             Domain YAML files
```
