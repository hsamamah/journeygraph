# JourneyGraph

A Neo4j knowledge graph of the WMATA Washington DC Metro transit system, built on GTFS static feeds and GTFS-RT real-time data. The project has two main components:

- **ETL pipeline** (`src/pipeline.py`) — ingests GTFS static and WMATA API data into Neo4j across five domain layers
- **LLM query pipeline** (`src/llm/`) — natural language querying over the graph via a multi-agent pipeline

> **Current state:** All five ETL layers are implemented and tested. The LLM pipeline is complete — Planner (domain routing + anchor extraction), Text2Cypher path (QueryWriter + CypherValidator + 3-attempt retry loop), Subgraph Context Builder, GDS analytical query support, an agentic pipeline variant, and Narration Agent. An eval framework (`tests/eval/`) supports config-matrix harness runs with LLM-as-judge scoring. See [`src/llm/README.md`](src/llm/README.md) for full details.

---

## How It Works

### ETL Pipeline

`pipeline.py` is the ETL entry point. It handles two things:

1. **Downloading the GTFS feed** — fetches the WMATA static GTFS zip, extracts it, and parses every CSV into a shared `dict[str, pd.DataFrame]` keyed by filename stem (e.g. `gtfs_data["stops"]`, `gtfs_data["fare_leg_rules"]`).

2. **Running domain layers** — passes that dict to each layer's `run()` function alongside a Neo4j connection. Layers are resolved in dependency order automatically.

Each layer owns one slice of the graph schema. The five layers together build the complete WMATA knowledge graph — physical infrastructure, service schedule, fare structure, real-time outages, and real-time service disruptions.

See [`src/layers/README.md`](src/layers/README.md) for the full layer overview.

### LLM Query Pipeline

The LLM pipeline accepts a natural language question and returns a prose answer grounded in the graph. The pipeline runs these stages in sequence:

```
Question
  → Planner (domain + path routing + anchor extraction)
  → AnchorResolver (station/route/date → node IDs)
  → [AnchorClarifier: LLM repair pass for failed lookups]
  → Text2Cypher path: QueryWriter → CypherValidator (3-attempt retry)
  → Subgraph path:    HopExpander → ContextSerializer
  → NarrationAgent (selects mode, makes final LLM call)
  → Prose answer
```

An agentic variant (`src/llm/agent.py`) replaces the fixed fork with a Claude tool-use loop that selects graph retrieval tools dynamically. Both pipelines share the Planner and NarrationAgent.

---

## Setup

**Prerequisites:** Python 3.14+, [`uv`](https://docs.astral.sh/uv/), a running Neo4j instance (Community Edition 5.x or later).

```bash
git clone <repo-url> && cd journeygraph
cp env.example .env   # fill in your values
```

**Install dependencies:**
```bash
uv sync                           # ETL pipeline only
uv sync --extra llm               # + LLM query pipeline
uv sync --extra demo              # + Jupyter demo notebooks
uv sync --extra llm --extra demo  # everything
uv sync --group dev               # + dev tools (ruff, pytest)
```

**.env** — minimum required variables:
```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
WMATA_API_KEY=your_wmata_api_key
```

For the LLM pipeline, also add:
```
ANTHROPIC_API_KEY=your_anthropic_api_key
```

Optional overrides (defaults shown):
```
GTFS_FEED_URL=https://api.wmata.com/gtfs/rail-bus-gtfs-static.zip
LLM_MODEL=claude-haiku-4-5-20251001
LLM_MAX_TOKENS=512
LLM_NARRATION_MAX_TOKENS=1024
```

---

## GTFS Static Feed

Download the WMATA GTFS feed before running any layers. It lands in `data/gtfs/` (git-ignored).

```bash
# Download only — does not touch Neo4j
uv run python -m src.pipeline --download-only

# Force re-download when the feed has been updated
uv run python -m src.pipeline --force-download
```

Once downloaded, subsequent runs use the cached files automatically:
```bash
uv run python -m src.pipeline --layers fare   # uses cached feed
```

> WMATA publishes new feeds roughly every 6 months. Check `feed_start_date` / `feed_end_date` in `data/gtfs/feed_info.txt` to know if yours is stale.

---

## Running the ETL Pipeline

```bash
# Run all layers
uv run python -m src.pipeline

# Run specific layers — dependencies resolved automatically
uv run python -m src.pipeline --layers fare
uv run python -m src.pipeline --layers fare --with-deps    # include upstream
uv run python -m src.pipeline --layers fare --cascade      # include downstream

# Preview execution plan without writing to the DB
uv run python -m src.pipeline --layers fare --with-deps --dry-run
```

**Layer dependency order:** `physical` → `service_schedule` + `fare` → `interruption` + `accessibility`

---

## Running the LLM Pipeline

```bash
# Single question
uv run python -m src.llm.run "how many trips were cancelled on the red line yesterday"

# Interactive REPL
uv run python -m src.llm.run --repl

# Smoke test — one question per domain + one rejection
uv run python -m src.llm.run --demo

# Agentic mode (tool-use loop instead of static fork)
uv run python -m src.llm.run "which stations are the biggest choke points" --agentic
```

See [`src/llm/README.md`](src/llm/README.md) for full CLI reference, pipeline architecture, and domain details.

---

## Project Structure

```
src/
├── common/              ← Shared utilities — config, Neo4j driver, logger, validators
│   └── README.md
├── ingest/              ← GTFS downloader + WMATA API client
├── layers/
│   ├── README.md        ← Layer overview and graph model summary
│   ├── physical/        ← Stops, pathways, levels, fare gates
│   ├── service_schedule/← Routes, trips, service calendar, stop sequences
│   ├── fare/            ← Fare zones, products, media, transfer rules
│   ├── accessibility/   ← Elevator/escalator outage events (WMATA Incidents API)
│   └── interruption/    ← Real-time cancellations, delays, skips (GTFS-RT)
├── llm/                 ← LLM query pipeline
│   └── README.md        ← Full architecture, CLI, and domain reference
└── pipeline.py          ← ETL entry point — download + layer orchestration
data/
├── raw/                 ← Downloaded zips (git-ignored)
└── gtfs/                ← Extracted GTFS CSVs (git-ignored)
queries/                 ← Cypher query library — one folder per layer
│   └── README.md
tests/
├── eval/                ← End-to-end LLM eval framework
│   └── README.md
└── llm/ + *.py          ← Unit tests
demos/                   ← Jupyter notebooks (GDS path-finding demos)
CONVENTIONS.md           ← WMATA-specific feed conventions and assumptions
```

---

## Adding a New Layer

The only hard requirement is a `run(gtfs_data, neo4j)` function in your layer's `__init__.py`:

```python
# src/layers/my_layer/__init__.py
import pandas as pd
from src.common.neo4j_tools import Neo4jManager

def run(gtfs_data: dict[str, pd.DataFrame], neo4j: Neo4jManager) -> None:
    stops = gtfs_data["stops"]   # any GTFS file is available here
    # ... write nodes and relationships to neo4j
```

**Register the layer** in `src/common/layers.py`:
```python
class Layer(str, Enum):
    MY_LAYER = "my_layer"

DEPENDENCIES = {
    Layer.MY_LAYER: [Layer.PHYSICAL],  # layers that must run first; [] if none
}
```

**Four rules to follow:**

1. **Config** — never import config constants at module level. Always call `get_config()` inside a function to avoid import-time failures when `.env` is absent.

2. **Neo4jManager** — instantiate inside `run()`, never at module level. Use `df_to_rows(df)` from `src.common.neo4j_tools` to convert DataFrames to parameter lists — it handles `NaN`/`NaT` → `None` conversion (Neo4j rejects numpy null types).

3. **Validators** — add pre- and post-load checks in `src/common/validators/` following the existing layer validators. Name them `validate_pre_transform` and `validate_post_load`. Use `run_count_check(neo4j, cypher)` from `base.py` for COUNT assertions.

4. **Cypher** — put `.cypher` files under `queries/<layer>/` and load them with `PROJECT_ROOT / "queries" / layer / "name.cypher"`. Use `MERGE` not `CREATE` for idempotency. See [`queries/README.md`](queries/README.md) for full conventions.

---

## Development

```bash
uv run ruff check src/   # lint
uv run ruff format src/  # format
uv run pytest            # run tests with coverage
uv run pytest -m slow    # include slow tests (real GTFS data)
```

VS Code: install the recommended extensions when prompted (`.vscode/extensions.json`) — ruff lints and formats on save automatically.

WMATA-specific feed conventions (stop ID prefixes, zone derivation, service calendar quirks) are documented in [`CONVENTIONS.md`](CONVENTIONS.md).
