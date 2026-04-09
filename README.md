# JourneyGraph

A Neo4j knowledge graph of the WMATA Washington DC Metro transit system, built on GTFS static feeds and GTFS-RT real-time data. The project has two main components:

- **ETL pipeline** (`src/pipeline.py`) — ingests GTFS static and WMATA API data into Neo4j across five domain layers
- **LLM query pipeline** (`src/llm/`) — natural language querying over the graph via a multi-agent pipeline

> **Current state:** All five ETL layers are implemented. The LLM pipeline is complete through L6 — Planner (L3), Subgraph Context Builder (L5), and Narration Agent (L6). An eval framework (`tests/eval/`) supports config-matrix harness runs with LLM-as-judge scoring — see [`src/llm/README.md`](src/llm/README.md) for details.

---

## How It Works

`pipeline.py` is the ETL entry point. It handles two things:

1. **Downloading the GTFS feed** — fetches the WMATA static GTFS zip, extracts it, and parses every CSV into a shared `dict[str, pd.DataFrame]` keyed by filename stem (e.g. `gtfs_data["stops"]`, `gtfs_data["fare_leg_rules"]`).

2. **Running domain layers** — passes that dict to each layer's `run()` function alongside a Neo4j connection. Layers are resolved in dependency order automatically.

Each layer is responsible for its own slice of the graph. The pipeline doesn't care how a layer is structured internally — it only calls `run(gtfs_data, neo4j)` from the layer's `__init__.py`.

The Fare layer (`src/layers/fare/`) is a good reference implementation. It splits the work into three files — `extract.py`, `transform.py`, `load.py` — but that structure is a convention, not a requirement.

---

## Setup

**Prerequisites:** Python 3.14+, [`uv`](https://docs.astral.sh/uv/), a running Neo4j instance.

```bash
git clone <repo-url> && cd journeygraph
cp .env.example .env   # fill in your values
```

**Install dependencies:**
```bash
uv sync                        # ETL pipeline only
uv sync --extra llm            # + LLM query pipeline
uv sync --extra demo           # + Jupyter demo notebooks
uv sync --extra llm --extra demo  # everything
uv sync --group dev            # + dev tools (ruff, pytest)
```

**.env** — see `.env.example` for all variables. Minimum required:
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

> The default feed URL is `https://api.wmata.com/gtfs/rail-bus-gtfs-static.zip`. Override by setting `GTFS_FEED_URL` in your `.env`.
> WMATA publishes new feeds roughly every 6 months — check `feed_start_date` / `feed_end_date` in `data/gtfs/feed_info.txt` to know if yours is stale.

---

## Running the Pipeline

```bash
# Run all layers
uv run python -m src.pipeline

# Run specific layers — dependencies resolved automatically
uv run python -m src.pipeline --layers fare
uv run python -m src.pipeline --layers fare --with-deps    # include upstream
uv run python -m src.pipeline --layers fare --cascade      # include downstream
```

---

## Project Structure

```
src/
├── common/         # Shared utilities (logger, Neo4j driver, config, paths, validators)
├── ingest/         # GTFS downloader + WMATA API client
├── layers/
│   ├── physical/         # Stops, pathways, levels              [implemented]
│   ├── service_schedule/ # Routes, trips, calendar              [implemented]
│   ├── fare/             # Fare products, zones, rules, media   [implemented]
│   ├── accessibility/    # Elevator/escalator outage events     [implemented]
│   └── interruption/     # Real-time service disruptions        [implemented]
├── llm/            # LLM query pipeline — see src/llm/README.md
└── pipeline.py     # ETL entry point — download + layer orchestration
data/
├── raw/            # Downloaded zips (git-ignored)
└── gtfs/           # Extracted GTFS CSVs (git-ignored)
queries/            # Cypher query library (one folder per layer)
tests/
├── eval/           # End-to-end eval framework — harness, scorer, question sets
└── unit/           # Unit tests
demos/              # Jupyter demo notebooks
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

The Fare layer splits this into `extract.py → transform.py → load.py` which is a good pattern for anything non-trivial — but a single file is fine for simpler layers.

**Register the layer** in `src/common/layers.py`:
```python
class Layer(str, Enum):
    MY_LAYER = "my_layer"

DEPENDENCIES = {
    Layer.MY_LAYER: [Layer.PHYSICAL],  # layers that must run first; [] if none
}
```

**Four rules to follow:**

1. **Config** — never import config constants at module level. Always call `get_config()` inside a function.
```python
# ✅ correct
def run(...):
    config = get_config()

# ❌ wrong — breaks pipeline startup without .env
from src.common.config import WMATA_API_KEY
```

2. **Neo4jManager** — instantiate inside `run()`, never at module level. Use `df_to_rows(df)` from `src.common.neo4j_tools` to convert DataFrames to parameter lists before passing to Cypher — it handles `NaN`/`NaT` → `None` conversion.

3. **Validators** — add pre- and post-load checks in `src/common/validators/` following `fare_zones.py`. Name them `validate_pre_transform` (called after extract, before transform) and `validate_post_load` (called after all Neo4j writes). Use `run_count_check(neo4j, cypher)` from `base.py` for post-load COUNT queries.

4. **Cypher** — put `.cypher` files under `queries/<layer>/` and load them with the `load_query()` pattern from `queries/README.md`.

---

## Development

```bash
uv run ruff check src/   # lint
uv run ruff format src/  # format
uv run pytest            # run tests with coverage
```

VS Code: install the recommended extensions when prompted (`.vscode/extensions.json`) — ruff will lint and format on save automatically.
