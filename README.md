# JourneyGraph

ETL pipeline that loads WMATA transit data into a Neo4j knowledge graph. Ingests GTFS static feeds and real-time WMATA API data across four domain layers: Physical Infrastructure, Service & Schedule, Fare, and Accessibility.

---

## Setup

**Prerequisites:** Python 3.14+, [`uv`](https://docs.astral.sh/uv/), a running Neo4j instance.

```bash
git clone <repo-url> && cd journeygraph
uv sync --group dev
```

**.env**
```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
WMATA_API_KEY=your_api_key
```

---

## Running the Pipeline [TO-DO]

```bash
# Full run (all layers)
uv run python -m src.pipeline

# Specific layers only
uv run python -m src.pipeline --layers physical fare

# Force re-download and re-extract GTFS data
uv run python -m src.pipeline --force-download --force-extract
```

---

## Project Structure

```
src/
├── common/         # Shared utilities (logger, Neo4j driver, config, paths)
├── ingest/         # GTFS downloader + WMATA API client
├── layers/
│   ├── physical/         # Stops, pathways, levels
│   ├── service_schedule/ # Routes, trips, calendar
│   ├── fare/             # Fare products, rules, media
│   └── accessibility/    # Elevator/escalator outage events
└── pipeline.py     # Entry point
data/
├── raw/            # Downloaded zips and API dumps (git-ignored)
└── gtfs/           # Extracted GTFS CSVs (git-ignored)
queries/            # Shared Cypher query library
tests/
```

---

## Adding a New Layer

Each layer follows the same three-file contract:

```
layers/<name>/
├── __init__.py   # exposes run(gtfs_data)
├── extract.py    # pulls relevant keys from gtfs_data dict
├── transform.py  # shapes data into graph-ready dicts
└── load.py       # writes to Neo4j via Neo4jManager
```

Register it in `pipeline.py` by adding its name to `LAYER_ORDER`.

---

## Development 

```bash
uv run ruff check src/   # lint
uv run ruff format src/  # format
uv run pytest            # run tests with coverage
```

VS Code: install the recommended extensions when prompted (`.vscode/extensions.json`) — ruff will lint and format on save automatically.