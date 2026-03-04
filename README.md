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

## GTFS Static Feed

The pipeline requires a local copy of the WMATA GTFS static feed before any layers can run. The feed is downloaded from the WMATA API using your `WMATA_API_KEY` and extracted into `data/gtfs/`.

**First-time setup — download only:**
```bash
uv run python -m src.pipeline --download-only
```

This downloads the zip to `data/raw/gtfs.zip`, extracts all CSVs into `data/gtfs/`, and exits without touching Neo4j. Both directories are git-ignored.

**Download and run in one command:**
```bash
# Download feed then run all layers
uv run python -m src.pipeline --download

# Download feed then run specific layers
uv run python -m src.pipeline --download --layers fare
```

**Subsequent runs** skip the download automatically if `data/gtfs/` already has files:
```bash
uv run python -m src.pipeline --layers fare   # uses cached feed
```

**Force a fresh download** when the feed has been updated (WMATA publishes new feeds roughly every 6 months — check `feed_start_date` / `feed_end_date` in `data/gtfs/feed_info.txt`):
```bash
uv run python -m src.pipeline --force-download
```

> The default feed URL points to `https://api.wmata.com/gtfs/rail-bus-gtfs-static.zip`. To override (e.g. for a local feed file during development), set `GTFS_FEED_URL` in your `.env`.

---

## Running the Pipeline

```bash
# Full run (all layers) — assumes feed already downloaded
uv run python -m src.pipeline

# Specific layers — dependencies resolved automatically
# e.g. requesting fare also runs physical first
uv run python -m src.pipeline --layers fare
uv run python -m src.pipeline --layers physical fare

# Check what would run without executing anything
uv run python -m src.pipeline --layers fare --dry-run
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
├── __init__.py   # exposes run(gtfs_data, neo4j)
├── extract.py    # pulls relevant keys from gtfs_data dict
├── transform.py  # cleans data + runs pre-load validation (raises on failure)
└── load.py       # writes to Neo4j + runs post-load validation (raises on failure)
```

**1. Register the layer** in `src/common/layers.py`:
```python
class Layer(str, Enum):
    MY_LAYER = "my_layer"           # add here

DEPENDENCIES = {
    Layer.MY_LAYER: [Layer.PHYSICAL],  # list layers that must run first
}
```

**2. Follow the config rule** — never import config constants at module level. Always call `get_config()` inside a function:
```python
# ✅ correct — called inside a function, only when needed
def download():
    config = get_config()
    requests.get(config.gtfs_feed_url, ...)

# ❌ wrong — raises at import time if .env is missing, blocks --download-only
from src.common.config import GTFS_FEED_URL
```

**3. Keep Neo4jManager lazy** — instantiate it inside `run()`, never at module level.

**4. Wire pre- and post-load validators** in `src/common/validators/` following the pattern in `fare_zones.py`. Pre-load runs at the end of `transform.py` and raises `ValueError` on failure. Post-load runs at the end of `load.py` after all writes complete.

---

## Development

```bash
uv run ruff check src/   # lint
uv run ruff format src/  # format
uv run pytest            # run tests with coverage
```

VS Code: install the recommended extensions when prompted (`.vscode/extensions.json`) — ruff will lint and format on save automatically.
