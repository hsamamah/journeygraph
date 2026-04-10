# src/common — Shared Utilities

Cross-layer utilities used by every part of the codebase: configuration, Neo4j driver, path resolution, logging, and pre/post-load validators.

---

## Files

| File | Purpose |
|---|---|
| `config.py` | Environment variable loading. `get_config()` → `Config` (ETL). `get_llm_config()` → `LLMConfig` (LLM pipeline). Both configs are frozen dataclasses — call them inside functions, never at module level. |
| `neo4j_tools.py` | `Neo4jManager` — thin wrapper around the Neo4j Bolt driver. Manages connection lifecycle, exposes `query(cypher, params)` and `execute_queries(statements)`. `df_to_rows(df)` converts a pandas DataFrame to a `list[dict]` with `NaN`/`NaT` replaced by `None` for safe Cypher parameter injection. |
| `paths.py` | `PROJECT_ROOT` constant — absolute path to the repository root, resolved at import time. Used by every module that loads files relative to the project (e.g. `PROJECT_ROOT / "queries" / "physical" / "nodes.cypher"`). |
| `logger.py` | `get_logger(name)` — returns a standard Python logger pre-configured with a consistent format. Pass `__name__` as the argument. |
| `layers.py` | `Layer` enum and `resolve_layers()` — layer dependency graph and topological sort for `pipeline.py`. See [Adding a New Layer](../../README.md#adding-a-new-layer). |
| `cross_layer.py` | Cross-layer utilities shared between multiple layers (e.g. shared route/stop lookups). |
| `feed_info.py` | Helpers for parsing and validating GTFS `feed_info.txt` metadata. |
| `utils.py` | Miscellaneous small helpers (date parsing, string normalisation). |
| `validators/` | Per-layer pre-transform and post-load validator functions — see below. |

---

## Configuration

Two configs, intentionally separate:

**`get_config()` → `Config`** — used by the ETL pipeline (`src/pipeline.py` and all layers). Requires:
```
NEO4J_URI        bolt://localhost:7687
NEO4J_USER       neo4j
NEO4J_PASSWORD   your_password
WMATA_API_KEY    your_wmata_api_key
```
Optional: `GTFS_FEED_URL` (defaults to WMATA static GTFS zip URL).

**`get_llm_config()` → `LLMConfig`** — used only by `src/llm/`. Requires:
```
ANTHROPIC_API_KEY   sk-ant-...
```
Optional: `LLM_PROVIDER`, `LLM_MODEL`, `LLM_MAX_TOKENS`, `LLM_NARRATION_MAX_TOKENS`.

Never call either config function at module level — always call inside a function so the pipeline can start without a `.env` file during testing.

---

## Neo4jManager

```python
from src.common.neo4j_tools import Neo4jManager, df_to_rows
from src.common.config import get_config

config = get_config()
with Neo4jManager(config) as neo4j:
    rows = neo4j.query("MATCH (s:Station) RETURN s.name AS name LIMIT 5")
    # rows is a list[dict]
```

**`df_to_rows(df)`** — always use this when passing a DataFrame as Cypher parameters. It handles `NaN`, `NaT`, and `pd.NA` → `None`, which Neo4j requires (it rejects numpy null types).

```python
params = df_to_rows(stations_df)
neo4j.execute_queries([("MERGE (s:Station {id: $id}) SET s.name = $name", params)])
```

---

## Validators

`src/common/validators/` holds pre-transform and post-load validation functions for each layer. Each validator follows this pattern:

```python
# src/common/validators/physical.py
def validate_pre_transform(gtfs_data: dict, strict: bool = False) -> None:
    """Checks on raw GTFS data — called after extract, before transform."""
    ...

def validate_post_load(neo4j: Neo4jManager) -> None:
    """Checks on the live graph — called after all Neo4j writes."""
    ...
```

`run_count_check(neo4j, cypher, label)` from `validators/base.py` is a helper for post-load COUNT queries — it runs the Cypher and raises if the count is zero.

---

## Layer Dependency Graph

```
physical
├── service_schedule  (depends on: physical)
├── fare              (depends on: physical)
├── interruption      (depends on: physical, service_schedule, fare)
└── accessibility     (depends on: physical, service_schedule, fare)
```

Run `uv run python -m src.pipeline --layers fare --dry-run` to print the resolved execution plan without writing to the database.
