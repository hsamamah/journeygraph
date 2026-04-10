# src/layers — ETL Domain Layers

Five domain layers that together build the complete WMATA knowledge graph. Each layer is responsible for one slice of the graph schema. Layers run in dependency order; the physical layer is the foundation everything else builds on.

---

## Layer Overview

| Layer | What it builds | Data source | Depends on |
|---|---|---|---|
| [`physical`](physical/README.md) | Stations, platforms, entrances, pathways, levels | GTFS `stops.txt`, `pathways.txt`, `levels.txt` | — |
| [`service_schedule`](service_schedule/README.md) | Routes, trips, service calendar, stop sequences | GTFS `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt` | physical |
| [`fare`](fare/README.md) | Fare zones, products, media, transfer rules | GTFS `fare_*.txt` | physical |
| [`accessibility`](accessibility/README.md) | Elevator/escalator outage events | WMATA Incidents REST API (live) | physical, service_schedule, fare |
| [`interruption`](interruption/README.md) | Real-time cancellations, delays, skips, service alerts | WMATA GTFS-RT (live) | physical, service_schedule, fare |

---

## Execution Model

Every layer exposes a single entry point in its `__init__.py`:

```python
def run(gtfs_data: dict[str, pd.DataFrame], neo4j: Neo4jManager) -> None:
    ...
```

`pipeline.py` calls this function after resolving the dependency order. Layers that pull from live APIs (accessibility, interruption) ignore `gtfs_data` and call `WMATAClient` directly inside `run()`.

Each layer follows the **ELT** pattern internally:

```
Extract   → pull from gtfs_data dict or API
    ↓
[Pre-transform validation]
    ↓
Transform → clean, normalise, classify, derive new columns
    ↓
Load      → MERGE nodes and relationships using parameterised Cypher
    ↓
[Post-load validation]
```

Validation gates (`src/common/validators/`) run before and after the transform/load phase. A pre-transform failure blocks the layer before any writes touch the database.

---

## Running layers

```bash
# Run all layers (resolves dependencies automatically)
uv run python -m src.pipeline

# Run one layer only
uv run python -m src.pipeline --layers fare

# Run a layer and everything it depends on
uv run python -m src.pipeline --layers fare --with-deps

# Preview the execution plan without writing anything
uv run python -m src.pipeline --layers fare --with-deps --dry-run
```

---

## Graph model at a glance

The five layers together build this node/relationship topology:

```
(Agency)-[:OPERATES]->(Route:Rail|Bus)
(Route)-[:SERVES]->(Station)
(Route)-[:HAS_PATTERN]->(RoutePattern)
(RoutePattern)-[:HAS_TRIP]->(Trip)
(Trip)-[:SCHEDULED_AT]->(Platform|BusStop)
(Trip)-[:FOLLOWS]->(RoutePattern)
(ServicePattern)-[:ACTIVE_ON]->(Date)
(Trip)-[:OPERATED_ON]->(ServicePattern)

(Station)-[:CONTAINS]->(Platform|StationEntrance|FareGate|Pathway|Level)
(Station)-[:IN_ZONE]->(FareZone)
(FareGate)-[:BELONGS_TO]->(Station)
(Pathway:Elevator|Escalator|Stairs|Walkway)-[:LINKS]->(...)

(FareMedia)-[:ACCEPTS]->(FareProduct)
(FareLegRule)-[:APPLIES_PRODUCT]->(FareProduct)
(FareTransferRule)-[:FROM_LEG|TO_LEG]->(FareLegRule)

(OutageEvent)-[:AFFECTS]->(Pathway)          ← accessibility layer
(Interruption:Delay|Skip|...)-[:AFFECTS_TRIP|AFFECTS_ROUTE|...]->(...)  ← interruption layer
```

---

## Adding a new layer

See [Adding a New Layer](../../README.md#adding-a-new-layer) in the root README.
