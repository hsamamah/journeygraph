# Physical Infrastructure Layer
Builds the physical transit graph from GTFS source data — stations, entrances, platforms, fare gates, bus stops, pathways, and levels — as Neo4j nodes and relationships.

---

## Overview

The physical layer is the foundation all other layers build on. It loads the static infrastructure of the transit network: the physical spaces you move through (stations, platforms, entrances) and the connections between them (pathways). Every node created here is referenced by the fare, service schedule, and accessibility layers.

Entry point is `run()` in `__init__.py`, called by `pipeline.py`. It orchestrates a strict ELT sequence with two validation gates.

**Must run before:** fare layer, accessibility layer.

---

## ELT Stages

| Stage | File | Input | Output |
|---|---|---|---|
| **Extract** | `extract.py` | `gtfs_data` dict (from ingest) | `stops`, `pathways`, `levels`, `feed_info` DataFrames |
| **Pre-transform validation** | `validators/physical.py` | Raw GTFS DataFrames | Pass/fail — blocks pipeline on failure |
| **Transform** | `transform.py` | Raw DataFrames | Cleaned, partitioned, link-ready DataFrames |
| **Load** | `load.py` + `queries/physical/` | Transformed DataFrames | Neo4j nodes, relationships, constraints |
| **Post-load validation** | `validators/physical.py` | Neo4j (live query) | Pass/fail — blocks pipeline on failure |

---

## Graph Model

### Nodes

| Label | Key property | GTFS source | Additional labels |
|---|---|---|---|
| `Station` | `id` | `stops.txt` `location_type=1` | — |
| `StationEntrance` | `id` | `stops.txt` `location_type=2`, prefix `ENT_` | — |
| `Platform` | `id` | `stops.txt` `location_type=0/4`, prefix `PF_` | — |
| `FareGate` | `id` | `stops.txt` pattern `_FG_` in `stop_id` | — |
| `BusStop` | `id` | `stops.txt`, numeric `stop_id`, not `location_type=3` | — |
| `Pathway` | `id` | `pathways.txt` | `:Escalator` (mode 4), `:Elevator` (mode 5), `:Stairs` (mode 2), `:Walkway` (mode 1), `:Paid`, `:Unpaid` |
| `Level` | `level_id` | `levels.txt` | — |

`Pathway` nodes carry `from_stop_id`, `to_stop_id`, `from_stop_desc`, `to_stop_desc`, `mode`, `is_bidirectional`, `zone`, `elevation_gain`, and `wheelchair_accessible`. The `from_stop_desc` / `to_stop_desc` properties hold the human-readable GTFS `stop_desc` of each endpoint's `NODE_` stop — used by the accessibility layer's pathway join to match outage descriptions against segment vocabulary.

All node `id` values are the GTFS `stop_id` (renamed by transform). Uniqueness constraints are applied on first load — see `queries/physical/constraints.cypher`.

### Relationships

| Pattern | Source |
|---|---|
| `(Station)-[:CONTAINS]->(StationEntrance)` | `parent_station` in `stops.txt` |
| `(Station)-[:CONTAINS]->(Platform)` | `parent_station` in `stops.txt` |
| `(Station)-[:CONTAINS]->(FareGate)` | `parent_station` in `stops.txt` |
| `(StationEntrance)-[:LINKS]->(Pathway)` | `from_stop_id` of pathway |
| `(Platform)-[:LINKS]->(Pathway)` | `from_stop_id` of pathway |
| `(Station)-[:LINKS]->(Pathway)` | `from_stop_id` of pathway |
| `(FareGate)-[:LINKS]->(Pathway)` | `from_stop_id` of pathway |
| `(BusStop)-[:LINKS]->(Pathway)` | `from_stop_id` of pathway |
| `(Pathway)-[:LINKS]->(StationEntrance)` | `to_stop_id` of pathway |
| `(Pathway)-[:LINKS]->(Platform)` | `to_stop_id` of pathway |
| `(Pathway)-[:LINKS]->(Station)` | `to_stop_id` of pathway |
| `(Pathway)-[:LINKS]->(FareGate)` | `to_stop_id` of pathway |
| `(Pathway)-[:LINKS]->(BusStop)` | `to_stop_id` of pathway |
| `(Pathway)-[:LINKS]->(Pathway)` | Deferred generic node pivot (see below) |

`LINKS` is always directional: the from-entity links into the Pathway, and the Pathway links out to the to-entity. Bidirectional pathways (`is_bidirectional=1`) receive reverse links on both stop-entity sides and both pathway-chain sides.

---

## Pathway Zone Classification

Each `Pathway` node carries a `zone` property (`'Paid'` or `'Unpaid'`) and a corresponding multi-label (`:Paid` or `:Unpaid`). This is derived entirely in `transform.py` — there is no zone column in the GTFS source.

### Zone anchors

Every stop in `stops.txt` is tagged with a side before pathways are categorised:

| Side | Predicate |
|---|---|
| `UNPAID` | `stop_id` starts with `ENT_` (station entrances) |
| `UNPAID` | `stop_id` ends with `_UNPAID` (faregate unpaid-side nodes) |
| `PAID` | `stop_id` starts with `PF_` (platforms) |
| `PAID` | `stop_id` ends with `_PAID` (faregate paid-side nodes) |
| `UNKNOWN` | Everything else (generic mezzanine/pivot nodes) |

The `_PAID` / `_UNPAID` suffix convention is used by this feed for generic nodes (`location_type=3`) that sit immediately beside a fare barrier (e.g. `NODE_A01_C01_N_FG_PAID`). These nodes are not loaded as graph nodes but their zone side is known and used as anchors.

### Side category → zone

Each pathway is categorised from its `from_side` and `to_side`:

| `side_category` | Condition | `zone` property | Labels applied |
|---|---|---|---|
| `exit_gate` | mode 7, PAID→UNPAID | — | — |
| `cross_faregate` | mode 6, sides differ | — | — |
| `pregate_internal` | both UNPAID | `'Unpaid'` | `:Unpaid` |
| `postgate_internal` | both PAID | `'Paid'` | `:Paid` |
| `pregate_internal` | one UNPAID, other UNKNOWN | `'Unpaid'` | `:Unpaid` |
| `postgate_internal` | one PAID, other UNKNOWN | `'Paid'` | `:Paid` |
| `mixed_non_gate` | both UNKNOWN, or contradictory | — | — |

**One-anchor rule:** this feed has no direct PAID→PAID or UNPAID→UNPAID pathway hops — every paid-zone pathway connects a known anchor (a `_PAID` node or `PF_*` platform) to a generic intermediate node (`PLF_*` boarding areas, mezzanine `NODE_*` nodes). The one-anchor rule assigns zone when one side is known and the other is `UNKNOWN`, as long as no conflicting zone is present on the other side.

Pathways where both endpoints are `UNKNOWN` (deep mezzanine hops with no direct zone anchor) remain unlabelled (`mixed_non_gate`) and carry no `:Paid`/`:Unpaid` label. Extending zone labels to stop-derived nodes (`:Platform`, `:StationEntrance`, etc.) is deferred until a downstream use case requires it.

---

## Endpoint Classification

Every `from_stop_id` and `to_stop_id` in `pathways.txt` is classified before any links are built. Classification happens in `endpoint_classifier.py` and gates the entire transform.

| Class | Meaning | Action |
|---|---|---|
| `MATCHED` | Stop exists in a loaded node partition (Station, StationEntrance, Platform, FareGate, BusStop) | `LINKS` relationship created |
| `DEFERRED` | Stop is a GTFS generic node (`location_type=3`, no `_FG_`) — an infrastructure pivot, not a physical space | No node loaded; used for Pathway→Pathway chaining |
| `GAP` | Stop is in `stops.txt` but matches no partition predicate | **Blocks pipeline** — indicates a classification bug |
| `MISSING` | Stop is not in `stops.txt` at all | **Blocks pipeline** — indicates broken source data |

A `GAP` result means the partition predicates in `transform.py` and `endpoint_classifier.py` are out of sync with a new stop ID convention. It must be fixed before the pipeline can proceed.

---

## Pathway Chain Links

GTFS uses **generic nodes** (`location_type=3`) as infrastructure pivots — intermediate points that represent a physical location (e.g. the bottom of an escalator) without being a transit stop. These nodes are not loaded into the graph.

Instead, when two pathways share a deferred pivot — `pw_X.to_stop_id == pw_Y.from_stop_id`, both DEFERRED — a direct `(pw_X)-[:LINKS]->(pw_Y)` relationship is created, collapsing the pivot.

**Bidirectional expansion:** a bidirectional pathway can be traversed in either direction, so its `from_stop` is also a valid *exit* and its `to_stop` is also a valid *entry*. The chain computation includes both orientations. This is necessary for escalator segments where both endpoints (`_ESC*_BT`, `_ESC*_TP`) are DEFERRED and the adjacent access pathways are bidirectional.

```
Concrete example (A03 North escalator 4):

  [FareGate]──(A03_106131, bidir)──> NODE_ESC4_TP
                                          │
                                    A03_106132 (escalator, unidir, BT→TP)
                                          │
  NODE_ESC4_BT ──(A03_106133, bidir)──> [PLF_GLENMONT] ──> [PF_A03_1]

Chain links built:
  A03_106133 → A03_106132  (pivot: NODE_ESC4_BT, via bidir reverse of 106133)
  A03_106132 → A03_106131  (pivot: NODE_ESC4_TP)
  + reverses for bidirectional participants
```

---

## Validation Gates

### Pre-transform (raw GTFS checks)

Runs after extract, before transform. All failures block the pipeline.

| # | Check | Severity |
|---|---|---|
| 1 | No duplicate `stop_id` in `stops.txt` | ❌ fail |
| 2 | All platforms/entrances/faregates reference a `parent_station` that exists | ❌ fail |
| 3 | All pathway endpoints classified as MATCHED or DEFERRED (no GAP or MISSING) | ❌ fail |
| 4 | All `pathway_mode` values within GTFS range 1–7 | ⚠ warn |
| 5 | At least one Station, one Platform, one FareGate present | ❌ fail |

### Post-load (graph integrity checks)

Runs after all nodes and relationships are written. Failures 6–9 and 11 block the pipeline.

| # | Check | Severity |
|---|---|---|
| 6–8 | No duplicate `id` on Station, Platform, FareGate nodes | ❌ fail |
| 9 | Every Pathway participates in at least one `[:LINKS]` relationship | ❌ fail |
| 10 | Every Station has at least one `[:CONTAINS]->(:Platform)` | ⚠ warn |
| 11 | All mode=4 Pathway nodes carry `:Escalator`; mode=5 carry `:Elevator` | ❌ fail |
| 12 | Node counts by label | ℹ info |

---

## File Structure

| File | Purpose |
|---|---|
| `__init__.py` | Orchestrator — runs extract → validate → transform → load in order |
| `extract.py` | Pulls `stops`, `pathways`, `levels`, `feed_info` from the shared `gtfs_data` dict |
| `transform.py` | Cleans stops and pathways, partitions nodes by type, classifies zone anchors, categorises pathway zones, classifies endpoints, builds directional link frames and pathway chain links |
| `load.py` | Writes all nodes and relationships to Neo4j using parameterised Cypher from `queries/physical/`. Uses `df_to_rows()` from `src.common.neo4j_tools` for `NaN`/`NaT` → `None` conversion. |
| `endpoint_classifier.py` | `EndpointClass` enum and `classify_endpoints()` — classifies each pathway endpoint as MATCHED, DEFERRED, GAP, or MISSING |

Cypher files live in `queries/physical/` (outside this package):

| File | Purpose |
|---|---|
| `constraints.cypher` | Uniqueness constraints and indexes — applied once on first load |
| `nodes.cypher` | `MERGE` statements for all node labels, plus Pathway multi-label migrations (mode labels and `:Paid`/`:Unpaid` zone labels) |
| `relationships.cypher` | `MERGE` statements for all `CONTAINS`, `LINKS` relationships |
