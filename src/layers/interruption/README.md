# Interruption Layer

Ingests real-time WMATA GTFS-RT feeds (trip updates and service alerts) and loads them into a three-tier disruption model: raw source nodes for audit, semantic `Interruption` nodes for querying, and typed relationships to the service graph.

---

## Overview

**Must run after:** physical, service_schedule, and fare layers.

**Data sources:** WMATA GTFS-RT feeds fetched via `WMATAClient` on each pipeline run — `TripUpdates` (per-trip cancellations, delays, skipped stops) and `ServiceAlerts` (free-text announcements).

This layer is **live** — it fetches from the WMATA API at the time `pipeline.py` runs, not from cached GTFS files.

---

## Three-Tier Disruption Model

The layer separates raw API data from semantic disruption events:

**Tier 1 — Raw source nodes** (audit trail, exact API data):
- `TripUpdate` — one node per trip update message
- `StopTimeUpdate` — one node per stop-level time update
- `ServiceAlert` — one node per service alert message
- `EntitySelector` — one node per `informed_entity` selector within an alert

**Tier 2 — Semantic disruption nodes** (queryable by type and severity):
- `Interruption` with one or more subtypes: `:Cancellation`, `:Delay`, `:Skip`, `:Detour`, `:ServiceChange`, `:Accessibility`

**Tier 3 — Impact relationships** (links disruptions to the service graph):

| Relationship | Source | Meaning |
|---|---|---|
| `AFFECTS_TRIP` | Interruption → Trip | This disruption affects a specific trip |
| `AFFECTS_ROUTE` | Interruption → Route | This disruption affects an entire route |
| `AFFECTS_STOP` | Interruption → Platform\|BusStop | Stop-level impact (defined in schema; may be sparse in live data) |
| `ON_DATE` | Interruption → Date | The date this disruption was observed |
| `DURING_PLANNED_SERVICE` | Interruption → ServicePattern | The service window this disruption falls within |
| `SOURCED_FROM` | Interruption → TripUpdate\|ServiceAlert | Link back to the raw source node |

---

## Interruption Classification

TripUpdate schedule relationships map to Interruption subtypes:

| Condition | Interruption subtype | Severity |
|---|---|---|
| `schedule_relationship = CANCELED` | `:Cancellation` | SEVERE |
| `arrival_delay` or `departure_delay ≥ 300 s` | `:Delay` | WARNING (300–899 s) or SEVERE (≥ 900 s) |
| Stop-level `schedule_relationship = SKIPPED` | `:Skip` | WARNING |
| ServiceAlert `effect = DETOUR` | `:Detour` | — |
| ServiceAlert `effect = OTHER_EFFECT` or misc | `:ServiceChange` | — |
| ServiceAlert `effect = ACCESSIBILITY_ISSUE` | `:Accessibility` | — |

The 5-minute (300 s) threshold for delays is consistent with WMATA's customer impact definitions and is referenced in the LLM pipeline's domain framing.

---

## Idempotency

Every node is loaded via `MERGE` — running the interruption layer twice on the same GTFS-RT snapshot does not create duplicate nodes. Trip updates are deduplicated on a hash of `(trip_id, start_date, schedule_relationship, delay, stop-level states)`. Service alerts deduplicate on `feed_entity_id`.

---

## ELT Stages

| Stage | File | Input | Output |
|---|---|---|---|
| **Extract** | `extract.py` | `WMATAClient` | `trip_updates`, `stop_time_updates`, `service_alerts`, `entity_selectors` DataFrames |
| **Transform** | `transform.py` | Raw DataFrames | Deduplicated DataFrames, Interruption classification, severity assignments |
| **Load** | `load.py` + `queries/interruption/` | Transformed DataFrames | Tier 1 + Tier 2 nodes, Tier 3 relationships |

---

## File Structure

| File | Purpose |
|---|---|
| `__init__.py` | Orchestrator — calls extract → transform → load |
| `extract.py` | Fetches GTFS-RT via `WMATAClient`, flattens protobuf to DataFrames |
| `transform.py` | Deduplication, Interruption type/severity classification |
| `load.py` | Writes all three tiers using Cypher from `queries/interruption/` |

Cypher files live in `queries/interruption/`:

| File | Purpose |
|---|---|
| `constraints.cypher` | Uniqueness constraints on raw source node keys |
| `nodes.cypher` | `MERGE` for TripUpdate, StopTimeUpdate, ServiceAlert, EntitySelector, Interruption subtypes |
| `relationships.cypher` | `MERGE` for all Tier 3 impact relationships |

---

## Known data characteristics

- `:Skip` interruptions in the live graph are currently only present on Orange (`O`) and Yellow (`Y`) rail lines
- `:Delay` interruptions in the live graph are currently bus-only — no rail delay data observed
- Bus `TripUpdate` messages frequently omit `start_date`, causing `ON_DATE` relationships to be absent for those trips (this is expected WMATA feed behaviour, not a data quality issue)
- `AFFECTS_STOP` is defined in the schema but may not be populated in current data — use `AFFECTS_ROUTE` or `AFFECTS_TRIP` for Cypher queries
