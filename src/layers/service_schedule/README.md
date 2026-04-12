# Service Schedule Layer

Builds the service graph from GTFS static data — agencies, routes, route patterns, trips, stop sequences, and the service calendar that determines which trips run on which days.

---

## Overview

**Must run after:** physical layer (`:Platform` and `:BusStop` nodes required for `SCHEDULED_AT` relationships).

**Data sources:** GTFS static files — `agency.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, `calendar_dates.txt` (optional), `stops.txt`, `feed_info.txt`.

---

## ELT Stages

| Stage | File | Input | Output |
|---|---|---|---|
| **Extract** | `extract.py` | `gtfs_data` dict | `agency`, `routes`, `trips`, `stop_times`, `calendar`, `stops`, `feed_info` DataFrames |
| **Pre-transform validation** | `validators/service_schedule.py` | Raw DataFrames | Pass/fail |
| **Transform** | `transform.py` | Raw DataFrames | Cleaned DataFrames with derived columns (mode, multi-labels, service patterns, route patterns) |
| **Load** | `load.py` + `queries/service_schedule/` | Transformed DataFrames | Neo4j nodes, relationships, constraints |

---

## Graph Model

### Nodes

| Label | Key property | Notes |
|---|---|---|
| `FeedInfo` | `feed_publisher_name` | One node per GTFS feed version |
| `Agency` | `agency_id` | Transit operator (WMATA) |
| `Route` | `route_id` | Multi-label: `:Rail` or `:Bus` based on `route_type` |
| `RoutePattern` | `shape_id` | Group of trips sharing the same stop sequence shape |
| `Trip` | `trip_id` | One scheduled run |
| `ServicePattern` | `service_id` | Calendar service window. Multi-label: `:Weekday`, `:Saturday`, `:Sunday`, `:Holiday`, or `:Maintenance` |
| `Date` | `date` | Calendar date (YYYYMMDD). Connected to service patterns via `ACTIVE_ON`. |

### Relationships

| Pattern | Notes |
|---|---|
| `(Agency)-[:OPERATES]->(Route)` | One agency per feed |
| `(Route)-[:HAS_PATTERN]->(RoutePattern)` | Route → its shape variants |
| `(RoutePattern)-[:HAS_TRIP]->(Trip)` | Pattern → individual scheduled runs |
| `(Trip)-[:FOLLOWS]->(RoutePattern)` | Reverse of HAS_TRIP (for traversal from trip) |
| `(Trip)-[:SCHEDULED_AT]->(Platform\|BusStop)` | Each stop-time becomes one relationship |
| `(Trip)-[:OPERATED_ON]->(ServicePattern)` | Links trip to its calendar window |
| `(ServicePattern)-[:ACTIVE_ON]->(Date)` | Resolved dates this pattern runs |
| `(Route)-[:SERVES]->(Station)` | Derived: unique route → station mappings for GDS analysis |

---

## Service Calendar Resolution

The transform merges `calendar.txt` (day-of-week flags + date range) with `calendar_dates.txt` (per-date exceptions) to produce a flat `(service_id, date)` list. Each date that a service runs becomes an `ACTIVE_ON` relationship.

**Service pattern classification** (from calendar day flags):
- `Weekday` — runs Mon–Fri
- `Saturday` — runs Saturday only
- `Sunday` — runs Sunday only
- `Holiday` — all flags zero (no regular service; exception-only dates)
- `Maintenance` — `service_id` ends with `_R` (WMATA convention for maintenance windows)

When a date falls on a US federal holiday, the `ACTIVE_ON` relationship carries a `holiday_name` property (e.g. `"Independence Day"`).

---

## Route Pattern vs. Trip

WMATA publishes many trips sharing the same stop sequence. The transform groups trips by `shape_id` into a `RoutePattern` node, reducing graph size and enabling shape-based subgraph queries without traversing every individual trip.

Rail platform assignment: stops with `PF_` prefix are loaded as `:Platform`. Bus stops (numeric IDs) are loaded as `:BusStop`. The `SCHEDULED_AT` relationship target differs by mode — rail trips link to Platform, bus trips to BusStop.

---

## File Structure

| File | Purpose |
|---|---|
| `__init__.py` | Orchestrator — runs extract → validate → transform → load |
| `extract.py` | Pulls 7 GTFS files (+ optional calendar_dates) from the shared dict |
| `transform.py` | Calendar resolution, mode classification, route pattern derivation, stop sequence processing |
| `load.py` | Writes all nodes and relationships using Cypher from `queries/service_schedule/` |

Cypher files live in `queries/service_schedule/`:

| File | Purpose |
|---|---|
| `constraints.cypher` | Uniqueness constraints + full-text index on route names |
| `nodes.cypher` | `MERGE` for all service node types |
| `relationships.cypher` | `MERGE` for all service relationships |
| `analytical.cypher` | Read-only service analysis queries |
