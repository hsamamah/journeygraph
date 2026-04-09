# Accessibility Layer
Ingests elevator and escalator outage data from the WMATA Incidents API and loads it into Neo4j as `:OutageEvent` nodes linked to the physical infrastructure graph.

---

## Overview

The accessibility layer serves two analytical purposes:

- **Operational monitoring** — which units are currently out of service, at which stations, and how severe
- **Infrastructure correlation** — how outage frequency and duration relate to station usage patterns (requires ridership data; schema is ready)

It introduces one new node type — `:OutageEvent` — which attaches to existing `:Pathway` nodes established by the Physical Infrastructure Layer. `:Pathway` was modeled as a node (not a relationship) in that layer specifically so outage events can anchor to it directly.

**Must run after:** physical layer (`:Pathway` and `:Station` nodes required).

---

## Data Source

**API endpoint:** `GET /Incidents.svc/json/ElevatorIncidents`

| API field | Status | Notes |
|---|---|---|
| `UnitName` | Retained | e.g. `'A02W03'`. Encodes station code + zone letter + sequence. Part of composite identity key. |
| `UnitType` | Retained | `'ELEVATOR'` \| `'ESCALATOR'` |
| `StationCode` | Retained | Used as primary anchor in Pathway join logic. |
| `LocationDescription` | Retained | Free-text segment description. Input to Pathway join (Tier 1 cascade). |
| `SymptomDescription` | Retained | `'Minor Repair'` \| `'Major Repair'` \| `'Modernization'` \| `'Inspection Repair'` \| `'Service Call'` \| `'Other'`. Source for `severity` derivation. |
| `DateOutOfService` | Retained | When the unit went out of service. Stable across polls. Part of composite identity key. |
| `DateUpdated` | Retained | WMATA's own update timestamp. Change detection signal — triggers a new snapshot node. |
| `EstimatedReturnToService` | Retained | Nullable. Source for `projected_duration_days`. |
| `StationName` | **Excluded** | Redundant — derivable by graph traversal from `:Pathway`. |
| `UnitStatus` | **Excluded** | 100% null in feed. Deprecated API field. |
| `SymptomCode` | **Excluded** | 100% null in feed. Deprecated API field. |
| `TimeOutOfService` | **Excluded** | Redundant with time portion of `DateOutOfService`. |
| `DisplayOrder` | **Excluded** | UI ordering artefact. No analytical value. |

---

## Graph Model

### Nodes

| Label | Key property | Source | Notes |
|---|---|---|---|
| `:OutageEvent` | `composite_key` | WMATA API | One node per outage snapshot. New node created when `date_updated` changes. See [Outage Lifecycle](#outage-lifecycle). |

**`:OutageEvent` properties:**

| Property | Type | Source | Notes |
|---|---|---|---|
| `composite_key` | String | Derived | `unit_name \| date_out_of_service_epoch \| date_updated_epoch`. Uniqueness anchor. |
| `unit_name` | String | API | e.g. `'A02W03'`. Encodes station + zone + sequence. |
| `unit_type` | String | API | `'ELEVATOR'` \| `'ESCALATOR'` |
| `station_code` | String | API | e.g. `'A02'`. Used by pathway joiner. |
| `location_description` | String | API | Free-text segment location. |
| `symptom_description` | String | API | Repair type. Source for `severity`. |
| `date_out_of_service` | Integer | API | Epoch milliseconds. Part of composite identity. |
| `date_updated` | Integer | API | Epoch milliseconds. Change detection signal. |
| `estimated_return` | Integer \| null | API | Epoch milliseconds. Nullable. |
| `severity` | Integer | Derived | From `symptom_description`: Minor/Other/Service Call/Inspection → `2`, Major Repair → `3`, Modernization → `4`. |
| `projected_duration_days` | Integer \| null | Derived | `estimated_return − date_out_of_service` in days. Null when `estimated_return` absent. |
| `status` | String | Ingestion | `'active'` \| `'resolved'`. Transitions to `'resolved'` when unit leaves API response. |
| `first_seen_poll` | String | Ingestion | ISO-8601 timestamp of poll that created this snapshot. |
| `last_seen_poll` | String | Ingestion | Updated in-place on every poll where data is unchanged. The only mutable property on a node. |
| `resolved_at` | String \| null | Ingestion | Null until resolution. Set to poll timestamp when unit first absent from response. |
| `actual_duration_days` | Integer \| null | Derived | Null until resolution. `resolved_at − date_out_of_service`. Primary metric for infrastructure correlation. |

### Relationships

| Pattern | Notes |
|---|---|
| `(:OutageEvent)-[:AFFECTS]->(:Pathway)` | Each snapshot links to the specific `:Pathway` node for the failed traversal segment. Multiple snapshots of the same outage all point to the same `:Pathway`. Resolved by `pathway_joiner.py`. |

Station-level context is derived by graph traversal — no direct `:Station` relationship:

```
(:OutageEvent)-[:AFFECTS]->(:Pathway)-[:LINKS]->(StationEntrance|Platform|Station)
                                                     ↑
                                     (via CONTAINS from Station)
```

---

## Outage Lifecycle

Full snapshot history is maintained. Each meaningful change creates a new `:OutageEvent` node.

| Poll event | Condition | Action |
|---|---|---|
| Unit appears — no existing node | New outage | `CREATE :OutageEvent` with `status='active'`, `first_seen_poll=now`, `last_seen_poll=now`. `CREATE [:AFFECTS]→:Pathway`. |
| Unit appears — `date_updated` unchanged | No change | `SET last_seen_poll = now`. No new node. |
| Unit appears — `date_updated` changed | Outage updated | `CREATE` new `:OutageEvent` snapshot with updated properties. Previous snapshot preserved with its `last_seen_poll`. `CREATE [:AFFECTS]→same :Pathway`. |
| Unit absent from response | Outage resolved | Find most recent active node for this `unit_name + date_out_of_service`. `SET status='resolved'`, `resolved_at=now`, `actual_duration_days = floor((resolved_at − date_out_of_service) / ms_per_day)`. |

**Composite identity key:** `unit_name + date_out_of_service` logically identifies one physical outage instance across all its snapshots. `date_updated` is included in `composite_key` to allow the `MERGE` to create a new snapshot node when it changes.

---

## Pathway Join Logic

The `[:AFFECTS]` relationship requires resolving an API outage record to a specific `:Pathway` node. No shared key exists between the WMATA REST API (`UnitName`) and GTFS `pathway_id`. Resolution uses a two-tier approach in `pathway_joiner.py`.

### Tier 1 — Cascaded programmatic join (~92% match rate)

One Neo4j round-trip per poll fetches all mode-4/5 Pathway candidates; all filtering runs in Python against a GTFS-enriched DataFrame. Seven sub-strategies are tried in order, returning on the first unambiguous match:

| Step | Strategy | What it resolves |
|---|---|---|
| F1 | Description segment key (GTFS-synonym-expanded) | Units with unique segment descriptions |
| F2 | Seq number alone (zone-conditional) | Units whose WMATA sequence maps cleanly to GTFS ESC/ELE number |
| F3 | Description narrows pool → seq or BT-endpoint tiebreak | Two pathways with same description; position or sequence disambiguates |
| F4 | Seq narrows pool → description or BT tiebreak | Multiple units share a sequence; description narrows to one |
| F5 | Synonym-expanded description → seq or BT tiebreak | WMATA "middle landing" / "platform level" / line-qualified names |
| F6 | Singleton fallback | Only one unit of this type at the station |
| F7 | Final tiebreaker: description-filtered pool, lowest-seq BT endpoint | Remaining ambiguous cases |

**Description segment key:** extracts the `between X and Y` phrase from `LocationDescription`, strips destination suffixes (`to Vienna/...`), and sorts the two nouns alphabetically so `"street and mezzanine" == "mezzanine and street"`.

**Synonym table:** WMATA informal terms mapped to GTFS equivalents — `"middle landing"→"intermediate passage"`, `"silver line platform"→"platform"`, `"platform level"→"platform"`, etc. Applied to GTFS descriptions when filtering (not to the WMATA side) so the comparison is always in GTFS vocabulary.

**GTFS enrichment:** `stops.txt` stop descriptions are loaded once per poll and joined to the candidate DataFrame (`from_desc`, `to_desc` columns). Escalator/elevator sequence numbers are extracted from stop IDs (e.g. `ESC3` → `3`).

### Tier 2 — Static lookup (complex stations + spatial overrides)

Checked first for every outage (no station filtering). Covers:

| Case | Codes | Reason |
|---|---|---|
| Metro Center | A01 / C01 | Multi-code stop IDs; many units per zone |
| Gallery Place | B01 / F01 | Secondary code not a `NODE_` prefix |
| L'Enfant Plaza | D03 / F03 | Secondary code mismatch |
| Fort Totten | B06 / E06 | Zone letter `X` not in any `NODE_` stop_id |
| Spatial overrides | e.g. A07X04 | Direction cues ("west side of Wisconsin Avenue") not resolvable programmatically |

Static entries are keyed by `(unit_name, unit_type)` to handle stations where the same zone+seq exists for both unit types.

---

## ELT Stages

| Stage | File | Input | Output |
|---|---|---|---|
| **Extract** | `extract.py` | `WMATAClient` | `{"outages": DataFrame}` — raw CamelCase fields |
| **Transform** | `transform.py` | Raw DataFrame | Normalised DataFrame: snake_case fields, epoch ms dates, `composite_key`, `severity`, `projected_duration_days`, `status`, poll timestamps |
| **Load** | `load.py` | Transformed result + `Neo4jManager` | `:OutageEvent` nodes, `[:AFFECTS]→:Pathway` relationships, stale resolution |

---

## File Structure

| File | Purpose |
|---|---|
| `__init__.py` | Orchestrator — runs extract → transform → load. Entry point called by `pipeline.py`. |
| `extract.py` | Calls `api_client.get_elevator_outages()`, wraps `list[dict]` into `{"outages": DataFrame}`. |
| `transform.py` | Renames fields, parses dates to epoch ms, computes `composite_key`, derives `severity` and `projected_duration_days`, sets `status = 'active'`, stamps poll timestamps. |
| `load.py` | Phase 1: constraints. Phase 2: `OutageEvent` nodes. Phase 3: `AFFECTS→Pathway` via `pathway_joiner`. Phase 4: stale resolution (`resolved_at`, `actual_duration_days`). |
| `pathway_joiner.py` | Two-tier Pathway resolution. Tier 1: 7-step cascade join against GTFS-enriched candidates. Tier 2: static lookup for complex interchange stations and spatial overrides. |
| `join_strategy_test.py` | Strategy comparison harness — runs strategies A–F against live WMATA data to measure match rates. Not a pytest file; run with `uv run python -m src.layers.accessibility.join_strategy_test`. |

Cypher files live in `queries/accessibility/`:

| File | Purpose |
|---|---|
| `constraints.cypher` | Uniqueness constraint on `composite_key`; indexes on `status`, `unit_name`, `severity`, `last_seen_poll`. Run once on DB initialisation. |
| `nodes.cypher` | `MERGE` on `composite_key` with `ON CREATE / ON MATCH` split — `ON MATCH` updates only `last_seen_poll`. |
| `relationships.cypher` | `MERGE` for `[:AFFECTS]→:Pathway`. |

---

## Prerequisites & Execution

The physical layer must be loaded before the accessibility layer (`:Pathway` nodes required for `[:AFFECTS]` links):

```bash
uv run python -m src.pipeline --layers physical accessibility
```

Run accessibility layer alone (physical already loaded):

```bash
uv run python -m src.pipeline --layers accessibility
```

Diagnose join match rate against live WMATA outages:

```bash
uv run python -m src.layers.accessibility.join_strategy_test
```
