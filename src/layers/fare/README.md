# Fare Layer

Loads WMATA's fare structure into Neo4j — fare zones, fare products, fare media (payment methods), and the rules governing which product applies for a given trip segment or transfer.

This layer is the reference implementation for the ELT pattern used across all layers. See `__init__.py` for the canonical orchestration example.

---

## Overview

**Must run after:** physical layer (`:Station` and `:FareGate` nodes required for zone anchoring).

**Data source:** GTFS static files — `fare_media.txt`, `fare_products.txt`, `fare_leg_rules.txt`, `fare_transfer_rules.txt`, `stops.txt` (for zone assignment).

---

## ELT Stages

| Stage | File | Input | Output |
|---|---|---|---|
| **Extract** | `extract.py` | `gtfs_data` dict | `fare_media`, `fare_products`, `fare_leg_rules`, `fare_transfer_rules`, `stops` DataFrames |
| **Pre-transform validation** | `validators/fare_zones.py` | Raw DataFrames | Pass/fail |
| **Transform** | `transform.py` | Raw DataFrames | Cleaned DataFrames + derived columns (`rule_id`, zone assignments) |
| **Load** | `load.py` + `queries/fare/` | Transformed DataFrames | Neo4j nodes, relationships, constraints |
| **Post-load validation** | `validators/fare_zones.py` | Neo4j (live query) | Pass/fail |

---

## Graph Model

### Nodes

| Label | Key property | GTFS source |
|---|---|---|
| `FareZone` | `zone_id` | Derived from `fare_leg_rules.txt` `from_area_id` / `to_area_id` |
| `FareMedia` | `fare_media_id` | `fare_media.txt` |
| `FareProduct` | `fare_product_id` | `fare_products.txt` |
| `FareLegRule` | `leg_group_id` | `fare_leg_rules.txt` |
| `FareTransferRule` | `rule_id` | `fare_transfer_rules.txt` (synthetic key — see below) |

### Relationships

| Pattern | Source |
|---|---|
| `(Station)-[:IN_ZONE]->(FareZone)` | `stops.txt` `zone_id` field |
| `(FareGate)-[:BELONGS_TO]->(Station)` | `stops.txt` `parent_station` |
| `(FareMedia)-[:ACCEPTS]->(FareProduct)` | `fare_media.txt` / `fare_products.txt` join |
| `(FareLegRule)-[:APPLIES_PRODUCT]->(FareProduct)` | `fare_leg_rules.txt` |
| `(FareLegRule)-[:FROM_AREA]->(FareZone)` | Rail rules only — origin zone |
| `(FareLegRule)-[:TO_AREA]->(FareZone)` | Rail rules only — destination zone |
| `(FareTransferRule)-[:FROM_LEG]->(FareLegRule)` | `fare_transfer_rules.txt` |
| `(FareTransferRule)-[:TO_LEG]->(FareLegRule)` | `fare_transfer_rules.txt` |

**Rail vs. bus zone anchoring:** Only rules for network IDs `metrorail` and `metrorail_shuttle` get `FROM_AREA` / `TO_AREA` zone links — bus fares are flat-rate with no origin/destination specificity.

**Synthetic `rule_id`:** GTFS `fare_transfer_rules.txt` has no primary key. The transform synthesises one as `from_leg_group_id + "__" + to_leg_group_id`. This holds as long as each (from, to) leg pair appears at most once in the feed.

---

## File Structure

| File | Purpose |
|---|---|
| `__init__.py` | Orchestrator — runs extract → validate → transform → load |
| `extract.py` | Pulls fare files from the shared `gtfs_data` dict |
| `transform.py` | Normalises fields, synthesises `rule_id`, derives zone assignments |
| `load.py` | Writes all nodes and relationships using parameterised Cypher from `queries/fare/` |

Cypher files live in `queries/fare/`:

| File | Purpose |
|---|---|
| `constraints.cypher` | Uniqueness constraints on all fare node types |
| `nodes.cypher` | `MERGE` for FareZone, FareMedia, FareProduct, FareLegRule, FareTransferRule |
| `relationships.cypher` | `MERGE` for all fare relationships |
| `analytical.cypher` | Read-only fare analysis queries (Q1–Q6 fare-only; Q7–Q10 fare+physical) |
