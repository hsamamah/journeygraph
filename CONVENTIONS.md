# CONVENTIONS.md — WMATA-Specific Conventions & Assumptions

This document records every convention in the codebase that is derived
from WMATA's GTFS feed structure rather than the GTFS specification
itself. These conventions are correct for the current feed (version
S1000246, 2025-12-14 to 2026-06-13) but may need revisiting if WMATA
changes their feed conventions or if this codebase is adapted for
another transit agency.

---

## Stop ID Prefix Conventions

**Location:** Used across all layers (fare, service, interruption transform and load).

WMATA assigns stop_id prefixes by location_type. The codebase uses
these prefixes for node type identification and cross-layer routing:

| Prefix   | Node Label       | GTFS location_type | Example            |
|----------|------------------|--------------------|--------------------|
| `STN_`   | `:Station`       | 1 (station)        | `STN_A01_C01`      |
| `ENT_`   | `:StationEntrance`| 2 (entrance)      | `ENT_A01_N`        |
| `PF_`    | `:Platform`      | 0 (stop/platform)  | `PF_A01_1`         |
| `PLF_`   | `:BoardingArea`  | 4 (boarding area)  | `PLF_A01_RED_GL`   |
| `NODE_`  | `:Pathway`       | 3 (generic node)   | `NODE_A01_ELE_01`  |

**Fare gate detection:** Stop IDs containing `_FG_` are classified as
fare gates (e.g. `NODE_A01_FG_PAID`). This is a substring match, not a
prefix match, because fare gate IDs follow the `NODE_` prefix pattern.

**Code locations:**
- `src/layers/fare/transform.py` — `_FG_` pattern for gate_zones
- `src/layers/service_schedule/transform.py` — `PF_` for rail platform detection
- `src/layers/service_schedule/load.py` — `PF_` for STOPS_AT/SCHEDULED_AT splitting

**Risk if convention changes:** Node type misclassification. Route SERVES
would point to wrong node types, SCHEDULED_AT would target non-existent
Platform nodes.

---

## Pathway Node Type Derivation

**Location:** `src/layers/service_schedule/transform.py` (node_type on Pathway)

`:Pathway` node types are derived from substrings in the `NODE_` stop_id.
The `:PathwayNode` label was renamed to `:Pathway` in schema v3 — do not
use `:PathwayNode` in Cypher queries or code, it no longer exists.

| Substring      | node_type    | Neo4j Label  |
|----------------|-------------|--------------|
| `ELE` or `ELV` | elevator    | `:Elevator`  |
| `ESC`          | escalator   | `:Escalator` |
| `FG`           | faregate    | `:FareGate`  |
| `MZ`           | mezzanine   | `:Mezzanine` |
| `STR`          | stairs      | `:Stairs`    |

---

## Rail Network IDs

**Location:** `src/layers/fare/transform.py`, line defining `RAIL_NETWORKS`

```python
RAIL_NETWORKS = {"metrorail", "metrorail_shuttle"}
```

Only these network_id values in `fare_leg_rules.txt` trigger FROM_AREA /
TO_AREA relationship creation (station-to-station anchoring). Bus
network IDs (`metrobus_regular`, `metrobus_express`) have flat fares
with no origin/destination specificity.

**Risk if convention changes:** A new rail network ID (e.g. for a future
streetcar) would not get zone-anchored fare rules.

---

## Maintenance Service Detection (`_R` Suffix)

**Location:** `src/layers/service_schedule/transform.py`, `_classify_service()`

```python
if sid.endswith("_R"):
    return "Maintenance"
```

WMATA uses the `_R` suffix on service_id values to indicate maintenance
windows (e.g. `WK_R`, `SAT_R`). This is a WMATA naming convention, not
a GTFS standard. The GTFS spec has no concept of "maintenance" service —
it's just another calendar entry.

**Risk if convention changes:** Maintenance services would be classified
by their day flags instead (likely "Holiday" if all-zero, or "Weekday"
if they run Mon-Fri).

---

## US Federal Holiday Detection

**Location:** `src/layers/service_schedule/transform.py`, `_compute_us_holidays()`

The ETL computes 11 US federal holidays with observed-date rules
(Saturday → Friday, Sunday → Monday) and attaches `holiday_name` to the
ACTIVE_ON relationship when a ServicePattern date falls on a holiday.

**Holidays included:**
New Year's Day, MLK Day, Presidents' Day, Memorial Day, Juneteenth,
Independence Day, Labor Day, Columbus Day, Veterans Day, Thanksgiving,
Christmas Day.

**Not included:**
- DC-specific holidays (Emancipation Day — April 16)
- Inauguration Day (every 4 years, Jan 20)
- WMATA-specific service holidays (if any differ from federal)

**Risk if convention changes:** holiday_name would be null for
unrecognized holidays. The ServicePattern label is still correctly
classified from calendar.txt day flags — only the name annotation is
affected.

---

## FareTransferRule Synthetic Key

**Location:** `src/layers/fare/load.py`

```python
df["rule_id"] = df["from_leg_group_id"] + "__" + df["to_leg_group_id"]
```

GTFS `fare_transfer_rules.txt` has no primary key column. The codebase
synthesizes `rule_id` by concatenating `from_leg_group_id` and
`to_leg_group_id` with `__` as separator.

**Assumption:** Each (from_leg, to_leg) pair appears at most once.
In the current feed this holds (15 unique rules, 15 unique pairs).
If WMATA adds multiple transfer rules for the same leg pair (e.g.
different duration limits or transfer counts), they would collide on
this synthetic key.

---

## Route Type Mapping

**Location:** `src/layers/service_schedule/transform.py`

```python
ROUTE_TYPE_MODE = {1: "rail", 3: "bus"}
```

WMATA uses only route_type 1 (Metro/subway) and 3 (bus). The GTFS
spec defines additional types (0=tram, 2=rail, 4=ferry, 5=cable tram,
6=aerial lift, 7=funicular, 11=trolleybus, 12=monorail) which are not
mapped. Any unknown type logs a warning and defaults to "bus".

---

## Single Agency

WMATA publishes a single-agency GTFS feed (agency_id = "1"). The
codebase handles multi-agency feeds but warns when `agency_id` is
missing from routes and falls back to the first agency in agency.txt.

---

## Cypher File Comment Convention

**Location:** All `queries/**/*.cypher` files, parsed by `_extract_statement()`.

Each Cypher statement block must start with a `// ── ` (note the
decorative dash characters) comment line containing a hint string that
load.py uses to extract individual statements. The regex is:

```python
re.split(r"\n(?=// ── )", cypher)
```

Adding a block with a different comment style (e.g. plain `//` or
`/* */`) will make it invisible to the loader.

---

## Rail Network IDs — Zone Anchoring

`metrorail` and `metrorail_shuttle` are treated as rail networks and require
FROM_AREA → FareZone anchoring for zone-priced rules. Exception: rules that
apply `metrorail_free_fare` product have null from/to area IDs in GTFS —
this is a known WMATA feed characteristic, not a data quality issue. The
shuttle free-fare rule (leg_metrorail_shuttle) is legitimately flat-rate.


## ServiceAlert Effect Mapping

WMATA uses OTHER_EFFECT for miscellaneous service modifications that don't
fit named GTFS-RT effect categories. Mapped to service_change in
EFFECT_TYPE_MAP. This is the most common alert effect in the bus feed.

Bus TripUpdates frequently omit start_date — ON_DATE relationships will
be missing for these. Expected; not a data quality issue.
