# src/common/validators/fare_zones.py
"""
Fare zone integrity checks, run in two phases:

  validate_pre_load  — checks raw GTFS DataFrames before any Neo4j writes
  validate_post_load — checks the graph after all fare nodes and relationships
                       have been committed

Pre-load checks:
  1. All area_ids in fare_leg_rules resolve to a known zone_id in stops
  2. Each area_id maps to exactly ONE zone_id (zone is sufficient anchor)
  3. Every faregate's zone_id matches its parent station's zone_id

Post-load checks:
  4. Every FareGate has a BELONGS_TO relationship to a Station
  5. Every rail FareLegRule has FROM_AREA → FareZone
  6. No FareGate is missing zone_id property
  7. FareZone node count matches expected unique zone count

Known data characteristic (non-blocking warning):
  - Zone 53 has stations but no faregate nodes (confirmed in GTFS analysis)
"""

from collections import defaultdict

import pandas as pd

from src.common.validators.base import ValidationResult

# Confirmed by running validate_fare_zones.py against the live GTFS feed.
# Update if feed version changes.
EXPECTED_FARE_ZONE_COUNT = 42


def validate_pre_load(
    stops: pd.DataFrame,
    fare_leg_rules: pd.DataFrame,
) -> ValidationResult:
    """
    Validates fare zone consistency against raw GTFS DataFrames.
    Should be called at the end of fare/transform.py before returning results.
    """
    result = ValidationResult()

    # ── Build zone maps ───────────────────────────────────────────────────────

    zoned = stops[stops["zone_id"].notna() & (stops["zone_id"] != "")].copy()
    zoned["zone_id"] = zoned["zone_id"].astype(str).str.strip()

    # Build stop → set[zone_ids] to capture duplicate stop_id rows
    # with conflicting zones. set_index().to_dict() would silently keep
    # only the last value, masking multi-zone conflicts (Check 2).
    stop_zone_sets: dict[str, set[str]] = defaultdict(set)
    for _, row in zoned.iterrows():
        stop_zone_sets[row["stop_id"]].add(row["zone_id"])

    # Flat lookup for Check 1 / Check 3 (first zone wins — conflict caught in Check 2)
    stop_zones: dict[str, str] = {
        sid: next(iter(zones)) for sid, zones in stop_zone_sets.items()
    }

    station_zones = {sid: z for sid, z in stop_zones.items() if sid.startswith("STN_")}

    gate_df = stops[
        stops["stop_id"].str.contains("_FG_", na=False)
        & stops["zone_id"].notna()
        & (stops["zone_id"] != "")
    ][["stop_id", "zone_id", "parent_station"]].copy()
    # Normalise to string so comparisons are type-safe regardless of
    # whether zone_id was read as float64 (e.g. 10.0) or string ("10")
    gate_df["zone_id"] = gate_df["zone_id"].astype(str).str.strip()

    # ── Check 1: all area_ids resolve ────────────────────────────────────────

    area_ids = pd.concat(
        [
            fare_leg_rules["from_area_id"].dropna(),
            fare_leg_rules["to_area_id"].dropna(),
        ]
    ).unique()
    area_ids = [a for a in area_ids if a != ""]

    unresolved = [a for a in area_ids if a not in stop_zones]
    if unresolved:
        result.fail(
            f"{len(unresolved)} area_id(s) in fare_leg_rules have no matching "
            f"zone_id in stops: {unresolved[:5]}"
        )
    else:
        result.note(f"All {len(area_ids)} area_ids resolve to a zone_id")

    # ── Check 2: each area maps to exactly one zone ───────────────────────────

    # Use stop_zone_sets (not stop_zones) so multi-zone stops are detected
    area_zones: dict[str, set[str]] = defaultdict(set)
    for area in area_ids:
        if area in stop_zone_sets:
            area_zones[area].update(stop_zone_sets[area])

    multi_zone = {a: z for a, z in area_zones.items() if len(z) > 1}
    if multi_zone:
        result.fail(
            f"{len(multi_zone)} area_id(s) map to multiple zone_ids — "
            f"zone anchoring is insufficient: {list(multi_zone.keys())[:5]}"
        )
    else:
        result.note("All area_ids map to exactly one zone_id — FareZone anchoring is valid")

    # ── Check 3: faregate zones match parent station zones ───────────────────

    mismatches: list[str] = []
    no_parent: list[str] = []

    for _, row in gate_df.iterrows():
        parent = row["parent_station"]
        if not parent or pd.isna(parent):
            no_parent.append(row["stop_id"])
            continue
        station_zone = station_zones.get(parent)
        if station_zone and station_zone != row["zone_id"]:
            mismatches.append(
                f"{row['stop_id']} (gate_zone={row['zone_id']}, "
                f"station_zone={station_zone})"
            )

    if mismatches:
        result.fail(
            f"{len(mismatches)} faregate(s) have zone_id mismatching parent station: "
            f"{mismatches[:3]}"
        )
    else:
        result.note(f"All {len(gate_df)} faregate zone_ids consistent with parent station")

    if no_parent:
        result.warn(f"{len(no_parent)} faregate(s) have no parent_station: {no_parent[:3]}")

    # ── Known characteristic: Zone 53 has no faregates ───────────────────────

    zone_53_gates = gate_df[gate_df["zone_id"] == "53"]
    if zone_53_gates.empty:
        result.warn(
            "Zone 53 has no faregate nodes — confirmed data characteristic "
            "(bus-only or entrance-only station)"
        )

    return result


def validate_post_load(neo4j_manager) -> ValidationResult:  # type: ignore[no-untyped-def]
    """
    Validates fare zone integrity by querying Neo4j after loading.
    Should be called at the end of fare/load.py after all writes complete.

    neo4j_manager: instance of src.common.neo4j_tools.Neo4jManager
    """
    result = ValidationResult()

    checks = [
        (
            # Check 4: all FareGates have BELONGS_TO → Station
            """
            MATCH (fg:FareGate)
            WHERE NOT (fg)-[:BELONGS_TO]->(:Station)
            RETURN count(fg) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} FareGate(s) missing BELONGS_TO → Station relationship",
            "All FareGates have BELONGS_TO → Station",
        ),
        (
            # Check 5: all rail FareLegRules anchor to FareZone
            """
            MATCH (flr:FareLegRule)
            WHERE flr.network_id IN ['metrorail', 'metrorail_shuttle']
            AND NOT (flr)-[:FROM_AREA]->(:FareZone)
            RETURN count(flr) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} rail FareLegRule(s) missing FROM_AREA → FareZone",
            "All rail FareLegRules anchor to FareZone via FROM_AREA",
        ),
        (
            # Check 6: no FareGate missing zone_id
            """
            MATCH (fg:FareGate)
            WHERE fg.zone_id IS NULL
            RETURN count(fg) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} FareGate(s) missing zone_id property",
            "All FareGates have zone_id property",
        ),
        (
            # Check 7: FareZone count matches expected
            """
            MATCH (fz:FareZone)
            RETURN count(fz) AS n
            """,
            lambda n: n == EXPECTED_FARE_ZONE_COUNT,
            lambda n: (
                f"Expected {EXPECTED_FARE_ZONE_COUNT} FareZone nodes, found {n}. "
                "If feed was updated, revise EXPECTED_FARE_ZONE_COUNT in fare_zones.py"
            ),
            f"FareZone count matches expected ({EXPECTED_FARE_ZONE_COUNT})",
        ),
    ]

    for cypher, ok_fn, err_fn, ok_msg in checks:
        with neo4j_manager as session:
            record = session.run(cypher).single()
            n = record["n"] if record else 0
        if ok_fn(n):
            result.note(ok_msg)
        else:
            result.fail(err_fn(n))

    return result
