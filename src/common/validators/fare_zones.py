# src/common/validators/fare_zones.py
"""
Fare zone integrity checks, run in two phases:

  validate_pre_transform — checks raw GTFS DataFrames before any transformation
                           runs. Called in fare/__init__.py after extract,
                           before transform.
  validate_post_load     — checks the graph after all fare nodes and
                           relationships have been committed.

Pre-transform checks:
  1.  All area_ids in fare_leg_rules resolve to a known zone_id in stops
  1b. All zone-priced rail FareLegRule rows have a non-null from_area_id
      (free-fare rules are exempt — leg_metrorail_shuttle has null areas by design)
  2.  Each area_id maps to exactly ONE zone_id (zone is sufficient anchor)
  3.  Every faregate's zone_id matches its parent station's zone_id

Post-load checks:
  4.  Every FareGate has a BELONGS_TO relationship to a Station
  5.  Every zone-priced rail FareLegRule has FROM_AREA → FareZone
      (free-fare rules exempt — they carry no zone anchoring in GTFS)
  6.  No FareGate is missing zone_id property
  7.  FareZone node count matches expected unique zone count (soft — warning only)
  8.  FareLegRule node count matches expected OD pair count (soft — warning only)

Known data characteristics (non-blocking warnings):
  - Zone 53 has stations but no faregate nodes (confirmed in GTFS analysis)
  - leg_metrorail_shuttle applies metrorail_free_fare with null from/to area IDs —
    flat-rate shuttle fare, not zone-priced. See CONVENTIONS.md →
    "Rail Network IDs — Zone Anchoring"
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

import pandas as pd

from src.common.validators.base import ValidationResult, run_count_check

if TYPE_CHECKING:
    from src.common.neo4j_tools import Neo4jManager

# ── Soft-check thresholds ─────────────────────────────────────────────────────
# Update after a feed version change that genuinely alters these counts.
# Set to None to disable a check entirely.

# 42 unique zone_ids found in current feed (S1000246, 2025-12-14 to 2026-06-13)
EXPECTED_FARE_ZONE_COUNT: int | None = 42

# 9,509 unique OD pairs: 9,506 metrorail + 2 metrobus flat-rate + 1 shuttle
# (leg_group_id × from_area_id × to_area_id, NULLs included in key)
EXPECTED_FARE_LEG_RULE_COUNT: int | None = 9509

# Product id prefix that marks a free/flat-rate rule with no zone anchoring.
# All rules whose fare_product_id starts with this prefix are exempt from
# the FROM_AREA requirement. Currently covers leg_metrorail_shuttle only.
_FREE_FARE_PREFIX = "metrorail_free_fare"

# WMATA rail network_ids that require zone-anchored FROM_AREA relationships
# (unless the rule is free-fare). Must stay in sync with transform.py RAIL_NETWORKS.
_RAIL_NETWORKS = {"metrorail", "metrorail_shuttle"}


# ── Pre-transform validator ───────────────────────────────────────────────────


def validate_pre_transform(
    stops: pd.DataFrame,
    fare_leg_rules: pd.DataFrame,
) -> ValidationResult:
    """
    Validates fare zone consistency against raw GTFS DataFrames.
    Called in fare/__init__.py after extract, before transform runs.
    """
    result = ValidationResult()

    # ── Build zone maps ───────────────────────────────────────────────────────

    zoned = stops[stops["zone_id"].notna() & (stops["zone_id"] != "")].copy()
    zoned["zone_id"] = zoned["zone_id"].astype(str).str.strip()

    # Build stop → set[zone_ids] to capture duplicate stop_id rows with
    # conflicting zones. set_index().to_dict() would silently keep only the
    # last value, masking multi-zone conflicts (Check 2).
    stop_zone_sets: dict[str, set[str]] = (
        zoned.groupby("stop_id")["zone_id"].agg(set).to_dict()
    )

    # Flat lookup for Check 1 / Check 3 (first zone wins; conflicts caught in Check 2)
    stop_zones: dict[str, str] = {
        sid: next(iter(zones)) for sid, zones in stop_zone_sets.items()
    }

    station_zones = {sid: z for sid, z in stop_zones.items() if sid.startswith("STN_")}

    gate_df = stops[
        stops["stop_id"].str.contains("_FG_", na=False)
        & stops["zone_id"].notna()
        & (stops["zone_id"] != "")
    ][["stop_id", "zone_id", "parent_station"]].copy()
    # Normalise to string so comparisons are type-safe regardless of whether
    # zone_id was read as float64 (e.g. 10.0) or string ("10")
    gate_df["zone_id"] = gate_df["zone_id"].astype(str).str.strip()

    # ── Check 1: all area_ids resolve to a known zone_id ─────────────────────

    area_ids = pd.concat(
        [
            fare_leg_rules["from_area_id"].dropna(),
            fare_leg_rules["to_area_id"].dropna(),
        ]
    ).unique()
    area_ids = [a for a in area_ids if str(a).strip() not in ("", "nan")]

    unresolved = [a for a in area_ids if a not in stop_zones]
    if unresolved:
        result.fail(
            f"{len(unresolved)} area_id(s) in fare_leg_rules have no matching "
            f"zone_id in stops: {unresolved[:5]}"
        )
    else:
        result.note(f"All {len(area_ids)} area_ids resolve to a zone_id")

    # ── Check 1b: all zone-priced rail rules have a non-null from_area_id ────
    #
    # Free-fare rules (metrorail_free_fare prefix) are exempt — they carry no
    # zone anchoring in GTFS by design. leg_metrorail_shuttle is the current
    # example: flat-rate shuttle, from_area_id and to_area_id are both null.
    # See CONVENTIONS.md → "Rail Network IDs — Zone Anchoring"

    if "network_id" in fare_leg_rules.columns:
        rail_mask = fare_leg_rules["network_id"].isin(_RAIL_NETWORKS)
        rail_rules = fare_leg_rules[rail_mask].copy()

        free_fare_mask = (
            rail_rules["fare_product_id"]
            .astype(str)
            .str.startswith(_FREE_FARE_PREFIX, na=False)
        )
        zone_priced = rail_rules[~free_fare_mask]

        # Treat pandas NaN, empty string, and the str(NaN)="nan" coercion as null
        null_from = zone_priced["from_area_id"].isna() | (
            zone_priced["from_area_id"].astype(str).str.strip().isin(["", "nan"])
        )
        n_exempt = free_fare_mask.sum()

        if null_from.any():
            bad_ids = zone_priced.loc[null_from, "leg_group_id"].tolist()
            result.fail(
                f"{null_from.sum()} zone-priced rail FareLegRule row(s) have null "
                f"from_area_id: {bad_ids[:5]}"
            )
        else:
            result.note(
                f"All zone-priced rail FareLegRule rows have from_area_id populated "
                f"({n_exempt} free-fare row(s) correctly exempt)"
            )

    # ── Check 2: each area_id maps to exactly one zone_id ────────────────────

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
        result.note(
            "All area_ids map to exactly one zone_id — FareZone anchoring is valid"
        )

    # ── Check 3: faregate zones match parent station zones ───────────────────

    no_parent_mask = gate_df["parent_station"].isna() | (gate_df["parent_station"] == "")
    no_parent_gates = gate_df[no_parent_mask]
    has_parent = gate_df[~no_parent_mask].copy()

    if not has_parent.empty:
        station_zone_series = pd.Series(station_zones, name="station_zone")
        has_parent = has_parent.join(station_zone_series, on="parent_station")
        known_station = has_parent["station_zone"].notna()
        mismatch_rows = has_parent[known_station & (has_parent["zone_id"] != has_parent["station_zone"])]
        mismatch_labels = (
            mismatch_rows["stop_id"].astype(str)
            + " (gate_zone=" + mismatch_rows["zone_id"].astype(str)
            + ", station_zone=" + mismatch_rows["station_zone"].astype(str) + ")"
        ).tolist()
    else:
        mismatch_labels = []

    if mismatch_labels:
        result.fail(
            f"{len(mismatch_labels)} faregate(s) have zone_id mismatching parent station: "
            f"{mismatch_labels[:3]}"
        )
    else:
        result.note(
            f"All {len(gate_df)} faregate zone_ids consistent with parent station"
        )

    if not no_parent_gates.empty:
        result.warn(
            f"{len(no_parent_gates)} faregate(s) have no parent_station: "
            f"{no_parent_gates['stop_id'].tolist()[:3]}"
        )

    # ── Known characteristic: Zone 53 has no faregates ───────────────────────

    if gate_df[gate_df["zone_id"] == "53"].empty:
        result.warn(
            "Zone 53 has no faregate nodes — confirmed data characteristic "
            "(bus-only or entrance-only station)"
        )

    return result


# ── Post-load validator ───────────────────────────────────────────────────────


def validate_post_load(neo4j_manager: "Neo4jManager") -> ValidationResult:
    """
    Validates fare zone integrity by querying Neo4j after loading.
    Called at the end of fare/load.py after all writes complete.
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
            # Check 5: all zone-priced rail FareLegRules have FROM_AREA → FareZone
            #
            # Free-fare rules are exempt: they apply the 'rail_free' FareProduct
            # and carry no zone anchoring in GTFS. Currently this covers only
            # leg_metrorail_shuttle. See CONVENTIONS.md →
            # "Rail Network IDs — Zone Anchoring"
            """
            MATCH (flr:FareLegRule)
            WHERE flr.network_id IN ['metrorail', 'metrorail_shuttle']
              AND NOT (flr)-[:FROM_AREA]->(:FareZone)
              AND NOT (flr)-[:APPLIES_PRODUCT]->(:FareProduct {fare_product_id: 'rail_free'})
            RETURN count(flr) AS n
            """,
            lambda n: n == 0,
            lambda n: (
                f"{n} zone-priced rail FareLegRule(s) missing FROM_AREA → FareZone"
            ),
            "All zone-priced rail FareLegRules have FROM_AREA → FareZone",
        ),
        (
            # Check 6: no FareGate missing zone_id property
            """
            MATCH (fg:FareGate)
            WHERE fg.zone_id IS NULL
            RETURN count(fg) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} FareGate(s) missing zone_id property",
            "All FareGates have zone_id property",
        ),
    ]

    for cypher, ok_fn, err_fn, ok_msg in checks:
        n = run_count_check(neo4j_manager, cypher)
        if ok_fn(n):
            result.note(ok_msg)
        else:
            result.fail(err_fn(n))

    # ── Check 7: FareZone count — soft check (warning, not failure) ───────────
    #
    # A different count after a feed update is expected and should not block
    # the pipeline. Revise EXPECTED_FARE_ZONE_COUNT when the feed changes.

    if EXPECTED_FARE_ZONE_COUNT is not None:
        n = run_count_check(neo4j_manager, "MATCH (fz:FareZone) RETURN count(fz) AS n")
        if n == EXPECTED_FARE_ZONE_COUNT:
            result.note(f"FareZone count matches expected ({EXPECTED_FARE_ZONE_COUNT})")
        else:
            result.warn(
                f"FareZone count changed: expected {EXPECTED_FARE_ZONE_COUNT}, "
                f"found {n} — if feed was updated, revise EXPECTED_FARE_ZONE_COUNT "
                f"in fare_zones.py"
            )

    # ── Check 8: FareLegRule count — soft check (warning, not failure) ────────
    #
    # Expected: 9,506 metrorail OD pairs + 2 metrobus flat-rate + 1 shuttle = 9,509
    # Derived from (leg_group_id, from_area_id, to_area_id) composite key.
    # Revise EXPECTED_FARE_LEG_RULE_COUNT when the feed changes.

    if EXPECTED_FARE_LEG_RULE_COUNT is not None:
        n = run_count_check(neo4j_manager, "MATCH (flr:FareLegRule) RETURN count(flr) AS n")
        if n == EXPECTED_FARE_LEG_RULE_COUNT:
            result.note(
                f"FareLegRule count matches expected ({EXPECTED_FARE_LEG_RULE_COUNT})"
            )
        else:
            result.warn(
                f"FareLegRule count changed: expected {EXPECTED_FARE_LEG_RULE_COUNT}, "
                f"found {n} — if feed was updated, revise EXPECTED_FARE_LEG_RULE_COUNT "
                f"in fare_zones.py"
            )

    return result
