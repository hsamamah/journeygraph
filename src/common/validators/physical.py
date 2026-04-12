# src/common/validators/physical.py
"""
Physical infrastructure integrity checks, run in two phases:

  validate_pre_transform — checks raw GTFS DataFrames before any transformation.
                           Called in physical/__init__.py after extract.
  validate_post_load     — checks the graph after all physical nodes and
                           relationships have been committed.

Pre-transform checks (raw GTFS stops + pathways):
  1.  No duplicate stop_id in stops.txt
  2.  All platforms, entrances, and faregates reference a parent_station
      that exists in stops.txt (orphaned children cannot get CONTAINS rels)
  3.  All pathway from_stop_id / to_stop_id values reference a known stop_id
  4.  All pathway_mode values are within the GTFS-defined set 1–7
      (unknown modes fall through to mixed_non_gate silently in transform)
  5.  At least one station, one platform, and one faregate are present
      (guards against loading an empty or wrong GTFS feed)

Post-load checks (Neo4j graph after load.run() completes):
  6.  No duplicate stop_id on Station nodes
  7.  No duplicate stop_id on Platform nodes
  8.  No duplicate stop_id on FareGate nodes
  9.  Every Pathway has at least one [:LINKS] relationship
      (a dangling Pathway means an endpoint stop_id was not loaded —
       indicates a partition gap in transform or a missing stop type)
  10. Every Station has at least one [:CONTAINS]->(:Platform)
      (warn only — a station with no platform children is invalid for
       the WMATA rail network but may occur in bus-only station data)
  11. Pathway label migration: all Pathway nodes with mode=4 carry :Escalator,
      mode=5 carry :Elevator (validates the label migration step ran correctly —
      these labels are queried by the accessibility layer)
  12. Soft node counts by label (info only)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from src.common.utils import safe_int
from src.common.validators.base import ValidationResult, run_count_check

if TYPE_CHECKING:
    from src.common.neo4j_tools import Neo4jManager

# GTFS location_type values
_LOC_STATION = 1
_LOC_ENTRANCE = 2

# GTFS pathway_mode valid range
_VALID_PATHWAY_MODES = {1, 2, 3, 4, 5, 6, 7}


# ── Pre-transform validator ───────────────────────────────────────────────────


def validate_pre_transform(
    stops: pd.DataFrame,
    pathways: pd.DataFrame,
) -> ValidationResult:
    """
    Validates raw GTFS DataFrames before any transformation runs.
    Called in physical/__init__.py after extract, before transform.
    """
    result = ValidationResult()

    # ── Check 1: no duplicate stop_id ────────────────────────────────────────
    #
    # Duplicate stop_ids cause MERGE to silently overwrite node properties,
    # producing a node with unpredictable final state.

    dups = stops[stops.duplicated(subset=["stop_id"], keep=False)]
    if not dups.empty:
        n = dups["stop_id"].nunique()
        examples = dups["stop_id"].unique()[:5].tolist()
        result.fail(
            f"{n} stop_id(s) appear more than once in stops.txt: {examples}"
        )
    else:
        result.note(f"No duplicate stop_ids ({len(stops)} stops)")

    # ── Check 2: all child stops have a parent_station that exists ────────────
    #
    # Platforms, entrances, and faregates must reference a parent_station in
    # stops.txt. Orphaned children produce disconnected nodes — CONTAINS and
    # BELONGS_TO relationships will silently fail to write for those nodes.

    known_stop_ids = set(stops["stop_id"].astype(str).str.strip())

    child_mask = (
        (stops["location_type"] == _LOC_ENTRANCE)
        | (stops["stop_id"].str.contains("_FG_", na=False))
        | (
            (stops["location_type"].isin([0, 4]))
            & (stops["stop_id"].str.upper().str.startswith("PF_", na=False))
        )
    )
    children = stops[child_mask & stops["parent_station"].notna()].copy()
    children["parent_station"] = children["parent_station"].astype(str).str.strip()

    orphaned = children[~children["parent_station"].isin(known_stop_ids)]
    if not orphaned.empty:
        examples = orphaned["stop_id"].tolist()[:5]
        result.fail(
            f"{len(orphaned)} platform/entrance/faregate stop(s) reference a "
            f"parent_station not found in stops.txt — CONTAINS rels will be "
            f"missing for these nodes: {examples}"
        )
    else:
        result.note(
            f"All {len(children)} platform/entrance/faregate stops have a "
            f"valid parent_station"
        )

    no_parent = stops[child_mask & stops["parent_station"].isna()]
    if not no_parent.empty:
        result.warn(
            f"{len(no_parent)} platform/entrance/faregate stop(s) have no "
            f"parent_station — CONTAINS rels cannot be created: "
            f"{no_parent['stop_id'].tolist()[:5]}"
        )

    # ── Check 3: all pathway endpoints are resolvable ─────────────────────────
    #
    # Each pathway endpoint stop_id must be one of:
    #   matched  — exists in a loaded node partition (Station, StationEntrance,
    #              Platform, FareGate, BusStop) — a LINKS rel will be created
    #   deferred — GTFS generic node (location_type=3, no _FG_) used as a
    #              pivot for pathway-to-pathway chaining — intentionally unloaded
    #
    # Block on:
    #   gap     — in stops.txt but no partition predicate matches it
    #   missing — not in stops.txt at all

    loaded_ids = (
        set(stops[stops["location_type"] == _LOC_STATION]["stop_id"].astype(str))
        | set(
            stops[
                (stops["location_type"] == _LOC_ENTRANCE)
                & stops["stop_id"].str.upper().str.startswith("ENT_", na=False)
            ]["stop_id"].astype(str)
        )
        | set(
            stops[
                # location_type=4 is a non-standard extension used by this GTFS feed;
                # treated identically to platforms (location_type=0).
                stops["location_type"].isin([0, 4])
                & stops["stop_id"].str.upper().str.startswith("PF_", na=False)
            ]["stop_id"].astype(str)
        )
        | set(stops[stops["stop_id"].str.contains("_FG_", na=False)]["stop_id"].astype(str))
        | set(stops[stops["stop_id"].apply(safe_int).notnull() & (stops["location_type"] != 3)]["stop_id"].astype(str))
    )
    deferred_ids = set(
        stops[
            (stops["location_type"] == 3)
            & ~stops["stop_id"].str.contains("_FG_", na=False)
        ]["stop_id"].astype(str)
    )

    all_endpoints = (
        pd.concat([pathways["from_stop_id"].dropna(), pathways["to_stop_id"].dropna()])
        .astype(str)
        .unique()
    )

    gap_ids = [
        s
        for s in all_endpoints
        if s not in loaded_ids and s in known_stop_ids and s not in deferred_ids
    ]
    missing_ids = [s for s in all_endpoints if s not in known_stop_ids]
    deferred_count = sum(1 for s in all_endpoints if s in deferred_ids)
    matched_count = sum(1 for s in all_endpoints if s in loaded_ids)

    if gap_ids:
        result.fail(
            f"{len(gap_ids)} pathway endpoint stop_id(s) exist in stops.txt but match "
            f"no loaded partition predicate (gap) — pipeline cannot proceed: {gap_ids[:5]}"
        )
    if missing_ids:
        result.warn(
            f"{len(missing_ids)} pathway endpoint stop_id(s) not found in stops.txt: "
            f"{missing_ids[:5]} — these pathways will have no LINKS relationship"
        )
    if not gap_ids and not missing_ids:
        result.note(
            f"All pathway endpoints resolved: {matched_count} matched, "
            f"{deferred_count} deferred (generic node pivots)"
        )

    # ── Check 4: all pathway_mode values are within GTFS range 1–7 ───────────
    #
    # Values outside 1–7 fall through to 'mixed_non_gate' in transform.
    # A new mode value in a future feed version would be silently misclassified.

    if "pathway_mode" in pathways.columns:
        modes = pathways["pathway_mode"].dropna().astype(int)
        unknown_modes = modes[~modes.isin(_VALID_PATHWAY_MODES)].unique().tolist()
        if unknown_modes:
            result.warn(
                f"pathway_mode value(s) outside GTFS range 1–7: {unknown_modes} — "
                f"these pathways will be classified as mixed_non_gate in transform"
            )
        else:
            result.note(
                f"All {len(modes)} pathway_mode values are within GTFS range 1–7"
            )

    # ── Check 5: minimum expected node counts ─────────────────────────────────
    #
    # Guards against loading an empty or wrong GTFS feed. An empty partition
    # means the downstream layers will have no physical nodes to link against.

    stations = stops[stops["location_type"] == _LOC_STATION]
    faregates = stops[stops["stop_id"].str.contains("_FG_", na=False)]
    platforms = stops[
        stops["location_type"].isin([0, 4])
        & stops["stop_id"].str.upper().str.startswith("PF_", na=False)
    ]

    for label, partition in [
        ("Station", stations),
        ("Platform", platforms),
        ("FareGate", faregates),
    ]:
        if partition.empty:
            result.fail(
                f"No {label} stops found in stops.txt — physical layer cannot "
                f"proceed with an empty {label} partition"
            )
        else:
            result.note(f"{label}: {len(partition)} stop(s) found in source")

    return result


# ── Post-load validator ────────────────────────────────────────────────────────


def validate_post_load(neo4j_manager: "Neo4jManager") -> ValidationResult:
    """
    Validates physical infrastructure integrity by querying Neo4j after loading.
    Called at the end of physical/load.py after all writes complete.
    """
    result = ValidationResult()

    # ── Blocking checks ───────────────────────────────────────────────────────

    blocking_checks = [
        (
            # Check 6: no duplicate id on Station nodes
            # Property is 'id' — transform renames stop_id → id; constraint
            # enforces uniqueness on id. See queries/physical/constraints.cypher.
            """
            MATCH (s:Station)
            WITH s.id AS sid, count(s) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} id value(s) appear on more than one Station node",
            "No duplicate id values on Station nodes",
        ),
        (
            # Check 7: no duplicate id on Platform nodes
            """
            MATCH (p:Platform)
            WITH p.id AS sid, count(p) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} id value(s) appear on more than one Platform node",
            "No duplicate id values on Platform nodes",
        ),
        (
            # Check 8: no duplicate id on FareGate nodes
            """
            MATCH (fg:FareGate)
            WITH fg.id AS sid, count(fg) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} id value(s) appear on more than one FareGate node",
            "No duplicate id values on FareGate nodes",
        ),
        (
            # Check 11a: Pathway nodes with mode=4 carry :Escalator label
            # The label migration in load.py sets this after all nodes are written.
            # A non-zero count means the migration step silently failed.
            """
            MATCH (p:Pathway)
            WHERE toInteger(p.mode) = 4 AND NOT p:Escalator
            RETURN count(p) AS n
            """,
            lambda n: n == 0,
            lambda n: (
                f"{n} Pathway node(s) with mode=4 are missing the :Escalator label — "
                f"label migration may not have run"
            ),
            "All mode=4 Pathway nodes carry :Escalator label",
        ),
        (
            # Check 11b: Pathway nodes with mode=5 carry :Elevator label
            """
            MATCH (p:Pathway)
            WHERE toInteger(p.mode) = 5 AND NOT p:Elevator
            RETURN count(p) AS n
            """,
            lambda n: n == 0,
            lambda n: (
                f"{n} Pathway node(s) with mode=5 are missing the :Elevator label — "
                f"label migration may not have run"
            ),
            "All mode=5 Pathway nodes carry :Elevator label",
        ),
    ]

    for cypher, ok_fn, err_fn, ok_msg in blocking_checks:
        n = run_count_check(neo4j_manager, cypher)
        if ok_fn(n):
            result.note(ok_msg)
        else:
            result.fail(err_fn(n))

    # ── Check 9: every Pathway participates in at least one [:LINKS] relationship
    #
    # Checks both directions (undirected) — a Pathway may have only incoming
    # LINKS (when its from_stop is a stop entity) or only outgoing LINKS
    # (when its to_stop is a stop entity), or only pathway-to-pathway LINKS.
    # A Pathway with no LINKS in either direction is a dangling node indicating
    # a classification or load failure.

    n = run_count_check(
        neo4j_manager,
        "MATCH (p:Pathway) WHERE NOT (p)-[:LINKS]-() RETURN count(p) AS n",
    )
    if n == 0:
        result.note("All Pathway nodes participate in at least one [:LINKS] relationship")
    else:
        result.warn(
            f"{n} Pathway node(s) have no [:LINKS] relationship in either direction — "
            f"indicates a classification gap or load failure"
        )

    # ── Check 10: every Station has at least one CONTAINS→Platform ────────────
    #
    # A station with no platform children is architecturally invalid for the
    # WMATA rail network. Warn rather than block — bus-only or entrance-only
    # stations may exist in edge cases.

    n = run_count_check(
        neo4j_manager,
        """
        MATCH (s:Station)
        WHERE NOT (s)-[:CONTAINS]->(:Platform)
        RETURN count(s) AS n
        """,
    )
    if n == 0:
        result.note("All Station nodes have at least one [:CONTAINS]->(:Platform)")
    else:
        result.warn(
            f"{n} Station(s) have no [:CONTAINS]->(:Platform) — expected for "
            f"WMATA rail stations; may indicate a load failure or bus-only station"
        )

    # ── Check 12: soft node counts (info only) ────────────────────────────────
    #
    # No hard expected values — recorded as info so changes after a feed
    # update are visible in the pipeline log.

    soft_counts = [
        ("MATCH (s:Station)          RETURN count(s) AS n", "Station"),
        ("MATCH (p:Platform)         RETURN count(p) AS n", "Platform"),
        ("MATCH (fg:FareGate)        RETURN count(fg) AS n", "FareGate"),
        ("MATCH (e:StationEntrance)  RETURN count(e) AS n", "StationEntrance"),
        ("MATCH (p:Pathway)          RETURN count(p) AS n", "Pathway"),
        ("MATCH (l:Level)            RETURN count(l) AS n", "Level"),
        ("MATCH (b:BusStop)          RETURN count(b) AS n", "BusStop"),
    ]

    for cypher, label in soft_counts:
        n = run_count_check(neo4j_manager, cypher)
        result.note(f"{label}: {n}")

    # ── Check 13: Level nodes are connected via ON_LEVEL ─────────────────────
    #
    # Level nodes are the only physical nodes that arrive with no relationships
    # until the ON_LEVEL, STARTING_LEVEL, and ENDING_LEVEL loaders run.
    # A high count of isolated Level nodes indicates those loaders failed
    # silently (e.g. level_id mismatch between stops.txt and levels.txt).
    # Warn rather than block — partial connectivity is still useful.

    n_isolated = run_count_check(
        neo4j_manager,
        """
        MATCH (l:Level)
        WHERE NOT (l)<-[:ON_LEVEL|STARTING_LEVEL|ENDING_LEVEL]-()
        RETURN count(l) AS n
        """,
    )
    n_total = run_count_check(neo4j_manager, "MATCH (l:Level) RETURN count(l) AS n")
    if n_isolated == 0:
        result.note(f"All {n_total} Level nodes have at least one incoming level relationship")
    elif n_total > 0 and n_isolated / n_total > 0.5:
        result.warn(
            f"{n_isolated}/{n_total} Level nodes have no incoming ON_LEVEL, "
            f"STARTING_LEVEL, or ENDING_LEVEL relationship — "
            f"level_id values may not match between stops.txt and levels.txt"
        )
    else:
        result.note(
            f"{n_isolated}/{n_total} Level nodes are not connected — "
            f"may be levels referenced by no stop or pathway in this feed"
        )

    return result
