# src/layers/physical/transform.py
"""
Physical layer — Transform

Converts raw GTFS DataFrames into clean, Neo4j-ready DataFrames for physical infrastructure.
Includes stop, pathway, and level cleaning, tagging, and partitioning.
"""

import numpy as np
import pandas as pd

from src.common.logger import get_logger
from src.common.utils import clean_str, safe_int
from src.layers.physical.endpoint_classifier import EndpointClass, classify_endpoints

log = get_logger(__name__)

# GTFS location_type values
_LOC_PLATFORM = 0  # Covers both Platform and BusStops
_LOC_STATION = 1
_LOC_ENTRANCE = 2
# location_type=3 is the GTFS generic node (infrastructure pivot, not a transit stop).
# These are treated as DEFERRED in endpoint classification and not loaded as graph nodes.
# FareGates are identified by the _FG_ pattern in stop_id, not by location_type.
_LOC_GENERIC = 3

PATHWAY_MODES = {
    1: "Walkway",
    2: "Stairs",
    3: "Moving sidewalk/travelator",
    4: "Escalator",
    5: "Elevator",
    6: "Fare gate/turnstile",
    7: "Exit gate (outbound turnstile)",  # Non-standard extension used by this feed
}
FAREGATE_MODES = {6, 7}



def _build_stop_on_level(stops_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns DataFrame[stop_id, level_id] for every stop that has a level_id.
    Covers Station, StationEntrance, Platform, FareGate — all stop types
    present in stops.txt with a non-null level value.
    """
    has_level = stops_df[stops_df["level"].notna()][["id", "level"]].copy()
    return (
        has_level
        .rename(columns={"id": "stop_id", "level": "level_id"})
        .drop_duplicates()
        .reset_index(drop=True)
    )


def _build_pathway_on_level(
    pathways_df: pd.DataFrame,
    stops_df: pd.DataFrame,
    levels_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """
    Build level-relationship frames for all Pathway nodes.

    Three relationship types are returned, keyed by relationship name:

      on_level        — Pathway -[:ON_LEVEL]-> Level
                        Used for elevators (all levels in the traversed range)
                        and pathways whose endpoints share the same level.

      starting_level  — Pathway -[:STARTING_LEVEL]-> Level
                        Used for escalators and multi-level walkways/stairs
                        (the level at from_stop_id).

      ending_level    — Pathway -[:ENDING_LEVEL]-> Level
                        Used for escalators and multi-level walkways/stairs
                        (the level at to_stop_id).

    Elevator range derivation (Option B):
        GTFS records only the two terminal stops of an elevator. A single
        elevator that serves L0→L2 is modelled as one pathway, but physically
        crosses L0, L1, and L2. For each elevator, all Level nodes at the same
        station with a level_index between the two endpoint indices (inclusive)
        are emitted as ON_LEVEL targets.

    Escalators always use STARTING_LEVEL / ENDING_LEVEL (direction matters).
    Walkways (mode=1) and other non-elevator modes check whether both endpoints
    share the same level: same → ON_LEVEL; different → STARTING/ENDING_LEVEL.
    """
    # ── Lookup tables ─────────────────────────────────────────────────────────
    stop_to_level: dict[str, str] = (
        stops_df.dropna(subset=["level"]).set_index("id")["level"].to_dict()
    )
    level_to_index: dict[str, int | None] = {
        row["level_id"]: safe_int(row["level_index"])
        for _, row in levels_df.iterrows()
    }

    # station_id for each stop: the stop itself if it IS a station,
    # otherwise its parent_station.
    def _station_of(row) -> str | None:
        if row["location_type"] == 1:
            return row["id"]
        ps = row.get("parent_station")
        return ps if ps and not pd.isna(ps) else None

    # Build station_id → sorted list of level_ids (ascending level_index)
    station_level_sets: dict[str, set[str]] = {}
    for _, row in stops_df[stops_df["level"].notna()].iterrows():
        sid = _station_of(row)
        lid = row["level"]
        if sid and lid:
            station_level_sets.setdefault(sid, set()).add(lid)

    station_sorted_levels: dict[str, list[str]] = {
        sid: sorted(
            lids,
            key=lambda l: (level_to_index.get(l) or 0),
        )
        for sid, lids in station_level_sets.items()
    }

    stop_to_station: dict[str, str | None] = {
        row["id"]: _station_of(row) for _, row in stops_df.iterrows()
    }

    # ── Augment pathway frame ──────────────────────────────────────────────────
    pw = pathways_df[["id", "mode", "from_stop_id", "to_stop_id"]].copy()
    pw["from_level_id"] = pw["from_stop_id"].map(stop_to_level)
    pw["to_level_id"] = pw["to_stop_id"].map(stop_to_level)
    pw["from_level_index"] = pw["from_level_id"].map(level_to_index)
    pw["to_level_index"] = pw["to_level_id"].map(level_to_index)
    pw["mode_int"] = pw["mode"].apply(safe_int)
    pw["station_id"] = pw["from_stop_id"].map(stop_to_station).fillna(
        pw["to_stop_id"].map(stop_to_station)
    )

    on_level_rows: list[dict] = []
    starting_rows: list[dict] = []
    ending_rows: list[dict] = []

    for _, row in pw.iterrows():
        pid = row["id"]
        mode = row["mode_int"]
        from_lid = row["from_level_id"] if not pd.isna(row["from_level_id"]) else None
        to_lid = row["to_level_id"] if not pd.isna(row["to_level_id"]) else None
        from_idx = row["from_level_index"] if not pd.isna(row["from_level_index"]) else None
        to_idx = row["to_level_index"] if not pd.isna(row["to_level_index"]) else None
        station_id = row["station_id"] if not pd.isna(row["station_id"]) else None

        if from_lid is None and to_lid is None:
            continue  # No level info — skip entirely

        if mode == 5:  # Elevator — range derivation
            if from_lid is None or to_lid is None:
                # One endpoint unresolvable — emit what we have
                on_level_rows.append({"pathway_id": pid, "level_id": from_lid or to_lid})
            elif from_lid == to_lid or from_idx == to_idx:
                on_level_rows.append({"pathway_id": pid, "level_id": from_lid})
            else:
                # Derive all levels crossed at this station
                min_idx = min(from_idx or 0, to_idx or 0)
                max_idx = max(from_idx or 0, to_idx or 0)
                if station_id and station_id in station_sorted_levels:
                    crossed = [
                        lid
                        for lid in station_sorted_levels[station_id]
                        if (level_to_index.get(lid) is not None)
                        and min_idx <= level_to_index[lid] <= max_idx
                    ]
                else:
                    crossed = list({from_lid, to_lid})
                for lid in crossed:
                    on_level_rows.append({"pathway_id": pid, "level_id": lid})

        elif mode == 4:  # Escalator — always directional
            if from_lid:
                starting_rows.append({"pathway_id": pid, "level_id": from_lid})
            if to_lid:
                ending_rows.append({"pathway_id": pid, "level_id": to_lid})

        else:  # Walkway (1), Stairs (2), moving sidewalk (3), etc.
            if from_lid is None or to_lid is None:
                on_level_rows.append({"pathway_id": pid, "level_id": from_lid or to_lid})
            elif from_lid == to_lid or from_idx == to_idx:
                on_level_rows.append({"pathway_id": pid, "level_id": from_lid})
            else:
                # Spans multiple levels — treat like escalator
                starting_rows.append({"pathway_id": pid, "level_id": from_lid})
                ending_rows.append({"pathway_id": pid, "level_id": to_lid})

    def _to_df(rows: list[dict]) -> pd.DataFrame:
        if rows:
            return pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)
        return pd.DataFrame(columns=["pathway_id", "level_id"])

    return {
        "on_level": _to_df(on_level_rows),
        "starting_level": _to_df(starting_rows),
        "ending_level": _to_df(ending_rows),
    }


def _partition_by_node_type(
    pairs: pd.DataFrame,
    stops_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """
    Partition (pathway_id, stop_id) pairs by target Neo4j node label.

    Merges pairs against stops_df to resolve location_type, then dispatches
    each row to the correct partition using the same predicates as the node
    loaders (id prefix conventions take priority over location_type).

    Args:
        pairs: DataFrame[pathway_id, stop_id] — already filtered to matched stops.
        stops_df: Post-rename stops with 'id' and 'location_type' columns.

    Returns:
        Dict keyed by node label constant: ENTRANCE, PLATFORM, STATION,
        FAREGATE, BUS_STOP. Each value is DataFrame[pathway_id, stop_id].
    """
    df = pairs.merge(
        stops_df[["id", "location_type"]].rename(columns={"id": "stop_id"}),
        on="stop_id",
        how="inner",
    )
    return {
        "ENTRANCE": df[
            df["location_type"] == _LOC_ENTRANCE
        ][["pathway_id", "stop_id"]].drop_duplicates().copy(),
        "PLATFORM": df[
            df["location_type"].isin([_LOC_PLATFORM, 4])
            & df["stop_id"].str.upper().str.startswith("PF_", na=False)
        ][["pathway_id", "stop_id"]].drop_duplicates().copy(),
        "STATION": df[
            df["location_type"] == _LOC_STATION
        ][["pathway_id", "stop_id"]].drop_duplicates().copy(),
        "FAREGATE": df[
            df["stop_id"].str.contains("_FG_", na=False)
        ][["pathway_id", "stop_id"]].drop_duplicates().copy(),
        "BUS_STOP": df[
            df["stop_id"].apply(safe_int).notnull()
            & ~df["stop_id"].str.contains("_FG_", na=False)
            & ~df["stop_id"].str.upper().str.startswith("PF_", na=False)
        ][["pathway_id", "stop_id"]].drop_duplicates().copy(),
    }


def run(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    stops_df = raw["stops"].copy()
    pathways_df = raw["pathways"].copy()
    levels_df = raw["levels"].copy()
    feed_info_df = raw["feed_info"].copy()

    # Clean stop_id and stop_desc columns
    def _clean_col(s: pd.Series) -> pd.Series:
        cleaned = s.astype(str).str.strip()
        return cleaned.where(~cleaned.isin(["", "nan", "None"]), other=None)

    stops_df["stop_id"] = _clean_col(stops_df["stop_id"])
    stops_df["stop_desc"] = _clean_col(stops_df["stop_desc"])
    if "parent_station" in stops_df:
        stops_df["parent_station"] = _clean_col(stops_df["parent_station"])
    if "level_id" in stops_df:
        stops_df["level_id"] = _clean_col(stops_df["level_id"])

    # Rename columns for ERD compatibility
    stops_df = stops_df.rename(
        columns={
            "stop_id": "id",
            "stop_name": "name",  # GTFS stop_name is canonical for 'name'
            "stop_desc": "desc",  # Optional, not in ERD but may be useful
            "level_id": "level",
            # Add more as needed for location, etc.
        }
    )

    faregates = stops_df[stops_df["id"].str.contains("_FG_", na=False)].copy()

    # Tag bus vs rail stops — exclude location_type=3 (generic nodes) to prevent
    # them being misclassified as MATCHED endpoints instead of DEFERRED pivots.
    bus_stops = stops_df[
        stops_df["id"].apply(safe_int).notnull() & (stops_df["location_type"] != 3)
    ].copy()
    stations = stops_df[stops_df["location_type"] == _LOC_STATION].copy()

    station_contains_platform = stops_df[stops_df["location_type"] == _LOC_PLATFORM][
        ["parent_station", "id"]
    ].rename(columns={"parent_station": "station_id", "id": "platform_id"})

    station_contains_entrance = stops_df[stops_df["location_type"] == _LOC_ENTRANCE][
        ["parent_station", "id"]
    ].rename(columns={"parent_station": "station_id", "id": "entrance_id"})

    station_contains_faregate = stops_df[
        stops_df["id"].str.contains("_FG_", na=False)
        & stops_df["parent_station"].notna()
    ][["parent_station", "id"]].rename(
        columns={"parent_station": "station_id", "id": "faregate_id"}
    )

    # Pathways: remove 'name', use 'id' from pathway_id, and map other fields
    pathways_df = pathways_df.rename(
        columns={
            "pathway_id": "id",
            "pathway_mode": "mode",
            "is_bidirectional": "is_bidirectional",
            "length": "length",
            "elevation_gain": "elevation_gain",
            "wheelchair_accessible": "wheelchair_accessible",
            # zone and mode labels are already handled in transform logic
        }
    )
    # Remove 'name' column if present (not in GTFS pathways)
    if "name" in pathways_df.columns:
        pathways_df = pathways_df.drop(columns=["name"])

    # Pathway mode description
    pathways_df["mode_description"] = (
        pathways_df["mode"].apply(safe_int).map(PATHWAY_MODES)
    )

    # Partition stops by location_type
    entrances = stops_df[
        (stops_df["location_type"] == 2)
        & (stops_df["id"].str.upper().str.startswith("ENT_"))
    ]

    platforms = stops_df[
        # location_type=4 is a non-standard extension used by this GTFS feed
        # to tag boarding areas; treated identically to platforms (location_type=0).
        (stops_df["location_type"].isin([0, 4]))
        & (stops_df["id"].str.upper().str.startswith("PF_"))
    ]

    # Tag pathway sides — use frozensets for O(1) membership, not Series (which
    # checks the index, not the values).
    #
    # Zone anchors beyond ENT_*/PF_* — this feed uses a naming convention where
    # generic nodes (location_type=3) that sit immediately beside a fare barrier
    # carry a _PAID or _UNPAID suffix (e.g. NODE_A01_C01_N_FG_PAID).  These are
    # not loaded as graph nodes but their zone side is known, so we include them
    # as anchors when tagging pathway endpoints.
    entrance_ids = frozenset(entrances["id"])
    platform_ids = frozenset(platforms["id"])
    faregate_unpaid_ids = frozenset(
        stops_df[stops_df["id"].str.endswith("_UNPAID", na=False)]["id"]
    )
    faregate_paid_ids = frozenset(
        stops_df[stops_df["id"].str.endswith("_PAID", na=False)]["id"]
    )
    ids = stops_df["id"]
    unpaid_mask = ids.isin(entrance_ids) | ids.isin(faregate_unpaid_ids)
    paid_mask   = ids.isin(platform_ids) | ids.isin(faregate_paid_ids)
    node_side = dict(zip(ids, np.select([unpaid_mask, paid_mask], ["UNPAID", "PAID"], default="UNKNOWN")))

    pathways_df["from_side"] = pathways_df["from_stop_id"].map(node_side)
    pathways_df["to_side"] = pathways_df["to_stop_id"].map(node_side)

    # Categorize pathways
    #
    # The paid zone in this feed has no direct PAID→PAID pathway hops — every
    # paid-zone pathway connects a known anchor (FG_PAID or PF_*) to a generic
    # intermediate node (PLF_* boarding points, mezzanine nodes, etc.).  The
    # same holds for the unpaid zone.  We therefore use a one-anchor rule:
    # if one endpoint is PAID and neither is UNPAID → postgate_internal (Paid);
    # if one endpoint is UNPAID and neither is PAID → pregate_internal (Unpaid).
    # Pathways where both endpoints are UNKNOWN (deep mezzanine hops) cannot be
    # zone-classified without graph traversal and remain mixed_non_gate for now.
    mode = pathways_df["mode"].apply(safe_int).fillna(-1).astype(int)
    fs = pathways_df["from_side"]
    ts = pathways_df["to_side"]
    pathways_df["side_category"] = np.select(
        [
            (mode == 7) & (fs == "PAID") & (ts == "UNPAID"),
            (mode == 6) & (fs != ts),
            (fs == "UNPAID") & (ts == "UNPAID"),
            (fs == "PAID") & (ts == "PAID"),
            fs.isin(["PAID"]) & ~ts.isin(["UNPAID"]),
            ts.isin(["PAID"]) & ~fs.isin(["UNPAID"]),
        ],
        ["exit_gate", "cross_faregate", "pregate_internal", "postgate_internal", "postgate_internal", "pregate_internal"],
        default="mixed_non_gate",
    )

    _zone_map = {"pregate_internal": "Unpaid", "postgate_internal": "Paid"}
    pathways_df["zone"] = pathways_df["side_category"].map(_zone_map)

    # Add stop descriptions for from_stop_id and to_stop_id
    desc_map = stops_df.set_index("id")["desc"]
    pathways_df["from_stop_desc"] = pathways_df["from_stop_id"].str.strip().map(desc_map)
    pathways_df["to_stop_desc"]   = pathways_df["to_stop_id"].str.strip().map(desc_map)

    # ── Endpoint classification ───────────────────────────────────────────────
    pathways_df["is_bidirectional"] = (
        pathways_df["is_bidirectional"].apply(safe_int).fillna(0).astype(int)
    )

    partition_id_sets = {
        "stations": frozenset(stations["id"].astype(str)),
        "entrances": frozenset(entrances["id"].astype(str)),
        "platforms": frozenset(platforms["id"].astype(str)),
        "faregates": frozenset(faregates["id"].astype(str)),
        "bus_stops": frozenset(bus_stops["id"].astype(str)),
    }

    all_endpoint_ids = pd.concat(
        [pathways_df["from_stop_id"].dropna(), pathways_df["to_stop_id"].dropna()]
    )
    endpoint_classifications = classify_endpoints(
        all_endpoint_ids, stops_df, partition_id_sets
    )

    gap_ids = endpoint_classifications.loc[
        endpoint_classifications["classification"] == EndpointClass.GAP.value, "stop_id"
    ].tolist()
    missing_ids = endpoint_classifications.loc[
        endpoint_classifications["classification"] == EndpointClass.MISSING.value, "stop_id"
    ].tolist()
    if gap_ids or missing_ids:
        raise ValueError(
            f"Pathway endpoint classification failed — pipeline cannot proceed.\n"
            f"  gap (in stops_df but no partition match): {gap_ids}\n"
            f"  missing (not in stops_df at all): {missing_ids}"
        )

    matched_ids_set = frozenset(
        endpoint_classifications.loc[
            endpoint_classifications["classification"] == EndpointClass.MATCHED.value, "stop_id"
        ]
    )
    deferred_ids = frozenset(
        endpoint_classifications.loc[
            endpoint_classifications["classification"] == EndpointClass.DEFERRED.value, "stop_id"
        ]
    )

    # ── Directional link frames ───────────────────────────────────────────────
    bidir_ids = frozenset(pathways_df.loc[pathways_df["is_bidirectional"] == 1, "id"])

    from_pairs = (
        pathways_df[["id", "from_stop_id"]]
        .rename(columns={"id": "pathway_id", "from_stop_id": "stop_id"})
        .dropna(subset=["stop_id"])
        .loc[lambda df: df["stop_id"].isin(matched_ids_set)]
    )
    to_pairs = (
        pathways_df[["id", "to_stop_id"]]
        .rename(columns={"id": "pathway_id", "to_stop_id": "stop_id"})
        .dropna(subset=["stop_id"])
        .loc[lambda df: df["stop_id"].isin(matched_ids_set)]
    )

    original_from_links = _partition_by_node_type(from_pairs, stops_df)
    original_to_links = _partition_by_node_type(to_pairs, stops_df)

    # Bidirectional mirrors: each bidirectional pathway adds reverse links on all sides.
    # Snapshot originals before mutation to avoid double-counting.
    from_links: dict[str, pd.DataFrame] = {}
    to_links: dict[str, pd.DataFrame] = {}
    for key in ("ENTRANCE", "PLATFORM", "STATION", "FAREGATE", "BUS_STOP"):
        bidir_to_rows = original_to_links[key][
            original_to_links[key]["pathway_id"].isin(bidir_ids)
        ]
        bidir_from_rows = original_from_links[key][
            original_from_links[key]["pathway_id"].isin(bidir_ids)
        ]
        from_links[key] = pd.concat(
            [original_from_links[key], bidir_to_rows], ignore_index=True
        ).drop_duplicates()
        to_links[key] = pd.concat(
            [original_to_links[key], bidir_from_rows], ignore_index=True
        ).drop_duplicates()

    # ── Pathway chain links (deferred generic node pivots) ────────────────────
    # A bidirectional pathway can be traversed in reverse, so its from_stop is
    # also a valid exit point and its to_stop is also a valid entry point.
    # Include both directions so that escalators/stairs between two DEFERRED nodes
    # gain chain links via the bidirectional access pathways that share their endpoints.
    _bidir_pw = pathways_df[pathways_df["is_bidirectional"] == 1]
    left = pd.concat(
        [
            pathways_df[["id", "to_stop_id"]].rename(
                columns={"id": "from_pathway_id", "to_stop_id": "pivot"}
            ),
            _bidir_pw[["id", "from_stop_id"]].rename(
                columns={"id": "from_pathway_id", "from_stop_id": "pivot"}
            ),
        ],
        ignore_index=True,
    ).loc[lambda df: df["pivot"].isin(deferred_ids)].drop_duplicates()

    right = pd.concat(
        [
            pathways_df[["id", "from_stop_id"]].rename(
                columns={"id": "to_pathway_id", "from_stop_id": "pivot"}
            ),
            _bidir_pw[["id", "to_stop_id"]].rename(
                columns={"id": "to_pathway_id", "to_stop_id": "pivot"}
            ),
        ],
        ignore_index=True,
    ).loc[lambda df: df["pivot"].isin(deferred_ids)].drop_duplicates()
    forward_chains = (
        left.merge(right, on="pivot", how="inner")
        .loc[lambda df: df["from_pathway_id"] != df["to_pathway_id"]]
        [["from_pathway_id", "to_pathway_id"]]
        .drop_duplicates()
    )

    pathway_chain_links = forward_chains

    # ── Level relationship frames ──────────────────────────────────────────────
    stop_on_level = _build_stop_on_level(stops_df)
    pathway_level_rels = _build_pathway_on_level(pathways_df, stops_df, levels_df)

    # Partition pathways
    fare_boundary_edges = pathways_df[pathways_df["mode"].isin(FAREGATE_MODES)].copy()
    pre_gate_edges = pathways_df[
        pathways_df["side_category"] == "pregate_internal"
    ].copy()
    post_gate_edges = pathways_df[
        pathways_df["side_category"] == "postgate_internal"
    ].copy()

    # Partition nodes
    pre_gate_nodes = stops_df[stops_df["id"].map(node_side) == "UNPAID"].copy()
    post_gate_nodes = stops_df[stops_df["id"].map(node_side) == "PAID"].copy()

    # Return all cleaned/partitioned DataFrames
    return {
        "stations": stations,
        "faregates": faregates,
        "platforms": platforms,
        "entrances": entrances,
        "station_contains_platform": station_contains_platform,
        "station_contains_entrance": station_contains_entrance,
        "station_contains_faregate": station_contains_faregate,
        "pathways": pathways_df,
        "levels": levels_df,
        "feed_info": feed_info_df,
        "bus_stops": bus_stops,
        "fare_boundary_edges": fare_boundary_edges,
        "pre_gate_edges": pre_gate_edges,
        "post_gate_edges": post_gate_edges,
        "pre_gate_nodes": pre_gate_nodes,
        "post_gate_nodes": post_gate_nodes,
        "from_links": from_links,
        "to_links": to_links,
        "pathway_chain_links": pathway_chain_links,
        "endpoint_classifications": endpoint_classifications,
        "stop_on_level": stop_on_level,
        "pathway_on_level": pathway_level_rels["on_level"],
        "pathway_starting_level": pathway_level_rels["starting_level"],
        "pathway_ending_level": pathway_level_rels["ending_level"],
    }
