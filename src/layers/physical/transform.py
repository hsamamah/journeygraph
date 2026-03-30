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


def get_level_description(level_id):
    if pd.isna(level_id) or clean_str(level_id) is None:
        return "Level Not specified"
    level_indicator = str(level_id).split("_")[-1]
    level_mapping = {
        "L0": "Street Level",
        "L1": "Mezzanine/Fare Control",
        "L2": "Platform Level",
        "L3": "Lower Platform/Deep Level",
        "UL1": "Upper Level (Above Ground)",
    }
    return level_mapping.get(level_indicator, f"Unknown Level ({level_indicator})")


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

    # Add level description
    stops_df["level_description"] = stops_df["level_id"].apply(get_level_description)

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
    from_desc_map = stops_df.set_index("id")["desc"]
    to_desc_map = stops_df.set_index("id")["desc"]
    pathways_df["from_stop_desc"] = pathways_df["from_stop_id"].str.strip().map(from_desc_map)
    pathways_df["to_stop_desc"]   = pathways_df["to_stop_id"].str.strip().map(to_desc_map)

    # Add level info for from/to stops
    from_level_map = stops_df.set_index("id")["level_description"]
    to_level_map = stops_df.set_index("id")["level_description"]
    pathways_df["from_level_description"] = pathways_df["from_stop_id"].str.strip().map(from_level_map)
    pathways_df["to_level_description"]   = pathways_df["to_stop_id"].str.strip().map(to_level_map)

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
    }
