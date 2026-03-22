# src/layers/physical/transform.py
"""
Physical layer — Transform

Converts raw GTFS DataFrames into clean, Neo4j-ready DataFrames for physical infrastructure.
Includes stop, pathway, and level cleaning, tagging, and partitioning.
"""

import pandas as pd

from src.common.logger import get_logger
from src.common.utils import clean_str, safe_int

log = get_logger(__name__)

# GTFS location_type values
_LOC_PLATFORM = 0  # Covers both Platform and BusStops
_LOC_STATION = 1
_LOC_ENTRANCE = 2
_LOC_FAREGATE = 3

PATHWAY_MODES = {
    1: "Walkway",
    2: "Stairs",
    3: "Moving sidewalk/travelator",
    4: "Escalator",
    5: "Elevator",
    6: "Fare gate/turnstile",
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


def run(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    stops_df = raw["stops"].copy()
    pathways_df = raw["pathways"].copy()
    levels_df = raw["levels"].copy()
    feed_info_df = raw["feed_info"].copy()

    # Clean stop_id and stop_desc columns
    stops_df["stop_id"] = stops_df["stop_id"].apply(clean_str)
    stops_df["stop_desc"] = stops_df["stop_desc"].apply(clean_str)
    if "parent_station" in stops_df:
        stops_df["parent_station"] = stops_df["parent_station"].apply(clean_str)
    if "level_id" in stops_df:
        stops_df["level_id"] = stops_df["level_id"].apply(clean_str)

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

    # Tag bus vs rail stops
    bus_stops = stops_df[stops_df["id"].apply(safe_int).notnull()].copy()
    stations = stops_df[stops_df["location_type"] == _LOC_STATION].copy()

    station_contains_platform = stops_df[stops_df["location_type"] == _LOC_PLATFORM][
        ["parent_station", "id"]
    ].rename(columns={"parent_station": "station_id", "id": "platform_id"})

    station_contains_entrance = stops_df[stops_df["location_type"] == _LOC_ENTRANCE][
        ["parent_station", "id"]
    ].rename(columns={"parent_station": "station_id", "id": "entrance_id"})

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
        (stops_df["location_type"].isin([0, 4]))
        & (stops_df["id"].str.upper().str.startswith("PF_"))
    ]

    # Tag pathway sides
    node_side = {}
    for stop in stops_df["id"]:
        if stop in entrances["id"]:
            node_side[stop] = "UNPAID"
        elif stop in platforms["id"]:
            node_side[stop] = "PAID"
        else:
            node_side[stop] = "UNKNOWN"

    pathways_df["from_side"] = pathways_df["from_stop_id"].map(node_side)
    pathways_df["to_side"] = pathways_df["to_stop_id"].map(node_side)

    # Categorize pathways
    def categorize(row):
        mode = int(row["mode"])
        fs, ts = row["from_side"], row["to_side"]
        if mode == 7 and fs == "PAID" and ts == "UNPAID":
            return "exit_gate"
        if mode == 6 and fs != ts:
            return "cross_faregate"
        if fs == "UNPAID" and ts == "UNPAID":
            return "pregate_internal"
        if fs == "PAID" and ts == "PAID":
            return "postgate_internal"
        return "mixed_non_gate"

    pathways_df["side_category"] = pathways_df.apply(categorize, axis=1)

    # Add stop descriptions for from_stop_id and to_stop_id
    from_desc_map = stops_df.set_index("id")["desc"]
    to_desc_map = stops_df.set_index("id")["desc"]
    pathways_df["from_stop_desc"] = (
        pathways_df["from_stop_id"].apply(clean_str).map(from_desc_map)
    )
    pathways_df["to_stop_desc"] = (
        pathways_df["to_stop_id"].apply(clean_str).map(to_desc_map)
    )

    # Add level info for from/to stops
    from_level_map = stops_df.set_index("id")["level_description"]
    to_level_map = stops_df.set_index("id")["level_description"]
    pathways_df["from_level_description"] = (
        pathways_df["from_stop_id"].apply(clean_str).map(from_level_map)
    )
    pathways_df["to_level_description"] = (
        pathways_df["to_stop_id"].apply(clean_str).map(to_level_map)
    )

    # Build a single (pathway_id, stop_id) frame covering both endpoints
    stop_types = stops_df[["id", "location_type"]].copy()

    from_side = pathways_df[["id", "from_stop_id"]].rename(
        columns={"id": "pathway_id", "from_stop_id": "stop_id"}
    )
    to_side = pathways_df[["id", "to_stop_id"]].rename(
        columns={"id": "pathway_id", "to_stop_id": "stop_id"}
    )
    all_links = (
        pd.concat([from_side, to_side], ignore_index=True)
        .dropna(subset=["stop_id"])
        .drop_duplicates()
        .merge(stop_types.rename(columns={"id": "stop_id"}), on="stop_id", how="inner")
    )

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
        "pathways": pathways_df,
        "levels": levels_df,
        "feed_info": feed_info_df,
        "bus_stops": bus_stops,
        "fare_boundary_edges": fare_boundary_edges,
        "pre_gate_edges": pre_gate_edges,
        "post_gate_edges": post_gate_edges,
        "pre_gate_nodes": pre_gate_nodes,
        "post_gate_nodes": post_gate_nodes,
        "links": {
            "ENTRANCE": all_links[all_links["location_type"] == _LOC_ENTRANCE],
            "PLATFORM": all_links[all_links["location_type"] == _LOC_PLATFORM],
            "STATION": all_links[all_links["location_type"] == _LOC_STATION],
            "FAREGATE": all_links[all_links["location_type"] == _LOC_FAREGATE],
        },
    }
