# src/layers/physical/endpoint_classifier.py
"""
Endpoint classification for pathway stop_ids.

Classifies each stop_id that appears as a pathway endpoint (from_stop_id or
to_stop_id) into one of four categories:

  MATCHED  — stop_id exists in a loaded node partition (Station, StationEntrance,
             Platform, FareGate, or BusStop). A directional LINKS relationship
             will be created for this endpoint.

  DEFERRED — stop_id is a GTFS generic node (location_type=3, no _FG_ pattern).
             These are used as pivots for pathway-to-pathway chaining and are
             intentionally not loaded as graph nodes. A Pathway -[:LINKS]-> Pathway
             relationship will be created instead.

  GAP      — stop_id exists in stops_df but matches no loaded partition predicate.
             Indicates a bug in the loading logic — pipeline should block.

  MISSING  — stop_id does not exist in stops_df at all.
             Indicates broken source data — pipeline should block.
"""

from __future__ import annotations

from enum import Enum

import pandas as pd


class EndpointClass(str, Enum):
    MATCHED = "matched"
    DEFERRED = "deferred"
    GAP = "gap"
    MISSING = "missing"


def classify_endpoints(
    stop_ids: pd.Series,
    stops_df: pd.DataFrame,
    partition_id_sets: dict[str, frozenset[str]],
) -> pd.DataFrame:
    """
    Classify each unique stop_id in stop_ids into one of four categories.

    Args:
        stop_ids: Series of stop_id values to classify (may contain duplicates/NaN).
        stops_df: Full stops DataFrame with 'id' and 'location_type' columns
                  (post-rename: stop_id has already been renamed to id).
        partition_id_sets: Dict mapping partition names to frozensets of loaded ids.
                           Expected keys: "stations", "entrances", "platforms",
                           "faregates", "bus_stops".

    Returns:
        DataFrame[stop_id, classification]
    """
    # list() forces a plain Python list from the pandas StringArray that
    # .unique() returns when the Series has StringDtype (pandas 2.x default).
    unique_ids: list[str] = list(stop_ids.dropna().astype(str).unique())
    known_ids = frozenset(stops_df["id"].astype(str))
    matched_ids = frozenset().union(*partition_id_sets.values())
    deferred_ids = frozenset(
        stops_df[
            (stops_df["location_type"] == 3)
            & ~stops_df["id"].str.contains("_FG_", na=False)
        ]["id"].astype(str)
    )

    def _classify(stop_id: str) -> str:
        if stop_id in matched_ids:
            return EndpointClass.MATCHED.value
        if stop_id in deferred_ids:
            return EndpointClass.DEFERRED.value
        if stop_id in known_ids:
            return EndpointClass.GAP.value
        return EndpointClass.MISSING.value

    result = pd.DataFrame({"stop_id": unique_ids})
    result["classification"] = result["stop_id"].map(_classify)
    return result
