# src/layers/interruption/__init__.py
"""
Interruption layer

Ingests GTFS-RT feeds (trip updates + service alerts) and produces a
three-tier disruption model:

  Tier 1 — Raw source nodes: TripUpdate, StopTimeUpdate, ServiceAlert,
           EntitySelector. Preserved for LLM explainability and audit trail.

  Tier 2 — Normalized Interruption nodes with multi-labels
           (:Cancellation, :Delay, :Skip, :Detour, :ServiceChange, :Accessibility).
           Created by transform rules from Tier 1 sources.

  Tier 3 — Cross-layer connections: AFFECTS_TRIP, AFFECTS_ROUTE,
           AFFECTS_STOP, ON_DATE, DURING_PLANNED_SERVICE.

Execution model:
  Single poll per pipeline run. Fetches current GTFS-RT state, flattens
  protobuf to DataFrames, applies transform rules, loads to Neo4j.
  MERGE-based loading ensures idempotency across re-runs.
  Designed so adding a --poll loop later just wraps run() in a timer.

Prerequisites:
  Service & Schedule layer must be loaded (Trip, Route, ServicePattern nodes).
  Physical layer should be loaded for AFFECTS_STOP connections.

Usage:
  python -m src.pipeline --layers interruption
"""

from typing import TYPE_CHECKING

from src.common.logger import get_logger
from src.layers.interruption import extract, transform

if TYPE_CHECKING:
    import pandas as pd

log = get_logger(__name__)


def run(gtfs_data: dict[str, pd.DataFrame], neo4j, api_client) -> None:
    """
    Execute the full interruption layer pipeline.

    Args:
        gtfs_data: shared GTFS static data (for feed_info, trip lookups)
        neo4j: Neo4jManager instance
        api_client: WMATAClient instance for GTFS-RT feeds
    """
    from src.layers.interruption import load

    log.info("=== Interruption layer: starting ===")
    raw = extract.run(api_client, gtfs_data)
    result = transform.run(raw)
    load.run(result, neo4j, gtfs_data)
    log.info("=== Interruption layer: complete ===")
