# src/layers/service_schedule/__init__.py
"""
Service & Schedule layer

Orchestrates extract → transform → load for the static GTFS baseline.
Exposes a single run() entry point consumed by pipeline.py.

Nodes created:
  :FeedInfo, :Agency, :Route (:Bus|:Rail), :RoutePattern,
  :Trip, :ServicePattern (:Weekday|:Saturday|:Sunday|:Holiday|:Maintenance),
  :Date

Relationships created:
  Agency     -[:OPERATES]->         Route
  Route      -[:OPERATED_BY]->      Agency
  Route      -[:HAS_PATTERN]->      RoutePattern
  Route      -[:SERVES]->           Station | BusStop
  RoutePattern -[:BELONGS_TO]->     Route
  RoutePattern -[:HAS_TRIP]->       Trip
  RoutePattern -[:STOPS_AT]->       Platform | BusStop  {stop_sequence, is_terminus}
  Trip       -[:FOLLOWS]->          RoutePattern
  Trip       -[:OPERATED_ON]->      ServicePattern
  Trip       -[:SCHEDULED_AT]->     Platform | BusStop  {mode, arrival_time, ...}
  ServicePattern -[:ACTIVE_ON]->    Date  {holiday_name?}
  (all nodes) -[:FROM_FEED]->       FeedInfo

Prerequisites:
  Physical layer must run first (Station, Platform, BusStop nodes).
"""


from typing import TYPE_CHECKING

from src.common.logger import get_logger
from src.layers.service_schedule import extract, transform

if TYPE_CHECKING:
    import pandas as pd

log = get_logger(__name__)


def run(gtfs_data: dict[str, pd.DataFrame], neo4j) -> None:
    """Execute the full service & schedule layer pipeline."""
    from src.layers.service_schedule import load

    log.info("=== Service & Schedule layer: starting ===")
    raw = extract.run(gtfs_data)
    result = transform.run(raw)
    load.run(result, neo4j)
    log.info("=== Service & Schedule layer: complete ===")
