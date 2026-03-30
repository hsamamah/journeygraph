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
from src.common.validators.service_schedule import validate_pre_transform
from src.layers.service_schedule import extract, transform

if TYPE_CHECKING:
    import pandas as pd

log = get_logger(__name__)


def run(gtfs_data: dict[str, pd.DataFrame], neo4j) -> None:
    """Execute the full service & schedule layer pipeline."""
    from src.layers.service_schedule import load

    log.info("=== Service & Schedule layer: starting ===")
    raw = extract.run(gtfs_data)

    # ── Pre-transform validation: checks raw GTFS before any logic runs ───────
    log.info("service: running pre-transform validation")
    fi = raw["feed_info"].iloc[0]
    validation = validate_pre_transform(
        trips=raw["trips"],
        stop_times=raw["stop_times"],
        stops=raw["stops"],
        calendar=raw["calendar"],
        calendar_dates=raw.get("calendar_dates"),
        feed_start=str(fi.get("feed_start_date", "")),
        feed_end=str(fi.get("feed_end_date", "")),
    )
    log.info("service: pre-transform validation result:\n%s", validation.summary())
    if not validation.passed:
        raise ValueError(
            f"Service layer pre-transform validation failed — aborting pipeline:\n"
            f"{validation.summary()}"
        )

    result = transform.run(raw)
    load.run(result, neo4j)
    log.info("=== Service & Schedule layer: complete ===")
