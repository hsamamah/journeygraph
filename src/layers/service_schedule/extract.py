# src/layers/service_schedule/extract.py
"""
Service & Schedule layer — Extract

Pulls GTFS files required by this layer from the shared gtfs_data dict.

Required files:
  agency          → Agency node
  routes          → Route nodes + mode classification
  trips           → Trip nodes + RoutePattern derivation
  stop_times      → SCHEDULED_AT / STOPS_AT relationships
  calendar        → ServicePattern nodes + ACTIVE_ON
  stops           → parent_station lookup for Route SERVES derivation
  feed_info       → FeedInfo node

Optional (gracefully absent):
  calendar_dates  → exceptions applied on top of calendar
"""

from typing import TYPE_CHECKING

from src.common.logger import get_logger

if TYPE_CHECKING:
    import pandas as pd

log = get_logger(__name__)

REQUIRED = {"agency", "routes", "trips", "stop_times", "calendar", "stops", "feed_info"}
OPTIONAL = {"calendar_dates"}


def run(gtfs_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Extract service-relevant DataFrames from the shared GTFS dataset.

    Returns a dict with keys matching REQUIRED ∪ OPTIONAL (optional keys
    present only when data exists). All DataFrames are defensive copies.
    """
    missing = REQUIRED - gtfs_data.keys()
    if missing:
        raise KeyError(
            f"Service layer extract failed — missing required GTFS file(s): {missing}"
        )

    extracted: dict[str, pd.DataFrame] = {
        key: gtfs_data[key].copy() for key in REQUIRED
    }

    for key in OPTIONAL:
        if key in gtfs_data:
            extracted[key] = gtfs_data[key].copy()
            log.info(
                "service extract: optional file '%s' found (%d rows)",
                key,
                len(gtfs_data[key]),
            )
        else:
            log.warning("service extract: optional file '%s' not present in feed", key)

    for key, df in extracted.items():
        log.info("service extract: %-25s %6d rows", key, len(df))

    return extracted
