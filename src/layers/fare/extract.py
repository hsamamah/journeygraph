# src/layers/fare/extract.py
"""
Fare layer — Extract

Pulls the five GTFS files required by the fare layer from the shared
gtfs_data dict produced by ingest/gtfs_loader.py.

Required files:
  stops             → FareZone nodes + IN_ZONE relationships
  fare_media        → FareMedia nodes
  fare_products     → FareProduct nodes (deduplicated to 5 logical nodes)
  fare_leg_rules    → FareLegRule nodes + FROM_AREA / TO_AREA / APPLIES_PRODUCT
  fare_transfer_rules → FareTransferRule nodes

Optional (gracefully absent):
  fare_transfer_rules — WMATA may not publish this file in all feed versions
"""

from typing import TYPE_CHECKING

from src.common.logger import get_logger

if TYPE_CHECKING:
    import pandas as pd

log = get_logger(__name__)

REQUIRED = {"stops", "fare_media", "fare_products", "fare_leg_rules", "feed_info"}
OPTIONAL = {"fare_transfer_rules"}


def run(gtfs_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Extract fare-relevant DataFrames from the shared GTFS dataset.

    Returns a dict with keys matching REQUIRED ∪ OPTIONAL (optional keys
    present only when data exists). All DataFrames are defensive copies.
    """
    missing = REQUIRED - gtfs_data.keys()
    if missing:
        raise KeyError(
            f"Fare layer extract failed — missing required GTFS file(s): {missing}"
        )

    extracted: dict[str, pd.DataFrame] = {
        key: gtfs_data[key].copy() for key in REQUIRED
    }

    for key in OPTIONAL:
        if key in gtfs_data:
            extracted[key] = gtfs_data[key].copy()
            log.info(
                "fare extract: optional file '%s' found (%d rows)",
                key,
                len(gtfs_data[key]),
            )
        else:
            log.warning("fare extract: optional file '%s' not present in feed", key)

    for key, df in extracted.items():
        log.info("fare extract: %-25s %6d rows", key, len(df))

    return extracted
