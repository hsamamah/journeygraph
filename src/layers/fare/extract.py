"""
layers/fare/extract.py

Pulls the GTFS tables relevant to the Fare Layer from the shared gtfs_data dict.
No file I/O happens here — gtfs_loader.py already handled that.
"""

import pandas as pd
from src.common.logger import get_logger

logger = get_logger(__name__)

# Keys this layer needs from the gtfs_data dict
REQUIRED = [
    "fare_leg_rules",
    "fare_products",
    "fare_media",
    "fare_transfer_rules",
    "stops",
]
OPTIONAL = ["feed_info"]


def run(gtfs_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Extract fare-relevant DataFrames from the shared gtfs_data dict.

    Args:
        gtfs_data: Full GTFS data dict from gtfs_loader.load()

    Returns:
        Subset dict containing only fare-relevant tables.
    """
    result = {}

    for key in REQUIRED:
        if key not in gtfs_data:
            raise KeyError(
                f"Fare layer extract: required GTFS table '{key}' is missing. "
                f"Check gtfs_loader.GTFS_FILES."
            )
        result[key] = gtfs_data[key]
        logger.info(f"  Extracted '{key}' — {len(result[key]):,} rows")

    for key in OPTIONAL:
        if key in gtfs_data:
            result[key] = gtfs_data[key]

    return result
