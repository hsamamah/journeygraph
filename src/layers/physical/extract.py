# src/layers/physical/extract.py
"""
Physical layer — Extract

Pulls the four GTFS files required by the physical layer from the shared
gtfs_data dict produced by ingest/gtfs_loader.py.

Required files:
  stops             → Physical nodes
  pathways          → Pathway nodes
  levels            → Level nodes
  feed_info         → FeedInfo nodes
"""

import pandas as pd

from src.common.logger import get_logger

log = get_logger(__name__)

REQUIRED = {"stops", "pathways", "levels", "feed_info"}
OPTIONAL = set()


def run(gtfs_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Extract physical infrastructure-relevant DataFrames from the shared GTFS dataset.

    Returns a dict with keys matching REQUIRED (optional keys present only when data exists).
    All DataFrames are defensive copies.
    """
    missing = REQUIRED - gtfs_data.keys()
    if missing:
        raise KeyError(
            f"Physical layer extract failed — missing required GTFS file(s): {missing}"
        )

    extracted: dict[str, pd.DataFrame] = {
        key: gtfs_data[key].copy() for key in REQUIRED
    }

    for key, df in extracted.items():
        log.info("physical extract: %-25s %6d rows", key, len(df))

    return extracted
