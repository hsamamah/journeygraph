"""
layers/fare/__init__.py

Each layer exposes a single run(gtfs_data) function.
pipeline.py calls this — it doesn't need to know the internals.
"""

import pandas as pd
from src.common.logger import get_logger

logger = get_logger(__name__)


def run(gtfs_data: dict[str, pd.DataFrame]):
    """Entry point called by pipeline.py."""
    from src.layers.fare import extract, transform, load

    logger.info("Fare layer — extract")
    raw = extract.run(gtfs_data)

    logger.info("Fare layer — transform")
    transformed = transform.run(raw)

    logger.info("Fare layer — load")
    load.run(transformed)
