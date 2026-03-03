"""
layers/fare/transform.py

Shapes raw fare DataFrames into graph-ready dicts for Neo4j loading.
"""

import pandas as pd
from src.common.logger import get_logger

logger = get_logger(__name__)


def run(raw: dict[str, pd.DataFrame]) -> dict:
    """
    Transform raw fare data into graph-ready node and relationship dicts.

    Args:
        raw: Output of fare extract.run()

    Returns:
        Dict with keys: fare_media, fare_products, fare_leg_rules,
                        fare_transfer_rules — each a list of dicts.
    """
    raise NotImplementedError("Fare transform not yet implemented.")
