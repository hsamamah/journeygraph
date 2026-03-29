# src/layers/fare/__init__.py
"""
Fare layer

Orchestrates extract → transform → load for all fare-related nodes
and relationships. Exposes a single run() entry point consumed by pipeline.py.

Nodes created:
  :FareZone, :FareMedia, :FareProduct, :FareLegRule, :FareTransferRule

Relationships created:
  Station      -[:IN_ZONE]->        FareZone
  FareGate     -[:IN_ZONE]->        FareZone
  FareGate     -[:BELONGS_TO]->     Station
  FareMedia    -[:ACCEPTS]->        FareProduct
  FareProduct  -[:ACCEPTED_VIA]->   FareMedia
  FareLegRule  -[:FROM_AREA]->      FareZone   (rail only)
  FareLegRule  -[:TO_AREA]->        FareZone   (rail only)
  FareLegRule  -[:APPLIES_PRODUCT]->FareProduct
  FareTransferRule -[:FROM_LEG]->   FareLegRule
  FareTransferRule -[:TO_LEG]->     FareLegRule
  FareTransferRule -[:APPLIES_PRODUCT]->FareProduct  (non-free only)

Prerequisites:
  Physical layer must run before fare layer.
  Station and FareGate nodes must exist before IN_ZONE / BELONGS_TO are wired.
  Coordinate via pipeline.py --layers ordering:
    python pipeline.py --layers physical fare
"""

from typing import TYPE_CHECKING

from src.common.logger import get_logger
from src.layers.fare import extract, transform

if TYPE_CHECKING:
    import pandas as pd

log = get_logger(__name__)


def run(gtfs_data: dict[str, pd.DataFrame], neo4j) -> None:
    """
    Execute the full fare layer pipeline:
      1. Extract fare DataFrames from shared gtfs_data
      2. Transform and validate (pre-load gate)
      3. Load to Neo4j and validate (post-load gate)

    neo4j: Neo4jManager instance (injected by pipeline.py)
    """
    # Lazy import — keeps neo4j package optional for unit tests
    from src.layers.fare import load

    log.info("=== Fare layer: starting ===")
    raw = extract.run(gtfs_data)
    result = transform.run(raw)
    load.run(result, neo4j)
    log.info("=== Fare layer: complete ===")
