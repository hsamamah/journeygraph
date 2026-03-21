# src/layers/physical/__init__.py
"""
Physical Infrastructure Layer

Orchestrates extract → transform → load for all physical infrastructure nodes
and relationships. Exposes a single run() entry point consumed by pipeline.py.

Nodes created:
  :Station, :StationEntrance, :Platform, :FareGate, :Pathway

Relationships created:
  Station      -[:CONTAINS]->        StationEntrance, Platform, FareGate
  Pathway      -[:LINKS]->           StationEntrance, Platform, FareGate
  Pathway      -[:ADJACENT_TO]->     Pathway
  FareGate     -[:BELONGS_TO]->      Station

Prerequisites:
  Physical layer must run before fare layer.
  Coordinate via pipeline.py --layers ordering:
    python pipeline.py --layers physical fare
"""

import pandas as pd

from src.common.logger import get_logger
from src.layers.physical import extract, transform

log = get_logger(__name__)


def run(gtfs_data: dict[str, pd.DataFrame], neo4j) -> None:
    """
    Execute the full physical infrastructure layer pipeline:
      1. Extract DataFrames from shared gtfs_data
      2. Transform and validate (pre-load gate)
      3. Load to Neo4j and validate (post-load gate)

    neo4j: Neo4jManager instance (injected by pipeline.py)
    """
    # Lazy import — keeps neo4j package optional for unit tests
    from src.layers.physical import load

    log.info("=== Physical layer: starting ===")
    raw = extract.run(gtfs_data)
    result = transform.run(raw)
    load.run(result, neo4j)
    log.info("=== Physical layer: complete ===")
