# src/layers/physical/__init__.py
"""
Physical Infrastructure Layer

Orchestrates extract → transform → load for all physical infrastructure nodes
and relationships. Exposes a single run() entry point consumed by pipeline.py.

Nodes created:
  :Station, :StationEntrance, :Platform, :FareGate, :BusStop, :Pathway, :Level

Relationships created (physical layer only):
  Station -[:CONTAINS]-> StationEntrance
  Station -[:CONTAINS]-> Platform
  Station -[:CONTAINS]-> FareGate
  (stop entity) -[:LINKS]-> Pathway   ← from_stop direction, all 5 entity types
  Pathway -[:LINKS]-> (stop entity)   ← to_stop direction, all 5 entity types
  Pathway -[:LINKS]-> Pathway         ← chain via deferred GTFS generic node pivots

Not built here (owned by other layers):
  FareGate -[:BELONGS_TO]-> Station   ← built by fare layer

Prerequisites:
  Physical layer must run before fare layer.
  Coordinate via pipeline.py --layers ordering:
    python pipeline.py --layers physical fare
"""

import pandas as pd

from src.common.logger import get_logger
from src.common.validators.physical import validate_pre_transform
from src.layers.physical import extract, transform

log = get_logger(__name__)


def run(gtfs_data: dict[str, pd.DataFrame], neo4j) -> None:
    """
    Execute the full physical infrastructure layer pipeline:
      1. Extract DataFrames from shared gtfs_data
      2. Validate raw GTFS source data (pre-transform gate)
      3. Transform into Neo4j-ready DataFrames
      4. Load to Neo4j and validate (post-load gate)

    neo4j: Neo4jManager instance (injected by pipeline.py)
    """
    # Lazy import — keeps neo4j package optional for unit tests
    from src.layers.physical import load

    log.info("=== Physical layer: starting ===")
    raw = extract.run(gtfs_data)

    # ── Pre-transform validation: checks raw GTFS before any logic runs ───────
    log.info("physical: running pre-transform validation")
    validation = validate_pre_transform(stops=raw["stops"], pathways=raw["pathways"])
    log.info("physical: pre-transform validation result:\n%s", validation.summary())
    if not validation.passed:
        raise ValueError(
            f"Physical layer pre-transform validation failed — aborting pipeline:\n"
            f"{validation.summary()}"
        )

    result = transform.run(raw)
    load.run(result, neo4j)
    log.info("=== Physical layer: complete ===")
