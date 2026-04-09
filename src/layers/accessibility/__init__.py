# src/layers/accessibility/__init__.py
"""
Accessibility layer

Ingests current elevator and escalator outage data from the WMATA REST API
and loads it into Neo4j as :OutageEvent nodes linked to :Pathway nodes.

Data model:
  OutageEvent — one node per outage snapshot. composite_key = unit_name +
  date_out_of_service + date_updated, so re-polling an unchanged outage updates
  last_seen_poll in-place; a WMATA update (date_updated changes) creates a new
  snapshot node preserving full change history. Outages absent from the API
  response are resolved with resolved_at and actual_duration_days (Phase 4).

Severity (Integer, from symptom_description):
  Service Call / Inspection Repair / Minor Repair / Other → 2
  Major Repair                                            → 3
  Modernization                                           → 4

Cross-layer connections:
  OutageEvent -[:AFFECTS]-> Pathway  (resolved by pathway_joiner; physical layer prereq)
  Station context derived by graph traversal: Pathway → PathwayNode → Station

Prerequisites:
  Physical layer must be loaded (:Pathway nodes required for [:AFFECTS] links).

Usage:
  python -m src.pipeline --layers physical accessibility
  python -m src.pipeline --layers accessibility  (physical already loaded)
"""

from typing import TYPE_CHECKING

from src.common.logger import get_logger
from src.layers.accessibility import extract, transform

if TYPE_CHECKING:
    import pandas as pd

log = get_logger(__name__)


def run(_gtfs_data: dict[str, pd.DataFrame], neo4j, api_client) -> None:
    """Execute the full accessibility layer pipeline."""
    from src.layers.accessibility import load

    log.info("=== Accessibility layer: starting ===")
    raw = extract.run(api_client)
    result = transform.run(raw)
    load.run(result, neo4j)
    log.info("=== Accessibility layer: complete ===")
