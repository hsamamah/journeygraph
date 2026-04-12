# src/layers/accessibility/extract.py
"""
Accessibility layer — Extract

Fetches current elevator and escalator outages from WMATA via WMATAClient
and wraps the raw JSON list into a pandas DataFrame.

Produces:
  outages — one row per ElevatorIncident from the WMATA REST API
            (raw CamelCase field names preserved; transform normalises them)
"""

from __future__ import annotations

import pandas as pd

from src.common.logger import get_logger

log = get_logger(__name__)

# Expected columns from the WMATA ElevatorIncidents endpoint.
# Used to guarantee a consistent empty frame when the API returns no data.
_OUTAGE_COLUMNS = [
    "UnitName",
    "UnitType",
    "UnitStatus",
    "StationCode",
    "StationName",
    "LocationDescription",
    "SymptomCode",
    "SymptomDescription",
    "TimeOutOfService",
    "TimeUpdated",
    "DisplayOrder",
    "DateOutOfService",
    "DateUpdated",
    "EstimatedReturnToService",
]


# ── Main entry point ─────────────────────────────────────────────────────────


def run(api_client) -> dict[str, pd.DataFrame]:
    log.info("accessibility extract: fetching ElevatorIncidents")

    raw: list[dict] = api_client.get_elevator_outages()

    outages = (
        pd.DataFrame(raw)
        if raw
        else pd.DataFrame(columns=_OUTAGE_COLUMNS)
    )

    log.info("accessibility extract: ElevatorIncidents %6d rows", len(outages))

    return {"outages": outages}
