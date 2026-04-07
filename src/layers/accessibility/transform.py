# src/layers/accessibility/transform.py
"""
Accessibility layer — Transform

Normalises the raw ElevatorIncidents DataFrame from the WMATA API:

  1. Renames CamelCase fields to snake_case; excluded API fields are dropped.
  2. Parses ISO-8601 date strings to epoch milliseconds (Neo4j datetime-compatible).
  3. Computes composite_key = unit_name + '|' + date_out_of_service + '|' + date_updated.
     Including date_updated ensures a new OutageEvent snapshot node is created
     whenever WMATA updates the outage record, preserving full change history.
  4. Derives severity (Integer) from symptom_description per schema §3:
       Service Call / Other / Inspection Repair / Minor Repair → 2
       Major Repair                                            → 3
       Modernization                                           → 4
  5. Computes projected_duration_days from estimated_return - date_out_of_service.
  6. Sets status = 'active' (all records from the API are current outages).
  7. Stamps first_seen_poll / last_seen_poll with the current poll ISO timestamp.

Excluded API fields (per schema §2): UnitStatus, StationName, SymptomCode,
TimeOutOfService, TimeUpdated, DisplayOrder — all are null, deprecated, or
derivable via graph traversal.

Performance:
  All operations are vectorised — no iterrows() or apply(axis=1).
  Date parsing uses pd.to_datetime with utc=True for safe epoch conversion.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from src.common.logger import get_logger

log = get_logger(__name__)

# ── Field rename map (retained fields only) ───────────────────────────────────
#
# Excluded per schema §2:
#   UnitStatus    — 100% null in feed (deprecated)
#   SymptomCode   — 100% null in feed (deprecated)
#   StationName   — redundant; derivable via Pathway → PathwayNode → Station
#   TimeOutOfService — redundant with time portion of DateOutOfService
#   TimeUpdated      — redundant with DateUpdated
#   DisplayOrder  — UI ordering artefact, no analytical value

_RENAME = {
    "UnitName": "unit_name",
    "UnitType": "unit_type",
    "StationCode": "station_code",
    "LocationDescription": "location_description",
    "SymptomDescription": "symptom_description",
    "DateOutOfService": "date_out_of_service",
    "DateUpdated": "date_updated",
    "EstimatedReturnToService": "estimated_return",
}

# ── Severity derivation ───────────────────────────────────────────────────────
#
# Integer scale per schema §3. Derived from symptom_description (repair scope),
# not unit_type (which describes the equipment, not the severity of the outage).
# Unknown / missing descriptions default to 2 (informational).

_SYMPTOM_SEVERITY: dict[str, int] = {
    "Service Call": 2,
    "Other": 2,
    "Inspection Repair": 2,
    "Minor Repair": 2,
    "Major Repair": 3,
    "Modernization": 4,
}

_MS_PER_DAY = 86_400_000  # milliseconds in one day


# ── Result container ──────────────────────────────────────────────────────────


@dataclass
class AccessibilityTransformResult:
    """Clean DataFrame ready for Neo4j ingestion."""

    outages: pd.DataFrame
    poll_timestamp: str  # ISO-8601 string shared across all rows in this poll
    stats: dict[str, int]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _parse_epoch_ms(series: pd.Series) -> pd.Series:
    """
    Parse ISO-8601 date strings (or UNKNOWN / None) to integer epoch milliseconds.
    Returns pd.NA for unparseable / missing values.
    Uses nullable Int64 dtype so downstream fillna(0).astype("int64") is safe.
    """
    cleaned = series.replace("UNKNOWN", pd.NA)
    parsed = pd.to_datetime(cleaned, errors="coerce", utc=True)
    # Use .dt accessor to convert tz-aware Series to epoch ms.
    # .values on a tz-aware Series returns an object array of Timestamps/NaT,
    # making the subsequent .astype("datetime64[ms]") version-dependent.
    # astype("datetime64[ms, UTC]") followed by view("int64") is the
    # documented pandas path and works correctly on both 1.x and 2.x.
    ms_series = parsed.astype("datetime64[ms, UTC]").view("int64")
    return ms_series.where(parsed.notna(), other=pd.NA).astype("Int64")


# ── Main entry point ──────────────────────────────────────────────────────────


def run(raw: dict[str, pd.DataFrame]) -> AccessibilityTransformResult:
    """Normalise raw ElevatorIncidents into OutageEvent-ready DataFrame."""
    log.info("accessibility transform: starting")

    outages = raw["outages"].copy()

    poll_timestamp = datetime.now(timezone.utc).isoformat()

    if outages.empty:
        log.warning("accessibility transform: no outage rows — returning empty result")
        return AccessibilityTransformResult(
            outages=outages,
            poll_timestamp=poll_timestamp,
            stats={"outages": 0},
        )

    # ── Step 1: rename and drop excluded columns ──────────────────────────────

    outages = outages.rename(columns=_RENAME)
    # Keep only the renamed (retained) columns; drop everything else
    outages = outages[[c for c in _RENAME.values() if c in outages.columns]]

    # ── Step 2: parse dates to epoch milliseconds ─────────────────────────────

    for col in ("date_out_of_service", "date_updated", "estimated_return"):
        if col in outages.columns:
            outages[col] = _parse_epoch_ms(outages[col])

    # ── Step 3: composite_key ─────────────────────────────────────────────────
    #
    # composite_key = unit_name + '|' + date_out_of_service + '|' + date_updated
    #
    # Including date_updated means MERGE creates a new OutageEvent node whenever
    # WMATA updates the record (symptom escalation, ETA change, etc.), preserving
    # full snapshot history per schema §6.1.
    #
    # The (unit_name, date_out_of_service) pair logically identifies the physical
    # outage instance across all its snapshots (schema §6.3); the accessibility_
    # outage_identity index on those two fields supports per-unit history queries.

    dos_str = outages["date_out_of_service"].fillna(0).astype("int64").astype(str) if "date_out_of_service" in outages.columns else pd.Series("0", index=outages.index)
    du_str  = outages["date_updated"].fillna(0).astype("int64").astype(str) if "date_updated" in outages.columns else pd.Series("0", index=outages.index)
    outages["composite_key"] = outages["unit_name"].astype(str) + "|" + dos_str + "|" + du_str

    # ── Step 4: severity (Integer from symptom_description) ──────────────────

    outages["severity"] = (
        outages["symptom_description"].map(_SYMPTOM_SEVERITY).fillna(2).astype(int)
    )

    # ── Step 5: projected_duration_days ──────────────────────────────────────
    #
    # Derived from estimated_return - date_out_of_service (both epoch ms).
    # Null when estimated_return is absent. Rounded down to whole days.

    if "estimated_return" in outages.columns and "date_out_of_service" in outages.columns:
        both_present = outages["estimated_return"].notna() & outages["date_out_of_service"].notna()
        duration_ms = (
            outages["estimated_return"].astype("Int64")
            - outages["date_out_of_service"].astype("Int64")
        )
        outages["projected_duration_days"] = (
            (duration_ms // _MS_PER_DAY).where(both_present, other=None)
        )
    else:
        outages["projected_duration_days"] = None

    # ── Step 6: status ────────────────────────────────────────────────────────

    outages["status"] = "active"

    # ── Step 7: poll timestamps ───────────────────────────────────────────────

    outages["poll_timestamp"] = poll_timestamp

    # ── Dedup on composite_key ────────────────────────────────────────────────

    before = len(outages)
    outages = outages.drop_duplicates(subset=["composite_key"])
    if len(outages) < before:
        log.info(
            "accessibility transform: dedup %d → %d on composite_key",
            before,
            len(outages),
        )

    # ── Stats ─────────────────────────────────────────────────────────────────

    stats = {"outages": len(outages)}
    if "unit_type" in outages.columns:
        for utype, count in outages["unit_type"].value_counts().items():
            log.info("accessibility transform:   unit_type=%-12s %d", utype, count)
    if "severity" in outages.columns:
        for sev, count in outages["severity"].value_counts().sort_index().items():
            log.info("accessibility transform:   severity=%-3s %d", sev, count)
    log.info("accessibility transform: outages %6d rows", len(outages))

    log.info("accessibility transform: complete")

    return AccessibilityTransformResult(
        outages=outages,
        poll_timestamp=poll_timestamp,
        stats=stats,
    )
