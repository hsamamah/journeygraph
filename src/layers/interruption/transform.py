# src/layers/interruption/transform.py
"""
Interruption layer — Transform

Applies deduplication and transform rules to produce the three-tier
disruption model from raw GTFS-RT DataFrames.

Deduplication:
  TripUpdates — hash of (trip_id, start_date, schedule_relationship,
                delay, stop-level states). MERGE on hash in Neo4j ensures
                repeated polls with same state don't create duplicates.
  ServiceAlerts — deduplicate on feed_entity_id.

Transform rules (v3 schema Section 3):
  Rule 1: CANCELED TripUpdate → :Interruption:Cancellation
  Rule 2: delay >= 300s       → :Interruption:Delay
  Rule 3: SKIPPED stop        → :Interruption:Skip
  Rule 4: ServiceAlert effect → mapped Interruption type
  Rule 5: Correlation rollup  — deferred to post-load enrichment
  Rule 6: Maintenance overlap — deferred to post-load enrichment

Severity:
  CANCELED → SEVERE
  delay 300–899s → WARNING, 900+ → SEVERE
  SKIPPED → WARNING
  ServiceAlert → from alert severity_level

Performance:
  All transform operations are fully vectorised using pandas boolean
  masking, groupby, merge, and string operations. No iterrows() or
  apply(axis=1) with row.to_dict() anywhere in the hot path.

  Hash computation: stop states are pre-aggregated into a single string
  per entity_id using groupby + join (C-level), then merged to trip_updates.
  The final sha256 call uses apply on a single pre-built string column —
  unavoidable since hashlib is not vectorisable, but the Python overhead
  is minimal compared to the previous per-row dict construction.

  With n=2,205 TripUpdates and m=27,652 StopTimeUpdates:
    _build_stu_index + apply(row.to_dict):  ~60M operations  (v1)
    O(1) dict lookup in apply:              ~30K operations  (v2)
    Fully vectorised groupby + merge:       ~C-speed          (v3, this file)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib

import pandas as pd

from src.common.logger import get_logger

log = get_logger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

DELAY_THRESHOLD = 300  # seconds (5 minutes) — Rule 2
SEVERE_DELAY = 900  # seconds (15 minutes)

# ServiceAlert effect → Interruption type mapping (Rule 4)
# OTHER_EFFECT mapped to service_change — WMATA uses it for miscellaneous
# service modifications that don't fit named categories.
# See CONVENTIONS.md → "ServiceAlert Effect Mapping"
EFFECT_TYPE_MAP = {
    "NO_SERVICE": "cancellation",
    "REDUCED_SERVICE": "service_change",
    "SIGNIFICANT_DELAYS": "delay",
    "DETOUR": "detour",
    "MODIFIED_SERVICE": "service_change",
    "STOP_MOVED": "service_change",
    "ACCESSIBILITY_ISSUE": "accessibility",
    "OTHER_EFFECT": "service_change",
}

# Interruption type → Neo4j multi-label
TYPE_LABEL_MAP = {
    "cancellation": "Cancellation",
    "delay": "Delay",
    "skip": "Skip",
    "detour": "Detour",
    "service_change": "ServiceChange",
    "accessibility": "Accessibility",
}


# ── Result container ─────────────────────────────────────────────────────────


@dataclass
class InterruptionTransformResult:
    """Clean DataFrames ready for Neo4j ingestion."""

    # Tier 1 — Raw source nodes
    trip_updates: pd.DataFrame  # with dedup_hash added
    stop_time_updates: pd.DataFrame
    service_alerts: pd.DataFrame
    entity_selectors: pd.DataFrame

    # Tier 2 — Normalized Interruption nodes
    interruptions: pd.DataFrame  # interruption_id, type, label, cause, effect,
    # severity, start_time, description, date

    # Tier 2→1 links
    interruption_sources: pd.DataFrame  # interruption_id, source_entity_id, source_type

    # Tier 2→Service layer links (AFFECTS_*)
    affects_trip: pd.DataFrame  # interruption_id, trip_id
    affects_route: pd.DataFrame  # interruption_id, route_id
    affects_stop: pd.DataFrame  # interruption_id, stop_id

    # Feed info (passed through)
    feed_info: pd.DataFrame | None

    # Metadata
    stats: dict[str, int] = field(default_factory=dict)


# ── Dedup hash ───────────────────────────────────────────────────────────────


def _compute_dedup_hashes(
    trip_updates: pd.DataFrame,
    stop_time_updates: pd.DataFrame,
) -> pd.Series:
    """
    Compute deduplication hashes for all TripUpdates in one vectorised pass.

    Strategy:
      1. Build a stop-state string per entity_id using groupby + apply on
         stop_time_updates (C-level grouping, single pass over m rows).
      2. Merge that string column onto trip_updates (one join).
      3. Concatenate trip-level fields + stop-state string into a single
         raw_hash column using vectorised string ops.
      4. Apply sha256 once per row on the pre-built string — unavoidable
         since hashlib is not vectorisable, but the Python work per row is
         now a single encode() + hexdigest() call instead of row.to_dict()
         + dict construction + inner loop.

    Returns a Series of 16-char hex strings aligned to trip_updates.index.
    """
    # Step 1: aggregate stop states per entity_id — O(m), C-level groupby
    if not stop_time_updates.empty:
        stu = stop_time_updates.sort_values(
            ["parent_entity_id", "stop_sequence"], na_position="last"
        ).copy()
        stu["_stop_state"] = (
            stu["stop_sequence"].fillna("").astype(str)
            + ":"
            + stu["schedule_relationship"].fillna("").astype(str)
            + ":"
            + stu["arrival_delay"].fillna("").astype(str)
            + ":"
            + stu["departure_delay"].fillna("").astype(str)
        )
        stop_strings = (
            stu.groupby("parent_entity_id")["_stop_state"]
            .apply("|".join)
            .reset_index()
            .rename(
                columns={
                    "parent_entity_id": "feed_entity_id",
                    "_stop_state": "_stop_states",
                }
            )
        )
    else:
        stop_strings = pd.DataFrame(columns=["feed_entity_id", "_stop_states"])

    # Step 2: merge stop state strings onto trip_updates — one join
    tu = trip_updates.copy()
    tu = tu.merge(stop_strings, on="feed_entity_id", how="left")
    tu["_stop_states"] = tu["_stop_states"].fillna("")

    # Step 3: build raw hash string — vectorised string concatenation
    # fillna("") before astype(str) prevents NaN columns from producing
    # float NaN in the concatenated string, which would cause encode() to fail.
    tu["_raw"] = (
        tu["trip_id"].fillna("").astype(str)
        + "|"
        + tu["start_date"].fillna("").astype(str)
        + "|"
        + tu["schedule_relationship"].fillna("").astype(str)
        + "|"
        + tu["delay"].fillna("").astype(str)
        + "|"
        + tu["_stop_states"]
    ).fillna("")  # guard: left merge can produce NaN if feed_entity_id unmatched

    # Step 4: hash — apply on single pre-built string column
    # str(s) is a belt-and-suspenders guard against any residual non-str values
    hashes = tu["_raw"].apply(
        lambda s: hashlib.sha256(str(s).encode()).hexdigest()[:16]
    )
    # Realign to original index (merge may have changed it)
    hashes.index = trip_updates.index
    return hashes


# ── Transform rules ──────────────────────────────────────────────────────────


def _apply_trip_update_rules(
    trip_updates: pd.DataFrame,
    stop_time_updates: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Apply Rules 1, 2, 3 — fully vectorised, no iterrows().

    Rules 1 + 2: boolean mask on schedule_relationship / delay column,
    then DataFrame construction via assign() and column selection.

    Rule 3: filter SKIPPED rows, merge parent TripUpdate context in one
    join, then build output DataFrames from the merged result.

    Returns:
      interruptions, sources, affects_trip, affects_route
    """
    int_parts: list[pd.DataFrame] = []
    src_parts: list[pd.DataFrame] = []
    trip_parts: list[pd.DataFrame] = []
    route_parts: list[pd.DataFrame] = []

    # ── Rule 1: CANCELED → Cancellation ──────────────────────────────────────

    canceled = trip_updates[trip_updates["schedule_relationship"] == "CANCELED"].copy()

    if not canceled.empty:
        canceled["interruption_id"] = "int_tu_" + canceled["feed_entity_id"]
        canceled["interruption_type"] = "cancellation"
        canceled["label"] = "Cancellation"
        canceled["cause"] = None
        canceled["effect"] = "NO_SERVICE"
        canceled["severity"] = "SEVERE"
        canceled["end_time"] = None
        canceled["description"] = (
            "Trip " + canceled["trip_id"].astype(str) + " cancelled"
        )

        int_parts.append(
            canceled[
                [
                    "interruption_id",
                    "interruption_type",
                    "label",
                    "cause",
                    "effect",
                    "severity",
                    "timestamp",
                    "end_time",
                    "description",
                    "start_date",
                ]
            ].rename(columns={"timestamp": "start_time", "start_date": "date"})
        )

        src_parts.append(
            pd.DataFrame(
                {
                    "interruption_id": canceled["interruption_id"].values,
                    "source_entity_id": canceled["feed_entity_id"].values,
                    "source_type": "TripUpdate",
                }
            )
        )

        with_trip = canceled[canceled["trip_id"].notna()]
        if not with_trip.empty:
            trip_parts.append(with_trip[["interruption_id", "trip_id"]])

        with_route = canceled[canceled["route_id"].notna()]
        if not with_route.empty:
            route_parts.append(with_route[["interruption_id", "route_id"]])

    # ── Rule 2: delay >= threshold → Delay ───────────────────────────────────

    delayed = trip_updates[
        trip_updates["delay"].notna()
        & (trip_updates["delay"] >= DELAY_THRESHOLD)
        & (trip_updates["schedule_relationship"] != "CANCELED")
    ].copy()

    if not delayed.empty:
        delayed["interruption_id"] = "int_tu_" + delayed["feed_entity_id"]
        delayed["interruption_type"] = "delay"
        delayed["label"] = "Delay"
        delayed["cause"] = None
        delayed["effect"] = "SIGNIFICANT_DELAYS"
        delayed["severity"] = delayed["delay"].apply(
            lambda d: "SEVERE" if d >= SEVERE_DELAY else "WARNING"
        )
        delayed["end_time"] = None
        delayed["description"] = (
            "Trip "
            + delayed["trip_id"].astype(str)
            + " delayed "
            + delayed["delay"].astype(str)
            + "s"
        )

        int_parts.append(
            delayed[
                [
                    "interruption_id",
                    "interruption_type",
                    "label",
                    "cause",
                    "effect",
                    "severity",
                    "timestamp",
                    "end_time",
                    "description",
                    "start_date",
                ]
            ].rename(columns={"timestamp": "start_time", "start_date": "date"})
        )

        src_parts.append(
            pd.DataFrame(
                {
                    "interruption_id": delayed["interruption_id"].values,
                    "source_entity_id": delayed["feed_entity_id"].values,
                    "source_type": "TripUpdate",
                }
            )
        )

        with_trip = delayed[delayed["trip_id"].notna()]
        if not with_trip.empty:
            trip_parts.append(with_trip[["interruption_id", "trip_id"]])

        with_route = delayed[delayed["route_id"].notna()]
        if not with_route.empty:
            route_parts.append(with_route[["interruption_id", "route_id"]])

    # ── Rule 3: SKIPPED stops → Skip ─────────────────────────────────────────
    #
    # Filter SKIPPED rows, then merge parent TripUpdate context in one join
    # instead of a per-row lookup.

    if not stop_time_updates.empty:
        skipped = stop_time_updates[
            stop_time_updates["schedule_relationship"] == "SKIPPED"
        ].copy()

        if not skipped.empty:
            # Join parent context in one merge — no iterrows()
            parent_cols = trip_updates[
                ["feed_entity_id", "start_date", "timestamp", "trip_id", "route_id"]
            ].rename(columns={"feed_entity_id": "parent_entity_id"})

            skipped = skipped.merge(parent_cols, on="parent_entity_id", how="left")

            skipped["interruption_id"] = (
                "int_skip_"
                + skipped["parent_entity_id"].astype(str)
                + "_"
                + skipped["stop_sequence"].astype(str)
            )
            skipped["interruption_type"] = "skip"
            skipped["label"] = "Skip"
            skipped["cause"] = None
            skipped["effect"] = "STOP_MOVED"
            skipped["severity"] = "WARNING"
            skipped["end_time"] = None
            skipped["description"] = (
                "Stop "
                + skipped["stop_id"].astype(str)
                + " skipped (seq "
                + skipped["stop_sequence"].astype(str)
                + ")"
            )

            int_parts.append(
                skipped[
                    [
                        "interruption_id",
                        "interruption_type",
                        "label",
                        "cause",
                        "effect",
                        "severity",
                        "timestamp",
                        "end_time",
                        "description",
                        "start_date",
                    ]
                ].rename(columns={"timestamp": "start_time", "start_date": "date"})
            )

            src_parts.append(
                pd.DataFrame(
                    {
                        "interruption_id": skipped["interruption_id"].values,
                        "source_entity_id": skipped["parent_entity_id"].values,
                        "source_type": "TripUpdate",
                    }
                )
            )

            with_trip = skipped[skipped["trip_id"].notna()]
            if not with_trip.empty:
                trip_parts.append(with_trip[["interruption_id", "trip_id"]])

            with_route = skipped[skipped["route_id"].notna()]
            if not with_route.empty:
                route_parts.append(with_route[["interruption_id", "route_id"]])

    # ── Assemble outputs ──────────────────────────────────────────────────────

    interruptions = (
        pd.concat(int_parts, ignore_index=True) if int_parts else _empty_interruptions()
    )
    sources = pd.concat(src_parts, ignore_index=True) if src_parts else _empty_sources()
    affects_trip = (
        pd.concat(trip_parts, ignore_index=True).drop_duplicates()
        if trip_parts
        else pd.DataFrame(columns=["interruption_id", "trip_id"])
    )
    affects_route = (
        pd.concat(route_parts, ignore_index=True).drop_duplicates()
        if route_parts
        else pd.DataFrame(columns=["interruption_id", "route_id"])
    )

    return interruptions, sources, affects_trip, affects_route


def _apply_alert_rules(
    service_alerts: pd.DataFrame,
    entity_selectors: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Apply Rule 4 — fully vectorised, no iterrows().

    Filters alerts by mapped effects, builds Interruption rows via column
    assign, then joins EntitySelectors in one merge to derive AFFECTS_*.

    Returns:
      interruptions, sources, affects_trip, affects_route, affects_stop
    """
    # Filter to mapped effects only
    mapped = service_alerts[service_alerts["effect"].isin(EFFECT_TYPE_MAP)].copy()

    if mapped.empty:
        return (
            _empty_interruptions(),
            _empty_sources(),
            pd.DataFrame(columns=["interruption_id", "trip_id"]),
            pd.DataFrame(columns=["interruption_id", "route_id"]),
            pd.DataFrame(columns=["interruption_id", "stop_id"]),
        )

    # Build Interruption rows — vectorised
    mapped["interruption_id"] = "int_sa_" + mapped["feed_entity_id"]
    mapped["interruption_type"] = mapped["effect"].map(EFFECT_TYPE_MAP)
    mapped["label"] = (
        mapped["interruption_type"].map(TYPE_LABEL_MAP).fillna("ServiceChange")
    )
    mapped["end_time"] = mapped["active_period_end"]
    mapped["start_time"] = mapped["active_period_start"]
    mapped["description"] = mapped["header_text"]
    mapped["severity"] = mapped["severity_level"].fillna("UNKNOWN_SEVERITY")

    # Derive date from active_period_start (epoch → YYYYMMDD) — vectorised
    def _epoch_to_date(series: pd.Series) -> pd.Series:
        def _convert(v):
            if pd.isna(v):
                return None
            try:
                return datetime.fromtimestamp(int(v)).strftime("%Y%m%d")
            except ValueError, TypeError, OSError:
                return None

        return series.apply(_convert)

    mapped["date"] = _epoch_to_date(mapped["active_period_start"])

    interruptions = mapped[
        [
            "interruption_id",
            "interruption_type",
            "label",
            "cause",
            "effect",
            "severity",
            "start_time",
            "end_time",
            "description",
            "date",
        ]
    ].copy()

    sources = pd.DataFrame(
        {
            "interruption_id": mapped["interruption_id"].values,
            "source_entity_id": mapped["feed_entity_id"].values,
            "source_type": "ServiceAlert",
        }
    )

    # Derive AFFECTS_* from EntitySelectors — one merge instead of per-alert loop
    if entity_selectors.empty:
        return (
            interruptions,
            sources,
            pd.DataFrame(columns=["interruption_id", "trip_id"]),
            pd.DataFrame(columns=["interruption_id", "route_id"]),
            pd.DataFrame(columns=["interruption_id", "stop_id"]),
        )

    # Join selectors to mapped alerts
    id_map = mapped[["feed_entity_id", "interruption_id"]]
    sel = entity_selectors.merge(
        id_map.rename(columns={"feed_entity_id": "parent_entity_id"}),
        on="parent_entity_id",
        how="inner",
    )

    affects_trip = (
        sel[sel["trip_id"].notna()][["interruption_id", "trip_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    affects_route = (
        sel[sel["route_id"].notna()][["interruption_id", "route_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    affects_stop = (
        sel[sel["stop_id"].notna()][["interruption_id", "stop_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return interruptions, sources, affects_trip, affects_route, affects_stop


# ── Empty frame helpers ───────────────────────────────────────────────────────


def _empty_interruptions() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "interruption_id",
            "interruption_type",
            "label",
            "cause",
            "effect",
            "severity",
            "start_time",
            "end_time",
            "description",
            "date",
        ]
    )


def _empty_sources() -> pd.DataFrame:
    return pd.DataFrame(columns=["interruption_id", "source_entity_id", "source_type"])


# ── Main entry point ─────────────────────────────────────────────────────────


def run(raw: dict[str, pd.DataFrame]) -> InterruptionTransformResult:
    """
    Transform raw GTFS-RT DataFrames into the three-tier disruption model.
    """
    log.info("interruption transform: starting")

    trip_updates = raw["trip_updates"]
    stop_time_updates = raw["stop_time_updates"]
    service_alerts = raw["service_alerts"]
    entity_selectors = raw["entity_selectors"]
    feed_info = raw.get("feed_info")

    # ── Dedup hashes on TripUpdates ──────────────────────────────────────────
    #
    # Fully vectorised: stop states aggregated via groupby (C-level),
    # merged onto trip_updates in one join, hash string built via vectorised
    # string ops. sha256 apply runs on a single pre-built string column.

    if not trip_updates.empty:
        trip_updates = trip_updates.copy()
        trip_updates["dedup_hash"] = _compute_dedup_hashes(
            trip_updates, stop_time_updates
        )
        before = len(trip_updates)
        trip_updates = trip_updates.drop_duplicates(subset=["dedup_hash"])
        log.info(
            "interruption transform: TripUpdate dedup %d → %d",
            before,
            len(trip_updates),
        )

    # ── ServiceAlert dedup on feed_entity_id ─────────────────────────────────

    if not service_alerts.empty:
        before = len(service_alerts)
        service_alerts = service_alerts.drop_duplicates(subset=["feed_entity_id"])
        log.info(
            "interruption transform: ServiceAlert dedup %d → %d",
            before,
            len(service_alerts),
        )

    # ── Apply transform rules ────────────────────────────────────────────────

    tu_ints, tu_srcs, tu_trips, tu_routes = _apply_trip_update_rules(
        trip_updates, stop_time_updates
    )
    sa_ints, sa_srcs, sa_trips, sa_routes, sa_stops = _apply_alert_rules(
        service_alerts, entity_selectors
    )

    # Combine Tier 2 results
    interruptions = pd.concat([tu_ints, sa_ints], ignore_index=True)
    sources = pd.concat([tu_srcs, sa_srcs], ignore_index=True)
    affects_trip = pd.concat([tu_trips, sa_trips], ignore_index=True).drop_duplicates()
    affects_route = pd.concat(
        [tu_routes, sa_routes], ignore_index=True
    ).drop_duplicates()
    affects_stop = sa_stops.drop_duplicates() if not sa_stops.empty else sa_stops

    # ── Stats ────────────────────────────────────────────────────────────────

    stats = {
        "trip_updates": len(trip_updates),
        "stop_time_updates": len(stop_time_updates),
        "service_alerts": len(service_alerts),
        "entity_selectors": len(entity_selectors),
        "interruptions": len(interruptions),
        "interruption_sources": len(sources),
        "affects_trip": len(affects_trip),
        "affects_route": len(affects_route),
        "affects_stop": len(affects_stop),
    }
    for k, v in stats.items():
        log.info("interruption transform: %-25s %6d rows", k, v)

    if not interruptions.empty:
        for itype, count in interruptions["interruption_type"].value_counts().items():
            log.info("interruption transform:   type=%-20s %d", itype, count)

    # ── Pre-load validation ───────────────────────────────────────────────────

    log.info("interruption transform: running pre-load validation")
    from src.common.validators.interruption import validate_pre_load

    validation = validate_pre_load(
        trip_updates=trip_updates,
        stop_time_updates=stop_time_updates,
        service_alerts=service_alerts,
        entity_selectors=entity_selectors,
        interruptions=interruptions,
        interruption_sources=sources,
    )
    log.info(
        "interruption transform: pre-load validation result:\n%s",
        validation.summary(),
    )
    if not validation.passed:
        raise ValueError(
            f"Interruption layer pre-load validation failed — aborting pipeline:\n"
            f"{validation.summary()}"
        )

    log.info("interruption transform: complete")

    return InterruptionTransformResult(
        trip_updates=trip_updates,
        stop_time_updates=stop_time_updates,
        service_alerts=service_alerts,
        entity_selectors=entity_selectors,
        interruptions=interruptions,
        interruption_sources=sources,
        affects_trip=affects_trip,
        affects_route=affects_route,
        affects_stop=affects_stop,
        feed_info=feed_info,
        stats=stats,
    )
