# src/common/validators/interruption.py
"""
Interruption layer integrity checks, run in two phases:

  validate_pre_load  — checks transformed DataFrames after dedup, before
                       any Neo4j writes. GTFS-RT has no static file to
                       validate upfront — this runs against the flattened
                       protobuf output from extract + transform.

  validate_post_load — checks the graph after all three tiers have been
                       committed and Rule 6 enrichment has run.

Pre-load checks:
  1.  No duplicate dedup_hash values after TripUpdate deduplication
  2.  No duplicate feed_entity_id values after ServiceAlert deduplication
  3.  No interruption_id collision between TripUpdate and ServiceAlert
      namespaces (int_tu_* vs int_sa_*)
  4.  Every SOURCED_FROM entry references a known interruption_id
  5.  TripUpdates that generated Interruptions but have null trip_id
      (AFFECTS_TRIP will be silently missing for these — warn only)
  6.  ServiceAlerts with unmapped effect values (dropped by Rule 4 — warn)

Post-load checks:
  7.  No duplicate dedup_hash on TripUpdate nodes
  8.  No duplicate feed_entity_id on ServiceAlert nodes
  9.  No duplicate interruption_id on Interruption nodes
  10. Every Interruption has at least one SOURCED_FROM relationship
  11. Every Interruption has ON_DATE → Date (warn if missing — date may
      have been null in source)
  12. Interruption:Cancellation and :Delay nodes have AFFECTS_TRIP
      (warn if missing — trip_id may have been null in TripUpdate)
  13. Rule 6 enrichment result — DURING_PLANNED_SERVICE count (info only)
  14. Soft counts by interruption type (info only — live snapshot, no
      stable thresholds)

Known characteristics:
  - Rules 5 (correlation rollup) is deferred — no post-load check for it.
  - Rule 6 (DURING_PLANNED_SERVICE) runs as post-load enrichment. Check 13
    reports the result but does not block on zero — no maintenance windows
    may be active at poll time.
  - GTFS-RT is a live snapshot. All counts are informational only.
  - Interruptions with null trip_id (route-level or stop-level alerts)
    will have no AFFECTS_TRIP. This is expected for alert-derived
    Interruptions where EntitySelector specifies route_id but not trip_id.
"""

from __future__ import annotations

import pandas as pd

from src.common.validators.base import ValidationResult

# Effect values that Rule 4 maps to an Interruption type.
# Alerts with effects outside this set are silently dropped.
# Must stay in sync with EFFECT_TYPE_MAP in transform.py.
_MAPPED_EFFECTS = {
    "NO_SERVICE",
    "REDUCED_SERVICE",
    "SIGNIFICANT_DELAYS",
    "DETOUR",
    "MODIFIED_SERVICE",
    "STOP_MOVED",
    "ACCESSIBILITY_ISSUE",
}


# ── Pre-load validator ────────────────────────────────────────────────────────


def validate_pre_load(
    trip_updates: pd.DataFrame,
    stop_time_updates: pd.DataFrame,
    service_alerts: pd.DataFrame,
    entity_selectors: pd.DataFrame,
    interruptions: pd.DataFrame,
    interruption_sources: pd.DataFrame,
) -> ValidationResult:
    """
    Validates transformed DataFrames after dedup, before Neo4j writes.
    Called at the end of interruption/transform.py before returning results.

    All inputs are the post-dedup, post-transform DataFrames from
    InterruptionTransformResult.
    """
    result = ValidationResult()

    # ── Check 1: no duplicate dedup_hash after TripUpdate dedup ──────────────
    #
    # The dedup step in transform.run() calls drop_duplicates(subset=["dedup_hash"]).
    # If duplicates remain here, the hash function has a collision or the
    # dedup step was bypassed.

    if not trip_updates.empty and "dedup_hash" in trip_updates.columns:
        dup_hashes = trip_updates[trip_updates.duplicated(subset=["dedup_hash"], keep=False)]
        if not dup_hashes.empty:
            n = dup_hashes["dedup_hash"].nunique()
            result.fail(
                f"{n} dedup_hash value(s) appear on more than one TripUpdate "
                f"after deduplication — hash collision or dedup bypass"
            )
        else:
            result.note(
                f"No duplicate dedup_hash values ({len(trip_updates)} TripUpdates "
                f"after dedup)"
            )
    else:
        result.note("No TripUpdates in this poll")

    # ── Check 2: no duplicate feed_entity_id after ServiceAlert dedup ─────────

    if not service_alerts.empty:
        dup_alerts = service_alerts[
            service_alerts.duplicated(subset=["feed_entity_id"], keep=False)
        ]
        if not dup_alerts.empty:
            n = dup_alerts["feed_entity_id"].nunique()
            result.fail(
                f"{n} feed_entity_id value(s) appear on more than one ServiceAlert "
                f"after deduplication"
            )
        else:
            result.note(
                f"No duplicate feed_entity_id values ({len(service_alerts)} "
                f"ServiceAlerts after dedup)"
            )
    else:
        result.note("No ServiceAlerts in this poll")

    # ── Check 3: no interruption_id collision across TU and SA namespaces ────
    #
    # TripUpdate-derived IDs use prefix "int_tu_", ServiceAlert-derived use
    # "int_sa_". Collision would mean one Interruption SOURCED_FROM both,
    # corrupting the rollup. In practice this can't happen with current ID
    # generation logic, but worth asserting explicitly.

    if not interruptions.empty:
        tu_ids = set(
            interruptions[
                interruptions["interruption_id"].str.startswith("int_tu_", na=False)
            ]["interruption_id"]
        )
        sa_ids = set(
            interruptions[
                interruptions["interruption_id"].str.startswith("int_sa_", na=False)
            ]["interruption_id"]
        )
        collision = tu_ids & sa_ids
        unknown_prefix = interruptions[
            ~interruptions["interruption_id"].str.startswith("int_tu_", na=False)
            & ~interruptions["interruption_id"].str.startswith("int_sa_", na=False)
            & ~interruptions["interruption_id"].str.startswith("int_skip_", na=False)
        ]
        if collision:
            result.fail(
                f"{len(collision)} interruption_id(s) appear in both int_tu_ and "
                f"int_sa_ namespaces: {list(collision)[:5]}"
            )
        elif not unknown_prefix.empty:
            result.warn(
                f"{len(unknown_prefix)} Interruption(s) have unrecognised id prefix "
                f"(expected int_tu_, int_sa_, int_skip_): "
                f"{unknown_prefix['interruption_id'].tolist()[:5]}"
            )
        else:
            result.note(
                f"No interruption_id namespace collisions "
                f"({len(tu_ids)} TU-derived, {len(sa_ids)} SA-derived, "
                f"{len(interruptions) - len(tu_ids) - len(sa_ids)} skip-derived)"
            )

    # ── Check 4: every SOURCED_FROM entry references a known interruption_id ─
    #
    # An orphaned source row means the Interruption node was not created but
    # a SOURCED_FROM relationship will be attempted — MERGE will silently
    # create a bare Interruption node with no properties.

    if not interruption_sources.empty and not interruptions.empty:
        known_ids = set(interruptions["interruption_id"].tolist())
        orphaned = interruption_sources[
            ~interruption_sources["interruption_id"].isin(known_ids)
        ]
        if not orphaned.empty:
            result.fail(
                f"{len(orphaned)} SOURCED_FROM row(s) reference interruption_id(s) "
                f"not in the Interruption DataFrame — orphaned source links: "
                f"{orphaned['interruption_id'].tolist()[:5]}"
            )
        else:
            result.note(
                f"All {len(interruption_sources)} SOURCED_FROM entries reference "
                f"a known Interruption"
            )

    # ── Check 5: TripUpdates that produced Interruptions but have null trip_id
    #
    # AFFECTS_TRIP will be silently missing for these. Warn so it's visible
    # in the pipeline log. This can be legitimate (e.g. WMATA omits trip_id
    # on some real-time feeds) but should be tracked.

    if not trip_updates.empty and not interruptions.empty:
        tu_int_ids = set(
            interruptions[
                interruptions["interruption_id"].str.startswith("int_tu_", na=False)
            ]["interruption_id"]
        )
        if tu_int_ids:
            # Derive feed_entity_id from int_id by stripping prefix
            tu_ent_ids = {iid.replace("int_tu_", "") for iid in tu_int_ids}
            null_trip = trip_updates[
                trip_updates["feed_entity_id"].isin(tu_ent_ids)
                & (trip_updates["trip_id"].isna()
                   | (trip_updates["trip_id"].astype(str).str.strip() == ""))
            ]
            if not null_trip.empty:
                result.warn(
                    f"{len(null_trip)} TripUpdate(s) produced an Interruption but "
                    f"have null trip_id — AFFECTS_TRIP will be missing for these: "
                    f"{null_trip['feed_entity_id'].tolist()[:5]}"
                )
            else:
                result.note(
                    "All TripUpdate-derived Interruptions have a trip_id"
                )

    # ── Check 6: ServiceAlerts with unmapped effect values ────────────────────
    #
    # Rule 4 silently drops alerts whose effect is not in EFFECT_TYPE_MAP.
    # Warn so dropped alerts are visible — a new WMATA effect value in a
    # future feed version would otherwise be invisible.

    if not service_alerts.empty:
        unmapped = service_alerts[
            ~service_alerts["effect"].isin(_MAPPED_EFFECTS)
        ]
        if not unmapped.empty:
            effects = unmapped["effect"].value_counts().to_dict()
            result.warn(
                f"{len(unmapped)} ServiceAlert(s) have effect values not mapped "
                f"by Rule 4 — these alerts produce no Interruption node: {effects}"
            )
        else:
            result.note(
                f"All {len(service_alerts)} ServiceAlerts have mapped effect values"
            )

    return result


# ── Post-load validator ───────────────────────────────────────────────────────


def validate_post_load(neo4j_manager) -> ValidationResult:  # type: ignore[no-untyped-def]
    """
    Validates the interruption layer graph after all writes and enrichment.
    Called at the end of interruption/load.py after _run_enrichment().

    neo4j_manager: instance of src.common.neo4j_tools.Neo4jManager
    """
    result = ValidationResult()

    def _run(cypher: str) -> int:
        with neo4j_manager.driver.session() as session:
            record = session.run(cypher).single()
            return record["n"] if record else 0

    # ── Blocking checks ───────────────────────────────────────────────────────

    blocking_checks = [
        (
            # Check 7: no duplicate dedup_hash on TripUpdate nodes
            """
            MATCH (tu:TripUpdate)
            WITH tu.dedup_hash AS h, count(tu) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} dedup_hash value(s) appear on more than one TripUpdate node",
            "No duplicate dedup_hash values on TripUpdate nodes",
        ),
        (
            # Check 8: no duplicate feed_entity_id on ServiceAlert nodes
            """
            MATCH (sa:ServiceAlert)
            WITH sa.feed_entity_id AS eid, count(sa) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} feed_entity_id value(s) appear on more than one ServiceAlert node",
            "No duplicate feed_entity_id values on ServiceAlert nodes",
        ),
        (
            # Check 9: no duplicate interruption_id on Interruption nodes
            """
            MATCH (i:Interruption)
            WITH i.interruption_id AS iid, count(i) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} interruption_id value(s) appear on more than one Interruption node",
            "No duplicate interruption_id values on Interruption nodes",
        ),
        (
            # Check 10: every Interruption has at least one SOURCED_FROM
            # An Interruption with no source cannot be explained or audited.
            # This would indicate a transform/load ordering issue.
            """
            MATCH (i:Interruption)
            WHERE NOT (i)-[:SOURCED_FROM]->()
            RETURN count(i) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} Interruption node(s) have no SOURCED_FROM relationship",
            "All Interruptions have at least one SOURCED_FROM relationship",
        ),
    ]

    for cypher, ok_fn, err_fn, ok_msg in blocking_checks:
        n = _run(cypher)
        if ok_fn(n):
            result.note(ok_msg)
        else:
            result.fail(err_fn(n))

    # ── Warning checks ────────────────────────────────────────────────────────

    # Check 11: Interruptions missing ON_DATE → Date
    # Expected when date was null in the source (e.g. TripUpdate with no
    # start_date, or ServiceAlert with no active_period). Warn so volume
    # is visible — high numbers may indicate a transform date-parsing issue.
    n = _run(
        """
        MATCH (i:Interruption)
        WHERE NOT (i)-[:ON_DATE]->(:Date)
        RETURN count(i) AS n
        """
    )
    if n > 0:
        result.warn(
            f"{n} Interruption(s) have no ON_DATE → Date relationship "
            f"(null date in source — may be expected for some RT feeds)"
        )
    else:
        result.note("All Interruptions have ON_DATE → Date")

    # Check 12: Cancellation/Delay nodes missing AFFECTS_TRIP
    # These are trip-level interruptions derived from TripUpdates — they
    # should almost always have a trip_id. Warn if missing; high numbers
    # suggest WMATA is omitting trip_id in RT feed.
    n = _run(
        """
        MATCH (i:Interruption)
        WHERE (i:Cancellation OR i:Delay)
          AND NOT (i)-[:AFFECTS_TRIP]->(:Trip)
        RETURN count(i) AS n
        """
    )
    if n > 0:
        result.warn(
            f"{n} Interruption:Cancellation/:Delay node(s) have no AFFECTS_TRIP "
            f"→ Trip relationship (null trip_id in TripUpdate source)"
        )
    else:
        result.note(
            "All Cancellation and Delay Interruptions have AFFECTS_TRIP → Trip"
        )

    # ── Check 13: Rule 6 enrichment result (info only) ────────────────────────
    #
    # DURING_PLANNED_SERVICE count. Zero is valid — no maintenance windows
    # may overlap with current disruptions at poll time. Non-zero confirms
    # the enrichment query fired and found overlapping dates.

    n = _run(
        """
        MATCH ()-[r:DURING_PLANNED_SERVICE]->()
        RETURN count(r) AS n
        """
    )
    result.note(
        f"Rule 6 enrichment: {n} DURING_PLANNED_SERVICE relationship(s) created"
    )

    # ── Check 14: soft counts by interruption type (info only) ────────────────
    #
    # GTFS-RT is a live snapshot — no stable thresholds. Counts are recorded
    # so unexpected changes are visible in the pipeline log.

    soft_counts = [
        ("MATCH (tu:TripUpdate)           RETURN count(tu) AS n", "TripUpdate"),
        ("MATCH (stu:StopTimeUpdate)      RETURN count(stu) AS n", "StopTimeUpdate"),
        ("MATCH (sa:ServiceAlert)         RETURN count(sa) AS n", "ServiceAlert"),
        ("MATCH (es:EntitySelector)       RETURN count(es) AS n", "EntitySelector"),
        ("MATCH (i:Interruption)          RETURN count(i) AS n", "Interruption (total)"),
        ("MATCH (i:Interruption:Cancellation) RETURN count(i) AS n", "Interruption:Cancellation"),
        ("MATCH (i:Interruption:Delay)    RETURN count(i) AS n", "Interruption:Delay"),
        ("MATCH (i:Interruption:Skip)     RETURN count(i) AS n", "Interruption:Skip"),
        ("MATCH (i:Interruption:Detour)   RETURN count(i) AS n", "Interruption:Detour"),
        ("MATCH (i:Interruption:ServiceChange) RETURN count(i) AS n", "Interruption:ServiceChange"),
        ("MATCH (i:Interruption:Accessibility) RETURN count(i) AS n", "Interruption:Accessibility"),
    ]

    for cypher, label in soft_counts:
        n = _run(cypher)
        result.note(f"{label}: {n}")

    return result
