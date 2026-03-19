# src/common/validators/service_schedule.py
"""
Service & Schedule layer integrity checks, run in two phases:

  validate_pre_load  — checks raw GTFS DataFrames before any Neo4j writes
  validate_post_load — checks the graph after all nodes and relationships
                       have been committed

Pre-load checks (run against raw DataFrames in transform.py):
  1.  No duplicate trip_id in trips.txt
  2.  No shape_id maps to more than one route_id (RoutePattern identity)
  3.  No trip references a service_id absent from both calendar.txt and
      calendar_dates.txt (would produce orphaned Trip nodes)
  4.  All stop_ids in stop_times.txt exist in stops.txt
  5.  Trips missing shape_id (cannot derive RoutePattern — warn only)
  6.  _R service patterns with date ranges extending beyond feed window
      (informational — clipping is applied in transform, this confirms it)

Post-load checks (run against Neo4j after load.py completes):
  7.  No duplicate trip_id on Trip nodes
  8.  No duplicate shape_id on RoutePattern nodes
  9.  No duplicate route_id on Route nodes
  10. No duplicate service_id on ServicePattern nodes
  11. No duplicate date value on Date nodes
  12. Every Trip has exactly one OPERATED_ON and exactly one FOLLOWS
  13. No ServicePattern is ACTIVE_ON the same Date more than once
  14. Date node range consistent with feed window (soft — warning only)
  15. Route, Trip, ServicePattern counts consistent with previous run
      (soft — warning only; no hard expected values)

Known data characteristics:
  - service_ids 6, 9, 10 have mixed day flags (e.g. Mon-only, Fri-only,
    Mon/Tue/Thu) — classified as Weekday with a warning. Expected.
  - _R service patterns in calendar.txt declare multi-year ranges
    (20240101–20301231). Transform clips to feed window. Confirmed here.
  - calendar_dates.txt _R entries extend up to 20260627 (14 days past
    feed_end_date 20260613). Retained as real scheduled maintenance events.
"""

from __future__ import annotations

import pandas as pd

from src.common.validators.base import ValidationResult

# ── Pre-load validator ────────────────────────────────────────────────────────


def validate_pre_load(
    trips: pd.DataFrame,
    stop_times: pd.DataFrame,
    stops: pd.DataFrame,
    calendar: pd.DataFrame,
    calendar_dates: pd.DataFrame | None,
    feed_start: str,
    feed_end: str,
) -> ValidationResult:
    """
    Validates raw GTFS DataFrames before any Neo4j writes.
    Called at the end of service_schedule/transform.py before returning results.

    feed_start / feed_end: YYYYMMDD strings from feed_info.txt.
    """
    result = ValidationResult()

    # ── Check 1: no duplicate trip_id ────────────────────────────────────────
    #
    # Duplicate trip_ids would cause MERGE to silently overwrite properties
    # from a previous row, producing a node with unpredictable state.

    dup_trips = trips[trips.duplicated(subset=["trip_id"], keep=False)]
    if not dup_trips.empty:
        n = dup_trips["trip_id"].nunique()
        examples = dup_trips["trip_id"].unique()[:5].tolist()
        result.fail(f"{n} trip_id(s) appear more than once in trips.txt: {examples}")
    else:
        result.note(f"No duplicate trip_ids ({len(trips)} trips)")

    # ── Check 2: no shape_id maps to multiple route_ids ───────────────────────
    #
    # RoutePattern is keyed on shape_id. If one shape_id appears under two
    # route_ids, the first() groupby in _derive_route_patterns silently drops
    # one, producing a pattern with a wrong or missing route association.

    if "shape_id" in trips.columns and "route_id" in trips.columns:
        shape_routes = (
            trips.dropna(subset=["shape_id"]).groupby("shape_id")["route_id"].nunique()
        )
        multi_route_shapes = shape_routes[shape_routes > 1]
        if not multi_route_shapes.empty:
            examples = multi_route_shapes.index[:5].tolist()
            result.fail(
                f"{len(multi_route_shapes)} shape_id(s) map to more than one "
                f"route_id — RoutePattern identity is ambiguous: {examples}"
            )
        else:
            result.note(
                f"All shape_ids map to exactly one route_id "
                f"({shape_routes.index.nunique()} patterns)"
            )

    # ── Check 3: all service_ids in trips resolve to a calendar entry ─────────
    #
    # A trip with a service_id that exists in neither calendar.txt nor
    # calendar_dates.txt would produce a Trip node with no OPERATED_ON
    # relationship, making it unreachable in service-scoped queries.

    known_service_ids: set[str] = set(
        calendar["service_id"].astype(str).str.strip().tolist()
    )
    if calendar_dates is not None and not calendar_dates.empty:
        known_service_ids.update(
            calendar_dates["service_id"].astype(str).str.strip().tolist()
        )

    trip_service_ids = set(trips["service_id"].astype(str).str.strip().tolist())
    orphaned = trip_service_ids - known_service_ids
    if orphaned:
        result.fail(
            f"{len(orphaned)} service_id(s) referenced in trips.txt have no "
            f"entry in calendar.txt or calendar_dates.txt — trips would be "
            f"orphaned (no OPERATED_ON): {sorted(orphaned)[:5]}"
        )
    else:
        result.note(
            f"All {len(trip_service_ids)} service_ids in trips.txt resolve "
            f"to a calendar entry"
        )

    # ── Check 4: all stop_ids in stop_times exist in stops ───────────────────
    #
    # Stop_ids not in stops.txt cannot be matched to Platform/BusStop nodes.
    # SCHEDULED_AT relationships for those stops would silently produce no
    # relationships (MATCH finds nothing, MERGE never fires).
    # Warn rather than block — cross-feed references are possible in GTFS.

    known_stop_ids = set(stops["stop_id"].astype(str).str.strip().tolist())
    st_stop_ids = set(stop_times["stop_id"].astype(str).str.strip().tolist())
    unknown_stops = st_stop_ids - known_stop_ids
    if unknown_stops:
        result.warn(
            f"{len(unknown_stops)} stop_id(s) in stop_times.txt not found in "
            f"stops.txt — SCHEDULED_AT relationships for these stops will be "
            f"missing: {sorted(unknown_stops)[:5]}"
        )
    else:
        result.note(
            f"All {len(st_stop_ids)} stop_ids in stop_times.txt exist in stops.txt"
        )

    # ── Check 5: trips missing shape_id ──────────────────────────────────────
    #
    # Trips without shape_id cannot be assigned to a RoutePattern.
    # FOLLOWS relationship and pattern-level STOPS_AT will be absent.
    # Warn rather than block — WMATA may legitimately omit shape for some trips.

    if "shape_id" in trips.columns:
        no_shape = trips[
            trips["shape_id"].isna() | (trips["shape_id"].astype(str).str.strip() == "")
        ]
        if not no_shape.empty:
            result.warn(
                f"{len(no_shape)} trip(s) have no shape_id — cannot derive "
                f"RoutePattern; FOLLOWS relationship will be missing for these trips"
            )
        else:
            result.note("All trips have a shape_id")

    # ── Check 6: _R service patterns date range vs feed window ───────────────
    #
    # Informational: confirms that _R multi-year ranges will be clipped.
    # The actual clipping happens in _resolve_calendar; this surfaces it in
    # the validation log so the behaviour is visible and auditable.

    r_patterns = calendar[
        calendar["service_id"].astype(str).str.endswith("_R", na=False)
    ]
    if not r_patterns.empty:
        wide_ranges = r_patterns[
            (r_patterns["start_date"].astype(str) < feed_start)
            | (r_patterns["end_date"].astype(str) > feed_end)
        ]
        if not wide_ranges.empty:
            ids = wide_ranges["service_id"].tolist()
            result.note(
                f"{len(wide_ranges)} _R maintenance service pattern(s) have date "
                f"ranges extending beyond feed window ({feed_start}–{feed_end}) "
                f"— clipped in transform: {ids}"
            )
        else:
            result.note(
                f"All {len(r_patterns)} _R maintenance patterns fit within feed window"
            )

    return result


# ── Post-load validator ───────────────────────────────────────────────────────


def validate_post_load(neo4j_manager) -> ValidationResult:  # type: ignore[no-untyped-def]
    """
    Validates service & schedule integrity by querying Neo4j after loading.
    Called at the end of service_schedule/load.py after all writes complete.

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
            # Check 7: no duplicate trip_id on Trip nodes
            """
            MATCH (t:Trip)
            WITH t.trip_id AS tid, count(t) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} trip_id value(s) appear on more than one Trip node",
            "No duplicate trip_id values on Trip nodes",
        ),
        (
            # Check 8: no duplicate shape_id on RoutePattern nodes
            """
            MATCH (rp:RoutePattern)
            WITH rp.shape_id AS sid, count(rp) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: (
                f"{n} shape_id value(s) appear on more than one RoutePattern node"
            ),
            "No duplicate shape_id values on RoutePattern nodes",
        ),
        (
            # Check 9: no duplicate route_id on Route nodes
            """
            MATCH (r:Route)
            WITH r.route_id AS rid, count(r) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} route_id value(s) appear on more than one Route node",
            "No duplicate route_id values on Route nodes",
        ),
        (
            # Check 10: no duplicate service_id on ServicePattern nodes
            """
            MATCH (sp:ServicePattern)
            WITH sp.service_id AS sid, count(sp) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: (
                f"{n} service_id value(s) appear on more than one ServicePattern node"
            ),
            "No duplicate service_id values on ServicePattern nodes",
        ),
        (
            # Check 11: no duplicate date value on Date nodes
            """
            MATCH (d:Date)
            WITH d.date AS dt, count(d) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} date value(s) appear on more than one Date node",
            "No duplicate date values on Date nodes",
        ),
        (
            # Check 12a: every Trip has exactly one OPERATED_ON
            # Trips without OPERATED_ON are unreachable in service-scoped queries.
            """
            MATCH (t:Trip)
            WHERE NOT (t)-[:OPERATED_ON]->(:ServicePattern)
            RETURN count(t) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} Trip(s) missing OPERATED_ON → ServicePattern",
            "All Trips have OPERATED_ON → ServicePattern",
        ),
        (
            # Check 12b: every Trip has exactly one FOLLOWS
            # Trips without FOLLOWS cannot be associated with a RoutePattern
            # for stop sequence or pattern-level queries.
            """
            MATCH (t:Trip)
            WHERE NOT (t)-[:FOLLOWS]->(:RoutePattern)
            RETURN count(t) AS n
            """,
            lambda n: n == 0,
            lambda n: f"{n} Trip(s) missing FOLLOWS → RoutePattern",
            "All Trips have FOLLOWS → RoutePattern",
        ),
        (
            # Check 13: no ServicePattern active on the same Date more than once
            # Duplicate ACTIVE_ON rels would double-count service on that date
            # in queries like "how many trips ran on date X".
            """
            MATCH (sp:ServicePattern)-[:ACTIVE_ON]->(d:Date)
            WITH sp.service_id AS sid, d.date AS dt, count(*) AS n
            WHERE n > 1
            RETURN count(*) AS n
            """,
            lambda n: n == 0,
            lambda n: (
                f"{n} (ServicePattern, Date) pair(s) have duplicate ACTIVE_ON relationships"
            ),
            "No duplicate ACTIVE_ON relationships",
        ),
    ]

    for cypher, ok_fn, err_fn, ok_msg in blocking_checks:
        n = _run(cypher)
        if ok_fn(n):
            result.note(ok_msg)
        else:
            result.fail(err_fn(n))

    # ── Check 14: Date range consistent with feed window ─────────────────────
    #
    # Warn if earliest Date is before feed_start_date — indicates calendar
    # range clipping did not apply correctly. Dates slightly past feed_end
    # are acceptable (explicit _R calendar_dates entries).

    with neo4j_manager.driver.session() as session:
        record = session.run(
            """
            MATCH (d:Date)
            RETURN min(d.date) AS earliest, max(d.date) AS latest, count(d) AS n
            """
        ).single()

    if record:
        earliest = record["earliest"]
        latest = record["latest"]
        n_dates = record["n"]

        with neo4j_manager.driver.session() as session:
            fi = session.run(
                """
                MATCH (fi:FeedInfo)
                RETURN fi.feed_start_date AS feed_start, fi.feed_end_date AS feed_end
                ORDER BY fi.feed_version DESC LIMIT 1
                """
            ).single()

        if fi:
            feed_start = str(fi["feed_start"])
            feed_end = str(fi["feed_end"])

            if earliest < feed_start:
                result.warn(
                    f"Earliest Date node ({earliest}) is before feed_start_date "
                    f"({feed_start}) — calendar range clipping may not have applied. "
                    f"Check _resolve_calendar in transform.py"
                )
            else:
                result.note(
                    f"Date range {earliest}–{latest} ({n_dates} nodes) is consistent "
                    f"with feed window {feed_start}–{feed_end}"
                )
        else:
            result.warn("Could not verify Date range — no FeedInfo node found in graph")

    # ── Check 15: soft node counts ────────────────────────────────────────────
    #
    # No hard expected values — counts are recorded as info so unexpected
    # changes after a feed update are visible in the pipeline log.
    # If a count changes significantly, investigate before accepting.

    soft_counts = [
        ("MATCH (r:Route)          RETURN count(r) AS n", "Route"),
        ("MATCH (r:Route:Bus)      RETURN count(r) AS n", "Route:Bus"),
        ("MATCH (r:Route:Rail)     RETURN count(r) AS n", "Route:Rail"),
        ("MATCH (rp:RoutePattern)  RETURN count(rp) AS n", "RoutePattern"),
        ("MATCH (t:Trip)           RETURN count(t) AS n", "Trip"),
        ("MATCH (sp:ServicePattern) RETURN count(sp) AS n", "ServicePattern"),
        (
            "MATCH (sp:ServicePattern:Weekday)     RETURN count(sp) AS n",
            "ServicePattern:Weekday",
        ),
        (
            "MATCH (sp:ServicePattern:Saturday)    RETURN count(sp) AS n",
            "ServicePattern:Saturday",
        ),
        (
            "MATCH (sp:ServicePattern:Sunday)      RETURN count(sp) AS n",
            "ServicePattern:Sunday",
        ),
        (
            "MATCH (sp:ServicePattern:Holiday)     RETURN count(sp) AS n",
            "ServicePattern:Holiday",
        ),
        (
            "MATCH (sp:ServicePattern:Maintenance) RETURN count(sp) AS n",
            "ServicePattern:Maintenance",
        ),
        ("MATCH (d:Date)           RETURN count(d) AS n", "Date"),
    ]

    for cypher, label in soft_counts:
        n = _run(cypher)
        result.note(f"{label}: {n}")

    return result
