# src/common/validators/accessibility.py
"""
Accessibility layer integrity checks, run in two phases:

  validate_pre_load  — checks the transformed AccessibilityTransformResult
                       DataFrames before any Neo4j writes. Catches data quality
                       issues that transform should have caught but may have missed
                       on edge-case API responses.

  validate_post_load — checks the graph after all four load phases have run,
                       including stale resolution. All failures are non-blocking
                       (warnings or info only) — the layer is a live poll and
                       should not abort on transient API anomalies.

Pre-load checks:
  1.  No duplicate composite_key values after transform dedup
      (transform already deduplicates, but this is a belt-and-suspenders guard)
  2.  All severity values are in {2, 3, 4} per schema §3
      (unknown symptom_descriptions default to 2, so violations indicate a
      new symptom type not yet mapped in _SYMPTOM_SEVERITY)

Post-load checks:
  3.  Active OutageEvent count is non-zero
      (zero active outages after a successful API call is suspicious — warn;
      WMATA typically has 20–40 active outages at any given time)
  4.  AFFECTS match rate ≥ 95%
      (unmatched outages have no [:AFFECTS]→:Pathway link; Tier 2 static
      lookup stub causes known gaps at 4 complex stations, but total unmatched
      should remain under 5% once the primary Tier 1 join is healthy)
  5.  Soft counts: active vs resolved OutageEvents, AFFECTS relationship count
      (informational only — live snapshot, no stable thresholds)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.common.validators.base import ValidationResult, run_count_check

if TYPE_CHECKING:
    from src.common.neo4j_tools import Neo4jManager
    from src.layers.accessibility.transform import AccessibilityTransformResult

# Acceptable unmatched AFFECTS rate — warn if exceeded
_MAX_UNMATCHED_RATE = 0.05


def validate_pre_load(result: AccessibilityTransformResult) -> ValidationResult:
    """
    Check transformed outage DataFrames before any Neo4j writes.

    Args:
        result: AccessibilityTransformResult from transform.run()

    Returns:
        ValidationResult — errors block the write; warnings do not.
    """
    vr = ValidationResult()
    outages = result.outages

    if outages.empty:
        vr.note("Pre-load: no outage rows to validate (empty transform result)")
        return vr

    # ── Check 1: no duplicate composite_key ──────────────────────────────────
    #
    # transform.run() deduplicates on composite_key. A duplicate here means the
    # dedup was bypassed or the composite_key derivation is non-unique — either
    # would cause MERGE to silently collapse distinct outage snapshots.

    dup_count = outages["composite_key"].duplicated().sum()
    if dup_count > 0:
        vr.fail(
            f"Pre-load check 1 FAILED: {dup_count} duplicate composite_key value(s) "
            f"after transform dedup — snapshot identity is broken"
        )
    else:
        vr.note(f"Pre-load check 1 passed: {len(outages)} unique composite_key values")

    # ── Check 2: all severity values in {{2, 3, 4}} ───────────────────────────
    #
    # severity is derived from symptom_description via _SYMPTOM_SEVERITY.
    # Unknown descriptions default to 2, so values outside {2, 3, 4} would
    # indicate a derivation bug, not just an unmapped description.

    valid_severities = {2, 3, 4}
    if "severity" in outages.columns:
        bad = outages[~outages["severity"].isin(valid_severities)]
        if not bad.empty:
            bad_vals = sorted(bad["severity"].unique().tolist())
            vr.warn(
                f"Pre-load check 2: {len(bad)} row(s) have severity outside {{2,3,4}}: "
                f"{bad_vals} — check _SYMPTOM_SEVERITY mapping for new symptom types"
            )
        else:
            vr.note("Pre-load check 2 passed: all severity values in {2, 3, 4}")

    return vr


def validate_post_load(neo4j: Neo4jManager) -> ValidationResult:
    """
    Check graph integrity after all four accessibility load phases have run.
    All findings are warnings or info — nothing raises.

    Args:
        neo4j: Live Neo4jManager for graph queries.

    Returns:
        ValidationResult — non-blocking; caller logs the summary.
    """
    vr = ValidationResult()

    # ── Check 3: active OutageEvent count is non-zero ─────────────────────────
    #
    # Zero active outages after a successful poll is suspicious. It may indicate:
    #   - The WMATA API returned an empty response (network error, rate limit)
    #   - All outages were incorrectly resolved as stale
    #   - Phase 2 (node write) failed silently
    #
    # This is a warning, not a block — WMATA may genuinely have zero outages
    # (extremely rare but theoretically possible during maintenance windows).

    active_count = run_count_check(
        neo4j,
        "MATCH (o:OutageEvent {status: 'active'}) RETURN count(o) AS n",
    )

    if active_count == 0:
        vr.warn(
            "Post-load check 3: zero active OutageEvent nodes — "
            "API may have returned an empty response or all outages were resolved. "
            "Verify WMATA API connectivity and stale-resolution logic."
        )
    else:
        vr.note(f"Post-load check 3: {active_count} active OutageEvent node(s)")

    # ── Check 4: AFFECTS match rate ≥ 95% ────────────────────────────────────
    #
    # Active OutageEvent nodes without an [:AFFECTS]→:Pathway link are not
    # analytically useful for the primary correlation query. Known causes:
    #   - Tier 2 static lookup stub (Metro Center, Gallery Place, L'Enfant Plaza,
    #     Fort Totten) — these 4 stations have ~30 units across ~98 stations
    #   - Novel unit_name formats not yet handled by Tier 1 programmatic join
    #
    # Threshold: >5% unmatched triggers a warning (schema §5 target).

    if active_count > 0:
        unmatched_count = run_count_check(
            neo4j,
            """
            MATCH (o:OutageEvent {status: 'active'})
            WHERE NOT (o)-[:AFFECTS]->(:Pathway)
            RETURN count(o) AS n
            """,
        )
        unmatched_rate = unmatched_count / active_count
        matched_count = active_count - unmatched_count

        if unmatched_rate > _MAX_UNMATCHED_RATE:
            vr.warn(
                f"Post-load check 4: AFFECTS match rate is "
                f"{matched_count}/{active_count} "
                f"({100 * (1 - unmatched_rate):.1f}%) — "
                f"below 95% threshold. "
                f"{unmatched_count} active OutageEvent(s) have no [:AFFECTS]→:Pathway link. "
                f"Check pathway_joiner logs for unmatched unit names."
            )
        else:
            vr.note(
                f"Post-load check 4: AFFECTS match rate "
                f"{matched_count}/{active_count} "
                f"({100 * (1 - unmatched_rate):.1f}%) — within threshold"
            )

    # ── Check 5: soft counts ──────────────────────────────────────────────────

    resolved_count = run_count_check(
        neo4j,
        "MATCH (o:OutageEvent {status: 'resolved'}) RETURN count(o) AS n",
    )
    affects_count = run_count_check(
        neo4j,
        "MATCH ()-[r:AFFECTS]->(:Pathway) RETURN count(r) AS n",
    )

    vr.note(
        f"Post-load check 5: OutageEvent counts — "
        f"active={active_count} resolved={resolved_count} "
        f"AFFECTS_relationships={affects_count}"
    )

    return vr
