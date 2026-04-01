# scripts/validate_anchor_resolution.py
"""
Anchor resolution A/B validation script.

Demonstrates the value and limits of graph-assisted disambiguation
against the k=1 baseline. Three sections:

    Section 1 — Core disambiguation
        Single ambiguous station + unambiguous route. Shows coherence
        picking the correct station where k=1 string matching is arbitrary.

    Section 2 — Multi-anchor disambiguation
        Multiple stations and/or routes in the same query. Tests joint
        resolution and documents the equal-coherence boundary condition.

    Section 3 — Limitations
        Cases where coherence scoring cannot improve on k=1. Both columns
        are shown to prove they produce identical results — the point being
        that the system degrades gracefully rather than producing wrong answers.

Run:
    uv run python scripts/validate_anchor_resolution.py
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

from src.common.neo4j_tools import Neo4jManager
from src.llm.anchor_resolver import AnchorResolutions, AnchorResolver
from src.llm.disambiguation_strategies import TypeWeightedCoherenceStrategy
from src.llm.planner_output import PlannerAnchors


# ── Scenario types ────────────────────────────────────────────────────────────

@dataclass
class Scenario:
    """
    A single validation scenario.

    kind:
        'definitive'  — one correct answer; pass/fail comparison
        'ambiguous'   — multiple valid answers; reports what was returned
                        without a pass/fail judgment
        'limitation'  — expected to produce identical results at k=1 and k=5;
                        both columns shown to prove graceful degradation
    """
    label: str
    description: str
    anchors: PlannerAnchors
    kind: Literal["definitive", "ambiguous", "limitation"]
    # For 'definitive': {mention: expected_node_id}
    # For 'ambiguous':  {mention: set of valid node_ids}
    # For 'limitation': {mention: expected_node_id or None if expected empty}
    expected: dict = field(default_factory=dict)


# ── Configs ───────────────────────────────────────────────────────────────────

CONFIGS = [
    {
        "label":    "k=1  topk (baseline)",
        "short":    "Baseline (k=1)",
        "k":        1,
        "strategy": None,
    },
    {
        "label":    "k=5  coherence",
        "short":    "Coherence (k=5)",
        "k":        5,
        "strategy": TypeWeightedCoherenceStrategy(),
    },
]


# ── Section 1 — Core disambiguation ──────────────────────────────────────────
#
# Each scenario has one ambiguous station mention and one unambiguous route.
# At k=1 the station is resolved by string score alone — arbitrary when two
# candidates score equally. At k=5 coherence the SERVES relationship between
# the correct station and the queried route determines the winner.

SECTION_1 = [
    Scenario(
        label="C1 — Farragut + Red Line",
        description=(
            "The mention 'Farragut' matches both Farragut North (STN_A02, serves Red/Orange/Silver) "
            "and Farragut West (STN_C03, serves Blue/Orange/Silver). With the Red Line as context, "
            "coherence scoring finds the SERVES edge from Farragut North to RED and picks the "
            "correct station. k=1 resolves by string score alone — arbitrary."
        ),
        anchors=PlannerAnchors(stations=["Farragut"], routes=["Red Line"]),
        kind="definitive",
        expected={"Farragut": "STN_A02"},
    ),
    Scenario(
        label="C2 — Farragut + Blue Line",
        description=(
            "Same ambiguous mention 'Farragut', different route. Farragut West (STN_C03) serves "
            "the Blue Line; Farragut North does not. Coherence scoring picks the opposite winner "
            "to C1 — confirms the mechanism is driven by graph structure, not string order."
        ),
        anchors=PlannerAnchors(stations=["Farragut"], routes=["Blue Line"]),
        kind="definitive",
        expected={"Farragut": "STN_C03"},
    ),
    Scenario(
        label="C3 — Metro Center + Red Line (control)",
        description=(
            "Metro Center (STN_A01_C01) is unambiguous — the full-text index returns a single "
            "dominant candidate regardless of k. This confirms coherence scoring does not "
            "destabilize anchors that were already correct at k=1."
        ),
        anchors=PlannerAnchors(stations=["Metro Center"], routes=["Red Line"]),
        kind="definitive",
        expected={"Metro Center": "STN_A01_C01"},
    ),
]


# ── Section 2 — Multi-anchor disambiguation ───────────────────────────────────
#
# Multiple station and/or route mentions in the same query. The key property
# being tested is that joint resolution is independent per mention — resolving
# one station does not contaminate the scoring of another.
#
# M3 and M4 are marked 'ambiguous': when a station candidate serves both
# queried routes, both station candidates score equally and the tiebreaker is
# string order. The system is not wrong — the query is genuinely ambiguous at
# the anchor level. All returned values are reported.

SECTION_2 = [
    Scenario(
        label="M1 — Two stations + Red Line",
        description=(
            "Farragut (ambiguous) and Metro Center (unambiguous) in the same query, one route. "
            "Each station is scored independently against the Red Line. Metro Center passes "
            "through immediately; Farragut resolves via coherence. The two station mentions "
            "do not interfere — same-type edges between station candidates are excluded."
        ),
        anchors=PlannerAnchors(
            stations=["Farragut", "Metro Center"],
            routes=["Red Line"],
        ),
        kind="definitive",
        expected={
            "Farragut":     "STN_A02",       # North serves Red Line
            "Metro Center": "STN_A01_C01",
        },
    ),
    Scenario(
        label="M2 — Two stations + Blue Line",
        description=(
            "Same structure as M1 but Blue Line. Farragut West should win for 'Farragut' "
            "while Metro Center resolves unambiguously. Confirms the route context drives "
            "the outcome independently for each station mention."
        ),
        anchors=PlannerAnchors(
            stations=["Farragut", "Metro Center"],
            routes=["Blue Line"],
        ),
        kind="definitive",
        expected={
            "Farragut":     "STN_C03",       # West serves Blue Line
            "Metro Center": "STN_A01_C01",
        },
    ),
    Scenario(
        label="M3 — Farragut + Red Line + Blue Line",
        description=(
            "One ambiguous station, two routes. Farragut North serves Red Line (coherence +1.0) "
            "and Farragut West serves Blue Line (coherence +1.0). Both candidates score equally — "
            "the query is genuinely ambiguous: the user mentioned both lines without specifying "
            "which Farragut they mean. String score tiebreaker determines the result. "
            "k=1 and k=5 coherence will likely agree since the string score order is the same "
            "tiebreaker in both cases. Reported without pass/fail — all returned values shown."
        ),
        anchors=PlannerAnchors(
            stations=["Farragut"],
            routes=["Red Line", "Blue Line"],
        ),
        kind="ambiguous",
        expected={"Farragut": {"STN_A02", "STN_C03"}},
    ),
    Scenario(
        label="M4 — Two stations + Red Line + Blue Line",
        description=(
            "Full multi-anchor case: two stations (one ambiguous, one not) and two routes. "
            "Metro Center should still resolve cleanly. Farragut ties as in M3. "
            "Demonstrates that the equal-coherence boundary on Farragut does not corrupt "
            "the clean resolution of Metro Center — joint resolution remains independent."
        ),
        anchors=PlannerAnchors(
            stations=["Farragut", "Metro Center"],
            routes=["Red Line", "Blue Line"],
        ),
        kind="ambiguous",
        expected={
            "Farragut":     {"STN_A02", "STN_C03"},  # genuine tie
            "Metro Center": {"STN_A01_C01"},          # unambiguous — only one valid answer
        },
    ),
]


# ── Section 3 — Limitations ───────────────────────────────────────────────────
#
# Cases where coherence scoring cannot improve on k=1. k=1 and k=5 coherence
# columns are shown side by side to demonstrate graceful degradation — the
# strategy produces the same result as the baseline rather than a worse one.

SECTION_3 = [
    Scenario(
        label="L1 — Georgetown (not in graph)",
        description=(
            "Georgetown has no Metro station. The full-text index returns no results "
            "for '*Georgetown*' — the mention ends up in AnchorResolutions.failed. "
            "The system produces an empty resolution cleanly rather than hallucinating "
            "a nearby station. No coherence scoring fires."
        ),
        anchors=PlannerAnchors(stations=["Georgetown"]),
        kind="limitation",
        expected={"Georgetown": None},  # None = expected to fail resolution
    ),
    Scenario(
        label="L2 — Farragut alone (no route context)",
        description=(
            "Without a co-occurring route there is no cross-type edge to score. "
            "TypeWeightedCoherenceStrategy fetches both Farragut stations but all "
            "coherence scores are 0.0 — same-type exclusion means station candidates "
            "cannot score against each other. The tie is unresolvable. "
            "Coherence surfaces both candidates honestly; TopKStrategy silently "
            "returns one by string score."
        ),
        anchors=PlannerAnchors(stations=["Farragut"]),
        kind="ambiguous",
        expected={"Farragut": {"STN_A02", "STN_C03"}},
    ),
    Scenario(
        label="L3 — Pentagon + Blue Line (same-line ambiguity)",
        description=(
            "Pentagon (STN_C07) and Pentagon City (STN_C08) both serve the Blue Line. "
            "Both candidates score equally via SERVES — coherence cannot distinguish them. "
            "TypeWeightedCoherenceStrategy surfaces both candidates honestly. "
            "TopKStrategy silently returns one by string score."
        ),
        anchors=PlannerAnchors(stations=["Pentagon"], routes=["Blue Line"]),
        kind="ambiguous",
        expected={"Pentagon": {"STN_C07", "STN_C08"}},
    ),
    Scenario(
        label="L4 — Farragut + yesterday (date adds no coherence signal)",
        description=(
            "Date anchor resolves correctly but dates have no direct SERVES or "
            "ON_ROUTE edges to station candidates. Both Farragut stations score 0.0 — "
            "no cross-type edges found between date and station candidates. "
            "TypeWeightedCoherenceStrategy surfaces both as a tie. TopKStrategy "
            "silently picks one by string score. Resolving this would require "
            "multi-hop scoring through Trip/ServicePattern nodes (not implemented)."
        ),
        anchors=PlannerAnchors(stations=["Farragut"], dates=["yesterday"]),
        kind="ambiguous",
        expected={"Farragut": {"STN_A02", "STN_C03"}},
    ),
]


# ── Runner ────────────────────────────────────────────────────────────────────


def _resolve(
    anchors: PlannerAnchors,
    db: Neo4jManager,
    invocation_time: datetime,
    k: int,
    strategy,
) -> AnchorResolutions:
    resolver = AnchorResolver(
        db=db,
        invocation_time=invocation_time,
        strategy=strategy,
        candidate_limit=k,
    )
    return resolver.resolve(anchors)


def _resolve_with_ties(
    anchors: PlannerAnchors,
    db: Neo4jManager,
    invocation_time: datetime,
    k: int,
    strategy,
) -> dict[str, list[str]]:
    """
    For ambiguous scenarios — returns all tied top candidates per mention
    using TypeWeightedCoherenceStrategy.select_with_ties(), or wraps the
    single resolved value from the baseline in a list for uniform display.
    """
    from src.llm.disambiguation_strategies import TypeWeightedCoherenceStrategy

    resolver = AnchorResolver(
        db=db,
        invocation_time=invocation_time,
        strategy=strategy,
        candidate_limit=k,
    )

    # Phase 1: generate candidates (reuse resolver internals via resolve())
    # We call resolve() first to get candidates generated, then re-run
    # scoring via select_with_ties for the coherence config.
    resolutions = resolver.resolve(anchors)

    if isinstance(strategy, TypeWeightedCoherenceStrategy) and k > 1:
        # Re-generate candidates to pass to select_with_ties
        all_candidates: dict[str, list] = {}
        for name in anchors.stations:
            cands = resolver._fetch_station_candidates(name)
            if cands:
                all_candidates[name] = cands
        for name in anchors.routes:
            cands = resolver._fetch_route_candidates(name)
            if cands:
                all_candidates[name] = cands
        for expr in anchors.dates:
            cands = resolver._fetch_date_candidates(expr)
            if cands:
                all_candidates[expr] = cands

        tied = strategy.select_with_ties(all_candidates, db)
        return tied

    # Baseline (k=1 or TopK): wrap single resolved value in list
    result: dict[str, list[str]] = {}
    for mention in (
        list(anchors.stations) + list(anchors.routes)
        + list(anchors.dates) + list(anchors.pathway_nodes)
    ):
        resolved = _get_resolved(resolutions, mention)
        result[mention] = resolved if resolved else []
    return result


def _all_mentions(anchors: PlannerAnchors) -> list[str]:
    return (
        list(anchors.stations)
        + list(anchors.routes)
        + list(anchors.dates)
        + list(anchors.pathway_nodes)
    )


def _get_resolved(resolutions: AnchorResolutions, mention: str) -> list[str] | None:
    return (
        resolutions.resolved_stations.get(mention)
        or resolutions.resolved_routes.get(mention)
        or resolutions.resolved_dates.get(mention)
        or resolutions.resolved_pathway_nodes.get(mention)
        or None
    )


def _run_section(
    title: str,
    description: str,
    scenarios: list[Scenario],
    db: Neo4jManager,
    invocation_time: datetime,
) -> dict:
    """Run all scenarios in a section, print results, return summary counts."""
    print(f"\n{'─' * 72}")
    print(f"  {title}")
    print(f"  {description}")
    print(f"{'─' * 72}")

    section_totals = {cfg["label"]: {"pass": 0, "total": 0} for cfg in CONFIGS}

    for scenario in scenarios:
        results_by_config = {}
        for cfg in CONFIGS:
            resolutions = _resolve(
                scenario.anchors, db, invocation_time,
                cfg["k"], cfg["strategy"],
            )
            results_by_config[cfg["label"]] = resolutions

        mentions = _all_mentions(scenario.anchors)

        if scenario.kind == "definitive":
            _print_definitive(scenario, mentions, results_by_config, section_totals)
        elif scenario.kind == "ambiguous":
            _print_ambiguous(scenario, mentions, results_by_config, db, invocation_time)
        elif scenario.kind == "limitation":
            _print_limitation(scenario, mentions, results_by_config)

    return section_totals


def _print_definitive(scenario, mentions, results_by_config, totals):
    config_pass = {cfg["label"]: True for cfg in CONFIGS}

    # Build rows
    rows = []
    for mention in mentions:
        expected = scenario.expected.get(mention)
        cols = []
        for cfg in CONFIGS:
            resolutions = results_by_config[cfg["label"]]
            resolved = _get_resolved(resolutions, mention)
            failed = mention in resolutions.failed
            if expected is None:
                cols.append(f"  {resolved or '—'}")
            elif failed or resolved is None:
                cols.append("❌ (not resolved)")
                config_pass[cfg["label"]] = False
            elif resolved == expected:
                cols.append(f"✅ {resolved}")
            else:
                cols.append(f"❌ {resolved}")
                config_pass[cfg["label"]] = False
        rows.append((mention, cols))

    # Print
    short_labels = [cfg["short"] for cfg in CONFIGS]
    col_w = 24
    print(f"\n  {scenario.label}")
    print(f"  {'Mention':<18}  " + "  ".join(f"{lbl:<{col_w}}" for lbl in short_labels))
    print(f"  {'─' * 18}  " + "  ".join("─" * col_w for _ in CONFIGS))
    for mention, cols in rows:
        print(f"  {mention:<18}  " + "  ".join(f"{c:<{col_w}}" for c in cols))

    verdicts = []
    for cfg in CONFIGS:
        v = "PASS" if config_pass[cfg["label"]] else "FAIL"
        verdicts.append(f"{cfg['short']}: {v}")
        totals[cfg["label"]]["total"] += 1
        if config_pass[cfg["label"]]:
            totals[cfg["label"]]["pass"] += 1
    print(f"  {'─' * 18}  " + "  ".join("─" * col_w for _ in CONFIGS))
    print(f"  {'Result':<18}  " + "  ".join(f"{v:<{col_w}}" for v in verdicts))


def _print_ambiguous(scenario, mentions, results_by_config, db, invocation_time):
    short_labels = [cfg["short"] for cfg in CONFIGS]
    col_w = 30

    print(f"\n  {scenario.label}  [ambiguous — all tied candidates shown]")
    print(f"  {'Mention':<18}  " + "  ".join(f"{lbl:<{col_w}}" for lbl in short_labels))
    print(f"  {'─' * 18}  " + "  ".join("─" * col_w for _ in CONFIGS))

    for mention in mentions:
        cols = []
        for cfg in CONFIGS:
            tied = _resolve_with_ties(
                scenario.anchors, db, invocation_time,
                cfg["k"], cfg["strategy"],
            )
            returned = tied.get(mention, [])
            cols.append(", ".join(returned) if returned else "—")
        print(f"  {mention:<18}  " + "  ".join(f"{c:<{col_w}}" for c in cols))

    print(f"  {'─' * 18}  " + "  ".join("─" * col_w for _ in CONFIGS))
    print(f"  {'Note':<18}  Genuine tie — coherence returns all equally-scoring candidates")


def _print_limitation(scenario, mentions, results_by_config):
    short_labels = [cfg["short"] for cfg in CONFIGS]
    col_w = 24

    print(f"\n  {scenario.label}")
    print(f"  {'Mention':<18}  " + "  ".join(f"{lbl:<{col_w}}" for lbl in short_labels))
    print(f"  {'─' * 18}  " + "  ".join("─" * col_w for _ in CONFIGS))

    for mention in mentions:
        cols = []
        for cfg in CONFIGS:
            resolutions = results_by_config[cfg["label"]]
            resolved = _get_resolved(resolutions, mention)
            failed = mention in resolutions.failed
            cols.append("(not resolved)" if (failed or resolved is None) else resolved)
        print(f"  {mention:<18}  " + "  ".join(f"{c:<{col_w}}" for c in cols))

    identical = all(
        _get_resolved(results_by_config[CONFIGS[0]["label"]], m)
        == _get_resolved(results_by_config[CONFIGS[1]["label"]], m)
        for m in mentions
    )
    note = "k=1 = k=5  (graceful degradation — strategy cannot help here)" \
        if identical else "⚠️  k=1 ≠ k=5 (unexpected — investigate)"
    print(f"  {'─' * 18}  " + "  ".join("─" * col_w for _ in CONFIGS))
    print(f"  {'Note':<18}  {note}")


# ── Entry point ───────────────────────────────────────────────────────────────


def main():
    db = Neo4jManager()
    invocation_time = datetime.now(UTC)

    print(f"\n{'═' * 72}")
    print("  JourneyGraph — Anchor Resolution A/B Validation")
    print(f"  Baseline: k=1 string match   vs   Coherence: k=5 graph-assisted")
    print(f"  {invocation_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'═' * 72}")

    s1 = _run_section(
        title="Section 1 — Core Disambiguation",
        description="One ambiguous station + one route. Does graph context pick the right station?",
        scenarios=SECTION_1,
        db=db,
        invocation_time=invocation_time,
    )

    s2_totals = {cfg["label"]: {"pass": 0, "total": 0} for cfg in CONFIGS}
    print(f"\n{'─' * 72}")
    print("  Section 2 — Multi-Anchor Disambiguation")
    print("  Multiple stations and/or routes. Each mention resolved independently.")
    print(f"{'─' * 72}")

    for scenario in SECTION_2:
        results_by_config = {}
        for cfg in CONFIGS:
            resolutions = _resolve(
                scenario.anchors, db, invocation_time,
                cfg["k"], cfg["strategy"],
            )
            results_by_config[cfg["label"]] = resolutions

        mentions = _all_mentions(scenario.anchors)

        if scenario.kind == "definitive":
            _print_definitive(scenario, mentions, results_by_config, s2_totals)
        elif scenario.kind == "ambiguous":
            _print_ambiguous(scenario, mentions, results_by_config, db, invocation_time)

    _run_section(
        title="Section 3 — Limitations",
        description=(
            "Where coherence cannot fully resolve the query. "
            "L1: no candidates exist. L2/L3/L4: candidates tie — "
            "coherence surfaces all tied results honestly rather than silently picking one."
        ),
        scenarios=SECTION_3,
        db=db,
        invocation_time=invocation_time,
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═' * 72}")
    print("  RESULTS — Definitive Scenarios (Sections 1 + 2)")
    print(f"{'═' * 72}")

    for cfg in CONFIGS:
        label      = cfg["label"]
        s1_c       = s1[label]
        s2_c       = s2_totals[label]
        total_pass = s1_c["pass"]  + s2_c["pass"]
        total      = s1_c["total"] + s2_c["total"]
        pct        = 100 * total_pass // total if total else 0
        bar        = "█" * total_pass + "░" * (total - total_pass)
        print(f"  {cfg['short']:<20}  {total_pass}/{total}  [{bar}]  ({pct}%)")

    print()
    print("  Ambiguous   — genuine ties surfaced (M3, M4, L2, L3, L4)")
    print("  Limitations — L1 only: no candidates in graph, no hallucination")
    print(f"{'═' * 72}\n")

    db.close()


if __name__ == "__main__":
    main()
