"""
anchor_resolver.py — Anchor resolution for the JourneyGraph LLM pipeline.

Converts string anchors extracted by the Planner into resolved graph node
identifiers. Two-phase process:

    Phase 1 — Candidate generation
        Each anchor mention is looked up in the graph via full-text index,
        returning up to candidate_limit candidates ranked by string match
        score. candidate_limit=1 is the baseline (current behaviour); higher
        values enable graph-assisted disambiguation.

    Phase 2 — Disambiguation
        A DisambiguationStrategy selects one candidate per mention from the
        full candidate pool. When candidate_limit=1 the strategy call is
        short-circuited — there is nothing to choose and no extra graph query
        fires. When candidate_limit>1 the strategy uses cross-anchor graph
        structure to pick the most coherent set of candidates.

DisambiguationStrategy is a Protocol — any object implementing select() can
be passed as a strategy. TopKStrategy (default) takes the highest-scoring
candidate per mention. TypeWeightedCoherenceStrategy (in
disambiguation_strategies.py) uses typed relationship weights to score
candidates by their mutual connectivity across all anchor types in the query.

candidate_limit and strategy name are exposed via AnchorResolver.config for
pipeline trace and A/B testing.
"""

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
import logging
import re
from typing import Protocol, TYPE_CHECKING

from src.common.neo4j_tools import Neo4jManager
from src.llm.planner_output import PlannerAnchors

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)


# ── Pathway prefix → Pathway multi-label ─────────────────────────────────────
# NODE_ prefix embedded in id encodes equipment type.
# PathwayNode is no longer in the model — multi-labels live on :Pathway.

PATHWAY_PREFIX_TO_LABEL: dict[str, str] = {
    "NODE_ELE": "Elevator",
    "NODE_ELV": "Elevator",
    "NODE_ESC": "Escalator",
    "NODE_FG": "FareGate",
    "NODE_MZ": "Mezzanine",
    "NODE_STR": "Stairs",
}

# ── YYYYMMDD pattern ──────────────────────────────────────────────────────────

_YYYYMMDD_RE = re.compile(r"^\d{8}$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ── Candidate — intermediate type produced by generation, consumed by strategy


@dataclass
class Candidate:
    """
    A single resolution candidate for one anchor mention.
    Produced by Phase 1 (candidate generation).
    Consumed by Phase 2 (DisambiguationStrategy.select()).
    """

    node_id: str  # graph node ID (station id, route_id, YYYYMMDD, etc.)
    display_name: str  # human-readable label for logging and trace
    score: float  # full-text index score — higher = better string match
    element_id: str  # Neo4j elementId — used by graph-querying strategies
    anchor_type: str  # 'station' | 'route' | 'date' | 'pathway_node'


# ── DisambiguationStrategy Protocol ──────────────────────────────────────────


class DisambiguationStrategy(Protocol):
    """
    Selects one candidate per anchor mention from the full candidate pool,
    and exposes ties when multiple candidates score equally.

    Receives all candidates across all anchor types in a single call so
    cross-anchor coherence strategies can see the full query context.
    Single-anchor strategies ignore candidates from other mentions.

    Args:
        candidates: {mention: [Candidate, ...]} for all anchor types.
                    Mentions with zero candidates are excluded — the resolver
                    handles those as failures before calling the strategy.
        db:         Live Neo4j connection. Passed so graph-querying strategies
                    don't need their own connection. None for strategies that
                    don't require graph access.

    Returns (select):
        {mention: node_id} — one ID per mention.
        Mentions the strategy cannot resolve are omitted.

    Returns (select_with_ties):
        {mention: [node_id, ...]} — all equally-scoring candidates per mention.
        List of length 1 when unambiguous, >1 when tied.
    """

    def select(
        self,
        candidates: dict[str, list[Candidate]],
        db: "Neo4jManager | None",
    ) -> dict[str, str]: ...

    def select_with_ties(
        self,
        candidates: dict[str, list[Candidate]],
        db: "Neo4jManager | None",
    ) -> dict[str, list[str]]: ...


# ── TopKStrategy — default, no graph queries ──────────────────────────────────


class TopKStrategy:
    """
    Default strategy. Takes the highest-scoring candidate per mention.
    Equivalent to the original top-1 resolver behaviour — no graph queries.
    Used automatically when candidate_limit=1 (short-circuited in resolve())
    and as the fallback when no other strategy is provided.

    select_with_ties() returns all candidates sharing the top string score.
    In practice the full-text index rarely produces exact ties on score, so
    this usually returns a single-element list — but surfaces the tie honestly
    when it does occur.
    """

    def select(
        self,
        candidates: dict[str, list[Candidate]],
        db: "Neo4jManager | None",
    ) -> dict[str, str]:
        return {
            mention: cands[0].node_id for mention, cands in candidates.items() if cands
        }

    def select_with_ties(
        self,
        candidates: dict[str, list[Candidate]],
        db: "Neo4jManager | None",
    ) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {}
        for mention, cands in candidates.items():
            if not cands:
                continue
            top_score = cands[0].score
            result[mention] = [c.node_id for c in cands if c.score == top_score]
        return result


# ── Output type ───────────────────────────────────────────────────────────────


@dataclass
class AnchorResolutions:
    resolved_stations: dict[str, list[str]] = field(default_factory=dict)
    # name → [id, ...]  list of length 1 when unambiguous, >1 when tied
    resolved_routes: dict[str, list[str]] = field(default_factory=dict)
    # name → [route_id, ...]
    resolved_dates: dict[str, list[str]] = field(default_factory=dict)
    # expr → [YYYYMMDD, ...]  dates are deterministic so always length 1
    resolved_pathway_nodes: dict[str, list[str]] = field(default_factory=dict)
    # name → [id, ...]
    failed: dict[str, str] = field(default_factory=dict)
    # anchor → reason — mentions that produced zero candidates or were declined

    @property
    def any_resolved(self) -> bool:
        return any(
            [
                self.resolved_stations,
                self.resolved_routes,
                self.resolved_dates,
                self.resolved_pathway_nodes,
            ]
        )

    def as_flat_dict(self) -> dict[str, list[str]]:
        """
        Flat merged view across all four types.
        Keys are original string anchors, values are lists of resolved IDs.
        A list of length 1 is unambiguous. Length >1 means candidates tied
        and all are included — the caller decides how to use them.
        """
        return {
            **self.resolved_stations,
            **self.resolved_routes,
            **self.resolved_dates,
            **self.resolved_pathway_nodes,
        }


# ── AnchorResolver ────────────────────────────────────────────────────────────


class AnchorResolver:
    """
    Resolves all four anchor types for a single pipeline invocation.

    Two-phase: candidate generation (full-text index, up to candidate_limit
    results per mention) followed by disambiguation (strategy selects one
    candidate per mention from the full candidate pool).

    When candidate_limit=1 the strategy call is short-circuited — the single
    candidate is selected directly, no extra graph query fires, and behaviour
    is identical to the original resolver.

    Args:
        db:              Neo4jManager instance. Injected at construction —
                         caller owns the connection lifecycle.
        invocation_time: Pipeline invocation datetime. Used to resolve
                         relative date expressions (yesterday, last Tuesday).
        strategy:        DisambiguationStrategy to use. Defaults to
                         TopKStrategy.
        candidate_limit: Maximum candidates fetched per mention from the
                         full-text index. 1 = baseline (no disambiguation).
                         Higher values enable graph-assisted disambiguation.
    """

    def __init__(
        self,
        db: Neo4jManager,
        invocation_time: datetime | None = None,
        strategy: DisambiguationStrategy | None = None,
        candidate_limit: int = 1,
    ) -> None:
        self.db = db
        self.invocation_time = invocation_time or datetime.now(UTC)
        self._strategy = strategy or TopKStrategy()
        self._k = candidate_limit

    @property
    def config(self) -> dict:
        """
        Resolver configuration for pipeline trace and A/B testing.
        Carried through to SubgraphOutput.resolver_config.
        """
        return {
            "candidate_limit": self._k,
            "strategy": type(self._strategy).__name__,
        }

    # ── Public ────────────────────────────────────────────────────────────────

    def resolve(self, anchors: PlannerAnchors) -> AnchorResolutions:
        """
        Resolve all anchors. Phase 1 generates candidates; Phase 2 runs the
        disambiguation strategy (or short-circuits when candidate_limit=1).
        """
        result = AnchorResolutions()

        # ── Phase 1: candidate generation ─────────────────────────────────────
        all_candidates: dict[str, list[Candidate]] = {}
        mention_to_type: dict[str, str] = {}

        for name in anchors.stations:
            cands = self._fetch_station_candidates(name)
            if cands:
                all_candidates[name] = cands
                mention_to_type[name] = "station"
            else:
                result.failed[name] = f"No Station matched '{name}'"

        for name in anchors.routes:
            cands = self._fetch_route_candidates(name)
            if cands:
                all_candidates[name] = cands
                mention_to_type[name] = "route"
            else:
                result.failed[name] = f"No Route matched '{name}'"

        for expr in anchors.dates:
            cands = self._fetch_date_candidates(expr)
            if cands:
                all_candidates[expr] = cands
                mention_to_type[expr] = "date"
            else:
                result.failed[expr] = f"Could not resolve date '{expr}'"

        for name in anchors.pathway_nodes:
            cands = self._fetch_pathway_candidates(name)
            if cands:
                all_candidates[name] = cands
                mention_to_type[name] = "pathway_node"
            else:
                result.failed[name] = f"No Pathway node matched '{name}'"

        if not all_candidates:
            log.warning(
                "anchor_resolver | zero candidates generated | anchors=%s", anchors
            )
            return result

        # ── Phase 2: disambiguation ────────────────────────────────────────────
        if self._k == 1:
            # Short-circuit: single candidate per mention, strategy irrelevant.
            log.info(
                "anchor_resolver | k=1 baseline | disambiguation skipped | strategy=%s",
                type(self._strategy).__name__,
            )
            selected: dict[str, list[str]] = {
                mention: [cands[0].node_id] for mention, cands in all_candidates.items()
            }
        else:
            log.info(
                "anchor_resolver | k=%d | strategy=%s | mentions=%d",
                self._k,
                type(self._strategy).__name__,
                len(all_candidates),
            )
            selected = self._strategy.select_with_ties(all_candidates, self.db)

        # Map selected node_id lists back into typed AnchorResolutions dicts
        for mention, node_ids in selected.items():
            anchor_type = mention_to_type.get(mention)
            tied = len(node_ids) > 1
            if anchor_type == "station":
                result.resolved_stations[mention] = node_ids
                log.info(
                    "anchor_resolver | station resolved | '%s' → %s%s",
                    mention,
                    node_ids,
                    " (tied)" if tied else "",
                )
            elif anchor_type == "route":
                result.resolved_routes[mention] = node_ids
                log.info(
                    "anchor_resolver | route resolved | '%s' → %s%s",
                    mention,
                    node_ids,
                    " (tied)" if tied else "",
                )
            elif anchor_type == "date":
                result.resolved_dates[mention] = node_ids
                log.info(
                    "anchor_resolver | date resolved | '%s' → %s",
                    mention,
                    node_ids,
                )
            elif anchor_type == "pathway_node":
                result.resolved_pathway_nodes[mention] = node_ids
                log.info(
                    "anchor_resolver | pathway resolved | '%s' → %s%s",
                    mention,
                    node_ids,
                    " (tied)" if tied else "",
                )

        # Record mentions the strategy declined to resolve
        for mention in all_candidates:
            if mention not in selected:
                result.failed[mention] = (
                    f"Strategy '{type(self._strategy).__name__}' "
                    f"could not disambiguate '{mention}'"
                )

        if not result.any_resolved:
            log.warning("anchor_resolver | zero anchors resolved | anchors=%s", anchors)

        return result

    # ── Station candidate generation ──────────────────────────────────────────

    def _fetch_station_candidates(self, name: str) -> list[Candidate]:
        """
        Full-text index lookup for stations. Returns up to self._k candidates
        ranked by string match score, then by degree on SERVES/SCHEDULED_AT
        as a tiebreaker within equal scores.
        """
        clean_name = re.sub(r'([+\-&|!(){}\[\]^"~*?:\\/])', r"\\\1", name)

        rows = self.db.query(
            """
            CALL db.index.fulltext.queryNodes("physical_station_name", $query) YIELD node, score
            RETURN node.id        AS id,
                   node.name      AS name,
                   score,
                   elementId(node) AS element_id,
                   size((node)-[:SERVES]-()) AS degree
            ORDER BY score DESC, degree DESC
            LIMIT $k
            """,
            {"query": f"*{clean_name}*", "k": self._k},
        )

        if not rows:
            log.warning("anchor_resolver | station not found | name=%s", name)

        return [
            Candidate(
                node_id=row["id"],
                display_name=row["name"],
                score=row["score"],
                element_id=row["element_id"],
                anchor_type="station",
            )
            for row in rows
        ]

    # ── Route candidate generation ────────────────────────────────────────────

    def _fetch_route_candidates(self, name: str) -> list[Candidate]:
        """
        Full-text index lookup for routes. Returns up to self._k candidates.
        Single-pass: handles short name exact match and long name substring.
        """
        clean_name = re.sub(r'([+\-&|!(){}\[\]^"~*?:\\/])', r"\\\1", name)
        lucene_query = (
            f'route_short_name:"{clean_name}" OR route_long_name:*{clean_name}*'
        )

        rows = self.db.query(
            """
            CALL db.index.fulltext.queryNodes("physical_route_name", $query) YIELD node, score
            RETURN node.route_id         AS route_id,
                   node.route_short_name AS short_name,
                   node.route_long_name  AS long_name,
                   score,
                   elementId(node)       AS element_id
            ORDER BY score DESC
            LIMIT $k
            """,
            {"query": lucene_query, "k": self._k},
        )

        if not rows:
            log.warning("anchor_resolver | route not found | name=%s", name)

        return [
            Candidate(
                node_id=row["route_id"],
                display_name=row["short_name"] or row["long_name"],
                score=row["score"],
                element_id=row["element_id"],
                anchor_type="route",
            )
            for row in rows
        ]

    # ── Date candidate generation ─────────────────────────────────────────────

    def _fetch_date_candidates(self, expr: str) -> list[Candidate]:
        """
        Date resolution is deterministic — returns 0 or 1 candidate.
        candidate_limit does not apply: there is nothing to disambiguate.
        Handles YYYYMMDD, YYYY-MM-DD, today, yesterday, last <weekday>.
        """
        normalized = self._normalize_date_expr(expr)
        if normalized is None:
            log.warning("anchor_resolver | date unresolvable | expr=%s", expr)
            return []

        rows = self.db.query(
            """
            MATCH (d:Date {date: $date})
            RETURN d.date AS date, elementId(d) AS element_id
            LIMIT 1
            """,
            {"date": normalized},
        )

        if not rows:
            log.warning(
                "anchor_resolver | date not in graph | expr=%s normalized=%s",
                expr,
                normalized,
            )
            return []

        return [
            Candidate(
                node_id=normalized,
                display_name=normalized,
                score=1.0,
                element_id=rows[0]["element_id"],
                anchor_type="date",
            )
        ]

    # ── Date normalization ────────────────────────────────────────────────────

    def _normalize_date_expr(self, expr: str) -> str | None:
        """
        Converts a date expression string to YYYYMMDD.
        Returns None if the expression cannot be parsed.
        """
        expr = expr.strip()
        today: date = self.invocation_time.date()

        if _YYYYMMDD_RE.match(expr):
            return expr

        if _ISO_DATE_RE.match(expr):
            return expr.replace("-", "")

        lower = expr.lower()

        if lower == "today":
            return today.strftime("%Y%m%d")

        if lower == "yesterday":
            return (today - timedelta(days=1)).strftime("%Y%m%d")

        # 'last <weekday>' — most recent past occurrence, not including today
        last_match = re.match(r"^last\s+(\w+)$", lower)
        if last_match:
            weekday_name = last_match.group(1).capitalize()
            weekday_map = {
                "Monday": 0,
                "Tuesday": 1,
                "Wednesday": 2,
                "Thursday": 3,
                "Friday": 4,
                "Saturday": 5,
                "Sunday": 6,
            }
            if weekday_name in weekday_map:
                target_wd = weekday_map[weekday_name]
                days_back = (today.weekday() - target_wd) % 7
                if days_back == 0:
                    days_back = 7  # 'last Monday' when today IS Monday → 7 days back
                return (today - timedelta(days=days_back)).strftime("%Y%m%d")

        return None

    # ── Pathway candidate generation ──────────────────────────────────────────

    def _fetch_pathway_candidates(self, name: str) -> list[Candidate]:
        """
        Two-tier pathway resolution matching the Accessibility layer strategy.

        Tier 1 — NODE_ prefixed id: direct lookup, deterministic.
        Tier 2 — WMATA unit name: station-code-scoped lookup, applies self._k.
        Complex interchange stations may require static lookup tables in the
        Accessibility layer.
        """
        # Tier 1 — NODE_ prefixed id
        if name.upper().startswith("NODE_"):
            label = self._prefix_to_label(name)
            if label is None:
                log.warning("anchor_resolver | unknown NODE_ prefix | name=%s", name)
                return []

            rows = self.db.query(
                f"""
                MATCH (p:Pathway:{label} {{id: $id}})
                RETURN p.id AS id, elementId(p) AS element_id
                LIMIT 1
                """,
                {"id": name},
            )

            if not rows:
                log.warning(
                    "anchor_resolver | pathway tier-1 miss | name=%s label=%s",
                    name,
                    label,
                )
                return []

            return [
                Candidate(
                    node_id=rows[0]["id"],
                    display_name=name,
                    score=1.0,
                    element_id=rows[0]["element_id"],
                    anchor_type="pathway_node",
                )
            ]

        # Tier 2 — WMATA unit name (e.g. 'A01 Elevator 1')
        station_code = self._extract_station_code(name)
        if station_code is None:
            log.warning(
                "anchor_resolver | pathway unit name unresolvable | name=%s", name
            )
            return []

        clean_name = re.sub(r'([+\-&|!(){}\[\]^"~*?:\\/])', r"\\\1", name)
        rows = self.db.query(
            """
            CALL db.index.fulltext.queryNodes("physical_pathway_name", $name_query) YIELD node AS p, score
            MATCH (p)-[:BELONGS_TO]->(s:Station)
            WHERE s.id STARTS WITH $station_code
            RETURN p.id AS id, elementId(p) AS element_id
            ORDER BY score DESC
            LIMIT $k
            """,
            {"name_query": f"*{clean_name}*", "station_code": station_code, "k": self._k},
        )

        if not rows:
            log.warning(
                "anchor_resolver | pathway tier-2 miss | name=%s station_code=%s",
                name,
                station_code,
            )
            return []

        return [
            Candidate(
                node_id=row["id"],
                display_name=name,
                score=1.0,
                element_id=row["element_id"],
                anchor_type="pathway_node",
            )
            for row in rows
        ]

    # ── Static helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _prefix_to_label(id: str) -> str | None:
        """Maps a NODE_ prefixed id to its Pathway multi-label."""
        upper = id.upper()
        for prefix, label in PATHWAY_PREFIX_TO_LABEL.items():
            if upper.startswith(prefix):
                return label
        return None

    @staticmethod
    def _extract_station_code(unit_name: str) -> str | None:
        """
        Extracts the WMATA station code from a unit name string.
        WMATA unit names follow the pattern '<StationCode> <Type> <Number>'
        e.g. 'A01 Elevator 1', 'C07 Escalator 3'.
        Station codes are one letter followed by two digits.
        """
        match = re.match(r"^([A-Za-z]\d{2})\b", unit_name.strip())
        return match.group(1).upper() if match else None
