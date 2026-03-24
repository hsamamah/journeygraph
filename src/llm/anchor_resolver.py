"""
anchor_resolver.py — Stage 1 of the Subgraph Context Builder.

Converts string anchors extracted by the Planner into resolved graph node
identifiers. Four anchor types: stations, routes, dates, pathway_nodes.

Resolution strategies per type:
    stations     — fuzzy case-insensitive substring match on name.
                   On ambiguity, highest-degree node wins (degree restricted
                   to SERVES and SCHEDULED_AT relationships).
    routes       — two-pass: exact match on route_short_name, then substring
                   match on route_long_name. Rail line color names resolved
                   via RAIL_COLOR_TO_LINE before both passes.
    dates        — normalized to YYYYMMDD matching Date.date property.
                   Relative expressions resolved against invocation time.
    pathway_nodes — prefix-based routing to multi-label (:Pathway:Elevator
                   etc.). WMATA unit names resolved via two-tier
                   Accessibility layer join strategy.

Returns AnchorResolutions — a typed wrapper over the raw string→node_id
mapping consumed by Stage 2 hop expansion and surfaced in SubgraphOutput.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import logging
import re

from src.common.neo4j_tools import Neo4jManager
from src.llm.planner_output import PlannerAnchors

log = logging.getLogger(__name__)


# ── Rail line color → WMATA route_short_name ─────────────────────────────────
# Used in route anchor resolution before the two-pass name match.
# Source: WMATA network. See CONVENTIONS.md → Rail line identifiers.

RAIL_COLOR_TO_LINE: dict[str, str] = {
    "red": "RD",
    "blue": "BL",
    "orange": "OR",
    "silver": "SV",
    "green": "GR",
    "yellow": "YL",
}

# ── Pathway prefix → Pathway multi-label ─────────────────────────────────────
# NODE_ prefix embedded in id encodes equipment type.
# PathwayNode is no longer in the model — multi-labels live on :Pathway.

PATHWAY_PREFIX_TO_LABEL: dict[str, str] = {
    "NODE_ELE": "Elevator",
    "NODE_ELV": "Elevator",
    "NODE_ESC": "Escalator",
    "NODE_FG":  "FareGate",
    "NODE_MZ":  "Mezzanine",
    "NODE_STR": "Stairs",
}

# ── YYYYMMDD pattern ──────────────────────────────────────────────────────────

_YYYYMMDD_RE = re.compile(r"^\d{8}$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ── Output type ───────────────────────────────────────────────────────────────


@dataclass
class AnchorResolutions:
    resolved_stations: dict[str, str] = field(default_factory=dict)       # name → id
    resolved_routes: dict[str, str] = field(default_factory=dict)         # name → route_id
    resolved_dates: dict[str, str] = field(default_factory=dict)          # expr → YYYYMMDD
    resolved_pathway_nodes: dict[str, str] = field(default_factory=dict)  # name → id
    failed: dict[str, str] = field(default_factory=dict)                  # anchor → reason

    @property
    def any_resolved(self) -> bool:
        return any([
            self.resolved_stations,
            self.resolved_routes,
            self.resolved_dates,
            self.resolved_pathway_nodes,
        ])

    def as_flat_dict(self) -> dict[str, str]:
        """
        Flat merged view for SubgraphOutput.anchor_resolutions and pipeline trace.
        All four dicts merged — keys are original string anchors, values are node IDs.
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

    Args:
        db: Neo4jManager instance. Injected at construction — caller owns
            the connection lifecycle.
        invocation_time: Pipeline invocation datetime. Used to resolve
            relative date expressions (yesterday, last Tuesday, etc.).
            Defaults to datetime.utcnow() if not provided.
    """

    def __init__(
        self,
        db: Neo4jManager,
        invocation_time: datetime | None = None,
    ):
        self.db = db
        self.invocation_time = invocation_time or datetime.now(timezone.utc)

    # ── Public ────────────────────────────────────────────────────────────────

    def resolve(self, anchors: PlannerAnchors) -> AnchorResolutions:
        result = AnchorResolutions()

        for name in anchors.stations:
            self._resolve_station(name, result)

        for name in anchors.routes:
            self._resolve_route(name, result)

        for expr in anchors.dates:
            self._resolve_date(expr, result)

        for name in anchors.pathway_nodes:
            self._resolve_pathway_node(name, result)

        if not result.any_resolved:
            log.warning("anchor_resolver | zero anchors resolved | anchors=%s", anchors)

        return result

    # ── Station resolution ────────────────────────────────────────────────────

    def _resolve_station(self, name: str, result: AnchorResolutions) -> None:
        """
        Fuzzy case-insensitive substring match on name.
        On ambiguity, the node with the highest degree across SERVES and
        SCHEDULED_AT relationships wins.
        """
        rows = self.db.query(
            """
            MATCH (s:Station)
            WHERE toLower(s.name) CONTAINS toLower($name)
            WITH s
            OPTIONAL MATCH (s)-[r]-()
            WHERE type(r) IN ['SERVES', 'SCHEDULED_AT']
            RETURN s.id      AS id,
                   s.name    AS name,
                   count(r)  AS degree
            ORDER BY degree DESC
            LIMIT 1
            """,
            {"name": name},
        )

        if not rows:
            log.warning("anchor_resolver | station not found | name=%s", name)
            result.failed[name] = f"No Station matched '{name}'"
            return

        row = rows[0]
        log.debug(
            "anchor_resolver | station resolved | '%s' → %s (%s, degree=%d)",
            name, row["id"], row["name"], row["degree"],
        )
        result.resolved_stations[name] = row["id"]

    # ── Route resolution ──────────────────────────────────────────────────────

    def _resolve_route(self, name: str, result: AnchorResolutions) -> None:
        """
        Three-step resolution:
          1. Color name → route_short_name via RAIL_COLOR_TO_LINE dict.
          2. Exact match on route_short_name (case-insensitive).
          3. Substring match on route_long_name (case-insensitive).
        First match wins.
        """
        # Step 1 — color name normalisation
        color_key = name.strip().lower()
        resolved_short_name = RAIL_COLOR_TO_LINE.get(color_key)
        if resolved_short_name:
            log.debug(
                "anchor_resolver | route color resolved | '%s' → short_name=%s",
                name, resolved_short_name,
            )

        # Step 2 — exact match on route_short_name
        candidate = resolved_short_name or name
        rows = self.db.query(
            """
            MATCH (r:Route)
            WHERE toLower(r.route_short_name) = toLower($candidate)
            RETURN r.route_id         AS route_id,
                   r.route_short_name AS short_name,
                   r.route_long_name  AS long_name
            LIMIT 1
            """,
            {"candidate": candidate},
        )

        if rows:
            row = rows[0]
            log.debug(
                "anchor_resolver | route resolved (exact short_name) | '%s' → %s",
                name, row["route_id"],
            )
            result.resolved_routes[name] = row["route_id"]
            return

        # Step 3 — substring match on route_long_name
        rows = self.db.query(
            """
            MATCH (r:Route)
            WHERE toLower(r.route_long_name) CONTAINS toLower($name)
            RETURN r.route_id         AS route_id,
                   r.route_short_name AS short_name,
                   r.route_long_name  AS long_name
            LIMIT 1
            """,
            {"name": name},
        )

        if rows:
            row = rows[0]
            log.debug(
                "anchor_resolver | route resolved (long_name substring) | '%s' → %s",
                name, row["route_id"],
            )
            result.resolved_routes[name] = row["route_id"]
            return

        log.warning("anchor_resolver | route not found | name=%s", name)
        result.failed[name] = f"No Route matched '{name}'"

    # ── Date resolution ───────────────────────────────────────────────────────

    def _resolve_date(self, expr: str, result: AnchorResolutions) -> None:
        """
        Normalizes a date expression to YYYYMMDD and verifies it exists
        as a Date node in the graph.

        Handles:
            YYYYMMDD         — passed through directly
            YYYY-MM-DD       — stripped of hyphens
            'today'          — resolved against invocation_time
            'yesterday'      — invocation_time - 1 day
            'last <weekday>' — most recent past occurrence of that weekday
        """
        normalized = self._normalize_date_expr(expr)
        if normalized is None:
            log.warning("anchor_resolver | date unresolvable | expr=%s", expr)
            result.failed[expr] = f"Could not parse date expression '{expr}'"
            return

        rows = self.db.query(
            "MATCH (d:Date {date: $date}) RETURN d.date AS date LIMIT 1",
            {"date": normalized},
        )

        if not rows:
            log.warning(
                "anchor_resolver | date not in graph | expr=%s normalized=%s",
                expr, normalized,
            )
            result.failed[expr] = (
                f"Date '{normalized}' resolved from '{expr}' not found in graph"
            )
            return

        log.debug("anchor_resolver | date resolved | '%s' → %s", expr, normalized)
        result.resolved_dates[expr] = normalized

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
                "Monday": 0, "Tuesday": 1, "Wednesday": 2,
                "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6,
            }
            if weekday_name in weekday_map:
                target_wd = weekday_map[weekday_name]
                days_back = (today.weekday() - target_wd) % 7
                if days_back == 0:
                    days_back = 7  # 'last Monday' when today is Monday → 7 days back
                return (today - timedelta(days=days_back)).strftime("%Y%m%d")

        return None

    # ── Pathway node resolution ───────────────────────────────────────────────

    def _resolve_pathway_node(self, name: str, result: AnchorResolutions) -> None:
        """
        Two-tier resolution matching the Accessibility layer join strategy.

        Tier 1 — programmatic: if name is a NODE_ prefixed id,
                 prefix-based routing to the correct Pathway multi-label.
                 Direct id lookup against the graph.

        Tier 2 — WMATA unit name (e.g. 'A01 Elevator 1'):
                 station code extracted from unit name, matched against
                 BELONGS_TO station relationship on Pathway nodes.
                 Complex interchange stations (Metro Center, Gallery Place,
                 L'Enfant Plaza, Fort Totten) require hand-curated lookup —
                 see static lookup tables in the Accessibility layer.
        """
        # Tier 1 — NODE_ prefixed id
        if name.upper().startswith("NODE_"):
            label = self._prefix_to_label(name)
            if label is None:
                log.warning("anchor_resolver | unknown NODE_ prefix | name=%s", name)
                result.failed[name] = f"Unrecognized NODE_ prefix in '{name}'"
                return

            rows = self.db.query(
                f"""
                MATCH (p:Pathway:{label} {{id: $id}})
                RETURN p.id AS id
                LIMIT 1
                """,
                {"id": name},
            )

            if rows:
                log.debug(
                    "anchor_resolver | pathway resolved (tier 1) | '%s' → %s",
                    name, rows[0]["id"],
                )
                result.resolved_pathway_nodes[name] = rows[0]["id"]
                return

            log.warning(
                "anchor_resolver | pathway tier 1 miss | name=%s label=%s",
                name, label,
            )
            result.failed[name] = f"Pathway node '{name}' not found in graph"
            return

        # Tier 2 — WMATA unit name (e.g. 'A01 Elevator 1')
        station_code = self._extract_station_code(name)
        if station_code is None:
            log.warning(
                "anchor_resolver | pathway unit name unresolvable | name=%s", name
            )
            result.failed[name] = (
                f"Could not extract station code from unit name '{name}'"
            )
            return

        rows = self.db.query(
            """
            MATCH (p:Pathway)-[:BELONGS_TO]->(s:Station)
            WHERE s.id CONTAINS $station_code
              AND toLower(p.name) CONTAINS toLower($unit_name)
            RETURN p.id AS id
            ORDER BY p.id
            LIMIT 1
            """,
            {"station_code": station_code, "unit_name": name},
        )

        if rows:
            log.debug(
                "anchor_resolver | pathway resolved (tier 2) | '%s' → %s",
                name, rows[0]["id"],
            )
            result.resolved_pathway_nodes[name] = rows[0]["id"]
            return

        log.warning(
            "anchor_resolver | pathway tier 2 miss | name=%s station_code=%s",
            name, station_code,
        )
        result.failed[name] = (
            f"Pathway node for unit name '{name}' not found "
            f"(station_code={station_code}). "
            "Complex interchange station may require static lookup table."
        )

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
