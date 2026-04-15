# src/llm/planner.py
"""
Planner — entry point for every natural language query in the LLM pipeline.

The Planner runs two stages and returns a PlannerOutput dataclass that all
downstream agents consume read-only.

Stage 1 — Single LLM call (domain classification + path routing + anchor extraction)
    One call handles domain classification (transfer_impact | delay_propagation |
    accessibility | null), path selection (text2cypher | subgraph | both), and
    anchor entity extraction (stations, routes, dates, pathway nodes, levels).
    Bundling avoids a two-call round-trip and ensures domain and routing decisions
    are made with the same context.
    null domain → rejected immediately with a rejection_reason from the LLM.
    JSON parse failure: retry once with a corrective nudge. On second failure,
    degrade to text2cypher-only with empty PlannerAnchors and set parse_warning.
    The SliceRegistry is validated before Stage 1 fires — no LLM tokens are
    spent if the DB is misconfigured (C6 sequencing constraint).

Stage 2 — PlannerOutput assembly (pure Python, no I/O)
    Consolidates Stage 1 results into the final PlannerOutput.
    schema_slice_key is kept separate from domain for future free-form routing.

Strict mode:
    Pass strict=True to promote SliceRegistry validation warnings to hard
    failures. Does not affect Stage 1 behaviour. The SliceRegistry receives
    the same strict flag and enforces it at startup.

Usage:
    with Neo4jManager() as db:
        registry = SliceRegistry(db, strict=False)

    llm_config = get_llm_config()
    planner = Planner(registry, llm_config)
    output = planner.run("how many trips were cancelled on the red line")

    if output.rejected:
        print(output.rejection_message)
    else:
        print(output.domain, output.path, output.anchors)
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import json
from typing import TYPE_CHECKING

from src.common.logger import get_logger
from src.llm.llm_factory import build_llm
from src.llm.planner_output import PlannerAnchors, PlannerOutput

if TYPE_CHECKING:
    from src.common.config import LLMConfig
    from src.llm.slice_registry import SliceRegistry

log = get_logger(__name__)


# ── Domain → schema slice key map ─────────────────────────────────────────────
# 1:1 for current domains — the separation supports future free-form routing
# without collapsing domain and slice key into the same concept.

_SLICE_KEY_MAP: dict[str, str] = {
    "delay_propagation": "delay_propagation",
    "transfer_impact": "transfer_impact",
    "accessibility": "accessibility",
}

_VALID_DOMAINS = frozenset(_SLICE_KEY_MAP.keys())
_VALID_PATHS = frozenset({"text2cypher", "subgraph", "both"})

# ── Stage 1 system prompt ─────────────────────────────────────────────────────
# Single LLM call: domain classification + path routing + anchor extraction.
# ~160 token fixed cost per call.

_STAGE1_SYSTEM_PROMPT = """\
You are a query router for a transit knowledge graph covering WMATA (Washington DC Metro).

You ONLY answer questions about these three domains:
  transfer_impact    — cancelled, skipped, or disrupted trips affecting connections or services
  delay_propagation  — delays spreading across routes, trips, or stops
  accessibility      — elevator/escalator outages, accessible pathway status, ADA infrastructure

Return a single valid JSON object — no preamble, no markdown, no explanation.
Required keys: domain, path, anchors, path_reasoning, anchor_notes, rejection_reason, use_gds

domain: "transfer_impact" | "delay_propagation" | "accessibility" | null
  Set null if the query is not about WMATA transit disruptions, accessibility, or delays.

path (set to null when domain is null):
  "text2cypher" — specific counts, lookups, binary yes/no questions, AND all graph algorithm
                  queries (shortest path, centrality, clustering, reachability). When use_gds
                  is true, always set path to "text2cypher".
  "subgraph"    — explanations or correlations answered by traversing existing graph edges
                  (e.g. "which routes serve this station", "what connects to X"). Do NOT use
                  subgraph for algorithm-based questions (centrality, path-finding, clustering).
  "both"        — questions combining a precise numerical part with an explanatory part

anchors (set to null when domain is null) — when provided, must have exactly these keys: {anchor_types}
  Each key maps to a list of strings extracted from the query (empty list if none found).
  stations:      named WMATA stations (e.g. "Metro Center", "Gallery Place", "Pentagon City")
  routes:        named lines or bus routes (e.g. "Red Line", "Yellow Line", "D80", "bus")
  dates:         time references in ISO format (YYYY-MM-DD), "today", "yesterday", or "last <weekday>".
                 Also accept vague expressions like "recently", "most recently", "now" — extract
                 them as-is; the resolver will handle them.
  pathway_nodes: ONLY specific unit IDs like "A01 Elevator 1" or "NODE_ELE_A01_01".
                 Do NOT extract generic type words like "elevator", "escalator", "stairs" —
                 those are covered by the station anchor. Leave this list empty if only a
                 generic equipment type is mentioned.
  levels:        named floor levels (e.g. "street level", "mezzanine", "platform level")

path_reasoning: one sentence explaining your path choice, or null if domain is null.
anchor_notes: one sentence noting any inference made (e.g. route resolved from corridor name), or null.
rejection_reason: one sentence explaining why the query is out of scope, or null if domain is not null.
use_gds: false (default — override only when the GDS section below is present and the query warrants it).\
"""

# Appended to the system prompt only when GDS is installed.
_GDS_PROMPT_ADDON = """

Graph Data Science (GDS) is available on this database.
Set use_gds: true AND path: "text2cypher" when the query implies graph algorithm reasoning such as:
  - Finding the shortest or fastest path between stations (Dijkstra, BFS)
  - Ranking stations by centrality (PageRank, betweenness, degree — "most important", "biggest choke point", "most direct connections")
  - Detecting communities or clusters of stations/routes (Louvain)
  - Identifying reachable stations within N transfers (BFS)
  - Identifying weakly or strongly connected components / isolated stations (WCC)

IMPORTANT: GDS questions are NEVER out-of-scope for WMATA. They ask about the transit network's
graph structure. Always set domain: "transfer_impact" for GDS network questions.
Always set path: "text2cypher" when use_gds: true — never "subgraph" or "both".

Do NOT set use_gds: true for plain lookups, counts, or filtering queries — those use standard Cypher.\
"""

# Corrective nudge appended to the prompt on a retry after JSON parse failure.
_RETRY_NUDGE = "\n\nIMPORTANT: Your previous response could not be parsed as JSON. Return only a valid JSON object with no other text."

# Fallback rejection message when Stage 1 parse fails completely.
_FALLBACK_REJECTION_MESSAGE = (
    "JourneyGraph can currently answer questions about transfer point impact, "
    "accessibility-infrastructure correlation, and delay propagation. "
    "Try asking about cancelled trips at a specific station, elevator outages "
    "affecting accessible routes, or delay patterns on a given route."
)


# ── Stage 1 result ─────────────────────────────────────────────────────────────


@dataclass
class _Stage1Result:
    """Internal result of the combined Stage 1 LLM call."""

    domain: str | None
    path: str | None
    anchors: PlannerAnchors | None
    path_reasoning: str | None
    anchor_notes: str | None
    rejection_reason: str | None
    parse_warning: str | None
    rejected: bool
    use_gds: bool = False


# ── Planner ───────────────────────────────────────────────────────────────────


class Planner:
    """
    Entry point for every natural language query in the LLM pipeline.

    The SliceRegistry is injected — DB validation happens once at registry
    construction, not per Planner instantiation or per run() call.
    The LLM instance is built once in __init__ and reused across run() calls.

    Args:
        slice_registry: Validated SliceRegistry from startup.
        llm_config:     Validated LLMConfig from get_llm_config().
        strict:         If True, promote SliceRegistry validation warnings
                        to hard failures. Passed through to SliceRegistry
                        at construction time.
    """

    def __init__(
        self,
        slice_registry: SliceRegistry,
        llm_config: LLMConfig,
        *,
        strict: bool = False,
    ) -> None:
        self._registry = slice_registry
        self._llm_config = llm_config
        self._strict = strict
        self._llm = build_llm(llm_config)
        # Circuit breaker: track Stage 1 JSON parse failures over a rolling
        # window. If the failure rate is high the LLM or API is likely degraded
        # and retrying is wasteful. deque(maxlen) enforces the rolling window
        # without manual trimming — entries beyond maxlen drop off automatically.
        self._parse_window = 10
        self._parse_attempts: deque[bool] = deque(maxlen=self._parse_window)
        self._parse_fail_threshold = 0.6
        self._gds_available: bool = slice_registry.gds_available
        log.debug(
            "Planner ready — provider=%s model=%s strict=%s gds=%s",
            llm_config.llm_provider,
            llm_config.llm_model,
            strict,
            self._gds_available,
        )

    # ── Public interface ──────────────────────────────────────────────────────

    def run(self, query: str) -> PlannerOutput:
        """
        Run the Planner pipeline for a single query.

        Args:
            query: Raw natural language query string from the user.

        Returns:
            PlannerOutput. Always returns — never raises on query-level
            failures. Check output.rejected and output.parse_warning for
            degraded states.
        """
        log.debug("Planner.run — query: %r", query)

        stage1 = self._stage1_llm(query)

        if stage1.rejected:
            log.info(
                "Planner rejected query — %s",
                stage1.rejection_reason or "domain=null",
            )
            return self._build_rejected_output(stage1)

        return self._stage2_assemble(stage1)

    # ── Stage 1: single LLM call ──────────────────────────────────────────────

    def _stage1_llm(self, query: str) -> _Stage1Result:
        """
        Single LLM call: domain classification + path routing + anchor extraction.

        Returns _Stage1Result. On second JSON parse failure (or circuit open),
        degrades to text2cypher-only with empty anchors and sets parse_warning.
        """
        anchor_types = '", "'.join(PlannerAnchors.__dataclass_fields__.keys())
        system_prompt = _STAGE1_SYSTEM_PROMPT.format(anchor_types=anchor_types)
        if self._gds_available:
            system_prompt += _GDS_PROMPT_ADDON
        user_message = f'Query: "{query}"'

        # Circuit breaker: skip retry if recent parse failure rate is high.
        # _parse_attempts is a deque(maxlen=_parse_window) — no manual slicing needed.
        circuit_open = (
            len(self._parse_attempts) >= self._parse_window
            and self._parse_attempts.count(False) / len(self._parse_attempts)
            >= self._parse_fail_threshold
        )
        if circuit_open:
            log.critical(
                "Stage 1 circuit breaker open — %.0f%% parse failures in last %d "
                "attempts; skipping retry to avoid wasting API tokens",
                self._parse_attempts.count(False) / len(self._parse_attempts) * 100,
                len(self._parse_attempts),
            )

        # Attempt 1
        raw = self._invoke_llm(system_prompt, user_message)
        parsed, error = _parse_json_response(raw)

        if parsed is None and not circuit_open:
            log.warning(
                "Stage 1 JSON parse failed on attempt 1 — retrying. Error: %s", error
            )
            raw = self._invoke_llm(system_prompt, user_message + _RETRY_NUDGE)
            parsed, error = _parse_json_response(raw)

        if parsed is None:
            self._parse_attempts.append(False)
            warning = (
                f"Stage 1 JSON parse failed after retry — rejecting query. "
                f"Last error: {error}. Last raw response: {raw!r:.120}"
            )
            log.warning(warning)
            return _Stage1Result(
                domain=None,
                path=None,
                anchors=None,
                path_reasoning=None,
                anchor_notes=None,
                rejection_reason=_FALLBACK_REJECTION_MESSAGE,
                parse_warning=warning,
                rejected=True,
            )

        self._parse_attempts.append(True)

        domain = parsed.get("domain")
        if domain is not None and domain not in _VALID_DOMAINS:
            log.warning(
                "Stage 1 returned unrecognised domain '%s' — treating as rejection",
                domain,
            )
            domain = None

        if domain is None:
            return _Stage1Result(
                domain=None,
                path=None,
                anchors=None,
                path_reasoning=None,
                anchor_notes=None,
                rejection_reason=parsed.get("rejection_reason") or None,
                parse_warning=None,
                rejected=True,
            )

        path = parsed.get("path", "text2cypher")
        if path not in _VALID_PATHS:
            log.warning(
                "Stage 1 returned unrecognised path '%s' — defaulting to text2cypher",
                path,
            )
            path = "text2cypher"

        raw_anchors = parsed.get("anchors") or {}
        anchors = _extract_anchors(raw_anchors)

        # Only honour use_gds when GDS is actually installed — guard against
        # the LLM hallucinating this flag when the prompt section was absent.
        use_gds = bool(parsed.get("use_gds")) and self._gds_available

        log.debug(
            "Stage 1 complete — domain=%s path=%s use_gds=%s anchors=%s",
            domain, path, use_gds, anchors,
        )
        return _Stage1Result(
            domain=domain,
            path=path,
            anchors=anchors,
            path_reasoning=parsed.get("path_reasoning") or None,
            anchor_notes=parsed.get("anchor_notes") or None,
            rejection_reason=None,
            parse_warning=None,
            rejected=False,
            use_gds=use_gds,
        )

    def _invoke_llm(self, system_prompt: str, user_message: str) -> str:
        """Invoke the LLM and return the raw response string."""
        response = self._llm.invoke(
            user_message,
            system_instruction=system_prompt,
        )
        return response.content

    # ── Stage 2: PlannerOutput assembly ──────────────────────────────────────

    def _stage2_assemble(self, stage1: _Stage1Result) -> PlannerOutput:
        """Assemble the final PlannerOutput from Stage 1 results."""
        domain = stage1.domain
        schema_slice_key = _SLICE_KEY_MAP[domain]

        return PlannerOutput(
            domain=domain,
            path=stage1.path,
            anchors=stage1.anchors,
            schema_slice_key=schema_slice_key,
            rejected=False,
            rejection_message=None,
            path_reasoning=stage1.path_reasoning,
            anchor_notes=stage1.anchor_notes,
            parse_warning=stage1.parse_warning,
            use_gds=stage1.use_gds,
        )

    def _build_rejected_output(self, stage1: _Stage1Result) -> PlannerOutput:
        """Build a rejected PlannerOutput when the LLM returns domain=null."""
        rejection_message = stage1.rejection_reason or _FALLBACK_REJECTION_MESSAGE
        return PlannerOutput(
            domain="",
            path="",
            anchors=PlannerAnchors(),
            schema_slice_key="",
            rejected=True,
            rejection_message=rejection_message,
            path_reasoning=None,
            anchor_notes=None,
            parse_warning=None,
        )


# ── Module-level helpers ──────────────────────────────────────────────────────


def _parse_json_response(raw: str) -> tuple[dict | None, str | None]:
    """
    Parse a raw LLM response string as JSON.

    Strips markdown code fences if present (```json ... ``` or ``` ... ```)
    before parsing. Returns (parsed_dict, None) on success, (None, error_str)
    on failure.
    """
    cleaned = raw.strip()

    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        start = 1 if lines[0].startswith("```") else 0
        end = len(lines) - 1 if lines[-1].strip() == "```" else len(lines)
        cleaned = "\n".join(lines[start:end]).strip()

    try:
        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            return None, f"Expected a JSON object, got {type(parsed).__name__}"
        return parsed, None
    except json.JSONDecodeError as exc:
        return None, str(exc)


def _extract_anchors(raw_anchors: dict) -> PlannerAnchors:
    """
    Build a PlannerAnchors from the raw anchors dict returned by Stage 1.

    Coerces each field to a list of strings. Non-list values and non-string
    list items are silently dropped — the LLM occasionally returns a string
    instead of a single-element list.
    """

    def _to_str_list(value) -> list[str]:
        if isinstance(value, list):
            return [str(v) for v in value if v is not None]
        if isinstance(value, str) and value:
            return [value]
        return []

    return PlannerAnchors(
        stations=_to_str_list(raw_anchors.get("stations")),
        routes=_to_str_list(raw_anchors.get("routes")),
        dates=_to_str_list(raw_anchors.get("dates")),
        pathway_nodes=_to_str_list(raw_anchors.get("pathway_nodes")),
        levels=_to_str_list(raw_anchors.get("levels")),
    )
