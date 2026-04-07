# src/llm/planner.py
"""
Planner — entry point for every natural language query in the LLM pipeline.

The Planner runs three stages and returns a PlannerOutput dataclass that all
downstream agents consume read-only.

Stage 1 — Rule-based domain classifier (pure Python, no I/O)
    Scores the query against per-domain signal vocabularies using normalized
    keyword matching with word-boundary regex. Normalization divides raw match
    count by distinct signals matched, preventing long queries from
    systematically outscoring short ones. Tiebreak by domain weight.
    Zero score across all domains → rejected immediately, no LLM call fired.

Stage 2 — Lightweight LLM call (fires only when Stage 1 succeeds)
    Single call handles both path selection (text2cypher | subgraph | both)
    and anchor entity extraction (stations, routes, dates, pathway nodes).
    Bundling avoids a second model call and a second point of failure.
    JSON parse failure: retry once with a corrective nudge. On second failure,
    degrade to text2cypher-only with empty PlannerAnchors and set parse_warning.
    The SliceRegistry is validated before Stage 2 fires — no LLM tokens are
    spent if the DB is misconfigured (C6 sequencing constraint).

Stage 3 — PlannerOutput assembly (pure Python, no I/O)
    Consolidates Stage 1 and Stage 2 results into the final PlannerOutput.
    schema_slice_key is kept separate from domain for future free-form routing.

Strict mode:
    Pass strict=True to promote SliceRegistry validation warnings to hard
    failures. Does not affect Stage 1 or Stage 2 behaviour. The SliceRegistry
    receives the same strict flag and enforces it at startup.

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

from dataclasses import dataclass
import json
import re
from typing import TYPE_CHECKING

from src.common.logger import get_logger
from src.llm.llm_factory import build_llm
from src.llm.planner_output import PlannerAnchors, PlannerOutput

if TYPE_CHECKING:
    from src.common.config import LLMConfig
    from src.llm.slice_registry import SliceRegistry

log = get_logger(__name__)


# ── Domain signal vocabularies ────────────────────────────────────────────────
# Per-domain keyword sets. Word-boundary regex compiled at module load —
# not per query — so there is no runtime compilation cost on the hot path.
#
# Tiebreak weights reflect subgraph traversal breadth:
#   delay_propagation traverses the most node types → weight 3
#   transfer_impact   mid-range traversal             → weight 2
#   accessibility     narrower, elevator-focused       → weight 1
#
# Edge case (documented in CONVENTIONS.md): a single matched signal always
# scores 1.0, which beats a domain with three matches scoring 0.67. This is
# intentional — a single strong signal like 'wheelchair' is a clean
# accessibility hit and should win.

_DOMAIN_SIGNALS: dict[str, set[str]] = {
    "delay_propagation": {
        "delay",
        "delayed",
        "late",
        "behind schedule",
        "propagate",
        "ripple",
        "downstream",
    },
    "transfer_impact": {
        "transfer",
        "interchange",
        "connection",
        "missed",
        "cancelled",
        "cancellation",
    },
    "accessibility": {
        "elevator",
        "escalator",
        "accessible",
        "wheelchair",
        "outage",
        "ada",
        "incident",
    },
}

_DOMAIN_WEIGHTS: dict[str, int] = {
    "delay_propagation": 3,
    "transfer_impact": 2,
    "accessibility": 1,
}

# Compile all signal patterns once at module load.
# re.escape handles multi-word signals ("behind schedule") safely.
# Prefix matching (leading \b only, no trailing \b) means each signal matches
# all morphological variants — "propagate" catches "propagating", "propagation",
# "propagates"; "delay" catches "delays", "delayed", "delaying"; "cancel" catches
# "cancelled", "cancellation", "cancellations". The leading \b still prevents
# false positives mid-word (e.g. "propagate" will not match "unpropagated").
_COMPILED_SIGNALS: dict[str, list[re.Pattern]] = {
    domain: [re.compile(r"\b" + re.escape(signal), re.IGNORECASE) for signal in signals]
    for domain, signals in _DOMAIN_SIGNALS.items()
}

# Explicit map from domain to schema slice key.
# 1:1 for current domains — the separation supports future free-form routing
# without collapsing domain and slice key into the same concept.
_SLICE_KEY_MAP: dict[str, str] = {
    "delay_propagation": "delay_propagation",
    "transfer_impact": "transfer_impact",
    "accessibility": "accessibility",
}

# Rejection message shown to users when Stage 1 scores zero across all domains.
_REJECTION_MESSAGE = (
    "JourneyGraph can currently answer questions about transfer point impact, "
    "accessibility-infrastructure correlation, and delay propagation. "
    "Try asking about cancelled trips at a specific station, elevator outages "
    "affecting accessible routes, or delay patterns on a given route."
)

# Stage 2 system prompt — fixed ~100 token cost per call.
# Domain-scoped anchor type list is injected at call time via {anchor_types}.
_STAGE2_SYSTEM_PROMPT = """You are a query routing agent for a transit knowledge graph.
Your job is to decide how to answer a user query and extract named entities from it.

Respond with a single valid JSON object — no preamble, no markdown, no explanation.
The JSON must have exactly these keys: path, anchors, path_reasoning, anchor_notes.

path values:
  "text2cypher" — specific counts, lookups, or binary questions
  "subgraph"    — explanations, correlations, or topological questions
  "both"        — questions with a precise part and an explanatory part

anchors must contain exactly these keys: {anchor_types}
Each anchor key maps to a list of strings extracted from the query (empty list if none found).

path_reasoning: one sentence explaining your path choice.
anchor_notes: one sentence noting any inference made (e.g. date resolution), or null."""

# Corrective nudge appended to the prompt on a retry after JSON parse failure.
_RETRY_NUDGE = "\n\nIMPORTANT: Your previous response could not be parsed as JSON. Return only a valid JSON object with no other text."


# ── Stage 1 scoring result ────────────────────────────────────────────────────


@dataclass
class _Stage1Result:
    """Internal result of Stage 1 domain classification."""

    domain: str | None  # winning domain, or None if all scores are zero
    scores: dict[str, float]  # normalized score per domain (for --verbose)
    rejected: bool


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
        # Circuit breaker: track Stage 2 JSON parse failures over a rolling
        # window. If the failure rate is high the LLM or API is likely degraded
        # and retrying is wasteful. _parse_attempts[i] is True on success.
        self._parse_attempts: list[bool] = []
        self._parse_window = 10  # rolling window size
        self._parse_fail_threshold = 0.6  # fraction to trigger warning
        log.debug(
            "Planner ready — provider=%s model=%s strict=%s",
            llm_config.llm_provider,
            llm_config.llm_model,
            strict,
        )

    # ── Public interface ──────────────────────────────────────────────────────

    def run(self, query: str) -> PlannerOutput:
        """
        Run the full three-stage Planner pipeline for a single query.

        Args:
            query: Raw natural language query string from the user.

        Returns:
            PlannerOutput. Always returns — never raises on query-level
            failures. Check output.rejected and output.parse_warning for
            degraded states.
        """
        log.debug("Planner.run — query: %r", query)

        # Stage 1: pure Python, no I/O
        stage1 = self._stage1_classify(query)
        if stage1.rejected:
            log.info("Planner rejected query — zero domain score")
            return self._build_rejected_output(stage1)

        # Stage 2: LLM call — only reached when Stage 1 succeeds
        anchors, path, path_reasoning, anchor_notes, parse_warning = self._stage2_llm(
            query, stage1.domain
        )

        # Stage 3: assembly
        return self._stage3_assemble(
            stage1, path, anchors, path_reasoning, anchor_notes, parse_warning
        )

    # ── Stage 1: rule-based domain classifier ────────────────────────────────

    def _stage1_classify(self, query: str) -> _Stage1Result:
        """
        Score the query against per-domain signal vocabularies.

        Normalization: raw_matches / distinct_signals_matched.
        A single strong signal scores 1.0 and beats three weak matches
        scoring 0.67. Tiebreak by domain weight.

        Returns _Stage1Result with domain=None and rejected=True when all
        normalized scores are zero.
        """
        scores: dict[str, float] = {}

        for domain, patterns in _COMPILED_SIGNALS.items():
            matched_signals = [p for p in patterns if p.search(query)]
            raw_count = len(matched_signals)
            if raw_count == 0:
                scores[domain] = 0.0
            else:
                scores[domain] = raw_count / len(matched_signals)

        log.debug("Stage 1 raw scores: %s", scores)

        if all(s == 0.0 for s in scores.values()):
            return _Stage1Result(domain=None, scores=scores, rejected=True)

        # Pick highest score; break ties by domain weight (higher = wins)
        winning_domain = max(
            scores,
            key=lambda d: (scores[d], _DOMAIN_WEIGHTS[d]),
        )
        return _Stage1Result(domain=winning_domain, scores=scores, rejected=False)

    # ── Stage 2: lightweight LLM call ────────────────────────────────────────

    def _stage2_llm(
        self, query: str, domain: str
    ) -> tuple[PlannerAnchors, str, str | None, str | None, str | None]:
        """
        Single LLM call for path selection and anchor extraction.

        Returns (anchors, path, path_reasoning, anchor_notes, parse_warning).
        On second JSON parse failure, degrades to text2cypher-only with
        empty PlannerAnchors and a parse_warning describing the failure.
        """
        anchor_types = '", "'.join(PlannerAnchors.__dataclass_fields__.keys())
        system_prompt = _STAGE2_SYSTEM_PROMPT.format(anchor_types=anchor_types)
        user_message = f'Domain: {domain}\nQuery: "{query}"'

        # Circuit breaker: skip retry if recent parse failure rate is high
        recent = self._parse_attempts[-self._parse_window :]
        circuit_open = (
            len(recent) >= self._parse_window
            and recent.count(False) / len(recent) >= self._parse_fail_threshold
        )
        if circuit_open:
            log.critical(
                "Stage 2 circuit breaker open — %.0f%% parse failures in last %d "
                "attempts; skipping retry to avoid wasting API tokens",
                recent.count(False) / len(recent) * 100,
                len(recent),
            )

        # Attempt 1
        raw = self._invoke_llm(system_prompt, user_message)
        parsed, error = _parse_json_response(raw)

        if parsed is None and not circuit_open:
            log.warning(
                "Stage 2 JSON parse failed on attempt 1 — retrying. Error: %s", error
            )
            # Attempt 2: append corrective nudge to user message
            raw = self._invoke_llm(system_prompt, user_message + _RETRY_NUDGE)
            parsed, error = _parse_json_response(raw)

        if parsed is None:
            self._parse_attempts.append(False)
            # Both attempts failed (or circuit open) — degrade gracefully
            warning = (
                f"Stage 2 JSON parse failed after retry — degrading to "
                f"text2cypher-only with empty anchors. "
                f"Last error: {error}. Last raw response: {raw!r:.120}"
            )
            log.warning(warning)
            return PlannerAnchors(), "text2cypher", None, None, warning

        self._parse_attempts.append(True)

        # Extract path — default to text2cypher on missing or invalid value
        path = parsed.get("path", "text2cypher")
        if path not in {"text2cypher", "subgraph", "both"}:
            log.warning(
                "Stage 2 returned unrecognised path '%s' — defaulting to text2cypher",
                path,
            )
            path = "text2cypher"

        anchors = _extract_anchors(parsed.get("anchors", {}))
        path_reasoning = parsed.get("path_reasoning") or None
        anchor_notes = parsed.get("anchor_notes") or None

        log.debug("Stage 2 complete — path=%s anchors=%s", path, anchors)
        return anchors, path, path_reasoning, anchor_notes, None

    def _invoke_llm(self, system_prompt: str, user_message: str) -> str:
        """Invoke the LLM and return the raw response string."""
        response = self._llm.invoke(
            user_message,
            system_instruction=system_prompt,
        )
        return response.content

    # ── Stage 3: PlannerOutput assembly ──────────────────────────────────────

    def _stage3_assemble(
        self,
        stage1: _Stage1Result,
        path: str,
        anchors: PlannerAnchors,
        path_reasoning: str | None,
        anchor_notes: str | None,
        parse_warning: str | None,
    ) -> PlannerOutput:
        """Assemble the final PlannerOutput from Stage 1 and Stage 2 results."""
        domain = stage1.domain
        schema_slice_key = _SLICE_KEY_MAP[domain]

        return PlannerOutput(
            domain=domain,
            path=path,
            anchors=anchors,
            schema_slice_key=schema_slice_key,
            rejected=False,
            rejection_message=None,
            path_reasoning=path_reasoning,
            anchor_notes=anchor_notes,
            parse_warning=parse_warning,
        )

    def _build_rejected_output(self, stage1: _Stage1Result) -> PlannerOutput:
        """Build a rejected PlannerOutput when Stage 1 scores zero."""
        return PlannerOutput(
            domain="",
            path="",
            anchors=PlannerAnchors(),
            schema_slice_key="",
            rejected=True,
            rejection_message=_REJECTION_MESSAGE,
            path_reasoning=None,
            anchor_notes=None,
            parse_warning=None,
        )

    # ── Diagnostic access for --verbose ──────────────────────────────────────

    def classify_only(self, query: str) -> _Stage1Result:
        """
        Run Stage 1 only and return the scoring result.
        Used by run.py --verbose to surface per-domain scores without
        re-running the full pipeline.
        """
        return self._stage1_classify(query)


# ── Module-level helpers ──────────────────────────────────────────────────────


def _parse_json_response(raw: str) -> tuple[dict | None, str | None]:
    """
    Parse a raw LLM response string as JSON.

    Strips markdown code fences if present (```json ... ``` or ``` ... ```)
    before parsing. Returns (parsed_dict, None) on success, (None, error_str)
    on failure.
    """
    cleaned = raw.strip()

    # Strip markdown fences — LLMs occasionally include them despite instructions
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        # Remove opening fence (```json or ```) and closing fence (```)
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
    Build a PlannerAnchors from the raw anchors dict returned by Stage 2.

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
