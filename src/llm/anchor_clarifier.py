# src/llm/anchor_clarifier.py
"""
anchor_clarifier.py — LLM-assisted anchor repair for the JourneyGraph pipeline.

When AnchorResolver returns station or route failures (mentions in
resolutions.failed), AnchorClarifier makes a single small LLM call to map
each failed mention to the closest valid WMATA name, then re-runs resolution
on the corrected names.

This is a silent repair pass — transparent to the user. If the LLM call fails,
or the corrected name still does not resolve, the original failure is preserved
in AnchorResolutions.failed unchanged.

Only station and route failures are eligible. Date, pathway node, and level
failures are structural and cannot be fixed by name fuzzing.

Lifecycle:
    Constructed once at pipeline startup (catalogue fetched once from DB).
    clarify() is called per query only when resolutions.failed is non-empty
    and contains at least one station or route failure.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from src.llm.llm_factory import build_llm
from src.llm.planner_output import PlannerAnchors

if TYPE_CHECKING:
    from src.common.config import LLMConfig
    from src.common.neo4j_tools import Neo4jManager
    from src.llm.anchor_resolver import AnchorResolutions, AnchorResolver

log = logging.getLogger(__name__)

# ── Prompts ───────────────────────────────────────────────────────────────────
# System prompt is fixed (~60 tokens). User prompt adds the failed mentions
# and valid names catalogue (~150 tokens for the full WMATA network).
# Total prompt budget: ~250 tokens. Output is a small JSON object.

_SYSTEM_PROMPT = """\
You are a WMATA transit name matcher. Map each failed mention to the closest \
valid WMATA station or route name from the lists provided. \
Return ONLY a valid JSON object — no prose, no markdown.\
"""

_USER_PROMPT_TEMPLATE = """\
These transit names could not be resolved:
{failed_block}

Valid WMATA station names:
{station_names}

Valid WMATA route short names:
{route_names}

For each failed name, return the best matching valid name, or null if no \
reasonable match exists. Use exactly this format:
{{"<failed mention>": "<corrected name or null>", ...}}

Only use names from the valid lists. Do not invent names.\
"""


class AnchorClarifier:
    """
    Silent LLM-assisted repair pass for failed anchor resolutions.

    Constructed once at pipeline startup. The valid-name catalogue (~100
    stations, ~10 routes) is fetched from the graph at construction time
    and cached in memory — no DB query per clarification call.

    One clarification attempt per query. If the corrected name still does
    not resolve, the failure is preserved in AnchorResolutions.failed.

    Args:
        db:         Neo4jManager. Injected at construction; caller owns lifecycle.
        llm_config: LLMConfig used to build the clarification LLM instance.
                    Uses max_tokens=256 — output is a small JSON object.
    """

    def __init__(self, db: Neo4jManager, llm_config: LLMConfig) -> None:
        self._llm = build_llm(llm_config, max_tokens=256)
        self._station_names, self._route_names = self._fetch_catalogue(db)
        log.info(
            "anchor_clarifier | catalogue loaded | stations=%d routes=%d",
            len(self._station_names),
            len(self._route_names),
        )

    # ── Public ────────────────────────────────────────────────────────────────

    def clarify(
        self,
        resolutions: AnchorResolutions,
        resolver: AnchorResolver,
    ) -> AnchorResolutions:
        """
        Attempt to repair station and route failures in resolutions.failed.

        Modifies resolutions in place: successfully clarified mentions are
        moved from .failed into .resolved_stations or .resolved_routes.
        Mentions that still fail after clarification remain in .failed.

        Args:
            resolutions: AnchorResolutions from the initial resolver.resolve()
                         call. Modified in place.
            resolver:    The same AnchorResolver instance used for the initial
                         pass — ensures invocation_time, strategy, and
                         candidate_limit are consistent on re-resolution.

        Returns:
            The same AnchorResolutions object, mutated.
        """
        station_failures, route_failures = self._partition_failures(resolutions.failed)
        if not station_failures and not route_failures:
            return resolutions

        all_failures = station_failures + route_failures
        log.info(
            "anchor_clarifier | clarifying %d failed mention(s) | %s",
            len(all_failures),
            all_failures,
        )

        corrections = self._call_llm(all_failures)
        if not corrections:
            return resolutions

        corrected_stations = [
            corrections[m]
            for m in station_failures
            if corrections.get(m) and corrections[m] != "null"
        ]
        corrected_routes = [
            corrections[m]
            for m in route_failures
            if corrections.get(m) and corrections[m] != "null"
        ]

        if not corrected_stations and not corrected_routes:
            log.info("anchor_clarifier | no usable corrections from LLM")
            return resolutions

        synthetic = PlannerAnchors(stations=corrected_stations, routes=corrected_routes)
        new_resolutions = resolver.resolve(synthetic)

        self._merge(resolutions, new_resolutions, corrections, station_failures, route_failures)
        return resolutions

    # ── Private ───────────────────────────────────────────────────────────────

    def _fetch_catalogue(
        self, db: Neo4jManager
    ) -> tuple[list[str], list[str]]:
        """Fetch all station names and route short names from the graph."""
        station_rows = db.query(
            "MATCH (s:Station) RETURN s.name AS name ORDER BY s.name"
        )
        station_names = [r["name"] for r in station_rows if r.get("name")]

        route_rows = db.query(
            "MATCH (r:Route) RETURN r.route_short_name AS name ORDER BY r.route_short_name"
        )
        route_names = [r["name"] for r in route_rows if r.get("name")]

        return station_names, route_names

    def _partition_failures(
        self, failed: dict[str, str]
    ) -> tuple[list[str], list[str]]:
        """
        Split failed mentions into station failures and route failures.

        Keyed on the reason strings written by AnchorResolver:
            "No Station matched 'X'" → station failure
            "No Route matched 'X'"   → route failure
        Other failures (date, pathway, level, strategy) are excluded.
        """
        station_failures = [m for m, reason in failed.items() if "Station" in reason]
        route_failures = [m for m, reason in failed.items() if "Route" in reason]
        return station_failures, route_failures

    def _call_llm(self, mentions: list[str]) -> dict[str, str] | None:
        """Call the LLM with the failed mentions and catalogue. Returns parsed
        JSON dict on success, None on LLM error or parse failure."""
        failed_block = "\n".join(f"- {m}" for m in mentions)
        user_prompt = _USER_PROMPT_TEMPLATE.format(
            failed_block=failed_block,
            station_names="\n".join(self._station_names),
            route_names="\n".join(self._route_names),
        )
        full_prompt = f"{_SYSTEM_PROMPT}\n\n{user_prompt}"

        try:
            response = self._llm.invoke(full_prompt)
            text = response.content if hasattr(response, "content") else str(response)
        except Exception as exc:
            log.warning(
                "anchor_clarifier | LLM call failed | %s: %s", type(exc).__name__, exc
            )
            return None

        cleaned = text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.splitlines()
            cleaned = "\n".join(lines[1:-1]).strip()

        try:
            result = json.loads(cleaned)
        except json.JSONDecodeError:
            log.warning(
                "anchor_clarifier | LLM response not valid JSON | %r", text[:200]
            )
            return None

        log.debug("anchor_clarifier | corrections | %s", result)
        return result

    def _merge(
        self,
        resolutions: AnchorResolutions,
        new_resolutions: AnchorResolutions,
        corrections: dict[str, str],
        station_failures: list[str],
        route_failures: list[str],
    ) -> None:
        """
        Merge newly resolved anchors into existing resolutions.

        The original failed mention is used as the key in resolved_* dicts
        (not the corrected name) so downstream consumers see keys that match
        the original user query.
        """
        for original in station_failures:
            corrected = corrections.get(original)
            if corrected and corrected in new_resolutions.resolved_stations:
                resolutions.resolved_stations[original] = (
                    new_resolutions.resolved_stations[corrected]
                )
                del resolutions.failed[original]
                log.info(
                    "anchor_clarifier | station clarified | '%s' → '%s' → %s",
                    original,
                    corrected,
                    resolutions.resolved_stations[original],
                )

        for original in route_failures:
            corrected = corrections.get(original)
            if corrected and corrected in new_resolutions.resolved_routes:
                resolutions.resolved_routes[original] = (
                    new_resolutions.resolved_routes[corrected]
                )
                del resolutions.failed[original]
                log.info(
                    "anchor_clarifier | route clarified | '%s' → '%s' → %s",
                    original,
                    corrected,
                    resolutions.resolved_routes[original],
                )
