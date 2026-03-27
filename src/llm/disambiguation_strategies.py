# src/llm/disambiguation_strategies.py
"""
disambiguation_strategies.py — Plug-in disambiguation strategies for AnchorResolver.

Contains strategies beyond the default TopKStrategy, which lives in
anchor_resolver.py alongside the DisambiguationStrategy protocol.

All strategies implement the same protocol:
    select(candidates, db) -> {mention: node_id}

Swap strategies by passing an instance to AnchorResolver(strategy=...).
candidate_limit controls how many candidates are generated per mention —
strategies only run when candidate_limit > 1.
"""

import logging
from typing import TYPE_CHECKING

from src.llm.anchor_resolver import Candidate

if TYPE_CHECKING:
    from src.common.neo4j_tools import Neo4jManager

log = logging.getLogger(__name__)


class TypeWeightedCoherenceStrategy:
    """
    Selects candidates by maximizing typed-relationship coherence across all
    anchor types in the query.

    A candidate scores higher when it shares semantically significant
    relationship types with candidates from other anchor mentions. The score
    for a candidate is the sum of weights of edges connecting it to any
    candidate from a different mention.

    Single-candidate mentions pass through directly — no graph query fires
    for unambiguous anchors. The graph query runs only when at least one
    mention has multiple candidates.

    Same-type pairs (two station candidates, two route candidates) are
    excluded from scoring to prevent self-reinforcement. Coherence signal
    comes only from cross-type edges.

    Tiebreaker: when coherence scores are equal, the candidate with the
    higher full-text index score (string match quality) wins.

    Relationship weights reflect semantic specificity in JourneyGraph:
        SERVES / ON_ROUTE       — direct station-route membership
        BELONGS_TO              — station-pathway membership
        SCHEDULED_AT            — trip-platform scheduling
        AFFECTS_STOP / AFFECTS_TRIP — interruption-entity links
        Other                   — low-weight fallback, never excluded
    """

    _PAIR_WEIGHTS: dict[tuple[str, str, str], float] = {
        ("station",      "SERVES",       "route"):        1.0,
        ("route",        "SERVES",       "station"):      1.0,
        ("station",      "ON_ROUTE",     "route"):        0.9,
        ("route",        "ON_ROUTE",     "station"):      0.9,
        ("station",      "BELONGS_TO",   "pathway_node"): 0.9,
        ("pathway_node", "BELONGS_TO",   "station"):      0.9,
        ("station",      "SCHEDULED_AT", "route"):        0.7,
        ("station",      "AFFECTS_STOP", "station"):      0.6,
        ("station",      "AFFECTS_TRIP", "route"):        0.5,
    }
    _DEFAULT_WEIGHT: float = 0.1   # low but nonzero for unlisted cross-type edges

    def select(
        self,
        candidates: dict[str, list[Candidate]],
        db: "Neo4jManager | None",
    ) -> dict[str, str]:
        result: dict[str, str] = {}
        ambiguous: dict[str, list[Candidate]] = {}

        # Pass-through for unambiguous mentions — no scoring needed
        for mention, cands in candidates.items():
            if len(cands) == 1:
                result[mention] = cands[0].node_id
            else:
                ambiguous[mention] = cands

        if not ambiguous:
            return result

        if db is None:
            log.warning(
                "TypeWeightedCoherenceStrategy | db is None — "
                "falling back to TopK for %d ambiguous mention(s)",
                len(ambiguous),
            )
            for mention, cands in ambiguous.items():
                result[mention] = cands[0].node_id
            return result

        # Build element_id → Candidate lookup for weight calculation.
        # Include ALL candidates from ALL mentions — unambiguous single-candidate
        # mentions must be in the graph query as scoring anchors. Without them,
        # cross-type edges (e.g. Farragut candidates ↔ Red Line) are never found
        # and coherence scores stay at 0.
        eid_to_cand: dict[str, Candidate] = {
            c.element_id: c
            for cands in candidates.values()   # all mentions, not just ambiguous
            for c in cands
        }

        # Gather element IDs of all candidates across all mentions
        all_eids = list(eid_to_cand.keys())

        # Single round-trip: all edges between any pair of candidates
        rows = db.query(
            """
            MATCH (a)-[r]-(b)
            WHERE elementId(a) IN $eids
              AND elementId(b) IN $eids
              AND elementId(a) <> elementId(b)
            RETURN elementId(a) AS from_eid,
                   elementId(b) AS to_eid,
                   type(r)      AS rel_type
            """,
            {"eids": all_eids},
        )

        # Accumulate coherence scores per element_id
        scores: dict[str, float] = {eid: 0.0 for eid in all_eids}

        for row in rows:
            from_cand = eid_to_cand.get(row["from_eid"])
            to_cand = eid_to_cand.get(row["to_eid"])

            if from_cand is None or to_cand is None:
                continue

            # Exclude same-type pairs — no self-reinforcement
            if from_cand.anchor_type == to_cand.anchor_type:
                continue

            triple = (from_cand.anchor_type, row["rel_type"], to_cand.anchor_type)
            weight = self._PAIR_WEIGHTS.get(triple, self._DEFAULT_WEIGHT)

            scores[row["from_eid"]] += weight
            scores[row["to_eid"]] += weight

        log.debug(
            "TypeWeightedCoherenceStrategy | coherence scores | %s",
            {eid: round(s, 3) for eid, s in scores.items() if s > 0},
        )

        # Select best candidate per ambiguous mention
        # Primary key: coherence score. Tiebreak: full-text string score.
        for mention, cands in ambiguous.items():
            best = max(
                cands,
                key=lambda c: (scores.get(c.element_id, 0.0), c.score),
            )
            result[mention] = best.node_id
            log.info(
                "TypeWeightedCoherenceStrategy | resolved | '%s' → %s "
                "(coherence=%.3f string_score=%.3f)",
                mention,
                best.node_id,
                scores.get(best.element_id, 0.0),
                best.score,
            )

        return result
