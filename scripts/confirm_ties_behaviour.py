"""
Confirm AnchorResolutions behaviour for Farragut with no route context.

Shows:
    resolved_stations — list of IDs per mention (length >1 when tied)
    node_constraints  — flat list, tied candidates included automatically

Run:
    uv run python -m scripts.confirm_ties_behaviour
"""

from datetime import UTC, datetime

from src.common.neo4j_tools import Neo4jManager
from src.llm.anchor_resolver import AnchorResolver
from src.llm.disambiguation_strategies import TypeWeightedCoherenceStrategy
from src.llm.planner_output import PlannerAnchors


def main():
    db = Neo4jManager()
    invocation_time = datetime.now(UTC)

    resolver = AnchorResolver(
        db=db,
        invocation_time=invocation_time,
        strategy=TypeWeightedCoherenceStrategy(),
        candidate_limit=5,
    )

    # Farragut with no route — both stations score 0.0, tie expected
    anchors = PlannerAnchors(stations=["Farragut"])
    resolutions = resolver.resolve(anchors)

    print("\n── resolved_stations ──────────────────────────────────────────")
    print(resolutions.resolved_stations)
    # Expected: {'Farragut': ['STN_C03', 'STN_A02']}  ← both IDs, tie included

    print("\n── node_constraints (no merge step needed) ────────────────────")
    node_constraints = {
        "station_ids": [
            id for ids in resolutions.resolved_stations.values() for id in ids
        ],
    }
    print(node_constraints)
    # Expected: {'station_ids': ['STN_C03', 'STN_A02']}

    print("\n── unambiguous check (Metro Center + Red Line) ────────────────")
    anchors2 = PlannerAnchors(stations=["Metro Center"], routes=["Red Line"])
    resolutions2 = resolver.resolve(anchors2)
    print("resolved_stations:", resolutions2.resolved_stations)
    print("resolved_routes:  ", resolutions2.resolved_routes)
    # Expected: {'Metro Center': ['STN_A01_C01']}  ← single-element list
    #           {'Red Line': ['RED']}

    db.close()


if __name__ == "__main__":
    main()
