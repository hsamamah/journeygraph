# src/common/cross_layer.py
"""
Cross-layer relationship warning utility.

Before creating relationships that target nodes from another layer,
call check_target_nodes() to log a warning if those nodes don't exist.

Usage in a layer's load.py:

    from src.common.cross_layer import check_target_nodes

    # Before wiring Route -[:SERVES]-> Station
    if check_target_nodes(neo4j, "Station", "service → physical"):
        neo4j.batch_write(cypher, rows, ...)

    # Returns True if nodes exist, False (with warning) if not.
    # The calling code can choose to skip or proceed (MATCH returns 0 rows).
"""

from src.common.logger import get_logger

log = get_logger(__name__)


def check_target_nodes(
    neo4j,
    label: str,
    context: str,
) -> bool:
    """
    Check whether any nodes with the given label exist in Neo4j.

    Args:
        neo4j:   Neo4jManager instance
        label:   Node label to check (e.g. 'Station', 'Platform', 'Trip')
        context: Human-readable context for the log message
                 (e.g. 'service → physical', 'interruption → service')

    Returns:
        True if at least one node exists, False otherwise.
        Logs a warning on False.
    """
    result = neo4j.query(f"MATCH (n:{label}) RETURN 1 LIMIT 1")

    if not result:
        log.warning(
            "⚠️  %s: no :%s nodes found — cross-layer relationships will "
            "be empty. Load the owning layer first to populate them.",
            context,
            label,
        )
        return False
    return True
