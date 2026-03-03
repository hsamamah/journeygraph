"""
layers/fare/load.py

Writes transformed fare data to Neo4j using Neo4jManager.
"""

from src.common.logger import get_logger
from src.common.neo4j_tools import Neo4jManager

logger = get_logger(__name__)


def run(transformed: dict):
    """
    Load fare nodes and relationships into Neo4j.

    Args:
        transformed: Output of fare transform.run()
    """
    raise NotImplementedError("Fare load not yet implemented.")
