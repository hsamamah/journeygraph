"""
neo4j_tools.py — Neo4j driver wrapper.

Uses config.py for credentials (no scattered os.getenv calls).
Supports context manager so the driver is always closed cleanly.

Usage:
    with Neo4jManager() as db:
        results = db.query("MATCH (n) RETURN n LIMIT 5")
"""

from neo4j import GraphDatabase
from src.common.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


class Neo4jManager:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    def close(self):
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # don't suppress exceptions

    def query(self, cypher: str, parameters: dict = None):
        """Run a Cypher query and return results as a list of dicts."""
        with self.driver.session() as session:
            return session.run(cypher, parameters or {}).data()

    def execute_write(self, cypher: str, parameters: dict = None):
        """Run a write query inside an explicit transaction."""
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(cypher, parameters or {}))
