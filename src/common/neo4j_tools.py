# src/common/neo4j_tools.py
"""
neo4j_tools.py — Neo4j driver wrapper.

Supports two construction modes:
  1. Explicit: Neo4jManager(uri=..., user=..., password=...)
  2. Config:   Neo4jManager()  <- reads from .env via config.py

Usage:
    with Neo4jManager() as db:
        results = db.query("MATCH (n) RETURN n LIMIT 5")
"""

import logging

from neo4j import GraphDatabase

log = logging.getLogger(__name__)


class Neo4jManager:
    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ):
        if uri and user and password:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
        else:
            from src.common.config import get_config

            config = get_config()
            self.driver = GraphDatabase.driver(
                config.neo4j_uri,
                auth=(config.neo4j_user, config.neo4j_password),
            )

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

    def batch_write(
        self,
        cypher: str,
        rows: list[dict],
        *,
        batch_size: int = 5_000,
        label: str = "batch",
    ) -> int:
        """
        Execute a parameterised UNWIND in chunks with progress logging.
        Returns total rows written.
        """
        total = len(rows)
        if total == 0:
            return 0
        for i in range(0, total, batch_size):
            chunk = rows[i : i + batch_size]
            self.execute_write(cypher, parameters={"rows": chunk})
            done = min(i + batch_size, total)
            log.info("  %s: %d / %d", label, done, total)
        return total
