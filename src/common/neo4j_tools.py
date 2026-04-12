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
from typing import Optional

import pandas as pd
from neo4j import GraphDatabase

log = logging.getLogger(__name__)


def df_to_rows(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of dicts, replacing NaN/NaT with None.

    Scalar (non-object) columns are converted via vectorised pandas C code.
    Object-dtype columns use a per-value fallback to handle non-scalar values
    (lists, arrays) that would raise on pd.isna().
    """
    def _to_none(v: object) -> object:
        try:
            return None if pd.isna(v) else v
        except (TypeError, ValueError):
            return v  # non-scalar — pass through unchanged

    obj_cols = [c for c in df.columns if df[c].dtype == object]
    scalar_cols = [c for c in df.columns if df[c].dtype != object]

    if not obj_cols:
        # All scalar: fully vectorised path
        return df.where(df.notna(), other=None).to_dict(orient="records")

    if not scalar_cols:
        # All object: fall back to per-value path (rare)
        return [
            {k: _to_none(v) for k, v in row.items()}
            for row in df.to_dict(orient="records")
        ]

    # Mixed: vectorise scalar columns, per-value for object columns
    out = df.copy()
    out[scalar_cols] = df[scalar_cols].where(df[scalar_cols].notna(), other=None)
    for col in obj_cols:
        out[col] = df[col].apply(_to_none)
    return out.to_dict(orient="records")


class Neo4jManager:
    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
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

    def query(self, cypher: str, parameters: dict | None = None):
        """Run a Cypher query and return results as a list of dicts."""
        with self.driver.session() as session:
            return session.run(cypher, parameters or {}).data()

    def execute_write(self, cypher: str, parameters: dict | None = None):
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
        # Reuse a single session for all chunks — session acquisition is not
        # free (pool locking, state reset) and doing it per-chunk multiplies
        # the overhead by total / batch_size (up to 800× for large datasets).
        with self.driver.session() as session:
            for i in range(0, total, batch_size):
                chunk = rows[i : i + batch_size]
                session.execute_write(lambda tx, c=chunk: tx.run(cypher, {"rows": c}))
                done = min(i + batch_size, total)
                log.info("  %s: %d / %d", label, done, total)
        return total
