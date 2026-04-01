# src/common/validators/base.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.common.neo4j_tools import Neo4jManager


def run_count_check(neo4j_manager: "Neo4jManager", cypher: str) -> int:
    """
    Run a single COUNT query against Neo4j and return the integer result.
    Shared by all post-load validators to avoid duplicating session boilerplate.
    """
    with neo4j_manager.driver.session() as session:
        record = session.run(cypher).single()
        return record["n"] if record else 0


@dataclass
class ValidationResult:
    """
    Accumulates validation findings across pre- and post-load checks.
    A result with any errors is considered failed and should block the pipeline.
    Warnings are informational and non-blocking.
    """

    passed: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    info: list[str] = field(default_factory=list)

    def fail(self, msg: str) -> None:
        self.errors.append(msg)
        self.passed = False

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def note(self, msg: str) -> None:
        self.info.append(msg)

    def summary(self) -> str:
        lines: list[str] = []
        for e in self.errors:
            lines.append(f"  ❌  {e}")
        for w in self.warnings:
            lines.append(f"  ⚠️   {w}")
        for i in self.info:
            lines.append(f"  ℹ️   {i}")
        return "\n".join(lines) if lines else "  ✅  All checks passed"
