# src/common/validators/base.py
from dataclasses import dataclass, field


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
