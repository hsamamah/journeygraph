# src/llm/text2cypher_output.py
"""
Text2CypherOutput — output contract for the Text2Cypher path.

Status: stub. The Query Writer and Cypher Validator are implemented on a
separate branch. This dataclass defines the contract the Narration Agent
expects so that narration_agent.py can be implemented and tested now.

When Text2Cypher ships, this file is replaced in-place — the Narration Agent
and any tests that construct Text2CypherOutput will not need changes provided
the fields below remain stable.

Design notes:
    ValidationError carries structured failure detail so the Narration Agent
    can surface a meaningful degraded message rather than a raw exception.

    attempt_count tracks how many Query Writer / Cypher Validator retry
    cycles ran. Used in the pipeline trace and in the PRECISE RESULTS block
    header surfaced to the LLM.

    validation_notes is one entry per retry attempt (empty on first-pass
    success). Preserved in the output so the pipeline trace can show what
    the Validator objected to.

    success=False with error=None indicates the retry budget was exhausted
    without a structured validation error (e.g. the LLM stopped generating).
    The Narration Agent treats any success=False the same way regardless of
    whether error is populated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ValidationCheck(str, Enum):
    """
    Ordered set of checks the Cypher Validator applies.

    Using str-Enum so values serialize to plain strings in JSON/trace dicts
    and compare equal to their string equivalents (e.g. in tests).
    """

    SYNTAX = "syntax"
    LABEL_WHITELIST = "label_whitelist"
    REL_TYPE = "rel_type"
    REL_DIRECTION = "rel_direction"
    PROPERTY_NAMES = "property_names"


@dataclass
class ValidationError:
    """
    Structured error from the Cypher Validator.

    Attributes:
        check:          Which of the five ordered checks failed.
        detail:         Human-readable description of the failure e.g.
                        'Label :ServiceAlert not in slice for transfer_impact'
        violated_rule:  The exact slice entry or convention that was violated.
        cypher_excerpt: The offending clause from the generated Cypher, used
                        as a targeted retry hint for the Query Writer.
    """

    check: ValidationCheck
    detail: str
    violated_rule: str
    cypher_excerpt: str


@dataclass
class Text2CypherOutput:
    """
    Output contract from the Text2Cypher path (Query Writer + Cypher Validator).

    Produced by the Cypher Validator after a successful validation pass, or
    on retry exhaustion with success=False. Consumed read-only by the
    Narration Agent's Input Assembler.

    Attributes:
        cypher:           Final validated Cypher query (comments stripped).
                          Empty string when success=False.
        results:          Raw Neo4j result rows from query execution.
                          Empty list when success=False.
        domain:           Query domain — mirrors PlannerOutput.domain.
        attempt_count:    Number of Query Writer / Validator cycles completed.
                          Range: 1–3 (max 2 retries).
        validation_notes: One entry per failed attempt describing the
                          ValidationError. Empty on first-pass success.
        success:          False if retry budget exhausted or driver error.
        error:            Final ValidationError on retry exhaustion.
                          None on success or non-validation failure.
    """

    cypher: str
    results: list[dict]
    domain: str
    attempt_count: int
    validation_notes: list[str] = field(default_factory=list)
    success: bool = False
    error: ValidationError | None = None
