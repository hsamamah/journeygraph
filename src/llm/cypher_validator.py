from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional
import re

from src.common.logger import get_logger
from src.llm.query_writer import GDS_PROCEDURE_WHITELIST

if TYPE_CHECKING:
    from src.llm.slice_registry import SchemaSlice

log = get_logger(__name__)

# Reject any query containing write clauses — the pipeline is strictly read-only.
# Checked before the query touches the driver so a single early return covers all paths.
_WRITE_CLAUSE_RE = re.compile(
    r'\b(CREATE|MERGE|SET|DELETE|DETACH\s+DELETE|REMOVE|DROP)\b',
    re.IGNORECASE,
)

# Any non-GDS, non-schema CALL namespace that should never appear in generated queries.
_BLOCKED_CALL_RE = re.compile(
    r'\bCALL\s+(apoc\.|dbms\.|db\.index\.|db\.create\.|db\.drop\.)',
    re.IGNORECASE,
)


@dataclass
class ValidationResult:
    valid: bool
    errors: List[str]
    results: Optional[Any] = None


def cypher_validator(
    cypher: str,
    schema_slice: SchemaSlice,
    property_registry: dict[str, list[str]],
    neo4j_driver: Any,
) -> ValidationResult:
    errors = []

    # Write-clause and blocked-CALL guards run before touching the driver —
    # no point running EXPLAIN on a query that will be rejected anyway.
    if _WRITE_CLAUSE_RE.search(cypher):
        errors.append("Write clauses (CREATE/MERGE/SET/DELETE/REMOVE/DROP) are not permitted")
        return ValidationResult(valid=False, errors=errors)

    blocked = _BLOCKED_CALL_RE.findall(cypher)
    if blocked:
        errors.append(f"Blocked CALL namespace(s): {blocked}")
        return ValidationResult(valid=False, errors=errors)

    try:
        with neo4j_driver.session() as session:
            session.run(f"EXPLAIN {cypher}")
    except Exception as e:
        errors.append(f"Syntax error: {e}")
        return ValidationResult(valid=False, errors=errors)

    domain = schema_slice.domain

    # GDS procedure whitelist — normalise to lowercase before comparison because
    # Neo4j procedure names are case-insensitive but the whitelist is lowercase.
    used_gds_procs = {
        p.lower()
        for p in re.findall(r'\bCALL\s+(gds\.[a-zA-Z0-9_.]+)', cypher, re.IGNORECASE)
    }
    for proc in used_gds_procs:
        if proc not in GDS_PROCEDURE_WHITELIST:
            errors.append(f"GDS procedure '{proc}' is not in the allowed whitelist")
    if errors:
        return ValidationResult(valid=False, errors=errors)

    # When the query consists purely of GDS procedure calls (no MATCH clause
    # referencing graph labels), skip the label/rel/property whitelist —
    # GDS projection config maps contain property key strings that would cause
    # false positives. For mixed queries (GDS + MATCH), the whitelist runs
    # normally to catch schema hallucinations in the non-GDS clauses.
    has_match_clause = bool(re.search(r'\bMATCH\b', cypher, re.IGNORECASE))
    if used_gds_procs and not has_match_clause:
        try:
            with neo4j_driver.session() as session:
                result = session.run(cypher)
                records = [r.data() for r in result]
            return ValidationResult(valid=True, errors=[], results=records)
        except Exception as e:
            log.info("Cypher execution error | %s", e)
            return ValidationResult(valid=False, errors=[f"Execution error: {e}"])

    allowed_labels = {
        label
        for node_str in (schema_slice.nodes + schema_slice.nodes_optional)
        for label in re.findall(r'[A-Za-z0-9_]+', node_str)
    }
    used_labels = set(re.findall(r'(?<!\[):([A-Za-z0-9_]+)', cypher))
    for label in used_labels:
        if label not in allowed_labels:
            errors.append(f"Label '{label}' not in whitelist for schema slice '{domain}'")

    # relationships_optional are valid schema — include in whitelist
    allowed_rels = {
        rel.rel_type
        for rel in (schema_slice.relationships + schema_slice.relationships_optional)
    }
    used_rels = set(re.findall(r'-\[:([A-Za-z0-9_]+)\]', cypher))
    for rel in used_rels:
        if rel not in allowed_rels:
            errors.append(f"Relationship type '{rel}' not in whitelist for schema slice '{domain}'")

    # Properties — property_registry is {label: [prop, ...]}
    # Exclude UNWIND aliases: `UNWIND expr AS alias` binds a map variable whose
    # field access (alias.key) looks like a property access but is not.
    unwind_aliases: set[str] = set(
        re.findall(r'\bUNWIND\b\s+.+?\bAS\b\s+(\w+)', cypher, re.IGNORECASE)
    )
    allowed_props = {prop for props in property_registry.values() for prop in props}
    used_props = {
        prop
        for base, prop in re.findall(r'(\w+)\.(\w+)', cypher)
        if base not in unwind_aliases
    }
    for prop in used_props:
        if prop not in allowed_props:
            errors.append(f"Property '{prop}' not in registry for schema slice '{domain}'")

    if errors:
        log.info("cypher_validator errors found : %s", errors)
        return ValidationResult(valid=False, errors=errors)

    # 5. Execute query (read-only), return results
    try:
        with neo4j_driver.session() as session:
            result = session.run(cypher)
            records = [r.data() for r in result]
        return ValidationResult(valid=True, errors=[], results=records)
    except Exception as e:
        log.info("Cypher execution error | %s", e)
        return ValidationResult(valid=False, errors=[f"Execution error: {e}"])
    

def validate_and_log_cypher(cypher, schema_slice, property_registry, neo4j_driver, logger):
    result = cypher_validator(cypher, schema_slice, property_registry, neo4j_driver)
    if not result.valid:
        logger.warning("cypher_validator | validation failed | %s", result.errors)
    return result