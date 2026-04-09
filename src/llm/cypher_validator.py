from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional
import re

from src.common.logger import get_logger

if TYPE_CHECKING:
    from src.llm.slice_registry import SchemaSlice

log = get_logger(__name__)


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
    try:
        with neo4j_driver.session() as session:
            session.run(f"EXPLAIN {cypher}")
    except Exception as e:
        errors.append(f"Syntax error: {e}")
        return ValidationResult(valid=False, errors=errors)

    domain = schema_slice.domain

    # Labels — schema_slice.nodes may contain multi-label strings (':A:B')
    allowed_labels = {
        label
        for node_str in schema_slice.nodes
        for label in re.findall(r'[A-Za-z0-9_]+', node_str)
    }
    used_labels = set(re.findall(r'(?<!\[):([A-Za-z0-9_]+)', cypher))
    for label in used_labels:
        if label not in allowed_labels:
            errors.append(f"Label '{label}' not in whitelist for schema slice '{domain}'")

    # Relationship types — schema_slice.relationships is list[RelationshipTriple]
    allowed_rels = {rel.rel_type for rel in schema_slice.relationships}
    used_rels = set(re.findall(r'-\[:([A-Za-z0-9_]+)\]', cypher))
    for rel in used_rels:
        if rel not in allowed_rels:
            errors.append(f"Relationship type '{rel}' not in whitelist for schema slice '{domain}'")

    # Properties — property_registry is {label: [prop, ...]}
    allowed_props = {prop for props in property_registry.values() for prop in props}
    used_props = set(re.findall(r'\.(\w+)', cypher))
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