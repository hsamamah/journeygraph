from dataclasses import dataclass
from typing import List, Optional, Any
import re
from src.common.logger import get_logger
log = get_logger(__name__)

@dataclass
class ValidationResult:
    valid: bool
    errors: List[str]
    results: Optional[Any] = None

def cypher_validator(cypher, schema_slice, property_registry, neo4j_driver):
    errors = []
    try:
        with neo4j_driver.session() as session:
            session.run(f"EXPLAIN {cypher}")
    except Exception as e:
        errors.append(f"Syntax error: {e}")
        return ValidationResult(valid=False, errors=errors)

    allowed_labels = set(property_registry.get('labels', []))
    used_labels = set(re.findall(r':([A-Za-z0-9_]+)', cypher))
    for label in used_labels:
        if label not in allowed_labels:
            errors.append(f"Label '{label}' not in whitelist for schema slice '{schema_slice}'")

    allowed_rels = set(property_registry.get('relationships', []))
    used_rels = set(re.findall(r'-\[:([A-Za-z0-9_]+)\]', cypher))
    for rel in used_rels:
        if rel not in allowed_rels:
            errors.append(f"Relationship type '{rel}' not in whitelist for schema slice '{schema_slice}'")

    allowed_props = set(property_registry.get('properties', []))
    used_props = set(re.findall(r'\.(\w+)', cypher))
    for prop in used_props:
        if prop not in allowed_props:
            errors.append(f"Property '{prop}' not in registry for schema slice '{schema_slice}'")

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