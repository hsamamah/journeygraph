# src/llm/slice_registry.py
"""
SliceRegistry — Schema Slice Registry for the JourneyGraph LLM pipeline.

Loads domain-scoped schema slices from YAML files at startup and validates
them against the live Neo4j graph. Exposes a simple get() interface for
downstream agents (Query Writer, Cypher Validator).

A schema slice is a domain-scoped whitelist: the exact node labels,
relationship types, traversal patterns, and WMATA data quirks the LLM
is permitted to reference for a given query domain. Injecting a slice
rather than the full graph schema keeps LLM prompts compact, focused,
and auditable.

Validation runs once at startup using a single Neo4j connection pass
(three introspection queries, then the connection is released):

    Check 1 — Completeness (static, no DB)
        All four required YAML fields present and non-empty.
        Always raises RuntimeError on failure — a broken YAML is a
        config error that must be fixed before the pipeline can run.

    Check 2 — Label validity (required nodes only)
        CALL db.labels() — every label in nodes: exists in the live
        graph. Labels in nodes_optional: are excluded — their absence
        is expected on a fresh DB. Individual labels are extracted from
        multi-label strings (":Interruption:Skip" → "Interruption",
        "Skip"). In default mode: logs a warning. In strict mode:
        raises RuntimeError.

    Check 3 — Relationship type validity (required relationships only)
        CALL db.relationshipTypes() — every relationship type in
        relationships: exists in the live graph. Relationships whose
        from_label or to_label is in nodes_optional: are skipped
        (auto-optional). Same strict/default behaviour as label validity.

A fourth call — CALL db.schema.nodeTypeProperties() — builds the
property registry. Properties are fanned out to every individual label
component of composite node-type keys (Neo4j returns multi-label types
alphabetically, e.g. "Delay:Interruption"). The result is stored on each
SchemaSlice, scoped to that slice's required + optional labels.

Strict mode:
    Pass strict=True to treat any validation warning as a hard failure.
    Applies to label validity (CN1) and relationship type validity.
    Does not affect completeness failures (always hard fail) or DB
    connectivity failures (always hard fail regardless of mode).

Usage:
    with Neo4jManager() as db:
        registry = SliceRegistry(db, strict=False)

    # Registry holds validated slices in memory — no persistent connection.

    slice = registry.get("transfer_impact")
    slice.nodes       # list of label strings
    slice.patterns    # list of pseudo-Cypher traversal templates
    slice.warnings    # list of WMATA data quirk strings
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import yaml

from src.common.logger import get_logger
from src.common.paths import SLICES_DIR

if TYPE_CHECKING:
    from src.common.neo4j_tools import Neo4jManager

log = get_logger(__name__)

# Required fields in every YAML slice file.
_REQUIRED_FIELDS: frozenset[str] = frozenset(
    {"nodes", "relationships", "patterns", "warnings"}
)


# ── Data structures ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RelationshipTriple:
    """A directed relationship triple parsed from a schema slice YAML."""

    from_label: str  # source node label (primary label, no colon prefix)
    rel_type: str  # relationship type name
    to_label: str  # target node label (primary label, no colon prefix)

    def __str__(self) -> str:
        return f"(:{self.from_label})-[:{self.rel_type}]->(:{self.to_label})"


@dataclass
class SchemaSlice:
    """
    A domain-scoped schema whitelist loaded from a YAML file.

    Attributes:
        domain:                 Domain key matching the YAML filename stem.
        nodes:                  Required node label strings. Checked against
                                db.labels() at startup — strict mode fails if
                                any are absent. Multi-label strings use colon
                                notation e.g. ':Interruption:Skip'.
        relationships:          Required directed triples. Validated against
                                db.relationshipTypes(). Auto-split at build
                                time: any triple whose from_label or to_label
                                is in nodes_optional is moved to
                                relationships_optional instead.
        patterns:               Pseudo-Cypher traversal templates. Injected
                                into the Query Writer system prompt.
        warnings:               WMATA-specific data quirks. Injected into the
                                Query Writer system prompt before the LLM writes
                                Cypher.
        nodes_optional:         Valid schema, absent on a fresh DB (RT/API
                                overlay nodes not yet ingested). Included in
                                the validator whitelist; never checked against
                                db.labels().
        relationships_optional: Auto-derived from nodes_optional at build time.
                                Any relationship touching an optional node is
                                moved here. Included in the validator whitelist.
        property_registry:      {label: [property_name, ...]} for all labels in
                                this slice (required + optional). Built from
                                db.schema.nodeTypeProperties() at startup.
    """

    domain: str
    nodes: list[str]
    relationships: list[RelationshipTriple]
    patterns: list[str]
    warnings: list[str]
    # Optional schema: valid per the WMATA data model but may be absent from the
    # live graph (e.g. no cancellations ingested yet). Included in the validator
    # whitelist so generated Cypher passes; never checked against db.labels() /
    # db.relationshipTypes(), so they never trigger strict-mode failures.
    nodes_optional: list[str] = field(default_factory=list)
    relationships_optional: list[RelationshipTriple] = field(default_factory=list)
    property_registry: dict[str, list[str]] = field(default_factory=dict)


# ── SliceRegistry ─────────────────────────────────────────────────────────────


class SliceRegistry:
    """
    Loads and validates all schema slices at startup.

    The registry holds validated SliceObjects in memory after startup.
    No persistent DB connection is held — the Neo4jManager passed to
    __init__ is used only for the three introspection queries, then
    released.

    All three DB queries run in a single startup pass to minimise
    connection overhead. Hard fails immediately if the DB is unreachable.
    """

    def __init__(self, neo4j: Neo4jManager, *, strict: bool = False) -> None:
        """
        Args:
            neo4j:  Connected Neo4jManager. Used only for startup validation.
            strict: If True, any label or relationship type validation warning
                    is promoted to a RuntimeError. Completeness failures and
                    DB connectivity failures always raise regardless of this flag.
        """
        self._strict = strict
        self._slices: dict[str, SchemaSlice] = {}
        self._load_and_validate(neo4j)

    # ── Public interface ──────────────────────────────────────────────────────

    def get(self, schema_slice_key: str) -> SchemaSlice:
        """
        Return a validated SchemaSlice by domain key.

        Raises:
            KeyError: if schema_slice_key is not a registered domain.
        """
        if schema_slice_key not in self._slices:
            available = sorted(self._slices.keys())
            raise KeyError(
                f"Unknown schema slice key: '{schema_slice_key}'. "
                f"Available domains: {available}"
            )
        return self._slices[schema_slice_key]

    def domains(self) -> list[str]:
        """Return sorted list of all registered domain keys."""
        return sorted(self._slices.keys())

    # ── Startup orchestration ─────────────────────────────────────────────────

    def _load_and_validate(self, neo4j: Neo4jManager) -> None:
        """
        Full startup sequence:
          1. Load YAML files from SLICES_DIR (static, no DB)
          2. Completeness check each file (static, no DB) — always hard fail
          3. Fetch live schema from Neo4j (single DB pass)
          4. Label + relationship type validation per slice (strict/default)
          5. Build SchemaSlice objects and register them
        """
        # Step 1 & 2: load files and check completeness before touching the DB
        raw_slices = self._load_yaml_files()

        # Step 3: single DB pass — hard fail if unreachable
        db_labels, db_rel_types, property_registry = self._fetch_db_schema(neo4j)

        # Steps 4 & 5: validate and register each slice
        for domain, raw in raw_slices.items():
            warnings = self._collect_validation_warnings(
                domain, raw, db_labels, db_rel_types
            )
            self._emit_warnings(domain, warnings)

            slice_obj = self._build_slice(domain, raw, property_registry)
            self._slices[domain] = slice_obj

        log.info(
            "SliceRegistry ready — %d slice(s) registered: %s",
            len(self._slices),
            self.domains(),
        )

    # ── Step 1: YAML loading ──────────────────────────────────────────────────

    def _load_yaml_files(self) -> dict[str, dict]:
        """
        Load all .yaml files from SLICES_DIR.

        Returns:
            {domain_key: raw_yaml_dict} where domain_key is the filename stem.

        Raises:
            RuntimeError: if SLICES_DIR does not exist, contains no YAML files,
                          or a file is not a valid YAML mapping.
        """
        if not SLICES_DIR.exists():
            raise RuntimeError(
                f"Slices directory not found: {SLICES_DIR}. "
                "Ensure src/llm/slices/ exists and contains YAML files."
            )

        yaml_files = sorted(SLICES_DIR.glob("*.yaml"))
        if not yaml_files:
            raise RuntimeError(f"No YAML slice files found in {SLICES_DIR}.")

        raw_slices: dict[str, dict] = {}
        for path in yaml_files:
            domain_key = path.stem
            with path.open() as fh:
                data = yaml.safe_load(fh)
            if not isinstance(data, dict):
                raise RuntimeError(
                    f"Slice file '{path.name}' is not a valid YAML mapping. "
                    "Top-level structure must be a dict."
                )
            # Completeness check — always hard fail (config error, not DB issue)
            missing = _REQUIRED_FIELDS - set(data.keys())
            if missing:
                raise RuntimeError(
                    f"Slice file '{path.name}' is missing required fields: "
                    f"{sorted(missing)}. All four fields are required: "
                    f"{sorted(_REQUIRED_FIELDS)}."
                )
            raw_slices[domain_key] = data
            log.debug("Loaded slice: '%s' (%s)", domain_key, path.name)

        return raw_slices

    # ── Step 3: DB introspection ──────────────────────────────────────────────

    def _fetch_db_schema(
        self, neo4j: Neo4jManager
    ) -> tuple[set[str], set[str], dict[str, list[str]]]:
        """
        Run three introspection queries in a single pass.

        Returns:
            db_labels:         All label strings present in the live graph.
            db_rel_types:      All relationship type strings present.
            property_registry: {label: [property_name, ...]} for all node types.

        Raises:
            RuntimeError: if the DB is unreachable or any query fails.
                          Always raises regardless of strict mode.
        """
        try:
            label_rows = neo4j.query("CALL db.labels() YIELD label RETURN label")
            rel_rows = neo4j.query(
                "CALL db.relationshipTypes() YIELD relationshipType "
                "RETURN relationshipType"
            )
            prop_rows = neo4j.query(
                "CALL db.schema.nodeTypeProperties() "
                "YIELD nodeType, propertyName "
                "RETURN nodeType, propertyName"
            )
        except Exception as exc:
            raise RuntimeError(
                f"SliceRegistry could not reach Neo4j for schema validation. "
                f"Ensure the database is running and credentials are correct. "
                f"Cause: {exc}"
            ) from exc

        db_labels: set[str] = {row["label"] for row in label_rows}
        db_rel_types: set[str] = {row["relationshipType"] for row in rel_rows}

        # db.schema.nodeTypeProperties() returns nodeType as e.g. ":`Trip`" for
        # single-label nodes and ":`Delay`:`Interruption`" (alphabetical) for
        # multi-label nodes. Strip decorators and fan out properties to every
        # individual label component so slice scoping finds them by single label.
        property_registry: dict[str, list[str]] = {}
        for row in prop_rows:
            raw_type = row["nodeType"].replace("`", "")
            prop = row["propertyName"]
            for part in raw_type.split(":"):
                if part:
                    property_registry.setdefault(part, []).append(prop)

        log.debug(
            "DB schema fetched — %d label(s), %d rel type(s), "
            "%d node type(s) with properties",
            len(db_labels),
            len(db_rel_types),
            len(property_registry),
        )
        return db_labels, db_rel_types, property_registry

    # ── Step 4: validation ────────────────────────────────────────────────────

    def _collect_validation_warnings(
        self,
        domain: str,
        raw: dict,
        db_labels: set[str],
        db_rel_types: set[str],
    ) -> list[str]:
        """
        Collect label and relationship type validation issues for one slice.

        Only checks required nodes (nodes:) and required relationships
        (relationships:). Optional nodes/relationships are never checked
        against the live graph — their absence is expected on a fresh DB.

        Relationship optionality is derived automatically: any relationship
        where from or to matches an optional node label is skipped here.

        Returns a list of warning message strings. The caller decides whether
        to log them (default) or raise on them (strict).
        """
        issues: list[str] = []

        # Build optional label set for relationship skip logic below
        optional_labels: set[str] = {
            lbl
            for node_label in raw.get("nodes_optional", [])
            for lbl in node_label.lstrip(":").split(":")
            if lbl
        }

        # Check 2: label validity (required nodes only)
        for node_label in raw.get("nodes", []):
            individual_labels = [
                lbl for lbl in node_label.lstrip(":").split(":") if lbl
            ]
            for label in individual_labels:
                if label not in db_labels:
                    issues.append(
                        f"Slice '{domain}': node label '{label}' "
                        f"(from '{node_label}') not found in live graph. "
                        "The owning layer may not have been loaded yet."
                    )

        # Check 3: relationship type validity (skip auto-optional relationships)
        for rel_entry in raw.get("relationships", []):
            if not isinstance(rel_entry, dict):
                continue
            from_lbl = rel_entry.get("from", "")
            to_lbl = rel_entry.get("to", "")
            if from_lbl in optional_labels or to_lbl in optional_labels:
                continue  # auto-optional — not checked against live graph
            rel_type = rel_entry.get("type", "")
            if rel_type and rel_type not in db_rel_types:
                issues.append(
                    f"Slice '{domain}': relationship type '{rel_type}' "
                    "not found in live graph."
                )

        return issues

    def _emit_warnings(self, domain: str, warnings: list[str]) -> None:
        """
        Log validation warnings. In strict mode, raise on the first issue.
        """
        for warning in warnings:
            if self._strict:
                raise RuntimeError(
                    f"SliceRegistry strict mode validation failed: {warning}"
                )
            log.warning(warning)

    # ── Step 5: slice construction ────────────────────────────────────────────

    def _build_slice(
        self,
        domain: str,
        raw: dict,
        property_registry: dict[str, list[str]],
    ) -> SchemaSlice:
        """
        Parse raw YAML dict into a SchemaSlice.

        Relationship entries with missing keys are skipped with a warning
        rather than raising — all other issues were already surfaced in
        validation. The property_registry is scoped to this slice's labels.
        """
        nodes: list[str] = raw.get("nodes", [])
        nodes_optional: list[str] = raw.get("nodes_optional", [])

        def _parse_rel_triples(entries: list, section: str) -> list[RelationshipTriple]:
            triples: list[RelationshipTriple] = []
            for entry in entries:
                if not isinstance(entry, dict) or not all(
                    k in entry for k in ("from", "type", "to")
                ):
                    log.warning(
                        "Slice '%s': skipping malformed %s entry: %s. "
                        "Expected keys: from, type, to.",
                        domain,
                        section,
                        entry,
                    )
                    continue
                triples.append(
                    RelationshipTriple(
                        from_label=entry["from"],
                        rel_type=entry["type"],
                        to_label=entry["to"],
                    )
                )
            return triples

        # Optional label set — used to auto-derive relationship optionality.
        # Any relationship where from_label or to_label is in this set is
        # treated as optional: valid schema but may be absent on a fresh DB.
        optional_labels: set[str] = {
            lbl
            for node_label in nodes_optional
            for lbl in node_label.lstrip(":").split(":")
            if lbl
        }

        all_rel_entries = _parse_rel_triples(raw.get("relationships", []), "relationships")
        relationships: list[RelationshipTriple] = []
        relationships_optional: list[RelationshipTriple] = []
        for rel in all_rel_entries:
            if rel.from_label in optional_labels or rel.to_label in optional_labels:
                relationships_optional.append(rel)
            else:
                relationships.append(rel)

        # Build property registry scoped to all labels (required + optional).
        slice_labels: set[str] = set()
        for node_label in nodes + nodes_optional:
            for lbl in node_label.lstrip(":").split(":"):
                if lbl:
                    slice_labels.add(lbl)

        scoped_properties = {
            label: props
            for label, props in property_registry.items()
            if label in slice_labels
        }

        return SchemaSlice(
            domain=domain,
            nodes=nodes,
            relationships=relationships,
            patterns=raw.get("patterns", []),
            warnings=raw.get("warnings", []),
            nodes_optional=nodes_optional,
            relationships_optional=relationships_optional,
            property_registry=scoped_properties,
        )
