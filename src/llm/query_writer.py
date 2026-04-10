# src/llm/query_writer.py
"""
QueryWriter — Text2Cypher LLM stage for the JourneyGraph pipeline.

Receives a natural language query, resolved anchor IDs, and a SchemaSlice,
then asks Claude to produce a single read-only Cypher query.

Prompt construction (system message):
  1. Role + output format instruction
  2. SchemaSlice constraints (node whitelist, relationship whitelist,
     optional schema, traversal patterns, data warnings) — injected as
     hard constraints so the LLM cannot hallucinate schema
  3. System conventions (conventions.json)
  4. Few-shot examples — all *.cypher files from queries/<domain>/

User message includes the raw query, the PlannerAnchors struct, and the
resolved anchor IDs as literal values (e.g. "yesterday → '20260408'")
with an explicit instruction to use them directly rather than $parameters.

Entry point: run_query_writer(query, planner_output, llm_config,
                              schema_slice, resolved_anchors)
"""

from __future__ import annotations

from dataclasses import dataclass, field
import requests
from typing import TYPE_CHECKING
import json
import os
import re

import anthropic

from src.common.logger import get_logger
from src.common.paths import PROJECT_ROOT
from src.llm.planner_output import PlannerAnchors

if TYPE_CHECKING:
    from src.common.config import LLMConfig
    from src.llm.planner_output import PlannerOutput
    from src.llm.slice_registry import SchemaSlice

log = get_logger(__name__)


@dataclass
class QueryWriterInput:
    user_query: str
    anchors: PlannerAnchors
    schema_slice: str          # domain key string e.g. "delay_propagation"
    schema_slice_obj: SchemaSlice | None  # full slice object for prompt injection
    patterns: list[str]        # few-shot .cypher files from queries/<domain>/
    conventions: dict          # parsed conventions.json
    resolved_anchors: dict[str, list[str]] = field(default_factory=dict)
    # mention → resolved graph IDs e.g. {'yesterday': ['20260408'], 'Red Line': ['RED']}
    refinement_errors: list[str] = field(default_factory=list)
    # validator errors from prior attempt — injected as a correction hint on retry

@dataclass
class QueryWriterOutput:
    cypher_query: str
    cot_comments: str 


class QueryWriter:
    def __init__(self, llm_config: LLMConfig, *, use_gds: bool = False) -> None:
        self.client = anthropic.Anthropic(api_key=llm_config.anthropic_api_key)
        self._model = llm_config.llm_model
        self._max_tokens = llm_config.llm_max_tokens
        self._gds_enabled: bool = use_gds

    def run(self, input: QueryWriterInput) -> QueryWriterOutput:
        system_prompt = self._build_system_prompt(
            input.conventions, input.patterns, input.schema_slice_obj, gds_enabled=self._gds_enabled
        )
        user_message = self._build_user_message(
            input.user_query,
            input.anchors,
            input.schema_slice,
            input.resolved_anchors,
            input.refinement_errors or None,
        )
        prompt = f"{system_prompt}\n\n{user_message}"

        log.debug("query_writer | sending prompt | domain=%s | prompt_len=%d", input.schema_slice, len(prompt))

        response = self.client.messages.create(
            model=self._model,
            max_tokens=self._max_tokens,
            messages=[{"role": "user", "content": prompt}])

        cypher_query, cot_comments = self._parse_llm_response(response.content[0].text)

        log.info(
            "query_writer | generated cypher | domain=%s\n%s",
            input.schema_slice,
            cypher_query,
        )

        return QueryWriterOutput(cypher_query=cypher_query, cot_comments=cot_comments)

    def _parse_llm_response(self, text: str):
        cypher_match = re.search(r"```cypher\n(.*?)```", text, re.DOTALL)
        cypher_query = cypher_match.group(1).strip() if cypher_match else ""
        comments = text.replace(cypher_match.group(0), "").strip() if cypher_match else text
        return cypher_query, comments

    def _build_system_prompt(
        self,
        conventions: dict,
        patterns: list[str],
        schema_slice: SchemaSlice | None = None,
        gds_enabled: bool = False,
    ) -> str:
        parts = [
            "You are a Cypher query writer for a Neo4j knowledge graph of the WMATA transit system.",
            "Given a user query, anchors (resolved graph entity IDs), and a schema slice, "
            "produce a single read-only Cypher query that answers the question.",
            "Output ONLY a ```cypher\\n...\\n``` code block followed by a brief explanation. "
            "Do not include any SQL or non-Cypher syntax.",
        ]

        if schema_slice is not None:
            # ── Node label whitelist ──────────────────────────────────────────
            # Only use these exact label strings — no others exist in the graph.
            node_list = "\n".join(f"  {n}" for n in schema_slice.nodes)
            parts.append(f"Allowed node labels (whitelist — use ONLY these):\n{node_list}")

            # ── Relationship whitelist ────────────────────────────────────────
            rel_list = "\n".join(f"  {r}" for r in schema_slice.relationships)
            parts.append(f"Allowed relationship types and directions:\n{rel_list}")

            # ── Optional schema (valid model, may have no live data) ──────────
            if schema_slice.nodes_optional:
                opt_nodes = "\n".join(f"  {n}" for n in schema_slice.nodes_optional)
                parts.append(
                    f"Optional node labels (valid schema, may currently have no data):\n{opt_nodes}"
                )
            if schema_slice.relationships_optional:
                opt_rels = "\n".join(f"  {r}" for r in schema_slice.relationships_optional)
                parts.append(
                    f"Optional relationship types (valid schema, may currently have no data):\n{opt_rels}"
                )

            # ── Traversal patterns ────────────────────────────────────────────
            if schema_slice.patterns:
                pat_list = "\n".join(f"  {p}" for p in schema_slice.patterns)
                parts.append(f"Key traversal patterns for this domain:\n{pat_list}")

            # ── Data warnings ─────────────────────────────────────────────────
            if schema_slice.warnings:
                warn_list = "\n".join(f"  - {w}" for w in schema_slice.warnings)
                parts.append(f"IMPORTANT data quirks — read before writing Cypher:\n{warn_list}")

        if gds_enabled:
            parts.append(_GDS_SYSTEM_SECTION)

        if conventions:
            parts.append(f"System conventions:\n{json.dumps(conventions, indent=2)}")
        if patterns:
            parts.append("Example Cypher queries for this domain:\n" + "\n---\n".join(patterns))
        return "\n\n".join(parts)

    def _build_user_message(
        self,
        user_query: str,
        anchors: PlannerAnchors,
        schema_slice: str,
        resolved_anchors: dict[str, list[str]] | None = None,
        refinement_errors: list[str] | None = None,
    ) -> str:
        resolved_block = ""
        if resolved_anchors:
            lines = [
                f"  {mention} → {ids[0]!r}"
                for mention, ids in resolved_anchors.items()
                if ids
            ]
            if lines:
                resolved_block = (
                    "\nResolved IDs (use these literal values directly in Cypher — "
                    "do NOT use $parameters):\n" + "\n".join(lines)
                )
        refinement_block = ""
        if refinement_errors:
            error_lines = "\n".join(f"  - {e}" for e in refinement_errors)
            refinement_block = (
                "\n\nYour previous Cypher query failed validation. "
                "Fix ONLY the issues listed below — do not change anything else:\n"
                + error_lines
            )
        return (
            f"User query: {user_query}\nAnchors: {anchors}{resolved_block}"
            f"\nSchema: {schema_slice}{refinement_block}"
        )

def _call_neo4j_text2cypher(query: str, schema: str = None, url: str = None, user: str = None, password: str = None) -> dict:
    """Experimental: call Neo4j's built-in Text2Cypher REST endpoint. Not used by the main pipeline."""
    url = url or os.environ.get("NEO4J_TEXT2CYPHER_URL", "http://localhost:7474/ai/text2cypher")
    user = user or os.environ.get("NEO4J_USER", "neo4j")
    password = password or os.environ.get("NEO4J_PASSWORD")
    if not password:
        raise ValueError("NEO4J_PASSWORD environment variable must be set")
    headers = {"Accept": "application/json"}
    payload = {"question": query}
    if schema:
        payload["schema"] = schema
    resp = requests.post(url, json=payload, headers=headers, auth=(user, password))
    resp.raise_for_status()
    return resp.json()


# Allowed GDS procedure names — the validator enforces this whitelist when
# CALL gds.* appears in generated Cypher.
GDS_PROCEDURE_WHITELIST: frozenset[str] = frozenset({
    # Graph projection (anonymous/ephemeral only — gds.graph.drop is intentionally excluded)
    "gds.graph.project",
    # Path finding
    "gds.shortestpath.dijkstra.stream",
    "gds.allshortestpaths.dijkstra.stream",
    "gds.shortestpath.astar.stream",
    "gds.bfs.stream",
    "gds.dfs.stream",
    # Centrality
    "gds.pagerank.stream",
    "gds.betweenness.stream",
    "gds.degree.stream",
    "gds.closeness.stream",
    "gds.articlerank.stream",
    "gds.eigenvector.stream",
    # Community detection
    "gds.louvain.stream",
    "gds.labelpropagation.stream",
    "gds.wcc.stream",
    "gds.scc.stream",
    "gds.trianglecount.stream",
    "gds.localclusteringcoefficient.stream",
    # Similarity
    "gds.nodesimilarity.stream",
    "gds.knn.stream",
    # Utility (gds.version excluded — startup probe only, not a query-answering procedure)
    "gds.list",
})

# Human-readable algorithm categories injected into the system prompt when GDS is enabled.
_GDS_SYSTEM_SECTION = """\
Graph Data Science (GDS) procedures are available. Use anonymous projections (no named graph lifecycle).
Allowed GDS procedure categories:
  Path finding:         gds.shortestPath.dijkstra.stream, gds.allShortestPaths.dijkstra.stream, gds.shortestPath.astar.stream, gds.bfs.stream
  Centrality:           gds.pageRank.stream, gds.betweenness.stream, gds.degree.stream, gds.closeness.stream
  Community detection:  gds.louvain.stream, gds.labelPropagation.stream, gds.wcc.stream, gds.scc.stream, gds.triangleCount.stream
  Similarity:           gds.nodeSimilarity.stream, gds.knn.stream
  Projection:           gds.graph.project (anonymous — do NOT assign to a named variable that outlives the query)
IMPORTANT: Always use ephemeral/anonymous projections inline. Do not use gds.graph.drop or named graph management."""


def run_query_writer(
    query: str,
    planner_output: PlannerOutput,
    llm_config: LLMConfig,
    schema_slice: SchemaSlice | None = None,
    resolved_anchors: dict[str, list[str]] | None = None,
    refinement_errors: list[str] | None = None,
    use_gds: bool = False,
) -> QueryWriterOutput:
    """
    Construct QueryWriterInput, load conventions/patterns, and run QueryWriter.

    Loads all .cypher files from the relevant queries/<domain>/ folder as
    few-shot patterns. Falls back to queries/physical/ if the domain folder
    doesn't exist.

    Args:
        schema_slice:     SchemaSlice from SliceRegistry. Nodes, relationships,
                          patterns, and warnings are injected into the system
                          prompt as hard constraints before the few-shot examples.
        resolved_anchors: Output of resolutions.as_flat_dict() — maps each
                          mention string to its resolved graph ID(s), e.g.
                          {'yesterday': ['20260408'], 'Red Line': ['RED']}.
                          Injected into the user message so the LLM writes
                          literal values instead of $parameters.
        use_gds:          When True, GDS procedure whitelist and few-shot examples
                          from queries/gds/analytical.cypher are added to the prompt.
    """
    with open(PROJECT_ROOT / "src" / "llm" / "conventions.json") as f:
        conventions = json.load(f)

    domain = getattr(planner_output, "schema_slice_key", "physical")
    analytical_path = PROJECT_ROOT / "queries" / domain / "analytical.cypher"
    patterns: list[str] = []

    if analytical_path.is_file():
        with open(analytical_path) as f:
            patterns.append(f"\n// --- analytical.cypher ---\n" + f.read())

    if use_gds:
        gds_path = PROJECT_ROOT / "queries" / "gds" / "analytical.cypher"
        if gds_path.is_file():
            with open(gds_path) as f:
                patterns.append(f"\n// --- gds/analytical.cypher ---\n" + f.read())
        else:
            log.warning("query_writer | use_gds=True but queries/gds/analytical.cypher not found")

    log.info(
        "query_writer | building prompt | domain=%s slice_injected=%s use_gds=%s few_shot_files=%d",
        domain,
        schema_slice is not None,
        use_gds,
        len(patterns),
    )

    query_writer_input = QueryWriterInput(
        user_query=query,
        anchors=planner_output.anchors,
        schema_slice=planner_output.schema_slice_key,
        schema_slice_obj=schema_slice,
        patterns=patterns,
        conventions=conventions,
        resolved_anchors=resolved_anchors or {},
        refinement_errors=refinement_errors or [],
    )
    return QueryWriter(llm_config, use_gds=use_gds).run(query_writer_input)
