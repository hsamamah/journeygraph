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
import glob
import re

import anthropic

from src.common.logger import get_logger
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

@dataclass
class QueryWriterOutput:
    cypher_query: str
    cot_comments: str 


class QueryWriter:
    def __init__(self, llm_config: LLMConfig) -> None:
        self.client = anthropic.Anthropic(api_key=llm_config.anthropic_api_key)
        self._model = llm_config.llm_model
        self._max_tokens = llm_config.llm_max_tokens

    def run(self, input: QueryWriterInput) -> QueryWriterOutput:
        system_prompt = self._build_system_prompt(
            input.conventions, input.patterns, input.schema_slice_obj
        )
        user_message = self._build_user_message(
            input.user_query, input.anchors, input.schema_slice, input.resolved_anchors
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
        return f"User query: {user_query}\nAnchors: {anchors}{resolved_block}\nSchema: {schema_slice}"

def call_neo4j_text2cypher(query: str, schema: str = None, url: str = None, user: str = None, password: str = None) -> dict:
    url = url or os.environ.get("NEO4J_TEXT2CYPHER_URL", "http://localhost:7474/ai/text2cypher")
    user = user or os.environ.get("NEO4J_USER", "neo4j")
    password = password or os.environ.get("NEO4J_PASSWORD", "test")
    headers = {"Accept": "application/json"}
    payload = {"question": query}
    if schema:
        payload["schema"] = schema
    resp = requests.post(url, json=payload, headers=headers, auth=(user, password))
    resp.raise_for_status()
    return resp.json()


def run_query_writer(
    query: str,
    planner_output: PlannerOutput,
    llm_config: LLMConfig,
    schema_slice: SchemaSlice | None = None,
    resolved_anchors: dict[str, list[str]] | None = None,
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
    """
    with open(os.path.join("src", "llm", "conventions.json")) as f:
        conventions = json.load(f)

    domain = getattr(planner_output, "schema_slice_key", "physical")
    queries_dir = os.path.join("queries", domain)
    patterns: list[str] = []

    source_dir = queries_dir if os.path.isdir(queries_dir) else os.path.join("queries", "physical")
    for cypher_file in sorted(glob.glob(os.path.join(source_dir, "*.cypher"))):
        with open(cypher_file) as f:
            patterns.append(f"\n// --- {os.path.basename(cypher_file)} ---\n" + f.read())

    log.info(
        "query_writer | building prompt | domain=%s slice_injected=%s few_shot_files=%d",
        domain,
        schema_slice is not None,
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
    )
    return QueryWriter(llm_config).run(query_writer_input)
