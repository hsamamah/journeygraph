from dataclasses import dataclass
import requests
from typing import Any
import json
import os
import glob

import anthropic

@dataclass
class QueryWriterInput:
    user_query: str
    anchors: PlannerAnchors
    schema_slice: str  # from PlannerOutput
    patterns: list[str]  # from queries folder
    conventions: dict    # json file from CONVENTIONS.md

@dataclass
class QueryWriterOutput:
    cypher_query: str
    cot_comments: str 


class QueryWriter:
    # def __init__(self, logger=None):
    #     self.logger = logger

    # def run(self, input: QueryWriterInput) -> QueryWriterOutput:
    #     result = call_neo4j_text2cypher(input.user_query, schema=input.schema_slice)
    #     cypher_query = result.get("cypher", "")
    #     cot_comments = result.get("reasoning", "")
        # return QueryWriterOutput(cypher_query=cypher_query, cot_comments=cot_comments)
    def __init__(self, llm=None, logger=None):
        self.llm = llm  
        self.logger = logger 
        self.client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    def run(self, input: QueryWriterInput) -> QueryWriterOutput:
        system_prompt = self._build_system_prompt(input.conventions, input.patterns)
        user_message = self._build_user_message(input.user_query, input.anchors, input.schema_slice)
        prompt = f"{system_prompt}\n\n{user_message}"

        response = self.client.messages.create(
            model="claude-haiku-4-5-20251001",  
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}])

        cypher_query, cot_comments = self._parse_llm_response(response.content[0].text)

        return QueryWriterOutput(cypher_query=cypher_query, cot_comments=cot_comments)

    def _parse_llm_response(self, text: str):
        import re
        cypher_match = re.search(r"```cypher\n(.*?)```", text, re.DOTALL)
        cypher_query = cypher_match.group(1).strip() if cypher_match else ""
        comments = text.replace(cypher_match.group(0), "").strip() if cypher_match else text
        return cypher_query, comments

    def _build_system_prompt(self, conventions: dict, patterns: list[str]) -> str:
        conventions_str = f"System conventions:\n{conventions}"
        if patterns:
            patterns_str = "\n\nExample Cypher patterns:\n" + "\n---\n".join(patterns)
        else:
            patterns_str = ""
        return conventions_str + patterns_str

    def _build_user_message(self, user_query: str, anchors: Any, schema_slice: str) -> str:
        return f"User query: {user_query}\nAnchors: {anchors}\nSchema: {schema_slice}"

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


def run_query_writer(query, planner_output):
    """
    Helper to construct QueryWriterInput, load conventions/patterns, and run QueryWriter.
    Loads all .cypher files (including constraints) from the relevant queries/<domain>/ folder.
    Returns QueryWriterOutput.
    """
    QueryWriterInput_ = QueryWriterInput
    QueryWriter_ = QueryWriter

    # Load conventions
    with open(os.path.join("src", "llm", "conventions.json")) as f:
        conventions = json.load(f)

    # Determine the schema slice/domain folder
    domain = getattr(planner_output, "schema_slice_key", "physical")
    queries_dir = os.path.join("queries", domain)
    patterns = []

    # Load all .cypher files in the domain's queries folder
    if os.path.isdir(queries_dir):
        for cypher_file in sorted(glob.glob(os.path.join(queries_dir, "*.cypher"))):
            with open(cypher_file) as f:
                content = f"\n// --- {os.path.basename(cypher_file)} ---\n" + f.read()
                patterns.append(content)
    else:
        # Fallback to physical if domain folder doesn't exist
        for cypher_file in sorted(glob.glob(os.path.join("queries", "physical", "*.cypher"))):
            with open(cypher_file) as f:
                content = f"\n// --- {os.path.basename(cypher_file)} ---\n" + f.read()
                patterns.append(content)

    query_writer_input = QueryWriterInput_(
        user_query=query,
        anchors=planner_output.anchors,
        schema_slice=planner_output.schema_slice_key,
        patterns=patterns,
        conventions=conventions,
    )
    query_writer = QueryWriter_()
    return query_writer.run(query_writer_input)