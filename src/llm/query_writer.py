from dataclasses import dataclass
from typing import Any
import os
import anthropic

@dataclass
class QueryWriterInput:
    user_query: str
    anchors: PlannerAnchors
    schema_slice: str  # from PlannerOutput
    patterns: list[str]  # Example Cypher patterns
    conventions: dict    # Prompt conventions

@dataclass
class QueryWriterOutput:
    cypher_query: str
    cot_comments: str 


class QueryWriter:
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
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        cypher_query, cot_comments = self._parse_llm_response(response.content[0].text)

        return QueryWriterOutput(cypher_query=cypher_query, cot_comments=cot_comments)

    def _parse_llm_response(self, text: str):
        import re
        cypher_match = re.search(r"```cypher\n(.*?)```", text, re.DOTALL)
        cypher_query = cypher_match.group(1).strip() if cypher_match else ""
        comments = text.replace(cypher_match.group(0), "").strip() if cypher_match else text
        return cypher_query, comments

    def _build_system_prompt(self, conventions: dict, patterns: list[str]) -> str:
        # Format conventions and patterns for the LLM system prompt
        conventions_str = f"System conventions:\n{conventions}"
        if patterns:
            patterns_str = "\n\nExample Cypher patterns:\n" + "\n---\n".join(patterns)
        else:
            patterns_str = ""
        return conventions_str + patterns_str

    def _build_user_message(self, user_query: str, anchors: Any, schema_slice: str) -> str:
        return f"User query: {user_query}\nAnchors: {anchors}\nSchema: {schema_slice}"