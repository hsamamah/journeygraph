# src/llm/__init__.py
"""
JourneyGraph LLM Query Pipeline
================================
Natural language querying over the WMATA knowledge graph.

Entry point: src/llm/run.py

Pipeline stages (this branch: Planner only):
  Planner         — domain classification, path selection, anchor extraction
  Query Writer    — schema-grounded Cypher generation    [future branch]
  Cypher Validator— structured validation + retry loop   [future branch]
  Context Builder — anchor resolution + hop expansion    [future branch]
  Narration Agent — final natural language response      [future branch]

Three query domains:
  transfer_impact      — cancelled trips and broken transfer opportunities
  accessibility        — elevator/escalator outages vs service disruptions
  delay_propagation    — origin and spread of delays across trips and stops
"""
