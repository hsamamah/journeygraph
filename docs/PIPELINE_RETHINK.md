# Post-Mortem & Architecture Rethink: Toward JourneyGraph v2

## Executive Summary
JourneyGraph v1 successfully implemented a graph-grounded QA pipeline using **Neo4j** and **LLMs** to answer transit-related questions. While the system achieved a **56% overall pass rate**, an in-depth evaluation revealed that the primary bottlenecks are **architectural rigidities** rather than model intelligence. This document outlines the transition from a "Constrained Corridor" to a "Schema-Aware Agentic System."

---

## 1. The "Constrained Corridor" (v1)
The current architecture functions as a linear, human-directed pipeline where every stage has its decision-making parameters pre-set:

* **Human-Authored Schema Slices:** Query generation is limited to specific node/relationship "slices" manually curated for three domains (accessibility, delay, transfer impact).
* **Linear Routing:** A question is forced into a single domain bucket; if a query spans multiple domains, it is "squeezed" into one, leading to data loss.
* **Fixed Hop Depth:** The subgraph expander uses hardcoded hop counts, preventing the system from "going deeper" when context is missing.
* **Silent Degradation:** When the system hits a logic wall (e.g., an unresolvable spatial anchor like "downtown"), it attempts to narrate a degraded answer rather than identifying the failure point.

---

## 2. Root Cause Analysis
Our evaluation of 57 questions across varying categories (Happy Path, Edge Case, GDS, Adversarial) identified three critical failure modes:

| Failure Mode | Technical Root Cause | Impact |
| :--- | :--- | :--- |
| **Planner Refusal** | Routing logic lacks a handler for GDS-specific intents (Centrality, Louvain, BFS). | **60% failure rate** in the GDS domain. |
| **Hallucination** | Narration agents fill gaps in information that the hardcoded subgraph didn't cover. | High hallucination in the accessibility domain (e.g., fabricated elevator IDs). |
| **Spatial Rejection** | Rigid anchors cannot resolve high-level terms (e.g., "downtown") without a specific graph node match. | Total refusal for underspecified spatial queries. |

---

## 3. The Proposal: Schema-Aware Agents
The rethink moves the "intelligence" from hardcoded Python logic into a dynamic LLM-driven loop.

### The Agentic Loop
Instead of pre-routing, the LLM is provided with a **Graph Schema Overview** (structure only) and a set of tools:
1.  **Plan:** The LLM decides what context is needed based on the full schema.
2.  **Act:** The LLM executes Cypher, GDS algorithms, or full-text searches as needed.
3.  **Observe:** The LLM evaluates the retrieved data; if it is insufficient, it expands the search or tries a new tool.
4.  **Iterate:** The process repeats until a satisfying, grounded answer is generated.



### Key Shifts
* **Dynamic Schema Slicing:** The LLM generates its own working context from the schema rather than relying on human-authored files.
* **Backtracking:** The agent can "go back" and try a different retrieval path if the first one returns empty.
* **Explicit Failure Visibility:** The agent surfaces why it stopped (e.g., "I tried to find elevator data for station X, but it doesn't exist"), reducing hallucination.

---

## 4. Strategic Trade-offs

| | v1: Constrained Corridor | v2: Proposed Agent |
| :--- | :--- | :--- |
| **Latency** | Bounded (~3 LLM calls) | Variable ($N$ iterations) |
| **Cost** | Predictable | Variable |
| **Failure Mode** | Silent | Explicit and Traceable |
| **Capability** | High precision for known types | Generalization to unknown types |

---

## 5. Conclusion & Next Steps
The JourneyGraph data model and eval harness are robust assets that remain relevant. The next phase of development will focus on retiring the hardcoded domain classifier and human-authored slices in favor of a **ReAct-style agent loop** that treats the graph schema as a navigable map rather than a series of hallways.
