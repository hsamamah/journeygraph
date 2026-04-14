# demos/

Runnable presentation artefacts for the JourneyGraph WMATA knowledge graph — notebooks, a Neo4j dashboard, and the Cypher queries that power them.

---

## Contents

```
demos/
├── llm_pipeline.ipynb      ← End-to-end LLM query pipeline walkthrough (3 questions)
├── dijkstra.ipynb          ← GDS Dijkstra shortest-path before/after an outage
├── pipeline_demo.py        ← Standalone script version of the LLM pipeline
├── dashboard_demo.json     ← Neo4j Dashboards v2 import file (5 pages)
└── queries/                ← Graph-view Cypher queries used in the dashboard
    ├── 01_station_compact.cypher
    ├── 02_station_full.cypher
    ├── 03_interruption_layer.cypher
    ├── 04_accessibility_outages.cypher
    ├── 05_trip_schedule.cypher
    └── 06_service_alerts.cypher
```

---

## `llm_pipeline.ipynb`

Walks through the full natural-language query pipeline end-to-end across three sample questions.

**Prerequisites** — `.env` at the project root with `ANTHROPIC_API_KEY`, `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` set. Neo4j running.

**Install**: `uv sync --extra demo`

### Pipeline stages

| Step | Component | What it does |
|---|---|---|
| 1 | **Planner** | Single LLM call — classifies domain, selects execution path (`text2cypher` / `subgraph` / `both`), extracts entity anchors |
| 2 | **Anchor Resolver** | Maps anchor strings to Neo4j node IDs via full-text index + disambiguation; normalises relative dates to `YYYYMMDD` |
| 3a | **Subgraph Builder** | Bidirectional hop expansion from anchors (`HopExpander`), serialised to ≤ 2,000 tokens (`ContextSerializer`) |
| 3b | **Text-to-Cypher** | `QueryWriter` generates a Cypher query from few-shot examples + conventions, self-corrects up to 3 times |
| 4 | **Narration Agent** | Terminal LLM call — response mode selected by Python logic, not LLM |

### Response modes (Step 4)

| Mode | Condition |
|---|---|
| `precision` | Text2Cypher succeeded, no subgraph |
| `contextual` | Subgraph succeeded, no Text2Cypher |
| `synthesis` | Both paths succeeded |
| `degraded` | Neither path succeeded |

### Three sample questions

| # | Question | Domain | Path |
|---|---|---|---|
| Q1 | *"How many trips were cancelled on the Red Line yesterday?"* | `transfer_impact` | `text2cypher` |
| Q2 | *"Which stations are most affected by the current delay on the Blue Line?"* | `delay_propagation` | `subgraph` |
| Q3 | *"Is the elevator at Metro Center currently out of service?"* | `accessibility` | `both` |

**Q1** is a full step-by-step walkthrough — every pipeline stage is shown in detail including both the **static pipeline** and the **agentic pipeline** (Claude function-calling loop, max 5 iterations). Q2 and Q3 run the same stages but surface outputs only.

### Static vs Agentic

| | Static pipeline | Agentic pipeline |
|---|---|---|
| Execution | Fixed `QueryWriter → Validator → Subgraph` fork | Claude tool-calling loop selects tools dynamically |
| Tools | — | `cypher_query`, `subgraph_expand` |
| Max iterations | 1 | 5 |

Each question ends with a **Comparison Notes** cell to fill in after running.

---

## `dijkstra.ipynb`

GDS 2.6.9 shortest-path demo — finds the fastest walking route through a station's physical pathway graph before and after an elevator outage.

Projects `StationEntrance`, `Pathway`, `FareGate`, `Platform` nodes connected by `[:LINKS]` into a weighted GDS graph (`traversal_time` as edge weight), then runs `gds.shortestPath.dijkstra` twice — once on the full graph and once with the outage elevator excluded — to show how an outage reroutes passengers.

---

## `dashboard_demo.json`

Neo4j Dashboards v2 import file. **Import via**: Neo4j Desktop → Dashboards → Import.

Five pages:

| Page | Focus | Widget types |
|---|---|---|
| **Overview** | Node + relationship counts by type | Tables, bar charts, graph |
| **Service & Routes** | Busiest stations, route mode split, transfer hubs, route patterns | Pie, bar charts, tables, graphs |
| **Calendar & Schedule** | Date range, trips by service pattern, scheduled trip graph | Tables, bar charts, graphs |
| **Disruptions & Accessibility** | Interruption types, Blue Line incident, service alerts | Bar chart, table, graphs |
| **Physical Layer** | Pathway types, outage counts, station maps, accessibility walking path | Pie, bar chart, graphs, GDS BFS table |

### GDS widgets

Two widgets on the **Physical Layer** page use GDS and will fail with `graph already exists` on reload. Both queries include a `CALL gds.graph.drop(..., false)` at the top to handle this automatically.

| Widget | Named projection |
|---|---|
| Metro Center — Accessible Walking Path | `walkPath` |
| Metro Center — BFS Accessibility Reachability | `accessBFS` |

---

## `queries/`

Graph-view Cypher queries used as the basis for dashboard graph widgets. Each file contains 2–3 queries returning nodes and paths for rendering in Neo4j Browser or Dashboard graph cards.

| File | What it shows |
|---|---|
| `01_station_compact.cypher` | Station + served Rail routes + platforms (Metro Center, L'Enfant Plaza) |
| `02_station_full.cypher` | Full physical accessibility graph — elevators, escalators, walkways, fare gates |
| `03_interruption_layer.cypher` | Skip / Cancellation interruptions with Date, Route, TripUpdate evidence |
| `04_accessibility_outages.cypher` | Active OutageEvent nodes linked to Pathway and Station |
| `05_trip_schedule.cypher` | Trips at a station, RoutePattern branches, ServicePattern calendar |
| `06_service_alerts.cypher` | ServiceAlert → EntitySelector → Route graph; alert + skip cross-layer view |
