# Shared Cypher Query Library

Each subdirectory mirrors a layer. Files are split by purpose:
- `constraints.cypher` — uniqueness constraints and indexes (run by load.py)
- `nodes.cypher` — parameterised MERGE statements for node creation (run by load.py)
- `relationships.cypher` — parameterised MERGE statements for relationships (run by load.py)
- `analytical.cypher` — read-only queries for analysis and LLM few-shot examples

```
queries/
├── physical/
│   ├── constraints.cypher       ← uniqueness constraints + full-text indexes (station name,
│   │                               pathway name, pathway stop_desc, level name, route name)
│   ├── nodes.cypher             ← Station, StationEntrance, Platform, BusStop, FareGate,
│   │                               Pathway, Level; mode/zone multi-label migrations
│   └── relationships.cypher     ← CONTAINS (Station→Entrance/Platform/FareGate),
│                                   LINKS (directional stop-entity ↔ Pathway),
│                                   ON_LEVEL (Pathway/node → Level), BELONGS_TO (Pathway → Station)
├── accessibility/
│   ├── analytical.cypher        ← elevator/escalator outage queries; OPTIONAL MATCH pattern
│   │                               for stations with no active outage
│   ├── constraints.cypher       ← composite_key uniqueness + status/unit_name/severity indexes
│   ├── nodes.cypher             ← OutageEvent MERGE with ON CREATE / ON MATCH split
│   └── relationships.cypher     ← AFFECTS (OutageEvent → Pathway)
├── fare/
│   ├── analytical.cypher        ← fare analysis + fare+physical cross-layer
│   ├── constraints.cypher
│   ├── nodes.cypher
│   └── relationships.cypher
├── service_schedule/
│   ├── analytical.cypher        ← service-only queries
│   ├── constraints.cypher
│   ├── nodes.cypher
│   └── relationships.cypher
├── cross_layer/
│   └── analytical.cypher        ← queries spanning service+physical+fare+interruption
├── delay_propagation/
│   └── analytical.cypher        ← delay queries; two traversal paths (AFFECTS_ROUTE/TRIP
│                                   and SOURCED_FROM provenance); two-pass temporal pattern
├── transfer_impact/
│   └── analytical.cypher        ← skip/cancellation counts; transfer partner impact via
│                                   shared Platform on SCHEDULED_AT
└── interruption/
    ├── constraints.cypher
    ├── nodes.cypher
    └── relationships.cypher
```

## Analytical query dependencies

| Query file | Runnable with |
|---|---|
| `service_schedule/analytical.cypher` | Service layer only |
| `fare/analytical.cypher` Q1–Q6 | Fare layer only |
| `fare/analytical.cypher` Q7–Q10 | Fare + Physical |
| `cross_layer/analytical.cypher` Q1–Q8 | Service + Physical |
| `cross_layer/analytical.cypher` Q9–Q10 | Fare + Physical |
| `cross_layer/analytical.cypher` Q11–Q14 | Service + Fare + Physical |
| `accessibility/analytical.cypher` | Physical + Accessibility layer |
| `delay_propagation/analytical.cypher` | Service + Interruption layer |
| `transfer_impact/analytical.cypher` | Service + Interruption layer |

## LLM few-shot usage

The `analytical.cypher` files in `delay_propagation/`, `transfer_impact/`, and `accessibility/` are loaded by `src/llm/query_writer.py` as few-shot examples for the Text2Cypher LLM stage. Keep them grounded: use resolved literal IDs (e.g. `'STN_B01_F01'`) rather than `$parameters`, carry all variables through `WITH` clauses, and follow the two-pass temporal pattern for "most recent date" queries.

## Loading a query in Python

```python
from src.common.paths import PROJECT_ROOT

def load_query(layer: str, name: str) -> str:
    path = PROJECT_ROOT / "queries" / layer / f"{name}.cypher"
    return path.read_text()
```

## Conventions

- One logical operation per file (create nodes, create relationships, add indexes)
- Use `MERGE` not `CREATE` for idempotency — pipeline may run more than once
- Parameters use `$param` syntax, never string interpolation
- Node identity property is `id` (mapped from GTFS `stop_id` in the physical layer transform)
- Add a comment at the top of each file describing what it does
- Analytical queries use `// ── Qn:` prefix for easy reference
- In `WITH` clauses, always carry through any variable referenced in subsequent `MATCH` or `RETURN`
