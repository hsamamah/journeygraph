# Shared Cypher Query Library

Each subdirectory mirrors a layer. Files are split by purpose:
- `constraints.cypher` — uniqueness constraints and indexes (run by load.py)
- `nodes.cypher` — parameterised MERGE statements for node creation (run by load.py)
- `relationships.cypher` — parameterised MERGE statements for relationships (run by load.py)
- `analytical.cypher` — read-only queries for analysis and demos (run manually in Neo4j Browser)

```
queries/
├── fare/
│   ├── constraints.cypher
│   ├── nodes.cypher
│   ├── relationships.cypher
│   └── analytical.cypher        ← fare analysis + fare+physical cross-layer
├── service_schedule/
│   ├── constraints.cypher
│   ├── nodes.cypher
│   ├── relationships.cypher
│   └── analytical.cypher        ← service-only queries (runnable now)
├── cross_layer/
│   └── analytical.cypher        ← queries spanning service+physical+fare
├── physical/                    ← Lauren's layer
├── accessibility/               ← OutageEvent queries (future)
└── interruption/                ← GTFS-RT deviation queries (future)
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
- Add a comment at the top of each file describing what it does
- Analytical queries use `// ── Qn:` prefix for easy reference
