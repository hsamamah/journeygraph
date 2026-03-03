# Shared Cypher Query Library

Each subdirectory mirrors a layer. Files are named by operation.

```
queries/
├── fare/
│   ├── create_fare_media.cypher
│   ├── create_fare_products.cypher
│   └── create_fare_leg_rules.cypher
├── physical/
├── service_schedule/
└── accessibility/
```

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
