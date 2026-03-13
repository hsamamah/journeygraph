# src/common/layers.py
"""
Layer registry and dependency resolution for pipeline.py.

Defines the canonical execution order of all domain layers and their
dependencies. When a layer is requested, all of its dependencies are
automatically included and run first.

Usage:
    from src.common.layers import Layer, resolve_layers

    # Requested via --layers flag:
    requested = [Layer.FARE]
    ordered   = resolve_layers(requested)
    # → [Layer.PHYSICAL, Layer.FARE]

    # Multiple layers with shared dependency:
    requested = [Layer.FARE, Layer.ACCESSIBILITY]
    ordered   = resolve_layers(requested)
    # → [Layer.PHYSICAL, Layer.FARE, Layer.ACCESSIBILITY]

Adding a new layer:
    1. Add a value to the Layer enum
    2. Add its dependencies to DEPENDENCIES (empty list if none)
    3. Register its run() in LAYER_RUNNERS in pipeline.py
"""

from enum import Enum


class Layer(str, Enum):
    PHYSICAL          = "physical"
    SERVICE_SCHEDULE  = "service_schedule"
    FARE              = "fare"
    ACCESSIBILITY     = "accessibility"
    INTERRUPTION      = "interruption"


# Directed acyclic graph of layer dependencies.
# A layer will not run until all its dependencies have completed.
DEPENDENCIES: dict[Layer, list[Layer]] = {
    Layer.PHYSICAL:         [],
    Layer.SERVICE_SCHEDULE: [],
    Layer.FARE:             [Layer.PHYSICAL],
    Layer.ACCESSIBILITY:    [Layer.PHYSICAL],
    Layer.INTERRUPTION:     [Layer.SERVICE_SCHEDULE, Layer.PHYSICAL],
}


def resolve_layers(requested: list[Layer]) -> list[Layer]:
    """
    Expand a list of requested layers to include all transitive dependencies,
    returned in a valid topological execution order (dependencies first).

    Raises ValueError on:
      - Unknown layer names
      - Circular dependencies (should never occur with current graph but
        guards against future additions)

    Examples:
        resolve_layers([Layer.FARE])
        → [Layer.PHYSICAL, Layer.FARE]

        resolve_layers([Layer.PHYSICAL, Layer.FARE])
        → [Layer.PHYSICAL, Layer.FARE]  (deduped, order preserved)

        resolve_layers([Layer.FARE, Layer.ACCESSIBILITY])
        → [Layer.PHYSICAL, Layer.FARE, Layer.ACCESSIBILITY]
    """
    ordered: list[Layer] = []
    visited: set[Layer] = set()
    in_progress: set[Layer] = set()  # cycle detection

    def visit(layer: Layer) -> None:
        if layer in visited:
            return
        if layer in in_progress:
            cycle = " → ".join(str(l) for l in in_progress) + f" → {layer}"
            raise ValueError(f"Circular layer dependency detected: {cycle}")

        in_progress.add(layer)
        for dep in DEPENDENCIES[layer]:
            visit(dep)
        in_progress.discard(layer)

        visited.add(layer)
        ordered.append(layer)

    # Expand requested set to include all transitive dependencies
    needed: set[Layer] = set()

    def collect_deps(layer: Layer) -> None:
        needed.add(layer)
        for dep in DEPENDENCIES[layer]:
            collect_deps(dep)

    for layer in requested:
        collect_deps(layer)

    # Topological sort over the full needed set
    for layer in needed:
        visit(layer)

    return ordered


def validate_layer_names(names: list[str]) -> list[Layer]:
    """
    Convert a list of string layer names (from CLI) to Layer enum values.
    Raises ValueError with a helpful message on unknown names.
    """
    valid = {l.value: l for l in Layer}
    result: list[Layer] = []
    unknown: list[str] = []

    for name in names:
        if name in valid:
            result.append(valid[name])
        else:
            unknown.append(name)

    if unknown:
        raise ValueError(
            f"Unknown layer(s): {unknown}. "
            f"Valid options: {sorted(valid.keys())}"
        )

    return result
