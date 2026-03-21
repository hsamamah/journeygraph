# src/common/layers.py
"""
Layer registry and dependency resolution for pipeline.py.

Three execution modes:
  Isolated (default):  Run only the requested layers, topo-sorted.
                       --layers fare → [fare]
  With deps:           Include all upstream transitive dependencies.
                       --layers fare --with-deps → [physical, fare]
  Cascade:             Include all downstream dependents.
                       --layers fare --cascade → [fare, ...]
                       (any layer that depends on fare)
  Both:                --layers fare --with-deps --cascade
                       → [physical, fare, ...downstream...]

Adding a new layer:
    1. Add a value to the Layer enum
    2. Add its dependencies to DEPENDENCIES (empty list if none)
    3. Register its module path in pipeline.py _LAYER_MODULES
"""

from enum import StrEnum


class Layer(StrEnum):
    PHYSICAL = "physical"
    SERVICE_SCHEDULE = "service_schedule"
    FARE = "fare"
    ACCESSIBILITY = "accessibility"
    INTERRUPTION = "interruption"


# Directed acyclic graph of layer dependencies.
# A layer will not run until all its dependencies have completed.
DEPENDENCIES: dict[Layer, list[Layer]] = {
    Layer.PHYSICAL: [],
    Layer.SERVICE_SCHEDULE: [Layer.PHYSICAL],
    Layer.FARE: [Layer.PHYSICAL],
    Layer.INTERRUPTION: [Layer.PHYSICAL, Layer.SERVICE_SCHEDULE, Layer.FARE],
    Layer.ACCESSIBILITY: [Layer.PHYSICAL, Layer.SERVICE_SCHEDULE, Layer.FARE],
}


def _topo_sort(layers: set[Layer]) -> list[Layer]:
    """
    Topological sort over a subset of layers, respecting DEPENDENCIES.
    Only considers edges within the given set.
    """
    ordered: list[Layer] = []
    visited: set[Layer] = set()
    in_progress: set[Layer] = set()

    def visit(layer: Layer) -> None:
        if layer in visited:
            return
        if layer in in_progress:
            cycle = " → ".join(str(l) for l in in_progress) + f" → {layer}"
            raise ValueError(f"Circular layer dependency detected: {cycle}")
        in_progress.add(layer)
        for dep in DEPENDENCIES[layer]:
            if dep in layers:
                visit(dep)
        in_progress.discard(layer)
        visited.add(layer)
        ordered.append(layer)

    for layer in layers:
        visit(layer)
    return ordered


def _collect_upstream(layers: list[Layer]) -> set[Layer]:
    """Expand to include all transitive upstream dependencies."""
    needed: set[Layer] = set()

    def collect(layer: Layer) -> None:
        needed.add(layer)
        for dep in DEPENDENCIES[layer]:
            collect(dep)

    for layer in layers:
        collect(layer)
    return needed


def _collect_downstream(layers: list[Layer]) -> set[Layer]:
    """Expand to include all transitive downstream dependents."""
    # Build reverse adjacency
    dependents: dict[Layer, list[Layer]] = {l: [] for l in Layer}
    for layer, deps in DEPENDENCIES.items():
        for dep in deps:
            dependents[dep].append(layer)

    needed: set[Layer] = set()

    def collect(layer: Layer) -> None:
        needed.add(layer)
        for child in dependents[layer]:
            collect(child)

    for layer in layers:
        collect(layer)
    return needed


def resolve_layers(
    requested: list[Layer],
    *,
    with_deps: bool = False,
    cascade: bool = False,
) -> list[Layer]:
    """
    Resolve requested layers into an ordered execution plan.

    Args:
        requested:  Layers explicitly asked for.
        with_deps:  Include upstream transitive dependencies.
        cascade:    Include downstream transitive dependents.

    Default (both False): runs only the requested layers, topo-sorted.

    Examples:
        resolve_layers([Layer.FARE])
        → [Layer.FARE]   (isolated — default)

        resolve_layers([Layer.FARE], with_deps=True)
        → [Layer.PHYSICAL, Layer.FARE]

        resolve_layers([Layer.FARE], cascade=True)
        → [Layer.FARE]   (nothing depends on fare currently)

        resolve_layers([Layer.SERVICE_SCHEDULE], cascade=True)
        → [Layer.SERVICE_SCHEDULE, Layer.INTERRUPTION]

        resolve_layers([Layer.FARE], with_deps=True, cascade=True)
        → [Layer.PHYSICAL, Layer.FARE]
    """
    needed: set[Layer] = set(requested)

    if with_deps:
        needed = _collect_upstream(list(needed))
    if cascade:
        needed = _collect_downstream(list(needed))

    return _topo_sort(needed)


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
            f"Unknown layer(s): {unknown}. Valid options: {sorted(valid.keys())}"
        )

    return result
