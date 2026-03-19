# tests/test_layers.py
"""
Tests for layer dependency resolution logic.
Covers: isolated mode (default), with_deps, cascade, topo ordering,
deduplication, CLI validation.
"""

import pytest

from src.common.layers import Layer, resolve_layers, validate_layer_names


# ── Isolated mode (default) ──────────────────────────────────────────────────

def test_isolated_returns_only_requested():
    result = resolve_layers([Layer.FARE])
    assert result == [Layer.FARE]


def test_isolated_no_deps_pulled():
    result = resolve_layers([Layer.FARE])
    assert Layer.PHYSICAL not in result


def test_isolated_multiple_layers_topo_sorted():
    result = resolve_layers([Layer.FARE, Layer.PHYSICAL])
    # Both present, PHYSICAL before FARE (topo order)
    assert Layer.PHYSICAL in result
    assert Layer.FARE in result
    assert result.index(Layer.PHYSICAL) < result.index(Layer.FARE)


def test_isolated_service_schedule_standalone():
    result = resolve_layers([Layer.SERVICE_SCHEDULE])
    assert result == [Layer.SERVICE_SCHEDULE]


def test_isolated_interruption_standalone():
    result = resolve_layers([Layer.INTERRUPTION])
    assert result == [Layer.INTERRUPTION]


def test_isolated_all_layers():
    result = resolve_layers(list(Layer))
    assert len(result) == len(Layer)
    assert set(result) == set(Layer)


# ── with_deps mode ───────────────────────────────────────────────────────────

def test_with_deps_fare_pulls_physical():
    result = resolve_layers([Layer.FARE], with_deps=True)
    assert Layer.PHYSICAL in result
    assert result.index(Layer.PHYSICAL) < result.index(Layer.FARE)


def test_with_deps_accessibility_pulls_physical():
    result = resolve_layers([Layer.ACCESSIBILITY], with_deps=True)
    assert Layer.PHYSICAL in result
    assert result.index(Layer.PHYSICAL) < result.index(Layer.ACCESSIBILITY)


def test_with_deps_interruption_pulls_service_schedule():
    result = resolve_layers([Layer.INTERRUPTION], with_deps=True)
    assert Layer.SERVICE_SCHEDULE in result
    assert Layer.PHYSICAL in result  # service_schedule depends on physical → transitive
    assert result.index(Layer.PHYSICAL) < result.index(Layer.SERVICE_SCHEDULE)
    assert result.index(Layer.SERVICE_SCHEDULE) < result.index(Layer.INTERRUPTION)


def test_with_deps_physical_not_duplicated():
    result = resolve_layers([Layer.PHYSICAL, Layer.FARE], with_deps=True)
    assert result.count(Layer.PHYSICAL) == 1


def test_with_deps_shared_dep_not_duplicated():
    """PHYSICAL appears once even when both FARE and ACCESSIBILITY request it."""
    result = resolve_layers([Layer.FARE, Layer.ACCESSIBILITY], with_deps=True)
    assert result.count(Layer.PHYSICAL) == 1
    assert result.index(Layer.PHYSICAL) < result.index(Layer.FARE)
    assert result.index(Layer.PHYSICAL) < result.index(Layer.ACCESSIBILITY)


def test_with_deps_no_deps_layer_unchanged():
    result = resolve_layers([Layer.PHYSICAL], with_deps=True)
    assert result == [Layer.PHYSICAL]


# ── cascade mode ─────────────────────────────────────────────────────────────

def test_cascade_service_schedule_includes_interruption():
    result = resolve_layers([Layer.SERVICE_SCHEDULE], cascade=True)
    assert Layer.INTERRUPTION in result
    assert result.index(Layer.SERVICE_SCHEDULE) < result.index(Layer.INTERRUPTION)


def test_cascade_physical_includes_fare_and_accessibility():
    result = resolve_layers([Layer.PHYSICAL], cascade=True)
    assert Layer.FARE in result
    assert Layer.ACCESSIBILITY in result


def test_cascade_leaf_layer_no_change():
    """FARE has no downstream dependents, cascade adds nothing."""
    result = resolve_layers([Layer.FARE], cascade=True)
    assert result == [Layer.FARE]


# ── combined with_deps + cascade ─────────────────────────────────────────────

def test_combined_modes():
    result = resolve_layers([Layer.SERVICE_SCHEDULE], with_deps=True, cascade=True)
    # SERVICE_SCHEDULE has no upstream deps, but INTERRUPTION is downstream
    assert Layer.INTERRUPTION in result
    assert result.index(Layer.SERVICE_SCHEDULE) < result.index(Layer.INTERRUPTION)


# ── topo ordering ────────────────────────────────────────────────────────────

def test_topo_order_stable_regardless_of_input_order():
    order_a = resolve_layers([Layer.FARE, Layer.PHYSICAL])
    order_b = resolve_layers([Layer.PHYSICAL, Layer.FARE])
    assert order_a.index(Layer.PHYSICAL) < order_a.index(Layer.FARE)
    assert order_b.index(Layer.PHYSICAL) < order_b.index(Layer.FARE)


# ── validate_layer_names ─────────────────────────────────────────────────────

def test_valid_names_convert_to_enum():
    result = validate_layer_names(["physical", "fare"])
    assert Layer.PHYSICAL in result
    assert Layer.FARE in result


def test_unknown_name_raises():
    with pytest.raises(ValueError, match="Unknown layer"):
        validate_layer_names(["physical", "nonexistent"])


def test_all_valid_layer_names_accepted():
    names = [l.value for l in Layer]
    result = validate_layer_names(names)
    assert set(result) == set(Layer)


def test_empty_list_returns_empty():
    result = validate_layer_names([])
    assert result == []
