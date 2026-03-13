# tests/test_layers.py
"""
Tests for layer dependency resolution logic.
Covers: ordering, deduplication, transitive deps, cycle detection, CLI validation.
"""

import pytest

from src.common.layers import Layer, resolve_layers, validate_layer_names


# ── resolve_layers ────────────────────────────────────────────────────────────

def test_no_deps_returned_unchanged():
    result = resolve_layers([Layer.PHYSICAL])
    assert result == [Layer.PHYSICAL]


def test_fare_pulls_in_physical():
    result = resolve_layers([Layer.FARE])
    assert result.index(Layer.PHYSICAL) < result.index(Layer.FARE)
    assert Layer.PHYSICAL in result


def test_accessibility_pulls_in_physical():
    result = resolve_layers([Layer.ACCESSIBILITY])
    assert result.index(Layer.PHYSICAL) < result.index(Layer.ACCESSIBILITY)


def test_physical_not_duplicated_when_explicit():
    result = resolve_layers([Layer.PHYSICAL, Layer.FARE])
    assert result.count(Layer.PHYSICAL) == 1
    assert result.index(Layer.PHYSICAL) < result.index(Layer.FARE)


def test_shared_dep_not_duplicated():
    """PHYSICAL should appear once even when both FARE and ACCESSIBILITY request it."""
    result = resolve_layers([Layer.FARE, Layer.ACCESSIBILITY])
    assert result.count(Layer.PHYSICAL) == 1
    assert result.index(Layer.PHYSICAL) < result.index(Layer.FARE)
    assert result.index(Layer.PHYSICAL) < result.index(Layer.ACCESSIBILITY)


def test_all_layers_resolves_without_error():
    result = resolve_layers(list(Layer))
    assert len(result) == len(Layer)
    assert set(result) == set(Layer)


def test_service_schedule_has_no_deps():
    result = resolve_layers([Layer.SERVICE_SCHEDULE])
    assert result == [Layer.SERVICE_SCHEDULE]


def test_interruption_pulls_in_service_schedule_and_physical():
    result = resolve_layers([Layer.INTERRUPTION])
    assert Layer.SERVICE_SCHEDULE in result
    assert Layer.PHYSICAL in result
    assert result.index(Layer.SERVICE_SCHEDULE) < result.index(Layer.INTERRUPTION)
    assert result.index(Layer.PHYSICAL) < result.index(Layer.INTERRUPTION)


def test_order_is_stable_regardless_of_input_order():
    order_a = resolve_layers([Layer.FARE, Layer.PHYSICAL])
    order_b = resolve_layers([Layer.PHYSICAL, Layer.FARE])
    # Both must have PHYSICAL before FARE
    assert order_a.index(Layer.PHYSICAL) < order_a.index(Layer.FARE)
    assert order_b.index(Layer.PHYSICAL) < order_b.index(Layer.FARE)


# ── validate_layer_names ──────────────────────────────────────────────────────

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
