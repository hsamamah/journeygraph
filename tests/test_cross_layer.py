# tests/test_cross_layer.py
"""
Tests for src/common/cross_layer.py

Covers: check_target_nodes — both the "nodes exist" and "nodes absent" paths.
"""

from unittest.mock import MagicMock
import pytest
from src.common.cross_layer import check_target_nodes


@pytest.fixture
def neo4j():
    return MagicMock()


class TestCheckTargetNodes:
    def test_returns_true_when_nodes_exist(self, neo4j):
        neo4j.query.return_value = [{"1": 1}]
        assert check_target_nodes(neo4j, "Station", "service → physical") is True

    def test_returns_false_when_no_nodes(self, neo4j):
        neo4j.query.return_value = []
        assert check_target_nodes(neo4j, "Station", "service → physical") is False

    def test_query_uses_limit_1(self, neo4j):
        neo4j.query.return_value = []
        check_target_nodes(neo4j, "Platform", "ctx")
        cypher = neo4j.query.call_args[0][0]
        assert "LIMIT 1" in cypher

    def test_query_targets_correct_label(self, neo4j):
        neo4j.query.return_value = []
        check_target_nodes(neo4j, "Trip", "ctx")
        cypher = neo4j.query.call_args[0][0]
        assert ":Trip" in cypher

    def test_logs_warning_when_absent(self, neo4j):
        neo4j.query.return_value = []
        with MagicMock() as mock_log:
            import src.common.cross_layer as mod
            original = mod.log
            mod.log = mock_log
            try:
                check_target_nodes(neo4j, "Route", "interruption → service")
                mock_log.warning.assert_called_once()
                args = mock_log.warning.call_args[0]
                assert "Route" in args[2]
            finally:
                mod.log = original

    def test_no_warning_when_nodes_exist(self, neo4j):
        neo4j.query.return_value = [{"1": 1}]
        with MagicMock() as mock_log:
            import src.common.cross_layer as mod
            original = mod.log
            mod.log = mock_log
            try:
                check_target_nodes(neo4j, "Route", "ctx")
                mock_log.warning.assert_not_called()
            finally:
                mod.log = original
