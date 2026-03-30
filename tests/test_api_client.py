# tests/test_api_client.py
"""
Tests for src/ingest/api_client.py

Covers: WMATAClient context manager, _fetch_all concurrency, and
per-source error isolation in get_all_trip_updates / get_all_alerts.
"""

from unittest.mock import MagicMock, patch, call
import pytest

from src.ingest.api_client import WMATAClient


@pytest.fixture
def client():
    """WMATAClient with a mocked session (no real HTTP or config needed)."""
    with patch("src.ingest.api_client.get_config") as mock_cfg:
        mock_cfg.return_value.wmata_api_key = "test-key"
        c = WMATAClient()
        c._session = MagicMock()
        yield c


# ── Context manager ───────────────────────────────────────────────────────────

class TestContextManager:
    def test_enter_returns_self(self, client):
        assert client.__enter__() is client

    def test_exit_closes_session(self, client):
        client.__exit__(None, None, None)
        client._session.close.assert_called_once()

    def test_with_statement(self):
        with patch("src.ingest.api_client.get_config") as mock_cfg:
            mock_cfg.return_value.wmata_api_key = "key"
            with WMATAClient() as c:
                c._session = MagicMock()
                session = c._session
        session.close.assert_called_once()


# ── _fetch_all ────────────────────────────────────────────────────────────────

class TestFetchAll:
    def test_returns_results_for_both_sources(self, client):
        feed = MagicMock()
        fetch_fn = MagicMock(return_value=feed)

        results = client._fetch_all(fetch_fn, "test")

        assert len(results) == 2
        sources = {src for _, src in results}
        assert sources == {"gtfs_rt_rail", "gtfs_rt_bus"}

    def test_fetch_fn_called_for_each_source(self, client):
        fetch_fn = MagicMock(return_value=MagicMock())

        client._fetch_all(fetch_fn, "test")

        assert fetch_fn.call_count == 2
        calls = {c.kwargs["source"] for c in fetch_fn.call_args_list}
        assert calls == {"rail", "bus"}

    def test_failed_source_is_skipped_not_raised(self, client):
        def flaky(source):
            if source == "bus":
                raise ConnectionError("timeout")
            return MagicMock()

        results = client._fetch_all(flaky, "test")

        assert len(results) == 1
        _, src = results[0]
        assert src == "gtfs_rt_rail"

    def test_both_sources_fail_returns_empty(self, client):
        fetch_fn = MagicMock(side_effect=ConnectionError("down"))
        results = client._fetch_all(fetch_fn, "test")
        assert results == []


# ── get_all_trip_updates / get_all_alerts ─────────────────────────────────────

class TestGetAllMethods:
    def test_get_all_trip_updates_delegates_to_fetch_all(self, client):
        expected = [(MagicMock(), "gtfs_rt_rail"), (MagicMock(), "gtfs_rt_bus")]
        with patch.object(client, "_fetch_all", return_value=expected) as mock_fetch:
            result = client.get_all_trip_updates()
        mock_fetch.assert_called_once_with(client.get_gtfs_rt_trip_updates, "trip updates")
        assert result is expected

    def test_get_all_alerts_delegates_to_fetch_all(self, client):
        expected = [(MagicMock(), "gtfs_rt_rail"), (MagicMock(), "gtfs_rt_bus")]
        with patch.object(client, "_fetch_all", return_value=expected) as mock_fetch:
            result = client.get_all_alerts()
        mock_fetch.assert_called_once_with(client.get_gtfs_rt_alerts, "alerts")
        assert result is expected
