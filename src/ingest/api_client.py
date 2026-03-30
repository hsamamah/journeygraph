"""
api_client.py — WMATA real-time API client.

Handles two response types:
  - JSON  : standard REST endpoints (elevator outages, incidents, etc.)
  - Protobuf: GTFS-RT feeds (trip updates, vehicle positions, alerts)

All methods return plain Python dicts or lists — no caller needs to
know about requests or protobuf internals.

Usage:
    client = WMATAClient()
    outages = client.get_elevator_outages()
    alerts  = client.get_gtfs_rt_alerts()
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

from google.transit import gtfs_realtime_pb2
import requests

from src.common.config import get_config
from src.common.logger import get_logger

logger = get_logger(__name__)

# ── Base URLs ─────────────────────────────────────────────────────────────────
_REST_BASE = "https://api.wmata.com"
_GTFSRT_BASE = "https://api.wmata.com/gtfs"

# ── Default request timeout (seconds) ────────────────────────────────────────
_TIMEOUT = 30


class WMATAClient:
    """Thin wrapper around the WMATA developer API."""

    def __init__(self, api_key: str | None = None):
        config = get_config()
        self._session = requests.Session()
        self._session.headers.update({"api_key": api_key or config.wmata_api_key})

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._session.close()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_json(self, path: str, params: dict | None = None) -> dict:
        url = f"{_REST_BASE}{path}"
        logger.info(f"GET (JSON) {url}")
        r = self._session.get(url, params=params or {}, timeout=_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def _get_protobuf(self, path: str) -> gtfs_realtime_pb2.FeedMessage:
        url = f"{_GTFSRT_BASE}{path}"
        logger.info(f"GET (Protobuf) {url}")
        r = self._session.get(url, timeout=_TIMEOUT)
        r.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(r.content)
        return feed

    # ── JSON endpoints ────────────────────────────────────────────────────────

    def get_elevator_outages(self) -> list[dict]:
        """
        Returns current elevator and escalator outages.
        Endpoint: /Incidents.svc/json/ElevatorIncidents
        """
        data = self._get_json("/Incidents.svc/json/ElevatorIncidents")
        outages = data.get("ElevatorIncidents", [])
        logger.info(f"Fetched {len(outages)} elevator/escalator outages")
        return outages

    def get_rail_incidents(self) -> list[dict]:
        """
        Returns current rail service incidents (delays, disruptions).
        Endpoint: /Incidents.svc/json/Incidents
        """
        data = self._get_json("/Incidents.svc/json/Incidents")
        incidents = data.get("Incidents", [])
        logger.info(f"Fetched {len(incidents)} rail incidents")
        return incidents

    def get_bus_incidents(self) -> list[dict]:
        """
        Returns current bus service incidents.
        Endpoint: /Incidents.svc/json/BusIncidents
        """
        data = self._get_json("/Incidents.svc/json/BusIncidents")
        incidents = data.get("BusIncidents", [])
        logger.info(f"Fetched {len(incidents)} bus incidents")
        return incidents

    # ── GTFS-RT Protobuf endpoints ────────────────────────────────────────────

    def get_gtfs_rt_alerts(self, source: str = "rail") -> gtfs_realtime_pb2.FeedMessage:
        """
        GTFS-RT service alerts feed (Protobuf).
        source: 'rail' or 'bus'
        """
        path = f"/{source}-gtfsrt-alerts.pb"
        feed = self._get_protobuf(path)
        logger.info(f"Fetched GTFS-RT {source} alerts — {len(feed.entity)} entities")
        return feed

    def get_gtfs_rt_trip_updates(
        self, source: str = "rail"
    ) -> gtfs_realtime_pb2.FeedMessage:
        """
        GTFS-RT trip updates feed (Protobuf).
        source: 'rail' or 'bus'
        """
        path = f"/{source}-gtfsrt-tripupdates.pb"
        feed = self._get_protobuf(path)
        logger.info(
            f"Fetched GTFS-RT {source} trip updates — {len(feed.entity)} entities"
        )
        return feed

    def get_all_trip_updates(self) -> list[tuple[gtfs_realtime_pb2.FeedMessage, str]]:
        """Fetch both rail and bus trip update feeds concurrently. Returns [(feed, source), ...]."""
        return self._fetch_all(self.get_gtfs_rt_trip_updates, "trip updates")

    def get_all_alerts(self) -> list[tuple[gtfs_realtime_pb2.FeedMessage, str]]:
        """Fetch both rail and bus alert feeds concurrently. Returns [(feed, source), ...]."""
        return self._fetch_all(self.get_gtfs_rt_alerts, "alerts")

    def _fetch_all(self, fetch_fn, label: str) -> list[tuple[gtfs_realtime_pb2.FeedMessage, str]]:
        results = []
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {pool.submit(fetch_fn, source=s): s for s in ("rail", "bus")}
            for fut in as_completed(futures):
                source = futures[fut]
                try:
                    results.append((fut.result(), f"gtfs_rt_{source}"))
                except Exception as exc:
                    logger.warning(f"Failed to fetch {source} {label}: {exc}")
        return results

    def get_gtfs_rt_vehicle_positions(self) -> gtfs_realtime_pb2.FeedMessage:
        """
        GTFS-RT vehicle positions feed (Protobuf).
        """
        feed = self._get_protobuf("/rail-gtfsrt-vehiclepositions.pb")
        logger.info(f"Fetched GTFS-RT vehicle positions — {len(feed.entity)} entities")
        return feed
