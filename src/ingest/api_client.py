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

import requests
from google.transit import gtfs_realtime_pb2

from src.common.config import WMATA_API_KEY
from src.common.logger import get_logger

logger = get_logger(__name__)

# ── Base URLs ─────────────────────────────────────────────────────────────────
_REST_BASE = "https://api.wmata.com"
_GTFSRT_BASE = "https://api.wmata.com/gtfs"

# ── Default request timeout (seconds) ────────────────────────────────────────
_TIMEOUT = 30


class WMATAClient:
    """Thin wrapper around the WMATA developer API."""

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({"api_key": WMATA_API_KEY})

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_json(self, path: str, params: dict = None) -> dict:
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

    def get_gtfs_rt_alerts(self) -> gtfs_realtime_pb2.FeedMessage:
        """
        GTFS-RT service alerts feed (Protobuf).
        Callers can iterate feed.entity for individual alerts.
        """
        feed = self._get_protobuf("/rail-gtfsrt-alerts.pb")
        logger.info(f"Fetched GTFS-RT alerts — {len(feed.entity)} entities")
        return feed

    def get_gtfs_rt_trip_updates(self) -> gtfs_realtime_pb2.FeedMessage:
        """
        GTFS-RT trip updates feed (Protobuf).
        Contains real-time arrival/departure predictions.
        """
        feed = self._get_protobuf("/rail-gtfsrt-tripupdates.pb")
        logger.info(f"Fetched GTFS-RT trip updates — {len(feed.entity)} entities")
        return feed

    def get_gtfs_rt_vehicle_positions(self) -> gtfs_realtime_pb2.FeedMessage:
        """
        GTFS-RT vehicle positions feed (Protobuf).
        """
        feed = self._get_protobuf("/rail-gtfsrt-vehiclepositions.pb")
        logger.info(f"Fetched GTFS-RT vehicle positions — {len(feed.entity)} entities")
        return feed
