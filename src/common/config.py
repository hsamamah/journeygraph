"""
config.py — single source of truth for all environment variables.

All other modules import from here instead of calling os.getenv() directly.
Add new env vars here as the project grows.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. Check your .env file."
        )
    return val


# ── Neo4j ─────────────────────────────────────────────────────────────────────
NEO4J_URI = _require("NEO4J_URI")
NEO4J_USER = _require("NEO4J_USER")
NEO4J_PASSWORD = _require("NEO4J_PASSWORD")

# ── WMATA API ─────────────────────────────────────────────────────────────────
WMATA_API_KEY = _require("WMATA_API_KEY")

# ── GTFS source URL (can be overridden in .env) ───────────────────────────────
GTFS_FEED_URL = os.getenv(
    "GTFS_FEED_URL",
    "https://api.wmata.com/gtfs/rail-gtfs-static.zip",  # default WMATA feed
)
