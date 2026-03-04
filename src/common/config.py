"""
config.py — single source of truth for all environment variables.

All other modules import from here instead of calling os.getenv() directly.
Add new env vars here as the project grows.
"""

from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise OSError(
            f"Required environment variable '{key}' is not set. Check your .env file."
        )
    return val


@dataclass(frozen=True)
class Config:
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    wmata_api_key: str
    gtfs_feed_url: str


def get_config() -> Config:
    """
    Build and validate config from environment variables.
    Raises EnvironmentError if any required variable is missing.
    Call this at runtime (in pipeline.py), not at module import time.
    """
    return Config(
        neo4j_uri=_require("NEO4J_URI"),
        neo4j_user=_require("NEO4J_USER"),
        neo4j_password=_require("NEO4J_PASSWORD"),
        wmata_api_key=_require("WMATA_API_KEY"),
        gtfs_feed_url=os.getenv(
            "GTFS_FEED_URL",
            "https://api.wmata.com/gtfs/rail-bus-gtfs-static.zip",
        ),
    )
