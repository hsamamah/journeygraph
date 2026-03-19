# src/common/feed_info.py
"""
Shared FeedInfo node management.

FeedInfo is feed-level metadata shared by all layers. Any layer's load.py
calls ensure_feed_info() as its first step. Uses MERGE so the first layer
to run creates the node; subsequent layers just match it.

Usage:
    from src.common.feed_info import ensure_feed_info

    def run(result, neo4j):
        feed_version = ensure_feed_info(neo4j, result.feed_info)
        # ... rest of load
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.common.logger import get_logger

if TYPE_CHECKING:
    import pandas as pd

log = get_logger(__name__)

# Cypher inlined here rather than in a .cypher file because:
#  - It's a single idempotent MERGE, not a layer-specific query set
#  - Every layer imports this module — no file path resolution needed
#  - Keeps the utility self-contained

_ENSURE_FEED_INFO = """
MERGE (fi:FeedInfo {feed_version: $feed_version})
SET   fi.feed_publisher_name = $feed_publisher_name,
      fi.feed_publisher_url  = $feed_publisher_url,
      fi.feed_lang           = $feed_lang,
      fi.feed_start_date     = $feed_start_date,
      fi.feed_end_date       = $feed_end_date,
      fi.feed_contact_email  = $feed_contact_email,
      fi.feed_contact_url    = $feed_contact_url
"""

_ENSURE_CONSTRAINT = """
CREATE CONSTRAINT feed_info_version IF NOT EXISTS
  FOR (n:FeedInfo) REQUIRE n.feed_version IS UNIQUE
"""


def ensure_feed_info(neo4j, feed_info_df: pd.DataFrame) -> str:
    """
    Ensure the FeedInfo node exists in Neo4j. Idempotent (MERGE).

    Args:
        neo4j: Neo4jManager instance
        feed_info_df: DataFrame with one row from feed_info.txt

    Returns:
        feed_version string (for use in FROM_FEED relationships)
    """
    if feed_info_df.empty:
        raise ValueError("feed_info DataFrame is empty — cannot create FeedInfo node")

    row = feed_info_df.iloc[0]

    params = {
        "feed_version": str(row.get("feed_version", "unknown")).strip(),
        "feed_publisher_name": str(row.get("feed_publisher_name", "")).strip() or None,
        "feed_publisher_url": str(row.get("feed_publisher_url", "")).strip() or None,
        "feed_lang": str(row.get("feed_lang", "")).strip() or None,
        "feed_start_date": str(row.get("feed_start_date", "")).strip() or None,
        "feed_end_date": str(row.get("feed_end_date", "")).strip() or None,
        "feed_contact_email": str(row.get("feed_contact_email", "")).strip() or None,
        "feed_contact_url": str(row.get("feed_contact_url", "")).strip() or None,
    }

    neo4j.execute_write(_ENSURE_CONSTRAINT)
    neo4j.execute_write(_ENSURE_FEED_INFO, parameters=params)
    log.info("feed_info: ensured FeedInfo node (version=%s)", params["feed_version"])

    return params["feed_version"]
