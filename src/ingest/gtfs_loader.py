"""
gtfs_loader.py — Extract phase for GTFS static feed.

Responsibilities:
  1. Download the GTFS zip from WMATA (if not already cached locally).
  2. Extract it into data/gtfs/.
  3. Parse each CSV into a pandas DataFrame.
  4. Return a single dict[str, pd.DataFrame] keyed by GTFS filename stem.

This module is the only place that touches raw GTFS files.
All layer extract.py files receive the output of load() as their input.
"""

from pathlib import Path
import zipfile

import pandas as pd
import requests
from typing import Optional

from src.common.config import get_config
from src.common.logger import get_logger
from src.common.paths import GTFS_DIR, RAW_DIR

logger = get_logger(__name__)

# GTFS files we care about — add new ones here as layers expand
GTFS_FILES = [
    "agency",
    "routes",
    "trips",
    "stops",
    "stop_times",
    "calendar",
    "calendar_dates",
    "shapes",
    "pathways",
    "levels",
    "fare_leg_rules",
    "fare_products",
    "fare_media",
    "fare_transfer_rules",
    "feed_info",
]

# dtypes to enforce on load — prevents silent int/float coercion on IDs
DTYPE_OVERRIDES: dict[str, dict] = {
    "stops": {"stop_id": str, "parent_station": str},
    "trips": {
        "trip_id": str,
        "route_id": str,
        "service_id": str,
        "shape_id": str,
        "block_id": str,
    },
    "stop_times": {"trip_id": str, "stop_id": str},
    "routes": {"route_id": str, "agency_id": str},
    "calendar": {"service_id": str},
    "calendar_dates": {"service_id": str},
    "pathways": {"pathway_id": str, "from_stop_id": str, "to_stop_id": str},
    "fare_leg_rules": {
        "leg_group_id": str,
        "network_id": str,
        "from_area_id": str,
        "to_area_id": str,
    },
    "fare_products": {"fare_product_id": str, "fare_media_id": str},
    "fare_media": {"fare_media_id": str},
    "fare_transfer_rules": {
        "from_leg_group_id": str,
        "to_leg_group_id": str,
        "fare_product_id": str,
    },
}


def download(force: bool = False) -> Path:
    """Download the GTFS zip to data/raw/. Skip if already present unless force=True."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = RAW_DIR / "gtfs.zip"

    if zip_path.exists() and not force:
        logger.info(
            f"GTFS zip already exists at {zip_path} — skipping download. "
            f"Pass force=True to re-download."
        )
        return zip_path

    config = get_config()
    logger.info(f"Downloading GTFS feed from {config.gtfs_feed_url} ...")
    headers = {"api_key": config.wmata_api_key}
    response = requests.get(config.gtfs_feed_url, headers=headers, timeout=60)
    response.raise_for_status()

    zip_path.write_bytes(response.content)
    logger.info(f"Downloaded {len(response.content) / 1_000_000:.1f} MB → {zip_path}")
    return zip_path


def extract_zip(zip_path: Path, force: bool = False) -> None:
    """Unzip GTFS archive into data/gtfs/. Skip if already extracted unless force=True."""
    GTFS_DIR.mkdir(parents=True, exist_ok=True)

    if any(GTFS_DIR.iterdir()) and not force:
        logger.info(f"GTFS CSVs already extracted at {GTFS_DIR} — skipping extraction.")
        return

    logger.info(f"Extracting {zip_path} → {GTFS_DIR} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(GTFS_DIR)
    logger.info("Extraction complete.")


def _parse_file(name: str) -> Optional[pd.DataFrame]:
    """Parse a single GTFS CSV file into a DataFrame. Returns None if file absent."""
    path = GTFS_DIR / f"{name}.txt"
    if not path.exists():
        logger.warning(f"Expected GTFS file not found, skipping: {path.name}")
        return None

    dtypes = DTYPE_OVERRIDES.get(name, {})
    df = pd.read_csv(path, dtype=dtypes, low_memory=False)
    df.columns = df.columns.str.strip()  # strip accidental whitespace in headers
    logger.info(f"  Loaded {name:30s} — {len(df):>8,} rows, {len(df.columns)} cols")
    return df


def load(
    force_download: bool = False, force_extract: bool = False
) -> dict[str, pd.DataFrame]:
    """
    Full GTFS ingest: download → extract → parse → return.

    Args:
        force_download: Re-download zip even if cached.
        force_extract:  Re-extract CSVs even if already present.

    Returns:
        Dict keyed by GTFS file stem (e.g. "stops", "routes").
        Missing optional files are omitted from the dict.
    """
    logger.info("═" * 60)
    logger.info("GTFS LOAD START")
    logger.info("═" * 60)

    zip_path = download(force=force_download)
    extract_zip(zip_path, force=force_extract)

    logger.info("Parsing GTFS CSVs ...")
    data: dict[str, pd.DataFrame] = {}
    for name in GTFS_FILES:
        df = _parse_file(name)
        if df is not None:
            data[name] = df

    logger.info("═" * 60)
    logger.info(f"GTFS LOAD COMPLETE — {len(data)} files loaded")
    logger.info("═" * 60)
    return data
