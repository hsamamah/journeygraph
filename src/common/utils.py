"""
utils.py — shared utility functions used across all layers.

Keep this module pure (no imports from src.*) so any layer can use it
without risking circular imports.
"""

from typing import Optional


def normalize_gtfs_time(time_str) -> Optional[int]:
    """
    Convert a GTFS HH:MM:SS time string to total seconds from start of service day.

    GTFS allows times past 24:00:00 for trips that run after midnight
    (e.g. '25:30:00' = 1.5 hours past midnight). This is intentional.

    Returns None for missing or null values.
    """
    if not time_str:
        return None
    s = str(time_str)
    if s == "nan":
        return None
    h, m, sec = map(int, s.split(":"))
    return (h * 3600) + (m * 60) + sec


def clean_str(value) -> Optional[str]:
    """Strip whitespace and return None for empty/null strings."""
    if value is None:
        return None
    s = str(value).strip()
    return None if s in ("", "nan") else s


def safe_int(value) -> Optional[int]:
    """Convert to int, returning None on failure."""
    try:
        return int(value)
    except TypeError, ValueError:
        return None


def safe_float(value) -> Optional[float]:
    """Convert to float, returning None on failure."""
    try:
        return float(value)
    except TypeError, ValueError:
        return None
