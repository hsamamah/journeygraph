"""
utils.py — shared utility functions used across all layers.

Keep this module pure (no imports from src.*) so any layer can use it
without risking circular imports.
"""


def normalize_gtfs_time(time_str) -> int | None:
    """
    Convert a GTFS HH:MM:SS time string to total seconds from start of service day.

    GTFS allows times past 24:00:00 for trips that run after midnight
    (e.g. '25:30:00' = 1.5 hours past midnight). This is intentional.

    Returns None for missing or null values.
    """
    if not time_str or str(time_str) == "nan":
        return None
    h, m, s = map(int, str(time_str).split(":"))
    return (h * 3600) + (m * 60) + s


def clean_str(value) -> str | None:
    """Strip whitespace and return None for empty/null strings."""
    if value is None or str(value).strip() in ("", "nan"):
        return None
    return str(value).strip()


def safe_int(value) -> int | None:
    """Convert to int, returning None on failure."""
    try:
        return int(value)
    except TypeError, ValueError:
        return None


def safe_float(value) -> float | None:
    """Convert to float, returning None on failure."""
    try:
        return float(value)
    except TypeError, ValueError:
        return None
