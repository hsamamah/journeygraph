# src/layers/accessibility/pathway_joiner.py
"""
Accessibility layer — Pathway Joiner

Resolves each OutageEvent to the specific :Pathway node it affects, producing
the (composite_key, pathway_id) pairs needed for the AFFECTS relationship.

The WMATA REST API (unit_name, station_code, unit_type, location_description)
and GTFS pathway data use incompatible identifier conventions — no shared key
exists. Resolution uses a two-tier approach (schema §5):

  Tier 1 — Programmatic join (standard stations, ~94 of 98 stations)
  ─────────────────────────────────────────────────────────────────
  Elevator/Escalator Pathway nodes carry DEFERRED stop_ids
  (e.g. NODE_A02_W_ESC1_BT) on their from_stop_id / to_stop_id properties.
  These encode: station code + directional zone + equipment type + sequence +
  position, which supports a four-part filter:

    1. Station   — station_code from API matches NODE_ stop_id prefix segment
    2. Zone      — unit_name[3] (direction letter W/N/S/E) appears in stop_id
    3. Type      — unit_type ESCALATOR → 'ESC', ELEVATOR → 'ELE'|'ELV'
    4. Segment   — location_description keyword ('street'|'platform') maps to
                   '_BT' (bottom) or '_TP' (top) position in stop_id

  Tier 2 — Static lookup (4 complex interchange stations)
  ──────────────────────────────────────────────────────
  Metro Center (A01/C01), Gallery Place (B01/F01), L'Enfant Plaza (D03/F03),
  Fort Totten (B06/E06) break the programmatic join due to multi-code
  identifiers, X-zone letters, and high unit counts. Unit names that match a
  known prefix in _STATIC_LOOKUP are resolved directly to a pathway_id.

  See schema §5.2–5.4 and §9 (deferred items) for full rationale.

Graph query:
  One round-trip per poll fetches all elevator/escalator Pathway candidates
  (mode 4 or 5) with their from_stop_id and to_stop_id. Matching is done in
  Python on the resulting DataFrame — no per-outage DB queries.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from src.common.logger import get_logger

if TYPE_CHECKING:
    from src.common.neo4j_tools import Neo4jManager

log = get_logger(__name__)

# ── GTFS stops.txt path ───────────────────────────────────────────────────────

_GTFS_STOPS = Path(__file__).parents[3] / "data" / "gtfs" / "stops.txt"

# ── Static lookup table (Tier 2) ─────────────────────────────────────────────
#
# Maps (unit_name, unit_type) → canonical pathway_id for the 4 complex
# interchange stations.  unit_type is "ESCALATOR" or "ELEVATOR" (uppercase,
# matching the WMATA API value).
#
# Key is a 2-tuple because some zone+seq combinations exist for both equipment
# types at the same station (e.g. D03W01 can be an elevator OR an escalator).
#
# Generated from GTFS stops.txt + pathways.txt by cross-referencing NODE_
# stop_id structure against the zone/seq encoding in WMATA unit_names.
# Canonical pathway = the row whose from_stop_id ends with _BT (bottom node),
# or the first pathway_id by sort when no _BT row exists.
#
# Coverage: all ESC/ELE pathways (mode 4/5) at the 4 complex stations.
# Missing entries occur where the WMATA seq number has no GTFS equivalent
# (station renumbering divergence) — those fall through to tier 1.

_STATIC_LOOKUP: dict[tuple[str, str], str] = {
    # ── Metro Center (A01/C01) ────────────────────────────────────────────────
    ("A01E01", "ESCALATOR"): "A01_C01_104175",  # E ESC1  street→mezzanine
    ("A01E02", "ESCALATOR"): "A01_C01_104184",  # E ESC2  mezzanine→upper platform
    ("A01E03", "ESCALATOR"): "A01_C01_104181",  # E ESC3  mezzanine→upper platform
    ("A01E04", "ESCALATOR"): "A01_C01_104187",  # E ESC4  mezzanine→upper platform
    ("A01E05", "ESCALATOR"): "A01_C01_104190",  # E ESC5  mezzanine→upper platform
    ("A01N01", "ELEVATOR"):  "A01_C01_104115",  # N ELE1  street→mezzanine
    ("A01N01", "ESCALATOR"): "A01_C01_104111",  # N ESC1  street→mezzanine
    ("A01N02", "ELEVATOR"):  "A01_C01_104126",  # N ELE2  mezzanine→lower platform
    ("A01N03", "ESCALATOR"): "A01_C01_104131",  # N ESC3  mezzanine→lower platform
    ("A01S01", "ELEVATOR"):  "A01_C01_104146",  # S ELE1  mezzanine→lower platform
    ("A01S01", "ESCALATOR"): "A01_C01_104135",  # S ESC1  street→mezzanine
    ("A01S03", "ESCALATOR"): "A01_C01_104151",  # S ESC3  mezzanine→lower platform
    ("A01S06", "ESCALATOR"): "A01_C01_104199",  # S transfer esc upper→lower platform
    ("A01W01", "ESCALATOR"): "A01_C01_104157",  # W ESC1  street→mezzanine
    ("A01W02", "ESCALATOR"): "A01_C01_104163",  # W ESC2  mezzanine→upper platform
    ("A01W03", "ESCALATOR"): "A01_C01_104166",  # W ESC3  mezzanine→upper platform
    ("A01W04", "ESCALATOR"): "A01_C01_104172",  # W ESC4  mezzanine→upper platform
    ("A01W05", "ESCALATOR"): "A01_C01_104169",  # W ESC5  mezzanine→upper platform
    # C01 aliases (same physical units, alternate station code)
    ("C01E01", "ESCALATOR"): "A01_C01_104175",
    ("C01E02", "ESCALATOR"): "A01_C01_104184",
    ("C01E03", "ESCALATOR"): "A01_C01_104181",
    ("C01E04", "ESCALATOR"): "A01_C01_104187",
    ("C01E05", "ESCALATOR"): "A01_C01_104190",
    ("C01N01", "ELEVATOR"):  "A01_C01_104115",
    ("C01N01", "ESCALATOR"): "A01_C01_104111",
    ("C01N02", "ELEVATOR"):  "A01_C01_104126",
    ("C01N03", "ESCALATOR"): "A01_C01_104131",
    ("C01S01", "ELEVATOR"):  "A01_C01_104146",
    ("C01S01", "ESCALATOR"): "A01_C01_104135",
    ("C01S03", "ESCALATOR"): "A01_C01_104151",
    ("C01S06", "ESCALATOR"): "A01_C01_104199",  # S transfer esc upper→lower platform
    ("C01W01", "ESCALATOR"): "A01_C01_104157",
    ("C01W02", "ESCALATOR"): "A01_C01_104163",
    ("C01W03", "ESCALATOR"): "A01_C01_104166",
    ("C01W04", "ESCALATOR"): "A01_C01_104172",
    ("C01W05", "ESCALATOR"): "A01_C01_104169",

    # ── Gallery Place (B01/F01) ───────────────────────────────────────────────
    ("B01E00", "ELEVATOR"):  "B01_F01_119138",  # E ELE    street→platform
    ("B01E00", "ESCALATOR"): "B01_F01_119131",  # E ESC    street→mezzanine
    ("B01E01", "ELEVATOR"):  "B01_F01_119143",  # E ELE1   lower mezzanine→platform
    ("B01E01", "ESCALATOR"): "B01_F01_119177",  # E ESC1   mezzanine→platform
    ("B01E02", "ELEVATOR"):  "B01_F01_119167",  # E ELE2   lower mezzanine→platform
    ("B01E02", "ESCALATOR"): "B01_F01_119180",  # E ESC2   mezzanine→platform
    ("B01E03", "ESCALATOR"): "B01_F01_119183",  # E ESC3   lower mezzanine→platform
    ("B01E04", "ESCALATOR"): "B01_F01_119193",  # E ESC4   lower mezzanine→platform
    ("B01E05", "ESCALATOR"): "B01_F01_119198",  # E ESC5   lower mezzanine→platform
    ("B01E06", "ESCALATOR"): "B01_F01_119161",  # E ESC6   mezzanine→platform
    ("B01E07", "ESCALATOR"): "B01_F01_119171",  # E ESC7   mezzanine→platform
    ("B01E08", "ESCALATOR"): "B01_F01_119174",  # E ESC8   lower mezzanine→platform
    ("B01E09", "ESCALATOR"): "B01_F01_119165",  # E ESC9   lower mezzanine→platform
    ("B01E10", "ESCALATOR"): "B01_F01_119157",  # E ESC10  lower mezzanine→platform
    ("B01E11", "ESCALATOR"): "B01_F01_119147",  # E ESC11  street→intermediate
    ("B01E12", "ESCALATOR"): "B01_F01_119150",  # E ESC12  intermediate→mezzanine
    ("B01W01", "ESCALATOR"): "B01_F01_119111",  # W ESC1   street→mezzanine
    ("B01W02", "ESCALATOR"): "B01_F01_119117",  # W ESC2   mezzanine→platform
    ("B01W03", "ESCALATOR"): "B01_F01_119120",  # W ESC3   mezzanine→platform
    ("B01W04", "ESCALATOR"): "B01_F01_119123",  # W ESC4   mezzanine→platform
    ("B01W05", "ESCALATOR"): "B01_F01_119126",  # W ESC5   mezzanine→platform
    # F01 aliases
    ("F01E00", "ELEVATOR"):  "B01_F01_119138",
    ("F01E00", "ESCALATOR"): "B01_F01_119131",
    ("F01E01", "ELEVATOR"):  "B01_F01_119143",
    ("F01E01", "ESCALATOR"): "B01_F01_119177",
    ("F01E02", "ELEVATOR"):  "B01_F01_119167",
    ("F01E02", "ESCALATOR"): "B01_F01_119180",
    ("F01E03", "ESCALATOR"): "B01_F01_119183",
    ("F01E04", "ESCALATOR"): "B01_F01_119193",
    ("F01E05", "ESCALATOR"): "B01_F01_119198",
    ("F01E06", "ESCALATOR"): "B01_F01_119161",
    ("F01E07", "ESCALATOR"): "B01_F01_119171",
    ("F01E08", "ESCALATOR"): "B01_F01_119174",
    ("F01E09", "ESCALATOR"): "B01_F01_119165",
    ("F01E10", "ESCALATOR"): "B01_F01_119157",
    ("F01E11", "ESCALATOR"): "B01_F01_119147",
    ("F01E12", "ESCALATOR"): "B01_F01_119150",
    ("F01W01", "ESCALATOR"): "B01_F01_119111",
    ("F01W02", "ESCALATOR"): "B01_F01_119117",
    ("F01W03", "ESCALATOR"): "B01_F01_119120",
    ("F01W04", "ESCALATOR"): "B01_F01_119123",
    ("F01W05", "ESCALATOR"): "B01_F01_119126",

    # ── L'Enfant Plaza (D03/F03) ──────────────────────────────────────────────
    ("D03E01", "ESCALATOR"): "D03_F03_146168",  # E ESC1  street→mezzanine
    ("D03E02", "ESCALATOR"): "D03_F03_146174",  # E ESC2  mezzanine→platform
    ("D03E03", "ESCALATOR"): "D03_F03_146181",  # E ESC3  mezzanine→platform
    ("D03N01", "ELEVATOR"):  "D03_F03_146114",  # N ELE1  street→mezzanine
    ("D03N01", "ESCALATOR"): "D03_F03_146111",  # N ESC1  street→mezzanine
    ("D03N02", "ELEVATOR"):  "D03_F03_146120",  # N ELE2  mezzanine→platform
    ("D03N02", "ESCALATOR"): "D03_F03_146126",  # N ESC2  mezzanine→platform
    ("D03N03", "ELEVATOR"):  "D03_F03_146123",  # N ELE3  mezzanine→platform
    ("D03N03", "ESCALATOR"): "D03_F03_146128",  # N ESC3  mezzanine→platform
    ("D03N04", "ESCALATOR"): "D03_F03_146134",  # N ESC4  mezzanine→platform
    ("D03N05", "ESCALATOR"): "D03_F03_146136",  # N ESC5  mezzanine→platform
    ("D03N06", "ESCALATOR"): "D03_F03_146141",  # N ESC6  mezzanine→platform
    ("D03N07", "ESCALATOR"): "D03_F03_146142",  # N ESC7  mezzanine→platform
    ("D03W01", "ELEVATOR"):  "D03_F03_146157",  # W ELE1  mezzanine→platform
    ("D03W01", "ESCALATOR"): "D03_F03_146146",  # W ESC1  street→mezzanine
    ("D03W02", "ESCALATOR"): "D03_F03_146153",  # W ESC2  mezzanine→platform
    ("D03W03", "ESCALATOR"): "D03_F03_146162",  # W ESC3  mezzanine→platform
    # F03 aliases
    ("F03E01", "ESCALATOR"): "D03_F03_146168",
    ("F03E02", "ESCALATOR"): "D03_F03_146174",
    ("F03E03", "ESCALATOR"): "D03_F03_146181",
    ("F03N01", "ELEVATOR"):  "D03_F03_146114",
    ("F03N01", "ESCALATOR"): "D03_F03_146111",
    ("F03N02", "ELEVATOR"):  "D03_F03_146120",
    ("F03N02", "ESCALATOR"): "D03_F03_146126",
    ("F03N03", "ELEVATOR"):  "D03_F03_146123",
    ("F03N03", "ESCALATOR"): "D03_F03_146128",
    ("F03N04", "ESCALATOR"): "D03_F03_146134",
    ("F03N05", "ESCALATOR"): "D03_F03_146136",
    ("F03N06", "ESCALATOR"): "D03_F03_146141",
    ("F03N07", "ESCALATOR"): "D03_F03_146142",
    ("F03W01", "ELEVATOR"):  "D03_F03_146157",
    ("F03W01", "ESCALATOR"): "D03_F03_146146",
    ("F03W02", "ESCALATOR"): "D03_F03_146153",
    ("F03W03", "ESCALATOR"): "D03_F03_146162",

    # ── Station-specific overrides (spatially ambiguous units) ───────────────
    # Units whose location descriptions contain directional cues (e.g. "west side
    # of Wisconsin Avenue") that the programmatic join cannot resolve.
    ("A07X04", "ESCALATOR"): "A07_110114",  # W ESC  street→mezzanine west side

    # ── Fort Totten (B06/E06) ─────────────────────────────────────────────────
    # Zone-less station: WMATA unit names use 'X' as zone letter.
    ("B06X00", "ELEVATOR"):  "B06_E06_124120",  # ELE     mezzanine upper/lower platform
    ("B06X01", "ESCALATOR"): "B06_E06_124115",  # ESC1    mezzanine→platform
    ("B06X02", "ESCALATOR"): "B06_E06_124127",  # ESC2    mezzanine→platform
    ("B06X04", "ESCALATOR"): "B06_E06_124136",  # ESC4    mezzanine→platform
    # E06 aliases
    ("E06X00", "ELEVATOR"):  "B06_E06_124120",
    ("E06X01", "ESCALATOR"): "B06_E06_124115",
    ("E06X02", "ESCALATOR"): "B06_E06_124127",
    ("E06X04", "ESCALATOR"): "B06_E06_124136",
}

_COMPLEX_STATION_CODES = frozenset({"A01", "C01", "B01", "F01", "D03", "F03", "B06", "E06"})


# ── Graph query ───────────────────────────────────────────────────────────────


def _fetch_pathway_candidates(neo4j: Neo4jManager) -> pd.DataFrame:
    """
    Fetch all elevator and escalator Pathway nodes from Neo4j.

    Returns DataFrame[pathway_id, from_stop_id, to_stop_id, mode] containing
    only pathways with mode 4 (escalator) or 5 (elevator).
    One round-trip per poll — all matching is done in Python.
    """
    rows = neo4j.query(
        """
        MATCH (p:Pathway)
        WHERE p.mode IN [4, 5]
        RETURN p.id          AS pathway_id,
               p.from_stop_id AS from_stop_id,
               p.to_stop_id   AS to_stop_id,
               p.mode         AS mode
        """
    )
    if not rows:
        log.warning("pathway_joiner: no elevator/escalator Pathway nodes found in graph")
        return pd.DataFrame(columns=["pathway_id", "from_stop_id", "to_stop_id", "mode"])

    df = pd.DataFrame(rows)
    log.info("pathway_joiner: fetched %d elevator/escalator pathway candidates", len(df))
    return df


# ── Tier 1: Cascaded programmatic join (Strategy F) ──────────────────────────
#
# Resolves outages by cascading through 7 sub-strategies, returning on the
# first unambiguous match:
#
#   F1  Description alone (GTFS-side synonym-expanded)
#   F2  Seq-number alone (zone-conditional)
#   F3  Description narrows pool → seq or BT-endpoint tiebreak
#   F4  Seq narrows pool → description or BT tiebreak (≈ Strategy D)
#   F5  Synonym-expanded description → seq or BT tiebreak
#   F6  Singleton at station (only one unit of this type)
#   F7  Final tiebreaker: description-filtered pool, lowest-seq BT endpoint
#
# Achieves ~92 % match rate on live WMATA outage data vs 0 % for the old
# full-text index approach (which scored 4 %).

# ── GTFS stop description helpers ────────────────────────────────────────────

def _load_stop_descriptions(path: Path = _GTFS_STOPS) -> dict[str, str]:
    """Return {stop_id: stop_desc} for all NODE_ stops in stops.txt."""
    result: dict[str, str] = {}
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            sid = row.get("stop_id", "")
            if sid.startswith("NODE_"):
                desc = row.get("stop_desc", "").strip().strip('"')
                if desc:
                    result[sid] = desc
    return result


_SEQ_RE = re.compile(r"(?:ESC|ELE|ELV)(\d+)", re.IGNORECASE)


def _enrich_candidates(
    candidates: pd.DataFrame,
    stop_desc: dict[str, str],
) -> pd.DataFrame:
    """Add from_desc, to_desc, from_seq, to_seq columns to the candidates DataFrame."""
    df = candidates.copy()
    df["from_desc"] = df["from_stop_id"].map(stop_desc).fillna("")
    df["to_desc"]   = df["to_stop_id"].map(stop_desc).fillna("")
    df["from_seq"]  = df["from_stop_id"].apply(lambda s: _extract_seq_from_stop(s))
    df["to_seq"]    = df["to_stop_id"].apply(lambda s: _extract_seq_from_stop(s))
    return df


def _extract_seq_from_stop(stop_id: str) -> int | None:
    m = _SEQ_RE.search(stop_id or "")
    return int(m.group(1)) if m else None


# ── Description segment normalisation ────────────────────────────────────────

# Stops capture at "to <destination>" or "," — prevents including GTFS line
# destination suffixes (e.g. "to Vienna/Franconia-Springfield") in the key.
_BETWEEN_RE = re.compile(r"between\s+(.+?)(?:\s+to\s+|\s*,|\s*$)", re.IGNORECASE)

# WMATA informal terms mapped to their GTFS equivalents.
# Applied to both WMATA descriptions (for key derivation) and GTFS descriptions
# (via _desc_filter_extended) to normalise vocabulary mismatches.
_WMATA_SYNONYMS: dict[str, str] = {
    "middle landing":             "intermediate passage",
    "middle  landing":            "intermediate passage",
    "amtrak station":             "street",
    "main entrance":              "street",
    "platform level":             "platform",
    "mezzanine level":            "mezzanine",
    "main concourse":             "mezzanine",
    "intermediate level":         "intermediate passage",
    # GTFS line-qualified platform names → generic "platform"
    "silver line platform":       "platform",
    "blue/orange lines platform": "platform",
    "red line platform":          "platform",
    "green line platform":        "platform",
    "yellow line platform":       "platform",
}


def _apply_synonyms(text: str) -> str:
    """Normalise WMATA-specific terms to their GTFS equivalents."""
    t = text.lower()
    for wmata_term, gtfs_term in _WMATA_SYNONYMS.items():
        t = t.replace(wmata_term, gtfs_term)
    # Strip trailing " level" to normalise "Platform Level" → "Platform"
    t = re.sub(r"\s+level\b", "", t)
    return t


def _desc_segment_key(text: str) -> str | None:
    """
    Normalise a 'between X and Y' description to a canonical, order-independent key.

    "Bottom of Escalator Between Street and Mezzanine" → "mezzanine_street"
    "Escalator between street and mezzanine"           → "mezzanine_street"

    Nouns are sorted so "A and B" == "B and A".
    """
    text_norm = re.sub(r"\s+", " ", text.strip().lower())
    m = _BETWEEN_RE.search(text_norm)
    if not m:
        return None
    phrase = m.group(1).strip()
    parts = re.split(r"\s+and\s+", phrase, maxsplit=1)
    norm_parts = [re.sub(r"\s+", "_", p.strip()) for p in parts]
    return "_".join(sorted(norm_parts))


def _desc_segment_key_extended(text: str) -> str | None:
    """Like _desc_segment_key but with synonym expansion applied first."""
    return _desc_segment_key(_apply_synonyms(text))


# ── Candidate filtering helpers ───────────────────────────────────────────────


def _mode_mask(candidates: pd.DataFrame, unit_type: str) -> pd.Series:
    if unit_type == "ESCALATOR":
        return candidates["mode"] == 4
    elif unit_type == "ELEVATOR":
        return candidates["mode"] == 5
    return pd.Series(False, index=candidates.index)


def _station_mask(candidates: pd.DataFrame, station_code: str) -> pd.Series:
    token = f"NODE_{station_code}_"
    return (
        candidates["from_stop_id"].fillna("").str.contains(token, regex=False)
        | candidates["to_stop_id"].fillna("").str.contains(token, regex=False)
    )


def _zone_mask(candidates: pd.DataFrame, zone_letter: str) -> pd.Series:
    token = f"_{zone_letter}_"
    return (
        candidates["from_stop_id"].fillna("").str.contains(token, regex=False)
        | candidates["to_stop_id"].fillna("").str.contains(token, regex=False)
    )


def _desc_filter(df: pd.DataFrame, key: str | None) -> pd.DataFrame:
    """Filter df to rows whose from_desc or to_desc segment key matches key."""
    if not key:
        return pd.DataFrame(columns=df.columns)
    from_keys = df["from_desc"].apply(_desc_segment_key)
    to_keys   = df["to_desc"].apply(_desc_segment_key)
    return df[(from_keys == key) | (to_keys == key)]


def _desc_filter_extended(df: pd.DataFrame, key: str | None) -> pd.DataFrame:
    """Like _desc_filter but applies synonym expansion to GTFS descriptions first."""
    if not key:
        return pd.DataFrame(columns=df.columns)
    from_keys = df["from_desc"].apply(_desc_segment_key_extended)
    to_keys   = df["to_desc"].apply(_desc_segment_key_extended)
    return df[(from_keys == key) | (to_keys == key)]


def _extract_seq_from_unit(unit_name: str) -> int | None:
    """Extract the 2-digit numeric suffix from a WMATA unit_name (e.g. A08N03 → 3).

    Returns None for sequence 00 — treated as absent rather than 0, since
    GTFS stop_ids do not use ELE0/ESC0 suffixes and a seq of 0 would produce
    false matches at any station with no sequence-0 pathway.
    """
    if len(unit_name) < 6:
        return None
    try:
        seq = int(unit_name[4:6].lstrip("0") or "0")
        return seq if seq != 0 else None
    except ValueError:
        return None


# ── Tier 1 match (Strategy F cascade) ────────────────────────────────────────


def _tier1_match(outage: pd.Series, candidates: pd.DataFrame) -> str | None:
    """
    Cascade join resolving a WMATA outage to a Pathway node.

    Args:
        outage:     One row from transform.run().outages.
        candidates: Enriched DataFrame from _enrich_candidates().

    Returns:
        pathway_id string on success, None if unresolvable.
    """
    station_code  = outage.get("station_code", "")
    unit_name     = str(outage.get("unit_name", ""))
    unit_type     = str(outage.get("unit_type", "")).upper()
    location_desc = str(outage.get("location_description", ""))

    if not station_code or len(unit_name) < 4:
        return None

    mode_c = candidates[_mode_mask(candidates, unit_type)]
    if mode_c.empty:
        return None

    station_c = mode_c[_station_mask(mode_c, station_code)]
    if station_c.empty:
        return None

    # ── F1: description alone (GTFS-side synonym-expanded) ───────────────────
    wmata_key = _desc_segment_key(location_desc)
    desc_c = _desc_filter_extended(station_c, wmata_key) if wmata_key else pd.DataFrame()
    if len(desc_c) == 1:
        return desc_c.iloc[0]["pathway_id"]

    # ── F2: seq alone (zone-conditional) ─────────────────────────────────────
    seq         = _extract_seq_from_unit(unit_name)
    zone_letter = unit_name[3].upper()
    zone_exists = _zone_mask(station_c, zone_letter).any()
    zone_c      = station_c[_zone_mask(station_c, zone_letter)] if zone_exists else station_c
    seq_c = zone_c[(zone_c["from_seq"] == seq) | (zone_c["to_seq"] == seq)] if seq else pd.DataFrame()
    if len(seq_c) == 1:
        return seq_c.iloc[0]["pathway_id"]

    # ── F3: description narrows pool → seq or BT tiebreak ────────────────────
    if wmata_key and not desc_c.empty:
        if seq:
            seq_on_desc = desc_c[(desc_c["from_seq"] == seq) | (desc_c["to_seq"] == seq)]
            if len(seq_on_desc) == 1:
                return seq_on_desc.iloc[0]["pathway_id"]
        bt = desc_c[desc_c["from_stop_id"].fillna("").str.endswith("_BT")]
        if len(bt) == 1:
            return bt.iloc[0]["pathway_id"]

    # ── F4: seq narrows pool → description or BT tiebreak (≈ Strategy D) ─────
    if not seq_c.empty and wmata_key:
        desc_on_seq = _desc_filter_extended(seq_c, wmata_key)
        if len(desc_on_seq) == 1:
            return desc_on_seq.iloc[0]["pathway_id"]
        if not desc_on_seq.empty:
            bt = desc_on_seq[desc_on_seq["from_stop_id"].fillna("").str.endswith("_BT")]
            if len(bt) == 1:
                return bt.iloc[0]["pathway_id"]
        if len(seq_c) > 1:
            bt   = seq_c[seq_c["from_stop_id"].fillna("").str.endswith("_BT")]
            pool = bt if not bt.empty else seq_c
            if pool["from_seq"].notna().any():
                pool = pool.sort_values("from_seq")
            log.warning(
                "pathway_joiner: F4 blind tiebreak for %s — %d seq candidates, "
                "no desc match; picking %s. Add to _STATIC_LOOKUP if wrong.",
                unit_name, len(seq_c), pool.iloc[0]["pathway_id"],
            )
            return pool.iloc[0]["pathway_id"]

    # ── F5: synonym-expanded description → seq or BT tiebreak ────────────────
    ext_key = _desc_segment_key_extended(location_desc)
    if ext_key and ext_key != wmata_key:
        ext_c = _desc_filter_extended(zone_c if zone_exists else station_c, ext_key)
        if len(ext_c) == 1:
            return ext_c.iloc[0]["pathway_id"]
        if not ext_c.empty:
            if seq:
                seq_on_ext = ext_c[(ext_c["from_seq"] == seq) | (ext_c["to_seq"] == seq)]
                if len(seq_on_ext) == 1:
                    return seq_on_ext.iloc[0]["pathway_id"]
            bt = ext_c[ext_c["from_stop_id"].fillna("").str.endswith("_BT")]
            if len(bt) == 1:
                return bt.iloc[0]["pathway_id"]

    # ── F6: singleton at station ──────────────────────────────────────────────
    if len(station_c) == 1:
        return station_c.iloc[0]["pathway_id"]

    # ── F7: final tiebreaker — description-filtered pool, lowest-seq BT ──────
    # Only fires when seq produced no candidates AND a description key exists
    # to narrow the pool. Without wmata_key the pool would be the full
    # station_c (potentially dozens of pathways) with no evidence for any pick.
    if seq_c.empty and wmata_key:
        pool = _desc_filter_extended(station_c, wmata_key)
        if not pool.empty:
            bt_pool = pool[pool["from_stop_id"].fillna("").str.endswith("_BT")]
            if not bt_pool.empty:
                pool = bt_pool
            if pool["from_seq"].notna().any():
                pool = pool.sort_values("from_seq")
            return pool.iloc[0]["pathway_id"]

    return None


# ── Tier 2: Static lookup ─────────────────────────────────────────────────────


def _tier2_match(outage: pd.Series) -> str | None:
    """Look up complex-station units in the static mapping table."""
    unit_name = outage.get("unit_name", "")
    unit_type = str(outage.get("unit_type", "")).upper()
    return _STATIC_LOOKUP.get((unit_name, unit_type))


# ── Main entry point ──────────────────────────────────────────────────────────


def resolve(outages: pd.DataFrame, neo4j: Neo4jManager) -> pd.DataFrame:
    """
    Resolve OutageEvent rows to Pathway nodes.

    Args:
        outages:  DataFrame produced by transform.run() — must contain
                  composite_key, unit_name, station_code, unit_type,
                  location_description columns.
        neo4j:    Live Neo4jManager for the graph query.

    Returns:
        DataFrame[composite_key, pathway_id] containing only rows where
        a match was found. Unmatched outages are logged as warnings.
    """
    if outages.empty:
        return pd.DataFrame(columns=["composite_key", "pathway_id"])

    # Fetch and enrich candidates once for the whole poll.
    candidates = _fetch_pathway_candidates(neo4j)
    stop_desc  = _load_stop_descriptions()
    enriched   = _enrich_candidates(candidates, stop_desc)

    results: list[dict] = []
    unmatched: list[str] = []
    static_hits = 0
    tier1_hits = 0

    for _, outage in outages.iterrows():
        composite_key = outage["composite_key"]
        unit_name     = outage.get("unit_name", "")

        # Static lookup checked first for all outages — covers complex interchange
        # stations and any spatially-ambiguous units with hardcoded entries.
        pathway_id = _tier2_match(outage)
        if pathway_id:
            results.append({"composite_key": composite_key, "pathway_id": pathway_id})
            static_hits += 1
            continue

        # Tier 1: cascaded programmatic join (Strategy F)
        pathway_id = _tier1_match(outage, enriched)
        if pathway_id:
            results.append({"composite_key": composite_key, "pathway_id": pathway_id})
            tier1_hits += 1
        else:
            unmatched.append(unit_name)

    total   = len(outages)
    matched = len(results)
    log.info(
        "pathway_joiner: matched %d / %d outages "
        "(tier1=%d static=%d unmatched=%d)",
        matched, total, tier1_hits, static_hits, len(unmatched),
    )

    if unmatched:
        log.warning(
            "pathway_joiner: %d unmatched unit(s) — no AFFECTS relationship created: %s",
            len(unmatched),
            ", ".join(sorted(set(unmatched))[:20]),
        )

    if not results:
        return pd.DataFrame(columns=["composite_key", "pathway_id"])

    return pd.DataFrame(results)
