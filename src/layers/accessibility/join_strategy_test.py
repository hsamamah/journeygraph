# src/layers/accessibility/join_strategy_test.py
"""
Join strategy comparison harness for OutageEvent → Pathway matching.

Run via:
    uv run python -m src.layers.accessibility.join_strategy_test

Four strategies are evaluated against the live outage + candidates data:

  A  Current baseline — station + zone_letter + mode + BT/TP suffix
  B  Sequence-number — station + mode + conditional-zone + ESC/ELE{n} in stop_id
  C  Description-segment — station + mode + WMATA description ↔ GTFS stop_desc
  D  Combined — B then C, with lowest-seq tiebreaker on residual ambiguity

Results are printed as a summary table.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, NamedTuple

import pandas as pd

from src.common.logger import get_logger

# Strategy A replicates the old zone+BT/TP baseline — these constants are local
# to avoid importing removed symbols from pathway_joiner.
_SEGMENT_KEYWORDS: dict[str, str] = {
    "street": "_BT",
    "mezzanine": "_BT",
    "platform": "_TP",
    "concourse": "_TP",
}

log = get_logger(__name__)

_GTFS_STOPS = Path(__file__).parents[3] / "data" / "gtfs" / "stops.txt"

# ── Outcome sentinel ──────────────────────────────────────────────────────────

class MatchOutcome(NamedTuple):
    pathway_id: str | None
    status: Literal["matched", "unmatched", "ambiguous"]


# ── Result container ──────────────────────────────────────────────────────────

@dataclass
class StrategyResult:
    matched: int = 0
    unmatched: int = 0
    ambiguous: int = 0
    total: int = 0
    examples: list[dict] = field(default_factory=list)
    ambiguous_examples: list[dict] = field(default_factory=list)
    unmatched_examples: list[dict] = field(default_factory=list)

    @property
    def match_rate(self) -> float:
        return self.matched / self.total if self.total else 0.0


# ── GTFS stop description loader ──────────────────────────────────────────────

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


# ── Candidates enrichment ─────────────────────────────────────────────────────

_SEQ_RE = re.compile(r"(?:ESC|ELE|ELV)(\d+)", re.IGNORECASE)


def _extract_seq(stop_id: str) -> int | None:
    m = _SEQ_RE.search(stop_id or "")
    return int(m.group(1)) if m else None


def _enrich_candidates(
    candidates: pd.DataFrame,
    stop_desc: dict[str, str],
) -> pd.DataFrame:
    """Add from_desc, to_desc, from_seq, to_seq columns."""
    df = candidates.copy()
    df["from_desc"] = df["from_stop_id"].map(stop_desc).fillna("")
    df["to_desc"]   = df["to_stop_id"].map(stop_desc).fillna("")
    df["from_seq"]  = df["from_stop_id"].apply(_extract_seq)
    df["to_seq"]    = df["to_stop_id"].apply(_extract_seq)
    return df


# ── Description segment normalisation ────────────────────────────────────────

_BETWEEN_RE = re.compile(r"between\s+(.+?)(?:\s+to\s+|\s*,|\s*$)", re.IGNORECASE)


def _desc_segment_key(text: str) -> str | None:
    """
    Normalise a 'between X and Y' description to a canonical, order-independent key.

    "Bottom of Escalator Between Street and  Mezzanine" → "mezzanine_street"
    "Escalator between street and mezzanine"            → "mezzanine_street"
    "Intermediate Passage and Mezzanine"                → "intermediate_passage_mezzanine"

    Nouns are sorted alphabetically so "A and B" == "B and A".
    """
    text_norm = re.sub(r"\s+", " ", text.strip().lower())
    m = _BETWEEN_RE.search(text_norm)
    if not m:
        return None
    phrase = m.group(1).strip()
    # Split on " and " to get the two sides
    parts = re.split(r"\s+and\s+", phrase, maxsplit=1)
    # Normalise each part: collapse spaces → underscores
    norm_parts = [re.sub(r"\s+", "_", p.strip()) for p in parts]
    # Sort for order-independence: "street_mezzanine" == "mezzanine_street"
    return "_".join(sorted(norm_parts))


# ── Shared helpers ────────────────────────────────────────────────────────────

def _mode_mask(candidates: pd.DataFrame, unit_type: str) -> pd.Series:
    unit_type = unit_type.upper()
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


# ── Strategy A — current baseline ─────────────────────────────────────────────


def _strategy_a(outage: pd.Series, candidates: pd.DataFrame) -> MatchOutcome:
    """
    Port of _tier1_match from pathway_joiner.py (no zone fallback, exact replication).
    """
    station_code = outage.get("station_code", "")
    unit_name    = outage.get("unit_name", "")
    unit_type    = str(outage.get("unit_type", "")).upper()
    location_desc = str(outage.get("location_description", "")).lower()

    if not station_code or len(unit_name) < 4:
        return MatchOutcome(None, "unmatched")

    zone_letter = unit_name[3].upper()

    mode_c = candidates[_mode_mask(candidates, unit_type)]
    if mode_c.empty:
        return MatchOutcome(None, "unmatched")

    station_c = mode_c[_station_mask(mode_c, station_code)]
    if station_c.empty:
        return MatchOutcome(None, "unmatched")

    zone_c = station_c[_zone_mask(station_c, zone_letter)]
    if zone_c.empty:
        return MatchOutcome(None, "unmatched")

    position_suffix = None
    for keyword, suffix in _SEGMENT_KEYWORDS.items():
        if keyword in location_desc:
            position_suffix = suffix
            break

    if position_suffix:
        pos_mask = (
            zone_c["from_stop_id"].fillna("").str.endswith(position_suffix)
            | zone_c["to_stop_id"].fillna("").str.endswith(position_suffix)
        )
        final_c = zone_c[pos_mask]
    else:
        final_c = zone_c

    if len(final_c) == 1:
        return MatchOutcome(final_c.iloc[0]["pathway_id"], "matched")
    if len(final_c) > 1:
        return MatchOutcome(None, "ambiguous")
    return MatchOutcome(None, "unmatched")


# ── Strategy B — sequence-number join ─────────────────────────────────────────


def _strategy_b(outage: pd.Series, candidates: pd.DataFrame) -> MatchOutcome:
    """
    Uses the 2-digit sequence suffix of unit_name (e.g. "A08N03" → 3) to
    match ESC{n}/ELE{n} in stop_ids.  Zone letter applied only when it actually
    exists as a zone for that station.
    """
    station_code  = outage.get("station_code", "")
    unit_name     = outage.get("unit_name", "")
    unit_type     = str(outage.get("unit_type", "")).upper()

    if not station_code or len(unit_name) < 6:
        return MatchOutcome(None, "unmatched")

    zone_letter = unit_name[3].upper()
    seq_str     = unit_name[4:6].lstrip("0") or "0"
    try:
        seq = int(seq_str)
    except ValueError:
        return MatchOutcome(None, "unmatched")

    mode_c = candidates[_mode_mask(candidates, unit_type)]
    if mode_c.empty:
        return MatchOutcome(None, "unmatched")

    station_c = mode_c[_station_mask(mode_c, station_code)]
    if station_c.empty:
        return MatchOutcome(None, "unmatched")

    # Apply zone filter only if the zone letter is present in this station's candidates
    zone_exists = _zone_mask(station_c, zone_letter).any()
    if zone_exists:
        station_c = station_c[_zone_mask(station_c, zone_letter)]
        if station_c.empty:
            return MatchOutcome(None, "unmatched")

    # Sequence filter: ESC{seq} or ELE{seq} in stop_id
    seq_mask = (
        (station_c["from_seq"] == seq) | (station_c["to_seq"] == seq)
    )
    seq_c = station_c[seq_mask]
    if seq_c.empty:
        return MatchOutcome(None, "unmatched")
    if len(seq_c) == 1:
        return MatchOutcome(seq_c.iloc[0]["pathway_id"], "matched")
    return MatchOutcome(None, "ambiguous")


# ── Strategy C — description-segment join ────────────────────────────────────


def _strategy_c(outage: pd.Series, candidates: pd.DataFrame) -> MatchOutcome:
    """
    Matches the 'between X and Y' segment phrase from WMATA location_description
    against the GTFS stop_desc of each pathway's endpoints.  Zone letter ignored.
    Prefers the pathway whose _BT endpoint matches.
    """
    station_code  = outage.get("station_code", "")
    unit_type     = str(outage.get("unit_type", "")).upper()
    location_desc = str(outage.get("location_description", ""))

    wmata_key = _desc_segment_key(location_desc)
    if not wmata_key or not station_code:
        return MatchOutcome(None, "unmatched")

    mode_c = candidates[_mode_mask(candidates, unit_type)]
    if mode_c.empty:
        return MatchOutcome(None, "unmatched")

    station_c = mode_c[_station_mask(mode_c, station_code)]
    if station_c.empty:
        return MatchOutcome(None, "unmatched")

    # Match description segment key against stop descriptions of either endpoint
    from_keys = station_c["from_desc"].apply(_desc_segment_key)
    to_keys   = station_c["to_desc"].apply(_desc_segment_key)
    desc_mask = (from_keys == wmata_key) | (to_keys == wmata_key)
    desc_c    = station_c[desc_mask]

    if desc_c.empty:
        return MatchOutcome(None, "unmatched")
    if len(desc_c) == 1:
        return MatchOutcome(desc_c.iloc[0]["pathway_id"], "matched")

    # Prefer the row where from_stop_id is the BT (canonical bottom) endpoint
    bt_mask  = desc_c["from_stop_id"].fillna("").str.endswith("_BT")
    bt_c     = desc_c[bt_mask]
    if len(bt_c) == 1:
        return MatchOutcome(bt_c.iloc[0]["pathway_id"], "matched")
    if bt_c.empty:
        return MatchOutcome(None, "ambiguous")
    return MatchOutcome(None, "ambiguous")


# ── Strategy D — B then C, lowest-seq tiebreaker ─────────────────────────────


def _strategy_d(outage: pd.Series, candidates: pd.DataFrame) -> MatchOutcome:
    """
    Pipeline: apply B's station+mode+zone-conditional+seq filters, then apply
    C's description segment filter on any residual ambiguity.  If still
    ambiguous, pick the pathway with the lowest seq number.
    """
    station_code  = outage.get("station_code", "")
    unit_name     = outage.get("unit_name", "")
    unit_type     = str(outage.get("unit_type", "")).upper()
    location_desc = str(outage.get("location_description", ""))

    if not station_code or len(unit_name) < 6:
        return MatchOutcome(None, "unmatched")

    zone_letter = unit_name[3].upper()
    seq_str     = unit_name[4:6].lstrip("0") or "0"
    try:
        seq = int(seq_str)
    except ValueError:
        return MatchOutcome(None, "unmatched")

    mode_c = candidates[_mode_mask(candidates, unit_type)]
    if mode_c.empty:
        return MatchOutcome(None, "unmatched")

    station_c = mode_c[_station_mask(mode_c, station_code)]
    if station_c.empty:
        return MatchOutcome(None, "unmatched")

    zone_exists = _zone_mask(station_c, zone_letter).any()
    if zone_exists:
        station_c = station_c[_zone_mask(station_c, zone_letter)]
        if station_c.empty:
            return MatchOutcome(None, "unmatched")

    seq_mask = (station_c["from_seq"] == seq) | (station_c["to_seq"] == seq)
    seq_c    = station_c[seq_mask]

    if len(seq_c) == 1:
        return MatchOutcome(seq_c.iloc[0]["pathway_id"], "matched")

    # Fall through to description filter on seq_c (or full station_c if seq empty)
    pool = seq_c if not seq_c.empty else station_c

    wmata_key = _desc_segment_key(location_desc)
    if wmata_key:
        from_keys = pool["from_desc"].apply(_desc_segment_key)
        to_keys   = pool["to_desc"].apply(_desc_segment_key)
        desc_mask = (from_keys == wmata_key) | (to_keys == wmata_key)
        pool      = pool[desc_mask]

    if pool.empty:
        return MatchOutcome(None, "unmatched")
    if len(pool) == 1:
        return MatchOutcome(pool.iloc[0]["pathway_id"], "matched")

    # Tiebreaker: lowest seq number, prefer _BT endpoint
    bt_pool = pool[pool["from_stop_id"].fillna("").str.endswith("_BT")]
    if not bt_pool.empty:
        pool = bt_pool

    # Pick lowest from_seq, fallback to first row
    if pool["from_seq"].notna().any():
        pool = pool.sort_values("from_seq")

    return MatchOutcome(pool.iloc[0]["pathway_id"], "matched")


# ── Strategy F — cascaded pipeline ───────────────────────────────────────────
#
# Runs sub-strategies in order, returning the first unique match:
#
#   F1  C alone (description-segment, no seq)         — catches unique-desc units
#   F2  B alone (seq-number, zone-conditional)         — catches clean seq maps
#   F3  C-first then B tiebreak                        — fixes C04X03-class errors
#   F4  B-first then C tiebreak           (= Strategy D) — already best combined
#   F5  Synonym-expanded C                             — "middle landing", "amtrak station"
#   F6  Singleton fallback                             — only 1 unit of type at station
#
# Term synonyms: WMATA uses informal names that differ from GTFS vocabulary.

_WMATA_SYNONYMS: dict[str, str] = {
    "middle landing":           "intermediate passage",
    "middle  landing":          "intermediate passage",
    "amtrak station":           "street",
    "main entrance":            "street",
    "platform level":           "platform",
    "mezzanine level":          "mezzanine",
    "main concourse":           "mezzanine",
    "intermediate level":       "intermediate passage",
    # GTFS line-qualified platform names → generic "platform"
    "silver line platform":     "platform",
    "blue/orange lines platform": "platform",
    "red line platform":        "platform",
    "green line platform":      "platform",
    "yellow line platform":     "platform",
}


def _apply_synonyms(text: str) -> str:
    """Replace WMATA-specific terms with their GTFS equivalents."""
    t = text.lower()
    for wmata_term, gtfs_term in _WMATA_SYNONYMS.items():
        t = t.replace(wmata_term, gtfs_term)
    # Strip trailing " level" from GTFS descriptions to normalise
    t = re.sub(r"\s+level\b", "", t)
    return t


def _desc_segment_key_extended(text: str) -> str | None:
    """Like _desc_segment_key but with synonym expansion applied first."""
    return _desc_segment_key(_apply_synonyms(text))


def _strategy_f(outage: pd.Series, candidates: pd.DataFrame) -> MatchOutcome:
    """
    Cascaded pipeline: try each sub-strategy in order, return first unique match.
    """
    station_code  = outage.get("station_code", "")
    unit_type     = str(outage.get("unit_type", "")).upper()
    location_desc = str(outage.get("location_description", ""))

    mode_c = candidates[_mode_mask(candidates, unit_type)]
    if mode_c.empty:
        return MatchOutcome(None, "unmatched")

    station_c = mode_c[_station_mask(mode_c, station_code)]
    if station_c.empty:
        return MatchOutcome(None, "unmatched")

    # ── F1: description alone (with GTFS-side synonym expansion) ────────────
    wmata_key = _desc_segment_key(location_desc)
    desc_c = _desc_filter_extended(station_c, wmata_key) if wmata_key else pd.DataFrame()
    if len(desc_c) == 1:
        return MatchOutcome(desc_c.iloc[0]["pathway_id"], "matched")

    # ── F2: seq alone ─────────────────────────────────────────────────────────
    seq = _extract_seq_from_unit(outage.get("unit_name", ""))
    zone_letter = outage.get("unit_name", "    ")[3].upper()
    zone_exists = _zone_mask(station_c, zone_letter).any()
    zone_c = station_c[_zone_mask(station_c, zone_letter)] if zone_exists else station_c
    seq_c = zone_c[(zone_c["from_seq"] == seq) | (zone_c["to_seq"] == seq)] if seq else pd.DataFrame()
    if len(seq_c) == 1:
        return MatchOutcome(seq_c.iloc[0]["pathway_id"], "matched")

    # ── F3: description narrows pool, BT endpoint tiebreaks ──────────────────
    if wmata_key and not desc_c.empty:
        # Seq tiebreak within desc matches
        if seq:
            seq_on_desc = desc_c[(desc_c["from_seq"] == seq) | (desc_c["to_seq"] == seq)]
            if len(seq_on_desc) == 1:
                return MatchOutcome(seq_on_desc.iloc[0]["pathway_id"], "matched")
        # BT tiebreak: prefer the pathway whose from_stop_id is the bottom node
        bt = desc_c[desc_c["from_stop_id"].fillna("").str.endswith("_BT")]
        if len(bt) == 1:
            return MatchOutcome(bt.iloc[0]["pathway_id"], "matched")

    # ── F4: seq narrows pool, description tiebreaks (= Strategy D core) ───────
    if not seq_c.empty and wmata_key:
        desc_on_seq = _desc_filter_extended(seq_c, wmata_key)
        if len(desc_on_seq) == 1:
            return MatchOutcome(desc_on_seq.iloc[0]["pathway_id"], "matched")
        if not desc_on_seq.empty:
            bt = desc_on_seq[desc_on_seq["from_stop_id"].fillna("").str.endswith("_BT")]
            if len(bt) == 1:
                return MatchOutcome(bt.iloc[0]["pathway_id"], "matched")
        # Seq pool ambiguous — use lowest seq BT as tiebreak (Strategy D tiebreaker)
        if len(seq_c) > 1:
            bt = seq_c[seq_c["from_stop_id"].fillna("").str.endswith("_BT")]
            pool = bt if not bt.empty else seq_c
            if pool["from_seq"].notna().any():
                pool = pool.sort_values("from_seq")
            return MatchOutcome(pool.iloc[0]["pathway_id"], "matched")

    # ── F5: synonym-expanded description ─────────────────────────────────────
    ext_key = _desc_segment_key_extended(location_desc)
    if ext_key and ext_key != wmata_key:
        ext_desc_c = _desc_filter_extended(zone_c if zone_exists else station_c, ext_key)
        if len(ext_desc_c) == 1:
            return MatchOutcome(ext_desc_c.iloc[0]["pathway_id"], "matched")
        if not ext_desc_c.empty:
            # Seq tiebreak
            if seq:
                ext_seq = ext_desc_c[(ext_desc_c["from_seq"] == seq) | (ext_desc_c["to_seq"] == seq)]
                if len(ext_seq) == 1:
                    return MatchOutcome(ext_seq.iloc[0]["pathway_id"], "matched")
            # BT tiebreak
            bt = ext_desc_c[ext_desc_c["from_stop_id"].fillna("").str.endswith("_BT")]
            if len(bt) == 1:
                return MatchOutcome(bt.iloc[0]["pathway_id"], "matched")

    # ── F6: singleton — only one unit of this type at station ────────────────
    if len(station_c) == 1:
        return MatchOutcome(station_c.iloc[0]["pathway_id"], "matched")

    # ── F7: final tiebreaker — description-filtered pool, lowest-seq BT ──────
    # Only fires when seq produced no candidates AND wmata_key exists to narrow
    # the pool. Without wmata_key the pool would be unnarrowed station_c.
    if seq_c.empty and wmata_key:
        pool = _desc_filter_extended(station_c, wmata_key)
        if not pool.empty:
            bt_pool = pool[pool["from_stop_id"].fillna("").str.endswith("_BT")]
            if not bt_pool.empty:
                pool = bt_pool
            if pool["from_seq"].notna().any():
                pool = pool.sort_values("from_seq")
            return MatchOutcome(pool.iloc[0]["pathway_id"], "matched")

    return MatchOutcome(None, "unmatched")


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
    """Extract the 2-digit numeric suffix from a WMATA unit_name.

    Returns None for sequence 00 — treated as absent, since GTFS stop_ids
    do not use ELE0/ESC0 suffixes and seq 0 would produce false matches.
    """
    if len(unit_name) < 6:
        return None
    try:
        seq = int(unit_name[4:6].lstrip("0") or "0")
        return seq if seq != 0 else None
    except ValueError:
        return None


# ── Strategy E — legacy full-text index approach (historical baseline) ────────
#
# Self-contained implementation of the old full-text index tier-1.
# Kept for comparison against the cascade (Strategy F).
# Requires a live Neo4j connection; _candidates is ignored.

_FT_BETWEEN_RE = re.compile(r"between\s+(.+?)$", re.IGNORECASE)


def _build_ft_query(location_desc: str) -> str | None:
    """Build Lucene AND query from 'between X and Y' phrase."""
    desc_norm = re.sub(r"\s+", " ", location_desc.strip().lower())
    m = _FT_BETWEEN_RE.search(desc_norm)
    if not m:
        return None
    phrase = m.group(1)
    nouns = [w for w in re.split(r"\s+and\s+|\s+", phrase) if w]
    if not nouns:
        return None
    return " AND ".join(f'"{w}"' for w in nouns)


def _strategy_e(outage: pd.Series, _candidates: pd.DataFrame, neo4j) -> MatchOutcome:
    """Full-text index join (old tier-1 approach). Requires a live Neo4j connection."""
    station_code  = outage.get("station_code", "")
    unit_type     = str(outage.get("unit_type", "")).upper()
    location_desc = str(outage.get("location_description", ""))
    mode          = 4 if unit_type == "ESCALATOR" else 5
    station_token = f"NODE_{station_code}_"

    ft_query = _build_ft_query(location_desc)
    if not ft_query:
        return MatchOutcome(None, "unmatched")

    rows = neo4j.query(
        """
        CALL db.index.fulltext.queryNodes('physical_pathway_stop_desc', $ft_query)
        YIELD node, score
        WHERE node.mode = $mode
          AND (node.from_stop_id CONTAINS $station_token
               OR  node.to_stop_id  CONTAINS $station_token)
        RETURN node.id AS pathway_id, score
        ORDER BY score DESC LIMIT 5
        """,
        {"ft_query": ft_query, "mode": mode, "station_token": station_token},
    )
    if not rows:
        return MatchOutcome(None, "unmatched")
    if len(rows) == 1:
        return MatchOutcome(rows[0]["pathway_id"], "matched")
    top, second = rows[0]["score"], rows[1]["score"]
    if second == 0 or top / second >= 1.5:
        return MatchOutcome(rows[0]["pathway_id"], "matched")
    return MatchOutcome(None, "ambiguous")


# ── Orchestrator ──────────────────────────────────────────────────────────────

_STRATEGIES: dict[str, callable] = {
    "A": _strategy_a,
    "B": _strategy_b,
    "C": _strategy_c,
    "D": _strategy_d,
    "E": _strategy_e,  # requires neo4j kwarg
    "F": _strategy_f,
}

_STRATEGY_LABELS = {
    "A": "Baseline (zone+BT/TP)",
    "B": "Seq-number",
    "C": "Description-segment",
    "D": "Combined (B→C→tiebreak)",
    "E": "Full-text index (tier1 prod)",
    "F": "Cascade (C→B→synonyms→singleton)",
}


def run_join_strategy_comparison(
    outages: pd.DataFrame,
    candidates: pd.DataFrame,
    gtfs_stops_path: str | Path = _GTFS_STOPS,
    neo4j=None,
) -> dict[str, StrategyResult]:
    """
    Run all join strategies against live outages and candidates DataFrames.

    Args:
        outages:         Output of transform.run().
        candidates:      Output of _fetch_pathway_candidates().
        gtfs_stops_path: Path to GTFS stops.txt for stop descriptions.
        neo4j:           Live Neo4jManager — required for strategy E.
                         If None, strategy E is skipped.

    Returns:
        dict keyed by strategy letter, each a StrategyResult.
    """
    stop_desc  = _load_stop_descriptions(Path(gtfs_stops_path))
    enriched   = _enrich_candidates(candidates, stop_desc)
    active     = {k: v for k, v in _STRATEGIES.items() if k != "E" or neo4j is not None}
    results    = {k: StrategyResult(total=len(outages)) for k in active}

    for _, outage in outages.iterrows():
        unit_name = outage.get("unit_name", "")
        station   = outage.get("station_code", "")
        for key, fn in active.items():
            if key == "E":
                outcome = fn(outage, enriched, neo4j)
            else:
                outcome = fn(outage, enriched)
            r = results[key]
            if outcome.status == "matched":
                r.matched += 1
                if len(r.examples) < 5:
                    pid = outcome.pathway_id
                    row = enriched[enriched["pathway_id"] == pid]
                    r.examples.append({
                        "unit_name":    unit_name,
                        "station_code": station,
                        "pathway_id":   pid,
                        "from_stop_id": row.iloc[0]["from_stop_id"] if not row.empty else "",
                        "to_stop_id":   row.iloc[0]["to_stop_id"] if not row.empty else "",
                    })
            elif outcome.status == "ambiguous":
                r.ambiguous += 1
                if len(r.ambiguous_examples) < 3:
                    r.ambiguous_examples.append({
                        "unit_name": unit_name,
                        "station_code": station,
                    })
            else:
                r.unmatched += 1
                r.unmatched_examples.append({
                    "unit_name": unit_name,
                    "station_code": station,
                    "location_description": outage.get("location_description", ""),
                })

    _print_summary(results, outages)
    return results


def _print_summary(results: dict[str, StrategyResult], outages: pd.DataFrame) -> None:
    print(f"\n{'─'*66}")
    print(f"  Join strategy comparison — {len(outages)} outages")
    print(f"{'─'*66}")
    print(f"  {'Strategy':<30}  {'Matched':>7}  {'Ambig':>7}  {'Unmatch':>7}  {'Rate':>6}")
    print(f"  {'─'*28}  {'─'*7}  {'─'*7}  {'─'*7}  {'─'*6}")
    for key, r in results.items():
        label = f"{key}: {_STRATEGY_LABELS[key]}"
        print(
            f"  {label:<30}  {r.matched:>7}  {r.ambiguous:>7}  {r.unmatched:>7}"
            f"  {r.match_rate:>5.0%}"
        )
    print(f"{'─'*66}\n")

    best = max(results.items(), key=lambda kv: kv[1].matched)
    print(f"  Best strategy: {best[0]} ({_STRATEGY_LABELS[best[0]]})")
    print(f"  Matched {best[1].matched}/{best[1].total} ({best[1].match_rate:.0%})\n")

    for key, r in results.items():
        if r.examples:
            print(f"  Strategy {key} examples:")
            for ex in r.examples[:3]:
                print(f"    {ex['unit_name']:10} → {ex['pathway_id']}")
                print(f"              from: {ex['from_stop_id']}")
                print(f"                to: {ex['to_stop_id']}")
            if r.ambiguous_examples:
                print(f"  Strategy {key} ambiguous:")
                for ex in r.ambiguous_examples:
                    print(f"    {ex['unit_name']:10} @ {ex['station_code']}")
            if key == "F" and r.unmatched_examples:
                print(f"  Strategy F unmatched:")
                for ex in r.unmatched_examples:
                    print(f"    {ex['unit_name']:10} @ {ex['station_code']}  \"{ex['location_description']}\"")
            print()


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    from src.common.config import get_config
    from src.common.neo4j_tools import Neo4jManager
    from src.ingest.api_client import WMATAClient
    from src.layers.accessibility import extract, transform
    from src.layers.accessibility.pathway_joiner import _fetch_pathway_candidates

    cfg    = get_config()
    neo4j  = Neo4jManager(cfg.neo4j_uri, cfg.neo4j_user, cfg.neo4j_password)
    client = WMATAClient(cfg.wmata_api_key)

    raw        = extract.run(client)
    result     = transform.run(raw)
    candidates = _fetch_pathway_candidates(neo4j)

    run_join_strategy_comparison(result.outages, candidates, neo4j=neo4j)
    neo4j.close()
