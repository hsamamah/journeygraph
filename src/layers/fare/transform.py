# src/layers/fare/transform.py
"""
Fare layer — Transform

Converts raw GTFS DataFrames into clean, Neo4j-ready DataFrames.
Runs pre-load validation at the end; raises ValueError if checks fail.

Produces a FareTransformResult with one DataFrame per node/relationship type.

Key design decisions applied here:
  - FareProduct: deduplicated to 5 logical nodes. Amount encoded on the
    APPLIES_PRODUCT relationship, not on the product node itself.
  - FareLegRule: from_area_id / to_area_id are station stop_ids in GTFS.
    Transformed to zone_ids so the graph anchors to FareZone nodes.
  - FareGate: owned by the physical layer. The fare layer derives the
    station_zones and gate_zones DataFrames needed to wire IN_ZONE
    relationships after physical layer has committed FareGate nodes.
  - Rail amount: encoded in fare_product_id suffix
    (e.g. metrorail_one_way_full_fare_225 → $2.25).
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re

import pandas as pd

from src.common.logger import get_logger
from src.common.utils import clean_str

log = get_logger(__name__)

# ── Logical FareProduct catalogue ─────────────────────────────────────────────
# Maps fare_product_id prefix patterns → stable logical product id used in graph
PRODUCT_MAP: dict[str, tuple[str, str]] = {
    # pattern                          (logical_id,               display_name)
    "metrobus_one_way_regular_fare": ("bus_regular", "Metrobus Regular"),
    "metrobus_one_way_express_fare": ("bus_express", "Metrobus Express"),
    "metrobus_transfer_discount": (
        "bus_transfer_discount",
        "Metrobus Transfer Discount",
    ),
    "metrorail_free_fare": ("rail_free", "Metrorail Free"),
    "metrorail_one_way_full_fare": ("rail_one_way", "Metrorail One-Way"),
}

# WMATA-specific: only these network_ids trigger zone-anchored fare rules.
# A new rail network ID would need to be added here.
# See CONVENTIONS.md → "Rail Network IDs"
RAIL_NETWORKS = {"metrorail", "metrorail_shuttle"}


# ── Result container ──────────────────────────────────────────────────────────


@dataclass
class FareTransformResult:
    """Clean DataFrames ready for Neo4j ingestion."""

    # Nodes
    fare_zones: pd.DataFrame  # zone_id (unique, sourced from stops)
    fare_media: pd.DataFrame  # fare_media_id, fare_media_name, fare_media_type
    fare_products: pd.DataFrame  # fare_product_id (logical), fare_product_name
    fare_leg_rules: pd.DataFrame  # leg_group_id, network_id

    # Relationship data (carried to load.py as DataFrame rows)
    leg_rule_applies_product: pd.DataFrame  # leg_group_id, fare_product_id, timeframe,
    # amount, currency
    leg_rule_from_area: pd.DataFrame  # leg_group_id, zone_id  [rail only]
    leg_rule_to_area: pd.DataFrame  # leg_group_id, zone_id  [rail only]
    fare_transfer_rules: pd.DataFrame  # full transfer rule rows
    station_zones: pd.DataFrame  # stop_id (STN_), zone_id
    gate_zones: pd.DataFrame  # stop_id (_FG_), zone_id, parent_station
    product_media_map: pd.DataFrame  # fare_product_id (logical), fare_media_id

    # Metadata
    feed_info: pd.DataFrame  # single row from feed_info.txt
    stats: dict[str, int] = field(default_factory=dict)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _logical_product(fare_product_id: str) -> tuple[str, str] | None:
    """Return (logical_id, display_name) for a raw fare_product_id, or None."""
    pid = clean_str(fare_product_id)
    for prefix, mapping in PRODUCT_MAP.items():
        if pid.startswith(prefix):
            return mapping
    return None


def _parse_amount(
    fare_product_id: str, product_amount_map: dict[str, float] | None = None
) -> float:
    """
    Extract amount from fare_product_id.

    Strategy:
      1. Try numeric suffix (rail): metrorail_one_way_full_fare_225 → 2.25
      2. Try product_amount_map (built from fare_products.txt amount column)
      3. Warn and return 0.0 if neither works
    """
    pid = clean_str(fare_product_id)
    match = re.search(r"_(\d+)$", pid)
    if match:
        return int(match.group(1)) / 100

    # Fallback: lookup from fare_products.txt (bus fares have no suffix)
    if product_amount_map and pid in product_amount_map:
        return product_amount_map[pid]

    log.warning(
        "fare transform: could not determine amount for fare_product_id '%s' "
        "— no numeric suffix and not found in fare_products.txt. Defaulting to 0.0",
        pid,
    )
    return 0.0


def _build_product_amount_map(fare_products_raw: pd.DataFrame) -> dict[str, float]:
    """
    Build a lookup of fare_product_id → amount from fare_products.txt.
    Used as fallback for products whose ID doesn't encode the amount.
    """
    if "amount" not in fare_products_raw.columns:
        return {}
    df = fare_products_raw[["fare_product_id", "amount"]].copy()
    df["fare_product_id"] = df["fare_product_id"].astype(str).str.strip()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["fare_product_id"].notna() & (df["fare_product_id"] != "") & (df["fare_product_id"] != "nan")]
    df = df.dropna(subset=["amount"])
    return df.set_index("fare_product_id")["amount"].to_dict()


# ── Transform functions ───────────────────────────────────────────────────────


def _transform_fare_zones(
    stops: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      fare_zones    — unique zone_id values (one row per zone)
      station_zones — STN_ stops with zone_id (for Station -[:IN_ZONE]-> FareZone)
      gate_zones    — _FG_ stops with zone_id (for FareGate -[:IN_ZONE]-> FareZone)
    """
    zoned = stops[
        stops["zone_id"].notna() & (stops["zone_id"].astype(str) != "")
    ].copy()
    zoned["zone_id"] = zoned["zone_id"].astype(str).str.strip()

    fare_zones = (
        pd.DataFrame({"zone_id": zoned["zone_id"].unique()})
        .sort_values("zone_id")
        .reset_index(drop=True)
    )

    # WMATA-specific: station stop_ids use STN_ prefix, faregate stop_ids
    # contain _FG_ as substring (e.g. NODE_A01_FG_PAID).
    # See CONVENTIONS.md → "Stop ID Prefix Conventions"
    station_zones = zoned[zoned["stop_id"].str.startswith("STN_", na=False)][
        ["stop_id", "zone_id"]
    ].reset_index(drop=True)

    gate_zones = zoned[zoned["stop_id"].str.contains("_FG_", na=False)][
        ["stop_id", "zone_id", "parent_station"]
    ].reset_index(drop=True)

    return fare_zones, station_zones, gate_zones


def _transform_fare_media(fare_media_raw: pd.DataFrame) -> pd.DataFrame:
    return fare_media_raw[
        ["fare_media_id", "fare_media_name", "fare_media_type"]
    ].copy()


def _transform_fare_products(
    fare_products_raw: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      fare_products     — 5 logical nodes (deduplicated)
      product_media_map — fare_product_id (logical), fare_media_id
                          (for ACCEPTED_VIA relationship)
    """
    seen: dict[str, str] = {}  # logical_id → display_name
    media_rows: list[dict] = []

    for _, row in fare_products_raw.iterrows():
        mapping = _logical_product(str(row.get("fare_product_id", "")))
        if not mapping:
            log.warning(
                "fare transform: unmapped fare_product_id '%s'",
                row.get("fare_product_id"),
            )
            continue
        logical_id, display_name = mapping
        seen[logical_id] = display_name

        media_rows.append(
            {
                "fare_product_id": logical_id,
                "fare_media_id": clean_str(str(row.get("fare_media_id", ""))),
            }
        )

    fare_products = pd.DataFrame(
        [{"fare_product_id": k, "fare_product_name": v} for k, v in seen.items()]
    )
    product_media_map = (
        pd.DataFrame(media_rows)
        .drop_duplicates(subset=["fare_product_id", "fare_media_id"])
        .reset_index(drop=True)
    )
    return fare_products, product_media_map


def _transform_fare_leg_rules(
    fare_leg_rules_raw: pd.DataFrame,
    stop_zone_map: dict[str, str],
    product_amount_map: dict[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    leg_rules_seen: dict[
        str, dict
    ] = {}  # rule_id → {rule_id, leg_group_id, network_id}
    applies_rows: list[dict] = []
    from_rows: list[dict] = []
    to_rows: list[dict] = []
    unresolvable_area_ids: set[str] = set()

    # Vectorised string normalisation — replaces per-row clean_str calls
    def _strip_col(col: str, src: str | None = None) -> pd.Series:
        raw = fare_leg_rules_raw.get(src or col, pd.Series(dtype=str))
        cleaned = raw.astype(str).str.strip()
        return cleaned.where(~cleaned.isin(["", "nan", "None"]), other=None)

    df = fare_leg_rules_raw.copy()
    df["_leg_group_id"]    = _strip_col("leg_group_id")
    df["_network_id"]      = _strip_col("network_id")
    df["_fare_product_id"] = _strip_col("fare_product_id")
    df["_timeframe"]       = _strip_col("from_timeframe_group_id")
    df["_from_area_id"]    = _strip_col("from_area_id").fillna("")
    df["_to_area_id"]      = _strip_col("to_area_id").fillna("")

    df = df[df["_leg_group_id"].notna()].copy()

    # Composite primary key — OD pair identity
    df["_rule_id"] = (
        df["_leg_group_id"] + "__"
        + df["_from_area_id"].where(df["_from_area_id"] != "", "NULL") + "__"
        + df["_to_area_id"].where(df["_to_area_id"] != "", "NULL")
    )

    # leg_rules — first occurrence per rule_id preserves network_id semantics
    leg_rules_df = (
        df.drop_duplicates(subset=["_rule_id"], keep="first")
        [["_rule_id", "_leg_group_id", "_network_id"]]
        .rename(columns={"_rule_id": "rule_id", "_leg_group_id": "leg_group_id", "_network_id": "network_id"})
        .reset_index(drop=True)
    )
    for _, r in leg_rules_df.iterrows():
        leg_rules_seen[r["rule_id"]] = r.to_dict()

    # APPLIES_PRODUCT — map logical product per unique fare_product_id, then join
    unique_pids = df["_fare_product_id"].dropna().unique()
    pid_mapping = {pid: _logical_product(pid) for pid in unique_pids}
    df["_mapping"] = df["_fare_product_id"].map(pid_mapping)
    applies_df = df[df["_mapping"].notna()].copy()
    if not applies_df.empty:
        applies_df["_logical_id"] = applies_df["_mapping"].map(lambda m: m[0])
        applies_df["_amount"] = applies_df["_fare_product_id"].map(
            lambda pid: _parse_amount(pid, product_amount_map)
        )
        applies_rows = applies_df[["_rule_id", "_logical_id", "_timeframe", "_amount"]].rename(
            columns={"_rule_id": "rule_id", "_logical_id": "fare_product_id", "_timeframe": "timeframe", "_amount": "amount"}
        ).assign(
            timeframe=lambda d: d["timeframe"].fillna("NULL"),
            currency="USD",
        ).to_dict(orient="records")

    # FROM_AREA / TO_AREA — rail only, one row per rule_id
    rail_df = df[
        df["_network_id"].isin(RAIL_NETWORKS)
        & (df["_from_area_id"] != "")
        & (df["_from_area_id"] != "nan")
    ].copy()

    if not rail_df.empty:
        rail_df["_from_zone"] = rail_df["_from_area_id"].map(stop_zone_map)
        rail_df["_to_zone"]   = rail_df["_to_area_id"].map(stop_zone_map)

        unresolvable_area_ids = (
            set(rail_df.loc[rail_df["_from_zone"].isna(), "_from_area_id"])
            | set(rail_df.loc[
                (rail_df["_to_area_id"] != "") & rail_df["_to_zone"].isna(),
                "_to_area_id",
            ])
        )

        from_rows = (
            rail_df[rail_df["_from_zone"].notna()]
            .drop_duplicates(subset=["_rule_id"], keep="first")
            [["_rule_id", "_from_zone"]]
            .rename(columns={"_rule_id": "rule_id", "_from_zone": "zone_id"})
            .to_dict(orient="records")
        )
        to_rows = (
            rail_df[rail_df["_to_zone"].notna()]
            .drop_duplicates(subset=["_rule_id"], keep="first")
            [["_rule_id", "_to_zone"]]
            .rename(columns={"_rule_id": "rule_id", "_to_zone": "zone_id"})
            .to_dict(orient="records")
        )

    if unresolvable_area_ids:
        raise ValueError(
            f"fare transform validation failed — {len(unresolvable_area_ids)} "
            f"area_id(s) in fare_leg_rules could not be resolved to a zone: "
            f"{sorted(unresolvable_area_ids)}"
        )

    leg_rules = pd.DataFrame(list(leg_rules_seen.values()))
    applies_product = (
        pd.DataFrame(applies_rows).drop_duplicates().reset_index(drop=True)
    )
    from_area = pd.DataFrame(from_rows).reset_index(drop=True)
    to_area = pd.DataFrame(to_rows).reset_index(drop=True)

    return leg_rules, applies_product, from_area, to_area


def _transform_fare_transfer_rules(
    fare_transfer_rules_raw: pd.DataFrame | None,
) -> pd.DataFrame:
    if fare_transfer_rules_raw is None or fare_transfer_rules_raw.empty:
        log.warning("fare transform: no fare_transfer_rules data — skipping")
        return pd.DataFrame()

    cols = [
        "from_leg_group_id",
        "to_leg_group_id",
        "transfer_count",
        "duration_limit",
        "duration_limit_type",
        "fare_transfer_type",
        "fare_product_id",
    ]
    present = [c for c in cols if c in fare_transfer_rules_raw.columns]
    df = fare_transfer_rules_raw[present].copy()

    # Resolve fare_product_id to logical id (nullable — free transfers have null)
    def _remap(pid: str | None) -> str | None:
        if not pid or pd.isna(pid):
            return None
        mapping = _logical_product(str(pid))
        return mapping[0] if mapping else None

    if "fare_product_id" in df.columns:
        df["fare_product_id"] = df["fare_product_id"].apply(_remap)

    return df.reset_index(drop=True)


# ── Main entry point ──────────────────────────────────────────────────────────


def run(raw: dict[str, pd.DataFrame]) -> FareTransformResult:
    """
    Transform raw GTFS DataFrames into a FareTransformResult.
    Runs pre-load validation; raises ValueError on failure.
    """
    log.info("fare transform: starting")

    stops = raw["stops"]
    fare_media_raw = raw["fare_media"]
    fare_products_raw = raw["fare_products"]
    fare_leg_raw = raw["fare_leg_rules"]
    transfer_raw = raw.get("fare_transfer_rules")
    feed_info_raw = raw["feed_info"]

    # Build stop → zone lookup used by leg rule transform
    zoned = stops[stops["zone_id"].notna() & (stops["zone_id"].astype(str) != "")]
    stop_zone_map: dict[str, str] = (
        zoned.set_index("stop_id")["zone_id"].astype(str).to_dict()
    )

    fare_zones, station_zones, gate_zones = _transform_fare_zones(stops)
    fare_media = _transform_fare_media(fare_media_raw)
    fare_products, product_media_map = _transform_fare_products(fare_products_raw)
    product_amount_map = _build_product_amount_map(fare_products_raw)
    leg_rules, applies_product, from_area, to_area = _transform_fare_leg_rules(
        fare_leg_raw, stop_zone_map, product_amount_map
    )
    transfer_rules = _transform_fare_transfer_rules(transfer_raw)

    stats = {
        "fare_zones": len(fare_zones),
        "fare_media": len(fare_media),
        "fare_products": len(fare_products),
        "fare_leg_rules": len(leg_rules),
        "applies_product": len(applies_product),
        "from_area": len(from_area),
        "to_area": len(to_area),
        "transfer_rules": len(transfer_rules),
        "station_zones": len(station_zones),
        "gate_zones": len(gate_zones),
    }
    for k, v in stats.items():
        log.info("fare transform: %-25s %6d rows", k, v)

    log.info("fare transform: complete")
    return FareTransformResult(
        fare_zones=fare_zones,
        fare_media=fare_media,
        fare_products=fare_products,
        fare_leg_rules=leg_rules,
        leg_rule_applies_product=applies_product,
        leg_rule_from_area=from_area,
        leg_rule_to_area=to_area,
        fare_transfer_rules=transfer_rules,
        station_zones=station_zones,
        gate_zones=gate_zones,
        product_media_map=product_media_map,
        feed_info=feed_info_raw,
        stats=stats,
    )
