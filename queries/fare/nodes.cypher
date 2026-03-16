// queries/fare/nodes.cypher
// Parameterised MERGE statements for all fare layer nodes.
// Called by fare/load.py via load_query().
// Each statement expects a list of dicts passed as $rows.

// ── :FareZone ──────────────────────────────────────────────────────────────
// $rows: [{zone_id}]
UNWIND $rows AS row
MERGE (fz:FareZone {zone_id: row.zone_id});

// ── :FareMedia ─────────────────────────────────────────────────────────────
// $rows: [{fare_media_id, fare_media_name, fare_media_type}]
UNWIND $rows AS row
MERGE (fm:FareMedia {fare_media_id: row.fare_media_id})
SET   fm.fare_media_name = row.fare_media_name,
      fm.fare_media_type = row.fare_media_type;

// ── :FareProduct ───────────────────────────────────────────────────────────
// $rows: [{fare_product_id, fare_product_name}]
// Amount is NOT stored here — it lives on the APPLIES_PRODUCT relationship.
UNWIND $rows AS row
MERGE (fp:FareProduct {fare_product_id: row.fare_product_id})
SET   fp.fare_product_name = row.fare_product_name;

// ── :FareLegRule ───────────────────────────────────────────────────────────
UNWIND $rows AS row
MERGE (flr:FareLegRule {rule_id: row.rule_id})
SET flr.leg_group_id = row.leg_group_id,
    flr.network_id   = row.network_id

// ── :FareTransferRule ──────────────────────────────────────────────────────
// $rows: [{rule_id, from_leg_group_id, to_leg_group_id, transfer_count,
//          duration_limit, duration_limit_type, fare_transfer_type}]
// rule_id is a synthetic key: from_leg_group_id + '__' + to_leg_group_id
UNWIND $rows AS row
MERGE (ftr:FareTransferRule {rule_id: row.rule_id})
SET   ftr.from_leg_group_id   = row.from_leg_group_id,
      ftr.to_leg_group_id     = row.to_leg_group_id,
      ftr.transfer_count      = row.transfer_count,
      ftr.duration_limit      = row.duration_limit,
      ftr.duration_limit_type = row.duration_limit_type,
      ftr.fare_transfer_type  = row.fare_transfer_type;
