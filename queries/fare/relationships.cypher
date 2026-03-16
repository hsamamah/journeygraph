// queries/fare/relationships.cypher
// Parameterised MERGE statements for all fare layer relationships.
// Physical layer (Station, FareGate nodes) must be loaded before this runs.

// ── Station -[:IN_ZONE]-> FareZone ─────────────────────────────────────────
// Bridges physical layer to fare layer.
// $rows: [{stop_id, zone_id}]
UNWIND $rows AS row
MATCH (s:Station  {stop_id:  row.stop_id})
MATCH (fz:FareZone {zone_id: row.zone_id})
MERGE (s)-[:IN_ZONE]->(fz);

// ── FareGate -[:IN_ZONE]-> FareZone ────────────────────────────────────────
// Bridges physical layer to fare layer.
// FareGate nodes are owned by the physical layer — must exist before this runs.
// $rows: [{stop_id, zone_id}]
UNWIND $rows AS row
MATCH (fg:FareGate  {gate_id:  row.stop_id})
MATCH (fz:FareZone  {zone_id:  row.zone_id})
MERGE (fg)-[:IN_ZONE]->(fz);

// ── FareGate -[:BELONGS_TO]-> Station ──────────────────────────────────────
// Owned by fare layer: wires the payment point back to its parent station.
// $rows: [{stop_id, parent_station}]
UNWIND $rows AS row
MATCH (fg:FareGate {gate_id:  row.stop_id})
MATCH (s:Station   {stop_id:  row.parent_station})
MERGE (fg)-[:BELONGS_TO]->(s);

// ── FareMedia -[:ACCEPTS]-> FareProduct ────────────────────────────────────
// $rows: [{fare_media_id, fare_product_id}]
UNWIND $rows AS row
MATCH (fm:FareMedia  {fare_media_id:  row.fare_media_id})
MATCH (fp:FareProduct {fare_product_id: row.fare_product_id})
MERGE (fm)-[:ACCEPTS]->(fp);

// ── FareProduct -[:ACCEPTED_VIA]-> FareMedia ───────────────────────────────
// Bidirectional for traversal efficiency.
// $rows: [{fare_product_id, fare_media_id}]
UNWIND $rows AS row
MATCH (fp:FareProduct {fare_product_id: row.fare_product_id})
MATCH (fm:FareMedia   {fare_media_id:   row.fare_media_id})
MERGE (fp)-[:ACCEPTED_VIA]->(fm);

// ── FareLegRule -[:FROM_AREA]-> FareZone ───────────────────────────────────
// Rail only. Bus leg rules have no area anchor.
UNWIND $rows AS row
MATCH (flr:FareLegRule {rule_id: row.rule_id})
MATCH (fz:FareZone {zone_id: row.zone_id})
MERGE (flr)-[:FROM_AREA]->(fz)

// ── FareLegRule -[:TO_AREA]-> FareZone ─────────────────────────────────────
// Rail only.
// $rows: [{rule_id, zone_id}]
UNWIND $rows AS row
MATCH (flr:FareLegRule {rule_id:  row.rule_id})
MATCH (fz:FareZone     {zone_id:  row.zone_id})
MERGE (flr)-[:TO_AREA]->(fz);

// ── FareLegRule -[:APPLIES_PRODUCT]-> FareProduct
UNWIND $rows AS row
MATCH (flr:FareLegRule {rule_id: row.rule_id})
MATCH (fp:FareProduct {fare_product_id: row.fare_product_id})
MERGE (flr)-[:APPLIES_PRODUCT {
    timeframe: row.timeframe,
    amount:    row.amount,
    currency:  row.currency
}]->(fp)

// ── FareTransferRule -[:FROM_LEG]-> FareLegRule  (matches all OD pairs in group)
UNWIND $rows AS row
MATCH (ftr:FareTransferRule {rule_id: row.rule_id})
MATCH (flr:FareLegRule {leg_group_id: row.from_leg_group_id})
MERGE (ftr)-[:FROM_LEG]->(flr)

// ── FareTransferRule -[:TO_LEG]-> FareLegRule ──────────────────────────────
// $rows: [{rule_id, to_leg_group_id}]
UNWIND $rows AS row
MATCH (ftr:FareTransferRule {rule_id:      row.rule_id})
MATCH (flr:FareLegRule      {leg_group_id: row.to_leg_group_id})
MERGE (ftr)-[:TO_LEG]->(flr);

// ── FareTransferRule -[:APPLIES_PRODUCT]-> FareProduct ─────────────────────
// Null on free transfer rows — only wired when fare_product_id is present.
// $rows: [{rule_id, fare_product_id}]
UNWIND $rows AS row
MATCH (ftr:FareTransferRule {rule_id:       row.rule_id})
MATCH (fp:FareProduct       {fare_product_id: row.fare_product_id})
MERGE (ftr)-[:APPLIES_PRODUCT]->(fp);
