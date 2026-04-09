// queries/fare/analytical.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Analytical queries for the Fare layer
// Queries marked [FARE ONLY] run with just fare nodes loaded.
// Queries marked [+ PHYSICAL] require Station/FareGate nodes from Physical.
// ═══════════════════════════════════════════════════════════════════════════
// ── Q1: Fare overview — node counts [FARE ONLY] ─────────────────────────
MATCH (fz:FareZone)
WITH count(fz) AS zones
MATCH (fp:FareProduct)
WITH zones, count(fp) AS products
MATCH (fm:FareMedia)
WITH zones, products, count(fm) AS media
MATCH (flr:FareLegRule)
WITH zones, products, media, count(flr) AS leg_rules
MATCH (ftr:FareTransferRule)
WITH zones, products, media, leg_rules, count(ftr) AS transfer_rules
RETURN zones, products, media, leg_rules, transfer_rules;

// ── Q2: Fare products and accepted media [FARE ONLY] ────────────────────
MATCH (fp:FareProduct)<-[:ACCEPTS]-(fm:FareMedia)
RETURN
  fp.fare_product_id AS product,
  fp.fare_product_name AS name,
  collect(fm.fare_media_name) AS accepted_via
ORDER BY product;

// ── Q3: Fare range by timeframe — min/max/avg rail fare [FARE ONLY] ─────
MATCH (flr:FareLegRule)-[ap:APPLIES_PRODUCT]->(fp:FareProduct)
WHERE flr.network_id = 'metrorail'
RETURN
  ap.timeframe AS timeframe,
  min(ap.amount) AS min_fare,
  max(ap.amount) AS max_fare,
  round(avg(ap.amount), 2) AS avg_fare,
  count(ap) AS od_pairs
ORDER BY timeframe;

// ── Q4: Peak vs off-peak price difference for same OD pair [FARE ONLY] ──
// Shows how much more you pay during weekday_regular vs weekend.
MATCH
  (flr:FareLegRule)-[peak:APPLIES_PRODUCT {timeframe: 'weekday_regular'}]->
  (fp1:FareProduct)
MATCH (flr)-[offpeak:APPLIES_PRODUCT {timeframe: 'weekend'}]->(fp2:FareProduct)
WHERE flr.network_id = 'metrorail'
RETURN
  flr.leg_group_id AS leg_rule,
  peak.amount AS peak_fare,
  offpeak.amount AS weekend_fare,
  round(peak.amount - offpeak.amount, 2) AS premium
ORDER BY premium DESC
LIMIT 10;

// ── Q5: Transfer rules — discount structure [FARE ONLY] ─────────────────
MATCH (ftr:FareTransferRule)-[:FROM_LEG]->(from_leg:FareLegRule)
MATCH (ftr)-[:TO_LEG]->(to_leg:FareLegRule)
OPTIONAL MATCH (ftr)-[:APPLIES_PRODUCT]->(fp:FareProduct)
RETURN
  from_leg.leg_group_id AS from_leg,
  to_leg.leg_group_id AS to_leg,
  ftr.transfer_count AS transfers,
  ftr.duration_limit AS duration_s,
  CASE ftr.fare_transfer_type
    WHEN 0 THEN 'free'
    WHEN 1 THEN 'add product cost'
    ELSE toString(ftr.fare_transfer_type)
  END AS transfer_type,
  fp.fare_product_name AS discount_product
ORDER BY from_leg, to_leg;

// ── Q6: Zones with most OD connections [FARE ONLY] ──────────────────────
MATCH (flr:FareLegRule)-[:FROM_AREA]->(fz:FareZone)
RETURN fz.zone_id AS zone, count(flr) AS outbound_rules
ORDER BY outbound_rules DESC
LIMIT 10;

// ── Q7: Farragut free transfer validation [+ PHYSICAL] ──────────────────
// Farragut North (STN_A02) and Farragut West (STN_C03) share a FareZone.
// The free transfer should emerge as a $0 FareLegRule via shared zone.
MATCH (s1:Station {id: 'STN_A02'})-[:IN_ZONE]->(fz:FareZone)
MATCH (s2:Station {id: 'STN_C03'})-[:IN_ZONE]->(fz)
MATCH (flr:FareLegRule)-[:FROM_AREA]->(fz)
MATCH (flr)-[:TO_AREA]->(fz)
MATCH (flr)-[ap:APPLIES_PRODUCT]->(fp:FareProduct)
WHERE ap.amount = 0.0
RETURN
  s1.name AS from_station,
  s2.name AS to_station,
  fz.zone_id AS shared_zone,
  fp.fare_product_name AS product,
  ap.amount AS fare,
  ap.timeframe AS timeframe;

// ── Q8: Fare lookup — price between two stations [+ PHYSICAL] ───────────
// Parameterised: replace $from and $to with station ids.
// Example: STN_A01_C01 (Metro Center) → STN_A02 (Farragut North)
MATCH (s1:Station {id: 'STN_A01_C01'})-[:IN_ZONE]->(fz1:FareZone)
MATCH (s2:Station {id: 'STN_A02'})-[:IN_ZONE]->(fz2:FareZone)
MATCH (flr:FareLegRule)-[:FROM_AREA]->(fz1)
MATCH (flr)-[:TO_AREA]->(fz2)
MATCH (flr)-[ap:APPLIES_PRODUCT]->(fp:FareProduct)
RETURN
  s1.name AS from_station,
  s2.name AS to_station,
  fp.fare_product_name AS product,
  ap.timeframe AS timeframe,
  ap.amount AS fare,
  ap.currency AS currency
ORDER BY ap.timeframe;

// ── Q9: Stations per fare zone [+ PHYSICAL] ─────────────────────────────
MATCH (s:Station)-[:IN_ZONE]->(fz:FareZone)
RETURN
  fz.zone_id AS zone,
  count(s) AS station_count,
  collect(s.name) AS stations
ORDER BY toInteger(fz.zone_id);

// ── Q10: FareGates per station [+ PHYSICAL] ─────────────────────────────
MATCH (fg:FareGate)-[:BELONGS_TO]->(s:Station)
MATCH (fg)-[:IN_ZONE]->(fz:FareZone)
RETURN s.name AS station, fz.zone_id AS zone, count(fg) AS gate_count
ORDER BY gate_count DESC
LIMIT 15;
