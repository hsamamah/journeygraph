// queries/cross_layer/analytical.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Cross-layer analytical queries
// These traverse multiple domain layers and require Physical layer nodes
// (Station, Platform, BusStop, Pathway, PathwayNode) to be loaded.
//
// Dependencies noted per query:
//   [SERVICE + PHYSICAL]  — schedule traversals through physical infrastructure
//   [FARE + PHYSICAL]     — fare resolution through zone anchoring
//   [ALL THREE]           — full-stack queries touching service, fare, physical
// ═══════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════
// SERVICE + PHYSICAL
// ═══════════════════════════════════════════════════════════════════════════
// ── Q1: Trip frequency per station (weekday) [SERVICE + PHYSICAL] ────────
// How many weekday trips serve each station? Identifies busiest hubs.
MATCH
  (sp:ServicePattern:Weekday)<-[:OPERATED_ON]-
  (t:Trip)-[:SCHEDULED_AT]->
  (p:Platform)-[:BELONGS_TO]->
  (s:Station)
RETURN
  s.stop_name AS station,
  s.stop_id AS station_id,
  count(DISTINCT t) AS weekday_trips
ORDER BY weekday_trips DESC
LIMIT 15;

// ── Q2: First and last train at a station [SERVICE + PHYSICAL] ───────────
// Replace station_id to query any station.
MATCH
  (sp:ServicePattern:Weekday)<-[:OPERATED_ON]-
  (t:Trip)-[sa:SCHEDULED_AT]->
  (p:Platform)-[:BELONGS_TO]->
  (s:Station {id: 'STN_A01_C01'})
WHERE sa.arrival_time IS NOT NULL
RETURN
  s.stop_name AS station,
  min(sa.arrival_time) AS first_arrival_sec,
  max(sa.arrival_time) AS last_arrival_sec,
  // Convert seconds to HH:MM for readability
  toString(min(sa.arrival_time) / 3600) +
  ':' +
  CASE
    WHEN (min(sa.arrival_time) % 3600) / 60 < 10 THEN '0'
    ELSE ''
  END +
  toString((min(sa.arrival_time) % 3600) / 60) AS first_train,
  toString(max(sa.arrival_time) / 3600) +
  ':' +
  CASE
    WHEN (max(sa.arrival_time) % 3600) / 60 < 10 THEN '0'
    ELSE ''
  END +
  toString((max(sa.arrival_time) % 3600) / 60) AS last_train;

// ── Q3: Routes serving a station [SERVICE + PHYSICAL] ────────────────────
MATCH (r:Route)-[:SERVES]->(s:Station {id: 'STN_A01_C01'})
RETURN
  s.stop_name AS station,
  r.mode AS mode,
  r.route_short_name AS route,
  r.route_long_name AS route_name
ORDER BY r.mode, r.route_short_name;

// ── Q4: Transfer opportunities — stations with multiple rail lines ───────
// [SERVICE + PHYSICAL]
// Finds stations served by more than one rail route — transfer hubs.
MATCH (r:Route:Rail)-[:SERVES]->(s:Station)
WITH
  s,
  collect(DISTINCT r.route_short_name) AS lines,
  count(DISTINCT r) AS line_count
WHERE line_count > 1
RETURN s.stop_name AS station, s.stop_id AS station_id, line_count, lines
ORDER BY line_count DESC;

// ── Q5: Bus routes within one stop of a rail station [SERVICE + PHYSICAL]
// Which bus routes have a stop that is also served by rail?
// Identifies bus-rail transfer points.
MATCH (r_rail:Route:Rail)-[:SERVES]->(s:Station)
MATCH (r_bus:Route:Bus)-[:SERVES]->(bs:BusStop)
WHERE
  point.distance(
    point({latitude: s.stop_lat, longitude: s.stop_lon}),
    point({latitude: bs.stop_lat, longitude: bs.stop_lon})
  ) <
  400
RETURN
  s.stop_name AS station,
  r_rail.route_short_name AS rail_line,
  r_bus.route_short_name AS bus_route,
  round(
    point.distance(
      point({latitude: s.stop_lat, longitude: s.stop_lon}),
      point({latitude: bs.stop_lat, longitude: bs.stop_lon})
    )) AS distance_m,
  bs.stop_name AS bus_stop
ORDER BY station, distance_m
LIMIT 25;

// ── Q6: Downstream stops from a station — delay propagation surface ──────
// [SERVICE + PHYSICAL]
// For a given station, find all stations downstream on the same route
// patterns. These are the stations affected if service is disrupted here.
MATCH
  (s:Station {id: 'STN_A01_C01'})<-[:BELONGS_TO]-
  (p:Platform)<-[sa1:STOPS_AT]-
  (rp:RoutePattern)
MATCH (rp)-[sa2:STOPS_AT]->(p2:Platform)-[:BELONGS_TO]->(s2:Station)
WHERE sa2.stop_sequence > sa1.stop_sequence
RETURN
  s.stop_name AS origin,
  rp.shape_id AS pattern,
  rp.headsign AS direction,
  s2.stop_name AS downstream_station,
  sa2.stop_sequence - sa1.stop_sequence AS stops_away
ORDER BY pattern, stops_away;

// ── Q7: Saturday vs weekday trip count per station [SERVICE + PHYSICAL] ──
// Measures service reduction on weekends.
MATCH
  (sp_wk:ServicePattern:Weekday)<-[:OPERATED_ON]-
  (t_wk:Trip)-[:SCHEDULED_AT]->
  (p:Platform)-[:BELONGS_TO]->
  (s:Station)
WITH s, count(DISTINCT t_wk) AS weekday_trips
MATCH
  (sp_sat:ServicePattern:Saturday)<-[:OPERATED_ON]-
  (t_sat:Trip)-[:SCHEDULED_AT]->
  (p2:Platform)-[:BELONGS_TO]->
  (s)
RETURN
  s.stop_name AS station,
  weekday_trips,
  count(DISTINCT t_sat) AS saturday_trips,
  round(100.0 * count(DISTINCT t_sat) / weekday_trips, 1) AS saturday_pct
ORDER BY saturday_pct ASC
LIMIT 15;

// ── Q8: Platform utilisation — trips per platform [SERVICE + PHYSICAL] ───
// Which platforms handle the most traffic?
MATCH (t:Trip)-[:SCHEDULED_AT]->(p:Platform)-[:BELONGS_TO]->(s:Station)
RETURN
  s.stop_name AS station,
  p.stop_id AS platform,
  p.stop_desc AS description,
  count(t) AS trip_count
ORDER BY trip_count DESC
LIMIT 20;

// ═══════════════════════════════════════════════════════════════════════════
// FARE + PHYSICAL
// ═══════════════════════════════════════════════════════════════════════════

// ── Q9: Most expensive rail journey [FARE + PHYSICAL] ────────────────────
MATCH
  (flr:FareLegRule)-[ap:APPLIES_PRODUCT {timeframe: 'weekday_regular'}]->
  (fp:FareProduct)
WHERE flr.network_id = 'metrorail' AND ap.amount > 0
MATCH (flr)-[:FROM_AREA]->(fz1:FareZone)<-[:IN_ZONE]-(s1:Station)
MATCH (flr)-[:TO_AREA]->(fz2:FareZone)<-[:IN_ZONE]-(s2:Station)
RETURN
  s1.stop_name AS from_station,
  s2.stop_name AS to_station,
  ap.amount AS peak_fare
ORDER BY peak_fare DESC
LIMIT 10;

// ── Q10: Zone boundary stations [FARE + PHYSICAL] ───────────────────────
// Stations at the edge of a fare zone — riders crossing here pay more.
MATCH (s:Station)-[:IN_ZONE]->(fz:FareZone)
MATCH (r:Route:Rail)-[:SERVES]->(s)
MATCH (r)-[:SERVES]->(s2:Station)-[:IN_ZONE]->(fz2:FareZone)
WHERE fz.zone_id <> fz2.zone_id
RETURN DISTINCT
  s.stop_name AS station,
  fz.zone_id AS zone,
  s2.stop_name AS neighbor,
  fz2.zone_id AS neighbor_zone,
  r.route_short_name AS line
ORDER BY zone, station;

// ═══════════════════════════════════════════════════════════════════════════
// ALL THREE LAYERS
// ═══════════════════════════════════════════════════════════════════════════

// ── Q11: Full journey cost — fare for a scheduled trip [ALL THREE] ───────
// "What does it cost to ride the Red Line from Metro Center to Farragut
// North during weekday peak?"
// Traverses: Trip → SCHEDULED_AT → Platform → Station → IN_ZONE → FareZone
//            → FROM_AREA ← FareLegRule → TO_AREA → FareZone → IN_ZONE ← Station
MATCH (s1:Station {id: 'STN_A01_C01'})-[:IN_ZONE]->(fz1:FareZone)
MATCH (s2:Station {id: 'STN_A02'})-[:IN_ZONE]->(fz2:FareZone)
MATCH (flr:FareLegRule)-[:FROM_AREA]->(fz1)
MATCH (flr)-[:TO_AREA]->(fz2)
MATCH (flr)-[ap:APPLIES_PRODUCT]->(fp:FareProduct)
// Also verify trips actually connect these stations
MATCH (t:Trip)-[sa1:SCHEDULED_AT]->(p1:Platform)-[:BELONGS_TO]->(s1)
MATCH (t)-[sa2:SCHEDULED_AT]->(p2:Platform)-[:BELONGS_TO]->(s2)
WHERE sa2.stop_sequence > sa1.stop_sequence
RETURN
  s1.stop_name AS from_station,
  s2.stop_name AS to_station,
  ap.timeframe AS timeframe,
  ap.amount AS fare,
  count(DISTINCT t) AS trips_available
ORDER BY timeframe;

// ── Q12: Cheapest route between two stations across all timeframes ───────
// [ALL THREE]
MATCH (s1:Station {id: 'STN_A01_C01'})-[:IN_ZONE]->(fz1:FareZone)
MATCH (s2:Station {id: 'STN_A02'})-[:IN_ZONE]->(fz2:FareZone)
MATCH (flr:FareLegRule)-[:FROM_AREA]->(fz1)
MATCH (flr)-[:TO_AREA]->(fz2)
MATCH (flr)-[ap:APPLIES_PRODUCT]->(fp:FareProduct)
RETURN
  s1.stop_name AS from_station,
  s2.stop_name AS to_station,
  ap.timeframe AS timeframe,
  ap.amount AS fare
ORDER BY fare ASC;

// ── Q13: Station accessibility + schedule — trips through elevator ───────
// stations [ALL THREE minus Accessibility events]
// Finds stations that have elevators AND heavy trip traffic — these are
// the stations where an elevator outage has the highest impact.
// Requires: Physical (Station, PathwayNode:Elevator) + Service (Trip, SCHEDULED_AT)
MATCH (e:Elevator)-[:BELONGS_TO]->(s:Station)
WITH s, count(DISTINCT e) AS elevator_count
MATCH (t:Trip)-[:SCHEDULED_AT]->(p:Platform)-[:BELONGS_TO]->(s)
RETURN
  s.stop_name AS station,
  s.stop_id AS station_id,
  elevator_count,
  count(DISTINCT t) AS total_trips,
  // Impact score: more trips × fewer elevators = higher risk
  round(1.0 * count(DISTINCT t) / elevator_count) AS impact_score
ORDER BY impact_score DESC
LIMIT 15;

// ── Q14: Bus-to-rail transfer cost — full multimodal journey ─────────────
// [ALL THREE]
// "I take the 70 bus to a station then ride rail. What's my total fare
// with the transfer discount?"
MATCH
  (flr_bus:FareLegRule {leg_group_id: 'leg_metrobus_regular'})-
    [ap_bus:APPLIES_PRODUCT]->
  (fp_bus:FareProduct)
MATCH (ftr:FareTransferRule)-[:FROM_LEG]->(flr_bus)
MATCH (ftr)-[:TO_LEG]->(flr_rail:FareLegRule {network_id: 'metrorail'})
OPTIONAL MATCH (ftr)-[:APPLIES_PRODUCT]->(fp_disc:FareProduct)
// Pick a specific rail OD pair
MATCH
  (flr_rail)-[ap_rail:APPLIES_PRODUCT {timeframe: 'weekday_regular'}]->
  (fp_rail:FareProduct)
MATCH (flr_rail)-[:FROM_AREA]->(fz1:FareZone)<-[:IN_ZONE]-(s1:Station)
MATCH (flr_rail)-[:TO_AREA]->(fz2:FareZone)<-[:IN_ZONE]-(s2:Station)
WHERE s1.stop_id = 'STN_A01_C01' AND s2.stop_id = 'STN_A02'
RETURN
  'Bus Regular' AS leg_1,
  ap_bus.amount AS bus_fare,
  s1.stop_name + ' → ' + s2.stop_name AS leg_2,
  ap_rail.amount AS rail_fare,
  fp_disc.fare_product_name AS discount,
  ftr.duration_limit / 60 + ' min window' AS transfer_window,
  CASE ftr.fare_transfer_type
    WHEN 0 THEN 'Free transfer'
    WHEN 1 THEN 'Discount applied'
  END AS transfer_type;
