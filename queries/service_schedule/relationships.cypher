// queries/service_schedule/relationships.cypher
// Parameterised MERGE statements for all service layer relationships.
// Physical layer (Station, Platform, BusStop) must be loaded before
// cross-layer relationships (SERVES, STOPS_AT, SCHEDULED_AT) run.
// ═══════════════════════════════════════════════════════════════════════════
// INTERNAL RELATIONSHIPS (service layer nodes only)
// ═══════════════════════════════════════════════════════════════════════════
// ── Agency -[:OPERATES]-> Route ──────────────────────────────────────────
// $rows: [{agency_id, route_id}]
UNWIND $rows AS row
MATCH (a:Agency {agency_id: row.agency_id})
MATCH (r:Route {route_id: row.route_id})
MERGE (a)-[:OPERATES]->(r);

// ── Route -[:OPERATED_BY]-> Agency ───────────────────────────────────────
// Bidirectional for traversal efficiency.
// $rows: [{route_id, agency_id}]
UNWIND $rows AS row
MATCH (r:Route {route_id: row.route_id})
MATCH (a:Agency {agency_id: row.agency_id})
MERGE (r)-[:OPERATED_BY]->(a);

// ── Route -[:HAS_PATTERN]-> RoutePattern ─────────────────────────────────
// $rows: [{route_id, shape_id}]
UNWIND $rows AS row
MATCH (r:Route {route_id: row.route_id})
MATCH (rp:RoutePattern {shape_id: row.shape_id})
MERGE (r)-[:HAS_PATTERN]->(rp);

// ── RoutePattern -[:BELONGS_TO]-> Route ──────────────────────────────────
// $rows: [{shape_id, route_id}]
UNWIND $rows AS row
MATCH (rp:RoutePattern {shape_id: row.shape_id})
MATCH (r:Route {route_id: row.route_id})
MERGE (rp)-[:BELONGS_TO]->(r);

// ── RoutePattern -[:HAS_TRIP]-> Trip ─────────────────────────────────────
// $rows: [{shape_id, trip_id}]
UNWIND $rows AS row
MATCH (rp:RoutePattern {shape_id: row.shape_id})
MATCH (t:Trip {trip_id: row.trip_id})
MERGE (rp)-[:HAS_TRIP]->(t);

// ── Trip -[:FOLLOWS]-> RoutePattern ──────────────────────────────────────
// $rows: [{trip_id, shape_id}]
UNWIND $rows AS row
MATCH (t:Trip {trip_id: row.trip_id})
MATCH (rp:RoutePattern {shape_id: row.shape_id})
MERGE (t)-[:FOLLOWS]->(rp);

// ── Trip -[:OPERATED_ON]-> ServicePattern ────────────────────────────────
// $rows: [{trip_id, service_id}]
UNWIND $rows AS row
MATCH (t:Trip {trip_id: row.trip_id})
MATCH (sp:ServicePattern {service_id: row.service_id})
MERGE (t)-[:OPERATED_ON]->(sp);

// ── ServicePattern -[:ACTIVE_ON]-> Date ──────────────────────────────────
// holiday_name is nullable — only set when the date is a known holiday.
// $rows: [{service_id, date, holiday_name}]
UNWIND $rows AS row
MATCH (sp:ServicePattern {service_id: row.service_id})
MATCH (d:Date {date: row.date})
MERGE (sp)-[r:ACTIVE_ON]->(d)
SET r.holiday_name = row.holiday_name;

// ═══════════════════════════════════════════════════════════════════════════
// FROM_FEED PROVENANCE
// ═══════════════════════════════════════════════════════════════════════════

// ── Agency -[:FROM_FEED]-> FeedInfo ──────────────────────────────────────
// $rows: [{feed_version}]  (single row — applied to all Agency nodes)
UNWIND $rows AS row
MATCH (a:Agency)
MATCH (fi:FeedInfo {feed_version: row.feed_version})
MERGE (a)-[:FROM_FEED]->(fi);

// ── Route -[:FROM_FEED]-> FeedInfo ───────────────────────────────────────
UNWIND $rows AS row
MATCH (r:Route)
MATCH (fi:FeedInfo {feed_version: row.feed_version})
MERGE (r)-[:FROM_FEED]->(fi);

// ── RoutePattern -[:FROM_FEED]-> FeedInfo ────────────────────────────────
UNWIND $rows AS row
MATCH (rp:RoutePattern)
MATCH (fi:FeedInfo {feed_version: row.feed_version})
MERGE (rp)-[:FROM_FEED]->(fi);

// ── Trip -[:FROM_FEED]-> FeedInfo ────────────────────────────────────────
// Large node set — load.py uses batch_write for this.
UNWIND $rows AS row
MATCH (t:Trip {trip_id: row.trip_id})
MATCH (fi:FeedInfo {feed_version: row.feed_version})
MERGE (t)-[:FROM_FEED]->(fi);

// ── ServicePattern -[:FROM_FEED]-> FeedInfo ──────────────────────────────
UNWIND $rows AS row
MATCH (sp:ServicePattern)
MATCH (fi:FeedInfo {feed_version: row.feed_version})
MERGE (sp)-[:FROM_FEED]->(fi);

// ── Date -[:FROM_FEED]-> FeedInfo ────────────────────────────────────────
UNWIND $rows AS row
MATCH (d:Date)
MATCH (fi:FeedInfo {feed_version: row.feed_version})
MERGE (d)-[:FROM_FEED]->(fi);

// ═══════════════════════════════════════════════════════════════════════════
// CROSS-LAYER RELATIONSHIPS (require Physical layer nodes)
// ═══════════════════════════════════════════════════════════════════════════

// ── Route -[:SERVES]-> Station ───────────────────────────────────────────
// Rail routes serve stations (derived via Platform parent_station lookup).
// $rows: [{route_id, stop_id}]  where stop_id is STN_ id
UNWIND $rows AS row
MATCH (r:Route {route_id: row.route_id})
MATCH (s:Station {id: row.stop_id})
MERGE (r)-[:SERVES]->(s);

// ── Route -[:SERVES]-> BusStop ───────────────────────────────────────────
// $rows: [{route_id, stop_id}]  where stop_id is numeric bus stop id
UNWIND $rows AS row
MATCH (r:Route {route_id: row.route_id})
MATCH (bs:BusStop {id: row.stop_id})
MERGE (r)-[:SERVES]->(bs);

// ── RoutePattern -[:STOPS_AT]-> Platform (rail) ──────────────────────────
// $rows: [{shape_id, stop_id, stop_sequence, is_terminus}]
UNWIND $rows AS row
MATCH (rp:RoutePattern {shape_id: row.shape_id})
MATCH (p:Platform {id: row.stop_id})
MERGE (rp)-[r:STOPS_AT]->(p)
SET r.stop_sequence = row.stop_sequence, r.is_terminus = row.is_terminus;

// ── RoutePattern -[:STOPS_AT]-> BusStop (bus) ────────────────────────────
// $rows: [{shape_id, stop_id, stop_sequence, is_terminus}]
UNWIND $rows AS row
MATCH (rp:RoutePattern {shape_id: row.shape_id})
MATCH (bs:BusStop {id: row.stop_id})
MERGE (rp)-[r:STOPS_AT]->(bs)
SET r.stop_sequence = row.stop_sequence, r.is_terminus = row.is_terminus;

// ── Trip -[:SCHEDULED_AT]-> Platform (rail) ──────────────────────────────
// $rows: [{trip_id, stop_id, arrival_time, departure_time, stop_sequence,
//          mode, shape_dist_traveled}]
UNWIND $rows AS row
MATCH (t:Trip {trip_id: row.trip_id})
MATCH (p:Platform {id: row.stop_id})
MERGE (t)-[r:SCHEDULED_AT {stop_sequence: row.stop_sequence}]->(p)
SET
  r.mode = row.mode,
  r.arrival_time = row.arrival_time,
  r.departure_time = row.departure_time,
  r.shape_dist_traveled = row.shape_dist_traveled;

// ── Trip -[:SCHEDULED_AT]-> BusStop (bus) ────────────────────────────────
// $rows: [{trip_id, stop_id, arrival_time, departure_time, stop_sequence,
//          mode, timepoint}]
UNWIND $rows AS row
MATCH (t:Trip {trip_id: row.trip_id})
MATCH (bs:BusStop {id: row.stop_id})
MERGE (t)-[r:SCHEDULED_AT {stop_sequence: row.stop_sequence}]->(bs)
SET
  r.mode = row.mode,
  r.arrival_time = row.arrival_time,
  r.departure_time = row.departure_time,
  r.timepoint = row.timepoint;
