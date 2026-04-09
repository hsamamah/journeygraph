// queries/delay_propagation/analytical.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Analytical queries for the Delay Propagation domain
//
// Core questions: where are delays active, how severe, how far have they
// spread across trips and stops?
//
// Two traversal paths:
//   Route-based:  Interruption:Delay -[:AFFECTS_ROUTE]-> Route
//   Trip-based:   Interruption:Delay -[:AFFECTS_TRIP]->  Trip
//                   -[:FOLLOWS]-> RoutePattern -[:BELONGS_TO]-> Route
//
// Severity thresholds:
//   WARNING = 300–899 seconds (5–14 min)
//   SEVERE  = 900+ seconds    (15+ min)
// Use :Delay label directly — do NOT filter WHERE interruption_type='delay'.
//
// Temporal pattern for "most recently":
//   Step 1 — find the most recent Date with matching events (WITH d ... LIMIT 1)
//   Step 2 — re-match using that Date to count/aggregate
//
// Anchor injection:
//   $route_short_name — e.g. 'D80' (Wisconsin Av), 'C53', 'D40'
//   $station_id       — resolved station id e.g. 'STN_A01_C01'
//   $station_name     — fallback name match e.g. 'Gallery Place'
//
// end_time is nullable — add IS NOT NULL when querying resolved delays only.
// Absence of a StopTimeUpdate does NOT mean on-time — on-time stops are omitted.
// ═══════════════════════════════════════════════════════════════════════════

// ── Q1: Active delays on a specific bus route ────────────────────────────
// "Are there any active delays on the Wisconsin Ave bus corridor right now?"
// $route_short_name = 'D80' for Wisconsin Av-Union Station, etc.
MATCH (i:Interruption:Delay)-[:AFFECTS_ROUTE]->(r:Route:Bus)
WHERE r.route_short_name = $route_short_name
RETURN
  i.interruption_id  AS delay_id,
  i.severity         AS severity,
  i.start_time       AS started,
  i.description      AS description
ORDER BY i.start_time DESC
LIMIT 10;

// ── Q2: Bus routes with most delayed trips — most recent date ────────────
// "Which bus trips have been delayed most recently and how severe?"
// Two-pass temporal pattern: find most recent date, then aggregate.
MATCH (d:Date)<-[:ON_DATE]-(i:Interruption:Delay)-[:AFFECTS_TRIP]->(t:Trip)
WITH d ORDER BY d.date DESC LIMIT 1
MATCH (d)<-[:ON_DATE]-(i:Interruption:Delay)-[:AFFECTS_TRIP]->(t:Trip)
      -[:FOLLOWS]->(rp:RoutePattern)-[:BELONGS_TO]->(r:Route)
RETURN
  i.severity              AS severity,
  r.route_short_name      AS route,
  count(DISTINCT t)       AS affected_trips,
  d.date                  AS date
ORDER BY affected_trips DESC
LIMIT 10;

// ── Q3: Active delays at a station — via AFFECTS_STOP ───────────────────
// Delays directly linked to a station or its platforms.
// Anchor resolver injects the station id as a literal (e.g. 'STN_B01_F01').
// Two OPTIONAL MATCHes cover both AFFECTS_STOP → Station and
// AFFECTS_STOP → Platform (via Station -[:CONTAINS]-> Platform).
// IMPORTANT: include s in the WITH clause so it stays in scope for RETURN.
MATCH (s:Station {id: 'STN_B01_F01'})
OPTIONAL MATCH (i1:Interruption:Delay)-[:AFFECTS_STOP]->(s)
OPTIONAL MATCH (s)-[:CONTAINS]->(p:Platform)
OPTIONAL MATCH (i2:Interruption:Delay)-[:AFFECTS_STOP]->(p)
WITH s, coalesce(i1, i2) AS i
WHERE i IS NOT NULL
RETURN
  s.name             AS station,
  i.interruption_id  AS delay_id,
  i.severity         AS severity,
  i.start_time       AS started,
  i.description      AS description
ORDER BY i.start_time DESC;

// ── Q4: Delays via AFFECTS_TRIP at a station — resolved id ───────────────
// When AFFECTS_STOP has no data, find delays via trips scheduled at the
// station's platforms. Always use the resolved station id as a literal.
// WITH must carry s through to keep it in scope for RETURN.
MATCH (s:Station {id: 'STN_B01_F01'})
MATCH (s)-[:CONTAINS]->(p:Platform)<-[:SCHEDULED_AT]-(t:Trip)
MATCH (i:Interruption:Delay)-[:AFFECTS_TRIP]->(t)
WITH s, i
RETURN
  s.name             AS station,
  i.interruption_id  AS delay_id,
  i.severity         AS severity,
  i.start_time       AS started,
  i.description      AS description
ORDER BY i.start_time DESC
LIMIT 20;

// ── Q5: System-wide delay summary — all bus routes ranked ────────────────
// "Any bus delays downtown right now?" — no anchor, returns all active.
// Use when no specific route or station anchor is resolved.
MATCH (i:Interruption:Delay)-[:AFFECTS_ROUTE]->(r:Route:Bus)
RETURN
  r.route_short_name  AS route,
  count(i)            AS active_delays
ORDER BY active_delays DESC
LIMIT 10;

// ── Q6: Delay provenance — raw TripUpdate for a named route ─────────────
// Shows Tier 1 raw RT data behind a delay for LLM explainability.
// TripUpdate.source distinguishes rail ('gtfs_rt_rail') from bus ('gtfs_rt_bus').
MATCH (i:Interruption:Delay)-[:AFFECTS_ROUTE]->(r:Route)
WHERE r.route_short_name = $route_short_name
MATCH (i)-[:SOURCED_FROM]->(tu:TripUpdate)
MATCH (tu)-[:HAS_STOP_UPDATE]->(stu:StopTimeUpdate)
RETURN
  tu.trip_id          AS trip_id,
  tu.delay            AS delay_seconds,
  tu.source           AS source,
  stu.stop_id         AS stop_id,
  stu.arrival_delay   AS arrival_delay_s,
  stu.departure_delay AS departure_delay_s
ORDER BY tu.delay DESC
LIMIT 20;

// ── Q7: Stations with matching name — ambiguous anchor disambiguation ────
// "What disruptions are there at Washington?" — name matches multiple stations.
// Returns candidate stations so narration can surface the ambiguity.
MATCH (s:Station)
WHERE s.name CONTAINS $station_name
RETURN
  s.name        AS station,
  s.id          AS station_id
ORDER BY s.name;
