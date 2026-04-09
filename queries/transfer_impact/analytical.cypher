// queries/transfer_impact/analytical.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Analytical queries for the Transfer Impact domain
//
// Core questions: which trips were skipped/cancelled on a date, how many,
// and which transfer opportunities did those interruptions break?
//
// Two interruption types in this domain:
//   :Interruption:Skip         — individual trip stop skipped
//   :Interruption:Cancellation — entire trip cancelled
// Use the multi-label directly — do NOT filter on interruption_type property.
//
// Temporal pattern for "most recently":
//   Step 1 — find the most recent Date with matching events (WITH d ... LIMIT 1)
//   Step 2 — re-match using that Date to count/aggregate
//   This two-pass pattern is required — a single MATCH with ORDER BY + LIMIT 1
//   would return one row, not one date's worth of events.
//
// Anchor injection:
//   $route_short_name — rail short name e.g. 'Y' (Yellow), 'O' (Orange),
//                       'R' (Red), 'B' (Blue), 'G' (Green), 'S' (Silver)
//   $station_id       — resolved station id e.g. 'STN_A01_C01'
//
// Trip has no mode label — traverse FOLLOWS → RoutePattern → BELONGS_TO
// → :Route:Bus|:Route:Rail to determine mode.
//
// date value format is YYYYMMDD string e.g. '20260315' — not a Date type.
// ═══════════════════════════════════════════════════════════════════════════

// ── Q1: Skipped trip count on a named rail route — most recent date ──────
// "How many Yellow Line trips were skipped most recently?"
// $route_short_name = 'Y' for Yellow, 'O' for Orange, 'R' for Red, etc.
MATCH (d:Date)<-[:ON_DATE]-(i:Interruption:Skip)-[:AFFECTS_ROUTE]->(r:Route:Rail)
WHERE r.route_short_name = $route_short_name
WITH d ORDER BY d.date DESC LIMIT 1
MATCH (d)<-[:ON_DATE]-(i:Interruption:Skip)-[:AFFECTS_ROUTE]->(r:Route:Rail)
WHERE r.route_short_name = $route_short_name
RETURN count(i) AS skipped_trips, d.date AS date;

// ── Q2: Cancelled trip count on a named rail route — most recent date ────
// Same two-pass temporal pattern as Q1, using Cancellation instead of Skip.
MATCH (d:Date)<-[:ON_DATE]-(i:Interruption:Cancellation)-[:AFFECTS_ROUTE]->(r:Route:Rail)
WHERE r.route_short_name = $route_short_name
WITH d ORDER BY d.date DESC LIMIT 1
MATCH (d)<-[:ON_DATE]-(i:Interruption:Cancellation)-[:AFFECTS_ROUTE]->(r:Route:Rail)
WHERE r.route_short_name = $route_short_name
RETURN r.route_long_name AS route, count(DISTINCT i) AS cancelled_trips, d.date AS date;

// ── Q3: Bus routes with most skipped trips — most recent date ────────────
// "Which bus routes near Metro Center have had trips skipped most recently?"
// No route anchor — returns all bus routes ranked by skip count on latest date.
MATCH (d:Date)<-[:ON_DATE]-(i:Interruption:Skip)-[:AFFECTS_ROUTE]->(r:Route:Bus)
WITH d ORDER BY d.date DESC LIMIT 1
MATCH (d)<-[:ON_DATE]-(i:Interruption:Skip)-[:AFFECTS_ROUTE]->(r:Route:Bus)
RETURN
  r.route_short_name AS route,
  r.route_long_name  AS name,
  count(i)           AS skipped_trips,
  d.date             AS date
ORDER BY skipped_trips DESC
LIMIT 10;

// ── Q4: Skipped trips by route and date — parameterized ─────────────────
// Deeper version of Q1: returns per-trip detail for the most recent skip date.
// Uses AFFECTS_TRIP for trip-level granularity rather than route-level count.
MATCH (d:Date)<-[:ON_DATE]-(i:Interruption:Skip)-[:AFFECTS_ROUTE]->(r:Route:Rail)
WHERE r.route_short_name = $route_short_name
WITH d ORDER BY d.date DESC LIMIT 1
MATCH (d)<-[:ON_DATE]-(i:Interruption:Skip)-[:AFFECTS_TRIP]->(t:Trip)
      -[:FOLLOWS]->(rp:RoutePattern)-[:BELONGS_TO]->(r:Route:Rail)
WHERE r.route_short_name = $route_short_name
RETURN
  r.route_long_name AS route,
  count(DISTINCT t) AS skipped_services,
  d.date            AS date;

// ── Q5: Transfer partner impact — trips sharing platform with cancelled trip
// Which other trips are affected when a trip is cancelled at a station?
// Transfer partners share a Platform via SCHEDULED_AT — no TRANSFERS relationship
// exists in the WMATA feed. Only rail trips use Platform; bus uses BusStop.
MATCH (s:Station {id: $station_id})
MATCH (s)-[:CONTAINS]->(p:Platform)
MATCH (i:Interruption:Cancellation)-[:AFFECTS_TRIP]->(t_cancelled:Trip)
      -[:SCHEDULED_AT]->(p)
MATCH (t_partner:Trip)-[:SCHEDULED_AT]->(p)
WHERE t_partner <> t_cancelled
RETURN
  p.id                              AS platform,
  count(DISTINCT t_cancelled)       AS cancelled_trips,
  count(DISTINCT t_partner)         AS transfer_partners_affected;

// ── Q6: System-wide skip summary by route — most recent date ─────────────
// All routes (rail + bus) ranked by skip count on the most recent date.
MATCH (d:Date)<-[:ON_DATE]-(i:Interruption:Skip)-[:AFFECTS_ROUTE]->(r:Route)
WITH d ORDER BY d.date DESC LIMIT 1
MATCH (d)<-[:ON_DATE]-(i:Interruption:Skip)-[:AFFECTS_ROUTE]->(r:Route)
MATCH (i)-[:AFFECTS_TRIP]->(t:Trip)-[:FOLLOWS]->(rp:RoutePattern)-[:BELONGS_TO]->(r)
RETURN
  r.route_short_name            AS route,
  r.mode                        AS mode,
  count(DISTINCT i)             AS skip_events,
  count(DISTINCT t)             AS affected_trips,
  d.date                        AS date
ORDER BY affected_trips DESC
LIMIT 15;
