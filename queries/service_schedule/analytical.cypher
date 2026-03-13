// queries/service_schedule/analytical.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Analytical queries for the Service & Schedule layer
// All queries below run against service layer nodes only — no Physical
// layer dependency. Runnable immediately after --layers service_schedule.
// ═══════════════════════════════════════════════════════════════════════════


// ── Q1: Service overview — node counts by type ───────────────────────────
// Quick health check after loading. Confirms expected counts.
MATCH (r:Route)  WITH count(r) AS routes
MATCH (t:Trip)   WITH routes, count(t) AS trips
MATCH (rp:RoutePattern) WITH routes, trips, count(rp) AS patterns
MATCH (sp:ServicePattern) WITH routes, trips, patterns, count(sp) AS services
MATCH (d:Date)   WITH routes, trips, patterns, services, count(d) AS dates
RETURN routes, trips, patterns, services, dates;


// ── Q2: Routes by mode — bus vs rail breakdown ───────────────────────────
MATCH (r:Route)
RETURN r.mode AS mode,
       count(r) AS route_count,
       collect(r.route_short_name)[..5] AS sample_names
ORDER BY route_count DESC;


// ── Q3: Service patterns by label — weekday/saturday/sunday/holiday/maintenance
MATCH (sp:ServicePattern)
RETURN labels(sp) AS labels,
       count(sp) AS count,
       collect(sp.service_id) AS service_ids
ORDER BY count DESC;


// ── Q4: Active dates per service pattern ─────────────────────────────────
// How many days does each service pattern run?
MATCH (sp:ServicePattern)-[:ACTIVE_ON]->(d:Date)
RETURN sp.service_id AS service_id,
       labels(sp) AS labels,
       count(d) AS active_days,
       min(d.date) AS first_date,
       max(d.date) AS last_date
ORDER BY active_days DESC;


// ── Q5: Busiest dates — which dates have the most trips? ─────────────────
// Traverses ServicePattern→Trip to count trips per date.
MATCH (d:Date)<-[:ACTIVE_ON]-(sp:ServicePattern)<-[:OPERATED_ON]-(t:Trip)
RETURN d.date AS date,
       d.day_of_week AS day,
       count(t) AS trip_count
ORDER BY trip_count DESC
LIMIT 10;


// ── Q6: Quietest dates — holidays and maintenance windows ────────────────
MATCH (d:Date)<-[:ACTIVE_ON]-(sp:ServicePattern)<-[:OPERATED_ON]-(t:Trip)
RETURN d.date AS date,
       d.day_of_week AS day,
       count(t) AS trip_count
ORDER BY trip_count ASC
LIMIT 10;


// ── Q7: Holiday service — which holidays have service and how much? ──────
MATCH (sp:ServicePattern)-[ao:ACTIVE_ON]->(d:Date)
WHERE ao.holiday_name IS NOT NULL
WITH ao.holiday_name AS holiday,
     d.date AS date,
     sp.service_id AS service_id
MATCH (sp2:ServicePattern {service_id: service_id})<-[:OPERATED_ON]-(t:Trip)
RETURN holiday,
       date,
       count(t) AS trip_count
ORDER BY date;


// ── Q8: Trips per route — which routes have the most trips? ──────────────
MATCH (r:Route)-[:HAS_PATTERN]->(rp:RoutePattern)-[:HAS_TRIP]->(t:Trip)
RETURN r.route_id AS route,
       r.route_short_name AS name,
       r.mode AS mode,
       count(t) AS trip_count
ORDER BY trip_count DESC
LIMIT 15;


// ── Q9: Route patterns per route — pattern complexity ────────────────────
// Routes with many patterns have complex service (branches, short-turns).
MATCH (r:Route)-[:HAS_PATTERN]->(rp:RoutePattern)
RETURN r.route_id AS route,
       r.route_short_name AS name,
       r.mode AS mode,
       count(rp) AS pattern_count
ORDER BY pattern_count DESC
LIMIT 15;


// ── Q10: Block chaining — trips linked by block_id ───────────────────────
// Finds consecutive trips on the same vehicle (shared block_id).
// Critical for delay propagation analysis.
MATCH (t1:Trip), (t2:Trip)
WHERE t1.block_id = t2.block_id
  AND t1.trip_id < t2.trip_id
WITH t1.block_id AS block, count(*) AS chain_length
RETURN block, chain_length
ORDER BY chain_length DESC
LIMIT 10;


// ── Q11: Maintenance service patterns — what runs during planned work? ───
MATCH (sp:ServicePattern:Maintenance)-[:ACTIVE_ON]->(d:Date)
MATCH (sp)<-[:OPERATED_ON]-(t:Trip)-[:FOLLOWS]->(rp:RoutePattern)-[:BELONGS_TO]->(r:Route)
RETURN sp.service_id AS service,
       d.date AS date,
       r.route_short_name AS route,
       count(t) AS trips
ORDER BY date, route;


// ── Q12: Feed provenance — confirm all nodes traced to FeedInfo ──────────
MATCH (fi:FeedInfo)
OPTIONAL MATCH (a:Agency)-[:FROM_FEED]->(fi)
OPTIONAL MATCH (r:Route)-[:FROM_FEED]->(fi)
OPTIONAL MATCH (t:Trip)-[:FROM_FEED]->(fi)
OPTIONAL MATCH (sp:ServicePattern)-[:FROM_FEED]->(fi)
OPTIONAL MATCH (d:Date)-[:FROM_FEED]->(fi)
RETURN fi.feed_version AS version,
       fi.feed_start_date AS start,
       fi.feed_end_date AS end,
       count(DISTINCT a) AS agencies,
       count(DISTINCT r) AS routes,
       count(DISTINCT t) AS trips,
       count(DISTINCT sp) AS service_patterns,
       count(DISTINCT d) AS dates;
