// demos/queries/05_trip_schedule.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Service schedule layer — Trip nodes linked to Platforms, RoutePatterns,
// and ServicePatterns.
// Returns nodes + relationships so Neo4j Browser renders a graph.
//
// Q1  A sample of Red Line trips scheduled at Metro Center — Trip → Platform
//     ← Station, Trip → RoutePattern ← Route.
// Q2  RoutePattern graph for the Red Line — Route → patterns → sample trips.
// Q3  ServicePattern calendar — one Weekday pattern with its active Dates.
// ═══════════════════════════════════════════════════════════════════════════


// ── Q1: Trips at Metro Center (Red Line, sample of 10) ───────────────────
// Shows Trip nodes arriving at the two Red Line platforms.  Each Trip also
// points to its RoutePattern and its parent Route, giving a three-layer view:
//   Route → RoutePattern → Trip → Platform ← Station
MATCH (stn:Station {name: 'Metro Center'})-[:CONTAINS]->(plt:Platform)
WHERE plt.id IN ['PF_A01_1', 'PF_A01_2']
MATCH path_sched = (t:Trip)-[:SCHEDULED_AT]->(plt)
MATCH path_patt  = (t)-[:FOLLOWS]->(rp:RoutePattern)<-[:HAS_PATTERN]-(r:Route {route_id: 'RED'})
RETURN stn, plt, t, rp, r, path_sched, path_patt LIMIT 10;


// ── Q2: Red Line route patterns ──────────────────────────────────────────
// Shows how the Red Line breaks into multiple RoutePattern branches.
// Each pattern node sits between the Route and its trips.
MATCH path = (r:Route {route_id: 'RED'})-[:HAS_PATTERN]->(rp:RoutePattern)
RETURN path;


// ── Q3: ServicePattern calendar — Weekday pattern with active Dates ───────
// Picks one Weekday ServicePattern and shows the dates it is active on.
// Limit to 14 dates so the graph stays readable (two weeks of service).
MATCH (sp:ServicePattern:Weekday)
WITH sp LIMIT 1
MATCH path = (sp)-[:ACTIVE_ON]->(d:Date)
RETURN sp, path LIMIT 14;
