// demos/queries/03_interruption_layer.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Interruption layer — Skip, Delay, and Cancellation nodes with their
// connected Date, Route, Platform, and Station nodes.
// Returns nodes + relationships so Neo4j Browser renders a graph.
//
// Q1  One Skip interruption node fully expanded — shows the ON_DATE,
//     AFFECTS_ROUTE, and SOURCED_FROM relationships in a single subgraph.
// Q2  Blue Line incident 115 on 2026-04-13 — all 22 Skip nodes connected
//     to their affected Station and Platform nodes.
// Q3  The single Cancellation node with its Route and Date.
// ═══════════════════════════════════════════════════════════════════════════


// ── Q1: Single Skip node — full neighbourhood ────────────────────────────
// Shows every relationship on one Skip interruption:
//   -[:ON_DATE]->      the calendar date
//   -[:AFFECTS_ROUTE]-> the route it hit
//   -[:SOURCED_FROM]->  a sample of TripUpdate evidence nodes
// Limit TripUpdates to 5 so the graph stays legible.
MATCH (i:Interruption:Skip {interruption_id: 'int_skip_115_14'})
MATCH path_date  = (i)-[:ON_DATE]->(d:Date)
MATCH path_route = (i)-[:AFFECTS_ROUTE]->(r:Route)
MATCH path_tu    = (i)-[:SOURCED_FROM]->(tu:TripUpdate)
RETURN i, path_date, path_route, path_tu LIMIT 8;


// ── Q2: Incident 115 — all skipped stations on 2026-04-13 ────────────────
// A single Blue Line service disruption caused 22 consecutive platforms to
// be skipped by 46 trips.  Each Skip node is linked to the Platform it
// affected; the Platform connects to its Station.
// Platform ID is embedded in the description: "Stop PF_C05_2 skipped …"
MATCH (i:Interruption:Skip)-[:ON_DATE]->(d:Date {date: '20260413'})
MATCH (i)-[:AFFECTS_ROUTE]->(r:Route {route_id: 'BLUE'})
WHERE i.interruption_id STARTS WITH 'int_skip_115_'
WITH i, d, r,
     split(split(i.description, 'Stop ')[1], ' skipped')[0] AS platform_id
MATCH path_plt = (stn:Station)-[:CONTAINS]->(plt:Platform {id: platform_id})
MATCH path_int = (i)-[:ON_DATE]->(d)
MATCH path_rt  = (i)-[:AFFECTS_ROUTE]->(r)
RETURN i, stn, plt, path_plt, path_int, path_rt;


// ── Q3: Cancellation node with Route and Date ────────────────────────────
// The single trip-level Cancellation in the dataset — a Green Line trip
// cancelled on 2026-04-13.  Shows the two-hop path:
//   Cancellation -[:ON_DATE]-> Date
//   Cancellation -[:AFFECTS_ROUTE]-> Route
MATCH (i:Interruption:Cancellation)
MATCH path_date  = (i)-[:ON_DATE]->(d:Date)
MATCH path_route = (i)-[:AFFECTS_ROUTE]->(r:Route)
RETURN i, path_date, path_route;
