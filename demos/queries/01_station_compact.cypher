// demos/queries/01_station_compact.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Compact station view — Station node with its served Route nodes and Platforms.
// Returns nodes + relationships so Neo4j Browser renders a graph.
//
// Q1  Metro Center with all rail lines and platforms.
// Q2  L'Enfant Plaza — the busiest transfer hub (5 lines).
// ═══════════════════════════════════════════════════════════════════════════


// ── Q1: Metro Center — served routes + platforms ──────────────────────────
// Shows the Station node, each Route:Rail node that serves it, and each
// Platform it contains.  Renders as a star graph in the Browser.
MATCH (stn:Station {name: 'Metro Center'})
OPTIONAL MATCH path_routes = (stn)<-[:SERVES]-(r:Route:Rail)
OPTIONAL MATCH path_platforms = (stn)-[:CONTAINS]->(plt:Platform)
RETURN stn, path_routes, path_platforms;


// ── Q2: L'Enfant Plaza — the 5-line transfer hub ──────────────────────────
// L'Enfant Plaza is served by Blue, Green, Orange, Silver, and Yellow lines.
// Returns the same shape: Station + Route nodes + Platform nodes.
MATCH (stn:Station {name: "L'Enfant Plaza"})
OPTIONAL MATCH path_routes = (stn)<-[:SERVES]-(r:Route:Rail)
OPTIONAL MATCH path_platforms = (stn)-[:CONTAINS]->(plt:Platform)
RETURN stn, path_routes, path_platforms;
