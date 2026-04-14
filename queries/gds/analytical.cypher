// queries/gds/analytical.cypher
// ═══════════════════════════════════════════════════════════════════════════
// GDS analytical queries for the WMATA knowledge graph
//
// GDS 2.6+ requires a NAMED graph (string argument) — anonymous inline maps
// are not supported. Each query uses the two-step pattern:
//   1. CALL gds.graph.project('tmpName', nodeLabels, relConfig) YIELD graphName
//   2. CALL gds.algorithm.stream(graphName, {config}) YIELD ...
//
// Named temporary graphs are dropped by the caller after the query returns.
// Use the single-query chaining form (project YIELD graphName → algorithm)
// so the graph name flows through Cypher without a separate statement.
//
// Graph model notes:
//   Station nodes carry .id (e.g. 'STN_A01_C01') and .name
//   Route nodes (Rail subtype) carry .shortName and .longName
//   Route -[:SERVES]-> Station (one route may serve many stations)
//   Platform nodes are contained in Stations via CONTAINS
//   No direct Station-to-Station relationship exists; use Route-Station
//   bipartite graph for network analysis.
//
// Anchor injection (when available):
//   $station_id_from / $station_id_to — resolved station IDs
//   $station_id                       — single resolved station ID
//   $route_short_name                 — e.g. 'R' (Red), 'B' (Blue)
// ═══════════════════════════════════════════════════════════════════════════

// ── Q1: Shortest path between two stations (Dijkstra) ────────────────────
// "What is the shortest path from Metro Center to Pentagon City?"
// Projects Station+Route bipartite graph, finds hop-optimal path.
// $station_id_from / $station_id_to are resolved anchor IDs.
CALL gds.graph.project('tmpGds', ['Station', 'Route'],
    {SERVES: {type: 'SERVES', orientation: 'UNDIRECTED'}})
YIELD graphName
MATCH (source:Station {id: $station_id_from}), (target:Station {id: $station_id_to})
CALL gds.shortestPath.dijkstra.stream(graphName, {sourceNode: source, targetNode: target})
YIELD nodeIds, totalCost
RETURN
    [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS path_nodes,
    toInteger(totalCost)                               AS hop_count
ORDER BY hop_count ASC
LIMIT 1;

// ── Q2: PageRank — most influential transfer hubs ─────────────────────────
// "Which stations are the most important transfer hubs on the metro network?"
// PageRank on Route-Station bipartite graph: stations served by more
// important routes (those that serve important stations) score higher.
CALL gds.graph.project('tmpGds', ['Station', 'Route'],
    {SERVES: {type: 'SERVES', orientation: 'UNDIRECTED'}})
YIELD graphName
CALL gds.pageRank.stream(graphName)
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS node, score
WHERE node:Station
RETURN
    node.name       AS station_name,
    node.id         AS station_id,
    round(score, 4) AS pagerank_score
ORDER BY pagerank_score DESC
LIMIT 10;

// ── Q3: Betweenness centrality — critical choke-point stations ────────────
// "Which stations are the biggest choke points on the WMATA network?"
// High betweenness = many shortest paths in the Route-Station graph pass
// through this station. Removing it would disrupt the most connections.
CALL gds.graph.project('tmpGds', ['Station', 'Route'],
    {SERVES: {type: 'SERVES', orientation: 'UNDIRECTED'}})
YIELD graphName
CALL gds.betweenness.stream(graphName)
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS node, score
WHERE node:Station
RETURN
    node.name       AS station_name,
    round(score, 2) AS betweenness_score
ORDER BY betweenness_score DESC
LIMIT 10;

// ── Q4: Degree centrality — stations with most direct route connections ────
// "Which stations have the most connections to other stations?"
// Degree of a Station node = number of rail routes that serve it.
CALL gds.graph.project('tmpGds', ['Station', 'Route'],
    {SERVES: {type: 'SERVES', orientation: 'UNDIRECTED'}})
YIELD graphName
CALL gds.degree.stream(graphName)
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS node, score
WHERE node:Station
RETURN
    node.name        AS station_name,
    toInteger(score) AS route_count
ORDER BY route_count DESC
LIMIT 10;

// ── Q5: Louvain community detection — natural station clusters ────────────
// "Which groups of stations naturally cluster together on the network?"
// Communities in the Route-Station graph correspond to route corridors.
CALL gds.graph.project('tmpGds', ['Station', 'Route'],
    {SERVES: {type: 'SERVES', orientation: 'UNDIRECTED'}})
YIELD graphName
CALL gds.louvain.stream(graphName)
YIELD nodeId, communityId
WITH communityId, collect(gds.util.asNode(nodeId)) AS allNodes
WITH communityId,
     [n IN allNodes WHERE n:Station | n.name] AS stations
WHERE size(stations) > 0
RETURN
    communityId,
    size(stations) AS station_count,
    stations
ORDER BY station_count DESC;

// ── Q6: Weakly connected components — isolated station groups ─────────────
// "Are there any stations disconnected from the main WMATA network?"
// In the Route-Station graph all 98 stations should form one component.
CALL gds.graph.project('tmpGds', ['Station', 'Route'],
    {SERVES: {type: 'SERVES', orientation: 'UNDIRECTED'}})
YIELD graphName
CALL gds.wcc.stream(graphName)
YIELD nodeId, componentId
WITH componentId, collect(gds.util.asNode(nodeId)) AS allNodes
WITH componentId,
     [n IN allNodes WHERE n:Station | n.name] AS stations
WHERE size(stations) > 0
RETURN
    componentId,
    size(stations) AS station_count,
    stations
ORDER BY station_count DESC;

// ── Q7: Node similarity — stations with similar route connectivity ─────────
// "Which pairs of stations serve the most similar set of routes?"
// nodeSimilarity on the bipartite Route-Station graph measures Jaccard
// similarity based on shared routes between station pairs.
CALL gds.graph.project('tmpGds', ['Station', 'Route'],
    {SERVES: {type: 'SERVES', orientation: 'NATURAL'}})
YIELD graphName
CALL gds.nodeSimilarity.stream(graphName)
YIELD node1, node2, similarity
WITH gds.util.asNode(node1) AS a, gds.util.asNode(node2) AS b, similarity
WHERE a:Station AND b:Station
RETURN
    a.name               AS station_a,
    b.name               AS station_b,
    round(similarity, 4) AS route_similarity_score
ORDER BY route_similarity_score DESC
LIMIT 15;

// ── Q8: BFS reachability — stations reachable within N transfers ───────────
// "Which stations can be reached from L'Enfant Plaza within 2 transfers?"
// maxDepth:2 in the bipartite graph = Station → Route → Station (one transfer).
// $station_id is the resolved anchor ID for the source station.
CALL gds.graph.project('tmpGds', ['Station', 'Route'],
    {SERVES: {type: 'SERVES', orientation: 'UNDIRECTED'}})
YIELD graphName
MATCH (source:Station {id: $station_id})
CALL gds.bfs.stream(graphName, {sourceNode: source, maxDepth: 2})
YIELD path
UNWIND nodes(path) AS n
WITH DISTINCT n
WHERE n:Station AND n.id <> $station_id
RETURN
    n.name AS reachable_station,
    n.id   AS station_id
ORDER BY reachable_station;

// ── Q9: Triangle count — stations in tightly knit interchange clusters ─────
// "Which stations are part of the most tightly connected interchange clusters?"
// In the bipartite Route-Station graph, triangles form when two stations
// share two common routes (both served by the same pair of lines).
CALL gds.graph.project('tmpGds', ['Station', 'Route'],
    {SERVES: {type: 'SERVES', orientation: 'UNDIRECTED'}})
YIELD graphName
CALL gds.triangleCount.stream(graphName)
YIELD nodeId, triangleCount
WITH gds.util.asNode(nodeId) AS node, triangleCount
WHERE node:Station AND triangleCount > 0
RETURN
    node.name     AS station_name,
    triangleCount AS triangle_count
ORDER BY triangle_count DESC
LIMIT 10;

// ── Q10: BFS accessibility — physical pathway reachability from entrance ───
// "Which platforms and fare gates are reachable from a station entrance
//  via the physical LINKS graph (wheelchair pathway network)?"
// Projects the physical pathway graph (StationEntrance, Platform, FareGate,
// Pathway nodes connected by LINKS), then BFS from one entrance.
// Nodes returned = everything reachable = the accessible footprint.
// Replace station name to check any station.
CALL gds.graph.project('accessBFS',
    ['StationEntrance', 'Platform', 'FareGate', 'Pathway'],
    {LINKS: {type: 'LINKS', orientation: 'UNDIRECTED'}})
YIELD graphName
MATCH (stn:Station {name: 'Metro Center'})-[:CONTAINS]->(entrance:StationEntrance)
WITH graphName, entrance LIMIT 1
CALL gds.bfs.stream(graphName, {sourceNode: entrance})
YIELD path
UNWIND nodes(path) AS n
WITH DISTINCT n
WHERE n:Platform OR n:FareGate OR n:StationEntrance
RETURN labels(n)[0] AS node_type, n.name AS name, n.id AS id
ORDER BY node_type, name;
