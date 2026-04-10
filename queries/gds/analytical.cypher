// queries/gds/analytical.cypher
// ═══════════════════════════════════════════════════════════════════════════
// GDS analytical queries for the WMATA knowledge graph
//
// All queries use native anonymous projections (nodeProjection /
// relationshipProjection) — compatible with GDS Community and Enterprise.
// Do NOT use nodeQuery/relationshipQuery (Cypher projection) — that form
// requires GDS Enterprise and will fail on Community installations.
//
// All projections are ephemeral/anonymous — no named graph lifecycle.
// Never use gds.graph.drop.
//
// Graph model notes:
//   Station nodes carry .id (e.g. 'STN_A01_C01') and .name
//   Platform nodes are CONTAINED_IN Station
//   Trip -[:FOLLOWS]-> RoutePattern -[:BELONGS_TO]-> Route
//   Station -[:TRANSFER_TO]-> Station (interchange connections)
//   Trip -[:SCHEDULED_AT]-> Platform -[:CONTAINED_IN]-> Station
//
// Anchor injection (when available):
//   $station_id_from / $station_id_to — resolved station IDs
//   $station_id                       — single resolved station ID
//   $route_short_name                 — e.g. 'R' (Red), 'B' (Blue), 'D80' (bus)
// ═══════════════════════════════════════════════════════════════════════════

// ── Q1: Shortest path between two stations (Dijkstra) ────────────────────
// "What is the shortest path from Metro Center to Pentagon?"
// Uses native TRANSFER_TO projection. $station_id_from / $station_id_to
// are resolved anchor IDs.
MATCH (source:Station {id: $station_id_from}), (target:Station {id: $station_id_to})
CALL gds.shortestPath.dijkstra.stream(
    {
        nodeProjection: 'Station',
        relationshipProjection: {
            TRANSFER_TO: { type: 'TRANSFER_TO', orientation: 'NATURAL', properties: {} }
        }
    },
    { sourceNode: source, targetNode: target }
)
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
    [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS stations_on_path,
    toInteger(totalCost)                               AS transfer_hops
ORDER BY totalCost ASC
LIMIT 1;

// ── Q2: PageRank — most influential transfer hubs ─────────────────────────
// "Which stations are the most important transfer hubs on the metro network?"
CALL gds.pageRank.stream({
    nodeProjection: 'Station',
    relationshipProjection: {
        TRANSFER_TO: { type: 'TRANSFER_TO', orientation: 'NATURAL' }
    }
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS station, score
RETURN
    station.name    AS station_name,
    station.id      AS station_id,
    round(score, 4) AS pagerank_score
ORDER BY pagerank_score DESC
LIMIT 10;

// ── Q3: Betweenness centrality — critical choke-point stations ────────────
// "Which stations are the biggest choke points on the WMATA network?"
// High betweenness = many shortest paths pass through this station.
CALL gds.betweenness.stream({
    nodeProjection: 'Station',
    relationshipProjection: {
        TRANSFER_TO: { type: 'TRANSFER_TO', orientation: 'UNDIRECTED' }
    }
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS station, score
RETURN
    station.name    AS station_name,
    round(score, 2) AS betweenness_score
ORDER BY betweenness_score DESC
LIMIT 10;

// ── Q4: Degree centrality — stations with most direct connections ──────────
// "Which stations have the most connections to other stations?"
CALL gds.degree.stream({
    nodeProjection: 'Station',
    relationshipProjection: {
        TRANSFER_TO: { type: 'TRANSFER_TO', orientation: 'UNDIRECTED' }
    }
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS station, score
RETURN
    station.name          AS station_name,
    toInteger(score)      AS connection_count
ORDER BY connection_count DESC
LIMIT 10;

// ── Q5: Louvain community detection — natural station clusters ────────────
// "Which groups of stations naturally cluster together on the network?"
// Stations in the same community are more densely connected to each other.
CALL gds.louvain.stream({
    nodeProjection: 'Station',
    relationshipProjection: {
        TRANSFER_TO: { type: 'TRANSFER_TO', orientation: 'UNDIRECTED' }
    }
})
YIELD nodeId, communityId
WITH communityId, collect(gds.util.asNode(nodeId).name) AS stations
RETURN
    communityId,
    size(stations) AS station_count,
    stations
ORDER BY station_count DESC;

// ── Q6: Weakly connected components — isolated station groups ─────────────
// "Are there any stations disconnected from the main WMATA network?"
CALL gds.wcc.stream({
    nodeProjection: 'Station',
    relationshipProjection: {
        TRANSFER_TO: { type: 'TRANSFER_TO', orientation: 'UNDIRECTED' }
    }
})
YIELD nodeId, componentId
WITH componentId, collect(gds.util.asNode(nodeId).name) AS stations
RETURN
    componentId,
    size(stations) AS station_count,
    stations
ORDER BY station_count DESC;

// ── Q7: Node similarity — stations with similar route connectivity ─────────
// "Which pairs of stations serve the most similar set of routes?"
// Requires a bipartite Station→Route graph. Use SCHEDULED_AT + FOLLOWS + BELONGS_TO
// to derive the Station-serves-Route relationship.
// NOTE: nodeSimilarity requires that both node types are projected together.
// Project Station and Route nodes; add a synthetic SERVES relationship first:
//   MATCH (s:Station)-[:CONTAINS]->(p:Platform)<-[:SCHEDULED_AT]-(t:Trip)
//         -[:FOLLOWS]->(rp:RoutePattern)-[:BELONGS_TO]->(r:Route)
//   MERGE (s)-[:SERVES]->(r)
// If SERVES relationships are not yet materialised, use degree/PageRank instead.
CALL gds.nodeSimilarity.stream({
    nodeProjection: ['Station', 'Route'],
    relationshipProjection: {
        SERVES: { type: 'SERVES', orientation: 'NATURAL' }
    }
})
YIELD node1, node2, similarity
WITH gds.util.asNode(node1) AS a, gds.util.asNode(node2) AS b, similarity
WHERE a:Station AND b:Station
RETURN
    a.name              AS station_a,
    b.name              AS station_b,
    round(similarity, 4) AS route_similarity_score
ORDER BY route_similarity_score DESC
LIMIT 15;

// ── Q8: BFS reachability — stations reachable within N transfers ───────────
// "Which stations can be reached from Metro Center within 2 transfers?"
// $station_id is the resolved anchor ID for the source station.
MATCH (source:Station {id: $station_id})
CALL gds.bfs.stream(
    {
        nodeProjection: 'Station',
        relationshipProjection: {
            TRANSFER_TO: { type: 'TRANSFER_TO', orientation: 'NATURAL' }
        }
    },
    { sourceNode: source, maxDepth: 2 }
)
YIELD path
UNWIND nodes(path) AS stationNode
WITH DISTINCT stationNode
WHERE stationNode:Station AND stationNode.id <> $station_id
RETURN
    stationNode.name AS reachable_station,
    stationNode.id   AS station_id
ORDER BY reachable_station;

// ── Q9: Triangle count — stations in tightly knit interchange clusters ─────
// "Which stations are part of the most tightly connected interchange clusters?"
// High triangle count = station is part of many 3-way transfer cycles.
CALL gds.triangleCount.stream({
    nodeProjection: 'Station',
    relationshipProjection: {
        TRANSFER_TO: { type: 'TRANSFER_TO', orientation: 'UNDIRECTED' }
    }
})
YIELD nodeId, triangleCount
WITH gds.util.asNode(nodeId) AS station, triangleCount
WHERE triangleCount > 0
RETURN
    station.name  AS station_name,
    triangleCount AS triangle_count
ORDER BY triangle_count DESC
LIMIT 10;
