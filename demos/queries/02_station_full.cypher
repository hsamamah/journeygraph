// demos/queries/02_station_full.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Full station view — Metro Center's complete physical accessibility graph.
// Returns nodes + relationships so Neo4j Browser renders a graph.
//
// Q1  Station + all immediate children (platforms, entrances, fare gates,
//     elevators, escalators).
// Q2  Elevator nodes only with their Level nodes — shows the vertical spine.
// Q3  A single level's walkway network — the horizontal connectivity graph
//     on the Lower Mezzanine/Upper Platform floor.
// ═══════════════════════════════════════════════════════════════════════════


// ── Q1: Station + all direct pathway elements ────────────────────────────
// One-hop expansion from Metro Center.  The Browser will colour nodes by
// their label — Platform, Elevator, Escalator, StationEntrance, FareGate
// all appear as distinct node types around the central Station node.
MATCH path = (stn:Station {name: 'Metro Center'})-[:CONTAINS]->(child)
WHERE child:Platform
   OR child:StationEntrance
   OR child:FareGate
   OR child:Pathway:Elevator
   OR child:Pathway:Escalator
RETURN path;


// ── Q2: Elevator spine — Station → Elevator → Level ──────────────────────
// Each elevator connects a pair of Level nodes.  This query shows how the
// three elevators at Metro Center bridge Street → Mezzanine → Platform.
MATCH path = (stn:Station {name: 'Metro Center'})-[:CONTAINS]->(e:Pathway:Elevator)
             -[:ON_LEVEL]->(lv:Level)
RETURN path;


// ── Q3: Lower Mezzanine walkway network ──────────────────────────────────
// The Lower Mezzanine/Upper Platform level (level_id A01_C01_L2) hosts the
// paid/unpaid walkway graph that connects fare gates to platforms.
// Limit to 40 walkway nodes to keep the graph readable.
MATCH (lv:Level {level_id: 'A01_C01_L2'})
MATCH path = (lv)<-[:ON_LEVEL]-(w:Pathway:Walkway)
RETURN path LIMIT 40;
