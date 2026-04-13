// demos/queries/04_accessibility_outages.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Accessibility layer — OutageEvent nodes linked to the Pathways they affect
// and the Stations that contain those pathways.
// Returns nodes + relationships so Neo4j Browser renders a graph.
//
// Q1  All active outages network-wide — OutageEvent → Pathway → Station.
// Q2  Gallery Place detail — the three active escalator outages at one station.
// Q3  Glenmont elevator outage — shows an Elevator pathway node.
// ═══════════════════════════════════════════════════════════════════════════


// ── Q1: Network-wide active outages ──────────────────────────────────────
// Each OutageEvent node connects via -[:AFFECTS]-> to the Pathway (Escalator
// or Elevator) it has knocked out.  The Station that contains the pathway
// appears at the top level.  Limit to keep the graph readable.
MATCH path_out = (o:OutageEvent {status: 'active'})-[:AFFECTS]->(p:Pathway)
MATCH path_stn = (stn:Station)-[:CONTAINS]->(p)
RETURN o, p, stn, path_out, path_stn LIMIT 30;


// ── Q2: Gallery Place — active escalator outages ─────────────────────────
// Gallery Place currently has three escalators out (Modernization programme).
// Shows the OutageEvent → Escalator ← Station subgraph for that station.
MATCH path_out = (o:OutageEvent {status: 'active'})-[:AFFECTS]->(p:Pathway:Escalator)
MATCH path_stn = (stn:Station {name: 'Gallery Place'})-[:CONTAINS]->(p)
RETURN o, p, stn, path_out, path_stn;


// ── Q3: Glenmont — active elevator outage ────────────────────────────────
// Glenmont has an active Elevator outage.  Returns the Elevator pathway node
// so you can see both the OutageEvent and the physical Elevator node it targets.
MATCH path_out = (o:OutageEvent {status: 'active'})-[:AFFECTS]->(p:Pathway:Elevator)
MATCH path_stn = (stn:Station {name: 'Glenmont'})-[:CONTAINS]->(p)
RETURN o, p, stn, path_out, path_stn;
