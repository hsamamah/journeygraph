// queries/physical/relationships.cypher
// Parameterised MERGE statements for all physical layer relationships.
// Called by physical/load.py via _load_query() + _extract_statement().

// ── Station -[:CONTAINS]-> StationEntrance ─────────────────────────────────
// $rows: [{station_id, entrance_id}]
// station_id = parent_station (GTFS), entrance_id = child stop id.
UNWIND $rows AS row
MATCH (s:Station        {id: row.station_id})
MATCH (e:StationEntrance {id: row.entrance_id})
MERGE (s)-[:CONTAINS]->(e);

// ── Station -[:CONTAINS]-> Platform ────────────────────────────────────────
// $rows: [{station_id, platform_id}]
UNWIND $rows AS row
MATCH (s:Station  {id: row.station_id})
MATCH (p:Platform {id: row.platform_id})
MERGE (s)-[:CONTAINS]->(p);

// ── Pathway -[:LINKS]-> StationEntrance ─────────────────────────────────────
// Called once for from_stop connections and once for to_stop connections.
// Python partitions rows by location_type = 2 before passing here.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (pw:Pathway         {id: row.pathway_id})
MATCH (e:StationEntrance  {id: row.stop_id})
MERGE (pw)-[:LINKS]->(e);

// ── Pathway -[:LINKS]-> Platform ────────────────────────────────────────────
// Python partitions rows by location_type in {0, 4} before passing here.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (pw:Pathway  {id: row.pathway_id})
MATCH (p:Platform  {id: row.stop_id})
MERGE (pw)-[:LINKS]->(p);

// ── Pathway -[:LINKS]-> Station ─────────────────────────────────────────────
// Python partitions rows by location_type = 1 before passing here.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (pw:Pathway {id: row.pathway_id})
MATCH (s:Station  {id: row.stop_id})
MERGE (pw)-[:LINKS]->(s);

// ── Pathway -[:LINKS]-> FareGate ────────────────────────────────────────────
// Python partitions rows by location_type = 3 before passing here.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (pw:Pathway {id: row.pathway_id})
MATCH (fg:FareGate {id: row.stop_id})
MERGE (pw)-[:LINKS]->(fg);
