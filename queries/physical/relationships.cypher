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

// ── Station -[:CONTAINS]-> FareGate ────────────────────────────────────────
// $rows: [{station_id, faregate_id}]
UNWIND $rows AS row
MATCH (s:Station  {id: row.station_id})
MATCH (fg:FareGate {id: row.faregate_id})
MERGE (s)-[:CONTAINS]->(fg);

// ── StationEntrance -[:LINKS]-> Pathway (from_stop) ────────────────────────
// from_stop_id of the pathway resolves to a :StationEntrance node.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (e:StationEntrance {id: row.stop_id})
MATCH (pw:Pathway        {id: row.pathway_id})
MERGE (e)-[:LINKS]->(pw);

// ── Pathway -[:LINKS]-> StationEntrance (to_stop) ───────────────────────────
// to_stop_id of the pathway resolves to a :StationEntrance node.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (pw:Pathway        {id: row.pathway_id})
MATCH (e:StationEntrance {id: row.stop_id})
MERGE (pw)-[:LINKS]->(e);

// ── Platform -[:LINKS]-> Pathway (from_stop) ────────────────────────────────
// from_stop_id of the pathway resolves to a :Platform node.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (p:Platform {id: row.stop_id})
MATCH (pw:Pathway {id: row.pathway_id})
MERGE (p)-[:LINKS]->(pw);

// ── Pathway -[:LINKS]-> Platform (to_stop) ──────────────────────────────────
// to_stop_id of the pathway resolves to a :Platform node.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (pw:Pathway {id: row.pathway_id})
MATCH (p:Platform {id: row.stop_id})
MERGE (pw)-[:LINKS]->(p);

// ── Station -[:LINKS]-> Pathway (from_stop) ─────────────────────────────────
// from_stop_id of the pathway resolves to a :Station node.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (s:Station  {id: row.stop_id})
MATCH (pw:Pathway {id: row.pathway_id})
MERGE (s)-[:LINKS]->(pw);

// ── Pathway -[:LINKS]-> Station (to_stop) ───────────────────────────────────
// to_stop_id of the pathway resolves to a :Station node.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (pw:Pathway {id: row.pathway_id})
MATCH (s:Station  {id: row.stop_id})
MERGE (pw)-[:LINKS]->(s);

// ── FareGate -[:LINKS]-> Pathway (from_stop) ────────────────────────────────
// from_stop_id of the pathway resolves to a :FareGate node.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (fg:FareGate {id: row.stop_id})
MATCH (pw:Pathway  {id: row.pathway_id})
MERGE (fg)-[:LINKS]->(pw);

// ── Pathway -[:LINKS]-> FareGate (to_stop) ──────────────────────────────────
// to_stop_id of the pathway resolves to a :FareGate node.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (pw:Pathway  {id: row.pathway_id})
MATCH (fg:FareGate {id: row.stop_id})
MERGE (pw)-[:LINKS]->(fg);

// ── BusStop -[:LINKS]-> Pathway (from_stop) ─────────────────────────────────
// from_stop_id of the pathway resolves to a :BusStop node.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (b:BusStop  {id: row.stop_id})
MATCH (pw:Pathway {id: row.pathway_id})
MERGE (b)-[:LINKS]->(pw);

// ── Pathway -[:LINKS]-> BusStop (to_stop) ───────────────────────────────────
// to_stop_id of the pathway resolves to a :BusStop node.
// $rows: [{pathway_id, stop_id}]
UNWIND $rows AS row
MATCH (pw:Pathway {id: row.pathway_id})
MATCH (b:BusStop  {id: row.stop_id})
MERGE (pw)-[:LINKS]->(b);

// ── Pathway -[:LINKS]-> Pathway (chain) ─────────────────────────────────────
// pw_X.to_stop_id == pw_Y.from_stop_id via a deferred generic node pivot.
// Bidirectional mirrors are pre-computed in transform and included in $rows.
// $rows: [{from_pathway_id, to_pathway_id}]
UNWIND $rows AS row
MATCH (px:Pathway {id: row.from_pathway_id})
MATCH (py:Pathway {id: row.to_pathway_id})
MERGE (px)-[:LINKS]->(py);

// ── (stop_entity) -[:ON_LEVEL]-> Level ───────────────────────────────────────
// Wire all stop nodes (Station, StationEntrance, Platform, FareGate, BusStop)
// to the Level node they reside on.
// Each OPTIONAL MATCH uses its label's unique index — avoids the full node
// scan that a label-less MATCH (n {id: ...}) would cause.
// $rows: [{stop_id, level_id}]
UNWIND $rows AS row
OPTIONAL MATCH (stn:Station          {id: row.stop_id})
OPTIONAL MATCH (ent:StationEntrance  {id: row.stop_id})
OPTIONAL MATCH (pf:Platform          {id: row.stop_id})
OPTIONAL MATCH (fg:FareGate          {id: row.stop_id})
WITH row, coalesce(stn, ent, pf, fg) AS n
WHERE n IS NOT NULL
MATCH (lv:Level {level_id: row.level_id})
MERGE (n)-[:ON_LEVEL]->(lv);

// ── Pathway -[:ON_LEVEL]-> Level ──────────────────────────────────────────────
// For elevators: every Level in the traversed range (range derivation).
// For same-level pathways (walkways, stairs, etc.): the shared Level.
// $rows: [{pathway_id, level_id}]
UNWIND $rows AS row
MATCH (pw:Pathway {id: row.pathway_id})
MATCH (lv:Level {level_id: row.level_id})
MERGE (pw)-[:ON_LEVEL]->(lv);

// ── Pathway -[:STARTING_LEVEL]-> Level ────────────────────────────────────────
// For escalators: the level at from_stop_id (direction of travel matters).
// Also used for multi-level walkways/stairs whose endpoints differ.
// $rows: [{pathway_id, level_id}]
UNWIND $rows AS row
MATCH (pw:Pathway {id: row.pathway_id})
MATCH (lv:Level {level_id: row.level_id})
MERGE (pw)-[:STARTING_LEVEL]->(lv);

// ── Pathway -[:ENDING_LEVEL]-> Level ──────────────────────────────────────────
// For escalators: the level at to_stop_id.
// Also used for multi-level walkways/stairs whose endpoints differ.
// $rows: [{pathway_id, level_id}]
UNWIND $rows AS row
MATCH (pw:Pathway {id: row.pathway_id})
MATCH (lv:Level {level_id: row.level_id})
MERGE (pw)-[:ENDING_LEVEL]->(lv);

// ── Station -[:CONTAINS]-> Pathway via intermediate ──────────────────────────
// Derived shortcut — Path 1 of 2.
// Covers Pathways whose endpoints are StationEntrance, Platform, or FareGate:
//   Station -[:CONTAINS]-> intermediate -[:LINKS]- Pathway
// Cross-station pathways receive CONTAINS from both stations (intentional).
// Must run after all [:CONTAINS] and [:LINKS] edges exist. Idempotent via MERGE.
MATCH (s:Station)-[:CONTAINS]->(n)-[:LINKS]-(pw:Pathway)
MERGE (s)-[:CONTAINS]->(pw);

// ── Station -[:CONTAINS]-> Pathway direct ────────────────────────────────────
// Derived shortcut — Path 2 of 2.
// Covers DEFERRED-node pivot pathways linked directly to a Station node:
//   Station -[:LINKS]- Pathway
MATCH (s:Station)-[:LINKS]-(pw:Pathway)
MERGE (s)-[:CONTAINS]->(pw);
