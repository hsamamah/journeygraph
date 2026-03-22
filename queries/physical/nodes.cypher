// queries/physical/nodes.cypher
// Parameterised MERGE statements for all physical layer nodes.
// Called by physical/load.py via _load_query() + _extract_statement().
// Each UNWIND statement expects a list of dicts passed as $rows.
// Label-migration statements (no $rows) are called without parameters.
// ── :Station ───────────────────────────────────────────────────────────────
// $rows: [{id, name, location}]
// id is the GTFS stop_id (renamed by transform). See constraints.cypher note.
UNWIND $rows AS row
MERGE (s:Station {id: row.id})
SET s.name = row.name, s.location = row.location;

// ── :StationEntrance ───────────────────────────────────────────────────────
// $rows: [{id, name, location, wheelchair_accessible, level}]
UNWIND $rows AS row
MERGE (e:StationEntrance {id: row.id})
SET
  e.name = row.name,
  e.location = row.location,
  e.wheelchair_accessible = row.wheelchair_accessible,
  e.level = row.level;

// ── :Platform ──────────────────────────────────────────────────────────────
// $rows: [{id, name, lines_accessible, level}]
UNWIND $rows AS row
MERGE (p:Platform {id: row.id})
SET
  p.name = row.name,
  p.lines_accessible = row.lines_accessible,
  p.level = row.level;

// ── :BusStop ───────────────────────────────────────────────────────────────
// $rows: [{id, name, location}]
// id is the GTFS stop_id (renamed by transform). See constraints.cypher note.
UNWIND $rows AS row
MERGE (s:BusStop {id: row.id})
SET s.name = row.name, s.location = row.location;

// ── :FareGate ──────────────────────────────────────────────────────────────
// $rows: [{id, name, zone_id, is_bidirectional}]
UNWIND $rows AS row
MERGE (fg:FareGate {id: row.id})
SET
  fg.name = row.name,
  fg.zone_id = row.zone_id,
  fg.is_bidirectional = row.is_bidirectional;

// ── :Pathway ───────────────────────────────────────────────────────────────
// Base node. Mode and zone labels applied separately via migration queries below.
// $rows: [{id, from_stop_id, mode, zone, elevation_gain, wheelchair_accessible}]
UNWIND $rows AS row
MERGE (pw:Pathway {id: row.id})
SET
  pw.stop_id = row.from_stop_id,
  pw.mode = row.mode,
  pw.zone = row.zone,
  pw.elevation_gain = row.elevation_gain,
  pw.wheelchair_accessible = row.wheelchair_accessible;

// ── :Level ─────────────────────────────────────────────────────────────────
// $rows: [{level_id, level_index, level_name}]
UNWIND $rows AS row
MERGE (lv:Level {level_id: row.level_id})
SET lv.level_index = row.level_index, lv.level_name = row.level_name;

// ── Pathway :Elevator label ────────────────────────────────────────────────
// Migration query — no $rows. Run after all :Pathway nodes are committed.
// pathway_mode 5 = Elevator (GTFS spec).
MATCH (pw:Pathway)
WHERE pw.mode = 5
SET pw: Elevator;

// ── Pathway :Escalator label ───────────────────────────────────────────────
// pathway_mode 4 = Escalator.
MATCH (pw:Pathway)
WHERE pw.mode = 4
SET pw: Escalator;

// ── Pathway :Stairs label ──────────────────────────────────────────────────
// pathway_mode 2 = Stairs.
MATCH (pw:Pathway)
WHERE pw.mode = 2
SET pw: Stairs;

// ── Pathway :Walkway label ─────────────────────────────────────────────────
// pathway_mode 1 = Walkway.
MATCH (pw:Pathway)
WHERE pw.mode = 1
SET pw: Walkway;

// ── Pathway :PaidZone label ────────────────────────────────────────────────
// Applied to pathways whose zone property is 'Paid'.
MATCH (pw:Pathway)
WHERE pw.zone = 'Paid'
SET pw: PaidZone;

// ── Pathway :UnpaidZone label ──────────────────────────────────────────────
// Applied to pathways whose zone property is 'Unpaid'.
MATCH (pw:Pathway)
WHERE pw.zone = 'Unpaid'
SET pw: UnpaidZone;
