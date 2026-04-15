// queries/accessibility/analytical.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Analytical queries for the Accessibility layer
//
// Live traversal path:
//   (:Station)-[:CONTAINS]->(:Pathway:Elevator)<-[:AFFECTS]-(:OutageEvent)
//   (:Station)-[:CONTAINS]->(:Pathway:Escalator)-[:STARTING_LEVEL]->(:Level)
//
// Anchor injection:
//   Prefer $station_id (resolved ID e.g. 'STN_A01_C01') when available.
//   Fall back to s.name CONTAINS $station_name for fuzzy name matching.
//
// OPTIONAL MATCH is required for outage lookups — null outage fields mean
// the unit has no recorded outage, which is itself a meaningful answer
// ("elevator is working / no active outage found").
//
// OutageEvent (WMATA Incidents API) and Interruption:Accessibility (GTFS-RT)
// are SEPARATE node types from separate sources — they are NOT linked in the
// current schema. Query them independently; never attempt to JOIN them.
// ═══════════════════════════════════════════════════════════════════════════

// ── Q1: Elevator outage status at a station (resolved anchor ID) ─────────
// "Is the elevator at Metro Center out of service?"
// Uses $station_id from anchor resolver (e.g. 'STN_A01_C01').
// OPTIONAL MATCH — null o fields mean no outage recorded (elevator working).
MATCH (s:Station {id: $station_id})
MATCH (s)-[:CONTAINS]->(e:Pathway:Elevator)
OPTIONAL MATCH (o:OutageEvent)-[:AFFECTS]->(e)
RETURN
  s.name                       AS station,
  e.id                         AS elevator_id,
  e.from_stop_desc             AS from_desc,
  e.to_stop_desc               AS to_desc,
  o.unit_name                  AS outage_unit,
  o.status                     AS status,
  o.symptom_description        AS symptom,
  o.estimated_return           AS eta
ORDER BY o.date_updated DESC;

// ── Q2: Elevator outage status at a station (name fallback) ──────────────
// Use when anchor resolver returns no ID — name CONTAINS for fuzzy match.
MATCH (s:Station)
WHERE s.name CONTAINS $station_name
MATCH (s)-[:CONTAINS]->(e:Pathway:Elevator)
OPTIONAL MATCH (o:OutageEvent)-[:AFFECTS]->(e)
RETURN
  s.name                       AS station,
  e.id                         AS elevator_id,
  e.from_stop_desc             AS from_desc,
  e.to_stop_desc               AS to_desc,
  o.unit_name                  AS outage_unit,
  o.status                     AS status,
  o.symptom_description        AS symptom,
  o.estimated_return           AS eta
ORDER BY o.date_updated DESC;

// ── Q3: Escalator level coverage at a station ────────────────────────────
// "Which escalators at Pentagon City connect street level to the platform?"
// STARTING_LEVEL = from_stop_id end (direction of travel matters).
// ENDING_LEVEL   = to_stop_id end.
// level_index: 0 = street, negative = underground, positive = above ground.
MATCH (s:Station {id: $station_id})
MATCH (s)-[:CONTAINS]->(e:Pathway:Escalator)
MATCH (e)-[:STARTING_LEVEL]->(sl:Level)
MATCH (e)-[:ENDING_LEVEL]->(el:Level)
RETURN
  e.id                         AS escalator_id,
  e.from_stop_desc             AS from_desc,
  e.to_stop_desc               AS to_desc,
  sl.level_name                AS from_level,
  sl.level_index               AS from_index,
  el.level_name                AS to_level,
  el.level_index               AS to_index
ORDER BY sl.level_index;

// ── Q4: Active outage count at a station ─────────────────────────────────
// "How many elevators are out of service at Metro Center?"
// "How many accessible elevator routes connect street level to the platform, and are any out of service?"
// count(DISTINCT o) avoids fan-out from multiple elevator matches.
// NULL o.unit_name means no active outage for that elevator (working).
// DO NOT use Level nodes or ON_LEVEL for outage count questions — use CONTAINS → Elevator directly.
// DO NOT use AFFECTS_ROUTE or SERVES here — those are delay_propagation, not accessibility.
MATCH (s:Station {id: $station_id})
OPTIONAL MATCH (s)-[:CONTAINS]->(e:Pathway:Elevator)
OPTIONAL MATCH (o:OutageEvent {status: 'active'})-[:AFFECTS]->(e)
RETURN
  s.name                       AS station,
  count(DISTINCT e)            AS total_elevators,
  count(DISTINCT o)            AS active_outages,
  collect(DISTINCT e.id)       AS elevator_ids;

// ── Q4b: Elevator count + outage check (level phrasing, no Level filter) ──
// "How many accessible elevator routes connect street level to the platform at Metro Center,
//  and are any of those elevators currently out of service?"
// CRITICAL: Even though the question mentions "street level" and "platform level", do NOT
// filter by Level nodes or use ON_LEVEL. All elevators at a station serve floor-to-floor
// movement — count them all via CONTAINS then check for active outages via AFFECTS.
// Level nodes are only needed when the question asks for the level_name or level_index values.
MATCH (s:Station {id: $station_id})
OPTIONAL MATCH (s)-[:CONTAINS]->(e:Pathway:Elevator)
OPTIONAL MATCH (o:OutageEvent {status: 'active'})-[:AFFECTS]->(e)
RETURN
  s.name                       AS station,
  count(DISTINCT e)            AS total_elevators,
  count(DISTINCT o)            AS active_outages,
  collect(DISTINCT e.id)       AS elevator_ids;

// ── Q5: Escalator outage status at a station ─────────────────────────────
// Same OPTIONAL MATCH pattern as Q1 — null fields mean no recorded outage.
MATCH (s:Station {id: $station_id})
MATCH (s)-[:CONTAINS]->(e:Pathway:Escalator)
OPTIONAL MATCH (o:OutageEvent)-[:AFFECTS]->(e)
RETURN
  s.name                       AS station,
  e.id                         AS escalator_id,
  e.from_stop_desc             AS from_desc,
  e.to_stop_desc               AS to_desc,
  o.unit_name                  AS outage_unit,
  o.status                     AS status,
  o.symptom_description        AS symptom,
  o.estimated_return           AS eta
ORDER BY o.date_updated DESC;

// ── Q6: Accessibility interruptions at a station (GTFS-RT source) ────────
// Interruption:Accessibility nodes are from GTFS-RT ServiceAlerts —
// a separate source from OutageEvent. Not linked. Query independently.
MATCH (s:Station {id: $station_id})
MATCH (i:Interruption:Accessibility)-[:AFFECTS_STOP]->(s)
MATCH (i)-[:ON_DATE]->(d:Date)
RETURN
  i.interruption_id            AS interruption_id,
  i.effect                     AS effect,
  i.severity                   AS severity,
  i.description                AS description,
  d.date                       AS date
ORDER BY d.date DESC;

// ── Q7: Escalators reachable from station entrances (LINKS traversal) ────
// "Which escalators at Pentagon City connect to the platform level?"
// "Are the escalators at Metro Center working?"
// Use StationEntrance → LINKS → Escalator when the question is about which
// escalators a passenger can actually reach from the station entrance.
// Do NOT use CONTAINS for this — CONTAINS returns all escalators including
// ones not reachable via entrances. LINKS*1..3 follows the physical path graph.
MATCH (s:Station)-[:CONTAINS]->(se:StationEntrance)
WHERE s.name CONTAINS $station_name
MATCH (se)-[:LINKS*1..3]-(e:Pathway:Escalator)
RETURN DISTINCT
  s.name          AS station,
  e.id            AS escalator_id,
  se.name         AS entrance_name
ORDER BY e.id;

// ── Q8: Elevator reachable from station entrances (LINKS traversal) ──────
// "Is the elevator at Gallery Place working right now?"
// "Any elevator issues at Gallery Place?"
// Same LINKS traversal as Q7 but for Elevator nodes.
// Returns the elevator ID and entrance name — then check OutageEvent via AFFECTS.
MATCH (s:Station)-[:CONTAINS]->(se:StationEntrance)
WHERE s.name CONTAINS $station_name
MATCH (se)-[:LINKS*1..3]-(e:Pathway:Elevator)
OPTIONAL MATCH (o:OutageEvent {status: 'active'})-[:AFFECTS]->(e)
RETURN DISTINCT
  s.name          AS station,
  e.id            AS elevator_id,
  se.name         AS entrance_name,
  o.unit_name     AS outage_unit,
  o.status        AS outage_status,
  o.symptom_description AS symptom
ORDER BY e.id;
