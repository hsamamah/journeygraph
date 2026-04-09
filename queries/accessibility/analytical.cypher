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

// ── Q4: Elevator outages + route disruptions at a station (independent) ──
// "Are there elevator outages at Metro Center, and any service disruptions?"
// OutageEvent and Interruption are SEPARATE sources — query independently,
// never join them. count(DISTINCT ...) handles OPTIONAL MATCH fan-out.
MATCH (s:Station {id: $station_id})
OPTIONAL MATCH (s)-[:CONTAINS]->(e:Pathway:Elevator)
OPTIONAL MATCH (o:OutageEvent)-[:AFFECTS]->(e)
OPTIONAL MATCH (i:Interruption)-[:AFFECTS_ROUTE]->(r:Route)-[:SERVES]->(s)
RETURN
  s.name                       AS station,
  count(DISTINCT o)            AS active_outages,
  count(DISTINCT i)            AS route_disruptions;

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
