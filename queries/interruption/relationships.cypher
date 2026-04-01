// queries/interruption/relationships.cypher
// Parameterised MERGE statements for all interruption layer relationships.
// ═══════════════════════════════════════════════════════════════════════════
// TIER 1: Raw source node relationships
// ═══════════════════════════════════════════════════════════════════════════
// ── TripUpdate -[:UPDATES]-> Trip ────────────────────────────────────────
// $rows: [{dedup_hash, trip_id}]
UNWIND $rows AS row
MATCH (tu:TripUpdate {dedup_hash: row.dedup_hash})
MATCH (t:Trip {trip_id: row.trip_id})
MERGE (tu)-[:UPDATES]->(t);

// ── TripUpdate -[:ON_DATE]-> Date ────────────────────────────────────────
// $rows: [{dedup_hash, date}]
UNWIND $rows AS row
MATCH (tu:TripUpdate {dedup_hash: row.dedup_hash})
MATCH (d:Date {date: row.date})
MERGE (tu)-[:ON_DATE]->(d);

// ── TripUpdate -[:HAS_STOP_UPDATE]-> StopTimeUpdate ──────────────────────
// $rows: [{dedup_hash, parent_entity_id, stop_sequence}]
UNWIND $rows AS row
MATCH (tu:TripUpdate {dedup_hash: row.dedup_hash})
MATCH
  (stu:StopTimeUpdate
    {parent_entity_id: row.parent_entity_id, stop_sequence: row.stop_sequence})
MERGE (tu)-[:HAS_STOP_UPDATE]->(stu);

// ── TripUpdate -[:FROM_FEED]-> FeedInfo ──────────────────────────────────
// $rows: [{dedup_hash, feed_version}]
UNWIND $rows AS row
MATCH (tu:TripUpdate {dedup_hash: row.dedup_hash})
MATCH (fi:FeedInfo {feed_version: row.feed_version})
MERGE (tu)-[:FROM_FEED]->(fi);

// ── StopTimeUpdate -[:AT_STOP]-> Platform|BusStop ────────────────────────
// $rows: [{parent_entity_id, stop_sequence, stop_id}]
// Prefix-based routing replaces OPTIONAL MATCH + coalesce:
//   PF_  → Platform (rail stop times)
//   else → BusStop  (bus stop times)
// This allows Neo4j to use the stop_id index on each label directly
// instead of running two scans per row.
// See CONVENTIONS.md → "Stop ID Prefix Conventions"
UNWIND $rows AS row
MATCH
  (stu:StopTimeUpdate
    {parent_entity_id: row.parent_entity_id, stop_sequence: row.stop_sequence})
CALL {
  WITH stu, row
  CALL {
    WITH row
    WITH row
    WHERE row.stop_id STARTS WITH 'PF_'
    MATCH (stop:Platform {id: row.stop_id})
    RETURN stop
      UNION ALL
    WITH row
    WITH row
    WHERE NOT row.stop_id STARTS WITH 'PF_'
    MATCH (stop:BusStop {id: row.stop_id})
    RETURN stop
  }
  MERGE (stu)-[:AT_STOP]->(stop)
};

// ── ServiceAlert -[:HAS_SELECTOR]-> EntitySelector ───────────────────────
// $rows: [{feed_entity_id, selector_group_id}]
UNWIND $rows AS row
MATCH (sa:ServiceAlert {feed_entity_id: row.feed_entity_id})
MATCH (es:EntitySelector {selector_group_id: row.selector_group_id})
MERGE (sa)-[:HAS_SELECTOR]->(es);

// ── ServiceAlert -[:ACTIVE_ON]-> Date ────────────────────────────────────
// $rows: [{feed_entity_id, date}]
UNWIND $rows AS row
MATCH (sa:ServiceAlert {feed_entity_id: row.feed_entity_id})
MATCH (d:Date {date: row.date})
MERGE (sa)-[:ACTIVE_ON]->(d);

// ── ServiceAlert -[:FROM_FEED]-> FeedInfo ────────────────────────────────
// $rows: [{feed_entity_id, feed_version}]
UNWIND $rows AS row
MATCH (sa:ServiceAlert {feed_entity_id: row.feed_entity_id})
MATCH (fi:FeedInfo {feed_version: row.feed_version})
MERGE (sa)-[:FROM_FEED]->(fi);

// ── EntitySelector -[:TARGETS_ROUTE]-> Route ─────────────────────────────
// $rows: [{selector_group_id, route_id}]
UNWIND $rows AS row
MATCH (es:EntitySelector {selector_group_id: row.selector_group_id})
MATCH (r:Route {route_id: row.route_id})
MERGE (es)-[:TARGETS_ROUTE]->(r);

// ── EntitySelector -[:TARGETS_TRIP]-> Trip ───────────────────────────────
// $rows: [{selector_group_id, trip_id}]
UNWIND $rows AS row
MATCH (es:EntitySelector {selector_group_id: row.selector_group_id})
MATCH (t:Trip {trip_id: row.trip_id})
MERGE (es)-[:TARGETS_TRIP]->(t);

// ── EntitySelector -[:TARGETS_STOP]-> Station|BusStop|Platform ───────────
// $rows: [{selector_group_id, stop_id}]
// Prefix-based routing:
//   STN_ → Station, PF_ → Platform, else → BusStop
UNWIND $rows AS row
MATCH (es:EntitySelector {selector_group_id: row.selector_group_id})
CALL {
  WITH es, row
  CALL {
    WITH row
    WITH row
    WHERE row.stop_id STARTS WITH 'STN_'
    MATCH (stop:Station {id: row.stop_id})
    RETURN stop
      UNION ALL
    WITH row
    WITH row
    WHERE row.stop_id STARTS WITH 'PF_'
    MATCH (stop:Platform {id: row.stop_id})
    RETURN stop
      UNION ALL
    WITH row
    WITH row
    WHERE
      NOT row.stop_id STARTS WITH 'STN_' AND NOT row.stop_id STARTS WITH 'PF_'
    MATCH (stop:BusStop {id: row.stop_id})
    RETURN stop
  }
  MERGE (es)-[:TARGETS_STOP]->(stop)
};

// ── EntitySelector -[:TARGETS_AGENCY]-> Agency ───────────────────────────
// $rows: [{selector_group_id, agency_id}]
UNWIND $rows AS row
MATCH (es:EntitySelector {selector_group_id: row.selector_group_id})
MATCH (a:Agency {agency_id: row.agency_id})
MERGE (es)-[:TARGETS_AGENCY]->(a);

// ═══════════════════════════════════════════════════════════════════════════
// TIER 2: Interruption relationships
// ═══════════════════════════════════════════════════════════════════════════

// ── Interruption -[:SOURCED_FROM]-> TripUpdate ───────────────────────────
// $rows: [{interruption_id, source_entity_id}]
UNWIND $rows AS row
MATCH (i:Interruption {interruption_id: row.interruption_id})
MATCH (tu:TripUpdate {feed_entity_id: row.source_entity_id})
MERGE (i)-[:SOURCED_FROM]->(tu);

// ── Interruption -[:SOURCED_FROM]-> ServiceAlert ─────────────────────────
// $rows: [{interruption_id, source_entity_id}]
UNWIND $rows AS row
MATCH (i:Interruption {interruption_id: row.interruption_id})
MATCH (sa:ServiceAlert {feed_entity_id: row.source_entity_id})
MERGE (i)-[:SOURCED_FROM]->(sa);

// ═══════════════════════════════════════════════════════════════════════════
// TIER 3: Cross-layer connections
// ═══════════════════════════════════════════════════════════════════════════

// ── Interruption -[:AFFECTS_TRIP]-> Trip ──────────────────────────────────
// $rows: [{interruption_id, trip_id}]
UNWIND $rows AS row
MATCH (i:Interruption {interruption_id: row.interruption_id})
MATCH (t:Trip {trip_id: row.trip_id})
MERGE (i)-[:AFFECTS_TRIP]->(t);

// ── Interruption -[:AFFECTS_ROUTE]-> Route ───────────────────────────────
// $rows: [{interruption_id, route_id}]
UNWIND $rows AS row
MATCH (i:Interruption {interruption_id: row.interruption_id})
MATCH (r:Route {route_id: row.route_id})
MERGE (i)-[:AFFECTS_ROUTE]->(r);

// ── Interruption -[:AFFECTS_STOP]-> Station|BusStop|Platform ─────────────
// $rows: [{interruption_id, stop_id}]
// Prefix-based routing:
//   STN_ → Station, PF_ → Platform, else → BusStop
UNWIND $rows AS row
MATCH (i:Interruption {interruption_id: row.interruption_id})
CALL {
  WITH i, row
  CALL {
    WITH row
    WITH row
    WHERE row.stop_id STARTS WITH 'STN_'
    MATCH (stop:Station {id: row.stop_id})
    RETURN stop
      UNION ALL
    WITH row
    WITH row
    WHERE row.stop_id STARTS WITH 'PF_'
    MATCH (stop:Platform {id: row.stop_id})
    RETURN stop
      UNION ALL
    WITH row
    WITH row
    WHERE
      NOT row.stop_id STARTS WITH 'STN_' AND NOT row.stop_id STARTS WITH 'PF_'
    MATCH (stop:BusStop {id: row.stop_id})
    RETURN stop
  }
  MERGE (i)-[:AFFECTS_STOP]->(stop)
};

// ── Interruption -[:ON_DATE]-> Date ──────────────────────────────────────
// $rows: [{interruption_id, date}]
UNWIND $rows AS row
MATCH (i:Interruption {interruption_id: row.interruption_id})
MATCH (d:Date {date: row.date})
MERGE (i)-[:ON_DATE]->(d);

// ═══════════════════════════════════════════════════════════════════════════
// POST-LOAD ENRICHMENT (Rules 5 + 6)
// Run after all nodes and relationships are loaded.
// ═══════════════════════════════════════════════════════════════════════════

// ── Rule 6: DURING_PLANNED_SERVICE — Interruption overlaps Maintenance ───
// Links Interruptions to ServicePattern:Maintenance when they share a date.
// $rows: (none — standalone query, no parameters)
MATCH
  (i:Interruption)-[:ON_DATE]->
  (d:Date)<-[:ACTIVE_ON]-
  (sp:ServicePattern:Maintenance)
MERGE (i)-[:DURING_PLANNED_SERVICE]->(sp);
