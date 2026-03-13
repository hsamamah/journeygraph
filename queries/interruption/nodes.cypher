// queries/interruption/nodes.cypher
// Parameterised MERGE statements for all interruption layer nodes.
// Note: FeedInfo node managed by src/common/feed_info.py (shared).

// ═══════════════════════════════════════════════════════════════════════════
// TIER 1: Raw source nodes
// ═══════════════════════════════════════════════════════════════════════════

// ── :TripUpdate ──────────────────────────────────────────────────────────
// $rows: [{dedup_hash, feed_entity_id, trip_id, route_id, start_date,
//          start_time, schedule_relationship, delay, timestamp, source}]
UNWIND $rows AS row
MERGE (tu:TripUpdate {dedup_hash: row.dedup_hash})
SET   tu.feed_entity_id        = row.feed_entity_id,
      tu.trip_id               = row.trip_id,
      tu.route_id              = row.route_id,
      tu.start_date            = row.start_date,
      tu.start_time            = row.start_time,
      tu.schedule_relationship = row.schedule_relationship,
      tu.delay                 = row.delay,
      tu.timestamp             = row.timestamp,
      tu.source                = row.source;

// ── :StopTimeUpdate ──────────────────────────────────────────────────────
// $rows: [{parent_entity_id, stop_sequence, stop_id, arrival_delay,
//          arrival_time, departure_delay, departure_time, schedule_relationship}]
// Keyed on parent_entity_id + stop_sequence for uniqueness.
UNWIND $rows AS row
MERGE (stu:StopTimeUpdate {
  parent_entity_id: row.parent_entity_id,
  stop_sequence: row.stop_sequence
})
SET   stu.stop_id               = row.stop_id,
      stu.arrival_delay         = row.arrival_delay,
      stu.arrival_time          = row.arrival_time,
      stu.departure_delay       = row.departure_delay,
      stu.departure_time        = row.departure_time,
      stu.schedule_relationship = row.schedule_relationship;

// ── :ServiceAlert ────────────────────────────────────────────────────────
// $rows: [{feed_entity_id, cause, effect, severity_level, header_text,
//          description_text, url, active_period_start, active_period_end, source}]
UNWIND $rows AS row
MERGE (sa:ServiceAlert {feed_entity_id: row.feed_entity_id})
SET   sa.cause                = row.cause,
      sa.effect               = row.effect,
      sa.severity_level       = row.severity_level,
      sa.header_text          = row.header_text,
      sa.description_text     = row.description_text,
      sa.url                  = row.url,
      sa.active_period_start  = row.active_period_start,
      sa.active_period_end    = row.active_period_end,
      sa.source               = row.source;

// ── :EntitySelector ──────────────────────────────────────────────────────
// $rows: [{selector_group_id, parent_entity_id, agency_id, route_id, stop_id, trip_id}]
UNWIND $rows AS row
MERGE (es:EntitySelector {selector_group_id: row.selector_group_id})
SET   es.parent_entity_id = row.parent_entity_id,
      es.agency_id        = row.agency_id,
      es.route_id         = row.route_id,
      es.stop_id          = row.stop_id,
      es.trip_id          = row.trip_id;

// ═══════════════════════════════════════════════════════════════════════════
// TIER 2: Normalized Interruption (one statement per multi-label variant)
// ═══════════════════════════════════════════════════════════════════════════

// ── :Interruption:Cancellation ───────────────────────────────────────────
// $rows: [{interruption_id, interruption_type, cause, effect, severity,
//          start_time, end_time, description}]
UNWIND $rows AS row
MERGE (i:Interruption:Cancellation {interruption_id: row.interruption_id})
SET   i.interruption_type = row.interruption_type,
      i.cause             = row.cause,
      i.effect            = row.effect,
      i.severity          = row.severity,
      i.start_time        = row.start_time,
      i.end_time          = row.end_time,
      i.description       = row.description;

// ── :Interruption:Delay ──────────────────────────────────────────────────
UNWIND $rows AS row
MERGE (i:Interruption:Delay {interruption_id: row.interruption_id})
SET   i.interruption_type = row.interruption_type,
      i.cause             = row.cause,
      i.effect            = row.effect,
      i.severity          = row.severity,
      i.start_time        = row.start_time,
      i.end_time          = row.end_time,
      i.description       = row.description;

// ── :Interruption:Skip ───────────────────────────────────────────────────
UNWIND $rows AS row
MERGE (i:Interruption:Skip {interruption_id: row.interruption_id})
SET   i.interruption_type = row.interruption_type,
      i.cause             = row.cause,
      i.effect            = row.effect,
      i.severity          = row.severity,
      i.start_time        = row.start_time,
      i.end_time          = row.end_time,
      i.description       = row.description;

// ── :Interruption:Detour ─────────────────────────────────────────────────
UNWIND $rows AS row
MERGE (i:Interruption:Detour {interruption_id: row.interruption_id})
SET   i.interruption_type = row.interruption_type,
      i.cause             = row.cause,
      i.effect            = row.effect,
      i.severity          = row.severity,
      i.start_time        = row.start_time,
      i.end_time          = row.end_time,
      i.description       = row.description;

// ── :Interruption:ServiceChange ──────────────────────────────────────────
UNWIND $rows AS row
MERGE (i:Interruption:ServiceChange {interruption_id: row.interruption_id})
SET   i.interruption_type = row.interruption_type,
      i.cause             = row.cause,
      i.effect            = row.effect,
      i.severity          = row.severity,
      i.start_time        = row.start_time,
      i.end_time          = row.end_time,
      i.description       = row.description;

// ── :Interruption:Accessibility ──────────────────────────────────────────
UNWIND $rows AS row
MERGE (i:Interruption:Accessibility {interruption_id: row.interruption_id})
SET   i.interruption_type = row.interruption_type,
      i.cause             = row.cause,
      i.effect            = row.effect,
      i.severity          = row.severity,
      i.start_time        = row.start_time,
      i.end_time          = row.end_time,
      i.description       = row.description;
