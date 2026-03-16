// queries/interruption/constraints.cypher
// All interruption layer uniqueness constraints and indexes.
// Note: FeedInfo constraint is managed by src/common/feed_info.py (shared).

// ── Tier 1: Raw source nodes ────────────────────────────────────────────────
CREATE CONSTRAINT trip_update_hash IF NOT EXISTS
  FOR (n:TripUpdate) REQUIRE n.dedup_hash IS UNIQUE;

CREATE CONSTRAINT service_alert_id IF NOT EXISTS
  FOR (n:ServiceAlert) REQUIRE n.feed_entity_id IS UNIQUE;

CREATE CONSTRAINT entity_selector_id IF NOT EXISTS
  FOR (n:EntitySelector) REQUIRE n.selector_group_id IS UNIQUE;

// StopTimeUpdate — composite NODE KEY backs the (parent_entity_id, stop_sequence)
// MERGE in nodes.cypher and the HAS_STOP_UPDATE MATCH in relationships.cypher.
// Without this, every MERGE/MATCH on StopTimeUpdate is a full label scan
// across all 26K+ nodes per poll.
CREATE CONSTRAINT stop_time_update_key IF NOT EXISTS
  FOR (n:StopTimeUpdate) REQUIRE (n.parent_entity_id, n.stop_sequence) IS NODE KEY;

// ── Tier 2: Normalized Interruption ─────────────────────────────────────────
CREATE CONSTRAINT interruption_id IF NOT EXISTS
  FOR (n:Interruption) REQUIRE n.interruption_id IS UNIQUE;

// ── Indexes for common query patterns ───────────────────────────────────────
CREATE INDEX trip_update_trip_id IF NOT EXISTS
  FOR (n:TripUpdate) ON (n.trip_id);

CREATE INDEX trip_update_timestamp IF NOT EXISTS
  FOR (n:TripUpdate) ON (n.timestamp);

// TripUpdate.feed_entity_id — needed for SOURCED_FROM MATCH in relationships.cypher.
// The uniqueness constraint is on dedup_hash (not feed_entity_id) so
// without this index the SOURCED_FROM MATCH does a full TripUpdate scan.
CREATE INDEX trip_update_feed_entity_id IF NOT EXISTS
  FOR (n:TripUpdate) ON (n.feed_entity_id);

CREATE INDEX service_alert_effect IF NOT EXISTS
  FOR (n:ServiceAlert) ON (n.effect);

CREATE INDEX interruption_type IF NOT EXISTS
  FOR (n:Interruption) ON (n.interruption_type);

CREATE INDEX interruption_severity IF NOT EXISTS
  FOR (n:Interruption) ON (n.severity);
