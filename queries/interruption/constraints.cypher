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

// ── Tier 2: Normalized Interruption ─────────────────────────────────────────
CREATE CONSTRAINT interruption_id IF NOT EXISTS
  FOR (n:Interruption) REQUIRE n.interruption_id IS UNIQUE;

// ── Indexes for common query patterns ───────────────────────────────────────
CREATE INDEX trip_update_trip_id IF NOT EXISTS
  FOR (n:TripUpdate) ON (n.trip_id);

CREATE INDEX trip_update_timestamp IF NOT EXISTS
  FOR (n:TripUpdate) ON (n.timestamp);

CREATE INDEX service_alert_effect IF NOT EXISTS
  FOR (n:ServiceAlert) ON (n.effect);

CREATE INDEX interruption_type IF NOT EXISTS
  FOR (n:Interruption) ON (n.interruption_type);

CREATE INDEX interruption_severity IF NOT EXISTS
  FOR (n:Interruption) ON (n.severity);
