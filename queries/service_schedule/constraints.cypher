// queries/service_schedule/constraints.cypher
// Run once on database initialisation before any data is loaded.
// All service layer uniqueness constraints and indexes.

// ── Uniqueness constraints (also create backing index) ─────────────────────
// Note: FeedInfo constraint is managed by src/common/feed_info.py (shared)

CREATE CONSTRAINT agency_id IF NOT EXISTS
  FOR (n:Agency) REQUIRE n.agency_id IS UNIQUE;

CREATE CONSTRAINT route_id IF NOT EXISTS
  FOR (n:Route) REQUIRE n.route_id IS UNIQUE;

CREATE CONSTRAINT route_pattern_shape_id IF NOT EXISTS
  FOR (n:RoutePattern) REQUIRE n.shape_id IS UNIQUE;

CREATE CONSTRAINT trip_id IF NOT EXISTS
  FOR (n:Trip) REQUIRE n.trip_id IS UNIQUE;

CREATE CONSTRAINT service_pattern_id IF NOT EXISTS
  FOR (n:ServicePattern) REQUIRE n.service_id IS UNIQUE;

CREATE CONSTRAINT date_id IF NOT EXISTS
  FOR (n:Date) REQUIRE n.date IS UNIQUE;

// ── Additional indexes for common query patterns ───────────────────────────
CREATE INDEX route_mode IF NOT EXISTS
  FOR (n:Route) ON (n.mode);

CREATE INDEX trip_block_id IF NOT EXISTS
  FOR (n:Trip) ON (n.block_id);

CREATE INDEX trip_service_id IF NOT EXISTS
  FOR (n:Trip) ON (n.service_id);

CREATE INDEX date_day_of_week IF NOT EXISTS
  FOR (n:Date) ON (n.day_of_week);
