// queries/accessibility/constraints.cypher

// Run once on database initialisation before any data is loaded.

// All accessibility layer uniqueness constraints and indexes.

// ── Uniqueness constraints ─────────────────────────────────────────────────

// Identity key for an OutageEvent snapshot is the composite of unit_name +
// date_out_of_service. Neo4j does not support native composite node keys via
// a single constraint, so a synthetic composite_key property (unit_name +
// '|' + date_out_of_service.epochMillis, set at ingestion) is used as the
// uniqueness anchor. This ensures deduplication across polling cycles while
// allowing multiple snapshots per physical outage instance when date_updated
// changes between polls.
// NOTE: last_seen_poll is the only mutable property on an OutageEvent node
// and is updated in-place — it does not affect snapshot identity.

CREATE CONSTRAINT accessibility_outage_event_key IF NOT EXISTS
FOR (n:OutageEvent)
REQUIRE n.composite_key IS UNIQUE;

// ── Lookup indexes for high-frequency query patterns ──────────────────────

// Active outage monitoring — status is the primary filter in all operational
// queries (Section 7.1). Covering index with unit_type supports the elevator-
// specific analytical query (Section 7.2) without a separate scan.

CREATE INDEX accessibility_outage_status IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.status);

CREATE INDEX accessibility_outage_status_unit_type IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.status, n.unit_type);

// Per-unit history lookups (Section 7.3) — unit_name is the primary filter
// when querying the full snapshot history of a specific physical unit.

CREATE INDEX accessibility_outage_unit_name IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.unit_name);

// Composite identity lookup — used by the ingestion poller to locate the
// most recent active snapshot for a given physical outage instance during
// change detection and resolution (Section 6.2 outage lifecycle).

CREATE INDEX accessibility_outage_identity IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.unit_name, n.date_out_of_service);

// Severity index — supports ORDER BY o.severity DESC in operational queries
// (Section 7.1) and worst_severity aggregation in analytical queries (7.2).

CREATE INDEX accessibility_outage_severity IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.severity);

// Polling window index — last_seen_poll is used during ingestion to detect
// stale active nodes that have silently dropped from the API response across
// consecutive poll cycles and should be marked RESOLVED.

CREATE INDEX accessibility_outage_last_seen_poll IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.last_seen_poll);
