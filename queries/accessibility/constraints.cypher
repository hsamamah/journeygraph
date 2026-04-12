// queries/accessibility/constraints.cypher

// Run once on database initialisation before any data is loaded.

// All accessibility layer uniqueness constraints and indexes.

// ── Uniqueness constraints ─────────────────────────────────────────────────

// Identity key for an OutageEvent snapshot is the composite of unit_name +
// date_out_of_service + date_updated. Neo4j does not support native composite
// node keys via a single constraint, so a synthetic composite_key property
// (unit_name + '|' + date_out_of_service + '|' + date_updated, set at
// ingestion) is used as the uniqueness anchor. A new node is created whenever
// date_updated changes, preserving full snapshot history.
// NOTE: last_seen_poll is the only mutable property on an OutageEvent node
// and is updated in-place — it does not affect snapshot identity.

CREATE CONSTRAINT accessibility_outage_event_key IF NOT EXISTS
FOR (n:OutageEvent)
REQUIRE n.composite_key IS UNIQUE;

// ── Lookup indexes for high-frequency query patterns ──────────────────────

// Active outage monitoring — status is the primary filter in all operational
// queries. Covering index with unit_type supports queries that filter by
// equipment type (e.g. unit_type = 'ESCALATOR') without a separate scan.

CREATE INDEX accessibility_outage_status IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.status);

CREATE INDEX accessibility_outage_status_unit_type IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.status, n.unit_type);

// Per-unit history lookups — unit_name is the primary filter when querying
// the full snapshot history of a specific physical unit.

CREATE INDEX accessibility_outage_unit_name IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.unit_name);

// Composite identity lookup — used by the ingestion poller to locate the
// most recent active snapshot for a given physical outage instance during
// change detection and resolution (outage lifecycle).

CREATE INDEX accessibility_outage_identity IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.unit_name, n.date_out_of_service);

// Severity index — supports ORDER BY o.severity DESC in operational queries
// and worst_severity aggregation in analytical queries.

CREATE INDEX accessibility_outage_severity IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.severity);

// Polling window index — last_seen_poll is used during ingestion to detect
// stale active nodes that have silently dropped from the API response across
// consecutive poll cycles and should be marked RESOLVED.

CREATE INDEX accessibility_outage_last_seen_poll IF NOT EXISTS
FOR (n:OutageEvent)
ON (n.last_seen_poll);
