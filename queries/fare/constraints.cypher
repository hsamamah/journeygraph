// queries/fare/constraints.cypher
// Run once on database initialisation before any data is loaded.
// All fare layer uniqueness constraints and indexes.

// ── Uniqueness constraints (also create backing index) ─────────────────────
CREATE CONSTRAINT fare_zone_id          IF NOT EXISTS
  FOR (n:FareZone)       REQUIRE n.zone_id          IS UNIQUE;

CREATE CONSTRAINT fare_media_id         IF NOT EXISTS
  FOR (n:FareMedia)      REQUIRE n.fare_media_id    IS UNIQUE;

CREATE CONSTRAINT fare_product_id       IF NOT EXISTS
  FOR (n:FareProduct)    REQUIRE n.fare_product_id  IS UNIQUE;

CREATE CONSTRAINT fare_leg_rule_id      IF NOT EXISTS
  FOR (n:FareLegRule)    REQUIRE n.rule_id          IS UNIQUE;

CREATE CONSTRAINT fare_transfer_rule_id IF NOT EXISTS
  FOR (n:FareTransferRule) REQUIRE n.rule_id        IS UNIQUE;

// ── Property existence constraints ────────────────────────────────────────
CREATE CONSTRAINT fare_zone_id_exists IF NOT EXISTS
  FOR (n:FareZone) REQUIRE n.zone_id IS NOT NULL;

CREATE CONSTRAINT fare_gate_zone_exists IF NOT EXISTS
  FOR (n:FareGate) REQUIRE n.zone_id IS NOT NULL;

// ── Additional indexes for common query patterns ───────────────────────────
CREATE INDEX fare_leg_rule_network IF NOT EXISTS
  FOR (n:FareLegRule) ON (n.network_id);

CREATE INDEX fare_gate_type IF NOT EXISTS
  FOR (n:FareGate) ON (n.gate_type);
