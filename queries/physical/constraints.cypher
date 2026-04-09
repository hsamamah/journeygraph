// queries/physical/constraints.cypher
// Run once on database initialisation before any data is loaded.
// All physical layer uniqueness constraints and indexes.
// ── Uniqueness constraints (also create backing index) ─────────────────────
// NOTE: Physical load uses the renamed column 'id' (originally stop_id from
// GTFS) as the node key, matching the current transform output. The fare
// layer's relationship Cypher references stop_id — reconcile in a future
// transform pass by removing the stop_id → id rename.
CREATE CONSTRAINT physical_station_id IF NOT EXISTS
FOR (n:Station)
REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT physical_bus_stop_id IF NOT EXISTS
FOR (n:BusStop)
REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT physical_entrance_id IF NOT EXISTS
FOR (n:StationEntrance)
REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT physical_platform_id IF NOT EXISTS
FOR (n:Platform)
REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT physical_faregate_id IF NOT EXISTS
FOR (n:FareGate)
REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT physical_pathway_id IF NOT EXISTS
FOR (n:Pathway)
REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT physical_level_id IF NOT EXISTS
FOR (n:Level)
REQUIRE n.level_id IS UNIQUE;

// ── Additional indexes for common query patterns ───────────────────────────

CREATE INDEX physical_pathway_mode IF NOT EXISTS
FOR (n:Pathway)
ON (n.mode);

CREATE INDEX physical_pathway_zone IF NOT EXISTS
FOR (n:Pathway)
ON (n.zone);

CREATE FULLTEXT INDEX physical_station_name IF NOT EXISTS
FOR (n:Station)
ON EACH [n.name];

CREATE FULLTEXT INDEX physical_route_name IF NOT EXISTS
FOR (n:Route)
ON EACH [n.route_short_name, n.route_long_name];

CREATE FULLTEXT INDEX physical_pathway_name IF NOT EXISTS
FOR (n:Pathway)
ON EACH [n.name];

CREATE FULLTEXT INDEX physical_pathway_stop_desc IF NOT EXISTS
FOR (n:Pathway)
ON EACH [n.from_stop_desc, n.to_stop_desc];

CREATE FULLTEXT INDEX physical_level_name IF NOT EXISTS
FOR (n:Level)
ON EACH [n.level_name];
