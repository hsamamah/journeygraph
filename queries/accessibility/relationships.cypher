// queries/accessibility/relationships.cypher
// Relationship statements for the accessibility layer.
//
// Phase 3 of load.py calls _extract_statement() with the hint below once
// pathway_joiner.py is implemented (schema §5).

// ── OutageEvent -[:AFFECTS]-> Pathway ────────────────────────────────────────
//
// Links each OutageEvent snapshot to the specific :Pathway node representing
// the failed traversal segment (elevator or escalator unit).
//
// Station-level context is derived by graph traversal — no direct Station link:
//   (:OutageEvent)-[:AFFECTS]->(:Pathway)
//     -[:CONNECTS_TO]->(:PathwayNode)-[:BELONGS_TO]->(:Station)
//
// pathway_id is resolved by pathway_joiner.py using a two-tier approach:
//   Tier 1 — programmatic join via station_code, unit_name zone, unit_type,
//             and location_description keyword (covers standard stations)
//   Tier 2 — static lookup table for 4 complex interchange stations:
//             Metro Center, Gallery Place, L'Enfant Plaza, Fort Totten
//
// $rows: [{composite_key, pathway_id}]

UNWIND $rows AS row
MATCH (o:OutageEvent {composite_key: row.composite_key})
MATCH (p:Pathway {id: row.pathway_id})
MERGE (o)-[:AFFECTS]->(p)
