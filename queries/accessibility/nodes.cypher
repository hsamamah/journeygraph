// queries/accessibility/nodes.cypher
// Parameterised MERGE statement for OutageEvent nodes.
// Called by accessibility/load.py via _load_query() + _extract_statement().
//
// composite_key = unit_name + '|' + date_out_of_service + '|' + date_updated.
// A new node is created whenever date_updated changes, preserving full snapshot
// history. last_seen_poll is the only in-place mutation on an existing node.
//
// resolved_at and actual_duration_days are null on CREATE and set by the
// stale-resolution pass in load.py Phase 4 when the unit leaves the API response.

// ── :OutageEvent ─────────────────────────────────────────────────────────────
// $rows: [{composite_key, unit_name, unit_type, station_code,
//          location_description, symptom_description,
//          date_out_of_service, date_updated, estimated_return,
//          severity, projected_duration_days,
//          status, poll_timestamp}]

UNWIND $rows AS row
MERGE (o:OutageEvent {composite_key: row.composite_key})
ON CREATE SET
  o.unit_name               = row.unit_name,
  o.unit_type               = row.unit_type,
  o.station_code            = row.station_code,
  o.location_description    = row.location_description,
  o.symptom_description     = row.symptom_description,
  o.date_out_of_service     = row.date_out_of_service,
  o.date_updated            = row.date_updated,
  o.estimated_return        = row.estimated_return,
  o.severity                = row.severity,
  o.projected_duration_days = row.projected_duration_days,
  o.status                  = row.status,
  o.first_seen_poll         = row.poll_timestamp,
  o.last_seen_poll          = row.poll_timestamp,
  o.resolved_at             = null,
  o.actual_duration_days    = null
ON MATCH SET
  o.last_seen_poll          = row.poll_timestamp
