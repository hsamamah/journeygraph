// queries/service_schedule/nodes.cypher
// Parameterised MERGE statements for all service layer nodes.
// Called by service_schedule/load.py via _extract_statement().
// Each statement expects a list of dicts passed as $rows.

// Note: FeedInfo node is managed by src/common/feed_info.py (shared utility).
// All layers call ensure_feed_info() at load start — not layer-specific.

// ── :Agency ──────────────────────────────────────────────────────────────────
// $rows: [{agency_id, agency_name, agency_url, agency_timezone, agency_lang,
//          agency_phone, agency_fare_url, agency_email}]
UNWIND $rows AS row
MERGE (a:Agency {agency_id: row.agency_id})
SET   a.agency_name     = row.agency_name,
      a.agency_url      = row.agency_url,
      a.agency_timezone = row.agency_timezone,
      a.agency_lang     = row.agency_lang,
      a.agency_phone    = row.agency_phone,
      a.agency_fare_url = row.agency_fare_url,
      a.agency_email    = row.agency_email;

// ── :Route:Bus ───────────────────────────────────────────────────────────────
// $rows: [{route_id, mode, route_short_name, route_long_name, route_color,
//          route_text_color, route_type, route_desc, route_url, route_sort_order}]
UNWIND $rows AS row
MERGE (r:Route:Bus {route_id: row.route_id})
SET   r.mode             = row.mode,
      r.route_short_name = row.route_short_name,
      r.route_long_name  = row.route_long_name,
      r.route_color      = row.route_color,
      r.route_text_color = row.route_text_color,
      r.route_type       = row.route_type,
      r.route_desc       = row.route_desc,
      r.route_url        = row.route_url,
      r.route_sort_order = row.route_sort_order;

// ── :Route:Rail ──────────────────────────────────────────────────────────────
// $rows: same as Route:Bus
UNWIND $rows AS row
MERGE (r:Route:Rail {route_id: row.route_id})
SET   r.mode             = row.mode,
      r.route_short_name = row.route_short_name,
      r.route_long_name  = row.route_long_name,
      r.route_color      = row.route_color,
      r.route_text_color = row.route_text_color,
      r.route_type       = row.route_type,
      r.route_desc       = row.route_desc,
      r.route_url        = row.route_url,
      r.route_sort_order = row.route_sort_order;

// ── :RoutePattern ────────────────────────────────────────────────────────────
// $rows: [{shape_id, headsign, direction_id}]
UNWIND $rows AS row
MERGE (rp:RoutePattern {shape_id: row.shape_id})
SET   rp.headsign     = row.headsign,
      rp.direction_id = row.direction_id;

// ── :Trip ────────────────────────────────────────────────────────────────────
// $rows: [{trip_id, direction_id, trip_headsign, trip_short_name, block_id}]
UNWIND $rows AS row
MERGE (t:Trip {trip_id: row.trip_id})
SET   t.direction_id   = row.direction_id,
      t.trip_headsign  = row.trip_headsign,
      t.trip_short_name = row.trip_short_name,
      t.block_id       = row.block_id;

// ── :ServicePattern:Weekday ──────────────────────────────────────────────────
// $rows: [{service_id}]
UNWIND $rows AS row
MERGE (sp:ServicePattern:Weekday {service_id: row.service_id});

// ── :ServicePattern:Saturday ─────────────────────────────────────────────────
UNWIND $rows AS row
MERGE (sp:ServicePattern:Saturday {service_id: row.service_id});

// ── :ServicePattern:Sunday ───────────────────────────────────────────────────
UNWIND $rows AS row
MERGE (sp:ServicePattern:Sunday {service_id: row.service_id});

// ── :ServicePattern:Holiday ──────────────────────────────────────────────────
UNWIND $rows AS row
MERGE (sp:ServicePattern:Holiday {service_id: row.service_id});

// ── :ServicePattern:Maintenance ──────────────────────────────────────────────
UNWIND $rows AS row
MERGE (sp:ServicePattern:Maintenance {service_id: row.service_id});

// ── :Date ────────────────────────────────────────────────────────────────────
// $rows: [{date, day_of_week}]
UNWIND $rows AS row
MERGE (d:Date {date: row.date})
SET   d.day_of_week = row.day_of_week;
