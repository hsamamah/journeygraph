// demos/queries/06_service_alerts.cypher
// ═══════════════════════════════════════════════════════════════════════════
// Service alert layer — ServiceAlert nodes linked to EntitySelectors and Routes.
// Returns nodes + relationships so Neo4j Browser renders a graph.
//
// Q1  All rail service alerts — ServiceAlert → EntitySelector → Route:Rail.
// Q2  One alert cross-linked to its Skip interruptions on the same route —
//     shows the connection between the real-time text alert and measured skips.
// ═══════════════════════════════════════════════════════════════════════════


// ── Q1: All active rail service alerts ───────────────────────────────────
// Five rail alerts exist in the current dataset.  Each flows through an
// EntitySelector node that names the affected route.
//   ServiceAlert -[:HAS_SELECTOR]-> EntitySelector -[:TARGETS_ROUTE]-> Route
MATCH path_sel  = (sa:ServiceAlert)-[:HAS_SELECTOR]->(es:EntitySelector)
MATCH path_rt   = (es)-[:TARGETS_ROUTE]->(r:Route:Rail)
RETURN sa, es, r, path_sel, path_rt;


// ── Q2: Blue Line alert + its Skip interruptions ─────────────────────────
// The Blue Line has a REDUCED_SERVICE alert and 91 skip events on 2026-04-13.
// This query stitches both layers together through the shared Route node,
// showing that the text alert corresponds to a real measurable disruption.
// Limit Skip nodes to 10 to keep the graph readable.
MATCH path_alert = (sa:ServiceAlert)-[:HAS_SELECTOR]->(es:EntitySelector)
                   -[:TARGETS_ROUTE]->(r:Route {route_id: 'BLUE'})
MATCH path_skip  = (i:Interruption:Skip)-[:AFFECTS_ROUTE]->(r)
MATCH path_date  = (i)-[:ON_DATE]->(d:Date {date: '20260413'})
RETURN sa, es, r, i, d,
       path_alert, path_skip, path_date LIMIT 12;
