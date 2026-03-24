"""
hop_expander.py — Stage 2 of the Subgraph Context Builder.

Receives AnchorResolutions from Stage 1 and a DomainExpansionConfig and
produces a RawSubgraph — the largest possible subgraph the domain config
permits, before any budget enforcement.

Two phases:
    1. Main expansion — bidirectional, sequential fixed-depth queries per hop.
       Each hop collects element IDs that seed the next hop. Relationship types
       and node labels are constrained per domain via DomainExpansionConfig.

    2. Provenance pass — fetched separately after main expansion completes.
       Provenance nodes are always fully collected here regardless of budget.
       Budget enforcement is the Context Serializer's concern, not ours.

Node identity: Neo4j elementId() is used internally for deduplication and
relationship tracking across hops. This avoids the multi-property-name
problem (Station.id, Route.route_id, Date.date). All original properties
are preserved on RawNode.props and passed downstream intact.

Output: RawSubgraph — consumed by the Context Serializer (Stage 3).
"""

from dataclasses import dataclass
import logging

from src.common.neo4j_tools import Neo4jManager
from src.llm.anchor_resolver import AnchorResolutions
from src.llm.expansion_config import EXPANSION_CONFIG, DomainExpansionConfig

log = logging.getLogger(__name__)


# ── Raw subgraph types ────────────────────────────────────────────────────────


@dataclass
class RawNode:
    element_id: str  # Neo4j internal elementId — used for dedup and rel tracking
    labels: list[str]
    props: dict  # all node properties, including domain-specific id property
    hop_distance: int  # hops from nearest anchor; 0 for anchor nodes themselves


@dataclass
class RawRel:
    rel_type: str
    from_element_id: str
    to_element_id: str
    props: dict


@dataclass
class RawSubgraph:
    nodes: list[RawNode]
    rels: list[RawRel]
    provenance_nodes: list[dict]  # raw TripUpdate / ServiceAlert properties —
    # always fully populated, never trimmed here
    anchor_element_ids: set[str]  # element IDs of anchor nodes — never trimmed
    domain: str
    node_count: int  # len(nodes) before any trimming — carried to SubgraphOutput


# ── HopExpander ───────────────────────────────────────────────────────────────


class HopExpander:
    """
    Expands the resolved anchors into a full raw subgraph.

    Args:
        db: Neo4jManager instance. Injected at construction — caller owns
            the connection lifecycle.
    """

    def __init__(self, db: Neo4jManager) -> None:
        self.db = db

    # ── Public ────────────────────────────────────────────────────────────────

    def expand(
        self,
        resolutions: AnchorResolutions,
        domain: str,
    ) -> RawSubgraph:
        """
        Entry point. Runs main expansion then provenance pass.

        Args:
            resolutions: AnchorResolutions from Stage 1.
            domain:      Domain key — used to look up DomainExpansionConfig.

        Returns:
            RawSubgraph with all nodes, relationships, and provenance collected.
        """
        config = EXPANSION_CONFIG[domain]

        # ── Phase 1: seed anchor nodes ────────────────────────────────────────
        nodes: dict[str, RawNode] = {}  # element_id → RawNode
        rels: dict[str, RawRel] = {}  # rel key → RawRel

        anchor_element_ids: set[str] = set()

        self._seed_anchors(resolutions, nodes, anchor_element_ids)

        if not nodes:
            log.warning("hop_expander | no anchor nodes seeded | domain=%s", domain)
            return RawSubgraph(
                nodes=[],
                rels=[],
                provenance_nodes=[],
                anchor_element_ids=set(),
                domain=domain,
                node_count=0,
            )

        log.info(
            "hop_expander | seeded %d anchor nodes | domain=%s",
            len(nodes),
            domain,
        )

        # ── Phase 2: hop expansion ────────────────────────────────────────────
        frontier: set[str] = set(nodes.keys())  # element IDs to expand from

        for hop in range(1, config.max_hops + 1):
            new_frontier: set[str] = set()

            rows = self.db.query(
                """
                MATCH (seed)-[r]-(neighbor)
                WHERE elementId(seed) IN $frontier
                  AND type(r) IN $expand_rels
                  AND any(lbl IN labels(neighbor) WHERE lbl IN $include_labels)
                RETURN
                    elementId(neighbor)         AS neighbor_eid,
                    labels(neighbor)            AS neighbor_labels,
                    properties(neighbor)        AS neighbor_props,
                    type(r)                     AS rel_type,
                    elementId(startNode(r))     AS from_eid,
                    elementId(endNode(r))       AS to_eid,
                    properties(r)               AS rel_props
                """,
                {
                    "frontier": list(frontier),
                    "expand_rels": config.expand_rels,
                    "include_labels": config.include_labels,
                },
            )

            for row in rows:
                n_eid = row["neighbor_eid"]

                # Register new node if not yet visited
                if n_eid not in nodes:
                    nodes[n_eid] = RawNode(
                        element_id=n_eid,
                        labels=row["neighbor_labels"],
                        props=row["neighbor_props"],
                        hop_distance=hop,
                    )
                    new_frontier.add(n_eid)

                # Register relationship (deduplicated by directed key)
                rel_key = f"{row['from_eid']}_{row['rel_type']}_{row['to_eid']}"
                if rel_key not in rels:
                    rels[rel_key] = RawRel(
                        rel_type=row["rel_type"],
                        from_element_id=row["from_eid"],
                        to_element_id=row["to_eid"],
                        props=row["rel_props"],
                    )

            log.info(
                "hop_expander | hop %d complete | new_nodes=%d total_nodes=%d | domain=%s",
                hop,
                len(new_frontier),
                len(nodes),
                domain,
            )

            # Stop early if frontier is exhausted before max_hops
            if not new_frontier:
                log.info(
                    "hop_expander | frontier exhausted at hop %d | domain=%s",
                    hop,
                    domain,
                )
                break

            frontier = new_frontier

        # ── Phase 3: provenance pass ──────────────────────────────────────────
        provenance_nodes = self._fetch_provenance(
            element_ids=set(nodes.keys()),
            config=config,
            domain=domain,
        )

        node_list = list(nodes.values())
        node_count = len(node_list)

        log.info(
            "hop_expander | expansion complete | nodes=%d rels=%d provenance=%d | domain=%s",
            node_count,
            len(rels),
            len(provenance_nodes),
            domain,
        )

        return RawSubgraph(
            nodes=node_list,
            rels=list(rels.values()),
            provenance_nodes=provenance_nodes,
            anchor_element_ids=anchor_element_ids,
            domain=domain,
            node_count=node_count,
        )

    # ── Seed queries ──────────────────────────────────────────────────────────

    def _seed_anchors(
        self,
        resolutions: AnchorResolutions,
        nodes: dict[str, RawNode],
        anchor_element_ids: set[str],
    ) -> None:
        """
        Loads anchor nodes from the graph using type-specific seed queries.
        Each anchor type uses its own property name as the lookup key.
        Populates nodes and anchor_element_ids in place.
        """
        seed_specs: list[tuple[str, list[str]]] = [
            # (Cypher MATCH clause, list of resolved IDs)
            (
                "MATCH (n:Station) WHERE n.id IN $ids",
                list(resolutions.resolved_stations.values()),
            ),
            (
                "MATCH (n:Route) WHERE n.route_id IN $ids",
                list(resolutions.resolved_routes.values()),
            ),
            (
                "MATCH (n:Date) WHERE n.date IN $ids",
                list(resolutions.resolved_dates.values()),
            ),
            (
                "MATCH (n:Pathway) WHERE n.id IN $ids",
                list(resolutions.resolved_pathway_nodes.values()),
            ),
        ]

        for match_clause, ids in seed_specs:
            if not ids:
                continue

            rows = self.db.query(
                f"""
                {match_clause}
                RETURN elementId(n)  AS element_id,
                       labels(n)     AS labels,
                       properties(n) AS props
                """,
                {"ids": ids},
            )

            for row in rows:
                eid = row["element_id"]
                nodes[eid] = RawNode(
                    element_id=eid,
                    labels=row["labels"],
                    props=row["props"],
                    hop_distance=0,
                )
                anchor_element_ids.add(eid)

    # ── Provenance pass ───────────────────────────────────────────────────────

    def _fetch_provenance(
        self,
        element_ids: set[str],
        config: DomainExpansionConfig,
        domain: str,
    ) -> list[dict]:
        """
        Fetches provenance nodes (TripUpdate, ServiceAlert, StopTimeUpdate)
        reachable from any node in the expanded subgraph via provenance_rels.

        Returns raw properties only — provenance nodes are not added to the
        main node set. They are carried separately on RawSubgraph.provenance_nodes
        and are always fully populated regardless of context budget.
        """
        if not config.provenance_rels or not element_ids:
            return []

        rows = self.db.query(
            """
            MATCH (n)-[r]-(p)
            WHERE elementId(n) IN $element_ids
              AND type(r) IN $provenance_rels
            RETURN DISTINCT
                labels(p)      AS labels,
                properties(p)  AS props,
                type(r)        AS rel_type
            """,
            {
                "element_ids": list(element_ids),
                "provenance_rels": config.provenance_rels,
            },
        )

        provenance: list[dict] = []
        for row in rows:
            provenance.append(
                {
                    "labels": row["labels"],
                    "props": row["props"],
                    "rel_type": row["rel_type"],
                }
            )

        log.info(
            "hop_expander | provenance pass | found=%d | domain=%s",
            len(provenance),
            domain,
        )

        return provenance
