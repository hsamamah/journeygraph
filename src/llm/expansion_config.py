from dataclasses import dataclass


@dataclass
class DomainExpansionConfig:
    max_hops: int
    expand_rels: list[
        str
    ]  # relationship types followed bidirectionally during hop expansion
    include_labels: list[str]  # node labels retained in the expanded subgraph
    provenance_rels: list[
        str
    ]  # fetched in a separate pass post-expansion, always fully populated
    max_results_per_hop: int = (
        500  # LIMIT applied per hop query — prevents runaway expansion on large graphs
    )


EXPANSION_CONFIG: dict[str, DomainExpansionConfig] = {
    "transfer_impact": DomainExpansionConfig(
        max_hops=4,
        expand_rels=[
            "AFFECTS_STOP",
            "AFFECTS_TRIP",
            "SCHEDULED_AT",
        ],
        include_labels=[
            "Interruption",
            "Trip",
            "Platform",
            "Station",
            "Route",
            "Date",
        ],
        provenance_rels=[
            "SOURCED_FROM",
        ],
    ),
    "accessibility": DomainExpansionConfig(
        max_hops=3,
        expand_rels=[
            "CONTAINS",
            "AFFECTS",
            "AFFECTS_STOP",
            "ON_LEVEL",
            "STARTING_LEVEL",
            "ENDING_LEVEL",
        ],
        include_labels=[
            "Station",
            "Pathway",
            "OutageEvent",
            "Interruption",
            "Date",
            "Level",
        ],
        provenance_rels=[
            "SOURCED_FROM",
        ],
    ),
    "delay_propagation": DomainExpansionConfig(
        max_hops=3,
        expand_rels=[
            # SOURCED_FROM removed from expand_rels — it is already in
            # provenance_rels, so including it here caused TripUpdate /
            # StopTimeUpdate nodes to be pulled in twice (once via hop
            # expansion, once via provenance pass), exploding the subgraph.
            "AFFECTS_STOP",
            "AFFECTS_TRIP",
            "AFFECTS_ROUTE",
            "HAS_STOP_UPDATE",
            "AT_STOP",
        ],
        include_labels=[
            "Interruption",
            "TripUpdate",
            "StopTimeUpdate",
            "Trip",
            "Route",
            "Platform",
            "BusStop",
            "Date",
        ],
        provenance_rels=[
            "SOURCED_FROM",
            "HAS_STOP_UPDATE",
        ],
        max_results_per_hop=100,  # reduced from 500 — delay graphs are dense
    ),
}
