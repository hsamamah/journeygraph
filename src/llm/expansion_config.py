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
    max_results_per_hop: int = 500  # LIMIT applied per hop query — prevents runaway expansion on large graphs


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
            "BELONGS_TO",
            "CONNECTED_BY",
            "AFFECTS",
            "AFFECTS_STOP",
        ],
        include_labels=[
            "Station",
            "Pathway",
            "OutageEvent",
            "Interruption",
            "Date",
        ],
        provenance_rels=[
            "SOURCED_FROM",
        ],
    ),
    "delay_propagation": DomainExpansionConfig(
        max_hops=3,
        expand_rels=[
            "AFFECTS_STOP",
            "AFFECTS_TRIP",
            "AFFECTS_ROUTE",
            "SOURCED_FROM",
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
    ),
}
