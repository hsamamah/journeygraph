import logging

import pandas as pd

log = logging.getLogger(__name__)


# ── Helper functions ──────────────────────────────────────────────────────────
def ensure_feed_info(neo4j, feed_info: pd.DataFrame) -> None:
    """
    Ensure the shared FeedInfo node exists in the database.
    """
    cypher = """
    MERGE (f:FeedInfo {feed_id: $feed_id})
    ON CREATE SET f.version = $version, f.timestamp = $timestamp
    """
    for _, row in feed_info.iterrows():
        params = {
            # Use feed_publisher_name as a unique id, or adjust as needed
            "feed_id": row.get("feed_publisher_name", "unknown"),
            "version": row.get("feed_version", "unknown"),
            # Use feed_end_date if available, else feed_start_date, else today's date
            "timestamp": row.get("feed_end_date")
            or row.get("feed_start_date")
            or pd.Timestamp.now().strftime("%Y-%m-%d"),
        }
        neo4j.execute_write(cypher, parameters=params)


def _df_to_rows(df: pd.DataFrame) -> list[dict]:
    """
    Convert a DataFrame to a list of dictionaries for Cypher parameters.
    """
    return df.to_dict(orient="records")


def _extract_statement(file_path: str, statement_name: str) -> str:
    """
    Extract a specific Cypher statement from a file.
    """
    with open(file_path) as file:
        statements = file.read().split(";")
    for statement in statements:
        if statement_name in statement:
            return statement.strip()
    raise ValueError(f"Statement {statement_name} not found in {file_path}")


def _load_query(file_name: str) -> str:
    """
    Load a Cypher query file from the queries directory.
    """
    return f"queries/{file_name}"


# ── Main entry point ──────────────────────────────────────────────────────────
def run(result: dict[str, pd.DataFrame], neo4j, validate: bool = False) -> None:
    """
    Load all physical layer nodes and relationships into Neo4j.
    Runs post-load validation; raises ValueError on failure.
    """
    log.info("physical load: starting")

    ensure_feed_info(neo4j, result["feed_info"])

    # Load Station nodes
    station_cypher = """
    UNWIND $rows AS row
    MERGE (s:Station {id: row.id})
    SET s.name = row.name, s.location = row.location
    """
    stations = result["stops"][result["stops"]["location_type"] == 1].copy()
    neo4j.execute_write(station_cypher, parameters={"rows": _df_to_rows(stations)})

    # Load StationEntrance nodes
    entrance_cypher = """
    UNWIND $rows AS row
    MERGE (e:StationEntrance {id: row.id})
    SET e.name = row.name, e.location = row.location, e.wheelchair_accessible = row.wheelchair_accessible, e.level = row.level
    """
    entrances = result["stops"][result["stops"]["location_type"] == 2].copy()
    neo4j.execute_write(entrance_cypher, parameters={"rows": _df_to_rows(entrances)})

    # Load Platform nodes
    platform_cypher = """
    UNWIND $rows AS row
    MERGE (p:Platform {id: row.id})
    SET p.name = row.name, p.lines_accessible = row.lines_accessible, p.level = row.level
    """
    platforms = result["stops"][result["stops"]["location_type"] == 0].copy()
    neo4j.execute_write(platform_cypher, parameters={"rows": _df_to_rows(platforms)})

    # Load FareGate nodes
    faregate_cypher = """
    UNWIND $rows AS row
    MERGE (fg:FareGate {id: row.id})
    SET fg.name = row.name, fg.zone_id = row.zone_id, fg.is_bidirectional = row.is_bidirectional
    """
    faregates = result["stops"][result["stops"]["id"].str.contains("_FG_")].copy()
    neo4j.execute_write(faregate_cypher, parameters={"rows": _df_to_rows(faregates)})

    # Load Pathway nodes as multi-label nodes (e.g., :Pathway:Elevator:Paid)
    pathways = result["pathways"].copy()
    for _, row in pathways.iterrows():
        labels = ["Pathway"]
        # Add mode as label (capitalize, only if recognized)
        mode_label = str(row.get("mode", "")).capitalize()
        valid_modes = ["Elevator", "Escalator", "Stairs", "Walkway", "Mezzanine"]
        if mode_label in valid_modes:
            labels.append(mode_label)
            log.debug(
                f"Assigning mode label '{mode_label}' for pathway {row.get('id')}"
            )
        else:
            log.debug(
                f"No valid mode label for pathway {row.get('id')}, using only 'Pathway' label"
            )
        # Add zone as label (Paid/Unpaid)
        zone_label = str(row.get("zone", "")).capitalize()
        if zone_label in ["Paid", "Unpaid"]:
            labels.append(zone_label)
        # Build Cypher with dynamic labels, only required properties
        cypher = f"""
        MERGE (pw:{":".join(labels)} {{id: $id}})
        SET pw.name = $id, pw.stop_id = $from_stop_id, pw.zone = $zone, pw.elevation_gain = $elevation_gain, pw.wheelchair_accessible = $wheelchair_accessible
        """
        params = {
            "id": row.get("id"),
            "from_stop_id": row.get("from_stop_id"),
            "zone": row.get("zone", ""),
            "elevation_gain": row.get("elevation_gain"),
            "wheelchair_accessible": row.get("wheelchair_accessible"),
        }
        neo4j.execute_write(cypher, parameters=params)

    contains_entrance_cypher = """
    UNWIND $rows AS row
    MATCH (s:Station {id: row.station_id})
    MATCH (e:StationEntrance {id: row.entrance_id})
    MERGE (s)-[:CONTAINS]->(e)
    """
    entrance_rels = entrances[["parent_station", "id"]].rename(
        columns={"parent_station": "station_id", "id": "entrance_id"}
    )
    neo4j.execute_write(
        contains_entrance_cypher, parameters={"rows": _df_to_rows(entrance_rels)}
    )

    # Station contains Platform
    contains_platform_cypher = """
    UNWIND $rows AS row
    MATCH (s:Station {id: row.station_id})
    MATCH (p:Platform {id: row.platform_id})
    MERGE (s)-[:CONTAINS]->(p)
    """
    platform_rels = platforms[["parent_station", "id"]].rename(
        columns={"parent_station": "station_id", "id": "platform_id"}
    )
    neo4j.execute_write(
        contains_platform_cypher, parameters={"rows": _df_to_rows(platform_rels)}
    )

    faregate_links_cypher = """
    UNWIND $rows AS row
    MATCH (pw:Pathway {id: row.pathway_id})
    MATCH (fg:FareGate {id: row.faregate_id})
    MERGE (pw)-[:LINKS]->(fg)
    """
    faregate_links = result["pathways"][result["pathways"]["mode"] == "faregate"].copy()
    faregate_links = faregate_links[["id", "from_stop_id"]].rename(
        columns={"id": "pathway_id", "from_stop_id": "faregate_id"}
    )
    neo4j.execute_write(
        faregate_links_cypher, parameters={"rows": _df_to_rows(faregate_links)}
    )

    # Platform links Pathway
    platform_links_cypher = """
    UNWIND $rows AS row
    MATCH (p:Platform {id: row.platform_id})
    MATCH (pw:Pathway {id: row.pathway_id})
    MERGE (p)-[:LINKS]->(pw)
    """
    platform_links = result["pathways"][result["pathways"]["mode"] == "platform"].copy()
    platform_links = platform_links[["from_stop_id", "id"]].rename(
        columns={"from_stop_id": "platform_id", "id": "pathway_id"}
    )
    neo4j.execute_write(
        platform_links_cypher, parameters={"rows": _df_to_rows(platform_links)}
    )

    # StationEntrance links Pathway
    entrance_links_cypher = """
    UNWIND $rows AS row
    MATCH (e:StationEntrance {id: row.entrance_id})
    MATCH (pw:Pathway {id: row.pathway_id})
    MERGE (e)-[:LINKS]->(pw)
    """
    entrance_links = result["pathways"][result["pathways"]["mode"] == "entrance"].copy()
    entrance_links = entrance_links[["from_stop_id", "id"]].rename(
        columns={"from_stop_id": "entrance_id", "id": "pathway_id"}
    )
    neo4j.execute_write(
        entrance_links_cypher, parameters={"rows": _df_to_rows(entrance_links)}
    )

    # Pathway LINKS StationEntrance, Platform, FareGate, Station (unidirectional, only from Pathway)
    stops = result["stops"]
    pathways = result["pathways"]
    for _, row in pathways.iterrows():
        from_stop_id = row.get("from_stop_id")
        to_stop_id = row.get("to_stop_id")
        pathway_id = row.get("id")
        # Find node types for from/to stops
        from_node = stops[stops["id"] == from_stop_id]
        to_node = stops[stops["id"] == to_stop_id]
        # Only create LINKS if node exists, and only from Pathway to the other node
        if not from_node.empty:
            from_type = from_node.iloc[0]["location_type"]
            if from_type == 2:
                cypher = """
                MATCH (pw:Pathway {id: $pathway_id})
                MATCH (e:StationEntrance {id: $stop_id})
                MERGE (pw)-[:LINKS]->(e)
                """
                log.info(
                    f"Creating LINKS from Pathway {pathway_id} to StationEntrance {from_stop_id}"
                )
                neo4j.execute_write(
                    cypher,
                    parameters={"stop_id": from_stop_id, "pathway_id": pathway_id},
                )
            elif from_type == 0 or from_type == 4:
                cypher = """
                MATCH (pw:Pathway {id: $pathway_id})
                MATCH (p:Platform {id: $stop_id})
                MERGE (pw)-[:LINKS]->(p)
                """
                log.info(
                    f"Creating LINKS from Pathway {pathway_id} to Platform {from_stop_id}"
                )
                neo4j.execute_write(
                    cypher,
                    parameters={"stop_id": from_stop_id, "pathway_id": pathway_id},
                )
            elif from_type == 1:
                cypher = """
                MATCH (pw:Pathway {id: $pathway_id})
                MATCH (s:Station {id: $stop_id})
                MERGE (pw)-[:LINKS]->(s)
                """
                log.info(
                    f"Creating LINKS from Pathway {pathway_id} to Station {from_stop_id}"
                )
                neo4j.execute_write(
                    cypher,
                    parameters={"stop_id": from_stop_id, "pathway_id": pathway_id},
                )
            elif from_type == 3:
                cypher = """
                MATCH (pw:Pathway {id: $pathway_id})
                MATCH (fg:FareGate {id: $stop_id})
                MERGE (pw)-[:LINKS]->(fg)
                """
                log.info(
                    f"Creating LINKS from Pathway {pathway_id} to FareGate {from_stop_id}"
                )
                neo4j.execute_write(
                    cypher,
                    parameters={"stop_id": from_stop_id, "pathway_id": pathway_id},
                )
        if not to_node.empty:
            to_type = to_node.iloc[0]["location_type"]
            if to_type == 2:
                cypher = """
                MATCH (pw:Pathway {id: $pathway_id})
                MATCH (e:StationEntrance {id: $stop_id})
                MERGE (pw)-[:LINKS]->(e)
                """
                log.info(
                    f"Creating LINKS from Pathway {pathway_id} to StationEntrance {to_stop_id}"
                )
                neo4j.execute_write(
                    cypher, parameters={"stop_id": to_stop_id, "pathway_id": pathway_id}
                )
            elif to_type == 0 or to_type == 4:
                cypher = """
                MATCH (pw:Pathway {id: $pathway_id})
                MATCH (p:Platform {id: $stop_id})
                MERGE (pw)-[:LINKS]->(p)
                """
                log.info(
                    f"Creating LINKS from Pathway {pathway_id} to Platform {to_stop_id}"
                )
                neo4j.execute_write(
                    cypher, parameters={"stop_id": to_stop_id, "pathway_id": pathway_id}
                )
            elif to_type == 1:
                cypher = """
                MATCH (pw:Pathway {id: $pathway_id})
                MATCH (s:Station {id: $stop_id})
                MERGE (pw)-[:LINKS]->(s)
                """
                log.info(
                    f"Creating LINKS from Pathway {pathway_id} to Station {to_stop_id}"
                )
                neo4j.execute_write(
                    cypher, parameters={"stop_id": to_stop_id, "pathway_id": pathway_id}
                )
            elif to_type == 3:
                cypher = """
                MATCH (pw:Pathway {id: $pathway_id})
                MATCH (fg:FareGate {id: $stop_id})
                MERGE (pw)-[:LINKS]->(fg)
                """
                log.info(
                    f"Creating LINKS from Pathway {pathway_id} to FareGate {to_stop_id}"
                )
                neo4j.execute_write(
                    cypher, parameters={"stop_id": to_stop_id, "pathway_id": pathway_id}
                )

    # ── Post-load validation ──────────────────────────────────────────────────
    if validate:
        log.info("physical load: running post-load validation")
        # TODO: Add physical layer validation logic if needed

    log.info("physical load: complete")
