# tests/test_fare_extract_transform.py
"""
Tests for src/layers/fare/extract.py and src/layers/fare/transform.py

Extract tests:  correct keys returned, missing required file raises,
                optional file handled gracefully
Transform tests: FareZone dedup, logical FareProduct mapping, amount parsing,
                 rail vs bus leg rule splitting, pre-load validation gate
"""

import pandas as pd
import pytest

from src.layers.fare.extract import run as extract_run
from src.layers.fare.transform import _logical_product, _parse_amount
from src.layers.fare.transform import run as transform_run

# ═══════════════════════════════════════════════════════════════
# EXTRACT
# ═══════════════════════════════════════════════════════════════


class TestFareExtract:
    def test_returns_all_required_keys(self, gtfs_data):
        result = extract_run(gtfs_data)
        assert "stops" in result
        assert "fare_media" in result
        assert "fare_products" in result
        assert "fare_leg_rules" in result

    def test_raises_on_missing_required_file(self, gtfs_data):
        del gtfs_data["stops"]
        with pytest.raises(KeyError, match="stops"):
            extract_run(gtfs_data)

    def test_optional_file_included_when_present(self, gtfs_data):
        gtfs_data["fare_transfer_rules"] = pd.DataFrame(
            [
                dict(
                    from_leg_group_id="leg_metrobus_regular",
                    to_leg_group_id="leg_metrorail",
                    transfer_count=1,
                    duration_limit=7200,
                    duration_limit_type=1,
                    fare_transfer_type=1,
                    fare_product_id="metrobus_transfer_discount",
                )
            ]
        )
        result = extract_run(gtfs_data)
        assert "fare_transfer_rules" in result

    def test_optional_file_absent_is_ok(self, gtfs_data):
        result = extract_run(gtfs_data)
        assert "fare_transfer_rules" not in result

    def test_returns_defensive_copies(self, gtfs_data):
        result = extract_run(gtfs_data)
        # Modifying the result should not affect the original
        result["stops"].loc[0, "zone_id"] = "MUTATED"
        assert gtfs_data["stops"].loc[0, "zone_id"] != "MUTATED"


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — helpers
# ═══════════════════════════════════════════════════════════════


class TestParseAmount:
    def test_rail_225(self):
        assert _parse_amount("metrorail_one_way_full_fare_225") == pytest.approx(2.25)

    def test_rail_free(self):
        assert _parse_amount("metrorail_free_fare_000") == pytest.approx(0.00)

    def test_rail_high_fare(self):
        assert _parse_amount("metrorail_one_way_full_fare_675") == pytest.approx(6.75)

    def test_bus_regular_fixed(self):
        # Bus fares have no numeric suffix — requires product_amount_map lookup
        amount_map = {"metrobus_one_way_regular_fare": 2.25}
        assert _parse_amount("metrobus_one_way_regular_fare", amount_map) == pytest.approx(2.25)

    def test_bus_express_fixed(self):
        amount_map = {"metrobus_one_way_express_fare": 4.25}
        assert _parse_amount("metrobus_one_way_express_fare", amount_map) == pytest.approx(4.25)


class TestLogicalProduct:
    def test_bus_regular(self):
        logical_id, _ = _logical_product("metrobus_one_way_regular_fare")
        assert logical_id == "bus_regular"

    def test_rail_one_way_any_amount(self):
        logical_id, _ = _logical_product("metrorail_one_way_full_fare_450")
        assert logical_id == "rail_one_way"

    def test_rail_free(self):
        logical_id, _ = _logical_product("metrorail_free_fare_000")
        assert logical_id == "rail_free"

    def test_unknown_returns_none(self):
        assert _logical_product("completely_unknown_fare_id") is None


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — FareZone
# ═══════════════════════════════════════════════════════════════


class TestTransformFareZones:
    def test_unique_zones_extracted(self, gtfs_data):
        result = transform_run(gtfs_data)
        zone_ids = set(result.fare_zones["zone_id"].tolist())
        assert "3" in zone_ids
        assert "10" in zone_ids
        assert "53" in zone_ids

    def test_no_duplicate_zones(self, gtfs_data):
        result = transform_run(gtfs_data)
        assert result.fare_zones["zone_id"].nunique() == len(result.fare_zones)

    def test_station_zones_contains_stn_nodes(self, gtfs_data):
        result = transform_run(gtfs_data)
        assert all(result.station_zones["stop_id"].str.startswith("STN_"))

    def test_gate_zones_contains_fg_nodes(self, gtfs_data):
        result = transform_run(gtfs_data)
        assert all(result.gate_zones["stop_id"].str.contains("_FG_"))

    def test_busstop_excluded_from_zones(self, gtfs_data):
        result = transform_run(gtfs_data)
        all_stops = pd.concat(
            [result.station_zones["stop_id"], result.gate_zones["stop_id"]]
        )
        assert "10000" not in all_stops.values


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — FareProduct deduplication
# ═══════════════════════════════════════════════════════════════


class TestTransformFareProducts:
    def test_deduplicates_to_logical_nodes(self, gtfs_data):
        result = transform_run(gtfs_data)
        # Multiple raw rows with different amounts should collapse to one logical node
        product_ids = result.fare_products["fare_product_id"].tolist()
        assert product_ids.count("rail_one_way") == 1

    def test_all_expected_logical_products_present(self, gtfs_data):
        result = transform_run(gtfs_data)
        product_ids = set(result.fare_products["fare_product_id"].tolist())
        assert "bus_regular" in product_ids
        assert "bus_express" in product_ids
        assert "rail_free" in product_ids
        assert "rail_one_way" in product_ids


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — FareLegRule splitting
# ═══════════════════════════════════════════════════════════════


class TestTransformFareLegRules:
    def test_rail_rules_have_from_area(self, gtfs_data):
        result = transform_run(gtfs_data)
        assert len(result.leg_rule_from_area) > 0

    def test_rail_rules_have_to_area(self, gtfs_data):
        result = transform_run(gtfs_data)
        assert len(result.leg_rule_to_area) > 0

    def test_from_area_resolves_to_zone_not_stop_id(self, gtfs_data):
        result = transform_run(gtfs_data)
        # Zone ids should be numeric strings like "3", not "STN_A02"
        assert not any(
            z.startswith("STN_") for z in result.leg_rule_from_area["zone_id"].tolist()
        )

    def test_bus_rules_not_in_from_area(self, gtfs_data):
        result = transform_run(gtfs_data)
        bus_prefixes = ("leg_metrobus_regular__", "leg_metrobus_express__")
        # rule_id encodes leg_group_id as prefix — bus rules have no from/to area
        # so they should not appear in leg_rule_from_area at all
        from_area_rule_ids = set(result.leg_rule_from_area["rule_id"].tolist())
        assert not any(
            rid.startswith(bus_prefixes) for rid in from_area_rule_ids
        )

    def test_applies_product_has_amount(self, gtfs_data):
        result = transform_run(gtfs_data)
        assert "amount" in result.leg_rule_applies_product.columns
        # All amounts should be non-negative floats
        assert (result.leg_rule_applies_product["amount"] >= 0).all()

    def test_farragut_free_transfer_amount_is_zero(self, gtfs_data):
        result = transform_run(gtfs_data)
        free_rows = result.leg_rule_applies_product[
            result.leg_rule_applies_product["fare_product_id"] == "rail_free"
        ]
        assert len(free_rows) > 0
        assert (free_rows["amount"] == 0.0).all()

    def test_timeframe_preserved_on_applies_product(self, gtfs_data):
        result = transform_run(gtfs_data)
        timeframes = set(result.leg_rule_applies_product["timeframe"].dropna().tolist())
        assert "weekday_regular" in timeframes


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — pre-load validation gate
# ═══════════════════════════════════════════════════════════════


class TestTransformValidationGate:
    def test_raises_on_unresolvable_area_id(self, gtfs_data):
        gtfs_data["fare_leg_rules"].loc[len(gtfs_data["fare_leg_rules"])] = {
            "leg_group_id": "leg_metrorail",
            "network_id": "metrorail",
            "from_area_id": "STN_GHOST",
            "to_area_id": "STN_A02",
            "fare_product_id": "metrorail_one_way_full_fare_225",
            "from_timeframe_group_id": "weekday_regular",
        }
        with pytest.raises(ValueError, match="validation failed"):
            transform_run(gtfs_data)


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — real GTFS data
# ═══════════════════════════════════════════════════════════════


def test_transform_on_real_gtfs(real_stops_df, real_fare_leg_df):
    """Full transform run against actual WMATA GTFS files."""
    # Use a complete fare_products fixture that includes all 5 logical products
    full_fare_products = pd.DataFrame(
        [
            dict(
                fare_product_id="metrobus_one_way_regular_fare",
                fare_product_name="Metrobus Regular",
                fare_media_id="smartrip_card",
            ),
            dict(
                fare_product_id="metrobus_one_way_express_fare",
                fare_product_name="Metrobus Express",
                fare_media_id="smartrip_card",
            ),
            dict(
                fare_product_id="metrorail_free_fare_000",
                fare_product_name="Metrorail Free",
                fare_media_id="smartrip_card",
            ),
            dict(
                fare_product_id="metrorail_one_way_full_fare_225",
                fare_product_name="Metrorail One-Way",
                fare_media_id="smartrip_card",
            ),
            dict(
                fare_product_id="metrobus_transfer_discount",
                fare_product_name="Metrobus Transfer Discount",
                fare_media_id="smartrip_card",
            ),
        ]
    )
    full_fare_media = pd.DataFrame(
        [
            dict(
                fare_media_id="smartrip_card",
                fare_media_name="SmarTrip Card",
                fare_media_type=2,
            ),
        ]
    )
    raw = {
        "stops": real_stops_df,
        "fare_leg_rules": real_fare_leg_df,
        "fare_media": full_fare_media,
        "fare_products": full_fare_products,
        "feed_info": pd.DataFrame([dict(
            feed_publisher_name="WMATA", feed_publisher_url="https://wmata.com",
            feed_lang="en", feed_start_date="20251214", feed_end_date="20260613",
            feed_version="S1000246", feed_contact_email="", feed_contact_url="",
        )]),
    }
    result = transform_run(raw)

    assert len(result.fare_zones) == 42
    assert len(result.station_zones) == 98
    assert len(result.gate_zones) == 240
    assert len(result.fare_products) == 5  # 5 logical nodes
    assert len(result.fare_leg_rules) == 4  # 4 unique leg_group_ids
