from stockagent.live.public_dashboards import sanitize_tw_positions


def test_public_position_explains_system_band_without_disclosing_raw_limits():
    row = {"symbol": "009806", "upper_limit": 9999.95, "lower_limit": 0.01,
           "price_limit_band_kind": "forged", "bracket_cross_evidence": {
               "contract": "historical_bracket_full_on_strict_observed_cross_paper_assumption_v1",
               "simulation_only": True, "broker_fill": False, "exchange_fill": False,
               "source_path": "/private/source"}}
    result = sanitize_tw_positions({"simulation_only": True, "production_order_possible": False,
                                    "rows": [row]})["rows"][0]
    assert result["price_limit_band_kind"] == "recorded_system_band_without_daily_price_limit"
    assert result["paper_bracket_full_cross_assumed"] is True
    assert "upper_limit" not in result and "lower_limit" not in result
    assert "bracket_cross_evidence" not in result
    assert "/private/source" not in str(result)


def test_unconfirmed_or_forged_labels_are_not_forwarded():
    row = {"symbol": "2330", "upper_limit": 110.0, "lower_limit": 90.0,
           "price_limit_band_kind": "recorded_system_band_without_daily_price_limit",
           "paper_bracket_full_cross_assumed": True,
           "bracket_cross_evidence": {"contract": "unverified", "broker_fill": False}}
    result = sanitize_tw_positions({"simulation_only": True, "production_order_possible": False,
                                    "rows": [row]})["rows"][0]
    assert "price_limit_band_kind" not in result
    assert "paper_bracket_full_cross_assumed" not in result
