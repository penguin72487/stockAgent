import pytest

from urllib.parse import parse_qs, urlsplit

from scripts.verify_tw_day_trade_mode_browser import _signal_url, _verify_signal_boundary


def test_native_public_signal_request_keeps_the_query_separator():
    url = _signal_url("https://example.test/tw-day-trade/", market="new", day="2026-02-25")
    parsed = urlsplit(url)
    assert parsed.path == "/tw-day-trade/api/signals"
    assert parse_qs(parsed.query) == {"mode": ["new"], "date": ["2026-02-25"],
                                     "offset": ["0"], "limit": ["1"]}


def _payload():
    return {"simulation_only": True, "production_order_possible": False, "total": 2757,
            "rows": [{"market": "new", "session_date": "2026-02-25", "symbol": "2330",
                      "signal_at": "2026-02-25T09:00:00+08:00"}],
            "feature_drivers_by_signal": {
                "2026-02-25|new|2330|2026-02-25T09:00:00+08:00": {
                    "drivers": [], "feature_proxy_publication_allowed": False}}}


def test_public_boundary_and_private_policy_are_independently_accepted():
    proof = _verify_signal_boundary(_payload(), market="new", day="2026-02-25", private_features=True)
    assert proof["total"] == 2757
    assert proof["private_feature_policy_verified"] is True


def test_public_research_boundary_requires_a_visible_pit_caveat():
    payload = _payload()
    record = next(iter(payload["feature_drivers_by_signal"].values()))
    record.update(research_only=True, historical_point_in_time_proven=False)
    proof = _verify_signal_boundary(payload, market="new", day="2026-02-25", private_features=True)
    assert proof["research_notice_required"] is True


@pytest.mark.parametrize("field,value", [("total", 0), ("simulation_only", False),
                                        ("production_order_possible", True), ("rows", [])])
def test_public_boundary_rejects_false_acceptance(field, value):
    payload = _payload()
    payload[field] = value
    with pytest.raises(ValueError, match="boundary"):
        _verify_signal_boundary(payload, market="new", day="2026-02-25")


def test_public_boundary_rejects_another_mode_or_session():
    for market, day in (("old", "2026-02-25"), ("new", "2026-10-13")):
        with pytest.raises(ValueError, match="boundary"):
            _verify_signal_boundary(_payload(), market=market, day=day)


@pytest.mark.parametrize("record", [{}, {"drivers": []},
    {"drivers": [{"feature": "private", "value": 123}], "feature_proxy_publication_allowed": False}])
def test_private_boundary_requires_explicit_no_raw_proxy_policy(record):
    payload = _payload()
    key = next(iter(payload["feature_drivers_by_signal"]))
    payload["feature_drivers_by_signal"][key] = record
    with pytest.raises(ValueError, match="publication policy"):
        _verify_signal_boundary(payload, market="new", day="2026-02-25", private_features=True)
