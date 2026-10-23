import pytest

from urllib.parse import parse_qs, urlsplit

from scripts.verify_tw_day_trade_mode_browser import (
    _expected_beacon_csp_denial, _failed_viewport_evidence, _is_cloudflare_beacon, _prepare_output_dir,
    _signal_url, _verify_signal_boundary,
)


def test_browser_run_atomically_reserves_a_fresh_evidence_directory(tmp_path):
    directory = tmp_path / "nested" / "run"
    assert _prepare_output_dir(directory) == directory / "receipt.json"
    assert directory.is_dir()
    with pytest.raises(FileExistsError, match="choose a new path"):
        _prepare_output_dir(directory)


def test_browser_run_does_not_overwrite_a_previous_failed_receipt(tmp_path):
    receipt = tmp_path / "receipt.json"
    original = b'{"status":"failed","error":"source timeout"}\n'
    receipt.write_bytes(original)
    with pytest.raises(FileExistsError, match="choose a new path"):
        _prepare_output_dir(tmp_path)
    assert receipt.read_bytes() == original


def test_browser_run_rejects_an_existing_file_as_output_directory(tmp_path):
    existing = tmp_path / "evidence"
    existing.write_bytes(b"retained")
    with pytest.raises(FileExistsError, match="choose a new path"):
        _prepare_output_dir(existing)
    assert existing.read_bytes() == b"retained"


@pytest.mark.parametrize("state_error,screenshot_error", [(False, False), (True, False),
                                                        (False, True), (True, True)])
def test_failed_viewport_preserves_available_diagnostics_without_masking_the_failure(
    tmp_path, state_error, screenshot_error,
):
    class Page:
        def evaluate(self, script):
            assert "signal_rows" in script
            assert ".slice(0, 2000)" in script
            if state_error:
                raise RuntimeError("page already closed")
            return {"signal_rows": 0, "signal_count": "request timed out"}

        def screenshot(self, *, path, timeout):
            assert path == str(tmp_path / "laptop_failed.png")
            assert timeout == 10_000
            if screenshot_error:
                raise RuntimeError("screenshot unavailable")

    proof = _failed_viewport_evidence(Page(), tmp_path, "laptop")
    assert len(proof["diagnostic_errors"]) == int(state_error) + int(screenshot_error)
    assert ("rendered_state" in proof) is not state_error
    assert bool(proof["screenshots"]) is not screenshot_error


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


@pytest.mark.parametrize("url,expected", [
    ("https://static.cloudflareinsights.com/beacon.min.js/v4", True),
    ("https://static.cloudflareinsights.com/beacon.min.js", True),
    ("https://static.cloudflareinsights.com/other.js", False),
    ("https://static.cloudflareinsights.com.attacker.test/beacon.min.js", False),
    ("https://example.test/static.cloudflareinsights.com/beacon.min.js", False),
    ("http://static.cloudflareinsights.com/beacon.min.js", False),
    ("https://static.cloudflareinsights.com/beacon.min.js-unrelated", False),
])
def test_optional_beacon_denial_has_exact_external_host_and_path(url, expected):
    assert _is_cloudflare_beacon(url) is expected


@pytest.mark.parametrize("url", ["https://static.cloudflareinsights.com/beacon.min.js/v4",
                                "https://example.test/app.js"])
def test_only_self_only_csp_denial_of_known_optional_beacon_is_security_evidence(url):
    message = (f"Loading the script '{url}' violates the following Content Security Policy directive: "
               '"script-src \'self\'". The action has been blocked.')
    assert _expected_beacon_csp_denial(message) is _is_cloudflare_beacon(url)
    assert _expected_beacon_csp_denial(message.replace("The action has been blocked.", "Report only.")) is False
    assert _expected_beacon_csp_denial(message.replace("script-src 'self'", "script-src https:")) is False
    assert _expected_beacon_csp_denial("Uncaught TypeError: application crashed") is False
