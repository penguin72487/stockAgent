import pytest

from scripts import benchmark_shioaji_recovery_responsiveness as probe


def test_probe_is_bounded_isolated_and_preserves_prices_and_accounting(monkeypatch, tmp_path):
    fake_api_calls = []
    original_api = lambda: fake_api_calls.append("real-api-must-not-be-called")
    monkeypatch.setattr(probe.provider, "_shioaji_stock_api", original_api)
    monkeypatch.setenv("STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT", str(tmp_path / "real-ledger"))
    result = probe.run_probe(symbols=4, batch_size=2, native_delay_ms=1, usage_delay_ms=0)
    assert result["state"] == "accepted_fixture_only"
    assert result["all_price_hashes_equal"]
    assert result["journal_events"] == 16
    assert [t["variant"] for t in result["trials"]] == [
        "synchronous", "background", "background", "synchronous",
    ]
    assert all(t["fixture_native_calls"] == 4 for t in result["trials"])
    assert all(t["durable_receipt_matches"] for t in result["trials"])
    assert result["live_broker_requests"] == result["source_files_written"] == 0
    assert result["promoted"] is result["running_processes_reloaded"] is result["full_live_job_measured"] is False
    assert not (tmp_path / "real-ledger").exists()
    assert not fake_api_calls
    assert probe.provider._shioaji_stock_api is original_api


@pytest.mark.parametrize("kwargs", [
    {"symbols": 0}, {"symbols": 9}, {"batch_size": 0}, {"batch_size": 9},
    {"native_delay_ms": float("nan")}, {"usage_delay_ms": float("inf")},
    {"native_delay_ms": -1}, {"usage_delay_ms": 21},
])
def test_probe_rejects_unbounded_inputs_before_api_access(monkeypatch, kwargs):
    monkeypatch.setattr(probe.provider, "_shioaji_stock_api", lambda: pytest.fail("real broker access"))
    with pytest.raises(ValueError):
        probe.run_probe(**kwargs)
