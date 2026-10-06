import json

from scripts import benchmark_opening_quote_path as benchmark


def test_offline_quote_benchmark_keeps_prices_rows_and_unknown_usage(tmp_path, monkeypatch):
    receipt = tmp_path / "receipt.json"
    output = tmp_path / "result.json"
    row = {"available": True, "price": 101.0, "open_prices": 100.0,
           "high_prices": 102.0, "low_prices": 99.0, "volumes": 1000.0,
           "bid_prices": 100.5, "ask_prices": 101.0, "bid_volumes": 3.0,
           "ask_volumes": 5.0, "reference_prices": 100.0,
           "upper_limit_prices": 110.0, "lower_limit_prices": 90.0}
    receipt.write_text(json.dumps({"row_count": 2, "rows": {"2330": row, "0050": row}}))
    before = receipt.read_bytes()
    monkeypatch.setattr(benchmark.sys, "argv", ["benchmark", "--receipt", str(receipt),
                                               "--output", str(output), "--repeats", "2",
                                               "--usage-delay-ms", "1", "--compare-scalar"])
    benchmark.main()
    result = json.loads(output.read_text())
    assert receipt.read_bytes() == before
    assert result["network_requests"] == 0 and result["production_writes"] is False
    assert result["parity_verified"] is True
    assert result["durable_events"] == 6
    assert len(result["samples"]) == 6
    assert set(result["median_ms"]) == {"legacy", "critical_scalar_control", "critical"}
    for sample in result["samples"]:
        assert sample["batch_sizes"] == [2]
        assert sample["available_count"] == 2
        assert sample["usage_calls"] == (2 if sample["variant"] == "legacy" else 0)
