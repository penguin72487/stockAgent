import json

import polars as pl

from scripts.benchmark_tw_opening_ownership import raw_trial, ipc_trial, run


def test_source_decode_coalescing_preserves_complete_arrow_output(tmp_path):
    source = tmp_path / "source.parquet"
    pl.DataFrame({"value": [1., -0.0, float("nan"), None], "unit": ["股"] * 4}).write_parquet(source)
    baseline = raw_trial(source, enabled=False)
    candidate = raw_trial(source, enabled=True)
    assert baseline["output_sha256"] == candidate["output_sha256"]
    assert baseline["physical_decodes"] == 3 and candidate["physical_decodes"] == 1
    assert candidate["rows"] == 4 and candidate["columns"] == 2


def test_ipc_pairs_use_full_axis_and_do_not_assert_live_slo(tmp_path):
    symbols = ["2330", "0050", "2426"]
    before = ipc_trial(tmp_path / "before", symbols, autonomous=False, stall_ms=5)
    after = ipc_trial(tmp_path / "after", symbols, autonomous=True, stall_ms=5)
    assert before["fixture_output_sha256"] == after["fixture_output_sha256"]
    assert before["server_request_queue_ms"] >= 5
    assert after["client_total_ms"] >= 0


def test_benchmark_receipts_are_offline_and_source_immutable(tmp_path):
    root = tmp_path / "stocks"
    root.mkdir()
    source = root / "2330_features.parquet"
    pl.DataFrame({"price": [10.0, 20.0]}).write_parquet(source)
    original = source.read_bytes()
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"requested_symbols": ["2330", "0050"], "requested_count": 2,
                                  "session_date": "2026-10-15", "parquet_root": str(root)}))
    output = tmp_path / "benchmark.json"
    result = run(receipt, output, repeats=2, stall_ms=5)
    assert result["api_requests"] == result["broker_orders"] == result["ledger_writes"] == 0
    assert result["symbol_count"] == 2 and result["all_outputs_equal"]
    assert "not_model_or_live_opening_slo" in result["scope"]
    assert len(result["pairs"]) == 2 and output.is_file()
    assert source.read_bytes() == original
