import json

import polars as pl

from scripts.benchmark_tw_opening_ownership import raw_trial, ipc_trial, run, factorized_stage_trial, run_cache_flush, run_ipc


def test_finmind_cadence_comparison_is_full_axis_offline_not_a_live_slo(tmp_path):
    from scripts.benchmark_tw_opening_sources import compare_finmind_cadence

    symbols = ["2330", "0050", "2426", "missing-at-first-poll"]
    result = compare_finmind_cadence(symbols, str(tmp_path / "stocks"), rounds=2)
    assert result["requested_count"] == len(symbols)
    assert result["network_requests"] == result["broker_orders"] == 0
    assert not result["opening_slo_claim"] and not result["production_ledger_modified"]
    assert result["financial_and_exchange_clock_parity"]
    assert "controlled_provider_clock_not_live_opening_slo" in result["scope"]
    assert result["median_virtual_completion_ms"]["before"] == 12400.
    assert result["median_virtual_completion_ms"]["after"] == 10300.
    assert len(result["samples"]) == 4
    assert all(row["durable_requested_count"] == row["durable_row_count"] == len(symbols)
               for row in result["samples"])


def test_source_decode_coalescing_preserves_complete_arrow_output(tmp_path):
    source = tmp_path / "source.parquet"
    pl.DataFrame({"value": [1., -0.0, float("nan"), None], "unit": ["股"] * 4}).write_parquet(source)
    baseline = raw_trial(source, enabled=False)
    candidate = raw_trial(source, enabled=True)
    assert baseline["output_sha256"] == candidate["output_sha256"]
    assert 1 <= baseline["physical_decodes"] <= 3 and candidate["physical_decodes"] == 1
    assert candidate["rows"] == 4 and candidate["columns"] == 2


def test_ipc_pairs_use_full_axis_and_do_not_assert_live_slo(tmp_path):
    symbols = ["2330", "0050", "2426"]
    before = ipc_trial(tmp_path / "before", symbols, autonomous=False, stall_ms=5)
    after = ipc_trial(tmp_path / "after", symbols, autonomous=True, stall_ms=5)
    assert before["fixture_output_sha256"] == after["fixture_output_sha256"]
    assert before["server_request_queue_ms"] >= 5
    assert after["client_total_ms"] >= 0
    assert after["server_receiver_thread"] == "tw-shared-quote-owner"


def test_ipc_only_needs_no_feature_body_or_source_lock(tmp_path, monkeypatch):
    from scripts import benchmark_tw_opening_ownership as benchmark
    from stockagent.live import quote_provider as quotes
    import pytest
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"requested_symbols": ["2330", "0050", "2426"],
                                  "requested_count": 3, "session_date": "2026-10-15"}))
    original = receipt.read_bytes()
    monkeypatch.setattr(benchmark, "live_tw_public_source_read_guard", lambda *_: pytest.fail("unrelated feature lease"))
    monkeypatch.setattr(quotes, "_shioaji_stock_api", lambda: pytest.fail("benchmark tried to login"))
    result = run_ipc(receipt, tmp_path / "ipc.json", repeats=2, stall_ms=5)
    assert len(result["pairs"]) == 2 and result["symbol_count"] == 3
    assert result["all_outputs_equal"] and "not_provider_model_or_live_opening_slo" in result["scope"]
    assert result["api_requests"] == result["broker_orders"] == result["ledger_writes"] == 0
    assert receipt.read_bytes() == original


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


def test_cache_flush_benchmark_preserves_full_axis_durability_and_restore(tmp_path):
    from datetime import datetime
    stamp = int(datetime.fromisoformat("2026-10-15T09:00:00+08:00").timestamp() * 1000)
    receipt = tmp_path / "source.json"
    receipt.write_text(json.dumps({"requested_symbols": ["2330", "0050", "missing"],
        "requested_count": 3, "session_date": "2026-10-15", "rows": {
            symbol: {"available": True, "open_prices": price, "price": price,
                     "timestamp_ms": stamp, "open_timestamp_ms": stamp,
                     "source": "twse_tpex:mis", "volumes": 5., "reference_prices": 99.}
            for symbol, price in (("2330", 100.12345678901235), ("0050", 200.))}}))
    original = receipt.read_bytes()
    result = run_cache_flush(receipt, tmp_path / "result.json", repeats=2, stall_ms=5)
    assert result["symbol_count"] == 3 and result["all_outputs_equal"]
    assert result["api_requests"] == result["broker_orders"] == result["ledger_writes"] == 0
    assert result["production_receipt_writes"] == 0
    assert "not_live_opening_slo" in result["scope"]
    assert len(result["pairs"]) == 2
    assert receipt.read_bytes() == original
    for pair in result["pairs"]:
        before, after = pair["original"], pair["off_lock"]
        assert before["baseline_code_sha256"] and after["baseline_code_sha256"] is None
        assert before["restored_row_count"] == after["restored_row_count"] == 2
        assert before["restored_full_output_sha256"] == after["restored_full_output_sha256"]
        assert all(row["file_and_directory_fsync_count"] == 2 * row["durable_write_count"]
                   for row in pair.values())


def test_complete_verified_stage_benchmark_keeps_original_baseline_and_source(tmp_path, monkeypatch):
    from stockagent.live import signal_engine as engine
    from stockagent.data import factorized_panel as source
    from test_live_panel_singleflight import factorized_example
    import time
    panel, path, _, config = factorized_example(tmp_path)
    original = path.read_bytes()
    monkeypatch.setattr(engine, "require_live_feature_source", lambda _: None)
    monkeypatch.setattr(engine, "checkpoint_manifest_symbols", lambda _: panel.symbols)
    monkeypatch.setattr(engine, "align_panel_to_checkpoint_universe", lambda value, *a, **kw: value)
    config.data.parquet_root = str(tmp_path / "stocks")
    attach = source.attach_factorized_live_features

    def overlap(*a, **kw):
        time.sleep(.02)  # Expose the old race deterministically in this tiny fixture only.
        return attach(*a, **kw)

    monkeypatch.setattr(source, "attach_factorized_live_features", overlap)
    kwargs = dict(checkpoint_payload={"model_state_dict": {}},
                  checkpoint_key="exact", fold_dir=tmp_path / "fold_11")
    before = factorized_stage_trial(panel, config, enabled=False, **kwargs)
    after = factorized_stage_trial(panel, config, enabled=True, **kwargs)
    assert before["attachment_calls"] == before["unique_decoded_source_caches"] == 2
    assert after["attachment_calls"] == after["unique_decoded_source_caches"] == 1
    assert before["output_identity_sha256"] == after["output_identity_sha256"]
    assert before["baseline_code_sha256"] and after["baseline_code_sha256"] is None
    assert before["shape"] == after["shape"] == [9, 3, 10]
    assert path.read_bytes() == original
    assert "not_dense_output_bytes" in after["oracle"]
