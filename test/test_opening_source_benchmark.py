from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime
from http.client import RemoteDisconnected
import base64
import hashlib
import json
from pathlib import Path
import shutil
import sys
from zoneinfo import ZoneInfo

import numpy as np
import pytest
import requests

from scripts import benchmark_tw_opening_sources as benchmark
from stockagent.live import quote_provider as quotes


def test_member_signature_benchmark_keeps_full_inventory_and_balanced_order(monkeypatch, tmp_path):
    from scripts import benchmark_opening_preparation as preparation
    member = tmp_path / "member"
    member.write_bytes(b"immutable source")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"common": {"path": "member"}, "blocks": []}))
    monkeypatch.setattr(preparation.bot, "_effective_market_config", lambda cfg: cfg)
    monkeypatch.setattr(preparation, "load_config", lambda _p: SimpleNamespace(
        data=SimpleNamespace(factorized_feature_manifest=str(manifest))))
    cfgs = [SimpleNamespace(market=market, config_path="fixture") for market in ("a", "b")]
    result = preparation.member_signature_probe(cfgs, repeats=2)
    assert result["network_requests"] == result["model_forwards"] == 0
    assert not result["ledger_modified"]
    row, = result["comparisons"]
    assert row["markets"] == ["a", "b"] and row["member_count"] == 2
    assert row["exact_full_signature_parity"]
    assert [sample["variant"] for sample in row["samples"]] == [
        "per_member_resolve", "per_scan_parent_resolve",
        "per_scan_parent_resolve", "per_member_resolve"]


def test_member_signature_benchmark_rejects_mixed_generations(monkeypatch, tmp_path):
    from scripts import benchmark_opening_preparation as preparation
    from stockagent.live import signal_engine
    member = tmp_path / "member"
    member.write_bytes(b"original")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"common": {"path": "member"}, "blocks": []}))
    monkeypatch.setattr(preparation.bot, "_effective_market_config", lambda cfg: cfg)
    monkeypatch.setattr(preparation, "load_config", lambda _p: SimpleNamespace(
        data=SimpleNamespace(factorized_feature_manifest=str(manifest))))
    original = signal_engine._factorized_member_signatures

    def changed(path):
        signatures = original(path)
        member.write_bytes(b"different")
        return signatures

    monkeypatch.setattr(signal_engine, "_factorized_member_signatures", changed)
    with pytest.raises(RuntimeError, match="source changed"):
        preparation.member_signature_probe([SimpleNamespace(market="a", config_path="fixture")], repeats=1)


def test_configured_fugle_routes_match_regular_stock_etf_scope(monkeypatch):
    monkeypatch.delenv("FINMIND_TOKEN", raising=False)
    monkeypatch.setenv("FUGLE_API_KEY", "test-not-a-live-credential")
    providers = benchmark.acquisition.optional_opening_providers()
    assert set(providers) == {"fugle_tse", "fugle_otc"}
    assert callable(benchmark.acquisition.fugle_market_provider("TIB"))


def test_mis_recovery_comparison_keeps_full_axis_rows_and_original_clock(tmp_path, monkeypatch):
    baseline = tmp_path / "before.py"
    shutil.copyfile(benchmark.acquisition.__file__, baseline)
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", None)
    monkeypatch.setattr(quotes, "_shioaji_stock_api", lambda: pytest.fail("offline comparison cannot login"))
    report = benchmark.compare_mis_recovery(["A", "B", "C"], tmp_path / "stocks", baseline, rounds=1)
    assert report["requested_count"] == 3
    assert report["financial_and_clock_parity"] and report["durable_row_parity"]
    assert report["network_requests"] == 0 and not report["opening_slo_claim"]
    assert not report["production_ledger_modified"] and not report["broker_order_submitted"]
    for row in report["samples"]:
        assert row["valid_open_count"] == row["durable_requested_count"] == 3
        assert [call["count"] for call in row["source_calls"]] == [3, 1, 2]


def test_scheduled_probe_requires_today_official_full_scope_and_avoids_protected_open(monkeypatch, tmp_path):
    from stockagent.live import market_status
    day = "2026-10-16"
    receipt = tmp_path / "scope.json"
    receipt.write_text(json.dumps({"session_date": day, "requested_symbols": ["A", "B"],
        "parquet_root": str(tmp_path / "stocks"), "official_trading_universe": {
            "session_date": day, "contract": "tw_official_current_regular_stock_etf_universe_v3",
            "symbols": ["B", "A"], "official_trading_count": 2}}))
    monkeypatch.setattr(market_status, "verified_tw_stock_session_day", lambda *a, **k: (True, "official_calendar"))
    def context(clock):
        return benchmark.scheduled_probe_context(observed_at=datetime.fromisoformat(day + "T" + clock + "+08:00"),
            receipt_path=receipt, output_root=tmp_path / "evidence")[2]
    assert context("09:00:59")["state"].startswith("skipped_protected_opening")
    assert context("10:30:00")["state"] == "probe"
    assert context("13:40:00")["phase"] == "postclose"
    assert context("10:30:00")["shioaji_policy"] == "native_owner_passive_audit_no_snapshot_polling"
    changed = json.loads(receipt.read_bytes())
    changed["requested_symbols"] = ["A"]
    receipt.write_text(json.dumps(changed))
    assert context("10:30:00")["state"] == "waiting_official_scope"


@pytest.mark.parametrize("captured_owners", [False, True])
def test_retained_mis_pipeline_keeps_all_prices_clocks_and_durable_rows(monkeypatch, tmp_path, captured_owners):
    fixed = datetime.fromisoformat("2026-10-16T09:00:01+08:00")
    baseline, retained = tmp_path / "before.py", tmp_path / "responses.json"
    shutil.copyfile(quotes.__file__, baseline)
    body = json.dumps({"msgArray": [{"c": "2330", "o": "100", "z": "101",
        "tlong": str(int(fixed.timestamp() * 1000)), "v": "1"}]}).encode()
    retained.write_text(json.dumps({"observed_at_taipei": fixed.isoformat(), "responses": [{
        "channels": "tse_2330.tw|otc_2330.tw", "http_status": 200,
        "body_base64": base64.b64encode(body).decode(), "body_sha256": hashlib.sha256(body).hexdigest()}]}))
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", None)
    monkeypatch.setattr(quotes, "_shioaji_stock_api", lambda: pytest.fail("no broker login"))
    clock_checks = []
    def scope_required(_root):
        clock_checks.append(benchmark.universe.datetime.now(ZoneInfo("Asia/Taipei")))
        assert clock_checks[-1] == fixed, "authority validation must share the retained response clock"
        return False  # The temporary test root has no registered production authority.
    monkeypatch.setattr(benchmark.universe, "official_scope_required", scope_required)
    options = {}
    canonical_authority = benchmark.universe
    if captured_owners:
        owner, authority = tmp_path / "owner.py", tmp_path / "authority.py"
        shutil.copyfile(benchmark.acquisition.__file__, owner)
        shutil.copyfile(canonical_authority.__file__, authority)
        options = {"baseline_acquisition_file": owner, "retained_universe_file": authority}
    result = benchmark.compare_mis_pipeline(["2330"], tmp_path, baseline, retained, rounds=2,
        simulated_http_delay_ms=1., simulated_progress_delay_ms=1., **options)
    if not captured_owners:
        assert clock_checks
    assert result["baseline_acquisition_sha256"] == result["candidate_acquisition_sha256"]
    assert result["retained_universe_verifier_explicit"] == captured_owners
    assert result["retained_universe_verifier_sha256"] == hashlib.sha256(
        Path(canonical_authority.__file__).read_bytes()).hexdigest()
    assert sys.modules["stockagent.data.tw_current_trading_universe"] is canonical_authority
    from stockagent.data import tw_current_trading_universe
    assert tw_current_trading_universe is canonical_authority
    assert not any(name in sys.modules for name in ("_mis_acquisition_baseline", "_mis_retained_universe"))
    assert result["financial_and_clock_parity"] and result["durable_rows_parity"]
    assert result["network_requests"] == 0 and not result["opening_slo_claim"]
    assert not result["production_ledger_modified"]
    assert result["controlled_stage_delays"] == {"http_delay_ms": 1., "progress_delay_ms": 1.,
        "boundary": "explicit_synthetic_delays_not_observed_provider_or_storage_latency"}
    assert [row["variant"] for row in result["samples"]] == ["before", "after", "after", "before"]
    assert all(row["request_count"] == row["valid_open_count"] == row["durable_requested_count"] == 1
               for row in result["samples"])
    assert all(row["source_dispatch"]["dispatch_telemetry_recorded"] is True
               and row["source_dispatch"]["provider_call_offset_ms"] >= 0
               and row["source_dispatch"]["dispatch_telemetry_wait_ms"] >= 0
               for row in result["samples"])
    for row in result["samples"]:
        assert row["local_stage_policy"] == "all_threads_nested_wall_spans_not_additive_v1"
        assert row["local_stage_spans"]["source_journal"]["calls"] >= 3
        assert row["local_stage_spans"]["fetch_tw_mis_opening_snapshot"]["calls"] >= 1
        assert all(span["sum_ms"] >= span["max_ms"] >= 0 and span["calls"] > 0
            for span in row["local_stage_spans"].values())
        assert len(row["input_durable_flushes"]) == row["local_stage_spans"]["persist_tw_opening_snapshot_cache"]["calls"]
        written = [item for item in row["input_durable_flushes"] if item["receipt_written"]]
        assert written and all(item["receipt_stages"]["row_count"] == 1 for item in written)
        assert row["mis_local_pipeline"]["mis_column_assembly_policy"] == "unique_destination_bulk_copy_ordered_conflict_fallback_v1"
    corrupt = json.loads(retained.read_bytes())
    corrupt["responses"][0]["body_sha256"] = "bad"
    retained.write_text(json.dumps(corrupt))
    with pytest.raises(ValueError, match="integrity"):
        benchmark.compare_mis_pipeline(["2330"], tmp_path, baseline, retained, rounds=1)


@pytest.mark.parametrize("option,value", [(option, "/not-loaded.py") for option in (
    "--baseline-acquisition-owner", "--retained-universe-owner", "--baseline-quote-owner", "--mis-responses",
    "--finmind-payload")] + [(option, "1") for option in (
    "--mis-simulated-http-delay-ms", "--mis-simulated-progress-delay-ms")])
def test_scheduled_probe_rejects_offline_owners_before_reading_scope(monkeypatch, option, value):
    monkeypatch.setattr(sys, "argv", ["probe", "--scheduled", option, value])
    monkeypatch.setattr(benchmark, "scheduled_probe_context", lambda **_kwargs: pytest.fail(
        "scheduled source admission must reject captured offline owners before reading any scope"))
    with pytest.raises(SystemExit) as exc:
        benchmark.main()
    assert exc.value.code == 2


@pytest.mark.parametrize("delay", [-1., 101., float("nan"), float("inf")])
def test_retained_mis_comparison_rejects_unbounded_synthetic_delays(monkeypatch, tmp_path, delay):
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", None)
    for option in ("simulated_http_delay_ms", "simulated_progress_delay_ms"):
        with pytest.raises(ValueError, match="finite and within"):
            benchmark.compare_mis_pipeline(["2330"], tmp_path, tmp_path / "unread.py",
                tmp_path / "unread.json", rounds=1, **{option: delay})


@pytest.mark.parametrize("option", ["--mis-simulated-http-delay-ms", "--mis-simulated-progress-delay-ms"])
def test_network_probe_rejects_synthetic_delay_options(monkeypatch, tmp_path, option):
    receipt = tmp_path / "scope.json"
    receipt.write_text(json.dumps({"requested_symbols": ["2330"], "parquet_root": str(tmp_path)}))
    monkeypatch.setattr(sys, "argv", ["probe", "--receipt", str(receipt), "--output", str(tmp_path / "out.json"),
        "--finmind-only", option, "1"])
    with pytest.raises(SystemExit) as exc:
        benchmark.main()
    assert exc.value.code == 2
    assert not (tmp_path / "out.json").exists()


def test_input_preparation_probe_keeps_all_modes_and_warm_boundary(monkeypatch):
    from scripts import benchmark_opening_preparation as preparation
    from stockagent.live import signal_engine
    import torch

    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: False)
    monkeypatch.setattr(preparation.bot, "_effective_market_config", lambda cfg: cfg)
    called = []

    class Config:
        def __init__(self, name):
            self.market = name

        def signal_kwargs(self, **kwargs):
            return {"market": self.market, **kwargs}

    def prefetch(**kwargs):
        called.append(kwargs["market"])
        assert kwargs["panel_date"] == "2026-10-21"
        kwargs["progress_callback"]({"message": "quote_transport"})
        return SimpleNamespace(snapshot=quotes.PriceSnapshot(prices=np.array([100.]), source="receipt_fixture",
            open_prices=np.array([99.]), available_mask=np.array([True]),
            exchange_timestamps_ms=np.array([1])), request_key="canonical-input")

    monkeypatch.setattr(signal_engine, "prefetch_live_signal_prices", prefetch)
    result = preparation.input_preparation_probe([Config("a"), Config("b")], repeats=2,
                                               panel_date="2026-10-21")
    assert result["requested_panel_date"] == "2026-10-21"
    assert sorted(called) == ["a", "a", "b", "b"]
    assert result["mode_count"] == 2 and not result["opening_slo_claim"]
    assert not result["signal_published"] and not result["ledger_modified"]
    assert result["derived_cache_writes_possible"] and result["network_requests"] == 0
    assert [row["cache_state"] for row in result["samples"]] == ["fresh_process_first_use", "process_warm_revalidated"]
    assert all(len(row["modes"]) == 2 for row in result["samples"])


def test_input_preparation_probe_rejects_an_already_cuda_warm_process(monkeypatch):
    from scripts import benchmark_opening_preparation as preparation
    import torch
    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: True)
    with pytest.raises(RuntimeError, match="fresh CUDA-cold"):
        preparation.input_preparation_probe([], repeats=1)


def test_independent_poll_comparison_preserves_complete_values_and_never_logs_in(monkeypatch, tmp_path):
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", None)
    baseline = tmp_path / "before.py"
    shutil.copyfile(benchmark.acquisition.__file__, baseline)
    result = benchmark.compare_independent_poll(["A", "B"], tmp_path, baseline, rounds=1)
    assert result["network_requests"] == 0 and not result["opening_slo_claim"]
    assert result["financial_and_exchange_clock_parity"]
    assert all(row["durable_requested_count"] == 2 for row in result["samples"])
    assert result["samples"][0]["financial_and_exchange_clock_sha256"] == result["samples"][1]["financial_and_exchange_clock_sha256"]


@pytest.mark.parametrize("cached_rows", [0, 7])
def test_callback_pipeline_comparison_is_exact_and_never_logs_in(monkeypatch, tmp_path, cached_rows):
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", None)
    monkeypatch.setattr(quotes, "_shioaji_stock_api", lambda: pytest.fail("offline comparison must not log in"))
    captured = tmp_path / "before.py"
    shutil.copyfile(quotes.__file__, captured)
    result = benchmark.compare_callback_pipeline([str(1000 + i) for i in range(25)], captured, rounds=2,
        cached_rows=cached_rows, accounting_delay_ms=1 if cached_rows else 0, response_delay_ms=1 if cached_rows else 0)
    assert result["snapshot_parity"] and result["progress_parity"] and result["durable_rows_parity"]
    assert result["network_requests"] == 0
    assert not result["opening_slo_claim"]
    assert not result["production_ledger_modified"]
    assert result["baseline_code_sha256"] == result["candidate_code_sha256"]
    assert [row["variant"] for row in result["samples"]] == ["before", "after", "after", "before"]
    assert all(row["snapshot_decode_count"] == 25 - cached_rows for row in result["samples"])
    assert all(row["pipeline_to_source_readback_ms"] >= row["initial_receipt_publish_ms"] for row in result["samples"])
    assert all(row["initial_receipt_writes"] == 1 and row["unchanged_receipt_writes"] == 0 for row in result["samples"])
    assert all(row["initial_storage_barriers"] == {"file": 1, "directory": 1} for row in result["samples"])


def test_negotiated_ipc_benchmark_keeps_exact_axis_and_bitwise_readback(tmp_path, monkeypatch):
    from scripts import benchmark_tw_opening_ownership as ownership
    pytest.importorskip("msgspec")
    monkeypatch.setattr(quotes, "_shioaji_stock_api", lambda: pytest.fail("no benchmark login"))
    receipt = tmp_path / "scope.json"
    receipt.write_text(json.dumps({"requested_symbols": ["2330", "0050", "2317", "A"],
        "requested_count": 4, "session_date": "2026-10-22"}))
    output = tmp_path / "report.json"
    result = ownership.run_ipc_serialization(receipt, output, repeats=2)
    assert result["all_outputs_equal"] and result["symbol_count"] == 4
    assert result["api_requests"] == result["broker_orders"] == result["ledger_writes"] == 0
    assert result["injected_stall_ms"] == 0
    assert json.loads(output.read_bytes())["official_receipt_sha256"] == hashlib.sha256(receipt.read_bytes()).hexdigest()
    for pair in result["pairs"]:
        for codec, sample in pair.items():
            assert sample["response_formats"] == [codec]
            assert sample["response_write_count"] == 1 and sample["response_write_bytes"] > 0
            assert sample["request_to_verified_readback_ms"] >= sample["client_total_ms"]
            assert sample["response_encode_publish_ms"] >= 0 and sample["client_response_decode_ms"] >= 0
        assert pair["json_v1"]["fixture_output_sha256"] == pair["msgpack_v1"]["fixture_output_sha256"]


@pytest.mark.parametrize("include_failures", [False, True])
def test_retained_finmind_comparison_keeps_full_financial_clock_and_scope_parity(monkeypatch, tmp_path, include_failures):
    now = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=9, minute=0, second=0, microsecond=0)
    baseline, quote_baseline, payload = tmp_path / "before.py", tmp_path / "quotes.py", tmp_path / "body.json"
    shutil.copyfile(benchmark.acquisition.__file__, baseline)
    shutil.copyfile(quotes.__file__, quote_baseline)
    payload.write_text(json.dumps({"status": 200, "data": [{"stock_id": symbol, "open": 100 + i,
        "date": now.isoformat(), "total_volume": 1} for i, symbol in enumerate("AB")]}))
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", None)
    result = benchmark.compare_finmind_decoder(list("AB"), tmp_path, baseline, payload,
        rounds=1, baseline_quotes_path=quote_baseline, include_source_failures=include_failures)
    assert result["financial_and_clock_parity"]
    assert result["durable_rows_parity"]
    assert result["observation_journal_parity"]
    assert result["controlled_parallel_failures"] == include_failures
    assert result["network_requests"] == 0 and not result["opening_slo_claim"]
    assert all(row["valid_open_count"] == row["durable_requested_count"] == 2 for row in result["samples"])
    assert all(row["quorum_reached"] for row in result["samples"])
    assert result["baseline_quote_owner_sha256"] == result["candidate_quote_owner_sha256"]
    assert not result["provider_thread_profiling_enabled"]
    for row in result["samples"]:
        assert row["total_with_readback_ms"] >= row["total_local_workflow_ms"] >= row["quorum_return_ms"]
        assert row["local_stage_policy"] == "all_threads_nested_wall_spans_not_additive_v1"
        assert row["local_stage_spans"]["seed_tw_opening_snapshot_cache"]["calls"] >= 2
        assert row["local_stage_spans"]["_record_source_events"]["calls"] > 0
        assert row["provider_thread_profile"] == []
    if include_failures:
        assert all(all(write["row_count"] for write in row["input_receipt_writes"]) for row in result["samples"])


def test_observation_coverage_rechecks_absence_age_not_model_or_fill_readiness():
    observed = datetime.fromisoformat("2026-10-22T09:01:00+08:00")
    stamp = int(observed.timestamp() * 1000)
    value = quotes.PriceSnapshot(prices=np.ones(4), source="opening:shared_opening_snapshot",
        open_prices=np.array([100., np.nan, np.nan, np.nan]), available_mask=np.ones(4, dtype=bool),
        timestamps_ms=np.full(4, stamp), exchange_timestamps_ms=np.full(4, stamp),
        no_open_print_timestamps_ms=np.array([0, stamp - 15000, stamp - 15001, stamp + 1]))
    result = benchmark.observation_coverage(value, session_date="2026-10-22", observed_at=observed)
    assert result["positive_open_count"] == result["observed_no_print_count"] == 1
    assert result["resolved_count"] == result["unknown_count"] == result["stale_no_print_proof_count"] == 2
    assert result["model_input_readiness"].startswith("unmeasured")
    with pytest.raises(ValueError, match="aware"):
        benchmark.observation_coverage(value, session_date="2026-10-22", observed_at=observed.replace(tzinfo=None))


@pytest.mark.parametrize("option,value", [("--profile-finmind-local", None), ("--retain-mis-responses", None),
                                         ("--source-drain-timeout", "91")])
def test_scheduled_probe_rejects_manual_profile_and_retention(monkeypatch, option, value):
    argv = ["probe", "--scheduled", option] + ([] if value is None else [value])
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(benchmark, "scheduled_probe_context", lambda **_kwargs: pytest.fail("reject before scope read"))
    with pytest.raises(SystemExit) as exc:
        benchmark.main()
    assert exc.value.code == 2


def test_finmind_only_probe_never_queries_mis_or_broker(monkeypatch, tmp_path):
    now = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=9, minute=0)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    source, target = tmp_path / "source.json", tmp_path / "probe.json"
    source.write_text(json.dumps({"session_date": now.date().isoformat(), "requested_symbols": ["A"],
        "rows": {}, "parquet_root": str(tmp_path)}))
    monkeypatch.setattr(benchmark, "datetime", Clock)
    monkeypatch.setattr(benchmark.acquisition, "datetime", Clock)
    monkeypatch.setattr(quotes, "datetime", Clock)
    monkeypatch.setattr(sys, "argv", ["probe", "--receipt", str(source), "--output", str(target),
                                     "--rounds", "1", "--finmind-only"])
    monkeypatch.setattr(quotes, "warm_tw_mis_quote_client", lambda: pytest.fail("FinMind probe must not query MIS"))
    monkeypatch.setattr(benchmark.acquisition, "configured_opening_providers", lambda *_: pytest.fail("FinMind probe must not query broker"))
    calls = []
    def provider(names, fallback, progress, *, force_network):
        calls.append(force_network)
        snapshot = quotes.PriceSnapshot(prices=np.array([100.]), open_prices=np.array([100.]),
            source="finmind:fixture", available_mask=np.array([True]), available_count=1,
            requested_count=1, timestamps_ms=np.array([int(now.timestamp() * 1000)]),
            exchange_timestamps_ms=np.array([int(now.timestamp() * 1000)]))
        progress(names, snapshot)
        return snapshot
    monkeypatch.setattr(benchmark.acquisition, "fetch_finmind_opening_prices", provider)
    assert benchmark.main() == 0
    result = json.loads(target.read_text())
    assert result["providers"] == ["finmind_snapshot"] and calls == [True]
    assert not result["mis_connection_prewarm"]["performed"]
    assert result["rounds"][0]["final_positive_opens"] == 1
    assert result["data_complete"] and result["rounds"][0]["quorum_reached"]
    assert result["workflow_timing"]["total_before_final_report_write_ms"] >= result["rounds"][0]["entry_to_round_readback_ms"]


def test_retry_comparison_preserves_arrays_and_does_not_query_providers(monkeypatch, tmp_path):
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", None)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE_KEY", None)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE", {})
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_REQUESTED", set())
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_DIRTY", False)
    monkeypatch.setattr(benchmark.acquisition, "configured_opening_providers", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("offline benchmark must not query real providers")))
    result = benchmark.compare_no_progress(["A", "B"], str(tmp_path), rounds=1, attempts=2)
    assert result["network_requests"] == 0
    assert result["snapshot_parity"]
    assert not result["production_ledger_modified"]
    assert not result["opening_slo_claim"]
    before, after = result["samples"]
    assert before["provider_calls"] == 2
    assert after["provider_calls"] == 1
    assert before["snapshot_sha256"] == after["snapshot_sha256"]
    assert before["valid_open_count"] == after["valid_open_count"] == 0


def test_snapshot_midpoint_fallback_is_never_an_opening_price():
    values = quotes._shioaji_snapshot_values(
        SimpleNamespace(buy_price=100., sell_price=102., close=None, open=None, ts=0),
        SimpleNamespace(reference=99.), received_ms=1,
    )
    assert values["price"] == 101.
    assert values["open"] is None
    assert values["simtrade"] == -1


def test_stream_comparison_keeps_full_axis_and_all_prices_without_network(monkeypatch, tmp_path):
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", None)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE_KEY", None)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE", {})
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_REQUESTED", set())
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_DIRTY", False)
    monkeypatch.setattr(benchmark.acquisition, "configured_opening_providers", lambda *a, **k: (_ for _ in ()).throw(AssertionError("real source not allowed")))
    result = benchmark.compare_stream_priority([str(1000 + i) for i in range(25)], str(tmp_path),
        rounds=1, snapshot_delay_seconds=.005, evidence_dir=tmp_path / "evidence")
    assert result["network_requests"] == 0
    assert not result["production_ledger_modified"]
    assert not result["opening_slo_claim"]
    before, after = result["samples"]
    assert before["snapshot_requested_rows"] == 25
    assert after["snapshot_requested_rows"] == 25 - after["native_fixture_open_count"]
    assert before["final_positive_opens"] == after["final_positive_opens"] == 25
    assert before["opening_price_sha256"] == after["opening_price_sha256"]
    assert len(list((tmp_path / "evidence").glob("*.acquisition.jsonl"))) == 2


def test_mis_probe_does_not_burst_after_failed_single_symbol_prewarm(monkeypatch, tmp_path):
    session_date = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    source, target = tmp_path / "source.json", tmp_path / "probe.json"
    source.write_text(json.dumps({"session_date": session_date, "requested_symbols": ["2330"],
        "rows": {}, "parquet_root": str(tmp_path)}))
    monkeypatch.setattr(sys, "argv", ["probe", "--receipt", str(source), "--output", str(target),
                                     "--rounds", "1", "--mis-only"])
    def refused():
        raise requests.ConnectionError(RemoteDisconnected("secret never persists"))
    monkeypatch.setattr(quotes, "warm_tw_mis_quote_client", refused)
    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", lambda *_a, **_k: pytest.fail("refused source must not receive a burst"))
    monkeypatch.setattr(benchmark.acquisition, "configured_opening_providers", lambda *_: pytest.fail("MIS-only probe must not use broker"))
    assert benchmark.main() == 0
    result = json.loads(target.read_text())
    assert result["status"] == "waiting_source"
    assert not result["data_complete"]
    assert result["rounds"] == []
    assert result["mis_connection_prewarm"]["error_chain_types"] == ["ConnectionError", "RemoteDisconnected"]
    assert result["mis_connection_prewarm"]["performed"]
    assert result["workflow_timing"]["total_before_final_report_write_ms"] >= result["mis_connection_prewarm"]["elapsed_ms"]
    assert "secret" not in str(result)


def test_mis_retention_keeps_original_body_scope_and_clock_without_request_secrets(monkeypatch, tmp_path):
    now = datetime.now(ZoneInfo("Asia/Taipei"))
    source, output = tmp_path / "scope.json", tmp_path / "result.json"
    source.write_text(json.dumps({"session_date": now.date().isoformat(), "requested_symbols": ["2330"],
        "rows": {}, "parquet_root": str(tmp_path)}))
    monkeypatch.setattr(sys, "argv", ["probe", "--receipt", str(source), "--output", str(output),
        "--rounds", "1", "--mis-only", "--retain-mis-responses"])
    monkeypatch.setattr(quotes, "warm_tw_mis_quote_client", lambda: {"ready": True})
    body = json.dumps({"msgArray": [{"c": "2330", "o": "100.00", "v": "1", "tlong": str(
        int(now.timestamp() * 1000))}]}, separators=(", ", ": ")).encode()

    def provider(names, fallback, **kwargs):
        # Only ex_ch is retained; URL, credentials/cookies and unrelated query
        # arguments must not enter the manifest. No extra HTTP/body read occurs.
        response = SimpleNamespace(status_code=200, request=SimpleNamespace(
            url="https://example.invalid/quote?ex_ch=tse_2330.tw&token=private-test-secret"),
            headers={"Cookie": "private-test-cookie"})
        assert quotes._decode_quote_response(response, body)["msgArray"][0]["o"] == "100.00"
        value = quotes.PriceSnapshot(prices=np.array([100.]), open_prices=np.array([100.]),
            source="official_mis:retained_test_not_market_data", available_mask=np.array([True]),
            available_count=1, requested_count=1, timestamps_ms=np.array([int(now.timestamp() * 1000)]),
            exchange_timestamps_ms=np.array([int(now.timestamp() * 1000)]))
        kwargs["on_progress"](names, value)
        return value

    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", provider)
    assert benchmark.main() == 0
    report = json.loads(output.read_bytes())
    retained = Path(report["retained_response_manifest"])
    manifest = json.loads(retained.read_bytes())
    assert manifest["requested_symbols"] == ["2330"]
    assert len(manifest["responses"]) == 1
    row = manifest["responses"][0]
    assert row["channels"] == "tse_2330.tw" and row["http_status"] == 200
    assert base64.b64decode(row["body_base64"]) == body
    assert row["body_sha256"] == hashlib.sha256(body).hexdigest()
    assert datetime.fromisoformat(row["decoded_at_taipei"]).tzinfo is not None
    assert "private-test" not in retained.read_text() + output.read_text()
    assert report["rounds"][0]["background_drained"]
    assert report["rounds"][0]["readback_coverage"]["positive_open_count"] == 1


@pytest.mark.parametrize("budget", ["4", "181", "nan", "inf"])
def test_source_completion_diagnostic_budget_rejects_unbounded_values(monkeypatch, tmp_path, budget):
    source = tmp_path / "scope.json"
    source.write_text(json.dumps({"requested_symbols": ["2330"], "parquet_root": str(tmp_path)}))
    monkeypatch.setattr(sys, "argv", ["probe", "--receipt", str(source), "--output", str(tmp_path / "unused.json"),
        "--source-drain-timeout", budget])
    with pytest.raises(SystemExit) as exc:
        benchmark.main()
    assert exc.value.code == 2
    assert not (tmp_path / "unused.json").exists()


def test_all_source_probe_skips_refused_mis_but_runs_healthy_sources(monkeypatch, tmp_path):
    now = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=14, minute=0)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz or ZoneInfo("Asia/Taipei"))
    for module in (benchmark, benchmark.acquisition, quotes):
        monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE_KEY", None)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE", {})
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_REQUESTED", set())
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_DIRTY", False)
    source, target = tmp_path / "source.json", tmp_path / "probe.json"
    source.write_text(json.dumps({"session_date": now.date().isoformat(), "requested_symbols": ["2330"],
        "rows": {}, "parquet_root": str(tmp_path)}))
    monkeypatch.setattr(sys, "argv", ["probe", "--receipt", str(source), "--output", str(target), "--rounds", "1"])
    monkeypatch.setattr(benchmark, "load_dotenv", lambda *a, **k: None)
    calls = []
    def healthy(names, values, progress):
        calls.append(names)
        return quotes.PriceSnapshot(prices=np.array([100.]), open_prices=np.array([100.]),
            source="engineering", available_mask=np.array([True]), available_count=1, requested_count=1,
            timestamps_ms=np.array([int(now.timestamp() * 1000)], dtype=np.int64),
            exchange_timestamps_ms=np.array([int(now.timestamp() * 1000)], dtype=np.int64))
    def refused():
        raise requests.ConnectionError(RemoteDisconnected("no burst"))
    monkeypatch.setattr(quotes, "warm_tw_mis_quote_client", refused)
    monkeypatch.setattr(benchmark.acquisition, "configured_opening_providers", lambda *_: {
        "official_mis": lambda *a: pytest.fail("refused source must not receive a burst"),
        "shioaji_shared": healthy, "finmind_snapshot": healthy})
    assert benchmark.main() == 0
    result = json.loads(target.read_text())
    assert calls == [["2330"], ["2330"]]
    assert result["providers_dispatch_skipped"] == {"official_mis": "failed_single_symbol_transport_probe"}
    assert result["rounds"][0]["final_positive_opens"] == 1
    assert result["rounds"][0]["providers"]["official_mis"]["state"] == "failed"
    assert result["mis_prewarm_dispatch"] == "independent_provider_worker_not_global_barrier"
    assert not result["broker_order_submitted"]


def test_slow_mis_transport_proof_never_holds_up_healthy_source(monkeypatch, tmp_path):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    now = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=14, minute=0)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz or ZoneInfo("Asia/Taipei"))

    for module in (benchmark, benchmark.acquisition, quotes):
        monkeypatch.setattr(module, "datetime", Clock)
    for key, value in (("_TW_MIS_OPENING_CACHE_KEY", None), ("_TW_MIS_OPENING_CACHE", {}),
                       ("_TW_MIS_OPENING_REQUESTED", set()), ("_TW_MIS_OPENING_DIRTY", False)):
        monkeypatch.setattr(quotes, key, value)
    source, target = tmp_path / "source.json", tmp_path / "probe.json"
    source.write_text(json.dumps({"session_date": now.date().isoformat(), "requested_symbols": ["2330", "0050"],
        "rows": {}, "parquet_root": str(tmp_path)}))
    monkeypatch.setattr(sys, "argv", ["probe", "--receipt", str(source), "--output", str(target), "--rounds", "1"])
    monkeypatch.setattr(benchmark, "load_dotenv", lambda *a, **k: None)
    probe_started, release_probe, healthy_started = threading.Event(), threading.Event(), threading.Event()

    def slow_proof():
        probe_started.set()
        assert release_probe.wait(timeout=5)
        raise requests.ConnectionError(RemoteDisconnected("private source diagnostic"))

    def healthy(names, values, progress):
        assert probe_started.wait(timeout=1)
        healthy_started.set()
        return quotes.PriceSnapshot(prices=np.array([100., np.nan]), open_prices=np.array([100., np.nan]),
            source="engineering", available_mask=np.array([True, False]), available_count=1, requested_count=2,
            timestamps_ms=np.array([int(now.timestamp() * 1000), 0], dtype=np.int64),
            exchange_timestamps_ms=np.array([int(now.timestamp() * 1000), 0], dtype=np.int64))

    monkeypatch.setattr(quotes, "warm_tw_mis_quote_client", slow_proof)
    monkeypatch.setattr(benchmark.acquisition, "configured_opening_providers", lambda *_: {
        "official_mis": lambda *a: pytest.fail("failed probe must not lead to a burst"),
        "finmind_snapshot": healthy})
    with ThreadPoolExecutor(max_workers=1) as pool:
        probe = pool.submit(benchmark.main)
        try:
            assert healthy_started.wait(timeout=1), "independent source waited for MIS transport proof"
            assert not release_probe.is_set()
        finally:
            release_probe.set()
        assert probe.result(timeout=3) == 0
    result = json.loads(target.read_text())
    assert result["status"] == "partial" and not result["data_complete"]
    assert not result["resolved_complete"]
    sample = result["rounds"][0]
    assert sample["final_positive_opens"] == sample["unknown_open_count"] == 1
    assert not sample["quorum_reached"] and not sample["data_complete"]
    assert result["mis_connection_prewarm"]["performed"]
    assert "private source diagnostic" not in target.read_text()


def test_combined_network_rounds_require_fresh_finmind_response_not_decoded_cache(monkeypatch, tmp_path):
    now = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=14, minute=0)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz or ZoneInfo("Asia/Taipei"))

    for module in (benchmark, benchmark.acquisition, quotes):
        monkeypatch.setattr(module, "datetime", Clock)
    for key, value in (("_TW_MIS_OPENING_CACHE_KEY", None), ("_TW_MIS_OPENING_CACHE", {}),
                       ("_TW_MIS_OPENING_REQUESTED", set()), ("_TW_MIS_OPENING_DIRTY", False)):
        monkeypatch.setattr(quotes, key, value)
    source, target = tmp_path / "source.json", tmp_path / "probe.json"
    source.write_text(json.dumps({"session_date": now.date().isoformat(), "requested_symbols": ["2330"],
        "rows": {}, "parquet_root": str(tmp_path)}))
    monkeypatch.setattr(sys, "argv", ["probe", "--receipt", str(source), "--output", str(target), "--rounds", "2"])
    monkeypatch.setattr(benchmark, "load_dotenv", lambda *a, **k: None)
    forced = []

    def finmind(names, values, progress, *, force_network=False):
        forced.append(force_network)
        return quotes.PriceSnapshot(prices=np.array([100.]), open_prices=np.array([100.]),
            source="engineering", available_mask=np.array([True]), available_count=1, requested_count=1,
            timestamps_ms=np.array([int(now.timestamp() * 1000)], dtype=np.int64),
            exchange_timestamps_ms=np.array([int(now.timestamp() * 1000)], dtype=np.int64))

    monkeypatch.setattr(benchmark.acquisition, "fetch_finmind_opening_prices", finmind)
    monkeypatch.setattr(benchmark.acquisition, "configured_opening_providers", lambda *_: {"finmind_snapshot": finmind})
    monkeypatch.setattr(quotes, "warm_tw_mis_quote_client", lambda: pytest.fail("no MIS source configured"))
    assert benchmark.main() == 0
    assert forced == [True, True]
    result = json.loads(target.read_text())
    assert result["data_complete"] and len(result["rounds"]) == 2
    assert result["finmind_probe_policy"] == "fresh_http_per_round_preserve_provider_error_backoff"


def test_probe_full_coverage_requires_independent_durable_readback_not_only_cache(monkeypatch, tmp_path):
    now = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=14, minute=0)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz or ZoneInfo("Asia/Taipei"))

    for module in (benchmark, benchmark.acquisition, quotes):
        monkeypatch.setattr(module, "datetime", Clock)
    for key, value in (("_TW_MIS_OPENING_CACHE_KEY", None), ("_TW_MIS_OPENING_CACHE", {}),
                       ("_TW_MIS_OPENING_REQUESTED", set()), ("_TW_MIS_OPENING_DIRTY", False)):
        monkeypatch.setattr(quotes, key, value)
    names = ["2330", "0050", "2317"]
    source, target = tmp_path / "source.json", tmp_path / "probe.json"
    source.write_text(json.dumps({"session_date": now.date().isoformat(), "requested_symbols": names,
        "rows": {}, "parquet_root": str(tmp_path)}))
    monkeypatch.setattr(sys, "argv", ["probe", "--receipt", str(source), "--output", str(target),
                                     "--rounds", "1", "--finmind-only"])
    monkeypatch.setattr(benchmark, "load_dotenv", lambda *a, **k: None)

    def complete_source(names, values, progress, **kwargs):
        return quotes.PriceSnapshot(prices=np.ones(3) * 100, open_prices=np.ones(3) * 100,
            source="engineering", available_mask=np.ones(3, dtype=bool), available_count=3, requested_count=3,
            timestamps_ms=np.full(3, int(now.timestamp() * 1000), dtype=np.int64),
            exchange_timestamps_ms=np.full(3, int(now.timestamp() * 1000), dtype=np.int64))

    writer = quotes._persist_tw_mis_opening_receipt

    def partial_writer(**kwargs):
        # Fault injection: the cache is complete but durable storage lost two
        # rows. Even a nominally successful writer must not earn full coverage.
        return writer(**{**kwargs, "rows": {name: row for name, row in kwargs["rows"].items() if name == "2330"}})

    monkeypatch.setattr(benchmark.acquisition, "fetch_finmind_opening_prices", complete_source)
    monkeypatch.setattr(quotes, "_persist_tw_mis_opening_receipt", partial_writer)
    assert benchmark.main() == 0
    result = json.loads(target.read_text())
    sample = result["rounds"][0]
    assert sample["volatile_final_positive_opens"] == 3 and sample["quorum_reached"]
    assert sample["durable_receipt_retained"] and sample["final_positive_opens"] == 1
    assert sample["unknown_open_count"] == 2 and not sample["data_complete"]
    assert result["status"] == "partial" and not result["resolved_complete"]
