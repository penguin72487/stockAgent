from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime
from http.client import RemoteDisconnected
import base64
import hashlib
import json
import shutil
import sys
from zoneinfo import ZoneInfo

import numpy as np
import pytest
import requests

from scripts import benchmark_tw_opening_sources as benchmark
from stockagent.live import quote_provider as quotes


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


def test_retained_mis_pipeline_keeps_all_prices_clocks_and_durable_rows(monkeypatch, tmp_path):
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
    result = benchmark.compare_mis_pipeline(["2330"], tmp_path, baseline, retained, rounds=2)
    assert clock_checks
    assert result["financial_and_clock_parity"] and result["durable_rows_parity"]
    assert result["network_requests"] == 0 and not result["opening_slo_claim"]
    assert not result["production_ledger_modified"]
    assert [row["variant"] for row in result["samples"]] == ["before", "after", "after", "before"]
    assert all(row["request_count"] == row["valid_open_count"] == row["durable_requested_count"] == 1
               for row in result["samples"])
    corrupt = json.loads(retained.read_bytes())
    corrupt["responses"][0]["body_sha256"] = "bad"
    retained.write_text(json.dumps(corrupt))
    with pytest.raises(ValueError, match="integrity"):
        benchmark.compare_mis_pipeline(["2330"], tmp_path, baseline, retained, rounds=1)


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
        kwargs["progress_callback"]({"message": "quote_transport"})
        return SimpleNamespace(snapshot=quotes.PriceSnapshot(prices=np.array([100.]), source="receipt_fixture",
            open_prices=np.array([99.]), available_mask=np.array([True]),
            exchange_timestamps_ms=np.array([1])), request_key="canonical-input")

    monkeypatch.setattr(signal_engine, "prefetch_live_signal_prices", prefetch)
    result = preparation.input_preparation_probe([Config("a"), Config("b")], repeats=2)
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


def test_callback_pipeline_comparison_is_exact_and_never_logs_in(monkeypatch, tmp_path):
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", None)
    monkeypatch.setattr(quotes, "_shioaji_stock_api", lambda: pytest.fail("offline comparison must not log in"))
    captured = tmp_path / "before.py"
    shutil.copyfile(quotes.__file__, captured)
    result = benchmark.compare_callback_pipeline([str(1000 + i) for i in range(25)], captured, rounds=2)
    assert result["snapshot_parity"] and result["progress_parity"] and result["durable_rows_parity"]
    assert result["network_requests"] == 0
    assert not result["opening_slo_claim"]
    assert not result["production_ledger_modified"]
    assert result["baseline_code_sha256"] == result["candidate_code_sha256"]
    assert [row["variant"] for row in result["samples"]] == ["before", "after", "after", "before"]
    assert all(row["snapshot_decode_count"] == 25 for row in result["samples"])
    assert all(row["initial_receipt_writes"] == 1 and row["unchanged_receipt_writes"] == 0 for row in result["samples"])
    assert all(row["initial_storage_barriers"] == {"file": 1, "directory": 1} for row in result["samples"])


def test_retained_finmind_comparison_keeps_full_financial_clock_and_scope_parity(monkeypatch, tmp_path):
    now = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=9, minute=0, second=0, microsecond=0)
    baseline, quote_baseline, payload = tmp_path / "before.py", tmp_path / "quotes.py", tmp_path / "body.json"
    shutil.copyfile(benchmark.acquisition.__file__, baseline)
    shutil.copyfile(quotes.__file__, quote_baseline)
    payload.write_text(json.dumps({"status": 200, "data": [{"stock_id": symbol, "open": 100 + i,
        "date": now.isoformat(), "total_volume": 1} for i, symbol in enumerate("AB")]}))
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", None)
    result = benchmark.compare_finmind_decoder(list("AB"), tmp_path, baseline, payload,
        rounds=1, baseline_quotes_path=quote_baseline)
    assert result["financial_and_clock_parity"]
    assert result["network_requests"] == 0 and not result["opening_slo_claim"]
    assert all(row["valid_open_count"] == row["durable_requested_count"] == 2 for row in result["samples"])
    assert all(row["quorum_reached"] for row in result["samples"])
    assert result["baseline_quote_owner_sha256"] == result["candidate_quote_owner_sha256"]


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
    assert "secret" not in str(result)


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
    assert not result["broker_order_submitted"]
