from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime
from http.client import RemoteDisconnected
import json
import shutil
import sys
from zoneinfo import ZoneInfo

import numpy as np
import pytest
import requests

from scripts import benchmark_tw_opening_sources as benchmark
from stockagent.live import quote_provider as quotes


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
