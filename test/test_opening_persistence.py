from datetime import datetime
from decimal import Decimal
import json
import os
import stat

import pytest

from scripts.benchmark_day_trade_persistence import benchmark
from stockagent.live import tw_day_trade_simulation as simulation


def test_compact_atomic_json_preserves_precision_unicode_and_durability(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    payload = {"台積電": [9007199254740993, -0.0, 0.12345678901234568],
               "decimal": Decimal("101.123456789"), "clock": datetime(2026, 10, 6, 9),
               "active": True, "missing": None}
    events = []
    fsync, replace = os.fsync, os.replace

    def counted_sync(fd):
        events.append("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
        fsync(fd)

    def counted_replace(source, dest):
        events.append("replace")
        replace(source, dest)

    monkeypatch.setattr(os, "fsync", counted_sync)
    monkeypatch.setattr(os, "replace", counted_replace)
    timing = {}
    simulation._atomic_json(path, payload, timings=timing)
    raw = path.read_text()
    assert json.loads(raw) == json.loads(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    assert raw.count("\n") == 1
    assert "台積電" in raw
    assert "9007199254740993" in raw and "-0.0" in raw
    assert events == ["file", "replace", "directory"]
    assert timing["file"] == path.name
    assert timing["total_ms"] >= 0
    assert all(timing[key] >= 0 for key in ("json_encode_ms", "file_write_fsync_ms", "replace_directory_fsync_ms"))


@pytest.mark.parametrize("failure", ["encode", "file_fsync"])
def test_failed_publication_preserves_previous_state_and_no_success_timing(tmp_path, monkeypatch, failure):
    path = tmp_path / "state.json"
    path.write_text('{"original":true}\n')
    payload = {}
    if failure == "encode":
        payload["cycle"] = payload
        error = ValueError
    else:
        def fail(fd):
            raise OSError("sync failed")
        monkeypatch.setattr(os, "fsync", fail)
        error = OSError
    timing = {}
    with pytest.raises(error):
        simulation._atomic_json(path, payload, timings=timing)
    assert path.read_text() == '{"original":true}\n'
    assert list(tmp_path.glob("*.tmp.*")) == []
    assert timing == {}


def test_full_persist_measures_four_durable_views_and_fails_closed(tmp_path, monkeypatch):
    engine = simulation.TwDayTradeSimulationEngine(tmp_path)
    now = datetime.fromisoformat("2026-10-06T09:00:00+08:00")
    engine._persist(now)
    metrics = engine.last_persist_metrics
    assert metrics["state_revision"] == engine.state["state_revision"]
    assert {item["file"] for item in metrics["files"]} == {
        "state.json", "positions.json", "status.json", "service_sync.json"}
    assert all(item["total_ms"] >= 0 for item in metrics["files"])
    previous_sync = engine.service_sync_path.read_bytes()

    def fail(fd):
        raise OSError("sync failed")

    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError):
        engine._persist(now)
    assert engine.last_persist_metrics is None
    assert engine.service_sync_path.read_bytes() == previous_sync


def test_paired_benchmark_keeps_all_data_and_never_writes_source_ledgers(tmp_path):
    engine = simulation.TwDayTradeSimulationEngine(tmp_path / "source")
    engine._persist(datetime.fromisoformat("2026-10-06T09:00:00+08:00"))
    before = engine.state_path.read_bytes()
    output = tmp_path / "benchmark" / "result.json"
    result = benchmark(engine.state_path, repeats=2, output=output)
    assert len(result["samples"]) == 4
    assert [row["variant"] for row in result["samples"]] == ["indented", "compact", "compact", "indented"]
    assert len({row["contract_sha256"] for row in result["samples"]}) == 1
    assert result["monitor_binding_verified_each_sample"] is True
    assert result["ledger_writes"] == 0 and result["production_writes"] is False
    assert engine.state_path.read_bytes() == before
    assert not engine.signals_path.exists() and not engine.orders_path.exists() and not engine.fills_path.exists()
    assert result["file_fsync_preserved"] and result["directory_fsync_preserved"]


def test_benchmark_rejects_non_paper_source(tmp_path):
    path = tmp_path / "state.json"
    path.write_text('{"simulation_only":false,"production_order_possible":true}')
    with pytest.raises(ValueError, match="paper simulation"):
        benchmark(path, repeats=1, output=tmp_path / "result.json")
