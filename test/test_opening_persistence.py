from datetime import datetime
from decimal import Decimal
import json
import os
import stat

import pytest

from scripts.benchmark_day_trade_persistence import benchmark, benchmark_journal
from stockagent.live import tw_day_trade_simulation as simulation


def test_complete_publication_codec_comparison_preserves_values_ack_and_source(tmp_path):
    from stockagent import private_json
    if not private_json.native_available():
        pytest.skip("optional native reference unavailable")
    engine = simulation.TwDayTradeSimulationEngine(tmp_path / "source")
    engine._persist(datetime.fromisoformat("2026-10-27T15:00:00+08:00"))
    source = engine.state_path.read_bytes()
    report = benchmark(engine.state_path, repeats=2, output=tmp_path / "result.json", ledger_codec=True)
    assert report["comparison"] == "full_publication_guarded_native_vs_stdlib_ledger_v1"
    assert report["full_publication_values_identical"]
    assert report["post_fsync_ack_verified_each_sample"]
    assert [row["variant"] for row in report["samples"]] == [
        "guarded_native_ledger", "stdlib_ledger", "stdlib_ledger", "guarded_native_ledger"]
    assert len({row["contract_sha256"] for row in report["samples"]}) == 1
    assert engine.state_path.read_bytes() == source


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


def test_batch_append_keeps_exact_bytes_one_file_sync_and_no_partial_encode(tmp_path, monkeypatch):
    path = tmp_path / "signals.jsonl"
    path.write_bytes(b'{"old":1}\n')
    rows = [{"symbol": str(i), "price": -0., "shares": i * 1000} for i in range(2764)]
    expected = path.read_bytes() + b"".join(simulation.ledger_bytes(row) + b"\n" for row in rows)
    calls, sync = [], os.fsync
    def counted_sync(fd):
        calls.append(stat.S_ISREG(os.fstat(fd).st_mode))
        sync(fd)
    monkeypatch.setattr(os, "fsync", counted_sync)
    simulation._append_jsonl_many(path, rows)
    assert path.read_bytes() == expected
    assert calls == [True]
    before = path.read_bytes()
    with pytest.raises(UnicodeEncodeError):
        simulation._append_jsonl_many(path, [rows[0], {"bad": "\ud800"}])
    assert path.read_bytes() == before and calls == [True]


def test_paired_journal_benchmark_keeps_source_and_retained_prefix(tmp_path):
    path = tmp_path / "input.jsonl"
    path.write_text('{"price":-0.0,"count":18446744073709551617}\n\n{"名稱":"台灣"}\n')
    before = path.read_bytes()
    result = benchmark_journal(path, repeats=2, output=tmp_path / "output/result.json")
    assert result["source_rows"] == 2
    assert result["exact_bytes_and_prefix_equal"] is True
    assert result["state_commit_directory_barriers_not_measured"] is True
    assert [row["variant"] for row in result["samples"]] == [
        "per_record_encoding", "batch_encoding", "batch_encoding", "per_record_encoding"]
    assert all(row["file_fsync_count"] == 1 for row in result["samples"])
    assert path.read_bytes() == before


@pytest.mark.parametrize("raw", [b"", b"null\n", b'{}\n{"broken":', b'{}\r{}\n'])
def test_journal_benchmark_rejects_incomplete_or_untyped_input(tmp_path, raw):
    path = tmp_path / "input.jsonl"
    path.write_bytes(raw)
    with pytest.raises(ValueError):
        benchmark_journal(path, repeats=1, output=tmp_path / "out.json")


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_journal_benchmark_refuses_nonregular_source_without_blocking(tmp_path, kind):
    path = tmp_path / "input.jsonl"
    if kind == "symlink":
        target = tmp_path / "target.jsonl"
        target.write_bytes(b'{}\n')
        path.symlink_to(target)
    else:
        os.mkfifo(path)
    with pytest.raises((ValueError, OSError)):
        benchmark_journal(path, repeats=1, output=tmp_path / "out.json")


@pytest.mark.parametrize("probe", [benchmark, benchmark_journal])
@pytest.mark.parametrize("alias", ["same_path", "symlink", "hardlink"])
def test_benchmark_output_cannot_overwrite_any_source_alias(tmp_path, probe, alias):
    source = tmp_path / "source.json"
    source.write_bytes(b'{"retained":true}\n')
    output = source
    if alias != "same_path":
        output = tmp_path / "alias.json"
        if alias == "symlink":
            output.symlink_to(source)
        else:
            os.link(source, output)
    with pytest.raises(ValueError, match="must not overwrite"):
        probe(source, repeats=1, output=output)
    assert source.read_bytes() == b'{"retained":true}\n'


def test_full_persist_measures_four_durable_views_and_fails_closed(tmp_path, monkeypatch):
    engine = simulation.TwDayTradeSimulationEngine(tmp_path)
    now = datetime.fromisoformat("2026-10-06T09:00:00+08:00")
    engine._persist(now)
    metrics = engine.last_persist_metrics
    assert metrics["state_revision"] == engine.state["state_revision"]
    assert {item["file"] for item in metrics["files"]} == {
        "state.json", "positions.json", "status.json", "service_sync.json", "service_sync_durable_ack.json"}
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
    assert [row["variant"] for row in result["samples"]] == [
        "stdlib_fingerprint", "guarded_native_fingerprint", "guarded_native_fingerprint", "stdlib_fingerprint"]
    assert len({row["contract_sha256"] for row in result["samples"]}) == 1
    assert result["monitor_binding_verified_each_sample"] is True
    assert result["projection_and_journal_generations_verified_each_sample"] is True
    assert result["post_fsync_ack_verified_each_sample"] is True
    assert len({row["logical_fingerprint_input_sha256"] for row in result["samples"]}) == 1
    assert result["ledger_writes"] == 0 and result["production_writes"] is False
    assert engine.state_path.read_bytes() == before
    assert not engine.signals_path.exists() and not engine.orders_path.exists() and not engine.fills_path.exists()
    assert result["file_fsync_preserved"] and result["directory_fsync_preserved"]


def test_complete_key_predicate_benchmark_keeps_full_values_barriers_and_source(tmp_path):
    engine = simulation.TwDayTradeSimulationEngine(tmp_path / "source")
    engine._persist(datetime.fromisoformat("2026-10-06T09:00:00+08:00"))
    before = engine.state_path.read_bytes()
    report = benchmark(engine.state_path, repeats=2, output=tmp_path / "out/result.json", key_guard=True)
    assert report["comparison"] == "exact_ledger_key_guard_complete_publication_v1"
    assert report["post_fsync_ack_verified_each_sample"]
    assert report["full_publication_values_identical"]
    assert [row["variant"] for row in report["samples"]] == [
        "legacy_python_key_generator", "exact_type_identity_key_guard",
        "exact_type_identity_key_guard", "legacy_python_key_generator"]
    assert len({row["contract_sha256"] for row in report["samples"]}) == 1
    assert engine.state_path.read_bytes() == before
    assert report["ledger_writes"] == 0 and report["production_writes"] is False
    assert not engine.signals_path.exists() and not engine.orders_path.exists() and not engine.fills_path.exists()
    assert report["file_fsync_preserved"] and report["directory_fsync_preserved"]


def test_benchmark_rejects_non_paper_source(tmp_path):
    path = tmp_path / "state.json"
    path.write_text('{"simulation_only":false,"production_order_possible":true}')
    with pytest.raises(ValueError, match="paper simulation"):
        benchmark(path, repeats=1, output=tmp_path / "result.json")


@pytest.mark.parametrize("value", [1., -0., 5e-324, 2**128, {"名稱": "台積電"}, float("inf")])
def test_private_material_fingerprint_preserves_changes_and_sorted_keys(value):
    before = {"contract": "dashboard_material_without_private_order_templates_v2", "value": None}
    changed = dict(before, value=value)
    assert simulation._dashboard_material_fingerprint(before) != simulation._dashboard_material_fingerprint(changed)
    assert simulation._dashboard_material_fingerprint(changed) == simulation._dashboard_material_fingerprint(
        dict(reversed(list(changed.items()))))


def test_full_persist_keeps_material_revision_stable_and_observes_nested_changes(tmp_path):
    engine = simulation.TwDayTradeSimulationEngine(tmp_path)
    clock = datetime.fromisoformat("2026-10-06T09:00:00+08:00")
    engine.state["modes"]["fixture"] = {"market": "fixture", "target_risk": {"gross": .1}}
    engine._persist(clock)
    original = engine.state["dashboard_content_revision"]
    engine._persist(clock)
    assert engine.state["dashboard_content_revision"] == original
    engine.state["modes"]["fixture"]["target_risk"]["gross"] = .2
    engine._persist(clock)
    assert engine.state["dashboard_content_revision"] == original + 1


def test_benchmark_detects_generation_corruption_instead_of_normalizing_it(tmp_path, monkeypatch):
    from scripts import benchmark_day_trade_persistence as module
    from stockagent.live.tw_day_trade_service_sync import PaperReadChanged
    engine = simulation.TwDayTradeSimulationEngine(tmp_path / "source")
    engine._persist(datetime.fromisoformat("2026-10-06T09:00:00+08:00"))
    original = module._publication_contract

    def corrupt_then_verify(engine, **kwargs):
        engine.positions_path.write_text("{}")
        return original(engine, **kwargs)

    monkeypatch.setattr(module, "_publication_contract", corrupt_then_verify)
    with pytest.raises(PaperReadChanged):
        benchmark(engine.state_path, repeats=1, output=tmp_path / "benchmark" / "result.json")
