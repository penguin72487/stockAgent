from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts import benchmark_tw_day_trade_compact_status as benchmark


def _index(tmp_path: Path):
    source = tmp_path / "orders.jsonl"
    source.write_text('{"session_date":"2026-09-24"}\n', encoding="utf-8")
    signature = benchmark._signature(source)
    cache = tmp_path / "cache"
    cache.mkdir()
    identity = f"{source.resolve()}\0recorded_at_fallback=0"
    name = f"ledger-session-index-v1-{hashlib.sha256(identity.encode()).hexdigest()}.json"
    metadata = {
        "schema_version": 1, "source": str(source.resolve()),
        "recorded_at_fallback": False, "device": signature[0], "inode": signature[1],
        "observed_size": signature[2], "modified_ns": signature[3],
        "scanned_offset": signature[2], "spans": {"2026-09-24": [[0, signature[2]]]},
    }
    (cache / name).write_text(json.dumps(metadata), encoding="utf-8")
    return source, cache, name, metadata


def test_benchmark_requires_exact_existing_index(tmp_path: Path) -> None:
    source, cache, name, _metadata = _index(tmp_path)
    assert benchmark._validated_index(source, cache, False, "2026-09-24") == (
        name, (cache / name).read_bytes(),
    )
    source.write_text("changed\n", encoding="utf-8")
    with pytest.raises(ValueError, match="stale index"):
        benchmark._validated_index(source, cache, False, "2026-09-24")


@pytest.mark.parametrize("field,value", (
    ("scanned_offset", 0), ("recorded_at_fallback", True),
    ("source", "/wrong-source"), ("schema_version", 2),
))
def test_benchmark_rejects_untrusted_index(
    tmp_path: Path, field: str, value: object,
) -> None:
    source, cache, name, metadata = _index(tmp_path)
    metadata[field] = value
    (cache / name).write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="stale index"):
        benchmark._validated_index(source, cache, False, "2026-09-24")


def test_benchmark_bounds_selected_session_bytes(tmp_path: Path, monkeypatch) -> None:
    source, cache, _name, _metadata = _index(tmp_path)
    monkeypatch.setattr(benchmark, "MAX_SESSION_BYTES", 1)
    with pytest.raises(ValueError, match="read exceeds"):
        benchmark._validated_index(source, cache, False, "2026-09-24")


def test_benchmark_parity_normalizes_only_mapping_key_order() -> None:
    first = {"health": "degraded", "rows": [{"a": 1, "b": 2}, {"a": 3}]}
    equivalent = {"rows": [{"b": 2, "a": 1}, {"a": 3}], "health": "degraded"}
    assert benchmark._digest(first) == benchmark._digest(equivalent)
    assert benchmark._digest(first) != benchmark._digest({
        **equivalent, "rows": list(reversed(equivalent["rows"])),
    })
    assert benchmark._digest(first) != benchmark._digest({**equivalent, "health": "ok"})


def test_benchmark_rejects_baseline_code_hash_before_import(tmp_path: Path) -> None:
    source = tmp_path / "baseline.py"
    source.write_text('raise AssertionError("must not execute unpinned source")\n')
    with pytest.raises(ValueError, match="SHA mismatch"):
        benchmark._worker({"baseline_module": str(source), "baseline_sha256": "0" * 64})


def test_benchmark_module_ab_uses_variant_not_order_fill_flag() -> None:
    workers = []
    for variant, wall in (("A", 20_000_000), ("B", 10_000_000)):
        workers.append({"measurements": [
            {
                "variant": variant, "include_order_fill_rows": False,
                "cache_class": kind, "wall_ns": wall, "cpu_ns": wall,
                "payload_sha256": "same", "canonical_payload_sha256": "same",
                "top_level_field_sha256": {"health": "same"},
            }
            for kind in ("fresh_module_first_call", "same_module_steady")
        ]})
    summary = benchmark._summary(workers)
    assert summary["canonical_payload_parity"] is True
    assert summary["timings"]["A"]["same_module_steady"]["wall_median_ms"] == 20
    assert summary["timings"]["B"]["same_module_steady"]["wall_median_ms"] == 10


def test_benchmark_worker_never_reindexes_or_loads_full_history(tmp_path: Path, monkeypatch) -> None:
    from stockagent.live import tw_day_trade_dashboard as dashboard

    # The worker monkeypatches only its own process in real usage. Restore these
    # bindings after this in-process fixture so adjacent dashboard tests are safe.
    for name in ("_ledger_line_session_date", "_benchmark_history_index", "_rows_for_sessions",
                 "_columnar_ledger_frame", "_latest_contiguous_session_rows", "_object",
                 "DEFAULT_OPENING_GATE_PATH"):
        monkeypatch.setattr(dashboard, name, getattr(dashboard, name))
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(tmp_path))
    seen = []

    def snapshot(**kwargs):
        seen.append(kwargs)
        with pytest.raises(RuntimeError, match="whole-ledger"):
            dashboard._ledger_line_session_date(b"{}", recorded_at_fallback=True)
        with pytest.raises(RuntimeError, match="whole-ledger"):
            dashboard._benchmark_history_index(tmp_path)
        return {"simulation_only": True, "production_order_possible": False,
                "orders": [], "fills": [], "health": "degraded"}

    monkeypatch.setattr(dashboard, "build_dashboard_snapshot", snapshot)
    config = {name: str(tmp_path / f"{name}.json")
              for name in ("root", "cache", "preopen", "discord", "guardian", "gate")}
    result = benchmark._worker({
        **config, "include": False, "session": "2026-09-24",
        "now": "2026-09-27T00:00:00+00:00", "iterations": 2,
    })
    assert [row["cache_class"] for row in result["measurements"]] == [
        "fresh_module_first_call", "same_module_steady",
    ]
    assert len({row["canonical_payload_sha256"] for row in result["measurements"]}) == 1
    assert all(row["include_order_fill_rows"] is False for row in seen)
    assert all(row["include_ledger_session_dates"] is False for row in seen)
