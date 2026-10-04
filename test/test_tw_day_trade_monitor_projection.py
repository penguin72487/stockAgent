from __future__ import annotations

import json
from pathlib import Path

from stockagent.live.tw_day_trade_monitor_projection import (
    build_shioaji_monitor_projection,
    validated_shioaji_monitor_projection,
)


def _state(revision: int = 7) -> dict:
    return {
        "state_revision": revision,
        "modes": {"a": {}, "b": {}},
        "benchmarks": {
            "0050": {"source": "local", "last_quote_at": "2026-09-26T01:00:00Z"},
            "TX": {"source": "shioaji:fop", "last_quote_at": "2026-09-26T01:01:00Z"},
            "TX2": {"source": "shioaji:fop", "last_quote_at": "2026-09-26T01:02:00Z"},
        },
    }


def _status(projection: dict | None, revision: int = 7) -> dict:
    return {
        "state_revision": revision,
        "simulation_only": True,
        "production_order_possible": False,
        "shioaji_monitor_projection": projection,
    }


def test_projection_preserves_full_state_monitor_counts_and_quote_times(tmp_path: Path):
    path = tmp_path / "state.json"
    state = _state()
    path.write_text(json.dumps(state), encoding="utf-8")
    projection = build_shioaji_monitor_projection(state, path, state_revision=7)
    assert projection is not None
    assert projection["mode_count"] == 2
    assert projection["benchmark_count"] == 3
    assert projection["source_count"] == 1
    assert projection["quote_times"] == [
        "2026-09-26T01:01:00Z", "2026-09-26T01:02:00Z",
    ]
    assert validated_shioaji_monitor_projection(_status(projection), path) == projection


def test_projection_rejects_generation_mismatch_and_atomic_replace(tmp_path: Path):
    path = tmp_path / "state.json"
    state = _state()
    path.write_text(json.dumps(state), encoding="utf-8")
    assert build_shioaji_monitor_projection(state, path, state_revision=8) is None
    projection = build_shioaji_monitor_projection(state, path, state_revision=7)
    assert projection is not None
    assert validated_shioaji_monitor_projection(_status(projection, 8), path) is None
    replacement = tmp_path / "replacement.json"
    replacement.write_text(json.dumps(_state(8)), encoding="utf-8")
    replacement.replace(path)
    assert validated_shioaji_monitor_projection(_status(projection), path) is None


def test_projection_rejects_unsafe_status_or_missing_state(tmp_path: Path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps(_state()), encoding="utf-8")
    projection = build_shioaji_monitor_projection(_state(), path, state_revision=7)
    assert projection is not None
    assert validated_shioaji_monitor_projection({**_status(projection), "simulation_only": False}, path) is None
    assert validated_shioaji_monitor_projection({**_status(projection), "production_order_possible": True}, path) is None
    assert validated_shioaji_monitor_projection(_status({**projection, "benchmark_count": True}), path) is None
    path.unlink()
    assert validated_shioaji_monitor_projection(_status(projection), path) is None
