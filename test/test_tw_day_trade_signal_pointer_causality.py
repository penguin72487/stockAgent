from datetime import datetime
import json
import os
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts import run_tw_day_trade_simulation as runner

NOW = datetime(2026, 10, 28, 9, 0, 15, tzinfo=ZoneInfo("Asia/Taipei"))


@pytest.mark.parametrize("hour,minute,expected", [
    (8, 59, False), (9, 0, True), (13, 19, True), (13, 20, False), (22, 20, False),
])
def test_signal_intake_stops_at_exit_cutoff(hour, minute, expected):
    assert runner._signal_intake_window_open(NOW.replace(hour=hour, minute=minute)) is expected


def _signal(tmp_path):
    root = tmp_path / "signals"
    directory = root / "2026-10-28" / "signal-1"
    directory.mkdir(parents=True)
    summary_path = directory / "summary.json"
    execution_path = directory / "execution_weights.json"
    ready = "2026-10-28T09:00:05+08:00"
    published = "2026-10-28T09:00:06+08:00"
    summary_path.write_text(json.dumps({"market": "m", "signal_id": "signal-1",
        "asof_date": "2026-10-28", "execution_mode": "tw_day_trade", "signal_ready_at": ready,
        "artifact_published_at": published}))
    execution_path.write_text(json.dumps({"schema_version": 1, "market": "m", "signal_id": "signal-1",
                                          "rows": [{"symbol": "2330", "target_weight": 0.1}]}))
    pointer = {"schema_version": 2, "market": "m", "signal_id": "signal-1",
               "signal_ready_at": ready, "artifact_published_at": published,
               "summary_path": str(summary_path), "weights_path": str(directory / "target_weights.parquet"),
               "execution_weights_path": str(execution_path)}
    return SimpleNamespace(live_output_dir=root, market="m", signal_market=None), pointer


def test_unpublished_summary_is_never_promoted_by_a_directory_scan(tmp_path):
    spec, pointer = _signal(tmp_path)
    runner._LATEST_SIGNAL_CACHE.clear()
    assert runner._latest_signal(spec, NOW) is None
    (spec.live_output_dir / "latest_signal.json").write_text(json.dumps(pointer))
    assert runner._latest_signal(spec, NOW)[1][0]["symbol"] == "2330"


def test_pointer_identity_mismatch_never_falls_back_to_summary(tmp_path):
    spec, pointer = _signal(tmp_path)
    pointer["signal_id"] = "wrong-id"
    (spec.live_output_dir / "latest_signal.json").write_text(json.dumps(pointer))
    runner._LATEST_SIGNAL_CACHE.clear()
    assert runner._latest_signal(spec, NOW) is None


def test_stale_or_corrupt_pointer_cannot_reuse_published_artifacts(tmp_path):
    spec, pointer = _signal(tmp_path)
    path = spec.live_output_dir / "latest_signal.json"
    path.write_text(json.dumps(pointer))
    runner._LATEST_SIGNAL_CACHE.clear()
    assert runner._latest_signal(spec, NOW) is not None
    path.write_text("{")
    assert runner._latest_signal(spec, NOW) is None
    path.write_text(json.dumps({**pointer, "signal_ready_at": "2026-10-27T09:00:05+08:00"}))
    assert runner._latest_signal(spec, NOW) is None


def test_pointer_switch_during_read_retries_without_mixed_generation(tmp_path, monkeypatch):
    spec, pointer = _signal(tmp_path)
    path = spec.live_output_dir / "latest_signal.json"
    path.write_text(json.dumps(pointer))
    original = runner.read_execution_weight_rows

    def switch(*args, **kwargs):
        rows = original(*args, **kwargs)
        replacement = path.with_suffix(".next")
        replacement.write_text(json.dumps({**pointer, "signal_id": "next"}))
        os.replace(replacement, path)
        return rows

    monkeypatch.setattr(runner, "read_execution_weight_rows", switch)
    runner._LATEST_SIGNAL_CACHE.clear()
    assert runner._latest_signal(spec, NOW) is None


def test_cached_pointer_rechecks_changed_summary_and_payload(tmp_path):
    spec, pointer = _signal(tmp_path)
    (spec.live_output_dir / "latest_signal.json").write_text(json.dumps(pointer))
    runner._LATEST_SIGNAL_CACHE.clear()
    assert runner._latest_signal(spec, NOW) is not None
    summary_path = Path(pointer["summary_path"])
    summary = json.loads(summary_path.read_text())
    summary["signal_id"] = "different"
    summary_path.write_text(json.dumps(summary))
    assert runner._latest_signal(spec, NOW) is None


@pytest.mark.parametrize("rows", [
    [{"symbol": "2330", "target_weight": 0.1}, {"symbol": "2330", "target_weight": -0.1}],
    [{"symbol": "2330", "target_weight": None}],
    [{"symbol": "2330", "target_weight": True}],
    [{"symbol": "2330", "target_weight": float("nan")}],
    [{"symbol": "2330", "target_weight": float("inf")}],
    [{"symbol": "2330", "target_weight": 10 ** 400}],
    [{"symbol": "", "target_weight": 0.1}],
])
def test_invalid_targets_are_not_admitted_as_published_signals(tmp_path, rows):
    spec, pointer = _signal(tmp_path)
    weights = Path(pointer["execution_weights_path"])
    payload = json.loads(weights.read_text())
    payload["rows"] = rows
    weights.write_text(json.dumps(payload))
    (spec.live_output_dir / "latest_signal.json").write_text(json.dumps(pointer))
    runner._LATEST_SIGNAL_CACHE.clear()
    assert runner._latest_signal(spec, NOW) is None
