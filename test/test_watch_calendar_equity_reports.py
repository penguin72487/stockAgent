import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from scripts import refresh_equity_calendar_report as renderer
from scripts import watch_calendar_equity_reports as watcher


def test_session_coverage_rejects_missing_expected_dates_and_allows_account_only_dates(tmp_path):
    expected = tmp_path / "stock_sessions.json"
    expected.write_text(json.dumps(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-08"]))
    days = np.array(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"], dtype="datetime64[D]")
    check = renderer._session_coverage(days, expected)
    assert check["expected_dates_checked"] == 3
    assert check["additional_account_dates"] == ["2026-01-07"]
    with pytest.raises(ValueError, match="2026-01-05"):
        renderer._session_coverage(days[[0, 2, 3]], expected)
    with pytest.raises(ValueError, match="does not span"):
        renderer._session_coverage(np.array(["2026-01-02", "2026-01-09"], dtype="datetime64[D]"), expected)
    assert renderer._session_coverage(days, None)["status"] == "not_independently_verified"


def test_observer_waits_for_completion_and_publishes_once(tmp_path, monkeypatch):
    root = tmp_path / "study"
    attempt = root / "_capacity" / "case" / "attempt-00"
    training = attempt / "training"
    training.mkdir(parents=True)
    progress = training / "progress.json"
    progress.write_text(json.dumps({"state": "running", "phase": "training"}))
    (attempt / "config.yaml").write_text("trading:\n  tw_futures_portfolio_benchmark_mode: tx_front_rolling_1x_gross\n")
    source = training / "walkforward_deployment_backtest.npz"
    source.write_bytes(b"immutable backtest")
    (training / "walkforward_deployment_coverage.json").write_text(
        json.dumps({"stitched_fold_ids": [1], "fabricated_gap_rows": 0})
    )
    assert watcher.scan([root], tmp_path / "renderer.py")[0]["state"] == "waiting_for_training_completion"

    calls = []

    def fake_render(command, **kwargs):
        calls.append(command)
        output = Path(command[command.index("--output-dir") + 1])
        output.mkdir()
        names = ["equity_account_sessions.csv", "walkforward_equity_curve.png", "walkforward_equity_curve_log.png"]
        for name in names:
            (output / name).write_text(name)
        (output / "account_session_display_receipt.json").write_text(json.dumps({
            "report_contract": watcher.CONTRACT,
            "source_sha256": watcher._sha256(source),
            "outputs_sha256": {name: watcher._sha256(output / name) for name in names},
        }))
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(watcher.subprocess, "run", fake_render)
    progress.write_text(json.dumps({"state": "complete", "phase": "complete"}))
    assert watcher.scan([root], tmp_path / "renderer.py")[0]["state"] == "published"
    assert watcher.scan([root], tmp_path / "renderer.py")[0]["state"] == "verified_existing"
    assert len(calls) == 1
    assert not list(training.glob(".calendar-display-*"))
    source.write_bytes(b"changed backtest")
    result = watcher.scan([root], tmp_path / "renderer.py")[0]
    assert result["state"] == "error"
    assert "different contract or source" in result["error"]
    assert len(calls) == 1
