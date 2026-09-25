from datetime import date
import json
import subprocess
import sys
from types import SimpleNamespace

import numpy as np
import pytest

from scripts import refresh_tw_day_trade_margin_actions as margin_actions
from scripts.refresh_tw_day_trade_margin_actions import commands


def test_new_execution_source_does_not_claim_current_year_is_baseline(tmp_path):
    output = tmp_path / "execution_actions"
    output.mkdir()
    planned = commands(tmp_path, output, 2026, date(2026, 9, 10))
    assert planned[0][planned[0].index("--start-year") + 1] == "2000"
    assert planned[1][planned[1].index("--retained-source-dir") + 1] == str(tmp_path)
    assert all(command[command.index("--output-dir") + 1] == str(output) for command in planned)
    (output / "tw_corporate_action_reference.summary.json").write_text(json.dumps({"baseline_established": True}))
    planned = commands(tmp_path, output, 2026, date(2026, 9, 11))
    assert planned[0][planned[0].index("--start-year") + 1] == "2026"
    assert all(command[command.index("--end-date") + 1] == "2026-09-11" for command in planned)


@pytest.mark.parametrize("failing_step", [None, 2])
def test_margin_action_readiness_records_step_timings_without_changing_result(
    tmp_path, monkeypatch: pytest.MonkeyPatch, failing_step: int | None,
) -> None:
    live = tmp_path / "live"
    live.mkdir()
    catalog = tmp_path / "configs/data_sync/packed_datasets.json"
    catalog.parent.mkdir(parents=True)
    catalog.write_text(json.dumps({"datasets": [{
        "dataset": "tw-public", "source": "live"
    }]}), encoding="utf-8")
    monkeypatch.setattr(margin_actions, "ROOT", tmp_path)
    monkeypatch.setattr(
        margin_actions, "commands",
        lambda *args: [[sys.executable, f"collector-{index}.py"] for index in range(1, 4)],
    )
    calls = 0

    def run(command, **kwargs):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(command, 1 if calls == failing_step else 0)

    monkeypatch.setattr(margin_actions.subprocess, "run", run)
    monkeypatch.setattr(
        margin_actions, "_load_corporate_action_reference",
        lambda *_: SimpleNamespace(
            coverage_end=np.datetime64("2026-09-23"),
            exact_coverage_end=np.datetime64("2026-09-23"),
        ),
    )
    monkeypatch.setattr(margin_actions, "load_share_replacements", lambda *a, **k: [])
    monkeypatch.setattr(sys, "argv", [
        "refresh_tw_day_trade_margin_actions.py", "--start-year", "2026",
        "--session-date", "2026-09-23", "--public-root", str(live),
    ])
    if failing_step:
        with pytest.raises(RuntimeError, match="collector failed"):
            margin_actions.main()
    else:
        margin_actions.main()

    receipt = json.loads(
        (live / "execution_actions/readiness.json").read_text(encoding="utf-8")
    )
    assert receipt["status"] == ("blocked" if failing_step else "source_ready")
    assert len(receipt["steps"]) == (failing_step or 3)
    assert all(step["elapsed_seconds"] >= 0 for step in receipt["steps"])
    assert receipt["total_elapsed_seconds"] >= 0
    if failing_step is None:
        assert receipt["validation_elapsed_seconds"] >= 0
