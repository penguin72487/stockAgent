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
        assert receipt["historical_catalog_sync"]["status"] == "waiting_baseline"


def _catalog_metadata(tmp_path):
    root = tmp_path / "public"
    tail = root / "execution_actions"
    tail.mkdir(parents=True)
    (root / "tw_share_replacement_reference.summary.json").write_text(json.dumps({
        "coverage_start": "2014-01-01", "coverage_end": "2026-09-17",
    }))
    (tail / "tw_share_replacement_reference.summary.json").write_text(json.dumps({
        "coverage_start": "2026-01-01", "coverage_end": "2026-09-30",
    }))
    return root, tail


def test_historical_sync_uses_canonical_merger_not_collectors(tmp_path, monkeypatch):
    root, tail = _catalog_metadata(tmp_path)
    seen = []

    def run(command, **kwargs):
        seen.append(command)
        assert command[1].endswith("scripts/merge_tw_share_replacement_reference.py")
        assert kwargs["timeout"] == 300 and kwargs["capture_output"]
        assert command[command.index("--cutover-date") + 1] == "2026-01-01"
        assert command[command.index("--required-start-date") + 1] == "2014-01-01"
        assert "--apply" in command
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps({
            "verified": True, "coverage": ["2014-01-01", "2026-09-30"],
        }))

    monkeypatch.setattr(margin_actions.subprocess, "run", run)
    result = margin_actions.sync_historical_share_catalog(root, tail, date(2026, 9, 30))
    assert len(seen) == 1
    assert result["status"] == "ready"
    assert result["action"] == "merged_verified_tail"
    assert result["elapsed_seconds"] >= 0


@pytest.mark.parametrize("problem", ["exit", "unverified", "wrong_horizon", "invalid_json", "timeout", "busy"])
def test_historical_sync_failure_stays_separate_and_visible(tmp_path, monkeypatch, problem):
    root, tail = _catalog_metadata(tmp_path)
    before = {path: path.read_bytes() for path in root.rglob("*.json")}

    def run(command, **kwargs):
        if problem == "timeout":
            raise subprocess.TimeoutExpired(command, 300)
        if problem == "busy":
            raise OSError("writer busy")
        proof = {"verified": problem != "unverified", "coverage": ["2014-01-01", "2026-09-30"]}
        if problem == "wrong_horizon":
            proof["coverage"][-1] = "2026-09-29"
        return subprocess.CompletedProcess(command, 1 if problem == "exit" else 0,
            stdout="bad-json" if problem == "invalid_json" else json.dumps(proof), stderr="rejected overlap")

    monkeypatch.setattr(margin_actions.subprocess, "run", run)
    result = margin_actions.sync_historical_share_catalog(root, tail, date(2026, 9, 30))
    assert result["status"] == "blocked"
    assert result["error_type"] and result["error"]
    assert result["elapsed_seconds"] >= 0
    assert {path: path.read_bytes() for path in before} == before


def test_current_catalog_must_verify_sources_before_no_op(tmp_path, monkeypatch):
    from scripts import merge_tw_share_replacement_reference as merger

    root, tail = _catalog_metadata(tmp_path)
    baseline = root / "tw_share_replacement_reference.summary.json"
    baseline.write_text('{"coverage_start":"2014-01-01","coverage_end":"2026-09-30"}')
    seen = []
    monkeypatch.setattr(merger, "_accepted", lambda *a, **k: seen.append((a, k)))
    monkeypatch.setattr(margin_actions.subprocess, "run", lambda *a, **k: pytest.fail("no remerge needed"))
    result = margin_actions.sync_historical_share_catalog(root, tail, date(2026, 9, 30))
    assert result["status"] == "ready" and result["action"] == "no_op_verified"
    assert seen == [((root,), {"start": date(2014, 1, 1), "end": date(2026, 9, 30)})]

    def corrupt(*args, **kwargs):
        raise ValueError("raw response rejected")

    monkeypatch.setattr(merger, "_accepted", corrupt)
    result = margin_actions.sync_historical_share_catalog(root, tail, date(2026, 9, 30))
    assert result["status"] == "blocked"


def test_historical_sync_malformed_baseline_does_not_start_work(tmp_path, monkeypatch):
    root, tail = _catalog_metadata(tmp_path)
    (root / "tw_share_replacement_reference.summary.json").write_text('{"coverage_start":"invalid"}')
    monkeypatch.setattr(margin_actions.subprocess, "run", lambda *a, **k: pytest.fail("invalid baseline"))
    assert margin_actions.sync_historical_share_catalog(root, tail, date(2026, 9, 30))["status"] == "blocked"


def test_historical_sync_rejects_short_tail_before_starting_merge(tmp_path, monkeypatch):
    root, tail = _catalog_metadata(tmp_path)
    (tail / "tw_share_replacement_reference.summary.json").write_text(json.dumps({
        "coverage_start": "2026-01-01", "coverage_end": "2026-09-29",
    }))
    monkeypatch.setattr(margin_actions.subprocess, "run", lambda *a, **k: pytest.fail("short tail"))
    assert margin_actions.sync_historical_share_catalog(root, tail, date(2026, 9, 30))["status"] == "blocked"


def test_historical_sync_accepts_real_canonical_subprocess_and_verified_no_op(tmp_path):
    from test_merge_tw_share_replacement_reference import _fixtures

    root, tail = _fixtures(tmp_path)
    result = margin_actions.sync_historical_share_catalog(root, tail, date(2026, 1, 4))
    assert result["status"] == "ready"
    assert result["action"] == "merged_verified_tail"
    assert result["merge"]["verified"] is True
    result = margin_actions.sync_historical_share_catalog(root, tail, date(2026, 1, 4))
    assert result["status"] == "ready"
    assert result["action"] == "no_op_verified"
