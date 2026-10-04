from __future__ import annotations

from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import run_discord_artifact_maintenance as runner


NOW = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)


def _job(day="2026-09-30", market="retry_test_market", status="failed", deadline=None):
    key = f"{day}:{market}:artifact_backfill"
    return key, {
        "key": key,
        "market": market,
        "status": status,
        "attempt": 5,
        "next_retry_at": deadline or "2026-10-01T02:08:36+08:00",
    }


@pytest.fixture
def receipt_scope(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    path = root / "artifacts/discord_bot/artifact_backfill_status.json"
    path.parent.mkdir(parents=True)
    configs = root / "markets"
    configs.mkdir()
    (configs / "market.yaml").write_text(
        "market: retry_test_market\nlabel: Fixture\nenabled: true\n"
        "config_path: unused.yaml\n"
    )
    monkeypatch.setattr(runner, "ROOT", root)
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH", str(path))
    monkeypatch.setenv("STOCKAGENT_MARKETS_DIR", str(configs))
    monkeypatch.setenv("STOCKAGENT_SCHEDULED_MARKETS", "retry_test_market")
    monkeypatch.setenv(
        "STOCKAGENT_ARTIFACT_RETRY_STATE_PATH", str(root / "retry_dispatch.json")
    )
    return root, path, configs


def _write(path, *jobs):
    path.write_text(json.dumps({"schema_version": 2, "jobs": dict(jobs)}))


def test_absent_receipt_is_nonmutating_idle(receipt_scope):
    _, path, _ = receipt_scope
    result = runner.retry_probe(observed=NOW)
    assert not result["due"]
    assert not path.exists()
    assert not path.with_name(path.name + ".worker.lock").exists()


def test_latest_ready_job_suppresses_obsolete_failure(receipt_scope):
    _, path, _ = receipt_scope
    _write(path, _job(day="2026-09-29"), _job(status="ready"))
    before = path.read_bytes()
    result = runner.retry_probe(observed=NOW)
    assert not result["due"]
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "deadline", ["2026-10-01T10:00:00+08:00", "2026-10-01T08:00:01+08:00"]
)
def test_future_backoff_does_not_even_load_configs(
    receipt_scope, monkeypatch, deadline
):
    _, path, _ = receipt_scope
    _write(path, _job(deadline=deadline))
    from stockagent.live import market_config

    monkeypatch.setattr(
        market_config, "load_market_configs", lambda _: pytest.fail("not due")
    )
    assert not runner.retry_probe(observed=NOW)["due"]


def test_aware_deadline_exact_boundary_preserves_receipt(receipt_scope):
    _, path, _ = receipt_scope
    key, job = _job(deadline="2026-10-01T08:00:00+08:00")
    _write(path, (key, job))
    before = path.read_bytes()
    result = runner.retry_probe(observed=NOW)
    assert result["due_keys"] == [key]
    assert result["advisory_only"]
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "defect", ["naive", "missing", "malformed_json", "jobs_not_object", "too_large"]
)
def test_invalid_receipt_never_dispatches_or_rewrites(receipt_scope, defect):
    _, path, _ = receipt_scope
    key, job = _job()
    if defect == "naive":
        job["next_retry_at"] = "2026-10-01T08:00:00"
    if defect == "missing":
        job["next_retry_at"] = None
    _write(path, (key, job))
    if defect == "malformed_json":
        path.write_text("{bad-json")
    if defect == "jobs_not_object":
        path.write_text('{"jobs": []}')
    if defect == "too_large":
        path.write_bytes(b" " * (runner.RETRY_RECEIPT_MAX_BYTES + 1))
    before = path.read_bytes()
    with pytest.raises(ValueError):
        runner.retry_probe(observed=NOW)
    assert path.read_bytes() == before


@pytest.mark.parametrize("disabled_by", ["selection", "yaml", "state"])
def test_unscheduled_or_disabled_markets_never_wake_worker(
    receipt_scope, monkeypatch, disabled_by
):
    root, path, configs = receipt_scope
    _write(path, _job())
    if disabled_by == "selection":
        monkeypatch.setenv("STOCKAGENT_SCHEDULED_MARKETS", "other")
    if disabled_by == "yaml":
        cfg = configs / "market.yaml"
        cfg.write_text(cfg.read_text().replace("enabled: true", "enabled: false"))
    if disabled_by == "state":
        (root / "artifacts/discord_bot/state.json").write_text(
            '{"markets":{"retry_test_market":{"enabled":false}}}'
        )
    assert not runner.retry_probe(observed=NOW)["due"]


def test_orphaned_running_job_can_wake_canonical_owner(receipt_scope):
    _, path, _ = receipt_scope
    key, job = _job(status="running")
    job["next_retry_at"] = None
    _write(path, (key, job))
    assert runner.retry_probe(observed=NOW)["due_keys"] == [key]


@pytest.mark.parametrize("selection", ["all", "ALL", "*", ""])
def test_all_market_selection_matches_canonical_case_insensitive_scope(
    receipt_scope, monkeypatch, selection
):
    _, path, _ = receipt_scope
    _write(path, _job())
    monkeypatch.setenv("STOCKAGENT_SCHEDULED_MARKETS", selection)
    assert runner.retry_probe(observed=NOW)["due"]


def test_live_kernel_lock_beats_even_bad_or_stale_receipts(receipt_scope):
    _, path, _ = receipt_scope
    path.write_text("not-json")
    with path.with_name(path.name + ".worker.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = runner.retry_probe(observed=NOW)
    assert not result["due"]
    assert result["reason"] == "canonical_worker_running"
    assert path.read_text() == "not-json"


def test_probe_cli_skips_without_torch_import_or_receipt_write(receipt_scope):
    _, path, _ = receipt_scope
    _write(path, _job(deadline="2099-01-01T00:00:00+00:00"))
    before = path.read_bytes()
    result = subprocess.run(
        [sys.executable, runner.__file__, "--retry-check-only"],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 1, result.stderr
    assert json.loads(result.stdout)["model_runtime_imported"] is False
    assert path.read_bytes() == before


def test_coalesced_cli_never_calls_systemctl_or_imports_torch(
    receipt_scope, monkeypatch
):
    root, path, _ = receipt_scope
    _write(path, _job(deadline="2000-01-01T00:00:00+00:00"))
    before = path.read_bytes()
    fake_bin = root / "fake_bin"
    fake_bin.mkdir()
    guarded_systemctl = fake_bin / "systemctl"
    guarded_systemctl.write_text("#!/bin/sh\nexit 89\n")
    guarded_systemctl.chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ['PATH']}")
    check = subprocess.run(
        [sys.executable, runner.__file__, "--retry-check-only"],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert check.returncode == 0, check.stderr
    probe = json.loads(check.stdout)
    state = runner._retry_dispatch_state_path()
    state.write_text(
        json.dumps(
            {
                "dispatch_accepted_at": datetime.now(timezone.utc).isoformat(),
                "wakeup_fingerprint": probe["wakeup_fingerprint"],
            }
        )
    )
    saved = state.read_bytes()
    result = subprocess.run(
        [sys.executable, runner.__file__, "--request-due-retry"],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["dispatch_reason"] == "unchanged_retry_wakeup_coalesced"
    assert payload["model_runtime_imported"] is False and not payload["requested"]
    assert path.read_bytes() == before and state.read_bytes() == saved


@pytest.mark.parametrize("allowed", [False, True])
def test_request_rechecks_market_runway_before_exact_owner_dispatch(
    receipt_scope, monkeypatch, allowed
):
    from scripts import check_outside_tw_opening_resource_window as window

    calls = []
    monkeypatch.setattr(
        runner,
        "retry_probe",
        lambda: {"due": True, "due_keys": ["fixture"], "wakeup_fingerprint": "abc"},
    )

    def checked(_observed, **kwargs):
        assert kwargs == {
            "minimum_runway_minutes": runner.ARTIFACT_RETRY_RUNWAY_MINUTES,
            "protected_until": runner.wall_time(13, 40),
        }
        return {"allowed": allowed}

    monkeypatch.setattr(window, "evaluate", checked)
    monkeypatch.setattr(
        runner.subprocess, "run", lambda *a, **kw: calls.append((a, kw))
    )
    assert runner.request_due_retry() == 0
    assert len(calls) == int(allowed)
    if allowed:
        assert calls[0][0][0] == [
            "systemctl",
            "start",
            "--no-block",
            runner.ARTIFACT_OWNER_UNIT,
        ]
        assert calls[0][1] == {"check": True, "timeout": 10}
    assert runner._retry_dispatch_state_path().exists() is allowed


def _permit_dispatch(monkeypatch):
    from scripts import check_outside_tw_opening_resource_window as window

    calls = []
    monkeypatch.setattr(window, "evaluate", lambda *a, **kw: {"allowed": True})
    monkeypatch.setattr(runner.subprocess, "run", lambda *a, **kw: calls.append(a))
    return calls


def test_unchanged_source_defer_coalesces_without_job_mutation(
    receipt_scope, monkeypatch, capsys
):
    _, path, _ = receipt_scope
    _write(path, _job())
    before = path.read_bytes()
    calls = _permit_dispatch(monkeypatch)
    assert runner.request_due_retry() == 0
    saved = runner._retry_dispatch_state_path().read_bytes()
    assert runner.request_due_retry() == 0
    assert len(calls) == 1
    assert runner._retry_dispatch_state_path().read_bytes() == saved
    assert path.read_bytes() == before
    result = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert result["dispatch_reason"] == "unchanged_retry_wakeup_coalesced"
    assert result["advisory_only"] and not result["requested"]


@pytest.mark.parametrize("changed", ["source_receipt", "job_attempt", "config"])
def test_new_source_or_job_bypasses_identical_wakeup_coalescing(
    receipt_scope, monkeypatch, changed
):
    root, path, configs = receipt_scope
    key, job = _job()
    _write(path, (key, job))
    calls = _permit_dispatch(monkeypatch)
    runner.request_due_retry()
    if changed == "source_receipt":
        receipt = (
            root / "artifacts/data_refresh/tw_public/completed_session/latest.json"
        )
        receipt.parent.mkdir(parents=True)
        receipt.write_text('{"status":"still_not_verified"}')
    elif changed == "job_attempt":
        job["attempt"] += 1
        _write(path, (key, job))
    else:
        cfg = configs / "market.yaml"
        cfg.write_text(cfg.read_text().replace("label: Fixture", "label: Revised"))
    runner.request_due_retry()
    assert len(calls) == 2


def test_elapsed_coalescing_interval_rechecks_unchanged_source(
    receipt_scope, monkeypatch
):
    _, path, _ = receipt_scope
    _write(path, _job())
    calls = _permit_dispatch(monkeypatch)
    runner.request_due_retry()
    state = runner._retry_dispatch_state_path()
    payload = json.loads(state.read_text())
    payload["dispatch_accepted_at"] = (
        datetime.now(timezone.utc)
        - timedelta(seconds=runner.UNCHANGED_RETRY_WAKEUP_SECONDS + 1)
    ).isoformat()
    state.write_text(json.dumps(payload))
    runner.request_due_retry()
    assert len(calls) == 2


def test_failed_dispatch_does_not_create_coalescing_state(receipt_scope, monkeypatch):
    _, path, _ = receipt_scope
    _write(path, _job())
    _permit_dispatch(monkeypatch)

    def rejected(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "systemctl")

    monkeypatch.setattr(runner.subprocess, "run", rejected)
    with pytest.raises(subprocess.CalledProcessError):
        runner.request_due_retry()
    assert not runner._retry_dispatch_state_path().exists()


def test_backwards_clock_jump_cannot_stall_recovery(receipt_scope, monkeypatch):
    _, path, _ = receipt_scope
    _write(path, _job())
    calls = _permit_dispatch(monkeypatch)
    runner.request_due_retry()
    state = runner._retry_dispatch_state_path()
    payload = json.loads(state.read_text())
    payload["dispatch_accepted_at"] = (
        datetime.now(timezone.utc) + timedelta(hours=1)
    ).isoformat()
    state.write_text(json.dumps(payload))
    runner.request_due_retry()
    runner.request_due_retry()
    assert len(calls) == 2


def test_dispatch_persistence_failure_is_not_reported_durable(
    receipt_scope, monkeypatch, capsys
):
    from stockagent.data_sync import desync_snapshots

    _, path, _ = receipt_scope
    _write(path, _job())
    calls = _permit_dispatch(monkeypatch)

    def failed(*args, **kwargs):
        raise OSError("fixture state is unwritable")

    monkeypatch.setattr(desync_snapshots, "atomic_write_json", failed)
    with pytest.raises(OSError):
        runner.request_due_retry()
    result = json.loads(capsys.readouterr().out)
    assert len(calls) == 1
    assert result["requested"] and not result["dispatch_state_persisted"]
    assert not runner._retry_dispatch_state_path().exists()


def test_retry_unit_is_lightweight_and_uses_original_worker():
    root = Path(runner.__file__).resolve().parents[1]
    service = (
        root / "deploy/systemd/stockagent-discord-artifact-retry.service.in"
    ).read_text()
    timer = (
        root / "deploy/systemd/stockagent-discord-artifact-retry.timer.in"
    ).read_text()
    installer = (root / "scripts/install_discord_bot_service.sh").read_text()
    assert "--request-due-retry" in service
    assert (
        f"--minimum-runway-minutes {runner.ARTIFACT_RETRY_RUNWAY_MINUTES} "
        "--protected-until 13:40"
    ) in service
    assert "ProtectSystem=strict" in service
    assert "RestrictAddressFamilies=AF_UNIX" in service
    assert "StateDirectory=stockagent-discord-artifact-retry" in service
    assert "00..06:*:00 Asia/Taipei" in timer
    assert "13..23:*:00 Asia/Taipei" in timer
    assert "Persistent=true" in timer
    assert "RETRY_SERVICE_NAME" in installer and "RETRY_TIMER_NAME" in installer


@pytest.mark.parametrize("minute, second, allowed", [(16, 59, True), (17, 0, False)])
def test_opening_runway_includes_existing_owner_stop_grace(minute, second, allowed):
    from scripts import check_outside_tw_opening_resource_window as window

    observed = datetime(
        2026, 10, 1, 6, minute, second, tzinfo=runner.ZoneInfo("Asia/Taipei")
    )
    result = window.evaluate(
        observed,
        minimum_runway_minutes=runner.ARTIFACT_RETRY_RUNWAY_MINUTES,
        protected_until=runner.wall_time(13, 40),
    )
    assert result["allowed"] is allowed
    assert result["protected_start"] == "06:17:00"
