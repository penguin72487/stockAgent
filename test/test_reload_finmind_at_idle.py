from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from downloader import download_finmind_complement as worker
from scripts import reload_finmind_at_idle as reload


def test_direct_cli_help_works_outside_repository_without_pythonpath(tmp_path):
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    result = subprocess.run([sys.executable, str(Path(reload.__file__).resolve()), "--help"],
                            cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert "--apply" in result.stdout and "No provider requests" in " ".join(result.stdout.split())


def setup_idle(root, now, *, reason="batch_complete", seconds=5):
    root.mkdir()
    with worker._db(root / "queue.sqlite3") as connection:
        worker.object_pipeline.recover_claims(connection)
        connection.commit()
    status = {"state": reason, "observed_at_utc": now.isoformat(), "active_task": None}
    (root / "status.json").write_text(json.dumps(status))
    waiting = {"schema_version": 1, "phase": "waiting", "reason": reason,
               "observed_at_utc": now.isoformat(), "next_check_at_utc": (now + timedelta(seconds=seconds)).isoformat(),
               "cycle_observed_at_utc": now.isoformat()}
    (root / "worker_status.json").write_text(json.dumps(waiting))
    return status, waiting


def test_idle_reload_requires_nonreactive_window_deadline_and_empty_claims(tmp_path):
    now = datetime(2026, 10, 14, tzinfo=UTC)
    root = tmp_path / "owner"
    setup_idle(root, now)
    assert reload.idle_proof(root, now)["reason"] == "batch_complete"
    assert reload.idle_proof(root, now + timedelta(seconds=4)) is None
    with worker._db(root / "queue.sqlite3") as connection:
        connection.execute("INSERT INTO finmind_object_prefetch_claims VALUES ('GoldPrice','','2026','pending')")
        connection.commit()
    assert reload.idle_proof(root, now) is None
    other = tmp_path / "reactive"
    setup_idle(other, now, reason="incremental_reserve", seconds=60)
    assert reload.idle_proof(other, now) is None


def test_mismatched_cycle_and_active_task_never_authorize_restart(tmp_path):
    now = datetime(2026, 10, 14, tzinfo=UTC)
    root = tmp_path / "owner"
    status, waiting = setup_idle(root, now)
    waiting["cycle_observed_at_utc"] = (now - timedelta(seconds=1)).isoformat()
    (root / "worker_status.json").write_text(json.dumps(waiting))
    assert reload.idle_proof(root, now) is None
    waiting["cycle_observed_at_utc"] = now.isoformat()
    (root / "worker_status.json").write_text(json.dumps(waiting))
    status["active_task"] = {"dataset": "GoldPrice"}
    (root / "status.json").write_text(json.dumps(status))
    assert reload.idle_proof(root, now) is None


@pytest.mark.parametrize("protected", [False, True])
def test_calendar_protected_completed_wait_is_idle_only_while_gate_remains_active(tmp_path, monkeypatch, protected):
    now = datetime(2026, 10, 14, 0, 25, tzinfo=UTC)
    root = tmp_path / "owner"
    setup_idle(root, now, reason="protected_opening", seconds=300)
    observed = []
    def gate(stamp):
        observed.append(stamp)
        return protected
    monkeypatch.setattr(reload, "protected_stock_opening", gate)
    proof = reload.idle_proof(root, now)
    assert bool(proof) == protected
    assert observed == [now]
    if proof:
        assert proof["reason"] == "protected_opening" and not any(proof["claims"].values())
    assert reload.idle_proof(root, now + timedelta(seconds=299)) is None


def test_protected_wait_never_overrides_inflight_claims_or_unknown_live_status(tmp_path, monkeypatch):
    now = datetime(2026, 10, 14, 0, 25, tzinfo=UTC)
    root = tmp_path / "owner"
    status, _waiting = setup_idle(root, now, reason="protected_opening", seconds=300)
    monkeypatch.setattr(reload, "protected_stock_opening", lambda stamp: True)
    status["active_task"] = {"dataset": "UKStockPrice"}
    (root / "status.json").write_text(json.dumps(status))
    assert reload.idle_proof(root, now) is None
    status["active_task"] = None
    (root / "status.json").write_text(json.dumps(status))
    with worker._db(root / "queue.sqlite3") as connection:
        connection.execute("INSERT INTO finmind_object_prefetch_claims VALUES ('GoldPrice','','2026','pending')")
        connection.commit()
    assert reload.idle_proof(root, now) is None


def test_reload_rechecks_the_actual_idle_cycle_after_preservation_readback(tmp_path, monkeypatch):
    now = datetime.now(UTC)
    root = tmp_path / "owner"
    setup_idle(root, now, reason="current_queue", seconds=60)
    monkeypatch.setattr(reload, "ROOT", root)
    monkeypatch.setattr(reload, "code_matches", lambda accepted: True)
    monkeypatch.setattr(reload, "identity", lambda unit: {"ActiveState": "active", "InvocationID": "same", "MainPID": "123"})
    monkeypatch.setattr(reload.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("not idle anymore")))
    def preservation(root, last):
        (root / "worker_status.json").write_text(json.dumps({"schema_version": 1, "phase": "running"}))
        return []
    monkeypatch.setattr(reload, "prior_files", preservation)
    acceptance = tmp_path / "acceptance.json"
    acceptance.write_text(json.dumps({"exact_output_parity": True, "code_stable": True,
                                     "status_wall_improvement_observed": True, "source_queue": str(root / "queue.sqlite3")}))
    output = tmp_path / "observed.json"
    assert reload.main(["--acceptance", str(acceptance), "--output", str(output), "--apply", "--wait-seconds", "0"]) == 0
    observed = json.loads(output.read_bytes())
    assert observed["state"] == "deferred_generation_or_deadline_changed"
    assert not observed["restart_requested"]


def test_observation_default_never_restarts_a_service(tmp_path, monkeypatch):
    now = datetime.now(UTC)
    root = tmp_path / "owner"
    setup_idle(root, now, reason="current_queue", seconds=60)
    monkeypatch.setattr(reload, "ROOT", root)
    monkeypatch.setattr(reload, "code_matches", lambda accepted: True)
    monkeypatch.setattr(reload, "identity", lambda unit: {"ActiveState": "active", "InvocationID": "generation", "MainPID": "123"})
    monkeypatch.setattr(reload.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("restart not authorized")))
    acceptance = tmp_path / "acceptance.json"
    acceptance.write_text(json.dumps({"exact_output_parity": True, "code_stable": True,
                                     "status_wall_improvement_observed": True, "source_queue": str(root / "queue.sqlite3")}))
    output = tmp_path / "observed.json"
    assert reload.main(["--acceptance", str(acceptance), "--output", str(output)]) == 0
    observed = json.loads(output.read_bytes())
    assert observed["state"] == "idle_observed_no_action"
    assert not observed["restart_requested"]
