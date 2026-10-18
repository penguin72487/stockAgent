from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from downloader import download_finmind_complement as worker
from scripts import reload_finmind_at_idle as reload


def preparation_threads(*extra):
    return [{'thread_name': 'MainThread', 'frames': [
        {'name': '_populate', 'filename': '/repo/downloader/download_finmind_complement.py'},
        {'name': 'run_once', 'filename': '/repo/downloader/download_finmind_complement.py'},
        *({'name': name} for name in extra)]}]


def test_preparation_stack_is_not_an_idle_flag_or_live_batch():
    assert reload.preparation_stack(preparation_threads())
    assert not reload.preparation_stack([])
    assert not reload.preparation_stack(preparation_threads('run_batch'))
    assert not reload.preparation_stack(preparation_threads() + [
        {'thread_name': 'fetch', 'frames': [{'name': '_request_independent'}]}])
    foreign = preparation_threads()
    foreign[0]['frames'][1]['filename'] = '/foreign.py'
    assert not reload.preparation_stack(foreign)


def test_preparation_guard_rechecks_frozen_claims_and_always_thaws(tmp_path, monkeypatch):
    import sqlite3
    root = tmp_path
    (root / 'status.json').write_text(json.dumps({'last_task': {'rows': 390}}))
    with sqlite3.connect(root / 'queue.sqlite3') as conn:
        conn.execute('CREATE TABLE finmind_object_prefetch_claims(id)')
        conn.execute('CREATE TABLE complement_year_batch_claims(id)')
    calls = []
    monkeypatch.setattr(reload, '_preparation_pid', lambda: 123)
    monkeypatch.setattr(reload, '_stacks', lambda _: preparation_threads())
    monkeypatch.setattr(reload, 'identity', lambda _: {'InvocationID': 'one'})
    monkeypatch.setattr(reload.subprocess, 'run', lambda args, **kw: calls.append(args))
    with pytest.raises(RuntimeError, match='fixture'):
        with reload.preparation_boundary(root, {'InvocationID': 'one'}) as proof:
            assert proof['mode'] == 'frozen_drained_preparation'
            assert set(proof['claims'].values()) == {0}
            raise RuntimeError('fixture')
    assert [args[1] for args in calls] == ['freeze', 'thaw']
    with sqlite3.connect(root / 'queue.sqlite3') as conn:
        conn.execute('INSERT INTO finmind_object_prefetch_claims VALUES (1)')
    calls.clear()
    with reload.preparation_boundary(root, {'InvocationID': 'one'}) as proof:
        assert proof is None
        assert [args[1] for args in calls] == ['freeze', 'thaw']


def test_failed_freeze_is_still_thawed(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(reload, '_preparation_pid', lambda: 123)
    monkeypatch.setattr(reload, '_stacks', lambda _: preparation_threads())
    def run(args, **kw):
        calls.append(args[1])
        if args[1] == 'freeze':
            raise subprocess.TimeoutExpired(args, 5)
    monkeypatch.setattr(reload.subprocess, 'run', run)
    with pytest.raises(subprocess.TimeoutExpired):
        with reload.preparation_boundary(tmp_path, {}):
            pytest.fail('No proof on freeze failure')
    assert calls == ['freeze', 'thaw']


def test_frozen_termination_pins_process_and_queues_signal_before_thaw(tmp_path, monkeypatch):
    import signal
    calls = []
    monkeypatch.setattr(reload, '_pidfd_open', lambda pid: calls.append(('pin', pid)) or 99)
    monkeypatch.setattr(reload.os, 'close', lambda fd: calls.append(('close', fd)))
    monkeypatch.setattr(reload, '_pidfd_signal', lambda fd, sig: calls.append(('signal', fd, sig)))
    monkeypatch.setattr(reload.subprocess, 'run', lambda args, **kw: calls.append(('systemctl', args[1])))
    monkeypatch.setattr(Path, 'read_text', lambda *_a, **_kw: 'SigIgn:\t0\nSigCgt:\t2\n')
    proof = {'mode': 'frozen_drained_preparation', 'pid': 123, 'claims': {'object': 0, 'year': 0}}
    reload.terminate_frozen_preparation(proof)
    assert calls == [('pin', 123), ('signal', 99, signal.SIGTERM), ('close', 99), ('systemctl', 'thaw')]
    monkeypatch.setattr(Path, 'read_text', lambda *_a, **_kw: 'SigIgn:\t0\nSigCgt:\t4000\n')
    calls.clear()
    with pytest.raises(ValueError, match='nondefault_sigterm'):
        reload.terminate_frozen_preparation(proof)
    assert calls == [('pin', 123), ('close', 99)]


def test_pidfd_native_or_libc_binding_checks_own_process_without_signalling():
    descriptor = reload._pidfd_open(os.getpid())
    try:
        reload._pidfd_signal(descriptor, 0)  # Existence/permission check only.
    finally:
        os.close(descriptor)


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
