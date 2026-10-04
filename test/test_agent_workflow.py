"""Engineering receipts must survive failures without inventing completion."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from scripts import agent_workflow as workflow_module
from scripts.agent_workflow import Workflow, dirty_paths, file_evidence, run_view, save


@pytest.fixture
def workflow(tmp_path):
    root = tmp_path / "checkout with spaces"
    root.mkdir()
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    (root / "tracked.txt").write_text("baseline\n")
    subprocess.run(["git", "add", "tracked.txt"], cwd=root, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "baseline",
        ],
        cwd=root,
        check=True,
    )
    return Workflow(root)


def prepare_run(workflow, monkeypatch, argv, name="check"):
    if not workflow.task_path("repair").exists():
        workflow.start_task(
            "repair", "Repair the requested provider", "Verify its source receipt"
        )
    monkeypatch.setattr(workflow_module, "command", lambda *args, **kwargs: "")
    return workflow.launch("repair", argv, "systemd", name)


def test_task_keeps_original_goal_and_overlapping_dirty_baseline(workflow):
    path = workflow.root / "tracked.txt"
    path.write_text("user work\n")
    task = workflow.start_task("repair", "Fix FinMind", "Inspect its ledger")
    assert workflow.state_dir.stat().st_mode & 0o777 == 0o700
    assert workflow.task_path("repair").stat().st_mode & 0o777 == 0o600
    assert {row["path"] for row in task["git_baseline"]["dirty_paths"]} == {
        "tracked.txt"
    }
    workflow.update_task(
        "repair", next_action="Validate the same provider", note="Root cause located"
    )
    with pytest.raises(ValueError, match="already exists"):
        workflow.start_task("repair", "Switch to FinLab", "Different work")
    assert workflow.read(workflow.task_path("repair"))["objective"] == "Fix FinMind"
    assert path.read_text() == "user work\n"


def test_porcelain_retains_rename_spaces_and_newlines():
    paths = dirty_paths(" R renamed file\0original file\0?? new\nfile\0")
    assert paths == [
        {"status": " R", "path": "renamed file", "original_path": "original file"},
        {"status": "??", "path": "new\nfile"},
    ]


def test_work_dispatch_preserves_literal_arguments_without_touching_task_runner(tmp_path):
    import shutil
    checkout = tmp_path / 'control checkout'
    scripts = checkout / 'scripts'
    scripts.mkdir(parents=True)
    original = Path(__file__).resolve().parents[1] / 'scripts/stockagent-agent.sh'
    shutil.copy2(original, scripts / 'stockagent-agent.sh')
    (scripts / 'agent_workflow.py').write_text('# untouched existing runner\n')
    (scripts / 'stockagent-control.sh').write_text('''#!/bin/bash
printf '%s\\n' "$@"
''')
    payload = '$(touch SHOULD_NOT_EXIST); literal `text`'
    result = subprocess.run(['bash', str(scripts/'stockagent-agent.sh'), 'work', 'submit', payload],
                            env={**os.environ,'STOCKAGENT_REPO_ROOT':str(checkout)},
                            cwd=tmp_path,text=True,capture_output=True,check=True)
    assert result.stdout.splitlines()==['submit',payload]
    assert not (tmp_path/'SHOULD_NOT_EXIST').exists()


@pytest.mark.parametrize("task_id", ("../escape", "a/b", "a; touch bad", "", "a" * 97))
def test_task_id_cannot_escape_receipt_directory(workflow, task_id):
    with pytest.raises(ValueError, match="id must"):
        workflow.start_task(task_id, "goal", "step")


def test_concurrent_task_updates_do_not_lose_progress(workflow):
    workflow.start_task("repair", "same goal", "start")
    with ThreadPoolExecutor(max_workers=2) as workers:
        list(
            workers.map(
                lambda note: workflow.update_task("repair", note=note),
                ("source inspected", "test passed"),
            )
        )
    task = workflow.read(workflow.task_path("repair"))
    assert {event["note"] for event in task["history"]} == {
        "source inspected",
        "test passed",
    }


def test_foreign_worktree_receipt_is_not_reused(workflow, tmp_path):
    task = workflow.start_task("repair", "goal", "step")
    task["repo_root"] = str(tmp_path / "another checkout")
    save(workflow.task_path("repair"), task)
    with pytest.raises(ValueError, match="incompatible"):
        workflow.read(workflow.task_path("repair"))


def test_other_host_receipt_cannot_describe_local_processes(workflow):
    task = workflow.start_task("repair", "goal", "step")
    task["host"] = "another-node"
    save(workflow.task_path("repair"), task)
    with pytest.raises(ValueError, match="incompatible"):
        workflow.read(workflow.task_path("repair"))


def test_empty_or_changed_evidence_cannot_become_current_proof(tmp_path, monkeypatch):
    path = tmp_path / "receipt.json"
    path.write_text("")
    with pytest.raises(ValueError, match="non-empty"):
        file_evidence(path)
    path.write_text('{"accepted":true}')
    original = Path.read_bytes

    def changing_read(target):
        content = original(target)
        target.write_text("changed while reading")
        return content

    monkeypatch.setattr(Path, "read_bytes", changing_read)
    with pytest.raises(ValueError, match="changed while"):
        file_evidence(path)


def test_actual_exit_code_stdout_stderr_and_literal_arguments(workflow, monkeypatch):
    payload = "$(touch SHOULD_NOT_EXIST); literal `text`"
    record = prepare_run(
        workflow,
        monkeypatch,
        [
            sys.executable,
            "-c",
            "import sys; print(sys.argv[1]); print('stderr retained', file=sys.stderr); sys.exit(7)",
            payload,
        ],
    )
    assert workflow.execute(record["run_id"]) == 7
    result = workflow.read(workflow.run_path(record["run_id"]))
    assert result["state"] == "failed" and result["exit_code"] == 7
    log = Path(result["log"]).read_text()
    assert payload in log and "stderr retained" in log
    assert not (workflow.root / "SHOULD_NOT_EXIST").exists()


def test_runtime_alias_uses_selected_python_and_duplicate_execution_is_rejected(
    workflow, monkeypatch
):
    record = prepare_run(
        workflow,
        monkeypatch,
        ["run_fintech_python", "-c", "import sys; print(sys.executable)"],
    )
    assert workflow.execute(record["run_id"]) == 0
    assert sys.executable in Path(record["log"]).read_text()
    with pytest.raises(ValueError, match="already executed"):
        workflow.execute(record["run_id"])


def test_failed_command_can_retry_without_erasing_failure(workflow, monkeypatch):
    failed = prepare_run(
        workflow, monkeypatch, [sys.executable, "-c", "raise SystemExit(3)"]
    )
    assert workflow.execute(failed["run_id"]) == 3
    evidence = workflow.root / "acceptance.json"
    evidence.write_text('{"accepted": true}')
    with pytest.raises(ValueError, match="must have succeeded"):
        workflow.update_task("repair", state="complete", evidence=[evidence])
    passed = prepare_run(
        workflow, monkeypatch, [sys.executable, "-c", "print('verified')"]
    )
    assert workflow.execute(passed["run_id"]) == 0
    task = workflow.update_task("repair", state="complete", evidence=[evidence])
    assert task["state"] == "complete"
    assert workflow.read(workflow.run_path(failed["run_id"]))["exit_code"] == 3


def test_queued_work_cannot_be_launched_twice_or_completed(workflow, monkeypatch):
    prepare_run(workflow, monkeypatch, [sys.executable, "-c", "print('later')"])
    with pytest.raises(ValueError, match="same-name"):
        workflow.launch("repair", ["true"], "systemd", "check")
    evidence = workflow.root / "acceptance.json"
    evidence.write_text('{"accepted":true}')
    with pytest.raises(ValueError, match="must have succeeded"):
        workflow.update_task("repair", state="complete", evidence=[evidence])


def test_completion_requires_current_evidence(workflow):
    workflow.start_task("repair", "goal", "step")
    with pytest.raises(ValueError, match="current non-empty evidence"):
        workflow.update_task("repair", state="complete")


def resolution_case(workflow, monkeypatch):
    failed = prepare_run(workflow, monkeypatch, [sys.executable, "-c", "raise SystemExit(3)"])
    assert workflow.execute(failed["run_id"]) == 3
    verified = prepare_run(
        workflow, monkeypatch, [sys.executable, "-c", "print('verified')"], name="verification"
    )
    assert workflow.execute(verified["run_id"]) == 0
    evidence = workflow.root / "acceptance.json"
    evidence.write_text(json.dumps({
        "state": "accepted_expected_rejection", "task_id": "repair",
        "run_id": failed["run_id"], "verified_by": verified["run_id"],
        "criterion": "Different primary keys must reject compatibility; no promotion.",
    }))
    return failed, verified, evidence


@pytest.mark.parametrize("outcome", ["superseded", "expected-rejection"])
def test_failure_disposition_preserves_run_and_log(workflow, monkeypatch, outcome):
    failed, verified, evidence = resolution_case(workflow, monkeypatch)
    original = workflow.run_path(failed["run_id"]).read_bytes()
    log = Path(failed["log"]).read_bytes()
    resolution = workflow.resolve_run(
        "repair", failed["run_id"], outcome=outcome, verified_by=verified["run_id"],
        note="Original failure retained; later verification establishes disposition.",
        evidence=[evidence],
    )
    assert resolution["outcome"] == outcome
    task = workflow.update_task("repair", state="complete", evidence=[evidence])
    assert task["run_resolutions"] == [resolution]
    assert workflow.run_path(failed["run_id"]).read_bytes() == original
    assert Path(failed["log"]).read_bytes() == log
    assert workflow.read(workflow.run_path(failed["run_id"]))["state"] == "failed"


@pytest.mark.parametrize("changed", ["evidence", "failed", "verification", "log"])
def test_failure_resolution_rechecks_all_proofs_on_completion(workflow, monkeypatch, changed):
    failed, verified, evidence = resolution_case(workflow, monkeypatch)
    workflow.resolve_run(
        "repair", failed["run_id"], outcome="superseded", verified_by=verified["run_id"],
        note="A verified replacement", evidence=[evidence],
    )
    target = {
        "evidence": evidence, "failed": workflow.run_path(failed["run_id"]),
        "verification": workflow.run_path(verified["run_id"]), "log": Path(failed["log"]),
    }[changed]
    target.write_bytes(target.read_bytes() + b"\n")
    current = workflow.root / "current.json"
    current.write_text('{"accepted":true}')
    with pytest.raises(ValueError, match="evidence changed"):
        workflow.update_task("repair", state="complete", evidence=[current])
    assert workflow.read(workflow.task_path("repair"))["state"] == "active"


def test_failure_resolution_cannot_waive_a_new_failure(workflow, monkeypatch):
    failed, verified, evidence = resolution_case(workflow, monkeypatch)
    workflow.resolve_run(
        "repair", failed["run_id"], outcome="superseded", verified_by=verified["run_id"],
        note="A verified replacement", evidence=[evidence],
    )
    next_failed = prepare_run(workflow, monkeypatch, [sys.executable, "-c", "raise SystemExit(4)"])
    assert workflow.execute(next_failed["run_id"]) == 4
    with pytest.raises(ValueError, match="must have succeeded"):
        workflow.update_task("repair", state="complete", evidence=[evidence])


@pytest.mark.parametrize("invalid", ["queued", "running", "cancelled", "unverified", "foreign", "earlier"])
def test_failure_resolution_rejects_unverified_or_foreign_runs(workflow, monkeypatch, invalid):
    failed, verified, evidence = resolution_case(workflow, monkeypatch)
    path = workflow.run_path(failed["run_id"] if invalid != "foreign" else verified["run_id"])
    record = workflow.read(path)
    if invalid == "foreign":
        record["task_id"] = "another-task"
    elif invalid == "earlier":
        record["queued_at_utc"] = "2999-01-01T00:00:00+00:00"
    else:
        record["state"] = {"unverified": "launch_unverified"}.get(invalid, invalid)
    save(path, record)
    with pytest.raises(ValueError, match="terminal failed run.*later successful"):
        workflow.resolve_run(
            "repair", failed["run_id"], outcome="superseded", verified_by=verified["run_id"],
            note="No waiver", evidence=[evidence],
        )
    assert not workflow.read(workflow.task_path("repair")).get("run_resolutions")


@pytest.mark.parametrize("invalid", ["empty-note", "no-evidence", "unbound", "unknown-outcome"])
def test_failure_resolution_requires_a_bound_reason(workflow, monkeypatch, invalid):
    failed, verified, evidence = resolution_case(workflow, monkeypatch)
    if invalid == "unbound":
        evidence.write_text('{"accepted":true}')
    with pytest.raises(ValueError):
        workflow.resolve_run(
            "repair", failed["run_id"],
            outcome="ignored" if invalid == "unknown-outcome" else "expected-rejection",
            verified_by=verified["run_id"], note="" if invalid == "empty-note" else "Criterion",
            evidence=[] if invalid == "no-evidence" else [evidence],
        )


def test_failure_resolution_is_append_only(workflow, monkeypatch):
    failed, verified, evidence = resolution_case(workflow, monkeypatch)
    arguments = dict(outcome="superseded", verified_by=verified["run_id"],
                     note="Verified replacement", evidence=[evidence])
    workflow.resolve_run("repair", failed["run_id"], **arguments)
    with pytest.raises(ValueError, match="already has"):
        workflow.resolve_run("repair", failed["run_id"], **arguments)


def test_changed_workflow_code_rejects_queued_execution(workflow, monkeypatch):
    record = prepare_run(
        workflow, monkeypatch, [sys.executable, "-c", "print('must not execute')"]
    )
    record["workflow_code_sha256"] = "different revision"
    save(workflow.run_path(record["run_id"]), record)
    assert workflow.execute(record["run_id"]) == 78
    terminal = workflow.read(workflow.run_path(record["run_id"]))
    assert terminal["state"] == "failed"
    assert not Path(record["log"]).exists()


def test_missing_start_receipt_stays_unverified(tmp_path):
    record = {
        "state": "queued",
        "queued_at_utc": "2000-01-01T00:00:00+00:00",
        "exit_code": None,
        "log": str(tmp_path / "missing.log"),
    }
    observed = run_view(record)
    assert observed["state"] == "launch_unverified" and observed["exit_code"] is None


def test_reconciled_run_can_retry_while_preserving_unknown_exit(workflow, monkeypatch):
    record = prepare_run(workflow, monkeypatch, ["true"])
    record["queued_at_utc"] = "2000-01-01T00:00:00+00:00"
    save(workflow.run_path(record["run_id"]), record)
    evidence = workflow.root / "recovery.json"
    evidence.write_text('{"old_job_stopped":true}')
    monkeypatch.setattr(
        workflow_module,
        "command",
        lambda *args, **kwargs: "ActiveState=inactive\nLoadState=not-found\nMainPID=0",
    )
    recovered = workflow.reconcile(
        record["run_id"], "checked original progress", [evidence]
    )
    assert recovered["state"] == "interrupted" and recovered["exit_code"] is None
    retry = workflow.launch("repair", ["true"], "systemd", "check")
    assert retry["run_id"] != record["run_id"]
    assert workflow.execute(retry["run_id"]) == 0
    assert (
        workflow.update_task("repair", state="complete", evidence=[evidence])["state"]
        == "complete"
    )


def test_reconciliation_cannot_override_live_runner(workflow, monkeypatch):
    record = prepare_run(workflow, monkeypatch, ["true"])
    record["queued_at_utc"] = "2000-01-01T00:00:00+00:00"
    record["process"] = workflow_module.process_identity(os.getpid())
    save(workflow.run_path(record["run_id"]), record)
    evidence = workflow.root / "recovery.json"
    evidence.write_text("evidence")
    with pytest.raises(ValueError, match="still alive"):
        workflow.reconcile(record["run_id"], "checked", [evidence])


def test_reconciliation_rejects_live_supervisor(workflow, monkeypatch):
    record = prepare_run(workflow, monkeypatch, ["true"])
    record["queued_at_utc"] = "2000-01-01T00:00:00+00:00"
    save(workflow.run_path(record["run_id"]), record)
    evidence = workflow.root / "recovery.json"
    evidence.write_text("evidence")
    monkeypatch.setattr(
        workflow_module,
        "command",
        lambda *args, **kwargs: "ActiveState=active\nMainPID=123",
    )
    with pytest.raises(ValueError, match="verified stopped"):
        workflow.reconcile(record["run_id"], "checked", [evidence])


def test_pid_reuse_and_new_boot_remain_interrupted(tmp_path, monkeypatch):
    identity = {"pid": os.getpid(), "start_ticks": "1", "boot_id": "old"}
    monkeypatch.setattr(
        workflow_module, "process_identity", lambda _pid: {**identity, "boot_id": "new"}
    )
    record = {
        "state": "running",
        "process": identity,
        "exit_code": None,
        "log": str(tmp_path / "missing.log"),
    }
    observed = run_view(record)
    assert observed["state"] == "interrupted" and observed["exit_code"] is None
    assert record["state"] == "running"


def test_termination_forwards_to_child_and_persists_cancellation(workflow, monkeypatch):
    record = prepare_run(
        workflow,
        monkeypatch,
        [
            sys.executable,
            "-c",
            "import time; print('ready', flush=True); time.sleep(30)",
        ],
    )
    code = "from pathlib import Path; import sys; from scripts.agent_workflow import Workflow; raise SystemExit(Workflow(Path(sys.argv[1]), Path(sys.argv[2])).execute(sys.argv[3]))"
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            code,
            str(workflow.root),
            str(workflow.state_dir),
            record["run_id"],
        ],
        cwd=Path(__file__).resolve().parents[1],
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if (
                Path(record["log"]).exists()
                and "ready" in Path(record["log"]).read_text()
            ):
                break
            time.sleep(0.02)
        else:
            pytest.fail("child never became ready")
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=5) == 143
        terminal = workflow.read(workflow.run_path(record["run_id"]))
        assert terminal["state"] == "cancelled"
        assert terminal["exit_code"] == -signal.SIGTERM
        assert terminal["received_signals"] == [signal.SIGTERM]
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        terminal = json.loads(workflow.run_path(record["run_id"]).read_text())
        child = terminal.get("child_process")
        if child and workflow_module.process_identity(child["pid"]) == child:
            os.killpg(child["pid"], signal.SIGKILL)
