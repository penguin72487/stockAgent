from __future__ import annotations

import json
import fcntl
import subprocess
import sys
import time

import pytest

from scripts import refresh_crypto_training_dataset as refresh


@pytest.fixture(autouse=True)
def isolated_source_lease(tmp_path, monkeypatch):
    monkeypatch.setattr(refresh, "ROOT", tmp_path)
    monkeypatch.setattr(refresh.releases, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(refresh, "_bybit_source_entry", lambda: {
        "dataset": "bybit", "source_coordination_lock": "bybit.lock",
    })


def test_mid_run_raw_writer_defers_without_publishing(tmp_path, monkeypatch) -> None:
    writer_snapshots = iter([[], [], ["321:download_bybit_perp_1m.py"]])
    commands: list[list[str]] = []
    monkeypatch.setattr(refresh, "active_raw_writers", lambda *_args: next(writer_snapshots))
    monkeypatch.setattr(
        refresh,
        "_run_step_process",
        lambda command, **_kwargs: commands.append(command),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "refresh_crypto_training_dataset.py",
            "--publish",
            "--output-dir",
            str(tmp_path),
        ],
    )

    refresh.main()

    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == "deferred_raw_writer"
    assert receipt["active_writers"] == ["321:download_bybit_perp_1m.py"]
    assert receipt["finished_at_utc"]
    assert len(receipt["steps"]) == 1
    assert len(receipt["step_attempts"]) == 1
    assert receipt["step_attempts"][0]["status"] == "process_completed"
    assert receipt["step_attempts"][0]["elapsed_seconds"] >= 0
    assert len(commands) == 1
    assert "publish" not in commands[0]


def test_materialization_signature_detects_atomic_source_replacement(tmp_path) -> None:
    source = tmp_path / "data_bybit/1m/BTCUSDT_features.parquet"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"old")
    funding = tmp_path / "data_bybit/funding/BTCUSDT_funding.parquet"
    funding.parent.mkdir(parents=True)
    funding.write_bytes(b"funding")
    (funding.parent / "instruments.csv").write_bytes(b"code\nBTCUSDT\n")
    (funding.parent / "funding_coverage.csv").write_bytes(b"symbol\nBTCUSDT\n")
    first = refresh.materialization_input_signature(tmp_path)
    replacement = source.with_suffix(".new")
    replacement.write_bytes(b"new")
    replacement.replace(source)
    second = refresh.materialization_input_signature(tmp_path)
    assert first["files"] == second["files"] == 4
    assert first["metadata_sha256"] != second["metadata_sha256"]


def test_source_change_during_materialization_defers_without_publishing(
    tmp_path, monkeypatch
) -> None:
    commands: list[list[str]] = []
    signatures = iter(
        [
            {"files": 2, "metadata_sha256": "before"},
            {"files": 2, "metadata_sha256": "after"},
        ]
    )
    monkeypatch.setattr(refresh, "active_raw_writers", lambda *_args: [])
    monkeypatch.setattr(
        refresh, "materialization_input_signature", lambda: next(signatures)
    )
    monkeypatch.setattr(
        refresh,
        "_run_step_process",
        lambda command, **_kwargs: commands.append(command),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "refresh_crypto_training_dataset.py",
            "--publish",
            "--output-dir",
            str(tmp_path),
        ],
    )

    refresh.main()

    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == "deferred_source_changed"
    assert receipt["input_signature_before"]["metadata_sha256"] == "before"
    assert receipt["input_signature_after"]["metadata_sha256"] == "after"
    assert len(receipt["steps"]) == 1
    assert len(receipt["step_attempts"]) == 2
    assert all(row["status"] == "process_completed" for row in receipt["step_attempts"])
    assert len(commands) == 2
    assert all("publish" not in command for command in commands)


def test_failed_stage_keeps_elapsed_and_command_in_receipt(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(refresh, "active_raw_writers", lambda *_args: [])

    def fail(command, **_kwargs):
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(refresh, "_run_step_process", fail)
    monkeypatch.setattr(
        sys, "argv", ["refresh_crypto_training_dataset.py", "--output-dir", str(tmp_path)],
    )

    with pytest.raises(subprocess.CalledProcessError):
        refresh.main()

    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == "failed"
    assert receipt["core_state"] == "failed"
    assert receipt["steps"] == []
    assert len(receipt["step_attempts"]) == 1
    assert receipt["step_attempts"][0]["status"] == "failed"
    assert receipt["step_attempts"][0]["elapsed_seconds"] >= 0
    assert receipt.get("active_step") is None


def _mock_complete_core(tmp_path, monkeypatch, *, publish=True, busy_venue=None):
    commands = []
    monkeypatch.setattr(refresh, "materialization_input_signature", lambda: {"files": 2, "metadata_sha256": "stable"})
    monkeypatch.setattr(refresh, "active_raw_writers", lambda names: (
        [f"12:download_{busy_venue}_perp_1m.py"]
        if busy_venue and f"download_{busy_venue}_perp_1m.py" in names else []
    ))

    def step(command, *, deadline, source_lock_fd):
        commands.append(command)
        with (tmp_path / "bybit.lock").open("a+b") as other:
            if source_lock_fd is not None:
                assert deadline > time.monotonic()
                with pytest.raises(BlockingIOError):
                    fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
                if "--source-lock-fd" in command:
                    assert command[-1] == str(source_lock_fd)
                    with refresh.releases._source_coordination_lock(
                        refresh._bybit_source_entry(), inherited_fd=source_lock_fd,
                    ):
                        pass
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                if command[1].endswith("retry_packed_syncthing_scans.py"):
                    assert deadline > time.monotonic()
                    assert command[-1] == "--batch-object-paths"
                else:
                    assert deadline is None
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if command[1].endswith("report_crypto_training_features.py"):
            report_dir = tmp_path / "bybit"
            report_dir.mkdir()
            (report_dir / "summary.json").write_text(json.dumps({
                "source_receipt_hashes_match": True,
                "materialization_current_to_raw": True,
            }))
        if "--result-receipt" in command:
            refresh.Path(command[command.index("--result-receipt") + 1]).write_text(json.dumps({
                "published": [{"dataset": "bybit", "snapshot_id": "test-release", "inventory_sha256": "a" * 64,
                               "scan_policy": "durably_queued"}],
                "skipped": [],
            }))
        if "--receipt" in command:
            refresh.Path(command[command.index("--receipt") + 1]).write_text(json.dumps({
                "dataset": "bybit", "status": "scan_request_acknowledged",
                "requested_scan_policy": {"batch_object_paths": True},
                "peer_convergence": "not_checked",
            }))

    monkeypatch.setattr(refresh, "_run_step_process", step)
    cold = tmp_path / "cold"
    cold.mkdir()
    (cold / ".stockagent-d-primary").touch()
    monkeypatch.setattr(sys, "argv", ["refresh", "--sync-root", str(cold), "--output-dir", str(tmp_path)] + (["--publish"] if publish else []))
    return commands


def test_core_and_publisher_share_lease_but_reports_do_not(tmp_path, monkeypatch):
    commands = _mock_complete_core(tmp_path, monkeypatch)
    refresh.main()
    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == "completed"
    assert receipt["core_state"] == "published"
    assert receipt["publication_state"] == "published"
    assert receipt["publication_result"]["snapshot_id"] == "test-release"
    assert receipt["transport_state"] == "request_acknowledged"
    assert receipt["transport_result"]["requested_scan_policy"] == {"batch_object_paths": True}
    assert len(commands) == 9
    assert "publish_data_releases.py" in commands[4][1]
    assert {r["name"] for r in receipt["reports"]} == {"coverage", "okx", "binance"}
    assert all(r["state"] == "completed" for r in receipt["reports"])
    with (tmp_path / "bybit.lock").open("a+b") as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_unrelated_live_venue_defers_reports_not_bybit_core(tmp_path, monkeypatch):
    commands = _mock_complete_core(tmp_path, monkeypatch, busy_venue="binance")
    refresh.main()
    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["core_state"] == "published"
    assert receipt["state"] == "completed_with_report_deferrals"
    assert {r["name"]: r["state"] for r in receipt["reports"]} == {
        "coverage": "deferred_raw_writer", "okx": "completed",
        "binance": "deferred_raw_writer",
    }
    assert len(commands) == 7


def test_unchanged_generation_reuses_result_without_relabelling_old_step_timings(tmp_path, monkeypatch):
    commands = _mock_complete_core(tmp_path, monkeypatch)
    refresh.main()
    before = json.loads((tmp_path / "refresh_receipt.json").read_text())
    refresh.main()
    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert len(commands) == 10
    assert receipt["no_op"] is True
    assert receipt["reused_previous_started_at_utc"] == before["started_at_utc"]
    assert len(receipt["steps"]) == len(receipt["step_attempts"]) == 1
    assert receipt["step_attempts"][0]["command"][1].endswith("retry_packed_syncthing_scans.py")
    assert receipt["publication_result"] == before["publication_result"]


def test_busy_source_lease_defers_before_any_child(tmp_path, monkeypatch):
    commands = _mock_complete_core(tmp_path, monkeypatch)
    with (tmp_path / "bybit.lock").open("a+b") as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        refresh.main()
    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == "deferred_source_lock"
    assert commands == []


def test_lease_budget_defer_releases_lock_and_never_publishes(tmp_path, monkeypatch):
    _mock_complete_core(tmp_path, monkeypatch)

    def timeout(command, **_kwargs):
        raise subprocess.TimeoutExpired(command, 150)

    monkeypatch.setattr(refresh, "_run_step_process", timeout)
    refresh.main()
    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == "deferred_source_lease_budget"
    assert receipt["core_state"] != "published"
    assert len(receipt["step_attempts"]) == 1
    with (tmp_path / "bybit.lock").open("a+b") as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_step_timeout_stops_process_group_before_releasing_source_fd(tmp_path):
    marker = tmp_path / "child_pid"
    code = (
        "import os, subprocess, sys, time; "
        "p=subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], "
        "pass_fds=(int(sys.argv[2]),)); "
        "open(sys.argv[1], 'w').write(str(p.pid)); time.sleep(30)"
    )
    with refresh.releases._source_coordination_lock(refresh._bybit_source_entry()) as fd:
        with pytest.raises(subprocess.TimeoutExpired):
            refresh._run_step_process(
                [sys.executable, "-c", code, str(marker), str(fd)],
                deadline=time.monotonic() + 0.5, source_lock_fd=fd,
            )
        with (tmp_path / "bybit.lock").open("a+b") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
    child_pid = int(marker.read_text())
    # An adopted killed child can briefly remain a zombie, but cannot write.
    for _ in range(50):
        try:
            state = (refresh.Path("/proc") / str(child_pid) / "stat").read_text().split(") ", 1)[1].split()[0]
        except FileNotFoundError:
            break
        if state == "Z":
            break
        time.sleep(0.01)
    else:
        pytest.fail("timed-out descendant is still running")
    with (tmp_path / "bybit.lock").open("a+b") as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


@pytest.mark.parametrize("failure", ["timeout", "nonzero", "missing_receipt"])
def test_publisher_failure_does_not_assert_that_no_head_committed(tmp_path, monkeypatch, failure):
    _mock_complete_core(tmp_path, monkeypatch)
    original = refresh._run_step_process

    def publish_failure(command, **kwargs):
        if "--result-receipt" not in command:
            return original(command, **kwargs)
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, 150)
        if failure == "nonzero":
            raise subprocess.CalledProcessError(2, command)
        return None

    monkeypatch.setattr(refresh, "_run_step_process", publish_failure)
    if failure == "timeout":
        refresh.main()
    else:
        with pytest.raises((subprocess.CalledProcessError, FileNotFoundError)):
            refresh.main()
    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["publication_state"] == "outcome_unknown"
    assert receipt["core_state"] == "publication_unknown"
    assert "publication_result" not in receipt
    assert receipt["reports"] == []


def test_post_publish_source_change_keeps_release_evidence_but_requires_reconciliation(tmp_path, monkeypatch):
    _mock_complete_core(tmp_path, monkeypatch)
    original = refresh._run_step_process

    def publish_then_source_changes(command, **kwargs):
        original(command, **kwargs)
        if "--result-receipt" in command:
            monkeypatch.setattr(refresh, "materialization_input_signature", lambda: {"files": 2, "metadata_sha256": "new"})

    monkeypatch.setattr(refresh, "_run_step_process", publish_then_source_changes)
    refresh.main()
    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == receipt["core_state"] == "needs_reconciliation"
    assert receipt["publication_state"] == "published"
    assert receipt["publication_result"]["snapshot_id"] == "test-release"
    assert receipt["reports"] == []


@pytest.mark.parametrize("failure", ["timeout", "nonzero", "bad_receipt"])
def test_transport_failure_cannot_revoke_committed_core(tmp_path, monkeypatch, failure):
    _mock_complete_core(tmp_path, monkeypatch)
    original = refresh._run_step_process

    def scan_fails(command, **kwargs):
        if not command[1].endswith("retry_packed_syncthing_scans.py"):
            return original(command, **kwargs)
        # Source writers may restart during the independent transport phase.
        with (tmp_path / "bybit.lock").open("a+b") as other:
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert kwargs["source_lock_fd"] is None
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, 300)
        if failure == "nonzero":
            raise subprocess.CalledProcessError(1, command)
        original(command, **kwargs)
        refresh.Path(command[command.index("--receipt") + 1]).write_text('{"dataset":"other","status":"scan_request_acknowledged"}')

    monkeypatch.setattr(refresh, "_run_step_process", scan_fails)
    refresh.main()
    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["core_state"] == receipt["publication_state"] == "published"
    assert receipt["state"] == "completed_with_transport_pending"
    assert receipt["transport_state"] == "pending"
    assert receipt["publication_result"]["snapshot_id"] == "test-release"


def test_source_advances_during_scan_without_invalidating_published_generation(tmp_path, monkeypatch):
    _mock_complete_core(tmp_path, monkeypatch)
    original = refresh._run_step_process

    def advancing(command, **kwargs):
        original(command, **kwargs)
        if command[1].endswith("retry_packed_syncthing_scans.py"):
            monkeypatch.setattr(refresh, "materialization_input_signature", lambda: {"files": 9, "metadata_sha256": "new"})

    monkeypatch.setattr(refresh, "_run_step_process", advancing)
    refresh.main()
    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == "completed"
    assert receipt["materialization_input_signature"]["metadata_sha256"] == "stable"


@pytest.mark.parametrize("no_op", [False, True])
@pytest.mark.parametrize("policy", [
    "missing", None, {}, False, {"batch_object_paths": False},
    {"batch_object_paths": 1}, {"batch_object_paths": "true"},
    {"batch_object_paths": True, "unexpected": True},
])
def test_transport_policy_must_match_without_revoking_committed_core(
    tmp_path, monkeypatch, policy, no_op,
):
    _mock_complete_core(tmp_path, monkeypatch)
    if no_op:
        refresh.main()
    original = refresh._run_step_process

    def wrong_policy(command, **kwargs):
        original(command, **kwargs)
        if command[1].endswith("retry_packed_syncthing_scans.py"):
            assert command[-1] == "--batch-object-paths"
            assert kwargs["source_lock_fd"] is None
            path = refresh.Path(command[command.index("--receipt") + 1])
            result = json.loads(path.read_text())
            if policy == "missing":
                result.pop("requested_scan_policy")
            else:
                result["requested_scan_policy"] = policy
            path.write_text(json.dumps(result))

    monkeypatch.setattr(refresh, "_run_step_process", wrong_policy)
    refresh.main()
    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["core_state"] == receipt["publication_state"] == "published"
    assert receipt["publication_result"]["snapshot_id"] == "test-release"
    assert receipt["state"] == "completed_with_transport_pending"
    assert receipt["transport_state"] == "pending"
    assert "requested batch policy" in receipt["transport_error"]
    assert "transport_result" not in receipt
    assert receipt.get("no_op", False) is no_op
