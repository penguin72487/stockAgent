from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import return_remote_legacy_archives as cli
from scripts.check_legacy_return_ready import readiness
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import LegacyArchiveSpec


@pytest.mark.parametrize("returncode,stderr,is_transport", [
    (255, "ssh: connect to host example port 22: No route to host", True),
    (255, "ssh: connect to host example port 22: Connection timed out", True),
    (255, "root@example: Permission denied (publickey).", True),
    (255, "Host key verification failed.", True),
    (255, "a remote artifact-specific error", False),
    (1, "remote artifact says no route to host", False),
])
def test_only_identifiable_ssh_failures_defer_a_whole_cohort(
        monkeypatch, returncode, stderr, is_transport):
    monkeypatch.setattr(cli, "private_control_prefix", lambda: "")
    monkeypatch.setattr(cli, "_ssh_base", lambda *args: ["ssh"])
    monkeypatch.setattr(cli, "_validate_ssh_target", lambda target: target)
    monkeypatch.setattr(cli.subprocess, "run", lambda *args, **kwargs:
                        subprocess.CompletedProcess(args[0], returncode, "", stderr))
    args = SimpleNamespace(identity_file="test-key", ssh_port=22, ssh_target="example")
    with pytest.raises(SnapshotError) as raised:
        cli.remote(args, {"action": "observe", "relative_root": "markets/a"})
    assert isinstance(raised.value, cli.RemoteTransportUnavailable) is is_transport


@pytest.fixture
def retained_cohort(tmp_path, monkeypatch):
    rows = [dict(relative_root=f"markets/{name}", state="inventoried", files=1,
                 logical_bytes=10, newest_mtime_ns=1, fingerprint=name, rows=[])
            for name in ("a", "b", "c")]
    (tmp_path / "inventory.json").write_text(json.dumps({"items": rows}))
    (tmp_path / "archive-catalog.json").write_text("{}")
    (tmp_path / "cohort-owner.lock").touch()
    specs = {cli.dataset_name(row["relative_root"]): LegacyArchiveSpec(
        cli.dataset_name(row["relative_root"]), row["relative_root"], 7, tmp_path / "encoded")
        for row in rows}
    monkeypatch.setattr(cli, "load_legacy_specs", lambda *_: specs)
    monkeypatch.setattr(cli, "_check_d_primary_mount", lambda *_: None)
    args = SimpleNamespace(state_root=tmp_path, sync_root=tmp_path / "packed", apply=True,
                           reuse_progress=None, include_root=[], max_items=None,
                           order="smallest", command="archive")
    policy = {"minimum_stable_hours": 12, "maximum_item_bytes": 100}
    return args, policy


def test_transport_defers_only_one_item_and_timer_can_resume(retained_cohort, monkeypatch):
    args, policy = retained_cohort
    calls = []

    def unavailable(_args, _policy, row, _spec):
        calls.append(row["relative_root"])
        raise cli.RemoteTransportUnavailable("ssh: No route to host")

    monkeypatch.setattr(cli, "archive_one", unavailable)
    assert cli.run_cohort(args, policy) == 0
    progress = json.loads((args.state_root / "progress.json").read_text())
    summary = json.loads((args.state_root / "summary.json").read_text())
    assert calls == ["markets/a"]
    assert progress["state"] == summary["state"] == "deferred-remote-transport"
    assert summary["deferred_reason"] == "remote_transport_unavailable"
    assert [row["state"] for row in progress["items"]] == [
        "return-failed-source-preserved", "inventoried", "inventoried"]
    assert len(list((args.state_root / "attempts").iterdir())) == 1
    assert readiness(args.state_root) == (True, "retained_cohort_has_retryable_items")

    def recovered(_args, _policy, row, _spec):
        calls.append(row["relative_root"])
        row["state"] = "remote-source-retired"

    monkeypatch.setattr(cli, "archive_one", recovered)
    monkeypatch.setattr(cli, "_probe_remote_transport", lambda _args: True)
    assert cli.run_cohort(args, policy) == 0
    progress = json.loads((args.state_root / "progress.json").read_text())
    assert calls == ["markets/a", "markets/a", "markets/b", "markets/c"]
    assert progress["state"] == "batch-complete"
    assert "deferred_reason" not in progress and "deferred_at_epoch" not in progress
    assert any("No route to host" in item.get("error", "") for item in
               (json.loads(path.read_text()) for path in (args.state_root / "attempts").iterdir()))


def test_artifact_specific_failure_does_not_defer_other_items(retained_cohort, monkeypatch):
    args, policy = retained_cohort
    calls = []

    def artifact_failure(_args, _policy, row, _spec):
        calls.append(row["relative_root"])
        raise SnapshotError("artifact needs separate review")

    monkeypatch.setattr(cli, "archive_one", artifact_failure)
    assert cli.run_cohort(args, policy) == 0
    progress = json.loads((args.state_root / "progress.json").read_text())
    assert calls == ["markets/a", "markets/b", "markets/c"]
    assert progress["state"] == "batch-finished-with-protected-items"
    assert "deferred_reason" not in progress


def test_transport_after_cold_verification_keeps_audit_pending(retained_cohort, monkeypatch):
    args, policy = retained_cohort

    def disconnected_after_verification(_args, _policy, row, _spec):
        row["cold_verified"] = True
        raise cli.RemoteTransportUnavailable("ssh: Connection reset by peer")

    monkeypatch.setattr(cli, "archive_one", disconnected_after_verification)
    assert cli.run_cohort(args, policy) == 0
    progress = json.loads((args.state_root / "progress.json").read_text())
    assert progress["items"][0]["state"] == "cold-verified-needs-audit"
    assert progress["items"][0]["cold_verified"] is True


def test_repeated_outage_skips_inventory_and_preserves_item_receipts(retained_cohort, monkeypatch):
    args, policy = retained_cohort

    def fail_item(_args, _policy, _row, _spec):
        raise cli.RemoteTransportUnavailable("No route to host")

    monkeypatch.setattr(cli, "archive_one", fail_item)
    assert cli.run_cohort(args, policy) == 0
    path = args.state_root / "progress.json"
    first = json.loads(path.read_text())
    attempts = {p.name: p.read_bytes() for p in (args.state_root / "attempts").iterdir()}

    def still_unavailable(_args):
        raise cli.RemoteTransportUnavailable("No route to host")

    monkeypatch.setattr(cli, "_probe_remote_transport", still_unavailable)
    monkeypatch.setattr(cli, "load_legacy_specs", lambda *_: pytest.fail("inventory must stay unopened"))
    assert cli.run_cohort(args, policy) == 0
    second = json.loads(path.read_text())
    assert second["items"] == first["items"]
    assert second["state"] == "deferred-remote-transport"
    assert second["transport_probe_count"] == 1
    assert {p.name: p.read_bytes() for p in (args.state_root / "attempts").iterdir()} == attempts
    assert readiness(args.state_root) == (True, "retained_cohort_has_retryable_items")


def test_probe_is_read_only_and_unknown_failure_keeps_normal_item_checks(monkeypatch):
    monkeypatch.setattr(cli, "_ssh_base", lambda *args: ["ssh"])
    monkeypatch.setattr(cli, "_validate_ssh_target", lambda target: target)
    calls = []

    def unknown(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 255, "", "artifact-specific failure")

    monkeypatch.setattr(cli.subprocess, "run", unknown)
    args = SimpleNamespace(identity_file="test-key", ssh_port=22, ssh_target="example")
    assert cli._probe_remote_transport(args) is False
    assert calls[0][0] == ["ssh", "example", "true"]
    assert calls[0][1]["timeout"] == 30


def test_timed_out_read_only_probe_defers_safely(monkeypatch):
    monkeypatch.setattr(cli, "_ssh_base", lambda *args: ["ssh"])
    monkeypatch.setattr(cli, "_validate_ssh_target", lambda target: target)

    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], 30)

    monkeypatch.setattr(cli.subprocess, "run", timeout)
    args = SimpleNamespace(identity_file="test-key", ssh_port=22, ssh_target="example")
    with pytest.raises(cli.RemoteTransportUnavailable, match="probe timed out"):
        cli._probe_remote_transport(args)


def test_cancelled_loose_and_directory_entries_do_not_block_other_retry(retained_cohort):
    args, _ = retained_cohort
    cancelled = {"relative_root": "markets/old.log", "state": "testing-preservation-cancelled-by-user",
                 "bytes": 10, "signature": [64, 2, 10, 1, 1, 33204, 1], "recovery_claimed": False}
    pending = {"relative_root": "markets/a", "state": "return-failed-source-preserved", "files": 1}
    path = args.state_root / "progress.json"
    path.write_text(json.dumps({"items": [cancelled, pending]}))
    before = path.read_bytes()
    assert readiness(args.state_root) == (True, "retained_cohort_has_retryable_items")
    assert path.read_bytes() == before
    cancelled_directory = {"relative_root": "markets/cancelled", "files": 4,
                           "state": "testing-preservation-cancelled-by-user"}
    path.write_text(json.dumps({"items": [cancelled, cancelled_directory]}))
    assert readiness(args.state_root) == (False, "no_retryable_items_in_retained_cohort")
    cancelled["signature"][2] += 1
    path.write_text(json.dumps({"items": [cancelled, pending]}))
    assert readiness(args.state_root) == (False, "progress_items_invalid")


def test_selected_readiness_works_as_direct_systemd_script(retained_cohort):
    from stockagent.data_sync.desync_snapshots import sha256_file
    args, _ = retained_cohort
    state = args.state_root
    (state / "progress.json").write_text(json.dumps({"items": [
        {"relative_root": "markets/a", "files": 1, "state": "return-failed-source-preserved"}]}))
    selection = state / "selection.json"
    selection.write_text(json.dumps({"schema_version": 1,
        "inventory_sha256": sha256_file(state / "inventory.json"), "relative_roots": ["markets/a"]}))
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    repo = Path(cli.__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, str(repo / "scripts/check_legacy_return_ready.py"),
                             str(state), "--selection", str(selection)], cwd=repo, env=env,
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"ready": True, "reason": "retained_cohort_has_retryable_items"}
