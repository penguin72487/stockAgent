from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import data_cache
from stockagent.data_sync import storage_automation as automation
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def lake_readiness(tmp_path):
    base, receipts = tmp_path / 'state', tmp_path / 'receipts'
    base.mkdir();receipts.mkdir()
    now = 1791170000.0
    timestamp = datetime.fromtimestamp(now, timezone.utc).isoformat()
    for name in ('catalog-status', 'source-replication-status'):
        (base / (name + '.json')).write_text(json.dumps({'state': 'working', 'observed_at_utc': timestamp}))
    (base / 'worker-ready.json').write_text(json.dumps({'code_identity_sha256': 'fixed-current-code'}))
    relay = {'state': 'degraded', 'observed_at_utc': timestamp, 'producer_device_id': 'penguin',
             'receiver_device_id': 'lab203', 'nas_mount_guard_verified': True,
             'runtime_lock_verified': True, 'single_owner_verified': True}
    (receipts / 'relay-status.json').write_text(json.dumps(relay))
    policy = tmp_path / 'private.json'
    policy.write_text(json.dumps({'state_root': str(base), 'receipt_root': str(receipts),
                                 'producer_device_id': 'penguin', 'receiver_device_id': 'lab203'}))
    policy.chmod(0o600)
    owners = {name: {'scheduler_active': True, 'scheduler_enabled': True}
              for name in ('temporal_server', 'lake_lifecycle', 'lifecycle_control_backup', 'lake_transport_gc')}
    live = {'status': 'RUNNING', 'worker_code_identity_sha256': 'fixed-current-code',
            'source_replication_workflow': {'status': 'RUNNING'}}
    return {'policy': policy, 'now': now, 'observations': owners, 'probe': lambda: live}, base, receipts, live


def test_selected_lakehouse_runtime_gate_keeps_pending_coverage_separate(lake_readiness):
    args, base, receipts, live = lake_readiness
    result = automation.lakehouse_readiness(**args)
    assert result['ready'] and result['loaded_worker_matches_current_code']
    assert result['full_history_archive_verified'] is False


@pytest.mark.parametrize('damage', ['stale', 'pair', 'loaded_code', 'owner', 'workflow', 'policy_mode'])
def test_lakehouse_runtime_gate_refuses_stale_wrong_peer_code_or_disabled_owner(lake_readiness, damage):
    args, base, receipts, live = lake_readiness
    if damage in ('stale', 'pair'):
        path = receipts / 'relay-status.json'
        value = json.loads(path.read_bytes())
        if damage == 'stale':
            value['observed_at_utc'] = datetime.fromtimestamp(args['now'] - 901, timezone.utc).isoformat()
        else:
            value['receiver_device_id'] = 'unknown-node'
        path.write_text(json.dumps(value))
    elif damage == 'loaded_code':
        live['worker_code_identity_sha256'] = 'different-current-code'
    elif damage == 'owner':
        args['observations']['lake_lifecycle']['scheduler_enabled'] = False
    elif damage == 'workflow':
        live['source_replication_workflow']['status'] = 'FAILED'
    else:
        args['policy'].chmod(0o644)
    assert not automation.lakehouse_readiness(**args)['ready']


def test_status_receipt_is_bounded_and_does_not_expose_secrets(tmp_path):
    path = tmp_path / "status.json"
    path.write_text(json.dumps({"state": "cycle-complete", "published": 1,
                                "api_key": "private-unit-key", "raw_argv": "do-not-print"}))
    status = automation.bounded_receipt(path, ("state", "published"), now=path.stat().st_mtime + 120)
    assert status["read_state"] == "read" and status["receipt_age_seconds"] == 120
    assert status["values"] == {"state": "cycle-complete", "published": 1}
    assert "private-unit-key" not in json.dumps(status) and "raw_argv" not in json.dumps(status)
    alias = tmp_path / "redirected.json"
    alias.symlink_to(path)
    assert automation.bounded_receipt(alias, ("state",), now=0)["read_state"] == "unsafe-or-oversized"
    monkey = tmp_path / "large.json"
    with monkey.open("wb") as handle:
        handle.truncate(automation.RECEIPT_LIMIT + 1)
    assert automation.bounded_receipt(monkey, ("state",), now=0)["read_state"] == "unsafe-or-oversized"


def test_status_rejects_missing_and_invalid_receipts_without_false_success(tmp_path):
    path = tmp_path / "status.json"
    assert automation.bounded_receipt(path, ("state",), now=0)["read_state"] == "missing"
    path.write_text("not-json")
    assert automation.bounded_receipt(path, ("state",), now=0)["read_state"] == "unreadable-or-invalid"
    path.write_text("[]")
    assert automation.bounded_receipt(path, ("state",), now=0)["read_state"] == "invalid"


def test_systemd_status_only_runs_read_only_show_and_preserves_last_failure(monkeypatch):
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(stdout="Id=existing.timer\nActiveState=active\nUnitFileState=enabled\n\n"
            "Id=existing.service\nActiveState=inactive\nResult=exit-code\nExecMainStatus=75\n")
    monkeypatch.setattr(automation.subprocess, "run", run)
    status = automation.systemd_observations({"delivery": "existing"})
    assert status["delivery"]["scheduler_active"] and status["delivery"]["scheduler_enabled"]
    assert status["delivery"]["service"]["Result"] == "exit-code"
    assert calls[0][:2] == ["systemctl", "show"]
    assert "start" not in calls[0] and "restart" not in calls[0]


def test_cron_status_checks_the_known_gc_command_and_running_daemon(tmp_path):
    path = tmp_path / "cron"
    command = tmp_path / "run_data_cache.sh"
    proc = tmp_path / "proc"
    (proc / "7").mkdir(parents=True)
    (proc / "7/comm").write_text("cron\n")
    path.write_text(f"*/5 * * * * root {command} gc >>/var/log/cache.log 2>&1\n")
    path.chmod(0o644)
    status = automation.cron_observation(path, command, arguments=("gc",), proc_root=proc)
    assert status["scheduler_enabled"] and status["scheduler_active"] and status["schedule"] == "*/5 * * * *"
    path.write_text(f"*/5 * * * * root {command} --help\n")
    assert not automation.cron_observation(path, command, arguments=("gc",), proc_root=proc)["scheduler_enabled"]
    path.write_text(f"*/5 * * * * root {command} gc --force\n")
    assert not automation.cron_observation(path, command, arguments=("gc",), proc_root=proc)["scheduler_enabled"]


def test_penguin_automation_status_does_not_claim_delivery_or_recovery(tmp_path, monkeypatch):
    state = tmp_path / "state"
    etc = tmp_path / "etc"
    systemd = tmp_path / "systemd"
    systemd.mkdir()
    owner = state / "stockagent-cold-artifacts"
    owner.mkdir(parents=True)
    (owner / "remote-ingress-status.json").write_text(json.dumps({
        "state": "cycle-running", "phase": "publishing-d-cold", "current_root": "markets/exact-run",
        "automation_contract": "cold-return-v3-full-convergence", "api_key": "never-show"}))
    monkeypatch.setattr(automation, "systemd_observations", lambda jobs: {})
    def missing(_root):
        raise SnapshotError("private-mount-error-detail")
    monkeypatch.setattr("stockagent.data_sync.cold_primary._check_d_primary_mount", missing)
    monkeypatch.setattr("scripts.configure_artifact_ingress_syncthing.credentials", lambda: pytest.fail("default status queried API"))
    result = automation.automation_status(tmp_path, tmp_path / "cold", state_base=state, etc_root=etc, systemd_root=systemd)
    assert result["cold_primary"]["state"] == "guard-failed"
    assert result["transport"]["state"] == "not-live-checked"
    assert result["automatic_materialization"] is False and result["cold_object_gc"] is False
    assert result["receipts"]["completed_training_return"]["values"]["phase"] == "publishing-d-cold"
    assert "never-show" not in json.dumps(result) and "private-mount-error-detail" not in json.dumps(result)


def test_control_backup_projection_uses_the_existing_receipt_contract(tmp_path, monkeypatch):
    state = tmp_path / 'state'
    path = state / 'stockagent/control-plane/backups/latest.json'
    path.parent.mkdir(parents=True)
    value = {'state': 'backup_written', 'sha256': 'a' * 64,
             'logical_state_identity_sha256': 'b' * 64, 'same_mvcc_snapshot_as_dump': True,
             'archive': '/private/dump', 'password': 'never-show'}
    path.write_text(json.dumps(value))
    monkeypatch.setattr('stockagent.data_sync.cold_primary._check_d_primary_mount', lambda _: None)
    result = automation.automation_status(tmp_path, tmp_path / 'cold', state_base=state,
        etc_root=tmp_path / 'etc', systemd_root=tmp_path / 'no-systemd')
    observed = result['receipts']['control_backup']['values']
    assert observed['sha256'] == value['sha256']
    assert observed['logical_state_identity_sha256'] == value['logical_state_identity_sha256']
    assert observed['same_mvcc_snapshot_as_dump'] is True
    assert 'password' not in observed and 'archive' not in observed


def test_edge_automation_observes_live_convergence_without_hydration(tmp_path, monkeypatch):
    state = tmp_path / "state"
    (state / "stockagent-packed-edge").mkdir(parents=True)
    (state / "stockagent-packed-edge/state.json").write_text("{}")
    calls = []
    monkeypatch.setattr("scripts.configure_artifact_ingress_syncthing.credentials", lambda: ("http://local", "private-unit-key"))
    def convergence(base, key, folder, peer):
        calls.append((folder, peer))
        return {"ok": False, "checks": {"folder_idle": False}, "folder_state": "scanning", "completion": 100,
                "transport": "quic-client"}
    monkeypatch.setattr("scripts.manage_packed_edge._convergence", convergence)
    monkeypatch.setattr("stockagent.data_sync.cold_primary._check_d_primary_mount", lambda *a: pytest.fail("edge checked penguin D mount"))
    result = automation.automation_status(tmp_path, tmp_path / "cold", state_base=state,
        etc_root=tmp_path / "etc", proc_root=tmp_path / "proc", live=True)
    assert calls == [("stockagent-packed", "penguin")]
    assert result["transport"]["ok"] is False and result["role"] == "index-only-edge"
    assert "private-unit-key" not in json.dumps(result)
    assert result["automatic_materialization"] is False


def test_automation_status_cli_is_explicit_and_read_only(monkeypatch, capsys):
    parser = data_cache.build_parser()
    args = parser.parse_args(["automation-status", "--live"])
    assert args.command == "automation-status" and args.live
    calls = []
    def status(repo, cold, **options):
        calls.append(options)
        return {"schema_version": 1, "cold_object_gc": False}
    monkeypatch.setattr(automation, "automation_status", status)
    assert data_cache.main(["automation-status", "--live"]) == 0
    assert calls == [{"live": True}]
    assert json.loads(capsys.readouterr().out)["cold_object_gc"] is False
