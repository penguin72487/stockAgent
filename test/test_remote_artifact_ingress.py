from __future__ import annotations

import json
import fcntl
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import ingest_remote_cold_artifacts as ingress
from scripts.ingest_remote_cold_artifacts import (
    RemoteCandidate,
    _decode_discovery,
    _resolved_release,
    _spec,
    _staging_artifact_root,
    _validate_remote_absolute,
    _validate_ssh_target,
    eligible_candidates,
)
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_ingress_identity_override_is_scoped_and_explicit_cli_wins(monkeypatch):
    monkeypatch.setenv('COLD_ARTIFACT_INGRESS_IDENTITY_FILE', '/keys/original')
    monkeypatch.delenv('STOCKAGENT_REMOTE_INGRESS_IDENTITY_FILE', raising=False)
    assert ingress.build_parser().parse_args([]).identity_file == Path('/keys/original')
    monkeypatch.setenv('STOCKAGENT_REMOTE_INGRESS_IDENTITY_FILE', '/keys/return-only')
    assert ingress.build_parser().parse_args([]).identity_file == Path('/keys/return-only')
    assert ingress.build_parser().parse_args(['--identity-file', '/keys/explicit']).identity_file == Path('/keys/explicit')
    monkeypatch.setenv('STOCKAGENT_REMOTE_INGRESS_IDENTITY_FILE', '')
    assert ingress.build_parser().parse_args([]).identity_file == Path('/keys/original')


def test_running_and_failed_cycles_preserve_only_last_completed_counts(tmp_path):
    status = tmp_path/'status.json'
    status.write_text(json.dumps({'automation_contract': ingress.AUTOMATION_CONTRACT,
        'state': 'cycle-complete', 'apply': True, 'observed_at_epoch': 1234,
        'observed': 5, 'eligible': 3, 'published': 1,
        'pending_return_count': 0, 'pending_return_limit': 32, 'deferred_return_count': 29,
        'rows': [{'action': 'published', 'relative_root': '/secret'},
                 {'action': 'return-cycle-limit', 'receipt': '/secret/file'}]}))
    args = SimpleNamespace(apply=True, output=status, state_root=tmp_path)
    ingress.cycle_progress(args, 'discovering-completed-runs')
    value = json.loads(status.read_text())
    assert value['last_completed_summary']['deferred'] == 1
    assert value['last_completed_summary']['published'] == 1
    assert value['last_completed_summary']['deferred_return_count'] == 29
    assert '/secret' not in json.dumps(value)
    ingress.cycle_progress(args, 'publishing-d-cold')
    failure = ingress.cycle_failure(args, ValueError('private message'))
    assert failure['last_completed_summary'] == value['last_completed_summary']
    assert 'private message' not in json.dumps(failure)
    alias = tmp_path/'alias.json'
    alias.symlink_to(status)
    assert ingress._last_completed_cycle(alias) is None


def test_return_workspace_keeps_native_scratch_compatibility(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(ingress, "admit_workspace", lambda path, required: calls.append((path, required)))
    args = SimpleNamespace(state_root=tmp_path / "state")
    assert ingress.return_staging_workspace(args, 123) == args.state_root
    assert calls == [(args.state_root, 123)]


def test_e_return_workspace_checks_enrollment_and_capacity(tmp_path, monkeypatch):
    from stockagent.data_sync import workspace_volume

    root = tmp_path / "e"
    (root / "restore").mkdir(parents=True)
    stage = root / "restore" / "completed-return-staging"
    monkeypatch.setattr(ingress, "E_WORKSPACE_MOUNT", root)
    monkeypatch.setattr(ingress, "E_RETURN_PAYLOAD_SCRATCH", stage)
    calls = []
    monkeypatch.setattr(workspace_volume, "admit", lambda required: calls.append(required))
    args = SimpleNamespace(state_root=tmp_path / "state", staging_root=stage,
                           prestage_root="markets/exact/run")
    assert ingress.return_staging_workspace(args, 123) == stage
    assert calls == [123]
    assert stage.stat().st_mode & 0o777 == 0o700
    normal = SimpleNamespace(staging_root=stage, policy=Path('configs/data_sync/training_return.json'))
    assert ingress.return_staging_workspace(normal, 456) == stage
    assert calls == [123, 456]
    with pytest.raises(SnapshotError, match="requires the completed-return policy"):
        ingress.return_staging_workspace(SimpleNamespace(staging_root=stage), 123)


def test_e_return_workspace_rejects_redirect_before_admission(tmp_path, monkeypatch):
    from stockagent.data_sync import workspace_volume

    root = tmp_path / "e"
    root.mkdir()
    (root / "restore").symlink_to(tmp_path, target_is_directory=True)
    stage = root / "restore" / "completed-return-staging"
    monkeypatch.setattr(ingress, "E_WORKSPACE_MOUNT", root)
    monkeypatch.setattr(ingress, "E_RETURN_PAYLOAD_SCRATCH", stage)
    monkeypatch.setattr(workspace_volume, "admit", lambda *_: pytest.fail("redirect reached admission"))
    with pytest.raises(SnapshotError, match="redirected"):
        ingress.return_staging_workspace(SimpleNamespace(staging_root=stage,
            prestage_root="markets/exact/run"), 123)


def test_e_prestage_fence_rejects_changed_identity(tmp_path, monkeypatch):
    from stockagent.data_sync import workspace_volume

    first, other = tmp_path / "first", tmp_path / "other"
    first.mkdir()
    other.mkdir()
    info = first.stat()
    calls = []
    monkeypatch.setattr(workspace_volume, "admit", lambda required: calls.append(("admit", required)))
    monkeypatch.setattr(workspace_volume, "check", lambda *, physical: calls.append(("check", physical)))
    ingress._fence_e_stage(first, (info.st_dev, info.st_ino), physical=True)
    assert calls == [("admit", 0)]
    with pytest.raises(SnapshotError, match="workspace changed"):
        ingress._fence_e_stage(other, (info.st_dev, info.st_ino), physical=False)
    assert calls[-1] == ("check", False)


def test_content_digest_checks_all_bytes_and_rejects_links(tmp_path):
    left, right = tmp_path / "left", tmp_path / "right"
    left.mkdir()
    right.mkdir()
    (left / "checkpoint").write_bytes(b"abc")
    (right / "checkpoint").write_bytes(b"abc")
    command = lambda root: [sys.executable, "-c", ingress._CONTENT_DIGEST_PROGRAM, str(root)]
    assert ingress._content_digest(command(left)) == ingress._content_digest(command(right))
    (right / "checkpoint").write_bytes(b"xyz")
    assert ingress._content_digest(command(left)) != ingress._content_digest(command(right))
    (right / "link").symlink_to(right / "checkpoint")
    with pytest.raises(subprocess.CalledProcessError):
        ingress._content_digest(command(right))


def test_prestage_exact_run_never_publishes_or_retires_other_sources(tmp_path, monkeypatch, capsys):
    root = "markets/exact/run"
    policy = Path("configs/data_sync/training_return.json").absolute()
    monkeypatch.setattr("sys.argv", ["ingress", "--ssh-target", "root@peer", "--identity-file", str(tmp_path / "key"),
        "--state-root", str(tmp_path / "state"), "--policy", str(policy),
        "--prestage-root", root, "--apply"])
    monkeypatch.setattr(ingress, "retry_training_layout", lambda *_: pytest.fail("prestage moved unrelated data"))
    monkeypatch.setattr(ingress, "publish_cold_artifact", lambda *_a, **_k: pytest.fail("prestage published D"))
    monkeypatch.setattr(ingress, "returned_source", lambda *_a, **_k: pytest.fail("prestage retired source"))
    monkeypatch.setattr(ingress, "_prestage_on_e", lambda args, _policy: {"relative_root": args.prestage_root})
    assert ingress.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["rows"] == [{"relative_root": root}]
    assert result["cold_published"] is False
    assert result["source_roots_retired"] == 0


def test_prestage_validates_exact_bytes_and_reuses_receipt(tmp_path, monkeypatch):
    from stockagent.data_sync.packed_snapshots import _collect_entries

    source = tmp_path / "source"
    source.mkdir()
    (source / "checkpoint.pt").write_bytes(b"model bytes")
    _, summary = _collect_entries(source)
    newest = (source / "checkpoint.pt").stat().st_mtime_ns
    candidate = RemoteCandidate("markets/exact/run", 1, 11,
        summary["portable_fingerprint_sha256"], newest, ())
    e_stage = tmp_path / "e-stage"
    e_stage.mkdir(mode=0o700)
    monkeypatch.setattr(ingress, "E_RETURN_PAYLOAD_SCRATCH", e_stage)
    monkeypatch.setattr(ingress, "return_staging_workspace", lambda *_: e_stage)
    monkeypatch.setattr(ingress, "_fence_e_stage", lambda *_a, **_k: None)
    monkeypatch.setattr(ingress, "discover_remote_artifacts", lambda **_: [candidate])
    transfers = []
    def transfer(item, **kwargs):
        transfers.append(kwargs)
        target = kwargs["destination_artifact_root"] / item.relative_root
        shutil.copytree(source, target, dirs_exist_ok=True)
    monkeypatch.setattr(ingress, "_rsync_candidate", transfer)
    monkeypatch.setattr(ingress, "validate_cold_artifact_source", lambda root, spec: {
        "source": str(root / spec.relative_root), "files": 1,
        "logical_bytes": 11, "newest_mtime_ns": newest})
    digest = {"files": 1, "logical_bytes": 11, "sha256": "c" * 64}
    monkeypatch.setattr(ingress, "_content_digest", lambda _: digest)
    monkeypatch.setattr(ingress, "_ssh_base", lambda *_: ["ssh"])
    monkeypatch.setattr(ingress, "cycle_progress", lambda *_: None)
    args = SimpleNamespace(prestage_root=candidate.relative_root, staging_root=e_stage,
        ssh_target="root@peer", ssh_port=22, identity_file=tmp_path / "key",
        remote_repo_root="/root/stockAgent", remote_artifact_root="/root/stockAgent/artifacts",
        origin_node_id="vastai1T", transfer_compression="none", state_root=tmp_path / "state")
    policy = ingress.load_policy(Path("configs/data_sync/training_return.json")) | {"stable_hours": 0}
    first = ingress._prestage_on_e(args, policy)
    second = ingress._prestage_on_e(args, policy)
    assert len(transfers) == 1 and transfers[0]["delete_extraneous"] is False
    assert first["reused"] is False and second["reused"] is True
    assert first["content_sha256_proof"] == digest
    assert first["cold_published"] is False and first["source_retired"] is False
    assert (source / "checkpoint.pt").read_bytes() == b"model bytes"
    monkeypatch.setattr(ingress, "_content_digest", lambda command: digest if command[0] == "ssh" else digest | {"sha256": "d" * 64})
    with pytest.raises(SnapshotError, match="file SHA256 evidence differs"):
        ingress._prestage_on_e(args, policy)
    assert len(transfers) == 1
    monkeypatch.setattr(ingress, "_content_digest", lambda _: digest)
    changed = RemoteCandidate(candidate.relative_root, candidate.files, candidate.logical_bytes,
        candidate.portable_fingerprint_sha256, candidate.newest_mtime_ns + 1, ())
    observations = iter(([candidate], [changed]))
    monkeypatch.setattr(ingress, "discover_remote_artifacts", lambda **_: next(observations))
    with pytest.raises(SnapshotError, match="source changed"):
        ingress._prestage_on_e(args, policy)


def test_prestage_refuses_busy_or_changed_source(tmp_path, monkeypatch):
    root = "markets/exact/run"
    busy = RemoteCandidate(root, 1, 11, "a" * 64, time.time_ns() - 10**12, ("pid=7:fd:model",))
    monkeypatch.setattr(ingress, "E_RETURN_PAYLOAD_SCRATCH", tmp_path)
    monkeypatch.setattr(ingress, "discover_remote_artifacts", lambda **_: [busy])
    monkeypatch.setattr(ingress, "_rsync_candidate", lambda *_a, **_k: pytest.fail("busy source copied"))
    args = SimpleNamespace(prestage_root=root, staging_root=tmp_path, ssh_target="root@peer",
        ssh_port=22, identity_file=tmp_path / "key", remote_repo_root="/root/stockAgent",
        remote_artifact_root="/root/stockAgent/artifacts")
    policy = ingress.load_policy(Path("configs/data_sync/training_return.json")) | {"stable_hours": 0}
    with pytest.raises(SnapshotError, match="not eligible"):
        ingress._prestage_on_e(args, policy)


@pytest.mark.parametrize("selected", ["/tmp/return", "/srv/stockagent-packed", "/srv/stockagent-packed/.local-state/other"])
def test_return_workspace_rejects_unenrolled_disk_namespace(tmp_path, monkeypatch, selected):
    from stockagent.data_sync import cold_primary
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount",
                        lambda *_: pytest.fail("unadmitted namespace reached the D guard"))
    with pytest.raises(SnapshotError, match="enrolled private D"):
        ingress.return_staging_workspace(SimpleNamespace(state_root=tmp_path, staging_root=Path(selected)), 123)


@pytest.mark.parametrize("source,filesystem,accepted", [("D:", "9p", True), ("D:\\", "9p", True),
    ("C:", "9p", False), ("D:", "ext4", False)])
def test_return_workspace_checks_real_d_mount_before_payload_admission(tmp_path, monkeypatch, source, filesystem, accepted):
    from stockagent.data_sync import cold_primary

    scratch = tmp_path / "stage"
    monkeypatch.setattr(ingress, "RETURN_PAYLOAD_SCRATCH", scratch)
    guarded = []
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", lambda root: guarded.append(root))
    monkeypatch.setattr(ingress.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        stdout=json.dumps({"filesystems": [{"source": source, "fstype": filesystem}]})))
    monkeypatch.setattr(ingress, "admit_workspace", lambda *_: pytest.fail("D payload reached SSD admission"))
    args = SimpleNamespace(state_root=tmp_path / "state", staging_root=scratch)
    if accepted:
        assert ingress.return_staging_workspace(args, 1) == scratch
        with pytest.raises(SnapshotError, match="capacity"):
            ingress.return_staging_workspace(args, 2**63)
    else:
        with pytest.raises(SnapshotError, match="native D volume"):
            ingress.return_staging_workspace(args, 1)
        assert not scratch.exists()
    assert guarded


def test_drvfs_return_clock_projection_keeps_native_and_remote_fences():
    candidate = RemoteCandidate("markets/run", 2, 123, "a" * 64, 1_792_832_424_696_799_341, ())
    exact = {"files": 2, "logical_bytes": 123, "newest_mtime_ns": candidate.newest_mtime_ns}
    projected = {**exact, "newest_mtime_ns": 1_792_832_424_000_000_000}
    assert ingress.staging_observation_matches(exact, candidate, d_payload_scratch=False)
    assert ingress.staging_observation_matches(projected, candidate, d_payload_scratch=True)
    assert not ingress.staging_observation_matches(projected, candidate, d_payload_scratch=False)
    assert not ingress.staging_observation_matches({**projected, "files": 3}, candidate, d_payload_scratch=True)
    assert not ingress.staging_observation_matches({**projected, "logical_bytes": 124}, candidate, d_payload_scratch=True)
    assert not ingress.staging_observation_matches({**projected, "newest_mtime_ns": projected["newest_mtime_ns"] - 1_000_000_000}, candidate, d_payload_scratch=True)


def test_cycle_failure_retains_phase_and_private_stderr_without_command(tmp_path):
    output = tmp_path / "status.json"
    output.write_text(json.dumps({"state": "cycle-running", "automation_contract": ingress.AUTOMATION_CONTRACT,
                                  "phase": "discovering-completed-runs"}))
    args = SimpleNamespace(output=output, state_root=tmp_path, apply=True)
    result = ingress.cycle_failure(args, subprocess.CalledProcessError(
        1, ["ssh", "private-command"], stderr="private-error"))
    assert result["phase"] == "discovering-completed-runs"
    assert "private-command" not in json.dumps(result)
    evidence = Path(result["diagnostics"])
    assert evidence.read_text() == "private-error"
    assert evidence.stat().st_mode & 0o777 == 0o600


def test_unobservable_consumers_allow_only_explicit_preservation_not_default_eligibility():
    unknown = RemoteCandidate("markets/run", 2, 123, "a" * 64, 1,
                              (ingress.UNOBSERVABLE_CONSUMERS,))
    options = dict(include_roots=["markets/run"], stable_hours=0, now_ns=100)
    assert not ingress.eligible_candidates([unknown], **options)
    assert ingress.eligible_candidates([unknown], **options, allow_unobservable_copy=True) == [unknown]
    busy = RemoteCandidate("markets/run", 2, 123, "a" * 64, 1, ("pid=123:cwd:/run",))
    assert not ingress.eligible_candidates([busy], **options, allow_unobservable_copy=True)


def test_recent_preserved_run_waits_without_repeated_decode_or_global_wave_block(tmp_path, monkeypatch):
    args = SimpleNamespace(sync_root=tmp_path / "cold", state_root=tmp_path / "state", apply=True,
                           origin_node_id="vastai1T")
    policy = ingress.load_policy(Path("configs/data_sync/training_return.json"))
    candidate = RemoteCandidate("markets/finished", 1, 10, "a" * 64,
                                ingress.time.time_ns(), ())
    resolved = SimpleNamespace(manifest={"dataset": "artifact-auto-finished-94d77c3e4f5a8756",
                                         "snapshot_id": "fixed-release"}, manifest_sha256="b" * 64)
    from stockagent.data_sync.artifact_maintenance import automatic_dataset_name
    resolved.manifest["dataset"] = automatic_dataset_name(candidate.relative_root)
    monkeypatch.setattr("stockagent.data_sync.cold_primary.verify_cold_resilience",
                        lambda *a: {"cold_primary_verified": True})
    monkeypatch.setattr("scripts.configure_artifact_ingress_syncthing.credentials",
                        lambda: ("http://local", "unit-key"))
    monkeypatch.setattr("scripts.manage_packed_edge._convergence", lambda *a: {"ok": True})
    monkeypatch.setattr(ingress, "make_ack", lambda *a, **k: pytest.fail("recent run repeated full cold decode"))
    monkeypatch.setattr(ingress, "remote_retirement", lambda *a, **k: pytest.fail("recent run attempted unlink"))
    result = ingress.returned_source(args, candidate, resolved, policy)
    assert result["state"] == "waiting-training-hot-retention" and result["deleted"] is False
    assert result["minimum_hot_retention_hours"] == 168
    assert ingress.waiting_returns(args.state_root, policy) == []


def test_migration_preference_defers_additional_restore_and_keeps_the_pending_source(tmp_path, monkeypatch):
    args = SimpleNamespace(sync_root=tmp_path/'cold', state_root=tmp_path/'state', apply=True,
                           origin_node_id='vastai1T')
    policy = ingress.load_policy(Path('configs/data_sync/training_return.json'))
    candidate = RemoteCandidate('markets/old-result', 1, 10, 'a'*64,
                                ingress.time.time_ns()-200*3600*10**9, ())
    resolved = SimpleNamespace(manifest={'dataset':ingress.automatic_dataset_name(candidate.relative_root),
                                         'snapshot_id':'fixed-release'}, manifest_sha256='b'*64)
    monkeypatch.setenv('STOCKAGENT_DEFER_OPTIONAL_RETURN_RESTORE','1')
    monkeypatch.setattr('stockagent.data_sync.cold_primary.verify_cold_resilience',
                        lambda *_a:{'cold_primary_verified':True})
    monkeypatch.setattr('scripts.configure_artifact_ingress_syncthing.credentials',
                        lambda:('http://local','unit-key'))
    monkeypatch.setattr('scripts.manage_packed_edge._convergence', lambda *_a:{'ok':True})
    monkeypatch.setattr(ingress,'make_ack',lambda *_a,**_k:pytest.fail('must defer cold restore'))
    monkeypatch.setattr(ingress,'remote_retirement',lambda *_a,**_k:pytest.fail('must retain source'))
    result = ingress.returned_source(args, candidate, resolved, policy)
    assert result == {'deleted':False, 'state':'recovery-deferred-user-migration',
                      'cold_recovery_completed':False}
    assert ingress.waiting_returns(args.state_root, policy) == []
    deferred = ingress._deferred_entries(args.state_root, policy)
    assert len(deferred) == 1 and deferred[0][1]['relative_root'] == candidate.relative_root
    assert deferred[0][1]['manifest_sha256'] == resolved.manifest_sha256


def test_deferred_marker_handoff_resumes_after_interruption_and_restores_original_queue(tmp_path, monkeypatch):
    policy = ingress.load_policy(Path('configs/data_sync/training_return.json'))
    args = SimpleNamespace(state_root=tmp_path, apply=True)
    root = 'markets/fixed-deferred'
    wave = {'relative_root':root, 'dataset':ingress.automatic_dataset_name(root),
            'snapshot_id':'fixed', 'manifest_sha256':'a'*64}
    marker = ingress._enroll_return(args, wave, policy)
    original_fsync = ingress._fsync_directory
    monkeypatch.setattr(ingress, '_fsync_directory', lambda *_: (_ for _ in ()).throw(OSError('interrupted')))
    with pytest.raises(OSError, match='interrupted'):
        ingress._move_waiting_marker(marker, wave, policy, tmp_path/'training-return-deferred')
    assert marker.exists() and ingress._deferred_entries(tmp_path, policy)[0][1] == wave
    monkeypatch.setattr(ingress, '_fsync_directory', original_fsync)
    ingress._move_waiting_marker(marker, wave, policy, tmp_path/'training-return-deferred')
    assert not marker.exists() and ingress.waiting_returns(tmp_path, policy) == []
    with pytest.raises(SnapshotError, match='awaiting deferred recovery'):
        ingress._enroll_return(args, wave, policy)
    ingress._resume_deferred_returns(tmp_path, policy)
    assert ingress.waiting_returns(tmp_path, policy) == [wave]
    assert ingress._deferred_entries(tmp_path, policy) == []


def test_deferred_requeue_respects_active_transport_capacity(tmp_path, monkeypatch):
    policy = ingress.load_policy(Path('configs/data_sync/training_return.json'))
    monkeypatch.setattr(ingress, 'MAX_PENDING_RETURNS', 1)
    args = SimpleNamespace(state_root=tmp_path, apply=True)
    def wave(root):
        return {'relative_root':root, 'dataset':ingress.automatic_dataset_name(root),
                'snapshot_id':'fixed', 'manifest_sha256':'a'*64}
    first, second = wave('markets/first'), wave('markets/second')
    path = ingress._enroll_return(args, first, policy)
    ingress._move_waiting_marker(path, first, policy, tmp_path/'training-return-deferred')
    active = ingress._enroll_return(args, second, policy)
    ingress._resume_deferred_returns(tmp_path, policy)
    assert ingress.waiting_returns(tmp_path, policy) == [second]
    assert ingress._deferred_entries(tmp_path, policy)[0][1] == first
    ingress._clear_return(active, second, policy)
    ingress._resume_deferred_returns(tmp_path, policy)
    assert ingress.waiting_returns(tmp_path, policy) == [first]
    assert not ingress._deferred_entries(tmp_path, policy)


def test_deferred_requeue_recovers_duplicate_legacy_marker_without_duplicate_pending(tmp_path):
    policy = ingress.load_policy(Path('configs/data_sync/training_return.json'))
    root = 'markets/legacy'
    wave = {'relative_root':root, 'dataset':ingress.automatic_dataset_name(root),
            'snapshot_id':'fixed', 'manifest_sha256':'a'*64}
    legacy = tmp_path/'training-return-waiting-peer.json'
    legacy.write_text(json.dumps(wave))
    pending = ingress._move_waiting_marker(legacy, wave, policy, tmp_path/'training-return-deferred')
    legacy.write_text(json.dumps(wave))  # Interrupted handoff left both names.
    ingress._resume_deferred_returns(tmp_path, policy)
    assert legacy.exists() and not pending.exists()
    assert ingress.waiting_returns(tmp_path, policy) == [wave]


def test_conflicting_pending_and_deferred_generations_are_not_reinterpreted(tmp_path, monkeypatch, capsys):
    policy_file = Path('configs/data_sync/training_return.json').absolute()
    policy = ingress.load_policy(policy_file)
    root = 'markets/conflict'
    wave = {'relative_root':root, 'dataset':ingress.automatic_dataset_name(root),
            'snapshot_id':'fixed', 'manifest_sha256':'a'*64}
    path = ingress._enroll_return(SimpleNamespace(state_root=tmp_path, apply=True), wave, policy)
    other = {**wave, 'manifest_sha256':'b'*64}
    directory = tmp_path/'training-return-deferred'; directory.mkdir()
    target = directory/ingress._pending_name(other); target.write_text(json.dumps(other))
    monkeypatch.setenv('STOCKAGENT_DEFER_OPTIONAL_RETURN_RESTORE', '1')
    monkeypatch.setattr('sys.argv', ['ingress','--apply','--ssh-target','root@peer',
        '--identity-file',str(tmp_path/'key'),'--origin-node-id','vastai1T',
        '--state-root',str(tmp_path),'--policy',str(policy_file)])
    monkeypatch.setattr(ingress, 'retry_training_layout', lambda *_: None)
    monkeypatch.setattr(ingress, 'discover_remote_artifacts', lambda **_:pytest.fail('conflict must stop before discovery'))
    assert ingress.main() == 2
    assert path.exists() and target.exists()
    assert 'generations conflict' in capsys.readouterr().err


def test_deferred_wave_is_not_reingested_while_new_candidates_can_progress(tmp_path, monkeypatch, capsys):
    policy_file = Path('configs/data_sync/training_return.json').absolute()
    policy = ingress.load_policy(policy_file)
    deferred = _candidate('markets/already-deferred')
    fresh = _candidate('markets/new-complete')
    wave = {'relative_root':deferred.relative_root, 'dataset':ingress.automatic_dataset_name(deferred.relative_root),
            'snapshot_id':'fixed', 'manifest_sha256':'a'*64}
    marker = ingress._enroll_return(SimpleNamespace(state_root=tmp_path, apply=True), wave, policy)
    ingress._move_waiting_marker(marker, wave, policy, tmp_path/'training-return-deferred')
    monkeypatch.setenv('STOCKAGENT_DEFER_OPTIONAL_RETURN_RESTORE', '1')
    monkeypatch.setattr('sys.argv', ['ingress','--ssh-target','root@peer',
        '--identity-file',str(tmp_path/'key'),'--origin-node-id','vastai1T',
        '--state-root',str(tmp_path),'--policy',str(policy_file)])
    monkeypatch.setattr(ingress, 'discover_remote_artifacts',
                        lambda **kw:[deferred, fresh] if kw['scope']=='markets' else [])
    monkeypatch.setattr(ingress, '_resolved_release', lambda *_a,**_k:None)
    assert ingress.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result['deferred_return_count'] == 1
    assert [(row['relative_root'],row['action']) for row in result['rows']] == [(fresh.relative_root,'would-ingest')]
    assert ingress._deferred_entries(tmp_path, policy)[0][1] == wave


@pytest.mark.parametrize('converged', [True, False])
def test_full_pending_queue_defers_only_converged_fixed_waves_without_source_mutation(
        tmp_path, monkeypatch, capsys, converged):
    policy_file = Path('configs/data_sync/training_return.json').absolute()
    policy = ingress.load_policy(policy_file)
    monkeypatch.setattr(ingress, 'MAX_PENDING_RETURNS', 2)
    monkeypatch.setenv('STOCKAGENT_DEFER_OPTIONAL_RETURN_RESTORE', '1')
    args = SimpleNamespace(state_root=tmp_path, apply=True)
    for suffix in ('first', 'second'):
        root = 'markets/' + suffix
        ingress._enroll_return(args, {'relative_root':root, 'dataset':ingress.automatic_dataset_name(root),
            'snapshot_id':'fixed-'+suffix, 'manifest_sha256':'a'*64}, policy)
    monkeypatch.setattr('sys.argv', ['ingress','--apply','--ssh-target','root@peer',
        '--identity-file',str(tmp_path/'key'),'--origin-node-id','vastai1T',
        '--state-root',str(tmp_path),'--policy',str(policy_file)])
    monkeypatch.setattr(ingress, 'retry_training_layout', lambda *_: None)
    monkeypatch.setattr(ingress, 'discover_remote_artifacts', lambda **_: pytest.fail('full deferred queue needs no SSH scan'))
    monkeypatch.setattr(ingress, '_resolve_waiting_release', lambda _args,wave: (SimpleNamespace(
        manifest_sha256=wave['manifest_sha256'], manifest={'dataset':wave['dataset'],
        'snapshot_id':wave['snapshot_id'], 'source':{'files':1, 'logical_bytes':10,
        'portable_fingerprint_sha256':'b'*64}}), False))
    monkeypatch.setattr('stockagent.data_sync.cold_primary.verify_cold_resilience',
                        lambda *_a:{'cold_primary_verified':True})
    monkeypatch.setattr('scripts.configure_artifact_ingress_syncthing.credentials',
                        lambda:('http://local','unit-key'))
    monkeypatch.setattr('scripts.manage_packed_edge._convergence', lambda *_a:{'ok':converged,'checks':[]})
    monkeypatch.setattr(ingress, 'make_ack', lambda *_a, **_k:pytest.fail('must not restore'))
    monkeypatch.setattr(ingress, 'remote_retirement', lambda *_a, **_k:pytest.fail('must not delete'))
    assert ingress.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result['pending_return_count'] == (0 if converged else 2)
    assert result['deferred_return_count'] == (2 if converged else 0)
    assert len(ingress.waiting_returns(tmp_path, policy)) == (0 if converged else 2)
    assert len(ingress._deferred_entries(tmp_path, policy)) == (2 if converged else 0)
    assert result['source_roots_retired'] == 0
from stockagent.training.lifecycle import TrainingRunLifecycle
from test_training_lifecycle import _write_minimal_completed_artifacts


@pytest.fixture(autouse=True)
def isolated_ingress_owners(tmp_path, monkeypatch):
    monkeypatch.setattr(ingress,'INGRESS_OWNER',tmp_path/'ingress-cycle.lock')
    monkeypatch.setattr(ingress,'PUBLICATION_OWNER',tmp_path/'common-publication.lock')


def shared_owner_held():
    with ingress.PUBLICATION_OWNER.open('a') as probe:
        try:
            fcntl.flock(probe,fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(probe,fcntl.LOCK_UN)
        return False


def _candidate(relative_root: str, *, references: tuple[str, ...] = ()) -> RemoteCandidate:
    return RemoteCandidate(
        relative_root=relative_root,
        files=501,
        logical_bytes=1_250_237_113,
        portable_fingerprint_sha256="a" * 64,
        newest_mtime_ns=1_000,
        process_references=references,
    )


def test_discovery_decoder_ignores_ssh_banner_and_validates_rows() -> None:
    payload = {
        "schema_version": 1,
        "candidates": [
            {
                "relative_root": "ablations/suite/layernorm",
                "files": 2,
                "logical_bytes": 7,
                "portable_fingerprint_sha256": "b" * 64,
                "newest_mtime_ns": 123,
                "process_references": [],
            }
        ],
    }
    rows = _decode_discovery("Welcome to the peer\n" + json.dumps(payload) + "\n")
    assert rows == [
        RemoteCandidate(
            relative_root="ablations/suite/layernorm",
            files=2,
            logical_bytes=7,
            portable_fingerprint_sha256="b" * 64,
            newest_mtime_ns=123,
            process_references=(),
        )
    ]


def test_eligible_candidates_are_exactly_allowlisted_and_unused() -> None:
    wanted = _candidate("ablations/suite/layernorm")
    busy = _candidate("ablations/suite/busy", references=("pid=7:fd:checkpoint",))
    unrelated = _candidate("ablations/other/large")

    rows = eligible_candidates(
        [unrelated, busy, wanted],
        include_roots=[wanted.relative_root, busy.relative_root],
        stable_hours=0,
        now_ns=2_000,
    )

    assert rows == [wanted]


def test_remote_and_staging_paths_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(SnapshotError, match="unsafe SSH target"):
        _validate_ssh_target("peer; touch /tmp/pwned")
    with pytest.raises(SnapshotError, match="unsafe remote repo"):
        _validate_remote_absolute("/root/../etc", "remote repo")

    candidate = _candidate("ablations/suite/layernorm")
    artifact_root = _staging_artifact_root(tmp_path, "vastai1T", _spec(candidate, 0))
    assert artifact_root.is_relative_to(tmp_path.resolve())
    assert "artifact-auto-layernorm-" in str(artifact_root)


def test_return_staging_root_refuses_redirected_intermediate_directory(tmp_path):
    outside = tmp_path/'outside'; outside.mkdir()
    (tmp_path/'ingress-staging').symlink_to(outside, target_is_directory=True)
    with pytest.raises(SnapshotError, match='redirected component'):
        _staging_artifact_root(tmp_path, 'vastai1T', _spec(_candidate('markets/run'), 0))


@pytest.mark.parametrize("profile", ["none", "zstd-1"])
def test_rsync_profile_preserves_original_byte_transfer_contract(tmp_path, monkeypatch, profile):
    calls = []
    monkeypatch.setattr(ingress, "_ssh_base", lambda *a: ["ssh", "-o", "BatchMode=yes"])
    monkeypatch.setattr(ingress.subprocess, "run", lambda command, **kw: calls.append((command, kw)))
    ingress._rsync_candidate(_candidate("ablations/suite/layernorm"),
        ssh_target="root@trusted", ssh_port=22, identity_file=tmp_path / "private-key",
        remote_artifact_root="/root/stockAgent/artifacts", destination_artifact_root=tmp_path,
        compression=profile)
    command, options = calls[0]
    for flag in ("--archive", "--partial", "--append-verify", "--delete-delay", "--protect-args"):
        assert flag in command
    assert command[-2] == "root@trusted:/root/stockAgent/artifacts/ablations/suite/layernorm/"
    assert command[-1] == str(tmp_path / "ablations/suite/layernorm") + "/"
    assert ("--compress-choice=zstd" in command) == (profile == "zstd-1")
    assert ("--compress-level=1" in command) == (profile == "zstd-1")
    assert options == {"check": True, "timeout": 21600}


@pytest.mark.parametrize("profile", ["unmeasured", False, []])
def test_invalid_rsync_profile_cannot_create_a_transfer_or_run_a_command(tmp_path, monkeypatch, profile):
    def forbidden(*a, **kw):
        raise AssertionError("must not execute an invalid transfer profile")
    monkeypatch.setattr(ingress.subprocess, "run", forbidden)
    with pytest.raises(SnapshotError, match="compression profile"):
        ingress._rsync_candidate(_candidate("ablations/suite/layernorm"),
            ssh_target="root@trusted", ssh_port=22, identity_file=tmp_path / "private-key",
            remote_artifact_root="/root/stockAgent/artifacts", destination_artifact_root=tmp_path,
            compression=profile)
    assert not (tmp_path / "ablations").exists()


def test_existing_release_must_match_remote_immutable_identity(
    tmp_path: Path, monkeypatch
) -> None:
    candidate = _candidate("ablations/suite/layernorm")
    spec = _spec(candidate, 0)
    (tmp_path / "heads" / spec.dataset).mkdir(parents=True)
    resolved = SimpleNamespace(
        manifest={
            "metadata": {"artifact_relative_root": spec.relative_root},
            "source": {
                "files": candidate.files,
                "logical_bytes": candidate.logical_bytes,
                "portable_fingerprint_sha256": "c" * 64,
            },
        }
    )
    monkeypatch.setattr(
        "scripts.ingest_remote_cold_artifacts.resolve_latest_packed",
        lambda *_args: resolved,
    )
    monkeypatch.setattr(
        "scripts.ingest_remote_cold_artifacts.verify_packed_snapshot",
        lambda *_args: {},
    )

    with pytest.raises(SnapshotError, match="changed after its immutable release"):
        _resolved_release(tmp_path, spec, candidate)


def test_exact_recheck_validates_the_selected_run_without_a_whole_scope_scan(tmp_path, monkeypatch, capsys):
    artifact = tmp_path / "artifacts"
    source = artifact / "markets/selected"
    lifecycle = TrainingRunLifecycle(source, execution_mode="naive", run_mode="train",
                                     strategy="none", model_name="transformer_base_portfolio")
    lifecycle.start(fold_ids=[1], dataset_fingerprint="exact-source", configuration_fingerprint="exact-config")
    lifecycle.start_group(group_name="train_2020-2021", group_index=1, group_total=1, fold_ids=[1], epoch_total=1)
    lifecycle.finish_fold(1)
    _write_minimal_completed_artifacts(lifecycle.layout, group_name="train_2020-2021", fold_id=1, execution_mode="naive")
    lifecycle.complete(fold_ids=[1])
    monkeypatch.setattr("sys.argv", ["discovery", str(artifact), "markets", "markets/selected"])
    monkeypatch.setattr("stockagent.data_sync.artifact_maintenance.discover_completed_runs",
                        lambda *_: pytest.fail("exact post-transfer verification scanned unrelated runs"))
    monkeypatch.setattr("stockagent.data_sync.artifact_maintenance.artifact_process_references", lambda *_: [])
    exec(ingress._REMOTE_DISCOVERY_PROGRAM, {})
    result = json.loads(capsys.readouterr().out)
    assert [r["relative_root"] for r in result["candidates"]] == ["markets/selected"]
    assert result["candidates"][0]["files"] > 0


def test_exact_remote_recheck_rejects_a_different_scope(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.argv", ["discovery", str(tmp_path), "markets", "ablations/run"])
    with pytest.raises(SnapshotError, match="outside its real scope"):
        exec(ingress._REMOTE_DISCOVERY_PROGRAM, {})


def test_ssh_discovery_preserves_ancestor_consumer_guard(tmp_path, monkeypatch, capsys):
    import stockagent.data_sync.artifact_maintenance as maintenance

    artifact = tmp_path / "artifacts"
    source = artifact / "markets/suite/selected"
    lifecycle = TrainingRunLifecycle(source, execution_mode="naive", run_mode="train",
                                     strategy="none", model_name="transformer_base_portfolio")
    lifecycle.start(fold_ids=[1], dataset_fingerprint="source", configuration_fingerprint="config")
    lifecycle.start_group(group_name="train_2020-2021", group_index=1, group_total=1, fold_ids=[1], epoch_total=1)
    lifecycle.finish_fold(1)
    _write_minimal_completed_artifacts(lifecycle.layout, group_name="train_2020-2021", fold_id=1, execution_mode="naive")
    lifecycle.complete(fold_ids=[1])

    process = tmp_path / "proc/99999999"
    process.mkdir(parents=True)
    (process / "cmdline").write_bytes(b"python\0--output-root\0" + bytes(source.parent) + b"\0")
    (process / "cwd").symlink_to(tmp_path, target_is_directory=True)
    monkeypatch.setattr(maintenance, "_process_directories", lambda: [process])
    monkeypatch.setattr(maintenance, "process_references", lambda *_: [])
    # Restore the module function after the transmitted override is evaluated.
    monkeypatch.setattr(maintenance, "_ancestor_process_references", maintenance._ancestor_process_references)
    monkeypatch.setattr(ingress, "_ssh_base", lambda *_: ["ssh"])

    def run_remote(command, **kwargs):
        shell = shlex.split(command[-1])[2]
        python_command = shlex.split(shell.split('exec "$python_bin" ', 1)[1])
        assert python_command[0] == "-c"
        monkeypatch.setattr("sys.argv", ["remote-discovery", *python_command[2:]])
        exec(python_command[1], {})
        return SimpleNamespace(stdout=capsys.readouterr().out)

    monkeypatch.setattr(ingress.subprocess, "run", run_remote)
    candidates = ingress.discover_remote_artifacts(
        ssh_target="root@trusted", ssh_port=22, identity_file=tmp_path / "key",
        remote_repo_root="/root/stockAgent", remote_artifact_root=str(artifact), scope="markets",
    )
    assert len(candidates) == 1
    assert candidates[0].relative_root == "markets/suite/selected"
    assert candidates[0].process_references == (f"pid=99999999:cmdline:{source.parent}",)


def test_readonly_discovery_retries_transient_ssh_failure_without_publishing_diagnostics(monkeypatch):
    attempts, sleeps = [], []

    def run(command, **kwargs):
        attempts.append((command, kwargs))
        if len(attempts) < 3:
            raise subprocess.CalledProcessError(255, command, stderr='private transport diagnostic')
        return SimpleNamespace(stdout='{"schema_version": 1, "candidates": []}')

    monkeypatch.setattr(ingress.subprocess, 'run', run)
    monkeypatch.setattr(ingress.time, 'sleep', sleeps.append)
    monkeypatch.setattr(ingress, '_ssh_base', lambda *_: ['ssh'])
    candidates = ingress.discover_remote_artifacts(
        ssh_target='root@trusted', ssh_port=22, identity_file=Path('/unused'),
        remote_repo_root='/root/stockAgent', remote_artifact_root='/root/stockAgent/artifacts',
        scope='markets')
    assert candidates == [] and len(attempts) == 3 and sleeps == [1, 2]
    assert all(options == {'check': True, 'capture_output': True, 'text': True, 'timeout': 300}
               for _, options in attempts)


@pytest.mark.parametrize('exit_code, expected_attempts', [(1, 1), (255, 3)])
def test_readonly_discovery_preserves_last_failure_and_never_retries_other_exit_codes(
        monkeypatch, exit_code, expected_attempts):
    attempts, sleeps = [], []

    def run(command, **kwargs):
        attempts.append(command)
        raise subprocess.CalledProcessError(exit_code, command, stderr=f'private-{len(attempts)}')

    monkeypatch.setattr(ingress.subprocess, 'run', run)
    monkeypatch.setattr(ingress.time, 'sleep', sleeps.append)
    with pytest.raises(subprocess.CalledProcessError) as failure:
        ingress._run_readonly_ssh_discovery(['ssh', 'peer', 'readonly'])
    assert len(attempts) == expected_attempts
    assert failure.value.stderr == f'private-{expected_attempts}'
    assert len(sleeps) == expected_attempts - 1


def test_remote_control_failure_preserves_private_diagnostics_without_publishing_contents(tmp_path, monkeypatch):
    args = SimpleNamespace(state_root=tmp_path, ssh_target="root@peer", ssh_port=22,
                           remote_repo_root="/root/stockAgent", remote_artifact_root="/root/stockAgent/artifacts",
                           identity_file=tmp_path / "key")
    args.identity_file.write_text("unit-key")
    args.identity_file.chmod(0o600)
    monkeypatch.setattr(ingress.subprocess, "run", lambda *a, **k:
                        SimpleNamespace(returncode=1, stderr="private-unit-secret", stdout=""))
    result = ingress.remote_retirement(args, {"identity_sha256": "a"*64}, {}, apply=False)
    assert result["deleted"] is False and "private-unit-secret" not in json.dumps(result)
    evidence = Path(result["diagnostics"])
    assert evidence.read_text() == "private-unit-secret" and evidence.stat().st_mode & 0o777 == 0o600


def test_remote_cache_pressure_uses_fixed_policy_and_never_exposes_diagnostics(tmp_path, monkeypatch):
    args = SimpleNamespace(ssh_target='root@peer', ssh_port=22, identity_file=tmp_path/'key')
    monkeypatch.setattr(ingress, '_ssh_base', lambda *_:['ssh'])
    calls = []
    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout='banner\n'+json.dumps({
            'contract':'stockagent.remote_cache_pressure_reclaim.v1', 'state':'checked',
            'removed_allocated_bytes':123, 'available_before_bytes':456, 'threshold_bytes':500}), stderr='')
    monkeypatch.setattr(ingress.subprocess, 'run', run)
    value = ingress.remote_cache_pressure(args)
    assert value['state'] == 'checked' and value['removed_allocated_bytes'] == 123
    assert calls[0][0] == ['ssh', 'root@peer', 'python3 -']
    assert "scope['reclaim_under_pressure']()" in calls[0][1]['input']
    monkeypatch.setattr(ingress.subprocess, 'run', lambda *_a, **_k:SimpleNamespace(
        returncode=0, stdout=json.dumps({'contract':'stockagent.remote_cache_pressure_reclaim.v1',
            'state':'partial', 'removed_allocated_bytes':0, 'profile_issues':1}), stderr=''))
    partial = ingress.remote_cache_pressure(args)
    assert partial['state'] == 'partial' and partial['profile_issues'] == 1
    monkeypatch.setattr(ingress.subprocess, 'run', lambda *_a, **_k:SimpleNamespace(
        returncode=1, stdout='private token', stderr='private traceback'))
    failure = ingress.remote_cache_pressure(args)
    assert failure['state'] == 'waiting' and failure['error_type'] == 'remote_exit_nonzero'
    assert 'private' not in json.dumps(failure)


def test_one_pending_return_does_not_block_new_preservation(tmp_path, monkeypatch, capsys):
    pending = _candidate("markets/waiting")
    other = _candidate("markets/older")
    wave = {"relative_root": pending.relative_root, "dataset": ingress.automatic_dataset_name(pending.relative_root),
            "snapshot_id": "release-waiting", "manifest_sha256": "a"*64}
    marker = tmp_path / "training-return-waiting-peer.json"
    marker.write_text(json.dumps(wave))
    policy = Path("configs/data_sync/training_return.json").absolute()
    monkeypatch.setattr("sys.argv", ["ingress", "--ssh-target", "root@peer", "--identity-file", str(tmp_path/"key"),
                        "--origin-node-id", "vastai1T", "--state-root", str(tmp_path), "--policy", str(policy)])
    discovery = []
    def exact_discovery(**kw):
        discovery.append(kw)
        assert "relative_root" not in kw
        return [pending, other] if kw["scope"] == "markets" else []
    monkeypatch.setattr(ingress, "discover_remote_artifacts", exact_discovery)
    monkeypatch.setattr(ingress, "_resolved_release", lambda *a, **kw: None)
    monkeypatch.setattr(ingress, "resolve_packed_snapshot_id", lambda *a: SimpleNamespace(
        manifest_sha256=wave["manifest_sha256"], manifest={"source": {
            "files":pending.files, "logical_bytes":pending.logical_bytes,
            "portable_fingerprint_sha256":pending.portable_fingerprint_sha256}}))
    monkeypatch.setattr(ingress, "returned_source", lambda *a, **kw: {"state":"waiting-packed-peer-convergence", "deleted":False})
    monkeypatch.setattr(ingress, "publish_cold_artifact", lambda *a, **kw: pytest.fail("unconfirmed wave was bypassed"))
    assert ingress.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["published"] == 0 and result["waiting_return"] == wave
    assert any(row["relative_root"] == other.relative_root and row["action"] == "would-ingest" for row in result["rows"])
    assert len(discovery) == 2 and result["pending_return_count"] == 1
    assert json.loads(marker.read_text()) == wave


def test_bounded_pending_queue_preserves_other_waves_and_legacy(tmp_path):
    policy = ingress.load_policy(Path("configs/data_sync/training_return.json"))
    args = SimpleNamespace(state_root=tmp_path, apply=True)
    def wave(i):
        root = f"markets/run-{i}"
        return {"relative_root":root, "dataset":ingress.automatic_dataset_name(root),
                "snapshot_id":f"fixed-{i}", "manifest_sha256":"a"*64}
    legacy = tmp_path/"training-return-waiting-peer.json"
    legacy.write_text(json.dumps(wave(0)))
    enrolled = ingress._enroll_return(args, wave(1), policy)
    assert ingress._enroll_return(args, wave(1), policy) == enrolled
    ingress._clear_return(enrolled, wave(1), policy)
    assert ingress.waiting_returns(tmp_path, policy) == [wave(0)] and legacy.exists()
    for i in range(1, ingress.MAX_PENDING_RETURNS):
        ingress._enroll_return(args, wave(i), policy)
    with pytest.raises(SnapshotError, match="queue is full"):
        ingress._enroll_return(args, wave(999), policy)
    assert len(ingress.waiting_returns(tmp_path, policy)) == ingress.MAX_PENDING_RETURNS
    with pytest.raises(SnapshotError, match="different exact release"):
        ingress._enroll_return(args, {**wave(0), "manifest_sha256":"b"*64}, policy)


def test_pending_queue_filename_binds_immutable_release(tmp_path):
    policy = ingress.load_policy(Path("configs/data_sync/training_return.json"))
    root = "markets/fixed"
    wave = {"relative_root":root, "dataset":ingress.automatic_dataset_name(root),
            "snapshot_id":"fixed-release", "manifest_sha256":"a"*64}
    path = ingress._enroll_return(SimpleNamespace(state_root=tmp_path, apply=True), wave, policy)
    path.write_text(json.dumps({**wave, "manifest_sha256":"b"*64}))
    with pytest.raises(SnapshotError, match="identity conflicts"):
        ingress.waiting_returns(tmp_path, policy)


def test_repacked_pending_wave_preserves_original_marker_until_normal_confirmation(tmp_path, monkeypatch):
    policy = ingress.load_policy(Path('configs/data_sync/training_return.json'))
    args = SimpleNamespace(sync_root=tmp_path/'cold', state_root=tmp_path/'state', apply=True,
                           origin_node_id='vastai1T')
    candidate = RemoteCandidate('markets/repacked', 1, 10, 'a'*64, ingress.time.time_ns(), ())
    dataset = ingress.automatic_dataset_name(candidate.relative_root)
    wave = {'relative_root':candidate.relative_root, 'dataset':dataset,
            'snapshot_id':'retired-original', 'manifest_sha256':'b'*64}
    marker = ingress._enroll_return(args, wave, policy)
    replacement = SimpleNamespace(manifest_sha256='c'*64,
                                   manifest={'dataset':dataset,'snapshot_id':'fixed-replacement'})
    monkeypatch.setattr(ingress, '_resolve_waiting_release', lambda *_args:(replacement, True))
    monkeypatch.setattr('stockagent.data_sync.cold_primary.verify_cold_resilience',
                        lambda *_args:{'cold_primary_verified':True})
    monkeypatch.setattr('scripts.configure_artifact_ingress_syncthing.credentials',
                        lambda:('http://local','unit-key'))
    monkeypatch.setattr('scripts.manage_packed_edge._convergence', lambda *_args:{'ok':True})
    monkeypatch.setattr(ingress, 'make_ack', lambda *_a, **_k:pytest.fail('recent run must not decode'))
    monkeypatch.setattr(ingress, 'remote_retirement', lambda *_a, **_k:pytest.fail('recent run must not delete'))
    result = ingress.returned_source(args, candidate, replacement, policy, pending_original=wave)
    assert result['state'] == 'waiting-training-hot-retention' and result['deleted'] is False
    assert not marker.exists() and ingress.waiting_returns(args.state_root, policy) == []


def test_full_pending_queue_selects_before_discovery_and_rotates_unavailable_wave(tmp_path, monkeypatch, capsys):
    policy_file = Path('configs/data_sync/training_return.json').absolute()
    policy = ingress.load_policy(policy_file)
    monkeypatch.setattr(ingress, 'MAX_PENDING_RETURNS', 2)
    args = SimpleNamespace(state_root=tmp_path, apply=True)
    waves = []
    for root in ('markets/full-0', 'markets/full-1'):
        wave = {'relative_root':root, 'dataset':ingress.automatic_dataset_name(root),
                'snapshot_id':'fixed-release', 'manifest_sha256':'a'*64}
        ingress._enroll_return(args, wave, policy); waves.append(wave)
    (tmp_path/'training-return-attempts.json').write_text(json.dumps({waves[0]['relative_root']:100,
                                                                     waves[1]['relative_root']:1}))
    monkeypatch.setattr('sys.argv', ['ingress','--apply','--ssh-target','root@peer',
        '--identity-file',str(tmp_path/'key'),'--origin-node-id','vastai1T',
        '--state-root',str(tmp_path),'--policy',str(policy_file)])
    selected = []
    def discovery(**kw):
        selected.append(kw['relative_root'])
        return []
    monkeypatch.setattr(ingress, 'discover_remote_artifacts', discovery)
    monkeypatch.setattr(ingress, '_resolve_waiting_release',
                        lambda *_a: (_ for _ in ()).throw(SnapshotError('private details')))
    assert ingress.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert selected == [waves[1]['relative_root']]
    assert result['rows'][0]['action'] == 'waiting-fixed-release-unavailable'
    assert 'private details' not in json.dumps(result) and len(ingress.waiting_returns(tmp_path, policy)) == 2
    assert ingress.main() == 0
    capsys.readouterr()
    assert selected[-1] == waves[0]['relative_root']


def test_waiting_release_cannot_redirect_the_owner_to_an_unenrolled_scope(tmp_path):
    wave = {"relative_root":"sources/private", "dataset":"unexpected", "snapshot_id":"release", "manifest_sha256":"a"*64}
    (tmp_path / "training-return-waiting-peer.json").write_text(json.dumps(wave))
    policy = ingress.load_policy(Path("configs/data_sync/training_return.json"))
    with pytest.raises(SnapshotError, match="waiting marker"):
        ingress.waiting_return(tmp_path, policy)


def test_an_absent_pending_source_is_confirmed_without_any_source_deletion(tmp_path, monkeypatch, capsys):
    wave = {"relative_root":"markets/absent", "dataset":ingress.automatic_dataset_name("markets/absent"),
            "snapshot_id":"release-held", "manifest_sha256":"a"*64}
    (tmp_path/"training-return-waiting-peer.json").write_text(json.dumps(wave))
    other = _candidate("markets/other")
    monkeypatch.setattr("sys.argv", ["ingress", "--ssh-target","root@peer", "--identity-file",str(tmp_path/"key"),
        "--origin-node-id","vastai1T", "--state-root",str(tmp_path), "--policy",str(Path("configs/data_sync/training_return.json").absolute())])
    monkeypatch.setattr(ingress,"discover_remote_artifacts",lambda **kw: [other] if kw["scope"]=="markets" else [])
    held = SimpleNamespace(manifest_sha256=wave["manifest_sha256"],
        manifest={"source":{"files":2,"logical_bytes":7,"portable_fingerprint_sha256":"b"*64}})
    monkeypatch.setattr(ingress,"resolve_packed_snapshot_id",lambda *a:held)
    calls=[]
    def confirm(*a,**kw):
        calls.append(kw)
        return {"state":"source-not-currently-eligible","deleted":False,"cold_return_verified":True}
    monkeypatch.setattr(ingress,"returned_source",confirm)
    monkeypatch.setattr(ingress,"publish_cold_artifact",lambda *a,**kw:pytest.fail("unconfirmed missing wave was bypassed"))
    assert ingress.main()==0
    assert calls==[{"retire_source":False}]
    result=json.loads(capsys.readouterr().out)
    assert result["rows"][0]["source_retirement"]["deleted"] is False


def test_deferred_release_content_is_fully_verified_before_any_remote_retirement(tmp_path, monkeypatch):
    from stockagent.data_sync.packed_snapshots import publish_packed_snapshot
    from scripts import configure_artifact_ingress_syncthing as syncthing
    source = tmp_path / "source"
    source.mkdir()
    (source / "result.bin").write_bytes(b"exact immutable result")
    relative = "markets/deferred-proof"
    dataset = ingress.automatic_dataset_name(relative)
    packed = tmp_path / "packed"
    resolved = publish_packed_snapshot(packed, dataset, source,
                                       metadata={"artifact_relative_root": relative})
    identity = resolved.manifest["source"]
    candidate = RemoteCandidate(relative, identity["files"], identity["logical_bytes"],
                                identity["portable_fingerprint_sha256"], 1, ())
    spec = ingress._spec(candidate, 0)
    payload = packed / resolved.manifest["archive"]["objects"][0]["relpath"]
    original = payload.read_bytes()
    payload.write_bytes(bytes(byte ^ 1 for byte in original))
    with pytest.raises(SnapshotError):
        ingress._resolved_release(packed, spec, candidate)
    held = ingress._resolved_release(packed, spec, candidate, verify_content=False)
    assert held.manifest_sha256 == resolved.manifest_sha256
    policy = ingress.load_policy(Path("configs/data_sync/training_return.json"))
    args = SimpleNamespace(sync_root=packed, state_root=tmp_path / "state", apply=True,
                           origin_node_id="vastai1T")
    monkeypatch.setattr("stockagent.data_sync.cold_primary.verify_cold_resilience",
                        lambda *a: {"cold_primary_verified": True})
    monkeypatch.setattr(syncthing, "credentials", lambda: ("http://local", "test-key"))
    responses = {
        "/rest/config/devices": [{"name": "vastai1T", "deviceID": "test-device"}],
        "/rest/db/status": {"state": "idle", "needBytes": 0, "needTotalItems": 0,
                            "needDeletes": 0, "errors": 0, "pullErrors": 0},
        "/rest/db/completion": {"completion": 100, "remoteState": "valid", "needBytes": 0,
                                "needItems": 0, "needDeletes": 0},
        "/rest/system/connections": {"connections": {"test-device": {"connected": True}}},
        "/rest/folder/errors": {"errors": []},
        "/rest/system/error": {"errors": None},
    }
    monkeypatch.setattr("scripts.manage_packed_edge._request_json", lambda base, key, route, *a: responses[route])
    acknowledgements = []
    real_ack=ingress.make_ack
    def independent_ack(*args,**kwargs):
        assert not shared_owner_held()
        return real_ack(*args,**kwargs)
    monkeypatch.setattr(ingress,'make_ack',independent_ack)
    def record_retirement(args, ack, policy, *, apply):
        assert shared_owner_held()
        acknowledgements.append(ack)
        return {"state": "unit-retirement-not-applied", "deleted": False}
    monkeypatch.setattr(ingress, "remote_retirement", record_retirement)
    with pytest.raises(SnapshotError):
        ingress.returned_source(args, candidate, held, policy)
    assert acknowledgements == [] and not (args.state_root / "training-return").exists()
    assert (source / "result.bin").read_bytes() == b"exact immutable result"
    payload.write_bytes(original)
    ingress.returned_source(args, candidate, held, policy)
    assert len(acknowledgements) == 1
    assert acknowledgements[0]["cold_reconstruction_verified"] is True
    assert acknowledgements[0]["independently_reconstructed_files"] == 1


def test_busy_ingress_cycle_cannot_overwrite_active_owner_status(tmp_path,monkeypatch):
    status=tmp_path/'status.json';status.write_text('{"state":"original-owner-running"}')
    monkeypatch.setattr('sys.argv',['ingress','--apply','--output',str(status)])
    monkeypatch.setenv('COLD_ARTIFACT_INGRESS_LOCK_WAIT_SECONDS','1')
    monkeypatch.setattr(ingress,'_run_cycle',lambda *a:pytest.fail('second writer entered the ingress cycle'))
    with ingress.INGRESS_OWNER.open('a') as existing:
        fcntl.flock(existing,fcntl.LOCK_EX)
        assert ingress.main()==75
    assert json.loads(status.read_text())=={'state':'original-owner-running'}


def test_ingress_waiter_joins_blocking_kernel_queue_and_releases_owner(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    owner_path=tmp_path/'owner.lock';entered=threading.Event();release=threading.Event()
    monkeypatch.setenv('COLD_ARTIFACT_INGRESS_LOCK_WAIT_SECONDS','3')
    def waiter():
        with ingress.ingress_owner(owner_path):
            entered.set()
            assert release.wait(timeout=3)
    with owner_path.open('a') as held, ThreadPoolExecutor(max_workers=1) as pool:
        fcntl.flock(held,fcntl.LOCK_EX)
        waiting=pool.submit(waiter)
        assert not entered.wait(timeout=0.1)
        fcntl.flock(held,fcntl.LOCK_UN)
        assert entered.wait(timeout=2)
        with owner_path.open('a') as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other,fcntl.LOCK_EX|fcntl.LOCK_NB)
        release.set();waiting.result(timeout=3)
    with owner_path.open('a') as other:
        fcntl.flock(other,fcntl.LOCK_EX|fcntl.LOCK_NB)


def test_independent_ingress_io_does_not_hold_the_shared_publication_owner(tmp_path,monkeypatch):
    candidate=_candidate('markets/closed-cycle')
    monkeypatch.setattr('sys.argv',['ingress','--apply','--ssh-target','root@peer',
        '--identity-file',str(tmp_path/'key'),'--state-root',str(tmp_path/'state'),
        '--sync-root',str(tmp_path/'cold'),'--scope','markets','--stable-hours','0',
        '--include-root',candidate.relative_root])
    events=[]
    def discovered(**kwargs):
        assert not shared_owner_held()
        return [candidate]
    monkeypatch.setattr(ingress,'discover_remote_artifacts',discovered)
    monkeypatch.setattr(ingress,'_resolved_release',lambda *a,**k:None)
    def transferred(candidate,**kwargs):
        events.append(('transfer',shared_owner_held()))
        source=kwargs['destination_artifact_root']/candidate.relative_root
        source.mkdir(parents=True);(source/'result').write_bytes(b'private test source')
    monkeypatch.setattr(ingress,'_rsync_candidate',transferred)
    def validated(root,spec):
        events.append(('source-audit',shared_owner_held()))
        return {'files':candidate.files,'logical_bytes':candidate.logical_bytes,
                'newest_mtime_ns':candidate.newest_mtime_ns,'source':str(root/spec.relative_root)}
    monkeypatch.setattr(ingress,'validate_cold_artifact_source',validated)
    def published(sync_root,root,spec,**kwargs):
        events.append(('publish',shared_owner_held()))
        return SimpleNamespace(manifest={'snapshot_id':'fixed-cycle','dataset':spec.dataset},manifest_sha256='a'*64)
    monkeypatch.setattr(ingress,'publish_cold_artifact',published)
    def verified(*args,**kwargs):
        events.append(('cold-verify',shared_owner_held()))
        return {'unit_workflow_verified':True}
    monkeypatch.setattr(ingress,'verify_packed_snapshot',verified)
    def returned(*args,**kwargs):
        events.append(('return-verification',shared_owner_held()))
        return {}
    monkeypatch.setattr(ingress,'returned_source',returned)
    assert ingress.main()==0
    assert events==[('transfer',False),('source-audit',False),('publish',True),
                    ('cold-verify',False),('return-verification',False)]


@pytest.mark.parametrize('mount_lost', [False, True])
@pytest.mark.parametrize('defer_restore', [False, True])
def test_completed_return_uses_guarded_e_workspace_without_drvfs_clock_relaxation(
        tmp_path, monkeypatch, capsys, mount_lost, defer_restore):
    stage = tmp_path/'e'; stage.mkdir()
    candidate = _candidate('markets/closed-e-cycle')
    policy = Path('configs/data_sync/training_return.json').absolute()
    monkeypatch.setattr(ingress, 'E_RETURN_PAYLOAD_SCRATCH', stage)
    if defer_restore:
        monkeypatch.setenv('STOCKAGENT_DEFER_OPTIONAL_RETURN_RESTORE', '1')
    else:
        monkeypatch.delenv('STOCKAGENT_DEFER_OPTIONAL_RETURN_RESTORE', raising=False)
    monkeypatch.setattr('sys.argv',['ingress','--apply','--ssh-target','root@peer',
        '--identity-file',str(tmp_path/'key'),'--origin-node-id','vastai1T',
        '--state-root',str(tmp_path/'state'),'--sync-root',str(tmp_path/'cold'),
        '--staging-root',str(stage),'--policy',str(policy)])
    monkeypatch.setattr(ingress, 'retry_training_layout', lambda *_: None)
    monkeypatch.setattr(ingress, 'return_staging_workspace', lambda *_:stage)
    monkeypatch.setattr(ingress, '_resolved_release', lambda *a, **kw:None)
    monkeypatch.setattr(ingress, 'discover_remote_artifacts',
                        lambda **kw:[candidate] if kw['scope']=='markets' else [])
    events = []
    def fence(root, identity, *, physical):
        events.append(('fence', physical))
        if physical and mount_lost:
            raise SnapshotError('E mount lost')
    monkeypatch.setattr(ingress, '_fence_e_stage', fence)
    def transfer(candidate, **kwargs):
        root = kwargs['destination_artifact_root']/candidate.relative_root
        root.mkdir(parents=True); (root/'result').write_bytes(b'test')
        events.append(('transfer', str(root)))
    monkeypatch.setattr(ingress, '_rsync_candidate', transfer)
    monkeypatch.setattr(ingress, 'validate_cold_artifact_source', lambda root,spec:{
        'files':candidate.files, 'logical_bytes':candidate.logical_bytes,
        'newest_mtime_ns':candidate.newest_mtime_ns, 'source':str(root/spec.relative_root)})
    def publish(_cold, root, spec, **kwargs):
        events.append(('publish', kwargs['metadata']['ingress_staging_clock_contract']))
        assert root.is_relative_to(stage)
        return SimpleNamespace(manifest={'snapshot_id':'fixed','dataset':spec.dataset},manifest_sha256='a'*64)
    monkeypatch.setattr(ingress, 'publish_cold_artifact', publish)
    def verify(*_args, **_kwargs):
        if defer_restore:
            pytest.fail('user deferred independent cold readback')
        events.append(('independent-readback', True))
        return {'verified':True}
    monkeypatch.setattr(ingress, 'verify_packed_snapshot', verify)
    monkeypatch.setattr(ingress, 'returned_source', lambda *_a,**_k:{'deleted':False})
    result = ingress.main()
    if mount_lost:
        assert result == 2 and not any(event[0]=='publish' for event in events)
        assert list(stage.rglob('result'))  # Preserve the in-flight source copy.
    else:
        assert result == 0 and ('publish', 'native_nanoseconds') in events
        assert [event[1] for event in events if event[0]=='fence'] == [False, True, False, False]
        assert not list(stage.rglob('result'))
        receipt = json.loads(next((tmp_path/'state/ingress-receipts').glob('*.json')).read_text())
        if defer_restore:
            assert receipt['verification']['state'] == 'source_hashed_publication_readback_deferred'
            assert receipt['verification']['independent_cold_readback'] is False
        else:
            assert receipt['verification'] == {'verified':True}
    capsys.readouterr()


@pytest.mark.parametrize("problem", ["system-error", "folder-error", "scanning", "unknown-needed-count", "disconnected"])
def test_automatic_source_cleanup_requires_full_current_transport_proof(tmp_path, monkeypatch, problem):
    from scripts import configure_artifact_ingress_syncthing as syncthing
    candidate = _candidate("markets/automatic-run")
    resolved = SimpleNamespace(manifest_sha256="a"*64,
        manifest={"dataset": ingress.automatic_dataset_name(candidate.relative_root), "snapshot_id": "fixed-release"})
    args = SimpleNamespace(sync_root=tmp_path / "cold", state_root=tmp_path / "state", apply=True,
                           origin_node_id="vastai1T")
    policy = ingress.load_policy(Path("configs/data_sync/training_return.json"))
    monkeypatch.setattr("stockagent.data_sync.cold_primary.verify_cold_resilience",
                        lambda *a: {"cold_primary_verified": True})
    monkeypatch.setattr(syncthing, "credentials", lambda: ("http://local", "private-unit-key"))
    responses = {
        "/rest/config/devices": [{"name": "vastai1T", "deviceID": "current-device"}],
        "/rest/db/status": {"state": "idle", "needBytes": 0, "needTotalItems": 0,
                            "needDeletes": 0, "errors": 0, "pullErrors": 0},
        "/rest/db/completion": {"completion": 100, "remoteState": "valid", "needBytes": 0,
                                "needItems": 0, "needDeletes": 0},
        "/rest/system/connections": {"connections": {"current-device": {"connected": True}}},
        "/rest/folder/errors": {"errors": []}, "/rest/system/error": {"errors": None},
    }
    if problem == "system-error":
        responses["/rest/system/error"] = {"errors": [{"message": "disk error"}]}
    elif problem == "folder-error":
        responses["/rest/folder/errors"] = {"errors": [{"error": "cannot read file"}]}
    elif problem == "scanning":
        responses["/rest/db/status"]["state"] = "scanning"
    elif problem == "unknown-needed-count":
        responses["/rest/db/status"].pop("needTotalItems")
    else:
        responses["/rest/system/connections"]["connections"]["current-device"]["connected"] = False
    monkeypatch.setattr("scripts.manage_packed_edge._request_json", lambda base, key, route, *a: responses[route])
    monkeypatch.setattr(ingress, "make_ack", lambda *a, **k: pytest.fail("unhealthy/unknown transport granted deletion ACK"))
    monkeypatch.setattr(ingress, "remote_retirement", lambda *a, **k: pytest.fail("unhealthy transport deleted source"))
    result = ingress.returned_source(args, candidate, resolved, policy)
    assert result["deleted"] is False and result["state"] == "waiting-packed-peer-convergence"
    assert not all(result["transport_checks"].values())
    assert len(ingress.waiting_returns(args.state_root, policy)) == 1
    assert "private-unit-key" not in json.dumps(result)


def test_transport_is_rechecked_after_slow_cold_recovery_before_source_cleanup(tmp_path, monkeypatch):
    from scripts import configure_artifact_ingress_syncthing as syncthing
    candidate = _candidate("markets/slow-recovery")
    resolved = SimpleNamespace(manifest_sha256="a"*64,
        manifest={"dataset": ingress.automatic_dataset_name(candidate.relative_root), "snapshot_id": "fixed-release"})
    args = SimpleNamespace(sync_root=tmp_path / "cold", state_root=tmp_path / "state", apply=True,
                           origin_node_id="vastai1T")
    policy = ingress.load_policy(Path("configs/data_sync/training_return.json"))
    monkeypatch.setattr("stockagent.data_sync.cold_primary.verify_cold_resilience", lambda *a: {"cold_primary_verified": True})
    monkeypatch.setattr(syncthing, "credentials", lambda: ("http://local", "private-unit-key"))
    observations = iter([{"ok": True, "checks": {"folder_idle": True}},
                         {"ok": False, "checks": {"folder_idle": False}}])
    monkeypatch.setattr("scripts.manage_packed_edge._convergence", lambda *a: next(observations))
    recovered = []
    monkeypatch.setattr(ingress, "make_ack", lambda *a, **k: recovered.append(True) or {"identity_sha256": "a"*64})
    monkeypatch.setattr(ingress, "remote_retirement", lambda *a, **k: pytest.fail("stale pre-decode convergence authorized unlink"))
    result = ingress.returned_source(args, candidate, resolved, policy)
    assert recovered == [True] and result["cold_recovery_completed"] is True
    assert result["deleted"] is False and len(ingress.waiting_returns(args.state_root, policy)) == 1


@pytest.mark.parametrize('changed', ['release', 'cold-authority', 'transport'])
def test_shared_retirement_owner_refreshes_release_authority_and_transport(tmp_path, monkeypatch, changed):
    from scripts import configure_artifact_ingress_syncthing as syncthing
    candidate = _candidate('markets/fresh-owner-gates')
    resolved = SimpleNamespace(manifest_sha256='a'*64,
        manifest={'dataset': ingress.automatic_dataset_name(candidate.relative_root), 'snapshot_id': 'fixed-release'})
    args = SimpleNamespace(sync_root=tmp_path/'cold', state_root=tmp_path/'state', apply=True,
                           origin_node_id='vastai1T')
    policy = ingress.load_policy(Path('configs/data_sync/training_return.json'))
    monkeypatch.setattr(syncthing, 'credentials', lambda: ('http://local', 'private-unit-key'))
    resilience_calls = []
    def resilience(*a):
        resilience_calls.append(shared_owner_held())
        return {'cold_primary_verified': not (changed == 'cold-authority' and shared_owner_held())}
    monkeypatch.setattr('stockagent.data_sync.cold_primary.verify_cold_resilience', resilience)
    peer_calls = []
    def peer(*a):
        peer_calls.append(shared_owner_held())
        return {'ok': not (changed == 'transport' and shared_owner_held()), 'checks': {'fresh': True}}
    monkeypatch.setattr('scripts.manage_packed_edge._convergence', peer)
    def fixed(*a):
        assert shared_owner_held()
        return SimpleNamespace(manifest_sha256=('b'*64 if changed == 'release' else resolved.manifest_sha256))
    monkeypatch.setattr(ingress, 'resolve_packed_snapshot_id', fixed)
    monkeypatch.setattr(ingress, 'make_ack', lambda *a, **k: {'identity_sha256': 'a'*64})
    monkeypatch.setattr(ingress, 'remote_retirement',
                        lambda *a, **k: pytest.fail('changed gate authorized source retirement'))
    result = ingress.returned_source(args, candidate, resolved, policy)
    assert result['deleted'] is False
    assert resilience_calls[0] is False and peer_calls[:2] == [False, False]
    if changed == 'transport':
        assert peer_calls[-1] is True and result['state'] == 'waiting-packed-peer-convergence'
        assert len(ingress.waiting_returns(args.state_root, policy)) == 1
    else:
        assert result['state'] == 'remote-retirement-failed'
    assert not shared_owner_held()
