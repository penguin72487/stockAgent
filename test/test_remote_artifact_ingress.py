from __future__ import annotations

import json
import fcntl
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


def test_one_pending_return_blocks_a_new_publication_wave(tmp_path, monkeypatch, capsys):
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
        assert kw["scope"] == "markets" and kw["relative_root"] == pending.relative_root
        return [pending]
    monkeypatch.setattr(ingress, "discover_remote_artifacts", exact_discovery)
    checked = []
    def preserved(sync_root, spec, candidate, **kwargs):
        assert kwargs == {"verify_content": False}
        checked.append(candidate.relative_root)
        return SimpleNamespace(manifest={"snapshot_id": wave["snapshot_id"]})
    monkeypatch.setattr(ingress, "_resolved_release", preserved)
    monkeypatch.setattr(ingress, "returned_source", lambda *a: {"state":"waiting-packed-peer-convergence", "deleted":False})
    monkeypatch.setattr(ingress, "publish_cold_artifact", lambda *a, **kw: pytest.fail("unconfirmed wave was bypassed"))
    assert ingress.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert checked == [pending.relative_root] and result["published"] == 0 and result["waiting_return"] == wave
    assert len(discovery) == 1
    assert json.loads(marker.read_text()) == wave


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
    assert (args.state_root / "training-return-waiting-peer.json").is_file()
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
    assert result["deleted"] is False and (args.state_root / "training-return-waiting-peer.json").is_file()


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
        assert (args.state_root/'training-return-waiting-peer.json').exists()
    else:
        assert result['state'] == 'remote-retirement-failed'
    assert not shared_owner_held()
