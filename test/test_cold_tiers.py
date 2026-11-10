import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

import pytest

from stockagent.data_sync import cold_tiers as tiers
from scripts import maintain_cold_tiers as maintenance


@pytest.mark.parametrize('identity_exit', [0, 78])
def test_unattended_wrapper_initializes_enrolled_bridge_before_work(tmp_path, identity_exit):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    source = Path(__file__).parents[1] / 'scripts/run_cold_tiers.sh'
    wrapper = scripts / source.name
    wrapper.write_bytes(source.read_bytes())
    (scripts / 'runtime_env.sh').write_text(f'''
ensure_enrolled_wsl_identity() {{
  [[ "$1" == "$PWD" ]] || return 79
  printf '%s\\n' identity > initialization-order
  export STORAGE_TEST_BRIDGE=verified
  return {identity_exit}
}}
run_fintech_python() {{
  [[ "${{STORAGE_TEST_BRIDGE:-}}" == verified ]] || return 77
  printf '%s\\n' dispatch >> initialization-order
  printf '%s\\n' "$@" > dispatch.argv
}}
''')
    result = subprocess.run(['bash', str(wrapper)], cwd=tmp_path,
                            capture_output=True, text=True, timeout=5)
    assert result.returncode == identity_exit, result.stdout + result.stderr
    expected = ['identity', 'dispatch'] if identity_exit == 0 else ['identity']
    assert (tmp_path / 'initialization-order').read_text().splitlines() == expected
    if identity_exit == 0:
        assert (tmp_path / 'dispatch.argv').read_text().splitlines() == [
            'scripts/maintain_cold_tiers.py', '--apply', '--seed-existing', '--yield-to-migration']
    else:
        assert not (tmp_path / 'dispatch.argv').exists()


def test_automatic_tier_work_defers_to_pressure_without_erasing_last_work(tmp_path, monkeypatch, capsys):
    policy = json.loads((Path(__file__).parents[1] / 'configs/data_sync/cold_tiers.json').read_text())
    output = tmp_path/'status.json'
    previous = {'contract': tiers.CONTRACT, 'last_work_cycle': {'contract': tiers.CONTRACT,
                'apply': True, 'objects_completed': 2, 'allocation_bytes_saved': 123}}
    output.write_text(json.dumps(previous))
    monkeypatch.setattr(maintenance, 'load_policy', lambda _: policy)
    monkeypatch.setattr(maintenance, 'migration_priority_active', lambda: True)
    monkeypatch.setattr(maintenance, 'is_work_window', lambda *_a: True)
    monkeypatch.setattr(maintenance, 'cycle', lambda *_a, **_kw: pytest.fail('must yield to retirement'))
    monkeypatch.setattr(maintenance.sys, 'argv', ['maintain', '--apply', '--seed-existing',
                        '--yield-to-migration', '--output', str(output)])
    assert maintenance.main() == 0
    status = json.loads(output.read_text())
    assert status['state'] == 'waiting_migration_priority' and status['original_objects_deleted'] == 0
    assert status['last_work_cycle']['allocation_bytes_saved'] == 123
    assert json.loads(capsys.readouterr().out)['state'] == 'waiting_migration_priority'


def test_manual_tier_work_does_not_implicitly_opt_into_deferral(tmp_path, monkeypatch, capsys):
    policy = json.loads((Path(__file__).parents[1] / 'configs/data_sync/cold_tiers.json').read_text())
    monkeypatch.setattr(maintenance, 'load_policy', lambda _: policy)
    monkeypatch.setattr(maintenance, 'migration_priority_active', lambda: pytest.fail('manual work is explicit'))
    monkeypatch.setattr(maintenance, 'cycle', lambda *_a, **_kw: {'state': 'bounded_cycle_complete',
                        'apply': True, 'original_objects_deleted': 0})
    monkeypatch.setattr(maintenance.sys, 'argv', ['maintain', '--apply', '--output', str(tmp_path/'status.json')])
    assert maintenance.main() == 0
    assert json.loads(capsys.readouterr().out)['state'] == 'bounded_cycle_complete'


@pytest.mark.parametrize(('free_gib', 'pending', 'expected'), [(100, True, True),
                          (128, True, False), (100, False, False)])
def test_migration_priority_requires_capacity_pressure_and_actionable_queue(tmp_path, monkeypatch, free_gib, pending, expected):
    from types import SimpleNamespace
    from scripts import run_source_local_retirement_queue as retirement
    from stockagent.data_sync import cold_primary, workspace_volume
    monkeypatch.setattr(workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path), 'cold_root': str(tmp_path)})
    monkeypatch.setattr(workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda _path: None)
    monkeypatch.setattr(retirement, '_retirement_work_pending', lambda _path: pending)
    monkeypatch.setattr(maintenance, 'migration_owner_active', lambda: False)
    monkeypatch.setattr(maintenance.os, 'statvfs', lambda _path: SimpleNamespace(f_bavail=free_gib, f_frsize=1024**3))
    assert maintenance.migration_priority_active() is expected


def test_active_migration_owner_defers_tier_even_if_queue_observation_is_stale(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import run_source_local_retirement_queue as retirement
    from stockagent.data_sync import cold_primary, workspace_volume
    monkeypatch.setattr(workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path), 'cold_root': str(tmp_path)})
    monkeypatch.setattr(workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda _path: None)
    monkeypatch.setattr(retirement, '_retirement_work_pending', lambda _path: False)
    monkeypatch.setattr(maintenance, 'migration_owner_active', lambda: True)
    monkeypatch.setattr(maintenance.os, 'statvfs', lambda _path: SimpleNamespace(f_bavail=100, f_frsize=1024**3))
    assert maintenance.migration_priority_active()


def test_migration_owner_probe_handles_active_and_unavailable_systemd(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(maintenance.subprocess, 'run', lambda *_a, **_kw:
                        SimpleNamespace(returncode=0, stdout='inactive\nactivating\ninactive\n'))
    assert maintenance.migration_owner_active()
    monkeypatch.setattr(maintenance.subprocess, 'run', lambda *_a, **_kw:
                        (_ for _ in ()).throw(TimeoutError('offline')))
    assert not maintenance.migration_owner_active()


def test_outside_window_poll_preserves_last_actual_work_counts():
    policy = json.loads((Path(__file__).parents[1] / "configs/data_sync/cold_tiers.json").read_text())
    work = {"contract": tiers.CONTRACT, "state": "bounded_cycle_complete", "apply": True,
            "observed_at_epoch": 1000, "intent_waves_attempted": 1, "objects_completed": 2,
            "logical_bytes_processed": 1234, "allocation_bytes_saved": 456}
    first = tiers.status_record(work, policy, {})
    later = tiers.status_record({"state": "outside_daily_window", "objects_completed": 0}, policy, first)
    assert later["objects_completed"] == 0
    assert later["last_work_cycle"] == work
    assert later["schedule"]["daily_window_start_hour"] == 3
    assert later["schedule"]["reserve_bytes"] == policy["reserve_bytes"]
    from stockagent.data_sync.storage_monitor import public_cold_tiers
    assert public_cold_tiers(later, now=later["observed_at_epoch"])["schedule"]["reserve_bytes"] == policy["reserve_bytes"]
    dry = tiers.status_record({**work, "apply": False, "objects_completed": 9}, policy, later)
    assert dry["last_work_cycle"] == work
from stockagent.data_sync import cold_primary
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def environment(tmp_path, monkeypatch):
    authority = tmp_path / "authority"
    authority.mkdir()
    monkeypatch.setattr(tiers, "AUTHORITY", authority)
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", lambda *_: None)
    original_statvfs = os.statvfs
    monkeypatch.setattr(tiers.os, "statvfs", lambda path: type("Space", (), {
        "f_bavail": 128 * 1024**3, "f_frsize": 1})() if path == authority else original_statvfs(path))
    policy = json.loads((Path(__file__).parents[1] / "configs/data_sync/cold_tiers.json").read_text())
    policy.update(sync_root=str(authority), state_root=str(authority / ".local-state/cold-tiers"),
                  native_batch_bytes=2*1024**2, maximum_cycle_bytes=8*1024**2)
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy))
    policy_path.chmod(0o600)
    lock = authority / ".local-state/locks/publish-retention-global.lock"
    lock.parent.mkdir(parents=True)
    lock.touch()
    control = tmp_path / "control"
    (control / "dataset-versions").mkdir(parents=True)
    for name in ("dataset-versions/owner.lock", "source-replication-owner.lock", "catalog-owner.lock"):
        (control / name).touch(mode=0o600)
    control_policy = tmp_path / "control-policy.json"
    control_policy.write_text(json.dumps({"cold_root": str(authority), "state_root": str(control)}))
    control_policy.chmod(0o600)
    monkeypatch.setattr(tiers, "CONTROL_POLICY", control_policy)
    return authority, policy, policy_path


def blob(authority, payload, *, private=False, hot=False):
    sha = hashlib.sha256(payload).hexdigest()
    relative = (tiers.PRIVATE_PREFIX if private else "") + f"objects/blobs/{sha[:2]}/{sha}.blob"
    path = authority / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    if not hot:
        os.utime(path, (time.time()-86400, time.time()-86400))
    return relative


def enqueue(environment, names):
    root, _, policy_path = environment
    return tiers.enqueue(root, "test", names, policy_path=policy_path)


def verified_native(rows, _policy):
    return [{"sha256": row["sha256"], "bytes": row["bytes"], "codec": "LZX",
             "before_allocated_bytes": row["bytes"], "after_allocated_bytes": row["bytes"]//2,
             "native_full_sha_verified": True} for row in rows]


def test_unenrolled_node_does_no_work(tmp_path):
    assert tiers.enqueue(tmp_path, "test", ["anything"], policy_path=tmp_path/"missing") == 0
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("path", ["../objects/x", "objects/blobs/aa/" + "b"*64 + ".blob",
    "objects/blobs/aa/" + "a"*64 + ".blob/extra", "/objects/blobs/aa/" + "a"*64 + ".blob"])
def test_tier_intent_cannot_escape_immutable_object_namespace(path):
    with pytest.raises(SnapshotError):
        tiers.object_relative(path)


def test_local_policy_and_identity_bound_queue_are_idempotent(environment):
    root, policy, path = environment
    assert tiers.load_policy(path) == policy
    relative = blob(root, b"source")
    assert enqueue(environment, [relative, "objects/packs/not-a-blob.zip"]) == 1
    assert enqueue(environment, [relative]) == 1
    incoming = list((Path(policy["state_root"])/"incoming").glob("*.json"))
    assert len(incoming) == 1
    assert tiers.read_intent(incoming[0])["objects"] == [relative]
    body = json.loads(incoming[0].read_text())
    body["objects"] = [relative, relative]
    incoming[0].write_text(json.dumps(body))
    with pytest.raises(SnapshotError, match="identity"):
        tiers.read_intent(incoming[0])


def test_private_history_is_enqueued_without_expanding_transport_scope(environment):
    root, policy, path = environment
    relative = blob(root, b"private", private=True)
    tiers.enqueue(root / ".local-state/private-history", "private", [relative.removeprefix(tiers.PRIVATE_PREFIX)], policy_path=path)
    intent = next((Path(policy["state_root"])/"incoming").glob("*.json"))
    assert tiers.read_intent(intent)["objects"] == [relative]


def test_bounded_baseline_resumes_and_preserves_unknown_partial_files(environment):
    root, policy, path = environment
    names = [blob(root, bytes([i])) for i in range(3)]
    unknown = root / "objects/blobs/00/.unfinished.partial"
    unknown.parent.mkdir(parents=True, exist_ok=True)
    unknown.write_bytes(b"not a committed CAS object")
    policy["baseline_seed_objects_per_cycle"] = 1
    first = tiers.seed_existing(policy, policy_path=path)
    assert first["objects_enqueued"] == 1 and not first["complete"]
    for _ in range(4):
        result = tiers.seed_existing(policy, policy_path=path)
    assert result["complete"] and result["objects_enqueued"] == len(names)
    assert unknown.read_bytes() == b"not a committed CAS object"


def test_outside_daily_window_does_not_probe_disks_or_start_compression(environment, monkeypatch):
    _, policy, _ = environment
    monkeypatch.setattr(tiers, "is_work_window", lambda *_: False)
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", lambda *_: pytest.fail("off-hours disk probe"))
    assert tiers.cycle(policy, apply=True)["state"] == "outside_daily_window"


def test_multiple_bounded_waves_finish_same_cohort_and_retain_raw_bytes(environment, monkeypatch):
    root, policy, path = environment
    names = [blob(root, bytes([i])*2*1024**2) for i in range(3)]
    enqueue(environment, names)
    calls = []
    def native(rows, policy):
        calls.append(rows)
        return verified_native(rows, policy)
    monkeypatch.setattr(tiers, "native_compress", native)
    result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert len(calls) == 3 and all(len(rows) == 1 for rows in calls)
    assert result["objects_completed"] == 3
    assert result["logical_bytes_processed"] == 6*1024**2
    assert result["original_objects_deleted"] == 0
    assert len(list((Path(policy["state_root"])/"cold").glob("*.json"))) == 1
    assert not list((Path(policy["state_root"])/"incoming").glob("*.json"))
    for name in names:
        assert hashlib.sha256((root/name).read_bytes()).hexdigest() == Path(name).stem


def test_busy_retirement_owner_defers_tiering_without_touching_payload(environment, monkeypatch):
    root, policy, path = environment
    name = blob(root, b"x"*2*1024**2)
    enqueue(environment, [name])
    monkeypatch.setattr(tiers, "native_compress", lambda *_: pytest.fail("busy publisher was interrupted"))
    lock = root / ".local-state/locks/publish-retention-global.lock"
    with lock.open("r+") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX)
        result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["state"] == "waiting_publication_window"
    assert (root/name).exists()
    assert list((Path(policy["state_root"])/"incoming").glob("*.json"))


def test_shared_publisher_defers_physical_encoding_and_releases_partial_owners(environment, monkeypatch):
    root, policy, path = environment
    name = blob(root, b"x"*2*1024**2)
    enqueue(environment, [name])
    monkeypatch.setattr(tiers, "native_compress", lambda *_: pytest.fail("active immutable reader was interrupted"))
    lock = root / ".local-state/locks/publish-retention-global.lock"
    with lock.open("r+") as publisher:
        fcntl.flock(publisher, fcntl.LOCK_SH)
        result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["state"] == "waiting_publication_window"
    assert result["objects_completed"] == 0
    control = Path(tiers.read_document(tiers.CONTROL_POLICY)["state_root"])
    for name in ("dataset-versions/owner.lock", "source-replication-owner.lock", "catalog-owner.lock"):
        with (control / name).open("r+") as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_physical_encoding_excludes_shared_readers_and_retirement(environment, monkeypatch):
    root, policy, path = environment
    name = blob(root, b"x"*2*1024**2)
    enqueue(environment, [name])
    lock = root / ".local-state/locks/publish-retention-global.lock"
    def native(rows, policy):
        for mode in (fcntl.LOCK_SH, fcntl.LOCK_EX):
            with lock.open("r+") as reader_or_retirement:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(reader_or_retirement, mode | fcntl.LOCK_NB)
        return verified_native(rows, policy)
    monkeypatch.setattr(tiers, "native_compress", native)
    result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["objects_completed"] == 1


@pytest.mark.parametrize("name", ["dataset-versions/owner.lock", "source-replication-owner.lock", "catalog-owner.lock"])
@pytest.mark.parametrize("mode", [fcntl.LOCK_SH, fcntl.LOCK_EX])
def test_original_control_owner_defers_tiering_without_consuming_wave(environment, monkeypatch, name, mode):
    root, policy, path = environment
    member = blob(root, b"x"*2*1024**2)
    enqueue(environment, [member])
    monkeypatch.setattr(tiers, "native_compress", lambda *_: pytest.fail("active control owner was interrupted"))
    control = Path(tiers.read_document(tiers.CONTROL_POLICY)["state_root"])
    with (control / name).open("r+") as owner:
        fcntl.flock(owner, mode)
        result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["state"] == "waiting_publication_window"
    assert result["objects_completed"] == result["failed_intents"] == 0
    assert not list((Path(policy["state_root"]) / "work").glob("*.json"))
    assert list((Path(policy["state_root"]) / "incoming").glob("*.json"))


def test_full_fixed_recovery_and_later_encoding_preserve_both_original_proofs(environment, monkeypatch, tmp_path):
    from stockagent.data_sync import dataset_migration as migration, packed_snapshots as packed
    from stockagent.data_sync.windows_cold_io import _stable
    root, policy, path = environment
    source = tmp_path / "original-source"
    source.mkdir()
    payload = b"x" * (2*1024**2 + 17)
    (source / "original.bin").write_bytes(payload)
    original_enqueue = tiers.enqueue
    monkeypatch.setattr(tiers, "enqueue", lambda sync_root, dataset, members, *, policy_path=path:
        original_enqueue(sync_root, dataset, members, policy_path=policy_path))
    release = packed.publish_packed_snapshot(root, "full-recovery", source,
        loose_file_threshold_bytes=128, pack_buckets=1, file_chunk_bytes=1024**2)
    names = [row["relpath"] for row in release.manifest["archive"]["objects"] if row["kind"] == "blob"]
    for name in names:
        os.utime(root / name, (time.time()-86400, time.time()-86400))
    enqueue(environment, names)
    before = {name: (root / name).lstat() for name in names}
    calls = []
    def native(rows, policy):
        calls.append("encoding")
        for row in rows:
            original = root / row["path"]
            assert hashlib.sha256(original.read_bytes()).hexdigest() == row["sha256"]
            mode = original.stat().st_mode & 0o777
            os.chmod(original, mode ^ 0o200)
            os.chmod(original, mode)
        return verified_native(rows, policy)
    monkeypatch.setattr(tiers, "native_compress", native)
    original_iterator = packed._iter_chunked_file
    interleaved = []
    def interleave(*args, **kwargs):
        if not interleaved:
            interleaved.append(tiers.cycle(policy, policy_path=path, apply=True, force_window=True))
            assert interleaved[0]["state"] == "waiting_publication_window"
        yield from original_iterator(*args, **kwargs)
    monkeypatch.setattr(packed, "_iter_chunked_file", interleave)
    control = Path(tiers.read_document(tiers.CONTROL_POLICY)["state_root"])
    def recover(label):
        return migration._recover(root, "full-recovery", release.manifest["snapshot_id"],
            tmp_path / (label + ".json"), tmp_path / (label + "-progress.json"))
    with migration._operation_owner(control / "dataset-versions"):
        first = recover("before-encoding")
    assert first["full_original_file_reconstruction_verified"] is True
    assert interleaved and not calls
    assert all(_stable(info, (root / name).lstat()) for name, info in before.items())
    result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["objects_completed"] and calls
    assert any(not _stable(info, (root / name).lstat()) for name, info in before.items())
    second = recover("after-encoding")
    assert second["full_original_file_reconstruction_verified"] is True
    assert second["manifest_sha256"] == first["manifest_sha256"]
    assert (source / "original.bin").read_bytes() == payload


def test_missing_original_control_owner_is_not_recreated(environment, monkeypatch):
    root, policy, path = environment
    name = blob(root, b"x"*2*1024**2)
    enqueue(environment, [name])
    control = Path(tiers.read_document(tiers.CONTROL_POLICY)["state_root"])
    owner = control / "dataset-versions/owner.lock"
    owner.unlink()
    monkeypatch.setattr(tiers, "native_compress", lambda *_: pytest.fail("missing owner cannot admit encoding"))
    result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["failed_intents"] == 1 and result["objects_completed"] == 0
    assert not owner.exists()


def test_replaced_original_control_owner_does_not_acknowledge_encoding(environment, monkeypatch):
    root, policy, path = environment
    name = blob(root, b"x"*2*1024**2)
    enqueue(environment, [name])
    control = Path(tiers.read_document(tiers.CONTROL_POLICY)["state_root"])
    owner = control / "catalog-owner.lock"
    def native(rows, policy):
        replacement = owner.with_suffix(".replacement")
        replacement.touch(mode=0o600)
        os.replace(replacement, owner)
        return verified_native(rows, policy)
    monkeypatch.setattr(tiers, "native_compress", native)
    result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["failed_intents"] == 1 and result["objects_completed"] == 0
    assert not list((Path(policy["state_root"]) / "work").glob("*.json"))
    assert list((Path(policy["state_root"]) / "incoming").glob("*.json"))


def test_hot_increment_waits_while_stable_members_finish(environment, monkeypatch):
    root, policy, path = environment
    cold = blob(root, b"x"*2*1024**2)
    hot = blob(root, b"y"*2*1024**2, hot=True)
    enqueue(environment, [cold, hot])
    monkeypatch.setattr(tiers, "native_compress", verified_native)
    result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["objects_completed"] == 1
    progress = tiers.read_document(next((Path(policy["state_root"])/"work").glob("*.json")))
    assert set(progress["completed"]) == {cold}
    assert (root/hot).exists()


def test_failed_native_readback_leaves_originals_and_pending_cohort(environment, monkeypatch):
    root, policy, path = environment
    name = blob(root, b"x"*2*1024**2)
    enqueue(environment, [name])
    def failed(*_):
        raise SnapshotError("SHA readback failed")
    monkeypatch.setattr(tiers, "native_compress", failed)
    result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["state"] == "partial_failures_preserved" and result["failed_intents"] == 1
    assert hashlib.sha256((root/name).read_bytes()).hexdigest() == Path(name).stem
    assert not (Path(policy["state_root"])/"cold").exists()
    assert list((Path(policy["state_root"])/"incoming").glob("*.json"))


def test_missing_cohort_does_not_block_other_stable_objects(environment, monkeypatch):
    root, policy, path = environment
    missing = blob(root, b"x"*2*1024**2)
    good = blob(root, b"y"*2*1024**2)
    enqueue(environment, [missing])
    enqueue(environment, [good])
    (root/missing).unlink()  # Simulate a pre-existing unavailable object.
    monkeypatch.setattr(tiers, "native_compress", verified_native)
    result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["state"] == "partial_failures_preserved"
    assert result["failed_intents"] == result["objects_completed"] == 1
    incoming = list((Path(policy["state_root"])/"incoming").glob("*.json"))
    assert len(incoming) == 1 and tiers.read_intent(incoming[0])["objects"] == [missing]
    failure = tiers.read_document(Path(policy["state_root"])/"failures"/incoming[0].name)
    assert failure["cohort_acknowledged"] is False
    assert failure["original_objects_deleted"] == 0
    assert (root/good).read_bytes() == b"y"*2*1024**2


def test_failed_intent_attempts_have_a_bound_even_without_payloads(environment, monkeypatch):
    root, policy, path = environment
    for i in range(3):
        name = blob(root, bytes([i])*2*1024**2)
        enqueue(environment, [name])
        (root/name).unlink()
    policy["maximum_cycle_objects"] = 1
    monkeypatch.setattr(tiers, "native_compress", lambda *_: pytest.fail("missing objects cannot be encoded"))
    result = tiers.cycle(policy, policy_path=path, apply=True, force_window=True)
    assert result["intent_waves_attempted"] == result["failed_intents"] == 1
    assert len(list((Path(policy["state_root"])/"incoming").glob("*.json"))) == 3


def test_fifo_or_redirected_control_document_is_rejected_without_blocking(tmp_path):
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(SnapshotError, match="regular"):
        tiers.read_document(fifo)
    actual = tmp_path / "actual.json"
    actual.write_text("{}")
    redirected = tmp_path / "redirected.json"
    redirected.symlink_to(actual)
    with pytest.raises(SnapshotError, match="redirected"):
        tiers.read_document(redirected)


def test_daily_window_uses_taipei_and_has_no_all_day_reservation(environment):
    _, policy, _ = environment
    from datetime import datetime
    assert tiers.is_work_window(policy, datetime.fromisoformat("2026-10-25T03:00:00+08:00").timestamp())
    assert not tiers.is_work_window(policy, datetime.fromisoformat("2026-10-25T07:00:00+08:00").timestamp())
