"""A private-copy instruction must not open public source publication."""
from datetime import datetime, timezone
import hashlib
import json
import threading
import fcntl

import pytest

from scripts.backup_delivery_receipt import READY, atomic_public, signed, verify_dispatch_delivery
from stockagent.data_sync.backup_stream import BackupStream
from stockagent.data_sync import news_private_backup as module
from stockagent.data_sync.packed_snapshots import initialize_packed_layout


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


@pytest.fixture
def news(tmp_path):
    root = tmp_path / "data_news"
    generation = "versions/current"
    marker = root / generation / "generation.json"
    marker.parent.mkdir(parents=True)
    marker.write_text('{}')
    parts = []
    for name, raw in [("versions/prior/articles/a.parquet", b"old retained observation"),
                      ("versions/current/documents/b.parquet", b"new observed body")]:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        parts.append({"path": name, "sha256": sha(raw), "bytes": len(raw),
                      "table_name": "articles" if "articles" in name else "documents", "rows": 1})
    raw = b"original frozen source snapshot"
    chunk_path = generation + "/snapshot_chunks/original.bin"
    (root / chunk_path).parent.mkdir()
    (root / chunk_path).write_bytes(raw)
    snapshot = {"snapshot_sha256": sha(raw), "snapshot_storage": {
        "format": "news_sqlite_chunks_v1", "chunk_bytes": 1024**2,
        "chunks": [{"path": chunk_path, "offset": 0, "bytes": len(raw), "sha256": sha(raw)}],
        "logical_bytes": len(raw)}}
    manifest = {"contract_version": 2, "generation": generation, "state": "ready",
                "storage_layout": "news_incremental_parts_v2", "parts": parts,
                "source_snapshot": snapshot, "row_counts": {"articles": 1, "documents": 1},
                "publish": False, "remote_training_export": False, "training_injection_enabled": False}
    raw = module.encoded(manifest)
    (root / generation / "manifest.json").write_bytes(raw)
    (root / "manifest.json").write_bytes(raw)
    policy = {"contract": module.POLICY, "source_root": "data_news", "generation": generation,
              "manifest_sha256": sha(raw), "private_backup_authorized": True,
              "public_publish_allowed": False, "automatic_training_injection_allowed": False,
              "source_licenses_changed": False, "route": "existing_paired_encrypted_Restic_relay",
              "user_instruction": "Copy my fixed news version to my existing personal NAS",
              "user_instruction_date": "2026-10-28", "producer_device_id": "source-test",
              "receiver_device_id": "relay-test", "repository_id": "a" * 64}
    return tmp_path, root, manifest, policy


def test_capture_includes_old_shared_parts_and_exact_source_snapshot(news):
    repo, root, original, policy = news
    before = (root / "manifest.json").read_bytes()
    plan = module.capture_plan(repo, policy, policy)
    module.validate_plan(plan, policy, policy)
    paths = {row["relative"] for row in plan["source_files"]}
    assert "data_news/versions/prior/articles/a.parquet" in paths
    assert "data_news/versions/current/snapshot_chunks/original.bin" in paths
    assert "data_news/manifest.json" in paths
    assert plan["files"] == 5
    assert (root / "manifest.json").read_bytes() == before
    assert not plan["public_publish_allowed"] and not plan["live_database_copied"]


@pytest.mark.parametrize("key,value", [("private_backup_authorized", False),
    ("public_publish_allowed", True), ("automatic_training_injection_allowed", True),
    ("source_licenses_changed", True), ("receiver_device_id", "another-peer"),
    ("source_root", "data_news_fulltext"), ("route", "public-lake")])
def test_explicit_policy_cannot_open_other_authorities(news, key, value):
    repo, _, _, policy = news
    changed = {**policy, key: value}
    with pytest.raises(ValueError, match="exact explicit paired policy"):
        module.capture_plan(repo, changed, policy)


def test_capture_refuses_missing_shared_donor(news):
    repo, root, manifest, policy = news
    (root / manifest["parts"][0]["path"]).unlink()
    with pytest.raises(FileNotFoundError):
        module.capture_plan(repo, policy, policy)


def test_capture_refuses_corrupt_source_snapshot(news):
    repo, root, manifest, policy = news
    (root / manifest["source_snapshot"]["snapshot_storage"]["chunks"][0]["path"]).write_bytes(b"bad")
    with pytest.raises(ValueError, match="chunk checksum"):
        module.capture_plan(repo, policy, policy)


def test_fixed_source_copy_does_not_overwrite_partial_staging(news, tmp_path):
    repo, root, _, policy = news
    plan = module.capture_plan(repo, policy, policy)
    row = plan["source_files"][0]
    source = root / row["source_relative"]
    target = tmp_path / "staged.json"
    assert module.copy_member(source, target, row)
    assert not module.copy_member(source, target, row)
    target.write_bytes(b"mismatch")
    with pytest.raises(ValueError, match="preserve mismatched"):
        module.copy_member(source, target, row)
    assert target.read_bytes() == b"mismatch"


def test_resume_signature_reuse_still_rejects_source_or_destination_changes(news, tmp_path):
    repo, root, _, policy = news
    plan = module.capture_plan(repo, policy, policy)
    row = plan['source_files'][0]
    source = root / row['source_relative']
    target = tmp_path / 'resume.json'
    module.copy_member(source, target, row)
    fresh = module.signature(target)
    assert not module.copy_member(source, target, row, verified_signature=fresh)
    target.write_bytes(b'a different destination generation')
    with pytest.raises(ValueError, match='staging generation changed'):
        module.copy_member(source, target, row, verified_signature=fresh)
    source.write_bytes(b'a different original source generation')
    with pytest.raises(ValueError, match='source generation changed'):
        module.copy_member(source, target, row, verified_signature=fresh)


@pytest.mark.parametrize("field,value", [("relative", "data_news/../../outside"),
    ("source_relative", "../outside"), ("source_relative", "/outside")])
def test_self_signed_plan_cannot_traverse_paths(news, field, value):
    repo, _, _, policy = news
    plan = module.capture_plan(repo, policy, policy)
    plan["source_files"][0][field] = value
    plan["identity_sha256"] = module.identity({k: v for k, v in plan.items() if k != "identity_sha256"})
    with pytest.raises(ValueError, match="path is unsafe"):
        module.validate_plan(plan, policy, policy)


def test_waves_send_each_shared_file_once_with_one_global_plan(news):
    repo, _, _, policy = news
    plan = module.capture_plan(repo, policy, policy)
    waves = module.deliveries(plan, maximum_bytes=4 * 1024**2, maximum_files=5)
    members = [row for wave in waves for row in wave["members"]]
    assert members == plan["source_files"]
    assert sum(len(wave["controls"]) for wave in waves) == 1
    assert all(len(wave["files"]) + 2 <= 5 for wave in waves)
    assert all(wave["complete_bytes"] <= 4 * 1024**2 for wave in waves)


def backup_queue(tmp_path, policy):
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="penguin")
    transport = tmp_path / "transport"
    transport.mkdir()
    (transport / ".stfolder").mkdir()
    (transport / ".stignore").write_text("(?d).staging\n(?d).staging/**\n")
    config = {"schema_version": 1, "cold_root": str(cold), "publication_catalog": str(tmp_path / "unused.json"),
        "transport_root": str(transport), "receipt_root": str(tmp_path / "receipts"),
        "state_root": str(tmp_path / "state"), "required_mounts": {}, "require_transport_mount": False,
        **{k: policy[k] for k in module.PAIRED}, "maximum_batch_bytes": 4 * 1024**2,
        "maximum_batch_files": 5, "maximum_pending_bytes": 64 * 1024**2,
        "maximum_pending_deliveries": 4, "maximum_retained_transport_bytes": 64 * 1024**2,
        "reserve_bytes": 1024, "readiness_max_age_seconds": 900, "automatic_pruning": False,
        "automatic_batch_deletion": False, "source_cleanup_authorized": False,
        "pipeline": {"maximum_waves_per_cycle": 4, "copy_workers": 2, "verify_workers": 2,
                     "retry_delays_seconds": [30]}}
    queue = BackupStream(config)
    ready = {"contract": READY, **{k: config[k] for k in module.PAIRED},
        "ingress_free_bytes": 64 * 1024**2, "nas_free_bytes": 64 * 1024**2, "reserve_bytes": 1024,
        "maximum_batch_bytes": 4 * 1024**2, "repository_check_mode": "full_read_data",
        "single_owner_verified": True, "nas_mount_guard_verified": True, "runtime_lock_verified": True,
        "automatic_pruning": False, "automatic_batch_deletion": False,
        "observed_at_utc": datetime.now(timezone.utc).isoformat()}
    atomic_public(queue.receipts / "readiness.json", signed(ready))
    return queue


def test_original_owner_publication_receiver_readback_and_idempotent_resume(news, tmp_path):
    repo, root, _, policy = news
    plan = module.capture_plan(repo, policy, policy)
    queue = backup_queue(tmp_path, policy)
    progress = tmp_path / "progress.json"
    first = module.publish_plan(queue, repo, plan, progress, wait_seconds=0)
    assert first["published_source_files"] == plan["files"]
    assert first["NAS_restore_ACK_verified"] is False
    assert first["source_files_deleted"] == 0
    ledger = queue.load_ledger()
    assert len(ledger["deliveries"]) == first["total_parts"]
    for key, record in ledger["deliveries"].items():
        dispatch = record["dispatch"]
        result = verify_dispatch_delivery(queue.transport / dispatch["batch_relative"], dispatch, workers=2)
        assert result["envelope_identity_sha256"] == key
        assert dispatch["delivery_kind"] == "private_personal_news_backup"
        assert result["durable_off_host_backup_verified"] is False
    second = module.publish_plan(queue, repo, plan, progress, wait_seconds=0)
    assert first["published_parts"] == second["published_parts"]
    assert len(queue.load_ledger()["deliveries"]) == len(ledger["deliveries"])
    assert json.loads((root / "manifest.json").read_bytes())["publish"] is False


def test_network_wait_releases_original_publication_owner(news, tmp_path, monkeypatch):
    repo, _, _, policy = news
    plan = module.capture_plan(repo, policy, policy)
    queue = backup_queue(tmp_path, policy)
    pending, finish = threading.Event(), threading.Event()
    def scan(_):
        pending.set()
        assert finish.wait(10)
    monkeypatch.setattr(queue, "scan_transport", scan)
    errors = []
    def publish():
        try:
            module.publish_plan(queue, repo, plan, tmp_path / "progress.json", wait_seconds=0)
        except BaseException as error:
            errors.append(error)
    worker = threading.Thread(target=publish)
    worker.start()
    try:
        assert pending.wait(5)
        # All publication finishes without joining the blocked notifier. The
        # ordinary owner can then acquire its journal lock before networking.
        import time
        deadline = time.monotonic() + 5
        with (queue.state / "owner.lock").open("a") as owner:
            while True:
                try:
                    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() > deadline:
                        pytest.fail("network wait retained the original backup owner")
                    time.sleep(.01)
    finally:
        finish.set()
        worker.join(5)
    assert not worker.is_alive() and not errors


def test_closed_wave_handoff_reloads_concurrent_owner_and_receipts_without_copying(news, tmp_path, monkeypatch):
    from test_backup_stream import acknowledge
    repo, root, _, policy = news
    plan = module.capture_plan(repo, policy, policy)
    queue = backup_queue(tmp_path, policy)
    acquire = module.acquire_owner
    acquisitions = []
    def concurrent_turn(owner, *, wait_seconds):
        if acquisitions:
            with (queue.state / 'owner.lock').open('a') as other:
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
                ledger = queue.load_ledger()
                ledger['concurrent_original_owner_turns'] = len(acquisitions)
                queue._save_ledger(ledger)
        acquire(owner, wait_seconds=wait_seconds)
        acquisitions.append(True)
    monkeypatch.setattr(module, 'acquire_owner', concurrent_turn)
    progress = tmp_path / 'progress.json'
    result = module.publish_plan(queue, repo, plan, progress, wait_seconds=0, yield_owner_between_waves=True)
    assert len(acquisitions) > 1 and result['published_source_files'] == plan['files']
    ledger = queue.load_ledger()
    assert ledger['concurrent_original_owner_turns'] == len(acquisitions) - 1
    for record in ledger['deliveries'].values():
        acknowledge(queue, record)
    def forbidden_copy(*args, **kwargs):
        raise AssertionError('receipt reconciliation must not copy source files')
    monkeypatch.setattr(module, 'copy_member', forbidden_copy)
    acknowledged = module.reconcile_plan(queue, plan, progress, wait_seconds=0)
    assert acknowledged['NAS_restore_ACK_verified'] and acknowledged['source_files_deleted'] == 0
    assert acknowledged['published_source_bytes'] == plan['source_bytes']
    assert (root / 'manifest.json').exists()


def test_news_handoff_readmits_capacity_and_retains_the_fixed_remaining_plan(news, tmp_path, monkeypatch):
    repo, root, _, policy = news
    plan = module.capture_plan(repo, policy, policy)
    queue = backup_queue(tmp_path, policy)
    acquire = module.acquire_owner
    acquisitions = []
    def consumed_after_yield(owner, *, wait_seconds):
        if acquisitions:
            queue.config['maximum_pending_bytes'] = 1
        acquire(owner, wait_seconds=wait_seconds)
        acquisitions.append(True)
    monkeypatch.setattr(module, 'acquire_owner', consumed_after_yield)
    with pytest.raises(module.NewsCapacityDeferred):
        module.publish_plan(queue, repo, plan, tmp_path / 'progress.json', wait_seconds=0,
                            yield_owner_between_waves=True)
    assert len(queue.load_ledger()['deliveries']) == 1
    assert (root / 'manifest.json').exists()
