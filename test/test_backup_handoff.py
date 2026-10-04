"""Frozen receiver package completeness and isolated importability."""
import hashlib
import fcntl
from pathlib import Path
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
import time

import pytest

from scripts.publish_backup_handoff import FILES, publish_handoff
from scripts.verify_backup_delivery import verify
from stockagent.data_sync.backup_stream import BackupStream


@pytest.fixture
def handoff_queue(tmp_path):
    root = tmp_path / "transport"
    root.mkdir()
    (root / ".stfolder").mkdir()
    (root / ".stignore").write_text("(?d).staging\n(?d).staging/**\n")
    config = {"schema_version": 1, "cold_root": str(tmp_path / "cold"), "transport_root": str(root),
        "receipt_root": str(tmp_path / "receipts"), "state_root": str(tmp_path / "state"),
        "required_mounts": {}, "require_transport_mount": False, "producer_device_id": "source-test",
        "receiver_device_id": "receiver-test", "repository_id": "a" * 64, "automatic_pruning": False,
        "automatic_batch_deletion": False, "source_cleanup_authorized": False,
        **{k: 64 * 1024**2 for k in ("maximum_batch_bytes", "maximum_batch_files", "maximum_pending_bytes",
                                   "maximum_pending_deliveries", "maximum_retained_transport_bytes", "reserve_bytes",
                                   "readiness_max_age_seconds")}}
    return BackupStream(config)


def test_closed_handoff_has_all_dependencies_without_source_checkout_fallback(handoff_queue):
    queue = handoff_queue
    proof = publish_handoff(queue, "continuous-backup-20261004-v1")
    root = Path(proof["source_path"])
    verified = verify(root, proof["envelope_identity_sha256"])
    assert verified["files_verified"] == len(FILES) + 1
    assert hashlib.sha256((root / "handoff-manifest.json").read_bytes()).hexdigest() == proof["handoff_manifest_sha256"]
    code = "import sys;sys.path.insert(0,sys.argv[1]);import scripts.backup_delivery_receipt;import stockagent.data_sync.backup_recovery;import stockagent.control.recovery;import scripts.configure_backup_receipts_syncthing;import stockagent.data_sync.nas_recovery_acceptance;import scripts.run_nas_recovery_acceptance;import stockagent.data_sync.recovery_queue;import scripts.run_lab203_recovery_queue;import scripts.install_lab203_recovery_queue"
    subprocess.run([sys.executable, "-I", "-B", "-c", code, str(root)], cwd=root.parent, check=True)
    verify(root, proof["envelope_identity_sha256"])
    assert proof["receiver_adapter_deployed"] is False
    with pytest.raises(ValueError, match="previously frozen"):
        publish_handoff(queue, "continuous-backup-20261004-v1")


def test_operator_can_wait_for_existing_owner_without_parallel_publication(handoff_queue):
    queue = handoff_queue
    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            publish_handoff(queue, 'continuous-backup-20261004-v4')
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(publish_handoff, queue, 'continuous-backup-20261004-v4', wait_for_owner=True)
            time.sleep(0.05)
            assert not future.done()
            assert not (queue.transport / 'tools/continuous-backup-20261004-v4').exists()
            fcntl.flock(owner, fcntl.LOCK_UN)
            proof = future.result(timeout=10)
    verify(Path(proof['source_path']), proof['envelope_identity_sha256'])


def test_handoff_can_publish_data_tasks_under_same_owner(handoff_queue, monkeypatch):
    from stockagent.data_sync import recovery_queue
    calls = []
    def requests(config, transport, ledger):
        with (handoff_queue.state / "owner.lock").open("a") as other:
            with pytest.raises(BlockingIOError): fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        calls.append(transport)
        return {"state": "requests_published", "published_requests": 1}
    monkeypatch.setattr(recovery_queue, "publish_requests", requests)
    result = publish_handoff(handoff_queue, "continuous-backup-20261004-v7", publish_recovery_tasks=True)
    assert calls == [handoff_queue.transport] and result["automatic_recovery_requests"]["published_requests"] == 1
    from scripts.install_lab203_recovery_queue import bootstrap_pins
    pins = bootstrap_pins(handoff_queue.transport / "tools/automatic-backup-bootstrap-v7.json")
    assert pins["envelope_identity_sha256"] == result["envelope_identity_sha256"]
    assert pins["handoff_manifest_sha256"] == result["handoff_manifest_sha256"]
    assert pins["receiver_hook_deployed"] is False


def test_installer_bootstrap_does_not_write_bytecode_into_closed_synced_package(handoff_queue):
    proof = publish_handoff(handoff_queue, "continuous-backup-20261004-v7")
    root = Path(proof["source_path"])
    # No -B and no reliance on an inherited environment: the installer protects itself.
    subprocess.run([sys.executable, "-I", str(root / "scripts/install_lab203_recovery_queue.py"), "--help"],
                   cwd=root.parent, capture_output=True, check=True)
    assert not list(root.rglob("__pycache__"))
    verify(root, proof["envelope_identity_sha256"])
