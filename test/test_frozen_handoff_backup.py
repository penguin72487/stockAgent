"""Source-pinned control backup, original-owner recovery, and capacity gates."""
from datetime import datetime, timezone
import json

import pytest

from scripts.backup_delivery_receipt import READY, atomic_public, signed, verify_dispatch_delivery
from scripts.verify_backup_delivery import CONTRACT
from stockagent.data_sync.backup_stream import BackupStream
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.frozen_handoff_backup import publish_handoff_backups
from stockagent.data_sync.offhost_backup import private_json


@pytest.fixture
def queue_and_handoff(tmp_path, monkeypatch):
    root = tmp_path / "transport"
    root.mkdir()
    (root / ".stfolder").mkdir()
    (root / ".stignore").write_text("(?d).staging\n(?d).staging/**\n")
    config = {"schema_version": 1, "cold_root": str(tmp_path / "cold"), "transport_root": str(root),
              "receipt_root": str(tmp_path / "receipts"), "state_root": str(tmp_path / "state"),
              "required_mounts": {}, "require_transport_mount": False,
              "producer_device_id": "source-test", "receiver_device_id": "receiver-test", "repository_id": "a" * 64,
              "automatic_pruning": False, "automatic_batch_deletion": False, "source_cleanup_authorized": False,
              **{key: 64 * 1024**2 for key in ("maximum_batch_bytes", "maximum_batch_files", "maximum_pending_bytes",
                  "maximum_pending_deliveries", "maximum_retained_transport_bytes", "reserve_bytes", "readiness_max_age_seconds")}}
    queue = BackupStream(config)
    monkeypatch.setattr(queue, "paired_transport", lambda: ("test", "test", "test"))
    ready = signed({"contract": READY, **{k: config[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")},
                    "ingress_free_bytes": 512 * 1024**2, "nas_free_bytes": 512 * 1024**2,
                    "reserve_bytes": 1024, "maximum_batch_bytes": 64 * 1024**2,
                    "repository_check_mode": "full_read_data", "single_owner_verified": True,
                    "nas_mount_guard_verified": True, "runtime_lock_verified": True,
                    "automatic_pruning": False, "automatic_batch_deletion": False,
                    "observed_at_utc": datetime.now(timezone.utc).isoformat()})
    atomic_public(queue.receipts / "readiness.json", ready)
    handoff = root / "tools" / "frozen-test"
    handoff.mkdir(parents=True)
    body = b"fixed source bytes\n"
    (handoff / "data.json").write_bytes(body)
    import hashlib
    row = {"relative": "data.json", "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    manifest = signed({"contract": "frozen_backup_receiver_handoff_v1", "files": [row],
                       **{k: config[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")},
                       "replace_existing_worker": False, "automatic_execution_from_syncthing": False,
                       "private_credentials_included": False})
    private_json(handoff / "handoff-manifest.json", manifest)
    raw = (handoff / "handoff-manifest.json").read_bytes()
    manifest_row = {"relative": "handoff-manifest.json", "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    envelope = signed({"contract": CONTRACT, "source_plan_identity_sha256": manifest["identity_sha256"],
                       "files": [row, manifest_row], "selection": "frozen_backup_receiver_handoff",
                       "all_history_verified": False, "unpublished_sources_included": False})
    private_json(handoff / "backup-envelope.json", envelope)
    (handoff / "READY").write_text(envelope["identity_sha256"] + "\n")
    return queue, handoff, envelope["identity_sha256"]


def test_pinned_handoff_gets_a_normal_dispatch_and_idempotent_retry(queue_and_handoff):
    queue, handoff, key = queue_and_handoff
    result = publish_handoff_backups(queue, [(handoff.name, key)])
    assert result["source_deleted"] is False and result["all_nas_acks_verified"] is False
    ledger = queue.load_ledger()
    dispatch = ledger["deliveries"][key]["dispatch"]
    batch = queue.transport / ("batch-" + key)
    assert verify_dispatch_delivery(batch, dispatch)["files_verified"] == 2
    assert (batch / "data.json").read_bytes() == (handoff / "data.json").read_bytes()
    assert json.loads((queue.transport / "tools/dispatch" / (key + ".json")).read_bytes()) == dispatch
    again = publish_handoff_backups(queue, [(handoff.name, key)])
    assert again["deliveries"][0]["state"] == "published_waiting_nas_receipt"
    assert len(queue.load_ledger()["deliveries"]) == 1


def test_corrupt_or_wrongly_pinned_handoff_never_creates_dispatch(queue_and_handoff):
    queue, handoff, key = queue_and_handoff
    with pytest.raises(ValueError):
        publish_handoff_backups(queue, [(handoff.name, "f" * 64)])
    (handoff / "data.json").write_bytes(b"changed")
    with pytest.raises(ValueError):
        publish_handoff_backups(queue, [(handoff.name, key)])
    assert not (queue.transport / ("batch-" + key)).exists()
    assert not queue.ledger_path.exists()


def test_capacity_gate_leaves_fixed_handoff_and_ledger_intact(queue_and_handoff, monkeypatch):
    queue, handoff, key = queue_and_handoff
    monkeypatch.setattr(queue, "staging_room", lambda usage: 0)
    with pytest.raises(SnapshotError, match="reserve"):
        publish_handoff_backups(queue, [(handoff.name, key)])
    assert not queue.load_ledger()["deliveries"]
    assert (handoff / "data.json").exists()
    assert not (queue.transport / ("batch-" + key)).exists()


def test_interrupted_publication_recovers_from_original_owner_journal(queue_and_handoff, monkeypatch):
    queue, handoff, key = queue_and_handoff
    from stockagent.data_sync import backup_stream
    original_rename = backup_stream.os.rename
    def interrupted(src, dst):
        if str(dst).endswith("batch-" + key):
            raise OSError("simulated interruption after durable journal")
        return original_rename(src, dst)
    with monkeypatch.context() as scope:
        scope.setattr(backup_stream.os, "rename", interrupted)
        with pytest.raises(OSError, match="simulated"):
            publish_handoff_backups(queue, [(handoff.name, key)])
    assert queue.load_ledger()["deliveries"][key]["state"] == "publication_prepared"
    result = publish_handoff_backups(queue, [(handoff.name, key)])
    assert result["deliveries"][0]["state"] == "published_waiting_nas_receipt"
    verify_dispatch_delivery(queue.transport / ("batch-" + key), queue.load_ledger()["deliveries"][key]["dispatch"])
