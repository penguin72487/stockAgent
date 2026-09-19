from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time

import pytest

from stockagent.data_sync import packed_retention as module
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_backup import BackupConfig, PackedBackup
from stockagent.data_sync.packed_retention import RetentionConfig, apply_plan, build_plan
from stockagent.data_sync.packed_snapshots import (
    initialize_packed_layout,
    publish_packed_snapshot,
    resolve_latest_packed,
    resolve_packed_snapshot_id,
    verify_packed_snapshot,
)


class TestGuard:
    __test__ = False

    def __init__(self, *_args, **_kwargs):
        pass

    def check(self, **_kwargs):
        pass


def _peer_proof(*, ok: bool = True) -> dict:
    return {
        "ok": ok,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "peers": [
            {"name": "lab203", "ok": ok},
            {"name": "vastai1T", "ok": ok},
        ],
    }


@pytest.fixture
def retention_store(tmp_path: Path, monkeypatch):
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="penguin")
    source = tmp_path / "source"
    source.mkdir()
    (source / "stable.txt").write_text("stable\n")
    (source / "changing.bin").write_bytes(b"old" * 2048)
    old = publish_packed_snapshot(
        cold, "prices", source, loose_file_threshold_bytes=1024, pack_buckets=2
    )

    backup_cfg = BackupConfig(
        source=cold,
        destination=tmp_path / "d/stockagent-backup/packed",
        mount_point=tmp_path / "d",
        mount_source="D:\\",
        volume_id="test-volume",
        state_dir=tmp_path / "backup-state",
        reserve_bytes=0,
    )
    backup_cfg.mount_point.mkdir()
    backup = PackedBackup(backup_cfg, guard=TestGuard())
    assert backup.run_once()["state"] == "up_to_date"

    (source / "changing.bin").write_bytes(b"new" * 2048)
    current = publish_packed_snapshot(
        cold, "prices", source, loose_file_threshold_bytes=1024, pack_buckets=2
    )
    assert backup.run_once()["state"] == "up_to_date"
    backup.close()

    backup_config_path = tmp_path / "packed_backup.json"
    backup_config_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": str(backup_cfg.source),
                "destination": str(backup_cfg.destination),
                "mount_point": str(backup_cfg.mount_point),
                "mount_source": backup_cfg.mount_source,
                "volume_id": backup_cfg.volume_id,
                "state_dir": str(backup_cfg.state_dir),
                "authority_node_id": backup_cfg.authority_node_id,
                "reserve_bytes": 0,
                "batch_objects": 32,
                "poll_seconds": 1,
                "checksum_recheck_days": 30,
            }
        )
    )
    cfg = RetentionConfig(
        sync_root=cold,
        archive_root=backup_cfg.destination,
        materialized_root=tmp_path / "materialized",
        backup_config=backup_config_path,
        state_dir=tmp_path / "retention-state",
        folder_id="stockagent-packed",
        required_peer_names=("lab203", "vastai1T"),
        grace_hours=0,
    )
    monkeypatch.setattr(module, "VolumeGuard", TestGuard)
    monkeypatch.setattr(module, "process_references", lambda _root: [])
    yield cfg, old, current


def test_rolling_retention_keeps_current_on_c_and_old_release_on_d(retention_store):
    cfg, old, current = retention_store
    plan = build_plan(cfg, now_ns=time.time_ns() + 1_000_000_000)
    assert not plan["blockers"]
    assert plan["candidate_manifests"] == 1
    assert plan["candidate_objects"] >= 2
    assert plan["reclaimable_bytes"] > 0
    assert all(item["archive_proven"] for item in plan["entries"])

    result = apply_plan(cfg, plan["plan_fingerprint"], peer_proof=_peer_proof())
    assert result["removed_manifests"] == 1
    assert result["removed_objects"] == plan["candidate_objects"]
    verify_packed_snapshot(cfg.sync_root, resolve_latest_packed(cfg.sync_root, "prices"))
    verify_packed_snapshot(
        cfg.archive_root,
        resolve_packed_snapshot_id(
            cfg.archive_root, "prices", old.manifest["snapshot_id"]
        ),
    )
    assert not old.manifest_path.exists()
    assert current.manifest_path.exists()


def test_pin_keeps_an_old_release_and_its_objects(retention_store):
    cfg, old, _current = retention_store
    pins = cfg.materialized_root / "pins"
    pins.mkdir(parents=True)
    (pins / "prices.pin.json").write_text(
        json.dumps({"snapshot_id": old.manifest["snapshot_id"]})
    )
    plan = build_plan(cfg, now_ns=time.time_ns() + 1_000_000_000)
    assert plan["protected_release_count"] == 2
    assert plan["candidate_manifests"] == 0
    assert plan["candidate_objects"] == 0


def test_missing_d_object_and_stale_peer_proof_fail_closed(retention_store):
    cfg, old, _current = retention_store
    old_blob = next(
        item for item in old.manifest["archive"]["objects"] if item["kind"] == "blob"
    )
    (cfg.archive_root / old_blob["relpath"]).unlink()
    plan = build_plan(cfg, now_ns=time.time_ns() + 1_000_000_000)
    assert "D archive proof incomplete" in plan["blockers"]

    with pytest.raises(SnapshotError, match="peer convergence proof"):
        apply_plan(cfg, plan["plan_fingerprint"], peer_proof=_peer_proof(ok=False))


def test_d_receipts_survive_device_number_change_but_not_file_change(retention_store):
    cfg, _old, _current = retention_store
    plan = build_plan(cfg, now_ns=time.time_ns() + 1_000_000_000)
    object_path = next(item["path"] for item in plan["entries"] if item["kind"] == "object")
    db_path = BackupConfig.load(cfg.backup_config).state_dir / "verified.sqlite3"
    with sqlite3.connect(db_path) as connection:
        source_json, target_json = connection.execute(
            "SELECT source_sig,target_sig FROM verified WHERE path=?", (object_path,)
        ).fetchone()
        old_source = json.loads(source_json)
        old_target = json.loads(target_json)
        old_source[0] += 1
        old_target[0] += 1
        connection.execute(
            "UPDATE verified SET source_sig=?,target_sig=? WHERE path=?",
            (json.dumps(old_source), json.dumps(old_target), object_path),
        )
    remounted = build_plan(cfg, now_ns=time.time_ns() + 1_000_000_000)
    assert not remounted["blockers"]

    old_target[4] += 1
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "UPDATE verified SET target_sig=? WHERE path=?",
            (json.dumps(old_target), object_path),
        )
    changed = build_plan(cfg, now_ns=time.time_ns() + 1_000_000_000)
    assert "D archive proof incomplete" in changed["blockers"]


def test_conflict_file_blocks_retention(retention_store):
    cfg, _old, _current = retention_store
    conflict = cfg.sync_root / "heads/prices/penguin.sync-conflict-20260913.json"
    conflict.write_text("{}")
    plan = build_plan(cfg, now_ns=time.time_ns() + 1_000_000_000)
    assert "Syncthing conflict files require audit" in plan["blockers"]
    assert conflict.relative_to(cfg.sync_root).as_posix() in plan["conflict_files"]
