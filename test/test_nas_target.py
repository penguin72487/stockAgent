import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.data_sync import nas_target as module
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.nas_target import NasTarget


@pytest.fixture
def target(tmp_path, monkeypatch):
    mount = tmp_path / "nas"
    (mount / "students/user").mkdir(parents=True)
    profile = {"schema_version": 1, "automatic_pruning": False, "reserve_bytes": 1024,
               "nas": {"host": "140.127.208.143", "share": "Lab203", "relative_directory": "students/user",
                       "repository_directory": "stockagent-restic", "mount_point": str(mount)}}
    config = tmp_path / "config.json"
    config.write_text(json.dumps(profile))
    monkeypatch.setattr(module, "mounted_volume", lambda _: ("101", "cifs", "//140.127.208.143/Lab203"))
    monkeypatch.setattr(module.shutil, "disk_usage", lambda _: SimpleNamespace(free=128 * 1024**2))
    return NasTarget(config)


def test_lost_mount_never_falls_back_to_local_directory(target, monkeypatch):
    def missing(_):
        raise SnapshotError("not mounted")
    monkeypatch.setattr(module, "mounted_volume", missing)
    with pytest.raises(SnapshotError, match="not mounted"):
        target.probe(size_mib=1, repeats=1)
    assert list(target.user_directory.iterdir()) == []


@pytest.mark.parametrize("observed", [("101", "ext4", "/dev/sdb"), ("101", "cifs", "//other/Lab203"), ("101", "cifs", "//140.127.208.143/OtherShare")])
def test_wrong_filesystem_server_or_share_is_rejected(target, monkeypatch, observed):
    monkeypatch.setattr(module, "mounted_volume", lambda _: observed)
    with pytest.raises(SnapshotError, match="exact configured SMB"):
        target.check()


def test_low_capacity_prevents_probe_before_writes(target, monkeypatch):
    monkeypatch.setattr(module.shutil, "disk_usage", lambda _: SimpleNamespace(free=1024))
    with pytest.raises(SnapshotError, match="capacity"):
        target.probe(size_mib=1, repeats=1)
    assert list(target.user_directory.iterdir()) == []


def test_remounted_target_cannot_reuse_previous_proof(target, monkeypatch):
    before = target.check()
    monkeypatch.setattr(module, "mounted_volume", lambda _: ("102", "cifs", "//140.127.208.143/Lab203"))
    with pytest.raises(SnapshotError, match="identity changed"):
        target.unchanged(before)


def test_bounded_probe_verifies_bytes_and_only_removes_own_files(target):
    unrelated = target.user_directory / "do-not-touch.txt"
    unrelated.write_text("another file")
    result = target.probe(size_mib=1, repeats=2)
    assert len(result["samples"]) == 2
    assert all(v["verified"] and v["bytes"] == 1024**2 for v in result["samples"])
    assert result["durable_off_host_backup_verified"] is False
    assert list(target.user_directory.iterdir()) == [unrelated]


def test_nas_user_directory_must_not_be_redirected(target, tmp_path):
    user = target.user_directory
    user.rmdir()
    user.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(SnapshotError, match="symlink"):
        target.check()
