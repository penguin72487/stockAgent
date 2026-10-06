from dataclasses import replace
import fcntl
import os
from pathlib import Path
import shutil
import threading
import time
from types import SimpleNamespace

import pytest

from scripts import return_remote_legacy_archives as cli
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import LegacyArchiveSpec


def test_private_staging_proceeds_while_another_publication_owns_lock(tmp_path, monkeypatch):
    original = tmp_path / "remote/markets/example"
    original.mkdir(parents=True)
    (original / "original.bin").write_bytes(b"original source")
    os.utime(original / "original.bin", (1_000_000_000, 1_000_000_000))
    before = cli.metadata_tree(original)
    before["process_references"] = []
    row = {**{key: before[key] for key in ("fingerprint", "files", "logical_bytes")},
           "relative_root": "markets/example", "newest_mtime_ns": 1}
    state = tmp_path / "state"
    state.mkdir()
    args = SimpleNamespace(state_root=state, sync_root=tmp_path, transfer_compression="none",
                           ssh_target="unused", ssh_port=22, identity_file=tmp_path / "unused")
    spec = LegacyArchiveSpec("legacy-example", "markets/example", 7, state / "encoding", 12)
    owner_path = tmp_path / "owner.lock"
    staged, committed = threading.Event(), threading.Event()
    errors = []
    monkeypatch.setattr(cli, "PUBLICATION_OWNER", owner_path)
    monkeypatch.setattr(cli, "remote", lambda *a: before)
    monkeypatch.setattr(cli, "_check_d_primary_mount", lambda *a: None)
    monkeypatch.setattr(cli, "admit_workspace", lambda *a: None)
    def copy(candidate, *, destination_artifact_root, **kwargs):
        shutil.copytree(original, destination_artifact_root / "markets/example")
    monkeypatch.setattr(cli, "_rsync_candidate", copy)
    def encode(*args, **kwargs):
        with owner_path.open("a") as separate:
            with pytest.raises(BlockingIOError):
                fcntl.flock(separate, fcntl.LOCK_EX | fcntl.LOCK_NB)
        staged.set()
    monkeypatch.setattr(cli, "prepare_archive", encode)
    def commit(*args):
        with cli.publication_owner(args[0], args[2], args[3]):
            with owner_path.open("a") as separate:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(separate, fcntl.LOCK_EX | fcntl.LOCK_NB)
            committed.set()
    monkeypatch.setattr(cli, "commit_prepared_archive", commit)
    def worker():
        try:
            cli.archive_one(args, {"reserve_bytes": 0}, row, spec)
        except BaseException as error:
            errors.append(error)
    with owner_path.open("a") as other:
        fcntl.flock(other, fcntl.LOCK_EX)
        thread = threading.Thread(target=worker)
        thread.start()
        try:
            assert staged.wait(3), "private encoding was blocked by unrelated cold publication"
            assert not committed.is_set()
        finally:
            fcntl.flock(other, fcntl.LOCK_UN)
    thread.join(3)
    assert not thread.is_alive() and not errors and committed.is_set()


def test_source_change_during_owner_wait_blocks_publication(tmp_path, monkeypatch):
    args = SimpleNamespace(sync_root=tmp_path, state_root=tmp_path)
    row = {"relative_root": "markets/example"}
    spec = LegacyArchiveSpec("legacy-example", row["relative_root"], 7, tmp_path)
    before = {"fingerprint": "original", "process_references": []}
    monkeypatch.setattr(cli, "_check_d_primary_mount", lambda *a: None)
    monkeypatch.setattr(cli, "PUBLICATION_OWNER", tmp_path / "owner.lock")
    monkeypatch.setattr(cli, "remote", lambda *a: {"fingerprint": "changed", "process_references": []})
    published = []
    monkeypatch.setattr(cli, "publish_archive", lambda *a, **kw: published.append(True))
    with pytest.raises(SnapshotError, match="changed while"):
        cli.commit_prepared_archive(args, {}, row, spec, before, tmp_path, tmp_path, {}, tmp_path)
    assert not published


def test_full_recovery_and_peer_wait_release_owner_but_exact_retirement_holds_it(tmp_path, monkeypatch):
    owner_path = tmp_path / "owner.lock"
    monkeypatch.setattr(cli, "PUBLICATION_OWNER", owner_path)
    args = SimpleNamespace(sync_root=tmp_path, state_root=tmp_path)
    row = {"relative_root": "markets/example"}
    spec = LegacyArchiveSpec("legacy-example", row["relative_root"], 7, tmp_path)
    before = {"fingerprint": "exact-original", "process_references": []}
    result = {"dataset": spec.dataset, "snapshot_id": "exact-release", "manifest_sha256": "a" * 64}
    timestamp = time.time() - 5
    proof = {**result, "manifest": {"files": []}, "verified_at_epoch": timestamp}
    resolved = SimpleNamespace(manifest={"snapshot_id": "exact-release", "metadata": {"legacy_manifest_sha256": "b" * 64}},
                               manifest_sha256="a" * 64)
    def lock_held():
        with owner_path.open("a") as separate:
            try:
                fcntl.flock(separate, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
        return False
    published, recovered, observations = [], [], []
    def publish(*args, **kwargs):
        assert lock_held()
        published.append(True)
        return result
    def recovery(*args, **kwargs):
        assert not lock_held()
        recovered.append(True)
        return proof
    class ReachedExactRetirement(Exception):
        pass
    def remote(args, request):
        if request["action"] == "observe":
            observations.append(lock_held())
            return before
        assert lock_held() and request["action"] == "retire"
        assert request["ack"]["verified_at_epoch"] == timestamp
        assert request["ack"]["snapshot_id"] == "exact-release"
        raise ReachedExactRetirement()
    monkeypatch.setattr(cli, "remote", remote)
    monkeypatch.setattr(cli, "publish_archive", publish)
    monkeypatch.setattr(cli, "verify_cold_archive", recovery)
    monkeypatch.setattr(cli, "resolve_latest_packed", lambda *_: resolved)
    monkeypatch.setattr(cli, "credentials", lambda: ("unused", "unused"))
    monkeypatch.setattr(cli, "scan_after_publish", lambda *a, **kw: pytest.fail("peer scan held mutation owner") if lock_held() else None)
    monkeypatch.setattr(cli, "_convergence", lambda *_: {"ok": True})
    monkeypatch.setattr(cli, "_check_d_primary_mount", lambda *_: None)
    with pytest.raises(ReachedExactRetirement):
        cli.commit_prepared_archive(args, {"retire_verified_source": True}, row, spec, before, tmp_path, tmp_path, {}, tmp_path)
    assert published == recovered == [True]
    assert observations == [True, False, True]
    assert not lock_held()


def test_recovery_cannot_authorize_another_committed_release():
    result = {"dataset": "one", "snapshot_id": "one-release", "manifest_sha256": "a" * 64}
    resolved = SimpleNamespace(manifest={"snapshot_id": "one-release"}, manifest_sha256="a" * 64)
    with pytest.raises(SnapshotError, match="exact committed"):
        cli.exact_recovery(result, {**result, "snapshot_id": "other"}, resolved)
    with pytest.raises(SnapshotError, match="head changed"):
        cli.exact_recovery(result, result, SimpleNamespace(manifest={"snapshot_id": "other"}, manifest_sha256="b" * 64))
