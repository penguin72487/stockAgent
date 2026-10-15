from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import restore_missing_crypto_workspace as recovery


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    live = tmp_path / "live"
    cold = tmp_path / "cold"
    materialized = tmp_path / "materialized"
    for path in (root, live, cold):
        path.mkdir()
    for name, path in (("ROOT", root), ("LIVE_ROOT", live), ("COLD_ROOT", cold),
                       ("MATERIALIZED_ROOT", materialized)):
        monkeypatch.setattr(recovery, name, path)
    snapshot = "bybit-fixed-original"
    (root / "data_bybit").symlink_to(materialized / "bybit" / snapshot)
    resolved = SimpleNamespace(manifest_sha256="fixed-sha", manifest={
        "source": {"logical_bytes": 5, "files": 1, "portable_fingerprint_sha256": "proof"}})
    monkeypatch.setattr(recovery, "resolve_packed_snapshot_id", lambda *args: resolved)
    monkeypatch.setattr(recovery.shutil, "disk_usage", lambda *args: SimpleNamespace(free=100 * 1024**3))
    calls = []

    def fetch(cold_root, scratch, selected):
        calls.append("full-canonical-fetch")
        target = scratch / "bybit" / snapshot
        target.mkdir(parents=True)
        (target / "original.parquet").write_bytes(b"value")
        return target

    monkeypatch.setattr(recovery, "fetch_packed_snapshot", fetch)
    return {"root": root, "live": live, "materialized": materialized, "calls": calls,
            "kwargs": {"dataset": "bybit", "snapshot_id": snapshot,
                       "manifest_sha256": "fixed-sha", "receipt_path": tmp_path / "receipt.json"}}


def test_recovery_is_explicit_dry_run_and_never_creates_a_new_source(workspace):
    receipt = recovery.recover(**workspace["kwargs"])
    assert receipt["state"] == "planned" and receipt["historical_baseline_only"]
    assert workspace["calls"] == []
    assert not (workspace["live"] / "data_bybit").exists()
    assert not (workspace["root"] / "data_bybit").exists()


def test_full_recovery_moves_exact_tree_to_mutable_root_before_link_change(workspace):
    old_link = recovery.link_identity(workspace["root"] / "data_bybit")
    receipt = recovery.recover(**workspace["kwargs"], apply=True)
    assert receipt["state"] == "restored_mutable_workspace"
    assert receipt["original_link"] == old_link
    assert receipt["full_canonical_recovery_verified"] and receipt["workspace_installed"]
    assert workspace["calls"] == ["full-canonical-fetch"]
    assert (workspace["root"] / "data_bybit").resolve() == workspace["live"] / "data_bybit"
    assert (workspace["root"] / "data_bybit" / "original.parquet").read_bytes() == b"value"
    assert not receipt["cold_objects_modified"] and not receipt["partial_attempts_deleted"]


def test_recovery_refuses_already_available_or_unrelated_link(workspace):
    source = workspace["root"] / "data_bybit"
    source.unlink()
    source.symlink_to(workspace["live"] / "unrelated")
    with pytest.raises(recovery.SnapshotError, match="expected missing"):
        recovery.recover(**workspace["kwargs"], apply=True)
    assert workspace["calls"] == []


def test_recovery_refuses_wrong_frozen_manifest_or_low_capacity(workspace, monkeypatch):
    with pytest.raises(recovery.SnapshotError, match="manifest SHA"):
        recovery.recover(**{**workspace["kwargs"], "manifest_sha256": "other"}, apply=True)
    monkeypatch.setattr(recovery.shutil, "disk_usage", lambda *args: SimpleNamespace(free=recovery.RESERVE_BYTES))
    with pytest.raises(recovery.SnapshotError, match="32 GiB"):
        recovery.recover(**workspace["kwargs"], apply=True)
    assert workspace["calls"] == []


def test_recovery_refuses_an_existing_unowned_workspace(workspace):
    (workspace["live"] / "data_bybit").mkdir()
    with pytest.raises(recovery.SnapshotError, match="existing mutable"):
        recovery.recover(**workspace["kwargs"], apply=True)
    assert workspace["calls"] == []


def test_failed_recovery_keeps_all_staging_and_original_link(workspace, monkeypatch):
    before = recovery.link_identity(workspace["root"] / "data_bybit")
    def failed(cold, scratch, selected):
        (scratch / "partial-original").write_bytes(b"keep")
        raise RuntimeError("full source verification failed")
    monkeypatch.setattr(recovery, "fetch_packed_snapshot", failed)
    with pytest.raises(RuntimeError, match="verification failed"):
        recovery.recover(**workspace["kwargs"], apply=True)
    assert recovery.link_identity(workspace["root"] / "data_bybit") == before
    assert list(workspace["live"].glob(".bybit-source-recovery-*/partial-original"))


def test_interrupted_install_requires_full_verify_again_not_a_second_fetch(workspace, monkeypatch):
    original_linker = recovery._atomic_symlink
    monkeypatch.setattr(recovery, "_atomic_symlink", lambda *args: (_ for _ in ()).throw(RuntimeError("interrupted")))
    with pytest.raises(RuntimeError, match="interrupted"):
        recovery.recover(**workspace["kwargs"], apply=True)
    verified = []
    monkeypatch.setattr(recovery, "verify_packed_snapshot", lambda *args, **kwargs: verified.append(kwargs))
    monkeypatch.setattr(recovery, "_atomic_symlink", original_linker)
    receipt = recovery.recover(**workspace["kwargs"], apply=True)
    assert receipt["state"] == "restored_mutable_workspace" and receipt["resumed_verified_install"]
    assert workspace["calls"] == ["full-canonical-fetch"]
    assert verified == [{"materialized_path": workspace["live"] / "data_bybit"}]
