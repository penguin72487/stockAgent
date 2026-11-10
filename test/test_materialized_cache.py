from __future__ import annotations

import errno
import json
import os
import time
from pathlib import Path

import pytest

from stockagent.data_sync import materialized_cache as materialized_cache_module
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import (
    evict_materialized_snapshots,
    materialized_cache_status,
    use_materialized_snapshot,
)
from stockagent.data_sync.packed_snapshots import (
    initialize_packed_layout,
    publish_packed_snapshot,
    resolve_packed_snapshot_id,
)


DAY_NS = 86_400 * 1_000_000_000
BASE_NS = time.time_ns()


@pytest.fixture(autouse=True)
def fresh_test_clock(monkeypatch):
    # A long suite can collect this module more than the allowed HLC skew
    # before executing it. Each test needs its own clock, while production's
    # future-head rejection and the test's relative lease ages stay intact.
    monkeypatch.setitem(globals(), "BASE_NS", time.time_ns())


def _release(tmp_path: Path, *, content: str = "first") -> tuple[Path, str]:
    source = tmp_path / "source"
    source.mkdir(exist_ok=True)
    (source / "prices.csv").write_text(content, encoding="utf-8")
    sync_root = tmp_path / "cold"
    if not sync_root.exists():
        initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(sync_root, "prices", source, pack_buckets=2)
    return sync_root, str(resolved.manifest["snapshot_id"])


def test_use_creates_verified_hot_lease_and_stable_link(tmp_path: Path) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    user_link = tmp_path / "data_prices"

    lease = use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        ttl_days=7,
        links=[user_link],
        now_ns=BASE_NS + 10 * DAY_NS,
    )

    target = Path(lease["target"])
    assert target.name == snapshot_id
    assert (target / "prices.csv").read_text(encoding="utf-8") == "first"
    assert user_link.resolve() == target
    assert (hot_root / "current" / "prices").resolve() == target
    assert lease["expires_ns"] == BASE_NS + 17 * DAY_NS
    assert json.loads(
        (hot_root / ".cache-state" / "leases" / "prices" / f"{snapshot_id}.json")
        .read_text(encoding="utf-8")
    )["state"] == "hot"


@pytest.mark.parametrize("collision", ["file", "directory", "other_link", "dangling_link", "parent_link"])
def test_recovery_preserves_existing_names_before_any_materialization(tmp_path, monkeypatch, collision):
    sync_root, snapshot_id = _release(tmp_path)
    hot_root, alias = tmp_path / "hot", tmp_path / "legacy/raw"
    alias.parent.mkdir()
    if collision == "file":
        alias.write_text("unique source")
    elif collision == "directory":
        alias.mkdir()
    elif collision == "parent_link":
        alias.parent.rmdir()
        (tmp_path / "redirected").mkdir()
        alias.parent.symlink_to(tmp_path / "redirected")
    else:
        target = tmp_path / "other"
        if collision == "other_link":
            target.mkdir()
        alias.symlink_to(target)
    monkeypatch.setattr(materialized_cache_module, "fetch_packed_snapshot",
                        lambda *_: pytest.fail("collision must precede source reads"))
    with pytest.raises(SnapshotError, match="existing cache link|redirected parent"):
        use_materialized_snapshot(sync_root, hot_root, "prices", snapshot_id=snapshot_id,
            links=[alias], preserve_existing_links=True)
    if collision == "file":
        assert alias.read_text() == "unique source"
    elif collision in {"other_link", "dangling_link"}:
        assert alias.readlink() == target
    assert not (hot_root / "prices" / snapshot_id).exists()


def test_recovery_link_race_cannot_replace_competing_name(tmp_path, monkeypatch):
    sync_root, snapshot_id = _release(tmp_path)
    hot_root, alias = tmp_path / "hot", tmp_path / "original"
    original = materialized_cache_module.fetch_packed_snapshot
    def competing_fetch(*args):
        target = original(*args)
        alias.symlink_to(tmp_path / "competitor")
        return target
    monkeypatch.setattr(materialized_cache_module, "fetch_packed_snapshot", competing_fetch)
    with pytest.raises(SnapshotError, match="existing cache link"):
        use_materialized_snapshot(sync_root, hot_root, "prices", snapshot_id=snapshot_id,
            links=[alias], preserve_existing_links=True)
    assert alias.readlink() == tmp_path / "competitor"
    assert not (hot_root / ".cache-state/leases/prices" / f"{snapshot_id}.json").exists()
    assert (hot_root / "prices" / snapshot_id / "prices.csv").read_text() == "first"


def test_subtree_alias_uses_one_release_and_same_lease_and_gc(tmp_path, monkeypatch):
    source, cold, hot = tmp_path / "raw", tmp_path / "cold", tmp_path / "hot"
    (source / "repair/inputs").mkdir(parents=True)
    (source / "repair/inputs/values.csv").write_text("-2,0,3\n")
    initialize_packed_layout(cold, node_id="node-a")
    release = publish_packed_snapshot(cold, "financial", source, pack_buckets=2)
    aliases = {tmp_path / "original/delta": "repair/inputs"}
    arguments = dict(snapshot_id=release.manifest["snapshot_id"], subtree_links=aliases,
                     preserve_existing_links=True)
    first = use_materialized_snapshot(cold, hot, "financial", now_ns=BASE_NS, **arguments)
    target = Path(first["target"])
    alias = next(iter(aliases))
    member = alias / "values.csv"
    identity = member.stat()
    assert member.read_bytes() == b"-2,0,3\n"
    assert first["schema_version"] == 2
    assert first["link_subtrees"] == {str(alias): "repair/inputs"}
    assert member.samefile(target / "repair/inputs/values.csv")
    second = use_materialized_snapshot(cold, hot, "financial", now_ns=BASE_NS + DAY_NS,
                                      verify_existing=True, **arguments)
    assert second["verification"] == "full" and member.stat().st_ino == identity.st_ino
    assert second["expires_ns"] == BASE_NS + 8 * DAY_NS
    monkeypatch.setattr(materialized_cache_module, "process_references", lambda *_: [])
    result = evict_materialized_snapshots(cold, hot, now_ns=BASE_NS + 9 * DAY_NS)
    assert result["evicted"] == 1
    assert not alias.is_symlink() and not target.exists()
    assert release.manifest_path.exists()  # Only the replaceable hot view retires.


@pytest.mark.parametrize("subtree", ["../outside", "/absolute", ".", "a//b", "a\\b", "prices.csv", "missing"])
def test_invalid_subtree_alias_never_publishes_a_lease(tmp_path, subtree):
    sync_root, snapshot_id = _release(tmp_path)
    hot, alias = tmp_path / "hot", tmp_path / "original"
    with pytest.raises(SnapshotError):
        use_materialized_snapshot(sync_root, hot, "prices", snapshot_id=snapshot_id,
            subtree_links={alias: subtree}, preserve_existing_links=True)
    assert not alias.is_symlink()
    assert not (hot / ".cache-state/leases/prices" / f"{snapshot_id}.json").exists()


def test_malformed_subtree_lease_cannot_admit_eviction(tmp_path, monkeypatch):
    sync_root, snapshot_id = _release(tmp_path)
    hot = tmp_path / "hot"
    lease = use_materialized_snapshot(sync_root, hot, "prices", snapshot_id=snapshot_id, now_ns=BASE_NS)
    lease.update(schema_version=2, link_subtrees={str(tmp_path / "foreign"): "../outside"})
    path = hot / ".cache-state/leases/prices" / f"{snapshot_id}.json"
    path.write_text(json.dumps(lease))
    monkeypatch.setattr(materialized_cache_module, "process_references", lambda *_: [])
    with pytest.raises(SnapshotError):
        evict_materialized_snapshots(sync_root, hot, now_ns=BASE_NS + 8 * DAY_NS)
    assert Path(lease["target"]).is_dir()


def test_expired_gc_requires_exact_cold_reconstruction_not_just_object_presence(tmp_path):
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    lease = use_materialized_snapshot(sync_root, hot_root, "prices", now_ns=BASE_NS)
    resolved = resolve_packed_snapshot_id(sync_root, "prices", snapshot_id)
    item = resolved.manifest["archive"]["objects"][0]
    cold = sync_root / item["relpath"]
    damaged = bytearray(cold.read_bytes())
    damaged[len(damaged) // 2] ^= 1
    cold.write_bytes(damaged)
    result = evict_materialized_snapshots(sync_root, hot_root, now_ns=BASE_NS + 8 * DAY_NS)
    assert result["evicted"] == 0
    assert result["results"][0]["reason"].startswith("cold-recovery-failed:")
    assert Path(lease["target"]).is_dir()


@pytest.mark.parametrize("change", ["content", "new_file"])
def test_gc_never_discards_unique_hot_changes_even_with_valid_cold(tmp_path, change):
    sync_root, _ = _release(tmp_path)
    hot_root = tmp_path / "hot"
    lease = use_materialized_snapshot(sync_root, hot_root, "prices", now_ns=BASE_NS)
    target = Path(lease["target"])
    changed = target / ("prices.csv" if change == "content" else "unique.csv")
    changed.write_text("local source values")
    result = evict_materialized_snapshots(sync_root, hot_root, now_ns=BASE_NS + 8 * DAY_NS)
    assert result["evicted"] == 0
    assert changed.read_text() == "local source values"
    assert result["results"][0]["reason"].startswith("cold-recovery-failed:")


def test_gc_observes_hot_mutation_after_recovery_before_unlink(tmp_path, monkeypatch):
    sync_root, _ = _release(tmp_path)
    hot_root = tmp_path / "hot"
    lease = use_materialized_snapshot(sync_root, hot_root, "prices", now_ns=BASE_NS)
    target = Path(lease["target"])
    calls = 0
    def observe(_target):
        nonlocal calls
        calls += 1
        if calls == 2:
            (target / "unique.csv").write_text("keep new observation")
        return []
    monkeypatch.setattr(materialized_cache_module, "process_references", observe)
    result = evict_materialized_snapshots(sync_root, hot_root, now_ns=BASE_NS + 8 * DAY_NS)
    assert result["evicted"] == 0
    assert result["results"][0]["reason"] == "hot-tree-changed-during-recovery"
    assert (target / "unique.csv").is_file()


def test_invalid_pin_is_not_silently_ignored_during_gc(tmp_path):
    sync_root, _ = _release(tmp_path)
    hot_root = tmp_path / "hot"
    lease = use_materialized_snapshot(sync_root, hot_root, "prices", now_ns=BASE_NS)
    (hot_root / "protected.pin.json").write_text('{"manifest":')
    with pytest.raises(SnapshotError, match="cannot read cache metadata"):
        evict_materialized_snapshots(sync_root, hot_root, now_ns=BASE_NS + 8 * DAY_NS)
    assert Path(lease["target"]).is_dir()


def test_gc_rechecks_pin_after_slow_independent_recovery(tmp_path, monkeypatch):
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    lease = use_materialized_snapshot(sync_root, hot_root, "prices", now_ns=BASE_NS)
    def recovery(*args, **kwargs):
        assert kwargs["reconstruct_all"] is True
        (hot_root / "late.pin.json").write_text(json.dumps({"manifest": {"snapshot_id": snapshot_id}}))
        return {}
    monkeypatch.setattr(materialized_cache_module, "verify_packed_snapshot", recovery)
    result = evict_materialized_snapshots(sync_root, hot_root, now_ns=BASE_NS + 8 * DAY_NS)
    assert result["results"][0]["reason"] == "pinned" and result["evicted"] == 0
    assert Path(lease["target"]).is_dir()


def test_reuse_renews_lease_without_rewriting_materialized_file(
    tmp_path: Path,
) -> None:
    sync_root, _ = _release(tmp_path)
    hot_root = tmp_path / "hot"
    first = use_materialized_snapshot(
        sync_root, hot_root, "prices", now_ns=BASE_NS + 10 * DAY_NS
    )
    file_path = Path(first["target"]) / "prices.csv"
    inode = file_path.stat().st_ino

    second = use_materialized_snapshot(
        sync_root, hot_root, "prices", now_ns=BASE_NS + 12 * DAY_NS
    )

    assert second["verification"] == "ready-marker"
    assert second["last_used_ns"] == BASE_NS + 12 * DAY_NS
    assert file_path.stat().st_ino == inode


def test_noop_publish_reuses_inventory_snapshot_id(tmp_path: Path) -> None:
    sync_root, first_id = _release(tmp_path, content="unchanged")

    _, second_id = _release(tmp_path, content="unchanged")

    assert second_id == first_id


def test_verify_quarantines_and_recovers_mutated_hot_tree(tmp_path: Path) -> None:
    sync_root, _ = _release(tmp_path, content="authoritative")
    hot_root = tmp_path / "hot"
    first = use_materialized_snapshot(sync_root, hot_root, "prices")
    target = Path(first["target"])
    (target / "prices.csv").write_text("mutated", encoding="utf-8")

    recovered = use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        verify_existing=True,
    )

    assert (Path(recovered["target"]) / "prices.csv").read_text() == "authoritative"
    quarantine = recovered["recovered_corrupt_materialization"]
    assert (Path(quarantine["tree"]) / "prices.csv").read_text() == "mutated"


def test_expired_gc_removes_only_hot_copy_and_can_refetch(tmp_path: Path) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    lease = use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        ttl_days=7,
        now_ns=BASE_NS + 10 * DAY_NS,
    )
    target = Path(lease["target"])

    before = evict_materialized_snapshots(
        sync_root, hot_root, dry_run=True, now_ns=BASE_NS + 18 * DAY_NS
    )
    assert before["would_evict"] == 1
    assert target.is_dir()

    result = evict_materialized_snapshots(
        sync_root, hot_root, now_ns=BASE_NS + 18 * DAY_NS
    )
    assert result["evicted"] == 1
    assert not target.exists()
    assert not (hot_root / "current" / "prices").exists()
    assert not (hot_root / "current" / "prices").is_symlink()
    assert result["results"][0]["removed_links"] == [
        str(hot_root / "current" / "prices")
    ]
    assert (sync_root / "manifests" / "prices" / f"{snapshot_id}.json").is_file()

    renewed = use_materialized_snapshot(
        sync_root, hot_root, "prices", now_ns=BASE_NS + 19 * DAY_NS
    )
    assert Path(renewed["target"]).is_dir()


@pytest.mark.parametrize("dry_run", [False, True])
def test_incomplete_reference_scan_retains_expired_payload_lease_and_link(tmp_path, monkeypatch, dry_run):
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    lease = use_materialized_snapshot(sync_root, hot_root, "prices", ttl_days=7,
                                      now_ns=BASE_NS + 10 * DAY_NS)
    lease_path = hot_root / ".cache-state/leases/prices" / f"{snapshot_id}.json"
    before = lease_path.read_bytes()
    target = Path(lease["target"])

    def incomplete(*args, **kwargs):
        raise SnapshotError("incomplete process-reference scan: injected permission error")

    monkeypatch.setattr(materialized_cache_module, "process_references", incomplete)
    with pytest.raises(SnapshotError, match="incomplete process-reference scan"):
        evict_materialized_snapshots(sync_root, hot_root, dry_run=dry_run,
                                     now_ns=BASE_NS + 18 * DAY_NS)
    assert lease_path.read_bytes() == before
    assert (target / "prices.csv").read_text() == "first"
    assert (hot_root / "current/prices").resolve(strict=True) == target


def test_edge_gc_requires_valid_external_cold_proof_for_missing_payload(
    tmp_path: Path,
) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    lease = use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        ttl_days=7,
        now_ns=BASE_NS + 10 * DAY_NS,
    )
    resolved = resolve_packed_snapshot_id(sync_root, "prices", snapshot_id)
    for item in resolved.manifest["archive"]["objects"]:
        (sync_root / item["relpath"]).unlink()

    conservative = evict_materialized_snapshots(
        sync_root, hot_root, now_ns=BASE_NS + 18 * DAY_NS
    )
    assert conservative["evicted"] == 0
    assert conservative["results"][0]["reason"].startswith(
        "cold-release-incomplete:"
    )

    proof = {
        "kind": "syncthing-durable-peer",
        "validated": True,
        "peer_name": "durable-a",
        "completion": 100,
        "remote_state": "valid",
    }
    edge = evict_materialized_snapshots(
        sync_root,
        hot_root,
        now_ns=BASE_NS + 18 * DAY_NS,
        external_cold_proof=proof,
    )
    assert edge["evicted"] == 1
    assert edge["results"][0]["cold_proof"] == proof
    assert not Path(lease["target"]).exists()


def test_edge_status_reads_metadata_without_local_payload(tmp_path: Path) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    resolved = resolve_packed_snapshot_id(sync_root, "prices", snapshot_id)
    for item in resolved.manifest["archive"]["objects"]:
        (sync_root / item["relpath"]).unlink()

    status = materialized_cache_status(
        sync_root,
        tmp_path / "hot",
        dataset="prices",
        require_objects=False,
    )
    assert status["datasets"][0]["cold"]["available"] is True
    assert status["datasets"][0]["cold"]["snapshot_id"] == snapshot_id


def test_edge_gc_clamps_legacy_long_lease_to_seven_days(
    tmp_path: Path, monkeypatch
) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        ttl_days=365,
        now_ns=BASE_NS + 10 * DAY_NS,
    )
    monkeypatch.setattr(
        materialized_cache_module,
        "process_references",
        lambda target: ["pid=123:maps:/hot/prices"],
    )
    proof = {
        "kind": "syncthing-durable-peer",
        "validated": True,
        "peer_name": "durable-a",
        "completion": 100,
        "remote_state": "valid",
    }

    result = evict_materialized_snapshots(
        sync_root,
        hot_root,
        now_ns=BASE_NS + 12 * DAY_NS,
        external_cold_proof=proof,
        max_ttl_days=7,
    )

    assert result["renewed"] == 1
    assert result["results"][0]["ttl_clamped_from_days"] == 365
    lease_path = (
        hot_root
        / ".cache-state"
        / "leases"
        / "prices"
        / f"{snapshot_id}.json"
    )
    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    assert lease["ttl_days"] == 7
    assert lease["expires_ns"] == BASE_NS + 19 * DAY_NS


def test_gc_auto_renews_a_hot_lease_referenced_by_a_process(
    tmp_path: Path, monkeypatch
) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    original = use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        ttl_days=7,
        now_ns=BASE_NS + 10 * DAY_NS,
    )
    evidence = ["pid=123:fd=7:/hot/prices/prices.csv"]
    monkeypatch.setattr(
        materialized_cache_module,
        "process_references",
        lambda target: evidence,
    )

    result = evict_materialized_snapshots(
        sync_root, hot_root, now_ns=BASE_NS + 12 * DAY_NS
    )

    assert result["renewed"] == 1
    assert result["evicted"] == 0
    assert result["results"][0]["reason"] == "in-use-auto-renewed"
    lease_path = (
        hot_root
        / ".cache-state"
        / "leases"
        / "prices"
        / f"{snapshot_id}.json"
    )
    renewed = json.loads(lease_path.read_text(encoding="utf-8"))
    assert renewed["last_used_ns"] == BASE_NS + 12 * DAY_NS
    assert renewed["expires_ns"] == BASE_NS + 19 * DAY_NS
    assert renewed["expires_ns"] > original["expires_ns"]
    assert renewed["auto_renewal_count"] == 1
    assert renewed["auto_renewal_evidence"] == evidence


def test_gc_auto_renew_dry_run_does_not_mutate_the_lease(
    tmp_path: Path, monkeypatch
) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    original = use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        ttl_days=7,
        now_ns=BASE_NS + 10 * DAY_NS,
    )
    monkeypatch.setattr(
        materialized_cache_module,
        "process_references",
        lambda target: ["pid=123:maps:/hot/prices"],
    )

    result = evict_materialized_snapshots(
        sync_root,
        hot_root,
        dry_run=True,
        now_ns=BASE_NS + 18 * DAY_NS,
    )

    assert result["would_renew"] == 1
    assert result["would_evict"] == 0
    lease_path = (
        hot_root
        / ".cache-state"
        / "leases"
        / "prices"
        / f"{snapshot_id}.json"
    )
    unchanged = json.loads(lease_path.read_text(encoding="utf-8"))
    assert unchanged["expires_ns"] == original["expires_ns"]
    assert "auto_renewed_ns" not in unchanged


def test_forced_evict_keeps_but_does_not_renew_an_in_use_tree(
    tmp_path: Path, monkeypatch
) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    original = use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        ttl_days=7,
        now_ns=BASE_NS + 10 * DAY_NS,
    )
    monkeypatch.setattr(
        materialized_cache_module,
        "process_references",
        lambda target: ["pid=123:fd=7:/hot/prices/prices.csv"],
    )

    result = evict_materialized_snapshots(
        sync_root,
        hot_root,
        dataset="prices",
        force=True,
        now_ns=BASE_NS + 18 * DAY_NS,
    )

    assert result["renewed"] == 0
    assert result["kept"] == 1
    assert result["results"][0]["reason"] == "in-use"
    lease_path = (
        hot_root
        / ".cache-state"
        / "leases"
        / "prices"
        / f"{snapshot_id}.json"
    )
    unchanged = json.loads(lease_path.read_text(encoding="utf-8"))
    assert unchanged["expires_ns"] == original["expires_ns"]


def test_gc_keeps_an_in_use_tree_with_an_invalid_ttl(
    tmp_path: Path, monkeypatch
) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        ttl_days=7,
        now_ns=BASE_NS + 10 * DAY_NS,
    )
    lease_path = (
        hot_root
        / ".cache-state"
        / "leases"
        / "prices"
        / f"{snapshot_id}.json"
    )
    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    lease["ttl_days"] = "nan"
    lease_path.write_text(json.dumps(lease), encoding="utf-8")
    monkeypatch.setattr(
        materialized_cache_module,
        "process_references",
        lambda target: ["pid=123:fd=7:/hot/prices/prices.csv"],
    )

    result = evict_materialized_snapshots(
        sync_root, hot_root, now_ns=BASE_NS + 18 * DAY_NS
    )

    assert result["renewed"] == 0
    assert result["kept"] == 1
    assert result["results"][0]["reason"] == "in-use-invalid-lease-ttl"
    assert Path(lease["target"]).is_dir()


def test_pin_blocks_expired_gc(tmp_path: Path) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    lease = use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        ttl_days=7,
        now_ns=BASE_NS + 10 * DAY_NS,
    )
    (hot_root / "prices.pin.json").write_text(
        json.dumps({"manifest": {"snapshot_id": snapshot_id}}),
        encoding="utf-8",
    )

    result = evict_materialized_snapshots(
        sync_root, hot_root, now_ns=BASE_NS + 18 * DAY_NS
    )

    assert result["evicted"] == 0
    assert result["results"][0]["reason"] == "pinned"
    assert Path(lease["target"]).is_dir()


def test_status_distinguishes_current_and_outdated_hot_release(
    tmp_path: Path,
) -> None:
    sync_root, first_id = _release(tmp_path, content="first")
    hot_root = tmp_path / "hot"
    use_materialized_snapshot(
        sync_root,
        hot_root,
        "prices",
        snapshot_id=first_id,
        now_ns=BASE_NS + 10 * DAY_NS,
    )
    _, second_id = _release(tmp_path, content="second")

    status = materialized_cache_status(
        sync_root, hot_root, dataset="prices", now_ns=BASE_NS + 11 * DAY_NS
    )

    row = status["datasets"][0]
    assert row["cold"]["snapshot_id"] == second_id
    assert row["hot"]["snapshot_id"] == first_id
    assert row["hot"]["state"] == "hot-outdated"


def test_status_reports_an_existing_unmanaged_materialization(
    tmp_path: Path,
) -> None:
    sync_root, snapshot_id = _release(tmp_path)
    hot_root = tmp_path / "hot"
    target = hot_root / "prices" / snapshot_id
    target.mkdir(parents=True)
    (target / "manual.txt").write_text("manual", encoding="utf-8")

    status = materialized_cache_status(
        sync_root, hot_root, dataset="prices", now_ns=BASE_NS + DAY_NS
    )

    hot = status["datasets"][0]["hot"]
    assert hot["state"] == "hot-unmanaged"
    assert hot["materialized_count"] == 1
    assert hot["materializations"][0]["snapshot_id"] == snapshot_id
    assert hot["materializations"][0]["managed"] is False


def _reference_process(tmp_path, monkeypatch, *, maps_text, cwd='/elsewhere', exe='/program', alias=None):
    process = tmp_path / 'proc/123'
    (process / 'fd').mkdir(parents=True)
    (process / 'cwd').symlink_to(cwd)
    (process / 'root').symlink_to('/')
    (process / 'exe').symlink_to(exe)
    if alias is not None:
        (process / 'fd/7').symlink_to(alias)
    (process / 'maps').write_text(maps_text)
    monkeypatch.setattr(materialized_cache_module, '_process_directories', lambda: [process])
    return process


@pytest.mark.parametrize('limit', [1, 3, 20, 1000])
def test_reference_evidence_order_limits_deleted_names_and_inode_alias(tmp_path, monkeypatch, limit):
    source = tmp_path / 'source'
    source.mkdir()
    original, alias = source / 'original', tmp_path / 'alias'
    original.write_bytes(b'protected original')
    os.link(original, alias)
    info = original.stat()
    child, spaced = source / 'raw', tmp_path / 'spaced name'
    inode_line = f'000-fff r--p 0 {os.major(info.st_dev):x}:{os.minor(info.st_dev):x} {info.st_ino} {alias}'
    text = '\n'.join((inode_line, f'000-fff r--p 0 00:00 0 {spaced}',
        f'arbitrary legacy map text {child}/file extra', f'000-fff r--p 0 00:00 0 {source}/deleted (deleted)'))
    _reference_process(tmp_path, monkeypatch, maps_text=text, cwd=child,
        exe=str(child / 'deleted-program') + ' (deleted)', alias=alias)
    expected = [f'pid=123:cwd:{child}', 'pid=123:root:/',
        f'pid=123:exe:{child}/deleted-program (deleted)', f'pid=123:fd=7:{alias}',
        f'pid=123:maps-inode:{info.st_dev}:{info.st_ino}', f'pid=123:maps:{source}',
        f'pid=123:maps:{child}', f'pid=123:maps:{spaced}']
    targets = [spaced, child, source, Path('/'), *(source / f'unrelated-{i}' for i in range(80))]
    assert materialized_cache_module.process_references_many(
        targets, inode_keys={(info.st_dev, info.st_ino)}, limit=limit) == expected[:limit]


def test_reference_negative_checks_preserve_sibling_boundary_and_inode_only_mode(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    _reference_process(tmp_path, monkeypatch, maps_text=f'000-fff r--p 0 00:00 0 {source}-other/file',
        cwd=str(source) + '-other', exe=str(source) + '-other/program')
    assert materialized_cache_module.process_references_many(
        [source, *(source / f'raw/{i}' for i in range(80))]) == []
    assert materialized_cache_module.process_references_many([], inode_keys={(99, 100)}) == []


def test_component_reference_lookup_matches_the_original_prefix_semantics():
    root_sets = [
        set(), {'/'}, {'/data/a', '/data/a b', '/data/ab'},
        {'/data/a', '/data/a/child', '/other/with space'},
    ]
    values = ['/data/a', '/data/a/child', '/data/ab/file', '/data/a b/file',
              '/data/a b (deleted)', '/data/a-other/file', '/other/with space/x',
              '/', '/ordinary', '//double-root', 'socket:[123]']
    for roots in root_sets:
        for value in values:
            cleaned = value.removesuffix(' (deleted)')
            expected = any(cleaned == root or cleaned.startswith(root + os.sep) for root in roots)
            assert materialized_cache_module._reference_path_matches(value, roots) == expected


def test_maps_common_filter_never_discards_an_original_match():
    target_sets = [[], ['/'], ['/x'], ['/x/a', '/x/space name'], ['/x/a', '/else/b']]
    maps_values = ['', '000-fff r--p 0 00:00 0 /x/a',
                   'arbitrary legacy text /x/space name/child extra',
                   '000-fff r--p 0 00:00 0 /x/a-other',
                   '000-fff r--p 0 00:00 0 /else/b (deleted)',
                   'legacy //child', 'plain text /']
    for targets in target_sets:
        common = os.path.commonpath(targets) if targets else None
        for maps in maps_values:
            lines = maps.splitlines()
            original_match = any(f' {target}/' in maps or any(
                line.endswith(f' {target}') for line in lines) for target in targets)
            if original_match:
                assert materialized_cache_module._maps_could_reference(maps, lines, common)


def test_many_disjoint_targets_keep_exact_map_evidence_in_original_order(tmp_path, monkeypatch):
    base = tmp_path / 'preparation'
    targets = [base / f'group-{i:03}' / f'item {i:03}' for i in range(400)]
    chosen = [targets[2], targets[87], targets[399]]
    maps = '\n'.join((f'000-fff r--p 0 00:00 0 {chosen[0]}',
                      f'legacy text {chosen[1]}/nested extra',
                      f'000-fff r--p 0 00:00 0 {chosen[2]}',
                      f'000-fff r--p 0 00:00 0 {base}/unrelated/file'))
    _reference_process(tmp_path, monkeypatch, maps_text=maps)
    expected = [f'pid=123:maps:{target}' for target in sorted(chosen)]
    assert materialized_cache_module.process_references_many(targets, limit=1000) == expected


def test_inode_only_reference_checks_cwd_through_a_different_mount_name(tmp_path, monkeypatch):
    source = tmp_path / 'original-directory'
    source.mkdir()
    alias = tmp_path / 'another-name'
    alias.symlink_to(source, target_is_directory=True)
    _reference_process(tmp_path, monkeypatch, maps_text='', cwd=alias)
    info = source.stat()
    assert materialized_cache_module.process_references_many(
        [], inode_keys={(info.st_dev, info.st_ino)}) == [f'pid=123:cwd:{alias}']


@pytest.mark.parametrize('code', [errno.EACCES, errno.EPERM])
def test_reference_visibility_failure_never_becomes_negative_evidence(tmp_path, monkeypatch, code):
    process = _reference_process(tmp_path, monkeypatch, maps_text='')
    original = os.readlink
    def denied(path, *args, **kwargs):
        if Path(path) == process / 'cwd':
            raise PermissionError(code, 'live process visibility denied')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(os, 'readlink', denied)
    with pytest.raises(SnapshotError, match='incomplete process-reference scan'):
        materialized_cache_module.process_references_many([tmp_path / 'source'])
