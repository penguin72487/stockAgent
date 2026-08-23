from __future__ import annotations

import json
import time
from pathlib import Path

from stockagent.data_sync import materialized_cache as materialized_cache_module
from stockagent.data_sync.materialized_cache import (
    evict_materialized_snapshots,
    materialized_cache_status,
    use_materialized_snapshot,
)
from stockagent.data_sync.packed_snapshots import (
    initialize_packed_layout,
    publish_packed_snapshot,
)


DAY_NS = 86_400 * 1_000_000_000
BASE_NS = time.time_ns()


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
    assert (sync_root / "manifests" / "prices" / f"{snapshot_id}.json").is_file()

    renewed = use_materialized_snapshot(
        sync_root, hot_root, "prices", now_ns=BASE_NS + 19 * DAY_NS
    )
    assert Path(renewed["target"]).is_dir()


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
