from __future__ import annotations

import json
import time
from pathlib import Path

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
