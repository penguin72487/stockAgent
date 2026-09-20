from __future__ import annotations

import os
from pathlib import Path

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.hot_cache_mirror import (
    apply_hot_cache_mirror,
    plan_hot_cache_mirror,
)


def _roots(tmp_path: Path) -> tuple[Path, Path]:
    local = tmp_path / "artifacts"
    hot = tmp_path / "hot"
    (local / "cache" / "panel").mkdir(parents=True)
    (hot / "cache" / "panel").mkdir(parents=True)
    (hot / ".stignore").write_text("(?d)/cache\n", encoding="utf-8")
    return local, hot


def test_prune_only_removes_second_hard_link_name(tmp_path: Path) -> None:
    local, hot = _roots(tmp_path)
    source = local / "cache" / "panel" / "features.npy"
    source.write_bytes(b"immutable derived bytes")
    os.link(source, hot / "cache" / "panel" / "features.npy")

    blocked = plan_hot_cache_mirror(local, hot, bridge_inactive=False)
    assert blocked["blockers"] == ["hot-bridge-active"]
    plan = plan_hot_cache_mirror(local, hot, bridge_inactive=True)
    result = apply_hot_cache_mirror(
        local,
        hot,
        tmp_path / "quarantine",
        expected_fingerprint=plan["plan_fingerprint"],
        bridge_inactive=True,
    )

    assert result["deleted_hot_files"] == 1
    assert result["physical_reclaim_estimate_bytes"] == 0
    assert source.read_bytes() == b"immutable derived bytes"
    assert source.stat().st_nlink == 1
    assert not (hot / "cache").exists()


def test_prune_refuses_unique_hot_cache_file(tmp_path: Path) -> None:
    local, hot = _roots(tmp_path)
    (local / "cache" / "panel" / "features.npy").write_bytes(b"local")
    unique = hot / "cache" / "panel" / "features.npy"
    unique.write_bytes(b"different")

    with pytest.raises(SnapshotError, match="not a hard link"):
        plan_hot_cache_mirror(local, hot, bridge_inactive=True)
    assert unique.read_bytes() == b"different"


def test_prune_refuses_changed_plan(tmp_path: Path) -> None:
    local, hot = _roots(tmp_path)
    source = local / "cache" / "panel" / "features.npy"
    source.write_bytes(b"old")
    os.link(source, hot / "cache" / "panel" / "features.npy")
    plan = plan_hot_cache_mirror(local, hot, bridge_inactive=True)
    source.write_bytes(b"new")

    with pytest.raises(SnapshotError, match="plan changed"):
        apply_hot_cache_mirror(
            local,
            hot,
            tmp_path / "quarantine",
            expected_fingerprint=plan["plan_fingerprint"],
            bridge_inactive=True,
        )
    assert (hot / "cache" / "panel" / "features.npy").read_bytes() == b"new"
