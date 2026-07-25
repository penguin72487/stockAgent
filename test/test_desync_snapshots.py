from __future__ import annotations

import hashlib
import json
import shutil
import time
from pathlib import Path

import pytest

from stockagent.data_sync.desync_snapshots import (
    HLC,
    SnapshotError,
    atomic_write_json,
    fetch_snapshot,
    initialize_layout,
    next_hlc,
    publish_snapshot,
    resolve_latest,
    scan_tree,
    verify_snapshot,
)


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode()


def _write_candidate(
    root: Path,
    *,
    dataset: str,
    node_id: str,
    physical_ns: int,
    logical: int = 0,
) -> dict:
    snapshot_id = f"{dataset}-{node_id}-{physical_ns}-{logical}"
    stamp = HLC(physical_ns=physical_ns, logical=logical, node_id=node_id)
    manifest = {
        "schema_version": 1,
        "dataset": dataset,
        "snapshot_id": snapshot_id,
        "hlc": stamp.to_dict(),
        "publisher": {"node_id": node_id},
        "source": {"portable_fingerprint_sha256": "0" * 64},
        "archive": {
            "format": "desync-caidx-v1",
            "index_relpath": f"indices/{dataset}/{snapshot_id}.caidx",
            "index_sha256": "0" * 64,
            "store_relpath": f"stores/{dataset}.castr",
        },
    }
    manifest_path = root / "manifests" / dataset / f"{snapshot_id}.json"
    atomic_write_json(manifest_path, manifest)
    manifest_sha = hashlib.sha256(_canonical(manifest)).hexdigest()
    atomic_write_json(
        root / "heads" / dataset / f"{node_id}.json",
        {
            "schema_version": 1,
            "dataset": dataset,
            "node_id": node_id,
            "snapshot_id": snapshot_id,
            "hlc": stamp.to_dict(),
            "manifest_relpath": f"manifests/{dataset}/{snapshot_id}.json",
            "manifest_sha256": manifest_sha,
        },
    )
    return manifest


def _desync_binary() -> str | None:
    found = shutil.which("desync")
    if found:
        return found
    local = Path.home() / ".local" / "bin" / "desync"
    return str(local) if local.is_file() else None


def test_hlc_ticks_after_observed_remote_stamp() -> None:
    observed = [
        HLC(physical_ns=100, logical=2, node_id="node-a"),
        HLC(physical_ns=100, logical=4, node_id="node-b"),
    ]

    assert next_hlc(observed, node_id="node-c", now_ns=99) == HLC(
        physical_ns=100,
        logical=5,
        node_id="node-c",
    )
    assert next_hlc(observed, node_id="node-c", now_ns=101) == HLC(
        physical_ns=101,
        logical=0,
        node_id="node-c",
    )


def test_resolve_latest_uses_deterministic_node_tie_break(tmp_path: Path) -> None:
    root = tmp_path / "sync"
    initialize_layout(root, node_id="local-node")
    _write_candidate(root, dataset="prices", node_id="node-a", physical_ns=100)
    expected = _write_candidate(
        root,
        dataset="prices",
        node_id="node-b",
        physical_ns=100,
    )

    resolved = resolve_latest(
        root,
        "prices",
        now_ns=100,
        verify_index=False,
    )

    assert resolved.manifest["snapshot_id"] == expected["snapshot_id"]
    assert resolved.manifest["publisher"]["node_id"] == "node-b"


def test_resolve_latest_rejects_only_future_clock_candidate(tmp_path: Path) -> None:
    root = tmp_path / "sync"
    initialize_layout(root, node_id="local-node")
    _write_candidate(
        root,
        dataset="prices",
        node_id="future-node",
        physical_ns=1_000_000_000_000,
    )

    with pytest.raises(SnapshotError, match="in the future"):
        resolve_latest(
            root,
            "prices",
            now_ns=1,
            max_clock_skew_seconds=1,
            verify_index=False,
        )


def test_resolve_latest_does_not_fall_back_past_incomplete_new_head(
    tmp_path: Path,
) -> None:
    root = tmp_path / "sync"
    initialize_layout(root, node_id="local-node")
    _write_candidate(root, dataset="prices", node_id="node-a", physical_ns=100)
    incomplete = _write_candidate(
        root,
        dataset="prices",
        node_id="node-b",
        physical_ns=101,
    )
    manifest_path = root / "manifests" / "prices" / f"{incomplete['snapshot_id']}.json"
    manifest_path.unlink()

    with pytest.raises(SnapshotError, match="refusing to fall back"):
        resolve_latest(
            root,
            "prices",
            now_ns=101,
            verify_index=False,
        )


def test_tree_portable_fingerprint_ignores_mtime_but_detects_size(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    payload = source / "prices.parquet"
    payload.write_bytes(b"abc")
    baseline = scan_tree(source)

    future = time.time() + 10
    payload.touch()
    payload.touch()
    payload.chmod(0o600)
    # Explicitly change mtime without changing portable content.
    import os

    os.utime(payload, (future, future))
    metadata_changed = scan_tree(source)
    assert (
        baseline["portable_fingerprint_sha256"]
        == metadata_changed["portable_fingerprint_sha256"]
    )
    assert (
        baseline["stability_fingerprint_sha256"]
        != metadata_changed["stability_fingerprint_sha256"]
    )

    payload.write_bytes(b"abcd")
    content_changed = scan_tree(source)
    assert (
        baseline["portable_fingerprint_sha256"]
        != content_changed["portable_fingerprint_sha256"]
    )


def test_init_refuses_sync_root_inside_git_worktree(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)

    with pytest.raises(SnapshotError, match="inside Git worktree"):
        initialize_layout(repo / "data-sync", node_id="node-a")


@pytest.mark.skipif(_desync_binary() is None, reason="desync is not installed")
def test_multiwriter_publish_resolve_fetch_verify_end_to_end(tmp_path: Path) -> None:
    binary = _desync_binary()
    assert binary is not None
    sync_root_a = tmp_path / "sync-a"
    sync_root_b = tmp_path / "sync-b"
    source = tmp_path / "source"
    source.mkdir()
    (source / "prices").mkdir()
    (source / "prices" / "2330.parquet").write_bytes(b"version-one")
    initialize_layout(sync_root_a, node_id="node-a")

    first = publish_snapshot(
        sync_root_a,
        "tw-public",
        source,
        node_id="node-a",
        desync_binary=binary,
        chunk_size_kib="2:8:32",
    )
    initialize_layout(sync_root_b, node_id="node-b")
    for relative in ("heads", "indices", "manifests", "stores"):
        shutil.copytree(
            sync_root_a / relative,
            sync_root_b / relative,
            dirs_exist_ok=True,
        )
    (source / "prices" / "2330.parquet").write_bytes(b"version-two")
    second = publish_snapshot(
        sync_root_b,
        "tw-public",
        source,
        node_id="node-b",
        desync_binary=binary,
        chunk_size_kib="2:8:32",
    )
    repeated = publish_snapshot(
        sync_root_b,
        "tw-public",
        source,
        node_id="node-b",
        desync_binary=binary,
        chunk_size_kib="2:8:32",
    )

    assert first.manifest["snapshot_id"] != second.manifest["snapshot_id"]
    assert second.manifest["snapshot_id"] != repeated.manifest["snapshot_id"]
    latest = resolve_latest(sync_root_b, "tw-public")
    assert latest.manifest["snapshot_id"] == repeated.manifest["snapshot_id"]

    target = fetch_snapshot(
        sync_root_b,
        tmp_path / "materialized",
        latest,
        desync_binary=binary,
    )
    assert (target / "prices" / "2330.parquet").read_bytes() == b"version-two"
    result = verify_snapshot(
        sync_root_b,
        latest,
        desync_binary=binary,
        materialized_path=target,
    )
    assert result["materialized_verified"] is True
    assert result["chunks"]["unique"] == result["chunks"]["in_store"]
