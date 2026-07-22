from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    fetch_snapshot,
    init_sync_root,
    publish_snapshot,
    resolve_status,
)


def _desync() -> str:
    value = shutil.which("desync") or str(Path.home() / ".local/bin/desync")
    if not Path(value).is_file():
        pytest.skip("desync is not installed")
    return value


def test_init_refuses_node_identity_replacement(tmp_path: Path) -> None:
    root = tmp_path / "sync"
    init_sync_root(root, "trainer-a")
    init_sync_root(root, "trainer-a")
    with pytest.raises(SnapshotError, match="refusing to replace"):
        init_sync_root(root, "trainer-b")


def test_publish_status_fetch_round_trip(tmp_path: Path) -> None:
    sync_root = tmp_path / "sync"
    source = tmp_path / "source"
    source.mkdir()
    (source / "nested").mkdir()
    (source / "nested" / "value.txt").write_text("from trainer-a\n", encoding="utf-8")
    init_sync_root(sync_root, "trainer-a")

    published = publish_snapshot(
        "sync-smoke", source, sync_root, {"test": "initial"}, _desync()
    )
    status = resolve_status("sync-smoke", sync_root, _desync())
    assert status["snapshot_id"] == published["snapshot_id"]
    assert status["publisher"] == "trainer-a"
    assert status["complete"] is True

    pin = tmp_path / "snapshot.pin.json"
    fetched = fetch_snapshot("sync-smoke", sync_root, tmp_path / "materialized", pin, _desync())
    restored = Path(fetched["path"]) / "nested" / "value.txt"
    assert restored.read_text(encoding="utf-8") == "from trainer-a\n"
    assert json.loads(pin.read_text(encoding="utf-8"))["snapshot_id"] == published["snapshot_id"]


def test_latest_head_uses_hlc_then_publisher(tmp_path: Path) -> None:
    root = tmp_path / "sync"
    source = tmp_path / "source"
    source.mkdir()
    (source / "value").write_text("one", encoding="utf-8")
    init_sync_root(root, "trainer-a")
    first = publish_snapshot("dataset", source, root, desync_bin=_desync())
    head_a = root / "heads" / "dataset" / "trainer-a.json"
    head_b = root / "heads" / "dataset" / "trainer-b.json"
    value = json.loads(head_a.read_text(encoding="utf-8"))
    value["publisher"] = "trainer-b"
    head_b.write_text(json.dumps(value), encoding="utf-8")
    status = resolve_status("dataset", root, _desync())
    assert status["publisher"] == "trainer-b"
    assert status["snapshot_id"] == first["snapshot_id"]
