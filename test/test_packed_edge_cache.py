from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from stockagent.data_sync.desync_snapshots import ResolvedSnapshot, SnapshotError
from stockagent.data_sync.packed_edge_cache import (
    ensure_edge_include,
    local_payload_inventory,
    prune_local_payloads,
    release_payload_relpaths,
    render_edge_ignore,
    verify_payload_relpaths,
    write_edge_ignore,
)


def _object(root: Path, kind: str, payload: bytes) -> tuple[Path, str]:
    digest = hashlib.sha256(payload).hexdigest()
    suffix = ".blob" if kind == "blobs" else ".zip"
    path = root / "objects" / kind / digest[:2] / f"{digest}{suffix}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path, path.relative_to(root).as_posix()


def test_edge_ignore_places_exact_exceptions_before_general_rules() -> None:
    payload = render_edge_ignore(
        ["objects/blobs/aa/" + "a" * 64 + ".blob"]
    ).decode()
    lines = payload.splitlines()
    assert lines.index("!/objects/blobs/aa/" + "a" * 64 + ".blob") < lines.index(
        "/objects/blobs/**"
    )
    with pytest.raises(SnapshotError, match="not a payload object"):
        render_edge_ignore(["objects/inventories/aa/inventory.json"])


def test_edge_include_preserves_existing_rules(tmp_path: Path) -> None:
    (tmp_path / ".stignore").write_text("(?d).local-state/**\n", encoding="utf-8")
    ensure_edge_include(tmp_path)
    ensure_edge_include(tmp_path)
    text = (tmp_path / ".stignore").read_text(encoding="utf-8")
    assert "(?d).local-state/**" in text
    assert text.count("#include .stignore-edge") == 1
    write_edge_ignore(tmp_path)
    assert "/objects/packs/**" in (tmp_path / ".stignore-edge").read_text()


def test_prune_keeps_allowed_payload_and_all_inventories(tmp_path: Path) -> None:
    kept, kept_relative = _object(tmp_path, "blobs", b"keep")
    removed, _ = _object(tmp_path, "packs", b"remove")
    inventory = tmp_path / "objects" / "inventories" / "aa" / "inventory.json.gz"
    inventory.parent.mkdir(parents=True)
    inventory.write_bytes(b"inventory")

    before = local_payload_inventory(tmp_path)
    preview = prune_local_payloads(
        tmp_path, allowed_relpaths=[kept_relative], apply=False
    )
    applied = prune_local_payloads(
        tmp_path, allowed_relpaths=[kept_relative], apply=True
    )

    assert before["files"] == 2
    assert preview["would_delete_files"] == 1
    assert applied["deleted_files"] == 1
    assert kept.is_file()
    assert not removed.exists()
    assert inventory.is_file()
    assert verify_payload_relpaths(tmp_path, [kept_relative])["files"] == 1


def test_release_payload_paths_reject_inventory_as_payload(tmp_path: Path) -> None:
    resolved = ResolvedSnapshot(
        manifest={
            "archive": {
                "objects": [
                    {"relpath": "objects/blobs/aa/" + "a" * 64 + ".blob"}
                ]
            }
        },
        manifest_path=tmp_path / "manifest.json",
        manifest_sha256="b" * 64,
        head_path=tmp_path / "head.json",
    )
    assert len(release_payload_relpaths(resolved)) == 1
