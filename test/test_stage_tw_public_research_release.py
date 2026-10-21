from __future__ import annotations

from pathlib import Path

import pytest

from scripts.stage_tw_public_research_release import (
    FORMAL_MEMBERS,
    _required_formal_members,
)


def test_research_release_includes_entitlement_manifest(tmp_path: Path) -> None:
    receipt = {
        "raw_receipt_manifest": {
            "relative_path": "raw/tw_corporate_action_entitlements/manifests/abc.jsonl"
        }
    }
    members = _required_formal_members(receipt, tmp_path)
    assert members[:-1] == FORMAL_MEMBERS
    assert members[-1] == (
        "raw/tw_corporate_action_entitlements/manifests/abc.jsonl"
    )


@pytest.mark.parametrize(
    "relative_path",
    ["", "../outside.jsonl", "/outside.jsonl", "raw/receipt.json"],
)
def test_research_release_rejects_unsafe_manifest_path(
    tmp_path: Path, relative_path: str
) -> None:
    receipt = {"raw_receipt_manifest": {"relative_path": relative_path}}
    with pytest.raises(RuntimeError, match="invalid formal entitlement"):
        _required_formal_members(receipt, tmp_path)


def test_research_release_requires_manifest_receipt(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="raw manifest receipt is missing"):
        _required_formal_members({}, tmp_path)


def test_unchanged_stage_keeps_inode_mtime_and_byte_proof(tmp_path):
    from scripts.stage_tw_public_research_release import _stage_copy
    from stockagent.data_sync.desync_snapshots import sha256_file
    source, target = tmp_path / "source", tmp_path / "stage/copy"
    source.write_bytes(b"exact original source")
    expected = sha256_file(source)
    assert _stage_copy(source, target, expected) is True
    before = target.stat()
    assert _stage_copy(source, target, expected) is False
    assert target.stat().st_ino == before.st_ino
    assert target.stat().st_mtime_ns == before.st_mtime_ns
    target.write_bytes(b"corrupt bytes")
    assert _stage_copy(source, target, expected) is True
    assert target.read_bytes() == source.read_bytes()
    assert target.stat().st_mode & 0o777 == 0o600


def test_stage_noop_still_rejects_changed_original_and_unsafe_target(tmp_path):
    from scripts.stage_tw_public_research_release import _stage_copy
    from stockagent.data_sync.desync_snapshots import sha256_file
    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"original source")
    expected = sha256_file(source)
    assert _stage_copy(source, target, expected)
    source.write_bytes(b"modified source")
    with pytest.raises(RuntimeError, match="changed"):
        _stage_copy(source, target, expected)
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="regular"):
        _stage_copy(source, link, expected)
    with pytest.raises(RuntimeError, match="overwrite"):
        _stage_copy(source, source, expected)
