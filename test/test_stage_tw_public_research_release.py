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
