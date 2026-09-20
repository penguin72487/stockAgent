from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.retired_peer import (
    HOT_FOLDER_ID,
    LAB203_DEVICE_ID,
    PACKED_FOLDER_ID,
    retired_lab203_config,
)


def _config() -> dict:
    penguin = "PENGUIN-ID"
    vast = "VAST-ID"
    return {
        "devices": [
            {"deviceID": penguin, "name": "penguin"},
            {"deviceID": LAB203_DEVICE_ID, "name": "lab203", "paused": True},
            {"deviceID": vast, "name": "vastai1T"},
        ],
        "folders": [
            {
                "id": PACKED_FOLDER_ID,
                "path": "/srv/stockagent-packed",
                "devices": [{"deviceID": penguin}, {"deviceID": LAB203_DEVICE_ID}, {"deviceID": vast}],
            },
            {
                "id": HOT_FOLDER_ID,
                "path": "/srv/stockagent-artifacts-hot",
                "devices": [{"deviceID": penguin}, {"deviceID": LAB203_DEVICE_ID}],
            },
        ],
        "remoteIgnoredDevices": [],
    }


def test_retirement_removes_only_lab_shares_and_orphaned_hot_folder() -> None:
    config = _config()
    changed = retired_lab203_config(config)

    assert config["devices"][1]["name"] == "lab203"
    assert [device["name"] for device in changed["devices"]] == ["penguin", "vastai1T"]
    assert [folder["id"] for folder in changed["folders"]] == [PACKED_FOLDER_ID]
    assert [device["deviceID"] for device in changed["folders"][0]["devices"]] == [
        "PENGUIN-ID",
        "VAST-ID",
    ]
    assert changed["remoteIgnoredDevices"][0]["deviceID"] == LAB203_DEVICE_ID


def test_retirement_refuses_unexpected_hot_peer() -> None:
    config = _config()
    config["folders"][1]["devices"].append({"deviceID": "OTHER"})
    with pytest.raises(SnapshotError, match="unexpected participant"):
        retired_lab203_config(config)


def test_retirement_requires_lab_paused() -> None:
    config = _config()
    config["devices"][1]["paused"] = False
    with pytest.raises(SnapshotError, match="not paused"):
        retired_lab203_config(config)


def test_retired_topology_cannot_reinstall_hot_bridge() -> None:
    repo = Path(__file__).resolve().parents[1]
    retention = json.loads((repo / "configs/data_sync/packed_retention.json").read_text())
    assert retention["required_peer_names"] == ["vastai1T"]

    result = subprocess.run(
        ["bash", str(repo / "scripts/install_hot_artifact_sync_service.sh")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "retired with lab203" in result.stderr
