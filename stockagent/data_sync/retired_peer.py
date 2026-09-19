"""Exact penguin-side Syncthing de-enrollment for the retired lab203 peer.

This changes sharing metadata only.  It never removes packed objects, heads,
manifests, hot artifacts, materializations, or the remote machine's files.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Mapping

from stockagent.data_sync.desync_snapshots import SnapshotError


LAB203_DEVICE_ID = (
    "TLOI2HH-6EZOSBC-H6YMGVW-APQOGFW-P2A7FZT-EKX6IUP-2HMRKS6-LBDHOQM"
)
PACKED_FOLDER_ID = "stockagent-packed"
HOT_FOLDER_ID = "stockagent-artifacts-hot"


def retired_lab203_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Build a minimal, auditable config change; fail on topology drift."""

    result = deepcopy(dict(config))
    devices = result.get("devices")
    folders = result.get("folders")
    ignored = result.get("remoteIgnoredDevices")
    if not isinstance(devices, list) or not isinstance(folders, list) or not isinstance(ignored, list):
        raise SnapshotError("Syncthing configuration has unexpected structure")
    lab = [device for device in devices if device.get("deviceID") == LAB203_DEVICE_ID]
    if len(lab) != 1 or lab[0].get("name") != "lab203" or lab[0].get("paused") is not True:
        raise SnapshotError("lab203 identity is absent, renamed, or not paused")
    if lab[0].get("introducer") or lab[0].get("autoAcceptFolders"):
        raise SnapshotError("lab203 introducer/auto-accept state requires separate audit")
    packed = [folder for folder in folders if folder.get("id") == PACKED_FOLDER_ID]
    hot = [folder for folder in folders if folder.get("id") == HOT_FOLDER_ID]
    if len(packed) != 1 or len(hot) != 1:
        raise SnapshotError("expected packed and hot folders are not both configured")
    if any(
        LAB203_DEVICE_ID in {device.get("deviceID") for device in folder.get("devices", [])}
        for folder in folders
        if folder.get("id") not in {PACKED_FOLDER_ID, HOT_FOLDER_ID}
    ):
        raise SnapshotError("lab203 is shared with an unexpected folder")
    hot_ids = {device.get("deviceID") for device in hot[0].get("devices", [])}
    if hot_ids != {LAB203_DEVICE_ID, _local_device_id(result)}:
        raise SnapshotError("hot folder has an unexpected participant")
    packed_ids = {device.get("deviceID") for device in packed[0].get("devices", [])}
    if LAB203_DEVICE_ID not in packed_ids or _local_device_id(result) not in packed_ids:
        raise SnapshotError("packed folder topology changed")
    if any(device.get("deviceID") == LAB203_DEVICE_ID for device in ignored):
        raise SnapshotError("lab203 is both configured and ignored")

    result["devices"] = [device for device in devices if device.get("deviceID") != LAB203_DEVICE_ID]
    result["folders"] = [
        {
            **folder,
            "devices": [
                device
                for device in folder.get("devices", [])
                if device.get("deviceID") != LAB203_DEVICE_ID
            ],
        }
        if folder.get("id") == PACKED_FOLDER_ID
        else folder
        for folder in folders
        if folder.get("id") != HOT_FOLDER_ID
    ]
    result["remoteIgnoredDevices"] = [
        *ignored,
        {
            "deviceID": LAB203_DEVICE_ID,
            "name": "lab203",
            "time": datetime.now(timezone.utc).isoformat(),
            "address": "",
        },
    ]
    return result


def _local_device_id(config: Mapping[str, Any]) -> str:
    candidates = [
        device["deviceID"]
        for device in config["devices"]
        if device.get("name") == "penguin"
    ]
    if len(candidates) != 1:
        raise SnapshotError("penguin local Syncthing identity is ambiguous")
    return str(candidates[0])
