#!/usr/bin/env python3
"""De-enroll lab203 on penguin without touching any stored data."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json  # noqa: E402
from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id  # noqa: E402
from stockagent.data_sync.retired_peer import (  # noqa: E402
    HOT_FOLDER_ID,
    LAB203_DEVICE_ID,
    PACKED_FOLDER_ID,
    retired_lab203_config,
)

LAB203_DATASET = "stockagent-migration-core"
LAB203_SNAPSHOT_ID = (
    "stockagent-migration-core-20260812T030055076520750Z-l0-lab203-a4d130cbc3aacb1b"
)


def _api() -> tuple[str, dict[str, str]]:
    xml = ET.parse(Path("/root/.local/state/syncthing/config.xml")).getroot()
    key = xml.findtext("./gui/apikey")
    address = xml.findtext("./gui/address") or "127.0.0.1:8384"
    if not key:
        raise SnapshotError("Syncthing API key is unavailable")
    port = address.rsplit(":", 1)[-1]
    if not port.isdigit():
        raise SnapshotError("Syncthing GUI address has no port")
    return f"http://127.0.0.1:{port}", {"X-API-Key": key}


def _get(base: str, headers: dict[str, str], path: str) -> dict:
    with urllib.request.urlopen(
        urllib.request.Request(base + path, headers=headers), timeout=20
    ) as response:
        return json.load(response)


def _cold_presence() -> str:
    roots = (
        Path("/srv/stockagent-packed"),
        Path("/mnt/d/stockagent-backup/packed"),
    )
    heads = [root / "heads" / LAB203_DATASET / "lab203.json" for root in roots]
    raw = [head.read_bytes() for head in heads]
    if raw[0] != raw[1]:
        raise SnapshotError("lab203 C/D cold heads differ")
    head = json.loads(raw[0])
    if head.get("node_id") != "lab203" or head.get("snapshot_id") != LAB203_SNAPSHOT_ID:
        raise SnapshotError("lab203 cold head changed from the audited release")
    hashes = [
        resolve_packed_snapshot_id(root, LAB203_DATASET, LAB203_SNAPSHOT_ID).manifest_sha256
        for root in roots
    ]
    if hashes[0] != hashes[1] or hashes[0] != head.get("manifest_sha256"):
        raise SnapshotError("lab203 C/D cold manifest proof differs")
    return hashes[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--plan-fingerprint")
    args = parser.parse_args()
    if args.apply != bool(args.plan_fingerprint):
        parser.error("--apply and --plan-fingerprint must be supplied together")
    try:
        base, headers = _api()
        config = _get(base, headers, "/rest/config")
        status = _get(base, headers, "/rest/system/status")
        local_id = status.get("myID")
        if local_id not in {
            device.get("deviceID")
            for device in config.get("devices", [])
            if device.get("name") == "penguin"
        }:
            raise SnapshotError("penguin Syncthing local identity mismatch")
        transformed = retired_lab203_config(config)
        manifest_sha256 = _cold_presence()
        fingerprint = hashlib.sha256(
            json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        bridge_active = subprocess.run(
            ["systemctl", "is-active", "stockagent-hot-artifact-sync.service"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip() == "active"
        result = {
            "device_id": LAB203_DEVICE_ID,
            "cold_dataset": LAB203_DATASET,
            "cold_snapshot_id": LAB203_SNAPSHOT_ID,
            "cold_manifest_sha256": manifest_sha256,
            "cold_presence_on_C_and_D": True,
            "cold_content_verification": "run separately before apply",
            "remove_folder": HOT_FOLDER_ID,
            "retain_folder": PACKED_FOLDER_ID,
            "retain_other_devices": [
                device["name"] for device in transformed["devices"]
            ],
            "hot_bridge_active": bridge_active,
            "plan_fingerprint": fingerprint,
            "apply_ready": not bridge_active,
        }
        if not args.apply:
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        if fingerprint != args.plan_fingerprint:
            raise SnapshotError("Syncthing config plan changed; rerun dry-run")
        if bridge_active:
            raise SnapshotError("hot artifact bridge must be stopped before retirement")
        state_dir = Path("/var/lib/stockagent-peer-retirement")
        state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(state_dir, 0o700)
        backup = state_dir / (
            "syncthing-before-lab203-"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + "-"
            + uuid.uuid4().hex
            + ".xml"
        )
        source_config = Path("/root/.local/state/syncthing/config.xml")
        with backup.open("xb") as stream:
            os.chmod(backup, 0o600)
            stream.write(source_config.read_bytes())
            stream.flush()
            os.fsync(stream.fileno())
        request = urllib.request.Request(
            base + "/rest/config",
            data=json.dumps(transformed, separators=(",", ":")).encode(),
            headers={**headers, "Content-Type": "application/json"},
            method="PUT",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status != 200:
                raise SnapshotError(f"Syncthing config update returned {response.status}")
        observed = _get(base, headers, "/rest/config")
        if any(device.get("deviceID") == LAB203_DEVICE_ID for device in observed["devices"]):
            raise SnapshotError("lab203 remains configured after update")
        if any(
            device.get("deviceID") == LAB203_DEVICE_ID
            for folder in observed["folders"]
            for device in folder.get("devices", [])
        ):
            raise SnapshotError("lab203 remains shared after update")
        if any(folder.get("id") == HOT_FOLDER_ID for folder in observed["folders"]):
            raise SnapshotError("orphaned hot folder remains configured")
        if not any(
            device.get("deviceID") == LAB203_DEVICE_ID
            for device in observed.get("remoteIgnoredDevices", [])
        ):
            raise SnapshotError("lab203 is not in Syncthing ignored devices")
        result.update(
            action="retired",
            backup_config_xml=str(backup),
            restart_required=_get(base, headers, "/rest/config/restart-required"),
        )
        atomic_write_json(state_dir / "lab203-retirement.json", result)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, KeyError, SnapshotError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
