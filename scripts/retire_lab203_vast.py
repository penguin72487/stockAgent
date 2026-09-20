#!/usr/bin/env python3
"""Run on Vast via stdin to stop sharing with lab203; never delete data."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

LAB_ID = "TLOI2HH-6EZOSBC-H6YMGVW-APQOGFW-P2A7FZT-EKX6IUP-2HMRKS6-LBDHOQM"
GUI = "http://127.0.0.1:18384"
BACKUP_DIR = Path("/var/lib/stockagent-peer-retirement")


def _api_key() -> str:
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            args = (proc / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        if not any(arg.endswith(b"syncthing") for arg in args[:1]):
            continue
        if b"--gui-address=127.0.0.1:18384" not in args:
            continue
        for arg in args:
            if arg.startswith(b"--gui-apikey="):
                return arg.split(b"=", 1)[1].decode()
    raise RuntimeError("Vast Syncthing API process not found")


def _get(key: str, path: str) -> dict:
    request = urllib.request.Request(GUI + path, headers={"X-API-Key": key})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def _put(key: str, config: dict) -> None:
    request = urllib.request.Request(
        GUI + "/rest/config",
        data=json.dumps(config, separators=(",", ":")).encode(),
        headers={"X-API-Key": key, "Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"Syncthing PUT returned {response.status}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--plan-fingerprint")
    args = parser.parse_args()
    if args.apply != bool(args.plan_fingerprint):
        parser.error("--apply and --plan-fingerprint must be supplied together")
    try:
        key = _api_key()
        current = _get(key, "/rest/config")
        my_id = _get(key, "/rest/system/status").get("myID")
        names = {device["deviceID"]: device.get("name") for device in current["devices"]}
        if names.get(my_id) != "vastai1T" or names.get(LAB_ID) != "lab203":
            raise RuntimeError("Vast/lab203 identity changed")
        lab = next(device for device in current["devices"] if device["deviceID"] == LAB_ID)
        if lab.get("paused") is not True or lab.get("introducer") or lab.get("autoAcceptFolders"):
            raise RuntimeError("lab203 not safely paused or is an introducer")
        if any(device.get("deviceID") == LAB_ID for device in current.get("remoteIgnoredDevices", [])):
            raise RuntimeError("lab203 is both configured and ignored")
        shared = [
            folder for folder in current["folders"]
            if any(device.get("deviceID") == LAB_ID for device in folder.get("devices", []))
        ]
        if len(shared) != 1 or shared[0].get("id") != "stockagent-packed":
            raise RuntimeError("lab203 shares an unexpected folder")
        if my_id not in {device.get("deviceID") for device in shared[0]["devices"]}:
            raise RuntimeError("Vast is not a participant in packed folder")
        fingerprint = hashlib.sha256(
            json.dumps(current, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        result = {
            "machine": "vastai1T",
            "retired_device_name": "lab203",
            "retired_device_id": LAB_ID,
            "removed_from_folder": "stockagent-packed",
            "plan_fingerprint": fingerprint,
            "data_deleted": False,
        }
        if not args.apply:
            print(json.dumps(result, sort_keys=True))
            return 0
        if fingerprint != args.plan_fingerprint:
            raise RuntimeError("config changed since dry run")
        BACKUP_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(BACKUP_DIR, 0o700)
        backup = BACKUP_DIR / (
            "vast-syncthing-before-lab203-"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + "-" + uuid.uuid4().hex + ".json"
        )
        with backup.open("x") as stream:
            os.chmod(backup, 0o600)
            json.dump(current, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        updated = dict(current)
        updated["devices"] = [
            device for device in current["devices"] if device["deviceID"] != LAB_ID
        ]
        updated["folders"] = [
            {
                **folder,
                "devices": [
                    device for device in folder["devices"]
                    if device["deviceID"] != LAB_ID
                ],
            } if folder["id"] == "stockagent-packed" else folder
            for folder in current["folders"]
        ]
        updated["remoteIgnoredDevices"] = [
            *current.get("remoteIgnoredDevices", []),
            {
                "deviceID": LAB_ID,
                "name": "lab203",
                "time": datetime.now(timezone.utc).isoformat(),
                "address": "",
            },
        ]
        _put(key, updated)
        observed = _get(key, "/rest/config")
        if any(device["deviceID"] == LAB_ID for device in observed["devices"]):
            raise RuntimeError("lab203 still configured")
        if any(
            device.get("deviceID") == LAB_ID
            for folder in observed["folders"] for device in folder.get("devices", [])
        ):
            raise RuntimeError("lab203 still shared")
        if not any(
            device.get("deviceID") == LAB_ID
            for device in observed.get("remoteIgnoredDevices", [])
        ):
            raise RuntimeError("lab203 not ignored")
        if not any(
            device.get("deviceID") == my_id
            for folder in observed["folders"]
            if folder.get("id") == "stockagent-packed"
            for device in folder.get("devices", [])
        ):
            raise RuntimeError("Vast packed folder damaged")
        result.update(
            action="retired",
            backup_config=str(backup),
            restart_required=_get(key, "/rest/config/restart-required"),
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
