#!/usr/bin/env python3
"""Enroll only the paired backup receipt return channel, preserving other shares."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
try:
    from scripts.configure_artifact_ingress_syncthing import credentials, request, assert_folder
except ModuleNotFoundError:
    from configure_artifact_ingress_syncthing import credentials, request, assert_folder


def run(config: dict, role: str, action: str) -> dict:
    if (config.get("schema_version") != 1 or config.get("folder_id") != "stockagent-backup-receipts-lab203"
            or config.get("producer_type") != "receiveonly" or config.get("receiver_type") != "sendonly"
            or config.get("producer_root") != "/srv/stockagent-backup-receipts-lab203"
            or config.get("receiver_root") != "/srv/lab203-backup/receipts"):
        raise ValueError("backup acknowledgements need the bounded return namespace")
    base, key = credentials()
    my_id = request(base, key, "/rest/system/status")["myID"]
    if my_id != config[role + "_device_id"]:
        raise ValueError("local Syncthing identity differs from the selected backup role")
    peer = config[("receiver" if role == "producer" else "producer") + "_device_id"]
    devices = request(base, key, "/rest/config/devices")
    if sum(d["deviceID"] == peer for d in devices) != 1:
        raise ValueError("only an already paired peer can return backup receipts")
    folders = request(base, key, "/rest/config/folders")
    before = {f["id"]: f for f in folders if f["id"] != config["folder_id"]}
    expected = {"id": config["folder_id"], "path": config[role + "_root"], "type": config[role + "_type"],
                "paused": False, "fsWatcherEnabled": True, "devices": [{"deviceID": my_id}, {"deviceID": peer}]}
    found = [f for f in folders if f["id"] == expected["id"]]
    root = Path(expected["path"])
    if any(p.is_symlink() for p in (root, *root.parents)):
        raise ValueError("receipt return folder is redirected")
    if action == "configure" and not found:
        if root.exists() and any(root.iterdir()):
            raise ValueError("unregistered receipt return folder must be empty")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        (root / ".stfolder").mkdir(mode=0o700, exist_ok=True)
        template = request(base, key, "/rest/config/defaults/folder")
        template.update(expected)
        template.update(label="NAS backup proofs only", rescanIntervalS=300, fsWatcherDelayS=1,
                        ignorePerms=True, ignoreDelete=False)
        request(base, key, "/rest/config/folders", method="POST", payload=template)
        found = [request(base, key, "/rest/config/folders/" + expected["id"])]
    if len(found) != 1:
        raise ValueError("receipt return folder is not enrolled")
    assert_folder(found[0], expected)
    after = {f["id"]: f for f in request(base, key, "/rest/config/folders") if f["id"] != config["folder_id"]}
    if before != after:
        raise ValueError("an unrelated Syncthing folder changed during enrollment")
    if action == "scan":
        request(base, key, "/rest/db/scan", {"folder": expected["id"]}, method="POST")
    status = request(base, key, "/rest/db/status", {"folder": expected["id"]})
    completion = request(base, key, "/rest/db/completion", {"folder": expected["id"], "device": peer})
    return {"folder_id": expected["id"], "role": role, "type": expected["type"],
            "unrelated_folders_preserved": True, "state": status["state"],
            "local_need_bytes": status["needBytes"], "completion": completion["completion"],
            "remote_state": completion["remoteState"], "machine_acknowledgements_received": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--role", choices=("producer", "receiver"), required=True)
    parser.add_argument("action", choices=("configure", "status", "scan"))
    args = parser.parse_args()
    print(json.dumps(run(json.loads(args.config.read_bytes()), args.role, args.action)))


if __name__ == "__main__":
    main()
