#!/usr/bin/env python3
"""Configure/audit only the explicitly enrolled artifact-quarantine folder.

Credentials stay inside the process. No raw config, process arguments or API
keys are printed. Existing folders and device identities are never rewritten.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


def credentials() -> tuple[str, str]:
    config_path = Path.home() / ".local/state/syncthing/config.xml"
    if config_path.is_file():
        gui = ET.parse(config_path).getroot().find("gui")
        address = gui.findtext("address")
        scheme = "https" if gui.get("tls") == "true" else "http"
        return f"{scheme}://{address}", gui.findtext("apikey")
    candidates = set()
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            args = (proc / "cmdline").read_bytes().decode().split("\0")
        except (OSError, UnicodeDecodeError):
            continue
        if not args or Path(args[0]).name != "syncthing" or "serve" not in args:
            continue
        options = dict(
            arg[2:].split("=", 1) for arg in args if arg.startswith("--") and "=" in arg
        )
        if options.get("gui-address") and options.get("gui-apikey"):
            candidates.add(("http://" + options["gui-address"], options["gui-apikey"]))
    if len(candidates) != 1:
        raise RuntimeError("cannot uniquely discover local Syncthing credentials")
    return candidates.pop()


def request(base: str, key: str, path: str, query=None, *, method="GET", payload=None):
    url = base.rstrip("/") + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"X-API-Key": key, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read()
        return json.loads(body) if body else None


def expected_folder(config, role, devices, my_id):
    own_name = config["origin"] if role == "producer" else config["receiver"]
    peer_name = config["receiver"] if role == "producer" else config["origin"]

    def identity(name):
        matches = [d["deviceID"] for d in devices if d.get("name") == name]
        if len(matches) != 1:
            raise RuntimeError(f"device name {name!r} is not unique")
        return matches[0]

    if identity(own_name) != my_id:
        raise RuntimeError("this node is not the configured transport role")
    peer = identity(peer_name)
    return {
        "id": config["folder_id"],
        "path": config["transport_root"],
        "type": "sendonly" if role == "producer" else "receiveonly",
        "devices": [{"deviceID": my_id}, {"deviceID": peer}],
        "paused": False,
        "fsWatcherEnabled": True,
    }, peer


def assert_folder(current, expected):
    for key in ("id", "path", "type", "paused", "fsWatcherEnabled"):
        if current.get(key) != expected[key]:
            raise RuntimeError(
                f"existing folder disagrees on {key}; refusing replacement"
            )
    if {d["deviceID"] for d in current["devices"]} != {
        d["deviceID"] for d in expected["devices"]
    }:
        raise RuntimeError("existing folder has a different peer scope")
    if current.get("ignoreDelete"):
        raise RuntimeError("unexpected ignoreDelete setting")


def run(config, role, action):
    base, key = credentials()

    def api(path, query=None, **kwargs):
        return request(base, key, path, query, **kwargs)

    identity = api("/rest/system/status")["myID"]
    expected, peer = expected_folder(
        config, role, api("/rest/config/devices"), identity
    )
    folders = api("/rest/config/folders")
    existing = [row for row in folders if row["id"] == expected["id"]]
    root = Path(expected["path"])
    # The transfer configuration cannot accidentally enroll a broad data tree.
    if str(root) != "/srv/" + expected["id"] or not expected["id"].startswith(
        "stockagent-artifact-ingress-"
    ):
        raise RuntimeError("folder is outside the quarantine namespace")
    if root.is_symlink():
        raise RuntimeError("transport folder must not be a symlink")
    if action == "configure" and not existing:
        if root.exists() and any(root.iterdir()):
            raise RuntimeError("unregistered transport folder is not empty")
        root.mkdir(parents=True, exist_ok=True)
        (root / ".stfolder").mkdir(exist_ok=True)
        template = api("/rest/config/defaults/folder")
        template.update(expected)
        template.update(
            label="Selected completed artifact quarantine",
            rescanIntervalS=3600,
            fsWatcherDelayS=1,
            ignorePerms=True,
            ignoreDelete=False,
        )
        api("/rest/config/folders", method="POST", payload=template)
        existing = [api("/rest/config/folders/" + expected["id"])]
    if len(existing) != 1:
        raise RuntimeError("authorized folder is not configured")
    assert_folder(existing[0], expected)
    if action == "scan":
        api("/rest/db/scan", {"folder": expected["id"]}, method="POST")
    status = api("/rest/db/status", {"folder": expected["id"]})
    completion = api("/rest/db/completion", {"folder": expected["id"], "device": peer})
    errors = api("/rest/folder/errors", {"folder": expected["id"]})
    system_errors = api("/rest/system/error")
    conn = api("/rest/system/connections").get("connections", {}).get(peer, {})
    checks = {
        "idle": status.get("state") == "idle",
        "local_need_zero": all(
            status.get(field, -1) == 0
            for field in (
                "needBytes",
                "needTotalItems",
                "needFiles",
                "needDirectories",
                "needSymlinks",
                "needDeletes",
            )
        ),
        "errors_zero": status.get("errors", -1) == 0
        and status.get("pullErrors", -1) == 0
        and not errors.get("errors"),
        "watch_error_empty": not status.get("watchError"),
        "system_errors_empty": not system_errors.get("errors"),
        "connected": bool(conn.get("connected")),
        "peer_complete": completion.get("completion") == 100
        and all(
            completion.get(f, -1) == 0
            for f in ("needBytes", "needItems", "needDeletes")
        )
        and completion.get("remoteState") == "valid",
    }
    result = {
        "folder": expected["id"],
        "role": role,
        "checks": checks,
        "ok": all(checks.values()),
        "state": status.get("state"),
        "needBytes": status.get("needBytes"),
        "localBytes": status.get("localBytes"),
        "localFiles": status.get("localFiles"),
        "completion": completion.get("completion"),
        "peerNeedBytes": completion.get("needBytes"),
        "remoteState": completion.get("remoteState"),
        "transport": conn.get("type"),
        "crypto": conn.get("crypto"),
        "restart_required": api("/rest/config/restart-required"),
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    state = Path(config["state_root"])
    state.mkdir(parents=True, exist_ok=True)
    from stockagent.data_sync.desync_snapshots import atomic_write_json

    atomic_write_json(state / f"syncthing-{role}.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, default=Path("configs/data_sync/artifact_ingress.json")
    )
    parser.add_argument("--config-json")
    parser.add_argument("--role", choices=("producer", "receiver"), required=True)
    parser.add_argument("action", choices=("configure", "status", "scan"))
    args = parser.parse_args()
    print(
        json.dumps(
            run(
                json.loads(args.config_json or args.config.read_text()),
                args.role,
                args.action,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    # Permit running from scripts/ without requiring editable installation.
    import sys

    if "__file__" in globals():
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    main()
