#!/usr/bin/env python3
"""Automatically publish complete artifacts and evict verified old sources."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.artifact_maintenance import maintain_completed_artifacts
from stockagent.data_sync.desync_snapshots import SnapshotError


PENGUIN_DEVICE_ID = "QZTXXEL-YBCBYK7-ZK2ZSMS-DQKVSCE-IFC7MIX-4CWG6LE-KHPDLJI-7DSE6QW"


def _request_json(base_url: str, api_key: str, path: str, query: dict[str, str]) -> Any:
    url = base_url.rstrip("/") + path + "?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(url, headers={"X-API-Key": api_key})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def syncthing_peer_convergence(
    base_url: str, api_key: str, folder: str, device: str
) -> dict[str, Any]:
    status = _request_json(base_url, api_key, "/rest/db/status", {"folder": folder})
    completion = _request_json(
        base_url, api_key, "/rest/db/completion", {"folder": folder, "device": device}
    )
    errors = _request_json(base_url, api_key, "/rest/folder/errors", {"folder": folder})
    connections = _request_json(base_url, api_key, "/rest/system/connections", {})
    connection = connections.get("connections", {}).get(device, {})
    checks = {
        "folder_idle": status.get("state") == "idle",
        "need_bytes_zero": int(status.get("needBytes", -1)) == 0,
        "need_items_zero": int(status.get("needTotalItems", -1)) == 0,
        "need_deletes_zero": int(status.get("needDeletes", -1)) == 0,
        "errors_zero": int(status.get("errors", -1)) == 0,
        "pull_errors_zero": int(status.get("pullErrors", -1)) == 0,
        "watch_error_empty": not status.get("watchError"),
        "peer_connected": bool(connection.get("connected")),
        "peer_completion_100": float(completion.get("completion", -1)) == 100.0,
        "peer_need_bytes_zero": int(completion.get("needBytes", -1)) == 0,
        "peer_need_items_zero": int(completion.get("needItems", -1)) == 0,
        "peer_need_deletes_zero": int(completion.get("needDeletes", -1)) == 0,
        "peer_remote_state_valid": completion.get("remoteState") == "valid",
        "folder_errors_empty": not errors.get("errors"),
    }
    return {
        "ok": all(checks.values()),
        "folder": folder,
        "device": device,
        "checks": checks,
        "completion": completion.get("completion"),
        "remoteState": completion.get("remoteState"),
        "transport": connection.get("type"),
        "crypto": connection.get("crypto"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=REPO_ROOT / "artifacts")
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument(
        "--state-root", type=Path, default=Path("/var/lib/stockagent-cold-artifacts")
    )
    parser.add_argument("--scope", default="ablations")
    parser.add_argument("--stable-hours", type=float, default=24.0)
    parser.add_argument("--retention-days", type=float, default=7.0)
    parser.add_argument("--max-publish", type=int, default=1)
    parser.add_argument("--syncthing-url", default="http://127.0.0.1:18384")
    parser.add_argument("--syncthing-folder", default="stockagent-packed")
    parser.add_argument("--peer-device", default=PENGUIN_DEVICE_ID)
    parser.add_argument("--apply", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    api_key = os.environ.get("STGUIAPIKEY") or os.environ.get("OPEN_BUTTON_TOKEN")

    def peer_check() -> dict[str, Any]:
        if not api_key:
            return {"ok": False, "error": "Syncthing API key is unavailable"}
        try:
            return syncthing_peer_convergence(
                args.syncthing_url, api_key, args.syncthing_folder, args.peer_device
            )
        except (OSError, urllib.error.URLError, ValueError) as exc:
            return {"ok": False, "error": str(exc)}

    try:
        result = maintain_completed_artifacts(
            args.artifact_root,
            args.sync_root,
            args.state_root,
            scope=args.scope,
            stable_hours=args.stable_hours,
            retention_days=args.retention_days,
            max_publish=args.max_publish,
            apply=args.apply,
            peer_converged=peer_check,
        )
    except (OSError, SnapshotError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
