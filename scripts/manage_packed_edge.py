#!/usr/bin/env python3
"""Manage an index-only packed-store cache on an ephemeral compute node."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError,
    _load_json,
    atomic_write_json,
)
from stockagent.data_sync.materialized_cache import (  # noqa: E402
    process_references,
    use_materialized_snapshot,
)
from stockagent.data_sync.packed_edge_cache import (  # noqa: E402
    EDGE_CACHE_SCHEMA_VERSION,
    EDGE_IGNORE_NAME,
    local_payload_inventory,
    prune_local_payloads,
    release_payload_relpaths,
    verify_payload_relpaths,
    write_edge_ignore,
    write_edge_receipt,
)
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    resolve_latest_packed,
    resolve_packed_snapshot_id,
)


def _request_json(
    base_url: str,
    api_key: str,
    path: str,
    query: dict[str, str],
    *,
    method: str = "GET",
) -> Any:
    url = base_url.rstrip("/") + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(
        url,
        data=b"" if method == "POST" else None,
        headers={"X-API-Key": api_key},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read()
    return json.loads(payload) if payload else None


def _device_id(base_url: str, api_key: str, name: str) -> str:
    devices = _request_json(base_url, api_key, "/rest/config/devices", {})
    matches = [
        str(item["deviceID"])
        for item in devices
        if item.get("name") == name and item.get("deviceID")
    ]
    if len(matches) != 1:
        raise SnapshotError(
            f"expected exactly one Syncthing device named {name!r}; found {len(matches)}"
        )
    return matches[0]


def _convergence(
    base_url: str, api_key: str, folder: str, peer_name: str
) -> dict[str, Any]:
    device = _device_id(base_url, api_key, peer_name)
    status = _request_json(base_url, api_key, "/rest/db/status", {"folder": folder})
    completion = _request_json(
        base_url,
        api_key,
        "/rest/db/completion",
        {"folder": folder, "device": device},
    )
    connection = _request_json(
        base_url, api_key, "/rest/system/connections", {}
    ).get("connections", {}).get(device, {})
    folder_errors = _request_json(
        base_url, api_key, "/rest/folder/errors", {"folder": folder}
    )
    system_errors = _request_json(base_url, api_key, "/rest/system/error", {})
    checks = {
        "folder_idle": status.get("state") == "idle",
        "need_bytes_zero": int(status.get("needBytes", -1)) == 0,
        "need_items_zero": int(status.get("needTotalItems", -1)) == 0,
        "need_deletes_zero": int(status.get("needDeletes", -1)) == 0,
        "errors_zero": int(status.get("errors", -1)) == 0,
        "pull_errors_zero": int(status.get("pullErrors", -1)) == 0,
        "watch_error_empty": not status.get("watchError"),
        "folder_errors_empty": not folder_errors.get("errors"),
        "system_errors_empty": not system_errors.get("errors"),
        "peer_connected": bool(connection.get("connected")),
        "peer_completion_100": float(completion.get("completion", -1)) == 100.0,
        "peer_need_bytes_zero": int(completion.get("needBytes", -1)) == 0,
        "peer_need_items_zero": int(completion.get("needItems", -1)) == 0,
        "peer_need_deletes_zero": int(completion.get("needDeletes", -1)) == 0,
        "peer_remote_state_valid": completion.get("remoteState") == "valid",
    }
    return {
        "ok": all(checks.values()),
        "peer_name": peer_name,
        "checks": checks,
        "folder_state": status.get("state"),
        "global_bytes": status.get("globalBytes"),
        "completion": completion.get("completion"),
        "remote_state": completion.get("remoteState"),
        "transport": connection.get("type"),
        "crypto": connection.get("crypto"),
    }


def _scan(base_url: str, api_key: str, folder: str) -> None:
    _request_json(
        base_url,
        api_key,
        "/rest/db/scan",
        {"folder": folder},
        method="POST",
    )


def _wait_for(
    predicate,
    *,
    timeout_seconds: float,
    interval_seconds: float = 2.0,
) -> Any:
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval_seconds)
    raise SnapshotError(f"timed out waiting for edge cache; last={last!r}")


def _load_state(path: Path) -> dict[str, Any]:
    state = _load_json(path)
    if int(state.get("schema_version", -1)) != EDGE_CACHE_SCHEMA_VERSION:
        raise SnapshotError(f"unsupported packed edge state: {path}")
    if state.get("mode") != "index-only":
        raise SnapshotError(f"packed edge mode is not index-only: {path}")
    return state


def _allowed_relpaths(sync_root: Path, state: dict[str, Any]) -> set[str]:
    result: set[str] = set()
    for dataset, snapshot_id in sorted(dict(state.get("hydrating", {})).items()):
        resolved = resolve_packed_snapshot_id(sync_root, dataset, snapshot_id)
        result.update(release_payload_relpaths(resolved))
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument(
        "--materialized-root",
        type=Path,
        default=Path("/srv/stockagent-packed-materialized"),
    )
    parser.add_argument(
        "--state-root", type=Path, default=Path("/var/lib/stockagent-packed-edge")
    )
    parser.add_argument("--syncthing-url", default="http://127.0.0.1:18384")
    parser.add_argument("--folder", default="stockagent-packed")
    parser.add_argument("--peer-name", default="penguin")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("audit")
    enable = sub.add_parser("enable")
    enable.add_argument("--apply", action="store_true")
    prune = sub.add_parser("prune")
    prune.add_argument("--apply", action="store_true")
    hydrate = sub.add_parser("use")
    hydrate.add_argument("dataset")
    hydrate.add_argument("--snapshot-id")
    hydrate.add_argument("--ttl-days", type=float, default=7.0)
    hydrate.add_argument("--link", type=Path, action="append", default=[])
    hydrate.add_argument("--timeout-seconds", type=float, default=21_600.0)
    return parser


def _print(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def main() -> int:
    args = build_parser().parse_args()
    api_key = os.environ.get("STGUIAPIKEY") or os.environ.get("OPEN_BUTTON_TOKEN")
    if not api_key:
        print("ERROR: Syncthing API key is unavailable", file=sys.stderr)
        return 2
    sync_root = args.sync_root.resolve()
    state_root = args.state_root.resolve()
    state_path = state_root / "state.json"
    receipt_root = state_root / "receipts"
    try:
        peer = _convergence(
            args.syncthing_url, api_key, args.folder, args.peer_name
        )
        references = process_references(sync_root / "objects")
        inventory = local_payload_inventory(sync_root)
        if args.command == "audit":
            _print(
                {
                    "mode": "audit",
                    "payload": inventory,
                    "process_references": references,
                    "peer": peer,
                    "edge_state": _load_json(state_path) if state_path.exists() else None,
                }
            )
            return 0
        if args.command == "enable":
            payload = {
                "mode": "index-only",
                "enabled": bool(args.apply),
                "payload_before": inventory,
                "process_references": references,
                "peer": peer,
                "hydrating": {},
            }
            if not peer["ok"]:
                raise SnapshotError("durable Syncthing peer is not fully converged")
            if references:
                raise SnapshotError("packed objects are referenced by a running process")
            if args.apply:
                state_root.mkdir(parents=True, exist_ok=True)
                write_edge_ignore(sync_root)
                _scan(args.syncthing_url, api_key, args.folder)
                _wait_for(
                    lambda: _convergence(
                        args.syncthing_url, api_key, args.folder, args.peer_name
                    )["ok"],
                    timeout_seconds=300,
                )
                atomic_write_json(state_path, {"schema_version": 1, **payload})
            receipt = write_edge_receipt(receipt_root, payload)
            _print(payload | {"receipt": str(receipt)})
            return 0
        state = _load_state(state_path)
        if args.command == "prune":
            if not peer["ok"]:
                raise SnapshotError("durable Syncthing peer is not fully converged")
            if references:
                raise SnapshotError("packed objects are referenced by a running process")
            allowed = _allowed_relpaths(sync_root, state)
            result = prune_local_payloads(
                sync_root, allowed_relpaths=allowed, apply=args.apply
            )
            if args.apply:
                _scan(args.syncthing_url, api_key, args.folder)
            payload = {
                "mode": "index-only",
                "peer": peer,
                "allowed_payloads": len(allowed),
                **result,
            }
            receipt = write_edge_receipt(receipt_root, payload)
            _print(payload | {"receipt": str(receipt)})
            return 0
        if args.command == "use":
            resolved = (
                resolve_packed_snapshot_id(sync_root, args.dataset, args.snapshot_id)
                if args.snapshot_id
                else resolve_latest_packed(sync_root, args.dataset)
            )
            snapshot_id = str(resolved.manifest["snapshot_id"])
            hydrating = dict(state.get("hydrating", {}))
            hydrating[args.dataset] = snapshot_id
            state["hydrating"] = hydrating
            allowed = _allowed_relpaths(sync_root, state)
            atomic_write_json(state_path, state)
            write_edge_ignore(sync_root, allowed)
            _scan(args.syncthing_url, api_key, args.folder)

            required = release_payload_relpaths(resolved)

            def ready() -> bool:
                return all((sync_root / path).is_file() for path in required)

            _wait_for(ready, timeout_seconds=args.timeout_seconds, interval_seconds=5)
            object_proof = verify_payload_relpaths(sync_root, required)
            lease = use_materialized_snapshot(
                sync_root,
                args.materialized_root,
                args.dataset,
                snapshot_id=snapshot_id,
                ttl_days=args.ttl_days,
                links=args.link,
                verify_existing=True,
            )
            hydrating.pop(args.dataset, None)
            state["hydrating"] = hydrating
            atomic_write_json(state_path, state)
            remaining = _allowed_relpaths(sync_root, state)
            write_edge_ignore(sync_root, remaining)
            _scan(args.syncthing_url, api_key, args.folder)
            prune = prune_local_payloads(
                sync_root, allowed_relpaths=remaining, apply=True
            )
            payload = {
                "mode": "index-only-use",
                "dataset": args.dataset,
                "snapshot_id": snapshot_id,
                "object_proof": object_proof,
                "lease": lease,
                "payload_prune": prune,
            }
            receipt = write_edge_receipt(receipt_root, payload)
            _print(payload | {"receipt": str(receipt)})
            return 0
    except (OSError, SnapshotError, ValueError, urllib.error.URLError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
