#!/usr/bin/env python3
"""Plan or apply D-backed rolling retention for penguin's packed store."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    atomic_write_bytes,
    atomic_write_json,
)
from stockagent.data_sync.packed_retention import RetentionConfig, apply_plan, build_plan


def _syncthing(config: RetentionConfig) -> dict:
    candidates = [Path("/root/.local/state/syncthing/config.xml"), Path("/root/.config/syncthing/config.xml")]
    path = next((item for item in candidates if item.exists()), None)
    if path is None:
        return {"ok": False, "error": "Syncthing config not found"}
    xml = ET.parse(path).getroot()
    key = xml.findtext("./gui/apikey")
    address = xml.findtext("./gui/address") or "127.0.0.1:8384"
    host, _, port = address.rpartition(":")
    base = f"http://127.0.0.1:{port or '8384'}"

    def get(endpoint: str, **query):
        suffix = "?" + urllib.parse.urlencode(query) if query else ""
        request = urllib.request.Request(base + endpoint + suffix, headers={"X-API-Key": key})
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)

    devices = {item["name"]: item["deviceID"] for item in get("/rest/config/devices") if item.get("name")}
    connections = get("/rest/system/connections").get("connections", {})
    status = get("/rest/db/status", folder=config.folder_id)
    folder_errors = get("/rest/folder/errors", folder=config.folder_id) or {}
    system_errors = get("/rest/system/error") or {}
    local_checks = {
        "folder_idle": status.get("state") == "idle",
        "need_bytes_zero": int(status.get("needBytes", -1)) == 0,
        "need_items_zero": int(status.get("needTotalItems", -1)) == 0,
        "need_deletes_zero": int(status.get("needDeletes", -1)) == 0,
        "errors_zero": int(status.get("errors", -1)) == 0,
        "pull_errors_zero": int(status.get("pullErrors", -1)) == 0,
        "watch_error_empty": not status.get("watchError"),
        "folder_errors_empty": not folder_errors.get("errors"),
        "system_errors_empty": not system_errors.get("errors"),
    }
    peers = []
    for name in config.required_peer_names:
        device = devices.get(name)
        if not device:
            peers.append({"name": name, "ok": False, "error": "device not configured"})
            continue
        completion = get("/rest/db/completion", folder=config.folder_id, device=device)
        connection = connections.get(device, {})
        checks = {
            "connected": bool(connection.get("connected")),
            "completion_100": float(completion.get("completion", -1)) == 100.0,
            "need_bytes_zero": int(completion.get("needBytes", -1)) == 0,
            "need_items_zero": int(completion.get("needItems", -1)) == 0,
            "need_deletes_zero": int(completion.get("needDeletes", -1)) == 0,
            "remote_state_valid": completion.get("remoteState") == "valid",
        }
        peers.append({"name": name, "ok": all(checks.values()), "checks": checks,
                      "completion": completion.get("completion"), "needBytes": completion.get("needBytes"),
                      "remoteState": completion.get("remoteState"), "transport": connection.get("type")})
    return {"ok": all(local_checks.values()) and all(item["ok"] for item in peers),
            "checked_at": datetime.now(timezone.utc).isoformat(), "local_checks": local_checks, "peers": peers}


def _summary(plan: dict, *, receipt: Path, peer: dict) -> dict:
    return {
        key: plan[key]
        for key in (
            "mode",
            "manifest_count",
            "current_head_count",
            "protected_release_count",
            "retained_release_count",
            "snapshot_reference_occurrence_bytes",
            "unique_stored_object_bytes",
            "content_address_reuse_bytes",
            "candidate_objects",
            "candidate_manifests",
            "reclaimable_bytes",
            "reclaimable_allocated_bytes",
            "archive_proven_objects",
            "archive_failure_count",
            "blockers",
            "plan_fingerprint",
        )
    } | {
        "peer_convergence": peer,
        "plan_receipt": str(receipt),
        "apply_ready": not plan["blockers"] and peer.get("ok", False),
    }


def _install_service(cfg: RetentionConfig, config_path: Path) -> dict:
    canonical = REPO_ROOT / "configs/data_sync/packed_retention.json"
    if config_path.resolve() != canonical.resolve():
        raise SnapshotError("service installation requires the canonical configuration")
    if cfg.sync_root != Path("/srv/stockagent-packed"):
        raise SnapshotError("service installation requires the canonical penguin cold store")
    if any(char in str(REPO_ROOT) for char in ('"', "%", "\\", "\n")):
        raise SnapshotError("unsupported characters in systemd repository path")
    installed = []
    for name in ("stockagent-packed-retention.service", "stockagent-packed-retention.timer"):
        template = REPO_ROOT / "services/systemd" / name
        destination = Path("/etc/systemd/system") / name
        rendered = template.read_text().replace("@REPO_ROOT@", str(REPO_ROOT)).encode()
        if destination.exists() and b"StockAgent rolling packed retention" not in destination.read_bytes():
            raise SnapshotError(f"refusing to replace unrelated unit: {destination}")
        atomic_write_bytes(destination, rendered)
        installed.append(str(destination))
    subprocess.run(["systemd-analyze", "verify", *installed], check=True)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", "--now", "stockagent-packed-retention.timer"], check=True)
    subprocess.run(["systemctl", "start", "stockagent-packed-retention.service"], check=True)
    return {"installed": installed, "timer_enabled": True, "initial_reconcile_started": True}


def _wait_for_convergence(config: RetentionConfig, timeout_seconds: int) -> dict:
    deadline = time.monotonic() + timeout_seconds
    last: dict = {"ok": False, "error": "Syncthing has not been checked"}
    while True:
        try:
            last = _syncthing(config)
        except Exception as exc:
            last = {"ok": False, "error": str(exc)}
        if last.get("ok") or time.monotonic() >= deadline:
            return last
        time.sleep(min(10, max(0.1, deadline - time.monotonic())))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "apply", "status", "install-service"))
    parser.add_argument("--config", type=Path, default=REPO_ROOT / "configs/data_sync/packed_retention.json")
    parser.add_argument(
        "--defer-if-blocked",
        action="store_true",
        help="apply only: report a healthy deferred run instead of failing when a safety gate is closed",
    )
    parser.add_argument(
        "--post-wait-seconds",
        type=int,
        help="apply only: override the configured post-delete Syncthing convergence wait",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="status only: include every candidate and diagnostic instead of a compact receipt summary",
    )
    args = parser.parse_args()
    cfg = RetentionConfig.load(args.config, repo_root=REPO_ROOT)
    if args.defer_if_blocked and args.command != "apply":
        parser.error("--defer-if-blocked requires apply")
    if args.post_wait_seconds is not None and (
        args.command != "apply" or args.post_wait_seconds < 0
    ):
        parser.error("--post-wait-seconds requires apply and a non-negative value")
    if args.full and args.command != "status":
        parser.error("--full requires status")
    if args.command == "install-service":
        print(json.dumps(_install_service(cfg, args.config), ensure_ascii=False, indent=2))
        return 0
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    if args.command == "status":
        latest = cfg.state_dir / "latest-plan.json"
        applied = cfg.state_dir / "last-apply.json"
        latest_value = json.loads(latest.read_text()) if latest.exists() else None
        applied_value = json.loads(applied.read_text()) if applied.exists() else None
        if not args.full and latest_value is not None:
            latest_value = {
                key: latest_value.get(key)
                for key in (
                    "created_at",
                    "mode",
                    "manifest_count",
                    "current_head_count",
                    "protected_release_count",
                    "retained_release_count",
                    "unique_stored_object_bytes",
                    "content_address_reuse_bytes",
                    "candidate_objects",
                    "candidate_manifests",
                    "reclaimable_bytes",
                    "reclaimable_allocated_bytes",
                    "archive_proven_objects",
                    "archive_failure_count",
                    "blockers",
                    "plan_fingerprint",
                    "peer_convergence",
                )
            }
        if not args.full and applied_value is not None:
            applied_value = {
                key: value
                for key, value in applied_value.items()
                if key != "removed_paths"
            }
        result = {
            "latest_plan": latest_value,
            "last_apply": applied_value,
            "full_receipts": str(cfg.state_dir),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    try:
        peer = _syncthing(cfg)
    except Exception as exc:
        peer = {"ok": False, "error": str(exc)}
    plan = build_plan(cfg)
    plan["peer_convergence"] = peer
    if not peer.get("ok"):
        plan["blockers"].append("all intended Syncthing peers must converge before C retention")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    receipt = cfg.state_dir / "plans" / f"rolling-retention-{stamp}.json"
    atomic_write_json(receipt, plan)
    atomic_write_json(cfg.state_dir / "latest-plan.json", plan)
    summary = _summary(plan, receipt=receipt, peer=peer)
    if args.command == "plan":
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    if plan["blockers"]:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if args.defer_if_blocked else 2
    active = []
    services = ("stockagent-packed-backup.service", "syncthing@root.service")
    result = None
    try:
        for service in services:
            if subprocess.run(["systemctl", "is-active", "--quiet", service]).returncode == 0:
                active.append(service)
                subprocess.run(["systemctl", "stop", service], check=True)
        result = apply_plan(cfg, plan["plan_fingerprint"], peer_proof=peer)
    finally:
        for service in reversed(active):
            subprocess.run(["systemctl", "start", service], check=False)
    if result is None:
        raise SnapshotError("retention did not produce an apply receipt")
    wait_seconds = (
        cfg.post_apply_convergence_timeout_seconds
        if args.post_wait_seconds is None
        else args.post_wait_seconds
    )
    post_peer = _wait_for_convergence(cfg, wait_seconds)
    result["post_delete_peer_convergence"] = post_peer
    result["post_delete_converged"] = bool(post_peer.get("ok"))
    atomic_write_json(cfg.state_dir / "last-apply.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if post_peer.get("ok") else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, SnapshotError, subprocess.SubprocessError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
