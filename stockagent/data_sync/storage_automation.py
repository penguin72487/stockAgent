"""Read-only view of the existing cold-delivery and safe-retirement owners.

This is not a scheduler or deletion owner. Observe timers/cron and bounded
receipts separately from live transport and exact data-recovery acceptance.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import stat
import subprocess
import time
from typing import Any

PENGUIN_JOBS = {
    "cold_transport": "stockagent-packed-transport",
    "cold_scan_retry": "stockagent-d-cold-scan-retry",
    "completed_training_return": "stockagent-remote-cold-artifact-ingress",
    "legacy_main": "stockagent-legacy-return@main",
    "legacy_partitions": "stockagent-legacy-return@partitions",
    "local_artifact_retirement": "stockagent-enrolled-artifact-retirement",
    "hot_cache_gc": "stockagent-data-cache-gc",
    "compiler_cache_gc": "stockagent-storage-pressure",
}
RECEIPT_LIMIT = 16 * 1024 * 1024


def bounded_receipt(path: Path, fields: tuple[str, ...], *, now: float) -> dict[str, Any]:
    result: dict[str, Any] = {"path": str(path), "read_state": "missing"}
    try:
        before = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size > RECEIPT_LIMIT:
            return {**result, "read_state": "unsafe-or-oversized"}
        value = json.loads(path.read_bytes())
        after = path.lstat()
        if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            return {**result, "read_state": "changed-during-read"}
        if not isinstance(value, dict):
            return {**result, "read_state": "invalid"}
        return {**result, "read_state": "read", "receipt_age_seconds": max(0, now - before.st_mtime),
                "values": {name: value[name] for name in fields if name in value}}
    except FileNotFoundError:
        return result
    except (OSError, ValueError):
        return {**result, "read_state": "unreadable-or-invalid"}


def systemd_observations(jobs: dict[str, str]) -> dict[str, dict[str, Any]]:
    properties = ("Id", "LoadState", "ActiveState", "SubState", "UnitFileState", "Result",
                  "ExecMainStatus", "StateChangeTimestamp", "NextElapseUSecRealtime")
    units = [base + suffix for base in jobs.values() for suffix in (".timer", ".service")]
    try:
        output = subprocess.run(["systemctl", "show", *units, "--property=" + ",".join(properties)],
                                text=True, capture_output=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired):
        output = ""
    observed = {}
    for block in output.strip().split("\n\n"):
        row = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        if row.get("Id") in units:
            observed[row["Id"]] = row
    result = {}
    for name, base in jobs.items():
        timer = observed.get(base + ".timer", {})
        service = observed.get(base + ".service", {})
        result[name] = {"owner": base, "scheduler": "systemd",
                        "scheduler_active": timer.get("ActiveState") == "active",
                        "scheduler_enabled": timer.get("UnitFileState") == "enabled",
                        "timer": timer, "service": service}
    return result


def cron_observation(path: Path, command: Path, *, proc_root: Path, arguments: tuple[str, ...] = ()) -> dict[str, Any]:
    result: dict[str, Any] = {"scheduler": "cron", "path": str(path), "schedule": None,
                              "scheduler_active": False, "scheduler_enabled": False}
    try:
        info = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022 or info.st_size > 8192:
            return {**result, "config_state": "unsafe"}
        # Never expose the raw command or environment: only accept the known
        # wrapper at its exact configured path and retain the cron schedule.
        matches = []
        for line in path.read_text().splitlines():
            fields = line.split()
            if (len(fields) >= 7 + len(arguments) and fields[5] == "root" and fields[6] == str(command)
                    and tuple(fields[7:7+len(arguments)]) == arguments and "--force" not in fields):
                matches.append(" ".join(fields[:5]))
        if len(matches) != 1:
            return {**result, "config_state": "missing-or-ambiguous-command"}
        running = False
        for process in proc_root.glob("[0-9]*"):
            try:
                if (process / "comm").read_text().strip() in {"cron", "crond"}:
                    running = True
                    break
            except OSError:
                continue
        return {**result, "config_state": "configured", "schedule": matches[0],
                "scheduler_enabled": True, "scheduler_active": running}
    except (OSError, ValueError):
        return {**result, "config_state": "missing-or-unreadable"}


def automation_status(
    repo_root: Path, sync_root: Path, *, state_base: Path = Path("/var/lib"),
    etc_root: Path = Path("/etc"), proc_root: Path = Path("/proc"),
    systemd_root: Path = Path("/run/systemd/system"), live: bool = False,
) -> dict[str, Any]:
    now = time.time()
    edge = (state_base / "stockagent-packed-edge/state.json").is_file()
    role = "index-only-edge" if edge else "cold-authority"
    if edge:
        jobs = {
            "hot_cache_gc": cron_observation(etc_root / "cron.d/stockagent-data-cache-gc",
                repo_root / "scripts/run_data_cache.sh", proc_root=proc_root, arguments=("gc",)),
            "compiler_cache_gc": cron_observation(etc_root / "cron.d/stockagent-storage-pressure",
                repo_root / "scripts/run_storage_pressure_maintenance.sh", proc_root=proc_root),
        }
    else:
        jobs = systemd_observations(PENGUIN_JOBS) if systemd_root.is_dir() else {
            name: {"owner": base, "scheduler": "unavailable", "scheduler_active": False,
                   "scheduler_enabled": False} for name, base in PENGUIN_JOBS.items()}
    mount: dict[str, Any] = {"state": "not-applicable-on-edge"}
    if not edge:
        try:
            from stockagent.data_sync.cold_primary import _check_d_primary_mount
            _check_d_primary_mount(sync_root)
            mount = {"state": "guard-passed", "cold_copy_location": "penguin D"}
        except (OSError, ValueError, RuntimeError) as error:
            mount = {"state": "guard-failed", "error_type": type(error).__name__}
    receipts = {}
    if not edge:
        receipts["completed_training_return"] = bounded_receipt(
            state_base / "stockagent-cold-artifacts/remote-ingress-status.json",
            ("state", "phase", "observed_at_epoch", "automation_contract", "current_root", "source_files", "source_logical_bytes",
             "observed", "eligible", "published", "source_roots_retired", "remote_reclaimed_bytes", "deferred_sources",
             "waiting_return", "complete_workflow_seconds", "error_type"), now=now)
        receipts["pending_return"] = bounded_receipt(
            state_base / "stockagent-cold-artifacts/training-return-waiting-peer.json",
            ("relative_root", "dataset", "snapshot_id", "manifest_sha256"), now=now)
        for name, directory in (("legacy_main", "stockagent-vast-legacy-return"),
                                ("legacy_partitions", "stockagent-vast-legacy-return-partitions")):
            receipts[name] = bounded_receipt(state_base / directory / "summary.json",
                ("state", "updated_at_epoch", "current_root", "counts", "cold_verified_source_bytes", "remote_reclaimed_bytes"), now=now)
    pressure = state_base / "stockagent-storage-pressure/receipts"
    if pressure.is_dir() and not pressure.is_symlink():
        latest = max((p for p in pressure.iterdir() if p.name.startswith("storage-pressure-apply-") and p.suffix == ".json"),
                     key=lambda p: p.name, default=None)
        if latest:
            receipts["compiler_cache_gc"] = bounded_receipt(latest,
                ("completed_at", "cache_scope", "enrolled_policy_sha256", "apply", "inventory_complete", "scan_skipped_reason",
                 "deferred_reason", "deleted_files", "deleted_allocated_bytes", "skipped_changed", "skipped_open", "errors"), now=now)
    receipts["compiler_policy"] = bounded_receipt(etc_root / "stockagent/storage-pressure.json",
        ("schema_version", "scope", "min_age_days", "high_watermark_percent", "target_percent"), now=now)
    st = os.statvfs(repo_root)
    transport: dict[str, Any] = {"state": "not-live-checked"}
    if live:
        try:
            from scripts.configure_artifact_ingress_syncthing import credentials
            from scripts.manage_packed_edge import _convergence
            base, key = credentials()
            transport = {"state": "live-observed", "observed_at_epoch": time.time(),
                         **_convergence(base, key, "stockagent-packed", "penguin" if edge else "vastai1T")}
        except (OSError, ValueError, RuntimeError, KeyError) as error:
            transport = {"state": "live-check-failed", "ok": False, "error_type": type(error).__name__}
    return {"schema_version": 1, "observed_at_epoch": now, "host": socket.gethostname(),
            "role": role, "authority": "penguin", "cold_primary": mount,
            "filesystem": {"total_bytes": st.f_blocks*st.f_frsize, "available_bytes": st.f_bavail*st.f_frsize},
            "jobs": jobs, "receipts": receipts, "transport": transport,
            "proof_scope": "read-only local scheduler/receipt observation; live transport only when requested; not whole cold recovery or full migration acceptance",
            "automatic_materialization": False, "cold_object_gc": False}


def print_human_status(value: dict[str, Any]) -> None:
    space = value["filesystem"]
    print(f"{value['host']} | {value['role']} | 可用 {space['available_bytes']/1e9:.2f} GB")
    print(f"冷庫：{value['cold_primary']['state']}；同步：{value['transport']['state']}")
    transport = value["transport"]
    if transport["state"] == "live-observed":
        print(f"同步檢查：{'通過' if transport['ok'] else '未通過'}；{transport['folder_state']}；{transport['completion']}%；{transport['transport']}")
    for name, job in value["jobs"].items():
        schedule = "已啟用" if job["scheduler_active"] and job["scheduler_enabled"] else "未確認／未啟用"
        receipt = value["receipts"].get(name, {})
        row = receipt.get("values", {})
        state = row.get("deferred_reason") or row.get("scan_skipped_reason") or row.get("state") or "需查看執行收據"
        runtime = job.get("service", {})
        if runtime.get("ActiveState") in {"active", "activating"}:
            state = "執行中／等待既有 owner" + ("；" + row["phase"] if row.get("phase") else "")
        elif runtime.get("Result") == "exec-condition":
            state = "條件檢查跳過（已有 worker／保護項目）"
        elif runtime.get("Result") not in {None, "success"}:
            state = "上次排程需檢查：" + runtime["Result"]
        print(f"{name}: {schedule} ({job['scheduler']})；{state}")
        if row.get("current_root"):
            print(f"  在途：{row['current_root']}；已回收 {row.get('remote_reclaimed_bytes',0)/1e9:.2f} GB")
        if "deleted_allocated_bytes" in row:
            print(f"  上次回收 {row['deleted_allocated_bytes']/1e9:.2f} GB；{row.get('deleted_files',0)} 檔")
        if receipt.get("read_state") == "read":
            print(f"  收據距今 {receipt['receipt_age_seconds']/60:.1f} 分鐘（非持續 heartbeat）")
    print("排程啟用 ≠ 冷恢復驗證完成；不自動解壓，不刪唯一冷資料。")
