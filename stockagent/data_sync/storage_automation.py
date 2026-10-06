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
    "nas_backup": "stockagent-backup-stream",
    "control_backup": "stockagent-control-backup",
    "code_verification": "stockagent-control-release-verification",
    "cold_transport": "stockagent-packed-transport",
    "cold_scan_retry": "stockagent-d-cold-scan-retry",
    "completed_training_return": "stockagent-remote-cold-artifact-ingress",
    "legacy_main": "stockagent-legacy-return@main",
    "legacy_partitions": "stockagent-legacy-return@partitions",
    "legacy_bulk": "stockagent-vast-bulk-return",
    "local_artifact_retirement": "stockagent-enrolled-artifact-retirement",
    "hot_cache_gc": "stockagent-data-cache-gc",
    "compiler_cache_gc": "stockagent-storage-pressure",
    "temporal_server": "stockagent-temporal",
    "lake_lifecycle": "stockagent-storage-lifecycle",
    "lifecycle_control_backup": "stockagent-lifecycle-control-backup",
    "lake_transport_gc": "stockagent-lake-transport-gc",
}
DAEMON_JOBS = {'temporal_server', 'lake_lifecycle'}
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
        if name in DAEMON_JOBS:
            result[name].update(scheduler='systemd-service', scheduler_active=service.get('ActiveState') == 'active',
                                scheduler_enabled=service.get('UnitFileState') == 'enabled')
    return result


def lakehouse_readiness(*, policy=Path('/etc/stockagent/lakehouse-control.json'), repo_root=None,
                       now=None, observations=None, probe=None):
    """Selected control health; backlog and full archive coverage stay separate."""
    if not policy.exists() and not policy.is_symlink():
        return {'required': False, 'ready': True}
    now = time.time() if now is None else now
    try:
        info = policy.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError('unsafe policy')
        c = json.loads(policy.read_bytes())
        jobs = {name: PENGUIN_JOBS[name] for name in ('temporal_server', 'lake_lifecycle', 'lifecycle_control_backup', 'lake_transport_gc')}
        owners = systemd_observations(jobs) if observations is None else observations
        owners_ready = all(owners.get(name, {}).get('scheduler_active') is True and
                           owners.get(name, {}).get('scheduler_enabled') is True for name in jobs)
        base = Path(c['state_root'])
        catalog = bounded_receipt(base / 'catalog-status.json', ('state', 'observed_at_utc', 'snapshot_id', 'unavailable_release_count'), now=now)
        source = bounded_receipt(base / 'source-replication-status.json', ('state', 'observed_at_utc', 'available_object_bytes', 'nas_archive_covered_bytes', 'pending_delivery_ids'), now=now)
        relay = bounded_receipt(Path(c['receipt_root']) / 'relay-status.json', ('state', 'observed_at_utc', 'producer_device_id', 'receiver_device_id',
                         'nas_mount_guard_verified', 'single_owner_verified', 'runtime_lock_verified'), now=now)
        from datetime import datetime, timezone
        def fresh(receipt):
            value = receipt.get('values', {})
            timestamp = datetime.fromisoformat(value.get('observed_at_utc', ''))
            return timestamp.tzinfo is not None and 0 <= now - timestamp.timestamp() <= 900
        receipts_fresh = all(fresh(r) for r in (catalog, source, relay))
        relay_value = relay.get('values', {})
        relay_identity = (all(relay_value.get(k) == c[k] for k in ('producer_device_id', 'receiver_device_id'))
                          and all(relay_value.get(k) is True for k in ('nas_mount_guard_verified', 'single_owner_verified', 'runtime_lock_verified')))
        if probe is None:
            root = repo_root or Path(__file__).resolve().parents[2]
            command = subprocess.run(['bash', str(root / 'scripts/run_lakehouse_control.sh'), 'status'], text=True,
                                     capture_output=True, timeout=20, check=True)
            live = json.loads(command.stdout.splitlines()[-1])
        else:
            live = probe()
        worker = bounded_receipt(base / 'worker-ready.json', ('code_identity_sha256',), now=now)
        same_code = worker.get('values', {}).get('code_identity_sha256') == live.get('worker_code_identity_sha256')
        workflows_ready = (live.get('status') == 'RUNNING' and live.get('source_replication_workflow', {}).get('status') == 'RUNNING'
                           and bool(live.get('worker_code_identity_sha256')) and same_code)
        return {'required': True, 'ready': owners_ready and receipts_fresh and relay_identity and workflows_ready,
                'owners_ready': owners_ready, 'receipts_fresh': receipts_fresh, 'relay_pairing_and_guards_verified': relay_identity,
                'workflow_queries_verified': workflows_ready, 'loaded_worker_matches_current_code': same_code,
                'owners': owners, 'catalog': catalog, 'source_replication': source, 'relay': relay, 'live': live,
                'full_history_archive_verified': False}
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError) as error:
        return {'required': True, 'ready': False, 'error_type': type(error).__name__}


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
        receipts["nas_backup"] = bounded_receipt(
            state_base / "stockagent/backup-stream/status.json",
            ("state", "observed_at_utc", "available_file_count", "available_bytes",
             "nas_covered_file_count", "nas_covered_bytes", "pending_delivery_count", "pending_bytes",
             "receipt_errors", "readiness_error", "auxiliary_error", "all_history_backup_verified",
             "canonical_reconstruction_verified", "automatic_recovery", "parallel_receiver", "combined_nas_coverage"), now=now)
        receipts["control_backup"] = bounded_receipt(
            state_base / "stockagent/control-plane/backups/latest.json",
            ("state", "observed_at_utc", "bytes", "sha256", "logical_state_identity_sha256",
             "logical_state_file_sha256", "same_mvcc_snapshot_as_dump", "archive_directory_readable"), now=now)
        receipts["code_verification"] = bounded_receipt(
            state_base / "stockagent/control-release-verification/status.json",
            ("state", "observed_at_utc", "registered_release_count", "verified_release_count",
             "work_states", "attempts_executed", "complete_workflow_seconds", "errors"), now=now)
        lake_state = state_base / 'stockagent/lakehouse-control'
        receipts['lake_lifecycle'] = bounded_receipt(lake_state / 'catalog-status.json',
            ('state', 'observed_at_utc', 'snapshot_id', 'release_count', 'unavailable_release_count',
             'complete_workflow_seconds', 'changed'), now=now)
        receipts['immutable_source_archive'] = bounded_receipt(lake_state / 'source-replication-status.json',
            ('state', 'observed_at_utc', 'available_object_count', 'available_object_bytes',
             'nas_archive_covered_objects', 'nas_archive_covered_bytes', 'pending_delivery_ids',
             'transport_cache_reclaimed_bytes', 'source_deletion_enabled', 'pending_bytes',
             'maximum_pending_deliveries', 'maximum_wave_bytes'), now=now)
        receipts['lake_relay'] = bounded_receipt(Path('/srv/stockagent-backup-receipts-lab203/lakehouse/relay-status.json'),
            ('state', 'observed_at_utc', 'nas_mount_guard_verified', 'runtime_lock_verified',
             'single_owner_verified', 'automatic_archive_deletion'), now=now)
        receipts['lifecycle_control_backup'] = bounded_receipt(lake_state / 'traditional-backup-status.json',
            ('state', 'observed_at_utc', 'delivery_identity_sha256', 'databases', 'last_nas_acceptance'), now=now)
        receipts['lake_transport_gc'] = bounded_receipt(lake_state / 'transport-gc-status.json',
            ('state', 'observed_at_utc', 'delivery_identity_sha256', 'retirement', 'complete_workflow_seconds'), now=now)
        receipts["completed_training_return"] = bounded_receipt(
            state_base / "stockagent-cold-artifacts/remote-ingress-status.json",
            ("state", "phase", "observed_at_epoch", "automation_contract", "current_root", "source_files", "source_logical_bytes",
             "observed", "eligible", "published", "source_roots_retired", "remote_reclaimed_bytes", "deferred_sources",
             "waiting_return", "complete_workflow_seconds", "error_type"), now=now)
        receipts["pending_return"] = bounded_receipt(
            state_base / "stockagent-cold-artifacts/training-return-waiting-peer.json",
            ("relative_root", "dataset", "snapshot_id", "manifest_sha256"), now=now)
        receipts["legacy_bulk"] = bounded_receipt(
            state_base / "stockagent-vast-bulk-return/status.json",
            ("state", "exit_code", "observed_at_epoch", "complete_workflow_seconds", "full_history_verified"), now=now)
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
    archive = value['receipts'].get('immutable_source_archive', {}).get('values', {})
    combined = value['receipts'].get('nas_backup', {}).get('values', {}).get('combined_nas_coverage', {})
    if combined.get('contract') == 'deduplicated_nas_file_coverage_v1':
        total = combined['available_bytes']
        complete = combined['nas_covered_bytes']
        print(f"NAS 去重覆蓋：{complete/1e9:.3f} / {total/1e9:.3f} GB；"
              f"{100*complete/total if total else 0:.2f}%；"
              f"尚缺 {combined['pending_bytes']/1e9:.3f} GB／{combined['pending_file_count']} 檔")
    if archive:
        print(f"NAS immutable archive：{archive.get('nas_archive_covered_bytes', 0)/1e9:.3f} / "
              f"{archive.get('available_object_bytes', 0)/1e9:.3f} GB；"
              f"{archive.get('nas_archive_covered_objects', 0)} / {archive.get('available_object_count', 0)} 物件；"
              f"{len(archive.get('pending_delivery_ids', []))} 批待驗收")
        print("  原 Restic 覆蓋另列，不相加成去重後的全歷史覆蓋。")
    print("排程啟用 ≠ 冷恢復驗證完成；不自動解壓，不刪唯一冷資料。")
