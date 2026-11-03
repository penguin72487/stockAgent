"""Read-only fleet storage telemetry, outside the HTTP and acquisition paths.

Capacity is cheap and sampled every minute. A single low-priority worker runs
bounded GNU du censuses independently; it never hashes payload, deletes files,
opens databases, imports a model, or treats a directory name as retirement proof.
The published projection contains neither connection details nor absolute paths.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import contextlib
from datetime import datetime, time as datetime_time, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import tempfile
from threading import Lock
import time
from zoneinfo import ZoneInfo

from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.storage_progress import public_status as public_operations
from stockagent.data_sync.storage_progress import metadata as operation_metadata
from stockagent.data_sync.storage_runtime import REMOTE_RUNTIME, public_status as public_runtime

CONTRACT = "stockagent.storage_monitor.v1"
VOLUMES = {
    "penguin-wsl": ("penguin", "C 槽 WSL 服務磁碟", "程式、服務、帳本、資料庫與控制索引"),
    "penguin-c": ("penguin", "Windows C 槽", "Windows 系統與應用程式"),
    "penguin-d": ("penguin", "Windows D 槽 · 主冷庫", "不可變冷庫與封閉資料正本"),
    "penguin-e": ("penguin", "Windows E 槽 · NVMe 工作磁碟", "進冷庫前的下載、增量、封裝、驗證與傳輸"),
    "penguin-workspace": ("penguin", "E 槽 Linux 資料卷", "原生 ext4 工作資料；容量包含在 E 槽內"),
    "vastai1T-disk": ("vastai1T", "Vast 工作磁碟", "遠端訓練；實例回收後不保證留存"),
    "lab203-disk": ("lab203", "lab203 中繼磁碟", "封閉批次、備份與還原暫存"),
    "nas-disk": ("nas", "NAS 歸檔磁碟", "不可變歸檔與加密備份；共享磁碟使用量包含其他使用者"),
}
NODE_LABELS = {"penguin": "Penguin 本機", "vastai1T": "Vast 遠端訓練", "lab203": "lab203 備份中繼", "nas": "NAS 歸檔"}
KINDS = {"project", "source", "cache", "runtime", "materialized", "control", "cold", "results", "other"}
NUMBER_FIELDS = ("total_bytes", "used_bytes", "free_bytes", "available_bytes", "reserved_free_bytes")
MAX_SNAPSHOT_BYTES = 8 * 1024**2
CAPACITY_HISTORY_CONTRACT = "stockagent.storage_capacity_daily_history.v1"
CAPACITY_HISTORY_DAYS = 120
RETIREMENT_WAVE_LOCK = Path('/run/lock/stockagent-wsl-retirement-wave-scheduler.lock')
DATASET_VERSION_OWNER_DIRECTORY = Path('/var/lib/stockagent/lakehouse-control/dataset-versions')
CENSUS_PRIORITY_POLL_SECONDS = 5.0


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def epoch(value) -> float | None:
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.timestamp() if parsed.tzinfo is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def nonnegative(value):
    return value if type(value) is int and 0 <= value <= 2**53 - 1 else None


def read_snapshot(path: Path) -> dict:
    """Bounded, regular local metadata only; absent is unknown, never zero."""
    from stockagent.service_layout import canonical_service_path
    path = canonical_service_path(path)
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("redirected_snapshot")
    with path.open("rb") as stream:
        raw = stream.read(MAX_SNAPSHOT_BYTES + 1)
    if len(raw) > MAX_SNAPSHOT_BYTES:
        raise ValueError("snapshot_too_large")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("snapshot_not_object")
    return value


def capacity(path: str, expected_filesystem: str | None = None) -> dict:
    """Fence mount identity before/after statvfs; do not descend any directory."""
    try:
        before = os.stat(path)
        mounted = subprocess.run(["findmnt", "-J", "-T", path, "-o", "TARGET,FSTYPE"],
            capture_output=True, text=True, timeout=5, check=True)
        mount = json.loads(mounted.stdout)["filesystems"][0]
        if expected_filesystem and mount["fstype"] != expected_filesystem:
            raise ValueError("mount_mismatch")
        space = os.statvfs(path)
        if os.stat(path).st_dev != before.st_dev:
            raise ValueError("mount_changed")
        return {"state": "measured", "observed_at_utc": utc(), "source": "statvfs",
            "total_bytes": space.f_blocks * space.f_frsize,
            "used_bytes": (space.f_blocks - space.f_bfree) * space.f_frsize,
            "free_bytes": space.f_bfree * space.f_frsize,
            "available_bytes": space.f_bavail * space.f_frsize,
            "reserved_free_bytes": (space.f_bfree - space.f_bavail) * space.f_frsize}
    except (OSError, ValueError, KeyError, IndexError, subprocess.SubprocessError) as exc:
        return {"state": "unavailable", "error_code": type(exc).__name__, "attempted_at_utc": utc()}


REMOTE_CAPACITY = """import json,os,datetime
s=os.statvfs('/')
capacity={'state':'measured','observed_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':'ssh_statvfs','total_bytes':s.f_blocks*s.f_frsize,'used_bytes':(s.f_blocks-s.f_bfree)*s.f_frsize,'free_bytes':s.f_bfree*s.f_frsize,'available_bytes':s.f_bavail*s.f_frsize,'reserved_free_bytes':(s.f_bfree-s.f_bavail)*s.f_frsize}
""" + REMOTE_RUNTIME + "\ncapacity['runtime']=runtime\nprint(json.dumps(capacity))\n"


def ssh_prefix(config: dict) -> list[str]:
    """Only the operator-enrolled target/key; no aliases, prompt or host enrollment."""
    target = os.environ.get(config["target_env"], "")
    port = os.environ.get(config["port_env"], "")
    identity = os.environ.get(config["identity_env"], "")
    if (not re.fullmatch(r"[A-Za-z0-9_.-]+@[A-Za-z0-9_.:-]+", target)
            or not port.isdigit() or not 1 <= int(port) <= 65535 or not Path(identity).is_file()):
        raise ValueError("remote_not_enrolled")
    return ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=yes",
            "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2", "-i", identity, "-p", port, target]


def remote_capacity(config: dict) -> dict:
    """Reuse the enrolled probe; retry one transient SSH reset within 15 s."""
    deadline = time.monotonic() + 15
    try:
        command = ssh_prefix(config) + ["python3 -c " + shlex.quote(REMOTE_CAPACITY)]
        for attempt in range(2):
            result = subprocess.run(command, capture_output=True, text=True,
                                    timeout=max(.1, deadline - time.monotonic()))
            if result.returncode:
                transient = result.returncode == 255 and any(text in result.stderr for text in
                    ('Connection reset by peer', 'kex_exchange_identification: read: Connection reset'))
                if attempt == 0 and transient and deadline - time.monotonic() > 1:
                    time.sleep(.25)
                    continue
                raise subprocess.CalledProcessError(result.returncode, command)
            value = json.loads(result.stdout)
            if not valid_capacity(value):
                raise ValueError("invalid_remote_capacity")
            return value
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        return {"state": "unavailable", "error_code": type(exc).__name__, "attempted_at_utc": utc()}


def valid_capacity(value: dict) -> bool:
    if not isinstance(value, dict):
        return False
    total, used, free = (nonnegative(value.get(key)) for key in NUMBER_FIELDS[:3])
    return (total is not None and total > 0 and used is not None and free is not None
            and used + free == total and epoch(value.get("observed_at_utc")) is not None
            and all(nonnegative(value.get(key)) is not None for key in NUMBER_FIELDS)
            and value["available_bytes"] <= free
            and value["reserved_free_bytes"] == free - value["available_bytes"])


def windows_capacity() -> dict:
    """Native capacities and the current distro's backing drive; no file walk."""
    from stockagent.data_sync.windows_cold_io import _windows_process_environment
    # EncodedCommand carries a fixed script; the distro name is a JSON value,
    # never interpolated as PowerShell or shell syntax.
    import base64
    script = r"""$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$request=[Console]::In.ReadToEnd() | ConvertFrom-Json
$disks=@(Get-PSDrive -PSProvider FileSystem | Where-Object {$_.Name -in @('C','D','E')} | ForEach-Object {
  @{drive=$_.Name;used_bytes=[long]$_.Used;free_bytes=[long]$_.Free}
})
$registered=@(Get-ChildItem 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss' -ErrorAction SilentlyContinue | Get-ItemProperty)
$parents=@($registered | Where-Object {$_.DistributionName -eq $request.distribution} | ForEach-Object {
  $base=$_.BasePath -replace '^\\\?\\',''
  if($base -match '^([CDE]):\\'){$Matches[1]}
})
$allocations=@()
try {
  Add-Type -TypeDefinition @'
using System;using System.Runtime.InteropServices;using System.ComponentModel;
public static class StockAgentAllocatedSize {
 [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true,ExactSpelling=true)]
 private static extern uint GetCompressedFileSizeW(string path,out uint high);
 public static ulong Read(string path) {
  uint high;uint low=GetCompressedFileSizeW(path,out high);int error=Marshal.GetLastWin32Error();
  if(low==uint.MaxValue && error!=0)throw new Win32Exception(error);
  return ((ulong)high<<32)|low;
 }
}
'@
  foreach($disk in $disks){
    $total=[long]0;$current=[long]0;$count=0;$currentCount=0;$complete=$true
    foreach($distro in $registered){
      if($distro.Version -ne 2){continue}
      $base=$distro.BasePath -replace '^\\\?\\',''
      if($base -notmatch ('^'+$disk.drive+':\\')){continue}
      $count++
      try {
        $leaf=if($distro.VhdFileName){[string]$distro.VhdFileName}else{'ext4.vhdx'}
        if($leaf -notmatch '^[^\\/:]+\.vhdx$'){throw 'invalid_vhd_leaf'}
        $file=Get-Item -LiteralPath (Join-Path $base $leaf) -Force
        if($file.PSIsContainer -or ($file.Attributes -band [IO.FileAttributes]::ReparsePoint)){throw 'redirected_vhd'}
        $bytes=[long][StockAgentAllocatedSize]::Read($file.FullName)
        $total+=$bytes
        if($distro.DistributionName -eq $request.distribution){$current+=$bytes;$currentCount++}
      }catch{$complete=$false}
    }
    $allocations+=@{drive=$disk.drive;complete=$complete;distribution_count=$count;
      current_distribution_count=$currentCount;current_distribution_allocated_bytes=$current;
      all_distributions_allocated_bytes=$total}
  }
}catch{$allocations=@()}
@{disks=$disks;backing_drives=$parents;wsl_allocations=$allocations} | ConvertTo-Json -Compress -Depth 5
"""
    try:
        env = _windows_process_environment()
        if env is None:
            return {}
        from stockagent.data_sync.windows_cold_io import POWERSHELL
        result = subprocess.run([POWERSHELL, "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand",
            base64.b64encode(script.encode("utf-16le")).decode()],
            input=json.dumps({"distribution": os.environ.get("WSL_DISTRO_NAME", "")}).encode(),
            capture_output=True, timeout=10, env=env, check=True)
        value = json.loads(result.stdout.decode("utf-8-sig"))
        measured = {}
        for row in value["disks"]:
            if row.get("drive") not in {"C", "D", "E"} or any(nonnegative(row.get(k)) is None for k in ("used_bytes", "free_bytes")):
                raise ValueError("invalid_native_volume")
            measured["penguin-" + row["drive"].lower()] = {"state": "measured", "observed_at_utc": utc(), "source": "native_windows",
                "total_bytes": row["used_bytes"] + row["free_bytes"], "used_bytes": row["used_bytes"],
                "free_bytes": row["free_bytes"], "available_bytes": row["free_bytes"], "reserved_free_bytes": 0}
        for row in value.get('wsl_allocations', []):
            identity = 'penguin-' + str(row.get('drive', '')).lower()
            if identity in measured:
                measured[identity]['wsl_allocation'] = row
        drives = set(value.get("backing_drives", []))
        if len(drives) == 1 and drives <= {"C", "D", "E"}:
            measured["contained_in"] = "penguin-" + next(iter(drives)).lower()
        return measured
    except (OSError, ValueError, KeyError, TypeError, SnapshotError, subprocess.SubprocessError):
        return {}


def windows_wsl_accounting(capacity: dict) -> dict:
    """Separate existing VHD allocation from free growth; never credit it as free."""
    raw = capacity.get('wsl_allocation')
    unknown = {'state': 'unknown'}
    if (not isinstance(raw, dict) or raw.get('complete') is not True
            or raw.get('current_distribution_count') != 1):
        return unknown
    keys = ('distribution_count', 'current_distribution_allocated_bytes',
            'all_distributions_allocated_bytes')
    if any(nonnegative(raw.get(key)) is None for key in keys):
        return unknown
    used, free = (nonnegative(capacity.get(key)) for key in ('used_bytes', 'available_bytes'))
    current, total = (raw[key] for key in keys[1:])
    if used is None or free is None or not 1 <= raw['distribution_count'] <= 128 or not 0 < current <= total <= used:
        return unknown
    return {'state': 'measured', 'current_wsl_allocated_bytes': current,
            'other_wsl_allocated_bytes': total-current, 'windows_and_other_used_bytes': used-total,
            'physical_free_bytes': free, 'current_wsl_capacity_ceiling_bytes': current+free,
            'basis': 'registered_vhd_physical_allocations_plus_host_free',
            'capacity_ceiling_includes_existing_allocation': True}


def guarded_volume(volume: dict) -> dict:
    """Receiver observations use the backup owner's exact CIFS identity guard."""
    if volume["id"] != "nas-disk":
        if volume['id'] == 'penguin-workspace':
            from stockagent.data_sync.workspace_volume import check
            check()
        return volume
    from stockagent.data_sync.nas_target import NasTarget
    from stockagent.data_sync.packed_backup import mounted_volume
    nas = NasTarget(Path(volume["nas_configuration"]))
    _, fstype, source = mounted_volume(nas.mount_point)
    if (fstype != "cifs" or source.casefold() != nas.source.casefold() or not nas.user_directory.is_dir()
            or any(p.is_symlink() for p in (nas.mount_point, *nas.mount_point.parents))):
        raise ValueError("nas_mount_guard_failed")
    # Monitoring must still report low capacity, including below the write
    # reserve. The normal NasTarget.check write-admission gate stays unchanged.
    return {**volume, "path": str(nas.user_directory), "filesystem": "cifs", "roots": [
        {"path": str(nas.user_directory), "label": "StockAgent 授權歸檔目錄", "kind": "cold"}]}


def retain_observation(previous: dict, current: dict) -> dict:
    if current.get("state") == "unavailable" and epoch(previous.get("observed_at_utc")) is not None:
        return {**previous, "state": "unavailable", "error_code": current.get("error_code"),
                "attempted_at_utc": current.get("attempted_at_utc")}
    return current


def receipt_capacities(root: Path) -> dict:
    """Existing paired readiness is evidence of free bytes, not NAS total/usage."""
    result = {}
    try:
        from scripts.backup_delivery_receipt import valid_signature, READY, READY_FIELDS
        value = read_snapshot(root / "readiness.json")
        valid_signature(value, READY, fields=READY_FIELDS)
        if (value.get("nas_mount_guard_verified") is not True or value.get("runtime_lock_verified") is not True
                or value.get("single_owner_verified") is not True or epoch(value.get("observed_at_utc")) is None):
            raise ValueError("unverified_readiness")
        for volume, key in (("nas-disk", "nas_free_bytes"), ("lab203-disk", "ingress_free_bytes")):
            if nonnegative(value.get(key)) is None:
                raise ValueError("invalid_readiness_capacity")
            result[volume] = {"state": "partial", "observed_at_utc": value["observed_at_utc"],
                              "source": "paired_backup_readiness", "available_bytes": value[key]}
    except (OSError, ValueError, KeyError, TypeError):
        pass
    # A separate observer receipt does not change the strict backup admission ABI.
    try:
        telemetry = receiver_snapshot(root)
        for row in telemetry.get("volumes", []):
            if row.get("id") in {"lab203-disk", "nas-disk"} and valid_capacity(row.get("capacity") or {}):
                result[row["id"]] = row["capacity"]
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return result


def receiver_snapshot(root: Path) -> dict:
    from scripts.backup_delivery_receipt import valid_signature, READY, READY_FIELDS
    ready = read_snapshot(root / "readiness.json")
    valid_signature(ready, READY, fields=READY_FIELDS)
    value = read_snapshot(root / "storage-status.json")
    valid_signature(value, CONTRACT)
    if (value.get("observer_node_id") != "lab203" or value.get("pairing") != {
            k: ready[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")}):
        raise ValueError("foreign_storage_receipt")
    return value


def receipt_classifications(root: Path) -> dict:
    try:
        return {row["id"]: row["classification"] for row in receiver_snapshot(root).get("volumes", [])
                if row.get("id") in {"lab203-disk", "nas-disk"} and isinstance(row.get("classification"), dict)}
    except (OSError, ValueError, KeyError, TypeError):
        return {}


def clean_label(value: str) -> str:
    # No absolute paths or control characters in the browser projection.
    return re.sub(r"[\x00-\x1f\x7f\ud800-\udfff]", "�", str(value))[:140]


def classify_name(name: str, inherited="other") -> str:
    if name.startswith("data_"):
        return "source"
    if name in {"cache", ".cache", "benchmarks", "benchmark", "tmp", "scratch", "training_cache"}:
        return "cache"
    if name in {"training_prepared", "prepared", "features", "panels"} or name.startswith("research_dataset"):
        return "materialized"
    if name in {"live", "runtime", "service_data", "stockagent-live", "discord_bot"}:
        return "runtime"
    if name in {"markets", "ablations", "datasets", "checkpoints", "results"}:
        return "results"
    if name in {"operations", "data_quality", "catalog", "receipts", "state"}:
        return "control"
    return inherited


def resolve_roots(config: dict, repo: Path) -> list[dict]:
    roots = [{**row, "path": str(row["path"]).replace("{repo}", str(repo))} for row in config.get("roots", [])]
    if config.get("discover_prefix"):
        with os.scandir(config["path"]) as entries:
            for index, entry in enumerate(entries):
                if index >= 4096:
                    raise ValueError("root_discovery_limit")
                if entry.name.startswith(config["discover_prefix"]) and entry.is_dir(follow_symlinks=False):
                    roots.append({"path": entry.path, "label": entry.name, "kind": "cold"})
    if config.get("discover_worktrees"):
        with os.scandir(repo.parent) as entries:
            for index, entry in enumerate(entries):
                if index >= 4096:
                    raise ValueError("root_discovery_limit")
                if entry.name.startswith(repo.name + "-") and entry.is_dir(follow_symlinks=False):
                    roots.append({"path": entry.path, "label": entry.name, "kind": "project"})
    for parent in config.get("discover_project_parents", []):
        device = os.stat(config["path"]).st_dev
        with os.scandir(parent["path"]) as entries:
            for index, entry in enumerate(entries):
                if index >= 4096:
                    raise ValueError("root_discovery_limit")
                if (entry.name.startswith(parent["prefix"]) and entry.is_dir(follow_symlinks=False)
                        and entry.stat(follow_symlinks=False).st_dev == device):
                    roots.append({"path": entry.path, "label": entry.name, "kind": "other"})
    unique = []
    for row in sorted(roots, key=lambda r: (len(Path(r["path"]).parts), r["path"])):
        path = Path(row["path"])
        if any(path == Path(r["path"]) or Path(r["path"]) in path.parents for r in unique):
            continue
        if path.is_dir() and not path.is_symlink():
            unique.append(row)
    return unique


def du_command(roots: list[dict], depth: int) -> list[str]:
    # One invocation per filesystem deduplicates hardlinks across all its roots.
    return ["nice", "-n", "19", "ionice", "-c", "3", "du", "-x", "-B1", "--null",
            "--max-depth=" + str(depth), "--", *(row["path"] for row in roots)]


def census_root_order(roots: list[dict], previous: dict | None) -> list[dict]:
    """Prior bytes are scheduling hints only, never a current measurement.

    Visit unmeasured roots before the known giant tree that exhausted the last
    deadline. Keep one du invocation and parse with the original root IDs.
    Ambiguous labels are not usable hints; ordering cannot expand scan scope.
    """
    hints = {}
    tree = previous.get('tree', []) if isinstance(previous, dict) else []
    for row in tree if isinstance(tree, list) else []:
        if not isinstance(row, dict):
            continue
        label = row.get('label')
        if isinstance(label, str):
            hints.setdefault(label, []).append(nonnegative(row.get('allocated_bytes')))

    def key(row):
        values = hints.get(clean_label(row['label']), [])
        amount = values[0] if len(values) == 1 else None
        return (amount is not None, amount or 0, row['path'])

    return sorted(roots, key=key)


def census_fingerprint(config: dict, volume: dict, remote: dict | None) -> str:
    scope = {"version": 2, "volume": volume, "depth": config["classification_depth"]}
    if remote:
        scope["enrolled_remote"] = [os.environ.get(remote[key], "") for key in ("target_env", "port_env")]
    return hashlib.sha256(json.dumps(scope, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def parse_du(raw: bytes, roots: list[dict], *, max_children: int = 128, allow_missing=False,
             partial_output=False) -> dict:
    nodes = {}
    root_paths = [Path(row["path"]) for row in roots]
    root_indices = {path: index for index, path in enumerate(root_paths)}
    if len(root_indices) != len(root_paths) or any(
            ancestor in root_indices for root in root_paths for ancestor in root.parents):
        raise ValueError("ambiguous_du_root")

    def owner_of(path):
        # Roots are disjoint. Walk each short ancestor chain once rather than
        # constructing it separately for every registered root and record.
        for ancestor in (path, *path.parents):
            index = root_indices.get(ancestor)
            if index is not None:
                return index, ancestor
        raise ValueError("ambiguous_du_root")

    for record in raw.split(b"\0"):
        if not record:
            continue
        amount, separator, path_bytes = record.partition(b"\t")
        if not separator or not amount.isdigit():
            raise ValueError("invalid_du_record")
        path = Path(os.fsdecode(path_bytes))
        owner, root = owner_of(path)
        if path in nodes:
            raise ValueError("ambiguous_du_root")
        relative = path.relative_to(root)
        nodes[path] = {"id": f"r{owner}/" + relative.as_posix(), "label": clean_label(path.name),
                       "allocated_bytes": int(amount), "kind": "other", "children": []}
    if partial_output:
        # du prints a directory only after walking it. Preserve completed
        # disjoint subtrees when timeout occurs before their root record.
        # Missing ancestors contain measured lower bounds, never false totals.
        completed_paths = set(nodes)
        for path in list(completed_paths):
            index, owner = owner_of(path)
            parent = path.parent
            while path != owner and parent not in nodes:
                relative = parent.relative_to(owner)
                nodes[parent] = {"id": f"r{index}/" + relative.as_posix(), "label": clean_label(parent.name),
                    "allocated_bytes": 0, "allocated_bytes_is_lower_bound": True, "kind": "other", "children": []}
                if parent == owner:
                    break
                parent = parent.parent
    deepest_first = sorted(nodes, key=lambda p: len(p.parts), reverse=True)
    if partial_output:
        for path in deepest_first:
            if path.parent in nodes and nodes[path.parent].get("allocated_bytes_is_lower_bound"):
                nodes[path.parent]["allocated_bytes"] += nodes[path]["allocated_bytes"]
    inconsistent = False
    for path in deepest_first:
        if path.parent in nodes:
            nodes[path.parent]["children"].append(nodes[path])
    def finish(node, inherited):
        nonlocal inconsistent
        node["kind"] = classify_name(node["label"], inherited)
        children = sorted(node["children"], key=lambda n: (-n["allocated_bytes"], n["label"]))
        amount = sum(child["allocated_bytes"] for child in children)
        inconsistent |= amount > node["allocated_bytes"]
        for child in children:
            finish(child, node["kind"])
        if len(children) > max_children:
            kept = children[:max_children]
            kept.append({"id": node["id"] + "/@other", "label": f"其餘 {len(children) - max_children} 個目錄",
                         "allocated_bytes": sum(n["allocated_bytes"] for n in children[max_children:]),
                         "allocated_bytes_is_lower_bound": any(n.get("allocated_bytes_is_lower_bound") for n in children[max_children:]),
                         "kind": node["kind"], "children": [], "folded_directories": len(children) - max_children})
            children = kept
        node["children"] = children
        node["direct_or_unexpanded_bytes"] = max(0, node["allocated_bytes"] - amount)
    tree = []
    missing = 0
    for i, root in enumerate(root_paths):
        if root not in nodes:
            if not allow_missing:
                raise ValueError("du_omitted_root")
            missing += 1
            continue
        node = nodes[root]
        node["label"] = clean_label(roots[i]["label"])
        finish(node, roots[i].get("kind", "other"))
        tree.append(node)
    if not tree:
        raise ValueError("du_omitted_all_roots")
    return {"tree": sorted(tree, key=lambda n: -n["allocated_bytes"]), "missing_roots": missing,
            "roots_total": len(roots),
            "roots_completed": sum(not node.get('allocated_bytes_is_lower_bound') for node in tree),
            "allocated_bytes": sum(node["allocated_bytes"] for node in tree),
            "allocated_bytes_is_lower_bound": partial_output or missing > 0,
            "concurrent_change_detected": inconsistent,
            "metric": "allocated_bytes_hardlinks_counted_once_per_scan",
            "scope": "registered_project_roots_only", "reclaimable_bytes": None}


class CensusProgress:
    """Read only completed du records without disturbing its shared FD offset.

    The worker owns the temporary stream. A lock fences its lifetime; pread
    observes at most eight MiB of metadata, never payload or a second walk.
    Progress always remains a provisional lower bound from this single scan.
    """
    def __init__(self):
        self._lock = Lock()
        self._capture = None
        self._consumed = 0

    @contextlib.contextmanager
    def capture(self, stream, roots, started, check_mount):
        with self._lock:
            if self._capture is not None:
                raise ValueError('census_progress_already_owned')
            self._capture = (stream.fileno(), roots, started, check_mount)
            self._consumed = 0
        try:
            yield
        finally:
            with self._lock:
                self._capture = None

    def snapshot(self):
        with self._lock:
            if self._capture is None:
                return None
            descriptor, roots, started, check_mount = self._capture
            try:
                size = os.fstat(descriptor).st_size
                if size <= self._consumed or size > MAX_SNAPSHOT_BYTES:
                    return None
                check_mount()
                raw = os.pread(descriptor, size, 0)
                complete = raw.rfind(b'\0') + 1
                if complete <= self._consumed:
                    return None
                parsed = parse_du(raw[:complete], roots, allow_missing=True, partial_output=True)
                check_mount()
                self._consumed = complete
                return {**parsed, 'state': 'scanning', 'started_at_utc': started,
                        'observed_at_utc': utc(), 'provisional': True}
            except (OSError, ValueError, KeyError, RuntimeError):
                return None  # The final census retains its own typed rejection.


def census(volume: dict, repo: Path, *, depth: int, timeout: int, remote: dict | None = None,
           previous: dict | None = None, progress: CensusProgress | None = None,
           wave_lock: Path | None = None) -> dict:
    """Do not scan a shared source tree during a supervised retirement wave."""
    if wave_lock is None:
        return _census(volume, repo, depth=depth, timeout=timeout, remote=remote,
                       previous=previous, progress=progress)
    try:
        fd = os.open(wave_lock, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        try:
            entry = os.fstat(fd)
            if not stat.S_ISREG(entry.st_mode) or entry.st_uid != os.geteuid():
                raise ValueError('untrusted_retirement_wave_lock')
            try:
                fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                return {'state': 'deferred', 'reason': 'retirement_wave_active'}
            reason = local_census_priority_reason(volume, repo)
            if reason:
                return {'state': 'deferred', 'reason': reason}
            interrupted_by = None
            def preempt():
                nonlocal interrupted_by
                interrupted_by = local_census_priority_reason(volume, repo)
                return interrupted_by is not None
            return _census(volume, repo, depth=depth, timeout=timeout, remote=remote,
                           previous=previous, progress=progress,
                           preempt=preempt, preempt_reason=lambda: interrupted_by)
        finally:
            os.close(fd)
    except (OSError, ValueError) as exc:
        return {'state': 'deferred', 'reason': 'retirement_wave_lock_unobservable',
                'error_code': type(exc).__name__}


def _interruptible_local_census(command, output, errors, timeout, preempt):
    """Reap the owned read-only scan before releasing its shared wave lock."""
    child = subprocess.Popen(command, stdout=output, stderr=errors, start_new_session=True)
    deadline = time.monotonic() + timeout
    def stop():
        try:
            child.terminate()
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                child.kill()
            except ProcessLookupError:
                pass
            child.wait()  # Never release the scan lock with this child alive.
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                stop()
                return None, True, False
            try:
                return child.wait(timeout=min(CENSUS_PRIORITY_POLL_SECONDS, remaining)), False, False
            except subprocess.TimeoutExpired:
                if preempt():
                    stop()
                    return child.returncode, False, True
    except BaseException:
        if child.poll() is None:
            stop()
        raise


def _census(volume: dict, repo: Path, *, depth: int, timeout: int, remote: dict | None = None,
            previous: dict | None = None, progress: CensusProgress | None = None,
            preempt=None, preempt_reason=None) -> dict:
    started = utc()
    try:
        if remote:
            roots = volume["roots"]
            command = ssh_prefix(remote) + [shlex.join([
                "timeout", "--signal=TERM", "--kill-after=5", str(timeout),
                *du_command(census_root_order(roots, previous), depth)])]
        else:
            volume = guarded_volume(volume)
            if capacity(volume["path"], volume.get("filesystem")).get("state") != "measured":
                raise ValueError("classification_mount_unavailable")
            roots = resolve_roots(volume, repo)
            if any(os.stat(root["path"]).st_dev != os.stat(volume["path"]).st_dev for root in roots):
                raise ValueError("root_on_unregistered_filesystem")
            command = du_command(census_root_order(roots, previous), depth)
        if not roots:
            return {"state": "not_scanned", "reason": "no_registered_roots", "tree": []}
        before_device = None if remote else os.stat(volume["path"]).st_dev
        def check_mount():
            if not remote:
                if os.stat(volume['path']).st_dev != before_device:
                    raise ValueError('classification_mount_changed')
                guarded_volume(volume)

        with contextlib.ExitStack() as stack:
            output = stack.enter_context(tempfile.TemporaryFile())
            errors = stack.enter_context(tempfile.TemporaryFile())
            if progress is not None:
                stack.enter_context(progress.capture(output, roots, started, check_mount))
            timed_out = preempted = False
            try:
                if preempt is not None and remote is None:
                    exit_code, timed_out, preempted = _interruptible_local_census(
                        command, output, errors, timeout, preempt)
                else:
                    result = subprocess.run(command, stdout=output, stderr=errors,
                                            timeout=timeout+10 if remote else timeout,
                                            start_new_session=True)
                    exit_code = result.returncode
                    timed_out = remote is not None and exit_code in {124, 137}
            except subprocess.TimeoutExpired:
                # subprocess.run terminates and reaps its own command. Local
                # nice/ionice exec into du, so no local scan child is orphaned.
                timed_out, exit_code = True, None
            if output.tell() > MAX_SNAPSHOT_BYTES or (not timed_out and not preempted and exit_code not in {0, 1}):
                raise ValueError("classification_output_invalid")
            output.seek(0)
            raw = output.read(MAX_SNAPSHOT_BYTES + 1)
            if timed_out or preempted:
                raw = raw[:raw.rfind(b"\0") + 1]  # An unfinished record is not an observation.
            if preempted and not raw:
                return {'state': 'deferred', 'reason': preempt_reason() if preempt_reason else 'retirement_wave_pending'}
            parsed = parse_du(raw, roots, allow_missing=timed_out or preempted or exit_code == 1,
                              partial_output=timed_out or preempted)
        if not remote and os.stat(volume["path"]).st_dev != before_device:
            raise ValueError("classification_mount_changed")
        if not remote:
            guarded_volume(volume)
        return {**parsed, "state": "complete" if not preempted and exit_code == 0 and not parsed["concurrent_change_detected"] else "partial",
                "started_at_utc": started, "observed_at_utc": utc(), "exit_code": exit_code,
                "timed_out": timed_out, "preempted": preempted,
                **({'reason': preempt_reason() if preempt_reason else 'retirement_wave_pending'} if preempted else {})}
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        return {"state": "unavailable", "error_code": type(exc).__name__, "attempted_at_utc": utc(),
                "started_at_utc": started}


def sanitize_tree(rows, *, budget: list[int] | None = None, depth=0, ancestry=()):
    budget = [2500] if budget is None else budget
    if not isinstance(rows, list) or depth > 6:
        return []
    result = []
    for row in rows[:129]:
        if not isinstance(row, dict) or budget[0] <= 0 or nonnegative(row.get("allocated_bytes")) is None:
            continue
        budget[0] -= 1
        identity = hashlib.sha256(json.dumps([*ancestry, str(row.get("id", "")), str(row.get("label", ""))], ensure_ascii=True).encode()).hexdigest()[:24]
        children = sanitize_tree(row.get("children"), budget=budget, depth=depth+1, ancestry=(*ancestry, identity))
        result.append({"id": identity, "label": clean_label(row.get("label", "")),
            "allocated_bytes": row["allocated_bytes"], "kind": row.get("kind") if row.get("kind") in KINDS else "other",
            "allocated_bytes_is_lower_bound": row.get("allocated_bytes_is_lower_bound") is True,
            "direct_or_unexpanded_bytes": max(0, row["allocated_bytes"] - sum(c["allocated_bytes"] for c in children)),
            "children": children})
    return result


def public_efficiency(value: dict) -> dict | None:
    if value.get("contract") != "stockagent.storage_efficiency.v1" or epoch(value.get("observed_at_utc")) is None:
        return None
    return {"observed_at_utc": value["observed_at_utc"],
        **{key: nonnegative(value.get(key)) for key in ("reclaimed_allocated_bytes", "protected_active_bytes", "compression_sample_files")},
        "source_files_deleted": 0 if value.get("source_files_deleted") == 0 else None,
        "cold_objects_deleted": 0 if value.get("cold_objects_deleted") == 0 else None,
        "production_codec_changed": value.get("production_codec_changed") is True}


def public_cold_tiers(value, *, now: float) -> dict | None:
    if not isinstance(value, dict) or value.get("contract") != "stockagent.cold_tiers.v1":
        return None
    observed = value.get("observed_at_epoch")
    if type(observed) not in (int, float) or not 0 < observed <= now + 300:
        return None
    allowed = {"outside_daily_window", "waiting_d_capacity", "waiting_publication_window",
               "bounded_cycle_complete", "partial_failures_preserved", "existing_tier_owner_busy"}
    state = value.get("state") if value.get("state") in allowed else "unknown"
    schedule = value.get("schedule") if isinstance(value.get("schedule"), dict) else {}
    start, end, stable = (schedule.get(key) for key in
        ("daily_window_start_hour", "daily_window_end_hour", "stable_after_hours"))
    schedule = ({"timezone": "Asia/Taipei", "start_hour": start, "end_hour": end, "stable_after_hours": stable}
        if schedule.get("daily_window_timezone") == "Asia/Taipei"
           and type(start) is int and type(end) is int and 0 <= start < end <= 24
           and type(stable) is int and 1 <= stable <= 168 else None)
    last = value.get("last_work_cycle")
    work = None
    if (isinstance(last, dict) and last.get("contract") == "stockagent.cold_tiers.v1"
            and last.get("apply") is True and type(last.get("observed_at_epoch")) in (int, float)
            and 0 < last["observed_at_epoch"] <= now + 300):
        work = {"observed_at_utc": datetime.fromtimestamp(last["observed_at_epoch"], timezone.utc).isoformat(),
                **{key: nonnegative(last.get(key)) for key in
                   ("objects_completed", "logical_bytes_processed", "allocation_bytes_saved", "failed_intents")}}
    return {"state": state, "observed_at_utc": datetime.fromtimestamp(observed, timezone.utc).isoformat(),
            "stale": now - observed > 900, "schedule": schedule, "last_work_cycle": work,
            "scope": "last_bounded_cycle_not_whole_lake_or_NAS", "original_objects_deleted": 0}


def public_status(value: dict, *, now: float | None = None) -> dict:
    """Whitelist every field; last measurement clocks survive failed probes."""
    now = time.time() if now is None else now
    if value.get("contract") != CONTRACT:
        value = {}
    raw_rows = value.get("volumes")
    rows = {r.get("id"): r for r in raw_rows if isinstance(r, dict) and isinstance(r.get("id"), str)} if isinstance(raw_rows, list) else {}
    placement = value.get("service_layout_generation")
    changed = (epoch(placement.get("changed_at_utc")) if isinstance(placement, dict)
               and placement.get("contract") == "stockagent.service_layout_generation.v1" else None)
    if changed is not None and changed > now + 300:
        changed = None
    nodes = {key: {"id": key, "label": label, "volumes": []} for key, label in NODE_LABELS.items()}
    for identity, (owner, label, role) in VOLUMES.items():
        row = rows.get(identity, {})
        cap = row.get("capacity") if isinstance(row.get("capacity"), dict) else {}
        observed = epoch(cap.get("observed_at_utc"))
        age = None if observed is None else max(0, now-observed)
        full = valid_capacity(cap)
        state = "unknown" if observed is None else "stale" if observed > now + 300 or age > 180 else cap.get("state", "unknown")
        projected_cap = {key: nonnegative(cap.get(key)) if full else None for key in NUMBER_FIELDS}
        if not full and cap.get("state") == "partial" and observed is not None:
            projected_cap["available_bytes"] = nonnegative(cap.get("available_bytes"))
        projected_cap.update(state=state, observed_at_utc=cap.get("observed_at_utc") if observed else None,
            age_seconds=round(age) if age is not None else None, complete=full,
            source=cap.get("source") if cap.get("source") in {"statvfs", "ssh_statvfs", "paired_backup_readiness", "native_windows", "guarded_nas_statvfs"} else None)
        group = row.get("classification") if isinstance(row.get("classification"), dict) else {}
        group_time = epoch(group.get("observed_at_utc"))
        tree = sanitize_tree(group.get("tree"))
        group_state = group.get("state") if group.get("state") in {"complete", "partial", "scanning", "unavailable", "not_scanned"} else "not_scanned"
        interval = value.get("classification_interval_seconds", 21600)
        interval = interval if type(interval) is int and 3600 <= interval <= 86400 else 21600
        roots_total, roots_completed = (nonnegative(group.get(key)) for key in ('roots_total', 'roots_completed'))
        if roots_total is None or roots_completed is None or roots_completed > roots_total:
            roots_completed = None
        classification = {"state": group_state, "observed_at_utc": group.get("observed_at_utc") if group_time else None,
            "started_at_utc": group.get("started_at_utc") if epoch(group.get("started_at_utc")) else None,
            "age_seconds": round(max(0, now-group_time)) if group_time else None,
            "stale": group_time is not None and (group_time > now+300 or now-group_time > 2*interval),
            "allocated_bytes": nonnegative(group.get("allocated_bytes")), "tree": tree,
            "allocated_bytes_is_lower_bound": group.get("allocated_bytes_is_lower_bound") is True,
            "roots_total": roots_total, "roots_completed": roots_completed,
            "reclaimable_bytes": None, "metric": "allocated_bytes", "scope": "registered_project_roots_only"}
        classification["layout_changed"] = (identity == "penguin-wsl" and changed is not None
                                             and group_time is not None and changed > group_time)
        if classification["layout_changed"]:
            classification["stale"] = True
        deferred = group.get('deferred_reason')
        classification['deferred_reason'] = deferred if isinstance(deferred, str) and deferred in {
            'retirement_wave_active', 'retirement_wave_pending', 'retirement_wave_lock_unobservable',
            'dataset_version_owner_active', 'dataset_version_owner_unobservable',
            'transport_relocation_owner_active',
            'tw_opening_guard_active', 'tw_opening_guard_unknown'} else None
        nodes[owner]["volumes"].append({"id": identity, "label": label, "role": role, "capacity": projected_cap,
            "classification": classification, "contained_in": row.get("contained_in") if row.get("contained_in") in VOLUMES else None})
    projected = {row["id"]: row for node in nodes.values() for row in node["volumes"]}
    guest = projected["penguin-wsl"]
    backing = projected.get(guest["contained_in"])
    # A VHD's guest free blocks are not unallocated bytes on the Windows host.
    # Derive this only from the whitelisted, fresh observations, never from an
    # injected precomputed field. Do not add guest and host capacity together.
    fresh = lambda cap: cap["complete"] and cap["state"] == "measured"
    writable = None
    if backing is not None and backing["id"] in {"penguin-c", "penguin-d"}:
        if fresh(guest["capacity"]) and fresh(backing["capacity"]):
            writable = min(guest["capacity"]["available_bytes"], backing["capacity"]["available_bytes"])
    guest["writable_headroom"] = {
        "state": "measured" if writable is not None else "unknown",
        "available_bytes": writable,
        "backing_volume_id": backing["id"] if backing is not None else None,
        "basis": "minimum_guest_and_backing_available_bytes",
        "conservative": True,
        "includes_application_reserve": False,
    }
    guest['host_storage_accounting'] = (
        windows_wsl_accounting(rows.get(backing['id'], {}).get('capacity', {}))
        if backing is not None and fresh(backing['capacity']) and fresh(guest['capacity'])
        else {'state': 'unknown'})
    return {"contract": CONTRACT, "read_only": True, "production_control_possible": False,
        "generated_at_utc": value.get("generated_at_utc") if epoch(value.get("generated_at_utc")) else None,
        "capacity_interval_seconds": 60, "classification_interval_seconds": interval,
        "nodes": list(nodes.values()), "aggregate_capacity_bytes": None,
        "efficiency": public_efficiency(value.get("efficiency")) if isinstance(value.get("efficiency"), dict) else None,
        "cold_tiers": public_cold_tiers(value.get("cold_tiers"), now=now),
        "operations": public_operations(value.get("operations"), now=now),
        "runtime": public_runtime(value.get("runtime"), now=now),
        "definitions": {"capacity": "磁碟整體實測容量；NAS 可能與他人共享。未量測不當作零。",
            "classification": "已登錄專案目錄的實際配置空間；單次掃描內 hardlink 只算一次。部分清點的 ≥ 是已量測下限，未掃描不當作零；掃描不是原子快照。",
            "containment": "WSL 位於 Windows 虛擬磁碟內；同磁碟掛載別名與跨節點副本不合併加總。",
            "writable_headroom": "保守寫入餘量取近期客體與 Windows 實體磁碟可用空間的較小值；未估算 VHD 內已配置區塊可重用量，未扣各服務保留量，也不保證其他程式不會同時寫入。缺少近期任一量測時不推測。",
            "host_storage_accounting": "按原生 VHD 實體配置區分 WSL、其他 WSL 及 Windows／其他檔案。WSL 配置上限包含既有占用，不是額外可用空間；客體可用與主機新增配置空間分開呈現。",
            "retirement": "目錄大小與壓縮率不是可刪除空間；需另通過引用、恢復、收據及來源保護檢查。"}}


def load_config(path: Path) -> dict:
    value = read_snapshot(path)
    if value.get("schema_version") != 1 or value.get("observer_node_id") not in {"penguin", "lab203"}:
        raise ValueError("unsupported_observer_configuration")
    for field, low, high in (("capacity_interval_seconds", 60, 60), ("classification_interval_seconds", 3600, 86400),
            ("classification_timeout_seconds", 30, 3600), ("classification_depth", 1, 3)):
        if type(value.get(field)) is not int or not low <= value[field] <= high:
            raise ValueError("invalid_observer_bounds:" + field)
    allowed = {"penguin-wsl", "penguin-c", "penguin-d", "penguin-e", "penguin-workspace"} if value["observer_node_id"] == "penguin" else {"lab203-disk", "nas-disk"}
    ids = [row.get("id") for row in value["volumes"]]
    if len(ids) != len(set(ids)) or not set(ids) <= allowed:
        raise ValueError("foreign_or_duplicate_volume")
    return value


def observe_capacities(config: dict) -> dict:
    measured = {}
    for row in config["volumes"]:
        try:
            volume = guarded_volume(row)
            measured[row["id"]] = capacity(volume["path"], volume.get("filesystem"))
            guarded_volume(volume)
            if row["id"] == "nas-disk" and measured[row["id"]].get("state") == "measured":
                measured[row["id"]]["source"] = "guarded_nas_statvfs"
        except (OSError, ValueError, KeyError, RuntimeError, SnapshotError) as exc:
            measured[row["id"]] = {"state": "unavailable", "error_code": type(exc).__name__, "attempted_at_utc": utc()}
    if config["observer_node_id"] == "penguin":
        measured.update(windows_capacity())
    if config.get("remote"):
        measured[config["remote"]["id"]] = remote_capacity(config["remote"])
    if config.get("receipt_root"):
        measured.update(receipt_capacities(Path(config["receipt_root"])))
    return measured


def priority_retirement_wave_pending(repo: Path) -> bool:
    """Defer a new WSL census for a runnable supervised cleanup, not capacity waits."""
    # The installed timer is not an agent run. Its short nonblocking write-lock
    # attempt can otherwise collide with the same census every minute forever.
    configuration = operation_metadata(repo / 'configs/data_sync/wsl_retirement_queue.json')
    relative = configuration.get('state_root')
    if (configuration.get('contract') == 'stockagent.frozen_retirement_queue.v1'
            and configuration.get('enabled') is True and isinstance(relative, str)
            and not Path(relative).is_absolute() and '..' not in Path(relative).parts
            and Path(relative).is_relative_to('service_data')):
        queued = operation_metadata(repo / relative / 'status.json')
        observed = epoch(queued.get('observed_at_utc'))
        if (queued.get('contract') == 'stockagent.frozen_retirement_queue.v1'
                and observed is not None and -120 <= time.time() - observed <= 180
                and queued.get('all_enrolled_accepted') is False
                and queued.get('state') in {'waiting_existing_wave', 'dispatching_original_owner', 'inspecting'}):
            return True
    from scripts.plan_wsl_retirement_waves import observe_active_supervisors
    active = observe_active_supervisors()
    if active is None:
        return True  # Unknown optional census state never blocks safe retirement.
    for run in active:
        if not any('scripts/run_wsl_retirement_waves.py' in str(arg) for arg in run['argv']):
            continue
        log = repo / 'artifacts/operations/agent-workflow/runs' / run['run_id'] / 'run.log'
        try:
            with log.open('rb') as stream:
                stream.seek(0, os.SEEK_END)
                stream.seek(max(0, stream.tell() - 32768))
                lines = stream.read().splitlines()
            latest = next((json.loads(line) for line in reversed(lines)
                           if line.startswith(b'{') and b'"event"' in line), {})
        except (OSError, ValueError, TypeError):
            return True
        if latest.get('event') != 'waiting_for_capacity':
            return True
    return False


def trading_opening_census_reason(repo: Path, *, now: datetime | None = None) -> str | None:
    """Yield only optional scans to the existing day-trade opening guard.

    The guardian owns the session calendar and protected-window decision. Its
    fresh receipt can release the scan on a closed day. If the receipt is not
    trustworthy, the documented 08:20-09:10 Taipei window is a conservative
    fallback; it does not stop capacity or service observations.
    """
    observed = now or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        raise ValueError('opening_guard_clock_requires_timezone')
    local = observed.astimezone(ZoneInfo('Asia/Taipei'))
    in_fallback_window = datetime_time(8, 20) <= local.time().replace(tzinfo=None) < datetime_time(9, 10)
    receipt = operation_metadata(repo / 'artifacts/operations/tw_day_trade_guardian/latest.json')
    stamp = epoch(receipt.get('observed_at_taipei'))
    components = receipt.get('components')
    guard = components.get('opening_resource_guard') if isinstance(components, dict) else None
    protected = guard.get('protected') if isinstance(guard, dict) else None
    session = receipt.get('session_state')
    if (receipt.get('schema_version') == 1 and receipt.get('session_date') == local.date().isoformat()
            and stamp is not None and -120 <= observed.timestamp() - stamp <= 180
            and session in {'open', 'closed', 'unknown'} and type(protected) is bool):
        if protected:
            return 'tw_opening_guard_active'
        if not in_fallback_window or session == 'closed':
            return None
        # A fresh open/unknown session contradicting the documented window
        # cannot safely release a metadata scan ahead of trading.
    return 'tw_opening_guard_unknown' if in_fallback_window else None


def local_census_priority_reason(volume: dict, repo: Path) -> str | None:
    """Keep optional recursive metadata scans out of a live publication's way.

    This observes the canonical owner's existing lock without creating or
    holding it. Capacity observations are independent and continue every tick.
    A failed observation defers the optional scan, not the publication.
    """
    opening = trading_opening_census_reason(repo)
    if opening:
        return opening
    if volume.get('id') in {'penguin-wsl', 'penguin-d', 'penguin-workspace'}:
        # A recursive du competes with the exact readback/retirement needed
        # to release D. Reuse bounded owner observations, never walk payloads
        # to decide whether another payload walk should start.
        options = operation_metadata(repo/'configs/data_sync/storage_monitor.json').get('operations', {})
        if not isinstance(options, dict):
            options = {}
        root = options.get('workspace_relocation_state_root')
        if isinstance(root, str):
            waiting = operation_metadata(Path(root)/'attempt.json')
            retiring = operation_metadata(Path(root)/'retirement-status.json')
            for observed, states in ((waiting, {'waiting_for_original_owner_or_reader'}),
                                     (retiring, {'verifying_and_retiring'})):
                timestamp = observed.get('observed_at_epoch')
                if (isinstance(timestamp, (int, float)) and not isinstance(timestamp, bool)
                        and -120 <= time.time() - timestamp <= 120
                        and observed.get('state') in states):
                    return 'transport_relocation_owner_active'
            backup_root = options.get('backup_state_root')
            if retiring.get('state') == 'verifying_and_retiring' and isinstance(backup_root, str):
                # A large batch can outlive the progress heartbeat. Observe
                # the existing kernel lock without creating it or holding a
                # lease; a stale counter is not proof the owner has exited.
                try:
                    fd = os.open(Path(backup_root)/'cycle.lock',
                                 os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
                    try:
                        info = os.fstat(fd)
                        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                            return 'retirement_wave_lock_unobservable'
                        try:
                            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                        except BlockingIOError:
                            return 'transport_relocation_owner_active'
                    finally:
                        os.close(fd)
                except OSError:
                    return 'retirement_wave_lock_unobservable'
    if priority_retirement_wave_pending(repo):
        return 'retirement_wave_pending'
    if volume.get('id') in {'penguin-wsl', 'penguin-d'}:
        from stockagent.data_sync.dataset_migration import migration_owner_observation
        try:
            observation = migration_owner_observation(DATASET_VERSION_OWNER_DIRECTORY, None)
        except (OSError, ValueError):
            return 'dataset_version_owner_unobservable'
        if observation['lock_held']:
            return 'dataset_version_owner_active'
    return None


def record_daily_capacity(history_path: Path, volumes: list[dict], *, now: datetime | None = None) -> bool:
    """Keep one fresh physical-capacity observation per volume and Taipei day.

    Net disk usage includes every writer, deletion, backup and compression; it
    is not provider ingestion and nested WSL/C observations cannot be added.
    This small sidecar never performs a filesystem census or participates in
    the published/signed monitoring contract.
    """
    observed = now or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        raise ValueError('capacity_history_clock_requires_timezone')
    timestamp = observed.timestamp()
    today = observed.astimezone(ZoneInfo('Asia/Taipei')).date().isoformat()
    if history_path.is_symlink():
        raise ValueError('redirected_capacity_history')
    try:
        history = read_snapshot(history_path)
    except FileNotFoundError:
        history = {'contract': CAPACITY_HISTORY_CONTRACT, 'timezone': 'Asia/Taipei', 'volumes': {}}
    if (history.get('contract') != CAPACITY_HISTORY_CONTRACT
            or history.get('timezone') != 'Asia/Taipei' or not isinstance(history.get('volumes'), dict)):
        raise ValueError('invalid_capacity_history')
    stored = history['volumes']
    if any(key not in VOLUMES or not isinstance(value, list) or len(value) > CAPACITY_HISTORY_DAYS
           for key, value in stored.items()):
        raise ValueError('invalid_capacity_history_volumes')
    changed = False
    for volume in volumes:
        identity = volume.get('id') if isinstance(volume, dict) else None
        cap = volume.get('capacity') if isinstance(volume, dict) else None
        if identity not in VOLUMES or not isinstance(cap, dict) or cap.get('state') != 'measured' or not valid_capacity(cap):
            continue
        when = epoch(cap.get('observed_at_utc'))
        if (when is None or not -5 <= timestamp - when <= 180
                or datetime.fromtimestamp(when, ZoneInfo('Asia/Taipei')).date().isoformat() != today
                or not isinstance(cap.get('source'), str) or not cap['source']):
            continue
        entries = stored.setdefault(identity, [])
        if any(not isinstance(row, dict) or not isinstance(row.get('date'), str) for row in entries):
            raise ValueError('invalid_capacity_history_rows')
        if any(row['date'] == today for row in entries):
            continue
        entries.append({'date': today, 'observed_at_utc': cap['observed_at_utc'],
                        'source': cap['source'], 'total_bytes': cap['total_bytes'],
                        'used_bytes': cap['used_bytes'], 'available_bytes': cap['available_bytes']})
        entries.sort(key=lambda row: row['date'])
        del entries[:-CAPACITY_HISTORY_DAYS]
        changed = True
    if changed:
        history['updated_at_utc'] = observed.astimezone(timezone.utc).isoformat()
        history['definition'] = 'Disk used-byte observations; net change is not source ingestion. WSL is contained in C.'
        atomic_write_json(history_path, history)
    return changed


def monitor(config_path: Path, output: Path, repo: Path, *, watch=False):
    """One owner lock; capacity keeps running while the census worker is busy."""
    config = load_config(config_path)
    if config["observer_node_id"] == "penguin" and not os.environ.get("WSL_DISTRO_NAME"):
        from stockagent.runtime_identity import enrolled_wsl_distribution
        os.environ["WSL_DISTRO_NAME"] = enrolled_wsl_distribution(repo)
    output.parent.mkdir(parents=True, exist_ok=True)
    lock_path = output.with_suffix(".lock")
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            previous = read_snapshot(output)
        except (OSError, ValueError):
            previous = {}
        rows = {row["id"]: row for row in previous.get("volumes", []) if isinstance(row, dict) and row.get("id") in VOLUMES}
        if any(v['id']=='penguin-workspace' for v in config['volumes']):
            rows.setdefault('penguin-workspace', {'id': 'penguin-workspace'})['contained_in'] = 'penguin-e'
        interrupted = set()
        for identity, row in rows.items():
            group = row.get('classification')
            if isinstance(group, dict) and group.get('state') == 'scanning':
                interrupted.add(identity)
                # A previous process cannot still own a scan under this lock.
                # Preserve only actual observed partial data and its clock.
                row['classification'] = ({**group, 'state': 'partial',
                    'reason': 'previous_census_interrupted'} if isinstance(group.get('tree'), list)
                    and nonnegative(group.get('allocated_bytes')) is not None
                    else {'state': 'not_scanned', 'reason': 'previous_census_interrupted', 'tree': []})
        tier_status = previous.get("cold_tiers")
        operations = previous.get("operations")
        runtime = previous.get("runtime")
        receiver_runtime = {}  # Private in-memory CPU delta; never serialized into a receipt.
        history_path = output.with_name("storage_operations_history.json")
        try:
            operation_history = read_snapshot(history_path)
        except (OSError, ValueError):
            operation_history = {}
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="storage-census") as pool:
            job = None
            job_progress = None
            specs = [(r, None) for r in config["volumes"]]
            if config.get("remote"):
                specs.insert(0, (config["remote"], config["remote"]))
            due = {}
            for volume, _ in specs:
                group = rows.get(volume["id"], {}).get("classification") or {}
                last = epoch(group.get("observed_at_utc"))
                remote = config.get("remote") if volume["id"] == (config.get("remote") or {}).get("id") else None
                compatible = (group.get('state') in {'complete', 'partial'}
                              and group.get("fingerprint") == census_fingerprint(config, volume, remote))
                needs_resume = (volume['id'] in interrupted or group.get('preempted') is True
                                or group.get('deferred_reason') is not None
                                or group.get('reason') == 'previous_census_interrupted')
                due[volume["id"]] = (last + config["classification_interval_seconds"]) if (
                    not needs_resume and compatible and last and last <= time.time()+300) else 0.
            job_identity = None
            job_fingerprint = None
            job_previous = None
            while True:
                started = time.monotonic()
                observations = observe_capacities(config)
                for identity, cap in observations.items():
                    if identity == "contained_in":
                        rows.setdefault("penguin-wsl", {"id": "penguin-wsl"})["contained_in"] = cap
                        continue
                    row = rows.setdefault(identity, {"id": identity})
                    row["capacity"] = retain_observation(row.get("capacity") or {}, cap)
                if config.get("receipt_root"):
                    for identity, group in receipt_classifications(Path(config["receipt_root"])).items():
                        rows.setdefault(identity, {"id": identity})["classification"] = group
                if job is not None and job.done():
                    group = job.result()
                    group["fingerprint"] = job_fingerprint
                    row = rows.setdefault(job_identity, {"id": job_identity})
                    if group['state'] == 'deferred':
                        row['classification'] = {**(job_previous or {'state': 'not_scanned'}),
                                                 'deferred_reason': group['reason']}
                        due[job_identity] = time.time() + 60
                    else:
                        row["classification"] = retain_observation(row.get("classification") or {}, group)
                        if group.get('preempted'):
                            row['classification']['deferred_reason'] = group['reason']
                        due[job_identity] = time.time() + (60 if group.get('preempted') else
                            900 if group["state"] == "unavailable" else config["classification_interval_seconds"])
                    job = None
                    job_progress = None
                    job_previous = None
                elif job is not None and job_progress is not None:
                    group = job_progress.snapshot()
                    if group is not None:
                        group['fingerprint'] = job_fingerprint
                        rows.setdefault(job_identity, {'id': job_identity})['classification'] = group
                if watch and job is None:
                    for volume, remote in specs:
                        identity = volume["id"]
                        if time.time() < due[identity]:
                            continue
                        is_local = config['observer_node_id'] == 'penguin' and remote is None
                        row = rows.setdefault(identity, {"id": identity})
                        reason = local_census_priority_reason(volume, repo) if is_local else None
                        if reason:
                            row['classification'] = {**(row.get('classification') or {'state': 'not_scanned'}),
                                                     'deferred_reason': reason}
                            due[identity] = time.time() + 60
                            continue
                        previous_group = row.get('classification') or {}
                        row["classification"] = {**previous_group, "state": "scanning", "started_at_utc": utc()}
                        row['classification'].pop('deferred_reason', None)
                        job_progress = CensusProgress()
                        options = {'wave_lock': RETIREMENT_WAVE_LOCK} if is_local else {}
                        job = pool.submit(census, volume, repo, depth=config["classification_depth"],
                                          timeout=config["classification_timeout_seconds"], remote=remote,
                                          previous=previous_group, progress=job_progress, **options)
                        job_identity = identity
                        job_fingerprint = census_fingerprint(config, volume, remote)
                        job_previous = previous_group
                        break
                snapshot = {"contract": CONTRACT, "observer_node_id": config["observer_node_id"], "generated_at_utc": utc(),
                            "classification_interval_seconds": config["classification_interval_seconds"], "volumes": list(rows.values())}
                if config["observer_node_id"] == "penguin":
                    from stockagent.data_sync.storage_runtime import collect as collect_runtime
                    try:
                        receiver = receiver_snapshot(Path(config["receipt_root"])) if config.get("receipt_root") else None
                    except (OSError, ValueError, KeyError, TypeError):
                        receiver = None
                    runtime = collect_runtime(runtime, observations.get("vastai1T-disk"), receiver=receiver)
                    snapshot["runtime"] = runtime
                    if isinstance(config.get("operations"), dict):
                        from stockagent.data_sync.storage_progress import collect
                        try:
                            operations, operation_history = collect(repo, config, operation_history,
                                runtime=runtime, capacities=observations)
                            atomic_write_json(history_path, operation_history)
                        except (OSError, ValueError, TypeError) as exc:
                            # Don't hide capacity or refresh an old progress clock.
                            print(json.dumps({"state": "operations_observer_unavailable",
                                              "error_code": type(exc).__name__}), flush=True)
                    if isinstance(operations, dict):
                        snapshot["operations"] = operations
                    try:
                        candidate = read_snapshot(Path("/var/lib/stockagent-cold-tiers/status.json"))
                        if public_cold_tiers(candidate, now=time.time()) is not None:
                            tier_status = candidate
                    except (OSError, ValueError):
                        pass  # Missing tier metadata does not hide disk capacity.
                    if isinstance(tier_status, dict):
                        snapshot["cold_tiers"] = tier_status
                elif config["observer_node_id"] == "lab203":
                    from stockagent.data_sync.storage_runtime import receiver_host_observation
                    receiver_runtime, snapshot["host_runtime"] = receiver_host_observation(
                        receiver_runtime, now=time.time())
                if config.get("pairing"):
                    from scripts.backup_delivery_receipt import signed
                    snapshot = signed({**snapshot, "pairing": config["pairing"]})
                atomic_write_json(output, snapshot)
                if watch and config["observer_node_id"] == "penguin":
                    try:
                        record_daily_capacity(output.with_name('storage_capacity_daily_history.json'), snapshot['volumes'])
                    except (OSError, ValueError, TypeError) as exc:
                        # A bad auxiliary history must not hide live capacity.
                        print(json.dumps({'state': 'capacity_history_unavailable',
                                          'error_code': type(exc).__name__}), flush=True)
                print(json.dumps({"state": "observed", "volumes": len(rows), "census_running": job is not None,
                                  "cycle_seconds": round(time.monotonic()-started, 3)}), flush=True)
                if not watch:
                    return
                time.sleep(max(1., config["capacity_interval_seconds"] - (time.monotonic()-started)))
