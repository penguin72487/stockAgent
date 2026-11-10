"""Guard the E-backed Linux work volume; C services and D authority stay put.

The data-only WSL instance supplies an ext4 filesystem through WSL's shared
mount namespace. A native E identity, registered VHD location, fixed marker,
and actual mount must agree. Missing storage never creates a C fallback.
"""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import re
import subprocess

from stockagent.data_sync.desync_snapshots import SnapshotError

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "configs/data_sync/workspace_volume.json"
MARKER = ".stockagent-e-workspace.json"
ENROLLMENT = Path("/var/lib/stockagent/workspace-volume/enrollment.json")
POWERSHELL = "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"
WSL = "/mnt/c/Windows/System32/wsl.exe"
FINLAB_FUTURES_CONTRACT = "finlab_futures_workspace_objects_v1"
FINLAB_FUTURES_ALIAS = ROOT / "data_finlab/intraday/objects/futures-workspace-v1"
FINLAB_FUTURES_ROLE = Path("downloads/finlab-futures-intraday-v1")
FINLAB_FUTURES_MARKER = ".stockagent-finlab-futures-workspace.json"


def configuration(path=CONFIG):
    c = json.loads(Path(path).read_bytes())
    if (c.get("schema_version") != 1 or c.get("contract") != "penguin_e_pre_cold_workspace_v1"
            or c.get("authority_node_id") != "penguin" or c.get("physical_volume") != "E:"
            or c.get("cold_physical_volume") != "D:" or c.get("cold_root") != "/srv/stockagent-packed"
            or not re.fullmatch(r"[0-9a-f-]{36}", c.get("volume_id", ""))
            or not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]+", c.get("distribution", ""))
            or c.get("install_location") != "E:\\StockAgent\\workspace"
            or c.get("shared_root") != "/mnt/wsl/stockagent-workspace"
            or c.get("mount_point") != "/srv/stockagent-workspace"
            or type(c.get("minimum_free_bytes")) is not int or c["minimum_free_bytes"] < 32 * 1024**3):
        raise SnapshotError("invalid E workspace enrollment configuration")
    return c


def marker(c):
    return {k: c[k] for k in ("schema_version", "contract", "authority_node_id",
                             "physical_volume", "volume_id", "distribution", "install_location")}


def native(script, *, request=None, timeout=60):
    """Fixed script, JSON stdin, validated local WSL bridge; no shell values."""
    from stockagent.data_sync.windows_cold_io import _windows_process_environment
    p = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-EncodedCommand",
                        base64.b64encode(script.encode("utf-16le")).decode("ascii")],
                       input=json.dumps(request or {}).encode(), capture_output=True,
                       timeout=timeout, env=_windows_process_environment())
    if p.returncode:
        raise SnapshotError("native E workspace operation failed: " + p.stderr.decode("utf-8", "replace")[-1200:])
    return json.loads(p.stdout.decode("utf-8-sig"))


def native_identity(c=None):
    c = c or configuration()
    value = native(r"""$ErrorActionPreference='Stop';$ProgressPreference='SilentlyContinue'
$r=[Console]::In.ReadToEnd()|ConvertFrom-Json
$v=New-Object IO.DriveInfo('E:\')
Add-Type -TypeDefinition @'
using System;using System.Text;using System.Runtime.InteropServices;
public static class StockAgentEVolume {
 [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]
 public static extern bool GetVolumeNameForVolumeMountPoint(string path,StringBuilder name,uint length);
}
'@
$guid=New-Object Text.StringBuilder 128
if(-not [StockAgentEVolume]::GetVolumeNameForVolumeMountPoint('E:\',$guid,128)){throw 'E volume GUID unavailable'}
$all=@(Get-ChildItem 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss'|Get-ItemProperty)
$matches=@($all|Where-Object {$_.DistributionName -eq $r.distribution})
$entry=$null
if($matches.Count -gt 1){throw 'duplicate distribution registration'}
if($matches.Count -eq 1){$entry=@{base_path=$matches[0].BasePath;version=$matches[0].Version}}
@{volume_id=$guid.ToString();filesystem=$v.DriveFormat;
  free_bytes=[long]$v.AvailableFreeSpace;size_bytes=[long]$v.TotalSize;registration=$entry}|ConvertTo-Json -Depth 5 -Compress
""", request={"distribution": c["distribution"]})
    if (c["volume_id"].lower() not in value["volume_id"].lower()
            or (value.get("disk_unique_id") is not None
                and value["disk_unique_id"].strip().lower() != c["disk_unique_id"].lower())
            or value["filesystem"] != "NTFS"):
        raise SnapshotError("E volume does not match its pinned NVMe identity")
    registration = value.get("registration")
    if registration:
        base = registration["base_path"].removeprefix("\\\\?\\").rstrip("\\")
        if base.lower() != c["install_location"].lower() or registration["version"] != 2:
            raise SnapshotError("data VHD is not registered at the fixed E location")
    return value


def wsl_command(c, arguments, *, timeout=120):
    from stockagent.data_sync.windows_cold_io import _windows_process_environment
    p = subprocess.run([WSL, *arguments], capture_output=True, timeout=timeout,
                       env=_windows_process_environment())
    def decode(value):
        return value.decode("utf-16le" if b"\x00" in value else "utf-8", "replace")
    if p.returncode:
        raise SnapshotError("data-only WSL operation failed: " + decode(p.stderr or p.stdout)[-1200:])
    return {"output": decode(p.stdout), "diagnostic": decode(p.stderr), "exit_code": p.returncode}


def _mount_info(path):
    p = subprocess.run(["findmnt", "-J", "-T", str(path), "-o", "TARGET,SOURCE,FSTYPE,UUID"],
                       capture_output=True, text=True, check=True, timeout=15)
    return json.loads(p.stdout)["filesystems"][0]


def check(*, physical=False, c=None):
    c = c or configuration()
    target, shared = Path(c["mount_point"]), Path(c["shared_root"])
    if target.is_symlink() or shared.is_symlink():
        raise SnapshotError("redirected E workspace mount")
    mount = _mount_info(target)
    if mount["target"] != str(target) or mount["fstype"] != "ext4" or not target.samefile(shared):
        raise SnapshotError("E workspace is not its enrolled shared ext4 mount")
    m = target / MARKER
    if m.is_symlink() or not m.is_file() or json.loads(m.read_bytes()) != marker(c):
        raise SnapshotError("E workspace identity marker differs")
    if ENROLLMENT.is_symlink() or not ENROLLMENT.is_file():
        raise SnapshotError("E workspace has no fixed local enrollment")
    enrollment = json.loads(ENROLLMENT.read_bytes())
    if (enrollment.get("marker") != marker(c) or not mount.get("uuid")
            or enrollment.get("filesystem_uuid") != mount["uuid"]):
        raise SnapshotError("E workspace filesystem UUID differs")
    value = {"state": "mounted", "mount_point": str(target), "filesystem_uuid": mount["uuid"],
             "physical_volume": "E:", "cold_physical_volume": "D:"}
    if physical:
        identity = native_identity(c)
        if not identity.get("registration"):
            raise SnapshotError("E data distribution registration disappeared")
        value["physical_free_bytes"] = identity["free_bytes"]
    return value


def admit(required_bytes=0, *, c=None):
    c = c or configuration()
    value = check(physical=True, c=c)
    s = os.statvfs(c["mount_point"])
    value["filesystem_available_bytes"] = s.f_bavail * s.f_frsize
    available = min(value["physical_free_bytes"], value["filesystem_available_bytes"])
    if type(required_bytes) is not int or required_bytes < 0:
        raise SnapshotError("invalid E workspace write estimate")
    if available < required_bytes + c["minimum_free_bytes"]:
        raise SnapshotError("E workspace lacks physical/guest verification headroom")
    return value


def check_finlab_futures(*, physical=False, c=None):
    """Validate the fixed receipt-compatible alias before future E writes.

    A bind mount keeps the existing relative receipt namespace, but must not
    hide existing files or silently turn into a C directory after E disappears.
    This check does not migrate old data or itself authorize a source call.
    """
    c = c or configuration()
    result = check(physical=physical, c=c)
    source = Path(c["mount_point"]) / FINLAB_FUTURES_ROLE
    target = FINLAB_FUTURES_ALIAS
    try:
        valid = (source.resolve(strict=True) == source and target.resolve(strict=True) == target
                 and not source.is_symlink() and not target.is_symlink() and source.is_dir()
                 and target.is_dir() and target.samefile(source)
                 and _mount_info(target)['target'] == str(target))
    except OSError:
        valid = False
    if not valid:
        raise SnapshotError("FinLab futures workspace alias is absent or redirected")
    identity = source / FINLAB_FUTURES_MARKER
    expected = {"contract": FINLAB_FUTURES_CONTRACT, "volume_id": c["volume_id"],
                "source": str(source), "target": str(target)}
    if identity.is_symlink() or not identity.is_file() or json.loads(identity.read_bytes()) != expected:
        raise SnapshotError("FinLab futures workspace identity differs")
    return {**result, "contract": FINLAB_FUTURES_CONTRACT, "source": str(source),
            "target": str(target)}


def path(kind, *, create=False):
    if kind not in {"downloads", "increments", "build", "verify", "restore", "transport", "migration"}:
        raise SnapshotError("unregistered E workspace role")
    c = configuration()
    check(c=c)
    value = Path(c["mount_point"]) / kind
    if value.is_symlink():
        raise SnapshotError("redirected E workspace role")
    if create:
        value.mkdir(mode=0o700, exist_ok=True)
    return value
