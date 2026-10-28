#!/usr/bin/env python3
"""Measure transparent NTFS compression on disposable copies of exact D blobs.

Original CAS bytes/metadata, manifests, mount options and OS caches are never
changed. Windows reports physical allocation; every candidate is fully hashed
before removal. This is a bounded hardware trial, not a production rollout.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.windows_cold_io import _windows_process_environment, windows_path

PROGRAM = r'''$ErrorActionPreference='Stop'
$request=[Console]::In.ReadToEnd() | ConvertFrom-Json
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class ColdAllocation {
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 public static extern uint GetCompressedFileSizeW(string name, out uint high);
 public static long Bytes(string name) {
   uint high; uint low=GetCompressedFileSizeW(name,out high);
   if(low==0xffffffff && Marshal.GetLastWin32Error()!=0) throw new System.ComponentModel.Win32Exception();
   return ((long)high<<32)|low;
 }
}
'@
if(Test-Path -LiteralPath $request.scratch){throw 'Scratch already exists'}
[void][System.IO.Directory]::CreateDirectory($request.scratch)
$rows=@()
foreach($source in $request.sources){
  $original=(Get-FileHash -LiteralPath $source.path -Algorithm SHA256).Hash.ToLowerInvariant()
  if($original -ne $source.sha256){throw 'Source CAS SHA mismatch'}
  foreach($codec in @('none','XPRESS16K','LZX')){
    $path=Join-Path $request.scratch ($source.sha256+'.'+$codec+'.probe')
    $total=[Diagnostics.Stopwatch]::StartNew()
    [System.IO.File]::Copy($source.path,$path,$false)
    $before=[ColdAllocation]::Bytes($path)
    $encode=[Diagnostics.Stopwatch]::StartNew()
    if($codec -ne 'none'){
      $details=& compact.exe /C /A /Q ('/EXE:'+$codec) $path 2>&1
      if($LASTEXITCODE -ne 0){throw 'Compression rejected'}
    }
    $encode.Stop()
    $allocated=[ColdAllocation]::Bytes($path)
    $read=[Diagnostics.Stopwatch]::StartNew()
    $sha=(Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
    $read.Stop();$total.Stop()
    if($sha -ne $original){throw 'Candidate original-byte readback differs'}
    $rows+=@{source_sha256=$original;codec=$codec;probe_filename=[IO.Path]::GetFileName($path);logical_bytes=[long]$source.bytes;
      initial_allocated_bytes=$before;allocated_bytes=$allocated;
      encode_seconds=$encode.Elapsed.TotalSeconds;read_sha_seconds=$read.Elapsed.TotalSeconds;
      copy_encode_readback_seconds=$total.Elapsed.TotalSeconds;full_original_sha_verified=$true}
  }
  if((Get-FileHash -LiteralPath $source.path -Algorithm SHA256).Hash.ToLowerInvariant() -ne $original){throw 'Source changed'}
}
@{rows=$rows;own_scratch_removed=$false;source_files_modified=0} | ConvertTo-Json -Compress -Depth 5
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--blob", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    canonical = Path("/srv/stockagent-packed")
    _check_d_primary_mount(canonical)
    if not 1 <= len(args.blob) <= 8 or args.output.exists():
        parser.error("use 1..8 exact blobs and a new receipt")
    sources, signatures = [], {}
    for path in args.blob:
        path = path.absolute()
        if (path.resolve(strict=True) != path or not path.is_relative_to(canonical / "objects/blobs")
                or re.fullmatch(r"[0-9a-f]{64}\.blob", path.name) is None):
            raise SnapshotError("benchmark accepts exact canonical CAS blobs only")
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= 64 * 1024**2:
            raise SnapshotError("benchmark blob must be a bounded regular file")
        signatures[path] = (info.st_dev, info.st_ino, info.st_size, info.st_mode, info.st_mtime_ns, info.st_ctime_ns)
        sources.append({"path": windows_path(path), "sha256": path.stem, "bytes": info.st_size})
    space = os.statvfs(canonical)
    if space.f_bavail * space.f_frsize < 64 * 1024**3 + 2 * sum(row["bytes"] for row in sources):
        raise SnapshotError("benchmark preserves the D reserve")
    scratch = canonical / ".local-state" / ("compression-probe-" + uuid.uuid4().hex)
    start = time.perf_counter()
    result = subprocess.run(["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand",
        base64.b64encode(PROGRAM.encode("utf-16le")).decode()],
        input=json.dumps({"scratch": windows_path(scratch), "sources": sources}).encode(),
        capture_output=True, env=_windows_process_environment(), timeout=240)
    if result.returncode:
        raise SnapshotError("native compression trial failed; keep its owned scratch for inspection: "
                            + result.stderr.decode(errors="replace")[-1000:])
    value = json.loads(result.stdout.decode("utf-8-sig"))
    expected_names = {row["sha256"] + "." + codec + ".probe" for row in sources
                      for codec in ("none", "XPRESS16K", "LZX")}
    if (set(os.listdir(scratch)) != expected_names or len(value["rows"]) != len(expected_names)
            or {row["probe_filename"] for row in value["rows"]} != expected_names):
        raise SnapshotError("owned probe inventory differs; retain scratch")
    for row in value["rows"]:
        path = scratch / row["probe_filename"]
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise SnapshotError("compressed file is not a regular Linux reader input")
        started = time.perf_counter()
        digest = hashlib.sha256()
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
            for part in iter(lambda: stream.read(1024**2), b""):
                digest.update(part)
        if (path.lstat() != before or before.st_size != row["logical_bytes"]
                or digest.hexdigest() != row["source_sha256"]):
            raise SnapshotError("Linux original-byte readback differs; retain scratch")
        row.update(linux_read_sha_seconds=time.perf_counter()-started, linux_original_sha_verified=True)
    for path, signature in signatures.items():
        info = path.lstat()
        if signature != (info.st_dev, info.st_ino, info.st_size, info.st_mode, info.st_mtime_ns, info.st_ctime_ns):
            raise SnapshotError("original CAS metadata changed during trial")
    for name in sorted(expected_names):
        (scratch / name).unlink()
    scratch.rmdir()
    value["own_scratch_removed"] = not scratch.exists()
    value.update(contract="stockagent.transparent_compression_trial.v1", observed_at_epoch=time.time(),
        complete_workflow_seconds=time.perf_counter()-start, production_compression_changed=False,
        timing_basis="native copy+encode+full SHA; warm cache, no cache eviction; physical allocation via GetCompressedFileSizeW")
    atomic_write_json(args.output, value)
    print(json.dumps(value))


if __name__ == "__main__":
    main()
