"""Bounded, read-only WDDM attribution; never infer Linux owners from Windows PIDs.

Board telemetry and process counters describe different boundaries. WDDM shared
allocations are not additive, and Microsoft documents process-memory counter
errors. These samples identify candidates, not permission to stop applications.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.audit_service_latency_coverage import gpu_snapshot  # noqa: E402

QUERY = r"""
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$engines = @(Get-CimInstance -ClassName Win32_PerfFormattedData_GPUPerformanceCounters_GPUEngine |
  Where-Object { $_.UtilizationPercentage -gt 0 } | ForEach-Object {
    [pscustomobject]@{ instance = [string]$_.Name; percent = [double]$_.UtilizationPercentage }
  })
$memory = @(Get-CimInstance -ClassName Win32_PerfFormattedData_GPUPerformanceCounters_GPUProcessMemory |
  Where-Object { $_.DedicatedUsage -gt 0 -or $_.SharedUsage -gt 0 } | ForEach-Object {
    [pscustomobject]@{
      instance = [string]$_.Name; dedicated_bytes = [uint64]$_.DedicatedUsage
      shared_bytes = [uint64]$_.SharedUsage; committed_bytes = [uint64]$_.TotalCommitted
    }
  })
$ids = @(@($engines.instance) + @($memory.instance) | ForEach-Object {
  if ($_ -match '^pid_(\d+)_') { [int]$matches[1] }
} | Sort-Object -Unique)
$processes = @($ids | ForEach-Object {
  $gpuProcessId = $_
  $process = Get-CimInstance Win32_Process -Filter "ProcessId = $gpuProcessId"
  if ($process) {
    [pscustomobject]@{
      pid = [int]$process.ProcessId; name = [string]$process.Name
      created_at_utc = $process.CreationDate.ToUniversalTime().ToString('o')
      stockagent_reference = [bool]([string]$process.CommandLine -match '(?i)stockagent')
    }
  }
})
ConvertTo-Json -InputObject ([pscustomobject]@{
  observed_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
  engines = $engines; memory = $memory; processes = $processes
}) -Compress -Depth 5
"""
INSTANCE = re.compile(r"^pid_(\d+)_luid_(0x[0-9a-f]+)_(0x[0-9a-f]+)_phys_(\d+)(?:_eng_(\d+)_engtype_(.+))?$", re.I)
SOURCES = (
    "https://devblogs.microsoft.com/directx/gpus-in-the-task-manager/",
    "https://learn.microsoft.com/en-us/troubleshoot/windows-client/performance/gpu-process-memory-counters-report-wrong-value",
)


def parse_snapshot(value):
    processes = {int(row["pid"]): {name: row[name] for name in (
        "pid", "name", "created_at_utc", "stockagent_reference",
    )} for row in value["processes"]}
    rows, unknown = {}, []
    for kind in ("engines", "memory"):
        for row in value[kind]:
            parsed = INSTANCE.fullmatch(row["instance"])
            if parsed is None or (kind == "engines" and parsed.group(5) is None):
                unknown.append({"kind": kind, "instance": row["instance"]})
                continue
            pid, hi, lo, physical, engine, engine_type = parsed.groups()
            key = (int(pid), hi.lower(), lo.lower(), int(physical))
            owner = rows.setdefault(key, {
                "windows_pid": key[0], "adapter_luid": [key[1], key[2]], "physical_index": key[3],
                "process_observation": processes.get(key[0]), "engines": [], "memory_counters": [],
                "pid_domain": "Windows", "linux_service_owner_verified": False,
            })
            if kind == "engines":
                owner["engines"].append({"index": int(engine), "type": engine_type, "percent": row["percent"]})
            else:
                owner["memory_counters"].append({name: row[name] for name in (
                    "dedicated_bytes", "shared_bytes", "committed_bytes",
                )})
    return {
        "observed_at_utc": value["observed_at_utc"],
        "processes": [rows[key] for key in sorted(rows)], "unrecognized_instances": unknown,
        "unrecognized_count": len(unknown), "all_gpu_memory_attributed": False,
        "process_identity_atomic_with_counters": False,
        "memory_counters_additive": False, "memory_counter_accuracy_verified": False,
        "linux_service_pid_mapping_available": False,
    }


def snapshot():
    if shutil.which("powershell.exe") is None:
        return {"state": "unavailable", "reason": "windows_powershell_unavailable"}
    started = time.perf_counter()
    try:
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", QUERY],
            capture_output=True, timeout=30, check=True,
        )
        if len(result.stdout) > 2 * 2**20:
            raise ValueError("windows_gpu_counter_output_exceeds_2MiB")
        raw = json.loads(result.stdout.decode("utf-8-sig"))
        value = parse_snapshot(raw)
        return {"state": "observed", "elapsed_seconds": time.perf_counter() - started, **value}
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError) as exc:
        return {"state": "unavailable", "error_type": type(exc).__name__,
                "elapsed_seconds": time.perf_counter() - started}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, choices=(1, 2, 3), default=2)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError("preserve prior GPU ownership evidence")
    if not args.output.resolve().is_relative_to((ROOT / "artifacts/operations").resolve()):
        raise ValueError("GPU ownership evidence must remain under artifacts/operations")
    result = {
        "contract": "windows_wddm_gpu_owner_observation_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), "sources": SOURCES,
        "board_before": gpu_snapshot(), "samples": [snapshot() for _ in range(args.samples)],
        "board_after": gpu_snapshot(), "provider_requests": 0, "cuda_initialized_by_probe": False,
        "services_or_applications_stopped": False, "source_files_written": 0,
        "boundary": "sequential_non_atomic_WDDM_samples_not_total_VRAM_reconciliation_or_CUDA_readiness",
    }
    atomic_write_json(args.output, result)
    print(json.dumps({"samples": [row["state"] for row in result["samples"]],
                      "process_counts": [len(row.get("processes", ())) for row in result["samples"]]}))


if __name__ == "__main__":
    main()
