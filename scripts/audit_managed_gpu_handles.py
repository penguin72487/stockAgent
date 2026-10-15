"""Read-only GPU-device FD inventory of current StockAgent Linux processes.

No CUDA/NVML initialization, provider request, process signal or service action.
Device-handle absence is a sampled Linux observation, not a claim about all
Windows allocations, zero board usage, CUDA readiness or future warmup.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402

UNIT = re.compile(r"(?:^|/)(stockagent-[^/\n]+\.service)(?:/|$)", re.MULTILINE)
GPU = re.compile(r"^/dev/(?:dxg|nvidia[^/]*|nvidia-caps/nvidia-cap\d+|dri/(?:card\d+|renderD\d+))(?: \(deleted\))?$")


def process_identity(path: Path) -> dict:
    raw = (path / "stat").read_text()
    fields = raw[raw.rindex(")") + 2:].split()
    return {"pid": int(path.name), "start_time_ticks": int(fields[19]),
            "cgroup": (path / "cgroup").read_text()}


def inspect_process(path: Path) -> dict | None:
    try:
        before = process_identity(path)
    except OSError:
        return None  # No inspected process; never manufacture a zero-FD row.
    match = UNIT.search(before["cgroup"])
    if not match:
        return None
    row = {"pid": before["pid"], "start_time_ticks": before["start_time_ticks"],
           "unit": match[1], "state": "unavailable", "gpu_device_handles": None}
    try:
        handles = []
        for entry in (path / "fd").iterdir():
            try:
                target = os.readlink(entry)
            except FileNotFoundError:
                continue  # A normally closed FD is not a process-identity error.
            if GPU.fullmatch(target):
                handles.append({"fd": int(entry.name), "device": target})
        if process_identity(path) != before:
            row["state"] = "identity_changed"
        else:
            row.update(state="observed", gpu_device_handles=sorted(handles, key=lambda value: value["fd"]))
    except OSError as exc:
        row.update(state="exited" if isinstance(exc, FileNotFoundError) else "unreadable",
                   error_type=type(exc).__name__)
    return row


def inventory(proc: Path = Path("/proc")) -> dict:
    rows = []
    candidates = 0
    for path in proc.iterdir():
        if not path.name.isdigit():
            continue
        candidates += 1
        row = inspect_process(path)
        if row is not None:
            rows.append(row)
    observed = [row for row in rows if row["state"] == "observed"]
    owners = [row for row in observed if row["gpu_device_handles"]]
    return {
        "contract": "stockagent_linux_managed_gpu_fd_sample_v1",
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "state": "observed" if len(observed) == len(rows) else "partial_visibility",
        "scope": "currently visible Linux processes whose cgroup contains a StockAgent service",
        "proc_candidates": candidates, "managed_processes": len(rows),
        "observed_processes": len(observed), "gpu_owner_count": len(owners),
        "gpu_device_handles": sum(len(row["gpu_device_handles"]) for row in owners),
        "rows": sorted(rows, key=lambda value: (value["unit"], value["pid"])),
        "provider_calls": 0, "services_changed": False, "cuda_initialized_by_probe": False,
        "all_windows_allocations_attributed": False, "atomic_whole_host_snapshot": False,
        "no_uninspected_process_claim": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    path = args.output.absolute()
    if path.exists() or path.is_symlink() or not path.resolve().is_relative_to((ROOT / "artifacts/operations").resolve()):
        parser.error("new artifact under artifacts/operations required")
    result = inventory()
    atomic_write_json(path, result)
    print({name: result[name] for name in ("state", "managed_processes", "observed_processes",
                                         "gpu_owner_count", "gpu_device_handles")})


if __name__ == "__main__":
    main()
