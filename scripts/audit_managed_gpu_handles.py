"""Read-only GPU-device FD inventory of current StockAgent Linux processes.

An explicit option also inspects other currently visible Linux processes.

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


def inspect_process(path: Path, *, include_unmanaged: bool = False) -> dict | None:
    try:
        before = process_identity(path)
    except (OSError, ValueError, IndexError) as exc:
        if not include_unmanaged:
            if isinstance(exc, OSError):
                return None  # No inspected process; never manufacture a zero-FD row.
            raise
        return {"pid": int(path.name), "start_time_ticks": None, "unit": None, "managed": None,
                "state": ("exited" if isinstance(exc, FileNotFoundError) else
                          "unreadable" if isinstance(exc, OSError) else "identity_invalid"),
                "gpu_device_handles": None, "error_type": type(exc).__name__}
    match = UNIT.search(before["cgroup"])
    if not match and not include_unmanaged:
        return None
    row = {"pid": before["pid"], "start_time_ticks": before["start_time_ticks"],
           "unit": match[1] if match else None, "state": "unavailable", "gpu_device_handles": None}
    if include_unmanaged:
        row["managed"] = match is not None
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
    except (OSError, ValueError, IndexError) as exc:
        if not include_unmanaged and not isinstance(exc, OSError):
            raise
        row.update(state=("exited" if isinstance(exc, FileNotFoundError) else
                          "unreadable" if isinstance(exc, OSError) else "identity_invalid"),
                   error_type=type(exc).__name__)
    return row


def inventory(proc: Path = Path("/proc"), *, include_unmanaged: bool = False) -> dict:
    rows = []
    candidates = 0
    for path in proc.iterdir():
        if not path.name.isdigit():
            continue
        candidates += 1
        row = inspect_process(path, include_unmanaged=include_unmanaged)
        if row is not None:
            rows.append(row)
    observed = [row for row in rows if row["state"] == "observed"]
    owners = [row for row in observed if row["gpu_device_handles"]]
    result = {
        "contract": ("stockagent_linux_visible_gpu_fd_sample_v1" if include_unmanaged else
                     "stockagent_linux_managed_gpu_fd_sample_v1"),
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "state": "observed" if len(observed) == len(rows) else "partial_visibility",
        "scope": ("all numeric process directories visible in the caller Linux proc namespace; identity failures remain unknown"
                  if include_unmanaged else
                  "currently visible Linux processes whose cgroup contains a StockAgent service"),
        "proc_candidates": candidates, "managed_processes": sum(row["unit"] is not None for row in rows),
        "observed_processes": len(observed), "gpu_owner_count": len(owners),
        "gpu_device_handles": sum(len(row["gpu_device_handles"]) for row in owners),
        "rows": sorted(rows, key=lambda value: (value["unit"] or "", value["pid"])),
        "provider_calls": 0, "services_changed": False, "cuda_initialized_by_probe": False,
        "all_windows_allocations_attributed": False, "atomic_whole_host_snapshot": False,
        "no_uninspected_process_claim": True,
    }
    if include_unmanaged:
        result.update(selected_processes=len(rows),
                      unmanaged_processes=sum(row["managed"] is False for row in rows),
                      ownership_unknown_processes=sum(row["managed"] is None for row in rows))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--include-unmanaged", action="store_true",
                        help="Inspect all visible Linux process directories, retaining unknown identity/FD failures")
    args = parser.parse_args()
    path = args.output.absolute()
    if path.exists() or path.is_symlink() or not path.resolve().is_relative_to((ROOT / "artifacts/operations").resolve()):
        parser.error("new artifact under artifacts/operations required")
    result = inventory(include_unmanaged=args.include_unmanaged)
    atomic_write_json(path, result)
    fields = ["state", "managed_processes", "observed_processes", "gpu_owner_count", "gpu_device_handles"]
    if args.include_unmanaged:
        fields += ["proc_candidates", "selected_processes", "unmanaged_processes", "ownership_unknown_processes"]
    print({name: result[name] for name in fields})


if __name__ == "__main__":
    main()
