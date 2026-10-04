#!/usr/bin/env python3
"""Return already-discarded backfill pages to a memory-constrained WSL host.

Only the registered historical-data backfill cgroup is eligible. This is not a
global cache drop or a memory limit: the kernel retains the choice of pages to
reclaim, so the requested amount is deliberately bounded by observed LazyFree.
"""

from __future__ import annotations

import argparse
import errno
import json
from pathlib import Path
import re
import shutil
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo


GIB = 1024**3
CGROUP = Path(
    "/sys/fs/cgroup/stockagent.slice/stockagent-heavy.slice/"
    "stockagent-heavy-data.slice/stockagent-registered-data-backfill.service"
)


def reclaim_budget_bytes(
    *,
    windows_free_bytes: int,
    lazyfree_bytes: int,
    min_free_bytes: int,
    max_reclaim_bytes: int,
    lazyfree_reserve_bytes: int,
) -> int:
    """Bound a request to cold, already-discarded pages and host pressure."""
    if windows_free_bytes >= min_free_bytes:
        return 0
    return max(0, min(max_reclaim_bytes, lazyfree_bytes - lazyfree_reserve_bytes))


def _windows_free_bytes() -> int | None:
    try:
        powershell = shutil.which("powershell.exe") or (
            "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"
        )
        result = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[long](Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        match = re.search(r"\d+", result.stdout.replace("\x00", ""))
        return int(match.group()) * 1024 if match else None
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def _lazyfree_bytes(cgroup: Path) -> tuple[int, int]:
    total = 0
    counted = 0
    for raw_pid in (cgroup / "cgroup.procs").read_text().splitlines():
        try:
            content = (Path("/proc") / raw_pid / "smaps_rollup").read_text()
        except (OSError, ValueError):  # A short-lived child may exit mid-scan.
            continue
        match = re.search(r"^LazyFree:\s+(\d+) kB$", content, re.MULTILINE)
        if match:
            total += int(match.group(1)) * 1024
            counted += 1
    return total, counted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform bounded reclaim")
    parser.add_argument("--min-windows-free-gib", type=float, default=32.0)
    parser.add_argument("--max-reclaim-gib", type=float, default=4.0)
    parser.add_argument("--lazyfree-reserve-gib", type=float, default=2.0)
    args = parser.parse_args()
    if min(
        args.min_windows_free_gib,
        args.max_reclaim_gib,
        args.lazyfree_reserve_gib,
    ) < 0:
        parser.error("memory thresholds cannot be negative")

    result: dict[str, object] = {"mode": "apply" if args.apply else "audit"}
    now = datetime.now(ZoneInfo("Asia/Taipei"))
    # Never add reclaim work to the opening path, even if backfill is running.
    if "microsoft" not in Path("/proc/sys/kernel/osrelease").read_text().lower():
        result["status"] = "not_wsl"
    elif (8, 20) <= (now.hour, now.minute) < (9, 10):
        result["status"] = "protected_opening_window"
    elif not CGROUP.is_dir():
        result["status"] = "backfill_not_running"
    else:
        windows_free = _windows_free_bytes()
        if windows_free is None:
            result["status"] = "windows_memory_unavailable"
        else:
            lazyfree, scanned = _lazyfree_bytes(CGROUP)
            budget = reclaim_budget_bytes(
                windows_free_bytes=windows_free,
                lazyfree_bytes=lazyfree,
                min_free_bytes=int(args.min_windows_free_gib * GIB),
                max_reclaim_bytes=int(args.max_reclaim_gib * GIB),
                lazyfree_reserve_bytes=int(args.lazyfree_reserve_gib * GIB),
            )
            result.update(
                windows_free_gib=round(windows_free / GIB, 2),
                backfill_lazyfree_gib=round(lazyfree / GIB, 2),
                scanned_pids=scanned,
                requested_reclaim_gib=round(budget / GIB, 2),
            )
            if not args.apply:
                result["status"] = "would_reclaim" if budget else "no_reclaim_needed"
            elif budget < GIB or scanned == 0:
                result["status"] = "no_reclaim_needed"
            else:
                before = int((CGROUP / "memory.current").read_text())
                try:
                    (CGROUP / "memory.reclaim").write_text(str(budget))
                except OSError as exc:
                    if exc.errno != errno.EAGAIN:
                        raise
                    result["status"] = "partial_reclaim"
                else:
                    result["status"] = "reclaimed"
                after = int((CGROUP / "memory.current").read_text())
                result["cgroup_reduction_gib"] = round((before - after) / GIB, 2)
    print(json.dumps(result, sort_keys=True))
    return 1 if result.get("status") == "windows_memory_unavailable" else 0


if __name__ == "__main__":
    raise SystemExit(main())
