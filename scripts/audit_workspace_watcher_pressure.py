"""Bounded, read-only native VS Code watcher and host inotify-pressure sample.

Do not raise kernel limits or restart the IDE. Stream fdinfo rather than loading
millions of watch descriptors into memory. This is not a game-FPS measurement.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.benchmark_finlab_effective_partitions import file_sha
from stockagent.data.finlab_acquisition_contract import process_owner


def sample(pid: int, *, watcher: bool = False) -> dict:
    owner = process_owner(pid)
    if owner is None:
        raise ValueError("observed native process generation unavailable")
    proc = Path(f"/proc/{pid}")
    command = [part.decode() for part in (proc / "cmdline").read_bytes().split(b"\0") if part]
    if watcher and "--type=fileWatcher" not in command:
        raise ValueError("the requested process is not the native file watcher")
    start = time.monotonic()
    rows, missing = [], 0
    for path in (proc / "fdinfo").iterdir():
        try:
            with path.open() as stream:
                count = sum(line.startswith("inotify wd:") for line in stream)
        except FileNotFoundError:
            missing += 1
            continue
        if count:
            rows.append({"fd": path.name, "watches": count})
    stat = (proc / "stat").read_text().rsplit(")", 1)[1].split()
    memory = {}
    for line in (proc / "status").read_text().splitlines():
        key, *parts = line.split()
        if key in ("VmRSS:", "RssAnon:", "RssFile:"):
            memory[key[:-1]+"_bytes"] = int(parts[0])*1024
    if process_owner(pid) != owner:
        raise ValueError("native process changed during sample")
    return {"owner": owner, "command": command[:4], "memory": memory,
        "inotify_watches": sum(row["watches"] for row in rows), "watch_fds": rows,
        "fdinfo_exited_during_read": missing, "cpu_ticks": int(stat[11])+int(stat[12]),
        "observed_monotonic": time.monotonic(), "fdinfo_scan_seconds": time.monotonic()-start}


def audit(pid: int, *, seconds: float) -> dict:
    if not 5 <= seconds <= 30:
        raise ValueError("bounded 5-30 second observation required")
    settings = ROOT / ".vscode/settings.json"
    settings_sha = file_sha(settings)
    before = sample(pid, watcher=True)
    time.sleep(seconds)
    after = sample(pid, watcher=True)
    if before["owner"] != after["owner"] or file_sha(settings) != settings_sha:
        raise ValueError("watcher/configuration generation changed during observation")
    manager = sample(1)
    limits = {name: int(Path(f"/proc/sys/fs/inotify/{name}").read_text())
              for name in ("max_user_watches", "max_user_instances", "max_queued_events")}
    elapsed = after["observed_monotonic"]-before["observed_monotonic"]
    cpu = (after["cpu_ticks"]-before["cpu_ticks"])/os.sysconf("SC_CLK_TCK")
    return {"contract": "native_workspace_watcher_pressure_sample_v1", "state": "observed",
        "observed_at_utc": datetime.now(UTC).isoformat(), "before": before, "after": after,
        "settings_sha256": settings_sha, "manager": manager, "limits": limits,
        "sample_seconds": elapsed, "watcher_cpu_seconds": cpu, "watcher_cpu_cores": cpu/elapsed,
        "watcher_fraction_of_uid_watch_limit": after["inotify_watches"]/limits["max_user_watches"],
        "all_uid_process_watches_enumerated": False, "provider_calls": 0,
        "kernel_settings_changed": False, "processes_restarted": False, "game_fps_measured": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--sample-seconds", default=10, type=float)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.absolute()
    if output.exists() or output.is_symlink() or not output.resolve().is_relative_to(ROOT / "artifacts/operations"):
        parser.error("fresh engineering receipt under artifacts/operations required")
    result = audit(args.pid, seconds=args.sample_seconds)
    atomic_write_json(output, result, durable=True)
    print(json.dumps({"state": result["state"], "watches": result["after"]["inotify_watches"],
                      "cpu_cores": result["watcher_cpu_cores"], "rss_bytes": result["after"]["memory"]["VmRSS_bytes"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
