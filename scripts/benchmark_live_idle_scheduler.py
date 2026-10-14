#!/usr/bin/env python3
"""Fresh-process ABBA of canonical idle callbacks, never a live-signal benchmark."""
from __future__ import annotations

import argparse
import ast
import asyncio
from dataclasses import asdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import resource
import subprocess
import sys
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
SOURCE = ROOT / "services/discord_bot/bot.py"
ORIGINALS = {"_preopen_market_symbol_count", "scheduled_signal", "postclose_fast_cache"}
ALLOWED_NEW = {
    "_preopen_readiness_payload", "_scheduler_poll_seconds",
    "_postclose_scheduler_poll_seconds", "_set_scheduler_poll_seconds",
}


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def nodes(source: str, *, omit=frozenset()) -> list[str]:
    return [ast.dump(node, include_attributes=False) for node in ast.parse(source).body
            if getattr(node, "name", "") not in omit]


def capture(directory: Path, clock: str) -> None:
    from services.discord_bot import bot as module

    directory.mkdir(parents=True, exist_ok=False)
    original = SOURCE.read_bytes()
    (directory / "original_bot.py").write_bytes(original)
    (directory / "original_bot.py").chmod(0o400)
    configs = {name: asdict(module._resolve_market(name))
               for name in module._scheduled_markets()}
    source_state = module.STATE_PATH.read_bytes() if module.STATE_PATH.exists() else b"{}"
    readiness = module._preopen_readiness_path().read_bytes()
    (directory / "state.json").write_bytes(source_state)
    (directory / "readiness.json").write_bytes(readiness)
    write(directory / "fixture.json", {
        "schema_version": 1, "clock": clock, "configs": configs,
        "original_sha256": digest(original), "state_sha256": digest(source_state),
        "readiness_sha256": digest(readiness),
        "scope": "All enabled configured markets, two complete already-settled idle callbacks; no API/model/trading work",
    })
    print(json.dumps({"captured": str(directory), "markets": sorted(configs)}))


async def worker(directory: Path, policy: str, seconds: float) -> dict:
    from services.discord_bot import bot as module
    import torch

    original = (directory / "original_bot.py").read_text()
    fixture = json.loads((directory / "fixture.json").read_text())
    current = SOURCE.read_bytes()
    if digest(original.encode()) != fixture["original_sha256"]:
        raise RuntimeError("Original source changed")
    if nodes(original, omit=ORIGINALS) != nodes(current.decode(), omit=ORIGINALS | ALLOWED_NEW):
        raise RuntimeError("Unexpected non-scheduler source change")
    for name, key in (("state.json", "state_sha256"), ("readiness.json", "readiness_sha256")):
        if digest((directory / name).read_bytes()) != fixture[key]:
            raise RuntimeError("Idle input fixture changed")
    if policy == "original":
        selected = [node for node in ast.parse(original).body if getattr(node, "name", "") in ORIGINALS]
        exec(compile(ast.Module(body=selected, type_ignores=[]), str(directory / "original_bot.py"), "exec"),
             module.__dict__)

    fixed = datetime.fromisoformat(fixture["clock"])

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz is not None else fixed.replace(tzinfo=None)

    configs = {key: SimpleNamespace(**value) for key, value in fixture["configs"].items()}
    module.datetime = Clock
    module._market_configs = lambda: configs
    module.STATE_PATH = directory / "state.json"
    module._preopen_readiness_path = lambda: directory / "readiness.json"
    reads = {"state": 0, "readiness": 0}
    original_read_text = Path.read_text

    def counted_read_text(path, *args, **kwargs):
        if path == directory / "state.json":
            reads["state"] += 1
        elif path == directory / "readiness.json":
            reads["readiness"] += 1
        return original_read_text(path, *args, **kwargs)

    Path.read_text = counted_read_text
    # Keep the real calendar implementation and its fail-closed decisions.
    decisions = {name: module._scheduled_market_session_day(cfg, Clock.now(module.ZoneInfo(cfg.timezone)))
                 for name, cfg in configs.items()}
    module.bot._last_scheduled_keys = {
        key for cfg in configs.values()
        if (key := module._scheduled_signal_key(cfg, Clock.now(module.ZoneInfo(cfg.timezone)))) is not None
    }
    module.bot._postclose_fast_completed_keys = {
        key for cfg in configs.values()
        if (key := module._postclose_fast_clock_key(cfg, Clock.now(module.ZoneInfo(cfg.timezone)), phase="cache"))
    }

    def forbidden(*args, **kwargs):
        raise AssertionError("Idle benchmark reached provider/model/publication/trading")

    for name in ("_ensure_signal_ready", "_run_market_signal_sync", "_enqueue_scheduled_results",
                 "_write_postclose_fast_cache_status_best_effort", "_prepare_scheduled_attempt_inputs"):
        setattr(module, name, forbidden)
    for name in ("is_available", "synchronize", "empty_cache", "current_device"):
        setattr(torch.cuda, name, forbidden)
    if torch.cuda.is_initialized():
        raise AssertionError("CUDA already initialized")
    loops = (module.scheduled_signal, module.postclose_fast_cache)
    counts = {"scheduled_signal": 0, "postclose_fast_cache": 0}
    for name, loop in zip(counts, loops):
        callback = loop.coro

        async def counted(name=name, callback=callback):
            counts[name] += 1
            await callback()

        loop.coro = counted
        loop._before_loop = None  # No Discord login; equal phase in both policies.
    expected_keys = sorted(module.bot._last_scheduled_keys)
    expected_close_keys = sorted(module.bot._postclose_fast_completed_keys)
    reads = dict.fromkeys(reads, 0)
    started, cpu_started = time.perf_counter(), time.process_time()
    for loop in loops:
        loop.start()
    await asyncio.sleep(seconds)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*(loop.get_task() for loop in loops), return_exceptions=True)
    wall, cpu = time.perf_counter() - started, time.process_time() - cpu_started
    if any(loop.failed() for loop in loops):
        raise AssertionError("Canonical scheduler loop failed")
    if sorted(module.bot._last_scheduled_keys) != expected_keys or sorted(module.bot._postclose_fast_completed_keys) != expected_close_keys:
        raise AssertionError("Idle scheduling state changed")
    final_decisions = {name: module._scheduled_market_session_day(cfg, Clock.now(module.ZoneInfo(cfg.timezone)))
                       for name, cfg in configs.items()}
    if final_decisions != decisions or SOURCE.read_bytes() != current or torch.cuda.is_initialized():
        raise AssertionError("Source/calendar/CUDA boundary changed")
    return {"policy": policy, "wall_seconds": wall, "cpu_seconds": cpu,
            "max_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            "counts": counts, "reads": reads, "calendar_decisions": decisions,
            "state": {"scheduled_keys": expected_keys, "postclose_keys": expected_close_keys},
            "source_sha256": digest(current), "cuda_initialized": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--clock", default="2026-10-14T19:00:00+08:00")
    parser.add_argument("--policy", choices=("original", "candidate"))
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--attempt", default="stable-v2")
    args = parser.parse_args()
    if args.capture:
        capture(args.directory, args.clock)
        return
    if not 10 <= args.seconds <= 120:
        parser.error("seconds must be between 10 and 120")
    if args.policy:
        print(json.dumps(asyncio.run(worker(args.directory, args.policy, args.seconds)), ensure_ascii=False))
        return
    output = args.directory / args.attempt
    output.mkdir(exist_ok=False)
    results = []
    for index, policy in enumerate(("original", "candidate", "candidate", "original")):
        command = [sys.executable, str(Path(__file__).resolve()), "--directory", str(args.directory),
                   "--policy", policy, "--seconds", str(args.seconds)]
        result = subprocess.run(command, text=True, capture_output=True)
        (output / f"trial-{index}-{policy}.stdout.log").write_text(result.stdout)
        (output / f"trial-{index}-{policy}.stderr.log").write_text(result.stderr)
        if result.returncode:
            write(output / "failed.json", {"policy": policy, "index": index,
                                           "returncode": result.returncode})
            raise RuntimeError(f"Worker failed; retained stderr: {output / f'trial-{index}-{policy}.stderr.log'}")
        row = json.loads(result.stdout.splitlines()[-1])
        results.append(row)
        write(output / f"trial-{index}-{policy}.json", row)
    if len({json.dumps(row["state"], sort_keys=True) for row in results}) != 1:
        raise AssertionError("ABBA scheduling state mismatch")
    if len({json.dumps(row["calendar_decisions"], sort_keys=True) for row in results}) != 1:
        raise AssertionError("ABBA calendar decision mismatch")
    if len({row["source_sha256"] for row in results}) != 1:
        raise AssertionError("ABBA candidate source changed")
    write(output / "receipt.json", {
        "status": "accepted_bounded_idle_callback_abba", "trials": results,
        "scope": "Two canonical already-settled idle callbacks; real configured markets and calendar; no source download, inference, order, or native service adoption claim",
    })
    print(json.dumps({"status": "accepted_bounded_idle_callback_abba", "receipt": str(output / "receipt.json")}))


if __name__ == "__main__":
    main()
