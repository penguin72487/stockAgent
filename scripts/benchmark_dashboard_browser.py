#!/usr/bin/env python3
"""Own and close one browser for the canonical read-only CDP timing probe.

Watch mode can arm before there are signals and never forces full history.
Receipts and append-only update observations survive a failed/timeout probe.
This does not log in to a provider, submit orders, or alter an engine/ledger.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.probe_browser_runtime import browser_profile_launch_options


def validate_probe(url: str, output: Path, *, watch_ms: int, port: int) -> Path:
    target = urlsplit(url)
    if (target.scheme not in {"http", "https"} or not target.hostname
            or target.username is not None or target.password is not None
            or target.query or target.fragment):
        raise ValueError("browser URL must be an HTTP(S) page without credentials/query/fragment")
    if type(watch_ms) is not int or not 0 <= watch_ms <= 900000:
        raise ValueError("watch-ms must be 0-900000")
    if type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError("CDP port must be 1024-65535")
    output = output.resolve()
    for candidate in (output, Path(str(output) + ".updates.jsonl"), Path(str(output) + ".launch.json")):
        if candidate.exists() or candidate.is_symlink():
            raise FileExistsError(f"browser evidence already exists: {candidate}")
    return output


def scheduled_probe_plan(now: datetime, root: Path) -> dict:
    """Persist missed arms as missed, never replay a late page as the open.

    This is a public-page observer, not a second trading-calendar authority.
    Holidays may produce an empty page observation; only native market/source
    receipts can establish an actual trading session and its opening SLO.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("scheduled browser observer needs an aware clock")
    local = now.astimezone(ZoneInfo("Asia/Taipei"))
    in_window = local.weekday() < 5 and local.hour == 8 and local.minute >= 58
    stamp = now.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return {"session_date": local.date().isoformat(),
        "scheduled_at_taipei": local.isoformat(timespec="microseconds"),
        "state": "armed" if in_window else "missed_opening_observation_window",
        "output": str(root / local.date().isoformat() / f"{stamp}.json"),
        "native_opening_success_claim": False,
        "boundary": "Read-only page observer; session, source, signal and full fills require independent native evidence"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="https://penguin72487.dpdns.org/tw-day-trade/")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--watch-ms", type=int)
    parser.add_argument("--scheduled", action="store_true",
                        help="Daily preopen observer; persist a missed-window receipt rather than running late")
    parser.add_argument("--scheduled-root", type=Path, default=ROOT / "artifacts/operations/tw_opening_browser")
    parser.add_argument("--cdp-port", type=int, default=9238)
    parser.add_argument("--browser-profile", choices=("default", "cpu-2d"), default="cpu-2d")
    args = parser.parse_args()
    scheduled = scheduled_probe_plan(datetime.now(UTC), args.scheduled_root) if args.scheduled else None
    if args.output is None:
        if scheduled is None:
            parser.error("output is required for a manual probe")
        args.output = Path(scheduled["output"])
    if args.watch_ms is None:
        args.watch_ms = 900000 if scheduled else 0
    try:
        output = validate_probe(args.url, args.output, watch_ms=args.watch_ms, port=args.cdp_port)
    except (ValueError, FileExistsError) as exc:
        parser.error(str(exc))
    output.parent.mkdir(parents=True, exist_ok=True)
    proof = {"contract": "owned_native_dashboard_browser_probe_v1", "url": args.url,
        "started_at_utc": datetime.now(UTC).isoformat(), "status": "failed",
        "browser_profile": args.browser_profile, "watch_ms": args.watch_ms,
        "simulation_only": True, "broker_orders": False, "ledger_writes": False,
        "scheduled_observation": scheduled,
        "workspace_code_at_probe_start": {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in ("scripts/benchmark_dashboard_browser.py", "scripts/benchmark_dashboard_browser.mjs")},
        "boundary": "Fresh owned browser; server/OS caches uncontrolled; no native opening success inferred from a night probe"}
    code = 1
    try:
        if scheduled is not None and scheduled["state"] != "armed":
            proof["status"] = scheduled["state"]
            proof["returncode"] = 0
            code = 0
            return code
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            options = browser_profile_launch_options(args.browser_profile)
            options.setdefault("args", []).append(f"--remote-debugging-port={args.cdp_port}")
            browser = pw.chromium.launch(**options, timeout=10000)
            proof["chromium_version"] = browser.version
            try:
                result = subprocess.run(["node", str(ROOT / "scripts/benchmark_dashboard_browser.mjs"),
                    str(args.cdp_port), args.url, str(output), f"--watch-ms={args.watch_ms}"],
                    cwd=ROOT, text=True, capture_output=True, timeout=180 + args.watch_ms / 1000)
                proof.update(returncode=result.returncode, stderr=result.stderr[-4000:])
                code = result.returncode or (0 if output.is_file() else 1)
                proof["status"] = "passed" if code == 0 else "failed"
            finally:
                browser.close()
    except Exception as exc:
        code = 1
        proof.update(status="failed", error_type=type(exc).__name__, error=str(exc)[:4000])
    finally:
        proof["finished_at_utc"] = datetime.now(UTC).isoformat()
        atomic_write_json(Path(str(output) + ".launch.json"), proof, durable=True)
        print(json.dumps({"status": proof["status"], "receipt": str(output),
                          "launcher_receipt": str(output) + ".launch.json"}))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
