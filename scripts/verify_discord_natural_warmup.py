"""Observe the unchanged native Discord owner's natural preparation warmup.

No forced warmup, CUDA call, provider request, signal publication, trading write
or restart. Ready is bound to this process, all configured modes and their
actual start/completion window; opening/execution remains a separate proof.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts import verify_discord_gpu_idle_runtime as native  # noqa: E402


def warm_proof(status: dict, control: dict, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    warm = status["startup_inference_warmup"]
    if warm.get("status") in {"pending", "running"}:
        return {"state": "waiting_natural_warmup", "phase": warm["status"]}
    expected = {row["market"] for row in control["signals"]}
    modes = warm.get("markets") or []
    if (warm.get("status") != "ready" or warm.get("run_id") != status["run_id"]
            or status["run_id"] != control["status"]["run_id"]
            or warm.get("model_cache_resident_ready") is not True
            or warm.get("ready_count") != len(expected) or warm.get("market_count") != len(expected)
            or len(modes) != len(expected) or {row["market"] for row in modes} != expected
            or any(row.get("status") != "ready" for row in modes)):
        raise ValueError("native_warmup_not_all_current_resident_modes")
    started, completed = (datetime.fromisoformat(warm[name]) for name in ("started_at", "completed_at"))
    seconds = warm["elapsed_seconds"]
    adopted = datetime.fromisoformat(control["observed_at_utc"])
    if (started.tzinfo is None or completed.tzinfo is None or completed < started
            or started < adopted or completed > now
            or type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0):
        raise ValueError("invalid_natural_warmup_clock")
    windows = {row["market"]: row["protection_start_minutes"] for row in control["startup_preparation"]}
    # The idle verifier intentionally loads inference dependencies lazily.
    # There is no module-global BOT; do not restore the expensive eager import
    # just to verify a pending warmup or a journal-only observation.
    bot = native.verification_cache().discord_bot
    for market in expected:
        cfg = bot._resolve_market(market)
        zone = ZoneInfo(cfg.timezone or "Asia/Taipei")
        begin, finish = started.astimezone(zone), completed.astimezone(zone)
        deadline = bot._startup_gpu_prepare_minutes(cfg)
        if (deadline is None or market not in windows or begin.date() != finish.date()
                or begin.date() != now.astimezone(zone).date()
                or begin.hour * 60 + begin.minute + begin.second / 60 < windows[market]
                or finish.hour * 60 + finish.minute + finish.second / 60 >= deadline):
            raise ValueError("natural_warmup_outside_preparation_window")
    return {"state": "accepted_native_natural_warmup", "started_at": warm["started_at"],
            "completed_at": warm["completed_at"], "elapsed_seconds": seconds,
            "ready_modes": sorted(expected), "completed_before_preparation": True}


def verify(control: dict) -> dict:
    if control.get("state") != "accepted_native_discord_gpu_idle_adoption":
        raise ValueError("requires accepted current idle-owner baseline")
    owner = native.owner()
    protected = {unit: native.identity(unit) for unit in native.PROTECTED}
    if owner != control["owner"] or protected != control["protected"]:
        raise ValueError("native_or_protected_owner_changed_since_idle_acceptance")
    if any(native.sha(name) != value for name, value in control["code_sha256"].items()):
        raise ValueError("accepted_warm_window_code_changed")
    status, process = native.status_for_owner()
    proof = warm_proof(status, control)
    acknowledged = native.engine_acknowledgements(status, [row["market"] for row in control["signals"]])
    if native.owner() != owner or protected != {unit: native.identity(unit) for unit in native.PROTECTED}:
        raise ValueError("owner_changed_during_natural_warmup_readback")
    if proof["state"] == "accepted_native_natural_warmup":
        if not native.gpu_handles(process):
            raise ValueError("native_resident_model_has_no_gpu_device_handle")
        journal = native.command_sync_since_restart()
        if journal["watchdog_or_fatal_observed"] or not journal["new_warm_ready_log_observed"]:
            raise ValueError("warmup_receipt_not_matched_to_native_terminal_log")
    else:
        journal = None
    return {**proof, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "owner": owner, "python_pid": int(process.name), "protected": protected,
            "status": status, "gpu_device_handles": native.gpu_handles(process),
            "engine_acknowledgements": acknowledged, "journal": journal,
            "provider_requests_by_verifier": 0, "forced_warmup": False,
            "cuda_calls_by_verifier": 0, "services_changed": False, "trading_writes": False,
            "opening_verified": False, "all_board_memory_attributed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink() or not args.output.resolve().is_relative_to((ROOT / "artifacts/operations").resolve()):
        parser.error("new evidence under artifacts/operations required")
    value = verify(json.loads(args.control.read_bytes()))
    atomic_write_json(args.output, value)
    print({"state": value["state"], "python_pid": value["python_pid"], "opening_verified": False})


if __name__ == "__main__":
    from stockagent.live.service_code_release import run_cli_against_active_source
    pinned = run_cli_against_active_source(ROOT, Path(__file__), sys.argv[1:])
    if pinned is None:
        main()
    else:
        raise SystemExit(pinned)
