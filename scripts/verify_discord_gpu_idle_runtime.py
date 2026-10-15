"""Read-only admission/adoption evidence for the canonical Discord idle window.

No model/CUDA/provider calls or service action. The operator may restart only
the Discord owner after --baseline is ready. --control verifies the changed
owner, fresh Gateway/engine acknowledgements and absence of GPU device handles.
The actual next warmup/opening remains a separate acceptance boundary.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts import verify_completed_session_cache as cache  # noqa: E402
from scripts.audit_service_latency_coverage import gpu_snapshot  # noqa: E402
from scripts.verify_public_feature_index_runtime import identity, sha  # noqa: E402
from stockagent.live.tw_day_trade_service_sync import load_service_sync  # noqa: E402

BOT = cache.discord_bot
UNIT = "stockagent-discord-bot.service"
PROTECTED = (
    "stockagent-tw-day-trade-simulation.service", "stockagent-shioaji-taifex-bidask.service",
    "stockagent-shioaji-historical-market-data.service", "stockagent-finmind-complement.service",
    "stockagent-shioaji-taifex-dashboard.service", "stockagent-public-dashboards.service",
)
CODE = (Path(__file__), ROOT / "services/discord_bot/bot.py", ROOT / "stockagent/live/signal_engine.py",
        ROOT / "stockagent/live/completed_inference_cache.py", ROOT / "scripts/run_discord_bot.sh",
        Path("/etc/systemd/system") / UNIT)


def owner():
    value = identity(UNIT)
    if value.get("ActiveState") != "active" or not value.get("InvocationID") or int(value.get("MainPID", 0)) <= 0:
        raise ValueError("discord_native_owner_not_running")
    return value


def status_for_owner():
    value = json.loads(BOT._discord_service_status_path().read_bytes())
    observed = datetime.fromisoformat(value["updated_at"])
    if not 0 <= (datetime.now(timezone.utc) - observed).total_seconds() <= 5:
        raise ValueError("discord_status_not_current")
    pid = int(value["run_id"].split("-", 1)[0])
    process = Path(f"/proc/{pid}")
    if (UNIT not in (process / "cgroup").read_text()
            or (process / "cwd").resolve() != ROOT.resolve()
            or (process / "exe").resolve() != Path(sys.executable).resolve()):
        raise ValueError("discord_heartbeat_not_the_canonical_python_owner")
    if value.get("discord_connected") is not True or value.get("core_health") != "ready":
        raise ValueError("discord_gateway_not_ready")
    return value, process


def pending_work(status):
    interactive = status["interactive_signal_jobs"]
    if interactive["running_count"] or interactive["active_count"]:
        raise ValueError("interactive_signal_work_active")
    if status["background_maintenance"]["running_count"]:
        raise ValueError("artifact_maintenance_active")
    warm = status["startup_inference_warmup"]
    if warm.get("status") == "running":
        raise ValueError("startup_warmup_active")
    if (BOT._opening_critical_work_pending() or BOT._interactive_signal_work_pending()
            or BOT._tw_public_refresh_in_progress()):
        raise ValueError("canonical_idle_admission_deferred")
    if not BOT._startup_inference_warmup_must_defer() or not BOT._offhours_gpu_cache_release_allowed():
        raise ValueError("not_the_closed_preparation_idle_window")


def gpu_handles(process):
    found = []
    for path in (process / "fd").iterdir():
        try:
            target = str(path.readlink())
        except FileNotFoundError:
            continue
        if target == "/dev/dxg" or target.startswith("/dev/nvidia"):
            found.append({"fd": path.name, "target": target})
    return found


def engine_acknowledgements(status, expected_markets):
    engine = load_service_sync(BOT._day_trade_state_dir())
    names = set(expected_markets)
    if not isinstance(engine, dict) or set(status["scheduled_day_trade_markets"]) != names:
        raise ValueError("discord_enabled_modes_not_acknowledged")
    lag = int(engine["state_revision"]) - int(status["engine_state_revision"])
    if lag != 0 or not engine["engine_run_id"] or engine["engine_run_id"] != status["engine_run_id"]:
        raise ValueError("discord_engine_ack_not_current")
    acknowledgements = []
    for market in sorted(names):
        mode = engine["modes"].get(market)
        if (not isinstance(mode, dict) or not mode.get("signal_id")
                or status["mode_signal_ids"].get(market) != mode["signal_id"]):
            raise ValueError("discord_mode_signal_not_acknowledged")
        acknowledgements.append({"market": market, "signal_id": mode["signal_id"], "lag": 0})
    return acknowledgements


def command_sync_since_restart():
    result = subprocess.run(
        ["journalctl", "-u", UNIT, "--invocation=0", "--output=cat", "--no-pager", "-n", "100"],
        capture_output=True, text=True, check=True, timeout=5,
    )
    text = result.stdout
    return {"commands_sync_or_deferred_observed": bool(re.search(r"^synced \d+ global app commands ", text, re.M))
            or "global app command sync deferred; local signal scheduler remains active " in text,
            "watchdog_or_fatal_observed": any(marker in text for marker in (
        "action=exit_for_systemd_restart", "Fatal Python error", "Watchdog timeout",
    )), "new_warm_ready_log_observed": "[startup-warm] status=ready" in text}


def current_code_admission() -> dict:
    return {"contract": "live_implementation_process_admission_v1",
            "code_sha256": cache.forward_cache.identity_digest(
                {"code": cache.forward_cache.loaded_implementation_identity()}),
            "requires_fresh_process_on_change": True}


def verify(benchmarks: Path | None, controls: Path | None, control=None, *,
           code_admission_only: bool = False):
    contract = ("discord_native_loaded_code_admission_v1" if code_admission_only else
                "discord_preparation_gpu_idle_runtime_v1")
    if (code_admission_only and (benchmarks is not None or controls is not None)
            or not code_admission_only and (benchmarks is None or controls is None)):
        raise ValueError("code admission and independent numerical acceptance are separate modes")
    if control is not None and control.get("contract") != contract:
        raise ValueError("discord_idle_control_contract_mismatch")
    current = owner()
    protected = {unit: identity(unit) for unit in PROTECTED}
    proofs = {str(path): sha(path) for path in CODE}
    status, process = status_for_owner()
    pending_work(status)
    full = cache.verify()
    if not code_admission_only:
        full["benchmarks"] = cache.matched_benchmarks(full, benchmarks, controls)
    acknowledgements = engine_acknowledgements(status, [row["market"] for row in full["markets"]])
    signals = [{"market": row["market"], "cache_key": row["cache_key"], "summary_path": row["summary_path"],
                "summary_sha256": sha(row["summary_path"])} for row in full["markets"]]
    result = {
        "contract": contract,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), "state": "ready_for_discord_idle_reload",
        "owner": current, "protected": protected, "code_sha256": proofs, "status": status,
        "python_pid": int(process.name), "gpu_device_handles": gpu_handles(process),
        "engine_acknowledgements": acknowledgements,
        "cache_acceptance": full, "signals": signals, "board_telemetry": gpu_snapshot(),
        "startup_preparation": [{"market": market,
            "protection_start_minutes": BOT._startup_gpu_protection_start_minutes(BOT._resolve_market(market))}
            for market in BOT._scheduled_markets()
            if getattr(BOT._resolve_market(market), "day_trade_simulation_enabled", False)],
        "services_started_or_restarted_by_verifier": False, "provider_requests": 0, "gpu_calls": 0,
        "trading_writes": False, "source_files_written": 0,
        "next_prewarm_and_opening_verified": False, "all_board_memory_attributed": False,
        "full_numerical_oracle_proven": not code_admission_only,
        "runtime_code_admission": status.get("live_implementation_admission"),
    }
    if owner() != current or protected != {unit: identity(unit) for unit in PROTECTED}:
        raise ValueError("native_owner_generation_changed_during_readback")
    if control is not None:
        for key in ("protected", "code_sha256", "signals", "startup_preparation"):
            if result[key] != control[key]:
                raise ValueError(f"discord_idle_adoption_control_changed:{key}")
        if current["InvocationID"] == control["owner"]["InvocationID"]:
            raise ValueError("discord_owner_not_reloaded")
        if (result["gpu_device_handles"] or status["startup_inference_warmup"].get("status") == "ready"):
            raise ValueError("discord_native_owner_not_gpu_cold")
        if code_admission_only and result["runtime_code_admission"] != current_code_admission():
            raise ValueError("discord_loaded_implementation_not_current")
        result["journal"] = command_sync_since_restart()
        if (not result["journal"]["commands_sync_or_deferred_observed"]
                or result["journal"]["watchdog_or_fatal_observed"]
                or result["journal"]["new_warm_ready_log_observed"]):
            raise ValueError("unexpected_native_owner_warmup_or_fatal")
        result["state"] = ("accepted_native_discord_loaded_code_admission" if code_admission_only else
                           "accepted_native_discord_gpu_idle_adoption")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--control", type=Path)
    parser.add_argument("--benchmarks", type=Path)
    parser.add_argument("--controls", type=Path)
    parser.add_argument("--code-admission-only", action="store_true",
                        help="Verify stable current artifacts and native loaded-code ownership; "
                             "not an independent GPU numerical benchmark")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.baseline == (args.control is not None):
        parser.error("choose baseline or control")
    if args.code_admission_only == (args.benchmarks is not None or args.controls is not None):
        parser.error("code admission takes no benchmarks; numerical acceptance requires both paths")
    if not args.code_admission_only and (args.benchmarks is None or args.controls is None):
        parser.error("numerical acceptance requires both --benchmarks and --controls")
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError("preserve idle-adoption evidence")
    if not args.output.resolve().is_relative_to((ROOT / "artifacts/operations").resolve()):
        raise ValueError("runtime evidence must remain under artifacts/operations")
    result = {"state": "failed"}
    try:
        result = verify(args.benchmarks, args.controls,
                        None if args.baseline else json.loads(args.control.read_bytes()),
                        code_admission_only=args.code_admission_only)
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        raise
    finally:
        atomic_write_json(args.output, result)
    print(json.dumps({"state": result["state"], "python_pid": result["python_pid"],
                      "gpu_device_handles": len(result["gpu_device_handles"])}))


if __name__ == "__main__":
    main()
