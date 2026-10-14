"""Read back an actual bounded source wait without forcing any service work.

Acceptance proves only the loaded native watchdog respected a live source
child beyond the inference idle timeout. Source readiness, signals and fills
have their own receipts. No service/provider/CUDA/trading action is taken.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import os
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.agent_workflow import process_identity  # noqa: E402
from scripts.verify_public_feature_index_runtime import identity, sha  # noqa: E402

UNIT = "stockagent-discord-bot.service"
PROTECTED = ("stockagent-tw-day-trade-simulation.service",
             "stockagent-shioaji-taifex-bidask.service")
CODE = (ROOT / "services/discord_bot/bot.py",
        ROOT / "services/discord_bot/source_watchdog.py", Path(__file__))
WAIT = re.compile(
    r"\[scheduled-watchdog\] market=(\S+) idle=([\d.]+)s "
    r"action=wait_bounded_source kind=source_subprocess pid=(\d+) "
    r"operation_id=(\d+) remaining=([\d.]+)s source_ready=false"
)


def child_proof(pid: int, *, parent: int, market: str) -> dict:
    handle = process_identity(pid)
    if handle is None:
        raise ValueError("source_child_no_longer_live")
    proc = Path(f"/proc/{pid}")
    status = dict(line.split(":", 1) for line in (proc / "status").read_text().splitlines()
                  if ":" in line)
    argv = (proc / "cmdline").read_bytes().split(b"\0")
    if (int(status["PPid"]) != parent or status["State"].strip().startswith("Z")
            or UNIT not in (proc / "cgroup").read_text()
            or (proc / "cwd").resolve() != ROOT.resolve()
            or (proc / "exe").resolve() != Path(sys.executable).resolve()
            or b"scripts/update_tw_day_trade_factorized_live.py" not in argv
            or b"--phase" not in argv or b"preopen" not in argv
            or market != "tw_day_trade_factorized_scale_separated_cash"):
        raise ValueError("not_the_selected_owned_preopen_source_child")
    return handle


def wait_clock_proof(row: dict, *, child: dict, idle: float,
                     remaining: float, now: datetime) -> dict:
    event = datetime.fromtimestamp(int(row["__REALTIME_TIMESTAMP"]) / 1_000_000,
                                   timezone.utc)
    age = (now - event).total_seconds()
    boot_second = next(int(line.split()[1]) for line in Path("/proc/stat").read_text().splitlines()
                       if line.startswith("btime "))
    started = boot_second + int(child["start_ticks"]) / os.sysconf("SC_CLK_TCK")
    # btime is integer-second precision; permit only that rounding uncertainty.
    # An old event must not attest a reused PID or extend the original deadline.
    if (not math.isfinite(idle) or idle <= 180
            or not 0 < remaining <= 1200 or not 0 <= age < remaining
            or started > event.timestamp() + 1):
        raise ValueError("source_wait_clock_expired_or_pid_reused")
    return {"event_at_utc": event.isoformat(), "event_age_seconds": age,
            "remaining_now_seconds": remaining - age}


def verify(invocation: str) -> dict:
    owner = identity(UNIT)
    if (owner.get("ActiveState") != "active" or int(owner.get("MainPID", 0)) <= 0
            or owner.get("InvocationID") != invocation):
        raise ValueError("native_owner_changed_before_source_wait_readback")
    protected = {unit: identity(unit) for unit in PROTECTED}
    code = {str(path): sha(path) for path in CODE}
    status_path = ROOT / "artifacts/discord_bot/service_status.json"
    status = json.loads(status_path.read_bytes())
    now = datetime.now(timezone.utc)
    if not 0 <= (now - datetime.fromisoformat(status["updated_at"])).total_seconds() <= 5:
        raise ValueError("native_status_is_not_fresh")
    pid = int(status["run_id"].split("-", 1)[0])
    bot_handle = process_identity(pid)
    proc = Path(f"/proc/{pid}")
    ppid = next(line.split(":", 1)[1] for line in (proc / "status").read_text().splitlines()
                if line.startswith("PPid:"))
    if (bot_handle is None or int(ppid) != int(owner["MainPID"])
            or UNIT not in (proc / "cgroup").read_text()
            or (proc / "cwd").resolve() != ROOT.resolve()
            or (proc / "exe").resolve() != Path(sys.executable).resolve()
            or b"services/discord_bot/bot.py" not in (proc / "cmdline").read_bytes().split(b"\0")
            or status.get("core_health") != "ready" or status.get("discord_connected") is not True):
        raise ValueError("heartbeat_not_the_current_native_gateway")
    journal = subprocess.run(
        ["journalctl", "-u", UNIT, f"_SYSTEMD_INVOCATION_ID={invocation}",
         "--output=json", "--no-pager", "-n", "800"],
        check=True, capture_output=True, text=True, timeout=5,
    )
    rows = [json.loads(line) for line in journal.stdout.splitlines() if line.strip()]
    matches = [(row, WAIT.search(str(row.get("MESSAGE", "")))) for row in rows]
    matches = [(row, match) for row, match in matches if match is not None]
    if (not matches or any("action=exit_for_systemd_restart" in str(row.get("MESSAGE", ""))
                           for row in rows)):
        raise ValueError("native_wait_not_observed_or_same_owner_fatally_stalled")
    row, match = matches[-1]
    market, idle, child, operation_id, remaining = match.groups()
    if (int(row.get("_PID", 0)) != pid or row.get("_SYSTEMD_INVOCATION_ID") != invocation
            or float(idle) <= 180 or not 0 < float(remaining) <= 1200):
        raise ValueError("source_wait_not_bound_to_original_timeout_and_owner")
    child = child_proof(int(child), parent=pid, market=market)
    clocks = wait_clock_proof(row, child=child, idle=float(idle),
                             remaining=float(remaining), now=now)
    if (identity(UNIT) != owner or {unit: identity(unit) for unit in PROTECTED} != protected
            or {str(path): sha(path) for path in CODE} != code
            or process_identity(pid) != bot_handle or process_identity(child["pid"]) != child):
        raise ValueError("owner_or_code_changed_during_source_wait_readback")
    return {"contract": "native_bounded_source_watchdog_readback_v1",
            "state": "accepted_native_bounded_source_wait", "observed_at_utc": now.isoformat(),
            "owner": owner, "bot_process": bot_handle, "protected": protected,
            "source_process": child, "code_sha256": code, "run_id": status["run_id"],
            "wait_event": row, "operation_id": int(operation_id),
            "idle_at_event_seconds": float(idle), "remaining_at_event_seconds": float(remaining),
            "clocks": clocks,
            "source_ready": False, "opening_verified": False,
            "provider_calls_by_verifier": 0, "cuda_calls_by_verifier": 0,
            "services_changed_by_verifier": False, "trading_writes_by_verifier": False}


def verify_completion(control: dict) -> dict:
    """Bind natural completion to the *same* previously observed live owner.

    This is a source-preparation/engine-ack boundary, not an independent full
    projection rehash, an on-time opening or a broker-fill acceptance.
    """
    from scripts import verify_discord_gpu_idle_runtime as native

    if control.get("state") != "accepted_native_bounded_source_wait":
        raise ValueError("accepted_current_bounded_wait_control_required")
    owner = identity(UNIT)
    protected = {unit: identity(unit) for unit in PROTECTED}
    if owner != control["owner"] or protected != control["protected"]:
        raise ValueError("native_or_protected_owner_changed_since_source_wait")
    code = {str(path): sha(path) for path in CODE[:2]}
    if any(control["code_sha256"].get(path) != digest for path, digest in code.items()):
        raise ValueError("loaded_source_watchdog_changed_since_wait")
    status, process = native.status_for_owner()
    if status["run_id"] != control["run_id"]:
        raise ValueError("native_gateway_run_changed_since_source_wait")
    if process_identity(control["source_process"]["pid"]) is not None:
        raise ValueError("original_source_child_still_live_or_pid_reused")
    receipt_path = ROOT / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1/updater_status.json"
    receipt = json.loads(receipt_path.read_bytes())
    target_date = datetime.fromtimestamp(int(control["wait_event"]["__REALTIME_TIMESTAMP"]) / 1_000_000,
                                         timezone.utc).astimezone(ZoneInfo("Asia/Taipei")).date().isoformat()
    if (receipt.get("status") != "verified" or receipt.get("phase") != "preopen"
            or receipt.get("attempt") != 1 or receipt.get("decision_session") != target_date
            or not 180 < receipt.get("wall_seconds", 0) < 1200):
        raise ValueError("not_the_current_completed_source_preparation_receipt")
    manifest_path = Path(receipt["manifest"])
    if (ROOT / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1/current").resolve() != manifest_path.parent.resolve():
        raise ValueError("source_projection_pointer_not_the_completed_generation")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("status") != "complete":
        raise ValueError("canonical_projection_not_complete")
    journal = subprocess.run(
        ["journalctl", "-u", UNIT, "_SYSTEMD_INVOCATION_ID=" + owner["InvocationID"],
         "--output=cat", "--no-pager", "-n", "800"],
        check=True, capture_output=True, text=True, timeout=5,
    ).stdout
    market = "tw_day_trade_factorized_scale_separated_cash"
    if ("action=wait_bounded_source" not in journal or "action=exit_for_systemd_restart" in journal
            or f"[pre-signal:{market}] done returncode=0" not in journal
            or f"[scheduled] market={market} status=awaiting_engine_confirmation session={target_date}" not in journal):
        raise ValueError("same_native_owner_did_not_complete_source_and_publish_signal")
    acknowledgements = native.engine_acknowledgements(status, status["scheduled_day_trade_markets"])
    if len(acknowledgements) != 5:
        raise ValueError("native_engine_did_not_acknowledge_all_five_modes")
    if (identity(UNIT) != owner or {unit: identity(unit) for unit in PROTECTED} != protected
            or {str(path): sha(path) for path in CODE[:2]} != code):
        raise ValueError("owner_or_loaded_code_changed_during_terminal_readback")
    return {"contract": "native_source_preparation_terminal_v1",
            "state": "accepted_native_source_preparation_terminal",
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "owner": owner, "protected": protected, "code_sha256": code,
            "bot_process": process_identity(int(process.name)), "run_id": status["run_id"],
            "source_process_before": control["source_process"], "source_process_terminated": True,
            "receipt": receipt, "receipt_sha256": sha(receipt_path),
            "manifest_sha256": sha(manifest_path), "engine_acknowledgements": acknowledgements,
            "source_ready": True, "opening_slo_verified": False, "broker_fill_verified": False,
            "full_projection_independently_rehashed": False, "provider_calls_by_verifier": 0,
            "cuda_calls_by_verifier": 0, "services_changed_by_verifier": False,
            "trading_writes_by_verifier": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--invocation")
    mode.add_argument("--terminal-control", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (args.output.exists() or args.output.is_symlink()
            or not args.output.resolve().is_relative_to((ROOT / "artifacts/operations").resolve())):
        parser.error("new evidence under artifacts/operations is required")
    if args.terminal_control is None:
        value = verify(args.invocation)
    else:
        value = verify_completion(json.loads(args.terminal_control.read_bytes()))
        value["control_path"] = str(args.terminal_control)
        value["control_sha256"] = sha(args.terminal_control)
    atomic_write_json(args.output, value)
    print({"state": value["state"], "source_ready": value["source_ready"],
           "opening_slo_verified": False})


if __name__ == "__main__":
    main()
