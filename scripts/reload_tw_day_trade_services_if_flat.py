#!/usr/bin/env python3
"""Plan a qualified paper-service reload; use --apply to actually restart.

Discord may reload independently while the engine keeps running. An engine
reload still requires off-session flat inventory and no pending paper orders.
"""

from __future__ import annotations

import argparse
from datetime import datetime, time as wall_time
import json
from pathlib import Path
import subprocess
import sys
import time
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.live.day_trade_maintenance import control_lease
from stockagent.live.tw_day_trade_service_sync import read_service_ack_pair

TAIPEI = ZoneInfo("Asia/Taipei")
STATE_PATH = REPO_ROOT / "artifacts/live/tw_day_trade_simulation/state.json"
RECEIPT_PATH = (
    REPO_ROOT / "artifacts/live/tw_day_trade_simulation/safe_reload_receipt.json"
)
SERVICES = (
    "stockagent-discord-bot.service",
    "stockagent-tw-day-trade-simulation.service",
)
BOT_STATUS = REPO_ROOT / "artifacts/discord_bot/service_status.json"
SYNC_PATH = STATE_PATH.parent / "service_sync.json"
PROTECTED_SERVICES = (
    SERVICES[1], "stockagent-tw-overnight-simulation.service",
    "stockagent-shioaji-taifex-bidask.service", "stockagent-shioaji-taifex-dashboard.service",
    "stockagent-public-dashboards.service", "stockagent-public-cloudflared.service",
)


def read_object(path):
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError(f"object receipt required: {path.name}")
    return value


def fresh(receipt, field, now):
    value = datetime.fromisoformat(str(receipt.get(field, "")).replace("Z", "+00:00"))
    if value.tzinfo is None or not 0 <= (now - value).total_seconds() <= 10:
        raise ValueError(f"stale or future receipt: {field}")


def reload_plan(state, bot, sync, *, now, discord_only):
    """Fail closed for unknown/malformed state; do not equate it to flat/idle."""
    fresh(bot, "updated_at", now)
    fresh(bot, "engine_published_at", now)
    fresh(sync, "published_at", now)
    if any(value.get("production_order_possible") is not False for value in (state, bot, sync)):
        raise ValueError("explicit paper-only receipts required")
    if sync.get("ledger_integrity_ready") is not True:
        raise ValueError("engine ledger integrity is not accepted")
    if (not isinstance(bot.get("run_id"), str) or not bot["run_id"]
            or not isinstance(sync.get("engine_run_id"), str) or not sync["engine_run_id"]
            or type(sync.get("state_revision")) is not int or sync["state_revision"] <= 0):
        raise ValueError("native owner identities and revision required")
    modes, enabled = state.get("modes"), state.get("enabled_markets")
    if (not isinstance(modes, dict) or not modes or not isinstance(enabled, list)
            or not enabled or not set(enabled).issubset(modes)):
        raise ValueError("complete enabled-mode state required")
    if not bot.get("discord_connected") or bot.get("core_health") != "ready":
        raise ValueError("Discord is not connected/ready; use failure recovery, not an idle cutover")
    opening, jobs, maintenance = (bot.get(name) for name in (
        "opening_input_dispatch", "interactive_signal_jobs", "background_maintenance"))
    if not all(isinstance(value, dict) for value in (opening, jobs, maintenance)):
        raise ValueError("native work-queue receipts required")
    counts = [opening.get("inflight_mode_count"), maintenance.get("running_count")]
    counts.extend(jobs.get(field) for field in (
        "active_count", "running_count", "queued_count", "waiting_source_count"))
    if any(type(value) is not int or value != 0 for value in counts):
        raise ValueError("Discord has active, pending or unknown work")
    if opening.get("scheduled_delivery_pending") is not False or opening.get("input_stages"):
        raise ValueError("opening inputs/delivery still pending")
    idle_warmup = {"ready", "disabled", "pending"} if discord_only else {"ready", "disabled"}
    if (bot.get("startup_inference_warmup") or {}).get("status") not in idle_warmup:
        raise ValueError("startup inference warmup is not idle/ready")
    clock = now.astimezone(TAIPEI).time().replace(tzinfo=None)
    if wall_time(8, 59, 30) <= clock < wall_time(9, 1):
        raise ValueError("opening protection window forbids deliberate reload")
    if not discord_only and wall_time(8, 30) <= clock < wall_time(13, 40):
        raise ValueError("engine reload requires off-session flat state")
    identities, nonflat, pending = {}, 0, 0
    for market, mode in modes.items():
        if not isinstance(mode, dict) or not isinstance(mode.get("positions"), dict):
            raise ValueError(f"malformed mode/positions: {market}")
        if mode.get("entry_retry_pending_commit") or mode.get("ledger_state_divergence"):
            raise ValueError(f"uncommitted or divergent ledger: {market}")
        for position in mode["positions"].values():
            if not isinstance(position, dict) or type(position.get("signed_shares")) is not int:
                raise ValueError(f"unknown position quantity: {market}")
            nonflat += bool(position["signed_shares"])
        for field in ("pending_entry_orders", "pending_reduction_orders"):
            orders = mode.get(field, {})
            if not isinstance(orders, dict):
                raise ValueError(f"malformed pending orders: {market}")
            pending += len(orders)
        if market in enabled:
            signal = mode.get("signal_id")
            if (not signal or signal != (bot.get("mode_signal_ids") or {}).get(market)
                    or signal != ((sync.get("modes") or {}).get(market) or {}).get("signal_id")):
                raise ValueError(f"signal acknowledgement differs: {market}")
            identities[market] = signal
    if not discord_only and (nonflat or pending):
        raise ValueError(f"engine is nonflat or has pending orders: {nonflat}/{pending}")
    if bot.get("engine_run_id") != sync.get("engine_run_id"):
        raise ValueError("Discord acknowledges another engine run")
    return {"services": list(SERVICES[:1] if discord_only else SERVICES),
            "discord_only": discord_only, "nonflat_count": nonflat, "pending_orders": pending,
            "signal_ids": identities, "engine_run_id": sync["engine_run_id"],
            "engine_state_revision": sync["state_revision"],
            "previous_discord_run_id": bot["run_id"]}


def owner(service):
    result = subprocess.run(["systemctl", "show", service, "-p", "ActiveState",
                             "-p", "MainPID", "-p", "InvocationID"],
                            check=True, capture_output=True, text=True, timeout=10)
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def native_ready(bot, sync, plan, *, now, expected_code, expected_entrypoint):
    fresh(bot, "updated_at", now)
    fresh(bot, "engine_published_at", now)
    fresh(sync, "published_at", now)
    warmup = bot.get("startup_inference_warmup") or {}
    warm_markets = warmup.get("markets") or []
    warm_names = [row.get("market") for row in warm_markets
                  if isinstance(row, dict) and row.get("status") == "ready"]
    return bool(
        isinstance(bot.get("run_id"), str) and bool(bot["run_id"])
        and bot["run_id"] != plan["previous_discord_run_id"]
        and bot.get("discord_connected") is True and bot.get("core_health") == "ready"
        and all(value.get("production_order_possible") is False for value in (bot, sync))
        and sync.get("ledger_integrity_ready") is True
        and bot.get("engine_run_id") == sync.get("engine_run_id")
        and (not plan["discord_only"] or sync.get("engine_run_id") == plan["engine_run_id"])
        and type(bot.get("engine_state_revision")) is int
        and bot["engine_state_revision"] == sync.get("state_revision")
        and (not plan["discord_only"] or bot["engine_state_revision"] >= plan["engine_state_revision"])
        and (bot.get("live_implementation_admission") or {}).get("code_sha256") == expected_code
        and (bot.get("live_implementation_admission") or {}).get("entrypoint_sha256") == expected_entrypoint
        and warmup.get("status") == "ready" and warmup.get("run_id") == bot["run_id"]
        and warmup.get("model_cache_resident_ready") is True
        and warmup.get("ready_count") == warmup.get("market_count") == len(plan["signal_ids"])
        and len(warm_names) == len(set(warm_names)) == len(plan["signal_ids"])
        and set(warm_names) == set(plan["signal_ids"])
        and all((bot.get("mode_signal_ids") or {}).get(market) == signal
                and ((sync.get("modes") or {}).get(market) or {}).get("signal_id") == signal
                for market, signal in plan["signal_ids"].items())
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--discord-only", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Otherwise inspect only; never restart")
    parser.add_argument("--output", type=Path, default=RECEIPT_PATH)
    parser.add_argument("--timeout-seconds", type=float, default=240)
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 600:
        parser.error("timeout-seconds must be between 1 and 600")
    receipt = {"observed_at": datetime.now(TAIPEI).isoformat(), "status": "blocked",
               "broker_orders": 0, "operator_ledger_writes": 0, "restart_requested": False}
    try:
        with control_lease(owner="qualified-paper-service-reload", timeout_seconds=0):
            plan = reload_plan(read_object(STATE_PATH), read_object(BOT_STATUS), read_object(SYNC_PATH),
                               now=datetime.now(TAIPEI), discord_only=args.discord_only)
            protected = PROTECTED_SERVICES if args.discord_only else PROTECTED_SERVICES[1:]
            before = {name: owner(name) for name in protected}
            if any(owner(name).get("ActiveState") != "active" for name in plan["services"]):
                raise ValueError("requested owner is not active; refuse to enable a stopped role")
            receipt.update(plan=plan, protected_before=before, status="planned")
            if args.apply:
                selected_before = {name: owner(name) for name in plan["services"]}
                from scripts.audit_tw_day_trade_restart import prefix_sha256, verify_prefix
                from stockagent.live.completed_inference_cache import implementation_identity, identity_digest, file_identity
                from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard
                from stockagent.live.market_config import load_market_config
                from stockagent.config import load_config
                inputs = {name: {"size_bytes": (STATE_PATH.parent / name).stat().st_size}
                          for name in ("events.jsonl", "orders.jsonl", "fills.jsonl")}
                for name, value in inputs.items():
                    value["sha256"] = prefix_sha256(STATE_PATH.parent / name, value["size_bytes"])
                code = implementation_identity()
                entrypoint = file_identity(REPO_ROOT / "services/discord_bot/bot.py")
                expected_code = identity_digest({"code": code})
                cfg = load_market_config(REPO_ROOT / "services/discord_bot/markets"
                                         / (next(iter(plan["signal_ids"])) + ".yaml"))
                config = load_config(cfg.config_path)
                # A producer mid-commit is retryable, not a reason to kill it
                # or to warm from an unaccepted mixed reference generation.
                with live_tw_public_source_read_guard(config.data.parquet_root):
                    plan = reload_plan(read_object(STATE_PATH), read_object(BOT_STATUS), read_object(SYNC_PATH),
                                       now=datetime.now(TAIPEI), discord_only=args.discord_only)
                    receipt.update(status="reloading", restart_requested=True, plan=plan,
                                   ledger_inputs=inputs, expected_code_sha256=expected_code,
                                   expected_entrypoint_sha256=entrypoint["sha256"])
                    atomic_write_json(args.output, receipt)
                    for service in plan["services"]:
                        subprocess.run(["systemctl", "restart", service], check=True, timeout=65)
                deadline = time.monotonic() + args.timeout_seconds
                while True:
                    try:
                        sync, bot, ack = read_service_ack_pair(SYNC_PATH, BOT_STATUS, timeout_seconds=1.)
                        accepted = native_ready(bot, sync, plan, now=datetime.now(TAIPEI),
                                                expected_code=expected_code, expected_entrypoint=entrypoint["sha256"]) and ack["matched"]
                    except (OSError, ValueError):
                        accepted = False
                    if accepted:
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError("reload occurred, but native Gateway/warmup/code/ACK acceptance is incomplete")
                    time.sleep(.5)
                after = {name: owner(name) for name in protected}
                if (before != after or implementation_identity() != code
                        or file_identity(REPO_ROOT / "services/discord_bot/bot.py") != entrypoint):
                    raise RuntimeError("protected owner or qualified code changed during reload")
                selected_after = {name: owner(name) for name in plan["services"]}
                if any(value.get("ActiveState") != "active" or value.get("MainPID") in {None, "0"}
                       or not value.get("InvocationID")
                       or value["InvocationID"] == selected_before[name].get("InvocationID")
                       for name, value in selected_after.items()):
                    raise RuntimeError("restarted service owner is not a new active invocation")
                prefixes = verify_prefix(STATE_PATH.parent, {"inputs": inputs})
                receipt.update(status="accepted", protected_after=after, ledger_prefixes=prefixes,
                               selected_before=selected_before, selected_after=selected_after,
                               engine_ack={"state_revision": sync["state_revision"], **ack},
                               new_discord_run_id=bot["run_id"], native_warmup=bot["startup_inference_warmup"],
                               background_maintenance=bot.get("background_maintenance"),
                               engine_restarted=not args.discord_only)
    except Exception as exc:
        receipt.update(status="failed_after_restart" if receipt["restart_requested"] else "blocked",
                       error_type=type(exc).__name__, error=str(exc))
    receipt["finished_at"] = datetime.now(TAIPEI).isoformat()
    atomic_write_json(args.output, receipt)
    print(json.dumps({key: value for key, value in receipt.items() if key not in {
        "native_warmup", "ledger_inputs", "ledger_prefixes", "protected_before", "protected_after"}},
        ensure_ascii=False), flush=True)
    return 0 if receipt["status"] in {"planned", "accepted"} else 75


if __name__ == "__main__":
    raise SystemExit(main())
