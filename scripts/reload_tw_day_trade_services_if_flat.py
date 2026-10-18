#!/usr/bin/env python3
"""Plan a qualified paper-service reload; use --apply to actually restart.

Either owner may reload independently. An engine reload still requires
off-session flat inventory and no pending paper orders; it never requires a
healthy unchanged Discord owner to reacquire evicted off-hours GPU models.
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


def fresh_native_pair(bot, sync, now):
    """A closed idle engine heartbeats without fabricating a ledger commit."""
    fresh(bot, "updated_at", now)
    heartbeat_field = "heartbeat_at" if "heartbeat_at" in sync else "published_at"
    fresh(sync, heartbeat_field, now)
    commit = datetime.fromisoformat(str(sync.get("published_at", "")).replace("Z", "+00:00"))
    ack = datetime.fromisoformat(str(bot.get("engine_published_at", "")).replace("Z", "+00:00"))
    heartbeat = datetime.fromisoformat(str(sync[heartbeat_field]).replace("Z", "+00:00"))
    if commit.tzinfo is None or ack.tzinfo is None or not commit == ack <= heartbeat:
        raise ValueError("heartbeat must follow the exact acknowledged ledger commit")


def code_drift_warmup(bot, enabled):
    """Only a complete same-run code-admission rejection is recoverable here."""
    warm = bot.get("startup_inference_warmup") or {}
    rows = warm.get("markets")
    return bool(
        bool(enabled) and len(enabled) == len(set(enabled))
        and warm.get("status") == "degraded" and warm.get("run_id") == bot.get("run_id")
        and warm.get("model_cache_resident_ready") is False
        and type(warm.get("ready_count")) is int and warm["ready_count"] == 0
        and type(warm.get("market_count")) is int and warm["market_count"] == len(enabled)
        and isinstance(rows, list) and len(rows) == len(enabled)
        and all(isinstance(row, dict) and row.get("status") == "failed"
                and row.get("error_type") == "LoadedImplementationChanged"
                and row.get("retryable") is True
                and isinstance(row.get("market"), str) for row in rows)
        and {row.get("market") for row in rows} == set(enabled))


def reload_plan(state, bot, sync, *, now, discord_only=False, engine_only=False,
                code_admission_only=False):
    """Fail closed for unknown/malformed state; do not equate it to flat/idle."""
    if discord_only and engine_only:
        raise ValueError("select only one independent owner")
    if code_admission_only and not discord_only:
        raise ValueError("code-only admission requires the independent Discord owner")
    fresh_native_pair(bot, sync, now)
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
    if ((bot.get("startup_inference_warmup") or {}).get("status") not in idle_warmup
            and not (code_admission_only and code_drift_warmup(bot, enabled))):
        raise ValueError("startup inference warmup is not idle/ready")
    clock = now.astimezone(TAIPEI).time().replace(tzinfo=None)
    if wall_time(8, 59, 30) <= clock < wall_time(9, 1):
        raise ValueError("opening protection window forbids deliberate reload")
    if not discord_only and wall_time(8, 30) <= clock < wall_time(13, 40):
        raise ValueError("engine reload requires off-session flat state")
    identities, nonflat, pending, terminal = {}, 0, 0, 0
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
            for order in orders.values():
                # The canonical engine retains filled entries for recovery.
                # Do not delete them or confuse container length with work.
                # Only explicit, fully sized zero-residual fills are exempt;
                # malformed, cancelled/unknown, unsized or nonzero rows block.
                if (isinstance(order, dict) and order.get("status") == "filled"
                        and type(order.get("remaining_shares")) is int
                        and order["remaining_shares"] == 0
                        and order.get("sizing_pending") is False):
                    terminal += 1
                else:
                    pending += 1
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
    selected = SERVICES[:1] if discord_only else SERVICES[1:] if engine_only else SERVICES
    return {"services": list(selected),
            **({"code_admission_only": True} if code_admission_only else {}),
            "discord_only": discord_only, "engine_only": engine_only,
            "nonflat_count": nonflat, "pending_orders": pending,
            "retained_terminal_orders": terminal,
            "signal_ids": identities, "engine_run_id": sync["engine_run_id"],
            "engine_state_revision": sync["state_revision"],
            "previous_discord_run_id": bot["run_id"]}


def owner(service):
    result = subprocess.run(["systemctl", "show", service, "-p", "ActiveState",
                             "-p", "MainPID", "-p", "InvocationID"],
                            check=True, capture_output=True, text=True, timeout=10)
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def admitted_bot_code(bot, expected_code, expected_entrypoint):
    admission = bot.get("live_implementation_admission")
    return (isinstance(admission, dict)
            and admission.get("code_sha256") == expected_code
            and admission.get("entrypoint_sha256") == expected_entrypoint)


def native_ready(bot, sync, plan, *, now, expected_code, expected_entrypoint):
    fresh_native_pair(bot, sync, now)
    warmup = bot.get("startup_inference_warmup") or {}
    warm_markets = warmup.get("markets") or []
    warm_names = [row.get("market") for row in warm_markets
                  if isinstance(row, dict) and row.get("status") == "ready"]
    engine_only = plan.get("engine_only", False)
    # Engine-only reload preserves the exact bot process. Historical warmup is
    # not GPU residency: the idle postclose owner may correctly have evicted it.
    # Never use this exemption when the bot itself was replaced.
    bot_owner_ready = (
        bot.get("run_id") == plan["previous_discord_run_id"] if engine_only else
        bot.get("run_id") != plan["previous_discord_run_id"]
        and warmup.get("status") == "ready" and warmup.get("run_id") == bot.get("run_id")
        and warmup.get("model_cache_resident_ready") is True
        and warmup.get("ready_count") == warmup.get("market_count") == len(plan["signal_ids"])
        and len(warm_names) == len(set(warm_names)) == len(plan["signal_ids"])
        and set(warm_names) == set(plan["signal_ids"])
    )
    if plan.get("code_admission_only"):
        # Explicitly separate loaded-code/ACK recovery from GPU readiness.
        # The native preparation clock still owns pending/running warmup.
        bot_owner_ready = (
            plan["discord_only"] and not engine_only
            and bot.get("run_id") != plan["previous_discord_run_id"]
            and warmup.get("run_id") == bot.get("run_id")
            and warmup.get("status") in {"pending", "running", "ready", "disabled"})
    return bool(
        isinstance(bot.get("run_id"), str) and bool(bot["run_id"])
        and bot_owner_ready
        and bot.get("discord_connected") is True and bot.get("core_health") == "ready"
        and all(value.get("production_order_possible") is False for value in (bot, sync))
        and sync.get("ledger_integrity_ready") is True
        and bot.get("engine_run_id") == sync.get("engine_run_id")
        and (not plan["discord_only"] or sync.get("engine_run_id") == plan["engine_run_id"])
        and (plan["discord_only"] or sync.get("engine_run_id") != plan["engine_run_id"])
        and type(bot.get("engine_state_revision")) is int
        and bot["engine_state_revision"] == sync.get("state_revision")
        and (not plan["discord_only"] or bot["engine_state_revision"] >= plan["engine_state_revision"])
        and admitted_bot_code(bot, expected_code, expected_entrypoint)
        and all((bot.get("mode_signal_ids") or {}).get(market) == signal
                and ((sync.get("modes") or {}).get(market) or {}).get("signal_id") == signal
                for market, signal in plan["signal_ids"].items())
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--discord-only", action="store_true")
    selection.add_argument("--engine-only", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Otherwise inspect only; never restart")
    parser.add_argument("--code-admission-only", action="store_true",
                        help="Discord only: accept fresh loaded code/ACK, NOT GPU warmup readiness")
    parser.add_argument("--output", type=Path, default=RECEIPT_PATH)
    parser.add_argument("--timeout-seconds", type=float, default=240)
    args = parser.parse_args()
    if args.code_admission_only and not args.discord_only:
        parser.error("--code-admission-only requires --discord-only")
    if not 1 <= args.timeout_seconds <= 600:
        parser.error("timeout-seconds must be between 1 and 600")
    receipt = {"observed_at": datetime.now(TAIPEI).isoformat(), "status": "blocked",
               "broker_orders": 0, "operator_ledger_writes": 0, "restart_requested": False}
    try:
        with control_lease(owner="qualified-paper-service-reload", timeout_seconds=0):
            sync, bot, ack = read_service_ack_pair(SYNC_PATH, BOT_STATUS, timeout_seconds=5.)
            if ack.get("matched") is not True:
                raise ValueError("native owner pair is not coherently acknowledged")
            plan = reload_plan(read_object(STATE_PATH), bot, sync,
                               now=datetime.now(TAIPEI), discord_only=args.discord_only,
                               engine_only=args.engine_only, code_admission_only=args.code_admission_only)
            protected = tuple(name for name in (SERVICES[0], *PROTECTED_SERVICES)
                              if name not in plan["services"])
            before = {name: owner(name) for name in protected}
            if any(owner(name).get("ActiveState") != "active" for name in plan["services"]):
                raise ValueError("requested owner is not active; refuse to enable a stopped role")
            receipt.update(plan=plan, protected_before=before, status="planned")
            if args.apply:
                selected_before = {name: owner(name) for name in plan["services"]}
                from scripts.audit_tw_day_trade_restart import inventory, prefix_sha256, verify_prefix
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
                engine_code = [file_identity(REPO_ROOT / name) for name in (
                    "scripts/run_tw_day_trade_simulation.py", "stockagent/live/quote_provider.py",
                    "stockagent/live/tw_day_trade_simulation.py")]
                expected_code = identity_digest({"code": code})
                cfg = load_market_config(REPO_ROOT / "services/discord_bot/markets"
                                         / (next(iter(plan["signal_ids"])) + ".yaml"))
                config = load_config(cfg.config_path)
                # A producer mid-commit is retryable, not a reason to kill it
                # or to warm from an unaccepted mixed reference generation.
                with live_tw_public_source_read_guard(config.data.parquet_root):
                    current_sync, current_bot, ack = read_service_ack_pair(
                        SYNC_PATH, BOT_STATUS, timeout_seconds=5.)
                    if ack.get("matched") is not True:
                        raise ValueError("native owner pair changed before reload")
                    plan = reload_plan(read_object(STATE_PATH), current_bot, current_sync,
                                       now=datetime.now(TAIPEI), discord_only=args.discord_only,
                                       engine_only=args.engine_only, code_admission_only=args.code_admission_only)
                    if (code_drift_warmup(current_bot, plan["signal_ids"])
                            and admitted_bot_code(current_bot, expected_code, entrypoint["sha256"])):
                        raise ValueError("reported code drift does not match the current admitted implementation")
                    # An unchanged bot cannot adopt new code by restarting
                    # only the engine. Reject before disrupting either owner,
                    # rather than waiting for an impossible post-reload ACK.
                    if args.engine_only and not admitted_bot_code(
                            current_bot, expected_code, entrypoint["sha256"]):
                        raise ValueError("unchanged Discord code admission differs; engine-only reload cannot adopt it")
                    before_inventory = inventory(read_object(STATE_PATH))
                    if (implementation_identity() != code
                            or file_identity(REPO_ROOT / "services/discord_bot/bot.py") != entrypoint
                            or any(file_identity(row["path"]) != row for row in engine_code)
                            or before != {name: owner(name) for name in protected}
                            or selected_before != {name: owner(name) for name in plan["services"]}):
                        raise RuntimeError("qualified code or owner changed before reload")
                    receipt.update(status="reloading", restart_requested=True, plan=plan,
                                   ledger_inputs=inputs, expected_code_sha256=expected_code,
                                   expected_entrypoint_sha256=entrypoint["sha256"],
                                   engine_code=engine_code)
                    atomic_write_json(args.output, receipt)
                    for service in plan["services"]:
                        subprocess.run(["systemctl", "restart", service], check=True, timeout=65)
                deadline = time.monotonic() + args.timeout_seconds
                while True:
                    # Waiting for GPU warmup cannot repair a different code
                    # generation. Report the race immediately, retain this
                    # invocation and never enter an automatic restart loop.
                    if (implementation_identity() != code
                            or file_identity(REPO_ROOT / "services/discord_bot/bot.py") != entrypoint
                            or any(file_identity(row["path"]) != row for row in engine_code)):
                        raise RuntimeError("qualified code changed during startup; wait for a stable generation before retrying reload")
                    try:
                        sync, bot, ack = read_service_ack_pair(SYNC_PATH, BOT_STATUS, timeout_seconds=1.)
                        accepted = native_ready(bot, sync, plan, now=datetime.now(TAIPEI),
                                                expected_code=expected_code, expected_entrypoint=entrypoint["sha256"]) and ack["matched"]
                        if accepted and args.code_admission_only:
                            from scripts.verify_discord_gpu_idle_runtime import command_sync_since_restart
                            journal = command_sync_since_restart()
                            accepted = (journal["commands_sync_or_deferred_observed"]
                                        and not journal["watchdog_or_fatal_observed"])
                            receipt["native_journal"] = journal
                    except (OSError, ValueError):
                        accepted = False
                    if accepted:
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError("reload occurred, but native Gateway/warmup/code/ACK acceptance is incomplete")
                    time.sleep(.5)
                after = {name: owner(name) for name in protected}
                if (before != after or implementation_identity() != code
                        or file_identity(REPO_ROOT / "services/discord_bot/bot.py") != entrypoint
                        or any(file_identity(row["path"]) != row for row in engine_code)):
                    raise RuntimeError("protected owner or qualified code changed during reload")
                selected_after = {name: owner(name) for name in plan["services"]}
                if any(value.get("ActiveState") != "active" or value.get("MainPID") in {None, "0"}
                       or not value.get("InvocationID")
                       or value["InvocationID"] == selected_before[name].get("InvocationID")
                       for name, value in selected_after.items()):
                    raise RuntimeError("restarted service owner is not a new active invocation")
                prefixes = verify_prefix(STATE_PATH.parent, {"inputs": inputs})
                # Prefix verification may legitimately outlive heartbeat TTL.
                # Re-observe the original coherent pair; never re-date the old
                # snapshot or restart again just because readback was slow.
                sync, bot, ack = read_service_ack_pair(SYNC_PATH, BOT_STATUS, timeout_seconds=5.)
                if (ack.get("matched") is not True or not native_ready(
                        bot, sync, plan, now=datetime.now(TAIPEI), expected_code=expected_code,
                        expected_entrypoint=entrypoint["sha256"])
                        or selected_after != {name: owner(name) for name in plan["services"]}
                        or before != {name: owner(name) for name in protected}
                        or implementation_identity() != code
                        or file_identity(REPO_ROOT / "services/discord_bot/bot.py") != entrypoint
                        or any(file_identity(row["path"]) != row for row in engine_code)):
                    raise RuntimeError("native readiness, code or owner changed during final readback")
                if not args.discord_only and inventory(read_object(STATE_PATH)) != before_inventory:
                    raise RuntimeError("engine reload changed persisted inventory, signals or accounting")
                receipt.update(status="accepted_code_admission_only" if args.code_admission_only else "accepted",
                               acceptance_scope="loaded_code_gateway_ack_not_gpu_readiness"
                               if args.code_admission_only else "native_warmup_code_gateway_ack",
                               # Evidence describes the requested check, not a
                               # second liveness check on snapshots retained
                               # across potentially slow prefix verification.
                               gpu_warmup_verified=not (args.code_admission_only or args.engine_only),
                               protected_after=after, ledger_prefixes=prefixes,
                               selected_before=selected_before, selected_after=selected_after,
                               native_readback_at=datetime.now(TAIPEI).isoformat(),
                               engine_heartbeat_at=sync.get("heartbeat_at", sync.get("published_at")),
                               engine_ack={"state_revision": sync["state_revision"], **ack},
                               new_discord_run_id=bot["run_id"], native_warmup=bot["startup_inference_warmup"],
                               background_maintenance=bot.get("background_maintenance"),
                               inventory_restored_exactly=not args.discord_only,
                               engine_restarted=not args.discord_only)
    except Exception as exc:
        receipt.update(status="failed_after_restart" if receipt["restart_requested"] else "blocked",
                       error_type=type(exc).__name__, error=str(exc))
    receipt["finished_at"] = datetime.now(TAIPEI).isoformat()
    atomic_write_json(args.output, receipt)
    print(json.dumps({key: value for key, value in receipt.items() if key not in {
        "native_warmup", "ledger_inputs", "ledger_prefixes", "protected_before", "protected_after"}},
        ensure_ascii=False), flush=True)
    return 0 if receipt["status"] in {"planned", "accepted", "accepted_code_admission_only"} else 75


if __name__ == "__main__":
    raise SystemExit(main())
