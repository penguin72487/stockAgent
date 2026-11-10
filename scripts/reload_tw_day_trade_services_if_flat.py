#!/usr/bin/env python3
"""Plan a qualified paper-service reload; use --apply to actually restart.

Either owner may reload independently. An engine reload still requires
off-session flat inventory and no pending paper orders; it never requires a
healthy unchanged Discord owner to reacquire evicted off-hours GPU models.
"""

from __future__ import annotations

import argparse
from contextlib import nullcontext
from datetime import date, datetime, time as wall_time
import json
import math
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

READ_MODEL_MEMBERS = frozenset({"scripts/serve_tw_day_trade_dashboard.py",
                                "stockagent/live/tw_day_trade_dashboard.py"})
FINANCIAL_JOURNALS = ("events.jsonl", "orders.jsonl", "fills.jsonl")


def engine_read_model_scope(previous: Path | None, candidate: Path, *, repo_root=REPO_ROOT) -> dict:
    """Only a verified UI-only delta can use the narrower restart proof.

    This never qualifies a model/executor/config/quote change or an unknown
    original pin. Such reloads retain the existing full-journal byte proof.
    """
    result = {"contract": "verified_ui_only_restart_scope_v1", "eligible": False}
    if previous is None or not Path(previous).is_file() or not Path(candidate).is_file():
        return result
    from stockagent.live.service_code_release import verify_frozen_source
    verify_frozen_source(previous, repo_root)
    verify_frozen_source(candidate, repo_root)
    before = json.loads(Path(previous).read_bytes()).get("code", {}).get("files", {})
    after = json.loads(Path(candidate).read_bytes()).get("code", {}).get("files", {})
    changed = {name for name in before.keys() | after.keys() if before.get(name) != after.get(name)}
    result["changed_members"] = sorted(changed)
    if before and before.keys() == after.keys() and changed and changed <= READ_MODEL_MEMBERS:
        result.update(eligible=True, previous_release=str(previous), candidate_release=str(candidate),
                      all_signal_history_bytes_verified=False,
                      financial_journals=list(FINANCIAL_JOURNALS),
                      remaining_journals_proof="exact_file_and_immutable_head_generations_not_full_content_hash")
    return result


def journal_generation_fence(root: Path, names) -> dict:
    """Capture exact local file identities AND the canonical immutable heads."""
    from stockagent.live.paper_journal import layout_generations
    files = {}
    for name in names:
        info = (root / name).stat()
        files[name] = [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]
    return {"files": files, "immutable_layout": layout_generations(root, names=names)}


def verify_journal_generation_fence(root: Path, expected: dict) -> None:
    if journal_generation_fence(root, tuple(expected["files"])) != expected:
        raise RuntimeError("UI-only reload changed a protected journal generation")


def read_object(path):
    from stockagent.live.paper_mode_snapshot import read_paper_state
    value = read_paper_state(path)
    if not isinstance(value, dict):
        raise ValueError(f"object receipt required: {path.name}")
    return value


def verify_engine_release_mode_specs(release, *, expected_capitals, repo_root=REPO_ROOT):
    """Load the real enabled configurations in the *candidate* code namespace.

    Source-manifest verification alone cannot show that a runner and its config
    schema are compatible. This isolated read-only probe starts no services and
    opens no paper ledger. A failed import, missing mode, configuration error or
    changed capital blocks the native restart before its drop-in is changed.
    """
    from stockagent.live.service_code_release import run_in_namespace

    if (not isinstance(expected_capitals, dict) or not expected_capitals
            or any(not isinstance(market, str) or not market
                   or isinstance(capital, bool) or not isinstance(capital, (int, float))
                   or not math.isfinite(capital) or capital <= 0
                   for market, capital in expected_capitals.items())):
        raise ValueError("candidate engine probe requires exact active market capitals")
    code = '''\
from pathlib import Path
import inspect, json, sys
from scripts.run_tw_day_trade_simulation import _mode_specs
kwargs = ({"promoted_research_state_dir": Path(sys.argv[2])}
          if "promoted_research_state_dir" in inspect.signature(_mode_specs).parameters else {})
specs, _, errors = _mode_specs(Path(sys.argv[1]), **kwargs)
rows = [{"market": spec.market, "capital": spec.initial_capital_twd} for spec in specs]
print("STOCKAGENT_ENGINE_MODE_PROBE=" + json.dumps({"modes": rows, "errors": errors}, allow_nan=False))
'''
    result = run_in_namespace(Path(release), Path(repo_root),
        ["-c", code, str(Path(repo_root) / "services/discord_bot/markets"), str(STATE_PATH.parent)], timeout=90)
    prefix = "STOCKAGENT_ENGINE_MODE_PROBE="
    lines = [line[len(prefix):] for line in result.stdout.splitlines() if line.startswith(prefix)]
    if result.returncode != 0 or len(lines) != 1:
        raise ValueError("candidate engine mode probe failed before restart: "
                         + (result.stderr or result.stdout or f"exit {result.returncode}")[-1200:])
    try:
        payload = json.loads(lines[0])
        rows, errors = payload["modes"], payload["errors"]
        if not isinstance(rows, list) or not isinstance(errors, dict):
            raise ValueError("invalid probe schema")
        actual = {}
        for row in rows:
            market, capital = row["market"], row["capital"]
            if (not isinstance(market, str) or not market or market in actual
                    or isinstance(capital, bool) or not isinstance(capital, (int, float))
                    or not math.isfinite(capital) or capital <= 0):
                raise ValueError("invalid candidate market or capital")
            actual[market] = float(capital)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("invalid candidate engine mode probe result") from exc
    expected = {market: float(capital) for market, capital in expected_capitals.items()}
    if errors or actual != expected:
        raise ValueError(f"candidate engine cannot load the exact active modes and capitals: "
                         f"errors={errors}, actual={actual}, expected={expected}")
    return {"contract": "candidate_engine_real_mode_spec_probe_v1", "status": "passed",
            "markets": actual, "services_started": 0, "ledger_opened": False,
            "native_readiness_claimed": False}


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


def read_fresh_ack_pair(*, timeout_seconds=5.):
    """Observe freshness as well as exact ACK, without relaxing either gate.

    The idle owner throttles heartbeats. A read on its boundary may be stale
    by milliseconds; wait for a real new heartbeat, never enlarge its TTL or
    treat the old commit time as the current liveness clock.
    """
    deadline = time.perf_counter() + timeout_seconds
    last_error = "native owner pair is not coherently acknowledged"
    while True:
        sync, bot, ack = read_service_ack_pair(SYNC_PATH, BOT_STATUS,
            timeout_seconds=min(timeout_seconds, max(.05, deadline - time.perf_counter())))
        if ack.get("matched") is not True:
            # The canonical observer already exhausted its ACK budget.
            return sync, bot, ack
        if ack.get("matched") is True:
            try:
                fresh_native_pair(bot, sync, datetime.now(TAIPEI))
            except ValueError as exc:
                last_error = str(exc)
            else:
                return sync, bot, ack
        remaining = deadline - time.perf_counter()
        if remaining <= 0:
            raise ValueError(last_error)
        time.sleep(min(.05, remaining))


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


def terminal_entry_price_rejection(order, mode, *, now):
    """A proven entry cancellation is terminal, not an active paper order."""
    if not isinstance(order, dict) or order.get("status") != "cancelled_entry_price_guard":
        return False
    from stockagent.live.tw_day_trade_simulation import (
        ENTRY_PRICE_GUARD_CONTRACT, ENTRY_PRICE_GUARD_REASONS, entry_price_guard_reason,
    )
    if (order.get("sizing_pending") is not False
            or type(order.get("remaining_shares")) is not int
            or order["remaining_shares"] < 0
            or order.get("entry_price_guard_contract") != ENTRY_PRICE_GUARD_CONTRACT):
        return False
    template = order.get("position")
    if (not isinstance(template, dict) or not template.get("signal_id")
            or template["signal_id"] != mode.get("signal_id")
            or not template.get("session_date")
            or template["session_date"] != mode.get("session_date")):
        return False
    reason = entry_price_guard_reason(
        side=template.get("side"), price=order.get("rejected_entry_price"),
        take_profit_price=template.get("take_profit_price"),
        stop_trigger_price=template.get("stop_trigger_price"),
    )
    if reason not in ENTRY_PRICE_GUARD_REASONS or reason != order.get("last_wait_reason"):
        return False
    try:
        cancelled = datetime.fromisoformat(str(order.get("cancelled_at")))
        quote_at = datetime.fromisoformat(str(order.get("last_quote_at")))
        return (cancelled.tzinfo is not None and quote_at.tzinfo is not None
                and quote_at <= cancelled <= now
                and cancelled.astimezone(TAIPEI).date().isoformat() == template["session_date"]
                and quote_at.astimezone(TAIPEI).date().isoformat() == template["session_date"])
    except (TypeError, ValueError):
        return False


def terminal_exit_priority_cancellation(order, mode, *, market, symbol, now):
    """Recognize the engine's settled, cancelled entry without erasing it.

    This status retains its unfilled quantity as audit evidence.  It is not an
    active order once the same committed session is flat and the engine has
    recorded its closing settlement. A newer post-close cache can advance
    signal_id; a retained order must still name an explicitly consumed signal.
    Unknown cancellations still block.
    """
    if not isinstance(order, dict) or order.get("status") != "cancelled_exit_priority":
        return False
    template = order.get("position")
    remaining = order.get("remaining_shares")
    consumed = mode.get("processed_signal_ids")
    known_consumed = (isinstance(consumed, list) and all(isinstance(value, str) for value in consumed)
                      and isinstance(template, dict) and bool(template.get("signal_id"))
                      and template["signal_id"] in consumed)
    # Readiness changes a fully settled prior session to waiting_09_00_signal
    # at the next day's midnight. The status change does not reactivate its
    # cancelled entries. Admit that later-day state only with the independent
    # zero-risk/evidence fields; a same-day waiting state remains ambiguous.
    try:
        previous_session = date.fromisoformat(str(mode.get("session_date"))) < now.astimezone(TAIPEI).date()
    except (TypeError, ValueError):
        previous_session = False
    overnight_flat = (mode.get("engine_status") == "waiting_09_00_signal" and previous_session and known_consumed
                      and mode.get("execution_evidence_complete") is True
                      and type(mode.get("pending_reduction_shares")) is int
                      and mode["pending_reduction_shares"] == 0
                      and type(mode.get("closing_auction_pending_count")) is int
                      and mode["closing_auction_pending_count"] == 0
                      and type(mode.get("unresolved_exit_count")) is int
                      and mode["unresolved_exit_count"] == 0
                      and isinstance(mode.get("positions"), dict)
                      and all(isinstance(position, dict)
                              and type(position.get("signed_shares")) is int
                              and position["signed_shares"] == 0
                              for position in mode["positions"].values()))
    if (not isinstance(template, dict) or type(remaining) is not int or remaining < 0
            or type(order.get("sizing_pending")) is not bool
            or type(mode.get("pending_entry_shares")) is not int
            or mode["pending_entry_shares"] != 0
            or (mode.get("engine_status") != "session_flat_after_exit" and not overnight_flat)
            or template.get("market") != market or template.get("symbol") != symbol
            or not mode.get("signal_id")
            or (template.get("signal_id") != mode["signal_id"] and not known_consumed)
            or not mode.get("session_date") or template.get("session_date") != mode["session_date"]):
        return False
    try:
        settled = datetime.fromisoformat(str(mode.get("closing_auction_settled_at")))
        local_settled = settled.astimezone(TAIPEI)
        return (settled.tzinfo is not None and settled <= now
                and local_settled.date().isoformat() == mode["session_date"]
                and local_settled.time().replace(tzinfo=None) >= wall_time(13, 30))
    except (TypeError, ValueError, OverflowError):
        return False


def reload_plan(state, bot, sync, *, now, discord_only=False, engine_only=False,
                code_admission_only=False, events_path: Path | None = None):
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
    warmup = bot.get("startup_inference_warmup") or {}
    # An engine-only cutover preserves this exact idle Discord owner. Its
    # scheduled cold preparation is not engine work or GPU readiness. Running,
    # unknown and different-run warmup still block; joint reload keeps its gate.
    unchanged_cold_bot = bool(engine_only and warmup.get("status") == "pending"
                              and warmup.get("run_id") == bot["run_id"])
    if (warmup.get("status") not in idle_warmup and not unchanged_cold_bot
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
        if (mode.get("entry_retry_pending_commit") or mode.get("research_terminal_pending_commit") is not None
                or mode.get("ledger_state_divergence")):
            raise ValueError(f"uncommitted or divergent ledger: {market}")
        for position in mode["positions"].values():
            if not isinstance(position, dict) or type(position.get("signed_shares")) is not int:
                raise ValueError(f"unknown position quantity: {market}")
            nonflat += bool(position["signed_shares"])
        # A prior completed session can retain cancelled quantities and
        # unchanged targets for audit. Reuse the stricter replay-owner proof:
        # flat inventory, complete settlement, zero pending counters, exact
        # consumed signal identity and matching immutable cancellation events.
        # A status label alone is never sufficient.
        terminal_prior = False
        if events_path is not None:
            from scripts.repair_tw_day_trade_flat_opening import _previous_terminal_orders_proven
            terminal_prior = _previous_terminal_orders_proven(
                mode, now.astimezone(TAIPEI).date(), events_path,
                completed_same_day_at=now, terminal_entry_validator=terminal_entry_price_rejection,
            )
        for field in ("pending_entry_orders", "pending_reduction_orders"):
            orders = mode.get(field, {})
            if not isinstance(orders, dict):
                raise ValueError(f"malformed pending orders: {market}")
            if terminal_prior:
                terminal += len(orders)
                continue
            for symbol, order in orders.items():
                if events_path is not None:
                    # When the canonical journal is available, a label or a
                    # single-row shortcut must not override a failed complete
                    # settlement/cancellation proof for this mode.
                    pending += 1
                    continue
                # The canonical engine retains filled entries for recovery.
                # Do not delete them or confuse container length with work.
                # Explicit completed fills and proven TP/SL rejections are
                # terminal. Unknown cancellations/unsized orders still block.
                if (isinstance(order, dict) and order.get("status") == "filled"
                        and type(order.get("remaining_shares")) is int
                        and order["remaining_shares"] == 0
                        and order.get("sizing_pending") is False):
                    terminal += 1
                elif field == "pending_entry_orders" and terminal_entry_price_rejection(order, mode, now=now):
                    terminal += 1
                elif (field == "pending_entry_orders"
                      and terminal_exit_priority_cancellation(order, mode, market=market,
                                                              symbol=symbol, now=now)):
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
    parser.add_argument("--source-release", type=Path,
                        help="Discord code-only admission: pin this verified read-only source namespace")
    parser.add_argument("--engine-source-release", type=Path,
                        help="Engine-only off-session flat reload: pin this verified source; never changes the Discord pin")
    parser.add_argument("--output", type=Path, default=RECEIPT_PATH)
    parser.add_argument("--timeout-seconds", type=float, default=240)
    parser.add_argument("--control-wait-seconds", type=float, default=0,
                        help="Bounded wait for the existing maintenance OS lease; never force unlock")
    args = parser.parse_args()
    if args.code_admission_only and not args.discord_only:
        parser.error("--code-admission-only requires --discord-only")
    if args.source_release and not (args.discord_only and args.code_admission_only):
        parser.error("--source-release requires --discord-only --code-admission-only")
    if args.engine_source_release and not args.engine_only:
        parser.error("--engine-source-release requires --engine-only")
    if not 1 <= args.timeout_seconds <= 600:
        parser.error("timeout-seconds must be between 1 and 600")
    if not 0 <= args.control_wait_seconds <= 120:
        parser.error("control-wait-seconds must be between 0 and 120")
    receipt = {"observed_at": datetime.now(TAIPEI).isoformat(), "status": "blocked",
               "broker_orders": 0, "operator_ledger_writes": 0, "restart_requested": False}
    control_started = time.perf_counter()
    try:
        with control_lease(owner="qualified-paper-service-reload", timeout_seconds=args.control_wait_seconds):
            receipt["control_wait_ms"] = (time.perf_counter() - control_started) * 1000
            state = read_object(STATE_PATH)
            sync, bot, ack = read_fresh_ack_pair()
            if ack.get("matched") is not True:
                raise ValueError("native owner pair is not coherently acknowledged")
            plan = reload_plan(state, bot, sync,
                               now=datetime.now(TAIPEI), discord_only=args.discord_only,
                               engine_only=args.engine_only, code_admission_only=args.code_admission_only,
                               events_path=STATE_PATH.parent / "events.jsonl")
            protected = tuple(name for name in (SERVICES[0], *PROTECTED_SERVICES)
                              if name not in plan["services"])
            before = {name: owner(name) for name in protected}
            if any(owner(name).get("ActiveState") != "active" for name in plan["services"]):
                raise ValueError("requested owner is not active; refuse to enable a stopped role")
            receipt.update(plan=plan, protected_before=before, status="planned")
            from stockagent.live.service_code_release import (
                configured_release, verify_frozen_source, namespace_identity,
                frozen_generation, install_drop_in, verify_owner_namespace, DROP_IN,
                ENGINE_DROP_IN, native_owner_pid,
            )
            # An engine-only reload also compares the unchanged Discord owner
            # against its pinned source, never against development workspace.
            release_path = args.source_release or configured_release(REPO_ROOT)
            if release_path is not None:
                release_path = release_path.absolute()
                receipt["source_release"] = verify_frozen_source(release_path, REPO_ROOT)
            engine_release = (None if args.discord_only else
                              args.engine_source_release or configured_release(REPO_ROOT, role="engine"))
            if engine_release is not None:
                engine_release = engine_release.absolute()
                receipt["engine_source_release"] = verify_frozen_source(engine_release, REPO_ROOT)
                if not args.engine_only:
                    raise ValueError("pinned engine reload requires the independent --engine-only workflow")
            if args.engine_source_release is not None:
                expected_capitals = {market: (state.get("modes") or {}).get(market, {}).get("initial_capital_twd")
                                     for market in plan["signal_ids"]}
                receipt["candidate_engine_mode_probe"] = verify_engine_release_mode_specs(
                    engine_release, expected_capitals=expected_capitals)
            read_model = (engine_read_model_scope(configured_release(REPO_ROOT, role="engine"), engine_release)
                          if args.engine_only and args.engine_source_release is not None else {"eligible": False})
            receipt["engine_read_model_scope"] = read_model
            if args.apply:
                selected_before = {name: owner(name) for name in plan["services"]}
                from scripts.audit_tw_day_trade_restart import inventory, prefix_sha256, verify_prefix
                from stockagent.live.completed_inference_cache import implementation_identity, identity_digest, file_identity
                from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard
                from stockagent.live.market_config import load_market_config
                from stockagent.config import load_config
                if release_path is not None:
                    namespace = namespace_identity(release_path, REPO_ROOT)
                    code, entrypoint = namespace["code"], namespace["entrypoint"]
                    source_generation = frozen_generation(release_path)
                    def code_unchanged():
                        return frozen_generation(release_path) == source_generation
                else:
                    code = implementation_identity()
                    entrypoint = file_identity(REPO_ROOT / "services/discord_bot/bot.py")
                    def code_unchanged():
                        return (implementation_identity() == code
                                and file_identity(REPO_ROOT / "services/discord_bot/bot.py") == entrypoint)
                if engine_release is not None:
                    engine_namespace = namespace_identity(engine_release, REPO_ROOT, role="engine")
                    engine_code = engine_namespace["engine_code"]
                    engine_generation = frozen_generation(engine_release)
                else:
                    engine_code = [file_identity(REPO_ROOT / name) for name in (
                        "scripts/run_tw_day_trade_simulation.py", "stockagent/live/quote_provider.py",
                        "stockagent/live/tw_day_trade_simulation.py")]
                def engine_code_unchanged():
                    if engine_release is not None:
                        return frozen_generation(engine_release) == engine_generation
                    return all(file_identity(row["path"]) == row for row in engine_code)
                expected_code = identity_digest({"code": code})
                source_admission_required = not (
                    args.discord_only and args.code_admission_only and release_path is not None)
                source_guard = nullcontext()
                if source_admission_required:
                    cfg = load_market_config(REPO_ROOT / "services/discord_bot/markets"
                                             / (next(iter(plan["signal_ids"])) + ".yaml"))
                    config = load_config(cfg.config_path)
                    source_guard = live_tw_public_source_read_guard(config.data.parquet_root)
                receipt["source_read_admission_required"] = source_admission_required
                receipt["data_or_gpu_readiness_claim"] = not args.code_admission_only
                # A producer mid-commit is retryable, not a reason to kill it
                # or to warm from an unaccepted mixed reference generation.
                with source_guard:
                    # Reject an active source writer before reading large
                    # ledger prefixes. Keep the canonical control -> source
                    # lock order and all independent post-reload readbacks.
                    from stockagent.live.paper_journal import FILENAMES
                    if read_model["eligible"]:
                        receipt["previous_engine_namespace"] = verify_owner_namespace(
                            Path(read_model["previous_release"]), REPO_ROOT,
                            native_owner_pid(REPO_ROOT, role="engine"), role="engine")
                    journals = (FINANCIAL_JOURNALS if read_model["eligible"] else FILENAMES
                                if engine_release is not None else FINANCIAL_JOURNALS)
                    protected_journals = tuple(name for name in FILENAMES if name not in journals)
                    generation_fence = (journal_generation_fence(STATE_PATH.parent, protected_journals)
                                        if read_model["eligible"] else None)
                    inputs = {name: {"size_bytes": (STATE_PATH.parent / name).stat().st_size}
                              for name in journals}
                    for name, value in inputs.items():
                        value["sha256"] = prefix_sha256(STATE_PATH.parent / name, value["size_bytes"])
                    if generation_fence is not None:
                        verify_journal_generation_fence(STATE_PATH.parent, generation_fence)
                    current_state = read_object(STATE_PATH)
                    current_sync, current_bot, ack = read_fresh_ack_pair()
                    if ack.get("matched") is not True:
                        raise ValueError("native owner pair changed before reload")
                    plan = reload_plan(current_state, current_bot, current_sync,
                                       now=datetime.now(TAIPEI), discord_only=args.discord_only,
                                       engine_only=args.engine_only, code_admission_only=args.code_admission_only,
                                       events_path=STATE_PATH.parent / "events.jsonl")
                    if args.engine_source_release is not None:
                        current_capitals = {market: (current_state.get("modes") or {}).get(market, {}).get("initial_capital_twd")
                                            for market in plan["signal_ids"]}
                        if verify_engine_release_mode_specs(engine_release, expected_capitals=current_capitals) != receipt["candidate_engine_mode_probe"]:
                            raise RuntimeError("candidate engine mode probe changed before restart")
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
                    if read_model["eligible"] and engine_read_model_scope(
                            configured_release(REPO_ROOT, role="engine"), engine_release) != read_model:
                        raise RuntimeError("UI-only source delta changed before restart")
                    if (not code_unchanged()
                            or not engine_code_unchanged()
                            or before != {name: owner(name) for name in protected}
                            or selected_before != {name: owner(name) for name in plan["services"]}):
                        raise RuntimeError("qualified code or owner changed before reload")
                    receipt.update(status="reloading", plan=plan,
                                   ledger_inputs=inputs, expected_code_sha256=expected_code,
                                   expected_entrypoint_sha256=entrypoint["sha256"],
                                   engine_code=engine_code)
                    atomic_write_json(args.output, receipt)
                    if release_path is not None and not args.engine_only:
                        receipt["previous_code_drop_in"] = DROP_IN.read_text() if DROP_IN.exists() else None
                        atomic_write_json(args.output, receipt)
                        receipt["parsed_unit_code_configuration"] = install_drop_in(release_path, REPO_ROOT)
                        receipt["code_drop_in_installed"] = True
                    if engine_release is not None:
                        receipt["previous_engine_code_drop_in"] = ENGINE_DROP_IN.read_text() if ENGINE_DROP_IN.exists() else None
                        atomic_write_json(args.output, receipt)
                        receipt["parsed_engine_unit_code_configuration"] = install_drop_in(
                            engine_release, REPO_ROOT, role="engine")
                        receipt["engine_code_drop_in_installed"] = True
                    receipt["restart_requested"] = True
                    atomic_write_json(args.output, receipt)
                    for service in plan["services"]:
                        subprocess.run(["systemctl", "restart", service], check=True, timeout=65)
                deadline = time.monotonic() + args.timeout_seconds
                while True:
                    # Waiting for GPU warmup cannot repair a different code
                    # generation. Report the race immediately, retain this
                    # invocation and never enter an automatic restart loop.
                    if (not code_unchanged()
                            or not engine_code_unchanged()):
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
                if (before != after or not code_unchanged()
                        or not engine_code_unchanged()):
                    raise RuntimeError("protected owner or qualified code changed during reload")
                selected_after = {name: owner(name) for name in plan["services"]}
                if any(value.get("ActiveState") != "active" or value.get("MainPID") in {None, "0"}
                       or not value.get("InvocationID")
                       or value["InvocationID"] == selected_before[name].get("InvocationID")
                       for name, value in selected_after.items()):
                    raise RuntimeError("restarted service owner is not a new active invocation")
                prefixes = verify_prefix(STATE_PATH.parent, {"inputs": inputs})
                if generation_fence is not None:
                    verify_journal_generation_fence(STATE_PATH.parent, generation_fence)
                    receipt["protected_journal_generations"] = generation_fence
                # Prefix verification may legitimately outlive heartbeat TTL.
                # Re-observe the original coherent pair; never re-date the old
                # snapshot or restart again just because readback was slow.
                sync, bot, ack = read_fresh_ack_pair()
                if (ack.get("matched") is not True or not native_ready(
                        bot, sync, plan, now=datetime.now(TAIPEI), expected_code=expected_code,
                        expected_entrypoint=entrypoint["sha256"])
                        or selected_after != {name: owner(name) for name in plan["services"]}
                        or before != {name: owner(name) for name in protected}
                        or not code_unchanged()
                        or not engine_code_unchanged()):
                    raise RuntimeError("native readiness, code or owner changed during final readback")
                if not args.discord_only and inventory(read_object(STATE_PATH)) != before_inventory:
                    raise RuntimeError("engine reload changed persisted inventory, signals or accounting")
                if generation_fence is not None:
                    verify_journal_generation_fence(STATE_PATH.parent, generation_fence)
                if release_path is not None:
                    receipt["native_code_namespace"] = verify_owner_namespace(
                        release_path, REPO_ROOT, int(bot["run_id"].split("-", 1)[0]))
                if engine_release is not None:
                    receipt["native_engine_code_namespace"] = verify_owner_namespace(
                        engine_release, REPO_ROOT, native_owner_pid(REPO_ROOT, role="engine"), role="engine")
                receipt.update(status="accepted_code_admission_only" if args.code_admission_only else "accepted",
                               acceptance_scope="loaded_code_gateway_ack_not_gpu_readiness"
                               if args.code_admission_only else "engine_restart_inventory_code_gateway_ack_not_gpu_readiness"
                               if args.engine_only else "native_warmup_code_gateway_ack",
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
        receipt.setdefault("control_wait_ms", (time.perf_counter() - control_started) * 1000)
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
