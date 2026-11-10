"""Add one isolated replay to the canonical paper ledger without replacing modes.

The prior account states and execution/decision byte streams are retained;
minute valuation is separately rebuilt from those unchanged executions. The
promoter uses the ordinary same-filesystem exchange and writer locks.
"""
from __future__ import annotations

from copy import deepcopy
from collections.abc import Mapping
from datetime import date, datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import shutil

from downloader.artifact_io import atomic_write_json
from stockagent.live.paper_journal import (
    journal_write_transaction, managed, open_paper_journal, rebind_cloned_journals,
)

CONTRACT = "additive_independent_paper_mode_v1"
LEDGERS = ("signals.jsonl", "fills.jsonl", "orders.jsonl", "events.jsonl", "latency.jsonl")
ABORTED_ACTIVATION = "aborted_unfunded_paper_addition_activation_v1"
PROVISIONAL_PUBLICATION = "operator_authorized_provisional_historical_component_v1"
CURVE_RECOVERY = "retained_paper_cash_and_scoped_opening_valuation_recovery_v1"


def sha(path: Path, count: int | None = None, *, offset: int = 0) -> str:
    digest = hashlib.sha256()
    with open_paper_journal(path) as stream:
        stream.seek(offset)
        remaining = count
        while remaining is None or remaining > 0:
            block = stream.read(1024**2 if remaining is None else min(1024**2, remaining))
            if not block:
                if remaining:
                    raise ValueError(f"truncated retained ledger: {path}")
                break
            digest.update(block)
            if remaining is not None:
                remaining -= len(block)
    return digest.hexdigest()


def _object(path):
    from stockagent.live.paper_mode_snapshot import read_paper_state
    value = read_paper_state(path)
    if not isinstance(value, dict):
        raise ValueError(f"expected object: {path}")
    return value


def _journal_rows(path: Path, count: int | None = None):
    """Read a complete accepted prefix without including a later mode's tail."""
    with open_paper_journal(path) as stream:
        remaining = count
        while remaining is None or remaining:
            line = stream.readline(-1 if remaining is None else remaining)
            if not line:
                if remaining:
                    raise ValueError("truncated curve-recovery journal prefix")
                break
            if remaining is not None:
                remaining -= len(line)
            if not line.endswith(b"\n"):
                raise ValueError("incomplete curve-recovery journal row")
            if line.strip():
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError("curve recovery requires journal objects")
                yield row


def inspect_retained_curve_recovery(
    root: Path, *, sessions: list[str], markets: set[str],
    marks_path: Path | None = None, prior_state: dict | None = None,
    prefixes: dict | None = None, reconstructed_at: datetime | None = None,
):
    """Read-only proof of cash gaps and narrowly eligible opening projections."""
    from itertools import chain
    from scripts import rebuild_tw_day_trade_minute_curves as curves
    state = deepcopy(prior_state if prior_state is not None else _object(root / "state.json"))
    if set(state.get("modes") or {}) != markets:
        raise ValueError("retained recovery account scope differs")
    path = marks_path or root / "marks.jsonl"
    source_paths = [path, *(root / name for name in ("fills.jsonl", "orders.jsonl", "events.jsonl"))]
    generation = lambda item: (item.stat().st_dev, item.stat().st_ino, item.stat().st_size, item.stat().st_mtime_ns)
    before_generations = [generation(item) for item in source_paths]
    def limit(name):
        if prefixes is None:
            return None
        proof = prefixes.get(name)
        if (not isinstance(proof, dict) or type(proof.get("bytes")) is not int
                or proof["bytes"] < 0):
            raise ValueError("missing exact retained journal prefix")
        return proof["bytes"]
    source = list(_journal_rows(path))
    fills = list(_journal_rows(root / "fills.jsonl", limit("fills.jsonl")))
    positions = curves.load_positions(root, start=date.fromisoformat(sessions[0]),
        end=date.fromisoformat(sessions[-1]), fill_rows=fills)
    at = reconstructed_at or datetime.now(curves.TAIPEI)
    derived, cash = curves.recover_proven_cash_session_marks(source, positions,
        state=state, fill_rows=fills,
        activity_rows=chain(_journal_rows(root / "orders.jsonl", limit("orders.jsonl")),
                            _journal_rows(root / "events.jsonl", limit("events.jsonl"))),
        completed_sessions=sessions, expected_markets=markets, reconstructed_at=at)
    endpoints = curves.validate_carried_endpoint_accounting(derived, positions, state=state,
        fill_rows=fills, start=date.fromisoformat(sessions[0]), end=date.fromisoformat(sessions[-1]),
        collect_failures=True)
    selected = {(row.get("market"), row.get("minute")): row for row in derived}
    grouped = {}
    for failure in endpoints.get("failures", []):
        key = (failure["market"], failure["minute"])
        grouped.setdefault(key, set()).add(failure.get("field", failure.get("error", "unknown")))
    targets = set()
    for (market, minute), fields in grouped.items():
        day = minute[:10]
        if not curves._eligible_replayed_opening_repair(selected.get((market, minute)) or {},
                market=market, key=minute, targets={(market, day)}, mismatched_fields=fields):
            raise ValueError(f"retained endpoint cannot be recovered as a sourced projection: {market}:{minute}")
        targets.add((market, day))
    if targets:
        endpoints = curves.validate_carried_endpoint_accounting(derived, positions, state=state,
            fill_rows=fills, start=date.fromisoformat(sessions[0]), end=date.fromisoformat(sessions[-1]),
            collect_failures=True, repair_opening_targets=targets)
    if endpoints.get("failures"):
        raise ValueError("retained endpoint accounting still differs")
    if prior_state is None and not retained_accounts_match(state["modes"], _object(root / "state.json")["modes"]):
        raise RuntimeError("retained account advanced during recovery planning")
    if before_generations != [generation(item) for item in source_paths]:
        raise RuntimeError("retained journal changed during recovery planning")
    proof = {"contract": CURVE_RECOVERY, "cash_account_gap_recovery": cash,
        "opening_projection_repairs": [f"{market}:{day}" for market, day in sorted(targets)],
        "endpoint_accounting": endpoints, "financial_journals_changed": False,
        "missing_signal_execution_replayed": False}
    return proof, derived[len(source):]


def _stable_regular_sha(path: Path) -> str:
    """Pin exact model bytes without accepting a file changed during hashing."""
    path = Path(path)
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ValueError("research model input must be an absolute regular file")
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024**2), b""):
            digest.update(chunk)
    after = path.stat()
    generation = lambda stat: (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
    if generation(before) != generation(after):
        raise RuntimeError("research model input changed during hashing")
    return digest.hexdigest()


def _pin_research_model(mode: Mapping) -> dict:
    """Bind the added component to its exact research model and cost ABI."""
    from stockagent.live.market_status import short_file_fingerprint

    checkpoint, config = Path(str(mode.get("checkpoint_path") or "")), Path(str(mode.get("config_path") or ""))
    checkpoint_hash, config_hash = _stable_regular_sha(checkpoint), _stable_regular_sha(config)
    contract_path = checkpoint.parent / "mode_artifact_contract.json"
    complete_path = checkpoint.parent / "fold_complete.json"
    contract_hash, complete_hash = _stable_regular_sha(contract_path), _stable_regular_sha(complete_path)
    contract, complete = _object(contract_path), _object(complete_path)
    penalty = (contract.get("mode_details") or {}).get("short_limit_up_close_penalty") or {}
    rate = mode.get("configured_research_short_limit_up_close_penalty_rate")
    fold = complete.get("fold_id")
    if (contract.get("execution_mode") != "tw_day_trade" or not isinstance(penalty, Mapping)
            or penalty.get("contract") != "official_limit_up_close_source_residual_short_expense_v1"
            or penalty.get("assessment") != "after_source_capacity_before_synthetic_terminal_liquidation"
            or penalty.get("base") != "uncovered_short_shares_times_official_close"
            or penalty.get("research_only") is not True or isinstance(rate, bool)
            or not isinstance(rate, (int, float)) or not math.isfinite(rate) or not 0 < rate <= 1
            or penalty.get("rate") != rate
            or mode.get("research_short_limit_up_close_penalty_rate") != rate
            or complete.get("status") != "complete" or complete.get("checkpoint_path") != checkpoint.name
            or type(fold) is not int or fold < 0 or checkpoint.parent.name != f"fold_{fold:02d}"
            or mode.get("checkpoint_fingerprint") != short_file_fingerprint(checkpoint)):
        raise ValueError("research model checkpoint, fold or cost contract differs")
    if (_stable_regular_sha(checkpoint) != checkpoint_hash or _stable_regular_sha(config) != config_hash
            or _stable_regular_sha(contract_path) != contract_hash or _stable_regular_sha(complete_path) != complete_hash):
        raise RuntimeError("research model inputs changed during identity validation")
    return {"contract": "research_paper_model_identity_v1", "checkpoint_path": str(checkpoint),
            "checkpoint_sha256": checkpoint_hash, "config_path": str(config), "config_sha256": config_hash,
            "mode_artifact_contract_sha256": contract_hash, "fold_complete_sha256": complete_hash,
            "research_cost_rate": float(rate)}


def verify_promoted_research_admission(live: Path, *, market: str, initial_capital: float,
                                      checkpoint: Path, config: Path, rate: float) -> dict:
    """Admit the selected research-cost ABI only from a real promoted addition.

    This read-only check is separate from the expensive pre-cutover audit. It
    binds its accepted receipts, exact model bytes and both managed journals;
    it does not assert fresh data, a broker fill, warmup or trading readiness.
    """
    from stockagent.live.paper_journal import layout_generations

    live = Path(live).resolve(strict=True)
    promotion_path, rebuild_path = live / "promotion_receipt.json", live / "rebuild_receipt.json"
    promotion_sha, rebuild_sha = _stable_regular_sha(promotion_path), _stable_regular_sha(rebuild_path)
    promotion, rebuild = _object(promotion_path), _object(rebuild_path)
    acceptance = promotion.get("acceptance") or {}
    component_acceptance = acceptance.get("new_component_acceptance") or {}
    authorization = rebuild.get("provisional_research_publication") or {}
    disclosure = component_acceptance.get("provisional_research") or {}
    audit = component_acceptance.get("strict_intraday_audit") or {}
    modes = acceptance.get("mode_set") or []
    minute = acceptance.get("minute_curve_validation") or {}
    count = component_acceptance.get("session_count")
    if (not market or promotion.get("simulation_only") is not True
            or promotion.get("production_order_possible") is not False
            or promotion.get("promotion_method") != "same_filesystem_atomic_directory_exchange"
            or Path(str(promotion.get("live_directory") or "")).resolve() != live
            or acceptance.get("contract") != CONTRACT or acceptance.get("added_market") != market
            or acceptance.get("rebuild_receipt_sha256") != rebuild_sha
            or rebuild.get("contract") != CONTRACT or rebuild.get("added_market") != market
            or rebuild.get("simulation_only") is not True or rebuild.get("production_order_possible") is not False
            or authorization.get("contract") != PROVISIONAL_PUBLICATION
            or authorization.get("market") != market or authorization.get("explicit_operator_opt_in") is not True
            or authorization.get("historical_only") is not True
            or authorization.get("source_gaps_may_change_results") is not True
            or authorization.get("broker_or_exchange_execution_claimed") is not False
            or disclosure.get("market") != market or disclosure.get("source_gaps_may_change_results") is not True
            or disclosure.get("broker_or_exchange_execution_claimed") is not False
            or audit.get("passed") is not True or audit.get("full_requested_range_passed") is not True
            or audit.get("error_count") != 0 or type(count) is not int or count < 1
            or not isinstance(modes, list) or not modes or len(modes) != len(set(modes)) or market not in modes
            or minute.get("validated_rows") != count * len(modes) * 270
            or minute.get("duplicate_minute_rows") != 0
            or minute.get("unverified_historical_interior_rows") != 0):
        raise ValueError("promoted research accounting acceptance is absent or incomplete")
    component = evidence_path(live, str(rebuild.get("new_mode_component") or ""))
    component_receipt, component_state = component / "rebuild_receipt.json", component / "state.json"
    if (authorization.get("component_rebuild_receipt_sha256") != _stable_regular_sha(component_receipt)
            or component_acceptance.get("rebuild_receipt_sha256") != _stable_regular_sha(component_receipt)
            or component_acceptance.get("state_sha256") != _stable_regular_sha(component_state)):
        raise ValueError("promoted research component differs from its acceptance")
    component_modes = _object(component_state).get("modes") or {}
    mode = component_modes.get(market)
    if not isinstance(mode, Mapping) or set(component_modes) != {market}:
        raise ValueError("promoted research component mode differs")
    pin = _pin_research_model(mode)
    if (pin != authorization.get("research_model")
            or Path(pin["checkpoint_path"]).resolve() != Path(checkpoint).resolve()
            or Path(pin["config_path"]).resolve() != Path(config).resolve()
            or pin["research_cost_rate"] != rate
            or isinstance(initial_capital, bool) or not isinstance(initial_capital, (int, float))
            or not math.isfinite(initial_capital) or initial_capital <= 0
            or mode.get("initial_capital_twd") != initial_capital):
        raise ValueError("promoted research model, capital or cost differs")
    paths = [live / "orders.jsonl", live / "fills.jsonl"]
    if not all(managed(path) for path in paths):
        raise ValueError("promoted research needs both managed execution journals")
    layout_generations(live, names=("orders.jsonl", "fills.jsonl"))
    if _stable_regular_sha(promotion_path) != promotion_sha or _stable_regular_sha(rebuild_path) != rebuild_sha:
        raise RuntimeError("promotion acceptance changed during research admission")
    return {"contract": "promoted_research_paper_accounting_admission_v1", "market": market,
            "checkpoint_sha256": pin["checkpoint_sha256"], "research_cost_rate": pin["research_cost_rate"],
            "historical_source_gaps_disclosed": True, "broker_fill_claimed": False,
            "native_warmup_or_data_readiness_claimed": False}


def enroll_research_execution_journals(candidate: Path, *, live: Path, capacity_reservation) -> dict:
    """Enroll only an isolated additive candidate, under a caller-held capacity lease.

    This prepares representation-level crash recovery; it never promotes the
    candidate or changes financial bytes. The caller must supply the canonical
    reservation/backing-volume context for the entire operation.
    """
    from stockagent.live.paper_journal import CHUNK_BYTES, managed, publish_snapshot

    candidate, live = Path(candidate).resolve(strict=True), Path(live).resolve(strict=True)
    if (candidate == live or candidate in live.parents or live in candidate.parents
            or candidate.stat().st_dev != live.stat().st_dev):
        raise ValueError("research journal enrollment requires a separate private same-filesystem candidate")
    if not callable(capacity_reservation):
        raise ValueError("research journal enrollment requires a capacity reservation")
    receipt = _object(candidate / "rebuild_receipt.json")
    _flat(_object(candidate / "state.json"))
    control_hashes = {name: hashlib.sha256((candidate / name).read_bytes()).hexdigest()
                      for name in ("state.json", "rebuild_receipt.json")}
    authorization = receipt.get("provisional_research_publication") or {}
    component = evidence_path(candidate, str(receipt.get("new_mode_component") or ""))
    expected_authorization = {
        "contract": PROVISIONAL_PUBLICATION, "market": receipt.get("added_market"),
        "component_rebuild_receipt_sha256": hashlib.sha256(
            (component / "rebuild_receipt.json").read_bytes()).hexdigest(),
        "explicit_operator_opt_in": True, "historical_only": True,
        "source_gaps_may_change_results": True, "broker_or_exchange_execution_claimed": False,
    }
    if "research_model" in authorization:
        component_mode = (_object(component / "state.json").get("modes") or {}).get(receipt.get("added_market"))
        if not isinstance(component_mode, Mapping):
            raise ValueError("research component model is missing")
        expected_authorization["research_model"] = _pin_research_model(component_mode)
    if receipt.get("contract") != CONTRACT or authorization != expected_authorization:
        raise ValueError("research journal enrollment needs an explicitly authorized additive candidate")
    paths = [candidate / "orders.jsonl", candidate / "fills.jsonl"]
    if any(path.is_symlink() or not path.is_file() for path in paths):
        raise ValueError("research execution journals must be regular files")
    lock_path = candidate / ".engine.lock"
    lock_path.touch(exist_ok=True)
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("private candidate has an active engine owner") from None
        try:
            _flat(_object(candidate / "state.json"))
            def check_controls():
                if any(hashlib.sha256((candidate / name).read_bytes()).hexdigest() != digest
                       for name, digest in control_hashes.items()):
                    raise RuntimeError("private candidate controls changed during journal enrollment")
            check_controls()
            enrolled = [managed(path) for path in paths]
            if len(set(enrolled)) != 1:
                raise ValueError("partial journal enrollment requires independent recovery")
            original = {path.name: {"bytes": path.stat().st_size, "sha256": sha(path)} for path in paths}
            maximum_growth = sum(info["bytes"] + ((info["bytes"] + CHUNK_BYTES - 1) // CHUNK_BYTES) * 128 * 1024
                                 for info in original.values()) + 2 * 1024**2
            if not all(enrolled):
                with capacity_reservation(maximum_growth):
                    check_controls()
                    for path in paths:
                        publish_snapshot(path, operation="seed")
                        check_controls()
            for path in paths:
                raw = hashlib.sha256()
                with path.open("rb") as stream:
                    for block in iter(lambda: stream.read(1024**2), b""):
                        raw.update(block)
                if (path.stat().st_size != original[path.name]["bytes"]
                        or raw.hexdigest() != original[path.name]["sha256"]
                        or sha(path) != original[path.name]["sha256"]):
                    raise RuntimeError("private journal enrollment changed or misread financial bytes")
            check_controls()
            return {"contract": "private_research_execution_journal_enrollment_v1",
                    "candidate": str(candidate), "live_ledger_mutated": False,
                    "already_enrolled": all(enrolled), "maximum_growth_bytes": maximum_growth,
                    "journals": original, "promotion_completed": False}
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def evidence_path(candidate: Path, relative: str) -> Path:
    path = (candidate / relative).resolve(strict=True)
    if Path(relative).is_absolute() or not path.is_relative_to(candidate.resolve()) or path == candidate.resolve():
        raise ValueError("addition evidence escaped candidate")
    return path


def _flat(state):
    """One fail-closed guard for planning, merging and promotion readback.

    A zero position count alone is not a quiescent account: an interrupted
    journal commit, pending revision or working order can still change it.
    """
    modes = state.get("modes") if isinstance(state, Mapping) else None
    if not isinstance(modes, Mapping):
        raise ValueError("invalid live mode state")
    active = []
    unresolved = {}
    for market, mode in modes.items():
        if not isinstance(mode, Mapping):
            raise ValueError(f"invalid live mode state: {market}")
        positions = mode.get("positions", {})
        if not isinstance(positions, Mapping) or any(not isinstance(p, Mapping) for p in positions.values()):
            raise ValueError(f"invalid position state in live mode: {market}")
        count = mode.get("open_position_count", 0)
        if type(count) is not int or count < 0:
            raise ValueError(f"invalid open position count in live mode: {market}")
        shares = [position.get("signed_shares") for position in positions.values()]
        if any(type(value) is not int for value in shares):
            raise ValueError(f"invalid signed shares in live mode: {market}")
        if count != 0 or any(value != 0 for value in shares):
            active.append(market)
        blockers = []
        for key in ("ledger_state_divergence", "entry_retry_pending_commit", "research_terminal_pending_commit"):
            if mode.get(key) is not None:
                blockers.append(key)
        if mode.get("pending_signal_id") not in (None, ""):
            blockers.append("pending_signal_id")
        for key in ("pending_entry_shares", "pending_reduction_shares", "pending_entry_source_count", "unresolved_exit_count"):
            value = mode.get(key)
            if value is not None and (type(value) is not int or value != 0):
                blockers.append(key)
        for key in ("pending_entry_orders", "pending_reduction_orders"):
            orders = mode.get(key, {})
            if not isinstance(orders, Mapping) or any(not isinstance(row, Mapping) for row in orders.values()):
                blockers.append(key + ":invalid")
                continue
            for order in orders.values():
                status = order.get("status")
                if not isinstance(status, str) or status not in {
                    "filled", "inventory_target_retained", "cancelled_entry_price_guard",
                    "cancelled_exit_priority", "cancelled_account_nav_budget_exhausted",
                    "cancelled_signal_revision",
                }:
                    blockers.append(key + ":" + (str(status) if status else "unknown"))
                    break
        if blockers:
            unresolved[str(market)] = blockers
    if active:
        raise ValueError(f"paper positions are open (inventory is open): {sorted(active)}")
    if unresolved:
        raise ValueError(f"unresolved paper activity: {unresolved}")


def retained_accounts_match(prior_modes: dict, current_modes: dict) -> bool:
    """Require exact flat accounts, allowing only the evidenced mark heartbeat.

    The running engine advances ``last_mark_at`` even after all inventory is
    closed. That clock is not a change to cash, P&L, positions or executions.
    Candidate-versus-prior validation remains byte/value exact; only comparison
    with the concurrently running source may accept this one metadata field.
    Unknown, naive, backwards or cross-day clocks remain a conflict.
    """
    if not isinstance(prior_modes, dict) or not isinstance(current_modes, dict):
        return False
    if set(prior_modes) != set(current_modes):
        return False
    try:
        _flat({"modes": prior_modes})
        _flat({"modes": current_modes})
        for market, before in prior_modes.items():
            after = current_modes[market]
            if before == after:
                continue
            before_account, after_account = dict(before), dict(after)
            before_clock = before_account.pop("last_mark_at", None)
            after_clock = after_account.pop("last_mark_at", None)
            if before_account != after_account:
                return False
            before_at = datetime.fromisoformat(before_clock)
            after_at = datetime.fromisoformat(after_clock)
            if (before_at.tzinfo is None or after_at.tzinfo is None
                    or before_at.date() != after_at.date()
                    or before_at > after_at):
                return False
    except (ValueError, TypeError, KeyError, AttributeError):
        return False
    return True


def _unfunded_activation(state: dict, *, added: str, retained: set[str], capital: float):
    """Only an aborted, disabled, zero-execution account can be superseded."""
    modes = state["modes"]
    if set(modes) != retained | {added} or set(state.get("enabled_markets") or ()) != retained:
        raise ValueError("aborted activation mode/enablement scope differs")
    mode = modes[added]
    if (mode.get("configured_enabled") is not False
            or mode.get("entry_fill_outcome") != "no_fill"
            or mode.get("initial_capital_twd") != capital
            or mode.get("total_equity_twd") != capital):
        raise ValueError("aborted activation is not a disabled unfunded paper account")
    for key in ("entry_filled_shares", "entry_fill_count", "open_position_count",
                "cumulative_realized_net_pnl_twd", "open_net_liquidation_pnl_twd",
                "cumulative_commission_rebate_accrued_twd", "pending_entry_shares",
                "pending_reduction_shares"):
        value = float(mode.get(key, 0))
        if not math.isfinite(value) or value != 0:
            raise ValueError(f"aborted activation has financial activity: {key}")
    for key in ("positions", "pending_entry_orders", "pending_reduction_orders"):
        if mode.get(key):
            raise ValueError(f"aborted activation has inventory/orders: {key}")
    _flat(state)


def _check_abort_tail(name: str, data: bytes, *, added: str, mode: dict) -> int:
    if name in {"fills.jsonl", "orders.jsonl"} and data:
        raise ValueError("cannot supersede an activation with any fill/order records")
    count = 0
    for line in data.splitlines():
        row = json.loads(line)
        if not isinstance(row, dict) or row.get("market") != added:
            raise ValueError("aborted activation contains another market's record")
        if (row.get("session_date", mode["session_date"]) != mode["session_date"]
                or row.get("signal_id", mode["signal_id"]) != mode["signal_id"]):
            raise ValueError("aborted activation contains unrelated decisions")
        for key in ("filled_shares", "previous_signed_shares", "reduction_filled_shares",
                    "entry_filled_shares", "entry_fill_count", "submitted"):
            value = float(row.get(key, 0))
            if not math.isfinite(value) or value != 0:
                raise ValueError("aborted activation contains an execution")
        if name == "signals.jsonl" and row.get("status") not in {"hold", "skipped", "blocked"}:
            raise ValueError("aborted activation contains a nonempty execution decision")
        if name == "events.jsonl" and row.get("event") not in {
                "signal_commit_started", "signal_registered", "exit_limits_submitted",
                "closing_auction_limits_submitted"}:
            raise ValueError("aborted activation contains an unknown account event")
        if name == "latency.jsonl" and row.get("simulation_only") is not True:
            raise ValueError("aborted activation latency is not paper evidence")
        count += 1
    return count


def inspect_aborted_activation(live: Path, candidate: Path) -> dict:
    """Read-only proof of the narrowly recoverable failed-deployment boundary.

    No history is erased or rewritten. The ordinary promoter will retain the
    entire live directory for rollback; all additional records are also copied
    into candidate evidence before they are superseded by accepted replay.
    """
    receipt = _object(candidate / "rebuild_receipt.json")
    if receipt.get("contract") != CONTRACT:
        raise ValueError("aborted activation requires an additive candidate")
    added, retained = receipt["added_market"], set(receipt["retained_markets"])
    prior = _object(evidence_path(candidate, receipt["prior_state"]))
    state = _object(live / "state.json")
    capital = _object(candidate / "state.json")["modes"][added]["initial_capital_twd"]
    _unfunded_activation(state, added=added, retained=retained, capital=capital)
    if not retained_accounts_match(prior["modes"], {m: state["modes"][m] for m in retained}):
        raise ValueError("retained accounts changed during the aborted activation")
    streams = {}
    for name in LEDGERS:
        source = live / name
        prefix = receipt["retained_execution_prefixes"].get(name, {"bytes": 0, "sha256": hashlib.sha256(b"").hexdigest()})
        if (not source.is_file() or source.stat().st_size < prefix["bytes"]
                or sha(source, prefix["bytes"]) != prefix["sha256"]):
            raise ValueError(f"aborted activation altered retained bytes: {name}")
        with open_paper_journal(source) as f:
            f.seek(prefix["bytes"])
            tail = f.read()
        count = _check_abort_tail(name, tail, added=added, mode=state["modes"][added])
        streams[name] = {"bytes": source.stat().st_size, "sha256": sha(source),
                         "offset": prefix["bytes"], "tail_bytes": len(tail),
                         "tail_sha256": hashlib.sha256(tail).hexdigest(), "tail_records": count}
        if sha(source, len(tail), offset=prefix["bytes"]) != streams[name]["tail_sha256"]:
            raise ValueError("aborted activation records advanced during inspection")
    current = _object(live / "state.json")
    if not retained_accounts_match(state["modes"], current["modes"]):
        raise ValueError("aborted activation account advanced during inspection")
    return {"contract": ABORTED_ACTIVATION, "added_market": added,
            "retained_markets": sorted(retained), "simulation_only": True,
            "production_order_possible": False, "state": state, "streams": streams}


def preserve_aborted_activation(live: Path, candidate: Path) -> dict:
    """Archive the unfunded activation, without changing the running ledger."""
    proof = inspect_aborted_activation(live, candidate)
    receipt = _object(candidate / "rebuild_receipt.json")
    if receipt.get("aborted_activation"):
        return validate_aborted_activation(candidate, live=live)
    component = evidence_path(candidate, receipt["new_mode_component"])
    relative = str(component.parent.relative_to(candidate.resolve()) / "aborted_activation")
    root = candidate / relative
    root.mkdir(exist_ok=False)
    atomic_write_json(root / "state.json", proof.pop("state"))
    for name, stream in proof["streams"].items():
        with open_paper_journal(live / name) as incoming, (root / name).open("wb") as out:
            incoming.seek(stream["offset"])
            shutil.copyfileobj(incoming, out, 1024**2)
        if ((root / name).stat().st_size != stream["tail_bytes"]
                or sha(root / name) != stream["tail_sha256"]):
            raise ValueError("aborted activation archive changed while copying")
    atomic_write_json(root / "receipt.json", proof)
    receipt["aborted_activation"] = relative
    atomic_write_json(candidate / "rebuild_receipt.json", receipt)
    return validate_aborted_activation(candidate, live=live)


def validate_aborted_activation(candidate: Path, *, live: Path | None = None) -> dict:
    receipt = _object(candidate / "rebuild_receipt.json")
    root = evidence_path(candidate, receipt["aborted_activation"])
    proof, state = _object(root / "receipt.json"), _object(root / "state.json")
    added, retained = receipt["added_market"], set(receipt["retained_markets"])
    if (proof.get("contract") != ABORTED_ACTIVATION or proof.get("added_market") != added
            or set(proof.get("retained_markets") or ()) != retained
            or proof.get("simulation_only") is not True or proof.get("production_order_possible") is not False
            or set(proof.get("streams") or {}) != set(LEDGERS)):
        raise ValueError("invalid aborted activation evidence contract")
    capital = _object(candidate / "state.json")["modes"][added]["initial_capital_twd"]
    _unfunded_activation(state, added=added, retained=retained, capital=capital)
    prior = _object(evidence_path(candidate, receipt["prior_state"]))
    if not retained_accounts_match(prior["modes"], {m: state["modes"][m] for m in retained}):
        raise ValueError("aborted activation changed retained accounts")
    for name, stream in proof["streams"].items():
        path = root / name
        prefix = receipt["retained_execution_prefixes"].get(name, {"bytes": 0})
        if (stream["offset"] != prefix["bytes"] or stream["bytes"] != stream["offset"] + stream["tail_bytes"]
                or path.stat().st_size != stream["tail_bytes"] or sha(path) != stream["tail_sha256"]
                or _check_abort_tail(name, path.read_bytes(), added=added, mode=state["modes"][added]) != stream["tail_records"]):
            raise ValueError(f"aborted activation archive differs: {name}")
        if live is not None and ((live / name).stat().st_size != stream["bytes"]
                                 or sha(live / name) != stream["sha256"]
                                 or sha(live / name, stream["offset"]) != receipt["retained_execution_prefixes"][name]["sha256"]
                                 or sha(live / name, stream["tail_bytes"], offset=stream["offset"]) != stream["tail_sha256"]):
            raise ValueError(f"live ledger advanced after aborted activation archive: {name}")
    if live is not None:
        current = _object(live / "state.json")
        _unfunded_activation(current, added=added, retained=retained, capital=capital)
        if not retained_accounts_match(state["modes"], current["modes"]):
            raise ValueError("live accounts advanced after aborted activation archive")
    return {"contract": proof["contract"], "evidence_directory": str(root),
            "streams": proof["streams"], "superseded_account_unfunded": True,
            "original_live_directory_retained_by_atomic_exchange": True}


def finalize_completed_replay_entries(component: Path, *, live: Path, market: str,
                                      completed_day: date, recorded_at: datetime) -> dict | None:
    """Finalize an isolated flat replay's expired intentions, never its fills."""
    component, live = Path(component).resolve(strict=True), Path(live).resolve(strict=True)
    if component == live or component in live.parents or live in component.parents:
        raise ValueError("entry finalization requires an isolated replay component")
    with (component / ".engine.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return _finalize_completed_replay_entries(component, market=market,
            completed_day=completed_day, recorded_at=recorded_at)


def _finalize_completed_replay_entries(component: Path, *, market: str,
                                       completed_day: date, recorded_at: datetime) -> dict | None:
    from stockagent.live.tw_day_trade_simulation import expire_replay_entry_remainders
    if _object(component / "rebuild_receipt.json").get("completed_entry_deadline_finalization"):
        return validate_completed_replay_entry_finalization(component)
    state = _object(component / "state.json")
    if (state.get("simulation_only") is not True or state.get("production_order_possible") is not False
            or set(state.get("modes") or {}) != {market}
            or recorded_at.tzinfo is None or completed_day >= recorded_at.date()):
        raise ValueError("entry finalization requires one completed replay session")
    mode = state["modes"][market]
    if (mode.get("session_date") != completed_day.isoformat()
            or mode.get("open_position_count") != 0
            or any(p.get("signed_shares") for p in (mode.get("positions") or {}).values())):
        raise ValueError("entry finalization cannot close any inventory")
    before = deepcopy(state)
    expired = expire_replay_entry_remainders(mode, as_of=recorded_at)
    _flat(state)
    if state == before:
        return None
    def financial_view(value):
        account = deepcopy(value["modes"][market])
        account.pop("pending_entry_orders", None)
        account.pop("pending_entry_shares", None)
        return account
    if financial_view(before) != financial_view(state):
        raise ValueError("entry finalization changed recorded inventory or economics")
    journals = {name: sha(component / name) if (component / name).is_file() else None
                for name in LEDGERS}
    receipt = _object(component / "rebuild_receipt.json")
    evidence = component / "completion_evidence" / "expired_entries"
    evidence.mkdir(parents=True, exist_ok=False)
    atomic_write_json(evidence / "state.before.json", before)
    proof = {"contract": "completed_replay_entry_deadline_finalization_v1", "market": market,
        "session_date": completed_day.isoformat(), "recorded_at": recorded_at.isoformat(),
        "state_before": str((evidence / "state.before.json").relative_to(component)),
        "state_before_sha256": sha(evidence / "state.before.json"),
        "expired_orders": expired, "unchanged_execution_journals": journals,
        "financial_values_changed": False, "broker_orders": 0, "new_fills": 0}
    receipt["completed_entry_deadline_finalization"] = proof
    atomic_write_json(component / "state.json", state)
    atomic_write_json(component / "rebuild_receipt.json", receipt)
    if any((sha(component / name) if (component / name).is_file() else None) != digest
           for name, digest in journals.items()):
        raise RuntimeError("execution journal changed during entry finalization")
    return proof


def validate_completed_replay_entry_finalization(component: Path) -> dict | None:
    from stockagent.live.tw_day_trade_simulation import expire_replay_entry_remainders
    receipt = _object(component / "rebuild_receipt.json")
    proof = receipt.get("completed_entry_deadline_finalization")
    if proof is None:
        if (component / "completion_evidence/expired_entries").exists():
            raise ValueError("incomplete entry finalization requires private recovery")
        return None
    if (proof.get("contract") != "completed_replay_entry_deadline_finalization_v1"
            or proof.get("financial_values_changed") is not False
            or proof.get("broker_orders") != 0 or proof.get("new_fills") != 0):
        raise ValueError("invalid completed-entry finalization proof")
    before_path = evidence_path(component, proof["state_before"])
    if sha(before_path) != proof["state_before_sha256"]:
        raise ValueError("entry finalization before-image changed")
    expected = _object(before_path)
    if set(expected.get("modes") or {}) != {proof["market"]}:
        raise ValueError("entry finalization scope differs")
    recorded = datetime.fromisoformat(proof["recorded_at"])
    if (expected["modes"][proof["market"]].get("session_date") != proof["session_date"]
            or recorded.tzinfo is None or date.fromisoformat(proof["session_date"]) >= recorded.date()):
        raise ValueError("entry finalization requires its completed-session clock")
    expired = expire_replay_entry_remainders(expected["modes"][proof["market"]],
        as_of=recorded)
    if (expired != proof["expired_orders"] or expected != _object(component / "state.json")
            or set(proof["unchanged_execution_journals"]) != set(LEDGERS)
            or any((sha(component / name) if (component / name).is_file() else None) != digest
                   for name, digest in proof["unchanged_execution_journals"].items())):
        raise ValueError("entry finalization changed inventory, economics or execution evidence")
    _flat(expected)
    return proof


def merge_addition(live: Path, new_replay: Path, candidate: Path, *, added_market: str,
                   aborted_activation_candidate: Path | None = None,
                   allow_provisional_research_market: str | None = None,
                   pin_research_model: bool = False) -> dict:
    """Stage an isolated union. Never write to either input directory."""
    if allow_provisional_research_market is not None and allow_provisional_research_market != added_market:
        raise ValueError("provisional research authorization must name exactly the added market")
    if type(pin_research_model) is not bool or (pin_research_model and allow_provisional_research_market != added_market):
        raise ValueError("research model pin requires exact provisional market authorization")
    old_state = _object(live / "state.json")
    new_state = _object(new_replay / "state.json")
    validate_completed_replay_entry_finalization(new_replay)
    _flat(old_state)
    _flat(new_state)
    abort = None
    if aborted_activation_candidate is not None:
        abort = inspect_aborted_activation(live, aborted_activation_candidate)
        if abort["added_market"] != added_market:
            raise ValueError("aborted activation targets a different addition")
        old_state = deepcopy(old_state)
        del old_state["modes"][added_market]
    if added_market in old_state["modes"] or set(new_state["modes"]) != {added_market}:
        raise ValueError("addition must contain exactly one genuinely new mode")
    source_proofs = {name: {"bytes": (live / name).stat().st_size, "sha256": sha(live / name)}
                     for name in LEDGERS if (live / name).is_file()}
    live_proofs = source_proofs
    if abort is not None:
        source_proofs = _object(aborted_activation_candidate / "rebuild_receipt.json")["retained_execution_prefixes"]
    if candidate.exists() and any(candidate.iterdir()):
        raise ValueError("additive candidate must be empty")
    shutil.copytree(live, candidate, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("*.lock", ".lock", "*.tmp.*", "quote_broker"))
    rebind_cloned_journals(live, candidate)
    if abort is not None:
        # Only this isolated copy loses the aborted tail. The original remains
        # unchanged, and the tail is separately archived before promotion.
        for name, proof in source_proofs.items():
            with journal_write_transaction(candidate / name, operation="replace"), (candidate / name).open("r+b") as stream:
                stream.truncate(proof["bytes"])
                if managed(candidate / name):
                    stream.flush()
                    os.fsync(stream.fileno())
        marks = candidate / "marks.jsonl"
        pending = candidate / ".retained_marks.tmp"
        with journal_write_transaction(marks, operation="replace"):
            with open_paper_journal(marks) as incoming, pending.open("wb") as out:
                for line in incoming:
                    if json.loads(line).get("market") != added_market:
                        out.write(line)
                if managed(marks):
                    out.flush()
                    os.fsync(out.fileno())
            pending.replace(marks)
    evidence_root = candidate / "addition_evidence"
    if evidence_root.exists():
        if evidence_root.is_symlink() or not evidence_root.is_dir():
            raise ValueError("existing addition evidence is not a private directory")
        previous_receipt_hash = hashlib.sha256((live / "rebuild_receipt.json").read_bytes()).hexdigest()
        suffix = hashlib.sha256(f"{added_market}:{previous_receipt_hash}".encode()).hexdigest()[:24]
        evidence = evidence_root / f"addition-{suffix}"
        evidence.mkdir(exist_ok=False)
    else:
        evidence = evidence_root
        evidence.mkdir()
    atomic_write_json(evidence / "prior_state.json", old_state)
    atomic_write_json(evidence / "prior_rebuild_receipt.json", _object(live / "rebuild_receipt.json"))
    # Keep the independently accepted new replay as a component with its own
    # source/price contracts; never relabel old executions as the new policy.
    component = evidence / "new_mode"
    shutil.copytree(new_replay, component, ignore=shutil.ignore_patterns("*.lock", ".lock"))
    rebind_cloned_journals(new_replay, component)
    appended = {}
    for name in (*LEDGERS, "marks.jsonl"):
        source = new_replay / name
        target = candidate / name
        if not source.is_file():
            continue
        if name in LEDGERS:
            appended[name] = {"offset": target.stat().st_size if target.is_file() else 0,
                              "bytes": source.stat().st_size, "sha256": sha(source)}
        if target.is_file() and target.stat().st_size:
            with open_paper_journal(target) as f:
                f.seek(-1, 2)
                if f.read(1) != b"\n":
                    raise ValueError(f"retained JSONL lacks a terminal newline: {name}")
        with journal_write_transaction(target, operation="append"), target.open("ab") as out, open_paper_journal(source) as incoming:
            shutil.copyfileobj(incoming, out, 1024**2)
            if managed(target):
                out.flush()
                os.fsync(out.fileno())
    merged = deepcopy(old_state)
    merged["modes"][added_market] = new_state["modes"][added_market]
    merged["enabled_markets"] = sorted(merged["modes"])
    merged["state_revision"] = int(merged.get("state_revision") or 0)+1
    merged["dashboard_content_revision"] = int(merged.get("dashboard_content_revision") or 0)+1
    atomic_write_json(candidate / "state.json", merged)
    positions_path = candidate / "positions.json"
    if positions_path.is_file() and (new_replay / "positions.json").is_file():
        positions = _object(positions_path)
        incoming = _object(new_replay / "positions.json")
        # Canonical positions snapshots wrap accounts under modes.
        if isinstance(positions.get("modes"), dict) and isinstance(incoming.get("modes"), dict):
            positions["modes"][added_market] = incoming["modes"][added_market]
            atomic_write_json(positions_path, positions)
    for path in (new_replay / "position_history").glob("*/*.json"):
        relative = path.relative_to(new_replay)
        target = candidate / relative
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        else:
            prior, incoming = _object(target), _object(path)
            if isinstance(prior.get("modes"), dict) and isinstance(incoming.get("modes"), dict):
                prior["modes"][added_market] = incoming["modes"][added_market]
                atomic_write_json(target, prior)
            else:
                raise ValueError(f"unsupported overlapping position history: {relative}")
    for name, proof in live_proofs.items():
        retained_proof = source_proofs[name]
        if ((live / name).stat().st_size != proof["bytes"] or sha(live / name) != proof["sha256"]
                or sha(candidate / name, retained_proof["bytes"]) != retained_proof["sha256"]):
            raise ValueError(f"existing ledger changed during addition: {name}")
    current_modes = _object(live / "state.json")["modes"]
    if abort is not None:
        current_modes = {m: row for m, row in current_modes.items() if m != added_market}
    if not retained_accounts_match(old_state["modes"], current_modes):
        raise ValueError("existing accounts advanced during addition; retry from a fresh snapshot")
    for name, proof in appended.items():
        if sha(new_replay / name) != proof["sha256"] or sha(component / name) != proof["sha256"]:
            raise ValueError(f"new component changed during addition: {name}")
    receipt = {"schema_version": 1, "contract": CONTRACT, "simulation_only": True,
               "production_order_possible": False, "added_market": added_market,
               "retained_markets": sorted(old_state["modes"]),
               "retained_execution_prefixes": source_proofs,
               "appended_execution_segments": appended,
               "new_mode_component": str(component.relative_to(candidate)),
               "prior_state": str((evidence / "prior_state.json").relative_to(candidate)),
               "prior_rebuild_receipt": str((evidence / "prior_rebuild_receipt.json").relative_to(candidate)),
               "old_signal_pins_and_executions_unchanged": True,
               "retained_live_comparison": "exact_flat_accounts_except_monotonic_same_day_last_mark_at_v1",
               "sessions": _object(new_replay / "rebuild_receipt.json")["sessions"]}
    if allow_provisional_research_market is not None:
        receipt["provisional_research_publication"] = {
            "contract": PROVISIONAL_PUBLICATION,
            "market": added_market,
            "component_rebuild_receipt_sha256": hashlib.sha256(
                (component / "rebuild_receipt.json").read_bytes()).hexdigest(),
            "explicit_operator_opt_in": True,
            "historical_only": True,
            "source_gaps_may_change_results": True,
            "broker_or_exchange_execution_claimed": False,
        }
        if pin_research_model:
            receipt["provisional_research_publication"]["research_model"] = _pin_research_model(new_state["modes"][added_market])
    atomic_write_json(candidate / "rebuild_receipt.json", receipt)
    if abort is not None:
        preserve_aborted_activation(live, candidate)
        receipt = _object(candidate / "rebuild_receipt.json")
    return receipt


def adopt_isolated_addition_curves(candidate: Path, output: Path, *, live: Path,
                                  provisional_opening_cost_targets: set[tuple[str, str, str]] | None = None,
                                  provisional_opening_first_bar_targets: set[tuple[str, str, str]] | None = None) -> dict:
    """Adopt derived outputs only inside the unpromoted additive candidate."""
    from scripts import rebuild_tw_day_trade_minute_curves as curves
    from scripts.promote_tw_day_trade_replay import _validate_minute_curve_coverage, _validate_benchmarks
    candidate, output, live = candidate.resolve(strict=True), output.resolve(strict=True), live.resolve(strict=True)
    if (candidate == live or candidate in live.parents or live in candidate.parents
            or not output.is_relative_to(candidate) or output == candidate
            or candidate.stat().st_dev != live.stat().st_dev):
        raise ValueError("curve adoption requires a private same-filesystem additive candidate")
    rebuild = _object(candidate / "rebuild_receipt.json")
    if rebuild.get("contract") != CONTRACT:
        raise ValueError("curve adoption requires the canonical addition receipt")
    receipt_path = output / "minute_curve_receipt.json"
    receipt = _object(receipt_path)
    if provisional_opening_cost_targets and provisional_opening_first_bar_targets:
        raise ValueError("choose one explicitly authorized opening reference policy")
    policy = curves.opening_assumption_policy(bool(provisional_opening_first_bar_targets))
    unselected = curves.opening_assumption_policy(not bool(provisional_opening_first_bar_targets))
    if ((receipt.get("strategy") or {}).get(unselected["key"])
            or receipt.get(unselected["flag"]) is True):
        raise ValueError("unapproved or mixed opening reference assumption policy")
    cost_scope = set(provisional_opening_first_bar_targets or provisional_opening_cost_targets or ())
    cost_proof = (receipt.get("strategy") or {}).get(policy["key"])
    if cost_scope != {tuple(item) for item in (cost_proof or {}).get("authorized_targets", [])}:
        raise ValueError("derived provisional opening cost scope differs from explicit authorization")
    if cost_scope and any(market not in rebuild["retained_markets"] for market, _, _ in cost_scope):
        raise ValueError("provisional opening cost cannot change the independently accepted new mode")
    required_sources = {"marks.jsonl", "fills.jsonl", "orders.jsonl", "events.jsonl", "state.json", "benchmark_history.json"}
    if set(receipt.get("source_ledger_hashes") or {}) != required_sources:
        raise ValueError("derived curves require the complete immutable source scope")
    proof = receipt.get("independently_preserved_addition_component") or {}
    if (receipt.get("simulation_only") is not True or receipt.get("production_order_possible") is not False
            or proof.get("market") != rebuild["added_market"]
            or proof.get("component") != rebuild["new_mode_component"]):
        raise ValueError("derived curves lack exact component and simulation acceptance")
    candidate_hashes = {}
    for name, digest in (receipt.get("source_ledger_hashes") or {}).items():
        if name not in {"marks.jsonl", "fills.jsonl", "orders.jsonl", "events.jsonl", "state.json", "benchmark_history.json"}:
            raise ValueError("unknown derived-curve source member")
        current = candidate_hashes[name] = sha(candidate / name)
        if current != digest:
            # A crash/failed validation may have installed derived valuations
            # before recording adoption. Reuse only exact accepted outputs,
            # with the independently preserved original source still proven.
            output_key = {"marks.jsonl": "marks", "benchmark_history.json": "benchmark_history"}.get(name)
            original = (output / "marks.before.jsonl" if name == "marks.jsonl"
                        else live / name)
            if (output_key is None or current != receipt["outputs"][output_key]["sha256"]
                    or sha(original) != digest):
                raise ValueError(f"accepted addition source changed during curve reconstruction: {name}")
    days = [row["session_date"] for row in rebuild["sessions"]]
    markets = set(_object(candidate / "state.json")["modes"])
    if curves.minute_curve_scope_failures(receipt, completed_session_dates=days, expected_markets=markets):
        raise ValueError("derived additive curve scope differs")
    for name, key in (("marks.jsonl", "marks"), ("benchmark_history.json", "benchmark_history")):
        if sha(output / name) != (receipt.get("outputs") or {}).get(key, {}).get("sha256"):
            raise ValueError("derived addition curve output changed")
    if sha(output / "marks.before.jsonl") != receipt["source_ledger_hashes"].get("marks.jsonl"):
        raise ValueError("retained curve before-image differs")
    # No engine can own this private candidate. A failure leaves only an
    # unpromoted derived tree; original live state and executions stay intact.
    with (candidate / ".engine.lock").open("a+") as lock, curves.minute_curve_write_lock(candidate):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if any(sha(candidate / name) != digest
               for name, digest in candidate_hashes.items()):
            raise RuntimeError("private addition inputs changed before curve adoption")
        for name, key in (("marks.jsonl", "marks"), ("benchmark_history.json", "benchmark_history")):
            if sha(output / name) != receipt["outputs"][key]["sha256"]:
                raise RuntimeError("derived addition curves changed before adoption")
        for name in ("marks.jsonl", "benchmark_history.json", "minute_curve_receipt.json"):
            if not (candidate / name).is_file() or sha(candidate / name) != sha(output / name):
                curves._atomic_text(candidate / name, (output / name).read_text(encoding="utf-8"))
        from stockagent.live.benchmark_history_projection import adopt_benchmark_projection
        benchmark_projection = adopt_benchmark_projection(state_dir=candidate, source_state_dir=output,
            expected_source_sha256=receipt["outputs"]["benchmark_history"]["sha256"],
            created_at=datetime.now().astimezone().isoformat())
        failures = []
        minute = _validate_minute_curve_coverage(candidate, completed_session_dates=days,
            expected_markets=markets, failures=failures)
        if failures:
            raise ValueError("derived addition curves failed independent minute validation: " + "; ".join(failures[:5]))
        _validate_benchmarks(candidate, completed_session_dates=days)
        rebuild["retained_curve_recovery"] = {
            "contract": CURVE_RECOVERY,
            "curve_receipt": str(receipt_path.relative_to(candidate)),
            "curve_receipt_sha256": sha(receipt_path),
            "marks_before": str((output / "marks.before.jsonl").relative_to(candidate)),
            "marks_before_sha256": sha(output / "marks.before.jsonl"),
            "source_price_or_execution_invented": False,
            "provisional_opening_cost_valuation_targets": [list(item) for item in sorted(cost_scope)],
        }
        if provisional_opening_first_bar_targets:
            recovery = rebuild["retained_curve_recovery"]
            recovery.pop("provisional_opening_cost_valuation_targets")
            recovery[policy["scope"]] = [list(item) for item in sorted(cost_scope)]
            recovery["provisional_opening_first_bar_references"] = [
                {"market": row["market"], "session_date": row["session_date"],
                 "symbol": cohort["symbol"], "source_minute": cohort["reference_bar"]["source_minute"]}
                for row in cost_proof["applied"] for cohort in row["cohorts"]]
        atomic_write_json(candidate / "rebuild_receipt.json", rebuild)
    return {"contract": CURVE_RECOVERY, "minute_rows": minute["validated_rows"],
            "cash_rows": (receipt.get("cash_account_gap_recovery") or {}).get("generated_rows"),
            "benchmark_projection_adoption": benchmark_projection,
            "financial_journals_changed": False}


def validate_retained_curve_recovery(candidate: Path, receipt: dict) -> dict | None:
    proof = receipt.get("retained_curve_recovery")
    if proof is None:
        return None
    if proof.get("contract") != CURVE_RECOVERY or proof.get("source_price_or_execution_invented") is not False:
        raise ValueError("invalid retained curve recovery contract")
    curve_path = evidence_path(candidate, proof["curve_receipt"])
    before_path = evidence_path(candidate, proof["marks_before"])
    if sha(curve_path) != proof["curve_receipt_sha256"] or sha(before_path) != proof["marks_before_sha256"]:
        raise ValueError("retained curve recovery evidence changed")
    curves = _object(curve_path)
    cash = curves.get("cash_account_gap_recovery") or {}
    if curves.get("source_ledger_hashes", {}).get("marks.jsonl") != proof["marks_before_sha256"]:
        raise ValueError("cash valuation before-image has a different lineage")
    prior = _object(evidence_path(candidate, receipt["prior_state"]))
    inspected, expected_rows = inspect_retained_curve_recovery(candidate,
        sessions=[row["session_date"] for row in receipt["sessions"]], markets=set(receipt["retained_markets"]),
        marks_path=before_path, prior_state=prior, prefixes=receipt["retained_execution_prefixes"],
        reconstructed_at=datetime.fromisoformat(cash["reconstructed_at"]))
    if inspected["cash_account_gap_recovery"] != cash:
        raise ValueError("cash recovery proof differs from independent retained-journal readback")
    actual = {}
    from scripts.rebuild_tw_day_trade_minute_curves import CASH_GAP_RECOVERY_CONTRACT
    for row in _journal_rows(candidate / "marks.jsonl"):
        if row.get("cash_account_gap_recovery_contract") == CASH_GAP_RECOVERY_CONTRACT:
            if row.get("market") in receipt["retained_markets"]:
                key = (row["market"], row["minute"])
                if key in actual:
                    raise ValueError("duplicate recovered cash minute")
                value = dict(row)
                value.pop("accepted_endpoint_accounting_verified", None)
                actual[key] = value
    if actual != {(row["market"], row["minute"]): row for row in expected_rows}:
        raise ValueError("derived cash minute values differ from retained-account proof")
    repairs = (curves.get("strategy") or {}).get("repaired_opening_endpoints") or []
    if {f"{row['market']}:{row['session_date']}" for row in repairs} != set(inspected["opening_projection_repairs"]):
        raise ValueError("opening projection repair scope differs from its independent account proof")
    # Recheck the final adopted endpoints against the original fill prefix,
    # not merely the receipt's list of intended repairs. A correct before-image
    # and scope cannot authorize tampered accounting in the derived output.
    from scripts import rebuild_tw_day_trade_minute_curves as curve_builder
    sessions = [row["session_date"] for row in receipt["sessions"]]
    first, last = date.fromisoformat(sessions[0]), date.fromisoformat(sessions[-1])
    fills = list(_journal_rows(candidate / "fills.jsonl",
        receipt["retained_execution_prefixes"]["fills.jsonl"]["bytes"]))
    positions = curve_builder.load_positions(candidate, start=first, end=last, fill_rows=fills)
    retained = [row for row in _journal_rows(candidate / "marks.jsonl")
                if row.get("market") in receipt["retained_markets"]]
    endpoints = curve_builder.validate_carried_endpoint_accounting(retained, positions,
        state=prior, fill_rows=fills, start=first, end=last, collect_failures=True)
    if endpoints.get("failures"):
        raise ValueError("adopted retained endpoint accounting differs from original fill prefix")
    first_bar = bool(proof.get("provisional_opening_first_bar_valuation_targets"))
    policy = curve_builder.opening_assumption_policy(first_bar)
    cost_proof = (curves.get("strategy") or {}).get(policy["key"])
    scope = proof.get(policy["scope"]) or []
    cost_acceptance = None
    affected = any(row.get(policy["key"]) is not None for row in retained)
    if any(row.get(curve_builder.opening_assumption_policy(not first_bar)["key"]) is not None for row in retained):
        raise ValueError("unbound or mixed retained opening valuation policy")
    if scope or cost_proof or affected:
        if (not cost_proof or scope != cost_proof.get("authorized_targets")
                or any(item[0] not in receipt["retained_markets"] for item in scope)):
            raise ValueError("provisional opening cost scope differs from the additive authorization")
        cost_acceptance = curve_builder.validate_provisional_opening_cost_valuations(
            retained, positions, state=prior, fill_rows=fills, authorization=cost_proof,
            source_rows=list(_journal_rows(before_path)), first_bar=first_bar,
            store=curve_builder.first_bar_valuation_store(curves) if first_bar else None)
        if first_bar:
            expected_references = [
                {"market": row["market"], "session_date": row["session_date"],
                 "symbol": cohort["symbol"], "source_minute": cohort["reference_bar"]["source_minute"]}
                for row in cost_proof["applied"] for cohort in row["cohorts"]]
            if proof.get("provisional_opening_first_bar_references") != expected_references:
                raise ValueError("public first-bar source clocks differ from independently verified references")
    return {"contract": CURVE_RECOVERY, "cash_rows": len(expected_rows),
            "opening_projection_repairs": inspected["opening_projection_repairs"],
            "adopted_endpoint_accounting": endpoints,
            **({policy["key"]: cost_acceptance} if cost_acceptance else {}),
            "financial_journals_changed": False, "missing_signal_execution_replayed": False}


def validate_addition(candidate: Path, *, expected_markets: set[str]) -> dict:
    """Validate both evidence chains plus the complete combined minute grid."""
    from scripts.promote_tw_day_trade_replay import _validate_rebuild, _validate_minute_curve_coverage, _promotion_input_hashes
    receipt = _object(candidate / "rebuild_receipt.json")
    input_hashes = _promotion_input_hashes(candidate)
    if receipt.get("contract") != CONTRACT or receipt.get("simulation_only") is not True or receipt.get("production_order_possible") is not False:
        raise ValueError("invalid additive paper contract")
    added = receipt["added_market"]
    retained = set(receipt["retained_markets"])
    if added in retained or expected_markets != retained | {added}:
        raise ValueError("additive mode sets do not match exactly")
    state = _object(candidate / "state.json")
    prior = _object(evidence_path(candidate, receipt["prior_state"]))
    _flat(state)
    if set(state["modes"]) != expected_markets:
        raise ValueError("candidate mode set changed")
    for market in retained:
        if state["modes"][market] != prior["modes"][market]:
            raise ValueError(f"existing account was altered: {market}")
    for name, proof in receipt["retained_execution_prefixes"].items():
        if name not in LEDGERS or sha(candidate / name, proof["bytes"]) != proof["sha256"]:
            raise ValueError(f"retained decision/execution bytes changed: {name}")
    component = evidence_path(candidate, receipt["new_mode_component"])
    validate_completed_replay_entry_finalization(component)
    if _object(component / "rebuild_receipt.json").get("contract") == CONTRACT:
        raise ValueError("an addition component must be an independent replay, not another union")
    segments = receipt.get("appended_execution_segments") or {}
    if set(segments) != {name for name in LEDGERS if (component / name).is_file()}:
        raise ValueError("addition is missing exact new execution segment proofs")
    for name, proof in segments.items():
        prefix = receipt["retained_execution_prefixes"].get(name, {}).get("bytes", 0)
        if (proof["offset"] != prefix or (candidate / name).stat().st_size != prefix + proof["bytes"]
                or (component / name).stat().st_size != proof["bytes"]
                or sha(component / name) != proof["sha256"]
                or sha(candidate / name, proof["bytes"], offset=prefix) != proof["sha256"]):
            raise ValueError(f"new execution segment differs from accepted replay: {name}")
    authorization = receipt.get("provisional_research_publication")
    if authorization is not None:
        expected_authorization = {
            "contract": PROVISIONAL_PUBLICATION,
            "market": added,
            "component_rebuild_receipt_sha256": hashlib.sha256(
                (component / "rebuild_receipt.json").read_bytes()).hexdigest(),
            "explicit_operator_opt_in": True,
            "historical_only": True,
            "source_gaps_may_change_results": True,
            "broker_or_exchange_execution_claimed": False,
        }
        if "research_model" in authorization:
            component_mode = (_object(component / "state.json").get("modes") or {}).get(added)
            if not isinstance(component_mode, Mapping):
                raise ValueError("research component model is missing")
            expected_authorization["research_model"] = _pin_research_model(component_mode)
        if authorization != expected_authorization:
            raise ValueError("invalid provisional research publication authorization")
        from stockagent.live.paper_journal import managed
        if not all(managed(candidate / name) for name in ("orders.jsonl", "fills.jsonl")):
            raise ValueError("provisional research requires both managed execution journals")
        acceptance = _validate_rebuild(component, expected_markets={added},
                                       allow_provisional_research_market=added)
        if (acceptance.get("provisional_research") or {}).get("market") != added:
            raise ValueError("provisional component acceptance did not retain its market and disclosure")
    else:
        acceptance = _validate_rebuild(component, expected_markets={added})
    if state["modes"][added] != _object(component / "state.json")["modes"][added]:
        raise ValueError("new mode differs from independently validated component")
    added_rows = [row for row in _journal_rows(candidate / "marks.jsonl") if row.get("market") == added]
    component_rows = list(_journal_rows(component / "marks.jsonl"))
    key = lambda row: (str(row.get("market")), str(row.get("minute")))
    if sorted(added_rows, key=key) != sorted(component_rows, key=key):
        raise ValueError("new mode minute values differ from independently validated component")
    curve_recovery = validate_retained_curve_recovery(candidate, receipt)
    dates = acceptance["session_dates"]
    failures = []
    minute = _validate_minute_curve_coverage(candidate, completed_session_dates=dates,
                                            expected_markets=expected_markets, failures=failures)
    if failures:
        raise ValueError("additive minute acceptance failed: " + "; ".join(failures[:15]))
    aborted = (validate_aborted_activation(candidate) if receipt.get("aborted_activation") else None)
    if input_hashes != _promotion_input_hashes(candidate):
        raise ValueError("additive ledger changed during validation")
    return {**acceptance, "contract": CONTRACT, "added_market": added, "mode_set": sorted(expected_markets),
            "retained_markets": sorted(retained), "old_signal_pins_and_executions_unchanged": True,
            "ending_equity_twd": {m: float(v["total_equity_twd"]) for m,v in state["modes"].items()},
            "final_open_positions": {m: int(v.get("open_position_count") or 0) for m,v in state["modes"].items()},
            "minute_curve_validation": minute, "state_sha256": sha(candidate / "state.json"),
            "rebuild_receipt_sha256": sha(candidate / "rebuild_receipt.json"),
            "promotion_input_hashes": input_hashes,
            "aborted_activation_recovery": aborted,
            "retained_curve_recovery": curve_recovery,
            "new_component_acceptance": acceptance}
