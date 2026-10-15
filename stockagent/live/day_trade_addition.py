"""Add one isolated replay to the canonical paper ledger without replacing modes.

The prior account states and execution/decision byte streams are retained;
minute valuation is separately rebuilt from those unchanged executions. The
promoter uses the ordinary same-filesystem exchange and writer locks.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import shutil

from downloader.artifact_io import atomic_write_json

CONTRACT = "additive_independent_paper_mode_v1"
LEDGERS = ("signals.jsonl", "fills.jsonl", "orders.jsonl", "events.jsonl", "latency.jsonl")
ABORTED_ACTIVATION = "aborted_unfunded_paper_addition_activation_v1"


def sha(path: Path, count: int | None = None, *, offset: int = 0) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
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
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected object: {path}")
    return value


def evidence_path(candidate: Path, relative: str) -> Path:
    path = (candidate / relative).resolve(strict=True)
    if Path(relative).is_absolute() or not path.is_relative_to(candidate.resolve()) or path == candidate.resolve():
        raise ValueError("addition evidence escaped candidate")
    return path


def _flat(state):
    for market, mode in state["modes"].items():
        if int(mode.get("open_position_count") or 0) or any(
                int(p.get("signed_shares") or 0) for p in (mode.get("positions") or {}).values()):
            raise ValueError(f"cannot add mode while existing inventory is open: {market}")


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
        with source.open("rb") as f:
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
    relative = "addition_evidence/aborted_activation"
    root = candidate / relative
    root.mkdir(exist_ok=False)
    atomic_write_json(root / "state.json", proof.pop("state"))
    for name, stream in proof["streams"].items():
        with (live / name).open("rb") as incoming, (root / name).open("wb") as out:
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


def merge_addition(live: Path, new_replay: Path, candidate: Path, *, added_market: str,
                   aborted_activation_candidate: Path | None = None) -> dict:
    """Stage an isolated union. Never write to either input directory."""
    old_state = _object(live / "state.json")
    new_state = _object(new_replay / "state.json")
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
    if abort is not None:
        # Only this isolated copy loses the aborted tail. The original remains
        # unchanged, and the tail is separately archived before promotion.
        for name, proof in source_proofs.items():
            with (candidate / name).open("r+b") as stream:
                stream.truncate(proof["bytes"])
        marks = candidate / "marks.jsonl"
        pending = candidate / ".retained_marks.tmp"
        with marks.open("rb") as incoming, pending.open("wb") as out:
            for line in incoming:
                if json.loads(line).get("market") != added_market:
                    out.write(line)
        pending.replace(marks)
    evidence = candidate / "addition_evidence"
    evidence.mkdir()
    atomic_write_json(evidence / "prior_state.json", old_state)
    atomic_write_json(evidence / "prior_rebuild_receipt.json", _object(live / "rebuild_receipt.json"))
    # Keep the independently accepted new replay as a component with its own
    # source/price contracts; never relabel old executions as the new policy.
    component = evidence / "new_mode"
    shutil.copytree(new_replay, component, ignore=shutil.ignore_patterns("*.lock", ".lock"))
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
            with target.open("rb") as f:
                f.seek(-1, 2)
                if f.read(1) != b"\n":
                    raise ValueError(f"retained JSONL lacks a terminal newline: {name}")
        with target.open("ab") as out, source.open("rb") as incoming:
            shutil.copyfileobj(incoming, out, 1024**2)
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
               "new_mode_component": "addition_evidence/new_mode",
               "prior_state": "addition_evidence/prior_state.json",
               "prior_rebuild_receipt": "addition_evidence/prior_rebuild_receipt.json",
               "old_signal_pins_and_executions_unchanged": True,
               "retained_live_comparison": "exact_flat_accounts_except_monotonic_same_day_last_mark_at_v1",
               "sessions": _object(new_replay / "rebuild_receipt.json")["sessions"]}
    atomic_write_json(candidate / "rebuild_receipt.json", receipt)
    if abort is not None:
        preserve_aborted_activation(live, candidate)
        receipt = _object(candidate / "rebuild_receipt.json")
    return receipt


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
    acceptance = _validate_rebuild(component, expected_markets={added})
    if state["modes"][added] != _object(component / "state.json")["modes"][added]:
        raise ValueError("new mode differs from independently validated component")
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
            "new_component_acceptance": acceptance}
