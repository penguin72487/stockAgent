"""Explicit offline recovery of a provably unwritten paper signal transaction.

An unmatched start is NOT automatically recoverable. The operator must hold the
paper writers, scan all three financial journals and reconcile FIFO cohorts.
The append-only abort event references that durable proof; startup only verifies
its small receipt, not another full historical scan on the opening hot path.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path

CONTRACT = "verified_unwritten_paper_signal_abort_v1"
EVENT = "signal_commit_aborted_unwritten"
FINANCIAL_JOURNALS = ("signals.jsonl", "orders.jsonl", "fills.jsonl")


def economic_fingerprint(mode: dict) -> str:
    # Exclude liveness/quotes, never quantities, cash, targets or commit identity.
    fields = ("signal_id", "processed_signal_ids", "entry_completed_at", "session_date",
              "initial_capital_twd", "cumulative_realized_net_pnl_twd", "cumulative_carry_cost_twd",
              "cumulative_corporate_action_net_twd", "corporate_action_receivable_twd",
              "corporate_action_payable_twd", "positions", "pending_entry_orders")
    book = {k: mode.get(k) for k in fields}
    book["ledger_state_divergence"] = {
        k: (mode.get("ledger_state_divergence") or {}).get(k)
        for k in ("kind", "signal_id", "session_date")}
    return hashlib.sha256(json.dumps(book, sort_keys=True,
                                    allow_nan=False).encode()).hexdigest()


def _identity(stat):
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def _close(a, b, label):
    if not (math.isfinite(float(a)) and math.isfinite(float(b))
            and math.isclose(float(a), float(b), rel_tol=1e-10, abs_tol=1e-6)):
        raise ValueError(f"unwritten recovery accounting mismatch: {label}")


def build_unwritten_commit_proof(root: Path, market: str, day: str) -> dict:
    """Read-only, full evidence scan. Caller must recheck under both writer locks."""
    from stockagent.live.paper_mode_snapshot import read_paper_state
    from stockagent.live.paper_journal import managed
    from stockagent.live.tw_day_trade_simulation import _event_temporal_key

    state = read_paper_state(root / "state.json")
    if state.get("simulation_only") is not True or state.get("production_order_possible") is not False:
        raise ValueError("unwritten recovery requires an exclusively paper ledger")
    mode = state["modes"][market]
    divergence = mode.get("ledger_state_divergence") or {}
    orphan = divergence.get("signal_id")
    previous = mode.get("signal_id")
    if (divergence.get("kind") != "signal_commit_started_without_registration"
            or not orphan or not previous or orphan == previous
            or divergence.get("session_date") != day or mode.get("session_date") != day
            or not mode.get("entry_completed_at")):
        raise ValueError("only an unmatched start with a prior accepted state is recoverable")
    events_path = root / "events.jsonl"
    event_stat = events_path.stat()
    starts, registrations = [], []
    for sequence, line in enumerate(events_path.read_text().splitlines()):
        row = json.loads(line)
        if row.get("market") != market:
            continue
        ordered = (_event_temporal_key(row, sequence=sequence), row.get("signal_id"), row)
        if row.get("event") == "signal_commit_started":
            starts.append(ordered)
        elif row.get("event") == "signal_registered":
            registrations.append(ordered)
        if row.get("signal_id") == orphan and row.get("event") != "signal_commit_started":
            raise ValueError("orphan already has durable completion or recovery evidence")
    if (not starts or not registrations or max(starts, key=lambda r: r[0])[1] != orphan
            or max(registrations, key=lambda r: r[0])[1] != previous):
        raise ValueError("unmatched start or prior accepted registration changed")
    positions = mode.get("positions") or {}
    flows = {pid: {"entry": [], "exit": []} for pid in positions}
    proofs = {}
    snapshots = {}
    realized_rows = []
    needle = orphan.encode()
    previous_seen = False
    for name in FINANCIAL_JOURNALS:
        path = root / name
        # Immutable journal segments have a separate transaction/recovery owner.
        if managed(path):
            raise ValueError("segmented journal requires its own recovery protocol")
        before = path.stat()
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            if name == "fills.jsonl":
                for line in stream:
                    digest.update(line)
                    if needle in line:
                        raise ValueError(f"orphan has durable financial rows: {name}")
                    if market.encode() not in line:
                        continue
                    row = json.loads(line)
                    if row.get("market") != market:
                        continue
                    if "net_pnl_twd" in row:
                        realized_rows.append(row["net_pnl_twd"])
                    if row.get("session_date") != day:
                        continue
                    pid = row.get("position_id")
                    if pid not in flows:
                        raise ValueError("current-session fill has no corresponding state cohort")
                    quantity = row.get("quantity")
                    if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
                        raise ValueError("invalid durable fill quantity")
                    if row.get("purpose") == "entry":
                        flows[pid]["entry"].append(row)
                    elif "net_pnl_twd" in row and "entry_fee_allocated_twd" in row:
                        flows[pid]["exit"].append(row)
                    else:
                        raise ValueError("unknown fill accounting contract")
            else:
                tail = b""
                while chunk := stream.read(8 * 1024 * 1024):
                    digest.update(chunk)
                    combined = tail + chunk
                    if needle in combined:
                        raise ValueError(f"orphan has durable financial rows: {name}")
                    if name == "signals.jsonl" and previous.encode() in combined:
                        previous_seen = True
                    tail = combined[-max(len(needle), len(previous.encode())):]
            after = os.fstat(stream.fileno())
            if after.st_size:
                stream.seek(-1, os.SEEK_END)
                if stream.read(1) != b"\n":
                    raise ValueError(f"partial financial journal tail: {name}")
        if _identity(before) != _identity(after) or _identity(after) != _identity(path.stat()):
            raise ValueError("financial journal changed during recovery scan")
        snapshots[path] = after
        proofs[name] = {"size": after.st_size, "sha256": digest.hexdigest(), "orphan_rows": 0}
    if not previous_seen:
        raise ValueError("previous accepted signal is absent from the signal journal")
    journal_realized = math.fsum(realized_rows)
    _close(journal_realized, mode.get("cumulative_realized_net_pnl_twd") or 0,
           "account cumulative realized PnL")
    reconciled = []
    for pid, p in positions.items():
        if p.get("session_date") != day:
            raise ValueError("unwritten recovery cannot infer historical/corporate-action inventory")
        entries, exits = flows[pid]["entry"], flows[pid]["exit"]
        if not entries:
            raise ValueError("state cohort has no durable entry fill")
        acquired = sum(r["quantity"] for r in entries)
        disposed = sum(r["quantity"] for r in exits)
        direction = 1 if p.get("side") == "long" else -1 if p.get("side") == "short" else 0
        if not direction or acquired - disposed != direction * int(p.get("signed_shares") or 0):
            raise ValueError("unwritten recovery inventory differs from durable fills")
        _close(math.fsum(r["price"] * r["quantity"] for r in entries) / acquired,
               p["entry_price"], f"{pid}: entry price")
        _close(math.fsum(r["fee_and_tax_twd"] for r in entries)
               - math.fsum(r["entry_fee_allocated_twd"] for r in exits),
               p.get("remaining_entry_fee_twd") or 0, f"{pid}: remaining entry fees")
        _close(math.fsum(r["net_pnl_twd"] for r in exits),
               p.get("realized_net_pnl_twd") or 0, f"{pid}: realized PnL")
        for row in exits:
            _close(direction * row["quantity"] * (row["price"] - float(
                p.get("inventory_basis_price", p["entry_price"]))),
                row["gross_pnl_twd"], f"{pid}: exit price to gross PnL")
            _close(row["gross_pnl_twd"] - row["entry_fee_allocated_twd"] - row["fee_and_tax_twd"],
                   row["net_pnl_twd"], f"{pid}: exit accounting")
        reconciled.append({"position_id": pid, "entry_quantity": acquired, "exit_quantity": disposed,
                           "signed_shares": p["signed_shares"], "fees_and_pnl_verified": True})
    if (_identity(event_stat) != _identity(events_path.stat())
            or any(_identity(stat) != _identity(path.stat()) for path, stat in snapshots.items())
            or economic_fingerprint(read_paper_state(root / "state.json")["modes"][market])
            != economic_fingerprint(mode)):
        raise ValueError("paper book changed during recovery scan")
    return {"contract": CONTRACT, "market": market, "session_date": day,
            "signal_id": orphan, "previous_signal_id": previous,
            "book_fingerprint": economic_fingerprint(mode), "journals": proofs,
            "positions": reconciled, "cumulative_realized_net_pnl_twd": journal_realized,
            "simulation_only": True, "broker_orders": False}


def abort_verified_unwritten(engine, proof: dict, *, recorded_at) -> dict:
    """Apply ONLY inside a verified isolated clone, never clear a live marker."""
    from downloader.artifact_io import atomic_write_json

    mode = engine.state["modes"][proof["market"]]
    if economic_fingerprint(mode) != proof["book_fingerprint"]:
        raise ValueError("unwritten recovery book changed after proof")
    payload = {**proof, "recorded_at": recorded_at.isoformat(),
               "original_divergence": dict(mode["ledger_state_divergence"])}
    key = hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()
    relative = Path("signal_commit_recovery_receipts") / f"{key}.json"
    path = engine.state_dir / relative
    atomic_write_json(path, payload, durable=True)
    event = {"market": proof["market"], "signal_id": proof["signal_id"],
             "session_date": proof["session_date"], "previous_signal_id": proof["previous_signal_id"],
             "receipt_path": str(relative), "receipt_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
             "contract": CONTRACT, "broker_orders": False, "simulation_only": True}
    if not accepted_abort(engine.state_dir, event, signal_id=proof["signal_id"],
                          previous_signal_id=proof["previous_signal_id"], market=proof["market"],
                          session_date=proof["session_date"]):
        raise ValueError("unwritten recovery receipt failed independent readback")
    engine._event(EVENT, recorded_at=recorded_at, **event)
    mode.setdefault("signal_commit_recoveries", []).append(event | {"recorded_at": recorded_at.isoformat()})
    mode.pop("ledger_state_divergence")
    mode.pop("readiness_error", None)
    mode["engine_status"] = "offline_reconciled_prior_signal"
    return event


def accepted_abort(root: Path, event: dict, *, signal_id: str, previous_signal_id: str,
                   market: str, session_date: str) -> bool:
    """Validate the explicit operator's compact proof without replaying history."""
    try:
        relative = Path(event["receipt_path"])
        if (relative.parent != Path("signal_commit_recovery_receipts")
                or relative.suffix != ".json"):
            return False
        raw = (root / relative).read_bytes()
        if hashlib.sha256(raw).hexdigest() != event["receipt_sha256"]:
            return False
        proof = json.loads(raw)
        expected = {"contract": CONTRACT, "market": market, "session_date": session_date,
                    "signal_id": signal_id, "previous_signal_id": previous_signal_id,
                    "simulation_only": True, "broker_orders": False}
        if any(proof.get(k) != v for k, v in expected.items()):
            return False
        # This is the compact, explicitly committed recovery authority, just
        # like signal_registered. Full prefix verification belongs to the
        # offline repair/restart audit, not a repeated multi-GB startup scan.
        return all(proof["journals"][name]["orphan_rows"] == 0
                   and len(proof["journals"][name]["sha256"]) == 64
                   and type(proof["journals"][name]["size"]) is int
                   and proof["journals"][name]["size"] > 0
                   for name in FINANCIAL_JOURNALS)
    except (KeyError, TypeError, ValueError, OSError):
        return False


def export_abort_receipts(root: Path, destination: Path) -> None:
    """Carry exact referenced recovery evidence into a standalone restart audit."""
    from downloader.artifact_io import atomic_write_bytes
    from stockagent.live.paper_journal import open_paper_journal

    events = root / "events.jsonl"
    if not events.exists():
        return
    with open_paper_journal(events, "r") as stream:
        for line in stream:
            event = json.loads(line)
            if event.get("event") != EVENT:
                continue
            relative = Path(event["receipt_path"])
            if relative.parent != Path("signal_commit_recovery_receipts") or relative.suffix != ".json":
                raise ValueError("invalid recovery evidence path")
            raw = (root / relative).read_bytes()
            if hashlib.sha256(raw).hexdigest() != event.get("receipt_sha256"):
                raise ValueError("changed recovery evidence")
            atomic_write_bytes(destination / relative, raw, durable=True)
