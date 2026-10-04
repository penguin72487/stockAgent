#!/usr/bin/env python3
"""Complete one explicitly authorized paper entry without forging broker fills.

The original order/fill evidence remains append-only.  This tool adds only the
missing paper quantity at the position's already-recorded entry price, records
that the quantity is a user assumption, and updates the materialized account
state.  It never connects to a broker or submits an order.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.promote_tw_day_trade_replay import _acquire_engine_lock, _atomic_json
from stockagent.live.tw_day_trade_simulation import TAIPEI, TwDayTradeSimulationEngine


CONTRACT = "user_authorized_same_recorded_price_full_target_paper_completion_v1"
APPEND_LEDGERS = ("orders.jsonl", "fills.jsonl", "events.jsonl")
MATERIALIZED_FILES = ("state.json", "positions.json", "status.json")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def make_plan(
    root: Path,
    *,
    market: str,
    session_date: date,
    symbol: str,
) -> dict[str, Any]:
    state_path = root / "state.json"
    state_bytes = state_path.read_bytes()
    state = json.loads(state_bytes)
    if state.get("simulation_only") is not True or state.get("production_order_possible") is not False:
        raise ValueError("target is not an exclusively paper ledger")
    mode = (state.get("modes") or {}).get(market)
    if not isinstance(mode, dict):
        raise ValueError(f"unknown paper mode: {market}")
    if str(mode.get("session_date") or "") != session_date.isoformat():
        raise ValueError("mode session does not match requested session")
    if mode.get("ledger_state_divergence"):
        raise ValueError("ledger has unresolved state divergence")
    matches = [
        position
        for position in (mode.get("positions") or {}).values()
        if str(position.get("symbol") or "") == symbol
        and str(position.get("session_date") or "") == session_date.isoformat()
    ]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one current position, found {len(matches)}")
    position = matches[0]
    requested = int(position.get("requested_shares") or 0)
    filled = int(position.get("filled_shares") or 0)
    signed = int(position.get("signed_shares") or 0)
    missing = requested - filled
    price = float(position.get("entry_price") or 0.0)
    if requested <= 0 or filled <= 0 or missing <= 0:
        raise ValueError("position has no positive paper entry gap")
    if abs(signed) != filled or signed == 0:
        raise ValueError("position is already partially exited or quantity is inconsistent")
    if position.get("exit_at") or position.get("last_exit_at"):
        raise ValueError("cannot enlarge an entry after an exit has begun")
    if not math.isfinite(price) or price <= 0:
        raise ValueError("recorded entry price is invalid")
    side = str(position.get("side") or "")
    if side not in {"long", "short"} or (signed > 0) != (side == "long"):
        raise ValueError("position side and signed quantity disagree")
    operation_key = hashlib.sha256(
        f"{market}|{session_date}|{symbol}|{filled}|{requested}|{price}".encode()
    ).hexdigest()[:16]
    return {
        "contract": CONTRACT,
        "operation_id": f"{session_date}:{market}:{symbol}:{operation_key}",
        "root": str(root.resolve()),
        "market": market,
        "session_date": session_date.isoformat(),
        "symbol": symbol,
        "position_id": str(position["position_id"]),
        "signal_id": str(position.get("signal_id") or mode.get("signal_id") or ""),
        "side": side,
        "recorded_entry_at": position.get("entry_at"),
        "recorded_quote_at": position.get("entry_quote_at"),
        "entry_price_source": position.get("entry_price_source"),
        "price": price,
        "previous_filled_shares": filled,
        "requested_shares": requested,
        "completion_shares": missing,
        "state_sha256": _sha256_bytes(state_bytes),
        "simulation_only": True,
        "production_order_possible": False,
        "broker_fill": False,
        "full_quantity_is_user_assumption": True,
    }


def _restore_file(path: Path, payload: bytes | None) -> None:
    if payload is None:
        if path.exists():
            path.unlink()
        return
    temporary = path.with_suffix(path.suffix + ".manual-entry-rollback")
    with temporary.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def apply_plan(root: Path, plan: dict[str, Any], *, recorded_at: datetime) -> dict[str, Any]:
    recorded_at = recorded_at.astimezone(TAIPEI)
    if recorded_at.date().isoformat() != plan["session_date"]:
        raise ValueError("paper completion must be recorded during the same session")
    with _acquire_engine_lock(root):
        current = make_plan(
            root,
            market=plan["market"],
            session_date=date.fromisoformat(plan["session_date"]),
            symbol=plan["symbol"],
        )
        if current != plan:
            raise ValueError("paper ledger changed after planning; create a new plan")

        materialized_before = {
            name: (root / name).read_bytes() if (root / name).exists() else None
            for name in MATERIALIZED_FILES
        }
        append_before = {
            name: {
                "size": (root / name).stat().st_size,
                "sha256": _sha256(root / name),
            }
            for name in APPEND_LEDGERS
        }
        receipt_dir = root / "manual_entry_completion_receipts" / plan["operation_id"].replace(":", "-")
        receipt_dir.mkdir(parents=True, exist_ok=False)
        for name, payload in materialized_before.items():
            if payload is not None:
                target = receipt_dir / f"before-{name}"
                target.write_bytes(payload)
        _atomic_json(receipt_dir / "plan.json", plan)

        try:
            engine = TwDayTradeSimulationEngine(root)
            mode = engine.state["modes"][plan["market"]]
            position = mode["positions"][plan["position_id"]]
            quantity = int(plan["completion_shares"])
            old_filled = int(plan["previous_filled_shares"])
            new_filled = int(plan["requested_shares"])
            price = float(plan["price"])
            fee_rate = float(
                position["buy_fee_rate"]
                if plan["side"] == "long"
                else position["sell_fee_rate"]
            )
            rebate_rate = float(position.get("commission_rebate_rate") or 0.0)
            gross_fee = quantity * price * fee_rate
            rebate = quantity * price * rebate_rate
            net_fee = gross_fee - rebate
            effective_at = str(plan["recorded_entry_at"] or recorded_at.isoformat(timespec="seconds"))
            order_id = f"{plan['position_id']}:manual_entry_completion:{plan['operation_id'].split(':')[-1]}"
            disclosure = {
                "contract": CONTRACT,
                "recorded_at": recorded_at.isoformat(timespec="seconds"),
                "effective_at": effective_at,
                "original_quote_at": plan["recorded_quote_at"],
                "entry_price_source": plan["entry_price_source"],
                "same_price_as_original_entry": True,
                "broker_fill": False,
                "exchange_fill": False,
                "exchange_depth_claim": False,
                "full_quantity_is_user_assumption": True,
                "previous_filled_shares": old_filled,
                "completion_shares": quantity,
                "completed_filled_shares": new_filled,
            }

            position.update(
                filled_shares=new_filled,
                executable_order_shares=new_filled,
                signed_shares=new_filled if plan["side"] == "long" else -new_filled,
                target_unsubmitted_shares=0,
                paper_market_fill=True,
                manual_entry_completion=disclosure,
            )
            for key, increment in (
                ("entry_fee_twd", net_fee),
                ("remaining_entry_fee_twd", net_fee),
                ("entry_gross_fee_and_tax_twd", gross_fee),
                ("entry_commission_rebate_accrued_twd", rebate),
            ):
                position[key] = float(position.get(key) or 0.0) + increment

            pending = (mode.get("pending_entry_orders") or {}).get(plan["symbol"])
            if isinstance(pending, dict):
                pending.update(remaining_shares=0, status="filled_by_manual_paper_completion")
            mode["entry_filled_shares"] = int(mode.get("entry_filled_shares") or 0) + quantity
            mode["entry_unfilled_shares"] = max(
                0, int(mode.get("entry_unfilled_shares") or 0) - quantity
            )
            mode["pending_entry_shares"] = sum(
                int(order.get("remaining_shares") or 0)
                for order in (mode.get("pending_entry_orders") or {}).values()
                if order.get("status") == "working"
            )
            mode["entry_fill_count"] = int(mode.get("entry_fill_count") or 0) + 1
            mode["entry_paper_market_fill_count"] = int(
                mode.get("entry_paper_market_fill_count") or 0
            ) + 1
            mode["entry_fill_outcome"] = (
                "filled" if int(mode["entry_unfilled_shares"]) == 0 else "partial"
            )
            mode["cumulative_commission_rebate_accrued_twd"] = float(
                mode.get("cumulative_commission_rebate_accrued_twd") or 0.0
            ) + rebate
            mode["manual_entry_completion"] = disclosure

            common = {
                "recorded_at": recorded_at.isoformat(timespec="seconds"),
                "session_date": plan["session_date"],
                "market": plan["market"],
                "signal_id": plan["signal_id"],
                "symbol": plan["symbol"],
                "position_id": plan["position_id"],
                "order_id": order_id,
                "quantity": quantity,
                "simulation_only": True,
                "production_order_possible": False,
                "counterfactual_paper_completion": disclosure,
            }
            engine._order(
                common
                | {
                    "purpose": "entry_completion",
                    "side": "buy" if plan["side"] == "long" else "sell_short",
                    "order_type": "PAPER_MKT_SAME_RECORDED_PRICE",
                    "price": None,
                    "status": "filled_paper_assumption",
                    "filled_quantity": quantity,
                    "unfilled_quantity": 0,
                    "model_requested_quantity": new_filled,
                    "broker_fill": False,
                }
            )
            engine._fill(
                common
                | {
                    "purpose": "entry_completion",
                    "fill_at": effective_at,
                    "quote_at": plan["recorded_quote_at"],
                    "exchange_match_at": None,
                    "price": price,
                    "fee_and_tax_twd": net_fee,
                    "gross_fee_and_tax_twd": gross_fee,
                    "commission_rebate_accrued_twd": rebate,
                    "fill_contract": CONTRACT,
                    "depth_assumption": "user_authorized_full_target_no_exchange_depth_or_fill_claim",
                    "broker_fill": False,
                }
            )
            for purpose, price_key, status_key in (
                ("take_profit", "take_profit_price", "take_profit_order_status"),
                ("stop_loss", "stop_trigger_price", "stop_order_status"),
            ):
                engine._order(
                    common
                    | {
                        "order_id": f"{order_id}:{purpose}:quantity_amendment",
                        "purpose": f"{purpose}_quantity_amendment",
                        "side": "sell" if plan["side"] == "long" else "buy_to_cover",
                        "order_type": "PAPER_QUANTITY_AMENDMENT",
                        "price": position.get(price_key),
                        "quantity": new_filled,
                        "previous_quantity": old_filled,
                        "status": position.get(status_key),
                    }
                )
            engine._event(
                "manual_paper_entry_completion_applied",
                recorded_at=recorded_at,
                operation_id=plan["operation_id"],
                market=plan["market"],
                session_date=plan["session_date"],
                symbol=plan["symbol"],
                position_id=plan["position_id"],
                completion_shares=quantity,
                completed_filled_shares=new_filled,
                price=price,
                broker_fill=False,
                full_quantity_is_user_assumption=True,
            )
            engine._mark_mode(plan["market"], recorded_at, {}, append_history=False)
            engine._persist(recorded_at)
            result = {
                **plan,
                "status": "applied",
                "recorded_at": recorded_at.isoformat(timespec="seconds"),
                "gross_fee_and_tax_twd": gross_fee,
                "commission_rebate_accrued_twd": rebate,
                "fee_and_tax_twd": net_fee,
                "append_ledger_prefixes": append_before,
                "rollback_snapshot_directory": str(receipt_dir),
            }
            _atomic_json(receipt_dir / "receipt.json", result)
            _atomic_json(root / "manual_entry_completion_receipt.json", result)
            return result
        except Exception:
            for name, proof in append_before.items():
                path = root / name
                with path.open("r+b") as handle:
                    handle.truncate(int(proof["size"]))
                    handle.flush()
                    os.fsync(handle.fileno())
            for name, payload in materialized_before.items():
                _restore_file(root / name, payload)
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("artifacts/live/tw_day_trade_simulation"))
    parser.add_argument("--market", required=True)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    plan = make_plan(
        args.state_dir,
        market=args.market,
        session_date=args.date,
        symbol=str(args.symbol).strip(),
    )
    payload = apply_plan(
        args.state_dir,
        plan,
        recorded_at=datetime.now(TAIPEI),
    ) if args.apply else plan
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
