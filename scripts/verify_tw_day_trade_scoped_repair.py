#!/usr/bin/env python3
"""Verify one append-only paper repair independently of its live writer."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import requests
import polars as pl

from scripts import repair_tw_day_trade_flat_opening as repair
from scripts import rebuild_tw_day_trade_open_price_replay as replay
from stockagent.live.paper_journal import open_paper_journal


def prefix_sha256(path: Path, length: int) -> str:
    digest = hashlib.sha256()
    with open_paper_journal(path) as handle:
        remaining = length
        while remaining:
            block = handle.read(min(8 * 1024 * 1024, remaining))
            if not block:
                raise RuntimeError(f"journal was truncated: {path}")
            digest.update(block)
            remaining -= len(block)
    return digest.hexdigest()


def suffix_rows(path: Path, offset: int):
    with open_paper_journal(path) as handle:
        handle.seek(offset)
        for line in handle:
            if line.endswith(b"\n"):
                yield json.loads(line)


def baseline(root: Path) -> dict:
    manifest = repair._object(root / "ledger_repair_manifest.json")
    if manifest["status"] != "candidate_validated_not_applied":
        raise RuntimeError("baseline requires an unapplied candidate")
    repair._assert_source_pins(manifest["source_pins"])
    result = {"status": "captured_before_apply", "market": manifest["market"],
              "recorded_at": datetime.now(repair.TAIPEI).isoformat(), "journals": {}}
    live = Path(manifest["live_dir"])
    for name in ("signals.jsonl", "orders.jsonl", "fills.jsonl", "marks.jsonl", "events.jsonl"):
        path = live / name
        stat = path.stat()
        digest = prefix_sha256(path, stat.st_size)
        after = path.stat()
        if (after.st_dev, after.st_ino) != (stat.st_dev, stat.st_ino) or after.st_size < stat.st_size:
            raise RuntimeError(f"journal identity changed during baseline: {path}")
        result["journals"][name] = {"prefix_bytes": stat.st_size, "sha256": digest,
                                    "device": stat.st_dev, "inode": stat.st_ino}
    repair._atomic_json(root / "journal_prefix_baseline.json", result)
    return result


def _api(base: str, path: str, params: dict) -> dict:
    response = requests.get(f"{base.rstrip('/')}/{path}", params=params, timeout=(10, 40))
    response.raise_for_status()
    value = response.json()
    if value.get("simulation_only") is not True or value.get("production_order_possible") is not False:
        raise RuntimeError(f"paper/read-only API boundary missing: {base}/{path}")
    return value


def source_no_trade_minutes(files: list[dict], day: str) -> dict[str, set[str]]:
    """Explicit zero-volume source rows are not missing price observations."""
    result: dict[str, set[str]] = {}
    for source in files:
        frame = pl.read_parquet(source["path"])
        if not {"ts", "symbol", "volume_shares", "Amount"} <= set(frame.columns):
            continue
        for row in frame.filter((pl.col("volume_shares") == 0) & (pl.col("Amount") == 0)).to_dicts():
            stamp = row["ts"]
            if stamp.date().isoformat() == day:
                result.setdefault(stamp.isoformat()[:16], set()).add(str(row["symbol"]))
    return result


def verify_carried_minute(row: dict, *, no_trade_symbols: set[str], held_symbols: set[str] | list[str]) -> None:
    carried = int(row.get("last_trade_carried_position_count") or 0)
    if (row.get("historical_minute_replay") is not True
            or row.get("minute_valuation_contract") != "right_labelled_historical_last_trade_mark_v1"
            or row.get("missing_price_position_count") != 0
            or carried <= 0 or carried != int(row.get("stale_position_count") or 0)
            or carried != sum(symbol in no_trade_symbols for symbol in held_symbols)):
        raise RuntimeError("carried valuation lacks explicit zero-trade source evidence")


def verify_minute_source_coverage(mode: dict, marks: list[dict], *, market: str, day: str,
                                  source_files: list[dict]) -> dict:
    selected = [row for row in marks if row.get("market") == market and row.get("session_date") == day]
    by_minute = {str(row["minute"])[:16]: row for row in selected}
    start = datetime.fromisoformat(f"{day}T09:01:00+08:00")
    expected = {(start + timedelta(minutes=i)).isoformat()[:16] for i in range(270)}
    if set(by_minute) != expected or len(selected) != 270:
        raise RuntimeError("exactly 270 source-backed marks, without duplicate minutes, required")
    no_trades = source_no_trade_minutes(source_files, day)
    carried = []
    for minute, row in by_minute.items():
        if int(row.get("missing_price_position_count") or 0):
            raise RuntimeError(f"minute has an unsourced held position: {minute}")
        if row.get("valuation_stale"):
            held = [str(position["symbol"]) for position in mode["positions"].values()
                    if str(position["entry_at"])[:16] <= minute < str(position.get("exit_at") or f"{day}T13:31")[:16]]
            verify_carried_minute(row, no_trade_symbols=no_trades.get(minute, set()), held_symbols=held)
            carried.append(minute)
    return {"minute_points": len(by_minute), "duplicate_minute_points": 0,
            "explicit_zero_trade_carried_minutes": sorted(carried), "unsourced_held_price_minutes": 0}


def verify_api_positions(rows: list[dict], expected: list[dict], signal_id: str | set[str]) -> dict:
    """Compare financial meaning even when the public DTO redacts private IDs."""
    fields = (
        "market", "session_date", "symbol", "side", "target_weight",
        "requested_shares", "filled_shares", "signed_shares", "entry_at",
        "entry_price", "exit_at", "exit_price", "realized_net_pnl_twd",
        "total_net_pnl_twd", "simulation_replay",
    )
    identity = lambda row: tuple(str(row.get(field) or "") for field in fields)
    signal_ids = {signal_id} if isinstance(signal_id, str) else signal_id
    actual = sorted(rows, key=identity)
    canonical = sorted(expected, key=identity)
    if len(actual) != len(canonical):
        raise RuntimeError("API repaired position count differs")
    for row, source in zip(actual, canonical, strict=True):
        if any(field not in row or row[field] != source.get(field) for field in fields):
            raise RuntimeError("API repaired position economics or provenance differ")
        if row.get("signal_id") is not None and row["signal_id"] not in signal_ids:
            raise RuntimeError("API disclosed a different repaired signal identity")
    return {"position_rows": len(actual),
            "private_signal_ids_redacted": all(row.get("signal_id") is None for row in actual),
            "exact_position_fields_verified": list(fields)}


def verify_published_fill_accounting(mode: dict, fills: list[dict], *, day: str,
                                     plan: dict, bars: dict, prior_realized: float,
                                     terminal_close_sources: list[dict] | tuple = ()) -> dict:
    """Independent quantity/price/fee oracle, not a second execution engine."""
    positions = {key: row for key, row in mode["positions"].items() if row.get("session_date") == day}
    totals = defaultdict(lambda: defaultdict(float))
    volume_used = defaultdict(int)
    selected = {row["signal_id"]: row for row in plan["versions"] if row["effective_entry_at"]}
    source_ids = {position["signal_id"]: (position.get("signal_id") or "").split(":published-minute-repair:")[0]
                  for position in positions.values()}
    terminal_prices = {}
    for source_file in terminal_close_sources:
        path = Path(source_file["path"])
        exchange = {"twse_daily_ohlcv": "twse", "tpex_daily_ohlcv": "tpex"}.get(path.parent.name)
        if exchange is None or repair._sha256(path) != source_file["sha256"]:
            raise RuntimeError("terminal official source identity changed")
        from scripts.settle_tw_day_trade_official_close import official_closes
        prices, _ = official_closes(path.parent.parent, datetime.fromisoformat(day).date(), exchanges=(exchange,))
        if terminal_prices.keys() & prices.keys():
            raise RuntimeError("duplicate terminal official symbol")
        terminal_prices.update(prices)
    entry_shares = exit_shares = 0
    for fill in fills:
        if fill.get("market") != mode["market"] or fill.get("session_date") != day:
            raise RuntimeError("candidate append contains a non-target fill")
        position = positions.get(fill.get("position_id"))
        if position is None or fill.get("simulation_only") is not True or fill.get("simulation_replay") is not True:
            raise RuntimeError("unowned/non-replay fill")
        quantity, price = int(fill["quantity"]), float(fill["price"])
        if quantity <= 0 or quantity != fill["quantity"] or quantity % int(position["lot_size"]):
            raise RuntimeError("invalid share/whole-lot quantity")
        at = datetime.fromisoformat(fill["fill_at"])
        key = at.isoformat(timespec="minutes")
        is_entry = fill.get("purpose") in {"entry", "entry_completion"}
        source = selected[source_ids[position["signal_id"]]]
        if is_entry and (at < datetime.fromisoformat(source["effective_entry_at"])
                         or at.time() >= repair.time(13, 20)):
            raise RuntimeError("entry violates publication/full-minute/exit priority clock")
        terminal = fill.get("purpose") in {"13_30_terminal_ledger_flatten", "official_close_reconciliation",
                                          "13_30_unlimited_close_paper_settlement"}
        if terminal:
            proof = terminal_prices.get(str(position["symbol"])) or {}
            receipt = fill.get("terminal_close_receipt") or {}
            evidence = receipt.get("source_evidence") or {}
            contract = "source_close_full_deliverable_residual_no_capacity_paper_only_v1"
            raw_row = proof.get("official_row") or {}
            try:
                volume = float(str(raw_row.get("成交股數", raw_row.get("成交量", ""))).replace(",", ""))
            except (ValueError, TypeError):
                volume = 0.
            expected_at = datetime.fromisoformat(f"{day}T13:30:00+08:00")
            if (at != expected_at or fill.get("fill_contract") != contract
                    or receipt.get("contract") != contract or receipt.get("broker_fill") is not False
                    or receipt.get("full_quantity_is_user_assumption") is not True
                    or receipt.get("effective_at") != expected_at.isoformat()
                    or proof.get("price") is None or not math.isfinite(volume) or volume <= 0
                    or not math.isclose(price, float(proof["price"]), rel_tol=0, abs_tol=1e-9)
                    or any(evidence.get(key) != value for key, value in {
                        "session_date": day, "price": proof.get("price"),
                        "source": proof.get("source"), "source_sha256": proof.get("source_sha256"),
                        "price_basis": "official_session_close"}.items())):
                raise RuntimeError("terminal fill lacks matching traded official close evidence")
        else:
            bar = (bars.get(str(position["symbol"])) or {}).get(key)
            if bar is None or not float(bar["low"]) <= price <= float(bar["high"]):
                raise RuntimeError(f"fill lacks observed minute price range: {position['symbol']}/{key}")
            if is_entry:
                expected_price = float(bar["vwap"])
                if not math.isclose(price, expected_price, abs_tol=1e-8, rel_tol=1e-12):
                    raise RuntimeError("entry differs from source minute VWAP/Close")
            volume_used[(position["symbol"], key)] += quantity
        side = position["side"]
        rate = float(position[("buy_fee_rate" if side == "long" else "sell_fee_rate") if is_entry
                              else ("sell_fee_rate" if side == "long" else "buy_fee_rate")])
        fee = quantity * price * (rate - float(position.get("commission_rebate_rate") or 0))
        if not math.isclose(float(fill["fee_and_tax_twd"]), fee, rel_tol=1e-10, abs_tol=1e-6):
            raise RuntimeError("fill fee/tax differs from frozen per-security schedule")
        values = totals[fill["position_id"]]
        prefix = "entry" if is_entry else "exit"
        values[prefix + "_shares"] += quantity
        values[prefix + "_notional"] += quantity * price
        values[prefix + "_fees"] += fee
        if is_entry:
            entry_shares += quantity
            tp, sl = float(position["take_profit_price"]), float(position["stop_trigger_price"])
            if not (sl < price < tp if side == "long" else tp < price < sl):
                raise RuntimeError("entry violates original TP/SL price guard")
        else:
            exit_shares += quantity
    for (symbol, minute), quantity in volume_used.items():
        bar = bars[symbol][minute]
        capacity = int(float(bar["volume_shares"]) * .5 // 1000) * 1000
        if quantity > capacity:
            raise RuntimeError(f"shared minute volume consumed twice: {symbol}/{minute}: {quantity}>{capacity}")
    realized = fees = 0.
    for key, position in positions.items():
        values = totals[key]
        quantity = int(position["filled_shares"])
        if (values["entry_shares"] != quantity or values["exit_shares"] != quantity
                or int(position.get("signed_shares") or 0) != 0):
            raise RuntimeError("position entry/exit quantities do not reconcile")
        direction = 1 if position["side"] == "long" else -1
        gross = direction * (values["exit_notional"] - values["entry_notional"])
        fee = values["entry_fees"] + values["exit_fees"]
        net = gross - fee
        for actual, expected in ((position["entry_price"], values["entry_notional"] / quantity),
                (position["realized_gross_pnl_twd"], gross), (position["entry_fee_twd"], values["entry_fees"]),
                (position["realized_exit_fee_twd"], values["exit_fees"]), (position["realized_net_pnl_twd"], net)):
            if not math.isclose(float(actual), expected, rel_tol=1e-9, abs_tol=1e-5):
                raise RuntimeError("position price/fee/PnL differs from independent fill reduction")
        realized += net
        fees += fee
    if entry_shares != exit_shares or not math.isclose(
            float(mode["cumulative_realized_net_pnl_twd"]) - prior_realized, realized,
            rel_tol=1e-9, abs_tol=1e-5):
        raise RuntimeError("daily realized PnL/closed inventory does not reconcile")
    return {"position_cohorts": len(positions), "entry_shares": entry_shares, "exit_shares": exit_shares,
            "net_fees_and_tax_twd": fees, "session_realized_net_pnl_twd": realized,
            "original_tp_sl_guards_verified": True, "shared_minute_capacity_verified": True,
            "publication_and_full_minute_clocks_verified": True}


def verify_applied_source_provenance(manifest: dict) -> dict:
    """After apply, a collector may refresh its mutable official rule alias.

    Never relax candidate/apply source pins. Only a declared eligibility alias
    can differ here, with the original private normalized snapshot still pinned
    byte-for-byte and every dated security/rule value equal to the new response.
    """
    if manifest.get("status") != "applied":
        raise RuntimeError("post-apply provenance verification requires applied status")
    references = {}
    for session in manifest["applied_sessions"]:
        for dataset, proof in session.get("eligibility_source_proof", {}).items():
            if dataset in {"twse_day_trade_eligibility", "tpex_day_trade_eligibility"}:
                references[proof["raw_path"]] = (session["session_date"], dataset, proof)
    fields = ["date", "證券代號", "證券名稱", "暫停現股賣出後現款買進當沖註記"]
    refreshed = []
    exact = 0
    for pin in manifest["source_pins"]:
        if repair._sha256(Path(pin["path"])) == pin["sha256"]:
            exact += 1
            continue
        reference = references.get(pin["path"])
        if reference is None or reference[2]["raw_sha256"] != pin["sha256"]:
            raise RuntimeError(f"repair source changed: {pin['path']}")
        day, dataset, proof = reference
        normalized = Path(proof["normalized_path"])
        if (repair._sha256(normalized) != proof["normalized_sha256"]
                or not any(p["path"] == str(normalized) and p["sha256"] == proof["normalized_sha256"]
                           for p in manifest["source_pins"])):
            raise RuntimeError("original exact-session rule snapshot changed or was not pinned")
        raw = Path(pin["path"]).read_bytes()
        frame, _ = replay._parse_historical_response_content(
            replay.DEFAULT_DATASETS[dataset], datetime.fromisoformat(day).date(), raw,
            replay._retained_rule_response_kind(dataset, raw))
        original = pl.read_parquet(normalized)
        if (not set(fields) <= set(frame.columns) or not set(fields) <= set(original.columns)
                or frame.height != proof["rows"] or original.height != proof["rows"]
                or not original.select(fields).sort(fields).equals(frame.select(fields).sort(fields))):
            raise RuntimeError("refreshed official reference changed dated security/rule values")
        refreshed.append({"dataset": dataset, "session_date": day, "mutable_raw_path": pin["path"],
                          "original_raw_sha256_at_apply": pin["sha256"],
                          "current_raw_sha256": hashlib.sha256(raw).hexdigest(),
                          "original_normalized_snapshot": str(normalized),
                          "original_normalized_sha256": proof["normalized_sha256"],
                          "normalized_rows": frame.height, "exact_rule_fields_verified": fields,
                          "original_raw_bytes_unchanged_claim": False,
                          "dated_rule_values_unchanged": True})
    return {"source_pins_total": len(manifest["source_pins"]), "byte_identical_source_pins": exact,
            "mutable_official_reference_refreshes": refreshed}


def verify(root: Path, api_bases: list[str]) -> dict:
    manifest = repair._object(root / "ledger_repair_manifest.json")
    if manifest["status"] != "applied" or manifest.get("non_target_modes_unchanged") is not True:
        raise RuntimeError("scoped append has not been accepted")
    source_proof = verify_applied_source_provenance(manifest)
    original = repair._object(root / "journal_prefix_baseline.json")
    live, market = Path(manifest["live_dir"]), manifest["market"]
    for name, proof in original["journals"].items():
        path = live / name
        stat = path.stat()
        identity_matches = (stat.st_dev, stat.st_ino) == (proof["device"], proof["inode"])
        if not identity_matches and manifest.get("atomic_recovery"):
            recovery = manifest["atomic_recovery"]
            failed_path = Path(recovery["interrupted_manifest_path"])
            if repair._sha256(failed_path) != recovery["interrupted_manifest_sha256"]:
                raise RuntimeError("interrupted recovery manifest changed")
            rollback = Path(recovery["rollback_directory"]).resolve(strict=True)
            if rollback == live.resolve() or rollback.parent != live.resolve().parent:
                raise RuntimeError("recovery rollback is not a distinct same-parent directory")
            old_path = rollback / name
            old = old_path.stat()
            identity_matches = ((old.st_dev, old.st_ino) == (proof["device"], proof["inode"])
                and prefix_sha256(old_path, proof["prefix_bytes"]) == proof["sha256"])
        if (not identity_matches or prefix_sha256(path, proof["prefix_bytes"]) != proof["sha256"]):
            raise RuntimeError(f"original journal prefix changed: {name}")
    state = repair._object(live / "state.json")
    mode = state["modes"][market]
    if repair._outcome(mode) != manifest["candidate_final_outcome"]:
        raise RuntimeError("live economics differ from independently validated candidate")
    if mode.get("ledger_state_divergence") or mode.get("open_position_count"):
        raise RuntimeError("target is divergent or not flat")
    proofs = []
    marks = list(suffix_rows(live / "marks.jsonl", manifest["journal_prefix_bytes"]["marks.jsonl"]))
    fills = list(suffix_rows(live / "fills.jsonl", manifest["journal_prefix_bytes"]["fills.jsonl"]))
    for item in manifest["applied_sessions"]:
        day, signal_id = item["session_date"], item["signal_id"]
        selected = [row for row in fills if row.get("market") == market and row.get("session_date") == day]
        entry = sum(int(row["quantity"]) for row in selected if row.get("purpose") in {"entry", "entry_completion"})
        exit_quantity = sum(int(row["quantity"]) for row in selected if row.get("purpose") not in {"entry", "entry_completion"})
        published = manifest.get("published_revisions") is True
        if (not published and entry != item["entry_requested_shares"]) or exit_quantity != entry:
            raise RuntimeError("entry/exit journal quantities do not balance")
        by_minute = {str(row["minute"])[:16]: row for row in marks
                     if row.get("market") == market and row.get("session_date") == day}
        start = datetime.fromisoformat(f"{day}T09:01:00+08:00")
        expected = {(start+timedelta(minutes=i)).isoformat()[:16] for i in range(270)}
        if set(by_minute) != expected:
            raise RuntimeError("270 exact source-backed minute marks required")
        source = next(source for source in manifest["sessions"] if source["session_date"] == day)
        coverage = verify_minute_source_coverage(mode, marks, market=market, day=day,
            source_files=source["minute_source_after"]["source_files"])
        carried_minutes = coverage["explicit_zero_trade_carried_minutes"]
        if published and coverage != manifest["candidate_minute_source_coverage"]:
            raise RuntimeError("applied minute source coverage differs from validated candidate")
        endpoints = []
        expected_positions = [position for position in mode["positions"].values()
                              if position.get("session_date") == day and (published or position.get("signal_id") == signal_id)]
        signal_ids = {position["signal_id"] for position in expected_positions} if published else signal_id
        accounting = {}
        if published:
            bars, _ = replay._minute_bar_rows(tuple(Path(value) for value in manifest["minute_roots"]),
                trading_date=datetime.fromisoformat(day).date(), symbols={position["symbol"] for position in expected_positions})
            accounting = verify_published_fill_accounting(mode, selected, day=day,
                plan=manifest["published_revision_plan"], bars=bars,
                prior_realized=float(manifest["anchor"]["cumulative_realized_net_pnl_twd"]),
                terminal_close_sources=(item.get("closing_proof") or {}).get("terminal_close_sources", []))
            if accounting != manifest["candidate_fill_accounting"]:
                raise RuntimeError("applied fill accounting differs from validated candidate")
        for base in api_bases:
            position_rows = []
            offset = 0
            while True:
                positions = _api(base, "positions", {"mode": market, "date": day, "limit": 200, "offset": offset})
                page = positions["rows"]
                position_rows.extend(page)
                if not positions.get("has_more"):
                    break
                if not page or len(position_rows) > len(expected_positions):
                    raise RuntimeError("position pagination did not make bounded progress")
                offset += len(page)
            position_proof = verify_api_positions(position_rows, expected_positions, signal_ids)
            history = _api(base, "history", {"range": "all", "resolution": "1m", "start_date": day, "end_date": day})
            series = next(row for row in history["minute_series"] if row["series_id"] == market)
            points = series["points"]
            if (len(points) != 270 or points[0][0] != int(start.timestamp() // 60)
                    or points[-1][0]-points[0][0] != 269 or history.get("downsampled") is True):
                raise RuntimeError(f"API does not expose the complete 270-minute curve: {base}")
            endpoints.append({"base": base, **position_proof, "minute_points": len(points),
                              "first_point": points[0], "last_point": points[-1]})
        proofs.append({"session_date": day, "entry_shares": entry, "exit_shares": exit_quantity,
                       "minute_points": len(by_minute), "explicit_zero_trade_carried_minutes": sorted(carried_minutes),
                       "unsourced_held_price_minutes": 0, "api_evidence": endpoints,
                       **({"progressive_fill_accounting": accounting} if published else {})})
    unit = subprocess.run(["systemctl", "show", repair.SERVICE, "-p", "ActiveState", "-p", "InvocationID", "-p", "MainPID"],
                          check=True, text=True, capture_output=True).stdout
    if "ActiveState=active" not in unit:
        raise RuntimeError("native paper owner did not recover")
    result = {"status": "accepted", "market": market, "recorded_at": datetime.now(repair.TAIPEI).isoformat(),
              "original_journal_prefixes_sha256_verified": True, "source_provenance": source_proof,
              "original_weights_unchanged": manifest.get("preserve_original_signal") is True or manifest.get("published_revisions") is True,
              "non_target_modes_unchanged_during_append": True, "candidate_live_economics_equal": True,
              "simulation_only": True, "production_order_possible": False, "sessions": proofs,
              "total_equity_twd": mode["total_equity_twd"], "native_owner": unit}
    repair._atomic_json(root / "acceptance.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("baseline", "verify"))
    parser.add_argument("--operation-root", type=Path, required=True)
    parser.add_argument("--api-base", action="append", default=[])
    args = parser.parse_args()
    result = baseline(args.operation_root) if args.action == "baseline" else verify(args.operation_root, args.api_base)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
