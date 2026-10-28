#!/usr/bin/env python3
"""Pin first-published paper signals and plan missing minute-source queries.

This is source inventory only: it does not build, mutate, or promote a ledger,
and does not collapse later progressive revisions into the opening signal.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
import hashlib
import io
import json
import math
from pathlib import Path
import sys
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.download_shioaji_tw_minute_kbars import minute_receipt_valid
from scripts.run_tw_day_trade_simulation import _mode_specs
from stockagent.live.minute_source_paths import DEFAULT_LOCAL_MINUTE_ROOTS
from stockagent.live.paper_journal import open_paper_journal
from stockagent.live.paper_mode_snapshot import read_paper_state
from stockagent.live.quote_provider import load_local_stock_0901_vwaps
from stockagent.live.signal_artifacts import read_execution_weight_rows

TAIPEI = ZoneInfo("Asia/Taipei")


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _registered_events(state_dir: Path, day: date) -> tuple[dict[str, list[dict]], str]:
    def scan(*, collect: bool) -> tuple[dict[str, list[dict]], str]:
        digest = hashlib.sha256()
        modes: dict[str, list[dict]] = {}
        with open_paper_journal(state_dir / "events.jsonl", "rb") as stream:
            for line in stream:
                digest.update(line)
                if not collect or day.isoformat().encode() not in line:
                    continue
                row = json.loads(line)
                if row.get("event") == "signal_registered" and row.get("session_date") == day.isoformat():
                    market = str(row.get("market") or "")
                    if not market or not row.get("signal_id") or not row.get("recorded_at"):
                        raise ValueError("signal registration has incomplete identity")
                    modes.setdefault(market, []).append({key: row.get(key) for key in (
                        "signal_id", "recorded_at", "source_signal_at", "simulation_replay",
                        "counterfactual_open_replay", "entry_fill_outcome",
                    )})
        return modes, digest.hexdigest()
    modes, digest = scan(collect=True)
    if scan(collect=False)[1] != digest:
        raise RuntimeError("source event journal changed during inspection")
    return modes, digest


def _terminal_empty(symbol: str, day: date, roots: tuple[Path, ...]) -> bool:
    for root in roots:
        receipt = root / "minute_chunks" / symbol / f"{day}_{day}.receipt.json"
        if not receipt.is_file():
            continue
        try:
            payload = json.loads(receipt.read_text(encoding="utf-8"))
            if (payload.get("status") == "empty" and payload.get("query_performed") is True
                    and minute_receipt_valid(receipt, symbol=symbol, start=day, end=day)):
                return True
        except (OSError, TypeError, ValueError):
            continue
    return False


def _verify_causal_references(
    *, day: date, symbols: set[str], books: Mapping[str, Mapping[str, Any]],
    twse_path: Path, tpex_path: Path,
) -> dict[str, Any]:
    """Identify evidence-backed 09:01 model references, never paper fills."""
    from scripts import rebuild_tw_day_trade_open_price_replay as replay

    source_hashes = {str(path.resolve()): _digest(path) for path in (twse_path, tpex_path)}
    # Establish a fresh read after the source hashes, not an earlier cached
    # daily slice from another caller.
    replay._official_aggregate_daily_rows.cache_clear()
    daily = replay._official_aggregate_daily_rows(twse_path, tpex_path, day)
    first, local_no_trade = replay._verified_local_0901_observations(books=books, trading_date=day)
    official_no_trade = {symbol for symbol in symbols
                         if replay._official_zero_volume_no_ohlc(daily.get(symbol) or {})}
    no_trade = (local_no_trade | official_no_trade) & symbols
    prior = replay._previous_official_last_trades(
        symbols=no_trade, trading_date=day, twse_path=twse_path, tpex_path=tpex_path,
    )
    opens = {symbol: row.get("open") for symbol, row in daily.items() if symbol in symbols}
    references, proof = replay._causal_0901_reference_map(
        symbols=symbols, official_daily_opens=opens, first_minute_opens=first,
        no_trade=no_trade, prior_trades=prior,
    )
    if source_hashes != {str(path.resolve()): _digest(path) for path in (twse_path, tpex_path)}:
        raise RuntimeError("official daily sources changed during reference verification")
    return {
        **proof,
        "official_daily_source_sha256": source_hashes,
        "receipt_verified_first_minute_rows": len(first),
        "receipt_verified_zero_volume_first_minute_rows": len(local_no_trade),
        "official_zero_volume_no_ohlc_symbols": sorted(official_no_trade),
        "references": {
            symbol: {"price": value,
                     "basis": "first_minute_trade" if symbol in first else "previous_real_trade_model_reference_only",
                     **({"previous_trade": prior[symbol]} if symbol in prior else {})}
            for symbol, value in sorted(references.items())
        },
        "not_a_fill_or_replay_acceptance": True,
    }


def inspect_sources(
    *, day: date, audit_path: Path, state_dir: Path, specs: list,
    minute_roots: tuple[Path, ...], verify_causal_references: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    audit_bytes = audit_path.read_bytes()
    opening = json.loads(audit_bytes)
    if (opening.get("contract") != "daily_opening_original_clock_deadline_v1"
            or opening.get("session_date") != day.isoformat()):
        raise ValueError("opening audit is not the requested session's original-clock receipt")
    expected = set(opening.get("expected_markets") or [])
    by_market = {spec.market: spec for spec in specs}
    if not expected or expected != set(by_market):
        raise ValueError("current enabled modes differ from the audited opening roster")
    published = (opening.get("publication") or {}).get("modes") or {}
    registrations, event_digest = _registered_events(state_dir, day)
    state = read_paper_state(state_dir / "state.json")
    if state.get("simulation_only") is not True or state.get("production_order_possible") is not False:
        raise ValueError("source is not a simulation-only paper ledger")
    missing_symbols: set[str] = set()
    result: dict[str, Any] = {}
    for market, spec in sorted(by_market.items()):
        published_row = published.get(market) or {}
        signal_id = str(published_row.get("signal_id") or "")
        if published_row.get("state") != "published" or not signal_id:
            raise ValueError(f"missing first publication: {market}")
        published_at = datetime.fromisoformat(str(published_row.get("artifact_published_at") or ""))
        if published_at.utcoffset() is None or published_at.astimezone(TAIPEI).date() != day:
            raise ValueError(f"invalid first publication clock: {market}")
        candidates = [p for p in spec.live_output_dir.glob(f"{day}*/**/summary.json")
                      if p.parent.name == signal_id]
        if len(candidates) != 1:
            raise ValueError(f"expected exactly one first signal artifact: {market}: {len(candidates)}")
        summary_path = candidates[0].resolve()
        summary_hash = _digest(summary_path)
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if (summary.get("signal_id") != signal_id
                or summary.get("market") != str(spec.signal_market or market)
                or summary.get("execution_mode") != "tw_day_trade"):
            raise ValueError(f"first signal identity differs: {market}")
        weights_path = summary_path.with_name("target_weights.parquet")
        if weights_path.is_file():
            weights_hash = _digest(weights_path)
            rows = pl.read_parquet(weights_path).to_dicts()
        else:
            weights_path = summary_path.with_name("execution_weights.json")
            if Path(str(summary.get("execution_weights_path") or "")).resolve() != weights_path.resolve():
                raise ValueError(f"first signal weights path differs: {market}")
            weights_hash = _digest(weights_path)
            rows = read_execution_weight_rows(weights_path, market=str(spec.signal_market or market), signal_id=signal_id)
        if _digest(summary_path) != summary_hash or _digest(weights_path) != weights_hash:
            raise RuntimeError(f"first signal changed during inspection: {market}")
        seen: set[str] = set()
        nonzero = 0
        missing: set[str] = set()
        for row in rows:
            symbol = str(row.get("symbol") or "")
            if not symbol or symbol in seen:
                raise ValueError(f"duplicate or empty signal symbol: {market}")
            seen.add(symbol)
            weight = float(row.get("target_weight") or 0)
            if not math.isfinite(weight):
                raise ValueError(f"nonfinite signal weight: {market}/{symbol}")
            if not weight:
                continue
            nonzero += 1
            try:
                price = float(row.get("open_price") or 0)
            except (TypeError, ValueError):
                price = 0
            if not math.isfinite(price) or price <= 0:
                missing.add(symbol)
        if not rows:
            raise ValueError(f"first signal has no rows: {market}")
        missing_symbols.update(missing)
        registered = registrations.get(market) or []
        current_mode = (state.get("modes") or {}).get(market) or {}
        positions = current_mode.get("positions") or {}
        result[market] = {
            "first_published_signal_id": signal_id,
            "first_published_at": published_at.isoformat(),
            "summary_path": str(summary_path), "summary_sha256": summary_hash,
            "weights_path": str(weights_path), "weights_sha256": weights_hash,
            "target_rows": len(rows), "nonzero_target_rows": nonzero,
            "nonzero_without_open_reference_symbols": sorted(missing),
            "opening_reference_basis": summary.get("opening_coverage_gate_basis"),
            "live_session_open_feature_applied": summary.get("live_session_open_feature_applied"),
            "live_session_open_reference_applied": summary.get("live_session_open_reference_applied"),
            "registration_count": len(registered), "registrations": registered,
            "first_registered_matches_first_publication": bool(registered and registered[0]["signal_id"] == signal_id),
            "current_state_session_date": current_mode.get("session_date"),
            "current_state_status": current_mode.get("engine_status"),
            "current_state_nonflat_positions": sum(bool(int(p.get("signed_shares") or 0)) for p in positions.values()),
        }
    books, local_receipt = load_local_stock_0901_vwaps(
        minute_roots, sorted(missing_symbols), trading_date=day,
    )
    if local_receipt.get("error_counts"):
        raise RuntimeError(f"local minute-source scan failed: {local_receipt['error_counts']}")
    no_local_row = missing_symbols - set(books)
    already_empty = {s for s in no_local_row if _terminal_empty(s, day, minute_roots)}
    to_fetch = sorted(no_local_row - already_empty)
    report = {
        "schema_version": 1, "contract": "first_published_signal_and_local_minute_inventory_v1",
        "session_date": day.isoformat(), "observed_at": datetime.now(TAIPEI).isoformat(),
        "status": "source_inventory_only_not_a_replay_or_deployment_acceptance",
        "simulation_only": True, "production_order_possible": False, "ledger_modified": False,
        "opening_audit_path": str(audit_path.resolve()), "opening_audit_sha256": hashlib.sha256(audit_bytes).hexdigest(),
        "logical_event_journal_sha256": event_digest, "modes": result,
        "first_signal_missing_distinct_symbols": len(missing_symbols),
        "local_0901_rows_found": len(books), "local_rows_are_not_fill_or_receipt_verification": True,
        "already_queried_empty_symbols": sorted(already_empty),
        "additional_query_symbols": to_fetch,
        "minute_roots": [str(root.resolve()) for root in minute_roots],
    }
    if verify_causal_references:
        from scripts.rebuild_tw_day_trade_open_price_replay import (
            DEFAULT_TPEX_DAILY_OHLCV_PATH, DEFAULT_TWSE_DAILY_OHLCV_PATH,
        )
        report["causal_0901_reference_verification"] = _verify_causal_references(
            day=day, symbols=missing_symbols, books=books,
            twse_path=DEFAULT_TWSE_DAILY_OHLCV_PATH, tpex_path=DEFAULT_TPEX_DAILY_OHLCV_PATH,
        )
    return report, to_fetch


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--opening-audit", type=Path, required=True)
    parser.add_argument("--markets-dir", type=Path, default=ROOT / "services/discord_bot/markets")
    parser.add_argument("--minute-data-root", type=Path, action="append", default=[])
    parser.add_argument("--verify-causal-references", action="store_true",
                        help="Verify first-minute receipts and previous real trades; never asserts fills")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fetch-symbols-output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    symbols_output = args.fetch_symbols_output.resolve()
    state_dir = args.state_dir.resolve()
    if output == symbols_output or state_dir in output.parents or state_dir in symbols_output.parents:
        raise ValueError("inventory outputs must be separate from the production ledger")
    roots = tuple(dict.fromkeys([*(p.resolve() for p in args.minute_data_root),
                                 *(p.resolve() for p in DEFAULT_LOCAL_MINUTE_ROOTS)]))
    specs, _configs, errors = _mode_specs(args.markets_dir)
    if errors:
        raise ValueError(f"mode configuration errors: {errors}")
    report, symbols = inspect_sources(day=args.date, audit_path=args.opening_audit,
                                      state_dir=state_dir, specs=specs, minute_roots=roots,
                                      verify_causal_references=args.verify_causal_references)
    output.parent.mkdir(parents=True, exist_ok=True)
    symbols_output.parent.mkdir(parents=True, exist_ok=True)
    csv_text = io.StringIO(newline="")
    writer = csv.writer(csv_text, lineterminator="\n")
    writer.writerow(["symbol"])
    writer.writerows([symbol] for symbol in symbols)
    # Exclusive files avoid silently overwriting earlier candidate evidence.
    with symbols_output.open("x", encoding="utf-8", newline="") as stream:
        stream.write(csv_text.getvalue())
    report["additional_query_manifest_path"] = str(symbols_output)
    report["additional_query_manifest_sha256"] = _digest(symbols_output)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(json.dumps({"status": report["status"], "modes": len(report["modes"]),
                      "first_signal_missing_distinct_symbols": report["first_signal_missing_distinct_symbols"],
                      "additional_queries": len(symbols), "output": str(output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
