#!/usr/bin/env python3
"""Prove prior trade-price references without inventing an executable quote.

The latest date is relative to the retained official aggregate, not a claim
that every upstream source is complete. The resulting prices are for an
isolated paper-replay diagnostic and cannot themselves authorize a fill.
"""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl

from downloader.download_tw_public_data import _parse_historical_response_content
from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from scripts.prepare_finmind_tw_day_trade_repair_source import _pin
from scripts.rebuild_tw_day_trade_open_price_replay import (
    DEFAULT_TPEX_DAILY_OHLCV_PATH, DEFAULT_TWSE_DAILY_OHLCV_PATH, _previous_official_last_trades,
)
from stockagent.data.tw_minute_unit_evidence import SPECS, pin_official_market_inputs, read_official_market_evidence


def _positive_number(value) -> float:
    if isinstance(value, bool):
        raise ValueError("invalid official numeric value")
    try:
        number = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid official numeric value") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError("positive official trade evidence is required")
    return number


def audit(*, inventory_path: Path, public_root: Path) -> dict:
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    day = date.fromisoformat(str(inventory.get("session_date") or ""))
    symbols = inventory.get("official_open_unavailable_symbols")
    if (inventory.get("ledger_modified") is not False or not isinstance(symbols, list)
            or not symbols or len(symbols) != len(set(symbols))
            or any(not isinstance(symbol, str) or not symbol for symbol in symbols)):
        raise ValueError("an exact private unpriced-target inventory is required")
    paths = (inventory_path, DEFAULT_TWSE_DAILY_OHLCV_PATH, DEFAULT_TPEX_DAILY_OHLCV_PATH,
             Path(__file__).resolve(), ROOT / "scripts/rebuild_tw_day_trade_open_price_replay.py",
             ROOT / "stockagent/data/tw_minute_unit_evidence.py", ROOT / "downloader/download_tw_public_data.py")
    before = [_pin(path) for path in paths]
    retained = _previous_official_last_trades(symbols=set(symbols), trading_date=day,
        twse_path=DEFAULT_TWSE_DAILY_OHLCV_PATH, tpex_path=DEFAULT_TPEX_DAILY_OHLCV_PATH)
    dates = {item["session_date"] for item in retained.values()}
    records = pin_official_market_inputs(public_root, dates)
    lookup = {(row["day"], row["venue"]): row for row in records}
    if len(lookup) != len(records):
        raise RuntimeError("ambiguous official daily source receipt")
    decoded: dict[tuple[str, str], pl.DataFrame] = {}
    used: dict[tuple[str, str], dict] = {}
    proofs = []
    for symbol, prior in sorted(retained.items()):
        prior_day = date.fromisoformat(str(prior["session_date"]))
        venue = {"twse_daily_ohlcv": "twse", "tpex_daily_ohlcv": "tpex"}.get(prior.get("source"))
        record = lookup.get((str(prior_day), venue))
        if prior_day >= day or record is None:
            raise RuntimeError(f"prior dated official trade receipt unavailable: {symbol}")
        key = (str(prior_day), venue)
        if key not in decoded:
            read_official_market_evidence(record, symbols=[])
            content = Path(record["path"]).read_bytes()
            if hashlib.sha256(content).hexdigest() != record["sha256"]:
                raise RuntimeError("official raw source changed during read")
            decoded[key], _ = _parse_historical_response_content(
                SPECS[record["dataset"]], prior_day, content, record["response_kind"])
            used[key] = record
        symbol_col, close_col = ("證券代號", "收盤價") if venue == "twse" else ("代號", "收盤")
        row = decoded[key].filter(pl.col(symbol_col).cast(pl.String).str.strip_chars() == symbol)
        if row.height != 1:
            raise RuntimeError(f"prior official symbol is absent or duplicated: {symbol}")
        item = row.row(0, named=True)
        price, volume = _positive_number(item[close_col]), _positive_number(item["成交股數"])
        if not volume.is_integer():
            raise ValueError("official share volume must be integral")
        if not math.isclose(price, _positive_number(prior["price"]), rel_tol=0, abs_tol=1e-9):
            raise RuntimeError(f"prior aggregate and raw official close disagree: {symbol}")
        proofs.append({"symbol": symbol, "price": price, "prior_session": str(prior_day),
                       "venue": venue, "prior_volume_shares": int(volume),
                       "official_raw_path": record["path"], "official_raw_sha256": record["sha256"],
                       "reference_kind": "previous_observed_official_trade_reference_only",
                       "reference_is_executable_quote": False, "fill_authorized": False,
                       "target_day_open_status": "inventory_classified_no_trade_or_halted_not_reverified_here"
                       if symbol in set(inventory.get("official_no_trade_symbols") or [])
                       else "opening_status_unresolved"})
    raw_pins = [_pin(Path(record["path"])) for record in used.values()]
    if any(pin["sha256"] != record["sha256"] or pin["size_bytes"] != record["size"]
           for pin, record in zip(raw_pins, used.values(), strict=True)):
        raise RuntimeError("prior official raw source changed during audit")
    pins = before + raw_pins
    if [_pin(path) for path in paths] != before:
        raise RuntimeError("prior-trade source changed during audit")
    return {"contract": "historical_previous_trade_reference_diagnostic_v1",
            "status": "reference_only_not_replayed", "target_session": str(day),
            "references": proofs, "unresolved_symbols": sorted(set(symbols) - set(retained)),
            "latest_trade_scope": "latest_positive_trade_in_retained_official_aggregates_only",
            "target_day_no_first_print_proven_for_all": False,
            "reference_is_executable_quote": False, "paper_fills_created": 0,
            "ledger_modified": False, "publication_authorized": False, "source_pins": pins}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--public-root", type=Path, default=ROOT / "data_tw_public")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or output.is_relative_to((ROOT / "service_data/live").resolve()):
        raise ValueError("a new private receipt outside live data is required")
    result = audit(inventory_path=args.inventory.resolve(strict=True),
                   public_root=args.public_root.resolve(strict=True))
    _write_new_plan(output, result)
    print(json.dumps({key: value for key, value in result.items() if key != "source_pins"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
