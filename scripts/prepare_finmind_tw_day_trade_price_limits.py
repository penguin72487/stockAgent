#!/usr/bin/env python3
"""Build a private, source-pinned price-limit input for paper replay.

No source requests, live ledger changes or public exports are made here.  A
vendor's extreme no-limit sentinel is not an executable TP/SL boundary.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import math
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl

from downloader.artifact_io import atomic_write_json
from downloader.provider_priority import inspect
from scripts.prepare_finmind_tw_day_trade_repair_source import _json, _pin
from stockagent.data_sync import workspace_volume


CONTRACT = "finmind_tw_day_trade_private_price_limits_v1"
MANIFEST = "finmind_price_limits_source.json"


def _selected_priority_items(status: dict, missing: set[str]) -> list[tuple[str, dict]]:
    """Select one session's symbols from a shared finite priority request."""
    selected = []
    seen: set[str] = set()
    for item in status.get("items", []):
        key = json.loads(item["key"])
        if not isinstance(key, list) or len(key) != 3 or key[0] != "TaiwanStockPriceLimit" or key[2] != "history":
            raise ValueError("unexpected priority request key")
        symbol = key[1]
        if symbol not in missing:
            continue
        if symbol in seen or item.get("state") != "complete":
            raise ValueError("missing, duplicate or incomplete price-limit request")
        seen.add(symbol)
        selected.append((symbol, item))
    if seen != missing:
        raise ValueError("priority request does not cover exact missing symbol scope")
    return selected


def source_price_bounds(row: dict) -> tuple[float, float | None, float | None, str]:
    """Keep ambiguous extreme/no-limit observations out of bracket prices."""
    ref, up, down = (float(row[key]) for key in ("reference_price", "limit_up", "limit_down"))
    if any(not math.isfinite(x) for x in (ref, up, down)) or ref <= 0:
        raise ValueError("invalid source price-limit numeric values")
    if (up == 0 and down == 0) or (up == 9999.95 and down == 0.01):
        return ref, None, None, "provider_no_limit_or_extreme_sentinel_not_executable"
    if not (0 < down <= ref <= up):
        raise ValueError("invalid source price-limit ordering")
    return ref, up, down, "provider_historical_price_limits"


def _projected_source_row(*, day: str, symbol: str, ref: float, up: float | None,
                          down: float | None, state: str, fetched_at: str,
                          columns: list[str]) -> dict:
    values = {"trading_date": day, "symbol": symbol, "reference_price": ref,
              "upper_limit_price": up, "lower_limit_price": down, "prepared_at": fetched_at,
              "source": f"finmind_historical_replay:{state}",
              # This is the source record's session, not a claim that the
              # historical row was available before that session opened.
              "source_session_date": day, "source_url": "",
              "historical_reconstruction": True}
    if any(column not in values for column in columns):
        raise ValueError("unknown original price-limit metadata column")
    return {column: values[column] for column in columns}


def _inputs(*, day: str, finmind_root: Path, baseline: Path,
            inventory: Path, request_id: str) -> tuple[pl.DataFrame, dict, list[dict]]:
    if date.fromisoformat(day).isoformat() != day:
        raise ValueError("session date must be canonical")
    if finmind_root.resolve(strict=True) != (ROOT / "data_finmind/complement").resolve(strict=True):
        raise ValueError("FinMind root is not the canonical owner")
    if baseline.resolve(strict=True) != (ROOT / "artifacts/live/tw_price_limits" / f"{day}.parquet").resolve(strict=True):
        raise ValueError("baseline is not the canonical original price-limit snapshot")
    inventory_value = _json(inventory)
    if (inventory_value.get("session_date") != day or inventory_value.get("ledger_modified") is not False
            or not isinstance(inventory_value.get("minute_symbols"), list)):
        raise ValueError("scoped revision inventory mismatch")
    audit_pin = None
    if inventory_value.get("contract") == "counterfactual_signal_price_limit_scope_v1":
        audit_path = Path(inventory_value["signal_source_audit_path"]).resolve(strict=True)
        audit_pin = _pin(audit_path)
        audit = _json(audit_path)
        expected = sorted(str(item["symbol"]) for item in audit.get("targets_without_valid_limits", [])
                          if item.get("date") == day)
        if (audit_pin["sha256"] != inventory_value.get("signal_source_audit_sha256")
                or audit.get("contract") != "isolated_counterfactual_signals_source_audit_v1"
                or audit.get("status") != "source_gaps_remain" or audit.get("ledger_modified") is not False
                or audit.get("publication_authorized") is not False
                or expected != sorted(inventory_value["minute_symbols"])):
            raise ValueError("counterfactual price-limit scope differs from source audit")
    original = pl.read_parquet(baseline)
    required = {"trading_date", "symbol", "reference_price", "upper_limit_price",
                "lower_limit_price", "prepared_at", "source"}
    optional = {"source_session_date", "source_url", "historical_reconstruction"}
    if (not required.issubset(original.columns) or set(original.columns) - required - optional
            or original.select(pl.col("symbol").is_duplicated().any()).item()):
        raise ValueError("unexpected or duplicate original price-limit snapshot")
    if set(original["trading_date"].to_list()) != {day}:
        raise ValueError("original price-limit snapshot has the wrong session")
    missing = (set(inventory_value["minute_symbols"]) - set(inventory_value.get("official_no_trade_symbols", []))
               - set(original["symbol"].to_list()))
    if not missing:
        raise ValueError("no missing price-limit symbols in this scope")
    status = inspect(finmind_root, request_id)
    if status.get("provider") != "finmind" or status.get("request_id") != request_id:
        raise ValueError("priority request identity mismatch")
    priority_path = finmind_root / "priority_queries" / f"{request_id}.json"
    pins = [_pin(inventory), _pin(baseline), _pin(priority_path)]
    if audit_pin is not None:
        pins.append(audit_pin)
    rows, unresolved = [], []
    selected = _selected_priority_items(status, missing)
    seen = {symbol for symbol, _item in selected}
    for symbol, item in selected:
        receipt_path = (finmind_root / str(item.get("receipt_path") or "")).resolve(strict=True)
        if not receipt_path.is_relative_to(finmind_root.resolve()):
            raise ValueError("receipt escapes owner")
        receipt = _json(receipt_path)
        if (receipt.get("dataset") != "TaiwanStockPriceLimit" or receipt.get("data_id") != symbol
                or receipt.get("partition") != "history" or receipt.get("status") != "complete"):
            raise ValueError("price-limit receipt identity mismatch")
        relative = Path(str(receipt.get("parquet_path") or ""))
        source_path = (finmind_root / relative).resolve(strict=True)
        if relative.is_absolute() or ".." in relative.parts or not source_path.is_relative_to(finmind_root.resolve()):
            raise ValueError("price-limit source escapes owner")
        receipt_pin, source_pin = _pin(receipt_path), _pin(source_path)
        if (source_pin["sha256"] != receipt.get("sha256")
                or source_pin["size_bytes"] != receipt.get("parquet_size_bytes")):
            raise ValueError("price-limit source differs from receipt")
        native = pl.read_parquet(source_path)
        if native.height != receipt.get("rows"):
            raise ValueError("price-limit source row count mismatch")
        if set(native["stock_id"].to_list()) != {symbol}:
            raise ValueError("price-limit source contains a different symbol")
        same_day = native.filter((pl.col("date") == day) & (pl.col("stock_id") == symbol))
        if same_day.height != 1:
            raise ValueError("exact same-day price-limit record missing or duplicated")
        ref, up, down, state = source_price_bounds(same_day.row(0, named=True))
        if up is None:
            unresolved.append(symbol)
        rows.append(_projected_source_row(
            day=day, symbol=symbol, ref=ref, up=up, down=down, state=state,
            fetched_at=str(receipt.get("fetched_at_utc") or ""), columns=original.columns))
        pins.extend((receipt_pin, source_pin))
    addition = pl.DataFrame(rows, schema=original.schema).select(original.columns)
    result = pl.concat([original, addition], how="vertical").sort("symbol")
    pins.extend(_pin(ROOT / path) for path in (
        "scripts/prepare_finmind_tw_day_trade_price_limits.py",
        "scripts/prepare_finmind_tw_day_trade_repair_source.py", "downloader/provider_priority.py"))
    if any(_pin(Path(pin["path"])) != pin for pin in pins):
        raise RuntimeError("price-limit input changed while constructing projection")
    return result, {"original_rows": original.height, "supplemented_symbols": sorted(seen),
                    "finite_bound_symbols": sorted(seen - set(unresolved)),
                    "unresolved_or_no_limit_symbols": sorted(unresolved),
                    "unresolved_bounds_remain_null": True,
                    "historical_availability_not_proven": True}, pins


def build(*, day: str, finmind_root: Path, baseline: Path, inventory: Path,
          request_id: str, output_root: Path) -> dict:
    workspace = workspace_volume.configuration()
    expected = Path(workspace["mount_point"]) / "build"
    destination = output_root.resolve()
    if not destination.is_relative_to(expected) or destination == expected:
        raise ValueError("private projection must use a new E build subdirectory")
    workspace_volume.admit(required_bytes=128 * 1024**2, c=workspace)
    frame, summary, pins = _inputs(day=day, finmind_root=finmind_root, baseline=baseline,
                                   inventory=inventory, request_id=request_id)
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    output = destination / f"{day}.parquet"
    frame.write_parquet(output, compression="zstd")
    with output.open("rb") as handle:
        os.fsync(handle.fileno())
    workspace_volume.admit(required_bytes=0, c=workspace)
    manifest = {"contract": CONTRACT, "session_date": day,
        "created_at_utc": datetime.now(timezone.utc).isoformat(), "simulation_only": True,
        "production_order_possible": False, "public_export_authorized": False,
        "finmind_root": str(finmind_root.resolve()), "baseline": str(baseline.resolve()),
        "inventory": str(inventory.resolve()), "request_id": request_id,
        "output": _pin(output), "source_pins": pins, "summary": summary}
    atomic_write_json(destination / MANIFEST, manifest)
    return verify(destination)


def verify(root: Path) -> dict:
    root = root.resolve(strict=True)
    manifest = _json(root / MANIFEST)
    if (manifest.get("contract") != CONTRACT or manifest.get("simulation_only") is not True
            or manifest.get("production_order_possible") is not False
            or manifest.get("public_export_authorized") is not False):
        raise ValueError("invalid private replay price-limit contract")
    output = (root / f"{manifest['session_date']}.parquet").resolve(strict=True)
    if not output.is_relative_to(root) or _pin(output) != manifest.get("output"):
        raise ValueError("private price-limit projection changed")
    frame, summary, pins = _inputs(day=manifest["session_date"], finmind_root=Path(manifest["finmind_root"]),
        baseline=Path(manifest["baseline"]), inventory=Path(manifest["inventory"]),
        request_id=manifest["request_id"])
    if pins != manifest.get("source_pins") or summary != manifest.get("summary") or not frame.equals(pl.read_parquet(output)):
        raise ValueError("price-limit projection lineage or contents changed")
    return {"contract": CONTRACT, "root": str(root), "session_date": manifest["session_date"],
            "output": manifest["output"], "source_pins": pins, "summary": summary}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "verify"))
    parser.add_argument("--day", required=True)
    parser.add_argument("--finmind-root", type=Path, default=ROOT / "data_finmind/complement")
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    value = (build(day=args.day, finmind_root=args.finmind_root, baseline=args.baseline,
                   inventory=args.inventory, request_id=args.request_id, output_root=args.output_root)
             if args.action == "build" else verify(args.output_root))
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
