#!/usr/bin/env python3
"""Receipt-bound private replay projection for explicit legacy FinMind units.

Old native receipts already declared listed/OTC lots and emerging shares but
predated the expanded volume-contract fields. Never edit those receipts or
infer a no-trade minute. Reuse the existing normalization and official-market
proofs; this retrospective source is neither an opening-time observation nor
an executable fill.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl

from downloader.artifact_io import atomic_write_json
from downloader.finmind_storage_objects import validate_parquet
from downloader.finmind_volume_units import FINMIND_CASH_INTRADAY_UNIT_SOURCE
from scripts.prepare_finmind_tw_day_trade_repair_source import (
    _candidate_signal_scope, _json, _official_prices, _pin, normalize_source,
)
from stockagent.data.tw_minute_unit_evidence import pin_official_market_inputs, read_official_market_evidence
from stockagent.data_sync import workspace_volume


CONTRACT = "finmind_tw_day_trade_legacy_private_replay_source_v1"
MANIFEST = "finmind_replay_legacy_source.json"
LEGACY_UNITS = "TWSE_TPEX_lots_emerging_shares"


def _require_legacy_units(units: object) -> None:
    if (not isinstance(units, dict)
            or units.get("stock_share_unit_status") != "mixed_market_lots_or_shares_preserved"
            or units.get("source_volume_units") != {"volume": LEGACY_UNITS}
            or units.get("unit_source") != FINMIND_CASH_INTRADAY_UNIT_SOURCE
            or any(key in units for key in ("contract", "documented_share_multipliers_by_market",
                                             "requires_same_symbol_dated_market_evidence"))):
        raise ValueError("legacy native unit declaration is missing, ambiguous or not legacy")


def _inputs(*, day: str, finmind_root: Path, public_root: Path,
            candidate_signal_preflight: Path, candidate_market: str,
            symbols: tuple[str, ...]) -> tuple[pl.DataFrame, dict, list[dict]]:
    if date.fromisoformat(day).isoformat() != day:
        raise ValueError("canonical session date is required")
    finmind_root, public_root = finmind_root.resolve(strict=True), public_root.resolve(strict=True)
    if (finmind_root != (ROOT / "data_finmind/complement").resolve(strict=True)
            or public_root != (ROOT / "data_tw_public").resolve(strict=True)):
        raise ValueError("source roots must be the canonical provider and public owners")
    if (not symbols or len(set(symbols)) != len(symbols)
            or any(not isinstance(symbol, str) or not symbol for symbol in symbols)):
        raise ValueError("nonempty unique explicit symbol scope is required")
    target_symbols, pins = _candidate_signal_scope(day, candidate_signal_preflight, candidate_market)
    requested = set(symbols)
    if not requested <= target_symbols:
        raise ValueError("legacy source scope must be nonzero targets in the pinned signal")
    receipt_path = finmind_root / "receipts/TaiwanStockKBar/all" / f"{day}.json"
    if receipt_path.is_symlink() or not receipt_path.resolve(strict=True).is_relative_to(finmind_root):
        raise ValueError("unsafe FinMind native receipt path")
    receipt = _json(receipt_path)
    if (receipt.get("status") != "complete" or receipt.get("dataset") != "TaiwanStockKBar"
            or receipt.get("data_id") != "" or receipt.get("partition") != day):
        raise ValueError("FinMind native receipt identity mismatch")
    _require_legacy_units(receipt.get("volume_units"))
    relative = Path(str(receipt.get("parquet_path") or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("FinMind source path escapes its owner root")
    source = (finmind_root / relative).resolve(strict=True)
    if not source.is_relative_to(finmind_root):
        raise ValueError("FinMind source path escapes its owner root")
    receipt_pin, source_pin = _pin(receipt_path), _pin(source)
    if (source_pin["sha256"] != receipt.get("sha256")
            or source_pin["size_bytes"] != receipt.get("parquet_size_bytes")
            or validate_parquet(source, "TaiwanStockKBar", day)["rows"] != receipt.get("rows")):
        raise ValueError("FinMind native bytes, size or rows differ from the receipt")
    records = pin_official_market_inputs(public_root, {day})
    if {row["venue"] for row in records} != {"twse", "tpex"}:
        raise ValueError("dated official market source receipts unavailable")
    official = _official_prices(records, requested)
    venues = pl.concat([read_official_market_evidence(row, symbols=sorted(requested))
                        for row in records], how="vertical")
    data, summary = normalize_source(pl.read_parquet(source), day=day, symbols=requested,
                                     venues=venues, official=official)
    pins.extend((receipt_pin, source_pin))
    for record in records:
        official_pin = _pin(Path(record["path"]))
        if official_pin["sha256"] != record["sha256"] or official_pin["size_bytes"] != record["size"]:
            raise RuntimeError("official raw source differs from its dated receipt")
        pins.append(official_pin)
    pins.extend(_pin(ROOT / name) for name in (
        "scripts/prepare_finmind_tw_day_trade_legacy_source.py",
        "scripts/prepare_finmind_tw_day_trade_repair_source.py",
        "stockagent/data/tw_minute_sources.py", "stockagent/data/tw_minute_unit_evidence.py",
        "downloader/download_tw_public_data.py", "downloader/finmind_storage_objects.py",
        "downloader/finmind_volume_units.py", "downloader/stock_volume_units.py",
        "downloader/parquet_integrity.py"))
    if any(_pin(Path(pin["path"])) != pin for pin in pins):
        raise RuntimeError("a source changed during legacy projection")
    summary.update({"native_source_sha256": source_pin["sha256"],
                    "provider_receipt_fetched_at_utc": receipt.get("fetched_at_utc"),
                    "legacy_unit_declaration": LEGACY_UNITS,
                    "native_receipt_modified": False, "historical_availability_not_proven": True})
    return data, summary, pins


def build(*, day: str, finmind_root: Path, public_root: Path,
          candidate_signal_preflight: Path, candidate_market: str,
          symbols: tuple[str, ...], output_root: Path) -> dict:
    workspace = workspace_volume.configuration()
    allowed = Path(workspace["mount_point"]) / "build"
    destination = output_root.resolve()
    if not destination.is_relative_to(allowed) or destination == allowed:
        raise ValueError("legacy projection must use a new private E build subdirectory")
    workspace_volume.admit(required_bytes=128 * 1024**2, c=workspace)
    data, summary, pins = _inputs(day=day, finmind_root=finmind_root, public_root=public_root,
        candidate_signal_preflight=candidate_signal_preflight, candidate_market=candidate_market,
        symbols=symbols)
    workspace_volume.admit(required_bytes=max(128 * 1024**2, data.estimated_size() * 2), c=workspace)
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    partition = destination / f"trade_date={day}"
    partition.mkdir(mode=0o700)
    output = partition / "data.parquet"
    data.write_parquet(output, compression="zstd")
    with output.open("rb") as handle:
        os.fsync(handle.fileno())
    workspace_volume.admit(required_bytes=0, c=workspace)
    atomic_write_json(destination / MANIFEST, {
        "contract": CONTRACT, "session_date": day, "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "simulation_only": True, "production_order_possible": False, "public_export_authorized": False,
        "finmind_root": str(finmind_root.resolve()), "public_root": str(public_root.resolve()),
        "candidate_signal_preflight": str(candidate_signal_preflight.resolve()), "candidate_market": candidate_market,
        "symbols": sorted(symbols), "output": _pin(output), "source_pins": pins, "summary": summary,
    })
    return verify(destination)


def verify(output_root: Path) -> dict:
    root = output_root.resolve(strict=True)
    manifest_path = root / MANIFEST
    if manifest_path.is_symlink():
        raise ValueError("redirected legacy projection manifest")
    manifest = _json(manifest_path)
    if (manifest.get("contract") != CONTRACT or manifest.get("simulation_only") is not True
            or manifest.get("production_order_possible") is not False
            or manifest.get("public_export_authorized") is not False):
        raise ValueError("invalid legacy private replay source contract")
    output = root / f"trade_date={manifest['session_date']}" / "data.parquet"
    if (output.is_symlink() or output.resolve(strict=True) != output
            or manifest.get("output", {}).get("path") != str(output)):
        raise ValueError("legacy projection escaped its source root")
    for pin in [manifest["output"], *manifest["source_pins"]]:
        if _pin(Path(pin["path"])) != pin:
            raise RuntimeError("pinned legacy replay input changed")
    data, summary, pins = _inputs(day=manifest["session_date"], finmind_root=Path(manifest["finmind_root"]),
        public_root=Path(manifest["public_root"]), candidate_signal_preflight=Path(manifest["candidate_signal_preflight"]),
        candidate_market=manifest["candidate_market"], symbols=tuple(manifest["symbols"]))
    if pins != manifest["source_pins"] or summary != manifest["summary"] or not data.equals(pl.read_parquet(output)):
        raise RuntimeError("legacy projection differs from its source reconstruction")
    return {"contract": CONTRACT, "status": "source_verified_not_ledger_applied", "root": str(root),
            "session_date": manifest["session_date"], "summary": summary,
            "source_pins": pins, "output": manifest["output"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "verify"))
    parser.add_argument("--day", required=True)
    parser.add_argument("--candidate-signal-preflight", type=Path, required=True)
    parser.add_argument("--candidate-market", required=True)
    parser.add_argument("--symbol", action="append", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    result = (build(day=args.day, finmind_root=ROOT / "data_finmind/complement", public_root=ROOT / "data_tw_public",
                    candidate_signal_preflight=args.candidate_signal_preflight, candidate_market=args.candidate_market,
                    symbols=tuple(args.symbol), output_root=args.output_root)
              if args.action == "build" else verify(args.output_root))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
