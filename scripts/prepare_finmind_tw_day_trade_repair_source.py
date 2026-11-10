#!/usr/bin/env python3
"""Build and re-verify a private, source-bound FinMind paper-replay input.

This adapter makes no provider calls, orders, ledger writes or public exports.
Absent bars remain absent. The source is a later historical observation, not
evidence that its prices were known at the original decision time.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl

from downloader.artifact_io import atomic_write_json
from downloader.finmind_storage_objects import validate_parquet
from downloader.finmind_volume_units import (
    FINMIND_CASH_INTRADAY_SHARE_MULTIPLIERS, FINMIND_CASH_INTRADAY_UNIT_CONTRACT,
    FINMIND_CASH_INTRADAY_UNIT_SOURCE,
)
from downloader.download_tw_public_data import _parse_historical_response_content
from stockagent.data.tw_minute_sources import dated_market_share_proofs, normalize_finmind
from stockagent.data.tw_minute_unit_evidence import (
    SPECS, pin_official_market_inputs, read_official_market_evidence,
)
from stockagent.data_sync import workspace_volume


CONTRACT = "finmind_tw_day_trade_private_replay_source_v1"
MANIFEST = "finmind_replay_source.json"


def _sha(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _pin(path: Path) -> dict:
    path = path.resolve(strict=True)
    before = path.stat()
    digest = _sha(path)
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise RuntimeError(f"source changed while hashing: {path}")
    return {"path": str(path), "sha256": digest, "size_bytes": before.st_size}


def _json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _candidate_signal_scope(day: str, preflight_path: Path, market: str) -> tuple[set[str], list[dict]]:
    """Select only nonzero targets from one independently pinned replay signal.

    This defines a data-fetch scope, not a claim that the signal existed at
    market open or that its targets were executable or filled.
    """
    if date.fromisoformat(day).isoformat() != day or not isinstance(market, str) or not market:
        raise ValueError("candidate scope needs an exact day and market")
    preflight_pin = _pin(preflight_path)
    preflight = _json(preflight_path)
    if (preflight.get("contract") != "exact_selected_signal_preflight_v1"
            or preflight.get("status") != "accepted_sources_only"
            or preflight.get("simulation_only") is not True
            or preflight.get("production_order_possible") is not False
            or preflight.get("execution_or_minute_coverage_accepted") is not False
            or type(preflight.get("expected_signals")) is not int
            or preflight["expected_signals"] < 1
            or preflight.get("accepted_signals") != preflight["expected_signals"]
            or preflight.get("failures") != []
            or not isinstance(preflight.get("artifacts"), list)):
        raise ValueError("candidate signal preflight is not source-accepted")
    matches = [row for row in preflight["artifacts"] if isinstance(row, dict)
               and row.get("session_date") == day and row.get("market") == market]
    if len(matches) != 1:
        raise ValueError("candidate signal scope is missing or ambiguous")
    item = matches[0]
    pins = [preflight_pin]
    for field in ("summary", "weights", "open_input"):
        path = Path(str(item.get(field + "_path") or ""))
        if not path.is_absolute():
            raise ValueError("candidate signal source path must be absolute")
        pin = _pin(path)
        if pin["sha256"] != item.get(field + "_sha256"):
            raise ValueError("candidate signal source changed since preflight")
        pins.append(pin)
    summary = _json(Path(item["summary_path"]))
    if (summary.get("market") != market or summary.get("signal_id") != item.get("signal_id")
            or summary.get("counterfactual_signal_regeneration") is not True
            or not isinstance(summary.get("checkpoint_fingerprint"), str)
            or not summary["checkpoint_fingerprint"]):
        raise ValueError("candidate signal identity or retrospective label mismatch")
    weights = pl.read_parquet(item["weights_path"])
    if not {"symbol", "date", "target_weight"} <= set(weights.columns) or not weights.height:
        raise ValueError("candidate signal weights schema is incomplete")
    weights = weights.select(pl.col("symbol").cast(pl.String), pl.col("date").cast(pl.String),
                             pl.col("target_weight").cast(pl.Float64, strict=False))
    if (weights.filter(pl.col("symbol").is_null() | (pl.col("symbol").str.strip_chars() == "")
                       | pl.col("date").is_null() | ~pl.col("date").str.starts_with(day + "T")
                       | pl.col("target_weight").is_null() | ~pl.col("target_weight").is_finite()).height
            or weights.select(pl.col("symbol").is_duplicated().any()).item()):
        raise ValueError("candidate signal symbols, dates or weights are invalid")
    symbols = set(weights.filter(pl.col("target_weight") != 0)["symbol"].to_list())
    if not symbols:
        raise ValueError("candidate signal has no nonzero symbols to source")
    if any(_pin(Path(pin["path"])) != pin for pin in pins):
        raise RuntimeError("candidate signal changed while deriving source scope")
    return symbols, pins


def _official_prices(records: list[dict], symbols: set[str]) -> dict[str, dict]:
    result = {}
    for record in records:
        # This validates the exact official URL, response, parser and raw SHA.
        read_official_market_evidence(record, symbols=sorted(symbols))
        content = Path(record["path"]).read_bytes()
        if len(content) != record["size"] or hashlib.sha256(content).hexdigest() != record["sha256"]:
            raise RuntimeError("official market response changed after its receipt check")
        frame, _ = _parse_historical_response_content(
            SPECS[record["dataset"]], date.fromisoformat(record["day"]),
            content, record["response_kind"])
        symbol_key, open_key, close_key = (
            ("證券代號", "開盤價", "收盤價") if record["venue"] == "twse"
            else ("代號", "開盤", "收盤"))
        for row in frame.select(symbol_key, open_key, close_key, "成交股數").to_dicts():
            symbol = str(row[symbol_key]).strip()
            if symbol not in symbols:
                continue
            if symbol in result:
                raise ValueError(f"duplicate official dated venue: {symbol}")
            try:
                values = [float(str(row[key]).replace(",", "").strip())
                          for key in (open_key, close_key, "成交股數")]
            except (ValueError, TypeError) as exc:
                raise ValueError(f"unpriceable official day: {symbol}") from exc
            if any(not math.isfinite(value) or value <= 0 for value in values):
                raise ValueError(f"unpriceable official day: {symbol}")
            result[symbol] = {"open": values[0], "close": values[1],
                              "volume_shares": values[2], "venue": record["venue"]}
    if set(result) != symbols:
        raise ValueError(f"official dated venue/price missing: {sorted(symbols - result.keys())}")
    return result


def normalize_source(raw: pl.DataFrame, *, day: str, symbols: set[str],
                     venues: pl.DataFrame, official: dict[str, dict]) -> tuple[pl.DataFrame, dict]:
    """Use dated venue unit proof; never pad a missing source minute."""
    if not symbols or date.fromisoformat(day).isoformat() != day:
        raise ValueError("a canonical date and non-empty exact symbol set are required")
    needed = {"stock_id", "date", "minute", "open", "high", "low", "close", "volume"}
    if not needed <= set(raw.columns):
        raise ValueError("FinMind native schema incomplete")
    selected = raw.filter(pl.col("stock_id").is_in(sorted(symbols)) & (pl.col("date") == day))
    if set(selected["stock_id"].to_list()) != symbols:
        raise ValueError("FinMind observations missing for requested symbols")
    if selected.select(pl.struct("stock_id", "date", "minute").is_duplicated().any()).item():
        raise ValueError("duplicate native symbol/date/minute")
    if set(official) != symbols:
        raise ValueError("official source does not cover exact symbol set")
    proofs = dated_market_share_proofs(venues)
    if set(proofs["symbol"].to_list()) != symbols:
        raise ValueError("dated venue unit proof incomplete")
    normalized = normalize_finmind(selected, share_proofs=proofs)
    normalized = normalized.filter(pl.col("phase").is_in([
        "regular", "closing_auction", "delayed_closing_auction"]))
    # The ordinary TWSE/TPEx session accepts orders but does not match during
    # 13:25--13:29. Do not silently treat a conflicting row as board liquidity.
    if normalized.filter((pl.col("phase") == "regular")
            & (pl.col("ts").dt.time() > datetime.strptime("13:25", "%H:%M").time())).height:
        raise ValueError("unexpected regular trade in the close-auction window")
    if normalized.filter((pl.col("_invalid_reason") != "")
                         | pl.col("volume_shares").is_null()).height:
        raise ValueError("invalid price, timestamp or unproven share volume")
    if normalized.select(pl.struct("symbol", "ts").is_duplicated().any()).item():
        raise ValueError("ambiguous normalized symbol/minute")
    normalized = normalized.sort("symbol", "ts")
    groups = normalized.group_by("symbol", maintain_order=True).agg(
        pl.col("Open").first().alias("first_open"), pl.col("Close").last().alias("last_close"),
        pl.col("volume_shares").sum().alias("volume_shares"))
    if set(groups["symbol"].to_list()) != symbols:
        raise ValueError("requested symbol has no regular-session observations")
    for row in groups.to_dicts():
        expected = official[row["symbol"]]
        if (not math.isclose(row["first_open"], expected["open"], rel_tol=0, abs_tol=1e-8)
                or not math.isclose(row["last_close"], expected["close"], rel_tol=0, abs_tol=1e-8)
                or row["volume_shares"] > expected["volume_shares"]):
            raise ValueError(f"minute/daily official disagreement: {row['symbol']}")
    first_key = datetime.fromisoformat(f"{day}T09:01:00")
    late_key = datetime.fromisoformat(f"{day}T13:33:00")
    first = set(normalized.filter(pl.col("ts") == first_key)["symbol"].to_list())
    delayed = sorted(set(normalized.filter((pl.col("ts") == late_key)
                      & (pl.col("volume_shares") > 0))["symbol"].to_list()))
    result = normalized.select(
        "symbol", pl.col("ts").dt.date().alias("date"), "ts", "Open", "High", "Low", "Close",
        "volume_shares", pl.when(pl.col("volume_shares") == 0).then(0.0)
        .otherwise(None).cast(pl.Float64).alias("Amount"))
    return result, {"requested_symbols": len(symbols), "source_rows": selected.height,
                    "normalized_rows": result.height, "first_minute_present_symbols": len(first),
                    "first_minute_missing_symbols": sorted(symbols - first),
                    "delayed_1333_symbols": delayed,
                    "missing_minutes_are_unknown_not_zero": True}


def _inputs(day: str, finmind_root: Path, public_root: Path,
            inventory_root: Path | None, *, candidate_signal_preflight: Path | None = None,
            candidate_market: str | None = None) -> tuple[pl.DataFrame, dict, list[dict]]:
    if finmind_root.resolve(strict=True) != (ROOT / "data_finmind/complement").resolve(strict=True):
        raise ValueError("FinMind source is not the canonical complement owner")
    if public_root.resolve(strict=True) != (ROOT / "data_tw_public").resolve(strict=True):
        raise ValueError("official market source is not the canonical public owner")
    if (inventory_root is None) == (candidate_signal_preflight is None):
        raise ValueError("choose exactly one revision inventory or candidate signal preflight")
    if candidate_signal_preflight is not None:
        symbols, pins = _candidate_signal_scope(day, candidate_signal_preflight, candidate_market)
    else:
        inventories = sorted(inventory_root.glob("*/source_inventory.json"))
        if not inventories:
            raise ValueError("no scoped revision inventories")
        pins = []
        symbols: set[str] = set()
        for path in inventories:
            item = _json(path)
            plan = path.with_name("published_revision_plan.json")
            if (item.get("session_date") != day or item.get("ledger_modified") is not False
                    or _json(plan).get("plan_sha256") != item.get("plan_sha256")):
                raise ValueError(f"scoped revision inventory mismatch: {path}")
            values = item.get("minute_symbols")
            if not isinstance(values, list) or any(not isinstance(x, str) or not x for x in values):
                raise ValueError("invalid inventory symbol scope")
            symbols.update(values)
            pins.extend((_pin(path), _pin(plan)))
    finmind_root = finmind_root.resolve(strict=True)
    receipt_path = finmind_root / "receipts/TaiwanStockKBar/all" / f"{day}.json"
    if receipt_path.is_symlink() or not receipt_path.resolve(strict=True).is_relative_to(finmind_root):
        raise ValueError("unsafe FinMind native receipt path")
    receipt = _json(receipt_path)
    if (receipt.get("status") != "complete" or receipt.get("dataset") != "TaiwanStockKBar"
            or receipt.get("data_id") != "" or receipt.get("partition") != day):
        raise ValueError("FinMind native receipt identity mismatch")
    units = receipt.get("volume_units") or {}
    if (units.get("contract") != FINMIND_CASH_INTRADAY_UNIT_CONTRACT
            or units.get("stock_share_unit_status") != "mixed_market_lots_or_shares_preserved"
            or units.get("documented_share_multipliers_by_market") != FINMIND_CASH_INTRADAY_SHARE_MULTIPLIERS
            or units.get("requires_same_symbol_dated_market_evidence") is not True
            or units.get("unit_source") != FINMIND_CASH_INTRADAY_UNIT_SOURCE):
        raise ValueError("FinMind native volume-unit contract is unavailable or changed")
    relative = Path(str(receipt.get("parquet_path") or ""))
    source_path = (finmind_root / relative).resolve(strict=True)
    if relative.is_absolute() or ".." in relative.parts or not source_path.is_relative_to(finmind_root):
        raise ValueError("FinMind source path escapes its owner root")
    receipt_pin, source_pin = _pin(receipt_path), _pin(source_path)
    if (source_pin["sha256"] != receipt.get("sha256")
            or source_pin["size_bytes"] != receipt.get("parquet_size_bytes")):
        raise ValueError("FinMind source differs from its receipt")
    result = validate_parquet(source_path, "TaiwanStockKBar", day)
    if result["rows"] != receipt.get("rows"):
        raise ValueError("FinMind source row count differs from its receipt")
    records = pin_official_market_inputs(public_root, {day})
    if {row["venue"] for row in records} != {"twse", "tpex"}:
        raise ValueError("dated official market source receipts unavailable")
    official = _official_prices(records, symbols)
    venues = pl.concat([read_official_market_evidence(row, symbols=sorted(symbols))
                        for row in records], how="vertical")
    raw = pl.read_parquet(source_path)
    data, summary = normalize_source(raw, day=day, symbols=symbols, venues=venues, official=official)
    pins.extend((receipt_pin, source_pin))
    for record in records:
        official_pin = _pin(Path(record["path"]))
        if (official_pin["sha256"] != record["sha256"]
                or official_pin["size_bytes"] != record["size"]):
            raise RuntimeError("official raw source differs from its dated journal receipt")
        pins.append(official_pin)
    pins.extend(_pin(ROOT / path) for path in (
        "scripts/prepare_finmind_tw_day_trade_repair_source.py",
        "stockagent/data/tw_minute_sources.py", "stockagent/data/tw_minute_unit_evidence.py",
        "downloader/download_tw_public_data.py", "downloader/finmind_storage_objects.py",
        "downloader/finmind_volume_units.py", "downloader/stock_volume_units.py",
        "downloader/parquet_integrity.py"))
    if any(_pin(Path(pin["path"])) != pin for pin in pins):
        raise RuntimeError("an input changed during source projection")
    summary.update({"provider_receipt_fetched_at_utc": receipt.get("fetched_at_utc"),
                    "native_source_sha256": source_pin["sha256"],
                    "historical_availability_not_proven": True})
    return data, summary, pins


def build(*, day: str, finmind_root: Path, public_root: Path,
          inventory_root: Path | None, output_root: Path,
          candidate_signal_preflight: Path | None = None, candidate_market: str | None = None) -> dict:
    workspace = workspace_volume.configuration()
    expected = Path(workspace["mount_point"]) / "build"
    destination = output_root.resolve()
    if not destination.is_relative_to(expected) or destination == expected:
        raise ValueError("private projection must use a new E build subdirectory")
    workspace_volume.admit(required_bytes=128 * 1024**2, c=workspace)
    data, summary, pins = _inputs(day, finmind_root, public_root, inventory_root,
        candidate_signal_preflight=candidate_signal_preflight, candidate_market=candidate_market)
    workspace_volume.admit(required_bytes=max(128 * 1024**2, data.estimated_size() * 2), c=workspace)
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    partition = destination / f"trade_date={day}"
    partition.mkdir(mode=0o700)
    output = partition / "data.parquet"
    data.write_parquet(output, compression="zstd")
    with output.open("rb") as handle:
        os.fsync(handle.fileno())
    workspace_volume.admit(required_bytes=0, c=workspace)
    manifest = {"contract": CONTRACT, "session_date": day,
        "created_at_utc": datetime.now(timezone.utc).isoformat(), "simulation_only": True,
        "production_order_possible": False, "public_export_authorized": False,
        "finmind_root": str(finmind_root.resolve()), "public_root": str(public_root.resolve()),
        "inventory_root": str(inventory_root.resolve()) if inventory_root is not None else None,
        "candidate_signal_preflight": str(candidate_signal_preflight.resolve()) if candidate_signal_preflight else None,
        "candidate_market": candidate_market, "output": _pin(output),
        "source_pins": pins, "summary": summary}
    atomic_write_json(destination / MANIFEST, manifest)
    return verify(destination)


def verify(output_root: Path) -> dict:
    root = output_root.resolve(strict=True)
    if (root / MANIFEST).is_symlink():
        raise ValueError("redirected private replay source manifest")
    manifest = _json(root / MANIFEST)
    if (manifest.get("contract") != CONTRACT or manifest.get("simulation_only") is not True
            or manifest.get("production_order_possible") is not False
            or manifest.get("public_export_authorized") is not False):
        raise ValueError("invalid private replay source contract")
    expected_output = root / f"trade_date={manifest['session_date']}" / "data.parquet"
    if (expected_output.is_symlink() or expected_output.resolve(strict=True) != expected_output
            or manifest.get("output", {}).get("path") != str(expected_output)):
        raise ValueError("private replay projection escaped its source root")
    for pin in [manifest["output"], *manifest["source_pins"]]:
        if _pin(Path(pin["path"])) != pin:
            raise RuntimeError(f"pinned replay input changed: {pin['path']}")
    inventory_root = Path(manifest["inventory_root"]) if manifest.get("inventory_root") else None
    candidate_preflight = (Path(manifest["candidate_signal_preflight"])
                           if manifest.get("candidate_signal_preflight") else None)
    data, summary, pins = _inputs(manifest["session_date"], Path(manifest["finmind_root"]),
        Path(manifest["public_root"]), inventory_root, candidate_signal_preflight=candidate_preflight,
        candidate_market=manifest.get("candidate_market"))
    if (pins != manifest["source_pins"] or summary != manifest["summary"]
            or not data.equals(pl.read_parquet(manifest["output"]["path"]))):
        raise RuntimeError("private replay projection differs from its source reconstruction")
    return {"contract": CONTRACT, "status": "source_verified_not_ledger_applied",
            "root": str(root), "session_date": manifest["session_date"],
            "summary": summary, "source_pins": pins, "output": manifest["output"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "verify"))
    parser.add_argument("--day", required=True)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--inventory-root", type=Path)
    scope.add_argument("--candidate-signal-preflight", type=Path)
    parser.add_argument("--candidate-market")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--finmind-root", type=Path, default=ROOT / "data_finmind/complement")
    parser.add_argument("--public-root", type=Path, default=Path("/srv/stockagent-live/data_tw_public"))
    args = parser.parse_args()
    result = (build(day=args.day, finmind_root=args.finmind_root, public_root=args.public_root,
                    inventory_root=args.inventory_root, output_root=args.output_root,
                    candidate_signal_preflight=args.candidate_signal_preflight,
                    candidate_market=args.candidate_market)
              if args.action == "build" else verify(args.output_root))
    if result["session_date"] != args.day:
        raise ValueError("verified source belongs to a different session")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
