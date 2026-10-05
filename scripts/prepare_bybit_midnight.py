"""Prepare one fixed, local 00:00 Bybit daily view from pinned retained sources.

No downloader, publication, snapshot, or training is started. Raw minute/funding
and public feature files remain in place. Every daily execution/funding label is
rebuilt because the 00:05 labels cannot be relabelled as midnight executions.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import sys

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "downloader"))
from artifact_io import atomic_write_parquet, sha256_file
from common import atomic_write_text
from materialize_bybit_perpetual_daily import _attach_funding_total_return, _daily_bars, _receipt_utc
from ohlcv_hot_tail import logical_parts
from scripts.prepare_bybit_daily_repairs import DEFAULT_PROBE, attach_announcement, canonical_hash, merge_icx_funding
from stockagent.data.crypto_exchange_scope import validate_bybit_midnight_view

DEFAULT_OUTPUT = "artifacts/cache/bybit_perpetual_daily_0000_repaired"
EXPECTED_SYMBOL_COUNT = 397
ANNOUNCED_SYMBOLS = frozenset({"HFTUSDT", "VINEUSDT", "ICXUSDT"})


def source_inventory(base: Path, probe: Path) -> tuple[list[str], list[Path]]:
    daily = sorted((base / "perpetual_daily").glob("*_features.parquet"))
    symbols = [path.stem.removesuffix("_features") for path in daily]
    if len(symbols) != EXPECTED_SYMBOL_COUNT or not ANNOUNCED_SYMBOLS.issubset(symbols):
        raise ValueError(f"expected unchanged {EXPECTED_SYMBOL_COUNT}-symbol reference universe")
    paths = [*daily, base / "funding/funding_coverage.csv", probe.resolve(strict=True)]
    for symbol in symbols:
        minute = base / "1m" / f"{symbol}_features.parquet"
        funding = base / "funding" / f"{symbol}_funding.parquet"
        if not minute.is_file() or not funding.is_file():
            raise ValueError(f"missing retained minute/funding source: {symbol}")
        paths.extend([*logical_parts(minute), funding])
    if not all(path.is_file() for path in paths):
        raise ValueError("missing midnight source evidence")
    return symbols, sorted(set(paths))


def checked_funding(frame: pl.DataFrame, symbol: str) -> dict:
    """Recover retained v3 coverage exactly as the canonical downloader does.

    A current-universe CSV may omit a delisted symbol, while its retained v3
    file still carries the proven head and download horizon. Do not extend that
    horizon to the last minute, publication date, or an assumed funding grid.
    """
    required = {"symbol", "category", "bybit_funding_contract_version",
                "funding_timestamp_ms", "funding_time_utc", "funding_rate",
                "funding_mark_price", "funding_coverage_start_utc",
                "download_snapshot_utc", "funding_prefix_quarantined_events"}
    if frame.is_empty() or required - set(frame.columns):
        raise ValueError(f"missing retained funding coverage: {symbol}")
    for field, value in (("symbol", symbol), ("category", "linear"), ("bybit_funding_contract_version", 3)):
        if set(frame[field].to_list()) != {value}:
            raise ValueError(f"incompatible funding identity/contract: {symbol}")
    if frame["funding_timestamp_ms"].n_unique() != frame.height:
        raise ValueError(f"duplicate funding event: {symbol}")
    rate = frame["funding_rate"].to_numpy()
    mark = frame["funding_mark_price"].to_numpy()
    if not np.isfinite(rate).all() or not np.isfinite(mark).all() or (mark <= 0).any():
        raise ValueError(f"invalid funding rate/mark: {symbol}")
    start = str(frame["funding_coverage_start_utc"].max())
    end = str(frame["download_snapshot_utc"].max())
    start_dt, end_dt = _receipt_utc(start), _receipt_utc(end)
    if start_dt >= end_dt:
        raise ValueError(f"invalid funding coverage horizon: {symbol}")
    for stamp, text in zip(frame["funding_timestamp_ms"], frame["funding_time_utc"]):
        parsed = _receipt_utc(text)
        if int(parsed.timestamp() * 1000) != stamp or not start_dt <= parsed <= end_dt:
            raise ValueError(f"funding event outside retained coverage: {symbol}")
    return {"head_complete": True, "coverage_start_utc": start, "coverage_end_utc": end}


def build_symbol(base: Path, symbol: str, coverage: dict, document: dict, events: dict) -> tuple[pl.DataFrame, dict]:
    funding_path = base / "funding" / f"{symbol}_funding.parquet"
    funding = pl.read_parquet(funding_path)
    retained = checked_funding(funding, symbol)
    row = coverage.get(symbol)
    if row is not None:
        if str(row.get("head_complete")).lower() not in {"true", "1", "yes"}:
            raise ValueError(f"funding coverage is not head-complete: {symbol}")
        if row.get("sha256") != sha256_file(funding_path):
            raise ValueError(f"funding coverage hash mismatch: {symbol}")
        for key in ("coverage_start_utc", "coverage_end_utc"):
            if _receipt_utc(row[key]) != _receipt_utc(retained[key]):
                raise ValueError(f"funding coverage metadata conflict: {symbol}")
    elif symbol not in ANNOUNCED_SYMBOLS:
        raise ValueError(f"unexpected missing current funding coverage: {symbol}")
    details = {"coverage_origin": "current_receipt" if row is not None else "retained_v3_metadata"}
    if symbol == "ICXUSDT":
        funding, repair = merge_icx_funding(funding, document)
        retained = repair["coverage"]
        details["funding_repair"] = repair
    daily, incomplete, unavailable = _daily_bars(base / "1m" / f"{symbol}_features.parquet", execution_minutes_utc=0)
    frame, executable, funding_events = _attach_funding_total_return(daily, funding, retained, execution_minutes_utc=0)
    if frame.is_empty() or frame["date"].n_unique() != frame.height or not frame["date"].is_sorted():
        raise ValueError(f"invalid midnight daily dates: {symbol}")
    if set(frame["bybit_perpetual_contract_version"]) != {7} or set(frame["daily_boundary_utc"]) != {"00:00"}:
        raise ValueError(f"wrong midnight execution contract: {symbol}")
    if symbol in events:
        frame = attach_announcement(frame, events[symbol])
    details.update(rows=frame.height, first_date=frame["date"].min(), last_date=frame["date"].max(),
                   executable_return_rows=executable, funding_events=funding_events,
                   incomplete_feature_sessions_retained=incomplete, execution_sessions_excluded=unavailable,
                   unvalued_forward_rows=int(frame["return_quarantined"].sum()), funding_coverage=retained)
    return frame, details


def prepare(base: Path, probe_path: Path, output: Path, *, workers: int = 4) -> dict:
    base = base.resolve(strict=True)
    output = output.absolute()
    cache = (ROOT / "artifacts/cache").resolve()
    if not 1 <= workers <= 4:
        raise ValueError("workers must be between 1 and 4")
    if output.is_symlink() or not output.resolve().is_relative_to(cache) or output.resolve() == cache:
        raise ValueError("output must be a bounded nonsymlink child of artifacts/cache")
    if output.resolve().is_relative_to(base) or base.is_relative_to(output.resolve()):
        raise ValueError("midnight view must not overlap the canonical source")
    symbols, paths = source_inventory(base, probe_path)
    receipt_path = output / "midnight_manifest.json"
    if output.exists():
        if not receipt_path.is_file():
            raise ValueError("existing output lacks completed midnight receipt; will not overwrite")
        validate_bybit_midnight_view(output / "perpetual_daily", venue_root=base)
        receipt = json.loads(receipt_path.read_text())
        if set(receipt["sources_sha256"]) != {str(path) for path in paths}:
            raise ValueError("midnight source inventory changed; explicit rebuild review required")
        return receipt
    # Pin bytes before loading any evidence into memory; an edit between the
    # initial hash and the reads is then caught by the final full rehash.
    print(f"Hashing {len(paths)} source files ({sum(path.stat().st_size for path in paths)} bytes)", flush=True)
    sources = {str(path): sha256_file(path) for path in paths}
    document = json.loads(probe_path.read_text())
    events = {event["metadata"]["symbol"]: event for event in document["official_announcements"]}
    if set(events) != ANNOUNCED_SYMBOLS or len(document["official_announcements"]) != len(events):
        raise ValueError("unexpected announcement repair scope")
    coverage_frame = pl.read_csv(base / "funding/funding_coverage.csv", infer_schema_length=10_000)
    if coverage_frame["symbol"].n_unique() != coverage_frame.height:
        raise ValueError("duplicate funding coverage symbol")
    coverage = {row["symbol"]: row for row in coverage_frame.to_dicts()}
    panel = output / "perpetual_daily"
    panel.mkdir(parents=True, exist_ok=False)
    derived, details = {}, {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(build_symbol, base, symbol, coverage, document, events): symbol for symbol in symbols}
        for index, future in enumerate(as_completed(futures), 1):
            symbol = futures[future]
            frame, detail = future.result()
            target = panel / f"{symbol}_features.parquet"
            atomic_write_parquet(target, frame, compression="snappy", write_statistics=True)
            derived[str(target.relative_to(output))] = sha256_file(target)
            details[symbol] = detail
            if index % 20 == 0 or index == len(symbols):
                print(f"Midnight daily {index}/{len(symbols)} complete", flush=True)
    _, final_paths = source_inventory(base, probe_path)
    if final_paths != paths or any(sha256_file(path) != sources[str(path)] for path in paths):
        raise ValueError("source changed while building midnight view; no completed receipt installed")
    receipt = {"schema_version": 1, "contract_version": 7, "base_root": str(base), "base_release": base.name,
               "decision_cutoff_utc": "00:00", "execution_boundary_utc": "00:00",
               "research_latency_assumption": "zero; boundary funding settles before new target; held interval (start,end]",
               "reference_symbols": symbols, "reference_universe_sha256": canonical_hash(symbols),
               "source_probe": str(probe_path.resolve()),
               "sources_sha256": sources, "derived_sha256": derived, "symbols": details,
               "announced_symbols": sorted(ANNOUNCED_SYMBOLS),
               "storage_contract": "local derived daily labels only; no raw/public duplication or source mutation",
               "public_feature_contract": "reuse existing 00:00-cutoff information, including prior-day 00:05-normalized funding information; not midnight execution labels",
               "exit_contract": "next-day-known announcement; constrained exit request, not synthetic settlement",
               "missing_held_valuation": "raises; no prices or funding events synthesized"}
    atomic_write_text(receipt_path, json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-root", default="data_bybit")
    parser.add_argument("--source-probe", default=DEFAULT_PROBE)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    receipt = prepare(Path(args.base_root), Path(args.source_probe), Path(args.output_root), workers=args.workers)
    print(json.dumps({"base_release": receipt["base_release"], "symbols": len(receipt["symbols"]),
                      "contract_version": receipt["contract_version"], "output": args.output_root}, indent=2))


if __name__ == "__main__":
    main()
