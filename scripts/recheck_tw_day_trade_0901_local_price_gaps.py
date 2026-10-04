#!/usr/bin/env python3
"""Recheck legacy missing 09:01 prices against retained local minute sources.

This is read-only with respect to the trading ledger.  A recovered price is
source evidence for a later isolated replay, never an exchange/broker fill.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Callable
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.quote_provider import load_local_stock_0901_vwaps  # noqa: E402


Resolver = Callable[..., tuple[dict[str, dict[str, Any]], dict[str, Any]]]


def _first_local_minutes(
    partition: Path, symbols: list[str]
) -> tuple[dict[str, int], str | None]:
    """Inspect availability without treating a later bar as a 09:01 fill."""

    if not partition.is_file() or not symbols:
        return {}, None
    try:
        import polars as pl

        schema = pl.scan_parquet(partition).collect_schema()
        if "symbol" not in schema or "minutes_from_open" not in schema:
            return {}, "missing_symbol_or_minutes_from_open_column"
        rows = (
            pl.scan_parquet(partition)
            .filter(pl.col("symbol").is_in(symbols))
            .group_by("symbol")
            .agg(pl.col("minutes_from_open").min().alias("first_minute"))
            .collect(engine="streaming")
        )
        return {
            str(row["symbol"]): int(row["first_minute"])
            for row in rows.iter_rows(named=True)
            if row["symbol"] is not None and row["first_minute"] is not None
        }, None
    except Exception as exc:
        return {}, type(exc).__name__


def _hft_universe_overlap(
    partition: Path, symbols: list[str]
) -> tuple[list[str], str | None]:
    """Find candidates for raw-tick review; HFT rows are not execution prices."""

    if not partition.is_file() or not symbols:
        return [], None
    try:
        import polars as pl

        if "code" not in pl.scan_parquet(partition).collect_schema():
            return [], "missing_code_column"
        rows = (
            pl.scan_parquet(partition)
            .filter(pl.col("code").is_in(symbols))
            .select(pl.col("code").unique())
            .collect(engine="streaming")
        )
        return sorted(str(code) for code in rows["code"] if code is not None), None
    except Exception as exc:
        return [], type(exc).__name__


def recheck(
    audit_path: Path,
    minute_roots: tuple[Path, ...],
    *,
    resolver: Resolver = load_local_stock_0901_vwaps,
    research_root: Path | None = None,
    hft_root: Path | None = None,
) -> dict[str, Any]:
    raw = audit_path.read_bytes()
    source = json.loads(raw)
    if (
        source.get("schema_version") != 1
        or source.get("claim")
        != "receipt_verified_minute_capacity_only_not_fills_or_replay_promotion"
        or not isinstance(source.get("missing_0901_price_symbols_by_date"), dict)
    ):
        raise ValueError(f"invalid full-history source audit: {audit_path}")
    by_date: dict[str, Any] = {}
    totals: Counter[str] = Counter()
    first_minute_histogram: Counter[str] = Counter()
    for day, raw_symbols in sorted(source["missing_0901_price_symbols_by_date"].items()):
        session = date.fromisoformat(day)
        if (
            not isinstance(raw_symbols, list)
            or not all(isinstance(symbol, str) and symbol for symbol in raw_symbols)
            or raw_symbols != sorted(set(raw_symbols))
        ):
            raise ValueError(f"invalid missing symbol set for {day}")
        resolved, receipt = resolver(
            minute_roots,
            raw_symbols,
            trading_date=session,
        )
        requested = set(raw_symbols)
        unexpected = set(resolved) - requested
        if unexpected:
            raise ValueError(f"local resolver returned unrequested symbols for {day}")
        still_missing = sorted(requested - set(resolved))
        errors = dict(receipt.get("error_counts") or {})
        date_result = {
            "requested_symbol_pairs": len(raw_symbols),
            "locally_resolved_symbol_pairs": len(resolved),
            "still_missing_symbols": still_missing,
            "error_counts": errors,
            "status": "source_error" if errors else "checked",
        }
        if research_root is not None:
            partition = research_root / f"trade_date={day}" / "data.parquet"
            first_minutes, diagnostic_error = _first_local_minutes(partition, still_missing)
            first_minute_counts = Counter(str(value) for value in first_minutes.values())
            absent = len(still_missing) - len(first_minutes)
            if diagnostic_error:
                date_result["first_minute_diagnostic"] = {
                    "status": "source_error",
                    "error": diagnostic_error,
                    "partition": str(partition.resolve()),
                }
                totals["dates_with_first_minute_diagnostic_errors"] += 1
            else:
                if absent:
                    first_minute_counts["absent_symbol_or_partition"] += absent
                date_result["first_minute_diagnostic"] = {
                    "status": "checked",
                    "partition_present": partition.is_file(),
                    "histogram": dict(sorted(first_minute_counts.items())),
                    "exact_0901_but_unresolved_symbols": sorted(
                        symbol for symbol, minute in first_minutes.items() if minute == 1
                    ),
                }
                first_minute_histogram.update(first_minute_counts)
                totals["first_minute_exact_0901_but_unresolved_pairs"] += (
                    first_minute_counts["1"]
                )
        if hft_root is not None:
            hft_partition = hft_root / f"trade_date={day}" / "data.parquet"
            overlap, hft_error = _hft_universe_overlap(hft_partition, still_missing)
            date_result["hft_universe_diagnostic"] = {
                "status": "source_error" if hft_error else "checked",
                "partition_present": hft_partition.is_file(),
                "overlap_symbols_require_raw_tick_validation": overlap,
                "error": hft_error,
            }
            totals["hft_universe_overlap_pairs"] += len(overlap)
            totals["dates_with_hft_universe_diagnostic_errors"] += bool(hft_error)
        by_date[day] = date_result
        totals["requested_symbol_pairs"] += len(raw_symbols)
        totals["locally_resolved_symbol_pairs"] += len(resolved)
        totals["still_missing_symbol_pairs"] += len(still_missing)
        totals["dates_with_source_errors"] += bool(errors)
    report = {
        "schema_version": 1,
        "created_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="seconds"),
        "claim": "local_0901_source_recheck_only_not_fills_or_replay_promotion",
        "source_audit_path": str(audit_path.resolve()),
        "source_audit_sha256": hashlib.sha256(raw).hexdigest(),
        "minute_roots": [str(root.resolve()) for root in minute_roots],
        "totals": dict(totals),
        "by_date": by_date,
    }
    if research_root is not None:
        report["research_root"] = str(research_root.resolve())
        report["first_minute_histogram_for_still_missing"] = dict(
            sorted(first_minute_histogram.items())
        )
    if hft_root is not None:
        report["hft_root"] = str(hft_root.resolve())
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--minute-data-root", type=Path, action="append")
    parser.add_argument(
        "--research-root", type=Path,
        default=Path("data_tw_minute/research_dataset"),
        help="Full-universe partition root used only to diagnose earliest available bars.",
    )
    parser.add_argument(
        "--hft-root", type=Path,
        default=Path("data_tw_microstructure/hft_dataset"),
        help="Top-200 HFT universe checked for overlap only; never used as a 09:01 fill.",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.minute_data_root:
        roots = tuple(path.resolve() for path in args.minute_data_root)
    else:
        from scripts.rebuild_tw_day_trade_open_price_replay import DEFAULT_MINUTE_DATA_ROOTS

        roots = tuple(path.resolve() for path in DEFAULT_MINUTE_DATA_ROOTS)
    report = recheck(
        args.audit, roots,
        research_root=args.research_root.resolve(),
        hft_root=args.hft_root.resolve(),
    )
    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(encoded, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        if args.output.exists():
            raise FileExistsError(args.output)
        args.output.write_text(encoded, encoding="utf-8")
        print(args.output)


if __name__ == "__main__":
    main()
