#!/usr/bin/env python3
"""Build receipt-backed historical TW day-trade price-limit snapshots.

The live MIS snapshot is authoritative before an actual opening, but it is not
historically queryable.  Historical replay therefore reconstructs only values
that can be proven from retained official sources:

* TPEx uses the prior official session's explicit next-day reference/up/down.
* TWSE uses the official ex-date reference when present, otherwise the prior
  official close, then the dated exchange tick and fluctuation-limit rules.

Rows without a positive official reference are omitted and reported.  Nothing
is substituted from the target day's high, low, close, or a later market quote.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.rebuild_tw_day_trade_open_price_replay import (  # noqa: E402
    DEFAULT_TPEX_DAILY_OHLCV_PATH,
    DEFAULT_TWSE_DAILY_OHLCV_PATH,
    _atomic_json,
    _official_aggregate_daily_rows,
    _sha256,
)
from stockagent.data.tw_price_rules import limit_price_numpy  # noqa: E402


TAIPEI = ZoneInfo("Asia/Taipei")
DEFAULT_PUBLIC_ROOT = Path("/srv/stockagent-live/data_tw_public")


def _number(value: object) -> float | None:
    try:
        parsed = float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0.0 else None


def _official_sessions(paths: tuple[Path, ...]) -> list[date]:
    values: set[date] = set()
    for path in paths:
        frame = (
            pl.scan_parquet(path)
            .select(pl.col("date").cast(pl.Date, strict=False).alias("date"))
            .filter(pl.col("date").is_not_null())
            .unique()
            .collect()
        )
        values.update(frame.get_column("date").to_list())
    return sorted(values)


def _corporate_references(path: Path) -> dict[tuple[date, str], dict[str, Any]]:
    frame = pl.read_parquet(
        path,
        columns=["date", "symbol", "market", "reference_price", "source_url"],
    )
    return {
        (row["date"], str(row["symbol"])): row
        for row in frame.iter_rows(named=True)
        if _number(row.get("reference_price")) is not None
    }


def _tpex_next_limits(path: Path, previous: date) -> dict[str, dict[str, Any]]:
    def numeric(column: str, alias: str) -> pl.Expr:
        return (
            pl.col(column)
            .cast(pl.String)
            .str.strip_chars()
            .str.replace_all(",", "")
            .cast(pl.Float64, strict=False)
            .alias(alias)
        )

    frame = (
        pl.scan_parquet(path)
        .filter(pl.col("date").cast(pl.String) == previous.isoformat())
        .select(
            pl.col("代號").cast(pl.String).str.strip_chars().alias("symbol"),
            numeric("次日參考價", "reference_price"),
            numeric("次日漲停價", "upper_limit_price"),
            numeric("次日跌停價", "lower_limit_price"),
            pl.col("_url").cast(pl.String).alias("source_url"),
        )
        .collect(engine="streaming")
    )
    output: dict[str, dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        symbol = str(row.get("symbol") or "")
        reference = _number(row.get("reference_price"))
        upper = _number(row.get("upper_limit_price"))
        lower = _number(row.get("lower_limit_price"))
        # 9999.95/0.01 is TPEx's explicit no-price-limit sentinel and is
        # retained verbatim rather than projected through the 10% stock rule.
        if symbol and upper is not None and lower is not None:
            output[symbol] = {
                "reference_price": reference,
                "upper_limit_price": upper,
                "lower_limit_price": lower,
                "source_url": row.get("source_url"),
            }
    return output


def build_snapshot(
    *,
    trading_date: date,
    previous_session: date,
    twse_path: Path,
    tpex_path: Path,
    corporate: Mapping[tuple[date, str], Mapping[str, Any]],
) -> tuple[pl.DataFrame, dict[str, Any]]:
    today = _official_aggregate_daily_rows(twse_path, tpex_path, trading_date)
    previous = _official_aggregate_daily_rows(twse_path, tpex_path, previous_session)
    tpex_explicit = _tpex_next_limits(tpex_path, previous_session)
    rows: list[dict[str, Any]] = []
    omitted: list[str] = []
    sources: dict[str, int] = {}
    prepared_at = datetime.now(TAIPEI).isoformat(timespec="seconds")
    day_array = np.asarray([np.datetime64(trading_date.isoformat(), "D")])
    for symbol, daily in sorted(today.items()):
        venue = "tpex" if daily.get("_official_source") == "tpex_daily_ohlcv" else "twse"
        explicit = tpex_explicit.get(symbol) if venue == "tpex" else None
        if explicit is not None:
            reference = _number(explicit.get("reference_price"))
            if reference is None:
                action = corporate.get((trading_date, symbol))
                prior = previous.get(symbol)
                reference = (
                    _number(action.get("reference_price"))
                    if action is not None
                    else _number(prior.get("close")) if prior is not None else None
                )
            upper = _number(explicit.get("upper_limit_price"))
            lower = _number(explicit.get("lower_limit_price"))
            source = "tpex:prior_session_explicit_next_day_limits"
            source_url = explicit.get("source_url")
        else:
            action = corporate.get((trading_date, symbol))
            prior = previous.get(symbol)
            reference = (
                _number(action.get("reference_price"))
                if action is not None
                else _number(prior.get("close")) if prior is not None else None
            )
            if reference is None:
                omitted.append(symbol)
                continue
            reference_array = np.asarray([reference], dtype=np.float64)
            upper = float(limit_price_numpy(reference_array, 1.10, day_array)[0])
            lower = float(limit_price_numpy(reference_array, 0.90, day_array)[0])
            source = (
                "twse:official_ex_date_reference_plus_dated_limit_rule"
                if action is not None
                else "twse:prior_official_close_plus_dated_limit_rule"
            )
            source_url = action.get("source_url") if action is not None else None
        if not all(
            value is not None and math.isfinite(float(value)) and float(value) > 0.0
            for value in (reference, upper, lower)
        ):
            omitted.append(symbol)
            continue
        sources[source] = sources.get(source, 0) + 1
        rows.append(
            {
                "trading_date": trading_date.isoformat(),
                "symbol": symbol,
                "reference_price": float(reference),
                "upper_limit_price": float(upper),
                "lower_limit_price": float(lower),
                "prepared_at": prepared_at,
                "source": source,
                "source_session_date": previous_session.isoformat(),
                "source_url": str(source_url or ""),
                "historical_reconstruction": True,
            }
        )
    if not rows:
        raise RuntimeError(f"no historical price limits reconstructed for {trading_date}")
    return pl.DataFrame(rows), {
        "session_date": trading_date.isoformat(),
        "previous_official_session": previous_session.isoformat(),
        "official_symbols": len(today),
        "rows": len(rows),
        "omitted_symbols": omitted,
        "source_counts": dict(sorted(sources.items())),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/live/tw_price_limits"))
    parser.add_argument("--twse-daily-ohlcv-path", type=Path, default=DEFAULT_TWSE_DAILY_OHLCV_PATH)
    parser.add_argument("--tpex-daily-ohlcv-path", type=Path, default=DEFAULT_TPEX_DAILY_OHLCV_PATH)
    parser.add_argument(
        "--corporate-action-reference",
        type=Path,
        default=DEFAULT_PUBLIC_ROOT / "tw_corporate_action_reference.parquet",
    )
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("--start-date must not be after --end-date")
    twse_path = args.twse_daily_ohlcv_path.resolve()
    tpex_path = args.tpex_daily_ohlcv_path.resolve()
    corporate_path = args.corporate_action_reference.resolve()
    sessions = _official_sessions((twse_path, tpex_path))
    in_range = [day for day in sessions if start <= day <= end]
    previous_by_day = {
        sessions[index]: sessions[index - 1]
        for index in range(1, len(sessions))
        if start <= sessions[index] <= end
    }
    corporate = _corporate_references(corporate_path)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "created_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "contract": "official_reference_only_historical_price_limit_reconstruction",
        "twse_daily_ohlcv": {"path": str(twse_path), "sha256": _sha256(twse_path)},
        "tpex_daily_ohlcv": {"path": str(tpex_path), "sha256": _sha256(tpex_path)},
        "corporate_action_reference": {
            "path": str(corporate_path),
            "sha256": _sha256(corporate_path),
        },
        "sessions": [],
    }
    for day in in_range:
        target = output_dir / f"{day.isoformat()}.parquet"
        if target.is_file() and not args.overwrite:
            receipt["sessions"].append(
                {"session_date": day.isoformat(), "status": "preserved_existing", "path": str(target), "sha256": _sha256(target)}
            )
            continue
        previous = previous_by_day.get(day)
        if previous is None:
            raise RuntimeError(f"no previous official session for {day}")
        frame, session_receipt = build_snapshot(
            trading_date=day,
            previous_session=previous,
            twse_path=twse_path,
            tpex_path=tpex_path,
            corporate=corporate,
        )
        temporary = target.with_suffix(".parquet.tmp")
        frame.write_parquet(temporary)
        temporary.replace(target)
        session_receipt.update({"status": "built", "path": str(target), "sha256": _sha256(target)})
        receipt["sessions"].append(session_receipt)
        print(json.dumps(session_receipt, ensure_ascii=False, sort_keys=True), flush=True)
    receipt_path = output_dir / "historical_reconstruction_receipt.json"
    _atomic_json(receipt_path, receipt)
    print(json.dumps({"sessions": len(in_range), "receipt": str(receipt_path)}, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
