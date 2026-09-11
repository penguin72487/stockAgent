#!/usr/bin/env python3
"""Build receipt-backed single-stock-futures 09:00 entries or 08:46 minute tape.

The 08:46 policy accepts receipt-backed one-minute KBars via --minute-root.
The legacy input is an archive of official TAIFEX ``Daily_YYYY_MM_DD.zip`` futures
transaction files.  Every date in the requested daily-source interval must
have one ZIP before the manifest is marked complete.  A covered date may
legitimately produce no entry row for a selected contract; a missing ZIP is a
coverage failure and cannot be confused with no trading after 09:00.
"""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile
from typing import Final

import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.download_taifex_recent_index_derivatives_ticks import _parse_zip
from stockagent.data.tw_stock_futures_day_trade import (
    TAIFEX_STOCK_FUTURES_0900_ENTRY_DATA_CONTRACT_VERSION,
    _REQUIRED_COLUMNS,
    select_causal_front_stock_futures,
    select_causal_front_stock_futures_candidates,
)
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_stock_futures_minute import (
    MINUTE_CONTRACT_VERSION, MINUTE_DATASET, build_futures_minute_bars,
)
from stockagent.data.tw_futures_portfolio_daily import TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION


DATASET: Final[str] = "taifex_stock_futures_0900_entry_v1"
ARCHIVE_RE: Final[re.Pattern[str]] = re.compile(
    r"^Daily_(\d{4})_(\d{2})_(\d{2})\.zip$"
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
    try:
        frame.write_parquet(temporary, compression="zstd", statistics=True)
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_write_json(payload: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        mode="w",
        encoding="utf-8",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        handle.write(text)
    try:
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _archive_inventory(root: Path) -> dict[date, Path]:
    result: dict[date, Path] = {}
    for path in sorted(root.rglob("Daily_????_??_??.zip")):
        match = ARCHIVE_RE.match(path.name)
        if match is None:
            continue
        trading_date = date(
            int(match.group(1)), int(match.group(2)), int(match.group(3))
        )
        previous = result.setdefault(trading_date, path)
        if previous != path:
            raise ValueError(
                f"duplicate TAIFEX futures ZIPs for {trading_date}: "
                f"{previous}, {path}"
            )
    return result


def _first_strictly_later_entries(
    transactions: pl.DataFrame,
    selected: pl.DataFrame,
) -> pl.DataFrame:
    """Select one deterministic post-09:00 public trade row per contract."""

    selected_keys = selected.select("date", "physical_contract").rename(
        {"date": "trading_date"}
    )
    return (
        transactions.filter(
            (pl.col("session") == "day")
            & (pl.col("event_date") == pl.col("trading_date"))
            & (pl.col("event_time").cast(pl.Int32) > 90000)
            & (pl.col("event_time").cast(pl.Int32) <= 90059)
            & (~pl.col("delivery_month_week").str.contains("/", literal=True))
        )
        .with_columns(
            pl.concat_str(
                [pl.col("product"), pl.lit(":"), pl.col("delivery_month_week")]
            ).alias("physical_contract"),
            pl.col("event_time").cast(pl.Int32).alias("entry_time_hhmmss"),
        )
        .join(
            selected_keys,
            on=["trading_date", "physical_contract"],
            how="inner",
            validate="m:1",
        )
        .sort(
            ["trading_date", "physical_contract", "event_ts", "source_row_number"],
            maintain_order=True,
        )
        .unique(
            subset=["trading_date", "physical_contract"],
            keep="first",
            maintain_order=True,
        )
        .select(
            pl.col("trading_date").alias("date"),
            "physical_contract",
            "entry_time_hhmmss",
            pl.col("price").cast(pl.Float64).alias("entry_price"),
            pl.col("matched_quantity").cast(pl.Float64),
            pl.lit(True).alias("source_row_observed"),
            "source_sha256",
        )
        .rename({"source_sha256": "source_file_sha256"})
        .sort("date", "physical_contract")
    )


def _empty_output() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "date": pl.Date,
            "physical_contract": pl.String,
            "entry_time_hhmmss": pl.Int32,
            "entry_price": pl.Float64,
            "matched_quantity": pl.Float64,
            "source_row_observed": pl.Boolean,
            "source_file_sha256": pl.String,
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path,
                        help="08:45 training config; inherit daily source, output and panel start date.")
    parser.add_argument("--check-only", action="store_true",
                        help="Read-only source check (KBar receipts/content or legacy ZIP inventory).")
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--shioaji-ticks-root", type=Path,
                         help="Continuous Shioaji history; validate dated physical identity before aggregation.")
    parser.add_argument("--daily-proxy-before", default=None,
                        help="Explicit exclusive cutoff for early futures daily OPEN-to-CLOSE approximation.")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--assemble-cached", action="store_true",
                        help="Assemble the existing SHA-verified dated shards without refreshing their raw-source snapshot.")
    parser.add_argument("--work-dir", type=Path, default=Path("artifacts/cache/futures_minute_history"))
    parser.add_argument('--repair-root', type=Path, help='Canonical exact-month KBar gap-repair workspace.')
    parser.add_argument('--official-evidence-dir', type=Path, help='SHA-bound complete official daily/spread evidence for repair.')
    parser.add_argument('--refresh-dates', nargs='+', help='Refresh only these dates; preserve every other SHA-verified cached shard.')
    parser.add_argument('--capacity-participation', type=float, help='Prove zero integer capacity from an official daily volume upper bound.')
    parser.add_argument('--capacity-rounding', choices=['floor', 'ceil'], default=None,
                        help='Minute participation rounding; ceil requires actual minute evidence for positive volume.')
    parser.add_argument('--quarantine-dates', nargs='*', help='Explicit user-authorized excluded decision dates; retain their unresolved evidence.')
    parser.add_argument('--quarantine-contract-days', type=json.loads,
                        help='Explicit JSON list of {date, physical_contract}; preserve other contracts and dates.')
    sources.add_argument("--minute-root", type=Path,
                         help="Existing Shioaji historical collector root; read only one-minute KBar chunks.")
    sources.add_argument(
        "--ticks-root",
        type=Path,
        help="Archive root containing official Daily_YYYY_MM_DD.zip files.",
    )
    parser.add_argument(
        "--daily-data-path",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
    )
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--execution-policy", choices=("post_0900", "scheduled_0846"), default=None)
    parser.add_argument("--archive-override", type=Path, action="append", default=[],
                        help="Explicit immutable corrected Daily_YYYY_MM_DD.zip; original source is preserved.")
    args = parser.parse_args()
    if args.config is not None:
        from stockagent.config import load_config
        from stockagent.data.tw_stock_futures_minute import MINUTE_MODE

        config = load_config(args.config)
        if args.capacity_participation is None:
            args.capacity_participation = config.trading.max_volume_participation
        if args.capacity_rounding is None:
            args.capacity_rounding = config.trading.tw_stock_futures_day_trade_minute_capacity_rounding
        if args.quarantine_dates is None:
            args.quarantine_dates = config.trading.tw_stock_futures_day_trade_quarantine_dates
        if args.quarantine_contract_days is None:
            args.quarantine_contract_days = config.trading.tw_stock_futures_day_trade_quarantine_contract_days
        if config.trading.execution_mode != MINUTE_MODE:
            parser.error("--config must select the 08:45 futures minute execution mode")
        if args.execution_policy not in (None, "scheduled_0846"):
            parser.error("--config cannot be combined with a different execution policy")
        args.execution_policy = "scheduled_0846"
        args.daily_data_path = args.daily_data_path or Path(config.trading.tw_stock_futures_day_trade_data_path)
        minute_path = Path(config.trading.tw_stock_futures_day_trade_minute_data_path)
        if args.output_dir is None and minute_path.name != "minutes.parquet":
            parser.error("configured minute output must be named minutes.parquet")
        args.output_dir = args.output_dir or minute_path.parent
        args.start_date = args.start_date or config.data.panel_start_date
        args.daily_proxy_before = args.daily_proxy_before or config.trading.tw_stock_futures_day_trade_daily_proxy_before
    args.execution_policy = args.execution_policy or "post_0900"
    args.capacity_rounding = args.capacity_rounding or 'floor'
    if args.capacity_participation is not None and not 0 < args.capacity_participation <= 1:
        parser.error('--capacity-participation must be within (0,1]')
    if args.assemble_cached and args.shioaji_ticks_root is None:
        parser.error("--assemble-cached requires --shioaji-ticks-root")
    if args.shioaji_ticks_root is not None:
        if args.execution_policy != "scheduled_0846" or not args.daily_proxy_before or args.workers < 1:
            parser.error("continuous ticks require scheduled_0846, explicit --daily-proxy-before and positive workers")
        date.fromisoformat(args.daily_proxy_before)
    if args.minute_root is not None and (args.execution_policy != "scheduled_0846" or args.archive_override):
        parser.error("--minute-root requires the scheduled_0846 policy and cannot use archive overrides")
    args.daily_data_path = args.daily_data_path or Path(
        "data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet"
    )
    args.output_dir = args.output_dir or Path(
        "data_tw_futures/taifex_stock_futures_minute_v1"
        if args.execution_policy == "scheduled_0846"
        else "data_tw_futures/taifex_stock_futures_0900_v1"
    )
    args.start_date = args.start_date or "2014-01-01"
    return args


def main() -> int:
    args = parse_args()
    start = date.fromisoformat(str(args.start_date))
    end = date.fromisoformat(str(args.end_date)) if args.end_date else date.max
    if start > end:
        raise ValueError("start-date must not be after end-date")
    if not getattr(args, "check_only", False):
        output = args.output_dir.resolve()
        for parent in (output, *output.parents):
            if (parent.name in {"stockagent-packed", "stockagent-packed-materialized"}
                    or (parent.parent / f".{parent.name}.READY.json").is_file()):
                raise ValueError(
                    f"builder output is immutable packed/materialized data: {output}; "
                    "set --output-dir to the catalog's writable tw-futures source workspace, "
                    "then publish a new release and update the training config"
                )
    if not args.daily_data_path.is_file():
        raise FileNotFoundError(args.daily_data_path)
    if (args.execution_policy != "scheduled_0846"
            and not getattr(args, "check_only", False) and not args.ticks_root.is_dir()):
        raise FileNotFoundError(args.ticks_root)

    daily_digest = sha256_file(args.daily_data_path)
    if args.execution_policy == "scheduled_0846":
        daily_manifest = json.loads(args.daily_data_path.with_name("manifest.json").read_text())
        if (daily_manifest.get("contract_version") != TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
                or daily_manifest.get("outputs", {}).get("continuous_daily", {}).get("sha256") != daily_digest):
            raise ValueError("daily candidate source contract or SHA mismatch")
    source = pl.from_arrow(
        pq.read_table(
            args.daily_data_path,
            columns=(list(dict.fromkeys([*_REQUIRED_COLUMNS, "contract", "shioaji_roots", "high", "low"]))
                     if getattr(args, "shioaji_ticks_root", None) is not None else list(_REQUIRED_COLUMNS)),
            filters=[("asset_class", "=", "stock_future")],
            memory_map=True,
        )
    ).filter(
        (pl.col("date") >= pl.lit(start)) & (pl.col("date") <= pl.lit(end))
    )
    minute_policy = args.execution_policy == "scheduled_0846"
    if minute_policy and args.output_dir == Path("data_tw_futures/taifex_stock_futures_0900_v1"):
        args.output_dir = Path("data_tw_futures/taifex_stock_futures_minute_v1")
    expected_dates = source["date"].unique().sort().to_list()
    if not expected_dates:
        raise ValueError("daily source has no stock-futures sessions in range")
    if getattr(args, "shioaji_ticks_root", None) is not None:
        from stockagent.data.tw_stock_futures_history import build_continuous_history
        return build_continuous_history(args, source, expected_dates, daily_digest)
    if getattr(args, "minute_root", None) is not None:
        return _build_from_kbars(args, source, expected_dates, daily_digest)
    archives = _archive_inventory(args.ticks_root)
    override_dates = set()
    for archive in args.archive_override:
        match = ARCHIVE_RE.fullmatch(archive.name)
        if not archive.is_file() or match is None:
            raise ValueError(f"invalid explicit archive revision: {archive}")
        revised_date = date(*(int(match.group(i)) for i in (1, 2, 3)))
        if revised_date not in expected_dates or revised_date in override_dates:
            raise ValueError(f"duplicate or out-of-range archive revision: {archive}")
        override_dates.add(revised_date)
        archives[revised_date] = archive
    missing_dates = [value for value in expected_dates if value not in archives]
    inventory_report = {
        "status": "partial" if missing_dates else "source_inventory_complete",
        "stage": "archive_inventory", "ticks_root": str(args.ticks_root),
        "requested_start": str(start), "requested_end": str(end),
        "session_start": str(expected_dates[0]), "session_end": str(expected_dates[-1]),
        "expected_sessions": len(expected_dates),
        "available_archives": len(expected_dates) - len(missing_dates),
        "missing_dates": list(map(str, missing_dates)), "incomplete_sessions": [],
        "source_daily_sha256": daily_digest,
        "contents_validated": False,
    }
    if getattr(args, "check_only", False):
        print(json.dumps(inventory_report, ensure_ascii=False, indent=2), flush=True)
        return 2 if missing_dates else 0
    if minute_policy and missing_dates:
        atomic_write_json(args.output_dir / "build_failure.json", inventory_report)
        print(
            f"[futures minutes] rejected before ZIP parsing: missing {len(missing_dates)}/"
            f"{len(expected_dates)} sessions ({missing_dates[0]}..{missing_dates[-1]}); "
            f"full inventory: {args.output_dir / 'build_failure.json'}",
            flush=True,
        )
        return 2

    selected = (select_causal_front_stock_futures_candidates(source) if minute_policy
                else select_causal_front_stock_futures(source))
    if selected.is_empty():
        raise ValueError("daily source has no selected stock futures in range")
    products = tuple(sorted(str(value) for value in selected["product"].unique()))

    frames: list[pl.DataFrame] = []
    source_receipts: list[dict] = []
    incomplete_sessions: list[str] = []
    for index, trading_date in enumerate(expected_dates, 1):
        archive = archives.get(trading_date)
        if archive is None:
            continue
        source_sha256 = _sha256_file(archive)
        transactions = _parse_zip(
            archive,
            kind="futures",
            trading_date=trading_date,
            source_sha256=source_sha256,
            futures_products=products,
            futures_outright_contracts_only=True,
        )
        selected_date = selected.filter(pl.col("date") == pl.lit(trading_date))
        entries = (build_futures_minute_bars(transactions) if minute_policy
                   else _first_strictly_later_entries(transactions, selected_date))
        if sha256_file(archive) != source_sha256:
            raise ValueError(f"source archive changed while parsing: {archive}")
        day_rows = transactions.filter(
            (pl.col("session") == "day") & (pl.col("event_date") == pl.lit(trading_date))
        )
        last_time = int(day_rows["event_time"].cast(pl.Int32).max() or 0)
        if minute_policy and (day_rows.is_empty() or last_time < 133000):
            incomplete_sessions.append(str(trading_date))
        source_receipts.append({"date": str(trading_date), "path": str(archive), "sha256": source_sha256,
                                "day_session_rows": day_rows.height, "day_last_time": last_time})
        if minute_policy or not entries.is_empty():
            frames.append(entries)
        if minute_policy:
            print(f"[futures minutes] {index}/{len(expected_dates)} {trading_date} rows={entries.height}", flush=True)

    if minute_policy:
        if sha256_file(args.daily_data_path) != daily_digest:
            raise ValueError("daily candidate source changed during minute build")
        output_path = args.output_dir / "minutes.parquet"
        # An incomplete attempt must not replace an accepted data/manifest pair.
        if missing_dates or incomplete_sessions:
            atomic_write_json(args.output_dir / "build_failure.json", {
                "status": "partial", "missing_dates": list(map(str, missing_dates)),
                "incomplete_sessions": incomplete_sessions,
                "requested_start": str(start), "requested_end": str(end),
            })
            print(f"[futures minutes] rejected missing_dates={list(map(str, missing_dates))} incomplete_sessions={incomplete_sessions}", flush=True)
            return 2
        output = pl.concat(frames, how="vertical_relaxed")
        atomic_write_parquet(output_path, output)
        atomic_write_json(args.output_dir / "manifest.json", {
            "dataset": MINUTE_DATASET, "contract_version": MINUTE_CONTRACT_VERSION,
            "status": "complete", "timezone": "Asia/Taipei",
            "source_daily_path": str(args.daily_data_path), "source_daily_sha256": daily_digest,
            "covered_dates": [str(d) for d in expected_dates], "sources": source_receipts,
            "clock": "right_labelled_minutes_[label-1min,label)",
            "quantity": "official_matched_contracts_B_plus_S_divided_by_two",
            "execution_claim": "historical_minute_vwap_not_order_book_or_guaranteed_fill",
            "rows": output.height,
            "outputs": {"minutes": {"path": str(output_path), "sha256": sha256_file(output_path)}},
        })
        print(f"[futures minutes] complete dates={len(expected_dates)} rows={output.height} output={output_path}", flush=True)
        return 0

    output = pl.concat(frames, how="vertical_relaxed") if frames else _empty_output()
    output_path = args.output_dir / "entry_0900.parquet"
    _atomic_write_parquet(output, output_path)
    status = "complete" if not missing_dates else "partial"
    manifest: dict[str, object] = {
        "dataset": DATASET,
        "contract_version": int(
            TAIFEX_STOCK_FUTURES_0900_ENTRY_DATA_CONTRACT_VERSION
        ),
        "status": status,
        "timezone": "Asia/Taipei",
        "decision_time": "09:00:00",
        "entry_rule": (
            "first_strictly_later_public_trade_row_through_09:00:59"
        ),
        "same_second_tie_break": (
            "official_source_row_order_because_public_file_has_no_subsecond_clock"
        ),
        "execution_claim": "historical_trade_price_proxy_not_quote_or_depth_fill",
        "coverage": {
            "start": expected_dates[0].isoformat(),
            "end": expected_dates[-1].isoformat(),
            "expected_trading_dates": len(expected_dates),
            "covered_trading_dates": len(expected_dates) - len(missing_dates),
            "missing_trading_dates": [value.isoformat() for value in missing_dates],
        },
        "selection": (
            "causal_nearby_physical_contract_from_taifex_portfolio_daily_v4"
        ),
        "source_daily_path": str(args.daily_data_path),
        "source_daily_sha256": _sha256_file(args.daily_data_path),
        "source_ticks_root": str(args.ticks_root),
        "rows": output.height,
        "outputs": {
            "entry_0900": {
                "path": str(output_path),
                "sha256": _sha256_file(output_path),
            }
        },
    }
    _atomic_write_json(manifest, args.output_dir / "manifest.json")
    print(
        f"[tw-stock-futures-0900] status={status} rows={output.height:,} "
        f"covered={len(expected_dates) - len(missing_dates)}/{len(expected_dates)} "
        f"output={output_path}",
        flush=True,
    )
    return 0 if status == "complete" else 2


def _build_from_kbars(args, source: pl.DataFrame, expected_dates: list[date], daily_digest: str) -> int:
    from stockagent.data.tw_stock_futures_kbars import KBAR_SOURCE, read_futures_kbar_sources
    from stockagent.data.tw_stock_futures_minute import validate_futures_minute_data

    selected = select_causal_front_stock_futures_candidates(source)
    if selected.is_empty():
        raise ValueError("daily source has no selected stock futures in range")
    output, sources, missing = read_futures_kbar_sources(args.minute_root, selected, expected_dates)
    report = {
        "status": "partial" if missing else "source_inventory_complete",
        "stage": "one_minute_kbars", "minute_root": str(args.minute_root),
        "expected_sessions": len(expected_dates), "selected_contract_days": selected.height,
        "session_start": str(expected_dates[0]), "session_end": str(expected_dates[-1]),
        "missing_contract_days": len(missing), "missing": missing,
        "source_daily_sha256": daily_digest, "contents_validated": not missing,
    }
    if args.check_only or missing:
        if missing and not args.check_only:
            atomic_write_json(args.output_dir / "build_failure.json", report)
        print(json.dumps({**report, "missing": missing[:20]}, ensure_ascii=False, indent=2), flush=True)
        return 2 if missing else 0
    if sha256_file(args.daily_data_path) != daily_digest:
        raise ValueError("daily candidate source changed during KBar build")
    # Validate the complete pair before replacing an accepted output.
    with tempfile.TemporaryDirectory(prefix="futures-kbars-") as temporary:
        staged = Path(temporary) / "minutes.parquet"
        atomic_write_parquet(staged, output)
        manifest = {
            "dataset": MINUTE_DATASET, "contract_version": MINUTE_CONTRACT_VERSION,
            "source_kind": KBAR_SOURCE, "status": "complete", "timezone": "Asia/Taipei",
            "source_daily_path": str(args.daily_data_path), "source_daily_sha256": daily_digest,
            "covered_dates": list(map(str, expected_dates)), "sources": sources,
            "clock": "right_labelled_minutes_[label-1min,label)",
            "quantity": "contracts", "price": "Amount/Volume",
            "execution_claim": "historical_minute_vwap_not_order_book_or_guaranteed_fill",
            "rows": output.height,
            "outputs": {"minutes": {"sha256": sha256_file(staged)}},
        }
        atomic_write_json(staged.with_name("manifest.json"), manifest)
        validate_futures_minute_data(staged, daily_sha256=daily_digest)
        output_path = args.output_dir / "minutes.parquet"
        atomic_write_parquet(output_path, output)
        manifest["outputs"]["minutes"] = {"path": str(output_path), "sha256": sha256_file(output_path)}
        atomic_write_json(args.output_dir / "manifest.json", manifest)
    print(f"[futures minutes] complete source=kbars dates={len(expected_dates)} rows={output.height} output={output_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
