#!/usr/bin/env python3
"""Resumably download all available ticks for one continuous futures alias."""

from __future__ import annotations

import argparse
import copy
from datetime import UTC, date, datetime, timedelta
import fcntl
import json
import os
from pathlib import Path
import sys
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from downloader.common import (
        SharedRateLimiter,
        describe_rate_limit,
        resolve_request_interval,
    )
except ModuleNotFoundError:  # direct script execution
    from common import SharedRateLimiter, describe_rate_limit, resolve_request_interval
from stockagent.live.shioaji_traffic_ledger import shioaji_query
from stockagent.live.shioaji_schedule import (
    HISTORICAL_MAX_TRAFFIC_FRACTION,
)

from downloader.download_shioaji_tw_kbars import (
    TrafficBudgetReached,
    _atomic_write_json,
    _check_traffic_budget,
    _sha256,
    _taiwan_market_hours_now,
    _write_parquet_atomic,
)
from downloader.shioaji_history_repair import (
    DEFAULT_FUTURES_ACTIVITY, FuturesActivity, checked_time,
    load_futures_activity, parsed_time, retry_due, retry_metadata, utc_stamp, verified_sha,
    futures_date_is_closed,
)


SOURCE = "shioaji_continuous_futures_historical_ticks_v1"
LEGACY_TX_SOURCE = "shioaji_txfr1_historical_ticks_v1"
RECEIPT_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 2
CONTRACT_UNAVAILABLE_EXIT = 78
CONNECTION_CAPACITY_EXIT = 79
HISTORY_START = date(2020, 3, 22)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", default="TXFR1")
    parser.add_argument(
        "--calendar-path",
        type=Path,
        default=Path("data_tw_index_futures/day_session_contracts.parquet"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_tw_index_futures/shioaji_history/TXFR1"),
    )
    parser.add_argument("--start-date", default=HISTORY_START.isoformat())
    parser.add_argument(
        "--end-date", default=(date.today() - timedelta(days=1)).isoformat()
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=None,
        help=(
            "Host-global seconds between quote requests; defaults to the official "
            "selected 10 requests/second account-wide ceiling."
        ),
    )
    parser.add_argument("--timeout-ms", type=int, default=120_000)
    parser.add_argument(
        "--max-traffic-fraction",
        type=float,
        default=HISTORICAL_MAX_TRAFFIC_FRACTION,
    )
    parser.add_argument("--simulation", action="store_true")
    parser.add_argument("--allow-market-hours", action="store_true")
    parser.add_argument("--max-dates", type=int, default=0)
    parser.add_argument("--oldest-first", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument('--contracts-file', type=Path, help='Batch all retained aliases with one API login.')
    parser.add_argument('--history-root', type=Path, default=Path('data_tw_futures/shioaji_history'))
    parser.add_argument('--tx-history-root', type=Path, default=Path('data_tw_index_futures/shioaji_history/TXFR1'))
    parser.add_argument('--batch-receipt', type=Path, default=Path('artifacts/data_repair/shioaji_futures_history/latest_batch.json'))
    parser.add_argument('--refresh-inventory', action='store_true')
    parser.add_argument('--refresh-empty', action='store_true')
    parser.add_argument('--official-activity', type=Path, default=DEFAULT_FUTURES_ACTIVITY)
    parser.add_argument('--dates-per-contract', type=int, default=32)
    parser.add_argument('--empty-probes-per-contract', type=int, default=2)
    return parser.parse_args()


def _calendar(path: Path, start: date, end: date) -> list[date]:
    frame = (
        pl.scan_parquet(path)
        .filter(
            (pl.col("product") == "TX")
            & (pl.col("date") >= pl.lit(start))
            & (pl.col("date") <= pl.lit(end))
        )
        .select("date")
        .unique()
        .sort("date")
        .collect()
    )
    return list(frame.get_column("date"))


def _receipt_path(root: Path, trading_date: date) -> Path:
    return root / "receipts" / f"trading_date={trading_date.isoformat()}.json"


def _data_path(root: Path, trading_date: date) -> Path:
    return root / "ticks" / f"trading_date={trading_date.isoformat()}" / "data.parquet"


def _valid_receipt(root: Path, trading_date: date) -> dict[str, Any] | None:
    path = _receipt_path(root, trading_date)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    allowed_sources = {SOURCE}
    if root.name == "TXFR1":
        allowed_sources.add(LEGACY_TX_SOURCE)
    if not isinstance(payload, dict) or not (
        payload.get("schema_version") == RECEIPT_SCHEMA_VERSION
        and payload.get("source") in allowed_sources
        and payload.get("contract") == root.name
        and payload.get("trading_date") == trading_date.isoformat()
        and payload.get("status") in {"complete", "source_empty"}
        and payload.get('session_finalized') is not False
    ):
        return None
    if payload.get("status") == "source_empty":
        return payload
    data_path = _data_path(root, trading_date)
    if not data_path.is_file() or verified_sha(data_path) != payload.get("sha256"):
        return None
    return payload


def _ticks_frame(
    payload: Any, *, trading_date: date, contract_code: str
) -> tuple[pl.DataFrame, bool]:
    fields = (
        "ts",
        "close",
        "volume",
        "bid_price",
        "bid_volume",
        "ask_price",
        "ask_volume",
        "tick_type",
    )
    values = {field: list(getattr(payload, field)) for field in fields}
    lengths = {field: len(value) for field, value in values.items()}
    if len(set(lengths.values())) != 1:
        raise ValueError(f"inconsistent Shioaji tick field lengths: {lengths}")
    if not values["ts"]:
        return pl.DataFrame(), True
    frame = (
        pl.DataFrame(values)
        .with_row_index("source_row_index")
        .with_columns(
            pl.col("ts").cast(pl.Int64),
            pl.col("ts").cast(pl.Datetime("ns")).alias("event_ts"),
            pl.lit(trading_date).cast(pl.Date).alias("trading_date"),
            pl.lit(contract_code).alias("query_contract"),
        )
    )
    source_order_monotonic = bool(frame.get_column("ts").is_sorted())
    if not source_order_monotonic:
        frame = frame.sort(["ts", "source_row_index"], maintain_order=True)
    return frame, source_order_monotonic


def _write_manifest(
    root: Path,
    *,
    contract: str,
    expected: list[date],
    stopped_for_traffic: bool,
    stopped_for_market_hours: bool,
    usage: tuple[int, int] | None,
    positive_dates: set[date] | None = None,
) -> dict[str, Any]:
    resolved: list[dict[str, Any]] = []
    for trading_date in expected:
        receipt = _valid_receipt(root, trading_date)
        if receipt is not None:
            resolved.append(receipt)
    resolved_dates = {str(item["trading_date"]) for item in resolved}
    available = [item for item in resolved if item.get("status") == "complete"]
    source_empty = [item for item in resolved if item.get("status") == "source_empty"]
    missing = [value.isoformat() for value in expected if value.isoformat() not in resolved_dates]
    positives = {str(day) for day in positive_dates or set()}
    gaps = [r['trading_date'] for r in source_empty if r['trading_date'] in positives]
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "dataset": SOURCE,
        "status": "complete" if not missing else "partial",
        "contract": contract,
        "history_start": expected[0].isoformat() if expected else None,
        "history_end": expected[-1].isoformat() if expected else None,
        "expected_trading_dates": len(expected),
        "resolved_trading_dates": len(resolved),
        "complete_trading_dates": len(available),
        "source_empty_trading_dates": len(source_empty),
        'coverage_state': 'source_gaps' if gaps else ('empty_replies_present' if source_empty else 'observed_data'),
        'positive_activity_empty_dates': gaps,
        'empty_recheck_due_dates': sum(retry_due(r, positive_activity=r['trading_date'] in positives) for r in source_empty),
        'checked_at_utc': utc_stamp(),
        "missing_trading_dates": missing,
        "rows": sum(int(item.get("rows", 0)) for item in available),
        "bytes": sum(int(item.get("size", 0)) for item in available),
        "stopped_for_traffic": stopped_for_traffic,
        "stopped_for_market_hours": stopped_for_market_hours,
        "traffic_used_bytes": usage[0] if usage else None,
        "traffic_limit_bytes": usage[1] if usage else None,
        "timestamp_contract": (
            "ts decodes directly to Asia/Taipei market wall-clock time; trading_date D "
            "contains the prior trading day's night session through D day close"
        ),
        "quote_contract": (
            "bid/ask fields are the one-level values attached to each historical trade; "
            "they are not historical five-level order books"
        ),
    }
    _atomic_write_json(root / "manifest.json", manifest)
    return manifest


def _write_contract_unavailable_manifest(
    root: Path,
    *,
    contract: str,
    expected: list[date],
) -> dict[str, Any]:
    """Persist a truthful terminal provider-catalog gap without fabricating data."""

    preserved = _write_manifest(root, contract=contract, expected=expected,
                                stopped_for_traffic=False, stopped_for_market_hours=False, usage=None)
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "dataset": SOURCE,
        "status": "contract_unavailable",
        "contract": contract,
        "history_start": expected[0].isoformat() if expected else None,
        "history_end": expected[-1].isoformat() if expected else None,
        "expected_trading_dates": len(expected),
        "resolved_trading_dates": 0,
        "complete_trading_dates": 0,
        "source_empty_trading_dates": 0,
        "missing_trading_dates": [value.isoformat() for value in expected],
        "rows": 0,
        "bytes": 0,
        "provider": "shioaji",
        "unavailable_reason": "shioaji_contract_catalog_missing",
        "checked_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "no_data_fabricated": True,
        "stopped_for_traffic": False,
        "stopped_for_market_hours": False,
        "traffic_used_bytes": None,
        "traffic_limit_bytes": None,
        "timestamp_contract": (
            "no timestamps are available because the requested continuous alias "
            "was absent from the provider contract catalog"
        ),
        "quote_contract": (
            "no quote rows were synthesized; a future catalog refresh may make "
            "the alias queryable"
        ),
    }
    for key in ('resolved_trading_dates', 'complete_trading_dates', 'source_empty_trading_dates',
                'missing_trading_dates', 'rows', 'bytes'):
        manifest[key] = preserved[key]
    manifest['next_retry_at_utc'] = utc_stamp(datetime.now(UTC) + timedelta(days=1))
    _atomic_write_json(root / "manifest.json", manifest)
    return manifest


def pending_dates(root: Path, expected: list[date], *, refresh_empty: bool,
                  positive_dates: set[date], empty_probes: int = 2,
                  oldest_first: bool = False, stats: dict | None = None) -> list[date]:
    missing, gaps, unknown = [], [], []
    empty_total = positive_gaps = 0
    for day in expected:
        receipt = _valid_receipt(root, day)
        if receipt and receipt.get('status') == 'source_empty':
            empty_total += 1
            positive_gaps += day in positive_dates
        if receipt is None:
            missing.append(day)
        elif refresh_empty and receipt.get('status') == 'source_empty' and retry_due(
                receipt, positive_activity=day in positive_dates):
            (gaps if day in positive_dates else unknown).append((checked_time(receipt), day))
    missing.sort(reverse=not oldest_first)
    gaps.sort(key=lambda item: (item[0], -item[1].toordinal()))
    unknown.sort(key=lambda item: (item[0], -item[1].toordinal()))
    if stats is not None:
        stats.update(missing_dates=len(missing), source_empty_dates=empty_total,
                     positive_activity_empty_dates=positive_gaps,
                     due_positive_empty_dates=len(gaps), due_unknown_empty_dates=len(unknown))
    # New dates and official contradictions precede bounded unknown-empty probes.
    return missing + [day for _, day in gaps] + [day for _, day in unknown[:empty_probes]]


def main(args=None, *, shared_api=None, activity: FuturesActivity | None = None) -> int:
    args = args or parse_args()
    if args.contracts_file is not None:
        return run_batch(args)
    if args.request_interval is not None and float(args.request_interval) < 0.0:
        raise ValueError("--request-interval must be >= 0")
    request_interval = resolve_request_interval(
        "shioaji_quote_query", args.request_interval
    )
    rate_limiter = SharedRateLimiter(request_interval, name="shioaji_quote_query")
    print(
        "[shioaji-tx-history] "
        f"{describe_rate_limit('shioaji_quote_query', request_interval)}",
        flush=True,
    )
    start = max(date.fromisoformat(args.start_date), HISTORY_START)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("start date must not be after end date")
    if not 0.0 < args.max_traffic_fraction < 1.0:
        raise ValueError("max traffic fraction must be between zero and one")
    if args.timeout_ms < 1 or args.max_dates < 0:
        raise ValueError("timeout and max dates must be valid")
    if _taiwan_market_hours_now() and not args.allow_market_hours:
        print(
            "[shioaji-tx-history] status=stopped_for_market_hours "
            "window=07:45-14:31",
            flush=True,
        )
        return 76

    expected = _calendar(args.calendar_path, start, end)
    if not expected:
        raise RuntimeError("official TX calendar contains no selected trading dates")
    if args.refresh_empty and activity is None:
        activity = load_futures_activity(args.official_activity, start=start, end=end)
    positive_dates = (activity or FuturesActivity()).continuous.get(str(args.contract), set())
    pending = getattr(args, '_pending_override', None)
    if pending is None:
        pending = pending_dates(args.output_dir, expected, refresh_empty=args.refresh_empty,
                            positive_dates=positive_dates, empty_probes=args.empty_probes_per_contract,
                            oldest_first=args.oldest_first)
    if args.max_dates:
        pending = pending[: args.max_dates]
    if args.dry_run:
        print(
            f"[shioaji-tx-history] expected={len(expected)} pending={len(pending)} "
            f"range={expected[0]}..{expected[-1]} output={args.output_dir}"
        )
        return 0
    if not pending:
        manifest = _write_manifest(
            args.output_dir,
            contract=str(args.contract),
            expected=expected,
            stopped_for_traffic=False,
            stopped_for_market_hours=False,
            usage=None,
            positive_dates=positive_dates,
        )
        print(
            "[shioaji-futures-history] "
            f"contract={args.contract} status={manifest['status']} "
            f"resolved={manifest['resolved_trading_dates']}"
            f"/{manifest['expected_trading_dates']} rows={manifest['rows']:,} "
            f"bytes={manifest['bytes']:,} local_receipts_only=true "
            "api_login=false",
            flush=True,
        )
        return 0

    args.output_dir.mkdir(parents=True, exist_ok=True)
    lock_handle = (args.output_dir / "download.lock").open("a+b")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_handle.close()
        raise RuntimeError("another TX history downloader holds the lock") from exc

    import shioaji as sj

    api_key = os.environ.get("SHIOAJI_API_KEY", "").strip()
    secret_key = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError("SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required")
    api = shared_api if shared_api is not None else sj.Shioaji(simulation=bool(args.simulation))
    logged_in = False
    stopped_for_traffic = False
    stopped_for_market_hours = False
    args.completed_queries = 0
    usage: tuple[int, int] | None = None
    try:
        if shared_api is None:
            api.set_event_callback(lambda *_args: None)
        try:
            if shared_api is None:
                api.login(api_key=api_key, secret_key=secret_key, subscribe_trade=False)
        except Exception as exc:  # noqa: BLE001 - normalize broker exception surface.
            if getattr(exc, "code", None) == 451 or "Too Many Connections" in str(exc):
                print(
                    "[shioaji-futures-history] "
                    f"contract={args.contract} status=waiting_connection_capacity "
                    "broker_code=451 retryable=true",
                    flush=True,
                )
                return CONNECTION_CAPACITY_EXIT
            raise
        logged_in = True
        contract = api.contracts.get(str(args.contract))
        if contract is None:
            manifest = _write_contract_unavailable_manifest(
                args.output_dir,
                contract=str(args.contract),
                expected=expected,
            )
            print(
                "[shioaji-futures-history] "
                f"contract={args.contract} status={manifest['status']} "
                "reason=shioaji_contract_catalog_missing "
                "resolved=0/"
                f"{manifest['expected_trading_dates']} no_data_fabricated=true",
                flush=True,
            )
            return CONTRACT_UNAVAILABLE_EXIT
        for index, trading_date in enumerate(pending, start=1):
            if _taiwan_market_hours_now() and not args.allow_market_hours:
                stopped_for_market_hours = True
                break
            try:
                usage = _check_traffic_budget(
                    api,
                    max_fraction=float(args.max_traffic_fraction),
                )
            except TrafficBudgetReached:
                stopped_for_traffic = True
                break
            rate_limiter.wait()
            if _taiwan_market_hours_now() and not args.allow_market_hours:
                stopped_for_market_hours = True
                break
            with shioaji_query(
                api,
                consumer="futures_history_backfill",
                method="ticks",
                asset_class="futures",
                details={
                    "contract": str(args.contract),
                    "date": trading_date.isoformat(),
                },
            ) as set_ledger_result:
                args.completed_queries += 1
                ticks = api.ticks(
                    contract=contract,
                    date=trading_date.isoformat(),
                    timeout=int(args.timeout_ms),
                )
                set_ledger_result(ticks)
            frame, source_order_monotonic = _ticks_frame(
                ticks, trading_date=trading_date, contract_code=str(args.contract)
            )
            prior_receipt = _valid_receipt(args.output_dir, trading_date)
            refresh_fields = retry_metadata(prior_receipt, empty=frame.is_empty(),
                                            positive_activity=trading_date in positive_dates)
            refresh_fields['session_finalized'] = futures_date_is_closed(
                trading_date, contract=str(args.contract)
            )
            if frame.is_empty():
                current_usage = api.usage()
                usage = int(current_usage.bytes), int(current_usage.limit_bytes)
                try:
                    _check_traffic_budget(
                        api,
                        max_fraction=float(args.max_traffic_fraction),
                    )
                except TrafficBudgetReached:
                    stopped_for_traffic = True
                    break
                _atomic_write_json(
                    _receipt_path(args.output_dir, trading_date),
                    {
                        "schema_version": RECEIPT_SCHEMA_VERSION,
                        "source": SOURCE,
                        "status": "source_empty",
                        "contract": str(args.contract),
                        "trading_date": trading_date.isoformat(),
                        "rows": 0,
                        "traffic_used_bytes_after_query": usage[0],
                        **refresh_fields,
                    },
                )
                print(
                    f"[shioaji-futures-history] {index}/{len(pending)} "
                    f"contract={args.contract} date={trading_date} status=source_empty",
                    flush=True,
                )
                continue
            output = _write_parquet_atomic(frame, _data_path(args.output_dir, trading_date))
            receipt = {
                "schema_version": RECEIPT_SCHEMA_VERSION,
                "source": SOURCE,
                "status": "complete",
                "contract": str(args.contract),
                "resolved_target_code_at_query": getattr(contract, "target_code", None),
                "trading_date": trading_date.isoformat(),
                "rows": frame.height,
                "source_order_monotonic": source_order_monotonic,
                **output,
                **refresh_fields,
            }
            _atomic_write_json(_receipt_path(args.output_dir, trading_date), receipt)
            print(
                f"[shioaji-futures-history] {index}/{len(pending)} "
                f"contract={args.contract} date={trading_date} "
                f"rows={frame.height:,} bytes={int(output['size']):,} "
                f"traffic={usage[0]:,}/{usage[1]:,}",
                flush=True,
            )
        if usage is None:
            current = api.usage()
            usage = int(current.bytes), int(current.limit_bytes)
    finally:
        try:
            if logged_in and shared_api is None:
                api.logout()
        finally:
            lock_handle.close()
    manifest = _write_manifest(
        args.output_dir,
        contract=str(args.contract),
        expected=expected,
        stopped_for_traffic=stopped_for_traffic,
        stopped_for_market_hours=stopped_for_market_hours,
        usage=usage,
        positive_dates=positive_dates,
    )
    print(
        "[shioaji-futures-history] "
        f"contract={args.contract} status={manifest['status']} "
        f"resolved={manifest['resolved_trading_dates']}"
        f"/{manifest['expected_trading_dates']} rows={manifest['rows']:,} "
        f"bytes={manifest['bytes']:,} stopped_for_traffic={stopped_for_traffic} "
        f"stopped_for_market_hours={stopped_for_market_hours}",
        flush=True,
    )
    if manifest["status"] == "complete":
        return 0
    return 76 if stopped_for_market_hours else (75 if stopped_for_traffic else 0)


def run_batch(args) -> int:
    """One bounded sweep, one client; shell supervisor owns timing/publication."""
    if args.dry_run:
        return _run_batch(args)
    args.history_root.mkdir(parents=True, exist_ok=True)
    with (args.history_root / 'batch.lock').open('a+b') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError('another futures history batch holds the lock') from exc
        return _run_batch(args)


def _run_batch(args) -> int:
    if not args.dry_run and _taiwan_market_hours_now() and not args.allow_market_hours:
        return 76
    if args.dates_per_contract < 1 or args.empty_probes_per_contract < 0 or args.max_dates < 0:
        raise ValueError('invalid batch query limits')
    start, end = max(date.fromisoformat(args.start_date), HISTORY_START), date.fromisoformat(args.end_date)
    expected = _calendar(args.calendar_path, start, end)
    if not expected:
        raise RuntimeError('no completed official query dates')
    activity = load_futures_activity(args.official_activity, start=start, end=end) if args.refresh_empty else FuturesActivity()
    api = None
    logged_in = False
    records = []
    queries = 0
    exit_code = 0
    consecutive_failures = 0
    receipt_path = args.batch_receipt.with_name('latest_plan.json') if args.dry_run else args.batch_receipt

    def login():
        nonlocal api, logged_in
        if api is not None:
            return api
        import shioaji as sj
        key, secret = os.getenv('SHIOAJI_API_KEY', '').strip(), os.getenv('SHIOAJI_SECRET_KEY', '').strip()
        if not key or not secret:
            raise RuntimeError('SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required')
        api = sj.Shioaji(simulation=args.simulation)
        api.set_event_callback(lambda *_: None)
        api.login(api_key=key, secret_key=secret, subscribe_trade=False)
        logged_in = True
        return api

    try:
        if args.refresh_inventory and not args.dry_run:
            from scripts.export_shioaji_futures_products import export_inventory
            export_inventory(login(), args.contracts_file.parent)
        catalog_sha = _sha256(args.contracts_file)
        aliases = pl.read_csv(args.contracts_file).sort(['priority', 'contract']).to_dicts()
        _atomic_write_json(receipt_path, {'schema_version': 2, 'status': 'planning',
                                         'target_end_date': str(end), 'total_contracts': len(aliases),
                                         'catalog_sha256': catalog_sha, 'started_at_utc': utc_stamp()})
        for index, item in enumerate(aliases):
            code = item['contract']
            root = args.tx_history_root if code == 'TXFR1' else args.history_root / code
            positive = activity.continuous.get(code, set())
            stats = {'contract': code, 'source_root': str(root)}
            pending = pending_dates(root, expected, refresh_empty=args.refresh_empty,
                                    positive_dates=positive, empty_probes=args.empty_probes_per_contract,
                                    oldest_first=args.oldest_first, stats=stats)
            stats['planned_queries'] = min(len(pending), args.dates_per_contract)
            stats['queries'] = 0
            stats['missing_dates_after_batch'] = stats['missing_dates']
            stats['positive_empty_dates_after_batch'] = stats['positive_activity_empty_dates']
            prior_manifest = {}
            try:
                prior_manifest = json.loads((root / 'manifest.json').read_text())
            except (OSError, ValueError):
                pass
            unavailable = prior_manifest.get('status') == 'contract_unavailable'
            cooldown = (unavailable and prior_manifest.get('catalog_sha256') == catalog_sha
                        and (parsed_time(prior_manifest.get('next_retry_at_utc')) or datetime.min.replace(tzinfo=UTC)) > datetime.now(UTC))
            stats['provider_unavailable'] = unavailable
            if pending and not args.dry_run and not cooldown and (not args.max_dates or queries < args.max_dates):
                child = copy.copy(args)
                child.contracts_file = None
                child.contract = code
                child.output_dir = root
                child.max_dates = min(args.dates_per_contract, args.max_dates - queries) if args.max_dates else args.dates_per_contract
                child._pending_override = pending
                child.completed_queries = 0
                try:
                    rc = main(child, shared_api=login(), activity=activity)
                    consecutive_failures = 0
                    stats['provider_unavailable'] = rc == CONTRACT_UNAVAILABLE_EXIT
                    if rc == CONTRACT_UNAVAILABLE_EXIT:
                        manifest = json.loads((root / 'manifest.json').read_text())
                        manifest['catalog_sha256'] = catalog_sha
                        _atomic_write_json(root / 'manifest.json', manifest)
                    else:
                        manifest = json.loads((root / 'manifest.json').read_text())
                        stats['missing_dates_after_batch'] = len(manifest['missing_trading_dates'])
                        stats['positive_empty_dates_after_batch'] = len(manifest.get('positive_activity_empty_dates', []))
                    if rc in (75, 76, 79):
                        exit_code = rc
                except Exception as exc:
                    if getattr(exc, 'code', None) == 451 or 'Too Many Connections' in str(exc):
                        exit_code = CONNECTION_CAPACITY_EXIT
                    else:
                        stats['error_type'] = type(exc).__name__
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            exit_code = 1
                        print(f'[shioaji-futures-history] contract={code} state=query_failed error_type={type(exc).__name__}', flush=True)
                finally:
                    stats['queries'] = child.completed_queries
                    queries += stats['queries']
            stats['latest_date_queried'] = stats['provider_unavailable'] or _valid_receipt(root, expected[-1]) is not None
            records.append(stats)
            if (index + 1) % 25 == 0:
                print(f'[shioaji-futures-history] scanned={index + 1}/{len(aliases)} queries={queries}', flush=True)
            if exit_code:
                break
        payload = {'schema_version': 2, 'status': 'planned' if args.dry_run else 'batch_finished',
                   'target_end_date': str(end), 'catalog_sha256': catalog_sha,
                   'total_contracts': len(aliases), 'scanned_contracts': len(records),
                   'queries': queries, 'planned_queries': sum(r['planned_queries'] for r in records),
                   'provider_unavailable_contracts': sum(r['provider_unavailable'] for r in records),
                   'positive_activity_empty_dates_before_batch': sum(r['positive_activity_empty_dates'] for r in records),
                   'unknown_empty_dates_due_before_batch': sum(r['due_unknown_empty_dates'] for r in records),
                   'failed_contracts': sum('error_type' in r for r in records),
                   'missing_dates_after_batch': sum(r['missing_dates_after_batch'] for r in records),
                   'positive_activity_empty_dates_after_batch': sum(r['positive_empty_dates_after_batch'] for r in records),
                   'current_query_sweep_complete': len(records) == len(aliases) and all(r['latest_date_queried'] and 'error_type' not in r for r in records),
                   'exit_code': exit_code, 'completed_at_utc': utc_stamp(),
                   'official_activity': activity.provenance, 'contracts': records,
                   'completion_contract': 'Batch progress is separate from source coverage; empty replies and unavailable codes remain explicit gaps.'}
        _atomic_write_json(receipt_path, payload)
        print('[shioaji-futures-history] batch_receipt=' + str(receipt_path) + f' queries={queries} exit={exit_code}', flush=True)
        return exit_code or (1 if payload['failed_contracts'] else 0)
    except Exception as exc:
        if getattr(exc, 'code', None) == 451 or 'Too Many Connections' in str(exc):
            return CONNECTION_CAPACITY_EXIT
        raise
    finally:
        if logged_in:
            try:
                api.logout()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
