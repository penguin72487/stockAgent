#!/usr/bin/env python3
"""Download receipt-backed Shioaji option, exact-future, and index history.

The collector deliberately separates four claims:

* Contract V2 discovery says what is queryable *now*.
* A KBar chunk receipt proves that one bounded ``api.kbars`` query completed.
* Tick targets are the trading dates actually observed in verified KBars.
* A Tick receipt proves that one ``api.ticks`` trading-date query completed.

This avoids inventing a holiday calendar, querying every option strike on every
calendar day, or treating a running process as completed historical coverage.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import fcntl
import hashlib
import heapq
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.common import (  # noqa: E402
    PersistentProgress,
    SharedRateLimiter,
    describe_rate_limit,
    resolve_request_interval,
)
from downloader.download_shioaji_tw_kbars import (  # noqa: E402
    TrafficBudgetReached,
    _atomic_write_json,
    _check_traffic_budget,
    _payload_dict,
    _sha256,
    _write_parquet_atomic,
    iter_date_chunks,
)
from downloader.download_shioaji_tx_futures_ticks import _ticks_frame  # noqa: E402
from stockagent.data.taifex_sessions import taifex_trading_date  # noqa: E402
from stockagent.live.shioaji_schedule import (  # noqa: E402
    HISTORICAL_MAX_TRAFFIC_FRACTION,
    historical_query_is_protected,
)
from stockagent.live.shioaji_traffic_ledger import shioaji_query  # noqa: E402


TAIPEI = ZoneInfo("Asia/Taipei")
SOURCE = "shioaji_historical_market_data_v2"
INVENTORY_SCHEMA_VERSION = 1
RECEIPT_SCHEMA_VERSION = 2
SUMMARY_SCHEMA_VERSION = 1
FOP_HISTORY_START = date(2020, 3, 22)
INDEX_HISTORY_START = date(2020, 3, 2)
MAX_KBAR_QUERY_DAYS = 29
CONNECTION_CAPACITY_EXIT = 79
TRAFFIC_BUDGET_EXIT = 75
MARKET_WINDOW_EXIT = 76
WEEKLY_OPTION_ROOT_RE = re.compile(r"^TX(?:[1-5UVWXY])$")
COLLECTION_PRIORITY = {
    "latest_weekly_option": 0,
    "latest_monthly_option": 1,
    "exact_futures": 2,
    "indices": 3,
}


@dataclass(frozen=True, slots=True)
class HistoryContract:
    collection: str
    priority: int
    security_type: str
    asset_class: str
    code: str
    root: str
    name: str
    exchange: str
    begin_date: date
    end_date: date
    delivery_date: date | None = None
    delivery_month: str = ""
    tenor_rank: int | None = None
    strike_price: float | None = None
    option_right: str = ""
    expiry_weekday: str = ""
    active_selection: bool = True


@dataclass(frozen=True, slots=True)
class HistoryTask:
    collection_priority: int
    end_ordinal_desc: int
    code: str
    method_rank: int
    method: str
    contract: HistoryContract
    start: date
    end: date

    def heap_key(self) -> tuple[int, int, str, int, str, HistoryTask]:
        return (
            self.collection_priority,
            self.end_ordinal_desc,
            self.code,
            self.method_rank,
            self.start.isoformat(),
            self,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data_tw_shioaji_history")
    )
    parser.add_argument(
        "--taifex-calendar",
        type=Path,
        default=Path("data_tw_index_futures/day_session_contracts.parquet"),
    )
    parser.add_argument(
        "--twse-calendar",
        type=Path,
        default=Path("data_tw_public/twse_taiex_ohlc.parquet"),
    )
    parser.add_argument("--chunk-days", type=int, default=MAX_KBAR_QUERY_DAYS)
    parser.add_argument("--timeout-ms", type=int, default=120_000)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--retry-backoff", type=float, default=3.0)
    parser.add_argument("--request-interval", type=float, default=None)
    parser.add_argument(
        "--max-traffic-fraction",
        type=float,
        default=HISTORICAL_MAX_TRAFFIC_FRACTION,
    )
    parser.add_argument(
        "--collections",
        default=",".join(COLLECTION_PRIORITY),
        help="Comma-separated collection ids.",
    )
    parser.add_argument(
        "--contract-codes",
        default="",
        help="Optional comma-separated exact contract codes for a bounded run.",
    )
    parser.add_argument("--max-queries", type=int, default=0)
    parser.add_argument("--simulation", action="store_true")
    parser.add_argument("--allow-market-hours", action="store_true")
    parser.add_argument("--no-refresh-inventory", action="store_true")
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _enum_text(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip()


def _date_value(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _info_dict(info: Any) -> dict[str, Any]:
    if hasattr(info, "dict"):
        payload = info.dict()
        if isinstance(payload, dict):
            return payload
    names = (
        "code",
        "name",
        "root",
        "exchange",
        "security_type",
        "begin_date",
        "delivery_date",
        "last_trading_date",
        "delivery_month",
        "strike_price",
        "option_right",
        "expiry_weekday",
        "underlying_code",
    )
    return {name: getattr(info, name, None) for name in names}


def _completed_from_calendar(path: Path, *, column: str = "date") -> date | None:
    if not path.is_file():
        return None
    today = datetime.now(TAIPEI).date()
    try:
        value = (
            pl.scan_parquet(path)
            .select(pl.col(column).cast(pl.Date, strict=False).alias("date"))
            .filter(pl.col("date") <= pl.lit(today))
            .select(pl.col("date").max())
            .collect()
            .item()
        )
    except (OSError, pl.exceptions.PolarsError, ValueError):
        return None
    return value if isinstance(value, date) else None


def latest_completed_session(taifex_calendar: Path, twse_calendar: Path) -> date:
    candidates = [
        value
        for value in (
            _completed_from_calendar(taifex_calendar),
            _completed_from_calendar(twse_calendar),
        )
        if value is not None
    ]
    if candidates:
        return max(candidates)
    local = datetime.now(TAIPEI)
    candidate = local.date() if local.time().hour >= 14 else local.date() - timedelta(1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def select_latest_option_infos(
    infos: Iterable[Any], *, completed_session: date
) -> tuple[list[Any], list[Any]]:
    """Select every strike/right in the nearest weekly and monthly expiries."""

    normalized: list[tuple[Any, dict[str, Any]]] = []
    for info in infos:
        values = _info_dict(info)
        expiry = _date_value(values.get("delivery_date"))
        if expiry is None or expiry <= completed_session:
            continue
        if str(values.get("underlying_code") or "") != "IX0001":
            continue
        normalized.append((info, values))
    monthly_expiries = sorted(
        {
            _date_value(values.get("delivery_date"))
            for _info, values in normalized
            if str(values.get("root") or "").upper() == "TXO"
        }
        - {None}
    )
    weekly_expiries = sorted(
        {
            _date_value(values.get("delivery_date"))
            for _info, values in normalized
            if WEEKLY_OPTION_ROOT_RE.fullmatch(
                str(values.get("root") or "").upper()
            )
        }
        - {None}
    )
    monthly_expiry = monthly_expiries[0] if monthly_expiries else None
    weekly_expiry = weekly_expiries[0] if weekly_expiries else None
    monthly = [
        info
        for info, values in normalized
        if str(values.get("root") or "").upper() == "TXO"
        and _date_value(values.get("delivery_date")) == monthly_expiry
    ]
    weekly = [
        info
        for info, values in normalized
        if WEEKLY_OPTION_ROOT_RE.fullmatch(str(values.get("root") or "").upper())
        and _date_value(values.get("delivery_date")) == weekly_expiry
    ]
    return weekly, monthly


def _history_contract(
    info: Any,
    *,
    collection: str,
    completed_session: date,
    tenor_rank: int | None = None,
) -> HistoryContract | None:
    values = _info_dict(info)
    code = str(values.get("code") or getattr(info, "code", "")).strip()
    security_type = _enum_text(
        values.get("security_type") or getattr(info, "security_type", "")
    ).upper()
    if not code or security_type not in {"OPT", "FUT", "IND"}:
        return None
    if security_type == "IND":
        begin = INDEX_HISTORY_START
        end = completed_session
        asset_class = "index"
    else:
        begin = max(_date_value(values.get("begin_date")) or FOP_HISTORY_START, FOP_HISTORY_START)
        final = (
            _date_value(values.get("last_trading_date"))
            or _date_value(values.get("delivery_date"))
            or completed_session
        )
        end = min(final, completed_session)
        asset_class = "options" if security_type == "OPT" else "futures"
    if begin > end:
        return None
    strike = values.get("strike_price")
    return HistoryContract(
        collection=collection,
        priority=COLLECTION_PRIORITY[collection],
        security_type=security_type,
        asset_class=asset_class,
        code=code,
        root=str(values.get("root") or "").strip().upper(),
        name=str(values.get("name") or code).strip(),
        exchange=_enum_text(values.get("exchange") or getattr(info, "exchange", "")),
        begin_date=begin,
        end_date=end,
        delivery_date=_date_value(values.get("delivery_date")),
        delivery_month=str(values.get("delivery_month") or ""),
        tenor_rank=tenor_rank,
        strike_price=float(strike) if strike is not None else None,
        option_right=_enum_text(values.get("option_right")),
        expiry_weekday=str(values.get("expiry_weekday") or ""),
    )


def discover_contracts(api: Any, *, completed_session: date) -> list[HistoryContract]:
    option_infos: list[Any] = []
    for root_item in api.contracts.option_roots():
        root = str(root_item[0] if isinstance(root_item, (tuple, list)) else root_item)
        root = root.strip().upper()
        if root != "TXO" and not WEEKLY_OPTION_ROOT_RE.fullmatch(root):
            continue
        try:
            option_infos.extend(api.contracts.options(root))
        except Exception:
            continue
    weekly, monthly = select_latest_option_infos(
        option_infos, completed_session=completed_session
    )
    rows: list[HistoryContract] = []
    for info in weekly:
        row = _history_contract(
            info,
            collection="latest_weekly_option",
            completed_session=completed_session,
        )
        if row is not None:
            rows.append(row)
    for info in monthly:
        row = _history_contract(
            info,
            collection="latest_monthly_option",
            completed_session=completed_session,
        )
        if row is not None:
            rows.append(row)

    futures: list[tuple[Any, dict[str, Any]]] = []
    for root_item in api.contracts.futures_roots():
        root = str(root_item[0] if isinstance(root_item, (tuple, list)) else root_item)
        try:
            chain = api.contracts.futures(root)
        except Exception:
            continue
        for info in chain:
            values = _info_dict(info)
            code = str(values.get("code") or "")
            if code.endswith("R1") or code.endswith("R2"):
                continue
            futures.append((info, values))
    ranks: dict[str, dict[str, int]] = {}
    for _info, values in futures:
        root = str(values.get("root") or "")
        delivery = _date_value(values.get("delivery_date")) or date.max
        code = str(values.get("code") or "")
        ranks.setdefault(root, {})[code] = 0
        ranks[root][code] = int(delivery.toordinal())
    rank_values: dict[tuple[str, str], int] = {}
    for root, codes in ranks.items():
        ordered = sorted(codes, key=lambda code: (codes[code], code))
        rank_values.update({(root, code): index + 1 for index, code in enumerate(ordered)})
    for info, values in futures:
        root = str(values.get("root") or "")
        code = str(values.get("code") or "")
        row = _history_contract(
            info,
            collection="exact_futures",
            completed_session=completed_session,
            tenor_rank=rank_values.get((root, code)),
        )
        if row is not None:
            rows.append(row)

    for base in api.contracts.list("IND"):
        try:
            info = api.contracts.info(base) or base
        except Exception:
            info = base
        row = _history_contract(
            info,
            collection="indices",
            completed_session=completed_session,
        )
        if row is not None:
            rows.append(row)
    deduped = {(row.security_type, row.code): row for row in rows}
    return sorted(
        deduped.values(),
        key=lambda row: (
            row.priority,
            row.delivery_date or date.max,
            row.root,
            row.strike_price if row.strike_price is not None else float("inf"),
            row.option_right,
            row.code,
        ),
    )


def _contract_record(row: HistoryContract, *, observed_at: str) -> dict[str, Any]:
    return {
        "collection": row.collection,
        "priority": row.priority,
        "security_type": row.security_type,
        "asset_class": row.asset_class,
        "code": row.code,
        "root": row.root,
        "name": row.name,
        "exchange": row.exchange,
        "begin_date": row.begin_date,
        "end_date": row.end_date,
        "delivery_date": row.delivery_date,
        "delivery_month": row.delivery_month,
        "tenor_rank": row.tenor_rank,
        "strike_price": row.strike_price,
        "option_right": row.option_right,
        "expiry_weekday": row.expiry_weekday,
        "active_selection": row.active_selection,
        "first_observed_at_utc": observed_at,
        "last_observed_at_utc": observed_at,
    }


def _write_inventory(
    root: Path, rows: Sequence[HistoryContract], *, completed_session: date
) -> dict[str, Any]:
    inventory_dir = root / "inventory"
    inventory_dir.mkdir(parents=True, exist_ok=True)
    observed = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    current_records = [_contract_record(row, observed_at=observed) for row in rows]
    path = inventory_dir / "contracts.parquet"
    previous: dict[tuple[str, str], dict[str, Any]] = {}
    if path.is_file():
        for item in pl.read_parquet(path).to_dicts():
            item["active_selection"] = False
            previous[(str(item["security_type"]), str(item["code"]))] = item
    for item in current_records:
        key = str(item["security_type"]), str(item["code"])
        prior = previous.get(key)
        if prior is not None:
            item["first_observed_at_utc"] = prior.get("first_observed_at_utc") or observed
        previous[key] = item
    merged = sorted(
        previous.values(),
        key=lambda item: (
            not bool(item.get("active_selection")),
            int(item.get("priority") or 99),
            str(item.get("code") or ""),
        ),
    )
    frame = pl.DataFrame(merged, infer_schema_length=None)
    temporary = path.with_suffix(".parquet.tmp")
    frame.write_parquet(temporary, compression="snappy", statistics=True)
    os.replace(temporary, path)
    current_path = inventory_dir / "current_contracts.parquet"
    current_tmp = current_path.with_suffix(".parquet.tmp")
    pl.DataFrame(current_records, infer_schema_length=None).write_parquet(
        current_tmp, compression="snappy"
    )
    os.replace(current_tmp, current_path)
    by_collection = {
        collection: sum(row.collection == collection for row in rows)
        for collection in COLLECTION_PRIORITY
    }
    option_expiries = {
        collection: sorted(
            {
                row.delivery_date.isoformat()
                for row in rows
                if row.collection == collection and row.delivery_date is not None
            }
        )
        for collection in ("latest_weekly_option", "latest_monthly_option")
    }
    manifest = {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "source": "shioaji_contract_v2",
        "generated_at_utc": observed,
        "completed_session": completed_session.isoformat(),
        "current_contracts": len(rows),
        "retained_contracts": len(merged),
        "by_collection": by_collection,
        "option_expiries": option_expiries,
        "exact_futures_r3_or_later": sum(
            row.collection == "exact_futures" and int(row.tenor_rank or 0) >= 3
            for row in rows
        ),
        "contracts_path": str(path),
        "contracts_sha256": _sha256(path),
        "current_contracts_path": str(current_path),
        "current_contracts_sha256": _sha256(current_path),
        "retention_contract": (
            "current Contract V2 selections are merged into a stable union; expired "
            "codes are never silently removed"
        ),
    }
    _atomic_write_json(inventory_dir / "manifest.json", manifest)
    return manifest


def _row_from_record(item: dict[str, Any]) -> HistoryContract:
    return HistoryContract(
        collection=str(item["collection"]),
        priority=int(item["priority"]),
        security_type=str(item["security_type"]),
        asset_class=str(item["asset_class"]),
        code=str(item["code"]),
        root=str(item.get("root") or ""),
        name=str(item.get("name") or item["code"]),
        exchange=str(item.get("exchange") or ""),
        begin_date=_date_value(item.get("begin_date")) or INDEX_HISTORY_START,
        end_date=_date_value(item.get("end_date")) or date.today(),
        delivery_date=_date_value(item.get("delivery_date")),
        delivery_month=str(item.get("delivery_month") or ""),
        tenor_rank=(int(item["tenor_rank"]) if item.get("tenor_rank") is not None else None),
        strike_price=(
            float(item["strike_price"]) if item.get("strike_price") is not None else None
        ),
        option_right=str(item.get("option_right") or ""),
        expiry_weekday=str(item.get("expiry_weekday") or ""),
        active_selection=bool(item.get("active_selection")),
    )


def load_inventory(root: Path, *, current_only: bool = False) -> list[HistoryContract]:
    path = root / "inventory" / (
        "current_contracts.parquet" if current_only else "contracts.parquet"
    )
    if not path.is_file():
        raise FileNotFoundError(f"historical contract inventory missing: {path}")
    return [_row_from_record(item) for item in pl.read_parquet(path).to_dicts()]


def _contract_root(root: Path, row: HistoryContract) -> Path:
    return root / "contracts" / row.asset_class / row.code


def _kbar_paths(root: Path, row: HistoryContract, start: date, end: date) -> tuple[Path, Path]:
    base = _contract_root(root, row) / "kbars" / (
        f"start={start.isoformat()}_end={end.isoformat()}"
    )
    return base / "data.parquet", base / "receipt.json"


def _tick_paths(root: Path, row: HistoryContract, trading_date: date) -> tuple[Path, Path]:
    base = _contract_root(root, row) / "ticks" / (
        f"trading_date={trading_date.isoformat()}"
    )
    return base / "data.parquet", base / "receipt.json"


def _read_receipt(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _valid_receipt(path: Path, data_path: Path, *, method: str, code: str) -> dict[str, Any] | None:
    payload = _read_receipt(path)
    if not payload or not (
        payload.get("schema_version") == RECEIPT_SCHEMA_VERSION
        and payload.get("source") == SOURCE
        and payload.get("method") == method
        and payload.get("contract") == code
        and payload.get("status") in {"complete", "source_empty"}
    ):
        return None
    if payload.get("status") == "source_empty":
        return payload
    if not data_path.is_file() or _sha256(data_path) != payload.get("sha256"):
        return None
    return payload


def _naive_wall_clock(ts_ns: int) -> datetime:
    return datetime(1970, 1, 1) + timedelta(microseconds=int(ts_ns) / 1_000)


def _kbar_frame(payload: Any, *, row: HistoryContract) -> tuple[pl.DataFrame, list[date]]:
    values = _payload_dict(payload)
    required = ("ts", "Open", "High", "Low", "Close", "Volume", "Amount")
    missing = [name for name in required if name not in values]
    if missing:
        raise ValueError(f"Shioaji Kbars missing fields for {row.code}: {missing}")
    lengths = {name: len(values[name]) for name in required}
    if len(set(lengths.values())) != 1:
        raise ValueError(f"inconsistent Shioaji Kbar field lengths: {lengths}")
    if not values["ts"]:
        return pl.DataFrame(), []
    timestamps = [int(value) for value in values["ts"]]
    if row.security_type in {"FUT", "OPT"}:
        trading_dates = [taifex_trading_date(_naive_wall_clock(value)) for value in timestamps]
    else:
        trading_dates = [_naive_wall_clock(value).date() for value in timestamps]
    frame = (
        pl.DataFrame({name: values[name] for name in required})
        .with_columns(
            pl.col("ts").cast(pl.Int64),
            pl.col("ts").cast(pl.Datetime("ns")).alias("event_ts"),
            pl.Series("trading_date", trading_dates, dtype=pl.Date),
            pl.lit(row.code).alias("query_contract"),
            pl.lit(row.collection).alias("collection"),
            pl.lit(row.security_type).alias("security_type"),
        )
        .filter(
            (pl.col("trading_date") >= pl.lit(row.begin_date))
            & (pl.col("trading_date") <= pl.lit(row.end_date))
        )
        .sort(["ts"], maintain_order=True)
    )
    bounded_dates = (
        list(frame.get_column("trading_date").unique().sort()) if frame.height else []
    )
    return frame, bounded_dates


def _chunk_receipts(root: Path, row: HistoryContract, *, chunk_days: int) -> Iterable[dict[str, Any]]:
    for start, end in iter_date_chunks(row.begin_date, row.end_date, chunk_days):
        data_path, receipt_path = _kbar_paths(root, row, start, end)
        receipt = _valid_receipt(
            receipt_path, data_path, method="kbars", code=row.code
        )
        if receipt is not None:
            yield receipt


def observed_tick_dates(root: Path, row: HistoryContract, *, chunk_days: int) -> list[date]:
    values: set[date] = set()
    for receipt in _chunk_receipts(root, row, chunk_days=chunk_days):
        for value in receipt.get("observed_trading_dates") or []:
            parsed = _date_value(value)
            if parsed is not None:
                values.add(parsed)
    return sorted(values)


def build_tasks(
    root: Path,
    rows: Sequence[HistoryContract],
    *,
    chunk_days: int,
) -> list[HistoryTask]:
    tasks: list[HistoryTask] = []
    for row in rows:
        for start, end in iter_date_chunks(row.begin_date, row.end_date, chunk_days):
            data_path, receipt_path = _kbar_paths(root, row, start, end)
            if _valid_receipt(receipt_path, data_path, method="kbars", code=row.code):
                continue
            tasks.append(
                HistoryTask(
                    row.priority,
                    -end.toordinal(),
                    row.code,
                    0,
                    "kbars",
                    row,
                    start,
                    end,
                )
            )
        for trading_date in observed_tick_dates(root, row, chunk_days=chunk_days):
            data_path, receipt_path = _tick_paths(root, row, trading_date)
            if _valid_receipt(receipt_path, data_path, method="ticks", code=row.code):
                continue
            tasks.append(
                HistoryTask(
                    row.priority,
                    -trading_date.toordinal(),
                    row.code,
                    1,
                    "ticks",
                    row,
                    trading_date,
                    trading_date,
                )
            )
    return sorted(
        tasks,
        key=lambda task: (
            task.collection_priority,
            task.end_ordinal_desc,
            task.code,
            task.method_rank,
            task.start,
        ),
    )


def _write_summary(
    root: Path,
    rows: Sequence[HistoryContract],
    *,
    chunk_days: int,
    state: str,
    usage: tuple[int, int] | None,
    progress_path: Path,
    persist: bool = True,
) -> dict[str, Any]:
    collection_rows: dict[str, dict[str, Any]] = {}
    totals = {
        "contracts": 0,
        "kbar_chunks": 0,
        "resolved_kbar_chunks": 0,
        "kbar_rows": 0,
        "tick_dates": 0,
        "resolved_tick_dates": 0,
        "tick_rows": 0,
        "stored_bytes": 0,
    }
    completed_contracts = 0
    for row in rows:
        bucket = collection_rows.setdefault(
            row.collection,
            {
                "contracts": 0,
                "complete_contracts": 0,
                "kbar_chunks": 0,
                "resolved_kbar_chunks": 0,
                "kbar_rows": 0,
                "tick_dates": 0,
                "resolved_tick_dates": 0,
                "tick_rows": 0,
                "stored_bytes": 0,
            },
        )
        bucket["contracts"] += 1
        contract_kbar_total = 0
        contract_kbar_resolved = 0
        for start, end in iter_date_chunks(row.begin_date, row.end_date, chunk_days):
            contract_kbar_total += 1
            data_path, receipt_path = _kbar_paths(root, row, start, end)
            receipt = _valid_receipt(
                receipt_path, data_path, method="kbars", code=row.code
            )
            if receipt is not None:
                contract_kbar_resolved += 1
                bucket["kbar_rows"] += int(receipt.get("rows") or 0)
                bucket["stored_bytes"] += int(receipt.get("size") or 0)
        dates = observed_tick_dates(root, row, chunk_days=chunk_days)
        resolved_ticks = 0
        for trading_date in dates:
            data_path, receipt_path = _tick_paths(root, row, trading_date)
            receipt = _valid_receipt(
                receipt_path, data_path, method="ticks", code=row.code
            )
            if receipt is not None:
                resolved_ticks += 1
                bucket["tick_rows"] += int(receipt.get("rows") or 0)
                bucket["stored_bytes"] += int(receipt.get("size") or 0)
        bucket["kbar_chunks"] += contract_kbar_total
        bucket["resolved_kbar_chunks"] += contract_kbar_resolved
        bucket["tick_dates"] += len(dates)
        bucket["resolved_tick_dates"] += resolved_ticks
        if contract_kbar_resolved == contract_kbar_total and resolved_ticks == len(dates):
            bucket["complete_contracts"] += 1
            completed_contracts += 1
    for bucket in collection_rows.values():
        for key in totals:
            if key in bucket:
                totals[key] += int(bucket[key])
    pending = build_tasks(root, rows, chunk_days=chunk_days)
    payload = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "source": SOURCE,
        "written_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "state": "complete" if not pending else state,
        **totals,
        "complete_contracts": completed_contracts,
        "pending_queries": len(pending),
        "tick_target_universe_finalized": bool(
            totals["resolved_kbar_chunks"] == totals["kbar_chunks"]
        ),
        "by_collection": collection_rows,
        "traffic_used_bytes": usage[0] if usage else None,
        "traffic_limit_bytes": usage[1] if usage else None,
        "max_traffic_fraction": HISTORICAL_MAX_TRAFFIC_FRACTION,
        "progress_path": str(progress_path),
        "completeness_contract": (
            "KBar chunks are complete/source_empty receipts; Tick targets are the "
            "union of trading dates observed by those verified KBar chunks, and each "
            "target requires its own complete/source_empty Tick receipt"
        ),
        "historical_depth_contract": (
            "historical ticks contain only the one best bid/ask attached to each trade; "
            "historical five-level books are not claimed"
        ),
        "no_data_fabricated": True,
    }
    if persist:
        _atomic_write_json(root / "summary.json", payload)
    return payload


def _query_with_retries(
    call,
    *,
    retries: int,
    retry_backoff: float,
) -> Any:
    last_error: BaseException | None = None
    for attempt in range(max(0, retries) + 1):
        try:
            return call()
        except BaseException as exc:  # Shioaji exposes several native exception types.
            last_error = exc
            if attempt >= max(0, retries):
                raise
            time.sleep(max(0.0, retry_backoff) * (2**attempt))
    assert last_error is not None
    raise last_error


def _query_task(
    api: Any,
    task: HistoryTask,
    *,
    output_root: Path,
    timeout_ms: int,
    retries: int,
    retry_backoff: float,
    rate_limiter: SharedRateLimiter,
) -> tuple[dict[str, Any], list[HistoryTask]]:
    row = task.contract
    contract = api.contracts.get(row.code)
    if contract is None:
        raise LookupError(f"contract_not_in_current_catalog:{row.security_type}:{row.code}")
    started = datetime.now(UTC)
    if task.method == "kbars":
        def call_kbars():
            rate_limiter.wait()
            with shioaji_query(
                api,
                consumer="historical_market_data_backfill",
                method="kbars",
                asset_class=row.asset_class,
                details={
                    "contract": row.code,
                    "start": task.start.isoformat(),
                    "end": task.end.isoformat(),
                },
            ) as set_result:
                payload = api.kbars(
                    contract=contract,
                    start=task.start.isoformat(),
                    end=task.end.isoformat(),
                    timeout=int(timeout_ms),
                )
                set_result(getattr(payload, "ts", []))
                return payload

        payload = _query_with_retries(
            call_kbars, retries=retries, retry_backoff=retry_backoff
        )
        frame, trading_dates = _kbar_frame(payload, row=row)
        data_path, receipt_path = _kbar_paths(
            output_root, row, task.start, task.end
        )
        output = (
            _write_parquet_atomic(frame, data_path) if frame.height else {}
        )
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "source": SOURCE,
            "status": "complete" if frame.height else "source_empty",
            "method": "kbars",
            "contract": row.code,
            "security_type": row.security_type,
            "collection": row.collection,
            "start": task.start.isoformat(),
            "end": task.end.isoformat(),
            "rows": frame.height,
            "observed_trading_dates": [value.isoformat() for value in trading_dates],
            "started_at_utc": started.isoformat().replace("+00:00", "Z"),
            "finished_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            **output,
        }
        _atomic_write_json(receipt_path, receipt)
        added: list[HistoryTask] = []
        for trading_date in trading_dates:
            tick_data, tick_receipt = _tick_paths(output_root, row, trading_date)
            if _valid_receipt(
                tick_receipt, tick_data, method="ticks", code=row.code
            ):
                continue
            added.append(
                HistoryTask(
                    row.priority,
                    -trading_date.toordinal(),
                    row.code,
                    1,
                    "ticks",
                    row,
                    trading_date,
                    trading_date,
                )
            )
        return receipt, added

    def call_ticks():
        rate_limiter.wait()
        with shioaji_query(
            api,
            consumer="historical_market_data_backfill",
            method="ticks",
            asset_class=row.asset_class,
            details={"contract": row.code, "date": task.start.isoformat()},
        ) as set_result:
            payload = api.ticks(
                contract=contract,
                date=task.start.isoformat(),
                timeout=int(timeout_ms),
            )
            set_result(getattr(payload, "ts", []))
            return payload

    payload = _query_with_retries(
        call_ticks, retries=retries, retry_backoff=retry_backoff
    )
    frame, source_order_monotonic = _ticks_frame(
        payload, trading_date=task.start, contract_code=row.code
    )
    if frame.height:
        frame = frame.with_columns(
            pl.lit(row.collection).alias("collection"),
            pl.lit(row.security_type).alias("security_type"),
        )
    data_path, receipt_path = _tick_paths(output_root, row, task.start)
    output = _write_parquet_atomic(frame, data_path) if frame.height else {}
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "source": SOURCE,
        "status": "complete" if frame.height else "source_empty",
        "method": "ticks",
        "contract": row.code,
        "security_type": row.security_type,
        "collection": row.collection,
        "trading_date": task.start.isoformat(),
        "rows": frame.height,
        "source_order_monotonic": source_order_monotonic,
        "quote_depth": 1,
        "started_at_utc": started.isoformat().replace("+00:00", "Z"),
        "finished_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        **output,
    }
    _atomic_write_json(receipt_path, receipt)
    return receipt, []


def main() -> int:
    args = parse_args()
    if not 1 <= int(args.chunk_days) <= MAX_KBAR_QUERY_DAYS:
        raise ValueError(f"--chunk-days must be within 1..{MAX_KBAR_QUERY_DAYS}")
    if not 0.0 < float(args.max_traffic_fraction) <= HISTORICAL_MAX_TRAFFIC_FRACTION:
        raise ValueError(
            f"--max-traffic-fraction must be within (0, {HISTORICAL_MAX_TRAFFIC_FRACTION}]"
        )
    if args.max_queries < 0 or args.timeout_ms < 1 or args.retries < 0:
        raise ValueError("query limits, timeout, and retries must be non-negative")
    collections = {
        value.strip()
        for value in str(args.collections).split(",")
        if value.strip()
    }
    unknown = collections - set(COLLECTION_PRIORITY)
    if unknown:
        raise ValueError(f"unknown collections: {sorted(unknown)}")
    selected_codes = {
        value.strip().upper()
        for value in str(args.contract_codes).split(",")
        if value.strip()
    }
    if historical_query_is_protected() and not args.allow_market_hours:
        print("[shioaji-history] state=waiting_market protected=07:45-14:31")
        return MARKET_WINDOW_EXIT
    args.output_dir.mkdir(parents=True, exist_ok=True)
    lock = (args.output_dir / "download.lock").open("a+b")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("[shioaji-history] state=already_running")
        return 0

    import shioaji as sj

    key = os.environ.get("SHIOAJI_API_KEY", "").strip()
    secret = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
    if not key or not secret:
        raise RuntimeError("SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required")
    api = sj.Shioaji(simulation=bool(args.simulation))
    logged_in = False
    progress_path = args.output_dir / "progress.json"
    usage: tuple[int, int] | None = None
    try:
        api.set_event_callback(lambda *_args: None)
        try:
            api.login(api_key=key, secret_key=secret, subscribe_trade=False)
        except Exception as exc:
            if getattr(exc, "code", None) == 451 or "Too Many Connections" in str(exc):
                print("[shioaji-history] state=waiting_connection_capacity code=451")
                return CONNECTION_CAPACITY_EXIT
            raise
        logged_in = True
        completed_session = latest_completed_session(
            args.taifex_calendar, args.twse_calendar
        )
        if not args.no_refresh_inventory:
            inventory = discover_contracts(api, completed_session=completed_session)
            manifest = _write_inventory(
                args.output_dir, inventory, completed_session=completed_session
            )
            print(
                "[shioaji-history] inventory "
                f"current={manifest['current_contracts']} "
                f"weekly={manifest['by_collection']['latest_weekly_option']} "
                f"monthly={manifest['by_collection']['latest_monthly_option']} "
                f"exact_futures={manifest['by_collection']['exact_futures']} "
                f"indices={manifest['by_collection']['indices']}",
                flush=True,
            )
        if args.inventory_only:
            return 0
        rows = [
            row
            for row in load_inventory(args.output_dir)
            if row.collection in collections
            and (not selected_codes or row.code in selected_codes)
        ]
        tasks = build_tasks(
            args.output_dir, rows, chunk_days=int(args.chunk_days)
        )
        if args.dry_run:
            summary = _write_summary(
                args.output_dir,
                rows,
                chunk_days=int(args.chunk_days),
                state="planned",
                usage=None,
                progress_path=progress_path,
                persist=False,
            )
            print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
            return 0
        progress = PersistentProgress(
            progress_path,
            label="Shioaji option, exact-future, and index historical data",
            total=len(tasks),
            unit="API query receipts",
            basis=(
                "latest weekly options, latest monthly options, exact futures, then "
                "indices; newest chunks first; Tick dates derive from verified KBars"
            ),
        )
        heap = [task.heap_key() for task in tasks]
        heapq.heapify(heap)
        scheduled = {
            (task.method, task.code, task.start, task.end) for task in tasks
        }
        request_interval = resolve_request_interval(
            "shioaji_quote_query", args.request_interval
        )
        rate_limiter = SharedRateLimiter(
            request_interval, name="shioaji_quote_query"
        )
        print(
            "[shioaji-history] "
            f"{describe_rate_limit('shioaji_quote_query', request_interval)} "
            f"pending={len(tasks)} contracts={len(rows)}",
            flush=True,
        )
        completed_queries = 0
        state = "running"
        while heap and (not args.max_queries or completed_queries < args.max_queries):
            if historical_query_is_protected() and not args.allow_market_hours:
                state = "waiting_market"
                break
            try:
                usage = _check_traffic_budget(
                    api, max_fraction=float(args.max_traffic_fraction)
                )
            except TrafficBudgetReached:
                state = "waiting_traffic"
                break
            *_key, task = heapq.heappop(heap)
            try:
                receipt, added = _query_task(
                    api,
                    task,
                    output_root=args.output_dir,
                    timeout_ms=int(args.timeout_ms),
                    retries=int(args.retries),
                    retry_backoff=float(args.retry_backoff),
                    rate_limiter=rate_limiter,
                )
            except LookupError as exc:
                # Retained expired catalog rows remain explicit unresolved gaps.
                progress.update(
                    f"{task.contract.collection}:{task.code}:{task.method}",
                    "contract_unavailable",
                )
                print(f"[shioaji-history] status=contract_unavailable error={exc}")
                continue
            completed_queries += 1
            for new_task in added:
                identity = (
                    new_task.method,
                    new_task.code,
                    new_task.start,
                    new_task.end,
                )
                if identity in scheduled:
                    continue
                scheduled.add(identity)
                heapq.heappush(heap, new_task.heap_key())
                progress.total += 1
            progress.update(
                f"{task.contract.collection}:{task.code}:{task.method}",
                str(receipt["status"]),
            )
            # The full receipt audit is O(total targets).  Progress JSON is
            # already refreshed per query, so audit the canonical summary at a
            # bounded cadence instead of turning every ten API responses into
            # a 31k+ filesystem scan.
            if completed_queries == 1 or completed_queries % 100 == 0:
                _write_summary(
                    args.output_dir,
                    rows,
                    chunk_days=int(args.chunk_days),
                    state=state,
                    usage=usage,
                    progress_path=progress_path,
                )
            print(
                f"[shioaji-history] query={completed_queries} "
                f"collection={task.contract.collection} contract={task.code} "
                f"method={task.method} range={task.start}..{task.end} "
                f"status={receipt['status']} rows={int(receipt.get('rows') or 0):,} "
                f"traffic={usage[0]:,}/{usage[1]:,}",
                flush=True,
            )
        summary = _write_summary(
            args.output_dir,
            rows,
            chunk_days=int(args.chunk_days),
            state=state,
            usage=usage,
            progress_path=progress_path,
        )
        final_state = str(summary["state"])
        progress.finish(
            state="complete" if final_state == "complete" else final_state,
            require_exact=final_state == "complete",
        )
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
        if final_state == "waiting_traffic":
            return TRAFFIC_BUDGET_EXIT
        if final_state == "waiting_market":
            return MARKET_WINDOW_EXIT
        return 0
    finally:
        if logged_in:
            try:
                api.logout()
            except Exception:
                pass
        lock.close()


if __name__ == "__main__":
    raise SystemExit(main())
