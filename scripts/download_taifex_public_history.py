#!/usr/bin/env python3
"""Archive missing official TAIFEX public history with causal receipts.

The price, settlement, open-interest and recent tick archives already have
dedicated collectors.  This collector fills the independent public state that
is otherwise lost from the rolling web pages:

* TXO put/call ratios from the first listed month (2001-12);
* futures and options institutional positioning within the free three-year
  query window;
* call/put institutional positioning within the same window; and
* TX-family large-trader concentration within the same window.

Every response is retained as a compressed immutable raw receipt, parsed to a
small per-request Parquet shard, and then merged from verified shards.  All
positioning rows are marked as post-close facts and become causal only on the
next receipt-verified TAIFEX session.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Final, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import (  # noqa: E402
    atomic_write_bytes,
    atomic_write_parquet,
)
from downloader.common import SharedRateLimiter, atomic_write_text  # noqa: E402
from scripts.taifex_daily_download_common import (  # noqa: E402
    month_ranges,
    sha256_path,
)


CONTRACT_VERSION: Final[int] = 1
USER_AGENT: Final[str] = "stockAgent/taifex-public-history-research"
PUT_CALL_URL: Final[str] = "https://www.taifex.com.tw/cht/3/pcRatioDown"


@dataclass(frozen=True, slots=True)
class PositioningSpec:
    name: str
    url: str
    parser: str
    payload_extra: tuple[tuple[str, str], ...] = ()


POSITIONING_SPECS: Final[tuple[PositioningSpec, ...]] = (
    PositioningSpec(
        "institutional_futures",
        "https://www.taifex.com.tw/cht/3/futContractsDate",
        "institutional",
    ),
    PositioningSpec(
        "institutional_options",
        "https://www.taifex.com.tw/cht/3/optContractsDate",
        "institutional",
    ),
    PositioningSpec(
        "institutional_calls_puts",
        "https://www.taifex.com.tw/cht/3/callsAndPutsDate",
        "institutional_calls_puts",
    ),
    PositioningSpec(
        "large_trader_futures_tx",
        "https://www.taifex.com.tw/cht/3/largeTraderFutQry",
        "large_trader",
        (("contractId", "TX"), ("contractId2", "TX")),
    ),
)

INSTITUTIONAL_COLUMNS: Final[tuple[str, ...]] = (
    "sequence",
    "product_name",
    "participant_type",
    "trade_long_lots",
    "trade_long_value_thousand_twd",
    "trade_short_lots",
    "trade_short_value_thousand_twd",
    "trade_net_lots",
    "trade_net_value_thousand_twd",
    "open_interest_long_lots",
    "open_interest_long_value_thousand_twd",
    "open_interest_short_lots",
    "open_interest_short_value_thousand_twd",
    "open_interest_net_lots",
    "open_interest_net_value_thousand_twd",
)
CALL_PUT_COLUMNS: Final[tuple[str, ...]] = (
    "sequence",
    "product_name",
    "option_side",
    "participant_type",
    "trade_long_lots",
    "trade_long_value_thousand_twd",
    "trade_short_lots",
    "trade_short_value_thousand_twd",
    "trade_net_lots",
    "trade_net_value_thousand_twd",
    "open_interest_long_lots",
    "open_interest_long_value_thousand_twd",
    "open_interest_short_lots",
    "open_interest_short_value_thousand_twd",
    "open_interest_net_lots",
    "open_interest_net_value_thousand_twd",
)
LARGE_TRADER_COLUMNS: Final[tuple[str, ...]] = (
    "contract_name",
    "expiry_bucket",
    "buy_top5_positions",
    "buy_top5_specific_positions",
    "buy_top5_share_pct",
    "buy_top5_specific_share_pct",
    "buy_top10_positions",
    "buy_top10_specific_positions",
    "buy_top10_share_pct",
    "buy_top10_specific_share_pct",
    "sell_top5_positions",
    "sell_top5_specific_positions",
    "sell_top5_share_pct",
    "sell_top5_specific_share_pct",
    "sell_top10_positions",
    "sell_top10_specific_positions",
    "sell_top10_share_pct",
    "sell_top10_specific_share_pct",
    "market_open_interest",
)
_PAIR_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^\s*([+-]?[\d,.]+)\s*(?:\(([+-]?[\d,.]+)\))?\s*$"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise RuntimeError(f"immutable artifact changed: {path}")
        return
    atomic_write_bytes(path, content, durable=True)


def _atomic_write_parquet(frame: pd.DataFrame, path: Path) -> None:
    table = pa.Table.from_pandas(frame, preserve_index=False)
    atomic_write_parquet(path, table, compression="zstd")


def _write_json(path: Path, payload: object) -> None:
    atomic_write_text(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )


def _relative(path: Path, root: Path) -> str:
    return str(path.resolve().relative_to(root.resolve()))


def _subtract_years(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year - years)
    except ValueError:
        return day.replace(year=day.year - years, day=28)


def _load_sessions(path: Path, end: date) -> tuple[list[date], str]:
    if not path.is_file():
        raise FileNotFoundError(f"TAIFEX session parquet does not exist: {path}")
    table = pq.read_table(path, columns=["date"])
    values = pd.to_datetime(table.column("date").to_pandas(), errors="raise")
    sessions = sorted({item.date() for item in values if item.date() <= end})
    if not sessions:
        raise RuntimeError(f"TAIFEX session parquet has no dates through {end}")
    return sessions, sha256_path(path)


def _next_session_map(sessions: Iterable[date]) -> dict[date, date]:
    ordered = sorted(set(sessions))
    return dict(zip(ordered, ordered[1:]))


def _clean_label(value: object) -> str:
    return re.sub(r"\s+", "", str(value)).strip()


def _integer_series(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.strip()
    cleaned = cleaned.replace({"": pd.NA, "-": pd.NA, "--": pd.NA, "nan": pd.NA})
    return pd.to_numeric(cleaned, errors="raise").astype("Int64")


def _parse_put_call(content: bytes, next_sessions: dict[date, date]) -> pd.DataFrame:
    decoded = content.decode("cp950", errors="strict")
    # TAIFEX appends a trailing comma to every row.  Without index_col=False,
    # pandas infers the date as an index and shifts every value one column.
    frame = pd.read_csv(StringIO(decoded), index_col=False)
    frame = frame.dropna(axis=1, how="all")
    if frame.shape[1] != 7:
        raise ValueError(f"unexpected put/call ratio column count: {frame.shape[1]}")
    frame.columns = [
        "date",
        "put_volume",
        "call_volume",
        "put_call_volume_ratio_pct",
        "put_open_interest",
        "call_open_interest",
        "put_call_open_interest_ratio_pct",
    ]
    frame["date"] = pd.to_datetime(frame["date"], format="%Y/%m/%d", errors="raise")
    for column in (
        "put_volume",
        "call_volume",
        "put_open_interest",
        "call_open_interest",
    ):
        frame[column] = _integer_series(frame[column])
    for column in (
        "put_call_volume_ratio_pct",
        "put_call_open_interest_ratio_pct",
    ):
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float64")
    frame["available_date"] = pd.to_datetime(
        [next_sessions.get(item.date()) for item in frame["date"]]
    )
    frame["published_after_close"] = True
    frame["availability_rule"] = "next_receipt_verified_taifex_session"
    return frame.sort_values("date").reset_index(drop=True)


def _read_one_html_table(content: bytes) -> pd.DataFrame:
    decoded = content.decode("utf-8", errors="strict")
    tables = pd.read_html(StringIO(decoded))
    candidates = [table for table in tables if table.shape[0] and table.shape[1] >= 8]
    if len(candidates) != 1:
        raise ValueError(f"expected one TAIFEX data table, found {len(candidates)}")
    return candidates[0]


def _causal_columns(
    frame: pd.DataFrame,
    requested_date: date,
    next_sessions: dict[date, date],
) -> pd.DataFrame:
    frame.insert(0, "date", pd.Timestamp(requested_date))
    frame["available_date"] = pd.Timestamp(next_sessions[requested_date])
    frame["published_after_close"] = True
    frame["availability_rule"] = "next_receipt_verified_taifex_session"
    return frame


def _parse_institutional(
    content: bytes,
    requested_date: date,
    next_sessions: dict[date, date],
    *,
    calls_puts: bool,
) -> pd.DataFrame:
    frame = _read_one_html_table(content)
    columns = CALL_PUT_COLUMNS if calls_puts else INSTITUTIONAL_COLUMNS
    if frame.shape[1] != len(columns):
        raise ValueError(
            f"unexpected institutional column count: {frame.shape[1]} != {len(columns)}"
        )
    frame.columns = list(columns)
    # The sequence cell becomes a label on TAIFEX's subtotal/total rows.  Keep
    # it as text so those economically useful aggregates are retained rather
    # than silently dropped or coerced to null.
    text_columns = ["sequence", "product_name", "participant_type"]
    if calls_puts:
        text_columns.append("option_side")
    for column in text_columns:
        frame[column] = frame[column].map(_clean_label)
    for column in columns:
        if column not in text_columns:
            frame[column] = _integer_series(frame[column])
    return _causal_columns(frame, requested_date, next_sessions)


def _parse_pair(
    value: object, *, percent: bool
) -> tuple[float | int | None, float | int | None]:
    text = re.sub(r"\s+", "", str(value).replace("％", "%").replace("%", ""))
    if text in {"", "-", "--", "nan"}:
        return None, None
    match = _PAIR_PATTERN.fullmatch(text)
    if match is None:
        raise ValueError(f"unrecognized large-trader value: {value!r}")
    parsed: list[float | int | None] = []
    for raw in match.groups():
        if raw is None:
            parsed.append(None)
            continue
        normalized = raw.replace(",", "")
        parsed.append(float(normalized) if percent else int(normalized))
    return parsed[0], parsed[1]


def _parse_large_trader(
    content: bytes,
    requested_date: date,
    next_sessions: dict[date, date],
) -> pd.DataFrame:
    source = _read_one_html_table(content)
    if source.shape[1] != 11:
        raise ValueError(f"unexpected large-trader column count: {source.shape[1]}")
    rows: list[dict[str, object]] = []
    for values in source.itertuples(index=False, name=None):
        row: dict[str, object] = {
            "contract_name": re.sub(r"\s+", "", str(values[0])),
            "expiry_bucket": re.sub(r"\s+", "", str(values[1])),
        }
        prefixes = ("buy_top5", "buy_top10", "sell_top5", "sell_top10")
        for index, prefix in enumerate(prefixes):
            positions, specific_positions = _parse_pair(
                values[2 + index * 2], percent=False
            )
            share, specific_share = _parse_pair(values[3 + index * 2], percent=True)
            row[f"{prefix}_positions"] = positions
            row[f"{prefix}_specific_positions"] = specific_positions
            row[f"{prefix}_share_pct"] = share
            row[f"{prefix}_specific_share_pct"] = specific_share
        row["market_open_interest"] = _parse_pair(values[10], percent=False)[0]
        rows.append(row)
    frame = pd.DataFrame(rows, columns=LARGE_TRADER_COLUMNS)
    integer_columns = [
        column
        for column in LARGE_TRADER_COLUMNS
        if column.endswith("positions") or column == "market_open_interest"
    ]
    for column in integer_columns:
        frame[column] = pd.array(frame[column], dtype="Int64")
    for column in [column for column in LARGE_TRADER_COLUMNS if column.endswith("pct")]:
        frame[column] = pd.array(frame[column], dtype="Float64")
    return _causal_columns(frame, requested_date, next_sessions)


def _parse_positioning(
    spec: PositioningSpec,
    content: bytes,
    requested_date: date,
    next_sessions: dict[date, date],
) -> pd.DataFrame:
    requested_text = requested_date.strftime("%Y/%m/%d")
    if requested_text.encode("ascii") not in content:
        raise ValueError(
            f"TAIFEX response does not acknowledge requested date {requested_text}"
        )
    if spec.parser == "institutional":
        return _parse_institutional(
            content, requested_date, next_sessions, calls_puts=False
        )
    if spec.parser == "institutional_calls_puts":
        return _parse_institutional(
            content, requested_date, next_sessions, calls_puts=True
        )
    if spec.parser == "large_trader":
        return _parse_large_trader(content, requested_date, next_sessions)
    raise ValueError(f"unknown parser: {spec.parser}")


def _receipt_path(root: Path, dataset: str, key: str) -> Path:
    return root / "receipts" / dataset / key[:4] / f"{key}.json"


def _valid_receipt(root: Path, dataset: str, key: str) -> dict[str, object] | None:
    path = _receipt_path(root, dataset, key)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (
            payload.get("contract_version") != CONTRACT_VERSION
            or payload.get("dataset") != dataset
            or payload.get("request_key") != key
            or payload.get("status") != "complete"
        ):
            return None
        for prefix in ("raw", "normalized"):
            artifact = root / str(payload[f"{prefix}_path"])
            if not artifact.is_file():
                return None
            if artifact.stat().st_size != int(payload[f"{prefix}_bytes"]):
                return None
            if sha256_path(artifact) != payload[f"{prefix}_sha256"]:
                return None
        return payload
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _receipt_covers_request(
    receipt: dict[str, object] | None,
    request_payload: dict[str, str],
) -> bool:
    return receipt is not None and receipt.get("request_payload") == request_payload


def _persist_response(
    root: Path,
    *,
    dataset: str,
    key: str,
    url: str,
    request_payload: dict[str, str],
    content: bytes,
    frame: pd.DataFrame,
    fetched_at: str,
    headers: dict[str, str],
) -> dict[str, object]:
    response_sha256 = _sha256_bytes(content)
    stored = gzip.compress(content, compresslevel=9, mtime=0)
    raw_path = root / "raw" / dataset / key[:4] / f"{key}_{response_sha256[:16]}.raw.gz"
    shard_path = root / "shards" / dataset / key[:4] / f"{key}.parquet"
    receipt_path = _receipt_path(root, dataset, key)
    _atomic_write_bytes(raw_path, stored)
    _atomic_write_parquet(frame, shard_path)
    payload: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "dataset": dataset,
        "request_key": key,
        "status": "complete",
        "source_url": url,
        "request_method": "POST",
        "request_payload": request_payload,
        "fetched_at_utc": fetched_at,
        "published_after_close": True,
        "availability_rule": "next_receipt_verified_taifex_session",
        "rows": len(frame),
        "response_bytes": len(content),
        "response_sha256": response_sha256,
        "response_content_type": headers.get("content-type"),
        "response_content_disposition": headers.get("content-disposition"),
        "raw_path": _relative(raw_path, root),
        "raw_bytes": raw_path.stat().st_size,
        "raw_sha256": sha256_path(raw_path),
        "normalized_path": _relative(shard_path, root),
        "normalized_bytes": shard_path.stat().st_size,
        "normalized_sha256": sha256_path(shard_path),
    }
    _write_json(receipt_path, payload)
    return payload


def _post(
    session: requests.Session,
    limiter: SharedRateLimiter,
    url: str,
    payload: dict[str, str],
    *,
    attempts: int,
) -> tuple[bytes, str, dict[str, str]]:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            limiter.wait()
            response = session.post(url, data=payload, timeout=120)
            response.raise_for_status()
            content = response.content
            if len(content) < 100:
                raise RuntimeError(f"TAIFEX returned only {len(content)} bytes")
            return (
                content,
                _utc_now(),
                {key.lower(): value for key, value in response.headers.items()},
            )
        except Exception as exc:  # requests and parser validation retry at caller
            last_error = exc
            if attempt == attempts:
                break
            response = getattr(exc, "response", None)
            retry_after = (
                None if response is None else response.headers.get("Retry-After")
            )
            if response is not None and response.status_code == 429:
                try:
                    delay = max(60.0, float(retry_after or 0.0))
                except ValueError:
                    delay = 60.0
                print(
                    f"[rate-limit] TAIFEX HTTP 429; cooling down {delay:.0f}s "
                    f"before attempt {attempt + 1}/{attempts}",
                    flush=True,
                )
            else:
                delay = min(30.0, float(2**attempt))
            limiter.defer(delay)
            time.sleep(min(delay, 60.0))
    raise RuntimeError(
        f"TAIFEX POST failed after {attempts} attempts: {url}"
    ) from last_error


def _download_put_call(
    root: Path,
    session: requests.Session,
    limiter: SharedRateLimiter,
    next_sessions: dict[date, date],
    start: date,
    end: date,
    attempts: int,
    progress: dict[str, object],
) -> None:
    ranges = list(month_ranges(start, end))
    for index, (range_start, range_end) in enumerate(ranges, start=1):
        key = range_start.strftime("%Y-%m")
        request_payload = {
            "queryStartDate": range_start.strftime("%Y/%m/%d"),
            "queryEndDate": range_end.strftime("%Y/%m/%d"),
            "down_type": "1",
        }
        receipt = _valid_receipt(root, "put_call_ratio", key)
        if not _receipt_covers_request(receipt, request_payload):
            content, fetched_at, headers = _post(
                session, limiter, PUT_CALL_URL, request_payload, attempts=attempts
            )
            frame = _parse_put_call(content, next_sessions)
            if frame.empty:
                raise RuntimeError(f"put/call ratio source is empty for {key}")
            _persist_response(
                root,
                dataset="put_call_ratio",
                key=key,
                url=PUT_CALL_URL,
                request_payload=request_payload,
                content=content,
                frame=frame,
                fetched_at=fetched_at,
                headers=headers,
            )
        progress.update(
            phase="put_call_ratio",
            current=key,
            completed=index,
            total=len(ranges),
            updated_at_utc=_utc_now(),
        )
        _write_json(root / "progress.json", progress)
        print(f"[put-call] {index}/{len(ranges)} {key}", flush=True)


def _download_positioning(
    root: Path,
    session: requests.Session,
    limiter: SharedRateLimiter,
    next_sessions: dict[date, date],
    sessions: list[date],
    attempts: int,
    progress: dict[str, object],
) -> None:
    total = len(sessions) * len(POSITIONING_SPECS)
    completed = 0
    for requested_date in sessions:
        key = requested_date.isoformat()
        for spec in POSITIONING_SPECS:
            completed += 1
            if _valid_receipt(root, spec.name, key) is None:
                if spec.parser == "large_trader":
                    request_payload = {
                        "queryDate": requested_date.strftime("%Y/%m/%d"),
                        **dict(spec.payload_extra),
                    }
                else:
                    request_payload = {
                        "queryType": "1",
                        "queryDate": requested_date.strftime("%Y/%m/%d"),
                        "goDay": "",
                        "doQuery": "1",
                        "dateaddcnt": "",
                    }
                content, fetched_at, headers = _post(
                    session, limiter, spec.url, request_payload, attempts=attempts
                )
                try:
                    frame = _parse_positioning(
                        spec, content, requested_date, next_sessions
                    )
                except Exception:
                    limiter.defer(5.0)
                    raise
                if frame.empty:
                    raise RuntimeError(f"{spec.name} source is empty for {key}")
                _persist_response(
                    root,
                    dataset=spec.name,
                    key=key,
                    url=spec.url,
                    request_payload=request_payload,
                    content=content,
                    frame=frame,
                    fetched_at=fetched_at,
                    headers=headers,
                )
            progress.update(
                phase=spec.name,
                current=key,
                completed=completed,
                total=total,
                updated_at_utc=_utc_now(),
            )
            _write_json(root / "progress.json", progress)
            print(f"[positioning] {completed}/{total} {spec.name} {key}", flush=True)


def _merge_dataset(root: Path, dataset: str) -> dict[str, object]:
    receipt_paths = sorted((root / "receipts" / dataset).glob("*/*.json"))
    shard_paths: list[Path] = []
    for receipt_path in receipt_paths:
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
        valid = _valid_receipt(root, dataset, str(payload.get("request_key", "")))
        if valid is None:
            raise RuntimeError(f"invalid receipt blocks merge: {receipt_path}")
        shard_paths.append(root / str(valid["normalized_path"]))
    if not shard_paths:
        raise RuntimeError(f"no verified shards found for {dataset}")
    tables = [pq.read_table(path) for path in shard_paths]
    table = pa.concat_tables(tables, promote_options="default")
    frame = table.to_pandas()
    sort_columns = [
        column for column in ("date", "sequence", "expiry_bucket") if column in frame
    ]
    if sort_columns:
        frame = frame.sort_values(sort_columns, kind="stable")
    frame = frame.drop_duplicates().reset_index(drop=True)
    target = root / "normalized" / f"{dataset}.parquet"
    _atomic_write_parquet(frame, target)
    dates = pd.to_datetime(frame["date"], errors="raise")
    return {
        "dataset": dataset,
        "status": "complete",
        "requests": len(shard_paths),
        "rows": len(frame),
        "first_date": dates.min().date().isoformat(),
        "last_date": dates.max().date().isoformat(),
        "output_path": _relative(target, root),
        "output_bytes": target.stat().st_size,
        "output_sha256": sha256_path(target),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data_taifex_public_history")
    parser.add_argument(
        "--session-parquet",
        default="data_tw_index_futures/day_session_contracts.parquet",
    )
    parser.add_argument(
        "--end-date", type=date.fromisoformat, default=date.today() - timedelta(days=1)
    )
    parser.add_argument(
        "--put-call-start", type=date.fromisoformat, default=date(2001, 12, 1)
    )
    parser.add_argument(
        "--positioning-start",
        type=date.fromisoformat,
        default=None,
        help="Defaults to the first date in TAIFEX's free rolling three-year window.",
    )
    parser.add_argument("--request-interval", type=float, default=1.0)
    parser.add_argument("--attempts", type=int, default=8)
    parser.add_argument(
        "--phase",
        choices=("all", "put-call", "positioning"),
        default="all",
    )
    args = parser.parse_args()
    if args.request_interval < 0.1:
        parser.error("--request-interval must be at least 0.1 seconds")
    if args.attempts < 1:
        parser.error("--attempts must be positive")

    root = Path(args.output_dir).expanduser().resolve()
    session_path = Path(args.session_parquet).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    sessions, calendar_sha256 = _load_sessions(session_path, args.end_date)
    effective_end = sessions[-1]
    positioning_start = args.positioning_start or (
        _subtract_years(args.end_date, 3) + timedelta(days=1)
    )
    positioning_sessions = [
        item for item in sessions if positioning_start <= item < effective_end
    ]
    # The last retained session has no receipt-verified next session yet, so it
    # cannot receive a fail-closed availability_date until the calendar advances.
    next_sessions = _next_session_map(sessions)
    if args.put_call_start > effective_end:
        parser.error("--put-call-start is after the latest verified session")
    if not positioning_sessions and args.phase in {"all", "positioning"}:
        parser.error("no positioning sessions have a receipt-verified next session")

    progress: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "state": "running",
        "phase": args.phase,
        "requested_end_date": args.end_date.isoformat(),
        "effective_end_date": effective_end.isoformat(),
        "positioning_start_date": positioning_start.isoformat(),
        "session_calendar_path": str(session_path),
        "session_calendar_sha256": calendar_sha256,
        "started_at_utc": _utc_now(),
        "updated_at_utc": _utc_now(),
    }
    _write_json(root / "progress.json", progress)

    http = requests.Session()
    http.headers.update({"User-Agent": USER_AGENT})
    limiter = SharedRateLimiter(
        args.request_interval,
        name="taifex_public_history",
    )
    try:
        if args.phase in {"all", "put-call"}:
            _download_put_call(
                root,
                http,
                limiter,
                next_sessions,
                args.put_call_start,
                effective_end,
                args.attempts,
                progress,
            )
        if args.phase in {"all", "positioning"}:
            _download_positioning(
                root,
                http,
                limiter,
                next_sessions,
                positioning_sessions,
                args.attempts,
                progress,
            )
    except Exception as exc:
        progress.update(
            state="failed",
            error_type=type(exc).__name__,
            error=str(exc),
            failed_at_utc=_utc_now(),
            updated_at_utc=_utc_now(),
        )
        _write_json(root / "progress.json", progress)
        raise

    selected = (
        ["put_call_ratio"]
        if args.phase == "put-call"
        else [spec.name for spec in POSITIONING_SPECS]
        if args.phase == "positioning"
        else ["put_call_ratio", *(spec.name for spec in POSITIONING_SPECS)]
    )
    summaries = [_merge_dataset(root, dataset) for dataset in selected]
    manifest = {
        "contract_version": CONTRACT_VERSION,
        "dataset": "taifex_public_history",
        "status": "complete",
        "source_authority": "Taiwan Futures Exchange",
        "source_pages": [
            "https://www.taifex.com.tw/cht/3/pcRatio",
            "https://www.taifex.com.tw/cht/3/futContractsDate",
            "https://www.taifex.com.tw/cht/3/optContractsDate",
            "https://www.taifex.com.tw/cht/3/largeTraderFutQry",
        ],
        "requested_end_date": args.end_date.isoformat(),
        "effective_end_date": effective_end.isoformat(),
        "positioning_start_date": positioning_start.isoformat(),
        "positioning_free_history_boundary": "rolling_three_year_web_query_window",
        "older_positioning_history": "requires_taifex_historical_data_application",
        "published_after_close": True,
        "availability_rule": "next_receipt_verified_taifex_session",
        "session_calendar_path": str(session_path),
        "session_calendar_sha256": calendar_sha256,
        "datasets": summaries,
        "completed_at_utc": _utc_now(),
    }
    _write_json(root / "manifest.json", manifest)
    progress.update(
        state="complete",
        phase="complete",
        updated_at_utc=_utc_now(),
        completed_at_utc=_utc_now(),
        datasets=summaries,
    )
    _write_json(root / "progress.json", progress)
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
