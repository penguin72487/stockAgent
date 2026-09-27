#!/usr/bin/env python3
"""Archive missing official TAIFEX public history with causal receipts.

The price, settlement, open-interest and recent tick archives already have
dedicated collectors.  This collector fills the independent public state that
is otherwise lost from the rolling web pages:

* TXO put/call ratios from the first listed month (2001-12);
* futures and options institutional positioning within the free three-year
  query window;
* call/put institutional positioning within the same window; and
* TX-family large-trader concentration within the same window; and
* all-product futures/options large-trader CSV history, in official three-
  calendar-month windows (a separate grain from the legacy TX HTML table).

Every response is retained as a compressed immutable raw receipt, parsed to a
small per-request Parquet shard, and then merged from verified shards.  All
positioning rows are marked as post-close facts and become causal only on the
next receipt-verified TAIFEX session.
"""

from __future__ import annotations

import argparse
import calendar
import csv
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
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import (  # noqa: E402
    _temporary_path,
    atomic_write_bytes,
    atomic_write_parquet,
)
from downloader.common import SharedRateLimiter, atomic_write_text  # noqa: E402
from scripts.taifex_daily_download_common import (  # noqa: E402
    month_ranges,
    sha256_path,
)


CONTRACT_VERSION: Final[int] = 1
LARGE_TRADER_PARSER_VERSION: Final[int] = 2
LARGE_TRADER_HISTORY_START: Final[date] = date(2004, 7, 1)
LARGE_TRADER_FIRST_PUBLICATION: Final[date] = date(2005, 1, 3)
LARGE_TRADER_ALIGNMENT_VERSION: Final[int] = 3
LARGE_TRADER_HISTORY_SOURCES: Final[tuple[str, ...]] = (
    "https://www.taifex.com.tw/cht/3/largeTraderOptQryDetail",
    "https://www.taifex.com.tw/cht/1/historyOfSurveillance",
)
LARGE_TRADER_LAUNCH_AVAILABILITY_RULE: Final[str] = (
    "first_publication_2005_01_03_post_close_inferred_next_verified_session"
)
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

LARGE_TRADER_RANGE_SPECS: Final[tuple[PositioningSpec, ...]] = (
    PositioningSpec(
        "large_trader_futures_all",
        "https://www.taifex.com.tw/cht/3/largeTraderFutDown",
        "large_trader_futures_csv",
    ),
    PositioningSpec(
        "large_trader_options_all",
        "https://www.taifex.com.tw/cht/3/largeTraderOptDown",
        "large_trader_options_csv",
    ),
)
LARGE_TRADER_CSV_HEADER: Final[tuple[str, ...]] = (
    "日期", "商品(契約)", "商品名稱(契約名稱)", "到期月份(週別)",
    "交易人類別", "前五大交易人買方", "前五大交易人賣方",
    "前十大交易人買方", "前十大交易人賣方", "全市場未沖銷部位數",
)
LARGE_TRADER_CSV_COLUMNS: Final[tuple[str, ...]] = (
    "date", "product_code", "product_name", "expiry_bucket", "trader_category",
    "buy_top5_positions", "sell_top5_positions", "buy_top10_positions",
    "sell_top10_positions", "market_open_interest",
)


class UnverifiedHistoryResponse(ValueError):
    """The official site says no data, not that the history is complete."""

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
    # Acquisition and causal alignment have different clocks.  The exchange
    # can publish today's report before our receipt-backed session calendar
    # contains its next session.  Keep the report now; do not guess a weekday
    # or hold the HTTP request until that later session has traded.
    next_session = next_sessions.get(requested_date)
    # Existing request shards store day-valued timestamps at millisecond
    # precision.  An untyped all-NaT scalar would become timestamp[ns] and
    # break Arrow concatenation with those receipt-backed historical shards.
    frame["available_date"] = pd.Series(
        next_session, index=frame.index, dtype="datetime64[ms]"
    )
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


def _quarter_ranges(start: date, end: date) -> Iterable[tuple[date, date]]:
    """Respect the download form's maximum of three calendar months."""
    cursor = start
    while cursor <= end:
        last_month = ((cursor.month - 1) // 3 + 1) * 3
        boundary = date(cursor.year, last_month, calendar.monthrange(cursor.year, last_month)[1])
        finish = min(boundary, end)
        yield cursor, finish
        cursor = finish + timedelta(days=1)


def _parse_large_trader_csv(
    spec: PositioningSpec,
    content: bytes,
    start: date,
    end: date,
    next_sessions: dict[date, date],
) -> pd.DataFrame:
    # The download is CP950, but the site's HTTP-200 "no data" HTML is UTF-8.
    # Content-Type alone cannot distinguish them: valid CSV also says text/html.
    if content.lstrip().lower().startswith((b"<!doctype", b"<html", b"<head")):
        if "查無資料" in content.decode("utf-8", errors="replace"):
            raise UnverifiedHistoryResponse("official response says 查無資料; history not verified")
        raise ValueError("TAIFEX returned HTML rather than a CSV download")
    records = csv.reader(StringIO(content.decode("cp950", errors="strict")))
    header = next(records, [])
    options = spec.parser == "large_trader_options_csv"
    expected = list(LARGE_TRADER_CSV_HEADER)
    columns = list(LARGE_TRADER_CSV_COLUMNS)
    if options:
        expected.insert(3, "買賣權")
        columns.insert(3, "option_side")
    if [value.strip().lstrip("\ufeff") for value in header] != expected:
        raise ValueError(f"unexpected large-trader CSV header: {header!r}")
    rows: list[list[str]] = []
    source_row_numbers: list[int] = []
    footer: list[str] = []
    for record in records:
        values = [value.strip() for value in record]
        if not values or not any(values):
            continue
        if re.fullmatch(r"\d{4}/\d{2}/\d{2}", values[0]):
            if footer or len(values) != len(columns):
                raise ValueError("malformed or interrupted large-trader CSV data row")
            rows.append(values)
            source_row_numbers.append(records.line_num)
        elif len(values) == 1 and values[0].startswith(
            ("月份類別格式", "交易人類別格式", "-表", "備註一", "備註二", "備註三")
        ):
            footer.append(values[0])
        else:
            raise ValueError(f"unrecognized large-trader CSV record: {values[:2]!r}")
    # The official download has three explanatory footer lines.  Requiring
    # them catches interrupted HTTP-200 bodies even if their last row is valid.
    if len(footer) != 3:
        raise ValueError("large-trader CSV is missing its complete official footer")
    if not rows:
        raise UnverifiedHistoryResponse("official CSV contains no observations; history not verified")
    frame = pd.DataFrame(rows, columns=columns)
    frame["date"] = pd.to_datetime(frame["date"], format="%Y/%m/%d", errors="raise")
    observed = set(frame["date"].dt.date)
    if any(day < start or day > end for day in observed):
        raise ValueError("large-trader CSV returned dates outside the requested range")
    if options and not frame["option_side"].isin(("買權", "賣權")).all():
        raise ValueError("unknown large-trader option side")
    for column in LARGE_TRADER_CSV_COLUMNS[5:]:
        frame[column] = _integer_series(frame[column])
        if (frame[column].dropna() < 0).any():
            raise ValueError(f"negative large-trader position count: {column}")
    grain = ["date", "product_code", "expiry_bucket", "trader_category"]
    if options:
        grain.append("option_side")
    # Some historical official downloads emit two identical all-dash TX
    # rows when no weekly contract exists.  The CSV footer documents '-' as
    # absence, not category 0/1 and not zero positions.  Keep each source row
    # (including its line identity) without inventing its trader category.
    placeholder = (
        frame["expiry_bucket"].eq("-")
        & frame["trader_category"].eq("-")
        & frame[list(LARGE_TRADER_CSV_COLUMNS[5:])].isna().all(axis=1)
        & any("無週到期" in line for line in footer)
    )
    if frame.loc[~placeholder].duplicated(grain).any():
        raise ValueError("duplicate large-trader natural keys in one response")
    if frame[["product_code", "product_name", "expiry_bucket"]].eq("").any().any():
        raise ValueError("large-trader CSV is missing a product or expiry label")
    frame["available_date"] = pd.Series(
        [next_sessions.get(max(day, LARGE_TRADER_FIRST_PUBLICATION)) for day in frame["date"].dt.date],
        dtype="datetime64[ms]",
    )
    frame["published_after_close"] = True
    frame["availability_rule"] = "next_receipt_verified_taifex_session"
    frame.loc[frame["date"].dt.date < LARGE_TRADER_FIRST_PUBLICATION, "availability_rule"] = (
        LARGE_TRADER_LAUNCH_AVAILABILITY_RULE
    )
    frame["source_row_number"] = pd.array(source_row_numbers, dtype="Int64")
    frame["row_quality"] = "observed"
    frame.loc[~frame["trader_category"].isin(("0", "1")), "row_quality"] = "unknown_trader_category"
    frame.loc[placeholder, "row_quality"] = "source_no_weekly_contract_placeholder"
    return frame.sort_values(grain, kind="stable").reset_index(drop=True)


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
    coverage: dict[str, object] | None = None,
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
        **_availability_summary(frame),
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
    if coverage is not None:
        payload["coverage"] = coverage
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


def _range_status_path(root: Path, dataset: str, key: str) -> Path:
    return root / "range_status" / dataset / key[:4] / f"{key}.json"


def _failed_range_is_reparseable(status: dict[str, object]) -> bool:
    """Only parser failures may reuse raw bytes; not source no-data or HTTP errors."""
    if status.get("status") != "failed" or not status.get("raw_path"):
        return False
    if status.get("failure_stage") == "parse":
        return True
    # Receipts written before parser-version tracking need a narrow migration
    # predicate.  A generic ValueError with bytes is not sufficient proof.
    return status.get("error_type") == "ValueError" and str(status.get("error", "")).startswith((
        "unknown large-trader category", "unknown large-trader option side",
        "unexpected large-trader CSV header", "malformed or interrupted large-trader CSV",
        "unrecognized large-trader CSV record", "large-trader CSV is missing",
        "large-trader CSV returned dates", "negative large-trader position count",
        "duplicate large-trader natural keys",
    ))


def _read_failed_range_response(
    root: Path, spec: PositioningSpec, status: dict[str, object],
    request_payload: dict[str, str],
) -> tuple[bytes, str, dict[str, str]]:
    if (
        not _failed_range_is_reparseable(status)
        or status.get("dataset") != spec.name
        or status.get("source_url") != spec.url
        or status.get("request_payload") != request_payload
    ):
        raise ValueError("failed raw response does not match this parser and request")
    raw_path = (root / str(status["raw_path"])).resolve()
    raw_path.relative_to((root / "raw" / spec.name).resolve())
    if not raw_path.is_file() or sha256_path(raw_path) != status.get("raw_sha256"):
        raise ValueError("failed raw response checksum verification failed")
    return (
        gzip.decompress(raw_path.read_bytes()),
        str(status.get("fetched_at_utc") or status["checked_at_utc"]),
        dict(status.get("response_headers", {})),
    )


def _download_large_trader_ranges(
    root: Path,
    session: requests.Session,
    limiter: SharedRateLimiter,
    next_sessions: dict[date, date],
    sessions: list[date],
    start: date,
    end: date,
    attempts: int,
    progress: dict[str, object],
    *,
    retry_unverified_hours: float = 24.0,
    reparse_failed_only: bool = False,
) -> list[dict[str, object]]:
    """Whole-market, resumable history; never multiply HTTP calls by product.

    Verified observations are reused individually, so the open quarter grows
    by new sessions rather than downloading its old rows every day.  Unknown
    early history and transport/coverage errors remain explicit, with a bounded
    retry delay, and cannot prevent later quarters or the other asset class.
    """
    summaries: list[dict[str, object]] = []
    requested_start = start
    start = max(start, LARGE_TRADER_HISTORY_START)
    expected = sorted(day for day in sessions if start <= day <= end)
    if not expected and end >= start:
        raise ValueError("no receipt-verified sessions for large-trader range history")
    for spec in LARGE_TRADER_RANGE_SPECS:
        observed: set[date] = set()
        for path in sorted((root / "receipts" / spec.name).glob("*/*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            valid = _valid_receipt(root, spec.name, str(payload.get("request_key", "")))
            if valid is None:
                raise RuntimeError(f"invalid range receipt blocks reuse: {path}")
            observed.update(date.fromisoformat(day) for day in valid["coverage"]["observed_dates"])
        # Negative/failed responses are not data receipts.  Their own raw
        # evidence and retry clock survive restart without certifying coverage.
        deferred: set[date] = set()
        replay_tasks: list[tuple[date, date, dict[str, object]]] = []
        replay_dates: set[date] = set()
        now = datetime.now(timezone.utc)
        for path in sorted((root / "range_status" / spec.name).glob("*/*.json")):
            status = json.loads(path.read_text(encoding="utf-8"))
            requested = status.get("request_payload", {})
            checked_start = date.fromisoformat(str(requested["queryStartDate"]).replace("/", "-"))
            checked_end = date.fromisoformat(str(requested["queryEndDate"]).replace("/", "-"))
            if checked_end < start or checked_start > end:
                continue
            if status.get("status") == "verified_calendar_coverage":
                continue
            if _failed_range_is_reparseable(status) and (
                reparse_failed_only or int(status.get("parser_version", 0)) < LARGE_TRADER_PARSER_VERSION
            ):
                payload = status["request_payload"]
                replay_start = date.fromisoformat(str(payload["queryStartDate"]).replace("/", "-"))
                replay_end = date.fromisoformat(str(payload["queryEndDate"]).replace("/", "-"))
                if start <= replay_start <= replay_end <= end:
                    replay_tasks.append((replay_start, replay_end, status))
                    replay_dates.update(day for day in expected if replay_start <= day <= replay_end)
                    continue
            checked = datetime.fromisoformat(status["checked_at_utc"])
            if (now - checked).total_seconds() >= retry_unverified_hours * 3600:
                continue
            raw = status.get("raw_path")
            if raw and (not (root / raw).is_file() or sha256_path(root / raw) != status["raw_sha256"]):
                continue
            deferred.update(date.fromisoformat(day) for day in status.get("missing_session_dates", []))
        tasks: list[tuple[date, date, dict[str, object] | None]] = list(replay_tasks)
        # Split at covered/deferred sessions as well as quarter boundaries.
        # This avoids re-fetching already acquired rows around a sparse gap.
        for quarter_start, quarter_end in (() if reparse_failed_only else _quarter_ranges(start, end)):
            pending: list[date] = []
            for day in (day for day in expected if quarter_start <= day <= quarter_end):
                if day in observed or day in deferred or day in replay_dates:
                    if pending:
                        tasks.append((pending[0], pending[-1], None))
                        pending = []
                else:
                    pending.append(day)
            if pending:
                tasks.append((pending[0], pending[-1], None))
        errors: list[dict[str, object]] = []
        for index, (range_start, range_end, replay) in enumerate(tasks, start=1):
            key = f"{range_start.isoformat()}_{range_end.isoformat()}"
            request_payload = {
                "queryStartDate": range_start.strftime("%Y/%m/%d"),
                "queryEndDate": range_end.strftime("%Y/%m/%d"),
            }
            range_expected = {day for day in expected if range_start <= day <= range_end}
            status: dict[str, object] = {
                "dataset": spec.name, "request_key": key, "source_url": spec.url,
                "request_payload": request_payload,
                "checked_at_utc": _utc_now(), "status": "failed",
                "parser_version": LARGE_TRADER_PARSER_VERSION,
                "response_origin": "verified_failed_raw_reparse" if replay else "http",
                "completion_claim": "retained_response_not_complete_history",
                "missing_session_dates": sorted(day.isoformat() for day in range_expected),
            }
            content: bytes | None = None
            failure_stage = "read_failed_raw" if replay else "http"
            try:
                if replay:
                    status["recovery"] = {
                        "previous_error": replay.get("error"),
                        "previous_checked_at_utc": replay["checked_at_utc"],
                        "previous_raw_sha256": replay["raw_sha256"],
                        "original_fetch_time_evidence": (
                            "recorded_fetch_time" if replay.get("fetched_at_utc") else "legacy_request_start_proxy"
                        ),
                    }
                    status.update(raw_path=replay["raw_path"], raw_sha256=replay["raw_sha256"])
                    content, fetched_at, headers = _read_failed_range_response(root, spec, replay, request_payload)
                else:
                    content, fetched_at, headers = _post(
                        session, limiter, spec.url, request_payload, attempts=attempts
                    )
                status.update(fetched_at_utc=fetched_at, response_headers=headers)
                failure_stage = "parse"
                frame = _parse_large_trader_csv(spec, content, range_start, range_end, next_sessions)
                failure_stage = "persist"
                source_rows = len(frame)
                # Other successful repairs may have covered part of an older
                # failed request.  Preserve its raw response but never create
                # overlapping normalized observation dates during replay.
                frame = frame.loc[~frame["date"].dt.date.isin(observed)].copy()
                acquired = set(frame["date"].dt.date)
                missing = range_expected - acquired - observed
                quality_counts = {str(key): int(value) for key, value in frame["row_quality"].value_counts().items()}
                coverage = {
                    "status": "partial_session_coverage" if missing else "verified_calendar_coverage",
                    "observed_dates": sorted(day.isoformat() for day in acquired),
                    "missing_session_dates": sorted(day.isoformat() for day in missing),
                    "expected_session_count": len(range_expected),
                    "observed_session_count": len(acquired),
                    "scope": "all_products_returned_by_official_range_csv",
                    "product_universe_completeness": "not_independently_verified",
                    "position_unit": "source_reported_contracts_or_contract_equivalents",
                    "trader_categories": {"0": "all_top_traders", "1": "specific_institutional_subset"},
                    "expiry_buckets": {"666666": "all_weekly_expiries", "999999": "all_expiries"},
                    "parser_version": LARGE_TRADER_PARSER_VERSION,
                    "availability_alignment_version": LARGE_TRADER_ALIGNMENT_VERSION,
                    "first_publication_date": LARGE_TRADER_FIRST_PUBLICATION.isoformat(),
                    "first_publication_timing": "post_close_inferred_from_regular_publication_rule",
                    "source_history_evidence_urls": list(LARGE_TRADER_HISTORY_SOURCES),
                    "row_quality_counts": quality_counts,
                    "unknown_trader_categories": sorted(frame.loc[
                        frame["row_quality"].eq("unknown_trader_category"), "trader_category"
                    ].unique().tolist()),
                    "source_rows": source_rows,
                    "excluded_already_observed_rows": source_rows - len(frame),
                }
                if replay:
                    coverage["recovery"] = status["recovery"]
                receipt = (_persist_response(
                    root, dataset=spec.name, key=key, url=spec.url,
                    request_payload=request_payload, content=content, frame=frame,
                    fetched_at=fetched_at, headers=headers, coverage=coverage,
                ) if not frame.empty else None)
                observed.update(acquired)
                status.update(
                    status=coverage["status"], missing_session_dates=coverage["missing_session_dates"],
                    rows=len(frame), row_quality_counts=quality_counts,
                )
                if receipt is not None:
                    status.update(raw_path=receipt["raw_path"], raw_sha256=receipt["raw_sha256"],
                                  receipt_path=_relative(_receipt_path(root, spec.name, key), root))
                elif replay:
                    status.update(raw_path=replay["raw_path"], raw_sha256=replay["raw_sha256"])
            except Exception as exc:
                status.update(
                    status="no_history_not_verified" if isinstance(exc, UnverifiedHistoryResponse) else "failed",
                    error_type=type(exc).__name__, error=str(exc), failure_stage=failure_stage,
                )
                if content is not None:
                    digest = _sha256_bytes(content)
                    raw_path = root / "raw" / spec.name / key[:4] / f"{key}_{digest[:16]}.raw.gz"
                    _atomic_write_bytes(raw_path, gzip.compress(content, compresslevel=9, mtime=0))
                    status.update(raw_path=_relative(raw_path, root), raw_sha256=sha256_path(raw_path))
            _write_json(_range_status_path(root, spec.name, key), status)
            if status["status"] != "verified_calendar_coverage":
                errors.append(status)
            progress.update(
                phase=spec.name, current=key, completed=index, total=len(tasks),
                current_range_status=status["status"], updated_at_utc=_utc_now(),
            )
            _write_json(root / "progress.json", progress)
            print(f"[large-trader-range] {index}/{len(tasks)} {spec.name} {key} {status['status']}", flush=True)
        missing = set(expected) - observed
        base = _merge_dataset(root, spec.name, next_sessions) if observed else {
            "dataset": spec.name, "rows": 0, "requests": 0,
            "first_date": None, "last_date": None, "availability_pending_rows": 0,
        }
        base.update(
            status="partial" if missing else "complete",
            completion_claim="observed_session_coverage_not_all_product_or_full_pit_proof",
            expected_session_count=len(expected),
            observed_session_count=len(set(expected) & observed),
            missing_session_count=len(missing),
            missing_session_dates=sorted(day.isoformat() for day in missing),
            deferred_session_count=len(missing & deferred),
            coverage_status="unverified_gaps" if missing else "verified_calendar_coverage",
            new_requests=sum(replay is None for _, _, replay in tasks),
            offline_reparse_requests=len(replay_tasks), new_problem_ranges=len(errors),
            history_boundary="official_2004_07_01_history_first_published_2005_01_03",
            requested_start_date=requested_start.isoformat(),
            effective_source_start_date=start.isoformat(),
            source_history_start_date=LARGE_TRADER_HISTORY_START.isoformat(),
            source_history_evidence_urls=list(LARGE_TRADER_HISTORY_SOURCES),
            first_publication_date=LARGE_TRADER_FIRST_PUBLICATION.isoformat(),
            availability_alignment_version=LARGE_TRADER_ALIGNMENT_VERSION,
            source_not_supported=(
                {"start_date": requested_start.isoformat(),
                 "end_date": min(end, LARGE_TRADER_HISTORY_START - timedelta(days=1)).isoformat(),
                 "reason": "before_official_history_start",
                 "verified_session_count": sum(requested_start <= day <= end and day < LARGE_TRADER_HISTORY_START
                                               for day in sessions)}
                if requested_start < LARGE_TRADER_HISTORY_START else None
            ),
            request_window="at_most_three_calendar_months_all_products",
            product_universe_completeness="not_independently_verified",
        )
        summaries.append(base)
    return summaries


def _availability_summary(frame: pd.DataFrame) -> dict[str, object]:
    """Report alignment separately from successful source acquisition."""
    pending = int(frame["available_date"].isna().sum())
    return {
        "completion_claim": "source_acquisition_not_full_pit_readiness",
        "availability_state": (
            "waiting_verified_next_session" if pending else "aligned"
        ),
        "availability_pending_rows": pending,
        "availability_aligned_rows": len(frame) - pending,
    }


def _merge_dataset(
    root: Path,
    dataset: str,
    next_sessions: dict[date, date] | None = None,
) -> dict[str, object]:
    if dataset in {spec.name for spec in LARGE_TRADER_RANGE_SPECS}:
        return _merge_range_dataset(root, dataset, next_sessions or {})
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
    # Reuse the immutable request receipts and source shards.  Only the merged
    # calendar projection evolves when a newly verified next session arrives.
    # Previously aligned dates remain unchanged, including for a narrower
    # subsequent CLI request; the calendar is not a source revision.
    pending = frame["available_date"].isna()
    if next_sessions is not None and pending.any():
        frame["available_date"] = pd.to_datetime(frame["available_date"])
        frame.loc[pending, "available_date"] = pd.to_datetime(
            [next_sessions.get(item.date()) for item in pd.to_datetime(frame.loc[pending, "date"])]
        ).to_numpy()
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
        **_availability_summary(frame),
        "requests": len(shard_paths),
        "rows": len(frame),
        "first_date": dates.min().date().isoformat(),
        "last_date": dates.max().date().isoformat(),
        "output_path": _relative(target, root),
        "output_bytes": target.stat().st_size,
        "output_sha256": sha256_path(target),
    }


def _merge_range_dataset(
    root: Path, dataset: str, next_sessions: dict[date, date]
) -> dict[str, object]:
    """Merge large all-product histories with bounded Arrow row-group memory.

    Parsers reject duplicate factual keys inside a response; official absence
    placeholders retain their distinct source-line identity.  The scheduler
    requests only previously unobserved dates; verify that proof again here,
    instead of globally sorting/deduplicating millions of rows in pandas.
    Physical order is receipt order, not a consumer's time-series sort order.
    """
    receipts: list[dict[str, object]] = []
    seen: set[date] = set()
    for path in sorted((root / "receipts" / dataset).glob("*/*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        valid = _valid_receipt(root, dataset, str(payload.get("request_key", "")))
        if valid is None:
            raise RuntimeError(f"invalid range receipt blocks merge: {path}")
        days = {date.fromisoformat(day) for day in valid["coverage"]["observed_dates"]}
        if days & seen:
            raise RuntimeError(f"overlapping observed dates block streaming merge: {path}")
        seen.update(days)
        receipts.append(valid)
    if not receipts:
        raise RuntimeError(f"no verified shards found for {dataset}")
    target = root / "normalized" / f"{dataset}.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_path(target)
    writer: pq.ParquetWriter | None = None
    rows = pending = publication_floor_rows = source_rule_corrected_rows = 0
    quality_counts: dict[str, int] = {}
    schema: pa.Schema | None = None
    try:
        for receipt in receipts:
            shard_days: set[date] = set()
            shard_rows = 0
            source = pq.ParquetFile(root / str(receipt["normalized_path"]))
            for batch in source.iter_batches(batch_size=65536):
                table = pa.Table.from_batches([batch]).replace_schema_metadata(None)
                # Additive parser quality fields do not invalidate old verified
                # shards.  Their old parser admitted only known categories and
                # unique factual keys; original row numbers were not retained.
                quality_fields = (
                    ("source_row_number", pa.int64()), ("row_quality", pa.large_string()),
                )
                extra_columns = {}
                for name, field_type in quality_fields:
                    if name in table.column_names:
                        extra_columns[name] = pc.cast(table[name], field_type)
                    elif name == "row_quality":
                        extra_columns[name] = pa.array(["observed"] * len(table), type=field_type)
                    else:
                        extra_columns[name] = pa.nulls(len(table), type=field_type)
                table = table.select([name for name in table.column_names if name not in extra_columns])
                for name, _ in quality_fields:
                    table = table.append_column(name, extra_columns[name])
                for item in pc.value_counts(table["row_quality"]).to_pylist():
                    quality_counts[item["values"]] = quality_counts.get(item["values"], 0) + item["counts"]
                unique_dates = pc.unique(table["date"])
                shard_days.update(value.date() for value in unique_dates.to_pylist())
                availability_index = table.schema.get_field_index("available_date")
                availability_type = table.schema.field(availability_index).type
                mapped = pa.array(
                    [next_sessions.get(value.date()) for value in unique_dates.to_pylist()],
                    type=pa.date32(),
                ).cast(availability_type)
                aligned = pc.coalesce(
                    table["available_date"],
                    pc.take(mapped, pc.index_in(table["date"], value_set=unique_dates)),
                )
                before_floor = aligned
                prelaunch = pc.less(
                    table["date"], pa.scalar(LARGE_TRADER_FIRST_PUBLICATION, pa.date32()).cast(table["date"].type)
                )
                floor_day = next_sessions.get(LARGE_TRADER_FIRST_PUBLICATION)
                floor = pa.scalar(floor_day, pa.date32()).cast(availability_type)
                # The exchange explicitly backfilled 2004 data on 2005-01-03.
                # This source evidence disproves old prelaunch alignment.  It
                # is the sole exception to preserving already aligned dates,
                # scoped to these two tables; immutable raw/shards stay intact.
                if floor_day:
                    floored = pc.max_element_wise(aligned, floor)
                else:
                    # Without the launch day's next-session evidence, remove
                    # disproven early dates.  An already certified later date
                    # must not regress or be invented from a sparse old map.
                    later = pc.greater(
                        table["available_date"],
                        pa.scalar(LARGE_TRADER_FIRST_PUBLICATION, pa.date32()).cast(availability_type),
                    )
                    floored = pc.if_else(pc.fill_null(later, False), table["available_date"],
                                         pa.nulls(len(table), availability_type))
                aligned = pc.if_else(prelaunch, floored, aligned)
                unchanged = pc.or_(
                    pc.fill_null(pc.equal(before_floor, aligned), False),
                    pc.and_(pc.is_null(before_floor), pc.is_null(aligned)),
                )
                source_rule_corrected_rows += pc.sum(pc.cast(pc.invert(unchanged), pa.int64())).as_py() or 0
                publication_floor_rows += pc.sum(pc.cast(prelaunch, pa.int64())).as_py() or 0
                table = table.set_column(availability_index, "available_date", aligned)
                rule_index = table.schema.get_field_index("availability_rule")
                table = table.set_column(rule_index, "availability_rule", pc.if_else(
                    prelaunch,
                    pa.scalar(LARGE_TRADER_LAUNCH_AVAILABILITY_RULE, type=table["availability_rule"].type),
                    table["availability_rule"],
                ))
                if writer is None:
                    schema = table.schema
                    writer = pq.ParquetWriter(temporary, schema, compression="zstd")
                elif table.schema != schema:
                    raise RuntimeError(f"incompatible range shard schema: {receipt['normalized_path']}")
                writer.write_table(table, row_group_size=65536)
                rows += len(table)
                shard_rows += len(table)
                pending += aligned.null_count
            expected_days = {date.fromisoformat(day) for day in receipt["coverage"]["observed_dates"]}
            if shard_days != expected_days or shard_rows != int(receipt["rows"]):
                raise RuntimeError(f"range receipt disagrees with shard: {receipt['normalized_path']}")
        if writer is None:
            raise RuntimeError(f"verified range shards have no rows: {dataset}")
        writer.close()
        writer = None
        os.replace(temporary, target)
    finally:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
    return {
        "dataset": dataset, "status": "complete", "requests": len(receipts), "rows": rows,
        "first_date": min(seen).isoformat(), "last_date": max(seen).isoformat(),
        "completion_claim": "source_acquisition_not_full_pit_readiness",
        "availability_state": "waiting_verified_next_session" if pending else "aligned",
        "availability_pending_rows": pending, "availability_aligned_rows": rows - pending,
        "availability_alignment_version": LARGE_TRADER_ALIGNMENT_VERSION,
        "first_publication_date": LARGE_TRADER_FIRST_PUBLICATION.isoformat(),
        "first_publication_timing": "post_close_inferred_from_regular_publication_rule",
        "first_publication_floor_rows": publication_floor_rows,
        "source_rule_corrected_rows": source_rule_corrected_rows,
        "source_rule_evidence_urls": list(LARGE_TRADER_HISTORY_SOURCES),
        "merge_backend": "pyarrow_streaming_disjoint_observation_dates",
        "row_quality_counts": quality_counts,
        "data_quality_state": (
            "unmapped_source_categories" if quality_counts.get("unknown_trader_category", 0)
            else "source_missingness_preserved" if quality_counts.get("source_no_weekly_contract_placeholder", 0)
            else "observed"
        ),
        "physical_order": "receipt_shards_not_global_date_order",
        "output_path": _relative(target, root), "output_bytes": target.stat().st_size,
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
    parser.add_argument(
        "--large-trader-start", type=date.fromisoformat, default=LARGE_TRADER_HISTORY_START,
        help="Requested start; the official supported history begins 2004-07-01.",
    )
    parser.add_argument(
        "--retry-unverified-hours", type=float, default=24.0,
        help="Retry previously empty/failed range observations after this many hours (0 forces retry).",
    )
    parser.add_argument(
        "--reparse-failed-ranges-only", action="store_true",
        help="With --phase large-trader-range, recover parser-failed raw responses without any HTTP requests.",
    )
    parser.add_argument("--request-interval", type=float, default=1.0)
    parser.add_argument("--attempts", type=int, default=8)
    parser.add_argument(
        "--phase",
        choices=("all", "put-call", "positioning", "large-trader-range"),
        default="all",
    )
    args = parser.parse_args()
    if args.request_interval < 0.1:
        parser.error("--request-interval must be at least 0.1 seconds")
    if args.attempts < 1:
        parser.error("--attempts must be positive")
    if args.retry_unverified_hours < 0:
        parser.error("--retry-unverified-hours must be nonnegative")
    if args.reparse_failed_ranges_only and args.phase != "large-trader-range":
        parser.error("--reparse-failed-ranges-only requires --phase large-trader-range")

    root = Path(args.output_dir).expanduser().resolve()
    session_path = Path(args.session_parquet).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    # The requested download horizon is not the calendar-evidence horizon.
    # A historical-only repair can reuse a later already-verified session to
    # align its final report without issuing an out-of-range HTTP request.
    calendar_sessions, calendar_sha256 = _load_sessions(session_path, date.max)
    sessions = [item for item in calendar_sessions if item <= args.end_date]
    if not sessions:
        parser.error("no receipt-verified TAIFEX sessions through the requested end")
    effective_end = sessions[-1]
    positioning_start = args.positioning_start or (
        _subtract_years(args.end_date, 3) + timedelta(days=1)
    )
    positioning_sessions = [
        item for item in sessions if positioning_start <= item <= effective_end
    ]
    # Missing next-session evidence blocks causal use, not source acquisition.
    next_sessions = _next_session_map(calendar_sessions)
    if args.put_call_start > effective_end and args.phase in {"all", "put-call"}:
        parser.error("--put-call-start is after the latest verified session")
    if args.large_trader_start > effective_end and args.phase in {"all", "large-trader-range"}:
        parser.error("--large-trader-start is after the latest verified session")
    if not positioning_sessions and args.phase in {"all", "positioning"}:
        parser.error("no receipt-verified positioning sessions in the requested range")

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
    range_summaries: list[dict[str, object]] = []
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
        if args.phase in {"all", "large-trader-range"}:
            range_summaries = _download_large_trader_ranges(
                root, http, limiter, next_sessions, sessions, args.large_trader_start,
                effective_end, args.attempts, progress,
                retry_unverified_hours=args.retry_unverified_hours,
                reparse_failed_only=args.reparse_failed_ranges_only,
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
        else []
        if args.phase == "large-trader-range"
        else ["put_call_ratio", *(spec.name for spec in POSITIONING_SPECS)]
    )
    summaries = [_merge_dataset(root, dataset, next_sessions) for dataset in selected] + range_summaries
    refreshed_state = "partial" if any(item.get("status") != "complete" for item in summaries) else "complete"
    # A bounded repair must not make other already acquired datasets vanish
    # from the provider manifest.  Keep their original evidence timestamps.
    manifest_path = root / "manifest.json"
    if manifest_path.is_file():
        previous = json.loads(manifest_path.read_text(encoding="utf-8"))
        updated_names = {item["dataset"] for item in summaries}
        summaries.extend(
            item for item in previous.get("datasets", []) if item["dataset"] not in updated_names
        )
    pending_rows = sum(int(item.get("availability_pending_rows", 0)) for item in summaries)
    acquisition_state = "partial" if any(item.get("status") != "complete" for item in summaries) else "complete"
    # A successfully completed phase must not inherit another phase's older
    # incomplete status into the runner's failure latch.  The provider-wide
    # manifest stays partial, so publication remains fail-closed independently.
    phase_execution_state = acquisition_state if args.phase == "all" else refreshed_state
    manifest = {
        "contract_version": CONTRACT_VERSION,
        "dataset": "taifex_public_history",
        "status": acquisition_state,
        "phase_execution_status": phase_execution_state,
        "phase_execution_scope": args.phase,
        "completion_claim": "source_acquisition_not_full_pit_readiness",
        "availability_alignment_version": LARGE_TRADER_ALIGNMENT_VERSION,
        "availability_state": (
            "waiting_verified_next_session" if pending_rows else "aligned"
        ),
        "availability_pending_rows": pending_rows,
        "source_authority": "Taiwan Futures Exchange",
        "source_pages": [
            "https://www.taifex.com.tw/cht/3/pcRatio",
            "https://www.taifex.com.tw/cht/3/futContractsDate",
            "https://www.taifex.com.tw/cht/3/optContractsDate",
            "https://www.taifex.com.tw/cht/3/largeTraderFutQry",
            "https://www.taifex.com.tw/cht/3/largeTraderFutView",
            "https://www.taifex.com.tw/cht/3/largeTraderOptView",
        ],
        "requested_end_date": args.end_date.isoformat(),
        "effective_end_date": effective_end.isoformat(),
        "positioning_start_date": positioning_start.isoformat(),
        "positioning_free_history_boundary": "institutional_web_queries_have_rolling_three_year_window",
        "older_positioning_history": "institutional_requires_application_large_trader_csv_is_independently_probed",
        "large_trader_requested_start_date": args.large_trader_start.isoformat(),
        "large_trader_effective_source_start_date": max(args.large_trader_start, LARGE_TRADER_HISTORY_START).isoformat(),
        "large_trader_source_history_evidence_urls": list(LARGE_TRADER_HISTORY_SOURCES),
        "refreshed_datasets": [*selected, *(item["dataset"] for item in range_summaries)],
        "published_after_close": True,
        "availability_rule": "next_receipt_verified_taifex_session",
        "session_calendar_path": str(session_path),
        "session_calendar_sha256": calendar_sha256,
        "datasets": summaries,
        "completed_at_utc": _utc_now(),
    }
    _write_json(root / "manifest.json", manifest)
    progress.update(
        state=acquisition_state,
        phase_execution_status=phase_execution_state,
        phase_execution_scope=args.phase,
        phase="complete",
        updated_at_utc=_utc_now(),
        completed_at_utc=_utc_now(),
        datasets=summaries,
        availability_state=manifest["availability_state"],
        availability_pending_rows=pending_rows,
        completion_claim=manifest["completion_claim"],
    )
    _write_json(root / "progress.json", progress)
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
    return 0 if phase_execution_state == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
