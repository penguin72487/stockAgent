from __future__ import annotations

import argparse
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import html
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, Callable

import polars as pl
import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.common import describe_rate_limit, resolve_end_date
from downloader.download_tw_public_data import (
    _configure_tw_public_rate_limiter,
    _http_get,
)
from downloader.tw_public_contract import DAILY_CLOSE_OPTIONAL_DATASETS


DATASET_NAME = "tw_transfer_adjustment_reference"
SCHEMA_VERSION = 1
RULE_URL = (
    "https://twse-regulation.twse.com.tw/TW/law/DAT06.aspx"
    "?FLCODE=FL007304&FLDATE=20020730&LSER=001"
)
FMSRFK_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/FMSRFK"
YAHOO_METADATA_KEYS = {
    "source": b"stockagent.source",
    "asset_class": b"stockagent.asset_class",
    "requested_start": b"stockagent.yahoo_requested_start",
    "checked_through": b"stockagent.yahoo_checked_through",
    "first_date": b"stockagent.first_date",
}
REQUIRED_RULE_PHRASES = (
    "第59條",
    "初次上市之有價證券已於櫃檯買賣",
    "終止櫃檯買賣之最近營業日最後一筆成交價格",
    "最近營業日之參考價格",
)


class TransferAdjustmentError(RuntimeError):
    """An official/Yahoo bridge invariant could not be verified."""


@dataclass(slots=True)
class Candidate:
    symbol: str
    company: str
    transfer_date: date
    previous_session_date: date
    official_reference_price: float
    yahoo_path: Path
    yahoo_frame: pl.DataFrame
    yahoo_receipt: dict[str, Any]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _file_receipt(path: Path, *, role: str | None = None) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    receipt: dict[str, Any] = {
        "path": str(path),
        "size": int(path.stat().st_size),
        "sha256": digest.hexdigest(),
    }
    if role is not None:
        receipt["role"] = role
    return receipt


def _receipt_matches(path: Path, receipt: Any) -> bool:
    if not path.is_file() or not isinstance(receipt, dict):
        return False
    try:
        actual = _file_receipt(path)
        return (
            actual["size"] == int(receipt["size"])
            and actual["sha256"] == str(receipt["sha256"])
        )
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _write_parquet_atomic(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata = dict(frame.to_arrow().schema.metadata or {})
    metadata.update(
        {
            b"stockagent.dataset": DATASET_NAME.encode("utf-8"),
            b"stockagent.schema_version": str(SCHEMA_VERSION).encode("ascii"),
            b"stockagent.source": b"TWSE_TPEx_official_verified_Yahoo_bridge",
        }
    )
    table = frame.to_arrow().replace_schema_metadata(metadata)
    with tempfile.NamedTemporaryFile(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        pq.write_table(table, temporary, compression="zstd")
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _safe_stem(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "receipt"


def _write_immutable_raw(
    raw_dir: Path,
    *,
    logical_key: str,
    suffix: str,
    content: bytes,
    source_url: str,
    role: str,
    reused: bool = False,
) -> dict[str, Any]:
    digest = _sha256_bytes(content)
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{_safe_stem(logical_key)}-{digest}{suffix}"
    if path.exists():
        if path.read_bytes() != content:
            raise TransferAdjustmentError(f"immutable raw receipt collision: {path}")
        reused = True
    else:
        with tempfile.NamedTemporaryFile(
            prefix=f".{path.name}.", suffix=".tmp", dir=raw_dir, delete=False
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        try:
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() != content:
                    raise TransferAdjustmentError(
                        f"immutable raw receipt collision: {path}"
                    )
        finally:
            temporary.unlink(missing_ok=True)
    return {
        **_file_receipt(path, role=role),
        "logical_key": logical_key,
        "source_url": source_url,
        "reused": bool(reused),
    }


def _cached_valid_raw(
    raw_dir: Path,
    *,
    logical_key: str,
    suffix: str,
    source_url: str,
    role: str,
    validator: Callable[[bytes], Any],
) -> tuple[bytes, Any, dict[str, Any]] | None:
    prefix = f"{_safe_stem(logical_key)}-"
    for path in sorted(raw_dir.glob(f"{prefix}*{suffix}")):
        try:
            content = path.read_bytes()
            digest = _sha256_bytes(content)
            if path.name != f"{prefix}{digest}{suffix}":
                continue
            parsed = validator(content)
            receipt = _write_immutable_raw(
                raw_dir,
                logical_key=logical_key,
                suffix=suffix,
                content=content,
                source_url=source_url,
                role=role,
                reused=True,
            )
            return content, parsed, receipt
        except Exception:
            continue
    return None


def _fetch_validated_raw(
    *,
    raw_dir: Path,
    logical_key: str,
    suffix: str,
    source_url: str,
    role: str,
    validator: Callable[[bytes], Any],
    params: dict[str, str] | None,
    timeout: int,
    retries: int,
    verify_ssl: bool,
    resume: bool,
) -> tuple[Any, dict[str, Any]]:
    if resume:
        cached = _cached_valid_raw(
            raw_dir,
            logical_key=logical_key,
            suffix=suffix,
            source_url=source_url,
            role=role,
            validator=validator,
        )
        if cached is not None:
            _, parsed, receipt = cached
            return parsed, receipt
    response = _http_get(
        source_url,
        timeout=timeout,
        verify_ssl=verify_ssl,
        params=params,
        retries=retries,
    )
    content = bytes(response.content)
    receipt = _write_immutable_raw(
        raw_dir,
        logical_key=logical_key,
        suffix=suffix,
        content=content,
        source_url=str(getattr(response, "url", source_url)),
        role=role,
    )
    parsed = validator(content)
    return parsed, receipt


def _decode_text(content: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "big5", "cp950"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise TransferAdjustmentError("official response has an unknown text encoding")


def _parse_historical_rule(content: bytes) -> dict[str, Any]:
    text = _decode_text(content)
    visible = html.unescape(re.sub(r"<[^>]+>", "", text))
    compact = re.sub(r"\s+", "", visible)
    missing = [phrase for phrase in REQUIRED_RULE_PHRASES if phrase not in compact]
    if missing:
        raise TransferAdjustmentError(
            f"historical Rule 59 receipt is missing required phrases: {missing}"
        )
    if "20020730" not in re.sub(r"[^0-9]", "", text):
        raise TransferAdjustmentError("historical Rule 59 receipt lacks 2002-07-30 identity")
    return {
        "effective_version": "2002-07-30",
        "article": 59,
        "required_phrase_count": len(REQUIRED_RULE_PHRASES),
    }


def _number(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "--", "---", "N/A", "nan", "null", "None"}:
        return None
    try:
        result = float(text)
    except ValueError:
        return None
    return result if math.isfinite(result) else None


def _parse_fmsrfk_payload(content: bytes, *, symbol: str, year: int) -> dict[int, dict[str, float]]:
    try:
        payload = json.loads(content.decode("utf-8-sig"))
    except Exception as exc:
        raise TransferAdjustmentError(
            f"FMSRFK {symbol} {year} is not valid JSON: {exc}"
        ) from exc
    if not isinstance(payload, dict) or str(payload.get("stat", "")).upper() != "OK":
        raise TransferAdjustmentError(
            f"FMSRFK {symbol} {year} status is not OK: {payload!r}"
        )
    declared_date = re.sub(r"[^0-9]", "", str(payload.get("date", "")))
    if declared_date and declared_date != f"{year}0101":
        raise TransferAdjustmentError(
            f"FMSRFK response date mismatch: requested={year}0101 declared={declared_date}"
        )
    title = re.sub(r"\s+", "", str(payload.get("title", "")))
    if symbol not in title or str(year - 1911) not in title:
        raise TransferAdjustmentError(
            f"FMSRFK response identity mismatch for {symbol} {year}: {title!r}"
        )
    fields = payload.get("fields")
    rows = payload.get("data")
    if not isinstance(fields, list) or not isinstance(rows, list):
        raise TransferAdjustmentError(f"FMSRFK {symbol} {year} lacks fields/data")

    def field_index(*names: str) -> int:
        normalized = [re.sub(r"\s+", "", str(value)) for value in fields]
        for name in names:
            if name in normalized:
                return normalized.index(name)
        raise TransferAdjustmentError(
            f"FMSRFK {symbol} {year} lacks one of fields {names}: {fields}"
        )

    month_index = field_index("月份", "月")
    high_index = field_index("最高價")
    low_index = field_index("最低價")
    result: dict[int, dict[str, float]] = {}
    for row in rows:
        if not isinstance(row, list) or max(month_index, high_index, low_index) >= len(row):
            raise TransferAdjustmentError(f"FMSRFK {symbol} {year} has a malformed row")
        month_value = re.sub(r"[^0-9]", "", str(row[month_index]))
        high = _number(row[high_index])
        low = _number(row[low_index])
        if not month_value or high is None or low is None:
            continue
        month = int(month_value)
        if not 1 <= month <= 12 or month in result or high < low or low <= 0.0:
            raise TransferAdjustmentError(
                f"FMSRFK {symbol} {year} has invalid month/high/low: {row}"
            )
        result[month] = {"high": high, "low": low}
    if not result:
        raise TransferAdjustmentError(f"FMSRFK {symbol} {year} contains no usable rows")
    return result


def _date_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String, strict=False)
        .str.slice(0, 10)
        .str.to_date(strict=False)
    )


def _number_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String, strict=False)
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .fill_nan(None)
    )


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise TransferAdjustmentError(
            f"missing or invalid JSON receipt {path}: {type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise TransferAdjustmentError(f"JSON receipt is not an object: {path}")
    return payload


def _require_summary_output(
    summary_path: Path,
    output_path: Path,
    *,
    identity: Callable[[dict[str, Any]], bool],
) -> dict[str, Any]:
    summary = _read_json_object(summary_path)
    checks = {
        "identity": identity(summary),
        "coverage_complete": summary.get("coverage_complete") is True,
        "output_receipt": _receipt_matches(output_path, summary.get("output_receipt")),
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise TransferAdjustmentError(
            f"official input receipt failed checks {failed}: {summary_path}"
        )
    return summary


def _validate_official_inputs(
    official_dir: Path, *, start: date, end: date
) -> tuple[dict[str, Path], list[dict[str, Any]]]:
    paths = {
        "newlisting": official_dir / "twse_api_company_newlisting.parquet",
        "twse_ohlcv": official_dir / "twse_daily_ohlcv.parquet",
        "tpex_ohlcv": official_dir / "tpex_daily_ohlcv.parquet",
        "corporate_reference": official_dir / "tw_corporate_action_reference.parquet",
        "taiex_calendar": official_dir / "twse_taiex_ohlc.parquet",
        "public_summary": official_dir / "download_summary.json",
        "corporate_summary": official_dir / "tw_corporate_action_reference.summary.json",
        "taiex_summary": official_dir / "twse_taiex_ohlc.summary.json",
    }
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise TransferAdjustmentError(f"required official inputs are missing: {missing}")

    public_summary = _read_json_object(paths["public_summary"])
    try:
        public_start_text = str(public_summary["start_date"]).strip().lower()
        public_start = (
            date(2000, 1, 1)
            if public_start_text == "earliest"
            else date.fromisoformat(public_start_text[:10])
        )
        public_end = date.fromisoformat(str(public_summary["end_date"])[:10])
    except (KeyError, TypeError, ValueError) as exc:
        raise TransferAdjustmentError("official download_summary has invalid coverage dates") from exc
    certified_daily_close = (
        public_summary.get("mode") == "daily"
        and public_summary.get("daily_close_ready") is True
        and int(public_summary.get("blocking_failed_count", -1)) == 0
        and set(public_summary.get("publication_lag_datasets") or ())
        <= DAILY_CLOSE_OPTIONAL_DATASETS
    )
    if (
        (
            public_summary.get("coverage_complete") is not True
            or int(public_summary.get("failed_count", -1)) != 0
        )
        and not certified_daily_close
    ) or public_start > start or public_end < end:
        raise TransferAdjustmentError(
            "official download_summary is not coverage-complete for the requested range"
        )

    _require_summary_output(
        paths["corporate_summary"],
        paths["corporate_reference"],
        identity=lambda value: value.get("coverage_complete") is True,
    )
    _require_summary_output(
        paths["taiex_summary"],
        paths["taiex_calendar"],
        identity=lambda value: (
            value.get("dataset") == "twse_taiex_ohlc"
            and value.get("source") == "TWSE"
            and value.get("baseline_established") is True
            and value.get("replacement_promoted") is True
            and int(value.get("unresolved_month_count", -1)) == 0
        ),
    )
    receipts = [
        _file_receipt(path, role=role)
        for role, path in paths.items()
    ]
    return paths, receipts


def _decode_metadata(path: Path) -> dict[str, str]:
    raw = pq.read_metadata(path).metadata or {}
    return {
        name: (raw.get(key) or b"").decode("utf-8", errors="strict").strip()
        for name, key in YAHOO_METADATA_KEYS.items()
    }


def _validate_yahoo_source(
    path: Path,
    *,
    symbol: str,
    start: date,
    end: date,
    calendar_dates: set[date],
) -> tuple[pl.DataFrame, dict[str, Any], dict[str, str]]:
    if not path.is_file():
        raise TransferAdjustmentError(f"Yahoo source is missing for transfer {symbol}: {path}")
    schema = set(pq.read_schema(path).names)
    required = {
        "date",
        "open",
        "max",
        "min",
        "close",
        "Stock Splits",
    }
    if required - schema:
        raise TransferAdjustmentError(
            f"Yahoo source {path} lacks columns {sorted(required - schema)}"
        )
    metadata = _decode_metadata(path)
    try:
        requested_start = date.fromisoformat(metadata["requested_start"][:10])
        checked_through = date.fromisoformat(metadata["checked_through"][:10])
        metadata_first = date.fromisoformat(metadata["first_date"][:10])
    except ValueError as exc:
        raise TransferAdjustmentError(f"Yahoo source metadata is invalid: {path}") from exc
    if (
        metadata["source"] != "yahoo"
        or metadata["asset_class"] != "tw_stocks"
        or requested_start > start
        or checked_through < end
    ):
        raise TransferAdjustmentError(
            f"Yahoo source metadata is not coverage-eligible for {symbol}: {metadata}"
        )
    frame = (
        pl.read_parquet(path, columns=sorted(required))
        .select(
            _date_expr("date").alias("date"),
            *[
                pl.col(column).cast(pl.Float64, strict=False).fill_nan(None).alias(column)
                for column in ("open", "max", "min", "close", "Stock Splits")
            ],
        )
        .drop_nulls(["date", "open", "max", "min", "close"])
        .filter(
            pl.all_horizontal(
                *[
                    pl.col(column).is_finite() & (pl.col(column) > 0.0)
                    for column in ("open", "max", "min", "close")
                ]
            )
            & (pl.col("max") >= pl.max_horizontal("open", "close", "min"))
            & (pl.col("min") <= pl.min_horizontal("open", "close", "max"))
        )
        .sort("date")
    )
    if frame.is_empty() or frame["date"].n_unique() != frame.height:
        raise TransferAdjustmentError(f"Yahoo source has no unique valid rows: {path}")
    if frame[0, "date"] != metadata_first:
        raise TransferAdjustmentError(
            f"Yahoo source first-date metadata mismatch for {symbol}: "
            f"metadata={metadata_first} rows={frame[0, 'date']}"
        )
    if metadata_first not in calendar_dates:
        raise TransferAdjustmentError(
            f"Yahoo source first row is not an official TAIEX session for {symbol}"
        )
    return frame, _file_receipt(path, role=f"yahoo_source:{symbol}"), metadata


def _load_calendar(path: Path) -> tuple[pl.DataFrame, set[date]]:
    frame = (
        pl.read_parquet(
            path,
            columns=[
                "date",
                "opening_index",
                "highest_index",
                "lowest_index",
                "closing_index",
            ],
        )
        .select(
            _date_expr("date").alias("date"),
            *[
                pl.col(column).cast(pl.Float64, strict=False).fill_nan(None).alias(column)
                for column in (
                    "opening_index",
                    "highest_index",
                    "lowest_index",
                    "closing_index",
                )
            ],
        )
        .drop_nulls()
        .filter(
            pl.all_horizontal(
                *[
                    pl.col(column).is_finite() & (pl.col(column) > 0.0)
                    for column in (
                        "opening_index",
                        "highest_index",
                        "lowest_index",
                        "closing_index",
                    )
                ]
            )
        )
        .sort("date")
    )
    if frame.is_empty() or frame["date"].n_unique() != frame.height:
        raise TransferAdjustmentError("TAIEX calendar contains duplicate or no valid dates")
    return frame, set(frame.get_column("date").to_list())


def _load_manifest(path: Path) -> tuple[pl.DataFrame, dict[str, Any]]:
    if not path.is_file():
        raise TransferAdjustmentError(f"Yahoo symbols manifest is missing: {path}")
    frame = pl.read_csv(path, infer_schema_length=10000)
    required = {"code", "market", "yahoo_symbol"}
    if required - set(frame.columns):
        raise TransferAdjustmentError(
            f"Yahoo symbols manifest lacks columns {sorted(required - set(frame.columns))}"
        )
    return frame, _file_receipt(path, role="yahoo_symbol_manifest")


def _load_candidates(
    *,
    paths: dict[str, Path],
    yahoo_dir: Path,
    start: date,
    end: date,
    calendar: pl.DataFrame,
    calendar_dates: set[date],
) -> tuple[
    list[Candidate],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    date,
]:
    newlisting = pl.read_parquet(paths["newlisting"])
    if {"Code", "Company", "Note"} - set(newlisting.columns):
        raise TransferAdjustmentError("newlisting input lacks Code/Company/Note")
    transfer_rows = (
        newlisting.select(
            pl.col("Code").cast(pl.String).str.strip_chars().str.to_uppercase().alias("symbol"),
            pl.col("Company").cast(pl.String).fill_null("").str.strip_chars().alias("company"),
            pl.col("Note").cast(pl.String).fill_null("").alias("note"),
        )
        .filter(pl.col("note").str.contains("櫃轉市"))
        .unique("symbol", keep="first")
    )
    transfer_codes = set(transfer_rows.get_column("symbol").to_list())
    names = dict(
        transfer_rows.select("symbol", "company").iter_rows()
    )
    if not transfer_codes:
        raise TransferAdjustmentError("newlisting input contains no 櫃轉市 candidates")

    twse_dates = (
        pl.scan_parquet(paths["twse_ohlcv"])
        .select(_date_expr("date").alias("date"))
        .drop_nulls()
        .collect()
    )
    if twse_dates.is_empty():
        raise TransferAdjustmentError("TWSE OHLCV input contains no valid dates")
    official_twse_start = twse_dates["date"].min()
    if official_twse_start is None:
        raise TransferAdjustmentError("TWSE OHLCV start date is unresolved")

    tpex = (
        pl.scan_parquet(paths["tpex_ohlcv"])
        .select(
            _date_expr("date").alias("date"),
            pl.col("代號").cast(pl.String).str.strip_chars().str.to_uppercase().alias("symbol"),
            _number_expr("收盤").alias("close"),
        )
        .filter(pl.col("symbol").is_in(sorted(transfer_codes)))
        .drop_nulls(["date", "symbol", "close"])
        .filter(pl.col("close").is_finite() & (pl.col("close") > 0.0))
        .collect()
        .sort(["symbol", "date"])
    )
    if tpex.is_empty() or tpex.select("symbol", "date").n_unique() != tpex.height:
        raise TransferAdjustmentError("TPEx transfer history is empty or has duplicate keys")
    tpex_start = tpex["date"].min()
    last_tpex = (
        tpex.group_by("symbol")
        .agg(
            pl.col("date").last().alias("previous_session_date"),
            pl.col("close").last().alias("official_reference_price"),
        )
        .filter(pl.col("previous_session_date") < official_twse_start)
        .sort("symbol")
    )

    manifest, manifest_receipt = _load_manifest(yahoo_dir / "symbols.csv")
    manifest_pairs = {
        (str(row["code"]).strip().upper(), str(row["yahoo_symbol"]).strip().upper())
        for row in manifest.select("code", "yahoo_symbol").iter_rows(named=True)
    }
    sessions = calendar.get_column("date").to_list()
    inputs: list[dict[str, Any]] = [manifest_receipt]
    exclusions: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    candidates: list[Candidate] = []
    for row in last_tpex.iter_rows(named=True):
        symbol = str(row["symbol"])
        previous = row["previous_session_date"]
        if tpex_start is None or previous < tpex_start:
            continue
        next_sessions = [value for value in sessions if value > previous]
        if not next_sessions:
            unresolved.append({"symbol": symbol, "error": "no TAIEX session after last TPEx row"})
            continue
        transfer_date = next_sessions[0]
        if (symbol, f"{symbol}.TW") not in manifest_pairs:
            exclusions.append(
                {
                    "symbol": symbol,
                    "reason": "canonical listed Yahoo identity is absent from manifest",
                }
            )
            continue
        yahoo_path = yahoo_dir / f"{symbol}_features.parquet"
        if not yahoo_path.is_file():
            exclusions.append(
                {
                    "symbol": symbol,
                    "reason": "canonical Yahoo source file is absent",
                }
            )
            continue
        try:
            yahoo, yahoo_receipt, _ = _validate_yahoo_source(
                yahoo_path,
                symbol=symbol,
                start=start,
                end=end,
                calendar_dates=calendar_dates,
            )
            if yahoo[0, "date"] != transfer_date:
                exclusions.append(
                    {
                        "symbol": symbol,
                        "reason": "Yahoo history does not begin at the TPEx-to-TWSE boundary",
                        "expected_transfer_date": transfer_date.isoformat(),
                        "yahoo_first_date": yahoo[0, "date"].isoformat(),
                    }
                )
                continue
            candidates.append(
                Candidate(
                    symbol=symbol,
                    company=names.get(symbol, symbol),
                    transfer_date=transfer_date,
                    previous_session_date=previous,
                    official_reference_price=float(row["official_reference_price"]),
                    yahoo_path=yahoo_path,
                    yahoo_frame=yahoo,
                    yahoo_receipt=yahoo_receipt,
                )
            )
            inputs.append(yahoo_receipt)
        except Exception as exc:
            unresolved.append(
                {"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"}
            )
    if not candidates and not unresolved:
        raise TransferAdjustmentError(
            "no pre-TWSE-history 櫃轉市 candidates were discovered from official TPEx history"
        )
    return candidates, inputs, exclusions, unresolved, official_twse_start


def _load_twse_quotes(path: Path, symbols: list[str]) -> pl.DataFrame:
    frame = (
        pl.scan_parquet(path)
        .select(
            _date_expr("date").alias("date"),
            pl.col("證券代號").cast(pl.String).str.strip_chars().str.to_uppercase().alias("symbol"),
            _number_expr("開盤價").alias("official_open"),
            _number_expr("最高價").alias("official_high"),
            _number_expr("最低價").alias("official_low"),
            _number_expr("收盤價").alias("official_close"),
        )
        .filter(pl.col("symbol").is_in(symbols))
        .drop_nulls()
        .filter(
            pl.all_horizontal(
                *[
                    pl.col(column).is_finite() & (pl.col(column) > 0.0)
                    for column in (
                        "official_open",
                        "official_high",
                        "official_low",
                        "official_close",
                    )
                ]
            )
        )
        .collect()
        .sort(["symbol", "date"])
    )
    if frame.select("symbol", "date").n_unique() != frame.height:
        raise TransferAdjustmentError("TWSE overlap quotes contain duplicate date-symbol keys")
    return frame


def _load_corporate_references(path: Path, symbols: list[str]) -> pl.DataFrame:
    schema = set(pq.read_schema(path).names)
    required = {"date", "symbol", "market", "previous_close", "source_url"}
    if required - schema:
        raise TransferAdjustmentError(
            f"corporate reference lacks columns {sorted(required - schema)}"
        )
    frame = (
        pl.read_parquet(path, columns=sorted(required))
        .select(
            _date_expr("date").alias("date"),
            pl.col("symbol").cast(pl.String).str.strip_chars().str.to_uppercase().alias("symbol"),
            pl.col("market").cast(pl.String).str.to_lowercase().alias("market"),
            pl.col("previous_close").cast(pl.Float64, strict=False).fill_nan(None),
            pl.col("source_url").cast(pl.String).alias("source_url"),
        )
        .filter(pl.col("symbol").is_in(symbols))
        .sort(["symbol", "date"])
    )
    return frame


def _split_events(
    frame: pl.DataFrame, *, transfer: date, validation_end: date
) -> list[tuple[date, float]]:
    events: list[tuple[date, float]] = []
    for row in frame.filter(
        pl.col("date").is_between(transfer, validation_end)
        & pl.col("Stock Splits").is_not_null()
        & pl.col("Stock Splits").is_finite()
        & (pl.col("Stock Splits") > 0.0)
    ).select("date", "Stock Splits").iter_rows(named=True):
        ratio = float(row["Stock Splits"])
        if ratio <= 0.0 or not math.isfinite(ratio):
            raise TransferAdjustmentError(f"invalid Yahoo split ratio: {row}")
        if not math.isclose(ratio, 1.0, rel_tol=0.0, abs_tol=1e-12):
            events.append((row["date"], ratio))
    return sorted(events)


def _scale_for_date(
    value_date: date,
    overlap_anchor: date,
    overlap_scale: float,
    events: list[tuple[date, float]],
) -> float:
    if value_date < overlap_anchor:
        multiplier = math.prod(
            ratio * ratio
            for event_date, ratio in events
            if value_date < event_date <= overlap_anchor
        )
        return overlap_scale * multiplier
    divisor = math.prod(
        ratio * ratio
        for event_date, ratio in events
        if overlap_anchor < event_date <= value_date
    )
    return overlap_scale / divisor


def _verify_candidate(
    candidate: Candidate,
    *,
    official_quotes: pl.DataFrame,
    corporate: pl.DataFrame,
    fmsrfk: dict[int, dict[int, dict[str, float]]],
    calendar_dates: set[date],
    overlap_sessions: int,
    overlap_scale_relative_tolerance: float,
    price_tolerance: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    yahoo = candidate.yahoo_frame
    official = official_quotes.filter(pl.col("symbol") == candidate.symbol)
    overlap = (
        official.join(
            yahoo.select(
                "date",
                pl.col("open").alias("yahoo_open"),
                pl.col("max").alias("yahoo_high"),
                pl.col("min").alias("yahoo_low"),
                pl.col("close").alias("yahoo_close"),
            ),
            on="date",
            how="inner",
        )
        .filter(pl.col("date").is_in(calendar_dates))
        .sort("date")
        .head(overlap_sessions)
    )
    if overlap.height != overlap_sessions:
        raise TransferAdjustmentError(
            f"{candidate.symbol} has {overlap.height}/{overlap_sessions} official overlap sessions"
        )
    overlap_start = overlap[0, "date"]
    overlap_end = overlap[-1, "date"]
    close_scales = [
        float(row["official_close"]) / float(row["yahoo_close"])
        for row in overlap.iter_rows(named=True)
    ]
    overlap_scale = math.fsum(close_scales) / len(close_scales)
    scale_min = min(close_scales)
    scale_max = max(close_scales)
    scale_relative_spread = (scale_max - scale_min) / overlap_scale
    if scale_relative_spread > overlap_scale_relative_tolerance:
        raise TransferAdjustmentError(
            f"{candidate.symbol} overlap scale is not stable: relative_spread={scale_relative_spread}"
        )
    overlap_errors: list[float] = []
    for row in overlap.iter_rows(named=True):
        for official_column, yahoo_column in (
            ("official_open", "yahoo_open"),
            ("official_high", "yahoo_high"),
            ("official_low", "yahoo_low"),
            ("official_close", "yahoo_close"),
        ):
            overlap_errors.append(
                abs(float(row[yahoo_column]) * overlap_scale - float(row[official_column]))
            )
    overlap_max_error = max(overlap_errors, default=0.0)
    if overlap_max_error > price_tolerance:
        raise TransferAdjustmentError(
            f"{candidate.symbol} overlap OHLC max error {overlap_max_error} exceeds {price_tolerance}"
        )

    fms_validation_end = date(
        overlap_end.year,
        overlap_end.month,
        monthrange(overlap_end.year, overlap_end.month)[1],
    )
    events = _split_events(
        yahoo,
        transfer=candidate.transfer_date,
        validation_end=fms_validation_end,
    )
    event_errors: list[float] = []
    event_details: list[dict[str, Any]] = []
    for event_date, ratio in events:
        references = corporate.filter(
            (pl.col("symbol") == candidate.symbol)
            & (pl.col("date") == event_date)
            & (pl.col("market") == "twse")
            & pl.col("previous_close").is_not_null()
            & (pl.col("previous_close") > 0.0)
        )
        if references.height != 1:
            raise TransferAdjustmentError(
                f"{candidate.symbol} split {event_date} has {references.height} official TWT49U anchors"
            )
        previous_rows = yahoo.filter(
            (pl.col("date") < event_date) & pl.col("date").is_in(calendar_dates)
        ).sort("date")
        if previous_rows.is_empty():
            raise TransferAdjustmentError(
                f"{candidate.symbol} split {event_date} lacks a prior Yahoo session"
            )
        previous_row = previous_rows.row(-1, named=True)
        reconstructed_previous = float(previous_row["close"]) * _scale_for_date(
            previous_row["date"], overlap_start, overlap_scale, events
        )
        official_previous = float(references[0, "previous_close"])
        error = abs(reconstructed_previous - official_previous)
        if error > price_tolerance:
            raise TransferAdjustmentError(
                f"{candidate.symbol} split {event_date} TWT49U error {error} exceeds {price_tolerance}"
            )
        event_errors.append(error)
        event_details.append(
            {
                "date": event_date.isoformat(),
                "split_ratio": ratio,
                "squared_scale_multiplier": ratio * ratio,
                "previous_session_date": previous_row["date"].isoformat(),
                "official_previous_close": official_previous,
                "reconstructed_previous_close": reconstructed_previous,
                "absolute_error": error,
                "source_url": references[0, "source_url"],
            }
        )

    first = yahoo.filter(pl.col("date") == candidate.transfer_date)
    if first.height != 1:
        raise TransferAdjustmentError(
            f"{candidate.symbol} transfer date has {first.height} Yahoo source rows"
        )
    first_row = first.row(0, named=True)
    transfer_scale = _scale_for_date(
        candidate.transfer_date, overlap_start, overlap_scale, events
    )
    reconstructed = {
        "open": float(first_row["open"]) * transfer_scale,
        "high": float(first_row["max"]) * transfer_scale,
        "low": float(first_row["min"]) * transfer_scale,
        "close": float(first_row["close"]) * transfer_scale,
    }
    factor = reconstructed["close"] / candidate.official_reference_price
    if not math.isfinite(factor) or factor <= 0.0:
        raise TransferAdjustmentError(f"{candidate.symbol} derived an invalid adjustment factor")

    # FMSRFK is a full-calendar-month statistic.  The six overlap sessions
    # establish the scale, but February must be reconstructed through its
    # actual month end rather than truncated at the sixth overlap session.
    early_window = yahoo.filter(
        pl.col("date").is_between(candidate.transfer_date, fms_validation_end)
    )
    off_calendar_dates = sorted(
        set(early_window.get_column("date").to_list()) - calendar_dates
    )
    official_window = early_window.filter(pl.col("date").is_in(calendar_dates))
    expected_months = sorted(
        {(value.year, value.month) for value in official_window.get_column("date").to_list()}
    )
    fms_errors: list[float] = []
    fms_details: list[dict[str, Any]] = []
    for year, month in expected_months:
        monthly = official_window.filter(
            (pl.col("date").dt.year() == year) & (pl.col("date").dt.month() == month)
        )
        reconstructed_high = max(
            float(row["max"])
            * _scale_for_date(row["date"], overlap_start, overlap_scale, events)
            for row in monthly.iter_rows(named=True)
        )
        reconstructed_low = min(
            float(row["min"])
            * _scale_for_date(row["date"], overlap_start, overlap_scale, events)
            for row in monthly.iter_rows(named=True)
        )
        official_month = fmsrfk.get(year, {}).get(month)
        if official_month is None:
            raise TransferAdjustmentError(
                f"{candidate.symbol} FMSRFK lacks {year}-{month:02d}"
            )
        high_error = abs(reconstructed_high - official_month["high"])
        low_error = abs(reconstructed_low - official_month["low"])
        if max(high_error, low_error) > price_tolerance:
            raise TransferAdjustmentError(
                f"{candidate.symbol} FMSRFK {year}-{month:02d} max error "
                f"{max(high_error, low_error)} exceeds {price_tolerance}"
            )
        fms_errors.extend((high_error, low_error))
        fms_details.append(
            {
                "month": f"{year}-{month:02d}",
                "official_high": official_month["high"],
                "official_low": official_month["low"],
                "reconstructed_high": reconstructed_high,
                "reconstructed_low": reconstructed_low,
                "high_absolute_error": high_error,
                "low_absolute_error": low_error,
            }
        )

    artifact_row = {
        "date": candidate.transfer_date,
        "symbol": candidate.symbol,
        "company": candidate.company,
        "adjustment_factor": factor,
        "official_reference_price": candidate.official_reference_price,
        "previous_official_session": candidate.previous_session_date,
        "reconstructed_open": reconstructed["open"],
        "reconstructed_high": reconstructed["high"],
        "reconstructed_low": reconstructed["low"],
        "reconstructed_close": reconstructed["close"],
        "overlap_start": overlap_start,
        "overlap_end": overlap_end,
        "overlap_sessions": overlap.height,
        "overlap_scale": overlap_scale,
        "overlap_scale_relative_spread": scale_relative_spread,
        "overlap_max_absolute_error": overlap_max_error,
        "split_event_count": len(events),
        "split_squared_multiplier": math.prod(
            ratio * ratio
            for event_date, ratio in events
            if candidate.transfer_date < event_date <= overlap_start
        ),
        "post_overlap_split_event_count": sum(
            event_date > overlap_start for event_date, _ in events
        ),
        "twt49u_max_absolute_error": max(event_errors, default=0.0),
        "fmsrfk_month_count": len(expected_months),
        "fmsrfk_max_absolute_error": max(fms_errors, default=0.0),
        "off_calendar_row_count": len(off_calendar_dates),
        "adjustment_source": "official_transfer_reference+yahoo_split_r2_verified",
        "validation_status": "verified",
        "yahoo_source_sha256": candidate.yahoo_receipt["sha256"],
    }
    detail = {
        "symbol": candidate.symbol,
        "transfer_date": candidate.transfer_date.isoformat(),
        "previous_official_session": candidate.previous_session_date.isoformat(),
        "official_reference_price": candidate.official_reference_price,
        "adjustment_factor": factor,
        "reconstructed_ohlc": reconstructed,
        "overlap": {
            "start": overlap_start.isoformat(),
            "end": overlap_end.isoformat(),
            "sessions": overlap.height,
            "scale": overlap_scale,
            "scale_min": scale_min,
            "scale_max": scale_max,
            "relative_spread": scale_relative_spread,
            "max_absolute_ohlc_error": overlap_max_error,
        },
        "split_events": event_details,
        "twt49u_max_absolute_error": max(event_errors, default=0.0),
        "fmsrfk_months": fms_details,
        "fmsrfk_max_absolute_error": max(fms_errors, default=0.0),
        "fmsrfk_validation_end": fms_validation_end.isoformat(),
        "off_calendar_rows": [value.isoformat() for value in off_calendar_dates],
        "yahoo_source_receipt": candidate.yahoo_receipt,
    }
    return artifact_row, detail


def _summary_path(output_path: Path) -> Path:
    return output_path.with_suffix(".summary.json")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a receipt-bound, fail-closed first-row adjustment artifact for "
            "pre-2004 TPEx-to-TWSE transfers."
        )
    )
    parser.add_argument("--mode", choices=("rebuild", "repair", "daily"), default="repair")
    parser.add_argument("--official-input-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--yahoo-source-dir", type=Path, required=True)
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("data_tw_public/tw_transfer_adjustment_reference.parquet"),
    )
    parser.add_argument("--start-date", default="2000-01-01")
    parser.add_argument("--end-date", default="today")
    parser.add_argument("--overlap-sessions", type=int, default=6)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--request-interval", type=float, default=None)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--verify-ssl", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--overlap-scale-relative-tolerance", type=float, default=5e-4)
    parser.add_argument("--price-tolerance", type=float, default=0.011)
    return parser.parse_args(argv)


def _run(args: argparse.Namespace) -> int:
    output_path = Path(args.output_path)
    summary_path = _summary_path(output_path)
    failure_summary_path = output_path.with_suffix(".failed.json")
    raw_root = output_path.parent / "raw" / DATASET_NAME
    interval = _configure_tw_public_rate_limiter(args.request_interval)
    print(f"[tw-transfer-adjustment] {describe_rate_limit('tw_public', interval)}", flush=True)
    generated_at = _utc_now()
    input_receipts: list[dict[str, Any]] = []
    raw_receipts: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    artifact_rows: list[dict[str, Any]] = []
    fatal_error: str | None = None
    required_candidate_count = 0
    candidate_keys: list[str] = []
    discovery_exclusions: list[dict[str, Any]] = []
    old_output_receipt = _file_receipt(output_path) if output_path.is_file() else None
    try:
        old_summary = _read_json_object(summary_path) if summary_path.is_file() else {}
    except TransferAdjustmentError:
        old_summary = {}
    preserve_old_summary = (
        old_output_receipt is not None
        and old_summary.get("coverage_complete") is True
        and _receipt_matches(output_path, old_summary.get("output_receipt"))
    )

    try:
        start = date.fromisoformat(str(args.start_date)[:10])
        end = date.fromisoformat(resolve_end_date(str(args.end_date))[:10])
        if start < date(2000, 1, 1) or end < start:
            raise TransferAdjustmentError("requested range must be valid and start at 2000-01-01 or later")
        if int(args.overlap_sessions) < 2:
            raise TransferAdjustmentError("--overlap-sessions must be at least 2")
        if float(args.price_tolerance) <= 0.0:
            raise TransferAdjustmentError("--price-tolerance must be positive")

        paths, official_receipts = _validate_official_inputs(
            Path(args.official_input_dir), start=start, end=end
        )
        input_receipts.extend(official_receipts)
        calendar, calendar_dates = _load_calendar(paths["taiex_calendar"])
        (
            candidates,
            yahoo_receipts,
            discovery_exclusions,
            discovery_errors,
            official_twse_start,
        ) = _load_candidates(
            paths=paths,
            yahoo_dir=Path(args.yahoo_source_dir),
            start=start,
            end=end,
            calendar=calendar,
            calendar_dates=calendar_dates,
        )
        required_candidate_count = len(candidates) + len(discovery_errors)
        candidate_keys = sorted(
            {
                f"{candidate.transfer_date.isoformat()}|{candidate.symbol}"
                for candidate in candidates
            }
            | {
                f"unresolved|{str(value.get('symbol', '')).strip()}"
                for value in discovery_errors
            }
        )
        input_receipts.extend(yahoo_receipts)
        unresolved.extend(discovery_errors)
        if unresolved:
            raise TransferAdjustmentError(
                f"candidate discovery left {len(unresolved)} unresolved transfer symbols"
            )

        _, rule_receipt = _fetch_validated_raw(
            raw_dir=raw_root / "rule",
            logical_key="twse_rule59_20020730",
            suffix=".html",
            source_url=RULE_URL,
            role="twse_historical_rule59",
            validator=_parse_historical_rule,
            params=None,
            timeout=int(args.timeout),
            retries=int(args.retries),
            verify_ssl=bool(args.verify_ssl),
            resume=bool(args.resume),
        )
        raw_receipts.append(rule_receipt)

        required_years = sorted(
            {
                year
                for candidate in candidates
                for year in range(candidate.transfer_date.year, official_twse_start.year + 1)
            }
        )
        fms_payloads: dict[str, dict[int, dict[int, dict[str, float]]]] = {
            candidate.symbol: {} for candidate in candidates
        }

        def fetch_fms(item: tuple[str, int]) -> tuple[str, int, dict[int, dict[str, float]], dict[str, Any]]:
            symbol, year = item
            params = {"response": "json", "date": f"{year}0101", "stockNo": symbol}
            parsed, receipt = _fetch_validated_raw(
                raw_dir=raw_root / "fmsrfk",
                logical_key=f"fmsrfk-{symbol}-{year}",
                suffix=".json",
                source_url=FMSRFK_URL,
                role=f"twse_fmsrfk:{symbol}:{year}",
                validator=lambda content: _parse_fmsrfk_payload(
                    content, symbol=symbol, year=year
                ),
                params=params,
                timeout=int(args.timeout),
                retries=int(args.retries),
                verify_ssl=bool(args.verify_ssl),
                resume=bool(args.resume),
            )
            return symbol, year, parsed, receipt

        requests = [(candidate.symbol, year) for candidate in candidates for year in required_years]
        with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
            futures = {executor.submit(fetch_fms, item): item for item in requests}
            for future in as_completed(futures):
                symbol, year = futures[future]
                try:
                    result_symbol, result_year, payload, receipt = future.result()
                    fms_payloads[result_symbol][result_year] = payload
                    raw_receipts.append(receipt)
                except Exception as exc:
                    unresolved.append(
                        {
                            "symbol": symbol,
                            "year": year,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
        if unresolved:
            raise TransferAdjustmentError(
                f"FMSRFK retrieval left {len(unresolved)} unresolved requests"
            )

        symbols = [candidate.symbol for candidate in candidates]
        official_quotes = _load_twse_quotes(paths["twse_ohlcv"], symbols)
        corporate = _load_corporate_references(paths["corporate_reference"], symbols)
        for candidate in sorted(candidates, key=lambda value: (value.transfer_date, value.symbol)):
            try:
                row, detail = _verify_candidate(
                    candidate,
                    official_quotes=official_quotes,
                    corporate=corporate,
                    fmsrfk=fms_payloads[candidate.symbol],
                    calendar_dates=calendar_dates,
                    overlap_sessions=int(args.overlap_sessions),
                    overlap_scale_relative_tolerance=float(
                        args.overlap_scale_relative_tolerance
                    ),
                    price_tolerance=float(args.price_tolerance),
                )
                row["rule_receipt_sha256"] = rule_receipt["sha256"]
                row["official_inputs_sha256"] = hashlib.sha256(
                    "\n".join(
                        sorted(str(value["sha256"]) for value in official_receipts)
                    ).encode("ascii")
                ).hexdigest()
                symbol_raw_receipts = [
                    value
                    for value in raw_receipts
                    if str(value.get("role", "")).startswith(
                        f"twse_fmsrfk:{candidate.symbol}:"
                    )
                ]
                row["fmsrfk_receipts_sha256"] = json.dumps(
                    sorted(value["sha256"] for value in symbol_raw_receipts),
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
                detail["fmsrfk_raw_receipts"] = symbol_raw_receipts
                artifact_rows.append(row)
                details.append(detail)
            except Exception as exc:
                unresolved.append(
                    {
                        "symbol": candidate.symbol,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        if unresolved:
            raise TransferAdjustmentError(
                f"verification left {len(unresolved)} unresolved transfer symbols"
            )
        if len(artifact_rows) != len(candidates) or not artifact_rows:
            raise TransferAdjustmentError("artifact rows do not cover every discovered candidate")

        artifact = pl.DataFrame(artifact_rows).sort(["date", "symbol"])
        if artifact.select("date", "symbol").n_unique() != artifact.height:
            raise TransferAdjustmentError("artifact contains duplicate date-symbol keys")
        _write_parquet_atomic(output_path, artifact)
        output_receipt = _file_receipt(output_path, role="transfer_adjustment_artifact")
        summary = {
            "schema_version": SCHEMA_VERSION,
            "dataset": DATASET_NAME,
            "mode": str(args.mode),
            "generated_at_utc": generated_at,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "coverage_complete": True,
            "candidate_count": len(candidates),
            "required_candidate_count": len(candidates),
            "candidate_keys": candidate_keys,
            "candidate_keys_sha256": hashlib.sha256(
                "\n".join(candidate_keys).encode("utf-8")
            ).hexdigest(),
            "rows": artifact.height,
            "unresolved_count": 0,
            "unresolved": [],
            "discovery_exclusion_count": len(discovery_exclusions),
            "discovery_exclusions": discovery_exclusions,
            "output_receipt": output_receipt,
            "input_receipts": sorted(
                input_receipts, key=lambda value: (str(value.get("role")), str(value["path"]))
            ),
            "raw_receipts": sorted(
                raw_receipts, key=lambda value: (str(value.get("role")), str(value["path"]))
            ),
            "per_symbol": details,
            "off_calendar_row_count": sum(
                len(value.get("off_calendar_rows", [])) for value in details
            ),
            "overlap_sessions_required": int(args.overlap_sessions),
            "price_tolerance": float(args.price_tolerance),
            "overlap_scale_relative_tolerance": float(
                args.overlap_scale_relative_tolerance
            ),
            "rate_limit_scope": "provider-host-global",
            "request_interval_seconds": interval,
            "requests_per_second": 1.0 / interval if interval > 0 else None,
            "replacement_promoted": True,
        }
        _write_json_atomic(summary_path, summary)
        failure_summary_path.unlink(missing_ok=True)
        print(
            f"[tw-transfer-adjustment] coverage complete rows={artifact.height} "
            f"output={output_path}",
            flush=True,
        )
        return 0
    except Exception as exc:
        fatal_error = f"{type(exc).__name__}: {exc}"
        summary = {
            "schema_version": SCHEMA_VERSION,
            "dataset": DATASET_NAME,
            "mode": str(getattr(args, "mode", "unknown")),
            "generated_at_utc": generated_at,
            "coverage_complete": False,
            "candidate_count": required_candidate_count,
            "required_candidate_count": required_candidate_count,
            "candidate_keys": candidate_keys,
            "candidate_keys_sha256": hashlib.sha256(
                "\n".join(candidate_keys).encode("utf-8")
            ).hexdigest(),
            "rows": 0,
            "unresolved_count": required_candidate_count,
            "unresolved": unresolved,
            "discovery_exclusion_count": len(discovery_exclusions),
            "discovery_exclusions": discovery_exclusions,
            "verified_candidate_count_unpromoted": len(artifact_rows),
            "fatal_error": fatal_error,
            "output_receipt": None,
            "previous_output_receipt": old_output_receipt,
            "production_preserved": old_output_receipt is not None,
            "input_receipts": input_receipts,
            "raw_receipts": raw_receipts,
            "per_symbol": details,
            "rate_limit_scope": "provider-host-global",
            "request_interval_seconds": interval,
            "requests_per_second": 1.0 / interval if interval > 0 else None,
            "replacement_promoted": False,
        }
        _write_json_atomic(
            failure_summary_path if preserve_old_summary else summary_path,
            summary,
        )
        print(f"[tw-transfer-adjustment] coverage incomplete: {fatal_error}", file=sys.stderr)
        return 1


def main() -> None:
    raise SystemExit(_run(parse_args()))


if __name__ == "__main__":
    main()
