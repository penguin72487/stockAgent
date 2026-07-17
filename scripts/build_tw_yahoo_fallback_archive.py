from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any

import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_public_features import _date_column_expr
from stockagent.data.tw_security import classify_tw_stock_or_etf


YAHOO_SOURCE_METADATA_KEY = b"stockagent.source"
YAHOO_ASSET_CLASS_METADATA_KEY = b"stockagent.asset_class"
YAHOO_REQUESTED_START_METADATA_KEY = b"stockagent.yahoo_requested_start"
YAHOO_CHECKED_THROUGH_METADATA_KEY = b"stockagent.yahoo_checked_through"
FEATURE_FILE_PATTERN = re.compile(
    r"^(?P<symbol>[0-9A-Z]{4,6})(?:_(?P<venue>TW|TWO))?_features\.parquet$",
    re.IGNORECASE,
)
OUTPUT_COLUMNS = (
    "date",
    "symbol",
    "name",
    "market",
    "open",
    "max",
    "min",
    "close",
    "Trading_Volume",
    "source_adjclose",
    "source_factor",
    "quote_source",
)


@dataclass(slots=True)
class FallbackBuildResult:
    symbol: str
    status: str
    rows: int
    first_date: str | None
    last_date: str | None
    source_files: str
    transfer_adjustment_rows: int = 0
    transfer_adjustment_keys: str = ""
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Project Yahoo TW stock/ETF files into an auditable OHLCV fallback archive. "
            "The archive is lower priority than TWSE/TPEx rows in the canonical builder."
        )
    )
    parser.add_argument("--input-dir", type=Path, default=Path("data_tw_public/fallback/yahoo_tw_stocks"))
    parser.add_argument("--official-input-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("data_tw_public/fallback/yahoo_tw_ohlcv.parquet"),
    )
    parser.add_argument("--start-date", default="2000-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--transfer-adjustment-reference",
        type=Path,
        default=None,
        help=(
            "Receipt-verified date+symbol factors for the first retained Yahoo "
            "source row after a verified TWSE/TPEx transfer. Canonical rebuilds "
            "always provide this artifact."
        ),
    )
    parser.add_argument("--summary-path", type=Path, default=None)
    parser.add_argument("--report-path", type=Path, default=None)
    return parser.parse_args()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _file_receipt(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "path": str(path),
        "size": int(path.stat().st_size),
        "sha256": digest.hexdigest(),
    }


def _receipt_matches(path: Path, receipt: Any) -> bool:
    if not isinstance(receipt, dict):
        return False
    try:
        actual = _file_receipt(path)
        return (
            int(receipt["size"]) == actual["size"]
            and str(receipt["sha256"]) == actual["sha256"]
        )
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _resolve_receipt_path(receipt: Any, *, summary_path: Path) -> Path | None:
    if not isinstance(receipt, dict):
        return None
    raw = str(receipt.get("path", "")).strip()
    if not raw:
        return None
    path = Path(raw)
    candidates = [path]
    if not path.is_absolute():
        candidates.extend((REPO_ROOT / path, summary_path.parent / path))
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _load_verified_transfer_adjustments(
    path: Path,
    *,
    start: date,
    end: date,
) -> tuple[dict[tuple[str, date], float], dict[str, Any]]:
    """Load a complete, receipt-bound transfer-factor artifact.

    The downloader identifies only canonical Yahoo source first rows.  This
    loader validates lineage and content; ``_read_symbol_fallback`` separately
    enforces that each key really lands on a null-factor first row.
    """

    summary_path = path.with_suffix(".summary.json")
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"missing or invalid transfer-adjustment summary {summary_path}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(summary, dict):
        raise RuntimeError(
            f"transfer-adjustment summary is not a JSON object: {summary_path}"
        )

    unresolved = summary.get("unresolved_count")
    rows = summary.get("rows")
    candidate_count = summary.get("candidate_count")
    required_candidate_count = summary.get("required_candidate_count")
    candidate_keys = summary.get("candidate_keys")
    candidate_keys_sha256 = summary.get("candidate_keys_sha256")
    input_receipts = summary.get("input_receipts")
    raw_receipts = summary.get("raw_receipts")
    try:
        summary_start = date.fromisoformat(str(summary.get("start_date", ""))[:10])
        summary_end = date.fromisoformat(str(summary.get("end_date", ""))[:10])
        summary_range_valid = summary_start <= start <= end <= summary_end
    except (TypeError, ValueError):
        summary_range_valid = False
    candidate_keys_valid = (
        isinstance(candidate_keys, list)
        and all(isinstance(value, str) and value for value in candidate_keys)
        and len(candidate_keys) == len(set(candidate_keys))
        and candidate_keys == sorted(candidate_keys)
    )
    checks = {
        "identity": (
            summary.get("dataset") == "tw_transfer_adjustment_reference"
            and isinstance(summary.get("schema_version"), int)
            and not isinstance(summary.get("schema_version"), bool)
            and int(summary["schema_version"]) >= 1
        ),
        "date_range": summary_range_valid,
        "coverage_complete": summary.get("coverage_complete") is True,
        "replacement_promoted": summary.get("replacement_promoted") is True,
        "unresolved_count": (
            isinstance(unresolved, int)
            and not isinstance(unresolved, bool)
            and unresolved == 0
        ),
        "rows": isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0,
        "candidate_count": (
            isinstance(candidate_count, int)
            and not isinstance(candidate_count, bool)
            and candidate_count >= 0
        ),
        "required_candidate_count": (
            isinstance(required_candidate_count, int)
            and not isinstance(required_candidate_count, bool)
            and required_candidate_count >= 0
        ),
        "candidate_accounting": (
            isinstance(candidate_count, int)
            and not isinstance(candidate_count, bool)
            and isinstance(required_candidate_count, int)
            and not isinstance(required_candidate_count, bool)
            and isinstance(rows, int)
            and not isinstance(rows, bool)
            and isinstance(unresolved, int)
            and not isinstance(unresolved, bool)
            and candidate_count == required_candidate_count
            and candidate_count == rows + unresolved
        ),
        "candidate_keys": candidate_keys_valid
        and len(candidate_keys) == candidate_count,
        "candidate_keys_sha256": candidate_keys_valid
        and str(candidate_keys_sha256)
        == hashlib.sha256("\n".join(candidate_keys).encode("utf-8")).hexdigest(),
        "historical_candidates_nonempty": not (
            start <= date(2003, 12, 31) and end >= date(2004, 1, 1)
        )
        or (
            isinstance(required_candidate_count, int)
            and not isinstance(required_candidate_count, bool)
            and required_candidate_count > 0
        ),
        "output_receipt": path.is_file()
        and _receipt_matches(path, summary.get("output_receipt")),
        "input_receipts": isinstance(input_receipts, list) and bool(input_receipts),
        "raw_receipts": isinstance(raw_receipts, list) and bool(raw_receipts),
    }
    for field_name, receipts in (
        ("input_receipts", input_receipts),
        ("raw_receipts", raw_receipts),
    ):
        if not checks[field_name]:
            continue
        for receipt in receipts:
            source_path = _resolve_receipt_path(receipt, summary_path=summary_path)
            if source_path is None or not _receipt_matches(source_path, receipt):
                checks[field_name] = False
                break
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(
            f"transfer-adjustment receipt failed checks {failed}: {summary_path}"
        )

    try:
        arrow_schema = pq.read_schema(path)
        schema = set(arrow_schema.names)
        required = {"date", "symbol", "adjustment_factor"}
        missing = sorted(required - schema)
        if missing:
            raise ValueError(f"missing columns {missing}")
        selected_columns = [
            column
            for column in (
                "date",
                "symbol",
                "adjustment_factor",
                "official_reference_price",
                "reconstructed_close",
            )
            if column in schema
        ]
        frame = pl.read_parquet(
            path,
            columns=selected_columns,
        ).select(
            _date_column_expr("date").alias("date"),
            pl.col("symbol")
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("symbol"),
            pl.col("adjustment_factor")
            .cast(pl.Float64, strict=False)
            .fill_nan(None)
            .alias("adjustment_factor"),
            *[
                pl.col(column)
                .cast(pl.Float64, strict=False)
                .fill_nan(None)
                .alias(column)
                for column in (
                    "official_reference_price",
                    "reconstructed_close",
                )
                if column in schema
            ],
        )
        metadata = arrow_schema.metadata or {}
        metadata_valid = (
            metadata.get(b"stockagent.dataset")
            == b"tw_transfer_adjustment_reference"
            and metadata.get(b"stockagent.source")
            == b"TWSE_TPEx_official_verified_Yahoo_bridge"
        )
    except Exception as exc:
        raise RuntimeError(
            f"transfer-adjustment artifact is unreadable {path}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    invalid = frame.filter(
        pl.col("date").is_null()
        | pl.col("symbol").is_null()
        | ~pl.col("adjustment_factor").is_finite()
        | (pl.col("adjustment_factor") <= 0.0)
    )
    invalid_symbols = [
        symbol
        for symbol in frame.get_column("symbol").drop_nulls().unique().to_list()
        if classify_tw_stock_or_etf(str(symbol)) is None
    ]
    duplicate_keys = (
        frame.group_by(["date", "symbol"])
        .len()
        .filter(pl.col("len") > 1)
        .height
    )
    out_of_range = frame.filter(~pl.col("date").is_between(start, end)).height
    artifact_candidate_keys = sorted(
        f"{row['date'].isoformat()}|{row['symbol']}"
        for row in frame.select("date", "symbol").iter_rows(named=True)
    )
    candidate_keys_match = candidate_keys_valid and candidate_keys == artifact_candidate_keys
    factor_formula_errors = 0
    if {"official_reference_price", "reconstructed_close"} <= set(frame.columns):
        factor_formula_errors = frame.filter(
            pl.col("official_reference_price").is_null()
            | ~pl.col("official_reference_price").is_finite()
            | (pl.col("official_reference_price") <= 0.0)
            | pl.col("reconstructed_close").is_null()
            | ~pl.col("reconstructed_close").is_finite()
            | (pl.col("reconstructed_close") <= 0.0)
            | (
                (
                    pl.col("adjustment_factor")
                    - pl.col("reconstructed_close")
                    / pl.col("official_reference_price")
                ).abs()
                > 1e-10
                * pl.max_horizontal(
                    pl.lit(1.0), pl.col("adjustment_factor").abs()
                )
            )
        ).height
    if (
        not metadata_valid
        or invalid.height
        or invalid_symbols
        or duplicate_keys
        or out_of_range
        or frame.height != rows
        or not candidate_keys_match
        or factor_formula_errors
    ):
        raise RuntimeError(
            "transfer-adjustment artifact failed content checks: "
            f"metadata_valid={metadata_valid} invalid_rows={invalid.height} "
            f"invalid_symbols={len(invalid_symbols)} "
            f"duplicate_keys={duplicate_keys} out_of_range={out_of_range} "
            f"candidate_keys_match={candidate_keys_match} "
            f"factor_formula_errors={factor_formula_errors} "
            f"rows={frame.height} summary_rows={rows} path={path}"
        )

    factors = {
        (str(row["symbol"]), row["date"]): float(row["adjustment_factor"])
        for row in frame.iter_rows(named=True)
    }
    lineage = {
        "path": str(path),
        "summary_path": str(summary_path),
        "reference_receipt": _file_receipt(path),
        "summary_receipt": _file_receipt(summary_path),
        "input_receipt_count": len(input_receipts),
        "raw_receipt_count": len(raw_receipts),
        "rows": len(factors),
        "candidate_count": candidate_count,
        "required_candidate_count": required_candidate_count,
    }
    return factors, lineage


def _yahoo_input_file_receipt(
    symbol: str,
    path: Path,
    venue: str | None,
    *,
    start: date,
    end: date,
) -> dict[str, Any]:
    _schema, coverage = _validated_yahoo_source_schema(
        path,
        start=start,
        end=end,
    )
    receipt = _file_receipt(path)
    receipt.update(
        {
            "symbol": symbol,
            "venue": venue,
            **coverage,
        }
    )
    return receipt


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _canonical_symbol(path: Path) -> tuple[str, str | None] | None:
    match = FEATURE_FILE_PATTERN.fullmatch(path.name)
    if match is None:
        return None
    symbol = match.group("symbol").upper()
    if classify_tw_stock_or_etf(symbol) is None:
        return None
    venue = match.group("venue")
    return symbol, venue.upper() if venue else None


def _manifest_records(path: Path) -> dict[str, tuple[str, str, str | None]]:
    if not path.exists():
        return {}
    frame = pl.read_csv(path, infer_schema_length=10000)
    if "code" not in frame.columns:
        return {}
    output: dict[str, tuple[str, str, str | None]] = {}
    invalid: list[str] = []
    duplicates: list[str] = []
    for row_number, row in enumerate(frame.iter_rows(named=True), start=2):
        raw_code = str(row.get("code") or "").strip().upper()
        match = re.fullmatch(
            r"(?P<symbol>[0-9A-Z]{4,6})(?:_(?P<venue>TW|TWO))?",
            raw_code,
        )
        if match is None:
            invalid.append(f"row={row_number} code={raw_code!r} reason=malformed_code")
            continue
        symbol = match.group("symbol")
        if classify_tw_stock_or_etf(symbol) is None:
            invalid.append(
                f"row={row_number} code={raw_code!r} "
                "reason=not_normal_broker_tradable_stock_or_etf"
            )
            continue
        venue = match.group("venue")
        code = symbol if venue is None else f"{symbol}_{venue}"
        if code in output:
            duplicates.append(f"row={row_number} code={raw_code!r}")
            continue
        name = str(row.get("name") or symbol).strip() or symbol
        yahoo_symbol = str(row.get("yahoo_symbol") or "").strip().upper()
        market: str | None
        if "," not in yahoo_symbol and yahoo_symbol.endswith(".TWO"):
            market = "tpex"
        elif "," not in yahoo_symbol and yahoo_symbol.endswith(".TW"):
            market = "twse"
        else:
            market = None
        output[code] = (symbol, name, market)
    if invalid or duplicates:
        examples = "; ".join([*invalid[:10], *duplicates[:10]])
        raise RuntimeError(
            "Yahoo fallback raw symbol manifest contains records outside the "
            "normal broker-tradable TW stock/ETF universe or duplicate exact "
            f"codes: invalid={len(invalid)} duplicates={len(duplicates)} "
            f"examples={examples} path={path}"
        )
    return output


def _manifest_metadata(
    records: dict[str, tuple[str, str, str | None]],
) -> dict[str, tuple[str, str | None]]:
    """Collapse exact manifest records to display metadata for output symbols.

    Prefer an unsuffixed active record over venue-specific delisted archive
    records. This prevents a later ``_TW``/``_TWO`` row from changing the venue
    assigned to a canonical active Yahoo file.
    """

    output: dict[str, tuple[str, str | None]] = {}
    priorities: dict[str, int] = {}
    for code, (symbol, name, market) in records.items():
        priority = 2 if code == symbol else 1
        if priority > priorities.get(symbol, -1):
            output[symbol] = (name, market)
            priorities[symbol] = priority
    return output


def _terminal_unavailable_codes(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    try:
        frame = pl.read_csv(path, infer_schema_length=10000)
    except Exception:
        return set()
    if not {"code", "status"} <= set(frame.columns):
        return set()
    accepted_statuses = {"not_found", "delisted_no_history", "delisted_removed"}
    codes: set[str] = set()
    for row in frame.iter_rows(named=True):
        if str(row.get("status") or "").strip() not in accepted_statuses:
            continue
        raw_code = str(row.get("code") or "").strip().upper()
        match = re.fullmatch(
            r"(?P<symbol>[0-9A-Z]{4,6})(?:_(?P<venue>TW|TWO))?",
            raw_code,
        )
        if match is None or classify_tw_stock_or_etf(match.group("symbol")) is None:
            continue
        venue = match.group("venue")
        codes.add(
            match.group("symbol")
            if venue is None
            else f"{match.group('symbol')}_{venue}"
        )
    return codes


def _whitelist_markets(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    candidates: dict[str, set[str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        yahoo_symbol = line.strip().upper()
        match = re.fullmatch(r"(?P<symbol>[0-9A-Z]{4,6})\.(?P<venue>TW|TWO)", yahoo_symbol)
        if match is None:
            continue
        symbol = match.group("symbol")
        if classify_tw_stock_or_etf(symbol) is None:
            continue
        market = "twse" if match.group("venue") == "TW" else "tpex"
        candidates.setdefault(symbol, set()).add(market)
    return {
        symbol: next(iter(markets))
        for symbol, markets in candidates.items()
        if len(markets) == 1
    }


def _official_market_lookup(root: Path) -> dict[str, str]:
    frames: list[pl.DataFrame] = []
    specs = (
        (root / "twse_daily_ohlcv.parquet", "證券代號", "twse"),
        (root / "tpex_daily_ohlcv.parquet", "代號", "tpex"),
    )
    for path, symbol_column, market in specs:
        if not path.exists() or symbol_column not in pq.read_schema(path).names:
            continue
        frames.append(
            pl.read_parquet(path, columns=["date", symbol_column]).select(
                _date_column_expr("date").alias("date"),
                pl.col(symbol_column)
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .str.to_uppercase()
                .alias("symbol"),
                pl.lit(market).alias("market"),
            )
        )
    if not frames:
        return {}
    latest = (
        pl.concat(frames, how="vertical_relaxed")
        .drop_nulls(["date", "symbol"])
        .sort(["symbol", "date", "market"])
        .unique("symbol", keep="last", maintain_order=True)
    )
    return dict(latest.select(["symbol", "market"]).iter_rows())


def _source_file_groups(
    root: Path,
    *,
    allowed_codes: set[str],
) -> dict[str, list[tuple[Path, str | None]]]:
    groups: dict[str, list[tuple[Path, str | None]]] = {}
    for path in sorted(root.glob("*_features.parquet")):
        parsed = _canonical_symbol(path)
        if parsed is None:
            continue
        symbol, venue = parsed
        code = symbol if venue is None else f"{symbol}_{venue}"
        if code not in allowed_codes:
            continue
        groups.setdefault(symbol, []).append((path, venue))
    return groups


def _number(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Float64, strict=False).fill_nan(None)


def _validated_yahoo_source_schema(
    path: Path,
    *,
    start: date,
    end: date,
) -> tuple[Any, dict[str, str]]:
    schema = pq.read_schema(path)
    metadata = schema.metadata or {}

    def required_text(key: bytes) -> str:
        raw = metadata.get(key)
        if not raw:
            raise ValueError(f"{path} missing required parquet metadata {key!r}")
        try:
            value = raw.decode("utf-8", errors="strict").strip()
        except UnicodeDecodeError as exc:
            raise ValueError(f"{path} has invalid UTF-8 metadata {key!r}") from exc
        if not value:
            raise ValueError(f"{path} has empty required parquet metadata {key!r}")
        return value

    source = required_text(YAHOO_SOURCE_METADATA_KEY)
    asset_class = required_text(YAHOO_ASSET_CLASS_METADATA_KEY)
    requested_start_text = required_text(YAHOO_REQUESTED_START_METADATA_KEY)
    checked_through_text = required_text(YAHOO_CHECKED_THROUGH_METADATA_KEY)
    try:
        requested_start = date.fromisoformat(requested_start_text)
        checked_through = date.fromisoformat(checked_through_text)
    except ValueError as exc:
        raise ValueError(f"{path} has invalid Yahoo coverage-date metadata") from exc
    if source != "yahoo":
        raise ValueError(f"{path} has unexpected source metadata: {source!r}")
    if asset_class != "tw_stocks":
        raise ValueError(f"{path} has unexpected asset-class metadata: {asset_class!r}")
    if requested_start > start:
        raise ValueError(
            f"{path} Yahoo requested_start={requested_start} is later than {start}"
        )
    if checked_through < end:
        raise ValueError(
            f"{path} Yahoo checked_through={checked_through} is earlier than {end}"
        )
    return schema, {
        "source": source,
        "asset_class": asset_class,
        "requested_start": requested_start_text,
        "checked_through": checked_through_text,
    }


def _read_symbol_fallback(
    symbol: str,
    sources: list[tuple[Path, str | None]],
    *,
    manifest: dict[str, tuple[str, str | None]],
    official_markets: dict[str, str],
    start: date,
    end: date,
    transfer_adjustments: dict[tuple[str, date], float] | None = None,
) -> tuple[FallbackBuildResult, pl.DataFrame | None]:
    source_names = ",".join(str(path) for path, _ in sources)
    transfer_adjustments = transfer_adjustments or {}
    try:
        frames: list[pl.DataFrame] = []
        applied_transfer_keys: list[str] = []
        manifest_name, manifest_market = manifest.get(symbol, (symbol, None))
        prepared_sources: list[tuple[Path, str | None, Any, set[str], str]] = []
        for path, venue in sources:
            schema, _coverage_metadata = _validated_yahoo_source_schema(
                path,
                start=start,
                end=end,
            )
            columns = set(schema.names)
            required = {"date", "open", "max", "min", "close", "Trading_Volume"}
            missing = sorted(required - columns)
            if missing:
                raise ValueError(f"{path} missing Yahoo OHLCV columns: {missing}")
            if venue == "TWO":
                market = "tpex"
            elif venue == "TW":
                market = "twse"
            else:
                market = manifest_market or official_markets.get(symbol)
            if market not in {"twse", "tpex"}:
                raise ValueError(f"cannot resolve TWSE/TPEx venue for Yahoo symbol {symbol}")
            prepared_sources.append((path, venue, schema, columns, market))

        data_sources = prepared_sources
        canonical_sources = [
            source for source in prepared_sources if source[1] is None
        ]
        if len(prepared_sources) > 1 and canonical_sources:
            if len(canonical_sources) != 1:
                raise ValueError(
                    f"multiple unsuffixed Yahoo canonical sources for symbol {symbol}"
                )
            canonical_source = canonical_sources[0]
            canonical_market = canonical_source[4]
            same_market_aliases = [
                source
                for source in prepared_sources
                if source[1] is not None and source[4] == canonical_market
            ]
            comparison_frames: dict[Path, pl.DataFrame] = {}

            def comparison_frame(
                source: tuple[Path, str | None, Any, set[str], str],
            ) -> pl.DataFrame:
                path, _venue, _schema, columns, _market = source
                cached = comparison_frames.get(path)
                if cached is not None:
                    return cached
                frame = (
                    pl.read_parquet(
                        path,
                        columns=[
                            column
                            for column in (
                                "date",
                                "open",
                                "max",
                                "min",
                                "close",
                                "Trading_Volume",
                            )
                            if column in columns
                        ],
                    )
                    .select(
                        _date_column_expr("date").alias("date"),
                        *[
                            _number(column).alias(column)
                            for column in (
                                "open",
                                "max",
                                "min",
                                "close",
                                "Trading_Volume",
                            )
                        ],
                    )
                    .filter(pl.col("date").is_between(start, end))
                    .sort("date")
                )
                duplicate_dates = frame.height - frame["date"].n_unique()
                if duplicate_dates:
                    raise ValueError(
                        f"Yahoo source {path} has {duplicate_dates} duplicate dates"
                    )
                comparison_frames[path] = frame
                return frame

            canonical_comparison = comparison_frame(canonical_source)
            for alias_source in same_market_aliases:
                alias_comparison = comparison_frame(alias_source)
                overlap = canonical_comparison.join(
                    alias_comparison,
                    on="date",
                    how="inner",
                    suffix="_alias",
                )
                conflicts = overlap.filter(
                    pl.any_horizontal(
                        [
                            ~pl.col(column).eq_missing(
                                pl.col(f"{column}_alias")
                            )
                            for column in (
                                "open",
                                "max",
                                "min",
                                "close",
                                "Trading_Volume",
                            )
                        ]
                    )
                )
                if conflicts.height:
                    raise ValueError(
                        "same-market Yahoo alias OHLCV conflict on "
                        f"{conflicts.height} date-symbol keys for {symbol}"
                    )
            # Venue-suffixed records remain in the exact-code input receipt
            # chain, but a same-market alias must never compete with the
            # unsuffixed canonical file for OHLCV or adjustment factors.  A
            # source resolved to the other exchange remains a distinct market
            # history and continues through the existing conflict gate below.
            data_sources = [
                source
                for source in prepared_sources
                if source[1] is None or source[4] != canonical_market
            ]

        for path, venue, _schema, columns, market in data_sources:
            adjclose = (
                pl.coalesce(_number("adjclose"), _number("close"))
                if "adjclose" in columns
                else _number("close")
            )
            frame = (
                pl.read_parquet(
                    path,
                    columns=[
                        column
                        for column in (
                            "date",
                            "open",
                            "max",
                            "min",
                            "close",
                            "Trading_Volume",
                            "adjclose",
                        )
                        if column in columns
                    ],
                )
                .select(
                    _date_column_expr("date").alias("date"),
                    pl.lit(symbol).alias("symbol"),
                    pl.lit(manifest_name).alias("name"),
                    pl.lit(market).alias("market"),
                    _number("open").alias("open"),
                    _number("max").alias("max"),
                    _number("min").alias("min"),
                    _number("close").alias("close"),
                    _number("Trading_Volume").fill_null(0.0).alias("Trading_Volume"),
                    adjclose.alias("source_adjclose"),
                    pl.lit("yahoo_fallback").alias("quote_source"),
                )
                .filter(pl.col("date").is_between(start, end))
                .drop_nulls(["date", "open", "max", "min", "close", "source_adjclose"])
                .filter(
                    pl.all_horizontal(
                        [
                            pl.col(column).is_finite() & (pl.col(column) > 0.0)
                            for column in ("open", "max", "min", "close", "source_adjclose")
                        ]
                    )
                    & (pl.col("max") >= pl.max_horizontal("open", "close"))
                    & (pl.col("min") <= pl.min_horizontal("open", "close"))
                    & (pl.col("max") >= pl.col("min"))
                    & pl.col("Trading_Volume").is_finite()
                    & (pl.col("Trading_Volume") >= 0.0)
                )
                .sort("date")
                .with_columns(
                    (
                        pl.col("source_adjclose")
                        / pl.col("source_adjclose").shift(1)
                    ).alias("source_factor")
                )
            )
            if not frame.is_empty():
                first_date = frame.item(0, "date")
                transfer_key = (symbol, first_date)
                transfer_factor = transfer_adjustments.get(transfer_key)
                if transfer_factor is not None and venue is None:
                    if frame.item(0, "source_factor") is not None:
                        raise ValueError(
                            "transfer adjustment matched a Yahoo source first row "
                            f"whose source_factor was already populated: {symbol}@{first_date}"
                        )
                    frame = frame.with_row_index("_source_row_index").with_columns(
                        pl.when(pl.col("_source_row_index") == 0)
                        .then(pl.lit(float(transfer_factor)))
                        .otherwise(pl.col("source_factor"))
                        .alias("source_factor")
                    ).drop("_source_row_index")
                    applied_transfer_keys.append(
                        f"{symbol}@{first_date.isoformat()}"
                    )
                frames.append(frame)
        if not frames:
            return (
                FallbackBuildResult(symbol, "empty", 0, None, None, source_names),
                None,
            )
        combined = pl.concat(frames, how="vertical_relaxed")
        conflicts = (
            combined.group_by(["date", "symbol"])
            .agg(pl.col("close").drop_nulls().n_unique().alias("close_values"))
            .filter(pl.col("close_values") > 1)
        )
        if conflicts.height:
            raise ValueError(
                f"Yahoo .TW/.TWO candidates conflict on {conflicts.height} date-symbol keys"
            )
        combined = (
            combined.sort(["symbol", "date", "market"])
            .unique(["symbol", "date"], keep="last", maintain_order=True)
            .select(OUTPUT_COLUMNS)
        )
        if len(applied_transfer_keys) != len(set(applied_transfer_keys)):
            raise ValueError(
                f"transfer adjustment applied more than once for Yahoo symbol {symbol}"
            )
        return (
            FallbackBuildResult(
                symbol=symbol,
                status="ok",
                rows=int(combined.height),
                first_date=str(combined["date"].min()),
                last_date=str(combined["date"].max()),
                source_files=source_names,
                transfer_adjustment_rows=len(applied_transfer_keys),
                transfer_adjustment_keys=",".join(sorted(applied_transfer_keys)),
            ),
            combined,
        )
    except Exception as exc:
        return (
            FallbackBuildResult(
                symbol=symbol,
                status="failed",
                rows=0,
                first_date=None,
                last_date=None,
                source_files=source_names,
                message=f"{type(exc).__name__}: {exc}",
            ),
            None,
        )


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start < date(2000, 1, 1):
        raise ValueError("Yahoo fallback is restricted to 2000-01-01 and later")
    if end < start:
        raise ValueError("--end-date must be on or after --start-date")
    if not args.input_dir.is_dir():
        raise FileNotFoundError(f"Yahoo fallback directory is missing: {args.input_dir}")

    transfer_adjustments: dict[tuple[str, date], float] = {}
    transfer_lineage: dict[str, Any] | None = None
    if args.transfer_adjustment_reference is not None:
        transfer_adjustments, transfer_lineage = _load_verified_transfer_adjustments(
            args.transfer_adjustment_reference,
            start=start,
            end=end,
        )

    symbol_manifest_path = args.input_dir / "symbols.csv"
    terminal_report_path = args.input_dir / "download_report.csv"
    manifest_records = _manifest_records(symbol_manifest_path)
    if not manifest_records:
        raise RuntimeError(
            f"Yahoo fallback symbol manifest is missing or empty: {symbol_manifest_path}"
        )
    manifest_codes = set(manifest_records)
    manifest = _manifest_metadata(manifest_records)
    manifest_symbols = set(manifest)
    groups = _source_file_groups(args.input_dir, allowed_codes=manifest_codes)
    if not groups:
        raise RuntimeError(f"no manifest-selected Yahoo stock/ETF parquet files found under {args.input_dir}")
    available_codes = {
        symbol if venue is None else f"{symbol}_{venue}"
        for symbol, sources in groups.items()
        for _path, venue in sources
    }
    terminal_unavailable_codes = _terminal_unavailable_codes(terminal_report_path)
    unaccounted_codes = manifest_codes - available_codes - terminal_unavailable_codes
    if unaccounted_codes:
        examples = ", ".join(sorted(unaccounted_codes)[:20])
        raise RuntimeError(
            "Yahoo fallback manifest contains records with neither a verified "
            f"source file nor a terminal unavailable result: count={len(unaccounted_codes)} "
            f"examples={examples}"
        )
    for symbol, market in _whitelist_markets(
        args.input_dir / "yahoo_whitelist.txt"
    ).items():
        name, manifest_market = manifest.get(symbol, (symbol, None))
        if manifest_market is None:
            manifest[symbol] = (name, market)
    official_markets = _official_market_lookup(args.official_input_dir)
    results: list[FallbackBuildResult] = []
    frames: list[pl.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                _read_symbol_fallback,
                symbol,
                sources,
                manifest=manifest,
                official_markets=official_markets,
                start=start,
                end=end,
                transfer_adjustments=transfer_adjustments,
            ): symbol
            for symbol, sources in groups.items()
        }
        for future in as_completed(futures):
            result, frame = future.result()
            results.append(result)
            if frame is not None:
                frames.append(frame)

    results.sort(key=lambda item: item.symbol)
    failures = [item for item in results if item.status == "failed"]
    expected_transfer_keys = {
        f"{symbol}@{row_date.isoformat()}"
        for symbol, row_date in transfer_adjustments
    }
    applied_transfer_key_list = [
        key
        for result in results
        for key in result.transfer_adjustment_keys.split(",")
        if key
    ]
    applied_transfer_keys = set(applied_transfer_key_list)
    duplicate_transfer_applications = len(applied_transfer_key_list) - len(
        applied_transfer_keys
    )
    unmatched_transfer_keys = expected_transfer_keys - applied_transfer_keys
    unexpected_transfer_keys = applied_transfer_keys - expected_transfer_keys
    transfer_accounting_complete = (
        duplicate_transfer_applications == 0
        and not unmatched_transfer_keys
        and not unexpected_transfer_keys
        and len(applied_transfer_keys) == len(expected_transfer_keys)
    )
    summary_path = args.summary_path or args.output_path.with_suffix(".summary.json")
    report_path = args.report_path or args.output_path.with_suffix(".report.csv")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [asdict(item) for item in results], infer_schema_length=None
    ).write_csv(report_path)
    source_file_count = sum(len(sources) for sources in groups.values())
    summary: dict[str, Any] = {
        "schema_version": 3,
        "generated_at_utc": _utc_now(),
        "source": "yahoo_fallback",
        "input_dir": str(args.input_dir),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "source_symbol_count": len(groups),
        "source_file_count": source_file_count,
        "manifest_symbol_count": len(manifest_symbols),
        "manifest_record_count": len(manifest_codes),
        "terminal_unavailable_record_count": len(
            terminal_unavailable_codes & manifest_codes
        ),
        "symbol_accounting_complete": not unaccounted_codes,
        "ok_symbol_count": sum(item.status == "ok" for item in results),
        "empty_symbol_count": sum(item.status == "empty" for item in results),
        "failed_symbol_count": len(failures),
        "rows": sum(item.rows for item in results),
        "output_path": str(args.output_path),
        "report_path": str(report_path),
        "coverage_receipts_complete": False,
        "transfer_adjustment_enabled": transfer_lineage is not None,
        "transfer_adjustment_reference_path": (
            transfer_lineage["path"] if transfer_lineage is not None else None
        ),
        "transfer_adjustment_reference_rows": len(expected_transfer_keys),
        "transfer_adjustment_candidate_count": (
            transfer_lineage["candidate_count"]
            if transfer_lineage is not None
            else 0
        ),
        "transfer_adjustment_required_candidate_count": (
            transfer_lineage["required_candidate_count"]
            if transfer_lineage is not None
            else 0
        ),
        "transfer_adjustment_applied_rows": len(applied_transfer_keys),
        "transfer_adjustment_unmatched_rows": len(unmatched_transfer_keys),
        "transfer_adjustment_unexpected_rows": len(unexpected_transfer_keys),
        "transfer_adjustment_duplicate_applications": duplicate_transfer_applications,
        "transfer_adjustment_accounting_complete": transfer_accounting_complete,
    }
    if failures:
        summary["failure_examples"] = [asdict(item) for item in failures[:20]]
        _write_json(summary_path, summary)
        raise RuntimeError(
            f"Yahoo fallback archive build failed for {len(failures)} symbols; report={report_path}"
        )
    if not frames:
        _write_json(summary_path, summary)
        raise RuntimeError("Yahoo fallback archive contains no usable rows")
    if not transfer_accounting_complete:
        summary["transfer_adjustment_unmatched_examples"] = sorted(
            unmatched_transfer_keys
        )[:20]
        summary["transfer_adjustment_unexpected_examples"] = sorted(
            unexpected_transfer_keys
        )[:20]
        _write_json(summary_path, summary)
        raise RuntimeError(
            "transfer adjustments did not map 1:1 to null-factor Yahoo source "
            "first rows: "
            f"reference={len(expected_transfer_keys)} "
            f"applied={len(applied_transfer_keys)} "
            f"unmatched={len(unmatched_transfer_keys)} "
            f"unexpected={len(unexpected_transfer_keys)} "
            f"duplicates={duplicate_transfer_applications}"
        )

    flattened_sources = [
        (symbol, path, venue)
        for symbol, sources in groups.items()
        for path, venue in sources
    ]
    input_receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                _yahoo_input_file_receipt,
                symbol,
                path,
                venue,
                start=start,
                end=end,
            ): (symbol, path)
            for symbol, path, venue in flattened_sources
        }
        for future in as_completed(futures):
            input_receipts.append(future.result())
    input_receipts.sort(key=lambda item: (str(item["symbol"]), str(item["path"])))
    if len(input_receipts) != source_file_count:
        raise RuntimeError(
            "Yahoo fallback input receipt count does not match selected source files"
        )
    input_manifest_path = args.output_path.with_suffix(".inputs.json")
    _write_json(
        input_manifest_path,
        {
            "schema_version": 1,
            "generated_at_utc": _utc_now(),
            "source": "yahoo",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "file_count": len(input_receipts),
            "files": input_receipts,
            "manifest_symbol_count": len(manifest_symbols),
            "manifest_record_count": len(manifest_codes),
            "terminal_unavailable_codes": sorted(
                terminal_unavailable_codes & manifest_codes
            ),
            "symbol_manifest_receipt": _file_receipt(symbol_manifest_path),
            "terminal_report_receipt": (
                _file_receipt(terminal_report_path)
                if terminal_unavailable_codes & manifest_codes
                else None
            ),
            "transfer_adjustment_reference_receipt": (
                transfer_lineage["reference_receipt"]
                if transfer_lineage is not None
                else None
            ),
            "transfer_adjustment_summary_receipt": (
                transfer_lineage["summary_receipt"]
                if transfer_lineage is not None
                else None
            ),
        },
    )
    summary.update(
        {
            "verified_source_file_count": len(input_receipts),
            "input_manifest_path": str(input_manifest_path),
            "input_manifest_receipt": _file_receipt(input_manifest_path),
            "coverage_receipts_complete": True,
            "transfer_adjustment_reference_receipt": (
                transfer_lineage["reference_receipt"]
                if transfer_lineage is not None
                else None
            ),
            "transfer_adjustment_summary_receipt": (
                transfer_lineage["summary_receipt"]
                if transfer_lineage is not None
                else None
            ),
            "transfer_adjustment_input_receipt_count": (
                transfer_lineage["input_receipt_count"]
                if transfer_lineage is not None
                else 0
            ),
            "transfer_adjustment_raw_receipt_count": (
                transfer_lineage["raw_receipt_count"]
                if transfer_lineage is not None
                else 0
            ),
        }
    )

    archive = (
        pl.concat(frames, how="vertical_relaxed")
        .sort(["symbol", "date", "market"])
        .unique(["symbol", "date"], keep="last", maintain_order=True)
    )
    duplicate_keys = (
        archive.group_by(["symbol", "date"]).len().filter(pl.col("len") > 1).height
    )
    if duplicate_keys:
        raise RuntimeError(f"Yahoo fallback archive has {duplicate_keys} duplicate keys")
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{args.output_path.name}.",
        suffix=".tmp",
        dir=args.output_path.parent,
        delete=False,
    )
    temporary = Path(handle.name)
    handle.close()
    try:
        archive.write_parquet(temporary, compression="zstd", statistics=True)
        os.replace(temporary, args.output_path)
    finally:
        temporary.unlink(missing_ok=True)
    summary.update(
        {
            "rows": int(archive.height),
            "symbols": int(archive["symbol"].n_unique()),
            "first_date": str(archive["date"].min()),
            "last_date": str(archive["date"].max()),
            "output_receipt": _file_receipt(args.output_path),
        }
    )
    _write_json(summary_path, summary)
    print(
        f"[tw-yahoo-fallback] rows={archive.height} symbols={summary['symbols']} "
        f"output={args.output_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
