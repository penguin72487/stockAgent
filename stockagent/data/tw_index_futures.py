"""Official TAIFEX day-session data for Taiwan index-futures strategies.

The source files are the CSV/ZIP artifacts returned by TAIFEX's
``futDataDown`` endpoint.  They contain both the general (day) and after-hours
sessions.  This module deliberately keeps only ``一般`` rows and monthly
contracts, then selects the nearest listed monthly contract for each
date/product.  Weekly MTX rows and calendar spreads are never mixed into the
continuous series.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
import csv
from datetime import date
import hashlib
import io
import math
from pathlib import Path
import re
from typing import Final, TextIO
import zipfile

import numpy as np


TAIFEX_INDEX_FUTURES_PRODUCTS: Final[tuple[str, ...]] = ("TX", "MTX", "TMF")
TAIFEX_INDEX_FUTURES_MULTIPLIERS: Final[dict[str, int]] = {
    "TX": 200,
    "MTX": 50,
    "TMF": 10,
}
# Shioaji and TAIFEX do not use the same root spelling for every product.
# These roots are lookup hints only.  Live code must resolve a concrete
# contract through Contract V2 rather than constructing an expiry code.
SHIOAJI_FUTURES_ROOTS: Final[dict[str, str]] = {
    "TX": "TXF",
    "MTX": "MXF",
    "TMF": "TMF",
}

TAIFEX_DAY_SESSION_LABEL: Final[str] = "一般"
TAIFEX_FUTURES_DATA_CONTRACT_VERSION: Final[int] = 1
_MONTHLY_CONTRACT_RE = re.compile(r"^[0-9]{6}$")
_DAY_SESSION_ALIASES: Final[frozenset[str]] = frozenset(
    {"一般", "一般交易時段", "day", "day_session", "regular"}
)
_REQUIRED_SOURCE_COLUMNS: Final[tuple[str, ...]] = (
    "交易日期",
    "契約",
    "到期月份(週別)",
    "開盤價",
    "最高價",
    "最低價",
    "收盤價",
    "成交量",
)
_NORMALIZED_COLUMNS: Final[tuple[str, ...]] = (
    "date",
    "product",
    "contract_month",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "log_return",
    "multiplier",
    "source_file",
    "source_sha256",
)


@dataclass(frozen=True, slots=True)
class TaiwanIndexFuturesDaySession:
    """Front-month day-session arrays aligned to a requested date axis."""

    dates: np.ndarray
    products: tuple[str, ...]
    contract_months: np.ndarray
    open_prices: np.ndarray
    high_prices: np.ndarray
    low_prices: np.ndarray
    close_prices: np.ndarray
    volumes: np.ndarray
    log_returns: np.ndarray
    tradable_mask: np.ndarray
    multipliers: np.ndarray

    def product_index(self, product: str) -> int:
        normalized = normalize_taifex_index_futures_product(product)
        try:
            return self.products.index(normalized)
        except ValueError as exc:
            raise KeyError(
                f"product {normalized!r} is absent; available={self.products}"
            ) from exc

    def reference_log_returns(self, product: str = "TX") -> np.ndarray:
        return self.log_returns[:, self.product_index(product)]

    def reference_tradable_mask(self, product: str = "TX") -> np.ndarray:
        return self.tradable_mask[:, self.product_index(product)]


@dataclass(frozen=True, slots=True)
class _FrontMonthRow:
    date: np.datetime64
    product: str
    contract_month: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    source_file: str
    source_sha256: str

    @property
    def log_return(self) -> float:
        return float(math.log(self.close / self.open))


def normalize_taifex_index_futures_product(product: object) -> str:
    normalized = str(product or "").strip().upper()
    aliases = {
        "TXF": "TX",
        "TX": "TX",
        "大台": "TX",
        "大臺": "TX",
        "MTX": "MTX",
        "MXF": "MTX",
        "小台": "MTX",
        "小臺": "MTX",
        "TMF": "TMF",
        "微台": "TMF",
        "微臺": "TMF",
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise ValueError(
            "Taiwan index-futures product must be one of TX, MTX, or TMF"
        ) from exc


def _normalized_products(products: Sequence[object] | None) -> tuple[str, ...]:
    values = (
        TAIFEX_INDEX_FUTURES_PRODUCTS
        if products is None
        else tuple(normalize_taifex_index_futures_product(item) for item in products)
    )
    if not values:
        raise ValueError("products must not be empty")
    if len(set(values)) != len(values):
        raise ValueError(f"products contains duplicates: {values}")
    return tuple(values)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _decoded_csv_stream(
    source_path: Path,
) -> Iterator[tuple[TextIO, str, str]]:
    """Yield one decoded CSV stream at a time from CSV or ZIP input."""

    source_sha256 = _sha256_path(source_path)
    if source_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(source_path) as archive:
            members = [
                info
                for info in archive.infolist()
                if not info.is_dir() and info.filename.lower().endswith(".csv")
            ]
            if not members:
                raise ValueError(f"{source_path} contains no CSV member")
            for member in members:
                with archive.open(member, "r") as binary:
                    text = io.TextIOWrapper(
                        binary,
                        encoding="cp950",
                        errors="strict",
                        newline="",
                    )
                    try:
                        yield text, f"{source_path.name}:{member.filename}", source_sha256
                    finally:
                        text.detach()
        return

    if source_path.suffix.lower() != ".csv":
        raise ValueError(f"unsupported TAIFEX source type: {source_path}")
    with source_path.open("rb") as binary:
        text = io.TextIOWrapper(
            binary,
            encoding="cp950",
            errors="strict",
            newline="",
        )
        try:
            yield text, source_path.name, source_sha256
        finally:
            text.detach()


def _parse_price(value: object) -> float:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "-", "--"}:
        return float("nan")
    try:
        parsed = float(text)
    except ValueError:
        return float("nan")
    return parsed if math.isfinite(parsed) and parsed > 0.0 else float("nan")


def _parse_volume(value: object) -> int:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "-", "--"}:
        return 0
    try:
        parsed = int(float(text))
    except ValueError:
        return 0
    return max(0, parsed)


def _parse_trading_date(value: object) -> np.datetime64 | None:
    text = str(value or "").strip().replace("-", "/")
    parts = text.split("/")
    if len(parts) != 3:
        return None
    try:
        parsed = date(*(int(part) for part in parts))
    except (TypeError, ValueError):
        return None
    return np.datetime64(parsed, "D")


def _iter_monthly_day_rows(
    source_path: Path,
    *,
    products: tuple[str, ...],
) -> Iterator[_FrontMonthRow]:
    product_set = set(products)
    for stream, source_name, source_sha256 in _decoded_csv_stream(source_path):
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise ValueError(f"{source_name} has no CSV header")
        reader.fieldnames = [
            str(name or "").lstrip("\ufeff").strip() for name in reader.fieldnames
        ]
        missing = sorted(set(_REQUIRED_SOURCE_COLUMNS) - set(reader.fieldnames))
        if missing:
            raise ValueError(
                f"{source_name} is missing TAIFEX columns: {missing}"
            )
        has_session_column = "交易時段" in reader.fieldnames
        for raw in reader:
            product = str(raw.get("契約") or "").strip().upper()
            if product not in product_set:
                continue
            if has_session_column:
                session = str(raw.get("交易時段") or "").strip().casefold()
                if session not in _DAY_SESSION_ALIASES:
                    continue
            contract_month = str(raw.get("到期月份(週別)") or "").strip()
            if _MONTHLY_CONTRACT_RE.fullmatch(contract_month) is None:
                continue
            date_value = _parse_trading_date(raw.get("交易日期"))
            if date_value is None:
                continue
            open_price = _parse_price(raw.get("開盤價"))
            high_price = _parse_price(raw.get("最高價"))
            low_price = _parse_price(raw.get("最低價"))
            close_price = _parse_price(raw.get("收盤價"))
            volume = _parse_volume(raw.get("成交量"))
            if not all(
                math.isfinite(value) and value > 0.0
                for value in (open_price, high_price, low_price, close_price)
            ):
                continue
            if volume <= 0:
                continue
            yield _FrontMonthRow(
                date=date_value,
                product=product,
                contract_month=contract_month,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
                source_file=source_name,
                source_sha256=source_sha256,
            )


def _front_month_rows(
    source_paths: Iterable[str | Path],
    *,
    products: tuple[str, ...],
) -> list[_FrontMonthRow]:
    by_contract: dict[tuple[np.datetime64, str, str], _FrontMonthRow] = {}
    for raw_path in source_paths:
        path = Path(raw_path).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"TAIFEX source does not exist: {path}")
        for row in _iter_monthly_day_rows(path, products=products):
            key = (row.date, row.product, row.contract_month)
            previous = by_contract.get(key)
            if previous is None:
                by_contract[key] = row
                continue
            comparable_previous = (
                previous.open,
                previous.high,
                previous.low,
                previous.close,
                previous.volume,
            )
            comparable_current = (
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
            )
            if comparable_current != comparable_previous:
                raise ValueError(
                    "conflicting TAIFEX rows for "
                    f"{row.date}/{row.product}/{row.contract_month}: "
                    f"{previous.source_file} vs {row.source_file}"
                )

    grouped: dict[tuple[np.datetime64, str], list[_FrontMonthRow]] = {}
    for row in by_contract.values():
        grouped.setdefault((row.date, row.product), []).append(row)

    selected: list[_FrontMonthRow] = []
    for key, candidates in grouped.items():
        date_value, _product = key
        calendar_month = str(date_value.astype("datetime64[M]")).replace("-", "")
        nonexpired = [
            row for row in candidates if row.contract_month >= calendar_month
        ]
        pool = nonexpired if nonexpired else candidates
        selected.append(min(pool, key=lambda row: row.contract_month))
    return sorted(selected, key=lambda row: (row.date, products.index(row.product)))


def build_taifex_index_futures_day_session(
    source_paths: Iterable[str | Path],
    output_path: str | Path,
    *,
    products: Sequence[object] | None = None,
) -> Path:
    """Normalize official files into one front-month day-session parquet."""

    normalized_products = _normalized_products(products)
    rows = _front_month_rows(source_paths, products=normalized_products)
    if not rows:
        raise ValueError("no usable front-month TAIFEX day-session rows were found")

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {
            "date": pa.array(
                np.asarray(
                    [row.date for row in rows],
                    dtype="datetime64[D]",
                ).astype(np.int32),
                type=pa.date32(),
            ),
            "product": [row.product for row in rows],
            "contract_month": [row.contract_month for row in rows],
            "open": np.asarray([row.open for row in rows], dtype=np.float64),
            "high": np.asarray([row.high for row in rows], dtype=np.float64),
            "low": np.asarray([row.low for row in rows], dtype=np.float64),
            "close": np.asarray([row.close for row in rows], dtype=np.float64),
            "volume": np.asarray([row.volume for row in rows], dtype=np.int64),
            "log_return": np.asarray(
                [row.log_return for row in rows],
                dtype=np.float64,
            ),
            "multiplier": np.asarray(
                [TAIFEX_INDEX_FUTURES_MULTIPLIERS[row.product] for row in rows],
                dtype=np.int64,
            ),
            "source_file": [row.source_file for row in rows],
            "source_sha256": [row.source_sha256 for row in rows],
        }
    )
    metadata = dict(table.schema.metadata or {})
    metadata.update(
        {
            b"stockagent.dataset": b"tw_index_futures_day_session_front_month",
            b"stockagent.contract_version": str(
                TAIFEX_FUTURES_DATA_CONTRACT_VERSION
            ).encode("ascii"),
            b"stockagent.session": TAIFEX_DAY_SESSION_LABEL.encode("utf-8"),
            b"stockagent.products": ",".join(normalized_products).encode("ascii"),
        }
    )
    table = table.replace_schema_metadata(metadata)
    target = Path(output_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    pq.write_table(table, temporary, compression="zstd")
    temporary.replace(target)
    return target


def load_taifex_index_futures_day_session(
    path: str | Path,
    *,
    panel_dates: np.ndarray | None = None,
    products: Sequence[object] | None = None,
) -> TaiwanIndexFuturesDaySession:
    """Load and optionally align normalized front-month data to panel dates."""

    import pyarrow.parquet as pq

    normalized_products = _normalized_products(products)
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(
            f"TAIFEX day-session parquet does not exist: {source}"
        )
    table = pq.read_table(source)
    missing = sorted(set(_NORMALIZED_COLUMNS) - set(table.column_names))
    if missing:
        raise ValueError(f"{source} is missing normalized columns: {missing}")
    metadata = table.schema.metadata or {}
    contract_version = metadata.get(b"stockagent.contract_version")
    if contract_version is not None and int(contract_version) != (
        TAIFEX_FUTURES_DATA_CONTRACT_VERSION
    ):
        raise ValueError(
            f"{source} uses unsupported futures data contract "
            f"{contract_version!r}"
        )

    payload = table.select(
        [
            "date",
            "product",
            "contract_month",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "log_return",
            "multiplier",
        ]
    ).to_pydict()
    source_dates = np.asarray(payload["date"], dtype="datetime64[D]")
    if panel_dates is None:
        dates = np.unique(source_dates)
    else:
        dates = np.asarray(panel_dates, dtype="datetime64[D]")
        if dates.ndim != 1 or dates.size == 0:
            raise ValueError("panel_dates must be a non-empty one-dimensional array")
        if dates.size > 1 and bool(np.any(dates[1:] <= dates[:-1])):
            raise ValueError("panel_dates must be strictly increasing")

    rows = int(dates.size)
    columns = len(normalized_products)
    contract_months = np.full((rows, columns), "", dtype="U16")
    open_prices = np.full((rows, columns), np.nan, dtype=np.float64)
    high_prices = np.full((rows, columns), np.nan, dtype=np.float64)
    low_prices = np.full((rows, columns), np.nan, dtype=np.float64)
    close_prices = np.full((rows, columns), np.nan, dtype=np.float64)
    volumes = np.zeros((rows, columns), dtype=np.int64)
    log_returns = np.full((rows, columns), np.nan, dtype=np.float64)
    date_to_row = {date_value: idx for idx, date_value in enumerate(dates)}
    product_to_col = {
        product: idx for idx, product in enumerate(normalized_products)
    }
    seen: set[tuple[int, int]] = set()

    for source_row, (date_value, raw_product) in enumerate(
        zip(source_dates, payload["product"], strict=True)
    ):
        product = str(raw_product).strip().upper()
        if product not in product_to_col or date_value not in date_to_row:
            continue
        row = date_to_row[date_value]
        col = product_to_col[product]
        key = (row, col)
        if key in seen:
            raise ValueError(
                f"{source} contains duplicate normalized row {date_value}/{product}"
            )
        seen.add(key)
        contract_months[row, col] = str(
            payload["contract_month"][source_row]
        ).strip()
        open_prices[row, col] = float(payload["open"][source_row])
        high_prices[row, col] = float(payload["high"][source_row])
        low_prices[row, col] = float(payload["low"][source_row])
        close_prices[row, col] = float(payload["close"][source_row])
        volumes[row, col] = int(payload["volume"][source_row])
        log_returns[row, col] = float(payload["log_return"][source_row])
        expected_multiplier = TAIFEX_INDEX_FUTURES_MULTIPLIERS[product]
        if int(payload["multiplier"][source_row]) != expected_multiplier:
            raise ValueError(
                f"{source} has multiplier mismatch for {product}: "
                f"{payload['multiplier'][source_row]} != {expected_multiplier}"
            )

    tradable = (
        np.isfinite(open_prices)
        & (open_prices > 0.0)
        & np.isfinite(close_prices)
        & (close_prices > 0.0)
        & np.isfinite(log_returns)
        & (volumes > 0)
    )
    return TaiwanIndexFuturesDaySession(
        dates=dates,
        products=normalized_products,
        contract_months=contract_months,
        open_prices=open_prices,
        high_prices=high_prices,
        low_prices=low_prices,
        close_prices=close_prices,
        volumes=volumes,
        log_returns=log_returns,
        tradable_mask=tradable,
        multipliers=np.asarray(
            [
                TAIFEX_INDEX_FUTURES_MULTIPLIERS[product]
                for product in normalized_products
            ],
            dtype=np.int64,
        ),
    )


__all__ = [
    "SHIOAJI_FUTURES_ROOTS",
    "TAIFEX_DAY_SESSION_LABEL",
    "TAIFEX_FUTURES_DATA_CONTRACT_VERSION",
    "TAIFEX_INDEX_FUTURES_MULTIPLIERS",
    "TAIFEX_INDEX_FUTURES_PRODUCTS",
    "TaiwanIndexFuturesDaySession",
    "build_taifex_index_futures_day_session",
    "load_taifex_index_futures_day_session",
    "normalize_taifex_index_futures_product",
]
