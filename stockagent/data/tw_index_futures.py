"""Official TAIFEX day-session data for Taiwan index-futures strategies.

The source files are the CSV/ZIP artifacts returned by TAIFEX's
``futDataDown`` endpoint.  They contain both the general (day) and after-hours
sessions.  This module deliberately keeps only ``一般`` rows and monthly
contracts.  It preserves every listed monthly contract so a point-in-time
front-month series can roll without treating the old/new contract price gap as
a return.  Weekly MTX rows and calendar spreads are never mixed into the
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
TAIFEX_INDEX_FUTURES_TENOR_SLOTS: Final[int] = 6
TAIFEX_INDEX_FUTURES_MULTIPLIERS: Final[dict[str, int]] = {
    "TX": 200,
    "MTX": 50,
    "TMF": 10,
}
# User/broker fee assumptions shared by live and research one-contract
# comparisons.  Exchange tax is date-versioned separately and must not be
# folded into these fixed per-side amounts.
TAIFEX_INDEX_FUTURES_FEE_PER_SIDE_TWD: Final[dict[str, float]] = {
    "TX": 60.0,
    "MTX": 24.0,
    "TMF": 16.0,
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
TAIFEX_FUTURES_DATA_CONTRACT_VERSION: Final[int] = 2
TAIFEX_ALL_FUTURES_DAILY_CONTRACT_VERSION: Final[int] = 1
_MONTHLY_CONTRACT_RE = re.compile(r"^[0-9]{6}$")
_WEEKLY_CONTRACT_RE = re.compile(r"^[0-9]{6}W[1-5]$")
_DAY_SESSION_ALIASES: Final[frozenset[str]] = frozenset(
    {"一般", "一般交易時段", "day", "day_session", "regular"}
)
_AFTER_HOURS_SESSION_ALIASES: Final[frozenset[str]] = frozenset(
    {"盤後", "盤後交易時段", "after_hours", "night", "night_session"}
)
TAIFEX_DAY_SESSION_ALIASES: Final[frozenset[str]] = _DAY_SESSION_ALIASES
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
    "is_front_month",
    "rolling_buy_hold_log_return",
    "front_month_roll",
    "source_file",
    "source_sha256",
)


@dataclass(frozen=True, slots=True)
class TaiwanIndexFuturesDaySession:
    """Front-month day-session arrays aligned to a requested date axis.

    ``log_returns`` are same-session open-to-close strategy labels.
    ``rolling_buy_hold_log_returns`` are a distinct, fully collateralized 1x
    long benchmark. On a front-month change, the benchmark is assumed to roll
    at the preceding session close and therefore measures the new contract
    from its own preceding close instead of booking the calendar-spread gap.
    """

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
    rolling_buy_hold_log_returns: np.ndarray | None = None
    rolling_buy_hold_tradable_mask: np.ndarray | None = None
    front_month_roll_mask: np.ndarray | None = None
    # All causally listable monthly expiries, ordered E1..E6 independently on
    # every date.  Product is the final axis in canonical TX/MTX/TMF order.
    # The legacy two-dimensional fields above remain the front-month view used
    # by the rolling benchmark and older futures-only experiments.
    tenor_contract_months: np.ndarray | None = None
    tenor_open_prices: np.ndarray | None = None
    tenor_high_prices: np.ndarray | None = None
    tenor_low_prices: np.ndarray | None = None
    tenor_close_prices: np.ndarray | None = None
    tenor_volumes: np.ndarray | None = None
    tenor_log_returns: np.ndarray | None = None
    tenor_tradable_mask: np.ndarray | None = None

    def require_tenor_panel(self) -> tuple[np.ndarray, ...]:
        values = (
            self.tenor_contract_months,
            self.tenor_open_prices,
            self.tenor_high_prices,
            self.tenor_low_prices,
            self.tenor_close_prices,
            self.tenor_volumes,
            self.tenor_log_returns,
            self.tenor_tradable_mask,
        )
        if any(value is None for value in values):
            raise ValueError(
                "TAIFEX futures data does not contain the E1..E6 tenor panel"
            )
        return tuple(np.asarray(value) for value in values)  # type: ignore[arg-type,return-value]

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

    def reference_rolling_buy_hold_log_returns(
        self,
        product: str = "TX",
    ) -> np.ndarray:
        if self.rolling_buy_hold_log_returns is None:
            raise ValueError(
                "TAIFEX data does not contain the rolling buy-and-hold "
                "benchmark; rebuild it with futures data contract v2"
            )
        return self.rolling_buy_hold_log_returns[:, self.product_index(product)]

    def reference_rolling_buy_hold_tradable_mask(
        self,
        product: str = "TX",
    ) -> np.ndarray:
        if self.rolling_buy_hold_tradable_mask is None:
            raise ValueError(
                "TAIFEX data does not contain rolling benchmark validity; "
                "rebuild it with futures data contract v2"
            )
        return self.rolling_buy_hold_tradable_mask[
            :, self.product_index(product)
        ]

    def reference_front_month_roll_mask(
        self,
        product: str = "TX",
    ) -> np.ndarray:
        if self.front_month_roll_mask is None:
            raise ValueError(
                "TAIFEX data does not contain front-month roll events; rebuild "
                "it with futures data contract v2"
            )
        return self.front_month_roll_mask[:, self.product_index(product)]


@dataclass(frozen=True, slots=True)
class _MonthlyContractRow:
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
                        yield (
                            text,
                            f"{source_path.name}:{member.filename}",
                            source_sha256,
                        )
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
) -> Iterator[_MonthlyContractRow]:
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
            raise ValueError(f"{source_name} is missing TAIFEX columns: {missing}")
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
            yield _MonthlyContractRow(
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


def _monthly_day_rows(
    source_paths: Iterable[str | Path],
    *,
    products: tuple[str, ...],
) -> list[_MonthlyContractRow]:
    by_contract: dict[tuple[np.datetime64, str, str], _MonthlyContractRow] = {}
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

    return sorted(
        by_contract.values(),
        key=lambda row: (
            row.date,
            products.index(row.product),
            row.contract_month,
        ),
    )


def _front_month_keys(
    rows: Sequence[_MonthlyContractRow],
) -> set[tuple[np.datetime64, str, str]]:
    grouped: dict[tuple[np.datetime64, str], list[_MonthlyContractRow]] = {}
    for row in rows:
        grouped.setdefault((row.date, row.product), []).append(row)

    selected: set[tuple[np.datetime64, str, str]] = set()
    for key, candidates in grouped.items():
        date_value, product = key
        calendar_month = str(date_value.astype("datetime64[M]")).replace("-", "")
        nonexpired = [row for row in candidates if row.contract_month >= calendar_month]
        pool = nonexpired if nonexpired else candidates
        front = min(pool, key=lambda row: row.contract_month)
        selected.add((date_value, product, front.contract_month))
    return selected


def _rolling_buy_hold_payload(
    rows: Sequence[_MonthlyContractRow],
    front_keys: set[tuple[np.datetime64, str, str]],
) -> dict[tuple[np.datetime64, str, str], tuple[float, bool]]:
    """Return front-row benchmark log returns and roll-event flags.

    The contract that is front month on date ``t`` is assumed to have been
    entered at the preceding futures-session close. Its return is therefore
    ``log(close[t, current] / close[t-1, current])``. This is identical to a
    normal close-to-close return away from a roll and removes the artificial
    old/new-contract level jump on a roll.
    """

    by_contract = {
        (row.date, row.product, row.contract_month): row for row in rows
    }
    front_by_product: dict[str, list[_MonthlyContractRow]] = {}
    for row in rows:
        key = (row.date, row.product, row.contract_month)
        if key in front_keys:
            front_by_product.setdefault(row.product, []).append(row)

    payload: dict[tuple[np.datetime64, str, str], tuple[float, bool]] = {}
    for product_rows in front_by_product.values():
        ordered = sorted(product_rows, key=lambda row: row.date)
        for index, current in enumerate(ordered):
            key = (current.date, current.product, current.contract_month)
            if index == 0:
                payload[key] = (float("nan"), False)
                continue
            previous_front = ordered[index - 1]
            prior_same_contract = by_contract.get(
                (
                    previous_front.date,
                    current.product,
                    current.contract_month,
                )
            )
            rolled = current.contract_month != previous_front.contract_month
            if prior_same_contract is None:
                payload[key] = (float("nan"), rolled)
                continue
            payload[key] = (
                float(math.log(current.close / prior_same_contract.close)),
                rolled,
            )
    return payload


def build_taifex_index_futures_day_session(
    source_paths: Iterable[str | Path],
    output_path: str | Path,
    *,
    products: Sequence[object] | None = None,
) -> Path:
    """Normalize official files into an auditable all-contract parquet."""

    normalized_products = _normalized_products(products)
    rows = _monthly_day_rows(source_paths, products=normalized_products)
    if not rows:
        raise ValueError("no usable monthly TAIFEX day-session rows were found")
    front_keys = _front_month_keys(rows)
    rolling_payload = _rolling_buy_hold_payload(rows, front_keys)

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
            "is_front_month": np.asarray(
                [
                    (row.date, row.product, row.contract_month) in front_keys
                    for row in rows
                ],
                dtype=bool,
            ),
            "rolling_buy_hold_log_return": np.asarray(
                [
                    rolling_payload.get(
                        (row.date, row.product, row.contract_month),
                        (float("nan"), False),
                    )[0]
                    for row in rows
                ],
                dtype=np.float64,
            ),
            "front_month_roll": np.asarray(
                [
                    rolling_payload.get(
                        (row.date, row.product, row.contract_month),
                        (float("nan"), False),
                    )[1]
                    for row in rows
                ],
                dtype=bool,
            ),
            "source_file": [row.source_file for row in rows],
            "source_sha256": [row.source_sha256 for row in rows],
        }
    )
    metadata = dict(table.schema.metadata or {})
    metadata.update(
        {
            b"stockagent.dataset": b"tw_index_futures_day_session_contracts",
            b"stockagent.contract_version": str(
                TAIFEX_FUTURES_DATA_CONTRACT_VERSION
            ).encode("ascii"),
            b"stockagent.session": TAIFEX_DAY_SESSION_LABEL.encode("utf-8"),
            b"stockagent.products": ",".join(normalized_products).encode("ascii"),
            b"stockagent.front_month_policy": b"nearest_unexpired_monthly",
            b"stockagent.rolling_benchmark": b"1x_long_front_month_gross",
            b"stockagent.roll_timing": b"preceding_session_close",
            b"stockagent.roll_gap_treatment": b"same_contract_close_to_close",
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
    """Load and optionally align v2 front-month and rolling benchmark data."""

    import pyarrow.parquet as pq

    normalized_products = _normalized_products(products)
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"TAIFEX day-session parquet does not exist: {source}")
    table = pq.read_table(source)
    missing = sorted(set(_NORMALIZED_COLUMNS) - set(table.column_names))
    if missing:
        raise ValueError(f"{source} is missing normalized columns: {missing}")
    metadata = table.schema.metadata or {}
    contract_version = metadata.get(b"stockagent.contract_version")
    if contract_version is None or int(contract_version) != (
        TAIFEX_FUTURES_DATA_CONTRACT_VERSION
    ):
        raise ValueError(
            f"{source} uses unsupported futures data contract {contract_version!r}"
        )
    expected_metadata = {
        b"stockagent.front_month_policy": b"nearest_unexpired_monthly",
        b"stockagent.rolling_benchmark": b"1x_long_front_month_gross",
        b"stockagent.roll_timing": b"preceding_session_close",
        b"stockagent.roll_gap_treatment": b"same_contract_close_to_close",
    }
    mismatched_metadata = {
        key.decode("ascii"): (metadata.get(key), expected)
        for key, expected in expected_metadata.items()
        if metadata.get(key) != expected
    }
    if mismatched_metadata:
        raise ValueError(
            f"{source} has unsupported rolling benchmark metadata: "
            f"{mismatched_metadata}"
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
            "is_front_month",
            "rolling_buy_hold_log_return",
            "front_month_roll",
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
    rolling_buy_hold_log_returns = np.full(
        (rows, columns), np.nan, dtype=np.float64
    )
    front_month_roll_mask = np.zeros((rows, columns), dtype=bool)
    date_to_row = {date_value: idx for idx, date_value in enumerate(dates)}
    product_to_col = {product: idx for idx, product in enumerate(normalized_products)}
    seen: set[tuple[int, int]] = set()

    for source_row, (date_value, raw_product) in enumerate(
        zip(source_dates, payload["product"], strict=True)
    ):
        product = str(raw_product).strip().upper()
        if (
            product not in product_to_col
            or date_value not in date_to_row
            or not bool(payload["is_front_month"][source_row])
        ):
            continue
        row = date_to_row[date_value]
        col = product_to_col[product]
        key = (row, col)
        if key in seen:
            raise ValueError(
                f"{source} contains duplicate normalized row {date_value}/{product}"
            )
        seen.add(key)
        contract_months[row, col] = str(payload["contract_month"][source_row]).strip()
        open_prices[row, col] = float(payload["open"][source_row])
        high_prices[row, col] = float(payload["high"][source_row])
        low_prices[row, col] = float(payload["low"][source_row])
        close_prices[row, col] = float(payload["close"][source_row])
        volumes[row, col] = int(payload["volume"][source_row])
        log_returns[row, col] = float(payload["log_return"][source_row])
        rolling_buy_hold_log_returns[row, col] = float(
            payload["rolling_buy_hold_log_return"][source_row]
        )
        front_month_roll_mask[row, col] = bool(
            payload["front_month_roll"][source_row]
        )
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
    rolling_buy_hold_tradable = tradable & np.isfinite(
        rolling_buy_hold_log_returns
    )

    tenor_shape = (rows, TAIFEX_INDEX_FUTURES_TENOR_SLOTS, columns)
    tenor_contract_months = np.full(
        (rows, TAIFEX_INDEX_FUTURES_TENOR_SLOTS), "", dtype="U6"
    )
    tenor_open_prices = np.full(tenor_shape, np.nan, dtype=np.float64)
    tenor_high_prices = np.full(tenor_shape, np.nan, dtype=np.float64)
    tenor_low_prices = np.full(tenor_shape, np.nan, dtype=np.float64)
    tenor_close_prices = np.full(tenor_shape, np.nan, dtype=np.float64)
    tenor_volumes = np.zeros(tenor_shape, dtype=np.int64)
    tenor_log_returns = np.full(tenor_shape, np.nan, dtype=np.float64)
    source_rows_by_date: dict[np.datetime64, list[int]] = {}
    for source_row, date_value in enumerate(source_dates):
        raw_product = str(payload["product"][source_row]).strip().upper()
        if raw_product in product_to_col and date_value in date_to_row:
            source_rows_by_date.setdefault(date_value, []).append(source_row)
    for date_value, source_rows_for_date in source_rows_by_date.items():
        row = date_to_row[date_value]
        months = sorted(
            {
                str(payload["contract_month"][source_row]).strip()
                for source_row in source_rows_for_date
                if _MONTHLY_CONTRACT_RE.fullmatch(
                    str(payload["contract_month"][source_row]).strip()
                )
            }
        )
        if len(months) > TAIFEX_INDEX_FUTURES_TENOR_SLOTS:
            raise ValueError(
                f"{source} contains {len(months)} monthly expiries on "
                f"{date_value}, exceeding E1..E{TAIFEX_INDEX_FUTURES_TENOR_SLOTS}"
            )
        month_to_tenor = {month: index for index, month in enumerate(months)}
        tenor_contract_months[row, : len(months)] = months
        seen_tenor_products: set[tuple[int, int]] = set()
        for source_row in source_rows_for_date:
            month = str(payload["contract_month"][source_row]).strip()
            product = str(payload["product"][source_row]).strip().upper()
            if month not in month_to_tenor or product not in product_to_col:
                continue
            tenor = month_to_tenor[month]
            product_col = product_to_col[product]
            key = (tenor, product_col)
            if key in seen_tenor_products:
                raise ValueError(
                    f"{source} contains duplicate tenor row "
                    f"{date_value}/{month}/{product}"
                )
            seen_tenor_products.add(key)
            tenor_open_prices[row, tenor, product_col] = float(
                payload["open"][source_row]
            )
            tenor_high_prices[row, tenor, product_col] = float(
                payload["high"][source_row]
            )
            tenor_low_prices[row, tenor, product_col] = float(
                payload["low"][source_row]
            )
            tenor_close_prices[row, tenor, product_col] = float(
                payload["close"][source_row]
            )
            tenor_volumes[row, tenor, product_col] = int(
                payload["volume"][source_row]
            )
            tenor_log_returns[row, tenor, product_col] = float(
                payload["log_return"][source_row]
            )
    tenor_tradable = (
        np.isfinite(tenor_open_prices)
        & (tenor_open_prices > 0.0)
        & np.isfinite(tenor_close_prices)
        & (tenor_close_prices > 0.0)
        & np.isfinite(tenor_log_returns)
        & (tenor_volumes > 0)
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
        rolling_buy_hold_log_returns=rolling_buy_hold_log_returns,
        rolling_buy_hold_tradable_mask=rolling_buy_hold_tradable,
        front_month_roll_mask=front_month_roll_mask,
        tenor_contract_months=tenor_contract_months,
        tenor_open_prices=tenor_open_prices,
        tenor_high_prices=tenor_high_prices,
        tenor_low_prices=tenor_low_prices,
        tenor_close_prices=tenor_close_prices,
        tenor_volumes=tenor_volumes,
        tenor_log_returns=tenor_log_returns,
        tenor_tradable_mask=tenor_tradable,
    )


def iter_taifex_daily_csv_streams(
    source_path: str | Path,
) -> Iterator[tuple[TextIO, str, str]]:
    """Expose the shared CP950 CSV/ZIP reader for other TAIFEX daily datasets."""

    yield from _decoded_csv_stream(Path(source_path).expanduser().resolve())


def parse_taifex_daily_price(value: object) -> float:
    return _parse_price(value)


def parse_taifex_daily_volume(value: object) -> int:
    return _parse_volume(value)


def parse_taifex_trading_date(value: object) -> np.datetime64 | None:
    return _parse_trading_date(value)


def _parse_optional_finite_number(value: object) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "-", "--"}:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_optional_nonnegative_count(value: object) -> int | None:
    parsed = _parse_optional_finite_number(value)
    if parsed is None or parsed < 0.0 or not float(parsed).is_integer():
        return None
    return int(parsed)


def _taifex_futures_series_type(contract: str) -> str:
    if _MONTHLY_CONTRACT_RE.fullmatch(contract) is not None:
        return "monthly"
    if _WEEKLY_CONTRACT_RE.fullmatch(contract) is not None:
        return "weekly"
    if "/" in contract:
        return "calendar_spread"
    return "other"


def _taifex_futures_session(
    raw_session: object,
    *,
    source_has_session: bool,
    source_name: str,
) -> tuple[str, bool]:
    if not source_has_session:
        return TAIFEX_DAY_SESSION_LABEL, False
    normalized = str(raw_session or "").strip().casefold()
    if normalized in _DAY_SESSION_ALIASES:
        return TAIFEX_DAY_SESSION_LABEL, True
    if normalized in _AFTER_HOURS_SESSION_ALIASES:
        return "盤後", True
    raise ValueError(
        f"{source_name} contains unsupported futures session {raw_session!r}"
    )


def build_taifex_all_futures_daily_sessions(
    source_paths: Iterable[str | Path],
    output_path: str | Path,
    *,
    batch_rows: int = 100_000,
) -> Path:
    """Build all official TAIFEX futures trade bars without Shioaji traffic.

    The output keeps every historical product and every outright monthly or
    weekly contract with a real OHLCV bar.  Calendar-spread order instruments
    are excluded because they are not individual futures contracts and the
    legacy source can contain multiple bars for one spread label.  Day and
    after-hours rows remain separate because the legacy archive has no after-
    hours field and silently combining the two would change the trading-date
    clock.  Pre-2017 rows are explicitly marked ``session_reported=False`` and
    treated as day-session observations, matching the source schema available
    at that time.
    """

    if batch_rows < 1:
        raise ValueError("batch_rows must be positive")

    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            pa.field("date", pa.date32(), nullable=False),
            pa.field("product", pa.string(), nullable=False),
            pa.field("contract", pa.string(), nullable=False),
            pa.field("series_type", pa.string(), nullable=False),
            pa.field("session", pa.string(), nullable=False),
            pa.field("session_reported", pa.bool_(), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("high", pa.float64(), nullable=False),
            pa.field("low", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.int64(), nullable=False),
            pa.field("settlement", pa.float64()),
            pa.field("open_interest", pa.int64()),
            pa.field("last_bid", pa.float64()),
            pa.field("last_ask", pa.float64()),
            pa.field("historical_high", pa.float64()),
            pa.field("historical_low", pa.float64()),
            pa.field("suspension_status", pa.string()),
            pa.field("spread_order_volume", pa.int64()),
            pa.field("source_file", pa.string(), nullable=False),
            pa.field("source_sha256", pa.string(), nullable=False),
        ],
        metadata={
            b"stockagent.dataset": b"taifex_all_futures_daily_sessions",
            b"stockagent.contract_version": str(
                TAIFEX_ALL_FUTURES_DAILY_CONTRACT_VERSION
            ).encode("ascii"),
            b"stockagent.session_policy": b"source_sessions_separate",
            b"stockagent.legacy_session_policy": b"day_only_unreported",
            b"stockagent.instrument_scope": b"outright_no_calendar_spreads",
        },
    )
    target = Path(output_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    if temporary.exists():
        temporary.unlink()

    writer: pq.ParquetWriter | None = None
    pending: list[dict[str, object]] = []
    seen_keys: set[tuple[np.datetime64, str, str, str]] = set()
    rows_written = 0

    def flush() -> None:
        nonlocal pending, rows_written, writer
        if not pending:
            return
        if writer is None:
            writer = pq.ParquetWriter(
                temporary,
                schema,
                compression="zstd",
                use_dictionary=True,
            )
        table = pa.Table.from_pylist(pending, schema=schema)
        writer.write_table(table)
        rows_written += len(pending)
        pending = []

    try:
        for raw_path in source_paths:
            path = Path(raw_path).expanduser().resolve()
            if not path.is_file():
                raise FileNotFoundError(f"TAIFEX source does not exist: {path}")
            for stream, source_name, source_sha256 in _decoded_csv_stream(path):
                reader = csv.DictReader(stream)
                if reader.fieldnames is None:
                    raise ValueError(f"{source_name} has no CSV header")
                reader.fieldnames = [
                    str(name or "").lstrip("\ufeff").strip()
                    for name in reader.fieldnames
                ]
                missing = sorted(
                    set(_REQUIRED_SOURCE_COLUMNS) - set(reader.fieldnames)
                )
                if missing:
                    raise ValueError(
                        f"{source_name} is missing TAIFEX columns: {missing}"
                    )
                has_session = "交易時段" in reader.fieldnames
                for raw in reader:
                    product = str(raw.get("契約") or "").strip().upper()
                    contract = str(raw.get("到期月份(週別)") or "").strip()
                    if not product or not contract:
                        continue
                    date_value = _parse_trading_date(raw.get("交易日期"))
                    if date_value is None:
                        continue
                    session, session_reported = _taifex_futures_session(
                        raw.get("交易時段"),
                        source_has_session=has_session,
                        source_name=source_name,
                    )
                    series_type = _taifex_futures_series_type(contract)
                    if series_type == "calendar_spread":
                        continue
                    prices = tuple(
                        _parse_optional_finite_number(raw.get(column))
                        for column in ("開盤價", "最高價", "最低價", "收盤價")
                    )
                    volume = _parse_optional_nonnegative_count(raw.get("成交量"))
                    if any(value is None for value in prices) or not volume:
                        continue
                    open_price, high_price, low_price, close_price = (
                        float(value) for value in prices if value is not None
                    )
                    if not all(
                        value > 0.0
                        for value in (open_price, high_price, low_price, close_price)
                    ):
                        continue
                    if high_price < max(open_price, close_price, low_price):
                        raise ValueError(
                            f"{source_name} has invalid high for "
                            f"{date_value}/{product}/{contract}/{session}"
                        )
                    if low_price > min(open_price, close_price, high_price):
                        raise ValueError(
                            f"{source_name} has invalid low for "
                            f"{date_value}/{product}/{contract}/{session}"
                        )
                    key = (date_value, product, contract, session)
                    if key in seen_keys:
                        raise ValueError(
                            "duplicate TAIFEX futures daily bar for "
                            f"{date_value}/{product}/{contract}/{session}"
                        )
                    seen_keys.add(key)
                    suspension = str(
                        raw.get("是否因訊息面暫停交易") or ""
                    ).strip()
                    pending.append(
                        {
                            "date": date.fromisoformat(str(date_value)),
                            "product": product,
                            "contract": contract,
                            "series_type": series_type,
                            "session": session,
                            "session_reported": session_reported,
                            "open": open_price,
                            "high": high_price,
                            "low": low_price,
                            "close": close_price,
                            "volume": int(volume),
                            "settlement": _parse_optional_finite_number(
                                raw.get("結算價")
                            ),
                            "open_interest": _parse_optional_nonnegative_count(
                                raw.get("未沖銷契約數")
                            ),
                            "last_bid": _parse_optional_finite_number(
                                raw.get("最後最佳買價")
                            ),
                            "last_ask": _parse_optional_finite_number(
                                raw.get("最後最佳賣價")
                            ),
                            "historical_high": _parse_optional_finite_number(
                                raw.get("歷史最高價")
                            ),
                            "historical_low": _parse_optional_finite_number(
                                raw.get("歷史最低價")
                            ),
                            "suspension_status": suspension or None,
                            "spread_order_volume": (
                                _parse_optional_nonnegative_count(
                                    raw.get("價差對單式委託成交量")
                                )
                            ),
                            "source_file": source_name,
                            "source_sha256": source_sha256,
                        }
                    )
                    if len(pending) >= batch_rows:
                        flush()
        flush()
        if writer is None or rows_written == 0:
            raise ValueError("no usable TAIFEX futures daily bars were found")
        writer.close()
        writer = None

        temporary.replace(target)
        return target
    except Exception:
        if writer is not None:
            writer.close()
        if temporary.exists():
            temporary.unlink()
        raise


__all__ = [
    "SHIOAJI_FUTURES_ROOTS",
    "TAIFEX_ALL_FUTURES_DAILY_CONTRACT_VERSION",
    "TAIFEX_DAY_SESSION_LABEL",
    "TAIFEX_DAY_SESSION_ALIASES",
    "TAIFEX_FUTURES_DATA_CONTRACT_VERSION",
    "TAIFEX_INDEX_FUTURES_MULTIPLIERS",
    "TAIFEX_INDEX_FUTURES_PRODUCTS",
    "TAIFEX_INDEX_FUTURES_TENOR_SLOTS",
    "TaiwanIndexFuturesDaySession",
    "build_taifex_all_futures_daily_sessions",
    "build_taifex_index_futures_day_session",
    "iter_taifex_daily_csv_streams",
    "load_taifex_index_futures_day_session",
    "normalize_taifex_index_futures_product",
    "parse_taifex_daily_price",
    "parse_taifex_daily_volume",
    "parse_taifex_trading_date",
]
