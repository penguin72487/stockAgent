"""Causal nearby single-stock-futures day-trade execution data.

The policy still observes the complete Taiwan stock panel.  This module adds
only executor labels aligned to that stock order: a stock without a causally
known front-month futures contract is masked out before portfolio allocation,
and a known contract without the execution prices required by its declared
clock cannot execute.  The default 09:00 research baseline uses the immutable
daily session OPEN-to-CLOSE label as an explicit counterfactual proxy.  Because
that OPEN is stamped 08:45, the proxy is not an executable 09:00 fill claim.
The separately receipted first strictly-later public trade through 09:00:59
remains available as an opt-in entry source.

TAIFEX occasionally has two product roots for the same underlying during a
code transition.  Selection is therefore one-to-one by ``date + underlying``:
nearest tenor first, then prior-session volume/open interest, then stable
product/physical-contract codes.  Current-session volume is deliberately not
used to choose the contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path
from typing import Any, Final

import numpy as np

from stockagent.data.panel import PanelData
from stockagent.data.tw_futures_portfolio_daily import (
    TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
)
from stockagent.research.taifex_transaction_tax import (
    stock_index_futures_tax_rate,
)

try:
    import polars as pl
except Exception:  # pragma: no cover - checked at the public entry point
    pl = None

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - checked at the public entry point
    pq = None


TAIFEX_STOCK_FUTURES_DAY_TRADE_DATA_CONTRACT_VERSION: Final[int] = 1
TAIFEX_STOCK_FUTURES_DAY_TRADE_BACKTEST_CONTRACT_VERSION: Final[int] = 1
TAIFEX_STOCK_FUTURES_0900_ENTRY_DATA_CONTRACT_VERSION: Final[int] = 1
TAIFEX_STOCK_FUTURES_DAY_TRADE_0900_BACKTEST_CONTRACT_VERSION: Final[int] = 2
TAIFEX_STOCK_FUTURES_INTEGER_DATA_CONTRACT_VERSION: Final[int] = 2
TAIFEX_STOCK_FUTURES_INTEGER_BACKTEST_CONTRACT_VERSION: Final[int] = 3
STOCK_FUTURES_INTEGER_CANDIDATE_MULTIPLIERS: Final[tuple[float, float]] = (
    2_000.0,
    100.0,
)
STOCK_FUTURES_INTEGER_EXECUTION_CHANNELS: Final[tuple[str, ...]] = (
    "long_net_simple_return",
    "short_net_simple_return",
    "open_notional_twd",
    "reserved_open_cash_twd",
    "maximum_contracts",
)
ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN: Final[str] = "daily_session_open"
ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN_0900_PROXY: Final[str] = (
    "daily_session_open_proxy"
)
ENTRY_PRICE_SOURCE_POST_0900_TRADE_SIDECAR: Final[str] = (
    "post_0900_trade_sidecar"
)
STOCK_FUTURES_DAY_TRADE_ENTRY_PRICE_SOURCES: Final[frozenset[str]] = frozenset(
    {
        ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN,
        ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN_0900_PROXY,
        ENTRY_PRICE_SOURCE_POST_0900_TRADE_SIDECAR,
    }
)
DEFAULT_DATA_PATH: Final[str] = (
    "data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet"
)
DEFAULT_0900_ENTRY_DATA_PATH: Final[str] = (
    "data_tw_futures/taifex_stock_futures_0900_v1/entry_0900.parquet"
)

_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "date",
    "product",
    "physical_contract",
    "underlying_symbol",
    "asset_class",
    "tenor_rank",
    "tenor_sort_date",
    "open",
    "close",
    "volume",
    "previous_volume",
    "previous_open_interest",
    "source_row_observed",
    "same_contract_as_previous_session",
    "executable",
    "contract_multiplier",
    "fixed_fee_research_supported",
)

_REQUIRED_0900_ENTRY_COLUMNS: Final[tuple[str, ...]] = (
    "date",
    "physical_contract",
    "entry_time_hhmmss",
    "entry_price",
    "matched_quantity",
    "source_row_observed",
    "source_file_sha256",
)


@dataclass(frozen=True, slots=True)
class TaiwanStockFuturesDayTradeDaily:
    """Dense executor-only arrays aligned to the full stock feature panel."""

    dates: np.ndarray
    symbols: tuple[str, ...]
    intraday_log_returns: np.ndarray
    policy_eligible_mask: np.ndarray
    executable_mask: np.ndarray
    round_trip_cost_rate_per_open_notional: np.ndarray
    prior_volume_notional: np.ndarray
    benchmark_log_returns: np.ndarray
    selected_rows: int
    selected_underlyings: int
    source_path: str
    manifest_path: str
    entry_clock: str = "taifex_day_session_open_0845"
    entry_source_path: str | None = None
    entry_manifest_path: str | None = None
    integer_candidate_execution: np.ndarray | None = None
    integer_candidate_multipliers: tuple[float, ...] = ()
    integer_candidate_selection: str | None = None
    contract_version: int = TAIFEX_STOCK_FUTURES_DAY_TRADE_DATA_CONTRACT_VERSION


def _require_dependencies() -> None:
    if pl is None or pq is None:
        raise RuntimeError(
            "tw_stock_futures_day_trade requires polars and pyarrow"
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def select_causal_front_stock_futures(frame: Any) -> Any:
    """Return one causally selected nearby contract per date/underlying.

    ``frame`` may contain both observed and carry-forward valuation rows from
    the immutable TAIFEX portfolio table.  A candidate must have existed on
    the preceding exchange session.  Current OPEN/CLOSE/volume decide only
    whether that already-selected contract can execute, never which contract
    is selected.
    """

    _require_dependencies()
    if isinstance(frame, pl.LazyFrame):
        frame = frame.collect()
    if not isinstance(frame, pl.DataFrame):
        raise TypeError("frame must be a Polars DataFrame or LazyFrame")
    missing = sorted(set(_REQUIRED_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(
            "TAIFEX stock-futures source is missing required columns: "
            + ", ".join(missing)
        )

    candidates = (
        frame.filter(
            (pl.col("asset_class") == "stock_future")
            & pl.col("fixed_fee_research_supported").fill_null(False)
            & (pl.col("tenor_rank") == 1)
            & pl.col("same_contract_as_previous_session").fill_null(False)
            & pl.col("underlying_symbol").is_not_null()
        )
        .with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("underlying_symbol").cast(pl.String),
            pl.col("previous_volume").fill_null(0).clip(lower_bound=0),
            pl.col("previous_open_interest").fill_null(0).clip(lower_bound=0),
        )
        .sort(
            [
                "date",
                "underlying_symbol",
                "tenor_sort_date",
                "previous_volume",
                "previous_open_interest",
                "product",
                "physical_contract",
            ],
            descending=[False, False, False, True, True, False, False],
            nulls_last=True,
        )
        .unique(subset=["date", "underlying_symbol"], keep="first", maintain_order=True)
        .sort("date", "underlying_symbol")
    )
    duplicates = (
        candidates.group_by("date", "underlying_symbol")
        .len()
        .filter(pl.col("len") != 1)
    )
    if duplicates.height:
        raise RuntimeError(
            "causal front-stock-futures selection is not one-to-one"
        )
    return candidates


def select_causal_front_stock_futures_candidates(frame: Any) -> Any:
    """Return one causal nearby standard and mini candidate per underlying.

    Product-code transitions can temporarily produce duplicate roots with the
    same multiplier.  They are resolved using only preceding-session liquidity
    and stable identifiers.  The two multiplier tiers remain separate so the
    executor can combine their integer quantities instead of prematurely
    collapsing the underlying to one physical contract.
    """

    _require_dependencies()
    if isinstance(frame, pl.LazyFrame):
        frame = frame.collect()
    if not isinstance(frame, pl.DataFrame):
        raise TypeError("frame must be a Polars DataFrame or LazyFrame")
    missing = sorted(set(_REQUIRED_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(
            "TAIFEX stock-futures source is missing required columns: "
            + ", ".join(missing)
        )

    candidates = frame.filter(
        (pl.col("asset_class") == "stock_future")
        & pl.col("fixed_fee_research_supported").fill_null(False)
        & (pl.col("tenor_rank") == 1)
        & pl.col("same_contract_as_previous_session").fill_null(False)
        & pl.col("underlying_symbol").is_not_null()
    ).with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("underlying_symbol").cast(pl.String),
        pl.col("contract_multiplier").cast(pl.Float64),
        pl.col("previous_volume").fill_null(0).clip(lower_bound=0),
        pl.col("previous_open_interest").fill_null(0).clip(lower_bound=0),
    )
    observed_multipliers = set(
        float(value)
        for value in candidates["contract_multiplier"].drop_nulls().unique().to_list()
    )
    supported = set(STOCK_FUTURES_INTEGER_CANDIDATE_MULTIPLIERS)
    unsupported = sorted(observed_multipliers - supported)
    if unsupported:
        raise ValueError(
            "integer stock-futures candidate pool encountered unsupported "
            "contract multipliers: " + ", ".join(f"{value:g}" for value in unsupported)
        )
    candidates = (
        candidates.filter(pl.col("contract_multiplier").is_in(sorted(supported)))
        .sort(
            [
                "date",
                "underlying_symbol",
                "contract_multiplier",
                "tenor_sort_date",
                "previous_volume",
                "previous_open_interest",
                "product",
                "physical_contract",
            ],
            descending=[False, False, True, False, True, True, False, False],
            nulls_last=True,
        )
        .unique(
            subset=["date", "underlying_symbol", "contract_multiplier"],
            keep="first",
            maintain_order=True,
        )
        .with_columns(
            pl.when(pl.col("contract_multiplier") == 2_000.0)
            .then(pl.lit(0, dtype=pl.Int8))
            .otherwise(pl.lit(1, dtype=pl.Int8))
            .alias("candidate_slot")
        )
        .sort("date", "underlying_symbol", "candidate_slot")
    )
    duplicates = candidates.group_by(
        "date", "underlying_symbol", "candidate_slot"
    ).len().filter(pl.col("len") != 1)
    if duplicates.height:
        raise RuntimeError(
            "causal standard/mini stock-futures selection is not one-to-one"
        )
    return candidates


def _with_execution_costs(
    selected: Any,
    *,
    fee_per_contract_per_side_twd: float,
) -> Any:
    fee = float(fee_per_contract_per_side_twd)
    if not np.isfinite(fee) or fee < 0.0:
        raise ValueError(
            "fee_per_contract_per_side_twd must be finite and non-negative"
        )
    unique_dates = selected.select("date").unique().sort("date")
    tax_schedule = unique_dates.with_columns(
        pl.Series(
            "transaction_tax_rate",
            [
                stock_index_futures_tax_rate(value)
                for value in unique_dates["date"].to_list()
            ],
            dtype=pl.Float64,
        )
    )
    selected = selected.join(tax_schedule, on="date", how="left", validate="m:1")
    executable = (
        pl.col("source_row_observed").fill_null(False)
        & pl.col("executable").fill_null(False)
        & pl.col("open").is_finite()
        & (pl.col("open") > 0.0)
        & pl.col("close").is_finite()
        & (pl.col("close") > 0.0)
        & (pl.col("volume").fill_null(0) > 0)
        & pl.col("contract_multiplier").is_finite()
        & (pl.col("contract_multiplier") > 0.0)
    )
    open_notional = pl.col("open") * pl.col("contract_multiplier")
    entry_tax = (open_notional * pl.col("transaction_tax_rate") + 0.5).floor()
    exit_tax = (
        pl.col("close")
        * pl.col("contract_multiplier")
        * pl.col("transaction_tax_rate")
        + 0.5
    ).floor()
    return selected.with_columns(
        pl.lit(True).alias("policy_eligible"),
        executable.alias("round_trip_executable"),
        pl.when(executable)
        .then((pl.col("close") / pl.col("open")).log())
        .otherwise(None)
        .alias("intraday_log_return"),
        pl.when(executable)
        .then((2.0 * fee + entry_tax + exit_tax) / open_notional)
        .otherwise(None)
        .alias("round_trip_cost_rate_per_open_notional"),
        pl.when(
            pl.col("open").is_finite()
            & (pl.col("open") > 0.0)
            & pl.col("contract_multiplier").is_finite()
            & (pl.col("contract_multiplier") > 0.0)
        )
        .then(pl.col("previous_volume").cast(pl.Float64) * open_notional)
        .otherwise(0.0)
        .alias("prior_volume_notional"),
    )


def _with_integer_execution_contract(
    selected: Any,
    *,
    fee_per_contract_per_side_twd: float,
    max_volume_participation: float,
) -> Any:
    """Build exact per-candidate PnL, collateral, and capacity channels.

    Sizing is causal at the open: the reserve uses opening notional, two fixed
    commissions, and two opening-price tax estimates.  The close ledger later
    replaces the estimated exit tax with the tax rounded from the actual close.
    Full notional is collateral only; it is returned when the same-session
    position closes, while actual PnL, fees, and tax change account equity.
    """

    fee = float(fee_per_contract_per_side_twd)
    participation = float(max_volume_participation)
    if not np.isfinite(fee) or fee < 0.0:
        raise ValueError(
            "fee_per_contract_per_side_twd must be finite and non-negative"
        )
    if not np.isfinite(participation) or not (0.0 < participation <= 1.0):
        raise ValueError("max_volume_participation must be in (0,1]")
    unique_dates = selected.select("date").unique().sort("date")
    tax_schedule = unique_dates.with_columns(
        pl.Series(
            "transaction_tax_rate",
            [
                stock_index_futures_tax_rate(value)
                for value in unique_dates["date"].to_list()
            ],
            dtype=pl.Float64,
        )
    )
    selected = selected.join(tax_schedule, on="date", how="left", validate="m:1")
    executable = (
        pl.col("source_row_observed").fill_null(False)
        & pl.col("executable").fill_null(False)
        & pl.col("open").is_finite()
        & (pl.col("open") > 0.0)
        & pl.col("close").is_finite()
        & (pl.col("close") > 0.0)
        & (pl.col("volume").fill_null(0) > 0)
        & pl.col("contract_multiplier").is_finite()
        & (pl.col("contract_multiplier") > 0.0)
    )
    open_notional = pl.col("open") * pl.col("contract_multiplier")
    gross_long = (
        (pl.col("close") - pl.col("open")) * pl.col("contract_multiplier")
    )
    entry_tax = (open_notional * pl.col("transaction_tax_rate") + 0.5).floor()
    exit_tax = (
        pl.col("close")
        * pl.col("contract_multiplier")
        * pl.col("transaction_tax_rate")
        + 0.5
    ).floor()
    actual_round_trip_cost = 2.0 * fee + entry_tax + exit_tax
    causal_reserved_cost = 2.0 * fee + 2.0 * entry_tax
    maximum_contracts = (
        pl.col("previous_volume").cast(pl.Float64) * participation
    ).floor().clip(lower_bound=0.0)
    return selected.with_columns(
        pl.lit(True).alias("policy_eligible"),
        executable.alias("round_trip_executable"),
        pl.when(executable)
        .then((pl.col("close") / pl.col("open")).log())
        .otherwise(None)
        .alias("intraday_log_return"),
        pl.when(executable)
        .then(actual_round_trip_cost / open_notional)
        .otherwise(None)
        .alias("round_trip_cost_rate_per_open_notional"),
        pl.when(executable)
        .then((gross_long - actual_round_trip_cost) / open_notional)
        .otherwise(None)
        .alias("long_net_simple_return"),
        pl.when(executable)
        .then((-gross_long - actual_round_trip_cost) / open_notional)
        .otherwise(None)
        .alias("short_net_simple_return"),
        pl.when(executable)
        .then(open_notional)
        .otherwise(None)
        .alias("open_notional_twd"),
        pl.when(executable)
        .then(open_notional + causal_reserved_cost)
        .otherwise(None)
        .alias("reserved_open_cash_twd"),
        pl.when(executable)
        .then(maximum_contracts)
        .otherwise(None)
        .alias("maximum_contracts"),
        pl.when(
            pl.col("open").is_finite()
            & (pl.col("open") > 0.0)
            & pl.col("contract_multiplier").is_finite()
            & (pl.col("contract_multiplier") > 0.0)
        )
        .then(pl.col("previous_volume").cast(pl.Float64) * open_notional)
        .otherwise(0.0)
        .alias("prior_volume_notional"),
    )


def _validate_sha256_text(value: object) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _load_0900_entries(
    data_path: Path,
    *,
    panel_dates: np.ndarray,
) -> tuple[Any, Path]:
    """Load an immutable 09:00 entry sidecar and verify its full date coverage."""

    manifest_path = data_path.parent / "manifest.json"
    if not data_path.is_file() or not manifest_path.is_file():
        raise FileNotFoundError(
            "09:00 stock-futures entry source/manifest missing; refusing to "
            f"substitute the 08:45 daily OPEN: {data_path}, {manifest_path}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("dataset") != "taifex_stock_futures_0900_entry_v1":
        raise ValueError("09:00 stock-futures entry dataset name mismatch")
    if int(manifest.get("contract_version", -1)) != int(
        TAIFEX_STOCK_FUTURES_0900_ENTRY_DATA_CONTRACT_VERSION
    ):
        raise ValueError("09:00 stock-futures entry contract version mismatch")
    if manifest.get("status") != "complete":
        raise ValueError("09:00 stock-futures entry manifest is not complete")
    if manifest.get("timezone") != "Asia/Taipei":
        raise ValueError("09:00 stock-futures entry timezone must be Asia/Taipei")
    if manifest.get("decision_time") != "09:00:00":
        raise ValueError("09:00 stock-futures decision_time contract mismatch")
    if manifest.get("entry_rule") != (
        "first_strictly_later_public_trade_row_through_09:00:59"
    ):
        raise ValueError("09:00 stock-futures entry rule contract mismatch")
    output = manifest.get("outputs", {}).get("entry_0900", {})
    if output.get("sha256") != _sha256_file(data_path):
        raise ValueError("09:00 stock-futures entry SHA-256 differs from manifest")

    coverage = manifest.get("coverage", {})
    if coverage.get("missing_trading_dates") not in ([], ()):
        raise ValueError("09:00 stock-futures source has missing trading dates")
    try:
        coverage_start = np.datetime64(str(coverage["start"]), "D")
        coverage_end = np.datetime64(str(coverage["end"]), "D")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("09:00 stock-futures coverage bounds are invalid") from exc
    if coverage_start > panel_dates[0] or coverage_end < panel_dates[-1]:
        raise ValueError(
            "09:00 stock-futures entry coverage does not span the complete panel: "
            f"coverage={coverage_start}..{coverage_end}, "
            f"panel={panel_dates[0]}..{panel_dates[-1]}"
        )

    table = pq.read_table(
        data_path,
        columns=list(_REQUIRED_0900_ENTRY_COLUMNS),
        filters=[
            ("date", ">=", date.fromisoformat(str(panel_dates[0]))),
            ("date", "<=", date.fromisoformat(str(panel_dates[-1]))),
        ],
        memory_map=True,
    )
    entries = pl.from_arrow(table).with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("physical_contract").cast(pl.String),
        pl.col("entry_time_hhmmss").cast(pl.Int32),
        pl.col("entry_price").cast(pl.Float64),
        pl.col("matched_quantity").cast(pl.Float64),
        pl.col("source_row_observed").cast(pl.Boolean),
        pl.col("source_file_sha256").cast(pl.String).str.to_lowercase(),
    ).rename({"source_row_observed": "entry_source_row_observed"})
    duplicate = (
        entries.group_by("date", "physical_contract")
        .len()
        .filter(pl.col("len") != 1)
    )
    if duplicate.height:
        raise ValueError(
            "09:00 entry sidecar must contain at most one row per "
            "date/physical_contract"
        )
    invalid = entries.filter(
        (~pl.col("entry_source_row_observed").fill_null(False))
        | (~pl.col("entry_price").is_finite())
        | (pl.col("entry_price") <= 0.0)
        | (~pl.col("matched_quantity").is_finite())
        | (pl.col("matched_quantity") <= 0.0)
        | (pl.col("entry_time_hhmmss") <= 90000)
        | (pl.col("entry_time_hhmmss") > 90059)
    )
    if invalid.height:
        raise ValueError(
            "09:00 entry sidecar contains non-observed, non-positive, or "
            "non-causal execution rows"
        )
    invalid_hashes = [
        value
        for value in entries["source_file_sha256"].to_list()
        if not _validate_sha256_text(value)
    ]
    if invalid_hashes:
        raise ValueError("09:00 entry sidecar contains invalid source SHA-256")
    return entries, manifest_path


def _with_0900_execution_costs(
    selected: Any,
    entries: Any,
    *,
    fee_per_contract_per_side_twd: float,
) -> Any:
    """Use only a post-decision 09:00 trade as the futures entry price."""

    fee = float(fee_per_contract_per_side_twd)
    if not np.isfinite(fee) or fee < 0.0:
        raise ValueError(
            "fee_per_contract_per_side_twd must be finite and non-negative"
        )
    selected = selected.join(
        entries,
        on=["date", "physical_contract"],
        how="left",
        validate="m:1",
    )
    unique_dates = selected.select("date").unique().sort("date")
    tax_schedule = unique_dates.with_columns(
        pl.Series(
            "transaction_tax_rate",
            [
                stock_index_futures_tax_rate(value)
                for value in unique_dates["date"].to_list()
            ],
            dtype=pl.Float64,
        )
    )
    selected = selected.join(tax_schedule, on="date", how="left", validate="m:1")
    executable = (
        pl.col("source_row_observed").fill_null(False)
        & pl.col("entry_source_row_observed").fill_null(False)
        & pl.col("entry_price").is_finite()
        & (pl.col("entry_price") > 0.0)
        & (pl.col("entry_time_hhmmss") > 90000)
        & (pl.col("entry_time_hhmmss") <= 90059)
        & pl.col("close").is_finite()
        & (pl.col("close") > 0.0)
        & (pl.col("volume").fill_null(0) > 0)
        & (pl.col("matched_quantity").fill_null(0.0) > 0.0)
        & pl.col("contract_multiplier").is_finite()
        & (pl.col("contract_multiplier") > 0.0)
    )
    entry_notional = pl.col("entry_price") * pl.col("contract_multiplier")
    entry_tax = (entry_notional * pl.col("transaction_tax_rate") + 0.5).floor()
    exit_tax = (
        pl.col("close")
        * pl.col("contract_multiplier")
        * pl.col("transaction_tax_rate")
        + 0.5
    ).floor()
    return selected.with_columns(
        pl.lit(True).alias("policy_eligible"),
        executable.alias("round_trip_executable"),
        pl.when(executable)
        .then((pl.col("close") / pl.col("entry_price")).log())
        .otherwise(None)
        .alias("intraday_log_return"),
        pl.when(executable)
        .then((2.0 * fee + entry_tax + exit_tax) / entry_notional)
        .otherwise(None)
        .alias("round_trip_cost_rate_per_open_notional"),
        pl.when(
            pl.col("entry_price").is_finite()
            & (pl.col("entry_price") > 0.0)
            & pl.col("contract_multiplier").is_finite()
            & (pl.col("contract_multiplier") > 0.0)
        )
        .then(pl.col("previous_volume").cast(pl.Float64) * entry_notional)
        .otherwise(0.0)
        .alias("prior_volume_notional"),
    )


def attach_stock_futures_day_trade_daily(
    panel: PanelData,
    data_path: str | Path = DEFAULT_DATA_PATH,
    *,
    fee_per_contract_per_side_twd: float,
    entry_price_source: str = ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN,
    entry_0900_data_path: str | Path | None = None,
    integer_contracts: bool = False,
    max_volume_participation: float = 0.5,
) -> PanelData:
    """Attach nearby-futures labels without changing model input symbol axes."""

    _require_dependencies()
    data_path = Path(data_path)
    manifest_path = data_path.parent / "manifest.json"
    if not data_path.is_file() or not manifest_path.is_file():
        raise FileNotFoundError(
            "TAIFEX stock-futures source/manifest missing: "
            f"{data_path}, {manifest_path}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if int(manifest.get("contract_version", -1)) != int(
        TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
    ):
        raise ValueError("TAIFEX futures source contract version mismatch")
    expected_hash = (
        manifest.get("outputs", {}).get("continuous_daily", {}).get("sha256")
    )
    if not isinstance(expected_hash, str) or expected_hash != _sha256_file(data_path):
        raise ValueError("TAIFEX futures source SHA-256 differs from manifest")

    dates = np.asarray(panel.dates, dtype="datetime64[D]")
    symbols = tuple(str(symbol) for symbol in panel.symbols)
    if dates.size == 0 or not symbols:
        raise ValueError("cannot attach stock futures to an empty stock panel")
    if np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1]):
        raise ValueError("stock panel dates must be finite and strictly increasing")
    if len(set(symbols)) != len(symbols):
        raise ValueError("stock panel symbols must be unique")

    table = pq.read_table(
        data_path,
        columns=list(_REQUIRED_COLUMNS),
        filters=[
            ("date", ">=", date.fromisoformat(str(dates[0]))),
            ("date", "<=", date.fromisoformat(str(dates[-1]))),
            ("asset_class", "=", "stock_future"),
        ],
        memory_map=True,
    )
    source = pl.from_arrow(table)
    selected = (
        select_causal_front_stock_futures_candidates(source)
        if integer_contracts
        else select_causal_front_stock_futures(source)
    )
    normalized_entry_source = str(entry_price_source).strip().lower()
    if normalized_entry_source not in STOCK_FUTURES_DAY_TRADE_ENTRY_PRICE_SOURCES:
        raise ValueError(
            "unsupported stock-futures day-trade entry_price_source: "
            f"{entry_price_source!r}"
        )
    uses_post_0900_sidecar = (
        normalized_entry_source
        == ENTRY_PRICE_SOURCE_POST_0900_TRADE_SIDECAR
    )
    if integer_contracts and uses_post_0900_sidecar:
        raise ValueError(
            "integer standard/mini candidate execution currently requires "
            "entry_price_source='daily_session_open_proxy'; the legacy 09:00 "
            "sidecar does not contain both candidate tiers"
        )
    if uses_post_0900_sidecar and entry_0900_data_path is None:
        raise ValueError(
            "post_0900_trade_sidecar requires entry_0900_data_path"
        )
    if not uses_post_0900_sidecar and entry_0900_data_path is not None:
        raise ValueError(
            "entry_0900_data_path is valid only with "
            "entry_price_source='post_0900_trade_sidecar'"
        )

    entry_manifest_path: Path | None = None
    if integer_contracts:
        selected = _with_integer_execution_contract(
            selected,
            fee_per_contract_per_side_twd=fee_per_contract_per_side_twd,
            max_volume_participation=max_volume_participation,
        )
    elif not uses_post_0900_sidecar:
        selected = _with_execution_costs(
            selected,
            fee_per_contract_per_side_twd=fee_per_contract_per_side_twd,
        )
    else:
        entry_path = Path(entry_0900_data_path)
        entries, entry_manifest_path = _load_0900_entries(
            entry_path,
            panel_dates=dates,
        )
        selected = _with_0900_execution_costs(
            selected,
            entries,
            fee_per_contract_per_side_twd=fee_per_contract_per_side_twd,
        )

    date_map = pl.DataFrame(
        {
            "date": dates,
            "date_index": np.arange(dates.size, dtype=np.int32),
        }
    ).with_columns(pl.col("date").cast(pl.Date))
    symbol_map = pl.DataFrame(
        {
            "underlying_symbol": symbols,
            "symbol_index": np.arange(len(symbols), dtype=np.int32),
        }
    )
    aligned = (
        selected.join(date_map, on="date", how="inner", validate="m:1")
        .join(symbol_map, on="underlying_symbol", how="inner", validate="m:1")
        .sort("date_index", "symbol_index")
    )
    duplicate_columns = ["date_index", "symbol_index"]
    if integer_contracts:
        duplicate_columns.append("candidate_slot")
    duplicate = (
        aligned.group_by(*duplicate_columns)
        .len()
        .filter(pl.col("len") != 1)
    )
    if duplicate.height:
        raise RuntimeError("aligned stock-futures candidate rows are not one-to-one")

    shape = panel.tradable_mask.shape
    if shape != (dates.size, len(symbols)):
        raise ValueError("stock panel arrays and ordered universe disagree")
    returns = np.full(shape, np.nan, dtype=np.float32)
    costs = np.full(shape, np.nan, dtype=np.float32)
    policy = np.zeros(shape, dtype=bool)
    executable = np.zeros(shape, dtype=bool)
    prior_volume_notional = np.zeros(shape, dtype=np.float32)
    integer_candidate_execution: np.ndarray | None = None
    if integer_contracts:
        integer_candidate_execution = np.full(
            (
                dates.size,
                len(symbols),
                len(STOCK_FUTURES_INTEGER_CANDIDATE_MULTIPLIERS),
                len(STOCK_FUTURES_INTEGER_EXECUTION_CHANNELS),
            ),
            np.nan,
            dtype=np.float32,
        )
    if aligned.height:
        di = aligned["date_index"].to_numpy().astype(np.int64, copy=False)
        si = aligned["symbol_index"].to_numpy().astype(np.int64, copy=False)
        policy[di, si] = True
        executable_values = aligned["round_trip_executable"].to_numpy().astype(
            bool, copy=False
        )
        if not integer_contracts:
            executable[di, si] = executable_values
            returns[di, si] = aligned["intraday_log_return"].to_numpy().astype(
                np.float32, copy=False
            )
            costs[di, si] = aligned[
                "round_trip_cost_rate_per_open_notional"
            ].to_numpy().astype(np.float32, copy=False)
            prior_volume_notional[di, si] = aligned[
                "prior_volume_notional"
            ].to_numpy().astype(np.float32, copy=False)
        else:
            assert integer_candidate_execution is not None
            slots = aligned["candidate_slot"].to_numpy().astype(np.int64, copy=False)
            candidate_values = np.column_stack(
                [
                    aligned[name].to_numpy()
                    for name in STOCK_FUTURES_INTEGER_EXECUTION_CHANNELS
                ]
            ).astype(np.float32, copy=False)
            integer_candidate_execution[di, si, slots, :] = candidate_values
            np.logical_or.at(executable, (di, si), executable_values)
            candidate_returns = aligned["intraday_log_return"].to_numpy().astype(
                np.float64, copy=False
            )
            candidate_costs = aligned[
                "round_trip_cost_rate_per_open_notional"
            ].to_numpy().astype(np.float64, copy=False)
            candidate_volume = aligned["prior_volume_notional"].to_numpy().astype(
                np.float64, copy=False
            )
            return_sum = np.zeros(shape, dtype=np.float64)
            cost_sum = np.zeros(shape, dtype=np.float64)
            active_count = np.zeros(shape, dtype=np.int16)
            finite_candidate = (
                executable_values
                & np.isfinite(candidate_returns)
                & np.isfinite(candidate_costs)
            )
            np.add.at(
                return_sum,
                (di[finite_candidate], si[finite_candidate]),
                np.expm1(candidate_returns[finite_candidate]),
            )
            np.add.at(
                cost_sum,
                (di[finite_candidate], si[finite_candidate]),
                candidate_costs[finite_candidate],
            )
            np.add.at(
                active_count,
                (di[finite_candidate], si[finite_candidate]),
                1,
            )
            active_underlying = active_count > 0
            returns[active_underlying] = np.log1p(
                return_sum[active_underlying] / active_count[active_underlying]
            ).astype(np.float32, copy=False)
            costs[active_underlying] = (
                cost_sum[active_underlying] / active_count[active_underlying]
            ).astype(np.float32, copy=False)
            np.add.at(prior_volume_notional, (di, si), candidate_volume.astype(np.float32))

    valid_exec_values = executable & np.isfinite(returns)
    simple = np.zeros(shape, dtype=np.float64)
    simple[valid_exec_values] = np.expm1(
        returns[valid_exec_values].astype(np.float64)
    )
    counts = np.count_nonzero(valid_exec_values, axis=1)
    summed = simple.sum(axis=1)
    benchmark = np.zeros(dates.size, dtype=np.float32)
    nonempty = counts > 0
    benchmark[nonempty] = np.log1p(summed[nonempty] / counts[nonempty]).astype(
        np.float32, copy=False
    )

    panel.stock_futures_day_trade_daily = TaiwanStockFuturesDayTradeDaily(
        dates=dates,
        symbols=symbols,
        intraday_log_returns=returns,
        policy_eligible_mask=policy,
        executable_mask=executable,
        round_trip_cost_rate_per_open_notional=costs,
        prior_volume_notional=prior_volume_notional,
        benchmark_log_returns=benchmark,
        selected_rows=int(aligned.height),
        selected_underlyings=int(aligned["underlying_symbol"].n_unique()),
        source_path=str(data_path),
        manifest_path=str(manifest_path),
        entry_clock=(
            "first_strictly_later_public_trade_after_090000_through_090059"
            if uses_post_0900_sidecar
            else (
                "taifex_day_session_open_0845_daily_proxy_for_0900_decision"
                if normalized_entry_source
                == ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN_0900_PROXY
                else "taifex_day_session_open_0845"
            )
        ),
        entry_source_path=(
            str(Path(entry_0900_data_path))
            if uses_post_0900_sidecar
            else (
                str(data_path)
                if normalized_entry_source
                == ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN_0900_PROXY
                else None
            )
        ),
        entry_manifest_path=(
            str(entry_manifest_path)
            if entry_manifest_path is not None
            else (
                str(manifest_path)
                if normalized_entry_source
                == ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN_0900_PROXY
                else None
            )
        ),
        integer_candidate_execution=integer_candidate_execution,
        integer_candidate_multipliers=(
            STOCK_FUTURES_INTEGER_CANDIDATE_MULTIPLIERS
            if integer_contracts
            else ()
        ),
        integer_candidate_selection=(
            "standard_2000_then_mini_100_causal_nearby_integer_basket_v1"
            if integer_contracts
            else None
        ),
        contract_version=(
            TAIFEX_STOCK_FUTURES_INTEGER_DATA_CONTRACT_VERSION
            if integer_contracts
            else TAIFEX_STOCK_FUTURES_DAY_TRADE_DATA_CONTRACT_VERSION
        ),
    )
    return panel


__all__ = [
    "DEFAULT_DATA_PATH",
    "DEFAULT_0900_ENTRY_DATA_PATH",
    "ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN",
    "ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN_0900_PROXY",
    "ENTRY_PRICE_SOURCE_POST_0900_TRADE_SIDECAR",
    "STOCK_FUTURES_DAY_TRADE_ENTRY_PRICE_SOURCES",
    "TAIFEX_STOCK_FUTURES_DAY_TRADE_BACKTEST_CONTRACT_VERSION",
    "TAIFEX_STOCK_FUTURES_DAY_TRADE_0900_BACKTEST_CONTRACT_VERSION",
    "TAIFEX_STOCK_FUTURES_DAY_TRADE_DATA_CONTRACT_VERSION",
    "TAIFEX_STOCK_FUTURES_0900_ENTRY_DATA_CONTRACT_VERSION",
    "TAIFEX_STOCK_FUTURES_INTEGER_BACKTEST_CONTRACT_VERSION",
    "TAIFEX_STOCK_FUTURES_INTEGER_DATA_CONTRACT_VERSION",
    "STOCK_FUTURES_INTEGER_CANDIDATE_MULTIPLIERS",
    "STOCK_FUTURES_INTEGER_EXECUTION_CHANNELS",
    "TaiwanStockFuturesDayTradeDaily",
    "_load_0900_entries",
    "_with_0900_execution_costs",
    "_with_integer_execution_contract",
    "attach_stock_futures_day_trade_daily",
    "select_causal_front_stock_futures",
    "select_causal_front_stock_futures_candidates",
]
