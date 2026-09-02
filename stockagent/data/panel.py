from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time
import fnmatch
from pathlib import Path
import csv
import hashlib
import json
import pickle
import os
from typing import Any

import numpy as np

try:
    import polars as pl
except Exception:  # pragma: no cover - optional parquet reader
    pl = None

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional parquet reader
    pa = None
    pc = None
    pq = None

from stockagent.data.panel_cache import (
    legacy_panel_cache_path,
    legacy_panel_meta_path,
    load_panel_cache_v2,
    load_panel_cache_v2_manifest,
    panel_cache_v2_is_valid,
    panel_cache_v2_dir,
    save_panel_cache_v2,
)
from stockagent.data.us_universe import (
    BROKER_TRADABLE_SECURITY_FILTER,
    normalize_us_symbol_key,
    us_broker_untradable_reason,
)

try:
    from stockagent.data import panel_numba as _panel_numba
except Exception:  # pragma: no cover - Numba is an acceleration dependency
    _panel_numba = None


RESERVED_COLUMNS = {"date", "symbol", "return_1d", "tradable"}
LOG_RETURN_FEATURE_COLUMNS = [
    # ==================================================
    # Price Log Return
    # 前一日價格變化
    # ==================================================
    "open_logret_1d",
    "max_logret_1d",
    "min_logret_1d",
    "close_logret_1d",

    # ==================================================
    # Volume
    # 成交量變化
    # ==================================================
    "trading_volume_logret_1d",
    "signed_vol",

    # ==================================================
    # Intraday Price Structure
    # 日內價格結構
    # ==================================================
    # "intraday_return_co",
    # "overnight_gap_oc",
    # "intraday_range",

    # ==================================================
    # Body
    # K棒實體
    # ==================================================
    "body_ratio",
    "signed_body_ratio",
    "delta_body_ratio",

    # ==================================================
    # CLV
    # 收盤位置
    # ==================================================
    "clv",
    "clv_centered",
    "delta_clv",

    # ==================================================
    # Shadow
    # 上下影線
    # ==================================================
    "upper_shadow",
    "lower_shadow",
    "shadow_imbalance",
]
DAY_TRADE_OPEN_GAP_FEATURE = "next_session_open_gap_logret"
# Execution-context features have a later availability timestamp than ordinary
# end-of-session features.  They are materialized only when named explicitly
# in feature_include; an empty include or wildcard must not silently expose a
# next-session value to a non-day-trade model.
BASE_PANEL_FEATURE_COLUMNS = [
    *LOG_RETURN_FEATURE_COLUMNS,
    DAY_TRADE_OPEN_GAP_FEATURE,
]
# Version 45 adds point-in-time TW margin-short eligibility/capacity arrays.
# Version 44 carries raw-close valuation through ordinary symbol halts while
# resetting the basis at lifecycle/corporate-action boundaries.  Version 43
# separated causal open-side day-trade masks from full-session close/volume
# availability.
# v46 separates point-in-time margin-short eligibility from the optional
# demonstrated-capacity ceiling.  Older caches folded capacity>0 into the
# eligibility mask and cannot support an explicit no-capacity-limit account.
# v47 carries receipt-verified exact cash-dividend entitlement/payment tensors.
# v48 applies the Article 76 Lunar New Year settlement-day counting exception
# to exact MOPS stop-transfer dates.  v49 places a permanent-exit liquidation
# on the final positive close of the ending security episode when the official
# termination date itself has no executable quote.
# v50 resolves that terminal liquidation only after every buy/sell side rule is
# known, and stores immutable logical-array fingerprints in the panel cache.
# v51 separates opening- and closing-auction short-open masks and keeps the
# official inventory headroom independent of later close-side execution facts.
# v52 reads product-supplied policy and execution masks independently. v53
# keeps forward valuation returns across rows that have a real execution mark
# even when their feature window is policy-ineligible. v54 invalidates cached
# feature tensors after the Bybit public-web input schema expansion.
PANEL_CACHE_VERSION = 54
# v2 distinguishes the cumulative corporate-action archive coverage from the
# latest incremental downloader request.  Keep this in the backend contract so
# panels built with the old requested_start_year interpretation are never
# reused for TW cash execution.
CORPORATE_ACTION_COVERAGE_CONTRACT_VERSION = 2
# Separates the full avoidance interval for every official action from the
# unresolved-only interval used when exact cash entitlements are enabled.
CORPORATE_ACTION_AVOIDANCE_CONTRACT_VERSION = 2
FEATURE_FILE_SUFFIX = "_features.parquet"
HOT_TAIL_DIRNAME = "_hot_tail"
DEFAULT_EXTERNAL_MARKET_SYMBOL = "__MARKET__"
EPSILON = 1e-8
# Treat single-day price ratios beyond 5x or below 1/5x as unusable labels and
# features. The full US universe contains stale/delisted Yahoo rows with
# penny-to-thousands jumps that otherwise dominate log-return backtests.
MAX_ABS_DAILY_PRICE_LOG_RETURN = float(np.log(5.0))
TW_MAX_ABS_DAILY_PRICE_LOG_RETURN = float(np.log(2.0))
PREV_DAY_LOG_RETURN_RENAME = {
    "open": "open_logret_1d",
    "max": "max_logret_1d",
    "min": "min_logret_1d",
    "close": "close_logret_1d",
    "Trading_Volume": "trading_volume_logret_1d",
}
# Canonical writers should use ``day_trade_eligible``.  The remaining aliases
# make the reader tolerant of existing research receipts without guessing from
# today's eligibility list.  Presence is still required: absence remains None
# and the day-trading dataset fails closed.
DAY_TRADE_ELIGIBILITY_COLUMNS = (
    "day_trade_eligible",
    "day_trading_eligible",
    "is_day_trade_eligible",
    "Day_Trade_Eligible",
    "_twpub_day_trade_eligible",
)
_MISSING_VOLUME_WARNED_SYMBOLS: set[str] = set()


@dataclass(slots=True)
class _SymbolSecurityMetadata:
    name: str
    market: str


class _MissingTradingVolumeError(ValueError):
    pass


def _normalize_trading_volume_policy(policy: str | bool | None) -> str:
    if isinstance(policy, bool):
        return "required" if policy else "optional"
    normalized = str(policy or "auto").strip().lower()
    if normalized not in {"auto", "required", "optional"}:
        raise ValueError(
            "trading_volume_policy must be one of: auto, required, optional; "
            f"got {policy!r}"
        )
    return normalized


def _path_requires_trading_volume(path: Path, policy: str | bool | None) -> bool:
    normalized = _normalize_trading_volume_policy(policy)
    if normalized == "required":
        return True
    if normalized == "optional":
        return False
    parts = {part.lower() for part in path.parts}
    path_text = path.as_posix().lower()
    if {"forex", "forex_pepperstone", "data_forex_frankfurter"} & parts:
        return False
    if "frankfurter" in path_text or "pepperstone" in path_text:
        return False
    volume_assets = {
        "tw_stocks",
        "us_stocks",
        "crypto",
        "data_parquet",
        "data_okx",
        "data_bybit",
        "data_binance",
    }
    return bool(volume_assets & parts)


def _require_trading_volume_column(path: Path, columns: set[str], policy: str | bool | None) -> None:
    if "Trading_Volume" in columns or not _path_requires_trading_volume(path, policy):
        return
    raise _MissingTradingVolumeError(
        f"{path.name} is missing required Trading_Volume column under "
        f"trading_volume_policy={_normalize_trading_volume_policy(policy)!r}. "
        "Use trading_volume_policy='optional' only for assets without meaningful volume."
    )


def _round_half_up(values: np.ndarray, decimals: int = 2) -> np.ndarray:
    """Round with half-up semantics (0.5 always rounds away from zero)."""
    if _panel_numba is not None:
        return _panel_numba.round_half_up(values, decimals=decimals)
    arr = np.asarray(values, dtype=np.float64)
    factor = float(10**decimals)
    out = np.full(arr.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(arr)
    pos = valid & (arr >= 0.0)
    neg = valid & (arr < 0.0)
    out[pos] = np.floor(arr[pos] * factor + 0.5) / factor
    out[neg] = np.ceil(arr[neg] * factor - 0.5) / factor
    return out


def _is_tw_market_path(path: Path) -> bool:
    parts = {part.lower() for part in path.parts}
    symbol = _symbol_name_from_path(path)
    return "tw_stocks" in parts or symbol.isdigit()


def _is_tw_official_path(path: Path) -> bool:
    parts = {part.lower() for part in path.parts}
    return {"data_tw_public", "stocks"} <= parts


def _price_decimals_for_path(path: Path) -> int:
    """Return market-specific price precision: TW=2, others=8 decimals."""
    return 2 if _is_tw_market_path(path) else 8


def _adjclose_decimals_for_path(path: Path) -> int:
    parts = {part.lower() for part in path.parts}
    if "data_bybit" in parts and "perpetual_daily" in parts:
        return 16
    return 8 if _is_tw_official_path(path) else _price_decimals_for_path(path)


def _max_abs_daily_price_log_return_for_path(path: Path) -> float:
    # Consecutive-session 2x moves in established TW files are scale changes,
    # not executable total returns. First listings have no preceding row, and
    # relistings after a gap are rejected by the market-session label contract.
    if _is_tw_official_path(path):
        return MAX_ABS_DAILY_PRICE_LOG_RETURN
    return (
        TW_MAX_ABS_DAILY_PRICE_LOG_RETURN
        if _is_tw_market_path(path)
        else MAX_ABS_DAILY_PRICE_LOG_RETURN
    )


def _return_price_column(frame: Any, path: Path) -> str:
    """Choose the price series used for forward return labels."""
    # Use adjusted close whenever available so corporate actions
    # (splits/dividends/capital changes) do not create fake label jumps.
    if "adjclose" in frame.columns:
        return "adjclose"
    return "close"


@dataclass(slots=True)
class PanelData:
    dates: np.ndarray
    symbols: list[str]
    feature_names: list[str]
    features: np.ndarray
    returns_1d: np.ndarray
    tradable_mask: np.ndarray
    alive_mask: np.ndarray
    benchmark_returns: np.ndarray
    close_prices: np.ndarray
    daily_volumes: np.ndarray | None = None
    can_buy_mask: np.ndarray | None = None
    can_sell_mask: np.ndarray | None = None
    # New/increased short eligibility at the closing auction.  This may use
    # session-t close execution facts such as the realized limit-down state.
    can_short_open_mask: np.ndarray | None = None
    # New/increased short eligibility at the opening auction.  This is a
    # distinct point-in-time contract: it may use open[t] but must never be
    # reconstructed from can_short_open_mask/can_sell_mask, both of which can
    # contain information first known at close[t].  None means unavailable;
    # carrying-mode consumers must fail closed.
    can_short_open_open_mask: np.ndarray | None = None
    force_short_cover_mask: np.ndarray | None = None
    force_exit_mask: np.ndarray | None = None
    open_prices: np.ndarray | None = None
    intraday_returns: np.ndarray | None = None
    day_trade_eligible_mask: np.ndarray | None = None
    day_trade_can_short_open_mask: np.ndarray | None = None
    day_trade_can_buy_open_mask: np.ndarray | None = None
    day_trade_can_sell_open_mask: np.ndarray | None = None
    raw_close_returns_1d: np.ndarray | None = None
    # Full last-executable-close..last-cum-right-close avoidance interval for
    # every receipt-verified action. ``avoid`` mode uses this even when an
    # exact entitlement exists; exact mode uses the unresolved-only mask below
    # together with the issuer payment ledger.
    corporate_action_avoidance_mask: np.ndarray | None = None
    # Backward-compatible field name.  This is now a receipt-verified, known
    # corporate-action avoidance transition, not an adjclose-difference guess.
    unresolved_corporate_action_mask: np.ndarray | None = None
    # Exact issuer-announced cash entitlement earned at the preceding close.
    # The yield is cash-per-share divided by that executable close; the delay
    # is the number of exchange sessions until the announced payment date.
    # None means no receipt-verified exact entitlement archive was available.
    cash_dividend_yield: np.ndarray | None = None
    cash_dividend_payment_delay_sessions: np.ndarray | None = None
    # Exchange-wide, point-in-time headroom for opening a margin short.  Values
    # are exact shares, not board lots.  None means the panel has no verified
    # historical margin evidence; consumers must fail closed rather than infer
    # it from can_sell_mask.
    short_capacity_shares: np.ndarray | None = None
    # Optional per-session/per-symbol legal or broker margin ratio.  Historical
    # sources in this module do not guess it; unknown values remain NaN/None.
    short_margin_rate: np.ndarray | None = None
    # Executor-only compressed events [T,S,C] or full minute paths [T,S,M,C]
    # for a daily policy. They are attached by train.py after panel-cache
    # loading and never enter model features or the ordinary panel cache.
    day_trade_minute_execution: np.ndarray | None = None
    # Exact logical-array hashes supplied only by an immutable panel-cache
    # generation. Synthetic or caller-mutated panels leave this unset and are
    # hashed directly by checkpoint construction.
    content_fingerprints: dict[str, dict[str, Any]] | None = None
    # Optional executor-only TAIFEX arrays. They are attached by train.py after
    # the stock panel has been built/cached and are never model input features.
    index_futures_day_session: Any | None = None
    index_futures_reference_product: str | None = None
    # Prior-session-only TX/MTX/TMF x E1..E6 model tokens and current-session
    # executor outcomes.  The former may enter the model; the latter are
    # labels/execution facts and must never be concatenated to stock features.
    index_futures_candidate_features: np.ndarray | None = None
    index_futures_candidate_mask: np.ndarray | None = None
    index_futures_context_symbols: tuple[str, ...] | None = None
    index_futures_execution_returns: np.ndarray | None = None
    index_options_monthly_day_session: Any | None = None
    index_options_weekly_day_session: Any | None = None
    index_options_chain_day_session: Any | None = None
    # Causal relative-tenor derivative candidates.  Option candidate metadata
    # is model context, while same-session simple returns and concrete mappings
    # remain labels/executor facts.
    index_derivatives_day_candidates: Any | None = None
    index_derivatives_candidate_features: np.ndarray | None = None
    index_derivatives_candidate_mask: np.ndarray | None = None
    index_derivatives_simple_returns: np.ndarray | None = None
    # Executor-only TAIFEX stock/ETF/index futures physical-contract identities.
    # The generic feature tensor remains owned by this PanelData;
    # physical contract codes and liquidation labels are never model inputs.
    futures_portfolio_daily: Any | None = None
    # Full cash-stock model context with a separate fixed all-TAIFEX action
    # axis. Prior-session futures tokens may enter the policy; current prices,
    # returns, fill gates, fees, and expiry liquidation remain executor-only.
    stock_context_futures_portfolio_daily: Any | None = None
    # Executor-only front-month single-stock-futures day-trade labels aligned
    # to the complete cash-stock universe.  The model still observes every
    # stock feature column; this attachment owns the causal futures mapping,
    # execution mask, return, capacity, and round-trip cost side channels.
    stock_futures_day_trade_daily: Any | None = None

    @property
    def num_dates(self) -> int:
        return int(self.features.shape[0])

    @property
    def num_symbols(self) -> int:
        return int(self.features.shape[1])


def _normalize_panel_start_date(value: str | date | np.datetime64 | None) -> np.datetime64 | None:
    if value is None or not str(value).strip():
        return None
    text = str(value).strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(
            "panel_start_date must be an ISO date (YYYY-MM-DD) or null, "
            f"got {value!r}"
        ) from exc
    return np.datetime64(parsed.isoformat(), "D")


def _slice_panel_start(panel: PanelData, panel_start_date: np.datetime64 | None) -> PanelData:
    """Apply the declared inclusive model horizon without deleting source history."""
    if panel_start_date is None:
        return panel
    panel_dates = np.asarray(panel.dates, dtype="datetime64[D]")
    start = int(np.searchsorted(panel_dates, panel_start_date, side="left"))
    if start >= int(panel_dates.size):
        raise ValueError(
            f"panel_start_date={panel_start_date} is after the last panel date "
            f"{panel_dates[-1] if panel_dates.size else 'n/a'}"
        )
    if start == 0:
        return panel
    slc = slice(start, None)

    def sliced(values: np.ndarray | None) -> np.ndarray | None:
        return None if values is None else values[slc]

    print(
        f"[panel] applied inclusive panel_start_date={panel_start_date}; "
        f"kept {panel.num_dates - start}/{panel.num_dates} dates"
    )
    return PanelData(
        dates=panel.dates[slc],
        symbols=panel.symbols,
        feature_names=panel.feature_names,
        features=panel.features[slc],
        returns_1d=panel.returns_1d[slc],
        tradable_mask=panel.tradable_mask[slc],
        can_buy_mask=sliced(panel.can_buy_mask),
        can_sell_mask=sliced(panel.can_sell_mask),
        can_short_open_mask=sliced(panel.can_short_open_mask),
        can_short_open_open_mask=sliced(panel.can_short_open_open_mask),
        force_short_cover_mask=sliced(panel.force_short_cover_mask),
        force_exit_mask=sliced(panel.force_exit_mask),
        short_capacity_shares=sliced(panel.short_capacity_shares),
        short_margin_rate=sliced(panel.short_margin_rate),
        alive_mask=panel.alive_mask[slc],
        benchmark_returns=panel.benchmark_returns[slc],
        close_prices=panel.close_prices[slc],
        daily_volumes=sliced(panel.daily_volumes),
        open_prices=sliced(panel.open_prices),
        intraday_returns=sliced(panel.intraday_returns),
        day_trade_eligible_mask=sliced(panel.day_trade_eligible_mask),
        day_trade_can_short_open_mask=sliced(panel.day_trade_can_short_open_mask),
        day_trade_can_buy_open_mask=sliced(panel.day_trade_can_buy_open_mask),
        day_trade_can_sell_open_mask=sliced(panel.day_trade_can_sell_open_mask),
        raw_close_returns_1d=sliced(panel.raw_close_returns_1d),
        corporate_action_avoidance_mask=sliced(
            panel.corporate_action_avoidance_mask
        ),
        unresolved_corporate_action_mask=sliced(
            panel.unresolved_corporate_action_mask
        ),
        cash_dividend_yield=sliced(panel.cash_dividend_yield),
        cash_dividend_payment_delay_sessions=sliced(
            panel.cash_dividend_payment_delay_sessions
        ),
    )


def slice_panel_start(
    panel: PanelData,
    panel_start_date: str | date | np.datetime64 | None,
) -> PanelData:
    """Apply the canonical inclusive panel boundary to an existing panel."""

    return _slice_panel_start(
        panel,
        _normalize_panel_start_date(panel_start_date),
    )


@dataclass(slots=True)
class _SymbolPanelArrays:
    symbol: str
    dates: np.ndarray
    features: np.ndarray
    returns_1d: np.ndarray
    close_prices: np.ndarray
    open_prices: np.ndarray
    intraday_returns: np.ndarray
    daily_volumes: np.ndarray
    tradable_mask: np.ndarray
    # Whether the row's execution mark is sufficient to value a position into
    # that row. This normally matches tradable_mask, but products with an
    # explicit policy/execution split (for example crypto perpetuals with an
    # incomplete feature session) may remain valuably executable while being
    # ineligible for a new model target.
    return_valuation_mask: np.ndarray
    can_buy_mask: np.ndarray
    can_sell_mask: np.ndarray
    day_trade_can_buy_open_mask: np.ndarray
    day_trade_can_sell_open_mask: np.ndarray
    alive_mask: np.ndarray
    day_trade_eligible_mask: np.ndarray | None = None


def _slice_symbol_arrays_start(
    arrays: _SymbolPanelArrays,
    panel_start_date: np.datetime64 | None,
) -> _SymbolPanelArrays:
    """Trim loaded rows before dense materialization.

    Per-symbol forward labels are already computed by the native reader, so
    slicing here preserves the existing panel-start semantics while avoiding a
    potentially huge dense allocation for dates that will immediately be
    discarded by ``_slice_panel_start``.
    """

    if panel_start_date is None or arrays.dates.size == 0:
        return arrays
    start = int(
        np.searchsorted(
            np.asarray(arrays.dates, dtype="datetime64[D]"),
            panel_start_date,
            side="left",
        )
    )
    if start == 0:
        return arrays
    slc = slice(start, None)

    def sliced(values: np.ndarray | None) -> np.ndarray | None:
        return None if values is None else values[slc]

    return _SymbolPanelArrays(
        symbol=arrays.symbol,
        dates=arrays.dates[slc],
        features=arrays.features[slc],
        returns_1d=arrays.returns_1d[slc],
        close_prices=arrays.close_prices[slc],
        open_prices=arrays.open_prices[slc],
        intraday_returns=arrays.intraday_returns[slc],
        daily_volumes=arrays.daily_volumes[slc],
        tradable_mask=arrays.tradable_mask[slc],
        return_valuation_mask=arrays.return_valuation_mask[slc],
        can_buy_mask=arrays.can_buy_mask[slc],
        can_sell_mask=arrays.can_sell_mask[slc],
        day_trade_can_buy_open_mask=arrays.day_trade_can_buy_open_mask[slc],
        day_trade_can_sell_open_mask=arrays.day_trade_can_sell_open_mask[slc],
        alive_mask=arrays.alive_mask[slc],
        day_trade_eligible_mask=sliced(arrays.day_trade_eligible_mask),
    )


@dataclass(slots=True)
class _ExternalFeatureArrays:
    feature_names: list[str]
    market_dates: np.ndarray
    market_values: np.ndarray
    by_symbol: dict[str, tuple[np.ndarray, np.ndarray]]
    rule_names: list[str]
    market_rule_values: np.ndarray
    by_symbol_rules: dict[str, tuple[np.ndarray, np.ndarray]]
    official_session_dates: np.ndarray


@dataclass(frozen=True, slots=True)
class _CorporateActionReferencePaths:
    parquet: Path
    summary: Path
    entitlements_parquet: Path | None = None
    entitlements_summary: Path | None = None


@dataclass(slots=True)
class _CorporateActionReference:
    event_dates_by_symbol: dict[str, np.ndarray]
    coverage_start: np.datetime64
    coverage_end: np.datetime64
    exact_cash_terms_by_symbol: dict[
        str, tuple[np.ndarray, np.ndarray, np.ndarray]
    ] | None = None
    exact_coverage_start: np.datetime64 | None = None
    exact_coverage_end: np.datetime64 | None = None
    # Receipt-verified MOPS stop-transfer starts.  Article 76 derives the
    # mandatory margin-short closeout and four-session short-open ban from
    # this date, independently of the long-side cash entitlement treatment.
    margin_short_stop_transfer_by_symbol: dict[
        str, tuple[np.ndarray, np.ndarray]
    ] | None = None


def _symbol_name_from_path(path: Path) -> str:
    return path.name.removesuffix(FEATURE_FILE_SUFFIX)


def _normalize_external_feature_path(path: str | Path | None) -> Path | None:
    if path is None:
        return None
    text = str(path).strip()
    if not text:
        return None
    return Path(text)


def _resolve_external_data_path(
    path: str | Path | None,
    *,
    include_features: bool,
    include_rules: bool,
    required: bool,
) -> Path | None:
    """Validate one external parquet request without coupling rules to inputs."""

    include_any = bool(include_features) or bool(include_rules)
    normalized = _normalize_external_feature_path(path)
    if required and not include_any:
        raise ValueError(
            "external_data_required=True requires external features or rules to be enabled"
        )
    if not include_any:
        return None
    if normalized is None:
        if required:
            raise FileNotFoundError(
                "external TW public data is required but external_feature_path is empty"
            )
        return None
    if not normalized.exists():
        raise FileNotFoundError(f"external_feature_path not found: {normalized}")
    return normalized


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_corporate_action_reference_paths(
    external_feature_path: Path | None,
    *,
    include_rules: bool,
) -> _CorporateActionReferencePaths | None:
    """Locate the canonical action reference adjacent to TW public features.

    The feature parquet normally lives under ``data_tw_public/features`` while
    the reference and its completeness receipt live one directory above.  A
    synthetic/non-TW external parquet is allowed to have no reference; the
    Taiwan cash dataset then fails closed instead of inventing a no-action
    history.
    """

    if not include_rules or external_feature_path is None:
        return None
    candidates: list[Path] = []
    for directory in (external_feature_path.parent, external_feature_path.parent.parent):
        candidate = directory / "tw_corporate_action_reference.parquet"
        if candidate not in candidates:
            candidates.append(candidate)
    for parquet_path in candidates:
        if not parquet_path.exists():
            continue
        summary_path = parquet_path.with_suffix(".summary.json")
        if not summary_path.exists():
            raise FileNotFoundError(
                "TW corporate-action reference exists without its completeness "
                f"receipt: {summary_path}"
            )
        entitlement_path = parquet_path.with_name(
            "tw_corporate_action_entitlements.parquet"
        )
        entitlement_summary = entitlement_path.with_suffix(".summary.json")
        if entitlement_path.exists() != entitlement_summary.exists():
            raise FileNotFoundError(
                "TW exact corporate-action entitlement parquet and receipt "
                "must either both exist or both be absent: "
                f"{entitlement_path}, {entitlement_summary}"
            )
        return _CorporateActionReferencePaths(
            parquet=parquet_path,
            summary=summary_path,
            entitlements_parquet=(
                entitlement_path if entitlement_path.exists() else None
            ),
            entitlements_summary=(
                entitlement_summary if entitlement_summary.exists() else None
            ),
        )
    return None


def _load_exact_cash_entitlements(
    paths: _CorporateActionReferencePaths,
) -> tuple[
    dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] | None,
    dict[str, tuple[np.ndarray, np.ndarray]] | None,
    np.datetime64 | None,
    np.datetime64 | None,
]:
    """Load a fail-closed MOPS cash-entitlement archive, when installed."""

    parquet_path = paths.entitlements_parquet
    summary_path = paths.entitlements_summary
    if parquet_path is None or summary_path is None:
        return None, None, None, None
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Invalid TW exact entitlement completeness receipt: {summary_path}"
        ) from exc
    if not bool(summary.get("baseline_established")) or not bool(
        summary.get("coverage_complete")
    ):
        raise ValueError("TW exact entitlement archive is not a complete baseline")
    if int(summary.get("failure_count", -1)) != 0:
        raise ValueError("TW exact entitlement receipt contains request failures")
    if int(summary.get("schema_version", -1)) < 3:
        raise ValueError("TW exact entitlement schema_version must be >= 3")
    raw_manifest_receipt = summary.get("raw_receipt_manifest")
    if not isinstance(raw_manifest_receipt, dict):
        raise ValueError("TW exact entitlement raw_receipt_manifest is missing")
    raw_manifest_relative = str(
        raw_manifest_receipt.get("relative_path", "")
    ).strip()
    if not raw_manifest_relative:
        raise ValueError(
            "TW exact entitlement raw receipt manifest path is missing"
        )
    entitlement_root = summary_path.parent.resolve()
    raw_manifest_path = (entitlement_root / raw_manifest_relative).resolve()
    if not raw_manifest_path.is_relative_to(entitlement_root):
        raise ValueError(
            "TW exact entitlement raw receipt manifest escapes its data root"
        )
    if not raw_manifest_path.is_file():
        raise FileNotFoundError(raw_manifest_path)
    if int(raw_manifest_receipt.get("size", -1)) != int(
        raw_manifest_path.stat().st_size
    ):
        raise ValueError(
            "TW exact entitlement raw receipt manifest size mismatch"
        )
    raw_manifest_sha = _sha256_file(raw_manifest_path)
    if (
        str(raw_manifest_receipt.get("sha256", "")).strip().lower()
        != raw_manifest_sha
    ):
        raise ValueError(
            "TW exact entitlement raw receipt manifest SHA-256 mismatch"
        )
    if raw_manifest_path.stem != raw_manifest_sha:
        raise ValueError(
            "TW exact entitlement raw receipt manifest is not content-addressed"
        )
    with raw_manifest_path.open("rb") as manifest_handle:
        raw_manifest_entries = sum(1 for line in manifest_handle if line.strip())
    if int(raw_manifest_receipt.get("entries", -1)) != raw_manifest_entries:
        raise ValueError(
            "TW exact entitlement raw receipt manifest row count mismatch"
        )
    reference_receipt = summary.get("reference_receipt")
    if not isinstance(reference_receipt, dict):
        raise ValueError("TW exact entitlement reference_receipt is missing")
    if int(reference_receipt.get("size", -1)) != int(paths.parquet.stat().st_size):
        raise ValueError("TW exact entitlement was built from another reference size")
    if str(reference_receipt.get("sha256", "")).strip().lower() != _sha256_file(
        paths.parquet
    ):
        raise ValueError("TW exact entitlement was built from another reference SHA-256")
    receipt = summary.get("output_receipt")
    if not isinstance(receipt, dict):
        raise ValueError("TW exact entitlement output_receipt is missing")
    if int(receipt.get("size", -1)) != int(parquet_path.stat().st_size):
        raise ValueError("TW exact entitlement size does not match its receipt")
    if str(receipt.get("sha256", "")).strip().lower() != _sha256_file(
        parquet_path
    ):
        raise ValueError("TW exact entitlement SHA-256 does not match its receipt")

    required = {
        "date",
        "symbol",
        "handling",
        "cash_dividend_per_share",
        "cash_payment_date",
        "stop_transfer_start",
    }
    missing = required - set(pq.read_schema(parquet_path).names)
    if missing:
        raise ValueError(
            "TW exact entitlement archive is missing required columns: "
            f"{sorted(missing)}"
        )
    table = pq.read_table(parquet_path, columns=sorted(required), memory_map=True)
    if int(summary.get("rows", -1)) != int(table.num_rows):
        raise ValueError("TW exact entitlement row count does not match its receipt")
    if int(summary.get("reference_rows", -1)) != int(table.num_rows):
        raise ValueError(
            "TW exact entitlement archive does not classify every reference event"
        )
    dates = table["date"].combine_chunks().to_numpy(zero_copy_only=False).astype(
        "datetime64[D]", copy=False
    )
    symbols = np.asarray(
        [
            str(value).strip().upper() if value is not None else ""
            for value in table["symbol"].to_pylist()
        ],
        dtype=str,
    )
    handling = np.asarray(
        [str(value).strip() if value is not None else "" for value in table["handling"].to_pylist()],
        dtype=str,
    )
    if bool((np.isnat(dates) | (symbols == "")).any()):
        raise ValueError("TW exact entitlement archive contains invalid event keys")
    if not bool(np.isin(handling, ["exact_cash", "avoid"]).all()):
        raise ValueError("TW exact entitlement archive contains an unknown handling mode")
    order = np.lexsort((dates, symbols))
    if dates.size > 1:
        ordered_dates = dates[order]
        ordered_symbols = symbols[order]
        if bool(
            (
                (ordered_dates[1:] == ordered_dates[:-1])
                & (ordered_symbols[1:] == ordered_symbols[:-1])
            ).any()
        ):
            raise ValueError("TW exact entitlement archive has duplicate date+symbol keys")

    cash = np.asarray(
        [np.nan if value is None else float(value) for value in table["cash_dividend_per_share"].to_pylist()],
        dtype=np.float64,
    )
    payment = np.asarray(
        [
            np.datetime64("NaT", "D")
            if value is None
            else np.datetime64(value, "D")
            for value in table["cash_payment_date"].to_pylist()
        ],
        dtype="datetime64[D]",
    )
    stop_transfer = np.asarray(
        [
            np.datetime64("NaT", "D")
            if value is None
            else np.datetime64(value, "D")
            for value in table["stop_transfer_start"].to_pylist()
        ],
        dtype="datetime64[D]",
    )
    exact = handling == "exact_cash"
    if bool((exact & (~np.isfinite(cash) | (cash <= 0.0) | np.isnat(payment))).any()):
        raise ValueError("TW exact cash events contain invalid amount or payment date")
    try:
        coverage_start = np.datetime64(str(summary["coverage_start"]), "D")
        coverage_end = np.datetime64(str(summary["coverage_end"]), "D")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("TW exact entitlement receipt has invalid coverage") from exc
    if coverage_end < coverage_start:
        raise ValueError("TW exact entitlement coverage is reversed")

    terms: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
    for symbol in np.unique(symbols[exact]):
        selected = exact & (symbols == symbol)
        symbol_order = np.argsort(dates[selected])
        terms[str(symbol)] = (
            dates[selected][symbol_order],
            cash[selected][symbol_order],
            payment[selected][symbol_order],
        )
    short_terms: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    has_stop_transfer = ~np.isnat(stop_transfer)
    for symbol in np.unique(symbols[has_stop_transfer]):
        selected = has_stop_transfer & (symbols == symbol)
        symbol_order = np.argsort(dates[selected])
        short_terms[str(symbol)] = (
            dates[selected][symbol_order],
            stop_transfer[selected][symbol_order],
        )
    print(
        "[panel] verified TW exact cash entitlements "
        f"events={int(exact.sum())} symbols={len(terms)} path={parquet_path}"
    )
    return terms, short_terms, coverage_start, coverage_end


def _load_corporate_action_reference(
    paths: _CorporateActionReferencePaths | None,
) -> _CorporateActionReference | None:
    """Load and strictly validate the official ex-date reference.

    This source is an execution-safety calendar only.  It is never appended to
    model features.  The adjacent receipt must prove a complete rebuild and
    match the exact parquet bytes before any transition is trusted.
    """

    if paths is None:
        return None
    if pq is None:
        raise RuntimeError("TW corporate-action rules require pyarrow")
    try:
        summary = json.loads(paths.summary.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Invalid TW corporate-action completeness receipt: {paths.summary}"
        ) from exc
    if not bool(summary.get("baseline_established")):
        raise ValueError("TW corporate-action reference has no established baseline")
    if not bool(summary.get("coverage_complete")):
        raise ValueError("TW corporate-action reference coverage is incomplete")
    if int(summary.get("failure_count", -1)) != 0:
        raise ValueError("TW corporate-action reference receipt contains failures")
    if int(summary.get("schema_version", -1)) < 3:
        raise ValueError("TW corporate-action reference schema_version must be >= 3")
    receipt = summary.get("output_receipt")
    if not isinstance(receipt, dict):
        raise ValueError("TW corporate-action reference output_receipt is missing")
    reference_stat_before = paths.parquet.stat()
    actual_size = int(reference_stat_before.st_size)
    expected_size = int(receipt.get("size", -1))
    if actual_size != expected_size:
        raise ValueError(
            "TW corporate-action reference size does not match its receipt "
            f"({actual_size} != {expected_size})"
        )
    actual_sha256 = _sha256_file(paths.parquet)
    expected_sha256 = str(receipt.get("sha256", "")).strip().lower()
    if actual_sha256 != expected_sha256:
        raise ValueError("TW corporate-action reference SHA-256 does not match its receipt")

    required_columns = {"date", "symbol", "reference_price", "event_type"}
    schema_names = set(pq.read_schema(paths.parquet).names)
    missing = required_columns - schema_names
    if missing:
        raise ValueError(
            "TW corporate-action reference is missing required columns: "
            f"{sorted(missing)}"
        )
    table = pq.read_table(
        paths.parquet,
        columns=["date", "symbol", "reference_price"],
        memory_map=True,
    )
    reference_stat_after = paths.parquet.stat()
    if (
        int(reference_stat_before.st_size) != int(reference_stat_after.st_size)
        or int(reference_stat_before.st_mtime_ns)
        != int(reference_stat_after.st_mtime_ns)
        or int(reference_stat_before.st_ctime_ns)
        != int(reference_stat_after.st_ctime_ns)
    ):
        raise RuntimeError(
            "TW corporate-action reference changed while it was being validated"
        )
    if int(summary.get("rows", -1)) != int(table.num_rows):
        raise ValueError(
            "TW corporate-action reference row count does not match its receipt"
        )
    dates = table["date"].combine_chunks().to_numpy(zero_copy_only=False).astype(
        "datetime64[ns]", copy=False
    )
    symbols = np.asarray(
        [str(value).strip().upper() if value is not None else "" for value in table["symbol"].to_pylist()],
        dtype=str,
    )
    reference_prices = (
        table["reference_price"]
        .combine_chunks()
        .to_numpy(zero_copy_only=False)
        .astype(np.float64, copy=False)
    )
    valid = (
        ~np.isnat(dates)
        & (symbols != "")
        & np.isfinite(reference_prices)
        & (reference_prices > 0.0)
    )
    if not bool(valid.all()):
        raise ValueError(
            "TW corporate-action reference contains null/invalid event keys or prices"
        )
    try:
        requested_start_year = int(summary.get("requested_start_year"))
        # ``requested_start_year`` is only the lower bound of the most recent
        # incremental download.  ``coverage_start_year`` is the cumulative,
        # receipt-verified archive boundary and therefore the only valid
        # boundary for deciding whether an older panel is fully protected.
        # Fall back only for older schema-3 receipts that predate this field.
        coverage_start_year = int(
            summary.get("coverage_start_year", requested_start_year)
        )
        if coverage_start_year > requested_start_year:
            raise ValueError(
                "coverage_start_year cannot follow requested_start_year"
            )
        coverage_start = np.datetime64(
            f"{coverage_start_year:04d}-01-01", "D"
        )
        receipt_end = np.datetime64(str(summary.get("end_date", "")), "D")
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "TW corporate-action receipt has invalid coverage boundaries"
        ) from exc
    if dates.size:
        if receipt_end < dates.max().astype("datetime64[D]"):
            raise ValueError(
                "TW corporate-action receipt end_date precedes its latest event"
            )
    order = np.lexsort((dates.astype("datetime64[ns]"), symbols))
    sorted_dates = dates[order]
    sorted_symbols = symbols[order]
    if sorted_dates.size > 1:
        duplicate = (
            (sorted_dates[1:] == sorted_dates[:-1])
            & (sorted_symbols[1:] == sorted_symbols[:-1])
        )
        if bool(duplicate.any()):
            raise ValueError(
                "TW corporate-action reference contains duplicate date+symbol keys"
            )
    event_dates_by_symbol: dict[str, np.ndarray] = {}
    if sorted_dates.size:
        boundaries = np.flatnonzero(sorted_symbols[1:] != sorted_symbols[:-1]) + 1
        starts = np.concatenate(([0], boundaries))
        stops = np.concatenate((boundaries, [sorted_dates.size]))
        for start, stop in zip(starts, stops):
            event_dates_by_symbol[str(sorted_symbols[start])] = sorted_dates[start:stop]
    print(
        "[panel] verified TW corporate-action reference "
        f"rows={table.num_rows} symbols={len(event_dates_by_symbol)} "
        f"path={paths.parquet}"
    )
    exact_terms, short_terms, exact_start, exact_end = (
        _load_exact_cash_entitlements(paths)
    )
    return _CorporateActionReference(
        event_dates_by_symbol=event_dates_by_symbol,
        coverage_start=coverage_start,
        coverage_end=receipt_end,
        exact_cash_terms_by_symbol=exact_terms,
        exact_coverage_start=exact_start,
        exact_coverage_end=exact_end,
        margin_short_stop_transfer_by_symbol=short_terms,
    )


def _load_external_feature_arrays(
    path: Path,
    *,
    market_symbol: str = DEFAULT_EXTERNAL_MARKET_SYMBOL,
    include_features: bool = True,
    include_rules: bool = True,
    date_start: date | None = None,
    date_end: date | None = None,
) -> _ExternalFeatureArrays:
    if pl is None or pq is None:
        raise RuntimeError("external TW public features require polars and pyarrow")
    if not path.exists():
        raise FileNotFoundError(f"external feature parquet not found: {path}")

    required = {"date", "symbol"}
    parquet_columns = list(pq.read_schema(path).names)
    missing = required - set(parquet_columns)
    if missing:
        raise ValueError(f"{path} is missing required external feature columns: {sorted(missing)}")

    candidate_columns = [
        column
        for column in parquet_columns
        if column not in required
    ]
    feature_names = (
        [column for column in candidate_columns if not str(column).startswith("_")]
        if include_features
        else []
    )
    rule_names = (
        [column for column in candidate_columns if str(column).startswith("_twpub_")]
        if include_rules
        else []
    )
    value_columns = [*feature_names, *rule_names]
    # Rules-only TW runs should not materialize millions of rows across every
    # public model feature. Arrow projection keeps I/O and peak RAM proportional
    # to the independently enabled column families.
    date_type = pq.read_schema(path).field("date").type

    def filter_value(value: date) -> date | datetime | str:
        if pa.types.is_string(date_type) or pa.types.is_large_string(date_type):
            return value.isoformat()
        if pa.types.is_timestamp(date_type):
            return datetime.combine(value, time.min)
        return value

    filters: list[tuple[str, str, date | datetime | str]] = []
    if date_start is not None:
        filters.append(("date", ">=", filter_value(date_start)))
    if date_end is not None:
        filters.append(("date", "<=", filter_value(date_end)))
    frame = pl.from_arrow(
        pq.read_table(
            path,
            columns=["date", "symbol", *value_columns],
            filters=filters or None,
            memory_map=True,
        )
    )
    rule_frame = (
        pl.from_arrow(
            pq.read_table(
                path,
                columns=["date", "symbol", *rule_names],
                memory_map=True,
            )
        )
        if filters and rule_names
        else None
    )
    if not feature_names:
        if not rule_names:
            return _ExternalFeatureArrays(
                feature_names=[],
                market_dates=np.empty((0,), dtype="datetime64[ns]"),
                market_values=np.empty((0, 0), dtype=np.float32),
                by_symbol={},
                rule_names=[],
                market_rule_values=np.empty((0, 0), dtype=np.float64),
                by_symbol_rules={},
                official_session_dates=np.empty((0,), dtype="datetime64[ns]"),
            )

    def prepare_frame(source: Any, columns: list[str]) -> Any:
        return (
            source.with_columns(
                [
                    _polars_datetime_ns_expr(source.schema, "date"),
                    pl.col("symbol")
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .str.to_uppercase()
                    .alias("symbol"),
                    *[
                        pl.col(name).cast(pl.Float64, strict=False).alias(name)
                        for name in columns
                    ],
                ]
            )
            .drop_nulls(["date", "symbol"])
            .filter(pl.col("symbol") != "")
            .group_by(["date", "symbol"])
            .agg([pl.col(name).drop_nulls().last().alias(name) for name in columns])
            .sort(["symbol", "date"])
        )

    frame = prepare_frame(frame, value_columns)
    if rule_frame is not None:
        rule_frame = prepare_frame(rule_frame, rule_names)
    else:
        rule_frame = frame
    if frame.is_empty() and (rule_frame is None or rule_frame.is_empty()):
        return _ExternalFeatureArrays(
            feature_names=feature_names,
            market_dates=np.empty((0,), dtype="datetime64[ns]"),
            market_values=np.empty((0, len(feature_names)), dtype=np.float32),
            by_symbol={},
            rule_names=rule_names,
            market_rule_values=np.empty((0, len(rule_names)), dtype=np.float64),
            by_symbol_rules={},
            official_session_dates=np.empty((0,), dtype="datetime64[ns]"),
        )

    # `frame` is already sorted by symbol/date. Convert each column family once,
    # then retain zero-copy contiguous symbol slices instead of materializing
    # thousands of tiny Polars frames during live inference.
    all_dates, all_feature_values = _external_frame_to_arrays(frame, feature_names)
    all_symbols = frame["symbol"].to_numpy()
    if all_dates.size != all_symbols.size:
        raise RuntimeError("external feature date/symbol arrays are misaligned")

    market_key = str(market_symbol).strip().upper()
    market_mask = all_symbols == market_key
    market_dates = all_dates[market_mask]
    market_values = all_feature_values[market_mask]

    rule_dates, all_rule_values = _external_frame_to_arrays(
        rule_frame,
        rule_names,
        dtype=np.float64,
    )
    rule_symbols = rule_frame["symbol"].to_numpy()
    if rule_dates.size != rule_symbols.size:
        raise RuntimeError("external rule date/symbol arrays are misaligned")
    rule_market_mask = rule_symbols == market_key
    market_rule_dates = rule_dates[rule_market_mask]
    market_rule_values = all_rule_values[rule_market_mask]

    official_session_dates = np.empty((0,), dtype="datetime64[ns]")
    official_idx = (
        rule_names.index("_twpub_official_traded")
        if "_twpub_official_traded" in rule_names
        else None
    )
    if official_idx is not None and market_rule_dates.size:
        traded = np.nan_to_num(market_rule_values[:, official_idx], nan=0.0) > 0.0
        if bool(traded.any()):
            official_session_dates = np.unique(market_rule_dates[traded])

    stock_mask = ~market_mask
    stock_symbols = all_symbols[stock_mask]
    stock_dates = all_dates[stock_mask]
    stock_feature_values = all_feature_values[stock_mask]
    by_symbol: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    by_symbol_rules: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    if stock_symbols.size:
        boundaries = np.flatnonzero(stock_symbols[1:] != stock_symbols[:-1]) + 1
        starts = np.concatenate((np.array([0]), boundaries))
        stops = np.concatenate((boundaries, np.array([stock_symbols.size])))
        for start, stop in zip(starts.tolist(), stops.tolist(), strict=True):
            key = str(stock_symbols[start]).upper()
            dates = stock_dates[start:stop]
            by_symbol[key] = (dates, stock_feature_values[start:stop])

    rule_stock_mask = ~rule_market_mask
    stock_rule_symbols = rule_symbols[rule_stock_mask]
    stock_rule_dates = rule_dates[rule_stock_mask]
    stock_rule_values = all_rule_values[rule_stock_mask]
    if stock_rule_symbols.size:
        boundaries = np.flatnonzero(stock_rule_symbols[1:] != stock_rule_symbols[:-1]) + 1
        starts = np.concatenate((np.array([0]), boundaries))
        stops = np.concatenate((boundaries, np.array([stock_rule_symbols.size])))
        for start, stop in zip(starts.tolist(), stops.tolist(), strict=True):
            key = str(stock_rule_symbols[start]).upper()
            by_symbol_rules[key] = (
                stock_rule_dates[start:stop],
                stock_rule_values[start:stop],
            )

    print(
        f"[panel] external features loaded path={path} "
        f"features={len(feature_names)} rules={len(rule_names)} market_rows={market_dates.size} symbols={len(by_symbol)}"
    )
    return _ExternalFeatureArrays(
        feature_names=feature_names,
        market_dates=market_dates,
        market_values=market_values,
        by_symbol=by_symbol,
        rule_names=rule_names,
        market_rule_values=market_rule_values,
        by_symbol_rules=by_symbol_rules,
        official_session_dates=official_session_dates,
    )


def _external_frame_to_arrays(
    frame: Any,
    feature_names: list[str],
    *,
    dtype: Any = np.float32,
) -> tuple[np.ndarray, np.ndarray]:
    if frame is None or frame.is_empty():
        return (
            np.empty((0,), dtype="datetime64[ns]"),
            np.empty((0, len(feature_names)), dtype=dtype),
        )
    dates = frame["date"].to_numpy().astype("datetime64[ns]", copy=False)
    if feature_names:
        values = (
            frame.select(
                [
                    pl.col(name).cast(pl.Float64, strict=False).fill_null(float("nan"))
                    for name in feature_names
                ]
            )
            .to_numpy()
            .astype(dtype, copy=False)
        )
    else:
        values = np.empty((int(dates.size), 0), dtype=dtype)
    valid_dates = ~np.isnat(dates)
    if not bool(valid_dates.all()):
        dates = dates[valid_dates]
        values = values[valid_dates]
    return dates, values


def _align_external_values(
    panel_dates: np.ndarray,
    source_dates: np.ndarray,
    source_values: np.ndarray,
) -> np.ndarray:
    feature_count = int(source_values.shape[1]) if source_values.ndim == 2 else 0
    output_dtype = (
        source_values.dtype
        if source_values.ndim == 2 and np.issubdtype(source_values.dtype, np.floating)
        else np.float64
    )
    out = np.full(
        (int(panel_dates.size), feature_count),
        np.nan,
        dtype=output_dtype,
    )
    if panel_dates.size == 0 or source_dates.size == 0 or feature_count == 0:
        return out
    row_idx = np.searchsorted(panel_dates, source_dates)
    in_bounds = (row_idx >= 0) & (row_idx < int(panel_dates.size))
    valid = np.zeros(source_dates.shape, dtype=bool)
    if bool(in_bounds.any()):
        valid[in_bounds] = panel_dates[row_idx[in_bounds]] == source_dates[in_bounds]
    if bool(valid.any()):
        out[row_idx[valid]] = source_values[valid]
    return out


def _overlay_external_values(
    target: np.ndarray,
    panel_dates: np.ndarray,
    source_dates: np.ndarray,
    source_values: np.ndarray,
) -> None:
    if target.size == 0 or panel_dates.size == 0 or source_dates.size == 0 or source_values.size == 0:
        return
    row_idx = np.searchsorted(panel_dates, source_dates)
    in_bounds = (row_idx >= 0) & (row_idx < int(panel_dates.size))
    if not bool(in_bounds.any()):
        return
    candidate_rows = row_idx[in_bounds]
    source_pos = np.nonzero(in_bounds)[0]
    exact = panel_dates[candidate_rows] == source_dates[source_pos]
    if not bool(exact.any()):
        return
    target_rows = candidate_rows[exact]
    values = source_values[source_pos[exact]]
    finite = np.isfinite(values)
    if bool(finite.all()):
        target[target_rows, :] = values
        return
    if not bool(finite.any()):
        return
    value_rows, value_cols = np.nonzero(finite)
    target[target_rows[value_rows], value_cols] = values[value_rows, value_cols]


_POINT_IN_TIME_STATE_FEATURE_PREFIXES = (
    "twpub_monthly_revenue_",
    "twpub_financial_",
    "twpub_company_",
    "twpub_tdcc_",
    "twpub_cbc_",
    "twpub_dgbas_",
    "twpub_mof_",
)
_POINT_IN_TIME_STATE_FEATURES = {
    "twpub_insider_holdings_log",
    "twpub_insider_pledge_ratio",
}


def _is_point_in_time_state_feature(name: str) -> bool:
    return name in _POINT_IN_TIME_STATE_FEATURES or name.startswith(
        _POINT_IN_TIME_STATE_FEATURE_PREFIXES
    )


def _seed_point_in_time_features_before_panel_start(
    target: np.ndarray,
    feature_names: list[str],
    panel_dates: np.ndarray,
    source_dates: np.ndarray,
    source_values: np.ndarray,
) -> None:
    """Restore the last known PIT state before an early-materialized horizon."""

    if (
        target.ndim != 2
        or target.shape[0] == 0
        or panel_dates.size == 0
        or source_dates.size == 0
        or source_values.ndim != 2
    ):
        return
    cutoff = int(np.searchsorted(source_dates, panel_dates[0], side="left"))
    if cutoff <= 0:
        return
    prior = source_values[:cutoff]
    for col_idx, name in enumerate(feature_names):
        if not _is_point_in_time_state_feature(name):
            continue
        valid = np.flatnonzero(np.isfinite(prior[:, col_idx]))
        if valid.size:
            target[0, col_idx] = prior[int(valid[-1]), col_idx]


def _forward_fill_point_in_time_features(values: np.ndarray, feature_names: list[str]) -> None:
    if values.ndim != 2 or values.shape[0] == 0:
        return
    for col_idx, name in enumerate(feature_names):
        if not _is_point_in_time_state_feature(name):
            continue
        column = values[:, col_idx]
        valid = np.flatnonzero(np.isfinite(column))
        if valid.size == 0:
            continue
        last_seen = np.maximum.accumulate(np.where(np.isfinite(column), np.arange(column.size), -1))
        fill_rows = (last_seen >= 0) & ~np.isfinite(column)
        column[fill_rows] = column[last_seen[fill_rows]]


def _contiguous_indexer(indices: list[int]) -> Any:
    if not indices:
        return slice(0, 0)
    start = int(indices[0])
    stop = start + len(indices)
    if indices == list(range(start, stop)):
        return slice(start, stop)
    return indices


def _normalize_security_filter(value: str | None) -> str:
    normalized = str(value or "none").strip().lower()
    if normalized in {"", "off", "false"}:
        normalized = "none"
    if normalized not in {"none", BROKER_TRADABLE_SECURITY_FILTER}:
        raise ValueError(
            "security_filter must be one of: none, broker_tradable; "
            f"got {value!r}"
        )
    return normalized


def _repo_fallback_us_symbols_path() -> Path:
    return Path(__file__).resolve().parents[2] / "configs" / "fallback_us_stocks_symbols.csv"


def _security_filter_metadata_paths(parquet_root: Path, security_filter: str) -> list[Path]:
    if security_filter != BROKER_TRADABLE_SECURITY_FILTER:
        return []
    candidates = [parquet_root / "symbols.csv", _repo_fallback_us_symbols_path()]
    return [path for path in candidates if path.exists()]


def _metadata_name_is_informative(symbol: str, name: str) -> bool:
    clean_name = str(name or "").strip().upper()
    if not clean_name:
        return False
    clean_symbol = str(symbol or "").strip().upper()
    base_symbol = normalize_us_symbol_key(clean_symbol)
    return clean_name not in {clean_symbol, base_symbol, f"{base_symbol}_DL"}


def _add_us_security_metadata(
    metadata: dict[str, _SymbolSecurityMetadata],
    *,
    code: str,
    yahoo_symbol: str,
    name: str,
    market: str,
) -> None:
    keys = {
        str(code or "").strip().upper(),
        normalize_us_symbol_key(code),
        str(yahoo_symbol or "").strip().upper(),
        normalize_us_symbol_key(yahoo_symbol),
    }
    entry = _SymbolSecurityMetadata(name=str(name or "").strip(), market=str(market or "").strip())
    new_is_informative = any(_metadata_name_is_informative(key, entry.name) for key in keys)
    for key in {key for key in keys if key}:
        existing = metadata.get(key)
        if existing is None:
            metadata[key] = entry
            continue
        existing_is_informative = _metadata_name_is_informative(key, existing.name)
        if new_is_informative and not existing_is_informative:
            metadata[key] = entry


def _load_us_security_metadata(paths: list[Path]) -> dict[str, _SymbolSecurityMetadata]:
    metadata: dict[str, _SymbolSecurityMetadata] = {}
    for path in paths:
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    _add_us_security_metadata(
                        metadata,
                        code=row.get("code", ""),
                        yahoo_symbol=row.get("yahoo_symbol", ""),
                        name=row.get("name", ""),
                        market=row.get("market", ""),
                    )
        except OSError:
            continue
    return metadata


def _filter_us_broker_tradable_paths(
    parquet_root: Path,
    parquet_paths: list[Path],
    metadata_paths: list[Path],
) -> list[Path]:
    metadata = _load_us_security_metadata(metadata_paths)
    kept: list[Path] = []
    pruned_reasons: dict[str, int] = {}
    for path in parquet_paths:
        symbol = _symbol_name_from_path(path)
        info = metadata.get(symbol.upper()) or metadata.get(normalize_us_symbol_key(symbol))
        name = info.name if info is not None else symbol
        market = info.market if info is not None else ("us_delisted" if symbol.upper().endswith("_DL") else "us_stocks")
        reason = us_broker_untradable_reason(symbol, name, market)
        if reason is None:
            kept.append(path)
            continue
        pruned_reasons[reason] = pruned_reasons.get(reason, 0) + 1

    if pruned_reasons:
        details = ", ".join(f"{reason}={count}" for reason, count in sorted(pruned_reasons.items()))
        print(
            f"[panel] security_filter=broker_tradable pruned "
            f"{sum(pruned_reasons.values())} {parquet_root.name} symbols ({details})"
        )
    return kept


def _is_usd_trading_pair(path: Path) -> bool:
    return _symbol_name_from_path(path).upper().endswith("USD")


def _tw_tick_size(
    price: np.ndarray,
    dates: np.ndarray | None = None,
) -> np.ndarray:
    """Regular-equity tick size under the TW rule active on each session."""
    if _panel_numba is not None:
        return _panel_numba.tw_tick_size(price, dates)
    from stockagent.data.tw_price_rules import tick_size_numpy

    return tick_size_numpy(price, dates)


def _to_float_array(values: Any, rows: int | None = None, default: float = np.nan) -> np.ndarray:
    if values is None:
        if rows is None:
            return np.asarray([], dtype=np.float64)
        return np.full(int(rows), default, dtype=np.float64)
    if pl is not None and isinstance(values, pl.Series):
        values = values.to_numpy()
    arr = np.asarray(values)
    try:
        return arr.astype(np.float64, copy=False)
    except (TypeError, ValueError):
        out = np.full(arr.shape, default, dtype=np.float64)
        flat = out.reshape(-1)
        for idx, value in enumerate(arr.reshape(-1)):
            try:
                flat[idx] = float(value)
            except (TypeError, ValueError):
                flat[idx] = default
        return out


def _frame_height(frame: Any) -> int:
    if pl is not None and isinstance(frame, pl.DataFrame):
        return int(frame.height)
    return int(len(frame))


def _frame_column_float_array(frame: Any, name: str, *, default: float = np.nan) -> np.ndarray:
    rows = _frame_height(frame)
    if name not in frame.columns:
        return np.full(rows, default, dtype=np.float64)
    if pl is not None and isinstance(frame, pl.DataFrame):
        return frame.get_column(name).cast(pl.Float64, strict=False).to_numpy()
    return _to_float_array(frame[name], rows=rows, default=default)


def _frame_column_bool_array(frame: Any, name: str, *, default: bool = False) -> np.ndarray:
    rows = _frame_height(frame)
    if name not in frame.columns:
        return np.full(rows, default, dtype=bool)
    if pl is not None and isinstance(frame, pl.DataFrame):
        return frame.get_column(name).cast(pl.Boolean, strict=False).fill_null(default).to_numpy()
    return np.asarray(frame[name], dtype=bool)


def _tw_limit_price(
    prev_close: np.ndarray,
    ratio: float,
    dates: np.ndarray | None = None,
) -> np.ndarray:
    """Compute dated TW daily limit price from a reference price.

    TW limit-up prices are rounded down to the nearest tick, while limit-down
    prices are rounded up to the nearest tick. The asymmetry matters around
    half-tick boundaries, e.g. prev_close=524 -> theoretical down=471.6 ->
    limit-down=472.0.  Historical rows use the pre-2005 tick buckets and the
    pre-2015 7% limit rather than projecting today's rules backward.
    """
    if _panel_numba is not None:
        return _panel_numba.tw_limit_price(prev_close, ratio, dates)
    from stockagent.data.tw_price_rules import limit_price_numpy

    return limit_price_numpy(_to_float_array(prev_close), ratio, dates)


def _frame_date_array(frame: Any) -> np.ndarray | None:
    if "date" not in frame.columns:
        return None
    if pl is not None and isinstance(frame, pl.DataFrame):
        values = frame.get_column("date").to_numpy()
    else:
        values = np.asarray(frame["date"])
    try:
        return np.asarray(values).astype("datetime64[D]", copy=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("TW limit-rule frame dates must be datetime-like") from exc


def _tw_reference_price_for_limits(frame: Any, prev_close_raw: np.ndarray) -> np.ndarray:
    """Compute TW daily reference price used for limit-up/down checks.

    Base rule uses previous close, then applies ex-right/ex-dividend adjustments
    when source columns are available:
    - Dividends: subtract cash dividend on ex-dividend day.
    - Stock Splits: divide by split ratio on ex-right day.
    """
    reference = _to_float_array(prev_close_raw).astype(np.float64, copy=True)

    if "Dividends" in frame.columns:
        dividends = np.nan_to_num(_frame_column_float_array(frame, "Dividends"), nan=0.0)
        reference = reference - dividends

    if "Stock Splits" in frame.columns:
        split_ratio = _frame_column_float_array(frame, "Stock Splits")
        valid_split = np.isfinite(split_ratio) & (split_ratio > 0.0) & (split_ratio != 1.0)
        reference[valid_split] = reference[valid_split] / split_ratio[valid_split]

    reference = np.where(reference > 0.0, reference, np.nan)
    return _round_half_up(reference, decimals=2)


def _compute_tw_limit_masks(frame: Any) -> tuple[np.ndarray, np.ndarray]:
    """Return dated (can_buy, can_sell) masks under TW daily limit rules.

    Rule:
    - limit-up day: cannot buy, can sell
    - limit-down day: can buy, cannot sell
    """
    tradable = _frame_column_bool_array(frame, "tradable")
    close_raw = _round_half_up(_frame_column_float_array(frame, "close_raw"), decimals=2)
    dates = _frame_date_array(frame)
    if _panel_numba is not None:
        dividends = (
            _frame_column_float_array(frame, "Dividends")
            if "Dividends" in frame.columns
            else np.full(tradable.shape, np.nan, dtype=np.float64)
        )
        stock_splits = (
            _frame_column_float_array(frame, "Stock Splits")
            if "Stock Splits" in frame.columns
            else np.full(tradable.shape, np.nan, dtype=np.float64)
        )
        return _panel_numba.tw_limit_masks_from_arrays(
            close_raw,
            tradable,
            dividends,
            stock_splits,
            dates,
        )
    prev_close_raw = _shift_array(close_raw, 1)
    reference_price = _tw_reference_price_for_limits(frame, prev_close_raw)

    limit_up_price = _tw_limit_price(reference_price, 1.10, dates)
    limit_down_price = _tw_limit_price(reference_price, 0.90, dates)

    # Use small price tolerance to absorb source rounding noise.
    is_limit_up = (close_raw >= (limit_up_price - 1e-9)) & (reference_price > 0.0)
    is_limit_down = (close_raw <= (limit_down_price + 1e-9)) & (reference_price > 0.0)

    can_buy = tradable & ~np.nan_to_num(is_limit_up, nan=False).astype(bool)
    can_sell = tradable & ~np.nan_to_num(is_limit_down, nan=False).astype(bool)
    return can_buy, can_sell


def _warn_missing_trading_volume(path: Path) -> None:
    symbol = _symbol_name_from_path(path)
    if symbol in _MISSING_VOLUME_WARNED_SYMBOLS:
        return
    _MISSING_VOLUME_WARNED_SYMBOLS.add(symbol)
    print(
        f"[panel] WARN {path.name}: missing Trading_Volume column; "
        "volume features (trading_volume_logret_1d, signed_vol) will be NaN"
    )


def _polars_datetime_ns_expr(schema: dict[str, Any], column: str = "date") -> Any:
    if pl is None:
        raise RuntimeError("Polars is not available")
    dtype = schema.get(column)
    expr = pl.col(column)
    if dtype == pl.String:
        return expr.str.to_datetime(strict=False).cast(pl.Datetime("ns"), strict=False).alias(column)
    return expr.cast(pl.Datetime("ns"), strict=False).alias(column)


def _prepare_symbol_frame(frame: Any, path: Path) -> Any:
    if pl is None:
        raise RuntimeError("_prepare_symbol_frame requires polars")
    if not isinstance(frame, pl.DataFrame):
        if pa is not None and isinstance(frame, pa.Table):
            frame = pl.from_arrow(frame)
        else:
            frame = pl.DataFrame(frame)
    if "date" not in frame.columns:
        raise ValueError(f"{path.name} is missing required date column")

    price_decimals = _price_decimals_for_path(path)
    adjclose_decimals = _adjclose_decimals_for_path(path)
    max_abs_price_log_return = _max_abs_daily_price_log_return_for_path(path)
    if "bybit_perpetual_contract_version" in frame.columns:
        max_abs_price_log_return = float("inf")

    def num(name: str):
        if name in frame.columns:
            return pl.col(name).cast(pl.Float64, strict=False)
        return pl.lit(None, dtype=pl.Float64)

    frame = (
        frame.with_columns(_polars_datetime_ns_expr(frame.schema, "date"))
        .drop_nulls("date")
        .sort("date")
        .with_columns(
            [
                _polars_round_half_up(num("open"), price_decimals).alias("open"),
                _polars_round_half_up(num("max"), price_decimals).alias("max"),
                _polars_round_half_up(num("min"), price_decimals).alias("min"),
                _polars_round_half_up(num("close"), price_decimals).alias("close"),
                _polars_round_half_up(num("adjclose"), adjclose_decimals).alias("adjclose"),
                pl.lit(_symbol_name_from_path(path)).alias("symbol"),
            ]
        )
        .with_columns(pl.col("close").cast(pl.Float32, strict=False).alias("close_raw"))
    )

    return_price = pl.col(_return_price_column(frame, path))
    return_1d = _polars_price_log_return(
        return_price.shift(-1),
        return_price,
        max_abs_price_log_return,
    )
    if "return_quarantined" in frame.columns:
        return_1d = (
            pl.when(
                pl.col("return_quarantined")
                .cast(pl.Boolean, strict=False)
                .fill_null(False)
            )
            .then(pl.lit(None, dtype=pl.Float64))
            .otherwise(return_1d)
        )
    close_valid = _polars_not_nan_or_null(pl.col("close"))
    if "Trading_Volume" in frame.columns:
        volume = num("Trading_Volume")
        volume_missing = volume.is_null() | volume.is_nan().fill_null(False)
        tradable_expr = close_valid & ((volume.fill_nan(0.0).fill_null(0.0) > 0.0) | volume_missing)
    else:
        _warn_missing_trading_volume(path)
        volume = pl.lit(None, dtype=pl.Float64)
        tradable_expr = close_valid

    frame = frame.with_columns(
        [
            _polars_safe_log(pl.col("close"), pl.col("open")).alias("intraday_return_co"),
            _polars_safe_log(pl.col("open"), pl.col("close").shift(1)).alias("overnight_gap_oc"),
            _polars_safe_log(pl.col("max"), pl.col("min")).alias("intraday_range"),
            *_polars_kbar_ratio_expressions(
                pl.col("open"), pl.col("max"), pl.col("min"), pl.col("close")
            ),
            return_1d.alias("return_1d"),
            _polars_price_log_return(pl.col("open"), pl.col("open").shift(1), max_abs_price_log_return).alias("open_logret_1d"),
            _polars_price_log_return(
                pl.col("open").shift(-1),
                pl.col("close"),
                max_abs_price_log_return,
            ).alias(DAY_TRADE_OPEN_GAP_FEATURE),
            _polars_price_log_return(pl.col("max"), pl.col("max").shift(1), max_abs_price_log_return).alias("max_logret_1d"),
            _polars_price_log_return(pl.col("min"), pl.col("min").shift(1), max_abs_price_log_return).alias("min_logret_1d"),
            _polars_price_log_return(pl.col("close"), pl.col("close").shift(1), max_abs_price_log_return).alias("close_logret_1d"),
            _polars_safe_log(volume, volume.shift(1)).alias("trading_volume_logret_1d"),
            tradable_expr.alias("tradable"),
        ]
    )
    frame = frame.with_columns(
        [
            (pl.col("clv") - 0.5).alias("clv_centered"),
            (pl.col("upper_shadow") - pl.col("lower_shadow")).alias("shadow_imbalance"),
            (pl.col("clv") - pl.col("clv").shift(1)).alias("delta_clv"),
            (pl.col("body_ratio") - pl.col("body_ratio").shift(1)).alias("delta_body_ratio"),
            (pl.col("intraday_return_co").sign() * pl.col("trading_volume_logret_1d")).alias("signed_vol"),
        ]
    )
    if "lifecycle_reset" in frame.columns:
        reset = (
            pl.col("lifecycle_reset")
            .cast(pl.Boolean, strict=False)
            .fill_null(False)
        )
        frame = frame.with_columns(
            [
                pl.when(reset)
                .then(pl.lit(None, dtype=pl.Float64))
                .otherwise(pl.col(name))
                .alias(name)
                for name in (
                    "open_logret_1d",
                    "max_logret_1d",
                    "min_logret_1d",
                    "close_logret_1d",
                    "trading_volume_logret_1d",
                    "signed_vol",
                    "delta_body_ratio",
                    "delta_clv",
                )
            ]
        )
    if "return_quarantined" in frame.columns:
        quarantine = (
            pl.col("return_quarantined")
            .cast(pl.Boolean, strict=False)
            .fill_null(False)
        )
        frame = frame.with_columns(
            pl.when(quarantine)
            .then(pl.lit(None, dtype=pl.Float64))
            .otherwise(pl.col(DAY_TRADE_OPEN_GAP_FEATURE))
            .alias(DAY_TRADE_OPEN_GAP_FEATURE)
        )
    for col in BASE_PANEL_FEATURE_COLUMNS:
        if col not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))
    return frame


def _load_symbol_frame(path: Path) -> Any:
    if pq is None:
        raise RuntimeError("PyArrow is not available")
    from downloader.ohlcv_hot_tail import read_logical_parquet

    return _prepare_symbol_frame(read_logical_parquet(path), path)


def _coerce_arrow_numeric_column(table, name: str, rows: int) -> np.ndarray:
    if name not in table.column_names:
        return np.full(rows, np.nan, dtype=np.float64)
    column = table[name].combine_chunks()
    values = column.to_numpy(zero_copy_only=False)
    try:
        return np.asarray(values, dtype=np.float64)
    except (TypeError, ValueError):
        if pc is not None and pa is not None:
            try:
                casted = pc.cast(column, pa.float64(), safe=False)
                return np.asarray(casted.to_numpy(zero_copy_only=False), dtype=np.float64)
            except Exception:
                pass
        if pl is not None:
            return pl.Series(values).cast(pl.Float64, strict=False).to_numpy()
        return _to_float_array(values, rows=rows)


def _coerce_arrow_datetime_ns_column(table, name: str, rows: int) -> np.ndarray:
    if name not in table.column_names:
        return np.full(rows, np.datetime64("NaT", "ns"), dtype="datetime64[ns]")
    column = table[name].combine_chunks()
    values = column.to_numpy(zero_copy_only=False)
    try:
        return np.asarray(values, dtype="datetime64[ns]")
    except (TypeError, ValueError):
        if pc is not None and pa is not None:
            try:
                casted = pc.cast(column, pa.timestamp("ns"), safe=False)
                return np.asarray(casted.to_numpy(zero_copy_only=False), dtype="datetime64[ns]")
            except Exception:
                pass
        if pl is not None:
            return (
                pl.Series(values)
                .cast(pl.String, strict=False)
                .str.to_datetime(strict=False)
                .to_numpy()
                .astype("datetime64[ns]", copy=False)
            )
        out = np.full(rows, np.datetime64("NaT", "ns"), dtype="datetime64[ns]")
        flat = np.asarray(values).reshape(-1)
        for idx, value in enumerate(flat[:rows]):
            try:
                out[idx] = np.datetime64(str(value), "ns")
            except Exception:
                out[idx] = np.datetime64("NaT", "ns")
        return out


def _shift_array(values: np.ndarray, periods: int) -> np.ndarray:
    if _panel_numba is not None:
        return _panel_numba.shift_array(values, periods)
    arr = np.asarray(values, dtype=np.float64)
    out = np.full(arr.shape, np.nan, dtype=np.float64)
    if periods > 0:
        out[periods:] = arr[:-periods]
    elif periods < 0:
        out[:periods] = arr[-periods:]
    else:
        out[:] = arr
    return out


def _safe_log_ratio_array(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    if _panel_numba is not None:
        return _panel_numba.safe_log_ratio_array(numerator, denominator)
    num = np.asarray(numerator, dtype=np.float64)
    den = np.asarray(denominator, dtype=np.float64)
    out = np.full(num.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(num) & np.isfinite(den) & (num > 0.0) & (den > 0.0)
    np.divide(num, den, out=out, where=valid)
    np.log(out, out=out, where=valid)
    out[~valid] = np.nan
    return out


def _sanitize_price_log_return_array(
    values: np.ndarray,
    max_abs_log_return: float = MAX_ABS_DAILY_PRICE_LOG_RETURN,
) -> np.ndarray:
    if _panel_numba is not None:
        return _panel_numba.sanitize_price_log_return_array(values, max_abs_log_return)
    out = np.asarray(values, dtype=np.float64).copy()
    invalid = np.isfinite(out) & (np.abs(out) > max_abs_log_return)
    out[invalid] = np.nan
    return out


def _polars_safe_log(num, den):
    if pl is None:
        raise RuntimeError("Polars is not available")
    return (
        pl.when(num.is_finite() & den.is_finite() & (num > 0.0) & (den > 0.0))
        .then((num / den).log())
        .otherwise(None)
    )


def _polars_sanitize_price_log_return(
    expr,
    max_abs_log_return: float = MAX_ABS_DAILY_PRICE_LOG_RETURN,
):
    if pl is None:
        raise RuntimeError("Polars is not available")
    return (
        pl.when(expr.is_null() | ~expr.is_finite())
        .then(None)
        .when(expr.abs() > max_abs_log_return)
        .then(None)
        .otherwise(expr)
    )


def _polars_price_log_return(
    num,
    den,
    max_abs_log_return: float = MAX_ABS_DAILY_PRICE_LOG_RETURN,
):
    return _polars_sanitize_price_log_return(
        _polars_safe_log(num, den),
        max_abs_log_return,
    )


def _polars_round_half_up(expr, decimals: int):
    factor = float(10**int(decimals))
    return (
        pl.when(expr.is_null() | expr.is_nan())
        .then(None)
        .when(expr >= 0.0)
        .then(((expr * factor) + 0.5).floor() / factor)
        .otherwise(((expr * factor) - 0.5).ceil() / factor)
    )


def _collect_polars_lazy_frame(lazy, *, engine: str = "auto"):
    engine = str(engine or "auto").strip().lower()
    if engine not in {"auto", "streaming"}:
        raise ValueError(f"Unsupported Polars collect engine: {engine!r}")
    try:
        return lazy.collect(engine=engine)
    except TypeError:
        if engine == "streaming":
            return lazy.collect(streaming=True)
        return lazy.collect()


def _polars_not_nan_or_null(expr):
    return expr.is_not_null() & ~expr.is_nan().fill_null(False)


def _polars_kbar_ratio_expressions(open_px, high_px, low_px, close_px) -> list[Any]:
    finite = (
        _polars_not_nan_or_null(open_px)
        & _polars_not_nan_or_null(high_px)
        & _polars_not_nan_or_null(low_px)
        & _polars_not_nan_or_null(close_px)
    )
    valid_envelope = (
        finite
        & (high_px >= pl.max_horizontal(open_px, close_px))
        & (low_px <= pl.min_horizontal(open_px, close_px))
    )
    spread = high_px - low_px
    valid_range = valid_envelope & (spread > EPSILON)

    def bounded_ratio(numerator, *, flat_value: float, lower: float, upper: float):
        return (
            pl.when(valid_range)
            .then((numerator / spread).clip(lower, upper))
            .when(valid_envelope)
            .then(pl.lit(flat_value, dtype=pl.Float64))
            .otherwise(None)
        )

    return [
        bounded_ratio(
            (close_px - open_px).abs(), flat_value=0.0, lower=0.0, upper=1.0
        ).alias("body_ratio"),
        bounded_ratio(
            close_px - open_px, flat_value=0.0, lower=-1.0, upper=1.0
        ).alias("signed_body_ratio"),
        bounded_ratio(
            close_px - low_px, flat_value=0.5, lower=0.0, upper=1.0
        ).alias("clv"),
        bounded_ratio(
            high_px - pl.max_horizontal(open_px, close_px),
            flat_value=0.0,
            lower=0.0,
            upper=1.0,
        ).alias("upper_shadow"),
        bounded_ratio(
            pl.min_horizontal(open_px, close_px) - low_px,
            flat_value=0.0,
            lower=0.0,
            upper=1.0,
        ).alias("lower_shadow"),
    ]


def _kbar_ratio_arrays(
    open_px: np.ndarray,
    high_px: np.ndarray,
    low_px: np.ndarray,
    close_px: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    shape = np.asarray(open_px).shape
    body_ratio = np.full(shape, np.nan, dtype=np.float64)
    signed_body_ratio = np.full(shape, np.nan, dtype=np.float64)
    clv = np.full(shape, np.nan, dtype=np.float64)
    upper_shadow = np.full(shape, np.nan, dtype=np.float64)
    lower_shadow = np.full(shape, np.nan, dtype=np.float64)

    finite = (
        np.isfinite(open_px)
        & np.isfinite(high_px)
        & np.isfinite(low_px)
        & np.isfinite(close_px)
    )
    valid_envelope = (
        finite
        & (high_px >= np.maximum(open_px, close_px))
        & (low_px <= np.minimum(open_px, close_px))
    )
    spread = high_px - low_px
    valid_range = valid_envelope & (spread > EPSILON)
    flat = valid_envelope & ~valid_range

    body_ratio[valid_range] = np.clip(
        np.abs(close_px[valid_range] - open_px[valid_range]) / spread[valid_range],
        0.0,
        1.0,
    )
    signed_body_ratio[valid_range] = np.clip(
        (close_px[valid_range] - open_px[valid_range]) / spread[valid_range],
        -1.0,
        1.0,
    )
    clv[valid_range] = np.clip(
        (close_px[valid_range] - low_px[valid_range]) / spread[valid_range],
        0.0,
        1.0,
    )
    upper_shadow[valid_range] = np.clip(
        (
            high_px[valid_range]
            - np.maximum(open_px[valid_range], close_px[valid_range])
        )
        / spread[valid_range],
        0.0,
        1.0,
    )
    lower_shadow[valid_range] = np.clip(
        (
            np.minimum(open_px[valid_range], close_px[valid_range])
            - low_px[valid_range]
        )
        / spread[valid_range],
        0.0,
        1.0,
    )

    body_ratio[flat] = 0.0
    signed_body_ratio[flat] = 0.0
    clv[flat] = 0.5
    upper_shadow[flat] = 0.0
    lower_shadow[flat] = 0.0
    return body_ratio, signed_body_ratio, clv, upper_shadow, lower_shadow


def _tw_limit_masks_from_arrays(
    close_raw: np.ndarray,
    tradable: np.ndarray,
    dividends: np.ndarray,
    stock_splits: np.ndarray,
    dates: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    if _panel_numba is not None:
        return _panel_numba.tw_limit_masks_from_arrays(
            close_raw,
            tradable,
            dividends,
            stock_splits,
            dates,
        )
    close = _round_half_up(np.asarray(close_raw, dtype=np.float64), decimals=2)
    prev_close = _shift_array(close, 1)
    reference = np.asarray(prev_close, dtype=np.float64).copy()

    div = np.nan_to_num(np.asarray(dividends, dtype=np.float64), nan=0.0)
    reference = reference - div

    splits = np.asarray(stock_splits, dtype=np.float64)
    valid_split = np.isfinite(splits) & (splits > 0.0) & (splits != 1.0)
    reference[valid_split] = reference[valid_split] / splits[valid_split]
    reference = np.where(reference > 0.0, reference, np.nan)

    limit_up = _tw_limit_price(reference, 1.10, dates).astype(np.float64, copy=False)
    limit_down = _tw_limit_price(reference, 0.90, dates).astype(np.float64, copy=False)

    base = np.asarray(tradable, dtype=bool)
    is_limit_up = np.isfinite(reference) & (close >= (limit_up - 1e-9))
    is_limit_down = np.isfinite(reference) & (close <= (limit_down + 1e-9))
    return base & ~is_limit_up, base & ~is_limit_down


def _tw_open_limit_masks_from_arrays(
    open_raw: np.ndarray,
    close_raw: np.ndarray,
    tradable: np.ndarray,
    dividends: np.ndarray,
    stock_splits: np.ndarray,
    dates: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Return open-side masks using the prior close reference for session t.

    The execution quote is today's open, while the limit reference is derived
    from yesterday's close (with any source-provided ex-right/ex-dividend
    adjustment).  Shifting the open itself would compare against yesterday's
    open and is mathematically the wrong Taiwan price-limit contract.

    Session-t high, low, and close must not affect these masks.  A security
    that opens inside its price band remains eligible to enter even if it
    reaches a limit later in the session.  Conversely, an opening print at the
    upper limit blocks only buy-first entry, while an opening print at the
    lower limit blocks only sell-first entry.
    """

    open_px = _round_half_up(np.asarray(open_raw, dtype=np.float64), decimals=2)
    prior_close = _shift_array(
        _round_half_up(np.asarray(close_raw, dtype=np.float64), decimals=2),
        1,
    )
    reference = np.asarray(prior_close, dtype=np.float64).copy()
    div = np.nan_to_num(np.asarray(dividends, dtype=np.float64), nan=0.0)
    reference = reference - div
    splits = np.asarray(stock_splits, dtype=np.float64)
    valid_split = np.isfinite(splits) & (splits > 0.0) & (splits != 1.0)
    reference[valid_split] = reference[valid_split] / splits[valid_split]
    reference = np.where(reference > 0.0, reference, np.nan)

    limit_up = _tw_limit_price(reference, 1.10, dates).astype(np.float64, copy=False)
    limit_down = _tw_limit_price(reference, 0.90, dates).astype(np.float64, copy=False)
    if np.asarray(tradable).shape != open_px.shape:
        raise ValueError("tradable and open prices must share shape")
    # Do not intersect with the full-session tradable flag: that flag depends
    # on the eventual close and reported daily volume, neither of which exists
    # when the opening order is submitted.
    base = np.isfinite(open_px) & (open_px > 0.0)
    # Legal TW opening prints cannot cross the daily price band, so >= / <= are
    # equivalent to equality for valid data while also failing closed on an
    # invalid vendor print beyond the legal limit.
    is_limit_up = np.isfinite(reference) & (open_px >= (limit_up - 1e-9))
    is_limit_down = np.isfinite(reference) & (open_px <= (limit_down + 1e-9))
    return base & ~is_limit_up, base & ~is_limit_down


def _load_symbol_arrays_pyarrow(
    path: Path,
    tradable_mode: str = "tradable",
    trading_volume_policy: str | bool | None = "auto",
) -> _SymbolPanelArrays:
    if pq is None:
        raise RuntimeError("PyArrow is not available")

    from downloader.ohlcv_hot_tail import read_logical_parquet

    table = read_logical_parquet(path).to_arrow()
    return _symbol_arrays_from_arrow_table(
        table,
        path,
        tradable_mode=tradable_mode,
        trading_volume_policy=trading_volume_policy,
    )


def _read_parquet_tail_table(path: Path, rows: int):
    if pq is None:
        raise RuntimeError("PyArrow is not available")
    tail_rows = max(1, int(rows))
    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    if metadata is None or int(metadata.num_row_groups) <= 0:
        return pq.read_table(path)

    row_groups: list[int] = []
    row_count = 0
    for group_idx in range(int(metadata.num_row_groups) - 1, -1, -1):
        row_groups.append(group_idx)
        row_count += int(metadata.row_group(group_idx).num_rows)
        if row_count >= tail_rows:
            break
    table = parquet_file.read_row_groups(sorted(row_groups))
    if int(table.num_rows) <= tail_rows:
        return table
    offset = int(table.num_rows) - tail_rows
    return table.slice(offset, tail_rows)


def _load_symbol_arrays_pyarrow_tail(
    path: Path,
    *,
    tail_rows: int,
    tradable_mode: str = "tradable",
    trading_volume_policy: str | bool | None = "auto",
) -> _SymbolPanelArrays:
    from downloader.ohlcv_hot_tail import read_logical_parquet

    table = read_logical_parquet(path, tail_rows=tail_rows).to_arrow()
    return _symbol_arrays_from_arrow_table(
        table,
        path,
        tradable_mode=tradable_mode,
        trading_volume_policy=trading_volume_policy,
    )


def _symbol_arrays_from_arrow_table(
    table: Any,
    path: Path,
    *,
    tradable_mode: str = "tradable",
    trading_volume_policy: str | bool | None = "auto",
) -> _SymbolPanelArrays:
    _require_trading_volume_column(path, set(table.column_names), trading_volume_policy)
    rows = int(table.num_rows)
    if rows == 0:
        empty_1d = np.empty((0,), dtype=np.float32)
        empty_mask = np.empty((0,), dtype=bool)
        return _SymbolPanelArrays(
            symbol=_symbol_name_from_path(path),
            dates=np.empty((0,), dtype="datetime64[ns]"),
            features=np.empty((0, len(BASE_PANEL_FEATURE_COLUMNS)), dtype=np.float32),
            returns_1d=empty_1d,
            close_prices=empty_1d,
            open_prices=empty_1d,
            intraday_returns=empty_1d,
            daily_volumes=empty_1d,
            tradable_mask=empty_mask,
            return_valuation_mask=empty_mask,
            can_buy_mask=empty_mask,
            can_sell_mask=empty_mask,
            day_trade_can_buy_open_mask=empty_mask,
            day_trade_can_sell_open_mask=empty_mask,
            alive_mask=empty_mask,
        )

    dates = _coerce_arrow_datetime_ns_column(table, "date", rows)
    order = np.argsort(dates)
    dates = dates[order]

    def col(name: str) -> np.ndarray:
        return _coerce_arrow_numeric_column(table, name, rows)[order]

    price_decimals = _price_decimals_for_path(path)
    adjclose_decimals = _adjclose_decimals_for_path(path)
    max_abs_price_log_return = _max_abs_daily_price_log_return_for_path(path)
    if "bybit_perpetual_contract_version" in table.column_names:
        max_abs_price_log_return = float("inf")
    open_px = _round_half_up(col("open"), decimals=price_decimals)
    high_px = _round_half_up(col("max"), decimals=price_decimals)
    low_px = _round_half_up(col("min"), decimals=price_decimals)
    close_px = _round_half_up(col("close"), decimals=price_decimals)
    execution_px = (
        _round_half_up(col("execution_price"), decimals=price_decimals)
        if "execution_price" in table.column_names
        else close_px
    )
    adjclose = _round_half_up(col("adjclose"), decimals=adjclose_decimals)
    volume = col("Trading_Volume") if "Trading_Volume" in table.column_names else np.full((rows,), np.nan, dtype=np.float64)
    capacity_volume = (
        col("execution_volume_equivalent")
        if "execution_volume_equivalent" in table.column_names
        else volume
    )
    raw_policy_tradable = (
        col("policy_tradable") if "policy_tradable" in table.column_names else None
    )
    raw_execution_available = (
        col("execution_available")
        if "execution_available" in table.column_names
        else None
    )
    eligibility_column = next(
        (name for name in DAY_TRADE_ELIGIBILITY_COLUMNS if name in table.column_names),
        None,
    )
    raw_day_trade_eligible = (
        col(eligibility_column) if eligibility_column is not None else None
    )
    day_trade_eligible = (
        None
        if raw_day_trade_eligible is None
        else np.isfinite(raw_day_trade_eligible) & (raw_day_trade_eligible != 0.0)
    )

    intraday_return_co = _safe_log_ratio_array(close_px, open_px)
    (
        body_ratio,
        signed_body_ratio,
        clv,
        upper_shadow,
        lower_shadow,
    ) = _kbar_ratio_arrays(open_px, high_px, low_px, close_px)
    clv_centered = clv - 0.5
    shadow_imbalance = upper_shadow - lower_shadow
    delta_clv = clv - _shift_array(clv, 1)
    delta_body_ratio = body_ratio - _shift_array(body_ratio, 1)

    return_price = adjclose if "adjclose" in table.column_names else close_px
    return_1d = _safe_log_ratio_array(_shift_array(return_price, -1), return_price)
    open_logret_1d = _safe_log_ratio_array(open_px, _shift_array(open_px, 1))
    # Stored on row t for the decision submitted at open[t+1].  Consequently a
    # normal Taiwan execution window ending at t sees exactly the next open and
    # still cannot see any high/low/close/volume value from session t+1.
    next_session_open_gap_logret = _safe_log_ratio_array(
        _shift_array(open_px, -1),
        close_px,
    )
    max_logret_1d = _safe_log_ratio_array(high_px, _shift_array(high_px, 1))
    min_logret_1d = _safe_log_ratio_array(low_px, _shift_array(low_px, 1))
    close_logret_1d = _safe_log_ratio_array(close_px, _shift_array(close_px, 1))
    return_1d = _sanitize_price_log_return_array(return_1d, max_abs_price_log_return)
    if "return_quarantined" in table.column_names:
        return_quarantined = col("return_quarantined")
        return_1d[np.isfinite(return_quarantined) & (return_quarantined != 0.0)] = np.nan
    open_logret_1d = _sanitize_price_log_return_array(open_logret_1d, max_abs_price_log_return)
    next_session_open_gap_logret = _sanitize_price_log_return_array(
        next_session_open_gap_logret,
        max_abs_price_log_return,
    )
    max_logret_1d = _sanitize_price_log_return_array(max_logret_1d, max_abs_price_log_return)
    min_logret_1d = _sanitize_price_log_return_array(min_logret_1d, max_abs_price_log_return)
    close_logret_1d = _sanitize_price_log_return_array(close_logret_1d, max_abs_price_log_return)
    trading_volume_logret_1d = _safe_log_ratio_array(volume, _shift_array(volume, 1))
    signed_vol = np.sign(intraday_return_co) * trading_volume_logret_1d

    if "lifecycle_reset" in table.column_names:
        raw_reset = col("lifecycle_reset")
        lifecycle_reset = np.isfinite(raw_reset) & (raw_reset != 0.0)
        for values in (
            open_logret_1d,
            max_logret_1d,
            min_logret_1d,
            close_logret_1d,
            trading_volume_logret_1d,
            signed_vol,
            delta_body_ratio,
            delta_clv,
        ):
            values[lifecycle_reset] = np.nan
    if "return_quarantined" in table.column_names:
        raw_quarantine = col("return_quarantined")
        quarantine = np.isfinite(raw_quarantine) & (raw_quarantine != 0.0)
        next_session_open_gap_logret[quarantine] = np.nan

    if "Trading_Volume" not in table.column_names:
        _warn_missing_trading_volume(path)
        trading_volume_logret_1d[:] = np.nan
        signed_vol[:] = np.nan

    feature_map = {
        "open_logret_1d": open_logret_1d,
        DAY_TRADE_OPEN_GAP_FEATURE: next_session_open_gap_logret,
        "max_logret_1d": max_logret_1d,
        "min_logret_1d": min_logret_1d,
        "close_logret_1d": close_logret_1d,
        "trading_volume_logret_1d": trading_volume_logret_1d,
        "signed_vol": signed_vol,
        "body_ratio": body_ratio,
        "signed_body_ratio": signed_body_ratio,
        "delta_body_ratio": delta_body_ratio,
        "clv": clv,
        "clv_centered": clv_centered,
        "delta_clv": delta_clv,
        "upper_shadow": upper_shadow,
        "lower_shadow": lower_shadow,
        "shadow_imbalance": shadow_imbalance,
    }
    features = np.column_stack(
        [feature_map[name] for name in BASE_PANEL_FEATURE_COLUMNS]
    ).astype(np.float32, copy=False)

    close_notna = np.isfinite(execution_px) & (execution_px > 0.0)
    if "Trading_Volume" in table.column_names:
        volume_missing = np.isnan(volume)
        tradable = close_notna & ((np.nan_to_num(volume, nan=0.0) > 0.0) | volume_missing)
    else:
        tradable = close_notna
    if raw_policy_tradable is not None:
        tradable &= np.isfinite(raw_policy_tradable) & (
            raw_policy_tradable != 0.0
        )
    execution_available = close_notna.copy()
    if raw_execution_available is not None:
        execution_available &= np.isfinite(raw_execution_available) & (
            raw_execution_available != 0.0
        )

    valid_dates = ~np.isnat(dates)
    if not bool(valid_dates.all()):
        dates = dates[valid_dates]
        features = features[valid_dates]
        return_1d = return_1d[valid_dates]
        open_px = open_px[valid_dates]
        intraday_return_co = intraday_return_co[valid_dates]
        close_px = close_px[valid_dates]
        execution_px = execution_px[valid_dates]
        volume = volume[valid_dates]
        capacity_volume = capacity_volume[valid_dates]
        tradable = tradable[valid_dates]
        execution_available = execution_available[valid_dates]
        close_notna = close_notna[valid_dates]
        if day_trade_eligible is not None:
            day_trade_eligible = day_trade_eligible[valid_dates]

    tradable = np.asarray(tradable, dtype=bool)
    if tradable_mode == "tw_limit_guard":
        dividends = col("Dividends") if "Dividends" in table.column_names else np.full(tradable.shape, np.nan)
        stock_splits = col("Stock Splits") if "Stock Splits" in table.column_names else np.full(tradable.shape, np.nan)
        if not bool(valid_dates.all()):
            dividends = dividends[valid_dates]
            stock_splits = stock_splits[valid_dates]
        can_buy_mask, can_sell_mask = _tw_limit_masks_from_arrays(
            close_px,
            tradable,
            dividends,
            stock_splits,
            dates,
        )
        day_trade_can_buy_open_mask, day_trade_can_sell_open_mask = (
            _tw_open_limit_masks_from_arrays(
                open_px,
                close_px,
                tradable,
                dividends,
                stock_splits,
                dates,
            )
        )
    elif tradable_mode == "tradable":
        can_buy_mask = execution_available.copy()
        can_sell_mask = execution_available.copy()
        open_tradable = np.isfinite(open_px) & (open_px > 0.0)
        day_trade_can_buy_open_mask = open_tradable.copy()
        day_trade_can_sell_open_mask = open_tradable.copy()
    else:
        raise RuntimeError(f"Unsupported tradable_mode for PyArrow panel backend: {tradable_mode!r}")
    return _SymbolPanelArrays(
        symbol=_symbol_name_from_path(path),
        dates=dates,
        features=features,
        returns_1d=return_1d.astype(np.float32, copy=False),
        close_prices=execution_px.astype(np.float32, copy=False),
        open_prices=open_px.astype(np.float32, copy=False),
        intraday_returns=intraday_return_co.astype(np.float32, copy=False),
        daily_volumes=capacity_volume.astype(np.float32, copy=False),
        tradable_mask=tradable,
        return_valuation_mask=(
            execution_available.copy()
            if raw_execution_available is not None
            else tradable.copy()
        ),
        can_buy_mask=np.asarray(can_buy_mask, dtype=bool),
        can_sell_mask=np.asarray(can_sell_mask, dtype=bool),
        day_trade_can_buy_open_mask=np.asarray(
            day_trade_can_buy_open_mask, dtype=bool
        ),
        day_trade_can_sell_open_mask=np.asarray(
            day_trade_can_sell_open_mask, dtype=bool
        ),
        alive_mask=np.asarray(close_notna, dtype=bool),
        day_trade_eligible_mask=(
            None
            if day_trade_eligible is None
            else np.asarray(day_trade_eligible, dtype=bool)
        ),
    )


def _load_symbol_arrays_polars_lazy(
    path: Path,
    tradable_mode: str = "tradable",
    *,
    collect_engine: str = "auto",
    trading_volume_policy: str | bool | None = "auto",
) -> _SymbolPanelArrays:
    if pl is None:
        raise RuntimeError("Polars is not available")
    if pq is None:
        raise RuntimeError("PyArrow is not available")

    from downloader.ohlcv_hot_tail import read_logical_parquet

    frame = read_logical_parquet(path)
    lazy = frame.lazy().sort("date")
    schema_names = set(frame.columns)
    _require_trading_volume_column(path, schema_names, trading_volume_policy)
    price_decimals = _price_decimals_for_path(path)
    adjclose_decimals = _adjclose_decimals_for_path(path)
    max_abs_price_log_return = _max_abs_daily_price_log_return_for_path(path)
    if "bybit_perpetual_contract_version" in schema_names:
        max_abs_price_log_return = float("inf")

    def num(name: str):
        if name in schema_names:
            return pl.col(name).cast(pl.Float64, strict=False)
        return pl.lit(None, dtype=pl.Float64)

    eligibility_column = next(
        (name for name in DAY_TRADE_ELIGIBILITY_COLUMNS if name in schema_names),
        None,
    )

    price_columns = [
        _polars_round_half_up(num("open"), price_decimals).alias("_open"),
        _polars_round_half_up(num("max"), price_decimals).alias("_max"),
        _polars_round_half_up(num("min"), price_decimals).alias("_min"),
        _polars_round_half_up(num("close"), price_decimals).alias("_close"),
        _polars_round_half_up(num("adjclose"), adjclose_decimals).alias("_adjclose"),
        _polars_round_half_up(
            num("execution_price") if "execution_price" in schema_names else num("close"),
            price_decimals,
        ).alias("_execution_price"),
        num("Trading_Volume").alias("_volume"),
        (
            num("execution_volume_equivalent")
            if "execution_volume_equivalent" in schema_names
            else num("Trading_Volume")
        ).alias("_capacity_volume"),
        num("policy_tradable").alias("_policy_tradable"),
        num("execution_available").alias("_execution_available"),
    ]
    if tradable_mode == "tw_limit_guard":
        price_columns.extend(
            [
                num("Dividends").alias("_dividends"),
                num("Stock Splits").alias("_stock_splits"),
            ]
        )
    lazy = lazy.with_columns(price_columns)
    return_price = pl.col("_adjclose") if "adjclose" in schema_names else pl.col("_close")
    return_1d = _polars_price_log_return(
        return_price.shift(-1),
        return_price,
        max_abs_price_log_return,
    )
    if "return_quarantined" in schema_names:
        return_1d = (
            pl.when(
                pl.col("return_quarantined")
                .cast(pl.Boolean, strict=False)
                .fill_null(False)
            )
            .then(pl.lit(None, dtype=pl.Float64))
            .otherwise(return_1d)
        )
    close_valid = (
        _polars_not_nan_or_null(pl.col("_close"))
        & _polars_not_nan_or_null(pl.col("_execution_price"))
        & (pl.col("_execution_price") > 0.0)
    )
    if "Trading_Volume" in schema_names:
        volume_missing = pl.col("_volume").is_null() | pl.col("_volume").is_nan().fill_null(False)
        tradable_expr = close_valid & (
            (pl.col("_volume").fill_nan(0.0).fill_null(0.0) > 0.0) | volume_missing
        )
    else:
        tradable_expr = close_valid
    if "policy_tradable" in schema_names:
        tradable_expr = tradable_expr & (
            pl.col("_policy_tradable").is_finite()
            & (pl.col("_policy_tradable") != 0.0)
        )
    execution_available_expr = close_valid
    if "execution_available" in schema_names:
        execution_available_expr = execution_available_expr & (
            pl.col("_execution_available").is_finite()
            & (pl.col("_execution_available") != 0.0)
        )

    lazy = lazy.with_columns(
        [
            _polars_safe_log(pl.col("_close"), pl.col("_open")).alias("intraday_return_co"),
            *_polars_kbar_ratio_expressions(
                pl.col("_open"), pl.col("_max"), pl.col("_min"), pl.col("_close")
            ),
            return_1d.alias("return_1d"),
            _polars_price_log_return(pl.col("_open"), pl.col("_open").shift(1), max_abs_price_log_return).alias("open_logret_1d"),
            _polars_price_log_return(
                pl.col("_open").shift(-1),
                pl.col("_close"),
                max_abs_price_log_return,
            ).alias(DAY_TRADE_OPEN_GAP_FEATURE),
            _polars_price_log_return(pl.col("_max"), pl.col("_max").shift(1), max_abs_price_log_return).alias("max_logret_1d"),
            _polars_price_log_return(pl.col("_min"), pl.col("_min").shift(1), max_abs_price_log_return).alias("min_logret_1d"),
            _polars_price_log_return(pl.col("_close"), pl.col("_close").shift(1), max_abs_price_log_return).alias("close_logret_1d"),
            _polars_safe_log(pl.col("_volume"), pl.col("_volume").shift(1)).alias("trading_volume_logret_1d"),
            tradable_expr.alias("tradable"),
        ]
    )
    lazy = lazy.with_columns(
        [
            (pl.col("clv") - 0.5).alias("clv_centered"),
            (pl.col("upper_shadow") - pl.col("lower_shadow")).alias("shadow_imbalance"),
            (pl.col("clv") - pl.col("clv").shift(1)).alias("delta_clv"),
            (pl.col("body_ratio") - pl.col("body_ratio").shift(1)).alias("delta_body_ratio"),
            (pl.col("intraday_return_co").sign() * pl.col("trading_volume_logret_1d")).alias("signed_vol"),
        ]
    )
    if "lifecycle_reset" in schema_names:
        reset = (
            pl.col("lifecycle_reset")
            .cast(pl.Boolean, strict=False)
            .fill_null(False)
        )
        lazy = lazy.with_columns(
            [
                pl.when(reset)
                .then(pl.lit(None, dtype=pl.Float64))
                .otherwise(pl.col(name))
                .alias(name)
                for name in (
                    "open_logret_1d",
                    "max_logret_1d",
                    "min_logret_1d",
                    "close_logret_1d",
                    "trading_volume_logret_1d",
                    "signed_vol",
                    "delta_body_ratio",
                    "delta_clv",
                )
            ]
        )
    if "return_quarantined" in schema_names:
        quarantine = (
            pl.col("return_quarantined")
            .cast(pl.Boolean, strict=False)
            .fill_null(False)
        )
        lazy = lazy.with_columns(
            pl.when(quarantine)
            .then(pl.lit(None, dtype=pl.Float64))
            .otherwise(pl.col(DAY_TRADE_OPEN_GAP_FEATURE))
            .alias(DAY_TRADE_OPEN_GAP_FEATURE)
        )
    selected_columns = [
        _polars_datetime_ns_expr(frame.schema, "date"),
        pl.col("_open").alias("open_px"),
        pl.col("_execution_price").alias("close_px"),
        pl.col("_capacity_volume").alias("daily_volume"),
        pl.col("return_1d"),
        pl.col("intraday_return_co"),
        pl.col("tradable"),
        execution_available_expr.alias("execution_available"),
        *[pl.col(name) for name in BASE_PANEL_FEATURE_COLUMNS],
    ]
    if eligibility_column is not None:
        selected_columns.append(
            num(eligibility_column).alias("day_trade_eligible")
        )
    if tradable_mode == "tw_limit_guard":
        selected_columns[3:3] = [
            pl.col("_dividends").alias("dividends"),
            pl.col("_stock_splits").alias("stock_splits"),
        ]
    out = _collect_polars_lazy_frame(lazy.select(selected_columns), engine=collect_engine)

    rows = int(out.height)
    if rows == 0:
        empty_1d = np.empty((0,), dtype=np.float32)
        empty_mask = np.empty((0,), dtype=bool)
        return _SymbolPanelArrays(
            symbol=_symbol_name_from_path(path),
            dates=np.empty((0,), dtype="datetime64[ns]"),
            features=np.empty((0, len(BASE_PANEL_FEATURE_COLUMNS)), dtype=np.float32),
            returns_1d=empty_1d,
            close_prices=empty_1d,
            open_prices=empty_1d,
            intraday_returns=empty_1d,
            daily_volumes=empty_1d,
            tradable_mask=empty_mask,
            return_valuation_mask=empty_mask,
            can_buy_mask=empty_mask,
            can_sell_mask=empty_mask,
            day_trade_can_buy_open_mask=empty_mask,
            day_trade_can_sell_open_mask=empty_mask,
            alive_mask=empty_mask,
        )

    dates = out["date"].to_numpy().astype("datetime64[ns]", copy=False)
    open_px = out["open_px"].to_numpy().astype(np.float64, copy=False)
    close_px = out["close_px"].to_numpy().astype(np.float64, copy=False)
    daily_volume = out["daily_volume"].to_numpy().astype(np.float64, copy=False)
    return_1d = out["return_1d"].to_numpy().astype(np.float64, copy=False)
    intraday_return_co = out["intraday_return_co"].to_numpy().astype(
        np.float64, copy=False
    )
    raw_day_trade_eligible = (
        None
        if eligibility_column is None
        else out["day_trade_eligible"].to_numpy().astype(np.float64, copy=False)
    )
    day_trade_eligible = (
        None
        if raw_day_trade_eligible is None
        else np.isfinite(raw_day_trade_eligible) & (raw_day_trade_eligible != 0.0)
    )
    tradable = out["tradable"].to_numpy().astype(bool, copy=False)
    execution_available = out["execution_available"].to_numpy().astype(
        bool, copy=False
    )
    features = np.column_stack(
        [
            out[name].to_numpy().astype(np.float64, copy=False)
            for name in BASE_PANEL_FEATURE_COLUMNS
        ]
    ).astype(np.float32, copy=False)
    close_notna = ~np.isnan(close_px)

    valid_dates = ~np.isnat(dates)
    if not bool(valid_dates.all()):
        dates = dates[valid_dates]
        features = features[valid_dates]
        return_1d = return_1d[valid_dates]
        open_px = open_px[valid_dates]
        intraday_return_co = intraday_return_co[valid_dates]
        close_px = close_px[valid_dates]
        daily_volume = daily_volume[valid_dates]
        tradable = tradable[valid_dates]
        execution_available = execution_available[valid_dates]
        close_notna = close_notna[valid_dates]
        if day_trade_eligible is not None:
            day_trade_eligible = day_trade_eligible[valid_dates]

    if tradable_mode == "tw_limit_guard":
        dividends = out["dividends"].to_numpy().astype(np.float64, copy=False)
        stock_splits = out["stock_splits"].to_numpy().astype(np.float64, copy=False)
        if not bool(valid_dates.all()):
            dividends = dividends[valid_dates]
            stock_splits = stock_splits[valid_dates]
        can_buy_mask, can_sell_mask = _tw_limit_masks_from_arrays(
            close_px,
            tradable,
            dividends,
            stock_splits,
            dates,
        )
        day_trade_can_buy_open_mask, day_trade_can_sell_open_mask = (
            _tw_open_limit_masks_from_arrays(
                open_px,
                close_px,
                tradable,
                dividends,
                stock_splits,
                dates,
            )
        )
    elif tradable_mode == "tradable":
        can_buy_mask = execution_available.copy()
        can_sell_mask = execution_available.copy()
        open_tradable = np.isfinite(open_px) & (open_px > 0.0)
        day_trade_can_buy_open_mask = open_tradable.copy()
        day_trade_can_sell_open_mask = open_tradable.copy()
    else:
        raise RuntimeError(f"Unsupported tradable_mode for Polars Lazy panel backend: {tradable_mode!r}")

    return _SymbolPanelArrays(
        symbol=_symbol_name_from_path(path),
        dates=dates,
        features=features,
        returns_1d=return_1d.astype(np.float32, copy=False),
        close_prices=close_px.astype(np.float32, copy=False),
        open_prices=open_px.astype(np.float32, copy=False),
        intraday_returns=intraday_return_co.astype(np.float32, copy=False),
        daily_volumes=daily_volume.astype(np.float32, copy=False),
        tradable_mask=np.asarray(tradable, dtype=bool),
        return_valuation_mask=np.asarray(
            execution_available
            if "execution_available" in schema_names
            else tradable,
            dtype=bool,
        ),
        can_buy_mask=np.asarray(can_buy_mask, dtype=bool),
        can_sell_mask=np.asarray(can_sell_mask, dtype=bool),
        day_trade_can_buy_open_mask=np.asarray(
            day_trade_can_buy_open_mask, dtype=bool
        ),
        day_trade_can_sell_open_mask=np.asarray(
            day_trade_can_sell_open_mask, dtype=bool
        ),
        alive_mask=np.asarray(close_notna, dtype=bool),
        day_trade_eligible_mask=(
            None
            if day_trade_eligible is None
            else np.asarray(day_trade_eligible, dtype=bool)
        ),
    )


def _build_panel_from_symbol_arrays(
    symbol_arrays: list[_SymbolPanelArrays],
    benchmark_name: str = "universe_average_return",
    external_features: _ExternalFeatureArrays | None = None,
    feature_include: tuple[str, ...] = (),
    feature_exclude: tuple[str, ...] = (),
) -> PanelData:
    if not symbol_arrays:
        raise RuntimeError("No valid parquet files could be loaded.")

    symbols = [item.symbol for item in symbol_arrays]
    benchmark_symbol_index = _resolve_benchmark_index(symbols, benchmark_name)
    dated_items = [item.dates for item in symbol_arrays if item.dates.size]
    if not dated_items:
        raise RuntimeError("No valid dated rows could be loaded.")
    if (
        benchmark_symbol_index is not None
        and symbol_arrays[benchmark_symbol_index].dates.size
    ):
        # Lookback is defined in exchange sessions, not arbitrary rows. A union
        # calendar admits vendor outliers on exchange holidays and silently
        # shortens every affected model window. The configured benchmark is the
        # canonical session calendar; symbols observed off-calendar are ignored.
        all_dates = np.unique(symbol_arrays[benchmark_symbol_index].dates)
    else:
        all_dates = np.unique(np.concatenate(dated_items))
    if external_features is not None and external_features.official_session_dates.size:
        # Preserve receipt-verified sessions even when every symbol quote is
        # absent (for example historical Saturday sessions).  Limit the marker
        # to the observed panel span so TAIEX's earlier archive does not extend
        # the configured universe outside its own data range.
        official_dates = np.unique(external_features.official_session_dates)
        observed_start = all_dates.min()
        observed_end = all_dates.max()
        official_dates = official_dates[
            (official_dates >= observed_start) & (official_dates <= observed_end)
        ]
        if official_dates.size:
            all_dates = np.unique(np.concatenate([all_dates, official_dates]))
    all_dates.sort()
    num_dates = int(all_dates.size)
    num_symbols = len(symbol_arrays)
    session_dates = all_dates
    all_base_feature_names = list(BASE_PANEL_FEATURE_COLUMNS)
    all_external_feature_names = list(external_features.feature_names) if external_features is not None else []
    # The next-session open is legal only when the caller deliberately opts in
    # by exact name.  In particular, an empty include and broad wildcards retain
    # the historical close-complete schema rather than silently acquiring a
    # later-availability execution feature.
    effective_feature_exclude = tuple(feature_exclude)
    if DAY_TRADE_OPEN_GAP_FEATURE not in feature_include:
        effective_feature_exclude = (
            *effective_feature_exclude,
            DAY_TRADE_OPEN_GAP_FEATURE,
        )
    (
        base_feature_indices,
        base_dest_indices,
        external_feature_indices,
        external_dest_indices,
        feature_names,
    ) = _resolve_panel_feature_indices(
        all_base_feature_names,
        all_external_feature_names,
        feature_include=feature_include,
        feature_exclude=effective_feature_exclude,
    )
    num_base_features = len(base_feature_indices)
    num_external_features = len(external_feature_indices)
    num_features = len(feature_names)
    base_source_indexer = _contiguous_indexer(base_feature_indices)
    base_dest_indexer = _contiguous_indexer(base_dest_indices)
    external_source_indexer = _contiguous_indexer(external_feature_indices)
    external_dest_indexer = _contiguous_indexer(external_dest_indices)
    external_dest_is_slice = isinstance(external_dest_indexer, slice)
    selected_external_feature_names = [all_external_feature_names[idx] for idx in external_feature_indices]
    total_available_features = len(all_base_feature_names) + len(all_external_feature_names)
    if num_features != total_available_features:
        print(
            f"[panel] feature filter kept {num_features}/{total_available_features} "
            f"(include={list(feature_include) or ['*']}, exclude={list(feature_exclude) or []})"
        )
    print(
        f"[panel] materializing panel dates={num_dates} symbols={num_symbols} "
        f"features={num_features} base_features={num_base_features} external_features={num_external_features}"
    )

    features = np.full((num_dates, num_symbols, num_features), np.nan, dtype=np.float32)
    returns_1d = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    open_prices = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    close_prices = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    intraday_returns = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    daily_volumes = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    tradable_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    can_buy_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    can_sell_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    day_trade_can_buy_open_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    day_trade_can_sell_open_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    alive_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    has_day_trade_eligibility = any(
        item.day_trade_eligible_mask is not None for item in symbol_arrays
    )
    day_trade_eligible_mask = (
        np.zeros((num_dates, num_symbols), dtype=bool)
        if has_day_trade_eligibility
        else None
    )
    masked_non_session_returns = 0

    market_external = None
    if external_features is not None and num_external_features:
        market_values = external_features.market_values[:, external_source_indexer]
        market_external = _align_external_values(
            all_dates,
            external_features.market_dates,
            market_values,
        )
        _seed_point_in_time_features_before_panel_start(
            market_external,
            selected_external_feature_names,
            all_dates,
            external_features.market_dates,
            market_values,
        )
        _forward_fill_point_in_time_features(
            market_external,
            selected_external_feature_names,
        )

    for sym_idx, item in enumerate(symbol_arrays):
        if item.dates.size == 0:
            continue
        row_idx = np.searchsorted(all_dates, item.dates)
        in_bounds = (row_idx >= 0) & (row_idx < num_dates)
        valid = np.zeros(item.dates.shape, dtype=bool)
        if bool(in_bounds.any()):
            valid[in_bounds] = all_dates[row_idx[in_bounds]] == item.dates[in_bounds]
        all_valid = bool(valid.all())
        if not all_valid:
            row_idx = row_idx[valid]
        item_features = item.features if all_valid else item.features[valid]
        item_dates = item.dates if all_valid else item.dates[valid]
        item_returns = item.returns_1d if all_valid else item.returns_1d[valid]
        item_return_valuation = (
            item.return_valuation_mask
            if all_valid
            else item.return_valuation_mask[valid]
        )
        item_tradable = item.tradable_mask if all_valid else item.tradable_mask[valid]
        symbol_features = features[:, sym_idx, :]
        if num_base_features:
            if isinstance(base_dest_indexer, slice):
                symbol_features[row_idx, base_dest_indexer] = item_features[:, base_source_indexer]
            else:
                symbol_features[np.ix_(row_idx, base_dest_indices)] = item_features[:, base_source_indexer]
        if market_external is not None:
            symbol_features[:, external_dest_indexer] = market_external
        if external_features is not None and num_external_features:
            symbol_external = external_features.by_symbol.get(str(item.symbol).upper())
            if symbol_external is not None:
                symbol_values = symbol_external[1][:, external_source_indexer]
                target = symbol_features[:, external_dest_indexer]
                _seed_point_in_time_features_before_panel_start(
                    target,
                    selected_external_feature_names,
                    all_dates,
                    symbol_external[0],
                    symbol_values,
                )
                _overlay_external_values(target, all_dates, symbol_external[0], symbol_values)
                _forward_fill_point_in_time_features(target, selected_external_feature_names)
                if not external_dest_is_slice:
                    symbol_features[:, external_dest_indexer] = target
        # A one-day label must end on the next market session and on an
        # executable quote. Per-symbol shift(-1) alone can bridge a halt or a
        # split/reduction gap and turn a multi-session price discontinuity into
        # a fictitious next-day return.
        valid_forward = np.zeros(item_returns.shape, dtype=bool)
        if item_returns.size > 1:
            session_idx = np.searchsorted(session_dates, item_dates)
            safe_session_idx = np.minimum(session_idx, session_dates.size - 1)
            on_session = (
                (session_idx >= 0)
                & (session_idx < session_dates.size)
                & (session_dates[safe_session_idx] == item_dates)
            )
            valid_forward[:-1] = (
                on_session[:-1]
                & on_session[1:]
                & np.asarray(item_return_valuation, dtype=bool)[1:]
                & (session_idx[1:] == session_idx[:-1] + 1)
            )
        masked_non_session_returns += int(
            np.count_nonzero(np.isfinite(item_returns) & ~valid_forward)
        )
        session_returns = np.asarray(item_returns, dtype=np.float32).copy()
        session_returns[~valid_forward] = np.nan
        returns_1d[row_idx, sym_idx] = session_returns
        open_prices[row_idx, sym_idx] = (
            item.open_prices if all_valid else item.open_prices[valid]
        )
        close_prices[row_idx, sym_idx] = item.close_prices if all_valid else item.close_prices[valid]
        intraday_returns[row_idx, sym_idx] = (
            item.intraday_returns if all_valid else item.intraday_returns[valid]
        )
        daily_volumes[row_idx, sym_idx] = item.daily_volumes if all_valid else item.daily_volumes[valid]
        tradable_mask[row_idx, sym_idx] = item_tradable
        can_buy_mask[row_idx, sym_idx] = item.can_buy_mask if all_valid else item.can_buy_mask[valid]
        can_sell_mask[row_idx, sym_idx] = item.can_sell_mask if all_valid else item.can_sell_mask[valid]
        day_trade_can_buy_open_mask[row_idx, sym_idx] = (
            item.day_trade_can_buy_open_mask
            if all_valid
            else item.day_trade_can_buy_open_mask[valid]
        )
        day_trade_can_sell_open_mask[row_idx, sym_idx] = (
            item.day_trade_can_sell_open_mask
            if all_valid
            else item.day_trade_can_sell_open_mask[valid]
        )
        alive_mask[row_idx, sym_idx] = item.alive_mask if all_valid else item.alive_mask[valid]
        if day_trade_eligible_mask is not None and item.day_trade_eligible_mask is not None:
            day_trade_eligible_mask[row_idx, sym_idx] = (
                item.day_trade_eligible_mask
                if all_valid
                else item.day_trade_eligible_mask[valid]
            )
        if (sym_idx + 1) % 500 == 0 or (sym_idx + 1) == num_symbols:
            print(f"[panel] materialized symbols {sym_idx + 1}/{num_symbols}")

    if masked_non_session_returns:
        print(
            "[panel] masked non-session/non-executable forward returns "
            f"count={masked_non_session_returns}"
        )

    if benchmark_symbol_index is None:
        valid_returns = np.isfinite(returns_1d)
        n_valid = valid_returns.sum(axis=1)
        sum_ret = np.nansum(np.where(valid_returns, returns_1d, 0.0), axis=1)
        benchmark_returns = np.zeros_like(sum_ret, dtype=np.float32)
        np.divide(sum_ret, n_valid, out=benchmark_returns, where=n_valid > 0)
    else:
        benchmark_returns = np.nan_to_num(
            returns_1d[:, benchmark_symbol_index],
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        ).astype(np.float32, copy=False)

    print("[panel] sanitizing feature NaN/inf values")
    features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0, copy=False)
    return PanelData(
        dates=np.asarray(all_dates, dtype="datetime64[ns]"),
        symbols=symbols,
        feature_names=feature_names,
        features=features,
        returns_1d=returns_1d,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        can_short_open_mask=can_sell_mask.copy(),
        force_short_cover_mask=np.zeros_like(tradable_mask, dtype=bool),
        force_exit_mask=np.zeros_like(tradable_mask, dtype=bool),
        alive_mask=alive_mask,
        benchmark_returns=benchmark_returns,
        close_prices=close_prices,
        daily_volumes=daily_volumes,
        open_prices=open_prices,
        intraday_returns=intraday_returns,
        day_trade_eligible_mask=day_trade_eligible_mask,
        day_trade_can_short_open_mask=None,
        day_trade_can_buy_open_mask=day_trade_can_buy_open_mask,
        day_trade_can_sell_open_mask=day_trade_can_sell_open_mask,
        raw_close_returns_1d=None,
        unresolved_corporate_action_mask=None,
    )


def _attach_raw_close_forward_returns(panel: PanelData) -> PanelData:
    """Attach causal raw-close valuation returns for a Taiwan cash ledger.

    A cash position survives a symbol-specific trading halt.  During a session
    with no new quote it is marked at the last observed raw close; when trading
    resumes, the whole price change since that close is recognized exactly
    once.  This is a valuation convention only: ``panel.close_prices`` remains
    the unfilled execution quote and the side masks still prohibit an order on
    a missing-quote row.

    The scan is O(T*S), the lower bound for materializing the dense return
    tensor, and needs only O(S) temporary state.  A terminal force-exit or an
    official corporate-action avoidance transition clears the carried mark
    after that row.  Consequently a stale value can never bridge a security
    incarnation or an ex-date whose cash/share entitlement is not modeled.  At
    the finite panel horizon, a reconstructable final mark values itself with a
    zero return; this records no unobserved price move while still allowing the
    final row to charge fees and preserve pending settlement claims.
    """

    close = np.asarray(panel.close_prices, dtype=np.float64)
    adjusted = np.asarray(panel.returns_1d)
    tradable = np.asarray(panel.tradable_mask, dtype=bool)
    if close.shape != adjusted.shape or close.shape != tradable.shape:
        raise ValueError(
            "close_prices, returns_1d, and tradable_mask must share [T,S] shape"
        )
    raw = np.full(close.shape, np.nan, dtype=np.float32)
    if close.shape[0] > 0:
        force_exit = (
            np.zeros(close.shape, dtype=bool)
            if panel.force_exit_mask is None
            else np.asarray(panel.force_exit_mask, dtype=bool)
        )
        corporate_action = (
            np.zeros(close.shape, dtype=bool)
            if panel.unresolved_corporate_action_mask is None
            else np.asarray(panel.unresolved_corporate_action_mask, dtype=bool)
        )
        if force_exit.shape != close.shape or corporate_action.shape != close.shape:
            raise ValueError(
                "cash valuation reset masks must share close_prices shape"
            )

        symbols = int(close.shape[1])
        carried_mark = np.full(symbols, np.nan, dtype=np.float64)
        previous_mark = np.full(symbols, np.nan, dtype=np.float64)
        reset_after_previous = np.zeros(symbols, dtype=bool)
        stale_gap_open = np.zeros(symbols, dtype=bool)
        stale_sessions = 0
        resumed_symbols = 0

        for row_index in range(int(close.shape[0])):
            quote = close[row_index]
            quote_valid = np.isfinite(quote) & (quote > 0.0)
            resumed_symbols += int(np.count_nonzero(quote_valid & stale_gap_open))
            stale_gap_open &= ~quote_valid
            carried_mark = np.where(quote_valid, quote, carried_mark)
            current_mark = carried_mark
            stale_now = ~quote_valid & np.isfinite(current_mark)
            stale_gap_open |= stale_now
            stale_sessions += int(np.count_nonzero(stale_now))

            if row_index > 0:
                valid_transition = (
                    np.isfinite(previous_mark)
                    & (previous_mark > 0.0)
                    & np.isfinite(current_mark)
                    & (current_mark > 0.0)
                    & ~reset_after_previous
                )
                if np.any(valid_transition):
                    raw[row_index - 1, valid_transition] = np.log(
                        current_mark[valid_transition]
                        / previous_mark[valid_transition]
                    ).astype(np.float32, copy=False)

            # A reset applies *after* this closing valuation.  The preceding
            # close can therefore still liquidate at its real quote, while no
            # stale mark is allowed to flow into the next security basis.
            reset_now = force_exit[row_index] | corporate_action[row_index]
            previous_mark = current_mark.copy()
            reset_after_previous = reset_now
            carried_mark = np.where(reset_now, np.nan, carried_mark)
            stale_gap_open &= ~reset_now

        terminal_mark_available = (
            np.isfinite(current_mark) & (current_mark > 0.0)
        )
        raw[-1, terminal_mark_available] = np.float32(0.0)

        if stale_sessions:
            print(
                "[panel] cash stale-close valuation "
                f"sessions={stale_sessions} resumed_quotes={resumed_symbols}"
            )
    panel.raw_close_returns_1d = raw
    return panel


def _article76_lunar_new_year_extra_business_days(
    panel_dates: np.ndarray,
    stop_transfer_date: np.datetime64,
) -> int:
    """Return Article 76's extra Lunar New Year business-day count.

    Article 76 ordinarily defines a business day as an exchange trading day.
    Around Lunar New Year it additionally counts one or both of the settlement
    days immediately following the final pre-holiday trading day.  The official
    panel session calendar identifies that holiday as the long January/February
    gap; its first two following weekdays are the two settlement days because
    the exchange deliberately stops trading two settlement days before the
    holiday closure.

    The return value is added to the ordinary ``stop_insertion - 6`` trading-row
    deadline.  A value of zero means the stop-transfer date is outside the
    statutory Lunar New Year exception window.
    """

    sessions = np.asarray(panel_dates, dtype="datetime64[D]")
    stop_date = np.datetime64(stop_transfer_date, "D")
    if sessions.size < 3 or np.isnat(stop_date):
        return 0
    stop_month = int(str(stop_date)[5:7])
    if stop_month not in {1, 2, 3}:
        return 0

    gaps = np.diff(sessions).astype("timedelta64[D]").astype(np.int64)
    for gap_index in np.flatnonzero(gaps >= 7):
        last_trade = sessions[int(gap_index)]
        first_post_trade = sessions[int(gap_index) + 1]
        if int(gap_index) + 2 >= sessions.size:
            continue
        second_post_trade = sessions[int(gap_index) + 2]
        # Exclude missing-data gaps and other long closures.  Every Taiwan
        # Lunar New Year market closure begins and ends in January/February.
        if int(str(last_trade)[5:7]) not in {1, 2} or int(
            str(first_post_trade)[5:7]
        ) not in {1, 2}:
            continue
        first_settlement = np.busday_offset(
            last_trade, 1, roll="forward"
        ).astype("datetime64[D]")
        second_settlement = np.busday_offset(
            last_trade, 2, roll="forward"
        ).astype("datetime64[D]")
        if stop_date < second_settlement or stop_date > second_post_trade:
            continue
        if stop_date == second_settlement:
            return 1
        if stop_date <= first_post_trade:
            return 2
        return 1
    return 0


def _apply_corporate_action_avoidance_transitions(
    panel: PanelData,
    reference: _CorporateActionReference | None,
    official_session_dates: np.ndarray | None = None,
) -> PanelData:
    """Mark the close immediately before every official ex-date.

    The mask is an execution-only safety rule.  The cash executor liquidates at
    that prior close and refuses a new position for the transition, avoiding a
    fabricated dividend/share ledger while keeping raw-price accounting exact.
    """

    if reference is None:
        panel.corporate_action_avoidance_mask = None
        panel.unresolved_corporate_action_mask = None
        return panel
    panel_dates = np.asarray(panel.dates, dtype="datetime64[D]")
    if panel_dates.size == 0:
        panel.corporate_action_avoidance_mask = np.zeros(
            panel.tradable_mask.shape, dtype=bool
        )
        panel.unresolved_corporate_action_mask = np.zeros(
            panel.tradable_mask.shape, dtype=bool
        )
        return panel
    if panel_dates[0] < reference.coverage_start:
        panel.corporate_action_avoidance_mask = None
        panel.unresolved_corporate_action_mask = None
        print(
            "[panel] corporate-action avoidance unavailable: panel begins before "
            "verified archive "
            f"({panel_dates[0]} < {reference.coverage_start}); tw_cash will fail closed"
        )
        return panel
    if panel_dates[-1] > reference.coverage_end:
        panel.corporate_action_avoidance_mask = None
        panel.unresolved_corporate_action_mask = None
        print(
            "[panel] corporate-action avoidance unavailable: panel extends beyond "
            "verified archive "
            f"({panel_dates[-1]} > {reference.coverage_end}); tw_cash will fail closed"
        )
        return panel

    # Counts, rather than a bool mask, let an exact cash event remove only its
    # own avoidance interval if another unresolved event overlaps it.
    avoidance_counts = np.zeros(panel.tradable_mask.shape, dtype=np.int16)
    verified_calendar = np.asarray(
        [] if official_session_dates is None else official_session_dates,
        dtype="datetime64[D]",
    )
    verified_future = verified_calendar[verified_calendar > panel_dates[-1]]
    next_boundary = (
        verified_future.min()
        if verified_future.size
        else np.busday_offset(panel_dates[-1], 1, roll="forward").astype(
            "datetime64[D]"
        )
    )
    symbol_to_index = {
        str(symbol).strip().upper(): idx for idx, symbol in enumerate(panel.symbols)
    }

    can_sell = np.asarray(
        panel.can_sell_mask
        if panel.can_sell_mask is not None
        else panel.tradable_mask,
        dtype=bool,
    )
    can_buy = np.asarray(
        panel.can_buy_mask
        if panel.can_buy_mask is not None
        else panel.tradable_mask,
        dtype=bool,
    )

    def avoidance_start(transition: int, sym_idx: int) -> int:
        # A single shared mask protects both cash longs and margin shorts.
        # Anchor it at the latest close where either position can be flattened,
        # then keep the entry ban active through the last cum-right close.
        executable = np.flatnonzero(
            can_sell[: transition + 1, sym_idx]
            & can_buy[: transition + 1, sym_idx]
            & np.isfinite(panel.close_prices[: transition + 1, sym_idx])
            & (panel.close_prices[: transition + 1, sym_idx] > 0.0)
        )
        return int(executable[-1]) if executable.size else 0

    applied_events = 0
    for symbol, event_dates_ns in reference.event_dates_by_symbol.items():
        sym_idx = symbol_to_index.get(symbol)
        if sym_idx is None:
            continue
        event_dates = np.asarray(event_dates_ns, dtype="datetime64[D]")
        in_horizon = (event_dates > panel_dates[0]) & (
            event_dates <= next_boundary
        )
        if not bool(in_horizon.any()):
            continue
        selected_dates = event_dates[in_horizon]
        # Official annual ex-date archives retain some dates on which the whole
        # market was later closed (for example typhoon holidays).  Search on the
        # verified exchange-session calendar and choose the last session before
        # the declared event date; this is also the last executable close before
        # the eventual adjusted opening and never shifts the liquidation later.
        # We also admit every declared event through the next verified official
        # session after the panel tail.  This includes an ex-date on an
        # intervening whole-market closure: there is no executable close between
        # the panel tail and that next session, so the tail is still the correct
        # transition.  If the calendar has no future row, the fallback is exactly
        # the next weekday; no farther event may collapse across an unobserved
        # intervening business session.
        transition_rows = (
            np.searchsorted(panel_dates, selected_dates, side="left") - 1
        )
        for transition in transition_rows:
            transition = int(transition)
            start = avoidance_start(transition, sym_idx)
            avoidance_counts[start : transition + 1, sym_idx] += 1
            applied_events += 1
    # Preserve the complete interval before exact entitlements remove their
    # own entries from the unresolved-only counter below. Reconstructing this
    # later from a single yield cell is unsafe when the last cum-right close is
    # limit-blocked or halted.
    panel.corporate_action_avoidance_mask = avoidance_counts > 0
    panel.cash_dividend_yield = None
    panel.cash_dividend_payment_delay_sessions = None
    exact_terms = reference.exact_cash_terms_by_symbol
    if exact_terms is not None:
        exact_start = reference.exact_coverage_start
        exact_end = reference.exact_coverage_end
        if exact_start is None or exact_end is None:
            raise RuntimeError("exact entitlement terms are missing coverage bounds")
        if panel_dates[0] < exact_start or panel_dates[-1] > exact_end:
            print(
                "[panel] exact cash entitlements unavailable for this horizon: "
                f"panel={panel_dates[0]}..{panel_dates[-1]} "
                f"archive={exact_start}..{exact_end}"
            )
        else:
            yields = np.zeros(panel.tradable_mask.shape, dtype=np.float32)
            delays = np.zeros(panel.tradable_mask.shape, dtype=np.int32)
            exact_events = 0
            downgraded_events = 0
            for symbol, (event_dates, cash_amounts, payment_dates) in exact_terms.items():
                sym_idx = symbol_to_index.get(symbol)
                if sym_idx is None:
                    continue
                selected = (event_dates > panel_dates[0]) & (
                    event_dates <= next_boundary
                )
                for event_date, cash_amount, payment_date in zip(
                    event_dates[selected],
                    cash_amounts[selected],
                    payment_dates[selected],
                ):
                    transition = int(
                        np.searchsorted(panel_dates, event_date, side="left") - 1
                    )
                    payment_row = int(
                        np.searchsorted(panel_dates, payment_date, side="left")
                    )
                    delay = payment_row - transition
                    if (
                        transition < 0
                        or payment_row >= int(panel_dates.size)
                        or delay < 1
                    ):
                        downgraded_events += 1
                        continue
                    close = float(panel.close_prices[transition, sym_idx])
                    if not np.isfinite(close) or close <= 0.0:
                        downgraded_events += 1
                        continue
                    yields[transition, sym_idx] = np.float32(
                        float(cash_amount) / close
                    )
                    delays[transition, sym_idx] = np.int32(delay)
                    start = avoidance_start(transition, sym_idx)
                    avoidance_counts[start : transition + 1, sym_idx] -= 1
                    if bool(
                        (avoidance_counts[start : transition + 1, sym_idx] < 0).any()
                    ):
                        raise RuntimeError(
                            "exact cash event did not match an official avoidance interval"
                        )
                    exact_events += 1
            panel.cash_dividend_yield = yields
            panel.cash_dividend_payment_delay_sessions = delays
            print(
                "[panel] attached exact cash-entitlement ledger "
                f"events={exact_events} downgraded_to_avoidance={downgraded_events}"
            )
            short_terms = reference.margin_short_stop_transfer_by_symbol or {}
            force_short_cover = np.asarray(
                panel.force_short_cover_mask
                if panel.force_short_cover_mask is not None
                else np.zeros_like(panel.tradable_mask, dtype=bool),
                dtype=bool,
            ).copy()
            can_short_open = np.asarray(
                panel.can_short_open_mask
                if panel.can_short_open_mask is not None
                else can_sell,
                dtype=bool,
            ).copy()
            can_short_open_open = (
                None
                if panel.can_short_open_open_mask is None
                else np.asarray(
                    panel.can_short_open_open_mask,
                    dtype=bool,
                ).copy()
            )
            legal_cover_events = 0
            conservative_cover_events = 0
            known_short_events: dict[str, set[int]] = {}

            def apply_short_cover_rule(
                *, sym_idx: int, deadline: int, ban_end: int
            ) -> bool:
                if deadline < 0 or deadline >= int(panel_dates.size):
                    return False
                executable = np.flatnonzero(
                    can_buy[: deadline + 1, sym_idx]
                    & np.isfinite(panel.close_prices[: deadline + 1, sym_idx])
                    & (panel.close_prices[: deadline + 1, sym_idx] > 0.0)
                )
                # Keep an impossible statutory cover observable.  If a short
                # is actually carried into a deadline with no earlier
                # executable buy, the executor must fail closed there rather
                # than silently dropping the action.
                cover_row = (
                    int(executable[-1]) if executable.size else int(deadline)
                )
                force_short_cover[cover_row, sym_idx] = True
                # When the deadline itself has no executable buy, the account
                # has to cover on the last earlier executable close.  Do not
                # allow a new short in the resulting gap because there would
                # be no second mandatory-cover event.
                can_short_open[
                    cover_row : min(int(ban_end), int(panel_dates.size)), sym_idx
                ] = False
                if can_short_open_open is not None:
                    can_short_open_open[
                        cover_row : min(
                            int(ban_end),
                            int(panel_dates.size),
                        ),
                        sym_idx,
                    ] = False
                return True

            for symbol, (event_dates, stop_transfer_dates) in short_terms.items():
                sym_idx = symbol_to_index.get(symbol)
                if sym_idx is None:
                    continue
                selected = (event_dates > panel_dates[0]) & (
                    event_dates <= next_boundary
                )
                for event_date, stop_transfer_date in zip(
                    event_dates[selected], stop_transfer_dates[selected]
                ):
                    known_short_events.setdefault(symbol, set()).add(
                        int(np.datetime64(event_date, "D").astype(np.int64))
                    )
                        # Article 76: close by the sixth exchange business day
                        # before stop-transfer begins, and prohibit new margin
                        # short sales for four sessions from that day.
                    stop_insertion = int(
                        np.searchsorted(
                            panel_dates, stop_transfer_date, side="left"
                        )
                    )
                    lunar_new_year_extra = (
                        _article76_lunar_new_year_extra_business_days(
                            panel_dates, stop_transfer_date
                        )
                    )
                    deadline = stop_insertion - 6 + lunar_new_year_extra
                    if apply_short_cover_rule(
                        sym_idx=sym_idx,
                        deadline=deadline,
                        ban_end=deadline + 4,
                    ):
                        legal_cover_events += 1

            # MOPS does not cover every ETF or historical complex action.  A
            # reference ex-date still proves the last cum-right close.  Under
            # T+2 entitlement settlement, transition-3 is the ordinary sixth
            # business day before stop-transfer; using it without the Lunar
            # New Year relaxation is no later than the statutory deadline and
            # is therefore a conservative, non-look-ahead short fallback.
            for symbol, event_dates_ns in reference.event_dates_by_symbol.items():
                sym_idx = symbol_to_index.get(symbol)
                if sym_idx is None:
                    continue
                known = known_short_events.get(symbol, set())
                event_dates = np.asarray(event_dates_ns, dtype="datetime64[D]")
                selected_dates = event_dates[
                    (event_dates > panel_dates[0]) & (event_dates <= next_boundary)
                ]
                for event_date in selected_dates:
                    event_day = int(event_date.astype(np.int64))
                    if event_day in known:
                        continue
                    transition = int(
                        np.searchsorted(panel_dates, event_date, side="left") - 1
                    )
                    if transition < 0:
                        continue
                    deadline = max(0, transition - 3)
                    if apply_short_cover_rule(
                        sym_idx=sym_idx,
                        deadline=deadline,
                        ban_end=transition + 1,
                    ):
                        conservative_cover_events += 1

            panel.force_short_cover_mask = force_short_cover
            panel.can_short_open_mask = can_short_open
            panel.can_short_open_open_mask = can_short_open_open
            print(
                "[panel] applied corporate-action margin-short rules "
                f"mops_article76={legal_cover_events} "
                f"conservative_t2_fallback={conservative_cover_events}"
            )
    mask = avoidance_counts > 0
    panel.unresolved_corporate_action_mask = mask
    print(
        "[panel] applied official corporate-action avoidance transitions "
        f"events={applied_events} "
        f"all_transitions={int(panel.corporate_action_avoidance_mask.sum())} "
        f"unresolved_transitions={int(mask.sum())}"
    )
    return panel


def build_tail_panel(
    parquet_root: str | Path,
    *,
    tail_rows: int,
    benchmark_name: str = "universe_average_return",
    usd_only_trading_pairs: bool = False,
    tradable_mode: str = "tradable",
    trading_volume_policy: str | bool | None = "auto",
    security_filter: str | None = "none",
    strict_no_fallback: bool | None = None,
    panel_load_workers: int = 4,
    external_feature_path: str | Path | None = None,
    external_market_symbol: str = DEFAULT_EXTERNAL_MARKET_SYMBOL,
    external_include_features: bool = True,
    external_include_rules: bool = True,
    external_data_required: bool = False,
    feature_include: Any = None,
    feature_exclude: Any = None,
    feature_zero_fill: Any = None,
    feature_shift_next_session: Any = None,
    panel_start_date: str | date | np.datetime64 | None = None,
) -> PanelData:
    """Build a panel from only the last rows of each symbol file for live inference."""
    parquet_root = Path(parquet_root)
    external_feature_path = _resolve_external_data_path(
        external_feature_path,
        include_features=external_include_features,
        include_rules=external_include_rules,
        required=external_data_required,
    )
    corporate_action_paths = _resolve_corporate_action_reference_paths(
        external_feature_path,
        include_rules=external_include_rules,
    )
    corporate_action_reference = _load_corporate_action_reference(
        corporate_action_paths
    )
    feature_include_patterns = _normalize_feature_patterns(feature_include, label="feature_include")
    feature_exclude_patterns = _normalize_feature_patterns(feature_exclude, label="feature_exclude")
    feature_zero_fill_patterns = _normalize_feature_patterns(
        feature_zero_fill, label="feature_zero_fill"
    )
    feature_shift_next_session_patterns = _normalize_feature_patterns(
        feature_shift_next_session, label="feature_shift_next_session"
    )
    normalized_panel_start_date = _normalize_panel_start_date(panel_start_date)
    parquet_paths = sorted(parquet_root.glob(f"*{FEATURE_FILE_SUFFIX}"))
    if not parquet_paths:
        raise FileNotFoundError(f"No parquet files found under {parquet_root}")

    if usd_only_trading_pairs:
        parquet_paths = [path for path in parquet_paths if _is_usd_trading_pair(path)]
        if not parquet_paths:
            raise FileNotFoundError(f"No USD trading pairs found under {parquet_root}")

    security_filter = _normalize_security_filter(security_filter)
    security_metadata_paths = _security_filter_metadata_paths(parquet_root, security_filter)
    if security_filter == BROKER_TRADABLE_SECURITY_FILTER:
        parquet_paths = _filter_us_broker_tradable_paths(parquet_root, parquet_paths, security_metadata_paths)
        if not parquet_paths:
            raise FileNotFoundError(f"No broker-tradable US symbols found under {parquet_root}")

    if strict_no_fallback is None:
        strict_no_fallback = str(os.getenv("STOCKAGENT_STRICT_NO_FALLBACK", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
    else:
        strict_no_fallback = bool(strict_no_fallback)

    tradable_mode = str(tradable_mode).strip().lower()
    trading_volume_policy = _normalize_trading_volume_policy(trading_volume_policy)
    read_rows = max(2, int(tail_rows))
    panel_load_workers = max(0, int(panel_load_workers))
    print(
        f"[panel] building live tail from {len(parquet_paths)} parquet files "
        f"(tail_rows={read_rows}, workers={panel_load_workers})..."
    )

    def _load_one_arrays(path: Path) -> tuple[Path, _SymbolPanelArrays | None, Exception | None]:
        try:
            arrays = _load_symbol_arrays_pyarrow_tail(
                path,
                tail_rows=read_rows,
                tradable_mode=tradable_mode,
                trading_volume_policy=trading_volume_policy,
            )
            if int(arrays.dates.size) == 0:
                raise ValueError(f"Symbol file is empty: {path.name}")
            return path, arrays, None
        except Exception as exc:
            return path, None, exc

    if panel_load_workers > 1 and len(parquet_paths) > 1:
        with ThreadPoolExecutor(max_workers=panel_load_workers) as executor:
            loaded_arrays = list(executor.map(_load_one_arrays, parquet_paths))
    else:
        loaded_arrays = [_load_one_arrays(path) for path in parquet_paths]

    valid_arrays: list[_SymbolPanelArrays] = []
    for path, arrays, exc in loaded_arrays:
        if exc is not None:
            if strict_no_fallback or isinstance(exc, _MissingTradingVolumeError):
                raise type(exc)(f"{path.name}: {exc}") from exc
            print(f"[panel] SKIP {path.name}: {exc}")
            continue
        if arrays is not None:
            valid_arrays.append(arrays)

    external_date_source = np.empty((0,), dtype="datetime64[ns]")
    if valid_arrays:
        benchmark_idx = _resolve_benchmark_index(
            [item.symbol for item in valid_arrays],
            benchmark_name,
        )
        if benchmark_idx is not None and valid_arrays[benchmark_idx].dates.size:
            external_date_source = valid_arrays[benchmark_idx].dates
        else:
            external_date_source = np.concatenate(
                [item.dates for item in valid_arrays if item.dates.size]
            )

    external_features = (
        _load_external_feature_arrays(
            external_feature_path,
            market_symbol=external_market_symbol,
            include_features=external_include_features,
            include_rules=external_include_rules,
            date_start=(
                date.fromisoformat(
                    str(external_date_source.min().astype("datetime64[D]"))
                )
                if external_date_source.size
                else None
            ),
            date_end=(
                date.fromisoformat(
                    str(external_date_source.max().astype("datetime64[D]"))
                )
                if external_date_source.size
                else None
            ),
        )
        if external_feature_path is not None
        else None
    )
    panel = _build_panel_from_symbol_arrays(
        valid_arrays,
        benchmark_name=benchmark_name,
        external_features=external_features,
        feature_include=feature_include_patterns,
        feature_exclude=feature_exclude_patterns,
    )
    panel = _apply_external_rule_masks(panel, external_features)
    panel = _zero_fill_panel_features(panel, feature_zero_fill_patterns)
    panel = _shift_panel_features_to_next_session(
        panel,
        feature_shift_next_session_patterns,
    )
    panel = _slice_panel_start(panel, normalized_panel_start_date)
    panel = _apply_corporate_action_avoidance_transitions(
        panel,
        corporate_action_reference,
        None if external_features is None else external_features.official_session_dates,
    )
    if panel.unresolved_corporate_action_mask is not None:
        panel = _attach_raw_close_forward_returns(panel)
    _print_feature_overview(panel)
    return panel


def load_cached_panel(
    parquet_root: str | Path,
    benchmark_name: str = "universe_average_return",
    usd_only_trading_pairs: bool = False,
    tradable_mode: str = "tradable",
    trading_volume_policy: str | bool | None = "auto",
    security_filter: str | None = "none",
    strict_no_fallback: bool | None = None,
    buy_tradable_mode: str | None = None,
    sell_tradable_mode: str | None = None,
    panel_backend: str = "auto",
    panel_load_workers: int = 4,
    external_feature_path: str | Path | None = None,
    external_market_symbol: str = DEFAULT_EXTERNAL_MARKET_SYMBOL,
    external_include_features: bool = True,
    external_include_rules: bool = True,
    external_data_required: bool = False,
    feature_include: Any = None,
    feature_exclude: Any = None,
    feature_zero_fill: Any = None,
    feature_shift_next_session: Any = None,
    panel_start_date: str | date | np.datetime64 | None = None,
) -> PanelData | None:
    del panel_load_workers
    parquet_root = Path(parquet_root)
    external_feature_path = _resolve_external_data_path(
        external_feature_path,
        include_features=external_include_features,
        include_rules=external_include_rules,
        required=external_data_required,
    )
    corporate_action_paths = _resolve_corporate_action_reference_paths(
        external_feature_path,
        include_rules=external_include_rules,
    )
    _load_corporate_action_reference(corporate_action_paths)
    feature_include_patterns = _normalize_feature_patterns(feature_include, label="feature_include")
    feature_exclude_patterns = _normalize_feature_patterns(feature_exclude, label="feature_exclude")
    feature_zero_fill_patterns = _normalize_feature_patterns(
        feature_zero_fill, label="feature_zero_fill"
    )
    include_day_trade_open_gap = (
        DAY_TRADE_OPEN_GAP_FEATURE in feature_include_patterns
        and DAY_TRADE_OPEN_GAP_FEATURE not in feature_exclude_patterns
    )
    base_feature_include_patterns = tuple(
        pattern
        for pattern in feature_include_patterns
        if pattern != DAY_TRADE_OPEN_GAP_FEATURE
    )
    base_feature_zero_fill_patterns = tuple(
        pattern
        for pattern in feature_zero_fill_patterns
        if pattern != DAY_TRADE_OPEN_GAP_FEATURE
    )
    feature_shift_next_session_patterns = _normalize_feature_patterns(
        feature_shift_next_session, label="feature_shift_next_session"
    )
    normalized_panel_start_date = _normalize_panel_start_date(panel_start_date)
    feature_shift_key = (
        f"feature_shift_next_session={list(feature_shift_next_session_patterns)!r}|"
        if feature_shift_next_session_patterns
        else ""
    )
    parquet_paths = sorted(parquet_root.glob(f"*{FEATURE_FILE_SUFFIX}"))
    if not parquet_paths:
        return None
    if usd_only_trading_pairs:
        parquet_paths = [path for path in parquet_paths if _is_usd_trading_pair(path)]
        if not parquet_paths:
            return None

    security_filter = _normalize_security_filter(security_filter)
    security_metadata_paths = _security_filter_metadata_paths(parquet_root, security_filter)
    if security_filter == BROKER_TRADABLE_SECURITY_FILTER:
        parquet_paths = _filter_us_broker_tradable_paths(parquet_root, parquet_paths, security_metadata_paths)
        if not parquet_paths:
            return None

    panel_backend = str(panel_backend).strip().lower()
    if panel_backend == "pyarrow":
        selected_backend = "pyarrow"
    elif panel_backend in {"polars", "polars_lazy", "polars_streaming"}:
        selected_backend = "polars_streaming" if panel_backend == "polars_streaming" else "polars_lazy"
    elif panel_backend == "auto" and pl is not None and pq is not None:
        selected_backend = "polars_lazy"
    elif panel_backend == "auto" and pq is not None:
        selected_backend = "pyarrow"
    else:
        return None

    if buy_tradable_mode is not None or sell_tradable_mode is not None:
        buy_mode = str(buy_tradable_mode if buy_tradable_mode is not None else tradable_mode).strip().lower()
        sell_mode = str(sell_tradable_mode if sell_tradable_mode is not None else tradable_mode).strip().lower()
        if buy_mode != sell_mode:
            return None
        tradable_mode = buy_mode
    tradable_mode = str(tradable_mode).strip().lower()
    trading_volume_policy = _normalize_trading_volume_policy(trading_volume_policy)
    external_key = str(external_feature_path) if external_feature_path is not None else "none"
    corporate_action_key = (
        (
            f"{corporate_action_paths.parquet}:"
            f"{corporate_action_paths.entitlements_parquet or 'none'}"
        )
        if corporate_action_paths is not None
        else "none"
    )
    backend_key = (
        f"{selected_backend}|benchmark={benchmark_name}|"
        f"usd_only={usd_only_trading_pairs}|tradable_mode={tradable_mode}|"
        f"tw_price_rules=v3|"
        f"trading_volume_policy={trading_volume_policy}|security_filter={security_filter}|"
        f"external={external_key}|external_market_symbol={external_market_symbol}|"
        f"external_features={bool(external_include_features)}|"
        f"external_rules={bool(external_include_rules)}|"
        f"corporate_action_reference={corporate_action_key}|"
        f"corporate_action_coverage_contract=v{CORPORATE_ACTION_COVERAGE_CONTRACT_VERSION}|"
        f"corporate_action_avoidance_contract=v{CORPORATE_ACTION_AVOIDANCE_CONTRACT_VERSION}|"
        f"feature_include={list(base_feature_include_patterns)!r}|"
        f"feature_exclude={list(feature_exclude_patterns)!r}|"
        f"feature_zero_fill={list(base_feature_zero_fill_patterns)!r}|"
        f"{feature_shift_key}"
        f"panel_start_date={normalized_panel_start_date}"
    )
    hot_tail_paths = [
        path.parent / HOT_TAIL_DIRNAME / path.name for path in parquet_paths
    ]
    source_paths = [
        *parquet_paths,
        *(path for path in hot_tail_paths if path.is_file()),
        *security_metadata_paths,
    ]
    if external_feature_path is not None:
        source_paths.append(external_feature_path)
    if corporate_action_paths is not None:
        source_paths.extend(
            [corporate_action_paths.parquet, corporate_action_paths.summary]
        )
        if corporate_action_paths.entitlements_parquet is not None:
            assert corporate_action_paths.entitlements_summary is not None
            source_paths.extend(
                [
                    corporate_action_paths.entitlements_parquet,
                    corporate_action_paths.entitlements_summary,
                ]
            )
    source_hash = _compute_source_hash(source_paths)
    panel = _load_valid_panel_cache(parquet_root, source_paths, backend_key, source_hash)
    if panel is not None:
        if include_day_trade_open_gap:
            panel = _append_configured_day_trade_open_gap_feature(
                panel,
                feature_zero_fill_patterns=feature_zero_fill_patterns,
                feature_shift_next_session_patterns=(
                    feature_shift_next_session_patterns
                ),
            )
        _print_feature_overview(panel)
    return panel


def _apply_external_rule_masks(panel: PanelData, external_features: _ExternalFeatureArrays | None) -> PanelData:
    if external_features is None or not external_features.rule_names:
        return panel

    rule_to_idx = {name: idx for idx, name in enumerate(external_features.rule_names)}
    up_idx = rule_to_idx.get("_twpub_tpex_next_limit_up_ret")
    down_idx = rule_to_idx.get("_twpub_tpex_next_limit_down_ret")
    traded_idx = rule_to_idx.get("_twpub_official_traded")
    delisted_idx = rule_to_idx.get("_twpub_delisted")
    short_ban_idx = rule_to_idx.get("_twpub_short_open_ban")
    margin_short_evidence_idx = rule_to_idx.get(
        "_twpub_margin_short_evidence_next_session"
    )
    short_capacity_idx = rule_to_idx.get(
        "_twpub_short_capacity_shares_next_session"
    )
    margin_short_schema_present = (
        margin_short_evidence_idx is not None or short_capacity_idx is not None
    )
    force_short_cover_idx = rule_to_idx.get("_twpub_force_short_cover")
    force_cover_lead_idx = rule_to_idx.get("_twpub_force_cover_lead_sessions")
    force_cover_anchor_idx = rule_to_idx.get("_twpub_force_cover_anchor_ordinal")
    if force_cover_anchor_idx is None:
        # Compatibility for public feature parquet generated before the anchor
        # semantics were named explicitly.
        force_cover_anchor_idx = rule_to_idx.get(
            "_twpub_force_cover_delisting_ordinal"
        )
    force_cover_cancel_idx = rule_to_idx.get("_twpub_force_cover_cancel_ordinal")
    trading_halt_idx = rule_to_idx.get("_twpub_trading_halt")
    day_trade_eligible_idx = rule_to_idx.get("_twpub_day_trade_eligible")
    day_trade_short_open_idx = rule_to_idx.get("_twpub_day_trade_short_open")

    def has_finite_rule_evidence(rule_index: int | None) -> bool:
        if rule_index is None:
            return False
        return any(
            values.ndim == 2
            and int(values.shape[1]) > int(rule_index)
            and bool(np.isfinite(values[:, rule_index]).any())
            for _, values in external_features.by_symbol_rules.values()
        )

    # A fixed external schema can contain all-null columns before the producer
    # has been backfilled.  Such a column is absence of evidence, not an
    # all-false historical rule mask.
    if not has_finite_rule_evidence(day_trade_eligible_idx):
        day_trade_eligible_idx = None
    if not has_finite_rule_evidence(day_trade_short_open_idx):
        day_trade_short_open_idx = None
    # The two margin fields form one atomic evidence contract.  A partially
    # generated schema is unknown, not permission to infer capacity.
    if not (
        has_finite_rule_evidence(margin_short_evidence_idx)
        and has_finite_rule_evidence(short_capacity_idx)
    ):
        margin_short_evidence_idx = None
        short_capacity_idx = None
    if not margin_short_schema_present and all(
        idx is None
        for idx in (
            up_idx,
            down_idx,
            traded_idx,
            delisted_idx,
            short_ban_idx,
            margin_short_evidence_idx,
            short_capacity_idx,
            force_short_cover_idx,
            force_cover_lead_idx,
            force_cover_anchor_idx,
            force_cover_cancel_idx,
            trading_halt_idx,
            day_trade_eligible_idx,
            day_trade_short_open_idx,
        )
    ):
        return panel

    can_buy = np.asarray(panel.can_buy_mask if panel.can_buy_mask is not None else panel.tradable_mask, dtype=bool).copy()
    can_sell = np.asarray(panel.can_sell_mask if panel.can_sell_mask is not None else panel.tradable_mask, dtype=bool).copy()
    day_trade_can_buy_open = np.asarray(
        panel.day_trade_can_buy_open_mask
        if panel.day_trade_can_buy_open_mask is not None
        else panel.tradable_mask,
        dtype=bool,
    ).copy()
    day_trade_can_sell_open = np.asarray(
        panel.day_trade_can_sell_open_mask
        if panel.day_trade_can_sell_open_mask is not None
        else panel.tradable_mask,
        dtype=bool,
    ).copy()
    can_short_open = np.asarray(
        panel.can_short_open_mask if panel.can_short_open_mask is not None else can_sell,
        dtype=bool,
    ).copy()
    can_short_open_open = (
        np.asarray(panel.can_short_open_open_mask, dtype=bool).copy()
        if panel.can_short_open_open_mask is not None
        else np.zeros_like(panel.tradable_mask, dtype=bool)
    )
    margin_short_rules_available = (
        margin_short_evidence_idx is not None and short_capacity_idx is not None
    )
    if margin_short_schema_present:
        # Margin eligibility must be reconstructed solely from exact official
        # evidence.  An all-null or partially generated rule schema is still a
        # declared margin contract, so every symbol/date remains false/zero.
        can_short_open = np.zeros_like(panel.tradable_mask, dtype=bool)
        can_short_open_open = np.zeros_like(panel.tradable_mask, dtype=bool)
        short_capacity_shares: np.ndarray | None = np.zeros(
            panel.tradable_mask.shape,
            dtype=np.int64,
        )
    elif panel.short_capacity_shares is None:
        short_capacity_shares = None
    else:
        short_capacity_shares = np.asarray(
            panel.short_capacity_shares,
            dtype=np.int64,
        ).copy()
    short_margin_rate = (
        None
        if panel.short_margin_rate is None
        else np.asarray(panel.short_margin_rate).copy()
    )
    force_short_cover = np.asarray(
        panel.force_short_cover_mask
        if panel.force_short_cover_mask is not None
        else np.zeros_like(panel.tradable_mask, dtype=bool),
        dtype=bool,
    ).copy()
    force_exit = np.asarray(
        panel.force_exit_mask
        if panel.force_exit_mask is not None
        else np.zeros_like(panel.tradable_mask, dtype=bool),
        dtype=bool,
    ).copy()
    close_prices = np.asarray(panel.close_prices, dtype=np.float64)
    day_trade_eligible = (
        np.asarray(panel.day_trade_eligible_mask, dtype=bool).copy()
        if panel.day_trade_eligible_mask is not None
        else np.zeros_like(panel.tradable_mask, dtype=bool)
        if day_trade_eligible_idx is not None
        else None
    )
    day_trade_short_open = (
        np.asarray(panel.day_trade_can_short_open_mask, dtype=bool).copy()
        if panel.day_trade_can_short_open_mask is not None
        else np.zeros_like(panel.tradable_mask, dtype=bool)
        if day_trade_short_open_idx is not None
        else None
    )
    changed_buy = 0
    changed_sell = 0

    for sym_idx, symbol in enumerate(panel.symbols):
        rule_payload = external_features.by_symbol_rules.get(str(symbol).upper())
        if rule_payload is None:
            continue
        # Preserve the source-market observation before official lifecycle
        # rules mutate it.  A same-symbol security that trades again on the
        # immediately following panel session is a venue/corporate transition,
        # not a terminal asset exit; permanently blocking it would create both
        # a fabricated sale and years of false untradability (for example 2301).
        base_tradable = np.asarray(panel.tradable_mask[:, sym_idx], dtype=bool).copy()
        rule_dates, rule_values = rule_payload
        if rule_values.size == 0:
            continue
        aligned_rules = _align_external_values(panel.dates, rule_dates, rule_values)
        if margin_short_rules_available:
            assert margin_short_evidence_idx is not None
            assert short_capacity_idx is not None
            assert short_capacity_shares is not None
            evidence_values = aligned_rules[:, margin_short_evidence_idx]
            capacity_values = aligned_rules[:, short_capacity_idx]
            source_valid = (
                np.isfinite(evidence_values)
                & (evidence_values > 0.0)
                & np.isfinite(capacity_values)
                & (capacity_values >= 0.0)
                & (capacity_values == np.floor(capacity_values))
                & (capacity_values <= float(np.iinfo(np.int64).max))
            )
            # Row t's official closing balance and explicitly named
            # next-business-day limit become usable on exactly panel session
            # t+1.  Missing t evidence never forward-fills across a gap.
            next_session_valid = np.zeros((panel.num_dates,), dtype=bool)
            next_session_capacity = np.zeros((panel.num_dates,), dtype=np.int64)
            if panel.num_dates > 1:
                next_session_valid[1:] = source_valid[:-1]
                valid_source_rows = source_valid[:-1]
                if bool(valid_source_rows.any()):
                    source_rows = np.flatnonzero(valid_source_rows)
                    next_session_capacity[source_rows + 1] = capacity_values[
                        source_rows
                    ].astype(np.int64, copy=False)
            # Eligibility and inventory are separate contracts.  A valid
            # margin-short row with zero demonstrated headroom is still an
            # eligible security; the capacity tensor blocks it only when the
            # account configuration enables the inventory ceiling.
            can_short_open[:, sym_idx] = next_session_valid
            can_short_open_open[:, sym_idx] = next_session_valid
            short_capacity_shares[:, sym_idx] = next_session_capacity
        if day_trade_eligible_idx is not None and day_trade_eligible is not None:
            eligibility_values = aligned_rules[:, day_trade_eligible_idx]
            observed = np.isfinite(eligibility_values)
            if bool(observed.any()):
                # The producer emits explicit 0/1 membership for every venue
                # universe row on the exact session.  Never carry membership
                # across a missing receipt.
                day_trade_eligible[observed, sym_idx] = (
                    eligibility_values[observed] > 0.0
                )
        if day_trade_short_open_idx is not None and day_trade_short_open is not None:
            direction_values = aligned_rules[:, day_trade_short_open_idx]
            observed_direction = np.isfinite(direction_values)
            if bool(observed_direction.any()):
                day_trade_short_open[observed_direction, sym_idx] = (
                    direction_values[observed_direction] > 0.0
                )
        if trading_halt_idx is not None:
            halted = np.isfinite(aligned_rules[:, trading_halt_idx]) & (aligned_rules[:, trading_halt_idx] > 0.0)
            if bool(halted.any()):
                can_buy[halted, sym_idx] = False
                can_sell[halted, sym_idx] = False
                day_trade_can_buy_open[halted, sym_idx] = False
                day_trade_can_sell_open[halted, sym_idx] = False
                can_short_open[halted, sym_idx] = False
                can_short_open_open[halted, sym_idx] = False
                panel.returns_1d[halted, sym_idx] = 0.0
        if short_ban_idx is not None:
            short_banned = np.isfinite(aligned_rules[:, short_ban_idx]) & (aligned_rules[:, short_ban_idx] > 0.0)
            can_short_open[short_banned, sym_idx] = False
            can_short_open_open[short_banned, sym_idx] = False
            if day_trade_short_open is not None:
                day_trade_short_open[short_banned, sym_idx] = False
        delisted_rows = np.empty((0,), dtype=np.int64)
        force_exit_windows: list[tuple[int, int, int]] = []
        delisted_blocked = np.zeros((panel.num_dates,), dtype=bool)
        if delisted_idx is not None:
            event_mask = np.isfinite(rule_values[:, delisted_idx]) & (rule_values[:, delisted_idx] > 0.0)
            if bool(event_mask.any()):
                # Official termination dates can fall on weekends. Apply the event
                # on the first panel session at or after the official date.
                candidate_rows = np.searchsorted(panel.dates, rule_dates[event_mask])
                candidate_rows = np.unique(
                    candidate_rows[candidate_rows < panel.num_dates]
                ).astype(np.int64)
                effective_rows: list[int] = []
                episode_start = 0
                for candidate in candidate_rows:
                    start = int(candidate)
                    later_traded = np.flatnonzero(base_tradable[start + 1 :])
                    next_traded = (
                        start + 1 + int(later_traded[0])
                        if later_traded.size
                        else panel.num_dates
                    )
                    # Immediate continuation under the same symbol is an
                    # exchange/corporate migration rather than a terminal
                    # position.  Longer gaps are real incarnation boundaries:
                    # exit the old security, then allow a later relisting.
                    if later_traded.size and next_traded == start + 1:
                        continue
                    if not delisted_blocked[start]:
                        effective_rows.append(start)
                        # The official termination session commonly follows a
                        # multi-day trading suspension.  Integer-share cash
                        # execution cannot sell at a missing termination-day
                        # quote, so liquidate at the final observed positive
                        # close of this security episode.  Search only inside
                        # the current episode: a later relisting must never be
                        # settled against the preceding incarnation's price.
                        force_exit_windows.append(
                            (int(episode_start), int(start), int(next_traded))
                        )
                        episode_start = next_traded
                    delisted_blocked[start:next_traded] = True
                delisted_rows = np.asarray(effective_rows, dtype=np.int64)
        if traded_idx is not None:
            official_traded = np.isfinite(aligned_rules[:, traded_idx]) & (aligned_rules[:, traded_idx] > 0.0)
            observed_rows = np.flatnonzero(official_traded)
            if observed_rows.size:
                # The official OHLC downloads can contain several sparse
                # archive snapshots rather than one continuous history.  Only
                # infer a missing-session suspension between nearby positive
                # observations; a multi-year absence is missing evidence, not
                # proof that the entire market was halted.  Longer suspensions
                # come from the explicit halt/resume rule feed above.
                suspended = np.zeros_like(official_traded)
                max_contiguous_gap = np.timedelta64(7, "D")
                for left, right in zip(observed_rows[:-1], observed_rows[1:]):
                    left_row = int(left)
                    right_row = int(right)
                    if right_row <= left_row + 1:
                        continue
                    if panel.dates[right_row] - panel.dates[left_row] <= max_contiguous_gap:
                        suspended[left_row + 1 : right_row] = True
                for delisted_row in delisted_rows:
                    prior = observed_rows[observed_rows < int(delisted_row)]
                    if not prior.size:
                        continue
                    left_row = int(prior[-1])
                    right_row = int(delisted_row)
                    if (
                        right_row > left_row + 1
                        and panel.dates[right_row] - panel.dates[left_row]
                        <= max_contiguous_gap
                    ):
                        suspended[left_row + 1 : right_row] = True
                suspended &= ~delisted_blocked
                if bool(suspended.any()):
                    panel.tradable_mask[suspended, sym_idx] = True
                    can_buy[suspended, sym_idx] = False
                    can_sell[suspended, sym_idx] = False
                    day_trade_can_buy_open[suspended, sym_idx] = False
                    day_trade_can_sell_open[suspended, sym_idx] = False
                    can_short_open[suspended, sym_idx] = False
                    can_short_open_open[suspended, sym_idx] = False
                    panel.returns_1d[suspended, sym_idx] = 0.0
        if delisted_rows.size:
            panel.tradable_mask[delisted_blocked, sym_idx] = False
            can_buy[delisted_blocked, sym_idx] = False
            can_sell[delisted_blocked, sym_idx] = False
            day_trade_can_buy_open[delisted_blocked, sym_idx] = False
            day_trade_can_sell_open[delisted_blocked, sym_idx] = False
            can_short_open[delisted_blocked, sym_idx] = False
            can_short_open_open[delisted_blocked, sym_idx] = False
        close_ret = _safe_log_ratio_array(close_prices[:, sym_idx], _shift_array(close_prices[:, sym_idx], 1))
        open_ret = _safe_log_ratio_array(
            np.asarray(panel.open_prices[:, sym_idx], dtype=np.float64),
            _shift_array(close_prices[:, sym_idx], 1),
        ) if panel.open_prices is not None else np.full_like(close_ret, np.nan)

        if up_idx is not None:
            next_limit_up_ret = _shift_array(aligned_rules[:, up_idx], 1)
            is_limit_up = np.isfinite(next_limit_up_ret) & np.isfinite(close_ret) & (close_ret >= (next_limit_up_ret - 1e-6))
            before = can_buy[:, sym_idx].copy()
            can_buy[:, sym_idx] &= ~is_limit_up
            open_is_limit_up = (
                np.isfinite(next_limit_up_ret)
                & np.isfinite(open_ret)
                & (open_ret >= (next_limit_up_ret - 1e-6))
            )
            day_trade_can_buy_open[:, sym_idx] &= ~open_is_limit_up
            changed_buy += int(np.count_nonzero(before & ~can_buy[:, sym_idx]))

        if down_idx is not None:
            next_limit_down_ret = _shift_array(aligned_rules[:, down_idx], 1)
            is_limit_down = np.isfinite(next_limit_down_ret) & np.isfinite(close_ret) & (close_ret <= (next_limit_down_ret + 1e-6))
            before = can_sell[:, sym_idx].copy()
            can_sell[:, sym_idx] &= ~is_limit_down
            open_is_limit_down = (
                np.isfinite(next_limit_down_ret)
                & np.isfinite(open_ret)
                & (open_ret <= (next_limit_down_ret + 1e-6))
            )
            day_trade_can_sell_open[:, sym_idx] &= ~open_is_limit_down
            changed_sell += int(np.count_nonzero(before & ~can_sell[:, sym_idx]))

        if force_short_cover_idx is not None:
            event_mask = np.isfinite(rule_values[:, force_short_cover_idx]) & (rule_values[:, force_short_cover_idx] > 0.0)
            for candidate in np.searchsorted(panel.dates, rule_dates[event_mask]):
                if candidate >= panel.num_dates:
                    continue
                # Closing a short is a buy.  Align weekend/holiday deadlines
                # to the first session where that buy can actually execute,
                # after halt, delisting and limit-up rules have been applied.
                executable_after = np.flatnonzero(can_buy[int(candidate) :, sym_idx])
                if executable_after.size:
                    force_short_cover[int(candidate) + int(executable_after[0]), sym_idx] = True
        if force_cover_lead_idx is not None and force_cover_anchor_idx is not None:
            lead_values = rule_values[:, force_cover_lead_idx]
            anchor_ordinals = rule_values[:, force_cover_anchor_idx]
            cancel_ordinals = (
                rule_values[:, force_cover_cancel_idx]
                if force_cover_cancel_idx is not None
                else np.full_like(anchor_ordinals, np.nan)
            )
            relative_event_rows = np.flatnonzero(
                np.isfinite(lead_values)
                & (lead_values > 0.0)
                & np.isfinite(anchor_ordinals)
                & (anchor_ordinals > 0.0)
            )
            for event_row in relative_event_rows:
                knowledge_idx = int(np.searchsorted(panel.dates, rule_dates[event_row], side="right"))
                anchor_date = np.datetime64(
                    date.fromordinal(int(round(float(anchor_ordinals[event_row]))))
                )
                anchor_session_idx = int(
                    np.searchsorted(panel.dates, anchor_date, side="left")
                )
                deadline_idx = anchor_session_idx - int(
                    round(float(lead_values[event_row]))
                )
                if np.isfinite(cancel_ordinals[event_row]):
                    cancellation_date = np.datetime64(
                        date.fromordinal(
                            int(round(float(cancel_ordinals[event_row])))
                        )
                    )
                    cancellation_session_idx = int(
                        np.searchsorted(panel.dates, cancellation_date, side="left")
                    )
                    # Cancellation is prospective: remove only an obligation
                    # whose actual exchange-session deadline has not passed.
                    if cancellation_session_idx <= deadline_idx:
                        continue
                if knowledge_idx >= panel.num_dates or knowledge_idx >= anchor_session_idx:
                    continue
                if deadline_idx >= knowledge_idx:
                    candidate = min(deadline_idx, panel.num_dates - 1)
                    executable = np.flatnonzero(can_buy[knowledge_idx : candidate + 1, sym_idx])
                    if executable.size:
                        force_short_cover[knowledge_idx + int(executable[-1]), sym_idx] = True
                    else:
                        # If every session through the contractual deadline is
                        # blocked, do not silently lose the cover obligation.
                        # Execute on the first available session from the
                        # deadline onward, but never cross the rule anchor.
                        end = min(anchor_session_idx, panel.num_dates)
                        executable_after = np.flatnonzero(
                            can_buy[candidate:end, sym_idx]
                        )
                        if executable_after.size:
                            force_short_cover[
                                candidate + int(executable_after[0]), sym_idx
                            ] = True
                else:
                    # A late notice cannot retroactively force a cover. Execute
                    # on the first known tradable session before the anchor.
                    end = min(anchor_session_idx, panel.num_dates)
                    executable = np.flatnonzero(can_buy[knowledge_idx:end, sym_idx])
                    if executable.size:
                        force_short_cover[knowledge_idx + int(executable[0]), sym_idx] = True

        # One terminal mask is consumed by both long liquidation and short
        # cover.  Therefore its close must be executable on both sides.  The
        # old implementation chose the final positive quote before delisting
        # and only afterwards applied limit/halt masks; a limit-up close could
        # consequently become an impossible short cover and crash the real
        # fold even though an earlier two-sided close existed.  Resolve the
        # terminal row after every side rule has been applied, and prohibit a
        # fresh position between that liquidation and the next incarnation of
        # the same symbol.
        for episode_start, termination_row, next_traded in force_exit_windows:
            episode_slice = slice(episode_start, termination_row + 1)
            executable = np.flatnonzero(
                can_buy[episode_slice, sym_idx]
                & can_sell[episode_slice, sym_idx]
                & np.isfinite(close_prices[episode_slice, sym_idx])
                & (close_prices[episode_slice, sym_idx] > 0.0)
            )
            if executable.size:
                liquidation_row = episode_start + int(executable[-1])
            else:
                # Preserve fail-closed observability for genuinely impossible
                # exits.  The executor will reject a held position rather than
                # fabricate an execution price or silently drop the event.
                quoted = np.flatnonzero(
                    np.isfinite(close_prices[episode_slice, sym_idx])
                    & (close_prices[episode_slice, sym_idx] > 0.0)
                )
                if not quoted.size:
                    continue
                liquidation_row = episode_start + int(quoted[-1])
            force_exit[liquidation_row, sym_idx] = True
            block_start = liquidation_row + 1
            block_end = min(int(next_traded), panel.num_dates)
            if block_start < block_end:
                can_buy[block_start:block_end, sym_idx] = False
                can_sell[block_start:block_end, sym_idx] = False
                can_short_open[block_start:block_end, sym_idx] = False
                can_short_open_open[block_start:block_end, sym_idx] = False
                day_trade_can_buy_open[block_start:block_end, sym_idx] = False
                day_trade_can_sell_open[block_start:block_end, sym_idx] = False

    if changed_buy or changed_sell:
        print(
            "[panel] external TW public limit rules updated masks "
            f"(blocked_buy={changed_buy}, blocked_sell={changed_sell})"
        )
    # The close and open auctions have different observable execution facts.
    # Keep the common official eligibility/capacity oracle phase-neutral, then
    # gate each phase with only the quote state known by that auction.
    effective_short_open = can_short_open & can_sell
    effective_short_open_at_open = (
        can_short_open_open & day_trade_can_sell_open
    )
    if short_capacity_shares is not None:
        short_capacity_shares = np.where(
            can_short_open | can_short_open_open,
            short_capacity_shares,
            0,
        ).astype(np.int64, copy=False)
    return PanelData(
        dates=panel.dates,
        symbols=panel.symbols,
        feature_names=panel.feature_names,
        features=panel.features,
        returns_1d=panel.returns_1d,
        tradable_mask=panel.tradable_mask,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        can_short_open_mask=effective_short_open,
        can_short_open_open_mask=effective_short_open_at_open,
        force_short_cover_mask=force_short_cover,
        force_exit_mask=force_exit,
        short_capacity_shares=short_capacity_shares,
        short_margin_rate=short_margin_rate,
        alive_mask=panel.alive_mask,
        benchmark_returns=panel.benchmark_returns,
        close_prices=panel.close_prices,
        daily_volumes=panel.daily_volumes,
        open_prices=panel.open_prices,
        intraday_returns=panel.intraday_returns,
        day_trade_eligible_mask=day_trade_eligible,
        day_trade_can_short_open_mask=day_trade_short_open,
        day_trade_can_buy_open_mask=day_trade_can_buy_open,
        day_trade_can_sell_open_mask=day_trade_can_sell_open,
        raw_close_returns_1d=panel.raw_close_returns_1d,
        corporate_action_avoidance_mask=panel.corporate_action_avoidance_mask,
        unresolved_corporate_action_mask=panel.unresolved_corporate_action_mask,
        cash_dividend_yield=panel.cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            panel.cash_dividend_payment_delay_sessions
        ),
    )


def _resolve_benchmark_index(symbols: list[str], benchmark_name: str) -> int | None:
    key = (str(benchmark_name) if benchmark_name is not None else "").strip()
    if not key:
        return None

    if key.lower() in {"universe_average_return", "universe_average", "universe", "average"}:
        return None

    normalized = key.upper().replace("-", "").replace("_", "")
    alias_candidates = [normalized]
    if not normalized.endswith("USD"):
        alias_candidates.append(f"{normalized}USD")

    symbol_to_idx = {symbol.upper(): idx for idx, symbol in enumerate(symbols)}
    for candidate in alias_candidates:
        if candidate in symbol_to_idx:
            return symbol_to_idx[candidate]

    known = ", ".join(symbols[:10])
    raise ValueError(
        f"benchmark_name={benchmark_name!r} not found in panel symbols. "
        f"Try one of: universe_average_return, BTC, BTCUSD (sample symbols: {known}...)"
    )


def _panel_cache_path(parquet_root: str | Path) -> Path:
    return legacy_panel_cache_path(parquet_root)


def _cache_meta_path(parquet_root: str | Path) -> Path:
    return legacy_panel_meta_path(parquet_root)


def _compute_source_hash(paths: list[Path]) -> str:
    """Fingerprint complete source contents, detecting races while hashing.

    Metadata-only keys can reuse stale panels when a sync/copy preserves path,
    size and mtime (and some filesystems expose coarse ctime).  A stale cache
    then defeats the stronger checkpoint panel fingerprint because the new
    source is never materialized.  Hash every byte once per cache validation;
    independent files are read concurrently to keep many-small-file universes
    practical.
    """

    def file_fingerprint(path: Path) -> tuple[str, int, str]:
        for attempt in range(2):
            before = path.stat()
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                while chunk := handle.read(1 << 20):
                    digest.update(chunk)
            after = path.stat()
            unchanged = (
                int(before.st_size) == int(after.st_size)
                and int(before.st_mtime_ns) == int(after.st_mtime_ns)
                and int(before.st_ctime_ns) == int(after.st_ctime_ns)
            )
            if unchanged:
                return str(path), int(after.st_size), digest.hexdigest()
            if attempt == 0:
                continue
        raise RuntimeError(f"Panel source changed while fingerprinting: {path}")

    ordered_paths = sorted(paths)
    workers = min(
        len(ordered_paths),
        max(1, min(16, int(os.cpu_count() or 1))),
    )
    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            identities = list(executor.map(file_fingerprint, ordered_paths))
    else:
        identities = [file_fingerprint(path) for path in ordered_paths]

    hasher = hashlib.sha256()
    for identity in identities:
        for item in identity:
            encoded = str(item).encode("utf-8")
            hasher.update(len(encoded).to_bytes(8, byteorder="little", signed=False))
            hasher.update(encoded)
    return hasher.hexdigest()


def _save_panel_cache(
    parquet_root: str | Path,
    panel: PanelData,
    source_hash: str,
    backend_key: str,
) -> None:
    save_panel_cache_v2(
        parquet_root,
        panel,
        source_hash=source_hash,
        backend_key=backend_key,
        version=PANEL_CACHE_VERSION,
    )


def _load_panel_cache(cache_path: Path) -> PanelData:
    cached = np.load(cache_path, allow_pickle=True)
    cached_keys = set(cached.files)
    tradable_mask = cached["tradable_mask"]
    can_buy_mask = cached["can_buy_mask"] if "can_buy_mask" in cached_keys else tradable_mask
    can_sell_mask = cached["can_sell_mask"] if "can_sell_mask" in cached_keys else tradable_mask
    can_short_open_mask = cached["can_short_open_mask"] if "can_short_open_mask" in cached_keys else can_sell_mask
    can_short_open_open_mask = (
        cached["can_short_open_open_mask"]
        if "can_short_open_open_mask" in cached_keys
        else None
    )
    force_short_cover_mask = (
        cached["force_short_cover_mask"]
        if "force_short_cover_mask" in cached_keys
        else np.zeros_like(tradable_mask, dtype=bool)
    )
    force_exit_mask = (
        cached["force_exit_mask"]
        if "force_exit_mask" in cached_keys
        else np.zeros_like(tradable_mask, dtype=bool)
    )
    close_prices = cached["close_prices"]
    daily_volumes = (
        cached["daily_volumes"]
        if "daily_volumes" in cached_keys
        else np.full_like(close_prices, np.nan, dtype=np.float32)
    )
    open_prices = cached["open_prices"] if "open_prices" in cached_keys else None
    intraday_returns = (
        cached["intraday_returns"] if "intraday_returns" in cached_keys else None
    )
    day_trade_eligible_mask = (
        cached["day_trade_eligible_mask"]
        if "day_trade_eligible_mask" in cached_keys
        else None
    )
    day_trade_can_short_open_mask = (
        cached["day_trade_can_short_open_mask"]
        if "day_trade_can_short_open_mask" in cached_keys
        else None
    )
    day_trade_can_buy_open_mask = (
        cached["day_trade_can_buy_open_mask"]
        if "day_trade_can_buy_open_mask" in cached_keys
        else None
    )
    day_trade_can_sell_open_mask = (
        cached["day_trade_can_sell_open_mask"]
        if "day_trade_can_sell_open_mask" in cached_keys
        else None
    )
    unresolved_corporate_action_mask = (
        cached["unresolved_corporate_action_mask"]
        if "unresolved_corporate_action_mask" in cached_keys
        else None
    )
    raw_close_returns_1d = (
        cached["raw_close_returns_1d"]
        if "raw_close_returns_1d" in cached_keys
        else None
    )
    short_capacity_shares = (
        cached["short_capacity_shares"]
        if "short_capacity_shares" in cached_keys
        else None
    )
    short_margin_rate = (
        cached["short_margin_rate"]
        if "short_margin_rate" in cached_keys
        else None
    )
    return PanelData(
        dates=cached["dates"],
        symbols=cached["symbols"].tolist(),
        feature_names=cached["feature_names"].tolist(),
        features=cached["features"],
        returns_1d=cached["returns_1d"],
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        can_short_open_mask=can_short_open_mask,
        can_short_open_open_mask=can_short_open_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        force_exit_mask=force_exit_mask,
        short_capacity_shares=short_capacity_shares,
        short_margin_rate=short_margin_rate,
        alive_mask=cached["alive_mask"],
        benchmark_returns=cached["benchmark_returns"],
        close_prices=close_prices,
        daily_volumes=daily_volumes,
        open_prices=open_prices,
        intraday_returns=intraday_returns,
        day_trade_eligible_mask=day_trade_eligible_mask,
        day_trade_can_short_open_mask=day_trade_can_short_open_mask,
        day_trade_can_buy_open_mask=day_trade_can_buy_open_mask,
        day_trade_can_sell_open_mask=day_trade_can_sell_open_mask,
        raw_close_returns_1d=raw_close_returns_1d,
        corporate_action_avoidance_mask=(
            cached["corporate_action_avoidance_mask"]
            if "corporate_action_avoidance_mask" in cached_keys
            else None
        ),
        unresolved_corporate_action_mask=unresolved_corporate_action_mask,
        cash_dividend_yield=(
            cached["cash_dividend_yield"]
            if "cash_dividend_yield" in cached_keys
            else None
        ),
        cash_dividend_payment_delay_sessions=(
            cached["cash_dividend_payment_delay_sessions"]
            if "cash_dividend_payment_delay_sessions" in cached_keys
            else None
        ),
    )


def _panel_from_cache_payload(payload: dict) -> PanelData:
    tradable_mask = payload["tradable_mask"]
    can_buy_mask = payload.get("can_buy_mask", tradable_mask)
    can_sell_mask = payload.get("can_sell_mask", tradable_mask)
    can_short_open_mask = payload.get("can_short_open_mask", can_sell_mask)
    can_short_open_open_mask = payload.get("can_short_open_open_mask")
    force_short_cover_mask = payload.get("force_short_cover_mask", np.zeros_like(tradable_mask, dtype=bool))
    force_exit_mask = payload.get(
        "force_exit_mask", np.zeros_like(tradable_mask, dtype=bool)
    )
    close_prices = payload["close_prices"]
    daily_volumes = payload.get("daily_volumes", np.full_like(close_prices, np.nan, dtype=np.float32))
    return PanelData(
        dates=payload["dates"],
        symbols=list(payload["symbols"]),
        feature_names=list(payload["feature_names"]),
        features=payload["features"],
        returns_1d=payload["returns_1d"],
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        can_short_open_mask=can_short_open_mask,
        can_short_open_open_mask=can_short_open_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        force_exit_mask=force_exit_mask,
        short_capacity_shares=payload.get("short_capacity_shares"),
        short_margin_rate=payload.get("short_margin_rate"),
        alive_mask=payload["alive_mask"],
        benchmark_returns=payload["benchmark_returns"],
        close_prices=close_prices,
        daily_volumes=daily_volumes,
        open_prices=payload.get("open_prices"),
        intraday_returns=payload.get("intraday_returns"),
        day_trade_eligible_mask=payload.get("day_trade_eligible_mask"),
        day_trade_can_short_open_mask=payload.get(
            "day_trade_can_short_open_mask"
        ),
        day_trade_can_buy_open_mask=payload.get(
            "day_trade_can_buy_open_mask"
        ),
        day_trade_can_sell_open_mask=payload.get(
            "day_trade_can_sell_open_mask"
        ),
        raw_close_returns_1d=payload.get("raw_close_returns_1d"),
        corporate_action_avoidance_mask=payload.get(
            "corporate_action_avoidance_mask"
        ),
        unresolved_corporate_action_mask=payload.get(
            "unresolved_corporate_action_mask"
        ),
        cash_dividend_yield=payload.get("cash_dividend_yield"),
        cash_dividend_payment_delay_sessions=payload.get(
            "cash_dividend_payment_delay_sessions"
        ),
        content_fingerprints=payload.get("_content_fingerprints"),
    )


def _print_feature_overview(panel: PanelData) -> None:
    feature_list = ", ".join(panel.feature_names)
    print(f"[panel] features ({len(panel.feature_names)}): {feature_list}")


def _normalize_feature_patterns(patterns: Any, *, label: str) -> tuple[str, ...]:
    if patterns is None:
        return ()
    if isinstance(patterns, str):
        raw_items = patterns.split(",")
    else:
        try:
            raw_items = list(patterns)
        except TypeError as exc:
            raise ValueError(f"{label} must be a list or comma-separated string") from exc

    cleaned: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        text = str(item).strip()
        if not text or text.startswith("#") or text in seen:
            continue
        cleaned.append(text)
        seen.add(text)
    return tuple(cleaned)


def _feature_pattern_indices(feature_names: list[str], patterns: tuple[str, ...], *, label: str) -> list[int]:
    name_to_index = {name: idx for idx, name in enumerate(feature_names)}
    selected: list[int] = []
    selected_set: set[int] = set()
    unmatched: list[str] = []
    for pattern in patterns:
        if any(char in pattern for char in "*?[]"):
            matches = [
                idx
                for idx, name in enumerate(feature_names)
                if fnmatch.fnmatchcase(name, pattern)
            ]
        else:
            idx = name_to_index.get(pattern)
            matches = [] if idx is None else [idx]
        if not matches:
            unmatched.append(pattern)
            continue
        for idx in matches:
            if idx not in selected_set:
                selected.append(idx)
                selected_set.add(idx)

    if unmatched:
        sample = ", ".join(feature_names[:40])
        suffix = "..." if len(feature_names) > 40 else ""
        raise ValueError(
            f"{label} did not match panel features: {unmatched}. "
            f"Available sample: {sample}{suffix}"
        )
    return selected


def _resolve_panel_feature_indices(
    base_feature_names: list[str],
    external_feature_names: list[str],
    *,
    feature_include: tuple[str, ...] = (),
    feature_exclude: tuple[str, ...] = (),
) -> tuple[list[int], list[int], list[int], list[int], list[str]]:
    all_feature_names = [*base_feature_names, *external_feature_names]
    if feature_include:
        selected = _feature_pattern_indices(all_feature_names, feature_include, label="feature_include")
    else:
        selected = list(range(len(all_feature_names)))

    if feature_exclude:
        excluded = set(_feature_pattern_indices(all_feature_names, feature_exclude, label="feature_exclude"))
        selected = [idx for idx in selected if idx not in excluded]

    if not selected:
        raise ValueError("feature_include/feature_exclude removed all panel features")

    num_base = len(base_feature_names)
    base_indices: list[int] = []
    base_dest_indices: list[int] = []
    external_indices: list[int] = []
    external_dest_indices: list[int] = []
    for dest_idx, source_idx in enumerate(selected):
        if source_idx < num_base:
            base_indices.append(source_idx)
            base_dest_indices.append(dest_idx)
        else:
            external_indices.append(source_idx - num_base)
            external_dest_indices.append(dest_idx)
    selected_names = [all_feature_names[idx] for idx in selected]
    return base_indices, base_dest_indices, external_indices, external_dest_indices, selected_names


def _filter_panel_features(
    panel: PanelData,
    *,
    feature_include: tuple[str, ...] = (),
    feature_exclude: tuple[str, ...] = (),
) -> PanelData:
    if not feature_include and not feature_exclude:
        return panel

    if feature_include:
        selected = _feature_pattern_indices(panel.feature_names, feature_include, label="feature_include")
    else:
        selected = list(range(len(panel.feature_names)))

    if feature_exclude:
        excluded = set(_feature_pattern_indices(panel.feature_names, feature_exclude, label="feature_exclude"))
        selected = [idx for idx in selected if idx not in excluded]

    if not selected:
        raise ValueError("feature_include/feature_exclude removed all panel features")

    if selected == list(range(len(panel.feature_names))):
        return panel

    filtered_names = [panel.feature_names[idx] for idx in selected]
    print(
        f"[panel] feature filter kept {len(filtered_names)}/{len(panel.feature_names)} "
        f"(include={list(feature_include) or ['*']}, exclude={list(feature_exclude) or []})"
    )
    return PanelData(
        dates=panel.dates,
        symbols=panel.symbols,
        feature_names=filtered_names,
        features=np.ascontiguousarray(panel.features[:, :, selected]),
        returns_1d=panel.returns_1d,
        tradable_mask=panel.tradable_mask,
        can_buy_mask=panel.can_buy_mask,
        can_sell_mask=panel.can_sell_mask,
        can_short_open_mask=panel.can_short_open_mask,
        can_short_open_open_mask=panel.can_short_open_open_mask,
        force_short_cover_mask=panel.force_short_cover_mask,
        force_exit_mask=panel.force_exit_mask,
        short_capacity_shares=panel.short_capacity_shares,
        short_margin_rate=panel.short_margin_rate,
        alive_mask=panel.alive_mask,
        benchmark_returns=panel.benchmark_returns,
        close_prices=panel.close_prices,
        daily_volumes=panel.daily_volumes,
        open_prices=panel.open_prices,
        intraday_returns=panel.intraday_returns,
        day_trade_eligible_mask=panel.day_trade_eligible_mask,
        day_trade_can_short_open_mask=panel.day_trade_can_short_open_mask,
        day_trade_can_buy_open_mask=panel.day_trade_can_buy_open_mask,
        day_trade_can_sell_open_mask=panel.day_trade_can_sell_open_mask,
        raw_close_returns_1d=panel.raw_close_returns_1d,
        corporate_action_avoidance_mask=panel.corporate_action_avoidance_mask,
        unresolved_corporate_action_mask=panel.unresolved_corporate_action_mask,
        cash_dividend_yield=panel.cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            panel.cash_dividend_payment_delay_sessions
        ),
    )


def _zero_fill_panel_features(
    panel: PanelData,
    feature_zero_fill: tuple[str, ...] = (),
) -> PanelData:
    if not feature_zero_fill:
        return panel
    selected = _feature_pattern_indices(
        panel.feature_names,
        feature_zero_fill,
        label="feature_zero_fill",
    )
    for feature_idx in selected:
        panel.features[:, :, feature_idx] = 0.0
    print(
        f"[panel] zero-filled {len(selected)}/{len(panel.feature_names)} retained features "
        f"(patterns={list(feature_zero_fill)})"
    )
    return panel


def _append_day_trade_open_gap_feature(panel: PanelData) -> PanelData:
    """Append the next opening gap without persisting another full panel cache.

    The cached panel remains the close-complete source of truth.  Row ``t`` of
    the appended channel is ``log(open[t+1] / close[t])`` so a lag-one Taiwan
    model deciding at open ``t+1`` sees that quote and nothing else from the
    new session.  Missing/non-consecutive quotes fail closed to zero.
    """

    if DAY_TRADE_OPEN_GAP_FEATURE in panel.feature_names:
        return panel
    if panel.open_prices is None:
        raise ValueError(
            f"{DAY_TRADE_OPEN_GAP_FEATURE} requires PanelData.open_prices"
        )
    opens = np.asarray(panel.open_prices, dtype=np.float64)
    closes = np.asarray(panel.close_prices, dtype=np.float64)
    if opens.shape != closes.shape or opens.shape != panel.features.shape[:2]:
        raise ValueError(
            "open_prices and close_prices must match panel feature [T,S] axes"
        )
    gap = np.zeros(opens.shape, dtype=np.float32)
    if opens.shape[0] > 1:
        raw = _safe_log_ratio_array(opens[1:], closes[:-1])
        raw = _sanitize_price_log_return_array(
            raw,
            TW_MAX_ABS_DAILY_PRICE_LOG_RETURN,
        )
        gap[:-1] = np.nan_to_num(
            raw,
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        ).astype(np.float32, copy=False)
    base_fingerprint = None
    open_fingerprint = None
    close_fingerprint = None
    if isinstance(panel.content_fingerprints, dict):
        base_fingerprint = panel.content_fingerprints.get("features")
        open_fingerprint = panel.content_fingerprints.get("open_prices")
        close_fingerprint = panel.content_fingerprints.get("close_prices")
    panel.features = np.concatenate((panel.features, gap[:, :, None]), axis=2)
    panel.feature_names = [*panel.feature_names, DAY_TRADE_OPEN_GAP_FEATURE]
    if all(
        isinstance(item, dict) and isinstance(item.get("sha256"), str)
        for item in (base_fingerprint, open_fingerprint, close_fingerprint)
    ):
        derived_sha = hashlib.sha256(
            json.dumps(
                {
                    "contract": "next_session_open_gap_logret_v1",
                    "base_features": base_fingerprint["sha256"],
                    "open_prices": open_fingerprint["sha256"],
                    "close_prices": close_fingerprint["sha256"],
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        panel.content_fingerprints = dict(panel.content_fingerprints)
        panel.content_fingerprints["features"] = {
            "present": True,
            "shape": [int(size) for size in panel.features.shape],
            "dtype": str(panel.features.dtype),
            "sha256": derived_sha,
        }
    else:
        # A synthetic or mutable source has no immutable cache receipt; force
        # the checkpoint path to hash the actual derived bytes.
        panel.content_fingerprints = None
    print(
        f"[panel] appended causal day-trade feature {DAY_TRADE_OPEN_GAP_FEATURE}; "
        f"features={len(panel.feature_names)}"
    )
    return panel


def _append_configured_day_trade_open_gap_feature(
    panel: PanelData,
    *,
    feature_zero_fill_patterns: tuple[str, ...],
    feature_shift_next_session_patterns: tuple[str, ...],
) -> PanelData:
    """Append OPEN gap, then apply its explicit availability transforms.

    The derived channel is intentionally kept out of the reusable close-panel
    cache.  Consequently its zero-fill and next-session shift must happen
    after attachment instead of being silently skipped with the cached base
    features.
    """

    panel = _append_day_trade_open_gap_feature(panel)
    if DAY_TRADE_OPEN_GAP_FEATURE in feature_zero_fill_patterns:
        panel = _zero_fill_panel_features(
            panel,
            (DAY_TRADE_OPEN_GAP_FEATURE,),
        )
    if DAY_TRADE_OPEN_GAP_FEATURE in feature_shift_next_session_patterns:
        panel = _shift_panel_features_to_next_session(
            panel,
            (DAY_TRADE_OPEN_GAP_FEATURE,),
        )
    return panel


def _shift_panel_features_to_next_session(
    panel: PanelData,
    feature_shift_next_session: tuple[str, ...] = (),
) -> PanelData:
    """Expose session-t feature values on the next verified panel session.

    This is an availability transform, not an ordinary row lag: the source
    archive keeps its real observation date while the model panel records the
    earliest session on which the completed value may be used.  Applying the
    shift before ``panel_start_date`` slicing preserves the prior source
    session at the configured model boundary.
    """

    if not feature_shift_next_session:
        return panel
    selected = _feature_pattern_indices(
        panel.feature_names,
        feature_shift_next_session,
        label="feature_shift_next_session",
    )
    if panel.num_dates == 0:
        return panel
    for feature_idx in selected:
        previous = panel.features[:-1, :, feature_idx].copy()
        panel.features[1:, :, feature_idx] = previous
        panel.features[0, :, feature_idx] = 0.0
    print(
        f"[panel] shifted {len(selected)}/{len(panel.feature_names)} retained features "
        "to the next panel session "
        f"(patterns={list(feature_shift_next_session)})"
    )
    return panel


def _check_cache_valid(cache_path: Path, meta_path: Path, parquet_paths: list[Path], backend_key: str) -> bool:
    """Check if cache is valid based on source hash and mtime."""
    if (not cache_path.exists()) or (not meta_path.exists()):
        return False
    
    try:
        with meta_path.open('rb') as f:
            meta = pickle.load(f)
        
        # ✅ OPTIMIZATION: Check both version and source hash for cache validity
        expected_hash = _compute_source_hash(parquet_paths)
        cache_valid = (
            meta.get('source_hash') == expected_hash and 
            meta.get('version') == PANEL_CACHE_VERSION and
            meta.get('backend_key') == backend_key
        )
        
        if cache_valid:
            # Also verify that cache file itself is newer than source files
            cache_mtime = cache_path.stat().st_mtime
            source_mtimes = [p.stat().st_mtime for p in parquet_paths]
            if cache_mtime < max(source_mtimes):
                # Cache is older than source files, invalidate
                return False
        
        return cache_valid
    except Exception as e:
        print(f"[panel] cache validation error: {e}")
        return False


def _load_valid_panel_cache(
    parquet_root: str | Path,
    parquet_paths: list[Path],
    backend_key: str,
    source_hash: str,
) -> PanelData | None:
    pinned_manifest = os.environ.get(
        "STOCKAGENT_PINNED_PANEL_CACHE_MANIFEST", ""
    ).strip()
    if pinned_manifest:
        expected_version = os.environ.get(
            "STOCKAGENT_PINNED_PANEL_CACHE_VERSION", ""
        ).strip()
        expected_generation = os.environ.get(
            "STOCKAGENT_PINNED_PANEL_CACHE_GENERATION", ""
        ).strip()
        expected_source_hash = os.environ.get(
            "STOCKAGENT_PINNED_PANEL_CACHE_SOURCE_HASH", ""
        ).strip()
        print(
            "[panel] loading explicitly pinned cache generation: "
            f"{pinned_manifest}"
        )
        return _panel_from_cache_payload(
            load_panel_cache_v2_manifest(
                pinned_manifest,
                mmap_mode="c",
                expected_version=(
                    int(expected_version) if expected_version else None
                ),
                expected_generation=expected_generation or None,
                expected_source_hash=expected_source_hash or None,
            )
        )
    if panel_cache_v2_is_valid(
        parquet_root,
        source_hash=source_hash,
        backend_key=backend_key,
        version=PANEL_CACHE_VERSION,
        source_paths=parquet_paths,
    ):
        cache_dir = panel_cache_v2_dir(parquet_root)
        print(f"[panel] loading cache v2 (valid): {cache_dir}")
        return _panel_from_cache_payload(
            load_panel_cache_v2(
                parquet_root,
                mmap_mode="c",
                source_hash=source_hash,
                backend_key=backend_key,
                version=PANEL_CACHE_VERSION,
            )
        )

    cache_path = _panel_cache_path(parquet_root)
    meta_path = _cache_meta_path(parquet_root)
    if _check_cache_valid(cache_path, meta_path, parquet_paths, backend_key):
        print(f"[panel] loading legacy cache (valid): {cache_path}")
        return _load_panel_cache(cache_path)
    return None


def load_panel_cache_v2_exact(
    parquet_root: str | Path,
    *,
    source_hash: str,
    backend_key: str,
    source_paths: list[Path] | None = None,
) -> PanelData | None:
    """Load one exact v2 cache contract without invoking a panel rebuild.

    This is the small public bridge used by latency-sensitive consumers that
    already own a complete source identity.  It deliberately does not follow
    the mutable ``meta.json`` pointer, a pinned-training manifest, or the
    legacy cache: the caller-provided source/backend contract must match an
    immutable v2 generation exactly.

    ``source_paths`` may be empty when ``source_hash`` itself is a complete
    receipt over every input (for example the live-tail cache).  Ordinary full
    panel callers should continue to use :func:`load_cached_panel`, which owns
    the canonical byte-hash construction for training data.
    """

    paths = list(source_paths or [])
    if not panel_cache_v2_is_valid(
        parquet_root,
        source_hash=str(source_hash),
        backend_key=str(backend_key),
        version=PANEL_CACHE_VERSION,
        source_paths=paths,
    ):
        return None
    return _panel_from_cache_payload(
        load_panel_cache_v2(
            parquet_root,
            mmap_mode="c",
            source_hash=str(source_hash),
            backend_key=str(backend_key),
            version=PANEL_CACHE_VERSION,
        )
    )


def build_panel(
    parquet_root: str | Path,
    benchmark_name: str = "universe_average_return",
    usd_only_trading_pairs: bool = False,
    tradable_mode: str = "tradable",
    trading_volume_policy: str | bool | None = "auto",
    security_filter: str | None = "none",
    strict_no_fallback: bool | None = None,
    buy_tradable_mode: str | None = None,
    sell_tradable_mode: str | None = None,
    panel_backend: str = "auto",
    panel_load_workers: int = 4,
    external_feature_path: str | Path | None = None,
    external_market_symbol: str = DEFAULT_EXTERNAL_MARKET_SYMBOL,
    external_include_features: bool = True,
    external_include_rules: bool = True,
    external_data_required: bool = False,
    feature_include: Any = None,
    feature_exclude: Any = None,
    feature_zero_fill: Any = None,
    feature_shift_next_session: Any = None,
    panel_start_date: str | date | np.datetime64 | None = None,
) -> PanelData:
    parquet_root = Path(parquet_root)
    external_feature_path = _resolve_external_data_path(
        external_feature_path,
        include_features=external_include_features,
        include_rules=external_include_rules,
        required=external_data_required,
    )
    corporate_action_paths = _resolve_corporate_action_reference_paths(
        external_feature_path,
        include_rules=external_include_rules,
    )
    corporate_action_reference = _load_corporate_action_reference(
        corporate_action_paths
    )
    feature_include_patterns = _normalize_feature_patterns(feature_include, label="feature_include")
    feature_exclude_patterns = _normalize_feature_patterns(feature_exclude, label="feature_exclude")
    feature_zero_fill_patterns = _normalize_feature_patterns(
        feature_zero_fill, label="feature_zero_fill"
    )
    include_day_trade_open_gap = (
        DAY_TRADE_OPEN_GAP_FEATURE in feature_include_patterns
        and DAY_TRADE_OPEN_GAP_FEATURE not in feature_exclude_patterns
    )
    base_feature_include_patterns = tuple(
        pattern
        for pattern in feature_include_patterns
        if pattern != DAY_TRADE_OPEN_GAP_FEATURE
    )
    base_feature_zero_fill_patterns = tuple(
        pattern
        for pattern in feature_zero_fill_patterns
        if pattern != DAY_TRADE_OPEN_GAP_FEATURE
    )
    feature_shift_next_session_patterns = _normalize_feature_patterns(
        feature_shift_next_session, label="feature_shift_next_session"
    )
    base_feature_shift_next_session_patterns = tuple(
        pattern
        for pattern in feature_shift_next_session_patterns
        if pattern != DAY_TRADE_OPEN_GAP_FEATURE
    )
    normalized_panel_start_date = _normalize_panel_start_date(panel_start_date)
    feature_shift_key = (
        f"feature_shift_next_session={list(feature_shift_next_session_patterns)!r}|"
        if feature_shift_next_session_patterns
        else ""
    )
    parquet_paths = sorted(parquet_root.glob(f"*{FEATURE_FILE_SUFFIX}"))
    if not parquet_paths:
        raise FileNotFoundError(f"No parquet files found under {parquet_root}")

    if usd_only_trading_pairs:
        parquet_paths = [path for path in parquet_paths if _is_usd_trading_pair(path)]
        if not parquet_paths:
            raise FileNotFoundError(f"No USD trading pairs found under {parquet_root}")

    security_filter = _normalize_security_filter(security_filter)
    security_metadata_paths = _security_filter_metadata_paths(parquet_root, security_filter)
    if security_filter == BROKER_TRADABLE_SECURITY_FILTER:
        parquet_paths = _filter_us_broker_tradable_paths(parquet_root, parquet_paths, security_metadata_paths)
        if not parquet_paths:
            raise FileNotFoundError(f"No broker-tradable US symbols found under {parquet_root}")

    panel_backend = str(panel_backend).strip().lower()
    valid_backends = {"auto", "polars", "polars_lazy", "polars_streaming", "pyarrow"}
    if panel_backend not in valid_backends:
        raise ValueError(f"panel_backend must be one of {sorted(valid_backends)}, got {panel_backend!r}")
    panel_load_workers = max(0, int(panel_load_workers))
    if buy_tradable_mode is not None or sell_tradable_mode is not None:
        buy_mode = str(buy_tradable_mode if buy_tradable_mode is not None else tradable_mode).strip().lower()
        sell_mode = str(sell_tradable_mode if sell_tradable_mode is not None else tradable_mode).strip().lower()
        if buy_mode != sell_mode:
            raise ValueError(
                "buy_tradable_mode and sell_tradable_mode must be identical when provided"
            )
        tradable_mode = buy_mode

    tradable_mode = str(tradable_mode).strip().lower()
    valid_tradable_modes = {"tradable", "tw_limit_guard"}
    if tradable_mode not in valid_tradable_modes:
        raise ValueError(
            f"tradable_mode must be one of {sorted(valid_tradable_modes)}, got {tradable_mode!r}"
        )
    trading_volume_policy = _normalize_trading_volume_policy(trading_volume_policy)
    if strict_no_fallback is None:
        strict_no_fallback = str(os.getenv("STOCKAGENT_STRICT_NO_FALLBACK", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
    else:
        strict_no_fallback = bool(strict_no_fallback)

    if panel_backend == "pyarrow":
        if pq is None:
            raise RuntimeError("data.panel_backend='pyarrow' requires the pyarrow package")
        selected_backend = "pyarrow"
    elif panel_backend in {"polars", "polars_lazy", "polars_streaming"}:
        if pl is None or pq is None:
            raise RuntimeError(f"data.panel_backend={panel_backend!r} requires the polars and pyarrow packages")
        selected_backend = "polars_streaming" if panel_backend == "polars_streaming" else "polars_lazy"
    elif panel_backend == "auto" and pl is not None and pq is not None:
        selected_backend = "polars_lazy"
    elif panel_backend == "auto" and pq is not None:
        selected_backend = "pyarrow"
    else:
        raise RuntimeError("data.panel_backend='auto' requires pyarrow")

    external_key = str(external_feature_path) if external_feature_path is not None else "none"
    corporate_action_key = (
        (
            f"{corporate_action_paths.parquet}:"
            f"{corporate_action_paths.entitlements_parquet or 'none'}"
        )
        if corporate_action_paths is not None
        else "none"
    )
    backend_key = (
        f"{selected_backend}|benchmark={benchmark_name}|"
        f"usd_only={usd_only_trading_pairs}|tradable_mode={tradable_mode}|"
        f"tw_price_rules=v3|"
        f"trading_volume_policy={trading_volume_policy}|security_filter={security_filter}|"
        f"external={external_key}|external_market_symbol={external_market_symbol}|"
        f"external_features={bool(external_include_features)}|"
        f"external_rules={bool(external_include_rules)}|"
        f"corporate_action_reference={corporate_action_key}|"
        f"corporate_action_coverage_contract=v{CORPORATE_ACTION_COVERAGE_CONTRACT_VERSION}|"
        f"corporate_action_avoidance_contract=v{CORPORATE_ACTION_AVOIDANCE_CONTRACT_VERSION}|"
        f"feature_include={list(base_feature_include_patterns)!r}|"
        f"feature_exclude={list(feature_exclude_patterns)!r}|"
        f"feature_zero_fill={list(base_feature_zero_fill_patterns)!r}|"
        f"{feature_shift_key}"
        f"panel_start_date={normalized_panel_start_date}"
    )
    hot_tail_paths = [
        path.parent / HOT_TAIL_DIRNAME / path.name for path in parquet_paths
    ]
    source_paths = [
        *parquet_paths,
        *(path for path in hot_tail_paths if path.is_file()),
        *security_metadata_paths,
    ]
    if external_feature_path is not None:
        source_paths.append(external_feature_path)
    if corporate_action_paths is not None:
        source_paths.extend(
            [corporate_action_paths.parquet, corporate_action_paths.summary]
        )
        if corporate_action_paths.entitlements_parquet is not None:
            assert corporate_action_paths.entitlements_summary is not None
            source_paths.extend(
                [
                    corporate_action_paths.entitlements_parquet,
                    corporate_action_paths.entitlements_summary,
                ]
            )
    source_hash = _compute_source_hash(source_paths)

    panel = _load_valid_panel_cache(parquet_root, source_paths, backend_key, source_hash)
    if panel is not None:
        if include_day_trade_open_gap:
            panel = _append_configured_day_trade_open_gap_feature(
                panel,
                feature_zero_fill_patterns=feature_zero_fill_patterns,
                feature_shift_next_session_patterns=(
                    feature_shift_next_session_patterns
                ),
            )
        _print_feature_overview(panel)
        return panel

    print(
        f"[panel] building from {len(parquet_paths)} parquet files "
        f"(backend={selected_backend}, workers={panel_load_workers})..."
    )
    polars_collect_engine = "streaming" if selected_backend == "polars_streaming" else "auto"

    def _load_one_arrays(path: Path) -> tuple[Path, _SymbolPanelArrays | None, Exception | None]:
        try:
            if selected_backend == "pyarrow":
                arrays = _load_symbol_arrays_pyarrow(
                    path,
                    tradable_mode=tradable_mode,
                    trading_volume_policy=trading_volume_policy,
                )
            else:
                arrays = _load_symbol_arrays_polars_lazy(
                    path,
                    tradable_mode=tradable_mode,
                    collect_engine=polars_collect_engine,
                    trading_volume_policy=trading_volume_policy,
                )
            if int(arrays.dates.size) == 0:
                raise ValueError(f"Symbol file is empty: {path.name}")
            return path, arrays, None
        except Exception as exc:
            return path, None, exc

    if panel_load_workers > 1 and len(parquet_paths) > 1:
        with ThreadPoolExecutor(max_workers=panel_load_workers) as executor:
            loaded_arrays = list(executor.map(_load_one_arrays, parquet_paths))
    else:
        loaded_arrays = [_load_one_arrays(path) for path in parquet_paths]

    valid_arrays: list[_SymbolPanelArrays] = []
    for path, arrays, exc in loaded_arrays:
        if exc is not None:
            if strict_no_fallback or isinstance(exc, _MissingTradingVolumeError):
                raise type(exc)(f"{path.name}: {exc}") from exc
            print(f"[panel] SKIP {path.name}: {exc}")
            continue
        if arrays is not None:
            valid_arrays.append(arrays)
    if normalized_panel_start_date is not None:
        valid_arrays = [
            _slice_symbol_arrays_start(arrays, normalized_panel_start_date)
            for arrays in valid_arrays
        ]
        print(
            "[panel] pruned per-symbol rows before dense materialization "
            f"for panel_start_date={normalized_panel_start_date}"
        )
    external_features = (
        _load_external_feature_arrays(
            external_feature_path,
            market_symbol=external_market_symbol,
            include_features=external_include_features,
            include_rules=external_include_rules,
        )
        if external_feature_path is not None
        else None
    )
    panel = _build_panel_from_symbol_arrays(
        valid_arrays,
        benchmark_name=benchmark_name,
        external_features=external_features,
        feature_include=base_feature_include_patterns,
        feature_exclude=feature_exclude_patterns,
    )
    panel = _apply_external_rule_masks(panel, external_features)
    panel = _zero_fill_panel_features(panel, base_feature_zero_fill_patterns)
    panel = _shift_panel_features_to_next_session(
        panel,
        base_feature_shift_next_session_patterns,
    )
    panel = _slice_panel_start(panel, normalized_panel_start_date)
    panel = _apply_corporate_action_avoidance_transitions(
        panel,
        corporate_action_reference,
        None if external_features is None else external_features.official_session_dates,
    )
    if panel.unresolved_corporate_action_mask is not None:
        panel = _attach_raw_close_forward_returns(panel)
    _save_panel_cache(parquet_root, panel, source_hash, backend_key)
    print(f"[panel] cache v2 saved: {panel_cache_v2_dir(parquet_root)}")
    if include_day_trade_open_gap:
        panel = _append_configured_day_trade_open_gap_feature(
            panel,
            feature_zero_fill_patterns=feature_zero_fill_patterns,
            feature_shift_next_session_patterns=(
                feature_shift_next_session_patterns
            ),
        )
    _print_feature_overview(panel)
    return panel
