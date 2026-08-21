"""Forward Shioaji simulation for the TAIFEX strategy catalogue.

This module is deliberately attached to the existing TAIFEX market-data
connection.  It maintains two separate truths:

* an attributable one-lot ideal ledger filled at the causally received best
  opposing price, including the user's fee and statutory tax schedule; and
* Shioaji ``simulation=True`` MKT+IOC orders and their broker callbacks.

The broker account nets all strategy orders, so broker positions are an
execution/reconciliation channel.  Per-strategy P&L always comes from the
independent ideal ledgers, never from a fabricated split of a net broker
position.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timezone
import fcntl
import json
import math
import os
from pathlib import Path
import queue
import threading
import time
from typing import Any, Final, Iterable, Mapping

import polars as pl

from stockagent.backtest.tw_index_derivatives_tick import (
    sweep_five_level_depth_scalar,
    txo_short_initial_margin_per_contract_scalar,
)
from stockagent.data.tw_index_derivatives_tick import TAIPEI
from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_FEE_PER_SIDE_TWD,
    TAIFEX_INDEX_FUTURES_MULTIPLIERS,
    normalize_taifex_index_futures_product,
)
from stockagent.data.taifex_sessions import (
    DAY_OPEN,
    NIGHT_CLOSE,
    NIGHT_OPEN,
    taifex_market_phase,
    taifex_continuous_session_open,
    taifex_session_kind,
    taifex_trading_date,
)
from stockagent.live.taifex_strategy_state import (
    held_option_codes,
    required_option_codes,
)
from stockagent.research.taifex_transaction_tax import (
    option_cash_settlement_transaction_tax_twd,
    option_premium_transaction_tax_twd,
    stock_index_futures_tax_rate,
    taifex_tax_per_contract_twd,
)
from stockagent.research.taifex_capital_returns import (
    TAIFEX_MARGIN_2026_08_13_ANNOUNCEMENT_URL,
    TAIFEX_MARGIN_2026_08_13_EFFECTIVE_DATE,
    TXO_RISK_MARGIN_TWD,
    TXO_RISK_MARGIN_TWD_2026_08_13,
    taifex_futures_margin_twd,
    taifex_initial_margin_twd,
    taifex_txo_risk_margin_twd,
)
from stockagent.research.taifex_volatility_models import (
    BidAskSurfaceQuote,
    SECONDS_PER_YEAR,
    build_bidask_iv_surface,
    fit_volatility_model,
)
from stockagent.research.taifex_volatility_metadata import (
    CATALOG_EXPANSION_ENTRY_IMMEDIATE_LIVE,
    CATALOG_EXPANSION_ENTRY_NEXT_CYCLE,
    CATALOG_EXPANSION_ENTRY_POLICIES,
    CLASSIC_VARIANT_ID,
    DYNAMIC_HEDGE_STRATEGY_IDS,
    MODEL_BLACK_SCHOLES,
    MODEL_VARIANT_PREFIX,
    PUT_CALL_PARITY_TX_STRATEGY_ID,
    ROLLING_STRADDLE_IDS,
    STRATEGY_IDS,
    STRATEGY_MODE_DAILY,
    STRATEGY_MODE_INTRADAY_FUTURES,
    STRATEGY_MODES,
    STRATEGY_SPEC_BY_ID,
    VOLATILITY_MODEL_IDS,
    VOLATILITY_MODEL_IMPLEMENTATION,
    VOLATILITY_MODEL_LABELS,
)


SCHEMA_VERSION: Final[int] = 1
EXECUTION_CONTRACT_VERSION: Final[int] = 12
OPTION_MULTIPLIER: Final[float] = 50.0
OPTION_FEE_PER_SIDE_TWD: Final[float] = 22.0
DEFAULT_OPTION_RISK_MARGIN_A_TWD: Final[float] = TXO_RISK_MARGIN_TWD_2026_08_13["A"]
DEFAULT_OPTION_RISK_MARGIN_B_TWD: Final[float] = TXO_RISK_MARGIN_TWD_2026_08_13["B"]
DEFAULT_OPTION_RISK_MARGIN_C_TWD: Final[float] = TXO_RISK_MARGIN_TWD_2026_08_13["C"]
OPTION_MARGIN_POLICY: Final[str] = (
    "single_leg_naked_a_b_conservative_no_combo_offset_c_reference_only"
)
DEFAULT_STRATEGY_CAPITAL_BUFFER_MULTIPLE: Final[float] = 2.0
STRATEGY_RECAPITALIZATION_POLICY: Final[str] = (
    "next_taifex_trading_date_restore_one_strategy_package"
)
FUTURES_FEE_PER_SIDE_TWD: Final[dict[str, float]] = (
    TAIFEX_INDEX_FUTURES_FEE_PER_SIDE_TWD
)
ENTRY_BOOK_MAX_AGE_SECONDS: Final[float] = 2.0
SURFACE_BOOK_MAX_AGE_SECONDS: Final[float] = 120.0
MARK_INTERVAL_SECONDS: Final[float] = 60.0
STATUS_INTERVAL_SECONDS: Final[float] = 5.0
PUT_CALL_PARITY_OPTION_QUANTITY: Final[int] = 4
PUT_CALL_PARITY_FUTURE_QUANTITY: Final[int] = 1
PUT_CALL_PARITY_SIGNAL_MAX_WAIT_SECONDS: Final[float] = 5.0
PUT_CALL_PARITY_MIN_NET_EDGE_TWD: Final[float] = 0.0
DEFAULT_INTRADAY_DECISION_INTERVAL_SECONDS: Final[int] = 60
DEFAULT_INTRADAY_ENTRY_CUTOFF: Final[datetime_time] = datetime_time(13, 20)
DEFAULT_INTRADAY_FLATTEN_TIME: Final[datetime_time] = datetime_time(13, 35)
DEFAULT_NIGHT_ENTRY_CUTOFF: Final[datetime_time] = datetime_time(4, 40)
DEFAULT_NIGHT_FLATTEN_TIME: Final[datetime_time] = datetime_time(4, 55)
TERMINAL_ORDER_STATUSES: Final[frozenset[str]] = frozenset(
    {"Cancelled", "Filled", "Failed"}
)


def _enum(value: Any) -> str:
    return str(getattr(value, "value", value or ""))


def _right(value: Any) -> str:
    normalized = _enum(value).strip().upper()
    if normalized in {"C", "CALL"}:
        return "C"
    if normalized in {"P", "PUT"}:
        return "P"
    raise ValueError(f"unsupported option right: {value!r}")


def _base(info: Any) -> Any:
    base = getattr(info, "base", None)
    if base is None:
        raise ValueError("Contract V2 info has no Base contract")
    return base


def _series_id(info: Any) -> str:
    root = str(getattr(info, "root", "")).strip().upper()
    month = str(getattr(info, "delivery_month", "")).strip()
    expiry = getattr(info, "delivery_date", None)
    if not root or not month or not isinstance(expiry, date):
        raise ValueError("option info lacks root/delivery_month/delivery_date")
    return f"{root}:{month}:{expiry.isoformat()}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _round_nearest_contract(value: float) -> int:
    if value >= 0.0:
        return int(math.floor(value + 0.5))
    return int(math.ceil(value - 0.5))


def _continuous_future_product(logical_code: str) -> str:
    normalized = str(logical_code).strip().upper()
    if len(normalized) >= 2 and normalized[-2] == "R" and normalized[-1].isdigit():
        normalized = normalized[:-2]
    return normalize_taifex_index_futures_product(normalized)


def _new_strategy_ledger(*, entry_state: str = "pending") -> dict[str, Any]:
    return {
        "gross_cash_twd": 0.0,
        "fees_twd": 0.0,
        "tax_twd": 0.0,
        "option_gross_cash_twd": 0.0,
        "option_fees_twd": 0.0,
        "option_tax_twd": 0.0,
        "futures_position": 0,
        "underlying_futures_position": 0,
        "option_positions": {},
        "option_position_metadata": {},
        "entry_state": entry_state,
        "alive": True,
        "margin_required_twd": 0.0,
        "margin_excess_twd": None,
        "entry_capital_requirement_twd": 0.0,
        "margin_call_count": 0,
        "forced_liquidation_pending": False,
        "pending_option_roll": None,
        "option_roll_count": 0,
        "last_option_roll_decision_ts_ns": None,
        "last_option_roll_signal_ts_ns": None,
        "last_option_roll_forward_mid": None,
        "last_option_roll_atm_strike": None,
        "last_dynamic_hedge_cycle_id": None,
        "last_dynamic_hedge_session_id": None,
        "last_dynamic_hedge_decision_ts_ns": None,
        "last_dynamic_hedge_forward_mid": None,
        "trade_sides": 0,
        "initial_capital_twd": 0.0,
        "cumulative_contributed_capital_twd": 0.0,
        "capital_contribution_count": 0,
        "recapitalization_count": 0,
        "bankruptcy_count": 0,
        "episode_count": 0,
        "last_capital_contribution_twd": None,
        "last_capital_contribution_decision_ts_ns": None,
        "last_capital_contribution_trading_date": None,
        "last_bankruptcy_decision_ts_ns": None,
        "last_bankruptcy_trading_date": None,
        "last_bankruptcy_total_equity_twd": None,
        "last_complete_open_liquidation_value_twd": None,
        "last_complete_mark_decision_ts_ns": None,
        "last_complete_mark_cycle_id": None,
        "last_complete_mark_futures_position": None,
        "last_complete_mark_underlying_futures_position": None,
        "last_complete_mark_option_positions": None,
    }


def _new_put_call_parity_state() -> dict[str, Any]:
    return {
        "pending_signal": None,
        "open_position": None,
        "last_settled_expiry": None,
        "blocked_expiry": None,
        "monitor": {
            "state": "waiting_for_same_expiry_monthly_books",
            "minimum_net_edge_twd": PUT_CALL_PARITY_MIN_NET_EDGE_TWD,
            "financing_interest_rate": 0.0,
            "package": "4 TXO Call/Put synthetic forwards versus 1 TX",
            "broker_submission": False,
        },
    }


def _book_price(row: Mapping[str, Any], side: str) -> float | None:
    value = row.get(f"{side}_price_1")
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if math.isfinite(price) and price > 0.0 else None


def _book_volume(row: Mapping[str, Any], side: str) -> int:
    try:
        return int(row.get(f"{side}_volume_1") or 0)
    except (TypeError, ValueError):
        return 0


def _book_age_seconds(row: Mapping[str, Any], decision_ns: int) -> float:
    return max(0.0, (int(decision_ns) - int(row["receive_ts_ns"])) / 1e9)


def _depth_swept_price(
    row: Mapping[str, Any] | None,
    *,
    delta_contracts: int,
    decision_ns: int,
    maximum_age_seconds: float,
) -> tuple[float, int] | None:
    """Return a complete five-level executable average price or fail closed."""

    quantity = abs(int(delta_contracts))
    if quantity == 0 or row is None:
        return None
    if (
        _executable_book(
            row,
            decision_ns=decision_ns,
            maximum_age_seconds=maximum_age_seconds,
            require_one_lot=True,
        )
        is None
    ):
        return None
    side = "ask" if delta_contracts > 0 else "bid"
    filled, point_notional = sweep_five_level_depth_scalar(
        float(quantity),
        [float(row.get(f"{side}_price_{level}") or 0.0) for level in range(1, 6)],
        [float(row.get(f"{side}_volume_{level}") or 0.0) for level in range(1, 6)],
    )
    if int(round(float(filled))) != quantity:
        return None
    average = float(point_notional) / quantity
    if not math.isfinite(average) or average <= 0.0:
        return None
    return average, int(row["receive_ts_ns"])


def _txo_short_margin_twd(
    *,
    option_price: float,
    underlying_price: float,
    strike: float,
    option_right: str,
    risk_margin_a_twd: float,
    risk_margin_b_twd: float,
) -> float:
    right = str(option_right).upper()
    if right not in {"C", "P"}:
        raise ValueError(f"invalid option right for margin: {option_right!r}")
    return txo_short_initial_margin_per_contract_scalar(
        option_price,
        underlying_price,
        strike,
        option_right=right,
        contract_multiplier=OPTION_MULTIPLIER,
        risk_margin_a_twd=risk_margin_a_twd,
        risk_margin_b_twd=risk_margin_b_twd,
    )


def _executable_book(
    row: Mapping[str, Any] | None,
    *,
    decision_ns: int,
    maximum_age_seconds: float,
    require_one_lot: bool,
) -> tuple[float, float] | None:
    if row is None or bool(row.get("simtrade", False)):
        return None
    if int(row.get("receive_ts_ns") or 0) > int(decision_ns):
        return None
    if _book_age_seconds(row, decision_ns) > maximum_age_seconds:
        return None
    bid = _book_price(row, "bid")
    ask = _book_price(row, "ask")
    if bid is None or ask is None or bid > ask:
        return None
    if require_one_lot and (
        _book_volume(row, "bid") < 1 or _book_volume(row, "ask") < 1
    ):
        return None
    return bid, ask


def _sanitize_event(value: Any, *, key: str = "") -> Any:
    lowered = key.casefold()
    if lowered in {"person_id", "account_id", "broker_id", "username"}:
        text = str(value or "")
        return "***" if text else ""
    if isinstance(value, Mapping):
        return {str(k): _sanitize_event(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_event(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "to_dict"):
        try:
            return _sanitize_event(value.to_dict())
        except Exception:
            pass
    try:
        return _sanitize_event(dict(value))
    except Exception:
        return str(value)


@dataclass(frozen=True, slots=True)
class OptionInstrument:
    code: str
    root: str
    series: str
    expiry: date
    strike: float
    right: str
    contract: Any


@dataclass(frozen=True, slots=True)
class FuturesInstrument:
    logical_code: str
    code: str
    contract: Any
    last_trading_date: date | None = None


@dataclass(frozen=True, slots=True)
class PutCallParityCandidate:
    direction: str
    expiry: date
    series: str
    strike: float
    call: OptionInstrument
    put: OptionInstrument
    call_contracts: int
    put_contracts: int
    future_contracts: int
    call_price: float
    put_price: float
    future_price: float
    call_liquidation_price: float
    put_liquidation_price: float
    call_receive_ts_ns: int
    put_receive_ts_ns: int
    future_receive_ts_ns: int
    gross_locked_edge_twd: float
    entry_fixed_fees_twd: float
    entry_transaction_tax_twd: float
    estimated_settlement_tax_twd: float
    net_after_estimated_cost_twd: float

    def public_payload(self) -> dict[str, Any]:
        return {
            "direction": self.direction,
            "expiry_date": self.expiry.isoformat(),
            "series": self.series,
            "strike": self.strike,
            "call_code": self.call.code,
            "put_code": self.put.code,
            "future_code": None,
            "call_contracts": self.call_contracts,
            "put_contracts": self.put_contracts,
            "future_contracts": self.future_contracts,
            "call_price": self.call_price,
            "put_price": self.put_price,
            "future_price": self.future_price,
            "maximum_book_age_ms": None,
            "gross_locked_edge_twd": self.gross_locked_edge_twd,
            "entry_fixed_fees_twd": self.entry_fixed_fees_twd,
            "entry_transaction_tax_twd": self.entry_transaction_tax_twd,
            "estimated_settlement_tax_twd": self.estimated_settlement_tax_twd,
            "total_estimated_cost_twd": (
                self.entry_fixed_fees_twd
                + self.entry_transaction_tax_twd
                + self.estimated_settlement_tax_twd
            ),
            "net_after_estimated_cost_twd": self.net_after_estimated_cost_twd,
            "profitable_after_cost": (
                self.net_after_estimated_cost_twd > PUT_CALL_PARITY_MIN_NET_EDGE_TWD
            ),
        }


def option_instruments(infos: Iterable[Any]) -> tuple[OptionInstrument, ...]:
    output: list[OptionInstrument] = []
    for info in infos:
        if not hasattr(info, "option_right"):
            continue
        expiry = getattr(info, "delivery_date", None)
        if not isinstance(expiry, date):
            continue
        base = _base(info)
        output.append(
            OptionInstrument(
                code=str(getattr(base, "code")),
                root=str(getattr(info, "root", "")).strip().upper(),
                series=_series_id(info),
                expiry=expiry,
                strike=float(getattr(info, "strike_price")),
                right=_right(getattr(info, "option_right")),
                contract=base,
            )
        )
    output.sort(
        key=lambda item: (item.expiry, item.root, item.strike, item.right, item.code)
    )
    return tuple(output)


class TaifexVolatilitySimulation:
    """Stateful coordinator called from the existing Shioaji FOP worker."""

    def __init__(
        self,
        *,
        api: Any,
        shioaji_module: Any,
        state_dir: Path,
        option_infos: Iterable[Any],
        underlying: FuturesInstrument,
        hedge: FuturesInstrument,
        final_settlement_path: Path,
        calibration_time: datetime_time = datetime_time(13, 29),
        bootstrap_after: date | None = None,
        broker_orders_enabled: bool = True,
        strategy_mode: str = STRATEGY_MODE_DAILY,
        intraday_decision_interval_seconds: int = (
            DEFAULT_INTRADAY_DECISION_INTERVAL_SECONDS
        ),
        intraday_entry_cutoff: datetime_time = DEFAULT_INTRADAY_ENTRY_CUTOFF,
        intraday_flatten_time: datetime_time = DEFAULT_INTRADAY_FLATTEN_TIME,
        night_entry_cutoff: datetime_time = DEFAULT_NIGHT_ENTRY_CUTOFF,
        night_flatten_time: datetime_time = DEFAULT_NIGHT_FLATTEN_TIME,
        option_risk_margin_a_twd: float = DEFAULT_OPTION_RISK_MARGIN_A_TWD,
        option_risk_margin_b_twd: float = DEFAULT_OPTION_RISK_MARGIN_B_TWD,
        option_risk_margin_c_twd: float = DEFAULT_OPTION_RISK_MARGIN_C_TWD,
        strategy_capital_buffer_multiple: float = (
            DEFAULT_STRATEGY_CAPITAL_BUFFER_MULTIPLE
        ),
        catalog_expansion_entry_policy: str = CATALOG_EXPANSION_ENTRY_NEXT_CYCLE,
        settlement_bootstrap_only: bool = False,
        startup_now: datetime | None = None,
        active_strategy_ids: Iterable[str] | None = None,
    ) -> None:
        normalized_mode = str(strategy_mode).strip().lower()
        if normalized_mode not in STRATEGY_MODES:
            raise ValueError(f"unsupported strategy mode: {strategy_mode!r}")
        if int(intraday_decision_interval_seconds) < 1:
            raise ValueError("intraday decision interval must be positive")
        requested_strategy_ids = tuple(
            dict.fromkeys(
                str(strategy_id)
                for strategy_id in (
                    STRATEGY_IDS
                    if active_strategy_ids is None
                    else active_strategy_ids
                )
            )
        )
        if not requested_strategy_ids:
            raise ValueError("active strategy scope must not be empty")
        unknown_requested_strategy_ids = set(requested_strategy_ids) - set(
            STRATEGY_IDS
        )
        if unknown_requested_strategy_ids:
            raise ValueError(
                "active strategy scope contains unknown variants: "
                + ",".join(sorted(unknown_requested_strategy_ids))
            )
        if not DAY_OPEN < intraday_entry_cutoff < intraday_flatten_time:
            raise ValueError(
                "intraday clock must satisfy day open < entry cutoff < flatten time"
            )
        if not night_entry_cutoff < night_flatten_time < NIGHT_CLOSE:
            raise ValueError(
                "night clock must satisfy entry cutoff < flatten time < night close"
            )
        margin_a = float(option_risk_margin_a_twd)
        margin_b = float(option_risk_margin_b_twd)
        margin_c = float(option_risk_margin_c_twd)
        capital_buffer_multiple = float(strategy_capital_buffer_multiple)
        if not math.isfinite(capital_buffer_multiple) or capital_buffer_multiple < 1.0:
            raise ValueError("strategy capital buffer multiple must be finite and >= 1")
        expansion_policy = str(catalog_expansion_entry_policy).strip().lower()
        if expansion_policy not in CATALOG_EXPANSION_ENTRY_POLICIES:
            raise ValueError(
                "unsupported catalog expansion entry policy: "
                f"{catalog_expansion_entry_policy!r}"
            )
        if (
            not math.isfinite(margin_a)
            or not math.isfinite(margin_b)
            or not math.isfinite(margin_c)
            or margin_a <= 0.0
            or margin_b <= 0.0
            or margin_c <= 0.0
            or margin_b > margin_a
            or margin_c > margin_b
        ):
            raise ValueError("option risk margins must satisfy finite A >= B >= C > 0")
        self.api = api
        self.sj = shioaji_module
        self.state_dir = Path(state_dir)
        self.state_path = self.state_dir / "state.json"
        self.status_path = self.state_dir / "status.json"
        self.events_path = self.state_dir / "events.jsonl"
        self.ledger_path = self.state_dir / "ideal_ledger.jsonl"
        self.capital_contributions_path = (
            self.state_dir / "capital_contributions.jsonl"
        )
        self.marks_path = self.state_dir / "marks.jsonl"
        self.calibrations_path = self.state_dir / "calibrations.jsonl"
        self.final_settlement_path = Path(final_settlement_path)
        self.calibration_time = calibration_time
        self.strategy_mode = normalized_mode
        self.intraday_decision_interval_seconds = int(
            intraday_decision_interval_seconds
        )
        self.intraday_entry_cutoff = intraday_entry_cutoff
        self.intraday_flatten_time = intraday_flatten_time
        self.night_entry_cutoff = night_entry_cutoff
        self.night_flatten_time = night_flatten_time
        self.option_risk_margin_a_twd = margin_a
        self.option_risk_margin_b_twd = margin_b
        self.option_risk_margin_c_twd = margin_c
        self.strategy_capital_buffer_multiple = capital_buffer_multiple
        self.catalog_expansion_entry_policy = expansion_policy
        self.settlement_bootstrap_only = bool(settlement_bootstrap_only)
        self.strategy_ids = requested_strategy_ids
        self.underlying = underlying
        self.hedge = hedge
        self.underlying_product = _continuous_future_product(underlying.logical_code)
        self.underlying_multiplier = float(
            TAIFEX_INDEX_FUTURES_MULTIPLIERS[self.underlying_product]
        )
        self.underlying_fee_per_side_twd = FUTURES_FEE_PER_SIDE_TWD[
            self.underlying_product
        ]
        self.hedge_product = _continuous_future_product(hedge.logical_code)
        self.hedge_multiplier = float(
            TAIFEX_INDEX_FUTURES_MULTIPLIERS[self.hedge_product]
        )
        self.hedge_fee_per_side_twd = FUTURES_FEE_PER_SIDE_TWD[self.hedge_product]
        self.options = option_instruments(option_infos)
        self.options_by_code = {item.code: item for item in self.options}
        self.contracts_by_code = {
            underlying.code: underlying.contract,
            hedge.code: hedge.contract,
            **{item.code: item.contract for item in self.options},
        }
        self.latest_books: dict[str, dict[str, Any]] = {}
        self.callback_queue: queue.Queue[tuple[str, Any]] = queue.Queue(10_000)
        self.callback_overflow = threading.Event()
        self.last_mark_monotonic = 0.0
        self.last_status_monotonic = 0.0
        self.broker_orders_enabled = bool(broker_orders_enabled)
        self.account = getattr(api, "futopt_account", None)
        if self.account is None:
            raise RuntimeError("simulation login has no futures/options account")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.lock_handle = (self.state_dir / "engine.lock").open("a+")
        try:
            fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                "another TAIFEX strategy engine holds engine.lock"
            ) from exc
        try:
            self.state = self._load_or_initialize_state(bootstrap_after)
            if self.settlement_bootstrap_only:
                self.broker_orders_enabled = False
                self._settle_expired_positions_for_subscription_bootstrap(
                    startup_now or datetime.now(TAIPEI)
                )
                self._write_status(force=True)
                return
            missing_required_codes = sorted(
                set(required_option_codes(self.state)) - set(self.options_by_code)
            )
            if missing_required_codes:
                raise RuntimeError(
                    "persisted held or pending-roll option contracts are not "
                    "subscribed on strategy worker 0: "
                    + ",".join(missing_required_codes)
                )
            # A prior timeout/reconciliation failure is a persistent fail-closed
            # decision.  A process restart must not silently re-enable broker-side
            # simulation orders merely because the CLI default is true.
            self.broker_orders_enabled = self.broker_orders_enabled and bool(
                self.state.get("broker_orders_enabled", False)
            )
            self.state["broker_orders_enabled"] = self.broker_orders_enabled
            self._reconcile_inflight_orders()
            self._write_status(force=True)
        except Exception:
            try:
                fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.lock_handle.close()
            raise

    def _repair_v8_restart_fabricated_active_cycle_pairs(
        self,
        payload: dict[str, Any],
        active_cycle: Mapping[str, Any],
    ) -> list[str]:
        """Remove the exact unbacked ATM pair fabricated by the v8 restart bug."""

        call_code = str(active_cycle.get("call_code") or "")
        put_code = str(active_cycle.get("put_code") or "")
        if not call_code or not put_code or call_code == put_code:
            return []
        active_codes = {call_code, put_code}
        net_ledger_deltas: dict[str, dict[str, int]] = {
            strategy_id: {call_code: 0, put_code: 0}
            for strategy_id in self.strategy_ids
        }
        if self.ledger_path.is_file():
            with self.ledger_path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise RuntimeError(
                            "cannot audit v8 restart positions against malformed "
                            f"ideal ledger line {line_number}"
                        ) from exc
                    if (
                        row.get("instrument_type") != "option"
                        or str(row.get("code") or "") not in active_codes
                    ):
                        continue
                    strategy_id = str(row.get("strategy_id") or "")
                    if strategy_id not in net_ledger_deltas:
                        continue
                    code = str(row["code"])
                    try:
                        delta = int(row["delta_contracts"])
                    except (KeyError, TypeError, ValueError) as exc:
                        raise RuntimeError(
                            "cannot audit v8 restart positions against invalid "
                            f"ideal ledger quantity at line {line_number}"
                        ) from exc
                    net_ledger_deltas[strategy_id][code] += delta

        cycle_entries = active_cycle.get("strategy_entries") or {}
        repaired: list[str] = []
        for strategy_id, ledger in (payload.get("strategies") or {}).items():
            if not isinstance(ledger, dict):
                continue
            positions = {
                str(code): int(quantity)
                for code, quantity in (ledger.get("option_positions") or {}).items()
                if int(quantity) != 0
            }
            if positions != {call_code: 1, put_code: 1}:
                continue
            ledger_deltas = net_ledger_deltas.get(str(strategy_id), {})
            if any(int(ledger_deltas.get(code, 0)) != 0 for code in active_codes):
                continue
            ledger["option_positions"] = {}
            metadata = ledger.get("option_position_metadata")
            if isinstance(metadata, dict):
                metadata.pop(call_code, None)
                metadata.pop(put_code, None)
                if not metadata:
                    ledger["option_position_metadata"] = {}
            if not bool(ledger.get("alive", True)):
                ledger["entry_state"] = "ruined"
            elif str(strategy_id) == PUT_CALL_PARITY_TX_STRATEGY_ID:
                parity_state = payload.get("put_call_parity_tx") or {}
                monitor_state = str(
                    (parity_state.get("monitor") or {}).get("state") or ""
                )
                ledger["entry_state"] = (
                    "waiting_for_profitable_parity"
                    if monitor_state == "no_positive_edge_after_cost"
                    else monitor_state or "waiting_for_same_expiry_monthly_books"
                )
            else:
                cycle_entry = str(cycle_entries.get(strategy_id) or "pending")
                ledger["entry_state"] = cycle_entry
            for key in (
                "last_complete_open_liquidation_value_twd",
                "last_complete_mark_decision_ts_ns",
                "last_complete_mark_cycle_id",
                "last_complete_mark_futures_position",
                "last_complete_mark_underlying_futures_position",
                "last_complete_mark_option_positions",
            ):
                ledger[key] = None
            repaired.append(str(strategy_id))
        return sorted(repaired)

    def _load_or_initialize_state(self, bootstrap_after: date | None) -> dict[str, Any]:
        if self.state_path.is_file():
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if int(payload.get("schema_version", -1)) != SCHEMA_VERSION:
                raise RuntimeError("strategy state schema mismatch")
            if payload.get("simulation_only") is not True:
                raise RuntimeError("strategy state is not marked simulation_only")
            stored_strategy_ids = tuple(payload.get("strategy_ids", ()))
            unknown_strategy_ids = set(stored_strategy_ids) - set(STRATEGY_IDS)
            if unknown_strategy_ids:
                raise RuntimeError(
                    "strategy state contains unknown variants: "
                    + ",".join(sorted(unknown_strategy_ids))
                )
            out_of_scope_strategy_ids = set(stored_strategy_ids) - set(
                self.strategy_ids
            )
            if out_of_scope_strategy_ids:
                raise RuntimeError(
                    "strategy state exceeds the configured active scope: "
                    + ",".join(sorted(out_of_scope_strategy_ids))
                )
            execution_version = int(payload.get("execution_contract_version", 1))
            migration_reasons: list[str] = []
            repaired_v8_restart_strategy_ids: list[str] = []
            if execution_version in {1, 2}:
                nonflat = any(
                    int(row.get("futures_position", 0)) != 0
                    for row in (payload.get("strategies") or {}).values()
                    if isinstance(row, Mapping)
                )
                inflight = payload.get("inflight_orders") or {}
                unsafe_inflight = any(
                    not isinstance(intent, Mapping)
                    or str(intent.get("strategy_id")) != "shared_option_position"
                    or str(intent.get("code")) not in self.options_by_code
                    for intent in inflight.values()
                )
                if nonflat or unsafe_inflight:
                    raise RuntimeError(
                        "execution-contract migration requires flat futures and "
                        "no non-option inflight orders"
                    )
                if execution_version == 1 and payload.get("active_cycle") is not None:
                    raise RuntimeError(
                        "v1 execution-contract migration requires no active cycle"
                    )
                payload["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
                payload.setdefault("strategy_mode", self.strategy_mode)
                payload.setdefault("last_intraday_decision_bucket", None)
                payload.setdefault("last_intraday_flatten_date", None)
                payload.setdefault("last_intraday_decision_session", None)
                payload.setdefault("last_intraday_flatten_session", None)
                payload["hedge_product"] = self.hedge_product
                payload["hedge_multiplier_twd_per_point"] = self.hedge_multiplier
                payload["hedge_fee_per_side_twd"] = self.hedge_fee_per_side_twd
                migration_reasons.append("flat_state_safe_migration")
            elif execution_version == 3:
                # Version 4 changes reporting only.  The ideal trade ledger,
                # broker intents, and positions are deliberately untouched;
                # version 5 additionally enables session-keyed night trading.
                payload["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
                migration_reasons.append("reporting_equity_carry_forward_migration")
            elif execution_version == 4:
                payload["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
                migration_reasons.append("day_night_session_execution_migration")
            elif execution_version == 5:
                payload["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
                migration_reasons.append("multi_strategy_option_ledger_migration")
            elif execution_version == 6:
                payload["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
                migration_reasons.append("official_margin_schedule_migration")
            elif execution_version == 7:
                payload["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
                migration_reasons.append("same_expiry_put_call_parity_tx_migration")
            elif execution_version == 8:
                payload["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
                migration_reasons.append("restart_safe_active_cycle_migration")
            elif execution_version == 9:
                payload["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
                migration_reasons.append("causal_rolling_straddle_migration")
            elif execution_version == 10:
                payload["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
                migration_reasons.append(
                    "next_trading_date_recapitalization_ledger_migration"
                )
            elif execution_version == 11:
                payload["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
                migration_reasons.append("gamma_scalping_trigger_state_migration")
            elif execution_version != EXECUTION_CONTRACT_VERSION:
                raise RuntimeError("strategy execution contract mismatch")

            active_cycle = payload.get("active_cycle")
            active_cycle_mapping = (
                active_cycle if isinstance(active_cycle, dict) else None
            )
            strategies = payload.setdefault("strategies", {})
            for strategy_id in stored_strategy_ids:
                ledger = strategies.setdefault(strategy_id, _new_strategy_ledger())
                legacy_positions = ledger.setdefault("option_positions", {})
                legacy_metadata = ledger.setdefault("option_position_metadata", {})
                if (
                    execution_version < 5
                    and active_cycle_mapping
                    and not legacy_positions
                ):
                    for right, key in (("C", "call_code"), ("P", "put_code")):
                        code = str(active_cycle_mapping.get(key) or "")
                        if not code:
                            continue
                        legacy_positions[code] = 1
                        legacy_metadata[code] = {
                            "series": active_cycle_mapping.get("series"),
                            "strike": active_cycle_mapping.get("strike"),
                            "option_right": right,
                        }
                    ledger["entry_state"] = "entered"
                else:
                    ledger.setdefault(
                        "entry_state",
                        "entered" if active_cycle_mapping else "pending",
                    )
                ledger.setdefault("last_dynamic_hedge_cycle_id", None)
                ledger.setdefault("last_dynamic_hedge_session_id", None)
                ledger.setdefault("last_dynamic_hedge_decision_ts_ns", None)
                ledger.setdefault("last_dynamic_hedge_forward_mid", None)
            if execution_version == 8 and active_cycle_mapping:
                repaired_v8_restart_strategy_ids = (
                    self._repair_v8_restart_fabricated_active_cycle_pairs(
                        payload,
                        active_cycle_mapping,
                    )
                )
            added_strategy_ids = [
                strategy_id
                for strategy_id in self.strategy_ids
                if strategy_id not in stored_strategy_ids
            ]
            enter_added_now = bool(
                active_cycle_mapping
                and self.catalog_expansion_entry_policy
                == CATALOG_EXPANSION_ENTRY_IMMEDIATE_LIVE
            )
            added_entry_state = (
                "pending"
                if enter_added_now or not active_cycle_mapping
                else "waiting_next_cycle"
            )
            for strategy_id in added_strategy_ids:
                strategies[strategy_id] = _new_strategy_ledger(
                    entry_state=added_entry_state
                )
            if added_strategy_ids or stored_strategy_ids != self.strategy_ids:
                payload["strategy_ids"] = list(self.strategy_ids)
                migration_reasons.append(
                    "strategy_catalog_expanded_immediate_live"
                    if enter_added_now
                    else "strategy_catalog_expanded"
                )
            if active_cycle_mapping:
                entries = active_cycle_mapping.setdefault("strategy_entries", {})
                for strategy_id in stored_strategy_ids:
                    entries.setdefault(strategy_id, "entered")
                for strategy_id in added_strategy_ids:
                    entries[strategy_id] = added_entry_state
                if enter_added_now:
                    active_cycle_mapping["catalog_expansion_entry_timing"] = (
                        "first_complete_fresh_book_after_runtime_restart"
                    )
                active_cycle_mapping.setdefault("broker_reference_opened", True)
            payload["catalog_expansion_entry_policy"] = (
                self.catalog_expansion_entry_policy
            )
            payload.setdefault("underlying_product", self.underlying_product)
            payload.setdefault(
                "underlying_multiplier_twd_per_point", self.underlying_multiplier
            )
            payload.setdefault(
                "underlying_fee_per_side_twd", self.underlying_fee_per_side_twd
            )
            parity_state = payload.setdefault(
                "put_call_parity_tx", _new_put_call_parity_state()
            )
            for key, value in _new_put_call_parity_state().items():
                parity_state.setdefault(key, value)
            payload.setdefault("last_intraday_decision_session", None)
            stored_margin_a = float(
                payload.get("option_risk_margin_a_twd", TXO_RISK_MARGIN_TWD["A"])
            )
            stored_margin_b = float(
                payload.get("option_risk_margin_b_twd", TXO_RISK_MARGIN_TWD["B"])
            )
            stored_margin_c = float(
                payload.get("option_risk_margin_c_twd", TXO_RISK_MARGIN_TWD["C"])
            )
            official_margin_step = (
                execution_version <= EXECUTION_CONTRACT_VERSION
                and stored_margin_a == TXO_RISK_MARGIN_TWD["A"]
                and stored_margin_b == TXO_RISK_MARGIN_TWD["B"]
                and stored_margin_c
                in {TXO_RISK_MARGIN_TWD["C"], TXO_RISK_MARGIN_TWD_2026_08_13["C"]}
                and self.option_risk_margin_a_twd == TXO_RISK_MARGIN_TWD_2026_08_13["A"]
                and self.option_risk_margin_b_twd == TXO_RISK_MARGIN_TWD_2026_08_13["B"]
                and self.option_risk_margin_c_twd == TXO_RISK_MARGIN_TWD_2026_08_13["C"]
            )
            if official_margin_step:
                payload["option_risk_margin_a_twd"] = self.option_risk_margin_a_twd
                payload["option_risk_margin_b_twd"] = self.option_risk_margin_b_twd
                payload["option_risk_margin_c_twd"] = self.option_risk_margin_c_twd
                migration_reasons.append("taifex_2026_08_13_margin_values")
            else:
                payload.setdefault(
                    "option_risk_margin_a_twd", self.option_risk_margin_a_twd
                )
                payload.setdefault(
                    "option_risk_margin_b_twd", self.option_risk_margin_b_twd
                )
                payload.setdefault(
                    "option_risk_margin_c_twd", self.option_risk_margin_c_twd
                )
            payload["option_risk_margin_effective_trading_date"] = (
                TAIFEX_MARGIN_2026_08_13_EFFECTIVE_DATE.isoformat()
            )
            payload["option_risk_margin_source_url"] = (
                TAIFEX_MARGIN_2026_08_13_ANNOUNCEMENT_URL
            )
            payload["option_margin_policy"] = OPTION_MARGIN_POLICY
            payload["strategy_recapitalization_policy"] = (
                STRATEGY_RECAPITALIZATION_POLICY
            )
            payload.setdefault(
                "strategy_capital_buffer_multiple",
                self.strategy_capital_buffer_multiple,
            )
            legacy_flatten_date = payload.get("last_intraday_flatten_date")
            payload.setdefault(
                "last_intraday_flatten_session",
                f"{legacy_flatten_date}:day" if legacy_flatten_date else None,
            )
            self._ensure_strategy_reporting_state(payload)
            self._ensure_strategy_recapitalization_state(
                payload,
                migrated_from_execution_version=execution_version,
            )
            if str(payload.get("strategy_mode")) != self.strategy_mode:
                raise RuntimeError(
                    "strategy state mode mismatch: "
                    f"state={payload.get('strategy_mode')} runtime={self.strategy_mode}"
                )
            if str(payload.get("hedge_product")) != self.hedge_product:
                raise RuntimeError(
                    "strategy hedge product mismatch: "
                    f"state={payload.get('hedge_product')} "
                    f"runtime={self.hedge_product}"
                )
            if str(payload.get("underlying_product")) != self.underlying_product:
                raise RuntimeError(
                    "strategy underlying product mismatch: "
                    f"state={payload.get('underlying_product')} "
                    f"runtime={self.underlying_product}"
                )
            if (
                float(
                    payload.get(
                        "option_risk_margin_a_twd",
                        self.option_risk_margin_a_twd,
                    )
                )
                != self.option_risk_margin_a_twd
                or float(
                    payload.get(
                        "option_risk_margin_b_twd",
                        self.option_risk_margin_b_twd,
                    )
                )
                != self.option_risk_margin_b_twd
                or float(
                    payload.get(
                        "option_risk_margin_c_twd",
                        self.option_risk_margin_c_twd,
                    )
                )
                != self.option_risk_margin_c_twd
                or float(
                    payload.get(
                        "strategy_capital_buffer_multiple",
                        self.strategy_capital_buffer_multiple,
                    )
                )
                != self.strategy_capital_buffer_multiple
            ):
                raise RuntimeError(
                    "strategy option risk-margin/capital contract mismatch"
                )
            if migration_reasons:
                self.state = payload
                self._persist_state()
                event: dict[str, Any] = {
                    "event": "execution_contract_migrated",
                    "at_utc": _now_iso(),
                    "from_version": execution_version,
                    "to_version": EXECUTION_CONTRACT_VERSION,
                    "strategy_mode": self.strategy_mode,
                    "reason": "+".join(dict.fromkeys(migration_reasons)),
                }
                if official_margin_step:
                    event["margin_schedule"] = {
                        "effective_trading_date": (
                            TAIFEX_MARGIN_2026_08_13_EFFECTIVE_DATE.isoformat()
                        ),
                        "source_url": TAIFEX_MARGIN_2026_08_13_ANNOUNCEMENT_URL,
                        "before_twd": dict(TXO_RISK_MARGIN_TWD),
                        "after_twd": dict(TXO_RISK_MARGIN_TWD_2026_08_13),
                        "positions_and_pnl_preserved": True,
                    }
                if repaired_v8_restart_strategy_ids:
                    event["repaired_v8_restart_strategy_ids"] = (
                        repaired_v8_restart_strategy_ids
                    )
                    event["repair_source"] = (
                        "active_cycle_pair_vs_immutable_ideal_trade_ledger"
                    )
                _append_jsonl(self.events_path, event)
            return payload
        today = datetime.now(TAIPEI).date()
        weekly_expiries = sorted(
            {
                item.expiry
                for item in self.options
                if item.root != "TXO" and item.expiry >= today
            }
        )
        inferred_bootstrap = bootstrap_after or (
            weekly_expiries[0] if weekly_expiries else today
        )
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "execution_contract_version": EXECUTION_CONTRACT_VERSION,
            "simulation_only": True,
            "production_order_possible": False,
            "strategy_ids": list(self.strategy_ids),
            "strategy_mode": self.strategy_mode,
            "underlying_product": self.underlying_product,
            "underlying_multiplier_twd_per_point": self.underlying_multiplier,
            "underlying_fee_per_side_twd": self.underlying_fee_per_side_twd,
            "hedge_product": self.hedge_product,
            "hedge_multiplier_twd_per_point": self.hedge_multiplier,
            "hedge_fee_per_side_twd": self.hedge_fee_per_side_twd,
            "option_risk_margin_a_twd": self.option_risk_margin_a_twd,
            "option_risk_margin_b_twd": self.option_risk_margin_b_twd,
            "option_risk_margin_c_twd": self.option_risk_margin_c_twd,
            "option_risk_margin_effective_trading_date": (
                TAIFEX_MARGIN_2026_08_13_EFFECTIVE_DATE.isoformat()
            ),
            "option_risk_margin_source_url": (
                TAIFEX_MARGIN_2026_08_13_ANNOUNCEMENT_URL
            ),
            "option_margin_policy": OPTION_MARGIN_POLICY,
            "strategy_recapitalization_policy": STRATEGY_RECAPITALIZATION_POLICY,
            "strategy_capital_buffer_multiple": (self.strategy_capital_buffer_multiple),
            "catalog_expansion_entry_policy": (self.catalog_expansion_entry_policy),
            "created_at_utc": _now_iso(),
            "updated_at_utc": _now_iso(),
            "bootstrap_after_date": inferred_bootstrap.isoformat(),
            "active_cycle": None,
            "pending_targets": {},
            "last_calibration_date": None,
            "last_settled_expiry": None,
            "last_intraday_decision_bucket": None,
            "last_intraday_decision_session": None,
            "last_intraday_flatten_date": None,
            "last_intraday_flatten_session": None,
            "broker_sequence": 0,
            "inflight_orders": {},
            "broker_order_failures": 0,
            "broker_orders_enabled": self.broker_orders_enabled,
            "engine_status": "waiting_for_bootstrap",
            "blocked_reason": None,
            "put_call_parity_tx": _new_put_call_parity_state(),
            "strategies": {
                strategy_id: _new_strategy_ledger()
                for strategy_id in self.strategy_ids
            },
        }
        self.state = payload
        self._persist_state()
        _append_jsonl(
            self.events_path,
            {
                "event": "state_initialized",
                "at_utc": _now_iso(),
                "bootstrap_after_date": inferred_bootstrap.isoformat(),
                "reason": "do_not_reconstruct_an_already_open_weekly_cycle",
            },
        )
        return payload

    def _strategy_initial_capital_twd(
        self,
        strategy_id: str,
        *,
        state: Mapping[str, Any] | None = None,
    ) -> float:
        """Return a conservative first-cycle funding base for one live policy."""

        current_state = state if state is not None else self.state
        ledger = current_state["strategies"][strategy_id]
        spec = STRATEGY_SPEC_BY_ID[strategy_id]
        option_capital = float(ledger.get("entry_capital_requirement_twd") or 0.0)
        if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID:
            if option_capital > 0.0:
                return float(option_capital * self.strategy_capital_buffer_multiple)
            margin_date = taifex_trading_date(datetime.now(TAIPEI))
            conservative_package_capital = (
                PUT_CALL_PARITY_OPTION_QUANTITY * self.option_risk_margin_a_twd
                + taifex_initial_margin_twd(self.underlying_product, margin_date)
                + PUT_CALL_PARITY_OPTION_QUANTITY * 2 * OPTION_FEE_PER_SIDE_TWD
                + self.underlying_fee_per_side_twd
            )
            return float(
                conservative_package_capital * self.strategy_capital_buffer_multiple
            )
        margin_contracts = 0
        if spec.hedge_policy == "fixed_future":
            margin_contracts = abs(int(spec.hedge_parameter or 0.0))
        elif spec.hedge_policy == "fixed_index_equivalent":
            margin_contracts = max(
                1,
                int(math.ceil(OPTION_MULTIPLIER / self.hedge_multiplier)),
            )
        elif spec.hedge_policy != "none":
            margin_contracts = max(
                1,
                int(math.ceil(OPTION_MULTIPLIER / self.hedge_multiplier)),
            )
        if margin_contracts == 0:
            return float(option_capital * self.strategy_capital_buffer_multiple)
        cycle = current_state.get("active_cycle") or {}
        entry_date = date.fromisoformat(
            str(cycle.get("entry_date") or datetime.now(TAIPEI).date())
        )
        reference_price = float(cycle.get("strike") or 0.0)
        hedge_tax = taifex_tax_per_contract_twd(
            reference_price,
            multiplier_twd_per_point=self.hedge_multiplier,
            tax_rate=stock_index_futures_tax_rate(entry_date),
        )
        future_capital = margin_contracts * (
            taifex_initial_margin_twd(self.hedge_product, entry_date)
            + self.hedge_fee_per_side_twd
            + hedge_tax
        )
        hedge_book = self._hedge_book(time.time_ns())
        if hedge_book is not None:
            future_capital += (
                margin_contracts
                * (hedge_book[1] - hedge_book[0])
                * self.hedge_multiplier
            )
        return float(
            (option_capital + future_capital) * self.strategy_capital_buffer_multiple
        )

    def _strategy_flat_equity_twd(self, strategy_id: str) -> float:
        ledger = self.state["strategies"][strategy_id]
        if ledger.get("option_positions") or any(
            int(ledger.get(field) or 0) != 0
            for field in ("futures_position", "underlying_futures_position")
        ):
            raise RuntimeError(
                f"strategy recapitalization requires a flat ledger: {strategy_id}"
            )
        cumulative_pnl = (
            float(ledger.get("gross_cash_twd") or 0.0)
            - float(ledger.get("fees_twd") or 0.0)
            - float(ledger.get("tax_twd") or 0.0)
        )
        return float(
            float(ledger.get("cumulative_contributed_capital_twd") or 0.0)
            + cumulative_pnl
        )

    def _strategy_recapitalization_is_due(
        self,
        strategy_id: str,
        decision_ns: int,
    ) -> bool:
        ledger = self.state["strategies"][strategy_id]
        if bool(ledger.get("alive", True)):
            return False
        raw_boundary = ledger.get("last_bankruptcy_trading_date")
        if not raw_boundary:
            return False
        bankruptcy_trading_date = date.fromisoformat(str(raw_boundary))
        observed_at = datetime.fromtimestamp(decision_ns / 1e9, tz=TAIPEI)
        return taifex_trading_date(observed_at) > bankruptcy_trading_date

    def _record_capital_contribution(
        self,
        strategy_id: str,
        *,
        amount_twd: float,
        target_one_package_capital_twd: float,
        equity_before_contribution_twd: float,
        decision_ns: int,
        reason: str,
    ) -> None:
        amount = float(amount_twd)
        if not math.isfinite(amount) or amount <= 0.0:
            raise ValueError("capital contribution must be finite and positive")
        ledger = self.state["strategies"][strategy_id]
        trading_date = taifex_trading_date(
            datetime.fromtimestamp(decision_ns / 1e9, tz=TAIPEI)
        )
        ledger["cumulative_contributed_capital_twd"] = float(
            ledger.get("cumulative_contributed_capital_twd") or 0.0
        ) + amount
        ledger["capital_contribution_count"] = int(
            ledger.get("capital_contribution_count") or 0
        ) + 1
        if reason == "post_bankruptcy_one_package_recapitalization":
            ledger["recapitalization_count"] = int(
                ledger.get("recapitalization_count") or 0
            ) + 1
        ledger["last_capital_contribution_twd"] = amount
        ledger["last_capital_contribution_decision_ts_ns"] = int(decision_ns)
        ledger["last_capital_contribution_trading_date"] = trading_date.isoformat()
        row = {
            "schema_version": SCHEMA_VERSION,
            "execution_contract_version": EXECUTION_CONTRACT_VERSION,
            "event": "external_capital_contribution",
            "recorded_at_utc": _now_iso(),
            "decision_ts_ns": int(decision_ns),
            "trading_date": trading_date.isoformat(),
            "strategy_id": strategy_id,
            "reason": reason,
            "amount_twd": amount,
            "target_one_package_capital_twd": float(
                target_one_package_capital_twd
            ),
            "equity_before_contribution_twd": float(
                equity_before_contribution_twd
            ),
            "cumulative_contributed_capital_twd": float(
                ledger["cumulative_contributed_capital_twd"]
            ),
            "capital_contribution_count": int(
                ledger["capital_contribution_count"]
            ),
            "recapitalization_count": int(ledger.get("recapitalization_count") or 0),
            "last_bankruptcy_decision_ts_ns": ledger.get(
                "last_bankruptcy_decision_ts_ns"
            ),
            "last_bankruptcy_trading_date": ledger.get(
                "last_bankruptcy_trading_date"
            ),
            "last_bankruptcy_total_equity_twd": ledger.get(
                "last_bankruptcy_total_equity_twd"
            ),
            "policy": STRATEGY_RECAPITALIZATION_POLICY,
        }
        _append_jsonl(self.capital_contributions_path, row)
        _append_jsonl(self.events_path, row)

    def _prepare_strategy_capital_for_entry(
        self,
        strategy_id: str,
        *,
        decision_ns: int,
    ) -> bool:
        """Fund one complete strategy package without resetting cumulative P&L."""

        ledger = self.state["strategies"][strategy_id]
        first_lifetime_entry = int(ledger.get("trade_sides") or 0) == 0
        recapitalizing = not bool(ledger.get("alive", True))
        if recapitalizing and not self._strategy_recapitalization_is_due(
            strategy_id,
            decision_ns,
        ):
            return False
        target_capital = float(self._strategy_initial_capital_twd(strategy_id))
        if not math.isfinite(target_capital) or target_capital <= 0.0:
            raise RuntimeError(
                f"strategy one-package capital is not positive: {strategy_id}"
            )
        contributed = float(
            ledger.get("cumulative_contributed_capital_twd") or 0.0
        )
        if first_lifetime_entry and contributed <= 0.0:
            self._record_capital_contribution(
                strategy_id,
                amount_twd=target_capital,
                target_one_package_capital_twd=target_capital,
                equity_before_contribution_twd=0.0,
                decision_ns=decision_ns,
                reason="initial_one_package_capital",
            )
            ledger["initial_capital_twd"] = target_capital
        elif first_lifetime_entry and contributed < target_capital:
            equity_before = self._strategy_flat_equity_twd(strategy_id)
            self._record_capital_contribution(
                strategy_id,
                amount_twd=target_capital - contributed,
                target_one_package_capital_twd=target_capital,
                equity_before_contribution_twd=equity_before,
                decision_ns=decision_ns,
                reason="first_entry_one_package_capital_top_up",
            )
            ledger["initial_capital_twd"] = max(
                float(ledger.get("initial_capital_twd") or 0.0),
                target_capital,
            )
        elif recapitalizing:
            equity_before = self._strategy_flat_equity_twd(strategy_id)
            contribution = target_capital - equity_before
            if not math.isfinite(contribution) or contribution <= 0.0:
                raise RuntimeError(
                    "bankrupt strategy does not require a positive restoring "
                    f"contribution: {strategy_id} equity={equity_before} "
                    f"target={target_capital}"
                )
            self._record_capital_contribution(
                strategy_id,
                amount_twd=contribution,
                target_one_package_capital_twd=target_capital,
                equity_before_contribution_twd=equity_before,
                decision_ns=decision_ns,
                reason="post_bankruptcy_one_package_recapitalization",
            )
            ledger["alive"] = True
            ledger["forced_liquidation_pending"] = False
        elif contributed <= 0.0:
            self._record_capital_contribution(
                strategy_id,
                amount_twd=target_capital,
                target_one_package_capital_twd=target_capital,
                equity_before_contribution_twd=0.0,
                decision_ns=decision_ns,
                reason="legacy_flat_one_package_capital",
            )
            ledger["initial_capital_twd"] = max(
                float(ledger.get("initial_capital_twd") or 0.0),
                target_capital,
            )
        return True

    def _latest_complete_mark_cache(self) -> dict[str, dict[str, Any]]:
        """Recover the latest complete pre-v4 liquidation mark after restart."""

        latest: dict[str, dict[str, Any]] = {}
        if not self.marks_path.is_file():
            return latest
        with self.marks_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                    strategy_id = str(row.get("strategy_id") or "")
                    open_value = float(row["open_liquidation_value_twd"])
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    continue
                if (
                    strategy_id in self.strategy_ids
                    and bool(row.get("option_books_valid"))
                    and bool(row.get("future_book_valid"))
                    and math.isfinite(open_value)
                ):
                    latest[strategy_id] = {
                        "open_value": open_value,
                        "decision_ts_ns": int(row.get("decision_ts_ns") or 0),
                        "cycle_id": row.get("active_cycle_id"),
                        "futures_position": int(row.get("futures_position") or 0),
                        "underlying_futures_position": int(
                            row.get("underlying_futures_position") or 0
                        ),
                        "option_positions": row.get("option_positions"),
                    }
        return latest

    def _ensure_strategy_reporting_state(self, payload: dict[str, Any]) -> None:
        recovered = self._latest_complete_mark_cache()
        for strategy_id in self.strategy_ids:
            ledger = payload["strategies"][strategy_id]
            defaults = _new_strategy_ledger()
            for key, value in defaults.items():
                ledger.setdefault(key, value)
            if float(ledger.get("initial_capital_twd") or 0.0) <= 0.0:
                ledger["initial_capital_twd"] = self._strategy_initial_capital_twd(
                    strategy_id,
                    state=payload,
                )
            cached = recovered.get(strategy_id)
            if (
                ledger.get("last_complete_open_liquidation_value_twd") is None
                and cached is not None
            ):
                ledger["last_complete_open_liquidation_value_twd"] = cached[
                    "open_value"
                ]
                ledger["last_complete_mark_decision_ts_ns"] = cached["decision_ts_ns"]
                ledger["last_complete_mark_cycle_id"] = cached["cycle_id"]
                ledger["last_complete_mark_futures_position"] = cached[
                    "futures_position"
                ]
                ledger["last_complete_mark_underlying_futures_position"] = cached[
                    "underlying_futures_position"
                ]
                ledger["last_complete_mark_option_positions"] = cached.get(
                    "option_positions"
                )
            ledger.setdefault("last_complete_open_liquidation_value_twd", None)
            ledger.setdefault("last_complete_mark_decision_ts_ns", None)
            ledger.setdefault("last_complete_mark_cycle_id", None)
            ledger.setdefault("last_complete_mark_futures_position", None)
            ledger.setdefault("last_complete_mark_underlying_futures_position", None)
            ledger.setdefault("last_complete_mark_option_positions", None)

    def _latest_strategy_bankruptcy_boundaries(self) -> dict[str, tuple[int, date]]:
        """Recover the latest verified bankruptcy clock from the event journal."""

        latest: dict[str, tuple[int, date]] = {}
        if not self.events_path.is_file():
            return latest
        with self.events_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (
                    row.get("event") != "strategy_forced_liquidation"
                    or "ruin" not in str(row.get("reason") or "")
                ):
                    continue
                strategy_id = str(row.get("strategy_id") or "")
                if strategy_id not in self.strategy_ids:
                    continue
                try:
                    decision_ns = int(row.get("decision_ts_ns") or 0)
                    if decision_ns > 0:
                        observed_at = datetime.fromtimestamp(
                            decision_ns / 1e9,
                            tz=TAIPEI,
                        )
                    else:
                        raw_at = str(row["at_utc"])
                        observed_at = datetime.fromisoformat(
                            raw_at.replace("Z", "+00:00")
                        ).astimezone(TAIPEI)
                        decision_ns = int(observed_at.timestamp() * 1e9)
                except (KeyError, TypeError, ValueError):
                    continue
                boundary = taifex_trading_date(observed_at)
                previous = latest.get(strategy_id)
                if previous is None or decision_ns > previous[0]:
                    latest[strategy_id] = (decision_ns, boundary)
        return latest

    def _ensure_strategy_recapitalization_state(
        self,
        payload: dict[str, Any],
        *,
        migrated_from_execution_version: int,
    ) -> None:
        """Add the explicit external-capital ledger without rewriting old P&L."""

        bankruptcy_boundaries = (
            self._latest_strategy_bankruptcy_boundaries()
            if migrated_from_execution_version < EXECUTION_CONTRACT_VERSION
            else {}
        )
        current_boundary = taifex_trading_date(datetime.now(TAIPEI))
        for strategy_id in self.strategy_ids:
            ledger = payload["strategies"][strategy_id]
            initial_capital = max(
                0.0,
                float(ledger.get("initial_capital_twd") or 0.0),
            )
            ledger.setdefault(
                "cumulative_contributed_capital_twd",
                initial_capital,
            )
            ledger.setdefault(
                "capital_contribution_count",
                1 if initial_capital > 0.0 else 0,
            )
            ledger.setdefault("recapitalization_count", 0)
            ledger.setdefault("bankruptcy_count", 0)
            ledger.setdefault(
                "episode_count",
                1 if int(ledger.get("trade_sides") or 0) > 0 else 0,
            )
            ledger.setdefault("last_capital_contribution_twd", None)
            ledger.setdefault("last_capital_contribution_decision_ts_ns", None)
            ledger.setdefault("last_capital_contribution_trading_date", None)
            ledger.setdefault("last_bankruptcy_decision_ts_ns", None)
            ledger.setdefault("last_bankruptcy_trading_date", None)
            ledger.setdefault("last_bankruptcy_total_equity_twd", None)
            if (
                migrated_from_execution_version < EXECUTION_CONTRACT_VERSION
                and float(
                    ledger.get("cumulative_contributed_capital_twd") or 0.0
                )
                <= 0.0
                and initial_capital > 0.0
            ):
                ledger["cumulative_contributed_capital_twd"] = initial_capital
                ledger["capital_contribution_count"] = 1
            if (
                migrated_from_execution_version < EXECUTION_CONTRACT_VERSION
                and int(ledger.get("episode_count") or 0) == 0
                and int(ledger.get("trade_sides") or 0) > 0
            ):
                ledger["episode_count"] = 1
            if migrated_from_execution_version >= EXECUTION_CONTRACT_VERSION:
                continue
            if bool(ledger.get("alive", True)):
                continue
            has_open_position = bool(ledger.get("option_positions")) or any(
                int(ledger.get(field) or 0) != 0
                for field in (
                    "futures_position",
                    "underlying_futures_position",
                )
            )
            if has_open_position:
                ledger["entry_state"] = "forced_liquidation_pending"
                ledger["forced_liquidation_pending"] = True
                continue
            recovered = bankruptcy_boundaries.get(strategy_id)
            if recovered is None:
                recovered = (
                    int(datetime.now(TAIPEI).timestamp() * 1e9),
                    current_boundary,
                )
            ledger["last_bankruptcy_decision_ts_ns"] = int(recovered[0])
            ledger["last_bankruptcy_trading_date"] = recovered[1].isoformat()
            ledger["bankruptcy_count"] = max(
                1,
                int(ledger.get("bankruptcy_count") or 0),
            )
            ledger["entry_state"] = "awaiting_next_trading_date_recapitalization"

    def _persist_state(self) -> None:
        self.state["updated_at_utc"] = _now_iso()
        _atomic_json(self.state_path, self.state)

    def on_book(self, row: Mapping[str, Any]) -> None:
        """Non-blocking market-data callback handoff."""

        self.latest_books[str(row["code"])] = dict(row)

    def on_tick(self, _row: Mapping[str, Any]) -> None:
        return

    def on_order_event(self, state: Any, message: Any) -> None:
        """Non-blocking order callback handoff."""

        try:
            self.callback_queue.put_nowait((str(state), message))
        except queue.Full:
            self.callback_overflow.set()

    def _drain_callbacks(self) -> None:
        if self.callback_overflow.is_set():
            self.broker_orders_enabled = False
            self.state["broker_orders_enabled"] = False
            self.state["blocked_reason"] = "order_callback_queue_overflow"
        changed = False
        while True:
            try:
                callback_state, raw_payload = self.callback_queue.get_nowait()
            except queue.Empty:
                break
            payload = _sanitize_event(raw_payload)
            _append_jsonl(
                self.events_path,
                {
                    "event": "broker_order_callback",
                    "at_utc": _now_iso(),
                    "state": callback_state,
                    "payload": payload,
                },
            )
            operation = (
                payload.get("operation", {}) if isinstance(payload, Mapping) else {}
            )
            if operation and str(operation.get("op_code", "")) != "00":
                self.state["broker_order_failures"] = (
                    int(self.state.get("broker_order_failures", 0)) + 1
                )
            order = payload.get("order", {}) if isinstance(payload, Mapping) else {}
            status = payload.get("status", {}) if isinstance(payload, Mapping) else {}
            trade_id = str(
                order.get("id") or status.get("id") or payload.get("trade_id") or ""
            )
            if trade_id:
                for custom, inflight in list(self.state["inflight_orders"].items()):
                    if str(inflight.get("trade_id", "")) == trade_id:
                        event_name = callback_state.casefold()
                        if (
                            "deal" in event_name
                            or int(status.get("cancel_quantity", 0) or 0) > 0
                        ):
                            self.state["inflight_orders"].pop(custom, None)
                            changed = True
        if changed:
            self._persist_state()

    def _trade_status(self, trade: Any) -> str:
        return _enum(getattr(getattr(trade, "status", None), "status", "Unknown"))

    def _reconcile_inflight_orders(self) -> None:
        inflight = dict(self.state.get("inflight_orders") or {})
        if not inflight:
            return
        try:
            self.api.update_status(self.account)
            trades = list(self.api.list_trades())
        except Exception as exc:
            self.broker_orders_enabled = False
            self.state["broker_orders_enabled"] = False
            self.state["blocked_reason"] = (
                f"inflight_reconciliation_failed:{type(exc).__name__}:{exc}"
            )
            self._persist_state()
            return
        by_custom = {
            str(getattr(getattr(trade, "order", None), "custom_field", "")): trade
            for trade in trades
        }
        unresolved: list[str] = []
        for custom, intent in inflight.items():
            trade = by_custom.get(custom)
            if trade is None:
                unresolved.append(custom)
                continue
            status = self._trade_status(trade)
            if status not in TERMINAL_ORDER_STATUSES:
                try:
                    self.api.cancel_order(trade)
                    time.sleep(0.25)
                    self.api.update_status(trade=trade)
                    status = self._trade_status(trade)
                except Exception:
                    unresolved.append(custom)
                    continue
            if status in TERMINAL_ORDER_STATUSES:
                self.state["inflight_orders"].pop(custom, None)
            else:
                unresolved.append(custom)
        if unresolved:
            self.broker_orders_enabled = False
            self.state["broker_orders_enabled"] = False
            self.state["blocked_reason"] = (
                f"unresolved_broker_orders:{','.join(unresolved)}"
            )
        self._persist_state()

    def _submit_broker_order(
        self,
        *,
        code: str,
        delta_contracts: int,
        strategy_id: str,
        reason: str,
    ) -> None:
        if delta_contracts == 0 or not self.broker_orders_enabled:
            return
        contract = self.contracts_by_code.get(code)
        if contract is None:
            raise LookupError(f"broker contract unavailable: {code}")
        self.state["broker_sequence"] = int(self.state.get("broker_sequence", 0)) + 1
        sequence = int(self.state["broker_sequence"])
        custom = f"V{sequence % 100000:05d}"
        action = self.sj.Action.Buy if delta_contracts > 0 else self.sj.Action.Sell
        intent = {
            "custom_field": custom,
            "created_at_utc": _now_iso(),
            "code": code,
            "delta_contracts": int(delta_contracts),
            "strategy_id": strategy_id,
            "reason": reason,
            "status": "intent_persisted_before_place_order",
        }
        self.state["inflight_orders"][custom] = intent
        self._persist_state()
        _append_jsonl(self.events_path, {"event": "broker_order_intent", **intent})
        try:
            order = self.sj.FuturesOrder(
                action=action,
                price=0,
                quantity=abs(int(delta_contracts)),
                price_type=self.sj.FuturesPriceType.MKT,
                order_type=self.sj.OrderType.IOC,
                octype=self.sj.FuturesOCType.Auto,
                custom_field=custom,
                account=self.account,
            )
            trade = self.api.place_order(contract, order)
            trade_id = str(
                getattr(getattr(trade, "order", None), "id", "")
                or getattr(getattr(trade, "status", None), "id", "")
            )
            status = self._trade_status(trade)
            self.state["inflight_orders"][custom].update(
                {"trade_id": trade_id, "initial_status": status}
            )
            _append_jsonl(
                self.events_path,
                {
                    "event": "broker_order_response",
                    "at_utc": _now_iso(),
                    "custom_field": custom,
                    "trade_id": trade_id,
                    "status": status,
                },
            )
            if status in TERMINAL_ORDER_STATUSES:
                self.state["inflight_orders"].pop(custom, None)
            self._persist_state()
        except Exception as exc:
            self.state["inflight_orders"][custom]["place_error"] = (
                f"{type(exc).__name__}: {exc}"
            )
            self.state["broker_order_failures"] = (
                int(self.state.get("broker_order_failures", 0)) + 1
            )
            self.broker_orders_enabled = False
            self.state["broker_orders_enabled"] = False
            self.state["blocked_reason"] = f"broker_place_failed:{custom}"
            self._persist_state()
            raise

    def _record_ideal_trade(
        self,
        *,
        strategy_id: str,
        instrument_type: str,
        product: str,
        code: str,
        delta_contracts: int,
        price_points: float,
        decision_ns: int,
        receive_ns: int,
        reason: str,
        series: str | None = None,
        strike: float | None = None,
        option_right: str | None = None,
        fee_twd: float | None = None,
        tax_twd: float | None = None,
        price_source: str | None = None,
        future_multiplier_twd_per_point: float | None = None,
        future_fee_per_side_twd: float | None = None,
        future_position_field: str = "futures_position",
        signal_decision_ns: int | None = None,
    ) -> None:
        if strategy_id not in self.strategy_ids:
            raise ValueError(f"unknown strategy id: {strategy_id}")
        quantity = abs(int(delta_contracts))
        multiplier = OPTION_MULTIPLIER
        if instrument_type != "option":
            multiplier = float(
                future_multiplier_twd_per_point
                if future_multiplier_twd_per_point is not None
                else self.hedge_multiplier
            )
            if future_position_field not in {
                "futures_position",
                "underlying_futures_position",
            }:
                raise ValueError(
                    f"unsupported futures position field: {future_position_field}"
                )
        if fee_twd is None:
            rate = (
                OPTION_FEE_PER_SIDE_TWD
                if instrument_type == "option"
                else float(
                    future_fee_per_side_twd
                    if future_fee_per_side_twd is not None
                    else self.hedge_fee_per_side_twd
                )
            )
            fee_twd = quantity * rate
        if tax_twd is None:
            if instrument_type == "option":
                tax_twd = quantity * option_premium_transaction_tax_twd(
                    price_points,
                    multiplier_twd_per_point=OPTION_MULTIPLIER,
                )
            else:
                tax_twd = quantity * taifex_tax_per_contract_twd(
                    price_points,
                    multiplier_twd_per_point=multiplier,
                    tax_rate=stock_index_futures_tax_rate(
                        datetime.fromtimestamp(decision_ns / 1e9, tz=TAIPEI).date()
                    ),
                )
        gross_cash = -int(delta_contracts) * float(price_points) * multiplier
        ledger = self.state["strategies"][strategy_id]
        ledger["gross_cash_twd"] = float(ledger["gross_cash_twd"]) + gross_cash
        ledger["fees_twd"] = float(ledger["fees_twd"]) + float(fee_twd)
        ledger["tax_twd"] = float(ledger["tax_twd"]) + float(tax_twd)
        ledger["trade_sides"] = int(ledger["trade_sides"]) + quantity
        if instrument_type == "option":
            ledger["option_gross_cash_twd"] = (
                float(ledger.get("option_gross_cash_twd") or 0.0) + gross_cash
            )
            ledger["option_fees_twd"] = float(
                ledger.get("option_fees_twd") or 0.0
            ) + float(fee_twd)
            ledger["option_tax_twd"] = float(
                ledger.get("option_tax_twd") or 0.0
            ) + float(tax_twd)
            positions = ledger.setdefault("option_positions", {})
            updated_position = int(positions.get(code, 0)) + int(delta_contracts)
            if updated_position:
                positions[code] = updated_position
                ledger.setdefault("option_position_metadata", {})[code] = {
                    "series": series,
                    "strike": strike,
                    "option_right": option_right,
                }
            else:
                positions.pop(code, None)
                ledger.setdefault("option_position_metadata", {}).pop(code, None)
        else:
            ledger[future_position_field] = int(
                ledger.get(future_position_field) or 0
            ) + int(delta_contracts)
        row = {
            "schema_version": SCHEMA_VERSION,
            "event": "ideal_executable_trade",
            "recorded_at_utc": _now_iso(),
            "strategy_id": strategy_id,
            "instrument_type": instrument_type,
            "product": product,
            "code": code,
            "series": series,
            "strike": strike,
            "option_right": option_right,
            "decision_ts_ns": int(decision_ns),
            "signal_decision_ts_ns": (
                int(signal_decision_ns) if signal_decision_ns is not None else None
            ),
            "book_receive_ts_ns": int(receive_ns),
            "book_age_ms": max(0.0, (decision_ns - receive_ns) / 1e6),
            "price_source": price_source
            or (
                "causally_received_best_ask"
                if delta_contracts > 0
                else "causally_received_best_bid"
            ),
            "price_points": float(price_points),
            "delta_contracts": int(delta_contracts),
            "multiplier_twd_per_point": multiplier,
            "gross_cash_flow_twd": gross_cash,
            "fixed_fee_twd": float(fee_twd),
            "transaction_tax_twd": float(tax_twd),
            "net_cash_flow_twd": gross_cash - float(fee_twd) - float(tax_twd),
            "reason": reason,
        }
        _append_jsonl(self.ledger_path, row)

    def _weekly_pairs_after(
        self, trading_date: date
    ) -> list[tuple[date, str, float, OptionInstrument, OptionInstrument]]:
        grouped: dict[tuple[date, str, float], dict[str, OptionInstrument]] = {}
        for item in self.options:
            if item.root == "TXO" or item.expiry <= trading_date:
                continue
            grouped.setdefault((item.expiry, item.series, item.strike), {})[
                item.right
            ] = item
        output = []
        for (expiry, series, strike), rights in grouped.items():
            if {"C", "P"} <= set(rights):
                output.append((expiry, series, strike, rights["C"], rights["P"]))
        output.sort(key=lambda item: (item[0], item[2], item[1]))
        return output

    def _underlying_book(
        self, decision_ns: int, *, max_age: float = ENTRY_BOOK_MAX_AGE_SECONDS
    ) -> tuple[float, float] | None:
        return _executable_book(
            self.latest_books.get(self.underlying.code),
            decision_ns=decision_ns,
            maximum_age_seconds=max_age,
            require_one_lot=True,
        )

    def _hedge_book(
        self,
        decision_ns: int,
        *,
        books: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> tuple[float, float] | None:
        source = self.latest_books if books is None else books
        return _executable_book(
            source.get(self.hedge.code),
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            require_one_lot=True,
        )

    def _put_call_parity_pairs(
        self,
    ) -> list[tuple[date, str, float, OptionInstrument, OptionInstrument]]:
        """Return worker-0 monthly pairs that settle with the current TX future."""

        expiry = self.underlying.last_trading_date
        if not isinstance(expiry, date):
            return []
        grouped: dict[tuple[str, float], dict[str, OptionInstrument]] = {}
        for item in self.options:
            if item.root != "TXO" or item.expiry != expiry:
                continue
            grouped.setdefault((item.series, item.strike), {})[item.right] = item
        output = []
        for (series, strike), rights in grouped.items():
            if {"C", "P"} <= set(rights):
                output.append((expiry, series, strike, rights["C"], rights["P"]))
        output.sort(key=lambda item: (item[0], item[2], item[1]))
        return output

    def _put_call_parity_candidate(
        self,
        *,
        decision_ns: int,
        direction: str,
        expiry: date,
        series: str,
        strike: float,
        call: OptionInstrument,
        put: OptionInstrument,
        require_receive_after_ns: int | None = None,
    ) -> PutCallParityCandidate | None:
        if direction == "sell_rich_synthetic_buy_tx":
            call_contracts = -PUT_CALL_PARITY_OPTION_QUANTITY
            put_contracts = PUT_CALL_PARITY_OPTION_QUANTITY
            future_contracts = PUT_CALL_PARITY_FUTURE_QUANTITY
        elif direction == "buy_cheap_synthetic_sell_tx":
            call_contracts = PUT_CALL_PARITY_OPTION_QUANTITY
            put_contracts = -PUT_CALL_PARITY_OPTION_QUANTITY
            future_contracts = -PUT_CALL_PARITY_FUTURE_QUANTITY
        else:
            raise ValueError(f"unsupported put-call parity direction: {direction}")

        call_row = self.latest_books.get(call.code)
        put_row = self.latest_books.get(put.code)
        future_row = self.latest_books.get(self.underlying.code)
        if call_row is None or put_row is None or future_row is None:
            return None
        rows = (call_row, put_row, future_row)
        if require_receive_after_ns is not None and any(
            int(row.get("receive_ts_ns") or 0) <= int(require_receive_after_ns)
            for row in rows
        ):
            return None
        call_fill = _depth_swept_price(
            call_row,
            delta_contracts=call_contracts,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
        )
        put_fill = _depth_swept_price(
            put_row,
            delta_contracts=put_contracts,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
        )
        future_fill = _depth_swept_price(
            future_row,
            delta_contracts=future_contracts,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
        )
        call_liquidation = _depth_swept_price(
            call_row,
            delta_contracts=-call_contracts,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
        )
        put_liquidation = _depth_swept_price(
            put_row,
            delta_contracts=-put_contracts,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
        )
        future_book = _executable_book(
            future_row,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            require_one_lot=True,
        )
        if (
            call_fill is None
            or put_fill is None
            or future_fill is None
            or call_liquidation is None
            or put_liquidation is None
            or future_book is None
        ):
            return None
        call_price, call_receive_ns = call_fill
        put_price, put_receive_ns = put_fill
        future_price, future_receive_ns = future_fill
        entry_cash = -(
            call_contracts * call_price + put_contracts * put_price
        ) * OPTION_MULTIPLIER - (
            future_contracts * future_price * self.underlying_multiplier
        )
        settlement_cash = future_contracts * strike * self.underlying_multiplier
        gross_edge = entry_cash + settlement_cash
        entry_fees = (
            abs(call_contracts) + abs(put_contracts)
        ) * OPTION_FEE_PER_SIDE_TWD + abs(
            future_contracts
        ) * self.underlying_fee_per_side_twd
        entry_tax = (
            abs(call_contracts)
            * option_premium_transaction_tax_twd(
                call_price,
                multiplier_twd_per_point=OPTION_MULTIPLIER,
            )
            + abs(put_contracts)
            * option_premium_transaction_tax_twd(
                put_price,
                multiplier_twd_per_point=OPTION_MULTIPLIER,
            )
            + abs(future_contracts)
            * taifex_tax_per_contract_twd(
                future_price,
                multiplier_twd_per_point=self.underlying_multiplier,
                tax_rate=stock_index_futures_tax_rate(
                    datetime.fromtimestamp(decision_ns / 1e9, tz=TAIPEI).date()
                ),
            )
        )
        settlement_proxy = sum(future_book) / 2.0
        settlement_tax = (
            PUT_CALL_PARITY_OPTION_QUANTITY
            * option_cash_settlement_transaction_tax_twd(
                settlement_proxy,
                settlement_date=expiry,
                multiplier_twd_per_point=OPTION_MULTIPLIER,
            )
            + PUT_CALL_PARITY_FUTURE_QUANTITY
            * taifex_tax_per_contract_twd(
                settlement_proxy,
                multiplier_twd_per_point=self.underlying_multiplier,
                tax_rate=stock_index_futures_tax_rate(expiry),
            )
        )
        net_edge = gross_edge - entry_fees - entry_tax - settlement_tax
        return PutCallParityCandidate(
            direction=direction,
            expiry=expiry,
            series=series,
            strike=strike,
            call=call,
            put=put,
            call_contracts=call_contracts,
            put_contracts=put_contracts,
            future_contracts=future_contracts,
            call_price=call_price,
            put_price=put_price,
            future_price=future_price,
            call_liquidation_price=call_liquidation[0],
            put_liquidation_price=put_liquidation[0],
            call_receive_ts_ns=call_receive_ns,
            put_receive_ts_ns=put_receive_ns,
            future_receive_ts_ns=future_receive_ns,
            gross_locked_edge_twd=float(gross_edge),
            entry_fixed_fees_twd=float(entry_fees),
            entry_transaction_tax_twd=float(entry_tax),
            estimated_settlement_tax_twd=float(settlement_tax),
            net_after_estimated_cost_twd=float(net_edge),
        )

    def _put_call_parity_payload(
        self,
        candidate: PutCallParityCandidate,
        *,
        decision_ns: int,
    ) -> dict[str, Any]:
        receive_times = (
            candidate.call_receive_ts_ns,
            candidate.put_receive_ts_ns,
            candidate.future_receive_ts_ns,
        )
        return {
            **candidate.public_payload(),
            "future_code": self.underlying.code,
            "observed_decision_ts_ns": int(decision_ns),
            "maximum_book_age_ms": max(
                0.0,
                (int(decision_ns) - min(receive_times)) / 1e6,
            ),
        }

    def _scan_put_call_parity(
        self,
        *,
        decision_ns: int,
        fixed_signal: Mapping[str, Any] | None = None,
    ) -> tuple[PutCallParityCandidate | None, int, int]:
        pairs = self._put_call_parity_pairs()
        candidates: list[PutCallParityCandidate] = []
        for expiry, series, strike, call, put in pairs:
            if fixed_signal is not None and (
                str(fixed_signal.get("expiry_date")) != expiry.isoformat()
                or str(fixed_signal.get("series")) != series
                or float(fixed_signal.get("strike") or math.nan) != strike
            ):
                continue
            directions = (
                (str(fixed_signal["direction"]),)
                if fixed_signal is not None
                else (
                    "sell_rich_synthetic_buy_tx",
                    "buy_cheap_synthetic_sell_tx",
                )
            )
            for direction in directions:
                candidate = self._put_call_parity_candidate(
                    decision_ns=decision_ns,
                    direction=direction,
                    expiry=expiry,
                    series=series,
                    strike=strike,
                    call=call,
                    put=put,
                    require_receive_after_ns=(
                        int(fixed_signal["signal_decision_ts_ns"])
                        if fixed_signal is not None
                        else None
                    ),
                )
                if candidate is not None:
                    candidates.append(candidate)
        best = max(
            candidates,
            key=lambda item: item.net_after_estimated_cost_twd,
            default=None,
        )
        return best, len(pairs), len(candidates)

    def _put_call_parity_capital_requirement_twd(
        self,
        candidate: PutCallParityCandidate,
        *,
        execution_decision_ns: int,
    ) -> float:
        option_requirement = 0.0
        for instrument, contracts, execution_price, liquidation_price in (
            (
                candidate.call,
                candidate.call_contracts,
                candidate.call_price,
                candidate.call_liquidation_price,
            ),
            (
                candidate.put,
                candidate.put_contracts,
                candidate.put_price,
                candidate.put_liquidation_price,
            ),
        ):
            fees = abs(contracts) * OPTION_FEE_PER_SIDE_TWD
            taxes = abs(contracts) * option_premium_transaction_tax_twd(
                execution_price,
                multiplier_twd_per_point=OPTION_MULTIPLIER,
            )
            if contracts > 0:
                option_requirement += (
                    contracts * execution_price * OPTION_MULTIPLIER + fees + taxes
                )
            else:
                option_requirement += (
                    abs(contracts)
                    * _txo_short_margin_twd(
                        option_price=liquidation_price,
                        underlying_price=candidate.future_price,
                        strike=instrument.strike,
                        option_right=instrument.right,
                        risk_margin_a_twd=self.option_risk_margin_a_twd,
                        risk_margin_b_twd=self.option_risk_margin_b_twd,
                    )
                    + abs(contracts)
                    * (liquidation_price - execution_price)
                    * OPTION_MULTIPLIER
                    + fees
                    + taxes
                )
        entry_date = datetime.fromtimestamp(
            execution_decision_ns / 1e9,
            tz=TAIPEI,
        ).date()
        future_tax = abs(candidate.future_contracts) * taifex_tax_per_contract_twd(
            candidate.future_price,
            multiplier_twd_per_point=self.underlying_multiplier,
            tax_rate=stock_index_futures_tax_rate(entry_date),
        )
        return float(
            option_requirement
            + abs(candidate.future_contracts)
            * taifex_initial_margin_twd(self.underlying_product, entry_date)
            + abs(candidate.future_contracts) * self.underlying_fee_per_side_twd
            + future_tax
            + candidate.estimated_settlement_tax_twd
        )

    def _enter_put_call_parity(
        self,
        candidate: PutCallParityCandidate,
        *,
        signal_decision_ns: int,
        execution_decision_ns: int,
    ) -> None:
        ledger = self.state["strategies"][PUT_CALL_PARITY_TX_STRATEGY_ID]
        parity_state = self.state["put_call_parity_tx"]
        if (
            ledger.get("option_positions")
            or int(ledger.get("underlying_futures_position") or 0) != 0
            or parity_state.get("open_position") is not None
        ):
            raise RuntimeError("put-call parity entry requires a flat strategy ledger")
        ledger["entry_capital_requirement_twd"] = (
            self._put_call_parity_capital_requirement_twd(
                candidate,
                execution_decision_ns=execution_decision_ns,
            )
        )
        if not self._prepare_strategy_capital_for_entry(
            PUT_CALL_PARITY_TX_STRATEGY_ID,
            decision_ns=execution_decision_ns,
        ):
            raise RuntimeError("put-call parity recapitalization is not yet eligible")
        for instrument, contracts, price, receive_ns in (
            (
                candidate.call,
                candidate.call_contracts,
                candidate.call_price,
                candidate.call_receive_ts_ns,
            ),
            (
                candidate.put,
                candidate.put_contracts,
                candidate.put_price,
                candidate.put_receive_ts_ns,
            ),
        ):
            self._record_ideal_trade(
                strategy_id=PUT_CALL_PARITY_TX_STRATEGY_ID,
                instrument_type="option",
                product="TXO",
                code=instrument.code,
                delta_contracts=contracts,
                price_points=price,
                decision_ns=execution_decision_ns,
                receive_ns=receive_ns,
                reason="put_call_parity_tx_positive_after_all_explicit_costs",
                series=instrument.series,
                strike=instrument.strike,
                option_right=instrument.right,
                price_source=(
                    "strictly_later_five_level_ask_vwap"
                    if contracts > 0
                    else "strictly_later_five_level_bid_vwap"
                ),
                signal_decision_ns=signal_decision_ns,
            )
        self._record_ideal_trade(
            strategy_id=PUT_CALL_PARITY_TX_STRATEGY_ID,
            instrument_type="future",
            product=self.underlying_product,
            code=self.underlying.code,
            delta_contracts=candidate.future_contracts,
            price_points=candidate.future_price,
            decision_ns=execution_decision_ns,
            receive_ns=candidate.future_receive_ts_ns,
            reason="put_call_parity_tx_positive_after_all_explicit_costs",
            price_source="strictly_later_five_level_depth_vwap",
            future_multiplier_twd_per_point=self.underlying_multiplier,
            future_fee_per_side_twd=self.underlying_fee_per_side_twd,
            future_position_field="underlying_futures_position",
            signal_decision_ns=signal_decision_ns,
        )

        ledger["entry_state"] = "entered"
        ledger["episode_count"] = int(ledger.get("episode_count") or 0) + 1
        public = self._put_call_parity_payload(
            candidate,
            decision_ns=execution_decision_ns,
        )
        open_position = {
            **public,
            "position_id": (
                f"{candidate.expiry.isoformat()}_{candidate.series}_"
                f"{candidate.strike:g}_{candidate.direction}"
            ),
            "signal_decision_ts_ns": int(signal_decision_ns),
            "entry_decision_ts_ns": int(execution_decision_ns),
            "locked_net_edge_after_estimated_cost_twd": (
                candidate.net_after_estimated_cost_twd
            ),
            "status": "locked_until_official_settlement",
        }
        parity_state["open_position"] = open_position
        parity_state["pending_signal"] = None
        parity_state["monitor"] = {
            **public,
            "state": "locked_until_official_settlement",
            "minimum_net_edge_twd": PUT_CALL_PARITY_MIN_NET_EDGE_TWD,
            "financing_interest_rate": 0.0,
            "broker_submission": False,
            "locked_net_edge_after_estimated_cost_twd": (
                candidate.net_after_estimated_cost_twd
            ),
        }
        _append_jsonl(
            self.events_path,
            {
                "event": "put_call_parity_tx_entered",
                "at_utc": _now_iso(),
                **open_position,
            },
        )
        self._persist_state()

    def _maybe_run_put_call_parity(
        self,
        now: datetime,
        decision_ns: int,
    ) -> None:
        ledger = self.state["strategies"][PUT_CALL_PARITY_TX_STRATEGY_ID]
        parity_state = self.state["put_call_parity_tx"]
        if parity_state.get("open_position") is not None:
            ledger["entry_state"] = "entered"
            return
        if not bool(ledger.get("alive", True)):
            if not self._strategy_recapitalization_is_due(
                PUT_CALL_PARITY_TX_STRATEGY_ID,
                decision_ns,
            ):
                ledger["entry_state"] = (
                    "awaiting_next_trading_date_recapitalization"
                )
                return
        expiry = self.underlying.last_trading_date
        if parity_state.get("blocked_expiry") and (
            not isinstance(expiry, date)
            or str(parity_state["blocked_expiry"]) == expiry.isoformat()
        ):
            ledger["entry_state"] = "forced_flat"
            parity_state["monitor"] = {
                **(parity_state.get("monitor") or {}),
                "state": "forced_flat_until_next_monthly_contract",
            }
            return
        if isinstance(expiry, date) and parity_state.get("blocked_expiry"):
            parity_state["blocked_expiry"] = None
            ledger["entry_state"] = "waiting_for_profitable_parity"
        if self.underlying_product != "TX":
            ledger["entry_state"] = "waiting_for_tx_underlying"
            parity_state["monitor"] = {
                "state": "unsupported_underlying_product",
                "underlying_product": self.underlying_product,
                "minimum_net_edge_twd": PUT_CALL_PARITY_MIN_NET_EDGE_TWD,
                "broker_submission": False,
            }
            return
        if not taifex_continuous_session_open(now):
            ledger["entry_state"] = "waiting_for_continuous_market"
            parity_state["monitor"] = {
                **(parity_state.get("monitor") or {}),
                "state": "waiting_for_continuous_market",
                "market_phase": taifex_market_phase(now),
            }
            return
        trading_date = taifex_trading_date(now)
        if not isinstance(expiry, date) or expiry < trading_date:
            ledger["entry_state"] = "waiting_for_same_expiry_monthly_books"
            parity_state["monitor"] = {
                **(parity_state.get("monitor") or {}),
                "state": "waiting_for_same_expiry_monthly_books",
            }
            return
        if (
            expiry == trading_date
            and taifex_session_kind(now) == "day"
            and now.time() >= datetime_time(13, 20)
        ):
            ledger["entry_state"] = "entry_closed_for_expiry_settlement"
            parity_state["monitor"] = {
                **(parity_state.get("monitor") or {}),
                "state": "entry_closed_for_expiry_settlement",
            }
            return

        pending = parity_state.get("pending_signal")
        if isinstance(pending, Mapping):
            signal_ns = int(pending.get("signal_decision_ts_ns") or 0)
            wait_seconds = max(0.0, (decision_ns - signal_ns) / 1e9)
            candidate, pair_count, evaluable_count = self._scan_put_call_parity(
                decision_ns=decision_ns,
                fixed_signal=pending,
            )
            if candidate is not None:
                if (
                    candidate.net_after_estimated_cost_twd
                    > PUT_CALL_PARITY_MIN_NET_EDGE_TWD
                ):
                    self._enter_put_call_parity(
                        candidate,
                        signal_decision_ns=signal_ns,
                        execution_decision_ns=decision_ns,
                    )
                    return
                parity_state["pending_signal"] = None
                ledger["entry_state"] = "waiting_for_profitable_parity"
                parity_state["monitor"] = {
                    **self._put_call_parity_payload(
                        candidate,
                        decision_ns=decision_ns,
                    ),
                    "state": "signal_cancelled_after_next_book_recheck",
                    "scanned_pair_count": pair_count,
                    "evaluable_direction_count": evaluable_count,
                }
                self._persist_state()
                return
            if wait_seconds <= PUT_CALL_PARITY_SIGNAL_MAX_WAIT_SECONDS:
                ledger["entry_state"] = "signal_pending_next_books"
                parity_state["monitor"] = {
                    **(parity_state.get("monitor") or {}),
                    "state": "signal_pending_next_books",
                    "signal_wait_seconds": wait_seconds,
                    "scanned_pair_count": pair_count,
                    "evaluable_direction_count": evaluable_count,
                }
                return
            parity_state["pending_signal"] = None

        candidate, pair_count, evaluable_count = self._scan_put_call_parity(
            decision_ns=decision_ns
        )
        if candidate is None:
            ledger["entry_state"] = "waiting_for_same_expiry_monthly_books"
            parity_state["monitor"] = {
                "state": "waiting_for_same_expiry_monthly_books",
                "expiry_date": expiry.isoformat(),
                "scanned_pair_count": pair_count,
                "evaluable_direction_count": evaluable_count,
                "minimum_net_edge_twd": PUT_CALL_PARITY_MIN_NET_EDGE_TWD,
                "financing_interest_rate": 0.0,
                "broker_submission": False,
            }
            return
        public = self._put_call_parity_payload(candidate, decision_ns=decision_ns)
        if candidate.net_after_estimated_cost_twd <= PUT_CALL_PARITY_MIN_NET_EDGE_TWD:
            ledger["entry_state"] = "waiting_for_profitable_parity"
            parity_state["monitor"] = {
                **public,
                "state": "no_positive_edge_after_cost",
                "scanned_pair_count": pair_count,
                "evaluable_direction_count": evaluable_count,
                "minimum_net_edge_twd": PUT_CALL_PARITY_MIN_NET_EDGE_TWD,
                "financing_interest_rate": 0.0,
                "broker_submission": False,
            }
            return
        parity_state["pending_signal"] = {
            "signal_decision_ts_ns": int(decision_ns),
            "direction": candidate.direction,
            "expiry_date": candidate.expiry.isoformat(),
            "series": candidate.series,
            "strike": candidate.strike,
        }
        ledger["entry_state"] = "signal_pending_next_books"
        parity_state["monitor"] = {
            **public,
            "state": "signal_pending_next_books",
            "scanned_pair_count": pair_count,
            "evaluable_direction_count": evaluable_count,
            "minimum_net_edge_twd": PUT_CALL_PARITY_MIN_NET_EDGE_TWD,
            "financing_interest_rate": 0.0,
            "broker_submission": False,
        }
        self._persist_state()

    def _intraday_session_state(self, now: datetime) -> dict[str, Any]:
        """Resolve causal day/night permissions for one observed timestamp."""

        session = taifex_session_kind(now)
        trading_date = taifex_trading_date(now)
        clock = now.time()
        entry_allowed = False
        calibration_allowed = False
        flatten_due = False
        if session == "day":
            entry_allowed = clock < self.intraday_entry_cutoff
            calibration_allowed = clock < self.intraday_flatten_time
            flatten_due = clock >= self.intraday_flatten_time
        elif session == "night":
            before_midnight = clock >= NIGHT_OPEN
            entry_allowed = before_midnight or clock < self.night_entry_cutoff
            calibration_allowed = before_midnight or clock < self.night_flatten_time
            flatten_due = not before_midnight and clock >= self.night_flatten_time
        return {
            "session": session,
            "trading_date": trading_date,
            "session_id": f"{trading_date.isoformat()}:{session}",
            "entry_allowed": entry_allowed,
            "calibration_allowed": calibration_allowed,
            "flatten_due": flatten_due,
        }

    def _strategy_entry_instruments(
        self,
        strategy_id: str,
        cycle: Mapping[str, Any],
    ) -> list[tuple[OptionInstrument, int]] | None:
        spec = STRATEGY_SPEC_BY_ID[strategy_id]
        if not spec.option_legs:
            return []
        expiry = date.fromisoformat(str(cycle["expiry_date"]))
        series = str(cycle["series"])
        strike = float(cycle["strike"])
        by_strike_right = {
            (item.strike, item.right): item
            for item in self.options
            if item.expiry == expiry and item.series == series
        }
        strikes = sorted({key[0] for key in by_strike_right})
        try:
            atm_index = strikes.index(strike)
        except ValueError:
            return None
        aggregated: dict[str, tuple[OptionInstrument, int]] = {}
        for right, offset, quantity in spec.option_legs:
            index = atm_index + int(offset)
            if not 0 <= index < len(strikes):
                return None
            instrument = by_strike_right.get((strikes[index], right))
            if instrument is None or int(quantity) == 0:
                return None
            previous = aggregated.get(instrument.code)
            aggregated[instrument.code] = (
                instrument,
                int(quantity) + (previous[1] if previous else 0),
            )
        return list(aggregated.values())

    def _enter_strategy_for_cycle(
        self,
        strategy_id: str,
        *,
        decision_ns: int,
    ) -> bool:
        if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID:
            return False
        cycle = self.state.get("active_cycle")
        if not isinstance(cycle, dict):
            return False
        ledger = self.state["strategies"][strategy_id]
        if not bool(ledger.get("alive", True)):
            if not self._strategy_recapitalization_is_due(strategy_id, decision_ns):
                ledger["entry_state"] = (
                    "awaiting_next_trading_date_recapitalization"
                )
                cycle.setdefault("strategy_entries", {})[strategy_id] = ledger[
                    "entry_state"
                ]
                return False
        if ledger.get("entry_state") in {"entered", "forced_flat"}:
            return True
        if ledger.get("entry_state") == "waiting_next_cycle":
            return False
        instruments = self._strategy_entry_instruments(strategy_id, cycle)
        if instruments is None:
            ledger["entry_state"] = "waiting_for_contract_ladder"
            cycle.setdefault("strategy_entries", {})[strategy_id] = ledger[
                "entry_state"
            ]
            return False
        fills: list[tuple[OptionInstrument, int, float, int]] = []
        for instrument, quantity in instruments:
            swept = _depth_swept_price(
                self.latest_books.get(instrument.code),
                delta_contracts=quantity,
                decision_ns=decision_ns,
                maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            )
            if swept is None:
                ledger["entry_state"] = "waiting_for_fresh_entry_depth"
                cycle.setdefault("strategy_entries", {})[strategy_id] = ledger[
                    "entry_state"
                ]
                return False
            fills.append((instrument, quantity, swept[0], swept[1]))
        underlying_entry = float(cycle.get("entry_forward_mid") or cycle["strike"])
        option_requirement = 0.0
        for instrument, quantity, price, _receive_ns in fills:
            per_leg_fee = abs(quantity) * OPTION_FEE_PER_SIDE_TWD
            per_leg_tax = abs(quantity) * option_premium_transaction_tax_twd(
                price,
                multiplier_twd_per_point=OPTION_MULTIPLIER,
            )
            if quantity > 0:
                option_requirement += (
                    quantity * price * OPTION_MULTIPLIER + per_leg_fee + per_leg_tax
                )
            else:
                liquidation = _depth_swept_price(
                    self.latest_books.get(instrument.code),
                    delta_contracts=abs(quantity),
                    decision_ns=decision_ns,
                    maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
                )
                liquidation_price = liquidation[0] if liquidation else price
                option_requirement += (
                    abs(quantity)
                    * _txo_short_margin_twd(
                        option_price=liquidation_price,
                        underlying_price=underlying_entry,
                        strike=instrument.strike,
                        option_right=instrument.right,
                        risk_margin_a_twd=self.option_risk_margin_a_twd,
                        risk_margin_b_twd=self.option_risk_margin_b_twd,
                    )
                    + abs(quantity) * (liquidation_price - price) * OPTION_MULTIPLIER
                    + per_leg_fee
                    + per_leg_tax
                )
        ledger["entry_capital_requirement_twd"] = float(option_requirement)
        if not self._prepare_strategy_capital_for_entry(
            strategy_id,
            decision_ns=decision_ns,
        ):
            ledger["entry_state"] = "awaiting_next_trading_date_recapitalization"
            cycle.setdefault("strategy_entries", {})[strategy_id] = ledger[
                "entry_state"
            ]
            return False
        for instrument, quantity, price, receive_ns in fills:
            self._record_ideal_trade(
                strategy_id=strategy_id,
                instrument_type="option",
                product="TXO",
                code=instrument.code,
                delta_contracts=quantity,
                price_points=price,
                decision_ns=decision_ns,
                receive_ns=receive_ns,
                reason="strategy_catalog_cycle_entry",
                series=instrument.series,
                strike=instrument.strike,
                option_right=instrument.right,
                price_source=(
                    "causally_received_five_level_ask_vwap"
                    if quantity > 0
                    else "causally_received_five_level_bid_vwap"
                ),
            )
        ledger["entry_state"] = "entered"
        ledger["episode_count"] = int(ledger.get("episode_count") or 0) + 1
        cycle.setdefault("strategy_entries", {})[strategy_id] = "entered"
        if strategy_id == CLASSIC_VARIANT_ID and not bool(
            cycle.get("broker_reference_opened")
        ):
            for instrument, quantity, _price, _receive_ns in fills:
                self._submit_broker_order(
                    code=instrument.code,
                    delta_contracts=quantity,
                    strategy_id="shared_option_position",
                    reason="shared_strategy_catalog_option_reference_entry",
                )
            cycle["broker_reference_opened"] = True
        _append_jsonl(
            self.events_path,
            {
                "event": "strategy_cycle_entered",
                "at_utc": _now_iso(),
                "cycle_id": cycle["cycle_id"],
                "strategy_id": strategy_id,
                "option_legs": len(fills),
                "broker_monitoring": STRATEGY_SPEC_BY_ID[strategy_id].broker_monitoring,
            },
        )
        self._persist_state()
        return True

    def _maybe_enter_cycle_strategies(self, decision_ns: int) -> None:
        if self.state.get("active_cycle") is None:
            return
        for strategy_id in self.strategy_ids:
            if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID:
                continue
            self._enter_strategy_for_cycle(
                strategy_id,
                decision_ns=decision_ns,
            )

    def _active_cycle_atm_pair(
        self,
        decision_ns: int,
    ) -> tuple[float, int, float, OptionInstrument, OptionInstrument] | None:
        """Resolve the current ATM pair from one causal underlying book."""

        cycle = self.state.get("active_cycle")
        if not isinstance(cycle, Mapping):
            return None
        underlying_row = self.latest_books.get(self.underlying.code)
        underlying_book = _executable_book(
            underlying_row,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            require_one_lot=True,
        )
        if underlying_book is None or underlying_row is None:
            return None
        expiry = date.fromisoformat(str(cycle["expiry_date"]))
        series = str(cycle["series"])
        grouped: dict[float, dict[str, OptionInstrument]] = {}
        for instrument in self.options:
            if instrument.expiry != expiry or instrument.series != series:
                continue
            grouped.setdefault(instrument.strike, {})[instrument.right] = instrument
        pairs = [
            (strike, rights["C"], rights["P"])
            for strike, rights in grouped.items()
            if {"C", "P"} <= set(rights)
        ]
        if not pairs:
            return None
        forward = sum(underlying_book) / 2.0
        strike, call, put = min(
            pairs,
            key=lambda pair: (abs(pair[0] - forward), pair[0]),
        )
        return (
            float(forward),
            int(underlying_row["receive_ts_ns"]),
            float(strike),
            call,
            put,
        )

    @staticmethod
    def _option_leg_matches_roll_policy(
        *,
        policy: str,
        option_right: str,
        strike: float,
        forward: float,
    ) -> bool:
        if policy == "itm_to_atm":
            return (option_right == "C" and strike < forward) or (
                option_right == "P" and strike > forward
            )
        if policy == "otm_to_atm":
            return (option_right == "C" and strike > forward) or (
                option_right == "P" and strike < forward
            )
        raise ValueError(f"unsupported option roll policy: {policy!r}")

    def _rolling_strategy_legs(
        self,
        strategy_id: str,
    ) -> dict[str, tuple[OptionInstrument, int]]:
        """Return the strategy's one Call and one Put or fail closed."""

        ledger = self.state["strategies"][strategy_id]
        positions = {
            str(code): int(quantity)
            for code, quantity in (ledger.get("option_positions") or {}).items()
            if int(quantity) != 0
        }
        expected = {
            right: int(quantity)
            for right, _offset, quantity in STRATEGY_SPEC_BY_ID[strategy_id].option_legs
        }
        by_right: dict[str, tuple[OptionInstrument, int]] = {}
        for code, quantity in positions.items():
            instrument = self.options_by_code.get(code)
            if instrument is None:
                raise RuntimeError(
                    f"rolling strategy holds unsubscribed option: {strategy_id}/{code}"
                )
            if instrument.right in by_right:
                raise RuntimeError(
                    f"rolling strategy has duplicate option right: "
                    f"{strategy_id}/{instrument.right}"
                )
            by_right[instrument.right] = (instrument, quantity)
        if set(by_right) != {"C", "P"} or any(
            by_right[right][1] != expected.get(right) for right in ("C", "P")
        ):
            raise RuntimeError(
                f"rolling strategy position invariant failed: {strategy_id}/{positions}"
            )
        return by_right

    def _execute_pending_option_roll(
        self,
        strategy_id: str,
        *,
        decision_ns: int,
    ) -> bool:
        ledger = self.state["strategies"][strategy_id]
        pending = ledger.get("pending_option_roll")
        if not isinstance(pending, Mapping):
            return False
        cycle = self.state.get("active_cycle")
        if not isinstance(cycle, Mapping) or str(pending.get("cycle_id")) != str(
            cycle.get("cycle_id")
        ):
            raise RuntimeError(
                f"pending option roll cycle identity mismatch: {strategy_id}"
            )
        signal_ns = int(pending["signal_decision_ts_ns"])
        if decision_ns <= signal_ns:
            return False
        current_legs = self._rolling_strategy_legs(strategy_id)
        prepared: list[
            tuple[
                OptionInstrument,
                OptionInstrument,
                int,
                float,
                int,
                float,
                int,
            ]
        ] = []
        for leg in pending.get("legs") or ():
            right = str(leg["option_right"])
            old_code = str(leg["old_code"])
            new_code = str(leg["new_code"])
            quantity = int(leg["quantity"])
            current_instrument, current_quantity = current_legs.get(right, (None, 0))
            if (
                current_instrument is None
                or current_instrument.code != old_code
                or current_quantity != quantity
            ):
                raise RuntimeError(
                    f"pending option roll position changed: {strategy_id}/{right}"
                )
            new_instrument = self.options_by_code.get(new_code)
            if new_instrument is None or new_instrument.right != right:
                raise RuntimeError(
                    f"pending option roll target unavailable: {strategy_id}/{new_code}"
                )
            close_sweep = _depth_swept_price(
                self.latest_books.get(old_code),
                delta_contracts=-quantity,
                decision_ns=decision_ns,
                maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            )
            open_sweep = _depth_swept_price(
                self.latest_books.get(new_code),
                delta_contracts=quantity,
                decision_ns=decision_ns,
                maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            )
            if (
                close_sweep is None
                or open_sweep is None
                or close_sweep[1] <= signal_ns
                or open_sweep[1] <= signal_ns
            ):
                return False
            prepared.append(
                (
                    current_instrument,
                    new_instrument,
                    quantity,
                    close_sweep[0],
                    close_sweep[1],
                    open_sweep[0],
                    open_sweep[1],
                )
            )
        if not prepared:
            raise RuntimeError(f"pending option roll has no legs: {strategy_id}")

        policy = str(pending["policy"])
        for (
            old_instrument,
            new_instrument,
            quantity,
            close_price,
            close_receive_ns,
            open_price,
            open_receive_ns,
        ) in prepared:
            self._record_ideal_trade(
                strategy_id=strategy_id,
                instrument_type="option",
                product="TXO",
                code=old_instrument.code,
                delta_contracts=-quantity,
                price_points=close_price,
                decision_ns=decision_ns,
                receive_ns=close_receive_ns,
                signal_decision_ns=signal_ns,
                reason=f"rolling_straddle_{policy}_close_old_leg",
                series=old_instrument.series,
                strike=old_instrument.strike,
                option_right=old_instrument.right,
                price_source=(
                    "causally_received_post_signal_five_level_ask_vwap"
                    if quantity < 0
                    else "causally_received_post_signal_five_level_bid_vwap"
                ),
            )
            self._record_ideal_trade(
                strategy_id=strategy_id,
                instrument_type="option",
                product="TXO",
                code=new_instrument.code,
                delta_contracts=quantity,
                price_points=open_price,
                decision_ns=decision_ns,
                receive_ns=open_receive_ns,
                signal_decision_ns=signal_ns,
                reason=f"rolling_straddle_{policy}_open_new_atm_leg",
                series=new_instrument.series,
                strike=new_instrument.strike,
                option_right=new_instrument.right,
                price_source=(
                    "causally_received_post_signal_five_level_ask_vwap"
                    if quantity > 0
                    else "causally_received_post_signal_five_level_bid_vwap"
                ),
            )
        ledger["pending_option_roll"] = None
        ledger["option_roll_count"] = int(ledger.get("option_roll_count") or 0) + len(
            prepared
        )
        ledger["last_option_roll_decision_ts_ns"] = int(decision_ns)
        ledger["last_option_roll_signal_ts_ns"] = signal_ns
        ledger["last_option_roll_forward_mid"] = float(pending["forward_mid"])
        ledger["last_option_roll_atm_strike"] = float(pending["atm_strike"])
        _append_jsonl(
            self.events_path,
            {
                "event": "rolling_straddle_legs_replaced",
                "at_utc": _now_iso(),
                "strategy_id": strategy_id,
                "cycle_id": cycle["cycle_id"],
                "policy": policy,
                "signal_decision_ts_ns": signal_ns,
                "execution_decision_ts_ns": int(decision_ns),
                "forward_mid": float(pending["forward_mid"]),
                "atm_strike": float(pending["atm_strike"]),
                "leg_count": len(prepared),
                "strictly_later_complete_books": True,
                "atomic_prevalidation": True,
            },
        )
        self._persist_state()
        return True

    def _maybe_roll_straddles(self, decision_ns: int) -> None:
        cycle = self.state.get("active_cycle")
        if not isinstance(cycle, Mapping):
            return

        signalled = False
        for strategy_id in ROLLING_STRADDLE_IDS:
            ledger = self.state["strategies"][strategy_id]
            if ledger.get("entry_state") != "entered" or not bool(
                ledger.get("alive", True)
            ):
                continue
            if isinstance(ledger.get("pending_option_roll"), Mapping):
                self._execute_pending_option_roll(
                    strategy_id,
                    decision_ns=decision_ns,
                )

        atm_pair = self._active_cycle_atm_pair(decision_ns)
        if atm_pair is None:
            return
        forward, underlying_receive_ns, atm_strike, call, put = atm_pair
        atm_by_right = {"C": call, "P": put}
        for strategy_id in ROLLING_STRADDLE_IDS:
            ledger = self.state["strategies"][strategy_id]
            if (
                ledger.get("entry_state") != "entered"
                or not bool(ledger.get("alive", True))
                or isinstance(ledger.get("pending_option_roll"), Mapping)
            ):
                continue
            policy = STRATEGY_SPEC_BY_ID[strategy_id].option_roll_policy
            legs = self._rolling_strategy_legs(strategy_id)
            replacements: list[dict[str, Any]] = []
            for right in ("C", "P"):
                old_instrument, quantity = legs[right]
                new_instrument = atm_by_right[right]
                if old_instrument.code == new_instrument.code:
                    continue
                if not self._option_leg_matches_roll_policy(
                    policy=policy,
                    option_right=right,
                    strike=old_instrument.strike,
                    forward=forward,
                ):
                    continue
                replacements.append(
                    {
                        "option_right": right,
                        "quantity": quantity,
                        "old_code": old_instrument.code,
                        "old_strike": old_instrument.strike,
                        "new_code": new_instrument.code,
                        "new_strike": new_instrument.strike,
                    }
                )
            if not replacements:
                continue
            ledger["pending_option_roll"] = {
                "state": "waiting_for_strictly_later_complete_books",
                "cycle_id": str(cycle["cycle_id"]),
                "policy": policy,
                "signal_decision_ts_ns": int(decision_ns),
                "underlying_book_receive_ts_ns": int(underlying_receive_ns),
                "forward_mid": float(forward),
                "atm_strike": float(atm_strike),
                "legs": replacements,
            }
            _append_jsonl(
                self.events_path,
                {
                    "event": "rolling_straddle_signal_created",
                    "at_utc": _now_iso(),
                    "strategy_id": strategy_id,
                    "cycle_id": cycle["cycle_id"],
                    "policy": policy,
                    "signal_decision_ts_ns": int(decision_ns),
                    "underlying_book_receive_ts_ns": int(underlying_receive_ns),
                    "forward_mid": float(forward),
                    "atm_strike": float(atm_strike),
                    "legs": replacements,
                },
            )
            signalled = True
        if signalled:
            self._persist_state()

    def _fixed_future_target(self, strategy_id: str) -> int | None:
        spec = STRATEGY_SPEC_BY_ID[strategy_id]
        if spec.hedge_policy == "fixed_future":
            return int(spec.hedge_parameter or 0.0)
        if spec.hedge_policy == "fixed_index_equivalent":
            return _round_nearest_contract(
                float(spec.hedge_parameter or 0.0)
                * OPTION_MULTIPLIER
                / self.hedge_multiplier
            )
        return None

    def _maybe_apply_fixed_future_targets(
        self,
        now: datetime,
        decision_ns: int,
    ) -> None:
        cycle = self.state.get("active_cycle")
        if not cycle:
            return
        session_state = self._intraday_session_state(now)
        if self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES:
            if not bool(session_state["calibration_allowed"]):
                return
        elif not (DAY_OPEN <= now.time() < datetime_time(9, 5)):
            return
        for strategy_id in DYNAMIC_HEDGE_STRATEGY_IDS:
            if strategy_id not in self.strategy_ids:
                continue
            target = self._fixed_future_target(strategy_id)
            if target is None:
                continue
            ledger = self.state["strategies"][strategy_id]
            if ledger.get("entry_state") != "entered":
                continue
            self._execute_future_target(
                strategy_id=strategy_id,
                target=target,
                decision_ns=decision_ns,
                reason="fixed_strategy_target_at_executable_bidask",
            )

    def _maybe_open_cycle(self, now: datetime, decision_ns: int) -> None:
        if self.state.get("active_cycle") is not None:
            return
        session_state = self._intraday_session_state(now)
        cycle_trade_date = now.date()
        if self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES:
            if not bool(session_state["entry_allowed"]):
                self.state["engine_status"] = "waiting_for_intraday_entry_window"
                return
            cycle_trade_date = session_state["trading_date"]
        else:
            bootstrap_after = date.fromisoformat(
                str(self.state["bootstrap_after_date"])
            )
            if now.date() <= bootstrap_after:
                self.state["engine_status"] = "waiting_for_bootstrap"
                return
            if not (DAY_OPEN <= now.time() < datetime_time(9, 5)):
                return
        underlying_book = self._underlying_book(decision_ns)
        if underlying_book is None:
            self.state["engine_status"] = "waiting_for_fresh_open_books"
            return
        forward = sum(underlying_book) / 2.0
        pairs = self._weekly_pairs_after(cycle_trade_date)
        if not pairs:
            self.state["engine_status"] = "waiting_for_listed_weekly_pair"
            return
        nearest_expiry = pairs[0][0]
        expiry_pairs = [item for item in pairs if item[0] == nearest_expiry]
        expiry, series, strike, call, put = min(
            expiry_pairs, key=lambda item: (abs(item[2] - forward), item[2])
        )
        call_row = self.latest_books.get(call.code)
        put_row = self.latest_books.get(put.code)
        call_book = _executable_book(
            call_row,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            require_one_lot=True,
        )
        put_book = _executable_book(
            put_row,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            require_one_lot=True,
        )
        if call_book is None or put_book is None or call_row is None or put_row is None:
            self.state["engine_status"] = "waiting_for_fresh_atm_call_put_books"
            return
        if self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES:
            if session_state["session"] == "night":
                entry_timing = (
                    "night_session_open"
                    if NIGHT_OPEN <= now.time() < datetime_time(15, 5)
                    else "night_late_start"
                )
            else:
                entry_timing = (
                    "intraday_late_start"
                    if now.time() >= datetime_time(9, 5)
                    else "session_open"
                )
        else:
            entry_timing = "session_open"
        cycle_id = f"{cycle_trade_date.isoformat()}_{series}_{strike:g}"
        self.state["active_cycle"] = {
            "cycle_id": cycle_id,
            "entry_date": cycle_trade_date.isoformat(),
            "entry_session": session_state["session"],
            "expiry_date": expiry.isoformat(),
            "series": series,
            "strike": strike,
            "call_code": call.code,
            "put_code": put.code,
            "call_entry_ask": call_book[1],
            "put_entry_ask": put_book[1],
            "entry_forward_mid": forward,
            "entry_timing": entry_timing,
            "strategy_mode": self.strategy_mode,
            "status": "open",
            "strategy_entries": {
                strategy_id: (
                    "independent_monthly_lifecycle"
                    if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID
                    else "pending"
                )
                for strategy_id in self.strategy_ids
            },
            "broker_reference_opened": False,
        }
        for strategy_id in self.strategy_ids:
            if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID:
                continue
            ledger = self.state["strategies"][strategy_id]
            ledger["entry_state"] = (
                "pending"
                if bool(ledger.get("alive", True))
                else "awaiting_next_trading_date_recapitalization"
            )
            ledger["option_positions"] = {}
            ledger["option_position_metadata"] = {}
            ledger["pending_option_roll"] = None
        self.state["pending_targets"] = {}
        self.state["engine_status"] = (
            "intraday_cycle_open"
            if self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES
            else "cycle_open"
        )
        self._persist_state()
        self._maybe_enter_cycle_strategies(decision_ns)
        _append_jsonl(
            self.events_path,
            {
                "event": "cycle_opened",
                "at_utc": _now_iso(),
                **self.state["active_cycle"],
            },
        )

    def _execute_future_target(
        self,
        *,
        strategy_id: str,
        target: int,
        decision_ns: int,
        reason: str,
        execution_snapshot: (
            tuple[tuple[float, float], Mapping[str, Any]] | None
        ) = None,
    ) -> bool:
        ledger = self.state["strategies"][strategy_id]
        current = int(ledger["futures_position"])
        if not bool(ledger.get("alive", True)) and int(target) != 0:
            return False
        change = int(target) - current
        if change == 0:
            return True
        if execution_snapshot is None:
            row = self.latest_books.get(self.hedge.code)
        else:
            _book, row = execution_snapshot
        swept = _depth_swept_price(
            row,
            delta_contracts=change,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
        )
        if swept is None or row is None:
            return False
        price, receive_ns = swept
        self._record_ideal_trade(
            strategy_id=strategy_id,
            instrument_type="future",
            product=self.hedge_product,
            code=self.hedge.code,
            delta_contracts=change,
            price_points=price,
            decision_ns=decision_ns,
            receive_ns=receive_ns,
            reason=reason,
            price_source="causally_received_five_level_depth_vwap",
        )
        self._persist_state()
        if STRATEGY_SPEC_BY_ID[strategy_id].broker_monitoring.startswith("mirrored"):
            self._submit_broker_order(
                code=self.hedge.code,
                delta_contracts=change,
                strategy_id=strategy_id,
                reason=reason,
            )
        return True

    def _execute_underlying_future_target(
        self,
        *,
        strategy_id: str,
        target: int,
        decision_ns: int,
        reason: str,
    ) -> bool:
        ledger = self.state["strategies"][strategy_id]
        current = int(ledger.get("underlying_futures_position") or 0)
        change = int(target) - current
        if change == 0:
            return True
        swept = _depth_swept_price(
            self.latest_books.get(self.underlying.code),
            delta_contracts=change,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
        )
        if swept is None:
            return False
        self._record_ideal_trade(
            strategy_id=strategy_id,
            instrument_type="future",
            product=self.underlying_product,
            code=self.underlying.code,
            delta_contracts=change,
            price_points=swept[0],
            decision_ns=decision_ns,
            receive_ns=swept[1],
            reason=reason,
            price_source="causally_received_five_level_depth_vwap",
            future_multiplier_twd_per_point=self.underlying_multiplier,
            future_fee_per_side_twd=self.underlying_fee_per_side_twd,
            future_position_field="underlying_futures_position",
        )
        self._persist_state()
        return True

    def _force_liquidate_strategy(
        self,
        strategy_id: str,
        *,
        decision_ns: int,
        reason: str,
    ) -> bool:
        ledger = self.state["strategies"][strategy_id]
        option_positions = dict(ledger.get("option_positions") or {})
        metadata = ledger.get("option_position_metadata") or {}
        prepared: list[tuple[str, int, float, int, Mapping[str, Any]]] = []
        for code, raw_position in option_positions.items():
            position = int(raw_position)
            if position == 0:
                continue
            swept = _depth_swept_price(
                self.latest_books.get(code),
                delta_contracts=-position,
                decision_ns=decision_ns,
                maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            )
            if swept is None:
                ledger["forced_liquidation_pending"] = True
                return False
            prepared.append(
                (code, position, swept[0], swept[1], metadata.get(code) or {})
            )
        future_position = int(ledger.get("futures_position") or 0)
        if (
            future_position
            and _depth_swept_price(
                self.latest_books.get(self.hedge.code),
                delta_contracts=-future_position,
                decision_ns=decision_ns,
                maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            )
            is None
        ):
            ledger["forced_liquidation_pending"] = True
            return False
        underlying_future_position = int(ledger.get("underlying_futures_position") or 0)
        if (
            underlying_future_position
            and _depth_swept_price(
                self.latest_books.get(self.underlying.code),
                delta_contracts=-underlying_future_position,
                decision_ns=decision_ns,
                maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            )
            is None
        ):
            ledger["forced_liquidation_pending"] = True
            return False
        for code, position, price, receive_ns, leg in prepared:
            self._record_ideal_trade(
                strategy_id=strategy_id,
                instrument_type="option",
                product="TXO",
                code=code,
                delta_contracts=-position,
                price_points=price,
                decision_ns=decision_ns,
                receive_ns=receive_ns,
                reason=reason,
                series=str(leg.get("series") or ""),
                strike=float(leg.get("strike") or 0.0),
                option_right=str(leg.get("option_right") or ""),
                price_source="forced_liquidation_five_level_depth_vwap",
            )
        if future_position and not self._execute_future_target(
            strategy_id=strategy_id,
            target=0,
            decision_ns=decision_ns,
            reason=reason,
        ):
            ledger["forced_liquidation_pending"] = True
            return False
        if underlying_future_position and not self._execute_underlying_future_target(
            strategy_id=strategy_id,
            target=0,
            decision_ns=decision_ns,
            reason=reason,
        ):
            ledger["forced_liquidation_pending"] = True
            return False
        ledger["forced_liquidation_pending"] = False
        ledger["pending_option_roll"] = None
        ledger["entry_state"] = "forced_flat"
        if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID:
            parity_state = self.state["put_call_parity_tx"]
            open_position = parity_state.get("open_position") or {}
            parity_state["blocked_expiry"] = open_position.get("expiry_date")
            parity_state["open_position"] = None
            parity_state["pending_signal"] = None
            parity_state["monitor"] = {
                **(parity_state.get("monitor") or {}),
                "state": "forced_flat_until_next_monthly_contract",
                "forced_flat_reason": reason,
            }
        self.state.get("pending_targets", {}).pop(strategy_id, None)
        closed_mark = self._strategy_mark(strategy_id, decision_ns)
        total_equity = closed_mark.get("total_equity_twd")
        if total_equity is None or float(total_equity) <= 0.0:
            ledger["alive"] = False
            bankruptcy_trading_date = taifex_trading_date(
                datetime.fromtimestamp(decision_ns / 1e9, tz=TAIPEI)
            )
            ledger["entry_state"] = (
                "awaiting_next_trading_date_recapitalization"
            )
            ledger["bankruptcy_count"] = int(
                ledger.get("bankruptcy_count") or 0
            ) + 1
            ledger["last_bankruptcy_decision_ts_ns"] = int(decision_ns)
            ledger["last_bankruptcy_trading_date"] = (
                bankruptcy_trading_date.isoformat()
            )
            ledger["last_bankruptcy_total_equity_twd"] = total_equity
            if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID:
                parity_state = self.state["put_call_parity_tx"]
                parity_state["blocked_expiry"] = None
                parity_state["monitor"] = {
                    **(parity_state.get("monitor") or {}),
                    "state": "awaiting_next_trading_date_recapitalization",
                }
        _append_jsonl(
            self.events_path,
            {
                "event": "strategy_forced_liquidation",
                "at_utc": _now_iso(),
                "strategy_id": strategy_id,
                "reason": reason,
                "decision_ts_ns": int(decision_ns),
                "bankruptcy_trading_date": ledger.get(
                    "last_bankruptcy_trading_date"
                ),
                "total_equity_twd": total_equity,
                "alive_after": bool(ledger.get("alive", True)),
                "recapitalization_policy": STRATEGY_RECAPITALIZATION_POLICY,
            },
        )
        self._persist_state()
        return True

    def _maybe_enforce_strategy_margin(self, decision_ns: int) -> None:
        for strategy_id in self.strategy_ids:
            ledger = self.state["strategies"][strategy_id]
            if not bool(ledger.get("alive", True)):
                continue
            if ledger.get("entry_state") != "entered":
                continue
            mark = self._strategy_mark(strategy_id, decision_ns)
            if not bool(mark.get("valuation_available")):
                continue
            total_equity = float(mark.get("total_equity_twd") or 0.0)
            margin_excess = float(mark.get("margin_excess_twd") or 0.0)
            has_short_option = any(
                int(quantity) < 0
                for quantity in (ledger.get("option_positions") or {}).values()
            )
            if total_equity > 0.0 and (not has_short_option or margin_excess >= 0.0):
                continue
            ledger["margin_call_count"] = int(ledger.get("margin_call_count") or 0) + 1
            self._force_liquidate_strategy(
                strategy_id,
                decision_ns=decision_ns,
                reason=(
                    "bankruptcy_recapitalizing_forced_flatten"
                    if total_equity <= 0.0
                    else "maintenance_margin_forced_flatten"
                ),
            )

    def _maybe_execute_pending_targets(self, now: datetime, decision_ns: int) -> None:
        if self.strategy_mode != STRATEGY_MODE_DAILY:
            return
        if not (DAY_OPEN <= now.time() < datetime_time(9, 5)):
            return
        cycle = self.state.get("active_cycle")
        if not cycle:
            return
        pending = dict(self.state.get("pending_targets") or {})
        changed = False
        for strategy_id, signal in pending.items():
            if str(signal.get("cycle_id")) != str(cycle["cycle_id"]):
                raise RuntimeError("pending target cycle identity mismatch")
            if date.fromisoformat(str(signal["decision_date"])) >= now.date():
                continue
            if self._execute_future_target(
                strategy_id=strategy_id,
                target=int(signal["target_contracts"]),
                decision_ns=decision_ns,
                reason="prior_close_model_delta_target_at_next_open",
            ):
                if bool(signal.get("hedge_trigger_due", True)):
                    ledger = self.state["strategies"][strategy_id]
                    ledger["last_dynamic_hedge_cycle_id"] = str(
                        signal["cycle_id"]
                    )
                    ledger["last_dynamic_hedge_session_id"] = signal.get(
                        "decision_session_id"
                    )
                    ledger["last_dynamic_hedge_decision_ts_ns"] = int(
                        signal["decision_ts_ns"]
                    )
                    ledger["last_dynamic_hedge_forward_mid"] = float(
                        signal["surface_forward_mid"]
                    )
                self.state["pending_targets"].pop(strategy_id, None)
                changed = True
        if changed:
            self._persist_state()

    def _surface_quotes(
        self,
        books: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> list[BidAskSurfaceQuote]:
        source = self.latest_books if books is None else books
        output: list[BidAskSurfaceQuote] = []
        for item in self.options:
            row = source.get(item.code)
            if row is None:
                continue
            bid = _book_price(row, "bid")
            ask = _book_price(row, "ask")
            if bid is None or ask is None:
                continue
            output.append(
                BidAskSurfaceQuote(
                    series=item.series,
                    expiry=item.expiry,
                    strike=item.strike,
                    option_right=item.right,
                    bid_price=bid,
                    ask_price=ask,
                    receive_ts_ns=int(row["receive_ts_ns"]),
                )
            )
        return output

    def _maybe_calibrate(self, now: datetime, decision_ns: int) -> None:
        cycle = self.state.get("active_cycle")
        if not cycle or str(cycle.get("status")) != "open":
            return
        expiry = date.fromisoformat(str(cycle["expiry_date"]))
        intraday = self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES
        session_state = self._intraday_session_state(now)
        decision_trade_date = session_state["trading_date"] if intraday else now.date()
        if expiry < decision_trade_date or (not intraday and expiry <= now.date()):
            return
        decision_bucket = None
        decision_books = dict(self.latest_books)
        hedge_row = decision_books.get(self.hedge.code)
        hedge_book = self._hedge_book(decision_ns, books=decision_books)
        if intraday:
            if not bool(session_state["calibration_allowed"]):
                return
            decision_bucket = decision_ns // (
                self.intraday_decision_interval_seconds * 1_000_000_000
            )
            if int(self.state.get("last_intraday_decision_bucket") or -1) == int(
                decision_bucket
            ):
                return
            if hedge_book is None or hedge_row is None:
                self.state["engine_status"] = "waiting_for_fresh_intraday_hedge_book"
                return
        elif self.state.get("last_calibration_date") == now.date().isoformat() or not (
            self.calibration_time <= now.time() < datetime_time(13, 30)
        ):
            return
        forward_row = decision_books.get(self.underlying.code)
        forward_book = _executable_book(
            forward_row,
            decision_ns=decision_ns,
            maximum_age_seconds=SURFACE_BOOK_MAX_AGE_SECONDS,
            require_one_lot=True,
        )
        if forward_book is None or forward_row is None:
            self.state["engine_status"] = "waiting_for_calibration_forward_book"
            return
        try:
            surface = build_bidask_iv_surface(
                self._surface_quotes(decision_books),
                calibration_decision_ns=decision_ns,
                forward_bid=forward_book[0],
                forward_ask=forward_book[1],
                forward_receive_ts_ns=int(forward_row["receive_ts_ns"]),
                maximum_staleness_seconds=SURFACE_BOOK_MAX_AGE_SECONDS,
            )
        except ValueError as exc:
            error = str(exc)
            if not error.startswith("live Bid/Ask IV surface is too sparse:"):
                raise
            self.state["engine_status"] = "waiting_for_sufficient_iv_surface"
            self.state["last_iv_surface_error"] = error
            return
        self.state["last_iv_surface_error"] = None
        expiry_ns = int(
            datetime.combine(expiry, datetime_time(13, 30), tzinfo=TAIPEI).timestamp()
            * 1e9
        )
        years = (expiry_ns - decision_ns) / 1e9 / SECONDS_PER_YEAR
        if years <= 0.0:
            return
        all_targets_executed = True
        fitted_models: dict[str, Any] = {}
        for strategy_id in DYNAMIC_HEDGE_STRATEGY_IDS:
            if strategy_id not in self.strategy_ids:
                continue
            spec = STRATEGY_SPEC_BY_ID[strategy_id]
            if spec.hedge_policy in {"fixed_future", "fixed_index_equivalent"}:
                continue
            if self.state["strategies"][strategy_id].get("entry_state") != "entered":
                continue
            model_id = (
                spec.hedge_policy.removeprefix("vol_model:")
                if spec.hedge_policy.startswith("vol_model:")
                else MODEL_BLACK_SCHOLES
            )
            fitted = fitted_models.get(model_id)
            if fitted is None:
                try:
                    fitted = fit_volatility_model(
                        surface,
                        model_id=model_id,
                        held_series=str(cycle["series"]),
                    )
                except ValueError as exc:
                    error = str(exc)
                    if not error.startswith("held-series IV surface is too sparse:"):
                        raise
                    self.state["engine_status"] = (
                        "waiting_for_sufficient_held_series_iv_surface"
                    )
                    self.state["last_iv_surface_error"] = error
                    return
                fitted_models[model_id] = fitted
            delta = fitted.straddle_delta(
                forward=surface.forward,
                strike=float(cycle["strike"]),
                years_to_expiry=years,
            )
            raw_target = -delta * OPTION_MULTIPLIER / self.hedge_multiplier
            if spec.hedge_policy == "bs_delta_scale":
                raw_target *= float(spec.hedge_parameter or 0.0)
            target = _round_nearest_contract(raw_target)
            ledger = self.state["strategies"][strategy_id]
            current = int(ledger["futures_position"])
            hedge_trigger_due = True
            hedge_trigger_kind = "every_decision"
            hedge_trigger_distance: float | None = None
            previous_hedge_forward = ledger.get("last_dynamic_hedge_forward_mid")
            previous_hedge_decision_ns = ledger.get(
                "last_dynamic_hedge_decision_ts_ns"
            )
            same_cycle = str(ledger.get("last_dynamic_hedge_cycle_id") or "") == str(
                cycle["cycle_id"]
            )
            same_session = str(
                ledger.get("last_dynamic_hedge_session_id") or ""
            ) == str(session_state["session_id"])
            if spec.hedge_policy == "bs_gamma_price_grid":
                hedge_trigger_kind = "forward_point_grid"
                threshold_points = float(spec.hedge_parameter or 0.0)
                if same_cycle and previous_hedge_forward is not None:
                    hedge_trigger_distance = abs(
                        float(surface.forward) - float(previous_hedge_forward)
                    )
                    hedge_trigger_due = hedge_trigger_distance >= threshold_points
                    if intraday and not same_session:
                        hedge_trigger_due = True
                if not hedge_trigger_due:
                    target = current
            elif spec.hedge_policy == "bs_gamma_time_grid":
                hedge_trigger_kind = "elapsed_minute_grid"
                threshold_seconds = float(spec.hedge_parameter or 0.0) * 60.0
                if same_cycle and previous_hedge_decision_ns is not None:
                    hedge_trigger_distance = max(
                        0.0,
                        (decision_ns - int(previous_hedge_decision_ns)) / 1e9,
                    )
                    hedge_trigger_due = hedge_trigger_distance >= threshold_seconds
                    if intraday and not same_session:
                        hedge_trigger_due = True
                if not hedge_trigger_due:
                    target = current
            if spec.hedge_policy == "bs_delta_band":
                net_delta = delta + current * self.hedge_multiplier / OPTION_MULTIPLIER
                if abs(net_delta) <= float(spec.hedge_parameter or 0.0):
                    target = current
            signal = {
                "cycle_id": cycle["cycle_id"],
                "decision_date": decision_trade_date.isoformat(),
                "decision_session_id": session_state["session_id"],
                "decision_ts_ns": decision_ns,
                "target_contracts": target,
                "straddle_delta": delta,
                "surface_forward_mid": float(surface.forward),
                "hedge_trigger_due": hedge_trigger_due,
                "hedge_trigger_kind": hedge_trigger_kind,
                "hedge_trigger_distance": hedge_trigger_distance,
            }
            if intraday:
                executed = self._execute_future_target(
                    strategy_id=strategy_id,
                    target=target,
                    decision_ns=decision_ns,
                    reason="intraday_model_delta_target_at_executable_bidask",
                    execution_snapshot=(hedge_book, hedge_row),
                )
                all_targets_executed = all_targets_executed and executed
                if executed and hedge_trigger_due:
                    ledger["last_dynamic_hedge_cycle_id"] = str(cycle["cycle_id"])
                    ledger["last_dynamic_hedge_session_id"] = str(
                        session_state["session_id"]
                    )
                    ledger["last_dynamic_hedge_decision_ts_ns"] = int(decision_ns)
                    ledger["last_dynamic_hedge_forward_mid"] = float(
                        surface.forward
                    )
            else:
                self.state["pending_targets"][strategy_id] = signal
            _append_jsonl(
                self.calibrations_path,
                {
                    "schema_version": SCHEMA_VERSION,
                    "recorded_at_utc": _now_iso(),
                    "trading_date": decision_trade_date.isoformat(),
                    "session": session_state["session"] if intraday else "day",
                    "decision_ts_ns": decision_ns,
                    "strategy_mode": self.strategy_mode,
                    "decision_interval_seconds": (
                        self.intraday_decision_interval_seconds if intraday else None
                    ),
                    "cycle_id": cycle["cycle_id"],
                    "strategy_id": strategy_id,
                    "series": cycle["series"],
                    "strike": cycle["strike"],
                    "model_id": model_id,
                    "model_label": VOLATILITY_MODEL_LABELS[model_id],
                    "implementation_level": spec.implementation_level,
                    "hedge_policy": spec.hedge_policy,
                    "hedge_parameter": spec.hedge_parameter,
                    "surface_forward_mid": surface.forward,
                    "surface_points": len(surface.points),
                    "surface_maturities": surface.maturity_count,
                    "target_contracts": target,
                    "straddle_delta": delta,
                    "hedge_trigger_due": hedge_trigger_due,
                    "hedge_trigger_kind": hedge_trigger_kind,
                    "hedge_trigger_distance": hedge_trigger_distance,
                    "previous_hedge_decision_ts_ns": previous_hedge_decision_ns,
                    "previous_hedge_forward_mid": previous_hedge_forward,
                    "execution_timing": (
                        "same_decision_executable_bidask"
                        if intraday
                        else "next_session_open"
                    ),
                    "target_executed": executed if intraday else False,
                    **fitted.diagnostics(),
                },
            )
        self.state["last_calibration_date"] = decision_trade_date.isoformat()
        if intraday:
            if all_targets_executed:
                self.state["last_intraday_decision_bucket"] = int(decision_bucket)
                self.state["last_intraday_decision_session"] = session_state[
                    "session_id"
                ]
                self.state["pending_targets"] = {}
                self.state["engine_status"] = "intraday_active"
            else:
                self.state["engine_status"] = "waiting_for_fresh_intraday_hedge_book"
        else:
            self.state["engine_status"] = "calibrated_waiting_next_open"
        self._persist_state()

    def _maybe_flatten_intraday_futures(self, now: datetime, decision_ns: int) -> None:
        session_state = self._intraday_session_state(now)
        if (
            self.strategy_mode != STRATEGY_MODE_INTRADAY_FUTURES
            or not bool(session_state["flatten_due"])
            or self.state.get("last_intraday_flatten_session")
            == session_state["session_id"]
        ):
            return
        session = str(session_state["session"])
        all_flat = True
        for strategy_id in self.strategy_ids:
            if not self._execute_future_target(
                strategy_id=strategy_id,
                target=0,
                decision_ns=decision_ns,
                reason=f"intraday_futures_flatten_before_{session}_session_close",
            ):
                all_flat = False
        if all_flat:
            self.state["last_intraday_flatten_date"] = session_state[
                "trading_date"
            ].isoformat()
            self.state["last_intraday_flatten_session"] = session_state["session_id"]
            self.state["pending_targets"] = {}
            self.state["engine_status"] = f"intraday_flat_for_{session}_close"
            self._persist_state()

    def _maybe_flatten_expiry_hedges(self, now: datetime, decision_ns: int) -> None:
        cycle = self.state.get("active_cycle")
        if not cycle:
            return
        if taifex_session_kind(now) != "day":
            return
        option_expiry_today = (
            date.fromisoformat(str(cycle["expiry_date"])) == now.date()
        )
        hedge_expiry_today = self.hedge.last_trading_date == now.date()
        if not option_expiry_today and not hedge_expiry_today:
            return
        if not (datetime_time(13, 24) <= now.time() < datetime_time(13, 30)):
            return
        all_flat = True
        for strategy_id in self.strategy_ids:
            if not self._execute_future_target(
                strategy_id=strategy_id,
                target=0,
                decision_ns=decision_ns,
                reason="expiry_day_mtx_flatten_before_option_settlement",
            ):
                all_flat = False
        if all_flat and option_expiry_today:
            cycle["status"] = "waiting_official_final_settlement"
            self.state["pending_targets"] = {}
            self.state["engine_status"] = "waiting_official_final_settlement"
            self._persist_state()
        elif all_flat and hedge_expiry_today:
            self.state["engine_status"] = "hedge_contract_flat_for_monthly_roll"
            self._persist_state()

    def _official_settlement(self, expiry: date) -> tuple[float, str, str] | None:
        if not self.final_settlement_path.is_file():
            return None
        frame = pl.read_parquet(self.final_settlement_path).filter(
            pl.col("settlement_date") == expiry
        )
        if frame.height != 1:
            return None
        row = frame.row(0, named=True)
        return (
            float(row["final_settlement_price"]),
            str(row.get("source_file", "")),
            str(row.get("source_sha256", "")),
        )

    def _settle_expired_positions_for_subscription_bootstrap(
        self,
        now: datetime,
    ) -> None:
        """Cash-settle expired state before current-contract subscription checks."""

        if now.tzinfo is None:
            raise ValueError("subscription bootstrap time must be timezone-aware")
        observed_now = now.astimezone(TAIPEI)
        decision_ns = time.time_ns()
        held_before = held_option_codes(self.state)
        try:
            self._maybe_settle_expired_put_call_parity(observed_now, decision_ns)
            self._maybe_settle_expired_cycle(observed_now, decision_ns)
        except Exception as exc:
            self.state["engine_status"] = "blocked_subscription_bootstrap_settlement"
            self.state["blocked_reason"] = (
                f"subscription_bootstrap_settlement_failed:{type(exc).__name__}:{exc}"
            )
            self._persist_state()
            raise

        trading_date = taifex_trading_date(observed_now)
        pending_expiries: list[str] = []
        cycle = self.state.get("active_cycle")
        if isinstance(cycle, Mapping):
            expiry = date.fromisoformat(str(cycle["expiry_date"]))
            if expiry < trading_date:
                pending_expiries.append(f"weekly_cycle:{expiry.isoformat()}")
        parity_state = self.state.get("put_call_parity_tx") or {}
        parity_position = parity_state.get("open_position")
        if isinstance(parity_position, Mapping):
            expiry = date.fromisoformat(str(parity_position["expiry_date"]))
            if expiry < trading_date:
                pending_expiries.append(f"put_call_parity_tx:{expiry.isoformat()}")
        if pending_expiries:
            reason = str(self.state.get("blocked_reason") or "")
            if not reason:
                reason = "expired_positions_remain:" + ",".join(pending_expiries)
            self.state["engine_status"] = "blocked_subscription_bootstrap_settlement"
            self.state["blocked_reason"] = reason
            self._persist_state()
            raise RuntimeError(reason)

        held_after = held_option_codes(self.state)
        if held_after != held_before:
            _append_jsonl(
                self.events_path,
                {
                    "event": "subscription_bootstrap_expired_positions_settled",
                    "at_utc": _now_iso(),
                    "trading_date": trading_date.isoformat(),
                    "held_option_codes_before": list(held_before),
                    "held_option_codes_after": list(held_after),
                    "official_settlement_only": True,
                },
            )
        self._persist_state()

    def _maybe_settle_expired_cycle(self, now: datetime, decision_ns: int) -> None:
        cycle = self.state.get("active_cycle")
        if not cycle:
            return
        expiry = date.fromisoformat(str(cycle["expiry_date"]))
        if expiry >= taifex_trading_date(now):
            return
        if any(
            int(self.state["strategies"][strategy_id]["futures_position"]) != 0
            for strategy_id in self.strategy_ids
        ):
            raise RuntimeError(
                "cannot cash-settle cycle while a shadow futures hedge remains open"
            )
        settlement = self._official_settlement(expiry)
        if settlement is None:
            self.state["engine_status"] = "blocked_missing_official_final_settlement"
            self.state["blocked_reason"] = f"missing_official_final_settlement:{expiry}"
            return
        settlement_price, source_file, source_sha = settlement
        for strategy_id in self.strategy_ids:
            if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID:
                continue
            ledger = self.state["strategies"][strategy_id]
            positions = dict(ledger.get("option_positions") or {})
            metadata = ledger.get("option_position_metadata") or {}
            for code, contracts in positions.items():
                position = int(contracts)
                if position == 0:
                    continue
                leg = metadata.get(code) or {}
                right = str(leg.get("option_right") or "")
                strike = float(leg.get("strike") or 0.0)
                if right == "C":
                    price = max(settlement_price - strike, 0.0)
                elif right == "P":
                    price = max(strike - settlement_price, 0.0)
                else:
                    raise RuntimeError(
                        f"missing option right for held strategy leg: {code}"
                    )
                tax = (
                    abs(position)
                    * option_cash_settlement_transaction_tax_twd(
                        settlement_price,
                        settlement_date=expiry,
                        multiplier_twd_per_point=OPTION_MULTIPLIER,
                    )
                    if price > 0.0
                    else 0.0
                )
                self._record_ideal_trade(
                    strategy_id=strategy_id,
                    instrument_type="option",
                    product="TXO",
                    code=code,
                    delta_contracts=-position,
                    price_points=price,
                    decision_ns=decision_ns,
                    receive_ns=decision_ns,
                    reason="official_taifex_final_cash_settlement",
                    series=str(leg.get("series") or cycle["series"]),
                    strike=strike,
                    option_right=right,
                    fee_twd=0.0,
                    tax_twd=tax,
                    price_source="official_taifex_final_settlement_intrinsic_value",
                )
        _append_jsonl(
            self.events_path,
            {
                "event": "cycle_cash_settled",
                "at_utc": _now_iso(),
                "cycle_id": cycle["cycle_id"],
                "expiry_date": expiry.isoformat(),
                "official_final_settlement": settlement_price,
                "source_file": source_file,
                "source_sha256": source_sha,
            },
        )
        self.state["active_cycle"] = None
        self.state["pending_targets"] = {}
        for strategy_id, ledger in self.state["strategies"].items():
            if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID:
                continue
            ledger["entry_state"] = (
                "pending"
                if bool(ledger.get("alive", True))
                else "awaiting_next_trading_date_recapitalization"
            )
            ledger["pending_option_roll"] = None
        self.state["last_settled_expiry"] = expiry.isoformat()
        self.state["blocked_reason"] = None
        self.state["engine_status"] = "flat_ready_for_next_cycle"
        self._persist_state()

    def _maybe_settle_expired_put_call_parity(
        self,
        now: datetime,
        decision_ns: int,
    ) -> None:
        parity_state = self.state["put_call_parity_tx"]
        open_position = parity_state.get("open_position")
        if not isinstance(open_position, Mapping):
            return
        expiry = date.fromisoformat(str(open_position["expiry_date"]))
        if expiry >= taifex_trading_date(now):
            return
        settlement = self._official_settlement(expiry)
        if settlement is None:
            parity_state["monitor"] = {
                **(parity_state.get("monitor") or {}),
                "state": "blocked_missing_official_final_settlement",
            }
            self.state["engine_status"] = "blocked_missing_official_final_settlement"
            self.state["blocked_reason"] = (
                f"missing_put_call_parity_tx_official_final_settlement:{expiry}"
            )
            return
        settlement_price, source_file, source_sha = settlement
        ledger = self.state["strategies"][PUT_CALL_PARITY_TX_STRATEGY_ID]
        metadata = ledger.get("option_position_metadata") or {}
        for code, raw_position in dict(ledger.get("option_positions") or {}).items():
            position = int(raw_position)
            if position == 0:
                continue
            leg = metadata.get(code) or {}
            right = str(leg.get("option_right") or "")
            strike = float(leg.get("strike") or 0.0)
            if right == "C":
                price = max(settlement_price - strike, 0.0)
            elif right == "P":
                price = max(strike - settlement_price, 0.0)
            else:
                raise RuntimeError(f"missing parity option right: {code}")
            tax = (
                abs(position)
                * option_cash_settlement_transaction_tax_twd(
                    settlement_price,
                    settlement_date=expiry,
                    multiplier_twd_per_point=OPTION_MULTIPLIER,
                )
                if price > 0.0
                else 0.0
            )
            self._record_ideal_trade(
                strategy_id=PUT_CALL_PARITY_TX_STRATEGY_ID,
                instrument_type="option",
                product="TXO",
                code=code,
                delta_contracts=-position,
                price_points=price,
                decision_ns=decision_ns,
                receive_ns=decision_ns,
                reason="put_call_parity_tx_official_cash_settlement",
                series=str(leg.get("series") or open_position.get("series") or ""),
                strike=strike,
                option_right=right,
                fee_twd=0.0,
                tax_twd=tax,
                price_source="official_taifex_final_settlement_intrinsic_value",
            )
        future_position = int(ledger.get("underlying_futures_position") or 0)
        if future_position == 0:
            raise RuntimeError("put-call parity settlement lost its TX future leg")
        future_tax = abs(future_position) * taifex_tax_per_contract_twd(
            settlement_price,
            multiplier_twd_per_point=self.underlying_multiplier,
            tax_rate=stock_index_futures_tax_rate(expiry),
        )
        self._record_ideal_trade(
            strategy_id=PUT_CALL_PARITY_TX_STRATEGY_ID,
            instrument_type="future",
            product=self.underlying_product,
            code=str(open_position.get("future_code") or self.underlying.code),
            delta_contracts=-future_position,
            price_points=settlement_price,
            decision_ns=decision_ns,
            receive_ns=decision_ns,
            reason="put_call_parity_tx_official_cash_settlement",
            fee_twd=0.0,
            tax_twd=future_tax,
            price_source="official_taifex_final_settlement",
            future_multiplier_twd_per_point=self.underlying_multiplier,
            future_fee_per_side_twd=self.underlying_fee_per_side_twd,
            future_position_field="underlying_futures_position",
        )
        settled_mark = self._strategy_mark(
            PUT_CALL_PARITY_TX_STRATEGY_ID,
            decision_ns,
        )
        parity_state["open_position"] = None
        parity_state["pending_signal"] = None
        parity_state["last_settled_expiry"] = expiry.isoformat()
        parity_state["monitor"] = {
            **(parity_state.get("monitor") or {}),
            "state": "settled_waiting_next_monthly_contract",
            "settled_at_decision_ts_ns": int(decision_ns),
            "official_final_settlement": settlement_price,
            "realized_cumulative_pnl_twd": settled_mark.get("cumulative_pnl_twd"),
        }
        ledger["entry_state"] = "settled_waiting_next_monthly_contract"
        if str(self.state.get("blocked_reason") or "").startswith(
            "missing_put_call_parity_tx_official_final_settlement:"
        ):
            self.state["blocked_reason"] = None
        _append_jsonl(
            self.events_path,
            {
                "event": "put_call_parity_tx_cash_settled",
                "at_utc": _now_iso(),
                "position_id": open_position.get("position_id"),
                "expiry_date": expiry.isoformat(),
                "official_final_settlement": settlement_price,
                "source_file": source_file,
                "source_sha256": source_sha,
                "realized_cumulative_pnl_twd": settled_mark.get("cumulative_pnl_twd"),
            },
        )
        self._persist_state()

    def _strategy_mark(self, strategy_id: str, decision_ns: int) -> dict[str, Any]:
        ledger = self.state["strategies"][strategy_id]
        fresh_open_value = 0.0
        cycle = self.state.get("active_cycle")
        parity_state = self.state.get("put_call_parity_tx") or {}
        parity_position = (
            parity_state.get("open_position")
            if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID
            else None
        )
        cycle_id = (
            parity_position.get("position_id")
            if isinstance(parity_position, Mapping)
            else cycle.get("cycle_id")
            if cycle
            else None
        )
        option_positions = {
            str(code): int(quantity)
            for code, quantity in (ledger.get("option_positions") or {}).items()
            if int(quantity) != 0
        }
        option_position_fingerprint = sorted(option_positions.items())
        short_margin_required = 0.0
        underlying_book = self._underlying_book(
            decision_ns,
            max_age=SURFACE_BOOK_MAX_AGE_SECONDS,
        )
        underlying_mark = (
            sum(underlying_book) / 2.0 if underlying_book is not None else None
        )
        option_metadata = ledger.get("option_position_metadata") or {}
        # A flat ledger has an exact zero liquidation value even while another
        # catalogue strategy already owns the active cycle.  It must not depend
        # on an option quote until it actually acquires a position.
        option_books_valid = not option_positions
        if (cycle or parity_position) and ledger.get("entry_state") in {
            "entered",
            "forced_flat",
            "ruined",
        }:
            option_books_valid = True
            for code, position in option_positions.items():
                swept = _depth_swept_price(
                    self.latest_books.get(code),
                    delta_contracts=-position,
                    decision_ns=decision_ns,
                    maximum_age_seconds=SURFACE_BOOK_MAX_AGE_SECONDS,
                )
                if swept is None:
                    option_books_valid = False
                    break
                fresh_open_value += position * swept[0] * OPTION_MULTIPLIER
                if position < 0:
                    leg = option_metadata.get(code) or {}
                    if underlying_mark is None:
                        option_books_valid = False
                        break
                    short_margin_required += abs(position) * _txo_short_margin_twd(
                        option_price=swept[0],
                        underlying_price=underlying_mark,
                        strike=float(leg.get("strike") or 0.0),
                        option_right=str(leg.get("option_right") or ""),
                        risk_margin_a_twd=self.option_risk_margin_a_twd,
                        risk_margin_b_twd=self.option_risk_margin_b_twd,
                    )
        future_position = int(ledger["futures_position"])
        future_sweep = (
            _depth_swept_price(
                self.latest_books.get(self.hedge.code),
                delta_contracts=-future_position,
                decision_ns=decision_ns,
                maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            )
            if future_position
            else None
        )
        future_mark_valid = future_position == 0 or future_sweep is not None
        if future_position and future_sweep:
            fresh_open_value += (
                future_position * future_sweep[0] * self.hedge_multiplier
            )
        underlying_future_position = int(ledger.get("underlying_futures_position") or 0)
        underlying_future_sweep = (
            _depth_swept_price(
                self.latest_books.get(self.underlying.code),
                delta_contracts=-underlying_future_position,
                decision_ns=decision_ns,
                maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            )
            if underlying_future_position
            else None
        )
        underlying_future_mark_valid = (
            underlying_future_position == 0 or underlying_future_sweep is not None
        )
        if underlying_future_position and underlying_future_sweep:
            fresh_open_value += (
                underlying_future_position
                * underlying_future_sweep[0]
                * self.underlying_multiplier
            )
        mark_at_taipei = datetime.fromtimestamp(decision_ns / 1e9, tz=TAIPEI)
        margin_trading_date = taifex_trading_date(mark_at_taipei)
        futures_margin_per_contract = taifex_initial_margin_twd(
            self.hedge_product, margin_trading_date
        )
        margin_required = (
            short_margin_required
            + abs(future_position) * futures_margin_per_contract
            + abs(underlying_future_position)
            * taifex_initial_margin_twd(self.underlying_product, margin_trading_date)
        )

        live_complete = (
            option_books_valid and future_mark_valid and underlying_future_mark_valid
        )
        valuation_source = "unavailable"
        valuation_carried_forward = False
        valuation_age_seconds: float | None = None
        open_value: float | None = None
        if live_complete:
            open_value = float(fresh_open_value)
            valuation_source = "fresh_executable_bidask"
            ledger["last_complete_open_liquidation_value_twd"] = open_value
            ledger["last_complete_mark_decision_ts_ns"] = int(decision_ns)
            ledger["last_complete_mark_cycle_id"] = cycle_id
            ledger["last_complete_mark_futures_position"] = future_position
            ledger["last_complete_mark_underlying_futures_position"] = (
                underlying_future_position
            )
            ledger["last_complete_mark_option_positions"] = option_position_fingerprint
            valuation_age_seconds = 0.0
        else:
            cached_value = ledger.get("last_complete_open_liquidation_value_twd")
            cached_ts = ledger.get("last_complete_mark_decision_ts_ns")
            cache_matches_position = (
                ledger.get("last_complete_mark_cycle_id") == cycle_id
                and ledger.get("last_complete_mark_futures_position") == future_position
                and ledger.get("last_complete_mark_underlying_futures_position")
                == underlying_future_position
                and ledger.get("last_complete_mark_option_positions")
                == option_position_fingerprint
            )
            try:
                cached_float = float(cached_value)
            except (TypeError, ValueError):
                cached_float = math.nan
            if cache_matches_position and math.isfinite(cached_float):
                open_value = cached_float
                valuation_source = "carried_forward_last_complete_mark"
                valuation_carried_forward = True
                if cached_ts is not None:
                    valuation_age_seconds = max(
                        0.0,
                        (int(decision_ns) - int(cached_ts)) / 1e9,
                    )

        cumulative_pnl: float | None = None
        total_equity: float | None = None
        initial_capital = float(ledger.get("initial_capital_twd") or 0.0)
        cumulative_contributed_capital = float(
            ledger.get("cumulative_contributed_capital_twd") or initial_capital
        )
        if open_value is not None:
            cumulative_pnl = (
                float(ledger["gross_cash_twd"])
                + open_value
                - float(ledger["fees_twd"])
                - float(ledger["tax_twd"])
            )
            total_equity = cumulative_contributed_capital + cumulative_pnl
        margin_excess = (
            total_equity - margin_required if total_equity is not None else None
        )
        ledger["margin_required_twd"] = float(margin_required)
        ledger["margin_excess_twd"] = margin_excess
        return {
            "schema_version": SCHEMA_VERSION,
            "recorded_at_utc": _now_iso(),
            "decision_ts_ns": decision_ns,
            "strategy_id": strategy_id,
            "gross_cash_twd": float(ledger["gross_cash_twd"]),
            "open_liquidation_value_twd": open_value,
            "fixed_fees_twd": float(ledger["fees_twd"]),
            "transaction_tax_twd": float(ledger["tax_twd"]),
            # Backward-compatible alias: historical readers interpreted this
            # field as cumulative P&L even though it was named net equity.
            "net_equity_twd": cumulative_pnl,
            "cumulative_pnl_twd": cumulative_pnl,
            "initial_capital_twd": initial_capital,
            "cumulative_contributed_capital_twd": (
                cumulative_contributed_capital
            ),
            "return_on_contributed_capital_pct": (
                cumulative_pnl / cumulative_contributed_capital * 100.0
                if cumulative_pnl is not None
                and cumulative_contributed_capital > 0.0
                else None
            ),
            "total_equity_twd": total_equity,
            "margin_required_twd": float(margin_required),
            "margin_excess_twd": margin_excess,
            "margin_trading_date": margin_trading_date.isoformat(),
            "futures_initial_margin_per_contract_twd": (futures_margin_per_contract),
            "alive": bool(ledger.get("alive", True)),
            "recapitalization_policy": STRATEGY_RECAPITALIZATION_POLICY,
            "capital_contribution_count": int(
                ledger.get("capital_contribution_count") or 0
            ),
            "recapitalization_count": int(
                ledger.get("recapitalization_count") or 0
            ),
            "bankruptcy_count": int(ledger.get("bankruptcy_count") or 0),
            "episode_count": int(ledger.get("episode_count") or 0),
            "last_capital_contribution_twd": ledger.get(
                "last_capital_contribution_twd"
            ),
            "last_capital_contribution_trading_date": ledger.get(
                "last_capital_contribution_trading_date"
            ),
            "last_bankruptcy_trading_date": ledger.get(
                "last_bankruptcy_trading_date"
            ),
            "last_bankruptcy_total_equity_twd": ledger.get(
                "last_bankruptcy_total_equity_twd"
            ),
            "recapitalization_eligible_now": (
                self._strategy_recapitalization_is_due(
                    strategy_id,
                    decision_ns,
                )
                if not bool(ledger.get("alive", True))
                else False
            ),
            "margin_call_count": int(ledger.get("margin_call_count") or 0),
            "forced_liquidation_pending": bool(
                ledger.get("forced_liquidation_pending")
            ),
            "option_roll_policy": STRATEGY_SPEC_BY_ID[strategy_id].option_roll_policy,
            "option_roll_pending": isinstance(
                ledger.get("pending_option_roll"), Mapping
            ),
            "option_roll_count": int(ledger.get("option_roll_count") or 0),
            "last_option_roll_decision_ts_ns": ledger.get(
                "last_option_roll_decision_ts_ns"
            ),
            "last_option_roll_signal_ts_ns": ledger.get(
                "last_option_roll_signal_ts_ns"
            ),
            "last_option_roll_forward_mid": ledger.get("last_option_roll_forward_mid"),
            "last_option_roll_atm_strike": ledger.get("last_option_roll_atm_strike"),
            "last_dynamic_hedge_cycle_id": ledger.get(
                "last_dynamic_hedge_cycle_id"
            ),
            "last_dynamic_hedge_session_id": ledger.get(
                "last_dynamic_hedge_session_id"
            ),
            "last_dynamic_hedge_decision_ts_ns": ledger.get(
                "last_dynamic_hedge_decision_ts_ns"
            ),
            "last_dynamic_hedge_forward_mid": ledger.get(
                "last_dynamic_hedge_forward_mid"
            ),
            "futures_position": future_position,
            "underlying_futures_position": underlying_future_position,
            "option_positions": option_positions,
            "entry_state": ledger.get("entry_state"),
            "active_cycle_id": cycle_id,
            "option_books_valid": option_books_valid,
            "future_book_valid": (future_mark_valid and underlying_future_mark_valid),
            "hedge_future_book_valid": future_mark_valid,
            "underlying_future_book_valid": underlying_future_mark_valid,
            "valuation_available": open_value is not None,
            "valuation_stale": valuation_carried_forward,
            "valuation_carried_forward": valuation_carried_forward,
            "valuation_age_seconds": valuation_age_seconds,
            "valuation_source": valuation_source,
            "mark_price_policy": ("five_level_depth_vwap_at_signed_liquidation_side"),
            "put_call_parity_tx": (
                parity_state.get("monitor")
                if strategy_id == PUT_CALL_PARITY_TX_STRATEGY_ID
                else None
            ),
        }

    def _write_marks(self, decision_ns: int) -> None:
        for strategy_id in self.strategy_ids:
            _append_jsonl(
                self.marks_path, self._strategy_mark(strategy_id, decision_ns)
            )
        self._persist_state()

    def _write_status(
        self, *, force: bool = False, decision_ns: int | None = None
    ) -> None:
        now_monotonic = time.monotonic()
        if (
            not force
            and now_monotonic - self.last_status_monotonic < STATUS_INTERVAL_SECONDS
        ):
            return
        timestamp_ns = int(decision_ns or time.time_ns())
        observed_at_taipei = datetime.now(TAIPEI)
        current_trading_date = taifex_trading_date(observed_at_taipei)
        official_margin_schedule = {
            level: {
                "futures_twd": taifex_futures_margin_twd(
                    self.hedge_product,
                    current_trading_date,
                    level=level,
                ),
                "txo_risk_margin_twd": taifex_txo_risk_margin_twd(
                    current_trading_date,
                    level=level,
                ),
            }
            for level in ("initial", "maintenance", "clearing")
        }
        strategy_marks = {
            strategy_id: self._strategy_mark(strategy_id, timestamp_ns)
            for strategy_id in self.strategy_ids
        }
        strategy_count = len(strategy_marks)
        valuation_available_count = sum(
            bool(mark.get("valuation_available")) for mark in strategy_marks.values()
        )
        fresh_valuation_count = sum(
            mark.get("valuation_source") == "fresh_executable_bidask"
            for mark in strategy_marks.values()
        )
        carried_valuation_count = sum(
            bool(mark.get("valuation_carried_forward"))
            for mark in strategy_marks.values()
        )
        required_held_codes = held_option_codes(self.state)
        subscribed_held_codes = tuple(
            code for code in required_held_codes if code in self.options_by_code
        )
        held_codes_with_any_book = tuple(
            code for code in required_held_codes if code in self.latest_books
        )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "execution_contract_version": EXECUTION_CONTRACT_VERSION,
            "updated_at_utc": _now_iso(),
            "simulation_only": True,
            "production_order_possible": False,
            "strategy_mode": self.strategy_mode,
            "current_session": taifex_session_kind(observed_at_taipei),
            "current_market_phase": taifex_market_phase(observed_at_taipei),
            "current_trading_date": current_trading_date.isoformat(),
            "intraday_decision_interval_seconds": (
                self.intraday_decision_interval_seconds
                if self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES
                else None
            ),
            "intraday_entry_cutoff": (
                self.intraday_entry_cutoff.isoformat()
                if self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES
                else None
            ),
            "intraday_flatten_time": (
                self.intraday_flatten_time.isoformat()
                if self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES
                else None
            ),
            "night_entry_cutoff": (
                self.night_entry_cutoff.isoformat()
                if self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES
                else None
            ),
            "night_flatten_time": (
                self.night_flatten_time.isoformat()
                if self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES
                else None
            ),
            "last_intraday_decision_bucket": self.state.get(
                "last_intraday_decision_bucket"
            ),
            "last_intraday_flatten_date": self.state.get("last_intraday_flatten_date"),
            "last_intraday_decision_session": self.state.get(
                "last_intraday_decision_session"
            ),
            "last_intraday_flatten_session": self.state.get(
                "last_intraday_flatten_session"
            ),
            "engine_status": self.state.get("engine_status"),
            "blocked_reason": self.state.get("blocked_reason"),
            "bootstrap_after_date": self.state.get("bootstrap_after_date"),
            "active_cycle": self.state.get("active_cycle"),
            "pending_targets": self.state.get("pending_targets"),
            "broker_orders_enabled": self.broker_orders_enabled,
            "broker_order_failures": int(self.state.get("broker_order_failures", 0)),
            "inflight_order_count": len(self.state.get("inflight_orders") or {}),
            "underlying_contract": self.underlying.code,
            "underlying_product": self.underlying_product,
            "underlying_multiplier_twd_per_point": self.underlying_multiplier,
            "underlying_fee_per_side_twd": self.underlying_fee_per_side_twd,
            "underlying_initial_margin_per_contract_twd": (
                taifex_initial_margin_twd(
                    self.underlying_product,
                    current_trading_date,
                )
            ),
            "hedge_contract": self.hedge.code,
            "hedge_product": self.hedge_product,
            "hedge_multiplier_twd_per_point": self.hedge_multiplier,
            "hedge_fee_per_side_twd": self.hedge_fee_per_side_twd,
            "option_risk_margin_a_twd": self.option_risk_margin_a_twd,
            "option_risk_margin_b_twd": self.option_risk_margin_b_twd,
            "option_risk_margin_c_twd": self.option_risk_margin_c_twd,
            "option_risk_margin_effective_trading_date": (
                TAIFEX_MARGIN_2026_08_13_EFFECTIVE_DATE.isoformat()
            ),
            "option_risk_margin_source_url": (
                TAIFEX_MARGIN_2026_08_13_ANNOUNCEMENT_URL
            ),
            "option_margin_policy": OPTION_MARGIN_POLICY,
            "margin_requirement_basis": "initial",
            "official_margin_schedule": official_margin_schedule,
            "futures_initial_margin_per_contract_twd": (
                taifex_initial_margin_twd(
                    self.hedge_product,
                    current_trading_date,
                )
            ),
            "strategy_capital_buffer_multiple": (self.strategy_capital_buffer_multiple),
            "strategy_recapitalization_policy": STRATEGY_RECAPITALIZATION_POLICY,
            "strategy_cumulative_contributed_capital_twd": sum(
                float(
                    mark.get("cumulative_contributed_capital_twd")
                    or mark.get("initial_capital_twd")
                    or 0.0
                )
                for mark in strategy_marks.values()
            ),
            "strategy_recapitalization_count": sum(
                int(mark.get("recapitalization_count") or 0)
                for mark in strategy_marks.values()
            ),
            "strategy_bankruptcy_count": sum(
                int(mark.get("bankruptcy_count") or 0)
                for mark in strategy_marks.values()
            ),
            "catalog_expansion_entry_policy": (self.catalog_expansion_entry_policy),
            "option_contract_count": len(self.options),
            "latest_book_count": len(self.latest_books),
            "held_option_contract_count": len(required_held_codes),
            "held_option_subscribed_count": len(subscribed_held_codes),
            "held_option_book_count": len(held_codes_with_any_book),
            "missing_held_option_subscription_codes": sorted(
                set(required_held_codes) - set(subscribed_held_codes)
            ),
            "strategy_count": strategy_count,
            "strategy_valuation_available_count": valuation_available_count,
            "strategy_fresh_valuation_count": fresh_valuation_count,
            "strategy_carried_valuation_count": carried_valuation_count,
            "put_call_parity_tx": self.state.get("put_call_parity_tx"),
            "strategies": strategy_marks,
        }
        _atomic_json(self.status_path, payload)
        self.last_status_monotonic = now_monotonic

    def step(self, *, now: datetime | None = None) -> None:
        observed_now = now or datetime.now(TAIPEI)
        decision_ns = time.time_ns()
        self._drain_callbacks()
        previous_step_error = self.state.get("last_engine_step_error")
        if (
            previous_step_error is None
            and self.state.get("engine_status") != "blocked"
            and str(self.state.get("blocked_reason") or "").startswith(
                "ValueError: held-series IV surface is too sparse:"
            )
        ):
            # Compatibility for the pre-v4 process, which could recover its
            # active status after subscription warm-up without tagging and
            # clearing this transient error.
            previous_step_error = self.state.get("blocked_reason")
        try:
            self._maybe_settle_expired_put_call_parity(observed_now, decision_ns)
            self._maybe_settle_expired_cycle(observed_now, decision_ns)
            self._maybe_open_cycle(observed_now, decision_ns)
            session_state = self._intraday_session_state(observed_now)
            if (
                self.strategy_mode == STRATEGY_MODE_INTRADAY_FUTURES
                and bool(session_state["entry_allowed"])
            ) or (
                self.strategy_mode == STRATEGY_MODE_DAILY
                and DAY_OPEN <= observed_now.time() < datetime_time(9, 5)
            ):
                self._maybe_enter_cycle_strategies(decision_ns)
            self._maybe_roll_straddles(decision_ns)
            self._maybe_execute_pending_targets(observed_now, decision_ns)
            self._maybe_apply_fixed_future_targets(observed_now, decision_ns)
            self._maybe_run_put_call_parity(observed_now, decision_ns)
            self._maybe_flatten_expiry_hedges(observed_now, decision_ns)
            self._maybe_flatten_intraday_futures(observed_now, decision_ns)
            self._maybe_calibrate(observed_now, decision_ns)
            self._maybe_enforce_strategy_margin(decision_ns)
        except Exception as exc:
            step_error = f"{type(exc).__name__}: {exc}"
            self.state["engine_status"] = "blocked"
            self.state["blocked_reason"] = step_error
            self.state["last_engine_step_error"] = step_error
            self._persist_state()
            _append_jsonl(
                self.events_path,
                {
                    "event": "engine_step_blocked",
                    "at_utc": _now_iso(),
                    "error": self.state["blocked_reason"],
                },
            )
        else:
            if (
                previous_step_error is not None
                and self.state.get("blocked_reason") == previous_step_error
            ):
                self.state["blocked_reason"] = None
                self.state["last_engine_step_error"] = None
                self._persist_state()
        now_monotonic = time.monotonic()
        if now_monotonic - self.last_mark_monotonic >= MARK_INTERVAL_SECONDS:
            self._write_marks(decision_ns)
            self.last_mark_monotonic = now_monotonic
        self._write_status(decision_ns=decision_ns)

    def close(self) -> None:
        self._drain_callbacks()
        self._write_status(force=True)
        self._persist_state()
        try:
            fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.lock_handle.close()


__all__ = [
    "CLASSIC_VARIANT_ID",
    "EXECUTION_CONTRACT_VERSION",
    "MODEL_VARIANT_PREFIX",
    "OptionInstrument",
    "STRATEGY_RECAPITALIZATION_POLICY",
    "STRATEGY_IDS",
    "TaifexVolatilitySimulation",
    "FuturesInstrument",
    "option_instruments",
]
