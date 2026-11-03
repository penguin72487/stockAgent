"""Independent, hash-pinned TX reference; never add benchmark rows to the action universe."""
from __future__ import annotations
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file


TX_ONE_CONTRACT_REPLAY_ARTIFACT_ABI = "tx_one_contract_source_bound_replay_artifact_v1"


@dataclass(frozen=True, slots=True)
class TxOneContractReplayContext:
    """Independent caller pins for a replay, not a source-admission decision.

    The event-source fingerprint must bind the reviewed originals, while the
    execution fingerprint identifies their clock/cost/roll interpretation.
    Possession of these hashes alone does not establish either interpretation.
    """

    event_source_fingerprint: str
    execution_release_fingerprint: str
    account_calendar_fingerprint: str
    replay_input_fingerprint: str

    def validate(self) -> None:
        for value in (self.event_source_fingerprint, self.execution_release_fingerprint,
                      self.account_calendar_fingerprint, self.replay_input_fingerprint):
            if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError("TX replay artifact context requires SHA256 fingerprints")


def _replay_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _tx_replay_inputs(events, initial_capital_twd, multiplier_twd_per_point,
                      opening_event, require_market_close_position) -> dict:
    if (type(require_market_close_position) is not bool
            or isinstance(initial_capital_twd, bool) or isinstance(multiplier_twd_per_point, bool)):
        raise ValueError("TX replay artifact requires numeric capital and a boolean position policy")
    return json.loads(_replay_json({
        "events": events, "opening_event": opening_event,
        "initial_capital_twd": initial_capital_twd,
        "multiplier_twd_per_point": multiplier_twd_per_point,
        "require_market_close_position": require_market_close_position,
    }))


def tx_one_contract_replay_input_fingerprint(
    events: list[dict], *, initial_capital_twd: float, multiplier_twd_per_point: float,
    opening_event: dict | None = None, require_market_close_position: bool = True,
) -> str:
    """Pin the exact tape, independent opening, capital and accounting policy."""
    inputs = _tx_replay_inputs(events, initial_capital_twd, multiplier_twd_per_point,
                              opening_event, require_market_close_position)
    return hashlib.sha256(_replay_json(inputs).encode()).hexdigest()


def _tx_replay_payload(inputs: dict, replay: dict, context: TxOneContractReplayContext) -> dict:
    arrays = {key: {"dtype": value.dtype.str, "values": value.astype(str).tolist()
                    if value.dtype.kind == "M" else value.tolist()}
              for key, value in replay.items() if isinstance(value, np.ndarray)}
    metadata = {key: value for key, value in replay.items() if not isinstance(value, np.ndarray)}
    return {
        "contract": TX_ONE_CONTRACT_REPLAY_ARTIFACT_ABI,
        "context": {name: getattr(context, name) for name in context.__dataclass_fields__},
        "inputs": inputs,
        "inputs_sha256": hashlib.sha256(_replay_json(inputs).encode()).hexdigest(),
        "arrays": arrays, "metadata": metadata,
    }


def tx_one_contract_replay_payload(
    events: list[dict], *, calendar, context: TxOneContractReplayContext,
    initial_capital_twd: float, multiplier_twd_per_point: float,
    opening_event: dict | None = None, require_market_close_position: bool = True,
) -> dict:
    """Build a JSON-safe, reproducible replay alongside retained input events.

    This reuses the canonical event account, including each funding boundary;
    it never computes returns by differencing deposit-inclusive daily equity.
    Both the complete tape and an independent fold's own opening are retained.
    Save with the existing atomic JSON writer, then read with independently
    supplied context pins. This payload is not training/source admission.
    """
    if not isinstance(context, TxOneContractReplayContext):
        raise ValueError("TX replay artifact requires the caller's pinned context")
    context.validate()
    if getattr(calendar, "fingerprint", None) != context.account_calendar_fingerprint:
        raise ValueError("TX replay artifact calendar or position policy differs")
    # Detach mutable caller dictionaries, reject NaN/Infinity, and keep the
    # precise independent opening even when it is absent from the shared tape.
    inputs = _tx_replay_inputs(events, initial_capital_twd, multiplier_twd_per_point,
                              opening_event, require_market_close_position)
    if hashlib.sha256(_replay_json(inputs).encode()).hexdigest() != context.replay_input_fingerprint:
        raise ValueError("TX replay artifact input fingerprint differs from pinned context")
    replay = replay_tx_one_contract_on_account_calendar(calendar=calendar, **inputs)
    return _tx_replay_payload(inputs, replay, context)


def tx_one_contract_replay_from_payload(
    payload: Mapping, *, context: TxOneContractReplayContext,
) -> dict:
    """Rebuild and verify persisted account arithmetic before report use.

    A same-calendar fingerprint cannot validate a different source, fold
    opening, funding path or roll policy. The caller supplies source pins;
    the stored events reproduce every serialized output exactly. This is a
    persistence/integrity gate, not proof of authentic historical execution.
    """
    from stockagent.data.tw_futures_account_calendar import build_futures_account_calendar

    if not isinstance(context, TxOneContractReplayContext):
        raise ValueError("TX replay artifact requires the caller's pinned context")
    context.validate()
    keys = {"contract", "context", "inputs", "inputs_sha256", "arrays", "metadata"}
    if (not isinstance(payload, Mapping) or set(payload) != keys
            or payload.get("contract") != TX_ONE_CONTRACT_REPLAY_ARTIFACT_ABI
            or payload.get("context") != {name: getattr(context, name) for name in context.__dataclass_fields__}):
        raise ValueError("TX replay artifact contract or pinned context differs")
    try:
        inputs = payload["inputs"]
        if (not isinstance(inputs, dict) or set(inputs) != {
                "events", "opening_event", "initial_capital_twd", "multiplier_twd_per_point",
                "require_market_close_position"}
                or payload["inputs_sha256"] != context.replay_input_fingerprint
                or hashlib.sha256(_replay_json(inputs).encode()).hexdigest() != payload["inputs_sha256"]):
            raise ValueError("TX replay artifact input fingerprint differs")
        arrays = payload["arrays"]
        decoded = {}
        for key in ("dates", "decision_dates", "market_session_mask"):
            item = arrays[key]
            if set(item) != {"dtype", "values"}:
                raise ValueError("TX replay artifact array layout differs")
            decoded[key] = np.asarray(item["values"], dtype=np.dtype(item["dtype"]))
        market = decoded["market_session_mask"]
        if market.dtype != np.bool_:
            raise ValueError("TX replay artifact requires a boolean market mask")
        calendar = build_futures_account_calendar(
            decoded["decision_dates"], decoded["dates"], ~market)
        if (calendar.fingerprint != context.account_calendar_fingerprint
                or type(inputs["require_market_close_position"]) is not bool
                or isinstance(inputs["initial_capital_twd"], bool)
                or isinstance(inputs["multiplier_twd_per_point"], bool)):
            raise ValueError("TX replay artifact calendar or position policy differs")
        replay = replay_tx_one_contract_on_account_calendar(calendar=calendar, **inputs)
        rebuilt = _tx_replay_payload(inputs, replay, context)
        # Compare schema, dtypes, every value and scope, rather than accepting
        # a return vector whose sum happens to agree with a daily endpoint.
        if _replay_json(rebuilt) != _replay_json(dict(payload)):
            raise ValueError("TX replay artifact disagrees with event replay")
        return replay
    except (KeyError, TypeError, OverflowError) as error:
        raise ValueError("invalid TX replay artifact layout") from error


def tx_one_contract_continuous_report_segment(
    payload: Mapping, *, context: TxOneContractReplayContext, start: int, stop: int,
) -> dict:
    """Derive a report segment from one verified continuous TX account.

    ``start`` and ``stop`` index decision sessions, not calendar days. An
    account-only event immediately before a decision belongs to that decision,
    including at a fold boundary. Every output is copied from the full replay;
    the account is never reset or re-created at the boundary. Cumulative
    deposits remain cumulative from the original account opening. The incoming
    equity and contract are audit context, not a complete executable state.

    The caller's independent pins and the *entire* persisted event tape are
    checked before selecting any rows. This is a report adapter, not proof of
    source admission, actual execution, or training/report integration.
    """
    from stockagent.data.tw_futures_account_calendar import (
        build_futures_account_calendar, slice_futures_account_calendar,
    )

    replay = tx_one_contract_replay_from_payload(payload, context=context)
    if replay["account_scope"] != "continuous_from_first_supplied_event":
        raise ValueError("TX report segment requires a continuous parent account")
    count = len(replay["decision_dates"])
    if (type(start) is not int or type(stop) is not int
            or not 0 <= start < stop <= count):
        raise ValueError("TX report segment requires a nonempty decision slice")
    calendar = build_futures_account_calendar(
        replay["decision_dates"], replay["dates"], ~replay["market_session_mask"],
    )
    segment_calendar = slice_futures_account_calendar(calendar, start, stop)
    first = 0 if start == 0 else int(calendar.decision_indices[start - 1]) + 1
    last = int(calendar.decision_indices[stop - 1]) + 1
    account_fields = {
        "dates", "market_session_mask", "end_contract_months", "cash_settlement_mask",
        "open_trade_count", "close_trade_count", "valuation_mark_count", "peak_reserved_margin_twd",
        "gross_pnl_twd", "transaction_and_settlement_cost_twd", "external_deposit_twd",
        "cumulative_external_deposit_twd", "equity_twd", "benchmark_log_returns",
    }
    decision_fields = {"decision_dates", "decision_log_returns"}
    array_fields = {name for name, value in replay.items() if isinstance(value, np.ndarray)}
    if array_fields != account_fields | decision_fields:
        raise ValueError("TX report segment does not recognize the parent replay arrays")
    result = {name: replay[name][first:last].copy() for name in account_fields}
    result.update({name: replay[name][start:stop].copy() for name in decision_fields})
    result.update(
        account_calendar_fingerprint=segment_calendar.fingerprint,
        account_replay_contract="tx_one_contract_account_calendar_replay_v1",
        account_scope="continuous_from_first_supplied_event",
        segment_contract="tx_verified_continuous_report_segment_v1",
        parent_event_source_fingerprint=context.event_source_fingerprint,
        parent_execution_release_fingerprint=context.execution_release_fingerprint,
        parent_account_calendar_fingerprint=context.account_calendar_fingerprint,
        parent_replay_input_fingerprint=context.replay_input_fingerprint,
        parent_decision_start=start,
        parent_decision_stop=stop,
        incoming_equity_twd=(float(payload["inputs"]["initial_capital_twd"]) if first == 0
                             else float(replay["equity_twd"][first - 1])),
        incoming_contract_month=("" if first == 0 else str(replay["end_contract_months"][first - 1])),
        incoming_cumulative_external_deposit_twd=(
            0.0 if first == 0 else float(replay["cumulative_external_deposit_twd"][first - 1])
        ),
    )
    return result


def resolve_tx_benchmark_path(trading) -> Path:
    path = Path(trading.tw_futures_portfolio_benchmark_data_path or trading.tw_futures_portfolio_data_path)
    expected = trading.tw_futures_portfolio_benchmark_sha256
    if expected is not None and sha256_file(path) != expected:
        raise ValueError("TX benchmark source SHA-256 mismatch")
    return path


def load_tx_front_rolling_benchmark(
    data_path: str | Path,
    panel_dates: np.ndarray,
    *,
    account_only_mask: np.ndarray | None = None,
) -> dict[str, np.ndarray]:
    """Gross 1x TX front-month close-to-close return with a prior-close roll.

    On a roll, use the *new* front contract's own previous TX-session close.
    A missing/unobserved mark is an error, not a zero return or a calendar-spread
    jump. Every TX source session within the panel horizon must be represented:
    silently skipping a session would omit its return. Dates preceding source
    inception are context only and receive zero.
    """

    dates = np.asarray(panel_dates, dtype="datetime64[D]")
    if dates.ndim != 1 or not dates.size or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1]):
        raise ValueError("TX benchmark panel dates must be finite and increasing")
    account_only = np.zeros(len(dates), dtype=bool)
    if account_only_mask is not None:
        account_only = np.asarray(account_only_mask)
        if account_only.dtype != np.bool_ or account_only.shape != dates.shape or account_only.all():
            raise ValueError("TX benchmark account-only mask must classify the dated panel")
    table = pq.read_table(
        data_path,
        columns=["date", "product", "contract", "tenor_rank", "close", "source_row_observed"],
        filters=[("product", "==", "TX"), ("date", "<=", date.fromisoformat(str(dates[-1])))],
        memory_map=True,
    )
    tx = pl.from_arrow(table).sort(["date", "contract"])
    if not tx.height:
        raise ValueError("TX benchmark has no source rows")
    source_dates = np.unique(tx["date"].to_numpy().astype("datetime64[D]"))
    front = tx.filter(pl.col("tenor_rank") == 1).sort("date")
    front_dates = front["date"].to_numpy().astype("datetime64[D]")
    if front_dates.size != source_dates.size or not np.array_equal(front_dates, source_dates):
        raise ValueError("TX benchmark requires exactly one front contract per source session")
    if np.intersect1d(dates[account_only], source_dates).size:
        raise ValueError("TX benchmark source contains a market session on an account-only date")
    last_market_date = dates[~account_only][-1]
    if last_market_date > source_dates[-1]:
        raise ValueError(
            f"TX benchmark source ends {source_dates[-1]} before panel {last_market_date}; "
            "trim the panel to the verified source first"
        )
    # A zero log return here is an accounting alignment placeholder, never an
    # observed quote or a benchmark trade on the settlement-only date.
    covered = (dates >= source_dates[0]) & ~account_only
    missing = np.setdiff1d(dates[covered], front_dates)
    if missing.size:
        raise ValueError(f"TX benchmark is missing a panel session: {missing[0]}")
    within_horizon = front_dates[(front_dates >= dates[0]) & (front_dates <= dates[-1])]
    omitted = np.setdiff1d(within_horizon, dates[~account_only])
    if omitted.size:
        raise ValueError(f"panel is missing a TX source session: {omitted[0]}")
    all_dates = tx["date"].to_numpy().astype("datetime64[D]")
    contracts = tx["contract"].to_numpy().astype(str)
    closes = tx["close"].to_numpy().astype(np.float64)
    observed = tx["source_row_observed"].to_numpy().astype(bool)
    marks: dict[tuple[np.datetime64, str], tuple[float, bool]] = {}
    for day, contract, close, is_observed in zip(all_dates, contracts, closes, observed):
        key = (day, contract)
        if key in marks or not np.isfinite(close) or close <= 0:
            raise ValueError(f"TX benchmark has duplicate or invalid close: {key}")
        marks[key] = (float(close), bool(is_observed))
    front_contracts = front["contract"].to_numpy().astype(str)
    count = len(dates)
    result: dict[str, np.ndarray] = {
        "dates": dates,
        "benchmark_log_returns": np.zeros(count, dtype=np.float32),
        "contract_months": np.full(count, "", dtype="U16"),
        "front_month_roll_mask": np.zeros(count, dtype=bool),
        "front_month_close": np.full(count, np.nan, dtype=np.float64),
        "prior_same_contract_close": np.full(count, np.nan, dtype=np.float64),
        "source_covered": covered,
    }
    for index, (day, contract) in enumerate(zip(front_dates, front_contracts)):
        if day < dates[0]:
            continue
        position = int(np.searchsorted(dates, day))
        if position == count or dates[position] != day:
            continue
        current_close, current_observed = marks[(day, contract)]
        if not current_observed:
            raise ValueError(f"TX front close was not observed on {day}")
        result["contract_months"][position] = contract
        result["front_month_close"][position] = current_close
        if index == 0:
            continue  # benchmark inception has no preceding source session
        prior_day = front_dates[index - 1]
        prior_mark = marks.get((prior_day, contract))
        if prior_mark is None or not prior_mark[1]:
            raise ValueError(f"TX {contract} lacks its observed own close on {prior_day}")
        prior_close = prior_mark[0]
        result["prior_same_contract_close"][position] = prior_close
        result["front_month_roll_mask"][position] = contract != front_contracts[index - 1]
        result["benchmark_log_returns"][position] = np.float32(np.log(current_close / prior_close))
    return result


def tx_front_benchmark_source_end(data_path: str | Path) -> np.datetime64:
    """Last observed front-month TX session in the pinned futures release."""

    table = pq.read_table(
        data_path,
        columns=["date"],
        filters=[("product", "==", "TX"), ("tenor_rank", "==", 1),
                 ("source_row_observed", "==", True)],
        memory_map=True,
    )
    if not table.num_rows:
        raise ValueError("verified TX front-month benchmark source is empty")
    return np.max(np.asarray(table["date"].to_numpy(), dtype="datetime64[D]"))


def replay_tx_one_contract_margin_benchmark(
    reference: dict[str, np.ndarray],
    *,
    expected_trading_dates: np.ndarray,
    initial_capital_twd: float,
    opening_margin_twd: np.ndarray,
    closing_margin_twd: np.ndarray,
    opening_transaction_cost_twd: np.ndarray,
    multiplier_twd_per_point: float,
) -> dict[str, np.ndarray]:
    """Replay a declared one-contract TX margin benchmark on observed marks.

    Each row spans the previous market close to this market close. The input
    reference must identify the *same physical contract* at both marks; a roll
    is assumed to occur at the previous close. Opening transaction costs are
    explicit caller-supplied TWD amounts: the first row pays entry costs, and a
    roll row pays both closing and opening costs. A zero cost means a declared
    gross scenario, not proof that a trade was free or executable.

    Cash is injected immediately before an entry/roll if needed to meet that
    row's opening initial margin after costs, and after the end mark to restore
    the supplied closing initial margin. The returned time-weighted log return
    excludes both deposits. The TWD balance, deposits, trade costs and PnL are
    separately observable. Positive end equity is required: a negative or
    zero close cannot be repaired retrospectively as if the time of a margin
    call were known.

    This is an arithmetic primitive, not a source-admission or fill claim.
    The caller must supply an independently sourced complete market-session
    calendar, bind dated margin and cost evidence, prove both sides of a roll
    executable (or explicitly label the price proxy), and include all relevant
    account-only events before using it in a full-period report.
    """
    dates = np.asarray(reference["dates"], dtype="datetime64[D]")
    if dates.ndim != 1 or not dates.size or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1]):
        raise ValueError("one-contract benchmark dates must be finite and strictly increasing")
    count = len(dates)
    expected = np.asarray(expected_trading_dates, dtype="datetime64[D]")
    if expected.ndim != 1 or not np.array_equal(dates, expected):
        raise ValueError("one-contract benchmark dates must cover the complete supplied market calendar")
    covered = np.asarray(reference["source_covered"])
    roll = np.asarray(reference["front_month_roll_mask"])
    contracts = np.asarray(reference["contract_months"])
    if (covered.shape != (count,) or covered.dtype != np.bool_ or not covered.all()
            or roll.shape != (count,) or roll.dtype != np.bool_
            or contracts.shape != (count,) or any(not str(value) for value in contracts)):
        raise ValueError("one-contract benchmark requires covered sessions, contract identities and boolean rolls")
    if count > 1 and not np.array_equal(roll[1:], contracts[1:] != contracts[:-1]):
        raise ValueError("one-contract benchmark roll flags must match physical contract changes")

    def vector(value: np.ndarray, name: str, *, positive: bool) -> np.ndarray:
        out = np.asarray(value, dtype=np.float64)
        if (out.shape != (count,) or not np.isfinite(out).all()
                or (np.any(out <= 0) if positive else np.any(out < 0))):
            raise ValueError(f"one-contract benchmark requires finite {'positive' if positive else 'nonnegative'} {name} on every row")
        return out

    current = vector(reference["front_month_close"], "current marks", positive=True)
    previous = vector(reference["prior_same_contract_close"], "same-contract previous marks", positive=True)
    opening_margin = vector(opening_margin_twd, "opening margin", positive=True)
    closing_margin = vector(closing_margin_twd, "closing margin", positive=True)
    costs = vector(opening_transaction_cost_twd, "transaction costs", positive=False)
    if np.any(costs[1:][~roll[1:]] != 0):
        raise ValueError("one-contract benchmark transaction costs require an entry or roll")
    initial = float(initial_capital_twd)
    multiplier = float(multiplier_twd_per_point)
    if not np.isfinite(initial) or initial <= 0 or not np.isfinite(multiplier) or multiplier <= 0:
        raise ValueError("one-contract benchmark capital and multiplier must be finite and positive")

    opening_deposit = np.zeros(count, dtype=np.float64)
    closing_deposit = np.zeros(count, dtype=np.float64)
    gross_pnl = (current - previous) * multiplier
    before_deposit_equity = np.empty(count, dtype=np.float64)
    equity = np.empty(count, dtype=np.float64)
    returns = np.empty(count, dtype=np.float64)
    balance = initial
    for index in range(count):
        opening_deposit[index] = max(0.0, opening_margin[index] + costs[index] - balance)
        start_equity = balance + opening_deposit[index]
        marked_equity = start_equity - costs[index] + gross_pnl[index]
        if not np.isfinite(marked_equity) or marked_equity <= 0:
            raise ValueError(f"one-contract benchmark is insolvent at {dates[index]}; intraday funding is unobserved")
        before_deposit_equity[index] = marked_equity
        returns[index] = np.log(marked_equity / start_equity)
        closing_deposit[index] = max(0.0, closing_margin[index] - marked_equity)
        balance = marked_equity + closing_deposit[index]
        equity[index] = balance

    return {
        "dates": dates,
        "contract_months": contracts,
        "front_month_roll_mask": roll,
        "front_month_close": current,
        "prior_same_contract_close": previous,
        "opening_margin_twd": opening_margin,
        "closing_margin_twd": closing_margin,
        "opening_transaction_cost_twd": costs,
        "gross_pnl_twd": gross_pnl,
        "opening_external_deposit_twd": opening_deposit,
        "closing_external_deposit_twd": closing_deposit,
        "cumulative_external_deposit_twd": np.cumsum(opening_deposit + closing_deposit),
        "equity_before_closing_deposit_twd": before_deposit_equity,
        "equity_twd": equity,
        "benchmark_log_returns": returns,
    }


def replay_tx_one_contract_event_tape(
    events: list[dict],
    *,
    expected_dates: np.ndarray,
    market_session_mask: np.ndarray,
    initial_capital_twd: float,
    multiplier_twd_per_point: float,
    require_market_close_position: bool = True,
) -> dict[str, np.ndarray]:
    """Replay a long TX contract from an explicit, chronologically ordered tape.

    This is an arithmetic primitive, not a fill simulator or source-admission
    rule. Every price event supplies its physical contract, timezone-aware
    ``time``, positive ``price`` and nonempty ``price_evidence``. The caller
    must prove that a trade price is executable, that a final fixing applies to
    this contract, that margin amounts are timely and that ``expected_dates``
    includes all relevant market and broker account days.

    Events are ``open``, ``close``, ``expiry``, ``cash_settlement``, ``mark``
    and ``margin``. Only ``open`` and ``close`` execute an order; they are
    forbidden on account-only dates. ``expiry`` ends the old market exposure
    after an explicit same-day mark but keeps its margin reserved. A new
    contract can open before the expired contract's later ``cash_settlement``;
    until then the arithmetic conservatively reserves both margins. Settlement
    uses the explicit final fixing and releases the expired margin without
    inventing a closing fill. This double-margin convention is a research
    assumption, not evidence of an individual broker's intraday treatment.
    Pending settlement must resolve by the end of its supplied day. The old/new
    price spread is never booked as PnL. A ``mark`` is required on each day
    ending with a position, including days with no orders. Event evidence is a
    caller-supplied identifier, not proof of authenticity.

    Costs on trades and final settlement are explicit TWD per-event amounts;
    marks and margin updates have zero cost. Funds added immediately before an
    entry or after a mark/margin update are external deposits, not profits.
    Time-weighted log returns are chained at each actual deposit boundary.
    Non-positive equity before a deposit fails because an unobserved intraday
    margin call or liquidation cannot be reconstructed retrospectively.
    """
    days = np.asarray(expected_dates, dtype="datetime64[D]")
    market = np.asarray(market_session_mask)
    if (days.ndim != 1 or not days.size or np.isnat(days).any()
            or np.any(days[1:] <= days[:-1]) or market.shape != days.shape
            or market.dtype != np.bool_ or not market.any()):
        raise ValueError("event benchmark requires ordered calendar dates and a boolean market mask")
    if type(require_market_close_position) is not bool:
        raise ValueError("event benchmark requires an explicit boolean position policy")
    if (isinstance(initial_capital_twd, (bool, np.bool_))
            or isinstance(multiplier_twd_per_point, (bool, np.bool_))):
        raise ValueError("event benchmark requires positive finite capital and multiplier")
    try:
        initial, multiplier = float(initial_capital_twd), float(multiplier_twd_per_point)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError("event benchmark requires positive finite capital and multiplier") from error
    if not np.isfinite(initial) or initial <= 0 or not np.isfinite(multiplier) or multiplier <= 0:
        raise ValueError("event benchmark requires positive finite capital and multiplier")
    taipei = ZoneInfo("Asia/Taipei")
    by_day: list[list[dict]] = [[] for _ in days]
    previous_time: datetime | None = None
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("event benchmark requires dictionary events")
        try:
            when = datetime.fromisoformat(event["time"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("event benchmark requires a valid timezone-aware event time") from error
        if when.tzinfo is None or when.utcoffset() is None:
            raise ValueError("event benchmark requires a timezone-aware event time")
        if previous_time is not None and when <= previous_time:
            raise ValueError("event benchmark event times must be strictly increasing")
        previous_time = when
        day = np.datetime64(when.astimezone(taipei).date(), "D")
        index = int(np.searchsorted(days, day))
        if index == len(days) or days[index] != day:
            raise ValueError(f"event benchmark event date {day} is outside the supplied calendar")
        by_day[index].append(event)

    n = len(days)
    pnl = np.zeros(n, dtype=np.float64)
    costs = np.zeros(n, dtype=np.float64)
    deposits = np.zeros(n, dtype=np.float64)
    returns = np.zeros(n, dtype=np.float64)
    equity = np.empty(n, dtype=np.float64)
    contracts = np.full(n, "", dtype="U32")
    settlements = np.zeros(n, dtype=bool)
    opens = np.zeros(n, dtype=np.int64)
    closes = np.zeros(n, dtype=np.int64)
    marks = np.zeros(n, dtype=np.int64)
    peak_margin = np.zeros(n, dtype=np.float64)
    balance = initial
    held = ""
    last_mark = np.nan
    held_margin = 0.0
    pending = ""
    pending_mark = np.nan
    pending_margin = 0.0

    def finite_number(event: dict, key: str, *, positive: bool) -> float:
        try:
            raw = event[key]
            if isinstance(raw, (bool, np.bool_)):
                raise ValueError("boolean is not a financial value")
            value = float(raw)
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            raise ValueError(f"event benchmark requires {key}") from error
        if not np.isfinite(value) or (value <= 0 if positive else value < 0):
            raise ValueError(f"event benchmark has invalid {key}")
        return value

    def evidence(event: dict, key: str) -> None:
        if not isinstance(event.get(key), str) or not event[key].strip():
            raise ValueError(f"event benchmark requires {key}")

    for index, day_events in enumerate(by_day):
        marked_held = False
        for event in day_events:
            kind = event.get("event")
            if kind not in {"open", "close", "expiry", "cash_settlement", "mark", "margin"}:
                raise ValueError("event benchmark has an unknown event type")
            if not market[index] and kind in {"open", "close"}:
                raise ValueError("account-only dates cannot execute an order")
            contract = event.get("contract")
            if not isinstance(contract, str) or not contract.strip() or len(contract) > 32:
                raise ValueError("event benchmark requires a physical contract identity")
            if kind == "open":
                if held:
                    raise ValueError("event benchmark cannot hold two contracts")
                if contract == pending:
                    raise ValueError("event benchmark cannot reopen an expired pending contract")
                price = finite_number(event, "price", positive=True)
                evidence(event, "price_evidence")
                margin = finite_number(event, "margin_twd", positive=True)
                evidence(event, "margin_evidence")
                cost = finite_number(event, "cost_twd", positive=False)
                deposit = max(0.0, pending_margin + margin + cost - balance)
                balance += deposit
                if not np.isfinite(balance):
                    raise ValueError(f"event benchmark funding overflow at {days[index]}")
                deposits[index] += deposit
                before = balance
                balance -= cost
                if balance <= 0:
                    raise ValueError(f"event benchmark is insolvent at {days[index]}")
                returns[index] += np.log(balance / before)
                costs[index] += cost
                opens[index] += 1
                held, last_mark, held_margin, marked_held = contract, price, margin, False
                peak_margin[index] = max(peak_margin[index], pending_margin + held_margin)
                continue
            if kind == "cash_settlement":
                if not pending or contract != pending:
                    raise ValueError("event benchmark settlement does not match an expired pending contract")
                price = finite_number(event, "price", positive=True)
                evidence(event, "price_evidence")
                cost = finite_number(event, "cost_twd", positive=False)
                gross = (price - pending_mark) * multiplier
                before = balance
                balance += gross - cost
                if not np.isfinite(balance) or balance <= 0:
                    raise ValueError(f"event benchmark is insolvent at {days[index]}; intraday funding is unobserved")
                pnl[index] += gross
                costs[index] += cost
                returns[index] += np.log(balance / before)
                settlements[index] = True
                pending, pending_mark, pending_margin = "", np.nan, 0.0
                deposit = max(0.0, held_margin - balance)
                balance += deposit
                deposits[index] += deposit
                continue
            if not held or contract != held:
                raise ValueError("event benchmark event does not match the held physical contract")
            if kind == "expiry":
                if pending or not marked_held:
                    raise ValueError("event benchmark expiry needs a same-day mark and no pending settlement")
                evidence(event, "expiry_evidence")
                if "price" in event or ("cost_twd" in event
                        and finite_number(event, "cost_twd", positive=False) != 0):
                    raise ValueError("event benchmark expiry cannot contain a price or cost")
                pending, pending_mark, pending_margin = held, last_mark, held_margin
                held, last_mark, held_margin, marked_held = "", np.nan, 0.0, False
                continue
            if kind == "margin":
                if "price" in event or ("cost_twd" in event
                        and finite_number(event, "cost_twd", positive=False) != 0):
                    raise ValueError("event benchmark margin update cannot contain a price or cost")
                margin = finite_number(event, "margin_twd", positive=True)
                evidence(event, "margin_evidence")
                held_margin = margin
                peak_margin[index] = max(peak_margin[index], pending_margin + held_margin)
                deposit = max(0.0, pending_margin + held_margin - balance)
                balance += deposit
                deposits[index] += deposit
                continue
            price = finite_number(event, "price", positive=True)
            evidence(event, "price_evidence")
            cost = finite_number(event, "cost_twd", positive=False)
            if kind == "mark" and cost != 0:
                raise ValueError("event benchmark mark cannot charge a transaction cost")
            gross = (price - last_mark) * multiplier
            before = balance
            balance += gross - cost
            if not np.isfinite(balance) or balance <= 0:
                raise ValueError(f"event benchmark is insolvent at {days[index]}; intraday funding is unobserved")
            pnl[index] += gross
            costs[index] += cost
            returns[index] += np.log(balance / before)
            last_mark = price
            if kind == "mark":
                margin = finite_number(event, "margin_twd", positive=True)
                evidence(event, "margin_evidence")
                held_margin = margin
                peak_margin[index] = max(peak_margin[index], pending_margin + held_margin)
                deposit = max(0.0, pending_margin + held_margin - balance)
                balance += deposit
                deposits[index] += deposit
                marked_held = True
                marks[index] += 1
            else:
                closes[index] += 1
                held, last_mark, held_margin, marked_held = "", np.nan, 0.0, False
                deposit = max(0.0, pending_margin - balance)
                balance += deposit
                deposits[index] += deposit
        if pending:
            raise ValueError(f"event benchmark lacks the expired-contract settlement at {days[index]}")
        if held and not marked_held:
            raise ValueError(f"event benchmark lacks a held-contract daily mark at {days[index]}")
        if market[index] and require_market_close_position and not held:
            raise ValueError(f"event benchmark is flat at the market close on {days[index]}")
        equity[index] = balance
        contracts[index] = held

    return {
        "dates": days,
        "market_session_mask": market,
        "end_contract_months": contracts,
        "cash_settlement_mask": settlements,
        "open_trade_count": opens,
        "close_trade_count": closes,
        "valuation_mark_count": marks,
        "peak_reserved_margin_twd": peak_margin,
        "gross_pnl_twd": pnl,
        "transaction_and_settlement_cost_twd": costs,
        "external_deposit_twd": deposits,
        "cumulative_external_deposit_twd": np.cumsum(deposits),
        "equity_twd": equity,
        "benchmark_log_returns": returns,
    }


def replay_tx_one_contract_event_tape_window(
    events: list[dict],
    *,
    opening_event: dict,
    expected_dates: np.ndarray,
    market_session_mask: np.ndarray,
    initial_capital_twd: float,
    multiplier_twd_per_point: float,
    require_market_close_position: bool = True,
) -> dict[str, np.ndarray]:
    """Replay a reset account for one evaluation window, not a sliced NAV.

    ``opening_event`` is this account's independently sourced opening trade and
    margin, on the window's first market date.  It is deliberately not inferred
    from an earlier account's position or mark.  Later events are taken from the
    supplied ordered tape and subjected to the same identity, mark, settlement,
    funding, and evidence checks as the continuous replay.  An event coincident
    with the opening must be exactly the same opening, otherwise its order is
    ambiguous.  A later settlement of a position from before this account's
    opening remains an error; the caller must provide the correct account tape.

    This adapter establishes arithmetic and reset semantics only: the caller
    must still admit the historical sources, event times, fills, and calendar.
    """
    days = np.asarray(expected_dates, dtype="datetime64[D]")
    market = np.asarray(market_session_mask)
    if (days.ndim != 1 or not days.size or np.isnat(days).any()
            or np.any(days[1:] <= days[:-1]) or market.shape != days.shape
            or market.dtype != np.bool_ or not market[0]):
        raise ValueError("reset benchmark window must begin on an ordered market calendar")
    if not isinstance(opening_event, dict) or opening_event.get("event") != "open":
        raise ValueError("reset benchmark window requires an explicit opening event")

    def event_time(event: dict) -> datetime:
        if not isinstance(event, dict):
            raise ValueError("event benchmark requires dictionary events")
        try:
            when = datetime.fromisoformat(event["time"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("event benchmark requires a valid timezone-aware event time") from error
        if when.tzinfo is None or when.utcoffset() is None:
            raise ValueError("event benchmark requires a timezone-aware event time")
        return when

    taipei = ZoneInfo("Asia/Taipei")
    opening_time = event_time(opening_event)
    if np.datetime64(opening_time.astimezone(taipei).date(), "D") != days[0]:
        raise ValueError("reset benchmark opening must belong to the first market date")

    selected = [opening_event]
    previous_time: datetime | None = None
    for event in events:
        when = event_time(event)
        if previous_time is not None and when <= previous_time:
            raise ValueError("event benchmark event times must be strictly increasing")
        previous_time = when
        if when < opening_time:
            continue
        if when == opening_time:
            if event != opening_event:
                raise ValueError("reset benchmark opening has an ambiguous coincident event")
            continue
        event_day = np.datetime64(when.astimezone(taipei).date(), "D")
        if event_day > days[-1]:
            continue
        selected.append(event)

    return replay_tx_one_contract_event_tape(
        selected,
        expected_dates=days,
        market_session_mask=market,
        initial_capital_twd=initial_capital_twd,
        multiplier_twd_per_point=multiplier_twd_per_point,
        require_market_close_position=require_market_close_position,
    )


def replay_tx_one_contract_on_account_calendar(
    events: list[dict],
    *,
    calendar,
    initial_capital_twd: float,
    multiplier_twd_per_point: float,
    opening_event: dict | None = None,
    require_market_close_position: bool = True,
) -> dict:
    """Replay an explicit account tape and project its returns to market days.

    Account-only returns remain in the dated account output and are also
    attributed to the following decision day by the canonical calendar
    aggregation.  Deposits are kept in the account ledger and do not enter
    returns.  ``opening_event`` selects an independently reset evaluation
    account; omitting it replays the supplied continuous account. A fresh
    reset account may start with account-only dates before its first
    market decision. It is flat on those dates and its unchanged starting cash
    is recorded without creating a trade. A continuous slice with a leading
    event still needs an incoming physical state and is rejected. A
    training-only trailing credit has no following market day in this horizon
    and is also rejected.

    This is an accounting and calendar adapter only. Source admission,
    executable prices, report wiring and historical readiness are separate.
    """
    import torch

    from stockagent.data.tw_futures_account_calendar import (
        FuturesAccountCalendar,
        _calendar_from_nodes,
        aggregate_account_log_returns_to_decisions,
    )

    if not isinstance(calendar, FuturesAccountCalendar) or calendar.training_trailing_event_credit:
        raise ValueError("TX benchmark requires a formal futures account calendar")
    dates, decisions, mask = calendar.dates, calendar.decision_dates, calendar.account_only_mask
    if (not isinstance(dates, np.ndarray) or dates.dtype != np.dtype("datetime64[D]")
            or dates.ndim != 1 or not len(dates) or np.isnat(dates).any()
            or np.any(dates[1:] <= dates[:-1]) or not isinstance(decisions, np.ndarray)
            or decisions.dtype != np.dtype("datetime64[D]") or decisions.ndim != 1
            or not len(decisions) or np.isnat(decisions).any()
            or np.any(decisions[1:] <= decisions[:-1]) or not isinstance(mask, np.ndarray)
            or mask.dtype != np.bool_ or mask.shape != dates.shape or mask[-1]):
        raise ValueError("TX benchmark account calendar has changed or includes out-of-window events")
    checked = _calendar_from_nodes(dates.copy(), decisions.copy(), mask.copy())
    names = ("dates", "decision_dates", "account_only_mask", "decision_indices",
             "account_only_indices", "prior_decision_index")
    if (checked.fingerprint != calendar.fingerprint
            or any(not np.array_equal(getattr(checked, name), getattr(calendar, name)) for name in names)):
        raise ValueError("TX benchmark account calendar has changed or includes out-of-window events")
    leading = int(checked.decision_indices[0])
    if leading and opening_event is None:
        raise ValueError("TX benchmark account calendar has changed or includes out-of-window events; "
                         "a continuous leading event needs incoming account state")
    kwargs = dict(
        expected_dates=checked.dates[leading:],
        market_session_mask=~checked.account_only_mask[leading:],
        initial_capital_twd=initial_capital_twd,
        multiplier_twd_per_point=multiplier_twd_per_point,
        require_market_close_position=require_market_close_position,
    )
    if opening_event is None:
        result = replay_tx_one_contract_event_tape(events, **kwargs)
    else:
        result = replay_tx_one_contract_event_tape_window(
            events, opening_event=opening_event, **kwargs,
        )
    if leading:
        # The fresh account has no contract and no transactions before its
        # explicitly sourced opening. Its cash stays at initial capital under
        # this benchmark's declared zero-interest account contract.
        for key, value in tuple(result.items()):
            if key == "dates":
                result[key] = checked.dates.copy()
            elif key == "market_session_mask":
                result[key] = ~checked.account_only_mask
            else:
                fill = initial_capital_twd if key == "equity_twd" else ("" if value.dtype.kind == "U" else 0)
                result[key] = np.concatenate((np.full(leading, fill, dtype=value.dtype), value))
    account_returns = torch.from_numpy(result["benchmark_log_returns"])
    decision_returns = aggregate_account_log_returns_to_decisions(checked, account_returns)
    result.update(
        decision_dates=checked.decision_dates.copy(),
        decision_log_returns=decision_returns.numpy(),
        account_calendar_fingerprint=checked.fingerprint,
        account_replay_contract="tx_one_contract_account_calendar_replay_v1",
        account_scope=("continuous_from_first_supplied_event" if opening_event is None
                       else "independent_window_reset"),
    )
    return result
