"""Attach an executor-only account calendar without inventing stock features.

The explicit training opt-in binds the returned event tape through the
dataset, loss, evaluation, reporting and checkpoint contracts. Source and
holiday admission gates still apply independently of that opt-in.
"""
from __future__ import annotations

from dataclasses import dataclass, fields, replace
import json
from pathlib import Path

import numpy as np
import polars as pl

from stockagent.data import tw_futures_margin as margin
from stockagent.data.panel import PanelData
from stockagent.data.tw_futures_account_calendar import build_futures_account_calendar
from stockagent.data.tw_stock_context_futures_portfolio import (
    attach_configured_stock_context_futures_portfolio,
)


# Exchange notices reviewed for 2013--2026 explicitly identify these closed
# settlement/processing days. Since 2018 the notices also specify broker
# account valuations after the preceding night's session. This conservative
# admission gate is not a source-bound calendar, statement or execution.
_KNOWN_BROKER_HOLIDAY_ACCOUNT_DATES = (
    "2013-02-07", "2013-02-08", "2014-01-28", "2014-01-29",
    "2015-02-16", "2015-02-17", "2016-02-04", "2016-02-05",
    "2017-01-25", "2017-01-26",
    "2018-02-13", "2018-02-14", "2019-01-31", "2019-02-01",
    "2020-01-21", "2020-01-22", "2021-02-08", "2021-02-09",
    "2022-01-27", "2022-01-28", "2023-01-18", "2023-01-19",
    "2024-02-06", "2024-02-07", "2025-01-23", "2025-01-24",
    "2026-02-12", "2026-02-13",
)


def _reject_unreviewed_early_holiday_calendars(
    dates: np.ndarray, *, source=None, days: pl.DataFrame | None = None,
) -> tuple[str, ...]:
    """A market calendar does not establish 2011/2012 broker processing."""
    first, last = str(dates[0]), str(dates[-1])
    admitted_days: list[str] = []
    for before, after in (("2011-01-28", "2011-02-08"),
                          ("2012-01-18", "2012-01-30")):
        if first <= before and last >= after:
            from stockagent.data.tw_futures_holiday_account_dates import HolidayAccountDatesSource
            if not isinstance(source, HolidayAccountDatesSource) or days is None:
                raise ValueError(
                    f"futures broker holiday account calendar {before} to {after} "
                    "requires source-bound 2011/2012 processing and valuation review"
                )
            source.verify()
            matching = [period for period in source.periods
                        if str(period[0]) == before and str(period[1]) == after]
            if len(matching) != 1:
                raise ValueError(
                    f"futures broker holiday account calendar {before} to {after} "
                    "requires source-bound 2011/2012 processing and valuation review"
                )
            if (not {"date", "accounting_only_date"} <= set(days.columns)
                    or days["date"].is_duplicated().any()):
                raise ValueError("early holiday requires an unambiguous source account calendar")
            records = {str(row["date"]): row["accounting_only_date"] for row in days.to_dicts()}
            expected = [str(day) for day in matching[0][2]]
            if not expected or any(records.get(day) is not True for day in expected):
                raise ValueError("reviewed early holiday processing dates are absent or not account-only")
            admitted_days.extend(expected)
    return tuple(admitted_days)


def _reject_missing_holiday_account_dates(days: pl.DataFrame, dates: np.ndarray) -> None:
    """Do not treat a settlement-only calendar as a complete account path."""
    first, last = str(dates[0]), str(dates[-1])
    expected = {day for day in _KNOWN_BROKER_HOLIDAY_ACCOUNT_DATES if first <= day <= last}
    if not expected:
        return
    records = {str(row["date"]): row["accounting_only_date"] for row in days.to_dicts()}
    missing = sorted(day for day in expected if day not in records)
    invalid = sorted(day for day in expected if day in records and records[day] is not True)
    if missing or invalid:
        raise ValueError(
            "holiday broker account calendar incomplete or conflicts with market sessions: "
            f"missing={missing} invalid={invalid}"
        )


def _reject_unreviewed_holiday_valuations(
    path: Path, dates: np.ndarray, *, extra_holiday_dates: tuple[str, ...] = (),
    source=None,
) -> None:
    """Keep known exception dates out until their complete source is admitted.

    Night closes are only one rule: exempt and day-only products also need
    their dated settlement/corporate-adjustment rules checked. A fixed product
    list cannot prove the account is complete when a different product is held.
    Merely tagging some observations does not certify the remaining positions.
    """
    frame = pl.scan_parquet(path)
    schema = frame.collect_schema()
    required = {"date", "product", "accounting_only_date", "cash_settlement"}
    if not required <= set(schema.names()):
        raise ValueError("account calendar source lacks the holiday valuation scope")
    first, last = str(dates[0]), str(dates[-1])
    holidays = [day for day in (*_KNOWN_BROKER_HOLIDAY_ACCOUNT_DATES, *extra_holiday_dates)
                if first <= day <= last]
    if not holidays:
        return
    potential = frame.filter(
        pl.col("accounting_only_date")
        & pl.col("date").cast(pl.String).is_in(holidays)
        & ~pl.col("cash_settlement").fill_null(False)
    )
    if source is None:
        if potential.select("date", "product").limit(1).collect().height:
            raise ValueError("holiday nonterminal valuations require complete source-bound account review")
    else:
        from stockagent.data.tw_futures_holiday_c11 import require_complete_holiday_c11
        required = ["date", "product", "contract", "physical_instance", "valuation_price"]
        if not set(required) <= set(schema.names()):
            raise ValueError("holiday financial scope lacks unambiguous physical identities")
        require_complete_holiday_c11(path, potential.select(required).unique().collect(), source)


@dataclass(slots=True)
class _AttachmentAxes:
    """Only the real date/symbol identities needed by the futures adapter."""

    dates: np.ndarray
    symbols: list[str]
    stock_context_futures_portfolio_daily: object | None = None


def attach_configured_futures_account_calendar(panel: PanelData, config) -> PanelData:
    """Prepare market sidecar and sparse account-only tape from one source.

    ``panel`` must already be clipped to its verified decision-clock horizon.
    No synthetic stock feature, market mark, policy decision or execution is
    created for an account-only node. Source/calendar identities are bound to
    the canonical attachment and its immutable manifests.
    """
    trading = config.trading
    if (trading.execution_mode != "tw_stock_context_futures_portfolio"
            or trading.tw_futures_portfolio_capital_basis != "initial_margin"
            or trading.tw_futures_portfolio_holding_policy != "carry"
            or trading.tw_futures_portfolio_decision_time is None):
        raise ValueError("dual account attachment requires the configured carrying margin clock")
    dates = np.asarray(panel.dates, dtype="datetime64[D]")
    clock_path = Path(trading.tw_futures_portfolio_entry_clock_path)
    clock = json.loads(clock_path.with_name("manifest.json").read_text())
    if not len(dates) or str(dates[0]) != clock.get("start_date") or str(dates[-1]) != clock.get("end_date"):
        raise ValueError("dual account panel must first be clipped to the verified clock horizon")
    daily_path = Path(trading.tw_futures_portfolio_data_path)
    holiday_source = None
    holiday_path = getattr(trading, "tw_futures_holiday_account_dates_source", None)
    if holiday_path is None:
        # Preserve the original failure boundary without reading a source that
        # cannot establish the early processing dates.
        _reject_unreviewed_early_holiday_calendars(dates)
    else:
        from stockagent.data.tw_futures_holiday_account_dates import load_holiday_account_dates
        holiday_source = load_holiday_account_dates(Path(holiday_path), workspace=Path.cwd())
    days = pl.scan_parquet(daily_path).select("date", "accounting_only_date").unique().sort("date").collect()
    if days["date"].is_duplicated().any() or days["accounting_only_date"].null_count():
        raise ValueError("futures source calendar has ambiguous account-only classification")
    early_holidays = _reject_unreviewed_early_holiday_calendars(dates, source=holiday_source, days=days)
    _reject_missing_holiday_account_dates(days, dates)
    valuation_source = None
    valuation_path = getattr(trading, "tw_futures_holiday_valuation_source", None)
    if valuation_path is not None:
        from stockagent.data.tw_futures_holiday_c11 import load_holiday_c11
        valuation_source = load_holiday_c11(Path(valuation_path), workspace=Path.cwd())
    _reject_unreviewed_holiday_valuations(
        daily_path, dates, extra_holiday_dates=early_holidays, source=valuation_source)
    calendar = build_futures_account_calendar(
        dates, days["date"].to_numpy(), days["accounting_only_date"].to_numpy(),
    )
    axes = _AttachmentAxes(dates=calendar.dates, symbols=list(panel.symbols))
    attached = attach_configured_stock_context_futures_portfolio(
        axes, config, futures_account_calendar=calendar,
    )
    if holiday_source is not None:
        holiday_source.verify()
    if valuation_source is not None:
        valuation_source.verify()
    if not np.array_equal(attached.dates, calendar.dates):
        raise ValueError("configured clock unexpectedly changed the dual account horizon")
    daily = attached.stock_context_futures_portfolio_daily
    if daily is None or daily.integer_execution is None:
        raise ValueError("configured dual account lacks the exact integer tape")
    events = np.asarray(daily.integer_execution[calendar.account_only_indices], dtype=np.float32)
    capacity = events[..., [1, 8]]
    if np.any(~np.isfinite(capacity)) or np.any(capacity != 0):
        raise ValueError("account-only rows must have no executable order capacity")
    terminal = events[..., [2, margin.CASH_SETTLEMENT]]
    if (np.any(~np.isfinite(terminal)) or np.any((terminal != 0) & (terminal != 1))
            or np.any(terminal[..., 0] != terminal[..., 1])):
        raise ValueError("account-only terminal events require explicit cash settlement")
    updates = {}
    for field in fields(daily):
        value = getattr(daily, field.name)
        if isinstance(value, np.ndarray) and value.ndim and len(value) == len(calendar.dates):
            updates[field.name] = value[calendar.decision_indices]
    updates.update(futures_account_calendar=calendar, account_only_integer_execution=events)
    return replace(panel, stock_context_futures_portfolio_daily=replace(daily, **updates))
