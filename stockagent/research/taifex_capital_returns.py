"""Capital-normalized return accounting for TAIFEX one-lot tick studies.

The strategy simulators remain the source of truth for executions and P&L.  This
module only replays their immutable trade ledger to estimate the cash/margin
base needed to support each intraday variant, then expresses the existing daily
P&L on that common capital basis.
"""

from __future__ import annotations

from datetime import date
import math
from typing import Final, Mapping

import numpy as np
import polars as pl


TRADING_DAYS_PER_YEAR: Final[float] = 252.0

# TAIFEX announcement dated 2026-06-17, effective after the 2026-06-18 day
# session.  The benchmark sample starts on 2026-06-25.  The schedule was checked
# against all subsequent TAIFEX margin announcements through the sample end.
TAIFEX_INITIAL_MARGIN_TWD: Final[dict[str, float]] = {
    "TX": 636_000.0,
    "MTX": 159_000.0,
    "TMF": 31_800.0,
}
TAIFEX_MARGIN_FIRST_TRADING_DATE: Final[date] = date(2026, 6, 19)
TAIFEX_MARGIN_VERIFIED_THROUGH: Final[date] = date(2026, 8, 6)
TAIFEX_MARGIN_ANNOUNCEMENT_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/11/newsDetail?idx=16991&newsType=1"
)
TAIFEX_MARGIN_PDF_URL: Final[str] = (
    "https://www.taifex.com.tw/file/taifex/eng/eng11/"
    "%E6%96%B0%E8%81%9E%E7%A8%BF_ENG_20260616%281%29.pdf"
)
TAIFEX_MARGIN_CSV_URL: Final[str] = (
    "https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/"
    "%E4%BF%9D%E8%AD%89%E9%87%91%E8%AA%BF%E6%95%B4%E5%88%97%E8%A1%A820260616.csv"
)
TAIFEX_MARGIN_CSV_SHA256: Final[str] = (
    "67c48684e1df9df834ef8a57379916d38e522da123ef9bd24755d2e1113e91af"
)


def _require_columns(frame: pl.DataFrame, required: set[str], *, label: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def compute_daily_required_capital(
    trades: pl.DataFrame,
    *,
    futures_initial_margin_twd: Mapping[str, float] = TAIFEX_INITIAL_MARGIN_TWD,
    margin_first_trading_date: date = TAIFEX_MARGIN_FIRST_TRADING_DATE,
    margin_verified_through: date = TAIFEX_MARGIN_VERIFIED_THROUGH,
) -> pl.DataFrame:
    """Replay the trade ledger and return each variant/day's peak funding need.

    Funding is intentionally limited to observed requirements, without stress
    add-ons:

    * long options: peak cumulative net premium cash debit, including fees/taxes;
    * futures: simultaneous absolute contracts times official initial margin;
    * mixed gamma strategies: both components at the same event time.

    Trades sharing a whole-second ``fill_ts`` are applied as one atomic event
    because the historical source does not provide a unique within-second order.
    The function rejects naked option shorts and non-flat session endings.
    """

    required = {
        "trading_date",
        "variant_id",
        "benchmark_family",
        "fill_ts",
        "instrument_type",
        "product",
        "series",
        "strike",
        "option_right",
        "delta_contracts",
        "gross_cash_flow_twd",
        "fixed_fee_twd",
    }
    _require_columns(trades, required, label="trade ledger")
    if trades.is_empty():
        raise ValueError("trade ledger is empty")
    if "transaction_tax_twd" not in trades.columns:
        trades = trades.with_columns(
            pl.lit(0.0, dtype=pl.Float64).alias("transaction_tax_twd")
        )

    dates = trades.get_column("trading_date")
    minimum_date = dates.min()
    maximum_date = dates.max()
    if minimum_date is None or maximum_date is None:
        raise ValueError("trade ledger has no valid trading dates")
    has_futures = bool(
        trades.filter(
            pl.col("instrument_type").cast(pl.Utf8).str.to_lowercase() == "future"
        ).height
    )
    if has_futures and (
        minimum_date < margin_first_trading_date
        or maximum_date > margin_verified_through
    ):
        raise ValueError(
            "official futures initial-margin schedule is not verified for the "
            f"full ledger window: ledger={minimum_date}..{maximum_date}, "
            f"verified={margin_first_trading_date}..{margin_verified_through}"
        )

    normalized_margins = {
        str(product).strip().upper(): float(value)
        for product, value in futures_initial_margin_twd.items()
    }
    if any(not math.isfinite(value) or value <= 0.0 for value in normalized_margins.values()):
        raise ValueError("futures initial margins must be finite and positive")

    ordered = trades.with_row_index("_ledger_order").sort(
        ["trading_date", "variant_id", "fill_ts", "_ledger_order"]
    )
    output: list[dict[str, object]] = []
    for session in ordered.partition_by(
        ["trading_date", "variant_id"], maintain_order=True
    ):
        trading_date = session.item(0, "trading_date")
        variant_id = str(session.item(0, "variant_id"))
        benchmark_family = str(session.item(0, "benchmark_family"))
        option_cash_balance = 0.0
        cumulative_futures_fees = 0.0
        cumulative_futures_taxes = 0.0
        option_positions: dict[tuple[str, float, str], int] = {}
        futures_positions: dict[str, int] = {}
        peak_required = 0.0
        peak_option_cash = 0.0
        peak_futures_margin = 0.0
        peak_futures_fees = 0.0
        peak_futures_taxes = 0.0
        peak_fill_ts = None

        for event in session.partition_by("fill_ts", maintain_order=True):
            fill_ts = event.item(0, "fill_ts")
            for row in event.iter_rows(named=True):
                instrument_type = str(row["instrument_type"]).strip().lower()
                delta_contracts = int(row["delta_contracts"])
                fixed_fee = float(row["fixed_fee_twd"])
                transaction_tax = float(row["transaction_tax_twd"])
                if delta_contracts == 0:
                    raise ValueError(
                        f"zero-contract trade: date={trading_date}, variant={variant_id}"
                    )
                if not math.isfinite(fixed_fee) or fixed_fee < 0.0:
                    raise ValueError(
                        f"invalid fixed fee: date={trading_date}, variant={variant_id}"
                    )
                if not math.isfinite(transaction_tax) or transaction_tax < 0.0:
                    raise ValueError(
                        "invalid transaction tax: "
                        f"date={trading_date}, variant={variant_id}"
                    )

                if instrument_type == "option":
                    gross_cash_flow = float(row["gross_cash_flow_twd"])
                    if not math.isfinite(gross_cash_flow):
                        raise ValueError(
                            f"non-finite option cash flow: date={trading_date}, "
                            f"variant={variant_id}"
                        )
                    option_cash_balance += (
                        gross_cash_flow - fixed_fee - transaction_tax
                    )
                    key = (
                        str(row["series"]),
                        float(row["strike"]),
                        str(row["option_right"]),
                    )
                    option_positions[key] = option_positions.get(key, 0) + delta_contracts
                elif instrument_type == "future":
                    product = str(row["product"]).strip().upper()
                    if product not in normalized_margins:
                        raise ValueError(
                            f"missing initial margin for futures product={product}"
                        )
                    cumulative_futures_fees += fixed_fee
                    cumulative_futures_taxes += transaction_tax
                    futures_positions[product] = (
                        futures_positions.get(product, 0) + delta_contracts
                    )
                else:
                    raise ValueError(
                        f"unsupported instrument_type={instrument_type!r}: "
                        f"date={trading_date}, variant={variant_id}"
                    )

            negative_options = {
                key: quantity
                for key, quantity in option_positions.items()
                if quantity < 0
            }
            if negative_options:
                raise ValueError(
                    "capital replay found a naked short option after an atomic "
                    f"event: date={trading_date}, variant={variant_id}, "
                    f"positions={negative_options}"
                )

            option_cash_requirement = max(-option_cash_balance, 0.0)
            futures_margin = sum(
                abs(quantity) * normalized_margins[product]
                for product, quantity in futures_positions.items()
            )
            total_required = (
                option_cash_requirement
                + futures_margin
                + cumulative_futures_fees
                + cumulative_futures_taxes
            )
            if total_required > peak_required:
                peak_required = total_required
                peak_option_cash = option_cash_requirement
                peak_futures_margin = futures_margin
                peak_futures_fees = cumulative_futures_fees
                peak_futures_taxes = cumulative_futures_taxes
                peak_fill_ts = fill_ts

        open_options = {
            key: quantity for key, quantity in option_positions.items() if quantity != 0
        }
        open_futures = {
            key: quantity for key, quantity in futures_positions.items() if quantity != 0
        }
        if open_options or open_futures:
            raise ValueError(
                "capital replay did not finish flat: "
                f"date={trading_date}, variant={variant_id}, "
                f"options={open_options}, futures={open_futures}"
            )
        if not math.isfinite(peak_required) or peak_required <= 0.0:
            raise ValueError(
                f"non-positive capital requirement: date={trading_date}, "
                f"variant={variant_id}, required={peak_required}"
            )
        output.append(
            {
                "trading_date": trading_date,
                "benchmark_family": benchmark_family,
                "variant_id": variant_id,
                "daily_peak_required_capital_twd": peak_required,
                "peak_option_cash_requirement_twd": peak_option_cash,
                "peak_futures_initial_margin_twd": peak_futures_margin,
                "peak_cumulative_futures_fees_twd": peak_futures_fees,
                "peak_cumulative_futures_taxes_twd": peak_futures_taxes,
                "peak_required_capital_fill_ts": peak_fill_ts,
            }
        )

    result = pl.DataFrame(output).sort(["variant_id", "trading_date"])
    duplicate_count = result.select(
        pl.len() - pl.struct(["trading_date", "variant_id"]).n_unique()
    ).item()
    if duplicate_count != 0:
        raise ValueError("capital replay produced duplicate variant/day rows")
    return result


def build_capital_normalized_returns(
    daily: pl.DataFrame,
    trades: pl.DataFrame,
    *,
    futures_initial_margin_twd: Mapping[str, float] = TAIFEX_INITIAL_MARGIN_TWD,
    margin_first_trading_date: date = TAIFEX_MARGIN_FIRST_TRADING_DATE,
    margin_verified_through: date = TAIFEX_MARGIN_VERIFIED_THROUGH,
    periods_per_year: float = TRADING_DAYS_PER_YEAR,
    carry_across_sessions: bool = False,
    pnl_column: str = "net_after_fee_twd",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return daily normalized curves plus one summary row per variant.

    The fixed capital base is the variant's maximum observed daily requirement.
    ``cumulative_return_on_capital`` is the exact one-lot cumulative P&L divided
    by that fixed base.  ``cumulative_compounded_return`` is also emitted as a
    daily-rebalanced statistical view; it does not claim that the one-lot trade
    count was resized as equity changed.
    """

    if not math.isfinite(periods_per_year) or periods_per_year <= 0.0:
        raise ValueError("periods_per_year must be finite and positive")
    _require_columns(
        daily,
        {"trading_date", "benchmark_family", "variant_id", pnl_column},
        label="daily benchmark results",
    )
    selected_daily = daily.select(
        "trading_date",
        "benchmark_family",
        "variant_id",
        pl.col(pnl_column).alias("normalized_pnl_twd"),
    )
    if selected_daily.height != selected_daily.select(
        pl.struct(["trading_date", "variant_id"]).n_unique()
    ).item():
        raise ValueError("daily benchmark results contain duplicate variant/day rows")
    if carry_across_sessions:
        # Reuse the canonical ledger replay by collapsing only the grouping
        # date.  The original fill timestamps retain the exact event order,
        # while positions are allowed to remain open until the sample's final
        # official-expiry settlement.  This produces one fixed funding base per
        # variant without a second margin/cash accounting implementation.
        replay_date = trades.get_column("trading_date").max()
        if replay_date is None:
            raise ValueError("carry ledger has no trading dates")
        replay = trades.with_columns(
            pl.lit(replay_date).cast(pl.Date).alias("trading_date")
        )
        capital = compute_daily_required_capital(
            replay,
            futures_initial_margin_twd=futures_initial_margin_twd,
            margin_first_trading_date=margin_first_trading_date,
            margin_verified_through=margin_verified_through,
        ).drop("trading_date")
        joined = selected_daily.join(
            capital,
            on=["benchmark_family", "variant_id"],
            how="inner",
            validate="m:1",
        )
        if joined.height != selected_daily.height:
            raise ValueError(
                "carry daily results did not fully join fixed capital: "
                f"daily={selected_daily.height}, joined={joined.height}"
            )
    else:
        capital = compute_daily_required_capital(
            trades,
            futures_initial_margin_twd=futures_initial_margin_twd,
            margin_first_trading_date=margin_first_trading_date,
            margin_verified_through=margin_verified_through,
        )
        joined = selected_daily.join(
            capital,
            on=["trading_date", "benchmark_family", "variant_id"],
            how="inner",
            validate="1:1",
        )
        if joined.height != selected_daily.height or joined.height != capital.height:
            raise ValueError(
                "daily benchmark and capital replay rows do not form a complete 1:1 join: "
                f"daily={selected_daily.height}, capital={capital.height}, joined={joined.height}"
            )

    daily_rows: list[dict[str, object]] = []
    metric_rows: list[dict[str, object]] = []
    for variant in joined.sort(["variant_id", "trading_date"]).partition_by(
        "variant_id", maintain_order=True
    ):
        variant_id = str(variant.item(0, "variant_id"))
        family = str(variant.item(0, "benchmark_family"))
        capital_base = float(
            variant.get_column("daily_peak_required_capital_twd").max()
        )
        if not math.isfinite(capital_base) or capital_base <= 0.0:
            raise ValueError(f"invalid capital base for variant={variant_id}")
        pnl = variant.get_column("normalized_pnl_twd").to_numpy().astype(np.float64)
        returns = pnl / capital_base
        if not np.all(np.isfinite(returns)):
            raise ValueError(f"non-finite daily return for variant={variant_id}")
        if not carry_across_sessions and np.any(returns <= -1.0):
            worst = float(np.min(returns))
            raise ValueError(
                "daily loss equals or exceeds the fixed capital base: "
                f"variant={variant_id}, worst_daily_return={worst}"
            )

        cumulative_return = np.cumsum(returns)
        if carry_across_sessions:
            # A carried option can appreciate by more than its original cash
            # requirement and later give back more than one original-capital
            # unit in a single day without the account being insolvent.  Daily
            # fixed-denominator compounding is therefore undefined/misleading.
            # Keep the compatibility field equal to the actual fixed-capital
            # marked-equity return and expose its peak-to-trough drawdown below.
            cumulative_compounded = cumulative_return.copy()
            compounded_wealth = 1.0 + cumulative_return
        else:
            compounded_wealth = np.cumprod(1.0 + returns)
            cumulative_compounded = compounded_wealth - 1.0
        wealth_with_origin = np.concatenate(
            [np.asarray([1.0], dtype=np.float64), compounded_wealth]
        )
        running_peak = np.maximum.accumulate(wealth_with_origin)
        drawdowns = wealth_with_origin / running_peak - 1.0
        mean_daily = float(np.mean(returns))
        daily_std = float(np.std(returns, ddof=1)) if returns.size > 1 else 0.0
        annualized_sharpe = (
            mean_daily / daily_std * math.sqrt(periods_per_year)
            if daily_std > 0.0
            else 0.0
        )
        daily_requirements = variant.get_column(
            "daily_peak_required_capital_twd"
        ).to_numpy()
        metric_rows.append(
            {
                "benchmark_family": family,
                "variant_id": variant_id,
                "observations": int(returns.size),
                "trading_days": int(returns.size),
                "periods_per_year": float(periods_per_year),
                "capital_base_twd": capital_base,
                "total_net_after_fee_twd": float(np.sum(pnl)),
                "total_normalized_pnl_twd": float(np.sum(pnl)),
                "normalized_pnl_column": pnl_column,
                "return_path_method": (
                    "fixed_capital_marked_equity"
                    if carry_across_sessions
                    else "daily_fixed_denominator_compounding"
                ),
                "cumulative_return_on_capital": float(cumulative_return[-1]),
                "cumulative_compounded_return": float(cumulative_compounded[-1]),
                "annualized_sharpe": float(annualized_sharpe),
                "mean_daily_return": mean_daily,
                "daily_return_std": daily_std,
                "maximum_drawdown_compounded_return": float(np.min(drawdowns)),
                "mean_required_capital_twd": float(np.mean(daily_requirements)),
                "median_required_capital_twd": float(np.median(daily_requirements)),
                "mean_capital_utilization": float(
                    np.mean(daily_requirements / capital_base)
                ),
            }
        )

        for row, daily_return, cumulative, compounded in zip(
            variant.iter_rows(named=True),
            returns,
            cumulative_return,
            cumulative_compounded,
        ):
            row.pop("benchmark_family_right", None)
            row.update(
                {
                    "capital_base_twd": capital_base,
                    "daily_return_on_capital": float(daily_return),
                    "cumulative_return_on_capital": float(cumulative),
                    "cumulative_compounded_return": float(compounded),
                }
            )
            daily_rows.append(row)

        for index in range(len(daily_rows) - len(returns), len(daily_rows)):
            local = index - (len(daily_rows) - len(returns))
            daily_rows[index]["fixed_capital_drawdown_return"] = float(
                drawdowns[local + 1]
            )

    normalized_daily = pl.DataFrame(daily_rows).sort(["variant_id", "trading_date"])
    metrics = pl.DataFrame(metric_rows).sort(
        "cumulative_return_on_capital", descending=True
    )
    return normalized_daily, metrics
