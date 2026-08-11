from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

import numpy as np
import polars as pl


REALIZED_HOLDINGS_SCHEMA_VERSION = 1


def build_realized_holdings_frames(
    dates: np.ndarray,
    symbols: Sequence[str],
    weights_history: np.ndarray,
    *,
    initial_capital: float | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build auditable long-form holdings and one-row-per-session summaries.

    ``weights_history`` must contain realised, not requested, portfolio weights.
    For ``tw_minute`` this is the actual filled inventory marked at the session
    close immediately before mandatory flattening.
    """

    date_values = np.asarray(dates, dtype="datetime64[D]")
    symbol_values = np.asarray([str(symbol) for symbol in symbols], dtype=object)
    weights = np.asarray(weights_history, dtype=np.float64)
    if date_values.ndim != 1 or date_values.size == 0:
        raise ValueError("realized holdings dates must be a non-empty vector")
    if np.any(date_values[1:] <= date_values[:-1]):
        raise ValueError("realized holdings dates must be strictly chronological")
    if symbol_values.ndim != 1 or symbol_values.size == 0:
        raise ValueError("realized holdings symbols must be a non-empty vector")
    if np.unique(symbol_values).size != symbol_values.size:
        raise ValueError("realized holdings symbols must be unique")
    if weights.shape != (date_values.size, symbol_values.size):
        raise ValueError(
            "realized holdings weights must have shape [dates,symbols]"
        )
    if not bool(np.isfinite(weights).all()):
        raise ValueError("realized holdings weights must be finite")
    if initial_capital is not None and (
        not np.isfinite(float(initial_capital)) or float(initial_capital) <= 0.0
    ):
        raise ValueError("initial_capital must be positive and finite")

    absolute = np.abs(weights)
    positive = np.clip(weights, 0.0, None)
    negative = np.clip(-weights, 0.0, None)
    gross = absolute.sum(axis=1)
    net = weights.sum(axis=1)
    long_gross = positive.sum(axis=1)
    short_gross = negative.sum(axis=1)
    active = absolute > 0.0
    long_count = (weights > 0.0).sum(axis=1)
    short_count = (weights < 0.0).sum(axis=1)
    active_count = active.sum(axis=1)
    squared_share = np.divide(
        absolute,
        gross[:, None],
        out=np.zeros_like(absolute),
        where=gross[:, None] > 0.0,
    )
    hhi = np.square(squared_share).sum(axis=1)
    effective_names = np.divide(
        1.0,
        hhi,
        out=np.zeros_like(hhi),
        where=hhi > 0.0,
    )
    sorted_absolute = np.sort(absolute, axis=1)

    def top_share(count: int) -> np.ndarray:
        top = sorted_absolute[:, -min(int(count), symbol_values.size) :].sum(axis=1)
        return np.divide(
            top,
            gross,
            out=np.zeros_like(top),
            where=gross > 0.0,
        )

    summary = pl.DataFrame(
        {
            "date": date_values,
            "gross_weight": gross,
            "net_weight": net,
            "long_gross_weight": long_gross,
            "short_gross_weight": short_gross,
            "active_positions": active_count.astype(np.int32),
            "long_positions": long_count.astype(np.int32),
            "short_positions": short_count.astype(np.int32),
            "top10_abs_weight_share": top_share(10),
            "top50_abs_weight_share": top_share(50),
            "holdings_hhi": hhi,
            "effective_names": effective_names,
        }
    )

    date_indices, symbol_indices = np.nonzero(active)
    held_weights = weights[date_indices, symbol_indices]
    ranks = np.empty(date_indices.size, dtype=np.int32)
    offset = 0
    for date_index in range(date_values.size):
        columns = np.flatnonzero(active[date_index])
        count = int(columns.size)
        if count == 0:
            continue
        order = np.argsort(-absolute[date_index, columns], kind="stable")
        local_ranks = np.empty(count, dtype=np.int32)
        local_ranks[order] = np.arange(1, count + 1, dtype=np.int32)
        ranks[offset : offset + count] = local_ranks
        offset += count
    holdings_data: dict[str, object] = {
        "date": date_values[date_indices],
        "snapshot": np.full(date_indices.size, "pre_mandatory_close", dtype=object),
        "symbol": symbol_values[symbol_indices],
        "side": np.where(held_weights > 0.0, "long", "short"),
        "weight": held_weights,
        "abs_weight": np.abs(held_weights),
        "gross_rank": ranks,
    }
    if initial_capital is not None:
        # This is deliberately named as a rebase, not actual notional. Actual
        # pre-close NAV differs after intraday P&L and fees.
        holdings_data["initial_capital_rebased_notional"] = (
            held_weights * float(initial_capital)
        )
    holdings = pl.DataFrame(holdings_data).sort(
        ["date", "gross_rank", "symbol"]
    )
    return holdings, summary


def _write_holdings_markdown(
    path: Path,
    holdings: pl.DataFrame,
    summary: pl.DataFrame,
    *,
    initial_capital: float | None,
) -> None:
    means = summary.select(
        pl.mean("gross_weight").alias("gross"),
        pl.mean("net_weight").alias("net"),
        pl.mean("long_gross_weight").alias("long"),
        pl.mean("short_gross_weight").alias("short"),
        pl.median("active_positions").alias("active"),
        pl.median("effective_names").alias("effective"),
        pl.mean("top10_abs_weight_share").alias("top10"),
        pl.mean("top50_abs_weight_share").alias("top50"),
    ).row(0, named=True)
    latest_date = summary["date"][-1]
    latest = holdings.filter(pl.col("date") == latest_date).head(20)
    zero_exposure = summary.filter(pl.col("gross_weight") == 0.0)["date"].to_list()
    trading_lines: list[str] = []
    if "turnover_multiple" in summary.columns:
        trading_lines.append(
            f"- Mean daily turnover: {float(summary['turnover_multiple'].mean()):.2f}x"
        )
    if "explicit_fee_fraction" in summary.columns:
        trading_lines.append(
            "- Mean daily explicit fees / initial capital: "
            f"{float(summary['explicit_fee_fraction'].mean()):.2%}"
        )
    if {"net_return", "explicit_fee_fraction"}.issubset(summary.columns):
        approximate_pre_fee = (
            summary["net_return"] + summary["explicit_fee_fraction"]
        ).mean()
        trading_lines.append(
            f"- Approximate mean pre-fee daily return: {float(approximate_pre_fee):+.2%}"
        )
    if "mean_model_requested_cash_weight" in summary.columns:
        trading_lines.extend(
            [
                "- Mean model-requested cash weight: "
                f"{float(summary['mean_model_requested_cash_weight'].mean()):.2%}",
                "- Mean model-requested risky gross weight: "
                f"{float(summary['mean_model_requested_risky_gross_weight'].mean()):.2%}",
                "- Latest model-requested cash weight: "
                f"{float(summary['last_model_requested_cash_weight'][-1]):.2%}",
            ]
        )
    lines = [
        "# Realized pre-close holdings",
        "",
        "These are actual filled portfolio weights marked immediately before "
        "the mandatory session-close flatten. Terminal overnight holdings are zero.",
        "",
        "## Portfolio profile",
        "",
        f"- Date range: {summary['date'][0]} to {latest_date}",
        f"- Mean gross weight: {float(means['gross']):.6f}",
        f"- Mean net weight: {float(means['net']):+.6f}",
        f"- Mean long gross: {float(means['long']):.6f}",
        f"- Mean short gross: {float(means['short']):.6f}",
        f"- Median active positions: {float(means['active']):.0f}",
        f"- Median effective names: {float(means['effective']):.1f}",
        f"- Mean Top-10 absolute share: {float(means['top10']):.2%}",
        f"- Mean Top-50 absolute share: {float(means['top50']):.2%}",
        f"- Zero-exposure sessions: {len(zero_exposure)}"
        + (f" ({', '.join(str(value) for value in zero_exposure[:8])})" if zero_exposure else ""),
        *trading_lines,
        "",
        f"## Latest snapshot: {latest_date}",
        "",
        "| Rank | Symbol | Side | Weight |",
        "|---:|:---|:---|---:|",
    ]
    for row in latest.iter_rows(named=True):
        lines.append(
            f"| {int(row['gross_rank'])} | {row['symbol']} | {row['side']} | "
            f"{float(row['weight']):+.4%} |"
        )
    if initial_capital is not None:
        lines.extend(
            [
                "",
                "`initial_capital_rebased_notional` is weight multiplied by the "
                f"configured initial capital ({float(initial_capital):,.0f}); it is "
                "not actual notional after intraday P&L and fees.",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _plot_holdings(
    output_dir: Path,
    holdings: pl.DataFrame,
    summary: pl.DataFrame,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    dates = summary["date"].to_numpy()
    long_gross = summary["long_gross_weight"].to_numpy()
    short_gross = summary["short_gross_weight"].to_numpy()
    net = summary["net_weight"].to_numpy()
    fig, (top, bottom) = plt.subplots(
        2,
        1,
        figsize=(12, 7),
        sharex=True,
        gridspec_kw={"height_ratios": [2.0, 1.0]},
    )
    top.plot(dates, long_gross, color="#1f5a94", linewidth=1.4, label="Long gross")
    top.plot(dates, net, color="#30343b", linewidth=1.0, label="Net")
    if "mean_model_requested_cash_weight" in summary.columns:
        top.plot(
            dates,
            summary["mean_model_requested_cash_weight"].to_numpy(),
            color="#2d8a58",
            linewidth=1.2,
            linestyle="--",
            label="Mean requested cash",
        )
    top.axhline(0.0, color="#8c9299", linewidth=0.8)
    top.yaxis.set_major_formatter(PercentFormatter(1.0))
    top.set_title("Realized pre-close portfolio exposure")
    top.set_ylabel("Portfolio weight")
    top.legend(frameon=False, ncols=3, loc="lower left")
    top.grid(axis="y", color="#d9dde2", linewidth=0.6)
    bottom.plot(
        dates,
        short_gross,
        color="#d47a2c",
        linewidth=1.3,
        label="Short gross",
    )
    bottom.yaxis.set_major_formatter(PercentFormatter(1.0))
    bottom.set_ylabel("Short weight")
    bottom.set_xlabel("Session")
    bottom.grid(axis="y", color="#d9dde2", linewidth=0.6)
    fig.text(
        0.01,
        0.01,
        "Actual filled inventory marked before mandatory close; overnight holdings are zero.",
        color="#555b63",
        fontsize=9,
    )
    fig.tight_layout(rect=(0.0, 0.04, 1.0, 1.0))
    fig.savefig(output_dir / "holdings_exposure.png", dpi=160)
    plt.close(fig)

    latest_date = summary["date"][-1]
    latest = holdings.filter(pl.col("date") == latest_date).head(20).sort(
        "weight"
    )
    values = latest["weight"].to_numpy()
    labels = latest["symbol"].to_list()
    colors = ["#1f5a94" if value >= 0.0 else "#d47a2c" for value in values]
    fig, axis = plt.subplots(figsize=(10, 8))
    bars = axis.barh(labels, values, color=colors, edgecolor="#30343b", linewidth=0.4)
    axis.axvline(0.0, color="#30343b", linewidth=0.9)
    axis.xaxis.set_major_formatter(PercentFormatter(1.0))
    axis.set_title(f"Largest realized pre-close holdings — {latest_date}")
    axis.set_xlabel("Portfolio weight")
    axis.grid(axis="x", color="#d9dde2", linewidth=0.6)
    padding = max(float(np.max(np.abs(values))) * 0.02, 1e-5)
    for bar, value in zip(bars, values):
        axis.text(
            value + (padding if value >= 0.0 else -padding),
            bar.get_y() + bar.get_height() / 2.0,
            f"{value:+.3%}",
            va="center",
            ha="left" if value >= 0.0 else "right",
            fontsize=8,
            color="#30343b",
        )
    axis.margins(x=0.16)
    fig.tight_layout()
    fig.savefig(output_dir / "latest_pre_close_holdings.png", dpi=160)
    plt.close(fig)


def save_realized_holdings_artifacts(
    output_dir: str | Path,
    dates: np.ndarray,
    symbols: Sequence[str],
    weights_history: np.ndarray,
    *,
    initial_capital: float | None = None,
    daily_performance: pl.DataFrame | None = None,
    write_plots: bool = True,
    source_artifact: str = "test_backtest.npz",
) -> dict[str, object]:
    resolved = Path(output_dir)
    resolved.mkdir(parents=True, exist_ok=True)
    holdings, summary = build_realized_holdings_frames(
        dates,
        symbols,
        weights_history,
        initial_capital=initial_capital,
    )
    if daily_performance is not None:
        performance = daily_performance
        if "date" not in performance.columns:
            raise ValueError("daily_performance must contain date")
        selected = [
            name
            for name in (
                "date",
                "net_return",
                "turnover_notional",
                "explicit_fees",
                "slippage_cost",
                "initial_equity",
                "mean_model_requested_cash_weight",
                "min_model_requested_cash_weight",
                "max_model_requested_cash_weight",
                "last_model_requested_cash_weight",
                "mean_model_requested_risky_gross_weight",
            )
            if name in performance.columns
        ]
        performance = performance.select(selected)
        date_expression = (
            pl.col("date").str.to_date(strict=True)
            if performance.schema["date"] == pl.String
            else pl.col("date").cast(pl.Date)
        )
        performance = performance.with_columns(date_expression)
        if performance["date"].n_unique() != performance.height:
            raise ValueError("daily_performance dates must be unique")
        summary = summary.join(performance, on="date", how="left", validate="1:1")
        if summary["net_return"].null_count() if "net_return" in summary.columns else 0:
            raise ValueError("daily_performance does not cover all holdings dates")
        if {"turnover_notional", "initial_equity"}.issubset(summary.columns):
            summary = summary.with_columns(
                (pl.col("turnover_notional") / pl.col("initial_equity"))
                .alias("turnover_multiple")
            )
        if {"explicit_fees", "initial_equity"}.issubset(summary.columns):
            summary = summary.with_columns(
                (pl.col("explicit_fees") / pl.col("initial_equity"))
                .alias("explicit_fee_fraction")
            )
    holdings.write_parquet(
        resolved / "pre_close_holdings.parquet",
        compression="zstd",
        statistics=True,
    )
    summary.write_parquet(
        resolved / "holdings_summary.parquet",
        compression="zstd",
        statistics=True,
    )
    _write_holdings_markdown(
        resolved / "holdings_summary.md",
        holdings,
        summary,
        initial_capital=initial_capital,
    )
    if write_plots:
        _plot_holdings(resolved, holdings, summary)
    contract: dict[str, object] = {
        "schema_version": REALIZED_HOLDINGS_SCHEMA_VERSION,
        "source_artifact": str(source_artifact),
        "row_grain": "session_symbol_nonzero_realized_weight",
        "snapshot": "immediately_before_mandatory_session_close",
        "terminal_overnight_holdings": "zero",
        "weight_definition": "actual_shares_times_close_mark_divided_by_pre_close_nav",
        "dates": int(summary.height),
        "symbols": int(len(symbols)),
        "nonzero_holding_rows": int(holdings.height),
        "zero_exposure_sessions": int(
            summary.filter(pl.col("gross_weight") == 0.0).height
        ),
        "initial_capital": (
            None if initial_capital is None else float(initial_capital)
        ),
        "model_requested_cash_available": bool(
            "mean_model_requested_cash_weight" in summary.columns
        ),
        "model_requested_cash_definition": (
            "explicit_cash_asset_weight_before_execution_constraints"
            if "mean_model_requested_cash_weight" in summary.columns
            else None
        ),
    }
    (resolved / "holdings_contract.json").write_text(
        json.dumps(contract, indent=2), encoding="utf-8"
    )
    return contract


__all__ = [
    "REALIZED_HOLDINGS_SCHEMA_VERSION",
    "build_realized_holdings_frames",
    "save_realized_holdings_artifacts",
]
