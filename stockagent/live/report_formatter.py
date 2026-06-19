from __future__ import annotations

import math
from typing import Any


def _fmt_pct(value: float | int | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except Exception:
        return "n/a"
    if not math.isfinite(number):
        return "n/a"
    return f"{number * 100:.{digits}f}%"


def _fmt_float(value: float | int | None, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except Exception:
        return "n/a"
    if not math.isfinite(number):
        return "n/a"
    return f"{number:.{digits}f}"


def _label(row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol", "") or "").strip()
    name = str(row.get("name", "") or "").strip()
    if name:
        return f"`{symbol}` {name}"
    return f"`{symbol}`"


def format_signal_message(summary: dict[str, Any], *, max_rows: int = 12) -> str:
    """Build a Discord-sized Traditional Chinese live signal message."""
    rebalance = list(summary.get("rebalance", []))[: max(0, int(max_rows))]
    top_positions = list(summary.get("top_positions", []))[: min(max(0, int(max_rows)), 8)]

    lines = [
        f"**stockAgent live signal** `{summary.get('asof_date', 'latest')}`",
        (
            f"panel=`{summary.get('panel_date', 'n/a')}` "
            f"fold=`{summary.get('fold_id', 'auto')}` "
            f"price=`{summary.get('price_source', 'panel')}`"
        ),
        (
            "今日估算: "
            f"portfolio={_fmt_pct(summary.get('portfolio_simple_return'))} "
            f"benchmark={_fmt_pct(summary.get('benchmark_simple_return'))} "
            f"turnover={_fmt_pct(summary.get('turnover'), 2)} "
            f"fees={_fmt_pct(summary.get('estimated_trade_cost'), 3)}"
        ),
    ]

    if top_positions:
        lines.append("")
        lines.append("目標持倉 Top:")
        for row in top_positions:
            lines.append(
                f"{_label(row)} {_fmt_pct(row.get('weight'))} "
                f"px={_fmt_float(row.get('current_price'), 2)}"
            )

    if rebalance:
        lines.append("")
        lines.append("調倉 Top:")
        for row in rebalance:
            delta = float(row.get("delta_weight", 0.0) or 0.0)
            side = "BUY/加多" if delta > 0 else "SELL/減碼"
            lines.append(
                f"{_label(row)} {side} "
                f"delta={_fmt_pct(delta)} "
                f"now={_fmt_pct(row.get('current_weight'))} "
                f"target={_fmt_pct(row.get('target_weight'))}"
            )

    message = "\n".join(lines)
    if len(message) <= 1900:
        return message
    return message[:1890].rstrip() + "\n..."
