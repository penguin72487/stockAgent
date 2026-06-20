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


def _fmt_path(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    return text


def format_signal_message(summary: dict[str, Any], *, max_rows: int = 12) -> str:
    """Build a Discord-sized Traditional Chinese live signal message."""
    rebalance = list(summary.get("rebalance", []))[: max(0, int(max_rows))]
    top_positions = list(summary.get("top_positions", []))[: min(max(0, int(max_rows)), 8)]
    warnings = list(summary.get("risk_warnings", []))
    target_risk = summary.get("target_risk", {}) if isinstance(summary.get("target_risk"), dict) else {}
    recent = summary.get("recent_performance", {}) if isinstance(summary.get("recent_performance"), dict) else {}
    explanation = summary.get("model_explanation", {}) if isinstance(summary.get("model_explanation"), dict) else {}

    market_label = str(summary.get("market_label") or summary.get("market") or "").strip()
    title = "**stockAgent live signal**"
    if market_label:
        title += f" {market_label}"
    lines = [
        f"{title} `{summary.get('asof_date', 'latest')}`",
        (
            f"panel=`{summary.get('panel_date', 'n/a')}` "
            f"fold=`{summary.get('fold_id', 'auto')}` "
            f"signal=`{summary.get('signal_id', 'n/a')}`"
        ),
        f"price=`{summary.get('price_source', 'panel')}` checkpoint=`{summary.get('checkpoint_fingerprint', 'n/a')}` config=`{summary.get('config_fingerprint', 'n/a')}`",
        (
            "今日估算: "
            f"portfolio={_fmt_pct(summary.get('portfolio_simple_return'))} "
            f"benchmark={_fmt_pct(summary.get('benchmark_simple_return'))} "
            f"turnover={_fmt_pct(summary.get('turnover'), 2)} "
            f"fees={_fmt_pct(summary.get('estimated_trade_cost'), 3)}"
        ),
        (
            "風險: "
            f"gross={_fmt_pct(target_risk.get('gross'))} "
            f"long={_fmt_pct(target_risk.get('long_gross'))} "
            f"short={_fmt_pct(target_risk.get('short_gross'))} "
            f"top={_fmt_pct(target_risk.get('top_abs_weight'))} "
            f"HHI={_fmt_float(target_risk.get('hhi'), 3)}"
        ),
    ]

    if recent:
        lines.append(
            f"近{recent.get('window_days', 'n')}日: "
            f"strategy={_fmt_pct(recent.get('strategy_return'))} "
            f"benchmark={_fmt_pct(recent.get('benchmark_return'))} "
            f"excess={_fmt_pct(recent.get('excess_return'))}"
        )

    notice = str(summary.get("market_notice") or "").strip()
    if notice:
        lines.append(f"notice: {notice}")

    if warnings:
        lines.append("warning: " + " | ".join(str(item) for item in warnings[:3]))

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
            side = str(row.get("action") or ("BUY" if delta > 0 else "SELL"))
            lines.append(
                f"{_label(row)} {side} "
                f"delta={_fmt_pct(delta)} "
                f"px={_fmt_float(row.get('trade_price', row.get('current_price')), 2)} "
                f"now={_fmt_pct(row.get('current_weight'))} "
                f"target={_fmt_pct(row.get('target_weight'))}"
            )

    score_drivers = list(explanation.get("top_score_drivers", []))[:3]
    feature_drivers = list(explanation.get("top_feature_drivers", []))[:3]
    if score_drivers or feature_drivers:
        lines.append("")
        lines.append(
            "模型摘要: "
            f"confidence_proxy={_fmt_float(explanation.get('confidence_proxy_score_std'), 3)}"
        )
        if score_drivers:
            lines.append(
                "score drivers: "
                + ", ".join(
                    f"{str(row.get('symbol'))}:{_fmt_float(row.get('score'), 3)}" for row in score_drivers
                )
            )
        if feature_drivers:
            lines.append(
                "feature drivers: "
                + ", ".join(
                    f"{str(row.get('feature'))}:{_fmt_float(row.get('weighted_abs_value'), 3)}"
                    for row in feature_drivers
                )
            )

    artifact = _fmt_path(summary.get("summary_path") or summary.get("output_dir"))
    weights_path = _fmt_path(summary.get("weights_path"))
    rebalance_path = _fmt_path(summary.get("rebalance_path"))
    if artifact or weights_path or rebalance_path:
        lines.append("")
        if artifact:
            lines.append(f"summary: `{artifact}`")
        if weights_path:
            lines.append(f"weights: `{weights_path}`")
        if rebalance_path:
            lines.append(f"rebalance: `{rebalance_path}`")

    message = "\n".join(lines)
    if len(message) <= 1900:
        return message
    return message[:1890].rstrip() + "\n..."
