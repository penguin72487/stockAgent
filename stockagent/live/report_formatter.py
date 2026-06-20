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


def _fmt_signed_pct(value: float | int | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except Exception:
        return "n/a"
    if not math.isfinite(number):
        return "n/a"
    return f"{number * 100:+.{digits}f}%"


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


def _kv_line(*pairs: tuple[str, Any]) -> str:
    return "  " + "  ".join(f"`{key}={value}`" for key, value in pairs)


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
        f"{title}",
        f"`{summary.get('asof_date', 'latest')}`",
        _kv_line(
            ("panel", summary.get("panel_date", "n/a")),
            ("fold", summary.get("fold_id", "auto")),
            ("signal", summary.get("signal_id", "n/a")),
        ),
        _kv_line(
            ("price", summary.get("price_source", "panel")),
            ("checkpoint", summary.get("checkpoint_fingerprint", "n/a")),
            ("config", summary.get("config_fingerprint", "n/a")),
        ),
        "",
        "**今日估算**",
        _kv_line(
            ("portfolio", _fmt_signed_pct(summary.get("portfolio_simple_return"))),
            ("benchmark", _fmt_signed_pct(summary.get("benchmark_simple_return"))),
            ("turnover", _fmt_pct(summary.get("turnover"), 2)),
            ("fees", _fmt_pct(summary.get("estimated_trade_cost"), 3)),
        ),
        "",
        "**風險**",
        _kv_line(
            ("gross", _fmt_pct(target_risk.get("gross"))),
            ("long", _fmt_pct(target_risk.get("long_gross"))),
            ("short", _fmt_pct(target_risk.get("short_gross"))),
        ),
        _kv_line(
            ("top", _fmt_pct(target_risk.get("top_abs_weight"))),
            ("HHI", _fmt_float(target_risk.get("hhi"), 3)),
        ),
    ]

    if recent:
        lines.extend(
            [
                "",
                f"**近{recent.get('window_days', 'n')}日**",
                _kv_line(
                    ("strategy", _fmt_signed_pct(recent.get("strategy_return"))),
                    ("benchmark", _fmt_signed_pct(recent.get("benchmark_return"))),
                    ("excess", _fmt_signed_pct(recent.get("excess_return"))),
                ),
            ]
        )

    notice = str(summary.get("market_notice") or "").strip()
    if notice:
        lines.append(f"notice: {notice}")

    if warnings:
        lines.append("warning: " + " | ".join(str(item) for item in warnings[:3]))

    if top_positions:
        lines.append("")
        lines.append("**目標持倉 Top**")
        for index, row in enumerate(top_positions, start=1):
            lines.append(f"{index}. {_label(row)}")
            lines.append(_kv_line(("weight", _fmt_pct(row.get("weight"))), ("px", _fmt_float(row.get("current_price"), 2))))

    if rebalance:
        lines.append("")
        lines.append("**調倉 Top**")
        for index, row in enumerate(rebalance, start=1):
            delta = float(row.get("delta_weight", 0.0) or 0.0)
            side = str(row.get("action") or ("BUY" if delta > 0 else "SELL"))
            lines.append(f"{index}. {_label(row)} **{side}**")
            lines.append(
                _kv_line(
                    ("delta", _fmt_signed_pct(delta)),
                    ("px", _fmt_float(row.get("trade_price", row.get("current_price")), 2)),
                    ("now", _fmt_pct(row.get("current_weight"))),
                    ("target", _fmt_pct(row.get("target_weight"))),
                )
            )

    score_drivers = list(explanation.get("top_score_drivers", []))[:3]
    feature_drivers = list(explanation.get("top_feature_drivers", []))[:3]
    if score_drivers or feature_drivers:
        lines.append("")
        lines.append("**模型摘要**")
        lines.append(_kv_line(("confidence_proxy", _fmt_float(explanation.get("confidence_proxy_score_std"), 3))))
        if score_drivers:
            lines.append("score drivers:")
            lines.extend(
                f"  {index}. {str(row.get('symbol'))}: score={_fmt_float(row.get('score'), 3)}"
                for index, row in enumerate(score_drivers, start=1)
            )
        if feature_drivers:
            lines.append("feature drivers:")
            lines.extend(
                f"  {index}. {str(row.get('feature'))}: value={_fmt_float(row.get('weighted_abs_value'), 3)}"
                for index, row in enumerate(feature_drivers, start=1)
            )

    artifact = _fmt_path(summary.get("summary_path") or summary.get("output_dir"))
    weights_path = _fmt_path(summary.get("weights_path"))
    rebalance_path = _fmt_path(summary.get("rebalance_path"))
    explain_path = _fmt_path(summary.get("decision_explanation_path"))
    if artifact or weights_path or rebalance_path or explain_path:
        lines.append("")
        lines.append("**files**")
        if artifact:
            lines.append(f"summary: `{artifact}`")
        if weights_path:
            lines.append(f"weights: `{weights_path}`")
        if rebalance_path:
            lines.append(f"rebalance: `{rebalance_path}`")
        if explain_path:
            lines.append(f"explain: `{explain_path}`")

    message = "\n".join(lines)
    if len(message) <= 1900:
        return message
    return message[:1890].rstrip() + "\n..."
