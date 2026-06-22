from __future__ import annotations

import math
from typing import Any

from stockagent.live.time_display import DEFAULT_DISPLAY_TIMEZONE, display_timezone_label, format_display_time


INVESTMENT_WARNING = (
    "投資警語：本訊號為量化模型依歷史與當前資料產生之研究/輔助資訊，"
    "不構成投資建議或收益保證；下單前請自行確認價格、流動性、交易成本與風險。"
)


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


def _float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _fmt_money(value: Any) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    sign = "+" if number > 0 else ""
    return f"{sign}{number:,.0f}"


def _fmt_capital(value: Any) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    return f"{number:,.0f}"


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


def _source_timezone(summary: dict[str, Any]) -> str:
    return str(summary.get("data_timezone") or summary.get("timezone") or summary.get("display_timezone") or DEFAULT_DISPLAY_TIMEZONE)


def _display_timezone(summary: dict[str, Any]) -> str:
    return str(summary.get("display_timezone") or DEFAULT_DISPLAY_TIMEZONE)


def _fmt_time(value: Any, summary: dict[str, Any]) -> str:
    return format_display_time(
        value,
        source_timezone=_source_timezone(summary),
        display_timezone=_display_timezone(summary),
    )


def _fmt_tz_label(summary: dict[str, Any]) -> str:
    return str(summary.get("display_timezone_label") or display_timezone_label(_display_timezone(summary)))


def _period_title(summary: dict[str, Any]) -> str:
    start = summary.get("previous_weights_date") or summary.get("drift_base_date")
    end = summary.get("asof_date") or summary.get("panel_date")
    if start and end:
        return f"**上個訊號到現在** `{_fmt_time(start, summary)}`..`{_fmt_time(end, summary)}`"
    return "**上個訊號到現在**"


def format_signal_message(summary: dict[str, Any], *, max_rows: int = 12) -> str:
    """Build a Discord-sized Traditional Chinese live signal message."""
    rebalance = list(summary.get("rebalance", []))[: max(0, int(max_rows))]
    top_positions = list(summary.get("top_positions", []))[: max(0, int(max_rows))]
    warnings = list(summary.get("risk_warnings", []))
    target_risk = summary.get("target_risk", {}) if isinstance(summary.get("target_risk"), dict) else {}
    recent = summary.get("recent_performance", {}) if isinstance(summary.get("recent_performance"), dict) else {}
    explanation = summary.get("model_explanation", {}) if isinstance(summary.get("model_explanation"), dict) else {}
    portfolio_return = _float_or_none(summary.get("portfolio_simple_return"))
    baseline_return = _float_or_none(summary.get("benchmark_simple_return"))
    period_excess = None if portfolio_return is None or baseline_return is None else portfolio_return - baseline_return

    market_label = str(summary.get("market_label") or summary.get("market") or "").strip()
    title = "**stockAgent live signal**"
    if market_label:
        title += f" {market_label}"
    lines = [
        f"{title}",
        f"`{_fmt_time(summary.get('asof_date', 'latest'), summary)}`  `tz={_fmt_tz_label(summary)}`",
        _kv_line(
            ("panel", _fmt_time(summary.get("panel_date", "n/a"), summary)),
            ("fold", summary.get("fold_id", "auto")),
            ("signal", summary.get("signal_id", "n/a")),
        ),
        _kv_line(
            ("price", summary.get("price_source", "panel")),
            ("checkpoint", summary.get("checkpoint_fingerprint", "n/a")),
            ("config", summary.get("config_fingerprint", "n/a")),
        ),
        "",
        _period_title(summary),
        _kv_line(
            ("portfolio", _fmt_signed_pct(portfolio_return)),
            ("baseline", _fmt_signed_pct(baseline_return)),
            ("excess", _fmt_signed_pct(period_excess)),
            ("turnover", _fmt_pct(summary.get("turnover"), 2)),
            ("fees", _fmt_pct(summary.get("estimated_trade_cost"), 3)),
        ),
    ]
    if _float_or_none(summary.get("portfolio_pnl_value")) is not None:
        lines.append(
            _kv_line(
                ("capital", _fmt_capital(summary.get("display_capital"))),
                ("pnl", _fmt_money(summary.get("portfolio_pnl_value"))),
                ("baseline_pnl", _fmt_money(summary.get("benchmark_pnl_value"))),
                ("excess_pnl", _fmt_money(summary.get("excess_pnl_value"))),
            )
        )
    lines.extend(
        [
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
    )

    if recent:
        recent_label = str(recent.get("window_label") or f"過去{recent.get('window_days', 'n')}期/天")
        lines.extend(
            [
                "",
                f"**{recent_label}**",
                _kv_line(
                    ("strategy", _fmt_signed_pct(recent.get("strategy_return"))),
                    ("baseline", _fmt_signed_pct(recent.get("benchmark_return"))),
                    ("excess", _fmt_signed_pct(recent.get("excess_return"))),
                ),
            ]
        )
        if _float_or_none(recent.get("strategy_pnl_value")) is not None:
            lines.append(
                _kv_line(
                    ("capital", _fmt_capital(summary.get("display_capital"))),
                    ("pnl", _fmt_money(recent.get("strategy_pnl_value"))),
                    ("baseline_pnl", _fmt_money(recent.get("benchmark_pnl_value"))),
                    ("excess_pnl", _fmt_money(recent.get("excess_pnl_value"))),
                )
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

    lines.extend(["", INVESTMENT_WARNING])

    message = "\n".join(lines)
    max_message_chars = 1850
    if len(message) <= max_message_chars:
        return message
    suffix = "\n...\n" + INVESTMENT_WARNING
    return message[: max(0, max_message_chars - len(suffix))].rstrip() + suffix
