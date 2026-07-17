from __future__ import annotations

import argparse
import contextlib
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.discord_bot import bot as bot_mod


def _time_call(fn: Callable[[], Any], *, repeats: int) -> dict[str, Any]:
    samples: list[float] = []
    last_result: Any = None
    error: str | None = None
    for _ in range(max(1, int(repeats))):
        started = time.perf_counter()
        try:
            last_result = fn()
        except Exception as exc:  # benchmark should keep scanning other commands
            error = f"{type(exc).__name__}: {exc}"
            samples.append(time.perf_counter() - started)
            break
        samples.append(time.perf_counter() - started)
    payload: dict[str, Any] = {
        "ok": error is None,
        "runs": len(samples),
        "seconds_min": min(samples) if samples else None,
        "seconds_median": statistics.median(samples) if samples else None,
        "seconds_max": max(samples) if samples else None,
        "error": error,
    }
    if isinstance(last_result, str):
        payload["chars"] = len(last_result)
        payload["pages"] = len(bot_mod._split_content_pages(last_result))
    elif isinstance(last_result, list):
        payload["items"] = len(last_result)
        if last_result and all(isinstance(item, str) for item in last_result):
            payload["chars"] = sum(len(item) for item in last_result)
    return payload


def _market_cfg(market: str):
    return bot_mod._resolve_market(market)


def _latest_pair(cfg):
    return bot_mod._latest_signal_or_raise(cfg)


@contextlib.contextmanager
def _temporary_bot_state():
    original_state = bot_mod.STATE_PATH
    original_audit = bot_mod.AUDIT_LOG_PATH
    with tempfile.TemporaryDirectory(prefix="stockagent-discord-bench-") as tmp:
        root = Path(tmp)
        bot_mod.STATE_PATH = root / "state.json"
        bot_mod.AUDIT_LOG_PATH = root / "audit_events.jsonl"
        try:
            yield
        finally:
            bot_mod.STATE_PATH = original_state
            bot_mod.AUDIT_LOG_PATH = original_audit


def _dummy_interaction() -> Any:
    permissions = SimpleNamespace(administrator=True)
    role = SimpleNamespace(id=1, name="trader")
    user = SimpleNamespace(id=123456, roles=[role], guild_permissions=permissions)
    return SimpleNamespace(user=user)


def _build_cases(market: str, *, include_signal_now: bool) -> dict[str, Callable[[], Any]]:
    cfg = _market_cfg(market)

    def latest_pair():
        return _latest_pair(cfg)

    def latest_message() -> str:
        summary_path, summary = latest_pair()
        return bot_mod._latest_signal_message(cfg, summary_path, summary, top_n=10)

    def signal_message() -> str:
        _, summary = latest_pair()
        signal_id = str(summary.get("signal_id") or "")
        found = bot_mod._find_signal_summary(signal_id)
        if found is None:
            raise FileNotFoundError(signal_id)
        path, found_summary = found
        risk = found_summary.get("target_risk", {}) if isinstance(found_summary.get("target_risk"), dict) else {}
        return "\n".join(
            [
                f"signal={found_summary.get('signal_id', signal_id)}",
                f"path={path}",
                f"gross={bot_mod._pct(risk.get('gross'))}",
            ]
        )

    def changes_pages() -> list[str]:
        summary_path, summary = latest_pair()
        return bot_mod._latest_changes_pages(cfg, summary_path, summary, limit=0, page_size=20)

    def performance_message() -> str:
        summary_path, summary = latest_pair()
        return bot_mod._performance_message(cfg, summary_path, summary, days=32)

    def risk_message() -> str:
        summary_path, summary = latest_pair()
        return bot_mod._risk_message(cfg, summary_path, summary, top_n=10)

    def positions_pages() -> list[str]:
        summary_path, summary = latest_pair()
        rows = bot_mod._latest_artifact_rows(summary, summary_path, "weights_path", "top_positions")
        rows = sorted(
            rows,
            key=lambda row: (
                bot_mod._row_abs(row, "target_weight"),
                bot_mod._row_abs(row, "delta_weight"),
                bot_mod._row_abs(row, "score"),
            ),
            reverse=True,
        )
        rows = [
            row
            for row in rows
            if bot_mod._row_abs(row, "target_weight") > 1e-9
            or bot_mod._row_abs(row, "current_weight") > 1e-9
            or bot_mod._row_abs(row, "delta_weight") > 1e-9
        ]
        return bot_mod._line_pages(
            title="target positions",
            rows=rows,
            formatter=bot_mod._position_line,
            page_size=20,
            header_lines=[],
        )

    def rebalance_pages() -> list[str]:
        summary_path, summary = latest_pair()
        rows = bot_mod._latest_artifact_rows(summary, summary_path, "rebalance_path", "rebalance")
        rows = bot_mod._sort_decision_rows(rows, "delta")
        return bot_mod._line_pages(
            title="rebalance",
            rows=rows,
            formatter=bot_mod._rebalance_line,
            page_size=20,
            header_lines=[],
        )

    def explain_signal_pages() -> list[str]:
        summary_path, summary = latest_pair()
        explain_path = bot_mod._summary_artifact_path(summary, "decision_explanation_path", summary_path)
        if explain_path is None or not explain_path.exists():
            raise FileNotFoundError("decision_explanation_path")
        rows = bot_mod._read_parquet_rows(explain_path)
        rows_all = bot_mod._sort_decision_rows(rows, "delta")
        rows_filtered = bot_mod._filter_decision_rows(rows_all, action="actionable", actionable_only=True)
        overview = bot_mod._decision_overview_page(
            summary=summary,
            summary_path=summary_path,
            explain_path=explain_path,
            rows_all=rows_all,
            rows_filtered=rows_filtered,
            symbol="",
            action="actionable",
            sort_by="delta",
        )
        return [
            overview,
            *bot_mod._line_pages(
                title="decision rows",
                rows=rows_filtered,
                formatter=bot_mod._decision_block,
                page_size=10,
            ),
        ]

    def stock_history_pages() -> list[str]:
        summary_path, summary = latest_pair()
        top_positions = summary.get("top_positions") if isinstance(summary.get("top_positions"), list) else []
        symbol = str((top_positions[0] if top_positions else {}).get("symbol") or "")
        if not symbol:
            rows = bot_mod._latest_artifact_rows(summary, summary_path, "weights_path", "top_positions")
            symbol = str((rows[0] if rows else {}).get("symbol") or "")
        result = bot_mod._load_stock_history_for_market(cfg, symbol, 32, True, None, None)
        label = result.symbol + (f" {result.name}" if result.name else "")
        return bot_mod._line_pages(
            title=f"stock history {label}",
            rows=result.rows,
            formatter=bot_mod._stock_history_block,
            page_size=10,
            header_lines=bot_mod._stock_history_header_lines(cfg, result),
        )

    def portfolio_history_pages() -> list[str]:
        result = bot_mod._load_portfolio_history_for_market(cfg, 32, 5, 0.0, None, None)
        return bot_mod._line_pages(
            title="portfolio history",
            rows=result.rows,
            formatter=bot_mod._portfolio_history_block,
            page_size=1,
            header_lines=bot_mod._portfolio_history_header_lines(cfg, result),
            min_page_size=1,
            default_page_size=1,
        )

    cases: dict[str, Callable[[], Any]] = {
        "guide": bot_mod._guide_message,
        "health": lambda: "\n".join(bot_mod._health_lines(market)),
        "markets": lambda: "\n".join(bot_mod._markets_lines()),
        "latest": latest_message,
        "changes": changes_pages,
        "performance": performance_message,
        "risk": risk_message,
        "positions": positions_pages,
        "rebalance": rebalance_pages,
        "signal": signal_message,
        "explain_signal": explain_signal_pages,
        "stock_history": stock_history_pages,
        "portfolio_history": portfolio_history_pages,
        "daily_summary": lambda: bot_mod._daily_summary_message(cfg),
        "watch_add_list": lambda: _benchmark_watch(cfg),
        "subscribe_add_list": lambda: _benchmark_subscribe(cfg),
        "set_market_enabled": lambda: _benchmark_set_market_enabled(cfg),
        "set_schedule": lambda: _benchmark_set_schedule(cfg),
        "set_capital": lambda: _benchmark_set_capital(cfg),
    }
    if include_signal_now:
        def signal_now_no_write() -> str:
            kwargs = bot_mod._signal_kwargs(
                market=market,
                top_n=10,
                price_source="panel",
                progress_callback=None,
                progress_label=f"benchmark:{market}",
            )
            kwargs["write"] = False
            return bot_mod.generate_live_signal(**kwargs).message

        cases["signal_now_no_write"] = signal_now_no_write
    return cases


def _benchmark_watch(cfg) -> str:
    with _temporary_bot_state():
        items = bot_mod._add_user_watch_symbol(123456, cfg.market, "BENCH")
        items = bot_mod._remove_user_watch_symbol(123456, cfg.market, "BENCH")
        return ", ".join(items) or "(empty)"


def _benchmark_subscribe(cfg) -> str:
    with _temporary_bot_state():
        bot_mod._set_user_subscription(123456, cfg.market, watchlist_only=True)
        lines = bot_mod._subscription_summary_lines(123456)
        bot_mod._remove_user_subscription(123456, cfg.market)
        return "\n".join(lines)


def _benchmark_set_market_enabled(cfg) -> str:
    with _temporary_bot_state():
        bot_mod._set_market_state(cfg.market, enabled=True)
        bot_mod._record_audit_event(f"market:{cfg.market}", "set_market_enabled", _dummy_interaction(), market=cfg.market)
        return f"{cfg.market}=enabled"


def _benchmark_set_schedule(cfg) -> str:
    with _temporary_bot_state():
        normalized = bot_mod._validate_hhmm(bot_mod._market_schedule_time(cfg))
        bot_mod._set_market_state(cfg.market, schedule_time=normalized)
        bot_mod._record_audit_event(f"market:{cfg.market}", "set_schedule", _dummy_interaction(), market=cfg.market)
        return normalized


def _benchmark_set_capital(cfg) -> str:
    with _temporary_bot_state():
        bot_mod._set_market_state(cfg.market, initial_capital=1000000.0, current_capital=1200000.0)
        bot_mod._record_audit_event(f"market:{cfg.market}", "set_capital", _dummy_interaction(), market=cfg.market)
        return bot_mod._capital_context_text(
            initial_capital=bot_mod._market_initial_capital(cfg),
            current_capital=bot_mod._market_current_capital(cfg),
        )


def _print_results(market: str, repeats: int, results: dict[str, Any]) -> None:
    print(f"discord bot command benchmark market={market} repeats={repeats}")
    for name, item in sorted(results.items(), key=lambda pair: (pair[1].get("seconds_median") or 0.0), reverse=True):
        status = "ok" if item["ok"] else "ERR"
        median = item.get("seconds_median")
        text = f"{median:.4f}s" if isinstance(median, float) else "n/a"
        size = ""
        if "pages" in item:
            size = f" pages={item['pages']} chars={item.get('chars', 'n/a')}"
        elif "items" in item:
            size = f" items={item['items']}"
        err = f" error={item['error']}" if item.get("error") else ""
        print(f"{name:22s} {status:3s} median={text} min={item.get('seconds_min'):.4f}s max={item.get('seconds_max'):.4f}s{size}{err}")


def _benchmark_market(market: str, *, repeats: int, include_signal_now: bool) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for name, fn in _build_cases(market, include_signal_now=include_signal_now).items():
        results[name] = _time_call(fn, repeats=repeats)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Discord bot command helper paths without logging in.")
    parser.add_argument("--market", default="tw", help="Market id to benchmark, or all.")
    parser.add_argument("--all-markets", action="store_true", help="Benchmark every configured market.")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--include-signal-now", action="store_true", help="Also run model inference.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    markets = sorted(bot_mod._market_configs()) if args.all_markets or str(args.market).lower() == "all" else [args.market]
    all_results: dict[str, Any] = {
        market: _benchmark_market(market, repeats=args.repeats, include_signal_now=args.include_signal_now)
        for market in markets
    }

    if args.json:
        print(json.dumps(all_results if len(markets) > 1 else all_results[markets[0]], indent=2, ensure_ascii=False))
        return
    for index, market in enumerate(markets):
        if index:
            print()
        _print_results(market, args.repeats, all_results[market])


if __name__ == "__main__":
    main()
