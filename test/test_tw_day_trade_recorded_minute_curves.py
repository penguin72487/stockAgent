from datetime import date, datetime, timedelta
import json
from zoneinfo import ZoneInfo

from scripts import audit_tw_day_trade_recorded_minute_curves as audit


def _marks(tmp_path):
    path = tmp_path / "marks.jsonl"
    start = datetime(2026, 10, 28, 9, 1, tzinfo=ZoneInfo("Asia/Taipei"))
    rows = [{"session_date": "2026-10-28", "market": "m",
             "minute": (start + timedelta(minutes=i)).isoformat(timespec="minutes"),
             "initial_capital_twd": 1000.0, "cumulative_realized_net_pnl_twd": 10.0,
             "open_net_liquidation_pnl_twd": 5.0, "cumulative_corporate_action_net_twd": 2.0,
             "cumulative_carry_cost_twd": 1.0, "total_equity_twd": 1016.0,
             "missing_price_position_count": 0, "valuation_stale": False} for i in range(270)]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return path, rows


def test_recorded_curve_checks_all_minutes_and_endpoint_without_execution_claim(tmp_path):
    path, _rows = _marks(tmp_path)
    result = audit.inspect_marks(day=date(2026, 10, 28), modes={"m": {"total_equity_twd": 1016.0}}, path=path)
    mode = result["modes"]["m"]
    assert mode["minute_count"] == 270 and not mode["missing_minutes"]
    assert not mode["invalid_equity_arithmetic_minutes"] and mode["terminal_equity_matches_snapshot"]
    assert not result["source_market_price_correctness_verified"]
    assert not result["signal_replay_or_broker_fills_verified"]


def test_recorded_curve_exposes_gaps_duplicate_and_bad_equity(tmp_path):
    path, rows = _marks(tmp_path)
    rows.pop(1)
    rows[-1]["total_equity_twd"] = 1017.0
    rows.append(rows[0])
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    result = audit.inspect_marks(day=date(2026, 10, 28), modes={"m": {"total_equity_twd": 1016.0}}, path=path)
    mode = result["modes"]["m"]
    assert mode["minute_count"] == 269 and len(mode["missing_minutes"]) == 1
    assert mode["duplicate_minute_appends"] == 1
    assert len(mode["invalid_equity_arithmetic_minutes"]) == 1
    assert not mode["terminal_equity_matches_snapshot"]
