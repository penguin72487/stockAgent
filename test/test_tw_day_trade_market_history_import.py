from __future__ import annotations

from datetime import datetime
from io import BytesIO
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts.import_tw_day_trade_market_history import (
    _apply_equity_carry,
    _filter_ledger,
    _verify_equity_continuity,
)
from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine


TAIPEI = ZoneInfo("Asia/Taipei")


def test_history_filter_selects_only_requested_market_and_completed_dates(
    tmp_path: Path,
) -> None:
    source = tmp_path / "marks.jsonl"
    rows = [
        {
            "market": "tw_day_trade_multi_basis_22",
            "session_date": "2026-02-25",
            "minute": "2026-02-25T09:01+08:00",
        },
        {
            "market": "tw_day_trade_multi_basis_22",
            "session_date": "2026-09-02",
            "minute": "2026-09-02T09:01+08:00",
        },
        {
            "market": "tw_day_trade_multi_basis",
            "session_date": "2026-02-25",
            "minute": "2026-02-25T09:01+08:00",
        },
    ]
    source.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    output = BytesIO()

    stats = _filter_ledger(
        source,
        output,
        market="tw_day_trade_multi_basis_22",
        dates=frozenset({"2026-02-25"}),
    )

    selected = [json.loads(line) for line in output.getvalue().splitlines()]
    assert stats["rows"] == 1
    assert stats["rows_by_session"] == {"2026-02-25": 1}
    assert selected == [rows[0]]


def test_equity_continuity_uses_prior_close_plus_current_open_pnl(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    live = tmp_path / "live"
    candidate.mkdir()
    live.mkdir()
    market = "tw_day_trade_multi_basis_22"
    (candidate / "marks.jsonl").write_text(
        json.dumps(
            {
                "market": market,
                "session_date": "2026-09-01",
                "minute": "2026-09-01T13:30+08:00",
                "initial_capital_twd": 10_000_000.0,
                "cumulative_realized_net_pnl_twd": 4_700_000.0,
                "open_net_liquidation_pnl_twd": 0.0,
                "total_equity_twd": 14_700_000.0,
                "open_position_count": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (live / "marks.jsonl").write_text(
        json.dumps(
            {
                "market": market,
                "session_date": "2026-09-02",
                "minute": "2026-09-02T09:01+08:00",
                "cumulative_realized_net_pnl_twd": 4_700_000.0,
                "open_net_liquidation_pnl_twd": -1_000.0,
                "total_equity_twd": 14_699_000.0,
                "equity_carry_source_session": "2026-09-01",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (live / "state.json").write_text(
        json.dumps(
            {
                "modes": {
                    market: {
                        "session_date": "2026-09-02",
                        "initial_capital_twd": 10_000_000.0,
                        "cumulative_realized_net_pnl_twd": 4_700_000.0,
                        "open_net_liquidation_pnl_twd": 2_000.0,
                        "total_equity_twd": 14_702_000.0,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    result = _verify_equity_continuity(
        candidate=candidate,
        live=live,
        market=market,
        end_date="2026-09-01",
    )

    assert result["status"] == "passed"
    assert result["continuity_residual_twd"] == 0.0
    assert result["state_reconciliation_residual_twd"] == 0.0


def test_apply_equity_carry_corrects_current_marks_and_is_idempotent(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    live = tmp_path / "live"
    candidate.mkdir()
    market = "tw_day_trade_multi_basis_22"
    (candidate / "marks.jsonl").write_text(
        json.dumps(
            {
                "market": market,
                "session_date": "2026-09-01",
                "minute": "2026-09-01T13:30+08:00",
                "initial_capital_twd": 10_000_000.0,
                "cumulative_realized_net_pnl_twd": 4_700_000.0,
                "open_net_liquidation_pnl_twd": 0.0,
                "total_equity_twd": 14_700_000.0,
                "open_position_count": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    engine = TwDayTradeSimulationEngine(live)
    engine.state["modes"] = {
        market: {
            "market": market,
            "session_date": "2026-09-02",
            "initial_capital_twd": 10_000_000.0,
            "cumulative_realized_net_pnl_twd": 0.0,
            "cumulative_commission_rebate_accrued_twd": 100.0,
            "open_net_liquidation_pnl_twd": -1_000.0,
            "total_equity_twd": 9_999_000.0,
        }
    }
    engine._persist(datetime(2026, 9, 2, 9, 2, tzinfo=TAIPEI))
    engine.marks_path.write_text(
        json.dumps(
            {
                "market": market,
                "session_date": "2026-09-02",
                "minute": "2026-09-02T09:01+08:00",
                "initial_capital_twd": 10_000_000.0,
                "cumulative_realized_net_pnl_twd": 0.0,
                "open_net_liquidation_pnl_twd": -1_000.0,
                "total_equity_twd": 9_999_000.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = _apply_equity_carry(
        candidate=candidate,
        live=live,
        market=market,
        end_date="2026-09-01",
        historical_commission_rebate_twd=1_500.0,
    )

    state = json.loads((live / "state.json").read_text(encoding="utf-8"))
    mode = state["modes"][market]
    assert result["status"] == "applied"
    assert result["corrected_current_session_minutes"] == 1
    assert mode["cumulative_realized_net_pnl_twd"] == 4_700_000.0
    assert mode["cumulative_commission_rebate_accrued_twd"] == 1_600.0
    assert mode["total_equity_twd"] == 14_699_000.0
    marks = [
        json.loads(line)
        for line in (live / "marks.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert len(marks) == 2
    assert marks[-1]["total_equity_twd"] == 14_699_000.0
    assert marks[-1]["supersedes_same_market_minute"] is True

    repeated = _apply_equity_carry(
        candidate=candidate,
        live=live,
        market=market,
        end_date="2026-09-01",
        historical_commission_rebate_twd=1_500.0,
    )

    assert repeated["status"] == "already_applied"
    assert len((live / "marks.jsonl").read_text(encoding="utf-8").splitlines()) == 2
