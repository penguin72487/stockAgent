from __future__ import annotations

from dataclasses import replace
from datetime import date
import json
from pathlib import Path

from scripts.complete_tw_day_trade_paper_entry import CONTRACT, apply_plan, make_plan
from stockagent.live.tw_day_trade_dashboard import (
    build_dashboard_position_page,
    build_dashboard_signal_page,
)
from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
from test_tw_day_trade_simulation import _eligibility, _now, _quote, _row, _spec, _summary


def test_manual_same_price_completion_appends_disclosed_fill_and_updates_state(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(root)
    quote = _quote(open_price=500.0, ask=1_000.0, ask_volume=1.0, minute_volume_lots=None)
    quote["quote_at"] = _now(9, 0, 6).isoformat()
    assert engine.register_signal(
        spec=spec,
        summary=_summary(),
        signal_rows=[_row(0.5)],
        quotes={"2330": quote},
        eligibility=_eligibility(),
        eligibility_coverage={},
        now=_now(9, 0, 7),
    ) == "registered"
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["filled_shares"] == 1_000
    assert position["requested_shares"] == 10_000
    before_fills = (root / "fills.jsonl").read_text().splitlines()

    plan = make_plan(root, market=spec.market, session_date=date(2026, 8, 13), symbol="2330")
    result = apply_plan(root, plan, recorded_at=_now(10, 0))

    state = json.loads((root / "state.json").read_text())
    completed = state["modes"][spec.market]["positions"][position["position_id"]]
    fills = (root / "fills.jsonl").read_text().splitlines()
    assert result["status"] == "applied"
    assert result["completion_shares"] == 9_000
    assert completed["filled_shares"] == 10_000
    assert completed["signed_shares"] == 10_000
    assert completed["target_unsubmitted_shares"] == 0
    assert completed["entry_price"] == 1_000.0
    assert completed["manual_entry_completion"]["broker_fill"] is False
    assert fills[: len(before_fills)] == before_fills
    supplemental = json.loads(fills[-1])
    assert supplemental["fill_contract"] == CONTRACT
    assert supplemental["quantity"] == 9_000
    assert supplemental["price"] == 1_000.0
    assert supplemental["broker_fill"] is False
    signal_row = build_dashboard_signal_page(
        state_dir=root,
        session_date="2026-08-13",
        mode=spec.market,
        symbol="2330",
    )["rows"][0]
    assert signal_row["initial_filled_shares"] == 1_000
    assert signal_row["filled_shares"] == 10_000
    assert signal_row["target_unsubmitted_shares"] == 0
    assert signal_row["reason"] == "paper_entry_completed_after_initial_execution"
    position_row = build_dashboard_position_page(
        state_dir=root,
        session_date="2026-08-13",
        mode=spec.market,
        symbol="2330",
    )["rows"][0]
    assert position_row["target_unsubmitted_shares"] == 0
    assert position_row["entry_completion_contract"] == CONTRACT
