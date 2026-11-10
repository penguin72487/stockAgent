import copy
from datetime import date
import json

import pytest

from scripts import audit_tw_day_trade_flat_snapshot_anchor as snapshot_audit
from scripts.audit_tw_day_trade_flat_snapshot_anchor import check_position


@pytest.fixture
def flat_paper_position():
    position = {"position_id": "p1", "market": "m", "session_date": "2026-10-29",
                "symbol": "2330", "side": "long", "entry_at": "2026-10-29T09:01:00+08:00",
                "filled_shares": 1000, "lot_size": 1000, "signed_shares": 0,
                "buy_fee_rate": 0.001425, "sell_fee_rate": 0.002925, "commission_rebate_rate": 0.00114,
                "entry_price": 10.0, "entry_fee_twd": 2.85, "realized_exit_fee_twd": 19.635,
                "realized_gross_pnl_twd": 1000.0, "realized_net_pnl_twd": 977.515}
    common = {"position_id": "p1", "market": "m", "session_date": "2026-10-29", "symbol": "2330",
              "quantity": 1000, "simulation_only": True}
    fills = [{**common, "purpose": "entry", "price": 10.0, "fee_and_tax_twd": 2.85,
              "fill_at": "2026-10-29T09:01:00+08:00"},
             {**common, "purpose": "13_20_limit_filled", "price": 11.0, "fee_and_tax_twd": 19.635,
              "fill_at": "2026-10-29T13:20:00+08:00"}]
    return position, fills


def test_snapshot_position_reduces_fills_and_fees(flat_paper_position):
    position, fills = flat_paper_position
    original = copy.deepcopy((position, fills))
    result = check_position(position, fills, market="m", day="2026-10-29")
    assert result["shares"] == 1000 and result["fill_count"] == 2
    assert result["net_pnl_twd"] == pytest.approx(977.515)
    assert (position, fills) == original


@pytest.mark.parametrize("damage", ["fee", "extra_share", "not_paper", "not_flat", "wrong_symbol", "naive_clock", "position_pnl"])
def test_snapshot_position_rejects_unsupported_or_inconsistent_evidence(flat_paper_position, damage):
    position, fills = flat_paper_position
    if damage == "fee":
        fills[0]["fee_and_tax_twd"] += 1
    elif damage == "extra_share":
        fills[1]["quantity"] += 1
    elif damage == "not_paper":
        fills[0]["simulation_only"] = False
    elif damage == "not_flat":
        position["signed_shares"] = 1000
    elif damage == "wrong_symbol":
        fills[1]["symbol"] = "2317"
    elif damage == "naive_clock":
        fills[1]["fill_at"] = "2026-10-29T13:20:00"
    else:
        position["realized_net_pnl_twd"] += 1
    with pytest.raises(RuntimeError):
        check_position(position, fills, market="m", day="2026-10-29")


@pytest.mark.parametrize("damage", [None, "wrong_prior_realized", "archive_mismatch"])
def test_flat_snapshot_audit_checks_identity_and_complete_daily_accounting(
        tmp_path, monkeypatch, flat_paper_position, damage):
    position, fills = flat_paper_position
    live = tmp_path / "live"
    archive = live / "position_history" / "2026-10-29" / "m.json"
    archive.parent.mkdir(parents=True)
    mode = {"market": "m", "session_date": "2026-10-29", "signal_id": "s", "positions": {"p1": position},
            "open_position_count": 0, "initial_capital_twd": 100000.0,
            "cumulative_realized_net_pnl_twd": 977.515, "total_equity_twd": 100977.515}
    state = {"simulation_only": True, "production_order_possible": False, "modes": {"m": mode}}
    snapshot = tmp_path / "state_before_apply.json"
    snapshot.write_text(json.dumps(state))
    archived = {"market": "m", "session_date": "2026-10-29", "signal_id": "s", "simulation_only": True,
                "production_order_possible": False, "positions": [copy.deepcopy(position)]}
    if damage == "archive_mismatch":
        archived["positions"][0]["entry_price"] += 1
    archive.write_text(json.dumps(archived))
    (live / "events.jsonl").write_text("")
    (live / "fills.jsonl").write_text("".join(json.dumps(item) + "\n" for item in fills))
    marks = [{"market": "m", "session_date": day, "minute": f"{day}T13:30+08:00",
              "recorded_at": f"{day}T13:30:00+08:00", "initial_capital_twd": 100000.0,
              "cumulative_realized_net_pnl_twd": pnl, "total_equity_twd": 100000.0 + pnl,
              "open_position_count": 0}
             for day, pnl in (("2026-10-28", 1.0 if damage == "wrong_prior_realized" else 0.0),
                              ("2026-10-29", 977.515))]
    (live / "marks.jsonl").write_text("".join(json.dumps(item) + "\n" for item in marks))
    monkeypatch.setattr(snapshot_audit, "_validated_taiex_session_dates", lambda *args: (
        [date(2026, 10, 28), date(2026, 10, 29), date(2026, 10, 30)], "calendar-sha"))
    monkeypatch.setattr(snapshot_audit.anchor, "_require_missing_session_target", lambda *args, **kwargs: None)
    kwargs = {"snapshot": snapshot, "live_dir": live, "markets": ["m"],
              "previous_day": date(2026, 10, 28), "anchor_day": date(2026, 10, 29),
              "target_day": date(2026, 10, 30), "calendar_root": tmp_path}
    if damage:
        with pytest.raises(RuntimeError):
            snapshot_audit.audit(**kwargs)
    else:
        result = snapshot_audit.audit(**kwargs)
        assert result["accounts"][0]["matching_paper_fills"] == 2
        assert result["publication_authorized"] is False and result["ledger_modified"] is False
