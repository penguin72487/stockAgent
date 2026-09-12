from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path

import pytest

from scripts.settle_tw_day_trade_official_close import (
    CONTRACT, LAST_PRICE_CONTRACT, apply_plan, make_plan, official_closes, reconcile_candidate,
)
from test_day_trade_margin_carry import setup_account
from test_tw_day_trade_simulation import _now


def reports(tmp_path, *, close="999", day="2026-08-13", volume="10"):
    root = tmp_path / "raw"
    for exchange in ("twse", "tpex"):
        directory = root / f"{exchange}_daily_ohlcv"
        directory.mkdir(parents=True, exist_ok=True)
        rows = [["2330", close, volume]] if exchange == "twse" else []
        (directory / f"{day}.json").write_text(json.dumps({"stat": "OK", "date": day.replace("-", ""),
            "tables": [{"fields": ["證券代號", "收盤價", "成交股數"], "data": rows}]}))
    return root


def test_source_no_close_is_not_zero_or_previous_close(tmp_path):
    root = reports(tmp_path, close="----")
    values, sources = official_closes(root, _now(13,30).date())
    assert values["2330"]["price"] is None
    assert len(sources) == 2
    path = root / "twse_daily_ohlcv/2026-08-13.json"
    j=json.loads(path.read_text());j["date"]="20260812";path.write_text(json.dumps(j))
    with pytest.raises(ValueError,match="date/status"):
        official_closes(root,_now(13,30).date())


def test_today_short_conversion_is_reversed_without_erasing_original_debit(tmp_path):
    engine,spec=setup_account(tmp_path,weight=-.2)
    mode=engine.state["modes"][spec.market]
    p=next(iter(mode["positions"].values()))
    cost=float(mode["cumulative_carry_cost_twd"])
    assert cost > 0
    old_debits=deepcopy(mode["carry_cost_ledger"])
    raw=reports(tmp_path,close="995")
    plan=make_plan(engine.state_dir,raw,_now(13,30).date())
    result=reconcile_candidate(engine.state_dir,plan,recorded_at=_now(18,0))
    state=json.loads(engine.state_path.read_text());mode=state["modes"][spec.market]
    p=next(iter(mode["positions"].values()))
    assert p["signed_shares"] == 0
    assert p["exit_price"] == 995
    assert mode["cumulative_carry_cost_twd"] == pytest.approx(0)
    assert mode["carry_cost_ledger"][:len(old_debits)] == old_debits
    assert sum(x["amount_twd"] for x in mode["carry_cost_ledger"]) == pytest.approx(0)
    assert result["conversion_cost_reversed_twd"] == pytest.approx(cost)
    assert p["manual_close_settlement"]["broker_fill"] is False
    assert p["manual_close_settlement"]["recorded_at"] == _now(18,0).isoformat()
    last=json.loads(engine.fills_path.read_text().splitlines()[-1])
    assert last["fill_contract"] == CONTRACT
    assert last["exchange_match_at"] is None
    assert last["fill_at"] == _now(13,30).isoformat()
    assert last["recorded_at"] == _now(18,0).isoformat()


def test_unpriced_residual_stays_in_ledger(tmp_path):
    engine,spec=setup_account(tmp_path)
    plan=make_plan(engine.state_dir,reports(tmp_path,close="----"),_now(13,30).date())
    before=engine.state_path.read_bytes()
    result=apply_plan(engine.state_dir,plan,recorded_at=_now(18,0))
    assert plan["settle_count"] == 0 and plan["blocked_count"] == 1
    assert result["status"] == "no_priced_residuals"
    assert before == engine.state_path.read_bytes()


def test_atomic_apply_retains_full_rollback_and_cannot_apply_twice(tmp_path):
    engine,spec=setup_account(tmp_path)
    plan=make_plan(engine.state_dir,reports(tmp_path),_now(13,30).date())
    original=engine.state_path.read_bytes()
    fills=engine.fills_path.read_bytes()
    result=apply_plan(engine.state_dir,plan,recorded_at=_now(18,0))
    assert result["remaining_count"] == 0
    backup=Path(result["rollback_directory"])
    assert (backup/"state.json").read_bytes() == original
    assert (backup/"fills.jsonl").read_bytes() == fills
    assert engine.fills_path.read_bytes().startswith(fills)
    assert json.loads(engine.state_path.read_text())["modes"][spec.market]["open_position_count"] == 0
    with pytest.raises(ValueError,match="book changed"):
        apply_plan(engine.state_dir,plan,recorded_at=_now(18,1))


def test_modified_plan_price_is_rejected_even_with_valid_source_digest(tmp_path):
    engine,spec=setup_account(tmp_path)
    plan=make_plan(engine.state_dir,reports(tmp_path),_now(13,30).date())
    plan["entries"][0]["price"]=1000
    with pytest.raises(ValueError,match="differs from the official"):
        apply_plan(engine.state_dir,plan,recorded_at=_now(18,0))


def test_before_close_and_changed_quantity_are_rejected(tmp_path):
    engine,spec=setup_account(tmp_path)
    plan=make_plan(engine.state_dir,reports(tmp_path),_now(13,30).date())
    with pytest.raises(ValueError,match="after today's"):
        reconcile_candidate(engine.state_dir,plan,recorded_at=_now(13,0))
    state=json.loads(engine.state_path.read_text())
    next(iter(state["modes"][spec.market]["positions"].values()))["signed_shares"]+=1000
    engine.state_path.write_text(json.dumps(state))
    with pytest.raises(ValueError,match="changed after plan"):
        reconcile_candidate(engine.state_dir,plan,recorded_at=_now(18,0))


def test_older_carry_costs_and_basis_survive_settlement(tmp_path):
    engine,spec=setup_account(tmp_path,weight=-.2)
    mode=engine.state["modes"][spec.market]
    next_day=_now(18,0)+timedelta(days=1)
    mode["session_date"]=str(next_day.date())
    engine._accrue_margin_carry_cost(mode,next_day)
    engine._persist(next_day)
    old_cost=mode["cumulative_carry_cost_twd"]
    plan=make_plan(engine.state_dir,reports(tmp_path,day=str(next_day.date()),close="995"),next_day.date())
    result=reconcile_candidate(engine.state_dir,plan,recorded_at=next_day)
    mode=json.loads(engine.state_path.read_text())["modes"][spec.market]
    assert mode["cumulative_carry_cost_twd"] == old_cost
    assert result["conversion_cost_reversed_twd"] == 0
    assert mode["open_position_count"] == 0


def test_explicit_last_price_preserves_price_date_and_prior_settlement(tmp_path):
    engine, spec = setup_account(tmp_path)
    previous = dict(contract=CONTRACT, session_date="2026-08-13", settled_count=73,
                    remaining_count=1, recorded_at=_now(17, 0).isoformat(), broker_fill=False)
    engine.state["modes"][spec.market]["manual_close_settlement"] = previous
    engine._persist(_now(17, 0))
    prior_receipt = dict(operation_id="2026-08-13:previous", rows=[{"preserved": True}])
    receipt_path = engine.state_dir / "official_close_settlement_receipt.json"
    receipt_path.write_text(json.dumps(prior_receipt))
    raw = reports(tmp_path, close="----", volume="0")
    reports(tmp_path, day="2026-08-12", close="995", volume="1,000")
    # Future data cannot become the last observed price.
    reports(tmp_path, day="2026-08-14", close="1100", volume="10,000")
    assert make_plan(engine.state_dir, raw, _now(18, 0).date())["blocked_count"] == 1
    plan = make_plan(engine.state_dir, raw, _now(18, 0).date(), last_traded_price_for=["2330"])
    entry = plan["entries"][0]
    assert entry["price"] == 995 and entry["price_date"] == "2026-08-12"
    assert plan["contract"] == LAST_PRICE_CONTRACT
    result = apply_plan(engine.state_dir, plan, recorded_at=_now(18, 0))
    assert result["remaining_count"] == 0
    assert json.loads(receipt_path.read_text()) == prior_receipt
    assert len(list((engine.state_dir / "settlement_receipts").glob("*.json"))) == 2
    mode = json.loads(engine.state_path.read_text())["modes"][spec.market]
    settlement = mode["manual_close_settlement"]
    assert settlement["settled_count"] == 74 and settlement["remaining_count"] == 0
    assert settlement["operations"][0] == previous
    assert settlement["last_traded_prices"][0]["price_date"] == "2026-08-12"
    p = next(iter(mode["positions"].values()))
    assert p["manual_close_settlement"]["price_date"] == "2026-08-12"
    fill = json.loads(engine.fills_path.read_text().splitlines()[-1])
    assert fill["fill_contract"] == LAST_PRICE_CONTRACT
    assert fill["counterfactual_settlement"]["price_date"] == "2026-08-12"
    assert fill["counterfactual_settlement"]["broker_fill"] is False
    assert fill["exchange_match_at"] is None


def test_last_price_does_not_use_zero_volume_carried_price_or_bid(tmp_path):
    engine, _ = setup_account(tmp_path)
    raw = reports(tmp_path, close="----", volume="0")
    reports(tmp_path, day="2026-08-12", close="1050", volume="0")
    reports(tmp_path, day="2026-08-11", close="990", volume="1,000")
    plan = make_plan(engine.state_dir, raw, _now(18, 0).date(), last_traded_price_for=["2330"])
    assert plan["entries"][0]["price"] == 990
    assert plan["entries"][0]["price_date"] == "2026-08-11"
    assert len(plan["sources"]) == 4


@pytest.mark.parametrize("close,volume,match", [
    ("----", "1", "positive volume without close"),
    ("----", "nan", "invalid official volume"),
    ("----", "-1", "invalid official volume"),
    ("----", "bad", "invalid official volume"),
])
def test_last_price_rejects_inconsistent_official_data(tmp_path, close, volume, match):
    engine, _ = setup_account(tmp_path)
    raw = reports(tmp_path, close=close, volume=volume)
    with pytest.raises(ValueError, match=match):
        make_plan(engine.state_dir, raw, _now(18, 0).date(), last_traded_price_for=["2330"])


def test_last_price_missing_report_and_unnecessary_or_unknown_scope_fail_closed(tmp_path):
    engine, _ = setup_account(tmp_path)
    raw = reports(tmp_path, close="----", volume="0")
    reports(tmp_path, day="2026-08-11", close="990")
    with pytest.raises(ValueError, match="missing report"):
        make_plan(engine.state_dir, raw, _now(18, 0).date(), last_traded_price_for=["2330"])
    with pytest.raises(ValueError, match="without residual"):
        make_plan(engine.state_dir, raw, _now(18, 0).date(), last_traded_price_for=["9999"])
    reports(tmp_path, close="995")
    with pytest.raises(ValueError, match="unnecessary"):
        make_plan(engine.state_dir, raw, _now(18, 0).date(), last_traded_price_for=["2330"])


@pytest.mark.parametrize("change", ["price", "date", "source"])
def test_last_price_apply_rechecks_price_date_and_source(tmp_path, change):
    engine, _ = setup_account(tmp_path)
    raw = reports(tmp_path, close="----", volume="0")
    reports(tmp_path, day="2026-08-12", close="995")
    plan = make_plan(engine.state_dir, raw, _now(18, 0).date(), last_traded_price_for=["2330"])
    if change == "price":
        plan["entries"][0]["price"] = 1000
    elif change == "date":
        plan["entries"][0]["price_date"] = "2026-08-13"
    else:
        reports(tmp_path, day="2026-08-12", close="1000")
    before = engine.state_path.read_bytes()
    with pytest.raises(ValueError, match="source"):
        apply_plan(engine.state_dir, plan, recorded_at=_now(18, 0))
    assert engine.state_path.read_bytes() == before


def test_dashboard_discloses_old_price_and_total_settlement_count():
    from stockagent.live.tw_day_trade_dashboard import _operational_issues
    issues = _operational_issues(modes=[dict(market="test", label="測試帳戶", manual_close_settlement={
        "settled_count": 75, "remaining_count": 0,
        "last_traded_prices": [{"symbol": "6680", "price": 53.3, "price_date": "2026-09-09"}],
    })], preopen={}, observed=_now(18, 0))
    issue = next(row for row in issues if row["code"] == "manual_official_close_settlement")
    assert "最後成交價" in issue["title"]
    for phrase in ("75 筆", "剩餘 0 筆", "6680：53.30 元", "2026-09-09", "不是本日收盤成交"):
        assert phrase in issue["detail"]
