from dataclasses import replace
from datetime import datetime

import pytest

from scripts import audit_tw_day_trade_margin_replay as audit
from scripts import rebuild_tw_day_trade_open_price_replay as replay
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE, HISTORICAL_BRACKET_FULL_CROSS_CONTRACT,
)


class Engine:
    def __init__(self, side="short"):
        position = {"symbol": "6624", "side": side, "signed_shares": -12000 if side == "short" else 12000,
                    "lot_size": 1000, "stop_trigger_price": 47.25, "take_profit_price": 40.0,
                    "price_limit_offset_ticks": 1}
        self.state = {"modes": {"m": {"positions": {"p": position},
            "historical_bracket_cross_assumption": HISTORICAL_BRACKET_FULL_CROSS_CONTRACT}}}
        self.calls = []

    def _close_position(self, position, mode, **kwargs):
        self.calls.append(kwargs)


def _bar(*, low=47.3, high=47.3, opening=47.3, close=47.3, volume=8000):
    return {"open": opening, "high": high, "low": low, "close": close,
            "vwap": close, "volume_shares": volume}


def _apply(engine, bar):
    now = datetime.fromisoformat("2026-06-05T09:02:00+08:00")
    replay._apply_historical_kbar_brackets(engine, market="m",
        bars={"6624": {now.isoformat(timespec="minutes"): bar}}, observed=now, append_mark=False)


def test_strict_stop_cross_assumes_full_residual_at_adverse_observed_extreme():
    engine = Engine()
    _apply(engine, _bar())
    call = engine.calls[0]
    assert call["quantity"] == 12000 and call["price"] == 47.3
    assert call["quote"]["minute_volume_lots"] == 8.0  # never inflate the source volume
    assert call["quote"]["fill_contract"] == HISTORICAL_BRACKET_FULL_CROSS_CONTRACT
    assert call["quote"]["bracket_cross_evidence"]["exchange_fill"] is False


def test_touch_without_strict_cross_keeps_observed_volume_cap():
    engine = Engine()
    _apply(engine, _bar(low=47.25, high=47.25, opening=47.25, close=47.25))
    assert engine.calls[0]["quantity"] == 4000
    assert engine.calls[0]["quote"]["fill_contract"] == replay.HISTORICAL_KBAR_FILL_CONTRACT


def test_both_brackets_in_one_minute_use_stop_first_and_no_bar_means_no_fill():
    engine = Engine()
    _apply(engine, _bar(low=39.0, high=48.0, opening=47.0, close=47.0))
    assert len(engine.calls) == 1 and engine.calls[0]["price"] == 48.0
    assert engine.calls[0]["quote"]["bracket_cross_evidence"]["bracket"] == "stop_loss"
    engine = Engine()
    replay._apply_historical_kbar_brackets(engine, market="m", bars={},
        observed=datetime.fromisoformat("2026-06-05T09:02:00+08:00"), append_mark=False)
    assert not engine.calls


def test_profit_cross_is_full_but_1320_can_disable_old_profit_order():
    engine = Engine()
    bar = _bar(low=39.9, high=41.0, opening=40.1, close=40.0)
    _apply(engine, bar)
    assert engine.calls[0]["quantity"] == 12000
    assert engine.calls[0]["quote"]["bracket_cross_evidence"]["bracket"] == "take_profit"
    engine = Engine()
    now = datetime.fromisoformat("2026-06-05T13:20:00+08:00")
    replay._apply_historical_kbar_brackets(engine, market="m",
        bars={"6624": {now.isoformat(timespec="minutes"): bar}}, observed=now,
        append_mark=False, allow_take_profit=False)
    assert not engine.calls


def test_full_cross_isolated_spec_cannot_be_enabled_for_normal_live_mode(tmp_path):
    from test_tw_day_trade_simulation import _spec
    spec = _spec(tmp_path)
    with pytest.raises(ValueError, match="requires a labelled isolated"):
        replace(spec, historical_bracket_full_on_cross=True)
    replace(spec, entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
            historical_counterfactual_provisional=True, historical_bracket_full_on_cross=True)


def test_independent_auditor_rechecks_source_cross_and_original_trigger():
    contract = HISTORICAL_BRACKET_FULL_CROSS_CONTRACT
    evidence = {"contract": contract, "symbol": "6624", "side": "short", "bracket": "stop_loss",
                "trigger_price": 47.25, "simulation_only": True, "exchange_fill": False, "broker_fill": False}
    fill = {"session_date": "2026-06-05", "symbol": "6624", "side": "buy_to_cover",
            "purpose": "stop_loss_inside_daily_limit_1_tick_trigger", "quantity": 12000,
            "requested_quantity": 12000, "remaining_quantity": 0, "price": 47.3,
            "fill_at": "2026-06-05T09:02:00+08:00", "simulation_replay": True,
            "fill_contract": contract, "bracket_cross_evidence": evidence}
    rebuild = {"historical_bracket_cross_assumption": contract, "sessions": [{"session_date": "2026-06-05",
        "intraday_replay": {"historical_bracket_cross_assumption": contract,
                            "full_bracket_fill_is_assumed_not_broker_or_exchange_fill": True}}]}
    entry = {"stop_trigger_price": 47.25}
    bar = {"low": 47.3, "high": 47.3, "volume_shares": 8000}
    assert audit._verified_full_cross_assumption(fill, bar, entry, rebuild)
    assert not audit._verified_full_cross_assumption(fill, bar | {"high": 47.25}, entry, rebuild)
    assert not audit._verified_full_cross_assumption(fill, bar, {"stop_trigger_price": 47.35}, rebuild)
    assert not audit._verified_full_cross_assumption(fill | {"remaining_quantity": 1000}, bar, entry, rebuild)


def test_only_independently_proven_flat_day_may_lack_held_minute_sources():
    day = "2026-03-23"
    session = {"session_date": day, "intraday_replay": {"source_files": [], "requested_symbols": 0,
        "resolved_symbols": 0, "missing_symbols": []},
        "modes": [{"entry": {"position_rows": 0}, "after_close": {"open_position_rows": 0}}]}
    marks = [{"session_date": day, "open_position_count": 0} for _ in range(270)]
    assert audit._source_free_flat_sessions([session], [], marks, []) == frozenset({day})
    assert not audit._source_free_flat_sessions([session], [{"session_date": day}], marks, [])
    assert not audit._source_free_flat_sessions([session], [], marks[:-1], [])
    assert not audit._source_free_flat_sessions([session], [], marks[:-1] + [marks[-1] | {"open_position_count": 1}], [])
    assert not audit._source_free_flat_sessions([session], [], marks, [{"session_date": day}])
