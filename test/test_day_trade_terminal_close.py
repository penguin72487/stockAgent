"""Terminal paper liquidity is explicit; the price and inventory remain real inputs."""
from copy import deepcopy
from dataclasses import replace
import json

import pytest

from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE, TERMINAL_CLOSE_UNLIMITED_CONTRACT,
    TwDayTradeSimulationEngine,
)
from test_day_trade_margin_carry import action_reference, register
from test_tw_day_trade_simulation import _spec, _now


def setup(tmp_path, weight=-.2):
    spec = replace(_spec(tmp_path), residual_margin_conversion=True,
        terminal_liquidation_unlimited_capacity=True,
        margin_corporate_action_reference_path=action_reference(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE)
    engine = TwDayTradeSimulationEngine(tmp_path / 'state')
    assert register(engine, spec, 0, weight) == 'registered'
    return engine, spec


def evidence(**changes):
    return dict(session_date='2026-08-13', price=995., source='twse_official_daily_close',
                source_sha256='a'*64, price_basis='official_session_close') | changes


def test_missing_close_retries_after_restart_without_short_conversion(tmp_path):
    engine, spec = setup(tmp_path)
    engine.process_quotes(quotes={}, now=_now(13,30))
    mode = engine.state['modes'][spec.market]
    assert mode['open_position_count'] == 1
    assert mode['engine_status'] == 'waiting_valid_terminal_close_source'
    assert not mode.get('closing_auction_settled_at')
    assert mode.get('cumulative_carry_cost_twd', 0) == 0
    engine = TwDayTradeSimulationEngine(engine.state_dir)
    quote = {'2330': {'session_close_evidence': evidence(), 'minute_volume_lots': 0}}
    engine.process_quotes(quotes=quote, now=_now(13,35))
    mode = engine.state['modes'][spec.market]
    assert mode['open_position_count'] == 0
    assert mode.get('cumulative_carry_cost_twd', 0) == 0
    fills = engine.fills_path.read_bytes()
    close = json.loads(fills.splitlines()[-1])
    assert close['fill_contract'] == TERMINAL_CLOSE_UNLIMITED_CONTRACT
    assert close['fill_at'] == _now(13,30).isoformat()
    assert close['recorded_at'] == _now(13,35).isoformat()
    assert close['quote_at'] is None and close['exchange_match_at'] is None
    engine.process_quotes(quotes=quote, now=_now(13,36))
    assert engine.fills_path.read_bytes() == fills


@pytest.mark.parametrize('bad', [
    {'session_date': '2026-08-12'}, {'price': 995.1}, {'price': 0},
    {'source_sha256': ''}, {'source': 'last_price_cache'},
    {'price_basis': 'previous_close'},
])
def test_invalid_close_evidence_never_manufactures_a_fill(tmp_path, bad):
    engine, spec = setup(tmp_path)
    before = engine.fills_path.read_bytes()
    engine.process_quotes(quotes={'2330': {'session_close_evidence': evidence(**bad)}}, now=_now(13,30))
    assert engine.state['modes'][spec.market]['open_position_count'] == 1
    assert engine.fills_path.read_bytes() == before


def test_source_proven_halt_is_not_overridden_by_unlimited_capacity(tmp_path):
    engine, spec = setup(tmp_path)
    p = next(iter(engine.state['modes'][spec.market]['positions'].values()))
    p['share_replacement_halted_until'] = '2026-08-20'
    engine.process_quotes(quotes={'2330': {'session_close_evidence': evidence()}}, now=_now(13,30))
    assert p['signed_shares'] != 0


@pytest.mark.parametrize('trial,volume,filled', [(False,1,True),(True,1,False),(False,0,False)])
def test_live_auction_requires_real_price_evidence_not_sufficient_capacity(tmp_path, trial, volume, filled):
    engine, spec = setup(tmp_path)
    quote = dict(last=995., bid_volume=0, ask_volume=0,
        auction_volume_source='exchange_non_trial_quote_trade', auction_volume_lots=volume,
        trade_simtrade=trial, trade_quote_at=_now(13,30).isoformat(), exchange_quote_at=_now(13,30).isoformat())
    engine.process_quotes(quotes={'2330':quote}, now=_now(13,30))
    assert (engine.state['modes'][spec.market]['open_position_count'] == 0) is filled


def test_scoped_settlement_preserves_retired_book(tmp_path):
    from scripts.settle_tw_day_trade_official_close import make_plan, reconcile_candidate
    from test_day_trade_official_close_settlement import reports
    engine, spec = setup(tmp_path)
    engine.state['modes']['retired'] = deepcopy(engine.state['modes'][spec.market])
    engine.state['modes']['retired']['configured_enabled'] = False
    engine._persist(_now(13,30))
    before = json.loads(engine.state_path.read_text())['modes']['retired']
    plan = make_plan(engine.state_dir, reports(tmp_path, close='995'), _now(13,30).date(), markets=[spec.market])
    result = reconcile_candidate(engine.state_dir, plan, recorded_at=_now(18,0))
    assert result['remaining_count'] == 0
    assert json.loads(engine.state_path.read_text())['modes']['retired'] == before


def test_runtime_report_hook_and_independent_close_audit(tmp_path):
    from types import SimpleNamespace
    from scripts.run_tw_day_trade_simulation import _attach_terminal_official_close_context
    from scripts.settle_tw_day_trade_official_close import official_closes
    from scripts.audit_tw_day_trade_margin_replay import _verify_terminal_close_fills
    from test_day_trade_official_close_settlement import reports
    engine, spec = setup(tmp_path)
    raw_root = reports(tmp_path, close='995', volume='10')
    quotes = {}
    _attach_terminal_official_close_context(quotes, engine=engine, specs=[spec],
        configs={spec.market: SimpleNamespace(day_trade_rule_data_dir=str(tmp_path))}, observed=_now(13,35))
    assert quotes['2330']['session_close_evidence']['price'] == 995.
    engine.process_quotes(quotes=quotes, now=_now(13,35))
    fills = [json.loads(line) for line in engine.fills_path.read_text().splitlines()]
    _, sources = official_closes(raw_root, _now(13,30).date())
    receipt = {'replay_contract': {'terminal_close_contract': TERMINAL_CLOSE_UNLIMITED_CONTRACT},
        'sessions': [{'session_date': '2026-08-13', 'close': {'terminal_close_sources': sources},
                     'modes': [{'market': spec.market, 'after_close': {
                         'terminal_close_contract': TERMINAL_CLOSE_UNLIMITED_CONTRACT,
                         'terminal_flatten_count': 1, 'open_position_rows': 0}}]}]}
    hashes = _verify_terminal_close_fills(fills, receipt, engine.state['modes'])
    assert len(hashes) == 2
    for change in ({'price': 1000.}, {'quantity': 1}, {'fill_at': _now(13,29).isoformat()},
                   {'remaining_quantity': 1}, {'exchange_match_at': _now(13,30).isoformat()}):
        broken = deepcopy(fills)
        broken[-1].update(change)
        with pytest.raises(ValueError, match='terminal close'):
            _verify_terminal_close_fills(broken, receipt, engine.state['modes'])
    with pytest.raises(ValueError, match='duplicate terminal close'):
        _verify_terminal_close_fills([*fills, fills[-1]], receipt, engine.state['modes'])
    reports(tmp_path, close='990', volume='10')
    with pytest.raises(ValueError, match='source mismatch'):
        _verify_terminal_close_fills(fills, receipt, engine.state['modes'])
