"""Terminal paper liquidity is explicit; the price and inventory remain real inputs."""
from copy import deepcopy
from dataclasses import replace
import json

import pytest

from stockagent.live.tw_day_trade_simulation import (
    DELAYED_AUCTION_PAPER_REPLAY_CONTRACT,
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


def delayed_auction(**changes):
    return dict(contract=DELAYED_AUCTION_PAPER_REPLAY_CONTRACT, session_date='2026-08-13',
        symbol='2330', source_minute_at=_now(13,33).isoformat(), observed_close=995.,
        volume_shares=1000, source_path='/private/observed.parquet', source_sha256='b'*64,
        clock_basis='historical_source_minute_label_not_broker_match') | changes


def _enable_replay(mode):
    mode.update(historical_minute_valuation=True, simulation_replay=True,
                counterfactual_open_replay=True, opening_repair={'simulation_only': True})


def test_delayed_paper_auction_settles_and_marks_at_1333_without_backdating(tmp_path):
    engine, spec = setup(tmp_path)
    mode = engine.state['modes'][spec.market]
    _enable_replay(mode)
    engine.process_quotes(quotes={}, now=_now(13,30))
    assert mode['open_position_count'] == 1
    quote = {'2330': {'session_close_evidence': evidence(), 'delayed_paper_auction': delayed_auction()}}
    engine.process_quotes(quotes=quote, now=_now(13,33))
    assert mode['open_position_count'] == 0
    close = json.loads(engine.fills_path.read_bytes().splitlines()[-1])
    assert close['fill_at'] == _now(13,33).isoformat()
    assert close['purpose'] == '13_33_delayed_close_paper_settlement'
    assert close['terminal_close_receipt']['effective_at'] == _now(13,33).isoformat()
    assert close['terminal_close_receipt']['broker_fill'] is False
    marks = [json.loads(line) for line in engine.marks_path.read_text().splitlines()]
    assert marks[-2]['minute'].startswith('2026-08-13T13:30')
    assert marks[-2]['open_position_count'] == 1
    assert marks[-1]['minute'].startswith('2026-08-13T13:33')
    assert marks[-1]['valuation_source'] == 'source_pinned_delayed_auction_event_v1'
    before = engine.marks_path.read_bytes()
    engine.process_quotes(quotes=quote, now=_now(13,35))
    assert engine.marks_path.read_bytes() == before
    assert register(engine, spec, 1, -.2) == 'registered'
    assert 'delayed_paper_auction_settled_at' not in engine.state['modes'][spec.market]


@pytest.mark.parametrize('changes', [
    {'symbol': '2317'}, {'session_date': '2026-08-12'}, {'observed_close': 990.},
    {'volume_shares': 0}, {'source_sha256': ''}, {'source_path': ''},
    {'clock_basis': 'exchange_match'},
    {'source_minute_at': '2026-08-13T13:30:00+08:00'},
    {'source_minute_at': '2026-08-13T13:33:00'},
])
def test_bad_delayed_auction_never_falls_back_to_1330(tmp_path, changes):
    engine, spec = setup(tmp_path)
    mode = engine.state['modes'][spec.market]
    _enable_replay(mode)
    before = engine.fills_path.read_bytes()
    engine.process_quotes(quotes={'2330': {'session_close_evidence': evidence(),
        'delayed_paper_auction': delayed_auction(**changes)}}, now=_now(13,33))
    assert mode['open_position_count'] == 1
    assert engine.fills_path.read_bytes() == before


def test_delayed_paper_clock_never_applies_to_live_mode(tmp_path):
    engine, spec = setup(tmp_path)
    before = engine.fills_path.read_bytes()
    engine.process_quotes(quotes={'2330': {'session_close_evidence': evidence(),
        'delayed_paper_auction': delayed_auction()}}, now=_now(13,33))
    assert engine.state['modes'][spec.market]['open_position_count'] == 1
    assert engine.fills_path.read_bytes() == before


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
    import hashlib
    from pathlib import Path
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
    source = quotes['2330']['session_close_evidence']
    assert source['price'] == 995. and source['symbol'] == '2330'
    assert hashlib.sha256(Path(source['source_path']).read_bytes()).hexdigest() == source['source_sha256']
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


def test_historical_terminal_close_retains_the_exact_official_source_path(tmp_path, monkeypatch):
    import hashlib
    from pathlib import Path
    import stockagent.live.tw_share_replacement as shares
    from scripts import rebuild_tw_day_trade_open_price_replay as replay
    from test_day_trade_official_close_settlement import reports

    engine, spec = setup(tmp_path)
    position = next(iter(engine.state['modes'][spec.market]['positions'].values()))
    opening = float(position['sizing_open_price'])
    monkeypatch.setattr(shares, 'halted_symbols', lambda *_: set())
    monkeypatch.setattr(replay, '_official_daily_row', lambda *_: {
        'open': opening, 'close': 995., '_official_source': 'twse'})
    root = reports(tmp_path, close='995', volume='10')
    quotes, coverage = replay._close_quotes(engine, {spec.market: spec},
        trading_date=_now(13, 30).date(), quote_at=_now(13, 30),
        twse_daily_ohlcv_path=tmp_path / 'twse.parquet',
        tpex_daily_ohlcv_path=tmp_path / 'tpex.parquet', official_raw_root=root)
    source = quotes['2330']['session_close_evidence']
    assert source['symbol'] == '2330'
    assert hashlib.sha256(Path(source['source_path']).read_bytes()).hexdigest() == source['source_sha256']
    assert any(item['path'] == source['source_path'] for item in coverage['terminal_close_sources'])


@pytest.mark.parametrize('missing_exchange', ['twse', 'tpex'])
@pytest.mark.parametrize('failure', ['missing', 'wrong_date', 'invalid_json', 'invalid_schema'])
def test_runtime_close_sources_fail_independently(tmp_path, missing_exchange, failure):
    from types import SimpleNamespace
    from scripts.run_tw_day_trade_simulation import _attach_terminal_official_close_context
    from test_day_trade_official_close_settlement import reports

    engine, spec = setup(tmp_path)
    root = reports(tmp_path, close='995', volume='10')
    if missing_exchange == 'twse':
        # Same retained trade, but through the other source's supported schema.
        listed = json.loads((root / 'twse_daily_ohlcv/2026-08-13.json').read_text())
        (root / 'tpex_daily_ohlcv/2026-08-13.json').write_text(json.dumps(listed))
    failed = root / f'{missing_exchange}_daily_ohlcv/2026-08-13.json'
    if failure == 'missing':
        failed.unlink()
    elif failure == 'wrong_date':
        raw = json.loads(failed.read_text())
        raw['date'] = '20260812'
        failed.write_text(json.dumps(raw))
    elif failure == 'invalid_json':
        failed.write_text('{invalid')
    else:
        failed.write_text('[]')
    quotes = {}
    _attach_terminal_official_close_context(
        quotes, engine=engine, specs=[spec],
        configs={spec.market: SimpleNamespace(day_trade_rule_data_dir=str(tmp_path))},
        observed=_now(13, 35),
    )
    valid_exchange = 'tpex' if missing_exchange == 'twse' else 'twse'
    assert quotes['2330']['session_close_evidence']['source'] == f'{valid_exchange}_official_daily_close'
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert engine.state['modes'][spec.market]['open_position_count'] == 0


@pytest.mark.parametrize('volume', ['0', 'nan', 'inf', '-1', '1.5', 'bad'])
def test_runtime_close_hook_never_uses_carried_or_invalid_volume(tmp_path, volume):
    from types import SimpleNamespace
    from scripts.run_tw_day_trade_simulation import _attach_terminal_official_close_context
    from test_day_trade_official_close_settlement import reports

    engine, spec = setup(tmp_path)
    reports(tmp_path, close='995', volume=volume)
    quotes = {}
    _attach_terminal_official_close_context(
        quotes, engine=engine, specs=[spec],
        configs={spec.market: SimpleNamespace(day_trade_rule_data_dir=str(tmp_path))},
        observed=_now(13, 35),
    )
    assert not quotes
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert engine.state['modes'][spec.market]['open_position_count'] == 1


def test_runtime_close_conflicting_venue_identity_does_not_authorize_fill(tmp_path):
    from types import SimpleNamespace
    from scripts.run_tw_day_trade_simulation import _attach_terminal_official_close_context
    from test_day_trade_official_close_settlement import reports

    engine, spec = setup(tmp_path)
    root = reports(tmp_path, close='995', volume='10')
    duplicate = json.loads((root / 'twse_daily_ohlcv/2026-08-13.json').read_text())
    (root / 'tpex_daily_ohlcv/2026-08-13.json').write_text(json.dumps(duplicate))
    quotes = {}
    _attach_terminal_official_close_context(
        quotes, engine=engine, specs=[spec],
        configs={spec.market: SimpleNamespace(day_trade_rule_data_dir=str(tmp_path))},
        observed=_now(13, 35),
    )
    assert not quotes
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert engine.state['modes'][spec.market]['open_position_count'] == 1
