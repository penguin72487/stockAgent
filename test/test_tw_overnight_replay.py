from datetime import datetime
import json

import polars as pl
import pytest

from scripts.rebuild_tw_overnight_history import _blocked_replay_signal
from scripts.run_tw_overnight_simulation import (
    SHARED_QUOTE_MIN_INTERVAL_SECONDS,
    _readiness_refresh_interval_seconds,
    _service_status_text,
    _shared_quote_due,
    _spec_reload_due,
    _spec_reload_interval_seconds,
)
from stockagent.live.tw_day_trade_dashboard import (
    build_dashboard_event_page,
    build_dashboard_history_snapshot,
    build_dashboard_position_page,
    build_dashboard_signal_page,
)
from stockagent.live.tw_overnight_replay import TwOvernightHistoricalReplayEngine
from stockagent.live.tw_overnight_simulation import TwOvernightSimulationEngine
from stockagent.live.tw_day_trade_service_sync import load_service_sync
from test_tw_overnight_simulation import _at, _row, _spec, _summary


def test_shared_quote_poll_is_bounded_without_waiting_a_full_minute():
    assert SHARED_QUOTE_MIN_INTERVAL_SECONDS == 1.0
    assert _shared_quote_due(100.0, float("-inf"))
    assert not _shared_quote_due(100.999, 100.0)
    assert _shared_quote_due(101.0, 100.0)


def test_overnight_idle_heartbeat_keeps_auction_windows_fast(tmp_path):
    for observed in (_at(9, 8, 30), _at(9, 9, 0), _at(9, 13, 20)):
        assert _readiness_refresh_interval_seconds(observed) == 10
        assert _spec_reload_interval_seconds(observed) == 30
    for observed in (_at(9, 7, 0), _at(9, 10, 0), _at(9, 14, 0)):
        assert _readiness_refresh_interval_seconds(observed) == 60
        assert _spec_reload_interval_seconds(observed) == 60
    assert _readiness_refresh_interval_seconds(
        _at(9, 13, 20), has_enabled_modes=False,
    ) == 60
    assert _spec_reload_due(100.0, 0.0, 60.0)
    assert not _spec_reload_due(120.0, 100.0, 60.0)

    engine = TwOvernightSimulationEngine(tmp_path / "live_state")
    engine.update_readiness([_spec(tmp_path)], now=_at(9, 10, 0))
    state_before = engine.state_path.read_bytes()
    status_before = engine.status_path.read_bytes()
    committed = load_service_sync(engine.state_dir)
    assert committed is not None
    engine.publish_liveness(_at(9, 10, 20))
    alive = load_service_sync(engine.state_dir)
    assert alive is not None
    assert alive["state_revision"] == committed["state_revision"]
    assert alive["published_at"] == committed["published_at"]
    assert alive["heartbeat_at"] != committed["heartbeat_at"]
    assert engine.state_path.read_bytes() == state_before
    assert engine.status_path.read_bytes() == status_before

    engine.state["enabled_markets"] = []
    assert "no enabled modes" in _service_status_text(engine, _at(9, 14, 0))


def select(engine, day, *, opening=103.0, close=101.0):
    text = f'2026-09-{day:02d}'
    engine.select_session(text, {'2330': {'date': text, 'open': opening, 'close': close}}, {
        'session_date': text, 'source_hashes': {'price_limits': 'a' * 64, 'official_daily': 'b' * 64},
    })
    return {'2330': {
        'last': close, 'open': opening, 'lower_limit': 90., 'upper_limit': 110.,
        'historical_limits_session_date': text, 'historical_limits_sha256': 'a' * 64,
    }}


def test_replay_reuses_account_and_keeps_exchange_timestamps_absent(tmp_path):
    spec = _spec(tmp_path)
    engine = TwOvernightHistoricalReplayEngine(tmp_path / 'replay')
    engine.update_readiness([spec], now=_at(9, 8, 30))
    quotes = select(engine, 9)
    summary = {**_summary(), 'counterfactual_signal_regeneration': True,
               'replay_effective_signal_at': _at(9, 13, 25).isoformat(),
               'generated_at': datetime.now().isoformat()}
    assert engine.register_close_signal(
        spec=spec, summary=summary, signal_rows=[_row()], quotes=quotes, now=_at(9, 13, 25),
    ) == 'registered'
    engine.process_quotes(quotes=quotes, now=_at(9, 13, 30))
    position = next(iter(engine.state['modes'][spec.market]['positions'].values()))
    assert position['signed_shares'] == 2000
    assert position['entry_price'] == 101.
    assert position['entry_exchange_at'] is None
    # No same-day open exit, even if the source also contains an OPEN.
    engine.process_quotes(quotes=quotes, now=_at(9, 13, 31))
    assert position['signed_shares'] == 2000
    quotes = select(engine, 10, opening=103.)
    engine.process_quotes(quotes=quotes, now=_at(10, 9, 0))
    assert position['signed_shares'] == 0
    assert position['exit_exchange_at'] is None
    expected = 2000 * (103 - 101) - 2000 * 101 * 0.000285 - 2000 * 103 * 0.003285
    assert position['net_pnl_twd'] == pytest.approx(expected)
    fills = [json.loads(line) for line in engine.fills_path.read_text().splitlines()]
    assert len(fills) == 2
    assert all(row['counterfactual'] and row['exchange_match_at'] is None for row in fills)
    assert all('counterfactual' in row['fill_contract'] for row in fills)
    marks = [json.loads(line) for line in engine.marks_path.read_text().splitlines()]
    assert marks[-1]['valuation_basis'] == 'flat_cash_balance'


def test_live_executor_rejects_historical_price_without_exchange_print(tmp_path):
    spec = _spec(tmp_path)
    engine = TwOvernightSimulationEngine(tmp_path / 'live_state')
    engine.update_readiness([spec], now=_at(9, 13, 25))
    quotes = {'2330': {'last': 101., 'open': 100., 'upper_limit': 110., 'lower_limit': 90.,
                       'historical_limits_session_date': '2026-09-09'}}
    engine.register_close_signal(spec=spec, summary=_summary(), signal_rows=[_row()],
                                 quotes=quotes, now=_at(9, 13, 25))
    engine.process_quotes(quotes=quotes, now=_at(9, 13, 30))
    assert not engine.state['modes'][spec.market]['positions']


def test_replay_refuses_live_directory_and_wrong_session(tmp_path):
    with pytest.raises(ValueError, match='isolated'):
        TwOvernightHistoricalReplayEngine(tmp_path / 'live' / 'state')
    engine = TwOvernightHistoricalReplayEngine(tmp_path / 'replay')
    select(engine, 9)
    with pytest.raises(ValueError, match='chronologically'):
        select(engine, 9)
    with pytest.raises(ValueError, match='differs'):
        engine._match_auction_print({}, symbol='2330', session_date='2026-09-10', price_field='open')


def test_missing_next_open_preserves_unclosed_cohort(tmp_path):
    spec = _spec(tmp_path)
    engine = TwOvernightHistoricalReplayEngine(tmp_path / 'replay')
    engine.update_readiness([spec], now=_at(9, 13, 25))
    quotes = select(engine, 9)
    summary = {**_summary(), 'counterfactual_signal_regeneration': True,
               'replay_effective_signal_at': _at(9, 13, 25).isoformat()}
    engine.register_close_signal(spec=spec, summary=summary, signal_rows=[_row()],
                                 quotes=quotes, now=_at(9, 13, 25))
    engine.process_quotes(quotes=quotes, now=_at(9, 13, 30))
    quotes = select(engine, 10, opening=None)
    engine.process_quotes(quotes=quotes, now=_at(10, 9, 0))
    position = next(iter(engine.state['modes'][spec.market]['positions'].values()))
    assert position['signed_shares'] == 2000
    assert len(engine.fills_path.read_text().splitlines()) == 1


def test_missing_next_open_blocks_only_new_cohort_without_fabricating_fill(tmp_path):
    spec = _spec(tmp_path)
    engine = TwOvernightHistoricalReplayEngine(tmp_path / 'replay')
    engine.update_readiness([spec], now=_at(9, 13, 25))
    quotes = select(engine, 9)
    summary = {**_summary(), 'counterfactual_signal_regeneration': True,
               'replay_effective_signal_at': _at(9, 13, 25).isoformat()}
    assert engine.register_close_signal(
        spec=spec, summary=summary, signal_rows=[_row()],
        quotes=quotes, now=_at(9, 13, 25),
    ) == 'registered'
    engine.process_quotes(quotes=quotes, now=_at(9, 13, 30))
    quotes = select(engine, 10, opening=None)
    engine.process_quotes(quotes=quotes, now=_at(10, 9, 0))
    second = {**summary, 'signal_id': 'second-signal',
              'replay_effective_signal_at': _at(10, 13, 25).isoformat()}
    outcome = engine.register_close_signal(
        spec=spec, summary=second, signal_rows=[_row()],
        quotes=quotes, now=_at(10, 13, 25),
    )
    blocked = _blocked_replay_signal(
        market=spec.market, day='2026-09-10', outcome=outcome,
        mode=engine.state['modes'][spec.market], signal_id='second-signal',
    )
    assert blocked is not None
    assert blocked['reason'] == 'prior_overnight_cohort_still_open'
    assert blocked['unresolved_positions'] == ['2026-09-09:2330:working']
    assert len(engine.fills_path.read_text().splitlines()) == 1
    quotes = select(engine, 11, opening=103.)
    engine.process_quotes(quotes=quotes, now=_at(11, 9, 0))
    assert next(iter(engine.state['modes'][spec.market]['positions'].values()))['signed_shares'] == 0
    assert len(engine.fills_path.read_text().splitlines()) == 2


def test_historical_replay_still_rejects_unrelated_execution_block():
    with pytest.raises(ValueError, match='signal_before_13_20_decision_gate'):
        _blocked_replay_signal(
            market='test', day='2026-09-10', outcome='blocked',
            mode={'blocked_reason': 'signal_before_13_20_decision_gate', 'positions': {}},
            signal_id='bad-signal',
        )
    with pytest.raises(ValueError, match='prior_overnight_cohort_still_open'):
        _blocked_replay_signal(
            market='test', day='2026-09-10', outcome='already_processed',
            mode={'blocked_reason': 'prior_overnight_cohort_still_open',
                  'signal_id': 'old-signal',
                  'positions': {'p': {'session_date': '2026-09-09', 'symbol': '2330',
                                      'signed_shares': 1000, 'opening_exit_order_status': 'working'}}},
            signal_id='new-signal',
        )


def test_historical_short_uses_ordinary_sell_tax_and_buyback_direction(tmp_path):
    spec = _spec(tmp_path)
    engine = TwOvernightHistoricalReplayEngine(tmp_path / 'replay')
    engine.update_readiness([spec], now=_at(9, 13, 25))
    quotes = select(engine, 9)
    summary = {**_summary(), 'counterfactual_signal_regeneration': True,
               'replay_effective_signal_at': _at(9, 13, 25).isoformat()}
    row = {**_row(), 'target_weight': -0.02, 'overnight_can_short_open': True,
           'overnight_short_capacity_shares': 2000}
    engine.register_close_signal(spec=spec, summary=summary, signal_rows=[row],
                                 quotes=quotes, now=_at(9, 13, 25))
    engine.process_quotes(quotes=quotes, now=_at(9, 13, 30))
    quotes = select(engine, 10, opening=103.)
    engine.process_quotes(quotes=quotes, now=_at(10, 9, 0))
    position = next(iter(engine.state['modes'][spec.market]['positions'].values()))
    expected = 2000 * (101 - 103) - 2000 * 101 * 0.003285 - 2000 * 103 * 0.000285
    assert position['signed_shares'] == 0
    assert position['net_pnl_twd'] == pytest.approx(expected)


def test_historical_official_open_is_not_rejected_by_bad_reconstructed_etf_band(tmp_path):
    spec = _spec(tmp_path)
    engine = TwOvernightHistoricalReplayEngine(tmp_path / 'replay')
    engine.update_readiness([spec], now=_at(9, 13, 25))
    quotes = select(engine, 9)
    summary = {**_summary(), 'counterfactual_signal_regeneration': True,
               'replay_effective_signal_at': _at(9, 13, 25).isoformat()}
    engine.register_close_signal(spec=spec, summary=summary, signal_rows=[_row()],
                                 quotes=quotes, now=_at(9, 13, 25))
    engine.process_quotes(quotes=quotes, now=_at(9, 13, 30))
    quotes = select(engine, 10, opening=125.)
    engine.process_quotes(quotes=quotes, now=_at(10, 9, 0))
    position = next(iter(engine.state['modes'][spec.market]['positions'].values()))
    assert position['signed_shares'] == 0
    assert position['exit_price'] == 125.


def test_next_cohort_uses_current_equity_and_blocks_after_account_default(tmp_path):
    spec = _spec(tmp_path)
    engine = TwOvernightHistoricalReplayEngine(tmp_path / 'replay')
    engine.update_readiness([spec], now=_at(9, 13, 25))
    quotes = select(engine, 9, close=100.)
    summary = {**_summary(), 'counterfactual_signal_regeneration': True,
               'replay_effective_signal_at': _at(9, 13, 25).isoformat()}
    engine.register_close_signal(
        spec=spec, summary=summary,
        signal_rows=[{**_row(), 'target_weight': 1.0}], quotes=quotes,
        now=_at(9, 13, 25),
    )
    engine.process_quotes(quotes=quotes, now=_at(9, 13, 30))
    quotes = select(engine, 10, opening=0.01, close=0.01)
    engine.process_quotes(quotes=quotes, now=_at(10, 9, 0))
    mode = engine.state['modes'][spec.market]
    assert mode['total_equity_twd'] < 0

    summary = {**summary, 'signal_id': 'close-signal-2',
               'replay_effective_signal_at': _at(10, 13, 25).isoformat()}
    assert engine.register_close_signal(
        spec=spec, summary=summary,
        signal_rows=[{**_row(), 'target_weight': 1.0, 'current_price': 0.01}],
        quotes=quotes, now=_at(10, 13, 25),
    ) == 'registered'
    mode = engine.state['modes'][spec.market]
    assert mode['entry_sizing_capital_twd'] == 0
    signal = json.loads(engine.signals_path.read_text().splitlines()[-1])
    assert signal['requested_shares'] == 0
    assert signal['reason'] == 'nonpositive_equity_no_new_exposure'


def test_dashboard_merges_overnight_counterfactual_history_without_minute_fabrication(tmp_path):
    for filename in ('signals.jsonl', 'orders.jsonl', 'fills.jsonl', 'marks.jsonl',
                     'benchmark_marks.jsonl', 'events.jsonl'):
        (tmp_path / filename).write_text('')
    (tmp_path / 'state.json').write_text(json.dumps({
        'product': 'tw_overnight',
        'modes': {'tw_overnight_unit': {
            'session_date': '2026-09-10', 'initial_capital_twd': 10_000_000,
        }},
    }))
    marks = [
        {'market': 'tw_overnight_unit', 'session_date': day,
         'minute': f'{day}T{clock}+08:00', 'initial_capital_twd': 10_000_000,
         'total_equity_twd': equity, 'historical_counterfactual_replay': True}
        for day, clock, equity in (
            ('2026-09-09', '09:00', 10_000_000),
            ('2026-09-09', '13:30', 10_100_000),
            ('2026-09-10', '09:00', 10_200_000),
            ('2026-09-10', '13:30', 10_150_000),
        )
    ]
    (tmp_path / 'overnight_history.json').write_text(json.dumps({
        'schema_version': 1, 'product': 'tw_overnight', 'simulation_only': True,
        'production_order_possible': False, 'marks': marks,
    }))
    pl.DataFrame([{
        'session_date': '2026-09-09', 'market': 'tw_overnight_unit',
        'symbol': '2330', 'signal_id': 'history-signal',
        'signal_at': '2026-09-09T13:25:00+08:00', 'target_weight': 0.1,
        'raw_score': 1.2, 'score': 0.8, 'side': 'long', 'status': 'working',
        'reason': None, 'requested_shares': 10_000, 'filled_shares': 0,
        'sizing_open_price': 100., 'sizing_capital_twd': 10_000_000.,
        'ask': None, 'bid': None, 'execution_price': None,
        'counterfactual_overnight_replay': True,
    }]).write_parquet(tmp_path / 'overnight_signal_history.parquet')
    pl.DataFrame([{
        'session_date': '2026-09-09', 'market': 'tw_overnight_unit',
        'symbol': '2330', 'event_kind': 'fill',
        'recorded_at': '2026-09-09T13:30:00+08:00',
        'fill_at': '2026-09-09T13:30:00+08:00', 'purpose': 'close_auction_entry',
        'price': 101., 'quantity': 10_000, 'simulation_only': True,
        'simulation_replay': True,
    }]).write_parquet(tmp_path / 'overnight_event_history.parquet')
    position_root = tmp_path / 'overnight_position_history' / '2026-09-09'
    position_root.mkdir(parents=True)
    (position_root / 'tw_overnight_unit.json').write_text(json.dumps({
        'session_date': '2026-09-09', 'market': 'tw_overnight_unit',
        'positions': [{'position_id': 'p1', 'session_date': '2026-09-09',
                       'market': 'tw_overnight_unit', 'symbol': '2330',
                       'filled_shares': 10_000, 'signed_shares': 0,
                       'counterfactual_overnight_replay': True}],
    }))

    history = build_dashboard_history_snapshot(
        state_dir=tmp_path, range_key='all', resolution='1m'
    )
    assert history['curve_granularity'] == 'auction_events'
    assert history['raw_points_in_range'] == 4
    assert history['range_summary'][0]['expected_points_per_session'] == 2
    assert history['range_summary'][0]['minute_coverage_ratio'] == 1
    signals = build_dashboard_signal_page(
        state_dir=tmp_path, start_date='2026-09-09', end_date='2026-09-09'
    )
    assert signals['total'] == 1
    assert signals['rows'][0]['counterfactual_overnight_replay'] is True
    positions = build_dashboard_position_page(
        state_dir=tmp_path, start_date='2026-09-09', end_date='2026-09-09'
    )
    assert positions['total'] == 1
    events = build_dashboard_event_page(
        state_dir=tmp_path, start_date='2026-09-09', end_date='2026-09-09'
    )
    assert events['fill_total'] == 1
