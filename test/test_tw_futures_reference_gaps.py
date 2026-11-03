from datetime import date, datetime

import polars as pl
import pytest

from stockagent.data.tw_futures_decision_clock import opening_reference_contract, resolve_opening_references
from stockagent.data.tw_futures_reference_gaps import (quarantine_reference_gaps,
    verify_partial_current_openings, withhold_uncertified_fallbacks)


def _events():
    return pl.DataFrame([
        {'physical_contract': 'P:202601', 'price': 100., 'trade_date': date(2026, 1, 2),
         'trade_ts': datetime(2026, 1, 2, 13, 30), 'available_by': datetime(2026, 1, 2, 13, 31),
         'source_sha256': 'a' * 64, 'is_session_open': False},
        {'physical_contract': 'P:202601', 'price': 102., 'trade_date': date(2026, 1, 4),
         'trade_ts': datetime(2026, 1, 4, 8, 45), 'available_by': datetime(2026, 1, 4, 8, 46),
         'source_sha256': 'b' * 64, 'is_session_open': True},
        {'physical_contract': 'P:202601', 'price': 103., 'trade_date': date(2026, 1, 4),
         'trade_ts': datetime(2026, 1, 4, 13, 30), 'available_by': datetime(2026, 1, 4, 13, 31),
         'source_sha256': 'b' * 64, 'is_session_open': False},
        {'physical_contract': 'Q:202601', 'price': 200., 'trade_date': date(2026, 1, 2),
         'trade_ts': datetime(2026, 1, 2, 13, 30), 'available_by': datetime(2026, 1, 2, 13, 31),
         'source_sha256': 'c' * 64, 'is_session_open': False},
        {'physical_contract': 'R:202601', 'price': 300., 'trade_date': date(2026, 1, 1),
         'trade_ts': datetime(2026, 1, 1, 13, 30), 'available_by': datetime(2026, 1, 1, 13, 31),
         'source_sha256': 'd' * 64, 'is_session_open': False},
    ])


def test_unverified_day_withholds_stale_reference_and_prior_denominator():
    decisions = pl.DataFrame([
        {'date': date(2026, 1, 3), 'physical_contract': 'P:202601'},
        {'date': date(2026, 1, 4), 'physical_contract': 'P:202601'},
        {'date': date(2026, 1, 5), 'physical_contract': 'P:202601'},
        {'date': date(2026, 1, 6), 'physical_contract': 'P:202601'},
        {'date': date(2026, 1, 4), 'physical_contract': 'Q:202601'},
        {'date': date(2026, 1, 4), 'physical_contract': 'R:202601'},
    ])
    gaps = pl.DataFrame([
        {'date': date(2026, 1, 3), 'physical_contract': 'P:202601'},
        {'date': date(2026, 1, 6), 'physical_contract': 'P:202601'},
    ])
    trades = _events()
    old = resolve_opening_references(decisions, trades, decision_time='09:00')
    safe = quarantine_reference_gaps(old, trades, gaps, coverage_start=date(2026, 1, 2))
    rows = {(r['date'], r['physical_contract']): r for r in safe.to_dicts()}
    assert rows[date(2026, 1, 3), 'P:202601']['reference_kind'] == 'unavailable'
    assert rows[date(2026, 1, 3), 'P:202601']['reference_quote'] is None
    opening = rows[date(2026, 1, 4), 'P:202601']
    assert opening['reference_kind'] == 'observed_session_open' and opening['reference_quote'] == 102.
    assert opening['prior_trade_quote'] is None and opening['opening_reference_log_return_from_prior_trade'] == 0.
    resumed = rows[date(2026, 1, 5), 'P:202601']
    assert resumed['reference_kind'] == 'last_observed_trade' and resumed['reference_quote'] == 103.
    assert resumed['prior_trade_quote'] == 103.
    assert rows[date(2026, 1, 6), 'P:202601']['reference_kind'] == 'unavailable'
    assert rows[date(2026, 1, 4), 'Q:202601']['reference_quote'] == 200.
    assert rows[date(2026, 1, 4), 'R:202601']['reference_kind'] == 'unavailable'
    assert safe.select('opening_reference_available').to_series().to_list().count(0.) == 3


def test_unverified_same_day_cannot_be_declared_a_verified_open():
    decisions = pl.DataFrame([{'date': date(2026, 1, 4), 'physical_contract': 'P:202601'}])
    trades = _events()
    old = resolve_opening_references(decisions, trades, decision_time='09:00')
    gaps = decisions.clone()
    safe = quarantine_reference_gaps(old, trades, gaps, coverage_start=date(2026, 1, 2)).row(0, named=True)
    assert safe['reference_kind'] == 'unavailable' and safe['reference_quote'] is None
    assert safe['opening_reference_available'] == safe['opening_reference_is_session_open'] == 0.


def test_empty_gap_set_still_withholds_history_older_than_the_axis():
    decisions = pl.DataFrame([{'date': date(2026, 1, 4), 'physical_contract': 'R:202601'}])
    trades = _events()
    old = resolve_opening_references(decisions, trades, decision_time='09:00')
    gaps = pl.DataFrame(schema={'date': pl.Date, 'physical_contract': pl.String})
    safe = quarantine_reference_gaps(old, trades, gaps, coverage_start=date(2026, 1, 2)).row(0, named=True)
    assert safe['reference_kind'] == 'unavailable' and safe['prior_trade_quote'] is None


def test_regular_session_evidence_alone_does_not_certify_latest_night_trade():
    decisions = pl.DataFrame([
        {'date': date(2026, 1, 4), 'physical_contract': 'P:202601'},
        {'date': date(2026, 1, 4), 'physical_contract': 'Q:202601'},
    ])
    trades = _events().with_columns(pl.col('is_session_open').alias('opening_order_proven'))
    old = resolve_opening_references(decisions, trades, decision_time='09:00')
    safe = {r['physical_contract']: r for r in withhold_uncertified_fallbacks(old, trades).to_dicts()}
    assert safe['Q:202601']['reference_kind'] == 'unavailable'
    assert safe['Q:202601']['reference_quote'] is None
    assert safe['P:202601']['reference_kind'] == 'observed_session_open'
    assert safe['P:202601']['reference_quote'] == 102. and safe['P:202601']['prior_trade_quote'] is None


def test_later_daily_open_does_not_resolve_an_unordered_first_second_at_decision():
    decisions = pl.DataFrame([{'date': date(2026, 1, 4), 'physical_contract': 'P:202601'}])
    trades = _events().with_columns(pl.lit(False).alias('opening_order_proven'))
    legacy = resolve_opening_references(decisions, trades, decision_time='09:00')
    assert legacy['reference_kind'].to_list() == ['observed_session_open']
    safe = withhold_uncertified_fallbacks(legacy, trades).row(0, named=True)
    assert safe['reference_kind'] == 'unavailable' and safe['reference_quote'] is None


@pytest.mark.parametrize('failure', ['none', 'unordered', 'uncovered', 'gap', 'future'])
def test_partial_opening_needs_causal_event_order_and_verified_coverage(failure):
    decisions = pl.DataFrame([{'date': date(2026, 1, 4), 'physical_contract': 'P:202601'}])
    trades = _events().with_columns(pl.col('is_session_open').alias('opening_order_proven'))
    refs = withhold_uncertified_fallbacks(resolve_opening_references(
        decisions, trades, decision_time='09:00'), trades)
    coverage = pl.DataFrame([{'date': date(2026, 1, 4), 'physical_contract': 'P:202601',
        'source_file_sha256': 'b' * 64, 'verified': failure != 'uncovered'}])
    gaps = (decisions if failure == 'gap' else
            pl.DataFrame(schema={'date': pl.Date, 'physical_contract': pl.String}))
    if failure == 'unordered':
        trades = trades.with_columns(pl.lit(False).alias('opening_order_proven'))
    if failure == 'future':
        refs = refs.with_columns(pl.lit(datetime(2026, 1, 4, 9, 1)).alias('reference_available_by'))
    if failure == 'none':
        verify_partial_current_openings(refs, trades, coverage, gaps)
    else:
        with pytest.raises(ValueError, match='partial reference'):
            verify_partial_current_openings(refs, trades, coverage, gaps)


def test_gap_contract_is_versioned_and_cannot_bypass_preparation():
    assert opening_reference_contract('09:00', version=3) != opening_reference_contract('09:00')
    with pytest.raises(ValueError, match='gap-aware'):
        resolve_opening_references(pl.DataFrame(), pl.DataFrame(), decision_time='09:00', version=3)


@pytest.mark.parametrize('unique_first_minute', [True, False])
def test_clock_builder_publishes_gap_aware_reference_and_paired_identity(tmp_path, monkeypatch, unique_first_minute):
    import json
    from types import SimpleNamespace
    import scripts.prepare_tw_futures_decision_clocks as builder
    import stockagent.data.tw_futures_margin as margin
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_futures_decision_clock import QUARANTINE_CONTRACT, validate_opening_reference_source

    days = [date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 4)]
    daily = tmp_path / 'daily.parquet'
    pl.DataFrame(dict(date=days, product=['CAF'] * 3, contract=['202601'] * 3,
        asset_class=['stock_future'] * 3, physical_contract=['CAF:202601'] * 3,
        symbol=['slot'] * 3, open=[99., 101., 102.], high=[101., 102., 103.],
        low=[99., 100., 101.], close=[100., 101., 102.], volume=[2., 2., 2.],
        executable=[True] * 3, observed_open=[99., 101., 102.], observed_close=[100., 101., 102.],
        source_row_observed=[True] * 3, official_source_sha256=['a' * 64] * 3)).write_parquet(daily)
    rules = tmp_path / 'rules.parquet'; rules.write_bytes(b'test rules')
    quarantine = tmp_path / 'quarantine.json'
    quarantine.write_text(json.dumps(dict(contract=QUARANTINE_CONTRACT,
        source_daily_sha256=sha256_file(daily), source_rules_sha256=sha256_file(rules),
        contract_days=[dict(date='2026-01-03', physical_contract='CAF:202601', reason='missing_source')])))
    minutes = pl.DataFrame(dict(date=[days[0], days[2]], physical_contract=['CAF:202601'] * 2,
        minute=[539, 539], vwap=[99., 102.], high=[99., 102. if unique_first_minute else 103.],
        low=[99., 102. if unique_first_minute else 101.],
        close=[99., 102.], volume=[1., 1.], source_file_sha256=['b' * 64] * 2))
    verified = minutes.select('date', 'physical_contract')
    monkeypatch.setattr(margin, 'validate_margin_rule_source', lambda *args: (None, {}))
    monkeypatch.setattr(builder, 'load_verified_history', lambda *args, **kwargs: (minutes, verified, {'test': True}))
    args = SimpleNamespace(daily=daily, rules=rules, output=tmp_path / 'output',
        start='2026-01-02', end='2026-01-04', history_root=tmp_path / 'history',
        tick_root=[], exact_kbar_root=[], finmind_bulk_root=None, with_opening_reference=True,
        execution_quarantine=quarantine)
    assert builder.build(args) == 0
    directory = args.output / '0900'
    entries = json.loads((directory / 'manifest.json').read_text())
    reference = json.loads((directory / 'opening_reference_manifest.json').read_text())
    assert reference['contract'] == opening_reference_contract('09:00', version=3)
    assert reference['pair_identity'] == entries['pair_identity']
    rows, _ = validate_opening_reference_source(directory / 'opening_reference.parquet',
        daily_path=daily, decision_time='09:00', entry_manifest=entries)
    by_day = {r['date']: r for r in rows.to_dicts()}
    assert by_day[days[1]]['reference_kind'] == 'unavailable'
    assert by_day[days[2]]['reference_kind'] == ('observed_session_open' if unique_first_minute else 'unavailable')
    assert by_day[days[2]]['prior_trade_quote'] is None
    entries_frame = pl.read_parquet(directory / 'entries.parquet')
    assert entries_frame.filter(pl.col('date') == days[1])['status'].to_list() == ['unverified_execution_quarantined']
