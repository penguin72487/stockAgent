from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from stockagent.data.tw_option_reference_candidates import compact_last_observations, reference_candidates


TZ = ZoneInfo('Asia/Taipei')
DAY = date(2026, 10, 6)
WINDOWS = {key: (525, 825) for key in ('AAO', 'BBO', 'CCO', 'DDO', 'EEO')}


def events():
    samples = [
        ('AAO', '2026-10-05T13:30:00', '5.0'),
        ('AAO', '2026-10-06T08:45:00', '6.00'),
        ('AAO', '2026-10-06T08:45:00', '6.0'),  # two sides, same decimal
        ('AAO', '2026-10-06T08:50:00', '7'),
        ('BBO', '2026-10-05T13:30:00', '10'),
        ('BBO', '2026-10-06T08:46:01', '8'),
        ('BBO', '2026-10-06T08:46:01', '9'),  # first-second order unknown
        ('CCO', '2026-10-05T13:30:00', '10'),
        ('CCO', '2026-10-05T13:30:00', '11'),  # last-second order unknown
        ('DDO', '2026-10-06T09:00:00', '3'),  # available after 09:00
        ('EEO', '2026-10-06T08:59:59', '2'),  # available at 09:00
    ]
    return pl.DataFrame([{'product_raw': p, 'series_raw': '202610', 'strike_raw': '20.00',
                          'right_raw': 'C', 'event_ts': datetime.fromisoformat(ts).replace(tzinfo=TZ),
                          'price_raw': price, 'source_sha256': 'a' * 64} for p, ts, price in samples])


def by_product(frame):
    return {row['product_raw']: row for row in frame.to_dicts()}


def test_completed_second_opening_and_last_trade_cannot_see_future():
    raw = events()
    a = by_product(reference_candidates(raw, trading_date=DAY, decision_time='08:45', session_windows=WINDOWS))
    assert a['AAO']['reference_kind'] == 'last_observed_trade_candidate'
    assert a['AAO']['reference_price_decimal'] == '5'
    assert a['CCO']['reference_kind'] == 'ambiguous_last_second'
    assert a['CCO']['reference_price_decimal'] is None
    assert 'DDO' not in a and 'EEO' not in a
    b = by_product(reference_candidates(raw, trading_date=DAY, decision_time='09:00', session_windows=WINDOWS))
    assert b['AAO']['reference_kind'] == 'observed_session_open_candidate'
    assert b['AAO']['reference_price_decimal'] == '6'
    assert b['BBO']['reference_kind'] == 'ambiguous_first_second'
    assert b['BBO']['reference_price_decimal'] is None
    assert 'DDO' not in b
    assert b['EEO']['reference_price_decimal'] == '2'
    assert b['EEO']['available_by'].isoformat() == '2026-10-06T09:00:00+08:00'
    # Neither an altered 09:00 print nor a later price can change 09:00 output.
    changed = raw.with_columns(pl.when(pl.col('product_raw') == 'DDO').then(pl.lit('999'))
                               .otherwise(pl.col('price_raw')).alias('price_raw'))
    assert reference_candidates(changed, trading_date=DAY, decision_time='09:00', session_windows=WINDOWS).equals(
        reference_candidates(raw, trading_date=DAY, decision_time='09:00', session_windows=WINDOWS))


def test_invalid_known_price_or_missing_timestamp_fails_closed():
    raw = events().with_columns(pl.when(pl.col('product_raw') == 'AAO').then(pl.lit('NaN'))
                                .otherwise(pl.col('price_raw')).alias('price_raw'))
    with pytest.raises(ValueError, match='invalid native price_raw'):
        reference_candidates(raw, trading_date=DAY, decision_time='08:45', session_windows=WINDOWS)
    raw = events().with_columns(pl.when(pl.col('product_raw') == 'AAO').then(None)
                                .otherwise(pl.col('event_ts')).alias('event_ts'))
    with pytest.raises(ValueError, match='source identity'):
        reference_candidates(raw, trading_date=DAY, decision_time='08:45', session_windows=WINDOWS)


def test_previous_source_compaction_preserves_ties_and_future_decision_results():
    original = events()
    prior = original.filter(pl.col('event_ts').dt.date() < DAY)
    today = original.filter(pl.col('event_ts').dt.date() == DAY)
    compacted = compact_last_observations(prior)
    today = today.select(compacted.columns)
    # Identical source side duplicates can collapse only in the derived state;
    # two different prices in the same last second must both survive.
    assert compacted.filter(pl.col('product_raw') == 'CCO').height == 2
    for clock in ('08:45', '09:00'):
        full = reference_candidates(original, trading_date=DAY, decision_time=clock, session_windows=WINDOWS)
        reduced = reference_candidates(pl.concat([compacted, today]), trading_date=DAY,
                                       decision_time=clock, session_windows=WINDOWS)
        assert full.equals(reduced)
    assert compact_last_observations(pl.concat([prior, prior])).equals(compacted)


def test_previous_source_compaction_rejects_unknown_identity():
    prior = events().filter(pl.col('event_ts').dt.date() < DAY)
    with pytest.raises(ValueError, match='identity'):
        compact_last_observations(prior.with_columns(pl.lit(None, dtype=pl.Datetime('us', 'Asia/Taipei'))
                                                    .alias('event_ts')))


def test_two_day_window_matches_all_source_rows_and_rejects_future_prior():
    from scripts.build_taifex_option_reference_source_window import SOURCE_COLUMNS, derive_day

    original = events().select(*SOURCE_COLUMNS)
    yesterday = original.filter(pl.col('event_ts').dt.date() < DAY)
    today = original.filter(pl.col('event_ts').dt.date() == DAY)
    prior = compact_last_observations(yesterday)
    result, next_state = derive_day(prior, today, DAY, WINDOWS)
    for clock in ('08:45', '09:00'):
        expected = reference_candidates(original, trading_date=DAY, decision_time=clock,
                                        session_windows=WINDOWS)
        got = result.filter(pl.col('decision_time') == clock).drop('decision_date', 'decision_time')
        assert got.equals(expected)
    assert next_state.equals(compact_last_observations(original))
    with pytest.raises(ValueError, match='crosses the decision date'):
        derive_day(next_state, today, DAY, WINDOWS)
    with pytest.raises(ValueError, match='without candidate windows'):
        derive_day(prior, today, DAY, {'AAO': (525, 825)})
