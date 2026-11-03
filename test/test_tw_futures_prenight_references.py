from datetime import date, datetime, timedelta

import polars as pl
import pytest

from scripts.audit_tw_futures_prenight_references import preliminary_candidates, last_second_proofs


def _coverage(day, *, product='TX', physical='TX:201101', source='a' * 64,
              verified=True, blocks=0, volume=1):
    return dict(date=day, physical_contract=physical, product=product, contract='201101',
                verified=verified, block_volume=blocks, outright_volume=volume,
                source_file_sha256=source)


def _event(day, hour, minute, *, opening, physical='TX:201101', source='a' * 64, price=100.):
    return dict(trade_date=day, physical_contract=physical, price=price,
                trade_ts=datetime(day.year, day.month, day.day, hour, minute),
                available_by=datetime(day.year, day.month, day.day, hour, minute + 1),
                source_sha256=source, is_session_open=opening)


def _reference(day, hour=8, minute=45, *, kind='unavailable'):
    return dict(date=day, physical_contract='TX:201101', symbol='slot',
                decision_ts=datetime(day.year, day.month, day.day, hour, minute), reference_kind=kind)


def test_candidate_requires_adjacent_day_and_never_overrides_open():
    previous, day = date(2011, 1, 3), date(2011, 1, 4)
    cov = pl.DataFrame([_coverage(previous), _coverage(day)])
    trades = pl.DataFrame([_event(previous, 13, 10, opening=False),
                           _event(day, 8, 45, opening=True)])
    refs = pl.DataFrame([_reference(day), _reference(day, 9, 0),
                         _reference(date(2011, 1, 6))])
    found = preliminary_candidates(refs, cov, cov, trades)
    assert found.height == 1
    assert found['decision_ts'].to_list() == [datetime(2011, 1, 4, 8, 45)]
    assert found['reference_trade_ts'].to_list() == [datetime(2011, 1, 3, 13, 10)]


@pytest.mark.parametrize('previous,day', [(date(2011, 1, 28), date(2011, 2, 8)),
                                           (date(2016, 2, 3), date(2016, 2, 15))])
def test_source_bound_early_holiday_uses_previous_session_without_overriding_open(previous, day):
    coverage = pl.DataFrame([_coverage(previous), _coverage(day)])
    trades = pl.DataFrame([_event(previous, 13, 10, opening=False),
                           _event(day, 8, 45, opening=True)])
    references = pl.DataFrame([_reference(day), _reference(day, 9, 0)])
    assert preliminary_candidates(references, coverage, coverage, trades).is_empty()
    candidate = preliminary_candidates(references, coverage, coverage, trades,
                                       early_holiday_sessions={day: previous})
    assert candidate['decision_ts'].to_list() == [datetime(day.year, day.month, day.day, 8, 45)]
    assert candidate['_previous_date'].to_list() == [previous]
    with pytest.raises(ValueError, match='separate pre-night'):
        preliminary_candidates(references, coverage, coverage, trades, weekend=True,
                               early_holiday_sessions={day: previous})
    with pytest.raises(ValueError, match='invalid early holiday'):
        preliminary_candidates(references, coverage, coverage, trades,
                               early_holiday_sessions={day: day})


def test_independent_early_holiday_readback_requires_reviewed_prior_date():
    import struct
    from scripts.verify_tw_futures_prenight_references import check_candidates

    previous, day = date(2011, 1, 28), date(2011, 2, 8)
    source = 'a' * 64
    row = dict(_reference(day), product='TX', contract='201101', _previous_date=previous,
               reference_trade_ts=datetime(2011, 1, 28, 13, 10), reference_quote=100.,
               reference_available_by=datetime(2011, 1, 28, 13, 11), reference_source_sha256=source)
    coverage = {(value, 'TX:201101'): _coverage(value) for value in (previous, day)}
    prior = {'first': '2011-01-28 09:00:00', 'last': '2011-01-28 13:10:00',
             'last_prices': {struct.pack('!f', 100.)}}
    original = {previous: {'sha256': source, 'rows': {('TX', '201101'): prior}},
                day: {'sha256': source, 'rows': {}}}
    assert check_candidates([row], coverage, coverage, original,
                            early_holiday_sessions={day: previous}) == []
    assert check_candidates([row], coverage, coverage, original,
                            early_holiday_sessions={day: date(2011, 1, 27)})[0]['reasons'] == ['date_or_availability']


def test_reviewed_early_holiday_notices_are_content_pinned(tmp_path, monkeypatch):
    import gzip
    import hashlib
    from scripts import tw_futures_early_holiday_evidence as evidence

    content = b'exact reviewed notice fixture'
    digest = hashlib.sha256(content).hexdigest()
    monkeypatch.setattr(evidence, 'REVIEWED', (
        (date(2010, 12, 16), date(2011, 1, 28), date(2011, 2, 8), digest),))
    path = tmp_path / 'raw' / digest[:2] / f'{digest}.body.gz'
    path.parent.mkdir(parents=True)
    path.write_bytes(gzip.compress(content))
    mapping, pins = evidence.early_holiday_closures(tmp_path)
    assert mapping == {date(2011, 2, 8): date(2011, 1, 28)}
    assert pins[0]['content_sha256'] == digest
    path.write_bytes(gzip.compress(b'different notice'))
    with pytest.raises(ValueError, match='original changed'):
        evidence.early_holiday_closures(tmp_path)


def test_reviewed_early_holiday_intervals_cannot_overlap(tmp_path, monkeypatch):
    import gzip
    import hashlib
    from scripts import tw_futures_early_holiday_evidence as evidence

    content = b'overlapping notice fixture'
    digest = hashlib.sha256(content).hexdigest()
    path = tmp_path / 'raw' / digest[:2] / f'{digest}.body.gz'
    path.parent.mkdir(parents=True)
    path.write_bytes(gzip.compress(content))
    monkeypatch.setattr(evidence, 'REVIEWED', (
        (date(2010, 12, 16), date(2011, 1, 28), date(2011, 2, 8), digest),
        (date(2011, 1, 30), date(2011, 2, 7), date(2011, 2, 9), digest)))
    with pytest.raises(ValueError, match='intervals overlap'):
        evidence.early_holiday_closures(tmp_path)


@pytest.mark.parametrize('cause', ['current_gap', 'prior_gap', 'current_block', 'prior_block', 'source_changed', 'night'])
def test_candidate_fails_closed_for_unverified_coverage_or_night(cause):
    previous, day = date(2011, 1, 3), date(2011, 1, 4)
    if cause == 'night':
        previous, day = date(2017, 5, 14), date(2017, 5, 15)
    before = _coverage(previous, verified=cause != 'prior_gap', blocks=int(cause == 'prior_block'))
    after = _coverage(day, verified=cause != 'current_gap', blocks=int(cause == 'current_block'))
    cov = pl.DataFrame([before, after])
    final = cov.with_columns(pl.when(pl.col('date') == day).then(pl.lit('b' * 64))
        .otherwise(pl.col('source_file_sha256')).alias('source_file_sha256')) if cause == 'source_changed' else cov
    trades = pl.DataFrame([_event(previous, 13, 10, opening=False), _event(day, 8, 45, opening=True)])
    assert preliminary_candidates(pl.DataFrame([_reference(day)]), cov, final, trades).is_empty()


def test_positive_day_with_missing_open_does_not_imply_no_trade():
    previous, day = date(2011, 1, 3), date(2011, 1, 4)
    cov = pl.DataFrame([_coverage(previous), _coverage(day)])
    trades = pl.DataFrame([_event(previous, 13, 10, opening=False)])
    assert preliminary_candidates(pl.DataFrame([_reference(day)]), cov, cov, trades).is_empty()


def test_last_second_requires_one_matching_real_price():
    targets = pl.DataFrame([
        dict(product='TX', contract='201101', reference_trade_ts=datetime(2011, 1, 3, 13, 10), reference_quote=100.),
        dict(product='MTX', contract='201101', reference_trade_ts=datetime(2011, 1, 3, 13, 10), reference_quote=200.),
        dict(product='TE', contract='201101', reference_trade_ts=datetime(2011, 1, 3, 13, 10), reference_quote=300.),
        dict(product='TF', contract='201101', reference_trade_ts=datetime(2011, 1, 3, 13, 10), reference_quote=400.),
    ])
    raw = pl.DataFrame([
        dict(futures_id='TX', contract_date='201101', date='2011-01-03 13:10:00', price=100., volume=2),
        dict(futures_id='TX', contract_date='201101', date='2011-01-03 13:10:00', price=100., volume=2),
        dict(futures_id='MTX', contract_date='201101', date='2011-01-03 13:10:00', price=200., volume=2),
        dict(futures_id='MTX', contract_date='201101', date='2011-01-03 13:10:00', price=201., volume=2),
        dict(futures_id='TE', contract_date='201101', date='2011-01-03 13:10:00', price=300., volume=0),
        dict(futures_id='TF', contract_date='201101', date='2011-01-03 13:10:00', price=400., volume=2),
        dict(futures_id='TF', contract_date='201101', date='2011-01-03 13:11:00', price=401., volume=2),
    ])
    got = {r['product']: r['last_second_proven'] for r in last_second_proofs(raw, targets).to_dicts()}
    assert got == {'TX': True, 'MTX': False, 'TE': False, 'TF': False}


def test_independent_reader_detects_a_trade_available_before_decision(tmp_path):
    import struct
    from scripts.verify_tw_futures_prenight_references import raw_extrema, check_candidates

    previous, day = date(2011, 1, 3), date(2011, 1, 4)
    source = 'a' * 64
    raw = pl.DataFrame([
        dict(futures_id='TX', contract_date='201101', date='2011-01-04 08:43:59', price=101., volume=1),
        dict(futures_id='TX', contract_date='201101', date='2011-01-04 08:44:00', price=102., volume=1),
    ])
    path = tmp_path / 'raw.parquet'
    raw.write_parquet(path)
    actual = raw_extrema(path, {('TX', '201101')})
    row = dict(_reference(day), product='TX', contract='201101', _previous_date=previous,
               reference_trade_ts=datetime(2011, 1, 3, 13, 10), reference_quote=100.,
               reference_available_by=datetime(2011, 1, 3, 13, 11), reference_source_sha256=source)
    c = {(value, 'TX:201101'): _coverage(value) for value in (previous, day)}
    old = {'first': '2011-01-03 09:00:00', 'last': '2011-01-03 13:10:00',
           'last_prices': {struct.pack('!f', 100.)}}
    originals = {previous: {'sha256': source, 'rows': {('TX', '201101'): old}},
                 day: {'sha256': source, 'rows': actual}}
    failures = check_candidates([row], c, c, originals)
    assert failures[0]['reasons'] == ['current_trade_available']
    row['decision_ts'] = datetime(2011, 1, 4, 8, 43)
    assert check_candidates([row], c, c, originals) == []


def test_raw_reader_prefilter_preserves_exact_physical_contract_and_last_second(tmp_path):
    import struct
    from scripts.verify_tw_futures_prenight_references import raw_extrema

    path = tmp_path / 'raw.parquet'
    pl.DataFrame([
        dict(futures_id='TX', contract_date='201101', date='2011-01-03 09:00:00', price=90., volume=1.),
        dict(futures_id='TX', contract_date='201101', date='2011-01-03 13:10:00', price=100., volume=1.),
        dict(futures_id='TX', contract_date='201101', date='2011-01-03 13:10:00', price=101., volume=1.),
        dict(futures_id='TX', contract_date='201101', date='2011-01-03 13:11:00', price=102., volume=0.),
        dict(futures_id='MTX', contract_date='201102', date='2011-01-03 13:00:00', price=50., volume=float('nan')),
        # Both fields match the individual prefilters, but the pair is not requested.
        dict(futures_id='TX', contract_date='201102', date='bad', price=100., volume=1.),
        dict(futures_id='other', contract_date='201101', date='bad', price=100., volume=1.),
        dict(futures_id=None, contract_date='201101', date='bad', price=100., volume=1.),
    ]).write_parquet(path)
    got = raw_extrema(path, {('TX', '201101'), ('MTX', '201102')})
    assert got == {
        ('TX', '201101'): {'first': '2011-01-03 09:00:00', 'last': '2011-01-03 13:10:00',
                           'last_prices': {struct.pack('!f', 100.), struct.pack('!f', 101.)}},
        ('MTX', '201102'): {'first': '2011-01-03 13:00:00', 'last': '2011-01-03 13:00:00',
                            'last_prices': {struct.pack('!f', 50.)}},
    }
    with pytest.raises(ValueError, match='timestamp'):
        raw_extrema(path, {('TX', '201102')})
    assert raw_extrema(path, set()) == {}


def test_reference_union_counts_each_contract_day_once_and_rejects_duplicate_clocks():
    from scripts.audit_tw_futures_prenight_union import summarize

    day = date(2019, 1, 7)
    rows = [
        dict(date=day, physical_contract='TX:201901', decision_ts=datetime(2019, 1, 7, 8, 45),
             product='TX', regime='weekend'),
        dict(date=day, physical_contract='TX:201901', decision_ts=datetime(2019, 1, 7, 9, 0),
             product='TX', regime='weekend'),
        dict(date=day, physical_contract='MTX:201901', decision_ts=datetime(2019, 1, 7, 8, 45),
             product='MTX', regime='other'),
    ]
    got = summarize(pl.DataFrame(rows))
    assert got['candidate_rows'] == 3
    assert got['distinct_physical_contract_days'] == 2
    assert got['physical_contract_days_at_multiple_clocks'] == 1
    assert got['distinct_products'] == 2
    with pytest.raises(ValueError, match='overlap'):
        summarize(pl.DataFrame(rows + [dict(rows[0], regime='other')]))


def test_afterhours_zero_requires_an_explicit_unique_matching_original_row():
    import io
    from scripts.audit_tw_futures_zero_afterhours import verify_original_rows

    header = '交易日期,契約,到期月份(週別),交易時段,成交量\n'
    good = '2024/03/01,CDF,202403,盤後,0\n'
    expected = {('2024-03-01', 'CDF', '202403')}
    assert verify_original_rows(io.StringIO(header + good), expected) == 1
    assert verify_original_rows(io.StringIO(header + good.replace('2024/03/01', '2024/3/1')), expected) == 1
    for body in (good + good, good.replace(',0\n', ',1\n'), good.replace(',0\n', ',-\n'), ''):
        with pytest.raises(ValueError):
            verify_original_rows(io.StringIO(header + body), expected)
    # A normal-session zero is not evidence about the after-hours session.
    with pytest.raises(ValueError, match='not found'):
        verify_original_rows(io.StringIO(header + good.replace('盤後', '一般')), expected)


def test_zero_afterhours_fallback_requires_exact_physical_day_and_keeps_open_precedence():
    previous, day = date(2020, 1, 2), date(2020, 1, 3)
    coverage = pl.DataFrame([_coverage(previous), _coverage(day)])
    trades = pl.DataFrame([_event(previous, 13, 10, opening=False),
                           _event(day, 8, 45, opening=True)])
    refs = pl.DataFrame([_reference(day), _reference(day, 9, 0)])
    zero = pl.DataFrame([dict(date=day, product='TX', contract='201101', source_sha256='b' * 64)])
    actual = preliminary_candidates(refs, coverage, coverage, trades, zero_afterhours=zero)
    assert actual.height == 1
    assert actual['decision_ts'].to_list() == [datetime(2020, 1, 3, 8, 45)]
    assert actual['night_zero_source_sha256'].to_list() == ['b' * 64]
    wrong_contract = zero.with_columns(pl.lit('201102').alias('contract'))
    assert preliminary_candidates(refs, coverage, coverage, trades, zero_afterhours=wrong_contract).is_empty()
    with pytest.raises(ValueError, match='unique'):
        preliminary_candidates(refs, coverage, coverage, trades,
                               zero_afterhours=pl.concat([zero, zero]))


def test_zero_afterhours_weekend_requires_documented_monday_rule():
    previous, day = date(2020, 1, 3), date(2020, 1, 6)
    coverage = pl.DataFrame([_coverage(previous), _coverage(day)])
    trades = pl.DataFrame([_event(previous, 13, 10, opening=False),
                           _event(day, 8, 45, opening=True)])
    zero = pl.DataFrame([dict(date=day, product='TX', contract='201101', source_sha256='b' * 64)])
    refs = pl.DataFrame([_reference(day)])
    assert preliminary_candidates(refs, coverage, coverage, trades, zero_afterhours=zero).is_empty()
    assert preliminary_candidates(refs, coverage, coverage, trades, weekend=True,
                                  zero_afterhours=zero).height == 1


def test_independent_zero_afterhours_reader_rejects_missing_and_wrong_source():
    import struct
    from scripts.verify_tw_futures_prenight_references import check_candidates

    previous, day, source = date(2020, 1, 2), date(2020, 1, 3), 'a' * 64
    row = dict(_reference(day), product='TX', contract='201101', _previous_date=previous,
               reference_trade_ts=datetime(2020, 1, 2, 13, 10), reference_quote=100.,
               reference_available_by=datetime(2020, 1, 2, 13, 11), reference_source_sha256=source,
               night_zero_source_sha256='b' * 64)
    coverage = {(value, 'TX:201101'): _coverage(value) for value in (previous, day)}
    old = {'first': '2020-01-02 09:00:00', 'last': '2020-01-02 13:10:00',
           'last_prices': {struct.pack('!f', 100.)}}
    originals = {previous: {'sha256': source, 'rows': {('TX', '201101'): old}},
                 day: {'sha256': source, 'rows': {}}}
    key = (day, 'TX', '201101')
    assert check_candidates([row], coverage, coverage, originals,
                            zero_afterhours={key: 'b' * 64}) == []
    for zero in ({}, {key: 'c' * 64}):
        failures = check_candidates([row], coverage, coverage, originals, zero_afterhours=zero)
        assert failures[0]['reasons'] == ['date_or_availability']


@pytest.mark.parametrize(('day', 'asset_class', 'prior_class', 'allowed'), [
    (date(2017, 5, 14), 'stock_future', 'stock_future', False),
    (date(2017, 5, 16), 'stock_future', 'stock_future', True),
    (date(2024, 1, 21), 'stock_future', 'stock_future', True),
    (date(2024, 1, 22), 'stock_future', 'stock_future', False),
    (date(2022, 6, 26), 'etf_future', 'etf_future', True),
    (date(2022, 6, 27), 'etf_future', 'etf_future', False),
    (date(2020, 1, 2), 'index_future', 'index_future', False),
    (date(2020, 1, 2), 'stock_future', 'etf_future', False),
    (date(2020, 1, 2), 'stock_future', None, False),
])
def test_equity_day_only_policy_requires_matching_dated_classification(day, asset_class, prior_class, allowed):
    previous = day - timedelta(days=1)
    cov = pl.DataFrame([_coverage(previous), _coverage(day)])
    trades = pl.DataFrame([_event(previous, 13, 10, opening=False), _event(day, 8, 45, opening=True)])
    classification = pl.DataFrame([
        {'date': previous, 'physical_contract': 'TX:201101', 'asset_class': prior_class},
        {'date': day, 'physical_contract': 'TX:201101', 'asset_class': asset_class},
    ])
    found = preliminary_candidates(pl.DataFrame([_reference(day)]), cov, cov, trades, classification)
    assert found.height == int(allowed)
    if allowed:
        assert found['asset_class'].to_list() == [asset_class]


def test_independent_equity_policy_rejects_mismatched_classification():
    import struct
    from scripts.verify_tw_futures_prenight_references import check_candidates

    day, previous, source = date(2020, 1, 2), date(2020, 1, 1), 'a' * 64
    row = dict(_reference(day), asset_class='stock_future', product='TX', contract='201101',
               _previous_date=previous, reference_trade_ts=datetime(2020, 1, 1, 13, 10),
               reference_quote=100., reference_available_by=datetime(2020, 1, 1, 13, 11),
               reference_source_sha256=source)
    coverage = {(value, 'TX:201101'): _coverage(value) for value in (previous, day)}
    old = {'first': '2020-01-01 09:00:00', 'last': '2020-01-01 13:10:00',
           'last_prices': {struct.pack('!f', 100.)}}
    originals = {previous: {'sha256': source, 'rows': {('TX', '201101'): old}},
                 day: {'sha256': source, 'rows': {}}}
    classes = {(previous, 'TX:201101'): 'stock_future', (day, 'TX:201101'): 'etf_future'}
    assert check_candidates([row], coverage, coverage, originals, classes)[0]['reasons'] == ['date_or_availability']
    classes[day, 'TX:201101'] = 'stock_future'
    assert check_candidates([row], coverage, coverage, originals, classes) == []


@pytest.mark.parametrize(('day', 'asset_class', 'allowed'), [
    (date(2018, 12, 24), 'stock_future', False),
    (date(2019, 1, 7), 'stock_future', True),
    (date(2019, 1, 8), 'stock_future', False),
    (date(2022, 6, 27), 'etf_future', False),
    (date(2024, 1, 22), 'stock_future', False),
    (date(2020, 1, 6), 'index_future', False),
])
def test_weekend_policy_requires_monday_and_2019_equity_day_only_scope(day, asset_class, allowed):
    previous = day - timedelta(days=3)
    cov = pl.DataFrame([_coverage(previous), _coverage(day)])
    trades = pl.DataFrame([_event(previous, 13, 10, opening=False), _event(day, 8, 45, opening=True)])
    classification = pl.DataFrame([
        {'date': previous, 'physical_contract': 'TX:201101', 'asset_class': asset_class},
        {'date': day, 'physical_contract': 'TX:201101', 'asset_class': asset_class},
    ])
    found = preliminary_candidates(pl.DataFrame([_reference(day)]), cov, cov, trades, classification, weekend=True)
    assert found.height == int(allowed)


def test_independent_reader_rejects_incorrect_physical_source_identity():
    import struct
    from scripts.verify_tw_futures_prenight_references import check_candidates

    day, previous, source = date(2020, 1, 6), date(2020, 1, 3), 'a' * 64
    row = dict(_reference(day), asset_class='stock_future', product='TX', contract='201101',
               _previous_date=previous, reference_trade_ts=datetime(2020, 1, 3, 13, 10),
               reference_quote=100., reference_available_by=datetime(2020, 1, 3, 13, 11),
               reference_source_sha256=source)
    coverage = {(value, 'TX:201101'): _coverage(value) for value in (previous, day)}
    old = {'first': '2020-01-03 09:00:00', 'last': '2020-01-03 13:10:00',
           'last_prices': {struct.pack('!f', 100.)}}
    originals = {previous: {'sha256': source, 'rows': {('TX', '201101'): old}},
                 day: {'sha256': source, 'rows': {}}}
    classes = {(value, 'TX:201101'): 'stock_future' for value in (previous, day)}
    assert check_candidates([row], coverage, coverage, originals, classes, weekend=True) == []
    coverage[day, 'TX:201101']['product'] = 'OTHER'
    assert check_candidates([row], coverage, coverage, originals, classes, weekend=True)[0]['reasons'] == ['physical_identity']
    with pytest.raises(ValueError, match='weekend policy'):
        preliminary_candidates(pl.DataFrame([_reference(day)]), pl.DataFrame([_coverage(previous), _coverage(day)]),
                               pl.DataFrame([_coverage(previous), _coverage(day)]), pl.DataFrame([]), weekend=True)
