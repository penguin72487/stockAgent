from datetime import date
import hashlib
import json

import polars as pl
import pytest

from scripts import audit_taifex_finmind_futures_daily_priority_presence as auditor


def _official():
    return pl.DataFrame({'date': [date(2023, 2, 2)] * 3, 'product': ['GDF'] * 3,
        'contract': ['202302', '202304', '202306'], 'session': ['一般'] * 3,
        'session_reported': [True, True, False], 'open': [1., 2., 3.],
        'high': [2., 3., 4.], 'low': [0.5, 1.5, 2.5], 'close': [1.5, 2.5, 3.5],
        'volume': [3, 4, 5], 'source_file': ['s.csv'] * 3, 'source_sha256': ['sha'] * 3})


def _vendor():
    return pl.DataFrame({'date': ['2023-02-02'], 'futures_id': ['GDF'],
        'contract_date': ['202302'], 'trading_session': ['position'], 'open': [1.], 'volume': [3]})


def _sponsor(close=2.5):
    return pl.DataFrame({'date': ['2023-02-02'], 'futures_id': ['GDF'],
        'contract_date': ['202304'], 'trading_session': ['position'], 'open': [2.],
        'max': [3.], 'min': [1.5], 'close': [close], 'volume': [4]})


def test_only_reported_session_keys_are_compared_and_fallback_is_exact():
    summary, missing = auditor.compare(_official(), _vendor())
    assert summary == {'official_positive_reported_session_keys': 2,
                       'official_positive_unreported_session_keys': 1,
                       'vendor_reported_session_keys_present': 1,
                       'vendor_reported_session_keys_absent': 1,
                       'vendor_present_but_not_positive_keys': 0}
    found = auditor._match_sponsor(missing, {'2023-02-02': _sponsor()})
    assert found[0]['contract'] == '202304' and found[0]['all_market_status'] == 'exact_ohlcv_match'
    assert auditor._match_sponsor(missing, {'2023-02-02': _sponsor(close=2.6)})[0]['all_market_status'] == 'ohlcv_conflict'
    assert auditor._match_sponsor(missing, {})[0]['all_market_status'] == 'receipt_unavailable'
    unknown_null = _official().with_columns(
        pl.when(pl.col('contract') == '202306').then(None).otherwise(pl.col('session_reported')).alias('session_reported'))
    assert auditor.compare(unknown_null, _vendor())[0]['official_positive_unreported_session_keys'] == 1


def test_zero_is_not_counted_as_absent_and_duplicate_or_unknown_session_fails():
    vendor = _vendor().with_columns(pl.lit(0).alias('volume'))
    summary, _ = auditor.compare(_official(), vendor)
    assert summary['vendor_reported_session_keys_absent'] == 1
    assert summary['vendor_present_but_not_positive_keys'] == 1
    vendor_null = vendor.with_columns(pl.lit(None, dtype=pl.Int64).alias('volume'))
    assert auditor.compare(_official(), vendor_null)[0]['vendor_present_but_not_positive_keys'] == 1
    vendor_nan = vendor.with_columns(pl.lit(float('nan')).alias('open'))
    assert auditor.compare(_official(), vendor_nan)[0]['vendor_present_but_not_positive_keys'] == 1
    with pytest.raises(ValueError, match='duplicate physical session'):
        auditor.compare(_official(), pl.concat([vendor, vendor]))
    with pytest.raises(ValueError, match='unsupported key'):
        auditor.compare(_official(), vendor.with_columns(pl.lit('unknown').alias('trading_session')))


def test_pinned_source_rejects_changed_bytes(tmp_path):
    path = tmp_path / 'source.json'
    path.write_text(json.dumps({'date': '2023-02-02'}))
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    assert json.loads(auditor._read_pinned(path, sha))['date'] == '2023-02-02'
    path.write_text('{}')
    with pytest.raises(ValueError, match='hash mismatch'):
        auditor._read_pinned(path, sha)
