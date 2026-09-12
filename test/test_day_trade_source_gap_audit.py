from datetime import date, datetime
import hashlib
import json

import pytest
import polars as pl

from scripts.audit_tw_day_trade_source_gaps import (
    assessment_domains, compare_paper_minute_marks, exact_gap_keys, verify_raw_manifest,
)


def test_catalogue_gaps_are_not_global_trajectory_blockers_or_training_acceptance():
    domains = assessment_domains([{'handling': 'avoid', 'len': 2132},
                                 {'handling': 'exact_cash', 'len': 12886}])
    assert domains['source_inventory']['unresolved_entitlement_catalogue_rows'] == 2132
    assert domains['source_inventory']['catalogue_rows_are_global_training_blockers'] is False
    assert domains['trajectory_accounting']['status'] == 'not_assessed'
    assert domains['trajectory_accounting']['missing_held_price_or_action_may_be_masked_away'] is False
    assert domains['annual_training_integration']['status'] == 'blocked'


def test_gap_scope_preserves_symbol_day_grain_without_expanding_whole_day():
    start, end = date(2020, 1, 1), date(2026, 9, 10)
    assert exact_gap_keys('2330', ['2019-12-31', '2026-08-13', '2026-08-14'], start=start, end=end) == [
        ('2330', '2026-08-13'), ('2330', '2026-08-14')]
    assert exact_gap_keys('0050', [], start=start, end=end) == []


@pytest.mark.parametrize('symbol,days', [
    ('../2330', ['2026-08-13']), ('2330', ['20260813']),
    ('2330', ['2026-08-13', '2026-08-13']), ('2330', ['2026-02-30']),
])
def test_gap_scope_rejects_malformed_identity_or_duplicate(symbol, days):
    with pytest.raises(ValueError):
        exact_gap_keys(symbol, days, start=date(2020, 1, 1), end=date(2026, 9, 10))


def raw_receipt(tmp_path):
    body = b'official source evidence'
    (tmp_path / 'source.html').write_bytes(body)
    request = {'url': 'https://www.twse.com.tw/example', 'data': {'date': '20260813'}}
    record = {'path': 'source.html', 'request': request,
        'request_sha256': hashlib.sha256(json.dumps(request, ensure_ascii=True, separators=(',', ':'), sort_keys=True).encode()).hexdigest(),
        'response_sha256': hashlib.sha256(body).hexdigest(), 'response_size': len(body)}
    raw = (json.dumps(record) + '\n').encode()
    (tmp_path / 'manifest.jsonl').write_bytes(raw)
    return {'relative_path': 'manifest.jsonl', 'sha256': hashlib.sha256(raw).hexdigest(),
            'size': len(raw), 'entries': 1}


def test_request_response_proof_verified_and_same_size_tampering_rejected(tmp_path):
    receipt = raw_receipt(tmp_path)
    assert verify_raw_manifest(tmp_path, receipt) == 1
    source = tmp_path / 'source.html'
    source.write_bytes(b'x' * source.stat().st_size)
    with pytest.raises(ValueError, match='request/response'):
        verify_raw_manifest(tmp_path, receipt)


def test_receipt_path_cannot_escape_selected_source_root(tmp_path):
    receipt = raw_receipt(tmp_path)
    child = tmp_path / 'child'
    child.mkdir()
    receipt['relative_path'] = '../manifest.jsonl'
    with pytest.raises(ValueError, match='manifest receipt'):
        verify_raw_manifest(child, receipt)


def minute_comparison_fixture(tmp_path):
    day = date(2026, 8, 13)
    partition = tmp_path / f'trade_date={day}/data.parquet'
    partition.parent.mkdir()
    pl.DataFrame({'symbol': ['2330'] * 4,
        'ts': [datetime(2026, 8, 13, h, m) for h, m in [(9, 3), (9, 6), (9, 7), (13, 30)]],
        **{k: [1000., 900., 1000., 1000.] for k in ['Open', 'High', 'Low', 'Close']},
        'volume_shares': [2000., 0., 2000., 2000.], 'Amount': [2e6, 0., 2e6, 2e6]}).write_parquet(partition)
    (tmp_path / 'manifest.json').write_text(json.dumps({'partitions': [{
        'trade_date': str(day), 'status': 'ok', 'rows': 4,
        'output': str(partition.relative_to(tmp_path)),
        'output_sha256': hashlib.sha256(partition.read_bytes()).hexdigest()}]}))
    limits = tmp_path / 'limits.parquet'
    pl.DataFrame({'symbol': ['2330'], 'trading_date': [str(day)],
        'lower_limit_price': [900.], 'upper_limit_price': [1100.]}).write_parquet(limits)
    return day, partition, limits


def test_source_comparison_uses_existing_paper_loader_and_preserves_missing_first_observation(tmp_path):
    day, _, limits = minute_comparison_fixture(tmp_path)
    result = compare_paper_minute_marks(tmp_path, limits, day, ['2330'])
    assert result['status'] == 'passed'
    assert result['comparison_cells'] == 270
    assert result['unavailable_cells'] == 2
    assert result['carried_cells'] == 265
    assert result['coverage']['ignored_zero_volume_rows'] == 1
    assert result['valuation_is_execution_price'] is False


def test_source_comparison_rejects_changed_partition(tmp_path):
    day, partition, limits = minute_comparison_fixture(tmp_path)
    pl.read_parquet(partition).with_columns(pl.lit(1005.).alias('Close')).write_parquet(partition)
    with pytest.raises(ValueError, match='receipt mismatch'):
        compare_paper_minute_marks(tmp_path, limits, day, ['2330'])


def test_source_comparison_rejects_wrong_date_limit_and_duplicate_scope(tmp_path):
    day, _, limits = minute_comparison_fixture(tmp_path)
    with pytest.raises(ValueError, match='unique explicit'):
        compare_paper_minute_marks(tmp_path, limits, day, ['2330', '2330'])
    pl.read_parquet(limits).with_columns(pl.lit('2026-08-12').alias('trading_date')).write_parquet(limits)
    with pytest.raises(ValueError, match='exact dated limits'):
        compare_paper_minute_marks(tmp_path, limits, day, ['2330'])
