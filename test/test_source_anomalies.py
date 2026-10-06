from datetime import UTC, datetime
from argparse import Namespace
import json

import polars as pl
import pytest

from downloader import source_anomalies as quality
from downloader import audit_ohlcv_data as audit
from scripts import audit_source_anomalies as coordinator


def bars(dates, *, volume=None):
    size = len(dates)
    return pl.DataFrame({'date': dates, 'open': [100.] * size, 'max': [102.] * size,
                         'min': [99.] * size, 'close': [101.] * size,
                         'Trading_Volume': volume or [1.] * size})


def arguments():
    return Namespace(end_date='2026-10-02', stale_max_lag_days=14,
                     daily_gap_days=10, intraday_gap_multiple=4)


def millis(value):
    return int(datetime.fromisoformat(value).replace(tzinfo=UTC).timestamp() * 1000)


def test_24x7_grid_counts_only_actual_completed_observed_span():
    frame = bars(['2026-10-01 00:00:00', '2026-10-01 00:02:00', '2026-10-01 00:03:00'])
    result = quality.candle_grid(frame, completed_end_ms=millis('2026-10-01T00:02:00'))
    assert result['theoretical_rows_in_observed_span'] == 3
    assert result['observed_unique_completed_rows'] == 2
    assert result['missing_minutes_in_observed_span'] == 1
    assert result['unfinished_or_future_rows'] == 1
    assert result['repair_windows_ms'] == [[millis('2026-10-01T00:01:00')] * 2]


@pytest.mark.parametrize('dates', [
    ['2026-10-01 00:00:00', '2026-10-01 00:01:00.000000001'],
    ['2026-10-01 00:00:00', 'not-a-date'],
])
def test_bad_timestamp_never_creates_a_theoretical_gap(dates):
    result = quality.candle_grid(bars(dates), completed_end_ms=millis('2026-10-02T00:00:00'))
    assert result['invalid_timestamps'] or result['off_grid_rows']
    assert result['missing_minutes_in_observed_span'] is None
    assert result['repair_windows_ms'] == []


def test_exact_duplicate_is_separate_from_missing_count():
    result = quality.candle_grid(bars(['2026-10-01 00:00:00'] * 2),
                                 completed_end_ms=millis('2026-10-02T00:00:00'))
    assert result['duplicate_excess_rows'] == 1
    assert result['missing_minutes_in_observed_span'] == 0


def test_native_signed_financial_values_and_nulls_are_not_errors(tmp_path):
    path = tmp_path / 'financial.parquet'
    pl.DataFrame({'source_index': ['2020Q1', '2020Q2'], 'value': [-50., None]}).write_parquet(path)
    result = quality.footer_profile(path)
    assert result['status'] == 'footer_checked'
    assert result['null_slots_from_footer'] == 1
    assert result['expected_rows'] is None
    assert result['infinite_values'] == 0


def test_infinite_extrema_get_exact_value_count(tmp_path):
    path = tmp_path / 'financial.parquet'
    pl.DataFrame({'value': [1., float('inf'), float('-inf'), None]}).write_parquet(path)
    result = quality.footer_profile(path)
    assert result['status'] == 'invalid_values'
    assert result['infinite_values'] == 2
    assert result['infinite_columns'] == ['value']


def test_shared_ohlcv_audit_rejects_positive_infinity(tmp_path):
    path = tmp_path / 'BTC_features.parquet'
    bars(['2026-10-01 00:00:00']).with_columns(pl.lit(float('inf')).alias('max')).write_parquet(path)
    result = audit._audit_file((tmp_path, path, arguments()))
    assert result.infinite_value_rows == 1
    assert 'infinite_values' in result.issues


def test_no_trade_zero_prices_are_not_proven_missing_execution(tmp_path):
    path = tmp_path / 'STOCK_features.parquet'
    bars(['2026-10-01'], volume=[0.]).with_columns(*(pl.lit(0.).alias(c) for c in quality.OHLC)).write_parquet(path)
    result = quality.market_profile(path, arguments())
    assert result['confirmed_value_error'] is False
    assert result['invalid_observed_rows'] == 0


def test_hot_tail_revisions_are_not_duplicates_or_stale_base_errors(tmp_path):
    path = tmp_path / 'BTC_features.parquet'
    dates = ['2026-10-01 00:00:00', '2026-10-01 00:01:00']
    bars(dates).with_columns(pl.lit(float('inf')).alias('max')).write_parquet(path)
    tail = tmp_path / '_hot_tail'; tail.mkdir()
    bars(dates).write_parquet(tail / path.name)
    result = quality.market_profile(path, arguments(), crypto_1m=True)
    assert result['rows'] == 2
    assert result['infinite_values'] == 0
    assert result['confirmed_value_error'] is False
    assert result['duplicate_excess_rows'] == 0
    assert result['missing_minutes_in_observed_span'] == 0


def test_streaming_tail_fills_cross_batch_gaps_with_nonnull_precedence(tmp_path, monkeypatch):
    path = tmp_path / 'BTC_features.parquet'
    base = ['2026-10-01 00:00:00', '2026-10-01 00:02:00',
            '2026-10-01 00:03:00', '2026-10-01 00:05:00']
    bars(base).write_parquet(path)
    folder = tmp_path / '_hot_tail'
    folder.mkdir()
    bars(['2026-10-01 00:01:00', '2026-10-01 00:02:00',
          '2026-10-01 00:04:00', '2026-10-01 00:06:00']).with_columns(
              pl.Series('close', [101., None, 101., 101.])).write_parquet(folder / path.name)
    real = quality.pq.ParquetFile
    class SmallBatch:
        def __init__(self, *args, **kwargs):
            self.inner = real(*args, **kwargs)
            self.schema_arrow = self.inner.schema_arrow
        def iter_batches(self, **kwargs):
            return self.inner.iter_batches(**{**kwargs, 'batch_size': 2})
    monkeypatch.setattr(quality.pq, 'ParquetFile', SmallBatch)
    result = quality.market_profile(path, arguments(), crypto_1m=True)
    assert result['rows'] == result['theoretical_rows_in_observed_span'] == 7
    assert result['missing_minutes_in_observed_span'] == result['invalid_observed_rows'] == 0
    assert result['status'] == 'values_checked'


def test_file_changed_during_check_is_not_a_repair_verdict(tmp_path, monkeypatch):
    path = tmp_path / 'DATA.parquet'; pl.DataFrame({'value': [1.]}).write_parquet(path)
    real = quality.file_binding
    calls = []
    def binding(*args, **kwargs):
        calls.append(True)
        result = real(*args, **kwargs)
        if len(calls) > 1:
            result[0]['signature'][2] += 1
        return result
    monkeypatch.setattr(quality, 'file_binding', binding)
    assert quality.footer_profile(path)['status'] == 'changed_during_check'


def test_window_merge_preserves_large_healthy_middle():
    assert quality.merge_windows([(0, 0), (60000, 60000), (600000, 600000)]) == [[0, 60000], [600000, 600000]]


def test_official_zero_quantity_and_active_unusable_prices_have_distinct_meaning():
    from scripts.audit_tw_raw_price_anomalies import classify
    frame = bars(['2020-01-02'] * 4, volume=[0., 1000., 0., -1.]).with_columns(
        pl.Series('open', [0., 0., 0., 100.]),
        pl.Series('max', [0., 0., 0., 102.]),
        pl.Series('min', [0., 0., 0., 99.]),
        pl.Series('close', [0., 0., 101., 101.]))
    assert classify(frame)['classification'].to_list() == [
        'no_trade_unpriceable_not_missing_execution',
        'quantity_without_regular_OHLC_trade_scope_unverified',
        'normal_native_positive_close_sentinel',
        'invalid_or_unknown_quantity']


def test_discovery_never_reads_excluded_TEJ(tmp_path, monkeypatch):
    path = tmp_path / 'data_binance' / '1m' / 'BTC_features.parquet'
    path.parent.mkdir(parents=True); bars(['2026-10-01 00:00:00']).write_parquet(path)
    monkeypatch.setattr(coordinator, '_selected_files', lambda root: {
        'group:binance': [path], 'group:tej': [tmp_path / 'data_tej/SECRET.parquet']})
    entries, families = coordinator.discover(tmp_path)
    assert list(entries) == [path]
    assert all('tej' not in item['family'] for item in families)


def test_bad_finlab_reference_is_reported_without_following_outside_path(tmp_path, monkeypatch):
    receipts = tmp_path / 'data_finlab/receipts'; receipts.mkdir(parents=True)
    receipt = receipts / 'bad.json'
    receipt.write_text(json.dumps({'dataset': 'test:value', 'parquet_path': '../../private.parquet'}))
    monkeypatch.setattr(coordinator, '_selected_files', lambda root: {})
    entries, _ = coordinator.discover(tmp_path)
    assert list(entries) == [receipt]
    assert entries[receipt]['discovery_error'] == 'invalid_or_unsafe_receipt_reference'


def test_malformed_receipt_is_not_silently_omitted(tmp_path, monkeypatch):
    receipts = tmp_path / 'data_finlab/receipts'; receipts.mkdir(parents=True)
    receipt = receipts / 'bad.json'; receipt.write_text('{bad')
    monkeypatch.setattr(coordinator, '_selected_files', lambda root: {})
    entries, _ = coordinator.discover(tmp_path)
    assert entries[receipt]['discovery_error'] == 'unreadable_receipt'
