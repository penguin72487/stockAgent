import csv
import json

import polars as pl

from scripts.report_source_anomaly_delivery import empty_minute_day_classification, provider_scope


def test_source_availability_explains_empty_minute_views_without_fake_gap_fills(tmp_path):
    minute, source = tmp_path / 'tw-minute-coverage', tmp_path / 'shioaji-history'
    minute.mkdir(); source.mkdir()
    pl.DataFrame({'symbol': ['OLD', 'EMPTY', 'SCOPE', 'UNKNOWN'],
        'trade_date': ['2026-10-02'] * 4,
        'reference_status': ['official_positive_volume'] * 3 + ['minute_only_unverified_daily_reference'],
        'observed_active_minutes': [0] * 4, 'full_grid_physical_minutes': [0] * 4,
        'full_grid_observed_trade_minutes': [0] * 4}).write_parquet(minute / 'pair_coverage.parquet')
    (source / 'missing_symbol_days.csv').write_text(
        'symbol,trade_date,category\nOLD,2026-10-02,contract_unavailable\nEMPTY,2026-10-02,source_gap\n')
    result = empty_minute_day_classification(tmp_path)
    assert result['by_broker_source_category'] == {
        'contract_unavailable': 1, 'source_gap': 1,
        'source_day_returned_scope_or_view_needs_reconciliation': 1,
        'minute_only_unverified_reference': 1}
    with (tmp_path / 'tw-minute-empty-day-classification.csv').open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 4
    assert all(r['not_proven_missing_regular_trade_execution'] == 'True' for r in rows)


def test_missing_or_unmapped_provider_is_not_claimed_fully_checked(tmp_path):
    assert empty_minute_day_classification(tmp_path)['state'] == 'not_audited'
    scope, proof = provider_scope('NOAA weather station')
    assert 'unverified' in scope and proof
    assert 'not_all' in provider_scope('FinMind')[0]
