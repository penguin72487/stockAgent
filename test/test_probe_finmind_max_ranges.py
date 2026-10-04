from datetime import date, timedelta
import json
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.finmind_volume_units import annotate_stock_share_units
from scripts import probe_finmind_max_ranges as probe


@pytest.mark.parametrize('old_value', [None, 7])
def test_entire_column_disappearance_fails_even_when_old_values_are_null(old_value):
    comparison = probe.compare_rows(
        [{'date': '2025-01-01', 'value': old_value}],
        [{'date': '2025-01-01'}],
    )
    assert comparison['compared_columns'] == ['date', 'value']
    assert comparison['missing_response_columns'] == ['value']
    assert not comparison['schema_equal']
    assert not comparison['equal']


def test_new_column_is_not_silently_ignored():
    comparison = probe.compare_rows(
        [{'date': '2025-01-01'}],
        [{'date': '2025-01-01', 'new_value': None}],
    )
    assert comparison['unexpected_response_columns'] == ['new_value']
    assert not comparison['equal']


def test_equal_schema_still_checks_all_values_and_duplicate_multiplicity():
    rows = [{'date': '2025-01-01', 'value': 7}, {'date': '2025-01-02', 'value': 8}]
    assert probe.compare_rows(rows, list(reversed(rows)))['equal']
    assert not probe.compare_rows(rows, [rows[0], rows[0]])['equal']
    assert not probe.compare_rows(rows, rows + [rows[0]])['equal']


@pytest.mark.parametrize('dataset', probe.DATASETS)
def test_probe_sources_do_not_need_generated_column_exclusions(dataset):
    rows = [{'date': '2025-01-01', 'value': 1}]
    annotated, _ = annotate_stock_share_units(dataset, rows)
    assert annotated == rows
    assert set(annotated[0]) == set(rows[0])


@pytest.mark.parametrize('days,verified', [
    (['2025-12-28', '2025-12-29'], True),
    (['2025-12-28'], False),
    ([], False),
    (['2025-12-29', '2025-12-30'], False),
])
def test_boundary_proof_requires_end_day_and_no_foreign_dates(days, verified):
    result = probe.boundary_result([{'date': day} for day in days],
                                   date(2025, 12, 22), date(2025, 12, 29))
    assert result['inclusive_end_verified'] is verified
    assert result['end_date_rows'] == days.count('2025-12-29')


def runtime(tmp_path, monkeypatch):
    monkeypatch.setattr(probe, '__file__', str(tmp_path / 'scripts' / 'probe_finmind_max_ranges.py'))
    monkeypatch.setattr(probe, 'load_env_file', lambda *_a, **_kw: None)
    monkeypatch.setenv('FINMIND_TOKEN', 'private-test-token')
    monkeypatch.setattr(probe, 'verified_account', lambda *_a: {'tier': 'Sponsor'})
    monkeypatch.setattr(probe, 'rate_limiter', lambda *_a: object())
    monkeypatch.setattr(probe, 'fixed_incremental_demand', lambda *_a: 4)
    monkeypatch.setattr(probe, 'backfill_budget', lambda *_a, **_kw: {'allowed': True})
    queue = tmp_path / 'data_finmind' / 'sponsor' / 'queue.sqlite3'
    queue.parent.mkdir(parents=True)
    with sqlite3.connect(queue):
        pass
    return queue


def test_boundary_only_dispatches_exactly_three_short_shared_budget_requests(tmp_path, monkeypatch, capsys):
    queue = runtime(tmp_path, monkeypatch)
    before = queue.read_bytes()
    calls = []
    budgets = []

    def budget(*_args, **kwargs):
        budgets.append(kwargs)
        return {'allowed': True}

    def fetch(_session, _limiter, _root, dataset, _token, params, **kwargs):
        calls.append((dataset, params, kwargs))
        return [{'date': params['end_date'], 'stock_id': 'fixture'}]

    monkeypatch.setattr(probe, 'backfill_budget', budget)
    monkeypatch.setattr(probe, '_fetch_rows', fetch)
    probe.main(['--boundary-only'])
    report = json.loads(capsys.readouterr().out)
    assert report['mode'] == 'inclusive_end_boundary'
    assert report['max_data_requests'] == len(calls) == len(budgets) == 3
    assert report['production_queue_writes'] == 0
    assert queue.read_bytes() == before
    assert all(item['inclusive_end_verified'] and item['end_date_rows'] == 1
               for item in report['probes'])
    assert all(item['fixed_incremental_requests'] == 4 for item in budgets)
    for dataset, params, kwargs in calls:
        assert params['end_date'] == str(probe.BOUNDARY_DATES[dataset])
        assert params['start_date'] == str(probe.BOUNDARY_DATES[dataset] - timedelta(days=7))
        assert kwargs['max_response_bytes'] == 64 * 1024 * 1024
    assert len(list((tmp_path / 'artifacts' / 'data_quality').glob('finmind_range_boundaries_*.json'))) == 1


def test_boundary_only_stops_when_budget_is_reserved(tmp_path, monkeypatch, capsys):
    runtime(tmp_path, monkeypatch)
    answers = iter([{'allowed': True}, {'allowed': False, 'basis': 'incremental_reserve'}])
    monkeypatch.setattr(probe, 'backfill_budget', lambda *_a, **_kw: next(answers))
    calls = []
    monkeypatch.setattr(probe, '_fetch_rows', lambda *_a, **_kw: calls.append(_a[-1]) or [])
    probe.main(['--boundary-only'])
    report = json.loads(capsys.readouterr().out)
    assert len(calls) == len(report['probes']) == 1
    assert not report['probes'][0]['inclusive_end_verified']
    assert report['halted'] == 'incremental_reserve'


def test_boundary_only_preserves_safe_error_and_stops_on_entitlement(tmp_path, monkeypatch, capsys):
    runtime(tmp_path, monkeypatch)
    calls = []

    def fetch(*args, **kwargs):
        calls.append(args[-1])
        raise probe.SourceError('not_entitled')

    monkeypatch.setattr(probe, '_fetch_rows', fetch)
    probe.main(['--boundary-only'])
    output = capsys.readouterr().out
    report = json.loads(output)
    assert len(calls) == 1 and report['halted'] == 'not_entitled'
    assert report['probes'][0]['error_code'] == 'not_entitled'
    assert 'private-test-token' not in output


def test_full_span_report_exposes_schema_regression_and_compared_columns(tmp_path, monkeypatch, capsys):
    queue = runtime(tmp_path, monkeypatch)
    dataset = 'TaiwanBusinessIndicator'
    monkeypatch.setattr(probe, 'DATASETS', (dataset,))
    parquet = queue.parent / 'source.parquet'
    pq.write_table(pa.Table.from_pylist([{'date': '2025-01-01', 'value': None}]), parquet)
    receipt = queue.parent / 'source.json'
    receipt.write_text(json.dumps({'parquet_path': parquet.name, 'sha256': probe._sha256(parquet)}))
    with sqlite3.connect(queue) as conn:
        conn.execute('CREATE TABLE tasks(dataset,state,partition,priority,receipt_path)')
        conn.execute('INSERT INTO tasks VALUES (?,?,?,?,?)',
                     (dataset, 'complete', '2025-01-01', 2, receipt.name))
    monkeypatch.setattr(probe, '_fetch_rows', lambda *_a, **_kw: [{'date': '2025-01-01'}])
    probe.main([])
    report = json.loads(capsys.readouterr().out)
    assert report['comparison_contract_version'] == 2
    assert report['ignored_generated_columns'] == []
    item = report['probes'][0]
    assert not item['exact_available_local_overlap']
    assert item['mismatched_partitions'] == ['2025-01-01']
    assert item['compared_columns'] == ['date', 'value']
    assert item['partition_comparisons'][0]['missing_response_columns'] == ['value']
