from datetime import date
import hashlib
import json
from pathlib import Path
import zipfile

import polars as pl
import pytest

from stockagent.data.tw_option_product_daily import (
    OptionDailySourceWindow, iter_option_product_daily_batches, iter_option_product_daily_window,
    load_option_daily_source_plan,
)


def _source(tmp_path: Path) -> tuple[Path, str]:
    path = tmp_path / 'options.zip'
    header = '交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,收盤價,結算價,成交量,交易時段\n'
    rows = (
        '2025/01/03,TXO,202501F1,23000,買權,1.05,2.1,2.2,5,一般\n'
        '2025/01/03,CAO,202501,23.50,賣權,0.15,0.2,0.3,2,\n'
        '2025/01/03,CAO,202501,23.50,賣權,0.15,0.2,0.3,2,\n'
        '2025/01/06,TGO,202501W1,68,買權,-,1.5,1.6,0,盤後\n'
        '2025/01/06,RHO,other,2,PUT,1,1,1,1,未知\n'
    )
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('member.csv', (header + rows).encode('cp950'))
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_source_rows_preserve_values_dates_sessions_and_duplicate_provenance(tmp_path):
    path, sha = _source(tmp_path)
    batches = list(iter_option_product_daily_batches(path, expected_sha256=sha))
    frame = pl.concat(batches)
    assert frame['product'].to_list() == ['TXO', 'CAO', 'CAO', 'TGO', 'RHO']
    assert frame['session'].to_list() == ['day', 'unreported', 'unreported', 'night', 'unknown']
    assert frame['series_scope'].to_list() == ['weekly_candidate', 'monthly_candidate',
                                                'monthly_candidate', 'weekly_candidate', 'unclassified']
    assert frame['right'].to_list() == ['C', 'P', 'P', 'C', 'P']
    assert frame['strike_raw'].to_list()[1:3] == ['23.50', '23.50']
    assert frame['source_record_ordinal'].to_list() == [1, 2, 3, 4, 5]
    assert set(frame['source_sha256']) == {sha}
    assert set(frame['source_member']) == {'options.zip:member.csv'}


def test_scope_filters_without_promoting_to_execution(tmp_path):
    path, sha = _source(tmp_path)
    frame = pl.concat(list(iter_option_product_daily_batches(
        path, expected_sha256=sha, products={'CAO', 'TGO'}, trading_dates={date(2025, 1, 3)})))
    assert frame['product'].to_list() == ['CAO', 'CAO']
    assert frame['source_record_ordinal'].to_list() == [2, 3]
    assert 'executable' not in frame.columns and 'available_at' not in frame.columns


@pytest.mark.parametrize('error', ['sha', 'product', 'date'])
def test_source_view_rejects_unpinned_or_invalid_scopes(tmp_path, error):
    path, sha = _source(tmp_path)
    kwargs = {'expected_sha256': 'a' * 64 if error == 'sha' else sha}
    if error == 'product':
        kwargs['products'] = {'cao'}
    if error == 'date':
        kwargs['trading_dates'] = {'2025-01-03'}
    with pytest.raises(ValueError):
        list(iter_option_product_daily_batches(path, **kwargs))


def test_reader_rejects_source_mutation_before_completion(tmp_path):
    path, sha = _source(tmp_path)
    batches = iter_option_product_daily_batches(path, expected_sha256=sha)
    assert next(batches).height == 5
    with path.open('ab') as source:
        source.write(b'changed')
    with pytest.raises(ValueError, match='changed'):
        list(batches)


def test_multi_source_window_filters_dates_and_products_without_filling(tmp_path):
    path, sha = _source(tmp_path)
    sources = [OptionDailySourceWindow(path, sha, date(2025, 1, 1), date(2025, 1, 3)),
               OptionDailySourceWindow(path, sha, date(2025, 1, 4), date(2025, 1, 7))]
    rows = pl.concat(list(iter_option_product_daily_window(
        sources, start=date(2025, 1, 2), end=date(2025, 1, 6), products={'CAO', 'TGO'})))
    assert rows['product'].to_list() == ['CAO', 'CAO', 'TGO']
    assert rows['date'].to_list() == [date(2025, 1, 3), date(2025, 1, 3), date(2025, 1, 6)]
    assert rows['session'].to_list() == ['unreported', 'unreported', 'night']


def test_multi_source_window_rejects_gap_and_overlap_before_reading(tmp_path):
    path, sha = _source(tmp_path)
    first = OptionDailySourceWindow(path, sha, date(2025, 1, 1), date(2025, 1, 3))
    for start, reason in ((date(2025, 1, 5), 'gap'), (date(2025, 1, 3), 'overlapping')):
        second = OptionDailySourceWindow(path, sha, start, date(2025, 1, 7))
        with pytest.raises(ValueError, match=reason):
            list(iter_option_product_daily_window([first, second], start=date(2025, 1, 1), end=date(2025, 1, 7)))


def test_source_plan_requires_exact_pinned_content(tmp_path):
    path, sha = _source(tmp_path)
    plan = tmp_path / 'plan.json'
    plan.write_text(json.dumps({'contract': 'tw_option_all_product_daily_source_plan_v1',
                                'sources': [{'path': str(path), 'sha256': sha,
                                             'start': '2025-01-01', 'end': '2025-12-31'}]}))
    digest = hashlib.sha256(plan.read_bytes()).hexdigest()
    assert load_option_daily_source_plan(plan, expected_sha256=digest) == [
        OptionDailySourceWindow(path, sha, date(2025, 1, 1), date(2025, 12, 31))]
    plan.write_text(plan.read_text() + ' ')
    with pytest.raises(ValueError, match='digest mismatch'):
        load_option_daily_source_plan(plan, expected_sha256=digest)
