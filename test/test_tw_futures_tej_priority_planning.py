import json
import sqlite3

import polars as pl

from downloader.tej_key_layout import KEY1_CONTRACT
from downloader.tej_priority import pack_gap_rectangles, query_keys
from scripts.plan_tw_futures_tej_gap_priority import (
    ADJUSTMENT, ATTRIBUTE, DAILY, plan_gaps, relevant_covered_keys,
)


def test_factored_prior_scope_preserves_all_worklist_and_control_cells():
    request = {'company_labels': ['A==>A', 'B==>B', 'X==>X'],
               'date_labels': ['2012/01/01', '2012/01/02', '2012/01/03']}
    scope = {'A', 'B'}
    original = query_keys(request)
    projected = relevant_covered_keys(request, scope)
    assert projected == {(company, day) for company, day in original if company in scope}
    assert ('A', '2012-01-03') in projected


def test_projection_keeps_the_same_sparse_rectangle_packing():
    request = {'company_labels': ['A==>A', 'X==>X'], 'date_labels': ['2012/01/03']}
    wanted = {'A': {'2012-01-01'}, 'B': {'2012-01-03'}}
    options = dict(max_companies=10, max_rows=100)
    assert pack_gap_rectangles(wanted, forbidden=query_keys(request), **options) == pack_gap_rectangles(
        wanted, forbidden=relevant_covered_keys(request, set(wanted)), **options)


def test_snapshot_scope_keeps_none_period_and_validates_layout():
    request = {'company_labels': ['A==>A', 'X==>X'], 'date_labels': [],
               'source_key_mode': 1, 'key_layout_contract': KEY1_CONTRACT}
    assert relevant_covered_keys(request, {'A'}) == {('A', None)}


def test_attribute_only_worklist_does_not_expand_unrelated_daily_sources(tmp_path):
    root = tmp_path / 'tej'
    root.mkdir()
    (root / 'axes.json').write_text(json.dumps({
        'company_labels': ['CCF201104==>CCF'], 'date_labels': []}))
    fields = ['Listed Date', 'Expiration Moon']
    with sqlite3.connect(root / 'queue.sqlite3') as db:
        db.executescript('''
            CREATE TABLE meta(key TEXT, value TEXT);
            CREATE TABLE tables(table_id TEXT, name TEXT, discovery_path TEXT,
                                fields_json TEXT, source_key_mode INTEGER,
                                query_type TEXT, smart_id TEXT);
            CREATE TABLE tasks(table_id TEXT, kind TEXT, state TEXT);
        ''')
        db.execute('INSERT INTO meta VALUES (?,?)', ('config', json.dumps({
            'max_companies_per_export': 100, 'max_rows_per_export': 100,
            'max_cells_per_export': 2000, 'history_search_start': '1900-01-01'})))
        for tid, name, axes in [(ATTRIBUTE, 'Future Attribute', 'axes.json'),
                                (ADJUSTMENT, 'Adjustment', None), (DAILY, 'Daily', None)]:
            db.execute('INSERT INTO tables VALUES (?,?,?,?,?,?,?)',
                       (tid, name, axes, json.dumps(fields), 1, 'Future', 'TEJ Derivatives'))
    worklist = tmp_path / 'work.csv'
    pl.DataFrame({'product': ['CCF'], 'contract': ['201104'],
                  'requested_fields': ['own_physical_successor_and_last_day'],
                  'exact_dates': ['']}).write_csv(worklist)
    terms = tmp_path / 'terms.parquet'
    pl.DataFrame({'product': [], 'contract': []}, schema={'product': pl.String, 'contract': pl.String}).write_parquet(terms)
    plan = plan_gaps(root, worklist, terms, tmp_path / 'plan.json')
    assert len(plan['requests']) == 1
    assert plan['requests'][0]['table_id'] == ATTRIBUTE
    assert plan['requests'][0]['request']['company_labels'] == ['CCF201104==>CCF']
    for tid in (ADJUSTMENT, DAILY):
        assert plan['diagnosis'][tid]['planned_queries'] == 0
        assert plan['diagnosis'][tid]['prior_inventory_skipped'] == 'no_relevant_requested_fields'
