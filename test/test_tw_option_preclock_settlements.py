from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from scripts.audit_tw_option_preclock_settlements import annotate
from scripts.audit_tw_option_preclock_settlements_native import native_index, relationship


TZ = ZoneInfo('Asia/Taipei')
DAY = date(2026, 10, 28)


def reference(product, traded):
    return {'product_raw': product, 'series_raw': '202610', 'strike_decimal': '100',
            'right_raw': 'C', 'decision_date': DAY, 'decision_time': '08:45',
            'trade_ts': datetime.fromisoformat(traded).replace(tzinfo=TZ)}


def settlement(product, day, positive=True):
    return {'product_raw': product, 'series_raw': '202610',
            'settlement_date': day, 'positive_numeric_price': positive}


def test_retrospective_relations_keep_generation_ambiguity_and_timing():
    references = pl.DataFrame([
        reference('BEFORE', '2026-10-20T09:00:00'),
        reference('REUSED', '2026-10-23T09:00:00'),
        reference('SAME', '2026-10-28T08:40:00'),
        reference('AFTER', '2026-10-27T09:00:00'),
        reference('DUP', '2026-10-20T09:00:00'),
        reference('MISSING', '2026-10-20T09:00:00'),
    ])
    settlements = pl.DataFrame([
        settlement('BEFORE', date(2026, 10, 21)),
        settlement('REUSED', date(2026, 10, 21)),
        settlement('SAME', DAY),
        settlement('AFTER', date(2026, 10, 29)),
        settlement('DUP', date(2026, 10, 21)),
        settlement('DUP', date(2026, 10, 21)),
        settlement('MISSING', date(2026, 10, 21), positive=False),
    ])
    result = {row['product_raw']: row for row in annotate(references, settlements).to_dicts()}
    assert result['BEFORE']['settlement_relation'] == 'single_source_settlement_before_decision_day'
    assert result['BEFORE']['reference_trade_relation'] == 'reference_trade_on_or_before_source_settlement'
    assert result['REUSED']['reference_trade_relation'] == 'reference_trade_after_source_settlement_generation_unknown'
    assert result['SAME']['settlement_relation'] == 'single_source_settlement_on_decision_day'
    assert result['AFTER']['settlement_relation'] == 'single_source_settlement_after_decision_day'
    assert result['DUP']['settlement_relation'] == 'multiple_source_rows_generation_unknown'
    assert len(result['DUP']['source_settlement_dates']) == 2
    assert result['MISSING']['settlement_relation'] == 'no_source_settlement_key'


def test_duplicate_reference_fails_closed():
    row = reference('A', '2026-10-20T09:00:00')
    with pytest.raises(ValueError, match='duplicate reference'):
        annotate(pl.DataFrame([row, row]), pl.DataFrame([settlement('A', DAY)]))


def test_missing_positive_settlement_date_fails_closed():
    settlements = pl.DataFrame([settlement('A', None)], schema_overrides={'settlement_date': pl.Date})
    with pytest.raises(ValueError, match='explicit identity and date'):
        annotate(pl.DataFrame([reference('A', '2026-10-20T09:00:00')]), settlements)


def test_native_index_reads_friday_and_preserves_raw_cells():
    body = b'''<table><thead><tr><th>\xe6\x9c\x80\xe5\xbe\x8c\xe7\xb5\x90\xe7\xae\x97\xe6\x97\xa5</th>
      <th>\xe5\xa5\x91\xe7\xb4\x84\xe6\x9c\x88\xe4\xbb\xbd</th><th>Index (TXO)</th></tr></thead>
      <tbody><tr><td>2026/10/27</td><td>202610F4</td><td>46,115</td></tr></tbody></table>'''
    row = {'source_row': 1, 'settlement_date': date(2026, 10, 27), 'series_raw': '202610F4',
           'product_raw': 'TXO', 'settlement_price_raw': '46,115', 'positive_numeric_price': True}
    assert native_index(body, [row]) == [('TXO', '202610F4', date(2026, 10, 27))]
    with pytest.raises(ValueError, match='normalized cells differ'):
        native_index(body, [{**row, 'settlement_price_raw': '46115'}])


def test_independent_relationship_preserves_duplicate_generation_ambiguity():
    row = reference('A', '2026-10-23T09:00:00')
    assert relationship(row, [date(2026, 10, 21)])[1].endswith('generation_unknown')
    assert relationship(row, [date(2026, 10, 21)] * 2)[0] == 'multiple_source_rows_generation_unknown'
