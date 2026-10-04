import json
from datetime import date

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.audit_tw_futures_free_gap_sources import exact_finmind_settlements
from scripts.plan_tw_futures_tej_gap_priority import _read_context_chain


def test_free_settlement_requires_the_own_month_date_product_and_day_session():
    requests = pl.DataFrame([dict(date='2014-11-19', futures_id='DTF', contract_date='201412')])
    rows = [dict(date='2014-11-19', futures_id='DTF', contract_date='201412',
        trading_session='position', settlement_price=1.7)]
    for key, value in [('date', '2014-11-18'), ('futures_id', 'DT1'),
            ('contract_date', '201411'), ('trading_session', 'after_market'), ('contract_date', '201411/201412')]:
        rows.append(dict(rows[0], **{key:value}))
    result = exact_finmind_settlements(pl.DataFrame(rows), requests)
    assert result.height == 1 and result['settlement_price'].item() == 1.7


@pytest.mark.parametrize('missing', ['contract_date', 'trading_session', 'settlement_price'])
def test_ineligible_provider_schema_does_not_become_an_empty_proof(missing):
    rows = pl.DataFrame([dict(date='2014-11-19', futures_id='DTF', contract_date='201412',
        trading_session='position', settlement_price=None)])
    requests = rows.select('date', 'futures_id', 'contract_date')
    with pytest.raises(ValueError):
        exact_finmind_settlements(rows.drop(missing), requests)


def test_context_reader_restores_bound_empty_rules_and_original_carry(tmp_path):
    from test_tw_futures_empty_account_prefix import account_rows
    from stockagent.data.tw_futures_margin_release import omit_unreachable_account_prefix
    frame, original, flags = account_rows()
    frame = frame.with_columns(pl.lit('AA1').alias('product'), pl.lit('202003').alias('contract'))
    original = original.with_columns(pl.lit('AA1').alias('product'), pl.lit('202003').alias('contract'))
    kept, _, excluded, suppressed = omit_unreachable_account_prefix(frame, original, flags)
    replay, diagnosis = tmp_path/'replay', tmp_path/'diagnosis'
    replay.mkdir(); diagnosis.mkdir()
    outputs = {}
    for name, data in [('frame', frame), ('compiled_rules', kept),
            ('context_only_original_rules', excluded), ('context_only_suppressed_carries', suppressed)]:
        path = replay/(name+'.parquet'); atomic_write_parquet(path, data)
        outputs[path.name] = dict(sha256=sha256_file(path))
    atomic_write_json(replay/'manifest.json', dict(affected_products=['AA1'], outputs=outputs,
        context_only_prefix_policy=dict(path='test_only_policy')))
    base_outputs = {}
    for name, data in [('frame', frame.head(0)), ('compiled_rules', original.head(0))]:
        path = diagnosis/(name+'.parquet'); atomic_write_parquet(path, data)
        base_outputs[name] = dict(sha256=sha256_file(path))
    atomic_write_json(diagnosis/'manifest.json', dict(outputs=base_outputs))
    gaps = frame.filter(pl.col('date') == date(2020, 1, 8)).select('date', 'physical_contract', 'product')
    restored_frame, restored, sources, chains = _read_context_chain(replay, diagnosis, gaps,
        frame_columns=frame.columns, rule_columns=original.columns,
        full_rule_scope=True, restore_context_prefix=True)
    assert restored.sort('date').equals(original.sort('date'))
    assert restored_frame.equals(frame) and chains == 1
    assert any('context_only_suppressed_carries' in item['path'] for item in sources)
    path = replay/'context_only_suppressed_carries.parquet'
    path.write_bytes(b'changed')
    with pytest.raises(ValueError, match='prefix original SHA'):
        _read_context_chain(replay, diagnosis, gaps, frame_columns=frame.columns,
            rule_columns=original.columns, full_rule_scope=True, restore_context_prefix=True)
