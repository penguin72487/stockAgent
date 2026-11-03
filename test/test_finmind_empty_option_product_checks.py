from datetime import UTC, date, datetime
import hashlib
import json

import polars as pl
import pytest

from downloader import download_finmind_complement as owner
from downloader.provider_priority import inspect
from scripts import request_finmind_empty_option_product_checks as checks
from scripts.run_finmind_option_tick_product_priority_wait import checked_inputs


DAY = date(2011, 2, 16)
NOW = datetime(2026, 10, 29, 4, tzinfo=UTC)


def fixture(tmp_path, monkeypatch):
    root = tmp_path / 'data_finmind/complement'
    root.mkdir(parents=True)
    daily = tmp_path / 'data_tw_index_options_daily'
    archive = daily / 'raw/annual/2011_opt.zip'
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b'official source fixture')
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    (daily / 'manifest.json').write_text(json.dumps({'receipts': [{'path': str(archive), 'sha256': digest}]}))
    (daily / 'research').mkdir()
    for filename in ('stockagent/data/tw_option_product_daily.py', 'stockagent/data/tw_index_futures.py',
                     'scripts/audit_taifex_option_source_universe.py',
                     'scripts/audit_taifex_option_daily_tick_coverage.py',
                     'scripts/request_finmind_empty_option_product_checks.py'):
        p = tmp_path / filename
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text('fixture implementation')
    monkeypatch.setattr(checks, '__file__', str(tmp_path / 'scripts/request_finmind_empty_option_product_checks.py'))
    receipt = root / f'receipts/TaiwanOptionTick/all/{DAY}.json'
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({'dataset': 'TaiwanOptionTick', 'data_id': '', 'partition': str(DAY),
                                   'kind': 'id_day', 'status': 'observed_empty', 'rows': 0,
                                   'request': {'query_shape': 'whole_market_storage_object_day'}}))
    with owner._db(root / 'queue.sqlite3') as con:
        owner._add_tasks(con, [('TaiwanOptionTick', '', str(DAY), 'id_day', 10)])
    source = pl.DataFrame({'date': [DAY, DAY, DAY], 'product': ['TXO', 'CAO', 'BAD'],
                           'series': ['201102', '201103', '201103'], 'strike_raw': ['8500', '85', '20'],
                           'right': ['C', 'P', 'P'], 'session': ['unreported', 'day', 'day'],
                           'volume_raw': ['5', '1', '0']})
    monkeypatch.setattr(checks, 'iter_option_product_daily_batches', lambda *_args, **_kwargs: iter([source]))
    monkeypatch.setattr(checks, 'capacity_check', lambda _path: {'state': 'admitted'})
    output = tmp_path / 'artifacts/markets/scope'
    output.mkdir(parents=True)
    return root, receipt, daily / 'research/gap', output


def test_empty_whole_market_checks_reuse_existing_owner_and_preserve_sources(tmp_path, monkeypatch):
    root, receipt, research, output = fixture(tmp_path, monkeypatch)
    original = receipt.read_bytes()
    evidence = checks.build_evidence(root, [DAY], research)
    plan = output / 'source_plan.json'
    result = checks.register_checks(root, evidence, plan, NOW)
    assert result['distinct_product_days'] == 2
    assert result['candidate_contract_keys'] == 2
    assert result['provider_calls_by_registration'] == 0 and result['training_ready'] is False
    assert receipt.read_bytes() == original
    saved = json.loads(plan.read_text())
    assert all(row['original_parquet_role'] == 'official_daily_positive_source_subset_not_finmind_tick'
               for row in saved['checks'])
    assert {row['data_id'] for row in saved['checks']} == {'TXO', 'CAO'}
    _, status = checked_inputs(plan, root, result['request_id'], NOW)
    assert all(row['state'] == 'pending' for row in status['items'])
    assert inspect(root, result['request_id'], now=NOW)['priority_active']
    with owner._db(root / 'queue.sqlite3') as con:
        rows = con.execute('SELECT data_id,kind,state FROM tasks ORDER BY data_id').fetchall()
        assert [tuple(row) for row in rows] == [('', 'id_day', 'pending'), ('CAO', 'id_day', 'pending'),
                                                 ('TXO', 'id_day', 'pending')]


def test_changed_empty_receipt_fails_before_product_tasks_are_registered(tmp_path, monkeypatch):
    root, receipt, research, output = fixture(tmp_path, monkeypatch)
    evidence = checks.build_evidence(root, [DAY], research)
    receipt.write_text(receipt.read_text() + ' ')
    with pytest.raises(ValueError, match='receipt changed'):
        checks.register_checks(root, evidence, output / 'source_plan.json', NOW)
    with owner._db(root / 'queue.sqlite3') as con:
        assert con.execute('SELECT count(*) FROM tasks').fetchone()[0] == 1


def test_wrong_query_shape_is_not_a_whole_market_empty_receipt(tmp_path, monkeypatch):
    root, receipt, research, _ = fixture(tmp_path, monkeypatch)
    doc = json.loads(receipt.read_text())
    doc['request']['query_shape'] = 'per_options_day'
    receipt.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match='whole-market object query'):
        checks.build_evidence(root, [DAY], research)


def test_official_float32_collision_cannot_be_admitted():
    frame = pl.DataFrame({'date': [DAY, DAY], 'product': ['TXO', 'TXO'], 'series': ['201102', '201102'],
                          'strike_raw': ['100000000', '100000001'], 'right': ['C', 'C'],
                          'session': ['day', 'day']})
    with pytest.raises(ValueError, match='collide'):
        checks.expected_contracts(frame)
