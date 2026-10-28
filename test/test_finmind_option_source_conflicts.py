from datetime import UTC, datetime, timedelta
import csv
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import finmind_corrections as corrections
from downloader.finmind_source_conflict_rechecks import (
    CONTRACT_BATCHES, PRODUCT_BATCHES, RECENT_BATCHES, SIDECAR, load_option_conflicts,
    load_option_contract_conflicts, load_option_product_conflicts, load_recent_option_conflicts,
)
from scripts.request_finmind_option_source_conflicts import register
from scripts.request_finmind_option_product_conflicts import register as register_products
from scripts.request_finmind_option_contract_conflicts import register as register_contracts
from scripts.request_finmind_recent_option_conflicts import register as register_recent
from scripts import run_finmind_option_conflict_wait as waiter

NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)


def _conn():
    conn = sqlite3.connect(':memory:')
    conn.execute('''CREATE TABLE tasks(dataset TEXT,data_id TEXT,partition TEXT,kind TEXT,priority INTEGER,
        state TEXT,next_attempt_at_utc TEXT,last_attempt_at_utc TEXT,rows INTEGER DEFAULT 0,bytes INTEGER DEFAULT 0,
        first_data_date TEXT,last_data_date TEXT,receipt_path TEXT,error_code TEXT,
        PRIMARY KEY(dataset,data_id,partition))''')
    return conn


def _task(conn, partition, *, dataset, data_id='', kind='id_day', state='observed_empty'):
    conn.execute('INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) VALUES (?,?,?,?,8,?)',
                 (dataset, data_id, partition, kind, state))
    return {'dataset': dataset, 'data_id': data_id, 'partition': partition, 'kind': kind}


def _install(conn, root, task, *, fetched, rows, status, products=('TXO',), **metadata):
    receipt = {**task, 'status': status, 'rows': rows, 'fetched_at_utc': fetched.isoformat(), **metadata}
    if rows:
        relative_data = f"parquet/{task['dataset']}/{task['data_id'] or 'all'}/{task['partition']}.parquet"
        path_data = root / relative_data
        path_data.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist([{'date': task['partition'], 'option_id': products[i % len(products)]}
                                            for i in range(rows)]), path_data)
        receipt.update(parquet_path=relative_data, parquet_size_bytes=path_data.stat().st_size,
                       sha256=_sha(path_data))
    relative = f"receipts/{task['dataset']}/{task['data_id'] or 'all'}/{task['partition']}.json"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(receipt))
    conn.execute('UPDATE tasks SET state=?,rows=?,receipt_path=? WHERE dataset=? AND data_id=? AND partition=?',
                 (status, rows, relative, task['dataset'], task['data_id'], task['partition']))


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _setup(tmp_path):
    root = tmp_path / 'data_finmind/complement'
    root.mkdir(parents=True)
    conn = _conn()
    tasks = []
    for day in ('2011-02-16', '2014-12-11'):
        task = _task(conn, day, dataset='TaiwanOptionTick', kind='id_day', state='observed_empty')
        _install(conn, root, task, rows=0, status='observed_empty', fetched=NOW - timedelta(days=1))
        tasks.append(task)
    _task(conn, '2011-02-16', dataset='TaiwanOptionTick', data_id='TXO', kind='id_day', state='observed_empty')
    _task(conn, '2011-02-17', dataset='TaiwanOptionTick', kind='id_day', state='observed_empty')
    official = tmp_path / 'data_tw_index_options_daily'
    annual = official / 'raw/annual'
    annual.mkdir(parents=True)
    sources = []
    for year in (2011, 2014):
        p = annual / f'{year}_opt.zip'
        p.write_bytes(f'official-{year}'.encode())
        sources.append({'path': str(p), 'sha256': _sha(p)})
    manifest = official / 'manifest.json'
    manifest.write_text('{}')
    output = tmp_path / 'artifacts/markets/option-audit'
    output.mkdir(parents=True)
    inventory = output / 'inventory.csv'
    fields = ['date', 'finmind_status', 'day_or_unreported_positive_candidate', 'day_positive_products',
              'unreported_session_positive_products', 'finmind_receipt_sha256']
    with inventory.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for task in tasks:
            receipt = root / 'receipts/TaiwanOptionTick/all' / f"{task['partition']}.json"
            writer.writerow({'date': task['partition'], 'finmind_status': 'observed_empty',
                             'day_or_unreported_positive_candidate': 'True',
                             'day_positive_products': 'TXO', 'finmind_receipt_sha256': _sha(receipt)})
    summary = output / 'summary.json'
    summary.write_text(json.dumps({
        'contract': 'taifex_option_annual_positive_days_finmind_receipts_v1',
        'finmind_root': str(root), 'source_manifest': str(manifest), 'source_manifest_sha256': _sha(manifest),
        'day_inventory': str(inventory), 'day_inventory_sha256': _sha(inventory), 'annual_sources': sources,
    }))
    return root, conn, tasks, summary, inventory, annual


def test_exact_source_conflicts_use_owner_audit_and_never_authorize_empty(tmp_path):
    root, conn, tasks, summary, _, _ = _setup(tmp_path)
    before = conn.execute('SELECT * FROM tasks ORDER BY data_id,partition').fetchall()
    result = register(root, summary, [t['partition'] for t in tasks], NOW)
    assert result['provider_calls'] == 0 and result['task_states_changed'] is False
    assert conn.execute('SELECT * FROM tasks ORDER BY data_id,partition').fetchall() == before
    with pytest.raises(FileExistsError):
        register(root, summary, [tasks[0]['partition']], NOW)
    applied = corrections.apply_worker_corrections(conn, root, 'complement', NOW)['source_conflicts']
    assert applied['applied'] == 2 and applied['unmatched_requests'] == 0
    states = conn.execute('SELECT data_id,partition,state,priority FROM tasks ORDER BY data_id,partition').fetchall()
    assert states == [('', '2011-02-16', 'pending', 1), ('', '2011-02-17', 'observed_empty', 8),
                      ('', '2014-12-11', 'pending', 1), ('TXO', '2011-02-16', 'observed_empty', 8)]
    assert corrections.correction_context(conn, tasks[0])['allow_empty'] is False
    assert corrections.apply_worker_corrections(conn, root, 'complement', NOW)['source_conflicts']['unchanged'] == 2
    context = corrections.correction_context(conn, tasks[0])
    _install(conn, root, tasks[0], rows=0, status='observed_empty', fetched=NOW + timedelta(seconds=1),
             **corrections.correction_receipt_metadata(context))
    checked = corrections.reconcile_worker_corrections(conn, root, 'complement', NOW + timedelta(seconds=2))
    assert checked['unverified'] == 1 and checked['repaired'] == 0
    assert conn.execute("SELECT state FROM finmind_correction_tasks WHERE partition='2011-02-16'").fetchone() == ('observed_empty_unverified',)
    assert corrections.apply_worker_corrections(conn, root, 'complement', NOW + timedelta(seconds=2))['source_conflicts']['unchanged'] == 2
    context = corrections.correction_context(conn, tasks[0])
    _install(conn, root, tasks[0], rows=1, status='complete', fetched=NOW + timedelta(seconds=3),
             products=('CAO',), **corrections.correction_receipt_metadata(context))
    incomplete = corrections.reconcile_worker_corrections(conn, root, 'complement', NOW + timedelta(seconds=4))
    assert incomplete['unverified'] == 1 and incomplete['repaired'] == 0
    _install(conn, root, tasks[0], rows=1, status='complete', fetched=NOW + timedelta(seconds=5),
             **corrections.correction_receipt_metadata(context))
    repaired = corrections.reconcile_worker_corrections(conn, root, 'complement', NOW + timedelta(seconds=6))
    assert repaired['repaired'] == 1
    assert conn.execute("SELECT state FROM finmind_correction_tasks WHERE partition='2011-02-16'").fetchone() == ('repaired',)


@pytest.mark.parametrize('changed', ['archive', 'inventory', 'receipt', 'summary'])
def test_changed_evidence_cannot_requeue_a_conflict(tmp_path, changed):
    root, conn, tasks, summary, inventory, annual = _setup(tmp_path)
    register(root, summary, [tasks[0]['partition']], NOW)
    files = {'archive': annual / '2011_opt.zip', 'inventory': inventory, 'summary': summary,
             'receipt': root / 'receipts/TaiwanOptionTick/all/2011-02-16.json'}
    files[changed].write_bytes(files[changed].read_bytes() + b' ')
    status = corrections.apply_worker_corrections(conn, root, 'complement', NOW)['source_conflicts']
    if changed == 'receipt':
        assert status['source_evidence_changed'] == 1 and status['applied'] == 0
    else:
        assert status['state'] == 'rejected'
    assert conn.execute("SELECT state FROM tasks WHERE data_id='' AND partition='2011-02-16'").fetchone() == ('observed_empty',)


def test_expiry_stops_new_admissions_without_touching_queue(tmp_path):
    root, conn, tasks, summary, _, _ = _setup(tmp_path)
    register(root, summary, [tasks[0]['partition']], NOW)
    assert load_option_conflicts(root, NOW + timedelta(days=7)) is None
    result = corrections.apply_worker_corrections(conn, root, 'complement', NOW + timedelta(days=7))
    assert 'source_conflicts' not in result
    assert conn.execute("SELECT state FROM tasks WHERE data_id='' AND partition='2011-02-16'").fetchone() == ('observed_empty',)
    assert (root / SIDECAR).exists()


def test_capacity_wait_never_calls_owner_or_changes_tasks(tmp_path, monkeypatch):
    root, conn, tasks, summary, _, _ = _setup(tmp_path)
    register(root, summary, [t['partition'] for t in tasks], datetime.now(UTC))
    with sqlite3.connect(root / 'queue.sqlite3') as target:
        conn.commit()
        conn.backup(target)
    monkeypatch.setattr(waiter, 'ROOT', root)
    monkeypatch.setattr(waiter, 'OWNERS', ())
    priority_receipt = root / 'priority_queries' / 'test-request.json'
    priority_receipt.parent.mkdir()
    priority_receipt.write_text('{}')
    monkeypatch.setattr(waiter.sys, 'argv', ['waiter', '--request-id', 'test-request'])
    monkeypatch.setattr(waiter, 'AcquisitionCapacitySampler', lambda _: SimpleNamespace(
        initial_sample=lambda: {'state': 'deferred', 'reason': 'physical_backing_reserve'}))
    monkeypatch.setattr(waiter.subprocess, 'run', lambda *a, **kw: pytest.fail('owner must not run'))
    class WaitStopped(Exception):
        pass
    monkeypatch.setattr(waiter.time, 'sleep', lambda _: (_ for _ in ()).throw(WaitStopped()))
    with pytest.raises(WaitStopped):
        waiter.main()
    with sqlite3.connect(root / 'queue.sqlite3') as check:
        assert check.execute("SELECT count(*) FROM tasks WHERE state='pending'").fetchone() == (0,)


def test_waiter_treats_incomplete_new_object_as_unresolved_terminal():
    assert waiter.done([{'correction_state': 'queued', 'task_state': 'complete'}])
    assert not waiter.done([{'correction_state': 'queued', 'task_state': 'pending'}])
    assert waiter.done([{'correction_state': 'already_refreshed', 'task_state': 'complete'}])


def test_waiter_deduplicates_exact_dates_without_losing_unresolved_scopes():
    requests = [{'start_date': '2025-01-02'}, {'start_date': '2025-01-02'}, {'start_date': '2025-01-03'}]
    states = [{'correction_state': 'queued', 'task_state': 'pending'}] * 3
    assert waiter.select_keys(requests, states) == [
        ('TaiwanOptionTick', '', '2025-01-02'), ('TaiwanOptionTick', '', '2025-01-03')]


def _recent_setup(tmp_path):
    root = tmp_path / 'data_finmind/complement'
    root.mkdir(parents=True)
    conn = _conn()
    day = '2026-09-24'
    task = _task(conn, day, dataset='TaiwanOptionTick', state='observed_empty')
    _install(conn, root, task, rows=0, status='observed_empty', fetched=NOW - timedelta(days=1))
    source = tmp_path / 'data_tw_index_derivatives_ticks/raw/options/OptionsDaily_2026_09_24.zip'
    source.parent.mkdir(parents=True)
    with zipfile.ZipFile(source, 'w') as archive:
        archive.writestr('data.csv', '\n'.join([
            'date,product,strike,month,right,time,price,qty,auction',
            '20260924,TXO,24000,202609,C,084500,4.2,1,',
            '20260924,CAO,120,202609,P,090000,1.5,2,',
            '20260923,BAD,120,202609,P,090000,1.5,2,',
            '20260924,NOQ,120,202609,P,090000,1.5,0,',
        ]).encode('cp950'))
    manifest = source.parents[2] / 'manifest.json'
    manifest.write_text(json.dumps({'raw_downloads': [{
        'kind': 'options', 'trading_date': day, 'path': str(source), 'sha256': _sha(source),
        'url': 'https://www.taifex.com.tw/file/taifex/Dailydownload/'
               'OptionsDailydownloadCSV/OptionsDaily_2026_09_24.zip',
    }]}))
    output = tmp_path / 'artifacts/markets/recent-conflict'
    return root, conn, task, source, manifest, output


def test_recent_official_tick_recheck_is_exact_and_requires_all_source_products(tmp_path):
    root, conn, task, source, manifest, output = _recent_setup(tmp_path)
    result = register_recent(root, manifest, output, [task['partition']], NOW)
    assert result['provider_calls'] == 0 and result['task_states_changed'] is False
    assert result['expected_source_products'][task['partition']] == ['CAO', 'TXO']
    assert conn.execute('SELECT state FROM tasks').fetchone() == ('observed_empty',)
    applied = corrections.apply_worker_corrections(conn, root, 'complement', NOW)
    assert applied['source_recent_tick_conflicts'][0]['applied'] == 1
    assert conn.execute('SELECT state FROM tasks').fetchone() == ('pending',)
    context = corrections.correction_context(conn, task)
    assert context['allow_empty'] is False
    _install(conn, root, task, rows=1, status='complete', fetched=NOW + timedelta(seconds=1),
             products=('TXO',), **corrections.correction_receipt_metadata(context))
    assert corrections.reconcile_worker_corrections(conn, root, 'complement', NOW + timedelta(seconds=2))['unverified'] == 1
    _install(conn, root, task, rows=2, status='complete', fetched=NOW + timedelta(seconds=3),
             products=('TXO', 'CAO'), **corrections.correction_receipt_metadata(context))
    assert corrections.reconcile_worker_corrections(conn, root, 'complement', NOW + timedelta(seconds=4))['repaired'] == 1
    sidecar = next((root / RECENT_BATCHES).glob('*.json'))
    assert load_recent_option_conflicts(root, NOW + timedelta(days=7), sidecar=sidecar) is None


@pytest.mark.parametrize('which', ['archive', 'manifest_snapshot', 'receipt_snapshot', 'live_receipt'])
def test_recent_source_change_never_requeues(tmp_path, which):
    root, conn, task, source, manifest, output = _recent_setup(tmp_path)
    register_recent(root, manifest, output, [task['partition']], NOW)
    paths = {'archive': source, 'manifest_snapshot': output / 'official_manifest.json',
             'receipt_snapshot': output / f"{task['partition']}_finmind_original_receipt.json",
             'live_receipt': root / 'receipts/TaiwanOptionTick/all' / f"{task['partition']}.json"}
    target = paths[which]
    target.write_bytes(target.read_bytes() + b' ')
    applied = corrections.apply_worker_corrections(conn, root, 'complement', NOW)
    status = applied['source_recent_tick_conflicts'][0]
    if which == 'live_receipt':
        assert status['source_evidence_changed'] == 1 and status['applied'] == 0
    else:
        assert status['state'] == 'rejected'
    assert conn.execute('SELECT state FROM tasks').fetchone() == ('observed_empty',)


def _product_setup(tmp_path):
    root, conn, _, coverage_path, inventory, annual = _setup(tmp_path)
    day = '2019-07-11'
    task = _task(conn, day, dataset='TaiwanOptionTick', state='complete')
    _install(conn, root, task, rows=1, status='complete', fetched=NOW - timedelta(days=1))
    archive = annual / '2019_opt.zip'
    archive.write_bytes(b'official-2019')
    with inventory.open('a', newline='') as file:
        csv.writer(file).writerow([day, 'complete_payload_sha_verified', 'True', 'TXO|CAO|CBO', '',
                                   _sha(root / 'receipts/TaiwanOptionTick/all/2019-07-11.json')])
    coverage = json.loads(coverage_path.read_text())
    coverage['day_inventory_sha256'] = _sha(inventory)
    coverage['annual_sources'].append({'path': str(archive), 'sha256': _sha(archive)})
    coverage_path.write_text(json.dumps(coverage))
    folder = coverage_path.parent
    presence_inventory = folder / 'presence.csv'
    with presence_inventory.open('w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['date', 'source_status', 'finmind_products', 'missing_day_products'])
        writer.writeheader()
        writer.writerow({'date': day, 'source_status': 'product_readback_verified',
                         'finmind_products': 'TXO', 'missing_day_products': 'CAO|CBO'})
    presence = folder / 'presence.json'
    presence.write_text(json.dumps({'contract': 'taifex_option_daily_vs_finmind_product_presence_v1',
                                    'coverage_summary_sha256': _sha(coverage_path),
                                    'day_inventory': str(presence_inventory),
                                    'day_inventory_sha256': _sha(presence_inventory)}))
    open_inventory = folder / 'open.csv'
    with open_inventory.open('w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['date', 'product', 'classification', 'official_positive_open_rows'])
        writer.writeheader()
        for product in ('CAO', 'CBO'):
            writer.writerow({'date': day, 'product': product, 'classification': 'official_open_missing_from_object',
                             'official_positive_open_rows': '1'})
    opening = folder / 'opening.json'
    opening.write_text(json.dumps({'contract': 'taifex_option_missing_open_product_presence_v1',
                                   'coverage_sha256': _sha(coverage_path), 'presence_summary_sha256': _sha(presence),
                                   'presence_inventory_sha256': _sha(presence_inventory),
                                   'inputs': [{'path': str(archive), 'sha256': _sha(archive)}],
                                   'inventory': str(open_inventory), 'inventory_sha256': _sha(open_inventory)}))
    return root, conn, task, coverage_path, presence, opening, archive


def test_product_conflict_rechecks_original_and_missing_products(tmp_path):
    root, conn, task, coverage, presence, opening, _ = _product_setup(tmp_path)
    before = conn.execute('SELECT * FROM tasks ORDER BY partition').fetchall()
    result = register_products(root, coverage, presence, opening, NOW)
    assert (result['dates'], result['product_days'], result['batches']) == (1, 2, 1)
    assert result['provider_calls'] == 0 and result['task_states_changed'] is False
    assert conn.execute('SELECT * FROM tasks ORDER BY partition').fetchall() == before
    with pytest.raises(FileExistsError):
        register_products(root, coverage, presence, opening, NOW)
    plan, _ = load_option_product_conflicts(root, NOW, sidecar=next((root / PRODUCT_BATCHES).glob('*.json')))
    assert plan['requests'][0]['expected_source_products'] == ['CAO', 'CBO', 'TXO']
    applied = corrections.apply_worker_corrections(conn, root, 'complement', NOW)['source_product_conflicts'][0]
    assert applied['applied'] == 1
    assert conn.execute("SELECT state FROM tasks WHERE partition='2019-07-11'").fetchone() == ('pending',)
    context = corrections.correction_context(conn, task)
    for second, products in enumerate((('CAO', 'CBO'), ('TXO', 'CAO'), ('TXO', 'CAO', 'CBO')), 1):
        _install(conn, root, task, rows=len(products), status='complete', fetched=NOW + timedelta(seconds=second * 2),
                 products=products, **corrections.correction_receipt_metadata(context))
        check = corrections.reconcile_worker_corrections(conn, root, 'complement', NOW + timedelta(seconds=second * 2 + 1))
        assert check['repaired'] == (1 if second == 3 else 0)
    assert conn.execute("SELECT state FROM finmind_correction_tasks WHERE partition='2019-07-11'").fetchone() == ('repaired',)


@pytest.mark.parametrize('changed', ['archive', 'receipt', 'presence', 'opening'])
def test_product_conflict_refuses_changed_source_evidence(tmp_path, changed):
    root, conn, _, coverage, presence, opening, archive = _product_setup(tmp_path)
    register_products(root, coverage, presence, opening, NOW)
    changed_path = {'archive': archive, 'receipt': root / 'receipts/TaiwanOptionTick/all/2019-07-11.json',
                    'presence': presence, 'opening': opening}[changed]
    changed_path.write_bytes(changed_path.read_bytes() + b' ')
    result = corrections.apply_worker_corrections(conn, root, 'complement', NOW)['source_product_conflicts'][0]
    if changed == 'receipt':
        assert result['source_evidence_changed'] == 1 and result['applied'] == 0
    else:
        assert result['state'] == 'rejected'
    assert conn.execute("SELECT state FROM tasks WHERE partition='2019-07-11'").fetchone() == ('complete',)


def test_contract_proof_requires_missing_keys_and_preserves_original_keys(tmp_path):
    root = tmp_path.resolve()
    original, current = root / 'original.parquet', root / 'current.parquet'
    def write(path, rows):
        pq.write_table(pa.table({'option_id': [r[0] for r in rows], 'contract_date': [r[1] for r in rows],
                                 'ExercisePrice': pa.array([r[2] for r in rows], type=pa.float32()),
                                 'PutCall': [r[3] for r in rows]}), path)
    prior_key = ('TXO', '202511F4', 25100.0, 'P')
    missing_key = ('CAO', '202511', 24.5, 'C')
    write(original, [prior_key])
    request = {'correction_id': 'contract-1', 'required_after_utc': NOW.isoformat(),
               'watermark_basis': 'source_conflict_detection', 'start_date': '2025-11-14',
               'end_date': '2025-11-14', 'original_parquet_path': original.name,
               'original_parquet_sha256': _sha(original), 'expected_source_contracts': [list(missing_key)]}
    task = {'dataset': 'TaiwanOptionTick', 'data_id': '', 'partition': '2025-11-14', 'kind': 'id_day'}
    def verify(rows):
        write(current, rows)
        receipt = {**task, 'status': 'complete', 'rows': len(rows), 'fetched_at_utc': NOW.isoformat(),
                   'parquet_path': current.name, 'parquet_size_bytes': current.stat().st_size,
                   'sha256': _sha(current),
                   'finmind_corrections': {'correction_ids': ['contract-1'], 'owner': 'complement',
                                           'dataset': task['dataset'], 'data_id': '', 'partition': task['partition']}}
        return corrections._verified_receipt(root, task, request, 'complement', receipt)
    assert verify([prior_key]) is None
    assert verify([missing_key]) is None
    evidence = verify([prior_key, missing_key])
    assert evidence['verified_expected_contract_keys'] == 1
    assert evidence['verified_prior_contract_keys'] == 1
    original.write_bytes(original.read_bytes() + b'changed')
    assert verify([prior_key, missing_key]) is None


def test_overlapping_source_conflicts_share_the_same_existing_task(tmp_path):
    root, conn, task, coverage, presence, opening, _ = _product_setup(tmp_path)
    register_products(root, coverage, presence, opening, NOW)
    result = corrections.apply_worker_corrections(conn, root, 'complement', NOW)
    assert result['source_product_conflicts'][0]['applied'] == 1
    first_json = conn.execute("SELECT request_json FROM finmind_correction_tasks WHERE partition='2019-07-11'").fetchone()[0]
    second = {**json.loads(first_json), 'correction_id': 'second-contract-conflict'}
    plan = {'plan_id': 'second-plan', 'requests': [second]}
    adopted = corrections._apply_worker_plan(conn, root, 'complement', NOW, plan, 'digest', max_receipts=128)
    assert adopted['applied'] == 1 and adopted['source_evidence_changed'] == 0
    assert set(corrections.correction_context(conn, task)['correction_ids']) == {
        json.loads(first_json)['correction_id'], 'second-contract-conflict'}


def _contract_setup(tmp_path):
    root, conn, task, coverage_path, _, _, archive = _product_setup(tmp_path)
    coverage = json.loads(coverage_path.read_text())
    inventory = Path(coverage['day_inventory'])
    with inventory.open(newline='') as file:
        rows = list(csv.DictReader(file))
    for row in rows:
        row['finmind_parquet_path'], row['finmind_parquet_sha256'] = '', ''
        if row['date'] == '2019-07-11':
            receipt = json.loads((root / 'receipts/TaiwanOptionTick/all/2019-07-11.json').read_text())
            row['finmind_parquet_path'] = str(root / receipt['parquet_path'])
            row['finmind_parquet_sha256'] = receipt['sha256']
    with inventory.open('w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    coverage['day_inventory_sha256'] = _sha(inventory)
    coverage_path.write_text(json.dumps(coverage))
    day_inventory = coverage_path.parent / 'contract-days.csv'
    day_inventory.write_text('date,source_status,missing_contracts\n2019-07-11,contract_key_readback_verified,2\n')
    missing = coverage_path.parent / 'missing-contracts.csv'
    missing.write_text('date,product,series,strike,right,session\n'
                       '2019-07-11,CAO,201908,24.5,C,day\n'
                       '2019-07-11,TXO,201908F2,11000,P,day\n')
    summary = coverage_path.parent / 'contract-summary.json'
    summary.write_text(json.dumps({'contract': 'taifex_option_ordinary_open_contract_vs_finmind_v1',
                                   'coverage_summary_sha256': _sha(coverage_path),
                                   'day_inventory': str(day_inventory), 'day_inventory_sha256': _sha(day_inventory),
                                   'missing_inventory': str(missing), 'missing_inventory_sha256': _sha(missing)}))
    return root, conn, task, coverage_path, summary, missing, archive


def test_contract_plan_is_exact_bounded_and_receipt_pinned(tmp_path):
    root, conn, task, coverage, summary, _, _ = _contract_setup(tmp_path)
    before = conn.execute('SELECT * FROM tasks ORDER BY partition').fetchall()
    status = register_contracts(root, coverage, summary, NOW)
    assert (status['dates'], status['contract_day_keys'], status['provider_calls']) == (1, 2, 0)
    assert conn.execute('SELECT * FROM tasks ORDER BY partition').fetchall() == before
    sidecar = next((root / CONTRACT_BATCHES).glob('*.json'))
    plan, _ = load_option_contract_conflicts(root, NOW, sidecar=sidecar)
    assert plan['requests'][0]['expected_source_contracts'] == [
        ['CAO', '201908', 24.5, 'C'], ['TXO', '201908F2', 11000.0, 'P']]
    applied = corrections.apply_worker_corrections(conn, root, 'complement', NOW)['source_contract_conflicts'][0]
    assert applied['applied'] == 1
    assert conn.execute("SELECT state FROM tasks WHERE partition='2019-07-11'").fetchone() == ('pending',)
    assert load_option_contract_conflicts(root, NOW + timedelta(days=7), sidecar=sidecar) is None


@pytest.mark.parametrize('changed', ['archive', 'receipt', 'inventory', 'summary'])
def test_contract_plan_rejects_changed_evidence(tmp_path, changed):
    root, conn, _, coverage, summary, inventory, archive = _contract_setup(tmp_path)
    register_contracts(root, coverage, summary, NOW)
    path = {'archive': archive, 'receipt': root / 'receipts/TaiwanOptionTick/all/2019-07-11.json',
            'inventory': inventory, 'summary': summary}[changed]
    path.write_bytes(path.read_bytes() + b' ')
    result = corrections.apply_worker_corrections(conn, root, 'complement', NOW)['source_contract_conflicts'][0]
    if changed == 'receipt':
        assert result['source_evidence_changed'] == 1 and result['applied'] == 0
    else:
        assert result['state'] == 'rejected'
    assert conn.execute("SELECT state FROM tasks WHERE partition='2019-07-11'").fetchone() == ('complete',)


def _monthly_contract_setup(root, conn, output, day):
    task = _task(conn, day, dataset='TaiwanOptionTick', state='complete')
    _install(conn, root, task, rows=1, status='complete', fetched=NOW - timedelta(days=1))
    workspace = root.parents[1]
    raw = workspace / 'data_tw_index_options_daily' / '2019-07-01_2019-07-31_all.csv'
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_bytes(b'official monthly source')
    identity = {'contract': 'taifex_option_all_products_daily_raw_v1',
                'url': 'https://www.taifex.com.tw/cht/3/optDataDown',
                'request': {'down_type': '1', 'queryStartDate': '2019/07/01', 'queryEndDate': '2019/07/31',
                            'commodity_id': 'all', 'commodity_id2': ''},
                'range_start': '2019-07-01', 'range_end': '2019-07-31', 'point_in_time_snapshot': False}
    raw.with_suffix('.intent.json').write_text(json.dumps(identity))
    raw.with_suffix('.receipt.json').write_text(json.dumps(
        {**identity, 'path': str(raw), 'bytes': raw.stat().st_size, 'sha256': _sha(raw)}))
    coverage = output / 'monthly-coverage.json'
    coverage.write_text(json.dumps({'contract': 'tw_option_all_product_monthly_range_readback_v1',
        'start': '2019-07-01', 'end': '2019-07-31', 'months_verified': 1,
        'sources': [{'start': '2019-07-01', 'end': '2019-07-31', 'path': str(raw),
                     'receipt': str(raw.with_suffix('.receipt.json')), 'bytes': raw.stat().st_size,
                     'sha256': _sha(raw)}]}))
    receipt_path = root / 'receipts/TaiwanOptionTick/all' / f'{day}.json'
    receipt = json.loads(receipt_path.read_text())
    days = output / 'monthly-days.csv'
    row = {'date': day, 'finmind_status': 'complete_payload_sha_verified',
           'finmind_receipt_sha256': _sha(receipt_path), 'finmind_parquet_path': str(root / receipt['parquet_path']),
           'finmind_parquet_sha256': receipt['sha256'], 'source_status': 'contract_key_readback_verified',
           'missing_contracts': 1}
    with days.open('w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    missing = output / 'monthly-missing.csv'
    missing.write_text(f'date,product,series,strike,right,session\n{day},CAO,201908,24.5,C,day\n')
    summary = output / 'monthly-summary.json'
    summary.write_text(json.dumps({'contract': 'taifex_option_monthly_open_contract_vs_finmind_v1',
        'source_readback': str(coverage), 'source_readback_sha256': _sha(coverage), 'finmind_root': str(root),
        'day_inventory': str(days), 'day_inventory_sha256': _sha(days),
        'missing_inventory': str(missing), 'missing_inventory_sha256': _sha(missing)}))
    return task, coverage, summary, raw, missing


def test_monthly_contract_plan_appends_to_existing_without_overwriting(tmp_path):
    root, conn, _, annual, annual_summary, _, _ = _contract_setup(tmp_path)
    register_contracts(root, annual, annual_summary, NOW)
    original = next((root / CONTRACT_BATCHES).glob('*.json'))
    original_sha = _sha(original)
    task, coverage, summary, _, _ = _monthly_contract_setup(root, conn, annual.parent, '2019-07-12')
    result = register_contracts(root, coverage, summary, NOW, append=True)
    assert (result['dates'], result['contract_day_keys'], result['provider_calls']) == (1, 1, 0)
    assert _sha(original) == original_sha
    sidecar = next(path for path in (root / CONTRACT_BATCHES).glob('*.json') if path != original)
    plan, _ = load_option_contract_conflicts(root, NOW, sidecar=sidecar)
    assert plan['requests'][0]['expected_source_contracts'] == [['CAO', '201908', 24.5, 'C']]
    applied = corrections.apply_worker_corrections(conn, root, 'complement', NOW)
    assert sum(item['applied'] for item in applied['source_contract_conflicts']) == 2
    assert conn.execute("SELECT state FROM tasks WHERE partition=?", (task['partition'],)).fetchone() == ('pending',)
    with pytest.raises(ValueError, match='overlap'):
        register_contracts(root, coverage, summary, NOW, append=True)


@pytest.mark.parametrize('changed', ['raw', 'receipt', 'intent', 'missing'])
def test_monthly_contract_plan_rejects_changed_source_or_inventory(tmp_path, changed):
    root, conn, _, annual, _, _, _ = _contract_setup(tmp_path)
    task, coverage, summary, raw, missing = _monthly_contract_setup(root, conn, annual.parent, '2019-07-12')
    register_contracts(root, coverage, summary, NOW)
    path = {'raw': raw, 'receipt': raw.with_suffix('.receipt.json'),
            'intent': raw.with_suffix('.intent.json'), 'missing': missing}[changed]
    path.write_bytes(path.read_bytes() + b' ')
    result = corrections.apply_worker_corrections(conn, root, 'complement', NOW)['source_contract_conflicts'][0]
    assert result['state'] == 'rejected'
    assert conn.execute("SELECT state FROM tasks WHERE partition=?", (task['partition'],)).fetchone() == ('complete',)


def test_monthly_contract_plan_rejects_duplicate_audit_dates(tmp_path):
    root, conn, _, annual, _, _, _ = _contract_setup(tmp_path)
    _, coverage, summary, _, _ = _monthly_contract_setup(root, conn, annual.parent, '2019-07-12')
    info = json.loads(summary.read_text())
    days = Path(info['day_inventory'])
    lines = days.read_text().splitlines()
    days.write_text('\n'.join([*lines, lines[-1]]) + '\n')
    info['day_inventory_sha256'] = _sha(days)
    summary.write_text(json.dumps(info))
    with pytest.raises(ValueError, match='duplicate'):
        register_contracts(root, coverage, summary, NOW)
    assert not list((root / CONTRACT_BATCHES).glob('*.json'))
