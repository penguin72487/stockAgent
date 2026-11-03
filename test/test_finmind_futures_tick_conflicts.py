from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import sqlite3

import polars as pl
import pytest

from downloader import finmind_corrections as corrections
from downloader.artifact_io import sha256_file
from downloader.finmind_futures_tick_conflicts import BATCHES, load_futures_tick_conflicts
from scripts.audit_taifex_finmind_futures_tick_presence import audit
from scripts.request_finmind_futures_tick_conflicts import register
from scripts import run_finmind_option_conflict_wait as waiter


NOW = datetime(2026, 10, 28, 12, tzinfo=UTC)


def _install(con, root, day, rows, fetched, **extra):
    rel = f'receipts/TaiwanFuturesTick/all/{day}.json'
    receipt = {'dataset': 'TaiwanFuturesTick', 'data_id': '', 'partition': day,
               'kind': 'id_day', 'status': 'complete' if rows else 'observed_empty',
               'rows': len(rows), 'fetched_at_utc': fetched.isoformat(), **extra}
    if rows:
        payload = root / 'parquet' / day / f'{fetched.timestamp()}.parquet'
        payload.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([{'date': f'{day} 09:00:00', 'futures_id': product,
                       'contract_date': contract, 'volume': volume}
                      for product, contract, volume in rows]).write_parquet(payload)
        receipt.update(parquet_path=str(payload.relative_to(root)),
                       parquet_size_bytes=payload.stat().st_size, sha256=sha256_file(payload))
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(receipt))
    con.execute('UPDATE tasks SET state=?,rows=?,receipt_path=? WHERE dataset=? AND data_id=? AND partition=?',
                (receipt['status'], len(rows), rel, receipt['dataset'], '', day))
    con.commit()


def _setup(tmp_path):
    root = tmp_path / 'data_finmind/complement'
    root.mkdir(parents=True)
    con = sqlite3.connect(root / 'queue.sqlite3')
    con.execute('''CREATE TABLE tasks(dataset TEXT,data_id TEXT,partition TEXT,kind TEXT,priority INTEGER,
        state TEXT,next_attempt_at_utc TEXT,last_attempt_at_utc TEXT,rows INTEGER DEFAULT 0,bytes INTEGER DEFAULT 0,
        first_data_date TEXT,last_data_date TEXT,receipt_path TEXT,error_code TEXT,
        PRIMARY KEY(dataset,data_id,partition))''')
    for day in ('2020-01-02', '2020-01-03', '2020-01-06'):
        con.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) VALUES (?, '',?,'id_day',8,'complete')",
                    ('TaiwanFuturesTick', day))
    _install(con, root, '2020-01-02', [('TX', '202001', 2)], NOW - timedelta(days=1))
    _install(con, root, '2020-01-03', [], NOW - timedelta(days=1))
    _install(con, root, '2020-01-06', [('TX', '202001', 1)], NOW - timedelta(days=1))
    official_dir = tmp_path / 'data_tw_index_futures'
    official_dir.mkdir()
    official = official_dir / 'official.parquet'
    pl.DataFrame({'date': [date(2020, 1, 2), date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 6)],
                  'product': ['TX', 'MTX', 'TX', 'TX'],
                  'contract': ['202001', '202001W1', '202001', '202001'],
                  'series_type': ['monthly', 'weekly', 'monthly', 'monthly'],
                  'session': ['一般'] * 4, 'session_reported': [True] * 4, 'volume': [1] * 4}).write_parquet(official)
    manifest = official_dir / 'manifest.json'
    manifest.write_text(json.dumps({'all_futures_daily': {'path': str(official), 'sha256': sha256_file(official)}}))
    result_dir = tmp_path / 'artifacts/markets/futures_audit'
    result_dir.mkdir(parents=True)
    summary, days, unmatched = audit(official, sha256_file(official), root,
                                     start=date(2020, 1, 2), end=date(2020, 1, 6))
    days_file, missing_file = result_dir / 'days.json', result_dir / 'unmatched.json'
    days_file.write_text(json.dumps(days))
    missing_file.write_text(json.dumps(unmatched))
    summary.update(official_manifest_path=str(manifest.relative_to(tmp_path)),
                   official_manifest_sha256=sha256_file(manifest),
                   days_path=str(days_file.relative_to(tmp_path)), days_sha256=sha256_file(days_file),
                   unmatched_path=str(missing_file.relative_to(tmp_path)), unmatched_sha256=sha256_file(missing_file))
    summary_file = result_dir / 'summary.json'
    summary_file.write_text(json.dumps(summary))
    return root, con, summary_file, official


def test_futures_conflicts_only_reopen_evidenced_tasks_and_verify_old_and_new_keys(tmp_path):
    root, con, summary, _ = _setup(tmp_path)
    plan_info = register(root, summary, NOW)
    assert (plan_info['dates'], plan_info['contract_day_keys']) == (2, 2)
    assert plan_info['provider_calls'] == 0 and not plan_info['task_states_changed']
    sidecar = Path(plan_info['plan_path'])
    plan, _ = load_futures_tick_conflicts(root, NOW, sidecar=sidecar)
    assert [req['expected_source_futures_contracts'] for req in plan['requests']] == [
        [['MTX', '202001W1']], [['TX', '202001']]]
    applied = corrections.apply_worker_corrections(con, root, 'complement', NOW)['source_futures_tick_conflicts'][0]
    assert applied['applied'] == 2 and applied['source_evidence_changed'] == 0
    assert con.execute('SELECT partition,state FROM tasks ORDER BY partition').fetchall() == [
        ('2020-01-02', 'pending'), ('2020-01-03', 'pending'), ('2020-01-06', 'complete')]
    task = {'dataset': 'TaiwanFuturesTick', 'data_id': '', 'partition': '2020-01-02', 'kind': 'id_day'}
    context = corrections.correction_context(con, task)
    assert context['allow_empty'] is False
    marker = corrections.correction_receipt_metadata(context)
    # A superficially recovered key must not hide loss of the prior TX key.
    _install(con, root, task['partition'], [('MTX', '202001W1', 1)], NOW + timedelta(seconds=1), **marker)
    result = corrections.reconcile_worker_corrections(con, root, 'complement', NOW + timedelta(seconds=2))
    assert result['unverified'] == 1 and result['repaired'] == 0
    _install(con, root, task['partition'], [('TX', '202001', 2), ('MTX', '202001W1', 1)],
             NOW + timedelta(seconds=3), **marker)
    result = corrections.reconcile_worker_corrections(con, root, 'complement', NOW + timedelta(seconds=4))
    assert result['repaired'] == 1
    assert load_futures_tick_conflicts(root, NOW + timedelta(days=7), sidecar=sidecar) is None


@pytest.mark.parametrize('changed', ['official', 'summary', 'receipt'])
def test_changed_evidence_does_not_reopen_futures_tasks(tmp_path, changed):
    root, con, summary, official = _setup(tmp_path)
    register(root, summary, NOW)
    target = {'official': official, 'summary': summary,
              'receipt': root / 'receipts/TaiwanFuturesTick/all/2020-01-02.json'}[changed]
    target.write_bytes(target.read_bytes() + b' ')
    result = corrections.apply_worker_corrections(con, root, 'complement', NOW)['source_futures_tick_conflicts'][0]
    if changed == 'receipt':
        assert result['source_evidence_changed'] == 1 and result['applied'] == 1
    else:
        assert result['state'] == 'rejected'
    assert con.execute("SELECT state FROM tasks WHERE partition='2020-01-02'").fetchone() == ('complete',)


def test_waiter_selects_only_the_requested_native_futures_keys(tmp_path):
    root, con, summary, _ = _setup(tmp_path)
    info = register(root, summary, NOW)
    plan, _ = load_futures_tick_conflicts(root, NOW, sidecar=Path(info['plan_path']))
    corrections.apply_worker_corrections(con, root, 'complement', NOW)
    con.commit()
    states = waiter.read_states(root, plan['requests'])
    assert [s['correction_state'] for s in states] == ['queued', 'queued']
    assert waiter.select_keys(plan['requests'], states) == [
        ('TaiwanFuturesTick', '', '2020-01-02'), ('TaiwanFuturesTick', '', '2020-01-03')]
