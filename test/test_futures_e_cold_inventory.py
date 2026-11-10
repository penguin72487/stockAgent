import json
import hashlib

from scripts import check_futures_e_cold_inventory as worker


def test_wait_then_compare_once_without_claiming_recovery(tmp_path, monkeypatch):
    workspace = tmp_path / 'e'; workspace.mkdir()
    (workspace / 'migration').mkdir(); (workspace / 'verify').mkdir()
    report = workspace / 'verify' / 'copy.json'; report.write_text('{"fixed":1}')
    item = {'name': worker.NAME}
    monkeypatch.setattr(worker.volume, 'check', lambda: None)
    monkeypatch.setattr(worker.volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(worker.queue, 'load_sources', lambda: [item])
    monkeypatch.setattr(worker.queue, 'paths', lambda item, workspace: (None, None, None, report))
    monkeypatch.setattr(worker.queue, 'verified_report', lambda *args: None)
    assert worker.check() == 75
    status = workspace / 'migration' / 'futures-e-cold-inventory-status.json'
    assert json.loads(status.read_bytes())['state'] == 'waiting_for_copy'
    proof = {'files': 2, 'logical_bytes': 30, 'relocation_evidence_sha256': 'a'*64}
    monkeypatch.setattr(worker.queue, 'verified_report', lambda *args: proof)
    monkeypatch.setattr(worker.audit, '_check_d_primary_mount', lambda path: None)
    head = {'D_snapshot_id': 'fixed', 'D_manifest_sha256': 'c'*64, 'D_inventory_sha256': 'b'*64}
    monkeypatch.setattr(worker, 'current_head_identity', lambda: dict(head))
    monkeypatch.setattr(worker, '_trading_allowed', lambda **kw: True)
    calls = []
    counts = {'exact_files': 1, 'exact_bytes': 10, 'changed_files': 1, 'changed_bytes': 20,
              'e_only_files': 0, 'e_only_bytes': 0, 'd_only_files': 0, 'd_non_file_entries': 1}
    def comparison(item, workspace, *, progress, guard, expected_head):
        calls.append(True)
        assert expected_head == head
        guard()
        progress({'staged_files_scanned': 1, 'staged_files_total': 2})
        return {'state': 'inventory_compared', **head,
                'D_inventory_source_prefix': 'training_prepared', 'counts': counts}
    monkeypatch.setattr(worker.audit, 'audit', comparison)
    assert worker.check() == 0 and calls == [True]
    result_path = workspace / 'verify' / 'futures-e-cold-inventory-comparison-v1.json'
    result = json.loads(result_path.read_bytes())
    assert not result['D_independent_restore_verified'] and not result['retirement_authorized']
    from stockagent.data_sync import storage_progress as monitor
    monkeypatch.setattr(worker.queue, 'ROOT', tmp_path)
    row, = monitor.workspace_cold_inventory_tasks(tmp_path, workspace)
    assert row['state'] == 'completed' and row['complete_evidence']
    assert row['metrics']['inventory_exact_files'] == 1
    assert worker.check() == 0 and calls == [True]
    previous = result_path.read_bytes()
    head.update(D_snapshot_id='new-fixed', D_manifest_sha256='d'*64, D_inventory_sha256='e'*64)
    row, = monitor.workspace_cold_inventory_tasks(tmp_path, workspace)
    assert row['state'] == 'waiting' and row['reason'] == 'futures_D_head_changed'
    assert not row['complete_evidence']
    assert worker.check() == 0 and calls == [True, True]
    history = workspace/'verify/futures-e-cold-inventory-history-v1'/(hashlib.sha256(previous).hexdigest()+'.json')
    assert history.read_bytes() == previous
    result = json.loads(result_path.read_bytes())
    assert result['D_manifest_sha256'] == 'd'*64 and result['D_snapshot_id'] == 'new-fixed'
    result['copy_result_sha256'] = 'c'*64
    result_path.write_text(json.dumps(result))
    assert worker.check() == 1
    assert json.loads(status.read_bytes())['state'] == 'blocked'
    row, = monitor.workspace_cold_inventory_tasks(tmp_path, workspace)
    assert row['state'] == 'blocked' and not row['complete_evidence']


def test_old_comparison_without_current_manifest_identity_is_historical_only():
    result = {'D_snapshot_id': 'fixed', 'D_inventory_sha256': 'a'*64}
    current = {**result, 'D_manifest_sha256': 'b'*64}
    assert not worker.matches_current(result, current)
    result['D_manifest_sha256'] = 'b'*64
    assert worker.matches_current(result, current)
    current['D_manifest_sha256'] = 'c'*64
    assert not worker.matches_current(result, current)


def test_regular_file_only_receipt_does_not_complete_a_copy_with_symlinks():
    proof = {'symlink_names_preserved':1}
    result = {'counts': {'exact_files': 1}}
    assert not worker.links_compared(result, proof)
    result['counts'].update(exact_symlinks=1, changed_symlinks=0, e_only_symlinks=0, d_only_symlinks=0)
    assert worker.links_compared(result, proof)
    result['counts']['exact_symlinks'] = 0
    assert not worker.links_compared(result, proof)


def test_same_head_old_receipt_is_retained_and_link_comparison_retried(tmp_path, monkeypatch):
    workspace = tmp_path/'e'; (workspace/'migration').mkdir(parents=True); (workspace/'verify').mkdir()
    report = workspace/'verify/copy.json'; report.write_text('{}')
    item = {'name':worker.NAME}
    monkeypatch.setattr(worker.volume, 'check', lambda: None)
    monkeypatch.setattr(worker.volume, 'configuration', lambda: {'mount_point':str(workspace)})
    monkeypatch.setattr(worker.queue, 'load_sources', lambda: [item])
    monkeypatch.setattr(worker.queue, 'paths', lambda *_: (None, None, None, report))
    monkeypatch.setattr(worker.queue, 'ROOT', tmp_path)
    proof = {'files':1, 'logical_bytes':3, 'relocation_evidence_sha256':'a'*64,
             'symlink_names_preserved':1}
    monkeypatch.setattr(worker.queue, 'verified_report', lambda *_: proof)
    head = {'D_snapshot_id':'fixed', 'D_inventory_sha256':'b'*64, 'D_manifest_sha256':'c'*64}
    monkeypatch.setattr(worker, 'current_head_identity', lambda: dict(head))
    monkeypatch.setattr(worker, '_trading_allowed', lambda **_kw: True)
    counts = {'exact_files':1, 'exact_bytes':3, 'changed_files':0, 'changed_bytes':0,
              'e_only_files':0, 'e_only_bytes':0, 'd_only_files':0, 'd_non_file_entries':1}
    old = {'contract':worker.CONTRACT, 'source':worker.NAME, 'state':'inventory_compared',
           'source_copy_report_sha256':hashlib.sha256(report.read_bytes()).hexdigest(),
           'copy_result_sha256':'a'*64, **head, 'D_inventory_source_prefix':'training_prepared',
           'counts':counts, 'observed_at_epoch':1, 'D_independent_restore_verified':False,
           'NAS_acceptance':False, 'source_files_deleted':0, 'retirement_authorized':False}
    result_path = workspace/'verify/futures-e-cold-inventory-comparison-v1.json'
    result_path.write_text(json.dumps(old)); original = result_path.read_bytes()
    from stockagent.data_sync import storage_progress as monitor
    row, = monitor.workspace_cold_inventory_tasks(tmp_path, workspace)
    assert row['state'] == 'waiting' and row['reason'] == 'futures_symlink_comparison_pending'
    new_counts = {**counts, 'exact_symlinks':1, 'changed_symlinks':0,
                  'e_only_symlinks':0, 'd_only_symlinks':0}
    monkeypatch.setattr(worker.audit, 'audit', lambda *_a, **_k: {
        'state':'inventory_compared', **head, 'D_inventory_source_prefix':'training_prepared',
        'counts':new_counts})
    assert worker.check() == 0
    assert json.loads(result_path.read_bytes())['counts'] == new_counts
    row, = monitor.workspace_cold_inventory_tasks(tmp_path, workspace)
    assert row['metrics']['inventory_exact_symlinks'] == 1
    assert 'inventory_exact_symlinks' in monitor.METRICS
    archive = workspace/'verify/futures-e-cold-inventory-history-v1'/(hashlib.sha256(original).hexdigest()+'.json')
    assert archive.read_bytes() == original


def test_market_window_defers_new_comparison_without_overwriting_history(tmp_path, monkeypatch):
    workspace = tmp_path/'e'; (workspace/'migration').mkdir(parents=True); (workspace/'verify').mkdir()
    report = workspace/'verify/copy.json'; report.write_text('{}')
    item = {'name': worker.NAME}
    monkeypatch.setattr(worker.volume, 'check', lambda: None)
    monkeypatch.setattr(worker.volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(worker.queue, 'load_sources', lambda: [item])
    monkeypatch.setattr(worker.queue, 'paths', lambda *_: (None, None, None, report))
    monkeypatch.setattr(worker.queue, 'verified_report', lambda *_: {'files': 1, 'logical_bytes': 10,
                                                                     'relocation_evidence_sha256': 'a'*64})
    monkeypatch.setattr(worker, 'current_head_identity', lambda: {'D_snapshot_id':'new',
        'D_inventory_sha256':'b'*64, 'D_manifest_sha256':'c'*64})
    monkeypatch.setattr(worker, '_trading_allowed', lambda **kw: False)
    monkeypatch.setattr(worker.audit, 'audit', lambda *_a, **_k: (_ for _ in ()).throw(AssertionError('must defer')))
    assert worker.check() == 75
    status = json.loads((workspace/'migration/futures-e-cold-inventory-status.json').read_bytes())
    assert status['state'] == 'waiting_trading'
    assert not (workspace/'verify/futures-e-cold-inventory-comparison-v1.json').exists()


def test_comparison_of_a_moving_head_is_preserved_as_history_and_retried(tmp_path, monkeypatch):
    workspace = tmp_path/'e'; (workspace/'migration').mkdir(parents=True); (workspace/'verify').mkdir()
    report = workspace/'verify/copy.json'; report.write_text('{}')
    item = {'name': worker.NAME}
    monkeypatch.setattr(worker.volume, 'check', lambda: None)
    monkeypatch.setattr(worker.volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(worker.queue, 'load_sources', lambda: [item])
    monkeypatch.setattr(worker.queue, 'paths', lambda *_: (None, None, None, report))
    monkeypatch.setattr(worker.queue, 'verified_report', lambda *_: {'files': 1, 'logical_bytes': 10,
                                                                     'relocation_evidence_sha256': 'a'*64})
    old = {'D_snapshot_id':'old', 'D_inventory_sha256':'b'*64, 'D_manifest_sha256':'c'*64}
    new = {'D_snapshot_id':'new', 'D_inventory_sha256':'d'*64, 'D_manifest_sha256':'e'*64}
    heads = iter((old, new))
    monkeypatch.setattr(worker, 'current_head_identity', lambda: next(heads))
    monkeypatch.setattr(worker, '_trading_allowed', lambda **kw: True)
    counts = {'exact_files':1, 'exact_bytes':10, 'changed_files':0, 'changed_bytes':0,
              'e_only_files':0, 'e_only_bytes':0, 'd_only_files':0, 'd_non_file_entries':0}
    monkeypatch.setattr(worker.audit, 'audit', lambda *_a, **_k: {
        'state':'inventory_compared', **old, 'D_inventory_source_prefix':'training_prepared', 'counts':counts})
    assert worker.check() == 75
    status = json.loads((workspace/'migration/futures-e-cold-inventory-status.json').read_bytes())
    assert status['state'] == 'waiting_head_changed' and status['D_snapshot_id'] == 'new'
    assert not (workspace/'verify/futures-e-cold-inventory-comparison-v1.json').exists()
    assert len(list((workspace/'verify/futures-e-cold-inventory-history-v1').glob('*.json'))) == 1


def test_monitor_shows_current_comparison_progress_and_refuses_unavailable_head(tmp_path, monkeypatch):
    from stockagent.data_sync import storage_progress as monitor
    workspace = tmp_path/'e'; (workspace/'migration').mkdir(parents=True); (workspace/'verify').mkdir()
    report = workspace/'verify/copy.json'; report.write_text('{}')
    item = {'name':worker.NAME}
    monkeypatch.setattr(worker.queue, 'ROOT', tmp_path)
    monkeypatch.setattr(worker.queue, 'load_sources', lambda: [item])
    monkeypatch.setattr(worker.queue, 'paths', lambda *_: (None, None, None, report))
    monkeypatch.setattr(worker.queue, 'verified_report', lambda *_: {'files':2, 'logical_bytes':30,
                                                                    'relocation_evidence_sha256':'a'*64})
    head = {'D_snapshot_id':'new', 'D_inventory_sha256':'b'*64, 'D_manifest_sha256':'c'*64,
            'D_entries_total':100}
    monkeypatch.setattr(worker, 'current_head_identity', lambda: head)
    status = {'contract':worker.CONTRACT, 'state':'comparing', **head,
              'observed_at_epoch': 1794070000,
              'progress': {'staged_files_scanned':1, 'staged_files_total':2, 'cold_entries_scanned':50}}
    (workspace/'migration/futures-e-cold-inventory-status.json').write_text(json.dumps(status))
    row, = monitor.workspace_cold_inventory_tasks(tmp_path, workspace)
    assert row['state'] == 'running' and row['completed_items'] == 51 and row['total_items'] == 102
    assert not row['complete_evidence']
    status['progress']['staged_files_scanned'] = 2
    status['progress']['cold_entries_scanned'] = 100
    (workspace/'migration/futures-e-cold-inventory-status.json').write_text(json.dumps(status))
    row, = monitor.workspace_cold_inventory_tasks(tmp_path, workspace)
    assert row['phase'] == 'workspace_cold_compare_tail' and not row['complete_evidence']
    monkeypatch.setattr(worker, 'current_head_identity', lambda: (_ for _ in ()).throw(OSError('D unavailable')))
    row, = monitor.workspace_cold_inventory_tasks(tmp_path, workspace)
    assert row['state'] == 'blocked' and row['reason'] == 'futures_D_head_unavailable'
