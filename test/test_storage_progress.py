"""Storage estimates must follow the evidence clock, phase and acceptance scope."""
import json
import hashlib
import sqlite3
import time
from pathlib import Path

import pytest

from stockagent.data_sync import storage_progress as ops
from stockagent.data_sync.storage_monitor import CONTRACT as STORAGE_CONTRACT, public_status as fleet


NOW = 1793459000.


def test_offline_replay_status_reports_phase_without_private_paths(tmp_path):
    from scripts.run_offline_replay_pair_migration import CONTRACT
    name = 'offline-replay-multibasis22'
    path = tmp_path/'migration/offline-replay-pairs-v1'/name/'status.json'
    save(path, {'contract': CONTRACT, 'dataset': name, 'state': 'running', 'observed_at_epoch': NOW,
                'source': '/private/path', 'source_identity': 'a'*64, 'files': 10, 'logical_bytes': 1000,
                'progress': {'stage': 'streaming_source_into_E_packs', 'files_completed': 4,
                             'logical_bytes_processed': 200}})
    first, second, _third = ops.offline_replay_pair_tasks(tmp_path, workspace_checked=True)
    assert (first['state'], first['phase'], first['completed_bytes'], first['total_bytes']) == (
        'running', 'offline_replay_pack', 200, 1000)
    assert second['state'] == 'waiting' and not first['complete_evidence']
    first['checked_at_utc'] = second['checked_at_utc'] = ops.iso(NOW)
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [first, second]}, now=NOW)
    assert len(public['tasks']) == 2 and '當沖多基底 22 舊回放' in public['tasks'][0]['label']
    assert '/private/path' not in json.dumps(public, ensure_ascii=False)
    assert public['tasks'][1]['eta']['reason'] == 'staging_queued'


def test_offline_replay_completion_requires_D_retirement_and_E_cleanup(tmp_path):
    from scripts.run_offline_replay_pair_migration import CONTRACT
    name = 'offline-replay-multibasis22'
    path = tmp_path/'migration/offline-replay-pairs-v1'/name/'status.json'
    value = {'contract': CONTRACT, 'dataset': name, 'state': 'completed', 'observed_at_epoch': NOW,
             'D_archive_committed': True, 'D_manifest_sha256': 'a'*64, 'C_payload_retired': True,
             'C_retirement': {'state': 'paired_payload_retired', 'files_removed': 2},
             'E_archive_cleanup': {'state': 'completed'},
             'progress': {'stage': 'retiring_C_pair', 'files_completed': 1, 'files_total': 2}}
    save(path, value)
    first, *_ = ops.offline_replay_pair_tasks(tmp_path, workspace_checked=True)
    assert first['state'] == 'completed' and first['complete_evidence']
    assert first['completed_items'] == first['total_items'] == 2
    value['D_archive_committed'] = False
    save(path, value)
    first, *_ = ops.offline_replay_pair_tasks(tmp_path, workspace_checked=True)
    assert first['state'] != 'completed' and not first['complete_evidence']


def test_offline_replay_blocked_reason_explains_consumer_and_capacity(tmp_path):
    from scripts.run_offline_replay_pair_migration import CONTRACT
    name = 'offline-replay-multibasis22'
    path = tmp_path/'migration/offline-replay-pairs-v1'/name/'status.json'
    value = {'contract': CONTRACT, 'dataset': name, 'state': 'waiting_retry', 'observed_at_epoch': NOW,
             'error': 'offline replay has an active process consumer'}
    save(path, value)
    first, *_ = ops.offline_replay_pair_tasks(tmp_path, workspace_checked=True)
    assert first['reason'] == 'offline_replay_consumer_busy'
    value['error'] = 'D free space is below offline replay install and reserve'
    save(path, value)
    first, *_ = ops.offline_replay_pair_tasks(tmp_path, workspace_checked=True)
    assert first['reason'] == 'offline_replay_capacity'


def test_pinned_research_retirements_have_safe_distinct_public_categories(tmp_path):
    old = 'c'*64
    names = ['completed-tw-forward-'+'a'*64, 'completed-tw-forward-'+'b'*64,
             'artifact-tw-day-trade-multi-basis-22-effective-rank-projection-l1-capital10m-v1',
             'artifact-tw-day-trade-v8-combined-annual-log-cash-v5']
    save(tmp_path/'migration/source-local-exact-repack/retirement-queue-status.json', {
        'contract': 'stockagent.source_local_repack_queue.v1.retirement-status', 'observed_at_epoch': NOW,
        'entries': [{'dataset': name, 'old_manifest_sha256': old, 'mode': 'direct',
                     'state': 'waiting_retry', 'reason': 'retirement_pinned'} for name in names]})
    rows = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': rows}, now=NOW)['tasks']
    labels = [row['label'] for row in public]
    assert len(labels) == len(set(labels)) == 4
    assert sum('台股研究回傳產物' in label for label in labels) == 2
    assert sum('台股當沖研究產物' in label for label in labels) == 2
    assert all(' #' in label for label in labels)
    assert all(name not in json.dumps(public, ensure_ascii=False) for name in names)
    assert ops.source_subject('completed-tw-forward-not-a-fixed-hash') == 'other-cold-dataset'


@pytest.mark.parametrize(('state', 'visible_state', 'reason'), [
    ('ready', 'waiting', 'edge_reader_monitoring'),
    ('recovered', 'waiting', 'edge_reader_monitoring'),
    ('remote_unavailable', 'blocked', 'edge_reader_remote_unavailable'),
    ('base_not_enrolled', 'blocked', 'edge_reader_base_missing'),
    ('reader_intact', 'blocked', 'edge_reader_runtime_wait'),
    ('waiting_trading', 'waiting', 'edge_reader_trading_deferred'),
    ('release_mismatch', 'blocked', 'edge_reader_identity_changed'),
    ('retrying', 'blocked', 'edge_reader_retry'),
    ('disabled', 'waiting', 'edge_reader_disabled'),
])
def test_edge_reader_recovery_monitor_has_no_payload_or_nas_claim(tmp_path, state, visible_state, reason):
    save(tmp_path/'migration/edge-reader-recovery-v1/status.json', {
        'contract': 'stockagent.edge_reader_recovery_queue.v1', 'apply': True,
        'state': state, 'reader_manifest_sha256': 'a'*64, 'observed_at_epoch': NOW,
        'probe_dataset_manifest_sha256': 'b'*64, 'last_recovered_at_epoch': NOW-10,
        'remote_payload_fetched': False, 'market_payload_deleted': False, 'NAS_accepted': False})
    row, = ops.edge_reader_recovery_tasks(tmp_path, workspace_checked=True)
    assert (row['state'], row['reason'], row['node_id']) == (visible_state, reason, 'vastai1T')
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [row]}, now=NOW)
    item, = public['tasks']
    assert not item['complete_evidence'] and '不下載或驗證市場內容' in item['scope']
    assert item['metrics']['edge_reader_last_recovered_epoch'] == NOW-10
    assert 'a'*64 not in json.dumps(public)


def test_edge_reader_recovery_monitor_rejects_missing_proof_and_wrong_scope(tmp_path):
    path = tmp_path/'migration/edge-reader-recovery-v1/status.json'
    data = {'contract': 'stockagent.edge_reader_recovery_queue.v1', 'apply': True,
            'state': 'ready', 'reader_manifest_sha256': 'a'*64, 'observed_at_epoch': NOW,
            'remote_payload_fetched': False, 'market_payload_deleted': False, 'NAS_accepted': False}
    save(path, data)
    assert ops.edge_reader_recovery_tasks(tmp_path, workspace_checked=True) == []
    data['probe_dataset_manifest_sha256'] = 'b'*64
    data['NAS_accepted'] = True
    save(path, data)
    assert ops.edge_reader_recovery_tasks(tmp_path, workspace_checked=True) == []
    assert ops.edge_reader_recovery_tasks(tmp_path, workspace_checked=False) == []


@pytest.mark.parametrize(('status', 'state', 'reason'), [
    ('scan_batch_processed', 'waiting', None),
    ('deferred_existing_folder_scan', 'waiting', 'cold_scan_owner_busy'),
    ('retry_partial_failure', 'blocked', 'cold_scan_retry_error'),
])
def test_cold_scan_notification_reports_only_the_observed_source_queue(tmp_path, status, state, reason):
    path = tmp_path/'latest.json'
    save(path, {'schema_version': 1, 'status': status, 'observed_at_utc': ops.iso(NOW),
                'pending_after': 23, 'acknowledged_count': 8, 'elapsed_seconds': 19.84,
                'peer_convergence': 'not_checked', 'release_verification': 'not_checked',
                'datasets': [{'dataset': 'sensitive-private-name'}]})
    row, = ops.cold_scan_notification_tasks(path)
    assert (row['state'], row['reason']) == (state, reason)
    assert row['metrics'] == {'cold_scan_pending': 23, 'cold_scan_last_acks': 8,
                              'cold_scan_last_elapsed_seconds': 20}
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [row]}, now=NOW)
    item, = public['tasks']
    assert item['metrics']['cold_scan_pending'] == 23 and not item['complete_evidence']
    assert '不表示 Vast 已同步' in item['scope']
    assert 'sensitive-private-name' not in json.dumps(public)


def test_cold_scan_notification_rejects_invalid_or_missing_receipt(tmp_path):
    path = tmp_path/'latest.json'
    assert ops.cold_scan_notification_tasks(path) == []
    save(path, {'schema_version': 1, 'status': ['invalid']})
    assert ops.cold_scan_notification_tasks(path) == []


@pytest.mark.parametrize(('state', 'public_state', 'phase', 'reason'), [
    ('waiting_reader', 'blocked', 'v3_reader_check', 'v3_reader_unavailable'),
    ('reader_ready', 'waiting', 'v3_reader_check', None),
    ('idle', 'waiting', 'v3_reader_idle', 'v3_reader_no_pending'),
])
def test_v3_reader_activation_monitor_never_claims_d_publication(tmp_path, state, public_state, phase, reason):
    now = time.time()
    save(tmp_path/'migration/source-local-exact-repack/v3-reader-activation-status.json', {
        'contract': 'stockagent.v3_reader_activation_queue.v1', 'apply': True,
        'state': state, 'observed_at_epoch': now, 'candidate_count': 4, 'blocked_count': 1,
        'last_activated_count': 2, 'last_activated_at_epoch': now})
    row, = ops.v3_reader_activation_tasks(tmp_path, workspace_checked=True)
    assert (row['state'], row['phase'], row['reason']) == (public_state, phase, reason)
    assert row['metrics']['v3_activated_last'] == 2 and not row['complete_evidence']
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(now), 'tasks': [row]}, now=now)
    assert len(public['tasks']) == 1 and not public['tasks'][0]['complete_evidence']
    assert '不表示 D 已安裝' in public['tasks'][0]['scope']


def test_foreign_head_monitor_distinguishes_peer_wait_from_retirement(tmp_path):
    from stockagent.data_sync import authoritative_head_handoff as authority
    now = time.time()
    root = tmp_path/'migration/authoritative-head-handoffs'
    requests = []
    for name, digest, state in (('dataset-a', 'a'*64, 'moved'), ('dataset-b', 'b'*64, 'completed')):
        request = {'dataset': name, 'old_manifest_sha256': digest}
        stage = root/name/digest
        stage.mkdir(parents=True)
        authority._save(stage, {'contract': authority.CONTRACT, 'request': request, 'state': state})
        requests.append(request)
    save(tmp_path/'migration/source-local-exact-repack/foreign-head-handoff-status.json', {
        'contract': 'stockagent.foreign_head_handoff_queue.v1', 'apply': True,
        'observed_at_epoch': now, 'candidate_count': 61,
        'recovered': [{**requests[0], 'state': 'waiting_peer'}]})
    rows = ops.foreign_head_handoff_tasks(tmp_path, workspace_checked=True)
    assert len(rows) == 2
    summary, pending = rows
    assert summary['metrics'] == {'foreign_head_candidates': 61, 'foreign_head_completed': 1,
                                  'foreign_head_pending': 1}
    assert pending['reason'] == 'foreign_head_peer_pending' and not pending['complete_evidence']
    assert pending['checked_at_utc'] == ops.iso(now)
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(now), 'tasks': rows}, now=now)
    assert len(public['tasks']) == 2 and all(not item['complete_evidence'] for item in public['tasks'])
    assert str(tmp_path) not in json.dumps(public) and 'dataset-a' not in json.dumps(public)
    assert ops.foreign_head_handoff_tasks(tmp_path, workspace_checked=False) == []


@pytest.mark.parametrize(('state', 'phase', 'reason'), [
    ('planned', 'foreign_head_recovery', 'foreign_head_recovery_pending'),
    ('peer_confirmed', 'foreign_head_release_pin', 'foreign_head_release_pin_pending'),
])
def test_foreign_head_monitor_reports_each_recovery_stage(tmp_path, state, phase, reason):
    from stockagent.data_sync import authoritative_head_handoff as authority
    request = {'dataset': 'dataset-a', 'old_manifest_sha256': 'a'*64}
    stage = tmp_path/'migration/authoritative-head-handoffs'/request['dataset']/request['old_manifest_sha256']
    stage.mkdir(parents=True)
    authority._save(stage, {'contract': authority.CONTRACT, 'request': request, 'state': state})
    save(tmp_path/'migration/source-local-exact-repack/foreign-head-handoff-status.json', {
        'contract': 'stockagent.foreign_head_handoff_queue.v1', 'apply': True,
        'observed_at_epoch': time.time(), 'candidate_count': 1})
    _summary, row = ops.foreign_head_handoff_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == phase and row['reason'] == reason and not row['complete_evidence']


def test_bybit_staging_is_a_separate_source_not_a_completed_handoff(tmp_path, monkeypatch):
    from scripts import run_workspace_source_staging as queue

    name, relative, marker = queue.BYBIT_STREAM_SCOPE
    item = {'name': name, 'source': relative, 'marker': marker,
            'catalog_source': 'data_bybit', 'budget_bytes': 1024,
            'seed_hardlinks': False, 'copy_mode': 'source_stream_no_readback'}
    monkeypatch.setattr(queue, 'ROOT', tmp_path)
    monkeypatch.setattr(queue, 'load_sources', lambda _path: [item])
    monkeypatch.setattr(queue, 'verified_report', lambda *_args: None)
    monkeypatch.setattr(ops, '_staging_e_io', lambda *_args: 7)
    workspace = tmp_path / 'workspace'
    source, destination, evidence, _ = queue.paths(item, workspace)
    save(evidence / 'intent.json', {'contract': 'workspace_exact_relocation_v1',
         'state': 'indexed', 'source': str(source), 'destination': str(destination),
         'files': 2, 'logical_bytes': 100, 'started_at_epoch': NOW - 5})
    save(evidence / 'progress.json', {'state': 'source_stream_copy', 'files_completed': 1,
         'bytes_completed': 30, 'observed_at_epoch': NOW})
    row, = ops.workspace_source_staging_tasks(tmp_path, tmp_path / 'config.json', workspace, now=NOW)
    assert (row['subject'], row['state'], row['phase']) == (
        'bybit-market', 'running', 'workspace_source_stream_copy')
    public = view(row)['tasks'][0]
    assert 'Bybit' in public['label']
    assert not public['complete_evidence']
    assert public['progress']['completed_bytes'] == 30
    assert public['progress']['total_bytes'] == 100


@pytest.mark.parametrize('done', [False, True])
def test_offline_repair_handoff_is_assessed_by_its_own_owner(done):
    staged = ops.task('workspace_source_staging', 'offline-stage', state='completed',
                      phase='workspace_source_stream_staged', observed=NOW, proof=True)
    staged['subject'] = 'tw-daytrade-offline-repair'
    handoff = ops.task('offline_repair_handoff', 'offline-handoff',
                       state='completed' if done else 'blocked',
                       phase='offline_repair_C_retired' if done else 'handoff_source_queued',
                       observed=NOW, proof=done, reason=None if done else 'source_or_copy_recheck_required')
    handoff['subject'] = 'tw-daytrade-offline-repair'
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [staged, handoff]}, now=NOW)
    flow = next(row for row in result['flow'] if row['id'] == 'e_handoff')
    assert flow['unassessed_source_count'] == 0
    assert flow['state'] == ('completed' if done else 'blocked')


def test_raw_minute_existing_d_reuse_has_its_own_honest_public_scope():
    row = ops.task('handoff_index_archive', 'example', state='running',
                   phase='handoff_source_retirement_plan', observed=NOW)
    row['subject'] = 'tw-minute-source-workspace'
    public = ops.public_status({'contract':ops.CONTRACT, 'observed_at_utc':ops.iso(NOW),
                                'tasks':[row]}, now=NOW)['tasks'][0]
    assert public['label'] == 'C 舊副本回收 · 永豐分鐘原始資料'
    assert '沿用逐項比對相符並固定的 D 封裝' in public['scope']
    assert '不代表獨立還原或 NAS 驗收' in public['scope']


def test_raw_minute_direct_public_coverage_never_creates_a_fake_merge_blocker(tmp_path, monkeypatch):
    from scripts import prepare_handoff_index_archive as cli
    from stockagent.data_sync import index_archive_existing
    name = 'tw-minute-source-workspace'
    monkeypatch.setattr(cli, 'ENROLLED', {'source':(name, ['.download.lock'], True)})
    monkeypatch.setattr(index_archive_existing, 'receipt_matches', lambda *_args:True)
    root = tmp_path/'migration/handoff-index-archive-v1'; root.mkdir(parents=True)
    receipt = {'dataset':name, 'old_manifest_sha256':'a'*64,
               'source_mode':'retained_handoff_C_index', 'state':'existing_D_head_matches_source_inventory'}
    value = {'contract':cli.CONTRACT, 'dataset':name, 'state':'C_payload_retired_root_and_locks_preserved',
             'C_payload_deleted':True, 'source_identity':'a'*64, 'prepared_sha256':'b'*64,
             'observed_at_epoch':NOW, 'D_archive_committed':True, 'D_publication':receipt,
             'D_E_stage_cleanup_complete':True,
             'C_retirement':{'state':'C_payload_retired_root_and_lock_preserved','prepared_sha256':'b'*64}}
    (root/(name+'.status.json')).write_text(json.dumps(value))
    rows = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert len(rows) == 2 and all(row['state'] == 'completed' for row in rows)
    assert rows[1]['phase'] == 'handoff_source_D_direct_reused'


def test_remote_return_projects_one_cycle_without_claiming_full_history(tmp_path):
    path = tmp_path/'remote-ingress-status.json'
    value = {'automation_contract': 'cold-return-v4-bounded-confirmations', 'state': 'cycle-complete',
             'apply': True, 'observed_at_epoch': NOW, 'observed': 52, 'eligible': 44, 'published': 1,
             'pending_return_count': 0, 'pending_return_limit': 32,
             'rows': [{'action': 'published', 'relative_root': '/private/run'},
                      *[{'action': 'return-cycle-limit', 'receipt': '/private/receipt'} for _ in range(43)]]}
    path.write_text(json.dumps(value))
    row, = ops.remote_return_tasks(path)
    public = view(row)['tasks'][0]
    assert public['state'] == 'waiting' and not public['complete_evidence']
    assert public['metrics']['remote_observed'] == 52
    assert public['metrics']['remote_eligible'] == 44
    assert public['metrics']['remote_published'] == 1
    assert public['metrics']['remote_cycle_deferred'] == 43
    assert public['progress']['percent'] is None
    assert '/private' not in json.dumps(public)


def test_remote_return_exposes_unavailable_wave_without_private_details(tmp_path):
    path = tmp_path/'return.json'
    path.write_text(json.dumps({'automation_contract':'cold-return-v4-bounded-confirmations',
        'state':'cycle-complete', 'apply':True, 'observed_at_epoch':NOW,
        'rows':[{'action':'waiting-fixed-release-unavailable','relative_root':'/secret',
                 'error_type':'SnapshotError'}]}))
    public = view(ops.remote_return_tasks(path)[0])['tasks'][0]
    assert public['state'] == 'blocked' and public['reason_code'] == 'remote_return_release_unavailable'
    assert '/secret' not in json.dumps(public)


def test_remote_return_exposes_user_deferred_restore_as_waiting(tmp_path):
    path = tmp_path/'return.json'
    path.write_text(json.dumps({'automation_contract':'cold-return-v4-bounded-confirmations',
        'state':'cycle-complete', 'apply':True, 'observed_at_epoch':NOW,
        'rows':[{'action':'confirming-held-wave',
                 'source_retirement':{'state':'recovery-deferred-user-migration','deleted':False}}]}))
    public = view(ops.remote_return_tasks(path)[0])['tasks'][0]
    assert public['state'] == 'waiting' and public['reason_code'] == 'recovery_deferred_user_migration'
    assert public['eta']['state'] == 'unknown'


def test_remote_return_displays_deferred_restore_separately_from_transport_capacity(tmp_path):
    path = tmp_path/'return.json'
    path.write_text(json.dumps({'automation_contract':'cold-return-v4-bounded-confirmations',
        'state':'cycle-complete', 'apply':True, 'observed_at_epoch':NOW,
        'observed':2, 'eligible':1, 'published':0, 'pending_return_count':3,
        'pending_return_limit':32, 'deferred_return_count':29, 'rows':[]}))
    public = view(ops.remote_return_tasks(path)[0])['tasks'][0]
    assert public['metrics']['remote_pending_confirmation'] == 3
    assert public['metrics']['remote_confirmation_limit'] == 32
    assert public['metrics']['remote_deferred_recovery'] == 29
    assert not public['complete_evidence']


def test_remote_return_projects_separately_timed_cache_pressure_observation(tmp_path):
    path = tmp_path/'return.json'
    path.write_text(json.dumps({'automation_contract':'cold-return-v4-bounded-confirmations',
        'state':'cycle-complete', 'apply':True, 'observed_at_epoch':NOW,
        'observed':0, 'eligible':0, 'published':0, 'rows':[]}))
    (tmp_path/'remote-cache-pressure.json').write_text(json.dumps({
        'contract':'stockagent.remote_cache_pressure_observation.v1','state':'checked',
        'observed_at_epoch':NOW-90, 'available_before_bytes':80, 'available_after_bytes':90,
        'removed_allocated_bytes':10, 'threshold_bytes':100, 'private_detail':'/secret'}))
    public = view(ops.remote_return_tasks(path)[0])['tasks'][0]
    assert public['metrics']['remote_cache_available_bytes'] == 90
    assert public['metrics']['remote_cache_threshold_bytes'] == 100
    assert public['metrics']['remote_cache_reclaimed_bytes'] == 10
    assert public['metrics']['remote_cache_observed_epoch'] == int(NOW-90)
    assert '/secret' not in json.dumps(public)
    cache = json.loads((tmp_path/'remote-cache-pressure.json').read_text())
    cache.update(state='partial', profile_issues=1)
    (tmp_path/'remote-cache-pressure.json').write_text(json.dumps(cache))
    public = view(ops.remote_return_tasks(path)[0])['tasks'][0]
    assert public['metrics']['remote_cache_profile_issues'] == 1
    cache.update(state='waiting', error_type='private problem')
    (tmp_path/'remote-cache-pressure.json').write_text(json.dumps(cache))
    public = view(ops.remote_return_tasks(path)[0])['tasks'][0]
    assert public['metrics']['remote_cache_failure_epoch'] == int(NOW-90)
    assert 'private problem' not in json.dumps(public)


@pytest.mark.parametrize('phase,expected', [
    ('discovering-completed-runs', 'remote_return_discovery'),
    ('transferring-from-origin', 'remote_return_transfer'),
    ('publishing-d-cold', 'remote_return_publish'),
    ('verifying-held-cold-wave', 'remote_return_confirmation'),
    ('prestage-e-transfer', 'remote_return_e_prestage'),
    ('prestage-e-content-proof', 'remote_return_e_check'),
    ('new-unrecognized-phase', 'working'),
])
def test_remote_return_running_stage_is_not_a_completion_receipt(tmp_path, phase, expected):
    path = tmp_path/'status.json'
    path.write_text(json.dumps({'automation_contract': 'cold-return-v4-bounded-confirmations',
        'state': 'cycle-running', 'phase': phase, 'observed_at_epoch': NOW,
        'source_files': 64, 'source_logical_bytes': 1000, 'current_root': '/private/run'}))
    row, = ops.remote_return_tasks(path)
    public = view(row)['tasks'][0]
    assert public['state'] == 'running' and public['phase'] == expected
    assert public['progress']['total_bytes'] == 1000
    assert public['progress']['completed_bytes'] is None and not public['complete_evidence']
    assert '/private' not in json.dumps(public)


def test_remote_return_keeps_previous_cycle_counts_during_a_new_cycle(tmp_path):
    path = tmp_path/'status.json'
    path.write_text(json.dumps({'automation_contract': 'cold-return-v4-bounded-confirmations',
        'state': 'cycle-running', 'phase': 'publishing-d-cold', 'observed_at_epoch': NOW,
        'last_completed_summary': {'observed_at_epoch': NOW-100, 'observed': 52, 'eligible': 44,
            'published': 1, 'deferred': 43, 'pending_return_count': 0, 'pending_return_limit': 32}}))
    public = view(ops.remote_return_tasks(path)[0])['tasks'][0]
    assert public['state'] == 'running' and not public['complete_evidence']
    assert public['metrics']['remote_cycle_deferred'] == 43
    assert public['metrics']['remote_summary_epoch'] == int(NOW-100)
    assert public['progress']['percent'] is None


def test_remote_return_failure_and_capacity_are_visible_without_private_diagnostics(tmp_path):
    path = tmp_path/'status.json'
    value = {'automation_contract': 'cold-return-v4-bounded-confirmations',
             'state': 'cycle_failed', 'observed_at_epoch': NOW, 'remote_exit_code': 255,
             'diagnostics': '/private/ssh.log'}
    path.write_text(json.dumps(value))
    public = view(ops.remote_return_tasks(path)[0])['tasks'][0]
    assert public['state'] == 'blocked' and public['reason_code'] == 'remote_return_connection_failed'
    assert '/private' not in json.dumps(public)
    value.update(state='cycle-complete', apply=True, rows=[{'action': 'waiting-local-capacity'}])
    path.write_text(json.dumps(value))
    public = view(ops.remote_return_tasks(path)[0])['tasks'][0]
    assert public['state'] == 'blocked' and public['reason_code'] == 'remote_return_capacity'


def test_remote_return_rejects_unapplied_or_foreign_status(tmp_path):
    path = tmp_path/'status.json'
    path.write_text(json.dumps({'automation_contract': 'cold-return-v4-bounded-confirmations',
        'state': 'cycle-complete', 'observed_at_epoch': NOW, 'apply': False, 'rows': []}))
    row, = ops.remote_return_tasks(path)
    assert row['state'] == 'unknown' and not row['complete_evidence']
    path.write_text(json.dumps({'automation_contract': 'other',
        'state': 'cycle-running', 'observed_at_epoch': NOW}))
    row, = ops.remote_return_tasks(path)
    assert row['state'] == 'unknown' and not row['complete_evidence']


def test_source_pause_is_pending_until_worker_observes_this_request(tmp_path):
    marker = {'contract': 'stockagent.source_replication_migration_pause.v1',
              'paused': True, 'requested_at_epoch': NOW}
    (tmp_path/'source-replication-migration-pause.json').write_text(json.dumps(marker))
    path = tmp_path/'source-replication-status.json'
    path.write_text(json.dumps({'state': 'paused_for_user_migration', 'observed_at_epoch': NOW-1}))
    row, = ops.source_replication_pause_tasks(tmp_path)
    assert row['state'] == 'waiting' and not row['complete_evidence']
    path.write_text(json.dumps({'state': 'paused_for_user_migration', 'observed_at_epoch': NOW+1}))
    row, = ops.source_replication_pause_tasks(tmp_path)
    assert row['state'] == 'completed' and row['complete_evidence']


def test_handoff_source_progress_separates_E_preparation_and_C_retirement(tmp_path):
    root = tmp_path/'migration/handoff-source-repack-v1'
    root.mkdir(parents=True)
    path = root/'coinmetrics-community.status.json'
    value = {'contract': 'stockagent.handoff_source_repack.v1', 'dataset': 'coinmetrics-community',
             'state': 'running', 'original_manifest_sha256': 'a'*64, 'observed_at_epoch': NOW,
             'files_total': 10, 'logical_bytes_total': 100,
             'progress': {'stage': 'streaming_source_into_E_packs', 'files_completed': 5,
                          'logical_bytes_processed': 60}}
    path.write_text(json.dumps(value))
    row, = ops.handoff_source_repack_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'handoff_source_packing'
    assert row['completed_bytes'] == 60 and not row['complete_evidence']
    value.update(state='E_source_hashed_prepared', C_payload_deleted=False)
    path.write_text(json.dumps(value))
    row, = ops.handoff_source_repack_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['phase'] == 'handoff_source_prepared'
    value.update(state='C_payload_retired_root_and_lock_preserved', C_payload_deleted=True,
                 prepared_sha256='b'*64, C_retirement={'state': 'C_payload_retired_root_and_lock_preserved',
                 'prepared_sha256': 'b'*64, 'files_removed': 9, 'logical_bytes_removed': 100,
                 'estimated_allocated_bytes_reclaimed': 112})
    path.write_text(json.dumps(value))
    row, = ops.handoff_source_repack_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'completed' and row['complete_evidence']
    assert row['metrics']['reclaimed_allocated_bytes'] == 112


def test_handoff_queue_shows_queued_retry_and_unpublished_D_independently(tmp_path):
    root = tmp_path/'migration/handoff-source-repack-v1'
    root.mkdir(parents=True)
    value = {'contract': 'stockagent.handoff_source_repack.v1', 'dataset': 'coinmetrics-community',
             'state': 'C_payload_retired_root_and_lock_preserved', 'C_payload_deleted': True,
             'D_new_head_committed': False, 'prepared_sha256': 'a'*64, 'observed_at_epoch': NOW,
             'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64}}
    (root/'coinmetrics-community.status.json').write_text(json.dumps(value))
    (root/'queue-status.json').write_text(json.dumps({'contract': 'stockagent.handoff_source_repack_queue.v1',
        'observed_at_epoch': NOW, 'entries': [{'dataset': 'coinmetrics-community', 'state': 'completed'},
            {'dataset': 'crypto-reference', 'state': 'waiting'}, {'dataset': 'dune-crypto', 'state': 'retrying'}]}))
    rows = ops.handoff_source_repack_tasks(tmp_path, workspace_checked=True)
    assert [(row['subject'], row['phase'], row['state']) for row in rows] == [
        ('coinmetrics-community', 'handoff_source_retired', 'completed'),
        ('coinmetrics-community', 'handoff_source_D_waiting', 'blocked'),
        ('crypto-reference', 'handoff_source_queued', 'waiting'),
        ('dune-crypto', 'handoff_source_queued', 'blocked')]
    assert not rows[1]['complete_evidence'] and not rows[2]['complete_evidence']
    (root/'D-queue-status.json').write_text(json.dumps({'contract': 'stockagent.handoff_private_D_queue.v1',
        'observed_at_epoch': NOW, 'entries': [{'dataset': 'coinmetrics-community',
        'state': 'waiting_priority', 'error': 'E_only_priority'}]}))
    row = next(row for row in ops.handoff_source_repack_tasks(tmp_path, workspace_checked=True)
               if row['kind'] == 'handoff_private_d')
    assert row['state'] == 'waiting' and row['reason'] == 'waiting_E_only_priority'


def test_private_D_retirement_shows_busy_owner_then_exact_retirement(tmp_path):
    root = tmp_path/'migration/handoff-source-repack-v1'
    root.mkdir(parents=True)
    value = {'contract': 'stockagent.handoff_source_repack.v1', 'dataset': 'coinmetrics-community',
             'state': 'C_payload_retired_root_and_lock_preserved', 'C_payload_deleted': True,
             'original_manifest_sha256': 'c'*64, 'prepared_sha256': 'a'*64,
             'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64},
             'D_new_head_committed': True, 'D_E_stage_cleanup_complete': True,
             'D_publication': {'dataset': 'coinmetrics-community', 'old_manifest_sha256': 'c'*64,
                 'new_manifest_sha256': 'd'*64, 'state': 'new_D_head_committed_from_exact_E_source'}}
    (root/'coinmetrics-community.status.json').write_text(json.dumps(value))
    (root/'queue-status.json').write_text(json.dumps({'contract': 'stockagent.handoff_source_repack_queue.v1',
        'entries': [{'dataset': 'coinmetrics-community', 'state': 'completed'}]}))
    (root/'D-retirement-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.handoff_private_D_retirement_queue.v1',
        'entries': [{'dataset': 'coinmetrics-community', 'state': 'retrying', 'error': 'owner_busy'}]}))
    rows = ops.handoff_source_repack_tasks(tmp_path, workspace_checked=True)
    row = next(row for row in rows if row['kind'] == 'handoff_private_retirement')
    assert row['state'] == 'blocked' and row['reason'] == 'handoff_private_old_owner_busy'
    value.update(D_old_release_removed=True, D_retirement={'state': 'completed', 'apply': True,
        'old_manifest_sha256': 'c'*64, 'new_manifest_sha256': 'd'*64, 'estimated_allocated_bytes_reclaimed': 123})
    (root/'coinmetrics-community.status.json').write_text(json.dumps(value))
    rows = ops.handoff_source_repack_tasks(tmp_path, workspace_checked=True)
    row = next(row for row in rows if row['kind'] == 'handoff_private_retirement')
    assert row['state'] == 'completed' and row['complete_evidence']


def test_prepack_progress_requires_live_owner_and_never_claims_C_retirement(tmp_path, monkeypatch):
    from scripts import prepare_handoff_index_archive as cli
    from scripts import run_handoff_index_archive_queue as archive_queue
    from scripts import run_handoff_index_prepack_queue as prepack_queue
    root = tmp_path/'migration/handoff-index-archive-v1'; root.mkdir(parents=True)
    monkeypatch.setattr(cli, 'ENROLLED', {'source':('dataset', [], False)})
    monkeypatch.setattr(archive_queue, 'lock_owner_pid', lambda _path:42)
    (root/'prepack-queue-status.json').write_text(json.dumps({
        'contract':prepack_queue.CONTRACT, 'state':'running', 'source':'source', 'dataset':'dataset',
        'owner_pid':42, 'observed_at_epoch':time.time()}))
    value = {'contract':cli.CONTRACT, 'dataset':'dataset', 'state':'running', 'source_identity':'a'*64,
             'observed_at_epoch':time.time(), 'files_total':100, 'logical_bytes_total':1000,
             'progress':{'stage':'streaming_source_into_E_packs', 'files_completed':30,
                         'logical_bytes_processed':300}}
    path = root/'dataset.status.json'; path.write_text(json.dumps(value))
    row, = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'handoff_source_packing'
    assert row['completed_bytes'] == 300 and row['_handoff_prepack_owner']
    assert not row['complete_evidence']
    monkeypatch.setattr(archive_queue, 'lock_owner_pid', lambda _path:None)
    row, = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and not row['complete_evidence']
    value['state'] = 'E_index_source_hashed_prepared'; path.write_text(json.dumps(value))
    monkeypatch.setattr(prepack_queue, 'prepared', lambda *_args:True)
    row, = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['phase'] == 'handoff_source_E_prepared'
    assert not row['complete_evidence']


def test_index_archive_does_not_treat_dead_owner_heartbeat_as_running(tmp_path, monkeypatch):
    from scripts import prepare_handoff_index_archive as cli
    from scripts import run_handoff_index_archive_queue as archive_queue
    root = tmp_path/'migration/handoff-index-archive-v1'; root.mkdir(parents=True)
    monkeypatch.setattr(cli, 'ENROLLED', {'source':('dataset', [], False)})
    (root/'queue-status.json').write_text(json.dumps({'contract':archive_queue.CONTRACT,
        'owner_pid':42, 'observed_at_epoch':time.time(),
        'entries':[{'dataset':'dataset', 'source':'source', 'state':'running'}]}))
    (root/'dataset.status.json').write_text(json.dumps({'contract':cli.CONTRACT, 'dataset':'dataset',
        'state':'running', 'observed_at_epoch':time.time(),
        'progress':{'stage':'retiring_C_payload', 'files_completed':5, 'files_total':10}}))
    monkeypatch.setattr(archive_queue, 'lock_owner_pid', lambda _path:42)
    row, = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'handoff_source_retiring'
    queue_path = root/'queue-status.json'
    current = json.loads(queue_path.read_text())
    current['implementation_sha256'] = next(iter(ops.LEGACY_ARCHIVE_OWNER_WALL_LIMITS))
    current['entries'][0]['started_at_epoch'] = time.time()-100
    queue_path.write_text(json.dumps(current))
    row, = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert row['reason'] == 'handoff_archive_legacy_timeout' and row['_legacy_archive_deadline'] > time.time()
    before_restart = row['generation']
    current['entries'][0]['started_at_epoch'] += 1
    queue_path.write_text(json.dumps(current))
    row, = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert row['generation'] != before_restart
    current['implementation_sha256'] = 'f'*64
    queue_path.write_text(json.dumps(current))
    row, = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert '_legacy_archive_deadline' not in row
    monkeypatch.setattr(archive_queue, 'lock_owner_pid', lambda _path:None)
    row, = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and not row['complete_evidence']


def test_legacy_archive_deadline_suppresses_an_impossible_completion_estimate():
    deadline = 2_000_000_000
    row = {'_legacy_archive_deadline':deadline,
           'eta':{'state':'estimated', 'completion_at_utc':ops.iso(deadline+60),
                  'remaining_seconds':300, 'rate_per_second':10}}
    ops.apply_legacy_archive_deadline(row)
    assert row['eta']['state'] == 'unknown' and row['eta']['completion_at_utc'] is None
    assert row['eta']['reason'] == 'handoff_archive_legacy_timeout'
    assert row['owner_deadline_at_utc'] == ops.iso(deadline)
    row = {'_legacy_archive_deadline':deadline,
           'eta':{'state':'estimated', 'completion_at_utc':ops.iso(deadline-60)}}
    ops.apply_legacy_archive_deadline(row)
    assert row['eta']['state'] == 'estimated'


def test_futures_D_coverage_reports_only_bound_inventory_evidence(tmp_path, monkeypatch):
    from scripts import run_handoff_index_archive_queue as archive_queue, run_handoff_index_coverage_queue as coverage
    from stockagent.data_sync import index_archive_existing, stream_repack
    root = tmp_path/'migration/handoff-index-archive-v1'; root.mkdir(parents=True)
    dataset, identity = 'fixture', 'a'*64
    monkeypatch.setattr(coverage, 'DATASETS', (dataset,))
    monkeypatch.setattr(archive_queue, 'lock_owner_pid', lambda _path:42)
    source = {'source_identity':identity}
    (root/(dataset+'.status.json')).write_text(json.dumps(source))
    value = {'contract':coverage.CONTRACT, 'state':'running', 'dataset':dataset, 'source_identity':identity,
             'owner_pid':42, 'observed_at_epoch':time.time(), 'started_at_epoch':time.time()-5,
             'progress':{'files_completed':5, 'files_total':10,
                         'logical_bytes_processed':50, 'logical_bytes_total':100}}
    (root/'coverage-precheck-status.json').write_text(json.dumps(value))
    row, = ops.handoff_index_coverage_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['completed_items'] == 5 and not row['complete_evidence']
    value['progress'] = {'stage':'scanning_D_parent_inventory', 'inventory_items_completed':8192}
    (root/'coverage-precheck-status.json').write_text(json.dumps(value))
    row, = ops.handoff_index_coverage_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'handoff_index_coverage_parent_scan'
    assert row['completed_items'] == 8192 and row['total_items'] is None and row['total_bytes'] is None
    stage = root/dataset/identity; stage.mkdir(parents=True)
    proof = {'identity_sha256':'b'*64, 'snapshot_id':'snapshot', 'manifest_sha256':'c'*64,
             'files':10, 'logical_bytes':100}
    (stage/'existing-D-match.json').write_text(json.dumps(proof))
    value.update(state='completed', matched_files=10, matched_logical_bytes=100,
                 comparison_identity='b'*64, D_or_NAS_readback=False, source_files_deleted=0)
    (root/(dataset+'.coverage-status.json')).write_text(json.dumps(value))
    monkeypatch.setattr(stream_repack, '_prepared_metadata', lambda *_a, **_k: {'identity_sha256':'d'*64})
    monkeypatch.setattr(index_archive_existing, 'receipt_matches', lambda *_a, **_k:True)
    (root/'coverage-precheck-status.json').write_text(json.dumps({'contract':coverage.CONTRACT, 'state':'waiting'}))
    row, = ops.handoff_index_coverage_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'completed' and row['complete_evidence']
    value['comparison_identity'] = 'f'*64
    (root/(dataset+'.coverage-status.json')).write_text(json.dumps(value))
    row, = ops.handoff_index_coverage_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and not row['complete_evidence']
    value.update(state='retrying', reason='PackedPublicationBusy')
    (root/(dataset+'.coverage-status.json')).write_text(json.dumps(value))
    row, = ops.handoff_index_coverage_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['reason'] == 'handoff_index_coverage_owner_busy'


def test_index_archive_C_completion_does_not_claim_D_completion(tmp_path):
    root = tmp_path/'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    value = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': 'binance',
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'D_archive_committed': False, 'prepared_sha256': 'a'*64,
        'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64}}
    (root/'binance.status.json').write_text(json.dumps(value))
    (root/'queue-status.json').write_text(json.dumps({'contract': 'stockagent.handoff_index_archive_queue.v1',
        'entries': [{'dataset': 'binance', 'state': 'completed'}, {'dataset': 'okx', 'state': 'waiting'}]}))
    rows = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert sorted((row['kind'], row['state']) for row in rows) == sorted([
        ('handoff_index_archive', 'completed'), ('handoff_index_D_storage', 'blocked'),
        ('handoff_index_D', 'blocked'), ('handoff_index_archive', 'waiting')])
    assert all(not row['complete_evidence'] for row in rows if row['kind'] in {'handoff_index_D_storage', 'handoff_index_D'})
    (root/'D-queue-status.json').write_text(json.dumps({'contract': 'stockagent.handoff_index_private_D_queue.v1',
        'observed_at_epoch': NOW, 'entries': [{'dataset': 'binance',
        'state': 'waiting_priority', 'error': 'E_only_priority'}]}))
    row = next(row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if row['kind'] == 'handoff_index_D_storage')
    assert row['state'] == 'waiting' and row['reason'] == 'waiting_E_only_priority'
    (root/'D-queue-status.json').write_text(json.dumps({'contract': 'stockagent.handoff_index_private_D_queue.v1',
        'observed_at_epoch': NOW, 'entries': [{'dataset': 'binance',
        'state': 'retrying', 'error': 'owner_busy', 'retry_after_epoch': NOW + 15}]}))
    row = next(row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if row['kind'] == 'handoff_index_D_storage')
    assert row['state'] == 'waiting' and row['reason'] == 'handoff_index_D_owner_busy'
    assert row['next_attempt_at_utc'] == ops.iso(NOW + 15)
    assert 'tw-minute-train-workspace' in ops.SUBJECTS


def test_index_archive_reports_each_inventory_phase_without_mixing_rates(tmp_path):
    root = tmp_path/'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    name = 'news-fulltext-bodies-workspace'
    value = {'contract':'stockagent.handoff_index_archive.v1', 'dataset':name,
             'state':'running', 'source_identity':'a'*64, 'observed_at_epoch':time.time(),
             'files_total':100, 'logical_bytes_total':10000,
             'progress':{'stage':'checking_source_namespace',
                         'inventory_items_completed':40, 'inventory_items_total':120}}
    path = root/(name+'.status.json')
    path.write_text(json.dumps(value))
    (root/'queue-status.json').write_text(json.dumps({'contract':'stockagent.handoff_index_archive_queue.v1',
        'observed_at_epoch':time.time(), 'entries':[{'dataset':name, 'state':'running'}]}))
    row = next(r for r in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True) if r['subject'] == name)
    assert row['phase'] == 'handoff_source_namespace' and row['state'] == 'running'
    assert row['completed_items'] == 40 and row['total_items'] == 120
    assert row['completed_bytes'] is None and row['total_bytes'] is None
    before = row['generation']
    value['progress']['stage'] = 'rechecking_source_namespace'
    value['progress']['inventory_items_completed'] = 0
    path.write_text(json.dumps(value))
    row = next(r for r in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True) if r['subject'] == name)
    assert row['generation'] != before and row['completed_items'] == 0


def test_existing_D_inventory_comparison_reports_its_actual_work_counters():
    counters = ops._D_progress_counters({'stage': 'comparing_D_existing_inventory',
        'inventory_items_completed': 27, 'files_completed': 20, 'files_total': 40,
        'logical_bytes_processed': 500, 'logical_bytes_total': 1000})
    assert counters == {'done_items': 20, 'total_items': 40, 'done_bytes': 500, 'total_bytes': 1000}
    assert ops.SUBJECTS['tw-futures-universe725-workspace'] == '台灣期貨 725 標的訓練來源'


def test_D_publisher_parent_scan_reports_unbounded_inventory_progress(tmp_path):
    root = tmp_path/'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    name = 'tw-futures-universe765-workspace'
    value = {'contract':'stockagent.handoff_index_archive.v1', 'dataset':name,
             'state':'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted':True,
             'source_identity':'b'*64, 'prepared_sha256':'a'*64,
             'C_retirement':{'state':'C_payload_retired_root_and_lock_preserved', 'prepared_sha256':'a'*64},
             'D_progress':{'stage':'scanning_D_parent_inventory', 'inventory_items_completed':8192},
             'observed_at_epoch':time.time()}
    (root/(name+'.status.json')).write_text(json.dumps(value))
    (root/'D-queue-status.json').write_text(json.dumps({'contract':'stockagent.handoff_index_private_D_queue.v1',
        'observed_at_epoch':time.time(), 'entries':[{'dataset':name, 'state':'running'}]}))
    row = next(row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if row['kind'] == 'handoff_index_D_storage')
    assert row['state'] == 'running' and row['phase'] == 'handoff_index_D_parent_scan'
    assert row['completed_items'] == 8192 and row['total_items'] is None and not row['complete_evidence']
    value['D_progress'] = {'stage':'comparing_D_existing_inventory', 'files_completed':95, 'files_total':100}
    (root/(name+'.status.json')).write_text(json.dumps(value))
    (root/'D-queue-status.json').write_text(json.dumps({'contract':'stockagent.handoff_index_private_D_queue.v1',
        'observed_at_epoch':time.time(), 'entries':[{'dataset':name, 'state':'retrying', 'error':'owner_busy'}]}))
    row = next(row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if row['kind'] == 'handoff_index_D_storage')
    assert row['state'] == 'waiting' and row['completed_items'] is None and row['total_items'] is None


def test_D_queue_including_offline_repair_datasets_reports_live_publication(tmp_path, monkeypatch):
    from scripts import prepare_handoff_index_archive as archive, prepare_offline_repair_archive as offline

    monkeypatch.setattr(archive, 'ENROLLED', {'source': ('bybit', ['.download.lock'], True)})
    root = tmp_path / 'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    value = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': 'bybit',
             'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
             'source_identity': 'b' * 64, 'prepared_sha256': 'a' * 64,
             'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a' * 64},
             'D_progress': {'stage': 'streaming_D_object', 'encoded_bytes_streamed': 30,
                            'encoded_bytes_total': 100, 'objects_completed': 0, 'objects_total': 1},
             'observed_at_epoch': time.time()}
    (root / 'bybit.status.json').write_text(json.dumps(value))
    entries = [{'dataset': 'bybit', 'state': 'running'}]
    entries.extend({'dataset': name, 'state': 'completed'} for name in offline.DATASET_SCOPES)
    (root / 'D-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.handoff_index_private_D_queue.v1',
        'observed_at_epoch': time.time(), 'entries': entries}))
    row = next(row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if row['kind'] == 'handoff_index_D_storage')
    assert row['state'] == 'running' and row['phase'] == 'handoff_source_D_installing'
    assert row['completed_bytes'] == 30 and row['total_bytes'] == 100
    assert not row['complete_evidence']


def test_index_archive_capacity_is_bound_to_prepared_identity_and_refreshed_observation(tmp_path):
    from stockagent.data_sync import stream_repack
    from scripts.run_stream_packed_repack import D_INSTALL_RESERVE
    root = tmp_path/'migration/handoff-index-archive-v1'
    identity = 'c'*64
    stage = root/'binance'/identity
    stage.mkdir(parents=True)
    prepared = {'contract': stream_repack.CONTRACT+'.prepared', 'dataset': 'binance',
        'old_manifest_sha256': identity, 'destination_independent_readback': False,
        'source_member_sha_streamed': True, 'raw_extraction_tree_created': False,
        'source_mode': 'retained_handoff_C_index',
        'packages': [{'sha256': 'd'*64, 'bytes': 80}], 'inventory': {'sha256': 'e'*64, 'bytes': 5}}
    prepared['identity_sha256'] = stream_repack._identity(prepared)
    (stage/'prepared.json').write_text(json.dumps(prepared))
    value = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': 'binance',
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'source_identity': identity, 'prepared_sha256': prepared['identity_sha256'],
        'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved',
                         'prepared_sha256': prepared['identity_sha256']}, 'observed_at_epoch': 1}
    (root/'binance.status.json').write_text(json.dumps(value))
    (root/'D-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.handoff_index_private_D_queue.v1', 'observed_at_epoch': 1000,
        'entries': [{'dataset': 'binance', 'state': 'retrying', 'error': 'capacity'}]}))
    row = next(x for x in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if x['kind'] == 'handoff_index_D_storage')
    assert row['_D_encoded_remaining_bytes'] == 85 and ops.timestamp(row['observed_at_utc']) == 1000
    ops._apply_stream_D_capacity([row], {'state': 'measured', 'observed_at_epoch': 1000,
                                           'available_bytes': D_INSTALL_RESERVE+10}, now=1001)
    assert row['metrics']['D_shortfall_bytes'] == 75 and row['metrics']['D_capacity_live'] == 1
    assert row['state'] == 'blocked'
    ops._apply_stream_D_capacity([row], {'state': 'measured', 'observed_at_epoch': 1001,
                                           'available_bytes': D_INSTALL_RESERVE+85}, now=1002)
    assert row['state'] == 'waiting' and row['reason'] == 'staging_queued'
    assert row['metrics']['D_shortfall_bytes'] == 0
    prepared['packages'][0]['bytes'] = 70  # Stale/tampered identity cannot yield an estimate.
    (stage/'prepared.json').write_text(json.dumps(prepared))
    row = next(x for x in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if x['kind'] == 'handoff_index_D_storage')
    assert '_D_encoded_remaining_bytes' not in row


def test_index_archive_D_publication_is_distinct_from_public_integration(tmp_path):
    root = tmp_path/'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    value = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': 'okx',
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'source_identity': 'c'*64, 'prepared_sha256': 'a'*64,
        'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64},
        'D_archive_committed': True, 'D_E_stage_cleanup_complete': True,
        'D_publication': {'dataset': 'okx', 'old_manifest_sha256': 'c'*64,
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'retained_handoff_C_index'}}
    (root/'okx.status.json').write_text(json.dumps(value))
    rows = ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
    assert [(row['kind'], row['state']) for row in rows] == [('handoff_index_archive', 'completed'),
        ('handoff_index_D_storage', 'completed'), ('handoff_index_D', 'blocked')]
    assert rows[0]['phase'] == 'handoff_index_C_retired_D' and not rows[2]['complete_evidence']


def test_legacy_parquet_index_archive_is_reported_with_all_registered_queue_entries(tmp_path):
    root = tmp_path/'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    value = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': 'legacy-parquet',
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'source_identity': 'c'*64, 'prepared_sha256': 'a'*64,
        'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved',
                         'prepared_sha256': 'a'*64, 'files_removed': 750,
                         'logical_bytes_removed': 173360411, 'estimated_allocated_bytes_reclaimed': 174903296},
        'D_archive_committed': True, 'D_E_stage_cleanup_complete': True,
        'D_publication': {'dataset': 'legacy-parquet', 'old_manifest_sha256': 'c'*64,
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'retained_handoff_C_index'}}
    (root/'legacy-parquet.status.json').write_text(json.dumps(value))
    (root/'queue-status.json').write_text(json.dumps({'contract': 'stockagent.handoff_index_archive_queue.v1',
        'observed_at_epoch': NOW, 'entries': [
            {'dataset': name, 'state': 'completed'} for name in
            ('legacy-parquet', 'free-public-context', 'public-economic-history', 'forex-frankfurter',
             'binance', 'binance-public-archive', 'okx')]}))
    rows = [row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
            if row.get('subject') == 'legacy-parquet']
    assert [(row['kind'], row['state']) for row in rows] == [
        ('handoff_index_archive', 'completed'), ('handoff_index_D_storage', 'completed'),
        ('handoff_index_D', 'blocked')]
    assert rows[0]['completed_items'] == 750 and rows[0]['metrics']['reclaimed_allocated_bytes'] == 174903296
    assert rows[1]['complete_evidence'] and not rows[2]['complete_evidence']


def test_private_existing_D_match_is_visible_without_public_integration(tmp_path):
    from stockagent.data_sync import index_archive_existing as existing, stream_repack
    root = tmp_path/'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    dataset = 'public-economic-history'
    proof = {'contract': existing.CONTRACT, 'dataset': dataset, 'source_identity': 'c'*64,
             'prepared_sha256': 'a'*64, 'snapshot_id': 'economic-private', 'manifest_sha256': 'd'*64,
             'root_name': 'data_public_economic', 'excluded_coordination_paths': list(existing.REVIEWED[dataset][1]),
             'matched_items': 10, 'files': 8, 'logical_bytes': 100,
             'source_payload_read': False, 'independent_D_readback': False, 'NAS_accepted': False}
    proof['identity_sha256'] = stream_repack._identity(proof)
    receipt = {'dataset': dataset, 'old_manifest_sha256': 'c'*64,
               'new_manifest_sha256': 'd'*64, 'new_snapshot_id': 'economic-private',
               'state': 'existing_D_head_matches_source_inventory', 'source_mode': 'retained_handoff_C_index',
               'files': 8, 'logical_bytes': 100, 'existing_match': proof}
    value = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': dataset,
             'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
             'source_identity': 'c'*64, 'prepared_sha256': 'a'*64,
             'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64},
             'D_archive_committed': True, 'D_E_stage_cleanup_complete': True, 'D_publication': receipt}
    (root/(dataset+'.status.json')).write_text(json.dumps(value))
    rows = [row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
            if row.get('subject') == dataset]
    assert [row['kind'] for row in rows] == ['handoff_index_archive', 'handoff_index_D_storage']
    assert rows[1]['phase'] == 'handoff_source_D_reused' and rows[1]['complete_evidence']
    public = view(rows[1])['tasks'][0]
    assert public['phase'] == 'handoff_source_D_reused' and '既有版本' in public['phase_label']


def test_inventory_stream_progress_is_a_separate_uncommitted_phase():
    row = ops.task('source_local_repack_queue', 'fixture', state='running',
                   phase='source_local_D_validating', observed=NOW-60, generation='fixed',
                   done_items=20, total_items=100)
    _, history = ops.estimate(row, {}, now=NOW-60)
    row.update(observed_at_utc=ops.iso(NOW), completed_items=40)
    row['eta'], _ = ops.estimate(row, history, now=NOW)
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [row]}, now=NOW)['tasks'][0]
    assert public['phase_label'] == '串流檢查新版本清冊'
    assert public['progress']['metric'] == 'items' and public['progress']['percent'] == 40
    assert public['eta']['remaining_seconds'] == 180 and public['complete_evidence'] is False


def test_index_comparison_displays_bound_counts_without_claiming_merge(tmp_path, monkeypatch):
    monkeypatch.setattr(ops, '_IntegrationGenerationObserver', lambda: lambda *_: 'current')
    root = tmp_path/'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    value = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': 'okx',
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'source_identity': 'c'*64, 'prepared_sha256': 'a'*64,
        'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64},
        'D_archive_committed': True, 'D_E_stage_cleanup_complete': True,
        'D_publication': {'dataset': 'okx', 'old_manifest_sha256': 'c'*64,
            'new_manifest_sha256': 'd'*64, 'new_snapshot_id': 'okx-private',
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'retained_handoff_C_index'}}
    (root/'okx.status.json').write_text(json.dumps(value))
    comparison = {'contract': 'stockagent.handoff_index_integration_plan.v1', 'dataset': 'okx',
        'state': 'metadata_compared', 'private_manifest_sha256': 'd'*64, 'private_snapshot_id': 'okx-private',
        'semantic_merge_complete': False, 'counters': {'equal': 5, 'public_only': 2, 'private_only': 1,
                                                       'private_excluded': 3, 'conflict': 7}}
    (root/'okx.diff-status.json').write_text(json.dumps(comparison))
    (root/'integration-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.handoff_index_integration_queue.v1', 'observed_at_epoch': time.time(),
        'entries': [{'dataset': 'okx', 'state': 'completed'}]}))
    row = next(row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if row['kind'] == 'handoff_index_D')
    assert row['phase'] == 'handoff_index_compared' and not row['complete_evidence']
    assert row['metrics']['source_conflict_items'] == 7 and row['metrics']['source_excluded_items'] == 3
    comparison['counters'] = {'equal': 750, 'public_only': 0, 'private_only': 0,
                              'private_excluded': 0, 'unexpected_public_excluded': 0, 'conflict': 0}
    (root/'okx.diff-status.json').write_text(json.dumps(comparison))
    row = next(row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if row['kind'] == 'handoff_index_D')
    assert row['phase'] == 'handoff_index_equal_pending' and not row['complete_evidence']
    (root/'integration-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.handoff_index_integration_queue.v1', 'observed_at_epoch': time.time(),
        'entries': [{'dataset': 'okx', 'state': 'retrying', 'error': 'owner_busy',
                     'retry_after_epoch': time.time()+15}]}))
    row = next(row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if row['kind'] == 'handoff_index_D')
    assert row['state'] == 'waiting' and row['phase'] == 'handoff_index_integration_waiting'
    assert row['reason'] == 'handoff_index_integration_owner_busy' and row['next_attempt_at_utc']
    (root/'integration-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.handoff_index_integration_queue.v1', 'observed_at_epoch': time.time(),
        'entries': [{'dataset': 'okx', 'state': 'completed'}]}))
    comparison['private_manifest_sha256'] = 'e'*64
    (root/'okx.diff-status.json').write_text(json.dumps(comparison))
    row = next(row for row in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
               if row['kind'] == 'handoff_index_D')
    assert row['phase'] == 'handoff_index_D_archived' and row['metrics']['source_conflict_items'] is None


@pytest.mark.parametrize('compacted', [False, True])
def test_index_generation_integration_requires_matching_current_head_and_repack_lineage(tmp_path, compacted, monkeypatch):
    monkeypatch.setattr(ops, '_IntegrationGenerationObserver', lambda: lambda *_: 'current')
    root = tmp_path/'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    name = 'forex-frankfurter'
    value = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': name,
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'source_identity': 'c'*64, 'prepared_sha256': 'a'*64,
        'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64},
        'D_archive_committed': True, 'D_E_stage_cleanup_complete': True,
        'D_publication': {'dataset': name, 'old_manifest_sha256': 'c'*64,
            'new_manifest_sha256': 'd'*64, 'new_snapshot_id': 'fx-private',
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'retained_handoff_C_index'}}
    (root/(name+'.status.json')).write_text(json.dumps(value))
    head = ('e' if compacted else 'f')*64
    comparison = {'contract': 'stockagent.handoff_index_integration_plan.v1', 'dataset': name,
        'state': 'metadata_compared', 'private_manifest_sha256': 'd'*64, 'private_snapshot_id': 'fx-private',
        'public_manifest_sha256': head, 'semantic_merge_complete': False, 'counters': {}}
    (root/(name+'.diff-status.json')).write_text(json.dumps(comparison))
    (root/'integration-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.handoff_index_integration_queue.v1', 'observed_at_epoch': time.time(),
        'entries': [{'dataset': name, 'state': 'completed'}]}))
    integration = {'contract': 'stockagent.frankfurter_fixed_generation_integration.v1',
        'state': 'public_generation_integrated', 'private_original_manifest_sha256': 'd'*64,
        'new_public_manifest_sha256': 'f'*64, 'public_only_rows': 0, 'conflicting_overlap_rows': 0}
    (root/name).mkdir()
    (root/name/'frankfurter-semantic-integration-v1.json').write_text(json.dumps(integration))
    def row():
        return next(item for item in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
                    if item['kind'] == 'handoff_index_D')
    if compacted:
        assert not row()['complete_evidence']
        repack = tmp_path/'migration/source-local-exact-repack'/name/(head+'.json')
        save(repack, {'contract': 'stockagent.source_local_exact_repack.v1', 'dataset': name,
            'old_manifest_sha256': 'f'*64, 'new_manifest_sha256': head,
            'state': 'new_D_head_committed_from_exact_E_source'})
    assert row()['complete_evidence'] and row()['state'] == 'completed'
    comparison['private_manifest_sha256'] = '0'*64
    (root/(name+'.diff-status.json')).write_text(json.dumps(comparison))
    assert not row()['complete_evidence']


def test_binance_integration_requires_current_public_and_private_generations(tmp_path, monkeypatch):
    monkeypatch.setattr(ops, '_IntegrationGenerationObserver', lambda: lambda *_: 'current')
    root = tmp_path/'migration/handoff-index-archive-v1'
    name = 'binance-public-archive'
    value = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': name,
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'source_identity': 'c'*64, 'prepared_sha256': 'a'*64,
        'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64},
        'D_archive_committed': True, 'D_E_stage_cleanup_complete': True,
        'D_publication': {'dataset': name, 'old_manifest_sha256': 'c'*64,
            'new_manifest_sha256': 'd'*64, 'new_snapshot_id': 'private-fixed',
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'retained_handoff_C_index'}}
    save(root/(name+'.status.json'), value)
    comparison = {'contract': 'stockagent.handoff_index_integration_plan.v1', 'dataset': name,
        'state': 'metadata_compared', 'private_manifest_sha256': 'd'*64, 'private_snapshot_id': 'private-fixed',
        'public_manifest_sha256': 'e'*64, 'semantic_merge_complete': False, 'counters': {}}
    save(root/(name+'.diff-status.json'), comparison)
    save(root/'integration-queue-status.json', {'contract': 'stockagent.handoff_index_integration_queue.v1',
        'observed_at_epoch': time.time(), 'entries': [{'dataset': name, 'state': 'completed'}]})
    promotion = {'contract': 'stockagent.semantic_link_promotion.v1', 'state': 'committed',
        'new_manifest_sha256': 'e'*64, 'private_manifest_sha256': 'd'*64, 'observed_at_epoch': NOW}
    path = root/name/('semantic-promotion-'+'f'*64+'.json')
    save(path, promotion)
    def row():
        return next(item for item in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
                    if item['kind'] == 'handoff_index_D')
    assert row()['state'] == 'completed' and row()['complete_evidence']
    promotion['new_manifest_sha256'] = '0'*64
    save(path, promotion)
    assert row()['state'] == 'blocked' and not row()['complete_evidence']
    # A newer public head may add paths while still retaining every fixed
    # private source member with the same content identity.
    comparison['counters'] = {'equal': 10, 'conflict': 0, 'public_only': 1,
        'private_only': 0, 'private_excluded': 0, 'unexpected_public_excluded': 0}
    save(root/(name+'.diff-status.json'), comparison)
    assert row()['state'] == 'completed' and row()['complete_evidence']
    comparison['counters']['conflict'] = 1
    save(root/(name+'.diff-status.json'), comparison)
    assert row()['state'] == 'blocked' and not row()['complete_evidence']
    save(root/'integration-queue-status.json', {'contract': 'stockagent.handoff_index_integration_queue.v1',
        'observed_at_epoch': time.time(), 'entries': [{'dataset': name, 'state': 'blocked_semantic_merge',
                                                       'work': 'archive_union_required'}]})
    assert row()['reason'] == 'handoff_index_binance_union_required' and not row()['complete_evidence']


def test_binance_fixed_audit_phase_progress_requires_current_generation_and_fresh_status(tmp_path):
    root = tmp_path/'migration/handoff-index-archive-v1'
    name, identity = 'binance-public-archive', 'f'*64
    source = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': name,
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'source_identity': 'c'*64, 'prepared_sha256': 'a'*64,
        'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64},
        'D_archive_committed': True, 'D_E_stage_cleanup_complete': True,
        'D_publication': {'dataset': name, 'old_manifest_sha256': 'c'*64,
            'new_manifest_sha256': 'd'*64, 'new_snapshot_id': 'private-fixed',
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'retained_handoff_C_index'}}
    comparison = {'contract': 'stockagent.handoff_index_integration_plan.v1', 'dataset': name,
        'state': 'metadata_compared', 'comparison_identity_sha256': identity,
        'private_manifest_sha256': 'd'*64, 'private_snapshot_id': 'private-fixed',
        'public_manifest_sha256': 'e'*64, 'semantic_merge_complete': False, 'counters': {}}
    now = time.time()
    save(root/(name+'.status.json'), source)
    save(root/(name+'.diff-status.json'), comparison)
    save(root/'integration-queue-status.json', {'contract': 'stockagent.handoff_index_integration_queue.v1',
        'observed_at_epoch': now, 'entries': [{'dataset': name, 'state': 'running', 'work': 'archive_rows'}]})
    status = {'contract': 'stockagent.binance_archive_generation_audit.v1', 'state': 'running',
        'comparison_identity_sha256': identity, 'private_manifest_sha256': 'd'*64,
        'public_manifest_sha256': 'e'*64, 'stage': 'reading_fixed_private',
        'stage_files_completed': 12, 'stage_files_total': 30, 'stage_bytes_completed': 40,
        'stage_bytes_total': 100, 'observed_at_epoch': now}
    path = root/name/'row-audit-status.json'
    save(path, status)
    def row(generation='current'):
        return next(item for item in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True,
            current_generation=lambda *_: generation) if item['kind'] == 'handoff_index_D')
    active = row()
    assert active['state'] == 'running' and active['phase'] == 'handoff_index_reading_fixed_private'
    assert (active['completed_items'], active['total_items'], active['completed_bytes'], active['total_bytes']) == (12, 30, 40, 100)
    assert row('changed')['completed_items'] is None
    status['observed_at_epoch'] = now-181
    save(path, status)
    assert row()['completed_items'] is None
    status.update(observed_at_epoch=now, private_manifest_sha256='0'*64)
    save(path, status)
    assert row()['completed_items'] is None


def test_fred_pin_recovery_reports_owner_progress_without_claiming_nas(tmp_path):
    root = tmp_path/'migration/fred-exact-pin-recovery-v1'
    value = {'contract': 'stockagent.fred_exact_pin_recovery.v1', 'dataset': 'fred-crypto-macro',
        'state': 'installing_exact_objects', 'original_manifest_sha256': 'a'*64,
        'expected_objects': 65, 'installed_objects': 16, 'current_head_changed': False,
        'source_deleted': False, 'independent_recovery_verified': False, 'NAS_accepted': False,
        'observed_at_epoch': NOW}
    save(root/'status.json', value)
    row, = ops.fred_pin_recovery_tasks(tmp_path, workspace_checked=True, now=NOW)
    assert row['state'] == 'running' and (row['completed_items'], row['total_items']) == (16, 65)
    assert not row['complete_evidence']
    value.update(state='exact_pin_restored', exact_pin_resolved=True, installed_objects=65)
    save(root/'status.json', value)
    row, = ops.fred_pin_recovery_tasks(tmp_path, workspace_checked=True, now=NOW)
    assert row['state'] == 'completed' and row['complete_evidence']
    value['exact_pin_resolved'] = False
    save(root/'status.json', value)
    assert ops.fred_pin_recovery_tasks(tmp_path, workspace_checked=True, now=NOW)[0]['state'] == 'waiting'
    assert not ops.fred_pin_recovery_tasks(tmp_path, workspace_checked=False, now=NOW)


@pytest.mark.parametrize('name', ['binance', 'yahoo-market', 'legacy-parquet', 'free-public-context'])
def test_latest_selection_displays_uncompared_prior_values(tmp_path, name, monkeypatch):
    monkeypatch.setattr(ops, '_IntegrationGenerationObserver', lambda: lambda *_: 'current')
    root = tmp_path/'migration/handoff-index-archive-v1'
    value = {'contract': 'stockagent.handoff_index_archive.v1', 'dataset': name,
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'source_identity': 'c'*64, 'prepared_sha256': 'a'*64,
        'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64},
        'D_archive_committed': True, 'D_E_stage_cleanup_complete': True,
        'D_publication': {'dataset': name, 'old_manifest_sha256': 'c'*64,
            'new_manifest_sha256': 'd'*64, 'new_snapshot_id': 'private-fixed',
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'retained_handoff_C_index'}}
    save(root/(name+'.status.json'), value)
    comparison = {'contract': 'stockagent.handoff_index_integration_plan.v1',
        'dataset': name, 'state': 'metadata_compared', 'private_manifest_sha256': 'd'*64,
        'private_snapshot_id': 'private-fixed', 'public_manifest_sha256': 'e'*64,
        'semantic_merge_complete': False, 'counters': {'equal': 10, 'conflict': 0,
            'public_only': 0, 'private_only': 0, 'private_excluded': 0, 'unexpected_public_excluded': 0}}
    save(root/(name+'.diff-status.json'), comparison)
    save(root/'integration-queue-status.json', {'contract': 'stockagent.handoff_index_integration_queue.v1',
        'observed_at_epoch': time.time(), 'entries': [{'dataset': name, 'state': 'completed'}]})
    path = root/name/('latest-promotion-'+'f'*64+'.json')
    promotion = {'contract': 'stockagent.latest_fixed_link_promotion.v1', 'state': 'committed',
        'new_manifest_sha256': 'e'*64, 'private_manifest_sha256': 'd'*64,
        'changed_member_values_compared': False, 'old_historical_value_coverage': 'not_compared',
        'uncompared_changed_members': 0 if name in {'legacy-parquet', 'free-public-context'} else 12,
        'observed_at_epoch': NOW}
    save(path, promotion)
    def row():
        return next(item for item in ops.handoff_index_archive_tasks(tmp_path, workspace_checked=True)
                    if item['kind'] == 'handoff_index_D')
    expected_phase = ('handoff_index_public_exact_equal' if name in {'legacy-parquet', 'free-public-context'}
                      else 'handoff_index_public_latest_selected')
    assert row()['state'] == 'completed' and row()['phase'] == expected_phase
    assert row()['metrics']['uncompared_changed_members'] == promotion['uncompared_changed_members']
    comparison['public_manifest_sha256'] = '1'*64
    save(root/(name+'.diff-status.json'), comparison)
    assert row()['state'] == 'completed', 'a later head with identical fixed files must not look unselected'
    if name in {'legacy-parquet', 'free-public-context'}:
        comparison['counters']['public_only'] = 1
        save(root/(name+'.diff-status.json'), comparison)
        assert row()['state'] == 'completed' and row()['phase'] == 'handoff_index_public_fixed_covered'
    comparison['counters']['conflict'] = 1
    save(root/(name+'.diff-status.json'), comparison)
    assert not row()['complete_evidence'], 'a changed later head still needs its own coverage decision'
    comparison['public_manifest_sha256'] = 'e'*64
    save(root/(name+'.diff-status.json'), comparison)
    promotion['old_historical_value_coverage'] = 'complete'
    save(path, promotion)
    assert not row()['complete_evidence']
    save(root/'integration-queue-status.json', {'contract': 'stockagent.handoff_index_integration_queue.v1',
        'observed_at_epoch': time.time(), 'entries': [{'dataset': name, 'state': 'waiting_generation_changed'}]})
    assert row()['reason'] == 'handoff_index_generation_changed'


def test_stale_queue_keeps_fixed_completion_only_while_actual_generation_matches(tmp_path):
    root = tmp_path/'migration/handoff-index-archive-v1'
    name = 'bybit'
    save(root/(name+'.status.json'), {
        'contract': 'stockagent.handoff_index_archive.v1', 'dataset': name,
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'source_identity': 'c'*64, 'prepared_sha256': 'a'*64,
        'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64},
        'D_archive_committed': True, 'D_E_stage_cleanup_complete': True,
        'D_publication': {'dataset': name, 'old_manifest_sha256': 'c'*64,
            'new_manifest_sha256': 'd'*64, 'new_snapshot_id': 'private-fixed',
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'retained_handoff_C_index'}})
    save(root/(name+'.diff-status.json'), {
        'contract': 'stockagent.handoff_index_integration_plan.v1', 'dataset': name,
        'state': 'metadata_compared', 'private_manifest_sha256': 'd'*64,
        'private_snapshot_id': 'private-fixed', 'public_manifest_sha256': 'e'*64,
        'catalog_sha256': 'f'*64, 'semantic_merge_complete': False,
        'counters': {'equal': 10, 'conflict': 0, 'public_only': 0, 'private_only': 0,
                     'private_excluded': 0, 'unexpected_public_excluded': 0}})
    save(root/'integration-queue-status.json', {
        'contract': 'stockagent.handoff_index_integration_queue.v1', 'observed_at_epoch': time.time()-3600,
        'entries': [{'dataset': name, 'state': 'completed'}]})
    save(root/name/('latest-promotion-'+'f'*64+'.json'), {
        'contract': 'stockagent.latest_fixed_link_promotion.v1', 'state': 'committed',
        'new_manifest_sha256': 'e'*64, 'private_manifest_sha256': 'd'*64,
        'changed_member_values_compared': False, 'old_historical_value_coverage': 'not_compared',
        'uncompared_changed_members': 3, 'observed_at_epoch': time.time()-3600})
    def row(result):
        return next(item for item in ops.handoff_index_archive_tasks(
            tmp_path, workspace_checked=True, current_generation=lambda *_: result)
            if item['kind'] == 'handoff_index_D')
    completed = row('current')
    assert completed['state'] == 'completed' and completed['complete_evidence']
    assert completed['metrics']['uncompared_changed_members'] == 3
    changed = row('changed')
    assert not changed['complete_evidence'] and changed['reason'] == 'handoff_index_generation_changed'
    unknown = row('unavailable')
    assert unknown['state'] == 'waiting' and not unknown['complete_evidence']
    assert unknown['reason'] == 'handoff_index_binding_unavailable'


def test_integration_generation_observer_checks_catalog_and_head_each_collection(tmp_path):
    from types import SimpleNamespace
    catalog = tmp_path/'catalog.json'
    catalog.write_text('{"version":1}')
    expected = {'catalog_sha256': hashlib.sha256(catalog.read_bytes()).hexdigest(),
                'public_manifest_sha256': 'a'*64}
    calls = []
    def mount(path):
        calls.append(('mount', path))
    def resolve(path, dataset, *, require_objects):
        calls.append(('resolve', dataset, require_objects))
        return SimpleNamespace(manifest_sha256='a'*64 if dataset == 'one' else 'b'*64)
    observe = ops._IntegrationGenerationObserver(cold=tmp_path, catalog=catalog,
                                                  mount_check=mount, resolve=resolve)
    assert observe('one', expected) == 'current'
    assert observe('two', expected) == 'changed'
    assert sum(call[0] == 'mount' for call in calls) == 1
    assert all(call[-1] is False for call in calls if call[0] == 'resolve')
    catalog.write_text('{"version":2}')
    assert observe('one', expected) == 'unavailable'
    assert ops._IntegrationGenerationObserver(cold=tmp_path, catalog=catalog,
        mount_check=mount, resolve=resolve)('one', expected) == 'changed'
    assert ops._IntegrationGenerationObserver(cold=tmp_path, catalog=catalog,
        mount_check=lambda _: (_ for _ in ()).throw(OSError('unmounted')), resolve=resolve)(
            'one', expected) == 'unavailable'


@pytest.mark.parametrize('defect', [None, 'old', 'new', 'deleted', 'state'])
def test_semantic_retirement_receipt_remains_visible_after_queue_advances(tmp_path, defect):
    queue = tmp_path/'migration/source-local-exact-repack/retirement-queue-status.json'
    save(queue, {'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
                 'observed_at_epoch': NOW, 'entries': []})
    name, old, new, comparison = 'binance-public-archive', 'a'*64, 'b'*64, 'c'*64
    result = {'state': 'completed', 'apply': True, 'dataset': name,
              'old_manifest_sha256': old, 'new_manifest_sha256': new,
              'object_count': 24, 'object_bytes': 1000, 'estimated_allocated_bytes_reclaimed': 900,
              'observed_at_epoch': NOW}
    receipt = {'contract': 'stockagent.semantic_link_promotion.v1', 'dataset': name, 'state': 'committed',
               'comparison_identity_sha256': comparison, 'old_manifest_sha256': old,
               'new_manifest_sha256': new, 'old_sources_deleted': True, 'retirement': result}
    if defect == 'old': result['old_manifest_sha256'] = 'd'*64
    if defect == 'new': result['new_manifest_sha256'] = 'd'*64
    if defect == 'deleted': receipt['old_sources_deleted'] = False
    if defect == 'state': result['state'] = 'running'
    save(tmp_path/'migration/handoff-index-archive-v1'/name/('semantic-promotion-'+comparison+'.json'), receipt)
    rows = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    if defect:
        assert rows == []
    else:
        row, = rows
        assert row['subject'] == name and row['state'] == 'completed' and row['complete_evidence']
        assert row['completed_items'] == 24 and row['completed_bytes'] == 1000
        assert row['metrics']['estimated_reclaimed_allocated_bytes'] == 900


@pytest.mark.parametrize('name', ['binance', 'yahoo-market'])
def test_latest_retirement_keeps_uncompared_scope_after_queue_advances(tmp_path, name):
    queue = tmp_path/'migration/source-local-exact-repack/retirement-queue-status.json'
    save(queue, {'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
                 'observed_at_epoch': NOW, 'entries': []})
    old, new, comparison = 'a'*64, 'b'*64, 'c'*64
    result = {'state': 'completed', 'apply': True, 'dataset': name, 'old_manifest_sha256': old,
        'new_manifest_sha256': new, 'object_count': 2, 'object_bytes': 1000,
        'estimated_allocated_bytes_reclaimed': 900, 'changed_member_values_compared': False,
        'old_historical_value_coverage': 'not_compared', 'observed_at_epoch': NOW}
    receipt = {'contract': 'stockagent.latest_fixed_link_promotion.v1', 'dataset': name, 'state': 'committed',
        'comparison_identity_sha256': comparison, 'old_manifest_sha256': old, 'new_manifest_sha256': new,
        'old_sources_deleted': True, 'changed_member_values_compared': False,
        'old_historical_value_coverage': 'not_compared', 'uncompared_changed_members': 12, 'retirement': result}
    path = tmp_path/'migration/handoff-index-archive-v1'/name/('latest-promotion-'+comparison+'.json')
    save(path, receipt)
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'completed' and row['phase'] == 'source_latest_retirement_working'
    assert row['metrics']['uncompared_changed_members'] == 12
    assert row['metrics']['estimated_reclaimed_allocated_bytes'] == 900
    result['changed_member_values_compared'] = True
    save(path, receipt)
    assert ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True) == []


def test_shared_object_work_and_completed_receipt_are_reported_separately(tmp_path):
    queue = tmp_path/'migration/source-local-exact-repack/retirement-queue-status.json'
    name, old, comparison = 'okx', 'a'*64, 'b'*64
    save(queue, {'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
        'observed_at_epoch': NOW, 'entries': [{'dataset': name, 'old_manifest_sha256': old,
            'mode': 'dedup', 'state': 'waiting_retry', 'reason': 'd_e_owner_busy',
            'logical_bytes': 1000, 'retry_after_epoch': NOW+180}]})
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['kind'] == 'source_object_dedup' and row['state'] == 'waiting'
    assert row['phase'] == 'source_shared_object_working'
    assert row['metrics']['duplicate_encoded_bytes_pending'] == 1000
    public = view(row)['tasks'][0]
    assert public['kind'] == 'source_object_dedup' and '去重' in public['label']
    assert public['reason_code'] == 'd_e_owner_busy'
    save(queue, {'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
                 'observed_at_epoch': NOW+200, 'entries': []})
    fixed = {'promotion_before': {'dataset': name, 'old_manifest_sha256': old,
                                   'comparison_identity_sha256': comparison},
             'comparison_identity_sha256': comparison, 'install_before': {}, 'items': []}
    identity = hashlib.sha256(json.dumps(fixed, sort_keys=True, separators=(',', ':'),
                                         ensure_ascii=False).encode()).hexdigest()
    receipt = {'contract': 'stockagent.fixed_generation_shared_objects.v1', 'state': 'completed',
        'apply': True, 'dataset': name, 'old_manifest_sha256': old, 'plan': fixed, 'plan_sha256': identity,
        'independent_D_readback': False, 'NAS_accepted': False, 'object_count': 1, 'object_bytes': 1000,
        'estimated_allocated_bytes_reclaimed': 900, 'completed_at_epoch': NOW+190}
    path = tmp_path/'migration/handoff-index-archive-v1'/name/('shared-objects-'+comparison+'.json')
    save(path, receipt)
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['kind'] == 'source_object_dedup' and row['state'] == 'completed'
    assert row['phase'] == 'source_shared_object_completed' and row['completed_bytes'] == 1000
    assert row['metrics']['estimated_reclaimed_allocated_bytes'] == 900
    receipt['plan_sha256'] = '0'*64
    save(path, receipt)
    assert ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True) == []


def _retirement_cursor_fixture(tmp_path, *, count=1, mode='direct'):
    name, old = 'okx', 'a'*64
    root = tmp_path/'migration/source-local-exact-repack'
    save(root/'retirement-queue-status.json', {'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
        'observed_at_epoch': NOW, 'entries': [{'dataset': name, 'old_manifest_sha256': old,
                                               'mode': mode, 'state': 'running'}]})
    stage = tmp_path/'migration/stream-source-repack-v1'/name/old
    stage.mkdir(parents=True)
    plan = {'items': [{'kind': 'object', 'relative': 'private-name-'+str(i)} for i in range(3)]}
    raw = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    digest = hashlib.sha256(raw).hexdigest()
    (stage/('retirement-plan-'+digest+'.json')).write_bytes(raw+b'\n')
    journal = {'contract': ('stockagent.source_local_repack_retirement.v1' if mode == 'direct'
                            else 'stockagent.superseded_repack_history.v1'), 'dataset': name,
               'old_manifest_sha256': old, 'state': 'removing', 'plan_sha256': digest,
               'plan_ref': 'retirement-plan-'+digest+'.json', 'removed_count': count,
               'pending': ['private-unconfirmed-name']}
    filename = ('retirement.json' if mode == 'direct' else
                ('prepared-history-retirement-' if mode == 'prepared_history' else 'history-retirement-')+digest+'.json')
    save(stage/filename, journal)
    import os
    os.utime(stage/filename, (NOW, NOW))
    return root, stage, journal


def test_active_retirement_projects_only_confirmed_journal_names_and_estimates(tmp_path):
    root, stage, journal = _retirement_cursor_fixture(tmp_path)
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_repack_retirement_unlink'
    assert (row['completed_items'], row['total_items']) == (1, 3)
    assert row['completed_bytes'] is None and not row['complete_evidence']
    _, history = ops.estimate(row, {}, now=NOW)
    journal['removed_count'] = 2
    save(stage/'retirement.json', journal)
    import os
    os.utime(stage/'retirement.json', (NOW+60, NOW+60))
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    eta, _ = ops.estimate(row, history, now=NOW+60)
    assert eta['state'] == 'estimated' and eta['unit'] == 'items' and eta['remaining_seconds'] == 60
    row['eta'] = eta
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW+60),
                                'tasks': [row]}, now=NOW+60)['tasks'][0]
    assert public['progress']['completed_items'] == 2 and public['progress']['total_items'] == 3
    assert not public['complete_evidence'] and 'private-name' not in json.dumps(public)


@pytest.mark.parametrize('defect', ['changed_plan', 'wrong_generation', 'count_overflow', 'redirected_plan'])
def test_active_retirement_rejects_unbound_or_invalid_cursor(tmp_path, defect):
    _root, stage, journal = _retirement_cursor_fixture(tmp_path)
    plan = stage/journal['plan_ref']
    if defect == 'changed_plan':
        plan.write_text('{"items": []}\n')
    elif defect == 'wrong_generation':
        journal['old_manifest_sha256'] = 'b'*64
        save(stage/'retirement.json', journal)
    elif defect == 'count_overflow':
        journal['removed_count'] = 4
        save(stage/'retirement.json', journal)
    else:
        plan.unlink(); plan.symlink_to(stage/'retirement.json')
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_repack_retirement_working'
    assert row['completed_items'] is None and not row['complete_evidence']


@pytest.mark.parametrize('mode', ['history', 'prepared_history'])
def test_history_retirement_projects_only_its_active_bounded_batch(tmp_path, mode):
    _root, stage, journal = _retirement_cursor_fixture(tmp_path, count=2, mode=mode)
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_repack_history_unlink' and row['state'] == 'running'
    assert (row['completed_items'], row['total_items']) == (2, 3)
    assert not row['complete_evidence']
    prefix = 'prepared-history-retirement-' if mode == 'prepared_history' else 'history-retirement-'
    path = stage/(prefix+journal['plan_sha256']+'.json')
    journal['state'] = 'completed'
    save(path, journal)
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] != 'source_repack_history_unlink' and row['completed_items'] is None


def test_history_retirement_rejects_multiple_active_journals(tmp_path):
    _root, stage, journal = _retirement_cursor_fixture(tmp_path, mode='history')
    other = dict(journal, plan_sha256='c'*64, plan_ref='retirement-plan-'+'c'*64+'.json')
    save(stage/('history-retirement-'+'c'*64+'.json'), other)
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_repack_history_working' and row['completed_items'] is None


def test_history_estimate_does_not_reuse_the_previous_batch_rate(tmp_path):
    _root, stage, journal = _retirement_cursor_fixture(tmp_path, mode='history')
    first, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    _, history = ops.estimate(first, {}, now=NOW)
    prior = stage/('history-retirement-'+journal['plan_sha256']+'.json')
    journal['state'] = 'completed'
    save(prior, journal)
    plan = {'items': [{'kind': 'object', 'relative': 'second-'+str(i)} for i in range(3)]}
    raw = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    digest = hashlib.sha256(raw).hexdigest()
    (stage/('retirement-plan-'+digest+'.json')).write_bytes(raw+b'\n')
    journal.update(state='removing', plan_sha256=digest, plan_ref='retirement-plan-'+digest+'.json', removed_count=2)
    next_path = stage/('history-retirement-'+digest+'.json')
    save(next_path, journal)
    import os
    os.utime(next_path, (NOW+60, NOW+60))
    second, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    eta, _ = ops.estimate(second, history, now=NOW+60)
    assert first['generation'] != second['generation'] and eta['state'] == 'sampling'


def test_transient_plan_read_failure_is_retried_for_the_same_generation(tmp_path, monkeypatch):
    _root, _stage, _journal = _retirement_cursor_fixture(tmp_path)
    original = ops._fixed_retirement_plan_count
    calls = []
    def sometimes_unavailable(*args):
        calls.append(True)
        return None if len(calls) == 1 else original(*args)
    monkeypatch.setattr(ops, '_fixed_retirement_plan_count', sometimes_unavailable)
    ops._cached_retirement_plan_count.cache_clear()
    first, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    second, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert first['completed_items'] is None and second['completed_items'] == 1
    assert len(calls) == 2


def test_all_retirement_names_do_not_replace_the_completion_receipt(tmp_path):
    _retirement_cursor_fixture(tmp_path, count=3)
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    eta, _ = ops.estimate(row, {}, now=NOW)
    assert row['state'] == 'running' and not row['complete_evidence']
    assert eta['state'] == 'finalizing' and eta['reason'] == 'finalizing'


def test_retirement_reservation_is_visible_while_waiting_and_after_owner_interrupt(tmp_path, monkeypatch):
    path = tmp_path/'migration/source-local-exact-repack/retirement-turn-observation.json'
    value = {'contract': 'stockagent.source_local_retirement_turn_observation.v1',
             'state': 'waiting_writer', 'observed_at_epoch': NOW, 'completion_proved': False,
             'owner_identity': {'pid': 123, 'start_ticks': 456,
                                'boot_id': '12345678-1234-1234-1234-123456789abc'}}
    save(path, value)
    monkeypatch.setattr(ops, '_process_identity_matches', lambda _identity: True)
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['reason'] == 'retirement_turn_waiting_writer'
    assert not row['complete_evidence']
    public = view(row)['tasks'][0]
    assert '不是回收完成時間' in public['reason'] and '12345678' not in json.dumps(public)
    monkeypatch.setattr(ops, '_process_identity_matches', lambda _identity: False)
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'retirement_turn_interrupted'
    value['state'] = 'released'
    save(path, value)
    assert ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True) == []


@pytest.mark.parametrize(('reason', 'state'), [
    ('d_e_owner_busy', 'waiting'),
    ('retirement_source_replication_busy', 'waiting'),
    ('retirement_backup_cycle_busy', 'waiting'),
    ('retirement_process_references', 'waiting'),
    ('retirement_pinned', 'blocked'),
    ('retirement_current_head', 'blocked'),
    ('retirement_metadata_retry', 'blocked'),
])
def test_retirement_waits_for_owner_but_keeps_real_blockers_visible(tmp_path, reason, state):
    queue = tmp_path/'migration/source-local-exact-repack/retirement-queue-status.json'
    save(queue, {'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
        'observed_at_epoch': NOW, 'entries': [{'dataset': 'okx', 'old_manifest_sha256': 'a'*64,
            'mode': 'direct', 'state': 'waiting_retry', 'reason': reason, 'retry_after_epoch': NOW+60}]})
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == state and row['reason'] == reason and not row['complete_evidence']
    public = view(row)['tasks'][0]
    assert public['state'] == state and public['reason_code'] == reason
    assert public['next_attempt_at_utc'] is not None
    assert (public['metrics'].get('head_transition_recheck') == 1) == (reason == 'retirement_current_head')


def test_private_head_retirement_does_not_claim_public_head_recheck(tmp_path):
    path = tmp_path/'migration/source-local-exact-repack/retirement-queue-status.json'
    save(path, {'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
        'observed_at_epoch': NOW, 'entries': [{'dataset': 'okx', 'old_manifest_sha256': 'a'*64,
            'mode': 'direct', 'private': True, 'state': 'waiting_retry',
            'reason': 'retirement_current_head', 'retry_after_epoch': NOW+60}]})
    row, = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert 'head_transition_recheck' not in view(row)['tasks'][0]['metrics']


def test_repack_retirement_cards_bind_each_fixed_source_generation(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    name, current, older, newer = 'forex-frankfurter', 'a'*64, 'b'*64, 'c'*64
    save(root/'retirement-queue-status.json', {
        'contract':'stockagent.source_local_repack_queue.v1.retirement-status', 'observed_at_epoch':NOW,
        'entries':[{'dataset':name, 'old_manifest_sha256':current, 'mode':'history','state':'waiting'}]})
    save(root/name/(newer+'.json'), {'contract':'stockagent.source_local_exact_repack.v1',
        'dataset':name, 'old_manifest_sha256':older, 'new_manifest_sha256':newer,
        'old_release_removed':True,
        'retirement':{'state':'completed','apply':True,'old_manifest_sha256':older,
                      'object_count':2,'object_bytes':100,'observed_at_epoch':NOW-10},
        'history_retirement':{'versions_removed':1,'object_bytes_removed':50,
                              'estimated_allocated_bytes_reclaimed':50,'observed_at_epoch':NOW-5,
                              'original_anchor_missing_without_successor':True}})
    rows = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert len(rows) == 3 and len({row['id'] for row in rows}) == 3
    histories = [row for row in rows if row['phase'] == 'source_repack_history_working']
    assert sorted(row['state'] for row in histories) == ['completed','waiting']
    completed, = [row for row in histories if row['state'] == 'completed']
    assert completed['metrics']['history_anchor_missing'] == 1
    assert view(completed)['tasks'][0]['metrics']['history_anchor_missing'] == 1


def test_missing_history_anchor_note_survives_a_new_waiting_sweep(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    name, old, new = 'forex-frankfurter', 'a'*64, 'b'*64
    save(root/'retirement-queue-status.json', {
        'contract':'stockagent.source_local_repack_queue.v1.retirement-status', 'observed_at_epoch':NOW,
        'entries':[{'dataset':name, 'old_manifest_sha256':old, 'mode':'history','state':'waiting'}]})
    save(root/name/(new+'.json'), {'contract':'stockagent.source_local_exact_repack.v1',
        'dataset':name, 'old_manifest_sha256':old, 'new_manifest_sha256':new, 'old_release_removed':True,
        'retirement':{'state':'completed','apply':True,'old_manifest_sha256':old,
                      'object_count':1,'object_bytes':100,'observed_at_epoch':NOW-10},
        'history_retirement':{'versions_removed':1,'object_bytes_removed':50,
            'estimated_allocated_bytes_reclaimed':50,'observed_at_epoch':NOW-5,
            'original_anchor_missing_without_successor':True}})
    rows = ops.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    history, = [row for row in rows if row['phase'] == 'source_repack_history_working']
    assert history['state'] == 'waiting' and history['metrics']['history_anchor_missing'] == 1
    assert view(history)['tasks'][0]['metrics']['history_anchor_missing'] == 1


def test_staged_object_attribution_never_claims_reconstruction_or_deletion(tmp_path):
    root = tmp_path/'migration/d-cold-object-staging-v1'
    root.mkdir(parents=True)
    value = {'contract': 'stockagent.staged_cold_orphan_attribution.v1', 'state': 'metadata_attributed',
        'payloads_read': False, 'complete_dataset_reconstructed': False, 'deletion_authorized': False,
        'object_count': 525, 'attributed_objects': 53, 'object_bytes': 1000, 'attributed_bytes': 200,
        'last_checked_at_epoch': time.time(), 'inventory_errors': [], 'deferred_batches': []}
    (root/'orphan-attribution-v1.json').write_text(json.dumps(value))
    row, = ops.staged_cold_attribution_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and not row['complete_evidence']
    assert row['completed_items'] == 53 and row['total_items'] == 525
    assert row['metrics']['unattributed_objects'] == 472
    value['deferred_batches'] = [{'batch': 'still-pending'}]
    (root/'orphan-attribution-v1.json').write_text(json.dumps(value))
    row, = ops.staged_cold_attribution_tasks(tmp_path, workspace_checked=True)
    assert row['reason'] == 'staged_cold_partial'
    assert row['completed_items'] == 53 and row['completed_bytes'] == 200
    assert row['total_items'] is None and row['total_bytes'] is None
    assert row['metrics']['staged_known_subset_objects'] == 525
    assert row['metrics']['staged_deferred_batches'] == 1
    value['deletion_authorized'] = True
    (root/'orphan-attribution-v1.json').write_text(json.dumps(value))
    assert ops.staged_cold_attribution_tasks(tmp_path, workspace_checked=True) == []


def test_historical_index_candidates_remain_separate_from_attribution_and_nas_acceptance(tmp_path):
    from scripts.attribute_staged_recovery_index import CONTRACT as candidate_contract, _result_identity
    root = tmp_path/'migration/d-cold-object-staging-v1'
    root.mkdir(parents=True)
    primary = {'contract': 'stockagent.staged_cold_orphan_attribution.v1', 'state': 'metadata_attributed',
        'payloads_read': False, 'complete_dataset_reconstructed': False, 'deletion_authorized': False,
        'input_identity_sha256': 'a'*64, 'object_count': 525, 'attributed_objects': 249,
        'object_bytes': 1000, 'attributed_bytes': 200, 'last_checked_at_epoch': NOW,
        'inventory_errors': [], 'deferred_batches': []}
    save(root/'orphan-attribution-v1.json', primary)
    candidate = {'contract': candidate_contract, 'state': 'metadata_index_matched',
        'primary_input_identity_sha256': primary['input_identity_sha256'], 'objects_scanned': 525,
        'deferred_batches': 0, 'payloads_read': False, 'current_NAS_readback': False,
        'complete_dataset_reconstructed': False, 'deletion_authorized': False,
        'historical_index_matched_objects': 83, 'historical_index_matched_bytes': 300,
        'not_locally_referenced_objects': 38, 'not_locally_referenced_bytes': 70, 'observed_at_epoch': NOW+0.25}
    candidate['identity_sha256'] = _result_identity(candidate)
    save(root/'orphan-recovery-index-candidates-v1.json', candidate)
    status = {'contract': candidate_contract+'.status', 'state': 'available',
              'candidate_identity_sha256': candidate['identity_sha256'], 'observed_at_epoch': NOW}
    save(root/'orphan-recovery-index-status-v1.json', status)
    row, = ops.staged_cold_attribution_tasks(tmp_path, workspace_checked=True)
    assert row['metrics']['staged_index_matched_objects'] == 83
    assert row['metrics']['staged_index_new_objects'] == 38
    assert row['metrics']['unattributed_objects'] == 276 and row['completed_items'] == 249
    assert row['metrics']['staged_index_unavailable'] == 0 and not row['complete_evidence']
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': [row]}, now=NOW)
    assert result['tasks'][0]['metrics']['staged_index_new_objects'] == 38
    assert result['tasks'][0]['metrics']['staged_index_observed_epoch'] == int(NOW)
    status['state'] = 'unavailable'
    save(root/'orphan-recovery-index-status-v1.json', status)
    row, = ops.staged_cold_attribution_tasks(tmp_path, workspace_checked=True)
    assert row['metrics']['staged_index_unavailable'] == 1
    candidate['not_locally_referenced_objects'] = 400  # Integrity and bounds both fail.
    save(root/'orphan-recovery-index-candidates-v1.json', candidate)
    row, = ops.staged_cold_attribution_tasks(tmp_path, workspace_checked=True)
    assert 'staged_index_new_objects' not in row['metrics']


def test_reclaimed_summary_counts_WSL_retirements_without_D_or_vhdx_double_count():
    rows = [ops.task('handoff_source_repack', 'a', state='completed', phase='handoff_source_retired',
                     observed=NOW, proof=True, metrics={'reclaimed_allocated_bytes': 100}),
            ops.task('handoff_index_archive', 'b', state='completed', phase='handoff_source_retired',
                     observed=NOW, proof=True, metrics={'reclaimed_allocated_bytes': 200}),
            ops.task('workspace_c_compaction', 'c', state='completed', phase='windows_storage_verified',
                     observed=NOW, proof=True, metrics={'reclaimed_allocated_bytes': 1000}),
            ops.task('handoff_private_retirement', 'd', state='completed', phase='handoff_private_old_done',
                     observed=NOW, proof=True, metrics={'estimated_reclaimed_allocated_bytes': 500})]
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': rows}, now=NOW)
    assert result['summary']['penguin_reclaimed_allocated_bytes'] == 300
    assert result['summary']['reclaimed_scope'] == 'completed_WSL_retirements_excluding_physical_vhdx_compaction'


def test_bulk_carrier_reclaimed_bytes_remain_separate_from_WSL_total(tmp_path):
    save(tmp_path/'status.json', {'state': 'deferred', 'observed_at_epoch': NOW,
        'original_carriers_reclaimed': 1, 'original_carrier_reclaimed_allocated_bytes': 100,
        'landing_parts_reclaimed': 3, 'landing_parts_reclaimed_allocated_bytes': 200,
        'original_carrier_summary_incomplete': False})
    row, = ops.bulk_return_tasks(tmp_path)
    assert row['metrics']['D_bulk_carrier_reclaimed_allocated_bytes'] == 100
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': [row]}, now=NOW)
    assert result['tasks'][0]['metrics']['D_bulk_carriers_reclaimed'] == 1
    assert result['tasks'][0]['metrics']['D_bulk_landing_parts_reclaimed'] == 3
    assert result['tasks'][0]['metrics']['D_bulk_landing_parts_reclaimed_allocated_bytes'] == 200
    assert result['summary']['penguin_reclaimed_allocated_bytes'] == 0


@pytest.mark.parametrize(('status', 'state', 'reason'), [
    ('healthy', 'waiting', 'd_mount_healthy'),
    ('repaired', 'waiting', 'd_mount_healthy'),
    ('waiting_D_mount_users', 'waiting', 'd_mount_busy'),
    ('waiting_trading_priority_window', 'waiting', 'd_mount_trading'),
    ('deferred', 'blocked', 'd_mount_deferred'),
])
def test_d_mount_periodic_status_is_observed_without_claiming_data_acceptance(tmp_path, status, state, reason):
    path = tmp_path/'mount.json'
    save(path, {'contract': 'stockagent.d_root_mount_repair.v1', 'observed_at_epoch': NOW, 'state': status})
    row, = ops.d_mount_repair_tasks(path)
    assert row['state'] == state and row['reason'] == reason and not row['complete_evidence']
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': [row]}, now=NOW)
    assert result['tasks'][0]['kind'] == 'd_mount_repair'
    if status in ('healthy', 'repaired'):
        assert result['tasks'][0]['eta']['reason'] == 'd_mount_recurring'
        assert result['tasks'][0]['eta']['completion_at_utc'] is None
        assert result['overall_eta']['unestimated_tasks'] == 0


def _windows_maintenance_fixture(tmp_path):
    sha = 'a' * 64
    preparation = tmp_path / 'prepared.json'
    save(preparation, {'contract': 'stockagent.windows_c_compaction_e_3tib_preparation.v1',
        'state': 'native_semantic_tests_passed_apply_requires_administrator',
        'data_target_bytes': 3298534883328, 'data_uuid': 'enrolled-data-uuid',
        'native_sources': {'worker': {'sha256': sha,
            'native_path': 'C:\\Users\\private-user\\AppData\\Local\\StockAgent\\WslStorage\\worker-' + sha + '.ps1'}},
        'native_verification': {'worker_sha256': sha, 'disks_modified': False,
                                'observed_at_utc': ops.iso(NOW)}})
    value = {'contract': 'stockagent-wsl-storage-maintenance-v1', 'mode': 'Compact',
        'distro': 'Ubuntu-26.04', 'script_sha256': sha.upper(),
        'observed_at_utc': ops.iso(NOW + 60), 'state': 'compacted-and-boot-verified',
        'errors': [], 'recovery': 'original boot audit passed',
        'before': {'allocated_bytes': 100, 'virtual_bytes': 300},
        'after': {'allocated_bytes': 80, 'virtual_bytes': 300},
        'data_expansion': {'distribution': 'StockAgentData', 'target_bytes': 3298534883328,
            'expected_filesystem_uuid': 'enrolled-data-uuid',
            'state': 'expanded_and_guest_filesystem_verified', 'readback': 'filesystem verified',
            'after': {'virtual_bytes': 3298534883328}}}
    return preparation, tmp_path / 'receipts', value


def test_minute_cache_rebind_status_is_scoped_and_never_claims_source_completion(tmp_path):
    path = tmp_path/'artifacts/operations/source_validation/tw-minute-source-cold/device-rebind-status.json'
    value = {'contract': 'stockagent.minute_validation_device_rebind.v1', 'generation': 'a'*64,
             'state': 'time_budget', 'cumulative_rebound': 7866, 'matched': 8192, 'raced': 326,
             'observed_at_epoch': NOW, 'payloads_read': False, 'source_files_deleted': 0}
    save(path, value)
    row, = ops.minute_validation_cache_tasks(tmp_path)
    assert row['state'] == 'running' and not row['complete_evidence']
    assert row['completed_items'] == 7866 and row['total_items'] is None
    public = view(row)['tasks'][0]
    assert public['metrics']['validation_cache_rebound'] == 7866
    assert '不是分鐘資料完成' in public['scope']
    value.update(state='waiting', reason='tw_day_trade_priority_window')
    save(path, value)
    row, = ops.minute_validation_cache_tasks(tmp_path)
    assert row['state'] == 'waiting' and row['reason'] == 'tw_day_trade_priority_window'
    value['state'] = 'completed'
    save(path, value)
    row, = ops.minute_validation_cache_tasks(tmp_path)
    assert row['state'] == 'completed' and row['complete_evidence']
    value['payloads_read'] = True
    save(path, value)
    assert ops.minute_validation_cache_tasks(tmp_path) == []


def test_windows_maintenance_external_prerequisite_does_not_expire_or_renew_clock(tmp_path, monkeypatch):
    preparation, receipts, _ = _windows_maintenance_fixture(tmp_path)
    monkeypatch.setattr(ops, 'children', lambda *a, **k: pytest.fail('must not scan hourly receipts'))
    monkeypatch.setattr(ops.subprocess, 'run', lambda *a, **k: pytest.fail('observer must not run Windows'))
    rows = ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW + 86400)
    assert len(rows) == 2
    for row in rows:
        assert row['state'] == 'blocked' and not row['complete_evidence']
        assert ops.timestamp(row['observed_at_utc']) == NOW
        eta, _ = ops.estimate(row, {}, now=NOW + 86400)
        assert eta['state'] == 'blocked' and eta['reason'] == 'windows_storage_administrator'
        assert eta['completion_at_utc'] is None
    public = json.dumps(view(rows[0], now=NOW+86400), ensure_ascii=False)
    assert 'private-user' not in public and 'worker-' not in public


def test_windows_maintenance_completion_needs_original_owner_and_guest_readback(tmp_path):
    preparation, receipts, value = _windows_maintenance_fixture(tmp_path)
    save(receipts / 'latest-c-e-maintenance.json', value)
    e, c = ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120)
    assert e['complete_evidence'] and c['complete_evidence']
    assert c['metrics']['reclaimed_allocated_bytes'] == 20
    assert ops.timestamp(e['observed_at_utc']) == NOW+60
    value['data_expansion']['readback'] = None
    save(receipts / 'latest-c-e-maintenance.json', value)
    e, _ = ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120)
    assert not e['complete_evidence']
    value['state'] = 'needs-attention'
    value['errors'] = ['private traceback and path']
    save(receipts / 'latest-c-e-maintenance.json', value)
    e, c = ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120)
    assert all(row['state'] == 'blocked' and row['reason'] == 'windows_storage_failed' for row in (e,c))
    assert 'traceback' not in json.dumps(view(e), ensure_ascii=False)
    # E capacity is a separate proof from unrelated service/source readiness.
    value['data_expansion'].update(readback='exact filesystem size and UUID verified',
                                   observed_at_utc=ops.iso(NOW+30))
    save(receipts / 'latest-c-e-maintenance.json', value)
    e, c = ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120)
    assert e['complete_evidence'] and not c['complete_evidence']
    assert ops.timestamp(e['observed_at_utc']) == NOW+30
    value['data_expansion']['observed_at_utc'] = ops.iso(NOW+10000)
    save(receipts / 'latest-c-e-maintenance.json', value)
    assert not ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120)[0]['complete_evidence']


def test_live_e_capacity_is_separate_from_older_failed_maintenance(tmp_path):
    preparation, receipts, value = _windows_maintenance_fixture(tmp_path)
    value.update(state='needs-attention', errors=['service boot audit failed'], recovery=None)
    value['data_expansion']['readback'] = None
    save(receipts / 'latest-c-e-maintenance.json', value)
    guest = {'filesystem_uuid': 'enrolled-data-uuid', 'virtual_bytes': 3298534883328,
             'filesystem_bytes': 3245616308224, 'observed_at_epoch': NOW + 120}
    e, c = ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120,
                                     guest_e_observation=guest)
    assert e['state'] == 'completed' and e['complete_evidence']
    assert e['phase'] == 'windows_storage_capacity_observed'
    assert e['reason'] is None
    assert e['metrics']['guest_e_capacity_bytes'] == 3298534883328
    assert e['metrics']['guest_e_filesystem_capacity_bytes'] == 3245616308224
    assert ops.timestamp(e['observed_at_utc']) == NOW + 120  # Current capacity, not the old receipt.
    assert ops.timestamp(e['activity_at_utc']) == NOW + 120
    assert c['reason'] == 'windows_storage_failed' and not c['complete_evidence']
    public = view(e, now=NOW + 120)['tasks'][0]
    assert public['metrics']['guest_e_capacity_bytes'] == 3298534883328
    assert public['metrics']['guest_e_filesystem_capacity_bytes'] == 3245616308224
    assert '不代表先前 Windows 維護' in public['scope']


def test_historical_c_maintenance_failure_does_not_block_current_migration_summary(tmp_path):
    preparation, receipts, value = _windows_maintenance_fixture(tmp_path)
    value.update(state='needs-attention', errors=['old offline maintenance failed'], recovery=None)
    save(receipts/'latest-c-e-maintenance.json', value)
    _, historical = ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120)
    actual_block = ops.task('workspace_source_handoff', 'current', state='blocked',
                            phase='source_handoff_root_changed', observed=NOW+120,
                            reason='source_handoff_root_changed')
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW+120),
                                'tasks': [historical, actual_block]}, now=NOW+120)
    old, current = result['tasks']
    assert old['historical_only'] and old['state'] == 'blocked' and not old['complete_evidence']
    assert old['reason_code'] == 'windows_storage_failed' and old['observed_at_utc'] == ops.iso(NOW+60)
    assert not current['historical_only'] and current['state'] == 'blocked'
    assert result['summary']['historical_unresolved'] == 1 and result['summary']['blocked'] == 1
    assert result['overall_eta']['blocked_tasks'] == 1 and result['overall_eta']['unestimated_tasks'] == 1
    assert not any(item['id'] == old['id'] for item in result['attention'])
    assert any(item['id'] == current['id'] for item in result['attention'])
    # A pending or currently running operation is not hidden as history.
    pending = ops.task('workspace_c_compaction', 'pending', state='blocked',
                       phase='windows_storage_pending', observed=NOW+120,
                       reason='windows_storage_administrator')
    result = view(pending, now=NOW+120)
    assert not result['tasks'][0]['historical_only'] and result['summary']['blocked'] == 1


@pytest.mark.parametrize('change', ['uuid', 'capacity', 'filesystem', 'missing_filesystem', 'stale', 'future'])
def test_live_e_capacity_rejects_unpaired_or_stale_observation(tmp_path, change):
    preparation, receipts, value = _windows_maintenance_fixture(tmp_path)
    value.update(state='needs-attention', errors=['old maintenance failed'], recovery=None)
    value['data_expansion']['readback'] = None
    save(receipts / 'latest-c-e-maintenance.json', value)
    guest = {'filesystem_uuid': 'enrolled-data-uuid', 'virtual_bytes': 3298534883328,
             'filesystem_bytes': 3245616308224, 'observed_at_epoch': NOW + 120}
    if change == 'uuid': guest['filesystem_uuid'] = 'another-device'
    elif change == 'capacity': guest['virtual_bytes'] = 1024**4
    elif change == 'filesystem': guest['filesystem_bytes'] = 1024**4
    elif change == 'missing_filesystem': del guest['filesystem_bytes']
    elif change == 'stale': guest['observed_at_epoch'] = NOW - 1000
    else: guest['observed_at_epoch'] = NOW + 121
    e, _ = ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120,
                                     guest_e_observation=guest)
    assert e['phase'] == 'windows_storage_failed'
    assert 'guest_e_capacity_bytes' not in e['metrics']
    assert not e['complete_evidence']


def test_offline_compaction_has_its_own_proof_even_if_later_boot_audit_fails(tmp_path):
    preparation, receipts, value = _windows_maintenance_fixture(tmp_path)
    value.update(state='needs-attention', errors=['boot audit unavailable'], recovery=None,
                 compaction_readback={'allocated_bytes': 70, 'virtual_bytes': 300},
                 after={'allocated_bytes': 90, 'virtual_bytes': 300},
                 events=[{'name': 'offline-compaction-verified-without-resize', 'at_utc': ops.iso(NOW+30)}])
    save(receipts / 'latest-c-e-maintenance.json', value)
    e, c = ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120)
    assert c['complete_evidence'] and c['metrics']['reclaimed_allocated_bytes'] == 30
    assert ops.timestamp(c['observed_at_utc']) == NOW+30
    assert e['complete_evidence']  # independently verified guest readback


@pytest.mark.parametrize('failure', ['missing_event', 'future_event', 'virtual_size_changed'])
def test_independent_compaction_rejects_unmatched_evidence(tmp_path, failure):
    preparation, receipts, value = _windows_maintenance_fixture(tmp_path)
    value.update(state='needs-attention', errors=['boot failed'], recovery=None,
                 compaction_readback={'allocated_bytes': 70, 'virtual_bytes': 300},
                 events=[{'name': 'offline-compaction-verified-without-resize', 'at_utc': ops.iso(NOW+30)}])
    if failure == 'missing_event':
        value['events'] = []
    elif failure == 'future_event':
        value['events'][0]['at_utc'] = ops.iso(NOW+10000)
    else:
        value['compaction_readback']['virtual_bytes'] = 301
    save(receipts / 'latest-c-e-maintenance.json', value)
    _, c = ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120)
    assert not c['complete_evidence']


@pytest.mark.parametrize('change', ['code', 'uuid', 'target', 'future', 'old'])
def test_windows_maintenance_rejects_unpaired_or_out_of_window_native_receipt(tmp_path, change):
    preparation, receipts, value = _windows_maintenance_fixture(tmp_path)
    if change == 'code': value['script_sha256'] = 'b' * 64
    elif change == 'uuid': value['data_expansion']['expected_filesystem_uuid'] = 'different'
    elif change == 'target': value['data_expansion']['target_bytes'] = 1024**4
    elif change == 'future': value['observed_at_utc'] = ops.iso(NOW + 10000)
    else: value['observed_at_utc'] = ops.iso(NOW - 1)
    save(receipts / 'latest-c-e-maintenance.json', value)
    assert all(not row['complete_evidence'] and row['reason'] == 'windows_storage_administrator'
               for row in ops.windows_storage_tasks(preparation, receipt_root=receipts, now=NOW+120))


def test_transport_cutover_requires_proof_of_old_copy_retirement_and_consumers(tmp_path):
    save(tmp_path / 'intent.json', {'logical_bytes': 1000, 'files': 2,
                                    'started_at_epoch': NOW - 10})
    save(tmp_path / 'progress.json', {'state': 'completed', 'observed_at_epoch': NOW})
    handoff = {'state': 'completed', 'original_sender_path_preserved': True,
               'copy': {'all_source_sha256_and_destination_readback_verified': True},
               'source_observations_deleted': 0, 'cold_objects_deleted': 0,
               'nas_objects_deleted': 0, 'observed_at_epoch': NOW}
    save(tmp_path / 'handoff.json', handoff)
    row, = ops.workspace_transport_tasks(tmp_path)
    public = view(row)['tasks'][0]
    assert row['state'] == 'blocked' and not row['complete_evidence']
    assert public['phase'] == 'workspace_retirement'
    assert public['reason_code'] == 'transport_retirement_unverified'
    assert public['eta']['completion_at_utc'] is None
    save(tmp_path / 'reconciliation.json', {'observed_at_epoch': NOW + 20,
        'reason_code': 'transport_stale_mount_namespace'})
    row, = ops.workspace_transport_tasks(tmp_path)
    assert row['reason'] == 'transport_stale_mount_namespace'
    assert ops.timestamp(row['observed_at_utc']) == NOW + 20
    handoff['old_D_copy_retirement_verified'] = True
    save(tmp_path / 'handoff.json', handoff)
    assert not ops.workspace_transport_tasks(tmp_path)[0]['complete_evidence']
    handoff.update(old_D_source_absent_confirmed=True, consumer_aliases_verified=True)
    save(tmp_path / 'handoff.json', handoff)
    row, = ops.workspace_transport_tasks(tmp_path)
    assert row['state'] == 'completed' and row['complete_evidence']


def test_transport_fixed_files_retired_reports_retained_empty_root_without_claiming_nas(tmp_path):
    save(tmp_path/'intent.json', {'logical_bytes': 1000, 'files': 2, 'started_at_epoch': NOW-10})
    save(tmp_path/'progress.json', {'state': 'completed', 'observed_at_epoch': NOW})
    save(tmp_path/'handoff.json', {'state': 'completed', 'original_sender_path_preserved': True,
        'copy': {'all_source_sha256_and_destination_readback_verified': True},
        'source_observations_deleted': 0, 'cold_objects_deleted': 0, 'nas_objects_deleted': 0})
    save(tmp_path/'comparison-status.json', {'contract': 'stockagent.transport_relocation_comparison.v1',
        'state': 'completed', 'identity_sha256': 'a'*64})
    receipt = {'contract': 'stockagent.transport_relocation_retirement.v1', 'state': 'registered_files_retired',
        'comparison_identity_sha256': 'a'*64, 'files_completed': 2, 'logical_bytes_completed': 1000,
        'old_root_and_empty_directories_retained': True, 'source_observations_deleted': 0,
        'cold_objects_deleted': 0, 'nas_objects_deleted': 0, 'observed_at_epoch': NOW+20}
    save(tmp_path/'retirement-receipt.json', receipt)
    observation = {'syncthing_alias_verified': True, 'namespace_inspection_complete': True,
        'stale_mount_namespaces': 0, 'reachable_stale_mount_namespaces': 0, 'observed_at_epoch': NOW+10}
    save(tmp_path/'reconciliation.json', observation)
    row = ops.workspace_transport_tasks(tmp_path)[0]
    assert row['state'] == 'waiting' and row['reason'] == 'transport_empty_root_pending'
    assert not row['complete_evidence']
    observation['observed_at_epoch'] = NOW+30
    save(tmp_path/'reconciliation.json', observation)
    row = ops.workspace_transport_tasks(tmp_path)[0]
    assert row['state'] == 'completed' and row['phase'] == 'workspace_transport_files_retired'
    assert row['reason'] is None
    assert row['complete_evidence'] and row['completed_bytes'] == 1000
    assert 'NAS' in view(row)['tasks'][0]['scope']
    receipt['old_root_and_empty_directories_retained'] = False
    save(tmp_path/'retirement-receipt.json', receipt)
    assert not ops.workspace_transport_tasks(tmp_path)[0]['complete_evidence']


def test_transport_comparison_is_separate_from_retirement_and_reports_differences(tmp_path):
    save(tmp_path / 'intent.json', {'logical_bytes': 1000, 'files': 3, 'started_at_epoch': NOW - 10})
    save(tmp_path / 'progress.json', {'state': 'completed', 'observed_at_epoch': NOW})
    save(tmp_path / 'handoff.json', {'state': 'completed'})
    status = {'contract': 'stockagent.transport_relocation_comparison.v1',
              'state': 'running', 'identity_sha256': 'a' * 64, 'observed_at_epoch': NOW,
              'source_names_deleted': 0, 'retirement_authorized': False, 'NAS_acceptance_claimed': False,
              'files_total': 3, 'files_completed': 2, 'logical_bytes_total': 1000,
              'logical_bytes_completed': 500, 'same_files': 1, 'different_files': 1,
              'missing_files': 0, 'unstable_files': 0, 'unavailable_files': 0}
    save(tmp_path / 'comparison-status.json', status)
    retirement, comparison = ops.workspace_transport_tasks(tmp_path)
    assert retirement['state'] == 'blocked' and not retirement['complete_evidence']
    public = view(comparison)['tasks'][0]
    assert public['state'] == 'running' and public['progress']['percent'] == 50
    assert public['metrics']['different_files'] == 1
    assert '不授權刪除' in public['scope'] and 'NAS' in public['scope']
    status.update(state='completed', files_completed=3, logical_bytes_completed=1000)
    save(tmp_path / 'comparison-status.json', status)
    retirement, comparison = ops.workspace_transport_tasks(tmp_path)
    assert comparison['complete_evidence'] and not retirement['complete_evidence']
    delta = {'contract': 'stockagent.transport_relocation_delta.v1', 'state': 'readback',
             'comparison_identity_sha256': 'a' * 64, 'observed_at_epoch': NOW,
             'files_total': 2, 'files_completed': 0, 'logical_bytes_total': 500,
             'logical_bytes_completed': 0, 'source_names_deleted': 0,
             'retirement_authorized': False, 'NAS_acceptance_claimed': False}
    save(tmp_path / 'delta-status.json', delta)
    retirement, comparison, archive = ops.workspace_transport_tasks(tmp_path)
    assert archive['phase'] == 'workspace_delta_readback' and not archive['complete_evidence']
    save(tmp_path / 'attempt.json', {'state': 'relocation_failed', 'observed_at_epoch': NOW + 1})
    assert ops.workspace_transport_tasks(tmp_path)[2]['state'] == 'blocked'
    delta['observed_at_epoch'] = NOW + 2
    delta.update(state='verified', files_completed=2, logical_bytes_completed=500)
    save(tmp_path / 'delta-status.json', delta)
    assert not ops.workspace_transport_tasks(tmp_path)[2]['complete_evidence']
    save(tmp_path / 'delta-receipt.json', {'contract': delta['contract'], 'state': 'verified',
         'comparison_identity_sha256': 'a' * 64, 'files': 2, 'logical_bytes': 500})
    retirement, comparison, archive = ops.workspace_transport_tasks(tmp_path)
    assert archive['complete_evidence'] and not retirement['complete_evidence']
    restore = {'contract': 'stockagent.transport_relocation_retirement.v1.delta_restore',
        'state': 'member_restore_verified', 'comparison_identity_sha256': 'a'*64,
        'files': 2, 'logical_bytes': 500, 'observed_at_epoch': NOW+3,
        'member_bytes_and_portable_metadata_verified': True,
        'full_tree_restore_claimed': False, 'NAS_acceptance_claimed': False}
    save(tmp_path / 'delta-restore-receipt.json', restore)
    retirement, comparison, archive, rehearsal = ops.workspace_transport_tasks(tmp_path)
    assert rehearsal['complete_evidence'] and not retirement['complete_evidence']
    assert '整棵資料樹' in view(rehearsal, now=NOW+3)['tasks'][0]['scope']
    identity = {'contract': 'stockagent.transport_relocation_retirement.v1',
                'comparison_identity_sha256': 'a'*64, 'files_completed': 3,
                'files_total': 3, 'observed_at_epoch': NOW+4,
                'source_observations_deleted': 0, 'cold_objects_deleted': 0,
                'nas_objects_deleted': 0}
    save(tmp_path/'retirement-status.json', {**identity, 'state': 'verifying_and_retiring'})
    save(tmp_path/'reconciliation.json', {'observed_at_epoch': NOW+5,
        'reason_code': 'transport_stale_mount_namespace'})
    partial = ops.workspace_transport_tasks(tmp_path)[-1]
    assert partial['state'] == 'blocked' and partial['reason'] == 'transport_stale_mount_namespace'
    assert ops.timestamp(partial['observed_at_utc']) == NOW+4  # Preserve the true counter clock.
    save(tmp_path/'retirement-status.json', {**identity, 'state': 'registered_files_retired'})
    save(tmp_path/'retirement-receipt.json', {**identity, 'state': 'registered_files_retired',
        'old_root_and_empty_directories_retained': True})
    rows = ops.workspace_transport_tasks(tmp_path)
    assert rows[-1]['complete_evidence'] and not rows[0]['complete_evidence']


def test_transport_retirement_shows_byte_fraction_without_using_file_count_as_eta():
    row = ops.task('workspace_transport_retirement', 'fixed', state='running',
                   phase='workspace_retirement', observed=NOW, generation='a' * 64,
                   done_bytes=400, total_bytes=1000, done_items=2, total_items=3)
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert public['progress']['metric'] == 'bytes' and public['progress']['percent'] == 40
    assert public['progress']['completed_items'] == 2
    assert public['eta']['reason'] == 'heterogeneous_workloads'
    assert row['eta']['unit'] == 'bytes' and row['eta']['remaining_seconds'] is None
    assert public['eta']['completion_at_utc'] is None


def test_worker_handoff_observation_has_no_data_counter_or_acceptance_side_effect(tmp_path):
    save(tmp_path / 'progress.json', {'state': 'completed', 'observed_at_epoch': NOW - 90})
    candidate = 'a' * 64
    worker = {'contract': 'stockagent.workspace_worker_handoff.v1',
              'state': 'waiting_for_original_owner', 'observed_at_epoch': NOW,
              'candidate_code_identity_sha256': candidate, 'failures': 0,
              'waiting_owner_lock': '/private/original/owner.lock',
              'publication_progress_renewed': False, 'original_active_owner_forced': False}
    save(tmp_path / 'worker-reload.json', worker)
    publication, handoff = ops.workspace_transport_tasks(tmp_path)
    assert ops.timestamp(publication['observed_at_utc']) == NOW - 90
    assert ops.timestamp(handoff['observed_at_utc']) == NOW
    assert handoff['state'] == 'waiting' and handoff['reason'] == 'original_owner'
    assert handoff['completed_bytes'] is handoff['completed_items'] is None
    public = view(handoff)['tasks'][0]
    assert not public['complete_evidence'] and public['eta']['completion_at_utc'] is None
    assert '/private/' not in json.dumps(public) and candidate not in json.dumps(public)
    worker.update(state='accepted', loaded_code_identity_sha256=candidate,
                  workflow_chains_preserved=True, postgres_and_temporal_server_not_restarted=True)
    save(tmp_path / 'worker-reload.json', worker)
    assert not ops.workspace_transport_tasks(tmp_path)[-1]['complete_evidence']
    worker['publication_owner_boundary_verified'] = True
    save(tmp_path / 'worker-reload.json', worker)
    assert ops.workspace_transport_tasks(tmp_path)[-1]['complete_evidence']
    worker['loaded_code_identity_sha256'] = 'b' * 64
    save(tmp_path / 'worker-reload.json', worker)
    assert not ops.workspace_transport_tasks(tmp_path)[-1]['complete_evidence']
    worker.update(state='worker_reload_failed', failures=3)
    save(tmp_path / 'worker-reload.json', worker)
    failed = ops.workspace_transport_tasks(tmp_path)[-1]
    assert failed['state'] == 'blocked' and failed['reason'] == 'owner_retry_limit'


def test_sqlite_workspace_move_reports_distinct_copy_readback_and_acceptance(tmp_path):
    save(tmp_path / 'journal.json', {'snapshot_sha256': 'f' * 64, 'snapshot_bytes': 1000})
    save(tmp_path / 'stream-progress.json', {'state': 'copy', 'bytes_completed': 500,
        'bytes_total': 1000, 'observed_at_epoch': NOW})
    first, = ops.workspace_private_scratch_tasks(tmp_path)
    public = view(first)['tasks'][0]
    assert public['phase'] == 'scratch_copy' and public['progress']['percent'] == 50
    assert public['complete_evidence'] is False
    save(tmp_path / 'stream-progress.json', {'state': 'readback', 'bytes_completed': 1000,
        'bytes_total': 1000, 'observed_at_epoch': NOW + 1})
    second, = ops.workspace_private_scratch_tasks(tmp_path)
    assert second['phase'] == 'scratch_readback' and second['complete_evidence'] is False
    assert first['generation'] != second['generation']  # Copy speed cannot estimate readback.
    save(tmp_path / 'attempt.json', {'reason_code': 'scratch_error', 'observed_at_epoch': NOW + 2})
    failed, = ops.workspace_private_scratch_tasks(tmp_path)
    assert failed['state'] == 'blocked' and failed['reason'] == 'scratch_error'
    save(tmp_path / 'complete.json', {'contract': 'private_sqlite_workspace_relocation_v1',
        'state': 'completed', 'owned_scratch_files_deleted': 1, 'source_files_deleted': 0,
        'cold_objects_deleted': 0, 'nas_files_deleted': 0, 'observed_at_epoch': NOW + 3})
    done, = ops.workspace_private_scratch_tasks(tmp_path)
    assert done['state'] == 'completed' and done['complete_evidence'] is True
    assert 'NAS' in view(done, now=NOW + 3)['tasks'][0]['scope']


@pytest.mark.parametrize('batch,label', [(1, '台股公開資料'), (4, 'FinLab')])
def test_control_workspace_move_tracks_fixed_identity_and_phase(tmp_path, batch, label):
    root = tmp_path / 'control'
    identity = 'a' * 64
    candidate = {'batch_number': batch, 'state_root': str(root), 'controls_identity': identity,
                 'files': 2, 'logical_bytes': 1000}
    waiting, = ops.workspace_controls_tasks([candidate])
    assert waiting['state'] == 'waiting' and waiting['complete_evidence'] is False
    save(root / 'journal.json', {'controls_identity': identity, 'state': 'planned'})
    save(root / 'copy/progress.json', {'state': 'copying_and_independent_sha_readback',
        'files_completed': 1, 'bytes_completed': 500, 'observed_at_epoch': NOW})
    copying, = ops.workspace_controls_tasks([candidate])
    public = view(copying)['tasks'][0]
    assert public['phase'] == 'workspace_copy' and public['progress']['percent'] == 50
    assert public['label'] == f'WSL → E · {label}準備副本'
    save(root / 'stream-progress.json', {'state': 'checking_binding', 'observed_at_epoch': NOW + 1})
    binding, = ops.workspace_controls_tasks([candidate])
    assert binding['phase'] == 'scratch_binding' and binding['total_bytes'] is None
    assert binding['generation'] != copying['generation']
    save(root / 'complete.json', {'contract': 'private_controls_workspace_relocation_v1',
        'controls_identity': identity, 'state': 'completed', 'source_files_deleted': 0,
        'cold_objects_deleted': 0, 'nas_files_deleted': 0, 'observed_at_epoch': NOW + 2})
    complete, = ops.workspace_controls_tasks([candidate])
    assert complete['state'] == 'completed' and complete['complete_evidence'] is True
    assert 'NAS' in view(complete, now=NOW + 2)['tasks'][0]['scope']
    candidate['controls_identity'] = 'b' * 64
    changed, = ops.workspace_controls_tasks([candidate])
    assert changed['state'] == 'blocked' and changed['complete_evidence'] is False


def test_bulk_return_capacity_deferral_is_visible_without_claiming_acceptance(tmp_path):
    save(tmp_path / 'status.json', {'state': 'waiting_cold_physical_capacity',
        'available_bytes': 0, 'observed_at_epoch': NOW, 'source_deleted': False})
    row, = ops.bulk_return_tasks(tmp_path)
    public = view(row)['tasks'][0]
    assert public['state'] == 'blocked' and public['reason_code'] == 'physical_capacity'
    assert public['metrics']['admission_available_bytes'] == 0
    assert public['complete_evidence'] is False and 'NAS' in public['scope']


def test_live_pack_recovery_reports_actual_node_work_without_acceptance(tmp_path):
    operation = tmp_path / 'operations' / ('a' * 20)
    save(tmp_path / 'current.json', {'operation_directory': str(operation), 'updated_at_utc': ops.iso(NOW)})
    save(operation / 'progress.json', {'contract': 'all_catalog_incremental_migration_v1',
        'source_count': 1, 'runtime_owner': {'active_dataset': 'tw-index-futures'},
        'datasets': {'tw-index-futures': {'state': 'recovery', 'attempt_id': 'current',
                                         'snapshot_id': 'fixed', 'manifest_sha256': 'a' * 64}}})
    path = operation / 'recovery/tw-index-futures.progress.json'
    progress = {'attempt_id': 'current', 'stage': 'parallel-pack-reconstruction',
        'updated_at_utc': ops.iso(NOW), 'completed_items': 10, 'total_items': 100,
        'completed_bytes': 40, 'total_bytes': 1000,
        'worker_nodes': {'local': 2, 'vast': 3}, 'pending_local_packs': 1, 'pending_remote_packs': 1}
    save(path, progress)
    row = ops.migration_tasks(tmp_path)[-1]
    public = view(row)['tasks'][0]
    assert public['phase'] == 'pack_reconstruction' and public['node_id'] == 'penguin_vast'
    assert public['metrics']['penguin_packs'] == 2 and public['metrics']['vast_packs'] == 3
    assert public['metrics']['active_jobs'] == 2
    assert public['state'] == 'running' and not public['complete_evidence']
    for stage, label in [('validate-packed-inventory', '核對固定封存清冊'),
                         ('capture-retained-pack-generations', '記錄已驗證封閉包'),
                         ('revalidate-completed-pack-bytes', '重新讀取已驗證封閉包'),
                         ('verify-owned-preparation-scratch', '核對本次作業的準備暫存檔'),
                         ('unlink-owned-preparation-scratch', '回收已驗證的本次準備暫存檔')]:
        progress['stage'] = stage
        save(path, progress)
        row = ops.migration_tasks(tmp_path)[-1]
        public = view(row)['tasks'][0]
        assert public['phase'] == stage
        assert label in public['phase_label'] and not public['complete_evidence']
    progress['stage'] = 'check-owned-scratch-process-references'
    progress['completed_items'] = progress['total_items']
    save(path, progress)
    row = ops.migration_tasks(tmp_path)[-1]
    public = view(row)['tasks'][0]
    assert public['phase'] == progress['stage'] and not public['complete_evidence']
    assert public['progress']['completed_items'] is None and public['progress']['total_items'] is None
    assert public['progress']['completed_bytes'] is None and public['progress']['total_bytes'] is None
    assert public['eta']['completion_at_utc'] is None
    progress['attempt_id'] = 'old'
    save(path, progress)
    assert len(ops.migration_tasks(tmp_path)) == 1


def test_control_copy_byte_bar_keeps_slower_file_tail_eta_in_its_own_units():
    row = ops.task('workspace_controls', 4, state='running', phase='workspace_copy',
                   generation='fixed-controls', observed=NOW - 60,
                   done_bytes=2500, total_bytes=10000, done_items=20, total_items=100)
    _, history = ops.estimate(row, {}, now=NOW - 60)
    row.update(observed_at_utc=ops.iso(NOW), completed_bytes=5000, completed_items=30)
    row['eta'], _ = ops.estimate(row, history, now=NOW)
    public = view(row)['tasks'][0]
    assert public['progress']['metric'] == 'bytes' and public['progress']['percent'] == 50
    assert public['progress']['completed_items'] == 30
    assert public['eta']['unit'] == 'items' and public['eta']['remaining_seconds'] == 420
    assert public['eta']['rate_per_second'] == pytest.approx(1 / 6, abs=0.0001)


def test_disposable_cleanup_progress_is_bound_and_keeps_local_deletion_separate_from_nas(tmp_path):
    from stockagent.data_sync.testing_disposition import LOCAL_TESTING_CONTRACT, identity
    policy = dict(contract=LOCAL_TESTING_CONTRACT, authority_node_id='penguin', origin_node_id='penguin',
        authorization='user explicitly discards old performance work copies', decision='discard_without_preservation',
        targets=[dict(base='artifact', relative='benchmarks/old-case', kind='directory', reason='benchmark')])
    policy['identity_sha256'] = identity(policy)
    save(tmp_path/'configs/data_sync/local_testing_artifact_disposition_20261102.json', policy)
    status = dict(contract='stockagent.local_testing_cleanup.v1',
        policy_identity_sha256=policy['identity_sha256'], state='discarding_exact_target',
        observed_at_epoch=NOW, targets_total=1, targets_completed=0, targets_blocked=0,
        apply=True, preservation_waived_by_user=True, recovery_claimed=False,
        cold_objects_deleted=False, nas_objects_deleted=False, reclaimed_allocated_bytes=4096)
    status_path = tmp_path/'service_data/storage-discard/performance-20261102/status.json'
    save(status_path, status)
    row, = ops.local_testing_cleanup_tasks(tmp_path)
    public = view(row)['tasks'][0]
    assert public['state'] == 'running' and public['complete_evidence'] is False
    assert public['metrics']['discarded_allocated_bytes'] == 4096
    assert 'reclaimed_allocated_bytes' not in public['metrics']
    assert 'NAS' in public['scope'] and '不代表' in public['scope']
    assert ops.TASK_SCHEDULES['local_testing_cleanup'] == ('retirement',)
    estimate, history = ops.estimate(row, {}, now=NOW)
    assert estimate['state'] == 'unknown' and estimate['reason'] == 'heterogeneous_workloads'
    row.update(completed_items=1, total_items=20, observed_at_utc=ops.iso(NOW+60))
    assert ops.estimate(row, history, now=NOW+60)[0]['completion_at_utc'] is None
    status.update(state='completed', targets_completed=1)
    save(status_path, status)
    row, = ops.local_testing_cleanup_tasks(tmp_path)
    assert row['complete_evidence'] is True and view(row)['tasks'][0]['eta']['state'] == 'completed'
    status['policy_identity_sha256'] = 'wrong'
    save(status_path, status)
    row, = ops.local_testing_cleanup_tasks(tmp_path)
    assert row['state'] == 'unknown' and row['complete_evidence'] is False
    assert not row['metrics']


def test_disposable_cleanup_generations_do_not_replace_earlier_receipts(tmp_path):
    from stockagent.data_sync.testing_disposition import LOCAL_TESTING_CONTRACT, identity
    for generation in ('20261102', '20261103', '20261106'):
        policy = dict(contract=LOCAL_TESTING_CONTRACT, authority_node_id='penguin', origin_node_id='penguin',
            authorization='user approved', decision='discard_without_preservation',
            targets=[dict(base='artifact', relative=f'benchmarks/case-{generation}', kind='directory', reason='benchmark')])
        policy['identity_sha256'] = identity(policy)
        save(tmp_path/f'configs/data_sync/local_testing_artifact_disposition_{generation}.json', policy)
        save(tmp_path/f'service_data/storage-discard/performance-{generation}/status.json', {
            'contract': 'stockagent.local_testing_cleanup.v1', 'policy_identity_sha256': policy['identity_sha256'],
            'state': 'completed', 'observed_at_epoch': NOW, 'targets_total': 1, 'targets_completed': 1,
            'apply': True, 'preservation_waived_by_user': True, 'recovery_claimed': False,
            'cold_objects_deleted': False, 'nas_objects_deleted': False})
    rows = ops.local_testing_cleanup_tasks(tmp_path)
    assert len(rows) == 3 and all(row['complete_evidence'] for row in rows)
    assert len({row['id'] for row in rows}) == 3


@pytest.mark.parametrize(('generation', 'suffix'), [('20261108', 'v1'), ('20261108_v2', 'v2')])
def test_disposable_cleanup_can_observe_the_E_receipt_without_claiming_a_timer(tmp_path, generation, suffix):
    from stockagent.data_sync.testing_disposition import LOCAL_TESTING_CONTRACT, identity
    policy = dict(contract=LOCAL_TESTING_CONTRACT, authority_node_id='penguin', origin_node_id='penguin',
        authorization='user approved', decision='discard_without_preservation',
        targets=[dict(base='artifact', relative='benchmarks/old', kind='directory', reason='benchmark')])
    policy['identity_sha256'] = identity(policy)
    save(tmp_path/f'configs/data_sync/local_testing_artifact_disposition_{generation}.json', policy)
    workspace = tmp_path/'workspace'
    save(workspace/f'migration/local-testing-cleanup-20261108-{suffix}/status.json', {
        'contract': 'stockagent.local_testing_cleanup.v1', 'policy_identity_sha256': policy['identity_sha256'],
        'state': 'completed', 'observed_at_epoch': NOW, 'targets_total': 1, 'targets_completed': 1,
        'apply': True, 'preservation_waived_by_user': True, 'recovery_claimed': False,
        'cold_objects_deleted': False, 'nas_objects_deleted': False, 'reclaimed_allocated_bytes': 1234})
    row, = ops.local_testing_cleanup_tasks(tmp_path, workspace)
    public = view(row)['tasks'][0]
    assert public['complete_evidence'] and public['metrics']['discarded_allocated_bytes'] == 1234
    assert public['continuation']['automatic'] is False
    missing, = ops.local_testing_cleanup_tasks(tmp_path)
    assert not missing['complete_evidence'] and missing['state'] == 'unknown'


def test_vast_testing_cleanup_requires_exact_policy_and_complete_matching_receipt(tmp_path, monkeypatch):
    from stockagent.data_sync.testing_disposition import CONTRACT, identity
    policy = dict(contract=CONTRACT, authority_node_id='penguin', origin_node_id='vastai1T',
                  authorization='user approved', decision='discard_without_preservation',
                  targets=[dict(base='artifact', relative=f'benchmarks/trial_{i}',
                                kind='directory', reason='benchmark') for i in range(3)])
    policy['identity_sha256'] = identity(policy)
    monkeypatch.setattr(ops, 'VAST_CAPACITY_TEST_POLICY_SHA256', policy['identity_sha256'])
    root = tmp_path/'verify/vast-old-capacity-bench-20261108-v1'
    save(root/'policy.json', policy)
    results = [dict(target=t, policy_identity_sha256=policy['identity_sha256'],
                    state='discarded-without-preservation', deleted=True, recovery_claimed=False,
                    external_shared_names_deleted=False, reclaimed_allocated_bytes=i+1)
               for i, t in enumerate(policy['targets'])]
    receipt = dict(policy_identity_sha256=policy['identity_sha256'], targets=3, deleted_targets=3,
                   failed_or_protected_targets=0, cold_deletion_claimed=False,
                   reclaimed_allocated_bytes=6, observed_at_epoch=NOW, results=results)
    save(root/'apply.json', receipt)
    row, = ops.vast_testing_cleanup_tasks(tmp_path)
    public = view(row)['tasks'][0]
    assert public['complete_evidence'] and public['node_id'] == 'vastai1T'
    assert public['metrics']['discarded_allocated_bytes'] == 6
    assert 'WSL' in public['scope'] and 'NAS' in public['scope']
    assert 'benchmarks/trial_' not in json.dumps(public)
    receipt['reclaimed_allocated_bytes'] = 7
    save(root/'apply.json', receipt)
    row, = ops.vast_testing_cleanup_tasks(tmp_path)
    assert row['state'] == 'unknown' and not row['complete_evidence'] and not row['metrics']
    receipt['reclaimed_allocated_bytes'] = 6
    receipt['results'][0]['target'] = receipt['results'][1]['target']
    save(root/'apply.json', receipt)
    row, = ops.vast_testing_cleanup_tasks(tmp_path)
    assert row['state'] == 'unknown' and not row['complete_evidence'] and not row['metrics']


def test_original_rescue_manifest_monitor_distinguishes_missing_mismatch_and_unavailable_mount(tmp_path):
    from types import SimpleNamespace
    state = tmp_path/'rescue'
    spec = {'contract':'stockagent.training_rescue.v1', 'schema_version':1,
            'run_id':'tw-v6-embedding-d128', 'data_sources':[
                {'dataset':'dataset-one', 'snapshot_id':'snapshot-one', 'manifest_sha256':'a'*64},
                {'dataset':'dataset-two', 'snapshot_id':'snapshot-two', 'manifest_sha256':'b'*64}]}
    save(state/'runs/tw-v6-embedding-d128/spec.json', spec)
    save(tmp_path/'manifests/dataset-one/snapshot-one.json', {'fixture': True})
    def resolver(_cold, _dataset, snapshot, *, require_objects):
        assert require_objects is False
        if snapshot == 'snapshot-two':
            raise FileNotFoundError('not on D')
        return SimpleNamespace(manifest_sha256='a'*64)
    row, = ops.tw_rescue_original_manifest_tasks(now=NOW, state_root=state, cold=tmp_path,
                                                 mount_check=lambda _: None, resolve=resolver)
    public = view(row)['tasks'][0]
    assert public['state'] == 'blocked' and public['reason_code'] == 'rescue_original_manifest_missing'
    assert (public['progress']['completed_items'], public['progress']['total_items']) == (1, 2)
    assert not public['complete_evidence'] and 'runtime' in public['scope']
    assert 'snapshot-one' not in json.dumps(public) and 'dataset-one' not in json.dumps(public)
    save(tmp_path/'manifests/dataset-two/snapshot-two.json', {'fixture': True})
    row, = ops.tw_rescue_original_manifest_tasks(now=NOW, state_root=state, cold=tmp_path,
        mount_check=lambda _: None, resolve=lambda *_a, **_kw: SimpleNamespace(manifest_sha256='c'*64))
    assert row['reason'] == 'rescue_original_manifest_mismatch' and not row['complete_evidence']
    def unavailable(_):
        raise RuntimeError('D mount unavailable')
    row, = ops.tw_rescue_original_manifest_tasks(now=NOW, state_root=state, cold=tmp_path,
                                                 mount_check=unavailable, resolve=resolver)
    assert row['state'] == 'unknown' and row['reason'] == 'metadata_unavailable'
    assert row['total_items'] is None and not row['complete_evidence']
    checks = []
    def mount_lost_after_read(_):
        checks.append(True)
        if len(checks) == 2:
            raise RuntimeError('D mount lost after read')
    row, = ops.tw_rescue_original_manifest_tasks(now=NOW, state_root=state, cold=tmp_path,
                                                 mount_check=mount_lost_after_read, resolve=resolver)
    assert row['state'] == 'unknown' and row['reason'] == 'metadata_unavailable'
    big = tmp_path/'manifests/dataset-one/snapshot-one.json'
    with big.open('wb') as stream:
        stream.truncate(ops.MAX_METADATA_BYTES+1)
    row, = ops.tw_rescue_original_manifest_tasks(now=NOW, state_root=state, cold=tmp_path,
                                                 mount_check=lambda _: None, resolve=resolver)
    assert row['state'] == 'unknown' and row['reason'] == 'metadata_unavailable'


def test_training_rescue_displays_capacity_block_and_separate_fixed_references(tmp_path):
    from stockagent.data_sync import rescue_source_references as refs
    cold, retention, state = tmp_path/'cold', tmp_path/'retention', tmp_path/'rescue'
    (cold/'.local-state/locks').mkdir(parents=True)
    (cold/'.local-state/locks/publish-retention-global.lock').touch()
    retention.mkdir()
    binding = {'dataset':'dataset-one', 'snapshot_id':'snapshot-one', 'manifest_sha256':'a'*64}
    refs.protect(cold, retention, [binding], guard=lambda _:None)
    save(state/'rescue-source-reference-status.json', {
        'contract':'stockagent.rescue_source_reconciliation.v1', 'state':'completed',
        'observed_at_epoch':NOW, 'protected_unique_bindings':1, 'source_objects_checked':False,
        'fixed_bindings':[binding],
        'recovery_or_nas_acceptance_claimed':False})
    save(state/'rescue-source-reference-completed.json', {
        'contract':'stockagent.rescue_source_reconciliation.v1', 'state':'completed',
        'observed_at_epoch':NOW, 'protected_unique_bindings':1, 'source_objects_checked':False,
        'fixed_bindings':[binding], 'recovery_or_nas_acceptance_claimed':False})
    save(state/'rescue-cycle-status.json', {
        'contract':'stockagent.rescue_capture_cycle.v1', 'state':'waiting_d_capacity',
        'observed_at_epoch':NOW, 'D_available_bytes':500, 'D_reserve_bytes':640,
        'source_retirement':False, 'recovery_or_nas_acceptance_claimed':False})
    capture, reference = ops.training_rescue_operational_tasks(state, retention)
    assert capture['state'] == 'blocked' and capture['reason'] == 'rescue_capture_D_capacity'
    assert capture['metrics']['D_observed_free_bytes'] == 500
    assert reference['state'] == 'completed' and reference['complete_evidence']
    assert '可能仍然缺失' in view(reference)['tasks'][0]['scope']
    save(state/'rescue-source-reference-status.json', {
        'contract':'stockagent.rescue_source_reconciliation.v1', 'state':'waiting',
        'observed_at_epoch':NOW+1, 'source_objects_checked':False,
        'recovery_or_nas_acceptance_claimed':False})
    _, reference = ops.training_rescue_operational_tasks(state, retention)
    assert reference['state'] == 'waiting' and not reference['complete_evidence']
    assert reference['metrics']['rescue_protected_bindings'] == 1
    assert reference['metrics']['rescue_last_reconciled_epoch'] == NOW
    (retention/refs.REGISTRY_NAME).unlink()
    _, reference = ops.training_rescue_operational_tasks(state, retention)
    assert reference['state'] == 'unknown' and not reference['complete_evidence']


def test_registered_rescue_manifest_audit_distinguishes_missing_from_mount_unavailable(tmp_path):
    from types import SimpleNamespace
    from stockagent.data_sync import rescue_source_references as refs
    cold, retention = tmp_path/'cold', tmp_path/'retention'
    (cold/'.local-state/locks').mkdir(parents=True)
    (cold/'.local-state/locks/publish-retention-global.lock').touch()
    retention.mkdir()
    sources = [{'dataset':'dataset-one', 'snapshot_id':'snapshot-one', 'manifest_sha256':'a'*64},
               {'dataset':'dataset-two', 'snapshot_id':'snapshot-two', 'manifest_sha256':'b'*64}]
    refs.protect(cold, retention, sources, guard=lambda _:None)
    save(cold/'manifests/dataset-one/snapshot-one.json', {'fixture':True})
    row, = ops.training_rescue_registered_manifest_tasks(now=NOW, retention_state=retention, cold=cold,
        mount_check=lambda _:None, resolve=lambda *_a, **_k:SimpleNamespace(manifest_sha256='a'*64))
    public = view(row)['tasks'][0]
    assert public['state'] == 'blocked' and public['reason_code'] == 'rescue_registered_manifest_missing'
    assert (public['progress']['completed_items'], public['progress']['total_items']) == (1, 2)
    assert not public['complete_evidence'] and 'E／NAS' in public['scope']
    def unavailable(_):
        raise RuntimeError('mount unavailable')
    row, = ops.training_rescue_registered_manifest_tasks(now=NOW, retention_state=retention,
        cold=cold, mount_check=unavailable)
    assert row['state'] == 'unknown' and row['reason'] == 'metadata_unavailable'


def test_round21_disposable_cleanup_uses_its_own_bound_evidence(tmp_path):
    from stockagent.data_sync.testing_disposition import LOCAL_TESTING_CONTRACT, identity
    policy = dict(contract=LOCAL_TESTING_CONTRACT, authority_node_id='penguin', origin_node_id='penguin',
        authorization='user approved', decision='discard_without_preservation',
        targets=[dict(base='artifact', relative='operations/test/trial.json', kind='file', reason='benchmark')])
    policy['identity_sha256'] = identity(policy)
    save(tmp_path/'configs/data_sync/testing_discard_opening_r21_20261108.json', policy)
    workspace = tmp_path/'workspace'
    save(workspace/'verify/testing-discard-opening-r21-20261108-v1/status.json', {
        'contract': 'stockagent.local_testing_cleanup.v1', 'policy_identity_sha256': policy['identity_sha256'],
        'state': 'completed', 'observed_at_epoch': NOW, 'targets_total': 1, 'targets_completed': 1,
        'apply': True, 'preservation_waived_by_user': True, 'recovery_claimed': False,
        'cold_objects_deleted': False, 'nas_objects_deleted': False, 'reclaimed_allocated_bytes': 1234})
    row, = ops.local_testing_cleanup_tasks(tmp_path, workspace)
    public = view(row)['tasks'][0]
    assert public['complete_evidence'] and public['metrics']['discarded_allocated_bytes'] == 1234
    assert public['continuation']['automatic'] is False


def test_opening_round_cleanup_uses_its_actual_timer(tmp_path, monkeypatch):
    from stockagent.data_sync.testing_disposition import LOCAL_TESTING_CONTRACT, identity
    policy = dict(contract=LOCAL_TESTING_CONTRACT, authority_node_id='penguin', origin_node_id='penguin',
        authorization='user approved', decision='discard_without_preservation',
        targets=[dict(base='artifact', relative='operations/test/trial.json', kind='file', reason='benchmark')])
    policy['identity_sha256'] = identity(policy)
    monkeypatch.setattr(ops, 'OPENING_ROUNDS_POLICY_SHA256', policy['identity_sha256'])
    save(tmp_path/'configs/data_sync/testing_discard_opening_rounds_20261108.json', policy)
    workspace = tmp_path/'workspace'
    save(workspace/'verify/testing-discard-opening-rounds-20261108-v1/status.json', {
        'contract': 'stockagent.local_testing_cleanup.v1', 'policy_identity_sha256': policy['identity_sha256'],
        'state': 'completed', 'observed_at_epoch': NOW, 'targets_total': 1, 'targets_completed': 1,
        'apply': True, 'preservation_waived_by_user': True, 'recovery_claimed': False,
        'cold_objects_deleted': False, 'nas_objects_deleted': False, 'reclaimed_allocated_bytes': 1234})
    row, = ops.local_testing_cleanup_tasks(tmp_path, workspace)
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': [row],
        'schedules': [{'id': 'opening_perf_cleanup', 'registered': True, 'timer_active': True,
                       'owner_running': False, 'owner_state': 'inactive'}]}, now=NOW)['tasks'][0]
    assert public['complete_evidence'] and public['metrics']['discarded_allocated_bytes'] == 1234
    assert public['continuation']['automatic'] is True
    assert public['continuation']['schedule_labels'] == ['舊效能測試副本回收']


def test_opening_round_cleanup_eta_uses_completed_file_checks(tmp_path):
    row = ops.task('local_testing_cleanup', 'fixed', state='running', phase='opening_perf_cleanup_working',
                   observed=NOW, generation='fixed', done_items=2, total_items=21)
    row['_opening_perf_timer'] = True
    first, history = ops.estimate(row, {}, now=NOW)
    assert first['state'] == 'sampling'
    row.update(completed_items=5, observed_at_utc=ops.iso(NOW+60))
    estimate, _ = ops.estimate(row, history, now=NOW+60)
    assert estimate['state'] == 'estimated' and estimate['unit'] == 'items'
    assert estimate['remaining_seconds'] == 320 and estimate['scope'] == 'current_phase_only'


@pytest.mark.parametrize(('pin', 'policy_file', 'status_dir'), [
    ('OPENING_REVIEWED_POLICY_SHA256', 'testing_discard_opening_reviewed_20261108.json',
     'testing-discard-opening-reviewed-20261108-v1'),
    ('MONITOR_IMAGES_POLICY_SHA256', 'testing_discard_monitor_images_20261108.json',
     'testing-discard-monitor-images-20261108-v1'),
])
def test_reviewed_opening_cleanup_shares_timer_without_file_rate_forecast(tmp_path, monkeypatch, pin, policy_file, status_dir):
    from stockagent.data_sync.testing_disposition import LOCAL_TESTING_CONTRACT, identity
    policy = dict(contract=LOCAL_TESTING_CONTRACT, authority_node_id='penguin', origin_node_id='penguin',
        authorization='user approved', decision='discard_without_preservation',
        targets=[dict(base='artifact', relative=f'operations/test/trial{n}.json', kind='file', reason='benchmark')
                 for n in range(2)])
    policy['identity_sha256'] = identity(policy)
    monkeypatch.setattr(ops, pin, policy['identity_sha256'])
    save(tmp_path/'configs/data_sync'/policy_file, policy)
    workspace = tmp_path/'workspace'
    save(workspace/'verify'/status_dir/'status.json', {
        'contract': 'stockagent.local_testing_cleanup.v1', 'policy_identity_sha256': policy['identity_sha256'],
        'state': 'checking_consumers', 'observed_at_epoch': NOW, 'targets_total': 2, 'targets_completed': 1,
        'apply': True, 'preservation_waived_by_user': True, 'recovery_claimed': False,
        'cold_objects_deleted': False, 'nas_objects_deleted': False, 'reclaimed_allocated_bytes': 1234})
    row, = ops.local_testing_cleanup_tasks(tmp_path, workspace)
    assert row['_opening_perf_timer'] and row['state'] == 'running'
    assert row['phase'] != 'opening_perf_cleanup_working'
    eta, _ = ops.estimate(row, {}, now=NOW)
    assert eta['state'] == 'unknown' and eta['reason'] == 'heterogeneous_workloads'


def test_d_cleanup_observes_its_own_fixed_policy_without_counting_wsl(tmp_path):
    from stockagent.data_sync.testing_disposition import D_WORKFLOW_TESTING_CONTRACT, identity
    policy = dict(contract=D_WORKFLOW_TESTING_CONTRACT, authority_node_id='penguin', origin_node_id='penguin',
        authorization='user approved old trial copies', decision='discard_without_preservation',
        targets=[dict(base='d-workflow-benchmark', relative='benchmarks/round69/full_trial_0_baseline',
                      kind='directory', reason='benchmark')])
    policy['identity_sha256'] = identity(policy)
    save(tmp_path/'configs/data_sync/d_workflow_trial_directories_disposition_20261106.json', policy)
    state = tmp_path/'d-state'
    save(state/'status.json', dict(contract='stockagent.local_testing_cleanup.v1',
        policy_identity_sha256=policy['identity_sha256'], state='discarding_exact_target',
        observed_at_epoch=NOW, targets_total=1, targets_completed=0, targets_blocked=0,
        apply=True, preservation_waived_by_user=True, recovery_claimed=False,
        cold_objects_deleted=False, nas_objects_deleted=False, reclaimed_allocated_bytes=4096))
    row, = ops.d_testing_cleanup_tasks(tmp_path, state)
    public = view(row)['tasks'][0]
    assert public['kind'] == 'd_testing_cleanup' and public['metrics']['discarded_allocated_bytes'] == 4096
    assert public['complete_evidence'] is False and 'D 槽' in public['label']
    assert ops.TASK_SCHEDULES['d_testing_cleanup'] == ('d_testing_cleanup',)
    estimate, _ = ops.estimate(row, {}, now=NOW)
    assert estimate['state'] == 'unknown' and estimate['reason'] == 'heterogeneous_workloads'
    save(state/'status.json', {'policy_identity_sha256': 'other'})
    assert ops.d_testing_cleanup_tasks(tmp_path, state)[0]['state'] == 'unknown'


def test_backup_retry_observation_does_not_renew_or_relabel_prior_scan(tmp_path):
    root = tmp_path / 'backup'
    save(root/'transport-scan-status.json', {
        'contract': 'backup_transport_scan_notification_v1', 'observed_at_epoch': NOW - 600,
        'pending_batch_scans': 7})
    retry = tmp_path/'retry.json'
    save(retry, {'schema_version': 1, 'observed_at_epoch': NOW,
                 'requested_scan_policy': {'backup_transport': True},
                 'backup_transport': {'state': 'existing_source_owner_busy', 'NAS_backup_verified': False}})
    row = next(row for row in ops.backup_tasks(root, retry) if row['kind'] == 'transport')
    assert row['state'] == 'waiting' and row['reason'] == 'original_owner'
    assert ops.timestamp(row['observed_at_utc']) == NOW
    assert ops.timestamp(row['last_scan_observed_at_utc']) == NOW - 600
    assert row['metrics']['pending_batches'] is None and row['complete_evidence'] is False
    assert view(row)['tasks'][0]['notification_observation'] == 'retry'
    bad = json.loads(retry.read_text())
    bad['backup_transport']['state'] = 'notification_retry_failed'
    save(retry, bad)
    assert next(row for row in ops.backup_tasks(root, retry) if row['kind'] == 'transport')['state'] == 'blocked'
    bad['requested_scan_policy'] = None
    save(retry, bad)
    row = next(row for row in ops.backup_tasks(root, retry) if row['kind'] == 'transport')
    assert ops.timestamp(row['observed_at_utc']) == NOW - 600


def test_fixed_encoding_progress_is_phase_scoped_and_requires_exact_prune_receipt(tmp_path):
    dataset = 'legacy-artifact-markets-crypto'
    binding = {'relative_root': 'markets/crypto', 'snapshot_id': 'fixed-original',
               'manifest_sha256': 'a'*64, 'retirement_receipt_sha256': 'b'*64}
    save(tmp_path/'configs/data_sync/legacy_stage_retirement.json', {
        'schema_version': 1, 'contract': 'reviewed_retired_legacy_d_encoding_v1',
        'authority_node_id': 'penguin', 'datasets': {dataset: binding}})
    root = tmp_path/'state'
    progress = {'contract': 'manual-stage-prune-progress-v1', 'dataset': dataset,
                'snapshot_id': binding['snapshot_id'], 'manifest_sha256': binding['manifest_sha256'],
                'acceptance_proof': False, 'checked_at_epoch': NOW,
                'state': 'running', 'prune_step': 'plan-check-receipts',
                'completed_items': 15, 'total_items': 90}
    save(root/dataset/'stage-prune-progress.json', progress)
    row, = ops.encoding_stage_tasks(tmp_path, root)
    assert row['state'] == 'running' and row['completed_items'] == 15
    assert row['complete_evidence'] is False
    result = {'dataset': dataset, 'snapshot_id': 'wrong', 'manifest_sha256': binding['manifest_sha256'],
              'action': 'encoding-stage-pruned', 'deleted': True, 'prune_receipt_sha256': 'c'*64,
              'cold_objects_deleted': 0, 'nas_snapshots_deleted': 0,
              'reclaimed_allocated_file_bytes': 1000, 'checked_at_epoch': NOW+60}
    save(root/dataset/'result.json', result)
    assert ops.encoding_stage_tasks(tmp_path, root)[0]['complete_evidence'] is False
    result['snapshot_id'] = binding['snapshot_id']
    save(root/dataset/'result.json', result)
    row, = ops.encoding_stage_tasks(tmp_path, root)
    assert row['state'] == 'completed' and row['complete_evidence'] is True
    assert row['completed_bytes'] == 1000 and row['observed_at_utc'] == ops.iso(NOW+60)
    assert ops.TASK_SCHEDULES['encoding_stage_retirement'] == ('artifact_retirement',)
    assert view(row)['tasks'][0]['complete_evidence'] is True


def test_fixed_encoding_retry_limit_and_unbound_observation_remain_visible(tmp_path):
    dataset = 'legacy-artifact-markets-us'
    binding = {'snapshot_id': 'original', 'manifest_sha256': 'a'*64}
    save(tmp_path/'configs/data_sync/legacy_stage_retirement.json', {
        'schema_version': 1, 'contract': 'reviewed_retired_legacy_d_encoding_v1',
        'authority_node_id': 'penguin', 'datasets': {dataset: binding}})
    save(tmp_path/'state'/dataset/'retry.json', {'binding': binding, 'attempts': 3, 'checked_at_epoch': NOW})
    row, = ops.encoding_stage_tasks(tmp_path, tmp_path/'state')
    assert row['state'] == 'blocked' and row['reason'] == 'owner_retry_limit'
    assert row['complete_evidence'] is False


def test_encoding_content_eta_uses_bytes_and_keeps_metadata_as_separate_phase():
    first = ops.task('encoding_stage_retirement', 'fixed', state='running',
        phase='verify-encoding-stage-files', generation='release', observed=NOW-60,
        done_bytes=0, total_bytes=1000, done_items=0, total_items=100)
    _eta, history = ops.estimate(first, {}, now=NOW-60)
    row = {**first, 'observed_at_utc': ops.iso(NOW), 'completed_bytes': 900, 'completed_items': 10}
    eta, _history = ops.estimate(row, history, now=NOW)
    assert eta['state'] == 'estimated' and eta['unit'] == 'bytes'
    assert eta['remaining_seconds'] < 10


@pytest.mark.parametrize(('kind', 'phase'), [('handoff_index_archive', 'handoff_source_packing'),
    ('handoff_source_repack', 'handoff_source_packing'), ('handoff_private_d', 'handoff_source_D_installing'),
    ('handoff_index_D_storage', 'handoff_source_D_installing'),
    ('source_local_repack_queue', 'source_local_D_installing'),
    ('source_local_repack_queue', 'source_local_E_packing')])
def test_handoff_sequential_pack_progress_and_eta_use_bytes(kind, phase):
    first = ops.task(kind, 'fixed', state='running', phase=phase, generation='release', observed=NOW-60,
                     done_bytes=0, total_bytes=1000, done_items=0, total_items=100)
    _eta, history = ops.estimate(first, {}, now=NOW-60)
    row = {**first, 'observed_at_utc': ops.iso(NOW), 'completed_bytes': 900, 'completed_items': 10}
    row['eta'], _history = ops.estimate(row, history, now=NOW)
    public = view(row)['tasks'][0]
    assert public['progress']['metric'] == 'bytes' and public['progress']['percent'] == 90
    assert public['eta']['unit'] == 'bytes' and public['eta']['remaining_seconds'] < 10


@pytest.mark.parametrize(('done_bytes', 'done_items', 'expected'), [
    (100_000_000, 100, 'unknown'),      # Large leading files, tiny remaining files.
    (100_000_000, 0, 'unknown'),        # One unfinished large file, no item rate yet.
    (10_000_000, 10000, 'estimated'),   # Representative byte and item mix.
])
@pytest.mark.parametrize('source_kind', ['source_local', 'futures_725'])
def test_large_source_pack_eta_does_not_extrapolate_an_unrepresentative_file_mix(
        done_bytes, done_items, expected, source_kind):
    futures = source_kind == 'futures_725'
    first = ops.task('handoff_index_archive' if futures else 'source_local_repack_queue', 'large-source', state='running',
        phase='handoff_source_packing' if futures else 'source_local_E_packing', generation='release', observed=NOW-60,
        done_bytes=0, total_bytes=100_000_000, done_items=0, total_items=100_000)
    if futures:
        first['subject'] = 'tw-futures-universe725-workspace'
    _, history = ops.estimate(first, {}, now=NOW-60)
    row = {**first, 'observed_at_utc': ops.iso(NOW), 'completed_bytes': done_bytes,
           'completed_items': done_items}
    # Keep a nonzero byte remainder in the two heterogeneous examples.
    if done_bytes == 100_000_000:
        row['total_bytes'] = 1_000_000_000
        first['total_bytes'] = 1_000_000_000
        _, history = ops.estimate(first, {}, now=NOW-60)
    eta, _ = ops.estimate(row, history, now=NOW)
    assert eta['state'] == expected
    if expected == 'unknown':
        assert eta['reason'] == 'heterogeneous_workloads' and eta['completion_at_utc'] is None
    else:
        assert eta['remaining_seconds'] > 0


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def work(**changes):
    return ops.task("retirement", "round", state="running", phase="decode-originals",
        observed=NOW, generation="fixed-original-sha", done_bytes=100, total_bytes=1000,
        done_items=2, total_items=10, **changes)


def view(row, *, now=NOW):
    return ops.public_status({"contract": ops.CONTRACT, "observed_at_utc": ops.iso(now), "tasks": [row]}, now=now)


def provider_workspace_fixture(tmp_path, name='finmind'):
    repo, workspace = tmp_path / 'repo', tmp_path / 'e'
    generation = 'provider-state-temporary-20261103'
    root = workspace / 'migration' / generation / name
    destination = workspace / 'downloads' / generation / name / 'data'
    entry = {'alias': str(repo / ('data_' + name)), 'destination': str(destination),
             'backup': str(repo / ('.data_' + name + '.pre-e-20261103')),
             'evidence': str(root), 'state': 'copying', 'filesystem_uuid': 'fixed-e-uuid'}
    registry = tmp_path / 'registry.json'
    value = {'contract': 'temporary_provider_e_workspace_v1', 'providers': {name: entry}}
    save(registry, value)
    intent = {'contract': 'workspace_scoped_relocation_v1', 'source': entry['alias'],
              'destination': str(destination), 'files': 100, 'logical_bytes': 1000,
              'started_at_epoch': NOW - 1000, 'excluded_subtrees': ['excluded-us-history']}
    save(root / 'intent.json', intent)
    save(root / 'progress.json', {'state': 'copying_and_independent_sha_readback',
        'files_completed': 90, 'files_total': 100, 'bytes_completed': 500,
        'bytes_total': 1000, 'observed_at_epoch': NOW})
    return repo, workspace, registry, root, value, intent


def test_provider_workspace_byte_bar_and_slow_file_tail_use_distinct_units(tmp_path):
    repo, workspace, registry, root, _, _ = provider_workspace_fixture(tmp_path)
    row, = ops.provider_workspace_tasks(repo, registry, workspace)
    assert row['state'] == 'running' and not row['complete_evidence']
    assert row['completed_bytes'] == 500 and row['completed_items'] == 90
    save(root / 'progress.json', {'state': 'copying_and_independent_sha_readback',
        'files_completed': 91, 'bytes_completed': 900, 'observed_at_epoch': NOW + 60})
    _, history = ops.estimate(row, {}, now=NOW)
    later, = ops.provider_workspace_tasks(repo, registry, workspace)
    later['eta'], _ = ops.estimate(later, history, now=NOW + 60)
    public = view(later, now=NOW + 60)['tasks'][0]
    assert public['progress']['metric'] == 'bytes' and public['progress']['percent'] == 90
    assert public['eta']['unit'] == 'items' and public['eta']['remaining_seconds'] == 540
    assert 'FinMind' in public['label']
    assert 'C 原件已回收' in public['scope'] and '排除子樹不計入' in public['scope']
    encoded = json.dumps(public)
    assert str(repo) not in encoded and str(workspace) not in encoded and 'fixed-e-uuid' not in encoded


def provider_runtime_failure_fixture(tmp_path, monkeypatch):
    repo, workspace, registry, root, _, intent = provider_workspace_fixture(tmp_path)
    progress = json.loads((root / 'progress.json').read_bytes())
    digest = lambda value: hashlib.sha256(json.dumps(value, sort_keys=True,
        separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    owner = {'pid': 1234, 'start_ticks': 10, 'boot_id': 'fixed-boot'}
    child = {'pid': 2345, 'start_ticks': 20, 'boot_id': 'fixed-boot'}
    value = {'contract': 'stockagent.provider_runtime_observation.v1', 'owner': owner,
             'acceptance': False, 'source_files_deleted': 0, 'providers': {'finmind': {
                 'process': child, 'intent_sha256': digest(intent), 'progress_sha256': digest(progress),
                 'observed_at_epoch': NOW, 'reason_code': 'provider_source_drift',
                 'private_exception': 'TOKEN_MUST_NOT_BE_PUBLISHED'}}}
    path = tmp_path / 'runtime-diagnostic.json'
    save(path, value)
    monkeypatch.setattr(ops, '_process_identity_matches', lambda identity: identity in (owner, child))
    return repo, workspace, registry, root, path, value


def test_failed_provider_child_is_not_running_or_finite_eta_and_retains_original_clocks(tmp_path, monkeypatch):
    repo, workspace, registry, root, path, value = provider_runtime_failure_fixture(tmp_path, monkeypatch)
    value['providers']['finmind']['observed_at_epoch'] = NOW + 60
    save(path, value)
    row, = ops.provider_workspace_tasks(repo, registry, workspace, diagnostic_path=path, now=NOW + 60)
    assert row['state'] == 'blocked' and not row['complete_evidence']
    assert row['observed_at_utc'] == ops.iso(NOW)
    assert row['failure_observed_at_utc'] == ops.iso(NOW + 60)
    row['checked_at_utc'] = ops.iso(NOW + 60)
    public = view(row, now=NOW + 60)['tasks'][0]
    assert public['eta']['state'] == 'blocked' and public['eta']['completion_at_utc'] is None
    assert public['reason_code'] == 'provider_source_drift'
    assert '來源清冊' in public['reason']
    encoded = json.dumps(public)
    assert 'TOKEN_MUST_NOT_BE_PUBLISHED' not in encoded
    assert '2345' not in encoded and str(workspace) not in encoded
    assert view(row, now=NOW + 500)['tasks'][0]['eta']['state'] == 'stale'


@pytest.mark.parametrize('changed', ['progress', 'intent', 'owner', 'child', 'clock', 'reason'])
def test_provider_runtime_failure_cannot_cross_attempt_or_process_boundaries(tmp_path, monkeypatch, changed):
    repo, workspace, registry, root, path, value = provider_runtime_failure_fixture(tmp_path, monkeypatch)
    if changed in ('progress', 'intent'):
        target = root / ('progress.json' if changed == 'progress' else 'intent.json')
        data = json.loads(target.read_bytes())
        data['new_attempt_marker'] = True
        save(target, data)
    elif changed in ('owner', 'child'):
        # Process identity includes start ticks, not merely a reused PID.
        monkeypatch.setattr(ops, '_process_identity_matches', lambda identity:
            identity is not None and identity.get('pid') != (1234 if changed == 'owner' else 2345))
    elif changed == 'clock':
        value['providers']['finmind']['observed_at_epoch'] = NOW + 1000
        save(path, value)
    else:
        value['providers']['finmind']['reason_code'] = 'arbitrary_private_reason'
        save(path, value)
    row, = ops.provider_workspace_tasks(repo, registry, workspace, diagnostic_path=path, now=NOW)
    assert row['state'] == 'running' and not row.get('failure_observed_at_utc')


def test_provider_failure_is_diagnostic_and_never_overrides_real_copy_or_handoff(tmp_path, monkeypatch):
    repo, workspace, registry, root, path, value = provider_runtime_failure_fixture(tmp_path, monkeypatch)
    value['providers']['finmind']['reason_code'] = 'provider_namespace_registry'
    save(path, value)
    copied = {'contract': 'workspace_scoped_relocation_v1', 'state': 'exact_destination_readback_verified',
        'selected_source_sha256_and_destination_readback_verified': True, 'source_namespace_unchanged': True,
        'source_files_deleted': 0, 'files': 100, 'logical_bytes': 1000,
        'excluded_subtrees': ['excluded-us-history'], 'observed_at_epoch': NOW}
    save(root / 'copy-result.json', copied)
    copy_row, handoff = ops.provider_workspace_tasks(repo, registry, workspace, diagnostic_path=path, now=NOW)
    assert copy_row['complete_evidence'] and copy_row['state'] == 'completed'
    assert handoff['state'] == 'blocked' and not handoff['complete_evidence']
    assert handoff['reason'] == 'provider_namespace_registry'
    value = json.loads(registry.read_bytes())
    entry = value['providers']['finmind']
    entry.update(state='active', copy_result_sha256='a' * 64, activated_at_epoch=NOW)
    save(registry, value)
    save(root / 'placement-result.json', {**entry, 'provider': 'finmind', 'original_files_deleted': 0})
    save(root / 'namespace-readback.json', {'provider': 'finmind', 'copy_result_sha256': 'a' * 64,
        'namespace_readbacks': [{'independent_stat': [1, 2]}], 'service_code_mounts_changed': 0})
    copy_row, handoff = ops.provider_workspace_tasks(repo, registry, workspace, diagnostic_path=path, now=NOW)
    assert handoff['state'] == 'completed' and not handoff.get('failure_observed_at_utc')


def test_runtime_diagnostic_rejects_reused_pid_and_previous_boot(tmp_path):
    identity = {'pid': 1234, 'start_ticks': 5678, 'boot_id': 'same-boot'}
    path = tmp_path / 'sys/kernel/random/boot_id'
    path.parent.mkdir(parents=True)
    path.write_text('same-boot\n')
    path = tmp_path / '1234/stat'
    path.parent.mkdir()
    fields = ['S'] + ['0'] * 18 + ['5678', '0']
    path.write_text('1234 (name with spaces) ' + ' '.join(fields))
    assert ops._process_identity_matches(identity, proc_root=tmp_path)
    assert not ops._process_identity_matches({**identity, 'start_ticks': 5679}, proc_root=tmp_path)
    assert not ops._process_identity_matches({**identity, 'boot_id': 'old-boot'}, proc_root=tmp_path)


def test_additional_sqlite_scratch_observations_bind_their_fixed_sha_and_keep_distinct_ids(tmp_path):
    save(tmp_path / 'journal.json', {'snapshot_sha256': 'a' * 64, 'snapshot_bytes': 1000})
    save(tmp_path / 'complete.json', {'contract': 'private_sqlite_workspace_relocation_v1',
        'state': 'completed', 'snapshot_sha256': 'a' * 64, 'owned_scratch_files_deleted': 1,
        'source_files_deleted': 0, 'cold_objects_deleted': 0, 'nas_files_deleted': 0,
        'observed_at_epoch': NOW})
    old, = ops.workspace_private_scratch_tasks(tmp_path)
    new, = ops.workspace_private_scratch_tasks(tmp_path, identity='second-exact-root',
        expected_sha256='a' * 64, subject='finlab')
    assert old['id'] != new['id'] and new['complete_evidence']
    assert 'FinLab' in view(new)['tasks'][0]['label']
    with pytest.raises(ValueError, match='identity differs'):
        ops.workspace_private_scratch_tasks(tmp_path, expected_sha256='b' * 64)


def test_provider_copy_completion_waits_for_actual_alias_and_namespace_receipts(tmp_path):
    repo, workspace, registry, root, value, intent = provider_workspace_fixture(tmp_path)
    copied = {'contract': intent['contract'], 'state': 'exact_destination_readback_verified',
              'selected_source_sha256_and_destination_readback_verified': True,
              'all_source_sha256_and_destination_readback_verified': False,
              'source_namespace_unchanged': True, 'source_files_deleted': 0,
              'files': 100, 'logical_bytes': 1000, 'excluded_subtrees': intent['excluded_subtrees'],
              'observed_at_epoch': NOW}
    save(root / 'copy-result.json', copied)
    copied_row, row = ops.provider_workspace_tasks(repo, registry, workspace)
    assert copied_row['state'] == 'completed' and copied_row['phase'] == 'workspace_copy'
    assert copied_row['complete_evidence']
    assert row['state'] == 'waiting' and row['phase'] == 'workspace_handoff'
    assert row['completed_bytes'] is None and row['completed_items'] is None
    assert not row['complete_evidence']
    assert view(row)['tasks'][0]['eta']['completion_at_utc'] is None
    row['checked_at_utc'] = ops.iso(NOW)
    public = view(row)['tasks'][0]
    assert public['eta']['reason'] == 'provider_workspace_handoff'
    assert public['observed_at_utc'] is None
    assert not public['progress_stale']
    assert 'C 原件仍保留' in public['eta']['message']
    assert view(row, now=NOW + 400)['tasks'][0]['eta']['state'] == 'stale'
    entry = value['providers']['finmind']
    entry.update(state='active', copy_result_sha256='a' * 64, activated_at_epoch=NOW)
    save(registry, value)
    placed = {**entry, 'provider': 'finmind', 'original_files_deleted': 0}
    save(root / 'placement-result.json', placed)
    _, row = ops.provider_workspace_tasks(repo, registry, workspace)
    assert not row['complete_evidence']  # Registry/placement alone is insufficient.
    ns = {'provider': 'finmind', 'copy_result_sha256': 'a' * 64,
          'namespace_readbacks': [{'independent_stat': [1, 2]}], 'service_code_mounts_changed': 0}
    save(root / 'namespace-readback.json', ns)
    _, row = ops.provider_workspace_tasks(repo, registry, workspace)
    assert row['complete_evidence'] and row['phase'] == 'provider_workspace_completed'
    assert view(row, now=NOW + 10000)['tasks'][0]['state'] == 'completed'
    ns['copy_result_sha256'] = 'b' * 64
    save(root / 'namespace-readback.json', ns)
    assert not ops.provider_workspace_tasks(repo, registry, workspace)[-1]['complete_evidence']
    ns['copy_result_sha256'] = 'a' * 64
    save(root / 'namespace-readback.json', ns)
    copied['excluded_subtrees'] = []
    save(root / 'copy-result.json', copied)
    assert ops.provider_workspace_tasks(repo, registry, workspace)[0]['state'] == 'blocked'


def test_provider_workspace_replan_rejects_old_progress_and_resets_estimate(tmp_path):
    repo, workspace, registry, root, _, intent = provider_workspace_fixture(tmp_path)
    row, = ops.provider_workspace_tasks(repo, registry, workspace)
    _, history = ops.estimate(row, {}, now=NOW)
    intent['started_at_epoch'] = NOW + 1
    save(root / 'intent.json', intent)
    later, = ops.provider_workspace_tasks(repo, registry, workspace)
    assert later['state'] == 'waiting' and later['completed_bytes'] is None
    assert later['observed_at_utc'] is None
    eta, _ = ops.estimate(later, history, now=NOW + 1)
    assert eta['completion_at_utc'] is None


def test_provider_workspace_never_reads_unenrolled_paths_or_walks_payloads(tmp_path, monkeypatch):
    repo, workspace, registry, root, value, _ = provider_workspace_fixture(tmp_path)
    reads = []
    original = ops.metadata
    def tracked(path, **kw):
        reads.append(path)
        return original(path, **kw)
    monkeypatch.setattr(ops, 'metadata', tracked)
    monkeypatch.setattr(Path, 'iterdir', lambda _: pytest.fail('no payload walk'))
    row, = ops.provider_workspace_tasks(repo, registry, workspace)
    assert row['state'] == 'running' and len(reads) == 6
    value['providers']['finmind']['evidence'] = str(tmp_path / 'private-unknown')
    save(registry, value)
    reads.clear()
    row, = ops.provider_workspace_tasks(repo, registry, workspace)
    assert row['state'] == 'blocked' and reads == [registry]
    value['providers']['arbitrary-provider'] = {}
    save(registry, value)
    with pytest.raises(ValueError, match='registry differs'):
        ops.provider_workspace_tasks(repo, registry, workspace)


def accepted_provider_workspaces(tmp_path):
    providers = {}
    for name in ('finlab', 'finmind', 'tej'):
        repo, workspace, registry, root, value, intent = provider_workspace_fixture(tmp_path, name)
        entry = value['providers'][name]
        entry.update(state='active', copy_result_sha256='a' * 64, activated_at_epoch=NOW)
        Path(entry['backup']).mkdir(parents=True)
        providers[name] = entry
        save(root / 'copy-result.json', {
            'contract': intent['contract'], 'state': 'exact_destination_readback_verified',
            'selected_source_sha256_and_destination_readback_verified': True,
            'source_namespace_unchanged': True, 'source_files_deleted': 0,
            'files': intent['files'], 'logical_bytes': intent['logical_bytes'],
            'excluded_subtrees': intent['excluded_subtrees'], 'observed_at_epoch': NOW})
        save(root / 'placement-result.json', {**entry, 'provider': name, 'original_files_deleted': 0})
        save(root / 'namespace-readback.json', {'provider': name, 'copy_result_sha256': 'a' * 64,
            'namespace_readbacks': [{'independent_stat': [1, 2]}], 'service_code_mounts_changed': 0})
    save(registry, {'contract': 'temporary_provider_e_workspace_v1', 'providers': providers})
    return repo, workspace, registry, providers


def test_e_copy_and_alias_acceptance_never_report_c_retirement_or_its_eta(tmp_path, monkeypatch):
    repo, workspace, registry, _ = accepted_provider_workspaces(tmp_path)
    monkeypatch.setattr(Path, 'iterdir', lambda _: pytest.fail('no payload census'))
    rows = ops.provider_workspace_tasks(repo, registry, workspace)
    assert len(rows) == 7 and sum(row['complete_evidence'] for row in rows) == 6
    retained = rows[-1]
    assert retained['kind'] == 'provider_originals' and retained['state'] == 'waiting'
    assert not retained['complete_evidence']
    assert retained['metrics'] == {'retained_original_roots': 3, 'total_original_roots': 3,
                                   'selected_copy_bytes': 3000}
    retained['checked_at_utc'] = ops.iso(NOW)
    public = view(retained)['tasks'][0]
    assert public['progress']['completed_bytes'] is None
    assert public['progress']['total_bytes'] is None
    assert public['observed_at_utc'] is None and not public['progress_stale']
    assert public['eta']['state'] == 'unknown' and public['eta']['completion_at_utc'] is None
    assert public['eta']['reason'] == 'provider_originals_pending'
    assert '不是 C 實際配置量或可回收量' in public['scope']
    assert view(retained, now=NOW + 400)['tasks'][0]['eta']['state'] == 'stale'


def test_missing_or_redirected_c_root_is_presence_only_not_retirement_proof(tmp_path):
    repo, workspace, registry, providers = accepted_provider_workspaces(tmp_path)
    root = Path(providers['finlab']['backup'])
    root.rmdir()  # An absent name is not proof that its bytes were safely reclaimed.
    row = ops.provider_workspace_tasks(repo, registry, workspace)[-1]
    assert row['metrics']['retained_original_roots'] == 2 and not row['complete_evidence']
    root.symlink_to(tmp_path / 'outside', target_is_directory=True)
    row = ops.provider_workspace_tasks(repo, registry, workspace)[-1]
    assert row['metrics']['retained_original_roots'] is None and not row['complete_evidence']
    assert row['state'] == 'waiting' and row['completed_bytes'] is None


def test_completed_retirement_cohort_does_not_hide_retained_c_sources():
    completed = ops.task('retirement', 'one-cohort', state='completed',
                         phase='done', observed=NOW, proof=True)
    retained = ops.task('provider_originals', 'source-handoff-retained', state='waiting',
                        phase='provider_originals_pending', reason='provider_originals_pending',
                        metrics={'retained_original_roots': 9, 'total_original_roots': 9,
                                 'selected_copy_bytes': 1000})
    retained['subject'] = 'source-originals'
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [completed, retained]}, now=NOW)
    stage = next(row for row in result['flow'] if row['id'] == 'retire')
    assert stage['state'] == 'waiting'
    assert (stage['completed_tasks'], stage['pending_tasks']) == (1, 1)
    assert stage['known_current_step_completion_at_utc'] is None
    row = next(row for row in result['tasks'] if row['kind'] == 'provider_originals')
    assert '已交接的一般來源' in row['label']
    assert row['metrics']['retained_original_roots'] == 9
    assert row['progress']['completed_bytes'] is None and not row['complete_evidence']
    unassessed = ops.task('workspace_source_staging', 'unassessed', state='completed',
                          phase='workspace_source_copy_verified', observed=NOW, proof=True)
    unassessed['subject'] = 'tw-futures-prepared'
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [completed, unassessed]}, now=NOW)
    stage = next(row for row in result['flow'] if row['id'] == 'retire')
    assert stage['state'] == 'blocked' and stage['unassessed_source_count'] == 1


def test_paired_lake_relay_memory_failure_is_visible_without_nas_acceptance(tmp_path):
    pairing = {'producer_device_id': 'producer', 'receiver_device_id': 'receiver'}
    job = {'delivery': 'lake-' + 'a' * 64, 'state': 'deferred',
           'failed_phase': 'native_DuckLake_full_table_SHA',
           'original_error_type': 'OutOfMemoryException', 'attempts': 189,
           'last_failure_at_utc': ops.iso(NOW - 10), 'next_retry_at_utc': ops.iso(NOW + 590),
           'completed_stages': ['independent_NAS_readback_full_SHA']}
    relay = {**pairing, 'observed_at_utc': ops.iso(NOW), 'single_owner_verified': True,
             'runtime_lock_verified': True, 'nas_mount_guard_verified': True, 'jobs': [job]}
    path = tmp_path / 'lakehouse/relay-status.json'
    save(path, relay)
    row, = ops.lake_relay_tasks(tmp_path, pairing, now=NOW)
    assert row['kind'] == 'lake_relay' and row['state'] == 'blocked'
    assert row['reason'] == 'lake_relay_memory_exhausted' and not row['complete_evidence']
    assert row['diagnosis_at_utc'] == ops.iso(NOW - 10)
    assert row['next_attempt_at_utc'] == ops.iso(NOW + 590)
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [row]}, now=NOW)
    assert result['tasks'][0]['reason_code'] == 'lake_relay_memory_exhausted'
    assert result['tasks'][0]['diagnosis_at_utc'] == ops.iso(NOW - 10)
    assert next(stage for stage in result['flow'] if stage['id'] == 'nas')['state'] == 'blocked'
    assert result['tasks'][0]['eta']['completion_at_utc'] is None
    assert 'lake-' + 'a' * 64 not in json.dumps(result)
    for key in ('producer_device_id', 'runtime_lock_verified', 'nas_mount_guard_verified'):
        changed = {**relay, key: False}
        save(path, changed)
        assert ops.lake_relay_tasks(tmp_path, pairing, now=NOW) == []
    save(path, {**relay, 'jobs': [{**job, 'state': 'accepted'}]})
    assert ops.lake_relay_tasks(tmp_path, pairing, now=NOW) == []  # no ACK inferred


def test_fixed_binance_inventory_difference_does_not_become_restore_or_reclaim(tmp_path, monkeypatch):
    from scripts import run_workspace_source_staging as queue
    monkeypatch.setattr(queue, 'ROOT', tmp_path)
    names = {'binance-market-e-primary-20261104-v1': 'binance',
             'binance-archive-e-primary-20261104-v1': 'binance-public-archive'}
    monkeypatch.setattr(queue, 'load_sources', lambda: [{'name': name} for name in names])
    monkeypatch.setattr(queue, 'verified_report', lambda item, workspace:
                        {'files': 3, 'logical_bytes': 60, 'observed_at_epoch': NOW - 100})
    counts = {'exact_files': 1, 'changed_files': 1, 'e_only_files': 1, 'd_only_files': 0,
              'exact_bytes': 10, 'changed_bytes': 20, 'e_only_bytes': 30}
    sources = [{'name': name, 'dataset': dataset, 'state': 'inventory_compared',
                'D_recovery_verified': False, 'retirement_authorized': False,
                'source_files_deleted': 0, 'D_snapshot_id': 'fixed-snapshot',
                'D_inventory_sha256': 'a' * 64, 'counts': counts.copy(),
                'staged_files': 3, 'staged_bytes': 60, 'staged_proof_at_epoch': NOW - 100}
               for name, dataset in names.items()]
    value = {'contract': 'stockagent.e_stage_d_inventory_comparison.v2',
             'observed_at_epoch': NOW, 'D_independent_restore_verified': False,
             'NAS_acceptance': False, 'retirement_authorized': False,
             'source_writes': 0, 'source_deletes': 0, 'sources': sources}
    receipt = tmp_path / 'verify/binance-e-cold-inventory-20261106-evening.json'
    save(receipt, value)
    rows = ops.workspace_cold_comparison_snapshot_tasks(tmp_path, tmp_path)
    assert len(rows) == 2 and all(row['complete_evidence'] for row in rows)
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': rows}, now=NOW)
    for row in public['tasks']:
        assert row['metrics']['inventory_changed_bytes'] == 20
        assert '不是 D 讀回' in row['scope'] and '區塊去重後容量' in row['scope']
        assert not row['continuation']['automatic']
    value['sources'][0]['counts']['changed_bytes'] = 21
    save(receipt, value)
    assert len(ops.workspace_cold_comparison_snapshot_tasks(tmp_path, tmp_path)) == 1
    value['retirement_authorized'] = True
    save(receipt, value)
    assert ops.workspace_cold_comparison_snapshot_tasks(tmp_path, tmp_path) == []


def test_partial_cold_readback_is_bound_to_comparison_and_never_authorizes_retirement(tmp_path):
    control, workspace = tmp_path / 'control', tmp_path / 'workspace'
    identity = 'a' * 64
    save(control / 'comparison-status.json', {'state': 'completed', 'identity_sha256': identity})
    save(control / 'delta-restore-receipt.json', {'state': 'member_restore_verified',
        'comparison_identity_sha256': identity, 'member_bytes_and_portable_metadata_verified': True})
    records = [{'object_sha256': 'b' * 64, 'partial_sha256': 'b' * 64,
                'object_bytes': 8, 'partial_bytes': 8, 'partial_matches_full_object': True},
               {'object_sha256': 'c' * 64, 'partial_sha256': hashlib.sha256(b'').hexdigest(),
                'object_bytes': 8, 'partial_bytes': 0, 'partial_matches_full_object': False}]
    value = {'contract': 'stockagent.transport_partial_cold_readback.v1',
             'comparison_identity_sha256': identity, 'observed_at_utc': ops.iso(NOW),
             'archive_sha256_verified': True, 'earlier_e_independent_member_restore_verified': True,
             'retirement_authorized': False, 'nas_acceptance_claimed': False,
             'source_files_deleted': 0, 'cold_objects_deleted': 0, 'objects_verified': 2,
             'partial_exact_full_objects': 1, 'empty_partial_count': 1,
             'object_logical_bytes': 16, 'records': records}
    receipt = workspace / 'verify/transport-retained-partial-cold-readback-20261106-v1.json'
    save(receipt, value)
    row, = ops.workspace_partial_cold_readback_tasks(control, workspace)
    assert row['complete_evidence'] and row['phase'] == 'partial_cold_verified'
    public = view(row)['tasks'][0]
    assert public['metrics']['partial_exact_full_files'] == 1
    assert public['metrics']['partial_empty_files'] == 1
    assert '不授權刪除' in public['scope'] and '不是 NAS 驗收' in public['scope']
    value['retirement_authorized'] = True
    save(receipt, value)
    assert ops.workspace_partial_cold_readback_tasks(control, workspace) == []


def test_partial_name_restore_is_a_separate_bound_stage(tmp_path, monkeypatch):
    from stockagent.data_sync import transport_restore_held_partials as restore
    monkeypatch.setattr(restore, 'FILES', 2)
    monkeypatch.setattr(restore, 'BYTES', 8)
    assert ops.workspace_partial_name_restore_tasks(tmp_path) == []
    body = {'contract': restore.CONTRACT, 'comparison_identity_sha256': restore.COMPARISON,
            'archive_sha256': restore.ARCHIVE_SHA, 'wave_intent_sha256': restore.WAVE,
            'files': 2, 'logical_bytes': 8, 'restore_to_sender_staging': True,
            'retirement_authorized_by_restore': False}
    body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    save(tmp_path / 'partial-name-restore-enrollment.json', body)
    save(tmp_path / 'comparison-status.json', {'state': 'completed', 'identity_sha256': restore.COMPARISON})
    row, = ops.workspace_partial_name_restore_tasks(tmp_path)
    assert row['state'] == 'waiting' and not row['complete_evidence'] and row['total_items'] == 2
    status = {'contract': restore.CONTRACT, 'state': 'restoring',
              'comparison_identity_sha256': restore.COMPARISON, 'files_completed': 1, 'files_total': 2,
              'logical_bytes_completed': 8, 'logical_bytes_total': 8, 'observed_at_epoch': NOW,
              'source_files_deleted': 0, 'NAS_acceptance_claimed': False, 'retirement_authorized': False}
    path = tmp_path / 'partial-name-restore-status.json'
    save(path, status)
    row, = ops.workspace_partial_name_restore_tasks(tmp_path)
    assert row['state'] == 'running' and row['completed_items'] == 1 and not row['complete_evidence']
    status.update(state='restored', files_completed=2, members=[
        {'relative': f'.staging/lake-raw-{restore.WAVE}/payload/blobs-{i:064x}.partial',
         'signature': [1] * 9} for i in range(2)])
    save(path, status)
    row, = ops.workspace_partial_name_restore_tasks(tmp_path)
    assert row['state'] == 'completed' and row['complete_evidence']
    public = view(row)['tasks'][0]
    assert '不表示 D 回收完成或 NAS 已驗收' in public['scope']
    status['members'][1]['relative'] = status['members'][0]['relative']
    save(path, status)
    row, = ops.workspace_partial_name_restore_tasks(tmp_path)
    assert row['state'] == 'blocked' and not row['complete_evidence']


def forecast(**changes):
    return {'state': 'estimated', 'scope': 'current_delivery', 'basis': 'recent_complete_workflows',
            'generated_at_utc': ops.iso(NOW), 'completion_at_utc': ops.iso(NOW+100),
            'fastest_completion_at_utc': ops.iso(NOW+50), 'slowest_completion_at_utc': ops.iso(NOW+200),
            'sample_count': 1, 'confidence': 'low', **changes}


def test_sql_capacity_wait_and_delivery_keep_separate_evidence(tmp_path):
    save(tmp_path/'control-backup-capacity.json', {
        'contract': 'stockagent.lifecycle_sql_capacity.v1', 'state': 'waiting_capacity',
        'observed_at_utc': ops.iso(NOW), 'preserved_staging_root': '/private/staging'})
    row, = ops.lifecycle_backup_tasks(tmp_path)
    assert row['state'] == 'blocked' and row['phase'] == 'capacity-preflight'
    assert row['complete_evidence'] is False
    assert '/private' not in json.dumps(view(row))
    save(tmp_path/'traditional-backup-status.json', {
        'state': 'waiting_encrypted_nas_backup', 'observed_at_utc': ops.iso(NOW+60),
        'dispatch': {'envelope_identity_sha256': 'a'*64, 'complete_bytes': 123, 'complete_files': 11},
        'last_nas_acceptance': {'independent_restore_verified': True}})
    row, = ops.lifecycle_backup_tasks(tmp_path)
    assert row['state'] == 'waiting' and row['phase'] == 'control_sql_delivery'
    assert row['total_bytes'] == 123 and row['total_items'] == 11
    assert row['complete_evidence'] is False  # Earlier ACK cannot accept this delivery.


def test_sql_backup_observer_rejects_unsupported_capacity_receipt(tmp_path):
    save(tmp_path/'control-backup-capacity.json', {'contract': 'unknown', 'state': 'admitted'})
    row, = ops.lifecycle_backup_tasks(tmp_path)
    assert row['state'] == 'unknown' and row['reason'] == 'metadata_unavailable'


def test_whole_batch_forecast_has_explicit_scope_and_whitelisted_actual_deadline():
    row = work(); row['completion_forecast'] = forecast(private_path='/secret', message='private error')
    actual = view(row)['tasks'][0]['completion_forecast']
    assert actual['state'] == 'estimated' and actual['remaining_seconds'] == 100
    assert actual['scope'] == 'current_delivery' and actual['confidence'] == 'low'
    assert '/secret' not in json.dumps(actual) and 'private error' not in json.dumps(actual)
    assert view(row)['overall_eta']['known_completion_at_utc'] == ops.iso(NOW+100)
    assert view(row)['overall_eta']['state'] == 'partial'  # Not all future phases.


@pytest.mark.parametrize('changes', [dict(state='blocked'), dict(observed_at_utc=ops.iso(NOW-301))])
def test_failed_or_stale_work_cannot_keep_a_future_completion_promise(changes):
    row = work(); row.update(changes); row['completion_forecast'] = forecast()
    assert view(row)['tasks'][0]['completion_forecast']['completion_at_utc'] is None


def test_missed_forecast_retains_original_time_without_countdown_reset():
    value = forecast()
    actual = ops.public_forecast(value, now=NOW+101)
    assert actual['state'] == 'overdue' and actual['completion_at_utc'] is None
    assert actual['previous_completion_at_utc'] == ops.iso(NOW+100)
    assert ops.public_forecast(forecast(slowest_completion_at_utc=ops.iso(NOW+1)), now=NOW)['state'] == 'unknown'


def test_packing_eta_uses_member_progress_even_when_bucket_count_does_not_change():
    source = {'stage': 'packing_changed_small_files', 'files_total': 1000, 'files_hashed': 100,
              'files_routed': 1000, 'logical_bytes_processed': 10000, 'logical_bytes_total': 100000}
    row = ops.task('migration_publication', 'fixed', state='running', phase=source['stage'],
        generation='fixed-source', observed=NOW, done_items=2, total_items=16)
    row['eta_work'] = ops.publication_eta_work(source)
    eta, history = ops.estimate(row, {}, now=NOW)
    assert eta['state'] == 'sampling'
    source.update(files_hashed=200, logical_bytes_processed=20000)
    row.update(observed_at_utc=ops.iso(NOW+60), eta_work=ops.publication_eta_work(source))
    row['eta'], _ = ops.estimate(row, history, now=NOW+60)
    actual = view(row, now=NOW+60)['tasks'][0]
    assert actual['progress']['completed_items'] == 2 and actual['progress']['total_items'] == 16
    assert actual['eta']['state'] == 'estimated' and actual['eta']['remaining_seconds'] == 480
    assert actual['eta']['scope'] == 'remaining_source_content'
    assert actual['eta']['unit'] == 'bytes'
    source['stage'] = 'routing_and_hashing'
    assert ops.publication_eta_work(source) is None
    source.update(stage='packing_changed_small_files', files_routed=999)
    assert ops.publication_eta_work(source) is None


def test_rate_uses_actual_phase_deltas_not_other_phase_elapsed():
    row = work(); row["elapsed_seconds"] = 100000
    eta, history = ops.estimate(row, {}, now=NOW)
    assert eta["state"] == "sampling"
    row.update(observed_at_utc=ops.iso(NOW+60), completed_bytes=220)
    eta, history = ops.estimate(row, history, now=NOW+60)
    assert eta["state"] == "estimated" and eta["rate_per_second"] == 2
    assert eta["remaining_seconds"] == 390 and eta["scope"] == "current_phase_only"
    row["eta"] = eta
    projected = view(row, now=NOW+60)["tasks"][0]["eta"]
    assert projected["state"] == "estimated" and projected["message"] is None and projected["reason"] is None
    row["phase"] = "verify-cold-objects"
    assert ops.estimate(row, history, now=NOW+60)[0]["state"] == "sampling"


@pytest.mark.parametrize("change", [{"generation": "different"}, {"total_bytes": 1200}, {"completed_bytes": 50}])
def test_changed_generation_denominator_or_counter_resets_rate(change):
    row = work(); _, h = ops.estimate(row, {}, now=NOW)
    row.update(observed_at_utc=ops.iso(NOW+60), **change)
    assert ops.estimate(row, h, now=NOW+60)[0]["state"] == "sampling"


def test_duplicate_receipt_does_not_invent_samples_or_countdown():
    row = work(); _, h = ops.estimate(row, {}, now=NOW)
    _, h = ops.estimate(row, h, now=NOW+60)
    assert len(h["points"]) == 1
    eta, _ = ops.estimate(row, h, now=NOW+301)
    assert eta["state"] == "stale" and eta["completion_at_utc"] is None


def test_reobserving_estimated_receipt_counts_down_to_same_absolute_deadline():
    row = work()
    _, history = ops.estimate(row, {}, now=NOW)
    row.update(observed_at_utc=ops.iso(NOW+60), completed_bytes=220)
    first, history = ops.estimate(row, history, now=NOW+60)
    later, updated = ops.estimate(row, history, now=NOW+120)
    assert first['completion_at_utc'] == later['completion_at_utc']
    assert later['remaining_seconds'] == first['remaining_seconds']-60
    assert len(updated['points']) == len(history['points']) == 2


def test_waiting_stops_projection_and_resumption_resamples_speed():
    row = work()
    _, history = ops.estimate(row, {}, now=NOW)
    row.update(observed_at_utc=ops.iso(NOW + 60), completed_bytes=220)
    eta, history = ops.estimate(row, history, now=NOW + 60)
    assert eta['state'] == 'estimated'
    row.update(state='waiting', observed_at_utc=ops.iso(NOW + 120))
    waiting, history = ops.estimate(row, history, now=NOW + 120)
    assert waiting['state'] == 'unknown' and waiting['completion_at_utc'] is None
    # Gateway also rejects a stale forecast from an older observer.
    row['eta'] = eta
    assert view(row, now=NOW + 120)['tasks'][0]['eta']['completion_at_utc'] is None
    row.update(state='running', observed_at_utc=ops.iso(NOW + 180), completed_bytes=240)
    resumed, _ = ops.estimate(row, history, now=NOW + 180)
    assert resumed['state'] == 'sampling' and resumed['completion_at_utc'] is None


def test_minute_metadata_checks_do_not_replace_progress_clock_or_invent_rate(monkeypatch, tmp_path):
    source = work()
    monkeypatch.setattr(ops, 'backup_tasks', lambda root, retry_path=None: [dict(source)])
    monkeypatch.setattr(ops, 'migration_tasks', lambda root, **_kwargs: [])
    monkeypatch.setattr(ops, 'retirement_tasks', lambda *args: [])
    monkeypatch.setattr(ops, 'receiver_task', lambda root, **_: ops.task('receiver', 'missing'))
    monkeypatch.setattr(ops, 'schedules', lambda: [])
    history = {}
    for delay in (0, 60, 360):
        result, history = ops.collect(tmp_path, {}, history, now=NOW+delay)
        row = result['tasks'][0]
        assert row['checked_at_utc'] == ops.iso(NOW+delay)
        assert row['observed_at_utc'] == ops.iso(NOW)
        assert row['completed_bytes'] == source['completed_bytes']
        assert len(history['tasks'][row['id']]['points']) == 1
    public = ops.public_status(result, now=NOW+360)['tasks'][0]
    assert public['stale'] is False and public['progress_stale'] is True
    assert public['eta']['reason'] == 'progress_pending'
    assert public['eta']['completion_at_utc'] is None


def test_observer_keeps_more_than_256_enrolled_operations_visible(monkeypatch, tmp_path):
    rows = [ops.task('registered_backup', f'fixture-{number}', state='completed',
                     phase='working', observed=NOW, proof=True) for number in range(300)]
    monkeypatch.setattr(ops, 'backup_tasks', lambda root, retry_path=None: rows)
    monkeypatch.setattr(ops, 'migration_tasks', lambda root, **_kwargs: [])
    monkeypatch.setattr(ops, 'retirement_tasks', lambda *args: [])
    monkeypatch.setattr(ops, 'receiver_task', lambda root, **_: ops.task('receiver', 'missing'))
    monkeypatch.setattr(ops, 'schedules', lambda: [])
    observation, _ = ops.collect(tmp_path, {}, {}, now=NOW)
    assert observation['omitted_task_count'] == 0
    public = ops.public_status(observation, now=NOW)
    assert sum(row['kind'] == 'registered_backup' for row in public['tasks']) == 300
    assert public['summary']['omitted_task_count'] == 0


@pytest.mark.parametrize('free,age,expected', [(0, 0, 'blocked'), (1, 0, 'blocked'),
    (64*1024**2, 0, 'running'), (0, 301, 'running')])
def test_full_physical_cold_volume_suspends_publication_forecast_only_with_fresh_measurement(
        monkeypatch, tmp_path, free, age, expected):
    row = ops.task('migration_publication', 'source', state='running', phase='routing_and_hashing',
        observed=NOW-10, generation='attempt', done_items=10, total_items=20)
    monkeypatch.setattr(ops, 'migration_tasks', lambda root, **_kwargs: [dict(row)])
    monkeypatch.setattr(ops, 'schedules', lambda: [])
    observation, _ = ops.collect(tmp_path, {}, {}, now=NOW, capacities={'penguin-d': {
        'state': 'measured', 'available_bytes': free, 'observed_at_utc': ops.iso(NOW-age)}})
    projected = next(item for item in observation['tasks'] if item['kind'] == 'migration_publication')
    assert projected['state'] == expected
    assert projected['observed_at_utc'] == ops.iso(NOW-10)
    assert projected['completed_items'] == 10
    if expected == 'blocked':
        assert projected['reason'] == 'physical_capacity'
        assert projected['eta']['completion_at_utc'] is None


def test_confirmed_fixed_acceptance_keeps_its_original_time_without_expiring():
    row = work()
    row.update(state='completed', complete_evidence=True, completed_bytes=1000,
               completed_items=10, checked_at_utc=ops.iso(NOW+86400))
    public = view(row, now=NOW+86400)['tasks'][0]
    assert public['checked_at_utc'] == ops.iso(NOW+86400)
    assert public['observed_at_utc'] == ops.iso(NOW)
    assert public['stale'] is False and public['progress_stale'] is False
    assert public['complete_evidence'] is True and public['eta']['state'] == 'completed'


@pytest.mark.parametrize('checked', [None, 'invalid', ops.iso(NOW+1000), ops.iso(NOW-301)])
def test_unread_or_invalid_check_clock_cannot_become_fresh(checked):
    row = work(); row['checked_at_utc'] = checked
    assert view(row)['tasks'][0]['stale'] is True


def test_observer_failure_cannot_be_hidden_by_a_fresh_check_clock():
    row = work(); row['checked_at_utc'] = ops.iso(NOW+301)
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': [row]}
    public = ops.public_status(raw, now=NOW+301)
    assert public['state'] == 'stale' and public['tasks'][0]['stale'] is True


def test_owner_observation_is_reused_without_refreshing_the_counter(monkeypatch, tmp_path):
    row = ops.task('migration', 'fixed', state='running', observed=NOW-1000)
    monkeypatch.setattr(ops, 'backup_tasks', lambda root, retry_path=None: [])
    monkeypatch.setattr(ops, 'migration_tasks', lambda root, **_kwargs: [dict(row)])
    monkeypatch.setattr(ops, 'retirement_tasks', lambda *args: [])
    monkeypatch.setattr(ops, 'receiver_task', lambda root, **_: ops.task('receiver', 'missing'))
    monkeypatch.setattr(ops, 'schedules', lambda: [])
    runtime = {'contract': 'stockagent.storage_runtime.v1', 'nodes': {'penguin': {
        'observed_at_utc': ops.iso(NOW), 'services': [{'id': 'lakehouse', 'state': 'active'}]}}}
    raw, _ = ops.collect(tmp_path, {}, {}, now=NOW, runtime=runtime)
    actual = ops.public_status(raw, now=NOW)['tasks'][0]
    assert actual['owner_state'] == 'active' and actual['observed_at_utc'] == row['observed_at_utc']
    runtime['nodes']['penguin']['observed_at_utc'] = ops.iso(NOW-301)
    raw, _ = ops.collect(tmp_path, {}, {}, now=NOW, runtime=runtime)
    assert raw['tasks'][0]['owner_state'] == 'unknown'


def test_liveness_without_progress_stops_eta_and_missed_estimate_expires():
    row = work(); _, h = ops.estimate(row, {}, now=NOW)
    for i in range(1, 7):
        row.update(observed_at_utc=ops.iso(NOW+i*60), completed_bytes=200)
        eta, h = ops.estimate(row, h, now=NOW+i*60)
    assert eta["state"] == "stalled" and eta["remaining_seconds"] is None
    row["eta"] = eta
    assert view(row, now=NOW+360)["tasks"][0]["eta"]["state"] == "stalled"
    row["eta"] = {"state": "estimated", "remaining_seconds": 1, "completion_at_utc": ops.iso(NOW+359)}
    assert view(row, now=NOW+360)["tasks"][0]["eta"]["state"] == "sampling"


def test_zero_interval_rate_is_not_silently_excluded_from_slow_estimate():
    row = work(); _, h = ops.estimate(row, {}, now=NOW)
    row.update(observed_at_utc=ops.iso(NOW+60), completed_bytes=200)
    _, h = ops.estimate(row, h, now=NOW+60)
    row.update(observed_at_utc=ops.iso(NOW+120))
    eta, _ = ops.estimate(row, h, now=NOW+120)
    assert eta["state"] == "estimated" and eta["slowest_seconds"] is None
    assert eta["remaining_seconds"] == 960  # Includes the minute with no work.


def test_full_counter_does_not_imply_acceptance_or_reclamation():
    row = work(); row.update(completed_bytes=1000, completed_items=10)
    row["metrics"] = {"reclaimed_allocated_bytes": 999999}
    row["eta"], _ = ops.estimate(row, {}, now=NOW)
    value = view(row)
    assert value["tasks"][0]["eta"]["state"] == "finalizing"
    assert value["summary"]["penguin_reclaimed_allocated_bytes"] == 0
    assert value["summary"]["completed"] == 0


def test_small_file_tail_uses_slower_observed_item_rate_and_bar():
    row = work()
    _, history = ops.estimate(row, {}, now=NOW)
    row.update(observed_at_utc=ops.iso(NOW+60), completed_bytes=990, completed_items=3)
    eta, history = ops.estimate(row, history, now=NOW+60)
    assert eta['unit'] == 'items' and eta['remaining_seconds'] == 420
    row['eta'] = eta
    public = view(row, now=NOW+60)['tasks'][0]
    assert public['eta']['unit'] == 'items' and public['progress']['percent'] == 30
    assert public['progress']['completed_bytes'] == 990 and public['progress']['completed_items'] == 3
    row.update(observed_at_utc=ops.iso(NOW+120), completed_bytes=1000, completed_items=4)
    eta, history = ops.estimate(row, history, now=NOW+120)
    assert eta['state'] == 'estimated' and eta['unit'] == 'items'
    assert eta['remaining_seconds'] == 360


def test_full_bytes_without_item_rate_does_not_invent_tail_finish_time():
    row = work()
    row['completed_bytes'] = 1000
    eta, _ = ops.estimate(row, {}, now=NOW)
    assert eta['unit'] == 'items' and eta['state'] == 'sampling'
    assert eta['completion_at_utc'] is None


@pytest.mark.parametrize('change', [{'generation': 'other'}, {'phase': 'verify-cold-objects'},
                                   {'total_items': 20}, {'completed_items': 1}])
def test_small_file_tail_resets_its_own_changed_identity_or_counter(change):
    row = work()
    _, history = ops.estimate(row, {}, now=NOW)
    row.update(observed_at_utc=ops.iso(NOW+60), completed_bytes=990, completed_items=3)
    _, history = ops.estimate(row, history, now=NOW+60)
    row.update(observed_at_utc=ops.iso(NOW+120), completed_bytes=1000, **change)
    eta, _ = ops.estimate(row, history, now=NOW+120)
    assert eta['unit'] == 'items' and eta['state'] == 'sampling'


def test_owner_wait_retains_current_capacity_cause_without_refreshing_coverage(tmp_path):
    save(tmp_path/'private-status.json', {
        'contract': 'stockagent.private_cold_NAS_enrollment.v1',
        'state': 'private_backup_waiting_publication_owner', 'observed_at_utc': ops.iso(NOW),
        'wave_budget': {'transport_detail_bounds_bytes': {
            'staging': 1000, 'retained_transport': 2000, 'physical': -100}}})
    row = ops.backup_tasks(tmp_path)[1]
    assert row['reason'] == 'physical_capacity'
    assert row['activity_at_utc'] == ops.iso(NOW) and row['completed_bytes'] is None


def test_deferred_catalog_reports_physical_budget_without_refreshing_coverage(tmp_path):
    old = ops.iso(NOW-3600)
    save(tmp_path/'status.json', {'observed_at_utc': old, 'activity_at_utc': ops.iso(NOW),
        'combined_nas_coverage': {'contract': 'deduplicated_nas_file_coverage_v1',
            'available_bytes': 200, 'nas_covered_bytes': 100,
            'available_file_count': 2, 'nas_covered_file_count': 1},
        'catalog_refresh': {'contract': 'stockagent.backup_catalog_refresh.v1',
            'state': 'deferred_physical_capacity', 'coverage_recomputed': False,
            'source_free_bytes': 100, 'source_reserve_bytes': 200}})
    row = ops.backup_tasks(tmp_path)[0]
    assert row['reason'] == 'physical_capacity' and row['observed_at_utc'] == old
    assert row['activity_at_utc'] == ops.iso(NOW) and row['completed_bytes'] == 100
    public = view(row)['tasks'][0]
    assert public['metrics'] == {'source_free_bytes': 100, 'source_reserve_bytes': 200}
    assert public['progress_stale'] is True and public['eta']['completion_at_utc'] is None
    assert public['complete_evidence'] is False


def test_queued_placement_preserves_nas_coverage_clock_and_has_a_distinct_wait(tmp_path):
    old = ops.iso(NOW - 3600)
    save(tmp_path / 'status.json', {'observed_at_utc': old, 'activity_at_utc': ops.iso(NOW),
        'combined_nas_coverage': {'contract': 'deduplicated_nas_file_coverage_v1',
            'available_bytes': 200, 'nas_covered_bytes': 100,
            'available_file_count': 2, 'nas_covered_file_count': 1},
        'catalog_refresh': {'contract': 'stockagent.backup_catalog_refresh.v1',
            'state': 'deferred_transport_placement', 'coverage_recomputed': False}})
    row = ops.backup_tasks(tmp_path)[0]
    assert row['reason'] == 'transport_placement_priority' and row['observed_at_utc'] == old
    assert row['activity_at_utc'] == ops.iso(NOW) and row['completed_bytes'] == 100
    public = view(row)['tasks'][0]
    assert public['progress_stale'] is True and public['eta']['completion_at_utc'] is None
    assert public['complete_evidence'] is False
    assert '安全交接' in public['reason'] and '舊 D 副本回收另計' in public['reason']


def test_private_readiness_clock_projection_never_publishes_unknown_fields(tmp_path):
    old = ops.iso(NOW-3600)
    save(tmp_path/'private-status.json', {
        'contract': 'stockagent.private_cold_NAS_enrollment.v1',
        'state': 'private_backup_waiting_receiver_readiness', 'observed_at_utc': old,
        'activity_at_utc': ops.iso(NOW), 'available_bytes': 200, 'nas_covered_bytes': 100,
        'receiver_readiness': {'state': 'expired', 'observed_at_utc': old,
            'valid_until_utc': ops.iso(NOW-2700), 'checked_at_utc': ops.iso(NOW),
            'private_secret': 'must-never-appear'}})
    row = ops.backup_tasks(tmp_path)[1]
    public = view(row)['tasks'][0]
    lease = public['receiver_readiness']
    assert lease['observed_at_utc'] == old and lease['valid_until_utc'] == ops.iso(NOW-2700)
    assert lease['checked_at_utc'] == ops.iso(NOW) and lease['state'] == 'expired'
    assert public['observed_at_utc'] == old and public['progress_stale'] is True
    assert 'must-never-appear' not in json.dumps(public)


@pytest.mark.parametrize('finished', [False, True])
def test_transport_retirement_is_its_own_phase_and_never_WSL_reclamation(tmp_path, finished):
    save(tmp_path/'transport-cache-progress.json', {
        'contract': 'stockagent.transport_cache_retirement_progress.v1',
        'scope': 'fixed_accepted_transport_candidates', 'generation_sha256': 'a'*64,
        'state': 'completed' if finished else 'running', 'observed_at_utc': ops.iso(NOW),
        'completed_items': 4 if finished else 2, 'total_items': 4,
        'completed_bytes': 200 if finished else 100, 'total_bytes': 200,
        'all_candidate_retirements_verified': finished,
        'source_objects_deleted': 0, 'NAS_snapshots_deleted': 0,
        'logical_transport_bytes_not_allocated_WSL_reclamation': True})
    row = next(r for r in ops.backup_tasks(tmp_path) if r['kind'] == 'transport_retirement')
    assert row['complete_evidence'] is finished
    result = view(row)
    assert result['tasks'][0]['progress']['percent'] == (100 if finished else 50)
    assert result['summary']['penguin_reclaimed_allocated_bytes'] == 0


def test_missing_transport_byte_total_cannot_claim_reclamation_completed(tmp_path):
    save(tmp_path/'transport-cache-progress.json', {
        'contract': 'stockagent.transport_cache_retirement_progress.v1',
        'scope': 'fixed_accepted_transport_candidates', 'generation_sha256': 'a'*64,
        'state': 'completed', 'observed_at_utc': ops.iso(NOW), 'completed_items': 1, 'total_items': 1,
        'all_candidate_retirements_verified': True,
        'source_objects_deleted': 0, 'NAS_snapshots_deleted': 0,
        'logical_transport_bytes_not_allocated_WSL_reclamation': True})
    row = next(r for r in ops.backup_tasks(tmp_path) if r['kind'] == 'transport_retirement')
    assert not row['complete_evidence'] and row['state'] == 'unknown'


def test_old_transport_owner_without_progress_receipt_shows_unknown_instead_of_zero(tmp_path):
    row = next(r for r in ops.backup_tasks(tmp_path) if r['kind'] == 'transport_retirement')
    assert row['state'] == 'unknown' and row['completed_bytes'] is None
    assert not row['complete_evidence'] and row['observed_at_utc'] is None
    assert view(row)['tasks'][0]['eta']['completion_at_utc'] is None


@pytest.mark.parametrize('reason', ['retirement_source_changed', 'retirement_process_references',
                                   '/private/error=password'])
def test_retirement_blocker_is_projected_as_a_safe_reason_without_exception_text(tmp_path, reason):
    save(tmp_path/'transport-cache-progress.json', {
        'contract': 'stockagent.transport_cache_retirement_progress.v1',
        'scope': 'fixed_accepted_transport_candidates', 'generation_sha256': 'a'*64,
        'state': 'blocked', 'observed_at_utc': ops.iso(NOW),
        'completed_items': 0, 'total_items': 2, 'completed_bytes': 0, 'total_bytes': 200,
        'source_objects_deleted': 0, 'NAS_snapshots_deleted': 0,
        'logical_transport_bytes_not_allocated_WSL_reclamation': True,
        'blocked_items': 2, 'reason_code': reason, 'detail': '/private/error=password'})
    row = next(r for r in ops.backup_tasks(tmp_path) if r['kind'] == 'transport_retirement')
    actual = view(row)['tasks'][0]
    assert actual['reason_code'] == (reason if reason in ops.REASONS else None)
    assert actual['metrics']['blocked_items'] == 2
    assert '/private/' not in json.dumps(view(row)) and 'password' not in json.dumps(view(row))
    assert actual['eta']['completion_at_utc'] is None and not actual['complete_evidence']


def test_lake_transport_retry_is_distinct_from_envelope_gc_and_retains_real_retry_time(tmp_path):
    save(tmp_path/'transport-gc-status.json', {
        'contract': 'stockagent.lake_transport_gc_status.v1', 'state': 'waiting_transport_retry',
        'observed_at_utc': ops.iso(NOW), 'next_retry_at_epoch': NOW+600,
        'reason_code': 'retirement_retry', 'retry_wait_count': 2, 'ready_candidate_count': 0,
        'retirement': {'detail': '/private/original'}})
    row = ops.lake_transport_tasks(tmp_path)[0]
    actual = view(row)['tasks'][0]
    assert row['kind'] == 'lake_transport_retirement' and row['state'] == 'waiting'
    assert actual['next_attempt_at_utc'] == ops.iso(NOW+600)
    assert actual['reason_code'] == 'retirement_retry' and actual['metrics']['retry_jobs'] == 2
    assert actual['progress']['percent'] is None and not actual['complete_evidence']
    assert '/private/' not in json.dumps(view(row))
    save(tmp_path/'transport-gc-status.json', {'contract': 'unbound', 'state': 'retired'})
    assert ops.lake_transport_tasks(tmp_path) == []


def test_minute_collection_reads_gc_from_its_own_root_and_preserves_each_original_owner(monkeypatch, tmp_path):
    gc_root = tmp_path/'lakehouse'
    save(gc_root/'transport-gc-status.json', {
        'contract': 'stockagent.lake_transport_gc_status.v1', 'state': 'waiting_transport_retry',
        'observed_at_utc': ops.iso(NOW-600), 'next_retry_at_epoch': NOW+600})
    envelope = ops.task('transport_retirement', 'envelope', observed=NOW-1000, state='blocked')
    monkeypatch.setattr(ops, 'spool_tasks', lambda root: [])
    monkeypatch.setattr(ops, 'backup_tasks', lambda root, retry_path=None: [dict(envelope)])
    monkeypatch.setattr(ops, 'migration_tasks', lambda root, **_kwargs: [])
    monkeypatch.setattr(ops, 'retirement_tasks', lambda *args: [])
    monkeypatch.setattr(ops, 'receiver_task', lambda root, **_: ops.task('receiver', 'missing'))
    monkeypatch.setattr(ops, 'schedules', lambda: [
        {'id': 'transport_gc', 'owner_state': 'activating'},
        {'id': 'registered_backup', 'owner_state': 'inactive'},
        {'id': 'private_backup', 'owner_state': 'inactive'}])
    raw, _ = ops.collect(tmp_path, {'operations': {'transport_gc_state_root': str(gc_root),
        'migration_state_root': str(tmp_path/'different-migration-root')}}, {}, now=NOW)
    rows = {row['kind']: row for row in raw['tasks']}
    assert rows['transport_retirement']['owner_state'] == 'inactive'
    assert rows['lake_transport_retirement']['owner_state'] == 'activating'
    assert rows['lake_transport_retirement']['observed_at_utc'] == ops.iso(NOW-600)
    assert rows['lake_transport_retirement']['checked_at_utc'] == ops.iso(NOW)


def test_private_activity_never_refreshes_old_coverage(tmp_path):
    private = {"contract": "stockagent.private_cold_NAS_enrollment.v1", "catalog_identity_sha256": "fixed",
        "available_bytes": 1000, "nas_covered_bytes": 200, "observed_at_utc": ops.iso(NOW-1000)}
    save(tmp_path/"status.json", {"private_history_backup": private})
    save(tmp_path/"private-status.json", {"contract": private["contract"], "catalog_identity_sha256": "fixed",
        "available_bytes": 1000, "state": "private_original_owner_acquired", "observed_at_utc": ops.iso(NOW)})
    row = ops.backup_tasks(tmp_path)[1]
    assert row["observed_at_utc"] == ops.iso(NOW-1000) and row["activity_at_utc"] == ops.iso(NOW)
    assert view(row)["tasks"][0]["stale"] is True
    save(tmp_path/"private-status.json", {"contract": private["contract"], "catalog_identity_sha256": "new",
        "available_bytes": 2000, "observed_at_utc": ops.iso(NOW)})
    assert ops.backup_tasks(tmp_path)[1]["completed_bytes"] is None


def test_private_coverage_does_not_regress_when_next_activity_overwrites_status(tmp_path):
    contract = "stockagent.private_cold_NAS_enrollment.v1"
    save(tmp_path/"status.json", {"private_history_backup": {"contract": contract,
        "catalog_identity_sha256": "fixed", "available_bytes": 1000, "nas_covered_bytes": 200,
        "observed_at_utc": ops.iso(NOW-1000)}})
    save(tmp_path/"private-status.json", {"contract": contract, "catalog_identity_sha256": "fixed",
        "state": "private_original_owner_acquired", "observed_at_utc": ops.iso(NOW)})
    retained = {"contract": "stockagent.private_cold_NAS_coverage.v1", "catalog_identity_sha256": "fixed",
        "available_bytes": 1000, "nas_covered_bytes": 400, "observed_at_utc": ops.iso(NOW-60)}
    save(tmp_path/"private-coverage.json", retained)
    row = ops.backup_tasks(tmp_path)[1]
    assert row["completed_bytes"] == 400 and row["observed_at_utc"] == ops.iso(NOW-60)
    assert row["activity_at_utc"] == ops.iso(NOW) and not row["complete_evidence"]
    retained["catalog_identity_sha256"] = "other"
    save(tmp_path/"private-coverage.json", retained)
    assert ops.backup_tasks(tmp_path)[1]["completed_bytes"] == 200


def test_empty_lab_queue_does_not_mean_full_nas_acceptance(tmp_path):
    save(tmp_path/"pipeline-status.json", {"contract": "parallel_nas_backup_relay_status_v1",
        "observed_at_utc": ops.iso(NOW), "jobs": []})
    row = ops.receiver_task(tmp_path)
    assert row["state"] == "waiting" and row["complete_evidence"] is False


def test_lab_cycle_results_do_not_masquerade_as_running_workers(tmp_path):
    save(tmp_path/"pipeline-status.json", {"contract": "parallel_nas_backup_relay_status_v1",
        "observed_at_utc": ops.iso(NOW), "jobs": [{"state": "queued"}] * 17 + [{"state": "accepted"}] * 4})
    row = ops.receiver_task(tmp_path)
    projected = view(row)["tasks"][0]
    assert projected["state"] == "waiting" and projected["complete_evidence"] is False
    assert projected["metrics"]["queued_jobs"] == 17
    assert projected["metrics"]["last_cycle_accepted_jobs"] == 4
    assert "active_jobs" not in projected["metrics"]


def test_lab_owner_readiness_and_lock_diagnostic_keep_separate_clocks(tmp_path, monkeypatch):
    from scripts.backup_delivery_receipt import READY, READY_FIELDS, signed
    pairing = {'producer_device_id': 'producer', 'receiver_device_id': 'receiver', 'repository_id': 'repository'}
    ready = signed({**{key: None for key in READY_FIELDS if key != 'identity_sha256'},
                    'contract': READY, **pairing, 'observed_at_utc': ops.iso(NOW-1000)})
    schedule = signed({'contract': 'stockagent.lab203_owner_schedule_observation.v1',
        'observer_node_id': 'lab203', 'pairing': pairing, 'observed_at_utc': ops.iso(NOW),
        'owners': [{'role': 'backup', 'installed': True, 'enabled': True, 'timer_active': True,
                    'currently_executing': True, 'systemd_active_state': 'activating',
                    'systemd_sub_state': 'start-pre'}],
        'original_readiness_reference': {'source_state': 'waiting_original_owner_recheck',
            'original_receipt_identity_sha256': ready['identity_sha256']}})
    diagnostic = signed({'contract': 'lab203.original_restic_lock_diagnostic_handoff.v1',
        'pairing': pairing, 'observed_at_utc': ops.iso(NOW-600), 'is_batch_acceptance_ACK': False,
        'diagnosis': {'readiness_identity_sha256': ready['identity_sha256'],
                      'restic_reported_exclusive_lock': True}, 'private': 'do not publish'})
    monkeypatch.setattr(ops, 'RECEIVER_LOCK_DIAGNOSTIC', diagnostic['identity_sha256'])
    path = tmp_path / f"lab203-restic-exclusive-lock-diagnostic-v1-{diagnostic['identity_sha256']}.json"
    save(path, diagnostic)
    save(tmp_path / 'readiness.json', ready)
    save(tmp_path / 'lab203-owner-schedule-v1.json', schedule)

    row = ops.receiver_task(tmp_path, now=NOW)
    public = view(row)['tasks'][0]
    assert public['state'] == 'blocked' and not public['complete_evidence']
    assert public['reason_code'] == 'receiver_readiness_lock_diagnosis'
    assert public['observed_at_utc'] == ops.iso(NOW)
    assert public['diagnosis_at_utc'] == ops.iso(NOW-600)
    assert public['continuation']['automatic'] is True
    assert public['continuation']['owner_running'] is False  # start-pre is not backup work.
    assert 'do not publish' not in json.dumps(public, ensure_ascii=False)

    # Re-reading the same schedule cannot renew its clock or assert a live lock.
    assert not ops.receiver_owner_diagnostic(tmp_path, now=NOW+ops.STALE_SECONDS+1)
    diagnostic['diagnosis']['restic_reported_exclusive_lock'] = False  # Invalid identity.
    save(path, diagnostic)
    assert ops.receiver_task(tmp_path, now=NOW)['reason'] == 'receiver_readiness'
    save(path, signed({k: v for k, v in diagnostic.items() if k != 'identity_sha256'}))
    updated = signed({**{k: v for k, v in ready.items() if k != 'identity_sha256'},
                      'observed_at_utc': ops.iso(NOW+1)})
    save(tmp_path / 'readiness.json', updated)
    assert not ops.receiver_owner_diagnostic(tmp_path, now=NOW+1).get('waiting')


def test_retirement_requires_exact_enrollment_and_original_acceptance(tmp_path):
    config = {"state_root": "queue", "cohorts": [{"id": "workflow-round12-20261031", "inventory_sha256": "fixed",
        "receipt_directory": "receipts"}]}
    save(tmp_path/"config.json", config)
    accepted = {"state": "accepted", "acceptance": {"inventory_sha256": "fixed", "items": 1, "reclaimed_allocated_bytes": 4096}}
    save(tmp_path/"queue/status.json", {"cohorts": {"workflow-round12-20261031": accepted}})
    save(tmp_path/"receipts/progress.json", {"inventory_sha256": "fixed", "all_selected_retired": True,
        "reclaimed_allocated_bytes": 4096, "checked_at_epoch": NOW-2000})
    row = ops.retirement_tasks(tmp_path, tmp_path/"config.json")[0]
    assert row["complete_evidence"] is True
    assert view(row)["summary"]["penguin_reclaimed_allocated_bytes"] == 4096
    accepted["acceptance"]["inventory_sha256"] = "different"
    save(tmp_path/"queue/status.json", {"cohorts": {"workflow-round12-20261031": accepted}})
    assert ops.retirement_tasks(tmp_path, tmp_path/"config.json")[0]["complete_evidence"] is False


@pytest.mark.parametrize("state,reason", [("blocked_identity_or_proof", "retirement_identity"),
    ("needs_attention", "owner_retry_limit")])
def test_retirement_proof_failure_is_a_current_blocker_not_old_running_work(tmp_path, state, reason):
    save(tmp_path/"config.json", {"state_root": "queue", "cohorts": [{"id": "wave", "inventory_sha256": "fixed",
        "receipt_directory": "receipts"}]})
    save(tmp_path/"receipts/progress.json", {"inventory_sha256": "fixed"})
    save(tmp_path/"receipts/dataset/phase.json", {"state": "completed", "phase": "prune-verified-encoding-stage",
        "started_at_epoch": NOW-4000})
    save(tmp_path/"queue/status.json", {"observed_at_utc": ops.iso(NOW), "cohorts": {"wave": {"state": state}}})
    row = view(ops.retirement_tasks(tmp_path, tmp_path/"config.json")[0])["tasks"][0]
    assert row["state"] == "blocked" and not row["stale"]
    assert row["phase"] == "retirement-proof-reconciliation"
    assert row["reason"] == ops.REASONS[reason] and not row["complete_evidence"]


@pytest.mark.parametrize('state', ['waiting_capacity', 'waiting_preflight', 'ready', 'enrolled'])
def test_enrolled_preflight_without_execution_receipt_is_a_real_wait(tmp_path, state):
    import hashlib
    enrolled = {'id': 'workflow-round15-20261101', 'inventory_sha256': 'fixed',
        'receipt_directory': 'receipts'}
    save(tmp_path/'config.json', {'state_root': 'queue', 'cohorts': [enrolled]})
    queue_row = {**enrolled, 'state': state,
        'enrollment_sha256': hashlib.sha256(json.dumps(enrolled, sort_keys=True).encode()).hexdigest(),
        'source_summary': {'allocated_unique_file_bytes_frozen': 4096, 'files_frozen': 3}}
    save(tmp_path/'queue/status.json', {'contract': 'stockagent.frozen_retirement_queue.v1',
        'observed_at_utc': ops.iso(NOW), 'cohorts': {enrolled['id']: queue_row}})
    result = view(ops.retirement_tasks(tmp_path, tmp_path/'config.json')[0])
    row = result['tasks'][0]
    assert row['state'] == 'waiting' and not row['stale'] and not row['complete_evidence']
    assert row['phase'] == ('capacity-preflight' if state == 'waiting_capacity' else 'waiting_preflight')
    assert row['metrics']['planned_reclaim_allocated_bytes'] == 4096
    assert row['progress']['completed_bytes'] is None and row['progress']['completed_items'] is None
    assert row['eta']['completion_at_utc'] is None
    assert result['summary']['penguin_reclaimed_allocated_bytes'] == 0


@pytest.mark.parametrize('defect', ['enrollment', 'inventory', 'target', 'contract'])
def test_preflight_observation_must_match_current_enrollment(tmp_path, defect):
    import hashlib
    enrolled = {'id': 'wave', 'inventory_sha256': 'fixed', 'receipt_directory': 'receipts'}
    save(tmp_path/'config.json', {'state_root': 'queue', 'cohorts': [enrolled]})
    row = {**enrolled, 'state': 'waiting_capacity',
        'enrollment_sha256': hashlib.sha256(json.dumps(enrolled, sort_keys=True).encode()).hexdigest()}
    if defect == 'enrollment': row['enrollment_sha256'] = 'other'
    elif defect == 'inventory': row['inventory_sha256'] = 'other'
    elif defect == 'target': row['receipt_directory'] = 'other'
    queue = {'contract': 'other' if defect == 'contract' else 'stockagent.frozen_retirement_queue.v1',
        'observed_at_utc': ops.iso(NOW), 'cohorts': {'wave': row}}
    save(tmp_path/'queue/status.json', queue)
    projected = view(ops.retirement_tasks(tmp_path, tmp_path/'config.json')[0])['tasks'][0]
    assert projected['state'] == 'unknown' and projected['metrics'] == {}


def test_old_completed_audit_not_reused_during_new_recovery(tmp_path):
    save(tmp_path/"config.json", {"state_root": "queue", "cohorts": [{"id": "wave", "inventory_sha256": "fixed", "receipt_directory": "receipts"}]})
    save(tmp_path/"receipts/progress.json", {"inventory_sha256": "fixed"})
    save(tmp_path/"receipts/dataset/phase.json", {"state": "running", "phase": "independent-D-original-recovery-for-quarantine", "started_at_epoch": NOW})
    save(tmp_path/"receipts/dataset/quarantine-audit-progress.json", {"checked_at_epoch": NOW-60,
        "state": "verified", "completed_items": 10, "total_items": 10})
    save(tmp_path/"receipts/dataset/recovery-progress.json", {"checked_at_epoch": NOW+60,
        "stage": "verify-materialized-files", "completed_items": 1, "total_items": 10})
    row = ops.retirement_tasks(tmp_path, tmp_path/"config.json")[0]
    assert row["completed_items"] == 1 and not row["complete_evidence"]


@pytest.mark.parametrize('stage', ['routing_and_hashing', 'packing_changed_small_files'])
def test_retirement_publication_uses_same_phase_counters_as_catalog_publication(tmp_path, stage):
    save(tmp_path/'config.json', {'state_root': 'queue', 'cohorts': [{
        'id': 'wave', 'inventory_sha256': 'fixed', 'receipt_directory': 'receipts'}]})
    save(tmp_path/'receipts/progress.json', {'inventory_sha256': 'fixed'})
    save(tmp_path/'receipts/dataset/phase.json', {'state': 'running',
        'phase': 'publish-under-shared-owner', 'started_at_epoch': NOW-10})
    publication = {'contract': 'manual-retirement-publication-progress-v1', 'stage': stage,
        'observed_at_epoch': NOW, 'acceptance': False, 'files_completed': 38,
        'files_routed': 25393, 'files_total': 30074,
        'logical_bytes_processed': 463660689, 'logical_bytes_total': 1003317197}
    save(tmp_path/'receipts/dataset/publication-progress.json', publication)
    row = ops.retirement_tasks(tmp_path, tmp_path/'config.json')[0]
    assert row['completed_items'] == (25393 if stage == 'routing_and_hashing' else 38)
    assert row['completed_bytes'] == 463660689 and row['total_bytes'] == 1003317197
    assert row['phase'] == stage and row['observed_at_utc'] == ops.iso(NOW)
    assert not row['complete_evidence']
    publication['observed_at_epoch'] = NOW-60
    save(tmp_path/'receipts/dataset/publication-progress.json', publication)
    row = ops.retirement_tasks(tmp_path, tmp_path/'config.json')[0]
    assert row['completed_items'] is None and row['completed_bytes'] is None


def stage_prune_work(tmp_path, **changes):
    save(tmp_path/"config.json", {"state_root": "queue", "cohorts": [{"id": "wave", "inventory_sha256": "fixed",
        "manifest_sha256": "cold-sha", "receipt_directory": "receipts"}]})
    save(tmp_path/"receipts/progress.json", {"inventory_sha256": "fixed"})
    save(tmp_path/"receipts/dataset/phase.json", {"state": "running", "phase": "apply-encoding-stage-prune",
        "started_at_epoch": NOW-60})
    save(tmp_path/"receipts/dataset/retirement.json", {"dataset": "dataset", "manifest_sha256": "cold-sha",
        "snapshot_id": "cold-snapshot", "deleted": True, "cold_primary_verified": True})
    observation = {"contract": "manual-stage-prune-progress-v1", "dataset": "dataset", "manifest_sha256": "cold-sha",
        "snapshot_id": "cold-snapshot", "acceptance_proof": False, "checked_at_epoch": NOW,
        "prune_step": "apply-verify-quarantine-archive", "stage": "scan-encoded-tree", "state": "running",
        "completed_items": 50, "total_items": 100, **changes}
    save(tmp_path/"receipts/dataset/stage-prune-progress.json", observation)
    return ops.retirement_tasks(tmp_path, tmp_path/"config.json")[0]


def test_stage_prune_counts_are_visible_without_claiming_retirement_acceptance(tmp_path):
    row = stage_prune_work(tmp_path)
    projected = view(row)["tasks"][0]
    assert projected["phase_label"] == "逐檔核對編碼暫存內容"
    assert projected["progress"]["percent"] == 50 and projected["state"] == "running"
    row = stage_prune_work(tmp_path, completed_items=100)
    row["eta"], _ = ops.estimate(row, {}, now=NOW)
    result = view(row)
    assert result["tasks"][0]["eta"]["state"] == "finalizing"
    assert not result["tasks"][0]["complete_evidence"]
    assert result["summary"]["penguin_reclaimed_allocated_bytes"] == 0


@pytest.mark.parametrize("changes", [
    {"manifest_sha256": "other-cold-version"}, {"snapshot_id": "other-snapshot"}, {"dataset": "other-dataset"},
    {"contract": "unbound-observation"}, {"acceptance_proof": True}, {"checked_at_epoch": NOW-120},
    {"prune_step": "unknown-step"}, {"prune_step": []}, {"prune_step": "plan-verify-archive"},
])
def test_stage_prune_rejects_foreign_or_old_observations(tmp_path, changes):
    row = stage_prune_work(tmp_path, **changes)
    assert row["completed_items"] is None and row["complete_evidence"] is False


def test_stage_prune_measurements_do_not_cross_substeps_or_item_scopes(tmp_path):
    row = stage_prune_work(tmp_path, prune_step="apply-reconstruct-cold")
    changed_step = stage_prune_work(tmp_path, prune_step="apply-reconstruct-expired-cold-proof")
    assert row["phase"] == changed_step["phase"]
    assert row["generation"] != changed_step["generation"]
    changed_scope = stage_prune_work(tmp_path, prune_step="apply-reconstruct-cold",
        item_scope="full-content-files", metadata_only=False)
    assert row["generation"] != changed_scope["generation"]
    failed = stage_prune_work(tmp_path, state="failed")
    assert view(failed)["tasks"][0]["eta"]["state"] == "blocked"


@pytest.mark.parametrize("phase", ["wait-for-retirement-owner", "wait-for-transport-convergence"])
def test_retirement_waiting_phase_is_not_displayed_as_active_hash_work(tmp_path, phase):
    save(tmp_path/"config.json", {"state_root": "queue", "cohorts": [{"id": "wave", "inventory_sha256": "fixed", "receipt_directory": "receipts"}]})
    save(tmp_path/"receipts/progress.json", {"inventory_sha256": "fixed"})
    save(tmp_path/"receipts/dataset/phase.json", {"state": "running", "phase": phase, "started_at_epoch": NOW})
    row = ops.retirement_tasks(tmp_path, tmp_path/"config.json")[0]
    assert row["state"] == "waiting" and row["phase"] == phase
    assert not row["complete_evidence"] and row["completed_bytes"] is None
    assert view(row)["tasks"][0]["phase_label"] == ops.PHASES[phase]


def test_migration_reads_bounded_real_progress_and_rejects_pointer_escape(tmp_path):
    operation = tmp_path/"operations"/("a"*20)
    save(tmp_path/"current.json", {"operation_directory": str(operation), "updated_at_utc": ops.iso(NOW)})
    save(operation/"progress.json", {"contract": "all_catalog_incremental_migration_v1", "source_count": 2,
        "padding": "x"*(ops.MAX_METADATA_BYTES+10), "datasets": {"one": {"state": "verified"},
        "two": {"state": "waiting_retry", "error": "absolute symlink is not snapshot-safe: /secret"}}})
    rows = ops.migration_tasks(tmp_path)
    assert rows[0]["completed_items"] == 1 and rows[0]["state"] == "blocked"
    assert rows[1]["reason"] == "source_link"
    save(tmp_path/"current.json", {"operation_directory": str(tmp_path/"outside")})
    assert ops.migration_tasks(tmp_path)[0]["state"] == "unknown"


@pytest.mark.parametrize(('error', 'error_type', 'expected'), [
    ('[Errno 5] Input/output error: /private/source', 'OSError', 'io_error'),
    ('[Errno 50] /private/source', 'OSError', None),
    ('[Errno 5] /private/source', 'OtherError', None),
])
def test_migration_reports_prior_io_error_separately_from_current_capacity(tmp_path, error, error_type, expected):
    operation = tmp_path / 'operations' / ('a' * 20)
    save(tmp_path / 'current.json', {'operation_directory': str(operation), 'updated_at_utc': ops.iso(NOW)})
    save(operation / 'progress.json', {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 1,
        'datasets': {'tw-index-futures': {'state': 'waiting_retry', 'error': error, 'error_type': error_type,
            'capacity_wait': {'contract': 'source_publication_capacity_v1'}}}})
    row = ops.migration_tasks(tmp_path)[1]
    assert row['reason'] == 'physical_capacity' and row.get('previous_source_failure') == expected
    public = view(row)['tasks'][0]
    assert (public['previous_source_failure'] or {}).get('code') == expected
    assert '/private/source' not in json.dumps(public)


@pytest.mark.parametrize(('available', 'observed', 'minimum', 'expected'), [
    (101, NOW, 100, 'capacity_recheck_due'),
    (99, NOW, 100, 'physical_capacity'),
    (101, NOW-1000, 100, 'physical_capacity'),
    (101, NOW, None, 'physical_capacity'),
])
def test_old_source_capacity_failure_needs_a_fresh_matching_measurement_for_recheck(available, observed, minimum, expected):
    row = ops.task('migration_dependency', 'example', state='blocked', reason='physical_capacity')
    row.update(_capacity_recheck_minimum=minimum, _capacity_recheck_observed=NOW-100)
    ops._apply_source_capacity_recheck([row], {'state': 'measured', 'available_bytes': available,
                                               'observed_at_utc': ops.iso(observed)}, now=NOW)
    assert row['reason'] == expected
    assert row['state'] == ('waiting' if expected == 'capacity_recheck_due' else 'blocked')
    if expected == 'capacity_recheck_due':
        assert row['phase'] == 'source_capacity_recheck'
    assert '_capacity_recheck_minimum' not in row
    assert not row['complete_evidence']


def test_migration_user_deferred_recovery_is_waiting_not_verified(tmp_path):
    operation = tmp_path / 'operations' / ('a' * 20)
    save(tmp_path / 'current.json', {'operation_directory': str(operation), 'updated_at_utc': ops.iso(NOW)})
    save(operation / 'progress.json', {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 2,
        'datasets': {'tw-index-futures': {'state': 'verified'},
                     'tw-futures': {'state': 'recovery_deferred_user_migration',
                                    'manifest_sha256': 'b' * 64}}})
    aggregate, deferred = ops.migration_tasks(tmp_path)
    assert aggregate['state'] == 'waiting' and aggregate['completed_items'] == 1
    assert aggregate['complete_evidence'] is False
    aggregate['checked_at_utc'] = ops.iso(NOW)
    public_aggregate = view(aggregate)['tasks'][0]
    assert public_aggregate['eta']['state'] == 'unknown'
    assert public_aggregate['eta']['reason'] == 'recovery_deferred_user_migration'
    assert public_aggregate['eta']['completion_at_utc'] is None
    assert deferred['state'] == 'waiting' and deferred['reason'] == 'recovery_deferred_user_migration'
    public = view(deferred)['tasks'][0]
    assert public['reason_code'] == 'recovery_deferred_user_migration'
    assert public['phase'] == 'recovery_deferred_user_migration'
    assert public['complete_evidence'] is False


def test_source_packaging_does_not_claim_independent_recovery_is_running(tmp_path):
    operation = tmp_path / 'operations' / ('a' * 20)
    save(tmp_path / 'current.json', {'operation_directory': str(operation), 'updated_at_utc': ops.iso(NOW)})
    value = {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 3,
        'runtime_owner': {'active_dataset': 'openbb'},
        'datasets': {'verified': {'state': 'verified'}, 'openbb': {'state': 'processing'},
                     'deferred': {'state': 'recovery_deferred_user_migration'}}}
    save(operation / 'progress.json', value)
    aggregate = ops.migration_tasks(tmp_path)[0]
    assert aggregate['state'] == 'waiting'
    assert aggregate['phase'] == 'recovery_deferred_user_migration'
    assert aggregate['reason'] == 'recovery_deferred_user_migration'
    assert aggregate['completed_items'] == 1 and not aggregate['complete_evidence']
    value['datasets']['openbb']['state'] = 'recovery'
    save(operation / 'progress.json', value)
    aggregate = ops.migration_tasks(tmp_path)[0]
    assert aggregate['state'] == 'running' and aggregate['phase'] == 'dataset_reconstruction'


def test_unenrolled_staged_source_is_visible_and_does_not_claim_automatic_cutover():
    name = 'tw-futures-prepared-e-primary-20261104-v1'
    stage = ops.task('workspace_source_staging', name, state='completed', observed=NOW,
                     done_bytes=1000, total_bytes=1000, done_items=10, total_items=10, proof=True)
    stage['subject'] = 'tw-futures-prepared'
    row, = ops.unenrolled_source_handoff_tasks([{'name': name}], set(), [stage])
    assert row['state'] == 'blocked' and not row['complete_evidence']
    assert row['total_bytes'] is None and row['total_items'] is None
    assert row['completed_bytes'] is None
    assert not ops.unenrolled_source_handoff_tasks([{'name': name}], {name}, [stage])
    stage['complete_evidence'] = False
    assert not ops.unenrolled_source_handoff_tasks([{'name': name}], set(), [stage])
    row['checked_at_utc'] = ops.iso(NOW)
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
        'tasks': [row], 'schedules': [{'id': 'workspace_source_handoff', 'registered': True,
            'mode': 'timer', 'timer_active': True, 'owner_running': False}]}, now=NOW)
    public, = result['tasks']
    assert public['reason_code'] == 'source_handoff_unenrolled'
    assert public['continuation']['automatic'] is False
    assert public['continuation']['next_check_at_utc'] is None
    assert public['next_observation_at_utc'] == ops.iso(NOW+60)


def test_unenrolled_source_reports_changed_root_without_claiming_child_completeness(tmp_path):
    from stockagent.data_sync.workspace_relocation import signature
    source = tmp_path/'source'
    source.mkdir()
    original = signature(source)
    assert ops.staged_source_root_changed(source, original) is False
    rebound = list(original)
    rebound[0] += 1  # WSL device renumbering alone is not a source change.
    assert ops.staged_source_root_changed(source, rebound) is False
    (source/'new-member').write_text('new')
    assert ops.staged_source_root_changed(source, original) is True
    assert ops.staged_source_root_changed(source, None) is None
    link = tmp_path/'alias'
    link.symlink_to(source, target_is_directory=True)
    assert ops.staged_source_root_changed(link, original) is None
    name = 'tw-minute-source-e-primary-20261104-v1'
    stage = ops.task('workspace_source_staging', name, state='completed', observed=NOW,
                     done_bytes=1000, total_bytes=1000, done_items=10, total_items=10, proof=True)
    stage.update(subject='tw-minute-source', _staged_source_root_changed=True)
    row, = ops.unenrolled_source_handoff_tasks([{'name': name}], set(), [stage])
    assert row['state'] == 'blocked' and row['reason'] == 'source_handoff_root_changed'
    row['checked_at_utc'] = ops.iso(NOW)
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
        'tasks': [row], 'schedules': [{'id': 'workspace_source_handoff', 'registered': True,
          'mode': 'timer', 'timer_active': True, 'owner_running': False}]}, now=NOW)
    public, = result['tasks']
    assert public['reason_code'] == 'source_handoff_root_changed'
    assert public['continuation']['automatic'] is False


def test_older_minute_copy_stops_looking_like_a_second_blocked_handoff_only_after_new_enrollment():
    old, new = 'tw-minute-source-e-primary-20261104-v1', 'tw-minute-source-stream-e-20261108-v1'
    entry = {'name': old, 'source': 'data_tw_minute/shioaji_1m'}
    old_stage = ops.task('workspace_source_staging', old, state='completed', proof=True)
    old_stage.update(subject='tw-minute-source', _staged_source_root_changed=True)
    new_stage = ops.task('workspace_source_staging', new, state='completed', proof=True)
    new_stage['subject'] = 'tw-minute-source'
    assert ops.unenrolled_source_handoff_tasks([entry], set(), [old_stage, new_stage])
    assert ops.unenrolled_source_handoff_tasks([entry], {new}, [old_stage])
    assert not ops.unenrolled_source_handoff_tasks([entry], {new}, [old_stage, new_stage])


def test_options_new_copy_supersedes_only_the_old_handoff_candidate():
    old = 'tw-index-options-daily-e-primary-20261104-v1'
    new = 'tw-index-options-daily-stream-e-20261108-v1'
    entries = [{'name': old, 'source': 'data_tw_index_options_daily'},
               {'name': new, 'source': 'data_tw_index_options_daily'}]
    rows = []
    for name in (old, new):
        stage = ops.task('workspace_source_staging', name, state='completed', observed=NOW, proof=True)
        stage['subject'] = 'tw-index-options-daily'
        if name == old:
            stage['_staged_source_root_changed'] = True
        rows.append(stage)
    result = ops.unenrolled_source_handoff_tasks(entries, set(), rows)
    assert len(result) == 1 and result[0]['reason'] == 'source_handoff_unenrolled'
    assert result[0]['id'] == ops.key(('workspace_source_handoff', new))
    rows[1]['_staged_source_root_changed'] = True
    result = ops.unenrolled_source_handoff_tasks(entries, set(), rows)
    assert len(result) == 1 and result[0]['reason'] == 'source_handoff_root_changed'


def test_offline_stream_observation_counts_bound_inflight_inodes_only(tmp_path):
    evidence, destination = tmp_path/'evidence', tmp_path/'destination'
    evidence.mkdir(); destination.mkdir()
    partial_name = '.source-stream-'+'a'*24+'.partial'
    final_temp = '.source-stream-'+'b'*24+'.partial'
    partial = destination/partial_name; partial.write_bytes(b'abc')
    final = destination/'complete'; final.write_bytes(b'defg')
    with sqlite3.connect(evidence/'files.sqlite') as db:
        db.execute('CREATE TABLE files(path TEXT, bytes INTEGER, state TEXT)')
        db.execute('CREATE TABLE stream_files(path TEXT, temp_name TEXT, device INTEGER, inode INTEGER, state TEXT)')
        for path, length, temporary, file in [('part', 10, partial_name, partial),
                                               ('complete', 4, final_temp, final)]:
            info = file.stat()
            db.execute('INSERT INTO files VALUES (?,?,?)', (path, length, 'pending'))
            db.execute('INSERT INTO stream_files VALUES (?,?,?,?,?)',
                       (path, temporary, info.st_dev, info.st_ino, 'copying'))
    assert ops.offline_stream_inflight_bytes(evidence, destination) == 7
    final.unlink()
    assert ops.offline_stream_inflight_bytes(evidence, destination) is None
    with sqlite3.connect(evidence/'files.sqlite') as db:
        db.execute("UPDATE stream_files SET path='../outside' WHERE path='part'")
        db.execute("UPDATE files SET path='../outside' WHERE path='part'")
    assert ops.offline_stream_inflight_bytes(evidence, destination) is None


def test_vast_sealed_copy_status_separates_copy_from_handoff(tmp_path):
    from scripts.run_dedup_candidate_queue import process_identity
    status = tmp_path/'migration/vast-pre-stop-sealed-e-v1/status.json'
    value = {'contract': 'stockagent.vast_sealed_e_handoff.v1', 'state': 'copying',
             'observed_at_epoch': NOW, 'owner_identity': process_identity(),
             'source_deleted': False, 'destination_readback': False, 'mount_handoff_completed': False,
             'progress': {'files_completed': 1, 'files_total': 2, 'bytes_completed': 10, 'bytes_total': 30}}
    save(status, value)
    row, = ops.vast_sealed_e_handoff_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'vast_sealed_copy'
    assert (row['completed_bytes'], row['total_bytes']) == (10, 30)
    value.update(state='copied_waiting_handoff', files_completed=2, bytes_completed=30)
    save(status, value)
    row, = ops.vast_sealed_e_handoff_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['reason'] == 'vast_sealed_handoff_pending'
    assert not row['complete_evidence']
    public = view(row)['tasks'][0]
    assert public['reason_code'] == 'vast_sealed_handoff_pending'
    assert public['progress']['percent'] == 100
    value.update(state='copying', owner_identity={'pid': 999999999, 'start_ticks': 1, 'boot_id': 'old'})
    save(status, value)
    row, = ops.vast_sealed_e_handoff_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'vast_sealed_copy_retry'
    value.update(state='completed', source_deleted=True, mount_handoff_completed=True)
    save(status, value)
    row, = ops.vast_sealed_e_handoff_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'completed' and row['complete_evidence']
    assert row['phase'] == 'vast_sealed_completed'


def test_migration_public_view_rejects_malformed_prior_error_value():
    row = ops.task('migration_dependency', 'tw-index-futures', state='blocked',
                   phase='source_dependency', observed=ops.iso(NOW), reason='physical_capacity')
    row['previous_source_failure'] = ['/private/source']
    public = view(row)['tasks'][0]
    assert public['previous_source_failure'] is None
    assert '/private/source' not in json.dumps(public)


@pytest.mark.parametrize(('error', 'expected'), [
    ('source file changed while packing: /private/source', 'source_changed'),
    ('E workspace is not its enrolled shared ext4 mount', 'source_workspace_mount'),
])
def test_migration_wait_reports_prior_change_without_exposing_source_path(tmp_path, error, expected):
    operation = tmp_path / 'operations' / ('a' * 20)
    save(tmp_path / 'current.json', {'operation_directory': str(operation), 'updated_at_utc': ops.iso(NOW)})
    save(operation / 'progress.json', {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 1,
        'datasets': {'local-tw-index-derivatives-ticks': {'state': 'waiting_retry', 'error': error}}})
    row = ops.migration_tasks(tmp_path)[1]
    assert row['reason'] == expected and row['subject'] == 'local-tw-index-derivatives-ticks'
    public, = view(row)['tasks']
    assert public['reason_code'] == expected and '/private/source' not in json.dumps(public)


def test_catalog_export_observation_keeps_failure_and_live_attempt_separate(tmp_path):
    path = tmp_path / 'catalog-observation.json'
    receipt = {'contract': 'stockagent.catalog_export_observation.v1', 'read_only': True,
        'observed_at_utc': ops.iso(NOW), 'phase': 'exporting', 'snapshot_id': 1061, 'workflow_run_id': 'run-1',
        'activity': {'id': '30', 'state': 'started', 'attempt': 32,
                     'failure': 'native_command_rejected', 'failure_at_utc': ops.iso(NOW - 120),
                     'started_at_utc': ops.iso(NOW - 60), 'detail': '/private/path'}}
    save(path, receipt)
    row, = ops.catalog_export_tasks(path)
    assert row['state'] == 'running' and not row['complete_evidence']
    public = view(row)
    item, = public['tasks']
    assert item['metrics']['attempt_number'] == 32
    assert item['previous_source_failure']['code'] == 'native_command_rejected'
    assert ops.timestamp(item['failure_observed_at_utc']) == NOW - 120
    assert item['eta']['completion_at_utc'] is None and '/private/path' not in json.dumps(public)
    assert any(entry['id'] == item['id'] + '-previous-failure' for entry in public['attention'])
    progress_path = tmp_path / 'catalog-progress.json'
    progress = {'contract': 'stockagent.catalog_export_progress.v1', 'workflow_run_id': 'run-1',
        'snapshot_id': 1061, 'activity_id': '30', 'attempt': 32, 'observed_at_utc': ops.iso(NOW - 5),
        'phase': 'catalog_copy', 'completed_items': 40, 'total_items': 100}
    save(progress_path, progress)
    bound, = ops.catalog_export_tasks(path, progress_path)
    assert bound['phase'] == 'catalog_copy' and bound['completed_items'] == 40
    assert ops.timestamp(bound['observed_at_utc']) == NOW - 5
    for key, mismatch in (('workflow_run_id', 'run-2'), ('snapshot_id', 1062), ('activity_id', '31'), ('attempt', 31)):
        changed = {**progress, key: mismatch}
        save(progress_path, changed)
        unbound, = ops.catalog_export_tasks(path, progress_path)
        assert unbound['phase'] == 'catalog_export' and unbound['completed_items'] is None
    receipt['activity'] = None
    save(path, receipt)
    assert ops.catalog_export_tasks(path) == []  # No pending job is not acceptance.


@pytest.mark.parametrize('nodes,accepted,retained_only', [
    ({'local': 3}, True, False),  # Remote startup failed; exact local fallback finished.
    ({'vast': 3}, True, False),
    ({'local': 1, 'vast': 2}, True, False),
    ({'retained': 3}, True, True),
    ({'local': 1, 'retained': 2}, True, False),
    ({}, False, False),
    ({'local': 2}, False, False),
    ({'local': 3, 'vast': 1}, False, False),
    ({'other': 3}, False, False),
    ({'local': 3, 'vast': None}, False, False),
    ({'local': True, 'vast': 2}, False, False),
    ({'local': '3'}, False, False),
    ({'local': 3.0}, False, False),
    ({'local': -1, 'vast': 4}, False, False),
])
@pytest.mark.parametrize('full_reconstruction,manifest', [(True, 'fixed'), (False, 'fixed'), (True, 'other')])
def test_cooperative_projection_requires_complete_fixed_recovery_and_closed_lane_counts(
        tmp_path, nodes, accepted, retained_only, full_reconstruction, manifest):
    operation = tmp_path/'operations'/('a'*20)
    save(tmp_path/'current.json', {'operation_directory': str(operation)})
    save(operation/'progress.json', {'contract': 'all_catalog_incremental_migration_v1',
        'source_count': 1, 'datasets': {'local-tw-futures': {
            'state': 'verified', 'manifest_sha256': 'fixed', 'verified_at_utc': ops.iso(NOW)}}})
    save(operation/'recovery/local-tw-futures.json', {
        'full_original_file_reconstruction_verified': full_reconstruction,
        'manifest_sha256': manifest,
        'pack_reconstruction_execution': {'contract': 'exact_pack_reconstruction_lanes_v1',
            'jobs': 3, 'nodes': nodes, 'remote_initialization_failure': 'TimeoutError'}})
    row = ops.migration_tasks(tmp_path)[1]
    proof = accepted and full_reconstruction and manifest == 'fixed'
    assert row['state'] == ('completed' if proof else 'unknown')
    assert row['complete_evidence'] is proof
    assert row['completed_items'] == (3 if proof else None)
    assert row['kind'] == ('retained_verification' if retained_only else 'cooperative')
    assert row['metrics']['vast_packs'] == nodes.get('vast', 0) or not accepted
    assert 'TimeoutError' not in json.dumps(view(row))


@pytest.mark.parametrize(('error_type', 'message', 'reason'), [
    ('SourceCoordinationLockTimeout', 'timed out on private lock', 'source_coordination_previous'),
    ('SnapshotError', 'native E workspace operation failed: private details', 'source_native_bridge_previous'),
    ('SnapshotError', 'cannot resolve exact packed pin: private path', 'source_exact_pin_missing'),
    ('SnapshotError', 'published mutable source changed during capture: private path', 'source_changed'),
])
def test_migration_dependency_explains_previous_failure_without_exposing_paths(tmp_path, error_type, message, reason):
    operation = tmp_path/'operations'/('a'*20)
    save(tmp_path/'current.json', {'operation_directory': str(operation), 'updated_at_utc': ops.iso(NOW)})
    save(operation/'progress.json', {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 1,
        'datasets': {'local-openbb': {'state': 'waiting_retry', 'attempts': 1,
                                      'error_type': error_type, 'error': message}}})
    row = next(row for row in ops.migration_tasks(tmp_path) if row['kind'] == 'migration_dependency')
    assert row['reason'] == reason
    public = view(row)['tasks'][0]
    assert public['reason_code'] == reason and 'private' not in json.dumps(public)


def test_fred_repaired_pin_waits_for_retry_only_when_bound_to_failed_attempt(tmp_path):
    state, repo, workspace = tmp_path/'state', tmp_path/'repo', tmp_path/'workspace'
    operation = state/'operations'/('a'*20)
    snapshot, digest = 'fred-original', 'a'*64
    save(state/'current.json', {'operation_directory': str(operation)})
    save(operation/'progress.json', {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 1,
        'datasets': {'fred-crypto-macro': {'state': 'waiting_retry', 'started_at_utc': ops.iso(NOW-20),
            'error': 'cannot resolve exact packed pin: /private/'+snapshot+'.json'}}})
    marker = repo/'data_fred_crypto_macro/state/retained-raw.json'
    binding = {'contract': 'bounded_raw_history_v1', 'dataset': 'fred-crypto-macro',
               'manifest_sha256': digest, 'snapshot_id': snapshot}
    save(marker, binding)
    repair_path = workspace/'migration/fred-exact-pin-recovery-v1/status.json'
    repair = {'contract': 'stockagent.fred_exact_pin_recovery.v1', 'dataset': 'fred-crypto-macro',
        'state': 'exact_pin_restored', 'original_manifest_sha256': digest, 'original_snapshot_id': snapshot,
        'expected_objects': 65, 'installed_objects': 65, 'exact_pin_resolved': True,
        'current_head_changed': False, 'source_deleted': False, 'observed_at_epoch': NOW-10}
    save(repair_path, repair)
    def dependency():
        return next(row for row in ops.migration_tasks(state, repo=repo, recovery_workspace=workspace, now=NOW)
                    if row['kind'] == 'migration_dependency')
    assert dependency()['state'] == 'waiting'
    assert dependency()['reason'] == 'source_exact_pin_repaired_retry'
    binding['manifest_sha256'] = 'b'*64
    save(marker, binding)
    assert dependency()['reason'] == 'source_exact_pin_missing'
    binding['manifest_sha256'] = digest
    save(marker, binding)
    repair['observed_at_epoch'] = NOW-30
    save(repair_path, repair)
    assert dependency()['reason'] == 'source_exact_pin_missing'


def test_refresh_eta_yields_at_actual_trading_priority_boundary(tmp_path, monkeypatch):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from scripts import check_outside_tw_opening_resource_window as gate
    tz = ZoneInfo('Asia/Taipei')
    monkeypatch.setattr(gate, 'evaluate', lambda day, **_: {'protected': day.weekday() < 5})
    saturday = datetime(2026, 11, 7, 12, 0, tzinfo=tz).timestamp()
    monday = datetime(2026, 11, 9, 6, 20, tzinfo=tz).timestamp()
    assert ops.next_refresh_priority_boundary(saturday, tmp_path) == monday
    def row(finish, guarded=True):
        return {'_whole_root_refresh_yields': guarded, 'eta': {'state': 'estimated',
            'completion_at_utc': ops.iso(finish), 'remaining_seconds': 100,
            'fastest_seconds': 80, 'slowest_seconds': 120, 'rate_per_second': 2}}
    before = row(monday-1)
    ops.apply_refresh_priority_boundary(before, now=saturday, repo=tmp_path)
    assert before['eta']['state'] == 'estimated'
    crossing = row(monday+1)
    ops.apply_refresh_priority_boundary(crossing, now=saturday, repo=tmp_path)
    assert crossing['eta']['state'] == 'unknown'
    assert crossing['eta']['reason'] == 'refresh_priority_boundary'
    assert crossing['eta']['completion_at_utc'] is None
    unguarded = row(monday+1, guarded=False)
    ops.apply_refresh_priority_boundary(unguarded, now=saturday, repo=tmp_path)
    assert unguarded['eta']['state'] == 'estimated'


def test_pending_futures_dependency_explains_wait_without_inventing_a_failure(tmp_path):
    operation = tmp_path / 'operations' / ('a' * 20)
    save(tmp_path / 'current.json', {'operation_directory': str(operation), 'updated_at_utc': ops.iso(NOW)})
    save(operation / 'progress.json', {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 2,
        'datasets': {'local-tw-index-futures': {'state': 'waiting_retry', 'error': 'private error'},
                     'local-tw-futures': {'state': 'waiting_retry', 'attempts': 0,
                         'dependency_wait': {'dataset': 'local-tw-index-futures', 'state': 'waiting_retry'}}}})
    rows = ops.migration_tasks(tmp_path)
    dependent = next(row for row in rows if row.get('subject') == 'local-tw-futures')
    assert dependent['reason'] == 'source_predecessor' and dependent['state'] == 'blocked'
    assert 'error' not in dependent and 'dependency_wait' not in dependent


def test_daily_refresh_wait_reports_trading_gate_and_keeps_original_failure(tmp_path, monkeypatch):
    operation = tmp_path/'operations'/('a'*20)
    save(tmp_path/'current.json', {'operation_directory': str(operation)})
    save(operation/'progress.json', {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 1,
        'whole_root_refresh': {'contract': 'stockagent.whole_root_refresh_queue.v1', 'window_allowed': False},
        'datasets': {'local-openbb': {'state': 'waiting_retry', 'error_type': 'FileNotFoundError',
            'error': "missing /private/manifests/local-openbb-state-abcdefabcdef/fixed.json",
            'freshness_refresh': {'contract': 'stockagent.whole_root_refresh.v1'},
            'retry_at_unix': NOW-100}}})
    monkeypatch.setattr(ops, '_storage_priority_window_end', lambda _: NOW+100)
    def row():
        return next(r for r in ops.migration_tasks(tmp_path, now=NOW) if r['kind'] == 'migration_dependency')
    deferred = row()
    assert deferred['state'] == 'waiting' and deferred['phase'] == 'source_refresh_trading_wait'
    assert deferred['reason'] == 'source_sqlite_binding_previous'
    assert deferred['next_attempt_at_utc'] == ops.iso(NOW+100)
    eta, _ = ops.estimate(deferred, {}, now=NOW)
    assert eta['reason'] == 'tw_day_trade_priority_window' and eta['completion_at_utc'] is None
    monkeypatch.setattr(ops, '_storage_priority_window_end', lambda _: None)
    assert row()['state'] == 'blocked' and row()['reason'] == 'source_sqlite_binding_previous'


def test_repaired_sqlite_reference_is_waiting_only_for_its_unchanged_attempt_and_preparation(tmp_path):
    operation = tmp_path/'operations'/('a'*20)
    save(tmp_path/'current.json', {'operation_directory': str(operation)})
    repair = {'contract': 'stockagent.migration_dependency_reference_repair.v1', 'dataset': 'local-openbb',
        'attempt_id': 'a'*32, 'state': 'references_reconciled', 'source_publication_complete': False,
        'source_payload_read': False, 'original_failure_preserved': True,
        'fixed_bindings': {'local-openbb-state-abcdefabcdef': 'b'*64}}
    data = {'state': 'waiting_retry', 'attempt_id': 'a'*32, 'dependency_reference_repair': repair,
        'error_type': 'FileNotFoundError', 'error': '/private/manifests/local-openbb-state-x/old.json'}
    progress = {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 1,
                'datasets': {'local-openbb': data}}
    save(operation/'progress.json', progress)
    prep_path = operation/'preparations/local-openbb.json'
    prep = {'contract': progress['contract'], 'dataset': 'local-openbb', 'sqlite_states': [
        {'dataset': 'local-openbb-state-abcdefabcdef', 'manifest_sha256': 'b'*64}]}
    save(prep_path, prep)
    def row():
        return next(r for r in ops.migration_tasks(tmp_path, now=NOW) if r['kind'] == 'migration_dependency')
    assert row()['state'] == 'waiting' and row()['reason'] == 'source_dependencies_repaired_retry'
    data['attempt_id'] = 'c'*32
    save(operation/'progress.json', progress)
    assert row()['state'] == 'blocked'
    data['attempt_id'] = 'a'*32
    save(operation/'progress.json', progress)
    prep['sqlite_states'][0]['manifest_sha256'] = 'c'*64
    save(prep_path, prep)
    assert row()['state'] == 'blocked'


def test_publication_owner_busy_is_a_retry_not_a_permanent_block(tmp_path):
    operation = tmp_path/'operations'/('a'*20)
    save(tmp_path/'current.json', {'operation_directory': str(operation)})
    save(operation/'progress.json', {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 1,
        'datasets': {'tw-minute-train': {'state': 'waiting_retry', 'attempt_id': 'current',
            'error_type': 'PackedPublicationBusy', 'error': 'publication owner busy: private-lock-name',
            'retry_at_unix': NOW-10}}})
    row = next(row for row in ops.migration_tasks(tmp_path) if row['kind'] == 'migration_dependency')
    assert row['state'] == 'waiting' and row['reason'] == 'source_publication_owner_busy'
    assert row['next_attempt_at_utc'] == ops.iso(NOW-10)
    assert 'private-lock-name' not in json.dumps(view(row))


@pytest.mark.parametrize('phase', ['censusing_source', 'routing_and_hashing', 'packing_changed_small_files', 'committing_complete_inventory'])
def test_current_publication_explains_source_work_without_claiming_reconstruction(tmp_path, phase):
    operation = tmp_path / 'operations' / ('a' * 20)
    save(tmp_path / 'current.json', {'operation_directory': str(operation), 'updated_at_utc': ops.iso(NOW)})
    progress = {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 2,
        'runtime_owner': {'active_dataset': 'local-tw-index-futures'},
        'datasets': {'one': {'state': 'verified'}, 'local-tw-index-futures': {'state': 'processing', 'attempt_id': 'current'}}}
    save(operation / 'progress.json', progress)
    publication = {'attempt_id': 'current', 'stage': phase, 'observed_at_utc': ops.iso(NOW),
        'files_completed': 230, 'files_total': None if phase == 'censusing_source' else 300,
        'logical_bytes_processed': 1000, 'logical_bytes_total': None if phase == 'censusing_source' else 2000}
    if phase == 'routing_and_hashing':
        publication['files_routed'] = 260
    if phase in {'committing_complete_inventory', 'packing_changed_small_files'}:
        publication.update(phase_completed_items=9, phase_total_items=10)
    save(operation / 'publication/local-tw-index-futures.json', publication)
    rows = ops.migration_tasks(tmp_path)
    assert rows[0]['completed_items'] == 1
    row = next(row for row in rows if row['kind'] == 'migration_publication')
    assert row['phase'] == phase and row['complete_evidence'] is False
    assert row['metrics']['processed_source_files'] == 230
    assert row['metrics']['processed_source_bytes'] == 1000
    if phase == 'committing_complete_inventory':
        assert row['completed_items'] == 9 and row['total_items'] == 10
        assert row['completed_bytes'] is None
    else:
        assert row['completed_items'] == (260 if phase == 'routing_and_hashing' else 230)
        assert row['completed_bytes'] == 1000
    if phase == 'packing_changed_small_files':
        publication.update(phase_completed_items=10, phase_total_items=10,
                           files_hashed=240, files_routed=300)
        save(operation / 'publication/local-tw-index-futures.json', publication)
        active = next(r for r in ops.migration_tasks(tmp_path) if r['kind'] == 'migration_publication')
        public = view(active)['tasks'][0]
        assert active['completed_items'] == 240 and active['total_items'] == 300
        assert public['progress']['percent'] < 100
        assert not public['complete_evidence']
    if phase == 'routing_and_hashing':
        publication.pop('files_routed')
        save(operation / 'publication/local-tw-index-futures.json', publication)
        legacy = next(r for r in ops.migration_tasks(tmp_path) if r['kind'] == 'migration_publication')
        assert legacy['completed_items'] is None  # Hash count excludes deferred small files.
        assert legacy['completed_bytes'] == 1000
    if phase == 'censusing_source':
        eta, _ = ops.estimate(row, {}, now=NOW)
        assert eta['state'] == 'unknown' and eta['reason'] == 'no_counter'
    # Prior-attempt counters and a committed release's old publication are
    # never shown as current source work or added to recovery acceptance.
    publication['attempt_id'] = 'old'
    save(operation / 'publication/local-tw-index-futures.json', publication)
    assert not any(row['kind'] == 'migration_publication' for row in ops.migration_tasks(tmp_path))
    publication['attempt_id'] = 'current'
    save(operation / 'publication/local-tw-index-futures.json', publication)
    progress['datasets']['local-tw-index-futures']['snapshot_id'] = 'fixed'
    save(operation / 'progress.json', progress)
    assert not any(row['kind'] == 'migration_publication' for row in ops.migration_tasks(tmp_path))


def test_live_minute_cache_observation_requires_exact_manifest_and_generation(tmp_path, monkeypatch):
    import os
    import sqlite3

    dependencies = ('scripts/audit_shioaji_tw_minute_dataset.py',
                    'stockagent/data/tw_price_rules.py', 'downloader/stock_volume_units.py',
                    'scripts/build_shioaji_tw_minute_dataset.py',
                    'stockagent/data/tw_exchange_price_classification.py',
                    'stockagent/data_sync/source_validation.py')
    hashes = []
    for name in dependencies:
        path = tmp_path/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(name)
        hashes.append(hashlib.sha256(path.read_bytes()).hexdigest())
    contract = hashlib.sha256(''.join(hashes).encode()).hexdigest()
    root = tmp_path/'data_tw_minute/research_dataset'
    parts = []
    for day in ('2020-03-02', '2020-03-03'):
        relative = f'trade_date={day}/data.parquet'
        path = root/relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(day.encode())
        parts.append({'trade_date': day, 'output': relative, 'output_sha256': 'a'*64})
    save(root/'manifest.json', {'partitions': parts})
    cache = tmp_path/'artifacts/operations/source_validation/tw-minute-train/files.sqlite3'
    cache.parent.mkdir(parents=True)
    db = sqlite3.connect(cache)
    db.execute('CREATE TABLE verified (path TEXT PRIMARY KEY, signature TEXT, contract TEXT, sha256 TEXT, result TEXT)')
    for i, part in enumerate(parts):
        path = root/part['output']
        st = path.stat()
        sig = [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns]
        db.execute('INSERT INTO verified VALUES (?,?,?,?,?)',
                   (str(path), json.dumps(sig), contract, 'a'*64 if i == 0 else 'b'*64, '{}'))
    db.commit()
    db.close()
    fields = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()
    auditor = {'pid': os.getpid(), 'start_ticks': int(fields[19]), 'started_at_epoch': time.time()+1}
    monkeypatch.setattr(ops, '_live_tw_minute_auditor', lambda _repo, _owner: auditor)
    monkeypatch.setattr(ops, '_process_identity_matches', lambda _owner: True)
    result = ops._tw_minute_validation_progress(tmp_path, {}, now=NOW)
    assert (result['completed'], result['total']) == (1, 2)
    # An unchanged file name with a changed generation is not a cache hit.
    (root/parts[0]['output']).write_bytes(b'changed')
    assert ops._tw_minute_validation_progress(tmp_path, {}, now=NOW)['completed'] == 0
    # A validator changed after the live child started cannot borrow old rows.
    os.utime(tmp_path/dependencies[0], (time.time()+10, time.time()+10))
    assert ops._tw_minute_validation_progress(tmp_path, {}, now=NOW) is None


def test_minute_validation_counter_is_attempt_scoped_not_a_completion(tmp_path, monkeypatch):
    operation = tmp_path/'operations'/('a'*20)
    save(tmp_path/'current.json', {'operation_directory': str(operation)})
    save(operation/'progress.json', {'contract': 'all_catalog_incremental_migration_v1', 'source_count': 1,
        'runtime_owner': {'active_dataset': 'tw-minute-train'},
        'datasets': {'tw-minute-train': {'state': 'processing', 'attempt_id': 'current'}}})
    publication = {'attempt_id': 'current', 'stage': 'preparing_source', 'observed_at_utc': ops.iso(NOW)}
    save(operation/'publication/tw-minute-train.json', publication)
    monkeypatch.setattr(ops, '_tw_minute_validation_progress', lambda *_a, **_k: {
        'completed': 9, 'total': 10, 'observed_at_epoch': NOW, 'manifest_sha256': 'a'*64,
        'validator_sha256': 'b'*64, 'auditor_start_ticks': 123})
    row = next(row for row in ops.migration_tasks(tmp_path, repo=tmp_path, now=NOW)
               if row['kind'] == 'migration_publication')
    assert row['phase'] == 'source_semantic_validation'
    assert (row['completed_items'], row['total_items']) == (9, 10)
    assert row['complete_evidence'] is False
    publication['attempt_id'] = 'different'
    save(operation/'publication/tw-minute-train.json', publication)
    assert not any(row['kind'] == 'migration_publication'
                   for row in ops.migration_tasks(tmp_path, repo=tmp_path, now=NOW))


@pytest.mark.parametrize('defect', ['missing_attempt', 'list_stage', 'dict_stage'])
def test_unbound_or_malformed_publication_never_crashes_or_invents_source_progress(tmp_path, defect):
    operation = tmp_path / 'operations' / ('a' * 20)
    save(tmp_path / 'current.json', {'operation_directory': str(operation)})
    row = {'state': 'processing', 'attempt_id': 'current'}
    publication = {'attempt_id': 'current', 'stage': 'censusing_source', 'files_completed': 100}
    if defect == 'missing_attempt':
        row.pop('attempt_id')
        publication.pop('attempt_id')
    else:
        publication['stage'] = [] if defect == 'list_stage' else {}
    save(operation / 'progress.json', {'contract': 'all_catalog_incremental_migration_v1',
        'source_count': 1, 'runtime_owner': {'active_dataset': 'local-tw-index-futures'},
        'datasets': {'local-tw-index-futures': row}})
    save(operation / 'publication/local-tw-index-futures.json', publication)
    assert not any(row['kind'] == 'migration_publication' for row in ops.migration_tasks(tmp_path))


def test_public_whitelist_rejects_paths_raw_errors_nonfinite_and_future_clock():
    row = work(); row["observed_at_utc"] = ops.iso(NOW+1000)
    row.update(path="/private/secret", error="password=secret", label="<script>alert(1)</script>")
    row["metrics"] = {"raw": "secret", "pending_bytes": float("inf")}
    row["eta"], _ = ops.estimate(row, {}, now=NOW)
    result = view(row)
    text = json.dumps(result)
    assert not any(word in text for word in ("/private", "password", "<script>", "Infinity"))
    assert result["tasks"][0]["eta"]["completion_at_utc"] is None
    assert result["tasks"][0]["stale"] is True


def test_missing_receipt_clock_is_unknown_rather_than_a_retained_old_receipt():
    row = ops.task("transport_retirement", "transport-retirement",
                   phase="retire-verified-transport", reason="metadata_unavailable")
    row["eta"], _ = ops.estimate(row, {}, now=NOW)
    public = ops.public_status({"contract": ops.CONTRACT,
                               "observed_at_utc": ops.iso(NOW), "tasks": [row]}, now=NOW)
    actual = public["tasks"][0]
    assert actual["eta"]["state"] == "unknown"
    assert actual["eta"]["reason"] == "metadata_unavailable"
    assert actual["observed_at_utc"] is None
    assert actual["progress"]["percent"] is None
    assert actual["eta"]["remaining_seconds"] is None


def test_missing_operations_do_not_hide_capacity_and_stale_snapshot_disables_eta():
    value = fleet({"contract": STORAGE_CONTRACT}, now=NOW)
    assert len(value["nodes"]) == 4 and value["operations"]["state"] == "stale"
    row = work(); row["eta"] = {"state": "estimated", "completion_at_utc": ops.iso(NOW+1000), "remaining_seconds": 1000}
    value = ops.public_status({"contract": ops.CONTRACT, "observed_at_utc": ops.iso(NOW-301), "tasks": [row]}, now=NOW)
    assert value["tasks"][0]["eta"]["state"] == "stale"


def test_metadata_rejects_symlinks_and_oversized_inputs(tmp_path):
    save(tmp_path/"real.json", {"state": "okay"})
    (tmp_path/"alias.json").symlink_to(tmp_path/"real.json")
    assert ops.metadata(tmp_path/"alias.json") == {}
    assert ops.metadata(tmp_path/"real.json", limit=1) == {}
    import os
    os.mkfifo(tmp_path/"pipe")
    assert ops.metadata(tmp_path/"pipe") == {}


def test_invalid_public_enum_shapes_cannot_crash_projection():
    row = work()
    row.update(phase=[], state={}, reason=[], subject=[], node_id=[], eta={"state": [], "reason": {}})
    result = view(row)
    assert result["tasks"][0]["phase"] == "working"
    assert result["tasks"][0]["state"] == "unknown"


def test_timer_projection_does_not_mislabel_next_poll_as_job_completion(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(ops.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout=json.dumps([
        {"unit": "stockagent-wsl-retirement-queue.timer", "next": int((NOW+60)*1e6), "last": int((NOW-60)*1e6)},
        {"unit": "stockagent-private-cold-backup.timer", "next": None, "last": int(NOW*1e6)}])))
    plan = ops.schedules()
    assert plan[0]["next_check_at_utc"] == ops.iso(NOW+60)
    assert plan[1]["next_check_at_utc"] is None
    assert "completion_at_utc" not in json.dumps(plan)


def test_vast_speculative_work_is_bounded_metadata_and_never_recovery_acceptance(tmp_path):
    path = tmp_path/'service_data/pack_compute_spool/status.json'
    save(path, {'contract': 'stockagent.pack_compute_spool.v1', 'state': 'running',
                'context_sha256': 'a'*64, 'observed_at_utc': ops.iso(NOW),
                'completed_items': 8, 'total_items': 10, 'completed_bytes': 80, 'total_bytes': 100})
    row = ops.spool_tasks(tmp_path)[0]
    assert row['node_id'] == 'penguin_vast' and row['completed_items'] == 8
    assert row['complete_evidence'] is False and row['phase'] == 'sealed_pack_staging'
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert '完整清冊' in public['scope'] and public['state'] == 'running'
    assert public['progress']['percent'] == 80


def test_vast_retry_deferrals_are_visible_without_exposing_object_identity(tmp_path):
    save(tmp_path/'service_data/pack_compute_spool/status.json', {
        'contract': 'stockagent.pack_compute_spool.v1', 'observed_at_utc': ops.iso(NOW),
        'scopes': [{'context_sha256': 'a'*64, 'dataset': 'local-tw-index-futures',
                    'observed_at_utc': ops.iso(NOW), 'state': 'blocked',
                    'completed_items': 8, 'total_items': 10, 'failed_items': 1,
                    'retry_deferred_items': 2}]})
    row = ops.spool_tasks(tmp_path)[0]
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert public['metrics']['blocked_items'] == 2
    assert public['state'] == 'blocked' and public['complete_evidence'] is False
    assert 'a'*64 not in json.dumps(public)


def test_vast_capacity_wait_is_not_misreported_as_a_missing_source(tmp_path):
    save(tmp_path/'service_data/pack_compute_spool/status.json', {
        'contract': 'stockagent.pack_compute_spool.v1', 'observed_at_utc': ops.iso(NOW),
        'scopes': [{'context_sha256': 'a'*64, 'dataset': 'local-tw-index-futures',
                    'observed_at_utc': ops.iso(NOW), 'state': 'blocked', 'capacity_wait': True,
                    'remote_spool_available_bytes': 31*1024**3, 'remote_spool_reserve_bytes': 32*1024**3,
                    'completed_items': 8, 'total_items': 10, 'retry_deferred_items': 2}]})
    row = ops.spool_tasks(tmp_path)[0]
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert public['reason_code'] == 'physical_capacity'
    assert public['metrics']['source_free_bytes'] == 31*1024**3
    assert public['metrics']['source_reserve_bytes'] == 32*1024**3
    assert public['eta']['completion_at_utc'] is None


@pytest.mark.parametrize('timer_active,owner_state,valid_marker,paused', [
    (False, 'inactive', True, True),
    (True, 'inactive', True, False),
    (False, 'active', True, False),
    (None, 'unknown', True, False),
    (False, 'inactive', False, False),
])
def test_user_paused_spool_never_projects_old_capacity_as_current(timer_active, owner_state, valid_marker, paused):
    row = ops.task('vast_spool', 'scope', state='blocked', phase='sealed_pack_staging',
        observed=NOW-900, done_items=8, total_items=10, reason='physical_capacity',
        metrics={'source_free_bytes': 31, 'source_reserve_bytes': 32, 'blocked_items': 2})
    row['next_attempt_at_utc'] = ops.iso(NOW+60)
    marker = {'contract': 'stockagent.migration_timer_pause.v1',
              'unit': 'stockagent-pack-compute-spool.timer' if valid_marker else 'other.timer'}
    ops.apply_spool_user_pause([row], marker, [{'id': 'vast_spool', 'timer_active': timer_active,
                                                'owner_state': owner_state}])
    if not paused:
        assert row['state'] == 'blocked' and row['reason'] == 'physical_capacity'
        return
    assert row['state'] == 'waiting' and row['phase'] == 'spool_paused_user'
    assert row['observed_at_utc'] == ops.iso(NOW-900) and row['completed_items'] == 8
    assert 'source_free_bytes' not in row['metrics'] and 'next_attempt_at_utc' not in row
    row['checked_at_utc'] = ops.iso(NOW)
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert public['eta']['reason'] == 'user_paused_extra_verification'
    assert public['eta']['completion_at_utc'] is None and not public['complete_evidence']


@pytest.mark.parametrize('field', ['valid', 'timer_returned', 'verification_enabled', 'spool_enabled', 'marker_invalid'])
def test_user_removed_spool_is_reported_only_with_current_disabled_evidence(field):
    row = ops.task('vast_spool', 'scope', state='blocked', phase='sealed_pack_staging',
                   observed=NOW-900, done_items=8, total_items=10, reason='physical_capacity',
                   metrics={'source_free_bytes': 31, 'blocked_items': 2})
    marker = {'contract': 'stockagent.vast_verification_decommission.v1', 'state': 'removed',
              'unit': 'stockagent-pack-compute-spool.timer', 'training_data_removed': False,
              'observed_at_epoch': NOW-10}
    plan = [{'id': 'vast_spool', 'registered': False, 'timer_active': False, 'owner_state': 'inactive'}]
    verification, spool = {'enabled': False}, {'enabled': False}
    if field == 'timer_returned':
        plan[0]['registered'] = True
    elif field == 'verification_enabled':
        verification['enabled'] = True
    elif field == 'spool_enabled':
        spool['enabled'] = True
    elif field == 'marker_invalid':
        marker['training_data_removed'] = True
    ops.apply_spool_user_removal([row], marker, plan, verification, spool)
    if field != 'valid':
        assert row['state'] == 'blocked' and row['reason'] == 'physical_capacity'
        return
    assert row['state'] == 'waiting' and row['phase'] == 'spool_removed_user'
    assert row['reason'] == 'user_removed_vast_verification' and row['completed_items'] is None
    assert row['metrics'] == {} and not row['complete_evidence']
    row['checked_at_utc'] = ops.iso(NOW)
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert public['eta']['reason'] == 'user_removed_vast_verification'
    assert not public['progress_stale'] and not public['complete_evidence']


def test_user_removed_legacy_backup_is_not_projected_as_an_active_failure():
    row = ops.task('bulk_return', 'coordinator', state='blocked', phase='working',
                   observed=NOW-900, reason='bulk_return_failed', metrics={'blocked_items': 4})
    marker = {'contract': 'stockagent.vast_verification_decommission.v1', 'state': 'removed',
              'legacy_backup_units_removed': True, 'training_data_removed': False,
              'observed_at_epoch': NOW-10}
    plan = [{'id': 'bulk_return', 'registered': False, 'timer_active': False, 'owner_state': 'inactive'}]
    ops.apply_legacy_vast_backup_user_removal([row], marker, plan)
    assert row['state'] == 'waiting' and row['phase'] == 'spool_removed_user' and row['metrics'] == {}
    row['checked_at_utc'] = ops.iso(NOW)
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert public['eta']['reason'] == 'user_removed_vast_verification' and not public['progress_stale']
    row['state'], row['reason'] = 'blocked', 'bulk_return_failed'
    plan[0]['registered'] = True
    ops.apply_legacy_vast_backup_user_removal([row], marker, plan)
    assert row['state'] == 'blocked' and row['reason'] == 'bulk_return_failed'


def test_vast_pressure_verification_exposes_only_current_batch(tmp_path):
    save(tmp_path/'service_data/pack_compute_spool/status.json', {
        'contract': 'stockagent.pack_compute_spool.v1', 'observed_at_utc': ops.iso(NOW),
        'scopes': [{'context_sha256':'a'*64, 'dataset':'local-tw-index-futures',
                    'observed_at_utc':ops.iso(NOW), 'state':'running',
                    'completed_items':200, 'total_items':400, 'completed_bytes':900, 'total_bytes':1000,
                    'pressure_state':'verify_fixed_d_originals', 'pressure_plan':'b'*32,
                    'pressure_done_bytes':40, 'pressure_total_bytes':100}]})
    row = ops.spool_tasks(tmp_path)[0]
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert public['phase'] == 'verify_primary_sources'
    assert public['progress']['completed_bytes'] == 40 and public['progress']['percent'] == 40
    assert public['progress']['completed_items'] is None and public['complete_evidence'] is False
    assert 'b'*32 not in json.dumps(public)


@pytest.mark.parametrize('total_bytes,expected', [(10000, 'unknown'), (2000, 'estimated')])
def test_vast_eta_does_not_extrapolate_a_different_pack_size_mix(total_bytes, expected):
    row = ops.task('vast_spool', 'test', state='running', phase='sealed_pack_staging',
        observed=NOW-60, generation='same', done_bytes=100, total_bytes=total_bytes,
        done_items=1, total_items=20)
    _, history = ops.estimate(row, {}, now=NOW-60)
    row.update(observed_at_utc=ops.iso(NOW), completed_bytes=200, completed_items=2)
    eta, _ = ops.estimate(row, history, now=NOW)
    assert eta['state'] == expected
    if expected == 'unknown':
        assert eta['reason'] == 'heterogeneous_workloads' and eta['completion_at_utc'] is None


def test_vast_unknown_pressure_outcome_is_visible_without_claiming_reclamation(tmp_path):
    save(tmp_path/'service_data/pack_compute_spool/status.json', {
        'contract':'stockagent.pack_compute_spool.v1', 'observed_at_utc':ops.iso(NOW),
        'scopes':[{'context_sha256':'a'*64, 'dataset':'local-tw-index-futures',
                   'observed_at_utc':ops.iso(NOW), 'state':'blocked', 'capacity_wait':True,
                   'pressure_state':'blocked_unknown_prior_result', 'completed_items':2, 'total_items':3}]})
    row = ops.spool_tasks(tmp_path)[0]
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert public['metrics']['rejected_jobs'] == 1
    assert public['complete_evidence'] is False and public['eta']['state'] == 'blocked'


def test_vast_separate_cold_scopes_show_candidate_wait_without_finalizing(tmp_path):
    save(tmp_path/'service_data/pack_compute_spool/status.json', {
        'contract': 'stockagent.pack_compute_spool.v1', 'observed_at_utc': ops.iso(NOW),
        'scopes': [
            {'context_sha256': 'a'*64, 'dataset': 'local-tw-index-futures',
             'observed_at_utc': ops.iso(NOW), 'state': 'waiting_for_new_sealed_packs',
             'completed_items': 192, 'total_items': 192, 'active_jobs': 0},
            {'context_sha256': 'b'*64, 'dataset': 'tw-index-futures',
             'observed_at_utc': ops.iso(NOW), 'state': 'running',
             'completed_items': 1, 'total_items': 10, 'active_jobs': 2}]})
    rows = ops.spool_tasks(tmp_path)
    assert len(rows) == 2 and rows[0]['id'] != rows[1]['id']
    for row in rows:
        row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = ops.public_status({'contract': ops.CONTRACT, 'read_only': True,
        'observed_at_utc': ops.iso(NOW), 'tasks': rows}, now=NOW)
    first, second = public['tasks']
    assert first['phase'] == 'waiting_new_sealed_packs'
    assert first['eta']['reason'] == 'upstream_publication'
    assert first['eta']['state'] == 'unknown' and first['complete_evidence'] is False
    assert '本機歷史' in first['label'] and '台灣指數期貨歷史' in second['label']
    assert second['metrics']['active_jobs'] == 2 and second['progress']['percent'] == 10


def test_idle_spool_keeps_original_counter_clock_and_uses_real_queue_liveness(tmp_path):
    old = ops.iso(NOW-900)
    save(tmp_path/'service_data/pack_compute_spool/status.json', {
        'contract': 'stockagent.pack_compute_spool.v1', 'observed_at_utc': ops.iso(NOW),
        'scopes': [{'context_sha256': 'a'*64, 'dataset': 'local-tw-index-futures',
            'observed_at_utc': old, 'inventory_observed_at_utc': old,
            'state': 'waiting_for_new_sealed_packs', 'completed_items': 192, 'total_items': 192}]})
    row = ops.spool_tasks(tmp_path)[0]
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert public['observed_at_utc'] == old and public['inventory_at_utc'] == old
    assert public['activity_at_utc'] == ops.iso(NOW)
    assert public['progress_stale'] is False and public['eta']['reason'] == 'upstream_publication'
    row['activity_at_utc'] = old
    row['eta'], _ = ops.estimate(row, {}, now=NOW)
    public = view(row)['tasks'][0]
    assert public['progress_stale'] is True and public['eta']['state'] == 'stale'


def test_counter_rounding_cannot_claim_full_progress_before_target():
    row = work(); row.update(completed_bytes=9999999, total_bytes=10000000)
    assert view(row)["tasks"][0]["progress"]["percent"] == 99.99


def test_malformed_rate_history_is_resampled():
    eta, _ = ops.estimate(work(), [], now=NOW)
    assert eta["state"] == "sampling"


def test_remaining_time_and_bounds_age_with_the_snapshot_clock():
    row = work()
    row["eta"] = {"state": "estimated", "completion_at_utc": ops.iso(NOW+600),
        "remaining_seconds": 600, "fastest_seconds": 300, "slowest_seconds": 900}
    public = ops.public_status({"contract": ops.CONTRACT, "observed_at_utc": ops.iso(NOW), "tasks": [row]}, now=NOW+60)
    eta = public["tasks"][0]["eta"]
    assert eta["remaining_seconds"] == 540
    assert eta["fastest_seconds"] == 240 and eta["slowest_seconds"] == 840


def test_schedule_distinguishes_disabled_timer_from_active_owner(monkeypatch):
    from types import SimpleNamespace
    def query(args, **kwargs):
        if "list-timers" in args:
            return SimpleNamespace(stdout=json.dumps([{"unit": "stockagent-wsl-retirement-queue.timer", "next": None}]))
        return SimpleNamespace(stdout="Id=stockagent-wsl-retirement-queue.timer\nActiveState=inactive\nSubState=dead\n\nId=stockagent-wsl-retirement-queue.service\nActiveState=activating\nSubState=start\n")
    monkeypatch.setattr(ops.subprocess, "run", query)
    row = ops.schedules()[0]
    assert row["timer_active"] is False and row["owner_running"] is True


def test_full_schedule_uses_two_queries_and_includes_continuous_owner(monkeypatch):
    from types import SimpleNamespace
    calls = []
    def query(args, **kwargs):
        calls.append(args)
        if 'list-timers' in args:
            return SimpleNamespace(stdout=json.dumps([{'unit': unit} for unit in ops.TIMERS]))
        return SimpleNamespace(stdout='\n\n'.join(
            f'Id={unit}\nLoadState=loaded\nActiveState=active\nResult=success\nMainPID=123\nExecMainStatus=0'
            for unit in (*ops.TIMERS, *ops.CONTINUOUS)))
    monkeypatch.setattr(ops.subprocess, 'run', query)
    plan = ops.schedules()
    assert len(calls) == 2 and len(plan) == len(ops.SCHEDULE_LABELS)
    assert {'release_verification', 'lifecycle_backup', 'control_backup', 'bulk_return', 'packed_transport'} <= {p['id'] for p in plan}
    owner = next(p for p in plan if p['id'] == 'lakehouse')
    assert owner['mode'] == 'continuous' and owner['owner_running'] is True
    assert 'completion_at_utc' not in json.dumps(plan)


def test_exact_skipped_storage_condition_uses_official_guard_without_promising_completion(monkeypatch):
    from types import SimpleNamespace
    timer = 'stockagent-staged-cold-attribution.timer'
    service = timer.removesuffix('.timer')+'.service'
    condition = ('{ path=/bin/bash ; argv[]=/bin/bash -c source scripts/runtime_env.sh; run_fintech_python '
                 'scripts/check_outside_tw_opening_resource_window.py --minimum-runway-minutes 120 '
                 '--protected-until 13:30 --official-calendar-root data_tw_public ; '
                 'ignore_errors=no ; code=exited ; status=1 }')
    def query(args, **kwargs):
        if 'list-timers' in args:
            return SimpleNamespace(stdout=json.dumps([{'unit': timer, 'next': (NOW+60)*1_000_000}]))
        return SimpleNamespace(stdout=f'Id={timer}\nActiveState=active\n\n'
            f'Id={service}\nActiveState=inactive\nResult=exec-condition\nExecCondition={condition}\n'
            'ExecCondition={ argv[]=/bin/bash -c other-check ; code=(null) ; status=0/0 }\n')
    monkeypatch.setattr(ops, 'TIMERS', {timer: ('staged_cold_attribution', 'test')})
    monkeypatch.setattr(ops, 'CONTINUOUS', {})
    monkeypatch.setattr(ops.subprocess, 'run', query)
    monkeypatch.setattr(ops, '_storage_priority_window_end', lambda now: now+3600)
    row, = ops.schedules(now=NOW)
    assert row['owner_result'] == 'exec-condition'
    assert row['window_guard_reason'] == 'tw_day_trade_priority_window'
    assert row['work_not_before_utc'] == ops.iso(NOW+3600)
    assert row['next_check_at_utc'] == ops.iso(NOW+60)
    assert 'completion_at_utc' not in row
    assert not ops._is_storage_priority_condition({'Result': 'exec-condition',
        'ExecCondition': condition.replace('--protected-until 13:30', '--protected-until 09:10')})
    assert not ops._is_storage_priority_condition({'Result': 'exec-condition',
        'ExecCondition': condition.replace('status=1', 'status=2')})


def test_priority_window_schedule_and_task_distinguish_checks_from_work():
    row = ops.task('staged_cold_attribution', 'fixed', state='blocked', phase='staged_cold_attributed',
                   observed=NOW, reason='staged_cold_unresolved')
    row['checked_at_utc'] = ops.iso(NOW)
    schedule = {'id': 'staged_cold_attribution', 'registered': True, 'timer_active': True,
        'owner_running': False, 'owner_state': 'inactive', 'owner_result': 'exec-condition',
        'next_check_at_utc': ops.iso(NOW+60), 'window_guard_reason': 'tw_day_trade_priority_window',
        'work_not_before_utc': ops.iso(NOW+3600)}
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': [row], 'schedules': [schedule]}
    public = ops.public_status(raw, now=NOW)
    assert public['schedules'][0]['work_not_before_utc'] == ops.iso(NOW+3600)
    assert public['tasks'][0]['continuation']['next_check_at_utc'] == ops.iso(NOW+60)
    assert public['tasks'][0]['continuation']['work_not_before_utc'] == ops.iso(NOW+3600)
    assert public['tasks'][0]['eta']['state'] == 'blocked'
    assert public['tasks'][0]['eta']['completion_at_utc'] is None
    schedule['work_not_before_utc'] = ops.iso(NOW+90000)
    assert ops.public_status(raw, now=NOW)['schedules'][0]['work_not_before_utc'] is None
    schedule.update(work_not_before_utc=ops.iso(NOW+3600), owner_running=True)
    assert 'work_not_before_utc' not in ops.public_status(raw, now=NOW)['tasks'][0]['continuation']


def test_partial_associated_schedule_cannot_claim_all_work_is_time_gated():
    row = ops.task('source_local_repack_queue', 'fixed', state='waiting', observed=NOW)
    row['checked_at_utc'] = ops.iso(NOW)
    schedule = {'id': 'source_local_repack_queue', 'registered': True, 'timer_active': True,
        'owner_running': False, 'owner_state': 'inactive', 'owner_result': 'exec-condition',
        'next_check_at_utc': ops.iso(NOW+60), 'window_guard_reason': 'tw_day_trade_priority_window',
        'work_not_before_utc': ops.iso(NOW+3600)}
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': [row], 'schedules': [schedule]}
    assert 'work_not_before_utc' not in ops.public_status(raw, now=NOW)['tasks'][0]['continuation']


def test_short_timer_due_time_and_next_minute_observation_do_not_become_a_retry_or_finish():
    row = work()
    row.update(state='waiting', checked_at_utc=ops.iso(NOW), observed_at_utc=ops.iso(NOW-1000),
               next_attempt_at_utc=ops.iso(NOW+600), continuation={'automatic': False, 'secret': '/private'})
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': [row],
           'schedules': [{'id': 'retirement', 'registered': True, 'timer_active': True,
                          'next_check_at_utc': ops.iso(NOW-5), 'owner_running': False}]}
    public = ops.public_status(raw, now=NOW)
    actual = public['tasks'][0]
    assert actual['continuation'] == {
        'automatic': True, 'owner_running': False, 'schedule_labels': ['WSL 回收佇列'],
        'next_check_at_utc': None, 'check_due': True, 'mode': 'scheduled'}
    assert actual['next_observation_at_utc'] == ops.iso(NOW+60)
    assert actual['next_attempt_at_utc'] == ops.iso(NOW+600)
    assert actual['observed_at_utc'] == ops.iso(NOW-1000)
    assert actual['eta']['completion_at_utc'] is None
    assert public['schedules'][0]['scheduled_check_at_utc'] == ops.iso(NOW-5)
    assert public['schedules'][0]['check_due'] is True
    assert '/private' not in json.dumps(public)
    stale = ops.public_status(raw, now=NOW+301)['tasks'][0]
    assert stale['continuation']['automatic'] is False and not stale['continuation']['check_due']
    assert stale['next_observation_at_utc'] is None


def test_disabled_timer_cannot_promise_automatic_continuation_or_worker_activity():
    row = work(); row['checked_at_utc'] = ops.iso(NOW)
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': [row],
           'schedules': [{'id': 'retirement', 'registered': True, 'timer_active': False,
                          'next_check_at_utc': None, 'owner_running': False}]}
    actual = ops.public_status(raw, now=NOW)['tasks'][0]
    assert actual['continuation']['mode'] == 'unconfirmed'
    assert actual['continuation']['automatic'] is False
    assert actual['continuation']['owner_running'] is False


def test_kernel_lock_waits_match_inodes_and_original_service_ancestry(tmp_path):
    import os
    backup = tmp_path/'backup'; backup.mkdir()
    versions = tmp_path/'versions'; versions.mkdir()
    lock = backup/'owner.lock'; lock.touch()
    # A lock file merely existing is not a busy lock.
    (versions/'owner.lock').touch()
    proc = tmp_path/'proc'; proc.mkdir()
    info = lock.stat()
    inode = f'{os.major(info.st_dev):02x}:{os.minor(info.st_dev):02x}:{info.st_ino}'
    (proc/'locks').write_text(f'4: FLOCK ADVISORY WRITE 89 {inode} 0 EOF\n4: -> FLOCK ADVISORY WRITE 99 {inode} 0 EOF\n')
    (proc/'89').mkdir(); (proc/'89'/'stat').write_text('89 (backup child) S 88 0 0')
    plan = [{'id': 'registered_backup', 'owner_pid': 88}, {'id': 'private_backup', 'owner_pid': 99}]
    result = ops.owner_locks({'operations': {'backup_state_root': str(backup), 'migration_state_root': str(versions)}}, plan, proc=proc)
    actual = next(r for r in result if r['id'] == 'backup_owner')
    assert actual == {'id': 'backup_owner', 'held': True, 'holder': 'registered_backup',
                      'waiter_count': 1, 'waiting': ['private_backup']}
    assert next(r for r in result if r['id'] == 'dataset_owner')['held'] is False
    assert str(tmp_path) not in json.dumps(result) and '89' not in json.dumps(result)


def test_lock_observation_rejects_redirected_or_unbounded_metadata(tmp_path):
    proc = tmp_path/'proc'; proc.mkdir()
    backup = tmp_path/'backup'; backup.mkdir()
    target = tmp_path/'real'; target.touch(); (backup/'owner.lock').symlink_to(target)
    config = {'operations': {'backup_state_root': str(backup), 'migration_state_root': str(tmp_path/'none')}}
    (proc/'locks').write_text('')
    assert ops.owner_locks(config, [], proc=proc) == []
    (proc/'locks').write_text('x'*(256*1024+1))
    assert ops.owner_locks(config, [], proc=proc) == []


def test_fresh_observer_keeps_real_blocker_ahead_of_old_counter_warning():
    row = work()
    row.update(state='blocked', reason='source_predecessor', checked_at_utc=ops.iso(NOW+1000))
    public = view(row, now=NOW+1000)
    actual = public['tasks'][0]
    assert actual['eta']['state'] == 'blocked' and actual['eta']['reason'] == 'source_predecessor'
    assert actual['progress_stale'] is True and actual['stale'] is False
    assert '前一來源' in public['attention'][0]['message']


def test_schedule_failures_disabled_and_continuous_owners_are_actionable():
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'schedules': [
        {'id': 'private_backup', 'registered': True, 'timer_active': True, 'owner_state': 'failed', 'owner_result': 'exit-code', 'exit_code': 7},
        {'id': 'registered_backup', 'registered': True, 'timer_active': False, 'owner_state': 'inactive'},
        {'id': 'lakehouse', 'registered': True, 'mode': 'continuous', 'owner_running': False, 'owner_state': 'inactive'},
        {'id': 'cold_tiers', 'registered': True, 'timer_active': True, 'owner_state': 'inactive', 'owner_result': 'success'}]}
    result = ops.public_status(raw, now=NOW)
    assert {r['id'] for r in result['attention']} == {'private_backup', 'registered_backup', 'lakehouse'}
    assert result['schedules'][0]['exit_code'] == 7
    assert '重新啟動' not in json.dumps(result, ensure_ascii=False)


def test_publication_veto_keeps_old_receipt_clock_and_never_exports_raw_errors(tmp_path):
    path = tmp_path / 'latest.json'
    save(path, {'schema_version': 1, 'dataset': 'tw-public', 'status': 'failed', 'return_code': 75,
        'completed_at_taipei': ops.iso(NOW - 14400), 'materialization_performed': False,
        'runtime_link_changed': False, 'error': '/private/secret',
        'blocking_findings': ['stale_official_symbol_build_receipt', ['/private/secret']]})
    receipt = ops.public_cold_publication_observation(path)
    assert receipt['reason_codes'] == ['stale_official_symbol_build_receipt']
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
        'public_cold_publication': receipt, 'schedules': [{
            'id': 'public_cold_publish', 'registered': True, 'timer_active': True,
            'owner_state': 'failed', 'owner_result': 'exit-code', 'exit_code': 75}]}
    issue = ops.public_status(raw, now=NOW)['attention'][0]
    assert issue['last_report_at_utc'] == ops.iso(NOW - 14400)
    assert '最近一次發布收據' in issue['message'] and '原建置器' in issue['message']
    assert '/private/secret' not in json.dumps(receipt) + json.dumps(issue)
    receipt['observed_at_utc'] = ops.iso(NOW + 121)
    issue = ops.public_status(raw, now=NOW)['attention'][0]
    assert issue['last_report_at_utc'] is None and '最近一次發布收據' not in issue['message']
    receipt['observed_at_utc'] = ops.iso(NOW - 14400)
    raw['schedules'][0].update(owner_state='inactive', owner_result='success', exit_code=0)
    assert not ops.public_status(raw, now=NOW)['attention']


@pytest.mark.parametrize('field,value', [('schema_version', 2), ('dataset', 'unknown'),
    ('status', 'ok'), ('return_code', 1), ('runtime_link_changed', True),
    ('materialization_performed', True), ('local_publication_committed', True)])
def test_publication_failure_projection_requires_original_veto_scope(tmp_path, field, value):
    data = {'schema_version': 1, 'dataset': 'tw-public', 'status': 'failed', 'return_code': 75,
        'completed_at_taipei': ops.iso(NOW), 'materialization_performed': False,
        'runtime_link_changed': False, 'blocking_findings': ['stale_official_symbol_build_receipt']}
    data[field] = value
    save(tmp_path / 'latest.json', data)
    assert ops.public_cold_publication_observation(tmp_path / 'latest.json') == {}


def test_boot_audit_reports_bounded_failure_causes_with_original_clock(tmp_path):
    path = tmp_path / 'latest.json'
    save(path, {'observed_at_utc': ops.iso(NOW - 15), 'boot_recovery': {
        'unavailable_critical_owners': ['stockagent-finmind-sponsor.service', '/private/secret'],
        'lakehouse_control': {'required': True, 'receipts_fresh': False,
            'loaded_worker_matches_current_code': False, 'workflow_queries_verified': False}}})
    audit = ops.boot_audit_observation(path)
    assert audit['observed_at_utc'] == ops.iso(NOW - 15)
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'boot_audit': audit,
           'schedules': [{'id': 'boot_recovery', 'registered': True, 'timer_active': True,
                          'owner_state': 'failed', 'owner_result': 'exit-code'}]}
    issue = ops.public_status(raw, now=NOW)['attention'][0]
    assert issue['last_report_at_utc'] == ops.iso(NOW - 15)
    assert 'FinMind' in issue['message'] and '回執過期' in issue['message']
    assert '版本' in issue['message'] and '尚未驗證' in issue['message']
    assert '/private/secret' not in json.dumps(issue)
    raw['boot_audit']['reason_codes'].append(['/private/secret'])
    assert '/private/secret' not in json.dumps(ops.public_status(raw, now=NOW))
    raw['boot_audit']['observed_at_utc'] = ops.iso(NOW - 721)
    old = ops.public_status(raw, now=NOW)['attention'][0]
    assert 'FinMind' not in old['message'] and old['last_report_at_utc'] is None


@pytest.mark.parametrize('mode', ['disabled', 'enabled', 'missing', 'failed'])
def test_disabled_legacy_finmind_is_not_misreported_as_failed_replacement(tmp_path, mode):
    units = ('stockagent-finmind-free.service', 'stockagent-finmind-sponsor.service',
             'stockagent-finmind-complement.service')
    states = {unit: {'UnitFileState': 'disabled', 'ActiveState': 'inactive'} for unit in units}
    if mode == 'enabled':
        states[units[0]]['UnitFileState'] = 'enabled'
    elif mode == 'missing':
        states.pop(units[0])
    elif mode == 'failed':
        states[units[0]]['ActiveState'] = 'failed'
    path = tmp_path / 'latest.json'
    save(path, {'observed_at_utc': ops.iso(NOW - 15), 'boot_recovery': {
        'unavailable_critical_owners': list(units), 'production_owner_states': states,
        'lakehouse_control': {'required': True, 'receipts_fresh': False}}})
    audit = ops.boot_audit_observation(path)
    assert audit['observed_at_utc'] == ops.iso(NOW - 15)
    expected = 'finmind_disabled' if mode == 'disabled' else 'finmind_inactive'
    assert expected in audit['reason_codes'] and 'lakehouse_receipts_stale' in audit['reason_codes']
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'boot_audit': audit,
           'schedules': [{'id': 'boot_recovery', 'registered': True, 'timer_active': True,
                          'owner_state': 'failed', 'owner_result': 'exit-code'}]}
    issue = ops.public_status(raw, now=NOW)['attention'][0]
    assert issue['level'] == 'blocked' and '回執過期' in issue['message']
    if mode == 'disabled':
        assert '常駐服務目前停用' in issue['message'] and '有限任務' in issue['message']
        assert '下載服務有未運行項目' not in issue['message']


def test_public_lock_projection_and_stale_observer_do_not_leak_or_invent_facts():
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'owner_locks': [
        {'id': 'backup_owner', 'held': True, 'holder': 'registered_backup', 'waiter_count': 1,
         'waiting': ['private_backup', '/private/secret'], 'pid': 998877, 'path': '/secret/path'},
        {'id': '/secret/path', 'held': True}], 'schedules': [
        {'id': 'private_backup', 'owner_pid': 998877, 'owner_result': [], 'exit_code': 'SECRET'}]}
    result = ops.public_status(raw, now=NOW)
    assert 'private/secret' not in json.dumps(result) and '998877' not in json.dumps(result)
    assert result['owner_locks'][0]['waiting_labels'] == ['私有冷庫備份']
    assert result['attention'][0]['level'] == 'waiting'
    assert '死鎖' in result['attention'][0]['message']
    stale = ops.public_status(raw, now=NOW+301)
    assert stale['attention'][0]['id'] == 'observer'
    assert stale['owner_locks'][0]['held'] is None and stale['owner_locks'][0]['waiter_count'] is None


def test_flow_reports_only_enrolled_work_and_cannot_promote_liveness_to_acceptance():
    row = work(); row.update(kind='migration', state='running', owner_state='active')
    result = view(row)
    recovery = next(r for r in result['flow'] if r['id'] == 'recover')
    assert recovery['state'] == 'running' and recovery['depends_on'] == ['publish']
    assert all(r['scope'] == 'enrolled_current_work_only' for r in result['flow'])
    assert '未知' in result['remote_schedule_message']


def test_source_publication_blockers_remain_visible_during_another_sources_recovery():
    recovery = work(); recovery.update(kind='migration', state='running', owner_state='active')
    blocked = ops.task('migration_dependency', 'mutable-source', state='blocked',
                       phase='source_inventory', observed=NOW, reason='source_changed')
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [recovery, blocked]}, now=NOW)
    publish = next(row for row in result['flow'] if row['id'] == 'publish')
    assert publish['state'] == 'blocked' and publish['blocked_tasks'] == 1
    assert publish['known_current_step_completion_at_utc'] is None
    assert next(row for row in result['flow'] if row['id'] == 'recover')['state'] == 'running'


def test_flow_estimate_is_scoped_to_measured_steps_and_keeps_unknown_work():
    measured = work(); measured.update(kind='migration', state='running', owner_state='active')
    _, history = ops.estimate(measured, {}, now=NOW)
    measured['completed_bytes'] += 100
    measured['observed_at_utc'] = ops.iso(NOW + 60)
    measured['checked_at_utc'] = ops.iso(NOW + 60)
    measured['eta'], _ = ops.estimate(measured, history, now=NOW + 60)
    waiting = ops.task('migration', 'another', state='waiting', phase='dataset_reconstruction', observed=NOW + 60)
    waiting.update(checked_at_utc=ops.iso(NOW + 60), owner_state='active')
    waiting['eta'], _ = ops.estimate(waiting, {}, now=NOW + 60)
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW + 60),
                                'tasks': [measured, waiting]}, now=NOW + 60)
    recovery = next(r for r in result['flow'] if r['id'] == 'recover')
    assert recovery['pending_tasks'] == 2 and recovery['estimated_current_steps'] == 1
    assert recovery['unestimated_tasks'] == 1 and recovery['known_current_step_completion_at_utc']
    assert 'completion_at_utc' not in recovery  # This is not a whole-stage forecast.


def test_completed_enrolled_handoff_does_not_hide_other_source_coverage():
    staged = ops.task('workspace_source_staging', 'one', state='completed',
                      phase='workspace_source_copy_verified', observed=NOW, proof=True)
    staged['subject'] = 'crypto-etf-history'
    other = ops.task('workspace_source_staging', 'two', state='waiting', observed=NOW)
    other['subject'] = 'tw-futures-prepared'
    handoff = ops.task('workspace_source_handoff', 'one', state='completed',
                       phase='provider_workspace_completed', observed=NOW, proof=True)
    handoff['subject'] = 'crypto-etf-history'
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [staged, other, handoff]}, now=NOW)
    flow = next(row for row in result['flow'] if row['id'] == 'e_handoff')
    assert flow['completed_tasks'] == 1 and flow['pending_tasks'] == 0
    assert flow['unassessed_source_count'] == 1 and flow['state'] == 'blocked'
    assert flow['unassessed_source_labels'] == [ops.SUBJECTS['tw-futures-prepared']]
    assert '未由此交接器' in flow['condition'] or '尚未登錄交接' in flow['condition']


def test_handoff_remains_running_when_registered_source_is_actively_advancing():
    staged = ops.task('workspace_source_staging', 'one', state='completed',
                      phase='workspace_source_copy_verified', observed=NOW, proof=True)
    staged['subject'] = 'crypto-etf-history'
    other = ops.task('workspace_source_staging', 'two', state='waiting', observed=NOW)
    other['subject'] = 'tw-futures-prepared'
    handoff = ops.task('workspace_source_handoff', 'one', state='running',
                       phase='source_handoff_initial', observed=NOW, done_items=3, total_items=10)
    handoff.update(subject='crypto-etf-history', owner_state='active')
    result = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                'tasks': [staged, other, handoff]}, now=NOW)
    flow = next(row for row in result['flow'] if row['id'] == 'e_handoff')
    assert flow['state'] == 'running' and flow['unassessed_source_count'] == 1


def test_previous_copy_recheck_uses_real_file_counter_when_byte_work_is_unknown():
    row = ops.task('workspace_source_staging', 'fixed', state='running', phase='workspace_seed_recheck',
                   observed=NOW, generation='fixed', done_items=100, total_items=1000,
                   total_bytes=100000)
    _, history = ops.estimate(row, {}, now=NOW)
    row.update(completed_items=220, observed_at_utc=ops.iso(NOW+60), checked_at_utc=ops.iso(NOW+60))
    row['eta'], _ = ops.estimate(row, history, now=NOW+60)
    assert row['eta']['state'] == 'estimated' and row['eta']['unit'] == 'items'
    assert row['eta']['remaining_seconds'] == 390
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW+60),
                                'tasks': [row]}, now=NOW+60)['tasks'][0]
    assert public['progress']['metric'] == 'items' and public['progress']['percent'] == 22.0
    assert public['eta']['completion_at_utc'] and not public['complete_evidence']


def test_missed_estimate_does_not_claim_the_owner_made_no_progress():
    row = work()
    row.update(checked_at_utc=ops.iso(NOW), observed_at_utc=ops.iso(NOW),
               eta={'state': 'estimated', 'unit': 'bytes', 'completion_at_utc': ops.iso(NOW-1)})
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW),
                                 'tasks': [row]}, now=NOW)['tasks'][0]
    assert public['eta']['state'] == 'sampling'
    assert public['eta']['reason'] == 'forecast_overdue'
    assert '原預估時間已過' in public['eta']['message']


def test_elapsed_forecast_requires_new_sample_before_declaring_stall():
    row = ops.task('source_repack_retirement', 'fixed', state='running',
                   phase='source_repack_retirement_unlink', generation='fixed-plan',
                   observed=NOW, done_items=100, total_items=300)
    _, history = ops.estimate(row, {}, now=NOW)
    row.update(observed_at_utc=ops.iso(NOW+60), completed_items=200)
    eta, history = ops.estimate(row, history, now=NOW+60)
    assert eta['state'] == 'estimated' and eta['remaining_seconds'] == 60
    eta, history = ops.estimate(row, history, now=NOW+121)
    assert eta['state'] == 'sampling' and eta['reason'] == 'forecast_overdue'
    assert eta['remaining_seconds'] is None
    row.update(observed_at_utc=ops.iso(NOW+360))
    eta, _ = ops.estimate(row, history, now=NOW+360)
    assert eta['state'] == 'stalled' and eta['reason'] == 'no_recent_progress'


def test_d_cold_tier_reserve_warning_is_fresh_scoped_and_not_a_restore_failure():
    gib = 1024**3
    raw = {'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(NOW), 'tasks': [],
           'cold_capacity_check': {'observed_at_utc': ops.iso(NOW), 'available_bytes': 30*gib,
                                   'cold_tier_reserve_bytes': 64*gib}}
    notice, = [entry for entry in ops.public_status(raw, now=NOW)['attention']
               if entry['id'] == 'd_cold_tier_capacity']
    assert '尚差 34.00 GiB' in notice['message'] and '冷區壓縮整理' in notice['message']
    assert '不代表已開始的 D 讀取還原停止' in notice['message']
    assert 'E 傳輸' in notice['message']
    raw['cold_capacity_check']['observed_at_utc'] = ops.iso(NOW-301)
    assert not any(entry['id'] == 'd_cold_tier_capacity'
                   for entry in ops.public_status(raw, now=NOW)['attention'])
    raw['cold_capacity_check'].update(observed_at_utc=ops.iso(NOW), available_bytes=64*gib)
    assert not any(entry['id'] == 'd_cold_tier_capacity'
                   for entry in ops.public_status(raw, now=NOW)['attention'])
