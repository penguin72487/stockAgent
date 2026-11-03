"""Storage estimates must follow the evidence clock, phase and acceptance scope."""
import json
from pathlib import Path

import pytest

from stockagent.data_sync import storage_progress as ops
from stockagent.data_sync.storage_monitor import CONTRACT as STORAGE_CONTRACT, public_status as fleet


NOW = 1793459000.


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


def test_control_workspace_move_tracks_fixed_identity_and_phase(tmp_path):
    root = tmp_path / 'control'
    identity = 'a' * 64
    candidate = {'batch_number': 1, 'state_root': str(root), 'controls_identity': identity,
                 'files': 2, 'logical_bytes': 1000}
    waiting, = ops.workspace_controls_tasks([candidate])
    assert waiting['state'] == 'waiting' and waiting['complete_evidence'] is False
    save(root / 'journal.json', {'controls_identity': identity, 'state': 'planned'})
    save(root / 'copy/progress.json', {'state': 'copying_and_independent_sha_readback',
        'files_completed': 1, 'bytes_completed': 500, 'observed_at_epoch': NOW})
    copying, = ops.workspace_controls_tasks([candidate])
    public = view(copying)['tasks'][0]
    assert public['phase'] == 'workspace_copy' and public['progress']['percent'] == 50
    assert public['label'] == 'WSL → E · 台股公開資料準備副本'
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
    for generation in ('20261102', '20261103'):
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
    assert len(rows) == 2 and all(row['complete_evidence'] for row in rows)
    assert rows[0]['id'] != rows[1]['id']


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


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def work(**changes):
    return ops.task("retirement", "round", state="running", phase="decode-originals",
        observed=NOW, generation="fixed-original-sha", done_bytes=100, total_bytes=1000,
        done_items=2, total_items=10, **changes)


def view(row, *, now=NOW):
    return ops.public_status({"contract": ops.CONTRACT, "observed_at_utc": ops.iso(now), "tasks": [row]}, now=now)


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
    monkeypatch.setattr(ops, 'migration_tasks', lambda root: [])
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


@pytest.mark.parametrize('free,age,expected', [(0, 0, 'blocked'), (1, 0, 'blocked'),
    (64*1024**2, 0, 'running'), (0, 301, 'running')])
def test_full_physical_cold_volume_suspends_publication_forecast_only_with_fresh_measurement(
        monkeypatch, tmp_path, free, age, expected):
    row = ops.task('migration_publication', 'source', state='running', phase='routing_and_hashing',
        observed=NOW-10, generation='attempt', done_items=10, total_items=20)
    monkeypatch.setattr(ops, 'migration_tasks', lambda root: [dict(row)])
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
    monkeypatch.setattr(ops, 'migration_tasks', lambda root: [dict(row)])
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
    row["eta"] = {"state": "estimated", "remaining_seconds": 1, "completion_at_utc": ops.iso(NOW+359)}
    assert view(row, now=NOW+360)["tasks"][0]["eta"]["state"] == "stalled"


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
    assert row['reason'] == 'original_owner' and row['observed_at_utc'] == old
    assert row['activity_at_utc'] == ops.iso(NOW) and row['completed_bytes'] == 100
    public = view(row)['tasks'][0]
    assert public['progress_stale'] is True and public['eta']['completion_at_utc'] is None
    assert public['complete_evidence'] is False


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
    monkeypatch.setattr(ops, 'migration_tasks', lambda root: [])
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


def test_migration_public_view_rejects_malformed_prior_error_value():
    row = ops.task('migration_dependency', 'tw-index-futures', state='blocked',
                   phase='source_dependency', observed=ops.iso(NOW), reason='physical_capacity')
    row['previous_source_failure'] = ['/private/source']
    public = view(row)['tasks'][0]
    assert public['previous_source_failure'] is None
    assert '/private/source' not in json.dumps(public)


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
    if phase in {'committing_complete_inventory', 'packing_changed_small_files'}:
        assert row['completed_items'] == 9 and row['total_items'] == 10
        assert row['completed_bytes'] is None
    else:
        assert row['completed_items'] == (260 if phase == 'routing_and_hashing' else 230)
        assert row['completed_bytes'] == 1000
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
