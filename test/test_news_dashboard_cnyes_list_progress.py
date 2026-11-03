"""Metadata/scope-only public projection; no publisher or queue access."""
import json
from urllib.parse import urlsplit
import threading

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.live.news_dashboard import build_news_public_status
from test_news_dashboard import NOW, _seed, _server


def index_snapshot():
    return {'contract_version': 1, 'enabled': True,
            'source_dates_observed': 12, 'verified_pages': 345, 'metadata_rows': 10218,
            'public_ID_row_observations': 10200, 'dates_all_reported_pages_observed': 8,
            'dates_with_changed_source_total': 1, 'article_bodies_accepted_by_index': 0,
            'all_history_complete': False, 'finish_eta_utc': None,
            'source_total_is_not_unique_story_count': True, 'category_gaps_preserved': True,
            'url': 'https://private.test/?key=PRIVATE_TOKEN', 'raw_response_body': 'PRIVATE_BODY',
            'receipt_path': '/PRIVATE_PATH', 'progress_ratio': .99}


def history_snapshot():
    return {'contract_version': 1, 'current_source_contract_scope_states': {
                'list_complete': 2805, 'list_complete_by_page_ID_equivalence': 86,
                'partition_list_complete': 1116, 'waiting_page_evidence': 3},
            'scope_states': {'PRIVATE_REASON': 999999}, 'retained_metadata_work': {'PRIVATE_WORK': 3},
            'retired_gap_receipts': 1116, 'list_coverage_is_not_body_coverage': True,
            'all_history_complete': False, 'finish_eta_utc': None,
            'unresolved_category_scopes': 0, 'progress_ratio': 1,
            'scope_url': 'https://private.test/?key=PRIVATE_TOKEN', 'raw_response_body': 'PRIVATE_BODY'}


def gaps_snapshot():
    return {'contract_version': 1, 'total_gaps': 1520, 'groups': [
                {'provider': 'cnyes', 'error_code': 'cnyes_single_day_page_cap', 'scopes': 1518},
                {'provider': 'other', 'error_code': 'PRIVATE_REASON', 'scopes': 2}],
            'sample_rows': 64, 'sample_truncated': True, 'all_gap_details_preserved': True,
            'historical_completeness_verified': False,
            'sample': [{'url': 'https://private.test/PRIVATE_BODY'}]}


def save(root, status):
    atomic_write_json(root / 'data_news_fulltext/status.json', status)
    return build_news_public_status(root, now=NOW)


def setup(root):
    status = _seed(root)
    status['providers']['cnyes'].update(public_index_progress=index_snapshot(),
                                      history_list_progress=history_snapshot())
    status['bulk_gap_projection'] = gaps_snapshot()
    return status


def cnyes(result):
    return next(row for row in result['providers'] if row['id'] == 'cnyes')


def test_lists_are_typed_scopes_not_body_progress_or_global_eta(tmp_path):
    status = setup(tmp_path)
    baseline = build_news_public_status(tmp_path, now=NOW)
    result = save(tmp_path, status)
    source = cnyes(result)
    assert result['cnyes_list_progress_projection_contract_version'] == 1
    index = source['public_index_progress']
    assert index['verified_pages'] == 345 and index['metadata_rows'] == 10218
    assert index['public_ID_row_observations'] == 10200 and index['source_dates_observed'] == 12
    assert index['dates_all_reported_pages_observed'] == 8
    assert index['dates_reconciling_page_membership'] is None  # Legacy snapshots did not record this.
    assert index['article_bodies_accepted_by_index'] == 0
    assert index['source_total_is_not_unique_story_count'] and index['category_gaps_preserved']
    history = source['history_list_progress']
    assert history['verified_list_scopes'] == 4007
    assert history['current_source_contract_scope_states']['waiting_page_evidence'] == 3
    assert history['unresolved_category_scopes'] == 1518  # Not the 64-row sample or owner hint.
    assert history['retired_gap_receipts'] == 1116
    assert history['list_coverage_is_not_body_coverage']
    for section in (index, history):
        assert section['progress_ratio'] is section['finish_eta_utc'] is None
        assert section['all_history_complete'] is False
    assert source['known_urls'] == 6 and source['complete_urls'] == 2
    assert source['progress_ratio'] == 2 / 6 and source['remaining_urls'] == 4
    assert result['acquisition'] == baseline['acquisition']
    assert not result['scope']['publisher_calls_on_page_view']
    assert 'PRIVATE_' not in json.dumps(result)


def test_original_priority_projects_only_bounded_admission_counts(tmp_path):
    status = setup(tmp_path)
    priority = {'contract_version': 1, 'state': 'scanning', 'problem': None,
                'targets': 854, 'processed_targets': 100, 'stale_targets': 2,
                'enqueued_pages': 99, 'existing_pages': 1, 'publisher_requests': 0,
                'accepted_as_originals': False, 'all_history_complete': False,
                'private_cursor': 'PRIVATE_CURSOR'}
    status['providers']['cnyes']['history_list_progress']['original_priority_admission'] = priority
    public = cnyes(save(tmp_path, status))['history_list_progress']['original_priority_admission']
    assert public['processed_targets'] == 100 and public['targets'] == 854
    assert public['enqueued_pages'] == 99 and not public['accepted_as_originals']
    assert 'PRIVATE_CURSOR' not in json.dumps(public)
    priority['state'] = 'complete'
    assert cnyes(save(tmp_path, status))['history_list_progress']['original_priority_admission'] is None
    priority.update(state='blocked', problem='plan_unavailable_or_invalid', targets=None,
                    processed_targets=None, stale_targets=None, enqueued_pages=None, existing_pages=None)
    public = cnyes(save(tmp_path, status))['history_list_progress']['original_priority_admission']
    assert public['state'] == 'blocked' and public['problem'] == 'plan_unavailable_or_invalid'
    priority['problem'] = 'checkpoint_archive_conflict'
    public = cnyes(save(tmp_path, status))['history_list_progress']['original_priority_admission']
    assert public['state'] == 'blocked' and public['problem'] == 'checkpoint_archive_conflict'
    priority['problem'] = 'PRIVATE_UNKNOWN_PROBLEM'
    assert cnyes(save(tmp_path, status))['history_list_progress']['original_priority_admission'] is None


def test_original_priority_audit_projects_only_consistent_page_counts(tmp_path):
    status = setup(tmp_path)
    priority = {'contract_version': 1, 'state': 'complete', 'problem': None,
                'targets': 854, 'processed_targets': 854, 'stale_targets': 0,
                'enqueued_pages': 92, 'existing_pages': 8, 'publisher_requests': 0,
                'accepted_as_originals': False, 'all_history_complete': False}
    audit = {'contract_version': 1, 'tracking_complete': True, 'tracked_pages': 100,
             'awaiting_pages': 20, 'verified_pages': 40, 'mismatched_pages': 15,
             'unverified_pages': 10, 'outside_pages': 5, 'changed_pages': 10,
             'local_raw_only': True, 'publisher_requests': 0, 'all_history_complete': False,
             'private_path': 'PRIVATE_PATH'}
    history = status['providers']['cnyes']['history_list_progress']
    history.update(original_priority_admission=priority, original_priority_audit=audit)
    public = cnyes(save(tmp_path, status))['history_list_progress']
    assert public['original_priority_audit']['mismatched_pages'] == 15
    assert 'recovered_id_pages' not in public['original_priority_audit']  # Old snapshots stay unknown.
    assert public['current_source_contract_scope_states']['list_complete'] == 2805
    assert 'PRIVATE_PATH' not in json.dumps(public)
    audit['mismatched_pages'] = 16
    assert cnyes(save(tmp_path, status))['history_list_progress']['original_priority_audit'] is None
    audit.update(mismatched_pages=15, tracking_complete=False)
    assert cnyes(save(tmp_path, status))['history_list_progress']['original_priority_audit'] is None
    audit.update(tracked_pages=99, awaiting_pages=19)
    assert not cnyes(save(tmp_path, status))['history_list_progress']['original_priority_audit']['tracking_complete']
    priority.update(state='blocked', problem='plan_unavailable_or_invalid', targets=None,
                    processed_targets=None, stale_targets=None, enqueued_pages=None, existing_pages=None)
    assert cnyes(save(tmp_path, status))['history_list_progress']['original_priority_audit'] is None


@pytest.mark.parametrize('invalid', [None, -1, True, 'PRIVATE_PATH'])
def test_original_priority_v2_rejects_missing_or_invalid_recovered_count(tmp_path, invalid):
    status = setup(tmp_path)
    history = status['providers']['cnyes']['history_list_progress']
    history['original_priority_admission'] = {
        'contract_version': 1, 'state': 'complete', 'problem': None, 'targets': 1,
        'processed_targets': 1, 'stale_targets': 0, 'enqueued_pages': 1, 'existing_pages': 0,
        'publisher_requests': 0, 'accepted_as_originals': False, 'all_history_complete': False}
    history['original_priority_audit'] = {
        'contract_version': 2, 'tracking_complete': True, 'tracked_pages': 1,
        'awaiting_pages': 0, 'verified_pages': 0, 'mismatched_pages': 0, 'unverified_pages': 0,
        'outside_pages': 0, 'changed_pages': 0, 'recovered_id_pages': invalid,
        'local_raw_only': True, 'publisher_requests': 0, 'all_history_complete': False}
    assert cnyes(save(tmp_path, status))['history_list_progress']['original_priority_audit'] is None
    history['original_priority_audit']['recovered_id_pages'] = 1
    public = cnyes(save(tmp_path, status))['history_list_progress']['original_priority_audit']
    assert public['contract_version'] == 2 and public['recovered_id_pages'] == 1
    assert 'PRIVATE_PATH' not in json.dumps(public)


@pytest.mark.parametrize('patch', [
    {'tracking_complete': 1}, {'tracked_pages': True}, {'awaiting_pages': -1},
    {'mismatched_pages': 'PRIVATE_PATH'}, {'publisher_requests': 1},
    {'local_raw_only': False}, {'all_history_complete': True}, {'contract_version': 3},
])
def test_original_priority_audit_rejects_invalid_counts_and_scope(tmp_path, patch):
    status = setup(tmp_path)
    history = status['providers']['cnyes']['history_list_progress']
    history['original_priority_admission'] = {
        'contract_version': 1, 'state': 'complete', 'problem': None, 'targets': 1,
        'processed_targets': 1, 'stale_targets': 0, 'enqueued_pages': 1, 'existing_pages': 0,
        'publisher_requests': 0, 'accepted_as_originals': False, 'all_history_complete': False}
    history['original_priority_audit'] = {
        'contract_version': 1, 'tracking_complete': True, 'tracked_pages': 1,
        'awaiting_pages': 1, 'verified_pages': 0, 'mismatched_pages': 0, 'unverified_pages': 0,
        'outside_pages': 0, 'changed_pages': 0, 'local_raw_only': True, 'publisher_requests': 0,
        'all_history_complete': False, **patch}
    result = cnyes(save(tmp_path, status))['history_list_progress']
    assert result['original_priority_audit'] is None and 'PRIVATE_PATH' not in json.dumps(result)


def test_revision_count_is_bounded_and_projected_without_scope_details(tmp_path):
    status = setup(tmp_path)
    status['providers']['cnyes']['public_index_progress']['dates_reconciling_page_membership'] = 3
    index = cnyes(save(tmp_path, status))['public_index_progress']
    assert index['dates_reconciling_page_membership'] == 3
    status['providers']['cnyes']['public_index_progress']['dates_reconciling_page_membership'] = 5
    assert cnyes(save(tmp_path, status))['public_index_progress'] is None


def test_index_local_recovery_projection_preserves_unknown_and_private_scope(tmp_path):
    status = setup(tmp_path)
    index = status['providers']['cnyes']['public_index_progress']
    assert cnyes(save(tmp_path, status))['public_index_progress']['local_recovery'] is None
    index.update(checkpoint_contract_version=1, pending_local_originals=2,
                 next_local_recovery_at_utc='1970-01-01T00:00:00+00:00',
                 local_recovery_ready=True, private_original_path='/PRIVATE_PATH')
    result = save(tmp_path, status)
    assert cnyes(result)['public_index_progress']['local_recovery'] == {
        'pending_originals': 2, 'next_retry_ready': True, 'next_retry_at_utc': None}
    assert 'PRIVATE_' not in json.dumps(result)
    index.update(pending_local_originals=0, next_local_recovery_at_utc=None, local_recovery_ready=False)
    assert cnyes(save(tmp_path, status))['public_index_progress']['local_recovery']['pending_originals'] == 0


@pytest.mark.parametrize('patch', [
    {'checkpoint_contract_version': True}, {'pending_local_originals': True},
    {'pending_local_originals': -1}, {'local_recovery_ready': 1},
    {'next_local_recovery_at_utc': None}, {'next_local_recovery_at_utc': 'invalid'},
    {'next_local_recovery_at_utc': '9999-12-31T23:59:59-23:59'},
])
def test_invalid_index_local_recovery_is_unknown_without_hiding_index(tmp_path, patch):
    status = setup(tmp_path)
    status['providers']['cnyes']['public_index_progress'].update({
        'checkpoint_contract_version': 1, 'pending_local_originals': 1,
        'next_local_recovery_at_utc': NOW.isoformat(), 'local_recovery_ready': True, **patch})
    index = cnyes(save(tmp_path, status))['public_index_progress']
    assert index['verified_pages'] == 345 and index['local_recovery'] is None


def test_local_overflow_recovery_shows_only_safe_page_counts(tmp_path):
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress']['overflow_retained_recovery'] = {
        'contract_version': 1, 'scan_state': 'scanning', 'pending_retained_pages': 17,
        'local_raw_only': True, 'category_gap_completion_claimed': False, 'publisher_requests': 0,
        'private_scope': 'PRIVATE_URL'}
    history = cnyes(save(tmp_path, status))['history_list_progress']
    recovery = history['overflow_retained_recovery']
    assert recovery['scan_state'] == 'scanning' and recovery['pending_retained_pages'] == 17
    assert recovery['local_retry_pages'] is None  # Older snapshot: do not invent a zero.
    assert 'PRIVATE_URL' not in json.dumps(history)
    status['providers']['cnyes']['history_list_progress']['overflow_retained_recovery']['local_retry_pages'] = 4
    assert cnyes(save(tmp_path, status))['history_list_progress']['overflow_retained_recovery']['local_retry_pages'] == 4


@pytest.mark.parametrize('problem', ['invalid_checkpoint', 'unsupported_version', 'audit_conflict'])
def test_local_overflow_checkpoint_problem_is_public_only_when_recognized(tmp_path, problem):
    status = setup(tmp_path)
    recovery = {'contract_version': 1, 'scan_state': 'unknown', 'pending_retained_pages': 17,
                'local_raw_only': True, 'category_gap_completion_claimed': False, 'publisher_requests': 0,
                'checkpoint_problem': problem, 'private_cursor': 'PRIVATE_CURSOR'}
    status['providers']['cnyes']['history_list_progress']['overflow_retained_recovery'] = recovery
    public = cnyes(save(tmp_path, status))['history_list_progress']['overflow_retained_recovery']
    assert public['checkpoint_problem'] == problem
    assert 'PRIVATE_CURSOR' not in json.dumps(public)
    recovery['checkpoint_problem'] = 'PRIVATE_CURSOR'
    public = cnyes(save(tmp_path, status))['history_list_progress']['overflow_retained_recovery']
    assert public['checkpoint_problem'] is None and 'PRIVATE_CURSOR' not in json.dumps(public)


def test_nonzero_local_scan_projects_candidate_counts_without_private_cursor(tmp_path):
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress']['unobserved_nonzero_retained_scan'] = {
        'contract_version': 1, 'scan_state': 'scanning', 'checked_steps': 40,
        'queued_candidate_pages': 17, 'local_raw_only': True, 'all_history_complete': False,
        'publisher_requests': 0, 'scope_url': 'PRIVATE_URL'}
    history = cnyes(save(tmp_path, status))['history_list_progress']
    local = history['unobserved_nonzero_retained_scan']
    assert local['checked_steps'] == 40 and local['queued_candidate_pages'] == 17
    assert local['scan_state'] == 'scanning' and history['finish_eta_utc'] is None
    assert 'PRIVATE_URL' not in json.dumps(history)


@pytest.mark.parametrize('problem', ['invalid_checkpoint', 'unsupported_version', 'audit_conflict'])
def test_nonzero_checkpoint_problem_is_safely_projected(tmp_path, problem):
    status = setup(tmp_path)
    scan = {'contract_version': 1, 'scan_state': 'unknown', 'checked_steps': None,
            'queued_candidate_pages': None, 'local_raw_only': True, 'all_history_complete': False,
            'publisher_requests': 0, 'checkpoint_problem': problem, 'private_cursor': 'PRIVATE_CURSOR'}
    status['providers']['cnyes']['history_list_progress']['unobserved_nonzero_retained_scan'] = scan
    public = cnyes(save(tmp_path, status))['history_list_progress']['unobserved_nonzero_retained_scan']
    assert public['checkpoint_problem'] == problem and 'PRIVATE_CURSOR' not in json.dumps(public)
    scan['checkpoint_problem'] = 'PRIVATE_CURSOR'
    public = cnyes(save(tmp_path, status))['history_list_progress']['unobserved_nonzero_retained_scan']
    assert public['checkpoint_problem'] is None and 'PRIVATE_CURSOR' not in json.dumps(public)


def test_legacy_local_scan_status_whitelists_states_and_never_exposes_cursors(tmp_path):
    status = setup(tmp_path)
    history = status['providers']['cnyes']['history_list_progress']
    history['legacy_metadata_scans'] = {
        'mismatch': {'scan_state': 'unknown', 'checkpoint_problem': 'unsupported_version', 'cursor': 'PRIVATE_CURSOR'},
        'unavailable': {'scan_state': 'scanning', 'checkpoint_problem': 'PRIVATE_CURSOR'},
        'empty': {'scan_state': 'PRIVATE_CURSOR', 'checkpoint_problem': 'PRIVATE_CURSOR'},
        'PRIVATE_CURSOR': {'scan_state': 'complete'}}
    public = cnyes(save(tmp_path, status))['history_list_progress']['legacy_metadata_scans']
    assert public == {'mismatch': {'scan_state': 'unknown', 'checkpoint_problem': 'unsupported_version'},
                      'unavailable': {'scan_state': 'scanning', 'checkpoint_problem': None},
                      'empty': {'scan_state': 'unknown', 'checkpoint_problem': None}}
    assert 'PRIVATE_CURSOR' not in json.dumps(public)
    del history['legacy_metadata_scans']
    assert cnyes(save(tmp_path, status))['history_list_progress']['legacy_metadata_scans'] is None


def test_accepted_batch_local_scan_projects_counts_without_private_cursor(tmp_path):
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress']['accepted_body_batch_scan'] = {
        'contract_version': 1, 'scan_state': 'local_retry_wait', 'scanned_batches': 40,
        'indexed_candidates': 17, 'local_retry_batches': 2, 'local_missing_retry_batches': 3,
        'local_invalid_retry_batches': 4, 'local_invalid_migration_state': 'scanning',
        'local_raw_only': True, 'all_history_complete': False,
        'publisher_requests': 0, 'path': 'PRIVATE_PATH'}
    history = cnyes(save(tmp_path, status))['history_list_progress']
    local = history['accepted_body_batch_scan']
    assert local['scan_state'] == 'local_retry_wait' and local['scanned_batches'] == 40
    assert local['indexed_candidates'] == 17 and history['finish_eta_utc'] is None
    assert local['local_retry_batches'] == 2 and local['local_missing_retry_batches'] == 3
    assert local['local_invalid_retry_batches'] == 4
    assert local['local_invalid_migration_state'] == 'scanning'
    assert 'PRIVATE_PATH' not in json.dumps(history)


@pytest.mark.parametrize('problem', ['invalid_checkpoint', 'unsupported_version', 'audit_conflict'])
def test_accepted_batch_checkpoint_problem_projects_only_public_codes(tmp_path, problem):
    status = setup(tmp_path)
    scan = {'contract_version': 1, 'scan_state': 'unknown', 'scanned_batches': None,
            'indexed_candidates': None, 'local_raw_only': True, 'all_history_complete': False,
            'publisher_requests': 0, 'checkpoint_problem': problem, 'private_cursor': 'PRIVATE_CURSOR'}
    status['providers']['cnyes']['history_list_progress']['accepted_body_batch_scan'] = scan
    public = cnyes(save(tmp_path, status))['history_list_progress']['accepted_body_batch_scan']
    assert public['checkpoint_problem'] == problem and 'PRIVATE_CURSOR' not in json.dumps(public)
    scan['checkpoint_problem'] = 'PRIVATE_CURSOR'
    public = cnyes(save(tmp_path, status))['history_list_progress']['accepted_body_batch_scan']
    assert public['checkpoint_problem'] is None and 'PRIVATE_CURSOR' not in json.dumps(public)


@pytest.mark.parametrize(('remaining', 'estimated', 'valid'), [
    (60, 100, True), (0, 40, True), (61, 100, False),
    (None, 100, False), (60, None, False), (True, 41, False),
    (-1, 39, False), (2**53, 2**53 + 40, False),
])
def test_accepted_frontier_estimate_requires_consistent_safe_integer_counts(tmp_path, remaining, estimated, valid):
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress']['accepted_body_batch_scan'] = {
        'contract_version': 1, 'scan_state': 'scanning', 'scanned_batches': 40,
        'indexed_candidates': 17, 'local_retry_batches': 2, 'local_missing_retry_batches': 3,
        'local_invalid_retry_batches': 4, 'local_invalid_migration_state': 'complete',
        'remaining_frontier_batches': remaining, 'estimated_frontier_batches': estimated,
        'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0}
    scan = cnyes(save(tmp_path, status))['history_list_progress']['accepted_body_batch_scan']
    assert scan['scanned_batches'] == 40 and scan['local_retry_batches'] == 2
    assert scan['remaining_frontier_batches'] == (remaining if valid else None)
    assert scan['estimated_frontier_batches'] == (estimated if valid else None)
    assert scan['all_history_complete'] is False


@pytest.mark.parametrize(('migration', 'invalid', 'expected_state', 'expected_migration'), [
    ('not_started', 0, 'scanning', 'not_started'),
    ('scanning', 0, 'scanning', 'scanning'),
    ('complete', 2, 'local_retry_wait', 'complete'),
    ('complete', 0, 'complete', 'complete'),
    ('unknown', 0, 'unknown', 'unknown'),
    ('PRIVATE_URL', 0, 'unknown', 'unknown'),
    (None, 2, 'local_retry_wait', None),
    (None, 0, 'unknown', None),
    (None, None, 'unknown', None),
])
def test_accepted_batch_progress_distinguishes_migration_and_retries(
        tmp_path, migration, invalid, expected_state, expected_migration):
    status = setup(tmp_path)
    history = status['providers']['cnyes']['history_list_progress']
    history['accepted_body_batch_scan'] = {
        'contract_version': 1, 'scan_state': 'complete', 'scanned_batches': 40,
        'indexed_candidates': 17, 'local_retry_batches': 0, 'local_missing_retry_batches': 0,
        'local_invalid_retry_batches': invalid, 'local_invalid_migration_state': migration,
        'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0}
    projected = cnyes(save(tmp_path, status))['history_list_progress']
    local = projected['accepted_body_batch_scan']
    assert local['scan_state'] == expected_state and local['local_invalid_migration_state'] == expected_migration
    assert local['local_invalid_retry_batches'] == invalid
    assert projected['unresolved_category_scopes'] == 1518
    assert 'PRIVATE_URL' not in json.dumps(projected)


@pytest.mark.parametrize('field', ['local_retry_batches', 'local_missing_retry_batches'])
def test_accepted_batch_pending_local_retries_cannot_show_complete(tmp_path, field):
    status = setup(tmp_path)
    scan = {'contract_version': 1, 'scan_state': 'complete', 'scanned_batches': 40,
        'indexed_candidates': 17, 'local_retry_batches': 0, 'local_missing_retry_batches': 0,
        'local_invalid_retry_batches': 0, 'local_invalid_migration_state': 'complete',
        'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0}
    scan[field] = 2
    status['providers']['cnyes']['history_list_progress']['accepted_body_batch_scan'] = scan
    local = cnyes(save(tmp_path, status))['history_list_progress']['accepted_body_batch_scan']
    assert local['scan_state'] == 'local_retry_wait' and local[field] == 2


def test_unqueried_local_retry_counts_project_as_unknown(tmp_path):
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress']['accepted_body_batch_scan'] = {
        'contract_version': 1, 'scan_state': 'unknown', 'scanned_batches': None,
        'indexed_candidates': None, 'local_retry_batches': None, 'local_missing_retry_batches': None,
        'local_invalid_retry_batches': None, 'local_invalid_migration_state': None,
        'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0}
    scan = cnyes(save(tmp_path, status))['history_list_progress']['accepted_body_batch_scan']
    assert scan['scan_state'] == 'unknown'
    assert scan['local_retry_batches'] is scan['local_missing_retry_batches'] is None
    assert scan['local_invalid_retry_batches'] is scan['local_invalid_migration_state'] is None


def test_completed_integrity_scan_projects_counts_and_invalid_scope_separately(tmp_path):
    status = setup(tmp_path)
    history = status['providers']['cnyes']['history_list_progress']
    history['current_source_contract_scope_states'].update(invalid_page_evidence=2, invalid_scope_evidence=3)
    history['completed_integrity_scan'] = {'contract_version': 1, 'scan_state': 'scanning',
        'checked_scopes': 40, 'affected_scopes': 43, 'local_metadata_only': True,
        'all_history_complete': False, 'publisher_requests': 0, 'cursor': 'PRIVATE_URL'}
    projected = cnyes(save(tmp_path, status))['history_list_progress']
    assert projected['invalid_page_evidence_scopes'] == 2 and projected['invalid_scope_evidence_scopes'] == 3
    assert projected['completed_integrity_scan']['checked_scopes'] == 40
    assert projected['completed_integrity_scan']['affected_scopes'] == 43
    assert 'PRIVATE_URL' not in json.dumps(projected)


def test_original_envelope_audit_projects_only_aggregate_counts(tmp_path):
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress']['original_envelope_audit'] = {
        'contract_version': 1, 'scan_state': 'scanning', 'checked_pages': 40, 'verified_pages': 30,
        'out_of_scope_pages': 5, 'mismatched_pages': 3,
        'unverified_pages': 2, 'retry_pending_pages': 1, 'local_raw_only': True, 'all_history_complete': False,
        'publisher_requests': 0, 'path': 'PRIVATE_PATH', 'request_url': 'PRIVATE_URL'}
    history = cnyes(save(tmp_path, status))['history_list_progress']
    scan = history['original_envelope_audit']
    assert (scan['checked_pages'], scan['mismatched_pages'], scan['unverified_pages']) == (40, 3, 2)
    assert scan['retry_pending_pages'] == 1
    assert 'PRIVATE_PATH' not in json.dumps(history) and 'PRIVATE_URL' not in json.dumps(history)
    assert history['finish_eta_utc'] is None


@pytest.mark.parametrize('problem', ['invalid_checkpoint', 'unsupported_version', 'audit_conflict'])
def test_original_audit_problem_is_sanitized_and_keeps_unknown_counts(tmp_path, problem):
    status = setup(tmp_path)
    scan = {'contract_version': 1, 'scan_state': 'unknown', 'checked_pages': None, 'verified_pages': None,
            'out_of_scope_pages': None, 'mismatched_pages': None, 'unverified_pages': None,
            'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0,
            'checkpoint_problem': problem, 'private_cursor': 'PRIVATE_CURSOR'}
    status['providers']['cnyes']['history_list_progress']['original_envelope_audit'] = scan
    public = cnyes(save(tmp_path, status))['history_list_progress']['original_envelope_audit']
    assert public['checkpoint_problem'] == problem and public['checked_pages'] is None
    assert 'PRIVATE_CURSOR' not in json.dumps(public)
    scan['checkpoint_problem'] = 'PRIVATE_CURSOR'
    assert cnyes(save(tmp_path, status))['history_list_progress']['original_envelope_audit']['checkpoint_problem'] is None


@pytest.mark.parametrize('bad', [True, -1, 'PRIVATE_PATH', 2**53, None])
def test_original_audit_invalid_pending_count_stays_unknown_without_leaking(tmp_path, bad):
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress']['original_envelope_audit'] = {
        'contract_version': 1, 'scan_state': 'complete', 'checked_pages': 2, 'verified_pages': 1,
        'out_of_scope_pages': 0, 'mismatched_pages': 0, 'unverified_pages': 1,
        'retry_pending_pages': bad, 'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0}
    scan = cnyes(save(tmp_path, status))['history_list_progress']['original_envelope_audit']
    assert scan['retry_pending_pages'] is None and 'PRIVATE_PATH' not in json.dumps(scan)


@pytest.mark.parametrize(('stamp', 'state', 'expected'), [
    ('2026-10-30T12:00:00+00:00', 'complete', '2026-10-30T12:00:00+00:00'),
    ('2026-10-30T20:00:00+08:00', 'complete', '2026-10-30T12:00:00+00:00'),
    ('2026-10-30T12:00:00+00:00', 'scanning', None),
    ('PRIVATE_URL', 'complete', None),
    ('2026-10-30T12:00:00', 'complete', None),
    (None, 'complete', None),
])
def test_original_audit_next_time_is_optional_and_sanitized(tmp_path, stamp, state, expected):
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress']['original_envelope_audit'] = {
        'contract_version': 1, 'scan_state': state, 'checked_pages': 40, 'verified_pages': 30,
        'out_of_scope_pages': 5, 'mismatched_pages': 3, 'unverified_pages': 2,
        'next_audit_at_utc': stamp, 'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0}
    scan = cnyes(save(tmp_path, status))['history_list_progress']['original_envelope_audit']
    assert scan['next_audit_at_utc'] == expected
    assert 'PRIVATE_URL' not in json.dumps(scan)


@pytest.mark.parametrize('change', [
    {'contract_version': True}, {'scan_state': 'PRIVATE_URL'}, {'checked_pages': True},
    {'mismatched_pages': -1}, {'verified_pages': True}, {'out_of_scope_pages': 4}, {'unverified_pages': 38}, {'local_raw_only': False},
    {'all_history_complete': True}, {'publisher_requests': True}, {'publisher_requests': 1},
])
def test_invalid_original_envelope_audit_does_not_hide_other_progress(tmp_path, change):
    status = setup(tmp_path)
    scan = {'contract_version': 1, 'scan_state': 'scanning', 'checked_pages': 40, 'verified_pages': 30,
            'out_of_scope_pages': 5, 'mismatched_pages': 3,
            'unverified_pages': 2, 'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0}
    scan.update(change)
    status['providers']['cnyes']['history_list_progress']['original_envelope_audit'] = scan
    history = cnyes(save(tmp_path, status))['history_list_progress']
    assert history['original_envelope_audit'] is None and history['unresolved_category_scopes'] == 1518


@pytest.mark.parametrize('change', [
    {'contract_version': True}, {'scan_state': 'PRIVATE_URL'}, {'checked_scopes': True},
    {'affected_scopes': -1}, {'local_metadata_only': False}, {'all_history_complete': True},
    {'publisher_requests': True}, {'publisher_requests': 1},
])
def test_unverified_integrity_scan_does_not_hide_other_history_progress(tmp_path, change):
    status = setup(tmp_path)
    scan = {'contract_version': 1, 'scan_state': 'scanning', 'checked_scopes': 40,
            'affected_scopes': 3, 'local_metadata_only': True, 'all_history_complete': False,
            'publisher_requests': 0}
    scan.update(change)
    status['providers']['cnyes']['history_list_progress']['completed_integrity_scan'] = scan
    projected = cnyes(save(tmp_path, status))['history_list_progress']
    assert projected['completed_integrity_scan'] is None and projected['unresolved_category_scopes'] == 1518


@pytest.mark.parametrize('change', [
    {'contract_version': True}, {'scan_state': 'PRIVATE_PATH'}, {'scanned_batches': True},
    {'indexed_candidates': -1}, {'local_retry_batches': True}, {'local_retry_batches': -1},
    {'local_missing_retry_batches': True}, {'local_missing_retry_batches': -1},
    {'local_invalid_retry_batches': True}, {'local_invalid_retry_batches': -1},
    {'local_raw_only': False}, {'all_history_complete': True},
    {'publisher_requests': True}, {'publisher_requests': 1},
])
def test_unverified_accepted_batch_scan_does_not_hide_other_history_progress(tmp_path, change):
    status = setup(tmp_path)
    local = {'contract_version': 1, 'scan_state': 'scanning', 'scanned_batches': 40,
             'indexed_candidates': 17, 'local_raw_only': True, 'all_history_complete': False,
             'publisher_requests': 0}
    local.update(change)
    status['providers']['cnyes']['history_list_progress']['accepted_body_batch_scan'] = local
    history = cnyes(save(tmp_path, status))['history_list_progress']
    assert history['accepted_body_batch_scan'] is None and history['unresolved_category_scopes'] == 1518


@pytest.mark.parametrize('change', [
    {'contract_version': True}, {'scan_state': 'PRIVATE_URL'}, {'checked_steps': True},
    {'queued_candidate_pages': -1}, {'queued_candidate_pages': 41}, {'local_raw_only': False},
    {'all_history_complete': True}, {'publisher_requests': True}, {'publisher_requests': 1},
])
def test_unverified_nonzero_scan_does_not_hide_other_history_progress(tmp_path, change):
    status = setup(tmp_path)
    local = {'contract_version': 1, 'scan_state': 'scanning', 'checked_steps': 40,
             'queued_candidate_pages': 17, 'local_raw_only': True, 'all_history_complete': False,
             'publisher_requests': 0}
    local.update(change)
    status['providers']['cnyes']['history_list_progress']['unobserved_nonzero_retained_scan'] = local
    history = cnyes(save(tmp_path, status))['history_list_progress']
    assert history['unobserved_nonzero_retained_scan'] is None and history['unresolved_category_scopes'] == 1518


@pytest.mark.parametrize('change', [
    {'contract_version': True}, {'scan_state': 'PRIVATE_URL'}, {'pending_retained_pages': True},
    {'pending_retained_pages': -1}, {'local_retry_pages': True}, {'local_retry_pages': -1},
    {'local_raw_only': False}, {'category_gap_completion_claimed': True},
    {'publisher_requests': True}, {'publisher_requests': 1},
])
def test_unverified_local_recovery_does_not_hide_other_history_progress(tmp_path, change):
    status = setup(tmp_path)
    recovery = {'contract_version': 1, 'scan_state': 'complete', 'pending_retained_pages': 0,
                'local_raw_only': True, 'category_gap_completion_claimed': False, 'publisher_requests': 0}
    recovery.update(change)
    status['providers']['cnyes']['history_list_progress']['overflow_retained_recovery'] = recovery
    history = cnyes(save(tmp_path, status))['history_list_progress']
    assert history['overflow_retained_recovery'] is None and history['unresolved_category_scopes'] == 1518


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'contract_version': 2}, {'enabled': False},
    {'source_dates_observed': True}, {'verified_pages': '345'}, {'metadata_rows': -1},
    {'public_ID_row_observations': 10219}, {'metadata_rows': 10351}, {'verified_pages': 11},
    {'dates_all_reported_pages_observed': 13}, {'dates_with_changed_source_total': 5},
    {'source_dates_observed': 0}, {'verified_pages': 2**53},
    {'article_bodies_accepted_by_index': True}, {'article_bodies_accepted_by_index': 1},
    {'all_history_complete': True}, {'finish_eta_utc': NOW.isoformat()},
    {'source_total_is_not_unique_story_count': False}, {'category_gaps_preserved': False},
])
def test_unreconciled_index_is_unknown_without_hiding_body_or_history(tmp_path, patch):
    status = setup(tmp_path)
    status['providers']['cnyes']['public_index_progress'].update(patch)
    source = cnyes(save(tmp_path, status))
    assert source['public_index_progress'] is None
    assert source['history_list_progress']['unresolved_category_scopes'] == 1518
    assert source['known_urls'] == 6 and source['complete_urls'] == 2


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'contract_version': 2}, {'current_source_contract_scope_states': None},
    {'current_source_contract_scope_states': {'list_complete': True}},
    {'current_source_contract_scope_states': {'list_complete': -1}},
    {'current_source_contract_scope_states': {'PRIVATE_REASON': 1}},
    {'retired_gap_receipts': True}, {'retired_gap_receipts': '1116'},
    {'list_coverage_is_not_body_coverage': False}, {'all_history_complete': True},
    {'finish_eta_utc': NOW.isoformat()},
])
def test_unreconciled_history_is_unknown_without_hiding_index(tmp_path, patch):
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress'].update(patch)
    source = cnyes(save(tmp_path, status))
    assert source['history_list_progress'] is None
    assert source['public_index_progress']['verified_pages'] == 345


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'contract_version': 2}, {'total_gaps': True}, {'total_gaps': 1},
    {'groups': None}, {'groups': [{'provider': 'cnyes', 'error_code': 'x', 'scopes': True}]},
    {'groups': [{'provider': 'cnyes', 'error_code': 'x', 'scopes': 1518}]},
    {'groups': [{'provider': 'cnyes', 'error_code': 'x', 'scopes': 760}] * 2},
    {'groups': [{'provider': 'PRIVATE_TOKEN/unsafe', 'error_code': 'x', 'scopes': 1520}]},
    {'all_gap_details_preserved': False}, {'historical_completeness_verified': True},
])
def test_inconsistent_full_gap_projection_does_not_fall_back_to_url_sample(tmp_path, patch):
    status = setup(tmp_path)
    status['bulk_gap_projection'].update(patch)
    history = cnyes(save(tmp_path, status))['history_list_progress']
    assert history['verified_list_scopes'] == 4007
    assert history['unresolved_category_scopes'] is None


@pytest.mark.parametrize('activity', ['running', 'stopped', 'draining', 'waiting_disk'])
def test_list_observations_remain_observations_not_live_completion_when_stopped(tmp_path, activity):
    status = setup(tmp_path)
    status['activity'] = activity
    result = save(tmp_path, status)
    source = cnyes(result)
    assert source['public_index_progress']['verified_pages'] == 345
    assert source['public_index_progress']['finish_eta_utc'] is None
    assert result['acquisition']['all_sources_eta_seconds'] is None
    if activity != 'running':
        assert source['conditional_eta_seconds'] is None


def test_legacy_disabled_empty_and_foreign_provider_do_not_fake_completion(tmp_path):
    status = _seed(tmp_path)
    result = save(tmp_path, status)
    assert cnyes(result)['public_index_progress'] is cnyes(result)['history_list_progress'] is None
    status['providers']['unregistered:example.org'].update(public_index_progress=index_snapshot(),
                                                         history_list_progress=history_snapshot())
    status['providers']['cnyes']['public_index_progress'] = {'enabled': False, 'article_bodies_accepted_by_index': 0}
    assert cnyes(save(tmp_path, status))['public_index_progress'] is None
    status['providers']['cnyes']['public_index_progress'] = {**index_snapshot(), **dict.fromkeys((
        'source_dates_observed', 'verified_pages', 'metadata_rows', 'public_ID_row_observations',
        'dates_all_reported_pages_observed', 'dates_with_changed_source_total'), 0)}
    status['providers']['cnyes']['history_list_progress'] = {**history_snapshot(), 'current_source_contract_scope_states': {}}
    status['bulk_gap_projection'] = {**gaps_snapshot(), 'groups': [], 'total_gaps': 0}
    result = save(tmp_path, status)
    source = cnyes(result)
    assert source['public_index_progress']['verified_pages'] == 0
    assert not source['public_index_progress']['all_history_complete']
    assert source['history_list_progress']['verified_list_scopes'] == 0
    assert source['history_list_progress']['unresolved_category_scopes'] == 0
    foreign = next(row for row in result['providers'] if row['id'] == 'unregistered:example.org')
    assert foreign['public_index_progress'] is foreign['history_list_progress'] is None


def test_new_list_projection_never_imports_collector_opens_queue_or_calls_network(tmp_path, monkeypatch):
    status = setup(tmp_path)
    import builtins
    import sqlite3
    import socket
    original_import = builtins.__import__

    def deny_import(name, *args, **kwargs):
        assert not name.startswith('downloader.news_fulltext')
        return original_import(name, *args, **kwargs)

    def deny_access(*args, **kwargs):
        pytest.fail('public list view must not access a queue or publisher')

    monkeypatch.setattr(builtins, '__import__', deny_import)
    monkeypatch.setattr(sqlite3, 'connect', deny_access)
    monkeypatch.setattr(socket, 'create_connection', deny_access)
    assert cnyes(save(tmp_path, status))['public_index_progress']['verified_pages'] == 345


@pytest.fixture(scope='module')
def browser():
    from playwright.sync_api import sync_playwright
    from scripts.probe_browser_runtime import browser_profile_launch_options
    with sync_playwright() as runtime:
        instance = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            yield instance
        finally:
            instance.close()


@pytest.mark.parametrize('width,kind,problem,message', [
    (320, 'overflow', 'unsupported_version', '盤點游標版本不支援，原資料保留，該盤點暫停。'),
    (1440, 'overflow', 'audit_conflict', '盤點游標留存紀錄衝突，原資料保留，該盤點暫停。'),
    (320, 'nonzero', 'unsupported_version', '盤點游標版本不支援，原資料保留，該盤點暫停。'),
    (320, 'accepted', 'unsupported_version', '盤點游標版本不支援，原資料保留，該盤點暫停。'),
    (1440, 'accepted', 'audit_conflict', '盤點游標留存紀錄衝突，原資料保留，該盤點暫停。'),
])
def test_actual_reader_displays_overflow_checkpoint_problem(tmp_path, browser, width, kind, problem, message):
    from playwright.sync_api import expect
    status = setup(tmp_path)
    history = status['providers']['cnyes']['history_list_progress']
    if kind == 'overflow':
        history['overflow_retained_recovery'] = {
            'contract_version': 1, 'scan_state': 'unknown', 'pending_retained_pages': 2,
            'local_raw_only': True, 'category_gap_completion_claimed': False, 'publisher_requests': 0,
            'checkpoint_problem': problem}
    elif kind == 'nonzero':
        history['unobserved_nonzero_retained_scan'] = {
            'contract_version': 1, 'scan_state': 'unknown', 'checked_steps': None, 'queued_candidate_pages': None,
            'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0,
            'checkpoint_problem': problem}
    else:
        history['accepted_body_batch_scan'] = {
            'contract_version': 1, 'scan_state': 'unknown', 'scanned_batches': None, 'indexed_candidates': None,
            'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0,
            'checkpoint_problem': problem}
    save(tmp_path, status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': width, 'height': 900})
    errors, nonlocal_calls = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: nonlocal_calls.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        page.locator('#source-search').fill('cnyes')
        detail = page.locator('[data-news-list-provider="cnyes"]')
        detail.locator('summary').click(timeout=10000)
        selector = {'overflow': '[data-news-overflow-recovery]', 'nonzero': '[data-news-nonzero-local-recovery]',
                    'accepted': '[data-news-accepted-batch-scan]'}[kind]
        expect(detail.locator(selector)).to_contain_text(message)
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert not errors and not nonlocal_calls
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.parametrize('width', [320, 1440])
def test_actual_reader_displays_scoped_legacy_checkpoint_problem(tmp_path, browser, width):
    from playwright.sync_api import expect
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress']['legacy_metadata_scans'] = {
        'mismatch': {'scan_state': 'unknown', 'checkpoint_problem': 'unsupported_version'},
        'unavailable': {'scan_state': 'scanning', 'checkpoint_problem': None},
        'empty': {'scan_state': 'unknown', 'checkpoint_problem': 'audit_conflict'}}
    save(tmp_path, status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': width, 'height': 900})
    errors, nonlocal_calls = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: nonlocal_calls.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        page.locator('#source-search').fill('cnyes')
        detail = page.locator('[data-news-list-provider="cnyes"]')
        detail.locator('summary').click(timeout=10000)
        local = detail.locator('[data-news-legacy-local-scans]')
        expect(local).to_contain_text('原件不符 狀態未知')
        expect(local).to_contain_text('原件缺失 盤點中')
        expect(local).to_contain_text('盤點游標留存紀錄衝突')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert not errors and not nonlocal_calls
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.parametrize('case', ['legacy', 'invalid_index', 'invalid_history', 'observed'])
def test_actual_reader_renders_fixture_missingness_and_keeps_native_details(tmp_path, browser, case):
    """Loopback fixture server; this is not a live provider/UI acceptance."""
    from playwright.sync_api import expect
    status = setup(tmp_path)
    if case == 'legacy':
        status['providers']['cnyes'].pop('public_index_progress')
        status['providers']['cnyes'].pop('history_list_progress')
    elif case == 'invalid_index':
        status['providers']['cnyes']['public_index_progress']['metadata_rows'] = True
    elif case == 'invalid_history':
        status['providers']['cnyes']['history_list_progress']['retired_gap_receipts'] = True
    elif case == 'observed':
        status['providers']['cnyes']['public_index_progress']['dates_reconciling_page_membership'] = 3
        status['providers']['cnyes']['public_index_progress'].update(
            checkpoint_contract_version=1, pending_local_originals=2,
            next_local_recovery_at_utc='1970-01-01T00:00:00+00:00', local_recovery_ready=True)
    save(tmp_path, status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': 320, 'height': 900})
    errors, nonlocal_calls = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: nonlocal_calls.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        page.locator('#source-search').fill('cnyes')
        detail = page.locator('[data-news-list-provider="cnyes"]')
        expect(detail).to_have_count(1)
        detail.locator('summary').click(timeout=10000)
        expect(detail).to_have_attribute('open', '')
        expect(detail.locator('[data-news-list-eta]')).to_contain_text('完成時間未知')
        assert detail.locator('progress').count() == 0
        if case in ('legacy', 'invalid_index'):
            expect(detail).to_have_attribute('data-news-index-state', 'unavailable')
            expect(detail).to_contain_text('不補零、不算完成')
            expect(detail.locator('[data-news-index-count]')).to_have_count(0)
        else:
            expect(detail).to_have_attribute('data-news-index-state', 'observed')
            expect(detail.locator('[data-news-index-count="pages"]')).to_have_text('345')
            expect(detail.locator('[data-news-index-count="rows"]')).to_have_text('10,218')
            if case == 'observed':
                expect(detail.locator('[data-news-index-count="revision_dates"]')).to_have_text('3')
                expect(detail.locator('[data-news-index-checkpoint]')).to_contain_text('2 個原件待核驗')
                expect(detail.locator('[data-news-index-checkpoint]')).to_contain_text('不是缺少的文章數')
        if case in ('legacy', 'invalid_history'):
            expect(detail).to_contain_text('缺口數未知，不視為已補齊')
            expect(detail.locator('[data-news-history-count]')).to_have_count(0)
        else:
            expect(detail.locator('[data-news-history-count="gaps"]')).to_have_text('1,518')
            expect(detail.locator('[data-news-legacy-local-scans]')).to_contain_text('快照未提供，狀態未知')
        page.locator('#source-search').fill('no-matching-source')
        expect(detail).to_have_count(0)
        page.locator('#source-search').fill('cnyes')
        expect(detail).to_have_attribute('open', '')
        detail.locator('summary').focus()
        page.keyboard.press('Enter')
        expect(detail).not_to_have_attribute('open', '')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert not errors and not nonlocal_calls
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
