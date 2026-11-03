from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from http.client import HTTPConnection
import hashlib
import json
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import serve_public_dashboards as gateway
from stockagent.live.news_dashboard import build_news_public_status, news_snapshot_revision


NOW = datetime(2026, 10, 8, 15, tzinfo=UTC)
REPO = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('source,version,value,total,expected', [
    ('finmind', 5, 0, 0, 0), ('finmind', 5, 1, 3, 1), ('finmind', 5, 1, 0, None),
    ('finmind', 4, 0, 0, None), ('finmind', 5, True, 1, None), ('finmind', 5, -1, 1, None),
    ('finmind', True, 1, 1, None), ('finmind', 5, None, 1, None),
    ('finmind', 6, 1, 1, 1), ('finlab', 5, 0, 0, None), ('finlab', 6, 1, 2, 1),
    ('finlab', 6, 2, 1, None), ('finlab', 6, False, 1, None),
    ('finmind', 7, 1, 2, 1), ('finlab', 7, 1, 2, 1),
])
def test_finmind_catalog_error_projection_is_scoped_and_validated(tmp_path, source, version, value, total, expected):
    status = _seed(tmp_path)
    status['metadata_inventory'] = {'contract_version': version, source + '_catalog_errors': value,
                                    'private_path': '/PRIVATE_FINMIND_PATH'}
    status['metadata_partition_errors'] = total
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['acquisition'][source + '_catalog_errors'] == expected
    assert 'PRIVATE_FINMIND_PATH' not in json.dumps(result)


def test_source_catalog_errors_do_not_exceed_total_metadata_errors(tmp_path):
    status = _seed(tmp_path)
    status['metadata_inventory'] = {'contract_version': 6, 'finlab_catalog_errors': 1, 'finmind_catalog_errors': 1}
    status['metadata_partition_errors'] = 1
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['acquisition']['finlab_catalog_errors'] is None
    assert result['acquisition']['finmind_catalog_errors'] is None


@pytest.mark.parametrize('version,pending,expected_count,expected_pending', [
    (6, None, 7, None), (7, 0, 7, 0), (7, 2, None, 2),
    (7, None, None, None), (7, True, None, None), (7, -1, None, None),
    (8, 0, None, None),
])
def test_metadata_alias_pending_masks_unverified_dashboard_counts(tmp_path, version, pending,
                                                                  expected_count, expected_pending):
    status = _seed(tmp_path)
    status['metadata_inventory'] = {'contract_version': version, 'receipt_alias_retargets_pending': pending}
    status['metadata_partitions'] = 7
    status['metadata_association_rows'] = 7
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    acquisition = build_news_public_status(tmp_path, now=NOW)['acquisition']
    assert acquisition['metadata_partitions'] == expected_count
    assert acquisition['metadata_association_rows'] == expected_count
    assert acquisition['metadata_selection_pending'] == expected_pending


def local_replay_sample():
    return {'contract_version': 1, 'current_source_contract_only': True,
        'retry_records_are_article_counts': False, 'entries': {
            'api_pages': {'records': 3, 'earliest_not_before_utc': NOW.isoformat(), 'path': 'PRIVATE_PATH'},
            'metadata_batches': {'records': 0, 'earliest_not_before_utc': None},
            'diagnostic_originals': None}, 'error': 'PRIVATE_ERROR'}


def metadata_resource_sample():
    return {'contract_version': 1, 'current_source_contract_only': True,
            'records_are_article_counts': False, 'records': 2, 'above_current_bound_records': 1,
            'current_bound_bytes': 32 << 20, 'max_observed_required_bytes': 64 << 20,
            'path': 'PRIVATE_RESOURCE_PATH'}


def metadata_large_sample():
    return {'contract_version': 1, 'current_source_contract_only': True,
            'retry_records_are_article_completion': False, 'assigned': True, 'retry_records': 2,
            'earliest_not_before_utc': NOW.isoformat(), 'path': 'PRIVATE_LARGE_SOURCE'}


def test_large_replay_projection_is_private_scoped_and_not_completion(tmp_path):
    status = _seed(tmp_path)
    status['cnyes_metadata_large_replay'] = metadata_large_sample()
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['metadata_large_replay'] == {key: value for key, value in metadata_large_sample().items() if key != 'path'}
    assert all(row['metadata_large_replay'] is None for row in result['providers'] if row['id'] != 'cnyes')
    assert not result['scope']['publisher_calls_on_page_view'] and 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'current_source_contract_only': False},
    {'retry_records_are_article_completion': True}, {'assigned': 1},
    {'retry_records': True}, {'retry_records': -1}, {'retry_records': 2**53},
    {'earliest_not_before_utc': None}, {'earliest_not_before_utc': 'invalid'},
])
def test_invalid_or_missing_large_replay_snapshot_is_unknown(tmp_path, patch):
    status = _seed(tmp_path)
    status['cnyes_metadata_large_replay'] = {**metadata_large_sample(), **patch}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['metadata_large_replay'] is None
    del status['cnyes_metadata_large_replay']
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['metadata_large_replay'] is None


def metadata_index_sample():
    return {'contract_version': 1, 'current_source_contract_only': True,
            'counts_are_article_completion': False, 'state': 'scanning', 'rows_seen': 123,
            'registered_heads': 4, 'dirty_processed': 2, 'cursor_resets': 1,
            'invalid_heads_observed': 1,
            'updated_at_utc': NOW.isoformat(), 'source_fingerprint': 'PRIVATE_INDEX_SOURCE'}


def test_metadata_index_projection_is_private_scoped_and_not_article_progress(tmp_path):
    status = _seed(tmp_path)
    status['cnyes_metadata_index_progress'] = metadata_index_sample()
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['metadata_index_progress'] == {
        key: value for key, value in metadata_index_sample().items() if key != 'source_fingerprint'}
    assert all(row['metadata_index_progress'] is None for row in result['providers'] if row['id'] != 'cnyes')
    assert result['acquisition']['known_urls'] == 10 and result['acquisition']['stored_body_articles'] == 2
    assert not result['scope']['publisher_calls_on_page_view'] and 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'current_source_contract_only': False},
    {'counts_are_article_completion': True}, {'state': 'done'}, {'rows_seen': -1},
    {'registered_heads': True}, {'dirty_processed': 2**53}, {'cursor_resets': None},
    {'invalid_heads_observed': -1},
    {'updated_at_utc': None}, {'updated_at_utc': 'invalid'},
])
def test_invalid_or_missing_metadata_index_snapshot_is_unknown(tmp_path, patch):
    status = _seed(tmp_path)
    status['cnyes_metadata_index_progress'] = {**metadata_index_sample(), **patch}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['metadata_index_progress'] is None
    del status['cnyes_metadata_index_progress']
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['metadata_index_progress'] is None


def test_metadata_resource_projection_is_private_scoped_and_not_article_progress(tmp_path):
    status = _seed(tmp_path)
    status['cnyes_metadata_resource_waits'] = metadata_resource_sample()
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['metadata_resource_waits'] == {
        key: value for key, value in metadata_resource_sample().items() if key != 'path'}
    assert all(row['metadata_resource_waits'] is None for row in result['providers'] if row['id'] != 'cnyes')
    assert result['acquisition']['known_urls'] == 10 and result['acquisition']['stored_body_articles'] == 2
    assert not result['scope']['publisher_calls_on_page_view'] and 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'current_source_contract_only': False},
    {'records_are_article_counts': True}, {'records': -1}, {'records': True},
    {'records': 2**53}, {'above_current_bound_records': 3},
    {'above_current_bound_records': 0}, {'current_bound_bytes': 0},
    {'max_observed_required_bytes': None}, {'max_observed_required_bytes': 1},
])
def test_invalid_or_missing_metadata_resource_snapshot_is_unknown(tmp_path, patch):
    status = _seed(tmp_path)
    status['cnyes_metadata_resource_waits'] = {**metadata_resource_sample(), **patch}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['metadata_resource_waits'] is None
    del status['cnyes_metadata_resource_waits']
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['metadata_resource_waits'] is None


def test_local_replay_projection_is_private_scoped_and_not_article_progress(tmp_path):
    status = _seed(tmp_path)
    status['cnyes_local_replay_retries'] = local_replay_sample()
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    entries = source['local_replay_retries']['entries']
    assert entries == {'api_pages': {'records': 3, 'earliest_not_before_utc': NOW.isoformat()},
        'metadata_batches': {'records': 0, 'earliest_not_before_utc': None}, 'diagnostic_originals': None}
    assert all(row['local_replay_retries'] is None for row in result['providers'] if row['id'] != 'cnyes')
    assert result['acquisition']['known_urls'] == 10 and result['acquisition']['stored_body_articles'] == 2
    assert not result['scope']['publisher_calls_on_page_view'] and 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('kind,value', [
    ('contract_version', True), ('current_source_contract_only', False),
    ('retry_records_are_article_counts', True), ('entries', {}),
    ('api_pages', {'records': True, 'earliest_not_before_utc': NOW.isoformat()}),
    ('api_pages', {'records': -1, 'earliest_not_before_utc': NOW.isoformat()}),
    ('api_pages', {'records': 2**53, 'earliest_not_before_utc': NOW.isoformat()}),
    ('api_pages', {'records': 3, 'earliest_not_before_utc': None}),
    ('api_pages', {'records': 3, 'earliest_not_before_utc': 'invalid'}),
    ('api_pages', {'records': 3, 'earliest_not_before_utc': '0001-01-01T00:00:00+23:59'}),
    ('api_pages', {'records': 0, 'earliest_not_before_utc': NOW.isoformat()}),
    ('api_pages', {'records': 0}),
])
def test_invalid_or_missing_local_replay_snapshot_is_unknown(tmp_path, kind, value):
    status = _seed(tmp_path)
    snapshot = local_replay_sample()
    if kind == 'api_pages':
        snapshot['entries'][kind] = value
    else:
        snapshot[kind] = value
    status['cnyes_local_replay_retries'] = snapshot
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['local_replay_retries'] is None
    del status['cnyes_local_replay_retries']
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['local_replay_retries'] is None


def disk_sample(*, free=19, required=25):
    return {'contract_version': 1, 'checked_at_utc': NOW.isoformat(), 'free_bytes': free,
        'required_free_bytes': required, 'shortfall_bytes': max(0,required-free),
        'fetch_allowed': free >= required, 'automatic_recheck_seconds': 60,
        'path': 'PRIVATE_STORAGE_PATH', 'token': 'PRIVATE_DISK_TOKEN'}


def test_url_inbox_projection_is_bounded_private_and_separate_from_articles(tmp_path):
    status = _seed(tmp_path)
    status['url_metadata_inbox'] = {'contract_version': 1, 'available': True,
        'request_files': 2, 'pending_requests': 2, 'targets_unexamined': 3,
        'completed_unarchived': 0, 'invalid_or_unreadable': 0,
        'url': 'PRIVATE_URL', 'token': 'PRIVATE_TOKEN'}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['url_metadata_inbox'] == {'contract_version': 1, 'request_files': 2,
        'pending_requests': 2, 'targets_unexamined': 3, 'completed_unarchived': 0,
        'invalid_or_unreadable': 0}
    assert result['acquisition']['known_urls'] == 10 and result['acquisition']['stored_body_articles'] == 2
    assert 'PRIVATE_' not in json.dumps(result)


def finlab_cnyes_priority_sample():
    return {'contract_version': 1, 'raw_unique_delta': 10, 'raw_hints_examined': 10,
        'targets': 3, 'request_state': 'processing', 'owner_checked': 2,
        'recorded_at_utc': NOW.isoformat(),
        'outcomes': {'metadata_registered': 1, 'existing_head_preserved': 1,
                     'source_contract_changed': 0, 'other': 0},
        'body_completion_proven': False, 'association_completion_proven': False,
        'request_id': 'PRIVATE_REQUEST', 'url': 'PRIVATE_URL'}


def test_finlab_cnyes_priority_projection_is_bounded_and_not_body_progress(tmp_path):
    status = _seed(tmp_path)
    status['finlab_cnyes_priority'] = finlab_cnyes_priority_sample()
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    priority = result['finlab_cnyes_priority']
    assert priority['targets'] == 3 and priority['owner_checked'] == 2
    assert priority['outcomes'] == {'metadata_registered': 1, 'existing_head_preserved': 1,
                                    'source_contract_changed': 0, 'other': 0}
    assert not priority['body_completion_proven'] and not priority['association_completion_proven']
    assert result['acquisition']['known_urls'] == 10 and result['acquisition']['stored_body_articles'] == 2
    assert 'PRIVATE_' not in json.dumps(result)


def test_finlab_cnyes_priority_projects_unverified_origin_as_subset(tmp_path):
    status = _seed(tmp_path)
    priority = finlab_cnyes_priority_sample()
    priority.update(unverified_origin=1, outcomes={'metadata_registered': 0, 'existing_head_preserved': 1,
        'source_contract_changed': 0, 'other': 1})
    status['finlab_cnyes_priority'] = priority
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    projected = build_news_public_status(tmp_path, now=NOW)['finlab_cnyes_priority']
    assert projected['unverified_origin'] == 1 and projected['outcomes']['other'] == 1
    assert sum(projected['outcomes'].values()) == projected['owner_checked']
    assert 'PRIVATE_' not in json.dumps(projected)


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'raw_unique_delta': -1}, {'raw_hints_examined': 9},
    {'targets': 513}, {'request_state': 'checked'}, {'request_state': []}, {'owner_checked': True},
    {'recorded_at_utc': 'bad'}, {'recorded_at_utc': (NOW + timedelta(minutes=1)).isoformat()},
    {'owner_checked': 3}, {'outcomes': {'metadata_registered': 2}},
    {'outcomes': {'metadata_registered': 1, 'existing_head_preserved': 1,
                  'source_contract_changed': 0, 'other': 0, 'unexpected': 99}},
    {'body_completion_proven': True}, {'association_completion_proven': True},
    {'unverified_origin': True}, {'unverified_origin': -1}, {'unverified_origin': 1},
])
def test_finlab_cnyes_priority_inconsistent_projection_is_unknown(tmp_path, patch):
    status = _seed(tmp_path)
    status['finlab_cnyes_priority'] = {**finlab_cnyes_priority_sample(), **patch}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    assert build_news_public_status(tmp_path, now=NOW)['finlab_cnyes_priority'] is None


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'available': False}, {'request_files': 65},
    {'request_files': 3}, {'pending_requests': -1}, {'targets_unexamined': True},
    {'targets_unexamined': 1}, {'targets_unexamined': 1025},
])
def test_invalid_or_old_url_inbox_projection_is_unknown(tmp_path, patch):
    status = _seed(tmp_path)
    status['url_metadata_inbox'] = {'contract_version': 1, 'available': True,
        'request_files': 2, 'pending_requests': 2, 'targets_unexamined': 3,
        'completed_unarchived': 0, 'invalid_or_unreadable': 0, **patch}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    assert build_news_public_status(tmp_path, now=NOW)['url_metadata_inbox'] is None
    del status['url_metadata_inbox']
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    assert build_news_public_status(tmp_path, now=NOW)['url_metadata_inbox'] is None


def candidate_work_sample():
    return {'contract_version': 1, 'available': True, 'request_files': 3,
        'pending_unadmitted': 1, 'already_registered': 1, 'invalid_or_unreadable': 1,
        'jobs': {'ready': 2, 'reserved': 0, 'orphaned_reservation': 1, 'uncertain_outcome': 1},
        'providers': {'cnyes': {'pending_unadmitted': 1, 'ready': 1, 'reserved': 0,
                                'orphaned_reservation': 1, 'uncertain_outcome': 1,
                                'url': 'PRIVATE_URL'}},
        'unregistered': {'pending_unadmitted': 0, 'ready': 1, 'reserved': 0,
                         'orphaned_reservation': 0, 'uncertain_outcome': 0,
                         'body': 'PRIVATE_BODY'}, 'token': 'PRIVATE_TOKEN'}


def test_candidate_work_projection_is_private_and_separate_from_article_progress(tmp_path):
    status = _seed(tmp_path)
    status['candidate_diagnostic_progress'] = candidate_work_sample()
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    work = result['candidate_diagnostic_progress']
    assert work['request_files'] == 3 and work['pending_unadmitted'] == 1
    assert work['jobs'] == {'ready': 2, 'reserved': 0, 'orphaned_reservation': 1, 'uncertain_outcome': 1}
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['candidate_diagnostic_work'] == work['providers']['cnyes']
    assert result['acquisition']['known_urls'] == 10 and result['acquisition']['stored_body_articles'] == 2
    assert not result['scope']['publisher_calls_on_page_view']
    assert 'PRIVATE_' not in json.dumps(result)


def test_retained_handoff_projection_separates_private_plan_from_runtime(tmp_path):
    from stockagent.live.news_dashboard import _retained_handoff_projection
    path = tmp_path / 'artifacts/private/certificate.json'
    path.parent.mkdir(parents=True)
    payload = json.dumps({'contract': 'cmnews_retained_link_handoff_v1',
        'source_root': str((tmp_path / 'data_news_fulltext').resolve()),
        'candidate_count': 3, 'requests': [{'url': 'PRIVATE_URL'}],
        'target_identity_or_body_verified': False, 'private_path': 'PRIVATE_PATH'}).encode()
    path.write_bytes(payload)
    path.chmod(0o600)
    config = {'root': 'data_news_fulltext', 'retained_link_handoff': {
        'path': 'artifacts/private/certificate.json', 'sha256': hashlib.sha256(payload).hexdigest()}}
    runtime = {'contract_version': 1, 'provider': 'cmnews', 'state': 'submitted', 'candidates': 3,
               'requests': 1, 'submitted_requests': 1, 'reason': None, 'body_verified': False,
               'plan_sha256': config['retained_link_handoff']['sha256'],
               'private': 'PRIVATE_RUNTIME'}
    result = _retained_handoff_projection(tmp_path, config, runtime)
    assert result['plan'] == {'provider': 'cmnews', 'candidates': 3, 'requests': 1,
                              'source_and_policy_revalidation_required': True, 'body_verified': False}
    assert result['runtime']['state'] == 'submitted' and not result['runtime']['body_verified']
    assert 'PRIVATE_' not in json.dumps(result)
    path.write_bytes(payload + b'changed')
    assert _retained_handoff_projection(tmp_path, config, runtime)['plan'] is None
    assert _retained_handoff_projection(tmp_path, config, {**runtime, 'submitted_requests': 2})['runtime'] is None


def test_candidate_local_retry_projection_preserves_reason_without_private_payload(tmp_path):
    status = _seed(tmp_path)
    status['candidate_diagnostic_progress'] = {**candidate_work_sample(),
        'local_retry': {'request_io': 1, 'quarantine': 0, 'archive': 1, 'path': 'PRIVATE_RETRY_PATH'}}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    work = build_news_public_status(tmp_path, now=NOW)['candidate_diagnostic_progress']
    assert work['local_retry'] == {'request_io': 1, 'quarantine': 0, 'archive': 1}
    assert work['pending_unadmitted'] == 1 and 'PRIVATE_' not in json.dumps(work)


@pytest.mark.parametrize('retry', [
    {'request_io': True, 'quarantine': 0, 'archive': 0},
    {'request_io': -1, 'quarantine': 0, 'archive': 0},
    {'request_io': 2, 'quarantine': 1, 'archive': 1},
    {'request_io': 1, 'quarantine': 0},
    'PRIVATE_RETRY_PATH',
])
def test_invalid_candidate_local_retry_is_unknown_without_hiding_other_progress(tmp_path, retry):
    status = _seed(tmp_path)
    status['candidate_diagnostic_progress'] = {**candidate_work_sample(), 'local_retry': retry}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    work = build_news_public_status(tmp_path, now=NOW)['candidate_diagnostic_progress']
    assert work['local_retry'] is None and work['pending_unadmitted'] == 1
    assert 'PRIVATE_' not in json.dumps(work)


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'available': False}, {'request_files': 17},
    {'request_files': 2}, {'pending_unadmitted': True}, {'invalid_or_unreadable': -1},
    {'jobs': {'ready': 2, 'reserved': 0, 'orphaned_reservation': 0, 'uncertain_outcome': 1}},
    {'providers': {'private_source': {'pending_unadmitted': 1, 'ready': 1, 'reserved': 0,
                                     'orphaned_reservation': 1, 'uncertain_outcome': 1}}},
    {'unregistered': {'pending_unadmitted': 0, 'ready': -1, 'reserved': 0,
                      'orphaned_reservation': 0, 'uncertain_outcome': 0}},
])
def test_invalid_or_missing_candidate_work_remains_unknown(tmp_path, patch):
    status = _seed(tmp_path)
    status['candidate_diagnostic_progress'] = {**candidate_work_sample(), **patch}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['candidate_diagnostic_progress'] is None
    assert next(row for row in result['providers'] if row['id'] == 'cnyes')['candidate_diagnostic_work'] is None
    del status['candidate_diagnostic_progress']
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    assert build_news_public_status(tmp_path, now=NOW)['candidate_diagnostic_progress'] is None


def test_disk_constraint_outlives_inventory_phase_and_clears_on_real_sample(tmp_path):
    status = _seed(tmp_path)
    status.update(activity='inventorying', disk_admission=disk_sample())
    path = tmp_path/'data_news_fulltext/status.json'
    atomic_write_json(path,status)
    result = build_news_public_status(tmp_path,now=NOW)
    source = next(row for row in result['providers'] if row['id']=='cnyes')
    assert result['health']=='waiting_disk' and result['worker']['source_activity']=='inventorying'
    assert result['worker']['disk_admission']['shortfall_bytes']==6
    assert source['state']=='waiting_disk' and source['conditional_eta_seconds'] is None
    assert result['acquisition']['stored_body_articles']==2 and result['acquisition']['known_urls']==10
    assert 'PRIVATE_STORAGE_PATH' not in json.dumps(result) and 'PRIVATE_DISK_TOKEN' not in json.dumps(result)
    status['disk_admission']=disk_sample(free=25)
    atomic_write_json(path,status)
    result = build_news_public_status(tmp_path,now=NOW)
    source = next(row for row in result['providers'] if row['id']=='cnyes')
    assert result['health']=='inventorying' and source['state']=='backfilling'
    assert result['worker']['disk_admission']['fetch_allowed'] and source['conditional_eta_seconds'] is not None


def unknown_backing_sample(state='pending', *, inflight=True):
    return {**disk_sample(), 'free_bytes': None, 'shortfall_bytes': None,
            'backing_free_bytes': None, 'fetch_allowed': False,
            'physical_backing_state': state, 'physical_probe_inflight': inflight}


@pytest.mark.parametrize(('state', 'inflight'), [
    ('pending', True), ('unavailable', False), ('unavailable', True), ('stale', True),
])
def test_unknown_backing_explains_wait_without_fabricating_capacity(tmp_path, state, inflight):
    status = _seed(tmp_path)
    status.update(activity='inventorying', disk_admission=unknown_backing_sample(state, inflight=inflight))
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id']=='cnyes')
    assert result['health'] == 'waiting_disk' and result['worker']['source_activity'] == 'inventorying'
    assert result['worker']['disk_admission'] is None
    assert result['worker']['disk_verification'] == {
        'contract_version': 1, 'state': state, 'probe_inflight': inflight,
        'checked_at_utc': NOW.isoformat(), 'required_free_bytes': 25, 'automatic_recheck_seconds': 60}
    assert result['acquisition']['stored_body_articles'] == 2
    assert result['acquisition']['eligible_eta_seconds'] is None and source['conditional_eta_seconds'] is None
    assert 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('patch', [
    {'physical_backing_state': 'invented'}, {'physical_backing_state': {}}, {'physical_probe_inflight': False},
    {'physical_probe_inflight': 1}, {'fetch_allowed': True}, {'free_bytes': 25},
    {'shortfall_bytes': 0}, {'backing_free_bytes': 0}, {'required_free_bytes': True},
    {'automatic_recheck_seconds': 0}, {'contract_version': True},
    {'checked_at_utc': (NOW-timedelta(seconds=181)).isoformat()},
    {'checked_at_utc': (NOW+timedelta(seconds=10)).isoformat()},
])
def test_invalid_unknown_backing_still_cannot_restore_live_forecast(tmp_path, patch):
    status = _seed(tmp_path)
    status.update(activity='inventorying', disk_admission={**unknown_backing_sample(), **patch})
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['health'] == 'unavailable'
    assert result['worker']['disk_admission'] is None and result['worker']['disk_verification'] is None
    assert result['acquisition']['eligible_eta_seconds'] is None


@pytest.mark.parametrize('patch', [
    {'free_bytes':True}, {'required_free_bytes':-1}, {'shortfall_bytes':0},
    {'free_bytes':None, 'shortfall_bytes':None, 'fetch_allowed':False},
    {'fetch_allowed':True}, {'contract_version':True}, {'automatic_recheck_seconds':0},
    {'checked_at_utc':(NOW-timedelta(seconds=181)).isoformat()},
    {'checked_at_utc':(NOW+timedelta(seconds=10)).isoformat()},
])
def test_invalid_or_stale_disk_sample_cannot_restore_a_live_forecast(tmp_path,patch):
    status = _seed(tmp_path)
    status.update(activity='inventorying',disk_admission={**disk_sample(),**patch})
    atomic_write_json(tmp_path/'data_news_fulltext/status.json',status)
    result = build_news_public_status(tmp_path,now=NOW)
    assert result['health']=='unavailable' and result['worker']['disk_admission'] is None
    assert next(row for row in result['providers'] if row['id']=='cnyes')['conditional_eta_seconds'] is None


@pytest.mark.parametrize(('age', 'available'), [(89, True), (90, True), (91, False)])
def test_backing_sample_projection_expires_at_the_owners_deadline(tmp_path, age, available):
    status = _seed(tmp_path)
    status.update(activity='waiting_next_due', disk_admission={**disk_sample(free=30),
        'checked_at_utc': (NOW-timedelta(seconds=age)).isoformat(),
        'physical_backing_state': 'available', 'physical_probe_inflight': True})
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert (result['worker']['disk_admission'] is not None) is available
    assert result['health'] == ('waiting' if available else 'unavailable')
    if not available:
        assert result['acquisition']['eligible_eta_seconds'] is None


def test_native_disk_sample_retains_the_general_heartbeat_expiry(tmp_path):
    status = _seed(tmp_path)
    status['disk_admission'] = {**disk_sample(free=30), 'physical_backing_state': 'not_required',
                               'checked_at_utc': (NOW-timedelta(seconds=120)).isoformat()}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    assert build_news_public_status(tmp_path, now=NOW)['worker']['disk_admission']['fetch_allowed']


def test_stopped_owner_is_not_reclassified_as_a_disk_wait(tmp_path):
    status = _seed(tmp_path)
    status.update(activity='stopped',disk_admission=disk_sample())
    atomic_write_json(tmp_path/'data_news_fulltext/status.json',status)
    assert build_news_public_status(tmp_path,now=NOW)['health']=='stopped'


def test_cna_authorization_setup_is_separate_from_disabled_html_and_secrets(tmp_path):
    status = _seed(tmp_path)
    status['providers']['cna'] = {'states': {'awaiting_authorization': 3},
                                'automatic_collection_enabled': False}
    status['states']['awaiting_authorization'] += 3
    status['unique_articles'] += 3
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    config_path = tmp_path / 'configs/news/cna_authorized_api.json'
    setup = json.loads((REPO / 'configs/news/cna_authorized_api.json').read_text())
    atomic_write_json(config_path, {**setup, 'api_key': 'PRIVATE_CNA_KEY', 'description': 'PRIVATE_BODY'})
    revision = news_snapshot_revision(tmp_path)
    result = build_news_public_status(tmp_path, now=NOW)
    cna = next(source for source in result['providers'] if source['id'] == 'cna')
    assert cna['state'] == 'authorized_api_pending' and not cna['enabled']
    assert cna['known_urls'] == 3 and cna['complete_urls'] == 0
    assert cna['states'] == {'awaiting_authorization': 3}
    assert cna['conditional_eta_seconds'] is None
    assert not cna['authorized_api_setup']['connection_verified']
    assert cna['authorized_api_setup']['user_authorization_declared'] is True
    assert 'PRIVATE_CNA_KEY' not in json.dumps(result) and 'PRIVATE_BODY' not in json.dumps(result)
    atomic_write_json(config_path, {**setup, 'user_authorization_declared': False})
    assert news_snapshot_revision(tmp_path) != revision
    cna = next(source for source in build_news_public_status(tmp_path, now=NOW)['providers'] if source['id'] == 'cna')
    assert cna['state'] == 'not_enabled' and cna['authorized_api_setup'] is None


def test_browser_required_api_phase_never_weakens_enabled_collector_acceptance():
    from scripts.verify_news_dashboard import required_source_role
    cna = {'configured': True, 'enabled': False, 'state': 'authorized_api_pending',
           'conditional_eta_seconds': None, 'authorized_api_setup': {
               'user_authorization_declared': True, 'phase': 'capability_discovery_only',
               'connection_verified': False, 'quota_verified': False, 'fulltext_pipeline_enabled': False}}
    assert required_source_role(cna, api_setup=True) == 'authorized_api_setup_only'
    with pytest.raises(AssertionError):
        required_source_role(cna)
    with pytest.raises(AssertionError):
        required_source_role({**cna, 'enabled': True}, api_setup=True)
    with pytest.raises(AssertionError):
        required_source_role({**cna, 'conditional_eta_seconds': 0}, api_setup=True)
    assert required_source_role({'configured': True, 'enabled': True}) == 'enabled_collector'


def _seed(root: Path, *, now: datetime = NOW) -> dict:
    status = {
        "contract_version": 1, "updated_at_utc": now.isoformat(), "activity": "running",
        "unique_articles": 10, "articles_with_stored_body": 2,
        "states": {"complete": 2, "pending": 4, "awaiting_authorization": 4},
        "accepted_text_bytes": 1200,
        "http_requests_this_run": {"cnyes": 12}, "http_response_bytes_this_run": 4096,
        "providers": {
            "cnyes": {"states": {"complete": 2, "pending": 4}, "text_bytes": 1200,
                      "automatic_collection_enabled": True, "authorization_kind": "personal_research"},
            "unregistered:example.org": {"states": {"awaiting_authorization": 4}},
        },
        "performance": {"owner_elapsed_seconds": 120, "owner_cpu_seconds": 6,
                        "providers": {"cnyes": {"counts": {"durably_accepted_versions": 12, "http_requests_started": 12},
                                                "stages": {"transport": {"count": 12, "wall_seconds": 90, "cpu_seconds": 1}},
                                                "minimum_request_start_gap_seconds": 2.01}},
                        "stages": {"parquet_compress_fsync": {"count": 1, "wall_seconds": 2, "cpu_seconds": 1}}},
        "token": "PRIVATE_TOKEN", "html": "PRIVATE_HTML", "body": "PRIVATE_BODY",
        "bulk_api_endpoints": [{"base_url": "https://example.org/?api_key=PRIVATE_TOKEN"}],
        "provider_next_attempt_utc": {}, "all_sources_eta_seconds": 1,
    }
    atomic_write_json(root / "data_news_fulltext/status.json", status)
    atomic_write_json(root / "configs/news_fulltext.json", {
        "providers": [{"id": "cnyes", "name": "鉅亨", "enabled": True,
                       "interval_seconds": 2, "parallelism": 2, "api_key": "PRIVATE_TOKEN"}],
        "secret": "PRIVATE_TOKEN",
    })
    return status


def _discovery_error_snapshot(root: Path, *, now: datetime = NOW):
    status = _seed(root, now=now)
    status['discovery_errors'] = 3
    status['discovery_error_rollup'] = {
        'contract_version': 1, 'state': 'ready', 'scanned_rows': 400,
        'active_errors': 3, 'providers': {
            'cnyes': {'active_errors': 2, 'kinds': {'cnyes_news': 2}, 'reasons': {'http_404': 2},
                      'private_url': 'PRIVATE_URL', 'raw_error': 'PRIVATE_TOKEN'},
            'globalvoices': {'active_errors': 1, 'kinds': {'rss_fulltext': 1}, 'reasons': {'invalid_feed': 1}},
        }, 'private_rows': ['PRIVATE_BODY'],
    }
    atomic_write_json(root / 'data_news_fulltext/status.json', status)
    return status


def test_discovery_error_rollup_is_per_provider_and_preserves_snapshot_semantics(tmp_path):
    _discovery_error_snapshot(tmp_path)
    result = build_news_public_status(tmp_path, now=NOW)
    rows = {row['id']: row for row in result['providers']}
    assert result['discovery_error_rollup'] == {'state': 'ready', 'scanned_rows': 400, 'active_errors': 3}
    assert rows['cnyes']['discovery_errors'] == {'active_errors': 2, 'kinds': {'cnyes_news': 2}, 'reasons': {'http_404': 2}}
    assert rows['globalvoices']['discovery_errors']['active_errors'] == 1
    assert rows['unregistered:example.org']['discovery_errors']['active_errors'] == 0
    assert rows['cnyes']['state'] == 'backfilling'  # A page error is not a provider-wide stop.
    assert 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize(('value', 'expected'), [
    (None, None), (0, 0), (2, 2), (True, None), (-1, None), ('2', None), (2**53, None),
])
def test_local_sqlite_busy_events_are_scoped_and_validated(tmp_path, value, expected):
    status = _seed(tmp_path)
    counts = status['performance']['providers']['cnyes']['counts']
    if value is not None:
        counts['local_sqlite_busy_commit_events'] = value
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    sources = {source['id']: source for source in result['providers']}
    assert sources['cnyes']['owner_local_sqlite_busy_commit_events'] == expected
    assert sources['unregistered:example.org']['owner_local_sqlite_busy_commit_events'] is None
    assert sources['cnyes']['state'] == 'backfilling'
    assert 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('variant', ['old', 'building', 'bad_total', 'unknown_reason', 'bad_provider', 'bad_count'])
def test_discovery_error_old_partial_or_invalid_snapshots_never_become_zero(tmp_path, variant):
    status = _discovery_error_snapshot(tmp_path)
    rollup = status['discovery_error_rollup']
    if variant == 'old':
        del status['discovery_error_rollup']
    elif variant == 'building':
        rollup.update(state='building', active_errors=None, providers=None)
    elif variant == 'bad_total':
        rollup['active_errors'] = 2
    elif variant == 'unknown_reason':
        rollup['providers']['cnyes']['reasons'] = {'PRIVATE_TOKEN': 2}
    elif variant == 'bad_provider':
        rollup['providers']['PRIVATE_TOKEN'] = rollup['providers'].pop('cnyes')
    else:
        rollup['providers']['cnyes']['active_errors'] = 1
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    row = next(item for item in result['providers'] if item['id'] == 'cnyes')
    assert row['discovery_errors'] is None
    assert result['discovery_error_rollup'] is None or result['discovery_error_rollup']['state'] == 'building'
    assert 'PRIVATE_' not in json.dumps(result)


def _resolution_snapshot():
    return {'contract_version': 1, 'known_redirect_urls': 1000, 'examined_urls': 500,
            'remaining_urls': 999999, 'counts': {'literal_original': 100, 'opaque_original': 400},
            'literal_links_to_queued_targets': 80, 'owner_examined_urls': 500,
            'owner_elapsed_seconds': 100, 'conditional_eta_seconds': 1,
            'original_url': 'https://private.test/?api_key=PRIVATE_TOKEN', 'body': 'PRIVATE_BODY'}


def _metadata_feed_snapshot(tmp_path):
    status = _seed(tmp_path)
    progress = {'contract_version': 1, 'enabled': True, 'registered_feeds': 14, 'verified_feeds': 2,
        'latest_feed_item_observations': 80, 'latest_feed_response_bytes': 40000,
        'feeds_with_errors': 0, 'poll_interval_seconds': 300,
        'next_check_at_utc': (NOW + timedelta(minutes=5)).isoformat(), 'last_check_at_utc': NOW.isoformat(),
        'article_bodies_accepted': 0, 'overlapping_feed_items_not_unique_articles': True,
        'all_history_complete': False, 'finish_eta_utc': None, 'raw_xml': 'PRIVATE_XML', 'items': ['PRIVATE_SUMMARY']}
    status['providers']['ltn'] = {'states': {'awaiting_authorization': 5}, 'text_bytes': 0,
        'automatic_collection_enabled': True, 'automatic_fulltext_allowed': False, 'metadata_feed_progress': progress}
    status['unique_articles'] += 5
    status['states']['awaiting_authorization'] += 5
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    return status


def test_metadata_feed_counts_never_become_fulltext_eta_or_exported_rows(tmp_path):
    _metadata_feed_snapshot(tmp_path)
    data = build_news_public_status(tmp_path, now=NOW)
    row = next(item for item in data['providers'] if item['id'] == 'ltn')
    assert row['enabled'] and row['state'] == 'metadata_polling' and not row['automatic_fulltext_allowed']
    assert row['complete_urls'] == 0 and row['conditional_eta_seconds'] is None
    assert row['conditional_finish_at_utc'] is None and row['completion_forecast'] is None
    assert row['metadata_feed_progress']['latest_feed_item_observations'] == 80
    assert row['metadata_feed_progress']['verified_feeds'] == 2
    assert 'PRIVATE_XML' not in json.dumps(data) and 'PRIVATE_SUMMARY' not in json.dumps(data)
    assert build_news_public_status(tmp_path, now=NOW + timedelta(seconds=181))['worker']['fresh'] is False


@pytest.mark.parametrize('cooling,errors,expected', [(False, 0, 'metadata_polling'),
    (False, 1, 'attention'), (True, 0, 'cooldown'), (True, 1, 'cooldown')])
def test_metadata_polling_cannot_hide_source_cooldowns_or_feed_errors(tmp_path, cooling, errors, expected):
    status = _metadata_feed_snapshot(tmp_path)
    status['providers']['ltn']['metadata_feed_progress']['feeds_with_errors'] = errors
    if cooling:
        status['provider_next_attempt_utc']['ltn'] = (NOW + timedelta(minutes=5)).isoformat()
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    row = next(item for item in build_news_public_status(tmp_path, now=NOW)['providers'] if item['id'] == 'ltn')
    assert row['state'] == expected
    assert row['conditional_eta_seconds'] is None and row['completion_forecast'] is None


@pytest.mark.parametrize('next_at,ready', [('1970-01-01T00:00:00+00:00', True),
    ((NOW - timedelta(seconds=1)).isoformat(), True),
    ((NOW + timedelta(minutes=5)).isoformat(), False), (None, False)])
def test_metadata_zero_due_is_ready_not_a_1970_calendar_date(tmp_path, next_at, ready):
    status = _metadata_feed_snapshot(tmp_path)
    status['providers']['ltn']['metadata_feed_progress']['next_check_at_utc'] = next_at
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    row = next(item for item in build_news_public_status(tmp_path, now=NOW)['providers'] if item['id'] == 'ltn')
    progress = row['metadata_feed_progress']
    assert progress['next_check_ready'] is ready
    assert progress['next_check_at_utc'] == (None if next_at == '1970-01-01T00:00:00+00:00' else next_at)


def _original_source_snapshot(status):
    status['providers']['unregistered:only-original.test'] = {
        'states': {}, 'text_bytes': 0, 'automatic_collection_enabled': False,
        'original_metadata': {'contract_version': 1, 'literal_metadata_links': 12, 'original_hosts': 2,
                              'original_url': 'https://secret.test/?token=PRIVATE_TOKEN', 'body': 'PRIVATE_BODY'},
    }
    status['original_source_inventory'] = {
        'contract_version': 1, 'resolution_contract_version': 1, 'available': True,
        'source_keys': 1, 'literal_metadata_links': 12, 'original_hosts': 2,
        'secret': 'PRIVATE_KEY', 'sources': {'PRIVATE_URL': 'PRIVATE_BODY'},
    }
    return status


def test_decoded_source_summary_is_typed_private_and_not_article_progress(tmp_path):
    status = _original_source_snapshot(_seed(tmp_path))
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    summary = result['original_source_inventory']
    assert summary['available'] and summary['literal_metadata_links'] == 12
    assert summary['source_keys'] == 1 and summary['original_hosts'] == 2
    assert not summary['body_completion_claimed'] and not summary['metadata_identity_verified']
    assert summary['publisher_requests'] == 0
    row = next(p for p in result['providers'] if p['id'] == 'unregistered:only-original.test')
    assert row['known_urls'] == row['complete_urls'] == 0 and not row['enabled']
    assert row['progress_ratio'] is row['conditional_eta_seconds'] is None
    assert row['original_metadata']['literal_metadata_links'] == 12
    assert result['acquisition']['known_urls'] == 10 and result['acquisition']['stored_body_articles'] == 2
    assert 'PRIVATE_' not in json.dumps(result) and '?token=' not in json.dumps(result)


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'resolution_contract_version': 2}, {'available': False},
    {'source_keys': 2}, {'source_keys': True}, {'literal_metadata_links': 13},
    {'literal_metadata_links': '12'}, {'original_hosts': 3}, {'original_hosts': -1},
])
def test_inconsistent_original_source_summary_is_unknown_without_hiding_body_progress(tmp_path, patch):
    status = _original_source_snapshot(_seed(tmp_path))
    status['original_source_inventory'].update(patch)
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert not result['original_source_inventory']['available']
    assert result['original_source_inventory']['literal_metadata_links'] is None
    assert all(row['original_metadata'] is None for row in result['providers'])
    assert result['acquisition']['known_urls'] == 10 and result['acquisition']['stored_body_articles'] == 2


def test_legacy_status_does_not_silently_claim_zero_decoded_original_sources(tmp_path):
    _seed(tmp_path)
    result = build_news_public_status(tmp_path, now=NOW)
    assert not result['original_source_inventory']['available']
    assert result['original_source_inventory']['source_keys'] is None
    assert result['original_source_inventory']['literal_metadata_links'] is None


def _work_snapshot():
    return {'contract_version': 1, 'active_missing_body_urls': 3,
            'existing_primary_body_candidates': 2, 'no_known_primary_body_urls': 1,
            'candidate_views_accepted_by_hint': False, 'candidate_body_equivalence_verified': False,
            'states': [{'url': 'PRIVATE_ARTICLE', 'body': 'PRIVATE_BODY'}], 'token': 'PRIVATE_TOKEN'}


def _lookup_recheck_snapshot():
    return {'contract_version': 1, 'pending_records_lower_bound': 3, 'count_exact': True,
            'records_are_article_completion': False, 'source_lookup_performed': False,
            'url': 'PRIVATE_URL', 'token': 'PRIVATE_TOKEN'}


def test_lookup_recheck_projection_preserves_scope_and_privacy(tmp_path):
    status = _seed(tmp_path)
    status['providers']['cnyes']['native_lookup_recheck_progress'] = _lookup_recheck_snapshot()
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['native_lookup_recheck_progress'] == {
        key: value for key, value in _lookup_recheck_snapshot().items() if key not in {'url', 'token'}}
    assert all(row['native_lookup_recheck_progress'] is None for row in result['providers'] if row['id'] != 'cnyes')
    assert result['acquisition']['stored_body_articles'] == 2 and 'PRIVATE_' not in json.dumps(result)
    status['providers']['cnyes']['native_lookup_recheck_progress'].update(
        pending_records_lower_bound=2049, count_exact=False)
    _save(tmp_path, status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['native_lookup_recheck_progress']['pending_records_lower_bound'] == 2049
    assert source['native_lookup_recheck_progress']['count_exact'] is False


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'pending_records_lower_bound': -1},
    {'pending_records_lower_bound': True}, {'pending_records_lower_bound': 2050},
    {'pending_records_lower_bound': 2049, 'count_exact': True},
    {'pending_records_lower_bound': 3, 'count_exact': False},
    {'count_exact': 1}, {'records_are_article_completion': True},
    {'source_lookup_performed': True},
])
def test_invalid_or_missing_lookup_recheck_snapshot_is_unknown(tmp_path, patch):
    status = _seed(tmp_path)
    status['providers']['cnyes']['native_lookup_recheck_progress'] = {**_lookup_recheck_snapshot(), **patch}
    _save(tmp_path, status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['native_lookup_recheck_progress'] is None
    del status['providers']['cnyes']['native_lookup_recheck_progress']
    _save(tmp_path, status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['native_lookup_recheck_progress'] is None


def test_body_work_split_preserves_ready_total_and_never_calls_hints_completed(tmp_path):
    status = _seed(tmp_path)
    status['providers']['cnyes']['body_work_progress'] = _work_snapshot()
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    work = source['body_work_progress']
    assert work['existing_primary_body_candidates'] == 2 and work['no_known_primary_body_urls'] == 1
    assert work['stored_body_recheck_urls'] == 1
    assert sum(work[key] for key in ('existing_primary_body_candidates', 'no_known_primary_body_urls', 'stored_body_recheck_urls')) == source['remaining_urls']
    assert work['no_known_primary_includes_unclassified']
    assert not work['candidate_views_accepted_by_hint'] and not work['candidate_body_equivalence_verified']
    assert source['complete_urls'] == 2 and source['progress_ratio'] == 2 / 6
    assert 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('patch', [{'contract_version': 2}, {'active_missing_body_urls': 5},
    {'active_missing_body_urls': 2}, {'existing_primary_body_candidates': -1},
    {'existing_primary_body_candidates': True}, {'no_known_primary_body_urls': None},
    {'candidate_views_accepted_by_hint': True}, {'candidate_body_equivalence_verified': True}])
def test_legacy_inconsistent_or_unverified_work_split_is_unknown_not_zero(tmp_path, patch):
    status = _seed(tmp_path)
    status['providers']['cnyes']['body_work_progress'] = {**_work_snapshot(), **patch}
    _save(tmp_path, status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['body_work_progress'] is None and source['remaining_urls'] == 4


def test_original_url_examination_progress_is_not_body_progress_or_private_content(tmp_path):
    status = _seed(tmp_path)
    status['original_url_resolution'] = _resolution_snapshot()
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)
    resolution = result['original_url_resolution']
    assert resolution['snapshot_consistent'] and resolution['progress_ratio'] == .5
    assert resolution['remaining_urls'] == 500 and resolution['conditional_eta_seconds'] == 100
    assert resolution['opaque_links'] == 400 and resolution['links_to_queued_targets'] == 80
    assert result['acquisition']['stored_body_articles'] == 2
    assert not resolution['body_completion_claimed'] and not resolution['metadata_identity_verified']
    assert resolution['publisher_requests'] == 0 and 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('patch', [{'examined_urls': 2000}, {'counts': {'literal_original': 500, 'opaque_original': 400}},
                                 {'literal_links_to_queued_targets': 101}, {'known_redirect_urls': True}])
def test_original_url_inconsistent_snapshot_has_no_ratio_or_eta(tmp_path, patch):
    status = _seed(tmp_path)
    status['original_url_resolution'] = {**_resolution_snapshot(), **patch}
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)['original_url_resolution']
    assert not result['snapshot_consistent'] and result['progress_ratio'] is None
    assert result['conditional_eta_seconds'] is None and result['remaining_urls'] is None


@pytest.mark.parametrize('activity', ['stopped', 'draining', 'waiting_disk'])
def test_original_url_eta_stops_when_the_owner_is_not_running(tmp_path, activity):
    status = _seed(tmp_path)
    status.update(activity=activity, original_url_resolution=_resolution_snapshot())
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    result = build_news_public_status(tmp_path, now=NOW)['original_url_resolution']
    assert result['progress_ratio'] == .5 and result['owner_urls_per_second'] is None
    assert result['conditional_eta_seconds'] is None


def _save(root: Path, status: dict) -> None:
    atomic_write_json(root / "data_news_fulltext/status.json", status)


def _server(root: Path, *, audience: str = "owner") -> gateway.PublicDashboardServer:
    """News is owner-only; exercise its reader through the actual owner listener."""
    return gateway.PublicDashboardServer(
        ("127.0.0.1", 0), audience=audience, public_static_root=REPO / "services/public_dashboards",
        taifex_static_root=REPO / "services/taifex_dashboard", tw_static_root=REPO / "services/tw_day_trade_dashboard",
        shioaji_static_root=REPO / "services/shioaji_api_dashboard", finlab_static_root=REPO / "services/finlab_dashboard",
        finmind_static_root=REPO / "services/finmind_dashboard", news_static_root=REPO / "services/news_dashboard",
        openbb_static_root=REPO / "services/openbb_archive_dashboard", data_monitor_static_root=REPO / "services/data_monitor_dashboard",
        traffic_static_root=REPO / "services/traffic_dashboard", traffic_history_root=root / "private_traffic",
        repo_root=root, taifex_upstream="http://127.0.0.1:1", tw_upstream="http://127.0.0.1:1",
    )


def test_project_source_counts_not_metadata_versions_or_legal_completion(tmp_path):
    _seed(tmp_path)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["health"] == "running" and result["snapshot_consistent"]
    acquisition = result["acquisition"]
    assert acquisition["known_urls"] == 10 and acquisition["stored_body_articles"] == 2
    assert acquisition["progress_ratio"] == 0.2
    assert acquisition["remaining_urls"] == acquisition["awaiting_authorization_urls"] == 4
    source = result["providers"][0]
    assert source["conditional_eta_seconds"] == 40
    assert source["owner_accepted_versions"] == 12 != source["complete_urls"]
    assert acquisition["all_sources_eta_seconds"] is None
    assert result["scope"]["historical_completeness_verified"] is False
    assert result["worker"]["requests"] == 12
    assert source["official_daily_quota"] is source["official_hourly_quota"] is None
    assert {stage["id"] for stage in result["stages"]} == {"parquet_compress_fsync", "transport"}
    encoded = json.dumps(result)
    for forbidden in ("PRIVATE_TOKEN", "PRIVATE_BODY", "PRIVATE_HTML", "base_url", "api_key", str(tmp_path)):
        assert forbidden not in encoded


def test_valid_full_source_snapshot_above_old_four_mib_keeps_exact_projection(tmp_path):
    from stockagent.live.news_dashboard import MAX_SNAPSHOT_BYTES, MAX_STATUS_SNAPSHOT_BYTES
    status = _seed(tmp_path)
    status['providers']['unregistered:more-news.tw'] = {'states': {'awaiting_authorization': 89}}
    status['states']['awaiting_authorization'] += 89
    status['unique_articles'] += 89
    _save(tmp_path, status)
    baseline = build_news_public_status(tmp_path, now=NOW)
    # Private, irrelevant telemetry must neither hide actual providers nor be
    # exposed by increasing the status-only input budget.
    status['private_unused_diagnostic'] = 'PRIVATE_PADDING' * (MAX_SNAPSHOT_BYTES // 14 + 1)
    _save(tmp_path, status)
    size = (tmp_path / 'data_news_fulltext/status.json').stat().st_size
    assert MAX_SNAPSHOT_BYTES < size < MAX_STATUS_SNAPSHOT_BYTES
    result = build_news_public_status(tmp_path, now=NOW)
    assert result == baseline
    assert result['snapshot_read_state'] == 'loaded' and result['snapshot_reader_contract_version'] == 3
    row = next(r for r in result['providers'] if r['id'] == 'unregistered:more-news.tw')
    assert row['known_urls'] == 89 and row['complete_urls'] == 0
    assert 'PRIVATE_PADDING' not in json.dumps(result)


def test_status_limit_remains_bounded_even_after_snapshot_is_cached(tmp_path, monkeypatch):
    from stockagent.live import news_dashboard
    status = _seed(tmp_path)
    assert build_news_public_status(tmp_path, now=NOW)['snapshot_read_state'] == 'loaded'
    limit = (tmp_path / 'data_news_fulltext/status.json').stat().st_size - 1
    monkeypatch.setattr(news_dashboard, 'MAX_STATUS_SNAPSHOT_BYTES', limit)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['snapshot_read_state'] == 'too_large' and result['health'] == 'unavailable'
    assert result['acquisition']['known_urls'] is None
    assert result['acquisition']['all_sources_eta_seconds'] is None
    assert result['snapshot_reader_contract_version'] == 3


def test_auxiliary_json_keeps_original_four_mib_limit(tmp_path):
    from stockagent.live.news_dashboard import _read, MAX_SNAPSHOT_BYTES
    path = tmp_path / 'auxiliary.json'
    atomic_write_json(path, {'private': 'x' * MAX_SNAPSHOT_BYTES})
    assert _read(path) == {}


@pytest.mark.parametrize('outcome', ['source_body_image_only', 'source_body_embedded_only', 'source_body_quote_only', 'source_article_empty_template'])
def test_source_body_outcome_remains_distinct_from_unknown_failures(tmp_path, outcome):
    status = _seed(tmp_path)
    status["states"]["pending"] = 2
    status["states"].update({outcome: 1, 'unknown_failure': 1})
    status["providers"]["cnyes"]["states"].update({'pending': 2, outcome: 1, 'unknown_failure': 1})
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(provider for provider in result["providers"] if provider["id"] == "cnyes")
    assert result["snapshot_consistent"]
    assert source["states"][outcome] == 1
    assert source["states"]["other"] == 1
    assert source["complete_urls"] == 2
    assert source["blocked_urls"] == 2 and source["remaining_urls"] == 2
    encoded = json.dumps(result)
    assert all(private not in encoded for private in ("PRIVATE_BODY", "PRIVATE_HTML", "PRIVATE_TOKEN", str(tmp_path)))


def test_finite_cohort_denominator_growth_and_unresolved_eta(tmp_path):
    status = _seed(tmp_path)
    status['states'] = {'complete': 2, 'waiting_cohort': 4, 'awaiting_authorization': 4}
    source = status['providers']['cnyes']
    source.update(states={'complete': 2, 'waiting_cohort': 4}, acquisition_mode='finite',
                  finite_cohort={'urls': 2, 'stored_bodies': 2, 'receipt_path': 'PRIVATE_TOKEN',
                                 'all_cohort_bodies_stored': False})
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(p for p in result['providers'] if p['id'] == 'cnyes')
    assert source['state'] == 'waiting_cohort'
    assert source['conditional_eta_seconds'] is source['conditional_finish_at_utc'] is None
    assert source['progress_ratio'] == 1 and source['blocked_urls'] == 0
    assert source['finite_cohort']['new_urls_outside_cohort'] == 4
    assert result['acquisition']['growth_waiting_urls'] == 4
    assert result['acquisition']['blocked_urls'] == 4
    assert 'PRIVATE_TOKEN' not in json.dumps(source)
    status['states'] = {'complete': 1, 'article_license_not_verified': 1, 'waiting_cohort': 4, 'awaiting_authorization': 4}
    status['providers']['cnyes']['states'] = {'complete': 1, 'article_license_not_verified': 1, 'waiting_cohort': 4}
    status['providers']['cnyes']['finite_cohort']['stored_bodies'] = 1
    _save(tmp_path, status)
    source = next(p for p in build_news_public_status(tmp_path, now=NOW)['providers'] if p['id'] == 'cnyes')
    assert source['state'] == 'attention' and source['conditional_eta_seconds'] is None
    assert source['progress_ratio'] == 0.5 and source['blocked_urls'] == 1


@pytest.mark.parametrize('mode', [[], {}, None, True, 'unexpected'])
def test_malformed_cohort_mode_does_not_break_public_projection(tmp_path, mode):
    status = _seed(tmp_path)
    status['providers']['cnyes']['acquisition_mode'] = mode
    status['providers']['cnyes']['finite_cohort'] = {'receipt_path': 'PRIVATE_TOKEN'}
    _save(tmp_path, status)
    source = next(p for p in build_news_public_status(tmp_path, now=NOW)['providers'] if p['id'] == 'cnyes')
    assert source['acquisition_mode'] == 'continuous' and source['finite_cohort'] is None
    assert 'PRIVATE_TOKEN' not in json.dumps(source)


def test_reviewed_supplement_progress_preserves_original_denominator_and_privacy(tmp_path):
    status = _seed(tmp_path)
    source = status['providers']['cnyes']
    source.update(states={'complete': 3, 'waiting_cohort': 3}, acquisition_mode='finite',
                  finite_cohort={'urls': 2, 'stored_bodies': 2},
                  finite_supplements={'scopes': 1, 'urls': 1, 'stored_bodies': 1,
                                      'original_cohort_unchanged': True,
                                      'receipt_path': 'PRIVATE_TOKEN', 'raw': 'PRIVATE_HTML'})
    status['states'] = {'complete': 3, 'waiting_cohort': 3, 'awaiting_authorization': 4}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(p for p in result['providers'] if p['id'] == 'cnyes')
    assert source['finite_cohort']['urls'] == 2 and source['progress_ratio'] == 1
    assert source['finite_cohort']['new_urls_outside_cohort'] == 4
    assert source['finite_cohort']['unreviewed_new_urls'] == 3
    assert source['growth_waiting_urls'] == 3
    assert source['finite_supplements'] == {'scopes': 1, 'urls': 1, 'stored_bodies': 1,
        'remaining_missing_bodies': 0, 'progress_ratio': 1, 'all_supplement_bodies_stored': True}
    assert source['state'] == 'waiting_cohort'
    assert source['conditional_eta_seconds'] is source['conditional_finish_at_utc'] is None
    assert all(secret not in json.dumps(result) for secret in ('PRIVATE_TOKEN', 'PRIVATE_HTML', str(tmp_path)))


def test_failed_supplement_has_no_false_finished_countdown(tmp_path):
    status = _seed(tmp_path)
    source = status['providers']['cnyes']
    source.update(states={'complete': 2, 'article_attribution_not_verified': 1, 'waiting_cohort': 3},
                  acquisition_mode='finite', finite_cohort={'urls': 2, 'stored_bodies': 2},
                  finite_supplements={'scopes': 1, 'urls': 1, 'stored_bodies': 0,
                                      'original_cohort_unchanged': True})
    status['states'] = {**source['states'], 'awaiting_authorization': 4}
    _save(tmp_path, status)
    source = next(p for p in build_news_public_status(tmp_path, now=NOW)['providers'] if p['id'] == 'cnyes')
    assert source['state'] == 'attention'
    assert source['finite_supplements']['progress_ratio'] == 0
    assert source['conditional_eta_seconds'] is source['conditional_finish_at_utc'] is None


def test_missing_title_is_visible_as_its_own_blocker_not_an_author_error(tmp_path):
    status = _seed(tmp_path)
    status['providers']['cnyes']['states'] = {'complete': 2, 'pending': 3, 'article_title_not_verified': 1}
    status['states'] = {**status['providers']['cnyes']['states'], 'awaiting_authorization': 4}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['states']['article_title_not_verified'] == 1
    assert 'article_attribution_not_verified' not in source['states']
    assert source['known_urls'] == 6 and source['complete_urls'] == 2
    assert source['remaining_urls'] == 3 and source['blocked_urls'] == 1
    assert result['states']['article_title_not_verified'] == 1


def test_recorded_nonbody_outcomes_leave_body_denominators_and_private_proofs_separate(tmp_path):
    status = _seed(tmp_path)
    raw = status['providers']['cnyes']
    raw.update(states={'complete': 3, 'source_not_found': 2, 'unsupported_article_path': 1}, acquisition_mode='finite',
               finite_cohort={'urls': 2, 'stored_bodies': 2, 'reviewed_nonbody_outside_cohort': {
                   'contract_version': 1, 'urls': 3, 'states': {'source_not_found': 2, 'unsupported_article_path': 1},
                   'accepted_article_bodies': 0, 'receipt_path': 'PRIVATE_PROOF', 'raw': 'PRIVATE_SOURCE'}},
               finite_supplements={'scopes': 1, 'urls': 1, 'stored_bodies': 1, 'original_cohort_unchanged': True})
    status['states'] = {**raw['states'], 'awaiting_authorization': 4}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(p for p in result['providers'] if p['id'] == 'cnyes')
    assert result['nonbody_source_outcome_progress_contract_version'] == 1
    assert source['finite_cohort']['reviewed_nonbody_outside_cohort'] == {'contract_version': 1, 'urls': 3,
        'states': {'source_not_found': 2, 'unsupported_article_path': 1}, 'accepted_article_bodies': 0}
    assert source['finite_cohort']['unreviewed_new_urls'] == source['growth_waiting_urls'] == 0
    assert source['finite_cohort']['urls'] == source['finite_cohort']['stored_bodies'] == 2
    assert source['finite_supplements']['stored_bodies'] == 1 and source['progress_ratio'] == 1
    assert all(secret not in json.dumps(result) for secret in ('PRIVATE_PROOF', 'PRIVATE_SOURCE', str(tmp_path)))


@pytest.mark.parametrize('updates', [{'contract_version': True}, {'urls': True}, {'urls': 4},
    {'states': {'source_not_found': 1}}, {'states': {'complete': 3}}, {'accepted_article_bodies': 1},
    {'accepted_article_bodies': False}])
def test_invalid_nonbody_counts_cannot_hide_unreviewed_scope(tmp_path, updates):
    status = _seed(tmp_path)
    raw = status['providers']['cnyes']
    raw.update(states={'complete': 3, 'source_not_found': 2, 'unsupported_article_path': 1}, acquisition_mode='finite',
               finite_cohort={'urls': 2, 'stored_bodies': 2, 'reviewed_nonbody_outside_cohort': {
                   'contract_version': 1, 'urls': 3, 'states': {'source_not_found': 2, 'unsupported_article_path': 1},
                   'accepted_article_bodies': 0, 'receipt_path': 'PRIVATE_PROOF', **updates}},
               finite_supplements={'scopes': 1, 'urls': 1, 'stored_bodies': 1, 'original_cohort_unchanged': True})
    status['states'] = {**raw['states'], 'awaiting_authorization': 4}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(p for p in result['providers'] if p['id'] == 'cnyes')
    assert source['finite_cohort']['reviewed_nonbody_outside_cohort'] is None
    assert source['finite_cohort']['unreviewed_new_urls'] == 3
    assert 'PRIVATE_PROOF' not in json.dumps(result)


@pytest.mark.parametrize('updates', [{'urls': 5}, {'stored_bodies': 2}, {'scopes': 0},
                                     {'scopes': True}, {'original_cohort_unchanged': False}])
def test_invalid_supplement_counts_never_change_original_scope_or_expose_private_fields(tmp_path, updates):
    status = _seed(tmp_path)
    source = status['providers']['cnyes']
    source.update(states={'complete': 2, 'waiting_cohort': 4}, acquisition_mode='finite',
                  finite_cohort={'urls': 2, 'stored_bodies': 2},
                  finite_supplements={'scopes': 1, 'urls': 1, 'stored_bodies': 1,
                                      'original_cohort_unchanged': True, 'receipt_path': 'PRIVATE_TOKEN', **updates})
    status['states'] = {**source['states'], 'awaiting_authorization': 4}
    _save(tmp_path, status)
    source = next(p for p in build_news_public_status(tmp_path, now=NOW)['providers'] if p['id'] == 'cnyes')
    assert source['finite_supplements'] is None and source['finite_cohort']['unreviewed_new_urls'] == 4
    assert source['finite_cohort']['urls'] == 2 and source['progress_ratio'] == 1
    assert source['conditional_eta_seconds'] is None
    assert 'PRIVATE_TOKEN' not in json.dumps(source)


def test_runtime_capacity_and_new_heads_override_baseline_and_revision_proxy(tmp_path):
    status = _seed(tmp_path)
    status["performance"]["providers"]["cnyes"]["counts"]["durably_accepted_new_url_heads"] = 10
    status["http_capacity"] = {"providers": {"cnyes": {"effective_interval_seconds": 0.05, "parallelism": 4}},
        "calibration": {"providers": {"cnyes": {"state": "verifying", "stable_samples": 2, "reason": "PRIVATE_REASON",
            "last_window": {"wall_seconds": 45, "actual_requests_per_second": 9.0, "new_heads_per_second": 180,
                            "ended_at_epoch": NOW.timestamp()}}}}}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(p for p in result["providers"] if p["id"] == "cnyes")
    assert source["client_min_interval_seconds"] == 0.05 and source["parallelism"] == 4
    assert source["last_window_requests_per_second"] == 9 and source["capacity_state"] == "verifying"
    assert source["conditional_eta_seconds"] == 48 and source["forecast_rate_basis"] == "new_stored_url_heads"
    assert "PRIVATE_REASON" not in json.dumps(result)
    status["performance"]["providers"]["cnyes"]["counts"]["durably_accepted_new_url_heads"] = 0
    _save(tmp_path, status)
    assert build_news_public_status(tmp_path, now=NOW)["providers"][0]["conditional_eta_seconds"] is None


def _origin_pause_status(tmp_path, origins):
    status = _seed(tmp_path)
    status['performance']['robots_origin_policy_contract_version'] = 1
    status['http_capacity'] = {'providers': {'cnyes': {
        'effective_interval_seconds': 0.5, 'parallelism': 3,
        'temporarily_paused_robots_origins': origins}}}
    return status


@pytest.mark.parametrize('provider', ['cnyes', 'globalvoices', 'cmnews'])
@pytest.mark.parametrize(('gate', 'value'), [
    ('temporarily_paused_robots_origins', {'https://private-source.test': NOW.timestamp() + 30}),
    ('temporarily_paused_gateway_hosts', {'private-source.test': NOW.timestamp() + 30}),
    ('unverified_robots_origins', {'https://private-source.test': NOW.timestamp() - 30}),
    ('unverified_gateway_hosts', {'private-source.test': NOW.timestamp() - 30}),
    ('unverified_robots_origins_overflow', True),
    ('unverified_robots_origins_overflow', 0),
    ('temporarily_paused_robots_origins', []),
    ('unverified_gateway_hosts', None),
])
def test_unscoped_provider_gates_withdraw_whole_pending_eta(tmp_path, provider, gate, value):
    status = _seed(tmp_path)
    if provider != 'cnyes':
        status['providers'][provider] = status['providers'].pop('cnyes')
        status['performance']['providers'][provider] = status['performance']['providers'].pop('cnyes')
        status['http_requests_this_run'][provider] = status['http_requests_this_run'].pop('cnyes')
        config = json.loads((tmp_path / 'configs/news_fulltext.json').read_text())
        config['providers'][0]['id'] = provider
        atomic_write_json(tmp_path / 'configs/news_fulltext.json', config)
    status['performance']['robots_origin_policy_contract_version'] = 1
    status['http_capacity'] = {'providers': {provider: {
        'effective_interval_seconds': 0.5, 'parallelism': 2, gate: value}}}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == provider)
    assert source['state'] == 'backfilling' and source['owner_articles_per_second'] == .1
    assert source['conditional_eta_seconds'] is None and source['conditional_finish_at_utc'] is None
    assert source['conditional_eta_unavailable_reason'] == 'source_request_scope_unknown'
    assert result['acquisition']['eligible_eta_seconds'] is None
    assert 'private-source.test' not in json.dumps(result)


def test_empty_provider_gate_maps_do_not_withdraw_eta(tmp_path):
    status = _seed(tmp_path)
    status['http_capacity'] = {'providers': {'cnyes': {
        'temporarily_paused_robots_origins': {}, 'temporarily_paused_gateway_hosts': {},
        'unverified_robots_origins': {}, 'unverified_gateway_hosts': {},
        'unverified_robots_origins_overflow': False}}}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['conditional_eta_seconds'] == 40
    assert source['conditional_eta_unavailable_reason'] is None
    assert result['acquisition']['eligible_eta_seconds'] == 40


@pytest.mark.parametrize('capacity', [None, [], {'providers': None}, {'providers': []},
    {'providers': {'cnyes': None}}, {'providers': {'cnyes': []}}])
def test_malformed_capacity_or_provider_control_withdraws_eta(tmp_path, capacity):
    status = _seed(tmp_path)
    status['http_capacity'] = capacity
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['state'] == 'backfilling' and source['owner_articles_per_second'] == .1
    assert source['conditional_eta_seconds'] is None
    assert source['conditional_eta_unavailable_reason'] == 'source_request_scope_unknown'
    assert result['acquisition']['eligible_eta_seconds'] is None


def test_unknown_source_scope_does_not_withdraw_healthy_provider_eta(tmp_path):
    status = _seed(tmp_path)
    status['providers']['globalvoices'] = deepcopy(status['providers']['cnyes'])
    status['performance']['providers']['globalvoices'] = deepcopy(status['performance']['providers']['cnyes'])
    status['http_requests_this_run']['globalvoices'] = 12
    status['unique_articles'] = 16
    status['articles_with_stored_body'] = 4
    status['states']['complete'] = 4
    status['states']['pending'] = 8
    status['http_capacity'] = {'providers': {
        'cnyes': {'unverified_gateway_hosts': {'private-source.test': NOW.timestamp() - 30}},
        'globalvoices': {'unverified_gateway_hosts': {}}}}
    config = json.loads((tmp_path / 'configs/news_fulltext.json').read_text())
    config['providers'].append({'id': 'globalvoices', 'enabled': True})
    atomic_write_json(tmp_path / 'configs/news_fulltext.json', config)
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    sources = {row['id']: row for row in result['providers']}
    assert sources['cnyes']['conditional_eta_seconds'] is None
    assert sources['globalvoices']['conditional_eta_seconds'] == 40
    assert result['acquisition']['eligible_eta_seconds'] is None


def test_origin_pause_is_private_count_and_probe_clock_not_provider_stop_or_permission(tmp_path):
    status = _origin_pause_status(tmp_path, {
        'https://amp-news.cnyes.com': NOW.timestamp() + 240,
        'https://m.cnyes.com': NOW.timestamp() + 30,
        'https://news.cnyes.com': NOW.timestamp() - 1})
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(p for p in result['providers'] if p['id'] == 'cnyes')
    assert source['state'] == 'backfilling' and source['next_attempt_at_utc'] is None
    assert source['conditional_eta_seconds'] is None
    assert source['conditional_eta_unavailable_reason'] == 'source_request_scope_unknown'
    assert source['robots_origin_policy_pause'] == {
        'contract_version': 1, 'scope': 'initial_origin_policy', 'active_origin_count': 2,
        'next_policy_probe_at_utc': (NOW + timedelta(seconds=30)).isoformat(),
        'permission_granted': False, 'provider_recovery_verified': False}
    assert all(origin not in json.dumps(result) for origin in status['http_capacity']['providers']['cnyes']['temporarily_paused_robots_origins'])


@pytest.mark.parametrize('origins', [{}, {'https://amp-news.cnyes.com': NOW.timestamp()},
    {'https://amp-news.cnyes.com': NOW.timestamp() - 1}])
def test_expired_or_empty_origin_pause_is_zero_active_not_recovery_proof(tmp_path, origins):
    _save(tmp_path, _origin_pause_status(tmp_path, origins))
    source = next(p for p in build_news_public_status(tmp_path, now=NOW)['providers'] if p['id'] == 'cnyes')
    pause = source['robots_origin_policy_pause']
    assert pause['active_origin_count'] == 0 and pause['next_policy_probe_at_utc'] is None
    assert pause['permission_granted'] is False and pause['provider_recovery_verified'] is False


@pytest.mark.parametrize('origins', [None, [], {'https://amp-news.cnyes.com': True},
    {'https://amp-news.cnyes.com': '1791680000'}, {'https://amp-news.cnyes.com': float('nan')},
    {'https://amp-news.cnyes.com': float('inf')}, {'https://amp-news.cnyes.com': -1},
    {'https://amp-news.cnyes.com': NOW.timestamp() + 306},
    {'https://PRIVATE_TOKEN@news.cnyes.com': NOW.timestamp() + 30},
    {'https://news.cnyes.com/PRIVATE_TOKEN': NOW.timestamp() + 30},
    {'https://news.cnyes.com?api_key=PRIVATE_TOKEN': NOW.timestamp() + 30},
    {'https://news.cnyes.com#PRIVATE_TOKEN': NOW.timestamp() + 30},
    {'https://NEWS.cnyes.com': NOW.timestamp() + 30},
    {'https://news.cnyes.com:invalid': NOW.timestamp() + 30},
    {'https://news.cnyes.com:65536': NOW.timestamp() + 30},
    {f'https://origin{number}.example.org': NOW.timestamp() + 30 for number in range(33)},
])
def test_malformed_or_unbounded_origin_observation_is_unknown_not_zero(tmp_path, origins):
    _save(tmp_path, _origin_pause_status(tmp_path, origins))
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(p for p in result['providers'] if p['id'] == 'cnyes')
    assert source['robots_origin_policy_pause'] is None
    assert 'PRIVATE_TOKEN' not in json.dumps(result)


@pytest.mark.parametrize('reason', ['legacy', 'bool_version', 'wrong_version', 'stale', 'future', 'stopped'])
def test_origin_pause_needs_current_live_loaded_contract(tmp_path, reason):
    status = _origin_pause_status(tmp_path, {'https://amp-news.cnyes.com': NOW.timestamp() + 30})
    if reason == 'legacy':
        del status['performance']['robots_origin_policy_contract_version']
    elif reason == 'bool_version':
        status['performance']['robots_origin_policy_contract_version'] = True
    elif reason == 'wrong_version':
        status['performance']['robots_origin_policy_contract_version'] = 2
    elif reason == 'stale':
        status['updated_at_utc'] = (NOW - timedelta(seconds=181)).isoformat()
    elif reason == 'future':
        status['updated_at_utc'] = (NOW + timedelta(seconds=6)).isoformat()
    else:
        status['activity'] = 'stopped'
    _save(tmp_path, status)
    source = next(p for p in build_news_public_status(tmp_path, now=NOW)['providers'] if p['id'] == 'cnyes')
    assert source['robots_origin_policy_pause'] is None


def test_global_provider_deadline_remains_separate_and_suspends_eta(tmp_path):
    status = _origin_pause_status(tmp_path, {'https://amp-news.cnyes.com': NOW.timestamp() + 30})
    status['provider_next_attempt_utc']['cnyes'] = (NOW + timedelta(seconds=300)).isoformat()
    _save(tmp_path, status)
    source = next(p for p in build_news_public_status(tmp_path, now=NOW)['providers'] if p['id'] == 'cnyes')
    assert source['state'] == 'cooldown' and source['conditional_eta_seconds'] is None
    assert source['next_attempt_at_utc'] == (NOW + timedelta(seconds=300)).isoformat()
    assert source['robots_origin_policy_pause']['next_policy_probe_at_utc'] == (NOW + timedelta(seconds=30)).isoformat()


def _request_scope_status(tmp_path, *, paused=4):
    status = _origin_pause_status(tmp_path, {'https://amp-news.cnyes.com': NOW.timestamp() + 30})
    status['providers']['cnyes']['body_work_progress'] = {
        **_work_snapshot(), 'active_missing_body_urls': 4, 'existing_primary_body_candidates': 3,
        'request_scope_progress': {'contract_version': 1, 'available': True,
            'active_missing_body_urls': 4, 'source_paused_missing_body_urls': paused,
            'other_missing_body_urls': 4 - paused, 'policy_paused_missing_body_urls': paused,
            'gateway_paused_missing_body_urls': 0, 'source_recovery_verified': False,
            'next_source_probe_at_utc': (NOW + timedelta(seconds=30)).isoformat() if paused else None,
            'pending_source_rechecks': {'contract_version': 1, 'available': True,
                'unverified_missing_body_urls': paused, 'due_missing_body_urls': 0,
                'next_probe_at_utc': (NOW + timedelta(seconds=30)).isoformat() if paused else None},
            'private_url': 'PRIVATE_URL', 'body': 'PRIVATE_BODY'}}
    return status


@pytest.mark.parametrize('paused', [1, 2, 4])
def test_known_paused_missing_scopes_do_not_borrow_healthy_lane_speed(tmp_path, paused):
    status = _request_scope_status(tmp_path, paused=paused)
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    scope = source['body_request_scope_progress']
    assert scope['available'] and scope['source_paused_missing_body_urls'] == paused
    assert scope['other_missing_body_urls'] == 4 - paused
    assert source['state'] == 'backfilling' and source['owner_articles_per_second'] == 0.1
    assert source['conditional_eta_seconds'] is source['conditional_finish_at_utc'] is None
    assert source['conditional_eta_unavailable_reason'] == 'source_request_scope_paused'
    assert result['acquisition']['eligible_eta_seconds'] is None
    assert source['remaining_urls'] == 4 and source['stored_body_urls'] is None
    assert not scope['source_recovery_verified'] and not scope['source_probe_ready']
    assert 'PRIVATE_' not in json.dumps(result) and 'amp-news.cnyes.com' not in json.dumps(result)


def test_probe_deadline_elapsed_is_not_source_recovery_or_completed_missing_bodies(tmp_path):
    _save(tmp_path, _request_scope_status(tmp_path))
    data = build_news_public_status(tmp_path, now=NOW + timedelta(seconds=31))
    source = next(row for row in data['providers'] if row['id'] == 'cnyes')
    scope = source['body_request_scope_progress']
    assert scope['source_probe_ready'] and not scope['source_recovery_verified']
    assert scope['source_paused_missing_body_urls'] == 4
    assert source['conditional_eta_seconds'] is None and source['complete_urls'] == 2


def test_unaffected_missing_scopes_keep_rate_and_eta_despite_unrelated_origin_pause(tmp_path):
    _save(tmp_path, _request_scope_status(tmp_path, paused=0))
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['conditional_eta_seconds'] == 40 and source['conditional_eta_unavailable_reason'] is None
    assert source['body_request_scope_progress']['other_missing_body_urls'] == 4
    assert source['robots_origin_policy_pause']['active_origin_count'] == 1


def test_malformed_control_withdraws_eta_even_with_saved_precise_scope(tmp_path):
    status = _request_scope_status(tmp_path, paused=0)
    status['http_capacity']['providers']['cnyes'] = None
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['body_request_scope_progress']['available']
    assert source['conditional_eta_seconds'] is None
    assert source['conditional_eta_unavailable_reason'] == 'source_request_scope_unknown'
    assert result['acquisition']['eligible_eta_seconds'] is None


@pytest.mark.parametrize(('field', 'value'), [
    ('temporarily_paused_robots_origins', []),
    ('unverified_gateway_hosts', None),
    ('unverified_gateway_hosts', {'private-source.test': 'bad'}),
    ('unverified_gateway_hosts', {'private-source.test': True}),
    ('unverified_robots_origins_overflow', True),
    ('unverified_robots_origins_overflow', 0),
])
def test_malformed_gate_withdraws_eta_even_with_saved_precise_scope(tmp_path, field, value):
    status = _request_scope_status(tmp_path, paused=0)
    status['http_capacity']['providers']['cnyes'][field] = value
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['body_request_scope_progress']['available']
    assert source['conditional_eta_seconds'] is None
    assert source['conditional_eta_unavailable_reason'] == 'source_request_scope_unknown'
    assert result['acquisition']['eligible_eta_seconds'] is None


def test_expired_unverified_source_never_borrows_other_lane_speed(tmp_path):
    status = _request_scope_status(tmp_path, paused=0)
    status['providers']['cnyes']['body_work_progress']['request_scope_progress']['pending_source_rechecks'] = {
        'contract_version': 1, 'available': True, 'unverified_missing_body_urls': 3,
        'due_missing_body_urls': 3, 'next_probe_at_utc': (NOW - timedelta(seconds=20)).isoformat(),
        'origin': 'PRIVATE_ORIGIN'}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    scope = source['body_request_scope_progress']
    assert scope['source_paused_missing_body_urls'] == 0
    assert scope['pending_source_rechecks']['unverified_missing_body_urls'] == 3
    assert scope['pending_source_rechecks']['due_missing_body_urls'] == 3
    assert source['conditional_eta_seconds'] is None
    assert source['conditional_eta_unavailable_reason'] == 'source_request_scope_paused'
    assert result['acquisition']['eligible_eta_seconds'] is None
    assert 'PRIVATE_ORIGIN' not in json.dumps(result)


def test_legacy_scope_cannot_prove_no_unverified_source_failures(tmp_path):
    status = _request_scope_status(tmp_path, paused=0)
    del status['providers']['cnyes']['body_work_progress']['request_scope_progress']['pending_source_rechecks']
    _save(tmp_path, status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['body_request_scope_progress']['pending_source_rechecks'] is None
    assert source['conditional_eta_seconds'] is None
    assert source['conditional_eta_unavailable_reason'] == 'source_request_scope_unknown'


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'available': 1}, {'unverified_missing_body_urls': True},
    {'unverified_missing_body_urls': 5}, {'due_missing_body_urls': 4},
    {'next_probe_at_utc': None}, {'next_probe_at_utc': 'PRIVATE_TIME'},
    {'next_probe_at_utc': (NOW + timedelta(days=2)).isoformat()},
    {'available': False},
])
def test_invalid_recheck_evidence_is_unknown_and_cannot_reenable_eta(tmp_path, patch):
    status = _request_scope_status(tmp_path, paused=0)
    status['providers']['cnyes']['body_work_progress']['request_scope_progress']['pending_source_rechecks'] = {
        'contract_version': 1, 'available': True, 'unverified_missing_body_urls': 3,
        'due_missing_body_urls': 3, 'next_probe_at_utc': (NOW - timedelta(seconds=20)).isoformat(), **patch}
    _save(tmp_path, status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['body_request_scope_progress']['pending_source_rechecks'] is None
    assert source['conditional_eta_seconds'] is None
    assert source['conditional_eta_unavailable_reason'] == 'source_request_scope_unknown'


@pytest.mark.parametrize('patch', [{'contract_version': True}, {'available': True, 'other_missing_body_urls': 1},
    {'source_paused_missing_body_urls': True}, {'active_missing_body_urls': 5},
    {'policy_paused_missing_body_urls': 0}, {'gateway_paused_missing_body_urls': 5},
    {'next_source_probe_at_utc': None}, {'next_source_probe_at_utc': 'PRIVATE_TIME'},
    {'next_source_probe_at_utc': (NOW + timedelta(days=2)).isoformat()},
    {'source_recovery_verified': True}, {'available': False}])
def test_invalid_request_scope_counts_are_unknown_and_cannot_reenable_eta(tmp_path, patch):
    status = _request_scope_status(tmp_path)
    status['providers']['cnyes']['body_work_progress']['request_scope_progress'].update(patch)
    _save(tmp_path, status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['body_request_scope_progress'] is None
    assert source['conditional_eta_seconds'] is None
    assert source['conditional_eta_unavailable_reason'] == 'source_request_scope_unknown'


def test_explicit_unavailable_request_scope_is_unknown_not_zero_or_completion(tmp_path):
    status = _request_scope_status(tmp_path)
    status['providers']['cnyes']['body_work_progress']['request_scope_progress'] = {
        'contract_version': 1, 'available': False, 'source_recovery_verified': False}
    _save(tmp_path, status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    scope = source['body_request_scope_progress']
    assert not scope['available'] and scope['source_paused_missing_body_urls'] is None
    assert source['conditional_eta_seconds'] is None and source['remaining_urls'] == 4


def test_endpoint_parser_circuits_are_finite_private_and_not_provider_refusals(tmp_path):
    status = _seed(tmp_path)
    status['bulk_endpoint_cooldowns'] = [{'provider': 'cnyes', 'not_before': NOW.timestamp() + 300,
        'endpoint': 'https://api.cnyes.com/PRIVATE_TOKEN', 'request_url': 'PRIVATE_BODY', 'error_code': 'PRIVATE_REASON'},
        {'provider': 'cnyes', 'not_before': NOW.timestamp() - 1},
        {'provider': 'cnyes', 'not_before': float('nan')}]
    status['bulk_api_pages_temporarily_blocked'] = 10
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(p for p in result['providers'] if p['id'] == 'cnyes')
    assert source['temporarily_blocked_bulk_endpoints'] == 1
    assert result['acquisition']['temporarily_blocked_bulk_pages'] == 10
    assert source['next_attempt_at_utc'] is None
    assert all(value not in json.dumps(result) for value in ['PRIVATE_TOKEN', 'PRIVATE_BODY', 'PRIVATE_REASON'])


@pytest.mark.parametrize("state", ["recovering_transport", "endpoint_validation_failed"])
def test_capacity_recovery_and_endpoint_errors_are_not_reported_as_source_limits(tmp_path, state):
    status = _seed(tmp_path)
    status["http_capacity"] = {
        "providers": {"cnyes": {"effective_interval_seconds": 0.5, "parallelism": 1}},
        "calibration": {"providers": {"cnyes": {
            "state": state, "reason": "PRIVATE_RECOVERY_REASON", "stable_samples": 0,
        }}},
    }
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(p for p in result["providers"] if p["id"] == "cnyes")
    assert source["capacity_state"] == state
    assert source["target_requests_per_second"] == 2
    assert source["source_maximum_verified"] is False
    assert "PRIVATE_RECOVERY_REASON" not in json.dumps(result)


@pytest.mark.parametrize("kind", ["stale", "future", "stopped", "cooldown", "projection_elapsed"])
def test_old_stopped_or_cooling_forecasts_never_stay_live(tmp_path, kind):
    status = _seed(tmp_path)
    now = NOW
    if kind == "stale":
        now += timedelta(seconds=181)
    elif kind == "future":
        now -= timedelta(seconds=6)
    elif kind == "stopped":
        status["activity"] = "stopped"
    elif kind == "projection_elapsed":
        now += timedelta(seconds=41)
    else:
        status["provider_next_attempt_utc"]["cnyes"] = (NOW + timedelta(seconds=30)).isoformat()
        status["performance"]["providers"]["cnyes"]["counts"]["http_status_403"] = 1
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=now)
    source = next(source for source in result["providers"] if source["id"] == "cnyes")
    assert source["conditional_eta_seconds"] is None
    assert result["acquisition"]["eligible_eta_seconds"] is None
    assert result["acquisition"]["stored_body_articles"] == 2
    if kind == "cooldown":
        assert source["state"] == "cooldown" and source["http_errors"] == {"403": 1}


@pytest.mark.parametrize("activity,health,forecast", [
    ("cycle_complete", "waiting", True), ("waiting_next_due", "waiting", True),
    ("inventorying", "inventorying", True), ("draining", "draining", False),
    ("waiting_disk", "waiting_disk", False), ("interrupted", "stopped", False),
    ("service_duration_complete", "stopped", False), ("selected_urls_complete", "stopped", False),
    ("inventory_complete", "stopped", False), ("configuration_reload_requested", "waiting_restart", False),
])
def test_canonical_owner_phases_distinguish_warm_cycles_pauses_and_terminal_jobs(tmp_path, activity, health, forecast):
    status = _seed(tmp_path)
    status["activity"] = activity
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["health"] == health
    assert result["worker"]["source_activity"] == activity
    assert result["acquisition"]["stored_body_articles"] == 2
    assert (result["acquisition"]["eligible_eta_seconds"] is not None) is forecast
    if not forecast:
        assert result["providers"][0]["state"] == health


@pytest.mark.parametrize("value", [None, True, -1, "10", float("nan"), float("inf"), 2**2000])
def test_invalid_numeric_values_are_unknown_and_do_not_escape(tmp_path, value):
    status = _seed(tmp_path)
    status["performance"]["owner_elapsed_seconds"] = value
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["worker"]["owner_elapsed_seconds"] is None
    assert result["acquisition"]["eligible_eta_seconds"] is None
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("value", [None, True, 0, 2, "1"])
def test_missing_or_incompatible_snapshots_are_not_zero_progress(tmp_path, value):
    status = _seed(tmp_path)
    status["contract_version"] = value
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["health"] == "invalid"
    assert result["acquisition"]["progress_ratio"] is None
    assert result["acquisition"]["known_urls"] is None


def test_absent_snapshot_does_not_download_or_open_a_queue(tmp_path):
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["health"] == "unavailable"
    assert result["acquisition"]["stored_body_articles"] is None
    assert list(tmp_path.iterdir()) == []
    assert news_snapshot_revision(tmp_path) == "missing:missing"


def test_inconsistent_counts_fail_closed_and_forecasts_are_suppressed(tmp_path):
    status = _seed(tmp_path)
    status["unique_articles"] = 100
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["health"] == "degraded"
    assert result["acquisition"]["progress_ratio"] is None
    assert all(source["conditional_eta_seconds"] is None for source in result["providers"])


def test_equal_total_but_different_provider_states_does_not_claim_completion(tmp_path):
    status = _seed(tmp_path)
    # The global count is still ten. The provider snapshot, however, claims
    # that the four globally pending CNYES URLs are already complete.
    status['providers']['cnyes']['states'] = {'complete': 6}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert result['health'] == 'degraded' and not result['snapshot_consistent']
    assert not result['provider_state_totals_reconciled']
    assert result['acquisition']['progress_ratio'] is None
    assert result['acquisition']['known_urls'] is result['acquisition']['stored_body_articles'] is None
    assert result['acquisition']['remaining_urls'] is result['acquisition']['text_bytes'] is None
    assert result['outcome_progress'] is None
    assert source['state'] == 'unavailable'
    assert source['progress_ratio'] is source['stored_body_progress_ratio'] is None
    assert source['conditional_eta_seconds'] is source['completion_forecast'] is None
    assert source['known_urls'] is source['complete_urls'] is source['remaining_urls'] is None
    assert source['states'] == {} and source['outcome_progress'] is None
    assert source['adapter_acceptance'] is None


def test_inconsistent_snapshot_cannot_claim_a_finite_cohort_is_complete(tmp_path):
    status = _seed(tmp_path)
    source = status['providers']['cnyes']
    source.update(states={'complete': 6}, acquisition_mode='finite',
                  finite_cohort={'urls': 6, 'stored_bodies': 6})
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert not result['snapshot_consistent']
    assert source['state'] == 'unavailable' and source['finite_cohort'] is None


@pytest.mark.parametrize('where,value', [
    ('global', -1), ('global', True), ('global', '1'),
    ('provider', -1), ('provider', True), ('provider', '1'),
])
def test_invalid_state_value_cannot_be_dropped_to_create_consistency(tmp_path, where, value):
    status = _seed(tmp_path)
    states = status['states'] if where == 'global' else status['providers']['cnyes']['states']
    states['invalid_extra_state'] = value
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['health'] == 'degraded' and not result['snapshot_consistent']
    assert result['acquisition']['progress_ratio'] is None


def test_unknown_states_are_reconciled_without_leaking_private_names(tmp_path):
    status = _seed(tmp_path)
    for states in (status['states'], status['providers']['cnyes']['states']):
        states['pending'] -= 1
        states['private_unrecognized_state'] = 1
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['snapshot_consistent'] and result['provider_state_totals_reconciled']
    assert result['states']['other'] == 1
    assert 'private_unrecognized_state' not in json.dumps(result)


def test_redacted_unknown_states_cannot_hide_mismatched_original_names(tmp_path):
    status = _seed(tmp_path)
    status['states']['pending'] -= 1
    status['states']['private_unknown_one'] = 1
    provider_states = status['providers']['cnyes']['states']
    provider_states['pending'] -= 1
    provider_states['private_unknown_two'] = 1
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert not result['snapshot_consistent'] and not result['provider_state_totals_reconciled']
    assert result['health'] == 'degraded' and 'private_unknown_' not in json.dumps(result)


@pytest.mark.parametrize('invalid', [None, [], 'bad'])
def test_missing_or_malformed_provider_states_are_not_zero(tmp_path, invalid):
    status = _seed(tmp_path)
    status['providers']['cnyes']['states'] = invalid
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['health'] == 'degraded' and not result['snapshot_consistent']
    assert not result['provider_state_totals_reconciled']


def test_inconsistent_snapshot_preserves_confirmed_stopped_state(tmp_path):
    status = _seed(tmp_path)
    status['activity'] = 'stopped'
    status['providers']['cnyes']['states'] = {'complete': 6}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert result['health'] == 'degraded' and not result['snapshot_consistent']
    assert source['state'] == 'stopped'
    assert source['progress_ratio'] is None and source['conditional_eta_seconds'] is None


def test_all_sources_remain_in_the_inventory_and_never_publish_free_form_errors(tmp_path):
    status = _seed(tmp_path)
    for number in range(1000):
        status["providers"][f"unregistered:source{number}.example.org"] = {"states": {"awaiting_authorization": 1}, "error": "PRIVATE_TOKEN"}
    status["unique_articles"] += 1000
    status["states"]["awaiting_authorization"] += 1000
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert len(result["providers"]) == 1002
    assert result["snapshot_consistent"]
    assert "PRIVATE_TOKEN" not in json.dumps(result)


@pytest.mark.parametrize("reason", ["article_identity_not_verified", "article_rights_not_verified",
    "article_rights_or_scope_excluded", "robots_not_plain_rules", "robots_redirect_refused",
    "paywall_or_login", "access_challenge"])
def test_finite_source_failure_reasons_are_not_collapsed_into_other(tmp_path, reason):
    status = _seed(tmp_path)
    status["states"][reason] = 1
    status["states"]["pending"] -= 1
    status["providers"]["cnyes"]["states"][reason] = 1
    status["providers"]["cnyes"]["states"]["pending"] -= 1
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["snapshot_consistent"]
    source = next(p for p in result["providers"] if p["id"] == "cnyes")
    assert source["states"][reason] == 1 and "other" not in source["states"]
    assert source["blocked_urls"] == 1


def test_canonical_routes_and_immediate_snapshot_revision_invalidation(tmp_path, monkeypatch):
    _seed(tmp_path, now=datetime.now(UTC))
    original = gateway.build_news_public_status
    calls = []
    def build(root):
        calls.append(root)
        return original(root)
    monkeypatch.setattr(gateway, "build_news_public_status", build)
    server = _server(tmp_path)
    try:
        handler = SimpleNamespace(server=server)
        for route in ("/news/", "/news/index.html", "/news/app.js", "/news/styles.css"):
            assert gateway.PublicDashboardHandler._static_response(handler, route) is not None
        assert gateway.PublicDashboardHandler._static_response(handler, "/news/../status.json") is None
        first = gateway.PublicDashboardHandler._api_response(handler, "/news/api/status", "")
        assert first is gateway.PublicDashboardHandler._api_response(handler, "/news/api/status", "")
        assert len(calls) == 1 and first.cache_control == "no-store"
        status = _seed(tmp_path, now=datetime.now(UTC))
        status["articles_with_stored_body"] = 3
        _save(tmp_path, status)
        second = gateway.PublicDashboardHandler._api_response(handler, "/news/api/status", "")
        assert json.loads(second.body)["acquisition"]["stored_body_articles"] == 3
        assert len(calls) == 2
        server._store_cached_response("unrelated-status", first, ttl_seconds=60)
        for _ in range(12):
            _save(tmp_path, status)
            gateway.PublicDashboardHandler._api_response(handler, "/news/api/status", "")
        assert sum(key.startswith("news-status:") for key in server._cache) == 1
        assert "unrelated-status" in server._cache
        assert server._cache_bytes == sum(entry.response.resident_bytes for entry in server._cache.values())
    finally:
        server.server_close()


@pytest.mark.parametrize('monotonic_delta', [0.1, 2.0])
def test_cached_news_response_cannot_extend_backing_sample_validity(tmp_path, monkeypatch, monotonic_delta):
    current = [datetime.now(UTC)]
    status = _seed(tmp_path, now=current[0])
    status.update(activity='waiting_next_due', disk_admission={**disk_sample(free=30),
        'checked_at_utc': (current[0]-timedelta(seconds=89)).isoformat(),
        'physical_backing_state': 'available', 'physical_probe_inflight': True})
    _save(tmp_path, status)
    original = gateway.build_news_public_status
    monkeypatch.setattr(gateway, 'build_news_public_status', lambda root: original(root, now=current[0]))
    monkeypatch.setattr(gateway.time, 'time', lambda: current[0].timestamp())
    monotonic = [1000.0]
    monkeypatch.setattr(gateway.time, 'monotonic', lambda: monotonic[0])
    server = _server(tmp_path)
    try:
        handler = SimpleNamespace(server=server)
        first = gateway.PublicDashboardHandler._api_response(handler, '/news/api/status', '')
        assert json.loads(first.body)['worker']['disk_admission']['fetch_allowed']
        current[0] += timedelta(seconds=2)
        monotonic[0] += monotonic_delta
        expired = gateway.PublicDashboardHandler._api_response(handler, '/news/api/status', '')
        assert expired is not first
        projected = json.loads(expired.body)
        assert projected['worker']['disk_admission'] is None and projected['health'] == 'unavailable'
    finally:
        server.server_close()


@pytest.mark.parametrize("path", ["/news", "/news/", "/news/app.js", "/news/api/status"])
@pytest.mark.parametrize("headers", [{}, {"X-StockAgent-Audience": "owner",
    "X-WireGuard-Role": "owner", "X-Forwarded-For": "192.168.50.100"}])
def test_public_news_is_denied_even_with_client_supplied_owner_headers(tmp_path, path, headers):
    _seed(tmp_path, now=datetime.now(UTC))
    server = _server(tmp_path, audience="public")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    conn = HTTPConnection(*server.server_address, timeout=5)
    try:
        conn.request("GET", path, headers=headers)
        response = conn.getresponse()
        assert response.status == 403
        assert json.loads(response.read()) == {"error": "access_denied"}
        assert response.getheader("Cache-Control") == "no-store"
        assert response.getheader("X-StockAgent-Audience") == "public"
    finally:
        conn.close(); server.shutdown(); server.server_close(); thread.join(timeout=2)


def test_news_endpoint_is_same_origin_and_rejects_control_requests(tmp_path):
    _seed(tmp_path, now=datetime.now(UTC))
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    conn = HTTPConnection(*server.server_address, timeout=5)
    try:
        conn.request("GET", "/news")
        response = conn.getresponse(); response.read()
        assert response.status == 308 and response.getheader("Location") == "/news/"
        conn.request("GET", "/news/api/status")
        response = conn.getresponse()
        assert response.status == 200 and json.loads(response.read())["read_only"]
        conn.request("POST", "/news/api/status")
        response = conn.getresponse(); response.read()
        assert response.status in {405, 501}
    finally:
        conn.close(); server.shutdown(); server.server_close(); thread.join(timeout=2)


def test_news_browser_distinguishes_unknown_backing_from_measured_low_disk(tmp_path):
    from playwright.sync_api import sync_playwright
    from scripts.probe_browser_runtime import browser_profile_launch_options
    now = datetime.now(UTC)
    status = _seed(tmp_path, now=now)
    status.update(activity='inventorying', disk_admission={**unknown_backing_sample(), 'checked_at_utc': now.isoformat()})
    _save(tmp_path, status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    base = f'http://127.0.0.1:{server.server_address[1]}'
    try:
        with sync_playwright() as runtime:
            browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
            try:
                page = browser.new_page(viewport={'width': 390, 'height': 900})
                remote, errors = [], []
                page.on('request', lambda request: remote.append(request.url) if not request.url.startswith(base) else None)
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.goto(base + '/news/', wait_until='domcontentloaded')
                page.locator('#news-health').get_by_text('等待磁碟空間').wait_for(timeout=10000)
                for state, inflight, message in [
                    ('pending', True, '正在查證實體磁碟空間'),
                    ('unavailable', False, '實體磁碟空間查證失敗'),
                    ('stale', True, '上次實體磁碟空間查證已過期'),
                ]:
                    status['disk_admission'] = {**unknown_backing_sample(state, inflight=inflight),
                                                'checked_at_utc': now.isoformat()}
                    _save(tmp_path, status)
                    projected = build_news_public_status(tmp_path, now=now)
                    page.evaluate('(data) => render(data)', projected)
                    assert message in page.locator('#capture-next').text_content()
                    assert '實體磁碟空間尚未確認' in page.locator('#eligible-eta-basis').text_content()
                    assert '磁碟可用' not in page.locator('#capture-next').text_content()
                    assert '暫無有效估時' in page.locator('#eligible-eta').text_content()
                status['disk_admission'] = {**disk_sample(), 'checked_at_utc': now.isoformat()}
                _save(tmp_path, status)
                page.evaluate('(data) => render(data)', build_news_public_status(tmp_path, now=now))
                assert '磁碟可用' in page.locator('#capture-next').text_content()
                assert '磁碟不足' in page.locator('#eligible-eta-basis').text_content()
                status['disk_admission'] = {**disk_sample(free=25), 'checked_at_utc': now.isoformat()}
                _save(tmp_path, status)
                page.evaluate('(data) => render(data)', build_news_public_status(tmp_path, now=now))
                assert '到期 feed 優先' in page.locator('#capture-next').text_content()
                assert not remote and not errors
            finally:
                browser.close()
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)


def test_news_browser_shows_discovery_errors_without_calling_publishers(tmp_path):
    from playwright.sync_api import sync_playwright
    from scripts.probe_browser_runtime import browser_profile_launch_options

    now = datetime.now(UTC)
    status = _discovery_error_snapshot(tmp_path, now=now)
    status['discovery_error_rollup']['providers']['cnyes']['reasons'] = {'http_404': 1, 'restricted_content': 1}
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{server.server_address[1]}'
    try:
        with sync_playwright() as runtime:
            browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
            try:
                page = browser.new_page(viewport={'width': 390, 'height': 900})
                remote, errors = [], []
                page.on('request', lambda request: remote.append(request.url) if not request.url.startswith(base) else None)
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.goto(base + '/news/', wait_until='domcontentloaded')
                page.locator('#news-health').get_by_text('正在抓取').wait_for(timeout=10000)
                page.evaluate("StockAgentAcquisition.reveal('pipelines')")
                page.locator('#pipelines').scroll_into_view_if_needed()
                detail = page.locator('[data-news-discovery-provider="cnyes"]')
                try:
                    page.locator('#pipeline-grid .pipeline-card').first.wait_for(timeout=10000)
                except Exception as error:
                    raise AssertionError({'page_errors': errors, 'grid': page.locator('#pipeline-grid').text_content()}) from error
                assert detail.count() == 1, {'grid': page.locator('#pipeline-grid').text_content(), 'errors': errors}
                assert '清點異常 2 頁' in detail.locator('summary').text_content()
                detail.locator('summary').click()
                assert 'HTTP 404 1 頁' in detail.text_content()
                assert '不能只靠此原因判斷卡住' in detail.text_content()
                assert '不代表整個來源停擺' in detail.text_content()
                detail.locator('summary').focus()
                page.evaluate('(data) => render(data)', build_news_public_status(tmp_path, now=now))
                detail = page.locator('[data-news-discovery-provider="cnyes"]')
                assert detail.get_attribute('open') is not None
                assert page.evaluate('document.activeElement === document.querySelector(\'[data-news-discovery-provider="cnyes"] > summary\')')
                page.locator('[data-news-filter=attention]').click()
                assert page.locator('[data-news-discovery-provider="cnyes"]').count() == 1
                assert 'PRIVATE_' not in page.locator('body').text_content()
                assert not remote and not errors
            finally:
                browser.close()
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)


def test_news_browser_labels_local_sqlite_busy_as_past_event(tmp_path):
    from playwright.sync_api import sync_playwright
    from scripts.probe_browser_runtime import browser_profile_launch_options

    now = datetime.now(UTC)
    status = _seed(tmp_path, now=now)
    status['performance']['providers']['cnyes']['counts']['local_sqlite_busy_commit_events'] = 2
    _save(tmp_path, status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{server.server_address[1]}'
    try:
        with sync_playwright() as runtime:
            browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
            try:
                page = browser.new_page(viewport={'width': 390, 'height': 900})
                remote, errors = [], []
                page.on('request', lambda request: remote.append(request.url) if not request.url.startswith(base) else None)
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.goto(base + '/news/', wait_until='domcontentloaded')
                page.locator('#news-health').get_by_text('正在抓取').wait_for(timeout=10000)
                page.evaluate("StockAgentAcquisition.reveal('traffic')")
                page.locator('#traffic').scroll_into_view_if_needed()
                page.locator('#traffic-rows').get_by_text('本機批次提交曾遇資料庫鎖定', exact=False).wait_for(timeout=10000)
                text = page.locator('#traffic-rows').text_content()
                assert '本程序：本機批次提交曾遇資料庫鎖定 2 次' in text
                assert '涉及此來源，非來源限流' in text
                status['activity'] = 'stopped'
                _save(tmp_path, status)
                stopped = build_news_public_status(tmp_path, now=now)
                assert stopped['health'] == 'stopped'
                page.evaluate('(data) => render(data)', stopped)
                page.locator('#traffic-rows').get_by_text('上次快照：本機批次提交曾遇資料庫鎖定', exact=False).wait_for(timeout=10000)
                text = page.locator('#traffic-rows').text_content()
                assert '上次快照：本機批次提交曾遇資料庫鎖定 2 次' in text
                assert '本程序：本機批次提交曾遇資料庫鎖定' not in text
                assert 'PRIVATE_' not in page.locator('body').text_content()
                assert not remote and not errors
            finally:
                browser.close()
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)


def test_news_browser_filters_pages_recovers_and_never_calls_publishers(tmp_path):
    from playwright.sync_api import sync_playwright
    from scripts.probe_browser_runtime import browser_profile_launch_options
    status = _seed(tmp_path, now=datetime.now(UTC))
    status['performance']['robots_origin_policy_contract_version'] = 1
    status['http_capacity'] = {'providers': {'cnyes': {
        'effective_interval_seconds': 0.5, 'parallelism': 3,
        'temporarily_paused_robots_origins': {'https://amp-news.cnyes.com': datetime.now(UTC).timestamp() + 120}}}}
    status["states"].update(pending=2, source_body_image_only=1, source_body_embedded_only=1)
    status["providers"]["cnyes"]["states"].update(pending=2, source_body_image_only=1, source_body_embedded_only=1)
    for number in range(50):
        status["providers"][f"unregistered:source{number}.example.org"] = {"states": {"awaiting_authorization": 1}}
    status["unique_articles"] += 50
    status["states"]["awaiting_authorization"] += 50
    _save(tmp_path, status)
    config_path = tmp_path / "configs/news_fulltext.json"
    config = json.loads(config_path.read_text())
    config["providers"].extend({"id": f"additional_{number}", "name": f"額外來源 {number}",
                                "enabled": False, "interval_seconds": 10, "parallelism": 1} for number in range(10))
    atomic_write_json(config_path, config)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with sync_playwright() as runtime:
            browser = runtime.chromium.launch(**browser_profile_launch_options("cpu-2d"), timeout=10000)
            try:
                for width in (1280, 390, 320):
                    page = browser.new_page(viewport={"width": width, "height": 900})
                    errors, remote_calls = [], []
                    page.on("pageerror", lambda error: errors.append(str(error)))
                    page.on("request", lambda request: remote_calls.append(request.url) if not request.url.startswith(base) else None)
                    page.goto(base + "/news/", wait_until="domcontentloaded")
                    page.locator("#news-health").get_by_text("正在抓取").wait_for(timeout=10000)
                    assert "2 / 60" in page.locator("#download-progress-label").text_content()
                    assert page.locator("#global-eta").text_content() == "未知"
                    page.evaluate("StockAgentAcquisition.reveal('pipelines')")
                    page.locator("[data-news-filter=enabled]").click()
                    page.wait_for_function("() => document.querySelectorAll('#pipeline-grid .pipeline-card').length === 1")
                    assert "來源正文僅圖片，未做 OCR" in page.locator("#pipeline-grid").text_content()
                    assert "來源正文僅外部嵌入，未取得文字" in page.locator("#pipeline-grid").text_content()
                    page.locator("[data-news-filter=all]").click()
                    page.wait_for_function("() => document.querySelectorAll('#pipeline-grid .pipeline-card').length === 24")
                    page.locator("#source-next").click()
                    assert page.locator("#source-page").text_content() == "2 / 3"
                    page.locator("#source-search").fill("no-such-source")
                    page.locator("#pipeline-empty").wait_for(state="visible")
                    page.locator("#source-search").fill("鉅亨")
                    page.wait_for_function("() => document.querySelectorAll('#pipeline-grid .pipeline-card').length === 1")
                    card = page.locator('#pipeline-grid .pipeline-card')
                    assert '局部端點暫停 1 個 origin' in card.text_content()
                    assert '到期不代表恢復存取' in card.text_content()
                    assert '整個來源最早再排' not in card.text_content()
                    page.locator('[data-news-filter="attention"]').click()
                    assert page.locator('#pipeline-grid .pipeline-card').count() == 1
                    page.locator('[data-news-filter="all"]').click()
                    page.locator("#source-sort").select_option("remaining")
                    page.route("**/news/api/status", lambda route: route.fulfill(status=503, content_type="application/json", body='{"error":"unavailable"}'))
                    page.evaluate("refresh()")
                    page.locator("#news-health").get_by_text("更新暫停，顯示上次資料").wait_for(timeout=5000)
                    assert page.locator("#body-count").text_content() == "2"
                    assert "更新失敗" in page.locator("#eligible-eta").text_content()
                    assert '觀測已過期' in page.locator('#pipeline-grid .pipeline-card').text_content()
                    assert '上次記錄局部端點暫停' in page.locator('#pipeline-grid .pipeline-card').text_content()
                    assert '其他可用端點照常排程' not in page.locator('#pipeline-grid .pipeline-card').text_content()
                    page.locator('a[href="#acq-quota"]').click()
                    page.wait_for_function("() => document.querySelectorAll('#traffic-rows tr').length === 11")
                    assert '觀測已過期' in page.locator('#traffic-rows').text_content()
                    page.locator('a[href="#acq-catalog"]').click()
                    page.unroute("**/news/api/status")
                    page.locator("#news-health").get_by_text("正在抓取").wait_for(timeout=8000)
                    assert page.locator("#source-search").input_value() == "鉅亨"
                    page.locator("#source-search").fill("")
                    page.wait_for_function("() => document.querySelectorAll('#pipeline-grid .pipeline-card').length === 24")
                    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                    page.locator('a[href="#acq-quota"]').click()
                    try:
                        page.locator("#traffic").scroll_into_view_if_needed()
                    except Exception:
                        print(json.dumps({'viewport_width': width, 'quota_navigation': page.evaluate("""() => ({
                            hash: location.hash, trafficHidden: document.querySelector('#traffic')?.hidden,
                            groups: [...document.querySelectorAll('[data-acq-group]')].map(n => ({
                                id: n.id, hidden: n.hidden, display: getComputedStyle(n).display
                            }))
                        })""")}), flush=True)
                        page.screenshot(path=str(tmp_path / f'news-quota-navigation-{width}.png'))
                        raise
                    page.wait_for_function("() => document.querySelectorAll('#traffic-rows tr').length === 11")
                    assert page.evaluate("""() => {
                        const caption = document.querySelector('#traffic caption').getBoundingClientRect();
                        const table = document.querySelector('#traffic table').getBoundingClientRect();
                        return caption.width >= table.width * 0.95 && caption.height < 160;
                    }""")
                    assert page.evaluate("""() => {
                        const section = document.querySelector('#traffic').getBoundingClientRect();
                        const table = document.querySelector('#traffic table').getBoundingClientRect();
                        const scroller = document.querySelector('#traffic .table-scroll');
                        const overflow = getComputedStyle(scroller).overflowY;
                        const catalog = document.querySelector('#acq-catalog').getBoundingClientRect();
                        return (section.bottom >= table.bottom ||
                            (['auto', 'scroll', 'hidden', 'clip'].includes(overflow) &&
                                section.bottom >= scroller.getBoundingClientRect().bottom)) && catalog.top >= section.bottom;
                    }""")
                    assert page.locator('[data-dashboard-nav] a[aria-current="page"]').count() == 1
                    assert not errors and not remote_calls
                    page.close()
            finally:
                browser.close()
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)
