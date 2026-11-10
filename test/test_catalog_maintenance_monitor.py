"""Only live catalog observations, not worker liveness, describe progress."""
import json

import pytest

from stockagent.data_sync import storage_progress as ops


@pytest.mark.parametrize(('state', 'reason', 'expected'), [
    ('running', None, 'running'),
    ('waiting', 'tw_day_trade_priority_window', 'waiting'),
    ('waiting', 'catalog_owner_busy', 'waiting'),
    ('waiting', 'catalog_cold_capacity', 'waiting'),
    ('blocked', 'catalog_refresh_failed', 'blocked'),
])
def test_catalog_observation_projects_liveness_not_acceptance(tmp_path, state, reason, expected):
    now = 1794189000
    value = {'contract': 'stockagent.catalog_activity_observation.v1', 'state': state,
             'reason': reason, 'observed_at_utc': ops.iso(now), 'catalog_acceptance_claimed': False}
    (tmp_path / 'catalog-activity-observation.json').write_text(json.dumps(value))
    row, = ops.catalog_maintenance_tasks(tmp_path)
    public = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(now),
                                'tasks': [row]}, now=now)
    visible, = public['tasks']
    assert visible['state'] == expected
    assert visible['complete_evidence'] is False
    assert visible['progress']['total_items'] is None
    assert visible['eta']['completion_at_utc'] is None
    if reason == 'tw_day_trade_priority_window':
        assert '當沖' in visible['reason']


def test_catalog_observation_rejects_forged_acceptance_or_unknown_reason(tmp_path):
    path = tmp_path / 'catalog-activity-observation.json'
    value = {'contract': 'stockagent.catalog_activity_observation.v1', 'state': 'waiting',
             'reason': 'arbitrary private detail', 'observed_at_utc': ops.iso(1794189000),
             'catalog_acceptance_claimed': False}
    path.write_text(json.dumps(value))
    assert ops.catalog_maintenance_tasks(tmp_path) == []
    value.update(reason='catalog_cycle_complete', catalog_acceptance_claimed=True)
    path.write_text(json.dumps(value))
    assert ops.catalog_maintenance_tasks(tmp_path) == []


def test_daily_root_queue_reports_due_and_ambiguous_without_false_completion(tmp_path):
    now = 1794189000
    root = tmp_path/'dataset-versions'
    operation = root/'operations'/('a'*16)
    operation.mkdir(parents=True)
    (root/'current.json').write_text(json.dumps({'operation_directory': str(operation), 'plan_sha256': 'b'*64}))
    progress = {'contract': 'all_catalog_incremental_migration_v1', 'updated_at_utc': ops.iso(now),
                'source_count': 1, 'datasets': {'example': {'state': 'recovery_deferred_user_migration'}},
                'whole_root_refresh': {'contract': 'stockagent.whole_root_refresh_queue.v1',
                    'scope': 'one_unexcluded_owner_per_registered_data_root', 'registered_roots': 33,
                    'eligible_roots': 30, 'due_roots': 8, 'ambiguous_roots': 2,
                    'window_allowed': False, 'source_freshness_claimed': False}}
    (operation/'progress.json').write_text(json.dumps(progress))
    row = next(row for row in ops.migration_tasks(root, now=now) if row['kind'] == 'whole_root_refresh')
    assert row['state'] == 'blocked' and row['reason'] == 'root_refresh_ambiguous'
    assert row['metrics']['whole_root_due'] == 8 and row['metrics']['whole_root_ambiguous'] == 2
    visible = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(now),
                                 'tasks': [row]}, now=now)['tasks'][0]
    assert visible['complete_evidence'] is False and visible['eta']['completion_at_utc'] is None
    assert visible['metrics']['whole_root_registered'] == 33


def test_latest_successor_never_looks_like_verified_old_history(tmp_path):
    now = 1794189000
    root = tmp_path/'dataset-versions'
    operation = root/'operations'/('c'*16)
    operation.mkdir(parents=True)
    (root/'current.json').write_text(json.dumps({'operation_directory': str(operation), 'plan_sha256': 'd'*64}))
    rebinding = {'contract': 'stockagent.refresh_latest_successor_binding.v1',
        'manifest_sha256': 'a'*64, 'changed_member_values_compared': False,
        'old_historical_value_coverage': 'not_compared', 'historical_recovery_claimed': False}
    (operation/'progress.json').write_text(json.dumps({'contract': 'all_catalog_incremental_migration_v1',
        'updated_at_utc': ops.iso(now), 'source_count': 1,
        'datasets': {'yahoo-market': {'state': 'recovery_deferred_user_migration',
                                    'freshness_refresh': {'base_rebinding': rebinding}}}}))
    row = next(row for row in ops.migration_tasks(root, now=now)
               if row.get('reason') == 'latest_successor_history_not_compared')
    visible = ops.public_status({'contract': ops.CONTRACT, 'observed_at_utc': ops.iso(now),
                                 'tasks': [row]}, now=now)['tasks'][0]
    assert visible['state'] == 'waiting' and visible['complete_evidence'] is False
    assert '未逐項比較' in visible['reason']
