import json
from datetime import datetime

import pytest

from stockagent.data_sync import archive_plan, storage_progress as progress


@pytest.fixture
def policy():
    columnar={'dataset_version_lifecycle':{'enabled':True,
        'daily_merge_window':{'timezone':'Asia/Taipei','start_hour':3,'end_hour':7},
        'ordinary_history_retention':'current-and-research-pins','ordinary_history_migration_catchup':True},
        'source_versioning':{'logical_version':'complete_inventory_not_delta_chain',
            'physical_version':'reuse_immutable_parts_and_sha256_chunks','file_chunk_bytes':8*1024**2,
            'private_history':{'source_retirement':False,'secret':'do-not-publish'}}}
    placement={'enabled':True,'maximum_wave_bytes':8*1024**3,'workspace_root':'/private/root'}
    return archive_plan.from_config(columnar,placement,pack_target_bytes=4*1024**3)


def test_policy_does_not_claim_raw_retirement_is_finished_or_leak_config(policy):
    assert policy['general_raw_retirement_enabled'] is False
    assert policy['dataset_pack_target_bytes']==4*1024**3
    assert policy['maximum_transfer_wave_bytes']==8*1024**3
    assert '/private' not in json.dumps(policy) and 'do-not-publish' not in json.dumps(policy)


def test_invalid_size_and_unrecognized_policy_are_not_published(policy):
    assert archive_plan.public_policy({'contract':'different'}) is None
    value={**policy,'maximum_transfer_wave_bytes':True,'untrusted':'/root/private'}
    clean=archive_plan.public_policy(value)
    assert clean['maximum_transfer_wave_bytes'] is None and 'untrusted' not in clean
    value['daily_merge_window']={'timezone':'Asia/Taipei','start_hour':3,'end_hour':3}
    assert archive_plan.public_policy(value) is None


def snapshot(policy,now,rows=()):
    return progress.public_status({'contract':progress.CONTRACT,
        'observed_at_utc':progress.iso(now),'archive_policy':policy,'tasks':list(rows)},now=now)


def test_next_merge_slot_is_eligibility_not_a_completion_prediction(policy):
    now=datetime.fromisoformat('2026-11-10T22:00:00+08:00').timestamp()
    value=snapshot(policy,now)
    expected=datetime.fromisoformat('2026-11-11T03:00:00+08:00').timestamp()
    assert progress.timestamp(value['archive_policy']['next_daily_merge_start_at_utc'])==expected
    assert value['archive_policy']['daily_window_open'] is False
    assert value['local_migration_eta']['known_completion_at_utc'] is None


def test_open_window_and_stale_observation_do_not_invent_future_start(policy):
    now=datetime.fromisoformat('2026-11-11T03:10:00+08:00').timestamp()
    value=snapshot(policy,now)['archive_policy']
    assert value['daily_window_open'] and value['next_daily_merge_start_at_utc'] is None
    stale=progress.public_status({'contract':progress.CONTRACT,'archive_policy':policy,
        'observed_at_utc':progress.iso(now-1000)},now=now)['archive_policy']
    assert stale['daily_window_open'] is None and stale['next_daily_merge_start_at_utc'] is None


def test_all_local_stages_are_visible_and_nas_or_recovery_cannot_gate_local_retirement(policy):
    value=snapshot(policy,datetime.now().timestamp())
    stages={row['id']:row for row in value['flow']}
    assert archive_plan.LOCAL_STAGES <= set(stages)
    assert all(stages[name]['branch']=='local_migration' for name in archive_plan.LOCAL_STAGES)
    assert not stages['recover']['required_for_local_migration']
    assert not stages['nas']['required_for_local_migration']
    assert 'recover' not in stages['retire']['depends_on']
    assert all('Vast' not in row['label'] for row in stages.values())


def test_remote_or_deferred_recovery_forecast_does_not_shift_local_eta(policy):
    now=datetime.fromisoformat('2026-11-10T22:00:00+08:00').timestamp()
    def running(kind,seconds):
        row=progress.task(kind,kind,state='running',phase='working',observed=now,
            done_items=1,total_items=100,done_bytes=100,total_bytes=1000)
        row.update(checked_at_utc=progress.iso(now),owner_state='active',
            eta={'state':'estimated','completion_at_utc':progress.iso(now+seconds),
                 'unit':'bytes','rate_per_second':1})
        return row
    value=snapshot(policy,now,[running('packed_archive_placement',100),running('receiver',1000),
                               running('migration',2000)])
    assert value['local_migration_eta']['known_completion_at_utc']==progress.iso(now+100)
    assert value['overall_eta']['known_completion_at_utc']==progress.iso(now+2000)
    assert value['local_migration_eta']['scope']=='local_migration_measured_steps_only'
