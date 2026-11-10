import json
import time

from stockagent.data_sync import storage_progress as progress


def receipt(tmp_path, entry):
    value = {'contract': 'stockagent.packed_object_placement.v1.queue',
             'observed_at_epoch': time.time(), 'entries': [entry],
             'totals': {'datasets': [{'dataset': 'local-tw-minute',
                 'retired_D_allocated_bytes': 4096}]}}
    (tmp_path/'status.json').write_text(json.dumps(value))


def test_wave_progress_stays_separate_from_the_whole_head_and_private_details(tmp_path):
    receipt(tmp_path, {'dataset': 'local-tw-minute', 'state': 'running',
        'manifest_sha256': 'a'*64, 'current_head_bytes': 500, 'already_placed_bytes': 50,
        'progress': {'stage': 'copying_old_D_archive_to_E', 'completed_bytes': 40,
                     'total_bytes': 100, 'completed_objects': 2, 'total_objects': 5,
                     'wave_identity': 'b'*64}, 'reason': '/private/source/no-public-path'})
    row, = progress.packed_archive_placement_tasks(tmp_path)
    assert row['total_bytes'] == 100 and not row['complete_evidence']
    public = progress.public_status({'contract': progress.CONTRACT,
        'observed_at_utc': progress.iso(time.time()), 'tasks': [row]})
    published, = public['tasks']
    assert published['progress']['percent'] == 40
    assert published['metrics']['archive_head_bytes'] == 500
    assert published['metrics']['archive_D_reclaimed_bytes'] == 4096
    assert '/private' not in json.dumps(public)
    assert '不代表全部遷移完成' in published['scope']


def test_only_the_completed_bound_wave_claims_completion(tmp_path):
    entry = {'dataset': 'local-tw-minute', 'state': 'wave_completed',
             'progress': {'completed_bytes': 100, 'total_bytes': 100,
                          'completed_objects': 5, 'total_objects': 5}}
    receipt(tmp_path, entry)
    row, = progress.packed_archive_placement_tasks(tmp_path)
    assert row['complete_evidence'] and row['state'] == 'completed'
    entry['progress']['completed_bytes'] = 99
    receipt(tmp_path, entry)
    row, = progress.packed_archive_placement_tasks(tmp_path)
    assert not row['complete_evidence']


def test_reader_rollout_and_capacity_wait_are_visible_without_invented_eta(tmp_path):
    (tmp_path/'status.json').write_text(json.dumps({
        'contract': 'stockagent.packed_object_placement.v1.queue',
        'state': 'waiting_reader_rollout', 'observed_at_epoch': time.time()}))
    row, = progress.packed_archive_placement_tasks(tmp_path)
    assert row['reason'] == 'archive_reader_rollout' and row['completed_bytes'] is None
    receipt(tmp_path, {'dataset': 'local-tw-minute', 'state': 'blocked',
                       'error_type': 'CapacityDeferred', 'bytes_in_wave': 100})
    row, = progress.packed_archive_placement_tasks(tmp_path)
    assert row['reason'] == 'archive_tier_capacity' and not row['complete_evidence']


def test_retired_E_copy_does_not_erase_cumulative_D_reclamation(tmp_path, monkeypatch):
    from stockagent.data_sync.packed_object_placement import Locations
    (tmp_path/'status.json').write_text(json.dumps({
        'contract':'stockagent.packed_object_placement.v1.queue',
        'observed_at_epoch':time.time(),'entries':[]}))
    states = [{'dataset':'local-tw-minute','state':'E_authoritative',
               'files':2,'bytes':100,'retired_D_allocated_bytes':8192},
              {'dataset':'local-tw-minute','state':'E_retired',
               'files':1,'bytes':50,'retired_D_allocated_bytes':4096}]
    monkeypatch.setattr(Locations,'totals',lambda _self:{'datasets':states})
    row,=progress.packed_archive_placement_tasks(tmp_path)
    assert row['completed_bytes']==100 and row['completed_items']==2
    assert row['metrics']['archive_D_reclaimed_bytes']==12288
    states[0]['state']='E_retired'
    row,=progress.packed_archive_placement_tasks(tmp_path)
    assert row['completed_bytes']==0 and row['completed_items']==0
    assert row['metrics']['archive_D_reclaimed_bytes']==12288


def test_daily_archive_slot_reports_its_next_start_without_a_completion_estimate(tmp_path):
    from datetime import datetime
    repo=tmp_path/'repo'
    (repo/'configs').mkdir(parents=True)
    (repo/'configs/columnar_storage.json').write_text(json.dumps({'dataset_version_lifecycle':{
        'daily_merge_window':{'timezone':'Asia/Taipei','start_hour':3,'end_hour':7},
        'ordinary_history_retention':'current-and-research-pins'}}))
    stamp=datetime.fromisoformat('2026-11-10T21:00:00+08:00').timestamp()
    row,=progress.ordinary_archive_tasks(repo,tmp_path/'state',now=stamp)
    assert row['state']=='waiting' and not row['complete_evidence']
    assert progress.timestamp(row['next_attempt_at_utc'])==datetime.fromisoformat('2026-11-11T03:00:00+08:00').timestamp()
    assert row['total_items'] is None


def test_daily_archive_public_receipt_has_reclaimed_space_and_no_private_error(tmp_path):
    from datetime import datetime
    repo=tmp_path/'repo'
    (repo/'configs').mkdir(parents=True)
    (repo/'configs/columnar_storage.json').write_text(json.dumps({'dataset_version_lifecycle':{
        'daily_merge_window':{'timezone':'Asia/Taipei','start_hour':3,'end_hour':7},
        'ordinary_history_retention':'current-and-research-pins','ordinary_history_migration_catchup':True}}))
    state=tmp_path/'state';state.mkdir()
    stamp=datetime.fromisoformat('2026-11-10T21:00:00+08:00').timestamp()
    (state/'status.json').write_text(json.dumps({'contract':'stockagent.ordinary_archive_cycle.v1',
        'day':'migration-catchup','state':'waiting','observed_at_epoch':stamp,
        'owners':[{'dataset':'one'}],'completed':{'one':{'coverage_protected_versions':2}},
        'reason':'/private/raw-secret.csv','totals':{'versions_removed':4,
        'estimated_allocated_bytes_reclaimed':8192}}))
    rows=progress.ordinary_archive_tasks(repo,state,now=stamp)
    public=progress.public_status({'contract':progress.CONTRACT,'observed_at_utc':progress.iso(stamp),'tasks':rows},now=stamp)
    row,=public['tasks']
    assert row['metrics']['ordinary_history_versions_removed']==4
    assert row['metrics']['ordinary_history_coverage_protected']==2
    assert 'raw-secret' not in json.dumps(public)
    assert row['reason']==progress.REASONS['ordinary_archive_protected'] and not row['complete_evidence']
