import json
from pathlib import Path

import pytest

from stockagent.data_sync import packed_snapshots as p, private_repack_history as history
from stockagent.data_sync import ordinary_archive
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def versions(tmp_path,monkeypatch):
    source,cold,stage,hot,retention=(tmp_path/name for name in ('data_fixture','cold','control','hot','retention'))
    source.mkdir();stage.mkdir();hot.mkdir();retention.mkdir()
    monkeypatch.setattr(p,'_notify_packed_publication',lambda *_a,**_k:None)
    result=[]
    for value in ('first','second','third'):
        (source/'value.txt').write_text(value)
        result.append(p.publish_packed_snapshot(cold,'fixture',source,node_id='test'))
    return cold,stage,hot,retention,result


def retire(versions,*,apply,process_check=lambda _paths:[]):
    cold,stage,hot,retention,releases=versions
    current,receipt=ordinary_archive.validate_current_inventory(cold,'fixture',stage)
    return history.retire(cold,stage,receipt,public_index=None,materialized=hot,
        retention_state=retention,current_head_only=True,apply=apply,process_check=process_check)


def test_unused_versions_are_removed_but_a_research_pin_and_current_bytes_survive(versions,tmp_path):
    cold,stage,hot,retention,releases=versions
    first,second,current=releases
    (hot/'research').mkdir()
    p.write_packed_pin(hot/'research/input.pin.json',first)
    planned=retire(versions,apply=False)
    assert planned['contract']==history.CURRENT_CONTRACT
    assert first.manifest_path.exists() and second.manifest_path.exists()
    done=retire(versions,apply=True)
    assert done['versions_removed']==1
    assert first.manifest_path.exists() and not second.manifest_path.exists()
    assert p.resolve_latest_packed(cold,'fixture').manifest_sha256==current.manifest_sha256
    recovered=p.fetch_packed_snapshot(cold,tmp_path/'research-recover',first)
    assert (recovered/'value.txt').read_text()=='first'
    latest=p.fetch_packed_snapshot(cold,tmp_path/'current-recover',current)
    assert (latest/'value.txt').read_text()=='third'


def test_current_inventory_is_validated_once_without_raw_restore(versions,monkeypatch):
    cold,stage,*_=versions
    for name in ('fetch_packed_snapshot','verify_packed_snapshot'):
        monkeypatch.setattr(p,name,lambda *_a,**_k:pytest.fail('unexpected raw reconstruction'))
    ordinary_archive.validate_current_inventory(cold,'fixture',stage)
    monkeypatch.setattr(p,'_open_inventory',lambda *_a,**_k:pytest.fail('unchanged validation repeated'))
    ordinary_archive.validate_current_inventory(cold,'fixture',stage)


def test_coverage_streams_old_inventory_and_indexes_only_paths_and_types(versions,monkeypatch):
    cold,stage,hot,retention,releases=versions
    _,receipt=ordinary_archive.validate_current_inventory(cold,'fixture',stage)
    with history._path_inventory(cold,releases[-1].manifest,guard=lambda:None) as rows:
        assert set(rows.lookup('value.txt'))=={'path','kind'}
    monkeypatch.setattr(p,'_open_inventory',lambda *_a,**_kw:pytest.fail('coverage built a full-row metadata index'))
    result=history.retire(cold,stage,receipt,public_index=None,materialized=hot,
        retention_state=retention,current_head_only=True,apply=True,process_check=lambda _paths:[])
    assert result['versions_removed']==2 and releases[-1].manifest_path.exists()


def test_active_or_unknown_protection_never_authorizes_deletion(versions):
    cold,stage,hot,retention,releases=versions
    with pytest.raises(SnapshotError,match='running process'):
        retire(versions,apply=True,process_check=lambda _paths:['live reader'])
    assert all(x.manifest_path.exists() for x in releases)
    (hot/'research').mkdir()
    (hot/'research/bad.pin.json').write_text('{"not_a_valid_source_pin":true}')
    with pytest.raises(SnapshotError):
        retire(versions,apply=True)
    assert all(x.manifest_path.exists() for x in releases)


def test_new_head_keeps_the_validated_anchor_and_its_current_successor(versions):
    cold,stage,hot,retention,releases=versions
    _,receipt=ordinary_archive.validate_current_inventory(cold,'fixture',stage)
    source=stage.parent/'data_fixture'
    (source/'value.txt').write_text('fourth')
    successor=p.publish_packed_snapshot(cold,'fixture',source,node_id='test')
    done=history.retire(cold,stage,receipt,public_index=None,materialized=hot,
        retention_state=retention,current_head_only=True,apply=True,process_check=lambda _p:[])
    assert done['versions_removed']==2
    assert done['retention_fence']=='fixed-validated-anchor-and-all-current-heads-v2'
    assert releases[-1].manifest_path.exists() and successor.manifest_path.exists()
    assert p.resolve_latest_packed(cold,'fixture').manifest_sha256==successor.manifest_sha256


def test_rewritten_immutable_anchor_is_still_rejected(versions):
    cold,stage,hot,retention,releases=versions
    _,receipt=ordinary_archive.validate_current_inventory(cold,'fixture',stage)
    value=json.loads(releases[-1].manifest_path.read_text())
    value['metadata']['unexpected-change']='tamper'
    releases[-1].manifest_path.write_text(json.dumps(value))
    with pytest.raises(SnapshotError):
        history.retire(cold,stage,receipt,public_index=None,materialized=hot,
            retention_state=retention,current_head_only=True,apply=True,process_check=lambda _p:[])
    assert all(x.manifest_path.exists() for x in releases)


def test_concurrent_target_publication_preserves_both_anchor_and_new_head(versions,monkeypatch):
    cold,stage,_,_,releases=versions
    source=stage.parent/'data_fixture'
    original=history.r._read_manifest
    published=[]
    def advance_head(root,relative):
        value=original(root,relative)
        if not published:
            published.append(None)
            (source/'value.txt').write_text('published during the census')
            published[0]=p.publish_packed_snapshot(cold,'fixture',source,node_id='test')
        return value
    monkeypatch.setattr(history.r,'_read_manifest',advance_head)
    result=retire(versions,apply=True)
    assert result['versions_removed']==2 and result['appended_manifests_reconciled']==1
    assert releases[-1].manifest_path.exists()
    assert p.resolve_latest_packed(cold,'fixture').manifest_sha256==published[0].manifest_sha256


def test_incomplete_latest_head_cannot_remove_an_older_unique_history_file(versions):
    cold,stage,_,_,releases=versions
    source=stage.parent/'data_fixture'
    (source/'value.txt').unlink()
    (source/'other.txt').write_text('new value')
    p.publish_packed_snapshot(cold,'fixture',source,node_id='test')
    result=retire(versions,apply=True)
    assert result['state']=='retained_unique_history'
    assert result['coverage_protected_versions']==3
    assert all(x.manifest_path.exists() for x in releases)


@pytest.fixture
def cycle(versions,tmp_path,monkeypatch):
    from datetime import datetime
    cold,_,hot,retention,_=versions
    repo=tmp_path/'repo'
    (repo/'configs/data_sync').mkdir(parents=True)
    policy={'daily_merge_window':{'timezone':'Asia/Taipei','start_hour':3,'end_hour':7},
            'ordinary_history_retention':'current-and-research-pins'}
    (repo/'configs/columnar_storage.json').write_text(json.dumps({'dataset_version_lifecycle':policy}))
    (repo/'configs/data_sync/packed_retention.json').write_text(json.dumps({
        'schema_version':1,'sync_root':str(cold),'archive_root':str(tmp_path/'archive'),
        'materialized_root':str(hot),'state_dir':str(retention),'backup_config':'unused.json',
        'folder_id':'test','required_peer_names':[]}))
    monkeypatch.setattr(ordinary_archive.workspace_volume,'check',lambda:None)
    monkeypatch.setattr(ordinary_archive,'_owner_locks',lambda *_:[])
    monkeypatch.setattr(ordinary_archive,'source_owners',lambda _:(
        [{'dataset':'fixture','authority':'public','source_root':'data_fixture'}],[]))
    now=datetime.fromisoformat('2026-11-11T03:10:00+08:00').timestamp()
    return repo,tmp_path/'cycle',now


def test_cycle_uses_original_research_pin_authority_and_skips_completed_head(cycle,versions,monkeypatch):
    repo,state,now=cycle
    cold,_,hot,_,releases=versions
    (hot/'active-study').mkdir()
    p.write_packed_pin(hot/'active-study/source.pin.json',releases[0])
    assert ordinary_archive.step(repo,state=state,now=now) is True
    assert releases[0].manifest_path.exists() and not releases[1].manifest_path.exists()
    assert ordinary_archive.step(repo,state=state,now=now+1) is False
    # Once no unprotected history remains, no full census is repeated that day.
    monkeypatch.setattr(ordinary_archive,'validate_current_inventory',lambda *_:pytest.fail('completed head repeated'))
    assert ordinary_archive.step(repo,state=state,now=now+2) is None
    status=json.loads((state/'status.json').read_text())
    assert status['totals']['versions_removed']==1


def test_cycle_defers_until_daily_slot_and_allows_explicit_migration_catchup(cycle,versions):
    repo,state,morning=cycle
    evening=morning+17*3600
    assert ordinary_archive.step(repo,state=state,now=evening) is None
    assert not state.exists()
    path=repo/'configs/columnar_storage.json'
    value=json.loads(path.read_text())
    value['dataset_version_lifecycle']['ordinary_history_migration_catchup']=True
    path.write_text(json.dumps(value))
    assert ordinary_archive.step(repo,state=state,now=evening) is True
    assert versions[-1][-1].manifest_path.exists()


def test_cycle_keeps_trading_guard_and_failure_cannot_authorize_unlink(cycle,versions):
    repo,state,now=cycle
    def protected():
        raise SnapshotError('protected market session')
    assert ordinary_archive.step(repo,state=state,now=now,guard=protected) is None
    assert all(row.manifest_path.exists() for row in versions[-1])
    status=json.loads((state/'status.json').read_text())
    assert status['state']=='waiting' and status['reason']=='protected market session'


def test_unavailable_dataset_does_not_starve_another_due_dataset(cycle,versions,monkeypatch):
    repo,state,now=cycle
    monkeypatch.setattr(ordinary_archive,'source_owners',lambda _:([
        {'dataset':'unavailable','authority':'public','source_root':'data_unavailable'},
        {'dataset':'fixture','authority':'public','source_root':'data_fixture'}],[]))
    assert ordinary_archive.step(repo,state=state,now=now) is None
    assert all(row.manifest_path.exists() for row in versions[-1])
    def check_new_subject():
        active=json.loads((state/'status.json').read_text())
        if active['state']=='running':
            assert 'reason' not in active
    assert ordinary_archive.step(repo,state=state,now=now+1,guard=check_new_subject) is True
    status=json.loads((state/'status.json').read_text())
    assert 'public/unavailable' in status['issues'] and 'reason' not in status


def test_busy_retirement_admission_does_not_decode_a_large_current_inventory(cycle,monkeypatch):
    from contextlib import contextmanager
    from stockagent.data_sync import repack_retirement
    repo,state,now=cycle
    @contextmanager
    def busy(*_a,**_kw):
        raise SnapshotError('retirement owner busy: publication')
        yield
    monkeypatch.setattr(repack_retirement,'_boundary',busy)
    monkeypatch.setattr(ordinary_archive,'validate_current_inventory',
        lambda *_:pytest.fail('inventory decoded while its retirement owner was busy'))
    assert ordinary_archive.step(repo,state=state,now=now) is None
    assert json.loads((state/'status.json').read_text())['reason']=='retirement owner busy: publication'


def test_long_inventory_guard_refreshes_owner_heartbeat_without_completing_work(cycle,versions,monkeypatch):
    from types import SimpleNamespace
    repo,state,now=cycle
    clock={'mono':0.0,'epoch':now}
    monkeypatch.setattr(ordinary_archive,'time',SimpleNamespace(
        monotonic=lambda:clock['mono'],time=lambda:clock['epoch']))
    checks=[]
    def planning(_cold,_stage,_receipt,**options):
        before=json.loads((state/'status.json').read_text())
        clock.update(mono=16.0,epoch=now+16)
        options['guard']()
        after=json.loads((state/'status.json').read_text())
        assert after['observed_at_epoch']==now+16>before['observed_at_epoch']
        assert after['state']=='running' and after['completed']=={}
        assert not after.get('totals')
        return {'state':'no_unpinned_history','dataset':'fixture',
                'versions_removed':0,'object_bytes_removed':0}
    monkeypatch.setattr(ordinary_archive.private_repack_history,'retire',planning)
    assert ordinary_archive.step(repo,state=state,now=now,guard=lambda:checks.append(True)) is False
    assert checks and all(release.manifest_path.exists() for release in versions[-1])
