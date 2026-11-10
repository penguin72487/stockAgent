import json
from pathlib import Path

import pytest

from stockagent.data_sync import d_pack_layout_inventory as inventory
from stockagent.data_sync import packed_snapshots as packed, stream_repack


def test_public_layout_has_only_aggregate_counts_and_preserves_the_observed_clock(tmp_path):
    import time
    measured=inventory.object_presence(tmp_path,[])
    measured.update(dataset_count=3,incomplete_datasets=1,private_path='/private/secret')
    public=inventory.public_status(measured,now=time.time())
    assert public['incomplete_datasets']==1 and not public['stale']
    assert public['observed_at_utc']==measured['observed_at_utc']
    assert 'private' not in json.dumps(public) and not public['retirement_authorized']
    measured['groups']['old_only']['present_names']=-1
    assert inventory.public_status(measured,now=time.time()) is None


def test_original_observer_caches_layout_without_repeating_the_disk_census(tmp_path,monkeypatch):
    import time
    from scripts.observe_d_backup_inventory import observe_layout
    from stockagent.data_sync import workspace_volume
    measured=inventory.object_presence(tmp_path,[])
    monkeypatch.setattr(workspace_volume,'check',lambda:None)
    calls=[]
    def collect(*_a,**_kw):
        calls.append('metadata');return {'objects':[],'dataset_count':2,'errors':[{}],
            'observed_at_utc':measured['observed_at_utc']}
    monkeypatch.setattr(inventory,'collect',collect)
    monkeypatch.setattr(inventory,'object_presence',lambda *_a,**_kw:{**measured,'objects':[]})
    output=tmp_path/'layout.json'
    assert observe_layout(output,now=time.time())=='completed'
    assert observe_layout(output,now=time.time()+60)=='current'
    assert calls==['metadata']
    assert json.loads(output.read_text())['incomplete_datasets']==1
    output.write_text('{damaged cache')
    assert observe_layout(output,now=time.time()+120)=='completed'
    assert calls==['metadata','metadata']


def test_protected_layout_census_keeps_its_last_fixed_snapshot(tmp_path,monkeypatch):
    from scripts.observe_d_backup_inventory import observe_layout
    measured=inventory.object_presence(tmp_path,[])
    output=tmp_path/'layout.json';output.write_text(json.dumps(measured))
    before=output.read_bytes()
    def protected():
        raise RuntimeError('trading priority')
    with pytest.raises(RuntimeError,match='trading priority'):
        observe_layout(output,force=True,guard=protected)
    assert output.read_bytes()==before


def test_live_inventory_separates_old_and_new_without_payload_reads(tmp_path, monkeypatch):
    source, cold, workspace = (tmp_path/name for name in ('source', 'cold', 'workspace'))
    source.mkdir(); workspace.mkdir()
    (source/'value.txt').write_bytes(b'original values\n'*20)
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='penguin')
    stream_repack.repack(cold, workspace/'build', 'fixture', old.manifest_sha256)
    new = packed.resolve_latest_packed(cold, 'fixture')
    private = cold/'.local-state/private-history'
    other = packed.publish_packed_snapshot(private, 'private-fixture', source, node_id='penguin')
    def no_payload(*_a, **_kw):
        pytest.fail('layout classification must not read an inventory or payload')
    monkeypatch.setattr(packed, '_iter_inventory', no_payload)
    monkeypatch.setattr(packed, '_open_inventory', no_payload)
    result = inventory.collect(cold, workspace, workers=2,
        layout_check=lambda _r, _n, m: m['archive'].get('new_pack_layout') == 'source-dataset-v1')
    assert result['complete'] and result['manifest_count'] == 3
    assert not result['payloads_read'] and not result['object_presence_checked']
    assert not result['retirement_authorized'] and not result['allocated_bytes_checked']
    assert result['groups']['new_only']['reference_bytes'] == sum(x['bytes'] for x in new.manifest['archive']['objects'])
    assert result['groups']['old_only']['reference_bytes'] == sum(x['bytes'] for x in old.manifest['archive']['objects']) + sum(x['bytes'] for x in other.manifest['archive']['objects'])
    rows = {(x['authority'], x['snapshot_id']): x for x in result['manifests']}
    assert rows['public', old.manifest['snapshot_id']]['current'] is False
    assert rows['public', new.manifest['snapshot_id']]['current'] is True
    assert rows['private', other.manifest['snapshot_id']]['layout'] == 'old'
    assert sum(x['reference_bytes'] for x in result['groups'].values()) == result['referenced_name_bytes']
    assert old.manifest_path.is_file() and other.manifest_path.is_file()


def test_busy_dataset_yields_an_explicit_partial_inventory(tmp_path, monkeypatch):
    source, cold, workspace = (tmp_path/name for name in ('source', 'cold', 'workspace'))
    source.mkdir(); workspace.mkdir(); (source/'value').write_text('data')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='penguin')
    with packed._try_publication_lock(cold/'.local-state/locks/publish-fixture.lock'):
        result = inventory.collect(cold, workspace, layout_check=lambda *_a: False)
    assert not result['complete'] and result['manifest_count'] == 0
    assert result['errors'][0]['error_type'] == 'PackedPublicationBusy'
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_a_changed_head_manifest_is_not_counted_as_a_valid_dataset(tmp_path, monkeypatch):
    source, cold, workspace = (tmp_path/name for name in ('source', 'cold', 'workspace'))
    source.mkdir(); workspace.mkdir(); (source/'value').write_text('data')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='penguin')
    value = json.loads(old.manifest_path.read_text())
    value['metadata'] = {**value.get('metadata', {}), 'unexpected': True}
    old.manifest_path.write_bytes(packed._canonical_json_bytes(value))
    result = inventory.collect(cold, workspace, layout_check=lambda *_a: True)
    assert not result['complete'] and result['referenced_name_bytes'] == 0
    assert result['errors'][0]['reason'] == 'layout inventory head SHA differs'


def test_presence_distinguishes_retired_names_and_counts_hardlinks_once(tmp_path):
    import os
    cold = tmp_path/'cold'
    cold.mkdir()
    old, new = cold/'old', cold/'new'
    old.write_bytes(b'data')
    os.link(old, new)
    (cold/'changed').write_bytes(b'changed length')
    rows = [{'authority': 'public', 'relative': name, 'sha256': 'a'*64,
             'bytes': 4, 'layout_group': kind} for name, kind in
            [('old', 'old_only'), ('new', 'new_only'), ('missing', 'old_only'), ('changed', 'old_only')]]
    result = inventory.object_presence(cold, rows)
    assert result['groups']['old_only'] == {'present_names': 1, 'present_name_bytes': 4,
        'missing_names': 1, 'missing_reference_bytes': 4, 'other_names': 1,
        'placed_on_E_names': 0, 'placed_on_E_name_bytes': 0}
    assert result['present_unique_inodes'] == 1
    assert result['present_allocated_bytes_hardlinks_once'] == old.stat().st_blocks*512
    assert result['shared_group_allocated_bytes'] == old.stat().st_blocks*512
    assert not result['payloads_read'] and not result['payload_SHA_verified']
    assert not result['retirement_authorized']


def test_presence_refuses_redirected_parents_and_ambiguous_inputs(tmp_path):
    cold = tmp_path/'cold'
    cold.mkdir()
    external = tmp_path/'external'
    external.mkdir()
    (external/'value').write_bytes(b'data')
    (cold/'redirected').symlink_to(external, target_is_directory=True)
    row = {'authority': 'public', 'relative': 'redirected/value', 'sha256': 'a'*64,
           'bytes': 4, 'layout_group': 'new_only'}
    assert inventory.object_presence(cold, [row])['objects'][0]['state'] == 'redirected'
    with pytest.raises(Exception, match='duplicate'):
        inventory.object_presence(cold, [row, row])
    with pytest.raises(Exception, match='relative|unsafe|path'):
        inventory.object_presence(cold, [{**row, 'relative': '../external/value'}])


def test_enrolled_E_object_is_neither_D_usage_nor_missing_data(tmp_path, monkeypatch):
    cold, workspace = tmp_path/'cold', tmp_path/'E'
    cold.mkdir(); workspace.mkdir()
    (cold/'objects').mkdir()
    archived = workspace/'preserved'
    archived.write_bytes(b'data')
    monkeypatch.setattr(packed, '_read_object_path', lambda *_a, **_kw: archived)
    row = {'authority': 'public', 'relative': 'objects/moved', 'sha256': 'a'*64,
           'bytes': 4, 'layout_group': 'new_only'}
    result = inventory.object_presence(cold, [row])
    assert result['objects'][0]['state'] == 'placed_on_E'
    assert result['groups']['new_only']['missing_names'] == 0
    assert result['groups']['new_only']['present_names'] == 0
    assert result['groups']['new_only']['placed_on_E_name_bytes'] == 4
    assert result['present_allocated_bytes_hardlinks_once'] == 0
    assert result['placed_E_allocated_bytes_hardlinks_once'] == archived.stat().st_blocks*512
