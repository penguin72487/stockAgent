import json
import shutil
from itertools import count
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.data_sync import handoff_index_archive, index_archive_existing as existing
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import materialized_cache
from stockagent.data_sync.desync_snapshots import SnapshotError
from test_handoff_source_repack import fixture


def setup_reuse(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    prepared = handoff_index_archive.prepare(source, stage, 'fixture', 'a'*64, index,
        **kwargs, preserved_paths=['.download.lock'], supplemental_root_lock=True)
    private = tmp_path/'private'
    head = packed.publish_packed_snapshot(private, 'fixture', source, node_id='test',
                                          excluded_subtrees=['.download.lock'])
    monkeypatch.setattr(existing, 'REVIEWED', {'fixture': (source.name, ('.download.lock',))})
    return source, private, stage, index, prepared, head


def test_existing_head_reuses_equal_inventory_and_cleans_only_sealed_E_members(tmp_path, monkeypatch):
    source, private, stage, index, prepared, head = setup_reuse(tmp_path, monkeypatch)
    monkeypatch.setattr(packed, 'fetch_packed_snapshot', lambda *_a, **_kw: pytest.fail('no D payload readback'))
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    assert proof['manifest_sha256'] == head.manifest_sha256 and proof['files'] == 2
    assert proof['source_payload_read'] is False and proof['independent_D_readback'] is False
    result = existing.cleanup(private, stage, 'fixture', 'a'*64, source.name, proof)
    assert result['state'] == 'completed' and not list(stage.glob('pack-*.zip'))
    assert packed.resolve_latest_packed(private, 'fixture').manifest_sha256 == head.manifest_sha256
    assert (stage/'existing-D-match.json').exists() and (stage/'prepared.json').exists()


def test_existing_head_rejects_changed_file_without_retiring_E(tmp_path, monkeypatch):
    source, private, stage, index, _prepared, _head = setup_reuse(tmp_path, monkeypatch)
    (source/'nested/a.parquet').write_bytes(b'different generation')
    packed.publish_packed_snapshot(private, 'fixture', source, node_id='test',
                                   excluded_subtrees=['.download.lock'])
    with pytest.raises(SnapshotError, match='does not exactly cover'):
        existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    assert list(stage.glob('pack-*.zip')) and not (stage/'existing-D-match.json').exists()


def test_existing_cleanup_resumes_after_interrupted_unlink(tmp_path, monkeypatch):
    source, private, stage, index, _prepared, _head = setup_reuse(tmp_path, monkeypatch)
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    original, hit = Path.unlink, []
    def interrupted(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        if path.parent == stage and path.name.startswith('pack-') and not hit:
            hit.append(path.name)
            raise OSError('after unlink')
        return result
    monkeypatch.setattr(Path, 'unlink', interrupted)
    with pytest.raises(OSError, match='after unlink'):
        existing.cleanup(private, stage, 'fixture', 'a'*64, source.name, proof)
    monkeypatch.setattr(Path, 'unlink', original)
    result = existing.cleanup(private, stage, 'fixture', 'a'*64, source.name, proof)
    assert result['state'] == 'completed'
    assert json.loads((stage/'existing-D-cleanup.json').read_text())['state'] == 'completed'


def test_existing_cleanup_preserves_E_if_D_head_changes(tmp_path, monkeypatch):
    source, private, stage, index, _prepared, _head = setup_reuse(tmp_path, monkeypatch)
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    (source/'nested/a.parquet').write_bytes(b'new current generation')
    packed.publish_packed_snapshot(private, 'fixture', source, node_id='test',
                                   excluded_subtrees=['.download.lock'])
    with pytest.raises(SnapshotError, match='head changed'):
        existing.cleanup(private, stage, 'fixture', 'a'*64, source.name, proof)
    assert list(stage.glob('pack-*.zip'))


def test_existing_reuse_rejects_missing_D_object_and_tampered_proof(tmp_path, monkeypatch):
    source, private, stage, index, _prepared, head = setup_reuse(tmp_path, monkeypatch)
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    bad = {**proof, 'logical_bytes': proof['logical_bytes'] + 1}
    with pytest.raises(SnapshotError, match='proof differs'):
        existing.cleanup(private, stage, 'fixture', 'a'*64, source.name, bad)
    obj = private / head.manifest['archive']['objects'][0]['relpath']
    obj.unlink()
    with pytest.raises(SnapshotError):
        existing.cleanup(private, stage, 'fixture', 'a'*64, source.name, proof)
    assert list(stage.glob('pack-*.zip'))


def setup_subtree_reuse(tmp_path, monkeypatch):
    source, private, stage, index, prepared, _head = setup_reuse(tmp_path, monkeypatch)
    parent = tmp_path/'parent-source'
    parent.mkdir()
    shutil.copytree(source, parent/'bodies')
    (parent/'other.txt').write_text('other parent source')
    head = packed.publish_packed_snapshot(private, 'parent', parent, node_id='test')
    pins = tmp_path/'pins'
    pins.mkdir()
    monkeypatch.setattr(existing, 'SUBTREE_COVERAGES', {'fixture': ('parent', parent.name, 'bodies/')})
    monkeypatch.setattr(existing, '_materialized_root', lambda: pins)
    return source, private, stage, index, prepared, head, pins


def test_existing_parent_subtree_reuse_pins_its_exact_snapshot_and_cleans_E(tmp_path, monkeypatch):
    source, private, stage, index, prepared, head, pins = setup_subtree_reuse(tmp_path, monkeypatch)
    monkeypatch.setattr(packed, 'fetch_packed_snapshot', lambda *_a, **_kw: pytest.fail('no D payload readback'))
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    assert proof['covering_dataset'] == 'parent' and proof['manifest_sha256'] == head.manifest_sha256
    assert proof['files'] == 2 and proof['covering_snapshot_pinned'] is True
    assert head.manifest['snapshot_id'] in materialized_cache._pinned_snapshot_ids(pins)
    result = existing.cleanup(private, stage, 'fixture', 'a'*64, source.name, proof)
    assert result['state'] == 'completed' and not list(stage.glob('pack-*.zip'))
    assert head.manifest['snapshot_id'] in materialized_cache._pinned_snapshot_ids(pins)


def test_parent_coverage_preserves_internal_symlink_identity(tmp_path, monkeypatch):
    from test_handoff_index_archive import _symlink_fixture
    source, _old_cold, stage, index, _old, kwargs = _symlink_fixture(tmp_path, monkeypatch)
    prepared = handoff_index_archive.prepare(source, stage, 'fixture', 'a'*64, index,
        **kwargs, preserved_paths=['.download.lock'], supplemental_root_lock=True,
        preserve_internal_symlinks=True)
    private, parent, pins = tmp_path/'private', tmp_path/'parent-source', tmp_path/'pins'
    parent.mkdir(); pins.mkdir()
    shutil.copytree(source, parent/'bodies', symlinks=True)
    head = packed.publish_packed_snapshot(private, 'parent', parent, node_id='test')
    monkeypatch.setattr(existing, 'REVIEWED', {'fixture': (source.name, ('.download.lock',))})
    monkeypatch.setattr(existing, 'SUBTREE_COVERAGES', {'fixture': ('parent', parent.name, 'bodies/')})
    monkeypatch.setattr(existing, '_materialized_root', lambda: pins)
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    assert proof['prepared_sha256'] == prepared['identity_sha256'] and proof['manifest_sha256'] == head.manifest_sha256
    link = parent/'bodies/nested/internal-link'
    link.unlink(); link.symlink_to('b.bin')
    packed.publish_packed_snapshot(private, 'parent', parent, node_id='test')
    with pytest.raises(SnapshotError, match='does not exactly cover'):
        existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    assert list(stage.glob('pack-*.zip'))


def test_two_subtrees_pin_the_same_parent_without_overwriting_each_other(tmp_path, monkeypatch):
    left, right = tmp_path/'left-fixture', tmp_path/'right-fixture'
    left.mkdir(); right.mkdir()
    a, _acold, astage, aindex, _aold, akw = fixture(left, monkeypatch)
    b, _bcold, bstage, bindex, _bold, bkw = fixture(right, monkeypatch)
    handoff_index_archive.prepare(a, astage, 'left', 'a'*64, aindex, **akw,
        preserved_paths=['.download.lock'], supplemental_root_lock=True)
    handoff_index_archive.prepare(b, bstage, 'right', 'b'*64, bindex, **bkw,
        preserved_paths=['.download.lock'], supplemental_root_lock=True)
    parent, private, pins = tmp_path/'parent', tmp_path/'private', tmp_path/'pins'
    parent.mkdir(); pins.mkdir()
    shutil.copytree(a, parent/'left'); shutil.copytree(b, parent/'right')
    head = packed.publish_packed_snapshot(private, 'parent', parent, node_id='test')
    monkeypatch.setattr(existing, 'REVIEWED', {'left':(a.name, ('.download.lock',)),
                                              'right':(b.name, ('.download.lock',))})
    monkeypatch.setattr(existing, 'SUBTREE_COVERAGES', {'left':('parent', parent.name, 'left/'),
                                                       'right':('parent', parent.name, 'right/')})
    monkeypatch.setattr(existing, '_materialized_root', lambda:pins)
    first = existing.match(private, astage, 'left', 'a'*64, aindex, a.name)
    second = existing.match(private, bstage, 'right', 'b'*64, bindex, b.name)
    left_pin, right_pin = existing._coverage_pin(first, astage), existing._coverage_pin(second, bstage)
    assert left_pin != right_pin and left_pin.is_file() and right_pin.is_file()
    assert first['snapshot_id'] == second['snapshot_id'] == head.manifest['snapshot_id']
    right_pin.unlink()
    assert existing._coverage_pin(first, astage) == left_pin
    assert head.manifest['snapshot_id'] in materialized_cache._pinned_snapshot_ids(pins)
    existing.restore_covering_pin(private, bstage, 'right', 'b'*64, b.name, second)
    assert right_pin.is_file() and left_pin.is_file()


def test_legacy_single_stage_coverage_pin_remains_valid(tmp_path, monkeypatch):
    source, private, stage, index, prepared, head, pins = setup_subtree_reuse(tmp_path, monkeypatch)
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    existing._coverage_pin(proof, stage).unlink()
    existing.s._repack_pin(pins, 'parent', head.manifest_sha256, head.manifest['snapshot_id'], stage)
    proof.pop('pin_format')
    proof['identity_sha256'] = existing.s._identity(proof)
    (stage/'existing-D-match.json').write_text(json.dumps(proof))
    receipt = {'state':'existing_D_head_matches_source_inventory', 'dataset':'fixture',
        'old_manifest_sha256':'a'*64, 'new_snapshot_id':head.manifest['snapshot_id'],
        'new_manifest_sha256':head.manifest_sha256, 'files':proof['files'],
        'logical_bytes':proof['logical_bytes'], 'existing_match':proof}
    assert existing.receipt_matches(receipt, prepared['identity_sha256'])
    assert existing.cleanup(private, stage, 'fixture', 'a'*64, source.name, proof)['state'] == 'completed'


def test_existing_parent_subtree_preserves_E_when_pin_disappears(tmp_path, monkeypatch):
    source, private, stage, index, _prepared, head, pins = setup_subtree_reuse(tmp_path, monkeypatch)
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    pin = existing._coverage_pin(proof, stage)
    pin.unlink()
    with pytest.raises(SnapshotError, match='pin changed'):
        existing.cleanup(private, stage, 'fixture', 'a'*64, source.name, proof)
    assert list(stage.glob('pack-*.zip'))


def test_cached_covering_match_reuses_only_the_same_head_pin_and_source(tmp_path, monkeypatch):
    source, private, stage, index, _prepared, _head, _pins = setup_subtree_reuse(tmp_path, monkeypatch)
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    with monkeypatch.context() as patch:
        patch.setattr(packed, '_iter_inventory', lambda *_a, **_k: pytest.fail('must not scan the inventory twice'))
        assert existing.cached_covering_match(private, stage, 'fixture', 'a'*64, index, source.name) == proof
    parent = tmp_path/'parent-source'
    (parent/'other.txt').write_text('a later D generation')
    packed.publish_packed_snapshot(private, 'parent', parent, node_id='test')
    assert existing.cached_covering_match(private, stage, 'fixture', 'a'*64, index, source.name) is None
    assert list(stage.glob('pack-*.zip'))


def test_parent_prefix_scan_reports_progress_and_checks_guard(tmp_path, monkeypatch):
    source, private, stage, index, _prepared, _head, _pins = setup_subtree_reuse(tmp_path, monkeypatch)
    original = packed._iter_inventory
    def many_prefix_rows(*args):
        for number in range(8200):
            yield {'path':f'a-{number:05d}', 'kind':'directory'}
        yield from original(*args)
    ticks = count()
    monkeypatch.setattr(packed, '_iter_inventory', many_prefix_rows)
    monkeypatch.setattr(existing, 'time', SimpleNamespace(monotonic=lambda:2*next(ticks)))
    guards, events = [], []
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name,
                           guard=lambda:guards.append(True), progress=events.append)
    assert proof['files'] == 2 and len(guards) > 2
    assert any(event['stage'] == 'scanning_D_parent_inventory' and event['inventory_items_completed'] == 8192
               for event in events)


def test_cached_covering_match_rejects_tampered_proof_or_missing_pin(tmp_path, monkeypatch):
    source, private, stage, index, _prepared, head, pins = setup_subtree_reuse(tmp_path, monkeypatch)
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    proof_path = stage/'existing-D-match.json'
    changed = {**proof, 'logical_bytes':proof['logical_bytes']+1}
    proof_path.write_text(json.dumps(changed))
    with pytest.raises(SnapshotError, match='identity or pin differs'):
        existing.cached_covering_match(private, stage, 'fixture', 'a'*64, index, source.name)
    proof_path.write_text(json.dumps(proof))
    existing._coverage_pin(proof, stage).unlink()
    with pytest.raises(SnapshotError, match='identity or pin differs'):
        existing.cached_covering_match(private, stage, 'fixture', 'a'*64, index, source.name)
    assert list(stage.glob('pack-*.zip'))


def test_existing_parent_pin_loss_is_visible_and_repaired_only_for_same_head(tmp_path, monkeypatch):
    source, private, stage, index, prepared, head, pins = setup_subtree_reuse(tmp_path, monkeypatch)
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    receipt = {'state':'existing_D_head_matches_source_inventory', 'dataset':'fixture',
        'old_manifest_sha256':'a'*64, 'new_snapshot_id':head.manifest['snapshot_id'],
        'new_manifest_sha256':head.manifest_sha256, 'files':proof['files'],
        'logical_bytes':proof['logical_bytes'], 'existing_match':proof}
    assert existing.receipt_matches(receipt, prepared['identity_sha256'])
    pin = existing._coverage_pin(proof, stage)
    pin.unlink()
    assert not existing.receipt_matches(receipt, prepared['identity_sha256'])
    existing.restore_covering_pin(private, stage, 'fixture', 'a'*64, source.name, proof)
    assert existing.receipt_matches(receipt, prepared['identity_sha256'])
    pin.unlink()
    parent = tmp_path/'parent-source'
    (parent/'other.txt').write_text('new generation')
    packed.publish_packed_snapshot(private, 'parent', parent, node_id='test')
    with pytest.raises(SnapshotError, match='covering D head changed'):
        existing.restore_covering_pin(private, stage, 'fixture', 'a'*64, source.name, proof)
    assert not pin.exists() and list(stage.glob('pack-*.zip'))


def test_existing_parent_subtree_rejects_changed_content(tmp_path, monkeypatch):
    source, private, stage, index, _prepared, _head, _pins = setup_subtree_reuse(tmp_path, monkeypatch)
    parent = tmp_path/'parent-source'
    (parent/'bodies/nested/a.parquet').write_bytes(b'changed')
    packed.publish_packed_snapshot(private, 'parent', parent, node_id='test')
    with pytest.raises(SnapshotError, match='does not exactly cover'):
        existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    assert list(stage.glob('pack-*.zip'))


def test_public_covering_head_is_reused_with_both_authorities_locked(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    prepared = handoff_index_archive.prepare(source, stage, 'fixture', 'a'*64, index,
        **kwargs, preserved_paths=['.download.lock'], supplemental_root_lock=True)
    public = tmp_path/'public'
    head = packed.publish_packed_snapshot(public, 'parent', source, node_id='test',
                                           excluded_subtrees=['.download.lock'])
    private = public/'.local-state/private-history'
    packed.initialize_packed_layout(private, node_id='test')
    (public/'.stockagent-d-primary').write_text('test')
    pins = tmp_path/'pins'; pins.mkdir()
    monkeypatch.setattr(existing, 'REVIEWED', {'fixture': (source.name, ('.download.lock',))})
    monkeypatch.setattr(existing, 'SUBTREE_COVERAGES', {'fixture': ('parent', source.name, '')})
    monkeypatch.setattr(existing, 'PUBLIC_COVERAGE_DATASETS', frozenset({'fixture'}))
    monkeypatch.setattr(existing, '_materialized_root', lambda: pins)
    proof = existing.match(private, stage, 'fixture', 'a'*64, index, source.name)
    assert proof['covering_authority'] == 'public' and proof['manifest_sha256'] == head.manifest_sha256
    assert proof['files'] == 2 and proof['logical_bytes'] == sum(
        (source/'nested'/name).stat().st_size for name in ('a.parquet', 'b.bin'))
    result = existing.cleanup(private, stage, 'fixture', 'a'*64, source.name, proof)
    assert result['state'] == 'completed' and not list(stage.glob('pack-*.zip'))
    assert packed.resolve_latest_packed(public, 'parent').manifest_sha256 == head.manifest_sha256


def test_minute_indexed_lock_reuses_the_matching_public_head(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    renamed = source.with_name('research_dataset')
    source.rename(renamed)
    dataset = 'tw-minute-train-workspace'
    handoff_index_archive.prepare(renamed, stage, dataset, 'a'*64, index,
        **kwargs, preserved_paths=['.download.lock'], supplemental_root_lock=False)
    public = tmp_path/'public'
    head = packed.publish_packed_snapshot(public, 'tw-minute-train', renamed, node_id='test',
                                           excluded_subtrees=['.download.lock'])
    private = public/'.local-state/private-history'
    packed.initialize_packed_layout(private, node_id='test')
    (public/'.stockagent-d-primary').write_text('test')
    pins = tmp_path/'pins'; pins.mkdir()
    monkeypatch.setattr(existing, '_materialized_root', lambda: pins)
    monkeypatch.setattr(packed, 'fetch_packed_snapshot', lambda *_a, **_kw: pytest.fail('no D payload readback'))
    proof = existing.match(private, stage, dataset, 'a'*64, index, renamed.name)
    assert proof['covering_authority'] == 'public' and proof['manifest_sha256'] == head.manifest_sha256
    assert proof['files'] == 2 and proof['independent_D_readback'] is False
    assert existing.cleanup(private, stage, dataset, 'a'*64, renamed.name, proof)['state'] == 'completed'


def test_futures_725_reuses_only_its_exact_public_parent_subtree(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    renamed = source.with_name('tw_futures_universe725_20261025_v2')
    source.rename(renamed)
    dataset = 'tw-futures-universe725-workspace'
    handoff_index_archive.prepare(renamed, stage, dataset, 'a'*64, index,
        **kwargs, preserved_paths=['.download.lock'], supplemental_root_lock=True)
    parent = tmp_path/'data_tw_index_futures'
    nested = parent/'training_prepared'
    nested.mkdir(parents=True)
    shutil.copytree(renamed, nested/renamed.name)
    (nested/'other-dataset').mkdir()
    (nested/'other-dataset'/'factorized.bin').write_bytes(b'untouched sibling')
    public = tmp_path/'public'
    head = packed.publish_packed_snapshot(public, 'tw-index-futures', parent, node_id='test')
    private = public/'.local-state/private-history'
    packed.initialize_packed_layout(private, node_id='test')
    (public/'.stockagent-d-primary').write_text('test')
    pins = tmp_path/'pins'; pins.mkdir()
    monkeypatch.setattr(existing, '_materialized_root', lambda: pins)
    monkeypatch.setattr(packed, 'fetch_packed_snapshot', lambda *_a, **_kw: pytest.fail('no D payload readback'))
    proof = existing.match(private, stage, dataset, 'a'*64, index, renamed.name)
    assert proof['manifest_sha256'] == head.manifest_sha256 and proof['files'] == 2
    assert proof['coverage_prefix'] == 'training_prepared/tw_futures_universe725_20261025_v2/'
    assert proof['covering_snapshot_pinned'] and not proof['independent_D_readback']
    assert existing.cleanup(private, stage, dataset, 'a'*64, renamed.name, proof)['state'] == 'completed'
    assert (nested/'other-dataset'/'factorized.bin').read_bytes() == b'untouched sibling'


def test_streamed_source_inventory_rejects_tampered_compressed_sha(tmp_path):
    import gzip
    import hashlib
    data = gzip.compress(b'{"path":"a","kind":"file"}\n')
    path = tmp_path/'inventory.jsonl.gz'
    path.write_bytes(data)
    with pytest.raises(SnapshotError, match='SHA or generation'):
        list(existing._source_inventory(path, '0'*64, len(data), ()))
    rows = list(existing._source_inventory(path, hashlib.sha256(data).hexdigest(), len(data), ()))
    assert [row['path'] for row in rows] == ['a']
