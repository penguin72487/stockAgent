"""A full cold history plus current increments stays a complete source release."""
from dataclasses import replace
import os
from pathlib import Path
import shutil

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError, scan_tree


def fixture(tmp_path, *, private=False, chunked=False):
    source = tmp_path / 'source'
    cold = tmp_path / 'cold'
    options = {'loose_file_threshold_bytes': 64, 'pack_buckets': 1}
    if private:
        from stockagent.data_sync.dataset_migration import private_history_root
        packed.initialize_packed_layout(cold, node_id='test')
        cold = private_history_root(cold, {'private_history': {
            'enabled': True, 'relative_root': '.local-state/private-history', 'shared_export': False,
            'nas_export': False, 'source_retirement': False}})
        options.update(private_observation_cohort=True, private_atomic_json_controls=True)
    if chunked:
        options['file_chunk_bytes'] = 64
    values = {'raw/A/old.gz': b'old original negative value -3.14',
              'raw/A/revision.gz': b'initial version' * 16,
              'raw/Z/deep/empty.gz': b'', 'raw/Z/deep/blob.bin': bytes(range(251)) * 5,
              'service.txt': b'current necessary view', 'state/current.json': b'{"version":1}'}
    for name, value in values.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    release = packed.publish_packed_snapshot(cold, 'source', source, **options)
    names = tuple(sorted(name for name in values if name.startswith('raw/')))
    request = packed.RetainedSourceFiles(release, names, 65536, 1024**2)
    return source, cold, release, request, options, values


@pytest.mark.parametrize('private,chunked', [(False, False), (False, True), (True, True)])
def test_full_union_revision_increment_recovery_and_noop_match_unretired_oracle(tmp_path, private, chunked):
    source, cold, base, request, options, values = fixture(tmp_path, private=private, chunked=chunked)
    oracle = tmp_path / 'unretired-oracle'
    shutil.copytree(source, oracle)
    # Only the isolated fixture retires files. Production retirement has its own gates.
    for name in ('raw/A/old.gz', 'raw/Z/deep/blob.bin', 'raw/Z/deep/empty.gz'):
        (source / name).unlink()
    updates = {'raw/A/revision.gz': b'corrected source revision -7.25',
               'raw/A/new.gz': b'new source increment', 'state/current.json': b'{"version":2}'}
    for name, value in updates.items():
        (source / name).write_bytes(value)
        (oracle / name).write_bytes(value)
    original_objects = {row['sha256'] for row in base.manifest['archive']['objects']}
    current = packed.publish_packed_snapshot(cold, 'source', source,
        retained_source_files=request, **options)
    expected = scan_tree(oracle)
    for key in ('files', 'directories', 'symlinks', 'logical_bytes', 'portable_fingerprint_sha256'):
        assert current.manifest['source'][key] == expected[key]
    selection = current.manifest['source']['selection']
    assert selection['physical_workspace_counts']['files'] == expected['files'] - 3
    assert selection['retained_source_base']['manifest_sha256'] == base.manifest_sha256
    restored = packed.fetch_packed_snapshot(cold, tmp_path / 'hot', current)
    actual = {path.relative_to(restored).as_posix(): path.read_bytes()
              for path in restored.rglob('*') if path.is_file()}
    assert actual == values | updates
    assert original_objects.intersection(row['sha256'] for row in current.manifest['archive']['objects'])
    assert packed.publish_packed_snapshot(cold, 'source', source,
        retained_source_files=request, **options).manifest_sha256 == current.manifest_sha256
    # The original fixed release is independently still recoverable.
    original = packed.fetch_packed_snapshot(cold, tmp_path / 'original-hot', base)
    assert {path.relative_to(original).as_posix(): path.read_bytes()
            for path in original.rglob('*') if path.is_file()} == values
    with pytest.raises(SnapshotError, match='cannot be dropped'):
        packed.publish_packed_snapshot(cold, 'source', source, **options)
    assert packed.resolve_latest_packed(cold, 'source').manifest_sha256 == current.manifest_sha256


@pytest.mark.parametrize('problem', ['dataset', 'outside-raw', 'missing-parent', 'symlink',
                                   'collision', 'duplicate', 'byte-bound', 'metadata-bound'])
def test_invalid_union_cannot_commit_or_change_the_source(tmp_path, monkeypatch, problem):
    source, cold, base, request, options, values = fixture(tmp_path)
    if problem == 'dataset':
        request = replace(request, release=replace(base, manifest={**base.manifest, 'dataset': 'other'}))
    elif problem == 'outside-raw':
        request = replace(request, names=('service.txt',))
    elif problem == 'missing-parent':
        shutil.rmtree(source / 'raw/Z')
    elif problem == 'symlink':
        (source / 'extra').symlink_to('service.txt')
    elif problem == 'collision':
        (source / 'raw/A/old.gz').unlink()
        (source / 'raw/A/old.gz').mkdir()
    elif problem == 'duplicate':
        request = replace(request, names=request.names * 2)
    elif problem == 'byte-bound':
        request = replace(request, maximum_bytes=1)
    elif problem == 'metadata-bound':
        monkeypatch.setattr(packed, 'MAX_PACK_MEMBERS', 3)
    with pytest.raises(SnapshotError):
        packed.publish_packed_snapshot(cold, 'source', source, retained_source_files=request, **options)
    assert packed.resolve_latest_packed(cold, 'source').manifest_sha256 == base.manifest_sha256
    assert (source / 'service.txt').read_bytes() == values['service.txt']


@pytest.mark.parametrize('private', [False, True])
@pytest.mark.parametrize('changed', ['source', 'cold-object', 'cold-owner'])
def test_union_commit_keeps_both_physical_and_fixed_cold_generation_fences(tmp_path, monkeypatch, private, changed):
    source, cold, base, request, options, _ = fixture(tmp_path, private=private)
    original = packed._merge_retained_source_files
    def changing(*args, **kwargs):
        original(*args, **kwargs)
        if changed == 'source':
            (source / 'service.txt').write_bytes(b'changed after complete physical fence')
        else:
            selected = (cold / base.manifest['archive']['objects'][0]['relpath']
                        if changed == 'cold-object' else cold / '.local-state/locks/publish-retention-global.lock')
            replacement = selected.with_suffix('.replacement')
            replacement.write_bytes(selected.read_bytes())
            os.replace(replacement, selected)
    monkeypatch.setattr(packed, '_merge_retained_source_files', changing)
    with pytest.raises(SnapshotError, match='changed|owner|cohort'):
        packed.publish_packed_snapshot(cold, 'source', source, retained_source_files=request, **options)
    assert packed.resolve_latest_packed(cold, 'source').manifest_sha256 == base.manifest_sha256


def test_corrupted_cold_original_is_not_replaced_by_a_current_hot_copy(tmp_path):
    source, cold, base, request, options, _ = fixture(tmp_path)
    item = base.manifest['archive']['objects'][0]
    path = cold / item['relpath']
    path.write_bytes(b'X' * item['bytes'])
    with pytest.raises(SnapshotError, match='checksum'):
        packed.publish_packed_snapshot(cold, 'source', source, retained_source_files=request, **options)
    assert packed.resolve_latest_packed(cold, 'source').manifest_sha256 == base.manifest_sha256
