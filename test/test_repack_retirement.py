import hashlib
import json
import os
from contextlib import contextmanager
from pathlib import Path

import pytest

from stockagent.data_sync import packed_snapshots as packed, repack_retirement, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_backup import signature


def test_fresh_manifest_census_covers_batches_and_prunes_absent_owners(tmp_path, monkeypatch):
    cold = tmp_path/'cold'
    metadata, paths = {}, set()
    for index in range(530):
        path = cold/'manifests'/f'dataset-{index // 10}'/f'snapshot-{index}.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{}')
        relative = path.relative_to(cold).as_posix()
        paths.add(relative)
        metadata[relative] = list(signature(path))
    missing = 'manifests/missing/deleted.json'
    refs = {'object': paths | {missing}}
    monkeypatch.setattr(repack_retirement, '_read_manifest',
                        lambda *_a: pytest.fail('an unchanged fixed manifest should not be reparsed'))
    known = repack_retirement.fresh_manifest_references(cold, metadata, {'object'}, refs)
    assert known == {Path(path).stem for path in paths}
    assert refs['object'] == paths


@pytest.mark.parametrize('broken', [False, True])
def test_fresh_manifest_census_rejects_symlinks(tmp_path, broken):
    cold = tmp_path/'cold'
    directory = cold/'manifests/fixture'
    directory.mkdir(parents=True)
    target = tmp_path/'target'
    if not broken:
        target.write_text('{}')
    (directory/'redirected.json').symlink_to(target)
    with pytest.raises(SnapshotError, match='cold manifest is not a regular file'):
        repack_retirement.fresh_manifest_references(cold, {}, set(), {})


def test_batched_history_scan_reads_each_head_once_and_checks_identity(tmp_path, monkeypatch):
    root = tmp_path/'head-history/heads/fixture/owner'
    root.mkdir(parents=True)
    versions, expected = [], set()
    for index in range(40):
        digest = hashlib.sha256(f'manifest-{index}'.encode()).hexdigest()
        snapshot = f'fixture-{index}'
        raw = json.dumps({'dataset': 'fixture', 'manifest_sha256': digest,
                          'snapshot_id': snapshot}).encode()
        path = root/(hashlib.sha256(raw).hexdigest()+'.json')
        path.write_bytes(raw)
        if index < 8:
            versions.append((digest, snapshot))
            expected.add(path.relative_to(tmp_path).as_posix())
    read = Path.read_bytes
    count = 0
    def counted(path):
        nonlocal count
        count += 1
        return read(path)
    monkeypatch.setattr(Path, 'read_bytes', counted)
    assert set(repack_retirement._histories_many(tmp_path, 'fixture', versions)) == expected
    assert count == 40  # Not 40 times the eight requested versions.
    with pytest.raises(SnapshotError, match='identities conflict'):
        repack_retirement._histories_many(tmp_path, 'fixture', versions+[('f'*64, versions[0][1])])


def _fixture(tmp_path, monkeypatch):
    source, cold, stage = tmp_path/'source', tmp_path/'cold', tmp_path/'E-stage'
    source.mkdir()
    (source/'small.txt').write_bytes(b'small file')
    (source/'large.bin').write_bytes(b'0123456789'*10)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        loose_file_threshold_bytes=12, file_chunk_bytes=16, pack_buckets=8)
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_k: None)
    return source, cold, stage, old


def _receipt(result):
    return {'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': result['dataset'],
            'old_manifest_sha256': result['old_manifest_sha256'],
            'new_manifest_sha256': result['new_manifest_sha256'],
            'new_snapshot_id': result['snapshot_id']}


@pytest.mark.parametrize('scoped', [False, True])
def test_shared_pin_namespace_recognizes_only_an_exact_other_authority_manifest(tmp_path, monkeypatch, scoped):
    source, public, _stage, _old = _fixture(tmp_path, monkeypatch)
    private = public/'.local-state/private-history'
    other = packed.publish_packed_snapshot(private, 'private-fixture', source, node_id='test')
    materialized = tmp_path/'materialized'
    materialized.mkdir()
    pin, _ = stream_repack._repack_pin(materialized, 'private-fixture', other.manifest_sha256,
                                       other.manifest['snapshot_id'], tmp_path/'stage', scoped=scoped)
    known = repack_retirement.include_other_authority_repack_pins(public, materialized, set())
    assert known == {other.manifest['snapshot_id']}
    protected, _evidence, errors = repack_retirement.packed_retention._local_protected_snapshot_ids(materialized, known)
    assert protected == known and not errors
    value = json.loads(pin.read_text())
    value['manifest_sha256'] = 'a'*64
    pin.write_text(json.dumps(value))
    assert repack_retirement.include_other_authority_repack_pins(public, materialized, set()) == set()
    _protected, _evidence, errors = repack_retirement.packed_retention._local_protected_snapshot_ids(materialized, set())
    assert errors  # An unresolved pin still prevents unrelated deletion.


def test_scoped_shared_pin_requires_its_exact_stage_binding(tmp_path, monkeypatch):
    source, public, _stage, _old = _fixture(tmp_path, monkeypatch)
    private = public/'.local-state/private-history'
    other = packed.publish_packed_snapshot(private, 'private-fixture', source, node_id='test')
    materialized = tmp_path/'materialized'
    materialized.mkdir()
    pin, _ = stream_repack._repack_pin(materialized, 'private-fixture', other.manifest_sha256,
                                       other.manifest['snapshot_id'], tmp_path/'stage', scoped=True)
    value = json.loads(pin.read_text())
    value['stage'] = str(tmp_path/'different-stage')
    pin.write_text(json.dumps(value))
    assert repack_retirement.include_other_authority_repack_pins(public, materialized, set()) == set()
    _protected, _evidence, errors = repack_retirement.packed_retention._local_protected_snapshot_ids(materialized, set())
    assert errors  # An invalid scope remains a blocker, not deletion authority.


@pytest.mark.parametrize('defect', [None, 'digest', 'manifest', 'filename', 'missing_object'])
def test_stale_other_authority_index_resolves_only_a_valid_exact_training_pin(tmp_path, monkeypatch, defect):
    _source, public, _stage, fixed = _fixture(tmp_path, monkeypatch)
    private = public/'.local-state/private-history'
    private.mkdir(parents=True)
    materialized = tmp_path/'materialized'
    pin = materialized/'.training-pins/run'/('wrong.pin.json' if defect == 'filename' else 'fixture.pin.json')
    pin.parent.mkdir(parents=True)
    packed.write_packed_pin(pin, fixed)
    if defect in {'digest', 'manifest'}:
        value = json.loads(pin.read_text())
        if defect == 'digest':
            value['manifest_sha256'] = '0'*64
        else:
            value['manifest']['metadata']['unrelated_change'] = True
        pin.write_text(json.dumps(value))
    if defect == 'missing_object':
        (public/fixed.manifest['archive']['objects'][0]['relpath']).unlink()
    known = repack_retirement.include_other_authority_repack_pins(private, materialized, set())
    protected, _evidence, errors = repack_retirement.packed_retention._local_protected_snapshot_ids(materialized, known)
    if defect is None:
        assert known == protected == {fixed.manifest['snapshot_id']} and not errors
    else:
        assert not known and not protected and errors


def _setup(tmp_path, monkeypatch, *, shared=False):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    receipt = {**_receipt(result), 'source_mode': 'D_streamed_to_E_dataset_pack'}
    old_relative = old.manifest_path.relative_to(cold).as_posix()
    refs = [old.manifest['archive']['inventory'], *old.manifest['archive']['objects']]
    metadata = {old_relative: list(signature(old.manifest_path))}
    releases = [{'manifest_relative': old_relative,
                 'required_file_keys': [row['relpath']+'@'+row['sha256'] for row in refs]}]
    if shared:
        fake = 'manifests/other/other-old.json'
        fake_path = cold/fake
        fake_path.parent.mkdir(parents=True)
        fake_manifest = {**old.manifest, 'dataset': 'other', 'snapshot_id': 'other-old'}
        fake_path.write_text(json.dumps(fake_manifest))
        metadata[fake] = list(signature(fake_path))
        releases.append({'manifest_relative': fake,
                         'required_file_keys': [row['relpath']+'@'+row['sha256'] for row in refs]})
    catalog = tmp_path/'catalog.json'
    catalog.write_text(json.dumps({'releases': releases, 'metadata_errors': [], 'excluded_manifests': []}))
    monkeypatch.setattr(repack_retirement.old_retirement, 'catalog_index',
                        lambda *_a, **_k: (metadata, {}, 'catalog-id'))
    monkeypatch.setattr(repack_retirement, '_private_catalog_generation', lambda *_a: [1])
    monkeypatch.setattr(repack_retirement, 'process_references_many', lambda *_a, **_k: [])
    monkeypatch.setattr(repack_retirement.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_k: (set(), [], []))
    kwargs = {'catalog': catalog, 'observation': tmp_path/'obs', 'cache': tmp_path/'cache',
              'materialized': tmp_path/'materialized', 'retention_state': tmp_path/'retention'}
    return cold, stage, old, receipt, kwargs, refs


def test_target_reference_cache_matches_full_catalog_scan(tmp_path, monkeypatch):
    cold, _stage, _old, _receipt, options, rows = _setup(tmp_path, monkeypatch, shared=True)
    metadata, _, identity = repack_retirement.old_retirement.catalog_index()
    targets = {row['relpath'] for row in rows}
    generation, cache = [1], tmp_path/'target-cache.json'
    expected = repack_retirement.catalog_references(options['catalog'], cold, metadata, targets, generation)
    result = repack_retirement.build_catalog_reference_cache(
        options['catalog'], cold, metadata, targets, generation, identity, cache)
    assert result['targets'] == len(targets)
    monkeypatch.setattr(repack_retirement, '_scan_catalog_named_references',
                        lambda *_a: pytest.fail('a matching cache should avoid a second catalog pass'))
    assert repack_retirement.catalog_references(options['catalog'], cold, metadata, targets, generation,
        reference_cache=cache, catalog_identity=identity) == expected


def test_target_cache_revisits_omitted_manifest_that_reappears(tmp_path, monkeypatch):
    cold, _stage, _old, _receipt, options, rows = _setup(tmp_path, monkeypatch, shared=True)
    metadata, _, identity = repack_retirement.old_retirement.catalog_index()
    omitted = 'manifests/other/other-old.json'
    catalog_value = json.loads(options['catalog'].read_text())
    catalog_value['releases'] = [row for row in catalog_value['releases'] if row['manifest_relative'] != omitted]
    options['catalog'].write_text(json.dumps(catalog_value))
    parked = tmp_path/'parked-manifest.json'
    (cold/omitted).rename(parked)
    targets = {row['relpath'] for row in rows}
    cache = tmp_path/'target-cache.json'
    repack_retirement.build_catalog_reference_cache(options['catalog'], cold, metadata, targets, [1], identity, cache)
    parked.rename(cold/omitted)
    refs = repack_retirement.catalog_references(options['catalog'], cold, metadata, targets, [1],
        reference_cache=cache, catalog_identity=identity)
    # A rename changes ctime; the final locked census must detect that change.
    repack_retirement.fresh_manifest_references(cold, metadata, targets, refs)
    assert all(omitted in owners for owners in refs.values())
    # Also exercise the case where the reappearing name still has the fixed
    # signature: omission must never be stored as a persistent absence.
    metadata[omitted] = list(signature(cold/omitted))
    refs = repack_retirement.catalog_references(options['catalog'], cold, metadata, targets, [1],
        reference_cache=cache, catalog_identity=identity)
    assert all(omitted in owners for owners in refs.values())


@pytest.mark.parametrize('damage', ['identity', 'generation', 'coverage'])
def test_invalid_target_reference_cache_falls_back_to_full_scan(tmp_path, monkeypatch, damage):
    cold, _stage, _old, _receipt, options, rows = _setup(tmp_path, monkeypatch, shared=True)
    metadata, _, identity = repack_retirement.old_retirement.catalog_index()
    targets = {row['relpath'] for row in rows}
    cache = tmp_path/'target-cache.json'
    expected = repack_retirement.catalog_references(options['catalog'], cold, metadata, targets, [1])
    repack_retirement.build_catalog_reference_cache(options['catalog'], cold, metadata, targets, [1], identity, cache)
    value = json.loads(cache.read_text())
    if damage == 'identity':
        value['identity_sha256'] = '0'*64
    elif damage == 'generation':
        value['catalog_generation'] = [2]
        value['identity_sha256'] = stream_repack._identity(value)
    else:
        value['references'].pop(next(iter(targets)))
        value['identity_sha256'] = stream_repack._identity(value)
    cache.write_text(json.dumps(value))
    scan = repack_retirement._scan_catalog_named_references
    calls = []
    def counted(*args):
        calls.append(True)
        return scan(*args)
    monkeypatch.setattr(repack_retirement, '_scan_catalog_named_references', counted)
    assert repack_retirement.catalog_references(options['catalog'], cold, metadata, targets, [1],
        reference_cache=cache, catalog_identity=identity) == expected
    assert calls == [True]


@pytest.mark.parametrize('bad_field', ['metadata_errors', 'excluded_manifests'])
def test_target_cache_rejects_incomplete_catalog(tmp_path, monkeypatch, bad_field):
    cold, _stage, _old, _receipt, options, rows = _setup(tmp_path, monkeypatch)
    metadata, _, identity = repack_retirement.old_retirement.catalog_index()
    value = json.loads(options['catalog'].read_text())
    value[bad_field] = ['incomplete']
    options['catalog'].write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match='incomplete or changed'):
        repack_retirement.build_catalog_reference_cache(options['catalog'], cold, metadata,
            {row['relpath'] for row in rows}, [1], identity, tmp_path/'target-cache.json')
    assert not (tmp_path/'target-cache.json').exists()


def test_foreign_current_head_blocks_before_expensive_catalog_scan(tmp_path, monkeypatch):
    cold, stage, old, receipt, options, _refs = _setup(tmp_path, monkeypatch)
    head = cold/'heads/fixture/other-node.json'
    head.write_text(json.dumps({'dataset': 'fixture', 'snapshot_id': old.manifest['snapshot_id'],
                                'manifest_sha256': old.manifest_sha256}))
    monkeypatch.setattr(repack_retirement.old_retirement, 'catalog_index',
                        lambda *_a, **_kw: pytest.fail('current head must be checked before the catalog'))
    with pytest.raises(SnapshotError, match='still a current head'):
        repack_retirement.retire(cold, stage, receipt, apply=True, **options)
    assert head.exists() and old.manifest_path.exists()


def test_busy_publication_yields_before_catalog_without_touching_old_release(tmp_path, monkeypatch):
    cold, stage, old, receipt, options, _refs = _setup(tmp_path, monkeypatch)
    attempt_counts = []
    @contextmanager
    def busy(_cold, _locks, *, attempts=1):
        attempt_counts.append(attempts)
        raise SnapshotError('retirement owner busy: publication')
        yield
    monkeypatch.setattr(repack_retirement, '_boundary', busy)
    monkeypatch.setattr(repack_retirement.old_retirement, 'catalog_index',
                        lambda *_a, **_kw: pytest.fail('busy owner must be checked before the catalog'))
    with pytest.raises(SnapshotError, match='owner busy: publication'):
        repack_retirement.retire(cold, stage, receipt, apply=True, **options)
    assert attempt_counts == [1]
    assert old.manifest_path.exists() and not (stage/'retirement.json').exists()


def test_exact_pin_yields_before_catalog_even_when_other_pins_are_unknown(tmp_path, monkeypatch):
    cold, stage, old, receipt, options, _refs = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(repack_retirement.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_k: ({old.manifest['snapshot_id']}, [], ['other pin is unknown']))
    monkeypatch.setattr(repack_retirement.old_retirement, 'catalog_index',
                        lambda *_a, **_kw: pytest.fail('pinned candidate must not scan the catalog'))
    with pytest.raises(SnapshotError, match='old release is pinned'):
        repack_retirement.retire(cold, stage, receipt, apply=True, **options)
    assert old.manifest_path.exists() and not (stage/'retirement.json').exists()


def test_unknown_pin_during_early_check_does_not_authorize_retirement(tmp_path, monkeypatch):
    cold, stage, old, receipt, options, _refs = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(repack_retirement.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_k: (set(), [], ['unreadable pin']))
    with pytest.raises(SnapshotError, match='protection metadata is unknown'):
        repack_retirement.retire(cold, stage, receipt, apply=True, **options)
    assert old.manifest_path.exists() and not (stage/'retirement.json').exists()


def test_retirement_keeps_shared_object_and_current_release(tmp_path, monkeypatch):
    cold, stage, old, receipt, kwargs, refs = _setup(tmp_path, monkeypatch, shared=True)
    plan = repack_retirement.retire(cold, stage, receipt, **kwargs)
    assert plan['state'] == 'planned' and plan['apply'] is False
    result = repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)
    assert result['state'] == 'completed' and not old.manifest_path.exists()
    assert (cold/refs[0]['relpath']).exists()  # required by another release
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == receipt['new_manifest_sha256']
    assert repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)['state'] == 'completed'


def test_retirement_preserves_old_release_if_pin_or_process_is_active(tmp_path, monkeypatch):
    cold, stage, old, receipt, kwargs, _refs = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(repack_retirement.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_k: ({old.manifest['snapshot_id']}, [], []))
    with pytest.raises(SnapshotError, match='pinned'):
        repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)
    assert old.manifest_path.exists()


def test_retirement_unlinks_old_name_but_preserves_other_hardlink(tmp_path, monkeypatch):
    cold, stage, _old, receipt, kwargs, refs = _setup(tmp_path, monkeypatch)
    old_path = cold/refs[0]['relpath']
    alias = tmp_path/'private-copy'
    os.link(old_path, alias)
    result = repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)
    member = next(item for item in result['plan']['items'] if item['relative'] == refs[0]['relpath'])
    assert member['allocated_bytes'] == 0
    assert not old_path.exists() and alias.is_file()


def test_retirement_resumes_after_unlink_interruption(tmp_path, monkeypatch):
    cold, stage, old, receipt, kwargs, _refs = _setup(tmp_path, monkeypatch)
    original = packed._fsync_directory
    failed = False
    def interrupted(path):
        nonlocal failed
        if path == old.manifest_path.parent and not failed:
            failed = True
            raise OSError('interrupted after unlink')
        return original(path)
    monkeypatch.setattr(packed, '_fsync_directory', interrupted)
    with pytest.raises(OSError, match='interrupted after unlink'):
        repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)
    assert not old.manifest_path.exists()
    journal = json.loads((stage/'retirement.json').read_text())
    assert 'plan' not in journal and journal['removed_count'] == 0
    assert (stage/journal['plan_ref']).is_file() and isinstance(journal['pending'], list)
    monkeypatch.setattr(packed, '_fsync_directory', original)
    result = repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)
    assert result['state'] == 'completed'


def test_retired_d_names_are_durably_queued_for_syncthing_even_after_retry(tmp_path, monkeypatch):
    from stockagent.data_sync import syncthing_scan
    cold, stage, old, receipt, kwargs, _refs = _setup(tmp_path, monkeypatch)
    (cold/syncthing_scan.D_PRIMARY_MARKER).write_text('{}')
    monkeypatch.setattr(syncthing_scan, 'CANONICAL_ROOT', cold)
    head = {'dataset':'fixture','snapshot_id':old.manifest['snapshot_id'],
            'manifest_sha256':old.manifest_sha256}
    raw = json.dumps(head).encode()
    history = cold/'head-history/heads/fixture/penguin'/(hashlib.sha256(raw).hexdigest()+'.json')
    history.parent.mkdir(parents=True, exist_ok=True); history.write_bytes(raw)
    original = syncthing_scan.queue_after_publish
    calls = []
    def interrupted(*args, **options):
        calls.append(True)
        if len(calls) == 1:
            raise OSError('notification journal unavailable')
        return original(*args, **options)
    monkeypatch.setattr(syncthing_scan, 'queue_after_publish', interrupted)
    with pytest.raises(OSError, match='notification journal unavailable'):
        repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)
    assert not history.exists()
    result = repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)
    assert result['state'] == 'completed'
    pending = json.loads((cold/'.local-state/scan-pending/fixture.json').read_text())
    assert history.relative_to(cold).as_posix() in pending['new_object_paths']
    assert old.manifest_path.relative_to(cold).as_posix() in pending['new_object_paths']


def test_busy_object_does_not_block_other_journalled_objects_and_retry_finishes(tmp_path, monkeypatch):
    cold, stage, old, receipt, kwargs, refs = _setup(tmp_path, monkeypatch)
    blocked = cold/refs[0]['relpath']
    original_unlink = Path.unlink
    def temporarily_busy(path, *args, **options):
        if path == blocked:
            raise PermissionError('sharing violation')
        return original_unlink(path, *args, **options)
    monkeypatch.setattr(Path, 'unlink', temporarily_busy)
    with pytest.raises(SnapshotError, match='temporarily unavailable'):
        repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)
    assert blocked.exists() and not old.manifest_path.exists()
    assert any(not (cold/item['relpath']).exists() for item in refs[1:])
    assert isinstance(json.loads((stage/'retirement.json').read_text())['pending'], list)
    monkeypatch.setattr(Path, 'unlink', original_unlink)
    assert repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)['state'] == 'completed'


def test_busy_manifest_stops_before_any_old_objects_are_removed(tmp_path, monkeypatch):
    cold, stage, old, receipt, kwargs, refs = _setup(tmp_path, monkeypatch)
    original_unlink = Path.unlink
    def busy_manifest(path, *args, **options):
        if path == old.manifest_path:
            raise PermissionError('sharing violation')
        return original_unlink(path, *args, **options)
    monkeypatch.setattr(Path, 'unlink', busy_manifest)
    with pytest.raises(PermissionError):
        repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)
    assert old.manifest_path.exists() and all((cold/item['relpath']).exists() for item in refs)


def test_changed_new_object_blocks_old_release_retirement(tmp_path, monkeypatch):
    cold, stage, old, receipt, kwargs, _refs = _setup(tmp_path, monkeypatch)
    latest = packed.resolve_latest_packed(cold, 'fixture')
    obj = cold/latest.manifest['archive']['objects'][0]['relpath']
    obj.write_bytes(b'x' * obj.stat().st_size)
    with pytest.raises(SnapshotError, match='replacement object generation changed'):
        repack_retirement.retire(cold, stage, receipt, **kwargs, apply=True)
    assert old.manifest_path.exists()


def test_retirement_lock_retry_releases_partial_boundary(tmp_path, monkeypatch):
    events = []
    attempts = 0
    @contextmanager
    def publication(*_a, **_k):
        events.append('global-acquired')
        try:
            yield
        finally:
            events.append('global-released')
    @contextmanager
    def other(_path):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise BlockingIOError('busy')
        yield
    monkeypatch.setattr(packed, '_try_publication_lock', publication)
    monkeypatch.setattr(repack_retirement.old_retirement, '_existing_lock', other)
    monkeypatch.setattr(repack_retirement.time, 'sleep', lambda _seconds: None)
    with repack_retirement._boundary(tmp_path, [tmp_path/'lock'], attempts=2):
        events.append('work')
    assert events == ['global-acquired', 'work', 'global-released']


def test_retirement_releases_other_owners_when_publication_is_busy(tmp_path, monkeypatch):
    events, attempts = [], 0
    @contextmanager
    def other(_path):
        events.append('other-acquired')
        try:
            yield
        finally:
            events.append('other-released')
    @contextmanager
    def publication(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise packed.PackedPublicationBusy('busy')
        events.append('global-acquired')
        try:
            yield
        finally:
            events.append('global-released')
    monkeypatch.setattr(packed, '_try_publication_lock', publication)
    monkeypatch.setattr(repack_retirement.old_retirement, '_existing_lock', other)
    monkeypatch.setattr(repack_retirement.time, 'sleep', lambda _seconds: None)
    with repack_retirement._boundary(tmp_path, [tmp_path/'lock'], attempts=2):
        events.append('work')
    assert events == ['other-acquired', 'other-released', 'other-acquired', 'global-acquired',
                      'work', 'global-released', 'other-released']
