"""Archive recovery, content parity and error lifecycle of the actual adapter."""
from collections import Counter
import gzip
import hashlib
import json
import os
import stat
import zipfile

import pytest

from scripts import profile_workflow_speeds as profile
from stockagent.workflow_snapshot_pack import (ARCHIVE, SnapshotPackStore, SnapshotStreams,
                          snapshot_json, validate_manifest_storage)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def store_at(path, *, limit=1 << 20, budget=4 << 20):
    path.mkdir()
    return profile.Evidence(path, max_file_bytes=limit, max_total_bytes=budget,
                            snapshot_storage='packed_gzip_v1')


def sealed(output, source, raw=b'{"wall_seconds": 1.5}'):
    store = store_at(output)
    source.write_bytes(raw)
    assert store.read(source) == (raw, sha(raw))
    profile.write_evidence_manifest(output / 'evidence_manifest.json', store, [])
    manifest = json.loads((output / 'evidence_manifest.json').read_bytes())
    return store, manifest, f'evidence/{sha(raw)}.gz'


def streams(output, manifest):
    return SnapshotStreams(output, lambda name: output / name, storage=manifest['snapshot_storage'])


def write_report(output, store, ledger, **extra):
    profile.json_lines(output / 'speed_samples.jsonl', ledger.samples)
    profile.write_csv(output / 'speed_summary.csv', ledger.summaries())
    profile.atomic_write_json(output / 'inventory.json', {'rows': ledger.inventory})
    profile.write_evidence_manifest(output / 'evidence_manifest.json', store, [])
    report = dict(speed_samples=len(ledger.samples), summary_groups=len(ledger.summaries()),
        sample_counts=dict(Counter(row['origin'] for row in ledger.samples)),
        inventory_counts=dict(Counter(row['kind'] for row in ledger.inventory)), **extra)
    report['outputs'] = {p.name: dict(bytes=p.stat().st_size, sha256=sha(p.read_bytes()))
                         for p in output.iterdir() if p.is_file() and p.name != 'collection.json'}
    profile.atomic_write_json(output / 'collection.json', report)


@pytest.mark.parametrize('buffer_bytes', [64, 8 << 20])
def test_packed_adapter_keeps_canonical_gzip_and_inflight_rereads(tmp_path, buffer_bytes):
    output = tmp_path / 'out'
    store = store_at(output)
    # A small dirty window exercises backpatch/read fallback in the shared buffer.
    store.snapshot_store.buffer._capacity = buffer_bytes
    raw = b'{"wall_seconds": 1.25, "signed": -0.0, "unicode": "\xe8\x82\xa1"}'
    first, same, other = (tmp_path / name for name in ('first.json', 'same.json', 'other.json'))
    first.write_bytes(raw)
    same.write_bytes(raw)
    other.write_bytes(b'{}')
    try:
        assert store.read(first) == (raw, sha(raw))
        assert store.read(first) == (raw, sha(raw))  # before ZIP central directory exists
        assert store.read(same) == (raw, sha(raw))
        assert store.read(other) == (b'{}', sha(b'{}'))
        assert store.read(first) == (raw, sha(raw))  # reread after another append
        profile.write_evidence_manifest(output / 'evidence_manifest.json', store, [])
        manifest = json.loads((output / 'evidence_manifest.json').read_bytes())
        assert store.snapshot_store.closed and store.snapshot_store.directory is None
        assert store.total_bytes == 2 * len(raw) + 2
        assert len(manifest['files']) == 3 and manifest['snapshot_storage']['member_count'] == 2
        with zipfile.ZipFile(output / ARCHIVE) as archive:
            assert archive.read(sha(raw) + '.gz') == gzip.compress(raw, compresslevel=3, mtime=0)
        assert store.read(first) == (raw, sha(raw))  # independently sealed read
        with streams(output, manifest) as opened:
            assert snapshot_json(opened, f'evidence/{sha(raw)}.gz')['wall_seconds'] == 1.25
        assert manifest['snapshot_storage']['archive_sha256'] == sha((output / ARCHIVE).read_bytes())
    finally:
        store.close_snapshot_store()


def test_packed_evidence_matches_legacy_bytes_budgets_classifications_and_provenance(tmp_path):
    outputs = [tmp_path / 'legacy', tmp_path / 'packed']
    for path in outputs:
        path.mkdir()
    stores = [profile.Evidence(outputs[0], max_file_bytes=64, max_total_bytes=64),
              profile.Evidence(outputs[1], max_file_bytes=64, max_total_bytes=64,
                               snapshot_storage='packed_gzip_v1')]
    raw_items = [b'{"wall_seconds": 1.5}', b'{"value": -0.0}', b'x' * 65, b'z' * 40]
    paths = []
    for index, raw in enumerate(raw_items):
        path = tmp_path / f'{index}.json'
        path.write_bytes(raw)
        paths.append(path)
    try:
        results = []
        for store in stores:
            values = [store.read(paths[0]), store.read(paths[1], retain_if=lambda raw: False),
                      store.read(paths[2]), store.read(paths[3]), store.read(tmp_path / 'absent')]
            results.append(values)
        assert results[0] == results[1]
        assert stores[0].sources == stores[1].sources
        assert stores[0].issues == stores[1].issues and stores[0].total_bytes == stores[1].total_bytes
        profile.write_evidence_manifest(outputs[1] / 'evidence_manifest.json', stores[1], [])
    finally:
        for store in stores:
            store.close_snapshot_store()


def test_packed_inspection_keeps_all_five_payload_outputs_and_exact_input_manifest(tmp_path):
    inputs = []
    for index, raw in enumerate([b'{"wall_seconds":2.5}', b'{"value": 0}', b'{"wall_seconds":2.5}', b'bad']):
        path = tmp_path / f'{index}.json'
        path.write_bytes(raw)
        inputs.append(path)
    stores, manifests = [], []
    try:
        for layout in ('individual_gzip', 'packed_gzip_v1'):
            output = tmp_path / layout
            output.mkdir()
            store = profile.Evidence(output, max_file_bytes=4096, max_total_bytes=16384, snapshot_storage=layout)
            stores.append(store)
            ledger = profile.Ledger()
            metrics, catalog = profile.inspect_benchmarks(store, ledger, inputs, explicit_receipts={inputs[-1]},
                read_workers=1, read_batch_bytes=4096)
            profile.write_csv(output / 'reported_metrics.csv', metrics)
            profile.write_csv(output / 'benchmark_receipts.csv', catalog)
            write_report(output, store, ledger)
            result = profile.verify_collection(output)
            assert result['state'] == 'verified' and result['outputs_verified'] == 6
            manifests.append(json.loads((output / 'evidence_manifest.json').read_bytes()))
        for name in ('speed_samples.jsonl', 'speed_summary.csv', 'reported_metrics.csv', 'benchmark_receipts.csv', 'inventory.json'):
            assert (tmp_path / 'individual_gzip' / name).read_bytes() == (tmp_path / 'packed_gzip_v1' / name).read_bytes()
        assert manifests[0]['files'] == manifests[1]['files']
        assert manifests[0]['issues'] == manifests[1]['issues']
    finally:
        for store in stores:
            store.close_snapshot_store()


def test_packed_reader_verifies_known_identity_and_native_build_json_paths(tmp_path):
    output = tmp_path / 'out'
    store, ledger = store_at(output), profile.Ledger()
    known = tmp_path / 'known.json'
    known.write_text(json.dumps({'contract': profile.CONTRACT, 'rows': [
        dict(kind='source_endpoint', id='tej:known', provider='TEJ', snapshot_at_utc='old',
             active_scope=True, service_keys=['old'])]}))
    profile.collect_known_source_inventory(ledger, store, known)
    native, module, receipt = (tmp_path / name for name in ('tree.cpp', 'tree.so', 'build.json'))
    native.write_bytes(b'native source')
    module.write_bytes(b'native binary')
    build = dict(contract='source_bound_metric_tree_cpp_build_v1',
                 identity=dict(source_sha256=sha(native.read_bytes()), python='fixture'),
                 module_sha256=sha(module.read_bytes()))
    receipt.write_text(json.dumps(build))
    backend = dict(selected='cpp', source_path=str(native), source_sha256=sha(native.read_bytes()),
        module_path=str(module), module_sha256=sha(module.read_bytes()),
        build_receipt=str(receipt), build_receipt_sha256=sha(receipt.read_bytes()))
    for path in (native, module, receipt):
        store.read(path)
    profile.atomic_write_json(output / 'environment.json', dict(python='fixture'))
    try:
        write_report(output, store, ledger, metric_backend=backend)
        result = profile.verify_collection(output)
        assert result['native_metric_backend_verified'] == 1
        assert result['known_source_identity_links_verified'] == 1
        ledger.inventory[0]['live_readiness_verified'] = True
        write_report(output, store, ledger, metric_backend=backend)
        with pytest.raises(ValueError, match='visibility boundary'):
            profile.verify_collection(output)
    finally:
        store.close_snapshot_store()


@pytest.mark.parametrize('failure', ['archive_sha', 'truncate', 'gzip_crc', 'raw_sha', 'inventory', 'partial'])
def test_packed_recovery_rejects_corrupt_missing_or_incomplete_evidence(tmp_path, failure):
    output = tmp_path / 'out'
    store, manifest, name = sealed(output, tmp_path / 'source')
    if failure == 'raw_sha':
        write_report(output, store, profile.Ledger())
        manifest = json.loads((output / 'evidence_manifest.json').read_bytes())
    archive_path = output / ARCHIVE
    value = manifest['snapshot_storage']
    if failure in ('archive_sha', 'truncate'):
        raw = archive_path.read_bytes()
        archive_path.write_bytes(raw[:-8] if failure == 'truncate' else raw[:15] + bytes([raw[15] ^ 1]) + raw[16:])
    elif failure in ('gzip_crc', 'raw_sha'):
        member = next(iter(value['members'].values()))
        raw = gzip.compress(b'{"wall_seconds": 2.5}', compresslevel=3, mtime=0)
        if failure == 'gzip_crc':
            raw = raw[:-8] + bytes([raw[-8] ^ 1]) + raw[-7:]
        with zipfile.ZipFile(archive_path, 'w', compression=zipfile.ZIP_STORED) as archive:
            info = zipfile.ZipInfo(member['name'])
            info.external_attr = (stat.S_IFREG | 0o644) << 16
            archive.writestr(info, raw)
        member['gzip_bytes'] = len(raw)
        value.update(archive_bytes=archive_path.stat().st_size, archive_sha256=sha(archive_path.read_bytes()))
    elif failure == 'inventory':
        value['members'] = {}
        with pytest.raises(ValueError, match='inventory'):
            validate_manifest_storage(manifest)
        return
    if failure == 'raw_sha':
        profile.atomic_write_json(output / 'evidence_manifest.json', manifest)
        report = json.loads((output / 'collection.json').read_bytes())
        report['outputs']['evidence_manifest.json'] = dict(
            bytes=(output / 'evidence_manifest.json').stat().st_size,
            sha256=sha((output / 'evidence_manifest.json').read_bytes()))
        profile.atomic_write_json(output / 'collection.json', report)
        with pytest.raises(ValueError, match='producer SHA'):
            profile.verify_collection(output)
        return
    with pytest.raises((ValueError, OSError, zipfile.BadZipFile)):
        with streams(output, manifest) as opened:
            with opened.open(name) as decoded:
                decoded.read(1 if failure == 'partial' else -1)


@pytest.mark.parametrize('failure', ['duplicate', 'extra', 'wrong_mode', 'compressed', 'bad_name'])
def test_packed_reader_rejects_unadmitted_zip_inventory_and_encoding(tmp_path, failure):
    output = tmp_path / 'out'
    store, manifest, name = sealed(output, tmp_path / 'source')
    value = manifest['snapshot_storage']
    item = next(iter(value['members'].values()))
    path = output / ARCHIVE
    with zipfile.ZipFile(path) as original:
        raw = original.read(item['name'])
    with zipfile.ZipFile(path, 'w') as archive:
        info = zipfile.ZipInfo(item['name'] if failure != 'bad_name' else '../elsewhere.gz')
        info.external_attr = ((stat.S_IFLNK if failure == 'wrong_mode' else stat.S_IFREG) | 0o644) << 16
        info.compress_type = zipfile.ZIP_DEFLATED if failure == 'compressed' else zipfile.ZIP_STORED
        archive.writestr(info, raw)
        if failure in ('extra', 'duplicate'):
            with pytest.warns(UserWarning) if failure == 'duplicate' else __import__('contextlib').nullcontext():
                archive.writestr(item['name'] if failure == 'duplicate' else 'extra.gz', raw)
    value.update(archive_bytes=path.stat().st_size, archive_sha256=sha(path.read_bytes()))
    with pytest.raises(ValueError, match='inventory|encoding'):
        with streams(output, manifest):
            pass


@pytest.mark.parametrize('change', ['replace', 'rewrite_restore_mtime', 'symlink', 'fifo'])
def test_packed_reader_pins_archive_generation_and_rejects_nonregular_paths(tmp_path, change):
    output = tmp_path / 'out'
    store, manifest, name = sealed(output, tmp_path / 'source')
    path = output / ARCHIVE
    if change in ('symlink', 'fifo'):
        retained = output / 'retained.zip'
        path.rename(retained)
        if change == 'symlink':
            path.symlink_to(retained)
        else:
            os.mkfifo(path)
        with pytest.raises((ValueError, OSError)):
            with streams(output, manifest):
                pass
        return
    with pytest.raises(ValueError, match='generation changed'):
        with streams(output, manifest) as opened:
            assert snapshot_json(opened, name)['wall_seconds'] == 1.5
            before, raw = path.stat(), path.read_bytes()
            if change == 'replace':
                replacement = output / 'replacement.zip'
                replacement.write_bytes(raw)
                os.replace(replacement, path)
            else:
                path.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
                os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))


def test_packed_raw_budget_cannot_be_bypassed_by_gzip_trailer(tmp_path):
    output = tmp_path / 'out'
    store, manifest, name = sealed(output, tmp_path / 'source')
    item = next(iter(manifest['snapshot_storage']['members'].values()))
    item['raw_bytes'] = 1
    with pytest.raises(ValueError, match='raw size'):
        with streams(output, manifest) as opened:
            with opened.open(name) as decoded:
                decoded.read()


def test_packed_writer_failure_closes_descriptors_and_retains_uncommitted_bytes(tmp_path, monkeypatch):
    output = tmp_path / 'out'
    store = store_at(output)
    source = tmp_path / 'source'
    source.write_bytes(b'{}')

    def fail(*args, **kwargs):
        raise OSError('short underlying write')

    monkeypatch.setattr(store.snapshot_store.archive, 'writestr', fail)
    try:
        assert store.read(source) is None
        assert store.issues[-1]['reason'] == 'OSError'
        with pytest.raises(ValueError, match='failed before seal'):
            profile.write_evidence_manifest(output / 'evidence_manifest.json', store, [])
    finally:
        store.close_snapshot_store()
    assert store.snapshot_store.closed and store.snapshot_store.stream.closed
    assert store.snapshot_store.directory is None and store.snapshot_store.descriptor is None
    assert (output / ARCHIVE).exists() and not (output / 'evidence_manifest.json').exists()


def test_packed_collect_releases_writer_when_early_stage_raises(tmp_path, monkeypatch):
    from argparse import Namespace
    retained = []
    original = profile.SnapshotPackStore

    def writer(*args, **kwargs):
        value = original(*args, **kwargs)
        retained.append(value)
        return value

    def fail(*args, **kwargs):
        raise RuntimeError('entrypoint stage failed')

    monkeypatch.setattr(profile, 'SnapshotPackStore', writer)
    monkeypatch.setattr(profile, 'configure_metric_backend', fail)
    output = tmp_path / 'out'
    with pytest.raises(RuntimeError, match='entrypoint stage failed'):
        profile.collect(Namespace(output_dir=output, max_file_mib=1, max_total_mib=1,
                                  evidence_storage_layout='packed_gzip_v1'))
    assert len(retained) == 1 and retained[0].closed and retained[0].stream.closed
    assert retained[0].directory is None and retained[0].descriptor is None
    assert (output / ARCHIVE).exists() and not (output / 'collection.json').exists()
