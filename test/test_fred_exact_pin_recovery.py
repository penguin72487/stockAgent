import json
from pathlib import Path
import shutil

import pytest

from stockagent.data_sync import fred_exact_pin_recovery as recovery
from stockagent.data_sync import packed_snapshots as packed, retained_source as retained
from stockagent.data_sync import private_handoff_retirement as retirement
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json


def fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    source = tmp_path/'source'
    (source/'raw').mkdir(parents=True)
    (source/'state').mkdir()
    (source/'raw/original.json.gz').write_bytes(b'fixed raw observation')
    (source/'summary.json').write_bytes(b'{"fixed":true}\n')
    materialized = tmp_path/'original'
    shutil.copytree(source, materialized)
    cold, pins, scratch = tmp_path/'cold', tmp_path/'pins', tmp_path/'scratch'
    scratch.mkdir(mode=0o700)
    fixed = packed.publish_packed_snapshot(cold, recovery.DATASET, materialized, node_id='fixture',
                                          pack_buckets=4, file_chunk_bytes=8*1024**2)
    raw = {'raw/original.json.gz': {'bytes': len(b'fixed raw observation'),
        'sha256': packed.sha256_file(source/'raw/original.json.gz')}}
    marker = {'contract': retained.CONTRACT, 'dataset': recovery.DATASET, 'source': str(source),
        'cold_root': str(cold), 'snapshot_id': fixed.manifest['snapshot_id'],
        'manifest_sha256': fixed.manifest_sha256,
        'inventory_sha256': fixed.manifest['archive']['inventory']['sha256'], 'raw_files': raw,
        'workspace_raw_verification': retained._raw_workspace_proof(source, materialized, raw),
        'recovery': {'manifest_sha256': fixed.manifest_sha256, 'materialized_verified': True,
            'independently_reconstructed_files': fixed.manifest['source']['files'],
            'independently_reconstructed_bytes': fixed.manifest['source']['logical_bytes']}}
    atomic_write_json(source/retained.BINDING, marker)
    pin = retained._pin_path(pins, recovery.DATASET, fixed.manifest_sha256)
    pin.parent.mkdir(parents=True)
    packed.write_packed_pin(pin, fixed, exclusive=True)
    heads = {p: p.read_bytes() for p in (cold/'heads'/recovery.DATASET).glob('*.json')}
    for item in [fixed.manifest['archive']['inventory'], *fixed.manifest['archive']['objects']]:
        (cold/item['relpath']).unlink()
    fixed.manifest_path.unlink()
    return source, cold, pins, scratch, materialized, fixed, pin, heads


def test_exact_fred_pin_is_restored_without_changing_head(tmp_path, monkeypatch):
    source, cold, pins, scratch, _materialized, fixed, pin, heads = fixture(tmp_path, monkeypatch)
    result = recovery.recover(source, cold, pins, scratch)
    assert result['state'] == 'exact_pin_restored' and result['exact_pin_resolved']
    assert result['source_deleted'] is False and result['independent_recovery_verified'] is False
    assert packed.resolve_packed_pin(cold, pin).manifest_sha256 == fixed.manifest_sha256
    assert all(path.read_bytes() == original for path, original in heads.items())
    assert recovery.recover(source, cold, pins, scratch)['state'] == 'already_available'
    assert not list(scratch.glob('candidate-*'))


def test_changed_materialization_cannot_install_an_approximation(tmp_path, monkeypatch):
    source, cold, pins, scratch, materialized, fixed, _pin, _heads = fixture(tmp_path, monkeypatch)
    path = materialized/'raw/original.json.gz'
    path.write_bytes(b'X'*len(path.read_bytes()))  # Same namespace and size; different bytes.
    with pytest.raises(SnapshotError, match='every original encoded SHA'):
        recovery.recover(source, cold, pins, scratch)
    assert not fixed.manifest_path.exists()
    assert not list(scratch.glob('candidate-*'))


def test_interrupted_exact_object_installation_resumes(tmp_path, monkeypatch):
    source, cold, pins, scratch, _materialized, fixed, pin, _heads = fixture(tmp_path, monkeypatch)
    real_install = packed._install_immutable_object
    calls = 0
    def interrupt_after_two(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError('injected interruption')
        return real_install(*args, **kwargs)
    monkeypatch.setattr(packed, '_install_immutable_object', interrupt_after_two)
    with pytest.raises(OSError, match='injected'):
        recovery.recover(source, cold, pins, scratch)
    assert not fixed.manifest_path.exists()
    monkeypatch.setattr(packed, '_install_immutable_object', real_install)
    assert recovery.recover(source, cold, pins, scratch)['state'] == 'exact_pin_restored'
    assert packed.resolve_packed_pin(cold, pin).manifest_sha256 == fixed.manifest_sha256


def test_retained_pin_protects_exact_version_even_before_recovery(tmp_path, monkeypatch):
    source, cold, pins, _scratch, _materialized, fixed, pin, _heads = fixture(tmp_path, monkeypatch)
    options = {'source_root': source, 'pin_root': pins, 'expected_cold': cold}
    expected = {fixed.manifest['snapshot_id']: fixed.manifest_sha256}
    assert not fixed.manifest_path.exists()
    protect_pin = retirement._retained_source_pin_protections
    assert protect_pin(cold, recovery.DATASET, **options) == expected
    marker = source/retained.BINDING
    marker_raw = marker.read_bytes()
    marker.unlink()  # A temporary source mount failure must not unprotect a retained pin.
    assert protect_pin(cold, recovery.DATASET, **options) == expected
    marker.write_bytes(marker_raw)
    monkeypatch.setattr(retirement, '_retained_source_pin_protections', lambda _cold, _dataset: expected)
    assert retirement._source_link_protections(cold, {}, recovery.DATASET) == expected
    pin_value = json.loads(pin.read_text())
    pin_value['manifest_sha256'] = '0' * 64
    atomic_write_json(pin, pin_value)
    with pytest.raises(SnapshotError, match='protection pin differs'):
        protect_pin(cold, recovery.DATASET, **options)
