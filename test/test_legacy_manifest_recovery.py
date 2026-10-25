"""Resume metadata-only captures without recoding or weakening byte proofs."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from types import SimpleNamespace
import time

import pytest

from stockagent.data_sync import legacy_artifact_archive as archive
from stockagent.data_sync.desync_snapshots import SnapshotError


def capture(tmp_path):
    root = tmp_path / 'repo/artifacts'
    source = root / 'markets/old-research'
    (source / 'empty').mkdir(parents=True)
    for name in ('a.csv', 'b.pt'):
        path = source / name
        path.write_bytes((name.encode() + b' original bytes\n') * 1024)
        old = time.time_ns() - 24 * 3600 * 10**9
        os.utime(path, ns=(old, old))
    spec = archive.LegacyArchiveSpec('legacy-manifest-recovery', 'markets/old-research',
        7, tmp_path / 'stage', 12, False, archive.ZSTD_COMPRESSION_PROFILE,
        archive.MANUAL_WSL_CAPTURE_CONTRACT)
    expected = archive.prepare_archive(spec, root, manual_capture=True)
    stage = spec.stage_root / spec.dataset
    target = stage / 'archive/legacy_archive_manifest.json'
    original_bytes = target.read_bytes()
    target.unlink()
    return root, source, spec, expected, original_bytes, stage, target


def test_complete_interrupted_capture_recovers_identical_bytes_without_reencoding(tmp_path, monkeypatch):
    root, source, spec, expected, original_bytes, stage, target = capture(tmp_path)
    before = {str(p.relative_to(stage)): (p.stat().st_ino, p.read_bytes())
              for p in stage.rglob('*') if p.is_file()}
    monkeypatch.setattr(archive, '_encode_one', lambda *a, **k: pytest.fail('reencoded a complete payload'))
    monkeypatch.setattr(archive, 'publish_packed_snapshot', lambda *a, **k: pytest.fail('published during recovery'))
    recovered = archive.recover_complete_archive_manifest(spec, root, manual_capture=True)
    assert recovered == expected and target.read_bytes() == original_bytes
    assert archive.verify_archive_directory(target.parent, source, spec=spec,
        manual_capture=True, artifact_root=root)['files'] == 2
    assert 'empty' in [r['path'] for r in recovered['directories']]
    assert all((stage / name).stat().st_ino == inode and (stage / name).read_bytes() == raw
               for name, (inode, raw) in before.items())


@pytest.mark.parametrize('drift', ['uniform', 'partial', 'inode', 'boolean'])
def test_recovery_preserves_real_observations_and_admits_only_uniform_device_change(tmp_path, monkeypatch, drift):
    root, source, spec, expected, original_bytes, stage, target = capture(tmp_path)
    original = archive._source_signature
    first_inode = (source / 'a.csv').stat().st_ino
    def observed(info):
        value = original(info)
        if drift == 'uniform' or info.st_ino == first_inode:
            if drift == 'inode': value['inode'] += 1
            elif drift == 'boolean': value['device'] = True
            else: value['device'] += 77
        return value
    monkeypatch.setattr(archive, '_source_signature', observed)
    if drift == 'uniform':
        assert archive.recover_complete_archive_manifest(spec, root, manual_capture=True) == expected
        assert target.read_bytes() == original_bytes
    else:
        with pytest.raises(SnapshotError):
            archive.recover_complete_archive_manifest(spec, root, manual_capture=True)
        assert not target.exists()


@pytest.mark.parametrize('missing', ['plan', 'receipt', 'payload'])
def test_incomplete_stage_stays_incomplete_without_manifest_or_reencoding(tmp_path, monkeypatch, missing):
    root, source, spec, expected, original_bytes, stage, target = capture(tmp_path)
    row = expected['files'][0]
    candidate = (stage / 'source_plan.json' if missing == 'plan' else
        stage / 'receipts' / (hashlib.sha256(row['path'].encode()).hexdigest() + '.json')
        if missing == 'receipt' else stage / 'archive' / row['encoded_path'])
    candidate.unlink()
    monkeypatch.setattr(archive, '_encode_one', lambda *a, **k: pytest.fail('unexpected payload write'))
    assert archive.recover_complete_archive_manifest(spec, root, manual_capture=True) is None
    assert not target.exists()


@pytest.mark.parametrize('damage', ['encoding', 'source', 'receipt', 'codec', 'extra', 'symlink', 'hardlink', 'space', 'consumer'])
def test_corrupt_unknown_or_live_stage_never_gets_a_manifest(tmp_path, monkeypatch, damage):
    root, source, spec, expected, original_bytes, stage, target = capture(tmp_path)
    row = expected['files'][0]
    encoded = stage / 'archive' / row['encoded_path']
    receipt = stage / 'receipts' / (hashlib.sha256(row['path'].encode()).hexdigest() + '.json')
    if damage == 'encoding': encoded.write_bytes(b'damaged stored bytes')
    elif damage == 'source': (source / row['path']).write_bytes(b'changed source')
    elif damage == 'receipt':
        broken = deepcopy(row); broken['original_sha256'] = '0' * 64
        receipt.write_text(json.dumps(broken))
    elif damage == 'codec':
        broken = deepcopy(row); broken['codec'] = []
        receipt.write_text(json.dumps(broken))
    elif damage == 'extra': (stage / 'unknown').write_bytes(b'keep this evidence')
    elif damage == 'symlink': (stage / 'redirect').symlink_to(source, target_is_directory=True)
    elif damage == 'hardlink': os.link(encoded, tmp_path / 'external-encoding-alias')
    elif damage == 'space': monkeypatch.setattr(archive.shutil, 'disk_usage', lambda p: SimpleNamespace(free=1))
    elif damage == 'consumer': monkeypatch.setattr(archive, 'artifact_process_references', lambda *a: ['pid=123:current-reader'])
    with pytest.raises(SnapshotError):
        archive.recover_complete_archive_manifest(spec, root, manual_capture=True)
    assert not target.exists() and encoded.exists() and receipt.exists()


def test_recovery_catches_an_earlier_source_mutating_during_a_later_decode(tmp_path, monkeypatch):
    root, source, spec, expected, original_bytes, stage, target = capture(tmp_path)
    real = archive._encoded_original_hashes
    calls = []
    def decode(path, codec):
        result = real(path, codec); calls.append(path)
        if len(calls) == 2:
            first = source / 'a.csv'
            before = first.stat()
            first.write_bytes(b'mutated after its byte check')
            os.utime(first, ns=(before.st_atime_ns, before.st_mtime_ns))
        return result
    monkeypatch.setattr(archive, '_encoded_original_hashes', decode)
    with pytest.raises(SnapshotError, match='changed during'):
        archive.recover_complete_archive_manifest(spec, root, manual_capture=True)
    assert not target.exists()


def test_existing_manifest_is_never_overwritten(tmp_path):
    root, source, spec, expected, original_bytes, stage, target = capture(tmp_path)
    target.write_bytes(b'preserve existing manifest')
    with pytest.raises(SnapshotError, match='already exists'):
        archive.recover_complete_archive_manifest(spec, root, manual_capture=True)
    assert target.read_bytes() == b'preserve existing manifest'


@pytest.mark.parametrize('invalid', ['manual', 'compression'])
def test_recovery_keeps_the_original_capture_admission(tmp_path, invalid):
    root, source, spec, expected, original_bytes, stage, target = capture(tmp_path)
    if invalid == 'compression':
        spec = replace(spec, compression_profile='unknown')
    with pytest.raises(SnapshotError, match='manual capture|compression profile'):
        archive.recover_complete_archive_manifest(spec, root, manual_capture=invalid != 'manual')
    assert not target.exists()
