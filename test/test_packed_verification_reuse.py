import hashlib
import json
from pathlib import Path
import uuid

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import packed_verification as worker
from stockagent.data_sync import packed_verification_reuse as reuse


@pytest.fixture
def completed(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    for name, content in {'empty': b'', 'raw': b'-1,NaN,1.50\n', '數值': b'original bytes'}.items():
        (source / name).write_bytes(content)
    cold = tmp_path / 'cold'
    selected = packed.publish_packed_snapshot(cold, 'example', source, pack_buckets=2)
    parent = tmp_path / 'evidence'
    options = {'evidence_directory': str(parent / uuid.uuid4().hex),
               'local_workers': 1, 'remote_workers': 1, 'policy_sha256': None}
    def unavailable(*_args):
        raise OSError('no remote needed by fixture')
    monkeypatch.setattr(worker, 'RemotePacks', unavailable)
    original = packed.verify_packed_snapshot(cold, selected, reconstruct_all=True, pack_verification=options)
    new_options = {**options, 'completed_evidence': options['evidence_directory'],
                   'evidence_directory': str(parent / uuid.uuid4().hex)}
    return cold, selected, options, new_options, original


def test_complete_exact_pack_proofs_resume_only_after_fresh_all_object_sha(completed, monkeypatch):
    cold, selected, old_options, new_options, original = completed
    seen, digest = [], packed.sha256_file
    def checked(path):
        if path.suffix == '.zip':
            seen.append(path)
        return digest(path)
    monkeypatch.setattr(packed, 'sha256_file', checked)
    monkeypatch.setattr(worker, 'inspect_pack', lambda *_: pytest.fail('completed originals decoded again'))
    restored = packed.verify_packed_snapshot(cold, selected, reconstruct_all=True, pack_verification=new_options)
    execution = restored['pack_reconstruction_execution']
    assert execution['jobs'] == len(seen) == original['pack_reconstruction_execution']['jobs']
    assert execution['fresh_full_encoded_sha_objects'] == len(seen)
    assert execution['original_ordered_sha_and_all_crc_reused'] is True
    assert execution['original_nodes'] == original['pack_reconstruction_execution']['nodes']
    assert execution['nodes'] == {'retained': len(seen)}
    assert restored['independently_reconstructed_files'] == 3
    assert restored['independently_reconstructed_bytes'] == original['independently_reconstructed_bytes']
    assert reuse.find_completed_pack_evidence(Path(old_options['evidence_directory']).parent,
        selected, new_options) == Path(old_options['evidence_directory'])


@pytest.mark.parametrize('defect', ['manifest', 'worker', 'commitment', 'crc', 'original-size',
    'acceptance', 'source-retirement', 'missing', 'unexpected', 'symlink', 'writable', 'adapter', 'nodes'])
def test_incomplete_changed_or_unsafe_receipts_never_reuse(completed, defect):
    cold, selected, _, options, _ = completed
    original = Path(options['completed_evidence'])
    item = next(item for item in selected.manifest['archive']['objects'] if item['kind'] == 'pack')
    path = original / (item['sha256'] + '.json')
    document = json.loads(path.read_text())
    keys = {'manifest': 'manifest_sha256', 'worker': 'worker_sha256',
            'commitment': 'originals_sha256', 'crc': 'stored_members_crc_verified',
            'original-size': 'logical_bytes', 'acceptance': 'acceptance',
            'source-retirement': 'source_retirement'}
    if defect in keys:
        document[keys[defect]] = '0' * 64 if defect in {'manifest', 'worker', 'commitment'} else -1
        path.write_text(json.dumps(document))
    elif defect == 'missing':
        path.unlink()
    elif defect == 'unexpected':
        (original / 'unknown').write_text('retain unknown evidence')
    elif defect == 'symlink':
        other = original.parent / 'redirected'
        path.rename(other)
        path.symlink_to(other)
    elif defect == 'writable':
        path.chmod(0o666)
    else:
        path = original / 'summary.json'
        document = json.loads(path.read_text())
        if defect == 'adapter':
            document['source_read_execution']['adapter_sha256'] = '0' * 64
        else:
            document['nodes'] = {'local': 0, 'vast': 0}
        path.write_text(json.dumps(document))
    with pytest.raises((ValueError, OSError), match='cohort|evidence|proof|totals|loop'):
        packed.verify_packed_snapshot(cold, selected, reconstruct_all=True, pack_verification=options)
    assert not Path(options['evidence_directory']).exists()


@pytest.mark.parametrize('change', ['bytes', 'generation', 'receipt', 'summary'])
def test_fresh_full_sha_and_all_generation_fences_are_required(completed, monkeypatch, change):
    cold, selected, _, options, _ = completed
    item = next(item for item in selected.manifest['archive']['objects'] if item['kind'] == 'pack')
    path = cold / item['relpath']
    if change == 'bytes':
        body = path.read_bytes()
        path.write_bytes(b'X' + body[1:])
    else:
        native = packed.sha256_file
        changed = False
        def digest(candidate):
            nonlocal changed
            value = native(candidate)
            if candidate == path and not changed:
                changed = True
                if change == 'generation':
                    candidate.write_bytes(candidate.read_bytes())
                else:
                    evidence = Path(options['completed_evidence'])
                    target = evidence / ('summary.json' if change == 'summary' else item['sha256'] + '.json')
                    target.write_text(target.read_text() + '\n')
            return value
        monkeypatch.setattr(packed, 'sha256_file', digest)
    with pytest.raises(ValueError, match='fresh complete pack SHA|generation or evidence'):
        packed.verify_packed_snapshot(cold, selected, reconstruct_all=True, pack_verification=options)
    assert not Path(options['evidence_directory']).exists()


def test_native_resume_hashes_all_exact_pack_members_in_bounded_batches(completed, monkeypatch):
    from stockagent.data_sync import windows_cold_io
    cold, selected, _, options, _ = completed
    calls = []
    def full_hash(paths):
        calls.extend(paths)
        return {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    monkeypatch.setattr(windows_cold_io, 'hash_many', full_hash)
    with packed._open_inventory(cold, selected.manifest) as inventory:
        proofs, summary = reuse.reuse_pack_jobs(cold, selected, inventory, options, native=True)
    assert len(calls) == len(proofs) == summary['fresh_full_encoded_sha_objects']


def test_pack_larger_than_native_member_limit_keeps_full_streaming_sha(completed, monkeypatch):
    from stockagent.data_sync import windows_cold_io
    cold, selected, _, options, _ = completed
    monkeypatch.setattr(packed, 'MAX_NATIVE_HASH_MEMBER_BYTES', 1)
    monkeypatch.setattr(windows_cold_io, 'hash_many', lambda *_: pytest.fail('oversized native request'))
    with packed._open_inventory(cold, selected.manifest) as inventory:
        proofs, summary = reuse.reuse_pack_jobs(cold, selected, inventory, options, native=True)
    assert summary['fresh_full_encoded_sha_objects'] == len(proofs) > 0


def test_changed_policy_during_fresh_read_never_reuses(completed, monkeypatch):
    cold, selected, _, options, _ = completed
    policy = Path(options['evidence_directory']).parent / 'policy.json'
    policy.write_text('changed after original verification')
    options = {**options, 'policy_path': str(policy)}
    with packed._open_inventory(cold, selected.manifest) as inventory:
        with pytest.raises(ValueError, match='policy changed'):
            reuse.reuse_pack_jobs(cold, selected, inventory, options, native=False)
    assert not Path(options['evidence_directory']).exists()


def test_completed_pack_counts_do_not_reset_at_remaining_blob_phase(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'small').write_bytes(b'-1')
    (source / 'large').write_bytes(b'unique original observation' * 10)
    cold = tmp_path / 'cold'
    selected = packed.publish_packed_snapshot(cold, 'mixed', source, loose_file_threshold_bytes=8)
    def unavailable(*_args):
        raise OSError('offline fixture')
    monkeypatch.setattr(worker, 'RemotePacks', unavailable)
    events = []
    options = {'evidence_directory': str(tmp_path / 'evidence'), 'local_workers': 1, 'remote_workers': 1}
    result = packed.verify_packed_snapshot(cold, selected, reconstruct_all=True,
                                          pack_verification=options, progress=events.append)
    last_pack = max(index for index, event in enumerate(events) if event['stage'] == 'parallel-pack-reconstruction')
    verified = events[last_pack]['verified_objects']
    assert verified > 0
    after = [event for event in events[last_pack + 1:] if event['stage'] == 'verify-cold-objects']
    assert after and all(event['completed_items'] >= verified for event in after)
    assert result['independently_reconstructed_files'] == 2
