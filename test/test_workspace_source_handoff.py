from __future__ import annotations

from contextlib import ExitStack, contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from scripts import run_workspace_source_staging as queue
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def synthetic(tmp_path, monkeypatch):
    repo = tmp_path / 'repo'
    repo.mkdir()
    source = repo / 'data_crypto_etf'
    source.mkdir()
    (source / '.download.lock').write_bytes(b'')
    (source / 'download_summary.json').write_text('{"v":1}')
    (source / 'members').mkdir()
    (source / 'members' / 'one').write_bytes(b'first generation')
    workspace = tmp_path / 'workspace'
    workspace.mkdir()
    destination = workspace / 'data_crypto_etf'
    shutil.copytree(source, destination)
    evidence = workspace / 'evidence'
    evidence.mkdir()
    (evidence / 'copy-result.json').write_text('{"state":"verified"}')
    report_path = workspace / 'report.json'
    report_path.write_text('{"scope":"test"}')
    rows = list(relocation.walk(source))
    files = [row for row in rows if row[1] == 'file']
    logical = sum(row[2][3] for row in files)
    intent = {'contract': 'workspace_exact_relocation_v1', 'source': str(source),
              'destination': str(destination), 'source_root_signature': relocation.signature(source),
              'entries': len(rows)}
    (evidence / 'intent.json').write_text(json.dumps(intent))
    with sqlite3.connect(evidence / 'files.sqlite') as conn:
        conn.execute('CREATE TABLE files (path TEXT PRIMARY KEY,kind TEXT,sig TEXT,sha TEXT,bytes INTEGER,state TEXT)')
        for name, kind, signature in rows:
            sha = relocation.digest(source / name) if kind == 'file' else None
            conn.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                         (name, kind, json.dumps(signature), sha,
                          signature[3] if kind == 'file' else 0,
                          'verified' if kind == 'file' else 'pending'))
    report = {'files': len(files), 'logical_bytes': logical,
              'relocation_evidence_sha256': relocation.digest(evidence / 'copy-result.json'),
              'source_marker_sha256': relocation.digest(source / 'download_summary.json')}
    item = {'source': 'data_crypto_etf'}
    monkeypatch.setattr(handoff, 'ROOT', repo)
    monkeypatch.setattr(handoff, 'SOURCE', source)
    monkeypatch.setattr(handoff, 'BACKUP', repo / '.data_crypto_etf.pre-e-20261104-v1')
    monkeypatch.setattr(handoff, 'CONTROL', tmp_path / 'control')
    monkeypatch.setattr(handoff, 'JOURNAL', tmp_path / 'control' / 'crypto-etf.json')
    monkeypatch.setattr(handoff, 'enrolled', lambda: (item, workspace, destination, evidence, report_path))
    monkeypatch.setattr(queue, 'paths', lambda _item, _workspace: (source, destination, evidence, report_path))
    monkeypatch.setattr(queue, 'verified_report', lambda _item, _workspace: report)
    monkeypatch.setattr(handoff.volume, 'check', lambda *a, **k: {'state': 'mounted'})
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *a: False)
    return {'repo': repo, 'source': source, 'destination': destination, 'workspace': workspace,
            'evidence': evidence, 'report': report, 'item': item}


def test_full_sha_and_exact_namespace_are_rechecked(synthetic):
    proof = handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])
    assert proof['files'] == 3
    (synthetic['destination'] / 'members' / 'one').write_bytes(b'changed')
    with pytest.raises(SnapshotError):
        handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])


def test_lock_hardlink_recovery_rejects_unrelated_generation(synthetic):
    handoff.BACKUP.mkdir()
    (handoff.BACKUP / '.download.lock').hardlink_to(handoff.SOURCE / '.download.lock')
    signature = relocation.signature(handoff.SOURCE / '.download.lock')
    assert handoff.verify_current_copy(synthetic['item'], synthetic['workspace'], lock_signature=signature)['files'] == 3
    signature[8] += 1
    with pytest.raises(SnapshotError, match='lock generation'):
        handoff.verify_current_copy(synthetic['item'], synthetic['workspace'], lock_signature=signature)


def test_streaming_large_copy_preserves_the_original_proof(synthetic, monkeypatch):
    expected = handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    assert handoff.verify_current_copy(synthetic['item'], synthetic['workspace']) == expected


@pytest.fixture
def futures_scoped_copy(synthetic, monkeypatch, request):
    source, target = synthetic['source'], synthetic['destination']
    link_value = getattr(request, 'param', 'one')
    (source/'members'/'alias').symlink_to(link_value)
    shutil.rmtree(target)
    shutil.copytree(source, target, symlinks=True)
    rows = list(relocation.walk(source, preserve_symlinks=True))
    files = [row for row in rows if row[1] == 'file']
    evidence = synthetic['evidence']
    intent = {'contract': 'workspace_scoped_relocation_v1', 'source': str(source),
              'destination': str(target), 'source_root_signature': relocation.signature(source),
              'entries': len(rows), 'preserve_symlinks': True, 'excluded_subtrees': []}
    (evidence/'intent.json').write_text(json.dumps(intent))
    with sqlite3.connect(evidence/'files.sqlite') as db:
        db.execute('DELETE FROM files')
        for name, kind, sig in rows:
            sha = (relocation.digest(source/name) if kind == 'file' else
                   hashlib.sha256(os.fsencode(os.readlink(source/name))).hexdigest()
                   if kind == 'symlink' else None)
            db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                       (name, kind, json.dumps(sig), sha, sig[3] if kind == 'file' else 0,
                        'verified' if kind in {'file', 'symlink'} else 'pending'))
    copy = {'contract': 'workspace_scoped_relocation_v1', 'state': 'exact_destination_readback_verified',
            'selected_source_sha256_and_destination_readback_verified': True,
            'all_source_sha256_and_destination_readback_verified': True,
            'excluded_subtrees': [], 'source_namespace_unchanged': True, 'source_files_deleted': 0,
            'files': len(files), 'logical_bytes': sum(row[2][3] for row in files),
            'observed_at_epoch': time.time()}
    (evidence/'copy-result.json').write_text(json.dumps(copy))
    synthetic['report'].update(files=copy['files'], logical_bytes=copy['logical_bytes'],
        symlink_names_preserved=1, relocation_evidence_sha256=relocation.digest(evidence/'copy-result.json'))
    monkeypatch.setattr(handoff, '_source_work_window_guard', lambda: lambda: None)
    return synthetic


def test_futures_prior_copy_checks_internal_symlink_without_rehashing_payloads(futures_scoped_copy, monkeypatch):
    real_digest = relocation.digest
    def no_payload_hash(path, *args, **kwargs):
        path = Path(path)
        if path.is_relative_to(futures_scoped_copy['source']) or path.is_relative_to(futures_scoped_copy['destination']):
            raise AssertionError('unexpected new payload readback')
        return real_digest(path, *args, **kwargs)
    monkeypatch.setattr(relocation, 'digest', no_payload_hash)
    monkeypatch.setattr(relocation, '_bounded_as_ready',
                        lambda *_a, **_kw: (_ for _ in ()).throw(AssertionError('redundant file pass')))
    with handoff.selected_source(handoff.FUTURES_NAME):
        proof = handoff.verify_current_copy(futures_scoped_copy['item'], futures_scoped_copy['workspace'])
    assert proof['new_payload_readback'] is False
    assert proof['internal_symlink_closure'] == {'links': 1, 'internal_targets_verified': 1, 'unresolved_targets': 0}


@pytest.mark.parametrize('futures_scoped_copy', ['../../outside'], indirect=True)
def test_futures_prior_copy_rejects_unresolved_symlink(futures_scoped_copy):
    with handoff.selected_source(handoff.FUTURES_NAME), pytest.raises(SnapshotError, match='unresolved'):
        handoff.verify_current_copy(futures_scoped_copy['item'], futures_scoped_copy['workspace'])


def test_futures_prior_copy_rejects_changed_staged_symlink(futures_scoped_copy):
    link = futures_scoped_copy['destination']/'members'/'alias'
    link.unlink()
    link.symlink_to('../download_summary.json')
    with handoff.selected_source(handoff.FUTURES_NAME), pytest.raises(SnapshotError):
        handoff.verify_current_copy(futures_scoped_copy['item'], futures_scoped_copy['workspace'])


def test_futures_prior_copy_rejects_member_mutation_between_namespace_passes(futures_scoped_copy, monkeypatch):
    destination = futures_scoped_copy['destination']
    original_walk = relocation.walk
    destination_passes = 0
    def changed_between_passes(root, **kwargs):
        nonlocal destination_passes
        yield from original_walk(root, **kwargs)
        if root == destination:
            destination_passes += 1
            if destination_passes == 1:
                member = root/'members'/'one'
                before = member.stat()
                member.write_bytes(member.read_bytes())
                os.utime(member, ns=(before.st_atime_ns, before.st_mtime_ns))
    monkeypatch.setattr(relocation, 'walk', changed_between_passes)
    with handoff.selected_source(handoff.FUTURES_NAME), pytest.raises(SnapshotError, match='generation changed'):
        handoff.verify_current_copy(futures_scoped_copy['item'], futures_scoped_copy['workspace'])


@pytest.mark.parametrize('spill', [False, True])
def test_reboot_device_renumber_reuses_only_the_same_fixed_c_source(synthetic, monkeypatch, spill):
    intent_path = synthetic['evidence']/'intent.json'
    intent = json.loads(intent_path.read_text())
    old_device = intent['source_root_signature'][0] + 1000000
    intent['source_root_signature'][0] = old_device
    intent_path.write_text(json.dumps(intent))
    with sqlite3.connect(synthetic['evidence']/'files.sqlite') as db:
        for name, value in db.execute('SELECT path,sig FROM files').fetchall():
            signature = json.loads(value)
            signature[0] = old_device
            db.execute('UPDATE files SET sig=? WHERE path=?', (json.dumps(signature), name))
    monkeypatch.setattr(handoff, '_canonical_c_source',
                        lambda path, dev: path == synthetic['source'] and dev == synthetic['repo'].stat().st_dev)
    if spill:
        monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    assert handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])['files'] == 3
    # A device renumber never relaxes inode/content-generation checks.
    file = synthetic['source']/'members'/'one'
    replacement = synthetic['source']/'members'/'replacement'
    shutil.copy2(file, replacement)
    os.replace(replacement, file)
    with pytest.raises(SnapshotError, match='source generation changed|source or E member metadata differs'):
        handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])


def test_source_device_rebind_does_not_admit_an_unrelated_root(synthetic, monkeypatch):
    source = synthetic['source']
    monkeypatch.setattr(handoff, '_canonical_c_source',
                        lambda path, dev: path == source and dev == synthetic['repo'].stat().st_dev)
    current = relocation.signature(source)
    recorded = [current[0] + 1000000, *current[1:]]
    assert handoff._source_device_rebind(source, recorded, current) is not None
    assert handoff._source_device_rebind(synthetic['workspace'], recorded, current) is None
    recorded[1] += 1
    assert handoff._source_device_rebind(source, recorded, current) is None


def test_streaming_large_copy_rejects_changed_or_added_destination(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    (synthetic['destination'] / 'members' / 'one').write_bytes(b'changed')
    with pytest.raises(SnapshotError):
        handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])
    shutil.copy2(synthetic['source'] / 'members' / 'one', synthetic['destination'] / 'members' / 'one')
    (synthetic['destination'] / 'extra').write_bytes(b'not enrolled')
    with pytest.raises(SnapshotError, match='namespace'):
        handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])


def test_streaming_large_copy_hashes_even_when_size_and_mtime_match(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    source = synthetic['source'] / 'members' / 'one'
    target = synthetic['destination'] / 'members' / 'one'
    src = source.stat()
    target.write_bytes(b'x' * src.st_size)
    os.utime(target, ns=(src.st_atime_ns, src.st_mtime_ns))
    with pytest.raises(SnapshotError, match='full SHA'):
        handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])


def test_streaming_copy_rejects_destination_changed_after_its_sha_readback(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    target = synthetic['destination'] / 'members' / 'one'
    bounded = relocation._bounded_as_ready
    def changed_after_hash(*args, **kwargs):
        yield from bounded(*args, **kwargs)
        before = target.stat()
        target.write_bytes(target.read_bytes())
        os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
        assert target.stat().st_ctime_ns != before.st_ctime_ns
    monkeypatch.setattr(relocation, '_bounded_as_ready', changed_after_hash)
    with pytest.raises(SnapshotError, match='generation changed after its full SHA'):
        handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])


def test_streaming_large_copy_rejects_an_index_without_unique_path_key(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    index = synthetic['evidence'] / 'files.sqlite'
    with sqlite3.connect(index) as conn:
        conn.execute('ALTER TABLE files RENAME TO old_files')
        conn.execute('CREATE TABLE files (path TEXT,kind TEXT,sig TEXT,sha TEXT,bytes INTEGER,state TEXT)')
        conn.execute('INSERT INTO files SELECT * FROM old_files')
        conn.execute('DROP TABLE old_files')
    with pytest.raises(SnapshotError, match='unique path key'):
        handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])


def test_streaming_large_copy_rechecks_hardlink_lock_generation(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    handoff.BACKUP.mkdir()
    (handoff.BACKUP / '.download.lock').hardlink_to(handoff.SOURCE / '.download.lock')
    signature = relocation.signature(handoff.SOURCE / '.download.lock')
    assert handoff.verify_current_copy(synthetic['item'], synthetic['workspace'], lock_signature=signature)['files'] == 3
    signature[8] += 1
    with pytest.raises(SnapshotError, match='lock generation'):
        handoff.verify_current_copy(synthetic['item'], synthetic['workspace'], lock_signature=signature)


@pytest.fixture
def late_archive_lock(synthetic, monkeypatch):
    """A completed fixed copy whose index predates the source writer lock."""
    source, destination, evidence = (synthetic[key] for key in ('source', 'destination', 'evidence'))
    (source / '.download.lock').unlink()
    (destination / '.download.lock').unlink()
    intent_path = evidence / 'intent.json'
    intent = json.loads(intent_path.read_text())
    intent['source_root_signature'] = relocation.signature(source)
    intent['entries'] -= 1
    intent_path.write_text(json.dumps(intent))
    with sqlite3.connect(evidence / 'files.sqlite') as conn:
        conn.execute("DELETE FROM files WHERE path='.download.lock'")
    synthetic['report']['files'] -= 1
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.BINANCE_ARCHIVE_NAME))
    monkeypatch.setattr(handoff, 'binance_handoff_window_allowed', lambda **_: True)
    return synthetic


def test_late_archive_lock_preserves_full_copy_verification(late_archive_lock):
    source, destination = (late_archive_lock[key] for key in ('source', 'destination'))
    with ExitStack() as locks:
        handoff._lock_writer_roots(locks, source, destination)
        assert handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])['files'] == 2
        handoff.BACKUP.mkdir()
        (handoff.BACKUP / '.download.lock').hardlink_to(source / '.download.lock')
        lock_sig = relocation.signature(source / '.download.lock')
        assert handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'],
                                           lock_signature=lock_sig)['files'] == 2


def test_small_okx_copy_with_late_root_lock_uses_full_streaming_verifier(late_archive_lock, monkeypatch):
    source, destination = (late_archive_lock[key] for key in ('source', 'destination'))
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.OKX_NAME))
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 100000)
    with ExitStack() as locks:
        for root in (source, destination):
            locks.enter_context(handoff._lock_supplemental_file(root / '.download.lock'))
        assert handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])['files'] == 2
        (destination / 'members' / 'one').write_bytes(b'not the original generation')
        with pytest.raises(SnapshotError):
            handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])


def test_late_archive_lock_does_not_accept_other_added_names(late_archive_lock):
    source, destination = (late_archive_lock[key] for key in ('source', 'destination'))
    with ExitStack() as locks:
        handoff._lock_writer_roots(locks, source, destination)
        (destination / 'unexpected').write_bytes(b'new data')
        with pytest.raises(SnapshotError, match='namespace'):
            handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])


def test_late_archive_lock_still_requires_full_content_hash(late_archive_lock):
    source, destination = (late_archive_lock[key] for key in ('source', 'destination'))
    target = destination / 'members' / 'one'
    with ExitStack() as locks:
        handoff._lock_writer_roots(locks, source, destination)
        before = target.stat()
        target.write_bytes(b'x' * before.st_size)
        os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
        with pytest.raises(SnapshotError, match='full SHA'):
            handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])


def _legacy_fixed_copy(fixture, monkeypatch):
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.LEGACY_PARQUET_NAME))
    report = fixture['report']
    result_path = fixture['evidence']/'copy-result.json'
    result_path.write_text(json.dumps({'contract': 'workspace_exact_relocation_v1',
        'state': 'exact_destination_readback_verified', 'files': report['files'],
        'logical_bytes': report['logical_bytes'], 'observed_at_epoch': time.time(),
        'all_source_sha256_and_destination_readback_verified': True,
        'source_namespace_unchanged': True, 'source_files_deleted': 0}))
    report['relocation_evidence_sha256'] = relocation.digest(result_path)


def test_legacy_handoff_uses_prior_verified_copy_without_new_payload_readback(late_archive_lock, monkeypatch):
    _legacy_fixed_copy(late_archive_lock, monkeypatch)
    source, destination = (late_archive_lock[key] for key in ('source', 'destination'))
    digest = relocation.digest
    def no_payload_read(path, **kwargs):
        assert not Path(path).is_relative_to(source) and not Path(path).is_relative_to(destination)
        return digest(path, **kwargs)
    monkeypatch.setattr(relocation, 'digest', no_payload_read)
    with ExitStack() as locks:
        handoff._lock_writer_roots(locks, source, destination)
        proof = handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])
    assert proof['files'] == 2
    assert proof['handoff_verification_mode'] == 'prior_verified_copy_generation_only'
    assert proof['new_payload_readback'] is False


@pytest.mark.parametrize('name', [handoff.MINUTE_NAME, handoff.MINUTE_RAW_NAME])
def test_minute_handoff_requires_its_exact_source_stream_proof(synthetic, monkeypatch, name):
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=name))
    result_path = synthetic['evidence']/'copy-result.json'
    report = synthetic['report']
    result = {'contract': 'stockagent.source_stream_staging.v1',
              'state': 'source_sha256_streamed_no_destination_readback',
              'source_sha256_streamed': True, 'destination_independent_readback': False,
              'all_source_sha256_and_destination_readback_verified': False,
              'source_namespace_unchanged': True, 'source_files_deleted': 0,
              'files': report['files'], 'logical_bytes': report['logical_bytes'],
              'observed_at_epoch': time.time()}
    result_path.write_text(json.dumps(result))
    report['relocation_evidence_sha256'] = relocation.digest(result_path)
    assert handoff._fixed_copy_completion_ns(synthetic['evidence'], report) > 0
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.HFT_NAME))
    with pytest.raises(SnapshotError, match='cannot support'):
        handoff._fixed_copy_completion_ns(synthetic['evidence'], report)
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=name))
    result['destination_independent_readback'] = True
    result_path.write_text(json.dumps(result))
    report['relocation_evidence_sha256'] = relocation.digest(result_path)
    with pytest.raises(SnapshotError, match='cannot support'):
        handoff._fixed_copy_completion_ns(synthetic['evidence'], report)


def test_raw_minute_handoff_yields_to_the_existing_runner_lock(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.MINUTE_RAW_NAME))
    runner = synthetic['repo']/'artifacts/data_repair/shioaji_minute_full/runner.lock'
    runner.parent.mkdir(parents=True)
    runner.touch()
    with runner.open('r+b') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with ExitStack() as locks, pytest.raises(BlockingIOError):
            handoff._lock_writer_roots(locks, synthetic['source'], synthetic['destination'])
    with ExitStack() as locks:
        handoff._lock_writer_roots(locks, synthetic['source'], synthetic['destination'])
        with runner.open('r+b') as other, pytest.raises(BlockingIOError):
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_news_sidecar_clock_drift_is_bound_to_unchanged_source_bytes(late_archive_lock, monkeypatch):
    _legacy_fixed_copy(late_archive_lock, monkeypatch)
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.NEWS_NAME))
    monkeypatch.setattr(handoff, 'NEWS_SIDECAR_PATHS', frozenset({'members/one'}))
    monkeypatch.setattr(handoff, 'LATE_ROOT_LOCK_SOURCES', frozenset({handoff.NEWS_NAME}))
    monkeypatch.setattr(handoff, 'PRIOR_COPY_GENERATION_SOURCES', frozenset({handoff.NEWS_NAME}))
    source = late_archive_lock['source']
    member = source/'members/one'
    info = member.stat()
    os.utime(member, ns=(info.st_atime_ns, info.st_mtime_ns+1000000))
    with ExitStack() as locks:
        for root in (source, late_archive_lock['destination']):
            locks.enter_context(handoff._lock_supplemental_file(root/'.download.lock'))
        proof = handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])
        assert proof['source_generation_overrides']['members/one'] == relocation.signature(member)
        assert proof['new_payload_readback'] is False
        handoff._same_copy_proof(proof, handoff.verify_current_copy(
            late_archive_lock['item'], late_archive_lock['workspace']))
        info = member.stat()
        member.write_bytes(b'x'*info.st_size)
        os.utime(member, ns=(info.st_atime_ns, info.st_mtime_ns))
        with pytest.raises(SnapshotError, match='source generation changed'):
            handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])


def test_news_override_rejects_an_unreviewed_path(late_archive_lock, monkeypatch):
    _legacy_fixed_copy(late_archive_lock, monkeypatch)
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.NEWS_NAME))
    monkeypatch.setattr(handoff, 'LATE_ROOT_LOCK_SOURCES', frozenset({handoff.NEWS_NAME}))
    monkeypatch.setattr(handoff, 'PRIOR_COPY_GENERATION_SOURCES', frozenset({handoff.NEWS_NAME}))
    member = late_archive_lock['source']/'members/one'
    info = member.stat()
    os.utime(member, ns=(info.st_atime_ns, info.st_mtime_ns+1000000))
    with ExitStack() as locks:
        for root in (late_archive_lock['source'], late_archive_lock['destination']):
            locks.enter_context(handoff._lock_supplemental_file(root/'.download.lock'))
        with pytest.raises(SnapshotError, match='source generation changed'):
            handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])


def test_legacy_handoff_rejects_target_changed_after_fixed_receipt(late_archive_lock, monkeypatch):
    _legacy_fixed_copy(late_archive_lock, monkeypatch)
    source, destination = (late_archive_lock[key] for key in ('source', 'destination'))
    target = destination/'members'/'one'
    time.sleep(0.01)
    before = target.stat()
    target.write_bytes(b'x'*before.st_size)
    os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
    with ExitStack() as locks:
        handoff._lock_writer_roots(locks, source, destination)
        with pytest.raises(SnapshotError, match='generation changed after its fixed copy receipt'):
            handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])


def test_legacy_handoff_rejects_changed_fixed_copy_receipt(late_archive_lock, monkeypatch):
    _legacy_fixed_copy(late_archive_lock, monkeypatch)
    source, destination = (late_archive_lock[key] for key in ('source', 'destination'))
    (late_archive_lock['evidence']/'copy-result.json').write_text('{}')
    with ExitStack() as locks:
        handoff._lock_writer_roots(locks, source, destination)
        with pytest.raises(SnapshotError, match='fixed copy receipt changed'):
            handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])


def test_archive_copy_with_an_originally_indexed_lock_uses_normal_full_verification(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.BINANCE_ARCHIVE_NAME))
    monkeypatch.setattr(handoff, 'binance_handoff_window_allowed', lambda **_: True)
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    with ExitStack() as locks:
        handoff._lock_writer_roots(locks, synthetic['source'], synthetic['destination'])
        assert handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])['files'] == 3


def test_late_archive_lock_change_during_readback_fails_closed(late_archive_lock, monkeypatch):
    source, destination = (late_archive_lock[key] for key in ('source', 'destination'))
    original_digest = relocation.digest
    changed = []
    def digest(path, **kwargs):
        result = original_digest(path, **kwargs)
        if path == source / 'members' / 'one' and not changed:
            (source / '.download.lock').write_bytes(b'changed')
            changed.append(True)
        return result
    monkeypatch.setattr(relocation, 'digest', digest)
    with ExitStack() as locks:
        handoff._lock_writer_roots(locks, source, destination)
        with pytest.raises(SnapshotError, match='lock generation'):
            handoff.verify_current_copy(late_archive_lock['item'], late_archive_lock['workspace'])
    assert changed


def test_late_archive_lock_rejects_nonempty_or_busy_lock(late_archive_lock):
    source, destination = (late_archive_lock[key] for key in ('source', 'destination'))
    (source / '.download.lock').write_bytes(b'not a coordination lock')
    with pytest.raises(SnapshotError, match='empty owned regular file'), ExitStack() as locks:
        handoff._lock_writer_roots(locks, source, destination)
    (source / '.download.lock').write_bytes(b'')
    with handoff._lock_supplemental_file(source / '.download.lock'):
        with pytest.raises(BlockingIOError), ExitStack() as locks:
            handoff._lock_writer_roots(locks, source, destination)


def test_late_archive_lock_recovers_after_exchange_without_losing_source(late_archive_lock, monkeypatch):
    scope = handoff.SourceScope(handoff.BINANCE_ARCHIVE_NAME, late_archive_lock['source'],
        late_archive_lock['repo'] / '.archive-pre-e', handoff.CONTROL / 'archive.json')
    monkeypatch.setattr(handoff, '_scope', lambda: scope)
    monkeypatch.setattr(handoff, 'registered_names', lambda: (handoff.NAME, handoff.BINANCE_ARCHIVE_NAME))
    monkeypatch.setattr(handoff, '_owner_clear', lambda _item: None)
    monkeypatch.setattr(handoff, 'mount_everywhere', lambda _value: [{'verified': True}])
    calls = []
    def interrupt_once(_path):
        calls.append(True)
        if len(calls) == 1:
            raise RuntimeError('interrupted after exchange')
    monkeypatch.setattr(handoff, 'immutable_placeholder', interrupt_once)
    with pytest.raises(RuntimeError, match='after exchange'):
        handoff.handoff()
    assert (scope.backup / 'members' / 'one').read_bytes() == b'first generation'
    assert {p.name for p in scope.source.iterdir()} == {'.download.lock'}
    assert handoff.read_journal()['state'] == 'prepared'
    assert handoff.handoff(boot=True)['state'] == 'active'
    assert (scope.backup / 'members' / 'one').read_bytes() == b'first generation'


def test_streaming_large_copy_rejects_index_change_while_hashing(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    index = synthetic['evidence'] / 'files.sqlite'
    original_digest = relocation.digest
    def digest(path, **kwargs):
        value = original_digest(path, **kwargs)
        if path == synthetic['source'] / 'members' / 'one':
            st = index.stat()
            os.utime(index, ns=(st.st_atime_ns, st.st_mtime_ns + 1000000))
        return value
    monkeypatch.setattr(relocation, 'digest', digest)
    with pytest.raises(SnapshotError, match='index changed'):
        handoff.verify_current_copy(synthetic['item'], synthetic['workspace'])


def test_guard_blocks_incomplete_and_missing_mount(synthetic):
    assert handoff.guard_path(synthetic['source']) is None
    assert handoff.guard_path(synthetic['repo'] / 'other') is None
    value = {'contract': handoff.CONTRACT, 'state': 'preparing', 'source': str(handoff.SOURCE),
             'backup': str(handoff.BACKUP), 'destination': str(synthetic['destination']),
             'evidence': str(synthetic['evidence']), 'workspace': str(synthetic['workspace'])}
    handoff.save(value)
    with pytest.raises(SnapshotError, match='in progress'):
        handoff.guard_path(synthetic['source'])
    value['state'] = 'active'
    handoff.save(value)
    with pytest.raises(SnapshotError, match='mount is missing'):
        handoff.guard_path(synthetic['source'])


def test_collector_waits_before_config_or_provider_calls(synthetic, monkeypatch):
    from downloader import download_crypto_etf_history as collector
    def unavailable(_path):
        raise SnapshotError('mount unavailable')
    monkeypatch.setattr(handoff, 'guard_path', unavailable)
    monkeypatch.setattr('sys.argv', ['collector', '--output-dir', str(synthetic['source']),
                                    '--config', str(synthetic['repo'] / 'does-not-exist')])
    assert collector.main() == 75


def test_collector_rejects_lock_inode_opened_before_handoff(tmp_path):
    from downloader import download_crypto_etf_history as collector
    path = tmp_path / '.download.lock'
    path.write_bytes(b'')
    with path.open('r+b') as old:
        assert collector._lock_still_owns_output(old, tmp_path)
        path.rename(tmp_path / 'retained.lock')
        path.write_bytes(b'')
        assert not collector._lock_still_owns_output(old, tmp_path)
        path.unlink()
        assert not collector._lock_still_owns_output(old, tmp_path)


def test_monitor_distinguishes_binding_from_backup_and_blocks_missing_mount(synthetic, monkeypatch):
    from stockagent.data_sync.storage_progress import workspace_source_handoff_tasks
    value = {'contract': handoff.CONTRACT, 'state': 'active', 'source': str(handoff.SOURCE),
             'backup': str(handoff.BACKUP), 'destination': str(synthetic['destination']),
             'evidence': str(synthetic['evidence']), 'workspace': str(synthetic['workspace']),
             'files': 3, 'logical_bytes': 20, 'checked_at_epoch': 1,
             'last_namespace_readback': [{'verified': True}]}
    handoff.save(value)
    monkeypatch.setattr(handoff, '_assert_retained', lambda v: synthetic['destination'])
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *args: True)
    rows = workspace_source_handoff_tasks(synthetic['repo'])
    row = rows[0]
    assert row['state'] == 'completed' and row['complete_evidence'] is True
    retained = next(item for item in rows if item['kind'] == 'provider_originals')
    assert retained['subject'] == 'source-originals' and retained['state'] == 'waiting'
    assert retained['metrics'] == {'retained_original_roots': 1, 'total_original_roots': 1,
                                   'selected_copy_bytes': 20}
    assert not retained['complete_evidence'] and retained['completed_bytes'] is None
    observation = handoff.JOURNAL.with_suffix('.observation.json')
    observation.write_text(json.dumps({'contract': 'stockagent.workspace_source_handoff_observation.v1',
        'source': handoff.NAME, 'state': 'blocked', 'reason_code': 'namespace_readback_mismatch',
        'observed_at_epoch': 2}))
    rows = workspace_source_handoff_tasks(synthetic['repo'])
    assert rows[0]['state'] == 'blocked' and not rows[0]['complete_evidence']
    assert rows[0]['reason'] == 'namespace_readback_mismatch'
    assert any(item['kind'] == 'provider_originals' for item in rows)  # original is still retained
    observation.write_text(json.dumps({'contract': 'stockagent.workspace_source_handoff_observation.v1',
        'source': handoff.NAME, 'state': 'active', 'reason_code': None, 'observed_at_epoch': 3}))
    assert workspace_source_handoff_tasks(synthetic['repo'])[0]['complete_evidence']
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *args: False)
    rows = workspace_source_handoff_tasks(synthetic['repo'])
    row = rows[0]
    assert row['state'] == 'blocked' and row['complete_evidence'] is False
    assert not any(item['kind'] == 'provider_originals' for item in rows)


def test_monitor_reports_retired_C_payload_after_receipt_is_bound(synthetic, monkeypatch):
    from stockagent.data_sync.storage_progress import workspace_source_handoff_tasks
    value = {'contract': handoff.CONTRACT, 'state': 'active', 'source': str(handoff.SOURCE),
             'backup': str(handoff.BACKUP), 'destination': str(synthetic['destination']),
             'evidence': str(synthetic['evidence']), 'workspace': str(synthetic['workspace']),
             'files': 3, 'logical_bytes': 20, 'checked_at_epoch': 1,
             'original_inode': 101, 'original_lock_inode': 102,
             'last_namespace_readback': [{'verified': True}]}
    handoff.save(value)
    monkeypatch.setattr(handoff, '_assert_retained', lambda _v: synthetic['destination'])
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_args: True)
    root = synthetic['workspace'] / 'migration/handoff-source-repack-v1'
    root.mkdir(parents=True)
    receipt = {'contract': 'stockagent.handoff_source_repack.v1',
               'state': 'C_payload_retired_root_and_lock_preserved', 'C_payload_deleted': True,
               'C_retirement': {'source_root_inode': 101, 'source_lock_inode': 102}}
    (root / 'crypto-etf-history.status.json').write_text(json.dumps(receipt))
    rows = workspace_source_handoff_tasks(synthetic['repo'])
    assert rows[0]['phase'] == 'source_handoff_retired' and rows[0]['complete_evidence']
    assert not any(item['kind'] == 'provider_originals' for item in rows)
    receipt['C_retirement']['source_root_inode'] = 999
    (root / 'crypto-etf-history.status.json').write_text(json.dumps(receipt))
    rows = workspace_source_handoff_tasks(synthetic['repo'])
    assert rows[0]['phase'] == 'provider_workspace_completed'
    assert any(item['kind'] == 'provider_originals' for item in rows)


def test_public_monitor_preserves_over_96_tasks_and_reports_omissions():
    from stockagent.data_sync import storage_progress as monitor
    now = 1793804000
    rows = [monitor.task('workspace_source_staging', str(i), state='waiting') for i in range(140)]
    public = monitor.public_status({'contract': monitor.CONTRACT, 'observed_at_utc': monitor.iso(now),
                                    'tasks': rows, 'omitted_task_count': 3}, now=now)
    assert len(public['tasks']) == 140
    assert public['summary']['omitted_task_count'] == 3


def test_handoff_retains_original_and_recovers_after_exchange(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, '_owner_clear', lambda item: None)
    calls = []
    def interrupt_once(path):
        calls.append(path)
        if len(calls) == 1:
            raise RuntimeError('simulated power loss after exchange')
    monkeypatch.setattr(handoff, 'immutable_placeholder', interrupt_once)
    monkeypatch.setattr(handoff, 'mount_everywhere', lambda value: [{'verified': True}])
    with pytest.raises(RuntimeError, match='power loss'):
        handoff.handoff()
    assert (handoff.BACKUP / 'members' / 'one').read_bytes() == b'first generation'
    assert {p.name for p in handoff.SOURCE.iterdir()} == {'.download.lock'}
    assert handoff.read_journal()['state'] == 'prepared'
    result = handoff.handoff(boot=True)
    assert result == {'state': 'active', 'namespaces': 1, 'c_source_retained': True}
    assert (handoff.BACKUP / 'members' / 'one').read_bytes() == b'first generation'
    assert handoff.read_journal()['state'] == 'active'


def test_uncoordinated_access_during_exchange_rolls_back_before_mount(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, '_owner_clear', lambda _item: None)
    monkeypatch.setattr(handoff, 'immutable_placeholder', lambda _path: pytest.fail('must roll back before mounting'))
    source = synthetic['source']
    value = {'contract':handoff.CONTRACT, 'state':'preparing', 'source':str(source),
             'backup':str(handoff.BACKUP), 'destination':str(synthetic['destination']),
             'evidence':str(synthetic['evidence']), 'workspace':str(synthetic['workspace']),
             'original_inode':source.stat().st_ino, 'original_lock_inode':(source/'.download.lock').stat().st_ino}
    handoff._prepare_placeholder(value)
    class ConflictAfterExchange:
        calls = 0
        def check(self, **_kwargs):
            self.calls += 1
            if self.calls == 2:
                raise BlockingIOError('concurrent access')
    with pytest.raises(BlockingIOError, match='concurrent access'):
        handoff._activate_prepared_locked(synthetic['item'], value, mutation_fence=ConflictAfterExchange())
    assert (source/'members'/'one').read_bytes() == b'first generation'
    assert {path.name for path in handoff.BACKUP.iterdir()} == {'.download.lock'}
    assert handoff.read_journal()['state'] == 'prepared'


def test_real_mutation_fence_rolls_back_reader_that_opens_C_after_exchange(synthetic, monkeypatch):
    from stockagent.data_sync.source_mutation_fence import SourceMutationFence
    monkeypatch.setattr(handoff, '_owner_clear', lambda _item: None)
    monkeypatch.setattr(handoff, 'immutable_placeholder', lambda _path: pytest.fail('must roll back before mounting'))
    source = synthetic['source']
    value = {'contract':handoff.CONTRACT, 'state':'preparing', 'source':str(source),
             'backup':str(handoff.BACKUP), 'destination':str(synthetic['destination']),
             'evidence':str(synthetic['evidence']), 'workspace':str(synthetic['workspace']),
             'original_inode':source.stat().st_ino, 'original_lock_inode':(source/'.download.lock').stat().st_ino}
    real_exchange = handoff.exchange
    exchanges = []
    def open_after_first_exchange(a, b):
        real_exchange(a, b)
        exchanges.append(True)
        if len(exchanges) == 1:
            with (b/'members'/'one').open('rb') as stream:
                assert stream.read(1) == b'f'
    with SourceMutationFence(source, synthetic['destination'], synthetic['evidence']/'files.sqlite') as fence:
        handoff._prepare_placeholder(value)
        monkeypatch.setattr(handoff, 'exchange', open_after_first_exchange)
        with pytest.raises(BlockingIOError, match='namespace changed'):
            handoff._activate_prepared_locked(synthetic['item'], value, mutation_fence=fence)
    assert len(exchanges) == 2 and (source/'members'/'one').read_bytes() == b'first generation'
    assert handoff.read_journal()['state'] == 'prepared'


def test_futures_handoff_shares_original_D_publication_owner(tmp_path, monkeypatch):
    from stockagent.data_sync import cold_primary, packed_snapshots
    cold = tmp_path/'cold'; cold.mkdir()
    monkeypatch.setattr(handoff, 'COLD_PRIMARY', cold)
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda _path: None)
    locks = cold/'.local-state/locks'
    with handoff._futures_publication_owner():
        with pytest.raises(packed_snapshots.PackedPublicationBusy):
            with packed_snapshots._try_publication_lock(locks/'publish-tw-index-futures.lock'):
                pass
    with packed_snapshots._try_publication_lock(locks/'publish-tw-index-futures.lock'):
        with pytest.raises(BlockingIOError, match='publication owner is busy'):
            with handoff._futures_publication_owner():
                pass
        with packed_snapshots._try_publication_lock(locks/'publish-retention-global.lock'):
            pass
    # An unrelated D-only retention job can run while the C/E namespace is
    # fenced; a same-dataset publisher remains excluded in either order.
    with packed_snapshots._try_publication_lock(locks/'publish-retention-global.lock'):
        with handoff._futures_publication_owner():
            with pytest.raises(packed_snapshots.PackedPublicationBusy):
                with packed_snapshots._try_publication_lock(locks/'publish-tw-index-futures.lock'):
                    pass


def test_new_handoff_reuses_one_check_only_while_both_writer_locks_stay_held(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, '_owner_clear', lambda _item: None)
    monkeypatch.setattr(handoff, 'immutable_placeholder', lambda _path: None)
    monkeypatch.setattr(handoff, 'mount_everywhere', lambda _value: [{'verified': True}])
    actual = handoff.verify_current_copy
    calls = []
    def checked(*args, **kwargs):
        calls.append(kwargs.get('progress_pass'))
        for root in (synthetic['source'], synthetic['destination']):
            with pytest.raises(BlockingIOError):
                with handoff.lock_file(root / '.download.lock'):
                    pass
        return actual(*args, **kwargs)
    monkeypatch.setattr(handoff, 'verify_current_copy', checked)
    assert handoff.handoff()['state'] == 'active'
    assert calls == [None]
    assert (handoff.BACKUP / 'members/one').read_bytes() == b'first generation'


def test_handoff_resumes_placeholder_after_interruption(synthetic, monkeypatch):
    monkeypatch.setattr(handoff, '_owner_clear', lambda item: None)
    monkeypatch.setattr(handoff, 'immutable_placeholder', lambda path: None)
    monkeypatch.setattr(handoff, 'mount_everywhere', lambda value: [])
    real_save = handoff.save
    did_fail = []
    def interrupted(value):
        if value['state'] == 'prepared' and not did_fail:
            did_fail.append(True)
            raise RuntimeError('simulated power loss after hardlink')
        return real_save(value)
    monkeypatch.setattr(handoff, 'save', interrupted)
    with pytest.raises(RuntimeError, match='power loss'):
        handoff.handoff()
    assert handoff.read_journal()['state'] == 'preparing'
    assert (handoff.SOURCE / '.download.lock').samefile(handoff.BACKUP / '.download.lock')
    assert handoff.handoff()['state'] == 'active'
    assert (handoff.BACKUP / 'members' / 'one').read_bytes() == b'first generation'


def test_busy_writer_does_not_begin_handoff(synthetic):
    with ExitStack() as locks:
        locks.enter_context(handoff.lock_file(handoff.SOURCE / '.download.lock'))
        with pytest.raises(BlockingIOError):
            handoff.handoff()
    assert not handoff.JOURNAL.exists()
    assert (handoff.SOURCE / 'members' / 'one').read_bytes() == b'first generation'


def test_exchange_is_atomic_and_preserves_both_sides(tmp_path):
    a, b = tmp_path / 'a', tmp_path / 'b'
    a.mkdir(); b.mkdir()
    (a / 'original').write_bytes(b'original')
    (b / 'placeholder').write_bytes(b'placeholder')
    handoff.exchange(a, b)
    assert (a / 'placeholder').read_bytes() == b'placeholder'
    assert (b / 'original').read_bytes() == b'original'


@pytest.fixture
def coin_stage(synthetic, tmp_path, monkeypatch):
    repo, workspace = synthetic['repo'], synthetic['workspace']
    source = repo / 'data_coinmetrics_community'
    shutil.copytree(synthetic['source'], source)
    destination = workspace / 'data_coinmetrics_community'
    shutil.copytree(source, destination)
    evidence = workspace / 'coin-evidence'
    evidence.mkdir()
    (evidence / 'copy-result.json').write_text('{"state":"verified"}')
    report_path = workspace / 'coin-report.json'
    report_path.write_text('{"scope":"test"}')
    rows = list(relocation.walk(source))
    files = [row for row in rows if row[1] == 'file']
    logical = sum(row[2][3] for row in files)
    (evidence / 'intent.json').write_text(json.dumps({
        'contract': 'workspace_exact_relocation_v1', 'source': str(source),
        'destination': str(destination), 'source_root_signature': relocation.signature(source),
        'entries': len(rows)}))
    with sqlite3.connect(evidence / 'files.sqlite') as conn:
        conn.execute('CREATE TABLE files (path TEXT PRIMARY KEY,kind TEXT,sig TEXT,sha TEXT,bytes INTEGER,state TEXT)')
        for name, kind, signature in rows:
            digest = relocation.digest(source / name, no_follow=True) if kind == 'file' else None
            conn.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                         (name, kind, json.dumps(signature), digest, signature[3], 'verified'))
    report = {'files': len(files), 'logical_bytes': logical,
              'relocation_evidence_sha256': hashlib.sha256((evidence/'copy-result.json').read_bytes()).hexdigest(),
              'source_marker_sha256': hashlib.sha256((source/'download_summary.json').read_bytes()).hexdigest()}
    item = {'source': 'data_coinmetrics_community'}
    old_paths, old_report = queue.paths, queue.verified_report
    monkeypatch.setattr(queue, 'paths', lambda i, w: (source, destination, evidence, report_path)
                        if i is item else old_paths(i, w))
    monkeypatch.setattr(queue, 'verified_report', lambda i, w: report if i is item else old_report(i, w))
    old_enrolled = handoff.enrolled
    monkeypatch.setattr(handoff, 'enrolled', lambda: (item, workspace, destination, evidence, report_path)
                        if handoff._scope().name == handoff.COIN_NAME else old_enrolled())
    return {'source': source, 'destination': destination, 'item': item, 'report': report,
            'workspace': workspace, 'repo': repo}


def test_coin_handoff_recovers_interrupted_exchange_and_keeps_original(coin_stage, monkeypatch):
    monkeypatch.setattr(handoff, '_owner_clear', lambda item: None)
    monkeypatch.setattr(handoff, 'mount_everywhere', lambda value: [{'verified': True}])
    calls = []
    def interrupt_once(path):
        calls.append(path)
        if len(calls) == 1:
            raise RuntimeError('power loss after exchange')
    monkeypatch.setattr(handoff, 'immutable_placeholder', interrupt_once)
    with handoff.selected_source(handoff.COIN_NAME):
        scope = handoff._scope()
        with pytest.raises(RuntimeError, match='power loss'):
            handoff.handoff()
        assert (scope.backup / 'members' / 'one').read_bytes() == b'first generation'
        assert handoff.read_journal()['state'] == 'prepared'
        result = handoff.handoff(boot=True)
        assert result['state'] == 'active' and result['c_source_retained'] is True
        assert handoff.read_journal()['source'] == str(coin_stage['source'])
        assert (scope.backup / 'members' / 'one').read_bytes() == b'first generation'
    assert not handoff.JOURNAL.exists()  # ETF's journal is independent.


def test_coin_collector_fails_closed_if_handoff_or_e_mount_is_unavailable(coin_stage, monkeypatch):
    from downloader import download_coinmetrics_community as collector
    monkeypatch.setattr(collector, 'parse_args', lambda: type('Args', (), {
        'output_dir': str(coin_stage['source']), 'lock_timeout_seconds': 0})())
    with handoff.selected_source(handoff.COIN_NAME):
        scope = handoff._scope()
        handoff.save({'contract': handoff.CONTRACT, 'state': 'preparing', 'source': str(scope.source),
            'backup': str(scope.backup), 'destination': str(coin_stage['destination']),
            'evidence': str(coin_stage['workspace'] / 'coin-evidence'), 'workspace': str(coin_stage['workspace'])})
    monkeypatch.setattr(collector, '_run_locked_download', lambda _: pytest.fail('provider work must not start'))
    with pytest.raises(SnapshotError, match='handoff is in progress'):
        collector.main()
    with handoff.selected_source(handoff.COIN_NAME):
        value = handoff.read_journal(); value['state'] = 'active'; handoff.save(value)
    with pytest.raises(SnapshotError, match='mount is missing'):
        collector.main()


def test_coin_recheck_progress_is_per_generation_and_never_acceptance(coin_stage):
    from stockagent.data_sync import storage_progress as monitor
    with handoff.selected_source(handoff.COIN_NAME):
        proof = handoff.verify_current_copy(coin_stage['item'], coin_stage['workspace'], progress_pass='initial')
        path = handoff._scope().journal.with_suffix('.progress.json')
        value = json.loads(path.read_bytes())
        assert value['state'] == 'checked' and value['completed_bytes'] == proof['logical_bytes']
        first_generation = value['generation']
        value.update(state='running', phase='full_sha_recheck', completed_bytes=5, completed_files=1)
        path.write_text(json.dumps(value))
    rows = monitor.workspace_source_handoff_tasks(coin_stage['repo'])
    row = next(item for item in rows if item['subject'] == 'coinmetrics-community')
    assert row['state'] == 'running' and row['complete_evidence'] is False
    assert row['phase'] == 'source_handoff_initial' and row['completed_bytes'] == 5
    started = monitor.timestamp(row['observed_at_utc'])
    first_eta, history = monitor.estimate(row, {}, now=started)
    assert first_eta['state'] == 'sampling'
    updated = {**row, 'completed_bytes': 8, 'completed_items': 2,
               'observed_at_utc': monitor.iso(started + 60)}
    projected, _ = monitor.estimate(updated, history, now=started + 60)
    assert projected['state'] == 'estimated' and projected['completion_at_utc']
    next_pass = {**updated, 'phase': 'source_handoff_preexchange',
                 'generation': monitor.key('different-pass')}
    reset, _ = monitor.estimate(next_pass, history, now=started + 60)
    assert reset['state'] == 'sampling'
    with handoff.selected_source(handoff.COIN_NAME):
        handoff.verify_current_copy(coin_stage['item'], coin_stage['workspace'], progress_pass='preexchange')
        value = json.loads(path.read_bytes())
        assert value['generation'] != first_generation and value['verification_pass'] == 'preexchange'


def test_interrupted_source_recheck_yields_without_claiming_acceptance(coin_stage, monkeypatch):
    checks = 0
    def window_guard():
        nonlocal checks
        checks += 1
        if checks >= 3:
            raise BlockingIOError('tw_day_trade_priority_window')
    monkeypatch.setattr(handoff, '_source_work_window_guard', lambda: window_guard)
    with handoff.selected_source(handoff.COIN_NAME):
        with pytest.raises(BlockingIOError, match='tw_day_trade_priority_window'):
            handoff.verify_current_copy(coin_stage['item'], coin_stage['workspace'], progress_pass='initial')
        progress = json.loads(handoff._scope().journal.with_suffix('.progress.json').read_bytes())
    assert progress['state'] == 'waiting'
    assert not (coin_stage['repo'] / '.data_coinmetrics_community.pre-e-20261105-v1').exists()


def test_recheck_reports_completed_files_before_a_slow_first_file(coin_stage, monkeypatch):
    original_digest = relocation.digest
    original_advance = handoff._RecheckProgress.advance
    slow_started, continue_slow = threading.Event(), threading.Event()
    slow = coin_stage['source'] / '.download.lock'

    def digest(path, *args, **kwargs):
        if path == slow:
            slow_started.set()
            if not continue_slow.wait(5):
                raise AssertionError('completed files were blocked behind a slow file')
        return original_digest(path, *args, **kwargs)

    def advance(progress, size):
        assert slow_started.wait(5)
        continue_slow.set()
        return original_advance(progress, size)

    monkeypatch.setattr(relocation, 'digest', digest)
    monkeypatch.setattr(handoff._RecheckProgress, 'advance', advance)
    with handoff.selected_source(handoff.COIN_NAME):
        proof = handoff.verify_current_copy(coin_stage['item'], coin_stage['workspace'], progress_pass='initial')
        progress = json.loads(handoff._scope().journal.with_suffix('.progress.json').read_bytes())
    assert proof['files'] == progress['completed_files'] == progress['total_files']
    assert proof['logical_bytes'] == progress['completed_bytes'] == progress['total_bytes']


def test_streaming_batches_keep_progress_when_another_batch_is_slow(coin_stage, monkeypatch):
    monkeypatch.setattr(handoff, 'IN_MEMORY_VERIFICATION_ENTRY_LIMIT', 0)
    source, destination = coin_stage['source'], coin_stage['destination']
    evidence = coin_stage['workspace'] / 'coin-evidence'
    with sqlite3.connect(evidence / 'files.sqlite') as db:
        for i in range(40):
            name = f'extra-{i:02d}'
            (source/name).write_bytes(b'x')
            shutil.copy2(source/name, destination/name)
            signature = relocation.signature(source/name)
            db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                (name, 'file', json.dumps(signature), relocation.digest(source/name), 1, 'verified'))
    intent_path = evidence / 'intent.json'
    intent = json.loads(intent_path.read_bytes())
    intent['entries'] += 40
    intent['source_root_signature'] = relocation.signature(source)
    intent_path.write_text(json.dumps(intent))
    coin_stage['report']['files'] += 40
    coin_stage['report']['logical_bytes'] += 40

    original_digest = relocation.digest
    original_advance = handoff._RecheckProgress.advance
    slow_started, release_slow = threading.Event(), threading.Event()
    def digest(path, *args, **kwargs):
        if path == source / '.download.lock':
            slow_started.set()
            if not release_slow.wait(10):
                raise AssertionError('all progress was blocked behind a slow batch')
        return original_digest(path, *args, **kwargs)
    def advance(progress, size):
        assert slow_started.wait(10)
        release_slow.set()
        return original_advance(progress, size)
    monkeypatch.setattr(relocation, 'digest', digest)
    monkeypatch.setattr(handoff._RecheckProgress, 'advance', advance)
    with handoff.selected_source(handoff.COIN_NAME):
        proof = handoff.verify_current_copy(coin_stage['item'], coin_stage['workspace'], progress_pass='initial')
        progress = json.loads(handoff._scope().journal.with_suffix('.progress.json').read_bytes())
    assert proof['files'] == progress['completed_files'] == progress['total_files']
    assert proof['logical_bytes'] == progress['completed_bytes'] == progress['total_bytes']


def test_namespace_scan_is_running_without_an_invented_hash_eta(coin_stage):
    from stockagent.data_sync import storage_progress as monitor
    with handoff.selected_source(handoff.COIN_NAME):
        progress = handoff._RecheckProgress('initial', files=100, logical_bytes=10000)
    rows = monitor.workspace_source_handoff_tasks(coin_stage['repo'])
    row = next(item for item in rows if item['subject'] == 'coinmetrics-community')
    assert row['state'] == 'running' and row['phase'] == 'source_handoff_namespace'
    assert row['completed_bytes'] is None and row['completed_items'] is None
    assert not row['complete_evidence']
    eta, _ = monitor.estimate(row, {}, now=monitor.timestamp(row['observed_at_utc']))
    assert eta['state'] == 'unknown' and eta['completion_at_utc'] is None
    progress.finish('blocked')
    row = next(item for item in monitor.workspace_source_handoff_tasks(coin_stage['repo'])
               if item['subject'] == 'coinmetrics-community')
    assert row['state'] == 'blocked' and not row['complete_evidence']


def test_namespace_progress_is_fresh_and_each_pass_has_its_own_generation(coin_stage):
    from stockagent.data_sync import storage_progress as monitor
    with handoff.selected_source(handoff.COIN_NAME):
        progress = handoff._RecheckProgress('initial', files=100, logical_bytes=10000)
        progress.begin_namespace(120, 1)
        progress.last_write = 0
        progress.advance_namespace(70)
    row = next(item for item in monitor.workspace_source_handoff_tasks(coin_stage['repo'])
               if item['subject'] == 'coinmetrics-community')
    assert row['state'] == 'running' and row['phase'] == 'source_handoff_namespace'
    assert row['completed_items'] == 70 and row['total_items'] == 120
    assert row['completed_bytes'] is None
    assert row['metrics'] == {'namespace_pass': 1, 'namespace_passes_total': 4}
    first_generation = row['generation']
    with handoff.selected_source(handoff.COIN_NAME):
        progress.begin_namespace(120, 2)
    row = next(item for item in monitor.workspace_source_handoff_tasks(coin_stage['repo'])
               if item['subject'] == 'coinmetrics-community')
    assert row['completed_items'] == 0 and row['total_items'] == 120
    assert row['metrics'] == {'namespace_pass': 2, 'namespace_passes_total': 4}
    assert row['generation'] != first_generation


def test_monitor_shows_actual_fence_setup_and_access_wait_reason(coin_stage):
    from scripts.run_workspace_source_handoff import record_outcome
    from stockagent.data_sync import storage_progress as monitor
    with handoff.selected_source(handoff.COIN_NAME):
        progress = handoff._RecheckProgress('initial', files=0, logical_bytes=0)
        progress.advance_fence(32, 100)
        progress.last_write = 0
        progress.advance_fence(64, 100)
    row = next(item for item in monitor.workspace_source_handoff_tasks(coin_stage['repo'])
               if item['subject'] == 'coinmetrics-community')
    assert row['state'] == 'running' and row['phase'] == 'source_handoff_fence_install'
    assert row['completed_items'] == 64 and row['total_items'] == 100
    assert row['completed_bytes'] is None and not row['complete_evidence']
    progress.finish('waiting')
    record_outcome({'source': handoff.COIN_NAME, 'state': 'waiting',
                    'reason': "source or E namespace changed during handoff (name='x', mask=0x20)"})
    row = next(item for item in monitor.workspace_source_handoff_tasks(coin_stage['repo'])
               if item['subject'] == 'coinmetrics-community')
    assert row['state'] == 'waiting' and row['reason'] == 'source_access_during_handoff'


def test_unregistered_source_scope_is_refused_without_side_effects():
    with pytest.raises(ValueError, match='not explicitly enrolled'):
        with handoff.selected_source('other-provider'):
            pytest.fail('unknown source must never be admitted')


def test_yahoo_enrollment_binds_the_reviewed_writer_code(tmp_path, monkeypatch):
    repo, control = tmp_path/'repo', tmp_path/'control'
    repo.mkdir(); control.mkdir()
    (repo/'writer.py').write_text('first')
    monkeypatch.setattr(handoff, 'ROOT', repo)
    monkeypatch.setattr(handoff, 'CONTROL', control)
    monkeypatch.setattr(handoff, 'YAHOO_WRITER_GUARD_FILES', ('writer.py',))
    assert handoff.YAHOO_NAME not in handoff.registered_names()
    marker = control/'yahoo-enrollment.json'
    marker.write_text(json.dumps(handoff.yahoo_enrollment_value()))
    assert handoff.YAHOO_NAME in handoff.registered_names()
    handoff._validate_yahoo_enrollment()
    (repo/'writer.py').write_text('changed')
    with pytest.raises(SnapshotError, match='writer guard differs'):
        handoff._validate_yahoo_enrollment()


def test_one_blocked_handoff_does_not_skip_other_reviewed_sources(tmp_path, monkeypatch, capsys):
    from scripts import run_workspace_source_handoff as runner
    monkeypatch.setattr('sys.argv', ['handoff'])
    monkeypatch.setattr(handoff, 'CONTROL', tmp_path / 'isolated-control')
    monkeypatch.setattr(handoff, 'JOURNAL', tmp_path / 'isolated-control' / 'crypto-etf.json')
    monkeypatch.setattr(runner, 'large_binance_handoff_allowed', lambda: True)
    monkeypatch.setattr(handoff, 'read_journal', lambda: None)
    def execute(*, boot):
        if handoff._scope().name == handoff.NAME:
            raise SnapshotError('ETF blocked for test')
        return {'state': 'active', 'c_source_retained': True}
    monkeypatch.setattr(handoff, 'handoff', execute)
    assert runner.main() == 1
    result = json.loads(capsys.readouterr().out)['sources']
    assert [entry['state'] for entry in result] == ['blocked'] + ['active'] * (len(handoff.registered_names()) - 1)
    with handoff.selected_source(handoff.NAME):
        recorded = json.loads(handoff._scope().journal.with_suffix('.observation.json').read_bytes())
    assert recorded['state'] == 'blocked' and recorded['reason_code'] == 'source_or_copy_recheck_required'


def test_handoff_queue_recovers_missing_mount_then_prioritizes_new_work(monkeypatch):
    from scripts import run_workspace_source_handoff as runner
    names = (handoff.NAME, handoff.COIN_NAME, handoff.FOREX_NAME, handoff.PEPPERSTONE_NAME)
    states = {names[0]: {'state':'active', 'destination':'/e/first'},
              names[1]: {'state':'active', 'destination':'/e/missing'},
              names[2]: {'state':'prepared', 'destination':'/e/prepared'}, names[3]: None}
    monkeypatch.setattr(handoff, 'read_journal', lambda: states[handoff._scope().name])
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount',
                        lambda _source, target: str(target) != '/e/missing')
    assert runner.ordered_sources(names, boot=False) == (names[1], names[2], names[3], names[0])
    assert runner.ordered_sources(names, boot=True) == names


def test_priority_boot_only_restores_the_registered_minute_source(tmp_path, monkeypatch, capsys):
    from scripts import run_workspace_source_handoff as runner
    monkeypatch.setattr('sys.argv', ['handoff', '--boot', '--priority-minute-boot'])
    monkeypatch.setattr(handoff, 'CONTROL', tmp_path / 'control')
    monkeypatch.setattr(handoff, 'JOURNAL', tmp_path / 'control' / 'crypto-etf.json')
    called = []
    monkeypatch.setattr(handoff, 'handoff', lambda *, boot: called.append((handoff._scope().name, boot)) or {'state': 'active'})
    assert runner.main() == 0
    assert not called and json.loads(capsys.readouterr().out)['sources'][0]['state'] == 'not_enrolled'
    handoff.CONTROL.mkdir()
    (handoff.CONTROL/'minute-enrollment.json').write_text('{}')
    assert runner.main() == 0
    assert called == [(handoff.MINUTE_NAME, True)]
    assert json.loads(capsys.readouterr().out)['sources'][0]['state'] == 'active'
    called.clear()
    (handoff.CONTROL/'minute-raw-enrollment.json').write_text('{}')
    assert runner.main() == 0
    assert called == [(handoff.MINUTE_NAME, True)], 'the raw migration must not delay priority trading startup'
    assert len(json.loads(capsys.readouterr().out)['sources']) == 1


def test_direct_collector_lock_is_bound_to_the_current_inode(tmp_path):
    lock = tmp_path / '.download.lock'; lock.write_bytes(b'')
    with lock.open('r+b') as original:
        handoff.guard_locked_path(tmp_path, original)
        lock.rename(tmp_path / 'old.lock')
        lock.write_bytes(b'')
        with pytest.raises(SnapshotError, match='lock inode changed'):
            handoff.guard_locked_path(tmp_path, original)


def test_frankfurter_handoff_waits_for_the_independent_official_writer(tmp_path):
    roots = [tmp_path / 'source', tmp_path / 'destination']
    for root in roots:
        (root / 'official_v2').mkdir(parents=True)
        (root / '.download.lock').write_bytes(b'')
        (root / 'official_v2/.download.lock').write_bytes(b'')
    with (roots[0] / 'official_v2/.download.lock').open('r+b') as active:
        fcntl.flock(active, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with handoff.selected_source(handoff.FOREX_NAME):
            with pytest.raises(BlockingIOError), ExitStack() as held:
                handoff._lock_writer_roots(held, *roots)
        # A failed nested lock must release every earlier root lock.
        with handoff.lock_file(roots[0] / '.download.lock'):
            pass
    with handoff.selected_source(handoff.FOREX_NAME), ExitStack() as held:
        handoff._lock_writer_roots(held, *roots)


def test_news_bodies_handoff_waits_for_the_original_queue_owner(tmp_path, monkeypatch):
    roots = [tmp_path / 'source', tmp_path / 'destination']
    for root in roots:
        root.mkdir()
    monkeypatch.setattr(handoff, 'ROOT', tmp_path)
    owner = tmp_path / 'data_news_fulltext/owner.lock'
    owner.parent.mkdir()
    owner.write_bytes(b'')
    with owner.open('r+b') as active:
        fcntl.flock(active, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with handoff.selected_source(handoff.NEWS_BODIES_NAME):
            with pytest.raises(BlockingIOError), ExitStack() as held:
                handoff._lock_writer_roots(held, *roots)
        with handoff.lock_file(roots[0] / '.download.lock'):
            pass
    with handoff.selected_source(handoff.NEWS_BODIES_NAME), ExitStack() as held:
        handoff._lock_writer_roots(held, *roots)


def test_frankfurter_collectors_wait_before_writing_when_mount_is_unavailable(tmp_path, monkeypatch):
    from downloader import download_forex_frankfurter as legacy, frankfurter_official_history as official
    root = tmp_path / 'missing'
    def unavailable(_path):
        raise SnapshotError('workspace is unavailable')
    monkeypatch.setattr(handoff, 'guard_path', unavailable)
    monkeypatch.setattr(legacy, 'parse_args', lambda: SimpleNamespace(output_dir=root, lock_timeout_seconds=0))
    with pytest.raises(SnapshotError, match='workspace is unavailable'):
        legacy.main()
    with pytest.raises(SnapshotError, match='workspace is unavailable'):
        official.run_official_history(root / 'official_v2')
    assert not root.exists()


def test_frankfurter_official_writer_rejects_a_replaced_lock_before_network(tmp_path, monkeypatch):
    from downloader import frankfurter_official_history as official
    root = tmp_path / 'official_v2'
    root.mkdir()
    original_flock = fcntl.flock
    def swapped(handle, operation):
        original_flock(handle, operation)
        (root / '.download.lock').rename(root / 'old.lock')
        (root / '.download.lock').write_bytes(b'')
    monkeypatch.setattr(official.fcntl, 'flock', swapped)
    with pytest.raises(SnapshotError, match='lock inode changed'):
        official.run_official_history(root)
    assert not (root / 'catalog.json').exists()


@pytest.mark.parametrize('provider', ['historical', 'reference', 'dune', 'catalog'])
def test_reviewed_collectors_stop_before_writing_or_calling_provider_when_guarded(tmp_path, monkeypatch, provider):
    from downloader import (download_crypto_historical_public as historical,
        download_crypto_keyed_context as reference, download_dune_crypto_history as dune,
        download_keyed_public_catalogs as catalog)
    root = tmp_path / 'not-created'
    def unavailable(_path):
        raise SnapshotError('workspace is unavailable')
    monkeypatch.setattr(handoff, 'guard_path', unavailable)
    if provider == 'historical':
        monkeypatch.setattr(historical, 'parse_args', lambda: SimpleNamespace(output_dir=root,
            start_date='2020-01-01', end_date='2020-01-02', sources=['cftc']))
        invoke = historical.main
    elif provider == 'reference':
        monkeypatch.setattr(reference, 'load_env_file', lambda *_a, **_k: None)
        monkeypatch.setattr(reference, 'parse_args', lambda: SimpleNamespace(output_dir=root, env_file=tmp_path/'empty'))
        invoke = reference.main
    elif provider == 'dune':
        monkeypatch.setenv('DUNE_API_KEY', 'synthetic-test-key')
        monkeypatch.setattr(dune, 'load_env_file', lambda *_a, **_k: None)
        monkeypatch.setattr(dune, 'parse_args', lambda: SimpleNamespace(output_dir=root,
            env_file=tmp_path/'empty', config=tmp_path/'config'))
        invoke = dune.main
    else:
        invoke = lambda: catalog.execute(root, [], {}, max_requests=0)
    with pytest.raises(SnapshotError, match='workspace is unavailable'):
        invoke()
    assert not root.exists()


@pytest.mark.parametrize('vanished', [1, 5])
def test_namespace_census_retries_vanished_process_without_accepting_it(synthetic, monkeypatch, vanished):
    from scripts import relocate_provider_workspace as provider
    from stockagent.data_sync import transport_namespace_handoff as transport
    namespace_id = os.stat('/proc/self/ns/mnt').st_ino
    descriptors, calls = [], []
    def namespaces(*, with_roots):
        calls.append(True)
        if len(calls) <= vanished:
            raise ProcessLookupError(3, 'No such process')
        fd = os.open('/proc/self/ns/mnt', os.O_RDONLY)
        root = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
        descriptors.extend((fd, root))
        return {namespace_id: (fd, root)}
    @contextmanager
    def detached(source, _flags):
        fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
        try:
            yield fd
        finally:
            os.close(fd)
    monkeypatch.setattr(provider, 'namespaces', namespaces)
    monkeypatch.setattr(transport, 'prepared_tree', detached)
    monkeypatch.setattr(handoff, '_assert_retained', lambda _value: synthetic['destination'])
    def fake_run(argv, **_kwargs):
        identity = handoff.inode(synthetic['destination'])
        if argv[1:4] == ['-S', '-c', 'import scripts.workspace_source_handoff_namespace as h; h.main()']:
            return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
                'verified': True, 'namespace_inode': namespace_id, 'destination_identity': identity}))
        return SimpleNamespace(returncode=0, stderr='', stdout=':'.join(map(str, identity)))
    monkeypatch.setattr(handoff.subprocess, 'run', fake_run)
    if vanished == 1:
        assert len(handoff.mount_everywhere({'placeholder_inode': 1, 'original_lock_inode': 2})) == 1
    else:
        with pytest.raises(BlockingIOError, match='census did not converge'):
            handoff.mount_everywhere({'placeholder_inode': 1, 'original_lock_inode': 2})
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_namespace_census_permission_error_is_not_treated_as_process_exit(synthetic, monkeypatch):
    from scripts import relocate_provider_workspace as provider
    monkeypatch.setattr(provider, 'namespaces', lambda **_kwargs: (_ for _ in ()).throw(
        PermissionError('namespace access denied')))
    monkeypatch.setattr(handoff, '_assert_retained', lambda _value: synthetic['destination'])
    with pytest.raises(PermissionError, match='access denied'):
        handoff.mount_everywhere({'placeholder_inode': 1, 'original_lock_inode': 2})


@pytest.mark.parametrize('missing_failures', [1, 3])
def test_namespace_missing_attach_is_reobserved_but_never_blindly_accepted(synthetic, monkeypatch, missing_failures):
    from scripts import relocate_provider_workspace as provider
    from stockagent.data_sync import transport_namespace_handoff as transport
    namespace_id = os.stat('/proc/self/ns/mnt').st_ino
    descriptors = []
    attempts = []
    def namespaces(*, with_roots):
        assert with_roots
        fd = os.open('/proc/self/ns/mnt', os.O_RDONLY)
        root = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
        descriptors.extend([fd, root])
        return {namespace_id: (fd, root)}
    @contextmanager
    def detached(source, flags):
        fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
        try:
            yield fd
        finally:
            os.close(fd)
    monkeypatch.setattr(provider, 'namespaces', namespaces)
    monkeypatch.setattr(provider, 'vanished_namespace_after_missing_attach', lambda *_: False)
    monkeypatch.setattr(transport, 'prepared_tree', detached)
    monkeypatch.setattr(handoff, '_assert_retained', lambda value: synthetic['destination'])
    original_run = handoff.subprocess.run
    def fake_run(argv, **kwargs):
        if argv[1:4] == ['-S', '-c', 'import scripts.workspace_source_handoff_namespace as h; h.main()']:
            attempts.append(1)
            if len(attempts) <= missing_failures:
                return SimpleNamespace(returncode=1, stdout='', stderr=
                    f'FileNotFoundError: [Errno 2] provider alias attach failed: {handoff.SOURCE}')
            identity = handoff.inode(synthetic['destination'])
            return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
                'verified': True, 'namespace_inode': namespace_id, 'destination_identity': identity}))
        if argv[1:3] == ['-S', '-c']:
            return SimpleNamespace(returncode=0, stderr='', stdout=':'.join(map(str, handoff.inode(synthetic['destination']))))
        return original_run(argv, **kwargs)
    monkeypatch.setattr(handoff.subprocess, 'run', fake_run)
    value = {'placeholder_inode': 1, 'original_lock_inode': 2}
    if missing_failures == 1:
        assert len(handoff.mount_everywhere(value)) == 1
        assert len(attempts) == 2
    else:
        with pytest.raises(RuntimeError, match='source namespace handoff failed'):
            handoff.mount_everywhere(value)
        assert len(attempts) == 3
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.parametrize('mismatches', [1, 3])
def test_namespace_stat_mismatch_requires_a_fresh_matching_readback(synthetic, monkeypatch, mismatches):
    from scripts import relocate_provider_workspace as provider
    from stockagent.data_sync import transport_namespace_handoff as transport
    namespace_id = os.stat('/proc/self/ns/mnt').st_ino
    descriptors, probes = [], []
    def namespaces(*, with_roots):
        fd = os.open('/proc/self/ns/mnt', os.O_RDONLY)
        root = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
        descriptors.extend((fd, root))
        return {namespace_id: (fd, root)}
    @contextmanager
    def detached(source, flags):
        fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
        try:
            yield fd
        finally:
            os.close(fd)
    monkeypatch.setattr(provider, 'namespaces', namespaces)
    monkeypatch.setattr(transport, 'prepared_tree', detached)
    monkeypatch.setattr(handoff, '_assert_retained', lambda value: synthetic['destination'])
    def fake_run(argv, **kwargs):
        identity = handoff.inode(synthetic['destination'])
        if argv[1:4] == ['-S', '-c', 'import scripts.workspace_source_handoff_namespace as h; h.main()']:
            return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
                'verified': True, 'namespace_inode': namespace_id, 'destination_identity': identity}))
        assert argv[1:3] == ['-S', '-c']  # The independent stat still runs in its own process.
        probes.append(True)
        return SimpleNamespace(returncode=0, stderr='', stdout=(
            '0:0' if len(probes) <= mismatches else ':'.join(map(str, identity))))
    monkeypatch.setattr(handoff.subprocess, 'run', fake_run)
    value = {'placeholder_inode': 1, 'original_lock_inode': 2}
    if mismatches == 1:
        assert len(handoff.mount_everywhere(value)) == 1
        assert len(probes) == 2
    else:
        with pytest.raises(SnapshotError, match='independent source namespace stat differs'):
            handoff.mount_everywhere(value)
        assert len(probes) == 3
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.parametrize('name', ['data_crypto_etf', 'data_coinmetrics_community',
    'data_crypto_historical_public', 'data_dune_crypto', 'data_keyed_public_catalogs',
    'data_crypto_reference', 'data_binance_archive', 'data_parquet', 'data_news', 'data_news_fulltext/bodies', 'data_yahoo',
    'data_cftc_legacy', 'data_fred_crypto_macro',
    'data_tw_microstructure/hft_dataset', 'data_tw_minute/shioaji_1m', 'data_tw_index_futures/training_prepared',
    'data_tw_index_options_daily/raw'])
def test_namespace_helper_accepts_only_reviewed_source_with_matching_marker(tmp_path, monkeypatch, name):
    from scripts import workspace_source_handoff_namespace as namespace
    root = tmp_path / 'repo'; root.mkdir()
    source = root / name; source.mkdir(parents=True)
    marker_name = ('annual/2001_opt.zip' if name == 'data_tw_index_options_daily/raw' else
                   handoff.FUTURES_MARKER if name == 'data_tw_index_futures/training_prepared' else
                   handoff.NEWS_BODIES_MARKER if name == 'data_news_fulltext/bodies' else
                   'manifest.json' if name == 'data_tw_microstructure/hft_dataset' else
                   'README.md' if name == 'data_parquet' else
                   'manifest.json' if name in {'data_news', 'data_cftc_legacy'} else 'download_summary.json')
    marker = source / marker_name; marker.parent.mkdir(parents=True, exist_ok=True); marker.write_text('{"snapshot":1}')
    info = source.stat()
    identity = [info.st_dev, info.st_ino]
    monkeypatch.setattr(namespace.common, 'mounts', lambda: {str(source): {}})
    descriptor = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
    proc = os.open('/proc', os.O_RDONLY | os.O_DIRECTORY)
    try:
        result = namespace.attach({'repository_root': str(root), 'target': str(source),
            'repository_identity': namespace.common.identity(root), 'proc_fd': proc,
            'tree_fd': descriptor, 'destination_identity': identity, 'isolated_probe': True,
            'marker_relative': marker_name, 'marker_sha256': relocation.digest(marker, no_follow=True)})
        assert result['verified'] and result['destination_identity'] == identity
        if name == 'data_tw_index_options_daily/raw':
            with pytest.raises(RuntimeError, match='marker scope differs'):
                namespace.attach({'repository_root': str(root), 'target': str(source),
                    'repository_identity': namespace.common.identity(root), 'proc_fd': proc,
                    'tree_fd': descriptor, 'destination_identity': identity, 'isolated_probe': True,
                    'marker_relative': '../txo_final_settlement_history.parquet',
                    'marker_sha256': relocation.digest(marker, no_follow=True)})
        marker.write_text('{"snapshot":2}')
        with pytest.raises(RuntimeError, match='marker readback differs'):
            namespace.attach({'repository_root': str(root), 'target': str(source),
                'repository_identity': namespace.common.identity(root), 'proc_fd': proc,
                'tree_fd': descriptor, 'destination_identity': identity, 'isolated_probe': True,
                'marker_relative': marker_name, 'marker_sha256': '0' * 64})
        with pytest.raises(RuntimeError, match='enrolled target'):
            namespace.attach({'repository_root': str(root), 'target': str(root / 'other'),
                'repository_identity': namespace.common.identity(root), 'proc_fd': proc})
    finally:
        os.close(descriptor); os.close(proc)
