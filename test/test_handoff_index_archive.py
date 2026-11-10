import json
import gzip
import hashlib
import os
import signal
import sqlite3
import subprocess
import sys
import zipfile
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from stockagent.data_sync import handoff_index_archive as archive, handoff_source_repack as handoff, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError
from test_handoff_source_repack import fixture, _simulate_c_device_renumber


def test_two_native_locks_are_preserved_while_indexed_payload_retires(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    from stockagent.data_sync import workspace_relocation
    (source/'.download.lock').unlink()  # Supplemental root lock was added after the fixed copy.
    for relative in ('state/collector.lock', 'rules/state/collector.lock'):
        path = source/relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'')
    with sqlite3.connect(index) as db:
        db.execute('DELETE FROM files')
        for name, kind, signature in workspace_relocation.walk(source):
            data = (source/name).read_bytes() if kind == 'file' else b''
            digest = hashlib.sha256(data).hexdigest() if kind == 'file' else None
            db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                       (name, kind, json.dumps(signature), digest, len(data), 'verified' if digest else 'pending'))
    (source/'.download.lock').write_bytes(b'')
    kwargs['original_lock_inode'] = (source/'.download.lock').stat().st_ino
    protected = ['.download.lock', 'state/collector.lock', 'rules/state/collector.lock']
    prepared = archive.prepare(source, stage, 'fixture', 'a'*64, index,
        **kwargs, preserved_paths=protected, supplemental_root_lock=True)
    assert prepared['preserved_paths'] == protected
    result = handoff.retire(source, stage, 'fixture', 'a'*64, index,
                            **kwargs, process_check=lambda _paths: [])
    assert result['state'] == 'C_payload_retired_root_and_lock_preserved'
    assert all((source/name).is_file() for name in protected)
    assert not (source/'nested/a.parquet').exists()


def _symlink_fixture(tmp_path, monkeypatch, target='a.parquet'):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    from stockagent.data_sync import workspace_relocation
    link = source/'nested/internal-link'
    link.symlink_to(target)
    with sqlite3.connect(index) as db:
        db.execute('UPDATE files SET sig=? WHERE path=?',
                   (json.dumps(workspace_relocation.signature(source/'nested')), 'nested'))
        db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)', (
            'nested/internal-link', 'symlink', json.dumps(workspace_relocation.signature(link)),
            hashlib.sha256(os.fsencode(target)).hexdigest(), 0, 'verified'))
    return source, cold, stage, index, old, kwargs


def test_internal_symlink_is_preserved_in_prepared_inventory_and_c_payload_retires(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _symlink_fixture(tmp_path, monkeypatch)
    prepared = archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
        preserved_paths=['.download.lock'], preserve_internal_symlinks=True)
    with gzip.open(stage/prepared['inventory']['name'], 'rt') as stream:
        rows = [json.loads(line) for line in stream]
    link = next(row for row in rows if row['path'] == 'nested/internal-link')
    assert link['kind'] == 'symlink' and link['target'] == 'a.parquet'
    assert prepared['preserve_internal_symlinks'] is True
    assert archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
        preserved_paths=['.download.lock'], preserve_internal_symlinks=True) == prepared
    result = handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                            process_check=lambda _paths: [], batch_size=1)
    assert result['files_removed'] == 2 and not (source/'nested/a.parquet').exists()
    assert os.path.islink(source/'nested/internal-link')
    assert os.readlink(source/'nested/internal-link') == 'a.parquet'


def test_interrupted_symlink_retirement_can_resume_after_its_target_is_retired(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _symlink_fixture(tmp_path, monkeypatch)
    archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                    preserved_paths=['.download.lock'], preserve_internal_symlinks=True)
    unlink = type(source).unlink
    def interrupted(path, *args, **options):
        result = unlink(path, *args, **options)
        if path == source/'nested/a.parquet':
            raise OSError('interrupted after unlink')
        return result
    monkeypatch.setattr(type(source), 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted after unlink'):
        handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                       process_check=lambda _paths: [], batch_size=1)
    monkeypatch.setattr(type(source), 'unlink', unlink)
    result = handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                            process_check=lambda _paths: [], batch_size=1)
    assert result['files_removed'] == 2 and os.path.islink(source/'nested/internal-link')


def test_unjournalled_missing_source_stays_blocked_across_retries(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs, preserved_paths=['.download.lock'])
    unlink = type(source).unlink
    def interrupted(path, *args, **options):
        result = unlink(path, *args, **options)
        if path == source/'nested/a.parquet':
            raise OSError('interrupted after unlink')
        return result
    monkeypatch.setattr(type(source), 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted after unlink'):
        handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                       process_check=lambda _paths: [], batch_size=1)
    monkeypatch.setattr(type(source), 'unlink', unlink)
    (source/'nested/b.bin').unlink()  # Not the owner's previously journalled unlink.
    for _ in range(2):
        with pytest.raises(SnapshotError, match='disappeared before its journalled batch'):
            handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                           process_check=lambda _paths: [], batch_size=1)


def test_sigkill_after_unlink_recovers_the_interrupted_sqlite_batch(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs, preserved_paths=['.download.lock'])
    code = '''
import json, os, signal, sys
from pathlib import Path
from stockagent.data_sync import handoff_source_repack as handoff
source, stage, index = map(Path, sys.argv[1:4])
kwargs = json.loads(sys.argv[4])
unlink = Path.unlink
def killed(path, *args, **options):
    result = unlink(path, *args, **options)
    if path == source/'nested/a.parquet':
        os.kill(os.getpid(), signal.SIGKILL)
    return result
Path.unlink = killed
handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
               process_check=lambda _paths: [], batch_size=1)
'''
    child = subprocess.run([sys.executable, '-c', code, str(source), str(stage), str(index), json.dumps(kwargs)],
                           capture_output=True, text=True, timeout=30)
    assert child.returncode == -signal.SIGKILL, child.stderr
    result = handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                            process_check=lambda _paths: [], batch_size=1)
    assert result['files_removed'] == 2
    assert not (source/'nested/a.parquet').exists() and not (source/'nested/b.bin').exists()
    assert (source/'.download.lock').is_file()


@pytest.mark.parametrize('target', ['../../outside', '/etc/passwd', 'absent'])
def test_symlink_archive_rejects_external_or_missing_targets(tmp_path, monkeypatch, target):
    source, _cold, stage, index, _old, kwargs = _symlink_fixture(tmp_path, monkeypatch, target)
    with pytest.raises(SnapshotError):
        archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
            preserved_paths=['.download.lock'], preserve_internal_symlinks=True)
    assert (source/'nested/a.parquet').is_file()


def test_symlink_archive_needs_explicit_preservation_policy(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _symlink_fixture(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match='rejects redirected'):
        archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                        preserved_paths=['.download.lock'])


def _linked_fixture(tmp_path, monkeypatch, *, internal_links=1, external=False):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    original = source/'nested/a.parquet'
    for number in range(internal_links):
        os.link(original, source/f'nested/link-{number}.bin')
    if external:
        os.link(original, tmp_path/'outside-source.bin')
    from stockagent.data_sync import workspace_relocation
    with sqlite3.connect(index) as db:
        for path in [source/'nested', original]:
            db.execute('UPDATE files SET sig=? WHERE path=?',
                       (json.dumps(workspace_relocation.signature(path)), path.relative_to(source).as_posix()))
        for number in range(internal_links):
            path = source/f'nested/link-{number}.bin'
            raw = path.read_bytes()
            db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                       (path.relative_to(source).as_posix(), 'file', json.dumps(workspace_relocation.signature(path)),
                        hashlib.sha256(raw).hexdigest(), len(raw), 'verified'))
    return source, cold, stage, index, old, kwargs


def _prepare_linked(source, stage, index, kwargs):
    return archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                           preserved_paths=['.download.lock'])


def test_index_archive_reports_distinct_inventory_and_namespace_phases(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    events = []
    archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                    preserved_paths=['.download.lock'], progress=events.append)
    for phase in ('checking_source_namespace', 'indexing_source_inventory',
                  'rechecking_source_namespace', 'writing_E_inventory'):
        observations = [event for event in events if event['stage'] == phase]
        assert observations and observations[0]['inventory_items_completed'] == 0
        assert observations[-1]['inventory_items_completed'] == observations[-1]['inventory_items_total']


def test_completed_serial_archive_is_reused_when_future_worker_count_changes(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    prepared = archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                               preserved_paths=['.download.lock'])
    assert archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                           preserved_paths=['.download.lock'], large_member_deflate_workers=4) == prepared


def test_retirement_reports_namespace_and_plan_before_unlink(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs, preserved_paths=['.download.lock'])
    events = []
    handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                   process_check=lambda _paths: [], progress=events.append, planning_progress=events.append)
    phases = [event['stage'] for event in events]
    assert phases.index('retirement_source_namespace') < phases.index('retirement_planning')
    assert phases.index('retirement_planning') < phases.index('retirement_plan_recheck')
    assert phases.index('retirement_plan_recheck') < phases.index('retiring_C_payload')


def test_hardlinks_are_retired_across_batches_and_allocation_counted_once(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(tmp_path, monkeypatch, internal_links=2)
    expected_allocation = sum((source/name).stat().st_blocks*512
                              for name in ('nested/a.parquet', 'nested/b.bin'))
    _prepare_linked(source, stage, index, kwargs)
    result = handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                            process_check=lambda _paths: [], batch_size=1)
    assert result['files_removed'] == 4
    assert result['estimated_allocated_bytes_reclaimed'] == expected_allocation
    assert not list((source/'nested').iterdir())


def test_hardlink_retirement_recovers_after_unlink_before_journal_commit(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(tmp_path, monkeypatch)
    _prepare_linked(source, stage, index, kwargs)
    unlink = type(source).unlink
    def interrupted(path, *args, **options):
        result = unlink(path, *args, **options)
        if path == source/'nested/a.parquet':
            raise OSError('interrupted after hardlink unlink')
        return result
    monkeypatch.setattr(type(source), 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted after hardlink'):
        handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                       process_check=lambda _paths: [], batch_size=1)
    monkeypatch.setattr(type(source), 'unlink', unlink)
    result = handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                            process_check=lambda _paths: [], batch_size=1)
    assert result['files_removed'] == 3
    assert not (source/'nested/link-0.bin').exists()


def test_external_hardlink_blocks_the_entire_retirement(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(
        tmp_path, monkeypatch, internal_links=1, external=True)
    _prepare_linked(source, stage, index, kwargs)
    with pytest.raises(SnapshotError, match='hardlinks extend outside'):
        handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                       process_check=lambda _paths: [])
    assert (source/'nested/a.parquet').exists() and (source/'nested/link-0.bin').exists()
    assert (tmp_path/'outside-source.bin').is_file()


def test_reviewed_external_hardlinks_preserve_other_names_and_count_no_allocation(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(
        tmp_path, monkeypatch, internal_links=1, external=True)
    monkeypatch.setattr(handoff, '_active_taifex_external_scope', lambda *_: True)
    dataset = 'taifex-public-workspace'
    archive.prepare(source, stage, dataset, 'a'*64, index, **kwargs, preserved_paths=['.download.lock'])
    outside = tmp_path/'outside-source.bin'
    data = outside.read_bytes()
    unique_allocation = (source/'nested/b.bin').stat().st_blocks*512
    checked = []
    def references(_paths, *, inode_keys):
        checked.extend(inode_keys)
        return []
    result = handoff.retire(source, stage, dataset, 'a'*64, index, **kwargs,
                            preserve_external_hardlinks=True, process_check=references, batch_size=1)
    assert checked and outside.read_bytes() == data and outside.stat().st_nlink == 1
    assert result['external_hardlink_groups_preserved'] == 1
    assert result['external_source_names_removed'] == 2
    assert result['estimated_allocated_bytes_reclaimed'] == unique_allocation
    assert not (source/'nested/a.parquet').exists() and not (source/'nested/link-0.bin').exists()


def test_external_hardlink_refuses_live_inode_reference(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(
        tmp_path, monkeypatch, internal_links=0, external=True)
    monkeypatch.setattr(handoff, '_active_taifex_external_scope', lambda *_: True)
    dataset = 'taifex-public-workspace'
    archive.prepare(source, stage, dataset, 'a'*64, index, **kwargs, preserved_paths=['.download.lock'])
    with pytest.raises(SnapshotError, match='running process'):
        handoff.retire(source, stage, dataset, 'a'*64, index, **kwargs,
            preserve_external_hardlinks=True, process_check=lambda _paths, **_kw:['busy'])
    assert (source/'nested/a.parquet').exists() and (tmp_path/'outside-source.bin').exists()


def test_external_hardlink_changed_through_other_name_is_not_retired(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(
        tmp_path, monkeypatch, internal_links=0, external=True)
    monkeypatch.setattr(handoff, '_active_taifex_external_scope', lambda *_: True)
    dataset = 'taifex-public-workspace'
    archive.prepare(source, stage, dataset, 'a'*64, index, **kwargs, preserved_paths=['.download.lock'])
    mutated = False
    def change_after_planning(event):
        nonlocal mutated
        if event['stage'] == 'retirement_plan_recheck' and not mutated:
            (tmp_path/'outside-source.bin').write_bytes(b'changed elsewhere')
            mutated = True
    with pytest.raises(SnapshotError, match='changed'):
        handoff.retire(source, stage, dataset, 'a'*64, index, **kwargs,
            preserve_external_hardlinks=True, process_check=lambda _paths, **_kw:[],
            planning_progress=change_after_planning)
    assert (source/'nested/a.parquet').exists() and (tmp_path/'outside-source.bin').exists()


def test_external_hardlink_recovers_after_last_in_scope_unlink(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(
        tmp_path, monkeypatch, internal_links=0, external=True)
    monkeypatch.setattr(handoff, '_active_taifex_external_scope', lambda *_: True)
    dataset = 'taifex-public-workspace'
    archive.prepare(source, stage, dataset, 'a'*64, index, **kwargs, preserved_paths=['.download.lock'])
    unlink = type(source).unlink
    def interrupted(path, *args, **options):
        result = unlink(path, *args, **options)
        if path == source/'nested/a.parquet':
            raise OSError('interrupted after the shared name was unlinked')
        return result
    monkeypatch.setattr(type(source), 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted after the shared name'):
        handoff.retire(source, stage, dataset, 'a'*64, index, **kwargs, batch_size=1,
            preserve_external_hardlinks=True, process_check=lambda _paths, **_kw:[])
    monkeypatch.setattr(type(source), 'unlink', unlink)
    result = handoff.retire(source, stage, dataset, 'a'*64, index, **kwargs, batch_size=1,
        preserve_external_hardlinks=True, process_check=lambda _paths, **_kw:[])
    assert result['external_hardlink_groups_preserved'] == 1
    assert (tmp_path/'outside-source.bin').exists()


def test_external_hardlink_recovers_after_sigkill_before_journal_commit(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(
        tmp_path, monkeypatch, internal_links=0, external=True)
    monkeypatch.setattr(handoff, '_active_taifex_external_scope', lambda *_: True)
    dataset = 'taifex-public-workspace'
    archive.prepare(source, stage, dataset, 'a'*64, index, **kwargs, preserved_paths=['.download.lock'])
    code = '''
import json, os, signal, sys
from pathlib import Path
from stockagent.data_sync import handoff_source_repack as h
source, stage, index = map(Path, sys.argv[1:4])
kwargs = json.loads(sys.argv[4])
h._active_taifex_external_scope = lambda *_: True
unlink = Path.unlink
def killed(path, *args, **options):
    result = unlink(path, *args, **options)
    if path == source/'nested/a.parquet':
        os.kill(os.getpid(), signal.SIGKILL)
    return result
Path.unlink = killed
h.retire(source, stage, 'taifex-public-workspace', 'a'*64, index, **kwargs,
         preserve_external_hardlinks=True, process_check=lambda _paths, **_kw:[], batch_size=1)
'''
    child = subprocess.run([sys.executable, '-c', code, str(source), str(stage), str(index), json.dumps(kwargs)],
                           capture_output=True, text=True, timeout=30)
    assert child.returncode == -signal.SIGKILL, child.stderr
    result = handoff.retire(source, stage, dataset, 'a'*64, index, **kwargs,
        preserve_external_hardlinks=True, process_check=lambda _paths, **_kw:[], batch_size=1)
    assert result['external_hardlink_groups_preserved'] == 1
    assert (tmp_path/'outside-source.bin').exists()


def test_external_hardlink_policy_cannot_be_used_for_an_unreviewed_dataset(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(tmp_path, monkeypatch, external=True)
    _prepare_linked(source, stage, index, kwargs)
    with pytest.raises(SnapshotError, match='not bound to the active TAIFEX source'):
        handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
            preserve_external_hardlinks=True, process_check=lambda _paths, **_kw:[])


def test_interrupted_hardlink_rejects_changed_survivor_even_if_mtime_restored(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(tmp_path, monkeypatch)
    _prepare_linked(source, stage, index, kwargs)
    unlink = type(source).unlink
    def interrupted(path, *args, **options):
        result = unlink(path, *args, **options)
        if path == source/'nested/a.parquet':
            raise OSError('interrupted after hardlink unlink')
        return result
    monkeypatch.setattr(type(source), 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted after hardlink'):
        handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                       process_check=lambda _paths: [], batch_size=1)
    monkeypatch.setattr(type(source), 'unlink', unlink)
    survivor = source/'nested/link-0.bin'
    old_stat = survivor.stat()
    survivor.write_bytes(b'x'*old_stat.st_size)
    os.utime(survivor, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
    with pytest.raises(SnapshotError, match='hardlink content changed'):
        handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                       process_check=lambda _paths: [], batch_size=1)
    assert survivor.is_file()


def test_external_hardlink_added_between_batches_is_rejected(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = _linked_fixture(tmp_path, monkeypatch)
    _prepare_linked(source, stage, index, kwargs)
    outside = tmp_path/'late-outside-link'
    def progress(_event):
        if not outside.exists():
            os.link(source/'nested/link-0.bin', outside)
    with pytest.raises(SnapshotError, match='hardlink identity or link count changed'):
        handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                       process_check=lambda _paths: [], batch_size=1, progress=progress)
    assert (source/'nested/link-0.bin').exists() and outside.exists()


def test_reviewed_news_sidecar_override_survives_pack_and_retirement(tmp_path, monkeypatch):
    from stockagent.data_sync import workspace_relocation, workspace_source_handoff
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(workspace_source_handoff, 'NEWS_SIDECAR_PATHS', frozenset({'nested/a.parquet'}))
    member = source/'nested/a.parquet'
    info = member.stat()
    os.utime(member, ns=(info.st_atime_ns, info.st_mtime_ns+1000000))
    overrides = {'nested/a.parquet': workspace_relocation.signature(member)}
    prepared = archive.prepare(source, stage, 'news-training-workspace', 'a'*64, index, **kwargs,
        preserved_paths=['.download.lock'], source_overrides=overrides)
    assert prepared['source_generation_overrides'] == overrides
    result = handoff.retire(source, stage, 'news-training-workspace', 'a'*64, index, **kwargs,
                            process_check=lambda _paths: [])
    assert result['files_removed'] == 2 and not member.exists()


def test_news_sidecar_override_and_hardlinks_compose_across_restart(tmp_path, monkeypatch):
    from stockagent.data_sync import workspace_relocation, workspace_source_handoff
    source, _cold, stage, index, _old, kwargs = _linked_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(workspace_source_handoff, 'NEWS_SIDECAR_PATHS', frozenset({'nested/b.bin'}))
    member = source/'nested/b.bin'
    info = member.stat()
    os.utime(member, ns=(info.st_atime_ns, info.st_mtime_ns+1000000))
    overrides = {'nested/b.bin': workspace_relocation.signature(member)}
    archive.prepare(source, stage, 'news-training-workspace', 'a'*64, index, **kwargs,
                    preserved_paths=['.download.lock'], source_overrides=overrides)
    unlink = type(source).unlink
    def interrupted(path, *args, **options):
        result = unlink(path, *args, **options)
        if path == source/'nested/a.parquet':
            raise OSError('interrupted after hardlink unlink')
        return result
    monkeypatch.setattr(type(source), 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted after hardlink'):
        handoff.retire(source, stage, 'news-training-workspace', 'a'*64, index, **kwargs,
                       process_check=lambda _paths: [], batch_size=1)
    monkeypatch.setattr(type(source), 'unlink', unlink)
    result = handoff.retire(source, stage, 'news-training-workspace', 'a'*64, index, **kwargs,
                            process_check=lambda _paths: [], batch_size=1)
    assert result['files_removed'] == 3 and not member.exists()


def test_source_override_cannot_change_inode_or_escape_reviewed_dataset(tmp_path, monkeypatch):
    from stockagent.data_sync import workspace_relocation, workspace_source_handoff
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(workspace_source_handoff, 'NEWS_SIDECAR_PATHS', frozenset({'nested/a.parquet'}))
    signature = workspace_relocation.signature(source/'nested/a.parquet')
    signature[1] += 1
    with pytest.raises(SnapshotError, match='beyond sidecar clocks'):
        archive.prepare(source, stage, 'news-training-workspace', 'a'*64, index, **kwargs,
                        preserved_paths=['.download.lock'], source_overrides={'nested/a.parquet': signature})
    with pytest.raises(SnapshotError, match='outside the reviewed'):
        archive.prepare(source, stage.parent/'second-stage', 'fixture', 'a'*64, index, **kwargs,
                        preserved_paths=['.download.lock'], source_overrides={'nested/a.parquet': signature})


@pytest.mark.parametrize('supplemental', [False, True])
def test_fixed_index_archive_and_retirement_preserve_all_locks(tmp_path, monkeypatch, supplemental):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    # Add the nested lock to the fixed index as it would exist at handoff.
    nested = source/'nested/.download.lock'
    nested.write_bytes(b'')
    from stockagent.data_sync import workspace_relocation
    import hashlib
    with sqlite3.connect(index) as db:
        db.execute("UPDATE files SET sig=? WHERE path='nested'",
                   (json.dumps(workspace_relocation.signature(source/'nested')),))
        db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)', ('nested/.download.lock', 'file',
            json.dumps(workspace_relocation.signature(nested)), hashlib.sha256(b'').hexdigest(), 0, 'verified'))
        if supplemental:
            db.execute("DELETE FROM files WHERE path='.download.lock'")
    preserved = ['.download.lock', 'nested/.download.lock']
    prepared = archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                               preserved_paths=preserved, supplemental_root_lock=supplemental)
    assert prepared['source_mode'] == 'retained_handoff_C_index'
    assert prepared['files'] == (3 if supplemental else 4)
    with zipfile.ZipFile(stage/prepared['packages'][0]['name']) as encoded:
        assert encoded.read('nested/a.parquet') == (source/'nested/a.parquet').read_bytes()
    assert archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                           preserved_paths=preserved, supplemental_root_lock=supplemental) == prepared
    result = handoff.retire(source, stage, 'fixture', 'a'*64, index,
                            **kwargs, process_check=lambda _paths: [])
    assert result['files_removed'] == 2
    assert (source/'.download.lock').exists() and nested.exists()
    assert not (source/'nested/a.parquet').exists()
    assert stream_repack._read_prepared(stage, 'fixture', 'a'*64) == prepared


def test_fixed_index_archive_refuses_changed_input(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    (source/'nested/a.parquet').write_bytes(b'changed')
    with pytest.raises(SnapshotError, match='generation changed'):
        archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                        preserved_paths=['.download.lock'])
    assert not (stage/'prepared.json').exists()


def test_fixed_index_archive_and_retirement_accept_same_c_volume_after_renumber(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    _simulate_c_device_renumber(source, index, monkeypatch)
    prepared = archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                               preserved_paths=['.download.lock'])
    assert prepared['source_member_sha_streamed']
    result = handoff.retire(source, stage, 'fixture', 'a'*64, index,
                            **kwargs, process_check=lambda _paths: [])
    assert result['files_removed'] == 2 and not (source/'nested/b.bin').exists()


def test_reviewed_futures_projection_rebinds_only_its_exact_retained_source(tmp_path, monkeypatch):
    from scripts import project_futures_725_workspace_index as projection
    from stockagent.data_sync import workspace_source_handoff, workspace_volume
    source, _cold, stage, original_index, _old, kwargs = fixture(tmp_path, monkeypatch)
    evidence = tmp_path/'migration'/projection.CHILD
    evidence.mkdir(parents=True)
    index = evidence/'files.sqlite'
    original_index.rename(index)
    _simulate_c_device_renumber(source, index, monkeypatch)
    original_source = tmp_path/'original-futures-725'
    scope = SimpleNamespace(source=original_source, backup=source)
    monkeypatch.setattr(workspace_source_handoff, '_registered_scope', lambda _name: scope)
    monkeypatch.setattr(workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(projection, 'validated_projection', lambda root, scope=None: {'name': projection.CHILD})
    path = evidence/'intent.json'
    intent = json.loads(path.read_text())
    intent.update(contract='workspace_scoped_relocation_v1', source=str(original_source),
                  excluded_subtrees=[], preserve_symlinks=True)
    path.write_text(json.dumps(intent))
    with sqlite3.connect(index) as db:
        db.execute("DELETE FROM files WHERE path='.download.lock'")
    prepared = archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                               preserved_paths=['.download.lock'], supplemental_root_lock=True)
    assert prepared['source_member_sha_streamed']
    result = handoff.retire(source, stage, 'fixture', 'a'*64, index, **kwargs,
                            process_check=lambda _paths: [])
    assert result['files_removed'] == 2 and (source/'.download.lock').is_file()
    intent['excluded_subtrees'] = ['unexpected']
    path.write_text(json.dumps(intent))
    assert handoff._source_index_rebind(source, index, kwargs['original_inode']) is None


def test_reviewed_taifex_scoped_copy_rebinds_only_its_active_fixed_index(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from scripts import run_workspace_source_staging as staging
    from stockagent.data_sync import workspace_source_handoff, workspace_volume
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    _simulate_c_device_renumber(source, index, monkeypatch)
    original_source = tmp_path/'original-taifex'
    scope = SimpleNamespace(source=original_source, backup=source)
    monkeypatch.setattr(workspace_source_handoff, '_registered_scope', lambda _name: scope)
    monkeypatch.setattr(workspace_source_handoff, 'selected_source', lambda _name: nullcontext())
    monkeypatch.setattr(workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(workspace_source_handoff, 'enrolled',
                        lambda: ({'name': 'taifex'}, tmp_path, tmp_path/'dest', index.parent, None))
    journal = {'state': 'active', 'original_inode': kwargs['original_inode'], 'copy_result_sha256': 'a'*64}
    monkeypatch.setattr(workspace_source_handoff, 'read_journal', lambda: journal)
    monkeypatch.setattr(staging, 'verified_report', lambda *_: {'relocation_evidence_sha256': 'a'*64})
    path = index.parent/'intent.json'
    intent = json.loads(path.read_text())
    intent.update(contract='workspace_scoped_relocation_v1', source=str(original_source),
                  excluded_subtrees=[], preserve_symlinks=True)
    path.write_text(json.dumps(intent))
    assert handoff._source_index_rebind(source, index, kwargs['original_inode']) is not None
    journal['copy_result_sha256'] = 'b'*64
    assert handoff._source_index_rebind(source, index, kwargs['original_inode']) is None
    journal['copy_result_sha256'] = 'a'*64
    journal['state'] = 'prepared'
    assert handoff._source_index_rebind(source, index, kwargs['original_inode']) is None
    journal['state'] = 'active'
    intent['excluded_subtrees'] = ['nested']
    path.write_text(json.dumps(intent))
    assert handoff._source_index_rebind(source, index, kwargs['original_inode']) is None


def test_renumbered_source_with_late_empty_root_lock_is_packed_and_retired(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    _simulate_c_device_renumber(source, index, monkeypatch)
    with sqlite3.connect(index) as db:
        db.execute("DELETE FROM files WHERE path='.download.lock'")
    prepared = archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                               preserved_paths=['.download.lock'], supplemental_root_lock=True)
    assert prepared['files'] == 2 and prepared['source_member_sha_streamed']
    result = handoff.retire(source, stage, 'fixture', 'a'*64, index,
                            **kwargs, process_check=lambda _paths: [])
    assert result['files_removed'] == 2 and (source/'.download.lock').is_file()


def test_fixed_index_archive_resumes_sealed_packs_after_interruption(tmp_path, monkeypatch):
    source, _cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(stream_repack, 'TARGET_BYTES', 1)
    consume = stream_repack._PackBuilder.consume
    def interrupted(self, stream, row):
        if self.checkpoint.packages:
            raise OSError('after sealed package')
        return consume(self, stream, row)
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', interrupted)
    with pytest.raises(OSError, match='after sealed package'):
        archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                        preserved_paths=['.download.lock'])
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', consume)
    result = archive.prepare(source, stage, 'fixture', 'a'*64, index, **kwargs,
                             preserved_paths=['.download.lock'])
    assert result['files'] == 3 and len(result['packages']) >= 2


def test_index_archive_cli_uses_original_C_without_touching_live_E_writer(tmp_path, monkeypatch):
    from scripts import prepare_handoff_index_archive as cli
    source, _cold, _stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    workspace = tmp_path/'workspace'
    migration = workspace/'migration'
    migration.mkdir(parents=True)
    evidence = tmp_path/'evidence'
    evidence.mkdir()
    (evidence/'files.sqlite').write_bytes(index.read_bytes())
    config = {'mount_point': str(workspace), 'minimum_free_bytes': 0}
    monkeypatch.setattr(cli, 'ENROLLED', {'fixture-source': ('fixture', ['.download.lock'], False)})
    monkeypatch.setattr(cli.workspace_volume, 'configuration', lambda: config)
    monkeypatch.setattr(cli.workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(cli.workspace_volume, 'admit', lambda *_a, **_kw: None)
    monkeypatch.setattr(cli.workspace_volume, 'path', lambda *_a, **_kw: migration)
    monkeypatch.setattr(cli.provider_workspace, 'exact_mount', lambda *_a: True)
    monkeypatch.setattr(cli.h, 'read_journal', lambda: {'state': 'active', 'destination': str(tmp_path/'destination'),
        'evidence': str(evidence), 'copy_result_sha256': 'b'*64,
        'files': old.manifest['source']['files'], 'logical_bytes': old.manifest['source']['logical_bytes'], **kwargs})
    monkeypatch.setattr(cli.h, '_assert_retained', lambda *_a: None)
    monkeypatch.setattr(cli.h, '_scope', lambda: SimpleNamespace(source=tmp_path/'alias', backup=source))
    monkeypatch.setattr(cli.h, '_source_work_window_guard', lambda: lambda: None)
    locks = []
    @contextmanager
    def selected(*_a, **_kw):
        yield
    @contextmanager
    def lock(path):
        locks.append(path)
        yield
    @contextmanager
    def reserve(*_a, **_kw):
        yield {'other_reserved_growth_bytes': 0}
    monkeypatch.setattr(cli.h, 'selected_source', selected)
    monkeypatch.setattr(cli.h, 'lock_file', lock)
    monkeypatch.setattr(cli, 'reserve_capacity', reserve)
    monkeypatch.setattr(cli.sys, 'argv', ['archive', '--source', 'fixture-source', '--retire'])
    cli.main()
    status = json.loads((migration/'handoff-index-archive-v1/fixture.status.json').read_text())
    assert status['state'] == 'C_payload_retired_root_and_locks_preserved'
    assert status['C_payload_deleted'] and not status['D_archive_committed']
    assert not (source/'nested/a.parquet').exists() and (source/'.download.lock').exists()
    assert all(not path.is_relative_to(tmp_path/'destination') for path in locks)
