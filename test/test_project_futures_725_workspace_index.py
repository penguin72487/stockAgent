from __future__ import annotations

from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time

import pytest

from scripts import project_futures_725_workspace_index as project
from scripts import run_workspace_source_staging as queue
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync.desync_snapshots import SnapshotError

REAL_QUEUE_PATHS = queue.paths
REAL_QUEUE_REPORT = queue.verified_report


@pytest.fixture
def fixed_parent(tmp_path, monkeypatch):
    repo = tmp_path/'repo'; repo.mkdir()
    source = repo/'data_tw_index_futures/training_prepared'; source.mkdir(parents=True)
    child = source/project.PART; child.mkdir()
    (child/project.MARKER).write_bytes(b'fixed markers')
    (child/'financial').mkdir(); (child/'financial'/'data.bin').write_bytes(b'financial')
    (source/'factorized-sibling').mkdir(); (source/'factorized-sibling'/'keep').write_bytes(b'untouched')
    workspace = tmp_path/'workspace'; (workspace/'migration').mkdir(parents=True); (workspace/'verify').mkdir()
    destination = workspace/'downloads'/project.PARENT/'data_tw_index_futures/training_prepared'
    destination.parent.mkdir(parents=True)
    shutil.copytree(source, destination)
    evidence = workspace/'migration'/project.PARENT; evidence.mkdir()
    index = evidence/'files.sqlite'
    rows = list(relocation.walk(source, preserve_symlinks=True))
    with sqlite3.connect(index) as db:
        db.execute('CREATE TABLE files (path TEXT PRIMARY KEY,kind TEXT,sig TEXT,sha TEXT,bytes INTEGER,state TEXT)')
        for name, kind, sig in rows:
            digest = relocation.digest(source/name) if kind == 'file' else None
            db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                       (name, kind, json.dumps(sig), digest, sig[3] if kind == 'file' else 0,
                        'verified' if kind == 'file' else 'pending'))
    copy = {'contract':'workspace_scoped_relocation_v1','state':'exact_destination_readback_verified',
            'all_source_sha256_and_destination_readback_verified':True,'source_namespace_unchanged':True,
            'selected_source_sha256_and_destination_readback_verified':True,'excluded_subtrees':[],
            'source_files_deleted':0,'observed_at_epoch':time.time()}
    (evidence/'copy-result.json').write_text(json.dumps(copy))
    (evidence/'intent.json').write_text(json.dumps({'contract':'workspace_scoped_relocation_v1',
        'state':'indexed','excluded_subtrees':[],'preserve_symlinks':True,
        'source':str(source),'destination':str(destination)}))
    report = workspace/'verify'/f'{project.PARENT}.json'
    proof = {'observed_at_epoch':time.time()}
    report.write_text(json.dumps(proof))
    item = {'name':project.PARENT,'source':'data_tw_index_futures/training_prepared'}
    monkeypatch.setattr(queue, 'load_sources', lambda: [item])
    monkeypatch.setattr(queue, 'paths', lambda _item, _workspace: (source,destination,evidence,report))
    monkeypatch.setattr(queue, 'verified_report', lambda _item, _workspace: proof)
    return {'workspace':workspace,'source':source,'destination':destination,'evidence':evidence,'report':report,
            'proof':proof,'index':index}


def test_projects_only_725_names_and_reuses_historical_evidence_without_reading_payload(fixed_parent, monkeypatch):
    original_digest = relocation.digest
    def guarded_digest(path, *args, **kwargs):
        path = Path(path)
        if path.is_relative_to(fixed_parent['source']) or path.is_relative_to(fixed_parent['destination']):
            raise AssertionError('source or destination payload read')
        return original_digest(path, *args, **kwargs)
    monkeypatch.setattr(relocation, 'digest', guarded_digest)
    events = []
    result = project.project(fixed_parent['workspace'], progress=events.append)
    assert result['files'] == 2 and result['logical_bytes'] == len(b'fixed markersfinancial')
    assert result['payload_read_during_projection'] is False
    assert result['source_files_deleted'] == 0 and result['source_alias_changed'] is False
    assert events[-1]['stage'] == 'completed'
    child_evidence = fixed_parent['workspace']/'migration'/project.CHILD
    with sqlite3.connect(child_evidence/'files.sqlite') as db:
        assert db.execute('SELECT path,kind FROM files ORDER BY path').fetchall() == [
            ('financial','directory'), ('financial/data.bin','file'), ('requested_725.csv','file')]
        assert db.execute('PRAGMA table_info(files)').fetchone()[5] == 1
    child_copy = json.loads((child_evidence/'copy-result.json').read_bytes())
    assert child_copy['observed_at_epoch'] == result['parent']['historical_copy_observed_at_epoch']
    assert child_copy['evidence_scope'] == 'derived_subset_of_historical_parent_copy'
    assert project.project(fixed_parent['workspace']) == result
    assert (fixed_parent['source']/'factorized-sibling'/'keep').read_bytes() == b'untouched'


def test_765_projection_preserves_one_indexed_symlink_without_reading_payload(fixed_parent, monkeypatch):
    scope = project.FUTURES_765
    source = fixed_parent['source']/scope.part
    (source/Path(scope.marker).parent).mkdir(parents=True)
    (source/scope.marker).write_bytes(b'fixed universe')
    (source/'internal-target').mkdir()
    (source/'internal-link').symlink_to('internal-target', target_is_directory=True)
    destination = fixed_parent['destination']/scope.part
    shutil.copytree(source, destination, symlinks=True)
    with sqlite3.connect(fixed_parent['index']) as db:
        entries = [(scope.part, 'directory', relocation.signature(source))]
        entries.extend((scope.part+'/'+name, kind, sig)
                       for name, kind, sig in relocation.walk(source, preserve_symlinks=True))
        for name, kind, sig in entries:
            original = fixed_parent['source']/name
            digest = (relocation.digest(original) if kind == 'file' else
                      hashlib.sha256(os.fsencode(os.readlink(original))).hexdigest() if kind == 'symlink' else None)
            db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                       (name, kind, json.dumps(sig), digest, sig[3] if kind == 'file' else 0,
                        'verified' if kind in {'file','symlink'} else 'pending'))
    stamp = time.time()+1
    copy_path = fixed_parent['evidence']/'copy-result.json'
    copy = json.loads(copy_path.read_text()); copy['observed_at_epoch'] = stamp
    copy_path.write_text(json.dumps(copy))
    fixed_parent['proof']['observed_at_epoch'] = stamp
    fixed_parent['report'].write_text(json.dumps(fixed_parent['proof']))
    original_digest = relocation.digest
    def guarded_digest(path, *args, **kwargs):
        path = Path(path)
        if path.is_relative_to(fixed_parent['source']) or path.is_relative_to(fixed_parent['destination']):
            raise AssertionError('source or destination payload read')
        return original_digest(path, *args, **kwargs)
    monkeypatch.setattr(relocation, 'digest', guarded_digest)
    result = project.project(fixed_parent['workspace'], scope=scope)
    assert result['name'] == scope.name and result['symlink_names_preserved'] == 1
    assert result['payload_read_during_projection'] is False
    with sqlite3.connect(fixed_parent['workspace']/'migration'/scope.name/'files.sqlite') as db:
        assert db.execute("SELECT kind,state FROM files WHERE path='internal-link'").fetchone() == ('symlink','verified')
    monkeypatch.setattr(project, 'ROOT', fixed_parent['source'].parents[1])
    assert project.validated_projection(fixed_parent['workspace'], scope=scope) == result
    assert (fixed_parent['source']/'factorized-sibling'/'keep').read_bytes() == b'untouched'


@pytest.mark.parametrize('failure', ['before_rename','after_rename'])
def test_interrupted_sealed_index_resumes_without_recopying_payload(fixed_parent, monkeypatch, failure):
    with monkeypatch.context() as patch:
        if failure == 'before_rename':
            patch.setattr(project.os, 'rename', lambda *_a: (_ for _ in ()).throw(OSError('interrupted')))
        else:
            real_write = project.atomic_write_json
            def interrupted(path, value, **kwargs):
                if Path(path).name == 'copy-result.json':
                    raise OSError('interrupted')
                return real_write(path, value, **kwargs)
            patch.setattr(project, 'atomic_write_json', interrupted)
        with pytest.raises(OSError, match='interrupted'):
            project.project(fixed_parent['workspace'])
    result = project.project(fixed_parent['workspace'])
    assert result['state'] == 'projected_historical_copy_index' and result['files'] == 2
    assert (fixed_parent['source']/project.PART/project.MARKER).read_bytes() == b'fixed markers'


def test_refuses_parent_index_changed_after_historical_report(fixed_parent):
    with sqlite3.connect(fixed_parent['index']) as db:
        db.execute("UPDATE files SET state='pending' WHERE path=?", (project.PART+'/'+project.MARKER,))
    with pytest.raises(SnapshotError, match='index changed'):
        project.project(fixed_parent['workspace'])


def test_refuses_factorized_or_symlink_members_within_725_scope(fixed_parent):
    with sqlite3.connect(fixed_parent['index']) as db:
        db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                   (project.PART+'/factorized_manifest.json','file','[]','a'*64,1,'verified'))
    fixed_parent['proof']['observed_at_epoch'] = time.time()
    fixed_parent['report'].write_text(json.dumps(fixed_parent['proof']))
    with pytest.raises(SnapshotError, match='factorized or unsupported'):
        project.project(fixed_parent['workspace'])


def test_does_not_begin_when_protected_guard_rejects(fixed_parent):
    def blocked():
        raise BlockingIOError('tw_day_trade_priority_window')
    with pytest.raises(BlockingIOError):
        project.project(fixed_parent['workspace'], guard=blocked)
    assert not (fixed_parent['workspace']/'migration'/project.CHILD).exists()


def test_resumes_committed_metadata_batches_without_reading_payload(fixed_parent, monkeypatch):
    monkeypatch.setattr(project, 'BATCH_ENTRIES', 1)
    stopping = [False]
    def progress(event):
        if event.get('stage') == 'indexing_existing_metadata' and event.get('completed_items') == 1:
            stopping[0] = True
    def guard():
        if stopping[0]:
            raise BlockingIOError('tw_day_trade_priority_window')
    with pytest.raises(BlockingIOError):
        project.project(fixed_parent['workspace'], progress=progress, guard=guard)
    partial = fixed_parent['workspace']/'migration'/project.CHILD/'files.sqlite.partial'
    with sqlite3.connect(partial) as db:
        assert db.execute('SELECT completed FROM projection').fetchone() == (1,)
        assert db.execute('SELECT count(*) FROM files').fetchone() == (1,)
    result = project.project(fixed_parent['workspace'])
    assert result['state'] == 'projected_historical_copy_index' and result['entries'] == 3


def test_rejects_modified_partial_checkpoint_even_when_count_matches(fixed_parent, monkeypatch):
    monkeypatch.setattr(project, 'BATCH_ENTRIES', 1)
    stopping = [False]
    def progress(event):
        if event.get('stage') == 'indexing_existing_metadata' and event.get('completed_items') == 1:
            stopping[0] = True
    def guard():
        if stopping[0]:
            raise BlockingIOError('interrupted')
    with pytest.raises(BlockingIOError):
        project.project(fixed_parent['workspace'], progress=progress, guard=guard)
    partial = fixed_parent['workspace']/'migration'/project.CHILD/'files.sqlite.partial'
    with sqlite3.connect(partial) as db:
        db.execute("UPDATE files SET sig='[]'")
    with pytest.raises(SnapshotError, match='differs from its fixed parent'):
        project.project(fixed_parent['workspace'])


def test_monitor_reports_real_progress_and_only_marks_the_projection_completed(fixed_parent, monkeypatch):
    from stockagent.data_sync import storage_progress as monitor
    monkeypatch.setattr(project, 'ROOT', fixed_parent['source'].parents[1])
    status = fixed_parent['workspace']/'migration'/(project.CHILD+'.status.json')
    status.write_text(json.dumps({'contract':project.CONTRACT, 'state':'indexing_existing_metadata',
        'parent_report_sha256':project._sha(fixed_parent['report']), 'started_at_epoch':time.time(),
        'observed_at_epoch':time.time(), 'progress':{'completed_items':1,'total_items':3}}))
    row, = monitor.futures_725_projection_tasks(project.ROOT, fixed_parent['workspace'])
    assert row['state'] == 'running' and row['completed_items'] == 1 and row['total_items'] == 3
    assert not row['complete_evidence']
    project.project(fixed_parent['workspace'])
    row, = monitor.futures_725_projection_tasks(project.ROOT, fixed_parent['workspace'])
    assert row['state'] == 'completed' and row['complete_evidence']
    assert row['completed_items'] == row['total_items'] == 3
    assert row['subject'] == 'tw-futures-universe725-workspace'
    assert 'futures_725_projection' in monitor.TASK_SCHEDULES
    index = fixed_parent['workspace']/'migration'/project.CHILD/'files.sqlite'
    with sqlite3.connect(index) as db:
        db.execute("UPDATE files SET sig='[]' WHERE kind='file'")
    row, = monitor.futures_725_projection_tasks(project.ROOT, fixed_parent['workspace'])
    assert row['state'] == 'blocked' and not row['complete_evidence']


def test_projected_copy_is_reused_by_original_handoff_under_both_writer_locks(fixed_parent, monkeypatch):
    from stockagent.data_sync import workspace_source_handoff as handoff
    workspace = fixed_parent['workspace']
    repo = fixed_parent['source'].parents[1]
    project.project(workspace)
    monkeypatch.setattr(project, 'ROOT', repo)
    monkeypatch.setattr(queue, 'ROOT', repo)
    monkeypatch.setattr(queue, 'paths', REAL_QUEUE_PATHS)
    monkeypatch.setattr(queue, 'verified_report', REAL_QUEUE_REPORT)
    monkeypatch.setattr(handoff, 'ROOT', repo)
    monkeypatch.setattr(handoff, 'CONTROL', fixed_parent['workspace']/'control')
    monkeypatch.setattr(handoff.volume, 'configuration', lambda: {'mount_point':str(workspace)})
    monkeypatch.setattr(handoff, '_source_work_window_guard', lambda: lambda: None)
    original_digest = relocation.digest
    def no_payload(path, *args, **kwargs):
        if Path(path).is_relative_to(fixed_parent['source']) or Path(path).is_relative_to(fixed_parent['destination']):
            raise AssertionError('new source or destination payload read')
        return original_digest(path, *args, **kwargs)
    monkeypatch.setattr(relocation, 'digest', no_payload)
    with handoff.selected_source(handoff.FUTURES_725_NAME):
        item, selected_workspace, destination, _evidence, _report = handoff.enrolled()
        with ExitStack() as locks:
            handoff._lock_writer_roots(locks, handoff._scope().source, destination)
            proof = handoff.verify_current_copy(item, selected_workspace)
    assert proof['files'] == 2 and proof['new_payload_readback'] is False
    assert (fixed_parent['source']/'factorized-sibling'/'keep').read_bytes() == b'untouched'
