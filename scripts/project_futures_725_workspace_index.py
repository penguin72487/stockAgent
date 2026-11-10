#!/usr/bin/env python3
"""Derive one exact futures subtree index from its previously copied E index.

This is metadata preparation only: it does not attach a mount, read a data
payload, alter C, publish to D, or authorize source retirement.
"""
from __future__ import annotations

import fcntl
import argparse
from dataclasses import dataclass
import hashlib
from itertools import zip_longest
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts import run_workspace_source_staging as queue
from stockagent.data_sync import workspace_relocation as relocation, workspace_source_handoff as handoff
from stockagent.data_sync import workspace_volume as volume
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.desync_snapshots import SnapshotError, _fsync_directory

PARENT = 'tw-futures-prepared-e-primary-20261104-v1'
CHILD = 'tw-futures-universe725-e-projection-20261108-v1'
PART = 'tw_futures_universe725_20261025_v2'
MARKER = 'requested_725.csv'
CONTRACT = 'stockagent.fixed_futures_subtree_index_projection.v1'
BATCH_ENTRIES = 32768


@dataclass(frozen=True)
class ProjectionScope:
    name: str
    part: str
    marker: str
    symlinks: int


FUTURES_765 = ProjectionScope('tw-futures-universe765-e-projection-20261108-v1',
    'tw_futures_universe765_20261027_v1', 'financial_all_twd_v8/requested_universe.csv', 1)


def default_scope() -> ProjectionScope:
    return ProjectionScope(CHILD, PART, MARKER, 0)


def handoff_item(scope: ProjectionScope | None = None) -> dict:
    scope = scope or default_scope()
    return {'name':scope.name, 'source':'data_tw_index_futures/training_prepared/'+scope.part,
            'catalog_source':'data_tw_index_futures', 'marker':scope.marker,
            'budget_bytes':1, 'seed_hardlinks':False, 'preserve_symlinks':True}


def _sha(path: Path) -> str:
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    return hashlib.sha256(read_stable_bytes(path, maximum=128*1024)).hexdigest()


def _load(path: Path, maximum=128 * 1024) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
        raise SnapshotError('fixed subtree projection evidence is unavailable or redirected')
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise SnapshotError('fixed subtree projection evidence is not an object')
    return value


def _real_directory(path: Path) -> None:
    if path.is_symlink() or not path.is_dir() or path.resolve(strict=True) != path:
        raise SnapshotError('fixed subtree projection directory is unavailable or redirected')


def _same_portable_directory(actual, expected):
    return (stat.S_ISDIR(actual[2]) and stat.S_IMODE(actual[2]) == stat.S_IMODE(expected[2])
            and actual[4] == expected[4] and actual[6:8] == expected[6:8])


def _same_parent(expected, actual):
    if expected == actual:
        return True
    if not isinstance(expected, dict) or not isinstance(actual, dict):
        return False
    left, right = dict(expected), dict(actual)
    before, after = left.pop('parent_index_signature', None), right.pop('parent_index_signature', None)
    if left != right or not isinstance(right.get('parent_index'), str):
        return False
    from stockagent.data_sync.stream_repack import _generation_matches
    return _generation_matches(Path(right['parent_index']), before, after)


def _parent(workspace: Path, scope: ProjectionScope | None = None) -> dict:
    scope = scope or default_scope()
    items = [item for item in queue.load_sources() if item.get('name') == PARENT]
    if len(items) != 1 or items[0].get('source') != 'data_tw_index_futures/training_prepared':
        raise SnapshotError('fixed parent source enrollment differs')
    item = items[0]
    source, destination, evidence, report_path = queue.paths(item, workspace)
    report = queue.verified_report(item, workspace)
    if not isinstance(report, dict):
        raise SnapshotError('fixed parent copy has not completed')
    intent, copy_path, index = _load(evidence/'intent.json'), evidence/'copy-result.json', evidence/'files.sqlite'
    copy = _load(copy_path)
    if (intent.get('contract') != 'workspace_scoped_relocation_v1'
            or intent.get('state') != 'indexed' or intent.get('excluded_subtrees') != []
            or intent.get('preserve_symlinks') is not True
            or intent.get('source') != str(source) or intent.get('destination') != str(destination)
            or copy.get('contract') != 'workspace_scoped_relocation_v1'
            or copy.get('excluded_subtrees') != []
            or copy.get('state') != 'exact_destination_readback_verified'
            or copy.get('all_source_sha256_and_destination_readback_verified') is not True
            or copy.get('selected_source_sha256_and_destination_readback_verified') is not True
            or copy.get('source_namespace_unchanged') is not True
            or copy.get('source_files_deleted') != 0):
        raise SnapshotError('fixed parent copy scope or original proof differs')
    if index.is_symlink() or not index.is_file():
        raise SnapshotError('fixed parent source index is unavailable')
    wal = index.with_name(index.name+'-wal')
    if wal.is_symlink() or wal.exists() and wal.stat().st_size:
        raise SnapshotError('fixed parent source index has an uncheckpointed WAL')
    before = relocation.signature(index)
    observed = report.get('observed_at_epoch')
    if (type(observed) not in (int, float) or observed <= 0 or observed > time.time()+120
            or before[5] > int(observed*1_000_000_000)+2048):
        raise SnapshotError('parent index changed after the historical copy report')
    source, destination = source/scope.part, destination/scope.part
    _real_directory(source)
    _real_directory(destination)
    with sqlite3.connect(f'file:{index}?mode=ro', uri=True) as db:
        root = db.execute('SELECT kind,sig FROM files WHERE path=? COLLATE BINARY', (scope.part,)).fetchone()
        marker = db.execute('SELECT kind,sig,sha,bytes,state FROM files WHERE path=? COLLATE BINARY',
                            (scope.part+'/'+scope.marker,)).fetchone()
    if relocation.signature(index) != before:
        raise SnapshotError('fixed parent index changed during metadata inspection')
    if not root or root[0] != 'directory' or not marker or marker[0] != 'file' or marker[4] != 'verified':
        raise SnapshotError('fixed parent index lacks the exact subtree or marker')
    root_sig, marker_sig = json.loads(root[1]), json.loads(marker[1])
    current_source = relocation.signature(source)
    mapping = handoff._source_device_rebind(source, root_sig, current_source)
    if not handoff._same_source_generation(current_source, root_sig, mapping):
        raise SnapshotError('futures subtree root differs from its fixed historical copy')
    staged = relocation.signature(destination)
    if (not _same_portable_directory(staged, root_sig)
            or staged[5] > int(copy['observed_at_epoch']*1_000_000_000)+2048):
        raise SnapshotError('futures staged subtree root changed after its fixed copy')
    source_marker, staged_marker = relocation.signature(source/scope.marker), relocation.signature(destination/scope.marker)
    if (not handoff._same_source_generation(source_marker, marker_sig, mapping)
            or not relocation.signature_metadata_matches(staged_marker, marker_sig)
            or staged_marker[5] > int(copy['observed_at_epoch']*1_000_000_000)+2048
            or not isinstance(marker[2], str) or re.fullmatch(r'[0-9a-f]{64}', marker[2]) is None
            or marker[3] != marker_sig[3]):
        raise SnapshotError('futures fixed subtree marker generation differs')
    return {'source': str(source), 'destination': str(destination), 'parent_index': str(index),
            'parent_index_signature': before, 'parent_report_sha256': _sha(report_path),
            'parent_copy_sha256': _sha(copy_path), 'source_root_signature': root_sig,
            'source_marker_sha256': marker[2], 'historical_copy_observed_at_epoch': copy['observed_at_epoch'],
            'parent_report_observed_at_epoch': observed}


def validated_projection(workspace: Path, scope: ProjectionScope | None = None) -> dict | None:
    """Validate the fixed projection receipt, without touching payload bytes.

    This remains a historical copy/index proof after a future source handoff;
    callers must separately fence current writers and check C/E generations.
    """
    from stockagent.data_sync.stream_repack import _generation_matches
    scope = scope or default_scope()
    workspace = Path(workspace).absolute()
    evidence = workspace/'migration'/scope.name
    report_path = workspace/'verify'/(scope.name+'.json')
    if not report_path.exists() and not report_path.is_symlink():
        return None
    report = _load(report_path)
    seal = _load(evidence/'sealed-index.json')
    intent = _load(evidence/'intent.json')
    copy = _load(evidence/'copy-result.json')
    parent = report.get('parent')
    source = ROOT/'data_tw_index_futures/training_prepared'/scope.part
    destination = workspace/'downloads'/PARENT/'data_tw_index_futures/training_prepared'/scope.part
    parent_index = workspace/'migration'/PARENT/'files.sqlite'
    parent_report = workspace/'verify'/(PARENT+'.json')
    parent_copy = workspace/'migration'/PARENT/'copy-result.json'
    child_index = evidence/'files.sqlite'
    for path in (parent_index, child_index):
        if path.is_symlink() or not path.is_file():
            raise SnapshotError('projected index is unavailable or redirected')
    if not isinstance(parent, dict):
        raise SnapshotError('projected parent identity is missing')
    if (report.get('contract') != CONTRACT or report.get('state') != 'projected_historical_copy_index'
            or report.get('name') != scope.name or report.get('source') != str(source)
            or report.get('e_destination') != str(destination) or parent.get('source') != str(source)
            or parent.get('destination') != str(destination) or parent.get('parent_index') != str(parent_index)
            or report.get('source_files_deleted') != 0 or report.get('source_alias_changed') is not False
            or report.get('payload_read_during_projection') is not False
            or report.get('independent_destination_readback_during_projection') is not False
            or not _generation_matches(parent_index, parent.get('parent_index_signature'), relocation.signature(parent_index))
            or not _generation_matches(child_index, report.get('index_signature'), relocation.signature(child_index))
            or parent.get('parent_report_sha256') != _sha(parent_report)
            or parent.get('parent_copy_sha256') != _sha(parent_copy)
            or report.get('child_copy_sha256') != _sha(evidence/'copy-result.json')
            or report.get('relocation_evidence_sha256') != report.get('child_copy_sha256')
            or seal.get('contract') != CONTRACT+'.sealed-index'
            or not _same_parent(seal.get('parent'), parent)
            or seal.get('index_sha256') != report.get('index_sha256')
            or any(type(report.get(k)) is not int or report[k] < 0 or seal.get(k) != report[k]
                   for k in ('entries','files','logical_bytes'))
            or report.get('files') <= 0 or report.get('symlink_names_preserved') != scope.symlinks
            or seal.get('symlinks', 0) != scope.symlinks
            or intent.get('contract') != 'workspace_scoped_relocation_v1'
            or intent.get('source') != str(source) or intent.get('destination') != str(destination)
            or intent.get('source_root_signature') != parent.get('source_root_signature')
            or intent.get('preserve_symlinks') is not True or intent.get('excluded_subtrees') != []
            or any(intent.get(k) != report.get(k) for k in ('entries','files','logical_bytes'))
            or copy.get('evidence_scope') != 'derived_subset_of_historical_parent_copy'
            or copy.get('new_payload_readback') is not False
            or copy.get('parent_copy_sha256') != parent.get('parent_copy_sha256')
            or copy.get('observed_at_epoch') != parent.get('historical_copy_observed_at_epoch')
            or any(copy.get(k) != report.get(k) for k in ('files','logical_bytes'))):
        raise SnapshotError('fixed subtree projection evidence differs')
    return report


def project(workspace: Path, *, progress=None, guard=lambda: None, scope: ProjectionScope | None = None) -> dict:
    scope = scope or default_scope()
    workspace = Path(workspace).absolute()
    migration, verify = workspace/'migration', workspace/'verify'
    _real_directory(migration)
    _real_directory(verify)
    descriptor = os.open(migration/(scope.name+'.owner.lock'), os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC, 0o600)
    with os.fdopen(descriptor, 'r+b') as owner:
        info = os.fstat(owner.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size != 0):
            raise SnapshotError('subtree projection owner is not its private empty lock')
        fcntl.flock(owner, fcntl.LOCK_EX|fcntl.LOCK_NB)
        guard()
        parent = _parent(workspace, scope)
        evidence, report_path = migration/scope.name, verify/(scope.name+'.json')
        evidence.mkdir(mode=0o700, exist_ok=True)
        _real_directory(evidence)
        expected_intent = {'contract': CONTRACT+'.intent', 'child': scope.name, 'parent': parent}
        intent_path, index_path = evidence/'projection-intent.json', evidence/'files.sqlite'
        if any(path.is_symlink() for path in (intent_path, report_path, evidence/'sealed-index.json',
                                              evidence/'copy-result.json', evidence/'intent.json')):
            raise SnapshotError('subtree projection control is redirected')
        if intent_path.exists():
            previous_intent = _load(intent_path)
            if (previous_intent.get('contract') != expected_intent['contract']
                    or previous_intent.get('child') != scope.name
                    or not _same_parent(previous_intent.get('parent'), parent)):
                raise SnapshotError('subtree projection belongs to a different parent generation')
        else:
            if any(evidence.iterdir()):
                raise SnapshotError('subtree projection directory contains an unexpected name')
            atomic_write_json(intent_path, expected_intent, durable=True)
        if report_path.exists():
            result = _load(report_path)
            from stockagent.data_sync.stream_repack import _generation_matches
            if (result.get('contract') != CONTRACT or not _same_parent(result.get('parent'), parent)
                    or result.get('source_files_deleted') != 0 or result.get('source_alias_changed') is not False
                    or result.get('payload_read_during_projection') is not False
                    or not _generation_matches(index_path, result.get('index_signature'), relocation.signature(index_path))
                    or result.get('child_copy_sha256') != _sha(evidence/'copy-result.json')):
                raise SnapshotError('completed subtree projection identity differs')
            return result
        partial = evidence/'files.sqlite.partial'
        partial_journal = evidence/'files.sqlite.partial-journal'
        seal_path = evidence/'sealed-index.json'
        start = scope.part+'/'
        upper = scope.part+'0'
        if seal_path.exists():
            seal = _load(seal_path)
            if (seal.get('contract') != CONTRACT+'.sealed-index' or not _same_parent(seal.get('parent'), parent)
                    or not isinstance(seal.get('index_sha256'), str)
                    or re.fullmatch(r'[0-9a-f]{64}', seal['index_sha256']) is None
                    or seal.get('symlinks', 0) != scope.symlinks
                    or any(type(seal.get(k)) is not int or seal[k] < 0 for k in ('entries','files','logical_bytes'))):
                raise SnapshotError('sealed subtree index identity differs')
            candidate = index_path if index_path.exists() else partial
            if candidate.is_symlink() or not candidate.is_file():
                raise SnapshotError('sealed subtree index is missing or redirected')
            old = candidate.lstat()
            old_signature = relocation.signature(candidate)
            if (not stat.S_ISREG(old.st_mode) or old.st_uid != os.geteuid() or old.st_nlink != 1
                    or relocation.digest(candidate, no_follow=True) != seal['index_sha256']
                    or relocation.signature(candidate) != old_signature):
                raise SnapshotError('sealed subtree index changed during recovery')
            if candidate == partial:
                if index_path.exists() or index_path.is_symlink():
                    raise SnapshotError('sealed subtree index destination already exists')
                os.rename(partial, index_path)
                _fsync_directory(evidence)
            entries, files, logical = seal['entries'], seal['files'], seal['logical_bytes']
        else:
            if index_path.exists() or index_path.is_symlink():
                raise SnapshotError('subtree projection has an unsealed index')
            previous = partial.exists() or partial.is_symlink()
            scratch = [path for path in (partial, partial_journal) if path.exists() or path.is_symlink()]
            if scratch:
                from stockagent.data_sync.materialized_cache import process_references_many
                for path in scratch:
                    info = path.lstat()
                    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
                        raise SnapshotError('interrupted projection scratch is not its owned regular file')
                if process_references_many(scratch):
                    raise BlockingIOError('interrupted projection scratch is still used by a process')
            if not previous:
                if partial_journal.exists() or partial_journal.is_symlink():
                    raise SnapshotError('projection journal has no matching database')
                fd = os.open(partial, os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW, 0o600)
                os.close(fd)
            with sqlite3.connect(partial, uri=True) as db:
                db.execute('PRAGMA synchronous=FULL')
                db.execute('PRAGMA journal_mode=DELETE')
                db.execute('PRAGMA temp_store=MEMORY')
                db.execute('ATTACH DATABASE ? AS original', (Path(parent['parent_index']).as_uri()+'?mode=ro',))
                if previous:
                    tables = {row[0] for row in db.execute("SELECT name FROM main.sqlite_master WHERE type='table'")}
                    if 'projection' not in tables:
                        if tables - {'files'} or 'files' in tables and db.execute('SELECT count(*) FROM files').fetchone()[0]:
                            raise SnapshotError('uninitialized subtree index contains unexpected records')
                        db.execute('DROP TABLE IF EXISTS files')
                        db.commit()
                        previous = False
                allowed = "('file','directory','symlink')" if scope.symlinks else "('file','directory')"
                forbidden = db.execute("SELECT path FROM original.files WHERE path>=? AND path<? AND "
                    f"(kind NOT IN {allowed} OR instr(lower(path),'factorized')>0) LIMIT 1", (start, upper)).fetchone()
                if forbidden:
                    raise SnapshotError('subtree includes factorized or unsupported members')
                expected = db.execute('SELECT count(*) FROM original.files WHERE path>=? AND path<?', (start, upper)).fetchone()[0]
                if not previous:
                    db.execute('CREATE TABLE files (path TEXT PRIMARY KEY,kind TEXT,sig TEXT,sha TEXT,bytes INTEGER,state TEXT) WITHOUT ROWID')
                    db.execute('CREATE TABLE projection (id INTEGER PRIMARY KEY CHECK(id=1), parent TEXT NOT NULL, '
                               'expected INTEGER NOT NULL, last_path TEXT NOT NULL, completed INTEGER NOT NULL)')
                    db.execute('INSERT INTO projection VALUES (1,?,?,?,0)', (json.dumps(parent,sort_keys=True),expected,''))
                    db.commit()
                    last_path, completed = '', 0
                else:
                    saved = db.execute('SELECT parent,expected,last_path,completed FROM projection WHERE id=1').fetchone()
                    if (not saved or not _same_parent(json.loads(saved[0]), parent) or saved[1] != expected
                            or not isinstance(saved[2], str) or type(saved[3]) is not int
                            or not 0 <= saved[3] <= expected
                            or (saved[3] == 0) != (saved[2] == '')
                            or saved[2] and not (start <= saved[2] < upper)):
                        raise SnapshotError('interrupted subtree projection checkpoint differs')
                    last_path, completed = saved[2], saved[3]
                    prior = db.execute('SELECT path,kind,sig,sha,bytes,state FROM files ORDER BY path COLLATE BINARY')
                    fixed = db.execute('SELECT substr(path,?),kind,sig,sha,bytes,state FROM original.files '
                        'WHERE path>=? AND path<? AND path<=? ORDER BY path COLLATE BINARY',
                        (len(start)+1, start, upper, last_path))
                    for checked, (a, b) in enumerate(zip_longest(prior, fixed), 1):
                        if a is None or b is None or a != b or checked > completed:
                            raise SnapshotError('interrupted subtree index differs from its fixed parent')
                        if checked % 4096 == 0:
                            guard()
                            if progress:
                                progress({'stage':'rechecking_committed_metadata', 'completed_items':checked,
                                          'total_items':completed})
                    actual = db.execute('SELECT count(*) FROM files').fetchone()[0]
                    if actual != completed:
                        raise SnapshotError('interrupted subtree index count differs from its checkpoint')
                if progress:
                    progress({'stage':'indexing_existing_metadata', 'completed_items':completed,'total_items':expected})
                while completed < expected:
                    guard()
                    names = db.execute('SELECT path FROM original.files WHERE path>=? AND path<? AND path>? '
                                       'ORDER BY path COLLATE BINARY LIMIT ?', (start,upper,last_path,BATCH_ENTRIES)).fetchall()
                    if not names:
                        raise SnapshotError('parent index ended before its fixed subtree count')
                    boundary = names[-1][0]
                    with db:
                        cursor = db.execute('INSERT INTO files SELECT substr(path,?),kind,sig,sha,bytes,state '
                            'FROM original.files WHERE path>=? AND path<? AND path>? AND path<=? '
                            'ORDER BY path COLLATE BINARY', (len(start)+1,start,upper,last_path,boundary))
                        if cursor.rowcount != len(names):
                            raise SnapshotError('projected subtree batch count differs')
                        completed += len(names)
                        db.execute('UPDATE projection SET last_path=?,completed=? WHERE id=1', (boundary,completed))
                    last_path = boundary
                    if progress:
                        progress({'stage':'indexing_existing_metadata','completed_items':completed,'total_items':expected})
                entries, files, logical, symlinks, invalid = db.execute(
                    "SELECT count(*),coalesce(sum(kind='file'),0),coalesce(sum(CASE WHEN kind='file' THEN bytes ELSE 0 END),0),"
                    "coalesce(sum(kind='symlink'),0),"
                    "coalesce(sum(CASE WHEN kind IN ('file','symlink') AND (state IS NULL OR state!='verified' "
                    "OR sha IS NULL OR length(sha)!=64 OR bytes IS NULL OR bytes<0 "
                    "OR (kind='symlink' AND bytes!=0)) THEN 1 ELSE 0 END),0) FROM files").fetchone()
                if entries != expected or invalid or not files or symlinks != scope.symlinks:
                    raise SnapshotError('derived subtree index is incomplete or contains unverified files')
                guard()
            if not _same_parent(parent, _parent(workspace, scope)):
                raise SnapshotError('parent copy or subtree root changed during index projection')
            with partial.open('rb') as handle:
                os.fsync(handle.fileno())
            index_before = relocation.signature(partial)
            last_report = 0.0
            def hashing(n):
                nonlocal last_report
                guard()
                now = time.monotonic()
                if progress and (n == index_before[3] or now-last_report >= 1):
                    progress({'stage':'sealing_index_sha','completed_bytes':n,'total_bytes':index_before[3]})
                    last_report = now
            index_sha = relocation.digest(partial, no_follow=True, progress=hashing)
            if relocation.signature(partial) != index_before:
                raise SnapshotError('subtree index changed while sealing its metadata')
            seal = {'contract':CONTRACT+'.sealed-index', 'parent':parent, 'index_sha256':index_sha,
                    'entries':entries, 'files':files, 'logical_bytes':logical}
            if scope.symlinks:
                seal['symlinks'] = symlinks
            atomic_write_json(seal_path, seal, durable=True)
            os.rename(partial, index_path)
            _fsync_directory(evidence)
        guard()
        if not _same_parent(parent, _parent(workspace, scope)):
            raise SnapshotError('parent copy or subtree root changed before projection completion')
        copy = {'contract': 'workspace_scoped_relocation_v1', 'state': 'exact_destination_readback_verified',
                    'files': files, 'logical_bytes': logical, 'source_files_deleted': 0,
                    'all_source_sha256_and_destination_readback_verified': True,
                    'selected_source_sha256_and_destination_readback_verified': True,
                    'excluded_subtrees': [], 'source_namespace_unchanged': True,
                    'observed_at_epoch': parent['historical_copy_observed_at_epoch'],
                    'evidence_scope': 'derived_subset_of_historical_parent_copy',
                    'new_payload_readback': False, 'parent_copy_sha256': parent['parent_copy_sha256']}
        atomic_write_json(evidence/'copy-result.json', copy, durable=True)
        atomic_write_json(evidence/'intent.json', {'contract':'workspace_scoped_relocation_v1',
                'state':'indexed', 'source':parent['source'], 'destination':parent['destination'],
                'source_root_signature':parent['source_root_signature'], 'preserve_symlinks':True,
                'excluded_subtrees':[], 'entries':entries, 'files':files, 'logical_bytes':logical,
                'derived_from_parent_index':parent['parent_index_signature']}, durable=True)
        result = {'contract': CONTRACT, 'state':'projected_historical_copy_index', 'name':scope.name,
                'parent':parent, 'source':parent['source'], 'e_destination':parent['destination'],
                'files':files, 'logical_bytes':logical, 'entries':entries, 'symlink_names_preserved':scope.symlinks,
                'source_marker_relative':scope.marker, 'source_marker_sha256':parent['source_marker_sha256'],
                'relocation_evidence':str(evidence/'copy-result.json'),
                'relocation_evidence_sha256':_sha(evidence/'copy-result.json'),
                'child_copy_sha256':_sha(evidence/'copy-result.json'),
                'index_signature':relocation.signature(index_path), 'index_sha256':seal['index_sha256'],
                'source_files_deleted':0,
                'source_alias_changed':False, 'payload_read_during_projection':False,
                'independent_destination_readback_during_projection':False,
                'observed_at_epoch':time.time()}
        atomic_write_json(report_path, result, durable=True)
        if progress:
            progress({'stage':'completed', 'completed_items':entries, 'total_items':entries})
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scope', choices=('725', '765'), default='725')
    scope = FUTURES_765 if parser.parse_args().scope == '765' else default_scope()
    config = volume.configuration()
    volume.check(c=config)
    workspace = Path(config['mount_point'])
    status = workspace/'migration'/(scope.name+'.status.json')
    started_at = time.time()
    parent_identity = None
    def report(event):
        atomic_write_json(status, {'contract':CONTRACT, 'state':event['stage'],
                                   'owner_pid':os.getpid(), 'started_at_epoch':started_at,
                                   'parent_report_sha256':parent_identity, 'progress':event,
                                   'observed_at_epoch':time.time()})
    try:
        parent = _parent(workspace, scope)
        parent_identity = parent['parent_report_sha256']
        budget = 2 * Path(parent['parent_index']).stat().st_size + 64*1024**2
        leases = workspace/'migration/provider-state-temporary-20261103/capacity-leases'
        guard = handoff._source_work_window_guard()
        if not handoff.binance_handoff_window_allowed(for_start=True):
            raise BlockingIOError('tw_day_trade_priority_window')
        key = hashlib.sha256((scope.name+parent['parent_report_sha256']).encode()).hexdigest()
        with reserve_capacity(leases, key=key, maximum_growth_bytes=budget,
                              capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
            volume.admit(budget+lease['other_reserved_growth_bytes'], c=config)
            result = project(workspace, progress=report, guard=guard, scope=scope)
    except BlockingIOError as error:
        report({'stage':'waiting', 'reason_code':'tw_day_trade_priority_window'
                if str(error) == 'tw_day_trade_priority_window' else 'owner_busy'})
        raise
    except (OSError, ValueError, sqlite3.Error, SnapshotError) as error:
        report({'stage':'blocked', 'reason_code':'projection_precondition', 'error_type':type(error).__name__})
        raise
    print(json.dumps({key:result[key] for key in ('state','name','files','logical_bytes','entries')}, ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except BlockingIOError as error:
        print(f'waiting: {error}', file=sys.stderr)
        raise SystemExit(75)
    except (OSError, ValueError, sqlite3.Error, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
