"""Refresh one source-bound subtree of an uncommitted workspace relocation.

The caller must fence the original writer(s), hold their native locks, and
establish that the old copy owner has released the three trees. This helper
preserves the previous payload and index. Its result still requires the full
canonical source/destination verification before a workspace is admitted.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time
import uuid

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from scripts.relocate_root_backup_alias import exchange_names
from stockagent.data_sync import workspace_relocation as relocation

CONTRACT = 'workspace_relocation_subtree_reconciliation_v1'


def _receipt(path, value):
    atomic_write_json(path, value, durable=True)


def _copy_exact(source: Path, target: Path, expected_sha: str):
    """Retain the old evidence bytes; never replace a prior backup."""
    if target.exists():
        if relocation.digest(target, no_follow=True) != expected_sha:
            raise ValueError('existing reconciliation backup has different bytes')
        return
    temporary = target.with_name(target.name+'.partial-'+uuid.uuid4().hex)
    before = relocation.signature(source)
    with source.open('rb') as reader, temporary.open('xb') as writer:
        shutil.copyfileobj(reader, writer, length=8*1024**2)
        writer.flush()
        os.fsync(writer.fileno())
    if (relocation.signature(source) != before or relocation.digest(temporary, no_follow=True) != expected_sha
            or relocation.digest(source, no_follow=True) != expected_sha):
        raise ValueError('original index changed during exact backup')
    os.link(temporary, target, follow_symlinks=False)
    temporary.unlink()
    relocation.sync_filesystem(target.parent)


def _subtree_counts(db, prefix):
    return db.execute("SELECT count(*),coalesce(sum(kind='file'),0),coalesce(sum(bytes),0) FROM files "
                      "WHERE path >= ? AND path < ?", (prefix+'/', prefix+'0')).fetchone()


def _not_mount(target):
    with Path('/proc/self/mountinfo').open('rb') as stream:
        raw = stream.read(1024**2+1)
    if len(raw) > 1024**2:
        raise ValueError('mount census exceeded its bound')
    path = str(target).encode()
    return not any((point := line.split()[4]) == path or point.startswith(path+b'/')
                   for line in raw.splitlines())


def reconcile_subtree(source: Path, destination: Path, evidence: Path, subtree: str, journal: Path,
                      *, workers=8, sync_batch_files=512):
    source, destination, evidence, journal = (Path(p).absolute() for p in (source, destination, evidence, journal))
    relative = Path(subtree)
    if (not subtree or len(relative.parts) != 1 or relative.is_absolute() or '..' in relative.parts
            or str(relative) != subtree):
        raise ValueError('reconciliation requires one exact source subtree')
    if (source.resolve(strict=True) != source or destination.resolve(strict=True) != destination
            or evidence.resolve(strict=True) != evidence or journal.is_relative_to(destination)):
        raise ValueError('reconciliation paths must be unredirected and disjoint')
    target, origin = destination/relative, source/relative
    if (origin.is_symlink() or target.is_symlink() or not origin.is_dir() or not target.is_dir()
            or origin.resolve() != origin or target.resolve() != target or not _not_mount(target)):
        raise ValueError('source or original target subtree is redirected or mounted')
    journal.mkdir(mode=0o700, parents=True, exist_ok=True)
    if journal.stat().st_dev != destination.stat().st_dev:
        raise ValueError('the retained payload and replacement must share the destination filesystem')
    plan_path, old_intent_path = journal/'reconciliation.json', journal/'intent-before.json'
    main_intent, main_index = evidence/'intent.json', evidence/'files.sqlite'
    stage, stage_evidence, backup = journal/'staged-payload', journal/'staged-evidence', journal/'index-before.sqlite'
    if plan_path.exists():
        plan = json.loads(plan_path.read_bytes())
        if (plan.get('contract') != CONTRACT or plan.get('source') != str(source)
                or plan.get('destination') != str(destination) or plan.get('evidence') != str(evidence)
                or plan.get('subtree') != subtree):
            raise ValueError('reconciliation journal identity differs')
        original_bytes = old_intent_path.read_bytes()
        if hashlib.sha256(original_bytes).hexdigest() != plan.get('old_intent_sha256'):
            raise ValueError('retained original intent changed')
        original = json.loads(original_bytes)
    else:
        if (evidence/'copy-result.json').exists():
            raise ValueError('a completed relocation is not an uncommitted copy')
        original_bytes = main_intent.read_bytes()
        original = json.loads(original_bytes)
        if (original.get('state') != 'indexed' or original.get('source') != str(source)
                or original.get('destination') != str(destination)
                or relocation.signature(source) != original.get('source_root_signature')
                or original.get('excluded_subtrees', [])):
            raise ValueError('original relocation source or scope differs')
        wal = main_index.with_name(main_index.name+'-wal')
        if wal.exists() and wal.stat().st_size:
            raise ValueError('the previous index still has a live WAL; preserve it')
        with closing(sqlite3.connect(main_index.as_uri()+'?mode=ro', uri=True)) as db:
            pending = db.execute("SELECT count(*) FROM files WHERE kind='file' AND state!='verified'").fetchone()[0]
            root = db.execute('SELECT kind,sig FROM files WHERE path=?', (subtree,)).fetchone()
            old_counts = _subtree_counts(db, subtree)
        if pending or root is None or root[0] != 'directory':
            raise ValueError('the old copy has unfinished files or an unknown subtree')
        plan = {'contract': CONTRACT, 'state': 'planned', 'source': str(source), 'destination': str(destination),
                'evidence': str(evidence), 'subtree': subtree, 'old_index_sha256': relocation.digest(main_index),
                'old_intent_sha256': hashlib.sha256(original_bytes).hexdigest(),
                'old_subtree_counts': list(old_counts), 'old_target_identity': relocation.signature(target)[:2],
                'started_at_epoch': time.time(), 'source_files_deleted': 0}
        if old_intent_path.exists():
            if old_intent_path.read_bytes() != original_bytes:
                raise ValueError('retained original intent differs')
        else:
            atomic_write_bytes(old_intent_path, original_bytes, durable=True, mode=0o600)
        _receipt(plan_path, plan)
    if plan.get('state') == 'complete':
        if (relocation.signature(source) != original['source_root_signature']
                or relocation.signature(origin) != plan['source_subtree_signature']
                or relocation.signature(target)[:2] != plan['replacement_identity']
                or relocation.signature(stage)[:2] != plan['old_target_identity']
                or relocation.digest(main_index, no_follow=True) != plan['updated_index_sha256']
                or relocation.digest(main_intent, no_follow=True) != plan['updated_intent_sha256']):
            raise ValueError('completed reconciliation no longer describes the current generation')
        return plan

    if plan['state'] == 'planned':
        _copy_exact(main_index, backup, plan['old_index_sha256'])
        replacement = relocation.copy(origin, stage, stage_evidence, workers=workers,
            preserve_symlinks=True, sync_batch_files=sync_batch_files)
        if not replacement['all_source_sha256_and_destination_readback_verified']:
            raise ValueError('replacement subtree lacks full independent readback')
        child_intent = json.loads((stage_evidence/'intent.json').read_bytes())
        if (relocation.signature(source) != original['source_root_signature']
                or relocation.signature(origin) != child_intent['source_root_signature']
                or relocation.signature(target)[:2] != plan['old_target_identity']
                or relocation.digest(main_index) != plan['old_index_sha256']):
            raise ValueError('source, target or original index changed before exchange')
        plan.update(state='prepared', source_subtree_signature=child_intent['source_root_signature'],
                    replacement_identity=relocation.signature(stage)[:2],
                    replacement_copy_result_sha256=relocation.digest(stage_evidence/'copy-result.json'),
                    replacement_index_sha256=relocation.digest(stage_evidence/'files.sqlite', no_follow=True),
                    replacement_entries=child_intent['entries'], replacement_files=child_intent['files'],
                    replacement_bytes=child_intent['logical_bytes'])
        _receipt(plan_path, plan)

    if plan['state'] == 'prepared':
        if relocation.signature(source) != original['source_root_signature'] or relocation.signature(origin) != plan['source_subtree_signature']:
            raise ValueError('source generation changed before exchange')
        stage_id, target_id = relocation.signature(stage)[:2], relocation.signature(target)[:2]
        if (stage_id, target_id) == (plan['replacement_identity'], plan['old_target_identity']):
            exchange_names(stage, target)
            relocation.sync_filesystem(destination)
        elif (stage_id, target_id) != (plan['old_target_identity'], plan['replacement_identity']):
            raise ValueError('retained and replacement subtree identities differ')
        plan.update(state='exchanged', exchanged_at_epoch=time.time())
        _receipt(plan_path, plan)

    if plan['state'] == 'exchanged':
        if (relocation.signature(target)[:2] != plan['replacement_identity']
                or relocation.signature(stage)[:2] != plan['old_target_identity']
                or relocation.signature(source) != original['source_root_signature']
                or relocation.signature(origin) != plan['source_subtree_signature']
                or relocation.digest(stage_evidence/'copy-result.json') != plan['replacement_copy_result_sha256']
                or relocation.digest(stage_evidence/'files.sqlite', no_follow=True) != plan['replacement_index_sha256']
                or relocation.digest(backup, no_follow=True) != plan['old_index_sha256']):
            raise ValueError('reconciliation evidence changed after exchange')
        if relocation.digest(main_index, no_follow=True) != plan['old_index_sha256']:
            raise ValueError('original evidence index changed before preparation')
        candidate = journal/('index-candidate-'+uuid.uuid4().hex+'.sqlite')
        _copy_exact(backup, candidate, plan['old_index_sha256'])
        child_index = stage_evidence/'files.sqlite'
        with closing(sqlite3.connect(candidate)) as db:
            db.execute('ATTACH DATABASE ? AS replacement', (str(child_index),))
            db.execute('BEGIN IMMEDIATE')
            root = db.execute('SELECT kind FROM files WHERE path=?', (subtree,)).fetchone()
            if root != ('directory',):
                raise ValueError('original indexed subtree is missing')
            db.execute('DELETE FROM files WHERE path >= ? AND path < ?', (subtree+'/', subtree+'0'))
            db.execute('UPDATE files SET sig=? WHERE path=?', (json.dumps(plan['source_subtree_signature']), subtree))
            db.execute("INSERT INTO files(path,kind,sig,sha,bytes,state) "
                       "SELECT ? || path,kind,sig,sha,bytes,state FROM replacement.files", (subtree+'/',))
            db.commit()
            checkpoint = db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
            if checkpoint[0] != 0:
                raise ValueError('updated evidence index could not be checkpointed')
        relocation.sync_filesystem(journal)
        plan.update(state='index_prepared', candidate_index=str(candidate),
                    candidate_index_sha256=relocation.digest(candidate, no_follow=True))
        _receipt(plan_path, plan)

    if plan['state'] == 'index_prepared':
        if (relocation.signature(target)[:2] != plan['replacement_identity']
                or relocation.signature(stage)[:2] != plan['old_target_identity']
                or relocation.signature(source) != original['source_root_signature']
                or relocation.signature(origin) != plan['source_subtree_signature']
                or relocation.digest(backup, no_follow=True) != plan['old_index_sha256']):
            raise ValueError('source or retained proof changed before index admission')
        candidate = Path(plan['candidate_index'])
        if not candidate.is_relative_to(journal) or candidate.parent != journal:
            raise ValueError('candidate index left the reconciliation journal')
        main_sha = relocation.digest(main_index, no_follow=True)
        if main_sha == plan['old_index_sha256']:
            if relocation.digest(candidate, no_follow=True) != plan['candidate_index_sha256']:
                raise ValueError('candidate index changed before admission')
            os.replace(candidate, main_index)
            relocation.sync_filesystem(evidence)
        elif main_sha != plan['candidate_index_sha256']:
            raise ValueError('evidence index has an unrecognized generation')
        old_entries, old_files, old_bytes = plan['old_subtree_counts']
        updated = dict(original)
        updated.update(entries=original['entries']-old_entries+plan['replacement_entries'],
                       files=original['files']-old_files+plan['replacement_files'],
                       logical_bytes=original['logical_bytes']-old_bytes+plan['replacement_bytes'])
        updated['source_reconciliations'] = [*original.get('source_reconciliations', []), {
            'subtree': subtree, 'journal': str(plan_path),
            'replacement_copy_result_sha256': plan['replacement_copy_result_sha256']}]
        _receipt(main_intent, updated)
        if relocation.signature(source) != original['source_root_signature'] or relocation.signature(origin) != plan['source_subtree_signature']:
            raise ValueError('source changed before final reconciliation receipt')
        plan.update(state='complete', updated_index_sha256=relocation.digest(main_index, no_follow=True),
                    updated_intent_sha256=relocation.digest(main_intent, no_follow=True),
                    retained_previous_payload=str(stage), retained_previous_index=str(backup),
                    completed_at_epoch=time.time(), full_relocation_still_requires_verification=True)
        _receipt(plan_path, plan)
    return plan
