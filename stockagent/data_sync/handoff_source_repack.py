"""Seal an unchanged C handoff original to E without another payload copy.

The original owner remains responsible for the source lock and mount guard.
Every regular file is compared with the fixed D inventory while it is written
to a dataset-local pack. No D publication, source deletion, peer delivery or
independent payload readback is implied by an E preparation.
"""
from __future__ import annotations

from contextlib import ExitStack, closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid
from itertools import zip_longest

from stockagent.data_sync import packed_snapshots as p, stream_repack as s, workspace_relocation as relocation
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_metadata import MetadataRows
from stockagent.data_sync.materialized_cache import process_references_many


def _index_connection(index: Path):
    if index.is_symlink() or not index.is_file() or (index.with_name(index.name+'-wal').exists()
            and index.with_name(index.name+'-wal').stat().st_size):
        raise SnapshotError('fixed source index is redirected or has uncheckpointed writes')
    return sqlite3.connect(f'file:{index}?mode=ro', uri=True)


def _source_index_rebind(source: Path, index: Path, original_inode: int):
    """Bind an old index device number to this same guarded C source inode."""
    from stockagent.data_sync import workspace_source_handoff as handoff
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    try:
        intent = json.loads(read_stable_bytes(index.parent/'intent.json', maximum=128 * 1024))
        recorded = intent.get('source_root_signature')
        current = relocation.signature(source)
        contract = intent.get('contract')
        if contract == 'workspace_scoped_relocation_v1':
            # A generic scoped copy is not a handoff. The futures projections
            # and the TAIFEX complete-source copy each require their exact
            # enrolled index, retained root and source-generation evidence.
            from scripts import project_futures_725_workspace_index as projection
            from stockagent.data_sync import workspace_volume
            workspace = Path(workspace_volume.configuration()['mount_point'])
            pairs = ((handoff.FUTURES_725_NAME, projection.default_scope()),
                     (handoff.FUTURES_765_NAME, projection.FUTURES_765))
            selected = next(((handoff._registered_scope(name), projected) for name, projected in pairs
                             if source == handoff._registered_scope(name).backup
                             and index == workspace/'migration'/projected.name/'files.sqlite'), None)
            if intent.get('excluded_subtrees') != [] or intent.get('preserve_symlinks') is not True:
                return None
            if selected is not None:
                if (intent.get('source') != str(selected[0].source)
                        or projection.validated_projection(workspace, scope=selected[1]) is None):
                    return None
            else:
                from scripts import run_workspace_source_staging as staging
                scope = handoff._registered_scope(handoff.TAIFEX_PUBLIC_NAME)
                if source != scope.backup or intent.get('source') != str(scope.source):
                    return None
                with handoff.selected_source(handoff.TAIFEX_PUBLIC_NAME):
                    item, bound_workspace, _destination, evidence, _report = handoff.enrolled()
                    value = handoff.read_journal()
                report = staging.verified_report(item, bound_workspace)
                if (bound_workspace != workspace or index != evidence/'files.sqlite'
                        or not isinstance(value, dict) or value.get('state') != 'active'
                        or value.get('original_inode') != original_inode
                        or not isinstance(report, dict)
                        or value.get('copy_result_sha256') != report.get('relocation_evidence_sha256')):
                    return None
        elif contract != 'workspace_exact_relocation_v1':
            return None
        if (not isinstance(recorded, list) or len(recorded) != 9
                or recorded[1] != original_inode or current[1] != original_inode):
            return None
        return handoff._source_device_rebind(source, recorded, current)
    except (OSError, ValueError, TypeError, KeyError, SnapshotError):
        return None


def _validated_source_overrides(dataset: str, db, overrides):
    """Bind the two reviewed news sidecar clock changes to the fixed index."""
    from stockagent.data_sync.workspace_source_handoff import NEWS_SIDECAR_PATHS
    if overrides is None:
        return {}
    if (dataset != 'news-training-workspace' or not isinstance(overrides, dict)
            or not overrides or not set(overrides) <= NEWS_SIDECAR_PATHS):
        raise SnapshotError('source generation overrides are outside the reviewed news sidecars')
    for name, actual in overrides.items():
        row = db.execute('SELECT kind,sig,sha,bytes,state FROM files WHERE path=? COLLATE BINARY',
                         (name,)).fetchone()
        if row is None:
            raise SnapshotError('source generation override is absent from the fixed index')
        kind, encoded, digest, size, state = row
        old = json.loads(encoded)
        if (kind != 'file' or state != 'verified' or not isinstance(actual, list) or len(actual) != 9
                or not isinstance(old, list) or len(old) != 9
                or any(type(x) is not int or x < 0 for x in actual)
                or any(actual[i] != old[i] for i in (0, 1, 2, 3, 6, 7, 8))
                or size != old[3] or size > 32768):
            raise SnapshotError('source generation override differs beyond sidecar clocks')
        p._validate_hash(digest, 'fixed news sidecar SHA')
    return overrides


def _checked_internal_symlink(source: Path, db, name: str, digest, *, check_target=True) -> str:
    """Bind link text to the fixed index without following an external target."""
    path = source/p._safe_relative_path(name, 'indexed symlink')
    before = relocation.signature(path)
    if not stat.S_ISLNK(before[2]):
        raise SnapshotError('indexed source symlink is no longer a symlink')
    target = os.readlink(path)
    p._validate_symlink_target(name, target)
    if hashlib.sha256(os.fsencode(target)).hexdigest() != p._validate_hash(digest, 'indexed symlink SHA'):
        raise SnapshotError('indexed source symlink target changed')
    if check_target:
        try:
            resolved = path.resolve(strict=True)
            relative = resolved.relative_to(source.resolve(strict=True))
        except (OSError, RuntimeError, ValueError) as error:
            raise SnapshotError('indexed source symlink target is missing or outside its root') from error
        if relative != Path('.'):
            member = db.execute('SELECT kind,state FROM files WHERE path=? COLLATE BINARY',
                                (relative.as_posix(),)).fetchone()
            if not member or member[0] not in {'file', 'directory'} or member[0] == 'file' and member[1] != 'verified':
                raise SnapshotError('indexed source symlink target is not in the fixed source')
    if relocation.signature(path) != before or os.readlink(path) != target:
        raise SnapshotError('indexed source symlink changed during inspection')
    return target


def _check_source(source: Path, db, *, original_inode: int, original_lock_inode: int,
                  supplemental_root_lock: bool = False, source_rebind=None, source_overrides=None,
                  progress=None, preserve_internal_symlinks: bool = False):
    from stockagent.data_sync.workspace_source_handoff import _same_source_generation
    info = source.lstat()
    if source.is_symlink() or not stat.S_ISDIR(info.st_mode) or info.st_ino != original_inode:
        raise SnapshotError('retained source root identity changed')
    expected_count = db.execute('SELECT count(*) FROM files').fetchone()[0]
    seen = 0
    if progress:
        progress(0, expected_count)
    for relative, kind, actual in relocation.walk(source, preserve_symlinks=preserve_internal_symlinks):
        row = db.execute('SELECT kind,sig,sha,bytes,state FROM files WHERE path=? COLLATE BINARY',
                         (relative,)).fetchone()
        if (row is None and supplemental_root_lock and relative == '.download.lock'
                and kind == 'file' and actual[1] == original_lock_inode and actual[3] == 0):
            continue
        if row is None or row[0] != kind:
            raise SnapshotError('retained source namespace differs from fixed copy index')
        expected = (source_overrides or {}).get(relative, json.loads(row[1]))
        if relative == '.download.lock':
            if kind != 'file' or actual[1] != original_lock_inode:
                raise SnapshotError('retained source lock identity changed')
        elif not _same_source_generation(actual, expected, source_rebind):
            raise SnapshotError('retained source generation changed since fixed copy')
        if kind == 'symlink':
            if not preserve_internal_symlinks or row[3] != 0 or row[4] != 'verified':
                raise SnapshotError('retained source symlink is not covered by the fixed copy')
            _checked_internal_symlink(source, db, relative, row[2])
        seen += 1
        if progress and seen % 1024 == 0:
            progress(seen, expected_count)
    if seen != expected_count:
        raise SnapshotError('retained source namespace is incomplete')
    if progress:
        progress(seen, expected_count)


def _create_hardlink_plan(db):
    """Count only complete, in-scope inode groups, never an external link.

    This table is part of the existing per-source retirement journal.  A
    hardlinked inode contributes its allocation once, at its final pathname.
    """
    db.execute('CREATE TABLE hardlink_members (path TEXT PRIMARY KEY, device INTEGER NOT NULL, '
               'inode INTEGER NOT NULL, links INTEGER NOT NULL, sha TEXT NOT NULL) WITHOUT ROWID')
    db.execute('CREATE INDEX hardlink_members_group ON hardlink_members(device,inode)')
    db.execute('CREATE TABLE hardlink_groups (device INTEGER NOT NULL, inode INTEGER NOT NULL, '
               'links INTEGER NOT NULL, sha TEXT NOT NULL, ctime INTEGER NOT NULL, '
               'PRIMARY KEY(device,inode)) WITHOUT ROWID')


def _finish_hardlink_plan(db, *, preserve_external=False):
    groups = db.execute('SELECT device,inode,count(*),min(links),max(links),min(sha),max(sha),max(path) '
                        'FROM hardlink_members GROUP BY device,inode').fetchall()
    for device, inode, count, low, high, sha_low, sha_high, last in groups:
        if (count < 1 or count > low or low != high or sha_low != sha_high
                or count < low and not preserve_external):
            raise SnapshotError('retained source hardlinks extend outside the retirement scope')
        row = db.execute('SELECT sig FROM files WHERE path=?', (last,)).fetchone()
        signature = json.loads(row[0])
        db.execute('INSERT INTO hardlink_groups VALUES (?,?,?,?,?)',
                   (device, inode, low, sha_low, signature[5]))
        if count < low:
            # An out-of-scope name keeps the inode allocated; do not count any
            # of this source's unlinks as reclaimed space.
            db.execute('UPDATE files SET allocated=0 WHERE path IN '
                       '(SELECT path FROM hardlink_members WHERE device=? AND inode=?)', (device, inode))
        else:
            db.execute('UPDATE files SET allocated=0 WHERE path IN '
                       '(SELECT path FROM hardlink_members WHERE device=? AND inode=? AND path<>?)',
                       (device, inode, last))


def _validate_hardlink_plan(db, *, preserve_external=False):
    """An older journal has no link tables and was allowed only nlink=1."""
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    selected = db.execute("SELECT count(*) FROM files WHERE json_extract(sig,'$[8]')>1").fetchone()[0]
    if not {'hardlink_members', 'hardlink_groups'} <= tables:
        if selected or {'hardlink_members', 'hardlink_groups'} & tables:
            raise SnapshotError('handoff retirement hardlink journal is incomplete')
        return False
    count = db.execute('SELECT count(*) FROM hardlink_members').fetchone()[0]
    invalid = db.execute("SELECT count(*) FROM hardlink_members m LEFT JOIN files f ON f.path=m.path "
        "WHERE f.path IS NULL OR json_extract(f.sig,'$[0]')!=m.device "
        "OR json_extract(f.sig,'$[1]')!=m.inode OR json_extract(f.sig,'$[8]')!=m.links "
        "OR m.links<=1 OR length(m.sha)!=64").fetchone()[0]
    groups = db.execute('SELECT m.device,m.inode,count(*),min(m.links),max(m.links),min(m.sha),max(m.sha),'
        'g.links,g.sha FROM hardlink_members m LEFT JOIN hardlink_groups g '
        'ON g.device=m.device AND g.inode=m.inode GROUP BY m.device,m.inode').fetchall()
    if selected != count or invalid or len(groups) != db.execute('SELECT count(*) FROM hardlink_groups').fetchone()[0]:
        raise SnapshotError('handoff retirement hardlink plan differs from the fixed source')
    if any(count > low or low != high or low != stored or sha_low != sha_high or sha_low != sha
           or count < low and not preserve_external
           for _dev, _ino, count, low, high, sha_low, sha_high, stored, sha in groups):
        raise SnapshotError('handoff retirement hardlink group differs')
    return bool(count)


def _same_hardlink_identity(actual, expected, rebind):
    """The unlink itself changes ctime and nlink, but no other identity field."""
    from stockagent.data_sync.workspace_source_handoff import _same_source_generation
    if not isinstance(actual, list) or not isinstance(expected, list) or len(actual) != 9 or len(expected) != 9:
        return False
    comparable = actual.copy()
    comparable[5], comparable[8] = expected[5], expected[8]
    return _same_source_generation(comparable, expected, rebind)


def _hardlink_batch_state(db, source: Path, device: int, inode: int, rebind, *, preserve_external=False):
    """Reconcile only a pending unlink; interrupted groups rehash one survivor."""
    row = db.execute('SELECT links,sha,ctime FROM hardlink_groups WHERE device=? AND inode=?',
                     (device, inode)).fetchone()
    if row is None:
        raise SnapshotError('handoff retirement hardlink group is missing')
    links, digest, recorded_ctime = row
    rows = db.execute('SELECT m.path,f.sig,f.state FROM hardlink_members m JOIN files f ON f.path=m.path '
                      'WHERE m.device=? AND m.inode=? ORDER BY m.path', (device, inode)).fetchall()
    missing_pending = done = 0
    reference = None
    for name, signature, state in rows:
        path = source/p._safe_relative_path(name, 'handoff hardlink member')
        present = os.path.lexists(path)
        if state == 2:
            done += 1
            if present:
                raise SnapshotError('retired hardlink pathname reappeared')
        elif not present:
            if state != 1:
                raise SnapshotError('unstarted hardlink pathname disappeared')
            missing_pending += 1
        elif reference is None:
            reference = (path, json.loads(signature))
    outside = links-len(rows)
    remaining = links-done-missing_pending
    if outside < 0 or outside and not preserve_external or remaining < outside:
        raise SnapshotError('handoff hardlink membership changed')
    if reference is None:
        if remaining != outside:
            raise SnapshotError('handoff hardlink membership changed')
        return {'remaining': remaining, 'ctime': recorded_ctime}
    if remaining <= outside:
        raise SnapshotError('handoff hardlink membership changed')
    path, expected = reference
    actual = relocation.signature(path)
    if not _same_hardlink_identity(actual, expected, rebind) or actual[8] != remaining:
        raise SnapshotError('handoff hardlink identity or link count changed')
    if missing_pending:
        # A crash may occur between unlink and the journal commit. Only in
        # that case recheck a surviving inode before accepting its new ctime.
        if relocation.digest(path, no_follow=True) != digest or relocation.signature(path) != actual:
            raise SnapshotError('interrupted hardlink content changed')
    elif actual[5] != recorded_ctime:
        raise SnapshotError('handoff hardlink generation changed')
    return {'remaining': remaining, 'ctime': actual[5]}


def prepare(cold: Path, source: Path, stage: Path, dataset: str, expected_sha: str,
            index: Path, *, original_inode: int, original_lock_inode: int,
            guard=lambda: None, progress=None) -> dict:
    """Build the same resumable E packs from the fixed original directory."""
    from stockagent.data_sync.workspace_source_handoff import _same_source_generation
    dataset = p.validate_slug(dataset, 'dataset')
    expected_sha = p._validate_hash(expected_sha, 'fixed private manifest SHA')
    cold, source, stage, index = (Path(x).absolute() for x in (cold, source, stage, index))
    if p._paths_overlap(cold, stage) or p._paths_overlap(source, stage) or stage.is_symlink():
        raise SnapshotError('handoff E stage overlaps its source or authority')
    stage.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stage.resolve(strict=True) != stage or stat.S_IMODE(stage.stat().st_mode) != 0o700:
        raise SnapshotError('handoff E stage is redirected or not private')
    intent = {'contract': s.CONTRACT+'.intent', 'dataset': dataset, 'old_manifest_sha256': expected_sha}
    path = stage/'intent.json'
    if path.exists():
        if path.is_symlink() or json.loads(path.read_text()) != intent:
            raise SnapshotError('handoff E stage belongs to another transaction')
    else:
        if any(stage.iterdir()):
            raise SnapshotError('handoff E stage is not empty')
        atomic_write_json(path, intent, durable=True)
    guard()
    source_rebind = _source_index_rebind(source, index, original_inode)
    if (stage/'prepared.json').exists():
        prepared = s._read_prepared(stage, dataset, expected_sha)
        if (prepared.get('source_mode') != 'retained_handoff_C_original'
                or prepared.get('source_root_inode') != original_inode
                or not s._generation_matches(index, prepared.get('source_index_signature'), relocation.signature(index))):
            raise SnapshotError('handoff E preparation belongs to another source generation')
        return prepared
    locks = cold/'.local-state/locks'
    with ExitStack() as stack:
        stack.enter_context(p._try_publication_lock(locks/'publish-retention-global.lock', shared=True))
        stack.enter_context(p._try_publication_lock(locks/f'publish-{dataset}.lock'))
        resolved = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if resolved.manifest_sha256 != expected_sha:
            raise SnapshotError('private D head changed before handoff preparation')
        index_before = relocation.signature(index)
        db = stack.enter_context(closing(_index_connection(index)))
        _check_source(source, db, original_inode=original_inode, original_lock_inode=original_lock_inode,
                      source_rebind=source_rebind)
        checkpoint = s._Checkpoint(stage, dataset, expected_sha)
        stack.callback(checkpoint.close)
        s._reset_unfinished(stage, (item['name'] for item in checkpoint.packages))
        original = stack.enter_context(p._open_inventory(cold, resolved.manifest))
        p._validate_inventory(resolved.manifest, original, retain_object_members=False)
        output = stack.enter_context(MetadataRows(p._entry_record, p._entry_from_record,
                                                  memory_rows=p.METADATA_MEMORY_ROWS))
        count = 0
        for row in original:
            found = db.execute('SELECT kind,sig,sha,bytes,state FROM files WHERE path=? COLLATE BINARY',
                               (row['path'],)).fetchone()
            if found is None or found[0] != row['kind']:
                raise SnapshotError('fixed D inventory differs from source copy index')
            if stat.S_IMODE(json.loads(found[1])[2]) != row.get('mode'):
                raise SnapshotError('fixed D member mode differs from source copy index')
            if row['kind'] == 'file' and (found[2] != row.get('sha256') or found[3] != row.get('size')
                                          or found[4] != 'verified'):
                raise SnapshotError('fixed D member differs from source copy index')
            entry = p._entry_from_record(row)
            if entry.kind == 'file':
                digest = checkpoint.lookup(entry.path)
                entry.storage = ({'kind': 'pack', 'member': entry.path, 'object_sha256': digest}
                                 if digest is not None else None)
            output.append(entry)
            count += 1
        if count != db.execute('SELECT count(*) FROM files').fetchone()[0]:
            raise SnapshotError('fixed D and source copy namespaces differ')
        builder = s._PackBuilder(stage, output, checkpoint=checkpoint, progress=progress)
        try:
            for row in original:
                if row['kind'] != 'file' or checkpoint.lookup(row['path']) is not None:
                    continue
                name = row['path']
                source_path = source/name
                if source_path.parent.resolve(strict=True) != source_path.parent:
                    raise SnapshotError('handoff source member parent is redirected')
                current = relocation.signature(source_path)
                expected = json.loads(db.execute('SELECT sig FROM files WHERE path=? COLLATE BINARY',
                                                 (name,)).fetchone()[0])
                if ((name == '.download.lock' and current[1] != original_lock_inode)
                        or (name != '.download.lock' and not _same_source_generation(current, expected, source_rebind))):
                    raise SnapshotError('retained source generation changed before packing')
                entry = p._entry_from_record(row)
                entry.source_stat = p._source_stat(source_path.lstat())
                with p._open_source_entry(source, entry) as stream:
                    builder.consume(stream, row)
            packages = builder.close()
        except BaseException:
            builder.abort()
            raise
        if (builder.files != resolved.manifest['source']['files']
                or builder.bytes != resolved.manifest['source']['logical_bytes']):
            raise SnapshotError('handoff pack did not consume all fixed members')
        _check_source(source, db, original_inode=original_inode, original_lock_inode=original_lock_inode,
                      source_rebind=source_rebind)
        if relocation.signature(index) != index_before:
            raise SnapshotError('fixed source copy index changed while packing')
        temporary = stage/('inventory-'+uuid.uuid4().hex+'.partial')
        inventory_sha = p._write_inventory(temporary, output)
        inventory = stage/('inventory-'+inventory_sha+'.jsonl.gz')
        os.rename(temporary, inventory)
        p._fsync_directory(stage)
        checkpoint.close()
        checkpoint_path = s._safe_stage_file(stage, 'checkpoint.sqlite')
        body = {'contract': s.CONTRACT+'.prepared', 'dataset': dataset,
                'old_manifest_sha256': expected_sha, 'old_snapshot_id': resolved.manifest['snapshot_id'],
                'packages': packages,
                'inventory': {'sha256': inventory_sha, 'name': inventory.name,
                              'bytes': inventory.stat().st_size, 'E_signature': s._signature(inventory)},
                'resume_index': {'name': checkpoint_path.name, 'bytes': checkpoint_path.stat().st_size,
                                 'E_signature': s._signature(checkpoint_path)},
                'files': builder.files, 'logical_bytes': builder.bytes,
                'source_member_sha_streamed': True, 'destination_independent_readback': False,
                'raw_extraction_tree_created': False, 'source_mode': 'retained_handoff_C_original',
                'source_root_inode': original_inode, 'source_index_signature': index_before,
                'observed_at_epoch': time.time()}
        body['identity_sha256'] = s._identity(body)
        atomic_write_json(stage/'prepared.json', body, durable=True)
        return body


def _active_taifex_external_scope(source: Path, index: Path, original_inode: int) -> bool:
    """Limit external-link retirement to the fixed, active TAIFEX handoff."""
    from stockagent.data_sync import workspace_source_handoff as handoff
    from scripts import run_workspace_source_staging as staging
    try:
        with handoff.selected_source(handoff.TAIFEX_PUBLIC_NAME):
            scope = handoff._scope()
            item, workspace, destination, evidence, _ = handoff.enrolled()
            value = handoff.read_journal()
            report = staging.verified_report(item, workspace)
            return (source == scope.backup and index == evidence/'files.sqlite'
                    and isinstance(value, dict) and value.get('state') == 'active'
                    and value.get('original_inode') == original_inode
                    and isinstance(report, dict)
                    and value.get('copy_result_sha256') == report.get('relocation_evidence_sha256')
                    and handoff._assert_retained(value) == destination
                    and handoff.provider_workspace.exact_mount(scope.source, destination))
    except (OSError, ValueError, TypeError, KeyError, SnapshotError):
        return False


def retire(source: Path, stage: Path, dataset: str, expected_sha: str, index: Path, *,
           original_inode: int, original_lock_inode: int, guard=lambda: None,
           process_check=process_references_many, progress=None, planning_progress=None, batch_size=512,
           preserve_external_hardlinks=False, reuse_existing_minute_D=False) -> dict:
    """Journal exact C payload unlinks; retain the handoff root and lock inode."""
    from stockagent.data_sync.workspace_source_handoff import _canonical_c_source, _same_source_generation
    source, stage, index = (Path(x).absolute() for x in (source, stage, index))
    dataset = p.validate_slug(dataset, 'dataset')
    expected_sha = p._validate_hash(expected_sha, 'fixed private manifest SHA')
    if type(batch_size) is not int or not 1 <= batch_size <= 512:
        raise SnapshotError('handoff retirement batch size is invalid')
    if type(preserve_external_hardlinks) is not bool:
        raise SnapshotError('external hardlink policy is invalid')
    if type(reuse_existing_minute_D) is not bool:
        raise SnapshotError('existing minute D reuse policy must be explicit')
    if reuse_existing_minute_D:
        from stockagent.data_sync import minute_raw_existing_d
        prepared, coverage = minute_raw_existing_d.retirement_preparation(
            source, stage, index, dataset=dataset, identity=expected_sha,
            original_inode=original_inode, original_lock_inode=original_lock_inode)
        original_guard = guard
        def guard():
            original_guard()
            minute_raw_existing_d.check_retirement_pin(coverage, stage)
    else:
        prepared = s._read_prepared(stage, dataset, expected_sha)
    mode = prepared.get('source_mode')
    if (mode not in {'retained_handoff_C_original', 'retained_handoff_C_index'}
            or prepared.get('source_root_inode') != original_inode
            or not s._generation_matches(index, prepared.get('source_index_signature'), relocation.signature(index))):
        raise SnapshotError('E preparation does not bind this retained source')
    if preserve_external_hardlinks and (dataset != 'taifex-public-workspace'
            or mode != 'retained_handoff_C_index'
            or not _active_taifex_external_scope(source, index, original_inode)):
        raise SnapshotError('external hardlink policy is not bound to the active TAIFEX source')
    preserved = prepared.get('preserved_paths', ['.download.lock'])
    if (not isinstance(preserved, list) or not preserved or len(preserved) != len(set(preserved))
            or '.download.lock' not in preserved
            or any(not isinstance(name, str) or p._safe_relative_path(name, 'preserved lock').as_posix() != name
                   for name in preserved)):
        raise SnapshotError('preserved handoff lock scope differs')
    supplemental = mode == 'retained_handoff_C_index' and prepared.get('supplemental_root_lock') is True
    preserve_symlinks = prepared.get('preserve_internal_symlinks', False)
    if type(preserve_symlinks) is not bool or preserve_symlinks and mode != 'retained_handoff_C_index':
        raise SnapshotError('handoff symlink preservation scope differs')
    with closing(_index_connection(index)) as source_index:
        overrides = _validated_source_overrides(dataset, source_index,
                                                prepared.get('source_generation_overrides'))
    guard()
    root_info = source.lstat()
    lock_info = (source/'.download.lock').lstat()
    if (source.is_symlink() or not stat.S_ISDIR(root_info.st_mode) or root_info.st_ino != original_inode
            or not stat.S_ISREG(lock_info.st_mode) or lock_info.st_ino != original_lock_inode):
        raise SnapshotError('retained handoff source or lock identity changed')
    shared_inodes = set()
    if preserve_external_hardlinks:
        with closing(_index_connection(index)) as fixed:
            shared_inodes = {(root_info.st_dev, row[0]) for row in fixed.execute(
                "SELECT DISTINCT json_extract(sig,'$[1]') FROM files "
                "WHERE kind='file' AND json_extract(sig,'$[8]')>1")}
    references = (process_check([source], inode_keys=shared_inodes) if preserve_external_hardlinks
                  else process_check([source]))
    if references:
        raise SnapshotError('retained handoff source is used by a running process')
    source_rebind = _source_index_rebind(source, index, original_inode)
    journal = stage/'C-payload-retirement.sqlite'
    if journal.is_symlink():
        raise SnapshotError('handoff retirement journal redirected')
    if preserve_symlinks:
        from stockagent.data_sync.workspace_source_handoff import _same_source_generation
        with closing(_index_connection(index)) as fixed:
            for name, encoded, digest, size, state in fixed.execute(
                    "SELECT path,sig,sha,bytes,state FROM files WHERE kind='symlink' ORDER BY path"):
                if (size != 0 or state != 'verified'
                        or not _same_source_generation(relocation.signature(source/name), json.loads(encoded), source_rebind)):
                    raise SnapshotError('retained source symlink generation changed before retirement')
                # On resume a valid internal file target may already have been
                # retired by this journal. The initial pass proves closure;
                # every resume still binds the link name, text and generation.
                _checked_internal_symlink(source, fixed, name, digest, check_target=not journal.exists())
    binding = {'contract': 'stockagent.handoff_C_payload_retirement.v1', 'dataset': dataset,
               'old_manifest_sha256': expected_sha, 'prepared_sha256': prepared['identity_sha256'],
               'source_inode': original_inode, 'source_device': root_info.st_dev,
               'source_lock_inode': original_lock_inode, 'source_index_signature': relocation.signature(index)}
    if mode == 'retained_handoff_C_index':
        binding.update(preserved_paths=preserved, supplemental_root_lock=supplemental)
    if preserve_external_hardlinks:
        binding['external_hardlinks'] = 'preserve_other_names_zero_reclaim_v1'
    if overrides:
        binding['source_generation_overrides'] = overrides
    placeholders = ','.join('?' for _ in preserved)
    fixed_query = ("SELECT path,sig,bytes FROM files WHERE kind='file' AND path NOT IN ("
                   + placeholders + ') ORDER BY path')
    last_phase_report = 0.0
    def phase_report(phase, completed, total):
        nonlocal last_phase_report
        if planning_progress is None:
            return
        guard()
        now = time.monotonic()
        if completed == 0 or completed == total or now-last_phase_report >= 1:
            last_phase_report = now
            planning_progress({'stage': phase, 'inventory_items_completed': completed, 'inventory_items_total': total})
    if not journal.exists():
        with closing(_index_connection(index)) as fixed:
            _check_source(source, fixed, original_inode=original_inode, original_lock_inode=original_lock_inode,
                          supplemental_root_lock=supplemental, source_rebind=source_rebind,
                          source_overrides=overrides, preserve_internal_symlinks=preserve_symlinks,
                          progress=lambda n, total: phase_report('retirement_source_namespace', n, total))
            temporary = stage/('C-retirement-'+uuid.uuid4().hex+'.partial.sqlite')
            descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            os.close(descriptor)
            try:
                with closing(sqlite3.connect(temporary)) as db:
                    db.execute('PRAGMA synchronous=FULL')
                    db.execute('PRAGMA journal_mode=DELETE')
                    db.execute('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID')
                    db.execute('CREATE TABLE files (path TEXT PRIMARY KEY, sig TEXT NOT NULL, bytes INTEGER NOT NULL, '
                               'allocated INTEGER NOT NULL, state INTEGER NOT NULL) WITHOUT ROWID')
                    _create_hardlink_plan(db)
                    db.executemany('INSERT INTO meta VALUES (?,?)',
                                   [(key, json.dumps(value, sort_keys=True)) for key, value in binding.items()])
                    count = 0
                    planned_total = fixed.execute("SELECT count(*) FROM files WHERE kind='file' AND path NOT IN ("
                                                  + placeholders + ')', preserved).fetchone()[0]
                    phase_report('retirement_planning', 0, planned_total)
                    for name, signature, size in fixed.execute(fixed_query, preserved):
                        relative = p._safe_relative_path(name, 'handoff retirement member')
                        target = source/relative
                        if target.parent.resolve(strict=True) != target.parent:
                            raise SnapshotError('handoff retirement member parent redirected')
                        info = target.lstat()
                        actual = relocation.signature_from_stat(info)
                        expected = overrides.get(name, json.loads(signature))
                        if not _same_source_generation(actual, expected, source_rebind) or actual[8] < 1:
                            raise SnapshotError('handoff retirement member changed')
                        db.execute('INSERT INTO files VALUES (?,?,?,?,0)',
                                   (name, signature, size, info.st_blocks*512))
                        if actual[8] > 1:
                            digest_row = fixed.execute('SELECT sha FROM files WHERE path=? COLLATE BINARY',
                                                       (name,)).fetchone()
                            if digest_row is None:
                                raise SnapshotError('handoff hardlink is absent from the fixed source index')
                            digest = p._validate_hash(digest_row[0], 'handoff hardlink SHA')
                            db.execute('INSERT INTO hardlink_members VALUES (?,?,?,?,?)',
                                       (name, expected[0], expected[1], expected[8], digest))
                        count += 1
                        if count % 1024 == 0 or count == planned_total:
                            phase_report('retirement_planning', count, planned_total)
                    preserved_count = fixed.execute('SELECT count(*) FROM files WHERE kind=\'file\' AND path IN ('
                                                    + placeholders + ')', preserved).fetchone()[0]
                    if count + preserved_count != prepared['files']:
                        raise SnapshotError('handoff retirement file count differs from E preparation')
                    _finish_hardlink_plan(db, preserve_external=preserve_external_hardlinks)
                    db.commit()
                os.link(temporary, journal, follow_symlinks=False)
                p._fsync_directory(stage)
            finally:
                if temporary.exists() and not temporary.is_symlink():
                    temporary.unlink()
                    p._fsync_directory(stage)
    journal_info = journal.lstat()
    if (not stat.S_ISREG(journal_info.st_mode) or journal_info.st_uid != os.geteuid()
            or stat.S_IMODE(journal_info.st_mode) != 0o600 or journal_info.st_nlink != 1):
        raise SnapshotError('handoff retirement journal is not a private regular file')
    with closing(sqlite3.connect(journal)) as db:
        db.execute('PRAGMA synchronous=FULL')
        values = {key: json.loads(value) for key, value in db.execute('SELECT key,value FROM meta')}
        if values != binding:
            stable_keys = set(binding) - {'source_device', 'source_index_signature'}
            if (set(values) != set(binding) or any(values.get(key) != binding[key] for key in stable_keys)
                    or type(values.get('source_device')) is not int or values['source_device'] < 0
                    or not _canonical_c_source(source, root_info.st_dev)
                    or not s._generation_matches(index, values.get('source_index_signature'),
                                                 binding['source_index_signature'])):
                raise SnapshotError('handoff retirement journal identity differs')
        totals = db.execute('SELECT count(*),coalesce(sum(bytes),0),coalesce(sum(allocated),0) FROM files').fetchone()
        has_hardlinks = _validate_hardlink_plan(db, preserve_external=preserve_external_hardlinks)
        # Verify the deletion plan against the sealed source index on every
        # resume, including after an interrupted batch. Counts alone would not
        # detect substitution of another pathname of the same length.
        with closing(_index_connection(index)) as fixed:
            kept_count, kept_bytes = fixed.execute('SELECT count(*),coalesce(sum(bytes),0) FROM files '
                "WHERE kind='file' AND path IN ("+placeholders+')', preserved).fetchone()
            if totals[0]+kept_count != prepared['files'] or totals[1]+kept_bytes != prepared['logical_bytes']:
                raise SnapshotError('handoff retirement journal totals differ')
            expected = fixed.execute(fixed_query, preserved)
            planned = db.execute('SELECT path,sig,bytes,allocated,state FROM files ORDER BY path')
            phase_report('retirement_plan_recheck', 0, totals[0])
            for checked, (original, item) in enumerate(zip_longest(expected, planned), 1):
                if (original is None or item is None or tuple(original) != tuple(item[:3])
                        or type(item[3]) is not int or item[3] < 0 or item[4] not in (0, 1, 2)):
                    raise SnapshotError('handoff retirement plan differs from fixed source index')
                if checked % 1024 == 0 or checked == totals[0]:
                    phase_report('retirement_plan_recheck', checked, totals[0])
            if has_hardlinks:
                for name, digest in db.execute('SELECT path,sha FROM hardlink_members'):
                    if fixed.execute('SELECT sha FROM files WHERE path=? COLLATE BINARY',
                                     (name,)).fetchone() != (digest,):
                        raise SnapshotError('handoff retirement hardlink SHA differs from fixed source')
        done_files, done_bytes = db.execute(
            'SELECT count(*),coalesce(sum(bytes),0) FROM files WHERE state=2').fetchone()
        # A retired row remains in the journal. Starting each 512-file query
        # at the beginning would rescan the growing retired prefix, and an
        # empty state=1 query would scan the entire journal on every batch.
        # First finish any interrupted state=1 rows, then visit state=0 rows;
        # the primary-key cursor bounds each pass even after a restart.
        pending_state, after_path = 1, ''
        while True:
            pending = list(db.execute('SELECT path,sig,bytes,allocated FROM files '
                                      'WHERE path>? AND state=? ORDER BY path LIMIT ?',
                                      (after_path, pending_state, batch_size)))
            if not pending:
                if pending_state == 1:
                    pending_state, after_path = 0, ''
                    continue
                break
            after_path = pending[-1][0]
            if pending_state == 0:
                with db:
                    db.executemany('UPDATE files SET state=1 WHERE path=? AND state=0', [(row[0],) for row in pending])
            guard()
            parents = set()
            link_states = {}
            for name, signature, _size, _allocated in pending:
                relative = p._safe_relative_path(name, 'handoff retirement member')
                target = source/relative
                if target.parent.resolve(strict=True) != target.parent:
                    raise SnapshotError('handoff retirement member parent redirected')
                member = (db.execute('SELECT device,inode FROM hardlink_members WHERE path=?', (name,)).fetchone()
                          if has_hardlinks else None)
                if member is not None and member not in link_states:
                    link_states[member] = _hardlink_batch_state(
                        db, source, *member, source_rebind, preserve_external=preserve_external_hardlinks)
                try:
                    info = target.lstat()
                except FileNotFoundError:
                    if pending_state == 0:
                        # This name was not part of an interrupted unlink:
                        # leave it pending so a retry cannot accept the loss.
                        with db:
                            db.execute('UPDATE files SET state=0 WHERE path=? AND state=1', (name,))
                        raise SnapshotError('handoff retirement member disappeared before its journalled batch')
                    info = None
                if info is not None:
                    actual, expected = relocation.signature_from_stat(info), overrides.get(name, json.loads(signature))
                    if member is None:
                        if not _same_source_generation(actual, expected, source_rebind):
                            raise SnapshotError('handoff retirement member changed before unlink')
                        target.unlink()
                    else:
                        state = link_states[member]
                        if (not _same_hardlink_identity(actual, expected, source_rebind)
                                or actual[8] != state['remaining'] or actual[5] != state['ctime']):
                            raise SnapshotError('handoff hardlink changed before unlink')
                        descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                        try:
                            held = os.fstat(descriptor)
                            if (held.st_dev, held.st_ino, held.st_ctime_ns, held.st_nlink) != (
                                    actual[0], actual[1], actual[5], actual[8]):
                                raise SnapshotError('handoff hardlink changed while opening')
                            target.unlink()
                            held = os.fstat(descriptor)
                            if (held.st_nlink != state['remaining']-1
                                    or not _same_hardlink_identity(relocation.signature_from_stat(held),
                                                                   expected, source_rebind)):
                                raise SnapshotError('handoff hardlink count changed during unlink')
                            state.update(remaining=held.st_nlink, ctime=held.st_ctime_ns)
                        finally:
                            os.close(descriptor)
                    parents.add(target.parent)
            for parent in sorted(parents):
                p._fsync_directory(parent)
            with db:
                db.executemany('UPDATE files SET state=2 WHERE path=? AND state=1', [(row[0],) for row in pending])
                if link_states:
                    db.executemany('UPDATE hardlink_groups SET ctime=? WHERE device=? AND inode=?',
                        [(state['ctime'], *key) for key, state in link_states.items()])
            done_files += len(pending)
            done_bytes += sum(row[2] for row in pending)
            if progress is not None:
                progress({'stage': 'retiring_C_payload', 'files_completed': done_files, 'files_total': totals[0],
                          'logical_bytes_processed': done_bytes, 'logical_bytes_total': totals[1]})
        guard()
        if (done_files != totals[0] or done_bytes != totals[1]
                or db.execute('SELECT 1 FROM files WHERE state!=2 LIMIT 1').fetchone() is not None):
            raise SnapshotError('handoff retirement journal still contains unfinished members')
        if (source.stat().st_ino != original_inode or (source/'.download.lock').stat().st_ino != original_lock_inode):
            raise SnapshotError('handoff root or lock changed during retirement')
        external_groups = external_source_names = 0
        if preserve_external_hardlinks:
            external_groups, external_source_names = db.execute(
                'SELECT count(*),coalesce(sum(n),0) FROM ('
                'SELECT count(*) n,g.links links FROM hardlink_members m JOIN hardlink_groups g '
                'ON g.device=m.device AND g.inode=m.inode GROUP BY m.device,m.inode HAVING n<g.links)'
            ).fetchone()
        return {'state': 'C_payload_retired_root_and_lock_preserved', 'files_removed': totals[0],
                'logical_bytes_removed': totals[1], 'estimated_allocated_bytes_reclaimed': totals[2],
                **({'external_hardlink_groups_preserved': external_groups,
                    'external_source_names_removed': external_source_names} if preserve_external_hardlinks else {}),
                'prepared_sha256': prepared['identity_sha256'], 'source_root_inode': original_inode,
                'source_lock_inode': original_lock_inode, 'destination_independent_readback': False,
                'NAS_accepted': False, 'observed_at_epoch': time.time()}
