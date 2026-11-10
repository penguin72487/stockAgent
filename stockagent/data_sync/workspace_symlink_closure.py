"""Bind a staged copy's links to its own copied namespace, without following out.

This is a fixed-copy observation, never a source-retirement or NAS receipt.
Dangling or externally resolved links remain present and explicitly unresolved.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import workspace_relocation as relocation, workspace_volume as volume
from stockagent.data_sync.desync_snapshots import SnapshotError

CONTRACT = 'stockagent.workspace_symlink_closure.v1'
FUTURES_765_PART = 'tw_futures_universe765_20261027_v1'


def _same_source_generation(expected, actual, device_mapping=None) -> bool:
    if expected == actual:
        return True
    return (isinstance(expected, list) and isinstance(actual, list)
            and len(expected) == len(actual) == 9
            and all(type(value) is int and value >= 0 for value in expected+actual)
            and isinstance(device_mapping, tuple) and len(device_mapping) == 2
            and (expected[0], actual[0]) == device_mapping and expected[1:] == actual[1:])


def _source_device_mapping(source: Path, expected, repo: Path):
    """Accept one device renumber only within the same guarded C repo root."""
    actual = relocation.signature(source)
    if _same_source_generation(expected, actual):
        return None
    if (not isinstance(expected, list) or len(expected) != 9
            or not source.is_relative_to(repo) or source == repo
            or source.is_symlink() or source.resolve(strict=True) != source
            or repo.is_symlink() or repo.resolve(strict=True) != repo
            or actual[0] != repo.stat().st_dev):
        raise SnapshotError('fixed symlink source root generation changed')
    mapping = (expected[0], actual[0])
    if not _same_source_generation(expected, actual, mapping):
        raise SnapshotError('fixed symlink source root generation changed')
    return mapping


def _active_futures_725_boundary(source: Path, destination: Path, expected, repo: Path):
    """Recognize the one child alias that legitimately changes this root.

    This boundary only permits checking indexed links outside the moved child;
    it is not evidence that the rest of the parent namespace is unchanged.
    """
    from stockagent.data_sync import workspace_source_handoff as handoff
    actual = relocation.signature(source)
    if (not isinstance(expected, list) or len(expected) != 9 or len(actual) != 9
            or not source.is_relative_to(repo) or source == repo
            or source.is_symlink() or source.resolve(strict=True) != source
            or actual[0] != repo.stat().st_dev
            or any(type(value) is not int or value < 0 for value in expected+actual)
            or any(expected[i] != actual[i] for i in (1, 2, 6, 7, 8))):
        raise SnapshotError('fixed symlink parent identity differs after child handoff')
    with handoff.selected_source(handoff.FUTURES_725_NAME):
        scope = handoff._scope()
        value = handoff.read_journal()
        if (scope.source != source/'tw_futures_universe725_20261025_v2'
                or not isinstance(value, dict) or value.get('state') != 'active'):
            raise SnapshotError('fixed symlink parent has no active reviewed child handoff')
        target = handoff._assert_retained(value)
        if (target != destination/'tw_futures_universe725_20261025_v2'
                or not handoff.provider_workspace.exact_mount(scope.source, target)):
            raise SnapshotError('fixed symlink child mount differs')
        keys = ('source', 'backup', 'destination', 'evidence', 'original_inode',
                'original_lock_inode', 'destination_inode', 'copy_result_sha256')
        binding = {key: value.get(key) for key in keys}
    return (expected[0], actual[0]) if expected[0] != actual[0] else None, binding


def _active_futures_765_boundary(source: Path, destination: Path):
    """Bind the second child alias without mistaking an old C link for E."""
    from scripts import project_futures_725_workspace_index as projection
    from stockagent.data_sync import workspace_source_handoff as handoff
    with handoff.selected_source(handoff.FUTURES_765_NAME):
        scope = handoff._scope()
        if scope.source != source/FUTURES_765_PART:
            raise SnapshotError('fixed symlink second child source differs')
        value = handoff.read_journal()
        if value is None:
            return None
        if value.get('state') != 'active':
            raise SnapshotError('fixed symlink second child handoff is in progress')
        target = handoff._assert_retained(value)
        if (target != destination/FUTURES_765_PART
                or not handoff.provider_workspace.exact_mount(scope.source, target)
                or value.get('internal_symlink_closure') != {
                    'links': 1, 'internal_targets_verified': 1, 'unresolved_targets': 0}
                or projection.validated_projection(Path(value['workspace']), scope=projection.FUTURES_765) is None):
            raise SnapshotError('fixed symlink second child mount or evidence differs')
        keys = ('source', 'backup', 'destination', 'evidence', 'original_inode',
                'original_lock_inode', 'destination_inode', 'copy_result_sha256')
        return {key: value.get(key) for key in keys}


def inspect_links(db, source: Path, destination: Path, *, source_device_mapping=None,
                  exclude_prefixes=()) -> dict:
    counts = {'links': 0, 'internal_targets_verified': 0, 'unresolved_targets': 0}
    for name, signature_text, text_sha, state in db.execute(
            "SELECT path,sig,sha,state FROM files WHERE kind='symlink'"):
        if any(name.startswith(prefix) for prefix in exclude_prefixes):
            continue
        relative = Path(name)
        if (not name or relative.is_absolute() or '..' in relative.parts
                or relative.as_posix() != name or state != 'verified'):
            raise SnapshotError('symlink index identity is invalid')
        origin, target = source / relative, destination / relative
        expected = json.loads(signature_text)
        if not _same_source_generation(expected, relocation.signature(origin), source_device_mapping) or not target.is_symlink():
            raise SnapshotError('source or staged symlink generation differs')
        destination_signature = relocation.signature(target)
        original_text, destination_text = os.readlink(origin), os.readlink(target)
        if (destination_text != original_text
                or hashlib.sha256(os.fsencode(original_text)).hexdigest() != text_sha):
            raise SnapshotError('staged symlink text differs from its fixed copy')
        counts['links'] += 1
        try:
            original_resolved, destination_resolved = origin.resolve(strict=True), target.resolve(strict=True)
            source_relative = original_resolved.relative_to(source)
            dest_relative = destination_resolved.relative_to(destination)
        except (OSError, ValueError, RuntimeError):
            counts['unresolved_targets'] += 1
            continue
        if source_relative != dest_relative:
            counts['unresolved_targets'] += 1
            continue
        if source_relative != Path('.'):
            member = db.execute('SELECT kind,sig,state FROM files WHERE path=?',
                                (source_relative.as_posix(),)).fetchone()
            if not member or member[0] not in {'file', 'directory'} or member[0] == 'file' and member[2] != 'verified':
                raise SnapshotError('resolved target is not a member of the fixed copy')
            fixed = json.loads(member[1])
            actual, staged = relocation.signature(original_resolved), relocation.signature(destination_resolved)
            if not _same_source_generation(fixed, actual, source_device_mapping):
                raise SnapshotError('resolved source target generation differs')
            if member[0] == 'file':
                matches = relocation.signature_metadata_matches(staged, fixed)
            else:
                matches = (stat.S_ISDIR(staged[2]) and stat.S_IMODE(staged[2]) == stat.S_IMODE(fixed[2])
                           and staged[4] == fixed[4] and staged[6:8] == fixed[6:8])
            if not matches:
                raise SnapshotError('resolved E target metadata differs')
        if (not _same_source_generation(expected, relocation.signature(origin), source_device_mapping)
                or relocation.signature(target) != destination_signature
                or os.readlink(target) != destination_text):
            raise SnapshotError('symlink changed during its target check')
        counts['internal_targets_verified'] += 1
    return counts


def inspect_mounted_765_links(db, source: Path, destination: Path) -> dict:
    """Recheck the indexed E link and target under the reviewed live bind."""
    from stockagent.data_sync.packed_snapshots import _validate_symlink_target
    prefix = FUTURES_765_PART+'/'
    counts = {'links': 0, 'internal_targets_verified': 0, 'unresolved_targets': 0}
    for name, signature_text, text_sha, state in db.execute(
            "SELECT path,sig,sha,state FROM files WHERE kind='symlink' AND path>=? AND path<?",
            (prefix, FUTURES_765_PART+'0')):
        relative = Path(name)
        if (not name.startswith(prefix) or relative.is_absolute() or '..' in relative.parts
                or relative.as_posix() != name or state != 'verified'):
            raise SnapshotError('mounted child symlink index identity is invalid')
        origin, target = source/relative, destination/relative
        before, staged = relocation.signature(origin), relocation.signature(target)
        expected = json.loads(signature_text)
        text = os.readlink(origin)
        _validate_symlink_target(name, text)
        if (not stat.S_ISLNK(before[2]) or before[:2] != staged[:2]
                or not stat.S_ISLNK(staged[2]) or os.readlink(target) != text
                or hashlib.sha256(os.fsencode(text)).hexdigest() != text_sha
                or stat.S_IMODE(staged[2]) != stat.S_IMODE(expected[2])
                or staged[3:5] != expected[3:5] or staged[6:8] != expected[6:8]):
            raise SnapshotError('mounted child symlink differs from its fixed copy')
        try:
            original_resolved, destination_resolved = origin.resolve(strict=True), target.resolve(strict=True)
            source_relative = original_resolved.relative_to(source)
            dest_relative = destination_resolved.relative_to(destination)
        except (OSError, ValueError, RuntimeError) as error:
            raise SnapshotError('mounted child symlink target is missing or outside its source') from error
        if (source_relative != dest_relative or not source_relative.parts
                or source_relative.parts[0] != FUTURES_765_PART):
            raise SnapshotError('mounted child symlink resolves outside the reviewed child')
        member = db.execute('SELECT kind,sig,state FROM files WHERE path=?',
                            (source_relative.as_posix(),)).fetchone()
        if not member or member[0] not in {'file', 'directory'} or member[0] == 'file' and member[2] != 'verified':
            raise SnapshotError('mounted child symlink target is absent from the fixed copy')
        fixed = json.loads(member[1])
        actual, dest_actual = relocation.signature(original_resolved), relocation.signature(destination_resolved)
        matches = (relocation.signature_metadata_matches(dest_actual, fixed) if member[0] == 'file' else
                   stat.S_ISDIR(dest_actual[2]) and stat.S_IMODE(dest_actual[2]) == stat.S_IMODE(fixed[2])
                   and dest_actual[4] == fixed[4] and dest_actual[6:8] == fixed[6:8])
        if (actual[:2] != dest_actual[:2] or not matches or relocation.signature(origin) != before
                or relocation.signature(target) != staged or os.readlink(origin) != text or os.readlink(target) != text):
            raise SnapshotError('mounted child symlink or target changed during verification')
        counts['links'] += 1
        counts['internal_targets_verified'] += 1
    if counts['links'] != 1:
        raise SnapshotError('mounted child symlink count differs from the reviewed scope')
    return counts


def _active_taifex_without_links(item, workspace, source, destination, evidence, report_path, report):
    """Recognize a fixed zero-link copy after its exact source was mounted on E."""
    from stockagent.data_sync import workspace_source_handoff as handoff

    if item.get('name') != handoff.TAIFEX_PUBLIC_NAME:
        return None
    with handoff.selected_source(handoff.TAIFEX_PUBLIC_NAME):
        scope = handoff._scope()
        value = handoff.read_journal()
        if value is None:
            return None
        if (value.get('state') != 'active' or scope.source != source
                or Path(value.get('destination', '')) != destination
                or Path(value.get('evidence', '')) != evidence
                or Path(value.get('workspace', '')) != workspace
                or value.get('copy_result_sha256') != report.get('relocation_evidence_sha256')
                or report.get('symlink_names_preserved') != 0
                or type(report.get('symlink_names_preserved')) is not int
                or handoff._assert_retained(value) != destination
                or not handoff.provider_workspace.exact_mount(source, destination)):
            raise SnapshotError('active zero-link handoff identity differs')
        binding = {key: value.get(key) for key in ('source', 'backup', 'destination', 'evidence',
                    'original_inode', 'original_lock_inode', 'destination_inode', 'copy_result_sha256')}
    if report_path.is_symlink() or relocation.digest(report_path, no_follow=True) != value.get('report_sha256'):
        raise SnapshotError('active zero-link source report differs')
    index = evidence/'files.sqlite'
    wal = evidence/'files.sqlite-wal'
    if index.is_symlink() or not index.is_file() or wal.exists() and wal.stat().st_size:
        raise SnapshotError('active zero-link fixed index is unavailable')
    before = relocation.signature(index)
    with sqlite3.connect(f'file:{index}?mode=ro', uri=True) as db:
        if db.execute("SELECT 1 FROM files WHERE kind='symlink' LIMIT 1").fetchone():
            raise SnapshotError('active source index contains a symbolic link')
    if relocation.signature(index) != before:
        raise SnapshotError('active zero-link index changed during inspection')
    return {'contract': CONTRACT, 'state': 'no_links', 'scope': 'fixed_index_after_active_handoff',
            'links': 0, 'internal_targets_verified': 0, 'unresolved_targets': 0,
            'source_copy_report_sha256': value['report_sha256'],
            'copy_result_sha256': value['copy_result_sha256'], 'handoff_binding': binding,
            'source_files_deleted': 0, 'source_alias_changed': True,
            'new_content_readback': False, 'D_or_NAS_acceptance': False, 'retirement_authorized': False}


def check_item(item, workspace: Path) -> dict:
    from scripts import run_workspace_source_staging as queue
    source, destination, evidence, report_path = queue.paths(item, workspace)
    report = queue.verified_report(item, workspace)
    if report is None:
        return {'state': 'waiting_for_copy'}
    volume.check()
    intent_path, index = evidence / 'intent.json', evidence / 'files.sqlite'
    intent = json.loads(intent_path.read_bytes())
    if (intent.get('contract') != 'workspace_scoped_relocation_v1'
            or intent.get('preserve_symlinks') is not True or intent.get('excluded_subtrees') != []
            or intent.get('source') != str(source) or intent.get('destination') != str(destination)):
        raise SnapshotError('fixed symlink scope changed')
    empty = _active_taifex_without_links(item, workspace, source, destination, evidence, report_path, report)
    if empty is not None:
        return empty
    partial = None
    second_child = None
    try:
        device_mapping = _source_device_mapping(source, intent.get('source_root_signature'), queue.ROOT)
    except SnapshotError:
        if item.get('name') != 'tw-futures-prepared-e-primary-20261104-v1':
            raise
        device_mapping, partial = _active_futures_725_boundary(
            source, destination, intent.get('source_root_signature'), queue.ROOT)
        second_child = _active_futures_765_boundary(source, destination)
    if index.is_symlink() or not index.is_file():
        raise SnapshotError('fixed symlink index is unavailable')
    wal = evidence / 'files.sqlite-wal'
    if wal.exists() and wal.stat().st_size:
        raise BlockingIOError('fixed symlink index still has an uncheckpointed WAL')
    report_sha = relocation.digest(report_path, no_follow=True)
    result_path = evidence / ('symlink-closure-after-725-765.json' if second_child is not None else
                              'symlink-closure-after-725.json' if partial is not None
                              else 'symlink-closure-result.json')
    if result_path.is_symlink():
        raise SnapshotError('fixed symlink receipt is redirected')
    if result_path.exists() and partial is None:
        if result_path.stat().st_size > 65536:
            raise SnapshotError('fixed symlink receipt is redirected')
        result = json.loads(result_path.read_bytes())
        if (result.get('contract') != CONTRACT or result.get('name') != item['name']
                or result.get('source_copy_report_sha256') != report_sha
                or result.get('copy_result_sha256') != report['relocation_evidence_sha256']
                or result.get('state') != 'verified_internal'
                or type(result.get('links')) is not int or result['links'] < 0
                or result.get('links') != result.get('internal_targets_verified')
                or type(result.get('unresolved_targets')) is not int or result['unresolved_targets'] != 0
                or result.get('source_files_deleted') != 0 or result.get('source_alias_changed') is not False
                or result.get('D_or_NAS_acceptance') is not False or result.get('retirement_authorized') is not False):
            raise SnapshotError('existing symlink receipt has a different fixed identity')
        return result
    marker = Path(item['marker'])
    if (marker.is_absolute() or '..' in marker.parts
            or relocation.digest(source / marker, no_follow=True) != report['source_marker_sha256']
            or relocation.digest(destination / marker, no_follow=True) != report['source_marker_sha256']):
        raise SnapshotError('fixed symlink copy marker changed')
    index_before = relocation.signature(index)
    source_before = relocation.signature(source)
    with sqlite3.connect(f'file:{index}?mode=ro', uri=True) as db:
        if partial is not None and db.execute(
                "SELECT 1 FROM files WHERE kind='symlink' AND path>=? AND path<? LIMIT 1",
                ('tw_futures_universe725_20261025_v2/', 'tw_futures_universe725_20261025_v20')).fetchone():
            raise SnapshotError('moved child contains a link outside this closure scope')
        prefixes = (FUTURES_765_PART+'/',) if second_child is not None else ()
        counts = inspect_links(db, source, destination, source_device_mapping=device_mapping,
                               exclude_prefixes=prefixes)
        if second_child is not None:
            mounted = inspect_mounted_765_links(db, source, destination)
            for key in counts:
                counts[key] += mounted[key]
    if (type(report.get('symlink_names_preserved')) is not int
            or counts['links'] != report['symlink_names_preserved']):
        raise SnapshotError('symlink count differs from the completed fixed copy')
    if (relocation.signature(index) != index_before
            or (relocation.signature(source) != source_before if partial is not None else
                not _same_source_generation(intent['source_root_signature'], relocation.signature(source), device_mapping))
            or relocation.digest(report_path, no_follow=True) != report_sha):
        raise SnapshotError('fixed symlink evidence changed during readback')
    if partial is not None:
        mapping_after, binding_after = _active_futures_725_boundary(
            source, destination, intent.get('source_root_signature'), queue.ROOT)
        if mapping_after != device_mapping or binding_after != partial:
            raise SnapshotError('fixed symlink child handoff changed during verification')
        if _active_futures_765_boundary(source, destination) != second_child:
            raise SnapshotError('fixed symlink second child handoff changed during verification')
    state = 'verified_internal' if counts['unresolved_targets'] == 0 else 'unresolved'
    result = {'contract': CONTRACT, 'name': item['name'], 'state': state, **counts,
              'source_copy_report_sha256': report_sha,
              'copy_result_sha256': report['relocation_evidence_sha256'],
              'observed_at_epoch': time.time(), 'source_files_deleted': 0,
              'source_alias_changed': partial is not None, 'new_content_readback': False,
              'D_or_NAS_acceptance': False, 'retirement_authorized': False}
    if partial is not None:
        result.update(scope='indexed_links_outside_725_after_child_handoff', child_handoff=partial)
    if second_child is not None:
        result.update(scope='indexed_links_after_725_765_child_handoffs', second_child_handoff=second_child)
    # Only a successful fixed observation is immutable. An unresolved attempt
    # is retried from the original source without destroying earlier evidence.
    if state == 'verified_internal':
        atomic_write_json(result_path, result, durable=True)
    return result
