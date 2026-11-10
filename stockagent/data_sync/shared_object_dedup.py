"""Consolidate a fixed, already selected generation without copying payloads.

The comparison uses the original source-hashed object descriptors and saved
publication signatures. It does not independently read D or claim recovery.
The transaction keeps the original receipts inside its durable journal and
updates their generation signatures after each exact hardlink transition.
"""
from __future__ import annotations

from contextlib import ExitStack
import copy
import json
import os
from pathlib import Path
import time

from scripts import retire_staged_cold_objects as owners
from stockagent.data_sync import packed_snapshots as p, semantic_link_promotion as promotion
from stockagent.data_sync import semantic_link_retirement as retirement, stream_repack as s
from stockagent.data_sync.desync_snapshots import SnapshotError, _fsync_directory, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.fixed_generation_shared_objects.v1'


def _read(path: Path) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 4*1024**2:
        raise SnapshotError('shared-object transaction metadata unavailable or redirected')
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise SnapshotError('shared-object transaction metadata is invalid')
    return value


def _stable_link_transition(before, after):
    return (isinstance(before, list) and isinstance(after, list) and len(before) == len(after) == 7
            and before[1:5] == after[1:5] and after[6] == before[6] + 1)


def _same_recorded_generation(first, second):
    # Device IDs may be renumbered across an independently guarded WSL mount.
    return (isinstance(first, list) and isinstance(second, list) and len(first) == len(second) == 7
            and first[1:] == second[1:])


def _refs(manifest):
    return [manifest['archive']['inventory'], *manifest['archive']['objects']]


def _updated_receipts(original_promotion, original_install, shared, journal):
    updated_promotion, updated_install = copy.deepcopy(original_promotion), copy.deepcopy(original_install)
    old_linked = {row['sha256']: row for row in original_promotion['linked']}
    for row in updated_promotion['linked']:
        if row['sha256'] in shared:
            row.pop('target_signature', None)
            row.update(mode='deduplicated_hardlink', source_signature=shared[row['sha256']])
    for row in updated_install['installed']:
        row['D_signature'] = shared.get(row['sha256'], old_linked[row['sha256']]['source_signature'])
    updated_promotion['duplicate_encoded_bytes_pending'] = 0
    updated_promotion['shared_object_transaction'] = {'contract': CONTRACT,
        'plan_sha256': journal['plan_sha256'], 'original_promotion_and_install_preserved_in_journal': True,
        'encoded_bytes_consolidated': sum(item['bytes'] for item in journal['plan']['items']),
        'independent_D_readback': False}
    return updated_promotion, updated_install


def consolidate(cold: Path, private: Path, *, promotion_path: Path, install_path: Path,
                journal_path: Path, plan: dict, policy: dict, extra_locks=(),
                guard=lambda: None, process_check=process_references_many, apply=False) -> dict:
    cold, private = Path(cold).absolute(), Path(private).absolute()
    promotion_path, install_path, journal_path = (Path(path).absolute() for path in
                                                   (promotion_path, install_path, journal_path))
    if (private != cold/'.local-state/private-history' or any(p._paths_overlap(cold, path)
            or path.is_symlink() or path.parent.resolve(strict=True) != path.parent
            for path in (promotion_path, install_path, journal_path))):
        raise SnapshotError('shared-object transaction authority differs')
    promotion.validate_latest_fixed_evidence(plan, policy)
    dataset = p.validate_slug(plan.get('dataset'), 'shared-object dataset')
    if dataset not in promotion.FAST_LATEST_DATASETS:
        raise SnapshotError('shared-object dataset is not enrolled')
    with ExitStack() as stack:
        for lock in extra_locks:
            stack.enter_context(owners._existing_lock(lock))
        for lock in (private/'.local-state/locks/publish-retention-global.lock',
                     cold/'.local-state/locks/publish-retention-global.lock',
                     private/'.local-state/locks'/f'publish-{dataset}.lock',
                     cold/'.local-state/locks'/f'publish-{dataset}.lock'):
            stack.enter_context(p._try_publication_lock(lock))
        guard()
        current_promotion, current_install = _read(promotion_path), _read(install_path)
        journal = _read(journal_path) if journal_path.exists() or journal_path.is_symlink() else None
        if journal is not None:
            if (journal.get('contract') != CONTRACT or journal.get('dataset') != dataset
                    or not isinstance(journal.get('plan'), dict)
                    or journal.get('plan_sha256') != s._identity(journal['plan'])
                    or journal.get('state') not in {'planned', 'sharing', 'receipts_updated', 'completed'}):
                raise SnapshotError('shared-object journal identity differs')
            original_promotion, original_install = (journal['plan'][key] for key in
                                                      ('promotion_before', 'install_before'))
        else:
            original_promotion, original_install = current_promotion, current_install
        if (original_promotion.get('contract') != promotion.LATEST_CONTRACT
                or original_promotion.get('state') != 'committed'
                or original_promotion.get('old_sources_deleted') is not True
                or original_promotion.get('dataset') != dataset
                or original_promotion.get('comparison_identity_sha256') != plan.get('identity_sha256')
                or original_promotion.get('private_manifest_sha256') != plan.get('private_manifest_sha256')
                or original_install.get('contract') != s.CONTRACT+'.install'
                or original_install.get('new_manifest_sha256') != plan.get('private_manifest_sha256')):
            raise SnapshotError('shared-object fixed promotion or installation differs')
        fixed = p.resolve_packed_snapshot_id(private, dataset, original_promotion['private_snapshot_id'],
                                              require_objects=True)
        public = p.resolve_packed_snapshot_id(cold, dataset, original_promotion['new_snapshot_id'],
                                               require_objects=True)
        if (fixed.manifest_sha256 != original_promotion['private_manifest_sha256']
                or public.manifest_sha256 != original_promotion['new_manifest_sha256']
                or _refs(fixed.manifest) != _refs(public.manifest)
                or not promotion.public_source_matches_fixed(public.manifest['source'], fixed.manifest['source'], plan)):
            raise SnapshotError('shared-object immutable manifest or content scope differs')
        if journal is None:
            if p.resolve_latest_packed(cold, dataset, require_objects=False).manifest_sha256 != public.manifest_sha256:
                raise SnapshotError('shared-object public head advanced')
            retirement._check_replacement(cold, private, original_promotion, plan)
            installed = {row.get('sha256'): row for row in original_install.get('installed', [])}
            linked = {row.get('sha256'): row for row in original_promotion.get('linked', [])}
            descriptors = _refs(fixed.manifest)
            if (len(installed) != len(descriptors) or len(linked) != len(descriptors)
                    or set(installed) != {row['sha256'] for row in descriptors}
                    or set(linked) != set(installed)):
                raise SnapshotError('shared-object source and install journals have different members')
            items = []
            for row in descriptors:
                digest, relative = row['sha256'], row['relpath']
                origin, target = private/relative, cold/relative
                before, record = installed[digest].get('D_signature'), linked[digest]
                if not (s._signature_matches(origin, before)
                        or promotion._same_original_after_link(before, record.get('source_signature'))
                        and s._signature_matches(origin, record.get('source_signature'))):
                    raise SnapshotError('shared-object install signature lacks an exact link transition')
                if record.get('mode') == 'existing_public_same_sha':
                    if (origin.is_symlink() or target.is_symlink() or origin.stat().st_nlink != 1
                            or target.stat().st_nlink != 1 or os.path.samefile(origin, target)
                            or origin.parent.resolve(strict=True) != origin.parent
                            or target.parent.resolve(strict=True) != target.parent):
                        raise SnapshotError('shared-object duplicate paths are not exclusive regular members')
                    items.append({'sha256': digest, 'relative': relative, 'bytes': row['bytes'],
                        'private_before': s._signature(origin), 'public_before': s._signature(target),
                        'public_allocated_bytes': target.stat().st_blocks * 512})
            if sum(item['bytes'] for item in items) != original_promotion.get('duplicate_encoded_bytes_pending'):
                raise SnapshotError('shared-object duplicate amount differs from promotion')
            if not items:
                return {'contract': CONTRACT, 'dataset': dataset, 'state': 'no_duplicates', 'apply': apply,
                        'old_manifest_sha256': original_promotion['old_manifest_sha256'], 'object_bytes': 0}
            body = {'promotion_before': original_promotion, 'install_before': original_install,
                    'comparison_identity_sha256': plan['identity_sha256'], 'items': items}
            journal = {'contract': CONTRACT, 'dataset': dataset, 'state': 'planned', 'plan': body,
                       'plan_sha256': s._identity(body), 'shared': {},
                       'independent_D_readback': False, 'NAS_accepted': False}
            if not apply:
                return {**journal, 'apply': False, 'object_bytes': sum(item['bytes'] for item in items)}
            atomic_write_json(journal_path, journal, durable=True)
        elif not apply:
            return {**journal, 'apply': False}

        if journal['plan'].get('comparison_identity_sha256') != plan['identity_sha256']:
            raise SnapshotError('shared-object comparison changed during retry')
        shared = journal.get('shared')
        items = journal['plan'].get('items')
        descriptors = {row['sha256']: row for row in _refs(fixed.manifest)}
        linked = {row['sha256']: row for row in original_promotion.get('linked', [])}
        if not isinstance(shared, dict) or not isinstance(items, list) or not items:
            raise SnapshotError('shared-object progress is invalid')
        try:
            item_ids = {item['sha256'] for item in items}
            valid_items = (len(item_ids) == len(items) and set(shared).issubset(item_ids)
                and item_ids == {sha for sha, row in linked.items() if row.get('mode') == 'existing_public_same_sha'}
                and all(item['relative'] == descriptors[item['sha256']]['relpath']
                        and item['bytes'] == descriptors[item['sha256']]['bytes']
                        and _same_recorded_generation(item['private_before'], linked[item['sha256']]['source_signature'])
                        and _same_recorded_generation(item['public_before'], linked[item['sha256']]['target_signature'])
                        and type(item['public_allocated_bytes']) is int and item['public_allocated_bytes'] >= 0
                        for item in items))
        except (KeyError, TypeError):
            valid_items = False
        if not valid_items:
            raise SnapshotError('shared-object journal member identities differ')
        if current_install != original_install or current_promotion != original_promotion:
            if set(shared) != item_ids:
                raise SnapshotError('shared-object original receipts changed before all members were shared')
            expected_promotion, expected_install = _updated_receipts(original_promotion, original_install, shared, journal)
            if current_install not in (original_install, expected_install) or current_promotion not in (original_promotion, expected_promotion):
                raise SnapshotError('shared-object original receipts changed during retry')
        for item in items:
            digest, relative = item['sha256'], item['relative']
            origin, target = private/relative, cold/relative
            temporary = target.with_name('.'+target.name+'.share-'+journal['plan_sha256'][:16])
            if process_check([origin, target]):
                raise SnapshotError('shared-object member is used by a running process')
            if origin.is_symlink() or target.is_symlink() or temporary.is_symlink():
                raise SnapshotError('shared-object member was redirected')
            if digest in shared:
                if temporary.exists() or not os.path.samefile(origin, target) or not s._signature_matches(origin, shared[digest]):
                    raise SnapshotError('completed shared-object member changed during retry')
                continue
            if temporary.exists():
                if not os.path.samefile(origin, temporary) or not s._signature_matches(target, item['public_before']):
                    raise SnapshotError('interrupted shared-object temporary differs')
            elif not os.path.samefile(origin, target):
                if (not s._signature_matches(origin, item['private_before'])
                        or not s._signature_matches(target, item['public_before'])):
                    raise SnapshotError('shared-object original generation changed')
                os.link(origin, temporary, follow_symlinks=False)
                _fsync_directory(temporary.parent)
            after = s._signature(origin)
            if not _stable_link_transition(item['private_before'], after):
                raise SnapshotError('shared-object link transition differs')
            if temporary.exists():
                os.replace(temporary, target)
                _fsync_directory(target.parent)
            if not os.path.samefile(origin, target):
                raise SnapshotError('shared-object atomic replacement did not link the same inode')
            shared[digest] = s._signature(origin)
            journal['state'] = 'sharing'
            atomic_write_json(journal_path, journal, durable=True)

        updated_promotion, updated_install = _updated_receipts(original_promotion, original_install, shared, journal)
        for row in updated_install['installed']:
            expected = row['D_signature']
            relative = next(ref['relpath'] for ref in _refs(fixed.manifest) if ref['sha256'] == row['sha256'])
            if not s._signature_matches(private/relative, expected):
                raise SnapshotError('shared-object installed generation changed before receipt update')
        if current_install not in (original_install, updated_install) or current_promotion not in (original_promotion, updated_promotion):
            raise SnapshotError('shared-object original receipts changed during retry')
        if current_install != updated_install:
            atomic_write_json(install_path, updated_install, durable=True)
        if current_promotion != updated_promotion:
            atomic_write_json(promotion_path, updated_promotion, durable=True)
        journal.update(state='completed', completed_at_epoch=time.time(), apply=True,
            old_manifest_sha256=original_promotion['old_manifest_sha256'],
            object_count=len(journal['plan']['items']),
            object_bytes=sum(item['bytes'] for item in journal['plan']['items']),
            estimated_allocated_bytes_reclaimed=sum(item['public_allocated_bytes'] for item in journal['plan']['items']))
        atomic_write_json(journal_path, journal, durable=True)
        return journal
