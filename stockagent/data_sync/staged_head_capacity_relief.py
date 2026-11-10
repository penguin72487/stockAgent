"""Make room for a sealed E repack by retiring exact old D objects.

This is the user's E-first migration, not backup or independent recovery.
While it runs, the old D head can be incomplete; the sealed local-only E
view is the temporary source until the existing installer commits the new D
head.  Only objects exclusive to the exact source manifest may be removed.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import itertools
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time

from stockagent.data_sync import packed_retention, packed_snapshots as p
from stockagent.data_sync import repack_retirement as retirement, stream_repack
from stockagent.data_sync import temporary_e_authority
from stockagent.data_sync.backup_stream import _private_catalog_generation
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.staged_current_head_capacity_relief.v1'
JOURNAL = 'current-head-capacity-relief.json'
HEADROOM = 256 * 1024**2
METADATA_WORKERS = 16
METADATA_BATCH = 128


def E_only_pending_generations(workspace: Path) -> set[tuple[str, str]]:
    """Identify exact E-only generations which need their D writer next.

    This checks bounded stage/receipt metadata only, not source payloads. A
    fixed publication receipt ends the priority; missing or malformed urgent
    metadata fails closed instead of letting another writer consume D space.
    """
    workspace = Path(workspace).absolute()
    root = workspace/'migration/stream-source-repack-v1'
    if not os.path.lexists(root):
        return set()
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('E-first priority namespace redirected')
    paths = list(root.glob('*/*/'+JOURNAL))
    if len(paths) > 1024:
        raise SnapshotError('E-first priority namespace exceeds its bound')
    pending = set()
    for path in paths:
        stage, directory = path.parent, path.parent.parent
        name = p.validate_slug(directory.name, 'priority dataset')
        expected = p._validate_hash(stage.name, 'priority old manifest SHA')
        if (directory.is_symlink() or stage.is_symlink() or path.is_symlink()
                or stage.resolve(strict=True) != stage or not path.is_file()
                or path.stat().st_size > 128*1024):
            raise SnapshotError('E-first priority journal redirected or oversized')
        value = json.loads(path.read_text())
        private = value.get('private', False) if isinstance(value, dict) else None
        if (not isinstance(value, dict) or value.get('contract') != CONTRACT
                or value.get('dataset') != name or value.get('old_manifest_sha256') != expected
                or type(private) is not bool
                or private and name not in stream_repack.PRIVATE_REPACK_DATASETS
                or value.get('state') not in ('planned', 'removing', 'completed')
                or type(value.get('removed_count')) is not int or value['removed_count'] < 0):
            raise SnapshotError('E-first priority journal invalid')
        if value['state'] == 'planned' and value['removed_count'] == 0 and not value.get('pending'):
            continue  # No old object has been retired yet.
        receipt_dir = workspace/'migration/source-local-exact-repack'/name
        if not receipt_dir.exists():
            pending.add((name, expected))
            continue
        if receipt_dir.is_symlink() or not receipt_dir.is_dir() or receipt_dir.resolve(strict=True) != receipt_dir:
            raise SnapshotError('E-first publication receipt namespace redirected')
        receipts = list(receipt_dir.glob('*.json'))
        if len(receipts) > 256:
            raise SnapshotError('E-first publication receipts exceed their bound')
        published = False
        for receipt in receipts:
            if (not re.fullmatch(r'[a-f0-9]{64}\.json', receipt.name)
                    or receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_size > 128*1024):
                continue
            try:
                body = json.loads(receipt.read_text())
            except (OSError, ValueError):
                continue
            if (isinstance(body, dict) and body.get('contract') == 'stockagent.source_local_exact_repack.v1'
                    and body.get('state') == 'new_D_head_committed_from_exact_E_source'
                    and body.get('dataset') == name and body.get('old_manifest_sha256') == expected
                    and body.get('new_manifest_sha256') == receipt.stem
                    and (body.get('private') is True) == private):
                published = True
                break
        if not published:
            pending.add((name, expected))
    return pending


def E_only_pending(workspace: Path) -> bool:
    return bool(E_only_pending_generations(workspace))


def load(stage: Path, dataset: str, old_sha: str, prepared: dict) -> dict | None:
    """Read an exact, durable plan; reject partial or substituted identities."""
    path = stage/JOURNAL
    if not os.path.lexists(path):
        return None
    if path.is_symlink() or not path.is_file():
        raise SnapshotError('current-head relief journal redirected')
    value = retirement.load_journal(path)
    plan = value.get('plan')
    if (value.get('contract') != CONTRACT or value.get('dataset') != dataset
            or value.get('old_manifest_sha256') != old_sha
            or type(value.get('private', False)) is not bool
            or value.get('prepared_sha256') != prepared['identity_sha256']
            or not isinstance(plan, dict) or value.get('plan_sha256') != stream_repack._identity(plan)
            or value.get('state') not in ('planned', 'removing', 'completed')
            or plan.get('old_snapshot_id') != prepared['old_snapshot_id']
            or not isinstance(plan.get('items'), list) or not isinstance(value.get('removed'), list)):
        raise SnapshotError('current-head relief journal identity differs')
    items = plan['items']
    names = []
    for item in items:
        if (not isinstance(item, dict) or item.get('kind') != 'object'
                or not isinstance(item.get('relative'), str)
                or type(item.get('bytes')) is not int or item['bytes'] < 0
                or type(item.get('allocated_bytes')) is not int or item['allocated_bytes'] <= 0
                or not isinstance(item.get('signature'), list)):
            raise SnapshotError('current-head relief plan member invalid')
        p._safe_relative_path(item['relative'], 'current-head relief member')
        names.append(item['relative'])
    removed = value['removed']
    pending = value.get('pending')
    if isinstance(pending, str):
        pending = [pending]
    if (len(names) != len(set(names)) or removed != names[:len(removed)]
            or pending is not None and (not isinstance(pending, list) or not pending or len(pending) > 64
                                        or pending != names[len(removed):len(removed)+len(pending)])
            or value.get('state') == 'completed' and len(removed) != len(names)):
        raise SnapshotError('current-head relief cursor differs')
    return value


def _require_only_own_pin(materialized: Path, retention_state: Path, known: set[str],
                          dataset: str, old_sha: str, old_id: str, stage: Path) -> None:
    pin = materialized/'.stream-repack-pins'/dataset/(old_sha+'.pin.json')
    expected = {'contract': stream_repack.CONTRACT+'.source-pin', 'dataset': dataset,
                'manifest': {'snapshot_id': old_id}, 'manifest_sha256': old_sha,
                'stage': str(stage)}
    if (pin.is_symlink() or not pin.is_file() or pin.stat().st_size > 16384
            or json.loads(pin.read_text()) != expected):
        raise SnapshotError('current-head relief needs its exact source pin')
    protected, evidence, errors = packed_retention._local_protected_snapshot_ids(
        materialized, known, research_pin_root=retention_state/'research-pins',
        research_pin_marker=retention_state/packed_retention.RESEARCH_PIN_MARKER)
    if errors:
        raise SnapshotError('current-head relief protection metadata is unknown')
    matches = [entry for entry in evidence if entry.startswith(old_id+':')]
    if old_id not in protected or matches != [old_id+':'+str(pin)]:
        raise SnapshotError('current-head relief has another or missing source pin')


def _require_single_local_head(cold: Path, dataset: str, old_sha: str, old_id: str,
                               publisher: str = 'penguin') -> None:
    """Require the exact sole publisher selected by the admission owner."""
    publisher = p.validate_slug(publisher, 'current-head publisher')
    root = cold/'heads'/dataset
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('current-head relief head namespace unavailable')
    heads = list(root.glob('*.json'))
    if len(heads) != 1 or heads[0].name != publisher+'.json' or heads[0].is_symlink():
        raise SnapshotError(f'current-head relief needs one {publisher} publisher head')
    value = json.loads(heads[0].read_text())
    if (value.get('dataset') != dataset or value.get('node_id') != publisher
            or value.get('snapshot_id') != old_id or value.get('manifest_sha256') != old_sha):
        raise SnapshotError('current-head relief publisher head differs')


def _require_single_private_head(cold: Path, dataset: str, old_sha: str, old_id: str,
                                 publisher: str) -> None:
    """A private replacement must retain its exact original publisher."""
    publisher = p.validate_slug(publisher, 'private publisher')
    root = cold/'heads'/dataset
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('private current-head relief head namespace unavailable')
    heads = list(root.glob('*.json'))
    if len(heads) != 1 or heads[0].name != publisher+'.json' or heads[0].is_symlink():
        raise SnapshotError('private current-head relief needs one original publisher head')
    value = json.loads(heads[0].read_text())
    if (value.get('dataset') != dataset or value.get('node_id') != publisher
            or value.get('snapshot_id') != old_id or value.get('manifest_sha256') != old_sha):
        raise SnapshotError('private current-head relief publisher head differs')


def _candidate(cold: Path, relative: str) -> dict | None:
    """Capture one immutable generation with one no-follow metadata call."""
    path = p._path_under(cold, relative, 'current-head relief member')
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise SnapshotError('current-head relief member is redirected or not a regular file')
    allocated = info.st_blocks*512 if info.st_nlink == 1 else 0
    if not allocated:
        return None
    return {'kind': 'object', 'relative': relative,
            'signature': [info.st_dev, info.st_ino, info.st_mode, info.st_size,
                          info.st_mtime_ns, info.st_ctime_ns, info.st_nlink],
            'bytes': info.st_size, 'allocated_bytes': allocated}


def _candidates(cold: Path, relatives: set[str]) -> list[dict]:
    # Metadata calls to Windows-backed D wait on the host; bounded parallel
    # lstat avoids repeated round trips without parallel payload reads.
    pending = iter(sorted(relatives))
    result = []
    with ThreadPoolExecutor(max_workers=METADATA_WORKERS, thread_name_prefix='relief-metadata') as pool:
        while batch := tuple(itertools.islice(pending, METADATA_BATCH)):
            result.extend(item for item in pool.map(lambda name: _candidate(cold, name), batch) if item)
    return result


def relieve(cold: Path, stage: Path, dataset: str, old_sha: str, *, catalog: Path,
            observation: Path, cache: Path, materialized: Path, retention_state: Path,
            install_lock: Path, reserve_bytes: int, extra_locks=(), guard=lambda: None,
            apply=False, free_bytes=None, headroom=HEADROOM) -> dict:
    """Plan or resume bounded old-object unlinks after source-hashed E sealing.

    The regular current D head is deliberately left in place, but may be
    temporarily unreadable after the first unlink. Existing research pins,
    other release references, live file users and all owner locks still win.
    """
    cold, stage = cold.absolute(), stage.absolute()
    dataset = p.validate_slug(dataset, 'dataset')
    old_sha = p._validate_hash(old_sha, 'old manifest SHA')
    if (p._paths_overlap(cold, stage) or stage.is_symlink() or stage.resolve(strict=True) != stage
            or type(reserve_bytes) is not int or reserve_bytes < 0
            or type(headroom) is not int or headroom < 0 or type(apply) is not bool):
        raise SnapshotError('invalid current-head relief scope')
    prepared = stream_repack._read_prepared(stage, dataset, old_sha)
    stream_repack.require_content_alias_install(stage, dataset, old_sha, prepared)
    old_id = p.validate_slug(prepared['old_snapshot_id'], 'old snapshot')
    old_relative = f'manifests/{dataset}/{old_id}.json'
    journal = load(stage, dataset, old_sha, prepared)
    if journal is not None and journal.get('private') is True:
        raise SnapshotError('current-head relief journal authority differs')
    from scripts import run_stream_packed_repack as stream_cli
    retired_lab203 = (dataset == stream_cli._RETIRED_LAB203_DATASET
                      and old_sha == stream_cli._RETIRED_LAB203_MANIFEST)
    publisher = 'lab203' if retired_lab203 else 'penguin'
    def require_publisher():
        _require_single_local_head(cold, dataset, old_sha, old_id, publisher)
        if retired_lab203:
            started = journal is not None and (journal.get('removed_count', 0) > 0 or journal.get('pending'))
            fixed = (prepared.get('identity_sha256') == stream_cli._RETIRED_LAB203_PREPARED
                     and prepared.get('source_member_sha_streamed') is True
                     and prepared.get('packed_format') is None)
            if not fixed or (not started and not stream_cli._retired_lab203_successor_ready(
                    cold, dataset, old_sha, 'penguin', prepared)):
                raise SnapshotError('retired lab203 current-head relief is not admitted')
    # Avoid scanning a large catalog while a long-lived original owner is busy.
    with retirement._boundary(cold, extra_locks, attempts=1):
        guard()
        latest = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if latest.manifest_sha256 != old_sha or latest.manifest['snapshot_id'] != old_id:
            raise SnapshotError('current-head relief original head changed')
        require_publisher()
    old_refs = retirement._refs(latest.manifest)
    generation = _private_catalog_generation(catalog)
    metadata, _, catalog_identity = retirement.old_retirement.catalog_index(
        catalog, observation, set(), cache_path=cache)
    if _private_catalog_generation(catalog) != generation:
        raise SnapshotError('current-head relief catalog changed during census')
    references = retirement.catalog_references(catalog, cold, metadata, old_refs, generation)
    with retirement._boundary(cold, extra_locks, attempts=1), \
            p._try_publication_lock(cold/'.local-state/locks'/f'publish-{dataset}.lock'), \
            p._try_publication_lock(install_lock), \
            temporary_e_authority.borrow(stage, dataset, old_sha) as view:
        guard()
        if _private_catalog_generation(catalog) != generation:
            raise SnapshotError('current-head relief catalog changed')
        current = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if (current.manifest_sha256 != old_sha or current.manifest['snapshot_id'] != old_id
                or retirement._refs(current.manifest) != old_refs
                or view.manifest['source'] != current.manifest['source']):
            raise SnapshotError('current-head relief source changed')
        require_publisher()
        known = retirement.fresh_manifest_references(cold, metadata, old_refs, references)
        known.add(old_id)
        known = retirement.include_other_authority_repack_pins(cold, materialized, known)
        _require_only_own_pin(materialized, retention_state, known, dataset, old_sha, old_id, stage)
        deletable = {ref for ref, owners in references.items() if owners == {old_relative}}
        # Keep the original inventory and manifest so their source identities
        # remain inspectable while the standard D head is incomplete.
        deletable.discard(current.manifest['archive']['inventory']['relpath'])
        if journal is None:
            # The stage may have a partially installed replacement; subtract
            # only objects actually committed by that exact install journal.
            from scripts.run_stream_packed_repack import remaining_install_bytes
            install_path = stage/'install.json'
            if install_path.is_symlink():
                raise SnapshotError('current-head relief install journal redirected')
            install = json.loads(install_path.read_text()) if install_path.is_file() else {}
            required = remaining_install_bytes(cold, prepared, install)
            available = shutil.disk_usage(cold).free if free_bytes is None else free_bytes
            if type(available) is not int or available < 0:
                raise SnapshotError('current-head relief free space unknown')
            shortage = max(0, required+reserve_bytes+headroom-available)
            if shortage == 0:
                return {'contract': CONTRACT, 'dataset': dataset, 'state': 'not_needed',
                        'D_required_bytes': required, 'D_free_bytes': available}
            candidates = _candidates(cold, deletable)
            # Once E is the sole temporary source, retire every exclusive old
            # payload in this transaction. This avoids a second D metadata
            # sweep after commit and leaves maximum room for the replacement.
            # Still do not start if the projected allocation cannot make it
            # fit under the current physical D floor.
            candidates.sort(key=lambda item: (-item['allocated_bytes'], item['relative']))
            items = candidates
            estimated = sum(item['allocated_bytes'] for item in items)
            if estimated < shortage:
                return {'contract': CONTRACT, 'dataset': dataset, 'state': 'insufficient_exclusive_space',
                        'D_shortfall_bytes': shortage, 'exclusive_allocated_bytes': estimated}
            plan = {'old_snapshot_id': old_id, 'old_refs': sorted(old_refs), 'items': items,
                    'catalog_identity_sha256': catalog_identity, 'D_required_bytes': required,
                    'D_free_bytes': available, 'D_reserve_bytes': reserve_bytes,
                    'D_shortfall_bytes': shortage}
            journal = {'contract': CONTRACT, 'dataset': dataset, 'old_manifest_sha256': old_sha,
                       'prepared_sha256': prepared['identity_sha256'], 'plan': plan,
                       'plan_sha256': stream_repack._identity(plan), 'removed': [], 'state': 'planned',
                       'planned_object_count': len(items), 'planned_allocated_bytes': estimated,
                       'temporary_standard_D_head_unavailable': True,
                       'independent_D_readback': False, 'NAS_accepted': False}
        else:
            planned = {item['relative'] for item in journal['plan']['items']}
            if (set(journal['plan'].get('old_refs', [])) != old_refs or not planned.issubset(deletable)):
                raise SnapshotError('current-head relief references changed')
        # Only a journalled pending batch can contain already-unlinked names.
        # Do not make 17,000 extra 9p round trips for the untouched members.
        removed = set(journal['removed'])
        pending = journal.get('pending') or []
        pending = {pending} if isinstance(pending, str) else set(pending)
        paths = [cold/item['relative'] for item in journal['plan']['items']
                 if item['relative'] not in removed
                 and (item['relative'] not in pending or os.path.lexists(cold/item['relative']))]
        if process_references_many(paths, immutable_regular_paths=True):
            raise SnapshotError('current-head relief object is used by a running process')
        if not apply:
            return {**journal, 'apply': False}
        if journal['state'] == 'completed':
            return journal
        for relative in journal['removed']:
            if os.path.lexists(cold/relative):
                raise SnapshotError('current-head relief removed name returned')
        require_publisher()
        retirement.journalled_unlink(cold, stage/JOURNAL, journal, guard=guard, parallel_objects=8)
        journal.update(state='completed', completed_at_epoch=time.time(),
                       estimated_allocated_bytes_reclaimed=sum(x['allocated_bytes'] for x in journal['plan']['items']))
        retirement.save_journal(stage/JOURNAL, journal)
        return journal


def relieve_private(cold: Path, stage: Path, dataset: str, old_sha: str, *, public_cold: Path, catalog: Path,
                    observation: Path, cache: Path, materialized: Path, retention_state: Path,
                    install_lock: Path, reserve_bytes: int, extra_locks=(), guard=lambda: None,
                    apply=False, free_bytes=None, headroom=HEADROOM) -> dict:
    """Free exclusive private D objects only after their fixed E source is sealed.

    The private head and manifest remain in place until their original writer
    commits a replacement. No new backup or independent payload readback is
    performed; pins, dependent manifests, readers and both authorities' locks
    remain binding. Each interrupted unlink is resumed through the same journal.
    """
    from stockagent.data_sync import private_handoff_retirement as private_retirement
    from scripts.run_stream_packed_repack import remaining_install_bytes

    cold, stage, public_cold = Path(cold).absolute(), Path(stage).absolute(), Path(public_cold).absolute()
    dataset = p.validate_slug(dataset, 'private dataset')
    old_sha = p._validate_hash(old_sha, 'private old manifest SHA')
    if (dataset not in stream_repack.PRIVATE_REPACK_DATASETS
            or cold != public_cold/'.local-state/private-history' or p._paths_overlap(public_cold, stage)
            or stage.is_symlink() or stage.resolve(strict=True) != stage
            or type(reserve_bytes) is not int or reserve_bytes < 0
            or type(headroom) is not int or headroom < 0 or type(apply) is not bool):
        raise SnapshotError('invalid private current-head relief scope')
    # A public publisher can create a hardlink to a private object. Hold the
    # existing public and private global owners, with only nonblocking lock
    # acquisition, before calculating or unlinking the final private plan.
    extra_locks = (*extra_locks, public_cold/'.local-state/locks/publish-retention-global.lock')
    prepared = stream_repack._read_prepared(stage, dataset, old_sha)
    stream_repack.require_content_alias_install(stage, dataset, old_sha, prepared)
    old_id = p.validate_slug(prepared['old_snapshot_id'], 'old private snapshot')
    old_relative = f'manifests/{dataset}/{old_id}.json'
    journal = load(stage, dataset, old_sha, prepared)
    if journal is not None and journal.get('private') is not True:
        raise SnapshotError('private current-head relief journal authority differs')
    with retirement._boundary(cold, extra_locks, attempts=1):
        guard()
        latest = p.resolve_latest_packed(cold, dataset, require_objects=False)
        publisher = latest.manifest.get('publisher', {}).get('node_id')
        if latest.manifest_sha256 != old_sha or latest.manifest['snapshot_id'] != old_id:
            raise SnapshotError('private current-head relief original head changed')
        if journal is None and not stream_repack.private_repack_source_allowed(dataset, latest.manifest):
            raise SnapshotError('private current-head relief belongs to an excluded source owner')
        _require_single_private_head(cold, dataset, old_sha, old_id, publisher)
        census = private_retirement._manifest_census(cold)
    old_refs = retirement._refs(latest.manifest)
    dependencies = private_retirement._source_link_protections(cold, census, dataset)
    if old_id in dependencies:
        raise SnapshotError('private current-head relief source is a fixed dependency pin')
    references = {ref: set() for ref in old_refs}
    for relative, expected_signature in census.items():
        manifest = retirement._read_manifest(cold, relative)
        private_retirement.check_dependency_metadata(manifest)
        if stream_repack._signature(cold/relative) != expected_signature:
            raise SnapshotError('private manifest changed during relief census')
        for ref in retirement._refs(manifest) & old_refs:
            references[ref].add(relative)
    generation = _private_catalog_generation(catalog)
    metadata, _, catalog_identity = retirement.old_retirement.catalog_index(
        catalog, observation, set(), cache_path=cache)
    if _private_catalog_generation(catalog) != generation:
        raise SnapshotError('private current-head relief catalog changed during census')
    known = {Path(name).stem for name in census} | {Path(name).stem for name in metadata}
    with retirement._boundary(cold, extra_locks, attempts=1), \
            p._try_publication_lock(cold/'.local-state/locks'/f'publish-{dataset}.lock'), \
            p._try_publication_lock(install_lock), \
            temporary_e_authority.borrow(stage, dataset, old_sha) as view:
        guard()
        if (_private_catalog_generation(catalog) != generation
                or private_retirement._manifest_census(cold) != census):
            raise SnapshotError('private current-head relief manifest census changed')
        if old_id in private_retirement._source_link_protections(cold, census, dataset):
            raise SnapshotError('private current-head relief source gained a fixed dependency pin')
        current = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if (current.manifest_sha256 != old_sha or current.manifest['snapshot_id'] != old_id
                or retirement._refs(current.manifest) != old_refs
                or view.manifest['source'] != current.manifest['source']):
            raise SnapshotError('private current-head relief source changed')
        _require_single_private_head(cold, dataset, old_sha, old_id, publisher)
        known = retirement.include_other_authority_repack_pins(cold, materialized, known | {old_id})
        _require_only_own_pin(materialized, retention_state, known, dataset, old_sha, old_id, stage)
        deletable = {ref for ref, owners in references.items() if owners == {old_relative}}
        deletable.discard(current.manifest['archive']['inventory']['relpath'])
        if journal is None:
            install_path = stage/'install.json'
            if install_path.is_symlink():
                raise SnapshotError('private current-head relief install journal redirected')
            install = json.loads(install_path.read_text()) if install_path.is_file() else {}
            required = remaining_install_bytes(cold, prepared, install)
            available = shutil.disk_usage(cold).free if free_bytes is None else free_bytes
            if type(available) is not int or available < 0:
                raise SnapshotError('private current-head relief free space unknown')
            shortage = max(0, required+reserve_bytes+headroom-available)
            if shortage == 0:
                return {'contract': CONTRACT, 'dataset': dataset, 'private': True, 'state': 'not_needed',
                        'D_required_bytes': required, 'D_free_bytes': available}
            items = sorted(_candidates(cold, deletable), key=lambda item: (-item['allocated_bytes'], item['relative']))
            estimated = sum(item['allocated_bytes'] for item in items)
            if estimated < shortage:
                return {'contract': CONTRACT, 'dataset': dataset, 'private': True,
                        'state': 'insufficient_exclusive_space', 'D_shortfall_bytes': shortage,
                        'exclusive_allocated_bytes': estimated}
            plan = {'old_snapshot_id': old_id, 'old_refs': sorted(old_refs), 'items': items,
                    'catalog_identity_sha256': catalog_identity, 'private_publisher': publisher,
                    'D_required_bytes': required, 'D_free_bytes': available,
                    'D_reserve_bytes': reserve_bytes, 'D_shortfall_bytes': shortage}
            journal = {'contract': CONTRACT, 'dataset': dataset, 'private': True,
                       'old_manifest_sha256': old_sha, 'prepared_sha256': prepared['identity_sha256'],
                       'plan': plan, 'plan_sha256': stream_repack._identity(plan), 'removed': [],
                       'state': 'planned', 'planned_object_count': len(items),
                       'planned_allocated_bytes': estimated, 'temporary_standard_D_head_unavailable': True,
                       'independent_D_readback': False, 'NAS_accepted': False}
        else:
            planned = {item['relative'] for item in journal['plan']['items']}
            if (journal['plan'].get('private_publisher') != publisher
                    or set(journal['plan'].get('old_refs', [])) != old_refs
                    or not planned.issubset(deletable)):
                raise SnapshotError('private current-head relief references changed')
        removed = set(journal['removed'])
        pending = journal.get('pending') or []
        pending = {pending} if isinstance(pending, str) else set(pending)
        paths = [cold/item['relative'] for item in journal['plan']['items']
                 if item['relative'] not in removed
                 and (item['relative'] not in pending or os.path.lexists(cold/item['relative']))]
        if process_references_many(paths, immutable_regular_paths=True):
            raise SnapshotError('private current-head relief object is used by a running process')
        if not apply:
            return {**journal, 'apply': False}
        if journal['state'] == 'completed':
            return journal
        for relative in journal['removed']:
            if os.path.lexists(cold/relative):
                raise SnapshotError('private current-head relief removed name returned')
        retirement.journalled_unlink(cold, stage/JOURNAL, journal, guard=guard, parallel_objects=8)
        journal.update(state='completed', completed_at_epoch=time.time(),
                       estimated_allocated_bytes_reclaimed=sum(x['allocated_bytes'] for x in journal['plan']['items']))
        retirement.save_journal(stage/JOURNAL, journal)
        return journal
