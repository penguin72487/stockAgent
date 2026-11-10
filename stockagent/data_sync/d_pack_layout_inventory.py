"""Read current D manifest metadata without scanning or opening data payloads.

Object names and content identities are separate totals. Neither is allocated
disk usage, a recovery proof, or permission to remove a referenced object.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
from functools import lru_cache
import json
import os
from pathlib import Path
import stat

from stockagent.data_sync import packed_snapshots as p
from stockagent.data_sync.desync_snapshots import SnapshotError

CONTRACT = 'stockagent.d_pack_layout_inventory.v1'
MAX_DATASETS = 20_000
MAX_MANIFESTS = 100_000
MAX_DOCUMENT_BYTES = 32*1024**2


def public_status(value, *, now):
    """Publish aggregate names/lengths only, never private source identities."""
    if not isinstance(value, dict) or value.get('contract') != CONTRACT+'.presence':
        return None
    try:
        observed = datetime.fromisoformat(value['observed_at_utc'])
        if observed.tzinfo is None or observed.timestamp() > now+120:
            return None
    except (ValueError, TypeError, KeyError):
        return None
    def count(value):
        return value if type(value) is int and 0 <= value < 2**53 else None
    labels={'new_only':'新封裝','old_only':'舊封裝',
            'shared_old_new':'新舊共用','unconfirmed':'增量／混合待確認'}
    groups=value.get('groups')
    if not isinstance(groups, dict) or set(groups) != set(labels):
        return None
    rows=[]
    for name,label in labels.items():
        row=groups[name]
        if not isinstance(row, dict):
            return None
        clean={key:count(row.get(key)) for key in ('present_names','present_name_bytes',
            'placed_on_E_names','placed_on_E_name_bytes','missing_names','other_names')}
        if any(number is None for number in clean.values()):
            return None
        rows.append({'id':name,'label':label,**clean})
    return {'observed_at_utc':observed.isoformat(),'stale':now-observed.timestamp()>12*3600,
        'groups':rows,'incomplete_datasets':count(value.get('incomplete_datasets')),
        'dataset_count':count(value.get('dataset_count')),
        'present_allocated_bytes_hardlinks_once':count(value.get('present_allocated_bytes_hardlinks_once')),
        'scope':'non_atomic_observed_names_not_disk_usage_or_recovery',
        'retirement_authorized':False,'payload_SHA_verified':False}


def _direct_layout(root: Path, workspace: Path, name: str, manifest: dict) -> bool:
    """Verify direct encodings only; do not replay every historical base chain.

    Incremental/mixed layouts remain explicitly unconfirmed. Classification
    never needs the retirement owner's much more expensive full lineage walk.
    """
    archive = manifest['archive']
    if (archive.get('new_pack_layout') != 'source-dataset-v1'
            or archive.get('reused_files') != 0 or archive.get('external_reused_files', 0)):
        return False
    selection = manifest['source'].get('selection') or {}
    if not isinstance(selection, dict):
        return False
    if not selection.get('retained_source_contract') and not selection.get('incremental_base'):
        return True
    from stockagent.data_sync import repack_generation_evidence as evidence, stream_repack
    old = manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256')
    try:
        p._validate_hash(old, 'layout original manifest')
        prepared = stream_repack._prepared_metadata(workspace/'migration/stream-source-repack-v1'/name/old, name, old)
        evidence.transition(root, workspace, name, old_sha=old, old_id=prepared['old_snapshot_id'],
            new_sha=hashlib.sha256(p._canonical_json_bytes(manifest)).hexdigest(), new_id=manifest['snapshot_id'],
            private=root.name == 'private-history' and root.parent.name == '.local-state',
            files=manifest['source']['files'], logical_bytes=manifest['source']['logical_bytes'],
            current_manifest=manifest)
        return True
    except (OSError, ValueError, TypeError, KeyError, SnapshotError):
        return False


def _read(path: Path) -> dict:
    if path.is_symlink():
        raise SnapshotError('layout inventory metadata redirected')
    with path.open('rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_DOCUMENT_BYTES:
            raise SnapshotError('layout inventory metadata is not bounded regular data')
        raw = stream.read(MAX_DOCUMENT_BYTES+1)
        after = os.fstat(stream.fileno())
    identity = lambda row: (row.st_dev, row.st_ino, row.st_size, row.st_mtime_ns, row.st_ctime_ns)
    if (len(raw) > MAX_DOCUMENT_BYTES or identity(before) != identity(after)
            or identity(after) != identity(path.stat(follow_symlinks=False))):
        raise SnapshotError('layout inventory metadata changed')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise SnapshotError('layout inventory metadata is not an object')
    return value


def collect(cold: Path, workspace: Path, *, workers=4, progress=lambda _event: None,
            layout_check=None) -> dict:
    """Classify fixed public/private manifests under each original dataset lock.

    Different datasets can be observed at different times. Publishing uses
    shared global locks too; this observer never holds an exclusive global
    owner or a dataset lock while reading another dataset.
    """
    if type(workers) is not int or not 1 <= workers <= 4:
        raise SnapshotError('layout inventory concurrency must be bounded')
    cold, workspace = Path(cold).absolute(), Path(workspace).absolute()
    if cold.is_symlink() or cold.resolve(strict=True) != cold:
        raise SnapshotError('layout inventory D root redirected')
    if layout_check is None:
        layout_check = lambda root, name, manifest: _direct_layout(root, workspace, name, manifest)
    jobs = []
    for authority, root in (('public', cold), ('private', cold/'.local-state/private-history')):
        directory = root/'manifests'
        if not directory.exists():
            continue
        if directory.is_symlink() or directory.resolve(strict=True) != directory:
            raise SnapshotError('layout inventory manifest root redirected')
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    jobs.append((authority, root, p.validate_slug(entry.name, 'inventory dataset')))
                    if len(jobs) > MAX_DATASETS:
                        raise SnapshotError('layout inventory dataset bound exceeded')
    started = datetime.now(timezone.utc).isoformat()

    def scan(job):
        authority, root, name = job
        try:
            with ExitStack() as owners:
                locks = root/'.local-state/locks'
                owners.enter_context(p._try_publication_lock(locks/'publish-retention-global.lock', shared=True))
                owners.enter_context(p._try_publication_lock(locks/f'publish-{name}.lock', shared=True))
                heads = {}
                directory = root/'heads'/name
                if directory.exists():
                    if directory.is_symlink() or directory.resolve(strict=True) != directory:
                        raise SnapshotError('layout inventory head root redirected')
                    for path in directory.glob('*.json'):
                        value = _read(path)
                        if value.get('dataset') != name:
                            raise SnapshotError('layout inventory head dataset differs')
                        snapshot = p.validate_slug(value.get('snapshot_id'), 'inventory head snapshot')
                        digest = p._validate_hash(value.get('manifest_sha256'), 'inventory head SHA')
                        if snapshot in heads and heads[snapshot] != digest:
                            raise SnapshotError('layout inventory head identities conflict')
                        heads[snapshot] = digest
                paths = sorted((root/'manifests'/name).glob('*.json'))
                if len(paths) > MAX_MANIFESTS:
                    raise SnapshotError('layout inventory manifest bound exceeded')
                results = []
                for path in paths:
                    manifest = _read(path)
                    p._validate_manifest(manifest)
                    digest = hashlib.sha256(p._canonical_json_bytes(manifest)).hexdigest()
                    if manifest['dataset'] != name or path.name != manifest['snapshot_id']+'.json':
                        raise SnapshotError('layout inventory manifest path differs')
                    current = manifest['snapshot_id'] in heads
                    if current and heads[manifest['snapshot_id']] != digest:
                        raise SnapshotError('layout inventory head SHA differs')
                    archive = manifest['archive']
                    confirmed = layout_check(root, name, manifest)
                    layout = ('new' if confirmed else 'old' if
                        archive.get('new_pack_layout') != 'source-dataset-v1' else 'unconfirmed')
                    results.append({'authority': authority, 'dataset': name,
                        'snapshot_id': manifest['snapshot_id'], 'manifest_sha256': digest,
                        'current': current, 'layout': layout,
                        'logical_bytes': manifest['source']['logical_bytes'],
                        'files': manifest['source']['files'],
                        'objects': [dict(item) for item in archive['objects']],
                        'inventory': dict(archive['inventory'])})
                if heads and not set(heads).issubset({row['snapshot_id'] for row in results}):
                    raise SnapshotError('layout inventory current manifest missing')
                return results, None
        except (OSError, ValueError, TypeError, KeyError, SnapshotError) as error:
            return [], {'authority': authority, 'dataset': name, 'error_type': type(error).__name__,
                        'reason': str(error)[:240]}

    objects, manifests, errors = {}, [], []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='cold-layout') as pool:
        futures = [pool.submit(scan, job) for job in sorted(jobs)]
        for number, future in enumerate(as_completed(futures), 1):
            rows, error = future.result()
            if error is not None:
                errors.append(error)
            for row in rows:
                refs = row.pop('objects')
                row['encoded_reference_bytes'] = sum(item['bytes'] for item in refs)
                row['object_count'] = len(refs)
                manifests.append(row)
                if len(manifests) > MAX_MANIFESTS:
                    raise SnapshotError('layout inventory total manifest bound exceeded')
                for item in refs:
                    relative = p._safe_relative_path(item['relpath'], 'inventory object').as_posix()
                    key = row['authority']+':'+relative
                    prior = objects.setdefault(key, {'authority': row['authority'], 'relative': relative,
                        'sha256': item['sha256'], 'bytes': item['bytes'], 'layouts': set(),
                        'datasets': set(), 'current_reference': False})
                    if prior['sha256'] != item['sha256'] or prior['bytes'] != item['bytes']:
                        raise SnapshotError('layout inventory object descriptors conflict')
                    prior['layouts'].add(row['layout'])
                    prior['datasets'].add(row['dataset'])
                    prior['current_reference'] |= row['current']
            progress({'datasets_completed': number, 'datasets_total': len(jobs),
                      'manifests': len(manifests), 'objects': len(objects), 'errors': len(errors)})
    totals = {key: {'object_names': 0, 'reference_bytes': 0} for key in
              ('new_only', 'old_only', 'shared_old_new', 'unconfirmed')}
    content = {}
    for item in objects.values():
        kinds = item['layouts']
        group = ('shared_old_new' if {'new', 'old'}.issubset(kinds) else
                 'new_only' if kinds == {'new'} else 'old_only' if kinds == {'old'} else 'unconfirmed')
        item['layout_group'] = group
        item['layouts'], item['datasets'] = sorted(kinds), sorted(item['datasets'])
        totals[group]['object_names'] += 1
        totals[group]['reference_bytes'] += item['bytes']
        if item['sha256'] in content and content[item['sha256']] != item['bytes']:
            raise SnapshotError('layout inventory content lengths conflict')
        content[item['sha256']] = item['bytes']
    return {'contract': CONTRACT, 'started_at_utc': started,
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'complete': not errors, 'dataset_count': len(jobs), 'manifest_count': len(manifests),
        'groups': totals, 'referenced_object_names': len(objects),
        'referenced_name_bytes': sum(item['bytes'] for item in objects.values()),
        'distinct_content_objects': len(content), 'distinct_content_bytes': sum(content.values()),
        'payloads_read': False, 'object_presence_checked': False, 'allocated_bytes_checked': False,
        'atomic_cross_dataset_snapshot': False, 'retirement_authorized': False,
        'manifests': sorted(manifests, key=lambda row: (row['authority'], row['dataset'], row['snapshot_id'])),
        'objects': sorted(objects.values(), key=lambda row: (row['authority'], row['relative'])),
        'errors': sorted(errors, key=lambda row: (row['authority'], row['dataset']))}


def object_presence(cold: Path, objects: list[dict], *, workers=4,
                    progress=lambda _event: None) -> dict:
    """Observe exact referenced names without reading or hashing payloads.

    Manifest references can outlive retired files. Keep missing and changed
    names separate, and count hardlinked inode allocation once across both
    authorities. These observations never authorize removal or prove recovery.
    """
    cold = Path(cold).absolute()
    if (type(workers) is not int or not 1 <= workers <= 4
            or cold.is_symlink() or cold.resolve(strict=True) != cold
            or len(objects) > 1_000_000):
        raise SnapshotError('invalid presence inventory root or bounds')
    seen = set()
    jobs = []
    for item in objects:
        authority = item.get('authority')
        group = item.get('layout_group')
        size = item.get('bytes')
        if (authority not in {'public', 'private'} or group not in
                {'new_only', 'old_only', 'shared_old_new', 'unconfirmed'}
                or type(size) is not int or size < 0):
            raise SnapshotError('invalid presence inventory descriptor')
        relative = p._safe_relative_path(item.get('relative'), 'presence object')
        p._validate_hash(item.get('sha256'), 'presence object SHA')
        key = (authority, relative.as_posix())
        if key in seen:
            raise SnapshotError('duplicate presence object descriptor')
        seen.add(key)
        root = cold if authority == 'public' else cold/'.local-state/private-history'
        jobs.append((item, root/relative))
    started = datetime.now(timezone.utc).isoformat()

    @lru_cache(maxsize=100_000)
    def local_parent(parent):
        # Observe each immutable object directory once. Resolving every
        # ancestor for every small blob would turn a census into millions of
        # DrvFS metadata round trips. This is only a non-atomic observation.
        return parent.resolve(strict=True) == parent

    def observe(job):
        item, path = job
        row = {key: item[key] for key in ('authority', 'relative', 'sha256', 'bytes', 'layout_group')}
        try:
            if not local_parent(path.parent):
                return {**row, 'state': 'redirected'}, None
            value = path.stat(follow_symlinks=False)
            if not stat.S_ISREG(value.st_mode):
                return {**row, 'state': 'not_regular'}, None
            if value.st_size != item['bytes']:
                return {**row, 'state': 'length_changed', 'observed_bytes': value.st_size}, None
            return {**row, 'state': 'present', 'allocated_bytes': value.st_blocks*512,
                    'hardlinks': value.st_nlink}, (value.st_dev, value.st_ino)
        except FileNotFoundError:
            # The logical D name may now have an exact, source-hashed E
            # placement. Keep that tier distinct from both D usage and loss.
            root = cold if item['authority'] == 'public' else cold/'.local-state/private-history'
            try:
                physical = p._read_object_path(root, item['relative'], label='presence object')
                if physical != path:
                    value = physical.stat(follow_symlinks=False)
                    if stat.S_ISREG(value.st_mode) and value.st_size == item['bytes']:
                        return {**row, 'state': 'placed_on_E', 'allocated_bytes': value.st_blocks*512,
                                'hardlinks': value.st_nlink}, (value.st_dev, value.st_ino)
                    return {**row, 'state': 'placement_changed'}, None
            except (OSError, ValueError, SnapshotError):
                return {**row, 'state': 'placement_unavailable'}, None
            return {**row, 'state': 'missing'}, None
        except OSError as error:
            return {**row, 'state': 'unreadable', 'error_type': type(error).__name__}, None

    rows, inodes, E_inodes = [], {}, {}
    groups = {key: {'present_names': 0, 'present_name_bytes': 0,
                    'missing_names': 0, 'missing_reference_bytes': 0, 'other_names': 0,
                    'placed_on_E_names': 0, 'placed_on_E_name_bytes': 0}
              for key in ('new_only', 'old_only', 'shared_old_new', 'unconfirmed')}
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='cold-presence') as pool:
        # map preserves lexical locality instead of seeking by completion order.
        for number, (row, inode) in enumerate(pool.map(observe, jobs), 1):
            rows.append(row)
            group = groups[row['layout_group']]
            if row['state'] == 'present':
                group['present_names'] += 1
                group['present_name_bytes'] += row['bytes']
                prior = inodes.setdefault(inode, {'bytes': row['allocated_bytes'], 'groups': set()})
                if prior['bytes'] != row['allocated_bytes']:
                    raise SnapshotError('presence allocation changed across hardlinked names')
                prior['groups'].add(row['layout_group'])
            elif row['state'] == 'placed_on_E':
                group['placed_on_E_names'] += 1
                group['placed_on_E_name_bytes'] += row['bytes']
                E_inodes[inode] = row['allocated_bytes']
            elif row['state'] == 'missing':
                group['missing_names'] += 1
                group['missing_reference_bytes'] += row['bytes']
            else:
                group['other_names'] += 1
            if number % 256 == 0 or number == len(jobs):
                progress({'objects_completed': number, 'objects_total': len(jobs)})
    return {'contract': CONTRACT+'.presence', 'started_at_utc': started,
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'groups': groups,
            'present_unique_inodes': len(inodes),
            'present_allocated_bytes_hardlinks_once': sum(x['bytes'] for x in inodes.values()),
            'placed_E_allocated_bytes_hardlinks_once': sum(E_inodes.values()),
            'shared_group_allocated_bytes': sum(x['bytes'] for x in inodes.values() if len(x['groups']) > 1),
            'objects': rows, 'payloads_read': False, 'payload_SHA_verified': False,
            'atomic_cross_dataset_snapshot': False, 'retirement_authorized': False}
