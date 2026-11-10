"""Daily current-plus-research retention through the existing retirement owner.

Only exact, unexcluded data-root owners are enrolled. The original head,
research/active pins, dependency graph, generation and unlink journal remain
the deletion authority. This module never unpacks raw observations.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import time

from stockagent.data_sync import archive_schedule, packed_snapshots as p
from stockagent.data_sync import private_repack_history, workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.ordinary_archive_cycle.v1'
STATE = Path('/var/lib/stockagent/ordinary-archive')


def _owner_locks(public, cold):
    other = public if cold != public else public/'.local-state/private-history'
    result = [Path('/var/lib/stockagent/backup-stream/cycle.lock'),
              Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')]
    if other.is_dir():
        result.append(other/'.local-state/locks/publish-retention-global.lock')
    return result


def head_generation(cold, dataset):
    """Atomic head replacement changes this directory; no payload walk."""
    path=cold/'heads'/dataset
    if path.is_symlink():
        raise SnapshotError('ordinary archive head directory redirected')
    try:
        info=path.stat()
        return [info.st_dev,info.st_ino,info.st_mtime_ns,info.st_ctime_ns]
    except FileNotFoundError:
        return None


def source_owners(repo):
    """Use the same whole-root enrollment as the canonical daily publisher."""
    from scripts.publish_data_releases import _load_catalog, _source_path
    from stockagent.data_sync.dataset_migration import migration_entries
    from stockagent.data_sync.dataset_version_layout import version_policy
    policy = version_policy(repo)
    catalog = _load_catalog(repo/'configs/data_sync/packed_datasets.json')
    entries = migration_entries(repo, catalog, policy)
    owners, ambiguous = [], []
    for root in sorted(policy['roots']):
        target = (repo/root).resolve()
        matches = [entry for entry in entries if _source_path(entry).resolve() == target
                   and not entry.get('excluded_subtrees') and not entry.get('included_root_files')
                   and not entry.get('publication_family')]
        if len(matches) != 1:
            ambiguous.append(root)
        elif not matches[0].get('closed_source_contract'):
            entry = matches[0]
            owners.append({'dataset':entry['dataset'], 'authority':'public' if entry.get('publish') else 'private',
                           'source_root':root})
    return owners, ambiguous


def validate_current_inventory(cold, dataset, stage, *, guard=lambda:None):
    """Cache a complete metadata validation by exact manifest and generation."""
    from stockagent.data_sync.stream_repack import _signature, _identity
    from stockagent.data_sync import packed_alias_v3
    current = p.resolve_latest_packed(cold, dataset, require_objects=True)
    descriptor = current.manifest['archive']['inventory']
    inventory = p._read_object_path(cold, descriptor['relpath'], 'inventory')
    binding = {'contract':private_repack_history.CURRENT_CONTRACT,
               'dataset':dataset, 'old_manifest_sha256':current.manifest_sha256,
               'snapshot_id':current.manifest['snapshot_id'], 'inventory_sha256':descriptor['sha256'],
               'inventory_signature':list(_signature(inventory)),
               'validator_sha256':hashlib.sha256(Path(p.__file__).read_bytes()+
                                                  Path(packed_alias_v3.__file__).read_bytes()).hexdigest()}
    path = stage/'current-inventory.json'
    if path.exists():
        if path.is_symlink() or path.stat().st_size > 65536:
            raise SnapshotError('ordinary archive inventory receipt redirected or oversized')
        cached = json.loads(path.read_bytes())
        if (cached.get('binding') == binding and cached.get('identity_sha256') == _identity(cached)
                and cached.get('inventory_metadata_validated') is True):
            return current, cached['receipt']
    with p._open_inventory(cold, current.manifest, progress=lambda _row:guard()) as entries:
        checked = p._validate_inventory(current.manifest, entries, retain_object_members=False,
                                        progress=lambda _row:guard())
    if list(_signature(inventory)) != binding['inventory_signature']:
        raise SnapshotError('ordinary current inventory changed during validation')
    receipt = {**binding, 'state':'validated_current_inventory', 'inventory_metadata_validated':True}
    value = {'binding':binding, 'receipt':receipt, 'inventory_metadata_validated':True,
             'counts':checked['counts'], 'observed_at_epoch':time.time(), 'raw_payloads_decoded':False}
    value['identity_sha256'] = _identity(value)
    atomic_write_json(path, value, durable=True)
    return current, receipt


def step(repo, *, now=None, state=STATE, apply=True, guard=lambda:None):
    """At most one dataset transaction per call; other cohorts keep running."""
    now = time.time() if now is None else now
    settings = json.loads((repo/'configs/columnar_storage.json').read_bytes())['dataset_version_lifecycle']
    if settings.get('ordinary_history_retention') != 'current-and-research-pins':
        return None
    window = archive_schedule.validate_window(settings.get('daily_merge_window'))
    if window is None:
        raise SnapshotError('ordinary history retirement needs the explicit daily merge window')
    catchup=settings.get('ordinary_history_migration_catchup',False)
    if type(catchup) is not bool:
        raise SnapshotError('ordinary migration catchup must be explicit')
    if not archive_schedule.allows_start(now, window) and not catchup:
        return None
    workspace_volume.check()
    from stockagent.data_sync.packed_retention import RetentionConfig
    retention = RetentionConfig.load(repo/'configs/data_sync/packed_retention.json', repo_root=repo)
    public = retention.sync_root
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    if state.resolve(strict=True) != state:
        raise SnapshotError('ordinary archive state redirected')
    from datetime import datetime
    from zoneinfo import ZoneInfo
    day = 'migration-catchup' if catchup else datetime.fromtimestamp(now, ZoneInfo(window['timezone'])).date().isoformat()
    with p._try_publication_lock(state/'owner.lock'):
        path = state/'status.json'
        from stockagent.data_sync.packed_object_placement import regular_document
        prior = regular_document(path,limit=4*1024**2) if path.exists() or path.is_symlink() else {}
        if prior.get('contract') != CONTRACT or prior.get('day') != day:
            owners, ambiguous = source_owners(repo)
            prior = {'contract':CONTRACT,'day':day,'owners':owners,'ambiguous_roots':ambiguous,
                     'completed':{},'attempts':{},'state':'checking','NAS_accepted':False}
        owners = prior['owners']
        # Refreshes and history retirement share the same window. Validate
        # each exact head when it is selected; no historical raw hydration.
        def root_for(row):
            return public if row['authority']=='public' else public/'.local-state/private-history'
        candidates = []
        for row in owners:
            key = row['authority']+'/'+row['dataset']
            completed = prior['completed'].get(key)
            try:
                unchanged = (completed is not None and completed.get('head_generation')
                             == head_generation(root_for(row), row['dataset']))
            except OSError:
                unchanged = False  # The selected cohort records its own failure below.
            if not unchanged:
                candidates.append(row)
        candidates.sort(key=lambda row:prior['attempts'].get(row['authority']+'/'+row['dataset'],0))
        if not candidates:
            partial = bool(prior['ambiguous_roots']) or any(
                row.get('state') == 'retained_unique_history' for row in prior['completed'].values())
            prior.update(state='partial' if partial else 'completed', observed_at_epoch=now)
            atomic_write_json(path, prior, durable=True)
            return None
        row = candidates[0]
        key = row['authority']+'/'+row['dataset']
        issues = prior.setdefault('issues', {})
        old_row = prior.get('current')
        if prior.get('state') == 'waiting' and prior.get('reason') and isinstance(old_row, dict):
            old_key = old_row['authority']+'/'+old_row['dataset']
            issues[old_key] = {'reason':prior['reason'],'error_type':prior.get('error_type'),
                               'observed_at_epoch':prior.get('observed_at_epoch')}
        prior['attempts'][key] = now
        prior.pop('reason', None)
        prior.pop('error_type', None)
        prior.update(state='running',current=row,observed_at_epoch=now)
        atomic_write_json(path, prior, durable=True)
        checked_at = time.monotonic()
        def working_guard():
            nonlocal checked_at
            guard()
            if time.monotonic()-checked_at >= 15:
                # A complete inventory may take minutes to decode. This is
                # an owner heartbeat, not a fabricated completion counter.
                prior['observed_at_epoch'] = time.time()
                atomic_write_json(path, prior, durable=False)
                checked_at = time.monotonic()
        cold = root_for(row)
        try:
            working_guard()
            current = p.resolve_latest_packed(cold,row['dataset'],require_objects=False)
            from stockagent.data_sync.superseded_repack_history import _sole_current_head
            sole=_sole_current_head(cold,row['dataset'])
            if sole is not None:
                prior['completed'][key]={'state':'no_unpinned_history',
                    'head_generation':head_generation(cold,row['dataset']),
                    'manifest_sha256':current.manifest_sha256}
                prior.update(state='checking',observed_at_epoch=time.time())
                atomic_write_json(path,prior,durable=True)
                return None
            stage=state/'datasets'/row['authority']/row['dataset']/current.manifest_sha256
            stage.mkdir(mode=0o700,parents=True,exist_ok=True)
            locks = _owner_locks(public, cold)
            # A long writer may retain a publication read lease. Probe the
            # existing retirement admission before decoding millions of
            # inventory rows; the final retirement boundary checks it again.
            from stockagent.data_sync import repack_retirement
            with repack_retirement._boundary(cold, locks):
                working_guard()
            current, receipt = validate_current_inventory(cold,row['dataset'],stage,guard=working_guard)
            result=private_repack_history.retire(cold,stage,receipt,
                public_index=None, materialized=retention.materialized_root,
                retention_state=retention.state_dir,
                extra_locks=locks,guard=working_guard,apply=apply,current_head_only=True)
            issues.pop(key, None)
            prior['last_result']={k:result.get(k) for k in ('state','dataset','versions_removed',
                'object_bytes_removed','protected_versions','estimated_allocated_bytes_reclaimed',
                'estimated_E_allocated_bytes_reclaimed','coverage_protected_versions')}
            if apply and result.get('state') == 'completed':
                totals = prior.setdefault('totals', {})
                for name in ('versions_removed', 'object_bytes_removed',
                             'estimated_allocated_bytes_reclaimed', 'estimated_E_allocated_bytes_reclaimed'):
                    totals[name] = totals.get(name, 0) + result.get(name, 0)
            if result['state'] in {'no_unpinned_history','retained_unique_history'}:
                prior['completed'][key]={**prior['last_result'],
                    'head_generation':head_generation(cold,row['dataset'])}
            prior.update(state='checking',observed_at_epoch=time.time())
            atomic_write_json(path,prior,durable=True)
            return result.get('versions_removed',0)>0
        except (OSError,ValueError,RuntimeError,SnapshotError) as error:
            issues[key]={'reason':str(error)[:300],'error_type':type(error).__name__,
                         'observed_at_epoch':time.time()}
            prior.update(state='waiting',observed_at_epoch=time.time(),
                         reason=str(error)[:300],error_type=type(error).__name__)
            atomic_write_json(path,prior,durable=True)
            return None
