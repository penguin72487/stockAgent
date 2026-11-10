"""Release exact old bulk-landing hardlinks after their dataset is sealed on E.

The ordinary D object remains in place. Removing its second, landing-area
name can make that object eligible for the existing capacity-relief owner;
this transaction itself does not claim to reclaim its allocated blocks.
"""
from __future__ import annotations

from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import stat
import time

from scripts import retire_staged_cold_objects as locks
from stockagent.data_sync import packed_snapshots as p, repack_retirement as retirement
from stockagent.data_sync import stream_repack, temporary_e_authority
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.bulk_landing_alias_retirement.v1'
JOURNAL = 'bulk-landing-alias-retirement.json'
_BATCH = re.compile(r'[0-9]{8}T[0-9]{6}-[0-9a-f]{12}\Z')
_SCOPES = frozenset({'markets', 'ablations', 'cache'})


def _read(path: Path, limit=2*1024*1024) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise SnapshotError('bulk landing control is missing or redirected')
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise SnapshotError('bulk landing control is not an object')
    return value


def _load(stage: Path, dataset: str, old_sha: str) -> dict | None:
    path = stage/JOURNAL
    if not os.path.lexists(path):
        return None
    value = _read(path)
    plan = value.get('plan')
    if (value.get('contract') != CONTRACT or value.get('dataset') != dataset
            or value.get('old_manifest_sha256') != old_sha or not isinstance(plan, dict)
            or value.get('plan_sha256') != stream_repack._identity(plan)
            or value.get('state') not in ('planned', 'removing', 'completed')
            or not isinstance(plan.get('items'), list) or not 0 < len(plan['items']) <= 4096
            or type(value.get('cursor')) is not int or not 0 <= value['cursor'] <= len(plan['items'])
            or value.get('pending') not in (None, value['cursor'])
            or value['state'] == 'completed' and value['cursor'] != len(plan['items'])):
        raise SnapshotError('bulk landing retirement journal differs')
    return value


def _landing(incoming: Path, manifest: dict, old_sha: str) -> tuple[Path, str, dict] | None:
    metadata = manifest.get('metadata', {})
    scope = metadata.get('source_scope')
    if (metadata.get('preservation_contract') != 'bounded_zstd_tar_preservation_v2'
            or scope not in _SCOPES or not manifest['dataset'].startswith('legacy-vast-bulk-')
            or manifest['source'].get('snapshot_root_name') != f'publish-{scope}-parts-v2'):
        return None
    digest = p._validate_hash(metadata.get('compressed_sha256'), 'bulk compressed SHA')
    incoming = Path(incoming).absolute()
    if incoming.is_symlink() or not incoming.is_dir() or incoming.resolve(strict=True) != incoming:
        raise SnapshotError('bulk landing root is unavailable or redirected')
    matches = []
    batches = list(incoming.iterdir())
    if len(batches) > 4096:
        raise SnapshotError('bulk landing root exceeds bounded discovery')
    for batch in batches:
        if not _BATCH.fullmatch(batch.name) or batch.is_symlink() or not batch.is_dir():
            continue
        publication = batch/(scope+'.cold-publication.json')
        receipt = batch/(scope+'.receipt.json')
        if not publication.exists() or not receipt.exists():
            continue
        row, received = _read(publication, 128*1024), _read(receipt, 128*1024)
        if (row.get('dataset') == manifest['dataset'] and row.get('manifest_sha256') == old_sha
                and row.get('snapshot_id') == manifest['snapshot_id']
                and row.get('compressed_sha256') == digest and received.get('compressed_sha256') == digest
                and received.get('scope') == scope):
            root = batch/manifest['source']['snapshot_root_name']
            if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
                raise SnapshotError('bulk landing source root is unavailable or redirected')
            matches.append((root, scope, row))
    if len(matches) > 1:
        raise SnapshotError('bulk landing source identity is ambiguous')
    return matches[0] if matches else None


def _rows(cold: Path, stage: Path, old: dict, view: dict) -> list[dict]:
    old_rows, new_rows = list(p._iter_inventory(cold, old)), list(p._iter_inventory(stage, view))
    if len(old_rows) > 4096 or len(new_rows) != len(old_rows) or len(old_rows) != old['source']['files']:
        raise SnapshotError('bulk landing source inventory scope differs')
    def identity(row):
        name = row.get('path')
        if (row.get('kind') != 'file' or not isinstance(name, str)
                or Path(name).name != name or name in ('.', '..')
                or type(row.get('size')) is not int or row['size'] < 0):
            raise SnapshotError('bulk landing inventory member is not a root file')
        return name, p._validate_hash(row.get('sha256'), 'bulk source SHA'), row['size']
    if [identity(row) for row in old_rows] != [identity(row) for row in new_rows]:
        raise SnapshotError('bulk landing E source inventory differs from original')
    return old_rows


def _same_blob_generation(path: Path, before: list) -> bool:
    actual = stream_repack._signature(path)
    return (len(before) == len(actual) == 7 and actual[1:5] == before[1:5]
            and (actual[0] == before[0] or stream_repack._trusted_device_renumbering(path, actual[0])))


def release(cold: Path, stage: Path, dataset: str, old_sha: str, *, incoming: Path,
            coordinator_lock: Path, ingress_lock: Path, install_lock: Path,
            extra_locks=(), guard=lambda: None, process_check=process_references_many,
            apply=False) -> dict:
    """Retire only observed landing aliases, under the original owners.

    E is already source-hashed and closed. This checks metadata and hardlink
    identity only; it performs no new payload readback or NAS verification.
    """
    cold, stage, incoming = Path(cold).absolute(), Path(stage).absolute(), Path(incoming).absolute()
    dataset, old_sha = p.validate_slug(dataset, 'dataset'), p._validate_hash(old_sha, 'old manifest SHA')
    if (type(apply) is not bool or p._paths_overlap(cold, stage)
            or p._paths_overlap(incoming, stage) or p._paths_overlap(incoming, cold)):
        raise SnapshotError('bulk landing retirement scope differs')
    try:
        with ExitStack() as stack:
            stack.enter_context(p._try_publication_lock(coordinator_lock))
            stack.enter_context(p._try_publication_lock(ingress_lock))
            stack.enter_context(retirement._boundary(cold, extra_locks))
            stack.enter_context(p._try_publication_lock(cold/'.local-state/locks'/f'publish-{dataset}.lock'))
            stack.enter_context(p._try_publication_lock(install_lock))
            view = stack.enter_context(temporary_e_authority.borrow(stage, dataset, old_sha))
            guard()
            old = p.resolve_latest_packed(cold, dataset, require_objects=False)
            if old.manifest_sha256 != old_sha or view.manifest['source'] != old.manifest['source']:
                raise SnapshotError('bulk landing source head or E view changed')
            landing = _landing(incoming, old.manifest, old_sha)
            if landing is None:
                return {'contract': CONTRACT, 'state': 'not_applicable'}
            source, scope, publication = landing
            rows = _rows(cold, stage, old.manifest, view.manifest)
            objects = {row['sha256']: row for row in old.manifest['archive']['objects']}
            wanted = {}
            for row in rows:
                digest = row['sha256']
                storage, obj = row.get('storage', {}), objects.get(digest, {})
                if (storage.get('kind') != 'blob' or storage.get('object_sha256') != digest
                        or obj.get('sha256') != digest or obj.get('bytes') != row['size']):
                    raise SnapshotError('bulk landing member is not the exact original blob')
                wanted[row['path']] = (digest, row['size'], obj['relpath'])
            journal = _load(stage, dataset, old_sha)
            if journal is None:
                items = []
                for name, (digest, size, relative) in wanted.items():
                    alias, blob = source/name, p._path_under(cold, relative, 'bulk original blob')
                    if not os.path.lexists(alias):
                        continue
                    a, b = alias.lstat(), blob.lstat()
                    if (not stat.S_ISREG(a.st_mode) or not stat.S_ISREG(b.st_mode)
                            or a.st_nlink != 2 or b.st_nlink != 2 or not os.path.samestat(a, b)
                            or a.st_size != size):
                        continue  # Never infer the other name of a different or shared inode.
                    items.append({'relative': alias.relative_to(incoming).as_posix(),
                                  'blob_relative': relative, 'sha256': digest, 'bytes': size,
                                  'signature': stream_repack._signature(alias)})
                if not items:
                    return {'contract': CONTRACT, 'state': 'no_matching_aliases'}
                plan = {'batch': source.parent.name, 'scope': scope, 'source_root': source.name,
                        'compressed_sha256': publication['compressed_sha256'],
                        'snapshot_id': old.manifest['snapshot_id'], 'items': items}
                journal = {'contract': CONTRACT, 'dataset': dataset, 'old_manifest_sha256': old_sha,
                           'prepared_sha256': stream_repack._read_prepared(stage, dataset, old_sha)['identity_sha256'],
                           'plan': plan, 'plan_sha256': stream_repack._identity(plan), 'cursor': 0,
                           'state': 'planned', 'allocated_bytes_reclaimed': 0,
                           'independent_D_readback': False, 'NAS_accepted': False}
                if not apply:
                    return {**journal, 'apply': False}
                atomic_write_json(stage/JOURNAL, journal, durable=True)
            plan = journal['plan']
            prepared = stream_repack._read_prepared(stage, dataset, old_sha)
            if (journal.get('prepared_sha256') != prepared['identity_sha256']
                    or plan.get('batch') != source.parent.name or plan.get('scope') != scope
                    or plan.get('source_root') != source.name or plan.get('snapshot_id') != old.manifest['snapshot_id']
                    or plan.get('compressed_sha256') != publication['compressed_sha256']):
                raise SnapshotError('bulk landing retirement source generation changed')
            items = plan['items']
            relatives = [item.get('relative') for item in items if isinstance(item, dict)]
            if len(relatives) != len(items) or len(set(relatives)) != len(items):
                raise SnapshotError('bulk landing retirement members differ')
            for item in items:
                rel = item['relative']
                if (not isinstance(rel, str) or Path(rel).parent != source.relative_to(incoming)
                        or Path(rel).name not in wanted
                        or (item.get('sha256'), item.get('bytes'), item.get('blob_relative')) != wanted[Path(rel).name]
                        or not isinstance(item.get('signature'), list) or len(item['signature']) != 7):
                    raise SnapshotError('bulk landing retirement member identity differs')
            pending_paths = [incoming/item['relative'] for item in items[journal['cursor']:]
                             if os.path.lexists(incoming/item['relative'])]
            blob_paths = [cold/item['blob_relative'] for item in items[journal['cursor']:]]
            if process_check([*pending_paths, *blob_paths], immutable_regular_paths=True):
                raise SnapshotError('bulk landing alias is used by a running process')
            if not apply:
                return {**journal, 'apply': False}
            journal['state'] = 'removing'
            atomic_write_json(stage/JOURNAL, journal, durable=True)
            while journal['cursor'] < len(items):
                guard()
                index = journal['cursor']
                item = items[index]
                alias, blob = incoming/item['relative'], cold/item['blob_relative']
                if blob.is_symlink() or not blob.is_file() or not _same_blob_generation(blob, item['signature']):
                    raise SnapshotError('bulk landing original blob changed')
                if os.path.lexists(alias):
                    if (alias.is_symlink() or not alias.is_file() or not os.path.samefile(alias, blob)
                            or not stream_repack._signature_matches(alias, item['signature'])):
                        raise SnapshotError('bulk landing alias changed before removal')
                    journal['pending'] = index
                    atomic_write_json(stage/JOURNAL, journal, durable=True)
                    alias.unlink()
                    p._fsync_directory(alias.parent)
                elif journal.get('pending') != index:
                    raise SnapshotError('bulk landing alias disappeared without durable intent')
                journal['cursor'] += 1
                journal.pop('pending', None)
                atomic_write_json(stage/JOURNAL, journal, durable=True)
            journal.update(state='completed', completed_at_epoch=time.time())
            atomic_write_json(stage/JOURNAL, journal, durable=True)
            return journal
    except (BlockingIOError, p.PackedPublicationBusy) as error:
        raise SnapshotError('bulk landing original owner busy') from error


def organizer_deferred(directory: Path, receipt: dict, workspace: Path | None = None,
                       cold: Path = Path('/srv/stockagent-packed')) -> bool:
    """Defer old bulk proofs when their exact local source is being migrated.

    A committed replacement is only source-hashed publication evidence, not
    independent recovery or permission to retire a remote source.
    """
    scope = receipt.get('scope')
    if scope not in _SCOPES:
        return False
    publication_path = directory/(scope+'.cold-publication.json')
    if not publication_path.exists():
        return False
    publication = _read(publication_path, 128*1024)
    if (publication.get('preservation_contract') != 'bounded_zstd_tar_preservation_v2'
            or not isinstance(publication.get('dataset'), str)
            or not publication['dataset'].startswith('legacy-vast-bulk-')):
        return False
    dataset = p.validate_slug(publication.get('dataset'), 'bulk dataset')
    old_sha = p._validate_hash(publication.get('manifest_sha256'), 'bulk old manifest SHA')
    if workspace is None:
        from stockagent.data_sync import workspace_volume
        workspace = Path(workspace_volume.configuration()['mount_point'])
    if publication.get('compressed_sha256') != receipt.get('compressed_sha256'):
        raise SnapshotError('bulk publication and received scope identity differ')
    stage = Path(workspace)/'migration/stream-source-repack-v1'/dataset/old_sha
    if os.path.lexists(stage/JOURNAL) and (stage.is_symlink() or not stage.is_dir()
                                           or stage.resolve(strict=True) != stage):
        raise SnapshotError('bulk landing migration stage is redirected')
    journal = _load(stage, dataset, old_sha)
    if journal is not None:
        plan = journal['plan']
        if (directory.name != plan.get('batch') or plan.get('scope') != scope
                or receipt.get('compressed_sha256') != plan.get('compressed_sha256')
                or publication.get('compressed_sha256') != plan.get('compressed_sha256')
                or publication.get('snapshot_id') != plan.get('snapshot_id')):
            raise SnapshotError('bulk landing migration and organizer identity differ')
        return True
    receipts = Path(workspace)/'migration/source-local-exact-repack'/dataset
    if not os.path.lexists(receipts):
        return False
    if receipts.is_symlink() or not receipts.is_dir() or receipts.resolve(strict=True) != receipts:
        raise SnapshotError('bulk replacement receipt namespace redirected')
    paths = list(receipts.glob('*.json'))
    if len(paths) > 256:
        raise SnapshotError('bulk replacement receipts exceed bounded discovery')
    for path in paths:
        if not re.fullmatch(r'[a-f0-9]{64}\.json', path.name):
            continue
        row = _read(path, 128*1024)
        if (row.get('contract') != 'stockagent.source_local_exact_repack.v1'
                or row.get('state') != 'new_D_head_committed_from_exact_E_source'
                or row.get('source_mode') != 'D_streamed_to_E_dataset_pack'
                or row.get('private') is True or row.get('dataset') != dataset
                or row.get('old_manifest_sha256') != old_sha
                or row.get('new_manifest_sha256') != path.stem):
            continue
        snapshot = p.validate_slug(row.get('new_snapshot_id'), 'bulk replacement snapshot')
        fixed = p.resolve_packed_snapshot_id(Path(cold), dataset, snapshot, require_objects=False)
        if (fixed.manifest_sha256 != path.stem
                or fixed.manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256') != old_sha
                or fixed.manifest.get('metadata', {}).get('compressed_sha256') != receipt['compressed_sha256']
                or fixed.manifest.get('metadata', {}).get('source_scope') != scope
                or fixed.manifest.get('metadata', {}).get('preservation_contract') != 'bounded_zstd_tar_preservation_v2'):
            raise SnapshotError('bulk replacement receipt does not match its D manifest')
        return True
    return False
