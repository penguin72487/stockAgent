"""Retire old landing names whose exact blobs were already replaced on D.

The original owner and locks remain authoritative.  A completed prior D
retirement proves which inode each old blob used; the new source-hashed D
inventory proves which content is retained.  No payload readback is claimed.
"""
from __future__ import annotations

from contextlib import ExitStack
import hashlib
import os
from pathlib import Path
import re
import stat
import time

from stockagent.data_sync import bulk_archive, bulk_original_carrier_retirement as carrier
from stockagent.data_sync import packed_snapshots as p, repack_retirement as retirement, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.bulk_committed_landing_retirement.v1'
JOURNAL = 'bulk-committed-landing-retirement.json'


def completed_reclamation(workspace: Path, directory: Path, scope: str) -> tuple[int, int] | None:
    """Summarize a completed journal even if a later owner attempt is deferred."""
    if scope not in {'markets', 'ablations', 'cache'}:
        raise SnapshotError('bulk landing summary scope invalid')
    publication_path, received_path = (directory/(scope+'.cold-publication.json'),
                                       directory/(scope+'.receipt.json'))
    if not publication_path.exists() or not received_path.exists():
        return None
    publication, received = carrier._read(publication_path, 128*1024), carrier._read(received_path, 128*1024)
    if publication.get('preservation_contract') != bulk_archive.PARTS_CONTRACT:
        return None
    dataset = p.validate_slug(publication.get('dataset'), 'bulk dataset')
    old_sha = p._validate_hash(publication.get('manifest_sha256'), 'bulk source SHA')
    stage = Path(workspace)/'migration/stream-source-repack-v1'/dataset/old_sha
    path = stage/JOURNAL
    if not os.path.lexists(path):
        return None
    if (stage.is_symlink() or not stage.is_dir() or stage.resolve(strict=True) != stage
            or path.is_symlink() or not path.is_file()):
        raise SnapshotError('bulk landing summary journal redirected')
    value = retirement.load_journal(path)
    if value.get('state') != 'completed':
        return None
    plan, amount = value.get('plan'), value.get('estimated_allocated_bytes_reclaimed')
    if (value.get('contract') != CONTRACT or value.get('dataset') != dataset or not isinstance(plan, dict)
            or value.get('plan_sha256') != stream_repack._identity(plan) or plan.get('dataset') != dataset
            or plan.get('old_manifest_sha256') != old_sha or plan.get('batch') != directory.name
            or plan.get('scope') != scope or publication.get('source_scope') != scope
            or received.get('scope') != scope or publication.get('compressed_sha256') != received.get('compressed_sha256')
            or plan.get('publication_sha256') != hashlib.sha256(publication_path.read_bytes()).hexdigest()
            or plan.get('received_sha256') != hashlib.sha256(received_path.read_bytes()).hexdigest()
            or not isinstance(plan.get('items'), list) or not 0 < len(plan['items']) <= 4096
            or not all(isinstance(item, dict) and type(item.get('allocated_bytes')) is int
                       and item['allocated_bytes'] >= 0 and isinstance(item.get('relative'), str)
                       for item in plan['items'])
            or value.get('removed') != [item['relative'] for item in plan['items']]
            or value.get('pending') is not None or value.get('independent_D_readback') is not False
            or value.get('NAS_accepted') is not False or type(amount) is not int or not 0 <= amount <= 2**53-1
            or amount != sum(item['allocated_bytes'] for item in plan['items'])):
        raise SnapshotError('bulk landing completed journal differs')
    return len(plan['items']), amount


def _same_volume(cold: Path, incoming: Path, current_device: int, old_device: int) -> bool:
    if current_device == old_device:
        return True
    # The canonical packed mount and the landing mount are separate 9p
    # namespaces for the same D: NTFS volume, so st_dev is not its identity.
    if (cold != Path('/srv/stockagent-packed') or incoming !=
            Path('/srv/stockagent-d-volume/stockagent-cold-primary/remote-artifact-incoming')):
        return False
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.packed_backup import mounted_volume
    mount = Path('/srv/stockagent-d-volume')
    alias = mount/'stockagent-cold-primary/packed'
    _check_d_primary_mount(cold)
    if (mount.is_symlink() or alias.is_symlink() or alias.resolve(strict=True) != alias
            or mounted_volume(mount)[2] != 'D:' or alias.stat().st_dev != current_device
            or cold.stat().st_ino != alias.stat().st_ino):
        raise SnapshotError('bulk landing and canonical D volume identity differs')
    return True


def _same_retired_inode(path: Path, signature: list, volume_check) -> bool:
    actual = stream_repack._signature(path)
    # ctime and link count necessarily change after the old blob name is
    # unlinked; mode, file ID, size and mtime must remain exactly the same.
    return (isinstance(signature, list) and len(signature) == 7 and signature[-1] == 2
            and actual[-1] == 1 and actual[1:5] == signature[1:5]
            and volume_check(actual[0], signature[0]))


def retire(directory: Path, receipt: dict, *, workspace: Path, cold: Path, incoming: Path,
           coordinator_fd: int, coordinator_lock: Path, ingress_lock: Path, install_lock: Path,
           extra_locks=(), guard=lambda: None, process_check=process_references_many,
           apply=False) -> dict:
    directory, workspace, cold, incoming = (Path(x).absolute() for x in (directory, workspace, cold, incoming))
    scope = receipt.get('scope')
    if (type(apply) is not bool or scope not in {'markets', 'ablations', 'cache'}
            or directory.parent != incoming or not re.fullmatch(r'[0-9]{8}T[0-9]{6}-[0-9a-f]{12}', directory.name)
            or directory.is_symlink() or directory.resolve(strict=True) != directory
            or not carrier._coordinator_held(coordinator_fd, coordinator_lock)):
        raise SnapshotError('bulk landing owner or scope differs')
    publication_path, received_path = (directory/(scope+'.cold-publication.json'),
                                       directory/(scope+'.receipt.json'))
    if not publication_path.exists():
        return {'contract': CONTRACT, 'state': 'not_applicable'}
    publication, received = carrier._read(publication_path, 128*1024), carrier._read(received_path, 128*1024)
    if received != receipt:
        raise SnapshotError('bulk landing received receipt changed')
    if publication.get('preservation_contract') != bulk_archive.PARTS_CONTRACT:
        return {'contract': CONTRACT, 'state': 'not_applicable'}
    dataset = p.validate_slug(publication.get('dataset'), 'bulk dataset')
    old_sha = p._validate_hash(publication.get('manifest_sha256'), 'bulk source SHA')
    if (not dataset.startswith('legacy-vast-bulk-'+scope+'-parts-v2-')
            or publication.get('source_scope') != scope
            or publication.get('compressed_sha256') != receipt.get('compressed_sha256')):
        raise SnapshotError('bulk landing publication identity differs')
    source = directory/('publish-'+scope+'-parts-v2')
    if source.is_symlink() or not source.is_dir() or source.resolve(strict=True) != source:
        raise SnapshotError('bulk landing source root is unavailable or redirected')
    stage = workspace/'migration/stream-source-repack-v1'/dataset/old_sha
    if not stage.exists():
        return {'contract': CONTRACT, 'state': 'waiting_E_stage_metadata'}
    if stage.is_symlink() or not stage.is_dir() or stage.resolve(strict=True) != stage:
        raise SnapshotError('bulk landing E stage redirected')
    install_path, old_journal_path = stage/'install.json', stage/'retirement.json'
    if not install_path.is_file() or not old_journal_path.is_file():
        return {'contract': CONTRACT, 'state': 'waiting_D_commit_or_old_retirement'}
    if old_journal_path.is_symlink():
        raise SnapshotError('bulk old retirement journal redirected')
    install = carrier._read(install_path, 8*1024*1024)
    if not isinstance(install.get('new_manifest_sha256'), str) or not isinstance(install.get('snapshot_id'), str):
        return {'contract': CONTRACT, 'state': 'waiting_D_commit_or_old_retirement'}
    new_sha = p._validate_hash(install['new_manifest_sha256'], 'bulk replacement SHA')
    new_id = p.validate_slug(install['snapshot_id'], 'bulk replacement snapshot')
    commit_path = workspace/'migration/source-local-exact-repack'/dataset/(new_sha+'.json')
    if not commit_path.is_file():
        return {'contract': CONTRACT, 'state': 'waiting_D_commit_or_old_retirement'}
    commit = carrier._read(commit_path, 128*1024)
    if (commit.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or commit.get('state') != 'new_D_head_committed_from_exact_E_source'
            or commit.get('source_mode') != 'D_streamed_to_E_dataset_pack'
            or commit.get('private') is True or commit.get('dataset') != dataset
            or commit.get('old_manifest_sha256') != old_sha
            or commit.get('new_manifest_sha256') != new_sha or commit.get('new_snapshot_id') != new_id):
        raise SnapshotError('bulk landing new D receipt differs')
    publication_sha = hashlib.sha256(publication_path.read_bytes()).hexdigest()
    received_sha = hashlib.sha256(received_path.read_bytes()).hexdigest()
    try:
        with ExitStack() as stack:
            stack.enter_context(p._try_publication_lock(ingress_lock))
            stack.enter_context(retirement._boundary(cold, extra_locks, attempts=5))
            stack.enter_context(p._try_publication_lock(cold/'.local-state/locks'/f'publish-{dataset}.lock'))
            stack.enter_context(p._try_publication_lock(install_lock))
            guard()
            if (not carrier._coordinator_held(coordinator_fd, coordinator_lock)
                    or hashlib.sha256(publication_path.read_bytes()).hexdigest() != publication_sha
                    or hashlib.sha256(received_path.read_bytes()).hexdigest() != received_sha):
                raise SnapshotError('bulk landing owner or source controls changed')
            prepared = stream_repack._prepared_metadata(stage, dataset, old_sha)
            replacement = retirement.check_replacement(cold, stage, dataset, old_sha, new_sha, new_id, prepared)
            volume_pairs = {}
            def volume_check(actual, recorded):
                pair = (actual, recorded)
                if pair not in volume_pairs:
                    volume_pairs[pair] = _same_volume(cold, incoming, actual, recorded)
                return volume_pairs[pair]
            old = retirement.load_journal(old_journal_path)
            old_plan = old.get('plan')
            if (old.get('contract') != retirement.CONTRACT or old.get('state') != 'completed'
                    or old.get('dataset') != dataset or old.get('old_manifest_sha256') != old_sha
                    or old.get('new_manifest_sha256') != new_sha or old.get('prepared_sha256') != prepared['identity_sha256']
                    or not isinstance(old_plan, dict) or old.get('plan_sha256') != stream_repack._identity(old_plan)
                    or old_plan.get('new_snapshot_id') != new_id or not isinstance(old_plan.get('items'), list)
                    or not all(isinstance(row, dict) for row in old_plan['items'])
                    or old.get('removed') != [row.get('relative') for row in old_plan['items']]):
                raise SnapshotError('bulk landing old retirement evidence differs')
            old_objects = {row['relative']: row for row in old_plan['items']
                           if isinstance(row, dict) and row.get('kind') == 'object'}
            rows = list(p._iter_inventory(cold, replacement.manifest))
            wanted = {}
            for row in rows:
                name, size = row.get('path'), row.get('size')
                if (row.get('kind') != 'file' or not isinstance(name, str) or Path(name).name != name
                        or name in ('.', '..') or type(size) is not int or size < 0 or name in wanted):
                    raise SnapshotError('bulk landing replacement inventory differs')
                digest = p._validate_hash(row.get('sha256'), 'bulk landing member SHA')
                relative = f'objects/blobs/{digest[:2]}/{digest}.blob'
                wanted[name] = (digest, size, relative, old_objects.get(relative))
            journal_path = stage/JOURNAL
            if journal_path.is_symlink():
                raise SnapshotError('bulk landing retirement journal redirected')
            journal = retirement.load_journal(journal_path) if journal_path.is_file() else None
            if journal is None:
                items = []
                for name, (digest, size, relative, old_item) in wanted.items():
                    path = source/name
                    if not os.path.lexists(path) or not old_item:
                        continue
                    info = path.lstat()
                    if (not stat.S_ISREG(info.st_mode) or info.st_size != size or old_item.get('bytes') != size
                            or not _same_retired_inode(path, old_item.get('signature'), volume_check)):
                        continue
                    items.append({'relative': path.relative_to(incoming).as_posix(), 'kind': 'object',
                                  'sha256': digest, 'blob_relative': relative, 'bytes': size,
                                  'signature': stream_repack._signature(path), 'allocated_bytes': info.st_blocks*512})
                if not items:
                    return {'contract': CONTRACT, 'state': 'no_matching_aliases'}
                plan = {'batch': directory.name, 'scope': scope, 'dataset': dataset,
                        'old_manifest_sha256': old_sha, 'new_manifest_sha256': new_sha,
                        'new_snapshot_id': new_id, 'old_retirement_plan_sha256': old['plan_sha256'],
                        'publication_sha256': publication_sha, 'received_sha256': received_sha, 'items': items}
                journal = {'contract': CONTRACT, 'dataset': dataset, 'plan': plan,
                           'plan_sha256': stream_repack._identity(plan), 'removed': [], 'state': 'planned',
                           'independent_D_readback': False, 'NAS_accepted': False}
                if not apply:
                    return {**journal, 'apply': False}
                retirement.save_journal(journal_path, journal)
            plan = journal.get('plan')
            expected = {'batch': directory.name, 'scope': scope, 'dataset': dataset,
                        'old_manifest_sha256': old_sha, 'new_manifest_sha256': new_sha,
                        'new_snapshot_id': new_id, 'old_retirement_plan_sha256': old['plan_sha256'],
                        'publication_sha256': publication_sha, 'received_sha256': received_sha}
            if (journal.get('contract') != CONTRACT or journal.get('dataset') != dataset
                    or journal.get('state') not in ('planned', 'removing', 'completed')
                    or not isinstance(plan, dict) or journal.get('plan_sha256') != stream_repack._identity(plan)
                    or any(plan.get(k) != v for k, v in expected.items())
                    or not isinstance(plan.get('items'), list) or not 0 < len(plan['items']) <= 4096):
                raise SnapshotError('bulk landing retirement identity differs')
            for item in plan['items']:
                rel = item.get('relative')
                name = Path(rel).name if isinstance(rel, str) else ''
                target = wanted.get(name)
                original = target[3] if target else None
                signature = item.get('signature')
                old_signature = original.get('signature') if original else None
                if (not target or Path(rel).parent != source.relative_to(incoming)
                        or item.get('kind') != 'object'
                        or (item.get('sha256'), item.get('bytes'), item.get('blob_relative')) != target[:3]
                        or not isinstance(signature, list) or len(signature) != 7 or signature[-1] != 1
                        or not isinstance(old_signature, list) or len(old_signature) != 7 or old_signature[-1] != 2
                        or signature[1:5] != old_signature[1:5] or original.get('bytes') != item.get('bytes')
                        or not volume_check(signature[0], old_signature[0])
                        or type(item.get('allocated_bytes')) is not int
                        or item['allocated_bytes'] < 0):
                    raise SnapshotError('bulk landing retirement member differs')
            if journal['state'] == 'completed':
                if (journal.get('removed') != [item['relative'] for item in plan['items']]
                        or journal.get('pending') is not None
                        or any(os.path.lexists(incoming/item['relative']) for item in plan['items'])):
                    raise SnapshotError('completed bulk landing retirement differs')
                return journal
            existing = [incoming/item['relative'] for item in plan['items'] if os.path.lexists(incoming/item['relative'])]
            if existing and process_check(existing, immutable_regular_paths=True):
                raise SnapshotError('bulk landing is used by a running process')
            if not apply:
                return {**journal, 'apply': False}
            retirement.journalled_unlink(incoming, journal_path, journal, guard=guard, parallel_objects=8)
            journal.update(state='completed', completed_at_epoch=time.time(),
                           estimated_allocated_bytes_reclaimed=sum(item['allocated_bytes'] for item in plan['items']))
            retirement.save_journal(journal_path, journal)
            return journal
    except (BlockingIOError, p.PackedPublicationBusy) as error:
        raise SnapshotError('bulk landing original owner busy') from error
