"""Retire a legacy whole carrier when its exact ordered parts are on D.

Only the old bulk owner's recorded one- or two-name carrier is touched. This uses
the source-hashed migration receipt and small immutable part-plan metadata; it
does not create a backup or perform an independent payload readback.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import zipfile

from contextlib import ExitStack
from stockagent.data_sync import bulk_archive, packed_snapshots as p, repack_retirement
from stockagent.data_sync import stream_repack, temporary_e_authority
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.windows_cold_io import metadata_path

PLAN = 'bulk-original-carrier-parts.json'
JOURNAL = 'bulk-original-carrier-retirement.json'
CONTRACT = 'stockagent.bulk_original_carrier_retirement.v1'


def _read(path: Path, maximum=2*1024*1024) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
        raise SnapshotError('bulk original carrier metadata missing or redirected')
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise SnapshotError('bulk original carrier metadata is invalid')
    return value


def _match_parts(plan: dict, rows: list[dict], metadata: dict, raw: bytes) -> None:
    bulk_archive.validate_parts_plan(plan)
    by_name = {row.get('path'): row for row in rows}
    if (len(by_name) != len(rows) or set(by_name) !=
            {'compressed_parts.json', 'member_inventory.json', *(item['path'] for item in plan['parts'])}
            or hashlib.sha256(raw).hexdigest() != metadata.get('compressed_parts_sha256')
            or plan['compressed_sha256'] != metadata.get('compressed_sha256')
            or str(plan['compressed_bytes']) != metadata.get('compressed_bytes')
            or by_name['compressed_parts.json'].get('sha256') != metadata.get('compressed_parts_sha256')
            or any(by_name[item['path']].get('sha256') != item['sha256']
                   or by_name[item['path']].get('size') != item['bytes'] for item in plan['parts'])):
        raise SnapshotError('bulk original carrier ordered part identity differs')


def capture(stage: Path, dataset: str, old_sha: str) -> dict:
    """Persist only the small part-plan metadata before E pack cleanup."""
    stage = Path(stage).absolute()
    with temporary_e_authority.borrow(stage, dataset, old_sha) as view:
        prepared = stream_repack._read_prepared(stage, dataset, old_sha)
        rows = list(p._iter_inventory(stage, view.manifest))
        by_name = {row.get('path'): row for row in rows}
        entry = by_name.get('compressed_parts.json', {})
        storage = entry.get('storage', {})
        if storage.get('kind') != 'pack':
            raise SnapshotError('bulk ordered plan is not in the expected E pack')
        matches = [item for item in prepared['packages'] if item['sha256'] == storage.get('object_sha256')]
        if len(matches) != 1:
            raise SnapshotError('bulk ordered plan E pack identity differs')
        with zipfile.ZipFile(stage/matches[0]['name']) as archive:
            info = archive.getinfo(storage['member'])
            if info.file_size > 1024*1024:
                raise SnapshotError('bulk ordered plan is oversized')
            raw = archive.read(info)
        plan = json.loads(raw)
        _match_parts(plan, rows, view.manifest['metadata'], raw)
        path = stage/PLAN
        if os.path.lexists(path):
            if path.is_symlink() or not path.is_file() or path.read_bytes() != raw:
                raise SnapshotError('existing bulk ordered plan differs')
        else:
            atomic_write_bytes(path, raw)
        return {'state': 'metadata_captured', 'parts': len(plan['parts']),
                'bytes': len(raw), 'whole_bytes': plan['compressed_bytes'],
                'payload_readback': False, 'deletion_authorized': False}


def _committed_plan(cold: Path, manifest: dict, rows: list[dict]) -> bytes:
    """Read just the bounded part plan from one committed source-local pack."""
    matches = [row for row in rows if row.get('path') == 'compressed_parts.json']
    if len(matches) != 1:
        raise SnapshotError('committed bulk ordered plan member differs')
    entry = matches[0]
    storage, size = entry.get('storage', {}), entry.get('size')
    if (not isinstance(storage, dict) or storage.get('kind') != 'pack'
            or type(size) is not int or not 0 < size <= 1024*1024):
        raise SnapshotError('committed bulk ordered plan storage differs')
    objects = [row for row in manifest['archive']['objects'] if row['sha256'] == storage.get('object_sha256')]
    if len(objects) != 1:
        raise SnapshotError('committed bulk ordered plan pack differs')
    obj = objects[0]
    target = p._path_under(cold, obj['relpath'], 'bulk ordered plan pack')
    if Path(cold) == Path('/srv/stockagent-packed'):
        from stockagent.data_sync.windows_dedup_zip_io import binary_zip_members
        groups = [{'path': target, 'expected_bytes': obj['bytes'],
                   'members': [{'name': storage['member'], 'size': size}]}]
        chunks, remaining = [], size
        with binary_zip_members(groups) as reader:
            while remaining:
                chunk = reader.read(remaining)
                if not chunk or len(chunk) > remaining:
                    raise SnapshotError('committed bulk ordered plan stream truncated')
                chunks.append(chunk)
                remaining -= len(chunk)
        raw = b''.join(chunks)
    else:
        with zipfile.ZipFile(target) as archive:
            info = archive.getinfo(storage['member'])
            if info.file_size != size:
                raise SnapshotError('committed bulk ordered plan size differs')
            raw = archive.read(info)
    if hashlib.sha256(raw).hexdigest() != entry.get('sha256'):
        raise SnapshotError('committed bulk ordered plan SHA differs')
    return raw


def _coordinator_held(fd: int, path: Path) -> bool:
    if type(fd) is not int or fd < 0:
        return False
    try:
        info = os.fstat(fd)
        lockinfo = Path(f'/proc/self/fdinfo/{fd}').read_text()
        return (stat.S_ISREG(info.st_mode) and not path.is_symlink()
                and os.path.samestat(info, path.lstat())
                and any(line.startswith('lock:') and 'FLOCK' in line and 'WRITE' in line
                        for line in lockinfo.splitlines()))
    except OSError:
        return False


def _same_payload(path: Path, recorded: list) -> bool:
    actual = stream_repack._signature(path)
    return (isinstance(recorded, list) and len(recorded) == len(actual) == 7
            and actual[1:5] == recorded[1:5]
            and (actual[0] == recorded[0] or stream_repack._trusted_device_renumbering(path, actual[0])))


def completed_reclamation(workspace: Path, directory: Path, scope: str) -> int | None:
    """Read the exact completed journal without depending on the latest retry."""
    if scope not in {'markets', 'ablations', 'cache'}:
        raise SnapshotError('bulk original carrier summary scope invalid')
    publication_path, received_path = (directory/(scope+'.cold-publication.json'),
                                       directory/(scope+'.receipt.json'))
    if not publication_path.exists() or not received_path.exists():
        return None
    publication, received = _read(publication_path, 128*1024), _read(received_path, 128*1024)
    if publication.get('preservation_contract') != bulk_archive.PARTS_CONTRACT:
        return None
    dataset = p.validate_slug(publication.get('dataset'), 'bulk dataset')
    old_sha = p._validate_hash(publication.get('manifest_sha256'), 'bulk source SHA')
    stage = Path(workspace)/'migration/stream-source-repack-v1'/dataset/old_sha
    path = stage/JOURNAL
    if not os.path.lexists(path):
        return None
    if stage.is_symlink() or not stage.is_dir() or stage.resolve(strict=True) != stage:
        raise SnapshotError('bulk original carrier summary stage redirected')
    value = _read(path)
    body = value.get('plan')
    amount = value.get('estimated_allocated_bytes_reclaimed')
    if (value.get('contract') != CONTRACT or value.get('state') != 'completed'
            or not isinstance(body, dict) or value.get('plan_sha256') != stream_repack._identity(body)
            or body.get('dataset') != dataset or body.get('old_manifest_sha256') != old_sha
            or body.get('batch') != directory.name or body.get('scope') != scope
            or publication.get('source_scope') != scope or received.get('scope') != scope
            or body.get('compressed_sha256') != publication.get('compressed_sha256')
            or received.get('compressed_sha256') != publication.get('compressed_sha256')
            or body.get('publication_sha256') != hashlib.sha256(publication_path.read_bytes()).hexdigest()
            or body.get('received_sha256') != hashlib.sha256(received_path.read_bytes()).hexdigest()
            or not isinstance(body.get('names'), list) or len(body['names']) not in (1, 2)
            or value.get('cursor') != len(body['names']) or value.get('pending') is not None
            or value.get('independent_D_readback') is not False or value.get('NAS_accepted') is not False
            or type(amount) is not int or not 0 <= amount <= 2**53-1 or amount != body.get('allocated_bytes')):
        raise SnapshotError('bulk original carrier completed journal differs')
    return amount


def retire(directory: Path, receipt: dict, *, workspace: Path, cold: Path, incoming: Path,
           coordinator_fd: int, coordinator_lock: Path, ingress_lock: Path, install_lock: Path,
           extra_locks=(), guard=lambda: None, process_check=process_references_many,
           apply=False) -> dict:
    """Retire the exact old carrier names after a new fixed D commit."""
    directory, workspace, cold, incoming = (Path(x).absolute() for x in (directory, workspace, cold, incoming))
    scope = receipt.get('scope')
    if (type(apply) is not bool or scope not in {'markets', 'ablations', 'cache'}
            or directory.parent != incoming or not re.fullmatch(r'[0-9]{8}T[0-9]{6}-[0-9a-f]{12}', directory.name)
            or directory.is_symlink() or directory.resolve(strict=True) != directory
            or not _coordinator_held(coordinator_fd, coordinator_lock)):
        raise SnapshotError('bulk original carrier owner or scope differs')
    publication_path = directory/(scope+'.cold-publication.json')
    if not publication_path.exists():
        return {'contract': CONTRACT, 'state': 'not_applicable'}
    publication = _read(publication_path, 128*1024)
    received_path = directory/(scope+'.receipt.json')
    if _read(received_path, 128*1024) != receipt:
        raise SnapshotError('bulk original carrier received receipt changed')
    publication_sha = hashlib.sha256(publication_path.read_bytes()).hexdigest()
    received_sha = hashlib.sha256(received_path.read_bytes()).hexdigest()
    if publication.get('preservation_contract') != bulk_archive.PARTS_CONTRACT:
        return {'contract': CONTRACT, 'state': 'not_applicable'}
    dataset = p.validate_slug(publication.get('dataset'), 'bulk dataset')
    old_sha = p._validate_hash(publication.get('manifest_sha256'), 'bulk source SHA')
    if (not dataset.startswith('legacy-vast-bulk-'+scope+'-parts-v2-')
            or publication.get('source_scope') != scope
            or publication.get('compressed_sha256') != receipt.get('compressed_sha256')):
        raise SnapshotError('bulk original carrier publication identity differs')
    stage = workspace/'migration/stream-source-repack-v1'/dataset/old_sha
    if not stage.exists():
        return {'contract': CONTRACT, 'state': 'waiting_E_stage_metadata'}
    if stage.is_symlink() or not stage.is_dir() or stage.resolve(strict=True) != stage:
        raise SnapshotError('bulk original carrier E stage redirected')
    install_path = stage/'install.json'
    if not install_path.is_file():
        return {'contract': CONTRACT, 'state': 'waiting_D_commit'}
    install = _read(install_path, 8*1024*1024)
    new_sha, new_id = install.get('new_manifest_sha256'), install.get('snapshot_id')
    if not isinstance(new_sha, str) or not isinstance(new_id, str):
        return {'contract': CONTRACT, 'state': 'waiting_D_commit'}
    new_sha, new_id = p._validate_hash(new_sha, 'bulk replacement SHA'), p.validate_slug(new_id, 'bulk replacement snapshot')
    receipt_path = workspace/'migration/source-local-exact-repack'/dataset/(new_sha+'.json')
    if not receipt_path.is_file():
        return {'contract': CONTRACT, 'state': 'waiting_D_commit'}
    new_receipt = _read(receipt_path, 128*1024)
    if (new_receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or new_receipt.get('state') != 'new_D_head_committed_from_exact_E_source'
            or new_receipt.get('source_mode') != 'D_streamed_to_E_dataset_pack'
            or new_receipt.get('private') is True or new_receipt.get('dataset') != dataset
            or new_receipt.get('old_manifest_sha256') != old_sha
            or new_receipt.get('new_manifest_sha256') != new_sha or new_receipt.get('new_snapshot_id') != new_id):
        raise SnapshotError('bulk original carrier new D receipt differs')
    if not isinstance(receipt.get('payload'), str):
        raise SnapshotError('bulk original carrier received path differs')
    first = metadata_path(Path(receipt['payload']))
    if first.parent != directory or first.name not in {scope+'.tar.zst', scope+'.tar.zst.partial'}:
        raise SnapshotError('bulk original carrier received path differs')
    second = directory/('publish-'+scope)/'payload.tar.zst'
    if (os.path.lexists(second.parent) and (second.parent.is_symlink() or not second.parent.is_dir()
                                            or second.parent.resolve(strict=True) != second.parent)):
        raise SnapshotError('bulk original carrier alias parent redirected')
    journal_path = stage/JOURNAL
    try:
        with ExitStack() as stack:
            stack.enter_context(p._try_publication_lock(ingress_lock))
            # Briefly wait for the current owner to finish without retaining
            # a partial lock set or interrupting its active transaction.
            stack.enter_context(repack_retirement._boundary(cold, extra_locks, attempts=5))
            stack.enter_context(p._try_publication_lock(cold/'.local-state/locks'/f'publish-{dataset}.lock'))
            stack.enter_context(p._try_publication_lock(install_lock))
            guard()
            if not _coordinator_held(coordinator_fd, coordinator_lock):
                raise SnapshotError('bulk original carrier coordinator changed')
            if (hashlib.sha256(publication_path.read_bytes()).hexdigest() != publication_sha
                    or hashlib.sha256(received_path.read_bytes()).hexdigest() != received_sha):
                raise SnapshotError('bulk original carrier source controls changed')
            prepared = stream_repack._prepared_metadata(stage, dataset, old_sha)
            replacement = repack_retirement.check_replacement(cold, stage, dataset, old_sha, new_sha, new_id, prepared)
            rows = list(p._iter_inventory(cold, replacement.manifest))
            path = stage/PLAN
            if os.path.lexists(path):
                if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024*1024:
                    raise SnapshotError('bulk original carrier part plan redirected or oversized')
                raw = path.read_bytes()
            else:
                raw = _committed_plan(cold, replacement.manifest, rows)
            plan = json.loads(raw)
            if not isinstance(plan, dict):
                raise SnapshotError('bulk original carrier part plan invalid')
            _match_parts(plan, rows, replacement.manifest['metadata'], raw)
            if (publication.get('compressed_parts_sha256') != hashlib.sha256(raw).hexdigest()
                    or receipt.get('received_bytes') != plan['compressed_bytes']):
                raise SnapshotError('bulk original carrier source bytes differ')
            if apply and not os.path.lexists(path):
                atomic_write_bytes(path, raw)
            if journal_path.exists() or journal_path.is_symlink():
                journal = _read(journal_path)
                body = journal.get('plan')
                if (journal.get('contract') != CONTRACT or not isinstance(body, dict)
                        or journal.get('plan_sha256') != stream_repack._identity(body)
                        or journal.get('state') not in ('planned', 'removing', 'completed')
                        or not isinstance(body.get('names'), list) or len(body['names']) not in (1, 2)
                        or type(journal.get('cursor')) is not int or not 0 <= journal['cursor'] <= len(body['names'])
                        or journal.get('pending') not in (None, journal['cursor'])
                        or journal.get('pending') is not None and journal['cursor'] == len(body['names'])
                        or journal['state'] == 'completed' and journal['cursor'] != len(body['names'])):
                    raise SnapshotError('bulk original carrier retirement journal differs')
            else:
                if not os.path.lexists(first):
                    return {'contract': CONTRACT, 'state': 'original_carrier_absent'}
                a = first.lstat()
                names = [first]
                if os.path.lexists(second):
                    b = second.lstat()
                    if not stat.S_ISREG(b.st_mode) or not os.path.samestat(a, b):
                        raise SnapshotError('bulk original carrier alias differs')
                    names.append(second)
                if (not stat.S_ISREG(a.st_mode) or a.st_nlink != len(names) or a.st_size != plan['compressed_bytes']
                        or a.st_ctime_ns > publication_path.stat().st_mtime_ns):
                    raise SnapshotError('bulk original carrier changed since source partition publication')
                body = {'dataset': dataset, 'old_manifest_sha256': old_sha, 'new_manifest_sha256': new_sha,
                        'new_snapshot_id': new_id, 'batch': directory.name, 'scope': scope,
                        'compressed_sha256': plan['compressed_sha256'], 'compressed_bytes': plan['compressed_bytes'],
                        'parts_sha256': hashlib.sha256(raw).hexdigest(),
                        'publication_sha256': publication_sha, 'received_sha256': received_sha,
                        'names': [target.relative_to(incoming).as_posix() for target in names],
                        'signature': stream_repack._signature(first), 'allocated_bytes': a.st_blocks*512,
                        'source_scope_completeness': publication.get('scope_completeness')}
                journal = {'contract': CONTRACT, 'plan': body, 'plan_sha256': stream_repack._identity(body),
                           'state': 'planned', 'cursor': 0, 'independent_D_readback': False, 'NAS_accepted': False}
                if not apply:
                    return {**journal, 'apply': False}
                atomic_write_json(journal_path, journal, durable=True)
            expected = {'dataset': dataset, 'old_manifest_sha256': old_sha, 'new_manifest_sha256': new_sha,
                        'new_snapshot_id': new_id, 'batch': directory.name, 'scope': scope,
                        'compressed_sha256': plan['compressed_sha256'], 'compressed_bytes': plan['compressed_bytes'],
                        'parts_sha256': hashlib.sha256(raw).hexdigest(),
                        'publication_sha256': publication_sha, 'received_sha256': received_sha}
            allowed_names = ([first.relative_to(incoming).as_posix()],
                             [first.relative_to(incoming).as_posix(), second.relative_to(incoming).as_posix()])
            if (any(body.get(key) != value for key, value in expected.items())
                    or body.get('names') not in allowed_names
                    or type(body.get('allocated_bytes')) is not int or body['allocated_bytes'] < 0
                    or not isinstance(body.get('signature'), list) or len(body['signature']) != 7
                    or any(type(item) is not int or item < 0 for item in body['signature'])
                    or body['signature'][-1] != len(body['names'])):
                raise SnapshotError('bulk original carrier retirement identity changed')
            names = tuple(incoming/relative for relative in body['names'])
            if journal['state'] == 'completed':
                if first.exists() or second.exists():
                    raise SnapshotError('retired bulk original carrier name returned')
                return journal
            remaining = [path for path in names if os.path.lexists(path)]
            if remaining and process_check(remaining, immutable_regular_paths=True):
                raise SnapshotError('bulk original carrier is used by a running process')
            if not apply:
                return {**journal, 'apply': False}
            journal['state'] = 'removing'
            atomic_write_json(journal_path, journal, durable=True)
            while journal['cursor'] < len(names):
                guard()
                index, target = journal['cursor'], names[journal['cursor']]
                if os.path.lexists(target):
                    if (target.is_symlink() or not target.is_file() or target.parent.resolve(strict=True) != target.parent
                            or not _same_payload(target, body['signature'])
                            or target.stat().st_nlink != len(names)-index):
                        raise SnapshotError('bulk original carrier generation or link count changed')
                    journal['pending'] = index
                    atomic_write_json(journal_path, journal, durable=True)
                    target.unlink()
                    p._fsync_directory(target.parent)
                elif journal.get('pending') != index:
                    raise SnapshotError('bulk original carrier name disappeared without durable intent')
                journal['cursor'] += 1
                journal.pop('pending', None)
                atomic_write_json(journal_path, journal, durable=True)
            journal.update(state='completed', completed_at_epoch=time.time(),
                           estimated_allocated_bytes_reclaimed=body['allocated_bytes'])
            atomic_write_json(journal_path, journal, durable=True)
            return journal
    except (BlockingIOError, p.PackedPublicationBusy) as error:
        raise SnapshotError('bulk original carrier owner busy') from error
