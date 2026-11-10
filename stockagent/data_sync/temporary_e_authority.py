"""Expose an already sealed E stream pack as a local temporary packed root.

This creates metadata only. It does not change D, transfer data, switch a
consumer, grant export rights, or authorize the removal of any old object.
The existing stage files remain the sole E payload copies.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import stat
import time

from stockagent.data_sync import packed_snapshots as p, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.temporary_e_packed_authority.v1'
NODE = 'e-stage-penguin'
MARKER = 'temporary-authority.json'
LOCK = '.temporary-authority.lock'


def _stage(stage: Path) -> Path:
    stage = Path(stage).absolute()
    if (stage.is_symlink() or stage.resolve(strict=True) != stage or not stage.is_dir()
            or stat.S_IMODE(stage.stat().st_mode) != 0o700):
        raise SnapshotError('temporary E authority stage is redirected or not private')
    return stage


def _marker(stage: Path, dataset: str, old_sha: str) -> dict:
    path = stage/MARKER
    if path.is_symlink() or not path.is_file():
        raise SnapshotError('temporary E authority marker is missing or redirected')
    value = json.loads(path.read_text())
    prepared = stream_repack._read_prepared(stage, dataset, old_sha)
    if (not isinstance(value, dict) or value.get('contract') != CONTRACT
            or value.get('dataset') != dataset or value.get('old_manifest_sha256') != old_sha
            or value.get('prepared_sha256') != prepared['identity_sha256']
            or value.get('local_only') is not True or value.get('D_retirement_authorized') is not False):
        raise SnapshotError('temporary E authority identity differs')
    return value


def expose(cold: Path, stage: Path, dataset: str, old_sha: str, *, now_ns=None, guard=lambda: None) -> dict:
    """Publish a fixed, local-only E view without copying its prepared packs."""
    cold, stage = Path(cold).absolute(), _stage(stage)
    dataset = p.validate_slug(dataset, 'dataset')
    old_sha = p._validate_hash(old_sha, 'original manifest SHA')
    if p._paths_overlap(cold, stage):
        raise SnapshotError('temporary E authority overlaps the cold store')
    locks = cold/'.local-state/locks'
    with p._try_publication_lock(locks/'publish-retention-global.lock', shared=True), \
            p._try_publication_lock(locks/f'publish-{dataset}.lock'), \
            p._try_publication_lock(stage/LOCK):
        guard()
        original = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if original.manifest_sha256 != old_sha:
            raise SnapshotError('temporary E authority original head changed')
        prepared = stream_repack._read_prepared(stage, dataset, old_sha)
        if (prepared['files'] != original.manifest['source']['files']
                or prepared['logical_bytes'] != original.manifest['source']['logical_bytes']):
            raise SnapshotError('temporary E authority source counts differ')
        marker_path = stage/MARKER
        if marker_path.is_symlink():
            raise SnapshotError('temporary E authority marker is redirected')
        if marker_path.exists():
            marker = json.loads(marker_path.read_text())
            if (marker.get('contract') != CONTRACT or marker.get('dataset') != dataset
                    or marker.get('old_manifest_sha256') != old_sha
                    or marker.get('prepared_sha256') != prepared['identity_sha256']
                    or marker.get('local_only') is not True
                    or marker.get('D_retirement_authorized') is not False
                    or marker.get('state') not in ('preparing', 'ready')):
                raise SnapshotError('temporary E authority belongs to another generation')
            created_ns = marker.get('created_ns')
            if type(created_ns) is not int or created_ns <= 0:
                raise SnapshotError('temporary E authority creation clock is invalid')
        else:
            created_ns = time.time_ns() if now_ns is None else now_ns
            if type(created_ns) is not int or created_ns <= 0:
                raise SnapshotError('temporary E authority creation clock is invalid')
            marker = {'contract': CONTRACT, 'state': 'preparing', 'dataset': dataset,
                      'old_manifest_sha256': old_sha, 'prepared_sha256': prepared['identity_sha256'],
                      'created_ns': created_ns, 'local_only': True, 'D_retirement_authorized': False}
            atomic_write_json(marker_path, marker, durable=True)
        stamp = p.next_hlc([p.HLC.from_mapping(original.manifest['hlc'])], node_id=NODE, now_ns=created_ns)
        seconds, nanos = divmod(stamp.physical_ns, 1_000_000_000)
        snapshot_id = p.validate_slug(
            f"{dataset[:40]}-{time.strftime('%Y%m%dT%H%M%S', time.gmtime(seconds))}"
            f"{nanos:09d}Z-l{stamp.logical}-{NODE}-{prepared['identity_sha256'][:16]}", 'snapshot_id')
        archive = stream_repack._archive(prepared, original.manifest)
        by_sha = {row['sha256']: row['name'] for row in prepared['packages']}
        if len(by_sha) != len(prepared['packages']):
            raise SnapshotError('temporary E authority contains duplicate packs')
        for item in archive['objects']:
            item['relpath'] = by_sha[item['sha256']]
        archive['inventory']['relpath'] = prepared['inventory']['name']
        manifest = {'schema_version': (p.ALIAS_SNAPSHOT_SCHEMA_VERSION
                                       if prepared.get('packed_format') == 'content-alias-v3'
                                       else original.manifest['schema_version']), 'dataset': dataset,
                    'snapshot_id': snapshot_id, 'published_at': p._utc_iso_from_ns(stamp.physical_ns),
                    'hlc': stamp.to_dict(), 'publisher': {'node_id': NODE},
                    'source': original.manifest['source'], 'archive': archive,
                    'metadata': {**original.manifest.get('metadata', {}),
                                 'temporary_e_authority_contract': CONTRACT,
                                 'physical_repack_original_manifest_sha256': old_sha,
                                 'temporary_e_prepared_sha256': prepared['identity_sha256'],
                                 'local_only': True}}
        p._validate_manifest(manifest)
        manifest_rel = Path('manifests')/dataset/(snapshot_id+'.json')
        manifest_path = stage/manifest_rel
        head_path = stage/'heads'/dataset/(NODE+'.json')
        manifest_sha = hashlib.sha256(p._canonical_json_bytes(manifest)).hexdigest()
        head = {'schema_version': p.PACKED_HEAD_SCHEMA_VERSION, 'dataset': dataset, 'node_id': NODE,
                'snapshot_id': snapshot_id, 'hlc': stamp.to_dict(),
                'updated_at': p._utc_iso_from_ns(stamp.physical_ns),
                'manifest_relpath': manifest_rel.as_posix(), 'manifest_sha256': manifest_sha}
        head_sha = hashlib.sha256(p._canonical_json_bytes(head)).hexdigest()
        planned = {'snapshot_id': snapshot_id, 'manifest_sha256': manifest_sha, 'head_sha256': head_sha}
        if any(marker.get(key, value) != value for key, value in planned.items()):
            raise SnapshotError('temporary E authority planned identity differs')
        marker.update(planned)
        atomic_write_json(marker_path, marker, durable=True)
        for parent in (manifest_path.parent, head_path.parent):
            parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if parent.is_symlink() or parent.resolve(strict=True) != parent:
                raise SnapshotError('temporary E authority metadata path is redirected')
        # Bounded packed reads borrow the ordinary publication owner. Create
        # that stable inode inside this private stage before exposing a head;
        # retirement acquires the same owner exclusively before cleanup.
        local = stage/'.local-state'
        local_locks = local/'locks'
        for parent in (local, local_locks):
            parent.mkdir(mode=0o700, exist_ok=True)
            if parent.is_symlink() or parent.resolve(strict=True) != parent:
                raise SnapshotError('temporary E authority read owner is redirected')
        with p._try_publication_lock(local_locks/'publish-retention-global.lock'):
            pass
        if p.write_immutable_json(manifest_path, manifest) != manifest_sha:
            raise SnapshotError('temporary E manifest identity differs')
        if os.path.lexists(head_path):
            if head_path.is_symlink() or json.loads(head_path.read_text()) != head:
                raise SnapshotError('temporary E authority head differs')
        else:
            atomic_write_json(head_path, head, durable=True)
        # This checks only metadata and member lengths. The original source
        # SHA was checked during sealing; no E payload is read back here.
        resolved = p.resolve_latest_packed(stage, dataset)
        if resolved.manifest_sha256 != manifest_sha:
            raise SnapshotError('temporary E authority head identity differs')
        marker.update(state='ready', manifest_sha256=manifest_sha, snapshot_id=snapshot_id,
                      files=prepared['files'], logical_bytes=prepared['logical_bytes'])
        atomic_write_json(marker_path, marker, durable=True)
        return marker


@contextmanager
def borrow(stage: Path, dataset: str, old_sha: str):
    """Keep a ready temporary authority pinned for one local reader."""
    stage = _stage(stage)
    dataset = p.validate_slug(dataset, 'dataset')
    old_sha = p._validate_hash(old_sha, 'original manifest SHA')
    with p._try_publication_lock(stage/LOCK, shared=True):
        marker = _marker(stage, dataset, old_sha)
        if marker.get('state') != 'ready':
            raise SnapshotError('temporary E authority is not available for new readers')
        resolved = p.resolve_latest_packed(stage, dataset)
        if (resolved.manifest_sha256 != marker.get('manifest_sha256')
                or resolved.manifest['snapshot_id'] != marker.get('snapshot_id')):
            raise SnapshotError('temporary E authority head changed')
        yield resolved


def retire(cold: Path, stage: Path, receipt: dict, *, guard=lambda: None) -> dict:
    """Withdraw an E view only after its exact replacement is committed on D."""
    cold, stage = Path(cold).absolute(), _stage(stage)
    if not isinstance(receipt, dict):
        raise SnapshotError('temporary E authority retirement receipt is malformed')
    dataset = p.validate_slug(receipt.get('dataset'), 'dataset')
    old_sha = p._validate_hash(receipt.get('old_manifest_sha256'), 'original manifest SHA')
    new_sha = p._validate_hash(receipt.get('new_manifest_sha256'), 'new manifest SHA')
    if (p._paths_overlap(cold, stage)
            or receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or receipt.get('state') != 'new_D_head_committed_from_exact_E_source'):
        raise SnapshotError('temporary E authority retirement receipt differs')
    locks = cold/'.local-state/locks'
    with p._try_publication_lock(locks/'publish-retention-global.lock', shared=True), \
            p._try_publication_lock(locks/f'publish-{dataset}.lock'), \
            p._try_publication_lock(stage/LOCK), \
            p._try_publication_lock(stage/'.local-state/locks/publish-retention-global.lock'):
        guard()
        marker = _marker(stage, dataset, old_sha)
        if marker.get('state') not in ('preparing', 'ready', 'retiring'):
            raise SnapshotError('temporary E authority retirement state differs')
        if marker.get('state') == 'retiring' and marker.get('replacement_D_sha256') != new_sha:
            raise SnapshotError('temporary E authority retirement replacement changed')
        exposure_state = (marker.get('exposure_state_before_retirement')
                          if marker.get('state') == 'retiring' else marker['state'])
        if exposure_state not in ('preparing', 'ready'):
            raise SnapshotError('temporary E authority publication state is unknown')
        replacement = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if (replacement.manifest_sha256 != new_sha
                or replacement.manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256') != old_sha
                or replacement.manifest['snapshot_id'] != receipt.get('new_snapshot_id')):
            raise SnapshotError('temporary E authority replacement D head differs')
        prepared = stream_repack._read_prepared(stage, dataset, old_sha)
        if (replacement.manifest['source']['files'] != prepared['files']
                or replacement.manifest['source']['logical_bytes'] != prepared['logical_bytes']):
            raise SnapshotError('temporary E authority replacement source counts differ')
        install = json.loads((stage/'install.json').read_text())
        installed_rows = install.get('installed', [])
        if (not isinstance(installed_rows, list)
                or not all(isinstance(row, dict) and isinstance(row.get('sha256'), str)
                           and isinstance(row.get('D_signature'), list) for row in installed_rows)):
            raise SnapshotError('temporary E authority replacement install is malformed')
        installed = {row['sha256']: row for row in installed_rows}
        references = [replacement.manifest['archive']['inventory'], *replacement.manifest['archive']['objects']]
        if (install.get('new_manifest_sha256') != new_sha
                or install.get('prepared_sha256') != prepared['identity_sha256']
                or len(installed) != len(installed_rows)
                or set(installed) != {row['sha256'] for row in references}):
            raise SnapshotError('temporary E authority replacement install differs')
        for item in references:
            path = p._path_under(cold, item['relpath'], 'replacement object')
            if path.is_symlink() or not path.is_file() or not stream_repack._signature_matches(path, installed[item['sha256']]['D_signature']):
                raise SnapshotError('temporary E authority replacement generation changed')
        from stockagent.data_sync.materialized_cache import process_references_many
        if process_references_many([stage]):
            raise SnapshotError('temporary E authority has a running reader')
        snapshot = marker.get('snapshot_id')
        if snapshot is None and exposure_state == 'preparing':
            # The first durable marker predates every authority file.
            if (stage/'heads'/dataset).exists() or (stage/'manifests'/dataset).exists():
                raise SnapshotError('unplanned temporary E metadata exists')
            members = []
        else:
            snapshot = p.validate_slug(snapshot, 'temporary snapshot ID')
            members = [('head', stage/'heads'/dataset/(NODE+'.json'), marker.get('head_sha256')),
                       ('manifest', stage/'manifests'/dataset/(snapshot+'.json'), marker.get('manifest_sha256'))]
        marker.update(state='retiring', replacement_D_sha256=new_sha,
                      exposure_state_before_retirement=exposure_state)
        atomic_write_json(stage/MARKER, marker, durable=True)
        for kind, path, digest in members:
            p._validate_hash(digest, 'temporary metadata SHA')
            if os.path.lexists(path):
                if (path.is_symlink() or not path.is_file() or path.parent.resolve(strict=True) != path.parent
                        or p.sha256_file(path) != digest):
                    raise SnapshotError('temporary E authority metadata changed before retirement')
                marker['pending'] = kind
                atomic_write_json(stage/MARKER, marker, durable=True)
                path.unlink()
                p._fsync_directory(path.parent)
            elif (exposure_state != 'preparing' and marker.get('pending') != kind
                  and not marker.get(kind+'_removed')):
                raise SnapshotError('temporary E authority metadata disappeared without intent')
            marker[kind+'_removed'] = True
            marker.pop('pending', None)
            atomic_write_json(stage/MARKER, marker, durable=True)
        result = {'contract': CONTRACT+'.retirement', 'state': 'retired', 'dataset': dataset,
                  'old_manifest_sha256': old_sha, 'new_manifest_sha256': new_sha,
                  'temporary_manifest_sha256': marker.get('manifest_sha256'), 'local_only': True}
        atomic_write_json(stage/'temporary-authority-retirement.json', result, durable=True)
        (stage/MARKER).unlink()
        p._fsync_directory(stage)
        return result
