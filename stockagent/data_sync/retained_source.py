"""Bounded native raw reads from a fixed packed release plus current increments.

This is source metadata under the existing collector/publication owners, not a
new cache, object pool or retirement executor. Preparing a binding does not
install it or authorize unlinking. Old publisher binaries must be drained before
installation; the original recovery/consumer/transport/quarantine gates still
own source retirement.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import threading
from typing import Callable

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError

CONTRACT = 'bounded_raw_history_v1'
BINDING = 'state/retained-raw.json'
LOCK = 'state/collector.lock'
DEFAULT_COLD = Path('/srv/stockagent-packed/.local-state/private-history')
# Private-history pins must not enter the public retention owner's cache: those
# exact private releases are deliberately absent from its public manifest set.
DEFAULT_PIN_ROOT = Path('/var/lib/stockagent/lakehouse-control/dataset-versions/private-materialized')
MAX_BINDING_BYTES = 1024**2
MAX_ORIGINAL_BYTES = 64 * 1024**2
MAX_OBJECT_BYTES = 4 * 1024**3


def _raw_workspace_proof(source: Path, original: Path, files: dict) -> dict:
    return {'contract': 'retained_raw_workspace_verification_v1', 'source': str(source),
        'fixed_materialization': str(original), 'files_verified': len(files),
        'encoded_bytes_verified': sum(row['bytes'] for row in files.values()),
        'raw_files_sha256': hashlib.sha256(json.dumps(files, sort_keys=True,
            separators=(',', ':')).encode()).hexdigest()}


def _root(source: Path) -> Path:
    source = source.absolute()
    info = source.lstat()
    if source.resolve(strict=True) != source or not stat.S_ISDIR(info.st_mode):
        raise SnapshotError('retained source root is absent or redirected')
    return source


def _signature(path: Path) -> tuple:
    info = path.lstat()
    if (path.resolve(strict=True) != path.absolute() or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid() or info.st_nlink != 1 or info.st_mode & 0o022):
        raise SnapshotError('retained source metadata must be owned single-link regular data')
    return (packed._source_stat(info), info.st_mode, info.st_uid, info.st_nlink)


def _read_binding(path: Path) -> tuple[bytes, tuple]:
    before = _signature(path)
    if not 0 < before[0][2] <= MAX_BINDING_BYTES:
        raise SnapshotError('retained source binding exceeds its metadata bound')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(descriptor, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        if (packed._source_stat(opened), opened.st_mode, opened.st_uid, opened.st_nlink) != before:
            raise SnapshotError('retained source binding changed while opening')
        raw = stream.read(before[0][2] + 1)
        closed = os.fstat(stream.fileno())
        if (len(raw) != before[0][2] or _signature(path) != before
                or (packed._source_stat(closed), closed.st_mode, closed.st_uid, closed.st_nlink) != before):
            raise SnapshotError('retained source binding changed while reading')
    return raw, before


def _pin_path(root: Path, dataset: str, manifest_sha256: str) -> Path:
    packed.validate_slug(dataset, 'retained source dataset')
    packed._validate_hash(manifest_sha256, 'retained source manifest SHA')
    root = root.absolute()
    if root.resolve() != root:
        raise SnapshotError('retained source pin root is redirected')
    path = root / 'pins' / 'retained-sources' / f'{dataset}-{manifest_sha256}.pin.json'
    if any(parent.exists() and (parent.resolve() != parent or not parent.is_dir())
           for parent in (path.parent, path.parent.parent)):
        raise SnapshotError('retained source pin parent is redirected')
    return path


def require_collector_owner(source: Path, descriptor: int) -> None:
    if type(descriptor) is not int or descriptor < 3:
        raise SnapshotError('retained source requires its original collector owner')
    path = _root(source) / LOCK
    before = _signature(path)
    opened = os.fstat(descriptor)
    held = Path(f'/proc/self/fdinfo/{descriptor}').read_text()
    if ((packed._source_stat(opened), opened.st_mode, opened.st_uid, opened.st_nlink) != before
            or not any(line.startswith('lock:') and {'FLOCK', 'WRITE'}.issubset(line.split())
                       for line in held.splitlines())):
        raise SnapshotError('retained source has no held exclusive collector lock')


@contextmanager
def collector_owner(source: Path, dataset: str):
    """Use the same catalogued exclusive owner for manual and scheduled runs."""
    from scripts.publish_data_releases import _source_coordination_lock
    from stockagent.data_sync import workspace_source_handoff
    source = source.absolute()
    workspace_source_handoff.guard_path(source)
    source.mkdir(parents=True, exist_ok=True)
    source = _root(source)
    entry = {'dataset': dataset, 'source': str(source),
        'source_coordination_lock_scope': 'source_parent',
        'source_coordination_lock': f'{source.name}/{LOCK}'}
    with _source_coordination_lock(entry) as descriptor:
        workspace_source_handoff.guard_path(source)
        require_collector_owner(source, descriptor)
        yield descriptor
        workspace_source_handoff.guard_path(source)
        require_collector_owner(source, descriptor)


def validate_catalog(entry: dict) -> None:
    if (entry.get('retained_source_contract') != CONTRACT
            or entry.get('dataset') != 'fred-crypto-macro'
            or entry.get('publish') is not False
            or not entry.get('source_coordination_lock')
            or entry.get('source_coordination_lock_mode', 'exclusive') != 'exclusive'
            or entry.get('included_root_files') or entry.get('excluded_subtrees')):
        raise SnapshotError('retained FRED history requires its complete private catalog and collector owner')


@dataclass(frozen=True, slots=True)
class RetainedRawSource:
    request: packed.RetainedSourceFiles
    originals: dict[str, bytes] | None
    binding_sha256: str
    check_generation: Callable[[], None]
    check_binding_generation: Callable[[], None]
    _encoded_files: dict[str, tuple[int, str]]
    _deferred_reader: Callable[[], object] | None = None
    _current_read: Callable[[], object | None] | None = None

    def get(self, name: str) -> bytes | None:
        if self.originals is None:
            if self._deferred_reader is None:
                raise SnapshotError('retained raw payloads were not admitted for this reader')
            if name not in self._encoded_files:
                return None
            return self._deferred_reader().data.get(name)
        return self.originals.get(name)

    def encoded_identity(self, name: str) -> tuple[int, str] | None:
        """Fixed original identity, not a new payload read/recovery proof."""
        return self._encoded_files.get(name)

    def matches_encoded(self, name: str, payload: bytes) -> bool:
        return self.encoded_identity(name) == (len(payload), hashlib.sha256(payload).hexdigest())

    def publication_request(self) -> packed.RetainedSourceFiles:
        current = self._current_read() if self._current_read is not None else None
        if current is not None and current.transfer_identity is not None:
            return replace(self.request, _verified_read=current)
        return self.request

    def locator(self, name: str) -> dict:
        release = self.request.release
        return {'backend': 'packed-fixed-release', 'dataset': release.manifest['dataset'],
            'snapshot_id': release.manifest['snapshot_id'], 'manifest_sha256': release.manifest_sha256,
            'path': name, 'binding_sha256': self.binding_sha256}


def load_retained_source(source: Path, dataset: str, *, cold: Path = DEFAULT_COLD,
                         pin_root: Path = DEFAULT_PIN_ROOT, read_originals: bool = True,
                         defer_originals: bool = False) -> RetainedRawSource | None:
    """Resolve fixed identity without treating invalid metadata as a cache miss.

    Eager/deferred payload reads verify originals. Metadata-first resolution
    alone proves neither object availability nor a new full recovery; the
    canonical publisher still verifies retained originals before commitment.
    """
    if defer_originals and read_originals:
        raise SnapshotError('deferred originals require explicit metadata-first resolution')
    source = _root(source)
    marker = source / BINDING
    if not marker.exists() and not marker.is_symlink():
        return None
    raw, binding_before = _read_binding(marker)
    try:
        def unique_fields(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise SnapshotError('retained source binding contains duplicate fields')
                result[key] = value
            return result
        binding = json.loads(raw, object_pairs_hook=unique_fields)
        if (not isinstance(binding, dict) or binding.get('contract') != CONTRACT
                or binding.get('source') != str(source) or binding.get('dataset') != dataset
                or binding.get('cold_root') != str(cold.absolute())):
            raise SnapshotError('retained source binding identity differs from its collector')
        files = binding['raw_files']
        if not isinstance(files, dict) or not 1 <= len(files) <= 4096:
            raise SnapshotError('retained source binding needs bounded explicit raw files')
        for name, row in files.items():
            if (not isinstance(name, str) or not name.startswith('raw/')
                    or packed._safe_relative_path(name, 'retained raw name').as_posix() != name
                    or not isinstance(row, dict) or set(row) != {'bytes', 'sha256'}
                    or type(row['bytes']) is not int or row['bytes'] < 0):
                raise SnapshotError('retained source binding contains an invalid raw record')
            packed._validate_hash(row['sha256'], 'retained original SHA')
        if sum(row['bytes'] for row in files.values()) > MAX_ORIGINAL_BYTES:
            raise SnapshotError('retained source originals exceed their memory admission')
        pin = _pin_path(pin_root, dataset, binding['manifest_sha256'])
        pin_before = _signature(pin)
        release = packed.resolve_packed_pin(cold, pin, expected_dataset=dataset)
        if (release.manifest_sha256 != binding['manifest_sha256']
                or release.manifest['snapshot_id'] != binding['snapshot_id']
                or release.manifest['archive']['inventory']['sha256'] != binding['inventory_sha256']):
            raise SnapshotError('retained source binding differs from its fixed pin')
        request = packed.RetainedSourceFiles(release, tuple(sorted(files)), MAX_ORIGINAL_BYTES, MAX_OBJECT_BYTES)
        deferred_reader = current_read = None
        if read_originals:
            verified = packed._read_packed_files_verified(cold, release, names=request.names,
                maximum_bytes=request.maximum_bytes, maximum_object_bytes=request.maximum_object_bytes,
                capture_read_proof=True)
            if verified.transfer_identity is not None:
                request = replace(request, _verified_read=verified)
            selected, originals = verified.entries, verified.data
            cold_guard = verified.check_generation
        else:
            with packed._open_inventory(cold, release.manifest) as entries:
                packed._validate_inventory(release.manifest, entries, retain_object_members=False)
                selected = [row for row in entries if row['path'] in files]
            originals, cold_guard = None, lambda: None
            if defer_originals:
                decoded = []
                decode_lock = threading.Lock()
                metadata_paths = (release.manifest_path,
                    cold / release.manifest['archive']['inventory']['relpath'])
                metadata_before = {path: packed._source_stat(path.lstat()) for path in metadata_paths}
                def metadata_guard():
                    packed._check_packed_read_authority(cold)
                    if any(path.resolve(strict=True) != path.absolute()
                           or packed._source_stat(path.lstat()) != before
                           for path, before in metadata_before.items()):
                        raise SnapshotError('retained fixed metadata generation changed')
                def decode_once():
                    with decode_lock:
                        if not decoded:
                            metadata_guard()
                            decoded.append(packed._read_packed_files_verified(cold, release, names=request.names,
                                maximum_bytes=request.maximum_bytes, maximum_object_bytes=request.maximum_object_bytes,
                                capture_read_proof=True))
                        return decoded[0]
                def current_read():
                    with decode_lock:
                        return decoded[0] if decoded else None
                def cold_guard():
                    read = current_read()
                    if read is None:
                        metadata_guard()  # No original availability claim before actual use/publication.
                    else:
                        read.check_generation()
                deferred_reader = decode_once
        if ({row['path']: {'bytes': row.get('size'), 'sha256': row.get('sha256')}
             for row in selected if row['kind'] == 'file'} != files):
            raise SnapshotError('retained source raw binding differs from the complete fixed inventory')
        proof = binding.get('recovery', {})
        if (not isinstance(proof, dict) or proof.get('manifest_sha256') != release.manifest_sha256
                or proof.get('materialized_verified') is not True
                or proof.get('independently_reconstructed_files') != release.manifest['source']['files']
                or proof.get('independently_reconstructed_bytes') != release.manifest['source']['logical_bytes']):
            raise SnapshotError('retained source binding lacks complete original recovery')
        workspace = binding.get('workspace_raw_verification')
        if workspace is not None:
            if (not isinstance(workspace, dict)
                    or not isinstance(workspace.get('fixed_materialization'), str)
                    or not Path(workspace['fixed_materialization']).is_absolute()
                    or '..' in Path(workspace['fixed_materialization']).parts
                    or workspace != _raw_workspace_proof(source,
                        Path(workspace['fixed_materialization']), files)):
                raise SnapshotError('retained workspace proof differs from its exact bound originals')
        def check_binding_generation():
            if _signature(marker) != binding_before or _signature(pin) != pin_before:
                raise SnapshotError('retained source binding/pin changed during the collector transaction')
        def check_generation():
            check_binding_generation()
            cold_guard()
        # The decoder just fenced its cold originals before returning. Only
        # these separately mutable controls need another immediate check.
        check_binding_generation()
        return RetainedRawSource(request, originals, hashlib.sha256(raw).hexdigest(),
            check_generation, check_binding_generation,
            {name: (row['bytes'], row['sha256']) for name, row in files.items()},
            deferred_reader, current_read)
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as error:
        raise SnapshotError(f'cannot resolve retained source: {error}') from error


def prepare_retained_source_binding(source: Path, cold: Path, pin_root: Path, release, *,
                                    source_lock_fd: int, progress=None,
                                    fixed_materialized_path: Path | None = None) -> dict:
    """Prepare initial enrollment; no binding installation or source deletion.

    A standard exact pin is durable before returning the candidate. Installation
    belongs to the original source owner after old publishers have been fenced.
    Later raw-retirement batches need an explicitly verified replacement binding;
    they must never silently extend this fixed initial selection.

    A separately reconstructed fixed materialization can verify the complete old
    release while the current source keeps new controls, projections and genuine
    increments. Its bound raw names must all match their originals, with source
    generation fences spanning the full cold verification. Neither path is a
    substitute for full original reconstruction or runtime/deletion admission.
    """
    source = _root(source)
    require_collector_owner(source, source_lock_fd)
    marker = source / BINDING
    if marker.exists() or marker.is_symlink():
        raise SnapshotError('initial retained enrollment cannot replace an existing binding')
    with packed._existing_packed_publication_owner(cold, shared=True):
        fixed = packed.resolve_packed_snapshot_id(cold, release.manifest['dataset'], release.manifest['snapshot_id'])
        if fixed.manifest_sha256 != release.manifest_sha256 or fixed.manifest != release.manifest:
            raise SnapshotError('retained enrollment requires its exact fixed manifest bytes')
        with packed._open_inventory(cold, release.manifest) as entries:
            packed._validate_inventory(release.manifest, entries, retain_object_members=False)
            files = {row['path']: {'bytes': row['size'], 'sha256': row['sha256']}
                     for row in entries if row['kind'] == 'file' and row['path'].startswith('raw/')}
        if (not 1 <= len(files) <= 4096
                or sum(row['bytes'] for row in files.values()) > MAX_ORIGINAL_BYTES):
            raise SnapshotError('initial retained enrollment exceeds its bounded raw scope')
        original = source if fixed_materialized_path is None else _root(fixed_materialized_path)
        signatures = {}
        if fixed_materialized_path is not None:
            from stockagent.data_sync import closed_source as closed
            for name, row in files.items():
                path = source / name
                try:
                    signatures[name] = _signature(path)
                    if closed._hash(path, row['bytes']) != row['sha256']:
                        raise SnapshotError('current raw differs from its fixed original; preserve the increment')
                except OSError as error:
                    raise SnapshotError('initial retained workspace raw is absent or redirected') from error
        proof = packed.verify_packed_snapshot(cold, release, materialized_path=original,
            reconstruct_all=True, progress=progress)
        require_collector_owner(source, source_lock_fd)
        if any(_signature(source / name) != before for name, before in signatures.items()):
            raise SnapshotError('current raw generation changed during complete original verification')
        pin = _pin_path(pin_root, release.manifest['dataset'], release.manifest_sha256)
        if pin.is_relative_to(source) or pin.is_relative_to(cold.absolute()):
            raise SnapshotError('retained source pin must use a separate owner metadata root')
        pin.parent.mkdir(parents=True, exist_ok=True)
        if pin.exists() or pin.is_symlink():
            if packed.resolve_packed_pin(cold, pin).manifest_sha256 != release.manifest_sha256:
                raise SnapshotError('retained source pin conflicts with the prepared release')
        else:
            packed.write_packed_pin(pin, release, exclusive=True)
        return {'contract': CONTRACT, 'source': str(source), 'cold_root': str(cold.absolute()),
            'dataset': release.manifest['dataset'], 'snapshot_id': release.manifest['snapshot_id'],
            'manifest_sha256': release.manifest_sha256,
            'inventory_sha256': release.manifest['archive']['inventory']['sha256'],
            'raw_files': files, 'recovery': proof, 'prepared_at_utc': datetime.now(timezone.utc).isoformat(),
            **({'workspace_raw_verification': _raw_workspace_proof(source, original, files)}
               if fixed_materialized_path is not None else {}),
            'retirement_authorized_by_binding': False}
