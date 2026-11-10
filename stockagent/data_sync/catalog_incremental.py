"""Incremental physical copies for the existing sealed DuckLake deliveries.

The catalog owner holds its publication lock. Only a prior committed delivery
can donate read-only files; the mutable DuckLake data directory never does.
The caller must still seal and independently verify the complete delivery.
"""
from __future__ import annotations

import errno
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import stat

from stockagent.data_sync.desync_snapshots import _fsync_directory
from stockagent.data_sync.immutable_replication import digest, read_manifest, safe
from stockagent.data_sync.work_progress import report_work

CONTRACT = 'ducklake_incremental_data_v1'
_LINK_UNAVAILABLE = {errno.EXDEV, errno.EOPNOTSUPP, errno.ENOSYS, errno.EPERM,
                     errno.EACCES, errno.EMLINK}


@dataclass(frozen=True)
class CatalogDonor:
    """Validated metadata reused only within one held publication transaction."""
    root: Path
    manifest: dict


def read_catalog_donor(root: Path) -> CatalogDonor:
    manifest = read_manifest(root)
    if manifest['context'].get('kind') != 'ducklake_catalog_and_data':
        raise ValueError('catalog donor is not a sealed catalog delivery')
    return CatalogDonor(root, manifest)


def _signature(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns, stat.S_IMODE(value.st_mode))


def _files(root):
    if root.is_symlink() or not root.is_dir():
        raise ValueError('catalog data directory is missing or redirected')
    def observe(path):
        relative = path.relative_to(root).as_posix()
        path = safe(root, relative)
        metadata = path.lstat()
        if stat.S_ISDIR(metadata.st_mode):
            return None
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError('catalog data contains a non-regular file')
        return relative, _signature(metadata)

    paths = sorted(root.rglob('*'))
    if len(paths) < 64:
        return dict(row for path in paths if (row := observe(path)) is not None)
    # The caller still holds the original catalog owner; these are independent
    # read-only path/type/stat checks, not concurrent publication or hashing.
    # Bound outstanding work and drain errors before releasing that owner. The
    # final complete source fence below uses the identical checks again.
    result = {}
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix='catalog-stat') as executor:
        for offset in range(0, len(paths), 64):
            for row in executor.map(observe, paths[offset:offset + 64]):
                if row is not None:
                    result[row[0]] = row[1]
    return result


def _copy(source, target, before, expected=None):
    """One input pass for a new file; no writable producer inode is shared."""
    value, count = hashlib.sha256(), 0
    fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as reader, target.open('xb') as writer:
        if _signature(os.fstat(reader.fileno())) != before:
            raise ValueError('catalog source changed before copy')
        while block := reader.read(8 * 1024**2):
            value.update(block)
            count += len(block)
            writer.write(block)
        writer.flush()
        os.fchmod(writer.fileno(), 0o444)
        os.fsync(writer.fileno())
        if (_signature(os.fstat(reader.fileno())) != before
                or _signature(source.lstat()) != before):
            raise ValueError('catalog source changed during copy')
    descriptor = {'sha256': value.hexdigest(), 'bytes': count}
    if count != before[2] or (expected is not None and descriptor != expected):
        raise ValueError('catalog source bytes changed during copy')
    return descriptor


def copy_catalog_data(source: Path, destination: Path, *, previous: Path | CatalogDonor | None = None):
    """Return exact source descriptors and physical-write statistics.

    A writable or cross-device donor is copied, never chmod'ed in place. A bad
    sealed donor or changing source fails publication. Missing link capability
    falls back to a durable independent copy; I/O and integrity errors propagate.
    Empty directories, like all logical file names, are retained.
    """
    source, destination = Path(source), Path(destination)
    safe(source, 'catalog-data-path-guard')
    safe(destination, 'catalog-data-path-guard')
    if (source == destination or source in destination.parents
            or destination in source.parents):
        raise ValueError('catalog copy roots overlap')
    observed = _files(source)
    donor_version = (previous if isinstance(previous, CatalogDonor)
                     else read_catalog_donor(previous) if previous is not None else None)
    prior = donor_version.manifest if donor_version is not None else None
    destination.mkdir()
    copied_files = copied_bytes = linked_files = linked_bytes = link_fallbacks = 0
    expected = {}
    report_work('catalog_copy', total=len(observed))
    for relative, before in observed.items():
        origin, target = safe(source, relative), safe(destination, relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        donor_key = 'data/' + relative
        descriptor = prior['files'].get(donor_key) if prior is not None else None
        donor = safe(donor_version.root, donor_key) if descriptor is not None else None
        linked = False
        if descriptor is not None and descriptor['bytes'] == before[2]:
            current = {'sha256': digest(origin), 'bytes': before[2]}
            if current == descriptor:
                metadata = donor.lstat()
                # Validate even a writable donor rather than silently hiding
                # corruption of a version that claimed these exact bytes.
                if not stat.S_ISREG(metadata.st_mode) or digest(donor) != descriptor['sha256']:
                    raise ValueError('sealed catalog donor bytes differ')
                if (not metadata.st_mode & 0o222
                        and metadata.st_dev == target.parent.stat().st_dev
                        and (metadata.st_dev, metadata.st_ino) != before[:2]):
                    try:
                        os.link(donor, target, follow_symlinks=False)
                    except OSError as error:
                        if error.errno not in _LINK_UNAVAILABLE:
                            raise
                        link_fallbacks += 1
                    else:
                        after = target.lstat()
                        # Creating a hardlink changes ctime and link count.
                        if (not stat.S_ISREG(after.st_mode) or after.st_mode & 0o222
                                or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                                != (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns)):
                            raise ValueError('catalog donor changed while linking')
                        linked = True
            else:
                descriptor = current
        else:
            descriptor = None
        if linked:
            linked_files += 1
            linked_bytes += descriptor['bytes']
        else:
            descriptor = _copy(origin, target, before, descriptor)
            copied_files += 1
            copied_bytes += descriptor['bytes']
        expected[donor_key] = descriptor
        report_work('catalog_copy', done=len(expected), total=len(observed))
    # The original catalog lock excludes its writer, but retain a complete
    # path/type/stat fence rather than assuming the lock proves stability.
    if _files(source) != observed:
        raise ValueError('catalog data changed during materialization')
    for directory in sorted((p for p in source.rglob('*') if p.is_dir())):
        safe(source, directory.relative_to(source).as_posix())
        (destination / directory.relative_to(source)).mkdir(parents=True, exist_ok=True)
    for directory in sorted((p for p in destination.rglob('*') if p.is_dir()), reverse=True):
        _fsync_directory(directory)
    _fsync_directory(destination)
    return expected, {'contract': CONTRACT,
        'previous_delivery_identity_sha256': prior['identity_sha256'] if prior else None,
        'logical_files': len(expected), 'logical_bytes': copied_bytes + linked_bytes,
        'copied_files': copied_files, 'copied_bytes': copied_bytes,
        'shared_files': linked_files, 'shared_bytes': linked_bytes,
        'link_fallbacks': link_fallbacks}
