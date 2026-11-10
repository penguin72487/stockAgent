"""Fixed-inventory content census for a later source-local repack.

The produced SQLite file is a disposable planning/index artifact. It neither
publishes an archive nor proves that any source payload is recoverable, and it
never authorizes source retirement. A digest is a content identity from the
fixed inventory, not a claim that every occurrence has been reread.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import time

from stockagent.data_sync import packed_snapshots as p
from stockagent.data_sync.desync_snapshots import SnapshotError

CONTRACT = 'stockagent.packed_dedup_plan.v1'
MAX_UNIQUE_CONTENTS = 500_000


def build_plan(cold: Path, dataset: str, expected_sha: str, output: Path, *,
               progress=None, guard=lambda: None, max_unique=MAX_UNIQUE_CONTENTS) -> dict:
    """Build a bounded SHA/size -> first-path map from one validated inventory.

    All logical paths, metadata and bytes remain in the original manifest.
    Exactly one representative is recorded per distinct SHA; identical SHA
    with inconsistent lengths is rejected. The source inventory is read and
    validated once; no source payload or decompressed tree is read or written.
    """
    cold, output = Path(cold).absolute(), Path(output).absolute()
    dataset = p.validate_slug(dataset, 'dataset')
    expected_sha = p._validate_hash(expected_sha, 'original manifest SHA')
    if type(max_unique) is not int or not 1 <= max_unique <= MAX_UNIQUE_CONTENTS:
        raise SnapshotError('content census unique-item bound is invalid')
    if (output.is_symlink() or output.resolve(strict=True) != output or not output.is_dir()
            or not stat.S_ISDIR(output.lstat().st_mode) or p._paths_overlap(cold, output)):
        raise SnapshotError('content census output must be a separate real directory')
    target = output/(dataset+'-'+expected_sha+'.sqlite')
    if os.path.lexists(target):
        raise SnapshotError('content census index already exists; retain its generation')
    guard()
    resolved = p.resolve_latest_packed(cold, dataset, require_objects=False)
    if resolved.manifest_sha256 != expected_sha:
        raise SnapshotError('content census original head changed')
    manifest = resolved.manifest
    p._validate_manifest(manifest)
    inventory_sha = p._validate_hash(manifest['archive']['inventory']['sha256'], 'inventory SHA')
    source_files = int(manifest['source']['files'])
    contents: dict[str, list] = {}
    by_storage: dict[str, list[int]] = {}
    files = logical = 0
    last_report = 0.0

    def rows():
        nonlocal files, logical, last_report
        for row in p._iter_inventory(cold, manifest):
            if row.get('kind') == 'file':
                digest = p._validate_hash(row.get('sha256'), 'file SHA')
                size = row.get('size')
                if type(size) is not int or size < 0 or size >= 2**63:
                    raise SnapshotError('content census file size is invalid')
                storage = row.get('storage')
                if not isinstance(storage, dict) or storage.get('kind') not in {'pack', 'blob', 'chunks'}:
                    raise SnapshotError('content census file storage is invalid')
                current = contents.get(digest)
                if current is None:
                    if len(contents) >= max_unique:
                        raise SnapshotError('content census unique-item memory bound reached')
                    # _iter_inventory is path-sorted: first is deterministic.
                    contents[digest] = [size, row['path'], 1]
                elif current[0] != size:
                    raise SnapshotError('one content SHA has inconsistent file lengths')
                else:
                    current[2] += 1
                counts = by_storage.setdefault(storage['kind'], [0, 0])
                counts[0] += 1
                counts[1] += size
                files += 1
                logical += size
                if files % 8192 == 0:
                    guard()
                    if progress is not None and time.monotonic()-last_report >= 5:
                        last_report = time.monotonic()
                        progress({'stage': 'indexing_fixed_inventory', 'files_completed': files,
                                  'files_total': source_files, 'logical_bytes': logical,
                                  'unique_contents': len(contents)})
            yield row

    # The existing canonical validator checks counts, extents, path modes,
    # symlinks and object references while the census observes the same rows.
    p._validate_inventory(manifest, rows(), retain_object_members=False)
    guard()
    again = p.resolve_latest_packed(cold, dataset, require_objects=False)
    if again.manifest_sha256 != expected_sha:
        raise SnapshotError('content census original head changed during scan')
    unique_bytes = sum(item[0] for item in contents.values())
    summary = {'contract': CONTRACT, 'dataset': dataset, 'old_manifest_sha256': expected_sha,
               'inventory_sha256': inventory_sha, 'files': files, 'logical_bytes': logical,
               'unique_contents': len(contents), 'unique_content_logical_bytes': unique_bytes,
               'duplicate_logical_bytes': logical-unique_bytes,
               'duplicate_path_count': files-len(contents), 'by_storage': by_storage,
               'existing_encoded_object_bytes': sum(int(item['bytes']) for item in manifest['archive']['objects']),
               'inventory_metadata_validated': True, 'source_payload_reread': False,
               'archive_written': False, 'retirement_authorized': False,
               'observed_at_epoch': time.time()}
    fd, name = tempfile.mkstemp(prefix='.dedup-plan-', suffix='.partial.sqlite', dir=output)
    os.fchmod(fd, 0o600)
    os.close(fd)
    temp = Path(name)
    try:
        with sqlite3.connect(temp) as db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID')
            db.execute('CREATE TABLE contents (sha256 TEXT PRIMARY KEY, size INTEGER NOT NULL, '
                       'representative_path BLOB NOT NULL, occurrences INTEGER NOT NULL) WITHOUT ROWID')
            db.execute('INSERT INTO meta VALUES (?,?)', ('summary', json.dumps(summary, sort_keys=True,
                                                                               separators=(',', ':'))))
            guard()
            db.executemany('INSERT INTO contents VALUES (?,?,?,?)',
                ((digest, item[0], item[1].encode('utf-8', errors='surrogatepass'), item[2])
                 for digest, item in sorted(contents.items())))
        with temp.open('rb') as source:
            os.fsync(source.fileno())
        # link, rather than replace, never overwrites a concurrent generation.
        os.link(temp, target, follow_symlinks=False)
        temp.unlink()
        p._fsync_directory(output)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    return {**summary, 'index_path': str(target), 'index_bytes': target.stat().st_size}
