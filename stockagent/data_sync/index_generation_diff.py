"""Plan an exact path-level comparison of public and retained source heads.

Only immutable inventory metadata is read.  Source payloads and public heads
are not changed; conflicting paths remain unresolved until a domain-specific
merge policy has been applied.
"""
from __future__ import annotations

from contextlib import ExitStack
import gzip
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

from stockagent.data_sync import packed_snapshots as p, stream_repack as s
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.index_generation_diff.v1'


def _category(path: str) -> str:
    parts = path.split('/')
    if path.endswith('.lock') or path.endswith('.sqlite') or path.endswith('.sqlite3') or 'state' in parts:
        return 'operational_state'
    if any('cache' in part.lower() for part in parts):
        return 'cache_candidate'
    if path.endswith('.parquet'):
        return 'parquet'
    if path.endswith(('.json', '.jsonl')):
        return 'json'
    return 'other'


def _same(a: dict, b: dict) -> bool:
    if a.get('kind') != b.get('kind') or a.get('mode') != b.get('mode'):
        return False
    if a.get('kind') == 'file':
        return a.get('size') == b.get('size') and a.get('sha256') == b.get('sha256')
    if a.get('kind') == 'symlink':
        return a.get('target') == b.get('target')
    return True


def compare(public: Path, private: Path, dataset: str, private_id: str, private_sha: str,
            excluded_subtrees: tuple[str, ...], output: Path, *, guard=lambda: None, progress=None,
            public_snapshot_id: str | None = None, public_manifest_sha256: str | None = None):
    public, private, output = (Path(path).absolute() for path in (public, private, output))
    dataset = p.validate_slug(dataset, 'dataset')
    private_id = p.validate_slug(private_id, 'private source snapshot')
    private_sha = p._validate_hash(private_sha, 'private source manifest SHA')
    if (public_snapshot_id is None) != (public_manifest_sha256 is None):
        raise SnapshotError('fixed public comparison requires both snapshot ID and manifest SHA')
    if public_snapshot_id is not None:
        public_snapshot_id = p.validate_slug(public_snapshot_id, 'fixed public snapshot')
        public_manifest_sha256 = p._validate_hash(public_manifest_sha256, 'fixed public manifest SHA')
    if p._paths_overlap(public, output) or p._paths_overlap(private, output) or output.is_symlink():
        raise SnapshotError('source-generation comparison output overlaps D or is redirected')
    excludes = tuple(sorted(set(excluded_subtrees)))
    if any(p._safe_relative_path(name, 'excluded source subtree').as_posix() != name for name in excludes):
        raise SnapshotError('source-generation exclusion differs')
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    if output.resolve(strict=True) != output or (output.stat().st_mode & 0o077):
        raise SnapshotError('source-generation comparison output is not private')
    with ExitStack() as locks:
        # Both stores share the same physical D, but have distinct owners.
        # No blocking acquisition: another publisher keeps its priority.
        locks.enter_context(p._try_publication_lock(private/'.local-state/locks/publish-retention-global.lock', shared=True))
        locks.enter_context(p._try_publication_lock(public/'.local-state/locks/publish-retention-global.lock', shared=True))
        guard()
        left = (p.resolve_packed_snapshot_id(public, dataset, public_snapshot_id, require_objects=False)
                if public_snapshot_id is not None else p.resolve_latest_packed(public, dataset, require_objects=False))
        if public_snapshot_id is not None and left.manifest_sha256 != public_manifest_sha256:
            raise SnapshotError('fixed public source generation changed')
        right = p.resolve_packed_snapshot_id(private, dataset, private_id, require_objects=False)
        if right.manifest_sha256 != private_sha:
            raise SnapshotError('fixed private source generation changed')
        public_rows = iter(p._iter_inventory(public, left.manifest))
        private_rows = iter(p._iter_inventory(private, right.manifest))
        a, b = next(public_rows, None), next(private_rows, None)
        counters = {key: 0 for key in ('equal', 'public_only', 'private_only', 'private_excluded',
                                       'unexpected_public_excluded', 'conflict',
                                       'conflict_bytes_public', 'conflict_bytes_private')}
        categories = {}
        count, started, last_report = 0, time.time(), 0.0
        temporary = output/('diff-'+uuid.uuid4().hex+'.partial')
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        try:
            with os.fdopen(fd, 'wb', buffering=1024*1024) as raw:
                hashed = s._SequentialHashWriter(raw)
                with gzip.GzipFile(fileobj=hashed, mode='wb', compresslevel=1, mtime=0) as target:
                    while a is not None or b is not None:
                        if b is None or a is not None and a['path'] < b['path']:
                            name, old, new = a['path'], a, None
                            a = next(public_rows, None)
                        elif a is None or b['path'] < a['path']:
                            name, old, new = b['path'], None, b
                            b = next(private_rows, None)
                        else:
                            name, old, new = a['path'], a, b
                            a, b = next(public_rows, None), next(private_rows, None)
                        restricted = new is not None and any(name == root or name.startswith(root+'/') for root in excludes)
                        if restricted:
                            decision = 'private_excluded'
                            if old is not None:
                                counters['unexpected_public_excluded'] += 1
                        elif old is None:
                            decision = 'private_only'
                        elif new is None:
                            decision = 'public_only'
                        elif _same(old, new):
                            decision = 'equal'
                        else:
                            decision = 'conflict'
                        counters[decision] += 1
                        if decision == 'conflict':
                            counters['conflict_bytes_public'] += old.get('size', 0) if old.get('kind') == 'file' else 0
                            counters['conflict_bytes_private'] += new.get('size', 0) if new.get('kind') == 'file' else 0
                            category = _category(name)
                            categories[category] = categories.get(category, 0)+1
                        if decision != 'equal':
                            row = {'path': name, 'decision': decision, 'category': _category(name),
                                   'public': ({key: old.get(key) for key in ('kind', 'mode', 'sha256', 'size', 'target')}
                                              if old is not None else None),
                                   'private': ({key: new.get(key) for key in ('kind', 'mode', 'sha256', 'size', 'target')}
                                               if new is not None else None)}
                            target.write((json.dumps(row, ensure_ascii=True, sort_keys=True, separators=(',', ':'))+'\n').encode())
                        count += 1
                        if progress is not None and time.monotonic()-last_report >= 3:
                            last_report = time.monotonic()
                            progress({'stage': 'comparing_inventories', 'items_compared': count,
                                      'conflicts_found': counters['conflict']})
                hashed.flush()
                os.fsync(raw.fileno())
                digest, length = hashed.digest.hexdigest(), hashed.count
            final = output/('diff-'+digest+'.jsonl.gz')
            if final.exists() or final.is_symlink():
                if final.is_symlink() or not final.is_file() or p.sha256_file(final) != digest:
                    raise SnapshotError('source-generation worklist identity collision')
                temporary.unlink()
            else:
                os.rename(temporary, final)
                p._fsync_directory(output)
            guard()
            current = (p.resolve_packed_snapshot_id(public, dataset, public_snapshot_id, require_objects=False)
                       if public_snapshot_id is not None else p.resolve_latest_packed(public, dataset, require_objects=False))
            if current.manifest_sha256 != left.manifest_sha256:
                raise SnapshotError('public D head changed during source-generation comparison')
            result = {'contract': CONTRACT, 'state': 'metadata_compared', 'dataset': dataset,
                'public_manifest_sha256': left.manifest_sha256, 'public_snapshot_id': left.manifest['snapshot_id'],
                'private_manifest_sha256': private_sha, 'private_snapshot_id': private_id,
                'excluded_subtrees': list(excludes), 'counters': counters, 'conflict_categories': categories,
                'worklist': {'name': final.name, 'sha256': digest, 'bytes': length},
                'source_payload_read': False, 'public_head_changed': False,
                'semantic_merge_complete': False, 'observed_at_epoch': time.time(),
                'duration_seconds': round(time.time()-started, 3),
                **({'comparison_scope': 'fixed_public_snapshot'} if public_snapshot_id is not None else {})}
            result['identity_sha256'] = s._identity(result)
            # Retain every exact comparison before updating the moving status;
            # later head changes must not erase the evidence used by a merge.
            p.write_immutable_json(output/('diff-plan-'+result['identity_sha256']+'.json'), result)
            if public_snapshot_id is None:
                atomic_write_json(output/'diff.json', result, durable=True)
            return result
        finally:
            if temporary.exists() and not temporary.is_symlink():
                temporary.unlink()
