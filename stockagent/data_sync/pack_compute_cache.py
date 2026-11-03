"""Disposable Vast pack computations; D and the original verifier own truth.

The fixed stdlib worker is sent over the existing private SSH connection. Only
closed SHA-addressed ZIP objects live here. No source tree is extracted, no
moving head is followed, and an unbound computation never accepts an inventory.
"""
from __future__ import annotations

import base64
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import sys
import time
import uuid

SOURCE_SHA256 = (hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
                 if '__file__' in globals() else globals().get('CACHE_WORKER_SHA256'))
CONTRACT = 'disposable_pack_compute_cache_v1'
MAX_OBJECT_BYTES = 16 * 1024**3
RETENTION_SECONDS = 7 * 86400


def bootstrap(worker_source, repo, reserve):
    if type(reserve) is not int or reserve < 1024**3:
        raise ValueError('remote scratch needs explicit physical reserve')
    source = Path(__file__).read_bytes()
    if hashlib.sha256(source).hexdigest() != SOURCE_SHA256:
        raise ValueError('compute-cache worker changed after import')
    return ("import base64,hashlib,types;w=base64.b64decode("+
            repr(base64.b64encode(worker_source).decode())+
            ");n={'__name__':'fixed_pack_library','WORKER_SHA256':hashlib.sha256(w).hexdigest()};"
            "exec(compile(w,'<fixed-pack-library>','exec'),n);s=base64.b64decode("+
            repr(base64.b64encode(source).decode())+
            ");c={'__name__':'fixed_cache_library','CACHE_WORKER_SHA256':hashlib.sha256(s).hexdigest()};"
            "exec(compile(s,'<fixed-cache-library>','exec'),c);"
            "c['serve'](types.SimpleNamespace(**n),"+repr(str(Path(repo)/'service_data/pack_compute_cache'))+","+str(reserve)+")")


def signature(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_mode, info.st_nlink)


class ComputeCache:
    def __init__(self, root, worker, reserve):
        self.root, self.worker, self.reserve = Path(root), worker, reserve
        # Private, real directories only, including all existing ancestors.
        for path in (self.root, *self.root.parents):
            if path.is_symlink():
                raise ValueError('remote compute cache path is redirected')
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = self.root.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_mode & 0o077):
            raise ValueError('remote compute cache is not privately owned')
        self.identity = (info.st_dev, info.st_ino)

    def guard(self):
        info = self.root.lstat()
        if (self.root.resolve() != self.root or (info.st_dev, info.st_ino) != self.identity
                or not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077):
            raise ValueError('remote compute cache authority changed')

    @contextlib.contextmanager
    def lock(self, item):
        self.guard()
        digest = item.get('sha256')
        if (not isinstance(digest, str) or re.fullmatch('[0-9a-f]{64}', digest) is None
                or type(item.get('bytes')) is not int or not 0 < item['bytes'] <= MAX_OBJECT_BYTES
                or item.get('kind') != 'pack'):
            raise ValueError('unbounded or invalid closed pack computation')
        path = self.root / (digest+'.lock')
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
                raise ValueError('compute-cache lock is not owned regular metadata')
            # Never wait on another job while holding remote admission state.
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield self.root / (digest+'.zip'), self.root / (digest+'.json')
            self.guard()
        finally:
            os.close(fd)  # Lock inode is retained; never split an active owner.

    def read_proof(self, path):
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                    or before.st_mode & 0o077 or before.st_nlink != 1 or before.st_size > 65536):
                raise ValueError('compute-cache proof is not bounded private metadata')
            raw = stream.read(65537)
            if signature(before) != signature(os.fstat(stream.fileno())) or signature(before) != signature(path.lstat()):
                raise ValueError('compute-cache proof changed during read')
        return json.loads(raw)

    def lookup(self, item, rows=None):
        with self.lock(item) as (path, control):
            try:
                proof = self.read_proof(control)
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            except FileNotFoundError:
                return None
            with os.fdopen(fd, 'rb') as stream:
                before = os.fstat(stream.fileno())
                if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                        or before.st_mode & 0o077 or before.st_nlink != 1):
                    raise ValueError('cached pack is redirected or not privately owned')
                # Every hit rehashes the COMPLETE encoded object. The cached
                # computation alone cannot conceal bit rot or changed bytes.
                digest = hashlib.sha256()
                while block := stream.read(8*1024**2):
                    digest.update(block)
                if (digest.hexdigest() != item['sha256'] or before.st_size != item['bytes']
                        or signature(before) != signature(os.fstat(stream.fileno()))
                        or signature(before) != signature(path.lstat())):
                    raise ValueError('cached pack full SHA/generation differs')
                if (proof.get('cache_worker_sha256') != SOURCE_SHA256
                        or proof.get('worker_sha256') != self.worker.SOURCE_SHA256
                        or proof.get('object_sha256') != item['sha256']
                        or proof.get('object_bytes') != item['bytes']
                        or proof.get('inventory_bound') is not False):
                    return None
                if rows is not None:
                    expected = self.worker.expected_result(item, rows)
                    if any(proof.get(k) != v for k, v in expected.items()):
                        # A current inventory may select a subset of an old
                        # pack. Keep every unselected stored member's CRC gate.
                        proof = self.worker.inspect_pack(stream, item, rows)
                    if proof.get('stored_members_crc_verified') != int(item.get('stored_file_count', item['file_count'])):
                        raise ValueError('cached stored member count differs')
                if (signature(before) != signature(os.fstat(stream.fileno()))
                        or signature(before) != signature(path.lstat())):
                    raise ValueError('cached pack generation changed during reconstruction')
                return {**proof, 'inventory_bound': rows is not None,
                        'cache_hit': True, 'payload_storage': 'private_disposable_disk',
                        'cache_worker_sha256': SOURCE_SHA256, 'remote_files_created': 0}

    def store(self, incoming, item):
        with self.lock(item) as (path, control):
            if shutil.disk_usage(self.root).free < self.reserve + item['bytes']:
                raise ValueError('remote temporary physical capacity unavailable')
            partial = self.root / (item['sha256']+'.'+uuid.uuid4().hex+'.partial')
            try:
                fd = os.open(partial, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'w+b') as stream:
                    remaining = item['bytes']
                    while remaining:
                        if shutil.disk_usage(self.root).free < self.reserve:
                            raise ValueError('remote temporary physical reserve reached')
                        block = incoming.read(min(remaining, 8*1024**2))
                        if not block:
                            raise EOFError('incomplete temporary pack upload')
                        stream.write(block)
                        remaining -= len(block)
                    stream.flush()
                    os.fsync(stream.fileno())
                    proof = self.worker.inspect_pack(stream, item, None)
                proof.update(cache_worker_sha256=SOURCE_SHA256, computed_at_unix=time.time())
                temporary = partial.with_suffix('.json.partial')
                try:
                    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
                    with os.fdopen(fd, 'w') as stream:
                        json.dump(proof, stream, separators=(',', ':'))
                        stream.flush(); os.fsync(stream.fileno())
                    # Replacing only our exact owned cache names is safe under
                    # the per-object lock; unknown redirected names are refused.
                    for old in (path, control):
                        if old.exists() or old.is_symlink():
                            info = old.lstat()
                            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
                                raise ValueError('preserve unknown remote scratch name')
                    os.replace(partial, path)
                    os.replace(temporary, control)
                    directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
                    try: os.fsync(directory)
                    finally: os.close(directory)
                finally:
                    temporary.unlink(missing_ok=True)
                return {**proof, 'cache_hit': False, 'payload_storage': 'private_disposable_disk',
                        'remote_files_created': 2}
            finally:
                partial.unlink(missing_ok=True)

    def gc(self, now=None):
        """Retire only old, valid, reconstructible cache pairs under their lock."""
        self.guard()
        now, removed, released = time.time() if now is None else now, 0, 0
        for control in self.root.glob('*.json'):
            if re.fullmatch('[0-9a-f]{64}.json', control.name) is None:
                continue
            try:
                proof = self.read_proof(control)
                if now - proof.get('computed_at_unix', now) < RETENTION_SECONDS:
                    continue
                item = {'kind': 'pack', 'sha256': control.stem, 'bytes': proof['object_bytes']}
                if proof.get('object_sha256') != control.stem or proof.get('cache_worker_sha256') != SOURCE_SHA256:
                    continue
                with self.lock(item) as (payload, metadata):
                    if self.read_proof(metadata) != proof:
                        continue
                    info = payload.lstat()
                    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                            or info.st_nlink != 1 or info.st_size != item['bytes']):
                        continue
                    released += info.st_blocks*512
                    payload.unlink(); metadata.unlink(); removed += 1
            except (OSError, ValueError, KeyError, TypeError):
                continue  # Preserve unknown/busy entries, not a prune authority.
        return {'removed_cache_pairs': removed, 'released_allocated_bytes': released}


def serve(worker, root, reserve):
    cache = ComputeCache(root, worker, reserve)
    incoming, outgoing = sys.stdin.buffer, sys.stdout.buffer
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    identity = {'contract': worker.CONTRACT, 'boot_id': boot, 'worker_sha256': worker.SOURCE_SHA256,
                'cache_worker_sha256': SOURCE_SHA256}
    worker._write_frame(outgoing, {**identity, 'state': 'ready', 'memory_headroom_bytes': worker._memory_headroom()})
    manifest = None
    while True:
        size = int.from_bytes(worker._read_blocking(incoming, 8), 'big')
        if not 0 < size <= 64*1024**2:
            raise ValueError('unbounded cached compute request')
        request = json.loads(worker._read_blocking(incoming, size))
        if request.get('boot_id') != boot or request.get('worker_sha256') != worker.SOURCE_SHA256:
            raise ValueError('cached compute stream identity differs')
        if request.get('mode') == 'stop':
            worker._write_frame(outgoing, {**identity, 'state': 'memory_worker_closed', **cache.gc()})
            return
        if manifest is None:
            manifest = request['manifest_sha256']
        if manifest != request.get('manifest_sha256'):
            raise ValueError('cached compute stream changed its fixed context')
        if (request.get('mode') not in {'stage', 'stage_lookup', 'verify', 'cached_verify'}
                or re.fullmatch('[0-9a-f]{32}', request['job_id']) is None
                or not 1 <= request['timeout_seconds'] <= 1800):
            raise ValueError('invalid cached compute request')
        signal.setitimer(signal.ITIMER_REAL, request['timeout_seconds'])
        try:
            item, mode = request['item'], request['mode']
            started = time.monotonic()
            if mode in {'stage_lookup', 'cached_verify'}:
                result = cache.lookup(item, request['rows'] if mode == 'cached_verify' else None)
                if result is None:
                    result = {'state': 'cache_miss'}
            else:
                result = cache.store(incoming, item)
                if mode == 'verify':
                    result = cache.lookup(item, request['rows'])
            worker._write_frame(outgoing, {**result, **identity, 'job_id': request['job_id'],
                'manifest_sha256': manifest, 'verification_seconds': time.monotonic()-started})
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
