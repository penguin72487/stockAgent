#!/usr/bin/env python3
"""Measure closed C-to-D blob writes; no publication or original-data cleanup."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import atomic_write_json, SnapshotError
from stockagent.data_sync.packed_snapshots import _copy_and_hash, _native_copy_and_hash
from stockagent.data_sync.training_return import admit_workspace
from stockagent.data_sync.windows_cold_io import hash_file


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--size-mib', type=int, choices=(64, 512, 1024), default=64)
    args = parser.parse_args()
    os.umask(0o077)
    if args.receipt.exists():
        raise SnapshotError('use a fresh benchmark receipt')
    cold = Path('/srv/stockagent-packed')
    _check_d_primary_mount(cold)
    needed = args.size_mib * 1024**2
    admit_workspace(Path('/var/tmp'), 32 * 1024**3 + needed)
    private = cold / '.local-state' / ('blob-write-measurement-' + uuid.uuid4().hex)
    private.mkdir(mode=0o700)
    trials = []
    # One privately generated, closed source; identical complete flow and
    # interleaved modes. Do not assert that OS caches were flushed.
    with tempfile.TemporaryDirectory(prefix='stockagent-cold-io-', dir='/var/tmp') as cdir:
        source = Path(cdir) / 'random-input'
        digest = hashlib.sha256()
        with source.open('xb') as stream:
            for _ in range(args.size_mib // 8):
                block = os.urandom(8 * 1024**2)
                digest.update(block)
                stream.write(block)
            stream.flush()
            os.fsync(stream.fileno())
        expected = digest.hexdigest()
        for round in range(3):
            for mode, method in [('drvfs', _copy_and_hash), ('native', _native_copy_and_hash)]:
                _check_d_primary_mount(cold)
                target = private / (mode + '-' + str(round) + '.partial')
                started = time.monotonic()
                copied = method(source, target)
                recovered = hash_file(target)
                seconds = time.monotonic() - started
                if copied != expected or recovered != expected or target.stat().st_size != source.stat().st_size:
                    raise SnapshotError('real D write or independent readback differed; scratch retained')
                trials.append({'mode':mode, 'round':round, 'bytes':source.stat().st_size,
                               'seconds':seconds, 'full_readback_sha256_verified':True})
                target.unlink()  # exact privately generated benchmark bytes only
                print(json.dumps(trials[-1]), flush=True)
        private.rmdir()
    result = {'schema_version':1, 'closed_source_sha256':expected, 'trials':trials,
              'original_files_changed':0, 'published_releases':0,
              'cold_objects_deleted':0, 'os_cache_flushed':False}
    atomic_write_json(args.receipt, result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
