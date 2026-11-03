#!/usr/bin/env python3
"""Bounded ABBA replay of exact ZIP window placement on unchanged source files.

Uses the canonical writer and its source fences. No publication, backup or
retirement is performed; only this benchmark's fully read-back outputs are
removed. Timings cover pack creation, required encoded SHA and member recovery.
"""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import time
from unittest.mock import patch
import zipfile

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import atomic_write_json, sha256_file


def select_entries(source, *, maximum_files, maximum_bytes):
    entries, byte_count, visited = [], 0, 0

    def walk(directory):
        with os.scandir(directory) as names:
            for name in names:
                if name.name.startswith('.') or name.is_symlink():
                    continue
                if name.is_dir(follow_symlinks=False):
                    yield from walk(Path(name.path))
                else:
                    yield name

    for name in walk(source):
        visited += 1
        if visited > 100_000:
            break
        info = name.stat(follow_symlinks=False)
        if (not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= 1024**2
                or Path(name.path).suffix not in {'.parquet', '.csv', '.json'}):
            continue
        if byte_count + info.st_size > maximum_bytes:
            break
        entry = packed._SourceEntry(Path(name.path).relative_to(source).as_posix(),
            'file', stat.S_IMODE(info.st_mode), info.st_size, source_stat=packed._source_stat(info))
        entry.sha256 = packed._hash_source_entry(source, entry)
        entries.append(entry)
        byte_count += info.st_size
        if len(entries) == maximum_files:
            break
    if byte_count <= 2 * packed.PACK_WRITE_BUFFER_BYTES:
        raise ValueError('need more than two write windows of fixed source content')
    return sorted(entries, key=lambda item: item.path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--maximum-files', type=int, default=4096)
    parser.add_argument('--maximum-mib', type=int, default=64)
    args = parser.parse_args()
    source = args.source.absolute()
    if (source.resolve() != source or not source.is_dir() or args.output.exists()
            or args.scratch.exists() or args.scratch.is_symlink()
            or not 1 <= args.maximum_files <= 8192 or not 24 <= args.maximum_mib <= 256):
        parser.error('use fixed ordinary source, fresh outputs and a bounded workload')
    args.scratch.mkdir(mode=0o700)
    if args.scratch.resolve() != args.scratch.absolute():
        raise ValueError('scratch namespace is redirected')
    if str(args.scratch).startswith('/srv/stockagent-packed/'):
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        _check_d_primary_mount(Path('/srv/stockagent-packed'))
    if shutil.disk_usage(args.scratch).free < 32 * 1024**3 + args.maximum_mib * 1024**2 * 2:
        raise ValueError('benchmark must retain physical disk reserve')
    entries = select_entries(source, maximum_files=args.maximum_files,
                             maximum_bytes=args.maximum_mib * 1024**2)
    report = {'contract': 'stockagent.pack_window_abba.v1', 'acceptance': False,
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'files': len(entries), 'source_bytes': sum(entry.size for entry in entries),
        'inventory_sha256': hashlib.sha256(json.dumps([(e.path, e.size, e.sha256)
            for e in entries], ensure_ascii=False).encode()).hexdigest(),
        'writer_sha256': sha256_file(Path(packed.__file__)), 'runs': []}
    for index, candidate in enumerate((False, True, True, False)):
        output = args.scratch / f'{index}.zip'
        began = time.monotonic()
        with (nullcontext() if candidate else
              patch.object(packed._SeekablePackBuffer, 'reserve_member', lambda *_: None)):
            produced_sha = packed._write_pack(source, entries, output, compression_level=6)
        packed_at = time.monotonic()
        encoded_sha = produced_sha or sha256_file(output)
        hashed_at = time.monotonic()
        if sha256_file(output) != encoded_sha:
            raise ValueError('independent full encoded SHA differs')
        with zipfile.ZipFile(output) as archive:
            if archive.namelist() != [entry.path for entry in entries]:
                raise ValueError('archive member names or order differ')
            for entry in entries:
                raw = archive.read(entry.path)  # checks complete member CRC too
                if len(raw) != entry.size or hashlib.sha256(raw).hexdigest() != entry.sha256:
                    raise ValueError('complete original recovery differs')
                packed._ensure_source_stat(source / entry.path, entry)
        finished = time.monotonic()
        if report['runs'] and report['runs'][0]['archive_sha256'] != encoded_sha:
            raise ValueError('archive bytes changed across the boundary-only replay')
        report['runs'].append({'candidate': candidate, 'archive_sha256': encoded_sha,
            'archive_bytes': output.stat().st_size, 'fused_sha': produced_sha is not None,
            'pack_seconds': packed_at-began, 'required_hash_seconds': hashed_at-packed_at,
            'independent_readback_seconds': finished-hashed_at,
            'complete_workflow_seconds': finished-began})
        output.unlink()  # only the exact generated output, after complete readback
        atomic_write_json(args.output, report)
    args.scratch.rmdir()
    report['verified_exact_replay'] = True
    atomic_write_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main()
