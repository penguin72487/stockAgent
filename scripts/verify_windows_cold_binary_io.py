#!/usr/bin/env python3
"""Explicit sustained native-D write/flush/read/SHA acceptance; private test data."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.desync_snapshots import atomic_write_json
from stockagent.data_sync.windows_cold_io import BinaryWriter, hash_file


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bytes', type=int, default=1024**3)
    p.add_argument('--receipt', type=Path, required=True)
    p.add_argument('--append-bytes',type=int,default=0)
    p.add_argument('--apply', action='store_true')
    args = p.parse_args()
    if not args.apply or not 1024**2 <= args.bytes <= 4 * 1024**3 or not 0<=args.append_bytes<=1024**3:
        p.error('Explicit apply and bounded 1 MiB to 4 GiB validation required')
    root = Path('/mnt/d/stockagent-cold-primary/remote-artifact-incoming') / ('.io-validation-' + uuid.uuid4().hex)
    root.mkdir(mode=0o700)
    path = root / 'native-binary-validation.bin'
    started = time.monotonic()
    value, size = hashlib.sha256(), 0
    writer = BinaryWriter(path, reserve_bytes=64 * 1024**3)
    try:
        while size < args.bytes:
            block = os.urandom(min(4 * 1024**2, args.bytes - size))
            value.update(block)
            writer.write(block)
            size += len(block)
        durable = writer.finish()
    finally:
        writer.close()
    written = time.monotonic()
    actual = hash_file(path)
    if actual != value.hexdigest() or actual != durable['sha256']:
        raise RuntimeError('Native binary full readback differs; retain private validation bytes')
    if args.append_bytes:
        writer=BinaryWriter(path,offset=size,prefix_sha256=actual,reserve_bytes=64*1024**3)
        appended=0
        try:
            while appended<args.append_bytes:
                block=os.urandom(min(4*1024**2,args.append_bytes-appended))
                value.update(block);writer.write(block);appended+=len(block)
            durable=writer.finish()
        finally:
            writer.close()
        size+=appended
        actual=hash_file(path)
        if actual!=value.hexdigest() or actual!=durable['sha256']:
            raise RuntimeError('Native exact-prefix append differs; retain test bytes')
    result = {'state': 'sustained_native_d_binary_io_verified', 'bytes': size,
              'sha256': actual, 'write_flush_seconds': written - started,
              'full_readback_seconds': time.monotonic() - written, 'flushed': durable['flushed'],
              'path': str(path), 'temporary_validation_removed': False}
    result['exact_prefix_append_bytes']=args.append_bytes
    atomic_write_json(args.receipt, result)
    # Only our exact, exclusive, fully hashed synthetic test data may be removed.
    if set(root.iterdir()) != {path} or path.is_symlink() or path.stat().st_size != size:
        raise RuntimeError('Validation directory changed; retain')
    path.unlink()
    root.rmdir()
    result['temporary_validation_removed'] = True
    atomic_write_json(args.receipt, result)
    print(json.dumps(result), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
