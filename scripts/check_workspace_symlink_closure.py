#!/usr/bin/env python3
"""Check enrolled staged symlink targets after the original copy completes."""
from __future__ import annotations

import json
import fcntl
import os
from pathlib import Path
import stat
import time

from downloader.artifact_io import atomic_write_json
from scripts import run_workspace_source_staging as queue
from stockagent.data_sync import workspace_symlink_closure as closure, workspace_volume as volume


def main() -> int:
    control = volume.ENROLLMENT.parent / 'source-symlink-closure-v1'
    control.mkdir(mode=0o700, parents=True, exist_ok=True)
    if control.is_symlink() or control.resolve(strict=True) != control:
        raise RuntimeError('symlink verification owner root is redirected')
    descriptor = os.open(control / 'owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'r+b') as owner:
        if not stat.S_ISREG(os.fstat(owner.fileno()).st_mode):
            raise RuntimeError('symlink verification owner lock is not a regular file')
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({'state': 'waiting', 'reason': 'verification_owner_busy'}))
            return 75
        return check_registered_sources()


def check_registered_sources() -> int:
    volume.check()
    workspace = Path(volume.configuration()['mount_point'])
    rows = []
    for item in queue.load_sources():
        if not item.get('preserve_symlinks', False):
            continue
        try:
            result = closure.check_item(item, workspace)
            rows.append({'name': item['name'], **result})
        except Exception as error:
            rows.append({'name': item['name'], 'state': 'blocked', 'error_type': type(error).__name__})
    migration = workspace / 'migration'
    if migration.is_symlink() or migration.resolve(strict=True) != migration:
        raise RuntimeError('symlink verification status root is redirected')
    atomic_write_json(migration / 'source-symlink-closure-status.json',
        {'contract': closure.CONTRACT, 'observed_at_epoch': time.time(), 'sources': rows}, durable=True)
    print(json.dumps({'sources': rows}, ensure_ascii=False))
    return 1 if any(row['state'] in {'blocked', 'unresolved'} for row in rows) else 0


if __name__ == '__main__':
    raise SystemExit(main())
