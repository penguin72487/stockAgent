"""Guarded native D ZIP member streams for source-local dedup candidates."""
from __future__ import annotations

import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile

from stockagent.data_sync import packed_snapshots as p, windows_cold_io as w
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError

SCRIPT = Path(__file__).resolve().parents[2]/'scripts/windows_dedup_zip_io.ps1'
_CAS = re.compile(r'^D:\\stockagent-cold-primary\\packed\\objects\\packs\\([0-9a-f]{2})\\([0-9a-f]{64})\.zip$', re.I)
_FIXTURE = re.compile(r'^D:\\stockagent-completed-benchmark-scratch\\stockagent-zip-reader-probe-[a-z0-9_]{8}\\pack-[0-9]+\.zip$', re.I)


@contextmanager
def binary_zip_members(groups, *, allow_fixture=False):
    """Stream bounded selected entries; source ZIP encoded SHA is not reread."""
    if (type(allow_fixture) is not bool or not isinstance(groups, list) or not 1 <= len(groups) <= 16):
        raise SnapshotError('native ZIP group bound is invalid')
    _check_d_primary_mount(Path('/srv/stockagent-packed'))
    configs, signatures, total, member_count, seen_paths = [], [], 0, 0, set()
    for group in groups:
        if not isinstance(group, dict):
            raise SnapshotError('native ZIP group is invalid')
        path, expected, members = Path(group['path']).absolute(), group.get('expected_bytes'), group.get('members')
        if (path in seen_paths or type(expected) is not int or not 0 <= expected <= 8*1024**3
                or not isinstance(members, list) or not members):
            raise SnapshotError('native ZIP source or members are invalid')
        seen_paths.add(path)
        before = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size != expected:
            raise SnapshotError('native ZIP source generation differs')
        native = w.windows_path(path)
        match = _CAS.fullmatch(native or '')
        fixture = bool(allow_fixture and _FIXTURE.fullmatch(native or ''))
        if (not match and not fixture) or match and match.group(1).lower() != match.group(2)[:2].lower():
            raise SnapshotError('native ZIP source is outside the fixed namespace')
        # This repeats the standard guarded read's real D mount/alias checks.
        w._configuration_on_checked_mount(path, mode='read', expected_bytes=expected)
        selected, names = [], set()
        for item in members:
            if not isinstance(item, dict):
                raise SnapshotError('native ZIP member descriptor is invalid')
            name, size = item.get('name'), item.get('size')
            if (not isinstance(name, str) or '\x00' in name
                    or p._safe_relative_path(name, 'native ZIP member').as_posix() != name
                    or name in names or type(size) is not int or not 0 <= size <= 128*1024**2):
                raise SnapshotError('native ZIP member descriptor is invalid')
            names.add(name)
            selected.append({'name': name, 'size': size})
            total += size
            member_count += 1
        signatures.append((path, before))
        configs.append({'path': native, 'expected_bytes': expected, 'members': selected, 'fixture': fixture})
    if member_count > 2048 or total > 128*1024**2:
        raise SnapshotError('native ZIP member batch exceeds its size or count bound')
    encoded = base64.b64encode(SCRIPT.read_text().encode('utf-16le')).decode('ascii')
    environment = w._windows_process_environment()
    errors = tempfile.TemporaryFile()
    process = reader = None
    try:
        process = subprocess.Popen([w.POWERSHELL, '-NoLogo', '-NoProfile', '-NonInteractive',
                                    '-EncodedCommand', encoded], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=errors, env=environment)
        process.stdin.write(json.dumps({'groups': configs}, ensure_ascii=True).encode('utf-8')+b'\n')
        process.stdin.close()
        reader = w._IdleBoundedReader(process.stdout, 120)
        yield reader
        if reader.read(1):
            raise SnapshotError('native ZIP batch has unconsumed or excess bytes')
        if process.wait(timeout=120):
            errors.seek(0)
            raise SnapshotError('native ZIP member reader failed: '+errors.read(4096).decode(errors='replace'))
        _check_d_primary_mount(Path('/srv/stockagent-packed'))
        if any(not w._stable(before, path.lstat()) for path, before in signatures):
            raise SnapshotError('native ZIP source changed during member reads')
    finally:
        if reader is not None:
            reader.close()
        if process is not None:
            if process.stdin is not None and not process.stdin.closed:
                try:
                    process.stdin.close()
                except BrokenPipeError:
                    pass
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()  # Only this read-only child, never a shared owner.
                    process.wait(timeout=10)
            process.stdout.close()
        errors.close()
