"""Verify every staged reader byte before executing its installer or smoke."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys

CONTRACT = 'stockagent.edge_reader_bundle_preflight.v1'
HASH = re.compile(r'^[a-f0-9]{64}$')
NAME = re.compile(r'^[A-Za-z0-9_./-]+$')
TMP = re.compile(r'^/tmp/stockagent-edge-recovery\.[A-Za-z0-9]{8}$')


def _read(path: Path, maximum=32*1024**2) -> bytes:
    if path.is_symlink() or not path.is_file() or path.resolve(strict=True) != path:
        raise ValueError('staged reader file is unavailable or redirected')
    before = path.stat()
    if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
            or before.st_nlink != 1 or before.st_size > maximum):
        raise ValueError('staged reader file ownership or size differs')
    body = path.read_bytes()
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError('staged reader file changed during inspection')
    return body


def _safe(name: str) -> bool:
    path = PurePosixPath(name)
    return bool(NAME.fullmatch(name)) and not path.is_absolute() and '..' not in path.parts and name == path.as_posix()


def inspect(request: dict, *, enforce_temporary_scope=True) -> dict:
    keys = {'root', 'source_identity_sha256', 'reader_manifest_sha256', 'wrapper_sha256', 'installer_sha256'}
    if (not isinstance(request, dict) or set(request) != keys or not isinstance(request.get('root'), str)
            or enforce_temporary_scope and not TMP.fullmatch(request['root'])
            or any(not isinstance(request.get(k), str) or not HASH.fullmatch(request[k]) for k in keys-{'root'})):
        raise ValueError('staged reader request differs')
    root = Path(request['root']).absolute()
    if (root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root
            or root.stat().st_uid != os.geteuid() or stat.S_IMODE(root.stat().st_mode) != 0o700):
        raise ValueError('staged reader root is unsafe')
    source = root/'source'
    if source.is_symlink() or not source.is_dir() or source.resolve(strict=True) != source:
        raise ValueError('staged reader source is unsafe')
    source_map = json.loads(_read(source/'edge-source-sha.json', maximum=256*1024))
    if (not isinstance(source_map, dict) or not 1 <= len(source_map) <= 512
            or any(not isinstance(k, str) or not _safe(k) or not isinstance(v, str) or not HASH.fullmatch(v)
                   for k, v in source_map.items())
            or hashlib.sha256(json.dumps(source_map, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
               != request['source_identity_sha256']):
        raise ValueError('staged reader source map differs')
    files = {}
    total = 0
    for current, directories, names in os.walk(source, followlinks=False):
        here = Path(current)
        if any((here/name).is_symlink() for name in directories):
            raise ValueError('staged reader directory is redirected')
        for name in names:
            path = here/name
            relative = path.relative_to(source).as_posix()
            if relative == 'edge-source-sha.json':
                continue
            if not _safe(relative) or relative in files:
                raise ValueError('staged reader name differs')
            body = _read(path)
            total += len(body)
            if len(files) >= 1024 or total > 64*1024**2:
                raise ValueError('staged reader exceeds its bound')
            files[relative] = hashlib.sha256(body).hexdigest()
    if {name: value for name, value in files.items() if not name.startswith('vendor/')} != source_map:
        raise ValueError('staged reader source members differ')
    manifest = ''.join(f'{value}  {name}\n' for name, value in sorted(files.items())).encode()
    if hashlib.sha256(manifest).hexdigest() != request['reader_manifest_sha256']:
        raise ValueError('staged reader release manifest differs')
    if (hashlib.sha256(_read(root/'wrapper.sh')).hexdigest() != request['wrapper_sha256']
            or hashlib.sha256(_read(root/'installer.py')).hexdigest() != request['installer_sha256']):
        raise ValueError('staged reader deployment tools differ')
    return {'contract': CONTRACT, 'state': 'verified', 'reader_file_count': len(files),
            'reader_manifest_sha256': request['reader_manifest_sha256']}


if __name__ == '__main__':
    try:
        print(json.dumps(inspect(json.load(sys.stdin)), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f'{type(error).__name__}: staged reader preflight failed', file=sys.stderr)
        raise SystemExit(1)
