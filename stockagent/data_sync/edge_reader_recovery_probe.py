"""Read-only preflight for reinstalling an enrolled isolated edge reader."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

CONTRACT = 'stockagent.edge_reader_recovery_probe.v1'
HASH = re.compile(r'^[a-f0-9]{64}$')
NAME = re.compile(r'^[A-Za-z0-9_./-]+$')


def _read(path: Path, *, maximum: int) -> bytes:
    if path.is_symlink() or not path.is_file() or path.resolve(strict=True) != path:
        raise ValueError('reader preflight file is unavailable or redirected')
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1 or info.st_size > maximum:
        raise ValueError('reader preflight file ownership or size differs')
    body = path.read_bytes()
    after = path.stat()
    if (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError('reader preflight file changed during observation')
    return body


def _release_intact(release: Path, repo: Path, expected: str) -> bool:
    try:
        if (release.is_symlink() or not release.is_dir() or release.resolve(strict=True) != release
                or release.stat().st_uid != os.geteuid() or stat.S_IMODE(release.stat().st_mode) != 0o700
                or not (release/'configs').is_symlink() or (release/'configs').resolve(strict=True) != repo/'configs'):
            return False
        raw = _read(release/'reader-manifest.sha256', maximum=256*1024)
        if hashlib.sha256(raw).hexdigest() != expected:
            return False
        seen = set()
        for line in raw.decode('ascii').splitlines():
            checksum, name = line.split('  ', 1)
            member = Path(name)
            if (not HASH.fullmatch(checksum) or not NAME.fullmatch(name) or member.is_absolute()
                    or '..' in member.parts or member.as_posix() != name or name in seen
                    or hashlib.sha256(_read(release/member, maximum=32*1024**2)).hexdigest() != checksum):
                return False
            seen.add(name)
            if len(seen) > 1024:
                return False
        return bool(seen)
    except (OSError, UnicodeError, ValueError):
        return False


def inspect(request: dict, *, state_root=Path('/var/lib/stockagent-packed-edge')) -> dict:
    fields = {'repo', 'baseline_wrapper_sha256', 'wrapper_sha256', 'reader_manifest_sha256'}
    if (not isinstance(request, dict) or set(request) != fields
            or not isinstance(request.get('repo'), str) or not request['repo'].startswith('/')
            or any(not isinstance(request.get(k), str) or not HASH.fullmatch(request[k]) for k in fields-{'repo'})):
        raise ValueError('reader recovery preflight request differs')
    repo, state = Path(request['repo']), Path(state_root).absolute()
    try:
        valid_roots = (repo.is_absolute() and '..' not in repo.parts and repo.resolve(strict=True) == repo
                       and not state.is_symlink() and state.is_dir() and state.resolve(strict=True) == state
                       and state.stat().st_uid == os.geteuid())
    except OSError:
        valid_roots = False
    if not valid_roots:
        return {'contract': CONTRACT, 'state': 'base_not_enrolled'}
    for path in (repo/'configs', repo/'scripts', repo/'artifacts/code_releases'):
        if path.is_symlink() or not path.is_dir() or path.resolve(strict=True) != path:
            return {'contract': CONTRACT, 'state': 'base_not_enrolled'}
    try:
        _read(state/'state.json', maximum=8*1024*1024)
        _read(state/'operation.lock', maximum=65536)
        wrapper = hashlib.sha256(_read(repo/'scripts/run_data_cache.sh', maximum=1024*1024)).hexdigest()
    except (OSError, ValueError):
        return {'contract': CONTRACT, 'state': 'base_not_enrolled'}
    if wrapper not in {request['baseline_wrapper_sha256'], request['wrapper_sha256']}:
        return {'contract': CONTRACT, 'state': 'wrapper_mismatch'}
    control = state/'reader-release.control'
    release = repo/'artifacts/code_releases'/('stockagent-edge-reader-v3-'+request['reader_manifest_sha256'][:16])
    if os.path.lexists(control):
        try:
            raw = _read(control, maximum=4096)
            if stat.S_IMODE(control.stat().st_mode) != 0o600 or raw != (
                    f"{release} {request['reader_manifest_sha256']} {repo}\n".encode()):
                return {'contract': CONTRACT, 'state': 'control_mismatch'}
        except (OSError, ValueError):
            return {'contract': CONTRACT, 'state': 'control_mismatch'}
        if os.path.lexists(release):
            # A present, intact release cannot be fixed by reinstallation when
            # its metadata probe fails (for example while the index syncs).
            # A changed release is never overwritten automatically.
            return {'contract': CONTRACT,
                    'state': 'reader_intact' if _release_intact(release, repo, request['reader_manifest_sha256'])
                             else 'release_mismatch'}
    return {'contract': CONTRACT, 'state': 'eligible',
            'control_present': os.path.lexists(control), 'wrapper_is_current': wrapper == request['wrapper_sha256']}


if __name__ == '__main__':
    try:
        print(json.dumps(inspect(json.load(sys.stdin)), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f'{type(error).__name__}: reader recovery preflight failed', file=sys.stderr)
        raise SystemExit(1)
