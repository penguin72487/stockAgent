#!/usr/bin/env python3
"""Stage and atomically select a source-pinned edge reader on one compute node.

The reader continues to use the existing edge lock, state, Syncthing and cache.
Only code, its isolated dependency and a small control file are installed.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile

CONTRACT = 'stockagent.isolated_edge_reader_release.v1'
PATH_PATTERN = re.compile(r'^[A-Za-z0-9_./-]+$')
HASH_PATTERN = re.compile(r'^[0-9a-f]{64}$')


def _hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _regular(path: Path, maximum=32*1024**2) -> bytes:
    if path.is_symlink() or not path.is_file() or path.resolve(strict=True) != path:
        raise ValueError('reader member is missing or redirected')
    before = path.stat()
    if before.st_size > maximum:
        raise ValueError('reader member exceeds its bound')
    body = path.read_bytes()
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError('reader member changed while copied')
    return body


def _safe_name(name: str) -> bool:
    path = PurePosixPath(name)
    return (bool(PATH_PATTERN.fullmatch(name)) and not path.is_absolute()
            and '..' not in path.parts and '.' not in path.parts and name == path.as_posix())


def _map(source: Path, expected_identity: str) -> dict[str, bytes]:
    value = json.loads(_regular(source/'edge-source-sha.json', maximum=256*1024))
    if (not isinstance(value, dict) or not 1 <= len(value) <= 512
            or any(not isinstance(k, str) or not _safe_name(k) or not isinstance(v, str)
                   or not HASH_PATTERN.fullmatch(v) for k, v in value.items())
            or _hash(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()) != expected_identity):
        raise ValueError('reader source identity differs')
    result = {}
    for name, digest in sorted(value.items()):
        body = _regular(source/name)
        if _hash(body) != digest:
            raise ValueError('reader source member differs')
        result[name] = body
    vendor = source/'vendor'
    if vendor.is_symlink() or not vendor.is_dir() or vendor.resolve(strict=True) != vendor:
        raise ValueError('reader dependency root is invalid')
    for current, directories, files in os.walk(vendor, followlinks=False):
        here = Path(current)
        if any((here/name).is_symlink() for name in directories):
            raise ValueError('reader dependency contains a redirected directory')
        directories[:] = [name for name in directories if name != '__pycache__']
        for name in files:
            if name.endswith(('.pyc', '.pyo')):
                continue
            path = here/name
            relative = path.relative_to(source).as_posix()
            if not _safe_name(relative):
                raise ValueError('reader dependency name is invalid')
            result[relative] = _regular(path)
    if len(result) > 1024 or sum(map(len, result.values())) > 64*1024**2:
        raise ValueError('reader release exceeds its bound')
    return result


def _manifest(files: dict[str, bytes]) -> bytes:
    return ''.join(f'{_hash(body)}  {name}\n' for name, body in sorted(files.items())).encode()


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _check_release(release: Path, repo: Path, manifest: bytes) -> None:
    if (release.is_symlink() or not release.is_dir() or release.resolve(strict=True) != release
            or stat.S_IMODE(release.stat().st_mode) != 0o700
            or not (release/'configs').is_symlink() or (release/'configs').resolve(strict=True) != repo/'configs'
            or _regular(release/'reader-manifest.sha256') != manifest):
        raise ValueError('reader release identity differs')
    seen = set()
    for line in manifest.decode().splitlines():
        digest, name = line.split('  ', 1)
        if (not HASH_PATTERN.fullmatch(digest) or not _safe_name(name) or name in seen
                or _hash(_regular(release/name)) != digest):
            raise ValueError('reader release member differs')
        seen.add(name)
    if not {'scripts/manage_packed_edge.py', 'scripts/verify_isolated_edge_reader.py',
            'stockagent/data_sync/packed_snapshots.py', 'stockagent/data_sync/packed_alias_v3.py',
            'stockagent/data_sync/packed_edge_cache.py', 'vendor/ijson/__init__.py'} <= seen:
        raise ValueError('reader release misses a required member')


def _smoke(release: Path, repo: Path) -> dict:
    env = {**os.environ, 'PYTHONPATH': str(release/'vendor'), 'PYTHONDONTWRITEBYTECODE': '1'}
    result = subprocess.run([sys.executable, str(release/'scripts/verify_isolated_edge_reader.py')],
                            cwd=release, env=env, capture_output=True, text=True, timeout=60, check=True)
    proof = json.loads(result.stdout)
    if (proof.get('contract') != 'stockagent.isolated_edge_reader_smoke.v1' or proof.get('state') != 'passed'
            or proof.get('network_used') is not False or proof.get('real_data_accessed') is not False
            or {row.get('schema_version') for row in proof.get('fixtures', [])} != {2, 3}):
        raise ValueError('reader fixture smoke differs')
    code = ('import json,sys; from pathlib import Path; '
            'from stockagent.data_sync import cache_capacity as c; '
            'p=Path(sys.argv[1])/"configs/data_sync/cache_capacity.json"; '
            'assert c.POLICY_PATH.resolve(strict=True)==p.resolve(strict=True); '
            'c.load_capacity_policy(); print(json.dumps({"policy_path_matches":True}))')
    policy = subprocess.run([sys.executable, '-c', code, str(repo)], cwd=release,
                            env={**env, 'PYTHONPATH': str(release)+os.pathsep+str(release/'vendor')},
                            capture_output=True, text=True, timeout=30, check=True)
    if json.loads(policy.stdout).get('policy_path_matches') is not True:
        raise ValueError('reader policy binding differs')
    return proof


def stage(source: Path, repo: Path, expected_identity: str) -> dict:
    source, repo = source.absolute(), repo.absolute()
    if (not HASH_PATTERN.fullmatch(expected_identity) or source.resolve(strict=True) != source
            or repo.resolve(strict=True) != repo or (repo/'configs').resolve(strict=True) != repo/'configs'):
        raise ValueError('reader source or canonical repository is redirected')
    files = _map(source, expected_identity)
    manifest = _manifest(files)
    digest = _hash(manifest)
    parent = repo/'artifacts/code_releases'
    if parent.is_symlink() or not parent.is_dir() or parent.resolve(strict=True) != parent:
        raise ValueError('reader release parent is unavailable or redirected')
    release = parent/('stockagent-edge-reader-v3-'+digest[:16])
    if not release.exists() and not release.is_symlink():
        temporary = Path(tempfile.mkdtemp(prefix='.edge-reader-pending-', dir=parent))
        try:
            for name, body in files.items():
                destination = temporary/name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
            (temporary/'reader-manifest.sha256').write_bytes(manifest)
            (temporary/'configs').symlink_to(repo/'configs')
            _check_release(temporary, repo, manifest)
            _smoke(temporary, repo)
            os.rename(temporary, release)
            _fsync_dir(parent)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)
    _check_release(release, repo, manifest)
    proof = _smoke(release, repo)
    return {'contract': CONTRACT, 'state': 'staged', 'release_root': str(release),
            'source_identity_sha256': expected_identity, 'manifest_sha256': digest,
            'files': len(files), 'smoke': proof, 'pointer_changed': False}


def activate(repo: Path, state: Path, release: Path, expected_manifest: str,
             wrapper_source: Path, expected_wrapper: str, expected_baseline: str,
             *, dataset: str, expected_dataset_sha: str) -> dict:
    repo, state, release, wrapper_source = (p.absolute() for p in (repo, state, release, wrapper_source))
    if (any(not HASH_PATTERN.fullmatch(v) for v in (expected_manifest, expected_wrapper, expected_baseline,
                                                     expected_dataset_sha))
            or repo.resolve(strict=True) != repo or state.resolve(strict=True) != state
            or release.parent != repo/'artifacts/code_releases'
            or release.name != 'stockagent-edge-reader-v3-'+expected_manifest[:16]):
        raise ValueError('reader activation scope differs')
    manifest = _regular(release/'reader-manifest.sha256')
    if _hash(manifest) != expected_manifest:
        raise ValueError('reader activation manifest differs')
    _check_release(release, repo, manifest)
    _smoke(release, repo)
    wrapper_body = _regular(wrapper_source)
    if _hash(wrapper_body) != expected_wrapper:
        raise ValueError('reader wrapper source differs')
    wrapper = repo/'scripts/run_data_cache.sh'
    original = _regular(wrapper)
    if _hash(original) not in {expected_baseline, expected_wrapper}:
        raise ValueError('canonical reader wrapper changed')
    control = state/'reader-release.control'
    wanted = f'{release} {expected_manifest} {repo}\n'.encode()
    if control.exists() or control.is_symlink():
        if _regular(control, maximum=4096) != wanted or stat.S_IMODE(control.stat().st_mode) != 0o600:
            raise ValueError('reader control already names another release')
    lock_path = state/'operation.lock'
    fd = os.open(lock_path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode) or not os.path.samestat(os.fstat(fd), lock_path.lstat()):
            raise ValueError('edge owner lock changed')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if _hash(_regular(wrapper)) not in {expected_baseline, expected_wrapper}:
            raise ValueError('canonical reader wrapper changed under owner')
        history = state/'reader-release-history'
        history.mkdir(mode=0o700, exist_ok=True)
        if history.is_symlink() or history.resolve(strict=True) != history:
            raise ValueError('reader wrapper history is redirected')
        if _hash(original) == expected_baseline:
            backup = history/(expected_baseline+'.sh')
            if backup.exists() or backup.is_symlink():
                if _regular(backup) != original:
                    raise ValueError('reader wrapper history differs')
            else:
                descriptor = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(descriptor, 'wb') as target:
                    target.write(original)
                    target.flush()
                    os.fsync(target.fileno())
            descriptor, temporary = tempfile.mkstemp(prefix='.edge-wrapper-', dir=wrapper.parent)
            try:
                with os.fdopen(descriptor, 'wb') as target:
                    target.write(wrapper_body)
                    target.flush()
                    os.fsync(target.fileno())
                os.chmod(temporary, stat.S_IMODE(wrapper.stat().st_mode))
                if _hash(_regular(wrapper)) != expected_baseline:
                    raise ValueError('canonical reader wrapper changed before switch')
                os.replace(temporary, wrapper)
                _fsync_dir(wrapper.parent)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        # Select the verified release only after the backward-compatible wrapper
        # is in place; no existing edge payload or owner state is modified.
        created = not control.exists()
        if created:
            descriptor, temporary = tempfile.mkstemp(prefix='.edge-reader-control-', dir=state)
            try:
                with os.fdopen(descriptor, 'wb') as target:
                    target.write(wanted)
                    target.flush()
                    os.fsync(target.fileno())
                os.chmod(temporary, 0o600)
                if control.exists() or control.is_symlink():
                    raise ValueError('reader control appeared before switch')
                os.replace(temporary, control)
                _fsync_dir(state)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        env = {**os.environ, 'STOCKAGENT_PACKED_EDGE_STATE': str(state/'state.json'),
               'STOCKAGENT_PACKED_EDGE_READER_CONTROL': str(control)}
        try:
            result = subprocess.run(['bash', str(wrapper), 'reader-status', dataset], cwd=repo, env=env,
                                    capture_output=True, text=True, timeout=30, check=True)
            status = json.loads(result.stdout)
            if (status.get('mode') != 'index-only-reader-status' or status.get('dataset') != dataset
                    or status.get('schema_version') != 3 or status.get('manifest_sha256') != expected_dataset_sha
                    or status.get('payload_fetched') is not False or status.get('payload_verified') is not False):
                raise ValueError('canonical edge reader status differs')
        except Exception:
            if created:
                control.unlink(missing_ok=True)
                _fsync_dir(state)
            raise
        return {'contract': CONTRACT, 'state': 'active', 'release_root': str(release),
                'manifest_sha256': expected_manifest, 'wrapper_sha256': expected_wrapper,
                'reader_status': status, 'payload_fetched': False}
    finally:
        os.close(fd)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    prepare = commands.add_parser('stage')
    prepare.add_argument('--source', type=Path, required=True)
    prepare.add_argument('--repo', type=Path, required=True)
    prepare.add_argument('--expected-source', required=True)
    switch = commands.add_parser('activate')
    for name in ('repo', 'state', 'release', 'wrapper-source'):
        switch.add_argument('--'+name, type=Path, required=True)
    for name in ('expected-manifest', 'expected-wrapper', 'expected-baseline', 'dataset', 'expected-dataset-sha'):
        switch.add_argument('--'+name, required=True)
    args = parser.parse_args()
    if args.command == 'stage':
        result = stage(args.source, args.repo, args.expected_source)
    else:
        result = activate(args.repo, args.state, args.release, args.expected_manifest, args.wrapper_source,
                          args.expected_wrapper, args.expected_baseline, dataset=args.dataset,
                          expected_dataset_sha=args.expected_dataset_sha)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
