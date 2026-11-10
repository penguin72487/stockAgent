#!/usr/bin/env python3
"""Reclaim old, unreferenced developer download caches; never touch user data.

Dry run by default. A renamed server distribution is a recoverable pending
transaction; an interrupted invocation resumes only after process checks.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time

VERSION = re.compile(r'^(?:Stable-|\.stockagent-retired-)([0-9a-f]{40})$')
DEFAULT_ROOT = Path('/root/.vscode-server/cli/servers')
EXTENSION_CACHE_ROOT = Path('/root/.vscode-server/data/CachedExtensionVSIXs')
EXTENSION_PACKAGE = re.compile(r'^[a-z0-9][a-z0-9-]*\.[a-z0-9][a-z0-9-]*-[0-9][A-Za-z0-9.+_-]*$')
PIP_HTTP_CACHE_ROOT = Path('/root/.cache/pip/http-v2')
PIP_CACHE_KEY = re.compile(r'^[0-9a-f]{56}(?:\.body)?$')


def references(root: Path, names: list[str]) -> dict[str, list[int]]:
    """Read process paths and only return PIDs, never command lines or secrets."""
    out = {name: [] for name in names}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            cmd = (proc/'cmdline').read_bytes()
            maps = (proc/'maps').read_bytes()
            paths = []
            for item in (proc/'cwd', proc/'exe'):
                try:
                    paths.append(os.readlink(item))
                except (FileNotFoundError, ProcessLookupError):
                    pass
            try:
                for item in (proc/'fd').iterdir():
                    try:
                        paths.append(os.readlink(item))
                    except (FileNotFoundError, ProcessLookupError):
                        pass
            except (FileNotFoundError, ProcessLookupError):
                pass
            for name in names:
                digest = VERSION.fullmatch(name).group(1)
                if (digest.encode() in cmd or digest.encode() in maps
                        or any(digest in path for path in paths)):
                    out[name].append(int(proc.name))
        except (FileNotFoundError, ProcessLookupError):
            continue  # a finished process cannot keep a reference
        except PermissionError as error:
            raise RuntimeError('cannot inspect all processes; no server may be retired') from error
    return out


def _no_nested_mount(root: Path):
    device = root.stat().st_dev
    for directory, names, _files in os.walk(root, followlinks=False):
        for name in names:
            path = Path(directory)/name
            if not path.is_symlink() and path.stat().st_dev != device:
                raise RuntimeError('server directory contains another mount')


def extension_cache_references(root: Path, names: list[str]) -> dict[str, list[int]]:
    """Inspect exact cache paths without printing command lines or secrets."""
    out = {name: [] for name in names}
    paths = {name: str(root/name) for name in names}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            cmd = (proc/'cmdline').read_bytes()
            maps = (proc/'maps').read_bytes()
            linked = []
            for item in (proc/'cwd', proc/'exe'):
                try:
                    linked.append(os.readlink(item))
                except (FileNotFoundError, ProcessLookupError):
                    pass
            try:
                for item in (proc/'fd').iterdir():
                    try:
                        linked.append(os.readlink(item))
                    except (FileNotFoundError, ProcessLookupError):
                        pass
            except (FileNotFoundError, ProcessLookupError):
                pass
            for name, path in paths.items():
                if (path.encode() in cmd or path.encode() in maps or path in linked
                        or str(root) in linked):
                    out[name].append(int(proc.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError as error:
            raise RuntimeError('cannot inspect all processes; no extension cache may be retired') from error
    return out


def reclaim_extension_cache(root: Path, *, apply=False, minimum_age_seconds=72*3600,
                            max_files=16, now=time.time, process_references=extension_cache_references):
    """Remove only old, unreferenced downloaded VSIX files, not extensions."""
    root = Path(root).absolute()
    if (root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root
            or minimum_age_seconds < 72*3600 or not 1 <= max_files <= 16):
        raise ValueError('unsafe extension cache scope or age')
    fd = os.open(root/'.stockagent-cleanup.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    results = []
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        candidates = []
        for path in root.iterdir():
            if not EXTENSION_PACKAGE.fullmatch(path.name) or path.is_symlink() or not path.is_file():
                continue
            info = path.stat()
            if (info.st_uid == os.geteuid() and info.st_dev == root.stat().st_dev and info.st_nlink == 1
                    and now()-info.st_mtime >= minimum_age_seconds):
                candidates.append((info.st_mtime, path.name, info))
        for _mtime, name, original in sorted(candidates)[:max_files]:
            path = root/name
            refs = process_references(root, [name])[name]
            if refs:
                results.append({'package':name, 'state':'in_use', 'reference_count':len(refs)})
                continue
            if not apply:
                results.append({'package':name, 'state':'eligible', 'allocated_bytes':original.st_blocks*512})
                continue
            current = path.lstat()
            fields = ('st_dev', 'st_ino', 'st_mode', 'st_uid', 'st_nlink', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
            if (not all(getattr(current, field) == getattr(original, field) for field in fields)
                    or now()-current.st_mtime < minimum_age_seconds):
                raise RuntimeError('extension cache file changed before retirement')
            path.unlink()
            results.append({'package':name, 'state':'removed', 'allocated_bytes':current.st_blocks*512})
        return {'contract':'stockagent.unused_vscode_extension_cache_reclaim.v1', 'apply':apply,
                'minimum_age_seconds':minimum_age_seconds, 'results':results}
    finally:
        os.close(fd)


def pip_processes() -> list[int]:
    """Fail closed if a pip process is active; do not expose its command."""
    active = []
    for proc in Path('/proc').iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            argv = (proc/'cmdline').read_bytes().split(b'\0')
            executable = Path(os.fsdecode(argv[0])).name if argv and argv[0] else ''
            if (re.fullmatch(r'pip(?:[0-9]+(?:\.[0-9]+)?)?', executable)
                    or any(argv[i:i+2] == [b'-m', b'pip'] for i in range(len(argv)-1))):
                active.append(int(proc.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError as error:
            raise RuntimeError('cannot inspect all processes; no pip cache may be retired') from error
    return active


def reclaim_pip_http_cache(root: Path, *, apply=False, minimum_age_seconds=72*3600,
                           max_files=512, now=time.time, process_references=extension_cache_references,
                           running_pip=pip_processes):
    """Discard only complete, old pip HTTP cache pairs; never installed packages."""
    root = Path(root).absolute()
    if (root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root
            or minimum_age_seconds < 72*3600 or not 1 <= max_files <= 512):
        raise ValueError('unsafe pip HTTP cache scope or age')
    fd = os.open(root/'.stockagent-cleanup.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if running_pip():
            return {'contract':'stockagent.unused_pip_http_cache_reclaim.v1', 'state':'in_use', 'apply':apply}
        groups = {}
        device = root.stat().st_dev
        def on_error(error):
            raise error
        for current, dirs, names in os.walk(root, followlinks=False, onerror=on_error):
            directory = Path(current)
            dirs[:] = [name for name in dirs if not (directory/name).is_symlink()
                       and (directory/name).stat().st_dev == device]
            for name in names:
                if not PIP_CACHE_KEY.fullmatch(name):
                    continue
                path = directory/name
                info = path.lstat()
                if not path.is_file() or path.is_symlink():
                    continue
                key = (directory, name.removesuffix('.body'))
                groups.setdefault(key, []).append((path, info))
        candidates = []
        for (directory, key), members in groups.items():
            existing = {directory/name for name in (key, key+'.body') if os.path.lexists(directory/name)}
            if (existing == {path for path,_ in members}
                    and all(info.st_uid == os.geteuid() and info.st_dev == device and info.st_nlink == 1
                   and now()-info.st_mtime >= minimum_age_seconds for _, info in members)):
                candidates.append(members)
        candidates.sort(key=lambda members: min(info.st_mtime for _,info in members))
        bounded, count = [], 0
        for members in candidates:
            if count + len(members) > max_files:
                break
            bounded.append(members); count += len(members)
        names = [path.relative_to(root).as_posix() for members in bounded for path,_ in members]
        refs = process_references(root, names)
        if running_pip():
            return {'contract':'stockagent.unused_pip_http_cache_reclaim.v1', 'state':'in_use', 'apply':apply}
        removed = eligible = allocated = 0
        fields = ('st_dev', 'st_ino', 'st_mode', 'st_uid', 'st_nlink', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        for members in bounded:
            if any(refs[path.relative_to(root).as_posix()] for path,_ in members):
                continue
            try:
                if any(path.is_symlink() or path.parent.resolve(strict=True) != path.parent
                       or any(getattr(path.lstat(), f) != getattr(info, f) for f in fields)
                       or now()-info.st_mtime < minimum_age_seconds for path,info in members):
                    continue
            except FileNotFoundError:
                continue
            eligible += len(members)
            if apply:
                for path,info in members:
                    path.unlink(); removed += 1; allocated += info.st_blocks*512
        return {'contract':'stockagent.unused_pip_http_cache_reclaim.v1', 'state':'checked', 'apply':apply,
                'eligible_files':eligible, 'removed_files':removed, 'removed_allocated_bytes':allocated}
    finally:
        os.close(fd)


def reclaim_under_pressure(*, disk=Path('/'), probe=os.statvfs,
                           extension_cleanup=reclaim_extension_cache,
                           pip_cleanup=reclaim_pip_http_cache):
    """One bounded maintenance pass when the remote disk approaches 1%."""
    before = probe(disk)
    total, available = before.f_blocks * before.f_frsize, before.f_bavail * before.f_frsize
    if total <= 0 or available < 0:
        raise ValueError('invalid remote disk observation')
    threshold = total // 80  # Begin at 1.25%; never change the actual Syncthing reserve.
    result = {'contract':'stockagent.remote_cache_pressure_reclaim.v1', 'state':'not_needed',
              'available_before_bytes':available, 'threshold_bytes':threshold,
              'removed_allocated_bytes':0, 'profile_issues':0, 'profiles':[]}
    if available >= threshold:
        return result
    for label, root, cleanup in (('vscode_extension_downloads', EXTENSION_CACHE_ROOT, extension_cleanup),
                                  ('pip_http_cache', PIP_HTTP_CACHE_ROOT, pip_cleanup)):
        if not root.exists() or root.is_symlink():
            result['profiles'].append({'profile':label, 'state':'unavailable'})
            continue
        try:
            value = cleanup(root, apply=True)
            removed = (value.get('removed_allocated_bytes', 0) if label == 'pip_http_cache' else
                       sum(row.get('allocated_bytes', 0) for row in value.get('results', [])
                           if row.get('state') == 'removed'))
            result['removed_allocated_bytes'] += removed
            result['profiles'].append({'profile':label, 'state':value.get('state', 'checked'),
                                       'removed_allocated_bytes':removed})
        except (OSError, ValueError, RuntimeError) as error:
            result['profiles'].append({'profile':label, 'state':'waiting', 'error_type':type(error).__name__})
    after = probe(disk)
    issues = sum(row.get('state') in {'waiting','unavailable','in_use'} for row in result['profiles'])
    result.update(state='partial' if issues else 'checked', profile_issues=issues,
                  available_after_bytes=after.f_bavail * after.f_frsize)
    return result


def _marker(root: Path, digest: str, info, *, create=False):
    path = root/('.stockagent-retired-'+digest+'.json')
    expected = {'contract':'stockagent.unused_vscode_server_reclaim.v1.transaction',
                'version':digest, 'device':info.st_dev, 'inode':info.st_ino}
    if os.path.lexists(path):
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 4096:
            raise RuntimeError('server retirement marker is redirected or oversized')
        if json.loads(path.read_text()) != expected:
            raise RuntimeError('server retirement marker identifies another generation')
    elif create:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(expected, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
    else:
        raise RuntimeError('server retirement marker is missing')
    return path


def reclaim(root: Path, *, apply=False, minimum_age_seconds=72*3600, max_versions=4,
            now=time.time, process_references=references):
    root = Path(root).absolute()
    if (root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root
            or minimum_age_seconds < 72*3600 or not 1 <= max_versions <= 4):
        raise ValueError('unsafe VS Code server cleanup scope or age')
    lock_path = root/'.stockagent-cleanup.lock'
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    results = []
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        candidates = []
        for path in root.iterdir():
            if not VERSION.fullmatch(path.name) or path.is_symlink() or not path.is_dir():
                continue
            info = path.stat()
            if info.st_uid != os.geteuid() or info.st_dev != root.stat().st_dev:
                continue
            if path.name.startswith('.stockagent-retired-'):
                _marker(root, VERSION.fullmatch(path.name).group(1), info)
                candidates.append((info.st_mtime, path.name, info.st_ino))
            elif now()-info.st_mtime >= minimum_age_seconds:
                candidates.append((info.st_mtime, path.name, info.st_ino))
        candidates.sort()
        for _mtime, name, expected_inode in candidates[:max_versions]:
            source = root/name
            digest = VERSION.fullmatch(name).group(1)
            refs = process_references(root, [name])[name]
            if refs:
                results.append({'version':digest, 'state':'in_use', 'reference_count':len(refs)})
                continue
            if not apply:
                results.append({'version':digest, 'state':'eligible'})
                continue
            info = source.stat()
            if (source.is_symlink() or not source.is_dir() or info.st_ino != expected_inode
                    or not name.startswith('.stockagent-retired-')
                    and now()-info.st_mtime < minimum_age_seconds):
                raise RuntimeError('candidate generation changed before retirement')
            _no_nested_mount(source)
            retired = root/('.stockagent-retired-'+digest)
            marker = _marker(root, digest, info, create=True)
            if source != retired:
                if os.path.lexists(retired):
                    raise RuntimeError('another retirement already owns this server generation')
                os.rename(source, retired)
            # A connection that appeared at the rename boundary must remain
            # able to finish; restore its original name if there is no collision.
            try:
                refs = process_references(root, [name, retired.name])
            except BaseException:
                if source != retired and not os.path.lexists(source):
                    os.rename(retired, source)
                raise
            if any(refs.values()):
                if source != retired and not os.path.lexists(source):
                    os.rename(retired, source)
                results.append({'version':digest, 'state':'in_use_after_rename'})
                continue
            _no_nested_mount(retired)
            shutil.rmtree(retired)
            marker.unlink()
            results.append({'version':digest, 'state':'removed'})
        return {'contract':'stockagent.unused_vscode_server_reclaim.v1', 'apply':apply,
                'minimum_age_seconds':minimum_age_seconds, 'results':results}
    finally:
        os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--root', type=Path)
    parser.add_argument('--extension-cache', action='store_true',
                        help='only old downloaded VSIX cache files; installed extensions are untouched')
    parser.add_argument('--pip-http-cache', action='store_true',
                        help='only old pip HTTP cache files; installed packages are untouched')
    args = parser.parse_args()
    try:
        if args.extension_cache and args.pip_http_cache:
            raise ValueError('choose one cache type')
        result = (reclaim_extension_cache(args.root or EXTENSION_CACHE_ROOT, apply=args.apply)
                  if args.extension_cache else
                  reclaim_pip_http_cache(args.root or PIP_HTTP_CACHE_ROOT, apply=args.apply)
                  if args.pip_http_cache else reclaim(args.root or DEFAULT_ROOT, apply=args.apply))
        print(json.dumps(result, sort_keys=True))
    except (OSError, ValueError, RuntimeError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
