#!/usr/bin/env python3
"""Small stdlib-only mount bridge for an already authorized provider handoff.

The parent supplies pinned directory FDs and a verified placement request. Old
service namespaces can have frozen Python packages and no E pathname; importing
new project code there would either fail or modify their code-release boundary.
This helper changes only the enrolled data aliases, never service code mounts.
"""
import json
import os
import ctypes
from pathlib import Path
import stat
import sys


def identity(path):
    info = os.stat(path)
    return [info.st_dev, info.st_ino]


_PROC_FD = None


def mounts():
    fd = os.open('self/mountinfo', os.O_RDONLY, dir_fd=_PROC_FD) if _PROC_FD is not None else os.open('/proc/self/mountinfo', os.O_RDONLY)
    with os.fdopen(fd, 'rb') as source:
        raw = source.read(1024**2+1)
    if len(raw) > 1024**2:
        raise RuntimeError('namespace mount census exceeds bound')
    return {line.split()[4].decode() for line in raw.splitlines()}


def attach(target, spec, *, placeholder=None):
    fd = spec['fd']
    info = os.fstat(fd)
    if type(fd) is not int or not stat.S_ISDIR(info.st_mode) or [info.st_dev, info.st_ino] != spec['identity']:
        raise RuntimeError('pinned source directory identity changed')
    if target.resolve(strict=True) != target or not target.is_dir():
        raise RuntimeError('provider alias is absent or redirected')
    mounted = str(target) in mounts()
    if not (mounted and identity(target) == spec['identity']):
        if mounted or any(target.iterdir()) or (placeholder is not None and identity(target) != placeholder):
            raise RuntimeError('refuse to hide an occupied/different provider alias')
        if any(name.startswith(str(target)+'/') for name in mounts()):
            raise RuntimeError('provider placeholder has an unexpected submount')
        # This is a DETACHED mount FD prepared by the canonical transport API.
        # Ordinary MS_BIND on an outside-namespace directory FD returns EINVAL.
        # Retain the target namespace's read-only/security restrictions.
        restrictions = os.statvfs(target).f_flag & 0xF
        if restrictions:
            attrs = MountAttributes(restrictions, 0, 0, 0)
            mount_api('mount_setattr', (fd, b'', 0x1000, ctypes.byref(attrs), ctypes.sizeof(attrs)),
                      (ctypes.c_int, ctypes.c_char_p, ctypes.c_uint,
                       ctypes.POINTER(MountAttributes), ctypes.c_size_t))
        try:
            attach_tree(fd, target)
        except OSError as error:
            # Include the precise alias; the errno alone cannot distinguish a
            # vanished namespace target from an unavailable detached source.
            raise OSError(error.errno, f'provider alias attach failed: {target}') from error
    if spec.get('readonly'):
        if not os.statvfs(target).f_flag & os.ST_RDONLY:
            raise RuntimeError('retained unused subtree is not read-only')
    if str(target) not in mounts() or identity(target) != spec['identity']:
        raise RuntimeError('namespace data alias readback differs')
    return {'target': str(target), 'identity': identity(target),
            'readonly': bool(os.statvfs(target).f_flag & os.ST_RDONLY)}


def run(request):
    global _PROC_FD
    _PROC_FD = request.get('proc_fd')
    root = Path(request['repository_root'])
    if (not root.is_absolute() or root.resolve(strict=True) != root
            or identity(root) != request['repository_identity']
            or request['provider'] not in {'finlab', 'finmind', 'tej'}):
        raise RuntimeError('namespace repository/provider identity differs')
    namespace = (os.stat('self/ns/mnt', dir_fd=_PROC_FD) if _PROC_FD is not None else os.stat('/proc/self/ns/mnt')).st_ino
    if request.get('namespace_inode') is not None and request['namespace_inode'] != namespace:
        raise RuntimeError('pinned namespace identity changed')
    target = root/('data_'+request['provider'])
    readbacks = [attach(target, request['root'], placeholder=request['placeholder_identity'])]
    for item in request['children']:
        rel = Path(item['relative'])
        if not rel.parts or rel.is_absolute() or '..' in rel.parts or str(rel) != item['relative']:
            raise RuntimeError('external provider subtree leaves its root')
        readbacks.append(attach(target/rel, item))
    return {'namespace_inode': namespace, 'provider': request['provider'], 'verified': True,
            'readbacks': readbacks}


def enter(request):
    """Load Python in the authority first, then enter even a masked namespace."""
    libc = ctypes.CDLL(None, use_errno=True)
    setns = libc.setns
    setns.argtypes, setns.restype = (ctypes.c_int, ctypes.c_int), ctypes.c_int
    if setns(request['namespace_fd'], 0) != 0:
        raise OSError(ctypes.get_errno(), 'provider namespace setns failed')
    os.fchdir(request['namespace_root_fd'])
    os.chroot('.')
    os.chdir('/')
    if request.get('isolated_probe'):
        unshare = libc.unshare
        unshare.argtypes, unshare.restype = (ctypes.c_int,), ctypes.c_int
        if unshare(0x00020000) != 0:  # CLONE_NEWNS; never mutate the original service in a probe.
            raise OSError(ctypes.get_errno(), 'isolated mount probe failed')


if __name__ == '__main__':
    raw = sys.stdin.buffer.read(32769)
    if len(raw) > 32768:
        raise RuntimeError('namespace placement request exceeds bound')
    request = json.loads(raw)
    if 'namespace_fd' in request:
        enter(request)
    print(json.dumps(run(request)))
