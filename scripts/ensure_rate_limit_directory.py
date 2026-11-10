#!/usr/bin/env python3
"""Create the shared per-user limiter directory before a sandboxed unit starts."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import pwd
import stat


def ensure(parent: Path, uid: int, gid: int) -> Path:
    """Use directory descriptors so an untrusted /tmp link is never followed."""
    if type(uid) is not int or type(gid) is not int or uid < 0 or gid < 0:
        raise ValueError('invalid rate-limit directory owner')
    name = f'stockagent-rate-limits-{uid}'
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    child_fd = None
    try:
        created = False
        try:
            os.mkdir(name, 0o700, dir_fd=parent_fd)
            created = True
        except FileExistsError:
            pass
        child_fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                           dir_fd=parent_fd)
        if created:
            os.fchown(child_fd, uid, gid)
        opened = os.fstat(child_fd)
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (not stat.S_ISDIR(opened.st_mode) or not os.path.samestat(opened, named)
                or opened.st_uid != uid or opened.st_gid != gid):
            raise ValueError('rate-limit directory owner or identity differs')
        # Limiters are shared by processes of the same UID only. Restrict a
        # pre-existing permissive directory without replacing its inode or
        # disturbing live rate-limit state files.
        if stat.S_IMODE(opened.st_mode) != 0o700:
            os.fchmod(child_fd, 0o700)
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not os.path.samestat(os.fstat(child_fd), named):
            raise ValueError('rate-limit directory changed during preparation')
        return parent/name
    finally:
        if child_fd is not None:
            os.close(child_fd)
        os.close(parent_fd)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--uid', type=int, required=True)
    args = parser.parse_args()
    account = pwd.getpwuid(args.uid)
    print(ensure(Path('/tmp'), account.pw_uid, account.pw_gid))


if __name__ == '__main__':
    main()
