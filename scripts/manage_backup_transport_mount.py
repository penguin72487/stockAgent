#!/usr/bin/env python3
"""Mount only the enrolled E transport; never create a C fallback."""
import argparse
from pathlib import Path
import subprocess

from stockagent.data_sync.transport_placement import CANONICAL, backing, check_transport, placement
from stockagent.data_sync.workspace_volume import check


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=("--check", "--mount", "--unmount"))
    # Preserve the shell entrypoint's existing --command ABI.
    import sys
    command = sys.argv[1] if len(sys.argv) == 2 else ""
    if command not in ("--check", "--mount", "--unmount"):
        p.error("expected --check, --mount or --unmount")
    root = backing()
    check(physical=True)
    if (root / ".transport-owner.json").read_bytes() != placement()[1]:
        raise ValueError("E transport has not completed its fixed migration")
    if command == "--mount" and not CANONICAL.is_mount():
        if CANONICAL.is_symlink() or not CANONICAL.is_dir():
            raise ValueError("invalid original sender mountpoint")
        # The old installer retained a verified C copy underneath the alias.
        # Never delete or use that hidden copy as an automatic fallback.
        subprocess.run(["mount", "--bind", str(root), str(CANONICAL)], check=True)
    if CANONICAL.is_mount():
        check_transport(physical=True)
        if command == "--unmount":
            subprocess.run(["umount", str(CANONICAL)], check=True)
    elif command != "--unmount":
        raise ValueError("E transport is not mounted")


if __name__ == "__main__":
    main()
