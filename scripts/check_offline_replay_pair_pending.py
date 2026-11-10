#!/usr/bin/env python3
"""Cheap timer hint: start the migration owner only if an enrolled item needs work."""
import json
import os
from pathlib import Path
import stat
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.offline_replay_policy import ENROLLED, completed


def needs_work(queue: Path) -> bool:
    for dataset in ENROLLED:
        path = queue/dataset/'status.json'
        try:
            if any(part.is_symlink() for part in (path, *path.parents)):
                return True
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            with os.fdopen(descriptor, 'rb') as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > 128*1024:
                    return True
                value = json.load(stream)
            if not completed(value, dataset):
                return True
        except (OSError, ValueError, TypeError):
            return True
    return False


if __name__ == '__main__':
    root = Path('/srv/stockagent-workspace/migration/offline-replay-pairs-v1')
    # systemd treats 1 as a skipped condition and retries the timer later.
    raise SystemExit(0 if needs_work(root) else 1)
