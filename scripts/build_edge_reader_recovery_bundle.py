#!/usr/bin/env python3
"""Freeze the enrolled edge reader and dependency on the verified E workspace."""
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync import edge_reader_recovery as recovery, workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError


def main() -> None:
    configuration = workspace_volume.configuration()
    workspace_volume.check(c=configuration)
    value = recovery.policy(ROOT/'configs/data_sync/edge_reader_recovery_v3.json',
                            ROOT/'configs/data_sync/edge_reader_activation.json')
    if value['enabled'] is not True:
        raise SnapshotError('edge reader recovery is disabled')
    result = recovery.build(ROOT, Path(configuration['mount_point']), recovery.default_site(), value)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
