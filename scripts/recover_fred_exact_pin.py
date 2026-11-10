#!/usr/bin/env python3
"""Resume the exact FRED pin repair using the existing C/E/D owners."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stockagent.data_sync import fred_exact_pin_recovery as repair, retained_source, workspace_volume  # noqa: E402
from stockagent.data_sync.cold_primary import _check_d_primary_mount  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json  # noqa: E402


def main() -> int:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = Path(config['cold_root'])/'.local-state/private-history'
    scratch = workspace_volume.path('migration', create=True)/'fred-exact-pin-recovery-v1'
    scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    def guard():
        workspace_volume.check(c=config)
        _check_d_primary_mount(Path(config['cold_root']))
    try:
        result = repair.recover(ROOT/'data_fred_crypto_macro', cold, retained_source.DEFAULT_PIN_ROOT, scratch,
                                guard=guard, admit=lambda amount: workspace_volume.admit(amount, c=config))
    except (OSError, ValueError, SnapshotError) as error:
        # Preserve any fixed pin and installed immutable objects for the next
        # retry. No exception path grants deletion or claims completion.
        if scratch.is_dir() and not scratch.is_symlink() and scratch.resolve(strict=True) == scratch:
            atomic_write_json(scratch/'last-error.json', {'contract': repair.CONTRACT,
                'state': 'waiting_retry', 'error_type': type(error).__name__,
                'observed_at_epoch': time.time(),
                'current_head_changed': False, 'source_deleted': False}, durable=True)
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        return 75
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
