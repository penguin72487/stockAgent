#!/usr/bin/env python3
"""Resume the original owner's exact private SQLite staging handoff to E."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time

from stockagent.data_sync.migration_scratch_relocation import relocate, relocate_controls
from stockagent.data_sync.desync_snapshots import atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--operation', required=True, type=Path)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--source-relative')
    parser.add_argument('--snapshot-sha256')
    parser.add_argument('--controls-sha256', help='fixed identity of an owned frozen control set')
    args = parser.parse_args()
    if bool(args.controls_sha256) == bool(args.source_relative or args.snapshot_sha256):
        parser.error('supply either --controls-sha256 or both SQLite identity arguments')
    if not args.controls_sha256 and not (args.source_relative and args.snapshot_sha256):
        parser.error('SQLite relocation requires --source-relative and --snapshot-sha256')
    try:
        result = (relocate_controls(args.operation, args.dataset, args.controls_sha256)
                  if args.controls_sha256 else
                  relocate(args.operation, args.dataset, args.source_relative, args.snapshot_sha256))
    except Exception as error:
        # The public monitor projects only the allowlisted reason, never this
        # private operation's paths, source contents or exception message.
        key_material = ('controls\0' + args.dataset if args.controls_sha256 else
                        args.dataset + '\0' + args.source_relative)
        key = hashlib.sha256(key_material.encode()).hexdigest()[:24]
        evidence = args.operation / 'workspace-relocation' / key
        evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
        message = str(error).lower()
        reason = ('scratch_owner_busy' if 'still active' in message else
                  'scratch_capacity' if 'headroom' in message or 'space' in message else
                  'scratch_reference' if 'reference' in message or 'in use' in message else
                  'scratch_error')
        atomic_write_json(evidence / 'attempt.json', {'state': 'failed', 'reason_code': reason,
            'error_type': type(error).__name__, 'observed_at_epoch': time.time(),
            'source_files_deleted': 0})
        raise
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0 if result['state'] == 'completed' else 75


if __name__ == '__main__':
    raise SystemExit(main())
