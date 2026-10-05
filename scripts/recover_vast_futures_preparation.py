#!/usr/bin/env python3
"""Narrow accidental-deletion rescue; reuse exact original cold reconstruction.

The received D carrier is preservation evidence, not canonical publication or
training readiness. Restore to a NEW private native-C path, retain all cold
bytes, and separately verify/promote on Vast. Never overwrite a live tree.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.bulk_archive import native_guard, restore_original_root, root_records
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

BATCH = Path('/srv/stockagent-d-volume/stockagent-cold-primary/remote-artifact-incoming/20261004T102032-b371004176fb')
RELATIVE_ROOT = 'markets/tw_futures_v8_margin_preparation'
WORK = Path('/var/lib/stockagent-futures-recovery/20261004')
DESTINATION = WORK / 'received-preparation'
RECEIPTS = ROOT / 'artifacts/operations/futures_preparation_recovery_20261004'


def load_originals():
    native_guard()
    receipt = json.loads((BATCH / 'markets.receipt.json').read_text())
    index = json.loads((BATCH / 'markets.original-index.json').read_text())
    if (receipt['scope'] != 'markets' or not receipt.get('completed_at_epoch')
            or receipt['compressed_sha256'] != index['compressed_sha256']):
        raise SnapshotError('received original inventory identity differs')
    record = next(r for r in root_records(index) if r['relative_root'] == RELATIVE_ROOT)
    if not record['directory_root'] or record['unsupported']:
        raise SnapshotError('received futures root is not fully recoverable')
    return receipt, index, record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    receipt, index, record = load_originals()
    plan = {'state': 'explicit_received_carrier_restore_plan', 'relative_root': RELATIVE_ROOT,
            'files': sum(r['kind'] == 'file' for r in record['rows']),
            'logical_bytes': record['logical_bytes'], 'destination': str(DESTINATION),
            'compressed_sha256': index['compressed_sha256'], 'cold_deleted': False,
            'canonical_publication_claim': False, 'training_readiness_claim': False}
    atomic_write_json(RECEIPTS / 'received-carrier-restore-plan.json', plan)
    print(json.dumps(plan), flush=True)
    if not args.apply:
        return
    WORK.mkdir(parents=True, mode=0o700, exist_ok=True)
    if DESTINATION.exists() or DESTINATION.is_symlink():
        raise SnapshotError('private destination already exists; preserve prior recovery')
    expected = {'compressed_sha256': index['compressed_sha256'], 'relative_root': RELATIVE_ROOT,
                'rows': record['rows']}
    atomic_write_json(WORK / 'expected-originals.json', expected)
    result = restore_original_root(Path(receipt['payload']), index, RELATIVE_ROOT, DESTINATION)
    atomic_write_json(RECEIPTS / 'received-carrier-restored.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
