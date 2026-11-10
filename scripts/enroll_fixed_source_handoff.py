#!/usr/bin/env python3
"""Enroll reviewed fixed copies for the existing source-handoff owner.

This step inspects only control metadata. The original owner fences writers,
checks the fixed source generation, switches the alias and retains C until the
separate source-hashed E archive is ready.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_workspace_source_staging as staging
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync import workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json


def enroll(name: str = handoff.YAHOO_NAME, *, apply: bool = False) -> dict:
    if type(apply) is not bool:
        raise ValueError('apply must be a boolean')
    if name == handoff.YAHOO_NAME:
        marker_name = 'yahoo-enrollment.json'
        binding_value, validate = handoff.yahoo_enrollment_value, handoff._validate_yahoo_enrollment
    elif name == handoff.PEPPERSTONE_NAME:
        marker_name = 'pepperstone-enrollment.json'
        binding_value, validate = handoff.pepperstone_enrollment_value, handoff._validate_pepperstone_enrollment
    elif name == handoff.MINUTE_RAW_NAME:
        marker_name = 'minute-raw-enrollment.json'
        binding_value, validate = handoff.minute_raw_enrollment_value, handoff._validate_minute_raw_enrollment
    elif name == handoff.BYBIT_NAME:
        marker_name = 'bybit-enrollment.json'
        binding_value, validate = handoff.bybit_enrollment_value, handoff._validate_bybit_enrollment
    elif name in handoff.RETAINED_LOCK_SOURCES:
        marker_name = handoff.RETAINED_LOCK_SOURCES[name]['label']+'-enrollment.json'
        binding_value = lambda: handoff.retained_enrollment_value(name)
        validate = lambda: handoff._validate_retained_enrollment(name)
    else:
        raise SnapshotError('source enrollment is not reviewed')
    with handoff.selected_source(name):
        item, workspace, destination, evidence, _ = handoff.enrolled()
        scope = handoff._scope()
        marker = handoff.CONTROL/marker_name
        if marker.exists() or marker.is_symlink():
            validate()
            return {'state':'already_enrolled', 'name':scope.name, 'new_payload_readback':False}
        workspace_volume.check()
        if (scope.source.is_symlink() or not scope.source.is_dir()
                or scope.source.resolve(strict=True) != scope.source
                or scope.source.stat().st_dev != handoff.ROOT.stat().st_dev
                or destination.is_symlink() or not destination.is_dir()
                or scope.backup.exists() or scope.backup.is_symlink()
                or handoff.read_journal() is not None
                or handoff.provider_workspace.exact_mount(scope.source, destination)):
            raise SnapshotError('source is not the expected unexchanged C generation')
        report = staging.verified_report(item, workspace)
        if report is None:
            raise SnapshotError('fixed E copy is unavailable')
        if name == handoff.BYBIT_NAME:
            staging.catalog_entries(item)  # Exact legacy alias must still resolve to the C source.
        intent = json.loads((evidence/'intent.json').read_bytes())
        recorded, actual = intent.get('source_root_signature'), relocation.signature(scope.source)
        rebind = handoff._source_device_rebind(scope.source, recorded, actual)
        if (not isinstance(recorded, list) or len(recorded) != 9
                or not (actual[0] == recorded[0] or rebind is not None)
                or any(actual[i] != recorded[i] for i in (1, 2, 6, 7, 8))):
            raise SnapshotError('source root identity changed since its fixed copy')
        index = evidence/'files.sqlite'
        wal = evidence/'files.sqlite-wal'
        if index.is_symlink() or not index.is_file() or (wal.exists() and wal.stat().st_size):
            raise SnapshotError('fixed source index is unavailable or not checkpointed')
        with sqlite3.connect(f'file:{index}?mode=ro', uri=True) as db:
            symlinks = db.execute("SELECT count(*) FROM files WHERE kind='symlink'").fetchone()[0]
        if symlinks:
            raise SnapshotError('fixed copy has unreviewed symbolic links')
        if name in handoff.RETAINED_LOCK_SOURCES:
            with ExitStack() as locks:
                spec = handoff.RETAINED_LOCK_SOURCES[name]
                for relative in spec.get('native_locks', ('state/collector.lock',)):
                    for root in (scope.source, destination):
                        locks.enter_context(handoff.lock_file(root/relative))
                for relative in spec.get('external_native_locks', ()):
                    locks.enter_context(handoff.lock_file(handoff.ROOT/relative))
        binding = binding_value()
        result = {'state':'ready', 'name':scope.name, 'files':report['files'],
                  'logical_bytes':report['logical_bytes'], 'new_payload_readback':False,
                  'source_files_deleted':0, 'source_alias_changed':False}
        if apply:
            with handoff.lock_file(handoff.CONTROL/'owner.lock'):
                if handoff.read_journal() is not None or marker.exists() or marker.is_symlink():
                    raise SnapshotError('source enrollment changed during its preflight')
                if relocation.signature(scope.source) != actual or binding != binding_value():
                    raise SnapshotError('source or writer code changed before enrollment')
                atomic_write_json(marker, binding, durable=True)
                validate()
                result['state'] = 'enrolled_for_existing_owner'
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--source', choices=(handoff.YAHOO_NAME, handoff.PEPPERSTONE_NAME,
                                             handoff.MINUTE_RAW_NAME, handoff.BYBIT_NAME, *handoff.RETAINED_LOCK_SOURCES),
                        default=handoff.YAHOO_NAME)
    args = parser.parse_args()
    print(json.dumps(enroll(args.source, apply=args.apply), ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
