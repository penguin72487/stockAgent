#!/usr/bin/env python3
"""Enroll the fixed 765 copy with the existing source-handoff owner.

This metadata-only step does not move, pack, delete, or read payloads. The
existing owner remains responsible for fencing writers and executing the move.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import project_futures_725_workspace_index as projection
from scripts import run_workspace_source_staging as staging
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync import workspace_symlink_closure as closure
from stockagent.data_sync import workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json


def enroll(*, apply: bool = False) -> dict:
    if type(apply) is not bool:
        raise ValueError('apply must be a boolean')
    with handoff.selected_source(handoff.FUTURES_765_NAME):
        item, workspace, destination, evidence, _ = handoff.enrolled()
        scope = handoff._scope()
        marker = handoff.CONTROL/'futures-765-enrollment.json'
        if marker.exists() or marker.is_symlink():
            handoff._validate_futures_765_enrollment()
            return {'state':'already_enrolled', 'name':scope.name, 'new_payload_readback':False}
        workspace_volume.check()
        if (scope.source.is_symlink() or not scope.source.is_dir()
                or scope.source.resolve(strict=True) != scope.source
                or scope.source.stat().st_dev != handoff.ROOT.stat().st_dev
                or destination.is_symlink() or not destination.is_dir()
                or scope.backup.exists() or scope.backup.is_symlink()
                or handoff.read_journal() is not None
                or handoff.provider_workspace.exact_mount(scope.source, destination)):
            raise SnapshotError('765 source is not the expected unexchanged C generation')
        report = projection.validated_projection(workspace, scope=projection.FUTURES_765)
        if report is None or staging.verified_report(item, workspace) != report:
            raise SnapshotError('765 fixed copy projection is unavailable')
        intent = json.loads((evidence/'intent.json').read_bytes())
        expected = intent['source_root_signature']
        actual = relocation.signature(scope.source)
        rebind = handoff._source_device_rebind(scope.source, expected, actual)
        if not handoff._same_source_generation(actual, expected, rebind):
            raise SnapshotError('765 source root changed since its fixed copy')
        marker_path = Path(item['marker'])
        if (relocation.digest(scope.source/marker_path, no_follow=True) != report['source_marker_sha256']
                or relocation.digest(destination/marker_path, no_follow=True) != report['source_marker_sha256']):
            raise SnapshotError('765 source or E marker differs from its fixed copy')
        with closing(sqlite3.connect(f'file:{evidence}/files.sqlite?mode=ro', uri=True)) as db:
            counts = closure.inspect_links(db, scope.source, destination, source_device_mapping=rebind)
        if counts != {'links':1, 'internal_targets_verified':1, 'unresolved_targets':0}:
            raise SnapshotError('765 symlink closure does not match the reviewed scope')
        if relocation.signature(scope.source) != actual:
            raise SnapshotError('765 source changed during enrollment')
        result = {'state':'ready', 'name':scope.name, 'files':report['files'],
                  'logical_bytes':report['logical_bytes'], 'internal_links':counts['links'],
                  'new_payload_readback':False, 'source_files_deleted':0, 'source_alias_changed':False}
        if apply:
            # The same owner lock serializes enrollment and the next timer run.
            with handoff.lock_file(handoff.CONTROL/'owner.lock'):
                if handoff.read_journal() is not None or marker.exists() or marker.is_symlink():
                    raise SnapshotError('765 enrollment changed during its preflight')
                if relocation.signature(scope.source) != actual:
                    raise SnapshotError('765 source changed before enrollment')
                atomic_write_json(marker, handoff.futures_765_enrollment_value(), durable=True)
                handoff._validate_futures_765_enrollment()
                result['state'] = 'enrolled_for_existing_owner'
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(json.dumps(enroll(apply=args.apply), ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
