#!/usr/bin/env python3
"""Commit one sealed private handoff dataset from E to its existing D owner."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import prepare_handoff_source_repack as handoff_cli
from scripts.run_stream_packed_repack import D_INSTALL_RESERVE, remaining_install_bytes
from stockagent.data_sync import packed_snapshots as packed, stream_repack, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

COLD = Path('/srv/stockagent-packed')


def publish(dataset: str):
    if dataset not in handoff_cli.ENROLLED.values():
        raise SnapshotError('private handoff dataset is not enrolled')
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    _check_d_primary_mount(COLD)
    root = workspace_volume.path('migration', create=True)/'handoff-source-repack-v1'
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('private handoff stage root redirected')
    status_path = root/(dataset+'.status.json')
    if status_path.is_symlink() or not status_path.is_file():
        raise SnapshotError('private handoff retirement receipt missing')
    value = json.loads(status_path.read_text())
    result = value.get('C_retirement')
    if (value.get('contract') != handoff_cli.CONTRACT or value.get('dataset') != dataset
            or value.get('state') != 'C_payload_retired_root_and_lock_preserved'
            or value.get('C_payload_deleted') is not True or not isinstance(result, dict)
            or result.get('state') != value['state']
            or result.get('prepared_sha256') != value.get('prepared_sha256')):
        raise SnapshotError('private handoff retirement receipt differs')
    expected = packed._validate_hash(value.get('original_manifest_sha256'), 'original private manifest SHA')
    stage = root/dataset/expected
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('private handoff E stage redirected')
    private = private_history_root(COLD, version_policy(ROOT), initialize=False)

    def guard():
        workspace_volume.check(c=config)
        _check_d_primary_mount(COLD)
        if private_history_root(COLD, version_policy(ROOT), initialize=False) != private:
            raise SnapshotError('private D authority changed')

    def save(**changes):
        value.update(changes, observed_at_epoch=time.time())
        atomic_write_json(status_path, value, durable=True)

    if value.get('D_new_head_committed') is True:
        receipt = value.get('D_publication')
        if (not isinstance(receipt, dict) or receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
                or receipt.get('dataset') != dataset or receipt.get('old_manifest_sha256') != expected):
            raise SnapshotError('private D publication receipt differs')
    else:
        guard()
        old = packed.resolve_latest_packed(private, dataset, require_objects=False)
        journal_path = stage/'install.json'
        journal = json.loads(journal_path.read_text()) if journal_path.is_file() and not journal_path.is_symlink() else {}
        if old.manifest_sha256 == expected:
            node = packed.validate_slug(old.manifest['publisher']['node_id'], 'publisher node')
        elif (journal.get('contract') == stream_repack.CONTRACT+'.install'
              and journal.get('new_manifest_sha256') == old.manifest_sha256
              and journal.get('old_manifest_sha256') == expected):
            node = packed.validate_slug(old.manifest['publisher']['node_id'], 'publisher node')
        else:
            raise SnapshotError('private D head changed before this publication')
        prepared = stream_repack._read_prepared(stage, dataset, expected)
        if prepared.get('identity_sha256') != value.get('prepared_sha256'):
            raise SnapshotError('private E stage differs from retirement receipt')

        last_report = 0.0
        def report(event):
            nonlocal last_report
            if event.get('stage') == 'validating_D_inventory' and time.monotonic()-last_report < 3:
                return
            last_report = time.monotonic()
            save(D_state='installing', D_progress=event, D_new_head_committed=False)

        def admit_install(prepared_record, install_journal):
            remaining = remaining_install_bytes(private, prepared_record, install_journal)
            available = shutil.disk_usage(COLD).free
            if available < remaining+D_INSTALL_RESERVE:
                raise SnapshotError('D free space is below the remaining private install and reserve')

        def admit_next(item):
            if shutil.disk_usage(COLD).free < item['bytes']+D_INSTALL_RESERVE:
                raise SnapshotError('D reserve reached during private install')

        published = stream_repack.repack(private, stage, dataset, expected, node_id=node,
            guard=guard, progress=report, admit_install=admit_install, admit_next=admit_next,
            install_lock=root.parent/'cold-install-write-owner.lock')
        receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': dataset,
            'old_manifest_sha256': expected, 'new_snapshot_id': published['snapshot_id'],
            'new_manifest_sha256': published['new_manifest_sha256'], 'files': published['files'],
            'logical_bytes': published['logical_bytes'], 'new_object_count': published['objects'],
            'source_mode': 'retained_handoff_C_original', 'private': True,
            'old_release_removed': False, 'independent_D_readback': False, 'NAS_accepted': False,
            'observed_at_epoch': time.time()}
        save(D_new_head_committed=True, D_state='head_committed', D_publication=receipt)
    if value.get('D_E_stage_cleanup_complete') is not True:
        cleanup = stream_repack.cleanup_stage(private, stage, receipt, guard=guard)
        save(D_E_stage_cleanup_complete=True, D_state='completed', D_E_stage_cleanup=cleanup)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True, choices=sorted(handoff_cli.ENROLLED.values()))
    args = parser.parse_args()
    print(json.dumps(publish(args.dataset), ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
