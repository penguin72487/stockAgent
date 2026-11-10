#!/usr/bin/env python3
"""Move a fixed source-index E archive to a separate private D dataset head."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import prepare_handoff_index_archive as source_cli, run_handoff_index_archive_queue as queue
from scripts import prepare_offline_repair_archive as offline_archive, retire_offline_repair_source as offline_retire
from scripts import run_workspace_source_staging as source_staging
from scripts.run_stream_packed_repack import D_INSTALL_RESERVE, remaining_install_bytes
from stockagent.data_sync import index_archive_existing, index_archive_publish, packed_snapshots as packed, stream_repack, workspace_volume
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

COLD = Path('/srv/stockagent-packed')


def publish(dataset: str):
    sources = {entry[0]: name for name, entry in source_cli.ENROLLED.items()}
    is_offline_repair = dataset in offline_archive.DATASET_SCOPES
    if dataset not in sources and not is_offline_repair:
        raise SnapshotError('source-index D dataset is not enrolled')
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    _check_d_primary_mount(COLD)
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'
    status_path = root/(dataset+'.status.json')
    if (root.is_symlink() or root.resolve(strict=True) != root or status_path.is_symlink()
            or not status_path.is_file()):
        raise SnapshotError('source-index archive status unavailable')
    value = json.loads(status_path.read_text())
    if not (offline_retire.retired_status_matches(value, dataset) if is_offline_repair else queue._done(value, dataset)):
        raise SnapshotError('source-index C retirement receipt is incomplete')
    identity = packed._validate_hash(value.get('source_identity'), 'source-index identity')
    stage = root/dataset/identity
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('source-index E stage redirected')
    if is_offline_repair:
        matches = [item for item in source_staging.load_sources()
                   if source_staging.stream_scope(item) == offline_archive.DATASET_SCOPES[dataset]]
        if len(matches) != 1:
            raise SnapshotError('offline repair staging enrollment differs')
        item = matches[0]
        source_staging.check_offline_repair_provenance(item)
        source, destination, evidence, _ = source_staging.paths(item, workspace)
        if (source_staging.verified_report(item, workspace) is None or not source.is_symlink()
                or source.resolve(strict=True) != destination):
            raise SnapshotError('offline repair E working path or staging receipt differs')
        journal = source_staging._read_json(stage/'C-offline-handoff.json')
        if (journal.get('contract') != 'stockagent.offline_repair_e_handoff.v1'
                or journal.get('state') != 'C_payload_retired' or journal.get('dataset') != dataset
                or journal.get('source_identity') != identity or journal.get('C_payload_deleted') is not True
                or journal.get('prepared_sha256') != value.get('prepared_sha256')):
            raise SnapshotError('offline repair C handoff receipt differs')
        index, root_name = evidence/'files.sqlite', source.name
    else:
        with handoff.selected_source(sources[dataset]):
            binding = handoff.read_journal()
            if not binding or binding.get('state') != 'active':
                raise SnapshotError('source-index handoff is not active')
            handoff._assert_retained(binding)
            scope = handoff._scope()
            index = Path(binding['evidence'])/'files.sqlite'
            root_name = scope.source.name
    private = private_history_root(COLD, version_policy(ROOT), initialize=False)

    def guard():
        workspace_volume.check(c=config)
        _check_d_primary_mount(COLD)
        if private_history_root(COLD, version_policy(ROOT), initialize=False) != private:
            raise SnapshotError('source-index private D authority changed')

    def save(**changes):
        value.update(changes, observed_at_epoch=time.time())
        atomic_write_json(status_path, value, durable=True)

    if value.get('D_archive_committed') is True:
        receipt = value.get('D_publication')
        if (not isinstance(receipt, dict) or receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
                or receipt.get('dataset') != dataset or receipt.get('old_manifest_sha256') != identity
                or receipt.get('source_mode') != 'retained_handoff_C_index'
                or receipt.get('state') not in {'new_D_head_committed_from_exact_E_source',
                                                'existing_D_head_matches_source_inventory'}):
            raise SnapshotError('source-index D receipt differs')
        if (receipt.get('state') == 'existing_D_head_matches_source_inventory'
                and not index_archive_existing.receipt_matches(receipt, value.get('prepared_sha256'))):
            if dataset not in index_archive_existing.SUBTREE_COVERAGES:
                raise SnapshotError('source-index existing D match receipt differs')
            index_archive_existing.restore_covering_pin(private, stage, dataset, identity, root_name,
                receipt.get('existing_match'), guard=guard, install_lock=root.parent/'cold-install-write-owner.lock')
            if not index_archive_existing.receipt_matches(receipt, value.get('prepared_sha256')):
                raise SnapshotError('source-index existing D match receipt differs after pin recovery')
    else:
        prepared = stream_repack._read_prepared(stage, dataset, identity)
        if prepared.get('identity_sha256') != value.get('prepared_sha256'):
            raise SnapshotError('source-index E archive differs from the C retirement receipt')
        last_report = 0.0
        def report(event):
            nonlocal last_report
            if event.get('stage') == 'validating_D_inventory' and time.monotonic()-last_report < 3:
                return
            last_report = time.monotonic()
            save(D_state='installing', D_progress=event, D_archive_committed=False)
        def admit_install(record, journal):
            remaining = remaining_install_bytes(private, record, journal)
            if shutil.disk_usage(COLD).free < remaining+D_INSTALL_RESERVE:
                raise SnapshotError('D free space is below source-index install and reserve')
        def admit_next(item):
            if shutil.disk_usage(COLD).free < item['bytes']+D_INSTALL_RESERVE:
                raise SnapshotError('D reserve reached during source-index install')
        install_lock = root.parent/'cold-install-write-owner.lock'
        try:
            if dataset in index_archive_existing.SUBTREE_COVERAGES:
                # Do not write a second copy when the reviewed parent already
                # contains this entire fixed source subtree.
                raise index_archive_publish.ExistingPrivateHead('reviewed covering parent')
            result = index_archive_publish.publish(private, stage, dataset, identity, index, root_name,
                guard=guard, progress=report, admit_install=admit_install, admit_next=admit_next,
                install_lock=install_lock)
        except index_archive_publish.ExistingPrivateHead:
            if dataset not in index_archive_existing.REVIEWED:
                raise
            proof = index_archive_existing.cached_covering_match(private, stage, dataset, identity, index, root_name,
                                                                  guard=guard, install_lock=install_lock)
            if proof is None:
                proof = index_archive_existing.match(private, stage, dataset, identity, index, root_name,
                                                      guard=guard, install_lock=install_lock, progress=report)
            result = {'state': 'existing_head_matched', 'snapshot_id': proof['snapshot_id'],
                      'new_manifest_sha256': proof['manifest_sha256'], 'files': proof['files'],
                      'logical_bytes': proof['logical_bytes'], 'objects': 0, 'existing_match': proof}
        reused = result.get('state') == 'existing_head_matched'
        receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'existing_D_head_matches_source_inventory' if reused else 'new_D_head_committed_from_exact_E_source',
            'dataset': dataset,
            'old_manifest_sha256': identity, 'new_snapshot_id': result['snapshot_id'],
            'new_manifest_sha256': result['new_manifest_sha256'], 'files': result['files'],
            'logical_bytes': result['logical_bytes'], 'new_object_count': result['objects'],
            'source_mode': 'retained_handoff_C_index', 'private': True,
            'public_dataset_merged': False, 'independent_D_readback': False,
            'NAS_accepted': False, 'observed_at_epoch': time.time()}
        if reused:
            receipt['existing_match'] = result['existing_match']
        save(D_archive_committed=True, D_state='existing_head_matched' if reused else 'head_committed', D_publication=receipt)
    if value.get('D_E_stage_cleanup_complete') is not True:
        if receipt['state'] == 'existing_D_head_matches_source_inventory':
            cleanup = index_archive_existing.cleanup(private, stage, dataset, identity, root_name,
                receipt.get('existing_match'), guard=guard, install_lock=root.parent/'cold-install-write-owner.lock')
        else:
            cleanup = stream_repack.cleanup_stage(private, stage, receipt, guard=guard)
        save(D_E_stage_cleanup_complete=True, D_state='completed', D_E_stage_cleanup=cleanup)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True, choices=sorted(
        {entry[0] for entry in source_cli.ENROLLED.values()} | set(offline_archive.DATASET_SCOPES)))
    args = parser.parse_args()
    print(json.dumps(publish(args.dataset), ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
