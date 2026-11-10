#!/usr/bin/env python3
"""Promote a proven Binance archive superset through its existing D owners."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import plan_handoff_index_integration as planner  # noqa: E402
from scripts import publish_data_releases as publisher  # noqa: E402
from stockagent.data_sync import (packed_member_stream as reader, semantic_link_promotion as promotion,
                                  stream_repack, workspace_volume)  # noqa: E402
from stockagent.data_sync.dataset_migration import private_history_root  # noqa: E402
from stockagent.data_sync.dataset_version_layout import version_policy  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402

DATASET = 'binance-public-archive'


def run() -> dict:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = planner.COLD
    planner._check_d_primary_mount(cold)
    private = private_history_root(cold, version_policy(ROOT), initialize=False)
    parent = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'
    root = parent/DATASET
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('Binance semantic integration root is redirected')
    status = json.loads(reader.read_stable_bytes(parent/(DATASET+'.status.json'), maximum=1024**2))
    if (status.get('contract') != 'stockagent.handoff_index_archive.v1' or status.get('dataset') != DATASET
            or status.get('D_archive_committed') is not True or status.get('D_E_stage_cleanup_complete') is not True):
        raise SnapshotError('Binance sealed private source is not committed')
    source_id = status.get('source_identity')
    if not isinstance(source_id, str) or len(source_id) != 64 or any(c not in '0123456789abcdef' for c in source_id):
        raise SnapshotError('Binance fixed source identity is invalid')
    stage = root/source_id
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('Binance original source staging identity differs')
    plan_path = stage/'integration-diff-v1/diff.json'
    plan = json.loads(reader.read_stable_bytes(plan_path, maximum=1024**2))
    if plan.get('identity_sha256') != stream_repack._identity(plan):
        raise SnapshotError('Binance fixed comparison identity differs')
    worklist_name = plan['worklist']['name']
    if Path(worklist_name).name != worklist_name:
        raise SnapshotError('Binance comparison worklist is not local')
    worklist = reader.read_stable_bytes(plan_path.parent/worklist_name, maximum=8*1024**2)
    if hashlib.sha256(worklist).hexdigest() != plan['worklist']['sha256']:
        raise SnapshotError('Binance comparison worklist SHA differs')
    identity = plan['identity_sha256']
    row_path, control_path = root/('row-audit-'+identity+'.json'), root/('control-audit-'+identity+'.json')
    row = json.loads(reader.read_stable_bytes(row_path, maximum=4*1024**2))
    controls = json.loads(reader.read_stable_bytes(control_path, maximum=1024**2))
    excludes, catalog_sha = planner.policy(DATASET)
    if excludes:
        raise SnapshotError('Binance source has a restricted subtree requiring separate promotion')
    entries = [entry for entry in publisher._load_catalog(publisher.DEFAULT_CATALOG) if entry.get('dataset') == DATASET]
    if len(entries) != 1:
        raise SnapshotError('Binance public catalog entry is not unique')
    publication = status.get('D_publication', {})
    if (publication.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or publication.get('state') != 'new_D_head_committed_from_exact_E_source'
            or publication.get('source_mode') != 'retained_handoff_C_index'
            or publication.get('old_manifest_sha256') != source_id
            or publication.get('new_manifest_sha256') != plan.get('private_manifest_sha256')
            or publication.get('new_snapshot_id') != plan.get('private_snapshot_id')):
        raise SnapshotError('Binance private D publication differs from the comparison')
    prepared = stream_repack._prepared_metadata(stage, DATASET, source_id)
    install = json.loads(reader.read_stable_bytes(stage/'install.json', maximum=1024**2))
    def guard():
        workspace_volume.check(c=config)
        planner._check_d_primary_mount(cold)
        if (private_history_root(cold, version_policy(ROOT), initialize=False) != private
                or planner.policy(DATASET) != (excludes, catalog_sha)):
            raise SnapshotError('Binance promotion authority or catalog policy changed')
    return promotion.promote(cold, private, dataset=DATASET, old_sha=plan['public_manifest_sha256'],
        private_id=plan['private_snapshot_id'], private_sha=plan['private_manifest_sha256'],
        prepared=prepared, install=install, plan=plan, rows=row, controls=controls, catalog_sha=catalog_sha,
        catalog_entry=entries[0], journal_path=root/('semantic-promotion-'+identity+'.json'), guard=guard)


if __name__ == '__main__':
    try:
        value = run()
        print(json.dumps({key: value.get(key) for key in ('contract', 'dataset', 'state',
            'old_manifest_sha256', 'private_manifest_sha256', 'new_manifest_sha256', 'linked_object_count',
            'payload_bytes_copied', 'old_sources_deleted', 'independent_D_readback', 'NAS_accepted')},
            ensure_ascii=False))
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
