#!/usr/bin/env python3
"""Select an enrolled sealed newer generation without inventing row equality."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import plan_handoff_index_integration as planner, publish_data_releases as publisher  # noqa: E402
from stockagent.data_sync import (packed_member_stream as reader, packed_snapshots as packed,
    semantic_link_promotion as promotion, stream_repack, workspace_volume)  # noqa: E402
from stockagent.data_sync.dataset_migration import private_history_root  # noqa: E402
from stockagent.data_sync.dataset_version_layout import version_policy  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402
from stockagent.data_sync.latest_promotion_evidence import (  # noqa: E402
    POLICY, MAX_WORKLIST_MEMBERS, MAX_WORKLIST_LINE, MAX_WORKLIST_TEXT_CHARS,
    MAX_WORKLIST_COMPRESSED_BYTES, validate_worklist,
)

DATASET = 'binance'
DATASETS = tuple(sorted(promotion.FAST_LATEST_DATASETS))
def run(dataset: str = DATASET, *, dry_run: bool = False) -> dict:
    if dataset not in DATASETS:
        raise SnapshotError('latest fixed dataset is not enrolled')
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = planner.COLD
    planner._check_d_primary_mount(cold)
    private = private_history_root(cold, version_policy(ROOT), initialize=False)
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'
    base = root/dataset
    if base.is_symlink() or base.resolve(strict=True) != base:
        raise SnapshotError('latest fixed source root is redirected')
    policy = json.loads(reader.read_stable_bytes(POLICY, maximum=65536))
    status = json.loads(reader.read_stable_bytes(root/(dataset+'.status.json'), maximum=1024**2))
    if (status.get('contract') != 'stockagent.handoff_index_archive.v1' or status.get('dataset') != dataset
            or status.get('D_archive_committed') is not True or status.get('D_E_stage_cleanup_complete') is not True):
        raise SnapshotError('latest fixed source has not completed its D installation')
    source_id = packed._validate_hash(status.get('source_identity'), 'fixed source identity')
    stage = base/source_id
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('latest fixed source stage is redirected')
    plan_path = stage/'integration-diff-v1/diff.json'
    plan = json.loads(reader.read_stable_bytes(plan_path, maximum=1024**2))
    promotion.validate_latest_fixed_evidence(plan, policy)
    worklist = plan.get('worklist', {})
    name = worklist.get('name')
    if not isinstance(name, str) or Path(name).name != name:
        raise SnapshotError('latest fixed selection worklist is not local')
    validate_worklist(plan, reader.read_stable_bytes(plan_path.parent/name, maximum=MAX_WORKLIST_COMPRESSED_BYTES))
    # The moving diff.json is refreshed after the public head changes. Pin
    # this exact pre-selection plan first, including comparisons produced by
    # older versions of the planner that did not yet write immutable plans.
    fixed_plan = plan_path.parent/('diff-plan-'+plan['identity_sha256']+'.json')
    if fixed_plan.is_symlink():
        raise SnapshotError('latest fixed selection immutable plan is redirected')
    if fixed_plan.exists():
        if json.loads(reader.read_stable_bytes(fixed_plan, maximum=1024**2)) != plan:
            raise SnapshotError('latest fixed selection immutable plan differs')
    elif not dry_run:
        packed.write_immutable_json(fixed_plan, plan)
    excludes, catalog_sha = planner.policy(dataset)
    if tuple(plan.get('excluded_subtrees', ())) != excludes:
        raise SnapshotError('latest fixed source public exclusions changed')
    entries = [entry for entry in publisher._load_catalog(publisher.DEFAULT_CATALOG) if entry.get('dataset') == dataset]
    if len(entries) != 1:
        raise SnapshotError('latest fixed source catalog entry is not unique')
    publication = status.get('D_publication', {})
    if (publication.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or publication.get('state') != 'new_D_head_committed_from_exact_E_source'
            or publication.get('source_mode') != 'retained_handoff_C_index'
            or publication.get('old_manifest_sha256') != source_id
            or publication.get('new_manifest_sha256') != plan.get('private_manifest_sha256')
            or publication.get('new_snapshot_id') != plan.get('private_snapshot_id')):
        raise SnapshotError('latest fixed D generation differs from its source receipt')
    prepared = stream_repack._prepared_metadata(stage, dataset, source_id)
    install = json.loads(reader.read_stable_bytes(stage/'install.json', maximum=1024**2))
    def guard():
        workspace_volume.check(c=config)
        planner._check_d_primary_mount(cold)
        if (private_history_root(cold, version_policy(ROOT), initialize=False) != private
                or planner.policy(dataset) != (excludes, catalog_sha)
                or json.loads(reader.read_stable_bytes(POLICY, maximum=65536)) != policy):
            raise SnapshotError('latest fixed authority or selection policy changed')
    return promotion.promote(cold, private, dataset=dataset, old_sha=plan['public_manifest_sha256'],
        private_id=plan['private_snapshot_id'], private_sha=plan['private_manifest_sha256'],
        prepared=prepared, install=install, plan=plan, rows={}, controls={}, catalog_sha=catalog_sha,
        catalog_entry=entries[0], journal_path=base/('latest-promotion-'+plan['identity_sha256']+'.json'),
        guard=guard, latest_policy=policy, dry_run=dry_run)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('--dataset', choices=DATASETS, default=DATASET)
        parser.add_argument('--dry-run', action='store_true')
        args = parser.parse_args()
        value = run(args.dataset, dry_run=args.dry_run)
        print(json.dumps({key: value.get(key) for key in ('contract', 'dataset', 'state',
            'old_manifest_sha256', 'private_manifest_sha256', 'new_manifest_sha256', 'linked_object_count',
            'payload_bytes_copied', 'uncompared_changed_members', 'old_historical_value_coverage',
            'old_sources_deleted', 'independent_D_readback', 'NAS_accepted', 'object_count',
            'reused_existing_objects', 'duplicate_encoded_bytes_pending', 'head_changed', 'source_deleted')}, ensure_ascii=False))
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
