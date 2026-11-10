#!/usr/bin/env python3
"""Compare fixed D source generations without modifying either dataset."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import prepare_handoff_index_archive as source_cli, run_handoff_index_d_queue as d_queue
from scripts.publish_data_releases import DEFAULT_CATALOG, _load_catalog, _source_path
from stockagent.data_sync import index_generation_diff, packed_snapshots as p, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

COLD = Path('/srv/stockagent-packed')
CONTRACT = 'stockagent.handoff_index_integration_plan.v1'


def policy(dataset):
    candidates = [entry for entry in _load_catalog(DEFAULT_CATALOG) if entry.get('dataset') == dataset]
    if len(candidates) != 1 or candidates[0].get('publish') is not True:
        raise SnapshotError('dataset has no unique public publication policy')
    entry = candidates[0]
    expected = next((name for name, detail in source_cli.ENROLLED.items() if detail[0] == dataset), None)
    if expected is None:
        raise SnapshotError('dataset source is not enrolled')
    from stockagent.data_sync import workspace_source_handoff as handoff
    with handoff.selected_source(expected):
        source = _source_path(entry).absolute()
        enrolled = handoff._scope().source.absolute()
        bybit_alias = (expected == handoff.BYBIT_NAME and entry.get('source') == 'data_bybit'
                       and source.is_symlink() and source.resolve(strict=True) == enrolled)
        if source != enrolled and not bybit_alias:
            raise SnapshotError('dataset public policy selects another source root')
    excludes = entry.get('excluded_subtrees', [])
    if not isinstance(excludes, list) or any(not isinstance(item, str) for item in excludes):
        raise SnapshotError('dataset public source exclusions are invalid')
    return tuple(sorted(excludes)), p.sha256_file(DEFAULT_CATALOG)


def plan(dataset: str):
    if dataset not in [entry[0] for entry in source_cli.ENROLLED.values()]:
        raise SnapshotError('source integration dataset is not enrolled')
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    _check_d_primary_mount(COLD)
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'
    status_path = root/(dataset+'.status.json')
    if status_path.is_symlink() or not status_path.is_file():
        raise SnapshotError('source integration D receipt unavailable')
    value = json.loads(status_path.read_text())
    if not d_queue._done(value, dataset):
        raise SnapshotError('source integration needs the exact D source generation')
    receipt = value['D_publication']
    identity = p._validate_hash(value.get('source_identity'), 'source identity')
    private_sha = p._validate_hash(receipt.get('new_manifest_sha256'), 'private source manifest')
    private_id = p.validate_slug(receipt.get('new_snapshot_id'), 'private source snapshot')
    private = private_history_root(COLD, version_policy(ROOT), initialize=False)
    excludes, catalog_sha = policy(dataset)
    stage = root/dataset/identity
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('source integration stage redirected')
    output = stage/'integration-diff-v1'
    progress_path = root/(dataset+'.diff-status.json')
    last = 0.0
    def guard():
        workspace_volume.check(c=config)
        _check_d_primary_mount(COLD)
        if private_history_root(COLD, version_policy(ROOT), initialize=False) != private:
            raise SnapshotError('source integration private authority changed')
        if policy(dataset) != (excludes, catalog_sha):
            raise SnapshotError('dataset public source policy changed during comparison')
    def progress(event):
        nonlocal last
        if time.monotonic()-last < 3:
            return
        last = time.monotonic()
        atomic_write_json(progress_path, {'contract': CONTRACT, 'dataset': dataset, 'state': 'running',
            'private_manifest_sha256': private_sha, 'progress': event, 'observed_at_epoch': time.time()})
    result = index_generation_diff.compare(COLD, private, dataset, private_id, private_sha, excludes,
                                            output, guard=guard, progress=progress)
    summary = {'contract': CONTRACT, 'dataset': dataset, 'state': 'metadata_compared',
        'public_manifest_sha256': result['public_manifest_sha256'], 'private_manifest_sha256': private_sha,
        'private_snapshot_id': private_id, 'catalog_sha256': catalog_sha,
        'counters': result['counters'], 'conflict_categories': result['conflict_categories'],
        'comparison_identity_sha256': result['identity_sha256'], 'source_payload_read': False,
        'public_head_changed': False, 'semantic_merge_complete': False, 'observed_at_epoch': time.time()}
    atomic_write_json(progress_path, summary, durable=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True, choices=sorted(entry[0] for entry in source_cli.ENROLLED.values()))
    args = parser.parse_args()
    print(json.dumps(plan(args.dataset), ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
