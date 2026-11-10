"""Identify a completed fixed-latest successor without claiming value equality.

This is a metadata/namespace proof for the existing history-retirement owner.
It performs no data mutation and never substitutes for its pin, current-head,
reference, active-process or original lock checks.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from stockagent.data_sync import packed_snapshots as packed, repack_retirement, stream_repack
from stockagent.data_sync import latest_promotion_evidence as evidence
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_member_stream import read_stable_bytes
from stockagent.data_sync.semantic_link_promotion import (
    FAST_LATEST_DATASETS, LATEST_CONTRACT, validate_latest_fixed_evidence,
)
from stockagent.data_sync.semantic_link_retirement import (
    LATEST_CONTRACT as RETIREMENT_CONTRACT, _check_replacement,
)


def latest_fixed_successor(cold: Path, stage: Path, dataset: str, old_sha: str, old_id: str):
    """Follow exactly one already-retired, policy-pinned latest promotion."""
    if dataset not in FAST_LATEST_DATASETS:
        return None
    from stockagent.data_sync.dataset_migration import private_history_root
    from stockagent.data_sync.dataset_version_layout import version_policy

    cold, stage = Path(cold), Path(stage)
    root = stage.parents[2] / 'handoff-index-archive-v1' / dataset
    if not root.exists():
        return None
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('latest successor evidence root redirected')
    paths = list(root.glob('latest-promotion-*.json'))
    if len(paths) > 128:
        raise SnapshotError('latest successor evidence exceeds bound')
    candidates = []
    for path in paths:
        value = json.loads(read_stable_bytes(path, maximum=1024**2))
        if isinstance(value, dict) and value.get('old_manifest_sha256') == old_sha:
            candidates.append((path, value))
    if not candidates:
        return None
    if len(candidates) != 1:
        raise SnapshotError('latest successor has ambiguous promotion evidence')
    path, promoted = candidates[0]
    identity = packed._validate_hash(promoted.get('comparison_identity_sha256'), 'latest comparison SHA')
    new_sha = packed._validate_hash(promoted.get('new_manifest_sha256'), 'latest successor SHA')
    new_id = packed.validate_slug(promoted.get('new_snapshot_id'), 'latest successor snapshot')
    private_sha = packed._validate_hash(promoted.get('private_manifest_sha256'), 'latest private SHA')
    summary = promoted.get('retirement')
    if (path.name != 'latest-promotion-'+identity+'.json'
            or promoted.get('contract') != LATEST_CONTRACT or promoted.get('state') != 'committed'
            or promoted.get('dataset') != dataset or promoted.get('old_sources_deleted') is not True
            or promoted.get('changed_member_values_compared') is not False
            or promoted.get('old_historical_value_coverage') != 'not_compared'
            or not isinstance(summary, dict) or summary.get('state') != 'completed'
            or summary.get('apply') is not True or summary.get('dataset') != dataset
            or summary.get('old_manifest_sha256') != old_sha or summary.get('new_manifest_sha256') != new_sha
            or summary.get('changed_member_values_compared') is not False
            or summary.get('old_historical_value_coverage') != 'not_compared'):
        raise SnapshotError('latest successor promotion or retirement summary differs')

    journal_path = root / ('latest-retirement-'+identity+'.json')
    before = stream_repack._signature(journal_path)
    journal = repack_retirement.load_journal(journal_path)
    if stream_repack._signature(journal_path) != before:
        raise SnapshotError('latest successor retirement journal changed')
    retired_plan = journal.get('plan')
    items = retired_plan.get('items') if isinstance(retired_plan, dict) else None
    removed = journal.get('removed')
    old_relative = f'manifests/{dataset}/{old_id}.json'
    if (journal.get('contract') != RETIREMENT_CONTRACT or journal.get('state') != 'completed'
            or journal.get('dataset') != dataset or journal.get('old_manifest_sha256') != old_sha
            or journal.get('new_manifest_sha256') != new_sha
            or journal.get('comparison_identity_sha256') != identity
            or journal.get('changed_member_values_compared') is not False
            or journal.get('old_historical_value_coverage') != 'not_compared'
            or not isinstance(retired_plan, dict)
            or journal.get('plan_sha256') != stream_repack._identity(retired_plan)
            or not isinstance(items, list) or any(not isinstance(item, dict) for item in items)
            or not isinstance(removed, list) or removed != [item.get('relative') for item in items]
            or retired_plan.get('old_snapshot_id') != old_id or retired_plan.get('new_snapshot_id') != new_id
            or not any(item.get('kind') == 'manifest' and item.get('relative') == old_relative for item in items)
            or os.path.lexists(cold / old_relative)):
        raise SnapshotError('latest successor does not prove the exact prior manifest was retired')

    plans = list(root.glob('*/integration-diff-v1/diff-plan-'+identity+'.json'))
    if len(plans) != 1:
        raise SnapshotError('latest successor comparison is missing or ambiguous')
    plan = json.loads(read_stable_bytes(plans[0], maximum=1024**2))
    policy = json.loads(read_stable_bytes(evidence.POLICY, maximum=65536))
    validate_latest_fixed_evidence(plan, policy)
    if (plan.get('identity_sha256') != identity or plan.get('dataset') != dataset
            or plan.get('public_manifest_sha256') != old_sha or plan.get('public_snapshot_id') != old_id
            or plan.get('private_manifest_sha256') != private_sha
            or plan.get('private_snapshot_id') != promoted.get('private_snapshot_id')):
        raise SnapshotError('latest successor comparison identity differs')
    worklist = plan.get('worklist', {}).get('name') if isinstance(plan.get('worklist'), dict) else None
    if not isinstance(worklist, str) or Path(worklist).name != worklist:
        raise SnapshotError('latest successor worklist name differs')
    evidence.validate_worklist(plan, read_stable_bytes(plans[0].parent / worklist,
                                                      maximum=evidence.MAX_WORKLIST_COMPRESSED_BYTES))
    conflicts = plan['counters']['conflict']
    if (promoted.get('uncompared_changed_members') != conflicts
            or summary.get('uncompared_changed_members') != conflicts
            or journal.get('uncompared_changed_members') != conflicts):
        raise SnapshotError('latest successor uncompared-member count differs')
    repo = Path(__file__).resolve().parents[2]
    private = private_history_root(cold, version_policy(repo), initialize=False)
    # The original latest promotion may have been followed by a legitimate
    # newer publication. This function never changes the head; the caller
    # separately protects every current head and this exact fixed anchor.
    newer = _check_replacement(cold, private, promoted, plan, require_current_head=False)
    if newer.manifest_sha256 != new_sha or newer.manifest != promoted.get('manifest'):
        raise SnapshotError('latest successor live manifest differs from its fixed promotion')
    return {'manifest_sha256': new_sha, 'snapshot_id': new_id, 'comparison_identity_sha256': identity,
            'changed_member_values_compared': False, 'old_historical_value_coverage': 'not_compared',
            'uncompared_changed_members': conflicts}
