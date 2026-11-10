#!/usr/bin/env python3
"""Publish an audited FX superset through the existing dataset owner.

This deliberately handles only a proven superset of a fixed pair of releases.
True data/metadata conflicts remain for a domain merge; neither source is
deleted. The excluded official subtree stays in its separate private source.
"""
from __future__ import annotations

from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import stat
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import plan_handoff_index_integration as planner  # noqa: E402
from stockagent.data_sync import (index_generation_diff as diff, packed_snapshots as packed,
                                  provider_workspace, stream_repack, workspace_source_handoff as handoff,
                                  workspace_volume)  # noqa: E402
from stockagent.data_sync.dataset_migration import private_history_root  # noqa: E402
from stockagent.data_sync.dataset_version_layout import version_policy  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json  # noqa: E402
from stockagent.data_sync.packed_member_stream import read_stable_bytes as _stable_bytes  # noqa: E402

DATASET = 'forex-frankfurter'
CONTRACT = 'stockagent.frankfurter_fixed_generation_integration.v1'


def validate_audits(row: dict, metadata: dict, plan: dict) -> None:
    common = ('public_manifest_sha256', 'private_manifest_sha256', 'worklist_sha256')
    if any(row.get(k) != metadata.get(k) or row.get(k) != plan.get(k) for k in common):
        raise SnapshotError('row, metadata and comparison identities differ')
    if (row.get('contract') != 'stockagent.frankfurter_generation_row_audit.v1'
            or metadata.get('contract') != 'stockagent.frankfurter_generation_metadata_audit.v1'
            or row.get('source_mutated') is not False or metadata.get('source_mutated') is not False
            or row.get('errors') != [] or metadata.get('errors') != []):
        raise SnapshotError('source-generation audits are not complete')
    pairs = plan['counters']['conflict_categories']['parquet']
    files = sum(plan['counters']['conflict_categories'].values()) - pairs
    if (row.get('pairs') != pairs or row.get('pair_compared') != pairs
            or row.get('live_matches_private') != pairs or row.get('live_changed_from_private') != 0
            or any(row.get(k) != 0 for k in ('public_only_rows', 'conflicting_overlap_rows',
                                            'duplicate_dates', 'null_dates'))
            or metadata.get('files') != files or metadata.get('live_matches_private') != files
            or metadata.get('bad_sha_binding') != 0
            or metadata.get('equal_except_source_sha') != pairs):
        raise SnapshotError('source generations need an explicit row or metadata merge')
    if (metadata.get('different_fields') != {'non_json': 1, 'applied_end_date': 1,
            'requested_end_date': 1, 'row_count': 1, 'provider_end_date': 1}
            or plan['counters'].get('public_only') != 0
            or plan['counters'].get('private_only') != 0
            or plan['counters'].get('unexpected_public_excluded') != 0):
        raise SnapshotError('source generation contains unresolved non-price differences')


def check_live_source(source: Path, public_rows: dict, private_rows: dict) -> int:
    """Bind the current E source bytes to the audited private generation."""
    files = 0
    for name, original in public_rows.items():
        relative = packed._safe_relative_path(name, 'FX source path')
        if relative.as_posix() != name:
            raise SnapshotError('source inventory has a noncanonical path')
        newer = private_rows.get(name)
        if not isinstance(newer, dict) or original['kind'] != newer['kind']:
            raise SnapshotError('old source path is missing from the newer generation')
        path = source.joinpath(*relative.parts)
        if original['kind'] == 'directory':
            if path.is_symlink() or not path.is_dir() or path.resolve(strict=True) != path:
                raise SnapshotError('source directory is missing or redirected')
            continue
        if original['kind'] != 'file' or original.get('mode') != newer.get('mode'):
            raise SnapshotError('source path kind or mode needs explicit reconciliation')
        value = _stable_bytes(path, maximum=64 * 1024**2)
        if len(value) != newer['size'] or hashlib.sha256(value).hexdigest() != newer['sha256']:
            raise SnapshotError('live source no longer matches its audited private generation')
        if stat.S_IMODE(path.lstat().st_mode) != newer['mode']:
            raise SnapshotError('live source permissions changed')
        if name.endswith('_features.head.json'):
            pair = name.removesuffix('.head.json') + '.parquet'
            proof = json.loads(value)
            if not isinstance(proof, dict) or proof.get('source_sha256') != private_rows.get(pair, {}).get('sha256'):
                raise SnapshotError('live historical coverage proof does not bind its data file')
        files += 1
    return files


def integrate() -> dict:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = planner.COLD
    planner._check_d_primary_mount(cold)
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'/DATASET
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('FX integration root is redirected')
    output = root/'frankfurter-semantic-integration-v1.json'
    if output.is_symlink():
        raise SnapshotError('FX integration receipt is redirected')
    if output.exists():
        previous = json.loads(_stable_bytes(output, maximum=1024**2))
        current = packed.resolve_latest_packed(cold, DATASET, require_objects=False)
        if (previous.get('contract') == CONTRACT and previous.get('state') == 'public_generation_integrated'
                and previous.get('new_public_manifest_sha256') == current.manifest_sha256):
            return previous
        raise SnapshotError('existing FX integration receipt has a different current head')
    plans = list(root.glob('*/integration-diff-v1/diff.json'))
    if len(plans) != 1:
        raise SnapshotError('FX integration requires one fixed comparison')
    plan_path = plans[0]
    plan = json.loads(_stable_bytes(plan_path, maximum=1024**2))
    if (plan.get('contract') != diff.CONTRACT or plan.get('dataset') != DATASET
            or plan.get('identity_sha256') != stream_repack._identity(plan)):
        raise SnapshotError('FX comparison identity differs')
    worklist_name = packed._safe_relative_path(plan['worklist']['name'], 'comparison worklist')
    if len(worklist_name.parts) != 1:
        raise SnapshotError('FX comparison worklist name is not local')
    worklist = plan_path.parent/worklist_name
    if hashlib.sha256(_stable_bytes(worklist, maximum=4*1024**2)).hexdigest() != plan['worklist']['sha256']:
        raise SnapshotError('FX comparison worklist SHA differs')
    row_path, meta_path = root/'frankfurter-row-audit-v1.json', root/'frankfurter-metadata-audit-v1.json'
    row_bytes = _stable_bytes(row_path, maximum=1024**2)
    meta_bytes = _stable_bytes(meta_path, maximum=1024**2)
    row, metadata = json.loads(row_bytes), json.loads(meta_bytes)
    checked_plan = {'public_manifest_sha256': plan['public_manifest_sha256'],
        'private_manifest_sha256': plan['private_manifest_sha256'],
        'worklist_sha256': plan['worklist']['sha256'],
        'counters': {**plan['counters'], 'conflict_categories': plan['conflict_categories']}}
    validate_audits(row, metadata, checked_plan)
    excludes, catalog_sha = planner.policy(DATASET)
    if list(excludes) != plan['excluded_subtrees']:
        raise SnapshotError('FX public exclusion policy changed')
    private = private_history_root(cold, version_policy(ROOT), initialize=False)
    public_release = packed.resolve_packed_snapshot_id(cold, DATASET, plan['public_snapshot_id'], require_objects=False)
    private_release = packed.resolve_packed_snapshot_id(private, DATASET, plan['private_snapshot_id'], require_objects=False)
    if (public_release.manifest_sha256 != plan['public_manifest_sha256']
            or private_release.manifest_sha256 != plan['private_manifest_sha256']):
        raise SnapshotError('FX fixed source generations changed')
    public_rows = {x['path']: x for x in packed._iter_inventory(cold, public_release.manifest)}
    private_rows = {x['path']: x for x in packed._iter_inventory(private, private_release.manifest)}
    with handoff.selected_source(handoff.FOREX_NAME), ExitStack() as locks:
        value = handoff.read_journal()
        if not value or value.get('state') != 'active':
            raise SnapshotError('FX workspace handoff is not active')
        destination = handoff._assert_retained(value)
        scope = handoff._scope()
        if not provider_workspace.exact_mount(scope.source, destination):
            raise SnapshotError('FX service source is not the enrolled E mount')
        work_guard = handoff._source_work_window_guard()
        work_guard()
        locks.enter_context(handoff.lock_file(handoff.CONTROL/'owner.lock'))
        locks.enter_context(handoff.lock_file(scope.source/'.download.lock'))
        locks.enter_context(handoff.lock_file(scope.source/'official_v2/.download.lock'))
        if (handoff.read_journal() != value or handoff._assert_retained(value) != destination
                or not provider_workspace.exact_mount(scope.source, destination)
                or planner.policy(DATASET) != (excludes, catalog_sha)):
            raise SnapshotError('FX source or policy changed while acquiring original owners')
        source = scope.source.absolute()
        checked = check_live_source(source, public_rows, private_rows)
        result_path = root/('frankfurter-publication-'+plan['identity_sha256']+'.json')
        if result_path.is_symlink():
            raise SnapshotError('FX publication receipt is redirected')
        current = packed.resolve_latest_packed(cold, DATASET, require_objects=False)
        if not result_path.exists():
            if current.manifest_sha256 != plan['public_manifest_sha256']:
                raise SnapshotError('FX public head changed before its publication receipt')
            command = [sys.executable, str(ROOT/'scripts/publish_data_releases.py'), 'publish', DATASET,
                '--sync-root', str(cold), '--reader-contract', 'packed-v2-chunks',
                '--expected-previous-manifest-sha256', plan['public_manifest_sha256'],
                '--result-receipt', str(result_path)]
            result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=3600)
            if result.returncode:
                raise SnapshotError('canonical FX publisher failed; inspect current head and original receipt: '
                                    + result.stderr[-1000:])
        receipt = json.loads(_stable_bytes(result_path, maximum=1024**2))
        published = receipt.get('published')
        if not isinstance(published, list) or len(published) != 1 or published[0].get('dataset') != DATASET:
            raise SnapshotError('canonical FX publisher returned no exact publication receipt')
        current = packed.resolve_latest_packed(cold, DATASET, require_objects=False)
        if published[0].get('snapshot_id') != current.manifest['snapshot_id']:
            raise SnapshotError('FX publication receipt and current head differ')
        outcome = {'contract': CONTRACT, 'state': 'public_generation_integrated',
            'public_original_manifest_sha256': plan['public_manifest_sha256'],
            'private_original_manifest_sha256': plan['private_manifest_sha256'],
            'comparison_identity_sha256': plan['identity_sha256'],
            'new_public_manifest_sha256': current.manifest_sha256,
            'catalog_sha256': catalog_sha, 'live_source_files_hashed': checked,
            'row_audit_sha256': hashlib.sha256(row_bytes).hexdigest(),
            'metadata_audit_sha256': hashlib.sha256(meta_bytes).hexdigest(),
            'public_only_rows': row['public_only_rows'], 'conflicting_overlap_rows': row['conflicting_overlap_rows'],
            'new_live_rows': row['live_only_rows'], 'private_excluded_items': plan['counters']['private_excluded'],
            'old_sources_deleted': False, 'independent_D_readback': False, 'NAS_accepted': False,
            'observed_at_epoch': time.time()}
        atomic_write_json(output, outcome, durable=True)
        return outcome


if __name__ == '__main__':
    try:
        print(json.dumps(integrate(), ensure_ascii=False))
    except (OSError, ValueError, SnapshotError, subprocess.SubprocessError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
