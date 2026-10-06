#!/usr/bin/env python3
"""One-shot penguin retirement of explicitly inventoried offline artifacts.

Reuse legacy exact capture, D publication/recovery, consumer gates and the
canonical source/mirror quarantine transaction. Never enroll a timer or delete
source directories, active service data, cold objects or NAS snapshots.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import replace
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.manage_cold_artifacts import _bridge_inactive
from scripts.manage_packed_retention import _syncthing, _wait_for_convergence
from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.artifact_maintenance import artifact_process_references, artifact_process_references_many
from stockagent.data_sync.artifact_retirement import load_retirement_peer_names
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, _safe_relative_path, validate_slug
from stockagent.data_sync.legacy_artifact_archive import (
    MANUAL_WSL_CAPTURE_CONTRACT, MANUAL_WSL_SCOPES, MANUAL_OFFLINE_SIMULATION_CONTRACT,
    reviewed_offline_simulation_root, LegacyArchiveSpec,
    MANUAL_TRANSFER_QUARANTINE_CONTRACT, reviewed_transfer_quarantine_root,
    MANUAL_MINUTE_DERIVED_VIEW_CONTRACT, reviewed_minute_derived_view_root,
    apply_archive_stage_prune, load_legacy_specs, plan_archive_stage_prune,
    prepare_archive, publish_archive, verify_archive_directory, verify_cold_archive,
)
from stockagent.data_sync.legacy_artifact_retirement import (
    plan_legacy_retirement, apply_legacy_retirement, plan_legacy_quarantine_resume,
)
from stockagent.data_sync.packed_retention import RetentionConfig
from stockagent.data_sync.packed_snapshots import resolve_latest_packed
from stockagent.data_sync.remote_legacy_return import active_configuration_references, active_configuration_references_many, metadata_tree, real

COMMON_OWNER = Path('/run/lock/stockagent-remote-cold-artifact-ingress.lock')
SCOPES = MANUAL_WSL_SCOPES | {'markets', 'ablations', 'live', 'stockagent-transfer-quarantine', 'data_tw_minute'}
EXISTING_CATALOG = ROOT / 'configs/data_sync/legacy_artifact_archives.json'
HOT_ARTIFACT_ROOT = Path('/srv/stockagent-artifacts-hot')
HOT_MIRROR_ORIGIN = 'retained-hot-mirror-preservation-v1'
TRANSFER_QUARANTINE_ORIGIN = 'retained-transfer-quarantine-preservation-v1'
MINUTE_DERIVED_VIEW_ORIGIN = 'retained-minute-derived-view-preservation-v1'
MINUTE_VIEW_NAMES = {
    'developing-v5': 'data_tw_minute/research_dataset_developing_v5',
    'schema2-volume-bug': 'data_tw_minute/research_dataset_schema2_volume_bug_20260807',
}


def source_layout(origin=None):
    """Keep an unequal historical mirror separate from the authority version."""
    if origin is None:
        return (ROOT/'artifacts', HOT_ARTIFACT_ROOT,
                Path('/var/lib/stockagent-legacy-artifacts'),
                Path('/var/lib/stockagent-cold-artifacts/activations'))
    if origin == TRANSFER_QUARANTINE_ORIGIN:
        states=Path('/var/lib/stockagent-legacy-transfer-quarantine')
        return (Path('/srv'),states/'no-secondary-mirror',states,
                Path('/var/lib/stockagent-cold-artifacts/transfer-quarantine-activations'))
    if origin == MINUTE_DERIVED_VIEW_ORIGIN:
        states=Path('/var/lib/stockagent-legacy-minute-derived-views')
        return (ROOT,states/'no-secondary-mirror',states,
                Path('/var/lib/stockagent-cold-artifacts/minute-derived-view-activations'))
    if origin != HOT_MIRROR_ORIGIN:
        raise SnapshotError('unknown manual artifact source origin')
    states = Path('/var/lib/stockagent-legacy-hot-mirrors')
    return (HOT_ARTIFACT_ROOT, states/'no-secondary-mirror', states,
            Path('/var/lib/stockagent-cold-artifacts/hot-mirror-activations'))


def dataset_prefix(origin=None):
    if origin == TRANSFER_QUARANTINE_ORIGIN:
        return 'legacy-wsl-retired-transfer-'
    if origin == MINUTE_DERIVED_VIEW_ORIGIN:
        return 'legacy-wsl-minute-derived-'
    return 'legacy-wsl-hot-mirror-' if origin == HOT_MIRROR_ORIGIN else 'legacy-wsl-offline-'


def reusable_publication(target: Path, spec: LegacyArchiveSpec, sync_root: Path):
    """Bind a completed commit for resume, without claiming byte recovery."""
    receipt = target / 'publication.json'
    if not receipt.exists():
        return None
    published = json.loads(receipt.read_text())
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    if (published.get('dataset') != spec.dataset
            or published.get('snapshot_id') != resolved.manifest['snapshot_id']
            or published.get('manifest_sha256') != resolved.manifest_sha256):
        raise SnapshotError('completed publication differs from its fixed D release')
    return published


@contextmanager
def phase(target: Path, name: str):
    """Persist current work separately from completed retirement evidence."""
    path = target / 'phase.json'
    started = time.monotonic()
    value = {'phase': name, 'state': 'running', 'started_at_epoch': time.time()}
    atomic_write_json(path, value)
    try:
        yield
    except BaseException as error:
        value.update(state='failed', error_type=type(error).__name__)
        raise
    else:
        value['state'] = 'completed'
    finally:
        value.update(finished_at_epoch=time.time(), elapsed_seconds=time.monotonic()-started)
        atomic_write_json(path, value)


@contextmanager
def publication_owner(target: Path):
    with phase(target, 'wait-for-publication-owner'):
        with COMMON_OWNER.open('a') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX)
            yield


def observe(artifact_root: Path, relative: str, *, service_refs: list[str] | None = None,
            configuration_refs: list[str] | None = None, process_refs: list[str] | None = None,
            repo_root: Path | None = None) -> dict:
    path = _safe_relative_path(relative, 'reviewed offline artifact')
    if path.parts[0] not in SCOPES or len(path.parts) < 2:
        raise SnapshotError('offline preservation requires one explicit artifact child')
    if path.parts[0] == 'live' and not reviewed_offline_simulation_root(relative):
        raise SnapshotError('canonical live ledgers are outside this cleanup')
    if path.parts[0]=='stockagent-transfer-quarantine' and not reviewed_transfer_quarantine_root(relative):
        raise SnapshotError('only an explicit dated retired transport root is in scope')
    if path.parts[0]=='data_tw_minute' and not reviewed_minute_derived_view_root(relative):
        raise SnapshotError('only the explicitly retired derived minute views are in scope')
    source = real(artifact_root / path)
    if not source.is_dir():
        raise SnapshotError('offline artifact must be a real directory')
    tree = metadata_tree(source)
    repository = repo_root if repo_root is not None else artifact_root.parent
    process_scope = artifact_root/'data_tw_minute' if path.parts[0]=='data_tw_minute' else artifact_root
    refs = ((process_refs if process_refs is not None else artifact_process_references(source, process_scope))
            + (configuration_refs if configuration_refs is not None else
               active_configuration_references(source, repository))
            + (service_refs if service_refs is not None else
               artifact_service_references((source,), repository)[str(source)]))
    newest = max((r['signature'][3] for r in tree['rows'] if r['kind'] == 'file'), default=0)
    blockers = list(refs)
    if any(r['kind'] == 'unsupported' or r['cross_filesystem'] for r in tree['rows']):
        blockers.append('non-regular-or-cross-filesystem')
    if not tree['files']:
        blockers.append('empty-artifact')
    if time.time_ns() - newest < 12 * 3600 * 10**9:
        blockers.append('twelve-hour-stability-pending')
    return {'relative_root': relative, 'fingerprint': tree['fingerprint'],
            'observation_rows': tree['rows'],
            'files': tree['files'], 'logical_bytes': tree['logical_bytes'],
            'allocated_unique_file_bytes': tree['allocated_unique_file_bytes'],
            'newest_mtime_ns': newest, 'blockers': sorted(set(blockers))}


def shared_link_observation_drift(before: dict, after: dict):
    """Accept only link removal observations; byte preservation is still fresh.

    Every path, inode, device, size, mtime and mode must remain identical. This
    is capture admission, never a substitute for source SHA or cold recovery.
    Older inventories without full rows must be re-inventoried on any drift.
    """
    old_rows, new_rows = before.get('observation_rows'), after.get('observation_rows')
    if not isinstance(old_rows,list) or not isinstance(new_rows,list) or len(old_rows)!=len(new_rows):
        return None
    drift=[]
    for old,new in zip(old_rows,new_rows,strict=True):
        if any(old[key]!=new[key] for key in ('path','kind','cross_filesystem')):
            return None
        first,second=old['signature'],new['signature']
        if first==second:
            continue
        if (old['kind']!='file' or any(first[i]!=second[i] for i in (0,1,2,3,5))
                or not 1 <= second[6] < first[6] or second[4] < first[4]):
            return None
        drift.append({'path':old['path'],'before_links':first[6],'current_links':second[6]})
    return drift or None


def inventory(relatives: list[str], output: Path, existing_datasets: list[str] | None = None,
              *, hot_mirror: bool = False, transfer_quarantine: bool = False,
              minute_derived_view: bool = False) -> dict:
    if sum((hot_mirror,transfer_quarantine,minute_derived_view))>1:
        raise SnapshotError('different physical source roles cannot share an inventory')
    if (hot_mirror or transfer_quarantine or minute_derived_view) and existing_datasets:
        raise SnapshotError('historical hot mirrors require their own new exact archive identity')
    retained = {}
    catalog_sha = None
    if existing_datasets:
        specs = load_legacy_specs(EXISTING_CATALOG)
        catalog_sha = hashlib.sha256(EXISTING_CATALOG.read_bytes()).hexdigest()
        for name in existing_datasets:
            spec = specs[name]
            if (spec.stage_root != Path('/var/lib/stockagent-legacy-archive-stage')
                    or spec.manual_capture_min_stable_hours is None):
                raise SnapshotError('existing retirement requires the enrolled bounded C stage')
            relatives.append(spec.relative_root)
            retained[spec.relative_root] = name
    if output.exists() or len(set(relatives)) != len(relatives):
        raise SnapshotError('use a fresh inventory and unique explicit roots')
    paths = [Path(p) for p in relatives]
    if any(a in b.parents or b in a.parents for i, a in enumerate(paths) for b in paths[i+1:]):
        raise SnapshotError('selected offline roots overlap')
    # Inventory is read-only admission. Resolve the common service configuration
    # once through its canonical batched API; apply/encoding/quarantine always
    # obtain current independent consumer observations again.
    origin = (MINUTE_DERIVED_VIEW_ORIGIN if minute_derived_view else TRANSFER_QUARANTINE_ORIGIN
              if transfer_quarantine else HOT_MIRROR_ORIGIN if hot_mirror else None)
    if transfer_quarantine and any(not reviewed_transfer_quarantine_root(p) for p in relatives):
        raise SnapshotError('retired transport inventory cannot include other physical roots')
    if minute_derived_view and any(not reviewed_minute_derived_view_root(p) for p in relatives):
        raise SnapshotError('retired minute view inventory excludes current originals and other data roots')
    artifact_root = source_layout(origin)[0]
    sources = [real(artifact_root / _safe_relative_path(p, 'reviewed offline artifact')) for p in relatives]
    service_refs = artifact_service_references(sources, ROOT)
    configuration_refs = active_configuration_references_many(sources, ROOT)
    # No references anywhere is a complete batched negative observation. If
    # any job uses a selected root, resolve per-root attribution conservatively.
    process_scope=artifact_root/'data_tw_minute' if minute_derived_view else artifact_root
    process_refs = None if artifact_process_references_many(sources, process_scope) else []
    rows = [observe(artifact_root, relative, service_refs=service_refs[str(source)],
                    configuration_refs=configuration_refs[str(source)], process_refs=process_refs,
                    repo_root=ROOT)
            for relative, source in zip(relatives, sources, strict=True)]
    for row in rows:
        if row['relative_root'] in retained:
            row['existing_dataset'] = retained[row['relative_root']]
    result = {'schema_version': 1, 'contract': MANUAL_WSL_CAPTURE_CONTRACT,
              'authority_node_id': 'penguin', 'observed_at_epoch': time.time(), 'items': rows}
    if hot_mirror or transfer_quarantine or minute_derived_view:
        result.update(source_origin=origin, artifact_root=str(artifact_root))
    if catalog_sha:
        result['existing_catalog_sha256'] = catalog_sha
    atomic_write_json(output, result)
    return result


def apply(inventory_path: Path, receipt_dir: Path, *, wait_for_owner: bool = False) -> dict:
    before = inventory_path.read_bytes()
    receipt_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (receipt_dir / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
        return _apply(inventory_path, before, receipt_dir)


def verify_for_retirement(spec, sync_root: Path, target: Path):
    """Select enough private scratch without replacing physical admission."""
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    needed = int(resolved.manifest['source']['logical_bytes']) + 32 * 1024**3
    temporary_free = shutil.disk_usage(Path(tempfile.gettempdir())).free
    workspace = None if temporary_free >= needed else target / 'verification-scratch'
    atomic_write_json(target / 'recovery-workspace-plan.json', {
        'dataset': spec.dataset, 'snapshot_id': resolved.manifest['snapshot_id'],
        'manifest_sha256': resolved.manifest_sha256,
        'required_bytes_including_reserve': needed, 'temporary_free_bytes': temporary_free,
        'selected_workspace': str(workspace) if workspace is not None else None,
        'physical_admission_still_required': workspace is not None,
    })
    proof = verify_cold_archive(spec, sync_root, verification_root=workspace)
    if (proof['snapshot_id'] != resolved.manifest['snapshot_id']
            or proof['manifest_sha256'] != resolved.manifest_sha256):
        raise SnapshotError('fixed D release changed while selecting recovery workspace')
    return proof


def retire_after_convergence(spec, cfg, target: Path, plan: dict, options: dict):
    """Wait for transient scans without discarding or extending a full audit.

    Only transport observations may change while queued. The canonical apply
    still refreshes every gate and rejects changed generations or expired proof.
    Return None only when the caller must repeat its full audit outside the lock.
    """
    transport_blockers = {'cold-peer-not-converged', 'peer-proof-stale',
                          'peer-proof-missing-timestamp'}
    if set(plan['blockers']) - transport_blockers or not plan.get('full_verification'):
        raise SnapshotError('canonical retirement blocked: ' + str(plan['blockers']))
    verified_at = plan['full_verification']['verified_at_epoch']
    with phase(target, 'wait-for-retirement-owner'):
        with COMMON_OWNER.open('a') as common:
            fcntl.flock(common, fcntl.LOCK_EX)
            age = time.time() - verified_at
            if not 0 <= age <= 1500:
                return None
            # Holding the mutation owner prevents another publisher from
            # starting a new scan while an existing scan finishes. No gate is
            # bypassed: apply probes transport again, including after quarantine.
            peer = _wait_for_convergence(cfg, min(300, max(0, int(1500 - age))))
            atomic_write_json(target / 'retirement-peer-wait.json', {
                'peer_proof': peer, 'original_verified_at_epoch': verified_at,
                'checked_at_epoch': time.time(), 'proof_time_extended': False,
            })
            if not 0 <= time.time() - verified_at <= 1500:
                return None
            if peer.get('ok') is not True:
                raise SnapshotError('canonical retirement transport remains unconverged')
            with phase(target, 'verify-and-retire-hot-source'):
                return apply_legacy_retirement(spec,
                    expected_fingerprint=plan['plan_fingerprint'],
                    owned_verified_plan=plan, **options)


def finish_retirement(spec, cfg, target, retired, *, source_origin=None):
    artifact_root, _mirror, states, _activations = source_layout(source_origin)
    if retired.get('deleted') is not True or (artifact_root / spec.relative_root).exists():
        raise SnapshotError('retirement receipt and source absence differ')
    stage = spec.stage_root / spec.dataset
    with phase(target, 'prune-verified-encoding-stage'):
        if stage.exists():
            prune = plan_archive_stage_prune(spec, cfg.sync_root, artifact_root=artifact_root, state_root=states)
            atomic_write_json(target / 'stage-prune-plan.json', prune)
            pruned = apply_archive_stage_prune(spec, cfg.sync_root, artifact_root=artifact_root,
                state_root=states, expected_fingerprint=prune['stage_fingerprint'], owned_verified_plan=prune)
            atomic_write_json(target / 'stage-pruned.json', pruned)
        else:
            prune = json.loads((target / 'stage-prune-plan.json').read_text())
            pruned = json.loads((target / 'stage-pruned.json').read_text())
            if pruned.get('pruned') is not True:
                raise SnapshotError('missing encoding stage has no completed prune receipt')
            proof = verify_cold_archive(spec, cfg.sync_root)
            if proof['snapshot_id'] != retired['snapshot_id'] or proof['manifest_sha256'] != retired['manifest_sha256']:
                raise SnapshotError('retired exact release changed during handoff')
    accepted = {'dataset': spec.dataset, 'relative_root': spec.relative_root, 'retired': True,
                'snapshot_id': retired['snapshot_id'], 'manifest_sha256': retired['manifest_sha256'],
                'reclaimed_allocated_bytes': retired['reclaimed_allocated_file_bytes'],
                'stage_reclaimed_allocated_bytes': prune['reclaimable_allocated_file_bytes'],
                'cold_objects_deleted': 0, 'nas_snapshots_deleted': 0}
    if source_origin is not None:
        accepted.update(source_origin=source_origin, artifact_root=str(artifact_root))
    atomic_write_json(target / 'acceptance.json', accepted)
    return accepted


def retirement_options(cfg, artifact_root, mirror_root, states, activations):
    return dict(repo_root=ROOT, artifact_root=artifact_root,
        hot_root=mirror_root, sync_root=cfg.sync_root,
        materialized_root=cfg.materialized_root, state_root=states,
        activation_root=activations, backup_config=cfg.backup_config,
        peer_proof=_wait_for_convergence(cfg, 300), peer_probe=lambda: _syncthing(cfg),
        bridge_inactive=_bridge_inactive(Path('/srv/stockagent-artifacts-hot')),
        manual_immediate=True, manual_capture=True)


def _apply(inventory_path: Path, raw: bytes, receipt_dir: Path) -> dict:
    inv = json.loads(raw)
    if (inv.get('schema_version') != 1 or inv.get('contract') != MANUAL_WSL_CAPTURE_CONTRACT
            or inv.get('authority_node_id') != 'penguin' or not inv.get('items')):
        raise SnapshotError('offline inventory contract differs')
    origin = inv.get('source_origin')
    artifact_root, mirror_root, states, activations = source_layout(origin)
    if origin is not None and (inv.get('artifact_root') != str(artifact_root)
                              or inv.get('existing_catalog_sha256')):
        raise SnapshotError('historical mirror inventory changed its fixed physical source')
    reviewed_origin = (reviewed_transfer_quarantine_root if origin==TRANSFER_QUARANTINE_ORIGIN
                       else reviewed_minute_derived_view_root if origin==MINUTE_DERIVED_VIEW_ORIGIN else None)
    if reviewed_origin and any(not reviewed_origin(row['relative_root']) for row in inv['items']):
        raise SnapshotError('frozen separate-source inventory contains an unreviewed physical root')
    existing_specs = {}
    if inv.get('existing_catalog_sha256'):
        if hashlib.sha256(EXISTING_CATALOG.read_bytes()).hexdigest() != inv['existing_catalog_sha256']:
            raise SnapshotError('existing archive catalog changed after inventory')
        existing_specs = load_legacy_specs(EXISTING_CATALOG)
    cfg = RetentionConfig.load(ROOT / 'configs/data_sync/packed_retention.json', repo_root=ROOT)
    cfg = replace(cfg, required_peer_names=load_retirement_peer_names(
        ROOT / 'configs/data_sync/artifact_retirement.json', authority_node_id='penguin'))
    results = []
    for row in inv['items']:
        relative = row['relative_root']
        path = _safe_relative_path(relative, 'frozen offline artifact')
        if (len(path.parts) < 2 or path.parts[0] not in SCOPES
                or (path.parts[0] == 'live' and not reviewed_offline_simulation_root(relative))):
            raise SnapshotError('frozen root is outside the reviewed offline scopes')
        prefix = dataset_prefix(origin)
        dataset = row.get('existing_dataset') or (prefix + hashlib.sha256(
            (relative + row['fingerprint']).encode()).hexdigest()[:24])
        validate_slug(dataset, 'frozen offline dataset')
        if row.get('existing_dataset') and dataset not in existing_specs:
            raise SnapshotError('existing dataset lacks its frozen catalog binding')
        target = receipt_dir / dataset
        target.mkdir(exist_ok=True)
        atomic_write_json(receipt_dir / 'progress.json', {
            'contract': MANUAL_WSL_CAPTURE_CONTRACT,
            'inventory_sha256': hashlib.sha256(raw).hexdigest(),
            'checked_at_epoch': time.time(), 'current_root': relative, 'items': results,
            'selected_items': len(inv['items']), 'all_selected_retired': False,
            'reclaimed_allocated_bytes': sum(r.get('reclaimed_allocated_bytes', 0) for r in results),
        })
        capture_contract = (MANUAL_OFFLINE_SIMULATION_CONTRACT if Path(relative).parts[0] == 'live'
                            else MANUAL_WSL_CAPTURE_CONTRACT)
        if Path(relative).parts[0]=='stockagent-transfer-quarantine':
            if origin!=TRANSFER_QUARANTINE_ORIGIN or not reviewed_transfer_quarantine_root(relative):
                raise SnapshotError('retired transfer capture requires its separate physical source role')
            capture_contract=MANUAL_TRANSFER_QUARANTINE_CONTRACT
        if Path(relative).parts[0]=='data_tw_minute':
            if origin!=MINUTE_DERIVED_VIEW_ORIGIN or not reviewed_minute_derived_view_root(relative):
                raise SnapshotError('retired minute view capture requires its separate physical source role')
            capture_contract=MANUAL_MINUTE_DERIVED_VIEW_CONTRACT
        spec = LegacyArchiveSpec(dataset, relative, 7, Path('/var/lib/stockagent-legacy-archive-stage'),
                                 12, False, 'zstd-1-adaptive-over-8m', capture_contract)
        if row.get('existing_dataset'):
            if dataset not in existing_specs or existing_specs[dataset].relative_root != relative:
                raise SnapshotError('existing archive identity differs from the frozen root')
            spec = existing_specs[dataset]
        prior = target / 'acceptance.json'
        if prior.exists():
            accepted = json.loads(prior.read_text())
            if accepted.get('retired') is True and not (artifact_root / relative).exists():
                results.append(accepted)
                continue
            references = artifact_service_references((artifact_root / relative,), ROOT)[str(artifact_root / relative)]
            if accepted.get('retired') is True and references:
                # A recovered live input must remain hot. Preserve the historical
                # acceptance rather than treating it as permission to retire again.
                results.append({'relative_root': relative, 'retired': False,
                                'retained_for_current_service': True,
                                'service_references': references,
                                'error': 'previously retired root is now required by a current service'})
                continue
            raise SnapshotError('prior retirement requires reconciliation')
        try:
            retirement_receipt = target / 'retirement.json'
            if retirement_receipt.exists() and not (artifact_root / relative).exists():
                accepted = finish_retirement(spec, cfg, target, json.loads(retirement_receipt.read_text()),
                                             source_origin=origin)
                results.append(accepted)
                atomic_write_json(receipt_dir / 'progress.json', {'items':results, 'handoff_reconciled':relative})
                continue
            state_path = states/'retirements'/(dataset+'.json')
            state = json.loads(state_path.read_bytes()) if state_path.is_file() else {}
            if state.get('state') == 'cold-only' and state.get('retirement_receipt'):
                retired = state['retirement_receipt']
                if (retired.get('dataset') != dataset or retired.get('relative_root') != relative
                        or retired.get('snapshot_id') != state.get('snapshot_id')
                        or retired.get('manifest_sha256') != state.get('manifest_sha256')
                        or retired.get('deleted') is not True):
                    raise SnapshotError('canonical retirement state and its durable receipt differ')
                atomic_write_json(retirement_receipt, retired)
                results.append(finish_retirement(spec, cfg, target, retired, source_origin=origin))
                continue
            if state.get('state') == 'retiring':
                original_plan = json.loads((target/'retirement-plan.json').read_bytes())
                with phase(target, 'independent-D-original-recovery-for-quarantine'):
                    proof = verify_for_retirement(spec, cfg.sync_root, target)
                    if any(proof[k] != original_plan[k] for k in ('snapshot_id','manifest_sha256')):
                        raise SnapshotError('interrupted retirement fixed D identity differs')
                atomic_write_json(target/'quarantine-cold-recovery.json',
                                  {k:v for k,v in proof.items() if k != 'manifest'})
                options = retirement_options(cfg, artifact_root, mirror_root, states, activations)
                for attempt in range(3):
                    with phase(target, 'full-quarantine-resume-audit'):
                        plan = plan_legacy_quarantine_resume(spec, original_plan, **options)
                        atomic_write_json(target/'quarantine-resume-plan.json', plan)
                    retired = retire_after_convergence(spec, cfg, target, plan, options)
                    if retired is not None:
                        break
                else:
                    raise SnapshotError('complete quarantine audit repeatedly expired before mutation')
                atomic_write_json(retirement_receipt, retired)
                results.append(finish_retirement(spec, cfg, target, retired, source_origin=origin))
                continue
            if origin is not None and (mirror_root/relative).exists():
                raise SnapshotError('historical mirror has an unexpected secondary source')
            current = observe(artifact_root, relative, repo_root=ROOT)
            drift=(shared_link_observation_drift(row,current)
                   if current['fingerprint'] != row['fingerprint'] else [])
            if current['blockers'] or (current['fingerprint'] != row['fingerprint'] and drift is None):
                raise SnapshotError('offline source changed or has current consumers: ' + str(current['blockers'][:3]))
            if drift:
                atomic_write_json(target/'inventory-shared-link-drift.json',{
                    'before_fingerprint':row['fingerprint'],'current_fingerprint':current['fingerprint'],
                    'changed_observations':drift,'source_bytes_verified':False,
                    'admission_only':True})
            atomic_write_json(target / 'catalog.json', {'schema_version': 1, 'authority_node_id': 'penguin',
                'archives': [{'dataset': dataset, 'relative_root': relative, 'minimum_stable_days': 7,
                              'manual_capture_min_stable_hours': 12, 'durable_staging': False,
                              'compression': spec.compression_profile, 'archive_only': True,
                              'stage_root': str(spec.stage_root), 'capture_contract': spec.capture_contract}]})
            # Admission, stable source hashes, every encoded byte and independent
            # decoding are still owned by the canonical archive implementation.
            # Encoding only mutates this run's private stage. Keep unrelated D
            # publishers progressing while the canonical preparation completes.
            if not row.get('existing_dataset'):
                with phase(target, 'prepare-exact-source-archive'):
                    archive=spec.stage_root/spec.dataset/'archive'
                    if (archive/'legacy_archive_manifest.json').exists():
                        verify_archive_directory(archive,artifact_root/relative,spec=spec,
                                                 manual_capture=True,artifact_root=artifact_root,
                                                 repo_root=ROOT)
                    else:
                        prepare_archive(spec, artifact_root, manual_capture=True, repo_root=ROOT)
                published = reusable_publication(target, spec, cfg.sync_root)
                if published is None:
                    with phase(target, 'publish-to-D'):
                        _check_d_primary_mount(cfg.sync_root)
                        published = publish_archive(spec, artifact_root, cfg.sync_root, repo_root=ROOT,
                            manual_capture=True, pack_buckets=4,
                            publication_owner=lambda: publication_owner(target))
                    atomic_write_json(target / 'publication.json', published)
            with phase(target, 'independent-D-original-recovery'):
                proof = verify_for_retirement(spec, cfg.sync_root, target)
                if not row.get('existing_dataset') and any(
                        proof[k] != published[k] for k in ('snapshot_id', 'manifest_sha256')):
                    raise SnapshotError('fixed D publication changed during independent recovery')
            atomic_write_json(target / 'cold-recovery.json', {k:v for k,v in proof.items() if k != 'manifest'})
            options = retirement_options(cfg, artifact_root, mirror_root, states, activations)
            for attempt in range(3):
                with phase(target, 'full-source-mirror-D-retirement-audit'):
                    plan = plan_legacy_retirement(spec, **options)
                    atomic_write_json(target / 'retirement-plan.json', plan)
                retired = retire_after_convergence(spec, cfg, target, plan, options)
                if retired is not None:
                    break
            else:
                raise SnapshotError('complete retirement audit repeatedly expired before mutation')
            atomic_write_json(target / 'retirement.json', retired)
            accepted = finish_retirement(spec, cfg, target, retired, source_origin=origin)
            results.append(accepted)
        except (OSError, ValueError, SnapshotError) as error:
            results.append({'relative_root': relative, 'retired': False, 'error': str(error)})
            atomic_write_json(target / 'failure.json', results[-1])
        progress = {'contract': MANUAL_WSL_CAPTURE_CONTRACT, 'inventory_sha256': hashlib.sha256(raw).hexdigest(),
                    'checked_at_epoch': time.time(), 'items': results,
                    'reclaimed_allocated_bytes': sum(r.get('reclaimed_allocated_bytes', 0) for r in results),
                    'all_selected_retired': all(r['retired'] for r in results) and len(results) == len(inv['items'])}
        atomic_write_json(receipt_dir / 'progress.json', progress)
        print(json.dumps(results[-1]), flush=True)
    progress = {'contract': MANUAL_WSL_CAPTURE_CONTRACT, 'inventory_sha256': hashlib.sha256(raw).hexdigest(),
                'checked_at_epoch': time.time(), 'items':results,
                'reclaimed_allocated_bytes':sum(r.get('reclaimed_allocated_bytes',0) for r in results),
                'all_selected_retired':all(r['retired'] for r in results) and len(results)==len(inv['items'])}
    atomic_write_json(receipt_dir / 'progress.json', progress)
    return progress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['inventory', 'apply'])
    parser.add_argument('--relative-root', action='append')
    parser.add_argument('--existing-dataset', action='append')
    parser.add_argument('--hot-mirror', action='store_true',
                        help='Inventory explicit unequal retained hot versions with separate identities')
    parser.add_argument('--transfer-quarantine',action='store_true',
                        help='Inventory one explicitly dated retired /srv transport quarantine')
    parser.add_argument('--minute-derived-view',action='store_true',
                        help='Inventory only the two reviewed retired research minute views')
    parser.add_argument('--minute-view-name',action='append',choices=tuple(MINUTE_VIEW_NAMES),
                        help='Select a retired minute view without putting source path operands in an invoker argv')
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--receipt-dir', type=Path)
    parser.add_argument('--wait-for-owner', action='store_true',
                        help='Queue the same frozen manual scope behind its existing cleanup owner')
    parser.add_argument('--retry-rounds', type=int, default=1, choices=(1, 2, 3),
                        help='Bounded fresh retries of the identical frozen scope after a completed round')
    args = parser.parse_args()
    os.umask(0o077)
    if socket.gethostname() != 'penguin':
        parser.error('local retirement is penguin-only')
    if args.command == 'inventory':
        if args.wait_for_owner or args.retry_rounds != 1:
            parser.error('owner wait and retries apply only to the frozen cleanup')
        if args.minute_view_name and (not args.minute_derived_view or args.relative_root or args.existing_dataset):
            parser.error('minute view names require only the separate minute-derived-view role')
        if args.minute_derived_view and not args.relative_root and not args.existing_dataset:
            args.relative_root = [MINUTE_VIEW_NAMES[name] for name in (args.minute_view_name or MINUTE_VIEW_NAMES)]
        if not args.relative_root and not args.existing_dataset:
            parser.error('inventory needs explicit relative roots')
        result = inventory(args.relative_root or [], args.inventory, args.existing_dataset,
                           hot_mirror=args.hot_mirror,transfer_quarantine=args.transfer_quarantine,
                           minute_derived_view=args.minute_derived_view)
        print(json.dumps({'items':len(result['items']), 'blocked':sum(bool(r['blockers']) for r in result['items'])}))
        return 0
    if args.relative_root or args.existing_dataset or args.hot_mirror or args.transfer_quarantine or args.minute_derived_view or args.minute_view_name or not args.receipt_dir:
        parser.error('apply needs the fixed inventory and receipt directory')
    frozen = hashlib.sha256(args.inventory.read_bytes()).hexdigest()
    for round_index in range(args.retry_rounds):
        if hashlib.sha256(args.inventory.read_bytes()).hexdigest() != frozen:
            raise SnapshotError('frozen cleanup inventory changed between retry rounds')
        result = apply(args.inventory, args.receipt_dir, wait_for_owner=args.wait_for_owner)
        if result['all_selected_retired']:
            break
        if round_index + 1 < args.retry_rounds:
            time.sleep((30, 120)[round_index])
    return 0 if result['all_selected_retired'] else 75


if __name__ == '__main__':
    raise SystemExit(main())
