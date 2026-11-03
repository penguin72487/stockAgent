#!/usr/bin/env python3
"""Read-only preflight for exact offline-retirement cohorts.

The existing owner executes each item; this planner neither writes nor deletes
data and never turns a capacity estimate or an old receipt into permission.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import retire_local_offline_artifacts as owner

RESERVE = 32 * 1024**3
PER_FILE_FORECAST = 65536
CONTRACT = 'stockagent.wsl_retirement_wave_plan.v1'


def _overlap(a: Path, b: Path) -> bool:
    return a == b or a in b.parents or b in a.parents


def _owner_lock_observation(receipt: Path) -> str:
    path = receipt / 'owner.lock'
    try:
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return 'not_observed'
    except OSError:
        return 'unobservable'
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return 'unobservable'
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 'busy'
        fcntl.flock(fd, fcntl.LOCK_UN)
        return 'not_busy_at_observation'
    finally:
        os.close(fd)


def observe_active_supervisors() -> list[dict] | None:
    """Observe active agent units, including commands waiting to start a cohort.

    Other supervisors and independent processes are deliberately outside this
    bounded observation; absence here is not a process-reference proof.
    """
    try:
        run = subprocess.run(['systemctl', 'list-units', 'stockagent-agent-*',
                              '--type=service', '--state=active,activating,reloading',
                              '--plain', '--no-legend'], capture_output=True, text=True,
                             check=True, timeout=10)
        active = []
        for line in run.stdout.splitlines():
            unit = line.split()[0]
            match = re.fullmatch(r'stockagent-agent-([A-Za-z0-9_-]+)\.service', unit)
            if not match:
                return None
            run_id = match.group(1)
            path = ROOT / 'artifacts/operations/agent-workflow/runs' / run_id / 'run.json'
            data = json.loads(path.read_bytes())
            if data.get('run_id') != run_id or not isinstance(data.get('argv'), list):
                return None
            active.append({'run_id': run_id, 'argv': data['argv']})
        return active
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def _owned_quarantine_candidate(data: dict, item: dict, receipt: Path) -> dict | None:
    """Recognize an interrupted transaction for scheduling, not for deletion.

    This checks only the small, fixed identity records and quarantine layout.
    The existing retirement owner must independently revalidate the cold
    release, all source bytes, references, transport and final unlink gates.
    """
    try:
        if data.get('source_origin') is not None or item.get('existing_dataset'):
            return None
        relative = str(owner._safe_relative_path(item['relative_root'], 'frozen offline artifact'))
        fingerprint = item['fingerprint']
        if not isinstance(fingerprint, str) or not re.fullmatch(r'[0-9a-f]{64}', fingerprint):
            return None
        link_policy = data.get('symlink_policy')
        dataset = owner.dataset_prefix(None) + hashlib.sha256(
            (relative + fingerprint + ('\0' + link_policy if link_policy else '')).encode()
        ).hexdigest()[:24]
        owner.validate_slug(dataset, 'frozen offline dataset')
        artifact, hot, states, _ = owner.source_layout(None)
        source, mirror = artifact.resolve() / relative, hot.resolve() / relative
        if (source.exists() or source.is_symlink() or mirror.exists() or mirror.is_symlink()
                or source.resolve(strict=False) != source or mirror.resolve(strict=False) != mirror):
            return None
        state_path = states / 'retirements' / (dataset + '.json')
        plan_path = receipt / dataset / 'retirement-plan.json'
        if (state_path.resolve(strict=False) != state_path.absolute()
                or plan_path.resolve(strict=False) != plan_path.absolute()
                or not state_path.is_file() or not plan_path.is_file()):
            return None
        state, plan = json.loads(state_path.read_bytes()), json.loads(plan_path.read_bytes())
        if (state.get('state') != 'retiring' or state.get('dataset') != dataset
                or state.get('relative_root') != relative or plan.get('dataset') != dataset
                or plan.get('relative_root') != relative or plan.get('source') != str(source)
                or plan.get('hot_tree') != str(mirror) or plan.get('manual_immediate') is not True
                or plan.get('manual_capture') is not True):
            return None
        for key in ('plan_fingerprint', 'snapshot_id', 'manifest_sha256'):
            if not isinstance(plan.get(key), str) or not plan[key] or state.get(key) != plan[key]:
                return None
        if not re.fullmatch(r'[0-9a-f]{64}', plan['manifest_sha256']):
            return None
        binding = plan.get('full_verification', {}).get('binding', {})
        if binding.get('dataset') != dataset or 'hot' not in binding:
            return None
        quarantine = Path(state.get('quarantine', ''))
        parent = states / 'retirements' / 'quarantine' / dataset
        expected = {'source'} | ({'hot'} if binding['hot'] != 'absent' else set())
        if (quarantine.parent != parent or quarantine.resolve(strict=False) != quarantine
                or not quarantine.is_dir() or quarantine.is_symlink()
                or {p.name for p in parent.iterdir()} != {quarantine.name}
                or {p.name for p in quarantine.iterdir()} != expected
                or any(not (quarantine / name).is_dir() or (quarantine / name).is_symlink()
                       or (quarantine / name).resolve() != quarantine / name
                       or (quarantine / name).stat().st_dev != states.stat().st_dev
                       for name in expected)):
            return None
        return {'dataset': dataset, 'path': str(quarantine),
                'manifest_sha256': plan['manifest_sha256'],
                'requires_fresh_owner_proofs': True}
    except (OSError, ValueError, KeyError, TypeError, AttributeError, owner.SnapshotError):
        return None


def inspect_cohort(inventory: Path, receipt: Path, active_runs: list[dict] | None = None,
                   *, fixed_d_stage_only: bool = False, d_stage_recovery_only: bool = False) -> dict:
    raw = inventory.read_bytes()
    data = json.loads(raw)
    if (data.get('schema_version') != 1 or data.get('contract') != owner.MANUAL_WSL_CAPTURE_CONTRACT
            or data.get('authority_node_id') != 'penguin' or not isinstance(data.get('items'), list)
            or not data['items']):
        raise ValueError(f'invalid frozen inventory: {inventory}')
    link_policy = data.get('symlink_policy')
    if link_policy is not None and (link_policy != owner.OPAQUE_SYMLINK_POLICY
            or data.get('source_origin') is not None or len(data['items']) != 1
            or data['items'][0].get('symlink_policy') != link_policy
            or not isinstance(data['items'][0].get('symlinks'), list)
            or data.get('existing_catalog_sha256') is not None
            or data['items'][0].get('existing_dataset')
            or not data['items'][0].get('relative_root', '').startswith('benchmarks/')
            or not owner.reviewed_wsl_artifact_root(data['items'][0]['relative_root'])):
        raise ValueError('invalid frozen symlink capture scope')
    if link_policy is None and any('symlinks' in item or 'symlink_policy' in item for item in data['items']):
        raise ValueError('frozen symlinks lack their explicit policy')
    source_root = owner.source_layout(data.get('source_origin'))[0]
    stage, _ = owner.stage_profile(data)
    if d_stage_recovery_only and (fixed_d_stage_only
            or stage not in (owner.D_BENCHMARK_STAGE_ROOT, owner.D_OFFLINE_STAGE_ROOT)
            or len(data['items']) != 1 or data.get('source_origin') is not None
            or data['items'][0].get('existing_dataset')):
        raise ValueError('D recovery mode requires one reviewed guarded-D cohort')
    sources, old_blockers = [], []
    logical, files, allocated = 0, 0, 0
    for item in data['items']:
        relative = owner._safe_relative_path(item['relative_root'], 'frozen offline artifact')
        source = source_root / relative
        present = source.exists() or source.is_symlink()
        sources.append({'relative_root': str(relative), 'path': str(source),
                        'resolved_path': str(source.resolve(strict=False)), 'present': present,
                        'quarantine_resume_candidate': (None if present else
                            _owned_quarantine_candidate(data, item, receipt))})
        for key in ('logical_bytes', 'files', 'allocated_unique_file_bytes'):
            if type(item.get(key)) is not int or item[key] < 0:
                raise ValueError(f'invalid {key} in {inventory}')
        logical += item['logical_bytes']
        files += item['files']
        allocated += item['allocated_unique_file_bytes']
        old_blockers.extend(item.get('blockers', []))
    digest = hashlib.sha256(raw).hexdigest()
    progress = receipt / 'progress.json'
    historical = 'absent'
    if progress.is_file() and not progress.is_symlink():
        try:
            previous = json.loads(progress.read_bytes())
            if previous.get('inventory_sha256') != digest:
                historical = 'different_inventory_do_not_reuse_without_reconciliation'
            elif previous.get('all_selected_retired') is True:
                historical = ('retired_and_source_absent' if not any(s['present'] for s in sources)
                              else 'retired_but_source_present_reconcile')
            else:
                historical = 'not_retired_or_interrupted'
        except (OSError, ValueError):
            historical = 'unreadable_do_not_reuse_without_reconciliation'
    needles = {str(inventory.absolute()), str(receipt.absolute())}
    for path in (inventory.absolute(), receipt.absolute()):
        if path.is_relative_to(ROOT):
            needles.add(str(path.relative_to(ROOT)))
    matching_runs = (None if active_runs is None else
                     [run['run_id'] for run in active_runs
                      if any(needle in str(arg) for arg in run['argv'] for needle in needles)])
    fixed_publication = None
    fixed_error = None
    if fixed_d_stage_only and historical != 'retired_and_source_absent':
        try:
            if len(data['items']) != 1:
                raise ValueError('fixed-stage mode admits exactly one frozen cohort item')
            item = data['items'][0]
            relative = item['relative_root']
            dataset = item.get('existing_dataset') or (owner.dataset_prefix(data.get('source_origin')) +
                      hashlib.sha256((relative + item['fingerprint'] +
                                      ('\0' + link_policy if link_policy else '')).encode()).hexdigest()[:24])
            owner.validate_slug(dataset, 'frozen offline dataset')
            spec = owner.LegacyArchiveSpec(dataset, relative, 7, stage)
            cfg = owner.RetentionConfig.load(ROOT / 'configs/data_sync/packed_retention.json', repo_root=ROOT)
            published = owner.require_existing_fixed_d_stage(spec, cfg.sync_root, receipt / dataset)
            if (not isinstance(published.get('manifest_sha256'), str)
                    or not re.fullmatch(r'[0-9a-f]{64}', published['manifest_sha256'])
                    or not isinstance(published.get('snapshot_id'), str)):
                raise ValueError('fixed publication has invalid identity')
            resolved = owner.resolve_latest_packed(cfg.sync_root, dataset)
            if resolved.manifest_sha256 != published['manifest_sha256']:
                raise ValueError('fixed release changed during capacity observation')
            archive = resolved.manifest['archive']
            cold_items = [archive['inventory'], *archive['objects']]
            if any(type(item.get('bytes')) is not int or item['bytes'] < 0 for item in cold_items):
                raise ValueError('fixed release has invalid cold object sizes')
            fixed_publication = {key: published[key] for key in ('dataset', 'snapshot_id', 'manifest_sha256')}
            # The native packed reader may still stage one cold object on C;
            # its separate admission and reserve continue to apply.
            fixed_publication['max_temporary_object_bytes'] = max(item['bytes'] for item in cold_items)
        except (OSError, ValueError, TypeError, KeyError, owner.SnapshotError) as error:
            fixed_error = f'{type(error).__name__}: {error}'
    argv = ['/bin/bash', 'scripts/run_authority_storage_operation.sh',
            'scripts/retire_local_offline_artifacts.py', 'apply',
            '--inventory', str(inventory.absolute()), '--receipt-dir',
            str(receipt.absolute()), '--wait-for-owner']
    if fixed_publication is not None:
        argv += ['--fixed-d-stage-only', '--expected-fixed-manifest-sha256',
                 fixed_publication['manifest_sha256']]
    if d_stage_recovery_only:
        argv += ['--d-stage-recovery-only']
    return {'inventory': str(inventory.absolute()), 'inventory_sha256': digest,
            'receipt_directory': str(receipt.absolute()), 'sources': sources,
            'stage_root': str(stage), 'logical_bytes_frozen': logical,
            'allocated_unique_file_bytes_frozen': allocated, 'files_frozen': files,
            'old_inventory_blockers': old_blockers,
            'observed_receipt_owner': _owner_lock_observation(receipt),
            'observed_active_agent_runs': matching_runs,
            'historical_progress': historical,
            'fixed_d_stage_only': fixed_d_stage_only, 'fixed_d_publication': fixed_publication,
            'd_stage_recovery_only': d_stage_recovery_only,
            'fixed_d_preflight_error': fixed_error, 'canonical_argv': argv}


def capacity_observation() -> dict:
    paths = {'wsl': Path('/'), 'windows_c': Path('/mnt/c'),
             'primary_d': Path('/srv/stockagent-packed')}
    result = {}
    for label, path in paths.items():
        if label != 'wsl' and not os.path.ismount(path):
            result[label] = None
            continue
        try:
            result[label] = shutil.disk_usage(path).free
        except OSError:
            result[label] = None
    return result


def build_plan(cohorts: list[dict], parallelism: int, free: dict, *,
               use_backup_reserve: bool = False) -> dict:
    if type(use_backup_reserve) is not bool or (use_backup_reserve and any(
            not (c.get('fixed_d_stage_only') or c.get('d_stage_recovery_only')) for c in cohorts)):
        raise ValueError('backup reserve use requires an explicit guarded-D recovery mode')
    if not 1 <= parallelism <= 8:
        raise ValueError('parallelism must be between 1 and 8')
    conflicts = []
    paths = []
    receipts = []
    for i, cohort in enumerate(cohorts):
        r = Path(cohort['receipt_directory']).resolve(strict=False)
        for j, other in receipts:
            if _overlap(r, other):
                conflicts.append({'kind': 'receipt_directories_overlap', 'cohorts': [j, i]})
        receipts.append((i, r))
        for src in cohort['sources']:
            p = Path(src['resolved_path'])
            for j, other in paths:
                if _overlap(p, other):
                    conflicts.append({'kind': 'source_roots_overlap', 'cohorts': [j, i]})
            paths.append((i, p))
    eligible = []
    for i, cohort in enumerate(cohorts):
        reasons = []
        if cohort['old_inventory_blockers']:
            reasons.append('frozen_inventory_had_blockers_reinventory_first')
        if cohort['observed_receipt_owner'] in ('busy', 'unobservable'):
            reasons.append('receipt_owner_busy_or_unobservable')
        if cohort.get('observed_active_agent_runs') is None:
            reasons.append('active_agent_supervisors_unobservable')
        elif cohort['observed_active_agent_runs']:
            reasons.append('active_or_waiting_agent_run_already_mentions_cohort')
        if not all(source['present'] or source.get('quarantine_resume_candidate')
                   for source in cohort['sources']):
            reasons.append('one_or_more_sources_absent_or_already_retired')
        if cohort['historical_progress'] in ('retired_but_source_present_reconcile',
                'different_inventory_do_not_reuse_without_reconciliation',
                'unreadable_do_not_reuse_without_reconciliation'):
            reasons.append('receipt_requires_reconciliation')
        if cohort.get('fixed_d_stage_only') and cohort['historical_progress'] != 'retired_and_source_absent':
            publication = cohort.get('fixed_d_publication')
            if (not isinstance(publication, dict)
                    or type(publication.get('max_temporary_object_bytes')) is not int
                    or publication['max_temporary_object_bytes'] < 0):
                reasons.append('fixed_d_stage_or_publication_unavailable')
        if any(i in conflict['cohorts'] for conflict in conflicts):
            reasons.append('cohort_overlaps_another_input')
        cohort['preflight_deferrals'] = reasons
        if not reasons:
            eligible.append(i)
    waves = []
    for start in range(0, len(eligible), parallelism):
        indexes = eligible[start:start + parallelism]
        logical = sum(cohorts[i]['logical_bytes_frozen'] for i in indexes)
        files = sum(cohorts[i]['files_frozen'] for i in indexes)
        # Deliberately a heuristic, not a hard reservation or a proof of the
        # maximum. Other writers can still consume space after observation.
        forecast = logical + files * PER_FILE_FORECAST
        # An explicitly pinned, existing D stage cannot fall back to creating
        # any source-sized C workspace. Keep the same conservative per-file
        # control budget and the largest possible individual cold-object
        # scratch; only omit the complete C recovery tree.
        # New guarded-D recovery mode also forbids replacement C trees. Its
        # optional native object reader retains physical admission and falls
        # back to verified D reads, so that SSD payload is not mandatory.
        local_forecast = sum(
            cohorts[i]['files_frozen'] * PER_FILE_FORECAST +
            (cohorts[i]['fixed_d_publication']['max_temporary_object_bytes']
             if cohorts[i].get('fixed_d_stage_only') else
             0 if cohorts[i].get('d_stage_recovery_only') else cohorts[i]['logical_bytes_frozen'])
            for i in indexes)
        # The user earmarked this pool for backup/verification/retirement.
        # An explicitly enrolled maintenance wave may spend it, while still
        # admitting its entire forecast against both real backing volumes.
        # Ordinary callers retain the original floor; this is no free-space
        # credit and does not change any downloader's reserve.
        retained_reserve = 0 if use_backup_reserve else RESERVE
        workspace = {'wsl': local_forecast, 'windows_c': local_forecast,
                     'primary_d': 2 * forecast}
        required = {volume: amount + retained_reserve for volume, amount in workspace.items()}
        blockers = [key for key, value in required.items()
                    if free.get(key) is None or free[key] < value]
        waves.append({'cohorts': indexes, 'required_bytes_heuristic': required,
                      'workspace_bytes_heuristic': workspace,
                      'retained_reserve_bytes': retained_reserve,
                      'capacity_deferrals': blockers,
                      'capacity_is_reserved': False,
                      'local_forecast_mode': ('guarded_d_encoding_recovery_only'
                          if all(cohorts[i].get('d_stage_recovery_only') for i in indexes) else
                          'fixed_d_stage_control_only'
                          if all(cohorts[i].get('fixed_d_stage_only') for i in indexes)
                          else 'source_and_controls_or_mixed')})
    return {'contract': CONTRACT, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'parallelism': parallelism, 'cohorts': cohorts, 'overlap_conflicts': conflicts,
            'observed_free_bytes': free, 'waves': waves,
            'capacity_reserve_policy': 'use_backup_reserve' if use_backup_reserve else 'retain_reserve',
            'backup_reserve_pool_bytes': RESERVE,
            'fresh_source_consumer_and_cold_proofs_required': True,
            'other_supervisors_and_writers_not_excluded': True,
            'authorizes_launch_or_retirement': False,
            'explanation': 'Planning only. Check active and queued supervisors, capacity and '
                           'canonical gates again. Execute only through the existing owner.'}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cohort', action='append', nargs=2, metavar=('INVENTORY', 'RECEIPT'), required=True)
    parser.add_argument('--parallelism', type=int, default=1)
    parser.add_argument('--fixed-d-stage-only', action='store_true',
                        help='Plan only existing pinned D stages without source-sized local workspaces')
    parser.add_argument('--d-stage-recovery-only', action='store_true',
                        help='Prepare on guarded D and disallow full local recovery-tree allocation')
    parser.add_argument('--use-backup-reserve', action='store_true',
                        help='Spend the maintenance reserve on this explicitly selected guarded-D recovery')
    args = parser.parse_args()
    active_runs = observe_active_supervisors()
    options = {'fixed_d_stage_only': True} if args.fixed_d_stage_only else {}
    if args.d_stage_recovery_only:
        options['d_stage_recovery_only'] = True
    cohorts = [inspect_cohort(Path(inv), Path(receipt), active_runs, **options) for inv, receipt in args.cohort]
    print(json.dumps(build_plan(cohorts, args.parallelism, capacity_observation(),
                                use_backup_reserve=args.use_backup_reserve),
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
