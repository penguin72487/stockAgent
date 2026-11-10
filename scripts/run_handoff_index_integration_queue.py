#!/usr/bin/env python3
"""Schedule immutable source-generation comparisons without modifying data."""
from __future__ import annotations

import fcntl
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import (plan_handoff_index_integration as planner,
                     prepare_handoff_index_archive as source_cli,
                     run_handoff_index_d_queue as d_queue,
                     run_handoff_source_repack_queue as source_queue)
from stockagent.data_sync import packed_snapshots as packed, semantic_link_promotion as promotion, workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.handoff_index_integration_queue.v1'
OWNER_BUSY_RETRY_SECONDS = 15


def _latest_enabled(dataset, source):
    policy = source_queue._metadata(ROOT/'configs/data_sync/fast_latest_generation_migration.json')
    generations = policy.get('generations') if isinstance(policy.get('generations'), dict) else {}
    binding = generations.get(dataset) if isinstance(generations.get(dataset), dict) else {}
    return (policy.get('contract') == 'stockagent.fast_latest_generation_migration.v1'
            and policy.get('allow_uncompared_historical_values') is True
            and isinstance(policy.get('datasets'), list) and dataset in policy['datasets']
            and binding.get('private_manifest_sha256') == source.get('D_publication', {}).get('new_manifest_sha256')
            and isinstance(binding.get('public_manifest_sha256'), str)
            and re.fullmatch('[a-f0-9]{64}', binding['public_manifest_sha256']) is not None)


def _latest_promotion_done(root, dataset, source, *, resolve=None, allow_advanced=False):
    base = root/dataset
    paths = list(base.glob('latest-promotion-*.json')) if base.is_dir() and not base.is_symlink() else []
    if len(paths) > 64:
        raise SnapshotError('latest fixed promotion receipt set exceeds its bound')
    head = (resolve or packed.resolve_latest_packed)(planner.COLD, dataset, require_objects=False).manifest_sha256
    return any(value.get('contract') == 'stockagent.latest_fixed_link_promotion.v1'
               and value.get('state') == 'committed' and (allow_advanced or value.get('new_manifest_sha256') == head)
               and value.get('private_manifest_sha256') == source.get('D_publication', {}).get('new_manifest_sha256')
               and value.get('changed_member_values_compared') is False
               and value.get('old_historical_value_coverage') == 'not_compared'
               for value in (source_queue._metadata(path) for path in paths))


def _latest_public_binding_matches(root, dataset):
    policy = source_queue._metadata(ROOT/'configs/data_sync/fast_latest_generation_migration.json')
    generations = policy.get('generations') if isinstance(policy.get('generations'), dict) else {}
    binding = generations.get(dataset) if isinstance(generations.get(dataset), dict) else {}
    comparison = source_queue._metadata(root/(dataset+'.diff-status.json'))
    return (isinstance(binding.get('public_manifest_sha256'), str)
            and comparison.get('public_manifest_sha256') == binding['public_manifest_sha256'])


def _forex_done(root, source, *, resolve=None):
    receipt = source_queue._metadata(root/'forex-frankfurter/frankfurter-semantic-integration-v1.json')
    if (receipt.get('contract') != 'stockagent.frankfurter_fixed_generation_integration.v1'
            or receipt.get('state') != 'public_generation_integrated'
            or receipt.get('private_original_manifest_sha256') != source.get('D_publication', {}).get('new_manifest_sha256')
            or receipt.get('public_only_rows') != 0 or receipt.get('conflicting_overlap_rows') != 0):
        return False
    resolve = resolve or packed.resolve_latest_packed
    head = resolve(planner.COLD, 'forex-frankfurter', require_objects=False).manifest_sha256
    prior = receipt.get('new_public_manifest_sha256')
    if head == prior:
        return True
    if not isinstance(head, str) or len(head) != 64 or any(c not in '0123456789abcdef' for c in head):
        return False
    repack = source_queue._metadata(root.parent/'source-local-exact-repack/forex-frankfurter'/(head+'.json'))
    return (repack.get('contract') == 'stockagent.source_local_exact_repack.v1'
            and repack.get('dataset') == 'forex-frankfurter' and repack.get('old_manifest_sha256') == prior
            and repack.get('new_manifest_sha256') == head
            and repack.get('state') == 'new_D_head_committed_from_exact_E_source')


def _forex_audits_ready(root, source):
    """Admit only a fully bound, already completed source-generation audit."""
    from scripts import integrate_frankfurter_fixed_generations as fx
    identity = source.get('source_identity')
    if not isinstance(identity, str) or len(identity) != 64 or any(c not in '0123456789abcdef' for c in identity):
        return False
    base = root/'forex-frankfurter'
    plan = source_queue._metadata(base/identity/'integration-diff-v1/diff.json')
    rows = source_queue._metadata(base/'frankfurter-row-audit-v1.json')
    metadata = source_queue._metadata(base/'frankfurter-metadata-audit-v1.json')
    if (plan.get('contract') != 'stockagent.index_generation_diff.v1'
            or plan.get('private_manifest_sha256') != source.get('D_publication', {}).get('new_manifest_sha256')
            or not isinstance(plan.get('counters'), dict) or not isinstance(plan.get('conflict_categories'), dict)
            or not isinstance(plan.get('worklist'), dict)):
        return False
    checked = {'public_manifest_sha256': plan.get('public_manifest_sha256'),
        'private_manifest_sha256': plan.get('private_manifest_sha256'),
        'worklist_sha256': plan['worklist'].get('sha256'),
        'counters': {**plan['counters'], 'conflict_categories': plan['conflict_categories']}}
    try:
        fx.validate_audits(rows, metadata, checked)
        return True
    except (KeyError, ValueError, SnapshotError):
        return False


def _archive_audit_done(root, source):
    name = 'binance-public-archive'
    plan = source_queue._metadata(root/(name+'.diff-status.json'))
    identity = plan.get('comparison_identity_sha256')
    if not isinstance(identity, str) or len(identity) != 64 or any(c not in '0123456789abcdef' for c in identity):
        return False
    receipt = source_queue._metadata(root/name/('row-audit-'+identity+'.json'))
    count = plan.get('conflict_categories', {}).get('parquet')
    return (type(count) is int and count > 0 and receipt.get('contract') == 'stockagent.binance_archive_generation_audit.v1'
            and receipt.get('state') == 'compared' and receipt.get('comparison_identity_sha256') == identity
            and receipt.get('public_manifest_sha256') == plan.get('public_manifest_sha256')
            and receipt.get('private_manifest_sha256') == source.get('D_publication', {}).get('new_manifest_sha256')
            and receipt.get('files_completed') == receipt.get('files_total') == count)


def _archive_needs_audit(root):
    plan = source_queue._metadata(root/'binance-public-archive.diff-status.json')
    count = plan.get('conflict_categories', {}).get('parquet') if isinstance(plan.get('conflict_categories'), dict) else None
    return type(count) is int and count > 0


def _archive_requires_union(root):
    """A private-only promotion cannot preserve public-only member paths."""
    plan = source_queue._metadata(root/'binance-public-archive.diff-status.json')
    counts = plan.get('counters') if isinstance(plan.get('counters'), dict) else {}
    public_only = counts.get('public_only')
    return type(public_only) is int and public_only > 0


def _archive_recent_audit(root, now):
    status = source_queue._metadata(root/'binance-public-archive/row-audit-status.json')
    stamp = status.get('observed_at_epoch')
    return (status.get('contract') == 'stockagent.binance_archive_generation_audit.v1'
            and status.get('state') == 'running' and type(stamp) in (int, float)
            and 0 <= now-stamp <= 30)


def _archive_controls_done(root, source):
    name = 'binance-public-archive'
    plan = source_queue._metadata(root/(name+'.diff-status.json'))
    identity = plan.get('comparison_identity_sha256')
    if not isinstance(identity, str) or len(identity) != 64 or any(c not in '0123456789abcdef' for c in identity):
        return False
    receipt = source_queue._metadata(root/name/('control-audit-'+identity+'.json'))
    return (receipt.get('contract') == 'stockagent.binance_archive_control_audit.v1'
            and receipt.get('state') == 'compared' and receipt.get('comparison_identity_sha256') == identity
            and receipt.get('public_manifest_sha256') == plan.get('public_manifest_sha256')
            and receipt.get('private_manifest_sha256') == source.get('D_publication', {}).get('new_manifest_sha256')
            and receipt.get('files_completed') == receipt.get('files_total') == 5)


def _archive_promotion_done(root, source, *, resolve=None):
    name = 'binance-public-archive'
    base = root/name
    files = list(base.glob('semantic-promotion-*.json')) if base.is_dir() else []
    if len(files) > 64:
        raise SnapshotError('Binance semantic promotion receipt set exceeds its bound')
    head = (resolve or packed.resolve_latest_packed)(planner.COLD, name, require_objects=False).manifest_sha256
    for path in files:
        receipt = source_queue._metadata(path)
        if (receipt.get('contract') == 'stockagent.semantic_link_promotion.v1'
                and receipt.get('state') == 'committed' and receipt.get('new_manifest_sha256') == head
                and receipt.get('private_manifest_sha256') == source.get('D_publication', {}).get('new_manifest_sha256')):
            return True
    return False


def _complete(root, dataset, source, *, resolve=None, policy=None):
    if not d_queue._done(source, dataset):
        return False
    result = source_queue._metadata(root/(dataset+'.diff-status.json'))
    if (result.get('contract') != planner.CONTRACT or result.get('dataset') != dataset
            or result.get('state') != 'metadata_compared'
            or result.get('private_manifest_sha256') != source['D_publication'].get('new_manifest_sha256')
            or result.get('private_snapshot_id') != source['D_publication'].get('new_snapshot_id')
            or result.get('source_payload_read') is not False
            or result.get('semantic_merge_complete') is not False):
        return False
    resolve = resolve or packed.resolve_latest_packed
    policy = policy or planner.policy
    current = resolve(planner.COLD, dataset, require_objects=False)
    _, catalog_sha = policy(dataset)
    return (result.get('public_manifest_sha256') == current.manifest_sha256
            and result.get('catalog_sha256') == catalog_sha)


def run_once(root: Path, *, now=time.time, runner=source_queue.run_observed, code_sha=None,
             resolve=None, policy=None):
    root = Path(root).absolute()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('source integration queue root redirected')
    fd = os.open(root/'integration-queue-owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError('source integration queue already has an owner') from error
        path = root/'integration-queue-status.json'
        previous = source_queue._metadata(path)
        if code_sha is None:
            paths = (Path(__file__), ROOT/'scripts/plan_handoff_index_integration.py',
                     ROOT/'stockagent/data_sync/index_generation_diff.py',
                     ROOT/'scripts/promote_latest_fixed_generation.py',
                     ROOT/'stockagent/data_sync/semantic_link_promotion.py')
            code_sha = hashlib.sha256(b''.join(item.read_bytes() for item in paths)).hexdigest()
        old_rows = (previous.get('entries', []) if previous.get('contract') == CONTRACT
                    and previous.get('implementation_sha256') == code_sha else [])
        cursor = previous.get('last_selected_dataset') if previous.get('contract') == CONTRACT else None
        if not isinstance(cursor, str):
            cursor = None
        prior = {row.get('dataset'): row for row in old_rows if isinstance(row, dict)}
        entries = []
        for dataset, _locks, _supplemental in source_cli.ENROLLED.values():
            source = source_queue._metadata(root/(dataset+'.status.json'))
            old = prior.get(dataset, {})
            retry = old.get('retry_after_epoch', 0)
            if type(retry) not in (int, float) or retry < 0 or retry > now()+86400:
                retry = 0
            finished = old.get('finished_at_epoch')
            if (old.get('error') == 'owner_busy' and type(finished) in (int, float)
                    and 0 <= finished <= now()+120):
                retry = min(retry, finished+OWNER_BUSY_RETRY_SECONDS)
            try:
                work = None
                if dataset in source_cli.PRIVATE_ONLY_DATASETS and d_queue._done(source, dataset):
                    state = 'not_applicable_private_only'
                elif dataset in source_cli.DIRECT_PUBLIC_COVERAGE_DATASETS and d_queue._done(source, dataset):
                    state = 'completed'
                elif not d_queue._done(source, dataset):
                    state = 'waiting_input'
                elif not _complete(root, dataset, source, resolve=resolve, policy=policy):
                    state = 'retrying' if retry > now() else 'waiting'
                elif (dataset == 'forex-frankfurter' and not _forex_done(root, source, resolve=resolve)
                      and _forex_audits_ready(root, source)):
                    state = 'retrying' if retry > now() else 'waiting_semantic'
                    work = 'forex_integration'
                elif (dataset == 'binance-public-archive' and _archive_needs_audit(root)
                      and not _archive_promotion_done(root, source, resolve=resolve)):
                    if not _archive_audit_done(root, source):
                        work = 'archive_rows'
                    elif not _archive_controls_done(root, source):
                        work = 'archive_controls'
                    elif _archive_requires_union(root):
                        # Do not retry a promotion whose own contract rejects
                        # public-only paths. A fresh generation is re-planned
                        # above; no source or receipt is retired here.
                        work = 'archive_union_required'
                    else:
                        work = 'archive_promotion'
                    state = ('blocked_semantic_merge' if work == 'archive_union_required' else
                             'waiting_existing_audit' if work == 'archive_rows' and _archive_recent_audit(root, now()) else
                             'retrying' if retry > now() else 'waiting_semantic')
                elif (dataset in promotion.FAST_LATEST_DATASETS and _latest_enabled(dataset, source)
                      and not _latest_promotion_done(root, dataset, source, resolve=resolve, allow_advanced=True)):
                    if _latest_public_binding_matches(root, dataset):
                        state = 'retrying' if retry > now() else 'waiting_semantic'
                        work = 'latest_fixed_promotion'
                    else:
                        state = 'waiting_generation_changed'
                else:
                    state = 'completed'
                error = old.get('error') if state == 'retrying' else None
            except (OSError, ValueError, SnapshotError) as exc:
                state, error, retry = 'retrying', type(exc).__name__, now()+source_queue.RETRY_SECONDS
            entry = {'dataset': dataset, 'state': state,
                     'retry_after_epoch': retry if state == 'retrying' else None}
            if work:
                entry['work'] = work
            if error:
                entry['error'] = error
            entries.append(entry)
        def save():
            atomic_write_json(path, {'contract': CONTRACT, 'implementation_sha256': code_sha,
                'observed_at_epoch': now(), 'owner_pid': os.getpid(), 'entries': entries,
                'last_selected_dataset': cursor}, durable=True)
        # A repeatedly busy dataset must not monopolize the next timer tick
        # just because its retry deadline expired before the timer fired.
        # Rotate over independent comparison and promotion work while keeping
        # their common lock and all per-dataset gates unchanged.
        names = [row['dataset'] for row in entries]
        offset = (names.index(cursor) + 1) % len(entries) if entries and cursor in names else 0
        ordered = entries[offset:] + entries[:offset]
        selected = next((row for row in ordered if row['state'] in {'waiting', 'waiting_semantic'}), None)
        if selected is None:
            save()
            return entries
        semantic = selected['state'] == 'waiting_semantic'
        cursor = selected['dataset']
        selected.update(state='running', started_at_epoch=now(), retry_after_epoch=None)
        save()
        semantic_script = {'forex_integration': 'integrate_frankfurter_fixed_generations.py',
            'archive_rows': 'audit_binance_archive_generation.py',
            'archive_controls': 'audit_binance_archive_controls.py',
            'archive_promotion': 'promote_binance_archive_semantic.py',
            'latest_fixed_promotion': 'promote_latest_fixed_generation.py'}.get(selected.get('work'))
        if semantic and semantic_script is None:
            raise SnapshotError('semantic integration work is not enrolled')
        command = ([sys.executable, str(ROOT/'scripts'/semantic_script)]
                   if semantic else [sys.executable, str(ROOT/'scripts/plan_handoff_index_integration.py'),
                                     '--dataset', selected['dataset']])
        if selected.get('work') == 'latest_fixed_promotion':
            command.extend(['--dataset', selected['dataset']])
        try:
            result = runner(command, on_heartbeat=save, timeout=source_queue.CHILD_TIMEOUT_SECONDS)
            source = source_queue._metadata(root/(selected['dataset']+'.status.json'))
            accepted = ({'forex_integration': lambda: _forex_done(root, source, resolve=resolve),
                         'archive_rows': lambda: _archive_audit_done(root, source),
                         'archive_controls': lambda: _archive_controls_done(root, source),
                         'archive_promotion': lambda: _archive_promotion_done(root, source, resolve=resolve),
                         'latest_fixed_promotion': lambda: _latest_promotion_done(root, selected['dataset'], source, resolve=resolve)}[selected['work']]()
                        if semantic else _complete(root, selected['dataset'], source, resolve=resolve, policy=policy))
            if result.returncode == 0 and accepted:
                selected.update(state='completed')
            else:
                reason = ('owner_busy' if result.returncode and 'owner busy' in result.stderr.lower()
                          else 'worker_failed' if result.returncode else 'completion_receipt_missing')
                delay = OWNER_BUSY_RETRY_SECONDS if reason == 'owner_busy' else source_queue.RETRY_SECONDS
                selected.update(state='retrying', retry_after_epoch=now()+delay, error=reason)
                if result.returncode:
                    print(result.stderr[-4096:], file=sys.stderr)
        except (OSError, ValueError, SnapshotError, subprocess.TimeoutExpired) as error:
            selected.update(state='retrying', retry_after_epoch=now()+source_queue.RETRY_SECONDS,
                            error=type(error).__name__)
        selected['finished_at_epoch'] = now()
        save()
        return entries
    finally:
        os.close(fd)


def main():
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'
    print({'contract': CONTRACT, 'entries': run_once(root)})


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
