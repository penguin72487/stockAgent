#!/usr/bin/env python3
"""Drain a small bounded batch of exact D retirements independently of E builds."""
from contextlib import ExitStack
from pathlib import Path
import fcntl
import json
import os
import re
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.run_source_local_repack_queue import (
    discover_pending_retirements, positive_pin_hint_applies, retirement_cooldown_active, run_one_retirement,
)
from scripts import retire_staged_cold_objects
from stockagent.data_sync import (packed_retention, packed_snapshots, repack_retirement, retirement_turn,
                                  staged_head_capacity_relief, stream_repack,
                                  superseded_repack_history, workspace_volume)
from stockagent.data_sync.backup_stream import _private_catalog_generation
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

MAX_PIN_PREFETCH_CANDIDATES = 128
MAX_PIN_PREFETCH_PREPARED_BYTES = 16 * 1024**2
MAX_PIN_PREFETCH_SINGLE_PREPARED_BYTES = 1024**2


def _foreign_head_recovery_pending(root):
    """Yield between retirements for a local head rollback or pin finalization.

    An intact moved head waiting on its peer keeps its own durable pin and
    must not stall unrelated D retirement work.
    """
    directory = Path(root).parent/'authoritative-head-handoffs'
    try:
        if not directory.exists():
            return False
        if directory.is_symlink() or directory.resolve(strict=True) != directory:
            return False
        journals = list(directory.glob('*/*/journal.json'))
        if len(journals) > 4096:
            return False
        for path in journals:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
                continue
            value = json.loads(path.read_text())
            if not isinstance(value, dict):
                continue
            request = value.get('request')
            if (value.get('contract') != 'stockagent.authoritative_head_handoff.v1'
                    or value.get('identity_sha256') != stream_repack._identity(value)
                    or not isinstance(request, dict)):
                continue
            name = request.get('dataset')
            digest = request.get('old_manifest_sha256')
            if (not isinstance(name, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', name)
                    or not isinstance(digest, str) or not re.fullmatch(r'[a-f0-9]{64}', digest)
                    or path.parent != directory/name/digest):
                continue
            if value.get('state') in ('planned', 'peer_confirmed'):
                return True
            if value.get('state') == 'moved' and (Path('/srv/stockagent-packed/heads')/name/'vastai1T.json').exists():
                return True
    except (OSError, ValueError, TypeError):
        return False
    return False


def _priority_handoff_pending(root, *, now=time.time):
    """Give a sealed E handoff a turn between D retirement transactions.

    Old or unrecognized observations never stop the existing retirement owner.
    This is scheduling only: the original locks and publication checks remain
    authoritative when the handoff tries to acquire them.
    """
    if _foreign_head_recovery_pending(root):
        return True
    queue_root = Path(root).parent/'handoff-index-archive-v1'
    path = queue_root/'D-queue-status.json'
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 128*1024:
            return False
        value = json.loads(path.read_bytes())
        observed = value.get('observed_at_epoch')
        rows = value.get('entries')
        if (value.get('contract') != 'stockagent.handoff_index_private_D_queue.v1'
                or type(observed) not in (int, float) or not -5 <= now()-observed <= 180
                or not isinstance(rows, list) or len(rows) > 64):
            return False
        for row in rows:
            if not isinstance(row, dict):
                continue
            pending = (row.get('state') in {'waiting', 'running'} or
                       row.get('state') == 'retrying' and row.get('error') == 'owner_busy'
                       and row.get('owner_lock') == 'publish_retention_global')
            name = row.get('dataset')
            if not pending or not isinstance(name, str) or re.fullmatch(r'[a-z0-9][a-z0-9-]{0,99}', name) is None:
                continue
            status = queue_root/(name+'.status.json')
            if status.is_symlink() or not status.is_file() or status.stat().st_size > 128*1024:
                continue
            proof = json.loads(status.read_bytes())
            if (proof.get('contract') == 'stockagent.handoff_index_archive.v1'
                    and proof.get('dataset') == name and proof.get('C_payload_deleted') is True
                    and proof.get('D_E_stage_cleanup_complete') is not True):
                return True
    except (OSError, ValueError, TypeError, AttributeError):
        return False
    return False


def _last_publication_contention(root):
    """Use only the immediately recorded common-owner failure as a hint."""
    path = Path(root)/'retirement-queue-status.json'
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 4*1024*1024:
            return False
        status = json.loads(path.read_bytes())
        if not isinstance(status, dict):
            return False
        attempt = status.get('last_attempt')
        return (status.get('contract') == 'stockagent.source_local_repack_queue.v1.retirement-status'
                and isinstance(attempt, dict) and attempt.get('state') == 'waiting_retry'
                and attempt.get('reason') == 'd_e_owner_busy'
                and attempt.get('owner_lock') == 'publish_retention_global')
    except (OSError, ValueError, TypeError):
        return False


def _publication_owner_busy(cold):
    """Probe the existing inode without creating or retaining any owner."""
    try:
        with retire_staged_cold_objects._existing_lock(
                Path(cold)/'.local-state/locks/publish-retention-global.lock'):
            return False
    except (OSError, SnapshotError):
        return True


def _other_retirement_owner_busy():
    """A replication or backup owner may block every retirement candidate."""
    for path in (Path('/var/lib/stockagent/backup-stream/cycle.lock'),
                 Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')):
        try:
            with retire_staged_cold_objects._existing_lock(path):
                pass
        except (OSError, SnapshotError):
            return True
    return False


def _last_external_owner_contention(root):
    path = Path(root)/'retirement-queue-status.json'
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 4*1024*1024:
            return False
        status = json.loads(path.read_bytes())
        attempt = status.get('last_attempt')
        return (status.get('contract') == 'stockagent.source_local_repack_queue.v1.retirement-status'
                and isinstance(attempt, dict) and attempt.get('state') == 'waiting_retry'
                and attempt.get('reason') in {'retirement_source_replication_busy', 'retirement_backup_cycle_busy'})
    except (OSError, ValueError, TypeError, AttributeError):
        return False


def run_batch(root, *, run=run_one_retirement, monotonic=time.monotonic,
              priority_pending=_priority_handoff_pending, publication_busy=None, sleep=time.sleep,
              max_attempts=16, max_seconds=180):
    """Reuse the existing owner and per-item admission without an idle minute per item."""
    start = monotonic()
    attempted = 0
    while attempted < max_attempts and (not attempted or monotonic()-start < max_seconds):
        if priority_pending(root):
            break
        if not run(root):
            break
        attempted += 1
        if _last_external_owner_contention(root):
            # Every candidate needs that same external owner. One observed
            # failure is sufficient until the next timer, and does not block
            # independent E builds or D publishers for a fruitless batch.
            break
        if publication_busy is not None and _last_publication_contention(root):
            # All retirements need this same lock. Give its actual owner a
            # short chance to finish instead of spawning many identical failed
            # children. No lock is retained; each child still reacquires it.
            for _ in range(5):
                if priority_pending(root) or monotonic()-start >= max_seconds:
                    return attempted
                if not publication_busy():
                    break
                sleep(1)
            else:
                break
    return attempted


def _retirement_work_pending(root, *, now=time.time):
    """Only reserve a turn for a recent queue with actionable candidates."""
    path = Path(root)/'retirement-queue-status.json'
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 4*1024*1024:
            return False
        value = json.loads(path.read_bytes())
        observed, rows = value.get('observed_at_epoch'), value.get('entries')
        if (value.get('contract') != 'stockagent.source_local_repack_queue.v1.retirement-status'
                or type(observed) not in (int, float) or not -5 <= now()-observed <= 180
                or not isinstance(rows, list) or len(rows) > 4096):
            return False
        return any(isinstance(row, dict) and (row.get('state') == 'waiting'
            or row.get('state') == 'waiting_retry' and row.get('reason') == 'd_e_owner_busy'
            and type(row.get('retry_after_epoch')) in (int, float)
            and row['retry_after_epoch'] <= now()+60) for row in rows)
    except (OSError, ValueError, TypeError, AttributeError):
        return False


def run_pressure_turn(root, cold, workspace, *, run=run_batch, busy=None, other_busy=None, guard=lambda: None,
                      monotonic=time.monotonic, sleep=time.sleep, free_bytes=None, wait_seconds=600,
                      observe=lambda _state: None):
    """Give retirement a bounded turn after the current D writer finishes.

    This only coordinates scheduling. Every child still independently obtains
    its original publication locks and checks pins, references and identities.
    A sole E recovery and a higher-priority handoff always get precedence.
    """
    free = shutil.disk_usage(cold).free if free_bytes is None else free_bytes
    if type(free) is not int or free < 0 or type(wait_seconds) not in (int, float) or wait_seconds < 0:
        raise ValueError('invalid retirement turn capacity or wait')
    other_busy = _other_retirement_owner_busy if other_busy is None else other_busy
    # E-only recovery must not wait for a reserved retirement turn. It can
    # nevertheless hold the common publication lock for minutes while it
    # streams back to D; keep the ordinary bounded contention probe active
    # on this path as well so one timer does not launch 16 doomed children.
    probe = busy if busy is not None else lambda: _publication_owner_busy(Path(cold))
    if (free >= retirement_turn.PRESSURE_BYTES or not _retirement_work_pending(root)
            or staged_head_capacity_relief.E_only_pending_generations(workspace) or other_busy()):
        return run(root, publication_busy=probe)
    def report(state):
        try:
            observe(state)
        except (OSError, ValueError, TypeError, SnapshotError):
            pass  # Observability cannot authorize or stop the canonical owner.
    try:
        with retirement_turn.reservation(root):
            start = monotonic()
            priority_checks = 0
            priority_pending = False
            def higher_priority(*, force=False):
                nonlocal priority_checks, priority_pending
                priority_checks += 1
                if force or priority_checks == 1 or priority_checks % 5 == 0:
                    priority_pending = _priority_handoff_pending(root)
                return priority_pending
            try:
                while True:
                    guard()
                    if higher_priority():
                        return 0
                    owner = ExitStack()
                    try:
                        owner.enter_context(retirement_turn.turn(root, retirement=True))
                    except packed_snapshots.PackedPublicationBusy:
                        owner.close()
                        report('waiting_writer')
                        if monotonic()-start >= wait_seconds:
                            return 0
                        sleep(1)
                        continue
                    with owner:
                        while probe() and monotonic()-start < wait_seconds:
                            report('waiting_writer')
                            guard()
                            if higher_priority():
                                return 0
                            sleep(1)
                        guard()
                        if probe() or higher_priority(force=True):
                            return 0
                        report('retiring')
                        return run(root, publication_busy=probe)
            finally:
                report('released')
    except packed_snapshots.PackedPublicationBusy:
        return 0


def _ready_public_rows(root, mode, *, now=None):
    """Avoid pre-scanning items the same queue cannot yet execute."""
    now = time.time() if now is None else now
    prior = {}
    path = Path(root)/'retirement-queue-status.json'
    try:
        if not path.is_symlink() and path.is_file() and path.stat().st_size <= 4*1024*1024:
            value = json.loads(path.read_bytes())
            entries = value.get('entries') if isinstance(value, dict) else None
            if (isinstance(value, dict)
                    and value.get('contract') == 'stockagent.source_local_repack_queue.v1.retirement-status'
                    and isinstance(entries, list) and len(entries) <= 4096):
                prior = {(item.get('dataset'), item.get('old_manifest_sha256'), item.get('mode', 'direct')): item
                         for item in entries if isinstance(item, dict)
                         and isinstance(item.get('dataset'), str)
                         and isinstance(item.get('old_manifest_sha256'), str)
                         and isinstance(item.get('mode', 'direct'), str)}
    except (OSError, ValueError, TypeError):
        pass  # A missing or invalid hint never suppresses the real queue.
    candidates = discover_pending_retirements(root)
    return [row for row in candidates
            if row['mode'] == mode and row.get('private') is not True
            and not retirement_cooldown_active(prior.get((row['dataset'], row['old_manifest_sha256'],
                                                           row['mode']), {}), now)][:64]


def _ready_public_direct_rows(root, *, now=None):
    return _ready_public_rows(root, 'direct', now=now)


def prefetch_positive_pins(root, workspace, *,
                           materialized=Path('/srv/stockagent-packed-materialized'),
                           retention_state=Path('/var/lib/stockagent-packed-retention'), now=time.time):
    """Batch re-observe previously pinned exact releases as scheduling hints.

    A positive can only defer its exact candidate for a short retry. A missing,
    invalid, changed or unobservable hint falls back to the canonical child;
    it never proves that a release is unpinned or authorizes an unlink.
    """
    root, workspace = Path(root).absolute(), Path(workspace).absolute()
    path = root/'retirement-queue-status.json'
    try:
        if (root != workspace/'migration/source-local-exact-repack'
                or root.is_symlink() or root.resolve(strict=True) != root
                or path.is_symlink() or not path.is_file() or path.stat().st_size > 4*1024*1024):
            return None
        status = json.loads(path.read_bytes())
        entries = status.get('entries') if isinstance(status, dict) else None
        if (not isinstance(status, dict)
                or status.get('contract') != 'stockagent.source_local_repack_queue.v1.retirement-status'
                or not isinstance(entries, list) or len(entries) > 4096):
            return None
        observed = now()
        candidates = {}
        prepared_bytes = 0
        for row in entries:
            if len(candidates) >= MAX_PIN_PREFETCH_CANDIDATES:
                break
            if (not isinstance(row, dict) or row.get('mode') != 'direct'
                    or row.get('private') is True or row.get('state') != 'waiting_retry'
                    or row.get('reason') != 'retirement_pinned'
                    or retirement_cooldown_active(row, observed)):
                continue
            try:
                if not isinstance(row.get('dataset'), str) or not isinstance(row.get('old_manifest_sha256'), str):
                    continue
                name = packed_snapshots.validate_slug(row.get('dataset'), 'dataset')
                digest = packed_snapshots._validate_hash(row.get('old_manifest_sha256'), 'old manifest SHA')
                stage = workspace/'migration/stream-source-repack-v1'/name/digest
                if stage.is_symlink() or stage.resolve(strict=True) != stage:
                    continue
                prepared_path = stage/'prepared.json'
                if prepared_path.is_symlink() or not prepared_path.is_file():
                    continue
                size = prepared_path.stat().st_size
                if (size > MAX_PIN_PREFETCH_SINGLE_PREPARED_BYTES
                        or prepared_bytes+size > MAX_PIN_PREFETCH_PREPARED_BYTES):
                    continue
                prepared_bytes += size
                prepared = stream_repack._prepared_metadata(stage, name, digest)
                if not isinstance(prepared.get('old_snapshot_id'), str):
                    continue
                snapshot_id = packed_snapshots.validate_slug(prepared.get('old_snapshot_id'), 'old snapshot')
                candidates[(name, digest)] = snapshot_id
            except (OSError, ValueError, KeyError, TypeError, SnapshotError):
                continue
        if len(candidates) < 2:
            return None  # One child cannot amortize a separate pin census.
        protected, _, _ = packed_retention._local_protected_snapshot_ids(
            Path(materialized), set(candidates.values()),
            research_pin_root=Path(retention_state)/'research-pins',
            research_pin_marker=Path(retention_state)/packed_retention.RESEARCH_PIN_MARKER)
        return {'observed_at_epoch': now(),
                'keys': frozenset(key for key, snapshot in candidates.items() if snapshot in protected)}
    except (OSError, ValueError, KeyError, TypeError, SnapshotError):
        return None


def _history_lookahead_groups(paths, protected, load_refs, *, max_versions=32):
    """Predict bounded graph targets; a cache miss always falls back to the owner.

    Match the history owner's batch and positive-witness selection. This is
    merely a hint for a generation-bound catalog scan, never deletion proof.
    A changed pin, head or manifest can only cause the real owner to miss the
    hint and run its normal complete scan and final locked checks.
    """
    remaining = iter(path for path in paths if path.stem not in protected)
    groups, selected, pending = [], 0, None
    while selected < max_versions:
        candidates, targets, occurrences = [], set(), 0
        while selected < max_versions:
            path = pending or next(remaining, None)
            pending = None
            if path is None:
                break
            refs, count = load_refs(path)
            if candidates and occurrences + count > superseded_repack_history.MAX_REFERENCE_OCCURRENCES:
                pending = path
                break
            candidates.append(path)
            targets.update(refs)
            occurrences += count
            selected += 1
            if len(candidates) >= superseded_repack_history.MAX_VERSIONS:
                break
        if not candidates:
            break
        excluded, witnessed, witnesses, inspected = set(candidates), set(), 0, 0
        for path in paths:
            if path in excluded:
                continue
            inspected += 1
            if inspected > superseded_repack_history.MAX_WITNESS_CANDIDATES:
                break
            refs, _ = load_refs(path)
            covered = (targets-witnessed) & refs
            if covered:
                witnesses += 1
                witnessed.update(covered)
                if len(witnessed) == len(targets) or witnesses >= superseded_repack_history.MAX_WITNESS_MANIFESTS:
                    break
        groups.append(targets-witnessed)
        if pending is None and path is None:
            break
    return groups


def _history_may_need_catalog(cold, row):
    """Skip only cache preparation when a bounded name probe sees <=1 manifest.

    The child still takes its ordinary locks and diagnoses every state; this
    inexpensive hint never completes, skips or authorizes a retirement.
    """
    try:
        name = packed_snapshots.validate_slug(row['dataset'], 'dataset')
        directory = Path(cold)/'manifests'/name
        if directory.is_symlink() or not directory.is_dir():
            return False
        count = 0
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.name.endswith('.json'):
                    count += 1
                    if count > 1:
                        return True
    except (OSError, ValueError, KeyError, TypeError, SnapshotError):
        pass
    return False


def _history_prefetch_groups(root, workspace, cold, row, metadata, *,
                             materialized=Path('/srv/stockagent-packed-materialized'),
                             retention_state=Path('/var/lib/stockagent-packed-retention')):
    """Read a small upcoming window, with bounded source bytes and memory."""
    name, old = row['dataset'], row['old_manifest_sha256']
    stage = workspace/'migration/stream-source-repack-v1'/name/old
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('history prefetch stage redirected')
    install = stage/'install.json'
    if install.is_symlink() or not install.is_file() or install.stat().st_size > 128*1024:
        raise SnapshotError('history prefetch install unavailable')
    install_data = json.loads(install.read_bytes())
    new_sha = packed_snapshots._validate_hash(install_data.get('new_manifest_sha256'), 'new manifest SHA')
    receipt_path = root/name/(new_sha+'.json')
    if receipt_path.is_symlink() or not receipt_path.is_file() or receipt_path.stat().st_size > 1024*1024:
        raise SnapshotError('history prefetch receipt unavailable')
    receipt = json.loads(receipt_path.read_bytes())
    if (receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or receipt.get('dataset') != name or receipt.get('old_manifest_sha256') != old
            or receipt.get('new_manifest_sha256') != new_sha or receipt.get('old_release_removed') is not True):
        raise SnapshotError('history prefetch receipt identity differs')
    new_id = packed_snapshots.validate_slug(receipt.get('new_snapshot_id'), 'new snapshot')
    directory = cold/'manifests'/name
    if directory.is_symlink() or not directory.is_dir() or directory.resolve(strict=True) != directory:
        raise SnapshotError('history prefetch manifest namespace unavailable')
    paths = sorted(path for path in directory.glob('*.json') if path.stem != new_id)
    if len(paths) > 10000:
        raise SnapshotError('history prefetch exceeds manifest bound')
    known = {Path(relative).stem for relative in metadata} | {path.stem for path in paths} | {new_id}
    protected = (superseded_repack_history._protect(materialized, retention_state, known, cold=cold)
                 | superseded_repack_history._heads(cold, name) | {new_id})
    cache, read_bytes = {}, 0
    deadline = time.monotonic()+45
    def load_refs(path):
        nonlocal read_bytes
        if path not in cache:
            if len(cache) >= 48 or time.monotonic() >= deadline:
                raise SnapshotError('history prefetch bounded work exhausted')
            size = path.lstat().st_size
            if size <= 0 or size > 8*1024**2 or read_bytes+size > 128*1024**2:
                raise SnapshotError('history prefetch manifest bytes exceed bound')
            manifest = repack_retirement._read_manifest(cold, path.relative_to(cold).as_posix())
            refs = [manifest['archive']['inventory'], *manifest['archive']['objects']]
            cache[path] = ({item['relpath'] for item in refs}, len(refs))
            read_bytes += size
        return cache[path]
    return _history_lookahead_groups(paths, protected, load_refs)


def prepare_batch_reference_cache(root, workspace, cold, config, *,
                                  catalog=Path('/var/lib/stockagent/backup-stream/catalog.json'),
                                  observation=Path('/var/lib/stockagent/backup-stream/catalog-observation.json'),
                                  selected_history=None, positive_pin_hints=None):
    """Share one fixed catalog pass among ready public source retirements."""
    pending = [row for row in _ready_public_direct_rows(root)
               if not positive_pin_hint_applies(positive_pin_hints, row, time.time())]
    history = ([selected_history] if isinstance(selected_history, dict)
               and selected_history.get('mode') == 'history'
               and selected_history.get('private') is not True else [])
    if history and not _history_may_need_catalog(cold, history[0]):
        history = []
    if len(pending) < 2 and not history:
        return {'state': 'not_needed'}
    owner = os.open(root/'catalog-target-cache-owner.lock',
                    os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'state': 'builder_busy'}
        targets = set()
        covered = 0
        for row in pending:
            try:
                stage = workspace/'migration/stream-source-repack-v1'/row['dataset']/row['old_manifest_sha256']
                if stage.is_symlink() or stage.resolve(strict=True) != stage:
                    continue
                prepared = stream_repack._prepared_metadata(stage, row['dataset'], row['old_manifest_sha256'])
                fixed = packed_snapshots.resolve_packed_snapshot_id(cold, row['dataset'],
                    prepared['old_snapshot_id'], require_objects=False)
                if fixed.manifest_sha256 != row['old_manifest_sha256']:
                    continue
                # A foreign publisher's current head cannot be retired yet.
                # Do not repeatedly scan a multi-GiB catalog for those known
                # blockers; the child still checks again under its own lock.
                repack_retirement._require_not_current_head(cold, row['dataset'],
                    row['old_manifest_sha256'], prepared['old_snapshot_id'])
                targets.update(repack_retirement._refs(fixed.manifest))
                covered += 1
            except (OSError, ValueError, KeyError, TypeError, SnapshotError):
                continue  # The exact child owner still diagnoses this source.
        if covered < 2 and not history:
            return {'state': 'insufficient_candidates'}
        base = workspace/'migration/d-cold-object-staging-v1'
        if base.is_symlink() or base.resolve(strict=True) != base:
            raise SnapshotError('retirement catalog metadata root is redirected')
        metadata, _, identity = retire_staged_cold_objects.catalog_index(
            catalog, observation, set(), cache_path=base/'catalog-index-cache.json')
        generation = _private_catalog_generation(catalog)
        path = base/'catalog-target-reference-cache-v1.json'
        groups = []
        if history:
            try:
                groups = _history_prefetch_groups(root, workspace, cold, history[0], metadata)
                print({'history_prefetch': 'observed', 'dataset': history[0]['dataset'],
                       'groups': len(groups), 'immediate_targets': len(groups[0]) if groups else 0},
                      flush=True)
            except (OSError, ValueError, TypeError, KeyError, SnapshotError) as error:
                # Keep the type observable while the normal owner continues;
                # do not expose private paths from the exception text.
                print({'history_prefetch': 'fallback', 'error_type': type(error).__name__}, flush=True)
        if covered < 2 and len(groups) < 2:
            # A single imminent scan cannot amortize its own preparation and
            # must not evict a useful multi-batch target cache from E.
            return {'state': 'not_needed'}
        immediate = targets | (groups[0] if groups else set())
        if not immediate and not groups:
            return {'state': 'insufficient_candidates'}
        cached = repack_retirement.cached_catalog_references(path, catalog, metadata, immediate, generation, identity)
        if cached is not None:
            result = {'state': 'reused', 'candidate_count': covered, 'targets': len(immediate)}
            if history:
                result['history_groups'] = len(groups)
            return result
        for group in groups:
            if len(targets | group) > 100000:
                break
            targets.update(group)
        if not targets or len(targets) > 100000:
            return {'state': 'insufficient_candidates'}
        workspace_volume.admit(2*repack_retirement.MAX_TARGET_CACHE_BYTES, c=config)
        result = repack_retirement.build_catalog_reference_cache(
            catalog, cold, metadata, targets, generation, identity, path)
        result = {'state': 'built', 'candidate_count': covered, **result}
        if history:
            result['history_groups'] = len(groups)
        return result
    finally:
        os.close(owner)


def main():
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    root = workspace_volume.path('migration', create=True)/'source-local-exact-repack'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if _priority_handoff_pending(root):
        print({'state': 'yielding_to_sealed_handoff', 'retirement_attempts': 0}, flush=True)
        return
    # These are public-only scheduling hints. Avoid rebuilding them while the
    # public publisher holds the owner: private candidates still get their turn,
    # and every selected retirement keeps its own canonical locked checks.
    if _publication_owner_busy(Path(config['cold_root'])):
        pin_hints = None
        cache = {'state': 'deferred_public_owner_busy'}
    else:
        pin_hints = prefetch_positive_pins(root, Path(config['mount_point']))
        try:
            cache = prepare_batch_reference_cache(root, Path(config['mount_point']),
                                                  Path(config['cold_root']), config,
                                                  positive_pin_hints=pin_hints)
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            # The existing full reference scan remains the authority on cache
            # failure; one optimization must never prevent ordinary retirement.
            cache = {'state': 'fallback_full_scan', 'error_type': type(error).__name__}
    print({'catalog_reference_cache': cache}, flush=True)
    def prepare_selected_history(row):
        # The retirement owner has already picked the exact ready generation;
        # queue order and round-robin cursor need not be guessed in advance.
        if row.get('mode') != 'history' or row.get('private') is True:
            return
        try:
            result = prepare_batch_reference_cache(root, Path(config['mount_point']),
                Path(config['cold_root']), config, selected_history=row, positive_pin_hints=pin_hints)
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            result = {'state': 'fallback_full_scan', 'error_type': type(error).__name__}
        if result.get('state') != 'not_needed':
            print({'catalog_reference_cache_selected_history': result, 'dataset': row['dataset']}, flush=True)
    ordinary_attempted = False
    def run_selected(_root):
        nonlocal ordinary_attempted
        from stockagent.data_sync.ordinary_archive import step as ordinary_archive_step
        if not ordinary_attempted:
            # One ordinary inventory may contain millions of paths. Do not
            # scan sixteen different roots while holding a single fair turn;
            # subsequent calls retain the original bounded repack retirements.
            ordinary_attempted = True
            ordinary = ordinary_archive_step(Path(__file__).resolve().parents[1], guard=guard.check)
            if ordinary:
                return True
        cold = Path(config['cold_root'])
        scopes = set()
        if _publication_owner_busy(cold):
            scopes.add('public')
        if _publication_owner_busy(cold/'.local-state/private-history'):
            scopes.add('private')
        return run_one_retirement(_root, on_selected=prepare_selected_history, positive_pin_hints=pin_hints,
                                  busy_publication_scopes=scopes)
    from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
    from stockagent.data_sync.dataset_migration import migration_process_identity
    guard = TradingPriorityGuard()
    try:
        identity = migration_process_identity()
    except (OSError, ValueError, IndexError):
        identity = None  # A missing display identity must not stop retirement.
    last_observation = {'state': None, 'at': 0.0}
    def observe_turn(state):
        if identity is None:
            return
        now = time.monotonic()
        if state == last_observation['state'] and now-last_observation['at'] < 20:
            return
        workspace_volume.check(c=config)
        atomic_write_json(root/'retirement-turn-observation.json', {
            'contract': 'stockagent.source_local_retirement_turn_observation.v1',
            'state': state, 'observed_at_epoch': time.time(), 'owner_identity': identity,
            'completion_proved': False}, durable=False)
        last_observation.update(state=state, at=now)
    print({'retirement_attempts': run_pressure_turn(root, Path(config['cold_root']),
          Path(config['mount_point']), guard=guard.check, observe=observe_turn,
          run=lambda path, publication_busy: run_batch(path, run=run_selected,
                                                       publication_busy=publication_busy))}, flush=True)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
