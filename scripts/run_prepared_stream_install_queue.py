#!/usr/bin/env python3
"""Install already sealed E source packs without waiting for unrelated builds."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_source_local_repack_queue as q, run_stream_packed_repack as stream_cli
from stockagent.data_sync import packed_snapshots as p, source_repack_bloat, staged_head_capacity_relief, stream_repack, temporary_e_authority, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.prepared_stream_install_queue.v1'
RETRY_SECONDS = 60


def relief_capacity_may_now_fit(shortage: int, previous: dict) -> bool:
    """Wake a measured capacity retry early; the child still rechecks everything."""
    if (previous.get('error') != 'insufficient_exclusive_space'
            or type(shortage) is not int or shortage < 0):
        return False
    if shortage == 0:
        return True
    exclusive = previous.get('D_exclusive_allocated_bytes')
    return (type(exclusive) is int and exclusive >= 0
            and shortage + staged_head_capacity_relief.HEADROOM <= exclusive)


def relief_priority(item: dict):
    """Prefer an old head which could release the missing D capacity.

    Encoded object lengths are only an upper-bound scheduling hint: sharing,
    hardlinks and filesystem allocation are checked by the real relief owner.
    No object becomes eligible for deletion from this estimate.
    """
    upper = item.get('D_old_object_bytes_upper_bound')
    shortage = item.get('D_shortfall_bytes')
    remaining = item.get('encoded_bytes_remaining', 0)
    if type(upper) is not int or type(shortage) is not int:
        return (1, 0, remaining, item.get('dataset', ''))
    if upper < shortage:
        return (2, 0, remaining, item.get('dataset', ''))
    return (0, -(upper-remaining), remaining, item.get('dataset', ''))


def choose_candidate(entries: list[dict], E_only: set[tuple[str, str]]) -> dict | None:
    """An incomplete D head takes precedence over every unrelated install."""
    if E_only:
        urgent = [item for item in entries if
                  (item.get('dataset'), item.get('expected_manifest_sha256')) in E_only
                  and item.get('state') in {'waiting', 'waiting_relief'}]
        return min(urgent, key=lambda item: item['dataset']) if urgent else None
    resume = [item for item in entries if item['state'] == 'waiting_relief' and item.get('resume_relief')]
    ready = [item for item in entries if item['state'] == 'waiting']
    relief_ready = [item for item in entries if item['state'] == 'waiting_relief']
    candidates = resume or ready or relief_ready
    return (min(candidates, key=relief_priority) if candidates and not (resume or ready)
            else min(candidates, key=lambda item: (item['encoded_bytes_remaining'], item['dataset']))
            if candidates else None)


def committed_head_needs_cleanup(cold: Path, stage: Path, dataset: str, expected: str, journal: dict) -> bool:
    """Resume a committed head even if E cleanup already removed some packs."""
    if not isinstance(journal, dict) or journal.get('state') != 'head_pending':
        return False
    prepared = stream_repack._prepared_metadata(stage, dataset, expected)
    if (journal.get('contract') != stream_repack.CONTRACT+'.install'
            or journal.get('dataset') != dataset or journal.get('old_manifest_sha256') != expected
            or journal.get('prepared_sha256') != prepared['identity_sha256']
            or journal.get('old_snapshot_id') != prepared['old_snapshot_id']
            or not isinstance(journal.get('installed'), list)):
        raise SnapshotError('head recovery install journal differs')
    new_sha = p._validate_hash(journal.get('new_manifest_sha256'), 'new manifest SHA')
    current = p.resolve_latest_packed(cold, dataset, require_objects=False)
    if current.manifest_sha256 == expected:
        return False  # The durable intent precedes the actual head switch.
    if (current.manifest_sha256 != new_sha or current.manifest['snapshot_id'] != journal.get('snapshot_id')
            or current.manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256') != expected):
        raise SnapshotError('head recovery current D generation differs')
    relief = staged_head_capacity_relief.load(stage, dataset, expected, prepared)
    if relief is not None and relief['state'] != 'completed':
        raise SnapshotError('head recovery current-head relief is incomplete')
    return True


def ensure_local_E_view(cold: Path, stage: Path, name: str, expected: str, *, guard=lambda: None) -> None:
    """Reuse a sealed E view without reacquiring the unrelated D global lock."""
    marker = stage/temporary_e_authority.MARKER
    state = (temporary_e_authority._marker(stage, name, expected).get('state')
             if marker.exists() or marker.is_symlink() else None)
    if state == 'ready':
        with temporary_e_authority.borrow(stage, name, expected):
            guard()
    elif state in (None, 'preparing'):
        if temporary_e_authority.expose(cold, stage, name, expected, guard=guard).get('state') != 'ready':
            raise SnapshotError('temporary E authority is not ready')
    else:
        raise SnapshotError('temporary E authority cannot be resumed in this state')


def run_once(workspace: Path, cold: Path, *, now=time.time, runner=q.run_observed,
             free_bytes=None, code_sha=None, enable_relief=False, guard=lambda: None,
             admit=lambda _bytes: None):
    if type(enable_relief) is not bool:
        raise SnapshotError('E-first relief must be an explicit boolean')
    workspace, cold = Path(workspace).absolute(), Path(cold).absolute()
    root = workspace/'migration/source-local-exact-repack'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('prepared install queue root redirected')
    fd = os.open(root/'prepared-install-queue-owner.lock',
                 os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError('prepared install queue already has an owner') from error
        path = root/'prepared-install-queue-status.json'
        upstream_path = root/'queue-status.json'
        if upstream_path.is_symlink():
            raise SnapshotError('prepared install upstream redirected')
        try:
            upstream = json.loads(upstream_path.read_text())
        except FileNotFoundError:
            upstream = {}
        if not isinstance(upstream, dict):
            raise SnapshotError('prepared install upstream status invalid')
        rows = upstream.get('entries', []) if upstream.get('contract') == q.CONTRACT+'.status' else []
        if not isinstance(rows, list) or len(rows) > 1024:
            raise SnapshotError('prepared install upstream entries invalid')
        try:
            previous = json.loads(path.read_text()) if not path.is_symlink() else {}
        except (OSError, ValueError):
            previous = {}
        if not isinstance(previous, dict):
            previous = {}
        if code_sha is None:
            code_sha = hashlib.sha256(b''.join(item.read_bytes() for item in (Path(__file__),
                ROOT/'scripts/run_stream_packed_repack.py', ROOT/'stockagent/data_sync/stream_repack.py',
                ROOT/'scripts/run_staged_head_capacity_relief.py',
                ROOT/'stockagent/data_sync/staged_head_capacity_relief.py',
                ROOT/'stockagent/data_sync/source_repack_bloat.py',
                ROOT/'stockagent/data_sync/private_handoff_retirement.py',
                ROOT/'stockagent/data_sync/repack_retirement.py',
                ROOT/'stockagent/data_sync/bulk_landing_alias_retirement.py',
                ROOT/'stockagent/data_sync/bulk_original_carrier_retirement.py'))).hexdigest()
        old = previous.get('entries', []) if (previous.get('contract') == CONTRACT
                                               and previous.get('implementation_sha256') == code_sha) else []
        if not isinstance(old, list) or len(old) > 1024:
            old = []
        prior = {(item.get('dataset'), item.get('expected_manifest_sha256')): item
                 for item in old if isinstance(item, dict) and isinstance(item.get('dataset'), str)
                 and isinstance(item.get('expected_manifest_sha256'), str)}
        available = shutil.disk_usage(cold).free if free_bytes is None else free_bytes
        if type(available) is not int or available < 0:
            raise SnapshotError('prepared install cold capacity unknown')
        entries = []
        E_only = staged_head_capacity_relief.E_only_pending_generations(workspace)
        dedup_held = source_repack_bloat.auto_holds(workspace)
        for row in rows:
            if (not isinstance(row, dict) or row.get('origin') not in ('cold', 'private_cold')
                    or row.get('state') not in {'waiting', 'retrying', 'running'}):
                continue
            try:
                name = p.validate_slug(row.get('dataset'), 'dataset')
                expected = p._validate_hash(row.get('expected_manifest_sha256'), 'source manifest SHA')
                if row.get('origin') == 'private_cold' and name not in stream_repack.PRIVATE_REPACK_DATASETS:
                    raise SnapshotError('private streaming dataset is not enrolled')
            except (TypeError, ValueError, SnapshotError):
                # Isolate a malformed upstream row; do not interpolate its
                # untrusted name into any path or prevent independent work.
                entries.append({'state': 'invalid_candidate', 'error': 'invalid_dataset_or_manifest'})
                continue
            stage = workspace/'migration/stream-source-repack-v1'/name/expected
            if row.get('E_prepared') is not True:
                # A source build can finish sealing E and then lose the
                # publication lock before the older main queue records its
                # prepared flag. Discover that exact generation directly;
                # never overtake a main owner still marked as running.
                if row.get('state') == 'running' or not (stage/'prepared.json').is_file():
                    continue
            old_item = prior.get((name, expected), {})
            entry = {'dataset': name, 'expected_manifest_sha256': expected, 'origin': row['origin']}
            try:
                upstream_retry = row.get('retry_after_epoch')
                if (row.get('reason') == 'repack_foreign_head_wait'
                        and type(upstream_retry) in (int, float) and now() < upstream_retry <= now()+86400
                        and (name, expected) not in E_only):
                    # A newly enrolled Vast handoff can follow a committed D
                    # successor. Do not keep a sealed batch in the old wait
                    # solely because the previous queue did not know this.
                    successor_ready = False
                    if row.get('E_prepared') is True and row.get('origin') == 'cold':
                        try:
                            checked = stream_repack._prepared_metadata(stage, name, expected)
                            successor_ready = stream_cli.foreign_handoff_successor_ready(
                                cold, name, expected, 'penguin', checked)
                        except (OSError, ValueError, KeyError, SnapshotError):
                            pass
                    if successor_ready:
                        upstream_retry = 0
                if (row.get('reason') == 'repack_foreign_head_wait'
                        and type(upstream_retry) in (int, float) and now() < upstream_retry <= now()+86400
                        and (name, expected) not in E_only):
                    entry.update(state='waiting_head_transition', retry_after_epoch=upstream_retry)
                    # The exact source is already sealed on E. Make its
                    # local-only dataset view available now rather than wait
                    # for another publisher's old head to be handed off.
                    # This does not redirect normal readers or delete D.
                    if row.get('E_prepared') is True and row.get('origin') == 'cold':
                        try:
                            ensure_local_E_view(cold, stage, name, expected, guard=guard)
                            entry['E_temporary_authority_ready'] = True
                            if stream_cli._source_head_nodes(cold, name) == {'lab203': expected}:
                                # Keep the same precise owner and live capacity
                                # diagnosis during the upstream retry window;
                                # this is an observation, not permission to
                                # modify the other publisher's head.
                                entry['relief_head_owner'] = 'lab203'
                                prepared = stream_repack._read_prepared(stage, name, expected)
                                journal_file = stage/'install.json'
                                if journal_file.is_symlink():
                                    raise SnapshotError('prepared install journal redirected')
                                journal = json.loads(journal_file.read_text()) if journal_file.is_file() else {}
                                remaining = stream_cli.remaining_install_bytes(cold, prepared, journal)
                                reserve = stream_cli.install_reserve_bytes(name, expected, private=False)
                                entry.update(encoded_bytes_remaining=remaining, D_free_bytes=available,
                                             D_reserve_bytes=reserve,
                                             D_shortfall_bytes=max(0, remaining+reserve-available))
                        except (OSError, ValueError, KeyError, SnapshotError) as error:
                            entry['E_temporary_authority_error'] = type(error).__name__
                    entries.append(entry)
                    continue
                if q.prior_published(root, row):
                    entry.update(state='completed')
                elif row.get('state') == 'running':
                    entry.update(state='waiting_original_owner')
                else:
                    if stage.is_symlink() or stage.resolve(strict=True) != stage:
                        raise SnapshotError('prepared install stage redirected')
                    journal_path = stage/'install.json'
                    if journal_path.is_symlink():
                        raise SnapshotError('prepared install journal redirected')
                    journal = json.loads(journal_path.read_text()) if journal_path.is_file() else {}
                    object_root = (cold/'.local-state/private-history'
                                   if row['origin'] == 'private_cold' else cold)
                    if committed_head_needs_cleanup(object_root, stage, name, expected, journal):
                        # Original E payloads may already have been removed by
                        # the interrupted cleanup; the canonical installer
                        # reconciles its head and cleanup journal itself.
                        entry.update(state='waiting', encoded_bytes_remaining=0,
                                     D_free_bytes=available, D_reserve_bytes=0,
                                     D_shortfall_bytes=0, recover_committed_head=True)
                        entries.append(entry)
                        continue
                    prepared = stream_repack._read_prepared(stage, name, expected)
                    if prepared.get('packed_format') == 'content-alias-v3':
                        try:
                            stream_repack.require_content_alias_install(stage, name, expected, prepared)
                        except SnapshotError:
                            entry.update(state='waiting_v3_activation',
                                         reason='content_alias_local_reader_not_enrolled')
                            entries.append(entry)
                            continue
                    remaining = stream_cli.remaining_install_bytes(object_root, prepared, journal)
                    reserve = stream_cli.install_reserve_bytes(name, expected, private=row['origin'] == 'private_cold')
                    shortage = max(0, remaining+reserve-available)
                    entry.update(encoded_bytes_remaining=remaining, D_free_bytes=available,
                                 D_reserve_bytes=reserve, D_shortfall_bytes=shortage)
                    relief = staged_head_capacity_relief.load(stage, name, expected, prepared)
                    if relief is not None and (relief.get('private') is True) != (row['origin'] == 'private_cold'):
                        raise SnapshotError('prepared relief journal authority differs')
                    retry = old_item.get('retry_after_epoch', 0)
                    if type(retry) not in (int, float) or not now() <= retry <= now()+86400:
                        retry = 0
                    if retry and relief_capacity_may_now_fit(shortage, old_item):
                        # A completed unrelated retirement can make this
                        # generation viable before its old cooldown expires.
                        # This is only a scheduling hint, never unlink proof.
                        retry = 0
                    if (name, expected) in E_only:
                        retry = 0  # Retry the sole complete temporary source first.
                    pending = journal.get('pending') if isinstance(journal, dict) else None
                    has_owned_incoming = (isinstance(pending, dict)
                                          and isinstance(pending.get('incoming'), dict))
                    if (name, expected) not in E_only and relief is None:
                        # Do not expose the inflated E encoding as a temporary
                        # authority before the original dedup owner has had a
                        # chance to replace it. Existing install/relief intents
                        # are never redirected, even when capacity is tight.
                        held = (name, expected) in dedup_held
                        if shortage and not held and not journal:
                            try:
                                with p._try_publication_lock(
                                        object_root/'.local-state/locks'/f'publish-{name}.lock'):
                                    current = p.resolve_latest_packed(object_root, name, require_objects=False)
                                    if current.manifest_sha256 != expected:
                                        raise SnapshotError('dedup scheduling original head changed')
                                    held = source_repack_bloat.preflight(
                                        workspace, object_root, name, expected, current.manifest,
                                        guard=guard, admit=admit,
                                        private=row['origin'] == 'private_cold', allow_sealed=True) is not None
                            except (OSError, ValueError, KeyError, SnapshotError) as error:
                                # This optional metadata optimization grants no
                                # permission to install or remove anything. The
                                # original capacity/authority gates still run.
                                entry['dedup_planning_error'] = type(error).__name__
                        if held:
                            entry.update(state='waiting_dedup', reason='source_duplicate_bloat')
                            entries.append(entry)
                            continue
                    if (enable_relief and relief is not None
                            and relief['state'] != 'completed'):
                        # Complete an interrupted exact unlink transaction even
                        # if its partial removal already made D appear writable.
                        entry.update(state='waiting_relief', resume_relief=True)
                    elif retry:
                        entry.update(state='retrying', retry_after_epoch=retry, error=old_item.get('error'))
                        if old_item.get('error') == 'insufficient_exclusive_space':
                            entry['relief_state'] = 'insufficient_exclusive_space'
                            for key in ('D_exclusive_allocated_bytes', 'D_relief_shortfall_bytes',
                                        'relief_observed_at_epoch'):
                                value = old_item.get(key)
                                if type(value) is int and value >= 0:
                                    entry[key] = value
                            ensure_local_E_view(object_root, stage, name, expected, guard=guard)
                            entry['E_temporary_authority_ready'] = True
                    elif shortage and has_owned_incoming:
                        # Let the original installer first reconcile its exact
                        # journalled partial under the shared D write owner.
                        # Admission is measured again after that cleanup.
                        entry.update(state='waiting', interrupted_incoming_reconciliation=True)
                    elif shortage:
                        entry.update(state='waiting_capacity')
                        try:
                            current = p.resolve_latest_packed(object_root, name, require_objects=False)
                            if current.manifest_sha256 != expected:
                                raise SnapshotError('capacity scheduling original head changed')
                            # This deliberately ignores sharing and is never a
                            # capacity admission or retirement authorization.
                            entry['D_old_object_bytes_upper_bound'] = sum(
                                item['bytes'] for item in current.manifest['archive']['objects'])
                            ensure_local_E_view(object_root, stage, name, expected, guard=guard)
                            entry['E_temporary_authority_ready'] = True
                            if enable_relief and relief is None:
                                # Public relief normally needs Penguin's head.
                                # Only the fixed retired lab203 release may
                                # use its separately admitted successor path.
                                heads = stream_cli._source_head_nodes(object_root, name)
                                publisher = ('penguin' if row['origin'] == 'cold' else
                                             p.validate_slug(current.manifest.get('publisher', {}).get('node_id'),
                                                             'private source publisher'))
                                retired_lab203 = (row['origin'] == 'cold' and
                                    stream_cli._retired_lab203_successor_ready(
                                        object_root, name, expected, 'penguin', prepared))
                                if heads == {publisher: expected} or retired_lab203:
                                    entry['state'] = 'waiting_relief'
                                else:
                                    entry['state'] = 'waiting_relief_head_owner'
                                    entry['relief_head_owner'] = ('lab203' if heads == {'lab203': expected}
                                                                  else 'other')
                            elif relief is not None and relief['state'] == 'completed':
                                entry['relief_completed_but_capacity_short'] = True
                        except (OSError, ValueError, KeyError, SnapshotError) as error:
                            # An unavailable view must not prevent another
                            # dataset from using an independently free slot.
                            entry['E_temporary_authority_error'] = type(error).__name__
                    else:
                        entry.update(state='waiting')
            except (OSError, TypeError, ValueError, KeyError, SnapshotError) as error:
                entry.update(state='retrying', retry_after_epoch=now()+RETRY_SECONDS,
                             error=type(error).__name__)
            entries.append(entry)
        def save():
            atomic_write_json(path, {'contract': CONTRACT, 'implementation_sha256': code_sha,
                'observed_at_epoch': now(), 'owner_pid': os.getpid(), 'entries': entries}, durable=True)
        selected = choose_candidate(entries, E_only)
        if selected is None:
            save()
            return entries
        relief_selected = selected['state'] == 'waiting_relief'
        selected.update(state='running_relief' if relief_selected else 'running', started_at_epoch=now())
        save()
        progress = workspace/'migration/stream-source-repack-v1'/('prepared-install-'+selected['dataset']+'.progress.json')
        if relief_selected:
            command = [sys.executable, str(ROOT/'scripts/run_staged_head_capacity_relief.py'),
                       '--dataset', selected['dataset'], '--old-manifest-sha256',
                       selected['expected_manifest_sha256'], '--apply']
            if selected['origin'] == 'private_cold':
                command.append('--private-source')
        else:
            command = [sys.executable, str(ROOT/'scripts/run_stream_packed_repack.py'), '--dataset', selected['dataset'],
                       '--expected-manifest-sha256', selected['expected_manifest_sha256'], '--progress-file', str(progress)]
            if selected['origin'] == 'private_cold':
                command.append('--private-source')
        try:
            result = runner(command, on_heartbeat=save)
            if relief_selected and result.returncode == 0:
                value = json.loads(result.stdout)
                if (not isinstance(value, dict) or value.get('contract') != staged_head_capacity_relief.CONTRACT
                        or value.get('dataset') != selected['dataset']
                        or (value.get('private') is True) != (selected['origin'] == 'private_cold')
                        or value.get('state') not in
                        {'completed', 'not_needed', 'insufficient_exclusive_space'}):
                    raise ValueError('E-first relief result identity differs')
                selected.update(state='relief_completed' if value['state'] in {'completed', 'not_needed'} else 'retrying',
                                relief_state=value['state'])
                if value['state'] == 'insufficient_exclusive_space':
                    selected.update(error='insufficient_exclusive_space', retry_after_epoch=now()+300)
                    for source, target in (('exclusive_allocated_bytes', 'D_exclusive_allocated_bytes'),
                                           ('D_shortfall_bytes', 'D_relief_shortfall_bytes')):
                        measured = value.get(source)
                        if type(measured) is int and measured >= 0:
                            selected[target] = measured
                    selected['relief_observed_at_epoch'] = int(now())
            elif result.returncode == 0 and q.prior_published(root, selected):
                selected.update(state='completed')
            else:
                error = ('foreign_head_wait' if 'another publisher current head prevents old-source retirement' in result.stderr
                         else 'relief_pinned' if relief_selected and 'pin' in result.stderr.lower()
                         else 'relief_reader_busy' if relief_selected and 'running process' in result.stderr.lower()
                         else 'capacity' if 'D needs space' in result.stderr or 'D reserve reached' in result.stderr
                         else 'trading_priority' if 'day-trade priority window' in result.stderr.lower()
                         else 'retirement_turn' if any(name in result.stderr for name in
                              ('source-local-retirement-turn.lock', 'source-local-retirement-intent.lock'))
                         else 'owner_busy' if 'busy' in result.stderr.lower() or 'lock' in result.stderr.lower()
                         else 'worker_failed' if result.returncode else 'completion_receipt_missing')
                selected.update(state='retrying', error=error,
                                retry_after_epoch=now()+(300 if error == 'foreign_head_wait'
                                                        else 30 if error == 'retirement_turn' else RETRY_SECONDS))
                if result.returncode:
                    print(result.stderr[-4096:], file=sys.stderr)
        except (OSError, ValueError, KeyError, TypeError, SnapshotError, subprocess.TimeoutExpired) as error:
            selected.update(state='retrying', error=type(error).__name__, retry_after_epoch=now()+RETRY_SECONDS)
        selected['finished_at_epoch'] = now()
        save()
        return entries
    finally:
        os.close(fd)


def main():
    from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
    config = workspace_volume.configuration()
    priority = TradingPriorityGuard()
    def guard():
        priority.check()
        workspace_volume.check(c=config)
        _check_d_primary_mount(stream_cli.COLD)
    guard()
    print(json.dumps({'contract': CONTRACT, 'entries': run_once(Path(config['mount_point']), stream_cli.COLD,
        enable_relief=True, guard=guard, admit=lambda count: workspace_volume.admit(count, c=config))}))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
