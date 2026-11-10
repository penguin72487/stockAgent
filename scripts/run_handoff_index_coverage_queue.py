#!/usr/bin/env python3
"""Compare prepared futures inventories against their existing D parent once.

The original publication owner rechecks the fixed head, pin and source before
using the cached comparison. This queue does not retire C/E or read payloads.
"""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import prepare_handoff_index_archive as cli, publish_handoff_index_archive as publisher
from scripts import run_handoff_source_repack_queue as source_queue
from stockagent.data_sync import index_archive_existing as existing, stream_repack, workspace_volume
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_snapshots import PackedPublicationBusy

CONTRACT = 'stockagent.handoff_index_coverage_queue.v1'
DATASETS = ('tw-futures-universe725-workspace', 'tw-futures-universe765-workspace')


def candidates(root: Path):
    sources = {detail[0]: source for source, detail in cli.ENROLLED.items()}
    for dataset in DATASETS:
        source = sources.get(dataset)
        if source is None or dataset not in existing.SUBTREE_COVERAGES:
            continue
        value = source_queue._metadata(root/(dataset+'.status.json'))
        identity = value.get('source_identity')
        if (value.get('contract') != cli.CONTRACT or value.get('dataset') != dataset
                or value.get('D_archive_committed') is True or not isinstance(identity, str)
                or re.fullmatch(r'[0-9a-f]{64}', identity) is None):
            continue
        stage = root/dataset/identity
        if not (stage/'prepared.json').is_file() or (stage/'prepared.json').is_symlink():
            continue
        with handoff.selected_source(source):
            binding = handoff.read_journal()
            if not isinstance(binding, dict) or binding.get('state') != 'active':
                continue
            handoff._assert_retained(binding)
            index, root_name = Path(binding['evidence'])/'files.sqlite', handoff._scope().source.name
        prepared = stream_repack._read_prepared(stage, dataset, identity)
        yield dataset, identity, stage, index, root_name, prepared


def run_once(root: Path, private: Path, *, guard=lambda: None, now=time.time):
    root, private = Path(root).absolute(), Path(private).absolute()
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('coverage precheck queue root redirected')
    owner = os.open(root/'coverage-precheck-owner.lock',
                    os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError('coverage precheck queue already has an owner') from error
        status = root/'coverage-precheck-status.json'
        previous = source_queue._metadata(status)
        install_lock = root.parent/'cold-install-write-owner.lock'
        selected = None
        for dataset, identity, stage, index, root_name, prepared in candidates(root):
            retry = (previous.get('retry_after_epoch') if previous.get('contract') == CONTRACT
                     and previous.get('dataset') == dataset and previous.get('source_identity') == identity else None)
            last = previous.get('finished_at_epoch', previous.get('observed_at_epoch'))
            if (previous.get('reason') == 'PackedPublicationBusy' and type(retry) in (int, float)
                    and type(last) in (int, float)):
                retry = min(retry, last+15)
            if type(retry) in (int, float) and now() < retry <= now()+86400:
                continue
            try:
                proof = existing.cached_covering_match(private, stage, dataset, identity, index, root_name,
                                                        guard=guard, install_lock=install_lock)
            except (OSError, ValueError, SnapshotError) as error:
                result = {'contract':CONTRACT, 'state':'retrying', 'owner_pid':os.getpid(),
                          'dataset':dataset, 'source_identity':identity, 'observed_at_epoch':now(),
                          'reason':type(error).__name__,
                          'retry_after_epoch':now()+(15 if isinstance(error, PackedPublicationBusy) else 120)}
                atomic_write_json(status, result, durable=True)
                atomic_write_json(root/(dataset+'.coverage-status.json'), result, durable=True)
                return result
            if proof is None:
                selected = dataset, identity, stage, index, root_name, prepared
                break
        result = {'contract':CONTRACT, 'state':'waiting', 'owner_pid':os.getpid(),
                  'observed_at_epoch':now(), 'reason':'no_prepared_unmatched_futures'}
        if selected is None:
            atomic_write_json(status, result, durable=True)
            return result
        dataset, identity, stage, index, root_name, prepared = selected
        result.update(state='running', dataset=dataset, source_identity=identity,
            prepared_identity=prepared['identity_sha256'],
            started_at_epoch=now(), reason=None,
            progress={'stage':'comparing_D_existing_inventory', 'files_completed':0,
                      'files_total':prepared['files'], 'logical_bytes_processed':0,
                      'logical_bytes_total':prepared['logical_bytes']})
        def save():
            result['observed_at_epoch'] = now()
            atomic_write_json(status, result, durable=True)
            atomic_write_json(root/(dataset+'.coverage-status.json'), result, durable=True)
        def report(event):
            result['progress'] = event
            save()
        save()
        try:
            proof = existing.match(private, stage, dataset, identity, index, root_name,
                                   guard=guard, install_lock=install_lock, progress=report)
            guard()
            result.update(state='completed', reason=None, matched_files=proof['files'],
                          matched_logical_bytes=proof['logical_bytes'],
                          comparison_identity=proof['identity_sha256'],
                          D_or_NAS_readback=False, source_files_deleted=0)
        except (OSError, ValueError, SnapshotError) as error:
            result.update(state='retrying', reason=type(error).__name__,
                          retry_after_epoch=now()+(15 if isinstance(error, PackedPublicationBusy) else 120))
            print(f'{type(error).__name__}: {error}', file=sys.stderr)
        result['finished_at_epoch'] = now()
        save()
        return result
    finally:
        os.close(owner)


def main():
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    _check_d_primary_mount(publisher.COLD)
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'
    private = private_history_root(publisher.COLD, version_policy(ROOT), initialize=False)
    next_window_check = 0.0
    next_mount_check = 0.0
    def guard():
        nonlocal next_window_check, next_mount_check
        current = time.monotonic()
        if current >= next_window_check:
            if not handoff.binance_handoff_window_allowed(for_start=False):
                raise BlockingIOError('tw_day_trade_priority_window')
            next_window_check = current+30
        if current >= next_mount_check:
            workspace_volume.check(c=config)
            _check_d_primary_mount(publisher.COLD)
            if private_history_root(publisher.COLD, version_policy(ROOT), initialize=False) != private:
                raise SnapshotError('coverage precheck D authority changed')
            next_mount_check = current+2
    print(json.dumps(run_once(root, private, guard=guard), ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
