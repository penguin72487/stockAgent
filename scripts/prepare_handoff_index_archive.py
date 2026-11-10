#!/usr/bin/env python3
"""Seal an enrolled, version-different retained C source into E packages."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.run_stream_packed_repack import stage_growth_budget
from stockagent.data_sync import handoff_index_archive, handoff_source_repack, provider_workspace, workspace_volume
from stockagent.data_sync import workspace_source_handoff as h, workspace_relocation
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

ENROLLED = {
    h.OPTIONS_RAW_NAME: ('tw-index-options-raw-workspace', ['.download.lock'], True),
    h.BYBIT_NAME: ('bybit', ['.download.lock', '1m/.download.lock', 'daily/.download.lock'], True),
    h.MINUTE_RAW_NAME: ('tw-minute-source-workspace', ['.download.lock'], True),
    h.FUTURES_725_NAME: ('tw-futures-universe725-workspace', ['.download.lock'], True),
    h.FUTURES_765_NAME: ('tw-futures-universe765-workspace', ['.download.lock'], True),
    h.YAHOO_NAME: ('yahoo-market', ['.download.lock'], True),
    h.PEPPERSTONE_NAME: ('forex-pepperstone', ['.download.lock'], True),
    h.CFTC_NAME: ('cftc-legacy-pre2000-workspace', ['.download.lock', 'state/collector.lock'], True),
    h.FRED_NAME: ('fred-crypto-macro-workspace', ['.download.lock', 'state/collector.lock'], True),
    h.TAIFEX_PUBLIC_NAME: ('taifex-public-workspace',
                           ['.download.lock', 'state/collector.lock', 'rules/state/collector.lock'], True),
    h.LEGACY_PARQUET_NAME: ('legacy-parquet', ['.download.lock'], True),
    h.FREE_PUBLIC_NAME: ('free-public-context', ['.download.lock'], True),
    h.PUBLIC_ECONOMIC_NAME: ('public-economic-history', ['.download.lock', '.bea.lock', '.census.lock', '.moi.lock'], True),
    h.NEWS_NAME: ('news-training-workspace', ['.download.lock', '.build.lock'], True),
    h.NEWS_BODIES_NAME: ('news-fulltext-bodies-workspace', ['.download.lock'], True),
    h.HFT_NAME: ('tw-microstructure-train-workspace', ['.download.lock'], True),
    h.MINUTE_NAME: ('tw-minute-train-workspace', ['.download.lock'], False),
    h.FOREX_NAME: ('forex-frankfurter', ['.download.lock', 'official_v2/.download.lock'], False),
    h.OKX_NAME: ('okx', ['.download.lock', '1m/.download.lock', '1m/.quality_priority.lock',
                         'daily/.download.lock'], True),
    h.BINANCE_ARCHIVE_NAME: ('binance-public-archive', ['.download.lock'], True),
    h.BINANCE_NAME: ('binance', ['.download.lock', '1m/.download.lock', '1m/.quality_priority.lock',
                                 'daily/.download.lock'], False),
}
# This local-only source has no public generation to merge into.
PRIVATE_ONLY_DATASETS = frozenset({'public-economic-history', 'news-training-workspace', 'taifex-public-workspace',
                                   'news-fulltext-bodies-workspace', 'tw-microstructure-train-workspace',
                                   'tw-minute-train-workspace', 'tw-futures-universe725-workspace',
                                   'tw-futures-universe765-workspace', 'fred-crypto-macro-workspace',
                                   'cftc-legacy-pre2000-workspace', 'tw-index-options-raw-workspace'})
# These sources already matched a pinned public D generation, so there is no
# second private-to-public generation to merge after their retirement.
DIRECT_PUBLIC_COVERAGE_DATASETS = frozenset({'tw-minute-source-workspace'})
CONTRACT = 'stockagent.handoff_index_archive.v1'


def prepare_existing_minute(*, value, scope, destination, index, identity, stage, status,
                            source_locks, guard, report, retire):
    """Retire this one source using its already matched, pinned public D pack."""
    from stockagent.data_sync import minute_raw_existing_d as minute
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    if not retire:
        raise SnapshotError('minute existing D reuse is reserved for the retirement owner')
    stage.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('minute existing D stage redirected')
    _check_d_primary_mount(minute.COLD)
    with ExitStack() as stack:
        source_locks(stack)
        guard()
        report({'stage':'comparing_existing_D_inventory', 'files_completed':0, 'logical_bytes_processed':0})
        proof = minute.prepare(minute.COLD, stage, index, Path(value['evidence'])/'copy-result.json',
            identity=identity, copy_result_sha256=value['copy_result_sha256'],
            original_inode=value['original_inode'], original_lock_inode=value['original_lock_inode'],
            files=value['files'], logical_bytes=value['logical_bytes'], materialized=minute.MATERIALIZED,
            guard=guard, progress=report)
    receipt = minute.publication_receipt(proof, stage)
    atomic_write_json(stage/'existing-D-match.json', receipt['existing_match'], durable=True)
    result = {'contract':CONTRACT, 'state':'E_index_source_hashed_prepared', 'dataset':minute.DATASET,
        'source_identity':identity, 'prepared_sha256':proof['identity_sha256'],
        'files':proof['files'], 'logical_bytes':proof['logical_bytes'], 'E_packages':0,
        'C_payload_deleted':False, 'D_archive_committed':True, 'D_publication':receipt,
        'D_E_stage_cleanup_complete':True, 'E_stage_cleanup_reason':'no_new_packages',
        'NAS_accepted':False, 'observed_at_epoch':time.time()}
    atomic_write_json(status, result, durable=True)
    try:
        with ExitStack() as stack:
            stack.enter_context(h.lock_file(h.CONTROL/'owner.lock'))
            source_locks(stack)
            current = h.read_journal()
            for key in ('source', 'backup', 'destination', 'evidence', 'original_inode',
                        'original_lock_inode', 'destination_inode', 'copy_result_sha256',
                        'source_generation_overrides'):
                if not isinstance(current, dict) or current.get(key) != value.get(key):
                    raise SnapshotError('active minute handoff identity changed before C retirement')
            if current.get('state') != 'active' or not provider_workspace.exact_mount(scope.source, destination):
                raise SnapshotError('active minute E mount changed before C retirement')
            _check_d_primary_mount(minute.COLD)
            retired = handoff_source_repack.retire(scope.backup, stage, minute.DATASET, identity, index,
                original_inode=value['original_inode'], original_lock_inode=value['original_lock_inode'],
                guard=guard, progress=report, planning_progress=report, reuse_existing_minute_D=True)
        result.update(state='C_payload_retired_root_and_locks_preserved', C_payload_deleted=True,
                      C_retirement=retired, observed_at_epoch=time.time())
        atomic_write_json(status, result, durable=True)
    except (OSError, ValueError, SnapshotError) as error:
        atomic_write_json(status, {**result, 'state':'C_retirement_waiting',
            'error_type':type(error).__name__, 'observed_at_epoch':time.time()}, durable=True)
        raise
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, choices=sorted(ENROLLED))
    parser.add_argument('--retire', action='store_true')
    args = parser.parse_args()
    dataset, protected, supplemental = ENROLLED[args.source]
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('index archive root redirected')
    status = root/(dataset+'.status.json')
    if status.is_symlink():
        raise SnapshotError('index archive status redirected')
    previous = json.loads(status.read_text()) if status.is_file() else {}
    if previous and (not isinstance(previous, dict) or previous.get('contract') != CONTRACT
                     or previous.get('dataset') != dataset):
        raise SnapshotError('index archive status identity differs')
    if (args.retire and previous.get('state') == 'C_payload_retired_root_and_locks_preserved'
            and previous.get('C_payload_deleted') is True):
        print(json.dumps(previous, ensure_ascii=False))
        return
    last_report = 0.0
    with h.selected_source(args.source):
        value = h.read_journal()
        if not value or value.get('state') != 'active':
            raise SnapshotError('source handoff is not active')
        h._assert_retained(value)
        scope = h._scope()
        destination, index = Path(value['destination']), Path(value['evidence'])/'files.sqlite'
        if not provider_workspace.exact_mount(scope.source, destination):
            raise SnapshotError('current service source does not point to its enrolled E mount')
        # The old C source is quiesced independently. Current E writers keep
        # their own locks; the original handoff owner prevents an alias swap.
        def source_locks(stack):
            lock = scope.backup/'.download.lock'
            stack.enter_context(h._lock_supplemental_file(lock) if supplemental else h.lock_file(lock))
            for name in protected[1:]:
                stack.enter_context(h.lock_file(scope.backup/name))
            if args.source == h.BINANCE_NAME and (scope.backup/'15m').is_dir():
                raise SnapshotError('unexpected Binance 15m lock requires fixed archive enrollment')
        index_sig = workspace_relocation.signature(index)
        identity_body = {'contract': CONTRACT+'.source', 'source': args.source,
            'copy_result_sha256': value['copy_result_sha256'], 'original_inode': value['original_inode'],
            'original_lock_inode': value['original_lock_inode'], 'source_index_signature': index_sig,
            'protected_paths': protected, 'supplemental_root_lock': supplemental}
        overrides = value.get('source_generation_overrides')
        if overrides:
            identity_body['source_generation_overrides'] = overrides
        if args.source == h.FUTURES_765_NAME:
            identity_body['preserve_internal_symlinks'] = True
        identity = hashlib.sha256(json.dumps(identity_body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        stage = root/dataset/identity
        if previous.get('source_identity') not in (None, identity):
            raise SnapshotError('index archive source generation changed')
        work_window = h._source_work_window_guard()
        next_mount_check = 0.0
        def guard():
            nonlocal next_mount_check
            work_window()
            now = time.monotonic()
            if now >= next_mount_check:
                workspace_volume.check(c=config)
                h._assert_retained(value)
                next_mount_check = now+2.0
        def report(event):
            nonlocal last_report
            if time.monotonic()-last_report < 3 and event.get('stage') == 'streaming_source_into_E_packs':
                return
            last_report = time.monotonic()
            atomic_write_json(status, {'contract': CONTRACT, 'state': 'running', 'dataset': dataset,
                'source_identity': identity, 'files_total': value['files'],
                'logical_bytes_total': value['logical_bytes'], 'progress': event, 'observed_at_epoch': time.time()})
        if (args.source in h.HEAVY_SOURCES and not stage.exists()
                and (not h.binance_handoff_window_allowed(for_start=True)
                     or args.source == h.OPTIONS_RAW_NAME
                     and not h.options_raw_handoff_window_allowed(for_start=True))):
            raise SnapshotError('waiting for the protected day-trade window')
        if args.source == h.MINUTE_RAW_NAME:
            result = prepare_existing_minute(value=value, scope=scope, destination=destination, index=index,
                identity=identity, stage=stage, status=status, source_locks=source_locks,
                guard=guard, report=report, retire=args.retire)
            print(json.dumps(result, ensure_ascii=False))
            return
        budget = stage_growth_budget(stage, dataset, identity, value['logical_bytes'], value['files'], 0,
                                     already_committed=False)
        controls = workspace/'migration/provider-state-temporary-20261103/capacity-leases'
        with ExitStack() as stack:
            source_locks(stack)
            with reserve_capacity(controls, key=identity, maximum_growth_bytes=budget,
                    capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
                workspace_volume.admit(budget+lease['other_reserved_growth_bytes'], c=config)
                report({'stage': 'streaming_source_into_E_packs', 'files_completed': 0, 'logical_bytes_processed': 0})
                prepared = handoff_index_archive.prepare(scope.backup, stage, dataset, identity, index,
                    original_inode=value['original_inode'], original_lock_inode=value['original_lock_inode'],
                    preserved_paths=protected, supplemental_root_lock=supplemental, source_overrides=overrides,
                    guard=guard, progress=report, preserve_internal_symlinks=args.source == h.FUTURES_765_NAME)
        result = {'contract': CONTRACT, 'state': 'E_index_source_hashed_prepared', 'dataset': dataset,
            'source_identity': identity, 'prepared_sha256': prepared['identity_sha256'],
            'files': prepared['files'], 'logical_bytes': prepared['logical_bytes'],
            'E_packages': len(prepared['packages']), 'C_payload_deleted': False,
            'D_archive_committed': False, 'NAS_accepted': False, 'observed_at_epoch': time.time()}
        atomic_write_json(status, result, durable=True)
        if args.retire:
            try:
                with ExitStack() as stack:
                    stack.enter_context(h.lock_file(h.CONTROL/'owner.lock'))
                    source_locks(stack)
                    current = h.read_journal()
                    for key in ('source', 'backup', 'destination', 'evidence', 'original_inode',
                                'original_lock_inode', 'destination_inode', 'copy_result_sha256',
                                'source_generation_overrides'):
                        if not isinstance(current, dict) or current.get(key) != value.get(key):
                            raise SnapshotError('active handoff identity changed before C retirement')
                    if current.get('state') != 'active' or not provider_workspace.exact_mount(scope.source, destination):
                        raise SnapshotError('active E service mount changed before C retirement')
                    retired = handoff_source_repack.retire(scope.backup, stage, dataset, identity, index,
                        original_inode=value['original_inode'], original_lock_inode=value['original_lock_inode'],
                        guard=guard, progress=report, planning_progress=report,
                        preserve_external_hardlinks=args.source == h.TAIFEX_PUBLIC_NAME)
                result.update(state='C_payload_retired_root_and_locks_preserved', C_payload_deleted=True,
                              C_retirement=retired, observed_at_epoch=time.time())
                atomic_write_json(status, result, durable=True)
            except (OSError, ValueError, SnapshotError) as error:
                atomic_write_json(status, {**result, 'state': 'C_retirement_waiting',
                    'error_type': type(error).__name__, 'observed_at_epoch': time.time()}, durable=True)
                raise
        print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
