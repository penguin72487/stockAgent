#!/usr/bin/env python3
"""Make a verified temporary E copy of an existing futures source bundle.

This never changes the original source, aliases, publication or training
configuration. The normal source owner and financial admission remain in force.
"""
from __future__ import annotations

from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_volume as volume
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.materialized_cache import process_references_many

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT/'data_tw_index_futures/preparation_sources/margin_sources'
NAME = 'futures-margin-source-e-temporary-20261103-v1'
REPORT = ROOT/'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1'/f'{NAME}.json'
BUDGET = 12*1024**3


def has_in_scope_hardlinks(db):
    """Use the completed source census; external hardlinks need no E alias."""
    row = db.execute("""
        SELECT 1 FROM files
        WHERE kind='file' AND json_extract(sig,'$[8]') > 1
        GROUP BY json_extract(sig,'$[0]'), json_extract(sig,'$[1]')
        HAVING count(*) > 1 LIMIT 1
    """).fetchone()
    return row is not None


def main(*, source=None, name=None, marker=None, report=None, budget=None, seed_hardlinks=False,
         preserve_symlinks=False, source_stream_no_readback=False):
    source = SOURCE if source is None else Path(source)
    name = NAME if name is None else name
    marker = Path('source_manifest.json') if marker is None else Path(marker)
    report = REPORT if report is None else Path(report)
    budget = BUDGET if budget is None else budget
    if (not name or Path(name).name != name or marker.is_absolute() or '..' in marker.parts
            or not marker.parts or type(budget) is not int or budget <= 0 or type(seed_hardlinks) is not bool
            or type(preserve_symlinks) is not bool or type(source_stream_no_readback) is not bool
            or source_stream_no_readback and (seed_hardlinks or preserve_symlinks)):
        raise ValueError('a bounded source name, relative marker and E budget are required')
    if os.geteuid() != 0 or source.is_symlink() or source.resolve(strict=True) != source:
        raise RuntimeError('the original futures source must be its enrolled real root')
    if (source/marker).resolve(strict=True) != source/marker:
        raise RuntimeError('the source marker must not traverse a redirected path')
    # The reservation below atomically accounts for concurrent E writers. Do
    # the physical admission once, after obtaining it and before payload I/O.
    initial_capacity = volume.check()
    downloads = volume.path('downloads')
    migration = volume.path('migration')
    relative_source = (source.relative_to(ROOT) if source.is_relative_to(ROOT)
                       else Path('data_tw_index_futures/preparation_sources')/source.name)
    destination = downloads/name/relative_source
    evidence = migration/name
    controls = migration/'provider-state-temporary-20261103/capacity-leases'
    with ExitStack() as locks:
        owner = locks.enter_context((volume.ENROLLMENT.parent/(name+'.lock')).open('a'))
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lease = locks.enter_context(reserve_capacity(controls, key=hashlib.sha256(name.encode()).hexdigest(),
            maximum_growth_bytes=budget, capacity_bytes=2**63-1,
            minimum_free_bytes=volume.configuration()['minimum_free_bytes']))
        capacity = volume.admit(budget + lease['other_reserved_growth_bytes'])
        refs = process_references_many([source], limit=100)
        if refs:
            raise RuntimeError('the source has a live owner; wait without changing it')
        marker_sha = relocation.digest(source/marker, no_follow=True)
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
        if destination.parent.resolve(strict=True) != destination.parent or evidence.resolve(strict=True) != evidence:
            raise RuntimeError('the E source copy path was redirected')
        seed_needed, symlink_count = False, 0
        if source_stream_no_readback:
            from stockagent.data_sync import source_stream_staging
            from stockagent.data_sync.workspace_source_handoff import binance_handoff_window_allowed
            next_window_check = 0.0
            def trading_guard():
                nonlocal next_window_check
                now = time.monotonic()
                if now >= next_window_check:
                    if not binance_handoff_window_allowed(for_start=False):
                        raise BlockingIOError('tw_day_trade_priority_window')
                    next_window_check = now + 30.0
            result = source_stream_staging.copy(source, destination, evidence, workers=4, budget_bytes=budget,
                                                guard=trading_guard)
            intent = json.loads((evidence/'intent.json').read_bytes())
            db = sqlite3.connect(f'file:{evidence / "files.sqlite"}?mode=ro', uri=True)
            try:
                bound_marker = db.execute("SELECT sha FROM files WHERE path=? AND kind='file' AND state='verified'",
                                          (marker.as_posix(),)).fetchone()
            finally:
                db.close()
            if (result.get('source_sha256_streamed') is not True
                    or result.get('destination_independent_readback') is not False
                    or bound_marker != (marker_sha,)):
                raise RuntimeError('source stream or source marker generation differs')
        else:
            db = relocation.index(source, destination, evidence, preserve_symlinks=preserve_symlinks)
            try:
                intent = json.loads((evidence/'intent.json').read_bytes())
                seed_needed = seed_hardlinks and has_in_scope_hardlinks(db)
                symlink_count = db.execute("SELECT count(*) FROM files WHERE kind='symlink'").fetchone()[0]
            finally:
                db.close()
            if intent['logical_bytes'] + intent['entries']*16384 + 1024**3 > budget:
                raise RuntimeError('the source grew beyond the reserved E staging budget')
            print(json.dumps({'state':'copying','files':intent['files'],'logical_bytes':intent['logical_bytes']}), flush=True)
        if seed_needed:
            # Seed a fresh/partial stage without overwriting any existing file.
            # The canonical copier below still checks every original generation,
            # complete namespace, full source SHA and independent E readback.
            atomic_write_json(evidence/'progress.json', {'state':'seeding_hardlinked_copy',
                'files_total':intent['files'], 'observed_at_epoch':time.time(), 'source_files_deleted':0})
            subprocess.run(['rsync', '--archive', '--hard-links', '--no-inc-recursive', '--numeric-ids',
                            '--ignore-existing', '--no-devices', '--no-specials', '--',
                            str(source)+'/', str(destination)+'/'], check=True, timeout=21600)
        if not source_stream_no_readback:
            result = relocation.copy(source, destination, evidence, workers=4, sync_batch_files=512, verify_workers=4,
                                     deduplicate_hardlink_verification=seed_needed,
                                     preserve_symlinks=preserve_symlinks)
            if (not result['all_source_sha256_and_destination_readback_verified']
                    or relocation.digest(source/marker, no_follow=True) != marker_sha
                    or relocation.digest(destination/marker, no_follow=True) != marker_sha):
                raise RuntimeError('futures source changed or independent readback failed')
        result_report = {'state':('source_stream_staged_without_destination_readback' if source_stream_no_readback
                                  else 'verified_temporary_e_source_copy'), 'source':str(source),
                  'e_destination':str(destination), 'relocation_evidence':str(evidence/'copy-result.json'),
                  'relocation_evidence_sha256':relocation.digest(evidence/'copy-result.json'),
                  'source_marker_relative':str(marker), 'source_marker_sha256':marker_sha, 'files':intent['files'],
                  'logical_bytes':intent['logical_bytes'], 'capacity_at_admission':capacity,
                  'initial_capacity':initial_capacity, 'source_files_deleted':0,
                  'source_alias_changed':False, 'trainer_or_publisher_repointed':False,
                  'hardlinked_seed_requested':seed_hardlinks,
                  'hardlinked_seed_used':seed_needed,
                  'hardlink_generation_proof_cache_hits':result.get('hardlink_generation_proof_cache_hits', 0),
                  'symlink_names_preserved':symlink_count,
                  'symlink_targets_independently_verified':False,
                  'destination_independent_readback': not source_stream_no_readback,
                  'financial_or_training_admission':False, 'observed_at_epoch':time.time()}
        atomic_write_json(report, result_report)
        print(json.dumps(result_report, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
