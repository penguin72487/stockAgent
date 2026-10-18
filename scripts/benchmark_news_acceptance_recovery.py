"""Compare exact full-receipt recovery on a fixed cohort without a writer.

Every phase reads every original JSON. The isolated SQLite index contains the
same committed pairs; an orphan/write is rejected. This is a receipt-stage
benchmark, not publisher throughput, boot acceptance or full-history validation.
"""
from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import hashlib
from importlib import metadata
import json
import locale
import os
from pathlib import Path
import resource
import sqlite3
import sys
import time
from types import SimpleNamespace
from urllib.parse import quote

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_queue import NewsQueue, source_path, utc_now
from downloader import news_fulltext_queue as queue_module
from downloader import news_receipt_io as receipt_io_module
from downloader.news_fulltext_queue import read_acceptance_receipt_input as _binary_receipt_input
from stockagent import private_json


_RECEIPT_PREFETCH_CHARACTERS = 1 << 20


def _binary_recovery(queue, paths):
    """Benchmark only: canonical SQL/orphan replay, alternate input I/O."""
    original_recover = queue._recover_acceptance_receipt
    def recover(path, relative, committed):
        original_recover(path, relative, committed, receipt_text=_binary_receipt_input(path))
    queue._recover_acceptance_receipt = recover
    try:
        queue.recover_acceptance(receipt_paths=paths)
    finally:
        queue._recover_acceptance_receipt = original_recover


def _text_recovery(queue, paths):
    """Original synchronous text reference, still canonical SQL/orphan replay."""
    original_recover = queue._recover_acceptance_receipt
    def recover(path, relative, committed):
        original_recover(path, relative, committed, receipt_text=path.read_text())
    queue._recover_acceptance_receipt = recover
    try:
        queue.recover_acceptance(receipt_paths=paths)
    finally:
        queue._recover_acceptance_receipt = original_recover


def _bounded_receipt_text(path):
    with path.open(mode='r') as handle:
        text = handle.read(_RECEIPT_PREFETCH_CHARACTERS + 1)
    return text if len(text) <= _RECEIPT_PREFETCH_CHARACTERS else None


def _prefetch_recovery(queue, paths, workers):
    """Benchmark-only I/O prototype, not a second SQL/acceptance pipeline.

    Production does not enable this candidate: warmed reads took more wall
    and CPU. Invoke canonical recovery and change only its explicit text input
    seam. Every error/validation/commit stays on the original owner thread.
    """
    if type(workers) is not int or not 1 <= workers <= 8:
        raise ValueError('bounded_receipt_read_ahead_workers_required')
    remaining, pending = iter(sorted(paths)), deque()
    original_recover = queue._recover_acceptance_receipt
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='news-receipt-read') as readers:
        for _ in range(8):
            path = next(remaining, None)
            if path is not None:
                pending.append((path, readers.submit(_bounded_receipt_text, path)))
        def recover(path, relative, committed):
            expected, future = pending.popleft()
            if path != expected:
                raise ValueError('original_receipt_read_order_required')
            text = future.result()
            following = next(remaining, None)
            if following is not None:
                pending.append((following, readers.submit(_bounded_receipt_text, following)))
            original_recover(path, relative, committed, receipt_text=text)
        queue._recover_acceptance_receipt = recover
        try:
            queue.recover_acceptance(receipt_paths=paths)
            if pending:
                raise ValueError('every_original_receipt_must_be_consumed')
        finally:
            queue._recover_acceptance_receipt = original_recover


def benchmark(root, *, maximum_receipts=4096, cycles=2, json_decode_only=False,
              read_ahead_workers=0, evict_receipt_pages=False, binary_read_only=False,
              batched_read_ahead_workers=0, adaptive_read_ahead=False, path_identity_only=False,
              adaptive_reprobe_only=False, seed_wait_bound_prefix=False):
    if type(maximum_receipts) is not int or not 1 <= maximum_receipts <= 8192:
        raise ValueError('bounded_receipt_cohort_required')
    if type(cycles) is not int or not 1 <= cycles <= 3:
        raise ValueError('bounded_ab_ba_cycles_required')
    if type(json_decode_only) is not bool:
        raise ValueError('explicit_json_decode_comparison_required')
    if type(read_ahead_workers) is not int or not 0 <= read_ahead_workers <= 8:
        raise ValueError('bounded_receipt_read_ahead_workers_required')
    if type(batched_read_ahead_workers) is not int or not 0 <= batched_read_ahead_workers <= 2:
        raise ValueError('bounded_batched_receipt_readers_required')
    if type(adaptive_read_ahead) is not bool:
        raise ValueError('explicit_adaptive_receipt_reading_required')
    if type(path_identity_only) is not bool:
        raise ValueError('explicit_receipt_path_comparison_required')
    if (type(adaptive_reprobe_only) is not bool or type(seed_wait_bound_prefix) is not bool
            or seed_wait_bound_prefix and not adaptive_reprobe_only):
        raise ValueError('explicit_adaptive_reprobe_comparison_required')
    if (type(evict_receipt_pages) is not bool or type(binary_read_only) is not bool
            or sum((json_decode_only, bool(read_ahead_workers), binary_read_only, bool(batched_read_ahead_workers), adaptive_read_ahead, path_identity_only, adaptive_reprobe_only)) > 1):
        raise ValueError('separate_receipt_io_and_decode_comparisons_required')
    if evict_receipt_pages and not (hasattr(os, 'posix_fadvise') and hasattr(os, 'POSIX_FADV_DONTNEED')):
        raise ValueError('receipt_only_page_advice_unavailable')
    implementation_paths = (Path(__file__), Path('downloader/news_fulltext_queue.py'), Path('downloader/news_receipt_io.py'),
                            Path('stockagent/private_json.py'))
    implementation_before = {str(path): sha256_file(path) for path in implementation_paths}
    root = root.resolve(strict=True)
    source = sqlite3.connect('file:' + quote(str(root / 'queue.sqlite3'), safe='/') + '?mode=ro', uri=True)
    try:
        source.execute('PRAGMA query_only=ON')
        pairs = source.execute('SELECT path,receipt_path FROM accepted_batches ORDER BY path LIMIT ?',
                               (maximum_receipts,)).fetchall()
    finally:
        source.close()
    pairs.sort(key=lambda pair: pair[1])
    if not pairs or len({r for _, r in pairs}) != len(pairs):
        raise ValueError('unique_committed_receipt_cohort_required')
    paths = [source_path(root, receipt) for _, receipt in pairs]
    cohort_json_bytes = sum(path.stat().st_size for path in paths)
    before = [sha256_file(path) for path in paths]
    decode_parity = None
    if json_decode_only or binary_read_only or batched_read_ahead_workers or adaptive_read_ahead or path_identity_only or adaptive_reprobe_only:
        # Outside timed recovery: compare all fields, not only parquet_path,
        # and bind the equality proof to the unchanged full receipt bytes.
        semantic_hashes, mismatches = [], []
        for path in paths:
            text = path.read_text()
            reference = json.dumps(private_json.loads(text) if binary_read_only or batched_read_ahead_workers or adaptive_read_ahead or path_identity_only or adaptive_reprobe_only else json.loads(text),
                                   sort_keys=True, ensure_ascii=True)
            candidate = json.dumps(private_json.loads(_binary_receipt_input(path) if binary_read_only or batched_read_ahead_workers or adaptive_read_ahead or path_identity_only or adaptive_reprobe_only else text),
                                   sort_keys=True, ensure_ascii=True)
            semantic_hashes.append(hashlib.sha256(reference.encode()).hexdigest())
            if reference != candidate:
                mismatches.append(str(path.relative_to(root)))
        decode_parity = {'all_fields_equal': not mismatches, 'mismatches': mismatches,
            'full_json_sha256': hashlib.sha256(json.dumps(semantic_hashes).encode()).hexdigest(),
            'native_available': private_json.native_available(),
            'text_read_and_stdlib_fallback_preserved': True,
            'binary_input_only_comparison': binary_read_only}
        if mismatches:
            raise ValueError('full_receipt_decode_parity_required')
    queue = NewsQueue.__new__(NewsQueue)
    queue.root = root
    queue.metrics = SimpleNamespace(measure=lambda *a, **k: nullcontext())
    queue.db = sqlite3.connect(':memory:')
    queue.db.row_factory = sqlite3.Row
    queue.db.executescript('CREATE TABLE accepted_batches(path TEXT PRIMARY KEY,receipt_path TEXT NOT NULL);'
                          'CREATE INDEX accepted_batches_receipt_path ON accepted_batches(receipt_path,path);')
    queue.db.executemany('INSERT INTO accepted_batches VALUES (?,?)', pairs)
    queue.db.commit()
    changes = queue.db.total_changes

    def reject_write(*args, **kwargs):
        raise ValueError('benchmark_must_not_replay_orphan_or_write')
    queue._accept_records = reject_write

    def legacy():
        # The original per-receipt algorithm, with the same JSON validation.
        for path in sorted(paths):
            receipt = json.loads(path.read_text())
            if not queue.db.execute('SELECT 1 FROM accepted_batches WHERE path=?',
                                    (receipt['parquet_path'],)).fetchone():
                reject_write()

    results = []
    original_decode = queue_module.decode_private_json
    original_queue_parent = queue_module.receipt_parent_matches
    original_io_parent = receipt_io_module.receipt_parent_matches
    original_prefetch_limit = queue_module.RECEIPT_PREFETCH_MAX_CHUNKS
    original_choose = queue_module.adaptive_receipt_readers
    path_parity = None
    reference_mode, candidate_mode = (('sticky_batched_prefetch', 'periodic_batched_prefetch') if adaptive_reprobe_only else
                                      ('parent_path_objects', 'lexical_parent_parts') if path_identity_only else
                                      ('binary_serial', 'adaptive_batched_prefetch') if adaptive_read_ahead else
                                      ('binary_serial', 'binary_batched_prefetch') if batched_read_ahead_workers else
                                      ('text_read', 'binary_read') if binary_read_only else
                                      ('serial_read', 'prefetch_read') if read_ahead_workers else
                                      ('stdlib_json', 'shared_private_json') if json_decode_only
                                      else ('legacy', 'bounded'))
    try:
        if path_identity_only:
            path_parity = all(original_io_parent(path, root / 'receipts') == (path.parent == root / 'receipts')
                              for path in paths)
            if not path_parity:
                raise ValueError('original_lexical_parent_equality_required')
        for cycle in range(cycles):
            for mode in (reference_mode, candidate_mode, candidate_mode, reference_mode):
                if evict_receipt_pages:
                    # Optional isolated cache experiment, never global
                    # drop_caches, receipt mutation or other providers' pages.
                    # Linux is allowed to ignore the advice: no cold-boot claim.
                    for path in paths:
                        with path.open('rb') as handle:
                            os.posix_fadvise(handle.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
                calls = {'committed_lookup_statements': 0}
                def trace(statement):
                    if statement.startswith(('SELECT 1 FROM accepted_batches', 'SELECT receipt_path,path FROM accepted_batches')):
                        calls['committed_lookup_statements'] += 1
                queue.db.set_trace_callback(trace)
                cpu, wall = time.process_time(), time.perf_counter()
                if adaptive_reprobe_only:
                    # A reference slice covering all remaining paths is the
                    # original sticky mode: once admitted, no further probe.
                    # Both variants retain the original synchronous probes
                    # before admission and identical complete recovery.
                    queue_module.RECEIPT_PREFETCH_MAX_CHUNKS = (original_prefetch_limit if mode == candidate_mode
                        else (len(paths) + 127) // 128)
                    if seed_wait_bound_prefix:
                        first_probe = [True]
                        def choose(probe):
                            seed = first_probe[0]
                            first_probe[0] = False
                            return 2 if seed else original_choose(probe)
                        queue_module.adaptive_receipt_readers = choose
                    queue.recover_acceptance(receipt_paths=paths)
                    queue_module.adaptive_receipt_readers = original_choose
                elif path_identity_only:
                    predicate = original_io_parent if mode == candidate_mode else lambda path, directory: path.parent == directory
                    queue_module.receipt_parent_matches = predicate
                    receipt_io_module.receipt_parent_matches = predicate
                    queue.recover_acceptance(receipt_paths=paths)
                elif adaptive_read_ahead:
                    queue.recover_acceptance(receipt_paths=paths, read_ahead_workers=None if mode == candidate_mode else 0)
                elif batched_read_ahead_workers:
                    queue.recover_acceptance(receipt_paths=paths,
                        read_ahead_workers=batched_read_ahead_workers if mode == candidate_mode else 0)
                elif binary_read_only:
                    if mode == reference_mode:
                        _text_recovery(queue, paths)
                    else:
                        _binary_recovery(queue, paths)
                elif read_ahead_workers:
                    if mode == reference_mode:
                        _text_recovery(queue, paths)
                    else:
                        _prefetch_recovery(queue, paths, read_ahead_workers)
                elif json_decode_only:
                    queue_module.decode_private_json = json.loads if mode == reference_mode else original_decode
                    _text_recovery(queue, paths)
                elif mode == 'legacy':
                    legacy()
                else:
                    queue.recover_acceptance(receipt_paths=paths)
                results.append({'cycle': cycle + 1, 'mode': mode,
                    'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu,
                    'receipt_json_reads': len(paths), **calls,
                    'receipt_io': dict(queue.acceptance_receipt_io) if adaptive_read_ahead or path_identity_only or adaptive_reprobe_only else None,
                    'sqlite_changes': queue.db.total_changes - changes})
        query_plan = [row[-1] for row in queue.db.execute('EXPLAIN QUERY PLAN SELECT receipt_path,path '
            'FROM accepted_batches INDEXED BY accepted_batches_receipt_path WHERE receipt_path IN (?)', (pairs[0][1],))]
    finally:
        queue_module.decode_private_json = original_decode
        queue_module.receipt_parent_matches = original_queue_parent
        receipt_io_module.receipt_parent_matches = original_io_parent
        queue_module.RECEIPT_PREFETCH_MAX_CHUNKS = original_prefetch_limit
        queue_module.adaptive_receipt_readers = original_choose
        queue.db.set_trace_callback(None)
        queue.db.close()
    unchanged = before == [sha256_file(path) for path in paths]
    medians = {}
    from statistics import median
    for mode in (reference_mode, candidate_mode):
        selected = [r for r in results if r['mode'] == mode]
        medians[mode] = {key: median(r[key] for r in selected) for key in ('wall_seconds', 'cpu_seconds')}
    accepted = unchanged and all(r['sqlite_changes'] == 0 and r['committed_lookup_statements'] == (
        len(paths) if r['mode'] == 'legacy' else (len(paths) + 127) // 128) for r in results)
    implementation_after = {str(path): sha256_file(path) for path in implementation_paths}
    accepted = accepted and implementation_before == implementation_after
    return {'contract_version': 8 if adaptive_reprobe_only else 7 if path_identity_only else 6 if adaptive_read_ahead else 5 if batched_read_ahead_workers else 4 if binary_read_only else 3 if read_ahead_workers else 2 if json_decode_only else 1,
        'accepted': accepted, 'observed_at_utc': utc_now(),
        'scope': 'AB/BA instrumented fixed committed-receipt stage in isolated SQLite; not live boot or publisher maximum',
        'cohort_receipts': len(paths), 'cohort_sha256': hashlib.sha256(json.dumps(
            [[*pair, digest] for pair, digest in zip(pairs, before)], sort_keys=True).encode()).hexdigest(),
        'cohort_json_bytes': cohort_json_bytes,
        'decoder_runtime': {'python': sys.version, 'native_available': private_json.native_available(),
            'native_distribution_version': metadata.version('msgspec') if private_json.native_available() else None},
        'cache_scope': ('receipt-only POSIX_FADV_DONTNEED requested before each timed trial; eviction effectiveness unverified; not a cold boot'
                        if evict_receipt_pages else 'same receipt cohort reread after input hashing/full-value parity; not a cold boot or page-cache eviction test'),
        'receipt_only_page_advice_requested': evict_receipt_pages,
        'candidate_read_ahead_workers': read_ahead_workers,
        'candidate_batched_read_ahead_workers': batched_read_ahead_workers,
        'candidate_adaptive_read_ahead': adaptive_read_ahead,
        'candidate_adaptive_reprobe_only': adaptive_reprobe_only,
        'wait_bound_initial_admission_seeded_equally': seed_wait_bound_prefix,
        'initial_admission_seed_is_not_a_cold_boot_measurement': seed_wait_bound_prefix,
        'candidate_path_identity_only': path_identity_only,
        'path_identity_parity': path_parity,
        'receipt_json_reads_basis': 'complete logical JSON inputs decoded, not physical reads; oversized prefetched inputs fall back to the full owner read',
        'candidate_binary_read_only': binary_read_only,
        'default_receipt_encoding': locale.getpreferredencoding(False),
        'source_receipts_unchanged': unchanged, 'production_database_written': False, 'publisher_requests': 0,
        'results': results, 'medians': medians, 'decode_parity': decode_parity,
        'wall_speedup': medians[reference_mode]['wall_seconds'] / medians[candidate_mode]['wall_seconds'],
        'cpu_speedup': medians[reference_mode]['cpu_seconds'] / medians[candidate_mode]['cpu_seconds'],
        'peak_process_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'query_plan': query_plan, 'all_history_complete': False,
        'implementation_sha256': implementation_before,
        'implementation_unchanged': implementation_before == implementation_after}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_news_fulltext'))
    parser.add_argument('--maximum-receipts', type=int, default=4096)
    parser.add_argument('--cycles', type=int, default=2)
    parser.add_argument('--json-decode-only', action='store_true',
                        help='Compare stdlib and existing private-cache full JSON decoding; identical bounded SQL and read order')
    parser.add_argument('--read-ahead-workers', type=int, default=0,
                        help='Compare serial receipt I/O against a bounded I/O-only pool (1..8)')
    parser.add_argument('--binary-read-only', action='store_true',
                        help='Compare original text reads with exact ASCII-byte input; same complete receipt decoder and SQL')
    parser.add_argument('--batched-read-ahead-workers', type=int, default=0,
                        help='Compare original binary reads against at most two SQL-sized I/O batch readers')
    parser.add_argument('--adaptive-read-ahead', action='store_true',
                        help='Compare serial input against sampled wait-bound activation of batched readers')
    parser.add_argument('--path-identity-only', action='store_true',
                        help='Compare original lexical parent objects against identical POSIX components; same adaptive full recovery')
    parser.add_argument('--adaptive-reprobe-only', action='store_true',
                        help='Compare original sticky admission with periodic serial probes; identical complete recovery')
    parser.add_argument('--seed-wait-bound-prefix', action='store_true',
                        help='Seed initial wait admission equally for a warm-tail regression scenario; not physical cold boot')
    parser.add_argument('--evict-receipt-pages', action='store_true',
                        help='Request per-file Linux DONTNEED advice before trials; no global cache eviction or source mutation')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    result = benchmark(args.root, maximum_receipts=args.maximum_receipts, cycles=args.cycles,
                       json_decode_only=args.json_decode_only, read_ahead_workers=args.read_ahead_workers,
                       evict_receipt_pages=args.evict_receipt_pages, binary_read_only=args.binary_read_only,
                       batched_read_ahead_workers=args.batched_read_ahead_workers, adaptive_read_ahead=args.adaptive_read_ahead,
                       path_identity_only=args.path_identity_only, adaptive_reprobe_only=args.adaptive_reprobe_only,
                       seed_wait_bound_prefix=args.seed_wait_bound_prefix)
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ('accepted', 'cohort_receipts', 'medians', 'wall_speedup', 'publisher_requests')}))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
