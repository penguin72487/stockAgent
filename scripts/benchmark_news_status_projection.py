"""Pinned read-only coverage projection replay; never open an HTTP owner."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import nullcontext
import cProfile
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import pstats
import sqlite3
import statistics
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0,str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import NewsQueue, summarize_bulk_coverage, summarize_discovery_status, utc_now
from downloader.news_fulltext_status import (
    bulk_item_error_count, copy_status_value, _JSON_SCALARS, _NOT_COPIED,
    BULK_COVERAGE_SCAN_SQL, bulk_coverage_rows, scan_bulk_coverage,
)


def legacy_discovery_status(db, now):
    statements = (
        ('SELECT count(*) FROM discovery WHERE superseded_by_windows_json IS NULL AND due<=?', (now,)),
        ('SELECT count(*) FROM discovery WHERE superseded_by_windows_json IS NULL AND error_code IS NOT NULL', ()),
        ('SELECT count(*) FROM discovery WHERE superseded_by_windows_json IS NOT NULL', ()),
        ('SELECT count(*) FROM discovery WHERE superseded_by_windows_json IS NOT NULL AND error_code IS NOT NULL', ()),
        ("SELECT count(*) FROM discovery WHERE kind IN ('wordpress_posts','cnyes_news','rss_fulltext') "
         'AND superseded_by_windows_json IS NULL AND due<=?', (now,)),
    )
    return tuple(db.execute(statement, params).fetchone()[0] for statement, params in statements)


def legacy_bulk_item_error_count(db):
    return db.execute('SELECT count(*) FROM bulk_item_errors').fetchone()[0]


def scan_discovery_status(db, now):
    """Exact previous single-table projection, not the older five-query path."""
    return tuple(db.execute('''SELECT coalesce(sum(superseded_by_windows_json IS NULL AND due<=?),0),
        coalesce(sum(superseded_by_windows_json IS NULL AND error_code IS NOT NULL),0),
        coalesce(sum(superseded_by_windows_json IS NOT NULL),0),
        coalesce(sum(superseded_by_windows_json IS NOT NULL AND error_code IS NOT NULL),0),
        coalesce(sum(kind IN ('wordpress_posts','cnyes_news','rss_fulltext') AND superseded_by_windows_json IS NULL AND due<=?),0)
        FROM discovery''', (now,now)).fetchone())


def _recursive_status_copy_reference(value, memo=None):
    """Previous scalar-recursive copy for a bounded A/B heartbeat replay."""
    kind = type(value)
    if kind in _JSON_SCALARS:
        return value
    if kind not in (dict, list):
        return deepcopy(value, memo)
    if memo is None:
        memo = {}
    prior = memo.get(id(value), _NOT_COPIED)
    if prior is not _NOT_COPIED:
        return prior
    result = {} if kind is dict else []
    memo[id(value)] = result
    if kind is dict:
        for key, item in value.items():
            result[key if type(key) is str else deepcopy(key, memo)] = _recursive_status_copy_reference(item, memo)
    else:
        result.extend(_recursive_status_copy_reference(item, memo) for item in value)
    return result


def profile_current_status(root, registry, repetitions=5, *, compare_discovery_summary=False,
                           compare_bulk_error_summary=False, compare_discovery_transactional=False,
                           compare_scope_cache=False, compare_capacity_copy=False, verify_body_states=False,
                           compare_cycle_publication=False, capacity_copy_reference='deepcopy',
                           compare_bulk_coverage_summary=False, compare_service_transition=False):
    """Replay the canonical whole heartbeat without an owner or source writes.

    The only publisher is replaced with exact JSON serialization in this
    process. SQLite is read-only, one WAL snapshot and frozen clocks keep
    outputs comparable. No HTTP client or NewsQueue initializer is opened.
    This excludes live metrics locking and filesystem publication latency.
    """
    if not 3 <= repetitions <= 15:
        raise ValueError('bounded_repetition_count_required')
    if sum((compare_discovery_summary, compare_bulk_error_summary, compare_discovery_transactional,
            compare_scope_cache, compare_capacity_copy, compare_cycle_publication,
            compare_bulk_coverage_summary, compare_service_transition)) > 1:
        raise ValueError('one_projection_comparison_per_snapshot_required')
    if (capacity_copy_reference not in {'deepcopy', 'recursive'}
            or capacity_copy_reference != 'deepcopy' and not compare_capacity_copy):
        raise ValueError('invalid_capacity_copy_reference')
    payload = (root / 'status.json').read_bytes()
    published = json.loads(payload)
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        if not owner or owner[0] != published.get('owner_id'):
            raise ValueError('status_database_owner_mismatch')
        view = NewsQueue.__new__(NewsQueue)
        view.root, view.db, view.registry, view.owner = root, db, registry, owner[0]
        if 'acceptance_receipt_io' in published:
            view.acceptance_receipt_io = deepcopy(published['acceptance_receipt_io'])
        view.metrics = SimpleNamespace(snapshot=lambda: deepcopy(published['performance']),
                                       measure=lambda *args, **kwargs: nullcontext())
        view.http_capacity_snapshot = lambda: copy_status_value(published['http_capacity'])
        view._original_url_started = time.monotonic() - 120
        view._original_url_start_count = 0
        serialized = []
        bulk_coverage_verification = None
        if compare_bulk_coverage_summary:
            wall, cpu = time.perf_counter(), time.process_time()
            oracle = [tuple(row) for row in scan_bulk_coverage(db)]
            native = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                "AND name='bulk_endpoint_metrics'").fetchone() is not None
            if not native:
                # TEMP state exists only in this pinned read-only connection.
                # The production owner performs its own atomic migration.
                db.execute('CREATE TEMP TABLE bulk_endpoint_metrics AS '+BULK_COVERAGE_SCAN_SQL)
            actual = [tuple(row) for row in bulk_coverage_rows(db)]
            if actual != oracle:
                raise ValueError('bulk_endpoint_summary_disagrees_with_source_snapshot')
            bulk_coverage_verification = {'contract_version': 1, 'accepted': True,
                'verification_and_temporary_seed': {'wall_seconds': time.perf_counter()-wall,
                    'cpu_seconds': time.process_time()-cpu},
                'source_endpoint_groups': len(oracle), 'coverage_scopes': sum(row[2] for row in oracle),
                'seeded_in_temporary_table': not native, 'live_transactional_summary_verified': native,
                'source_table_mutations': 0, 'schema_migrated': False,
                'source_projection_sha256': hashlib.sha256(json.dumps(oracle, ensure_ascii=False).encode()).hexdigest()}
        body_state_verification = None
        if verify_body_states:
            # One independent source scan, not one scan per heartbeat/trial.
            # Before owner adoption, only a TEMP relation may supply the new
            # derived column. Never migrate or repair a live writer's schema.
            wall, cpu = time.perf_counter(), time.process_time()
            oracle = [tuple(row) for row in db.execute('''SELECT provider,state,count(*),
                coalesce(sum(body_bytes),0),sum(body_sha256 IS NOT NULL) FROM articles
                GROUP BY provider,state ORDER BY provider,state''')]
            scan = {'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu}
            native = 'stored_body_urls' in {row[1] for row in db.execute('PRAGMA main.table_info(state_metrics)')}
            if not native:
                db.execute('''CREATE TEMP TABLE state_metrics(provider TEXT,state TEXT,articles INTEGER,
                    body_bytes INTEGER,stored_body_urls INTEGER,PRIMARY KEY(provider,state)) WITHOUT ROWID''')
                db.executemany('INSERT INTO temp.state_metrics VALUES (?,?,?,?,?)', oracle)
            actual = [tuple(row) for row in db.execute('''SELECT provider,state,articles,body_bytes,stored_body_urls
                FROM state_metrics WHERE articles>0 ORDER BY provider,state''')]
            if actual != oracle:
                raise ValueError('article_body_state_counters_disagree_with_source_snapshot')
            body_state_verification = {'contract_version': 1, 'accepted': True, 'source_scan': scan,
                'source_scans': 1, 'provider_state_groups': len(oracle),
                'known_url_heads': sum(row[2] for row in oracle),
                'stored_body_url_heads': sum(row[4] for row in oracle),
                'source_projection_sha256': hashlib.sha256(json.dumps(oracle, ensure_ascii=False).encode()).hexdigest(),
                'seeded_in_temporary_table': not native, 'live_transactional_counters_verified': native,
                'source_table_mutations': 0, 'schema_migrated': False}

        def serialize_without_publication(path, result, *, durable=False, compact=False):
            if path != root / 'status.json' or durable:
                raise ValueError('unexpected_status_publication')
            serialized.append((json.dumps(result, ensure_ascii=False,
                indent=None if compact else 2, separators=(',', ':') if compact else None) + '\n').encode())

        summary_seeded_in_temporary_table = False
        discovery_seeded_in_temporary_tables = False
        discovery_seed_seconds = None
        discovery_groups = None
        bulk_error_rows = None
        if compare_discovery_transactional:
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_static_counts'").fetchone()
            if not exists:
                # Scratch-only TEMP projections inside the same pinned WAL
                # read. No source owner or main/WAL migration is created.
                begun = time.perf_counter()
                db.execute('''CREATE TEMP TABLE discovery_static_counts AS SELECT 1 AS singleton,
                    coalesce(sum(superseded_by_windows_json IS NULL AND error_code IS NOT NULL),0) AS active_errors,
                    coalesce(sum(superseded_by_windows_json IS NOT NULL),0) AS archived_rows,
                    coalesce(sum(superseded_by_windows_json IS NOT NULL AND error_code IS NOT NULL),0) AS archived_errors FROM discovery''')
                db.execute('''CREATE TEMP TABLE discovery_deadline_counts(due REAL NOT NULL,
                    body_capable INTEGER NOT NULL,rows INTEGER NOT NULL,PRIMARY KEY(due,body_capable)) WITHOUT ROWID''')
                db.execute('''INSERT INTO temp.discovery_deadline_counts SELECT due,
                    (kind IN ('wordpress_posts','cnyes_news','rss_fulltext')),count(*) FROM discovery
                    WHERE superseded_by_windows_json IS NULL AND due IS NOT NULL GROUP BY 1,2''')
                discovery_seed_seconds = time.perf_counter()-begun
                discovery_seeded_in_temporary_tables = True
            discovery_groups = db.execute('SELECT count(*) FROM discovery_deadline_counts').fetchone()[0]
            now = time.time()
            if summarize_discovery_status(db,now) != scan_discovery_status(db,now):
                raise ValueError('discovery_transactional_summary_mismatch')
        if compare_bulk_error_summary:
            bulk_error_rows = legacy_bulk_item_error_count(db)
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                "AND name='bulk_item_error_metrics'").fetchone()
            if not exists:
                # A pre-migration owner stays untouched. SQLite mode=ro still
                # permits an isolated TEMP relation, never a main/WAL write.
                # Trigger correctness is a separate test/live acceptance gate.
                db.execute('CREATE TEMP TABLE bulk_item_error_metrics(singleton INTEGER PRIMARY KEY, rows INTEGER)')
                db.execute('INSERT INTO bulk_item_error_metrics VALUES (1,?)', (bulk_error_rows,))
                summary_seeded_in_temporary_table = True
            if bulk_item_error_count(db) != bulk_error_rows:
                raise ValueError('bulk_error_incremental_count_mismatch')
        from downloader import news_fulltext_cnyes_history as history
        scope_parser = history.scope_key
        uncached_scope_parser = lambda url: history._parse_scope_key(url, history.CNYES_QUERY_DAY_OFFSET_SECONDS)
        if compare_scope_cache:
            # Only this isolated reader's pure parse cache is cleared. Live
            # evidence, source rows and the acquisition owner's cache are not.
            history._cached_scope_key.cache_clear()
        observed, epoch, monotonic = utc_now(), time.time(), time.monotonic()
        projectors = ({'legacy': (scan_discovery_status, bulk_item_error_count),
                       'summary': (summarize_discovery_status, bulk_item_error_count)}
                      if compare_discovery_transactional else
                      {'legacy': (legacy_discovery_status, bulk_item_error_count),
                       'summary': (summarize_discovery_status, bulk_item_error_count)}
                      if compare_discovery_summary else
                      {'legacy': (summarize_discovery_status, legacy_bulk_item_error_count),
                       'summary': (summarize_discovery_status, bulk_item_error_count)}
                      if compare_bulk_error_summary else
                      {'legacy': (summarize_discovery_status, bulk_item_error_count),
                       'summary': (summarize_discovery_status, bulk_item_error_count)}
                      if compare_scope_cache or compare_capacity_copy or compare_cycle_publication or compare_bulk_coverage_summary or compare_service_transition else
                      {'current': (summarize_discovery_status, bulk_item_error_count)})
        measurements, identities = {name: [] for name in projectors}, {name: set() for name in projectors}
        sizes, publication_counts = {}, {name: [] for name in projectors}

        def project(name):
            if compare_service_transition:
                cycle = deepcopy(published.get('cycle', {'article_attempts_started': 0,
                    'durably_accepted_versions': 0, 'http_requests': {}}))
                if name == 'legacy':
                    view.status(activity='cycle_complete', cycle=cycle)
                return view.status(activity='waiting_next_due', cycle=cycle)
            if not compare_cycle_publication:
                return view.status(activity='read_only_projection_replay')
            result = view.status(activity='read_only_projection_replay', publish=False)
            if name == 'legacy':
                view.publish_status(result)
            # The same captured epilogue is appended by both timing arms.
            # It is a local replay fixture, never new acquisition progress.
            result['cycle'] = deepcopy(published.get('cycle', {'article_attempts_started': 0,
                'durably_accepted_versions': 0, 'http_requests': {}}))
            result['http_counters_basis'] = 'since current owner started; cycle contains separate deltas'
            view.publish_status(result)
            return result

        with (patch('downloader.news_fulltext_queue.atomic_write_json', serialize_without_publication),
              patch('downloader.news_fulltext_queue.utc_now', return_value=observed),
              patch('time.time', return_value=epoch), patch('time.monotonic', return_value=monotonic)):
            for number in range(repetitions):
                order = list(projectors) if number % 2 == 0 else list(reversed(projectors))
                for name in order:
                    discovery_projector, error_projector = projectors[name]
                    if compare_capacity_copy:
                        reference = deepcopy if capacity_copy_reference == 'deepcopy' else _recursive_status_copy_reference
                        copy_value = reference if name == 'legacy' else copy_status_value
                        view.http_capacity_snapshot = lambda: copy_value(published['http_capacity'])
                    cache_before = history._cached_scope_key.cache_info()._asdict() if compare_scope_cache else None
                    with (patch('downloader.news_fulltext_queue.summarize_discovery_status', discovery_projector),
                          patch('downloader.news_fulltext_queue.bulk_item_error_count', error_projector),
                          patch('downloader.news_fulltext_queue.bulk_coverage_rows', scan_bulk_coverage
                                if compare_bulk_coverage_summary and name == 'legacy' else bulk_coverage_rows),
                          patch.object(history, 'scope_key', uncached_scope_parser
                                      if compare_scope_cache and name == 'legacy' else scope_parser)):
                        started_wall, started_cpu = time.perf_counter(), time.process_time()
                        project(name)
                        measurements[name].append({'wall_seconds': time.perf_counter() - started_wall,
                                                   'cpu_seconds': time.process_time() - started_cpu})
                    if compare_scope_cache:
                        measurements[name][-1].update(scope_cache_before=cache_before,
                            scope_cache_after=history._cached_scope_key.cache_info()._asdict())
                    publication_counts[name].append(len(serialized))
                    output = serialized[-1]
                    serialized.clear()
                    sizes[name] = len(output)
                    identities[name].add(hashlib.sha256(output).hexdigest())
            if compare_capacity_copy:
                # The final ABBA timing arm can be the reference. Profile the
                # current implementation explicitly, not that arm's closure.
                view.http_capacity_snapshot = lambda: copy_status_value(published['http_capacity'])
            profiler = cProfile.Profile(timer=time.process_time)
            queries = {}

            class TimedReads:
                def execute(self, statement, parameters=()):
                    start_wall, start_cpu = time.perf_counter(), time.process_time()
                    cursor = db.execute(statement, parameters)
                    item = queries.setdefault(statement, {'calls': 0, 'wall_seconds': 0, 'cpu_seconds': 0})
                    item['calls'] += 1
                    item['wall_seconds'] += time.perf_counter() - start_wall
                    item['cpu_seconds'] += time.process_time() - start_cpu
                    return cursor

            view.db = TimedReads()
            profiler.runcall(project, 'summary' if compare_cycle_publication or compare_service_transition else 'current')
        stats = pstats.Stats(profiler)
        functions = sorted(stats.stats.items(), key=lambda item: -item[1][3])[:20]
        medians = {name: {unit: statistics.median(m[unit] for m in samples)
                         for unit in ('wall_seconds', 'cpu_seconds')} for name, samples in measurements.items()}
        digests = set().union(*identities.values())
        return {'contract_version': 11 if compare_service_transition else 10 if compare_bulk_coverage_summary else 9 if compare_capacity_copy and capacity_copy_reference == 'recursive' else 8 if compare_cycle_publication else 7 if compare_capacity_copy else 6 if verify_body_states else 5,
                'accepted': len(digests) == 1,
                'observed_at_utc': observed, 'owner_id': owner[0],
                'input_status_sha256': hashlib.sha256(payload).hexdigest(),
                'serialized_status_bytes': sizes,
                'deterministic_projection_sha256': {name: sorted(values) for name, values in identities.items()},
                'measurements': measurements, 'repetitions': repetitions,
                'medians': medians,
                **({'body_state_verification': body_state_verification} if verify_body_states else {}),
                **({'bulk_coverage_verification': bulk_coverage_verification} if compare_bulk_coverage_summary else {}),
                'profiler_time_basis': 'explicit process CPU clock; profiled run excluded from timing medians',
                'speedups': {unit: medians['legacy'][unit] / medians['summary'][unit]
                             for unit in ('wall_seconds', 'cpu_seconds')}
                             if compare_discovery_summary or compare_bulk_error_summary or compare_discovery_transactional or compare_scope_cache or compare_capacity_copy or compare_cycle_publication or compare_bulk_coverage_summary or compare_service_transition else None,
                **({'service_transition': {'publication_counts': publication_counts,
                    'same_waiting_projection': len(digests) == 1,
                    'comparison': 'two complete projections versus one; cycle counters retained in both final views',
                    'disk_admission_and_wake_latency_excluded': True,
                    'profiled_implementation': 'summary'}} if compare_service_transition else {}),
                **({'cycle_publication': {'publication_counts': publication_counts,
                    'all_final_values_preserved': len(digests) == 1,
                    'epilogue_basis': 'captured cycle or explicit empty replay fixture; no acquired work implied',
                    'profiled_implementation': 'summary'}} if compare_cycle_publication else {}),
                **({'capacity_copy': {'contract_version': 1, 'all_published_values_preserved': len(digests) == 1,
                    'input': 'fixed published HTTP capacity projection; no controller tick or HTTP locks',
                    'reference_implementation': capacity_copy_reference,
                    'profiled_implementation': 'summary',
                    'history_windows_removed': 0, 'source_calibration_mutated': False}}
                   if compare_capacity_copy else {}),
                **({'scope_parse_cache': {'maximum_entries': history.SCOPE_KEY_CACHE_SIZE,
                    'maximum_url_chars': history.SCOPE_KEY_CACHE_MAX_URL_CHARS,
                    'first_summary_trial_includes_cold_cache': True,
                    'source_rows_cached': False, 'date_offset_in_cache_key': True}}
                   if compare_scope_cache else {}),
                'discovery_summary_seeded_in_temporary_tables': discovery_seeded_in_temporary_tables,
                'discovery_temporary_seed_wall_seconds': discovery_seed_seconds,
                'discovery_deadline_groups': discovery_groups,
                'live_discovery_summary_verified': bool(compare_discovery_transactional and not discovery_seeded_in_temporary_tables),
                'bulk_item_error_rows': bulk_error_rows,
                'bulk_error_summary_seeded_in_temporary_table': summary_seeded_in_temporary_table,
                'live_incremental_bulk_error_state_verified': bool(compare_bulk_error_summary and
                                                                   not summary_seeded_in_temporary_table),
                'top_cumulative_functions': [{'file': key[0], 'line': key[1], 'function': key[2],
                    'primitive_calls': value[0], 'total_calls': value[1],
                    'self_cpu_seconds': value[2], 'cumulative_cpu_seconds': value[3]}
                    for key, value in functions],
                'top_query_execution_times': [{'statement': statement, **times}
                    for statement, times in sorted(queries.items(), key=lambda item: -item[1]['wall_seconds'])[:12]],
                'publisher_requests': 0, 'source_states_mutated': False,
                'status_published': False, 'all_history_complete': False,
                'scope': 'canonical complete status building and exact JSON serialization on one WAL snapshot; excludes live metrics locks and filesystem publication'}
    finally:
        db.close()


def compare_cnyes_index_rows_cache(root, registry, repetitions=5):
    """Compare complete status projections without opening an acquisition owner.

    A long read transaction would deliberately disable the cache, so use a
    read-only autocommit connection and reject the comparison if another
    connection commits or any projected byte changes during the experiment.
    Live metrics locking, HTTP and filesystem publication are excluded.
    """
    from downloader.news_fulltext_cnyes_index import PublicIndexRowsCache

    if not 3 <= repetitions <= 15:
        raise ValueError('bounded_repetition_count_required')
    payload = (root / 'status.json').read_bytes()
    published = json.loads(payload)
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        if not owner or owner[0] != published.get('owner_id'):
            raise ValueError('status_database_owner_mismatch')

        class UncachedRows:
            def fetch(self, db, sql, parameters):
                return db.execute(sql, parameters).fetchall()

        def make_view(cache):
            view = NewsQueue.__new__(NewsQueue)
            view.root, view.db, view.registry, view.owner = root, db, registry, owner[0]
            if 'acceptance_receipt_io' in published:
                view.acceptance_receipt_io = deepcopy(published['acceptance_receipt_io'])
            view.metrics = SimpleNamespace(snapshot=lambda: deepcopy(published['performance']),
                                           measure=lambda *args, **kwargs: nullcontext())
            view.http_capacity_snapshot = lambda: copy_status_value(published['http_capacity'])
            view._original_url_started = monotonic - 120
            view._original_url_start_count = 0
            view._cnyes_public_index_rows_cache = cache
            return view

        def revision():
            return (db.total_changes, db.execute('PRAGMA main.data_version').fetchone()[0],
                    db.execute('PRAGMA main.schema_version').fetchone()[0],
                    db.execute('PRAGMA temp.schema_version').fetchone()[0])

        serialized = []

        def serialize_without_publication(path, result, *, durable=False, compact=False):
            if path != root / 'status.json' or durable:
                raise ValueError('unexpected_status_publication')
            data = (json.dumps(result, ensure_ascii=False, indent=None if compact else 2,
                separators=(',', ':') if compact else None) + '\n').encode()
            serialized.append((hashlib.sha256(data).hexdigest(), len(data)))

        observed, epoch, monotonic = utc_now(), time.time(), time.monotonic()
        views = {'uncached': make_view(UncachedRows()), 'cached': make_view(PublicIndexRowsCache())}
        measurements, identities = {name: [] for name in views}, {name: set() for name in views}
        sizes = {}
        initial_revision = revision()
        revisions_unchanged = True
        with (patch('downloader.news_fulltext_queue.atomic_write_json', serialize_without_publication),
              patch('downloader.news_fulltext_queue.utc_now', return_value=observed),
              patch('time.time', return_value=epoch), patch('time.monotonic', return_value=monotonic)):
            for number in range(repetitions):
                order = list(views) if number % 2 == 0 else list(reversed(views))
                for name in order:
                    revisions_unchanged &= revision() == initial_revision and not db.in_transaction
                    wall, cpu = time.perf_counter(), time.process_time()
                    views[name].status(activity='read_only_projection_replay')
                    measurements[name].append({'wall_seconds': time.perf_counter() - wall,
                                               'cpu_seconds': time.process_time() - cpu})
                    revisions_unchanged &= revision() == initial_revision and not db.in_transaction
                    digest, sizes[name] = serialized.pop()
                    identities[name].add(digest)
        medians = {name: {unit: statistics.median(sample[unit] for sample in samples)
                          for unit in ('wall_seconds', 'cpu_seconds')}
                   for name, samples in measurements.items()}
        digests = set().union(*identities.values())
        return {'contract_version': 1, 'accepted': revisions_unchanged and len(digests) == 1,
                'observed_at_utc': observed, 'owner_id': owner[0],
                'input_status_sha256': hashlib.sha256(payload).hexdigest(),
                'database_revision_unchanged': revisions_unchanged,
                'serialized_status_bytes': sizes,
                'deterministic_projection_sha256': {name: sorted(values) for name, values in identities.items()},
                'measurements': measurements, 'medians': medians, 'repetitions': repetitions,
                'speedups': {unit: medians['uncached'][unit] / medians['cached'][unit]
                             for unit in ('wall_seconds', 'cpu_seconds')},
                'first_cached_trial_includes_cold_cache': True,
                'publisher_requests': 0, 'source_states_mutated': False, 'status_published': False,
                'all_history_complete': False,
                'scope': 'complete status and exact JSON serialization; stable read-only autocommit view; excludes live locks, HTTP and filesystem publication'}
    finally:
        db.close()


def legacy_projection(db, registry):
    splits = {row[0]: json.loads(row[1]) for row in db.execute("SELECT scope_url,child_urls_json FROM bulk_window_splits")}
    rows = []
    for row in db.execute("SELECT * FROM bulk_api_coverage"):
        item = dict(row)
        source = registry.sources.get(item["provider"])
        item["superseded_by_windows"] = splits.get(item["base_url"])
        item["enabled"] = bool(source and source.allowed and not item["superseded_by_windows"]
                               and item["base_url"].split("?", 1)[0] in (*source.wordpress_apis, *source.cnyes_apis))
        rows.append(item)
    return rows


def benchmark(root, registry, repetitions=5):
    db = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN")
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        observed = utc_now()
        oracle_rows = legacy_projection(db, registry)
        actual_rows = summarize_bulk_coverage(db, registry)
        expected = {}
        for row in oracle_rows:
            key = (row["provider"], row["base_url"].split("?", 1)[0])
            item = expected.setdefault(key, Counter(coverage_scopes=0, leaf_scopes=0, superseded_scopes=0, total_articles=0, total_pages=0, enabled=False))
            item["coverage_scopes"] += 1
            superseded = bool(row["superseded_by_windows"])
            item["superseded_scopes"] += superseded
            item["leaf_scopes"] += not superseded
            if not superseded:
                item["total_articles"] += row["total_articles"]
                item["total_pages"] += row["total_pages"]
            item["enabled"] = bool(item["enabled"] or row["enabled"])
            item["observed_at_utc"] = max(item.get("observed_at_utc", ""), row["observed_at_utc"])
        actual = {(row["provider"], row["base_url"]): {key: value for key, value in row.items() if key not in {"provider", "base_url"}}
                  for row in actual_rows}
        measurements = {"legacy": [], "summary": []}
        sizes, identities = {}, {}
        for number in range(repetitions):
            for name in (("legacy", "summary") if number % 2 == 0 else ("summary", "legacy")):
                wall, cpu = time.perf_counter(), time.process_time()
                rows = legacy_projection(db, registry) if name == "legacy" else summarize_bulk_coverage(db, registry)
                serialized = json.dumps(rows, ensure_ascii=False, indent=2).encode()
                measurements[name].append({"wall_seconds": time.perf_counter() - wall, "cpu_seconds": time.process_time() - cpu})
                sizes[name] = len(serialized)
                identities[name] = hashlib.sha256(serialized).hexdigest()
        medians = {name: {unit: statistics.median(row[unit] for row in samples) for unit in ("wall_seconds", "cpu_seconds")}
                   for name, samples in measurements.items()}
        return {"contract_version": 2, "accepted": dict(expected) == actual,
                "observed_at_utc": observed, "owner_id": owner[0] if owner else None,
                "coverage_scopes": len(oracle_rows), "source_endpoints": len(actual_rows),
                "all_scope_totals_preserved": dict(expected) == actual,
                "serialized_projection_bytes": sizes, "projection_sha256": identities,
                "medians": medians, "measurements": measurements, "repetitions": repetitions,
                "scope": "one WAL snapshot; coverage projection and JSON serialization only, not overall CPU or throughput",
                "publish": False, "all_history_complete": False}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument('--profile-current-status', action='store_true')
    parser.add_argument('--compare-cnyes-index-rows-cache', action='store_true')
    parser.add_argument('--compare-discovery-summary', action='store_true')
    parser.add_argument('--compare-bulk-error-summary', action='store_true')
    parser.add_argument('--compare-discovery-transactional', action='store_true')
    parser.add_argument('--compare-scope-cache', action='store_true')
    parser.add_argument('--compare-capacity-copy', action='store_true')
    parser.add_argument('--compare-cycle-publication', action='store_true')
    parser.add_argument('--compare-service-transition', action='store_true')
    parser.add_argument('--compare-bulk-coverage-summary', action='store_true')
    parser.add_argument('--capacity-copy-reference', choices=('deepcopy', 'recursive'), default='deepcopy')
    parser.add_argument('--verify-body-states', action='store_true')
    args = parser.parse_args(argv)
    if not 3 <= args.repeat <= 15:
        parser.error("bounded repetition count required")
    if args.compare_cnyes_index_rows_cache and (args.profile_current_status or args.verify_body_states or
            args.compare_discovery_summary or args.compare_bulk_error_summary or args.compare_discovery_transactional or
            args.compare_scope_cache or args.compare_capacity_copy or args.compare_cycle_publication or
            args.compare_service_transition or args.compare_bulk_coverage_summary):
        parser.error('index rows cache comparison requires its own read-only autocommit view')
    if (args.compare_discovery_summary or args.compare_bulk_error_summary or args.compare_discovery_transactional or args.compare_scope_cache or args.compare_capacity_copy or args.compare_cycle_publication or args.compare_bulk_coverage_summary or args.compare_service_transition) and not args.profile_current_status:
        parser.error('projection comparison requires --profile-current-status')
    if args.verify_body_states and not args.profile_current_status:
        parser.error('body-state verification requires --profile-current-status')
    if args.capacity_copy_reference != 'deepcopy' and not args.compare_capacity_copy:
        parser.error('capacity copy reference requires --compare-capacity-copy')
    if sum((args.compare_discovery_summary,args.compare_bulk_error_summary,args.compare_discovery_transactional,
            args.compare_scope_cache, args.compare_capacity_copy, args.compare_cycle_publication,
            args.compare_bulk_coverage_summary, args.compare_service_transition)) > 1:
        parser.error('select one projection comparison')
    config, registry, repo = load_config(args.config)
    result = (compare_cnyes_index_rows_cache(repo / config['root'], registry, args.repeat)
              if args.compare_cnyes_index_rows_cache else
              profile_current_status(repo / config['root'], registry, args.repeat,
                                    compare_discovery_summary=args.compare_discovery_summary,
                                    compare_bulk_error_summary=args.compare_bulk_error_summary,
                                    compare_discovery_transactional=args.compare_discovery_transactional,
                                    compare_scope_cache=args.compare_scope_cache,
                                    compare_capacity_copy=args.compare_capacity_copy,
                                    compare_cycle_publication=args.compare_cycle_publication,
                                    compare_bulk_coverage_summary=args.compare_bulk_coverage_summary,
                                    compare_service_transition=args.compare_service_transition,
                                    capacity_copy_reference=args.capacity_copy_reference,
                                    verify_body_states=args.verify_body_states)
              if args.profile_current_status else benchmark(repo / config["root"], registry, args.repeat))
    result["implementation_sha256"] = {str(path): sha256_file(path) for path in (Path(__file__), Path("downloader/news_fulltext_queue.py"), Path('downloader/news_fulltext_status.py'), Path('downloader/news_fulltext_cnyes_history.py'), Path('downloader/news_fulltext_cnyes_index.py'))}
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ("accepted", "coverage_scopes", "source_endpoints", "serialized_projection_bytes", "serialized_status_bytes", "medians", "scope") if key in result}))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
