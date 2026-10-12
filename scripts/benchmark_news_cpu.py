"""Frozen, receipt-verified news replay: text parity, CPU and durable pipeline.

No HTTP, frontier mutations, public bodies or implied source rate-limit proof.
The explicit old BeautifulSoup oracle makes the performance claim rerunnable.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import cProfile
from datetime import datetime
import hashlib
import io
import json
from pathlib import Path
import pstats
import re
import resource
import sqlite3
import statistics
import tempfile
import time
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pyarrow.parquet as pq
import soupsieve

from downloader.artifact_io import atomic_write_json, atomic_write_text, sha256_file
from downloader import news_fulltext_bulk as bulk, news_fulltext_policy as policy, news_fulltext_publishers as publishers
from downloader.news_fulltext import accept_discovery, discovery_fingerprint, fetch_article, fetch_discovery, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metrics import NewsMetrics
from downloader.news_fulltext_queue import NewsQueue, source_path, utc_now, verify_bodies
from stockagent.data.news_source_catalog import configuration_paths


def reference_plain_text(fragment, exclude_selectors=(), *, parser="html.parser"):
    if "<" not in fragment and "&" not in fragment:
        return "\n".join(line.strip() for line in fragment.splitlines() if line.strip())
    soup = BeautifulSoup(fragment, parser)
    for node in soup.select("script,style,nav,header,footer,aside,form,figure,iframe,noscript,svg"):
        node.decompose()
    for selector in exclude_selectors:
        for node in soup.select(selector):
            node.decompose()
    return "\n".join(line.strip() for line in soup.get_text("\n", strip=True).splitlines() if line.strip())


def reference_select_nodes_v1(root, selector, *, limit=0):
    """Exact pre-v80 candidate narrowing, not the older all-CSS oracle."""
    simple = re.fullmatch(r'([#.])([A-Za-z_][A-Za-z0-9_-]*)', selector)
    attribute = re.fullmatch(r'([a-z][a-z0-9-]*)?\[([a-zA-Z_][a-zA-Z0-9_-]*)([^\[\]]*)\]', selector)
    tag = re.fullmatch(r'[a-z][a-z0-9-]*', selector)
    if simple:
        candidates = root.find_all(attrs={"id" if simple[1] == '#' else "class": simple[2]})
    elif attribute:
        candidates = root.find_all(attribute[1]) if attribute[1] else root.find_all(attrs={attribute[2].lower(): True})
    elif tag:
        candidates = root.find_all(selector)
    else:
        return root.select(selector, limit=limit)
    matcher = soupsieve.compile(selector)
    result = []
    for node in candidates:
        if matcher.match(node):
            result.append(node)
            if limit and len(result) >= limit:
                break
    return result


@contextmanager
def implementation(reference=False, *, id_prefix_only=False, selector_chain_only=False):
    if id_prefix_only and selector_chain_only:
        raise ValueError('choose_one_exact_optimization_reference')
    original = (policy._plain_text, bulk._plain_text, policy._plain_text_node,
                policy._select_nodes, policy._has_paywall, policy._native_exclusions,
                publishers._select_nodes)
    if reference and selector_chain_only:
        policy._select_nodes = reference_select_nodes_v1
        # These three native identity calls were direct CSS in v79. Keep that
        # exact caller baseline, not a predecessor already partially optimized.
        publishers._select_nodes = lambda root, selector: root.select(selector)
    elif reference and id_prefix_only:
        # Exactly the previous prefix fallback, not the much older all-CSS
        # oracle. Only this isolated audit process is patched; never a daemon.
        def legacy_exclusions(selectors):
            return None if any(s.startswith('[id^=') for s in selectors) else original[5](selectors)
        policy._native_exclusions = legacy_exclusions
    elif reference:
        policy._plain_text = bulk._plain_text = reference_plain_text
        policy._plain_text_node = lambda node, selectors=(), *, parser="html.parser": reference_plain_text(str(node), selectors, parser=parser)
        policy._select_nodes = lambda root, selector, *, limit=0: root.select(selector, limit=limit)
        policy._has_paywall = lambda root: root.select_one('.paywall, [data-paywall], form[action*=login]') is not None
        publishers._select_nodes = lambda root, selector: root.select(selector)
    try:
        yield
    finally:
        (policy._plain_text, bulk._plain_text, policy._plain_text_node,
         policy._select_nodes, policy._has_paywall, policy._native_exclusions,
         publishers._select_nodes) = original


def diagnostic_corpus(registry, report_paths):
    """One to sixteen explicitly retained private HTML responses, no frontier.

    Include source-negative and slower cases instead of choosing only healthy
    bodies. Reuse the canonical diagnostic integrity/parser proof. Continuous
    source scope is required: a benchmark must not open a finite cohort.
    """
    from scripts.verify_news_source_diagnostics import verify

    if not 1 <= len(report_paths) <= 4:
        raise ValueError('one_to_four_diagnostic_reports_required')
    inputs, selected = {}, {}
    for path in report_paths:
        if path.stat().st_size > 16 << 20:
            raise ValueError('diagnostic_report_oversized')
        inputs[str(path)] = sha256_file(path)
        report = json.loads(path.read_text())
        for probe in report['probes']:
            if probe.get('private_diagnostic_path'):
                selected[probe['url']] = probe
                if len(selected) > 16:
                    raise ValueError('bounded_retained_html_corpus_required')
    if not selected:
        raise ValueError('no_retained_html_diagnostics')
    files = []
    for probe in selected.values():
        path = Path(probe['private_diagnostic_path'])
        receipt_path = path.with_suffix('.receipt.json')
        source = registry.require(probe['url'])
        if (probe['status'] != 200 or probe['provider'] != source.provider
                or not source.allowed or source.acquisition_mode != 'continuous'
                or probe['url'].startswith('https://api.cnyes.com/')
                or path.stat().st_size > 10 << 20 or receipt_path.stat().st_size > 65536):
            raise ValueError('private_continuous_html_diagnostic_required')
        metadata = pq.read_metadata(path)
        if (metadata.num_rows != 1 or metadata.num_row_groups != 1
                or metadata.row_group(0).total_byte_size > 16 << 20):
            raise ValueError('bounded_single_response_diagnostic_required')
        files.append({'path': str(path), 'sha256': sha256_file(path),
                      'receipt_path': str(receipt_path), 'receipt_sha256': sha256_file(receipt_path)})
    proof = verify(list(selected.values()), registry)
    if not proof['accepted'] or not all(r.get('diagnostic_integrity_verified') for r in proof['results']):
        raise ValueError('retained_diagnostic_proof_not_accepted')
    samples = []
    for probe, file in zip(selected.values(), files, strict=True):
        raw = pq.read_table(file['path'], use_threads=False).to_pylist()[0]
        payload = raw['raw_response_body']
        if (not isinstance(payload, bytes) or len(payload) > 8 << 20
                or hashlib.sha256(payload).hexdigest() != probe['response_sha256']
                or sha256_file(Path(file['path'])) != file['sha256']):
            raise ValueError('retained_diagnostic_changed_during_load')
        samples.append({'provider': probe['provider'], 'url': probe['url'], 'body': payload,
                        'response_sha256': probe['response_sha256'], 'parquet_path': file['path']})
    return samples, files, dict(Counter(r['provider'] for r in samples)), inputs


def corpus(root, registry, limit):
    """Bounded per-provider indexed reads; never scan every historic receipt."""
    db = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    db.execute("BEGIN")
    counts, result, seen, files = Counter(), [], set(), []
    try:
        paths = set()
        for provider in ("cnyes", "fed_board", "globalvoices"):
            paths.update(r[0] for r in db.execute(
                "SELECT DISTINCT parquet_path FROM articles WHERE provider=? AND body_sha256 IS NOT NULL "
                "ORDER BY parquet_path DESC LIMIT ?", (provider, limit * 2)))
        for relative in sorted(paths, reverse=True):
            receipt_row = db.execute("SELECT receipt_path FROM accepted_batches WHERE path=?", (relative,)).fetchone()
            if not receipt_row:
                raise ValueError("missing_replay_receipt")
            receipt_path = source_path(root, receipt_row[0])
            receipt = json.loads(receipt_path.read_text())
            path = source_path(root, relative)
            if sha256_file(path) != receipt["sha256"]:
                raise ValueError("replay_file_digest_mismatch")
            table = pq.read_table(path)
            if table.num_rows != receipt["rows"]:
                raise ValueError("replay_file_rows_mismatch")
            used = False
            for record in table.to_pylist():
                raw = record.get("raw_response_body")
                provider = record["provider"]
                if raw is None or provider not in registry.sources or counts[provider] >= limit or raw.startswith(b"%PDF"):
                    continue
                digest = hashlib.sha256(raw).hexdigest()
                if digest != record["response_sha256"]:
                    raise ValueError("replay_response_digest_mismatch")
                key = (provider, record.get("source_request_url") or record["requested_url"], digest)
                if key in seen:
                    continue
                seen.add(key)
                counts[provider] += 1
                result.append({"provider": provider, "url": key[1], "body": raw,
                               "response_sha256": digest, "saved_body_sha256": record["body_sha256"],
                               "saved_article_id": record["article_id"], "parquet_path": relative})
                used = True
            if used:
                files.append({"path": relative, "sha256": receipt["sha256"], "receipt_path": receipt_row[0]})
        return result, files, dict(counts)
    finally:
        db.close()


def frozen_corpus(root, registry, report_path):
    """Replay exactly the earlier response identities, not a newer live sample."""
    report = json.loads(report_path.read_text())
    if not report.get('accepted') or report.get('publish') is not False:
        raise ValueError('frozen_corpus_report_not_accepted_private_replay')
    if report.get('source_corpus_kind') == 'private_html_diagnostics':
        raise ValueError('replay_diagnostic_corpus_with_its_exact_diagnostic_probes_not_accepted_batch_loader')
    identities = report['source_responses']
    if not 1 <= len(identities) <= 384 or len(report['source_files']) > 768:
        raise ValueError('unbounded_frozen_corpus')
    wanted = {(r['provider'], r['url'], r['response_sha256']): r for r in identities}
    if len(wanted) != len(identities):
        raise ValueError('duplicate_frozen_response_identity')
    found = {}
    for file in report['source_files']:
        path, receipt_path = source_path(root, file['path']), source_path(root, file['receipt_path'])
        receipt = json.loads(receipt_path.read_text())
        if receipt['sha256'] != file['sha256'] or sha256_file(path) != file['sha256']:
            raise ValueError('frozen_file_receipt_digest_mismatch')
        table = pq.read_table(path)
        if table.num_rows != receipt['rows']:
            raise ValueError('frozen_file_row_count_mismatch')
        for record in table.to_pylist():
            raw = record.get('raw_response_body')
            key = (record['provider'], record.get('source_request_url') or record['requested_url'], record['response_sha256'])
            if key not in wanted or raw is None:
                continue
            if (record['provider'] not in registry.sources or file['path'] != wanted[key]['parquet_path']
                    or hashlib.sha256(raw).hexdigest() != key[2]):
                raise ValueError('frozen_response_identity_or_digest_mismatch')
            found[key] = {'provider': key[0], 'url': key[1], 'body': raw, 'response_sha256': key[2],
                          'saved_body_sha256': record['body_sha256'], 'saved_article_id': record['article_id'],
                          'parquet_path': file['path']}
    if set(found) != set(wanted):
        raise ValueError('frozen_response_missing')
    samples = [found[(r['provider'], r['url'], r['response_sha256'])] for r in identities]
    return samples, report['source_files'], dict(Counter(r['provider'] for r in samples))


def extract(samples, registry):
    result = []
    http = SimpleNamespace(registry=registry)
    for row in samples:
        source = registry.sources[row["provider"]]
        try:
            if row["url"].startswith("https://api.cnyes.com/"):
                records, errors = bulk.cnyes_records(Response(row["url"], 200, {"content-type": "application/json"}, row["body"]), http, source)
                stable = [{k: r[k] for k in ("article_id", "title", "body_sha256", "body_bytes", "published_at_raw", "authors_json")}
                          for r in records]
            else:
                record = policy.extract_article(row["body"], source, page_url=row["url"])
                stable, errors = [{k: record.get(k) for k in (
                    "title", "body_sha256", "published_at_raw", "modified_at_raw", "authors_json",
                    "published_time_basis", "extraction_method", "article_identity_basis",
                    "body_completeness", "author_attribution_detail_json",
                    "verified_image_captions_json", "verified_article_notices_json")}], []
            result.append({"response_sha256": row["response_sha256"], "records": stable, "errors": errors})
        except policy.NewsError as error:
            result.append({"response_sha256": row["response_sha256"], "rejected": error.code})
    return result


def pipeline(samples, registry, config, parent):
    """Replay actual publisher envelopes through dispatch, commits and hashes."""
    with tempfile.TemporaryDirectory(prefix="news-cpu-replay-", dir=parent) as directory:
        root = Path(directory)
        queue = NewsQueue(root, registry)
        http = SimpleNamespace(registry=registry, metrics=queue.metrics)
        def response(url, **kwargs):
            row = by_url[url]
            mime = 'application/json' if url.startswith('https://api.cnyes.com/') else 'text/html'
            return Response(url, 200, {'content-type': mime}, row['body'])
        http.get = response
        by_url = {r["url"]: r for r in samples if r["url"].startswith("https://api.cnyes.com/")}
        replayed = len(by_url)
        try:
            for url in by_url:
                task = {"url": url, "provider": "cnyes", "kind": "cnyes_news", "etag": None,
                        "last_modified": None, "parser_fingerprint": discovery_fingerprint(registry.sources["cnyes"])}
                with queue.db:
                    queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
                accept_discovery(queue, task, *fetch_discovery(http, task))
            by_url = {r['url']: r for r in samples if not r['url'].startswith('https://api.cnyes.com/')}
            replayed += len(by_url)
            rejected = Counter()
            pending, pending_text, pending_raw, batch_sizes = [], 0, 0, []
            for url in by_url:
                with queue.db:
                    queue.add_articles([{'url': url}])
                row = queue.claim_url(url, config['lease_seconds'])
                if row is None:
                    raise ValueError('offline_replay_body_not_dispatchable')
                try:
                    record = fetch_article(http, row)
                    pending.append(record)
                    pending_text += record['body_bytes']
                    pending_raw += len(record['raw_response_body'] or b'')
                    pending_raw += len(record.get('url_resolution_response_body') or b'')
                    # Match the formal owner's size limits and end-of-work
                    # drain. Ten one-row fsyncs are not its default workflow.
                    if (len(pending) >= config['batch_articles']
                            or pending_text >= config['batch_text_bytes']
                            or pending_raw >= config.get('batch_raw_response_bytes', 16 << 20)):
                        queue.accept(pending)
                        batch_sizes.append(len(pending))
                        pending, pending_text, pending_raw = [], 0, 0
                except policy.NewsError as error:
                    queue.failed(row, error, config['max_attempts'])
                    rejected[error.code] += 1
            if pending:
                queue.accept(pending)
                batch_sizes.append(len(pending))
            proof = verify_bodies(root)
            proof['source_rejections'] = dict(rejected)
            proof['html_batch_sizes'] = batch_sizes
            proof["replayed_responses"] = replayed
            proof["new_heads"] = sum(p.get('counts', {}).get('durably_accepted_new_url_heads', 0)
                                     for p in queue.metrics.snapshot()['providers'].values())
            return proof
        finally:
            queue.close()


def timed(operation):
    wall, cpu = time.perf_counter(), time.process_time()
    result = operation()
    return result, {"wall_seconds": time.perf_counter() - wall, "cpu_seconds": time.process_time() - cpu}


def dispatch_replay(root, registry, config, repetitions=7):
    """Same pinned frontier/query/order, indexed vs old full scan; no dispatch.

    This measures only queue selection, not transport or the whole owner CPU.
    The production writer is never opened or changed by this audit.
    """
    db = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    now = time.time()
    results, measurements, plans = {}, {'reference': [], 'optimized': []}, {}
    try:
        db.execute('BEGIN')
        discovery_jobs = db.execute('SELECT count(*) FROM discovery').fetchone()[0]
        sources = [source for source in registry.sources.values() if source.allowed]
        statements = []
        for source in sources:
            clause, parameters = NewsQueue.discovery_dispatch_filter(source)
            # Freeze circuit time alongside the WAL generation for exact parity.
            if source.cnyes_apis:
                parameters[-1] = now
            sql = "SELECT * FROM discovery {index} WHERE provider=? AND due<=? AND superseded_by_windows_json IS NULL AND " + clause + " "
            sql += "ORDER BY kind='feed' DESC,kind IN ('wordpress_posts','cnyes_news','rss_fulltext') DESC,priority,due,url LIMIT ?"
            # SQLite constant-folds finite sources' WHERE 0. Forcing a partial
            # index on a query with no eligible rows can yield "no query
            # solution". Keep their empty output in both measured populations.
            statements.append((source.provider, sql, (source.provider, now, *parameters, config['workers'] + 1), clause != "0"))
        for repetition in range(repetitions):
            for name in (('reference', 'optimized') if repetition % 2 == 0 else ('optimized', 'reference')):
                hint = 'NOT INDEXED' if name == 'reference' else 'INDEXED BY discovery_ready_order'
                def operation():
                    return {provider: [row[0] for row in db.execute(sql.format(index=hint if eligible else ''), args)]
                            for provider, sql, args, eligible in statements}
                output, timing = timed(operation)
                results[name] = output
                measurements[name].append(timing)
                if repetition == 0:
                    plans[name] = {provider: [row[3] for row in db.execute('EXPLAIN QUERY PLAN ' + sql.format(index=hint if eligible else ''), args)]
                                   for provider, sql, args, eligible in statements}
        ratios = {}
        for unit in ('cpu_seconds', 'wall_seconds'):
            before = statistics.median(m[unit] for m in measurements['reference'])
            after = statistics.median(m[unit] for m in measurements['optimized'])
            ratios[unit] = {'reference_median': before, 'optimized_median': after, 'speedup': before / after}
        return {'accepted': bool(discovery_jobs) and results['reference'] == results['optimized'],
                'discovery_jobs': discovery_jobs, 'providers_queried': len(statements), 'repetitions': repetitions,
                'selected_job_counts': {p: len(rows) for p, rows in results['optimized'].items()},
                'selected_job_urls_sha256': hashlib.sha256(json.dumps(results['optimized'], sort_keys=True).encode()).hexdigest(),
                'query_plans': plans, 'ratios': ratios, 'measurements': measurements,
                'scope': 'read-only pinned production frontier selection; not total owner CPU or HTTP throughput'}
    finally:
        db.close()


def status_projection_replay(root, registry, output_parent, repetitions=7):
    """The entire canonical status write, with one pinned WAL and clock.

    Only the previous sparse-outcome JOIN is restored. Never initialize a
    NewsQueue/writer, open its lock, fetch a publisher or write production status.
    Include every provider/finite scope and the actual temporary JSON write.
    """
    from downloader import news_fulltext_cohorts as cohorts, news_fulltext_queue as queue_module
    snapshot = json.loads((root / 'status.json').read_text())
    db = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    original = (time.time, time.monotonic, queue_module.utc_now, cohorts._NONBODY_OUTCOME_PROGRESS_SQL)
    legacy_sql = original[3].replace('o CROSS JOIN articles a WHERE a.id=o.article_id AND',
                                     'o JOIN articles a ON a.id=o.article_id WHERE')
    if legacy_sql == original[3]:
        db.close()
        raise ValueError('previous_outcome_join_not_reconstructed')
    stamp, wall_clock, monotonic_clock = utc_now(), time.time(), time.monotonic()
    measurements, mismatches = {'reference': [], 'optimized': []}, []
    expected, result = None, None
    try:
        db.execute('BEGIN')
        owner = dict(db.execute('SELECT key,value FROM queue_settings'))['current_owner_id']
        q = NewsQueue.__new__(NewsQueue)  # No constructor, migration, owner or lock.
        q.db, q.owner, q.registry = db, owner, registry
        q.metrics = SimpleNamespace(snapshot=lambda: snapshot['performance'])
        q.http_capacity_snapshot = lambda: snapshot.get('http_capacity')
        source_progress = snapshot['original_url_resolution']
        q._original_url_started = monotonic_clock - source_progress['owner_elapsed_seconds']
        q._original_url_start_count = source_progress['examined_urls'] - source_progress['owner_examined_urls']
        cooldowns = {p: datetime.fromisoformat(t).timestamp() for p, t in snapshot.get('provider_next_attempt_utc', {}).items()}
        time.time, time.monotonic = lambda: wall_clock, lambda: monotonic_clock
        queue_module.utc_now = lambda: stamp
        with tempfile.TemporaryDirectory(prefix='status-replay-', dir=output_parent) as directory:
            q.root = Path(directory)
            for repetition in range(repetitions):
                order = ('reference', 'optimized') if repetition % 2 == 0 else ('optimized', 'reference')
                for name in order:
                    cohorts._NONBODY_OUTCOME_PROGRESS_SQL = legacy_sql if name == 'reference' else original[3]
                    result, measurement = timed(lambda: q.status(activity='private_read_only_replay',
                                                                  provider_cooldowns=cooldowns))
                    measurements[name].append(measurement)
                    # Fixed clocks make ALL projected data comparable, not just
                    # summary totals; author/scope/outcome details are not dropped.
                    payload = json.dumps(result, sort_keys=True, ensure_ascii=False).encode()
                    digest = hashlib.sha256(payload).hexdigest()
                    expected = digest if expected is None else expected
                    if digest != expected:
                        mismatches.append({'repetition': repetition, 'implementation': name, 'sha256': digest})
                    print(json.dumps({'repetition': repetition, 'implementation': name,
                                      'status_projection': measurement, 'parity': digest == expected}), flush=True)
        plans = {}
        provider = next((p for p, item in result['providers'].items() if item.get('finite_cohort')), 'publisher')
        for name, sql in (('reference', legacy_sql), ('optimized', original[3])):
            plans[name] = [r[3] for r in db.execute('EXPLAIN QUERY PLAN ' + sql,
                            (provider, 'source_capture_fingerprint:' + provider))]
        ratios = {}
        for unit in ('wall_seconds', 'cpu_seconds'):
            before = statistics.median(r[unit] for r in measurements['reference'])
            after = statistics.median(r[unit] for r in measurements['optimized'])
            ratios[unit] = {'reference_median': before, 'optimized_median': after, 'speedup': before / after}
        return {'accepted': not mismatches and db.total_changes == 0, 'observed_at_utc': stamp,
                'comparison_basis': 'previous_nonbody_progress_join_only; full canonical status projection/write',
                'repetitions': repetitions, 'measurements': measurements, 'ratios': ratios,
                'mismatches': mismatches, 'projection_sha256': expected, 'query_plans': plans,
                'known_urls': result['unique_articles'], 'stored_heads': result['articles_with_stored_body'],
                'source_keys': len(result['providers']), 'owner_id': owner, 'queue_writes': db.total_changes,
                'reviewed_nonbody_by_provider': {p: item['finite_cohort']['reviewed_nonbody_outside_cohort']
                    for p, item in result['providers'].items() if item.get('finite_cohort')},
                'scope': 'one pinned read-only WAL and fixed source clocks; actual private temporary status write; not network throughput or full history',
                'production_status_written': False, 'publisher_http_requests': 0,
                'publish': False, 'all_history_complete': False}
    finally:
        time.time, time.monotonic, queue_module.utc_now, cohorts._NONBODY_OUTCOME_PROGRESS_SQL = original
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--responses-per-provider", type=int, default=32)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument('--replay-from', type=Path, help='Accepted private report whose exact response identities must be retained')
    parser.add_argument('--diagnostic-probes', type=Path, action='append', default=[],
                        help='One to four existing probe reports; at most sixteen retained continuous-source HTML responses')
    parser.add_argument('--id-prefix-only', action='store_true',
                        help='Compare only the previous ID-prefix reparse fallback, not all legacy CPU paths')
    parser.add_argument('--selector-chain-only', action='store_true',
                        help='Compare pre-v80 selector candidate narrowing, not the older all-CPU oracle')
    parser.add_argument('--status-only', action='store_true',
                        help='Pinned read-only full canonical status/write parity and performance; no bodies or publisher requests')
    args = parser.parse_args(argv)
    if args.id_prefix_only and args.selector_chain_only:
        parser.error('choose one exact optimization reference')
    if not 1 <= args.responses_per_provider <= 128 or not 1 <= args.repeat <= 7:
        parser.error("bounded response count/repeat required")
    config, registry, repo = load_config(args.config)
    configuration_sha256 = {str(p.relative_to(repo)): sha256_file(p) for p in configuration_paths(args.config)}
    implementation_sha256 = {str(p.relative_to(repo)): sha256_file(p) for p in (
        repo / 'downloader/news_fulltext_policy.py', repo / 'downloader/news_fulltext_bulk.py',
        repo / 'downloader/news_fulltext_publishers.py', repo / 'downloader/news_fulltext_queue.py',
        repo / 'downloader/news_fulltext.py', repo / 'scripts/benchmark_news_cpu.py',
        repo / 'downloader/news_fulltext_cohorts.py', repo / 'downloader/news_fulltext_urls.py')}
    if args.status_only:
        if args.id_prefix_only or args.selector_chain_only or args.diagnostic_probes or args.replay_from:
            parser.error('status-only cannot mix body replay inputs or ID-prefix comparison')
        args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        result = status_projection_replay(repo / config['root'], registry, args.output.parent, args.repeat)
        if any(sha256_file(repo / path) != digest for path, digest in
               {**configuration_sha256, **implementation_sha256}.items()):
            raise ValueError('benchmark_implementation_or_configuration_changed')
        result.update(contract_version=1, configuration_sha256=configuration_sha256,
                      implementation_sha256=implementation_sha256)
        atomic_write_json(args.output, result)
        print(json.dumps({k: result[k] for k in ('accepted', 'known_urls', 'stored_heads', 'ratios', 'mismatches')}))
        return 0 if result['accepted'] else 1
    if args.diagnostic_probes and args.replay_from:
        parser.error('choose retained probe reports OR an accepted frozen corpus')
    diagnostic_inputs = {}
    if args.diagnostic_probes:
        samples, files, counts, diagnostic_inputs = diagnostic_corpus(registry, args.diagnostic_probes)
    else:
        samples, files, counts = (frozen_corpus(repo / config['root'], registry, args.replay_from) if args.replay_from else
                                 corpus(repo / config["root"], registry, args.responses_per_provider))
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not samples:
        raise ValueError("no_replay_samples")
    results = {"reference": {"extraction": [], "pipeline": []}, "optimized": {"extraction": [], "pipeline": []}}
    expected, mismatches, proofs, profile_outputs = None, [], {}, {}
    for repetition in range(args.repeat):
        for name in (("reference", "optimized") if repetition % 2 == 0 else ("optimized", "reference")):
            profiler = cProfile.Profile() if repetition == 0 else None
            with implementation(name == "reference", id_prefix_only=args.id_prefix_only,
                                selector_chain_only=args.selector_chain_only):
                if profiler:
                    profiler.enable()
                output, measurement = timed(lambda: extract(samples, registry))
                if profiler:
                    profiler.disable()
                    stream = io.StringIO()
                    pstats.Stats(profiler, stream=stream).sort_stats("cumulative").print_stats(35)
                    profile_outputs[name] = stream.getvalue()
                results[name]["extraction"].append(measurement)
                expected = expected or output
                for row, old, new in zip(samples, expected, output):
                    if old != new:
                        mismatches.append({"provider": row["provider"], "url": row["url"], "response_sha256": row["response_sha256"]})
                proof, measurement = timed(lambda: pipeline(samples, registry, config, args.output.parent))
                results[name]["pipeline"].append(measurement)
                proofs[name] = {k: proof.get(k) for k in ("accepted", "errors", "body_versions_verified", "new_heads", "replayed_responses", "source_rejections", "html_batch_sizes")}
            print(json.dumps({"repetition": repetition, "implementation": name, "extraction": results[name]["extraction"][-1],
                              "pipeline": measurement, "proof": proofs[name]}, ensure_ascii=False), flush=True)
    ratios = {}
    for phase in ("extraction", "pipeline"):
        # Discard profiled extraction timing; paired unprofiled samples remain.
        start = 1 if phase == "extraction" and args.repeat > 1 else 0
        for unit in ("wall_seconds", "cpu_seconds"):
            before = statistics.median(r[unit] for r in results["reference"][phase][start:])
            after = statistics.median(r[unit] for r in results["optimized"][phase][start:])
            ratios[phase + "_" + unit] = {"reference_median": before, "optimized_median": after, "speedup": before / after}
    result = {"accepted": not mismatches and all(p["accepted"] for p in proofs.values()) and proofs["reference"]["new_heads"] == proofs["optimized"]["new_heads"],
              "observed_at_utc": utc_now(), 'contract_version': 3,
              'html_pipeline_basis': 'canonical worker, size-bounded owner batch, end drain and immutable receipt/head verification; local replay, not socket concurrency or source rate',
              'comparison_basis': ('previous_selector_candidate_v1_only' if args.selector_chain_only else
                                   'previous_id_prefix_fallback_only' if args.id_prefix_only else 'legacy_all_cpu_paths'),
              'source_corpus_kind': 'private_html_diagnostics' if args.diagnostic_probes else 'durably_accepted_responses',
              'diagnostic_inputs_sha256': diagnostic_inputs, 'publisher_http_requests': 0,
              "source_corpus_report_sha256": sha256_file(args.replay_from) if args.replay_from else None,
              "configuration_sha256": configuration_sha256,
              "implementation_sha256": implementation_sha256,
              "counts": counts, "source_files": files, "source_responses": [{k: r[k] for k in ("provider", "url", "response_sha256", "parquet_path")} for r in samples],
              "ratios": ratios, "measurements": results, "proofs": proofs, "mismatches": mismatches,
              "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
              "scope": "frozen private raw-response replay, including canonical durable acceptance; not production HTTP throughput or all history",
              "publish": False, "all_history_complete": False}
    if diagnostic_inputs:
        # This mode measures the exact parser/capture/receipt chain. No query
        # of a newer live frontier is included in its frozen CPU comparison.
        if any(sha256_file(Path(path)) != digest for path, digest in diagnostic_inputs.items()):
            raise ValueError('diagnostic_report_changed_during_replay')
        if any(sha256_file(Path(file['path'])) != file['sha256']
               or sha256_file(Path(file['receipt_path'])) != file['receipt_sha256'] for file in files):
            raise ValueError('diagnostic_source_changed_during_replay')
        result['dispatch'] = {'accepted': True, 'measured': False,
                              'basis': 'bounded exact-URL dispatch included in temporary durable pipeline; no live-frontier benchmark'}
    else:
        result['dispatch'] = dispatch_replay(repo / config['root'], registry, config)
    result['accepted'] = result['accepted'] and result['dispatch']['accepted']
    result['accepted'] = result['accepted'] and proofs['reference']['source_rejections'] == proofs['optimized']['source_rejections']
    result['accepted'] = result['accepted'] and proofs['reference']['html_batch_sizes'] == proofs['optimized']['html_batch_sizes']
    if any(sha256_file(repo / path) != digest for path, digest in
           {**configuration_sha256, **implementation_sha256}.items()):
        raise ValueError('benchmark_implementation_or_configuration_changed')
    atomic_write_json(args.output, result)
    for name, profile in profile_outputs.items():
        atomic_write_text(args.output.with_suffix('.' + name + '.profile.txt'), profile, durable=True)
    print(json.dumps({k: result[k] for k in ("accepted", "counts", "ratios", "proofs", "mismatches")}, ensure_ascii=False))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
