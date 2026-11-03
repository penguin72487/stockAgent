"""Measure URL inbox continuation using a private queue and fake publisher.

Run through the repository runtime in an isolated network namespace. The
control changes only the in-memory continuation hint; every sample uses the
same owner, queue, input shape and receipt validation. Scratch queues remain
under /var/tmp for the independent readback.
"""
import argparse
from copy import deepcopy
from contextlib import nullcontext
from pathlib import Path
from tempfile import mkdtemp
import hashlib
import json
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / 'test'))
from test_news_fulltext import config as make_config, http_fixture, FakeResponse, article_html
from downloader import news_fulltext as main
from downloader import news_fulltext_url_metadata as urls
from downloader.news_fulltext_queue import NewsQueue, verify_bodies


def run(enabled, index, bench_root, *, profile_first=False):
    config = make_config.__wrapped__()
    staged = deepcopy(config['providers'][0])
    staged.update(id='staged', domains=['staged.test'], enabled=False)
    config['providers'].append(staged)
    config.update(workers=1, batch_articles=1)
    priority_url = 'https://publisher.test/story'
    pending_urls = [f'https://staged.test/story/{n}' for n in range(772)]

    class DelayedResponse(FakeResponse):
        def iter_content(self, **kwargs):
            state['response_started_at'] = time.monotonic() - started
            time.sleep(1.2)
            state['response_finished_at'] = time.monotonic() - started
            yield self.body

    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              priority_url: DelayedResponse(article_html())}
    private_root = bench_root / f'run-{index}'
    private_root.mkdir(mode=0o700)
    with nullcontext(str(private_root)) as temp:
        root = Path(temp)
        http, session = http_fixture(config, root, routes)
        queue = NewsQueue(root, http.registry)
        queue.add_articles([{'url': priority_url}])
        urls.submit_urls(queue.root, queue.registry, pending_urls[:512])
        urls.submit_urls(queue.root, queue.registry, pending_urls[512:])
        real_consume, real_accept, real_disk = urls.consume_urls, queue.accept, main.refresh_disk_admission
        real_fetch, real_wait, real_status = main.fetch_article, main.wait, queue.status
        state = {'admitted_at': None, 'body_at': None, 'response_started_at': None,
                 'response_finished_at': None, 'worker_started_at': None, 'worker_finished_at': None,
                 'accept_started_at': None, 'slices': 0, 'url_slices': [], 'waits': [], 'statuses': []}
        if profile_first and index == 0:
            import cProfile
            accept_profile = cProfile.Profile()
        else:
            accept_profile = None

        def fetch(*args, **kwargs):
            state['worker_started_at'] = time.monotonic() - started
            try:
                return real_fetch(*args, **kwargs)
            finally:
                state['worker_finished_at'] = time.monotonic() - started

        def wait(*args, **kwargs):
            begin = time.monotonic() - started
            result = real_wait(*args, **kwargs)
            state['waits'].append({'start': begin, 'end': time.monotonic() - started,
                                   'completed': len(result[0])})
            return result

        def status(*args, **kwargs):
            begin = time.monotonic() - started
            try:
                return real_status(*args, **kwargs)
            finally:
                state['statuses'].append({'start': begin, 'end': time.monotonic() - started,
                                           'activity': kwargs.get('activity')})

        def consume(*args, **kwargs):
            begin = time.monotonic() - started
            result = real_consume(*args, **kwargs)
            state['slices'] += 1
            if not enabled:
                queue._url_metadata_continuation_pending = False
            count = queue.db.execute("SELECT count(*) FROM articles WHERE provider='staged'").fetchone()[0]
            if count == len(pending_urls) and state['admitted_at'] is None:
                state['admitted_at'] = time.monotonic() - started
                if state['body_at'] is not None:
                    http.stop.set()
            state['url_slices'].append({'start': begin, 'end': time.monotonic() - started,
                                         'processed': result['processed']})
            return result

        def accept(records):
            state['accept_started_at'] = time.monotonic() - started
            if accept_profile is not None:
                accept_profile.enable()
            try:
                result = real_accept(records)
            finally:
                if accept_profile is not None:
                    accept_profile.disable()
            state['body_at'] = time.monotonic() - started
            if state['admitted_at'] is not None:
                http.stop.set()
            return result

        urls.consume_urls, queue.accept, main.refresh_disk_admission = consume, accept, lambda *_: False
        main.fetch_article, main.wait, queue.status = fetch, wait, status
        started = time.monotonic()
        cpu_started = time.process_time()
        try:
            main.run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=5)
            cpu = time.process_time() - cpu_started
            wall = time.monotonic() - started
            heads = queue.db.execute("SELECT url,state,attempts FROM articles WHERE provider='staged' ORDER BY url").fetchall()
            items = queue.db.execute('SELECT outcome,count(*) FROM url_metadata_items GROUP BY outcome').fetchall()
            verification = verify_bodies(root)
            assert len(heads) == 772 and all(row['state'] == 'awaiting_authorization' and row['attempts'] == 0 for row in heads)
            assert len(items) == 1 and tuple(items[0]) == ('metadata_registered', 772)
            assert verification['accepted'] and state['admitted_at'] is not None and state['body_at'] is not None
            assert {url for url, _ in session.calls} == set(routes)
            assert (0 <= state['worker_started_at'] <= state['response_started_at'] <= state['response_finished_at']
                    <= state['worker_finished_at'] <= state['accept_started_at'] <= state['body_at'] <= wall)
            metrics = queue.metrics.snapshot()
            stages = {key: metrics['stages'][key] for key in ('batch_build', 'parquet_compress_fsync',
                'receipt_hash_fsync', 'acceptance_sqlite_commit', 'status_write') if key in metrics['stages']}
            extraction = metrics['providers'].get('publisher', {}).get('stages', {}).get('article_extraction')
            def profile_summary(profile):
                if profile is None:
                    return None
                import pstats
                stats = pstats.Stats(profile).stats
                ranked = sorted(stats.items(), key=lambda item: item[1][3], reverse=True)[:20]
                return [{'file': name, 'line': line, 'function': function, 'calls': row[1],
                         'self_seconds': row[2], 'cumulative_seconds': row[3]}
                        for (name, line, function), row in ranked]
            return {'continuation_enabled': enabled, 'wall_seconds': round(wall, 6), 'cpu_seconds': round(cpu, 6),
                    'all_772_admitted_seconds': round(state['admitted_at'], 6),
                    'priority_body_accepted_seconds': round(state['body_at'], 6), 'slices': state['slices'],
                    'priority_response_started_seconds': round(state['response_started_at'], 6),
                    'priority_response_finished_seconds': round(state['response_finished_at'], 6),
                    'priority_body_handoff_seconds': round(state['body_at'] - state['response_finished_at'], 6),
                    'priority_worker_started_seconds': round(state['worker_started_at'], 6),
                    'priority_worker_finished_seconds': round(state['worker_finished_at'], 6),
                    'priority_parse_and_worker_tail_seconds': round(state['worker_finished_at'] - state['response_finished_at'], 6),
                    'priority_owner_wait_seconds': round(state['accept_started_at'] - state['worker_finished_at'], 6),
                    'priority_accept_seconds': round(state['body_at'] - state['accept_started_at'], 6),
                    'priority_accept_started_seconds': round(state['accept_started_at'], 6),
                    'stage_metrics': stages, 'publisher_article_extraction': extraction,
                    'cold_accept_profile': profile_summary(accept_profile),
                    'owner_timeline': {'url_slices': state['url_slices'], 'waits': state['waits'],
                                       'statuses': state['statuses']},
                    'heads': len(heads), 'url_item_rows': sum(row[1] for row in items),
                    'body_readback_accepted': verification['accepted'], 'synthetic_http_requests': len(session.calls),
                    'private_root': str(private_root)}
        finally:
            urls.consume_urls, queue.accept, main.refresh_disk_admission = real_consume, real_accept, real_disk
            main.fetch_article, main.wait, queue.status = real_fetch, real_wait, real_status
            http.close()
            queue.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--order', choices=('abba', 'baab'), default='abba',
                        help='A disables and B enables the continuation hint')
    parser.add_argument('--profile-first', action='store_true',
                        help='profile first durable accept; timings include profiler overhead')
    args = parser.parse_args()
    destination = args.output.resolve()
    if any(destination.is_relative_to(REPO / name) for name in ('data_news', 'data_news_fulltext')):
        raise ValueError('benchmark_output_must_not_be_in_a_production_news_root')
    bench_root = Path(mkdtemp(prefix='news-url-owner-abba-', dir='/var/tmp'))
    def implementation_snapshot():
        # Capture the shared news family, the durable Arrow writer and the
        # fixture's configuration, including files used by lazy imports. This
        # is an explicit dependency set, not a claim about every loaded byte.
        paths = sorted((REPO / 'downloader').glob('news_fulltext*.py'))
        paths.extend(REPO / name for name in ('downloader/artifact_io.py',
            'configs/news_fulltext.json', 'test/test_news_fulltext.py',
            'scripts/benchmark_news_url_owner_continuation.py',
            'scripts/readback_news_url_owner_continuation.py'))
        return {str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in paths}

    implementation_before = implementation_snapshot()
    runs = [run(enabled, index, bench_root, profile_first=args.profile_first) for index, enabled in enumerate(
        (False, True, True, False) if args.order == 'abba' else (True, False, False, True))]
    if implementation_snapshot() != implementation_before:
        raise RuntimeError('benchmark_implementation_changed_during_measurement')
    from importlib.metadata import version
    result = {'contract': 'news_url_continuation_private_abba_v1',
              'scope': 'synthetic 772 URL targets, two request files, 1.2-second in-process fake response',
              'control': 'current owner with only the continuation hint disabled',
              'order': args.order,
              'profile_first': args.profile_first,
              'network_requests': 0, 'production_queue_writes': 0,
              'implementation_sha256': implementation_before,
              'implementation_fingerprint_scope': 'news_fulltext family, artifact_io, fixture/config and benchmark/readback; checked before and after',
              'runtime': {'python': sys.version, 'packages': {name: version(name) for name in
                          ('pyarrow', 'beautifulsoup4', 'soupsieve', 'lxml', 'requests')}},
              'runs': runs}
    with destination.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result, ensure_ascii=False))
