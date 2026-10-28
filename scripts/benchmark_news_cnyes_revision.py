"""Offline, paired local dispatch and wake benchmark; never calls a publisher."""
import hashlib
import json
import statistics
import tempfile
import time
from pathlib import Path

from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import cnyes_scope
from downloader.news_fulltext_cnyes_index import request_url
from downloader.news_fulltext_queue import NewsQueue


config, registry, _ = load_config(Path('configs/news_fulltext.json'))
source = registry.sources['cnyes']


def measure(queue, iterations=7):
    samples = []
    queue.discovery_candidates(source, 17)
    for _ in range(iterations):
        wall, cpu = time.perf_counter(), time.process_time()
        rows = queue.discovery_candidates(source, 17)
        samples.append({'wall_seconds': time.perf_counter()-wall,
                        'cpu_seconds': time.process_time()-cpu, 'selected': len(rows)})
    return {'samples': samples, 'median_wall_seconds': statistics.median(x['wall_seconds'] for x in samples),
            'median_cpu_seconds': statistics.median(x['cpu_seconds'] for x in samples)}


def measure_wake(queue, iterations=7):
    samples = []
    for _ in range(iterations):
        wall, cpu = time.perf_counter(), time.process_time()
        delay = queue.next_wake_delay({}, maximum=60)
        samples.append({'wall_seconds': time.perf_counter()-wall,
                        'cpu_seconds': time.process_time()-cpu, 'delay': delay})
    return {'samples': samples, 'median_wall_seconds': statistics.median(x['wall_seconds'] for x in samples),
            'median_cpu_seconds': statistics.median(x['cpu_seconds'] for x in samples)}


def measure_writes(queue, urls, iterations=7):
    samples = []
    for _ in range(iterations):
        wall, cpu = time.perf_counter(), time.process_time()
        with queue.db:
            queue.db.executemany('UPDATE discovery SET due=due+1 WHERE url=?', ((url,) for url in urls))
        samples.append({'wall_seconds': time.perf_counter()-wall, 'cpu_seconds': time.process_time()-cpu})
    return {'rows_per_transaction': len(urls), 'samples': samples,
            'median_wall_seconds': statistics.median(x['wall_seconds'] for x in samples),
            'median_cpu_seconds': statistics.median(x['cpu_seconds'] for x in samples)}


class DisableCache:
    disabled = True

    def hit(self, *_):
        return False

    def wake_deadline(self, *_):
        return None


with tempfile.TemporaryDirectory(prefix='cnyes-revision-bench-', dir='/var/tmp') as directory:
    queue = NewsQueue(Path(directory), registry)
    try:
        days = range(16323, 16323 + 232)
        now = time.time()
        rows = [(request_url(source, day, page), 'index', 'cnyes', now + 600 if page == 1 else 0)
                for day in days for page in range(1, 49)]
        with queue.db:
            queue.db.executemany('INSERT INTO discovery(url,kind,provider,due) VALUES (?,?,?,?)', rows)
            other_urls = ['https://other.test/feed/'+str(i) for i in range(128)]
            queue.db.executemany("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'feed','other',0)",
                                 [(url,) for url in other_urls])
        normal = measure(queue)
        with queue.db:
            queue.db.executemany('INSERT INTO cnyes_index_revision_rounds VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                [(cnyes_scope(request_url(source, day)), source.cnyes_index_data_fingerprint,
                  source.authorization_fingerprint, f'{day:064x}', f'{day:064x}', 'anchor',
                  None, None, None, None, '2026-10-28T00:00:00+00:00') for day in days])
        queue._cnyes_revision_empty_selection = DisableCache()
        blocked_baseline = measure(queue)
        wake_baseline = measure_wake(queue)
        cnyes_urls = [url for url, _, _, due in rows if due == 0][:128]
        writes_other_baseline = measure_writes(queue, other_urls)
        writes_cnyes_baseline = measure_writes(queue, cnyes_urls)
        del queue._cnyes_revision_empty_selection
        wall, cpu = time.perf_counter(), time.process_time()
        cold = queue.discovery_candidates(source, 17)
        cold = {'wall_seconds': time.perf_counter()-wall, 'cpu_seconds': time.process_time()-cpu,
                'selected': len(cold)}
        blocked = measure(queue)
        wake_cached = measure_wake(queue)
        writes_other_cached = measure_writes(queue, other_urls)
        assert queue._cnyes_revision_empty_selection.hit(source, 17, True, ())
        blocked_after_other_provider = measure(queue)
        writes_cnyes_cached = measure_writes(queue, cnyes_urls)
        assert not queue._cnyes_revision_empty_selection.hit(source, 17, True, ())
        wall, cpu = time.perf_counter(), time.process_time()
        refreshed = queue.discovery_candidates(source, 17)
        post_mutation_proof = {'wall_seconds': time.perf_counter()-wall, 'cpu_seconds': time.process_time()-cpu,
                               'selected': len(refreshed)}
        first = request_url(source, 16323, 1)
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
        ready = [row['url'] for row in queue.discovery_candidates(source, 17)]
        assert first in ready
        assert all(x['selected'] == 17 for x in normal['samples'])
        assert all(x['selected'] == 0 for x in blocked_baseline['samples'])
        assert all(x['selected'] == 0 for x in blocked['samples'])
        assert all(x['selected'] == 0 for x in blocked_after_other_provider['samples'])
        assert all(x['delay'] > 1 for x in wake_baseline['samples'] + wake_cached['samples'])
        files = ('downloader/news_fulltext_queue.py', 'downloader/news_fulltext_cnyes_dispatch_cache.py')
        result = {'contract_version': 2, 'synthetic': True, 'source_requests': 0,
                  'production_queue_writes': 0, 'days': 232, 'pages_per_day': 48,
                  'due_pages': 232*47, 'samples_per_scenario': 7,
                  'ordinary_selection': normal, 'blocked_uncached': blocked_baseline,
                  'blocked_first_proof': cold, 'blocked_cached': blocked,
                  'blocked_cached_after_other_provider_writes': blocked_after_other_provider,
                  'post_cnyes_mutation_proof': post_mutation_proof,
                  'writes_other_uncached': writes_other_baseline, 'writes_other_with_triggers': writes_other_cached,
                  'writes_cnyes_uncached': writes_cnyes_baseline, 'writes_cnyes_with_triggers': writes_cnyes_cached,
                  'wake_uncached': wake_baseline, 'wake_cached': wake_cached,
                  'first_page_due_update_selected_immediately': first in ready,
                  'implementation_sha256': {p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in files},
                  'limits': 'Synthetic SQLite dispatch/wake only; no provider network, body parsing or source throughput.'}
        Path('/var/tmp/cnyes-revision-cache-benchmark-20261028.json').write_text(
            json.dumps(result, indent=2)+'\n')
        print(json.dumps({k: result[k] for k in ('days','pages_per_day','due_pages','first_page_due_update_selected_immediately')}))
        print(json.dumps({'normal_median_wall_seconds':normal['median_wall_seconds'],
                          'blocked_uncached_median_wall_seconds':blocked_baseline['median_wall_seconds'],
                          'blocked_first_proof_seconds':cold['wall_seconds'],
                          'blocked_cached_median_wall_seconds':blocked['median_wall_seconds'],
                          'blocked_cached_after_other_provider_writes_seconds':blocked_after_other_provider['median_wall_seconds'],
                          'post_cnyes_mutation_proof_seconds':post_mutation_proof['wall_seconds'],
                          '128_other_writes_before_after_seconds':[writes_other_baseline['median_wall_seconds'],writes_other_cached['median_wall_seconds']],
                          '128_cnyes_writes_before_after_seconds':[writes_cnyes_baseline['median_wall_seconds'],writes_cnyes_cached['median_wall_seconds']],
                          'wake_uncached_median_wall_seconds':wake_baseline['median_wall_seconds'],
                          'wake_cached_median_wall_seconds':wake_cached['median_wall_seconds']}))
    finally:
        queue.close()
