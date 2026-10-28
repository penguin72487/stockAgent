"""Measure repeat CNYES host-paused selection in a synthetic local queue."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
import tempfile
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_cnyes_index import request_url
from downloader.news_fulltext_queue import NewsQueue


class DisabledCache:
    disabled = True

    def hit(self, *_):
        return False

    def wake_deadline(self, *_):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    source = registry.sources['cnyes']
    blocked = ('api.cnyes.com',)
    samples = {'uncached': [], 'cached': []}
    with tempfile.TemporaryDirectory(prefix='news-host-proof-bench-', dir='/var/tmp') as directory:
        queue = NewsQueue(Path(directory), registry)
        try:
            rows = [(request_url(source, day, page),) for day in range(16323, 16323 + 232)
                    for page in range(1, 49)]
            with queue.db:
                queue.db.executemany("INSERT INTO discovery(url,kind,provider) VALUES (?,'index','cnyes')", rows)
            assert queue.db.execute('SELECT count(*) FROM cnyes_index_revision_rounds').fetchone()[0] == 0
            queue._cnyes_revision_empty_selection = DisabledCache()
            for _ in range(7):
                wall, cpu = time.perf_counter(), time.process_time()
                selected = queue.discovery_candidates(source, 17, excluded_hosts=blocked)
                samples['uncached'].append({'wall_seconds': time.perf_counter()-wall, 'cpu_seconds': time.process_time()-cpu})
                assert not selected
            del queue._cnyes_revision_empty_selection
            wall, cpu = time.perf_counter(), time.process_time()
            assert not queue.discovery_candidates(source, 17, excluded_hosts=blocked)
            cold = {'wall_seconds': time.perf_counter()-wall, 'cpu_seconds': time.process_time()-cpu}
            for _ in range(7):
                wall, cpu = time.perf_counter(), time.process_time()
                selected = queue.discovery_candidates(source, 17, excluded_hosts=blocked)
                samples['cached'].append({'wall_seconds': time.perf_counter()-wall, 'cpu_seconds': time.process_time()-cpu})
                assert not selected
            healthy = 'https://news.cnyes.com/rss'
            with queue.db:
                queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'feed','cnyes')", (healthy,))
            assert [row['url'] for row in queue.discovery_candidates(source, 17, excluded_hosts=blocked)] == [healthy]
            assert queue.discovery_candidates(source, 17)[0]['url'] == healthy
        finally:
            queue.close()
    medians = {mode: {metric: statistics.median(row[metric] for row in group)
                      for metric in ('wall_seconds', 'cpu_seconds')} for mode, group in samples.items()}
    paths = (Path(__file__), Path('downloader/news_fulltext_queue.py'),
             Path('downloader/news_fulltext_host_circuit.py'), Path('downloader/news_fulltext_cnyes_dispatch_cache.py'))
    result = {'contract_version': 1, 'synthetic': True, 'revision_rounds': 0, 'paused_jobs': len(rows),
              'samples': samples, 'first_proof': cold, 'medians': medians,
              'new_healthy_host_invalidated_proof': True, 'real_publisher_calls': 0,
              'production_queue_written': False, 'source_hashes': {str(p): sha256_file(p) for p in paths},
              'limits': 'Synthetic hot SQLite selection only; no body processing or source throughput.'}
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
