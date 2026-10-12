"""Pinned read-only dispatch benchmark: equal work contract, different index.

No HTTP, leases or queue writes. Include all requested providers and lanes, even
when the new index loses. Date/kind ties need not choose the same URL, but their
eligibility and ordering scores must match. This is not service speed or source
capacity acceptance.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import statistics
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_queue import NewsQueue, utc_now


def benchmark(root: Path, providers, *, repetitions=5):
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        observed = utc_now()
        deadline = time.time()
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        cases = []
        for provider in providers:
            for stored_only, undated_only, lane in ((False, False, 'missing_preferred'),
                    (True, False, 'stored_recheck'), (False, True, 'undated_missing')):
                indexed = NewsQueue.article_dispatch_statement(stored_only=stored_only, undated_only=undated_only)
                reference = indexed.replace('INDEXED BY dispatch_work_kind', 'INDEXED BY dispatch_latest')
                statements = {'reference': reference, 'indexed': indexed}
                samples = {name: [] for name in statements}
                outputs, plans = {}, {}
                for name, statement in statements.items():
                    plans[name] = {state: [row[3] for row in db.execute('EXPLAIN QUERY PLAN ' + statement,
                        (provider, state, deadline))] for state in ('pending', 'retry', 'retry_cooldown')}
                for repetition in range(repetitions):
                    for name in (('reference', 'indexed') if repetition % 2 == 0 else ('indexed', 'reference')):
                        start, cpu = time.perf_counter(), time.process_time()
                        rows = [db.execute(statements[name], (provider, state, deadline)).fetchone()
                                for state in ('pending', 'retry', 'retry_cooldown')]
                        samples[name].append({'wall_seconds': time.perf_counter() - start,
                                              'cpu_seconds': time.process_time() - cpu})
                        outputs[name] = [None if row is None else {
                            'missing_body': row['body_sha256'] is None,
                            'metadata_order_day': row['metadata_order_day'],
                            'provider': row['provider'], 'state': row['state'],
                            'due_eligible': row['due'] <= deadline,
                        } for row in rows]
                ratios = {}
                for unit in ('wall_seconds', 'cpu_seconds'):
                    before = statistics.median(sample[unit] for sample in samples['reference'])
                    after = statistics.median(sample[unit] for sample in samples['indexed'])
                    ratios[unit] = {'reference_median': before, 'indexed_median': after,
                                    'speedup': before / after if after else None}
                cases.append({'provider': provider, 'lane': lane,
                    'ordering_and_eligibility_match': outputs['reference'] == outputs['indexed'],
                    'indexed_no_temporary_sort': all('TEMP B-TREE' not in step
                                                   for steps in plans['indexed'].values() for step in steps),
                    'outputs': outputs, 'query_plans': plans, 'measurements': samples, 'ratios': ratios})
        index_pages = db.execute("SELECT sum(pgsize),sum(ncell) FROM dbstat WHERE name='dispatch_work_kind'").fetchone()
        return {'contract_version': 2, 'observed_at_utc': observed,
                'owner_id': owner[0] if owner else None, 'providers': list(providers),
                'queue_known_urls': db.execute('SELECT coalesce(sum(articles),0) FROM state_metrics').fetchone()[0],
                'dispatch_index_physical_bytes': index_pages[0], 'dispatch_index_entries': index_pages[1],
                'repetitions': repetitions, 'cases': cases,
                'accepted': bool(cases) and all(c['ordering_and_eligibility_match'] and c['indexed_no_temporary_sort'] for c in cases),
                'scope': 'same pinned WAL snapshot, full provider ready-state query; not production HTTP/service throughput or history completeness',
                'implementation_sha256': {str(p): sha256_file(p) for p in (
                    Path('downloader/news_fulltext_queue.py'), Path(__file__))},
                'publish': False, 'all_history_complete': False}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_news_fulltext'))
    parser.add_argument('--provider', action='append', default=[])
    parser.add_argument('--repeat', type=int, default=5)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if not 3 <= args.repeat <= 9 or len(args.provider) > 8:
        parser.error('bounded provider/repetition count required')
    result = benchmark(args.root, list(dict.fromkeys(args.provider or ['cnyes'])), repetitions=args.repeat)
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ('accepted', 'queue_known_urls', 'dispatch_index_physical_bytes', 'scope')}))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
