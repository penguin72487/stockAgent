"""Exact retained routing work and receipt replay, no publisher/production writes.

Three keyset windows from one pinned WAL, not selected fast hosts or titles.
Balanced ABBA repetitions preserve ordered metadata upserts, protected states,
all alias associations, real retained body/raw clocks and durable recovery.
Offline routing cost is not a publisher throughput or official-limit proof.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import resource
import sqlite3
import statistics
import tempfile
import time

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader import news_fulltext_queue as queue_module
from downloader import news_fulltext_urls as routing
from scripts.verify_news_recent_batches import verify_recent_batches


KEY = 'google_literal_url_routing_v1'
QUERY = ('SELECT r.*,a.title,a.metadata_date_raw FROM original_url_resolutions r '
         'JOIN articles a ON a.id=r.alias_id WHERE r.contract_version=? AND r.alias_id>? '
         "AND r.outcome='literal_original' ORDER BY r.alias_id LIMIT ?")


@contextmanager
def local_metadata_clock(value):
    before = queue_module.utc_now
    queue_module.utc_now = lambda: value
    try:
        yield
    finally:
        queue_module.utc_now = before


def reference_sync(q, *, limit=256, max_seconds=.04):
    """Exact pre-v79 routine: whole-policy cursor, per-proof existence seek."""
    fingerprint = q.db.execute("SELECT value FROM queue_settings WHERE key='policy_fingerprint'").fetchone()[0]
    progress = routing._read_checkpoint(q, KEY, {'policy_fingerprint': fingerprint, 'after': ''}, routing._valid_routing)
    if progress['after'] == 'done':
        return 0
    started, examined = time.monotonic(), 0
    rows = q.db.execute(QUERY, (routing._VERSION, bytes.fromhex(progress['after']), limit)).fetchall()
    with q.metrics.measure('original_url_routing'), q.db:
        for proof in rows:
            if examined and time.monotonic() - started >= max_seconds:
                break
            routing._route_literal(q, proof, proof)
            progress['after'] = proof['alias_id'].hex()
            examined += 1
        if not rows:
            progress['after'] = 'done'
        if examined or not rows:
            q.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (KEY, json.dumps(progress)))
    return examined


def _insert(db, table, rows):
    if not rows:
        return
    columns = list(rows[0])
    db.executemany('INSERT INTO ' + table + '(' + ','.join(columns) + ') VALUES ('
                   + ','.join('?' for _ in columns) + ')', ([row[column] for column in columns] for row in rows))


def _rows(db, table, ids):
    result = []
    for offset in range(0, len(ids), 400):
        chunk = ids[offset:offset + 400]
        result.extend(dict(row) for row in db.execute('SELECT * FROM ' + table + ' WHERE id IN ('
                      + ','.join('?' for _ in chunk) + ')', chunk))
    return result


def _digest(q):
    tables = ('articles', 'source_links', 'original_url_resolutions', 'original_url_metrics',
              'original_url_host_metrics', 'state_metrics', 'stored_body_metrics', 'hosts', 'finite_cohorts')
    result = {}
    for table in tables:
        rows = [tuple(row) for row in q.db.execute('SELECT * FROM ' + table + ' ORDER BY 1,2')]
        result[table] = hashlib.sha256(repr(rows).encode()).hexdigest()
    return result


def _sealed_batch(root, db):
    row = db.execute('SELECT path,receipt_path FROM accepted_batches ORDER BY path DESC LIMIT 1').fetchone()
    if not row:
        raise ValueError('real_retained_accepted_batch_required')
    receipt_path, body_path = root / row['receipt_path'], root / row['path']
    receipt = json.loads(receipt_path.read_bytes())
    if receipt['rows'] > 128 or body_path.stat().st_size > 32 << 20 or sha256_file(body_path) != receipt['sha256']:
        raise ValueError('bounded_sealed_batch_required')
    table = pq.read_table(body_path, use_threads=False)
    if table.num_rows != receipt['rows']:
        raise ValueError('sealed_batch_rows_mismatch')
    return table.to_pylist(), {'path': row['path'], 'receipt_path': row['receipt_path'],
        'sha256': receipt['sha256'], 'receipt_sha256': sha256_file(receipt_path), 'rows': table.num_rows}


def benchmark(config_path, workspace, *, limit=256, repetitions=2):
    cfg, registry, repo = load_config(config_path)
    root = repo / cfg['root']
    workspace.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = sqlite3.connect(f'file:{root / "queue.sqlite3"}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        settings = dict(db.execute('SELECT key,value FROM queue_settings'))
        observed = queue_module.utc_now()
        saved = json.loads(settings[KEY])
        after = saved['after']
        cursor = bytes.fromhex(after) if after not in {'', 'done'} else b''
        windows = []
        finite = [dict(row) for row in db.execute('SELECT * FROM finite_cohorts')]
        records, sealed = _sealed_batch(root, db)
        metrics = [dict(row) for row in db.execute('SELECT * FROM original_url_metrics WHERE urls>0')]
        for label, lower in [('first_literal_prefix', b''), ('current_keyset', cursor), ('last_keyspace', b'\xf0')]:
            proofs = [dict(row) for row in db.execute(QUERY, (routing._VERSION, lower, limit))]
            aliases = _rows(db, 'articles', [row['alias_id'] for row in proofs])
            targets = _rows(db, 'articles', list({row['original_id'] for row in proofs}))
            alias_ids = [row['alias_id'] for row in proofs]
            links = []
            for offset in range(0, len(alias_ids), 400):
                chunk = alias_ids[offset:offset + 400]
                links.extend(dict(row) for row in db.execute('SELECT * FROM source_links WHERE article_id IN ('
                             + ','.join('?' for _ in chunk) + ')', chunk))
            proof_columns = {row[1] for row in db.execute('PRAGMA table_info(original_url_resolutions)')}
            windows.append({'label': label, 'lower': lower.hex(), 'proofs': [
                {k: v for k, v in proof.items() if k in proof_columns} for proof in proofs],
                'aliases': aliases, 'targets': targets, 'links': links})
        owner = settings.get('current_owner_id')
    finally:
        db.close()

    cases = []
    with tempfile.TemporaryDirectory(prefix='news-route-replay-', dir=workspace) as temp:
        for window in windows:
            phases, outputs = [], {}
            for repetition in range(repetitions):
                for phase, mode in enumerate(('reference', 'candidate', 'candidate', 'reference')):
                    q = queue_module.NewsQueue(Path(temp) / f'{window["label"]}-{repetition}-{phase}', registry)
                    try:
                        with q.db:
                            _insert(q.db, 'finite_cohorts', finite)
                            combined = {row['id']: row for row in window['aliases'] + window['targets']}
                            _insert(q.db, 'articles', list(combined.values()))
                            _insert(q.db, 'source_links', window['links'])
                            _insert(q.db, 'original_url_resolutions', window['proofs'])
                            payload = ({'policy_fingerprint': settings['policy_fingerprint'], 'after': window['lower']}
                                if mode == 'reference' else {'contract_version': routing.ORIGINAL_URL_ROUTING_CONTRACT_VERSION,
                                    'routing_fingerprint': routing.routing_fingerprint(registry), 'after': window['lower']})
                            q.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (KEY, json.dumps(payload)))
                        execute = reference_sync if mode == 'reference' else routing.sync_original_url_routes
                        examined, slices = 0, 0
                        start, cpu = time.perf_counter(), time.process_time()
                        with local_metadata_clock(observed):
                            while count := execute(q, limit=limit):
                                examined += count
                                slices += 1
                                if slices > limit + 1:
                                    raise ValueError('routing_slice_did_not_converge')
                        cost = {'wall_seconds': time.perf_counter() - start, 'cpu_seconds': time.process_time() - cpu}
                        outputs[mode] = _digest(q)
                        # Replay the SAME real sealed records, without relabeling
                        # old publication/capture times or sending new requests.
                        with local_metadata_clock(observed), q.db:
                            q.add_articles([{'url': row['requested_url']} for row in records])
                        with local_metadata_clock(observed):
                            q.accept(records)
                        proof = verify_recent_batches(q.root, maximum_batches=8)
                        batch = q.db.execute('SELECT path FROM accepted_batches').fetchone()[0]
                        accepted_digest = sha256_file(q.root / batch)
                        # Controlled receipt-before-SQL recovery only in this
                        # temporary benchmark, never in the production queue.
                        with q.db:
                            q.db.execute('DELETE FROM accepted_batches WHERE path=?', (batch,))
                        with local_metadata_clock(observed):
                            q.recover_acceptance()
                        recovery = verify_recent_batches(q.root, maximum_batches=8)
                        full_cost = {'full_replay_wall_seconds': time.perf_counter() - start,
                                     'full_replay_cpu_seconds': time.process_time() - cpu}
                        phases.append({'mode': mode, 'repetition': repetition, 'examined': examined,
                            'slices': slices, **cost, **full_cost, 'body_receipt_verified': proof['accepted'],
                            'receipt_recovery_verified': recovery['accepted'], 'replayed_body_versions': len(records),
                            'accepted_parquet_sha256': accepted_digest,
                            'protected_input_states': dict(Counter(row['state'] for row in window['targets']))})
                    finally:
                        q.close()
            comparisons = {}
            for unit in ('wall_seconds', 'cpu_seconds', 'full_replay_wall_seconds', 'full_replay_cpu_seconds'):
                old = statistics.median(p[unit] for p in phases if p['mode'] == 'reference')
                new = statistics.median(p[unit] for p in phases if p['mode'] == 'candidate')
                comparisons[unit] = {'reference_median': old, 'candidate_median': new,
                                     'ratio': old / new if new else None}
            cases.append({'window': window['label'], 'keyset_after': window['lower'],
                'retained_aliases': len(window['proofs']), 'table_digests': outputs,
                'all_routing_tables_match': outputs['reference'] == outputs['candidate'],
                'all_sealed_replay_parquet_digests_match': len({p['accepted_parquet_sha256'] for p in phases}) == 1,
                'phases': phases, 'comparisons': comparisons})
    return {'contract_version': 1, 'observed_at_utc': observed, 'input_owner_id': owner,
        'routing_contract_version': routing.ORIGINAL_URL_ROUTING_CONTRACT_VERSION,
        'routing_fingerprint': routing.routing_fingerprint(registry), 'legacy_routing_checkpoint': saved,
        'proof_metrics': metrics, 'sealed_body_input': sealed, 'case_limit': limit, 'repetitions': repetitions,
        'cases': cases, 'accepted': all(case['all_routing_tables_match'] and case['all_sealed_replay_parquet_digests_match']
            and all(p['examined'] == case['retained_aliases'] and p['body_receipt_verified'] and p['receipt_recovery_verified']
                    for p in case['phases']) for case in cases),
        'cost_scope': 'routing window AND whole routing/sealed replay/receipt/SQL recovery/verification window; temporary fixture setup is separate',
        'peak_process_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'production_queue_writes': False, 'publisher_requests': 0, 'publish': False,
        'official_limit_proved': False, 'publisher_speedup_claimed': False, 'whole_history_complete': False,
        'implementation_sha256': {str(path): sha256_file(path) for path in (
            Path(__file__), Path('downloader/news_fulltext_urls.py'), Path('downloader/news_fulltext_queue.py'))}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--limit', type=int, default=256)
    parser.add_argument('--repeat', type=int, default=2)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if not 16 <= args.limit <= 512 or not 1 <= args.repeat <= 3:
        parser.error('bounded retained alias/repetition count required')
    result = benchmark(args.config, args.output.parent, limit=args.limit, repetitions=args.repeat)
    atomic_write_json(args.output, result)
    print(json.dumps({'accepted': result['accepted'], 'cases': [
        {k: case[k] for k in ('window', 'retained_aliases', 'all_routing_tables_match', 'comparisons')}
        for case in result['cases']], 'body_input_rows': result['sealed_body_input']['rows'],
        'publisher_requests': 0, 'production_queue_writes': False}))
    return int(not result['accepted'])


if __name__ == '__main__':
    raise SystemExit(main())
