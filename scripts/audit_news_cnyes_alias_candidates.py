"""Bounded read-only alias candidates, not a proof of interchangeable pages.

An existing desktop body can explain a failed mobile view without a missing
story. Never accept aliases, reset failures, fetch publishers or copy bodies.
The sample is host/state index order, not random or the whole remaining queue.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urljoin, urlsplit

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_publishers import cnyes_supported_mobile_view
from downloader.news_fulltext_queue import utc_now


SAMPLE_SQL = ("SELECT id,url,title FROM articles INDEXED BY host_state "
              "WHERE host='m.cnyes.com' AND state='pending' "
              "AND provider='cnyes' AND body_sha256 IS NULL LIMIT ?")

AMP_SQL = ("SELECT a.id,a.url,a.state,a.error_code,a.attempts,a.due,a.last_checked_at_utc,"
           "p.id AS candidate_id,p.url AS candidate_url,p.state AS candidate_state,"
           "p.body_sha256,p.parquet_path,p.captured_at_utc "
           "FROM articles a INDEXED BY host_state LEFT JOIN articles p "
           "ON p.id=a.cnyes_canonical_candidate_id AND p.provider='cnyes' "
           "WHERE a.host='amp-news.cnyes.com' AND a.provider='cnyes' AND a.body_sha256 IS NULL "
           "AND a.state IN ('pending','retry','retry_cooldown','inflight') ORDER BY a.url LIMIT ?")
AMP_MAX_ENCODED_BYTES = 64 << 20
AMP_MAX_DECODED_BYTES = 128 << 20
AMP_MAX_BATCH_ROWS = 4096


def _declared_amp_head(raw, canonical_url, amp_url, *, extraction_method):
    """Inspect only a native HTML head, never infer aliases from API IDs/text."""
    if extraction_method == 'cnyes_api_content_v1':
        return {'kind': 'api_content_not_reviewed_native_amp_declaration',
                'native_head_inspected': False, 'publisher_declared_counterpart': False}
    if not raw.lstrip().startswith(b'<'):
        return {'kind': 'not_native_html', 'native_head_inspected': False,
                'publisher_declared_counterpart': False}
    from bs4 import BeautifulSoup, SoupStrainer
    soup = BeautifulSoup(raw, 'lxml', parse_only=SoupStrainer('head'))
    heads = soup.find_all('head')
    canonical = heads[0].select('link[rel=canonical][href]') if len(heads) == 1 else []
    amp = heads[0].select('link[rel=amphtml][href]') if len(heads) == 1 else []
    canonical_match = len(canonical) == 1 and urljoin(canonical_url, canonical[0]['href']) == canonical_url
    amp_match = len(amp) == 1 and urljoin(canonical_url, amp[0]['href']) == amp_url
    match = canonical_match and amp_match
    return {'kind': 'native_head_declares_exact_amp_counterpart' if match else 'native_head_not_exact_counterpart',
            'native_head_inspected': True, 'head_count': len(heads), 'canonical_link_count': len(canonical),
            'amphtml_link_count': len(amp), 'canonical_matches': canonical_match,
            'amphtml_matches': amp_match, 'publisher_declared_counterpart': match}


def audit_amp_declarations(root: Path, maximum_rows: int = 256) -> dict:
    """Read immutable candidate envelopes once, without parsing accepted prose.

    The complete currently queued AMP scope is bounded independently of raw I/O.
    A matching head declaration is evidence to review, not permission to fetch
    AMP or to commit an alias. No body extractor, HTTP or queue owner is created.
    """
    if type(maximum_rows) is not int or not 1 <= maximum_rows <= 4096:
        raise ValueError('bounded_integer_sample_required')
    import pyarrow as pa
    import pyarrow.parquet as pq
    from downloader.news_fulltext_queue import source_path
    started, cpu = time.monotonic(), time.process_time()
    with closing(sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        observed = utc_now()
        rows = [dict(row) for row in db.execute(AMP_SQL, (maximum_rows+1,))]
        truncated = len(rows) > maximum_rows
        rows = rows[:maximum_rows]
        plan = [row['detail'] for row in db.execute('EXPLAIN QUERY PLAN '+AMP_SQL, (maximum_rows+1,))]
        paths = sorted({row['parquet_path'] for row in rows if row['candidate_state'] == 'complete'
                        and row['body_sha256'] and row['parquet_path']})
        receipts = {path: db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone()
                    for path in paths}
    def selected_hash(values):
        return hashlib.sha256(json.dumps([{k: v.hex() if isinstance(v, bytes) else v for k, v in row.items()}
            for row in values], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    selected_sha = selected_hash(rows)
    counts, findings, grouped = Counter(), [], {}
    for row in rows:
        record = {'amp_url': row['url'], 'candidate_url': row['candidate_url'],
                  'candidate_body_sha256': row['body_sha256'], 'publisher_declared_counterpart': False}
        match = re.fullmatch(r'https://amp-news[.]cnyes[.]com/news/id/([1-9][0-9]*)', row['url'])
        if not match or row['candidate_url'] != 'https://news.cnyes.com/news/id/'+match[1]:
            record['kind'] = 'no_exact_literal_primary_candidate'
        elif not (row['candidate_state'] == 'complete' and row['body_sha256'] and row['parquet_path']):
            record['kind'] = 'no_complete_primary_candidate'
        else:
            grouped.setdefault(row['parquet_path'], []).append((row, record))
            continue
        counts[record['kind']] += 1
        findings.append(record)
    encoded_bytes = decoded_bytes = original_bytes = 0
    batches = []
    columns = ['article_id', 'body_sha256', 'requested_url', 'final_url', 'source_request_url',
               'response_sha256', 'source_response_article_id', 'raw_response_body', 'extraction_method']
    for relative in sorted(grouped):
        if not receipts[relative]:
            raise ValueError('amp_candidate_has_no_committed_receipt')
        receipt_path = source_path(root, receipts[relative][0])
        if receipt_path.stat().st_size > 65536:
            raise ValueError('amp_candidate_receipt_exceeds_bound')
        with receipt_path.open('rb') as stream:
            receipt_bytes = stream.read(65537)
        if len(receipt_bytes) > 65536:
            raise ValueError('amp_candidate_receipt_exceeds_bound')
        receipt = json.loads(receipt_bytes)
        path = source_path(root, relative)
        if path.stat().st_size > AMP_MAX_ENCODED_BYTES-encoded_bytes:
            raise ValueError('amp_candidate_encoded_budget_exceeded')
        with path.open('rb') as stream:
            payload = stream.read(AMP_MAX_ENCODED_BYTES-encoded_bytes+1)
        encoded_bytes += len(payload)
        if encoded_bytes > AMP_MAX_ENCODED_BYTES:
            raise ValueError('amp_candidate_encoded_budget_exceeded')
        digest = hashlib.sha256(payload).hexdigest()
        if digest != receipt['sha256']:
            raise ValueError('amp_candidate_committed_parquet_hash_mismatch')
        parquet = pq.ParquetFile(pa.BufferReader(payload))
        decoded_bytes += sum(parquet.metadata.row_group(i).total_byte_size for i in range(parquet.metadata.num_row_groups))
        if (parquet.metadata.num_rows > AMP_MAX_BATCH_ROWS or decoded_bytes > AMP_MAX_DECODED_BYTES
                or parquet.metadata.num_rows != receipt['rows']):
            raise ValueError('amp_candidate_decoded_budget_or_rows_invalid')
        batch = parquet.read(columns=columns, use_threads=False).to_pylist()
        indexed = {record['article_id']: record for record in batch}
        if len(indexed) != len(batch):
            raise ValueError('amp_candidate_duplicate_article_identity')
        envelopes = {record['article_id']: record['raw_response_body'] for record in batch
                     if record['raw_response_body'] is not None}
        inspected = {}
        for row, finding in grouped[relative]:
            record = indexed.get(row['candidate_id'].hex())
            if (not record or record['body_sha256'] != row['body_sha256']
                    or record['requested_url'] != row['candidate_url'] or record['final_url'] != row['candidate_url']):
                raise ValueError('amp_candidate_head_and_original_identity_mismatch')
            raw_id = record['source_response_article_id']
            raw = envelopes.get(raw_id)
            if not isinstance(raw, bytes):
                raise ValueError('amp_candidate_original_response_hash_mismatch')
            if raw_id not in inspected:
                original_bytes += len(raw)
                inspected[raw_id] = hashlib.sha256(raw).hexdigest()
            if inspected[raw_id] != record['response_sha256']:
                raise ValueError('amp_candidate_original_response_hash_mismatch')
            finding.update(_declared_amp_head(raw, row['candidate_url'], row['url'],
                                            extraction_method=record['extraction_method']))
            finding.update(source_request_url=record['source_request_url'], response_sha256=record['response_sha256'],
                           parquet_path=relative, committed_parquet_sha256=digest)
            counts[finding['kind']] += 1
            findings.append(finding)
        batches.append({'path': relative, 'sha256': digest, 'rows': len(batch),
                        'selected_candidate_heads': len(grouped[relative]), 'distinct_originals': len(inspected)})
    with closing(sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        after = [dict(row) for row in db.execute(AMP_SQL, (maximum_rows+1,))][:maximum_rows]
    return {'contract_version': 1, 'observed_at_utc': observed, 'queue_owner_id': owner[0] if owner else None,
        'inspection_kind': 'retained_primary_native_amp_declarations', 'selected_queued_amp_heads': len(rows),
        'maximum_rows': maximum_rows, 'sample_limit_reached': truncated,
        'entire_current_queued_amp_scope_selected': not truncated, 'selected_inputs_sha256': selected_sha,
        'selected_inputs_unchanged': selected_hash(after) == selected_sha,
        'counts': dict(counts), 'findings': sorted(findings, key=lambda row: row['amp_url']),
        'committed_batches': batches, 'query_plan': plan,
        'publisher_requests': 0, 'source_states_mutated': False, 'body_extractors_called': 0,
        'accepted_body_text_retained': False, 'alias_source_equivalence_verified': False, 'aliases_admitted': 0,
        'historical_completeness_verified': False, 'publish': False, 'remote_training_export': False,
        'inventory_cost': {'wall_seconds': time.monotonic()-started, 'cpu_seconds': time.process_time()-cpu,
                           'encoded_bytes_read_once': encoded_bytes, 'decoded_byte_bound_used': decoded_bytes,
                           'distinct_original_response_bytes': original_bytes}}


def audit(root: Path, maximum_rows: int = 512) -> dict:
    if type(maximum_rows) is not int or not 1 <= maximum_rows <= 4096:
        raise ValueError('bounded_integer_sample_required')
    started, cpu = time.monotonic(), time.process_time()
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    counts, digest, selected = Counter(), hashlib.sha256(), []
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        observed = utc_now()
        plan = [r['detail'] for r in db.execute('EXPLAIN QUERY PLAN ' + SAMPLE_SQL, (maximum_rows,))]
        rows = db.execute(SAMPLE_SQL, (maximum_rows,)).fetchall()
        for row in rows:
            digest.update(row['id'])
            parts = urlsplit(row['url'])
            match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
            if parts.scheme != 'https' or parts.netloc != 'm.cnyes.com' or not match or parts.fragment:
                counts['unsupported_request_shape'] += 1
                continue
            story_id = match[1]
            target = db.execute('SELECT id,state,body_sha256,captured_at_utc,title FROM articles WHERE url=?',
                                ('https://news.cnyes.com/news/id/' + story_id,)).fetchone()
            candidate = bool(target and target['state'] == 'complete' and target['body_sha256'])
            counts['existing_canonical_body_candidate' if candidate else 'no_complete_canonical_body_candidate'] += 1
            supported = cnyes_supported_mobile_view(parts)
            counts['supported_current_mobile_view' if supported else 'unverified_query_view'] += 1
            title_match = None
            if candidate and row['title'] and target['title']:
                title_match = row['title'] == target['title']
                counts['exact_metadata_title_match' if title_match else 'different_metadata_titles'] += 1
            selected.append({'mobile_article_id': row['id'].hex(), 'publisher_story_id': story_id,
                'query_present': bool(parts.query), 'supported_current_mobile_view': supported,
                'existing_canonical_body_candidate': candidate,
                'canonical_article_id': target['id'].hex() if target else None,
                'canonical_captured_at_utc': target['captured_at_utc'] if candidate else None,
                'exact_metadata_title_match': title_match})
        return {'contract_version': 1, 'observed_at_utc': observed,
            'queue_owner_id': owner[0] if owner else None, 'maximum_rows': maximum_rows,
            'sampled_pending_mobile_heads': len(rows), 'sample_limit_reached': len(rows) == maximum_rows,
            'sample_basis': 'bounded pending mobile host/state index order; not random, latest-first or all remaining work',
            'selected_ids_sha256': digest.hexdigest(), 'counts': dict(counts), 'candidates': selected,
            'query_plan': plan, 'publisher_requests': 0, 'source_states_mutated': False,
            'canonical_receipts_verified': False, 'alias_source_equivalence_verified': False,
            'historical_completeness_verified': False, 'publish': False, 'remote_training_export': False,
            'inventory_cost': {'wall_seconds': time.monotonic() - started, 'cpu_seconds': time.process_time() - cpu},
            'next_action': 'verify source-declared identity, rights, grain and original receipts before any canonical reuse; retain distinct observations and unknown aliases'}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--maximum-rows', type=int, default=512)
    parser.add_argument('--amp-declarations', action='store_true',
                        help='Inspect retained primary candidate declarations for current missing AMP heads; zero HTTP/writes')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    result = (audit_amp_declarations if args.amp_declarations else audit)(repo / config['root'], args.maximum_rows)
    result['current_source_fingerprint'] = registry.sources['cnyes'].data_fingerprint
    result['configuration_sha256'] = sha256_file(args.config)
    result['implementation_sha256'] = sha256_file(Path(__file__))
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print({k: result[k] for k in ('sampled_pending_mobile_heads', 'selected_queued_amp_heads', 'counts', 'inventory_cost',
                                  'publisher_requests', 'source_states_mutated', 'alias_source_equivalence_verified') if k in result})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
