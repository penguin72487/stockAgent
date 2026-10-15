"""Read-only retained-response proof and replay progress; no publisher calls."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import html
import json
from pathlib import Path
import sqlite3
import time
import re
from types import SimpleNamespace

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records, wordpress_base, _plain_text
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metrics import bulk_body_page_classification
from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import source_path, utc_now
from downloader.news_fulltext_replay import REPLAY_CHECKPOINT_CONTRACT_VERSION


def _checkpoint_progress(db, fingerprint):
    # Legacy "checked" meant parsed once, not resumed after a lease/retry.
    # Read old schemas without migration: only the canonical owner may write.
    columns = {r[1] for r in db.execute('PRAGMA table_info(cached_bulk_replays)')}
    version = 'checkpoint_contract_version' if 'checkpoint_contract_version' in columns else '1'
    phase = [dict(r) for r in db.execute(
        'SELECT state,' + version + ' AS checkpoint_contract_version,count(*) AS response_files,'
        'sum(accepted_heads) AS recovered_heads FROM cached_bulk_replays '
        'WHERE source_fingerprint=? GROUP BY state,' + version, (fingerprint,))]
    fresh = sum(r['response_files'] for r in phase if r['state'] == 'pending')
    deferred = sum(r['response_files'] for r in phase if r['state'] == 'deferred')
    legacy = sum(r['response_files'] for r in phase if r['state'] == 'checked'
                 and r['checkpoint_contract_version'] < REPLAY_CHECKPOINT_CONTRACT_VERSION)
    known = sum(r['response_files'] for r in phase)
    unfinished = fresh + deferred + legacy
    return {'phase_by_state': phase, 'known_response_files': known,
            'pending_response_files': unfinished, 'fresh_pending_response_files': fresh,
            'deferred_response_files': deferred, 'legacy_checked_response_files': legacy,
            'examined_response_files': known - unfinished,
            'replay_checkpoint_contract_version': REPLAY_CHECKPOINT_CONTRACT_VERSION}


def audit(root, registry, relative_paths, *, sample_legacy_pages=0):
    if len(relative_paths) + sample_legacy_pages > 16 or not 0 <= sample_legacy_pages <= 16:
        raise ValueError('select at most sixteen retained responses')
    source = registry.sources['cnyes']
    replay_fingerprint = cnyes_bulk_fingerprint(source)
    db = sqlite3.connect(f'file:{root.resolve() / "queue.sqlite3"}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        observed = utc_now()
        progress = _checkpoint_progress(db, replay_fingerprint)
        paths = list(relative_paths)
        if sample_legacy_pages:
            columns = {r[1] for r in db.execute('PRAGMA table_info(cached_bulk_replays)')}
            legacy = 'AND checkpoint_contract_version=1' if 'checkpoint_contract_version' in columns else ''
            paths.extend('quarantine/' + r[0] + '.parquet' for r in db.execute(
                "SELECT response_sha256 FROM cached_bulk_replays WHERE source_fingerprint=? AND state='checked' "
                + legacy + ' ORDER BY priority,response_sha256 LIMIT ?', (replay_fingerprint, sample_legacy_pages)))
        errors = [dict(r) for r in db.execute('SELECT response_sha256,error_code,checked_at_utc FROM cached_bulk_replays '
            "WHERE source_fingerprint=? AND state='failed_validation' LIMIT 16", (replay_fingerprint,))]
        circuits = [dict(r) for r in db.execute('SELECT endpoint,error_code,request_url,not_before FROM bulk_endpoint_cooldowns '
            'WHERE provider=? AND data_fingerprint=?', ('cnyes', source.data_fingerprint))]
        proofs = [_source_proof(root, registry, source, db, relative, replay_fingerprint) for relative in dict.fromkeys(paths)]
        db.rollback()
    finally:
        db.close()
    return {'accepted': bool(proofs) and all(p['parser_or_metadata_verified'] for p in proofs),
            'observed_at_utc': observed, 'source_fingerprint': source.data_fingerprint,
            'api_replay_fingerprint': replay_fingerprint, 'cnyes_bulk_page_classification_version': 2,
            **progress, 'validation_errors': errors, 'current_parser_circuits': circuits,
            'source_proofs': proofs, 'publisher_http_requests': 0,
            'scope': 'retained public-response integrity, current parser and pinned head-readiness proof; examined files are not full bodies downloaded',
            'all_history_complete': False, 'publish': False, 'training_injection_enabled': False}


def _source_proof(root, registry, source, db, relative, fingerprint):
    path = source_path(root, relative)
    if path.stat().st_size > 16 << 20:
        raise ValueError('cached_source_envelope_oversized')
    receipt = json.loads(source_path(root, relative + '.receipt.json').read_text())
    if receipt['parquet_path'] != relative or receipt['publish'] is not False or sha256_file(path) != receipt['sha256']:
        raise ValueError('cached_source_receipt_invalid')
    table = pq.read_table(path, use_threads=False)
    if table.num_rows != 1:
        raise ValueError('cached_source_grain_invalid')
    raw = table.to_pylist()[0]
    if len(raw['raw_response_body']) > 16 << 20:
        raise ValueError('cached_source_raw_response_oversized')
    digest = hashlib.sha256(raw['raw_response_body']).hexdigest()
    if raw['response_sha256'] != digest or receipt['response_sha256'] != digest or raw['publish'] is not False:
        raise ValueError('cached_source_response_digest_invalid')
    if registry.require(raw['request_url']).provider != 'cnyes' or wordpress_base(raw['request_url']) not in source.cnyes_apis:
        raise ValueError('cached_source_provider_invalid')
    if (raw['authorization_fingerprint'] != source.authorization_fingerprint
            or json.loads(raw['authorization_json']) != source.authorization):
        raise ValueError('cached_source_authorization_changed')
    captured = datetime.fromisoformat(raw['captured_at_utc'])
    if captured.tzinfo is None or captured > datetime.now(timezone.utc):
        raise ValueError('cached_source_observation_clock_invalid')
    response = Response(raw['request_url'], 200, {'content-type': 'application/json'}, raw['raw_response_body'])
    page, posts = cnyes_pagination(response)
    if receipt['article_count'] != len(posts):
        raise ValueError('cached_source_row_count_invalid')
    if any(type(p.get('payment')) is not int or p['payment'] != 0 for p in posts):
        raise ValueError('cached_source_contains_restricted_rows')
    records, item_errors = cnyes_records(response, SimpleNamespace(registry=registry), source)
    counts = Counter(e['error'] for e in item_errors)
    item_outcomes = []
    for error in item_errors[:16]:
        index = error.get('item_number')
        post = posts[index] if type(index) is int and 0 <= index < len(posts) else {}
        fragment = post.get('content')
        encoded = isinstance(fragment, str) and '<' not in fragment and bool(re.match(r'\s*&lt;[a-zA-Z][^;]*?(?:&gt;|[\s&])', fragment))
        text = _plain_text(html.unescape(fragment) if encoded else fragment, parser=source.html_parser) if isinstance(fragment, str) else None
        title = _plain_text(post['title'], parser=source.html_parser) if isinstance(post.get('title'), str) else None
        item_outcomes.append({k: error.get(k) for k in ('item_number', 'url', 'error')}
            | {'source_content_kind': type(fragment).__name__, 'source_text_characters': len(text) if text is not None else None,
               'source_content_equals_title': bool(text and text == title),
               'source_content_is_null_placeholder': bool(text and text.lower() == 'null')})
    head_readiness = Counter()
    for record in records:
        head = db.execute('SELECT state,due,body_sha256 FROM articles WHERE id=?',
                          (bytes.fromhex(record['article_id']),)).fetchone()
        if head and head['body_sha256']:
            kind = 'already_stored_body'
        elif head and (head['state'] == 'inflight' or
                       head['state'] in {'retry', 'retry_cooldown'} and head['due'] > time.time()):
            kind = 'temporarily_blocked_missing_body'
        elif head and head['state'] not in {'pending', 'retry', 'retry_cooldown', 'body_not_found'}:
            kind = 'protected_terminal_state'
        else:
            kind = 'ready_missing_body' if head else 'not_yet_registered_body'
        head_readiness[kind] += 1
    checkpoint = db.execute('SELECT state,accepted_heads,checked_at_utc FROM cached_bulk_replays '
                            'WHERE source_fingerprint=? AND response_sha256=?', (fingerprint, digest)).fetchone()
    return {'parquet_path': relative, 'parquet_sha256': receipt['sha256'],
        'response_sha256': digest, 'request_url': raw['request_url'], 'source_observed_at_utc': raw['captured_at_utc'],
        'pagination': page, 'validated_full_bodies': len(records), 'item_error_counts': dict(counts),
        'bounded_item_outcomes': item_outcomes,
        'bulk_body_page_classification': bulk_body_page_classification(records, item_errors, cnyes=True),
        'head_readiness': dict(head_readiness), 'replay_checkpoint': dict(checkpoint) if checkpoint else None,
        'source_replay_permitted': bool(source.allowed and source.cnyes_apis),
        'metadata_only_html_fallback': bool(not records and counts and set(counts) <= {'bulk_body_not_provided', 'bulk_exact_duplicate_row'}),
        'parser_or_metadata_verified': not (set(counts) - {'bulk_body_not_provided', 'bulk_exact_duplicate_row'})}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--source-parquet', action='append', default=[], help='Exact retained path relative to the canonical root')
    parser.add_argument('--sample-legacy-pages', type=int, default=0, help='Bounded read-only sample of old checked checkpoints')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if not 0 <= args.sample_legacy_pages <= 16 or not 1 <= len(args.source_parquet) + args.sample_legacy_pages <= 16:
        parser.error('select one to sixteen retained responses')
    config, registry, repo = load_config(args.config)
    result = audit(repo / config['root'], registry, args.source_parquet, sample_legacy_pages=args.sample_legacy_pages)
    result['implementation_sha256'] = {str(p): sha256_file(repo / p) for p in (
        Path('downloader/news_fulltext.py'), Path('downloader/news_fulltext_bulk.py'),
        Path('downloader/news_fulltext_capacity.py'), Path('downloader/news_fulltext_metrics.py'),
        Path('downloader/news_fulltext_policy.py'), Path('downloader/news_fulltext_queue.py'),
        Path('downloader/news_fulltext_replay.py'), Path('scripts/audit_news_cached_replay.py'))}
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
