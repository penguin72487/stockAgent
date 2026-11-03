"""Read-only retained-response proof and replay progress; no publisher calls."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import html
import json
from pathlib import Path
import sqlite3
import time
import re
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records, wordpress_base, _plain_text
from downloader.news_fulltext_cnyes_history import _read_accepted_batch_original, digest as history_digest
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metrics import bulk_body_page_classification
from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import source_path, utc_now
from downloader.news_fulltext_replay import REPLAY_CHECKPOINT_CONTRACT_VERSION
from downloader.parquet_integrity import signature


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


def _check_history_envelope(relative, raw, receipt, response_sha):
    """Bind a history envelope to its actual request, bytes and private receipt."""
    key = history_digest((raw['request_url'], raw['authorization_fingerprint']))
    expected_dir = f'discovery_responses/by_request/{key[:2]}/{key}/'
    allowed = (relative == f'discovery_responses/{response_sha}.parquet'
               or relative.startswith(expected_dir) and re.fullmatch(
                   re.escape(response_sha) + r'(?:-\d{6,})?\.parquet', relative[len(expected_dir):]))
    if not allowed:
        raise ValueError('cached_history_envelope_path_invalid')
    if (receipt.get('contract') != 'cnyes_history_source_envelope_v1'
            or type(receipt.get('raw_response_bytes')) is not int
            or receipt['raw_response_bytes'] != len(raw['raw_response_body'])
            or receipt.get('article_bodies_accepted') is not False):
        raise ValueError('cached_history_envelope_receipt_invalid')


def _source_proof(root, registry, source, db, relative, fingerprint, *, target_hashes=None,
                  target_hashes_by_request=None, artifact_kind='canonical_quarantine',
                  receipt_relative=None, accepted_request_url=None, accepted_response_sha=None):
    if target_hashes is not None and target_hashes_by_request is not None:
        raise ValueError('ambiguous_cached_target_selection')
    if artifact_kind not in {'canonical_quarantine', 'cnyes_history_source_envelope', 'accepted_body_batch'}:
        raise ValueError('unsupported_cached_source_kind')
    if artifact_kind == 'accepted_body_batch':
        if not all(isinstance(value, str) for value in (receipt_relative, accepted_request_url, accepted_response_sha)):
            raise ValueError('cached_accepted_batch_identity_missing')
    else:
        receipt_relative = relative + '.receipt.json'
    path = source_path(root, relative)
    source_before = signature(path)
    if artifact_kind != 'accepted_body_batch' and path.stat().st_size > 16 << 20:
        raise ValueError('cached_source_envelope_oversized')
    receipt_path = source_path(root, receipt_relative)
    receipt_before = signature(receipt_path)
    if receipt_path.stat().st_size > 65536:
        raise ValueError('cached_source_receipt_oversized')
    if artifact_kind == 'accepted_body_batch':
        try:
            loaded = _read_accepted_batch_original(SimpleNamespace(root=root, db=db),
                {'parquet_path': relative, 'receipt_path': receipt_relative}, source,
                accepted_request_url, accepted_response_sha)
        except sqlite3.Error as error:
            raise ValueError('cached_accepted_batch_catalog_unavailable') from error
        if loaded is None:
            raise ValueError('cached_accepted_batch_original_invalid')
        raw, receipt, receipt_bytes = loaded['original'], loaded['receipt'], loaded['receipt_bytes']
        if (tuple(loaded['source_file_signature']) != source_before
                or tuple(loaded['source_receipt_signature']) != receipt_before):
            raise ValueError('cached_source_changed_during_proof')
        source_sha = receipt['sha256']
    else:
        receipt_bytes = receipt_path.read_bytes()
        receipt = json.loads(receipt_bytes)
        with path.open('rb') as handle:
            source_bytes = handle.read((16 << 20) + 1)
        if len(source_bytes) > 16 << 20:
            raise ValueError('cached_source_envelope_oversized')
        source_sha = hashlib.sha256(source_bytes).hexdigest()
        if receipt['parquet_path'] != relative or receipt['publish'] is not False or source_sha != receipt['sha256']:
            raise ValueError('cached_source_receipt_invalid')
        # Decode the exact bytes whose receipt was checked.
        table = pq.read_table(pa.BufferReader(source_bytes), use_threads=False)
        if table.num_rows != 1:
            raise ValueError('cached_source_grain_invalid')
        raw = table.to_pylist()[0]
    if len(raw['raw_response_body']) > 16 << 20:
        raise ValueError('cached_source_raw_response_oversized')
    digest = hashlib.sha256(raw['raw_response_body']).hexdigest()
    if (raw['response_sha256'] != digest or raw['publish'] is not False
            or artifact_kind != 'accepted_body_batch' and receipt['response_sha256'] != digest):
        raise ValueError('cached_source_response_digest_invalid')
    if artifact_kind == 'cnyes_history_source_envelope':
        _check_history_envelope(relative, raw, receipt, digest)
    if registry.require(raw['request_url']).provider != 'cnyes' or wordpress_base(raw['request_url']) not in source.cnyes_apis:
        raise ValueError('cached_source_provider_invalid')
    if (raw['authorization_fingerprint'] != source.authorization_fingerprint
            or artifact_kind != 'accepted_body_batch' and json.loads(raw['authorization_json']) != source.authorization):
        raise ValueError('cached_source_authorization_changed')
    captured = datetime.fromisoformat(raw['captured_at_utc'])
    if captured.tzinfo is None or captured > datetime.now(timezone.utc):
        raise ValueError('cached_source_observation_clock_invalid')
    response = Response(raw['request_url'], 200, {'content-type': 'application/json'}, raw['raw_response_body'])
    page, posts = cnyes_pagination(response)
    if artifact_kind == 'canonical_quarantine' and receipt['article_count'] != len(posts):
        raise ValueError('cached_source_row_count_invalid')
    if any(type(p.get('payment')) is not int or p['payment'] != 0 for p in posts):
        raise ValueError('cached_source_contains_restricted_rows')
    other_request_targets = defaultdict(set)
    if target_hashes_by_request is not None:
        # Equal response bytes can have been captured for a different query.
        # Select target observations only after the original request is read
        # from the verified envelope; unrelated observations must not affect
        # either the accepted post hashes or the attribution.
        target_hashes = target_hashes_by_request.get(raw['request_url'], {})
        for request_url, targets in target_hashes_by_request.items():
            if request_url != raw['request_url']:
                for news_id, post_hashes in targets.items():
                    if news_id not in target_hashes:
                        other_request_targets[news_id].update(post_hashes)
    records, item_errors = cnyes_records(response, SimpleNamespace(registry=registry), source)
    target_outcomes, source_only_outcomes = None, []
    if target_hashes is not None:
        # A page checkpoint alone cannot establish what it said about a
        # particular missing article. Bind each target to the exact saved
        # post, and report only hashes/outcomes, never copied publisher text.
        posts_by_id = {post['newsId']: post for post in posts}
        records_by_url = {record['requested_url']: record for record in records}
        errors_by_url = {item['url']: item['error'] for item in item_errors
                         if item['error'] != 'bulk_exact_duplicate_row'}
        target_outcomes = []
        for news_id, expected_hashes in sorted(target_hashes.items()):
            if (type(news_id) is not int or news_id <= 0 or not expected_hashes
                    or news_id not in posts_by_id):
                raise ValueError('cached_target_post_missing')
            post_sha = history_digest(posts_by_id[news_id])
            if set(expected_hashes) != {post_sha}:
                raise ValueError('cached_target_post_identity_mismatch')
            url = f'https://news.cnyes.com/news/id/{news_id}'
            record = records_by_url.get(url)
            error = errors_by_url.get(url)
            if bool(record) == bool(error):
                raise ValueError('cached_target_parser_outcome_ambiguous')
            target_outcomes.append({'news_id': news_id, 'source_post_sha256': post_sha,
                'outcome': 'validated_source_body' if record else error,
                'body_sha256': record['body_sha256'] if record else None,
                'body_bytes': record['body_bytes'] if record else None})
        # A mismatched observation is not valid page provenance. The raw
        # response itself can nevertheless prove a post from its own actual
        # request and clock, so record that separately without adopting the
        # mismatched observation or copying publisher text.
        for news_id, claimed_hashes in sorted(other_request_targets.items()):
            post = posts_by_id.get(news_id)
            if post is None:
                source_only_outcomes.append({'news_id': news_id, 'source_post_sha256': None,
                    'outcome': 'target_id_absent_from_raw_source', 'body_sha256': None,
                    'body_bytes': None, 'matches_observed_post_hash': False})
                continue
            post_sha = history_digest(post)
            url = f'https://news.cnyes.com/news/id/{news_id}'
            record, error = records_by_url.get(url), errors_by_url.get(url)
            if bool(record) == bool(error):
                raise ValueError('cached_target_parser_outcome_ambiguous')
            source_only_outcomes.append({'news_id': news_id, 'source_post_sha256': post_sha,
                'outcome': 'validated_source_body' if record else error,
                'body_sha256': record['body_sha256'] if record else None,
                'body_bytes': record['body_bytes'] if record else None,
                'matches_observed_post_hash': claimed_hashes == {post_sha}})
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
    # Replay checkpoints are keyed by content SHA for canonical quarantine;
    # they do not prove this separately captured request was replayed.
    checkpoint = (db.execute('SELECT state,accepted_heads,checked_at_utc FROM cached_bulk_replays '
                  'WHERE source_fingerprint=? AND response_sha256=?', (fingerprint, digest)).fetchone()
                  if artifact_kind == 'canonical_quarantine' else None)
    result = {'parquet_path': relative, 'receipt_path': receipt_relative,
        'source_artifact_kind': artifact_kind, 'parquet_sha256': receipt['sha256'],
        'response_sha256': digest, 'request_url': raw['request_url'], 'source_observed_at_utc': raw['captured_at_utc'],
        'pagination': page, 'validated_full_bodies': len(records), 'item_error_counts': dict(counts),
        'bounded_item_outcomes': item_outcomes,
        'bulk_body_page_classification': bulk_body_page_classification(records, item_errors, cnyes=True),
        'head_readiness': dict(head_readiness), 'replay_checkpoint': dict(checkpoint) if checkpoint else None,
        'source_replay_permitted': bool(source.allowed and source.cnyes_apis),
        'metadata_only_html_fallback': bool(not records and counts and set(counts) <= {'bulk_body_not_provided', 'bulk_exact_duplicate_row'}),
        'parser_or_metadata_verified': not (set(counts) - {'bulk_body_not_provided', 'bulk_exact_duplicate_row'})}
    if target_outcomes is not None:
        result['target_outcomes'] = target_outcomes
    if target_hashes_by_request is not None:
        result['source_only_target_outcomes'] = source_only_outcomes
    if (source_path(root, relative) != path or source_path(root, receipt_relative) != receipt_path
            or sha256_file(path) != source_sha or receipt_path.read_bytes() != receipt_bytes
            or signature(path) != source_before or signature(receipt_path) != receipt_before):
        raise ValueError('cached_source_changed_during_proof')
    result['source_file_signature'] = list(source_before)
    result['source_receipt_signature'] = list(receipt_before)
    return result


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
