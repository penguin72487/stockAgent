"""Verify bounded native RSS continuation and immutable selected body heads.

No HTTP, writer lock, source-state resets, raw export or history-completeness
claim. Page checkpoints use one pinned WAL snapshot; body proofs reuse the
canonical selected-head verifier and must still match that snapshot.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sqlite3

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import discovery_fingerprint, load_config
from downloader.news_fulltext_bulk import BULK_RESTRICTED_ITEM_ERRORS, wordpress_base
from downloader.news_fulltext_feed import feed_page, feed_url
from downloader.news_fulltext_queue import utc_now
from scripts.audit_news_selected_articles import audit as audit_bodies


def _clock(value):
    clock = datetime.fromisoformat(value)
    if clock.tzinfo is None:
        raise ValueError('actual_timezone_aware_observation_required')
    return clock


def validate_scope(diagnostic, capture, registry):
    probes, jobs = diagnostic['probes'], capture['jobs']
    if (not 1 <= len(probes) <= 4 or len({p['url'] for p in probes}) != len(probes)
            or [p['url'] for p in probes] != [j['url'] for j in jobs]
            or not capture.get('native_capture_completed')):
        raise ValueError('bounded_completed_native_feed_jobs_required')
    candidates = {}
    for probe, job in zip(probes, jobs):
        source = registry.require(probe['url'])
        if (wordpress_base(probe['url']) not in source.fulltext_feeds
                or probe.get('status') != 200 or job.get('source_status') != 200
                or probe.get('error') or job.get('error')
                or job['provider'] != source.provider):
            raise ValueError('registered_successful_feed_scope_required')
        feed_page(probe['url'])
        if not 0 <= len(probe['permitted_body_candidates']) <= 100:
            raise ValueError('bounded_feed_candidate_scope_required')
        # A mixed envelope contributes metadata only, never accepted bodies.
        restricted = any(e['error'] in BULK_RESTRICTED_ITEM_ERRORS for e in job['item_errors'])
        if restricted and job['accepted_bodies'] != 0:
            raise ValueError('restricted_native_feed_must_not_accept_bodies')
        if not restricted and not job['item_errors']:
            for candidate in probe['permitted_body_candidates']:
                url = candidate['requested_url']
                if registry.require(url).provider != source.provider or url in candidates:
                    raise ValueError('distinct_same_provider_body_candidates_required')
                candidates[url] = {'probe': candidate, 'job': job}
    _clock(capture['observed_at_utc'])
    _clock(capture['finished_at_utc'])
    return candidates


def audit(root, registry, diagnostic_path, capture_path):
    diagnostic = json.loads(diagnostic_path.read_text())
    capture = json.loads(capture_path.read_text())
    if capture['diagnostic_sha256'] != sha256_file(diagnostic_path):
        raise ValueError('native_diagnostic_digest_mismatch')
    candidates = validate_scope(diagnostic, capture, registry)
    errors, pages, heads = [], [], {}
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        snapshot_at = utc_now()
        for job in capture['jobs']:
            source = registry.require(job['url'])
            row = db.execute('SELECT * FROM discovery WHERE url=?', (job['url'],)).fetchone()
            restricted = any(e['error'] in BULK_RESTRICTED_ITEM_ERRORS for e in job['item_errors'])
            expected_error = 'bulk_response_contains_restricted_text' if restricted else None
            checkpoint_matches = bool(row and row['kind'] == 'rss_fulltext'
                and row['provider'] == source.provider and row['error_code'] == expected_error
                and row['response_sha256'] == job['response_sha256']
                and row['parser_fingerprint'] == discovery_fingerprint(source)
                and row['last_checked_at_utc']
                and _clock(row['last_checked_at_utc']) >= _clock(job['observed_at_utc']))
            evidence = db.execute('SELECT total FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                 (wordpress_base(job['url']), feed_page(job['url']))).fetchone()
            next_url = feed_url(wordpress_base(job['url']), feed_page(job['url']) + 1)
            following = db.execute('SELECT kind,provider FROM discovery WHERE url=?', (next_url,)).fetchone()
            continuation = bool(evidence and evidence['total'] == -1 and following
                                and following['kind'] == 'rss_fulltext' and following['provider'] == source.provider)
            item_errors = [dict(r) for r in db.execute(
                'SELECT article_url,error_code FROM bulk_item_errors WHERE request_url=?', (job['url'],))]
            preserved_errors = all(any(r['article_url'] == e['url'] and r['error_code'] == e['error']
                                       for r in item_errors) for e in job['item_errors'])
            raw_not_quarantined = not any((root / ('quarantine/' + job['response_sha256'] + suffix)).exists()
                                         for suffix in ('.parquet', '.parquet.receipt.json')) if restricted else None
            pages.append({'url': job['url'], 'checkpoint_matches_native_capture': checkpoint_matches,
                          'next_page_url': next_url, 'next_page_registered_without_invented_total': continuation,
                          'restricted_metadata_only': restricted, 'restricted_errors_preserved': preserved_errors,
                          'restricted_envelope_not_quarantined': raw_not_quarantined,
                          'native_accepted_bodies': job['accepted_bodies']})
            if not checkpoint_matches or not continuation or not preserved_errors or raw_not_quarantined is False:
                errors.append({'url': job['url'], 'error': 'native_feed_continuation_not_verified'})
        for url in candidates:
            row = db.execute('SELECT state,body_sha256 FROM articles WHERE url=?', (url,)).fetchone()
            heads[url] = row['body_sha256'] if row and row['state'] == 'complete' else None
    finally:
        db.close()
    urls = list(candidates)
    proofs = [audit_bodies(root, urls[i:i + 16]) for i in range(0, len(urls), 16)]
    articles = [a for proof in proofs for a in proof['articles']]
    if not proofs or any(not p['accepted'] for p in proofs):
        errors.append({'error': 'selected_feed_body_receipts_not_verified'})
    verified = {a['url']: a for a in articles}
    newly_committed = {}
    for url, candidate in candidates.items():
        article = verified.get(url)
        if not article or not heads[url] or article['body_sha256'] != heads[url]:
            errors.append({'url': url, 'error': 'native_head_changed_since_page_snapshot'})
            continue
        if _clock(article['captured_at_utc']) >= _clock(capture['observed_at_utc']):
            probe, job = candidate['probe'], candidate['job']
            if (article['source_request_url'] != job['url'] or article['response_sha256'] != job['response_sha256']
                    or article['extraction_method'] != 'rss_content_encoded_v1'
                    or any(article[k] != probe[k] for k in ('body_sha256', 'published_at_raw'))
                    or json.loads(article['authors_json']) != json.loads(probe['authors_json'])):
                errors.append({'url': url, 'error': 'new_rss_body_source_identity_author_or_clock_mismatch'})
            newly_committed[job['url']] = newly_committed.get(job['url'], 0) + 1
    for job in capture['jobs']:
        if newly_committed.get(job['url'], 0) != job['accepted_bodies']:
            errors.append({'url': job['url'], 'error': 'native_accepted_count_not_verified'})
    return {'contract_version': 1, 'verified_at_utc': utc_now(), 'page_snapshot_at_utc': snapshot_at,
            'accepted': not errors, 'errors': errors, 'pages': pages,
            'selected_durable_body_heads': len(articles), 'newly_committed_body_heads': sum(newly_committed.values()),
            'preexisting_body_heads_preserved': len(articles) - sum(newly_committed.values()),
            'body_receipt_proofs': proofs, 'publisher_requests': 0, 'source_states_mutated': False,
            'diagnostic_sha256': sha256_file(diagnostic_path), 'capture_sha256': sha256_file(capture_path),
            'scope': 'explicit native RSS boundary jobs and selected durable heads; not all feeds or source history',
            'historical_completeness_verified': False, 'publish': False, 'training_injection_enabled': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--diagnostics', type=Path, required=True)
    parser.add_argument('--capture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    result = audit(repo / config['root'], registry, args.diagnostics, args.capture)
    result['implementation_sha256'] = {str(p): sha256_file(p) for p in (
        Path(__file__), repo / 'downloader/news_fulltext.py', repo / 'downloader/news_fulltext_queue.py')}
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ('accepted', 'errors', 'selected_durable_body_heads',
                                          'newly_committed_body_heads', 'preexisting_body_heads_preserved')}, ensure_ascii=False))
    return int(not result['accepted'])


if __name__ == '__main__':
    raise SystemExit(main())
