"""Replay bounded retained problem pages against native queue outcomes.

Read-only SQLite/raw responses and immutable body receipts; no publisher HTTP,
queue owner, source-state reset, body export or historical completeness claim.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import FAILURE_OBSERVATION_CONTRACT_VERSION, utc_now
from scripts.audit_news_selected_articles import audit as audit_bodies


def audit(root, registry, diagnostic_path):
    diagnostic = json.loads(diagnostic_path.read_text())
    probes = diagnostic['probes']
    if not 1 <= len(probes) <= 8 or len({p['url'] for p in probes}) != len(probes):
        raise ValueError('bounded_distinct_cnyes_problem_pages_required')
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    errors, observations, complete_urls = [], [], []
    try:
        db.execute('BEGIN')
        for probe in probes:
            source = registry.require(probe['url'])
            if source.provider != 'cnyes' or probe.get('status') != 200:
                raise ValueError('reviewed_cnyes_response_required')
            raw_path = Path(probe['private_raw_path']).resolve()
            if not raw_path.is_relative_to(diagnostic_path.parent.resolve()):
                raise ValueError('diagnostic_raw_path_out_of_scope')
            if raw_path.stat().st_size > 8 << 20:
                raise ValueError('diagnostic_raw_response_oversized')
            payload = raw_path.read_bytes()
            if hashlib.sha256(payload).hexdigest() != probe['response_sha256']:
                raise ValueError('diagnostic_raw_response_digest_mismatch')
            row = db.execute('SELECT state,error_code,last_checked_at_utc,captured_at_utc,body_sha256 '
                             'FROM articles WHERE url=?', (probe['url'],)).fetchone()
            body = None
            try:
                body = extract_article(payload, source, page_url=probe['url'])
                outcome = 'complete'
                complete_urls.append(probe['url'])
            except NewsError as error:
                outcome = error.code
            clock_fresh = bool(row and row['last_checked_at_utc'] and
                datetime.fromisoformat(row['last_checked_at_utc']) >= datetime.fromisoformat(diagnostic['observed_at_utc']))
            matches = bool(row and row['state'] == outcome and clock_fresh and (
                row['body_sha256'] == body['body_sha256'] if body else
                row['error_code'] == outcome and row['body_sha256'] is None))
            item = {'url': probe['url'], 'raw_response_sha256': probe['response_sha256'],
                    'previous_probe_outcome': probe.get('extraction_error', 'complete'),
                    'current_parser_outcome': outcome, 'native_queue_outcome': dict(row) if row else None,
                    'actual_check_clock_fresh': clock_fresh, 'native_outcome_matches_retained_source': matches,
                    'accepted_as_article': outcome == 'complete', 'body_characters': len(body['body']) if body else 0}
            observations.append(item)
            if not matches:
                errors.append({'url': probe['url'], 'error': 'native_outcome_or_check_clock_mismatch'})
    finally:
        db.close()
    proof = audit_bodies(root, complete_urls, registry=registry) if complete_urls else None
    if proof is not None and not proof['accepted']:
        errors.append({'error': 'native_body_receipt_not_verified'})
    if proof is not None:
        verified_heads = {item['url']: item['body_sha256'] for item in proof['articles']}
        for item in observations:
            observed_head = (item['native_queue_outcome'] or {}).get('body_sha256')
            if item['accepted_as_article'] and verified_heads.get(item['url']) != observed_head:
                errors.append({'url': item['url'], 'error': 'native_head_changed_since_outcome_snapshot'})
    return {'contract_version': 1, 'article_failure_observation_contract_version': FAILURE_OBSERVATION_CONTRACT_VERSION,
            'observed_at_utc': utc_now(), 'diagnostic_path': str(diagnostic_path),
            'diagnostic_sha256': sha256_file(diagnostic_path), 'source_fingerprint': registry.sources['cnyes'].data_fingerprint,
            'accepted': not errors, 'errors': errors, 'observations': observations,
            'source_outcome_counts': dict(Counter(r['current_parser_outcome'] for r in observations)),
            'body_receipt_proof': proof, 'publisher_requests': 0, 'source_states_mutated': False,
            'scope': 'explicit retained problem-page outcomes and recovered bodies; not all errors or history',
            'historical_completeness_verified': False, 'publish': False, 'training_injection_enabled': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--diagnostics', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    result = audit(repo / config['root'], registry, args.diagnostics)
    result['implementation_sha256'] = {str(path): sha256_file(path) for path in (
        Path(__file__), repo / 'downloader/news_fulltext_policy.py',
        repo / 'downloader/news_fulltext_publishers.py', repo / 'downloader/news_fulltext_queue.py')}
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ('accepted', 'source_outcome_counts', 'errors', 'scope')}, ensure_ascii=False))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
