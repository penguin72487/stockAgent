"""Read-only native routing acceptance: exact counters and sampled URL proofs.

This does not certify every publisher body, metadata headline/stock identity or
complete history. No collector/Google/publisher request is performed.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import utc_now
from stockagent.data.news_source_urls import GOOGLE_LITERAL_URL_CONTRACT, resolve_google_literal_url


def verify(config_path: Path, *, maximum_samples=128, root=None):
    if type(maximum_samples) is not int or not 1 <= maximum_samples <= 1024:
        raise ValueError('invalid proof sample bound')
    started = time.perf_counter()
    config, registry, repo = load_config(config_path)
    root = root or repo / config['root']
    version = GOOGLE_LITERAL_URL_CONTRACT
    db = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()[0]
        expected = db.execute("SELECT coalesce(sum(articles),0) FROM state_metrics WHERE provider='unregistered:news.google.com'").fetchone()[0]
        actual = {(r[0], r[1], r[2]): r[3] for r in db.execute('SELECT outcome,error_code,target_queued,count(*) FROM original_url_resolutions WHERE contract_version=? GROUP BY outcome,error_code,target_queued', (version,))}
        metrics = {(r[0], r[1], r[2]): r[3] for r in db.execute('SELECT outcome,error_code,target_queued,urls FROM original_url_metrics WHERE contract_version=? AND urls>0', (version,))}
        errors, samples, counts = [], [], Counter()
        cursor = db.execute('SELECT * FROM original_url_resolutions WHERE contract_version=? ORDER BY alias_id LIMIT ?', (version, maximum_samples)).fetchall()
        # Ensure literal and gated/unresolved paths are checked when present,
        # without hiding the fact that URL-proof verification is sampled.
        seen = {r['alias_id'] for r in cursor}
        for outcome in ('literal_original', 'opaque_original', 'unresolved'):
            row = db.execute('SELECT * FROM original_url_resolutions WHERE contract_version=? AND outcome=? LIMIT 1', (version, outcome)).fetchone()
            if row and row['alias_id'] not in seen:
                cursor.append(row)
                seen.add(row['alias_id'])
        for proof in cursor:
            alias = db.execute('SELECT url,host,state,body_sha256,metadata_date_raw FROM articles WHERE id=?', (proof['alias_id'],)).fetchone()
            if alias is None or alias['host'] != 'news.google.com':
                errors.append({'alias_id': proof['alias_id'].hex(), 'error': 'missing_original_metadata'})
                continue
            decoded = resolve_google_literal_url(alias['url'])
            expected_id = hashlib.sha256(decoded.original_url.encode()).digest() if decoded.original_url else None
            valid = (decoded.outcome == proof['outcome'] and decoded.error_code == proof['error_code']
                     and decoded.original_url == proof['original_url'] and expected_id == proof['original_id']
                     and decoded.alternate_url == proof['alternate_url'] and decoded.envelope_sha256 == proof['envelope_sha256']
                     and decoded.envelope_bytes == proof['envelope_bytes'] and proof['metadata_identity_verified'] == 0)
            if not valid:
                errors.append({'alias_id': proof['alias_id'].hex(), 'error': 'url_proof_mismatch'})
            target = db.execute('SELECT url,provider,state,body_sha256,due,first_seen_utc FROM articles WHERE id=?', (expected_id,)).fetchone() if expected_id else None
            if proof['target_queued'] and (target is None or target['url'] != decoded.original_url):
                errors.append({'alias_id': proof['alias_id'].hex(), 'error': 'queued_target_missing_or_mismatched'})
            counts[proof['outcome']] += 1
            source = registry.match(decoded.original_url) if decoded.original_url else None
            samples.append({'alias_id': proof['alias_id'].hex(), 'original_url': decoded.original_url,
                            'metadata_state': alias['state'], 'metadata_has_body': bool(alias['body_sha256']),
                            'target_provider': target['provider'] if target else source.provider if source else None,
                            'target_state': target['state'] if target else None, 'target_has_body': bool(target and target['body_sha256']),
                            'target_due': target['due'] if target else None, 'metadata_date_raw': alias['metadata_date_raw'],
                            'envelope_sha256': decoded.envelope_sha256, 'proof_matches_decoder': valid})
        examined = sum(actual.values())
        checks = {'incremental_counters_match_full_proof_table': actual == metrics,
                  'examined_within_known_metadata_cohort': 0 <= examined <= expected,
                  'at_least_one_real_metadata_proof_sampled': bool(samples),
                  'sampled_proofs_and_targets_consistent': not errors}
        return {'contract_version': 1, 'resolver_contract_version': version, 'observed_at_utc': utc_now(),
                'owner_id': owner, 'known_redirect_urls': expected, 'examined_urls': examined,
                'remaining_metadata_urls': expected - examined, 'samples_verified': len(samples),
                'sample_outcomes': dict(counts), 'samples': samples, 'errors': errors, 'checks': checks,
                'accepted': all(checks.values()), 'url_proof_verification_scope': 'bounded native sample plus exact whole-table metric reconciliation',
                'metadata_identity_verified': False, 'historical_completeness_verified': False,
                'publisher_requests': 0, 'body_acceptance_claimed': False,
                'resolver_sha256': sha256_file(repo / 'stockagent/data/news_source_urls.py'),
                'wall_seconds': time.perf_counter() - started}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--maximum-samples', type=int, default=128)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    result = verify(args.config, maximum_samples=args.maximum_samples)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ('owner_id', 'known_redirect_urls', 'examined_urls', 'samples_verified', 'checks', 'accepted', 'wall_seconds')}))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
