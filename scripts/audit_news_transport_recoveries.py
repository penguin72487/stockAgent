"""Read-only causal proof: an observed error must precede a verified new body.

Resetting a connection, keeping an old body or a service heartbeat is not an
article recovery. Reuse canonical transport and body/receipt verification;
never reset source deadlines, own a production queue or contact a publisher.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import utc_now
from scripts.audit_news_selected_articles import audit as audit_bodies
from scripts.audit_news_transport_observations import _recent_files, _verify_event


ARTICLE_ENDPOINTS = frozenset({'article_html', 'article_pdf'})


def later_complete_head(row, failure_at):
    if (not row or row['state'] != 'complete' or not row['body_sha256']
            or not row['captured_at_utc']):
        return False
    captured, failed = (datetime.fromisoformat(value) for value in (row['captured_at_utc'], failure_at))
    if captured.utcoffset() is None or failed.utcoffset() is None:
        raise ValueError('timezone_aware_forward_capture_required')
    return captured > failed


def audit(root, registry, transport):
    if transport.get('accepted') is not True or not transport.get('owner_id'):
        raise ValueError('verified_current_transport_snapshot_required')
    providers = transport['providers']
    if len(providers) > 256 or sum(len(row['verified_failure_events']) for row in providers.values()) > 256:
        raise ValueError('bounded_transport_sample_required')
    snapshot = datetime.fromisoformat(transport['observed_at_utc'])
    days = (snapshot.strftime('%Y%m%d'), (snapshot - timedelta(days=1)).strftime('%Y%m%d'))
    witnesses = []
    database = f'file:{root / "queue.sqlite3"}?mode=ro'
    with sqlite3.connect(database, uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()[0]
        if owner != transport['owner_id']:
            raise ValueError('failure_snapshot_owner_changed')
        for provider, sample in providers.items():
            expected = {row['object_sha256']: row for row in sample['verified_failure_events']}
            if len(expected) != len(sample['verified_failure_events']) or len(expected) > 128:
                raise ValueError('bounded_distinct_failure_events_required')
            found = {}
            for path in _recent_files(root / 'transport_problems' / provider, days, 128):
                digest = path.stem.rsplit('-', 1)[-1]
                if digest not in expected:
                    continue
                if path.is_symlink() or path.stat().st_size > 65536:
                    raise ValueError('unsafe_original_transport_observation')
                event = json.loads(path.read_bytes())
                verified = _verify_event(path, event, provider, registry.sources[provider], registry)
                if (verified != expected[digest] or event['owner_id'] != owner
                        or sha256_file(path) != expected[digest]['file_sha256']):
                    raise ValueError('immutable_transport_observation_changed')
                found[digest] = event
            if set(found) != set(expected):
                raise ValueError('exact_transport_failure_sample_not_found')
            for digest, event in found.items():
                row = db.execute('SELECT provider,state,error_code,due,body_sha256,captured_at_utc '
                                 'FROM articles WHERE url=?', (event['requested_url'],)).fetchone()
                forward = (event['endpoint'] in ARTICLE_ENDPOINTS and row is not None
                           and row['provider'] == provider and later_complete_head(row, event['observed_at_utc']))
                witnesses.append({'provider': provider, 'url': event['requested_url'],
                    'failure_object_sha256': digest, 'failure_at_utc': event['observed_at_utc'],
                    'endpoint': event['endpoint'], 'phase': event['phase'],
                    'current': dict(row) if row else None, 'later_formal_capture_observed': bool(forward),
                    'new_body_raw_byline_clocks_receipt_and_current_replay_verified': False})
    urls = list(dict.fromkeys(row['url'] for row in witnesses if row['later_formal_capture_observed']))
    proofs, verified = [], {}
    for offset in range(0, len(urls), 16):
        proof = audit_bodies(root, urls[offset:offset + 16], registry=registry)
        proofs.append(proof)
        if proof['accepted']:
            verified.update({row['url']: row for row in proof['articles']})
    for row in witnesses:
        body = verified.get(row['url'])
        row['new_body_raw_byline_clocks_receipt_and_current_replay_verified'] = bool(body
            and body['current_source_replay_verified'] and body['capture_contract_matches_current_source']
            and body['captured_at_utc'] == row['current']['captured_at_utc']
            and body['body_sha256'] == row['current']['body_sha256'])
    with sqlite3.connect(database, uri=True) as db:
        same_owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()[0] == owner
    article_rows = [row for row in witnesses if row['endpoint'] in ARTICLE_ENDPOINTS]
    recovered = sum(row['new_body_raw_byline_clocks_receipt_and_current_replay_verified'] for row in article_rows)
    return {'contract_version': 1, 'verified_at_utc': utc_now(), 'owner_id': owner,
        'transport_sample_at_utc': transport['observed_at_utc'], 'witnesses': witnesses,
        'actual_body_receipt_proofs': proofs, 'sample_failure_events': len(witnesses),
        'sample_article_failure_events': len(article_rows), 'verified_article_recoveries': recovered,
        'all_sample_article_failures_recovered': bool(article_rows) and recovered == len(article_rows),
        'accepted': same_owner and all(proof['accepted'] for proof in proofs),
        'same_owner_at_completion': same_owner, 'publisher_requests': 0, 'queue_writes': False,
        'publish': False, 'all_history_complete': False,
        'scope': 'immutable bounded current-owner error sample versus strictly later verified current body heads; not all source failures'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--transport-snapshot', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.transport_snapshot.stat().st_size > 8 << 20 or args.output.exists():
        raise ValueError('bounded_input_and_new_immutable_output_required')
    config, registry, repo = load_config(args.config)
    result = audit(repo / config['root'], registry, json.loads(args.transport_snapshot.read_bytes()))
    result.update(transport_snapshot_sha256=sha256_file(args.transport_snapshot),
                  implementation_sha256=sha256_file(Path(__file__)))
    atomic_write_json(args.output, result, durable=True)
    print(json.dumps({key: value for key, value in result.items()
                      if key not in {'witnesses', 'actual_body_receipt_proofs'}}))
    return int(not result['accepted'])


if __name__ == '__main__':
    raise SystemExit(main())
