"""Cross-check missing CNYES heads against retained page originals, without HTTP or queue writes."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time

import pyarrow as pa

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_cnyes_history import digest as history_digest
from downloader.news_fulltext_policy import NewsError, cnyes_bulk_fingerprint
from scripts.audit_news_cached_replay import _source_proof


STATES = ('source_article_empty_template', 'source_article_test_only', 'source_body_image_only',
          'source_body_quote_only', 'source_not_found', 'body_not_found')
PREFIX = 'https://news.cnyes.com/news/id/'


def _stamp(path):
    try:
        info = path.stat()
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    except FileNotFoundError:
        return None


def _prior_pairs(prior, *, fingerprint=None, parser_identity=None):
    """Only explicit, previously verified article/response pairs are reusable."""
    if isinstance(prior, list):
        result = set()
        for item in prior:
            result.update(_prior_pairs(item, fingerprint=fingerprint, parser_identity=parser_identity))
        return result
    if prior.get('contract') == 'cnyes_retained_missing_crosscheck_v1' and (
            prior.get('source_fingerprint') != fingerprint or prior.get('parser_identity') != parser_identity):
        return set()  # Recheck after source, authorization or parser semantics change.
    pairs = set()
    for row in prior.get('rows', []):
        news_id = row.get('news_id')
        for item in row.get('observations', []):
            # Older audits verified raw identity and absence but did not run
            # the complete admission parser on nonempty fragments.
            if (item.get('evidence_state') == 'verified'
                    and item.get('content_class') in {'content_missing', 'content_empty_or_null'}):
                pairs.add((news_id, item['source_response_sha256']))
    for item in prior.get('target_outcomes', []):
        if item.get('evidence_state') == 'verified':
            pairs.add((item['news_id'], item['source_response_sha256']))
    return pairs


def _candidate_index(db, prior_pairs, source, only_responses=None):
    heads = {}
    for state in STATES:
        for row in db.execute('SELECT url,last_checked_at_utc FROM articles INDEXED BY host_state '
                'WHERE host=? AND state=? AND body_sha256 IS NULL AND parquet_path IS NULL',
                ('news.cnyes.com', state)):
            url = row['url']
            if url.startswith(PREFIX) and re.fullmatch(r'[0-9]+', url[len(PREFIX):]):
                heads[int(url[len(PREFIX):])] = {'state': state, 'last_checked_at_utc': row['last_checked_at_utc']}
    if len(heads) > 20_000:
        raise ValueError('missing_head_bound_exceeded')
    candidates = defaultdict(lambda: {'targets': defaultdict(set), 'observations': defaultdict(set)})
    counts = Counter()
    for row in db.execute('SELECT * FROM cnyes_history_page_observations'):
        counts['observations_scanned'] += 1
        if counts['observations_scanned'] > 100_000:
            raise ValueError('observation_bound_exceeded')
        if only_responses is not None and row['response_sha256'] not in only_responses:
            continue
        try:
            observation = json.loads(row['observation_json'])
            identities = observation['identities']
            if not isinstance(identities, list) or len(identities) > 60:
                raise ValueError('invalid_identities')
            hits = [item for item in identities if type(item.get('newsId')) is int and item['newsId'] in heads
                    and (item['newsId'], row['response_sha256']) not in prior_pairs]
            if not hits:
                continue
            artifact = observation['artifact']
            relative = 'quarantine/' + row['response_sha256'] + '.parquet'
            if (not re.fullmatch(r'[0-9a-f]{64}', row['response_sha256'])
                    or history_digest(observation) != row['observation_sha256']
                    or observation.get('provider') != 'cnyes'
                    or observation.get('scope_url') != row['scope_url']
                    or observation.get('page') != row['page']
                    or observation.get('response_sha256') != row['response_sha256']
                    or observation.get('contract_version') != row['evidence_contract_version']
                    or observation.get('source_bulk_fingerprint') != row['source_bulk_fingerprint']
                    or observation.get('authorization_fingerprint') != row['authorization_fingerprint']):
                raise ValueError('invalid_observation_identity')
            if (artifact.get('kind') != 'canonical_quarantine' or artifact.get('parquet_path') != relative
                    or artifact.get('receipt_path') != relative + '.receipt.json'):
                counts['noncanonical_matching_observations'] += 1
                continue
            if row['authorization_fingerprint'] != source.authorization_fingerprint:
                counts['other_authorization_matching_observations'] += 1
                continue
            for item in hits:
                post_sha = item['source_post_sha256']
                if not isinstance(post_sha, str) or not re.fullmatch(r'[0-9a-f]{64}', post_sha):
                    raise ValueError('invalid_post_digest')
            page = candidates[row['response_sha256']]
            for item in hits:
                page['targets'][item['newsId']].add(item['source_post_sha256'])
                page['observations'][item['newsId']].add(row['observation_sha256'])
        except (ValueError, TypeError, KeyError, AttributeError):
            counts['invalid_matching_observations'] += 1
    return heads, candidates, counts


def audit(root, registry, prior, *, max_responses=2000, max_seconds=300, only_responses=None):
    if not 1 <= max_responses <= 10_000 or not 1 <= max_seconds <= 900:
        raise ValueError('invalid_audit_bounds')
    if only_responses is not None:
        only_responses = set(only_responses)
        if not 1 <= len(only_responses) <= 64 or any(not isinstance(value, str)
                or not re.fullmatch(r'[0-9a-f]{64}', value) for value in only_responses):
            raise ValueError('invalid_selected_responses')
    root = root.resolve()
    db_path, wal_path = root / 'queue.sqlite3', root / 'queue.sqlite3-wal'
    before = (_stamp(db_path), _stamp(wal_path))
    if before[0] is None or before[1] is not None and before[1][2]:
        raise ValueError('immutable_snapshot_requires_empty_wal')
    db = sqlite3.connect(db_path.as_uri() + '?mode=ro&immutable=1', uri=True)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    source = registry.sources['cnyes']
    fingerprint = cnyes_bulk_fingerprint(source)
    repo = Path(__file__).resolve().parent.parent
    parser_paths = ('scripts/audit_news_cached_replay.py', 'downloader/news_fulltext_bulk.py',
                    'downloader/news_fulltext_cnyes_history.py', 'downloader/news_fulltext_policy.py')
    parser_identity = {path: sha256_file(repo / path) for path in parser_paths}
    start = time.monotonic()
    outcomes, failures = [], []
    try:
        heads, candidates, counts = _candidate_index(db, _prior_pairs(
            prior, fingerprint=fingerprint, parser_identity=parser_identity), source, only_responses)
        ordered = sorted(candidates.items(), key=lambda item: (-len(item[1]['targets']), item[0]))
        for response_sha, page in ordered:
            if len(failures) + counts['responses_verified'] >= max_responses or time.monotonic() - start >= max_seconds:
                break
            try:
                proof = _source_proof(root, registry, source, db, 'quarantine/' + response_sha + '.parquet',
                                      fingerprint, target_hashes=page['targets'])
                if proof['response_sha256'] != response_sha:
                    raise ValueError('cached_target_response_identity_mismatch')
                for item in proof['target_outcomes']:
                    outcomes.append({**item, 'source_response_sha256': response_sha,
                        'source_observed_at_utc': proof['source_observed_at_utc'],
                        'parquet_sha256': proof['parquet_sha256'],
                        'observation_sha256': sorted(page['observations'][item['news_id']]),
                        'source_replay_permitted': proof['source_replay_permitted'],
                        'head': heads[item['news_id']], 'evidence_state': 'verified'})
                counts['responses_verified'] += 1
            except (OSError, ValueError, KeyError, TypeError, NewsError, pa.ArrowException) as error:
                failures.append({'source_response_sha256': response_sha,
                    'error': error.code if isinstance(error, NewsError) else type(error).__name__ + ':' + str(error)[:120],
                    'target_count': len(page['targets'])})
            if (len(failures) + counts['responses_verified']) % 100 == 0:
                print(json.dumps({'responses_processed': len(failures) + counts['responses_verified'],
                                  'responses_total': len(ordered)}), flush=True)
    finally:
        db.close()
    if before != (_stamp(db_path), _stamp(wal_path)):
        raise ValueError('production_snapshot_changed_during_audit')
    if parser_identity != {path: sha256_file(repo / path) for path in parser_paths}:
        raise ValueError('parser_changed_during_audit')
    pair_counts = Counter(item['outcome'] for item in outcomes)
    unresolved = sum(counts[key] for key in ('invalid_matching_observations',
        'noncanonical_matching_observations', 'other_authorization_matching_observations'))
    return {'contract': 'cnyes_retained_missing_crosscheck_v1',
        'completed_at_utc': datetime.now(timezone.utc).isoformat(),
        'elapsed_seconds': round(time.monotonic() - start, 3),
        'source_fingerprint': fingerprint, 'parser_identity': parser_identity,
        'missing_heads_in_selected_states': len(heads),
        'candidate_responses': len(candidates), 'candidate_pairs': sum(len(p['targets']) for p in candidates.values()),
        'candidate_heads': len({i for p in candidates.values() for i in p['targets']}),
        **counts, 'response_failures': failures, 'pair_outcome_counts': dict(pair_counts),
        'target_outcomes': outcomes, 'unfinished_responses': len(candidates) - len(failures) - counts['responses_verified'],
        'unresolved_matching_observations': unresolved,
        'selected_responses_requested': len(only_responses) if only_responses is not None else None,
        'selected_responses_without_candidate': sorted(only_responses - candidates.keys()) if only_responses is not None else [],
        'publisher_http_requests': 0, 'production_queue_writes': 0, 'article_bodies_copied': 0,
        'historical_point_in_time_verified': False, 'all_history_complete': False,
        'scope': 'selected exact desktop IDs; previously verified pairs excluded; original page/receipt and target post verified'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--exclude-audit', type=Path, action='append', required=True,
                        help='Previously verified audit; repeat for a partial continuation')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-responses', type=int, default=2000)
    parser.add_argument('--max-seconds', type=int, default=300)
    parser.add_argument('--response-sha256', action='append', help='Optionally inspect only these exact retained responses')
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    root = (repo / config['root']).resolve()
    output = args.output.resolve()
    if output.is_relative_to(root) or output.exists():
        parser.error('output must be new and outside the source root')
    paths = list(dict.fromkeys(args.exclude_audit))
    if len(paths) > 16:
        parser.error('at most sixteen prior audits')
    saved, priors = [], []
    for path in paths:
        before = _stamp(path)
        if before is None or before[2] > 128 << 20:
            parser.error('missing or oversized prior audit')
        digest = sha256_file(path)
        priors.append(json.loads(path.read_bytes()))
        saved.append((path, before, digest))
    result = audit(root, registry, priors, max_responses=args.max_responses, max_seconds=args.max_seconds,
                   only_responses=args.response_sha256)
    if any(before != _stamp(path) or digest != sha256_file(path) for path, before, digest in saved):
        raise ValueError('prior_audit_changed_during_read')
    result['excluded_prior_audit_sha256'] = [digest for _, _, digest in saved]
    result['implementation_sha256'] = {str(p): sha256_file(repo / p) for p in (
        Path('scripts/audit_news_cnyes_retained_missing.py'), Path('scripts/audit_news_cached_replay.py'),
        Path('downloader/news_fulltext_bulk.py'), Path('downloader/news_fulltext_cnyes_history.py'))}
    atomic_write_bytes(output, (json.dumps(result, ensure_ascii=False, indent=2) + '\n').encode(), durable=True, mode=0o600)
    print(json.dumps({k: v for k, v in result.items() if k not in {'target_outcomes', 'response_failures'}},
                     ensure_ascii=False))
    return 0 if not (result['response_failures'] or result['unfinished_responses']
                     or result['unresolved_matching_observations'] or result['selected_responses_without_candidate']) else 1


if __name__ == '__main__':
    raise SystemExit(main())
