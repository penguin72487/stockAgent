"""Inspect only unfinished CNYES issuer keys using retained, verified originals.

Candidate title/code matches are leads, never verified issuer identity or news bodies.
This program neither contacts a publisher nor submits work to the live queue.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from zoneinfo import ZoneInfo

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, cnyes_bulk_fingerprint
from downloader.parquet_integrity import signature
from scripts.audit_news_cnyes_issuer_overlap import HASH, PREFIX, _source_posts, issuer_key, select_sources
from scripts.audit_news_issuer_overlap import match_local_records, verified_finlab


TAIPEI = ZoneInfo('Asia/Taipei')


def candidate(post):
    """Retain conflicting names while looking for a bounded, explicitly tentative key."""
    title, stocks, markets, epoch = (post.get(name) for name in ('title', 'stock', 'market', 'publishAt'))
    source = post.get('source')
    if source != '台灣證券交易所':
        reason = 'publisher_not_twse'
    elif not isinstance(title, str) or len(title) > 4096:
        reason = 'title_absent_or_oversized'
    elif not isinstance(stocks, list) or len(stocks) != 1 or not isinstance(stocks[0], str) or not stocks[0]:
        reason = 'stock_not_single_string'
    elif type(epoch) is not int:
        reason = 'publication_epoch_missing'
    elif not isinstance(markets, list):
        reason = 'market_metadata_missing'
    else:
        names = {m.get('name') for m in markets if isinstance(m, dict)
                 and m.get('code') == stocks[0] and isinstance(m.get('name'), str) and m['name']}
        reason = ('market_name_missing' if not names else 'market_names_ambiguous' if len(names) != 1 else
                  'title_market_name_mismatch' if not title.startswith(next(iter(names)) + ':') else
                  'unexplained_original_key_rejection')
    symbol = stocks[0] if isinstance(stocks, list) and len(stocks) == 1 and isinstance(stocks[0], str) else None
    names = sorted({m.get('name') for m in markets if symbol and isinstance(m, dict) and m.get('code') == symbol
                    and isinstance(m.get('name'), str) and len(m['name']) <= 1024}) if isinstance(markets, list) else []
    prefix = subject = day = None
    if isinstance(title, str) and len(title) <= 4096 and ':' in title:
        prefix, subject = title.split(':', 1)
        if not prefix or not subject:
            prefix = subject = None
    if type(epoch) is int:
        try:
            day = datetime.fromtimestamp(epoch, TAIPEI).date().isoformat()
        except (ValueError, OverflowError, OSError):
            pass
    key = (symbol, day, subject) if source == '台灣證券交易所' and symbol and day and subject else None
    return {'reason': reason, 'publisher_source': source if isinstance(source, str) else None,
        'stock_symbol_candidate': symbol, 'market_names_for_symbol': names,
        'publisher_title_literal': title if isinstance(title, str) and len(title) <= 4096 else None,
        'title_company_prefix_literal': prefix, 'title_subject_literal': subject,
        'publisher_epoch': epoch if type(epoch) is int else None, 'publisher_day_taipei': day,
        'tentative_finlab_key': key}


def _known_key_results(rows):
    """Reuse the previous exact comparison only for an identical three-field key."""
    known = {}
    for row in rows:
        if row.get('match_state') not in {'no_local_match', 'ambiguous_local_records',
                                          'single_local_nonempty_info', 'single_local_empty_info'}:
            continue
        key = tuple(row.get(name) for name in ('symbol_candidate', 'publisher_day_taipei', 'literal_subject'))
        records = row.get('local_record_candidates')
        if not all(isinstance(value, str) and value for value in key) or not isinstance(records, list):
            raise ValueError('invalid_previous_exact_key')
        state = row['match_state']
        if ((state == 'no_local_match' and records)
                or (state == 'ambiguous_local_records' and len(records) < 2)
                or (state in {'single_local_nonempty_info', 'single_local_empty_info'} and
                    (len(records) != 1 or not isinstance(records[0], dict)
                     or records[0].get('nonempty_info') is not (state == 'single_local_nonempty_info')))):
            raise ValueError('previous_match_state_inconsistent')
        if key in known and known[key] != records:
            raise ValueError('conflicting_previous_exact_key_results')
        known[key] = records
    return known


def audit(news_root, finlab_root, registry, checked, previous):
    news_root, finlab_root = news_root.resolve(), finlab_root.resolve()
    source = registry.sources['cnyes']
    outcomes, rows, counts = checked.get('target_outcomes'), previous.get('rows'), previous.get('counts', {})
    if (checked.get('contract') != 'cnyes_retained_missing_crosscheck_v1'
            or checked.get('source_fingerprint') != cnyes_bulk_fingerprint(source)
            or checked.get('response_failures') or checked.get('unfinished_responses') != 0
            or not isinstance(outcomes, list) or len(outcomes) != checked.get('candidate_pairs')
            or previous.get('contract') != 'cnyes_finlab_issuer_overlap_v1' or previous.get('accepted') is not True
            or previous.get('source_fingerprint') != checked['source_fingerprint']
            or previous.get('source_failures') != [] or counts.get('source_envelopes_failed') != 0
            or counts.get('changed_head_pairs', 0) or counts.get('unverified_post_identities') != 0
            or not isinstance(rows, list) or len(rows) != counts.get('novel_post_identities')
            or len(rows) != counts.get('verified_post_identities')
            or Counter(r.get('match_state') for r in rows) != Counter(counts.get('match_states', {}))):
        raise ValueError('invalid_upstream_audit')
    target_rows = {(r['news_id'], r['source_post_sha256']): r for r in rows
                   if r.get('match_state') == 'no_exact_issuer_key'}
    if (len(target_rows) != counts.get('match_states', {}).get('no_exact_issuer_key', 0)
            or len(target_rows) > 20_000):
        raise ValueError('unkeyed_targets_invalid')
    finlab_path, receipt_path, receipt, finlab_before, receipt_before, finlab_sha = verified_finlab(finlab_root)
    if finlab_sha != previous.get('finlab_receipt_sha256'):
        raise ValueError('finlab_receipt_changed')

    sources, by_source, pair_sources, heads = {}, defaultdict(set), defaultdict(set), {}
    for item in outcomes:
        pair = (item.get('news_id'), item.get('source_post_sha256'))
        if pair not in target_rows or item.get('evidence_state') != 'verified' or item.get('outcome') != 'bulk_body_not_provided':
            continue
        sha, parquet_sha, observed = (item.get(name) for name in ('source_response_sha256', 'parquet_sha256', 'source_observed_at_utc'))
        if (type(pair[0]) is not int or not isinstance(pair[1], str) or not HASH.fullmatch(pair[1])
                or not isinstance(sha, str) or not HASH.fullmatch(sha)
                or not isinstance(parquet_sha, str) or not HASH.fullmatch(parquet_sha) or not isinstance(observed, str)):
            raise ValueError('invalid_target_source_identity')
        if target_rows[pair].get('source_observations', {}).get(sha) != observed:
            raise ValueError('target_observation_mismatch')
        source_evidence = {'parquet_sha256': parquet_sha, 'source_observed_at_utc': observed}
        if sha in sources and sources[sha] != source_evidence:
            raise ValueError('conflicting_source_identity')
        sources[sha] = source_evidence
        by_source[sha].add(pair)
        pair_sources[pair].add(sha)
        head = item.get('head')
        if not isinstance(head, dict) or (pair in heads and heads[pair] != head):
            raise ValueError('conflicting_head_snapshot')
        heads[pair] = head
    if set(heads) != set(target_rows) or any(
            set(row.get('source_observations', {})) != pair_sources[pair] for pair, row in target_rows.items()):
        raise ValueError('unkeyed_source_evidence_incomplete')

    db_path, wal_path = news_root / 'queue.sqlite3', news_root / 'queue.sqlite3-wal'
    db_before = signature(db_path)
    wal_before = signature(wal_path) if wal_path.exists() else None
    if wal_before is not None and wal_before[2]:
        raise ValueError('immutable_snapshot_requires_empty_wal')
    stale = set()
    with closing(sqlite3.connect(db_path.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        for pair, head in heads.items():
            row = db.execute('SELECT state,last_checked_at_utc,body_sha256,parquet_path FROM articles '
                'WHERE url=? AND provider=?', (PREFIX + str(pair[0]), 'cnyes')).fetchone()
            if (row is None or row['body_sha256'] is not None or row['parquet_path'] is not None
                    or row['state'] != head['state'] or row['last_checked_at_utc'] != head['last_checked_at_utc']):
                stale.add(pair)

    remaining = set(target_rows) - stale
    initial = select_sources(by_source, remaining)
    attempts, failures, posts = set(), [], {}
    while remaining:
        plan = select_sources({sha: pairs for sha, pairs in by_source.items() if sha not in attempts}, remaining)
        if not plan:
            break
        for sha in plan:
            wanted = by_source[sha] & remaining
            if not wanted:
                continue
            attempts.add(sha)
            try:
                found = _source_posts(news_root, registry, source, sha, sources[sha], wanted)
                posts.update({pair: (sha, post) for pair, post in found.items()})
                remaining.difference_update(found)
            except (OSError, ValueError, TypeError, KeyError, NewsError, pa.ArrowException) as error:
                failures.append({'response_sha256': sha, 'pairs': len(wanted),
                    'error': error.code if isinstance(error, NewsError) else type(error).__name__ + ':' + str(error)[:100]})

    details = {pair: candidate(post) for pair, (_, post) in posts.items()}
    if any(issuer_key(post) is not None for _, post in posts.values()):
        raise ValueError('upstream_unkeyed_result_no_longer_agrees')
    known = _known_key_results(rows)
    keys = {tuple(value['tentative_finlab_key']) for value in details.values() if value['tentative_finlab_key']}
    novel = keys - known.keys()
    local = match_local_records(finlab_path, novel) if novel else {}
    if signature(finlab_path) != finlab_before or signature(receipt_path) != receipt_before:
        raise ValueError('finlab_source_changed_during_audit')
    wal_after = signature(wal_path) if wal_path.exists() else None
    if signature(db_path) != db_before or wal_after != wal_before:
        raise ValueError('news_snapshot_changed_during_audit')
    output = []
    for pair, values in sorted(details.items()):
        key = values['tentative_finlab_key']
        records = (known.get(tuple(key), local.get(tuple(key), [])) if key else [])
        output.append({'news_id': pair[0], 'source_post_sha256': pair[1], 'selected_response_sha256': posts[pair][0],
            'source_observations': target_rows[pair]['source_observations'], **values,
            'key_result_reused': bool(key and tuple(key) in known), 'finlab_record_candidates': records,
            'issuer_identity_verified': False, 'official_publication_time_verified': False,
            'article_body_found': False})
    return {'contract': 'cnyes_unkeyed_tentative_issuer_v1', 'completed_at_utc': datetime.now(timezone.utc).isoformat(),
        'source_fingerprint': checked['source_fingerprint'], 'finlab_receipt_sha256': finlab_sha,
        'finlab_receipt_status': receipt['status'],
        'counts': {'unkeyed_targets': len(target_rows), 'changed_heads': len(stale),
            'available_source_envelopes': len(by_source), 'initial_source_cover': len(initial),
            'source_envelopes_attempted': len(attempts), 'source_envelopes_failed': len(failures),
            'unverified_post_identities': len(remaining), 'verified_post_identities': len(posts),
            'reason_counts': dict(Counter(row['reason'] for row in output)), 'tentative_keys': len(keys),
            'reused_keys': len(keys & known.keys()), 'new_keys_scanned': len(novel),
            'rows_with_finlab_candidates': sum(bool(row['finlab_record_candidates']) for row in output)},
        'source_failures': failures, 'changed_head_identities': sorted(stale), 'unverified_identities': sorted(remaining),
        'rows': output, 'accepted': not stale and not failures and not remaining,
        'publisher_requests': 0, 'queue_writes': 0, 'article_bodies_copied': False,
        'cross_source_identity_verified': False, 'all_history_complete': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--finlab-root', type=Path, default=Path('data_finlab'))
    parser.add_argument('--checked-audit', type=Path, required=True)
    parser.add_argument('--prior-audit', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    output = args.output.resolve()
    if (args.output.is_symlink() or output.exists()
            or not output.is_relative_to((repo / 'artifacts/data_quality').resolve())):
        parser.error('output must be a new file under artifacts/data_quality')
    saved, values = [], []
    for path in (args.checked_audit, args.prior_audit):
        before = signature(path)
        if before[2] > 128 << 20:
            parser.error('input audit oversized')
        data = path.read_bytes()
        saved.append((path, before, hashlib.sha256(data).hexdigest()))
        values.append(json.loads(data))
    prior_hashes = values[1].get('input_audit_sha256')
    if not isinstance(prior_hashes, list) or len(prior_hashes) != 2 or prior_hashes[0] != saved[0][2]:
        raise ValueError('upstream_checked_audit_identity_mismatch')
    result = audit((repo / config['root']), args.finlab_root, registry, *values)
    if any(signature(path) != before or sha256_file(path) != digest for path, before, digest in saved):
        raise ValueError('input_changed_during_audit')
    result['input_audit_sha256'] = [entry[2] for entry in saved]
    result['implementation_sha256'] = {str(path): sha256_file(repo / path) for path in (
        Path('scripts/audit_news_cnyes_unkeyed.py'), Path('scripts/audit_news_cnyes_issuer_overlap.py'),
        Path('scripts/audit_news_issuer_overlap.py'), Path('downloader/news_fulltext_cnyes_history.py'))}
    atomic_write_bytes(output, (json.dumps(result, ensure_ascii=False, indent=2) + '\n').encode(), durable=True, mode=0o600)
    print(json.dumps({'accepted': result['accepted'], 'counts': result['counts']}, ensure_ascii=False))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
