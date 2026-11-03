"""Find new exact CNYES/FinLab issuer metadata matches, without downloading bodies."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import heapq
import json
from pathlib import Path
import re
import sqlite3
import sys
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import wordpress_base
from downloader.news_fulltext_cnyes_history import _read_accepted_batch_original, digest as post_digest
from downloader.news_fulltext_policy import NewsError, cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import source_path
from downloader.parquet_integrity import signature
from scripts.audit_news_cached_replay import _check_history_envelope
from scripts.audit_news_issuer_overlap import match_local_records, verified_finlab


PREFIX = 'https://news.cnyes.com/news/id/'
HASH = re.compile(r'[0-9a-f]{64}')
TAIPEI = ZoneInfo('Asia/Taipei')
# The first completed run counted non-keyed posts but did not emit their IDs.
# Only this exact old implementation has the proven omission semantics below.
LEGACY_UNKEYED_REPORT_CODE_SHA256 = '972fb2d797e4985ddccfdf3e82e5a7b63930b9f5c9625cffed1500de9fe76896'


def issuer_key(post):
    """Use only the publisher's literal security name and exact listed code."""
    title, stocks, markets, epoch = (post.get(key) for key in ('title', 'stock', 'market', 'publishAt'))
    if (post.get('source') != '台灣證券交易所' or not isinstance(title, str)
            or not isinstance(stocks, list) or len(stocks) != 1 or not isinstance(stocks[0], str)
            or not isinstance(markets, list) or type(epoch) is not int):
        return None
    names = {item.get('name') for item in markets if isinstance(item, dict)
             and item.get('code') == stocks[0] and isinstance(item.get('name'), str) and item['name']}
    if len(names) != 1:
        return None
    prefix = next(iter(names)) + ':'
    if not title.startswith(prefix) or not title[len(prefix):]:
        return None
    try:
        day = datetime.fromtimestamp(epoch, TAIPEI).date().isoformat()
    except (ValueError, OverflowError, OSError):
        return None
    return stocks[0], day, title[len(prefix):]


def previously_compared(prior, finlab_receipt_sha):
    """Skip the identical post only if the same FinLab version was compared."""
    if prior.get('finlab_receipt_sha256') != finlab_receipt_sha:
        return set()
    result = set()
    for row in prior.get('rows', []):
        observations = [x for x in row.get('observations', []) if x.get('evidence_state') == 'verified']
        keys = {tuple(x['candidate_finlab_key']) for x in observations if x.get('candidate_finlab_key')}
        state = row.get('issuer_match_state')
        if not (not keys and state == 'no_unambiguous_issuer_key' or len(keys) == 1 and state in {
                'no_local_match', 'ambiguous_local_records', 'single_local_nonempty_info', 'single_local_empty_info'}):
            continue
        for item in observations:
            digest = item.get('source_post_sha256')
            if type(row.get('news_id')) is int and isinstance(digest, str) and HASH.fullmatch(digest):
                result.add((row['news_id'], digest))
    return result


def select_sources(by_source, uncovered):
    """Greedy set cover over exact post hashes; each chosen envelope is read once."""
    reverse = defaultdict(list)
    counts = {}
    for source, pairs in by_source.items():
        relevant = pairs & uncovered
        if relevant:
            counts[source] = len(relevant)
            for pair in relevant:
                reverse[pair].append(source)
    queue = [(-count, source) for source, count in counts.items()]
    heapq.heapify(queue)
    selected, covered = [], set()
    while queue:
        negative, source = heapq.heappop(queue)
        if -negative != counts[source] or not counts[source]:
            continue
        selected.append(source)
        for pair in (by_source[source] & uncovered) - covered:
            covered.add(pair)
            for alternate in reverse[pair]:
                counts[alternate] -= 1
                if counts[alternate]:
                    heapq.heappush(queue, (-counts[alternate], alternate))
    return selected


def _source_posts(root, registry, source, sha, source_evidence, pairs):
    relative = source_evidence.get('artifact_path', 'quarantine/' + sha + '.parquet')
    kind = source_evidence.get('artifact_kind', 'canonical_quarantine')
    receipt_relative = source_evidence.get('receipt_path', relative + '.receipt.json')
    if (kind not in {'canonical_quarantine', 'cnyes_history_source_envelope', 'accepted_body_batch'}
            or kind == 'canonical_quarantine' and relative != 'quarantine/' + sha + '.parquet'):
        raise ValueError('unsupported_cnyes_source_envelope')
    if kind != 'accepted_body_batch' and receipt_relative != relative + '.receipt.json':
        raise ValueError('unsupported_cnyes_source_receipt')
    direct, direct_receipt = root / relative, root / receipt_relative
    if direct.is_symlink() or direct_receipt.is_symlink():
        raise ValueError('unsafe_source_symlink')
    path, receipt_path = source_path(root, relative), source_path(root, receipt_relative)
    before, receipt_before = signature(path), signature(receipt_path)
    if kind != 'accepted_body_batch' and before[2] > 16 << 20 or receipt_before[2] > 65536:
        raise ValueError('source_envelope_oversized')
    if kind == 'accepted_body_batch':
        request_sha = source_evidence.get('source_request_sha256')
        if not isinstance(request_sha, str) or not HASH.fullmatch(request_sha):
            raise ValueError('invalid_accepted_batch_request')
        db_path = root / 'queue.sqlite3'
        with closing(sqlite3.connect(db_path.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            loaded = _read_accepted_batch_original(SimpleNamespace(root=root, db=db),
                {'parquet_path': relative, 'receipt_path': receipt_relative}, source, None, sha,
                request_sha256=request_sha)
        if loaded is None or loaded['receipt']['sha256'] != source_evidence['parquet_sha256']:
            raise ValueError('source_accepted_batch_invalid')
        record, receipt = loaded['original'], loaded['receipt']
    else:
        receipt = json.loads(receipt_path.read_bytes())
        if (receipt.get('parquet_path') != relative or receipt.get('publish') is not False
                or receipt.get('response_sha256') != sha or receipt.get('sha256') != source_evidence['parquet_sha256']
                or sha256_file(path) != receipt.get('sha256')):
            raise ValueError('source_receipt_mismatch')
        parquet = pq.ParquetFile(path)
        if (parquet.metadata.num_rows != 1 or parquet.metadata.num_row_groups != 1
                or parquet.metadata.row_group(0).total_byte_size > 20 << 20):
            raise ValueError('source_envelope_grain_or_size_invalid')
        record, = parquet.read(use_threads=False).to_pylist()
    raw = record['raw_response_body']
    if (not isinstance(raw, bytes) or len(raw) > 16 << 20 or hashlib.sha256(raw).hexdigest() != sha
            or record['response_sha256'] != sha or record['publish'] is not False
            or record['captured_at_utc'] != source_evidence['source_observed_at_utc']
            or record['authorization_fingerprint'] != source.authorization_fingerprint
            or kind != 'accepted_body_batch' and json.loads(record['authorization_json']) != source.authorization
            or registry.require(record['request_url']).provider != 'cnyes'
            or wordpress_base(record['request_url']) not in source.cnyes_apis
            or source_evidence.get('source_request_sha256', hashlib.sha256(record['request_url'].encode()).hexdigest())
                != hashlib.sha256(record['request_url'].encode()).hexdigest()):
        raise ValueError('source_response_identity_or_authorization_mismatch')
    if kind == 'cnyes_history_source_envelope':
        _check_history_envelope(relative, record, receipt, sha)
    posts = json.loads(raw)['items']['data']
    if (not isinstance(posts, list) or len(posts) > 60
            or kind == 'canonical_quarantine' and len(posts) != receipt['article_count']
            or any(not isinstance(post, dict) or type(post.get('payment')) is not int or post['payment'] != 0 for post in posts)):
        raise ValueError('source_posts_invalid')
    wanted = set(pairs)
    wanted_ids = {item[0] for item in wanted}
    found = {}
    for post in posts:
        news_id = post.get('newsId')
        if type(news_id) is not int or news_id not in wanted_ids:
            continue
        key = (news_id, post_digest(post))
        if key in wanted:
            found[key] = post
    if set(found) != wanted:
        raise ValueError('source_post_identity_mismatch')
    if signature(path) != before or signature(receipt_path) != receipt_before:
        raise ValueError('source_changed_during_read')
    return found


def audit(news_root, finlab_root, registry, checked, prior, *, batch_rows=8192):
    """Use existing proofs only as evidence of identical source posts, never as article bodies."""
    started = time.monotonic()
    news_root, finlab_root = news_root.resolve(), finlab_root.resolve()
    source = registry.sources['cnyes']
    outcomes = checked.get('target_outcomes')
    contract = checked.get('contract')
    if (contract not in {'cnyes_retained_missing_crosscheck_v1', 'cnyes_retained_missing_crosscheck_v2',
                         'cnyes_retained_missing_crosscheck_v3', 'cnyes_retained_missing_crosscheck_v4'}
            or checked.get('source_fingerprint') != cnyes_bulk_fingerprint(source)
            or checked.get('response_failures') or checked.get('unfinished_responses') != 0
            or not isinstance(outcomes, list) or len(outcomes) != checked.get('candidate_pairs')
            or contract in {'cnyes_retained_missing_crosscheck_v2', 'cnyes_retained_missing_crosscheck_v3',
                            'cnyes_retained_missing_crosscheck_v4'} and (
                checked.get('unresolved_matching_observations') != 0
                or any(item.get('original_request_identity_checked') is not True for item in outcomes))
            or contract in {'cnyes_retained_missing_crosscheck_v3', 'cnyes_retained_missing_crosscheck_v4'}
                and checked.get('unfinished_source_envelopes') != 0):
        raise ValueError('invalid_cnyes_proof_scope')
    finlab_path, finlab_receipt_path, receipt, finlab_before, receipt_before, finlab_sha = verified_finlab(finlab_root)
    prior_pairs = previously_compared(prior, finlab_sha)
    db_path, wal_path = news_root / 'queue.sqlite3', news_root / 'queue.sqlite3-wal'
    db_before = signature(db_path)
    wal_before = signature(wal_path) if wal_path.exists() else None
    if wal_before is not None and wal_before[2]:
        raise ValueError('immutable_snapshot_requires_empty_wal')
    targets, sources, by_source = {}, {}, defaultdict(set)
    counts = Counter()
    with closing(sqlite3.connect(db_path.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        heads = {}
        for item in outcomes:
            if item.get('evidence_state') != 'verified' or item.get('outcome') != 'bulk_body_not_provided':
                continue
            news_id, post_sha, sha = item.get('news_id'), item.get('source_post_sha256'), item.get('source_response_sha256')
            if (type(news_id) is not int or news_id <= 0 or not isinstance(post_sha, str) or not HASH.fullmatch(post_sha)
                    or not isinstance(sha, str) or not HASH.fullmatch(sha)):
                raise ValueError('invalid_cnyes_target_identity')
            pair = (news_id, post_sha)
            if pair in prior_pairs:
                counts['previously_compared_pairs'] += 1
                continue
            if news_id not in heads:
                row = db.execute('SELECT state,last_checked_at_utc,body_sha256,parquet_path FROM articles '
                    'WHERE url=? AND provider=?', (PREFIX + str(news_id), 'cnyes')).fetchone()
                heads[news_id] = dict(row) if row else None
            head = heads[news_id]
            if (head is None or head['body_sha256'] is not None or head['parquet_path'] is not None
                    or head['state'] != item['head']['state']
                    or head['last_checked_at_utc'] != item['head']['last_checked_at_utc']):
                counts['changed_head_pairs'] += 1
                continue
            evidence = {'parquet_sha256': item.get('parquet_sha256'),
                        'source_observed_at_utc': item.get('source_observed_at_utc')}
            if (not isinstance(evidence['parquet_sha256'], str) or not HASH.fullmatch(evidence['parquet_sha256'])
                    or not isinstance(evidence['source_observed_at_utc'], str)):
                raise ValueError('invalid_cnyes_source_evidence')
            if contract in {'cnyes_retained_missing_crosscheck_v3', 'cnyes_retained_missing_crosscheck_v4'}:
                relative, kind, request_sha = (item.get('source_artifact_path'), item.get('source_artifact_kind'),
                                               item.get('source_request_sha256'))
                valid_kinds = {'canonical_quarantine', 'cnyes_history_source_envelope'}
                if contract == 'cnyes_retained_missing_crosscheck_v4':
                    valid_kinds.add('accepted_body_batch')
                if (not isinstance(relative, str) or kind not in valid_kinds
                        or not isinstance(request_sha, str) or not HASH.fullmatch(request_sha)):
                    raise ValueError('invalid_cnyes_source_envelope_evidence')
                evidence.update(artifact_path=relative, artifact_kind=kind, source_request_sha256=request_sha)
                if contract == 'cnyes_retained_missing_crosscheck_v4':
                    receipt_relative = item.get('source_receipt_path')
                    if not isinstance(receipt_relative, str):
                        raise ValueError('invalid_cnyes_source_envelope_evidence')
                    evidence['receipt_path'] = receipt_relative
                    source_key = (sha, relative, kind, receipt_relative, request_sha)
                else:
                    source_key = (sha, relative, kind)
            else:
                source_key = sha
            if source_key in sources and sources[source_key] != evidence:
                raise ValueError('conflicting_cnyes_response_evidence')
            sources[source_key] = evidence
            target = targets.setdefault(pair, {'head': head, 'observations': {}})
            target['observations'][source_key] = evidence['source_observed_at_utc']
            by_source[source_key].add(pair)
    if len(targets) > 20_000 or len(sources) > 20_000:
        raise ValueError('candidate_bound_exceeded')
    remaining, attempts, failures, selected_posts = set(targets), set(), [], {}
    initial = select_sources(by_source, remaining)
    while remaining:
        available = {sha: pairs for sha, pairs in by_source.items() if sha not in attempts}
        plan = select_sources(available, remaining)
        if not plan:
            break
        for source_key in plan:
            wanted = by_source[source_key] & remaining
            if not wanted:
                continue
            attempts.add(source_key)
            sha = source_key[0] if isinstance(source_key, tuple) else source_key
            try:
                posts = _source_posts(news_root, registry, source, sha, sources[source_key], wanted)
                for pair, post in posts.items():
                    selected_posts[pair] = (source_key, post)
                remaining.difference_update(posts)
            except (OSError, ValueError, TypeError, KeyError, sqlite3.Error, NewsError, pa.ArrowException) as error:
                failures.append({'response_sha256': sha, 'source_artifact_path': sources[source_key].get('artifact_path'),
                                 'pairs': len(wanted),
                                 'error': error.code if isinstance(error, NewsError) else type(error).__name__ + ':' + str(error)[:100]})
            if len(attempts) % 100 == 0:
                print(json.dumps({'source_envelopes_attempted': len(attempts),
                                  'post_identities_remaining': len(remaining)}), flush=True)
    candidates = {pair: issuer_key(post) for pair, (_, post) in selected_posts.items()}
    keys = {key for key in candidates.values() if key is not None}
    local = match_local_records(finlab_path, keys, batch_rows=batch_rows)
    if signature(finlab_path) != finlab_before or signature(finlab_receipt_path) != receipt_before:
        raise ValueError('finlab_source_changed_during_audit')
    wal_after = signature(wal_path) if wal_path.exists() else None
    if signature(db_path) != db_before or wal_after != wal_before:
        raise ValueError('news_snapshot_changed_during_audit')
    rows, states = [], Counter()
    for pair, key in sorted(candidates.items()):
        source_key, post = selected_posts[pair]
        sha = source_key[0] if isinstance(source_key, tuple) else source_key
        observations = targets[pair]['observations']
        if contract in {'cnyes_retained_missing_crosscheck_v3', 'cnyes_retained_missing_crosscheck_v4'}:
            observations = [{'source_response_sha256': key[0], 'source_artifact_path': key[1],
                             'source_artifact_kind': key[2], 'source_observed_at_utc': clock,
                             **({'source_receipt_path': key[3], 'source_request_sha256': key[4]}
                                if contract == 'cnyes_retained_missing_crosscheck_v4' else {})}
                            for key, clock in sorted(observations.items())]
        row = {'news_id': pair[0], 'source_post_sha256': pair[1], 'selected_response_sha256': sha,
               'source_observations': observations, 'source_publish_epoch': post.get('publishAt')}
        if contract in {'cnyes_retained_missing_crosscheck_v3', 'cnyes_retained_missing_crosscheck_v4'}:
            row['selected_source_artifact_path'] = source_key[1]
        if contract == 'cnyes_retained_missing_crosscheck_v4':
            row['selected_source_receipt_path'] = source_key[3]
            row['selected_source_request_sha256'] = source_key[4]
        if key is None:
            states['no_exact_issuer_key'] += 1
            rows.append({**row, 'symbol_candidate': None, 'publisher_day_taipei': None,
                         'literal_subject': None, 'match_state': 'no_exact_issuer_key', 'local_record_candidates': []})
            continue
        records = local.get(key, [])
        state = ('no_local_match' if not records else 'ambiguous_local_records' if len(records) > 1 else
                 'single_local_nonempty_info' if records[0]['nonempty_info'] else 'single_local_empty_info')
        states[state] += 1
        rows.append({**row, 'symbol_candidate': key[0],
            'publisher_day_taipei': key[1], 'literal_subject': key[2],
            'match_state': state, 'local_record_candidates': records})
    return {'contract': ('cnyes_finlab_issuer_overlap_v3' if contract == 'cnyes_retained_missing_crosscheck_v4'
                         else 'cnyes_finlab_issuer_overlap_v2' if contract == 'cnyes_retained_missing_crosscheck_v3'
                         else 'cnyes_finlab_issuer_overlap_v1'),
        'completed_at_utc': datetime.now(timezone.utc).isoformat(),
        'elapsed_seconds': round(time.monotonic() - started, 3), 'source_fingerprint': checked['source_fingerprint'],
        'finlab_receipt_sha256': finlab_sha, 'finlab_receipt_path': str(finlab_receipt_path),
        'finlab_receipt_status': receipt['status'], 'finlab_prior_version_identical': prior.get('finlab_receipt_sha256') == finlab_sha,
        'counts': {**counts, 'novel_post_identities': len(targets), 'novel_heads': len({pair[0] for pair in targets}),
            'available_source_envelopes': len(by_source), 'initial_source_cover': len(initial),
            'source_envelopes_attempted': len(attempts), 'source_envelopes_failed': len(failures),
            'unverified_post_identities': len(remaining), 'verified_post_identities': len(selected_posts),
            'distinct_candidate_keys': len(keys), 'match_states': dict(states)},
        'source_failures': failures, 'rows': rows, 'publisher_requests': 0, 'queue_writes': 0,
        'source_body_text_exported': False, 'body_equivalence_verified': False,
        'publication_clock_verified_by_match': False, 'news_training_injection': False,
        'all_history_complete': False, 'accepted': not failures and not remaining,
        'comparison_basis': 'same stock code, publisher Taipei day and literal issuer subject; independent sources',
        'source_envelope_count_semantics': 'distinct artifact and exact request for v4 proofs'}


def complete_legacy_unkeyed_rows(checked, prior, completed, input_hashes):
    """Recover the omitted IDs by set difference; never rerun a completed source."""
    outcomes = checked.get('target_outcomes')
    if (completed.get('contract') != 'cnyes_finlab_issuer_overlap_v1' or completed.get('accepted') is not True
            or completed.get('input_audit_sha256') != input_hashes
            or completed.get('source_fingerprint') != checked.get('source_fingerprint')
            or completed.get('implementation_sha256', {}).get('scripts/audit_news_cnyes_issuer_overlap.py')
                != LEGACY_UNKEYED_REPORT_CODE_SHA256
            or checked.get('contract') != 'cnyes_retained_missing_crosscheck_v1'
            or checked.get('response_failures') or checked.get('unfinished_responses') != 0
            or not isinstance(outcomes, list) or len(outcomes) != checked.get('candidate_pairs')):
        raise ValueError('legacy_issuer_report_identity_mismatch')
    counts = completed.get('counts', {})
    if (counts.get('changed_head_pairs', 0) or counts.get('source_envelopes_failed') != 0
            or counts.get('unverified_post_identities') != 0
            or counts.get('verified_post_identities') != counts.get('novel_post_identities')
            or completed.get('source_failures') != []):
        raise ValueError('legacy_issuer_report_scope_incomplete')
    previous = previously_compared(prior, completed['finlab_receipt_sha256'])
    pairs = defaultdict(dict)
    for item in outcomes:
        if item.get('evidence_state') == 'verified' and item.get('outcome') == 'bulk_body_not_provided':
            pair = (item['news_id'], item['source_post_sha256'])
            if pair not in previous:
                pairs[pair][item['source_response_sha256']] = item['source_observed_at_utc']
    if (len(pairs) != counts['novel_post_identities']
            or len({pair[0] for pair in pairs}) != counts.get('novel_heads')):
        raise ValueError('legacy_issuer_post_coverage_mismatch')
    rows = completed.get('rows')
    if not isinstance(rows, list):
        raise ValueError('legacy_issuer_rows_missing')
    listed = {(row['news_id'], row['source_post_sha256']) for row in rows}
    if len(listed) != len(rows) or not listed <= pairs.keys():
        raise ValueError('legacy_issuer_rows_invalid')
    match_states = counts.get('match_states')
    if (not isinstance(match_states, dict)
            or any(type(value) is not int or value < 0 for value in match_states.values())
            or sum(match_states.values()) != len(pairs)
            or Counter(row['match_state'] for row in rows) !=
                Counter({state: count for state, count in match_states.items() if state != 'no_exact_issuer_key'})
            or any(row['selected_response_sha256'] not in pairs[(row['news_id'], row['source_post_sha256'])]
                   or row['source_observations'] != pairs[(row['news_id'], row['source_post_sha256'])]
                   for row in rows)):
        raise ValueError('legacy_issuer_rows_or_counts_inconsistent')
    missing = set(pairs) - listed
    if len(missing) != counts.get('match_states', {}).get('no_exact_issuer_key', 0):
        raise ValueError('legacy_issuer_unkeyed_count_mismatch')
    additions = [{'news_id': news_id, 'source_post_sha256': post_sha,
        'selected_response_sha256': None, 'source_observations': pairs[(news_id, post_sha)],
        'source_publish_epoch': None, 'symbol_candidate': None, 'publisher_day_taipei': None,
        'literal_subject': None, 'match_state': 'no_exact_issuer_key', 'local_record_candidates': [],
        'provenance': 'derived_from_complete_legacy_coverage_and_keyed_rows'}
        for news_id, post_sha in sorted(missing)]
    return {**completed, 'rows': sorted(rows + additions, key=lambda row: (row['news_id'], row['source_post_sha256'])),
        'derived_unkeyed_rows': len(additions), 'derivation': 'set complement; individual selected envelope and publish epoch unknown',
        'derived_at_utc': datetime.now(timezone.utc).isoformat(), 'current_queue_state_rechecked': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--finlab-root', type=Path, default=Path('data_finlab'))
    parser.add_argument('--checked-audit', type=Path, required=True)
    parser.add_argument('--prior-audit', type=Path, required=True)
    parser.add_argument('--augment-completed-run', type=Path,
                        help='Fill the first run\'s omitted unkeyed IDs from its complete coverage evidence')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    news_root, finlab_root = (repo / config['root']).resolve(), args.finlab_root.resolve()
    output = args.output.resolve()
    if (args.output.is_symlink() or output.exists()
            or any(output.is_relative_to(root) for root in (news_root, finlab_root))):
        parser.error('output must be new and outside both source roots')
    inputs = [args.checked_audit, args.prior_audit]
    if args.augment_completed_run:
        inputs.append(args.augment_completed_run)
    saved, values = [], []
    for path in inputs:
        before = signature(path)
        if before[2] > 128 << 20:
            parser.error('input audit oversized')
        content = path.read_bytes()
        values.append(json.loads(content))
        saved.append((path, before, hashlib.sha256(content).hexdigest()))
    if args.augment_completed_run:
        result = complete_legacy_unkeyed_rows(values[0], values[1], values[2], [entry[2] for entry in saved[:2]])
        result['derived_from_report_sha256'] = saved[2][2]
    else:
        result = audit(news_root, finlab_root, registry, *values)
    if any(signature(path) != before or sha256_file(path) != digest for path, before, digest in saved):
        raise ValueError('input_audit_changed_during_read')
    result['input_audit_sha256'] = [digest for _, _, digest in saved[:2]]
    result['derivation_implementation_sha256' if args.augment_completed_run else 'implementation_sha256'] = {
        str(path): sha256_file(repo / path) for path in (
        Path('scripts/audit_news_cnyes_issuer_overlap.py'), Path('scripts/audit_news_issuer_overlap.py'),
        Path('downloader/news_fulltext_cnyes_history.py'))}
    atomic_write_bytes(output, (json.dumps(result, ensure_ascii=False, indent=2) + '\n').encode(), durable=True, mode=0o600)
    print(json.dumps({key: result[key] for key in ('accepted', 'elapsed_seconds', 'counts')}, ensure_ascii=False))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
