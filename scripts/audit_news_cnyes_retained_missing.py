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
from downloader.news_fulltext_cnyes_history import _checked_page_observation
from downloader.news_fulltext_policy import NewsError, cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import source_path
from scripts.audit_news_cached_replay import _source_proof


STATES = ('source_article_empty_template', 'source_article_test_only', 'source_body_image_only',
          'source_body_quote_only', 'source_not_found', 'body_not_found')
PREFIX = 'https://news.cnyes.com/news/id/'

# Explicitly reviewed v3 -> v4 bridge: the older proof checked the same
# request, original post and files for these two single-response formats.
# Any change to the reviewed reader disables reuse until reviewed again.
_V3_REVIEWED_PARSER = {
    'scripts/audit_news_cnyes_retained_missing.py': 'e6bc78e0dd935b0bc17132d2f07ffa05cd80ca2697bb23b1d47a52d58182467d',
    'scripts/audit_news_cached_replay.py': '7695948d0e8c58e7a5903e84a6fdbc4c2c5c918f4d61846e98aa3aeff82e9c6f',
    'downloader/news_fulltext_bulk.py': '62c0b83cf936819e71f5bae731fbde808f81c75bf1f5bf855fc82485dfae0da8',
    'downloader/news_fulltext_cnyes_history.py': 'bd76234deb7e9d79330d6203c33b9d8ae2092ed60bcc1dc8ef5dc55a13665e4e',
    'downloader/news_fulltext_policy.py': 'd4ee857f04aec54c3e17299a3ddd7cde0d492e11d8f65ccc6c4dbafcd62f26f1',
}
_V3_REVIEWED_CURRENT = {
    'scripts/audit_news_cached_replay.py': '23792105265d5543a2a18dc4eafc142755cba2f29b110b450519274c57b37124',
    'downloader/news_fulltext_bulk.py': '62c0b83cf936819e71f5bae731fbde808f81c75bf1f5bf855fc82485dfae0da8',
    # Reviewed local scheduler and checkpoint changes: current observations are
    # independently rechecked before a prior original proof can be reused.
    # Reuse only content proofs, not audit state or completeness claims.
    # Original-audit cursor isolation, frontier and retry selection do not
    # change the per-observation or original-envelope content verifier.
    # Direct-incident admission, local work sequencing and damaged scope
    # marker isolation do not change the observation decoder or content
    # verifier used by this reader.
    # Archiving malformed markers before a head replacement also changes
    # scheduling and evidence retention, not either content reader.
    # The equal-day recovery and bounded original cache change owner behavior;
    # this reader still decodes each observation and original independently.
    # Alias-cycle reconstruction and per-slice retry bookkeeping also leave
    # the independently used observation and original-content readers intact.
    # Linking recovery head versions to archived events changes status
    # accounting and writer bookkeeping, not either independent reader.
    # The later batch-candidate wake and priority admission ordering change
    # scheduling only; their source-proof dependencies were compared against
    # the retained reviewed e840 reader.
    'downloader/news_fulltext_cnyes_history.py': 'e44eb5b0675d5f1a576e01c537a574984247176eb736554c9240926b2a09cda4',
    # The reviewed CMNews-only clock label cannot change the CNYES provider
    # guard, fingerprint or original-content reader; see the offline AST and
    # registry comparison in cnyes_cmnews_policy_bridge_20261030.json.
    'downloader/news_fulltext_policy.py': '3ed62b93b576ab03d33c57bf888e719dd1ac94a8ef81c1c146d705238c0bb17c',
}
# Exact v4 proof recorded before the history writer's receipt-binding fix.
# Its reader and all other source parsers are unchanged by that fix.
_V4_REVIEWED_PARSER = {
    **_V3_REVIEWED_CURRENT,
    'downloader/news_fulltext_policy.py': _V3_REVIEWED_PARSER['downloader/news_fulltext_policy.py'],
    'scripts/audit_news_cnyes_retained_missing.py': '2a70776d3ac32383a9fc3263bc874f38f99a024e0995b7600efd0c65a0928126',
    'downloader/news_fulltext_cnyes_history.py': 'efa5c5d4aa9227b3df865ba2fde3a404ae7ff8e9b555b60c1663f1c5d044bfff',
}
# The previous reader only changed selection and independently verified content
# proof reuse. Chunked scans do not change the proof of an individual original.
_V4_REVIEWED_RECENT_PARSER = {
    **_V3_REVIEWED_CURRENT,
    'downloader/news_fulltext_policy.py': _V3_REVIEWED_PARSER['downloader/news_fulltext_policy.py'],
    'scripts/audit_news_cnyes_retained_missing.py': '8cd5d759f65e40027c04f09273c413ebc2929d30e81a58e5fc0642a1924262ce',
    'downloader/news_fulltext_cnyes_history.py': 'c4746be93374616e0275da94fed1151488256797d7644ca807b84e988adb2ce9',
}
# The prior reviewed reader before priority admission ordering changed. Its
# original-content and current-observation dependencies were compared again.
_V4_REVIEWED_PRE_PRIORITY_PARSER = {
    **_V3_REVIEWED_CURRENT,
    'downloader/news_fulltext_policy.py': _V3_REVIEWED_PARSER['downloader/news_fulltext_policy.py'],
    'scripts/audit_news_cnyes_retained_missing.py': 'c046261b751da208bd0c67d3feb2e28252900004236ed4f16582daea54f78379',
    'downloader/news_fulltext_cnyes_history.py': '0d41bf62ae44dcf24c01a675a5be30dd70eff4f29208ffffc22bac17c0fe8997',
}
_V3_COMPAT_SELF_SHA256 = '1bab822ea0672e72a0abcfa2e2675a825fa5f7a1aa4b923962e16fe1806e21e1'

_SCAN_COLUMNS = ('scope_url', 'page', 'response_sha256', 'evidence_contract_version',
                 'source_bulk_fingerprint', 'authorization_fingerprint')
_CHUNK_REPORT_CONTRACT = 'cnyes_retained_missing_crosscheck_chunk_v1'
_SCAN_CONTRACT = 'cnyes_retained_observation_scan_chunk_v1'
_PARSER_PATHS = ('scripts/audit_news_cnyes_retained_missing.py', 'scripts/audit_news_cached_replay.py',
                 'downloader/news_fulltext_bulk.py', 'downloader/news_fulltext_cnyes_history.py',
                 'downloader/news_fulltext_policy.py')


def _reviewed_current_reader(parser_identity):
    if not isinstance(parser_identity, dict) or any(
            parser_identity.get(path) != expected for path, expected in _V3_REVIEWED_CURRENT.items()):
        return False
    # Hash this exact reader as well, without recursively hashing the digest.
    content = Path(__file__).read_bytes()
    marker = ("_V3_COMPAT_SELF_SHA256 = '" + _V3_COMPAT_SELF_SHA256 + "'").encode()
    if content.count(marker) != 1:
        return False
    normalized = content.replace(marker, b"_V3_COMPAT_SELF_SHA256 = '<reviewed>'", 1)
    return hashlib.sha256(normalized).hexdigest() == _V3_COMPAT_SELF_SHA256


def _reviewed_v3_compatible(prior, parser_identity):
    return prior.get('parser_identity') == _V3_REVIEWED_PARSER and _reviewed_current_reader(parser_identity)


def _reviewed_v4_compatible(prior, parser_identity):
    return (prior.get('parser_identity') in (_V4_REVIEWED_PARSER, _V4_REVIEWED_RECENT_PARSER,
                                              _V4_REVIEWED_PRE_PRIORITY_PARSER)
            and _reviewed_current_reader(parser_identity))


def _stamp(path):
    try:
        info = path.stat()
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    except FileNotFoundError:
        return None


def _json_digest(value):
    digest = hashlib.sha256()
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    parts, size = [], 0
    for part in encoder.iterencode(value):
        parts.append(part)
        size += len(part)
        if size >= 65536:
            digest.update(''.join(parts).encode())
            parts, size = [], 0
    if parts:
        digest.update(''.join(parts).encode())
    return digest.hexdigest()


def _scan_key_valid(key):
    return (isinstance(key, list) and len(key) == 6
            and all(isinstance(key[i], str) and len(key[i]) <= 65536 for i in (0, 2, 4, 5))
            and all(type(key[i]) is int for i in (1, 3)))


def _candidate_digest(ordered):
    """Bind retry offsets to exact request, target and observation identities."""
    digest = hashlib.sha256()
    for key, page in ordered:
        requests = []
        for url, request in sorted(page['requests'].items()):
            targets = [[news_id, sorted(hashes), sorted(request['observations'][news_id])]
                       for news_id, hashes in sorted(request['targets'].items())]
            requests.append([url, targets])
        digest.update(json.dumps([key, requests], ensure_ascii=False, separators=(',', ':')).encode())
        digest.update(b'\n')
    return digest.hexdigest()


def _resume_scan(report, *, root, snapshot, fingerprint, parser_identity, prior_digest, chunk_size):
    if not isinstance(report, dict) or report.get('contract') != _CHUNK_REPORT_CONTRACT:
        raise ValueError('scan_checkpoint_invalid')
    scan = report.get('observation_scan')
    if not isinstance(scan, dict) or scan.get('contract') != _SCAN_CONTRACT:
        raise ValueError('scan_checkpoint_invalid')
    saved = scan.get('checkpoint_sha256')
    if not isinstance(saved, str) or saved != _json_digest({k: v for k, v in scan.items() if k != 'checkpoint_sha256'}):
        raise ValueError('scan_checkpoint_invalid')
    if scan.get('snapshot') != snapshot:
        raise ValueError('scan_snapshot_changed')
    if (report.get('source_fingerprint') != fingerprint or report.get('parser_identity') != parser_identity
            or scan.get('source_fingerprint') != fingerprint or scan.get('parser_identity') != parser_identity
            or scan.get('source_root') != str(root)):
        raise ValueError('scan_scope_changed')
    if scan.get('prior_digest') != prior_digest:
        raise ValueError('scan_prior_changed')
    action, offset, count = scan.get('resume_action'), scan.get('candidate_offset'), scan.get('candidate_count')
    observed, last, old_chunk_size = scan.get('observations_scanned'), scan.get('last_scanned_key'), scan.get('chunk_size')
    if (type(offset) is not int or type(count) is not int or not 0 <= offset <= count
            or type(old_chunk_size) is not int or not 1 <= old_chunk_size <= 100_000
            or type(observed) is not int or not 0 <= observed <= old_chunk_size or count > observed
            or type(scan.get('has_more')) is not bool
            or scan.get('start_after') is not None and not _scan_key_valid(scan['start_after'])
            or last is not None and not _scan_key_valid(last)
            or (last is None) != (observed == 0)
            or scan['has_more'] and observed != old_chunk_size
            or last is not None and scan.get('start_after') is not None and tuple(last) <= tuple(scan['start_after'])
            or not isinstance(scan.get('candidate_digest'), str)
            or not re.fullmatch(r'[0-9a-f]{64}', scan['candidate_digest'])
            or type(scan.get('chunk_response_failures')) is not int
            or type(scan.get('chunk_request_mismatches')) is not int
            or min(scan['chunk_response_failures'], scan['chunk_request_mismatches']) < 0
            or report.get('unfinished_source_envelopes') != count - offset
            or not isinstance(report.get('response_failures'), list)
            or not isinstance(report.get('request_mismatches'), list)
            or scan['chunk_response_failures'] < len(report['response_failures'])
            or scan['chunk_request_mismatches'] < len(report['request_mismatches'])):
        raise ValueError('scan_checkpoint_invalid')
    if action == 'finished' and offset == count and not scan['has_more']:
        raise ValueError('scan_already_finished')
    if action == 'next_chunk' and offset == count and scan['has_more'] and scan['last_scanned_key'] is not None:
        return scan['last_scanned_key'], 0, None, 0, 0
    if action == 'retry_chunk' and old_chunk_size != chunk_size:
        raise ValueError('scan_scope_changed')
    if (action == 'retry_chunk' and offset < count and isinstance(scan.get('candidate_digest'), str)
            and re.fullmatch(r'[0-9a-f]{64}', scan['candidate_digest'])):
        return (scan['start_after'], offset, scan['candidate_digest'],
                scan['chunk_response_failures'], scan['chunk_request_mismatches'])
    raise ValueError('scan_checkpoint_invalid')


def _prior_pairs(prior, *, root, fingerprint=None, parser_identity=None, db=None):
    """Reuse only observations with a verified original-request identity."""
    if isinstance(prior, list):
        result = set()
        for item in prior:
            result.update(_prior_pairs(item, root=root, fingerprint=fingerprint, parser_identity=parser_identity, db=db))
        return result
    if not isinstance(prior, dict) or prior.get('source_fingerprint') != fingerprint:
        return set()
    contract = prior.get('contract')
    current_v4 = (contract == 'cnyes_retained_missing_crosscheck_v4'
                  and (prior.get('parser_identity') == parser_identity
                       or _reviewed_v4_compatible(prior, parser_identity)))
    current_chunk = contract == _CHUNK_REPORT_CONTRACT and prior.get('parser_identity') == parser_identity
    reviewed_v3 = contract == 'cnyes_retained_missing_crosscheck_v3' and _reviewed_v3_compatible(prior, parser_identity)
    if not (current_v4 or current_chunk or reviewed_v3):
        # A v1 response proof can still support facts about its raw payload,
        # but it did not prove the request of each referenced observation.
        return set()
    pairs, signatures = set(), {}
    for item in prior.get('target_outcomes', []):
        if item.get('evidence_state') == 'verified' and item.get('original_request_identity_checked') is True:
            relative = item.get('source_artifact_path')
            if not isinstance(relative, str):
                continue
            if reviewed_v3 and item.get('source_artifact_kind') not in {'canonical_quarantine', 'cnyes_history_source_envelope'}:
                continue
            receipt_relative = relative + '.receipt.json' if reviewed_v3 else item.get('source_receipt_path')
            if not isinstance(receipt_relative, str):
                continue
            if item.get('source_artifact_kind') == 'accepted_body_batch':
                try:
                    committed = db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (relative,)).fetchone() if db else None
                except sqlite3.Error:
                    committed = None
                if not committed or committed[0] != receipt_relative:
                    continue
            key = relative, receipt_relative
            if key not in signatures:
                try:
                    path = source_path(root, relative)
                    receipt = source_path(root, receipt_relative)
                    signatures[key] = (list(_stamp(path)), list(_stamp(receipt)))
                except (OSError, TypeError, ValueError):
                    signatures[key] = None
            if signatures[key] != (item.get('source_file_signature'), item.get('source_receipt_signature')):
                # A prior proof is reusable only while both original files
                # retain their verified inode, size and high-resolution clocks.
                continue
            for observation_sha in item.get('observation_sha256', []):
                pairs.add((item['news_id'], item['source_response_sha256'], observation_sha))
    return pairs


def _candidate_index(db, prior_pairs, source, only_responses=None, *, observation_chunk_size=None, start_after=None):
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
    candidates = defaultdict(lambda: {'requests': defaultdict(
        lambda: {'targets': defaultdict(set), 'observations': defaultdict(set)})})
    counts = Counter()
    previously_verified, unverified = set(), set()
    scan = {'last_scanned_key': None, 'has_more': False}
    query = 'SELECT * FROM cnyes_history_page_observations'
    parameters = ()
    if only_responses is not None:
        # Bound the Python work to the selected response identities. SQLite
        # may still scan the table if no response index exists; read-only
        # audits must not create an index in the production database.
        parameters = tuple(sorted(only_responses))
        query += ' WHERE response_sha256 IN (' + ','.join('?' for _ in parameters) + ')'
    elif observation_chunk_size is not None:
        # The table's primary key is the same six-column order. Keyset paging
        # avoids OFFSET rescans and never requires a production index write.
        if start_after is not None:
            query += ' WHERE (' + ','.join(_SCAN_COLUMNS) + ') > (' + ','.join('?' for _ in _SCAN_COLUMNS) + ')'
            parameters = tuple(start_after)
        query += ' ORDER BY ' + ','.join(_SCAN_COLUMNS) + ' LIMIT ?'
        parameters += (observation_chunk_size + 1,)
    for row in db.execute(query, parameters):
        if observation_chunk_size is not None and counts['observations_scanned'] == observation_chunk_size:
            scan['has_more'] = True
            break
        counts['observations_scanned'] += 1
        if counts['observations_scanned'] > 100_000:
            raise ValueError('observation_bound_exceeded')
        if observation_chunk_size is not None:
            scan['last_scanned_key'] = [row[column] for column in _SCAN_COLUMNS]
        try:
            observation = json.loads(row['observation_json'])
            identities = observation['identities']
            if not isinstance(identities, list) or len(identities) > 60:
                raise ValueError('invalid_identities')
            hits = [item for item in identities if type(item.get('newsId')) is int and item['newsId'] in heads]
            if not hits:
                continue
            observation = _checked_page_observation(row, source)
            if observation is None:
                raise ValueError('invalid_observation_identity')
            # A prior result cannot excuse a damaged current row which still
            # carries the old observation SHA. Check identity before skipping.
            hits = [item for item in observation['identities'] if item['newsId'] in heads
                    and (item['newsId'], row['response_sha256'], row['observation_sha256']) not in prior_pairs]
            if not hits:
                previously_verified.add(row['response_sha256'])
                counts['previously_verified_observations'] += 1
                continue
            unverified.add(row['response_sha256'])
            artifact = observation['artifact']
            relative = artifact.get('parquet_path')
            if (not re.fullmatch(r'[0-9a-f]{64}', row['response_sha256'])
                    or observation.get('provider') != 'cnyes'
                    or observation.get('scope_url') != row['scope_url']
                    or observation.get('page') != row['page']
                    or observation.get('response_sha256') != row['response_sha256']
                    or observation.get('contract_version') != row['evidence_contract_version']
                    or observation.get('source_bulk_fingerprint') != row['source_bulk_fingerprint']
                    or observation.get('authorization_fingerprint') != row['authorization_fingerprint']):
                raise ValueError('invalid_observation_identity')
            kind = artifact.get('kind')
            canonical = kind == 'canonical_quarantine' and relative == 'quarantine/' + row['response_sha256'] + '.parquet'
            history = (kind == 'cnyes_history_source_envelope' and isinstance(relative, str)
                       and relative.startswith('discovery_responses/') and relative.endswith('.parquet')
                       and not Path(relative).is_absolute() and '..' not in Path(relative).parts)
            receipt_relative = artifact.get('receipt_path')
            accepted = (kind == 'accepted_body_batch' and isinstance(relative, str)
                        and isinstance(receipt_relative, str) and relative.startswith('bodies/')
                        and relative.endswith('.parquet') and receipt_relative.startswith('receipts/')
                        and receipt_relative.endswith('.json') and not Path(relative).is_absolute()
                        and not Path(receipt_relative).is_absolute()
                        and '..' not in Path(relative).parts and '..' not in Path(receipt_relative).parts)
            if not (accepted or (canonical or history) and receipt_relative == relative + '.receipt.json'):
                counts['noncanonical_matching_observations'] += 1
                continue
            if row['authorization_fingerprint'] != source.authorization_fingerprint:
                counts['other_authorization_matching_observations'] += 1
                continue
            for item in hits:
                post_sha = item['source_post_sha256']
                if not isinstance(post_sha, str) or not re.fullmatch(r'[0-9a-f]{64}', post_sha):
                    raise ValueError('invalid_post_digest')
            # Content-identical responses may be independently captured for
            # different requests. Each saved envelope owns its own clock.
            # One accepted batch can contain distinct requests for identical
            # bytes. Each exact request has its own original and clock.
            request_key = observation['request_url'] if accepted else ''
            request = candidates[(row['response_sha256'], relative, kind, receipt_relative, request_key)]['requests'][observation['request_url']]
            for item in hits:
                request['targets'][item['newsId']].add(item['source_post_sha256'])
                request['observations'][item['newsId']].add(row['observation_sha256'])
        except (ValueError, TypeError, KeyError, AttributeError):
            counts['invalid_matching_observations'] += 1
            unverified.add(row['response_sha256'])
    return heads, candidates, counts, previously_verified - unverified, scan


def audit(root, registry, prior, *, max_responses=2000, max_seconds=300, only_responses=None,
          observation_chunk_size=None, resume_scan=None):
    if not 1 <= max_responses <= 10_000 or not 1 <= max_seconds <= 900:
        raise ValueError('invalid_audit_bounds')
    if (observation_chunk_size is not None and (type(observation_chunk_size) is not int
            or not 1 <= observation_chunk_size <= 100_000)):
        raise ValueError('invalid_observation_chunk_size')
    if (resume_scan is not None and observation_chunk_size is None
            or observation_chunk_size is not None and only_responses is not None):
        raise ValueError('incompatible_scan_scope')
    if only_responses is not None:
        only_responses = set(only_responses)
        if not 1 <= len(only_responses) <= 64 or any(not isinstance(value, str)
                or not re.fullmatch(r'[0-9a-f]{64}', value) for value in only_responses):
            raise ValueError('invalid_selected_responses')
    root = root.resolve()
    db_path, wal_path, journal_path = (root / 'queue.sqlite3', root / 'queue.sqlite3-wal',
                                       root / 'queue.sqlite3-journal')
    before = tuple(_stamp(path) for path in (db_path, wal_path, journal_path))
    if before[0] is None or any(stamp is not None and stamp[2] for stamp in before[1:]):
        raise ValueError('immutable_snapshot_requires_empty_wal_and_journal')
    db = sqlite3.connect(db_path.as_uri() + '?mode=ro&immutable=1', uri=True)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    source = registry.sources['cnyes']
    fingerprint = cnyes_bulk_fingerprint(source)
    repo = Path(__file__).resolve().parent.parent
    parser_identity = {path: sha256_file(repo / path) for path in _PARSER_PATHS}
    snapshot = [list(stamp) if stamp is not None else None for stamp in before]
    prior_digest = _json_digest(prior) if observation_chunk_size is not None else None
    start = time.monotonic()
    outcomes, source_only, failures, request_mismatches = [], [], [], []
    attempted, verified_shas = set(), set()
    start_after, candidate_offset, expected_candidate_digest = None, 0, None
    previous_failures, previous_mismatches = 0, 0
    try:
        if resume_scan is not None:
            start_after, candidate_offset, expected_candidate_digest, previous_failures, previous_mismatches = _resume_scan(
                resume_scan, root=root, snapshot=snapshot, fingerprint=fingerprint, parser_identity=parser_identity,
                prior_digest=prior_digest, chunk_size=observation_chunk_size)
        if start_after is not None:
            where = ' AND '.join(column + '=?' for column in _SCAN_COLUMNS)
            if not db.execute('SELECT 1 FROM cnyes_history_page_observations WHERE ' + where,
                              tuple(start_after)).fetchone():
                raise ValueError('scan_checkpoint_key_not_found')
        heads, candidates, counts, already_verified, scan = _candidate_index(db, _prior_pairs(
            prior, root=root, fingerprint=fingerprint, parser_identity=parser_identity, db=db), source, only_responses,
            observation_chunk_size=observation_chunk_size, start_after=start_after)
        def all_targets(page):
            return {news_id for request in page['requests'].values() for news_id in request['targets']}
        ordered = sorted(candidates.items(), key=lambda item: (-len(all_targets(item[1])), item[0]))
        candidate_digest = _candidate_digest(ordered) if observation_chunk_size is not None else None
        if expected_candidate_digest is not None and candidate_digest != expected_candidate_digest:
            raise ValueError('scan_candidates_changed')
        if candidate_offset > len(ordered):
            raise ValueError('scan_checkpoint_invalid')
        for key, page in ordered[candidate_offset:]:
            # A slow index scan must not starve all proof work on every retry.
            # One proof can exceed the time slice; its result remains explicit.
            if (len(attempted) >= max_responses or (observation_chunk_size is None or attempted)
                    and time.monotonic() - start >= max_seconds):
                break
            response_sha, relative, kind, receipt_relative, accepted_request = key
            attempted.add(key)
            try:
                proof = _source_proof(root, registry, source, db, relative,
                    fingerprint, target_hashes_by_request={url: request['targets']
                                                          for url, request in page['requests'].items()},
                    artifact_kind=kind, receipt_relative=receipt_relative,
                    accepted_request_url=accepted_request or None, accepted_response_sha=response_sha)
                if proof['response_sha256'] != response_sha:
                    raise ValueError('cached_target_response_identity_mismatch')
                exact = page['requests'].get(proof['request_url'])
                mismatched = defaultdict(set)
                for request_url, request in page['requests'].items():
                    if request_url != proof['request_url']:
                        for news_id, observation_shas in request['observations'].items():
                            for observation_sha in observation_shas:
                                mismatched[observation_sha].add(news_id)
                request_mismatches.extend({'source_response_sha256': response_sha,
                    'source_artifact_path': relative, 'source_artifact_kind': kind,
                    'observation_sha256': observation_sha, 'target_count': len(ids)}
                    for observation_sha, ids in sorted(mismatched.items()))
                counts['request_unverified_pairs'] += len(all_targets(page) - set(exact['targets'] if exact else ()))
                for item in proof['target_outcomes']:
                    outcomes.append({**item, 'source_response_sha256': response_sha,
                        'source_artifact_path': relative, 'source_artifact_kind': kind,
                        'source_receipt_path': receipt_relative,
                        'source_observed_at_utc': proof['source_observed_at_utc'],
                        'source_request_sha256': hashlib.sha256(proof['request_url'].encode()).hexdigest(),
                        'parquet_sha256': proof['parquet_sha256'],
                        'source_file_signature': proof['source_file_signature'],
                        'source_receipt_signature': proof['source_receipt_signature'],
                        'observation_sha256': sorted(exact['observations'][item['news_id']]),
                        'original_request_identity_checked': True,
                        'source_replay_permitted': proof['source_replay_permitted'],
                        'head': heads[item['news_id']], 'evidence_state': 'verified'})
                for item in proof['source_only_target_outcomes']:
                    source_only.append({**item, 'source_response_sha256': response_sha,
                        'source_artifact_path': relative, 'source_artifact_kind': kind,
                        'source_receipt_path': receipt_relative,
                        'source_observed_at_utc': proof['source_observed_at_utc'],
                        'source_request_sha256': hashlib.sha256(proof['request_url'].encode()).hexdigest(),
                        'parquet_sha256': proof['parquet_sha256'],
                        'mismatched_observation_sha256': sorted(
                            observation_sha for observation_sha, ids in mismatched.items()
                            if item['news_id'] in ids),
                        'observation_reference_verified': False,
                        'source_replay_permitted': proof['source_replay_permitted'],
                        'head': heads[item['news_id']], 'evidence_state': 'verified_raw_source_only'})
                counts['source_envelopes_verified'] += 1
                verified_shas.add(response_sha)
            except (OSError, ValueError, KeyError, TypeError, NewsError, pa.ArrowException) as error:
                failures.append({'source_response_sha256': response_sha,
                    'source_artifact_path': relative, 'source_artifact_kind': kind,
                    'error': error.code if isinstance(error, NewsError) else type(error).__name__ + ':' + str(error)[:120],
                    'target_count': len(all_targets(page))})
            if len(attempted) % 100 == 0:
                print(json.dumps({'source_envelopes_processed': len(attempted),
                                  'source_envelopes_total': len(ordered)}), flush=True)
    finally:
        db.close()
    if before != tuple(_stamp(path) for path in (db_path, wal_path, journal_path)):
        raise ValueError('production_snapshot_changed_during_audit')
    if parser_identity != {path: sha256_file(repo / path) for path in _PARSER_PATHS}:
        raise ValueError('parser_changed_during_audit')
    pair_counts = Counter(item['outcome'] for item in outcomes)
    unresolved = sum(counts[key] for key in ('invalid_matching_observations',
        'noncanonical_matching_observations', 'other_authorization_matching_observations')) + len(request_mismatches)
    candidate_shas = {key[0] for key in candidates}
    unfinished = set(candidates) - attempted - {key for key, _ in ordered[:candidate_offset]}
    result = {'contract': _CHUNK_REPORT_CONTRACT if observation_chunk_size is not None else 'cnyes_retained_missing_crosscheck_v4',
        'completed_at_utc': datetime.now(timezone.utc).isoformat(),
        'elapsed_seconds': round(time.monotonic() - start, 3),
        'source_fingerprint': fingerprint, 'parser_identity': parser_identity,
        'missing_heads_in_selected_states': len(heads),
        'candidate_responses': len(candidate_shas), 'responses_verified': len(verified_shas),
        'candidate_source_envelopes': len(candidates), 'source_envelopes_verified': counts['source_envelopes_verified'],
        'candidate_source_request_groups': len(candidates),
        'candidate_original_artifacts': len({(key[1], key[3]) for key in candidates}),
        'candidate_pairs': sum(len(all_targets(p)) for p in candidates.values()),
        'candidate_heads': len({i for p in candidates.values() for i in all_targets(p)}),
        **counts, 'response_failures': failures, 'pair_outcome_counts': dict(pair_counts),
        'target_outcomes': outcomes, 'source_only_target_outcomes': source_only,
        'source_only_outcome_counts': dict(Counter(item['outcome'] for item in source_only)),
        'unfinished_responses': len({key[0] for key in unfinished}),
        'unfinished_source_envelopes': len(unfinished),
        'request_mismatched_observations': len(request_mismatches),
        'request_unverified_pairs': counts['request_unverified_pairs'], 'request_mismatches': request_mismatches,
        'unresolved_matching_observations': unresolved,
        'observation_scan_scope': ('selected_responses' if only_responses is not None
                                   else 'observation_chunk' if observation_chunk_size is not None else 'all_observations'),
        'selected_responses_requested': len(only_responses) if only_responses is not None else None,
        'selected_responses_previously_verified': sorted(only_responses & already_verified) if only_responses is not None else [],
        'selected_responses_without_candidate': sorted(only_responses - candidate_shas - already_verified)
            if only_responses is not None else [],
        'publisher_http_requests': 0, 'production_queue_writes': 0, 'article_bodies_copied': 0,
        'historical_point_in_time_verified': False, 'all_history_complete': False,
        'scope': 'selected exact desktop IDs; accepted batches count each exact request separately; original request, page, receipt and post verified'}
    if observation_chunk_size is not None:
        offset = candidate_offset + len(attempted)
        action = 'retry_chunk' if unfinished else 'next_chunk' if scan['has_more'] else 'finished'
        info = {'contract': _SCAN_CONTRACT, 'source_root': str(root), 'snapshot': snapshot,
            'source_fingerprint': fingerprint, 'parser_identity': parser_identity, 'prior_digest': prior_digest,
            'chunk_size': observation_chunk_size, 'start_after': start_after,
            'last_scanned_key': scan['last_scanned_key'], 'has_more': scan['has_more'],
            'observations_scanned': counts['observations_scanned'], 'candidate_digest': candidate_digest,
            'candidate_offset': offset, 'candidate_count': len(ordered), 'resume_action': action,
            'chunk_response_failures': previous_failures + len(failures),
            'chunk_request_mismatches': previous_mismatches + len(request_mismatches),
            'chunk_unresolved_matching_observations': unresolved + previous_mismatches}
        info['checkpoint_sha256'] = _json_digest(info)
        result['observation_scan'] = info
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--exclude-audit', type=Path, action='append', required=True,
                        help='Previously verified audit; repeat for a partial continuation')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-responses', type=int, default=2000)
    parser.add_argument('--max-seconds', type=int, default=300)
    parser.add_argument('--response-sha256', action='append', help='Optionally inspect only these exact retained responses')
    parser.add_argument('--observation-chunk-size', type=int,
                        help='Scan this many observations in primary-key order; writes a chunk-only proof')
    parser.add_argument('--resume-scan', type=Path,
                        help='Continue or retry a chunk from its unchanged prior report')
    args = parser.parse_args(argv)
    if args.resume_scan is not None and args.observation_chunk_size is None:
        parser.error('--resume-scan requires --observation-chunk-size')
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
    resume, resume_digest = None, None
    if args.resume_scan is not None:
        before = _stamp(args.resume_scan)
        if before is None or before[2] > 128 << 20:
            parser.error('missing or oversized scan report')
        resume_digest = sha256_file(args.resume_scan)
        resume = json.loads(args.resume_scan.read_bytes())
        saved.append((args.resume_scan, before, resume_digest))
    result = audit(root, registry, priors, max_responses=args.max_responses, max_seconds=args.max_seconds,
                   only_responses=args.response_sha256, observation_chunk_size=args.observation_chunk_size,
                   resume_scan=resume)
    if any(before != _stamp(path) or digest != sha256_file(path) for path, before, digest in saved):
        raise ValueError('prior_audit_changed_during_read')
    result['excluded_prior_audit_sha256'] = [digest for _, _, digest in saved[:len(paths)]]
    if resume_digest is not None:
        result['resumed_scan_audit_sha256'] = resume_digest
    result['implementation_sha256'] = {str(p): sha256_file(repo / p) for p in (
        Path('scripts/audit_news_cnyes_retained_missing.py'), Path('scripts/audit_news_cached_replay.py'),
        Path('downloader/news_fulltext_bulk.py'), Path('downloader/news_fulltext_cnyes_history.py'))}
    payload = (json.dumps(result, ensure_ascii=False, indent=2) + '\n').encode()
    if args.observation_chunk_size is not None and len(payload) > 128 << 20:
        raise ValueError('scan_report_too_large_reduce_chunk_size')
    atomic_write_bytes(output, payload, durable=True, mode=0o600)
    print(json.dumps({k: v for k, v in result.items()
                      if k not in {'target_outcomes', 'source_only_target_outcomes',
                                   'response_failures', 'request_mismatches'}},
                     ensure_ascii=False))
    chunk = result.get('observation_scan', {})
    return 0 if not (result['response_failures'] or result['unfinished_responses']
                     or result['unresolved_matching_observations'] or result['selected_responses_without_candidate']
                     or chunk.get('chunk_response_failures') or chunk.get('chunk_unresolved_matching_observations')) else 1


if __name__ == '__main__':
    raise SystemExit(main())
