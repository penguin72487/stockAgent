"""Bounded metadata repair from retained HTML, under the canonical news owner.

This is a fixed current-head cohort, not another crawler. Preserve the original
response/capture and every old immutable receipt. Only independently verified
mobile aliases may acquire a new derived version; leases, refusals, validators,
attempts and the last HTTP check remain untouched.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import re
import time
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_policy import CONTRACT_VERSION, NewsError, extract_article
from downloader.news_fulltext_queue import source_path, utc_now


METADATA_REPLAY_VERSION = 2
PRIOR_NEWSROOM_REVIEW_FINGERPRINT = '940494a29ae4940afe046da3b5cdfa715867e9ac5cc402c203a59acad0fd1969'
HEAD_FIELDS = ('state', 'parquet_path', 'body_sha256', 'captured_at_utc',
               'last_checked_at_utc', 'authorization_fingerprint', 'due',
               'attempts', 'error_code', 'lease_owner', 'lease_until')


def _ensure_cohort(queue, source):
    """Index one bounded host cohort per parser contract, not the entire lake."""
    fingerprint = source.data_fingerprint
    marker = 'cnyes_mobile_metadata_cohort_v1:' + fingerprint
    with queue.db:
        queue.db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_replays(
            source_fingerprint TEXT NOT NULL, article_id BLOB NOT NULL,
            original_path TEXT NOT NULL, original_body_sha256 TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending', error_code TEXT,
            checked_at_utc TEXT,
            PRIMARY KEY(source_fingerprint,article_id)) WITHOUT ROWID''')
        queue.db.execute('CREATE INDEX IF NOT EXISTS retained_metadata_ready '
                         'ON retained_metadata_replays(source_fingerprint,state,original_path,article_id)')
        repair_marker = 'retained_mobile_metadata_scope_v2:' + fingerprint
        if not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (repair_marker,)).fetchone():
            # Re-examine only the local checkpoints affected by the observed
            # same-scope review and loader-title corrections. No article state,
            # request, refusal, cooldown, attempt or stored head is reset.
            queue.db.execute("UPDATE retained_metadata_replays SET state='pending' WHERE source_fingerprint=? "
                             "AND state='failed_validation' AND error_code IN "
                             "('retained_metadata_authorization_changed','retained_metadata_title_conflict')", (fingerprint,))
            queue.db.execute("UPDATE retained_metadata_replays SET state='not_mobile_html_candidate' "
                             "WHERE source_fingerprint=? AND state='failed_validation' "
                             "AND error_code='retained_metadata_not_registered_mobile_article'", (fingerprint,))
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (repair_marker, utc_now()))
        loader_marker = 'retained_mobile_loader_selector_v1:' + fingerprint
        if not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (loader_marker,)).fetchone():
            # Native old HTML records use the configured CSS selector, not a
            # generic css_article label. Revisit only the local title proof;
            # the immutable old head and all HTTP scheduling remain untouched.
            queue.db.execute("UPDATE retained_metadata_replays SET state='pending' WHERE source_fingerprint=? "
                             "AND state='failed_validation' AND error_code='retained_metadata_title_conflict'",
                             (fingerprint,))
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (loader_marker, utc_now()))
        if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
            return
        # Host index limits discovery to the registered representation. Refuse
        # overflow, rather than silently calling a truncated cohort complete.
        heads = queue.db.execute("SELECT id,parquet_path,body_sha256 FROM articles "
                                 "WHERE host='m.cnyes.com' AND body_sha256 IS NOT NULL "
                                 "LIMIT 100001").fetchall()
        if len(heads) > 100000:
            raise ValueError('retained_metadata_cohort_exceeds_bound')
        queue.db.executemany('INSERT OR IGNORE INTO retained_metadata_replays '
                             '(source_fingerprint,article_id,original_path,original_body_sha256) VALUES (?,?,?,?)',
                             ((fingerprint, r['id'], r['parquet_path'], r['body_sha256']) for r in heads))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
                         (marker, json.dumps({'heads': len(heads), 'observed_at_utc': utc_now(),
                                              'http_requests': 0, 'all_history_complete': False}, sort_keys=True)))


def _checkpoint_batch(queue, fingerprint, checkpoints):
    """Commit bounded local outcomes together, never body acceptance.

    The caller has at most 64 heads. A crash before this commit repeats their
    guards; a crash after immutable acceptance is still recovered by the
    changed head. SQLite failure rolls back this whole local checkpoint group.
    """
    if not checkpoints:
        return
    with queue.db:
        queue.db.executemany('UPDATE retained_metadata_replays SET state=?,error_code=?,checked_at_utc=? '
                            'WHERE source_fingerprint=? AND article_id=?',
                            ((state, error, utc_now(), fingerprint, key) for key, state, error in checkpoints))
    queue.metrics.count('cnyes', 'retained_metadata_checkpoint_rows', len(checkpoints))
    queue.metrics.count('cnyes', 'retained_metadata_checkpoint_transactions')


def _needs_repair(record):
    return (not record.get('published_at_raw') or not json.loads(record.get('authors_json') or '[]')
            or record.get('body_completeness') == 'article_container_unverified')


def _same_local_newsroom_scope(old, head, source):
    previous = json.loads(old['authorization_json'])
    digest = hashlib.sha256(json.dumps(previous, sort_keys=True).encode()).hexdigest()
    if digest != old['authorization_fingerprint'] or head['authorization_fingerprint'] != digest:
        return False
    if digest == source.authorization_fingerprint and previous == source.authorization:
        return True
    # The observed older review predates an added Hao-community restriction;
    # this operation is strictly m.cnyes.com newsroom HTML, never Hao, an API
    # permission upgrade, a transfer, a license or training authorization.
    current = source.authorization
    return (digest == PRIOR_NEWSROOM_REVIEW_FINGERPRINT
            and previous.get('kind') == current.get('kind') == 'personal_research'
            and all(previous.get(k) == current.get(k) for k in ('scope', 'evidence_url', 'automated_access_review'))
            and current.get('rights_restrictions', '').startswith(previous.get('rights_restrictions', ''))
            and set(previous) == {'kind', 'scope', 'evidence_url', 'reviewed_on', 'automated_access_review', 'rights_restrictions'})


def _repair_record(old, payload, head, source, receipt_path, receipt_digest):
    """Re-derive solely from the exact observed HTML; never infer a clock."""
    from downloader.news_fulltext_publishers import cnyes_supported_mobile_view
    url = urlsplit(old['final_url'])
    if not cnyes_supported_mobile_view(url):
        raise ValueError('retained_metadata_not_registered_mobile_article')
    if (old['provider'] != source.provider or old['article_id'] != head['id'].hex()
            or old['requested_url'] != head['url'] or old['body_sha256'] != head['body_sha256']
            or old['captured_at_utc'] != head['captured_at_utc']):
        raise ValueError('retained_metadata_head_identity_mismatch')
    if not _same_local_newsroom_scope(old, head, source):
        raise ValueError('retained_metadata_authorization_changed')
    captured = datetime.fromisoformat(old['captured_at_utc'])
    if captured.tzinfo is None or captured > datetime.now(timezone.utc):
        raise ValueError('retained_metadata_observation_clock_invalid')
    if (old['contract_version'] != CONTRACT_VERSION or old['publish'] is not False
            or old['remote_training_export'] is not False or old['historical_point_in_time'] is not False
            or old.get('collection_purpose') != 'personal_research'
            or any(old.get(k) is not False for k in ('fair_use_legally_determined', 'commercial_use_enabled', 'training_injection_enabled'))):
        raise ValueError('retained_metadata_unapproved_source_contract')
    if (hashlib.sha256(payload).hexdigest() != old['response_sha256']
            or len(payload) != old['http_response_bytes']
            or hashlib.sha256(old['body'].encode()).hexdigest() != old['body_sha256']
            or len(old['body'].encode()) != old['body_bytes']):
        raise ValueError('retained_metadata_source_digest_mismatch')
    article = extract_article(payload, source, page_url=old['final_url'])
    if article.get('extraction_method') != 'cnyes_visible_canonical_alias_v1':
        raise ValueError('retained_metadata_alias_not_independently_verified')
    loader_title = (old['title'] in {'...', '…'} and old.get('body_completeness') == 'article_container_unverified'
                    and old.get('extraction_method') == 'css:#article-container' and not old.get('published_at_raw')
                    and not json.loads(old.get('authors_json') or '[]'))
    if not loader_title and re.sub(r'\s+', '', old['title']) != re.sub(r'\s+', '', article['title']):
        raise ValueError('retained_metadata_title_conflict')
    # A missing field may be repaired. An existing conflicting source field is
    # evidence requiring review, not permission to quietly overwrite history.
    if old.get('published_at_raw') and old['published_at_raw'] != article['published_at_raw']:
        raise ValueError('retained_metadata_publication_conflict')
    if json.loads(old.get('authors_json') or '[]') not in ([], json.loads(article['authors_json'])):
        raise ValueError('retained_metadata_author_conflict')
    return {**old, **article,
            'body_bytes': len(article['body'].encode()), 'raw_response_body': payload,
            'source_response_article_id': old['article_id'],
            'data_fingerprint': source.data_fingerprint, 'parser_backend': source.html_parser,
            'source_observed_at_utc': old['captured_at_utc'], 'reparsed_at_utc': utc_now(),
            'acquisition_transport': 'retained_raw_reparse',
            'metadata_replay_contract_version': METADATA_REPLAY_VERSION,
            'original_capture_data_fingerprint': old['data_fingerprint'],
            'original_capture_title': old['title'],
            'replay_title_basis': 'verified_same_id_current_headline; old_loader_title_preserved' if loader_title else 'existing_source_title_corroborated',
            'replay_policy_authorization_fingerprint': source.authorization_fingerprint,
            'replay_authorization_basis': 'same_local_newsroom_scope; historical_authorization_and_restrictions_preserved',
            'replay_source_parquet': head['parquet_path'], 'replay_source_receipt': receipt_path,
            'replay_source_receipt_sha256': receipt_digest,
            'retained_metadata_head_json': json.dumps({k: head[k] for k in HEAD_FIELDS}, sort_keys=True),
            # Historical HTTP timings are provenance, never a new online sample.
            'replay_preserves_last_http_check': True}


def replay_retained_cnyes_metadata(queue, *, maximum_heads=32, maximum_wall_seconds=0.25,
                                   maximum_bytes=32 << 20, stop=None, target_article_ids=None):
    """Bound each maintenance quantum; failed source proofs do not block peers.

    Immutable acceptance and checkpoint are separate crash-safe commits. A
    crash after acceptance is recognized by the changed head on the next tick.
    No HTTP request or HTTP attempt/cooldown reset occurs here.
    A bounded explicit ID set repairs an observed source-format cohort through
    these same guards, without reparsing unrelated history first. Resident
    maintenance leaves it unset and continues its durable fixed cohort.
    """
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed:
        return {'heads_checked': 0, 'repaired_versions': 0, 'http_requests': 0}
    if (type(maximum_heads) is not int or not 1 <= maximum_heads <= 64
            or type(maximum_wall_seconds) not in (int, float) or not 0 < maximum_wall_seconds <= 1
            or type(maximum_bytes) is not int or not 1 <= maximum_bytes <= 32 << 20):
        raise ValueError('unbounded retained metadata replay')
    if target_article_ids is not None and (
            not isinstance(target_article_ids, (list, tuple)) or not 1 <= len(target_article_ids) <= maximum_heads
            or any(not isinstance(key, bytes) or len(key) != 32 for key in target_article_ids)
            or len(set(target_article_ids)) != len(target_article_ids)):
        raise ValueError('invalid bounded retained metadata target IDs')
    started = time.monotonic()
    totals = Counter(heads_checked=0, repaired_versions=0, batches_read=0, raw_bytes_checked=0)
    with queue.metrics.measure('retained_metadata_replay', 'cnyes'):
        _ensure_cohort(queue, source)
        fp = source.data_fingerprint
        # Deferred rows are reconsidered using indexed head state, never by
        # repeatedly parsing their HTML while a request/denial remains active.
        target_clause = '' if target_article_ids is None else ' AND article_id IN (' + ','.join('?' for _ in target_article_ids) + ')'
        targets = () if target_article_ids is None else tuple(target_article_ids)
        pending = queue.db.execute('SELECT * FROM retained_metadata_replays WHERE source_fingerprint=? '
                                   "AND state='pending'" + target_clause + ' ORDER BY original_path,article_id LIMIT ?',
                                   (fp, *targets, maximum_heads)).fetchall()
        deferred = queue.db.execute('SELECT r.* FROM retained_metadata_replays r JOIN articles a ON a.id=r.article_id '
                                    "WHERE r.source_fingerprint=? AND r.state='deferred' AND a.state='complete' "
                                    'AND a.lease_owner IS NULL AND a.lease_until IS NULL '
                                    + target_clause.replace('article_id', 'r.article_id')
                                    + ' ORDER BY r.original_path,r.article_id LIMIT ?',
                                    (fp, *targets, maximum_heads - len(pending))).fetchall()
        groups, eligibility_checkpoints = defaultdict(list), []
        for job in [*pending, *deferred]:
            head = queue.db.execute('SELECT * FROM articles WHERE id=?', (job['article_id'],)).fetchone()
            if not head or (head['parquet_path'], head['body_sha256']) != (job['original_path'], job['original_body_sha256']):
                eligibility_checkpoints.append((job['article_id'], 'superseded', None))
            elif head['state'] != 'complete' or head['lease_owner'] is not None or head['lease_until'] is not None:
                eligibility_checkpoints.append((job['article_id'], 'deferred', None))
            else:
                groups[job['original_path']].append(dict(head))
        _checkpoint_batch(queue, fp, eligibility_checkpoints)
        for relative, heads in groups.items():
            if (time.monotonic() - started >= maximum_wall_seconds or totals['raw_bytes_checked'] >= maximum_bytes
                    or stop and stop.is_set()):
                break
            eligible, checkpoints = [], []
            try:
                path = source_path(queue.root, relative)
                if path.stat().st_size > maximum_bytes:
                    raise ValueError('retained_metadata_batch_exceeds_resource_bound')
                receipt_row = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (relative,)).fetchone()
                if not receipt_row:
                    raise ValueError('retained_metadata_missing_acceptance_receipt')
                receipt_path = source_path(queue.root, receipt_row[0])
                receipt = json.loads(receipt_path.read_text())
                if (receipt['contract_version'] != CONTRACT_VERSION or receipt['parquet_path'] != relative
                        or receipt['publish'] is not False or sha256_file(path) != receipt['sha256']):
                    raise ValueError('retained_metadata_batch_receipt_invalid')
                batch = pq.ParquetFile(path)
                if batch.metadata.num_rows != receipt['rows'] or batch.metadata.num_rows > 512:
                    raise ValueError('retained_metadata_batch_grain_invalid')
                metadata = batch.read(columns=['article_id', 'body_sha256', 'published_at_raw',
                                                'authors_json', 'body_completeness', 'final_url'], use_threads=False).to_pylist()
                counts = Counter(r['article_id'] for r in metadata)
                indexed_meta = {r['article_id']: r for r in metadata}
                selected = []
                for head in heads:
                    key = head['id'].hex()
                    old = indexed_meta.get(key)
                    if counts[key] != 1 or old['body_sha256'] != head['body_sha256']:
                        raise ValueError('retained_metadata_batch_head_not_unique')
                    if urlsplit(old['final_url']).hostname != 'm.cnyes.com':
                        checkpoints.append((head['id'], 'not_mobile_html_candidate', None))
                        totals['heads_checked'] += 1
                    elif not _needs_repair(old):
                        checkpoints.append((head['id'], 'current_metadata_complete', None))
                        totals['heads_checked'] += 1
                    else:
                        selected.append(head)
                if not selected:
                    continue
                # Bound materialization before decompressing response/body
                # columns, using the actual Arrow footer, not compressed size.
                uncompressed = sum(batch.metadata.row_group(g).column(c).total_uncompressed_size
                                   for g in range(batch.metadata.num_row_groups)
                                   for c in range(batch.metadata.num_columns))
                if uncompressed > maximum_bytes:
                    raise ValueError('retained_metadata_batch_exceeds_resource_bound')
                records = batch.read(use_threads=False).to_pylist()
                indexed = {r['article_id']: r for r in records}
                envelopes = {r['article_id']: r['raw_response_body'] for r in records if r.get('raw_response_body') is not None}
                totals['batches_read'] += 1
                receipt_digest = sha256_file(receipt_path)
                for head in selected:
                    if time.monotonic() - started >= maximum_wall_seconds or stop and stop.is_set():
                        break
                    try:
                        old = indexed[head['id'].hex()]
                        payload = envelopes[old['source_response_article_id']]
                        if len(payload) + totals['raw_bytes_checked'] > maximum_bytes:
                            break
                        totals['raw_bytes_checked'] += len(payload)
                        eligible.append(_repair_record(old, payload, head, source, receipt_row[0], receipt_digest))
                    except (ValueError, KeyError, TypeError, NewsError) as error:
                        checkpoints.append((head['id'], 'failed_validation',
                                            error.code if isinstance(error, NewsError) else str(error)[:160]))
                        queue.metrics.count('cnyes', 'retained_metadata_validation_errors')
                    totals['heads_checked'] += 1
            except (ValueError, KeyError, TypeError, OSError, pa.ArrowException) as error:
                # This is a local/source-proof gap, never a verdict that a
                # publisher has corrupt data. Leave original heads untouched.
                for head in heads:
                    checkpoints.append((head['id'], 'failed_validation', str(error)[:160]))
                queue.metrics.count('cnyes', 'retained_metadata_validation_errors', len(heads))
                continue
            finally:
                # Keep unrelated complete/deferred/failure outcomes durable
                # even when a later body acceptance fails. No verified repair
                # is checkpointed before the independent immutable commit.
                _checkpoint_batch(queue, fp, checkpoints)
            if eligible:
                # Storage/commit failures propagate, leaving the cohort pending.
                # The canonical owner restart recovers durable receipts first.
                repaired = queue.accept(eligible)
                _checkpoint_batch(queue, fp, [(bytes.fromhex(record['article_id']), 'verified_repair', None)
                                              for record in eligible])
                totals['repaired_versions'] += repaired
        queue.metrics.count('cnyes', 'retained_metadata_heads_checked', totals['heads_checked'])
        queue.metrics.count('cnyes', 'retained_metadata_repaired_versions', totals['repaired_versions'])
    return {**totals, 'http_requests': 0, 'all_history_complete': False,
            'wall_seconds': time.monotonic() - started}
