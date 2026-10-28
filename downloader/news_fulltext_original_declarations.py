"""Acquire literal originals from retained declarations, never accept aliases.

The queue owner records a same-response declaration in the failure transaction.
Its existing intake pump drains that indexed outbox; there is no second HTTP
client, renewed failure allowance, generated slug or replay of completed work.
"""
from collections import Counter
import hashlib
import math
import re
import sqlite3
import time
from pathlib import Path

NORMAL_KIND = 'cmnews_normal_failure_metadata_v1'
RESPONSE_KIND = 'cmnews_same_response_declaration_v1'
_HASH = re.compile(r'[0-9a-f]{64}\Z')


def witness_shape(value):
    """Bound the new v3 inbox union before any file/DB-backed validation."""
    from downloader.news_fulltext_repairs import HEAD_FIELDS
    if not isinstance(value, dict):
        return False
    kind = value.get('kind')
    fields = {'kind', 'parent_url', 'parent_head'}
    fields |= ({'metadata_path', 'metadata_sha256'} if kind == NORMAL_KIND else
        {'parent_source', 'received_at_utc', 'response_sha256', 'response_bytes', 'attribute'}
        if kind == RESPONSE_KIND else set())
    return (kind in {NORMAL_KIND, RESPONSE_KIND} and set(value) == fields
        and isinstance(value['parent_url'], str) and 0 < len(value['parent_url']) <= 2048
        and isinstance(value['parent_head'], dict) and set(value['parent_head']) == set(HEAD_FIELDS)
        and type(value['parent_head']['attempts']) is int and value['parent_head']['attempts'] >= 0
        and type(value['parent_head']['due']) in (int, float) and math.isfinite(value['parent_head']['due'])
        and (kind != NORMAL_KIND or isinstance(value['metadata_path'], str)
            and 0 < len(value['metadata_path']) <= 4096
            and isinstance(value['metadata_sha256'], str) and _HASH.fullmatch(value['metadata_sha256']) is not None))


def validate_witness(db, registry, url, witness):
    """Approve acquisition scope only, bound to the unchanged failed parent."""
    from downloader.news_fulltext_candidate_diagnostics import recorded_cmnews_canonical
    from downloader.news_fulltext_cmoneylearning import cmoneylearning_literal_original
    from downloader.news_fulltext_repairs import _bytes, _clock, _head, _source_contract
    error = 'invalid_observed_original_declaration'
    try:
        if not witness_shape(witness) or not cmoneylearning_literal_original(url):
            raise ValueError(error)
        source = registry.require(url)
        if (source.provider != 'cmoneylearning' or not source.allowed
                or source.embedded_article_layout != 'cmoneylearning_visible_article_v8'
                or source.acquisition_mode != 'finite' or not source.allow_reviewed_supplements):
            raise ValueError(error)
        parent_url, target = witness['parent_url'], witness['parent_head']
        parent = registry.require(parent_url)
        head = db.execute('SELECT * FROM articles WHERE url=?', (parent_url,)).fetchone()
        if (parent.provider != 'cmnews' or not parent.allowed
                or parent.embedded_article_layout != 'cmnews_visible_article_v1'
                or head is None or head['body_sha256'] is not None or _head(head) != target
                or target['url'] != parent_url or target['state'] != 'article_identity_not_verified'
                or target['error_code'] != 'article_identity_not_verified'
                or head['lease_owner'] or head['lease_until']):
            raise ValueError(error)
        identity = _source_contract(db, parent, parent_url)[1]
        if witness['kind'] == NORMAL_KIND:
            recorded = recorded_cmnews_canonical(db, parent, target)
            if (not recorded or recorded['canonical_url'] != url
                    or Path(recorded['metadata_path']) != Path(witness['metadata_path'])
                    or recorded['metadata_sha256'] != witness['metadata_sha256']):
                raise ValueError(error)
        else:
            attribute = witness['attribute']
            literal = attribute['canonical_attribute']
            if (witness['parent_source'] != identity
                    or not isinstance(witness['response_sha256'], str)
                    or _HASH.fullmatch(witness['response_sha256']) is None
                    or type(witness['response_bytes']) is not int or not 0 < witness['response_bytes'] <= 8 << 20
                    or not (_clock(witness['received_at_utc']) <= _clock(target['last_checked_at_utc']) <= time.time())
                    or type(attribute['contract_version']) is not int or attribute['contract_version'] != 1
                    or attribute['native_scope_verified'] is not True
                    or type(attribute['canonical_count']) is not int or attribute['canonical_count'] != 1
                    or type(attribute['automatic_requests']) is not int or attribute['automatic_requests'] != 0
                    or any(attribute.get(k) is not False for k in ('article_or_alias_accepted',
                        'raw_or_names_or_body_retained', 'source_permission_granted'))
                    or literal['literal'] != url or literal['absolute'] != url
                    or literal['literal_sha256'] != hashlib.sha256(url.encode()).hexdigest()
                    or literal['source_declaration_only'] is not True or literal['relative_reference'] is not False
                    or literal['safe_to_request_assumed'] is not False):
                raise ValueError(error)
        return source, _bytes(dict(contract_version=2, url=url, witness=witness,
            parent_head=target, parent_source=identity, acquisition_scope_only=True, body_admission=False))
    except (KeyError, TypeError, ValueError, OverflowError, RecursionError):
        raise ValueError(error) from None


def _schema(queue):
    if getattr(queue, '_original_declaration_outbox_ready', False):
        return
    transactional = queue.db.in_transaction
    queue.db.execute('CREATE TABLE IF NOT EXISTS original_declaration_outbox ('
        'id TEXT PRIMARY KEY,state TEXT NOT NULL,payload TEXT NOT NULL,'
        'created_at_utc TEXT NOT NULL,completed_at_utc TEXT,outcome TEXT)')
    queue.db.execute('CREATE INDEX IF NOT EXISTS original_declaration_pending '
        'ON original_declaration_outbox(state,id)')
    # A transactional DDL success may still roll back with its failed head.
    if not transactional:
        queue._original_declaration_outbox_ready = True


def record_failure(queue, row, error):
    """Called INSIDE the original queue-failure transaction; never commit."""
    from downloader.news_fulltext_diagnostics import FailureResponse
    from downloader.news_fulltext_repairs import _bytes, _head, _source_contract
    if row['provider'] != 'cmnews' or error.code != 'article_identity_not_verified':
        return
    attribute = getattr(error, 'cmnews_original_declaration', None)
    context = getattr(error, 'failure_response', None)
    if attribute is None or not isinstance(context, FailureResponse):
        return
    try:
        parent = queue.registry.require(row['url'])
        response = context.response
        if (not parent.allowed or parent.provider != context.provider or context.provider != 'cmnews'
                or context.source_fingerprint != parent.data_fingerprint
                or context.requested_url != row['url'] or response.url != row['url']
                or response.status != 200 or response.redirects
                or not re.search(r'text/html|application/xhtml[+]xml', response.headers.get('content-type', ''), re.I)):
            return
        url = attribute['canonical_attribute']['literal']
        original = queue.db.execute('SELECT state,attempts,body_sha256 FROM articles WHERE url=?', (url,)).fetchone()
        if original is not None and (original['body_sha256'] or original['state'] != 'waiting_cohort' or original['attempts']):
            queue.metrics.count('cmnews', 'original_declaration_existing_head_reused')
            return
        head = queue.db.execute('SELECT * FROM articles WHERE id=?', (row['id'],)).fetchone()
        witness = dict(kind=RESPONSE_KIND, parent_url=row['url'], parent_head=_head(head),
            parent_source=_source_contract(queue.db, parent, row['url'])[1],
            received_at_utc=context.received_at_utc, response_sha256=hashlib.sha256(response.body).hexdigest(),
            response_bytes=len(response.body), attribute=attribute)
        source, _ = validate_witness(queue.db, queue.registry, url, witness)
        payload = _bytes(dict(url=url, declared_original=witness,
            source_data_fingerprint=source.data_fingerprint,
            source_authorization_fingerprint=source.authorization_fingerprint))
        if len(payload) > 16384:
            return
        _schema(queue)
        queue.db.execute('INSERT OR IGNORE INTO original_declaration_outbox VALUES (?,\'pending\',?,?,NULL,NULL)',
            (hashlib.sha256(payload).hexdigest(), payload.decode(), context.received_at_utc))
        queue.metrics.count('cmnews', 'original_declaration_outbox_recorded')
    except Exception:
        # The failed original remains a failure; a metadata error must not lose
        # it or stop unrelated publishers. Failure evidence/budgets stay intact.
        queue.metrics.count('cmnews', 'original_declaration_outbox_record_errors')


def consume(queue, *, limit=16, max_seconds=.02, stop=None):
    """Resume pending declarations through the existing owner, in O(work)."""
    import json
    from downloader.news_fulltext_cohorts import approve_declared_original
    from downloader.news_fulltext_policy import NewsError
    from downloader.news_fulltext_queue import utc_now
    source = queue.registry.sources.get('cmoneylearning')
    if not source or not source.allowed or source.embedded_article_layout != 'cmoneylearning_visible_article_v8':
        return {'processed': 0, 'outcomes': {}}
    if (type(limit) is not int or not 1 <= limit <= 32 or type(max_seconds) not in (int, float)
            or not 0 < max_seconds <= .1):
        raise ValueError('invalid_original_declaration_slice')
    _schema(queue)
    started, processed, counts = time.monotonic(), 0, Counter()
    rows = queue.db.execute("SELECT * FROM original_declaration_outbox WHERE state='pending' ORDER BY id LIMIT ?", (limit,)).fetchall()
    for row in rows:
        if time.monotonic()-started >= max_seconds or stop is not None and stop.is_set():
            break
        try:
            with queue.db:
                try:
                    if hashlib.sha256(row['payload'].encode()).hexdigest() != row['id']:
                        raise ValueError('outbox_payload_digest_mismatch')
                    payload = json.loads(row['payload'])
                    if (payload['source_data_fingerprint'] != source.data_fingerprint
                            or payload['source_authorization_fingerprint'] != source.authorization_fingerprint):
                        raise ValueError('source_contract_changed')
                    head = queue.db.execute('SELECT * FROM articles WHERE url=?', (payload['url'],)).fetchone()
                    if head is not None and (head['state'] != 'waiting_cohort' or head['attempts']
                            or head['body_sha256'] or head['lease_owner'] or head['lease_until'] or head['due'] > time.time()):
                        outcome = 'existing_head_preserved'
                    else:
                        approve_declared_original(queue, payload['url'], payload['declared_original'])
                        outcome = 'declared_original_acquisition_registered'
                    state = 'completed'
                except (ValueError, KeyError, TypeError, RecursionError, OverflowError, NewsError):
                    outcome, state = 'declared_original_witness_rejected', 'rejected'
                queue.db.execute('UPDATE original_declaration_outbox SET state=?,completed_at_utc=?,outcome=? WHERE id=? AND state=\'pending\'',
                    (state, utc_now(), outcome, row['id']))
        except (OSError, sqlite3.OperationalError):
            # Roll back membership and leave the original pending declaration
            # intact. The normal intake interval retries without another GET
            # or crashing/restarting an otherwise healthy publisher owner.
            counts['original_declaration_storage_deferred'] += 1
            queue.metrics.count('cmnews', 'original_declaration_storage_deferred')
            break
        processed += 1
        counts[outcome] += 1
        queue.counter('original_declaration_' + outcome, 1)
    return {'processed': processed, 'outcomes': dict(counts)}
