"""Bounded metadata repair from retained HTML, under the canonical news owner.

This is a local current-head index, not another crawler. Preserve the original
response/capture, prior checkpoints and every old immutable receipt. Only
independently verified mobile aliases may acquire a new derived version;
leases, refusals, validators, attempts and the last HTTP check remain untouched.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import time
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_metadata_index import (
    METADATA_INDEX_IDLE_SECONDS, advance_metadata_index_scan, begin_metadata_index_scan,
    inherit_unchanged_chart_checkpoints, initialize_metadata_index,
)
from downloader.news_fulltext_policy import CONTRACT_VERSION, NewsError, extract_article
from downloader.news_fulltext_queue import replay_source_path, utc_now


METADATA_REPLAY_VERSION = 2
METADATA_REPLAY_POLL_SECONDS = 10
METADATA_REPLAY_MAX_BYTES = 32 << 20
METADATA_INITIAL_COHORT_LIMIT = 100000
METADATA_LOCAL_RETRY_CONTRACT_VERSION = 1
METADATA_LOCAL_RETRY_MAX_SECONDS = 300
METADATA_RESOURCE_WAIT_CONTRACT_VERSION = 1
PRIOR_NEWSROOM_REVIEW_FINGERPRINT = '940494a29ae4940afe046da3b5cdfa715867e9ac5cc402c203a59acad0fd1969'
HEAD_FIELDS = ('state', 'parquet_path', 'body_sha256', 'captured_at_utc',
               'last_checked_at_utc', 'authorization_fingerprint', 'due',
               'attempts', 'error_code', 'lease_owner', 'lease_until')


class _ResourceBudgetExceeded(Exception):
    """A local slice limit, not evidence that a publisher response is invalid."""
    def __init__(self, required_bytes, phase):
        self.required_bytes = required_bytes
        self.phase = phase


def _inherit_unchanged_chart_checkpoints(queue, source):
    """Keep prior exact-head results when only the chart proof changed.

    The predecessor fingerprint is reconstructed from the complete current
    policy with just this parser ABI removed; a change to scope, rights or any
    other capture setting therefore cannot match it. New candidates and changed
    heads stay pending. Old body failures and batch-wide head mismatches are
    reconsidered; other unchanged terminal observations can be copied.
    This runs in the cohort-creation transaction and never changes an article.
    """
    return inherit_unchanged_chart_checkpoints(queue.db, source.data_fingerprint,
                                               source.parser_repair_prior_capture_fingerprint)


def _ensure_incremental_chart_reuse(queue, source, cohort_value=None):
    """Revisit a legacy incremental cursor once after enabling exact reuse.

    Old incremental scans could already have passed pending heads without
    inheriting their prior proof. Restart only that bounded scan, once per
    fingerprint, without resetting any article, checkpoint or HTTP state.
    A fresh cohort will already perform inheritance while it advances.
    """
    fp = source.data_fingerprint
    marker = 'retained_mobile_chart_incremental_reuse_v1:' + fp
    if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
        return
    reset = False
    previous = source.parser_repair_prior_capture_fingerprint
    if (cohort_value is not None and isinstance(previous, str)
            and re.fullmatch(r'[0-9a-f]{64}', previous) and previous != fp):
        try:
            cohort = json.loads(cohort_value)
        except (TypeError, ValueError):
            cohort = None
        needs_scan = (not isinstance(cohort, dict)
                      or cohort.get('initial_index_mode') == 'incremental_scan'
                      or 'unchanged_prior_checkpoints' not in cohort
                      or bool(cohort.get('invalid_heads_need_scan')))
        if needs_scan:
            result = queue.db.execute('''UPDATE retained_metadata_incremental_scans
                SET state='scanning',cursor_state=NULL,cursor_rowid=0,schema_version=?,
                    cursor_resets=cursor_resets+1,updated_at_utc=? WHERE source_fingerprint=?''',
                (queue.db.execute('PRAGMA schema_version').fetchone()[0], utc_now(), fp))
            reset = bool(result.rowcount)
    queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
                     (marker, json.dumps({'legacy_scan_reset': reset, 'observed_at_utc': utc_now()}, sort_keys=True)))


def _ensure_cohort(queue, source):
    """Index one bounded host cohort per parser contract, not the entire lake."""
    fingerprint = source.data_fingerprint
    marker = 'cnyes_mobile_metadata_cohort_v1:' + fingerprint
    with queue.db:
        from downloader.news_fulltext_large_replay_owner import initialize as initialize_large_replay
        initialize_large_replay(queue.db)
        if not queue.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                "AND name='retained_metadata_incremental_scans'").fetchone():
            initialize_metadata_index(queue.db)
        queue.db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_local_retries(
            source_fingerprint TEXT NOT NULL,original_path TEXT NOT NULL,
            not_before_epoch REAL NOT NULL,local_attempts INTEGER NOT NULL,
            last_attempt_at_utc TEXT NOT NULL,last_error TEXT NOT NULL,
            PRIMARY KEY(source_fingerprint,original_path)) WITHOUT ROWID''')
        queue.db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_resource_waits(
            source_fingerprint TEXT NOT NULL,original_path TEXT NOT NULL,
            required_bytes INTEGER NOT NULL,phase TEXT NOT NULL,observed_at_utc TEXT NOT NULL,
            PRIMARY KEY(source_fingerprint,original_path)) WITHOUT ROWID''')
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
        isolation_marker = 'retained_mobile_head_isolation_v1:' + fingerprint
        if not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (isolation_marker,)).fetchone():
            # The former batch-wide exception could assign this one head's
            # mismatch to every peer in its source batch. Preserve those
            # outcomes, then reconsider only that exact local proof once.
            # A new failure under the isolated contract remains terminal.
            observed_at = utc_now()
            queue.db.execute('''INSERT INTO retained_metadata_checkpoint_history
                (source_fingerprint,article_id,original_path,original_body_sha256,state,error_code,
                 checked_at_utc,archived_at_utc,reason)
                SELECT source_fingerprint,article_id,original_path,original_body_sha256,state,error_code,
                       checked_at_utc,?,'batch_head_isolation_upgrade'
                FROM retained_metadata_replays WHERE source_fingerprint=? AND state='failed_validation'
                    AND error_code='retained_metadata_batch_head_not_unique' ''', (observed_at, fingerprint))
            updated = queue.db.execute('''UPDATE retained_metadata_replays
                SET state='pending',error_code=NULL,checked_at_utc=NULL
                WHERE source_fingerprint=? AND state='failed_validation'
                    AND error_code='retained_metadata_batch_head_not_unique' ''', (fingerprint,)).rowcount
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
                             (isolation_marker, json.dumps({'reconsidered': updated,
                                 'observed_at_utc': observed_at, 'http_requests': 0}, sort_keys=True)))
        cohort_row = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if cohort_row:
            begin_metadata_index_scan(queue.db, fingerprint, initial_cohort_seeded=False, now=utc_now())
            _ensure_incremental_chart_reuse(queue, source, cohort_row[0])
            return
        # Retain the existing small-cohort initialization. Larger populations
        # use the same durable bounded scanner instead of stopping or silently
        # treating the first 100,000 heads as the complete baseline.
        candidates = queue.db.execute("SELECT id,parquet_path,body_sha256 FROM articles INDEXED BY host_state "
                                      "WHERE host='m.cnyes.com' LIMIT ?",
                                      (METADATA_INITIAL_COHORT_LIMIT + 1,)).fetchall()
        if len(candidates) > METADATA_INITIAL_COHORT_LIMIT:
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
                             (marker, json.dumps({'heads': None, 'observed_at_utc': utc_now(),
                                                  'initial_index_mode': 'incremental_scan',
                                                  'http_requests': 0, 'all_history_complete': False}, sort_keys=True)))
            begin_metadata_index_scan(queue.db, fingerprint, initial_cohort_seeded=False, now=utc_now())
            _ensure_incremental_chart_reuse(queue, source)
            return
        heads = [row for row in candidates if row['body_sha256']]
        # A digest without a path cannot be replayed. Defer its evidence to the
        # bounded scanner instead of failing the whole initial transaction.
        invalid_heads = any(not r['parquet_path'] for r in heads)
        queue.db.executemany('INSERT OR IGNORE INTO retained_metadata_replays '
                             '(source_fingerprint,article_id,original_path,original_body_sha256) VALUES (?,?,?,?)',
                             ((fingerprint, r['id'], r['parquet_path'], r['body_sha256'])
                              for r in heads if r['parquet_path']))
        inherited = _inherit_unchanged_chart_checkpoints(queue, source)
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
                         (marker, json.dumps({'heads': len(heads), 'observed_at_utc': utc_now(),
                                              'unchanged_prior_checkpoints': inherited,
                                              'invalid_heads_need_scan': invalid_heads,
                                              'http_requests': 0, 'all_history_complete': False}, sort_keys=True)))
        begin_metadata_index_scan(queue.db, fingerprint, initial_cohort_seeded=not invalid_heads, now=utc_now())
        _ensure_incremental_chart_reuse(queue, source)


def _checkpoint_batch(queue, fingerprint, checkpoints, *, clear_retry_path=None,
                      resource_wait=None, clear_resource_path=None):
    """Commit bounded local outcomes together, never body acceptance.

    The caller has at most 64 heads. A crash before this commit repeats their
    guards; a crash after immutable acceptance is still recovered by the
    changed head. SQLite failure rolls back this whole local checkpoint group.
    """
    if not checkpoints and clear_retry_path is None and resource_wait is None and clear_resource_path is None:
        return
    with queue.db:
        if checkpoints:
            queue.db.executemany('UPDATE retained_metadata_replays SET state=?,error_code=?,checked_at_utc=? '
                                'WHERE source_fingerprint=? AND article_id=?',
                                ((state, error, utc_now(), fingerprint, key) for key, state, error in checkpoints))
        if clear_retry_path is not None:
            queue.db.execute('DELETE FROM retained_metadata_local_retries '
                             'WHERE source_fingerprint=? AND original_path=?', (fingerprint, clear_retry_path))
        if resource_wait is not None:
            relative, required, phase = resource_wait
            queue.db.execute('''INSERT INTO retained_metadata_resource_waits VALUES (?,?,?,?,?)
                ON CONFLICT(source_fingerprint,original_path) DO UPDATE SET
                    required_bytes=excluded.required_bytes,phase=excluded.phase,
                    observed_at_utc=excluded.observed_at_utc''',
                (fingerprint, relative, required, phase, utc_now()))
        if clear_resource_path is not None:
            queue.db.execute('DELETE FROM retained_metadata_resource_waits '
                             'WHERE source_fingerprint=? AND original_path=?', (fingerprint, clear_resource_path))
    if checkpoints:
        queue.metrics.count('cnyes', 'retained_metadata_checkpoint_rows', len(checkpoints))
        queue.metrics.count('cnyes', 'retained_metadata_checkpoint_transactions')


def retained_metadata_next_wake_delay(queue, maximum):
    """Bound an idle wake for local work without granting any HTTP request."""
    if type(maximum) not in (int, float) or not math.isfinite(maximum) or maximum < 0:
        raise ValueError('invalid_retained_metadata_wake_bound')
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.request_enabled:
        return maximum
    tables = {row[0] for row in queue.db.execute("SELECT name FROM sqlite_master WHERE type='table' "
        "AND name IN ('retained_metadata_replays','retained_metadata_local_retries','retained_metadata_resource_waits',"
        "'retained_metadata_incremental_scans','retained_metadata_large_retries')")}
    if not {'retained_metadata_replays', 'retained_metadata_local_retries'} <= tables:
        return maximum
    fp, now = source.data_fingerprint, time.time()
    resource_filter = (''' AND NOT EXISTS(SELECT 1 FROM retained_metadata_resource_waits w
        WHERE w.source_fingerprint=r.source_fingerprint AND w.original_path=r.original_path
            AND w.required_bytes>?)''' if 'retained_metadata_resource_waits' in tables else '')
    resource_parameters = (METADATA_REPLAY_MAX_BYTES,) if resource_filter else ()
    def ready(state, extra='', parameters=()):
        return queue.db.execute('''SELECT 1 FROM retained_metadata_replays r
        LEFT JOIN retained_metadata_local_retries l ON l.source_fingerprint=r.source_fingerprint
            AND l.original_path=r.original_path
        WHERE r.source_fingerprint=? AND r.state=?
            AND (l.not_before_epoch IS NULL OR l.not_before_epoch<=?) '''
            + resource_filter + ' ' + extra + ' LIMIT 1',
            (fp, state, now, *resource_parameters, *parameters)).fetchone()

    pending = ready('pending')
    if not pending and 'retained_metadata_resource_waits' in tables:
        pending = ready('resource_wait')
    if not pending:
        # Older versions treated the local byte bound as a permanent failed
        # validation. Revisit only that exact outcome under the current bound.
        pending = ready('failed_validation', 'AND r.error_code=?', ('retained_metadata_batch_exceeds_resource_bound',))
    deferred = None if pending else queue.db.execute('''SELECT 1 FROM retained_metadata_replays r
        JOIN articles a ON a.id=r.article_id
        LEFT JOIN retained_metadata_local_retries l ON l.source_fingerprint=r.source_fingerprint
            AND l.original_path=r.original_path
        WHERE r.source_fingerprint=? AND r.state='deferred' AND a.state='complete'
            AND a.lease_owner IS NULL AND a.lease_until IS NULL
            AND (l.not_before_epoch IS NULL OR l.not_before_epoch<=?)'''
            + resource_filter + ' LIMIT 1', (fp, now, *resource_parameters)).fetchone()
    delay = min(maximum, METADATA_REPLAY_POLL_SECONDS) if pending or deferred else maximum
    if 'retained_metadata_incremental_scans' in tables:
        scanning = queue.db.execute('''SELECT 1 FROM retained_metadata_incremental_scans
            WHERE source_fingerprint=? AND state IN ('scanning','draining')''', (fp,)).fetchone()
        if scanning:
            delay = min(delay, METADATA_INDEX_IDLE_SECONDS)
    # Begin with the small retry-path index; the ordinary metadata cohort can
    # contain tens of thousands of pending heads. SQLite's default join order
    # otherwise scans every pending head to find one local retry deadline.
    resource_retry = " OR r.state='resource_wait'" if resource_filter else ''
    retry = queue.db.execute('''SELECT min(l.not_before_epoch) FROM retained_metadata_local_retries l
        CROSS JOIN retained_metadata_replays r CROSS JOIN articles a
        WHERE l.source_fingerprint=? AND r.source_fingerprint=l.source_fingerprint
            AND r.original_path=l.original_path AND (r.state IN ('pending','deferred')
                OR (r.state='failed_validation' AND r.error_code='retained_metadata_batch_exceeds_resource_bound')'''
            + resource_retry + ''')
            AND a.id=r.article_id AND a.state='complete'
            AND a.lease_owner IS NULL AND a.lease_until IS NULL''' + resource_filter,
            (fp, *resource_parameters)).fetchone()
    # A due retry may still miss the current wall-time slice while the owner is
    # busy. Keep a bounded idle wait instead of spinning at a zero deadline.
    if retry and retry[0] is not None:
        delay = min(delay, max(1.0, retry[0] - now))
    if 'retained_metadata_large_retries' in tables:
        from downloader.news_fulltext_large_replay_owner import next_wake_delay as large_wake_delay
        delay = large_wake_delay(queue, fp, delay, METADATA_REPLAY_MAX_BYTES)
    return delay


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


def _selected_batch_records(path, batch, metadata, selected):
    """Decode only requested Python records while preserving shared envelopes.

    Parquet's dictionary-encoded byte arrays can expand far beyond the footer's
    uncompressed column sizes. For a single head, stream until its last needed
    envelope; for multiple heads retain Arrow dictionaries and convert only the
    selected rows and referenced raw responses to Python. This reduces
    allocation but is not a hard process-memory guarantee.
    """
    if batch.metadata.num_rows != len(metadata):
        raise ValueError('retained_metadata_batch_grain_invalid')
    selected_ids = {head['id'].hex() for head in selected}
    positions = defaultdict(list)
    refs = set()
    for index, row in enumerate(metadata):
        article_id = row['article_id']
        positions[article_id].append(index)
        if article_id in selected_ids and isinstance(row.get('source_response_article_id'), str):
            refs.add(row['source_response_article_id'])
    if len(selected_ids) == 1:
        selected_id = next(iter(selected_ids))
        selected_index = positions[selected_id][-1]
        envelope_positions = {index for ref in refs for index in positions.get(ref, ())}
        last_index = max({selected_index, *envelope_positions})
        indexed, envelopes, last_seen = {}, {}, -1
        for index, row_batch in enumerate(batch.iter_batches(batch_size=1, use_threads=False)):
            last_seen = index
            record = None
            if index == selected_index:
                record = row_batch.to_pylist()[0]
                if record['article_id'] != selected_id:
                    raise ValueError('retained_metadata_batch_head_not_unique')
                indexed[selected_id] = record
            if index in envelope_positions:
                ref = metadata[index]['article_id']
                if row_batch.column(row_batch.schema.get_field_index('article_id'))[0].as_py() != ref:
                    raise ValueError('retained_metadata_batch_head_not_unique')
                raw_index = row_batch.schema.get_field_index('raw_response_body')
                value = (record.get('raw_response_body') if record is not None
                         else row_batch.column(raw_index)[0].as_py() if raw_index >= 0 else None)
                if value is not None:
                    envelopes[ref] = value
            if index >= last_index:
                break
        if last_seen < last_index:
            raise ValueError('retained_metadata_batch_grain_invalid')
        return indexed, envelopes
    dictionary_columns = [field.name for field in batch.schema_arrow
                          if pa.types.is_string(field.type) or pa.types.is_binary(field.type)
                          or pa.types.is_large_string(field.type) or pa.types.is_large_binary(field.type)]
    reader = pq.ParquetFile(path, read_dictionary=dictionary_columns)
    table = reader.read(use_threads=False)
    if table.num_rows != len(metadata):
        raise ValueError('retained_metadata_batch_grain_invalid')
    indexed = {}
    for article_id in selected_ids:
        index = positions[article_id][-1]
        row = table.slice(index, 1).to_pylist()[0]
        if row['article_id'] != article_id:
            raise ValueError('retained_metadata_batch_head_not_unique')
        indexed[article_id] = row
    envelopes = {}
    if 'raw_response_body' in table.column_names:
        raw = table.column('raw_response_body')
        ids = table.column('article_id')
        for ref in refs:
            # The original whole-batch mapping kept the last non-null envelope
            # for an ID. Preserve that behavior even with duplicate IDs.
            for index in reversed(positions.get(ref, ())):
                if ids[index].as_py() != ref:
                    raise ValueError('retained_metadata_batch_head_not_unique')
                value = (indexed[ref].get('raw_response_body') if ref in indexed and index == positions[ref][-1]
                         else raw[index].as_py())
                if value is not None:
                    envelopes[ref] = value
                    break
    return indexed, envelopes


def replay_retained_cnyes_metadata(queue, *, maximum_heads=32, maximum_wall_seconds=0.25,
                                   maximum_bytes=METADATA_REPLAY_MAX_BYTES, stop=None, target_article_ids=None):
    """Bound each maintenance quantum; failed source proofs do not block peers.

    Immutable acceptance and checkpoint are separate crash-safe commits. A
    crash after acceptance is recognized by the changed head on the next tick.
    No HTTP request or HTTP attempt/cooldown reset occurs here.
    A bounded explicit ID set repairs an observed source-format cohort through
    these same guards, without reparsing unrelated history first. Resident
    maintenance leaves it unset and advances its durable current-head index.
    """
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed:
        return {'heads_checked': 0, 'repaired_versions': 0, 'http_requests': 0}
    if (type(maximum_heads) is not int or not 1 <= maximum_heads <= 64
            or type(maximum_wall_seconds) not in (int, float) or not 0 < maximum_wall_seconds <= 1
            or type(maximum_bytes) is not int or not 1 <= maximum_bytes <= METADATA_REPLAY_MAX_BYTES):
        raise ValueError('unbounded retained metadata replay')
    if target_article_ids is not None and (
            not isinstance(target_article_ids, (list, tuple)) or not 1 <= len(target_article_ids) <= maximum_heads
            or any(not isinstance(key, bytes) or len(key) != 32 for key in target_article_ids)
            or len(set(target_article_ids)) != len(target_article_ids)):
        raise ValueError('invalid bounded retained metadata target IDs')
    started = time.monotonic()
    totals = Counter(heads_checked=0, repaired_versions=0, batches_read=0, raw_bytes_checked=0,
                     local_io_deferrals=0)
    with queue.metrics.measure('retained_metadata_replay', 'cnyes'):
        _ensure_cohort(queue, source)
        fp = source.data_fingerprint
        large_polled_at_start = bool(getattr(queue, '_large_metadata_job', None))
        if large_polled_at_start:
            from downloader.news_fulltext_large_replay_owner import poll_or_start
            checked, repaired = poll_or_start(queue, source, METADATA_REPLAY_MAX_BYTES, stop=stop, start=False)
            totals['heads_checked'] += checked
            totals['repaired_versions'] += repaired
        if (target_article_ids is None and not (stop and stop.is_set())
                and time.monotonic() - started < maximum_wall_seconds):
            index_result = advance_metadata_index_scan(
                queue.db, fp, now=utc_now(), prior_fingerprint=source.parser_repair_prior_capture_fingerprint)
            for key, value in index_result.items():
                totals['incremental_' + key] += value
        # Deferred rows are reconsidered using indexed head state, never by
        # repeatedly parsing their HTML while a request/denial remains active.
        target_clause = '' if target_article_ids is None else ' AND r.article_id IN (' + ','.join('?' for _ in target_article_ids) + ')'
        targets = () if target_article_ids is None else tuple(target_article_ids)
        retry_filter = ''' AND NOT EXISTS(SELECT 1 FROM retained_metadata_local_retries l
            WHERE l.source_fingerprint=r.source_fingerprint AND l.original_path=r.original_path
                AND l.not_before_epoch>?)'''
        resource_filter = ''' AND NOT EXISTS(SELECT 1 FROM retained_metadata_resource_waits w
            WHERE w.source_fingerprint=r.source_fingerprint AND w.original_path=r.original_path
                AND w.required_bytes>?)'''
        pending = queue.db.execute('SELECT r.* FROM retained_metadata_replays r WHERE r.source_fingerprint=? '
                                   "AND r.state='pending'" + retry_filter + resource_filter + target_clause
                                   + ' ORDER BY r.original_path,r.article_id LIMIT ?',
                                   (fp, time.time(), maximum_bytes, *targets, maximum_heads)).fetchall()
        # Apply the byte bound before LIMIT. A large original must neither be
        # marked invalid nor occupy every selection ahead of eligible peers.
        resource = queue.db.execute('SELECT r.* FROM retained_metadata_replays r WHERE r.source_fingerprint=? '
                                    "AND r.state='resource_wait'" + retry_filter + resource_filter + target_clause
                                    + ' ORDER BY r.original_path,r.article_id LIMIT ?',
                                    (fp, time.time(), maximum_bytes, *targets, maximum_heads - len(pending))).fetchall()
        # Repair the old exact misclassification once it is genuinely retried;
        # other source, receipt and parser failures remain terminal evidence.
        legacy = queue.db.execute('SELECT r.* FROM retained_metadata_replays r WHERE r.source_fingerprint=? '
                                  "AND r.state='failed_validation' AND r.error_code='retained_metadata_batch_exceeds_resource_bound'"
                                  + retry_filter + resource_filter + target_clause + ' ORDER BY r.original_path,r.article_id LIMIT ?',
                                  (fp, time.time(), maximum_bytes, *targets, maximum_heads - len(pending) - len(resource))).fetchall()
        deferred = queue.db.execute('SELECT r.* FROM retained_metadata_replays r JOIN articles a ON a.id=r.article_id '
                                    "WHERE r.source_fingerprint=? AND r.state='deferred' AND a.state='complete' "
                                    'AND a.lease_owner IS NULL AND a.lease_until IS NULL '
                                    + retry_filter + resource_filter + target_clause
                                    + ' ORDER BY r.original_path,r.article_id LIMIT ?',
                                    (fp, time.time(), maximum_bytes, *targets,
                                     maximum_heads - len(pending) - len(resource) - len(legacy))).fetchall()
        groups, eligibility_checkpoints = defaultdict(list), []
        for job in [*pending, *resource, *legacy, *deferred]:
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
            retry_exists = queue.db.execute('SELECT 1 FROM retained_metadata_local_retries '
                                            'WHERE source_fingerprint=? AND original_path=?', (fp, relative)).fetchone()
            resource_exists = queue.db.execute('SELECT 1 FROM retained_metadata_resource_waits '
                                               'WHERE source_fingerprint=? AND original_path=?', (fp, relative)).fetchone()
            local_io_deferred = False
            resource_wait = None
            resource_blocked_heads = heads
            try:
                path = replay_source_path(queue.root, relative)
                compressed = path.stat().st_size
                if compressed > maximum_bytes:
                    raise _ResourceBudgetExceeded(compressed, 'compressed_file')
                receipt_row = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (relative,)).fetchone()
                if not receipt_row:
                    raise ValueError('retained_metadata_missing_acceptance_receipt')
                receipt_path = replay_source_path(queue.root, receipt_row[0])
                receipt = json.loads(receipt_path.read_text())
                if (receipt['contract_version'] != CONTRACT_VERSION or receipt['parquet_path'] != relative
                        or receipt['publish'] is not False or sha256_file(path) != receipt['sha256']):
                    raise ValueError('retained_metadata_batch_receipt_invalid')
                batch = pq.ParquetFile(path)
                if batch.metadata.num_rows != receipt['rows'] or batch.metadata.num_rows > 512:
                    raise ValueError('retained_metadata_batch_grain_invalid')
                metadata = batch.read(columns=['article_id', 'body_sha256', 'published_at_raw',
                                                'authors_json', 'body_completeness', 'final_url',
                                                'source_response_article_id'], use_threads=False).to_pylist()
                counts = Counter(r['article_id'] for r in metadata)
                indexed_meta = {r['article_id']: r for r in metadata}
                selected = []
                for head in heads:
                    try:
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
                    except (ValueError, KeyError, TypeError, NewsError) as error:
                        # A verified source batch can contain one bad head or
                        # metadata value. Do not poison other rows; whole-file
                        # receipt/read failures still fail the entire batch.
                        checkpoints.append((head['id'], 'failed_validation',
                                            error.code if isinstance(error, NewsError) else str(error)[:160]))
                        totals['heads_checked'] += 1
                        queue.metrics.count('cnyes', 'retained_metadata_validation_errors')
                if not selected:
                    continue
                # Bound materialization before decompressing response/body
                # columns, using the actual Arrow footer, not compressed size.
                # Metadata-only outcomes from this same batch are already
                # resolved and must not be relabeled as resource waits.
                resource_blocked_heads = selected
                uncompressed = sum(batch.metadata.row_group(g).column(c).total_uncompressed_size
                                   for g in range(batch.metadata.num_row_groups)
                                   for c in range(batch.metadata.num_columns))
                if uncompressed > maximum_bytes:
                    raise _ResourceBudgetExceeded(uncompressed, 'arrow_uncompressed')
                indexed, envelopes = _selected_batch_records(path, batch, metadata, selected)
                totals['batches_read'] += 1
                receipt_digest = sha256_file(receipt_path)
                attempted_in_batch = 0
                for head in selected:
                    # Reading/verifying a batch is an indivisible operation and
                    # can itself exceed the soft wall slice. Once started,
                    # finish one head so repeated slow reads cannot starve it.
                    # A stop request still takes precedence.
                    if (stop and stop.is_set()) or (attempted_in_batch and
                            time.monotonic() - started >= maximum_wall_seconds):
                        break
                    attempted_in_batch += 1
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
            except _ResourceBudgetExceeded as error:
                # This is a local budget observation, not a source failure.
                # The wait and these heads' checkpoints commit together.
                resource_wait = (relative, error.required_bytes, error.phase)
                checkpoints.extend((head['id'], 'resource_wait', 'retained_metadata_batch_exceeds_resource_bound')
                                   for head in resource_blocked_heads)
                queue.metrics.count('cnyes', 'retained_metadata_resource_wait_heads', len(resource_blocked_heads))
                continue
            except OSError:
                # A local mount/read outage is not a failed publisher proof.
                # Persist one deadline per original batch, so its heads cannot
                # monopolize the bounded pending selection after a restart.
                local_io_deferred = True
                with queue.db:
                    previous = queue.db.execute('SELECT local_attempts FROM retained_metadata_local_retries '
                                                'WHERE source_fingerprint=? AND original_path=?', (fp, relative)).fetchone()
                    attempts = (previous[0] if previous else 0) + 1
                    delay = min(METADATA_LOCAL_RETRY_MAX_SECONDS, 2 ** min(attempts - 1, 9))
                    queue.db.execute('''INSERT INTO retained_metadata_local_retries VALUES (?,?,?,?,?,?)
                        ON CONFLICT(source_fingerprint,original_path) DO UPDATE SET
                            not_before_epoch=max(retained_metadata_local_retries.not_before_epoch,excluded.not_before_epoch),
                            local_attempts=excluded.local_attempts,last_attempt_at_utc=excluded.last_attempt_at_utc,
                            last_error=excluded.last_error''',
                        (fp, relative, time.time() + delay, attempts, utc_now(), 'retained_metadata_local_io_unavailable'))
                    queue.counter('retained_metadata_local_io_deferrals', 1)
                totals['local_io_deferrals'] += 1
                queue.metrics.count('cnyes', 'retained_metadata_local_io_deferrals')
                continue
            except (ValueError, KeyError, TypeError, pa.ArrowException) as error:
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
                _checkpoint_batch(queue, fp, checkpoints,
                                  clear_retry_path=relative if retry_exists and not local_io_deferred else None,
                                  resource_wait=resource_wait,
                                  clear_resource_path=relative if resource_exists and resource_wait is None
                                      and not local_io_deferred else None)
            if eligible:
                # Storage/commit failures propagate, leaving the cohort pending.
                # The canonical owner restart recovers durable receipts first.
                repaired = queue.accept(eligible)
                _checkpoint_batch(queue, fp, [(bytes.fromhex(record['article_id']), 'verified_repair', None)
                                              for record in eligible])
                totals['repaired_versions'] += repaired
        # A completed large proof may already have spent this slice on one
        # parent validation/commit. Leave another proof for the next cycle so
        # grouped results do not extend an expired slice a second time. When
        # no large job existed initially, still allow starting one even if
        # earlier local work used the slice, avoiding large-head starvation.
        if (maximum_bytes == METADATA_REPLAY_MAX_BYTES and not (stop and stop.is_set())
                and (not large_polled_at_start or time.monotonic() - started < maximum_wall_seconds)):
            from downloader.news_fulltext_large_replay_owner import poll_or_start
            checked, repaired = poll_or_start(queue, source, METADATA_REPLAY_MAX_BYTES, stop=stop,
                                             targets=target_article_ids)
            totals['heads_checked'] += checked
            totals['repaired_versions'] += repaired
        queue.metrics.count('cnyes', 'retained_metadata_heads_checked', totals['heads_checked'])
        queue.metrics.count('cnyes', 'retained_metadata_repaired_versions', totals['repaired_versions'])
    return {**totals, 'http_requests': 0, 'all_history_complete': False,
            'local_retry_contract_version': METADATA_LOCAL_RETRY_CONTRACT_VERSION,
            'resource_wait_contract_version': METADATA_RESOURCE_WAIT_CONTRACT_VERSION,
            'wall_seconds': time.monotonic() - started}
