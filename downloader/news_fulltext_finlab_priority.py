"""Offer newly observed FinLab CNYES URLs to the existing metadata mailbox.

This is an optional latency hint, not a replacement for the ordered receipt
import.  The comparison and bounded source-date lookup run outside the queue
writer, and cannot authorize a request or publish a source association.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import sqlite3
import threading
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_policy import NewsError, normalize_url
from downloader.news_fulltext_url_metadata import (
    FINLAB_CNYES_HINT_KIND, FINLAB_CNYES_HINT_VERSION,
    MAX_FINLAB_CNYES_RAW_HINTS, MAX_TARGETS, _FinlabRowVerifier,
    _finlab_file_identity as _stamp, _read_finlab_hint_source_rows, _source_identity, _submit,
)

_DATASET = 'tw_news_cnyes'
_PREFIX = 'finlab_cnyes_delta_hint_v1:'
_LATEST = 'finlab_cnyes_delta_hint_latest_v1'
_MAX_RAW_HINTS = MAX_FINLAB_CNYES_RAW_HINTS
_MIN_REMAINING_ROWS = 4 * 4096


def _update_latest(queue, origin_id, serialized):
    latest = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (_LATEST,)).fetchone()
    try:
        latest_id = json.loads(latest[0]).get('origin_id') if latest else None
    except (ValueError, TypeError, AttributeError):
        latest_id = None
    if type(latest_id) is int and latest_id > 0:
        original = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                                    (_PREFIX + str(latest_id),)).fetchone()
        if not original or original[0] != latest[0]:
            latest_id = None
    else:
        latest_id = None
    # Receipt generations use increasing origin IDs. A late older request
    # must not replace an already recorded newer hint.
    if type(latest_id) is not int or latest_id <= origin_id:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (_LATEST, serialized))


def _url_column(path, expected_rows):
    file = pq.ParquetFile(path)
    try:
        if file.metadata.num_rows != expected_rows or 'url' not in file.schema_arrow.names:
            raise ValueError('priority_input_schema_or_rows_mismatch')
        column = file.read(columns=['url']).column(0)
        typ = column.type.value_type if pa.types.is_dictionary(column.type) else column.type
        if not (pa.types.is_string(typ) or pa.types.is_large_string(typ)):
            raise ValueError('priority_input_url_not_string')
        return column.cast(pa.string())
    finally:
        file.close()


def _compare(old, old_sha, old_rows, current, current_stamp, current_rows, cancelled):
    """Return bounded hints; the ordinary importer still consumes every row."""
    old_stamp = _stamp(old)
    if cancelled.is_set() or sha256_file(old) != old_sha or _stamp(old) != old_stamp:
        raise ValueError('priority_baseline_changed')
    before = _url_column(old, old_rows)
    if cancelled.is_set():
        return None
    after = _url_column(current, current_rows)
    if cancelled.is_set():
        return None
    # Arrow can expose Parquet dictionary columns; cast both sides first.
    new_mask = pc.invert(pc.is_in(after, value_set=pc.unique(before)))
    changed = pc.filter(after, new_mask)
    added = pc.unique(pc.drop_null(changed))
    hints = added.slice(0, _MAX_RAW_HINTS)
    indices = pc.take(pc.indices_nonzero(new_mask), pc.index_in(hints, value_set=changed)).to_pylist()
    raw_hints = hints.to_pylist()
    # Read URL and optional date together once per selected row group. The
    # row's own URL is checked against the delta hint, rather than assuming
    # the index computed by Arrow names the intended source row.
    source_rows = (_read_finlab_hint_source_rows(current, list(current_stamp), current_rows, indices)
                   if indices and not cancelled.is_set() else ())
    if isinstance(source_rows, tuple) and len(source_rows) == len(indices):
        dates = [row[1] if row is not None else '' for row in source_rows]
        proofs = [(row[0], row[1], index) if row is not None else None
                  for row, index in zip(source_rows, indices, strict=True)]
    else:
        dates, proofs = [''] * len(indices), [None] * len(indices)
    if _stamp(old) != old_stamp or _stamp(current) != current_stamp or cancelled.is_set():
        raise ValueError('priority_input_changed')
    return {'raw_unique_delta': len(added), 'raw_hints': raw_hints, 'raw_dates': dates,
            'raw_indices': indices, 'source_row_proofs': tuple(proofs),
            'old_stamp': old_stamp, 'current_stamp': current_stamp}


class FinlabCnyesPriority:
    """A speculative local comparison, polled by the one existing owner."""

    def __init__(self, reader, old_id, old, old_sha, old_rows, created_at):
        self.reader, self.old_id, self.old, self.old_sha = reader, old_id, old, old_sha
        self.created_at = created_at
        self.cancelled = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='news-cnyes-url-delta')
        self.future = self.pool.submit(_compare, old, old_sha, old_rows, reader.parquet,
                                       _stamp(reader.parquet), reader.expected_rows, self.cancelled)
        self.retry_at = 0.0
        self.done = False

    @classmethod
    def start(cls, reader, root, receipt, provider):
        queue = reader.queue
        if (provider != 'finlab' or receipt.get('dataset') != _DATASET or reader.link_column != 'url'
                or reader.expected_rows - reader.processed_rows < _MIN_REMAINING_ROWS
                or queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
                                    (_PREFIX + str(reader.origin_id),)).fetchone()):
            return None
        running = getattr(queue, '_finlab_cnyes_priority_future', None)
        if running is not None and not running.done():
            # A replaced receipt may cancel an earlier comparison while its
            # Arrow read is still unwinding. Do not start redundant readers.
            queue.metrics.count('cnyes', 'finlab_delta_hint_previous_reader_busy')
            return None
        source = queue.registry.sources.get('cnyes')
        if source is None or not source.allowed:
            return None
        selected = queue.db.execute('SELECT origin_id FROM metadata_current_origins WHERE receipt_path=?',
                                    (reader.path,)).fetchone()
        query = ('SELECT o.id,o.parquet_path,o.sha256,o.rows FROM origins o '
                 'LEFT JOIN metadata_ingest_progress p ON p.origin_id=o.id '
                 "WHERE o.receipt_path=? AND o.provider='finlab' AND o.id!=? "
                 "AND (p.origin_id IS NULL OR p.state='complete') ")
        params = [reader.path, reader.origin_id]
        if selected:
            query += 'AND o.id=? '
            params.append(selected[0])
        baseline = queue.db.execute(query + 'ORDER BY o.id DESC LIMIT 1', params).fetchone()
        if not baseline or baseline[2] == reader.origin_key.rsplit(':', 1)[-1]:
            return None
        old = Path(baseline[1]).resolve(strict=True)
        if not old.is_relative_to(Path(root).resolve(strict=True)):
            return None
        created = queue.db.execute('SELECT imported_at_utc FROM origins WHERE id=?', (reader.origin_id,)).fetchone()
        if not created:
            return None
        hint = cls(reader, baseline[0], old, baseline[2], baseline[3], created[0])
        queue._finlab_cnyes_priority_future = hint.future
        return hint

    def poll(self):
        if self.done or not self.future.done() or time.monotonic() < self.retry_at:
            return
        reader, queue = self.reader, self.reader.queue
        try:
            result = self.future.result()
        except Exception:
            # A bad baseline is only a missed hint. The ordered import is
            # still responsible for reporting its own current-input errors.
            queue.metrics.count('cnyes', 'finlab_delta_hint_unavailable')
            self.done = True
            return
        try:
            if result is None or not reader.unchanged() or _stamp(self.old) != result['old_stamp']:
                self.done = True
                return
            if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
                                (_PREFIX + str(reader.origin_id),)).fetchone():
                # The owner may already have recovered the marker after a
                # mailbox write succeeded but this producer was interrupted.
                self.done = True
                return
            targets, verified, seen = [], [], set()
            for raw, date, row_index, proof in zip(result['raw_hints'], result['raw_dates'],
                                                   result['raw_indices'], result['source_row_proofs'], strict=True):
                try:
                    url = normalize_url(raw)
                except (NewsError, TypeError, ValueError):
                    continue
                identity = _source_identity(queue.registry, url)
                if url in seen or identity['provider'] != 'cnyes' or not identity['allowed']:
                    continue
                seen.add(url)
                targets.append({'url': url, 'source': identity, 'source_date': date, 'source_row_index': row_index})
                verified.append(proof == (url, date, row_index))
                if len(targets) == MAX_TARGETS:
                    break
            if not reader.unchanged() or _stamp(self.old) != result['old_stamp']:
                self.done = True
                return
            submission = (_submit(queue.root, {'contract_version': FINLAB_CNYES_HINT_VERSION,
                          'kind': FINLAB_CNYES_HINT_KIND,
                          'created_at_utc': self.created_at,
                          'source_origin': {'origin_id': reader.origin_id,
                              'sha256': reader.origin_key.rsplit(':', 1)[-1],
                              'expected_rows': reader.expected_rows,
                              'file_identity': list(result['current_stamp'])},
                          'priority_evidence': {'baseline_origin_id': self.old_id,
                              'baseline_sha256': self.old_sha,
                              'raw_unique_delta': result['raw_unique_delta'],
                              'raw_hints_examined': len(result['raw_hints'])},
                          'targets': targets}) if targets else None)
            if submission:
                try:
                    verifier = getattr(queue, '_finlab_hint_row_verifier', None)
                    if verifier is None:
                        verifier = queue._finlab_hint_row_verifier = _FinlabRowVerifier()
                    verifier.prime(submission['request_id'], tuple(verified))
                except (OSError, RuntimeError):
                    # The durable hint can still be independently verified by
                    # the owner, or admitted without a date on failure.
                    queue.metrics.count('cnyes', 'finlab_row_verifier_unavailable')
            from downloader.news_fulltext_queue import utc_now
            record = {'contract': 'finlab_cnyes_delta_hint_v1', 'origin_id': reader.origin_id,
                      'source_sha256': reader.origin_key.rsplit(':', 1)[-1], 'baseline_origin_id': self.old_id,
                      'baseline_sha256': self.old_sha, 'raw_unique_delta': result['raw_unique_delta'],
                      'raw_hints_examined': len(result['raw_hints']), 'targets': len(targets),
                      'request_id': submission['request_id'] if submission else None,
                      'recorded_at_utc': utc_now(),
                      'source_associations_complete': False, 'body_verified': False}
            with queue.db:
                serialized = json.dumps(record, sort_keys=True)
                queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)',
                                 (_PREFIX + str(reader.origin_id), serialized))
                # A single exact-key pointer permits bounded status reads even
                # after restart; the per-origin marker remains authoritative.
                serialized = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                                              (_PREFIX + str(reader.origin_id),)).fetchone()[0]
                _update_latest(queue, reader.origin_id, serialized)
            if submission:
                queue._url_metadata_continuation_pending = True
            queue.metrics.count('cnyes', 'finlab_delta_hint_targets', len(targets))
            self.done = True
        except (OSError, ValueError, TypeError, KeyError, sqlite3.OperationalError, NewsError):
            # An unavailable mailbox or old input is never allowed to block
            # the complete ordered receipt import. A stable request has a
            # deterministic ID even if interrupted before the SQL marker.
            self.retry_at = time.monotonic() + 2
            queue.metrics.count('cnyes', 'finlab_delta_hint_deferred')

    def close(self):
        self.cancelled.set()
        self.future.cancel()
        self.pool.shutdown(wait=False, cancel_futures=True)


def recover_from_bound_request(queue, request, request_id, source_record):
    """Recover a missing progress marker in the owner's admission transaction.

    A request may outlive its producer or even a replacement receipt. The
    original and baseline registrations must still match the sealed request;
    neither this marker nor the mailbox receipt proves body acquisition.
    """
    origin, evidence = request['source_origin'], request['priority_evidence']
    key = _PREFIX + str(origin['origin_id'])
    if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (key,)).fetchone():
        return
    baseline = queue.db.execute('SELECT provider,sha256,receipt_path FROM origins WHERE id=?',
                                (evidence['baseline_origin_id'],)).fetchone()
    if (not baseline or baseline[0] != 'finlab' or baseline[1] != evidence['baseline_sha256']
            or baseline[2] != source_record[6]):
        return
    from downloader.news_fulltext_queue import utc_now
    record = {'contract': 'finlab_cnyes_delta_hint_v1', 'origin_id': origin['origin_id'],
              'source_sha256': origin['sha256'], 'baseline_origin_id': evidence['baseline_origin_id'],
              'baseline_sha256': evidence['baseline_sha256'], 'raw_unique_delta': evidence['raw_unique_delta'],
              'raw_hints_examined': evidence['raw_hints_examined'], 'targets': len(request['targets']),
              'request_id': request_id, 'recorded_at_utc': utc_now(),
              'source_associations_complete': False, 'body_verified': False,
              'recovered_from_owner_request': True}
    serialized = json.dumps(record, sort_keys=True)
    queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)', (key, serialized))
    serialized = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0]
    _update_latest(queue, origin['origin_id'], serialized)


def priority_progress(queue):
    """Read the latest hint and exact owner receipt, without exposing URLs.

    A mailbox write is not an owner admission. Missing or inconsistent job
    evidence therefore remains unknown; no publisher or body work is implied.
    """
    try:
        row = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (_LATEST,)).fetchone()
        if not row:
            return None
        record = json.loads(row[0])
        if (not isinstance(record, dict) or record.get('contract') != 'finlab_cnyes_delta_hint_v1'
                or record.get('body_verified') is not False or record.get('source_associations_complete') is not False):
            return None
        origin_id = record.get('origin_id')
        if type(origin_id) is not int or origin_id <= 0:
            return None
        original = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                                    (_PREFIX + str(origin_id),)).fetchone()
        if not original or original[0] != row[0]:
            return None
        recorded = record.get('recorded_at_utc')
        if not isinstance(recorded, str) or len(recorded) > 64:
            return None
        clock = datetime.fromisoformat(recorded.replace('Z', '+00:00'))
        if clock.tzinfo is None or clock.utcoffset() != timedelta(0):
            return None
        delta, examined, targets = (record.get(k) for k in ('raw_unique_delta', 'raw_hints_examined', 'targets'))
        if (any(type(x) is not int or not 0 <= x < 2**53 for x in (delta, examined, targets))
                or examined != min(delta, _MAX_RAW_HINTS) or targets > min(examined, MAX_TARGETS)):
            return None
        request_id = record.get('request_id')
        if ((targets and (not isinstance(request_id, str) or not re.fullmatch(r'[0-9a-f]{64}', request_id)))
                or (not targets and request_id is not None)):
            return None
        outcomes = {name: 0 for name in ('metadata_registered', 'existing_head_preserved',
                                          'source_contract_changed', 'other')}
        state, checked = ('not_applicable', 0) if not targets else ('unobserved', None)
        unverified_origin = 0 if not targets else None
        if targets and queue.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='url_metadata_jobs'").fetchone():
            job = queue.db.execute('SELECT targets,next_index,completed_at_utc FROM url_metadata_jobs WHERE request_id=?',
                                   (request_id,)).fetchone()
            if job:
                state = 'unknown'
                total, cursor, completed = job
                if (type(total) is int and type(cursor) is int and total == targets and 0 <= cursor <= total
                        and ((completed is None and cursor < total) or (isinstance(completed, str) and cursor == total))):
                    counts = queue.db.execute('SELECT outcome,count(*) FROM url_metadata_items WHERE request_id=? GROUP BY outcome',
                                              (request_id,)).fetchall()
                    if all(isinstance(name, str) and type(count) is int and count >= 0 for name, count in counts):
                        for name, count in counts:
                            outcomes[name if name in outcomes else 'other'] += count
                        if sum(outcomes.values()) == cursor:
                            state, checked = ('checked' if completed is not None else 'processing'), cursor
                            unverified_origin = sum(count for name, count in counts
                                                    if name == 'metadata_registered_unverified_origin')
        return {'contract_version': 1, 'raw_unique_delta': delta, 'raw_hints_examined': examined,
                'targets': targets, 'request_state': state, 'owner_checked': checked,
                'outcomes': outcomes if checked is not None else None,
                'unverified_origin': unverified_origin,
                'recorded_at_utc': clock.isoformat(),
                'body_completion_proven': False, 'association_completion_proven': False}
    except (OSError, ValueError, TypeError, KeyError, OverflowError, sqlite3.Error, RecursionError):
        return None

