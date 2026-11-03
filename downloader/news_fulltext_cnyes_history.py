"""Durable Anue list evidence, distinct from article-body acceptance.

Retain exact request/response clocks and source IDs before closing a partition
gap. Equal effective dates alone never prove equal results: an old scope may
reuse a current list only when *every* retained page ID hash and total match.
No source raw envelope, article, byline, failure or retry clock is relabelled.
"""
from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
import marshal
import time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_bulk import (
    BULK_RESTRICTED_ITEM_ERRORS, CNYES_QUERY_DAY_OFFSET_SECONDS, cnyes_pagination,
    cnyes_scope, cnyes_url, wordpress_base, wordpress_page,
)
from downloader.news_fulltext_policy import CONTRACT_VERSION as NEWS_CONTRACT_VERSION, NewsError, cnyes_bulk_fingerprint


CONTRACT_VERSION = 1
CONSISTENCY_RECHECK_CONTRACT_VERSION = 2
CONSISTENCY_RECHECK_MAX_FACTOR = 16
COMPLETE_STATES = ('list_complete', 'list_complete_by_page_ID_equivalence', 'partition_list_complete')
REPAIR_STATES = ('waiting_page_evidence', 'inconsistent_source_snapshot', 'invalid_page_evidence',
                 'inconsistent_original_page_ID_reference')
REPAIR_CURSOR = 'cnyes_history_incomplete_scope_cursor_v1'
# Merge the existing index's per-state ordered seeks. An IN query with one
# global ORDER BY requires SQLite to materialize a temporary sort on each
# cursor slice; each scope has exactly one state, so UNION ALL is equivalent.
_REPAIR_SCOPE_BRANCH = ('SELECT scope_url FROM cnyes_history_scope_heads WHERE provider=? '
    'AND evidence_contract_version=? AND source_bulk_fingerprint=? AND authorization_fingerprint=? '
    'AND state=? AND scope_url>?')
REPAIR_SCOPE_SEEK = ' UNION ALL '.join([_REPAIR_SCOPE_BRANCH] * len(REPAIR_STATES)) + ' ORDER BY scope_url LIMIT ?'
INTEGRITY_INTERVAL_SECONDS = 3600
# A clean original-file audit must recur: later heads or local file damage are
# not covered by the preceding snapshot. Keep the full byte/receipt scan daily;
# unavailable originals retain the existing shorter local retry interval.
ORIGINAL_ENVELOPE_REAUDIT_SECONDS = 24 * 60 * 60
INTEGRITY_STATES = COMPLETE_STATES + ('invalid_scope_evidence',)
_INTEGRITY_SCOPE_BRANCH = ('SELECT scope_url FROM cnyes_history_scope_heads WHERE provider=? '
    'AND evidence_contract_version=? AND source_bulk_fingerprint=? AND authorization_fingerprint=? '
    'AND state=? AND scope_url>? AND scope_url<=?')
INTEGRITY_SCOPE_SEEK = ' UNION ALL '.join([_INTEGRITY_SCOPE_BRANCH] * len(INTEGRITY_STATES)) + ' ORDER BY scope_url LIMIT ?'
OVERFLOW_RETAINED_SCAN_VERSION = 1
OVERFLOW_RETAINED_SCAN_PREFIX = 'cnyes_history_overflow_retained_scan_v1:'
LEGACY_MISMATCH_SCAN_KEY = 'cnyes_history_retained_mismatch_scan_v2'
LEGACY_UNAVAILABLE_SCAN_KEY = 'cnyes_history_retained_unavailable_scan_v1'
LEGACY_EMPTY_SCAN_PREFIX = 'cnyes_history_unobserved_empty_scan_v1:'
LEGACY_NONZERO_SCAN_PREFIX = 'cnyes_history_unobserved_nonzero_scan_v1:'
# v2 also considers an exact retained history head after the discovery job
# has advanced to another response. Preserve v1 checkpoints as evidence;
# a previous completed v1 scan could have skipped such a batch.
ACCEPTED_BATCH_SCAN_PREFIX = 'cnyes_history_accepted_batch_scan_v2:'
ACCEPTED_INVALID_SCAN_PREFIX = 'cnyes_history_accepted_invalid_scan_v1:'
ACCEPTED_INVALID_CONFIRM_SECONDS = 2
ACCEPTED_INVALID_RECHECK_SECONDS = 3600
SCOPE_KEY_CACHE_SIZE = 4096
SCOPE_KEY_CACHE_MAX_URL_CHARS = 2048
PAGE_DIGEST_CACHE_SIZE = 64
PAGE_DIGEST_CACHE_MAX_JSON_CHARS = 8192
ORIGINAL_ENVELOPE_AUDIT_CACHE_SIZE = 64
ORIGINAL_ENVELOPE_CACHE_MAX_RAW_BYTES = 128 * 1024


class _CheckpointAuditConflict(Exception):
    """A pre-existing audit key does not contain the original cursor."""


def now():
    return datetime.now(timezone.utc).isoformat()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def _audit_sqlite_row(row):
    """Retain even malformed SQLite BLOB/REAL cells without coercing them."""
    def cell(value):
        if isinstance(value, bytes):
            return {'sqlite_type': 'blob', 'hex': value.hex()}
        if type(value) is float and not math.isfinite(value):
            return {'sqlite_type': 'real', 'value': str(value)}
        return value
    return {key: cell(value) for key, value in dict(row).items()}


def _decode_unproven_marker(raw):
    """Keep unknown or malformed markers distinct from valid source evidence."""
    try:
        marker = json.loads(raw)
        if type(marker) is not dict:
            return None
        encoded(marker)  # Scope proof JSON cannot contain non-finite values.
        return marker
    except (ValueError, TypeError, OverflowError, RecursionError):
        return None


def _archive_corrupt_unproven_marker(queue, scope, page, raw):
    """Save the exact SQLite value, including BLOBs, without overwriting a conflict."""
    saved_raw = _audit_sqlite_row({'evidence_json': raw})['evidence_json']
    archive = {'scope_url': scope, 'page': page, 'original_evidence_json': saved_raw}
    marker_sha = digest(archive)
    archive_key = 'cnyes_history_corrupt_unproven_marker_v1:' + marker_sha
    archive_value = encoded(archive)
    queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)', (archive_key, archive_value))
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (archive_key,)).fetchone()
    return marker_sha, bool(saved and saved[0] == archive_value)


def _remove_unproven_marker(queue, scope, page):
    """Remove a marker only after its malformed value is transactionally journalled.

    The caller owns the surrounding transaction. An absent marker needs only
    an indexed read; an archive conflict leaves its exact cell untouched.
    """
    row = queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages '
                           'WHERE scope_url=? AND page=?', (scope, page)).fetchone()
    if not row:
        return True
    if _decode_unproven_marker(row['evidence_json']) is None:
        _, preserved = _archive_corrupt_unproven_marker(queue, scope, page, row['evidence_json'])
        if not preserved:
            return False
    queue.db.execute('DELETE FROM cnyes_history_unproven_pages WHERE scope_url=? AND page=?', (scope, page))
    return True


def _archive_invalid_checkpoint(queue, marker, prefix, raw):
    """Preserve the exact SQLite value before allowing an idempotent rescan."""
    audit_key = prefix + 'invalid:' + digest((marker, _audit_sqlite_row(raw)))
    with queue.db:
        queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)', (audit_key, raw[0]))
        saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (audit_key,)).fetchone()
        if not saved or type(saved[0]) is not type(raw[0]) or saved[0] != raw[0]:
            raise _CheckpointAuditConflict
        queue.db.execute('DELETE FROM queue_settings WHERE key=?', (marker,))


class _UnsupportedLegacyCheckpointVersion(ValueError):
    pass


def _decode_legacy_scan_checkpoint(value, position):
    state = json.loads(value)
    if (type(state) is dict and 'version' in state
            and (type(state['version']) is not int or state['version'] != 1)):
        raise _UnsupportedLegacyCheckpointVersion
    if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != 1
            or state.get('state') not in ('scanning', 'complete') or type(state.get(position)) is not str):
        raise ValueError('invalid legacy checkpoint')
    return state


def _legacy_scan_checkpoint(queue, marker, prefix, position, stop):
    """Read a bounded, repeatable local scan; preserve damaged bytes before restarting."""
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    initial = {'version': 1, 'state': 'scanning', position: ''}
    if saved is None:
        return initial, None, False
    try:
        return _decode_legacy_scan_checkpoint(saved[0], position), None, False
    except _UnsupportedLegacyCheckpointVersion:
        return None, 'unsupported_legacy_checkpoint_version', False
    except (TypeError, ValueError):
        if stop is not None and stop.is_set():
            return None, None, False
        try:
            _archive_invalid_checkpoint(queue, marker, prefix, saved)
        except _CheckpointAuditConflict:
            return None, 'legacy_checkpoint_audit_conflict', False
        return initial, None, True


def _legacy_scan_status(db, marker, prefix, position):
    """Project fixed state codes only; never expose the checkpoint's key or cursor."""
    result = {'scan_state': 'unknown', 'checkpoint_problem': None}
    saved = db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    if saved is None:
        result['scan_state'] = 'not_started'
        return result
    try:
        result['scan_state'] = _decode_legacy_scan_checkpoint(saved[0], position)['state']
    except _UnsupportedLegacyCheckpointVersion:
        result['checkpoint_problem'] = 'unsupported_version'
    except (TypeError, ValueError):
        result['checkpoint_problem'] = 'invalid_checkpoint'
        audit_key = prefix + 'invalid:' + digest((marker, _audit_sqlite_row(saved)))
        archived = db.execute('SELECT value FROM queue_settings WHERE key=?', (audit_key,)).fetchone()
        if archived and (type(archived[0]) is not type(saved[0]) or archived[0] != saved[0]):
            result['checkpoint_problem'] = 'audit_conflict'
    return result


def legacy_empty_scan_key(source):
    return LEGACY_EMPTY_SCAN_PREFIX + digest((
        source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))


@lru_cache(maxsize=PAGE_DIGEST_CACHE_SIZE)
def _page_observation_digest(serialized):
    # Cache only the digest, not a mutable observation. The full serialized
    # text is the key, so changing even one byte cannot reuse the old result.
    return digest(json.loads(serialized))


def _checked_page_observation(row, source=None):
    """Read a committed page only when its metadata and content hash agree."""
    if not row:
        return None
    try:
        serialized = row['observation_json']
        observation = json.loads(serialized)
        info = observation['pagination']
        identities = observation['identities']
        calculated_sha = (_page_observation_digest(serialized)
                          if type(serialized) is str and len(serialized) <= PAGE_DIGEST_CACHE_MAX_JSON_CHARS
                          else digest(observation))
        if (type(observation) is not dict or calculated_sha != row['observation_sha256']
                or (observation['scope_url'], observation['page'], observation['response_sha256'],
                    observation['contract_version'], observation['source_bulk_fingerprint'],
                    observation['authorization_fingerprint']) !=
                   (row['scope_url'], row['page'], row['response_sha256'], row['evidence_contract_version'],
                    row['source_bulk_fingerprint'], row['authorization_fingerprint'])
                or type(observation['provider']) is not str
                or type(observation['artifact']) is not dict
                or type(observation['source_day_offset_seconds']) is not int
                or type(info) is not dict or info['scope'] != row['scope_url'] or info['page'] != row['page']
                or any(type(info[k]) is not int or info[k] < 0 for k in ('total', 'pages', 'duplicate_rows'))
                or type(info['ids_sha256']) is not str
                or cnyes_scope(observation['request_url']) != row['scope_url']
                or wordpress_page(observation['request_url']) != row['page']
                or type(identities) is not list
                or any(type(item) is not dict or type(item.get('newsId')) is not int
                       or item['newsId'] <= 0 or type(item.get('publishAt')) is not int
                       or type(item.get('source_post_sha256')) is not str
                       or len(item['source_post_sha256']) != 64 for item in identities)
                or source is not None and (observation['provider'] != source.provider
                    or wordpress_base(observation['request_url']) not in source.cnyes_apis)):
            return None
        ids = {item['newsId'] for item in identities}
        if (type(info['returned_rows']) is not int or info['returned_rows'] != len(identities)
                or type(info['unique_rows']) is not int or info['unique_rows'] != len(ids)
                or info['duplicate_rows'] != len(identities) - len(ids)
                or info['ids_sha256'] != hashlib.sha256(json.dumps(sorted(ids)).encode()).hexdigest()):
            return None
        return observation
    except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
        return None


def _matching_shared_reference(queue, scope, page, sha, observation, *, allow_original_request_mismatch=False):
    """A shared page needs its own retained, unmodified source receipt."""
    try:
        marker = queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages WHERE scope_url=? AND page=?',
                                  (observation['scope_url'], page)).fetchone()
        if marker:
            parsed_marker = _decode_unproven_marker(marker[0]) if allow_original_request_mismatch else None
            if (parsed_marker is None or parsed_marker.get('reason') != 'original_envelope_request_mismatch'
                    or parsed_marker.get('actual_observation_sha256') != sha
                    or parsed_marker.get('original_request_url') not in (None, observation['request_url'])):
                return None
        reference = queue.db.execute('SELECT * FROM cnyes_history_page_ID_references '
            'WHERE scope_url=? AND page=? AND observation_sha256=?', (scope, page, sha)).fetchone()
        original = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                    (scope, page)).fetchone()
        info = observation['pagination']
        key = scope_key(scope)
        if (not reference or not original or not key or key != scope_key(observation['scope_url'])
                or reference['reference_scope'] != observation['scope_url']
                or json.loads(reference['original_ID_evidence_json']) != dict(original)
                or (original['total'], original['ids_sha256']) != (info['total'], info['ids_sha256'])):
            return None
        return reference
    except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
        return None


def _history_scan_slice_done(checked, started, max_seconds, stop):
    """Permit one bounded step after a slow seek; a stop remains immediate."""
    return bool(stop is not None and stop.is_set()
                or checked > 0 and time.monotonic() - started >= max_seconds)


def _retained_envelopes(root, sha, request_url=None, authorization_fingerprint=None):
    """Find exact-request captures first, then the two legacy locations.

    Identical response bytes do not identify the request which observed them.
    All candidates still need content, request, authorization and clock checks.
    """
    if type(sha) is not str or len(sha) != 64 or any(ch not in '0123456789abcdef' for ch in sha):
        return ()
    candidates = []
    if type(request_url) is str and type(authorization_fingerprint) is str:
        key = digest((request_url, authorization_fingerprint))
        directory = root / 'discovery_responses' / 'by_request' / key[:2] / key
        candidates.append((directory / (sha + '.parquet'), 'cnyes_history_source_envelope'))
        candidates.extend((path, 'cnyes_history_source_envelope')
                          for path in sorted(directory.glob(sha + '-*.parquet')))
    candidates.extend(((root / 'quarantine' / (sha + '.parquet'), 'canonical_quarantine'),
                       (root / 'discovery_responses' / (sha + '.parquet'), 'cnyes_history_source_envelope')))
    return tuple(candidates)


def _read_original_envelope_identity(path, receipt_path, sha, *, expected_parquet_path, expected_kind,
                                     include_original=False):
    """Verify bytes and clock; return identity and digests of verified files."""
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    # Older envelopes may have only the two hashes and publish flag. When a
    # receipt claims newer fields, however, never ignore a contradiction or
    # silently treat an unknown contract as that older format.
    if (not isinstance(receipt, dict)
            or expected_kind not in ('canonical_quarantine', 'cnyes_history_source_envelope')):
        return None
    if 'parquet_path' in receipt and receipt['parquet_path'] != expected_parquet_path:
        return None
    if 'contract' in receipt:
        if (receipt['contract'] != 'cnyes_history_source_envelope_v1'
                or expected_kind != 'cnyes_history_source_envelope'
                or not {'parquet_path', 'raw_response_bytes', 'article_bodies_accepted'} <= receipt.keys()):
            return None
    records = pq.read_table(path).to_pylist()
    if len(records) != 1:
        return None
    parquet_sha = sha256_file(path)
    if (receipt['sha256'] != parquet_sha or receipt['response_sha256'] != sha
            or receipt.get('publish') is not False):
        return None
    original = records[0]
    raw = original['raw_response_body']
    raw = raw.encode() if isinstance(raw, str) else raw
    if not isinstance(raw, bytes):
        return None
    if ('raw_response_bytes' in receipt and
            (type(receipt['raw_response_bytes']) is not int or receipt['raw_response_bytes'] != len(raw))):
        return None
    if 'article_bodies_accepted' in receipt and receipt['article_bodies_accepted'] is not False:
        return None
    if 'article_count' in receipt or 'accepted_body_count' in receipt:
        total, accepted = receipt.get('article_count'), receipt.get('accepted_body_count')
        if type(total) is not int or type(accepted) is not int or not 0 <= accepted <= total:
            return None
        if original.get('errors_json') is not None:
            errors = json.loads(original['errors_json'])
            if not isinstance(errors, list) or len(errors) != total - accepted:
                return None
    captured = datetime.fromisoformat(original['captured_at_utc'])
    if (original.get('response_sha256') != sha or original.get('publish') is not False
            or hashlib.sha256(raw).hexdigest() != sha or captured.tzinfo is None or captured.utcoffset() is None
            or captured > datetime.now(timezone.utc)):
        return None
    identity = original['request_url'], original.get('authorization_fingerprint')
    verified = identity, hashlib.sha256(receipt_bytes).digest(), parquet_sha, captured
    # The audit cache keeps only identity and digests. The recovery owner can
    # also use the very row that passed these checks, without another decode.
    return (*verified, original) if include_original else verified


def _classify_original_envelope(identity, request_url, authorization_fingerprint):
    if identity is None:
        return 'invalid'
    if identity[0] != request_url:
        return 'request_mismatch'
    if identity[1] != authorization_fingerprint:
        return 'authorization_mismatch'
    return 'verified'


def _original_envelope_state(root, artifact, request_url, sha, authorization_fingerprint):
    """Verify a local raw envelope; distinguish identity from local failures."""
    from downloader.news_fulltext_queue import replay_source_path
    try:
        path = replay_source_path(root, artifact['parquet_path'])
        receipt_path = replay_source_path(root, artifact['receipt_path'])
        before = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)
        verified = _read_original_envelope_identity(path, receipt_path, sha,
            expected_parquet_path=artifact['parquet_path'], expected_kind=artifact['kind'])
        if before != (_original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)):
            raise OSError('original envelope changed during verification')
        return _classify_original_envelope(verified[0] if verified else None,
                                           request_url, authorization_fingerprint)
    except FileNotFoundError:
        return 'unavailable'
    except (ValueError, KeyError, TypeError, AttributeError, OverflowError, pa.ArrowException):
        return 'invalid'


def _original_envelope_file_signature(path):
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


class _OriginalEnvelopeAuditCache:
    """Reuse decoded identity only after rehashing both current source files."""

    def __init__(self, *, max_entries=ORIGINAL_ENVELOPE_AUDIT_CACHE_SIZE):
        if type(max_entries) is not int or max_entries <= 0:
            raise ValueError('invalid original envelope audit cache size')
        self.max_entries = max_entries
        self.entries = OrderedDict()

    def check(self, root, artifact, request_url, sha, authorization_fingerprint):
        from downloader.news_fulltext_queue import replay_source_path
        try:
            path = replay_source_path(root, artifact['parquet_path'])
            receipt_path = replay_source_path(root, artifact['receipt_path'])
            key = path, receipt_path, sha, artifact['parquet_path'], artifact['kind']
            before = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)
            cached = self.entries.get(key)
            if cached and cached[0] == before:
                receipt_sha = hashlib.sha256(receipt_path.read_bytes()).digest()
                parquet_sha = sha256_file(path)
                if (receipt_sha, parquet_sha) == cached[2:4]:
                    verified = cached[1:]
                else:
                    cached = None
            if not cached or cached[0] != before:
                self.entries.pop(key, None)
                verified = _read_original_envelope_identity(path, receipt_path, sha,
                    expected_parquet_path=artifact['parquet_path'], expected_kind=artifact['kind'], include_original=True)
                if verified is not None:
                    raw = verified[4]['raw_response_body']
                    raw_size = len(raw.encode()) if isinstance(raw, str) else len(raw) if isinstance(raw, bytes) else -1
                    if raw_size < 0 or raw_size > ORIGINAL_ENVELOPE_CACHE_MAX_RAW_BYTES:
                        verified = verified[:4]  # Do not retain large response bodies in the owner cache.
            after = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)
            if before != after:
                self.entries.pop(key, None)
                raise OSError('original envelope changed during verification')
            if verified is not None and verified[3] > datetime.now(timezone.utc):
                # Rehashing the files is not enough if the wall clock moved
                # backwards since this source observation was cached.
                self.entries.pop(key, None)
                verified = None
            if verified is not None:
                identity = verified[0]
                self.entries[key] = (before, *verified)
                self.entries.move_to_end(key)
                if len(self.entries) > self.max_entries:
                    self.entries.popitem(last=False)
            else:
                identity = None
            return _classify_original_envelope(identity, request_url, authorization_fingerprint)
        except FileNotFoundError:
            return 'unavailable'
        except (ValueError, KeyError, TypeError, AttributeError, OverflowError, pa.ArrowException):
            return 'invalid'

    def verified_original(self, root, artifact, sha):
        """Reuse a small decoded original only while both file hashes still agree."""
        from downloader.news_fulltext_queue import replay_source_path
        path = replay_source_path(root, artifact['parquet_path'])
        receipt = replay_source_path(root, artifact['receipt_path'])
        key = path, receipt, sha, artifact['parquet_path'], artifact['kind']
        before = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt)
        cached = self.entries.get(key)
        if not cached or len(cached) != 6 or cached[0] != before:
            return None
        if (hashlib.sha256(receipt.read_bytes()).digest(), sha256_file(path)) != cached[2:4]:
            return None
        if before != (_original_envelope_file_signature(path), _original_envelope_file_signature(receipt)):
            raise OSError('original envelope changed during cache read')
        if cached[4] > datetime.now(timezone.utc):
            return None
        self.entries.move_to_end(key)
        return cached[1:]


def _matches_original_envelope(root, artifact, request_url, sha, authorization_fingerprint):
    return _original_envelope_state(root, artifact, request_url, sha, authorization_fingerprint) == 'verified'


def _read_accepted_batch_original(queue, artifact, source, request_url, sha, *, request_sha256=None):
    """Read an exact original from a committed batch, preserving its receipt and clock."""
    from downloader.news_fulltext_queue import replay_source_path

    try:
        if not isinstance(request_url, str) and not isinstance(request_sha256, str):
            return None
        relative, receipt_relative = artifact['parquet_path'], artifact['receipt_path']
        accepted = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (relative,)).fetchone()
        if not accepted or accepted[0] != receipt_relative:
            return None
        path = replay_source_path(queue.root, relative)
        receipt_path = replay_source_path(queue.root, receipt_relative)
        before = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)
        receipt_bytes = receipt_path.read_bytes()
        receipt = json.loads(receipt_bytes)
        batch = pq.ParquetFile(path)
        columns = ['provider', 'source_request_url', 'raw_response_body', 'response_sha256',
                   'captured_at_utc', 'authorization_fingerprint', 'publish']
        if (type(receipt.get('contract_version')) is not int or receipt['contract_version'] != NEWS_CONTRACT_VERSION
                or receipt.get('parquet_path') != relative or receipt.get('publish') is not False
                or type(receipt.get('rows')) is not int or receipt['rows'] != batch.metadata.num_rows
                or sha256_file(path) != receipt.get('sha256') or not set(columns) <= set(batch.schema_arrow.names)):
            return None
        matched = None
        for chunk in batch.iter_batches(batch_size=16, columns=columns, use_threads=False):
            for row in chunk.to_pylist():
                actual_request = row.get('source_request_url')
                if (row.get('provider') != source.provider or not isinstance(actual_request, str)
                        or request_url is not None and actual_request != request_url
                        or request_sha256 is not None and hashlib.sha256(actual_request.encode()).hexdigest() != request_sha256
                        or row.get('response_sha256') != sha or row.get('raw_response_body') is None
                        or row.get('authorization_fingerprint') != source.authorization_fingerprint
                        or row.get('publish') is not False):
                    continue
                raw = row['raw_response_body']
                raw = raw.encode() if isinstance(raw, str) else raw
                captured = datetime.fromisoformat(row['captured_at_utc'])
                if (hashlib.sha256(raw).hexdigest() == sha and captured.tzinfo is not None
                        and captured.utcoffset() is not None and captured <= datetime.now(timezone.utc)):
                    matched = {**row, 'request_url': actual_request, 'raw_response_body': raw}
                    break
            if matched:
                break
        if (matched and (_original_envelope_file_signature(path),
                _original_envelope_file_signature(receipt_path)) == before
                and receipt_path.read_bytes() == receipt_bytes):
            return {'original': matched, 'receipt': receipt, 'receipt_bytes': receipt_bytes,
                    'source_file_signature': before[0], 'source_receipt_signature': before[1]}
        return None
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError, pa.ArrowException):
        return None


def _matches_accepted_batch_original(queue, artifact, source, request_url, sha):
    """Reuse a committed body batch only if its original proves this request."""
    return _read_accepted_batch_original(queue, artifact, source, request_url, sha) is not None


def scope_key(url):
    """Cache only immutable URL arithmetic, never source rows or evidence.

    Heartbeats and index seeding revisit more scopes than urlsplit's own
    small cache holds. The offset is part of the key; a changed date rule
    cannot borrow an old result. Unusual/oversized inputs retain the original
    parser and its exceptions, without occupying persistent cache space.
    """
    if (type(url) is str and len(url) <= SCOPE_KEY_CACHE_MAX_URL_CHARS
            and type(CNYES_QUERY_DAY_OFFSET_SECONDS) is int):
        return _cached_scope_key(url, CNYES_QUERY_DAY_OFFSET_SECONDS)
    return _parse_scope_key(url, CNYES_QUERY_DAY_OFFSET_SECONDS)


@lru_cache(maxsize=SCOPE_KEY_CACHE_SIZE)
def _cached_scope_key(url, query_day_offset):
    return _parse_scope_key(url, query_day_offset)


def _parse_scope_key(url, query_day_offset):
    query = parse_qs(url.split('?', 1)[1]) if '?' in url else {}
    if not {'startAt', 'endAt'} <= query.keys():
        return None
    low, high = int(query['startAt'][0]), int(query['endAt'][0])
    if high < low or query.get('limit') != ['30']:
        return None
    first, last = ((value + query_day_offset) // 86400 for value in (low, high))
    return wordpress_base(url), first, last


def index_scope(queue, scope, provider):
    key = scope_key(scope)
    if key:
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_scope_keys VALUES (?,?,?,?,?)',
                         (scope, provider, *key))


def index_children(queue, scope, provider, children):
    index_scope(queue, scope, provider)
    for url in children:
        child = cnyes_scope(url)
        index_scope(queue, child, provider)
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_edges VALUES (?,?,?)', (scope, child, url))


def initialize_accepted_batch_candidates(queue):
    """Prepare the reference and outcome audit before orphan receipt replay."""
    # Orphan receipt recovery runs before the full history migration. A newly
    # registered old batch may be behind an already completed scan frontier;
    # persist that local retry in the same acceptance transaction.
    queue._cnyes_history_fully_initialized = False
    queue.db.execute('''
        CREATE TABLE IF NOT EXISTS cnyes_history_accepted_batch_local_retries(
            scan_key TEXT NOT NULL,parquet_path TEXT NOT NULL,receipt_path TEXT NOT NULL,
            not_before_epoch REAL NOT NULL,local_attempts INTEGER NOT NULL,
            PRIMARY KEY(scan_key,parquet_path)
        ) WITHOUT ROWID
    ''')
    queue.db.execute('''
        CREATE TABLE IF NOT EXISTS cnyes_history_accepted_batch_candidates(
            request_url TEXT NOT NULL,response_sha256 TEXT NOT NULL,parquet_path TEXT NOT NULL,
            receipt_path TEXT NOT NULL,PRIMARY KEY(request_url,response_sha256,parquet_path)
        ) WITHOUT ROWID
    ''')
    queue.db.execute('''
        CREATE TABLE IF NOT EXISTS cnyes_history_candidate_receipt_revisions(
            revision_sha256 TEXT PRIMARY KEY,request_url TEXT NOT NULL,response_sha256 TEXT NOT NULL,
            parquet_path TEXT NOT NULL,old_receipt_path TEXT NOT NULL,new_receipt_path TEXT NOT NULL,
            observed_at_utc TEXT NOT NULL
        ) WITHOUT ROWID
    ''')
    queue.db.execute('''
        CREATE TABLE IF NOT EXISTS cnyes_history_retained_outcome_resolutions(
            resolution_sha256 TEXT PRIMARY KEY,request_url TEXT NOT NULL,original_state TEXT NOT NULL,
            original_outcome_json TEXT,resolution_state TEXT NOT NULL,observation_sha256 TEXT,
            evidence_kind TEXT,resolved_at_utc TEXT NOT NULL
        ) WITHOUT ROWID
    ''')
    # A new database may not have the history work table until after orphan
    # receipt replay. In that case there cannot yet be old work to wake.
    queue._cnyes_retained_work_initialized = bool(queue.db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cnyes_history_retained_work'").fetchone())
    # Receipt replay runs before the full history migration on startup. An
    # older database can have retained work but lack the newer audit tables.
    queue._cnyes_original_repair_initialized = queue.db.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN "
        "('cnyes_history_unproven_pages','cnyes_history_page_heads',"
        "'cnyes_history_page_observations','cnyes_history_retained_local_retries',"
        "'cnyes_history_retained_revision_audit')").fetchone()[0] == 5


def initialize(queue):
    initialize_accepted_batch_candidates(queue)
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS cnyes_history_page_observations(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,response_sha256 TEXT NOT NULL,
            evidence_contract_version INTEGER NOT NULL,source_bulk_fingerprint TEXT NOT NULL,
            authorization_fingerprint TEXT NOT NULL,
            observation_sha256 TEXT NOT NULL UNIQUE,observation_json TEXT NOT NULL,
            PRIMARY KEY(scope_url,page,response_sha256,evidence_contract_version,source_bulk_fingerprint,authorization_fingerprint)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_page_heads(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,observation_sha256 TEXT NOT NULL,
            last_confirmed_at_utc TEXT NOT NULL,
            PRIMARY KEY(scope_url,page)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_page_head_observation
            ON cnyes_history_page_heads(observation_sha256);
        CREATE TABLE IF NOT EXISTS cnyes_history_corrupt_observation_audit(
            audit_sha256 TEXT PRIMARY KEY,original_row_json TEXT NOT NULL,original_heads_json TEXT NOT NULL,
            reason TEXT NOT NULL,detected_at_utc TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_page_ID_references(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,observation_sha256 TEXT NOT NULL,
            original_ID_evidence_json TEXT NOT NULL,reference_scope TEXT NOT NULL,
            PRIMARY KEY(scope_url,page,observation_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_unproven_pages(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,evidence_json TEXT NOT NULL,
            PRIMARY KEY(scope_url,page)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_equal_day_recovery_heads(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,old_observation_sha256 TEXT NOT NULL,
            recovered_observation_sha256 TEXT NOT NULL,source_scope_url TEXT NOT NULL,event_key TEXT NOT NULL,
            PRIMARY KEY(scope_url,page,old_observation_sha256,recovered_observation_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_original_file_retries(
            audit_key TEXT NOT NULL,scope_url TEXT NOT NULL,page INTEGER NOT NULL,
            observation_sha256 TEXT NOT NULL,
            not_before_epoch REAL NOT NULL CHECK(typeof(not_before_epoch) IN ('integer','real')
                AND not_before_epoch>=0 AND not_before_epoch<=1e18),
            attempts INTEGER NOT NULL CHECK(typeof(attempts)='integer' AND attempts>=0),
            PRIMARY KEY(audit_key,scope_url,page,observation_sha256)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_original_file_retry_due
            ON cnyes_history_original_file_retries(audit_key,not_before_epoch,scope_url,page,observation_sha256);
        CREATE TABLE IF NOT EXISTS cnyes_history_original_priority_pages(
            plan_sha256 TEXT NOT NULL,audit_key TEXT NOT NULL,scope_url TEXT NOT NULL,
            page INTEGER NOT NULL,observation_sha256 TEXT NOT NULL,
            last_result TEXT NOT NULL CHECK(last_result IN
                ('awaiting','verified','request_mismatch','authorization_mismatch',
                 'invalid','unavailable','skipped','outside','superseded')),
            PRIMARY KEY(plan_sha256,audit_key,scope_url,page,observation_sha256)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_original_priority_audit_lookup
            ON cnyes_history_original_priority_pages(audit_key,scope_url,page,observation_sha256);
        CREATE INDEX IF NOT EXISTS cnyes_history_original_priority_awaiting
            ON cnyes_history_original_priority_pages(plan_sha256,audit_key,scope_url,page,observation_sha256)
            WHERE last_result='awaiting';
        CREATE TABLE IF NOT EXISTS cnyes_history_original_priority_watch(
            plan_sha256 TEXT NOT NULL,audit_key TEXT NOT NULL,observation_sha256 TEXT NOT NULL,
            PRIMARY KEY(plan_sha256,audit_key,observation_sha256)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_original_priority_watch_observation
            ON cnyes_history_original_priority_watch(observation_sha256,plan_sha256,audit_key);
        CREATE TABLE IF NOT EXISTS cnyes_history_original_priority_pending(
            plan_sha256 TEXT NOT NULL,audit_key TEXT NOT NULL,scope_url TEXT NOT NULL,
            page INTEGER NOT NULL,observation_sha256 TEXT NOT NULL,
            PRIMARY KEY(plan_sha256,audit_key,scope_url,page,observation_sha256)
        ) WITHOUT ROWID;
        CREATE TRIGGER IF NOT EXISTS cnyes_history_original_priority_new_head
        AFTER INSERT ON cnyes_history_page_heads BEGIN
            INSERT OR IGNORE INTO cnyes_history_original_priority_pending
            SELECT w.plan_sha256,w.audit_key,NEW.scope_url,NEW.page,NEW.observation_sha256
            FROM cnyes_history_original_priority_watch w
            JOIN queue_settings active ON active.key='cnyes_original_audit_active_plan_v1'
                AND active.value=w.plan_sha256 || ':' || w.audit_key
            WHERE w.observation_sha256=NEW.observation_sha256
              AND NOT EXISTS (SELECT 1 FROM cnyes_history_original_priority_pages p
                WHERE p.plan_sha256=w.plan_sha256 AND p.audit_key=w.audit_key
                  AND p.scope_url=NEW.scope_url AND p.page=NEW.page
                  AND p.observation_sha256=NEW.observation_sha256 AND p.last_result!='superseded');
        END;
        CREATE TRIGGER IF NOT EXISTS cnyes_history_original_priority_changed_head
        AFTER UPDATE OF observation_sha256 ON cnyes_history_page_heads
        WHEN NEW.observation_sha256 IS NOT OLD.observation_sha256 BEGIN
            INSERT OR IGNORE INTO cnyes_history_original_priority_pending
            SELECT w.plan_sha256,w.audit_key,NEW.scope_url,NEW.page,NEW.observation_sha256
            FROM cnyes_history_original_priority_watch w
            JOIN queue_settings active ON active.key='cnyes_original_audit_active_plan_v1'
                AND active.value=w.plan_sha256 || ':' || w.audit_key
            WHERE w.observation_sha256=NEW.observation_sha256
              AND NOT EXISTS (SELECT 1 FROM cnyes_history_original_priority_pages p
                WHERE p.plan_sha256=w.plan_sha256 AND p.audit_key=w.audit_key
                  AND p.scope_url=NEW.scope_url AND p.page=NEW.page
                  AND p.observation_sha256=NEW.observation_sha256 AND p.last_result!='superseded');
        END;
        CREATE TABLE IF NOT EXISTS cnyes_history_scope_keys(
            scope_url TEXT PRIMARY KEY,provider TEXT NOT NULL,endpoint TEXT NOT NULL,
            first_day INTEGER NOT NULL,last_day INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_effective_dates
            ON cnyes_history_scope_keys(endpoint,first_day,last_day,provider);
        CREATE TABLE IF NOT EXISTS cnyes_history_edges(
            parent_scope TEXT NOT NULL,child_scope TEXT NOT NULL,child_request_url TEXT NOT NULL,
            PRIMARY KEY(parent_scope,child_scope)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_parent_lookup ON cnyes_history_edges(child_scope,parent_scope);
        CREATE TABLE IF NOT EXISTS cnyes_history_scope_proofs(
            proof_sha256 TEXT PRIMARY KEY,scope_url TEXT NOT NULL,proof_json TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_corrupt_proof_audit(
            audit_sha256 TEXT PRIMARY KEY,original_row_json TEXT NOT NULL,
            reason TEXT NOT NULL,detected_at_utc TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_integrity_incidents(
            incident_sha256 TEXT PRIMARY KEY,scope_url TEXT NOT NULL,original_head_json TEXT NOT NULL,
            original_proof_json TEXT,original_gap_json TEXT,detected_at_utc TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_integrity_checkpoint_audit(
            checkpoint_sha256 TEXT PRIMARY KEY,checkpoint_key TEXT NOT NULL,original_value TEXT NOT NULL,
            detected_at_utc TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_scope_heads(
            scope_url TEXT PRIMARY KEY,provider TEXT NOT NULL,state TEXT NOT NULL,
            proof_sha256 TEXT NOT NULL,member_ids_json TEXT NOT NULL,
            source_bulk_fingerprint TEXT NOT NULL,authorization_fingerprint TEXT NOT NULL,
            evidence_contract_version INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_state_counts ON cnyes_history_scope_heads(state);
        CREATE INDEX IF NOT EXISTS cnyes_history_current_contract ON cnyes_history_scope_heads(
            provider,evidence_contract_version,source_bulk_fingerprint,authorization_fingerprint,state);
        CREATE TABLE IF NOT EXISTS cnyes_history_gap_retirements(
            scope_url TEXT NOT NULL,proof_sha256 TEXT NOT NULL,gap_json TEXT NOT NULL,retired_at_utc TEXT NOT NULL,
            PRIMARY KEY(scope_url,proof_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_retained_work(
            request_url TEXT PRIMARY KEY,scope_url TEXT NOT NULL,page INTEGER NOT NULL,response_sha256 TEXT NOT NULL,
            expected_total INTEGER NOT NULL,expected_ids_sha256 TEXT NOT NULL,source_checked_at_utc TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending',outcome_json TEXT
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_frontier ON cnyes_history_retained_work(state,request_url);
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_scope_page
            ON cnyes_history_retained_work(scope_url,page,state);
        CREATE TABLE IF NOT EXISTS cnyes_history_retained_revision_audit(
            revision_sha256 TEXT PRIMARY KEY,request_url TEXT NOT NULL,
            original_work_json TEXT NOT NULL,archived_at_utc TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_local_inventory_scopes(
            scope_url TEXT PRIMARY KEY,source_bulk_fingerprint TEXT NOT NULL,
            authorization_fingerprint TEXT NOT NULL,
            scan_state TEXT NOT NULL CHECK(scan_state IN ('scanning','ready')),
            created_at_utc TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_local_inventory_state
            ON cnyes_history_local_inventory_scopes(scan_state);
        CREATE TABLE IF NOT EXISTS cnyes_history_accepted_batch_outcomes(
            parquet_path TEXT NOT NULL,receipt_path TEXT NOT NULL,state TEXT NOT NULL,
            reason TEXT NOT NULL,observed_at_utc TEXT NOT NULL,
            PRIMARY KEY(parquet_path,observed_at_utc,state)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_accepted_outcome_state_path
            ON cnyes_history_accepted_batch_outcomes(state,parquet_path);
        CREATE TABLE IF NOT EXISTS cnyes_history_accepted_batch_local_retries(
            scan_key TEXT NOT NULL,parquet_path TEXT NOT NULL,receipt_path TEXT NOT NULL,
            not_before_epoch REAL NOT NULL,local_attempts INTEGER NOT NULL,
            PRIMARY KEY(scan_key,parquet_path)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_accepted_batch_retry_due
            ON cnyes_history_accepted_batch_local_retries(scan_key,not_before_epoch,parquet_path);
        CREATE TABLE IF NOT EXISTS cnyes_history_accepted_missing_retries(
            scan_key TEXT NOT NULL,parquet_path TEXT NOT NULL,receipt_path TEXT NOT NULL,
            not_before_epoch REAL NOT NULL,local_attempts INTEGER NOT NULL,
            PRIMARY KEY(scan_key,parquet_path)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_accepted_missing_retry_due
            ON cnyes_history_accepted_missing_retries(scan_key,not_before_epoch,parquet_path);
        CREATE TABLE IF NOT EXISTS cnyes_history_accepted_invalid_retries(
            scan_key TEXT NOT NULL,parquet_path TEXT NOT NULL,receipt_path TEXT NOT NULL,
            not_before_epoch REAL NOT NULL,witness_json TEXT,confirmed INTEGER NOT NULL DEFAULT 0 CHECK(confirmed IN (0,1)),
            local_attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(scan_key,parquet_path)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_accepted_invalid_retry_due
            ON cnyes_history_accepted_invalid_retries(scan_key,not_before_epoch,parquet_path);
        CREATE TABLE IF NOT EXISTS cnyes_history_retained_local_retries(
            request_url TEXT PRIMARY KEY,
            original_state TEXT NOT NULL CHECK(original_state IN ('pending','overflow_pending')),
            not_before_epoch REAL NOT NULL,local_attempts INTEGER NOT NULL,
            error_code TEXT NOT NULL,last_attempt_at_utc TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_local_retry_due
            ON cnyes_history_retained_local_retries(not_before_epoch,request_url);
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_local_retry_origin_due
            ON cnyes_history_retained_local_retries(original_state,not_before_epoch,request_url);
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_waiting_batch_scan
            ON cnyes_history_retained_local_retries(request_url)
            WHERE error_code='accepted_batch_scan_pending';
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_resolutions_request
            ON cnyes_history_retained_outcome_resolutions(request_url);
    ''')
    queue._cnyes_retained_work_initialized = True
    queue._cnyes_original_repair_initialized = True
    queue._cnyes_history_fully_initialized = True
    if not hasattr(queue, '_cnyes_history_repair_resweep'):
        # A previous owner may have advanced its cursor before a crash while
        # newer evidence appeared behind it. Complete one extra sweep after
        # that durable cursor reaches the end.
        queue._cnyes_history_repair_resweep = bool(queue.db.execute(
            'SELECT 1 FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone())
    marker = 'cnyes_history_evidence_initialization_v' + str(CONTRACT_VERSION)
    if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
        return
    sources = {p: s for p, s in queue.registry.sources.items() if s.allowed and s.cnyes_apis}
    with queue.db:
        for row in queue.db.execute('SELECT * FROM bulk_window_splits').fetchall():
            source = sources.get(row['provider'])
            if source and wordpress_base(row['scope_url']) in source.cnyes_apis:
                index_children(queue, row['scope_url'], row['provider'], json.loads(row['child_urls_json']))
        # Only replacement leaves of an actual historical gap need migration.
        # Do not re-enumerate every old complete article or every history root.
        leaves = queue.db.execute('''
            SELECT DISTINCT p.* FROM cnyes_history_edges e
            JOIN bulk_window_gaps g ON g.scope_url=e.parent_scope
            JOIN bulk_api_coverage c ON c.base_url=e.child_scope
            JOIN bulk_page_evidence p ON p.scope_url=c.base_url
            LEFT JOIN bulk_window_splits s ON s.scope_url=c.base_url
            WHERE c.total_pages<=30 AND s.scope_url IS NULL AND p.page<=c.total_pages
        ''').fetchall()
        # The old adapter stored no page hash for exact zero-row windows.
        # Expect an empty set, but require a real retained/new empty envelope;
        # do not manufacture a historical observation from this expectation.
        empty = queue.db.execute('''
            SELECT DISTINCT c.base_url AS scope_url,1 AS page,0 AS total,c.observed_at_utc
            FROM cnyes_history_edges e JOIN bulk_window_gaps g ON g.scope_url=e.parent_scope
            JOIN bulk_api_coverage c ON c.base_url=e.child_scope
            LEFT JOIN bulk_window_splits s ON s.scope_url=c.base_url
            WHERE c.total_articles=0 AND c.total_pages=1 AND s.scope_url IS NULL
        ''').fetchall()
        leaves.extend({**dict(row), 'ids_sha256': hashlib.sha256(json.dumps([]).encode()).hexdigest()} for row in empty)
        for row in leaves:
            key = scope_key(row['scope_url'])
            if not key:
                continue
            query = parse_qs(row['scope_url'].split('?', 1)[1])
            url = cnyes_url(key[0], row['page'], start=int(query['startAt'][0]), end=int(query['endAt'][0]))
            job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
            if job and job['response_sha256'] and job['last_checked_at_utc']:
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_work '
                    '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
                    'VALUES (?,?,?,?,?,?,?)', (url, row['scope_url'], row['page'], job['response_sha256'], row['total'],
                                               row['ids_sha256'], row['observed_at_utc']))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, encoded({
            'observed_at_utc': now(), 'retained_page_candidates': len(leaves),
            'article_or_source_deadlines_reset': False, 'scope_enumeration_is_not_body_acceptance': True})))


def retain_response(queue, response, source, *, accepted, restricted, accepted_batch=None):
    """Reference a committed envelope; save one only if none was accepted.

    A restricted shared envelope is never persisted. Its public ID/date
    projection and real response digest remain metadata-only evidence.
    """
    if restricted:
        return {'kind': 'restricted_public_metadata_only', 'raw_envelope_persisted': False}
    if accepted:
        if accepted_batch is not None:
            path, receipt = accepted_batch['parquet_path'], accepted_batch['receipt_path']
            committed = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone()
            if (not committed or committed[0] != receipt
                    or accepted[0].get('raw_response_body') != response.body
                    or accepted[0].get('source_request_url') != response.url):
                raise ValueError('committed_bulk_source_batch_required')
            return {'kind': 'accepted_body_batch', 'parquet_path': path, 'receipt_path': receipt}
        row = queue.db.execute('SELECT a.parquet_path,b.receipt_path FROM articles a JOIN accepted_batches b '
            'ON a.parquet_path=b.path WHERE a.id=?', (bytes.fromhex(accepted[0]['article_id']),)).fetchone()
        if not row:
            raise ValueError('committed_bulk_source_batch_required')
        return {'kind': 'accepted_body_batch', 'parquet_path': row[0], 'receipt_path': row[1]}
    sha = hashlib.sha256(response.body).hexdigest()
    prior = queue.db.execute('SELECT * FROM cnyes_history_page_observations '
        'WHERE scope_url=? AND page=? AND response_sha256=? AND evidence_contract_version=? '
        'AND source_bulk_fingerprint=? AND authorization_fingerprint=?',
        (cnyes_scope(response.url), int(parse_qs(response.url.split('?', 1)[1]).get('page', ['1'])[0]), sha,
         CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
    observation = _checked_page_observation(prior, source)
    if observation:
        artifact = observation['artifact']
        if (observation['request_url'] == response.url
                and observation['source_bulk_fingerprint'] == cnyes_bulk_fingerprint(source)
                and observation['authorization_fingerprint'] == source.authorization_fingerprint
                and artifact.get('parquet_path') and artifact.get('receipt_path')
                and (queue.root / artifact['parquet_path']).is_file()
                and (queue.root / artifact['receipt_path']).is_file()):
            kind = artifact.get('kind')
            matches = (_matches_accepted_batch_original(queue, artifact, source, response.url, sha)
                       if kind == 'accepted_body_batch' else
                       kind in ('canonical_quarantine', 'cnyes_history_source_envelope')
                       and _matches_original_envelope(queue.root, artifact, response.url, sha,
                                                      source.authorization_fingerprint))
            if matches:
                return artifact  # Reuse only a verified original, retaining its actual clock.
    candidates = _retained_envelopes(queue.root, sha, response.url, source.authorization_fingerprint)
    for path, kind in candidates:
        relative = str(path.relative_to(queue.root))
        artifact = {'kind': kind, 'parquet_path': relative, 'receipt_path': relative + '.receipt.json'}
        if _matches_original_envelope(queue.root, artifact, response.url, sha, source.authorization_fingerprint):
            return artifact
    # Never overwrite an orphaned, corrupt, or differently attributed file.
    # The canonical owner serializes writers; a retry can retain a new actual
    # observation under another name while preserving all earlier evidence.
    target = candidates[0][0]
    stem = target.stem
    suffix = 0
    while target.exists() or target.with_suffix('.parquet.receipt.json').exists():
        suffix += 1
        target = target.with_name(f'{stem}-{suffix:06d}.parquet')
    relative = str(target.relative_to(queue.root))
    atomic_write_parquet(target, table_from_records([{
        'request_url': response.url, 'response_sha256': sha, 'raw_response_body': response.body,
        'captured_at_utc': now(), 'authorization_fingerprint': source.authorization_fingerprint,
        'authorization_json': encoded(source.authorization), 'publish': False,
    }]), compression='zstd', compression_level=9, durable=True)
    atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
        'contract': 'cnyes_history_source_envelope_v1', 'parquet_path': relative,
        'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False,
        'raw_response_bytes': len(response.body), 'article_bodies_accepted': False,
    }, durable=True)
    return {'kind': 'cnyes_history_source_envelope', 'parquet_path': relative,
            'receipt_path': relative + '.receipt.json'}


def record_page(queue, response, info, posts, source, artifact, *, observed_at=None, origin='normal_source_response'):
    """Caller commits only after body acceptance; repeated bytes keep clocks."""
    if not scope_key(info['scope']):
        return
    if not source.allowed or wordpress_base(response.url) not in source.cnyes_apis:
        raise NewsError('cnyes_history_unregistered_endpoint')
    stamp = observed_at or now()
    identities = [{'newsId': row['newsId'], 'publishAt': row['publishAt'],
                   'source_post_sha256': digest(row)} for row in posts]
    observation = {'contract_version': CONTRACT_VERSION, 'provider': source.provider,
        'request_url': response.url, 'scope_url': info['scope'], 'page': info['page'],
        'response_sha256': hashlib.sha256(response.body).hexdigest(), 'pagination': info,
        'identities': identities, 'source_day_offset_seconds': CNYES_QUERY_DAY_OFFSET_SECONDS,
        'source_observed_at_utc': stamp, 'evidence_recorded_at_utc': now(), 'origin': origin,
        'source_bulk_fingerprint': cnyes_bulk_fingerprint(source),
        'authorization_fingerprint': source.authorization_fingerprint, 'artifact': artifact,
        'body_completeness_verified': False}
    old = queue.db.execute('SELECT * FROM cnyes_history_page_observations '
        'WHERE scope_url=? AND page=? AND response_sha256=? AND evidence_contract_version=? '
        'AND source_bulk_fingerprint=? AND authorization_fingerprint=?',
        (info['scope'], info['page'], observation['response_sha256'], CONTRACT_VERSION,
         cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
    current = queue.db.execute('SELECT last_confirmed_at_utc,observation_sha256 FROM cnyes_history_page_heads WHERE scope_url=? AND page=?',
                               (info['scope'], info['page'])).fetchone()
    affected_scopes = set()
    previous = _checked_page_observation(old, source)
    previous_request_differs = bool(previous and previous['request_url'] != response.url)
    previous_artifact_invalid = bool(previous and previous['artifact'] != artifact and (
        previous['artifact'].get('kind') in ('canonical_quarantine', 'cnyes_history_source_envelope')
        and not _matches_original_envelope(queue.root, previous['artifact'], response.url,
                                           observation['response_sha256'], source.authorization_fingerprint)
        or previous['artifact'].get('kind') == 'accepted_body_batch'
        and not _matches_accepted_batch_original(queue, previous['artifact'], source,
                                                 previous['request_url'], observation['response_sha256'])))
    # Equal bytes and a normalized scope do not identify an exact request.
    # Do not let an older local replay replace a newer confirmed head.
    if (previous_request_differs and current
            and datetime.fromisoformat(current[0]) > datetime.fromisoformat(stamp)):
        return
    if old and (previous is None or previous_request_differs or previous_artifact_invalid):
        # The source bytes alone cannot recover a damaged metadata row. Keep
        # the exact damaged row and all heads first, then record this real new
        # observation in the same transaction. Shared heads must be rebuilt
        # from their original page-ID receipts; they cannot inherit a new clock.
        heads = [dict(row) for row in queue.db.execute('SELECT * FROM cnyes_history_page_heads '
                                                       'WHERE observation_sha256=?', (old['observation_sha256'],))]
        archived = {'original_row': _audit_sqlite_row(old),
                    'original_heads': [_audit_sqlite_row(head) for head in heads],
                    'reason': ('page_observation_request_changed_same_scope' if previous_request_differs else
                               'page_original_envelope_invalid_or_request_mismatch' if previous_artifact_invalid
                               else 'page_observation_integrity_mismatch')}
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_corrupt_observation_audit VALUES (?,?,?,?,?)',
                         (digest(archived), encoded(archived['original_row']), encoded(archived['original_heads']),
                          archived['reason'], now()))
        affected_scopes = {row['scope_url'] for row in heads}
        # The discovery scan above already found every affected head. Delete
        # by its primary key instead of scanning the whole head table again.
        queue.db.executemany('DELETE FROM cnyes_history_page_heads '
            'WHERE scope_url=? AND page=? AND observation_sha256=?',
            ((row['scope_url'], row['page'], old['observation_sha256']) for row in heads))
        if (previous_request_differs or previous_artifact_invalid) and any(
                row['scope_url'] == info['scope'] and row['page'] == info['page'] for row in heads):
            current = None  # The newer head was itself attached to an invalid original.
        queue.db.execute('DELETE FROM cnyes_history_page_observations WHERE observation_sha256=?', (old['observation_sha256'],))
        old = None
    sha = old['observation_sha256'] if old else digest(observation)
    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)',
        (info['scope'], info['page'], observation['response_sha256'], CONTRACT_VERSION,
         cnyes_bulk_fingerprint(source), source.authorization_fingerprint, sha, encoded(observation)))
    if current and datetime.fromisoformat(current[0]) > datetime.fromisoformat(stamp):
        marker = queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages '
                                  'WHERE scope_url=? AND page=?', (info['scope'], info['page'])).fetchone()
        reason = _decode_unproven_marker(marker[0]) if marker else None
        repair_unproven_head = bool(reason is not None and origin == 'verified_retained_source_response'
                and reason.get('actual_observation_sha256') == current['observation_sha256']
                and reason.get('reason') in ('original_envelope_request_mismatch',
                    'original_envelope_authorization_mismatch', 'original_envelope_invalid',
                    'original_envelope_unavailable'))
        if not repair_unproven_head:
            for scope in affected_scopes:
                reconcile_scope(queue, scope, source)
            return  # Old retained bytes never overwrite forward-confirmed metadata.
    queue.db.execute('INSERT OR REPLACE INTO cnyes_history_page_heads VALUES (?,?,?,?)', (info['scope'], info['page'], sha, stamp))
    from downloader.news_fulltext_cnyes_priority import notify_head
    notify_head(queue, info['scope'], info['page'], sha)
    _remove_unproven_marker(queue, info['scope'], info['page'])
    index_scope(queue, info['scope'], source.provider)
    reconcile_scope(queue, info['scope'], source)
    # A local inventory is still collecting independent original pages. Do
    # not wake or alias neighbouring source jobs halfway through that scan.
    fence = queue.db.execute('SELECT 1 FROM cnyes_history_local_inventory_scopes WHERE scope_url=?',
                             (info['scope'],)).fetchone()
    if not fence:
        share_matching_page_IDs(queue, info['scope'], sha, info, source, stamp)
    for scope in affected_scopes - {info['scope']}:
        reconcile_scope(queue, scope, source)
    resolve_forward_retained_outcomes(queue, info['scope'], info['page'], source)
    if fence:
        _release_complete_local_inventory_fence(queue, info['scope'], source)


def schedule_original_page_check(queue, url, *, reason, evidence_sha):
    """One purpose-specific recheck; never touch failed/retired source jobs."""
    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
    if (not job or job['error_code'] is not None or job['attempts'] != 0
            or job['superseded_by_windows_json'] is not None):
        return False
    marker = 'cnyes_history_direct_page_recheck_v1:' + digest((url, evidence_sha))
    if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
        return False
    queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, encoded({
        'prior_job': dict(job), 'reason': reason, 'changed_at_utc': now(),
        'source_failures_or_deadlines_reset': False})))
    queue.db.execute('UPDATE discovery SET due=min(due,?),etag=NULL,last_modified=NULL WHERE url=?', (time.time(), url))
    return True


def schedule_consistency_recheck(queue, url, source, *, scope, evidence_sha, confirmed_at):
    """Retry an unresolved list contradiction, never a completed diagnostic.

    Keep the original one-shot witness immutable. A successful but still
    contradictory response is not list completion: the next check uses the
    source's discovery cadence with capped exponential spacing. The actual
    successful response clock fences each attempt across owner restarts; a
    loop, new proof SHA or process restart cannot create a fresh allowance.
    Refused/failed/split jobs retain their own stronger controls.
    """
    if (not source.allowed or source.acquisition_mode == 'finite' or source.known_url_only
            or wordpress_base(url) not in source.cnyes_apis or cnyes_scope(url) != scope):
        return False
    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
    if (not job or job['kind'] != 'cnyes_news' or job['provider'] != source.provider
            or job['error_code'] is not None or job['attempts'] != 0
            or job['superseded_by_windows_json'] is not None or job['due'] <= time.time()):
        return False
    current = queue.db.execute('SELECT state,source_bulk_fingerprint,authorization_fingerprint,evidence_contract_version '
        'FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()
    if (not current or current['state'] not in REPAIR_STATES
            or tuple(current)[1:] != (cnyes_bulk_fingerprint(source), source.authorization_fingerprint, CONTRACT_VERSION)):
        return False
    marker = 'cnyes_history_direct_page_recheck_v1:' + digest((url, evidence_sha))
    # A different representation/proof must not erase this URL's retry history.
    stable = 'cnyes_history_consistency_recheck_v2:' + digest((
        url, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (stable,)).fetchone()
    try:
        previous = json.loads(saved[0]) if saved else None
        if saved:
            if (type(previous) is not dict
                    or previous['contract_version'] != CONSISTENCY_RECHECK_CONTRACT_VERSION
                    or previous['url'] != url or previous['scope_url'] != scope):
                return False
            marker = previous['prior_v1_marker']
        page = int(parse_qs(url.split('?', 1)[1]).get('page', ['1'])[0])
        observed = queue.db.execute('SELECT h.last_confirmed_at_utc,o.response_sha256,o.observation_json '
            'FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
            'ON o.observation_sha256=h.observation_sha256 WHERE h.scope_url=? AND h.page=? '
            'AND o.evidence_contract_version=? AND o.source_bulk_fingerprint=? AND o.authorization_fingerprint=?',
            (scope, page, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
        if (not observed or observed['last_confirmed_at_utc'] != confirmed_at
                or observed['response_sha256'] != job['response_sha256']):
            return False
        coverage = queue.db.execute('SELECT total_articles,total_pages FROM bulk_api_coverage WHERE base_url=?', (scope,)).fetchone()
        pagination = json.loads(observed['observation_json'])['pagination']
        if not coverage or (pagination['total'], pagination['pages']) == tuple(coverage):
            return False  # Waiting for other pages is not permission to revisit a healthy page.
        prior = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if not prior:
            if saved:
                return False
            return schedule_original_page_check(queue, url,
                reason='contradictory_list_page_evidence', evidence_sha=evidence_sha)
        original = json.loads(prior[0])
        original_job = original['prior_job']
        if (type(original_job) is not dict or original_job['url'] != url
                or original_job['kind'] != 'cnyes_news' or original_job['provider'] != source.provider
                or original_job['attempts'] != 0 or original_job['error_code'] is not None
                or original_job['superseded_by_windows_json'] is not None):
            return False
        checked = datetime.fromisoformat(job['last_checked_at_utc'])
        confirmed = datetime.fromisoformat(confirmed_at)
        initial = datetime.fromisoformat(original['changed_at_utc'])
        original_checked = datetime.fromisoformat(original_job['last_checked_at_utc'])
        if (any(clock.tzinfo is None for clock in (checked, confirmed, initial, original_checked))
                or not original_checked <= initial < confirmed <= checked
                or original['reason'] != 'contradictory_list_page_evidence'):
            return False
        count = previous['attempts'] if saved else 0
        if type(count) is not int or count < (1 if saved else 0):
            return False
        if saved:
            if (previous['prior_v1_marker_sha256'] != hashlib.sha256(prior[0].encode()).hexdigest()
                    or checked <= datetime.fromisoformat(previous['successful_response_checked_at_utc'])
                    or confirmed <= datetime.fromisoformat(previous['page_confirmed_at_utc'])):
                return False
        delay = source.discovery_interval_seconds * min(2 ** min(count, 4), CONSISTENCY_RECHECK_MAX_FACTOR)
        due = max(time.time(), checked.timestamp() + delay)
    except (IndexError, KeyError, ValueError, TypeError, OverflowError):
        # A malformed/missing old witness is not permission to reset a budget.
        return False
    if due >= job['due']:
        return False
    proof = {'contract_version': CONSISTENCY_RECHECK_CONTRACT_VERSION,
        'url': url, 'scope_url': scope, 'attempts': count + 1,
        'successful_response_checked_at_utc': job['last_checked_at_utc'],
        'page_confirmed_at_utc': confirmed_at, 'scheduled_at_utc': now(),
        'current_evidence_sha256': evidence_sha,
        'next_due_epoch': due, 'spacing_seconds': delay,
        'prior_v1_marker': marker, 'prior_v1_marker_sha256': hashlib.sha256(prior[0].encode()).hexdigest(),
        'prior_discovery_job': dict(job), 'source_failures_or_deadlines_reset': False,
        'article_heads_or_completed_questions_requeued': False}
    queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (stable, encoded(proof)))
    queue.db.execute('UPDATE discovery SET due=?,etag=NULL,last_modified=NULL WHERE url=?', (due, url))
    queue.metrics.count(source.provider, 'history_consistency_rechecks_scheduled')
    return True


def _local_inventory_defers_source_recheck(queue, scope):
    """Finish local evidence first; never reset publisher failures or clocks."""
    marker = queue.db.execute('SELECT scan_state FROM cnyes_history_local_inventory_scopes '
                              'WHERE scope_url=?', (scope,)).fetchone()
    if not marker:
        return False
    if marker['scan_state'] == 'scanning' or queue.db.execute(
            "SELECT 1 FROM cnyes_history_retained_work WHERE scope_url=? "
            "AND state IN ('pending','overflow_pending','local_retry') LIMIT 1", (scope,)).fetchone():
        return True
    # Once local work settles, use the existing dispatch fairness policy.
    # Waiting for every article to drain could starve a growing history queue.
    queue.db.execute('DELETE FROM cnyes_history_local_inventory_scopes WHERE scope_url=?', (scope,))
    return False


def _release_complete_local_inventory_fence(queue, scope, source):
    """Retire a local-only fence after all pages have current proof."""
    queue.db.execute('DELETE FROM cnyes_history_local_inventory_scopes WHERE scope_url=? AND scan_state=? '
        'AND EXISTS (SELECT 1 FROM cnyes_history_scope_heads h WHERE h.scope_url=? AND h.state IN (?,?,?) '
        'AND h.evidence_contract_version=? AND h.source_bulk_fingerprint=? AND h.authorization_fingerprint=?) '
        "AND NOT EXISTS (SELECT 1 FROM cnyes_history_retained_work w WHERE w.scope_url=? "
        "AND w.state IN ('pending','overflow_pending','local_retry'))",
        (scope, 'ready', scope, *COMPLETE_STATES, CONTRACT_VERSION, cnyes_bulk_fingerprint(source),
         source.authorization_fingerprint, scope))


def repair_scope_pages(queue, scope, source):
    """Repair missing/currently contradictory evidence, not completed history.

    An old successful job's annual revisit is not a list-completeness proof.
    Prefer the existing retained-envelope worker; otherwise wake only that
    original successful page. Refusals, active/due jobs and split parents keep
    their own schedules. Conflicting successful observations use durable,
    cadence-spaced recovery rather than a permanent one-shot dead end.
    """
    result = {'retained_pages_queued': 0, 'page_checks_scheduled': 0}
    key = scope_key(scope)
    if (not key or not source.allowed or key[0] not in source.cnyes_apis
            or source.acquisition_mode == 'finite' or source.known_url_only):
        return result
    if _local_inventory_defers_source_recheck(queue, scope):
        return result
    coverage = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?', (scope,)).fetchone()
    if (not coverage or not 1 <= coverage['total_pages'] <= 30
            or queue.db.execute('SELECT 1 FROM bulk_window_splits WHERE scope_url=?', (scope,)).fetchone()):
        return result
    observations = {}
    for row in queue.db.execute('SELECT h.page AS head_page,h.last_confirmed_at_utc,o.* '
            'FROM cnyes_history_page_heads h LEFT JOIN cnyes_history_page_observations o '
            'ON o.observation_sha256=h.observation_sha256 WHERE h.scope_url=? AND h.page<=?',
            (scope, coverage['total_pages'])):
        observation = _checked_page_observation(row, source)
        if observation is not None and row['head_page'] == row['page']:
            observations[row['head_page']] = (row['observation_sha256'], observation, row['last_confirmed_at_utc'])
    query = parse_qs(scope.split('?', 1)[1])
    batch_scan_complete = None
    for page in range(1, coverage['total_pages'] + 1):
        prior = observations.get(page)
        bad_reference = (prior and prior[1]['scope_url'] != scope
                         and not _matching_shared_reference(queue, scope, page, prior[0], prior[1]))
        if prior and not bad_reference and (prior[1]['pagination']['total'], prior[1]['pagination']['pages'],
                prior[1]['contract_version'], prior[1]['source_bulk_fingerprint'],
                prior[1]['authorization_fingerprint'], prior[1]['source_day_offset_seconds']) == (
                coverage['total_articles'], coverage['total_pages'], CONTRACT_VERSION,
                cnyes_bulk_fingerprint(source), source.authorization_fingerprint, CNYES_QUERY_DAY_OFFSET_SECONDS):
            continue
        url = cnyes_url(key[0], page, start=int(query['startAt'][0]), end=int(query['endAt'][0]))
        job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
        if (not job or job['kind'] != 'cnyes_news' or job['provider'] != source.provider
                or job['error_code'] is not None or job['attempts'] != 0
                or job['superseded_by_windows_json'] is not None or job['due'] <= time.time()):
            continue
        work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
        if work and work['state'] in ('pending', 'overflow_pending', 'local_retry'):
            continue  # A local candidate or its retry must settle before a new source check.
        if not prior:
            legacy = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                      (scope, page)).fetchone()
            if not legacy and page == 1 and coverage['total_articles'] == 0 and coverage['total_pages'] == 1:
                # A valid zero-result response has no legacy page-ID row. Its
                # empty set is an expectation, never proof without raw bytes.
                legacy = {'total': 0, 'ids_sha256': hashlib.sha256(json.dumps([]).encode()).hexdigest(),
                          'observed_at_utc': coverage['observed_at_utc']}
            sha = job['response_sha256']
            files_present = False
            if not work and legacy and sha and job['last_checked_at_utc']:
                try:
                    files_present = any(path.is_file() and path.with_suffix('.parquet.receipt.json').is_file()
                                        for path, _ in _retained_envelopes(
                                            queue.root, sha, url, source.authorization_fingerprint))
                except (OSError, MemoryError):
                    # Persist the candidate for the local worker; an unreadable
                    # disk is not evidence that a publisher request is needed.
                    files_present = True
                if not files_present:
                    files_present = bool(queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates '
                        'WHERE request_url=? AND response_sha256=? LIMIT 1', (url, sha)).fetchone())
            if files_present:
                queue.db.execute('INSERT INTO cnyes_history_retained_work '
                    '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
                    'VALUES (?,?,?,?,?,?,?)', (url, scope, page, sha, legacy['total'], legacy['ids_sha256'],
                                               legacy['observed_at_utc']))
                result['retained_pages_queued'] += 1
                continue
            if legacy and legacy['total'] > 0 and _retained_envelopes(queue.root, sha):
                # An unindexed accepted body batch may still contain this
                # exact original. Do not create a redundant publisher job
                # while that bounded local inventory is incomplete.
                if batch_scan_complete is None:
                    batch_scan_complete = accepted_batch_inventory_complete(queue, source)
                if not batch_scan_complete:
                    continue
        evidence = digest((scope, page, coverage['total_articles'], coverage['total_pages'],
            CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
            prior[0] if prior else ('missing_page', job['last_checked_at_utc'], job['response_sha256'])))
        prior_current = prior and prior[1]['scope_url'] == scope and (
            prior[1]['contract_version'], prior[1]['source_bulk_fingerprint'],
            prior[1]['authorization_fingerprint'], prior[1]['source_day_offset_seconds']) == (
                CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
                CNYES_QUERY_DAY_OFFSET_SECONDS)
        if bad_reference:
            result['page_checks_scheduled'] += schedule_original_page_check(queue, url,
                reason='invalid_original_page_ID_reference', evidence_sha=evidence)
        elif prior_current:
            result['page_checks_scheduled'] += schedule_consistency_recheck(queue, url, source,
                scope=scope, evidence_sha=evidence, confirmed_at=prior[2])
        else:
            result['page_checks_scheduled'] += schedule_original_page_check(queue, url,
                reason='missing_current_list_page_evidence' if not prior else 'contradictory_list_page_evidence',
                evidence_sha=evidence)
    return result


def repair_incomplete_scopes(queue, *, stop=None, limit=8, max_seconds=.2, allow_one_after_timeout=False):
    """Bounded, indexed restart recovery; rotate past unresolved early scopes."""
    started, done = time.monotonic(), 0
    result = {'scopes_checked': 0, 'retained_pages_queued': 0, 'page_checks_scheduled': 0}
    source = queue.registry.sources.get('cnyes')
    if (not source or not source.allowed or limit <= 0 or max_seconds <= 0
            or stop is not None and stop.is_set()):
        return result
    # Any explicit sweep supersedes the cached completion of an earlier one.
    queue._cnyes_history_repair_covered = None
    cursor = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone()
    if not cursor:
        # A new sweep starts from the beginning and covers earlier changes.
        queue._cnyes_history_repair_resweep = False
    prefix = (source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)
    after = cursor[0] if cursor else ''
    params = tuple(value for state in REPAIR_STATES for value in (*prefix, state, after)) + (limit,)
    scopes = queue.db.execute(REPAIR_SCOPE_SEEK, params).fetchall()
    with queue.db:
        for row in scopes:
            if (stop is not None and stop.is_set()
                    or (done > 0 or not allow_one_after_timeout) and time.monotonic() - started >= max_seconds):
                break
            for name, count in repair_scope_pages(queue, row[0], source).items():
                result[name] += count
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (REPAIR_CURSOR, row[0]))
            done += 1
        if done == len(scopes) and len(scopes) < limit:
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (REPAIR_CURSOR,))
    result['scopes_checked'] = done
    if done < len(scopes):
        # A timeout or stop before the first row does not write a cursor.
        # The owner must still know that the fetched slice is unfinished.
        result['unfinished_slice'] = True
    return result


def share_matching_page_IDs(queue, scope, sha, info, source, stamp):
    """Reuse actual IDs/dates across equal-day queries one proven page at a time.

    Keep the observation's ORIGINAL query, raw artifact and capture clock.
    The other page's ORIGINAL ID hash/clock is an immutable reference receipt,
    not a fabricated raw response or an alias article. Missing/disagreeing
    pages fall back to their original canonical jobs, with refusal gates intact.
    """
    key = scope_key(scope)
    query_source = queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations WHERE observation_sha256=?',
                                    (sha,)).fetchone()
    if not query_source:
        return
    observed = json.loads(query_source[0])
    if any(not key[1] <= (entry['publishAt'] + CNYES_QUERY_DAY_OFFSET_SECONDS) // 86400 <= key[2]
           for entry in observed['identities']):
        return
    for (other,) in queue.db.execute('SELECT scope_url FROM cnyes_history_scope_keys WHERE '
        'endpoint=? AND first_day=? AND last_day=? AND provider=? AND scope_url!=?', (*key, source.provider, scope)).fetchall():
        if queue.db.execute('SELECT 1 FROM cnyes_history_local_inventory_scopes WHERE scope_url=?', (other,)).fetchone():
            continue  # The original scope is still collecting its own pages.
        coverage = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?', (other,)).fetchone()
        if not coverage or info['page'] > min(coverage['total_pages'], 30):
            continue
        existing = queue.db.execute('SELECT o.*,h.last_confirmed_at_utc FROM cnyes_history_page_heads h '
            'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
            'WHERE h.scope_url=? AND h.page=?', (other, info['page'])).fetchone()
        own = _checked_page_observation(existing, source)
        if own:
            compatible = (own['contract_version'] == CONTRACT_VERSION
                and own['source_bulk_fingerprint'] == cnyes_bulk_fingerprint(source)
                and own['authorization_fingerprint'] == source.authorization_fingerprint)
            if compatible and (own['scope_url'] == other or datetime.fromisoformat(existing['last_confirmed_at_utc']) >= datetime.fromisoformat(stamp)):
                continue  # Independent/current source evidence is stronger than a shared reference.
        original = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                    (other, info['page'])).fetchone()
        if (original is None or coverage['total_articles'] != info['total'] or coverage['total_pages'] != info['pages']
                or (original['total'], original['ids_sha256']) != (info['total'], info['ids_sha256'])):
            q = parse_qs(other.split('?', 1)[1])
            url = cnyes_url(key[0], info['page'], start=int(q['startAt'][0]), end=int(q['endAt'][0]))
            if not _remove_unproven_marker(queue, other, info['page']):
                reconcile_scope(queue, other, source)
                continue
            queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (other, info['page'], encoded({
                'actual_observation_sha256': sha, 'actual_reference_scope': scope,
                'actual_confirmed_at_utc': stamp, 'original_page_evidence': None if original is None else dict(original),
                'original_request_url': url, 'reason': 'unproven_or_different_original_page_ID_set'})))
            schedule_original_page_check(queue, url, reason='unproven_or_different_original_page_ID_set', evidence_sha=sha)
            reconcile_scope(queue, other, source)
            continue
        if not _remove_unproven_marker(queue, other, info['page']):
            reconcile_scope(queue, other, source)
            continue
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_page_ID_references VALUES (?,?,?,?,?)',
                         (other, info['page'], sha, encoded(dict(original)), scope))
        queue.db.execute('INSERT OR REPLACE INTO cnyes_history_page_heads VALUES (?,?,?,?)',
                         (other, info['page'], sha, stamp))
        from downloader.news_fulltext_cnyes_priority import notify_head
        notify_head(queue, other, info['page'], sha)
        reconcile_scope(queue, other, source)
        resolve_forward_retained_outcomes(queue, other, info['page'], source)


def _same_day_page_requests(first, second):
    """Constrain equivalence to the complete, known query grammar."""
    a, b = urlsplit(first), urlsplit(second)
    if ((a.scheme, a.netloc, a.path) != (b.scheme, b.netloc, b.path)
            or a.fragment or b.fragment or first == second):
        return False
    left, right = (parse_qs(value.query, keep_blank_values=True, strict_parsing=True) for value in (a, b))
    keys = {'startAt', 'endAt', 'page', 'limit'}
    if (set(left) != keys or set(right) != keys
            or any(len(query[key]) != 1 for query in (left, right) for key in keys)
            or left['page'] != right['page'] or left['limit'] != ['30'] or right['limit'] != ['30']):
        return False
    return bool(scope_key(first) and scope_key(first) == scope_key(second))


def _recover_equal_day_original_mismatch(queue, source, row, target, *, verifier=None):
    """Repair only list-ID evidence using a separately verified real original.

    The old direct observation remains intact and the alias never claims the
    other request's raw response or a historical point-in-time snapshot.
    Caller owns the auditor's transaction; this helper makes no HTTP requests.
    """
    from downloader.news_fulltext_queue import replay_source_path

    scope, page, old_sha = row['head_scope'], row['head_page'], row['head_sha']
    if (not source.allowed or target['provider'] != source.provider
            or wordpress_base(target['request_url']) not in source.cnyes_apis
            or target['scope_url'] != scope or target['page'] != page
            or target['contract_version'] != CONTRACT_VERSION
            or target['source_day_offset_seconds'] != CNYES_QUERY_DAY_OFFSET_SECONDS
            or target['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
            or target['authorization_fingerprint'] != source.authorization_fingerprint):
        return False
    artifact = target['artifact']
    if artifact.get('kind') not in ('canonical_quarantine', 'cnyes_history_source_envelope'):
        return False
    path = replay_source_path(queue.root, artifact['parquet_path'])
    receipt = replay_source_path(queue.root, artifact['receipt_path'])
    before = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt)
    verified = verifier.verified_original(queue.root, artifact, target['response_sha256']) if verifier else None
    if verified is None:
        verified = _read_original_envelope_identity(path, receipt, target['response_sha256'],
            expected_parquet_path=artifact['parquet_path'], expected_kind=artifact['kind'], include_original=True)
    if verified is None:
        return False
    identity, receipt_sha, parquet_sha, captured, original = verified
    actual_url = identity[0]
    if (identity[1] != source.authorization_fingerprint or json.loads(original['authorization_json']) != source.authorization
            or wordpress_base(actual_url) not in source.cnyes_apis
            or not _same_day_page_requests(actual_url, target['request_url'])):
        return False
    actual_scope = cnyes_scope(actual_url)
    if queue.db.execute('SELECT 1 FROM cnyes_history_local_inventory_scopes WHERE scope_url=?', (scope,)).fetchone():
        return False
    source_marker = queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages WHERE scope_url=? AND page=?',
                                     (actual_scope, page)).fetchone()
    source_marker_data = _decode_unproven_marker(source_marker[0]) if source_marker else None
    if source_marker and (source_marker_data is None
                          or source_marker_data.get('reason') != 'original_envelope_request_mismatch'
                          or source_marker_data.get('actual_observation_sha256') != old_sha
                          or source_marker_data.get('original_request_url') not in (None, target['request_url'])):
        return False
    marker = queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages WHERE scope_url=? AND page=?',
                              (scope, page)).fetchone()
    marker_data = _decode_unproven_marker(marker[0]) if marker else None
    if marker and (marker_data is None or marker_data.get('reason') != 'original_envelope_request_mismatch'
                   or marker_data.get('actual_observation_sha256') != old_sha
                   or marker_data.get('original_request_url') not in (None, target['request_url'])):
        return False
    head = queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=? AND page=?',
                            (scope, page)).fetchone()
    real = queue.db.execute('SELECT h.last_confirmed_at_utc,o.* FROM cnyes_history_page_heads h '
        'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
        'WHERE h.scope_url=? AND h.page=?', (actual_scope, page)).fetchone()
    actual = _checked_page_observation(real, source)
    if (not head or head['observation_sha256'] != old_sha or not actual
            or actual['contract_version'] != CONTRACT_VERSION
            or actual['source_day_offset_seconds'] != CNYES_QUERY_DAY_OFFSET_SECONDS
            or actual['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
            or actual['authorization_fingerprint'] != source.authorization_fingerprint):
        return False
    direct = (actual['scope_url'] == actual_scope and actual['request_url'] == actual_url
              and actual['artifact'] == artifact and actual['response_sha256'] == target['response_sha256'])
    if direct and source_marker:
        return False  # A direct source head with its own marker needs its own proof.
    # Some legacy source heads are themselves references to the target, so
    # neither head has a direct observation despite the original raw response
    # still being present.  Only an independently checked same-day ID receipt
    # permits reconstructing that missing observation from the real envelope.
    alias_reference = None
    source_head = source_evidence = source_coverage = None
    if not direct:
        if (actual_scope == scope or real['observation_sha256'] != old_sha or actual != target
                or queue.db.execute('SELECT 1 FROM cnyes_history_local_inventory_scopes WHERE scope_url=?',
                                    (actual_scope,)).fetchone()):
            return False
        alias_reference = _matching_shared_reference(queue, actual_scope, page, old_sha, target,
                                                       allow_original_request_mismatch=True)
        source_head = queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=? AND page=?',
                                       (actual_scope, page)).fetchone()
        source_evidence = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                           (actual_scope, page)).fetchone()
        source_coverage = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?',
                                           (actual_scope,)).fetchone()
        if not alias_reference or not source_head or not source_evidence or not source_coverage:
            return False
    raw = original['raw_response_body']
    raw = raw.encode() if isinstance(raw, str) else raw
    info, posts = cnyes_pagination(SimpleNamespace(url=actual_url, body=raw,
                                                     headers={'content-type': 'application/json'}))
    identities = [{'newsId': post['newsId'], 'publishAt': post['publishAt'],
                   'source_post_sha256': digest(post)} for post in posts]
    key = scope_key(scope)
    if (direct and (info != actual['pagination'] or identities != actual['identities'])
            or identities != target['identities']
            or any(type(post.get('payment')) is not int or post['payment'] != 0 for post in posts)
            or any(not key[1] <= (item['publishAt'] + CNYES_QUERY_DAY_OFFSET_SECONDS) // 86400 <= key[2]
                   for item in identities)
            # Individual pages below the publisher cap can be reused even
            # when the whole day overflows; reconcile_scope keeps that day
            # explicitly incomplete until its separate gap is resolved.
            or not 1 <= page <= min(info['pages'], 30)
            or any(target['pagination'][field] != info[field] for field in
                   ('page', 'total', 'pages', 'ids_sha256', 'returned_rows', 'unique_rows', 'duplicate_rows'))):
        return False
    evidence = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                (scope, page)).fetchone()
    coverage = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?', (scope,)).fetchone()
    if (not evidence or not coverage or (evidence['total'], evidence['ids_sha256']) != (info['total'], info['ids_sha256'])
            or (coverage['total_articles'], coverage['total_pages']) != (info['total'], info['pages'])):
        return False
    if not direct and ((source_evidence['total'], source_evidence['ids_sha256']) != (info['total'], info['ids_sha256'])
                       or (source_coverage['total_articles'], source_coverage['total_pages']) != (info['total'], info['pages'])):
        return False
    source_observed = datetime.fromisoformat(actual['source_observed_at_utc']) if direct else captured
    source_confirmed = datetime.fromisoformat(real['last_confirmed_at_utc'])
    target_observed = datetime.fromisoformat(target['source_observed_at_utc'])
    target_confirmed = datetime.fromisoformat(head['last_confirmed_at_utc'])
    target_receipt = datetime.fromisoformat(evidence['observed_at_utc'])
    clocks = (source_observed, source_confirmed, target_observed, target_confirmed, target_receipt)
    if (any(value.tzinfo is None or value.utcoffset() is None or value > datetime.now(timezone.utc) for value in clocks)
            or source_observed < captured or source_confirmed < source_observed
            or target_observed < captured or target_confirmed < target_observed or target_receipt < captured):
        return False
    direct_observation = None
    source_sha = real['observation_sha256']
    if not direct:
        source_receipt = datetime.fromisoformat(source_evidence['observed_at_utc'])
        if (source_receipt.tzinfo is None or source_receipt.utcoffset() is None
                or source_receipt < captured or source_receipt > datetime.now(timezone.utc)
                or source_confirmed < target_observed):
            return False
        # Preserve the true original capture time.  The heads retain their
        # prior confirmation clocks, which are independently checked above.
        direct_observation = {'contract_version': CONTRACT_VERSION, 'provider': source.provider,
            'request_url': actual_url, 'scope_url': actual_scope, 'page': page,
            'response_sha256': target['response_sha256'], 'pagination': info, 'identities': identities,
            'source_day_offset_seconds': CNYES_QUERY_DAY_OFFSET_SECONDS,
            'source_observed_at_utc': original['captured_at_utc'], 'evidence_recorded_at_utc': now(),
            'origin': 'verified_retained_source_response', 'source_bulk_fingerprint': cnyes_bulk_fingerprint(source),
            'authorization_fingerprint': source.authorization_fingerprint, 'artifact': artifact,
            'body_completeness_verified': False}
        source_sha = digest(direct_observation)
        existing_source = queue.db.execute('SELECT 1 FROM cnyes_history_page_observations '
            'WHERE scope_url=? AND page=? AND response_sha256=? AND evidence_contract_version=? '
            'AND source_bulk_fingerprint=? AND authorization_fingerprint=?',
            (actual_scope, page, target['response_sha256'], CONTRACT_VERSION,
             cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
        if existing_source or queue.db.execute('SELECT 1 FROM cnyes_history_page_observations '
                                               'WHERE observation_sha256=?', (source_sha,)).fetchone():
            return False
    reference = (scope, page, source_sha, encoded(dict(evidence)), actual_scope)
    existing = queue.db.execute('SELECT * FROM cnyes_history_page_ID_references '
        'WHERE scope_url=? AND page=? AND observation_sha256=?', reference[:3]).fetchone()
    if existing and tuple(existing) != reference:
        return False
    event = {'contract': 'cnyes_equal_day_original_mismatch_v1', 'reason': 'original_envelope_request_mismatch',
        'original_target_head': _audit_sqlite_row(head), 'original_target_observation': _audit_sqlite_row(row),
        'original_target_marker': None if marker is None else _audit_sqlite_row(marker),
        'original_target_page_ID_evidence': _audit_sqlite_row(evidence),
        'actual_source_observation_sha256': source_sha,
        'actual_source_request_url': actual_url, 'actual_source_captured_at_utc': original['captured_at_utc'],
        'actual_source_parquet_sha256': parquet_sha, 'actual_source_receipt_sha256': receipt_sha.hex(),
        'publisher_requests': 0, 'body_completeness_verified': False, 'historical_point_in_time': False}
    if direct_observation is not None:
        event.update({'reconstructed_source_from_retained_original': True,
                      'original_source_head': _audit_sqlite_row(source_head),
                      'original_source_reference': _audit_sqlite_row(alias_reference),
                      'original_source_marker': None if source_marker is None else _audit_sqlite_row(source_marker),
                      'original_source_page_ID_evidence': _audit_sqlite_row(source_evidence)})
    event_key = 'cnyes_history_equal_day_original_recovery_v1:' + digest(event)
    event_value = encoded(event)
    prior = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (event_key,)).fetchone()
    if prior and prior[0] != event_value:
        return False
    if before != (_original_envelope_file_signature(path), _original_envelope_file_signature(receipt)):
        raise OSError('original envelope changed during equal-day recovery')
    if not queue.db.in_transaction:
        queue.db.execute('BEGIN')
    queue.db.execute('SAVEPOINT cnyes_equal_day_original_recovery')
    try:
        queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)', (event_key, event_value))
        if queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (event_key,)).fetchone()[0] != event_value:
            raise _CheckpointAuditConflict
        if direct_observation is not None:
            serialized = encoded(direct_observation)
            queue.db.execute('INSERT INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)',
                (actual_scope, page, target['response_sha256'], CONTRACT_VERSION, cnyes_bulk_fingerprint(source),
                 source.authorization_fingerprint, source_sha, serialized))
            stored = queue.db.execute('SELECT * FROM cnyes_history_page_observations WHERE observation_sha256=?',
                                      (source_sha,)).fetchone()
            if not stored or stored['observation_json'] != serialized or _checked_page_observation(stored, source) != direct_observation:
                raise _CheckpointAuditConflict
            changed_source = queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=? '
                'WHERE scope_url=? AND page=? AND observation_sha256=? AND last_confirmed_at_utc=?',
                (source_sha, actual_scope, page, old_sha, source_head['last_confirmed_at_utc'])).rowcount
            if changed_source != 1:
                raise _CheckpointAuditConflict
            if source_marker:
                removed = queue.db.execute('DELETE FROM cnyes_history_unproven_pages '
                    'WHERE scope_url=? AND page=? AND evidence_json=?',
                    (actual_scope, page, source_marker[0])).rowcount
                if removed != 1:
                    raise _CheckpointAuditConflict
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_page_ID_references VALUES (?,?,?,?,?)', reference)
        if tuple(queue.db.execute('SELECT * FROM cnyes_history_page_ID_references '
                'WHERE scope_url=? AND page=? AND observation_sha256=?', reference[:3]).fetchone()) != reference:
            raise _CheckpointAuditConflict
        if marker:
            queue.db.execute('DELETE FROM cnyes_history_unproven_pages WHERE scope_url=? AND page=? AND evidence_json=?',
                             (scope, page, marker[0]))
        changed = queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=? '
            'WHERE scope_url=? AND page=? AND observation_sha256=? AND last_confirmed_at_utc=?',
            (source_sha, scope, page, old_sha, head['last_confirmed_at_utc'])).rowcount
        if changed != 1:
            raise _CheckpointAuditConflict
        # Link the exact old and new page versions to the archived recovery
        # event, in the same savepoint as both head changes.  The dashboard
        # can then distinguish this repair from an unrelated later head change.
        links = [(scope, page, old_sha, source_sha, actual_scope, event_key)]
        if direct_observation is not None:
            links.append((actual_scope, page, old_sha, source_sha, actual_scope, event_key))
        for link in links:
            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_equal_day_recovery_heads VALUES (?,?,?,?,?,?)', link)
            saved = queue.db.execute('SELECT * FROM cnyes_history_equal_day_recovery_heads '
                'WHERE scope_url=? AND page=? AND old_observation_sha256=? AND recovered_observation_sha256=?',
                link[:4]).fetchone()
            if saved is None or tuple(saved) != link:
                raise _CheckpointAuditConflict
        from downloader.news_fulltext_cnyes_priority import notify_head
        notify_head(queue, scope, page, source_sha)
        if direct_observation is not None:
            notify_head(queue, actual_scope, page, source_sha)
            reconcile_scope(queue, actual_scope, source)
            resolve_forward_retained_outcomes(queue, actual_scope, page, source)
        reconcile_scope(queue, scope, source)
        resolve_forward_retained_outcomes(queue, scope, page, source)
        if before != (_original_envelope_file_signature(path), _original_envelope_file_signature(receipt)):
            raise OSError('original envelope changed while recording equal-day recovery')
    except BaseException:
        queue.db.execute('ROLLBACK TO cnyes_equal_day_original_recovery')
        queue.db.execute('RELEASE cnyes_equal_day_original_recovery')
        raise
    queue.db.execute('RELEASE cnyes_equal_day_original_recovery')
    return True


def save_proof(queue, scope, source, state, facts, members):
    previous = queue.db.execute('SELECT state,proof_sha256 FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()
    proof = {'contract_version': CONTRACT_VERSION, 'scope_url': scope, 'provider': source.provider,
        'state': state, 'facts': facts, 'member_ids_sha256': digest(sorted(members)),
        'unique_article_IDs': len(members), 'body_completeness_verified': False,
        'historical_point_in_time': False, 'source_bulk_fingerprint': cnyes_bulk_fingerprint(source)}
    proof['authorization_fingerprint'] = source.authorization_fingerprint
    sha = digest(proof)
    stored = queue.db.execute('SELECT * FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (sha,)).fetchone()
    if stored:
        try:
            valid = stored['scope_url'] == scope and digest(json.loads(stored['proof_json'])) == sha
        except (ValueError, TypeError):
            valid = False
        if not valid:
            original = _audit_sqlite_row(stored)
            reason = 'scope_proof_integrity_mismatch'
            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_corrupt_proof_audit VALUES (?,?,?,?)',
                             (digest((original, reason)), encoded(original), reason, now()))
            queue.db.execute('DELETE FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (sha,))
    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_scope_proofs VALUES (?,?,?)', (sha, scope, encoded(proof)))
    queue.db.execute('INSERT OR REPLACE INTO cnyes_history_scope_heads VALUES (?,?,?,?,?,?,?,?)',
                     (scope, source.provider, state, sha, encoded(sorted(members)), cnyes_bulk_fingerprint(source),
                      source.authorization_fingerprint, CONTRACT_VERSION))
    if state in REPAIR_STATES and (not previous or previous['proof_sha256'] != sha):
        # A changed scope must be included even if an earlier sweep completed.
        # A transaction rollback can only cause an extra, harmless re-sweep.
        queue._cnyes_history_repair_covered = None
        queue._cnyes_history_repair_resweep = True
    if state not in COMPLETE_STATES and previous and previous[0] in COMPLETE_STATES:
        queue.db.execute('INSERT OR IGNORE INTO bulk_window_gaps VALUES (?,?,?,?)',
                         (scope, source.provider, 'cnyes_source_list_evidence_changed', now()))
    if state in COMPLETE_STATES:
        gap = queue.db.execute('SELECT * FROM bulk_window_gaps WHERE scope_url=?', (scope,)).fetchone()
        if gap:
            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_gap_retirements VALUES (?,?,?,?)',
                             (scope, sha, encoded(dict(gap)), now()))
            queue.db.execute('DELETE FROM bulk_window_gaps WHERE scope_url=?', (scope,))
    return sha


def reconcile_scope(queue, scope, source, *, _current_contract_only=False):
    coverage = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?', (scope,)).fetchone()
    if not coverage:
        return
    if coverage['total_pages'] > 30:
        if queue.db.execute('SELECT 1 FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone():
            save_proof(queue, scope, source, 'source_page_cap_exceeded', {
                'total_articles_raw': coverage['total_articles'], 'total_pages_raw': coverage['total_pages']}, [])
            reconcile_parents(queue, scope, source, _current_contract_only=_current_contract_only)
        return
    rows = queue.db.execute('SELECT h.page AS head_page,h.observation_sha256 AS head_observation_sha256,o.* '
        'FROM cnyes_history_page_heads h '
        'LEFT JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
        'WHERE h.scope_url=? AND h.page<=? ORDER BY h.page', (scope, coverage['total_pages'])).fetchall()
    observations = [_checked_page_observation(row, source) for row in rows]
    pages, total = coverage['total_pages'], coverage['total_articles']
    facts = {'total_articles_raw': total, 'total_pages_raw': pages,
             'page_observation_sha256': [row['head_observation_sha256'] for row in rows], 'source_day_bounds': scope_key(scope)[1:],
             'shared_page_ID_references': []}
    invalid = [{'page': row['head_page'], 'observation_sha256': row['head_observation_sha256']}
               for row, observation in zip(rows, observations)
               if observation is None or observation['page'] != row['head_page']]
    if invalid:
        facts['invalid_page_evidence'] = invalid
        save_proof(queue, scope, source, 'invalid_page_evidence', facts, [])
        repair_scope_pages(queue, scope, source)
        reconcile_parents(queue, scope, source, _current_contract_only=_current_contract_only)
        return
    members, identities, state = set(), {}, 'waiting_page_evidence'
    if {row['page'] for row in observations} == set(range(1, pages + 1)):
        state = 'list_complete'
        for row, observation in zip(rows, observations):
            info = observation['pagination']
            if (info['total'] != total or info['pages'] != pages
                    or observation['contract_version'] != CONTRACT_VERSION
                    or observation['source_day_offset_seconds'] != CNYES_QUERY_DAY_OFFSET_SECONDS
                    or observation['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
                    or observation['authorization_fingerprint'] != source.authorization_fingerprint):
                state = 'inconsistent_source_snapshot'
                break
            if observation['scope_url'] != scope:
                reference = _matching_shared_reference(queue, scope, row['head_page'],
                    row['head_observation_sha256'], observation)
                if not reference:
                    state = 'inconsistent_original_page_ID_reference'
                    break
                facts['shared_page_ID_references'].append(dict(reference))
            first, last = facts['source_day_bounds']
            for entry in observation['identities']:
                day = (entry['publishAt'] + CNYES_QUERY_DAY_OFFSET_SECONDS) // 86400
                prior = identities.setdefault(entry['newsId'], (entry['publishAt'], entry['source_post_sha256']))
                if not first <= day <= last or prior != (entry['publishAt'], entry['source_post_sha256']):
                    state = 'inconsistent_source_identity_or_date'
                    break
                members.add(entry['newsId'])
            if state != 'list_complete':
                break
    facts['declared_totals_are_not_unique_article_denominators'] = True
    facts['duplicate_rows_observed'] = sum(row['pagination']['duplicate_rows'] for row in observations)
    unproven = []
    for marker in queue.db.execute('SELECT page,evidence_json FROM cnyes_history_unproven_pages '
                                   'WHERE scope_url=? AND page<=? ORDER BY page', (scope, pages)).fetchall():
        evidence = _decode_unproven_marker(marker['evidence_json'])
        if evidence is None:
            # Keep the exact damaged cell even if a later source response
            # replaces this marker. An unknown marker never proves a page.
            marker_sha, preserved = _archive_corrupt_unproven_marker(queue, scope, marker['page'],
                                                                      marker['evidence_json'])
            evidence = {'reason': 'invalid_unproven_marker', 'page': marker['page'],
                        'marker_sha256': marker_sha, 'archive_preserved': preserved}
            key = scope_key(scope)
            if (preserved and key and key[0] in source.cnyes_apis and source.allowed
                    and source.acquisition_mode != 'finite' and not source.known_url_only):
                query = parse_qs(scope.split('?', 1)[1])
                url = cnyes_url(key[0], marker['page'], start=int(query['startAt'][0]), end=int(query['endAt'][0]))
                schedule_original_page_check(queue, url, reason='invalid_unproven_marker', evidence_sha=marker_sha)
        unproven.append(evidence)
    if unproven and state in ('list_complete', 'waiting_page_evidence'):
        facts['original_page_rechecks_required'] = unproven
        state = 'waiting_original_page_recheck'
    if state == 'list_complete' and facts['shared_page_ID_references']:
        state = 'list_complete_by_page_ID_equivalence'
    save_proof(queue, scope, source, state, facts, members)
    if state in REPAIR_STATES:
        repair_scope_pages(queue, scope, source)
    reconcile_parents(queue, scope, source, _current_contract_only=_current_contract_only)


def reconcile_parents(queue, child, source, *, _path=None, _current_contract_only=False):
    path = frozenset() if _path is None else _path
    if child in path:
        return
    path = path | {child}
    for (parent,) in queue.db.execute('SELECT parent_scope FROM cnyes_history_edges WHERE child_scope=?', (child,)).fetchall():
        if parent in path:
            continue
        gap = queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (parent,)).fetchone()
        previous = queue.db.execute('SELECT provider,evidence_contract_version,source_bulk_fingerprint,'
                                    'authorization_fingerprint FROM cnyes_history_scope_heads WHERE scope_url=?',
                                    (parent,)).fetchone()
        if _current_contract_only and (not previous or tuple(previous) != (
                source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)):
            continue
        if not gap and not previous:
            continue  # Full-history roots are not silently certified by this gap-repair contract.
        children = queue.db.execute('SELECT e.child_scope,h.state,h.proof_sha256,h.member_ids_json,c.total_articles, '
            'h.source_bulk_fingerprint,h.authorization_fingerprint,h.evidence_contract_version '
            'FROM cnyes_history_edges e LEFT JOIN cnyes_history_scope_heads h ON h.scope_url=e.child_scope '
            'LEFT JOIN bulk_api_coverage c ON c.base_url=e.child_scope WHERE e.parent_scope=? ORDER BY e.child_scope',
            (parent,)).fetchall()
        old = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?', (parent,)).fetchone()
        if not old or not children:
            continue
        if any(row['state'] not in COMPLETE_STATES or row['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
               or row['authorization_fingerprint'] != source.authorization_fingerprint
               or row['evidence_contract_version'] != CONTRACT_VERSION for row in children):
            if previous:
                save_proof(queue, parent, source, 'waiting_child_evidence', {
                    'child_scopes': [row['child_scope'] for row in children],
                    'child_states': [row['state'] for row in children]}, [])
                reconcile_parents(queue, parent, source, _path=path, _current_contract_only=_current_contract_only)
            continue
        ranges = sorted(scope_key(row['child_scope'])[1:] for row in children)
        expected = scope_key(parent)[1:]
        all_ids, disjoint = set(), True
        for row in children:
            ids = set(json.loads(row['member_ids_json']))
            disjoint = disjoint and all_ids.isdisjoint(ids)
            all_ids.update(ids)
        if (ranges[0][0] != expected[0] or ranges[-1][1] != expected[1]
                or any(left[1] + 1 != right[0] for left, right in zip(ranges, ranges[1:]))
                or not disjoint or sum(row['total_articles'] for row in children) != old['total_articles']):
            if previous:
                save_proof(queue, parent, source, 'inconsistent_partition_evidence', {
                    'child_proofs': [row['proof_sha256'] for row in children],
                    'original_parent_total_raw': old['total_articles'],
                    'current_child_total_sum': sum(row['total_articles'] for row in children)}, [])
                reconcile_parents(queue, parent, source, _path=path, _current_contract_only=_current_contract_only)
            continue  # Preserve the genuine cap/count conflict, never manufacture a complete parent.
        save_proof(queue, parent, source, 'partition_list_complete', {
            'total_articles_raw': old['total_articles'], 'source_day_bounds': expected,
            'children': [{'scope_url': row['child_scope'], 'proof_sha256': row['proof_sha256']} for row in children],
            'all_child_ID_sets_disjoint': True, 'child_total_sum_matches_retained_parent': True,
            'original_parent_page_evidence_preserved': True,
        }, all_ids)
        reconcile_parents(queue, parent, source, _path=path, _current_contract_only=_current_contract_only)


def progress(db, source=None):
    states = {row[0]: row[1] for row in db.execute('SELECT state,count(*) FROM cnyes_history_scope_heads GROUP BY state')}
    work = {row[0]: row[1] for row in db.execute('SELECT state,count(*) FROM cnyes_history_retained_work GROUP BY state')}
    current = None if source is None else {row[0]: row[1] for row in db.execute(
        'SELECT state,count(*) FROM cnyes_history_scope_heads WHERE provider=? AND evidence_contract_version=? AND source_bulk_fingerprint=? '
        'AND authorization_fingerprint=? GROUP BY state',
        (source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))}
    legacy_scans = {kind: {'scan_state': 'unknown', 'checkpoint_problem': None}
                    for kind in ('mismatch', 'unavailable', 'empty')}
    if source is not None:
        for kind, marker, prefix, position in (
                ('mismatch', LEGACY_MISMATCH_SCAN_KEY, LEGACY_MISMATCH_SCAN_KEY + ':', 'request_url'),
                ('unavailable', LEGACY_UNAVAILABLE_SCAN_KEY, LEGACY_UNAVAILABLE_SCAN_KEY + ':', 'request_url'),
                ('empty', legacy_empty_scan_key(source), LEGACY_EMPTY_SCAN_PREFIX, 'base_url')):
            legacy_scans[kind] = _legacy_scan_status(db, marker, prefix, position)
    scan_state, scan_problem = 'unknown', None
    if source is not None:
        marker_key = overflow_scan_key(source)
        marker = db.execute('SELECT value FROM queue_settings WHERE key=?', (marker_key,)).fetchone()
        if marker is None:
            scan_state = 'not_started'
        else:
            try:
                checkpoint = json.loads(marker[0])
                if (type(checkpoint) is dict and 'version' in checkpoint
                        and (type(checkpoint['version']) is not int
                             or checkpoint['version'] != OVERFLOW_RETAINED_SCAN_VERSION)):
                    scan_problem = 'unsupported_version'
                elif (type(checkpoint) is dict and type(checkpoint.get('version')) is int
                        and checkpoint['version'] == OVERFLOW_RETAINED_SCAN_VERSION
                        and checkpoint.get('state') in ('scanning', 'complete')
                        and type(checkpoint.get('scope_url')) is str and type(checkpoint.get('page')) is int
                        and 0 <= checkpoint['page'] <= 30):
                    scan_state = checkpoint['state']
                else:
                    scan_problem = 'invalid_checkpoint'
            except (TypeError, ValueError):
                scan_problem = 'invalid_checkpoint'
            if scan_problem == 'invalid_checkpoint':
                audit_key = OVERFLOW_RETAINED_SCAN_PREFIX + 'invalid:' + digest((marker_key, _audit_sqlite_row(marker)))
                saved = db.execute('SELECT value FROM queue_settings WHERE key=?', (audit_key,)).fetchone()
                if saved and (type(saved[0]) is not type(marker[0]) or saved[0] != marker[0]):
                    scan_problem = 'audit_conflict'
    retry_overflow = db.execute("SELECT count(*) FROM cnyes_history_retained_local_retries "
        "WHERE original_state='overflow_pending'").fetchone()[0]
    local_recovery = {'contract_version': 1, 'scan_state': scan_state, 'checkpoint_problem': scan_problem,
        'pending_retained_pages': work.get('overflow_pending', 0) + retry_overflow,
        'local_retry_pages': work.get('local_retry', 0), 'local_raw_only': True,
        'category_gap_completion_claimed': False, 'publisher_requests': 0}
    nonzero = {'contract_version': 1, 'scan_state': 'unknown', 'checkpoint_problem': None, 'checked_steps': None,
        'queued_candidate_pages': None, 'local_raw_only': True,
        'all_history_complete': False, 'publisher_requests': 0}
    if source is not None:
        marker_key = nonzero_scan_key(source)
        marker = db.execute('SELECT value FROM queue_settings WHERE key=?', (marker_key,)).fetchone()
        if marker is None:
            nonzero.update(scan_state='not_started', checked_steps=0, queued_candidate_pages=0)
        else:
            try:
                checkpoint = json.loads(marker[0])
                if (type(checkpoint) is dict and 'version' in checkpoint
                        and (type(checkpoint['version']) is not int or checkpoint['version'] != 1)):
                    nonzero['checkpoint_problem'] = 'unsupported_version'
                elif (type(checkpoint) is dict and type(checkpoint.get('version')) is int and checkpoint['version'] == 1
                        and checkpoint.get('state') in ('scanning', 'complete')
                        and type(checkpoint.get('scope_url')) is str and type(checkpoint.get('page')) is int
                        and 0 <= checkpoint['page'] <= 30
                        and type(checkpoint.get('checked')) is int and checkpoint['checked'] >= 0
                        and type(checkpoint.get('queued')) is int and checkpoint['queued'] >= 0):
                    nonzero.update(scan_state=checkpoint['state'], checked_steps=checkpoint['checked'],
                                   queued_candidate_pages=checkpoint['queued'])
                else:
                    nonzero['checkpoint_problem'] = 'invalid_checkpoint'
            except (TypeError, ValueError):
                nonzero['checkpoint_problem'] = 'invalid_checkpoint'
            if nonzero['checkpoint_problem'] == 'invalid_checkpoint':
                audit_key = LEGACY_NONZERO_SCAN_PREFIX + 'invalid:' + digest((marker_key, _audit_sqlite_row(marker)))
                archived = db.execute('SELECT value FROM queue_settings WHERE key=?', (audit_key,)).fetchone()
                if archived and (type(archived[0]) is not type(marker[0]) or archived[0] != marker[0]):
                    nonzero['checkpoint_problem'] = 'audit_conflict'
    accepted = {'contract_version': 1, 'scan_state': 'unknown', 'checkpoint_problem': None, 'scanned_batches': None,
        'indexed_candidates': None, 'local_retry_batches': None, 'local_missing_retry_batches': None,
        'local_invalid_retry_batches': None, 'local_invalid_migration_state': None,
        'next_local_retry_epoch': None,
        'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0}
    if source is not None:
        scan_key = accepted_batch_scan_key(source)
        marker = db.execute('SELECT value FROM queue_settings WHERE key=?', (scan_key,)).fetchone()
        retry = db.execute('SELECT count(*),min(not_before_epoch) FROM cnyes_history_accepted_batch_local_retries '
                           'WHERE scan_key=?', (scan_key,)).fetchone()
        missing = db.execute('SELECT count(*),min(not_before_epoch) FROM cnyes_history_accepted_missing_retries '
                             'WHERE scan_key=?', (scan_key,)).fetchone()
        invalid = (db.execute('SELECT count(*),min(not_before_epoch) FROM cnyes_history_accepted_invalid_retries '
                              'WHERE scan_key=?', (scan_key,)).fetchone()
                   if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                 "AND name='cnyes_history_accepted_invalid_retries'").fetchone() else None)
        migration_state = None
        if invalid is not None:
            migration = db.execute('SELECT value FROM queue_settings WHERE key=?',
                (ACCEPTED_INVALID_SCAN_PREFIX + digest(scan_key),)).fetchone()
            if migration is None:
                migration_state = 'not_started'
            else:
                try:
                    migration_state = _parse_invalid_accepted_migration_checkpoint(migration[0])['state']
                except (TypeError, ValueError):
                    migration_state = 'unknown'
        deadlines = [value for value in (retry[1], missing[1], invalid[1] if invalid else None) if value is not None]
        accepted.update(local_retry_batches=retry[0], local_missing_retry_batches=missing[0],
                        local_invalid_retry_batches=invalid[0] if invalid else None,
                        local_invalid_migration_state=migration_state,
                        next_local_retry_epoch=min(deadlines) if deadlines else None)
        if marker is None:
            accepted.update(scan_state='not_started', scanned_batches=0, indexed_candidates=0)
        else:
            try:
                checkpoint = _decode_accepted_batch_checkpoint(marker[0])
                scan_state = ('local_retry_wait' if
                              (checkpoint['state'] == 'scanning' and checkpoint['retry_after'] > time.time())
                              or ((retry[0] or missing[0]) and checkpoint['path'] >= checkpoint['end_path'])
                              else checkpoint['state'])
                if scan_state == 'complete':
                    if _accepted_batch_frontier_has_rows(db, checkpoint):
                        scan_state = 'scanning'
                    elif migration_state in ('not_started', 'scanning'):
                        scan_state = 'scanning'
                    elif migration_state in (None, 'unknown'):
                        scan_state = 'unknown'
                    elif invalid is not None and invalid[0]:
                        scan_state = 'local_retry_wait'
                accepted.update(scan_state=scan_state, scanned_batches=checkpoint['checked'],
                                indexed_candidates=checkpoint['indexed'])
            except _UnsupportedAcceptedBatchCheckpointVersion:
                accepted['checkpoint_problem'] = 'unsupported_version'
            except (TypeError, ValueError):
                accepted['checkpoint_problem'] = 'invalid_checkpoint'
                audit_key = ACCEPTED_BATCH_SCAN_PREFIX + 'invalid:' + digest((scan_key, _audit_sqlite_row(marker)))
                archived = db.execute('SELECT value FROM queue_settings WHERE key=?', (audit_key,)).fetchone()
                if archived and (type(archived[0]) is not type(marker[0]) or archived[0] != marker[0]):
                    accepted['checkpoint_problem'] = 'audit_conflict'
    integrity = {'contract_version': 1, 'scan_state': 'unknown', 'checked_scopes': None,
                 'affected_scopes': None, 'local_metadata_only': True,
                 'all_history_complete': False, 'publisher_requests': 0}
    if source is not None:
        marker = db.execute('SELECT value FROM queue_settings WHERE key=?', (integrity_scan_key(source),)).fetchone()
        if marker is None:
            integrity.update(scan_state='not_started', checked_scopes=0, affected_scopes=0)
        else:
            try:
                checkpoint = json.loads(marker[0])
                if _integrity_checkpoint_valid(checkpoint):
                    integrity.update(scan_state=checkpoint['state'], checked_scopes=checkpoint['checked'],
                                     affected_scopes=checkpoint['invalid'])
            except (TypeError, ValueError):
                pass
    original_audit = {'contract_version': 1, 'scan_state': 'unknown', 'checkpoint_problem': None, 'checked_pages': None,
                      'verified_pages': None, 'out_of_scope_pages': None, 'mismatched_pages': None, 'unverified_pages': None,
                      'retry_pending_pages': None, 'next_audit_at_utc': None,
                      'local_raw_only': True, 'publisher_requests': 0, 'all_history_complete': False}
    if source is not None:
        marker = db.execute('SELECT value FROM queue_settings WHERE key=?', (original_envelope_audit_key(source),)).fetchone()
        if marker is None:
            original_audit.update(scan_state='not_started', checked_pages=0, verified_pages=0,
                                  out_of_scope_pages=0, mismatched_pages=0, unverified_pages=0)
        else:
            try:
                checkpoint = _decode_original_audit_checkpoint(marker[0])
                if checkpoint['state'] == 'complete' and _original_audit_frontier_has_rows(db, checkpoint):
                    checkpoint = {**checkpoint, 'state': 'scanning'}
                if _original_audit_checkpoint_valid(checkpoint):
                    original_audit.update(scan_state=checkpoint['state'], checked_pages=checkpoint['checked'],
                                          verified_pages=checkpoint['verified'], mismatched_pages=checkpoint['mismatched'],
                                          unverified_pages=checkpoint['unverified'], out_of_scope_pages=(checkpoint['checked']
                                              - checkpoint['verified'] - checkpoint['mismatched'] - checkpoint['unverified']))
                    deadlines = [checkpoint['next_epoch']] if checkpoint['next_epoch'] > 0 else []
                    if checkpoint.get('retry_index_ready') and db.execute(
                            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cnyes_history_original_file_retries'").fetchone():
                        retry = db.execute('SELECT count(*),min(not_before_epoch) '
                            'FROM cnyes_history_original_file_retries WHERE audit_key=?',
                            (original_envelope_audit_key(source),)).fetchone()
                        original_audit['retry_pending_pages'] = retry[0]
                        if retry[1] is not None:
                            deadlines.append(retry[1])
                    if checkpoint['state'] == 'complete' and deadlines:
                        try:
                            original_audit['next_audit_at_utc'] = datetime.fromtimestamp(
                                min(deadlines), timezone.utc).isoformat()
                        except (ValueError, OverflowError, OSError):
                            pass  # An unrepresentable deadline stays unknown.
            except _UnsupportedOriginalAuditCheckpointVersion:
                original_audit['checkpoint_problem'] = 'unsupported_version'
            except (TypeError, ValueError):
                original_audit['checkpoint_problem'] = 'invalid_checkpoint'
                audit_key, original = _original_audit_archive_identity(original_envelope_audit_key(source), marker[0])
                archived = db.execute('SELECT checkpoint_key,original_value FROM cnyes_history_integrity_checkpoint_audit '
                                      'WHERE checkpoint_sha256=?', (audit_key,)).fetchone()
                if archived and (archived[0] != original_envelope_audit_key(source) or archived[1] != original):
                    original_audit['checkpoint_problem'] = 'audit_conflict'
    return {'contract_version': CONTRACT_VERSION, 'scope_states': states, 'retained_metadata_work': work,
            'current_source_contract_scope_states': current,
            'retired_gap_receipts': db.execute('SELECT count(*) FROM cnyes_history_gap_retirements').fetchone()[0],
            'legacy_metadata_scans': legacy_scans,
            'overflow_retained_recovery': local_recovery,
            'unobserved_nonzero_retained_scan': nonzero,
            'accepted_body_batch_scan': accepted,
            'completed_integrity_scan': integrity,
            'original_envelope_audit': original_audit,
            'list_coverage_is_not_body_coverage': True, 'all_history_complete': False, 'finish_eta_utc': None}


def overflow_scan_key(source):
    return OVERFLOW_RETAINED_SCAN_PREFIX + digest((
        source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))


def nonzero_scan_key(source):
    return LEGACY_NONZERO_SCAN_PREFIX + digest((
        source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))


def accepted_batch_scan_key(source):
    return ACCEPTED_BATCH_SCAN_PREFIX + digest((
        source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))


class _UnsupportedAcceptedBatchCheckpointVersion(ValueError):
    pass


def _decode_accepted_batch_checkpoint(value):
    state = json.loads(value)
    if (type(state) is dict and 'version' in state
            and (type(state['version']) is not int or state['version'] != 1)):
        raise _UnsupportedAcceptedBatchCheckpointVersion
    if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != 1
            or state.get('state') not in ('scanning', 'complete') or type(state.get('path')) is not str
            or type(state.get('end_path')) is not str or state['path'] > state['end_path']
            or any(type(state.get(k)) is not int or state[k] < 0 for k in ('checked', 'indexed', 'attempts'))
            or type(state.get('retry_after')) not in (int, float)
            or not math.isfinite(state['retry_after']) or state['retry_after'] < 0):
        raise ValueError('invalid accepted batch checkpoint')
    return state


def _accepted_batch_frontier_has_rows(db, state):
    """A complete marker cannot hide registered paths after its cursor."""
    return bool(db.execute('SELECT 1 FROM accepted_batches WHERE path>? AND path<=? LIMIT 1',
                           (state['path'], state['end_path'])).fetchone())


def _parse_invalid_accepted_migration_checkpoint(raw):
    state = json.loads(raw)
    if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != 1
            or state.get('state') not in ('scanning', 'complete') or type(state.get('path')) is not str
            or type(state.get('end_path')) is not str):
        raise ValueError('invalid invalid-batch migration checkpoint')
    return state


def accepted_batch_inventory_complete(queue, source):
    """Whether initial inventory can release source rechecks.

    Known missing files are independently retried and reported as incomplete;
    an unknown or transiently unreadable file still holds the source fence.
    """
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                             (accepted_batch_scan_key(source),)).fetchone()
    if not saved:
        return not queue.db.execute('SELECT 1 FROM accepted_batches LIMIT 1').fetchone()
    try:
        state = _decode_accepted_batch_checkpoint(saved[0])
        return (state['state'] == 'complete'
                and not _accepted_batch_frontier_has_rows(queue.db, state)
                and not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries '
                                         'WHERE scan_key=? LIMIT 1', (accepted_batch_scan_key(source),)).fetchone())
    except (TypeError, ValueError):
        return False


def _register_accepted_batch_candidate(queue, url, sha, relative, receipt_relative):
    inserted = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_candidates VALUES (?,?,?,?)',
                                (url, sha, relative, receipt_relative)).rowcount
    changed = inserted
    if not inserted:
        previous = queue.db.execute('SELECT receipt_path FROM cnyes_history_accepted_batch_candidates '
            'WHERE request_url=? AND response_sha256=? AND parquet_path=?', (url, sha, relative)).fetchone()
        if previous and previous[0] != receipt_relative:
            changed = queue.db.execute('UPDATE cnyes_history_accepted_batch_candidates SET receipt_path=? '
                'WHERE request_url=? AND response_sha256=? AND parquet_path=? AND receipt_path=? '
                'AND EXISTS (SELECT 1 FROM accepted_batches WHERE path=? AND receipt_path=?)',
                (receipt_relative, url, sha, relative, previous[0], relative, receipt_relative)).rowcount
            if changed:
                observed_at = now()
                values = (url, sha, relative, previous[0], receipt_relative, observed_at)
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_candidate_receipt_revisions VALUES (?,?,?,?,?,?,?)',
                                 (digest(values), *values))
    if changed and getattr(queue, '_cnyes_retained_work_initialized', False):
        prior = queue.db.execute('SELECT * FROM cnyes_history_retained_work '
            "WHERE request_url=? AND response_sha256=? AND state IN "
            "('retained_unavailable','retained_invalid','retained_request_mismatch')",
            (url, sha)).fetchone()
        if prior:
            # Preserve the old failure as an outcome; a candidate is only
            # permission to validate it, never evidence of a source page.
            _save_retained_resolution(queue, prior, 'pending', None, 'new_accepted_body_batch_candidate')
            queue._cnyes_original_priority_needs_wake = True
        if getattr(queue, '_cnyes_original_repair_initialized', False):
            # The global batch scan may be long. A matching newly indexed
            # candidate is sufficient to retry this local work, but never to
            # declare success or change publisher cooldowns. Preserve the
            # displaced retry verbatim; an invalid candidate returns to the
            # original scan fence after normal receipt/identity validation.
            waiting = queue.db.execute('SELECT * FROM cnyes_history_retained_work '
                "WHERE request_url=? AND response_sha256=? AND state='local_retry'", (url, sha)).fetchone()
            retry = queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries '
                "WHERE request_url=? AND error_code='accepted_batch_scan_pending'", (url,)).fetchone()
            if waiting and retry:
                evidence = {'work': _audit_sqlite_row(waiting), 'local_retry': _audit_sqlite_row(retry)}
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_revision_audit VALUES (?,?,?,?)',
                    (digest(evidence), url, encoded(evidence), now()))
                queue.db.execute('UPDATE cnyes_history_retained_local_retries '
                    "SET not_before_epoch=0,error_code='new_accepted_body_batch_candidate' "
                    "WHERE request_url=? AND error_code='accepted_batch_scan_pending'", (url,))
                queue._cnyes_original_priority_needs_wake = True
        # An original-envelope audit can already have marked a head unproven
        # before this batch was indexed. Wake its local-only repair now rather
        # than waiting for the next whole integrity pass.
        source = queue.registry.sources.get('cnyes')
        try:
            scope = cnyes_scope(url)
            page = int(parse_qs(url.split('?', 1)[1]).get('page', ['1'])[0])
        except (ValueError, TypeError, IndexError, OverflowError):
            return inserted
        if (getattr(queue, '_cnyes_original_repair_initialized', False)
                and source and source.allowed and source.cnyes_apis):
            row = queue.db.execute('SELECT o.* FROM cnyes_history_unproven_pages m '
                'JOIN cnyes_history_page_heads h ON h.scope_url=m.scope_url AND h.page=m.page '
                'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
                'WHERE m.scope_url=? AND m.page=?', (scope, page)).fetchone()
            observation = _checked_page_observation(row, source)
            if observation:
                if observation['request_url'] == url and observation['response_sha256'] == sha:
                    _queue_unverified_original_local_repair(queue, source, observation)
                elif observation['scope_url'] != scope:
                    _queue_unverified_alias_local_repair(queue, source, scope, page)
    return inserted


def register_accepted_body_batch(queue, records, relative, receipt_relative):
    """Index newly accepted raw references in their existing commit."""
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        return
    for row in records:
        url, sha = row.get('source_request_url'), row.get('response_sha256')
        if (row.get('provider') != source.provider or not isinstance(url, str) or 'startAt=' not in url
                or row.get('raw_response_body') is None or not _retained_envelopes(queue.root, sha)):
            continue
        try:
            key = scope_key(url)
            page = int(parse_qs(url.split('?', 1)[1]).get('page', ['1'])[0])
        except (TypeError, ValueError, IndexError, OverflowError):
            continue
        if key and key[0] in source.cnyes_apis and 1 <= page <= 30:
            _register_accepted_batch_candidate(queue, url, sha, relative, receipt_relative)
            if getattr(queue, '_cnyes_history_fully_initialized', False):
                # The normal cursor may have already passed this page; the
                # candidate itself is not evidence and only wakes validation.
                _queue_late_accepted_candidate(queue, source, url, sha)
            else:
                # During orphan replay the full page/work tables may not yet
                # exist. If a frozen scan cannot visit this path, journal a
                # retry atomically with the acceptance instead of losing it on
                # a second crash. An unstarted scan already includes it.
                marker = accepted_batch_scan_key(source)
                saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
                if saved:
                    try:
                        state = _decode_accepted_batch_checkpoint(saved[0])
                        covered = (state['state'] == 'scanning' and state['path'] < relative <= state['end_path'])
                    except (TypeError, ValueError, KeyError):
                        covered = False
                    if not covered:
                        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_local_retries '
                            'VALUES (?,?,?,?,?)', (marker, relative, receipt_relative, 0, 0))


def _accepted_batch_metadata_pairs(path, source):
    """Find candidate request/hash pairs from footers; never read body text."""
    batch = pq.ParquetFile(path)
    required = {'source_request_url', 'response_sha256', 'raw_response_body'}
    if not required <= set(batch.schema_arrow.names):
        return
    bases = tuple(source.cnyes_apis)
    for number in range(batch.metadata.num_row_groups):
        group = batch.metadata.row_group(number)
        columns = {group.column(i).path_in_schema: group.column(i) for i in range(group.num_columns)}
        raw = columns['raw_response_body'].statistics
        if raw and raw.has_null_count and raw.null_count == group.num_rows:
            continue
        request = columns['source_request_url'].statistics
        response = columns['response_sha256'].statistics
        if request and request.has_min_max and isinstance(request.min, str) and isinstance(request.max, str):
            low, high = request.min, request.max
            if not any(low <= base + '\uffff' and high >= base for base in bases):
                continue
            if (low == high and 'startAt=' in low and response and response.has_min_max
                    and isinstance(response.min, str) and response.min == response.max):
                yield low, response.min
                continue
        # Mixed or missing statistics cannot prove absence. Read only the two
        # metadata columns; the validating worker will inspect the raw row.
        for chunk in batch.iter_batches(batch_size=256, row_groups=[number],
                columns=['source_request_url', 'response_sha256'], use_threads=False):
            for row in chunk.to_pylist():
                if isinstance(row.get('source_request_url'), str) and isinstance(row.get('response_sha256'), str):
                    yield row['source_request_url'], row['response_sha256']


class _MissingAcceptedReceipt(FileNotFoundError):
    """The batch exists but its associated receipt is not currently present."""


@contextmanager
def _accepted_batch_index_savepoint(queue):
    """Commit a batch's candidates only after its complete metadata read."""
    queue.db.execute('SAVEPOINT cnyes_accepted_batch_index')
    try:
        yield
    except BaseException:
        queue.db.execute('ROLLBACK TO cnyes_accepted_batch_index')
        queue.db.execute('RELEASE cnyes_accepted_batch_index')
        raise
    queue.db.execute('RELEASE cnyes_accepted_batch_index')


def _current_accepted_batch_retry_row(queue, row):
    """Use the current registration; a vanished registration is not success."""
    current = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (row['path'],)).fetchone()
    if not current:
        raise OSError('accepted batch registration unavailable')
    updated = dict(row)
    updated['receipt_path'] = current[0]
    return updated


def _index_accepted_batch_metadata(queue, source, row, path, *, receipt_checked=False):
    """Yield identity-matched local candidates; validation still happens later."""
    for url, sha in _accepted_batch_metadata_pairs(path, source):
        if not _retained_envelopes(queue.root, sha):
            continue
        try:
            key = scope_key(url)
            page = int(parse_qs(url.split('?', 1)[1]).get('page', ['1'])[0])
        except (TypeError, ValueError, IndexError, OverflowError):
            continue
        if not key or key[0] not in source.cnyes_apis or not 1 <= page <= 30:
            continue
        scope = cnyes_scope(url)
        job = queue.db.execute('SELECT response_sha256,provider,kind FROM discovery WHERE url=?', (url,)).fetchone()
        eligible = bool(job and job['provider'] == source.provider and job['kind'] == 'cnyes_news'
            and job['response_sha256'] == sha and queue.db.execute(
                'SELECT 1 FROM bulk_page_evidence WHERE scope_url=? AND page=?', (scope, page)).fetchone())
        if not eligible:
            # A later source observation can change discovery's hash while
            # an older page head still needs its own independently verifiable
            # original. A matching hash alone, or an alias pointing to another
            # scope, is not the exact historical request.
            observation = _checked_page_observation(queue.db.execute(
                'SELECT o.* FROM cnyes_history_page_heads h '
                'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
                'WHERE h.scope_url=? AND h.page=?', (scope, page)).fetchone(), source)
            eligible = bool(observation and observation['request_url'] == url
                and observation['scope_url'] == scope and observation['page'] == page
                and observation['response_sha256'] == sha and observation['contract_version'] == CONTRACT_VERSION
                and observation['source_bulk_fingerprint'] == cnyes_bulk_fingerprint(source)
                and observation['authorization_fingerprint'] == source.authorization_fingerprint)
        if eligible:
            if not receipt_checked:
                # Most batches have no matching CNYES history candidate. Avoid
                # an extra filesystem lookup until one is actually found.
                from downloader.news_fulltext_queue import replay_source_path
                try:
                    replay_source_path(queue.root, row['receipt_path'])
                except FileNotFoundError as error:
                    raise _MissingAcceptedReceipt() from error
                receipt_checked = True
            inserted = _register_accepted_batch_candidate(queue, url, sha, row['path'], row['receipt_path'])
            yield url, sha, inserted


def _nonzero_inventory_will_visit(queue, source, scope, page):
    """Leave an unvisited scope to its existing all-pages inventory owner."""
    if any(queue.db.execute(sql, (scope,)).fetchone() for sql in (
            'SELECT 1 FROM cnyes_history_scope_heads WHERE scope_url=?',
            'SELECT 1 FROM cnyes_history_retained_work WHERE scope_url=? LIMIT 1',
            'SELECT 1 FROM bulk_window_gaps WHERE scope_url=?',
            'SELECT 1 FROM cnyes_history_edges e JOIN bulk_window_gaps g ON g.scope_url=e.parent_scope '
                'WHERE e.child_scope=? LIMIT 1')):
        return False
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (nonzero_scan_key(source),)).fetchone()
    if not saved:
        return True
    try:
        state = json.loads(saved[0])
        if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != 1
                or state.get('state') not in ('scanning', 'complete') or type(state.get('scope_url')) is not str
                or type(state.get('page')) is not int or not 0 <= state['page'] <= 30
                or any(type(state.get(k)) is not int or state[k] < 0 for k in ('checked', 'queued'))):
            return True  # The owner preserves and restarts an invalid checkpoint.
        return state['state'] == 'scanning' and (scope > state['scope_url']
            or scope == state['scope_url'] and state['page'] < page)
    except (TypeError, ValueError, KeyError):
        return True


def _queue_late_accepted_candidate(queue, source, url, sha):
    """Wake local validation even if a source recheck has already become due."""
    if source.acquisition_mode == 'finite' or source.known_url_only:
        return
    key = scope_key(url)
    page = int(parse_qs(url.split('?', 1)[1]).get('page', ['1'])[0])
    scope = cnyes_scope(url)
    if (not key or key[0] not in source.cnyes_apis or not 1 <= page <= 30
            or queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=?',
                                (scope, page)).fetchone()
            or queue.db.execute('SELECT 1 FROM bulk_window_splits WHERE scope_url=?', (scope,)).fetchone()):
        return
    job = queue.db.execute('SELECT provider,kind,response_sha256,last_checked_at_utc FROM discovery WHERE url=?',
                           (url,)).fetchone()
    evidence = queue.db.execute('SELECT p.total,p.ids_sha256,p.observed_at_utc,c.total_articles,c.total_pages '
        'FROM bulk_page_evidence p JOIN bulk_api_coverage c ON c.base_url=p.scope_url '
        'WHERE p.scope_url=? AND p.page=?', (scope, page)).fetchone()
    if (not job or job['provider'] != source.provider or job['kind'] != 'cnyes_news'
            or job['response_sha256'] != sha or not job['last_checked_at_utc'] or not evidence
            or evidence['total'] != evidence['total_articles'] or not 1 <= evidence['total_pages'] <= 30
            or page > evidence['total_pages']):
        return
    prior = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    if prior and prior['response_sha256'] == sha and prior['state'] in (
            'retained_unavailable', 'retained_invalid', 'retained_request_mismatch'):
        _save_retained_resolution(queue, prior, 'pending', None, 'new_accepted_body_batch_candidate')
        queue._cnyes_original_priority_needs_wake = True
    elif not prior and not _nonzero_inventory_will_visit(queue, source, scope, page):
        inserted = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            'VALUES (?,?,?,?,?,?,?)', (url, scope, page, sha, evidence['total'], evidence['ids_sha256'],
                                       evidence['observed_at_utc'])).rowcount
        if inserted:
            queue._cnyes_original_priority_needs_wake = True


def _defer_missing_accepted_batch(queue, marker, row):
    prior = queue.db.execute('SELECT local_attempts FROM cnyes_history_accepted_missing_retries '
                             'WHERE scan_key=? AND parquet_path=?', (marker, row['path'])).fetchone()
    attempts = (prior[0] if prior else 0) + 1
    deadline = time.time() + min(300, 2 ** min(attempts - 1, 9))
    queue.db.execute('INSERT OR REPLACE INTO cnyes_history_accepted_missing_retries VALUES (?,?,?,?,?)',
                     (marker, row['path'], row['receipt_path'], deadline, attempts))
    return deadline


def _defer_invalid_accepted_batch(queue, marker, row):
    """Remember an invalid local file without blocking unrelated source work."""
    deadline = time.time() + ACCEPTED_INVALID_CONFIRM_SECONDS
    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_invalid_retries '
        '(scan_key,parquet_path,receipt_path,not_before_epoch,witness_json,confirmed,local_attempts) '
        'VALUES (?,?,?,?,NULL,0,0)', (marker, row['path'], row['receipt_path'], deadline))
    # A changed registered receipt is a new local candidate, even if the path
    # has appeared in an older invalid-outcome record.
    queue.db.execute('UPDATE cnyes_history_accepted_invalid_retries '
        'SET receipt_path=?,not_before_epoch=?,witness_json=NULL,confirmed=0,local_attempts=0 '
        'WHERE scan_key=? AND parquet_path=? AND receipt_path!=?',
        (row['receipt_path'], deadline, marker, row['path'], row['receipt_path']))


@lru_cache(maxsize=1)
def _accepted_invalid_reader_code_fingerprint():
    """Fingerprint loaded reader code, not potentially newer files on disk."""
    from downloader.news_fulltext_queue import replay_source_path
    identity = hashlib.sha256()
    for implementation in (_accepted_batch_metadata_pairs, _index_accepted_batch_metadata,
                           _queue_late_accepted_candidate, _register_accepted_batch_candidate,
                           _retained_envelopes, replay_source_path):
        code = marshal.dumps(implementation.__code__)
        identity.update(len(code).to_bytes(8, 'big'))
        identity.update(code)
    return identity.hexdigest()


def _accepted_invalid_witness(queue, row):
    """Cheap change signal; only the normal validator can establish evidence."""
    from downloader.news_fulltext_queue import replay_source_path

    def witness(relative):
        try:
            return ['file', *_original_envelope_file_signature(replay_source_path(queue.root, relative))]
        except FileNotFoundError:
            return ['missing']
        except (ValueError, TypeError, RuntimeError):
            return ['invalid_path']
        except OSError as error:
            return ['io_error', type(error).__name__, error.errno]
    return encoded({'version': 2, 'reader_code_sha256': _accepted_invalid_reader_code_fingerprint(),
                    'pyarrow_version': pa.__version__,
                    'files': [witness(row['path']), witness(row['receipt_path'])]})


def _defer_invalid_batch_transient(queue, marker, row, error):
    attempts = row['local_attempts'] + 1
    deadline = time.time() + min(300, 2 ** min(attempts - 1, 9))
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_invalid_retries '
            'SET not_before_epoch=?,witness_json=NULL,confirmed=0,local_attempts=? '
            'WHERE scan_key=? AND parquet_path=?', (deadline, attempts, marker, row['path']))
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
            (row['path'], row['receipt_path'], 'local_retry', type(error).__name__, now()))


def _seed_prior_invalid_accepted_batches(queue, source, *, stop=None, limit=128, max_seconds=.01):
    """Boundedly revisit legacy invalid paths against their current registration."""
    result = {'complete': False, 'checked': 0}
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    marker = accepted_batch_scan_key(source)
    key = ACCEPTED_INVALID_SCAN_PREFIX + digest(marker)
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()
    try:
        last = (queue.db.execute("SELECT parquet_path FROM cnyes_history_accepted_batch_outcomes "
                                 "WHERE state='invalid' ORDER BY parquet_path DESC LIMIT 1").fetchone()
                if not saved else None)
        state = (_parse_invalid_accepted_migration_checkpoint(saved[0]) if saved
                 else {'version': 1, 'state': 'scanning', 'path': '', 'end_path': last[0] if last else ''})
    except (ValueError, TypeError, KeyError):
        original = saved[0] if type(saved[0]) is str else encoded(_audit_sqlite_row({'value': saved[0]}))
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_integrity_checkpoint_audit VALUES (?,?,?,?)',
                             (digest((key, original)), key, original, now()))
        last = queue.db.execute("SELECT parquet_path FROM cnyes_history_accepted_batch_outcomes "
                                "WHERE state='invalid' ORDER BY parquet_path DESC LIMIT 1").fetchone()
        state = {'version': 1, 'state': 'scanning', 'path': '', 'end_path': last[0] if last else ''}
    if state['state'] == 'complete':
        result['complete'] = True
        return result
    paths = queue.db.execute("SELECT DISTINCT parquet_path FROM cnyes_history_accepted_batch_outcomes "
        "WHERE state='invalid' AND parquet_path>? AND parquet_path<=? ORDER BY parquet_path LIMIT ?",
        (state['path'], state['end_path'], limit)).fetchall()
    started = time.monotonic()
    finished = True
    with queue.db:
        for (path,) in paths:
            if _history_scan_slice_done(result['checked'], started, max_seconds, stop):
                finished = False
                break
            current = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone()
            if current:
                _defer_invalid_accepted_batch(queue, marker, {'path': path, 'receipt_path': current[0]})
            state['path'] = path
            result['checked'] += 1
        if finished and len(paths) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (key, encoded(state)))
    return result


def seed_accepted_body_batch_candidates(queue, *, stop=None, limit=128, max_seconds=.01):
    """Index retained accepted batches once; candidates are not source proof."""
    from downloader.news_fulltext_queue import replay_source_path
    result = {'complete': False, 'frontier_complete': False, 'checked': 0, 'retried': 0, 'indexed': 0,
              'blocked': None, 'next_retry_epoch': None, 'restarted_invalid_checkpoint': False}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result.update(complete=True, blocked='source_disabled')
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    marker = accepted_batch_scan_key(source)
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    try:
        last = queue.db.execute('SELECT path FROM accepted_batches ORDER BY path DESC LIMIT 1').fetchone() if not saved else None
        state = _decode_accepted_batch_checkpoint(saved[0]) if saved else {'version': 1, 'state': 'scanning', 'path': '',
            'end_path': last[0] if last else '', 'checked': 0, 'indexed': 0, 'retry_after': 0, 'attempts': 0}
    except _UnsupportedAcceptedBatchCheckpointVersion:
        result['blocked'] = 'unsupported_accepted_batch_checkpoint_version'
        return result
    except (TypeError, ValueError, KeyError):
        if stop is not None and stop.is_set():
            return result
        try:
            _archive_invalid_checkpoint(queue, marker, ACCEPTED_BATCH_SCAN_PREFIX, saved)
        except _CheckpointAuditConflict:
            result['blocked'] = 'accepted_batch_checkpoint_audit_conflict'
            return result
        last = queue.db.execute('SELECT path FROM accepted_batches ORDER BY path DESC LIMIT 1').fetchone()
        state = {'version': 1, 'state': 'scanning', 'path': '', 'end_path': last[0] if last else '',
                 'checked': 0, 'indexed': 0, 'retry_after': 0, 'attempts': 0}
        result['restarted_invalid_checkpoint'] = True
    if stop is not None and stop.is_set():
        return result
    if state['state'] == 'complete':
        if not _accepted_batch_frontier_has_rows(queue.db, state) and not queue.db.execute(
                                'SELECT 1 FROM cnyes_history_accepted_batch_local_retries '
                                'WHERE scan_key=? LIMIT 1', (marker,)).fetchone():
            result.update(complete=True, frontier_complete=True)
            return result
        state['state'] = 'scanning'
    if state['retry_after'] > time.time():
        result.update(blocked='local_retry_wait', next_retry_epoch=state['retry_after'])
        return result
    # Retry at most one failed file per slice; a failing file must not hold
    # the cursor in front of every other independent retained batch.
    retry = queue.db.execute('SELECT parquet_path AS path,receipt_path,local_attempts '
        'FROM cnyes_history_accepted_batch_local_retries WHERE scan_key=? AND not_before_epoch<=? '
        'ORDER BY not_before_epoch,parquet_path LIMIT 1', (marker, time.time())).fetchone()
    rows = queue.db.execute('SELECT path,receipt_path FROM accepted_batches WHERE path>? AND path<=? '
                            'ORDER BY path LIMIT ?', (state['path'], state['end_path'], limit - bool(retry))).fetchall()
    work = ([(retry, True)] if retry else []) + [(row, False) for row in rows]
    started = time.monotonic()
    with queue.db:
        # SAVEPOINT must be nested inside the slice transaction so releasing a
        # successful file cannot commit it separately from the scan cursor.
        if not queue.db.in_transaction:
            queue.db.execute('BEGIN')
        for index, (row, is_retry) in enumerate(work):
            if stop is not None and stop.is_set() or index and time.monotonic() - started >= max_seconds:
                break
            try:
                indexed = 0
                with _accepted_batch_index_savepoint(queue):
                    if is_retry:
                        row = _current_accepted_batch_retry_row(queue, row)
                    path = replay_source_path(queue.root, row['path'])
                    for url, sha, inserted in _index_accepted_batch_metadata(queue, source, row, path):
                        indexed += inserted
                        # The nonzero-page inventory may already have passed
                        # this page. Make the new candidate visible to its
                        # local validator in the same per-batch transaction.
                        _queue_late_accepted_candidate(queue, source, url, sha)
                result['indexed'] += indexed
                if is_retry or state['attempts']:
                    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                        (row['path'], row['receipt_path'], 'recovered', 'local_batch_read_recovered', now()))
                state.update(attempts=0, retry_after=0)
                if is_retry:
                    queue.db.execute('DELETE FROM cnyes_history_accepted_batch_local_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
            except FileNotFoundError as error:
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                    (row['path'], row['receipt_path'], 'unavailable',
                     'local_batch_receipt_missing' if isinstance(error, _MissingAcceptedReceipt) else 'local_batch_file_missing', now()))
                _defer_missing_accepted_batch(queue, marker, row)
                if is_retry:
                    queue.db.execute('DELETE FROM cnyes_history_accepted_batch_local_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
                state.update(attempts=0, retry_after=0)
            except OSError as error:
                attempts = (row['local_attempts'] if is_retry else state['attempts']) + 1
                deadline = time.time() + min(300, 2 ** min(attempts - 1, 9))
                queue.db.execute('INSERT OR REPLACE INTO cnyes_history_accepted_batch_local_retries VALUES (?,?,?,?,?)',
                    (marker, row['path'], row['receipt_path'], deadline, attempts))
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                    (row['path'], row['receipt_path'], 'local_retry', type(error).__name__, now()))
                state.update(attempts=0, retry_after=0)
            except MemoryError as error:
                attempts = state['attempts'] + 1
                state.update(attempts=attempts, retry_after=time.time() + min(300, 2 ** min(attempts - 1, 9)))
                result.update(blocked='local_retry_wait', next_retry_epoch=state['retry_after'])
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                    (row['path'], row['receipt_path'], 'local_retry', type(error).__name__, now()))
                break  # Resource pressure is not an independent per-file failure.
            except (pa.ArrowException, ValueError, TypeError, KeyError) as error:
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                    (row['path'], row['receipt_path'], 'invalid', type(error).__name__, now()))
                _defer_invalid_accepted_batch(queue, marker, row)
                if is_retry:
                    queue.db.execute('DELETE FROM cnyes_history_accepted_batch_local_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
                state.update(attempts=0, retry_after=0)
            if not is_retry:
                state['path'] = row['path']
                result['checked'] += 1
            else:
                result['retried'] += 1
        remaining = queue.db.execute('SELECT 1 FROM accepted_batches WHERE path>? AND path<=? LIMIT 1',
                                     (state['path'], state['end_path'])).fetchone()
        result['frontier_complete'] = not remaining
        retry_after = queue.db.execute('SELECT min(not_before_epoch) FROM cnyes_history_accepted_batch_local_retries '
                                       'WHERE scan_key=?', (marker,)).fetchone()[0]
        if not remaining and retry_after is None and result['blocked'] is None:
            state['state'] = 'complete'
            result['complete'] = True
        elif not remaining and retry_after is not None and result['blocked'] is None:
            result.update(blocked='local_retry_wait', next_retry_epoch=retry_after)
        state['checked'] += result['checked']
        state['indexed'] += result['indexed']
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, encoded(state)))
    return result


def retry_missing_accepted_batches(queue, *, stop=None, limit=8, max_seconds=.01):
    """Retry absent files locally without blocking other source or history work."""
    from downloader.news_fulltext_queue import replay_source_path
    result = {'processed': 0, 'indexed': 0, 'next_retry_epoch': None}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        return result
    marker = accepted_batch_scan_key(source)
    if limit > 0 and max_seconds > 0 and not (stop is not None and stop.is_set()):
        rows = queue.db.execute('SELECT parquet_path AS path,receipt_path FROM cnyes_history_accepted_missing_retries '
            'WHERE scan_key=? AND not_before_epoch<=? ORDER BY not_before_epoch,parquet_path LIMIT ?',
            (marker, time.time(), limit)).fetchall()
        started = time.monotonic()
        with queue.db:
            if not queue.db.in_transaction:
                queue.db.execute('BEGIN')
            for index, row in enumerate(rows):
                if stop is not None and stop.is_set() or index and time.monotonic() - started >= max_seconds:
                    break
                try:
                    indexed = 0
                    with _accepted_batch_index_savepoint(queue):
                        row = _current_accepted_batch_retry_row(queue, row)
                        path = replay_source_path(queue.root, row['path'])
                        # Both files must be present before handing candidates to
                        # the existing receipt/content validator. A partially
                        # restored pair remains a local retry, not proof.
                        replay_source_path(queue.root, row['receipt_path'])
                        for url, sha, inserted in _index_accepted_batch_metadata(queue, source, row, path, receipt_checked=True):
                            indexed += inserted
                            _queue_late_accepted_candidate(queue, source, url, sha)
                    result['indexed'] += indexed
                    queue.db.execute('DELETE FROM cnyes_history_accepted_missing_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
                    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                        (row['path'], row['receipt_path'], 'recovered', 'local_batch_read_recovered', now()))
                except (OSError, MemoryError) as error:
                    _defer_missing_accepted_batch(queue, marker, row)
                    if not isinstance(error, FileNotFoundError):
                        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                            (row['path'], row['receipt_path'], 'local_retry', type(error).__name__, now()))
                    result['processed'] += 1
                    if isinstance(error, MemoryError):
                        break
                    continue
                except (pa.ArrowException, ValueError, TypeError, KeyError) as error:
                    queue.db.execute('DELETE FROM cnyes_history_accepted_missing_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
                    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                        (row['path'], row['receipt_path'], 'invalid', type(error).__name__, now()))
                    _defer_invalid_accepted_batch(queue, marker, row)
                result['processed'] += 1
    result['next_retry_epoch'] = queue.db.execute('SELECT min(not_before_epoch) '
        'FROM cnyes_history_accepted_missing_retries WHERE scan_key=?', (marker,)).fetchone()[0]
    return result


def retry_invalid_accepted_batches(queue, *, stop=None, limit=8, max_seconds=.01):
    """Revisit changed invalid batches using bounded local-only work."""
    from downloader.news_fulltext_queue import replay_source_path

    result = {'processed': 0, 'indexed': 0, 'unchanged': 0, 'next_retry_epoch': None,
              'migration': {'complete': False, 'checked': 0}}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result['migration']['complete'] = True
        return result
    marker = accepted_batch_scan_key(source)
    if limit > 0 and max_seconds > 0 and not (stop is not None and stop.is_set()):
        result['migration'] = _seed_prior_invalid_accepted_batches(
            queue, source, stop=stop, limit=limit, max_seconds=max_seconds / 2)
        rows = queue.db.execute('SELECT parquet_path AS path,receipt_path,witness_json,confirmed,local_attempts '
            'FROM cnyes_history_accepted_invalid_retries WHERE scan_key=? AND not_before_epoch<=? '
            'ORDER BY not_before_epoch,parquet_path LIMIT ?', (marker, time.time(), limit)).fetchall()
        started = time.monotonic()
        for row in rows:
            if _history_scan_slice_done(result['processed'], started, max_seconds / 2, stop):
                break
            current = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (row['path'],)).fetchone()
            if not current:
                with queue.db:
                    queue.db.execute('DELETE FROM cnyes_history_accepted_invalid_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
                result['processed'] += 1
                continue
            if current[0] != row['receipt_path']:
                with queue.db:
                    _defer_invalid_accepted_batch(queue, marker, {'path': row['path'], 'receipt_path': current[0]})
                result['processed'] += 1
                continue
            before = None
            try:
                before = _accepted_invalid_witness(queue, row)
                if row['confirmed'] and row['witness_json'] == before:
                    with queue.db:
                        queue.db.execute('UPDATE cnyes_history_accepted_invalid_retries SET not_before_epoch=? '
                            'WHERE scan_key=? AND parquet_path=?',
                            (time.time() + ACCEPTED_INVALID_RECHECK_SECONDS, marker, row['path']))
                    result['processed'] += 1
                    result['unchanged'] += 1
                    continue
                indexed = 0
                with queue.db:
                    path = replay_source_path(queue.root, row['path'])
                    for url, sha, inserted in _index_accepted_batch_metadata(queue, source, row, path):
                        indexed += inserted
                        _queue_late_accepted_candidate(queue, source, url, sha)
                    if before != _accepted_invalid_witness(queue, row):
                        raise OSError('accepted batch changed during local inventory')
                    queue.db.execute('DELETE FROM cnyes_history_accepted_invalid_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
                    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                        (row['path'], row['receipt_path'], 'recovered', 'invalid_local_batch_recovered', now()))
                result['indexed'] += indexed
            except FileNotFoundError:
                with queue.db:
                    _defer_missing_accepted_batch(queue, marker, row)
                    queue.db.execute('DELETE FROM cnyes_history_accepted_invalid_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
                    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                        (row['path'], row['receipt_path'], 'unavailable', 'invalid_batch_now_missing', now()))
            except (OSError, MemoryError) as error:
                _defer_invalid_batch_transient(queue, marker, row, error)
                if isinstance(error, MemoryError):
                    result['processed'] += 1
                    break
            except (pa.ArrowException, ValueError, TypeError, KeyError) as error:
                try:
                    after = _accepted_invalid_witness(queue, row)
                except MemoryError as memory_error:
                    _defer_invalid_batch_transient(queue, marker, row, memory_error)
                    result['processed'] += 1
                    break
                stable = before is not None and before == after
                delay = ACCEPTED_INVALID_RECHECK_SECONDS if stable else ACCEPTED_INVALID_CONFIRM_SECONDS
                with queue.db:
                    queue.db.execute('UPDATE cnyes_history_accepted_invalid_retries '
                        'SET not_before_epoch=?,witness_json=?,confirmed=?,local_attempts=0 '
                        'WHERE scan_key=? AND parquet_path=?',
                        (time.time() + delay, after if stable else None, int(stable), marker, row['path']))
                    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                        (row['path'], row['receipt_path'], 'invalid', type(error).__name__, now()))
            result['processed'] += 1
    result['next_retry_epoch'] = queue.db.execute('SELECT min(not_before_epoch) '
        'FROM cnyes_history_accepted_invalid_retries WHERE scan_key=?', (marker,)).fetchone()[0]
    return result


def seed_overflow_retained(queue, *, stop=None, limit=128, max_seconds=.05):
    """Find legacy raw pages in single-day overflows, once per source contract.

    This is a bounded local migration, not permission to fetch a page again.
    Index all eligible scopes before processing the new work so existing
    equal-day page-ID references can be shared even across restart/slices.
    Missing files, unequal requests and unobserved pages stay unresolved.
    """
    result = {'complete': False, 'checked': 0, 'queued': 0, 'blocked': None,
              'restarted_invalid_checkpoint': False}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result.update(complete=True, blocked='source_disabled')
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    marker = overflow_scan_key(source)
    raw = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    initial = {'version': OVERFLOW_RETAINED_SCAN_VERSION, 'state': 'scanning', 'scope_url': '', 'page': 30}
    try:
        state = json.loads(raw[0]) if raw else dict(initial)
        # An explicitly unknown version may belong to newer code. Preserve it
        # without interpreting its cursor or automatically starting a v1 scan.
        if (type(state) is dict and 'version' in state
                and (type(state['version']) is not int or state['version'] != OVERFLOW_RETAINED_SCAN_VERSION)):
            result['blocked'] = 'unsupported_overflow_scan_checkpoint_version'
            return result
        if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != OVERFLOW_RETAINED_SCAN_VERSION
                or state.get('state') not in ('scanning', 'complete')
                or type(state.get('scope_url')) is not str or type(state.get('page')) is not int
                or not 0 <= state['page'] <= 30):
            raise ValueError('invalid checkpoint')
    except (TypeError, ValueError, KeyError):
        # This migration is idempotent, but sharing page IDs requires the
        # entire overflow scope index. Archive the exact damaged SQLite value
        # before restarting that index; never release its existing work early.
        # Both operations commit together so a failed archive cannot erase the
        # only copy of the cursor. Retrying after a crash starts at the front.
        if stop is not None and stop.is_set():
            return result
        try:
            _archive_invalid_checkpoint(queue, marker, OVERFLOW_RETAINED_SCAN_PREFIX, raw)
        except _CheckpointAuditConflict:
            # Keep unrelated providers and local lanes running; this cursor
            # remains untouched and can be retried after evidence repair.
            result['blocked'] = 'overflow_checkpoint_audit_conflict'
            return result
        state = dict(initial)
        result['restarted_invalid_checkpoint'] = True
    if state['state'] == 'complete':
        result['complete'] = True
        return result

    started = time.monotonic()
    # The scope primary key is also the seek and ordering key.  CROSS JOIN
    # keeps SQLite from scanning the much larger coverage table first.
    comparator = '>' if state['page'] == 30 else '>='
    scopes = queue.db.execute('SELECT g.scope_url,c.total_articles FROM bulk_window_gaps g '
        'CROSS JOIN bulk_api_coverage c ON c.base_url=g.scope_url '
        'WHERE g.scope_url' + comparator + '? AND g.provider=? AND g.error_code=? '
        'AND c.total_pages>30 ORDER BY g.scope_url LIMIT ?',
        (state['scope_url'], source.provider, 'cnyes_single_day_page_cap', limit)).fetchall()
    exhausted_scopes = True
    with queue.db:
        for row in scopes:
            if result['checked'] >= limit or _history_scan_slice_done(result['checked'], started, max_seconds, stop):
                exhausted_scopes = False
                break
            scope, total = row['scope_url'], row['total_articles']
            try:
                key = scope_key(scope)
            except (TypeError, ValueError, OverflowError):
                key = None
            if (not key or key[0] not in source.cnyes_apis or key[1] != key[2]
                    or queue.db.execute('SELECT 1 FROM bulk_window_splits WHERE scope_url=?', (scope,)).fetchone()):
                state.update(scope_url=scope, page=30)
                result['checked'] += 1
                continue
            index_scope(queue, scope, source.provider)
            after = state['page'] if state['scope_url'] == scope else 0
            pages = queue.db.execute('SELECT p.page,p.total,p.ids_sha256,p.observed_at_utc '
                'FROM bulk_page_evidence p LEFT JOIN cnyes_history_page_heads h '
                'ON h.scope_url=p.scope_url AND h.page=p.page '
                "WHERE p.scope_url=? AND typeof(p.page)='integer' AND p.page>=1 "
                'AND p.page>? AND p.page<=30 AND h.scope_url IS NULL ORDER BY p.page',
                (scope, after)).fetchall()
            query = parse_qs(scope.split('?', 1)[1])
            for page in pages:
                if result['checked'] >= limit or _history_scan_slice_done(result['checked'], started, max_seconds, stop):
                    exhausted_scopes = False
                    break
                url = cnyes_url(key[0], page['page'], start=int(query['startAt'][0]), end=int(query['endAt'][0]))
                job = queue.db.execute('SELECT kind,provider,response_sha256,last_checked_at_utc '
                                       'FROM discovery WHERE url=?', (url,)).fetchone()
                sha = job['response_sha256'] if job else None
                if (job and job['kind'] == 'cnyes_news' and job['provider'] == source.provider
                        and job['last_checked_at_utc'] and page['total'] == total
                        and type(sha) is str and len(sha) == 64
                        and all(ch in '0123456789abcdef' for ch in sha)):
                    path = queue.root / 'quarantine' / (sha + '.parquet')
                    try:
                        files_present = path.is_file() and path.with_suffix('.parquet.receipt.json').is_file()
                        requests = []
                        if files_present:
                            requests = pq.read_table(path, columns=['request_url']).column('request_url').to_pylist()
                    except (OSError, MemoryError):
                        # Persist a candidate before advancing the scan. It is
                        # not an identity proof; the local worker validates the
                        # actual envelope after storage becomes available.
                        requests = [url]
                    except (ValueError, KeyError, TypeError, OverflowError):
                        requests = [url]  # Worker preserves invalid-evidence outcome.
                    if requests == [url]:
                        inserted = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_work '
                            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
                            'source_checked_at_utc,state) VALUES (?,?,?,?,?,?,?,?)',
                            (url, scope, page['page'], sha, page['total'], page['ids_sha256'],
                             page['observed_at_utc'], 'overflow_pending'))
                        result['queued'] += inserted.rowcount
                state.update(scope_url=scope, page=page['page'])
                result['checked'] += 1
            else:
                state.update(scope_url=scope, page=30)
                if not pages:
                    result['checked'] += 1
                continue
            break
        if exhausted_scopes and len(scopes) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, encoded(state)))
    return result


def prepare_restricted_history_dispatch(queue, task, source, *, compatible_fingerprint):
    """Recover a missing historical page witness on its existing due request.

    An older mixed public/restricted envelope cannot be retained, so a 304
    cannot reconstruct its IDs. Do not change the job, its validator, retry
    deadline, source circuit or protected content: bind only this dispatch to
    an unconditional response when no equivalent witness is already available.
    The per-item error survives transport failures and identifies old cases
    whose page-level error was later replaced by a 429 or a 304.
    """
    if (task.get('kind') != 'cnyes_news' or task.get('provider') != source.provider
            or not source.allowed or not source.request_enabled
            or source.acquisition_mode == 'finite' or source.known_url_only
            or not (task.get('etag') or task.get('last_modified'))
            or not task.get('response_sha256')
            or task.get('parser_fingerprint') != compatible_fingerprint):
        return
    try:
        url = task['url']
        if not scope_key(url) or wordpress_base(url) not in source.cnyes_apis:
            return
        page = wordpress_page(url)
        if not 1 <= page <= 30:
            return
    except (KeyError, ValueError, TypeError, AttributeError, OverflowError):
        return
    if task.get('error_code') != 'bulk_response_contains_restricted_text':
        marks = ','.join('?' for _ in BULK_RESTRICTED_ITEM_ERRORS)
        restricted = queue.db.execute(
            'SELECT 1 FROM bulk_item_errors WHERE request_url=? AND provider=? '
            f'AND error_code IN ({marks}) LIMIT 1',
            (url, source.provider, *sorted(BULK_RESTRICTED_ITEM_ERRORS))).fetchone()
        if not restricted:
            return
    scope = cnyes_scope(url)
    prior = queue.db.execute('SELECT total,ids_sha256 FROM bulk_page_evidence '
                             'WHERE scope_url=? AND page=?', (scope, page)).fetchone()
    if prior:
        work = {'request_url': url, 'scope_url': scope, 'page': page,
                'expected_total': prior['total'], 'expected_ids_sha256': prior['ids_sha256']}
        sha, witness_kind = _matching_forward_evidence(queue, work, source, allow_shared=True)
        if sha:
            row = queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                                   'WHERE observation_sha256=?', (sha,)).fetchone()
            try:
                observation = json.loads(row['observation_json'])
                info = observation['pagination']
                current_direct = (witness_kind != 'direct'
                                  or observation['response_sha256'] == task['response_sha256'])
                if current_direct and (info['total'], info['ids_sha256']) == (prior['total'], prior['ids_sha256']):
                    return
            except (KeyError, ValueError, TypeError, IndexError):
                pass
    task['_cnyes_missing_restricted_history_evidence'] = True


def _retained_scope_identity_matches(work, source):
    """A page or scope proof cannot cover a different request or page."""
    try:
        return (type(work['page']) is int and 1 <= work['page'] <= 30
                and wordpress_base(work['request_url']) in source.cnyes_apis
                and cnyes_scope(work['request_url']) == work['scope_url']
                and wordpress_page(work['request_url']) == work['page'])
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        return False


def _matching_forward_evidence(queue, work, source, *, allow_shared):
    """Return a current direct or independently verified page reference."""
    if not source or not source.allowed or not _retained_scope_identity_matches(work, source):
        return None, None
    if queue.db.execute('SELECT 1 FROM cnyes_history_unproven_pages WHERE scope_url=? AND page=?',
                        (work['scope_url'], work['page'])).fetchone():
        return None, None
    direct = queue.db.execute('SELECT o.observation_sha256,o.observation_json,o.scope_url,o.page '
        'FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
        'ON o.observation_sha256=h.observation_sha256 '
        'WHERE h.scope_url=? AND h.page=? AND o.page=? AND o.evidence_contract_version=? '
        'AND o.source_bulk_fingerprint=? AND o.authorization_fingerprint=?',
        (work['scope_url'], work['page'], work['page'], CONTRACT_VERSION,
         cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
    if not direct:
        return None, None
    try:
        observation = json.loads(direct['observation_json'])
        info = observation['pagination']
        if (not isinstance(observation, dict) or digest(observation) != direct['observation_sha256']
                or observation['contract_version'] != CONTRACT_VERSION or observation['provider'] != source.provider
                or observation['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
                or observation['authorization_fingerprint'] != source.authorization_fingerprint
                or observation['scope_url'] != direct['scope_url'] or observation['page'] != work['page']
                or info['scope'] != direct['scope_url'] or info['page'] != work['page']
                or wordpress_base(observation['request_url']) not in source.cnyes_apis):
            return None, None
        if direct['scope_url'] == work['scope_url'] and observation['request_url'] == work['request_url']:
            return direct['observation_sha256'], 'direct'
        if not allow_shared:
            return None, None
        if queue.db.execute('SELECT 1 FROM cnyes_history_unproven_pages WHERE scope_url=? AND page=?',
                            (direct['scope_url'], work['page'])).fetchone():
            return None, None
        key = scope_key(work['scope_url'])
        ref = queue.db.execute('SELECT * FROM cnyes_history_page_ID_references '
            'WHERE scope_url=? AND page=? AND observation_sha256=?',
            (work['scope_url'], work['page'], direct['observation_sha256'])).fetchone()
        original = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                    (work['scope_url'], work['page'])).fetchone()
        if (not key or key != scope_key(direct['scope_url']) or not ref or not original
                or ref['reference_scope'] != direct['scope_url']
                or json.loads(ref['original_ID_evidence_json']) != dict(original)
                or (original['total'], original['ids_sha256']) != (info['total'], info['ids_sha256'])
                or (work['expected_total'], work['expected_ids_sha256']) != (info['total'], info['ids_sha256'])
                or observation['source_day_offset_seconds'] != CNYES_QUERY_DAY_OFFSET_SECONDS):
            return None, None
        for item in observation['identities']:
            stamp = item['publishAt']
            if type(stamp) is not int or not key[1] <= (stamp + CNYES_QUERY_DAY_OFFSET_SECONDS) // 86400 <= key[2]:
                return None, None
        return direct['observation_sha256'], 'shared_page_id_reference'
    except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
        return None, None


def _verified_partition_scope_proof(queue, scope, source, *, stop=None, _memo=None):
    """Verify current partition commitments before skipping original work.

    This checks the persisted metadata graph, not original files or article
    bodies. A caller-supplied memo is valid only inside the same SQLite
    transaction and must be discarded immediately after any queue mutation.
    """
    if not source or not source.allowed:
        return None
    memo, active = ({} if _memo is None else _memo), set()
    fingerprint = cnyes_bulk_fingerprint(source)

    def verify(current, expected_sha=None):
        if stop is not None and stop.is_set() or current in active:
            return None
        if current in memo:
            value = memo[current]
            return value if value and (expected_sha is None or value['sha'] == expected_sha) else None
        active.add(current)
        value, cacheable = None, True
        try:
            row = queue.db.execute('SELECT h.*,p.scope_url AS proof_scope,p.proof_json,'
                'c.total_articles,c.total_pages FROM cnyes_history_scope_heads h '
                'JOIN cnyes_history_scope_proofs p ON p.proof_sha256=h.proof_sha256 '
                'JOIN bulk_api_coverage c ON c.base_url=h.scope_url WHERE h.scope_url=?', (current,)).fetchone()
            key = scope_key(current)
            if (not row or not key or key[0] not in source.cnyes_apis or row['provider'] != source.provider
                    or row['state'] not in COMPLETE_STATES or row['proof_scope'] != current
                    or row['evidence_contract_version'] != CONTRACT_VERSION
                    or row['source_bulk_fingerprint'] != fingerprint
                    or row['authorization_fingerprint'] != source.authorization_fingerprint
                    or queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (current,)).fetchone()):
                return None
            if expected_sha is not None and row['proof_sha256'] != expected_sha:
                # The child may be valid independently of a stale parent
                # reference. Do not memoize that mismatch as a bad child.
                cacheable = False
                return None
            proof = json.loads(row['proof_json'])
            members = json.loads(row['member_ids_json'])
            if (type(proof) is not dict or digest(proof) != row['proof_sha256']
                    or proof['scope_url'] != current or proof['state'] != row['state']
                    or proof['provider'] != source.provider or type(proof['contract_version']) is not int
                    or proof['contract_version'] != CONTRACT_VERSION or proof['source_bulk_fingerprint'] != fingerprint
                    or proof['authorization_fingerprint'] != source.authorization_fingerprint
                    or proof['body_completeness_verified'] is not False or proof['historical_point_in_time'] is not False
                    or type(members) is not list or any(type(i) is not int or i <= 0 for i in members)
                    or members != sorted(set(members)) or proof['unique_article_IDs'] != len(members)
                    or proof['member_ids_sha256'] != digest(members)):
                return None
            facts = proof['facts']
            if (type(facts) is not dict or type(row['total_articles']) is not int or row['total_articles'] < 0
                    or facts['total_articles_raw'] != row['total_articles']
                    or facts['source_day_bounds'] != list(key[1:])):
                return None
            if row['state'] == 'partition_list_complete':
                children = facts['children']
                edges = [r[0] for r in queue.db.execute(
                    'SELECT child_scope FROM cnyes_history_edges WHERE parent_scope=? ORDER BY child_scope', (current,))]
                if (type(children) is not list or not children or [c['scope_url'] for c in children] != edges
                        or facts['all_child_ID_sets_disjoint'] is not True
                        or facts['child_total_sum_matches_retained_parent'] is not True):
                    return None
                child_values = [verify(c['scope_url'], c['proof_sha256']) for c in children]
                if any(v is None for v in child_values):
                    return None
                ranges = sorted(scope_key(c['scope_url'])[1:] for c in children)
                seen_ids = set()
                for child in child_values:
                    if not seen_ids.isdisjoint(child['members']):
                        return None
                    seen_ids.update(child['members'])
                if (ranges[0][0] != key[1] or ranges[-1][1] != key[2]
                        or any(left[1] + 1 != right[0] for left, right in zip(ranges, ranges[1:]))
                        or sum(c['total'] for c in child_values) != row['total_articles']
                        or seen_ids != set(members)):
                    return None
            else:
                pages = row['total_pages']
                if (type(pages) is not int or not 1 <= pages <= 30 or facts['total_pages_raw'] != pages
                        or queue.db.execute('SELECT 1 FROM cnyes_history_unproven_pages '
                            'WHERE scope_url=? AND page<=? LIMIT 1', (current, pages)).fetchone()):
                    return None
                heads = queue.db.execute('SELECT h.page AS head_page,h.observation_sha256 AS head_sha,o.* '
                    'FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
                    'ON o.observation_sha256=h.observation_sha256 '
                    'WHERE h.scope_url=? AND h.page<=? ORDER BY h.page', (current, pages)).fetchall()
                if (len(heads) != pages or [h['head_page'] for h in heads] != list(range(1, pages + 1))
                        or [h['head_sha'] for h in heads] != facts['page_observation_sha256']):
                    return None
                seen_ids, references, duplicate_rows = {}, [], 0
                for head in heads:
                    if stop is not None and stop.is_set():
                        return None
                    obs = _checked_page_observation(head, source)
                    if not obs or head['page'] != head['head_page'] or head['observation_sha256'] != head['head_sha']:
                        return None
                    info = obs['pagination']
                    if (head['evidence_contract_version'] != CONTRACT_VERSION
                            or head['source_bulk_fingerprint'] != fingerprint
                            or head['authorization_fingerprint'] != source.authorization_fingerprint
                            or obs['source_day_offset_seconds'] != CNYES_QUERY_DAY_OFFSET_SECONDS
                            or info['total'] != row['total_articles'] or info['pages'] != pages):
                        return None
                    if head['scope_url'] != current:
                        ref = _matching_shared_reference(queue, current, head['head_page'], head['head_sha'], obs)
                        if not ref:
                            return None
                        references.append(dict(ref))
                    identities = obs['identities']
                    if type(identities) is not list:
                        return None
                    page_ids = set()
                    for item in identities:
                        article, stamp, post_sha = item['newsId'], item['publishAt'], item['source_post_sha256']
                        if (type(article) is not int or article <= 0 or type(stamp) is not int
                                or type(post_sha) is not str or len(post_sha) != 64
                                or not key[1] <= (stamp + CNYES_QUERY_DAY_OFFSET_SECONDS) // 86400 <= key[2]):
                            return None
                        prior = seen_ids.setdefault(article, (stamp, post_sha))
                        if prior != (stamp, post_sha):
                            return None
                        page_ids.add(article)
                    duplicate_rows += len(identities) - len(page_ids)
                if (set(seen_ids) != set(members) or duplicate_rows != facts['duplicate_rows_observed']
                        or references != facts['shared_page_ID_references']
                        or (row['state'] == 'list_complete_by_page_ID_equivalence') != bool(references)):
                    return None
            value = {'sha': row['proof_sha256'], 'members': set(members), 'total': row['total_articles']}
            return value
        except (ValueError, KeyError, TypeError, AttributeError, IndexError, OverflowError, RecursionError):
            return None
        finally:
            active.remove(current)
            if cacheable:
                memo[current] = value

    result = verify(scope)
    return result['sha'] if result else None


def integrity_scan_key(source):
    return 'cnyes_history_completed_integrity_v1:' + digest((
        source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))


def _integrity_checkpoint_valid(value):
    return (type(value) is dict and type(value.get('version')) is int and value['version'] == 1
            and value.get('state') in ('scanning', 'complete')
            and type(value.get('cursor')) is str and type(value.get('upper')) is str
            and value['cursor'] <= value['upper']
            and all(type(value.get(k)) is int and value[k] >= 0 for k in ('checked', 'invalid'))
            and type(value.get('next_epoch')) in (int, float)
            and math.isfinite(value['next_epoch']) and value['next_epoch'] >= 0)


class _IntegrityInterrupted(Exception):
    pass


def _integrity_related_scopes(queue, scope, stop):
    """Inspect all siblings of affected ancestors before repairing any head."""
    def check():
        if stop is not None and stop.is_set():
            raise _IntegrityInterrupted()
    ancestors, pending = {scope}, [scope]
    while pending:
        check()
        child = pending.pop()
        for row in queue.db.execute('SELECT parent_scope FROM cnyes_history_edges WHERE child_scope=?', (child,)):
            if row[0] not in ancestors:
                ancestors.add(row[0])
                pending.append(row[0])
    related, pending = set(ancestors), list(ancestors)
    while pending:
        check()
        parent = pending.pop()
        for row in queue.db.execute('SELECT child_scope FROM cnyes_history_edges WHERE parent_scope=?', (parent,)):
            if row[0] not in related:
                related.add(row[0])
                pending.append(row[0])
    return related


def _integrity_quarantine(queue, scope, source):
    head = queue.db.execute('SELECT * FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()
    if (not head or head['state'] not in COMPLETE_STATES
            or (head['provider'], head['evidence_contract_version'], head['source_bulk_fingerprint'],
                head['authorization_fingerprint']) != (source.provider, CONTRACT_VERSION,
                    cnyes_bulk_fingerprint(source), source.authorization_fingerprint)):
        return False
    proof = queue.db.execute('SELECT * FROM cnyes_history_scope_proofs WHERE proof_sha256=?',
                             (head['proof_sha256'],)).fetchone()
    gap = queue.db.execute('SELECT * FROM bulk_window_gaps WHERE scope_url=?', (scope,)).fetchone()
    original = (_audit_sqlite_row(head), None if proof is None else _audit_sqlite_row(proof),
                None if gap is None else _audit_sqlite_row(gap))
    sha = digest(original)
    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_integrity_incidents VALUES (?,?,?,?,?,?)',
        (sha, scope, encoded(original[0]), None if proof is None else encoded(original[1]),
         None if gap is None else encoded(original[2]), now()))
    save_proof(queue, scope, source, 'invalid_scope_evidence',
               {'incident_sha256': sha, 'auto_reconcile': gap is None}, [])
    return True


def _repair_integrity_component(queue, scope, source, *, stop=None):
    """Rebuild only after independently checking sibling evidence.

    The caller holds one SQLite transaction. An interruption rolls back this
    component and its cursor, and no source response or deadline is changed.
    """
    def check():
        if stop is not None and stop.is_set():
            raise _IntegrityInterrupted()
    related = _integrity_related_scopes(queue, scope, stop)
    prefix = (source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)
    invalid = []
    for current in sorted(related):
        check()
        row = queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=? AND provider=? '
            'AND evidence_contract_version=? AND source_bulk_fingerprint=? AND authorization_fingerprint=?',
            (current, *prefix)).fetchone()
        if row and row[0] in COMPLETE_STATES and not _verified_partition_scope_proof(queue, current, source, stop=stop):
            check()
            invalid.append(current)
    for current in invalid:
        _integrity_quarantine(queue, current, source)
    # Reconcile leaves first. Parent reconciliation already propagates after
    # each change, while quarantined siblings cannot be certified complete.
    for current in sorted(related):
        check()
        row = queue.db.execute('SELECT h.state,p.proof_json FROM cnyes_history_scope_heads h '
            'LEFT JOIN cnyes_history_scope_proofs p ON p.proof_sha256=h.proof_sha256 WHERE h.scope_url=? '
            'AND h.provider=? AND h.evidence_contract_version=? AND h.source_bulk_fingerprint=? '
            'AND h.authorization_fingerprint=?', (current, *prefix)).fetchone()
        if not row or row['state'] != 'invalid_scope_evidence':
            continue
        try:
            auto = json.loads(row['proof_json'])['facts']['auto_reconcile'] is True
        except (TypeError, ValueError, KeyError):
            auto = False
        children = [r[0] for r in queue.db.execute('SELECT child_scope FROM cnyes_history_edges '
                                                   'WHERE parent_scope=? ORDER BY child_scope', (current,))]
        if not auto:
            continue
        queue.db.execute('SAVEPOINT cnyes_integrity_reconcile')
        try:
            if not children:
                reconcile_scope(queue, current, source, _current_contract_only=True)
            elif all(_verified_partition_scope_proof(queue, child, source, stop=stop) for child in children):
                reconcile_parents(queue, children[0], source, _current_contract_only=True)
            check()
        except _IntegrityInterrupted:
            queue.db.execute('ROLLBACK TO cnyes_integrity_reconcile')
            queue.db.execute('RELEASE cnyes_integrity_reconcile')
            raise
        except (ValueError, KeyError, TypeError, AttributeError, IndexError, OverflowError, RecursionError):
            queue.db.execute('ROLLBACK TO cnyes_integrity_reconcile')
        queue.db.execute('RELEASE cnyes_integrity_reconcile')
    # Reconciliation may change multiple ancestors. Recheck all newly
    # completed commitments before publishing the transaction.
    for current in sorted(related):
        check()
        row = queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=? AND provider=? '
            'AND evidence_contract_version=? AND source_bulk_fingerprint=? AND authorization_fingerprint=?',
            (current, *prefix)).fetchone()
        if row and row[0] in COMPLETE_STATES and not _verified_partition_scope_proof(queue, current, source, stop=stop):
            check()
            _integrity_quarantine(queue, current, source)
    return len(invalid)


def sweep_completed_scopes(queue, *, stop=None, limit=8, max_seconds=.2, now_epoch=None):
    """Resumable local metadata audit; this never contacts a publisher."""
    result = {'complete': False, 'checked': 0, 'invalid': 0, 'next_epoch': None, 'blocked': None}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result['blocked'] = 'source_disabled'
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    epoch = time.time() if now_epoch is None else now_epoch
    if type(epoch) not in (int, float) or not math.isfinite(epoch) or epoch < 0:
        raise ValueError('invalid integrity scan clock')
    key = integrity_scan_key(source)
    prefix = (source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()
    try:
        state = json.loads(saved[0]) if saved else None
        if state is not None and not _integrity_checkpoint_valid(state):
            raise ValueError('invalid integrity checkpoint')
    except (ValueError, TypeError):
        original = (saved[0] if type(saved[0]) is str else encoded(_audit_sqlite_row({'value': saved[0]})))
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_integrity_checkpoint_audit VALUES (?,?,?,?)',
                (digest((key, original)), key, original, now()))
        state = None
    if state and state['state'] == 'complete' and epoch < state['next_epoch']:
        result.update(complete=True, next_epoch=state['next_epoch'])
        return result
    if not state or state['state'] == 'complete':
        last = [queue.db.execute('SELECT scope_url FROM cnyes_history_scope_heads WHERE provider=? '
            'AND evidence_contract_version=? AND source_bulk_fingerprint=? AND authorization_fingerprint=? '
            'AND state=? ORDER BY scope_url DESC LIMIT 1', (*prefix, item)).fetchone() for item in INTEGRITY_STATES]
        state = {'version': 1, 'state': 'scanning', 'cursor': '',
                 'upper': max((row[0] for row in last if row), default=''), 'checked': 0, 'invalid': 0,
                 'next_epoch': 0}
    params = tuple(value for item in INTEGRITY_STATES for value in (*prefix, item, state['cursor'], state['upper'])) + (limit,)
    started = time.monotonic()
    rows = queue.db.execute(INTEGRITY_SCOPE_SEEK, params).fetchall()
    finished, memo = True, {}
    with queue.db:
        # SAVEPOINT alone can commit on RELEASE when it is the outermost
        # transaction. Start a transaction explicitly so a scope repair and
        # its checkpoint become durable together and memoized reads share one
        # stable snapshot.
        if not queue.db.in_transaction:
            queue.db.execute('BEGIN')
        for row in rows:
            if _history_scan_slice_done(result['checked'], started, max_seconds, stop):
                finished = False
                break
            current = row[0]
            queue.db.execute('SAVEPOINT cnyes_integrity_scope')
            invalid_before = result['invalid']
            try:
                valid = _verified_partition_scope_proof(queue, current, source, stop=stop, _memo=memo)
                if stop is not None and stop.is_set():
                    raise _IntegrityInterrupted()
                if not valid:
                    result['invalid'] += _repair_integrity_component(queue, current, source, stop=stop)
                    memo.clear()  # Reconciliation can replace heads and parent proofs.
                if stop is not None and stop.is_set():
                    raise _IntegrityInterrupted()
            except _IntegrityInterrupted:
                queue.db.execute('ROLLBACK TO cnyes_integrity_scope')
                queue.db.execute('RELEASE cnyes_integrity_scope')
                result['invalid'] = invalid_before
                finished = False
                break
            queue.db.execute('RELEASE cnyes_integrity_scope')
            state['cursor'] = current
            result['checked'] += 1
            state['checked'] += 1
        state['invalid'] += result['invalid']
        if finished and len(rows) < limit:
            state['state'] = 'complete'
            state['next_epoch'] = epoch + INTEGRITY_INTERVAL_SECONDS
            result.update(complete=True, next_epoch=state['next_epoch'])
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (key, encoded(state)))
    return result


def original_envelope_audit_key(source):
    return 'cnyes_history_original_envelope_audit_v1:' + digest((
        source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))


def _original_audit_checkpoint_valid(state):
    return (type(state) is dict and type(state.get('version')) is int and state['version'] == 1
            and state.get('state') in ('scanning', 'complete')
            and all(type(state.get(k)) is str for k in ('cursor_scope', 'upper_scope'))
            and all(type(state.get(k)) is int and state[k] >= 0 for k in
                    ('cursor_page', 'upper_page', 'checked', 'verified', 'mismatched', 'unverified'))
            and state['verified'] + state['mismatched'] + state['unverified'] <= state['checked']
            and (state['cursor_scope'], state['cursor_page']) <= (state['upper_scope'], state['upper_page'])
            and type(state.get('next_epoch')) in (int, float) and math.isfinite(state['next_epoch'])
            and 0 <= state['next_epoch'] <= 1e18
            and ('retry_index_ready' not in state or type(state['retry_index_ready']) is bool)
            and ('classification_version' not in state or type(state['classification_version']) is int
                 and state['classification_version'] == 1))


class _UnsupportedOriginalAuditCheckpointVersion(ValueError):
    pass


def _decode_original_audit_checkpoint(raw):
    state = json.loads(raw)
    if (type(state) is dict and 'version' in state and type(state['version']) is not bool
            and (type(state['version']) is not int or state['version'] != 1)):
        raise _UnsupportedOriginalAuditCheckpointVersion
    if not _original_audit_checkpoint_valid(state):
        raise ValueError('invalid original envelope checkpoint')
    return state


def _original_audit_frontier_has_rows(db, state):
    return bool(db.execute('SELECT 1 FROM cnyes_history_page_heads '
        'WHERE (scope_url,page)>(?,?) AND (scope_url,page)<=(?,?) LIMIT 1',
        (state['cursor_scope'], state['cursor_page'], state['upper_scope'], state['upper_page'])).fetchone())


def _original_audit_archive_identity(key, raw):
    original = raw if type(raw) is str else encoded(_audit_sqlite_row({'value': raw}))
    return digest((key, original)), original


def _archive_original_audit_checkpoint(queue, key, raw):
    audit_sha, original = _original_audit_archive_identity(key, raw)
    with queue.db:
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_integrity_checkpoint_audit VALUES (?,?,?,?)',
                         (audit_sha, key, original, now()))
        archived = queue.db.execute('SELECT checkpoint_key,original_value FROM cnyes_history_integrity_checkpoint_audit '
                                    'WHERE checkpoint_sha256=?', (audit_sha,)).fetchone()
        if not archived or archived[0] != key or archived[1] != original:
            raise _CheckpointAuditConflict


def _current_original_audit_scope_state(queue, source, scope):
    """Permit repair only for a well-formed scope owned by this source contract."""
    try:
        parsed = scope_key(scope)
    except (TypeError, ValueError, IndexError, AttributeError, OverflowError):
        return None
    if not parsed or parsed[0] not in source.cnyes_apis:
        return None
    head = queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=? AND provider=? '
        'AND source_bulk_fingerprint=? AND authorization_fingerprint=? AND evidence_contract_version=?',
        (scope, source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
         CONTRACT_VERSION)).fetchone()
    return head[0] if head else None


def _original_local_repair_key(url, sha):
    return 'cnyes_history_unverified_local_repair_v1:' + digest((url, sha))


def _note_original_local_repair(queue, url, sha, scope, page, observation_sha):
    queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
        (_original_local_repair_key(url, sha), encoded({'scope_url': scope, 'page': page,
                                                        'observation_sha256': observation_sha})))


def _archive_terminal_retained_revision(queue, prior):
    """Free the URL slot without discarding an older completed outcome."""
    if prior['state'] not in ('restored', 'forward_evidence_available', 'retained_unavailable',
                              'retained_invalid', 'retained_request_mismatch'):
        return False
    retry = queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?',
                             (prior['request_url'],)).fetchone()
    original = {'work': _audit_sqlite_row(prior), 'local_retry': _audit_sqlite_row(retry) if retry else None}
    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_revision_audit VALUES (?,?,?,?)',
                     (digest(original), prior['request_url'], encoded(original), now()))
    queue.db.execute('DELETE FROM cnyes_history_retained_local_retries WHERE request_url=?', (prior['request_url'],))
    queue.db.execute('DELETE FROM cnyes_history_retained_work WHERE request_url=?', (prior['request_url'],))
    queue.db.execute('DELETE FROM queue_settings WHERE key=?',
                     (_original_local_repair_key(prior['request_url'], prior['response_sha256']),))
    return True


def _retained_repair_revision_matches(work, url, sha, scope, page, total, ids_sha):
    """An old hash alone does not identify the expectations it was checked against."""
    return (work['request_url'], work['response_sha256'], work['scope_url'], work['page'],
            work['expected_total'], work['expected_ids_sha256']) == (url, sha, scope, page, total, ids_sha)


def _direct_original_mismatch_incident(queue, observation_sha):
    """A recorded identity mismatch is independent of other page markers."""
    key = 'cnyes_history_original_envelope_direct_incident_v1:' + observation_sha
    row = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()
    try:
        incident = json.loads(row[0]) if row else {}
        return (incident.get('observation_sha256') == observation_sha
                and incident.get('reason') in ('request_mismatch', 'authorization_mismatch'))
    except (ValueError, TypeError, AttributeError):
        return False


def _queue_unverified_original_local_repair(queue, source, observation, *, finished_work=None):
    """Queue an explicitly unproven head under its exact request identity."""
    url, sha = observation['request_url'], observation['response_sha256']
    scope, page = observation['scope_url'], observation['page']
    row = queue.db.execute('SELECT h.observation_sha256,m.evidence_json FROM cnyes_history_page_heads h '
        'JOIN cnyes_history_unproven_pages m ON m.scope_url=h.scope_url AND m.page=h.page '
        'WHERE h.scope_url=? AND h.page=?', (scope, page)).fetchone()
    try:
        marker = _decode_unproven_marker(row['evidence_json']) if row else None
        if marker is None:
            return False
        stamp = datetime.fromisoformat(observation['source_observed_at_utc'])
        own_marker = (row and marker.get('actual_observation_sha256') == row['observation_sha256']
                      and marker.get('reason') in ('original_envelope_invalid', 'original_envelope_unavailable',
                                                  'original_envelope_request_mismatch', 'original_envelope_authorization_mismatch'))
        if (not row or not (own_marker or _direct_original_mismatch_incident(queue, row['observation_sha256']))
                or observation['contract_version'] != CONTRACT_VERSION
                or observation['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
                or observation['authorization_fingerprint'] != source.authorization_fingerprint
                or stamp.tzinfo is None or stamp.utcoffset() is None):
            return False
    except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
        return False
    info = observation['pagination']
    identity = (url, sha, scope, page, info['total'], info['ids_sha256'])
    if finished_work and _retained_repair_revision_matches(finished_work, *identity):
        # The just-finished attempt already inspected these exact expectations.
        # A marker alone is not new evidence and must not create a busy loop.
        return False
    prior = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    if prior and not _retained_repair_revision_matches(prior, *identity):
        if not _archive_terminal_retained_revision(queue, prior):
            return False  # An unfinished older revision keeps its turn.
        prior = None
    if not prior:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            'VALUES (?,?,?,?,?,?,?)', (url, scope, page, sha, info['total'], info['ids_sha256'],
                                       observation['source_observed_at_utc']))
    elif prior['state'] not in ('pending', 'overflow_pending', 'local_retry'):
        _save_retained_resolution(queue, prior, 'pending', None, 'unverified_original_local_candidate')
    elif prior['state'] == 'local_retry':
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 '
            "WHERE request_url=? AND error_code='original_local_candidate_unavailable'", (url,))
    if own_marker and marker['reason'] in ('original_envelope_invalid', 'original_envelope_unavailable'):
        _note_original_local_repair(queue, url, sha, scope, page, row['observation_sha256'])
    return True


def _has_alternate_original_candidate(queue, source, observation):
    """Presence permits validation; it never proves request identity."""
    url, sha = observation['request_url'], observation['response_sha256']
    original_path = observation['artifact'].get('parquet_path')
    try:
        for path, _ in _retained_envelopes(queue.root, sha, url, source.authorization_fingerprint):
            if str(path.relative_to(queue.root)) == original_path:
                continue
            if path.is_file() and path.with_suffix('.parquet.receipt.json').is_file():
                return True
    except (OSError, MemoryError):
        pass  # A transient local lookup failure is not a publisher failure.
    return bool(queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates '
        'WHERE request_url=? AND response_sha256=? LIMIT 1', (url, sha)).fetchone())


def _queue_unverified_alias_local_repair(queue, source, scope, page, *, finished_work=None,
                                         require_candidate=False):
    """Validate the alias's own retained response; never relabel its exemplar."""
    row = queue.db.execute('SELECT h.observation_sha256,m.evidence_json,o.scope_url AS original_scope,'
        'o.source_bulk_fingerprint,o.authorization_fingerprint,o.evidence_contract_version '
        'FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
        'ON o.observation_sha256=h.observation_sha256 '
        'JOIN cnyes_history_unproven_pages m ON m.scope_url=h.scope_url AND m.page=h.page '
        'WHERE h.scope_url=? AND h.page=?', (scope, page)).fetchone()
    try:
        marker = _decode_unproven_marker(row['evidence_json']) if row else None
        if marker is None:
            return False
        parsed = scope_key(scope)
        query = parse_qs(scope.split('?', 1)[1]) if parsed else {}
        if (not row or row['original_scope'] == scope or marker.get('actual_observation_sha256') != row['observation_sha256']
                or marker.get('reason') not in ('original_envelope_invalid', 'original_envelope_unavailable',
                                               'original_envelope_request_mismatch', 'original_envelope_authorization_mismatch')
                or row['evidence_contract_version'] != CONTRACT_VERSION
                or row['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
                or row['authorization_fingerprint'] != source.authorization_fingerprint
                or not parsed or parsed[0] not in source.cnyes_apis or not 1 <= page <= 30
                or 'startAt' not in query or 'endAt' not in query):
            return False
        url = cnyes_url(parsed[0], page, start=int(query['startAt'][0]), end=int(query['endAt'][0]))
    except (ValueError, TypeError, KeyError, IndexError, AttributeError, OverflowError):
        return False
    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
    evidence = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                (scope, page)).fetchone()
    if (not job or job['provider'] != source.provider or job['kind'] != 'cnyes_news'
            or not job['last_checked_at_utc'] or not evidence
            or type(job['response_sha256']) is not str or len(job['response_sha256']) != 64
            or any(ch not in '0123456789abcdef' for ch in job['response_sha256'])):
        return False
    identity = (url, job['response_sha256'], scope, page, evidence['total'], evidence['ids_sha256'])
    if finished_work and _retained_repair_revision_matches(finished_work, *identity):
        return False
    if ((require_candidate or marker['reason'] in ('original_envelope_invalid', 'original_envelope_unavailable'))
            and not _has_alternate_original_candidate(queue, source, {
                'request_url': url, 'response_sha256': job['response_sha256'], 'artifact': {}})):
        return False
    prior = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    if prior and not _retained_repair_revision_matches(prior, *identity):
        if not _archive_terminal_retained_revision(queue, prior):
            return False
        prior = None
    if not prior:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            'VALUES (?,?,?,?,?,?,?)', (url, scope, page, job['response_sha256'], evidence['total'],
                                       evidence['ids_sha256'], evidence['observed_at_utc']))
    elif prior['state'] not in ('pending', 'overflow_pending', 'local_retry'):
        _save_retained_resolution(queue, prior, 'pending', None, 'unverified_alias_local_candidate')
    elif prior['state'] == 'local_retry':
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 '
            "WHERE request_url=? AND error_code='original_local_candidate_unavailable'", (url,))
    if marker['reason'] in ('original_envelope_invalid', 'original_envelope_unavailable'):
        _note_original_local_repair(queue, url, job['response_sha256'], scope, page, row['observation_sha256'])
    return True


def _reconsider_unverified_original_after_work(queue, source, scope, page, *, finished_work=None):
    """A finished older revision can release this URL's next local proof."""
    row = queue.db.execute('SELECT o.*,m.evidence_json AS unproven_evidence FROM cnyes_history_unproven_pages m '
        'JOIN cnyes_history_page_heads h ON h.scope_url=m.scope_url AND h.page=m.page '
        'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
        'WHERE m.scope_url=? AND m.page=?', (scope, page)).fetchone()
    observation = _checked_page_observation(row, source)
    if not observation:
        return
    marker = _decode_unproven_marker(row['unproven_evidence'])
    if marker is None:
        # The local work finished, but an unreadable page marker still needs
        # its own preserved evidence and source recheck under normal gates.
        reconcile_scope(queue, scope, source)
        return
    if observation['scope_url'] == scope:
        reason = marker.get('reason')
        if (reason in ('original_envelope_request_mismatch', 'original_envelope_authorization_mismatch')
                or _direct_original_mismatch_incident(queue, row['observation_sha256'])
                or _has_alternate_original_candidate(queue, source, observation)):
            _queue_unverified_original_local_repair(queue, source, observation, finished_work=finished_work)
    else:
        _queue_unverified_alias_local_repair(queue, source, scope, page, finished_work=finished_work)


def _is_unverified_original_repair(queue, work):
    row = queue.db.execute('SELECT h.observation_sha256,o.response_sha256,o.scope_url AS original_scope,m.evidence_json '
        'FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
        'ON o.observation_sha256=h.observation_sha256 '
        'JOIN cnyes_history_unproven_pages m ON m.scope_url=h.scope_url AND m.page=h.page '
        'WHERE h.scope_url=? AND h.page=?', (work['scope_url'], work['page'])).fetchone()
    try:
        marker = _decode_unproven_marker(row['evidence_json']) if row else None
        local = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                                 (_original_local_repair_key(work['request_url'], work['response_sha256']),)).fetchone()
        repair = json.loads(local[0]) if local else {}
        return bool(row and marker is not None and (row['original_scope'] != work['scope_url']
                             or row['response_sha256'] == work['response_sha256'])
            and marker.get('actual_observation_sha256') == row['observation_sha256']
            and marker.get('reason') in ('original_envelope_invalid', 'original_envelope_unavailable')
            and repair == {'scope_url': work['scope_url'], 'page': work['page'],
                           'observation_sha256': row['observation_sha256']})
    except (ValueError, TypeError, AttributeError, RecursionError):
        return False


def audit_original_envelopes(queue, *, stop=None, limit=8, max_seconds=.2, now_epoch=None):
    """Audit legacy request identity in bounded, restartable owner slices.

    Missing or unreadable local files never grant permission to contact the
    publisher. An independently retained local candidate may be validated by
    the canonical owner without changing source jobs or their deadlines.
    """
    from downloader.news_fulltext_cnyes_priority import record_audit_result, select_priority_retries

    result = {'complete': False, 'checked': 0, 'mismatched': 0, 'unverified': 0,
              'equal_day_page_ID_recoveries': 0, 'next_epoch': None, 'blocked': None}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        return {**result, 'complete': True, 'blocked': 'source_disabled'}
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    epoch = time.time() if now_epoch is None else now_epoch
    if (type(epoch) not in (int, float) or not math.isfinite(epoch)
            or not 0 <= epoch <= 1e18 - ORIGINAL_ENVELOPE_REAUDIT_SECONDS):
        raise ValueError('invalid original envelope audit clock')
    key = original_envelope_audit_key(source)
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()
    try:
        state = _decode_original_audit_checkpoint(saved[0]) if saved else None
    except _UnsupportedOriginalAuditCheckpointVersion:
        result['blocked'] = 'unsupported_original_audit_checkpoint_version'
        return result
    except (ValueError, TypeError):
        if stop is not None and stop.is_set():
            return result
        try:
            _archive_original_audit_checkpoint(queue, key, saved[0])
        except _CheckpointAuditConflict:
            result['blocked'] = 'original_audit_checkpoint_audit_conflict'
            return result
        state = None
    if (state and state['state'] == 'scanning'
            and (not state.get('retry_index_ready') or state.get('classification_version') != 1)):
        # Older partial scans may already have passed missing originals or
        # unclassifiable metadata. Preserve the checkpoint and rescan the range.
        if stop is not None and stop.is_set():
            return result
        try:
            _archive_original_audit_checkpoint(queue, key, saved[0])
        except _CheckpointAuditConflict:
            result['blocked'] = 'original_audit_checkpoint_audit_conflict'
            return result
        state = None
    if state and state['state'] == 'complete' and _original_audit_frontier_has_rows(queue.db, state):
        state = {**state, 'state': 'scanning', 'next_epoch': 0}
    full_due = state is None or state['state'] == 'scanning' or epoch >= state['next_epoch']
    retry = queue.db.execute('SELECT not_before_epoch FROM cnyes_history_original_file_retries '
        'WHERE audit_key=? ORDER BY not_before_epoch,scope_url,page,observation_sha256 LIMIT 1', (key,)).fetchone()
    retry_due = retry is not None and retry[0] <= epoch
    if not full_due and not retry_due:
        result.update(complete=True, next_epoch=min(state['next_epoch'], retry[0] if retry else state['next_epoch']))
        return result
    # Persist alternation when both kinds are due, so repeated owner restarts do
    # not starve either the daily sweep or a large set of missing originals.
    preference_key = key + ':next_slice'
    if full_due and retry_due:
        preference = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (preference_key,)).fetchone()
        mode = 'full' if preference and preference[0] == 'full' else 'retry'
    else:
        mode = 'full' if full_due else 'retry'
    if mode == 'full' and (state is None or state['state'] == 'complete'):
        upper = queue.db.execute('SELECT scope_url,page FROM cnyes_history_page_heads '
                                 'ORDER BY scope_url DESC,page DESC LIMIT 1').fetchone()
        state = {'version': 1, 'state': 'scanning', 'cursor_scope': '', 'cursor_page': 0,
                 'upper_scope': upper[0] if upper else '', 'upper_page': upper[1] if upper else 0,
                 'checked': 0, 'verified': 0, 'mismatched': 0, 'unverified': 0, 'next_epoch': 0,
                 'retry_index_ready': True, 'classification_version': 1}
    retry_preference = None
    if mode == 'full':
        rows = queue.db.execute('SELECT h.scope_url AS head_scope,h.page AS head_page,h.observation_sha256 AS head_sha,o.* '
            'FROM cnyes_history_page_heads h LEFT JOIN cnyes_history_page_observations o '
            'ON o.observation_sha256=h.observation_sha256 '
            'WHERE (h.scope_url,h.page)>(?,?) AND (h.scope_url,h.page)<=(?,?) '
            'ORDER BY h.scope_url,h.page LIMIT ?',
            (state['cursor_scope'], state['cursor_page'], state['upper_scope'], state['upper_page'], limit)).fetchall()
    else:
        rows, retry_preference = select_priority_retries(queue, key, epoch, limit)
        if rows is None:
            rows = queue.db.execute('SELECT r.scope_url AS head_scope,r.page AS head_page,'
                'r.observation_sha256 AS retry_sha,h.observation_sha256 AS head_sha,o.* '
                'FROM cnyes_history_original_file_retries r LEFT JOIN cnyes_history_page_heads h '
                'ON h.scope_url=r.scope_url AND h.page=r.page LEFT JOIN cnyes_history_page_observations o '
                'ON o.observation_sha256=h.observation_sha256 '
                'WHERE r.audit_key=? AND r.not_before_epoch<=? '
                'ORDER BY r.not_before_epoch,r.scope_url,r.page,r.observation_sha256 LIMIT ?',
                (key, epoch, limit)).fetchall()
    verifier = getattr(queue, '_cnyes_original_envelope_audit_cache', None)
    if verifier is None:
        verifier = _OriginalEnvelopeAuditCache()
        queue._cnyes_original_envelope_audit_cache = verifier
    started, finished = time.monotonic(), True
    recovered_heads = {}
    with queue.db:
        if full_due and retry_due:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
                             (preference_key, 'full' if mode == 'retry' else 'retry'))
        for row in rows:
            if _history_scan_slice_done(result['checked'], started, max_seconds, stop):
                finished = False
                break
            # A preceding scope reconciliation in this slice may have
            # replaced a head selected by the initial bounded query.
            head = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads '
                                    'WHERE scope_url=? AND page=?', (row['head_scope'], row['head_page'])).fetchone()
            if (not head or head[0] != row['head_sha'] or
                    mode == 'retry' and row['retry_sha'] != row['head_sha']):
                # Only skip a retry when this very slice replaced this exact
                # old head with a fully checked source/reference pair.
                verified_here = bool(head and recovered_heads.get(
                    (row['head_scope'], row['head_page'], row['head_sha'])) == head[0])
                if mode == 'retry':
                    queue.db.execute('DELETE FROM cnyes_history_original_file_retries '
                        'WHERE audit_key=? AND scope_url=? AND page=? AND observation_sha256=?',
                        (key, row['head_scope'], row['head_page'], row['retry_sha']))
                    if head and not verified_here:
                        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_original_file_retries '
                            'VALUES (?,?,?,?,?,0)', (key, row['head_scope'], row['head_page'], head[0], epoch))
                else:
                    state['cursor_scope'], state['cursor_page'] = row['head_scope'], row['head_page']
                    state['checked'] += 1
                    if not verified_here:
                        state['unverified'] += 1
                        result['unverified'] += 1
                    if head and not verified_here:
                        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_original_file_retries '
                            'VALUES (?,?,?,?,?,0)', (key, row['head_scope'], row['head_page'], head[0], epoch))
                    elif not head and _current_original_audit_scope_state(queue, source, row['head_scope']) is not None:
                        reconcile_scope(queue, row['head_scope'], source)
                record_audit_result(queue, key, row['head_scope'], row['head_page'],
                                    row['retry_sha'] if mode == 'retry' else row['head_sha'], 'superseded')
                result['checked'] += 1
                continue
            observation = _checked_page_observation(row)
            status = 'skipped'
            same_contract = False
            if observation and observation['page'] == row['head_page']:
                try:
                    parsed_scope = scope_key(row['head_scope'])
                except (TypeError, ValueError, IndexError, AttributeError, OverflowError):
                    parsed_scope = None
                if not parsed_scope:
                    status = 'skipped'
                elif (parsed_scope[0] not in source.cnyes_apis or observation['provider'] != source.provider
                        or wordpress_base(observation['request_url']) not in source.cnyes_apis
                        or observation['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
                        or observation['authorization_fingerprint'] != source.authorization_fingerprint):
                    status = 'outside'
                else:
                    same_contract = True
                    if observation['artifact'].get('kind') not in ('canonical_quarantine', 'cnyes_history_source_envelope'):
                        status = 'outside'
                    else:
                        try:
                            status = verifier.check(queue.root, observation['artifact'], observation['request_url'],
                                                    observation['response_sha256'], source.authorization_fingerprint)
                        except (OSError, MemoryError):
                            status = 'unavailable'
            if status == 'request_mismatch' and observation:
                recovery_row = row
                if row['head_scope'] != observation['scope_url']:
                    # An alias can sort before its direct head.  Try the real
                    # observation before marking either scope unproven, so a
                    # recoverable legacy cycle does not block itself.
                    recovery_row = queue.db.execute('SELECT h.scope_url AS head_scope,h.page AS head_page,'
                        'h.observation_sha256 AS head_sha,o.* FROM cnyes_history_page_heads h '
                        'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
                        'WHERE h.scope_url=? AND h.page=? AND h.observation_sha256=?',
                        (observation['scope_url'], observation['page'], row['head_sha'])).fetchone()
                try:
                    recovered = bool(recovery_row and _recover_equal_day_original_mismatch(
                        queue, source, recovery_row, observation, verifier=verifier))
                except (OSError, MemoryError, ValueError, KeyError, TypeError, AttributeError,
                        OverflowError, NewsError, pa.ArrowException, _CheckpointAuditConflict):
                    recovered = False
                if recovered:
                    replacement = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads '
                        'WHERE scope_url=? AND page=?', (recovery_row['head_scope'], recovery_row['head_page'])).fetchone()
                    ref = (queue.db.execute('SELECT reference_scope FROM cnyes_history_page_ID_references '
                        'WHERE scope_url=? AND page=? AND observation_sha256=?',
                        (recovery_row['head_scope'], recovery_row['head_page'], replacement[0])).fetchone()
                           if replacement else None)
                    if replacement and ref:
                        for recovered_scope in (recovery_row['head_scope'], ref[0]):
                            current = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads '
                                'WHERE scope_url=? AND page=?', (recovered_scope, recovery_row['head_page'])).fetchone()
                            if current and current[0] == replacement[0]:
                                recovered_heads[(recovered_scope, recovery_row['head_page'],
                                                 recovery_row['head_sha'])] = replacement[0]
                    # The old exact-request original remains mismatched; only
                    # a new, independently evidenced list-ID head is adopted.
                    if mode == 'full':
                        state['mismatched'] += 1
                    result['mismatched'] += 1
                    result['equal_day_page_ID_recoveries'] += 1
                    status = 'superseded'
            if status == 'verified':
                if mode == 'full':
                    state['verified'] += 1
                marker = queue.db.execute('SELECT (SELECT evidence_json FROM cnyes_history_unproven_pages '
                    'WHERE scope_url=? AND page=?), (SELECT state FROM cnyes_history_scope_heads '
                    'WHERE scope_url=? AND provider=? '
                    'AND source_bulk_fingerprint=? AND authorization_fingerprint=? AND evidence_contract_version=?)',
                    (row['head_scope'], row['head_page'], row['head_scope'], source.provider,
                     cnyes_bulk_fingerprint(source), source.authorization_fingerprint, CONTRACT_VERSION)).fetchone()
                marker_data = _decode_unproven_marker(marker[0]) if marker[0] is not None else None
                own_marker = bool(marker_data is not None and
                    marker_data.get('reason') in ('original_envelope_invalid', 'original_envelope_unavailable',
                                                  'original_envelope_request_mismatch', 'original_envelope_authorization_mismatch')
                    and marker_data.get('actual_observation_sha256') == row['head_sha'])
                if marker[0] is not None and marker_data is None:
                    reconcile_scope(queue, row['head_scope'], source)
                elif own_marker and marker[1] is not None:
                    queue.db.execute('DELETE FROM cnyes_history_unproven_pages WHERE scope_url=? AND page=?',
                                     (row['head_scope'], row['head_page']))
                    reconcile_scope(queue, row['head_scope'], source)
                elif marker[1] == 'invalid_page_evidence':
                    # Metadata can be restored by another local worker without
                    # an original-file marker. Rebuild the scope proof too.
                    reconcile_scope(queue, row['head_scope'], source)
            elif status == 'skipped':
                if mode == 'full':
                    state['unverified'] += 1
                result['unverified'] += 1
                if _current_original_audit_scope_state(queue, source, row['head_scope']) is not None:
                    reconcile_scope(queue, row['head_scope'], source)
            elif (status == 'outside' and same_contract
                    and _current_original_audit_scope_state(queue, source, row['head_scope']) == 'invalid_page_evidence'):
                # A valid accepted-body or restricted-metadata observation can
                # repair its metadata proof without claiming a file audit.
                reconcile_scope(queue, row['head_scope'], source)
            elif status in ('invalid', 'unavailable'):
                if mode == 'full':
                    state['unverified'] += 1
                result['unverified'] += 1
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_unproven_pages VALUES (?,?,?)',
                    (row['head_scope'], row['head_page'], encoded({'reason': 'original_envelope_' + status,
                     'actual_observation_sha256': row['head_sha'], 'original_request_url': observation['request_url']})))
                reconcile_scope(queue, row['head_scope'], source)
                if (row['head_scope'] == observation['scope_url'] and row['head_page'] == observation['page']
                        and _has_alternate_original_candidate(queue, source, observation)):
                    _queue_unverified_original_local_repair(queue, source, observation)
                elif row['head_scope'] != observation['scope_url']:
                    _queue_unverified_alias_local_repair(queue, source, row['head_scope'], row['head_page'])
            elif status in ('request_mismatch', 'authorization_mismatch'):
                if mode == 'full':
                    state['mismatched'] += 1
                result['mismatched'] += 1
                # Mark the encountered head and its direct source head. Other
                # aliases are visited by this same resumable scan.
                scopes = {(row['head_scope'], row['head_page'])}
                direct = queue.db.execute('SELECT 1 FROM cnyes_history_page_heads '
                    'WHERE scope_url=? AND page=? AND observation_sha256=?',
                    (observation['scope_url'], observation['page'], row['head_sha'])).fetchone()
                if direct:
                    scopes.add((observation['scope_url'], observation['page']))
                incident_key = 'cnyes_history_original_envelope_incident_v1:' + row['head_sha']
                fresh = not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (incident_key,)).fetchone()
                for scope, page in scopes:
                    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_unproven_pages VALUES (?,?,?)',
                        (scope, page, encoded({'reason': 'original_envelope_' + status,
                         'actual_observation_sha256': row['head_sha'], 'original_request_url': observation['request_url']})))
                    reconcile_scope(queue, scope, source)
                if fresh:
                    queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (incident_key, encoded({
                        'observation_sha256': row['head_sha'], 'reason': status, 'detected_at_utc': now(),
                        'source_failures_or_deadlines_reset': False})))
                # The same observation can first be found through an alias
                # while its direct head is absent. The observation incident
                # is global, but admitting the direct repair is a separate,
                # durable one-shot operation when that head becomes available.
                direct_key = 'cnyes_history_original_envelope_direct_incident_v1:' + row['head_sha']
                direct_admitted = queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (direct_key,)).fetchone()
                if direct and not direct_admitted:
                    queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (direct_key, encoded({
                        'observation_sha256': row['head_sha'], 'reason': status, 'admitted_at_utc': now(),
                        'source_failures_or_deadlines_reset': False})))
                    info = observation['pagination']
                    try:
                        observed = datetime.fromisoformat(observation['source_observed_at_utc'])
                        has_clock = observed.tzinfo is not None and observed.utcoffset() is not None
                    except (ValueError, KeyError, TypeError):
                        has_clock = False
                    prior = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?',
                                             (observation['request_url'],)).fetchone()
                    identity = (observation['request_url'], observation['response_sha256'], observation['scope_url'],
                                observation['page'], info['total'], info['ids_sha256'])
                    if not prior and has_clock:
                        queue.db.execute('INSERT INTO cnyes_history_retained_work '
                            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
                            'VALUES (?,?,?,?,?,?,?)', (identity[0], identity[2], identity[3], identity[1],
                                                       identity[4], identity[5], observation['source_observed_at_utc']))
                    elif prior and not _retained_repair_revision_matches(prior, *identity):
                        _reconsider_unverified_original_after_work(queue, source, observation['scope_url'], observation['page'])
                    elif fresh and prior and prior['state'] not in ('pending', 'overflow_pending', 'local_retry'):
                        _save_retained_resolution(queue, prior, 'pending', None, 'original_envelope_identity_mismatch')
                    elif (prior and prior['state'] not in ('pending', 'overflow_pending', 'local_retry')
                          and _has_alternate_original_candidate(queue, source, observation)):
                        _queue_unverified_original_local_repair(queue, source, observation)
                    elif not prior:
                        schedule_original_page_check(queue, observation['request_url'],
                            reason='original_envelope_identity_mismatch_and_invalid_clock', evidence_sha=row['head_sha'])
                elif direct:
                    # A later independent local capture may appear after the
                    # first attempt finished. Reconsider it on the periodic
                    # audit, but never repeatedly queue an unchanged failure.
                    prior = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                                             (observation['request_url'],)).fetchone()
                    if ((not prior or prior['state'] not in ('pending', 'overflow_pending', 'local_retry'))
                            and _has_alternate_original_candidate(queue, source, observation)):
                        _queue_unverified_original_local_repair(queue, source, observation)
                if direct and row['head_scope'] != observation['scope_url']:
                    # An alias may retain its own exact capture even while the
                    # exemplar still exists with an unproven original. Each
                    # request can recover independently without relabelling.
                    _queue_unverified_alias_local_repair(queue, source, row['head_scope'], row['head_page'],
                                                         require_candidate=True)
                if not direct:
                    # An alias can outlive its original head. Give this alias
                    # its own recovery identity; never relabel the old raw.
                    alias_key = 'cnyes_history_original_envelope_alias_incident_v1:' + digest((
                        row['head_scope'], row['head_page'], row['head_sha']))
                    if not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (alias_key,)).fetchone():
                        parsed = scope_key(row['head_scope'])
                        query = parse_qs(row['head_scope'].split('?', 1)[1]) if parsed else {}
                        if parsed and 'startAt' in query and 'endAt' in query:
                            url = cnyes_url(parsed[0], row['head_page'], start=int(query['startAt'][0]),
                                            end=int(query['endAt'][0]))
                            job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
                            evidence = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                (row['head_scope'], row['head_page'])).fetchone()
                            prior = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?',
                                                     (url,)).fetchone()
                            local_candidate = (job and job['kind'] == 'cnyes_news' and job['provider'] == source.provider
                                               and job['last_checked_at_utc'] and evidence
                                               and _retained_envelopes(queue.root, job['response_sha256']))
                            if local_candidate and not prior:
                                queue.db.execute('INSERT INTO cnyes_history_retained_work '
                                    '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
                                    'VALUES (?,?,?,?,?,?,?)', (url, row['head_scope'], row['head_page'], job['response_sha256'],
                                    evidence['total'], evidence['ids_sha256'], evidence['observed_at_utc']))
                            elif local_candidate and prior['response_sha256'] == job['response_sha256'] and prior['state'] not in (
                                    'pending', 'overflow_pending', 'local_retry'):
                                _save_retained_resolution(queue, prior, 'pending', None, 'original_alias_identity_mismatch')
                            elif local_candidate and prior['response_sha256'] != job['response_sha256']:
                                _reconsider_unverified_original_after_work(queue, source, row['head_scope'], row['head_page'])
                            elif not local_candidate:
                                schedule_original_page_check(queue, url,
                                    reason='original_alias_identity_mismatch', evidence_sha=row['head_sha'])
                            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (alias_key, encoded({
                                'observation_sha256': row['head_sha'], 'changed_at_utc': now(),
                                'source_failures_or_deadlines_reset': False})))
                    else:
                        _queue_unverified_alias_local_repair(queue, source, row['head_scope'], row['head_page'],
                                                             require_candidate=True)
            retry_identity = (key, row['head_scope'], row['head_page'], row['head_sha'])
            if status in ('invalid', 'unavailable', 'skipped'):
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_original_file_retries VALUES (?,?,?,?,?,0)',
                                 (*retry_identity, epoch + INTEGRITY_INTERVAL_SECONDS))
            if mode == 'retry' and status in ('invalid', 'unavailable', 'skipped'):
                queue.db.execute('UPDATE cnyes_history_original_file_retries '
                    'SET not_before_epoch=?,attempts=attempts+1 '
                    'WHERE audit_key=? AND scope_url=? AND page=? AND observation_sha256=?',
                    (epoch + INTEGRITY_INTERVAL_SECONDS, *retry_identity))
            elif status in ('verified', 'request_mismatch', 'authorization_mismatch', 'outside', 'superseded'):
                queue.db.execute('DELETE FROM cnyes_history_original_file_retries '
                    'WHERE audit_key=? AND scope_url=? AND page=? AND observation_sha256=?', retry_identity)
            record_audit_result(queue, key, row['head_scope'], row['head_page'], row['head_sha'], status)
            if mode == 'full':
                state['cursor_scope'], state['cursor_page'] = row['head_scope'], row['head_page']
                state['checked'] += 1
            result['checked'] += 1
        if retry_preference is not None and result['checked']:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', retry_preference)
        if mode == 'full':
            if finished and len(rows) < limit:
                state['state'] = 'complete'
                state['next_epoch'] = epoch + ORIGINAL_ENVELOPE_REAUDIT_SECONDS
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (key, encoded(state)))
        retry = queue.db.execute('SELECT not_before_epoch FROM cnyes_history_original_file_retries '
            'WHERE audit_key=? ORDER BY not_before_epoch,scope_url,page,observation_sha256 LIMIT 1', (key,)).fetchone()
        if state and state['state'] == 'complete' and epoch < state['next_epoch'] and (not retry or retry[0] > epoch):
            result.update(complete=True, next_epoch=min(state['next_epoch'], retry[0] if retry else state['next_epoch']))
    return result


def _save_retained_resolution(queue, work, resolution, observation_sha, evidence_kind):
    values = (work['request_url'], work['state'], work['outcome_json'], resolution, observation_sha, evidence_kind)
    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_outcome_resolutions VALUES (?,?,?,?,?,?,?,?)',
                     (digest(values), *values, now()))
    queue.db.execute('UPDATE cnyes_history_retained_work SET state=? WHERE request_url=?',
                     (resolution, work['request_url']))


def resolve_forward_retained_outcomes(queue, scope, page, source):
    """When a new page head appears, update matching old work in the same transaction."""
    rows = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE scope_url=? AND page=? '
        "AND state IN ('retained_unavailable','retained_request_mismatch','retained_invalid','pending','local_retry')",
        (scope, page)).fetchall()
    for work in rows:
        original = work
        if work['state'] in ('pending', 'local_retry'):
            # A newly found batch may have woken an old failure before the
            # same source page is directly accepted. Preserve the original
            # failure in both transition receipts; ordinary pending work does
            # not acquire this treatment without a candidate-wake receipt.
            event = queue.db.execute('SELECT original_state,original_outcome_json '
                'FROM cnyes_history_retained_outcome_resolutions WHERE request_url=? '
                "AND resolution_state='pending' AND evidence_kind='new_accepted_body_batch_candidate' "
                'ORDER BY resolved_at_utc DESC LIMIT 1', (work['request_url'],)).fetchone()
            if not event:
                continue
            original = {**dict(work), 'state': event['original_state'], 'outcome_json': event['original_outcome_json']}
        allow_shared = True
        if original['state'] == 'retained_invalid':
            # General invalid originals require an independent direct page;
            # a matching page-ID reference is only sufficient for the
            # previously classified different-request case.
            try:
                allow_shared = (json.loads(original['outcome_json']).get('error')
                                == 'retained_original_request_differs_not_relabelled')
            except (ValueError, TypeError, AttributeError):
                allow_shared = False
        sha, kind = _matching_forward_evidence(queue, work, source, allow_shared=allow_shared)
        if sha:
            _save_retained_resolution(queue, original, 'forward_evidence_available', sha, kind)
            queue.db.execute('DELETE FROM cnyes_history_retained_local_retries WHERE request_url=?',
                             (work['request_url'],))


def reconcile_legacy_request_mismatches(queue, *, stop=None, limit=128, max_seconds=.05):
    """Reconcile invalid originals without losing the original result.

    Direct observations must belong to this exact request and current source
    contract. The v2 cursor revisits work after the earlier mismatch-only
    scan. This pass never creates observations or changes source jobs.
    """
    result = {'complete': False, 'checked': 0, 'reclassified': 0, 'blocked': None,
              'restarted_invalid_checkpoint': False}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result['blocked'] = 'source_disabled'
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    state, result['blocked'], result['restarted_invalid_checkpoint'] = _legacy_scan_checkpoint(
        queue, LEGACY_MISMATCH_SCAN_KEY, LEGACY_MISMATCH_SCAN_KEY + ':', 'request_url', stop)
    if state is None or stop is not None and stop.is_set():
        return result
    if state['state'] == 'complete':
        result['complete'] = True
        return result
    started = time.monotonic()
    rows = queue.db.execute("SELECT * FROM cnyes_history_retained_work WHERE state='retained_invalid' "
                            'AND request_url>? ORDER BY request_url LIMIT ?', (state['request_url'], limit)).fetchall()
    finished = True
    with queue.db:
        for work in rows:
            if _history_scan_slice_done(result['checked'], started, max_seconds, stop):
                finished = False
                break
            try:
                previous = json.loads(work['outcome_json'])
            except (ValueError, TypeError):
                previous = None
            observation_sha, kind = _matching_forward_evidence(queue, work, source, allow_shared=False)
            if observation_sha:
                _save_retained_resolution(queue, work, 'forward_evidence_available', observation_sha, kind)
                result['reclassified'] += 1
            elif isinstance(previous, dict) and previous.get('error') == 'retained_original_request_differs_not_relabelled':
                _save_retained_resolution(queue, work, 'retained_request_mismatch', None, None)
                result['reclassified'] += 1
            state['request_url'] = work['request_url']
            result['checked'] += 1
        if finished and len(rows) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (LEGACY_MISMATCH_SCAN_KEY, encoded(state)))
    return result


def reconcile_legacy_unavailable(queue, *, stop=None, limit=128, max_seconds=.05):
    """Find later page evidence for previously unavailable local originals."""
    result = {'complete': False, 'checked': 0, 'resolved': 0, 'blocked': None,
              'restarted_invalid_checkpoint': False}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result['blocked'] = 'source_disabled'
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    state, result['blocked'], result['restarted_invalid_checkpoint'] = _legacy_scan_checkpoint(
        queue, LEGACY_UNAVAILABLE_SCAN_KEY, LEGACY_UNAVAILABLE_SCAN_KEY + ':', 'request_url', stop)
    if state is None or stop is not None and stop.is_set():
        return result
    if state['state'] == 'complete':
        result['complete'] = True
        return result
    started = time.monotonic()
    rows = queue.db.execute("SELECT * FROM cnyes_history_retained_work WHERE state='retained_unavailable' "
                            'AND request_url>? ORDER BY request_url LIMIT ?', (state['request_url'], limit)).fetchall()
    finished = True
    with queue.db:
        for work in rows:
            if _history_scan_slice_done(result['checked'], started, max_seconds, stop):
                finished = False
                break
            sha, kind = _matching_forward_evidence(queue, work, source, allow_shared=True)
            if sha:
                _save_retained_resolution(queue, work, 'forward_evidence_available', sha, kind)
                result['resolved'] += 1
            state['request_url'] = work['request_url']
            result['checked'] += 1
        if finished and len(rows) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (LEGACY_UNAVAILABLE_SCAN_KEY, encoded(state)))
    return result


def seed_unobserved_empty_scopes(queue, *, stop=None, limit=128, max_seconds=.01):
    """Find old zero-result history without rescanning or certifying it.

    Walk the coverage primary key once in bounded slices. New successful
    responses already write a history head, so this pass only admits old,
    successful, future-due jobs lacking one. Existing source failures and
    already-due jobs remain under the normal HTTP owner's control.
    """
    result = {'complete': False, 'scanned': 0, 'reconciled': 0, 'blocked': None,
              'restarted_invalid_checkpoint': False}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result['blocked'] = 'source_disabled'
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    marker = legacy_empty_scan_key(source)
    state, result['blocked'], result['restarted_invalid_checkpoint'] = _legacy_scan_checkpoint(
        queue, marker, LEGACY_EMPTY_SCAN_PREFIX, 'base_url', stop)
    if state is None or stop is not None and stop.is_set():
        return result
    if state['state'] == 'complete':
        result['complete'] = True
        return result
    started = time.monotonic()
    rows = queue.db.execute('SELECT base_url,provider,total_articles,total_pages FROM bulk_api_coverage '
        'WHERE base_url>? ORDER BY base_url LIMIT ?', (state['base_url'], limit)).fetchall()
    finished = True
    with queue.db:
        for row in rows:
            if _history_scan_slice_done(result['scanned'], started, max_seconds, stop):
                finished = False
                break
            scope = row['base_url']
            if row['provider'] == source.provider and row['total_articles'] == 0 and row['total_pages'] == 1:
                try:
                    key = scope_key(scope)
                    query = parse_qs(scope.split('?', 1)[1]) if key else {}
                    url = cnyes_url(key[0], 1, start=int(query['startAt'][0]), end=int(query['endAt'][0])) if key else None
                except (ValueError, TypeError, IndexError, KeyError):
                    key, url = None, None
                if (key and key[0] in source.cnyes_apis
                        and not queue.db.execute('SELECT 1 FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()
                        and not queue.db.execute('SELECT 1 FROM bulk_window_splits WHERE scope_url=?', (scope,)).fetchone()):
                    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
                    if (job and job['kind'] == 'cnyes_news' and job['provider'] == source.provider
                            and job['error_code'] is None and job['attempts'] == 0
                            and job['superseded_by_windows_json'] is None and job['due'] > time.time()):
                        reconcile_scope(queue, scope, source)
                        result['reconciled'] += 1
            state['base_url'] = scope
            result['scanned'] += 1
        if finished and len(rows) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, encoded(state)))
    return result


def seed_unobserved_nonzero_pages(queue, *, stop=None, limit=128, max_seconds=.01):
    """Inventory legacy local page envelopes once, without a publisher request.

    Coverage and page IDs only locate candidates. The retained worker must
    verify their original request, receipt, bytes, authorization and contents.
    A per-scope fence survives bounded slices and crashes so incomplete local
    inventory cannot wake all of that scope's publisher pages prematurely.
    """
    result = {'complete': False, 'checked': 0, 'queued': 0, 'blocked': None, 'stale_fences_reconciled': 0,
              'restarted_invalid_checkpoint': False}
    source = queue.registry.sources.get('cnyes')
    if (not source or not source.allowed or not source.cnyes_apis
            or source.acquisition_mode == 'finite' or source.known_url_only):
        result.update(complete=True, blocked='source_disabled')
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    source_fp, auth_fp = cnyes_bulk_fingerprint(source), source.authorization_fingerprint
    marker = nonzero_scan_key(source)
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    initial = {'version': 1, 'state': 'scanning', 'scope_url': '', 'page': 30, 'checked': 0, 'queued': 0}
    try:
        state = json.loads(saved[0]) if saved else dict(initial)
        if (type(state) is dict and 'version' in state
                and (type(state['version']) is not int or state['version'] != 1)):
            result['blocked'] = 'unsupported_nonzero_scan_checkpoint_version'
            return result
        if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != 1
                or state.get('state') not in ('scanning', 'complete')
                or type(state.get('scope_url')) is not str or type(state.get('page')) is not int
                or not 0 <= state['page'] <= 30
                or type(state.get('checked')) is not int or state['checked'] < 0
                or type(state.get('queued')) is not int or state['queued'] < 0):
            raise ValueError('invalid checkpoint')
    except (TypeError, ValueError, KeyError):
        if stop is not None and stop.is_set():
            return result
        try:
            _archive_invalid_checkpoint(queue, marker, LEGACY_NONZERO_SCAN_PREFIX, saved)
        except _CheckpointAuditConflict:
            result['blocked'] = 'nonzero_checkpoint_audit_conflict'
            return result
        state = dict(initial)
        result['restarted_invalid_checkpoint'] = True
    if stop is not None and stop.is_set():
        return result
    # Contract changes can interrupt an old scan. Release only its local
    # fence, retain the old identity, and give its pending work a scope head
    # so the usual retry path still runs if its old raw fails validation.
    stale = queue.db.execute("SELECT scope_url FROM cnyes_history_local_inventory_scopes "
        "WHERE scan_state='scanning' AND (source_bulk_fingerprint!=? OR authorization_fingerprint!=?) LIMIT ?",
        (source_fp, auth_fp, limit)).fetchall()
    with queue.db:
        for row in stale:
            if stop is not None and stop.is_set():
                break
            scope = row['scope_url']
            queue.db.execute("UPDATE cnyes_history_local_inventory_scopes SET scan_state='ready' WHERE scope_url=?",
                             (scope,))
            try:
                key = scope_key(scope)
            except (TypeError, ValueError, OverflowError):
                key = None
            if key and key[0] in source.cnyes_apis:
                reconcile_scope(queue, scope, source)
            result['stale_fences_reconciled'] += 1
    if stop is not None and stop.is_set():
        return result
    if state['state'] == 'complete':
        result['complete'] = len(stale) < limit
        return result

    started = time.monotonic()
    comparator = '>' if state['page'] == 30 else '>='
    scopes = queue.db.execute('SELECT base_url,provider,total_articles,total_pages FROM bulk_api_coverage '
        'WHERE base_url' + comparator + '? ORDER BY base_url LIMIT ?', (state['scope_url'], limit)).fetchall()
    exhausted_scopes = True
    with queue.db:
        for row in scopes:
            if result['checked'] >= limit or _history_scan_slice_done(result['checked'], started, max_seconds, stop):
                exhausted_scopes = False
                break
            scope, total, pages_total = row['base_url'], row['total_articles'], row['total_pages']
            try:
                key = scope_key(scope)
                query = parse_qs(scope.split('?', 1)[1]) if key else {}
                low, high = int(query['startAt'][0]), int(query['endAt'][0])
            except (ValueError, TypeError, OverflowError, IndexError, KeyError):
                key = None
            own = queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes WHERE scope_url=?',
                                   (scope,)).fetchone()
            continuing = bool(own and own['scan_state'] == 'scanning'
                              and own['source_bulk_fingerprint'] == source_fp
                              and own['authorization_fingerprint'] == auth_fp)
            eligible = bool(row['provider'] == source.provider and total > 0 and 1 <= pages_total <= 30
                            and key and key[0] in source.cnyes_apis)
            if eligible and not continuing:
                eligible = not any(queue.db.execute(sql, (scope,)).fetchone() for sql in (
                    'SELECT 1 FROM cnyes_history_scope_heads WHERE scope_url=?',
                    'SELECT 1 FROM cnyes_history_retained_work WHERE scope_url=? LIMIT 1',
                    'SELECT 1 FROM bulk_window_splits WHERE scope_url=?',
                    'SELECT 1 FROM bulk_window_gaps WHERE scope_url=?',
                    'SELECT 1 FROM cnyes_history_edges e JOIN bulk_window_gaps g ON g.scope_url=e.parent_scope '
                        'WHERE e.child_scope=? LIMIT 1'))
            if not eligible:
                state.update(scope_url=scope, page=30)
                result['checked'] += 1
                continue
            after = state['page'] if state['scope_url'] == scope else 0
            pages = queue.db.execute('SELECT p.page,p.total,p.ids_sha256,p.observed_at_utc '
                'FROM bulk_page_evidence p LEFT JOIN cnyes_history_page_heads h '
                'ON h.scope_url=p.scope_url AND h.page=p.page '
                "WHERE p.scope_url=? AND typeof(p.page)='integer' AND p.page>=1 "
                'AND p.page>? AND p.page<=? AND h.scope_url IS NULL ORDER BY p.page',
                (scope, after, pages_total)).fetchall()
            for page in pages:
                if result['checked'] >= limit or _history_scan_slice_done(result['checked'], started, max_seconds, stop):
                    exhausted_scopes = False
                    break
                url = cnyes_url(key[0], page['page'], start=low, end=high)
                job = queue.db.execute('SELECT kind,provider,response_sha256,last_checked_at_utc '
                                       'FROM discovery WHERE url=?', (url,)).fetchone()
                sha = job['response_sha256'] if job else None
                if (job and job['kind'] == 'cnyes_news' and job['provider'] == source.provider
                        and job['last_checked_at_utc'] and page['total'] == total):
                    try:
                        present = any(path.is_file() and path.with_suffix('.parquet.receipt.json').is_file()
                                      for path, _ in _retained_envelopes(
                                          queue.root, sha, url, source.authorization_fingerprint))
                    except (OSError, MemoryError):
                        present = bool(_retained_envelopes(queue.root, sha))
                    if not present:
                        present = bool(queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates '
                            'WHERE request_url=? AND response_sha256=? LIMIT 1', (url, sha)).fetchone())
                    if present:
                        inserted = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_work '
                            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
                            'source_checked_at_utc) VALUES (?,?,?,?,?,?,?)',
                            (url, scope, page['page'], sha, total, page['ids_sha256'], page['observed_at_utc']))
                        if inserted.rowcount:
                            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_local_inventory_scopes '
                                'VALUES (?,?,?,?,?)', (scope, source_fp, auth_fp, 'scanning', now()))
                            result['queued'] += 1
                state.update(scope_url=scope, page=page['page'])
                result['checked'] += 1
            else:
                state.update(scope_url=scope, page=30)
                if not pages:
                    result['checked'] += 1
                queue.db.execute("UPDATE cnyes_history_local_inventory_scopes SET scan_state='ready' "
                    "WHERE scope_url=? AND scan_state='scanning' AND source_bulk_fingerprint=? "
                    "AND authorization_fingerprint=?", (scope, source_fp, auth_fp))
                # Even when no retained original exists, this eligible
                # history scope is still unproven. Give the normal repair
                # sweep a durable head after all its pages have been searched;
                # it will preserve source errors and existing due deadlines.
                reconcile_scope(queue, scope, source)
                continue
            break
        if exhausted_scopes and len(scopes) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        state['checked'] += result['checked']
        state['queued'] += result['queued']
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, encoded(state)))
    return result


def _defer_retained_work(queue, work, original_state, error_code, *, candidate_errors=None):
    """Persist a local retry without changing source request state or clocks."""
    with queue.db:
        prior = queue.db.execute('SELECT local_attempts FROM cnyes_history_retained_local_retries '
                                 'WHERE request_url=?', (work['request_url'],)).fetchone()
        attempts = (prior[0] if prior else 0) + 1
        delay = 300 if error_code != 'local_io_unavailable' else min(300, 2 ** min(attempts - 1, 9))
        queue.db.execute('INSERT OR REPLACE INTO cnyes_history_retained_local_retries '
            '(request_url,original_state,not_before_epoch,local_attempts,error_code,last_attempt_at_utc) '
            'VALUES (?,?,?,?,?,?)', (work['request_url'], original_state, time.time() + delay,
                                     attempts, error_code, now()))
        outcome = {'source_bodies_reaccepted': False, 'publisher_requests': 0,
                   'local_retry_reason': error_code}
        if candidate_errors:
            outcome['candidate_errors'] = candidate_errors
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='local_retry',outcome_json=? WHERE request_url=?",
                         (encoded(outcome), work['request_url']))


def maintenance_delay(result, *, now_epoch=None):
    """Wake for durable local retry deadlines, bounded like normal maintenance."""
    if not result['exhausted']:
        scans = ((result.get(key) or {}, counter) for key, counter in (
            ('legacy_mismatch_reconciliation', 'checked'), ('legacy_unavailable_reconciliation', 'checked'),
            ('unobserved_empty_scope_scan', 'scanned'), ('overflow_retained_scan', 'checked'),
            ('unobserved_nonzero_page_scan', 'checked'), ('accepted_body_batch_scan', 'checked'),
            ('completed_integrity_scan', 'checked'), ('original_envelope_audit', 'checked')))
        repair = result.get('evidence_repair') or {}
        nonzero = result.get('unobserved_nonzero_page_scan') or {}
        if (result.get('processed', 0) > 0
                or result.get('repair_priority_pending', False)
                or any(not scan.get('complete') and scan.get(counter, 0) > 0 for scan, counter in scans)
                or not nonzero.get('complete') and nonzero.get('stale_fences_reconciled', 0) > 0
                or (result.get('accepted_missing_batch_retries') or {}).get('processed', 0) > 0
                or (result.get('accepted_body_batch_scan') or {}).get('retried', 0) > 0
                or (result.get('accepted_invalid_batch_retries') or {}).get('processed', 0) > 0
                or ((result.get('accepted_invalid_batch_retries') or {}).get('migration') or {}).get('checked', 0) > 0
                or (result.get('repair_sweep_pending') and repair.get('scopes_checked', 0) > 0)):
            # Useful bounded work can continue on the next owner turn. HTTP
            # dispatch and response handling still occur between slices.
            return 0
        return 2
    deadline = result.get('next_local_retry_epoch')
    if type(deadline) in (int, float) and math.isfinite(deadline) and deadline >= 0:
        return min(300, max(1, deadline - (time.time() if now_epoch is None else now_epoch)))
    return 300


def _is_candidate_wake(queue, work):
    return bool(work['state'] in ('pending', 'local_retry') and queue.db.execute(
        'SELECT 1 FROM cnyes_history_retained_outcome_resolutions WHERE request_url=? '
        "AND resolution_state='pending' AND evidence_kind='new_accepted_body_batch_candidate' LIMIT 1",
        (work['request_url'],)).fetchone())


def backfill(queue, *, stop=None, limit=8, max_seconds=.2, defer_original_audit=False):
    """Bounded normal-owner metadata work; no second HTTP owner or body write."""
    started, done = time.monotonic(), 0
    source = queue.registry.sources.get('cnyes')
    source_available = bool(source and source.allowed and source.cnyes_apis)
    repair_signature = ((source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source),
                         source.authorization_fingerprint) if source_available else None)
    covered = getattr(queue, '_cnyes_history_repair_covered', None)
    repair_covered = bool(repair_signature and covered and covered[0] == repair_signature and started < covered[1])
    # Give a previously starved repair sweep its own turn before slow local
    # scans. Its durable scope cursor still owns progress across restarts.
    repair_first = (getattr(queue, '_cnyes_history_repair_priority', False)
                    and not repair_covered and limit > 0 and max_seconds > 0
                    and not (stop is not None and stop.is_set()))
    repair = (repair_incomplete_scopes(queue, stop=stop, limit=limit, max_seconds=max_seconds / 8,
                                       allow_one_after_timeout=True) if repair_first else None)
    legacy = reconcile_legacy_request_mismatches(queue, stop=stop, limit=limit, max_seconds=max_seconds / 8)
    unavailable = reconcile_legacy_unavailable(queue, stop=stop, limit=limit, max_seconds=max_seconds / 8)
    empty = seed_unobserved_empty_scopes(queue, stop=stop, limit=min(limit * 8, 1024), max_seconds=max_seconds / 8)
    accepted_scan = seed_accepted_body_batch_candidates(queue, stop=stop, limit=min(limit * 8, 1024), max_seconds=max_seconds / 8)
    missing_batches = retry_missing_accepted_batches(queue, stop=stop, limit=limit, max_seconds=max_seconds / 8)
    invalid_batches = retry_invalid_accepted_batches(queue, stop=stop, limit=limit, max_seconds=max_seconds / 8)
    # Direct envelopes and already indexed candidates do not depend on the
    # rest of the batch frontier. A candidate found after this cursor passes
    # is woken by the same batch transaction; source rechecks retain their
    # separate whole-inventory fence.
    nonzero = seed_unobserved_nonzero_pages(queue, stop=stop, limit=min(limit * 8, 1024),
                                           max_seconds=max_seconds / 8)
    scan = seed_overflow_retained(queue, stop=stop, limit=limit, max_seconds=max_seconds / 2)
    lanes = []
    if scan['complete']:
        lanes.append([(dict(row), 'overflow_pending') for row in queue.db.execute(
            "SELECT * FROM cnyes_history_retained_work WHERE state='overflow_pending' ORDER BY request_url LIMIT ?",
            (limit,)).fetchall()])
    lanes.append([(dict(row), 'pending') for row in queue.db.execute(
        "SELECT w.* FROM cnyes_history_retained_work w LEFT JOIN cnyes_history_local_inventory_scopes i "
        "ON i.scope_url=w.scope_url WHERE w.state='pending' AND (i.scope_url IS NULL OR i.scan_state='ready') "
        "ORDER BY w.request_url LIMIT ?",
        (limit,)).fetchall()])
    retry_filter = '' if scan['complete'] else " AND r.original_state='pending'"
    retries = queue.db.execute('SELECT w.*,r.original_state AS retry_original_state '
        'FROM cnyes_history_retained_local_retries r CROSS JOIN cnyes_history_retained_work w '
        "ON w.request_url=r.request_url AND w.state='local_retry' "
        'LEFT JOIN cnyes_history_local_inventory_scopes i ON i.scope_url=w.scope_url '
        "WHERE r.not_before_epoch<=? AND r.error_code!='accepted_batch_scan_pending' "
        "AND (i.scope_url IS NULL OR i.scan_state='ready')" + retry_filter +
        ' ORDER BY r.not_before_epoch,r.request_url LIMIT ?',
        (time.time(), limit)).fetchall()
    lanes.append([(dict(row), row['retry_original_state']) for row in retries])
    if accepted_scan['complete']:
        waiting = queue.db.execute('SELECT w.*,r.original_state AS retry_original_state '
            'FROM cnyes_history_retained_local_retries r CROSS JOIN cnyes_history_retained_work w '
            "ON w.request_url=r.request_url AND w.state='local_retry' "
            'LEFT JOIN cnyes_history_local_inventory_scopes i ON i.scope_url=w.scope_url '
            "WHERE r.error_code='accepted_batch_scan_pending' AND (i.scope_url IS NULL OR i.scan_state='ready')"
            + retry_filter + ' ORDER BY r.request_url LIMIT ?', (limit,)).fetchall()
        lanes.append([(dict(row), row['retry_original_state']) for row in waiting])
    # Rotate among new overflow, normal and due retry work so a single bad
    # file or a continuously replenished lane cannot starve other evidence.
    offset = getattr(queue, '_cnyes_history_lane_offset', 0) % len(lanes)
    queue._cnyes_history_lane_offset = (offset + 1) % len(lanes)
    positions = [0] * len(lanes)
    rows = []
    while len(rows) < limit:
        added = False
        for index in ((offset + step) % len(lanes) for step in range(len(lanes))):
            if positions[index] < len(lanes[index]) and len(rows) < limit:
                rows.append(lanes[index][positions[index]])
                positions[index] += 1
                added = True
        if not added:
            break
    local_io_deferrals = source_scope_deferrals = 0
    completed_urls = set()
    for work, original_state in rows:
        if work['request_url'] in completed_urls:
            continue
        # A slow metadata seek can consume the slice before this lane runs.
        # Like the bounded scanners, permit one ready item per positive slice;
        # otherwise a busy scan can starve validated local originals forever.
        if (max_seconds <= 0 or stop is not None and stop.is_set()
                or done > 0 and time.monotonic() - started >= max_seconds):
            break
        source = queue.registry.sources.get('cnyes')
        if not source or not source.allowed or not source.cnyes_apis:
            break
        if wordpress_base(work['request_url']) not in source.cnyes_apis:
            _defer_retained_work(queue, work, original_state, 'source_endpoint_unavailable')
            source_scope_deferrals += 1
            done += 1
            continue
        sha = work['response_sha256']
        state, outcome = 'retained_unavailable', {'source_bodies_reaccepted': False, 'publisher_requests': 0}
        proof = queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=? '
            'AND source_bulk_fingerprint=? AND authorization_fingerprint=? AND evidence_contract_version=?',
            (work['scope_url'], cnyes_bulk_fingerprint(source), source.authorization_fingerprint, CONTRACT_VERSION)).fetchone()
        current = queue.db.execute('SELECT 1 FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
            'ON o.observation_sha256=h.observation_sha256 WHERE h.scope_url=? AND h.page=? '
            'AND o.evidence_contract_version=? AND o.source_bulk_fingerprint=? AND o.authorization_fingerprint=?',
            (work['scope_url'], work['page'], CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
        if proof and proof[0] in COMPLETE_STATES or current:
            candidate_wake = _is_candidate_wake(queue, work)
            if candidate_wake:
                # A prior failure was reawakened by a local candidate. A
                # scope head alone cannot certify this exact failed request:
                # validate the observation and preserve the failure receipt.
                with queue.db:
                    resolve_forward_retained_outcomes(queue, work['scope_url'], work['page'], source)
                    resolved = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                                                (work['request_url'],)).fetchone()
                    if resolved and resolved[0] == 'forward_evidence_available':
                        _release_complete_local_inventory_fence(queue, work['scope_url'], source)
                if resolved and resolved[0] == 'forward_evidence_available':
                    completed_urls.add(work['request_url'])
                    done += 1
                    continue
            else:
                observation_sha, evidence_kind = (_matching_forward_evidence(queue, work, source, allow_shared=True)
                                                  if current else (None, None))
                partition_sha = (_verified_partition_scope_proof(queue, work['scope_url'], source, stop=stop)
                                 if not observation_sha and proof and proof[0] == 'partition_list_complete'
                                 and _retained_scope_identity_matches(work, source) else None)
                if stop is not None and stop.is_set():
                    break
                if observation_sha or partition_sha:
                    evidence_kind = evidence_kind if observation_sha else 'partition_scope_proof'
                    # A verified partition covers its entire scope. Resolve
                    # other work already selected in this bounded slice once,
                    # while candidate-woken failures still need direct proof.
                    group = ([w for w, _ in rows if w['scope_url'] == work['scope_url']
                              and w['request_url'] not in completed_urls and _retained_scope_identity_matches(w, source)
                              and not _is_candidate_wake(queue, w)]
                             if partition_sha else [work])
                    with queue.db:
                        for item in group:
                            if stop is not None and stop.is_set():
                                break
                            item_outcome = {**outcome, 'evidence_kind': evidence_kind,
                                'observation_sha256' if observation_sha else 'scope_proof_sha256': observation_sha or partition_sha}
                            _save_retained_resolution(queue, item, 'forward_evidence_available', observation_sha, evidence_kind)
                            queue.db.execute('UPDATE cnyes_history_retained_work SET outcome_json=? WHERE request_url=?',
                                             (encoded(item_outcome), item['request_url']))
                            queue.db.execute('DELETE FROM cnyes_history_retained_local_retries WHERE request_url=?',
                                             (item['request_url'],))
                            completed_urls.add(item['request_url'])
                            done += 1
                        _release_complete_local_inventory_fence(queue, work['scope_url'], source)
                    continue
        local_error, errors = None, []
        try:
            candidates = [(path, path.with_suffix('.parquet.receipt.json'), kind, None)
                          for path, kind in _retained_envelopes(
                              queue.root, sha, work['request_url'], source.authorization_fingerprint)]
        except (OSError, MemoryError) as error:
            code = 'local_memory_unavailable' if isinstance(error, MemoryError) else 'local_io_unavailable'
            _defer_retained_work(queue, work, original_state, code)
            local_io_deferrals += 1
            done += 1
            continue
        candidates.extend((row['parquet_path'], row['receipt_path'], 'accepted_body_batch', row['parquet_path'])
            for row in queue.db.execute('SELECT parquet_path,receipt_path FROM cnyes_history_accepted_batch_candidates '
                'WHERE request_url=? AND response_sha256=? ORDER BY parquet_path', (work['request_url'], sha)))
        if not candidates:
            errors.append('invalid_retained_response_sha256')
        for path, receipt_path, artifact_kind, accepted_relative in candidates:
            try:
                if accepted_relative is not None:
                    from downloader.news_fulltext_queue import replay_source_path
                    accepted = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?',
                                                (accepted_relative,)).fetchone()
                    if not accepted or accepted[0] != receipt_path:
                        raise ValueError('accepted_batch_not_committed')
                    path = replay_source_path(queue.root, accepted_relative)
                    receipt_path = replay_source_path(queue.root, receipt_path)
                    before = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)
                    try:
                        receipt = json.loads(receipt_path.read_bytes())
                        batch = pq.ParquetFile(path)
                        if (type(receipt.get('contract_version')) is not int
                                or receipt['contract_version'] != NEWS_CONTRACT_VERSION
                                or receipt.get('parquet_path') != accepted_relative or receipt.get('publish') is not False
                                or type(receipt.get('rows')) is not int or receipt['rows'] != batch.metadata.num_rows
                                or sha256_file(path) != receipt.get('sha256')):
                            raise ValueError('invalid_accepted_batch_receipt')
                        columns = ['provider', 'source_request_url', 'raw_response_body', 'response_sha256',
                                   'captured_at_utc', 'authorization_fingerprint', 'publish']
                        if not set(columns) <= set(batch.schema_arrow.names):
                            raise ValueError('accepted_batch_original_metadata_missing')
                        original = None
                        for chunk in batch.iter_batches(batch_size=16, columns=columns, use_threads=False):
                            original = next((row for row in chunk.to_pylist() if row.get('provider') == source.provider
                                and row.get('source_request_url') == work['request_url']
                                and row.get('raw_response_body') is not None and row.get('response_sha256') == sha), None)
                            if original is not None:
                                break
                        if original is None:
                            raise ValueError('accepted_batch_matching_original_missing')
                        original = {**original, 'request_url': original['source_request_url']}
                    finally:
                        try:
                            after = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)
                        except FileNotFoundError as error:
                            raise OSError('retained original disappeared during verification') from error
                        if before != after:
                            raise OSError('retained original changed during verification')
                else:
                    if not path.is_file() or not receipt_path.is_file():
                        continue
                    before = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)
                    try:
                        verified = _read_original_envelope_identity(path, receipt_path, sha,
                            expected_parquet_path=str(path.relative_to(queue.root)), expected_kind=artifact_kind,
                            include_original=True)
                    finally:
                        if before != (_original_envelope_file_signature(path),
                                      _original_envelope_file_signature(receipt_path)):
                            raise OSError('retained original changed during verification')
                    if verified is None:
                        raise ValueError('invalid_original_envelope_receipt')
                    original = verified[4]
                raw = original['raw_response_body']
                raw = raw.encode() if isinstance(raw, str) else raw
                if original['request_url'] != work['request_url']:
                    raise ValueError('retained_original_request_differs_not_relabelled')
                if hashlib.sha256(raw).hexdigest() != sha or original.get('publish') is not False:
                    raise ValueError('retained_original_bytes_or_private_scope_invalid')
                if original['authorization_fingerprint'] != source.authorization_fingerprint:
                    raise ValueError('retained_original_authorization_mismatch')
                captured = datetime.fromisoformat(original['captured_at_utc'])
                if (captured.tzinfo is None or captured.utcoffset() is None
                        or captured > datetime.now(timezone.utc)):
                    raise ValueError('retained_original_capture_clock_invalid')
                response = SimpleNamespace(url=original['request_url'], body=raw,
                                           headers={'content-type': 'application/json'})
                info, posts = cnyes_pagination(response)
                if any(type(post.get('payment')) is not int or post['payment'] != 0 for post in posts):
                    raise ValueError('restricted_retained_envelope_not_reused')
                if (info['total'] != work['expected_total'] or info['ids_sha256'] != work['expected_ids_sha256']
                        or info['page'] != work['page'] or info['scope'] != work['scope_url']):
                    raise ValueError('retained_source_page_identity_mismatch')
                if before != (_original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)):
                    raise OSError('retained original changed before recording')
                with queue.db:
                    record_page(queue, response, info, posts, source, {
                        'kind': artifact_kind, 'parquet_path': accepted_relative or str(path.relative_to(queue.root)),
                        'receipt_path': accepted[0] if accepted_relative is not None else str(receipt_path.relative_to(queue.root))},
                        observed_at=original['captured_at_utc'], origin='verified_retained_source_response')
                    if before != (_original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)):
                        raise OSError('retained original changed while recording')
                state, outcome['response_sha256'] = 'restored', sha
                break
            except FileNotFoundError as error:
                if accepted_relative is not None:
                    # The file can disappear after its initial inventory.
                    # Retain its registered receipt identity and retry it
                    # independently without resetting a publisher deadline.
                    marker = accepted_batch_scan_key(source)
                    with queue.db:
                        existing = queue.db.execute('SELECT not_before_epoch FROM cnyes_history_accepted_missing_retries '
                            'WHERE scan_key=? AND parquet_path=?', (marker, accepted_relative)).fetchone()
                        if existing:
                            deadline = existing[0]
                        else:
                            deadline = _defer_missing_accepted_batch(queue, marker,
                                {'path': accepted_relative, 'receipt_path': accepted[0]})
                            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                                (accepted_relative, accepted[0], 'unavailable',
                                 'local_batch_or_receipt_missing_during_validation', now()))
                    previous = missing_batches['next_retry_epoch']
                    missing_batches['next_retry_epoch'] = deadline if previous is None else min(previous, deadline)
                    errors.append('accepted_batch_file_missing')
                else:
                    # The candidate existed at dispatch; losing it during
                    # verification is local I/O, not a publisher failure.
                    local_error = error
            except (OSError, MemoryError) as error:
                local_error = error
            except (ValueError, KeyError, TypeError, OverflowError, NewsError, pa.ArrowException) as error:
                errors.append(str(error))
        if state != 'restored' and local_error is not None:
            code = 'local_memory_unavailable' if isinstance(local_error, MemoryError) else 'local_io_unavailable'
            _defer_retained_work(queue, work, original_state, code)
            local_io_deferrals += 1
            done += 1
            continue
        if state != 'restored' and _is_unverified_original_repair(queue, work):
            # A candidate may disappear or fail validation after the audit.
            # It must not convert an unavailable local original into a new
            # publisher request, nor erase any existing source cooldown.
            _defer_retained_work(queue, work, original_state, 'original_local_candidate_unavailable',
                                 candidate_errors=errors)
            done += 1
            continue
        if (state != 'restored' and not accepted_scan['complete'] and work['expected_total'] > 0
                and _retained_envelopes(queue.root, sha)):
            _defer_retained_work(queue, work, original_state, 'accepted_batch_scan_pending',
                                 candidate_errors=errors)
            done += 1
            continue
        if state != 'restored' and errors:
            # Identical bytes do not make two publisher requests identical.
            mismatch = 'retained_original_request_differs_not_relabelled'
            state = 'retained_request_mismatch' if all(error == mismatch for error in errors) else 'retained_invalid'
            outcome['error'] = errors[0] if state == 'retained_request_mismatch' else next(
                error for error in errors if error != mismatch)
            if len(errors) > 1:
                outcome['candidate_errors'] = errors
        # A missing raw envelope is not empty data. Schedule one canonical
        # successful page check for this effective date/page; preserve failed
        # jobs, source/endpoint cooldowns, leases, old hashes and real clocks.
        with queue.db:
            if (state in ('retained_unavailable', 'retained_invalid', 'retained_request_mismatch')
                    and original_state != 'overflow_pending'
                    and not queue.db.execute('SELECT 1 FROM cnyes_history_local_inventory_scopes WHERE scope_url=?',
                                             (work['scope_url'],)).fetchone()):
                key = scope_key(work['request_url'])
                recheck_marker = 'cnyes_history_evidence_recheck_v1:' + digest((*key, work['page']))
                if not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (recheck_marker,)).fetchone():
                    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (work['request_url'],)).fetchone()
                    if (job and job['error_code'] is None and job['attempts'] == 0
                            and job['superseded_by_windows_json'] is None):
                        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (recheck_marker, encoded({
                            'prior_job': dict(job), 'reason': 'missing_ID_and_publication_day_evidence',
                            'changed_at_utc': now(), 'source_failures_or_deadlines_reset': False})))
                        queue.db.execute('UPDATE discovery SET due=min(due,?),etag=NULL,last_modified=NULL WHERE url=?',
                                         (time.time(), work['request_url']))
                        outcome['one_successful_page_evidence_recheck_scheduled'] = True
            queue.db.execute('UPDATE cnyes_history_retained_work SET state=?,outcome_json=? WHERE request_url=?',
                             (state, encoded(outcome), work['request_url']))
            if state == 'restored':
                queue.db.execute('DELETE FROM queue_settings WHERE key=?',
                                 (_original_local_repair_key(work['request_url'], work['response_sha256']),))
            queue.db.execute('DELETE FROM cnyes_history_retained_local_retries WHERE request_url=?',
                             (work['request_url'],))
            _release_complete_local_inventory_fence(queue, work['scope_url'], source)
            _reconsider_unverified_original_after_work(queue, source, work['scope_url'], work['page'],
                                                       finished_work=work)
        done += 1
    # Cursor movement alone does not invalidate a completed sweep. Changes to
    # candidate work or scope evidence can make a previously fenced scope
    # repairable and must trigger another pass immediately.
    repair_changed_before = bool(done or legacy.get('reclassified', 0) or unavailable.get('resolved', 0)
        or empty.get('reconciled', 0) or accepted_scan.get('indexed', 0) or missing_batches.get('indexed', 0)
        or invalid_batches.get('indexed', 0) or nonzero.get('queued', 0)
        or nonzero.get('stale_fences_reconciled', 0) or scan.get('queued', 0))
    if repair_changed_before:
        repair_covered = False
        queue._cnyes_history_repair_covered = None
        queue._cnyes_history_repair_resweep = True
    else:
        # save_proof may have invalidated the cache while the local work ran.
        repair_covered = bool(repair_covered and getattr(queue, '_cnyes_history_repair_covered', None) == covered)
    repair_budget = max(0, max_seconds - (time.monotonic() - started))
    if repair is None:
        repair = ({'scopes_checked': 0, 'retained_pages_queued': 0, 'page_checks_scheduled': 0}
                  if repair_covered else repair_incomplete_scopes(queue, stop=stop, limit=limit, max_seconds=repair_budget))
    # Existing retained/source-recovery work gets the first opportunity on
    # every owner turn. The periodic audit still gets its own bounded slice,
    # so a continuously busy retained lane cannot starve it indefinitely.
    integrity = sweep_completed_scopes(queue, stop=stop, limit=limit, max_seconds=max_seconds / 8)
    if defer_original_audit:
        # The owner is still registering a finite, pinned priority plan. A
        # general audit could replace an unregistered target's current head
        # first, causing its successful recovery to be reported as stale.
        # Other local lanes and publisher dispatch continue as usual.
        original_audit = {'complete': False, 'checked': 0, 'mismatched': 0, 'unverified': 0,
                          'equal_day_page_ID_recoveries': 0, 'next_epoch': None, 'blocked': None,
                          'deferred_for_priority': True}
    else:
        original_audit = audit_original_envelopes(queue, stop=stop, limit=limit, max_seconds=max_seconds / 8)
    repair_pending = bool(queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone())
    repair_pending = repair_pending or bool(repair.get('unfinished_slice'))
    repair_changed_after = bool(integrity.get('invalid', 0) or original_audit.get('mismatched', 0)
                                or original_audit.get('unverified', 0)
                                or repair_first and repair_changed_before)
    if repair_changed_after:
        repair_covered = False
        queue._cnyes_history_repair_covered = None
        queue._cnyes_history_repair_resweep = True
    if not repair_pending and not repair_first and repair_budget <= 0 and source_available and not repair_covered:
        # The slice never started and therefore has no cursor. A bounded
        # index seek distinguishes real pending work from an idle owner.
        repair_pending = bool(queue.db.execute('SELECT 1 FROM cnyes_history_scope_heads WHERE provider=? '
            'AND evidence_contract_version=? AND source_bulk_fingerprint=? AND authorization_fingerprint=? '
            f"AND state IN ({','.join('?' for _ in REPAIR_STATES)}) LIMIT 1",
            (source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
             *REPAIR_STATES)).fetchone())
    repair_attempted = bool(repair_first or repair_budget > 0 and not repair_covered)
    repair_resweep_pending = bool(repair_attempted and not repair_pending
                                  and getattr(queue, '_cnyes_history_repair_resweep', False))
    if repair_resweep_pending:
        # Evidence changed after the sweep passed its scope. Finish the
        # current cursor before starting again, so busy inputs cannot starve
        # later scopes or leave a newly ready earlier scope behind.
        repair_pending = True
    if (repair_attempted and not repair_pending and not repair_changed_after and source_available
            and limit > 0 and max_seconds > 0 and not (stop is not None and stop.is_set())):
        # Match the existing idle 300-second recheck cadence, while new proof
        # or local work invalidates this in-memory optimization immediately.
        queue._cnyes_history_repair_covered = (repair_signature, time.monotonic() + 300)
    if limit > 0 and max_seconds > 0 and not (stop is not None and stop.is_set()):
        queue._cnyes_history_repair_priority = bool(source_available and repair_pending
            and (repair_resweep_pending or repair.get('scopes_checked', 0) == 0))
    repair_priority_pending = bool(getattr(queue, '_cnyes_history_repair_priority', False)
                                   and source_available and limit > 0 and max_seconds > 0
                                   and not (stop is not None and stop.is_set()))
    nonzero_blocked = nonzero.get('blocked') in ('unsupported_nonzero_scan_checkpoint_version',
                                                 'nonzero_checkpoint_audit_conflict')
    # Work behind a nonzero-scan fence cannot run while its checkpoint is
    # blocked. Do not mistake it for eligible work and spin every two seconds;
    # keep unrelated ready lanes and retry deadlines visible.
    if nonzero_blocked:
        pending = queue.db.execute('SELECT 1 FROM cnyes_history_retained_work w '
            'LEFT JOIN cnyes_history_local_inventory_scopes i ON i.scope_url=w.scope_url '
            "WHERE w.state='pending' AND (i.scope_url IS NULL OR i.scan_state='ready') LIMIT 1").fetchone()
    else:
        pending = queue.db.execute("SELECT 1 FROM cnyes_history_retained_work WHERE state='pending' LIMIT 1").fetchone()
    if scan['complete'] and not pending:
        pending = queue.db.execute("SELECT 1 FROM cnyes_history_retained_work WHERE state='overflow_pending' LIMIT 1").fetchone()
    scope_join = ('LEFT JOIN cnyes_history_local_inventory_scopes i ON i.scope_url=w.scope_url '
                  if nonzero_blocked else '')
    scope_filter = " AND (i.scope_url IS NULL OR i.scan_state='ready')" if nonzero_blocked else ''
    if accepted_scan['complete'] and not pending:
        pending = queue.db.execute('SELECT 1 FROM cnyes_history_retained_local_retries r '
            "JOIN cnyes_history_retained_work w ON w.request_url=r.request_url AND w.state='local_retry' "
            + scope_join + "WHERE r.error_code='accepted_batch_scan_pending'" + retry_filter + scope_filter + ' LIMIT 1').fetchone()
    next_retry = queue.db.execute('SELECT min(r.not_before_epoch) FROM cnyes_history_retained_local_retries r '
        'CROSS JOIN cnyes_history_retained_work w ON w.request_url=r.request_url AND w.state=\'local_retry\' '
        + scope_join + "WHERE r.error_code!='accepted_batch_scan_pending'" + retry_filter + scope_filter).fetchone()[0]
    accepted_retry = accepted_scan.get('next_retry_epoch')
    if accepted_retry is not None:
        next_retry = min(next_retry, accepted_retry) if next_retry is not None else accepted_retry
    missing_retry = missing_batches['next_retry_epoch']
    if missing_retry is not None:
        next_retry = min(next_retry, missing_retry) if next_retry is not None else missing_retry
    invalid_retry = invalid_batches['next_retry_epoch']
    if invalid_retry is not None:
        next_retry = min(next_retry, invalid_retry) if next_retry is not None else invalid_retry
    integrity_retry = integrity.get('next_epoch')
    if integrity_retry is not None:
        next_retry = min(next_retry, integrity_retry) if next_retry is not None else integrity_retry
    original_retry = original_audit.get('next_epoch')
    if original_retry is not None:
        next_retry = min(next_retry, original_retry) if next_retry is not None else original_retry
    ready_retry = next_retry is not None and next_retry <= time.time()
    exhausted = (not source_available or
                 (not pending and not ready_retry and not repair_pending
                  and (scan['complete'] or scan['blocked'] in ('unsupported_overflow_scan_checkpoint_version',
                                                               'overflow_checkpoint_audit_conflict'))
                  and all(item['complete'] or item['blocked'] in
                          ('unsupported_legacy_checkpoint_version', 'legacy_checkpoint_audit_conflict')
                          for item in (legacy, unavailable, empty))
                  and (accepted_scan['complete'] or accepted_scan['blocked'] in
                       ('local_retry_wait', 'unsupported_accepted_batch_checkpoint_version',
                        'accepted_batch_checkpoint_audit_conflict'))
                  and invalid_batches['migration']['complete']
                  and (nonzero['complete'] or nonzero_blocked)
                  and integrity['complete'] and (original_audit['complete'] or original_audit['blocked'] in
                      ('unsupported_original_audit_checkpoint_version', 'original_audit_checkpoint_audit_conflict'))))
    return {'processed': done, 'exhausted': exhausted, 'evidence_repair': repair,
            'repair_sweep_pending': repair_pending, 'repair_priority_pending': repair_priority_pending,
            'overflow_retained_scan': scan, 'legacy_mismatch_reconciliation': legacy,
            'legacy_unavailable_reconciliation': unavailable, 'unobserved_empty_scope_scan': empty,
            'accepted_body_batch_scan': accepted_scan, 'unobserved_nonzero_page_scan': nonzero,
            'completed_integrity_scan': integrity,
            'original_envelope_audit': original_audit,
            'accepted_missing_batch_retries': missing_batches,
            'accepted_invalid_batch_retries': invalid_batches,
            'local_io_deferrals': local_io_deferrals,
            'source_scope_deferrals': source_scope_deferrals,
            'next_local_retry_epoch': next_retry if source_available else None,
            'wall_seconds': time.monotonic() - started}
