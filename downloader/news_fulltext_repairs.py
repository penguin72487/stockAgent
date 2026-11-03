"""Reviewed failed-body work, admitted by the existing single queue writer.

Submission is read-only SQLite plus a private, immutable local request: no
queue lease, HTTP client or worker lifecycle. The resident owner rechecks the
exact failed head and current source contract before changing only its state.
SQL admission and its once-per-parser marker commit together; a crash before
archiving the request cannot grant a second attempt. Admission is not capture.
ABI4 also admits exactly witnessed legacy zero-byte saved-body rechecks;
ordinary missing-only requests still cannot alter a retained body head.
ABI5 admits one missing-only source recovery after an immutable successful
current native observation; old parser-repair and question markers stay intact.
ABI6 permits one explicit missing-only address-validation recheck per URL,
independent of parser generations. It grants no permission or DNS exemption.
ABI7 permits one reviewed missing-only publication-clock recheck per parser.
The current parser still owns clock validation; no timestamp is rewritten.
ABI10 classifies an exact paid-preview footer from sealed prior native geometry;
only state/error change, without new source checks, bodies or retries.
"""
from __future__ import annotations

from collections import Counter, OrderedDict
from contextlib import closing
from datetime import datetime
import errno
import hashlib
from itertools import islice
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from urllib.parse import urlsplit

import pyarrow as pa

from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext_policy import NewsError, normalize_url
from downloader.news_fulltext_queue import parser_repair_identity, utc_now
from downloader.news_fulltext_native_rechecks import (
    VERSION as NATIVE_SUCCESS_VERSION, KIND as NATIVE_SUCCESS_KIND, OUTCOME as NATIVE_SUCCESS_OUTCOME,
    validate_reference as validate_native_success_reference, verify as verify_native_success,
)

CONTRACT_VERSION = 2
EMPTY_RESPONSE_RECHECK_VERSION = 4
EMPTY_RESPONSE_RECHECK_KIND = 'reviewed_stored_empty_response_recheck'
ADDRESS_RECHECK_VERSION = 6
ADDRESS_RECHECK_KIND = 'reviewed_address_validation_recheck'
ADDRESS_RECHECK_OUTCOME = 'address_validation_recheck_enqueued'
ADDRESS_RECHECK_STATES = frozenset({'non_public_address'})
PUBLICATION_CLOCK_RECHECK_VERSION = 7
PUBLICATION_CLOCK_RECHECK_KIND = 'reviewed_publication_clock_recheck'
PUBLICATION_CLOCK_RECHECK_OUTCOME = 'publication_clock_recheck_enqueued'
PUBLICATION_CLOCK_RECHECK_STATES = frozenset({'invalid_bulk_publication_clock'})
ADMITTED_OUTCOMES = frozenset({'enqueued', 'revalidation_enqueued', NATIVE_SUCCESS_OUTCOME, 'native_article_route_recheck_enqueued',
                              ADDRESS_RECHECK_OUTCOME, PUBLICATION_CLOCK_RECHECK_OUTCOME})
CLASSIFIED_NONTEXT_OUTCOMES = frozenset({'classified_source_body_image_only',
                                       'classified_source_article_empty_template'})
CLASSIFIED_SOURCE_OUTCOMES = CLASSIFIED_NONTEXT_OUTCOMES | {'classified_paywall_or_login'}
STORED_HEAD_FIELDS = ('body_sha256', 'body_bytes', 'parquet_path', 'captured_at_utc',
                      'etag', 'last_modified', 'validator_url')
MAX_TARGETS = 512
MAX_PENDING = 64
MAX_REQUEST_BYTES = 1 << 20
PUMP_INTERVAL_SECONDS = 5
CONTINUATION_INTERVAL_SECONDS = 0.25
MAX_VALIDATED_REQUESTS = 16
MAX_VALIDATED_REQUEST_BYTES = 4 << 20
ROUND_ROBIN_CURSOR_KEY = 'reviewed_repairs:last_considered_request_v1'
_REQUEST_NAME = re.compile(r'[0-9a-f]{64}[.]json')
REPAIRABLE = frozenset({"body_not_found", "article_identity_not_verified",
                       "article_attribution_not_verified", "article_title_not_verified"})
_REVIEWED_SCOPE_FAILURE = "article_rights_or_scope_excluded"
# The admitted v6 pen-name scope superseded these older parser observations.
# A failure observed under v6/v7 must not get a second attempt via this repair.
_MITRADE_PRIOR_SCOPE_CUTOFF = "2026-10-20T23:21:29.541919+00:00"
HEAD_FIELDS = ("id", "url", "provider", "state", "error_code", "attempts", "due", "last_checked_at_utc")
SOURCE_FIELDS = {"contract_version", "data_fingerprint", "parser_fingerprint", "authorization_fingerprint"}


def _bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _clock(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise ValueError("invalid_repair_clock")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("naive_repair_clock")
    return stamp.timestamp()


def _head(row):
    return {key: bytes(row[key]).hex() if key == "id" else row[key] for key in HEAD_FIELDS}


def _reviewed_prior_scope_failure(row, registry):
    """One explicit missing-only retry under a pinned native-role correction.

    This is NOT a generic rights override. Pinned Mitrade pen-name and
    StockFeel author/source corrections can reconsider old native parser
    exclusions once. The current strict parser still rejects other publishers,
    reprints and unproved identities. No body, authorization, HTTP refusal,
    deadline, attempt count or failure budget is cleared here.
    """
    if registry is None or row["state"] != _REVIEWED_SCOPE_FAILURE:
        return False
    source = registry.sources.get(row["provider"])
    if source is not None and source.provider == 'contentplatform':
        from downloader.news_fulltext_contentplatform import (
            CONTENTPLATFORM_V4_HTTP_BINDING, CONTENTPLATFORM_V4_SCOPE_CUTOFF,
            contentplatform_article_id, contentplatform_known_native_source,
        )
        try:
            return bool(len(CONTENTPLATFORM_V4_HTTP_BINDING) == 5
                and contentplatform_known_native_source(source)
                and (source.data_fingerprint, source.authorization_fingerprint) == CONTENTPLATFORM_V4_HTTP_BINDING[:2]
                and contentplatform_article_id(row['url']) is not None
                and _clock(row['last_checked_at_utc']) < _clock(CONTENTPLATFORM_V4_SCOPE_CUTOFF))
        except (ValueError, TypeError, OverflowError):
            return False
    if source is not None and source.provider == 'managertoday':
        from downloader.news_fulltext_managertoday import (
            MANAGER_ORIGIN, MANAGER_V1_SCOPE_CUTOFF, manager_metadata_source)
        try:
            return bool(manager_metadata_source(source)
                and source.embedded_article_layout in {'manager_public_written_article_v2', 'manager_public_written_article_v3', 'manager_public_written_article_v4', 'manager_public_written_article_v5'}
                and re.fullmatch(re.escape(MANAGER_ORIGIN) + r'/articles/view/[1-9][0-9]*', row['url'])
                and _clock(row['last_checked_at_utc']) < _clock(MANAGER_V1_SCOPE_CUTOFF))
        except (ValueError, TypeError, OverflowError):
            return False
    if source is not None and source.provider == 'stockfeel':
        from downloader.news_fulltext_stockfeel import (
            STOCKFEEL_V2_REPAIR_COHORT, STOCKFEEL_V2_PRIOR_SCOPE_CUTOFF, stockfeel_role_repair_source,
            STOCKFEEL_V3_PRIOR_SCOPE_CUTOFF, stockfeel_url_key, stockfeel_writer_repair_source,
        )
        try:
            if stockfeel_writer_repair_source(source):
                return bool(stockfeel_url_key(row['url']) is not None
                    and _clock(row['last_checked_at_utc']) < _clock(STOCKFEEL_V3_PRIOR_SCOPE_CUTOFF))
            return bool(stockfeel_role_repair_source(source) and row['url'] in STOCKFEEL_V2_REPAIR_COHORT
                and _clock(row['last_checked_at_utc']) < _clock(STOCKFEEL_V2_PRIOR_SCOPE_CUTOFF))
        except (ValueError, TypeError, OverflowError):
            return False
    from downloader.news_fulltext_mitrade import MITRADE_V7_SCOPE_REPAIR_IDENTITY, mitrade_article_id
    if (source is None or source.provider != "mitrade" or not source.allowed or not source.known_url_only
            or source.embedded_article_layout != "mitrade_visible_article_v7"
            or source.data_fingerprint != MITRADE_V7_SCOPE_REPAIR_IDENTITY[1]
            or mitrade_article_id(row["url"]) is None):
        return False
    try:
        return _clock(row["last_checked_at_utc"]) < _clock(_MITRADE_PRIOR_SCOPE_CUTOFF)
    except (ValueError, TypeError, OverflowError):
        return False


def _head_problem(row, now, *, registry=None, allowed_failure_states=REPAIRABLE, stored_recheck=False):
    if row is None:
        return "unknown_url"
    if not stored_recheck and (row["body_sha256"] is not None or row["parquet_path"] is not None
            or row["body_bytes"] != 0 or row["captured_at_utc"] is not None):
        return "stored_head_protected"
    if stored_recheck and (not isinstance(row['body_sha256'], str)
            or re.fullmatch('[0-9a-f]{64}', row['body_sha256']) is None
            or type(row['body_bytes']) is not int or row['body_bytes'] <= 0
            or not isinstance(row['parquet_path'], str) or not row['parquet_path']
            or row['captured_at_utc'] is None):
        return 'stored_head_required'
    if row["lease_owner"] is not None or row["lease_until"] is not None:
        return "leased_head_protected"
    if (row["error_code"] != row["state"] or row["state"] not in allowed_failure_states
            and not _reviewed_prior_scope_failure(row, registry)):
        return "not_semantic_parser_failure"
    if type(row["attempts"]) is not int or row["attempts"] < 0:
        return "invalid_attempts"
    if type(row["due"]) not in (int, float) or not math.isfinite(row["due"]) or row["due"] < 0 or row["due"] > now:
        return "future_or_invalid_deadline"
    try:
        if _clock(row["last_checked_at_utc"]) > now:
            return "future_check_protected"
    except (ValueError, TypeError, OverflowError):
        return "missing_or_invalid_check"
    return None


def _empty_response_reference(reference):
    if (not isinstance(reference, dict) or set(reference) != {'receipt_path', 'receipt_sha256'}
            or not isinstance(reference['receipt_path'], str)
            or re.fullmatch(r'diagnostics/[0-9]{8}/cnyes/[0-9]{8}T[0-9]{12}Z-[0-9a-f]{8}[.]receipt[.]json',
                            reference['receipt_path']) is None
            or not isinstance(reference['receipt_sha256'], str)
            or re.fullmatch('[0-9a-f]{64}', reference['receipt_sha256']) is None):
        raise ValueError('invalid_empty_response_reference')


def _verify_empty_response(root, source, row, reference):
    """Reuse a canonical zero-byte witness, never an inferred missing article.

    Restrict legacy repair to the observed continuous Cnyes native scope.
    Read the small immutable receipt/Parquet pair, bind the exact current
    failed observation and saved head, and preserve the source's auth/clock.
    Nothing here requests a source, parses an article or declares it fresh.
    """
    import pyarrow.parquet as pq
    from downloader.artifact_io import sha256_file
    from downloader.news_fulltext_queue import source_path

    _empty_response_reference(reference)
    parts = urlsplit(row['url'])
    if (source.provider != 'cnyes' or not source.allowed or source.acquisition_mode == 'finite'
            or parts.scheme != 'https' or parts.netloc not in {'news.cnyes.com', 'm.cnyes.com'}
            or parts.query or parts.fragment or re.fullmatch(r'/news/id/[1-9][0-9]*', parts.path) is None
            or row['state'] not in {'body_not_found', 'article_identity_not_verified'}):
        raise ValueError('unsupported_empty_response_recheck_scope')
    if not (root / reference['receipt_path']).exists():
        raise FileNotFoundError('empty_response_receipt_temporarily_unavailable')
    receipt_path = source_path(root, reference['receipt_path'])
    if receipt_path.stat().st_size > 65536 or sha256_file(receipt_path) != reference['receipt_sha256']:
        raise ValueError('empty_response_receipt_digest_mismatch')
    receipt = json.loads(receipt_path.read_bytes(), object_pairs_hook=_unique)
    if not isinstance(receipt, dict):
        raise ValueError('invalid_empty_response_receipt')
    relative = reference['receipt_path'].removesuffix('.receipt.json') + '.parquet'
    if not (root / relative).exists():
        raise FileNotFoundError('empty_response_parquet_temporarily_unavailable')
    path = source_path(root, relative)
    if (path.stat().st_size > 256 << 10 or receipt.get('path') != relative
            or sha256_file(path) != receipt.get('sha256')):
        raise ValueError('empty_response_parquet_digest_mismatch')
    parquet = pq.ParquetFile(path)
    if (parquet.metadata.num_rows != 1 or parquet.metadata.num_row_groups != 1
            or parquet.metadata.row_group(0).total_byte_size > 256 << 10):
        raise ValueError('unbounded_empty_response_witness')
    record, = parquet.read(use_threads=False).to_pylist()
    if (any(record.get(key) != value for key, value in receipt.items() if key not in {'path', 'sha256'})
            or record.get('raw_response_body') is not None
            or receipt.get('provider') != source.provider or receipt.get('request_url') != row['url']
            or receipt.get('final_url') != row['url'] or type(receipt.get('status')) is not int
            or receipt['status'] != 200 or type(receipt.get('response_bytes')) is not int
            or receipt['response_bytes'] != 0 or receipt.get('response_sha256') != hashlib.sha256(b'').hexdigest()
            or receipt.get('error_code') != row['error_code'] or row['attempts'] != 1
            or receipt.get('data_fingerprint') != source.data_fingerprint
            or receipt.get('diagnostic_only') is not True or receipt.get('accepted_as_article') is not False
            or receipt.get('raw_response_retained') is not False or receipt.get('publish') is not False
            or receipt.get('remote_training_export') is not False
            or json.loads(receipt.get('authorization_json', 'null')) != source.authorization):
        raise ValueError('empty_response_observation_not_verified')
    headers = json.loads(receipt.get('headers_json', 'null'))
    observed, checked = _clock(receipt['captured_at_utc']), _clock(row['last_checked_at_utc'])
    if (not isinstance(headers, dict) or not isinstance(headers.get('content-type'), str)
            or headers['content-type'].split(';', 1)[0].strip().lower() not in {'text/html', 'application/xhtml+xml'}
            or not 0 <= checked - observed <= 1 or checked > time.time()
            or _clock(row['captured_at_utc']) > observed):
        raise ValueError('stale_or_invalid_empty_response_witness')


def _source(db, registry, url):
    source = registry.require(url)
    if source.article_path_pattern and not re.search(source.article_path_pattern, urlsplit(url).path):
        raise ValueError("unsupported_article_path")
    return _source_contract(db, source, url)


def _source_contract(db, source, url):
    """Attest source identity/cohort; callers still own their route purpose.

    Article repair always uses ``_source`` above. A finite metadata question
    can attest a known non-article head without expanding article permission.
    """
    if source.acquisition_mode == "finite":
        original = db.execute("SELECT 1 FROM finite_cohort_members WHERE provider=? AND url=?",
                              (source.provider, url)).fetchone()
        supplement = source.allow_reviewed_supplements and db.execute(
            "SELECT 1 FROM finite_supplement_members WHERE provider=? AND url=?", (source.provider, url)).fetchone()
        if not original and not supplement:
            raise ValueError("outside_frozen_cohort")
    identity = parser_repair_identity(source)
    if identity is None:
        raise ValueError("parser_generation_unattested")
    return source, dict(identity, authorization_fingerprint=source.authorization_fingerprint)


def _directory(root, *parts):
    target = root
    for part in ("reviewed_repairs", *parts):
        target = target / part
        if target.is_symlink():
            raise ValueError("repair_directory_symlink")
        target.mkdir(mode=0o700, exist_ok=True)
        if not target.is_dir() or target.stat().st_mode & 0o077:
            raise ValueError("repair_directory_not_private")
    return target


def _write_once(path, payload):
    if path.is_symlink():
        raise ValueError("repair_receipt_symlink")
    if path.exists():
        if path.stat().st_size > MAX_REQUEST_BYTES or path.read_bytes() != payload:
            raise ValueError("repair_evidence_conflict")
        return
    atomic_write_bytes(path, payload, durable=True, mode=0o600)


def _pending(directory):
    # Bounded directory discovery, not a scan of the article or history trees.
    with os.scandir(directory) as entries:
        return sorted((Path(entry.path) for entry in islice(
            (entry for entry in entries if entry.name.endswith(".json")), MAX_PENDING + 1)), key=lambda p: p.name)


def _rotation_order(queue, paths, *, cursor_key=ROUND_ROBIN_CURSOR_KEY, cursor_table='queue_settings'):
    """Continue after the previous request, including across owner restarts.

    Only bounded private mailbox ordering changes. A saved cursor is never
    used as a path, source permission, article identity or admission proof.
    """
    if cursor_table not in {'queue_settings', 'native_candidate_schedule'}:
        raise ValueError('invalid_cursor_table')
    saved = queue.db.execute(f'SELECT value FROM {cursor_table} WHERE key=?',
                             (cursor_key,)).fetchone()
    if saved is None or not isinstance(saved[0], str) or not _REQUEST_NAME.fullmatch(saved[0]):
        return paths
    split = next((index for index, path in enumerate(paths) if path.name > saved[0]), len(paths))
    return paths[split:] + paths[:split]


def _record_turn(queue, path, stop, *, cursor_key=ROUND_ROBIN_CURSOR_KEY, cursor_table='queue_settings'):
    # Commit with the existing admission transaction/cursor. An interrupted
    # transaction rolls back both the once-only outcome and this scheduling
    # position; removed/archived paths still give an unambiguous next turn.
    if cursor_table not in {'queue_settings', 'native_candidate_schedule'}:
        raise ValueError('invalid_cursor_table')
    if _REQUEST_NAME.fullmatch(path.name) and not (stop is not None and stop.is_set()):
        queue.db.execute(f'INSERT INTO {cursor_table} VALUES (?,?) '
                         'ON CONFLICT(key) DO UPDATE SET value=excluded.value '
                         f'WHERE {cursor_table}.value IS NOT excluded.value',
                         (cursor_key, path.name))


def _already_admitted(db, provider, parser_fingerprint, article_id, outcome):
    # A parser change must not mint another security recheck for this URL.
    if outcome == ADDRESS_RECHECK_OUTCOME:
        return db.execute('SELECT 1 FROM reviewed_repair_items WHERE provider=? AND article_id=? AND outcome=?',
                          (provider, article_id, outcome)).fetchone() is not None
    return db.execute('SELECT 1 FROM reviewed_repair_items WHERE provider=? AND parser_fingerprint=? '
                      'AND article_id=? AND outcome=?',
                      (provider, parser_fingerprint, article_id, outcome)).fetchone() is not None


def submit_repairs(root, registry, urls, *, reason, retained_originals=None, empty_response_witnesses=None,
                   native_success_witnesses=None, address_recheck=False, publication_clock_recheck=False,
                   native_route_witnesses=None):
    """Seal explicit eligible failures while the canonical owner stays alive."""
    if native_route_witnesses is not None:
        from downloader.news_fulltext_route_rechecks import submit
        if address_recheck or publication_clock_recheck or any(v is not None for v in
                (retained_originals,empty_response_witnesses,native_success_witnesses)):
            raise ValueError('invalid_native_route_recheck_mode')
        return submit(root,registry,urls,reason=reason,witnesses=native_route_witnesses)
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 240:
        raise ValueError("repair_requires_bounded_review_reason")
    selected = list(islice(urls, MAX_TARGETS + 1))
    if not 1 <= len(selected) <= MAX_TARGETS:
        raise ValueError("repair_requires_one_to_512_urls")
    selected = list(dict.fromkeys(normalize_url(url) for url in selected))
    if type(address_recheck) is not bool or address_recheck and (publication_clock_recheck or any(
            value is not None for value in (retained_originals, empty_response_witnesses, native_success_witnesses))):
        raise ValueError('invalid_address_recheck_mode')
    if type(publication_clock_recheck) is not bool or publication_clock_recheck and any(
            value is not None for value in (retained_originals, empty_response_witnesses, native_success_witnesses)):
        raise ValueError('invalid_publication_clock_recheck_mode')
    native_recheck = native_success_witnesses is not None
    if native_recheck:
        if (retained_originals is not None or empty_response_witnesses is not None
                or not isinstance(native_success_witnesses, dict) or set(native_success_witnesses) != set(selected)):
            raise ValueError('invalid_native_success_targets')
        for reference in native_success_witnesses.values():
            validate_native_success_reference(reference)
    empty_recheck = empty_response_witnesses is not None
    if empty_recheck:
        if (retained_originals is not None or not isinstance(empty_response_witnesses, dict)
                or set(empty_response_witnesses) != set(selected)):
            raise ValueError('invalid_empty_response_targets')
        for reference in empty_response_witnesses.values():
            _empty_response_reference(reference)
    if retained_originals is not None:
        from downloader.news_fulltext_replay import validate_nontext_original_reference
        if not isinstance(retained_originals,dict) or set(retained_originals)!=set(selected):
            raise ValueError('invalid_retained_outcome_targets')
        for reference in retained_originals.values():
            validate_nontext_original_reference(reference)
    root, now = Path(root).resolve(), time.time()
    targets, sources, excluded, empty_responses = [], {}, [], {}
    with closing(sqlite3.connect((root / "queue.sqlite3").as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN")
        has_markers = db.execute("SELECT 1 FROM sqlite_master WHERE name='reviewed_repair_items' AND type='table'").fetchone()
        for url in selected:
            row = db.execute("SELECT * FROM articles WHERE url=?", (url,)).fetchone()
            problem = _head_problem(row, now, registry=None if address_recheck or publication_clock_recheck else registry,
                allowed_failure_states=(PUBLICATION_CLOCK_RECHECK_STATES if publication_clock_recheck
                                        else ADDRESS_RECHECK_STATES if address_recheck else REPAIRABLE),
                stored_recheck=empty_recheck)
            if problem is None:
                try:
                    source, identity = _source(db, registry, url)
                    if source.provider != row["provider"]:
                        raise ValueError("provider_changed")
                    if retained_originals is not None and (source.provider!='globalvoices' or row['state']!='body_not_found'):
                        raise ValueError('unsupported_retained_outcome_target')
                    if empty_recheck:
                        _verify_empty_response(root, source, row, empty_response_witnesses[url])
                    if native_recheck:
                        verify_native_success(db, source, row, native_success_witnesses[url])
                    outcome = (PUBLICATION_CLOCK_RECHECK_OUTCOME if publication_clock_recheck
                               else ADDRESS_RECHECK_OUTCOME if address_recheck else NATIVE_SUCCESS_OUTCOME
                               if native_recheck else 'revalidation_enqueued' if empty_recheck else 'enqueued')
                    if retained_originals is None and has_markers and _already_admitted(
                            db, source.provider, identity['parser_fingerprint'], row['id'], outcome):
                        raise ValueError('already_admitted_for_address_validation' if address_recheck
                                         else 'already_admitted_for_parser')
                except (ValueError, NewsError, OSError, TypeError, KeyError, OverflowError, RecursionError, pa.ArrowException) as error:
                    problem = error.code if isinstance(error, NewsError) else str(error)
            if problem:
                excluded.append({"url": url, "reason": problem})
            else:
                targets.append(_head(row))
                sources[source.provider] = identity
                if empty_recheck:
                    empty_responses[row['id'].hex()] = {
                        'reference': empty_response_witnesses[url],
                        'stored_head': {key: row[key] for key in STORED_HEAD_FIELDS}}
    result = {"contract_version": (PUBLICATION_CLOCK_RECHECK_VERSION if publication_clock_recheck
                                  else ADDRESS_RECHECK_VERSION if address_recheck
                                  else NATIVE_SUCCESS_VERSION if native_recheck else CONTRACT_VERSION),
              "submitted_targets": len(targets), "excluded": excluded,
              "publisher_requests": 0, "queue_mutations": False, "accepted_as_bodies": False}
    if not targets:
        return dict(result, submitted=False, request_id=None)
    request = {"contract_version": CONTRACT_VERSION, "kind": "reviewed_missing_body_repair",
               "created_at_utc": utc_now(), "reason": reason.strip(), "sources": sources, "targets": targets}
    if address_recheck:
        request.update(contract_version=ADDRESS_RECHECK_VERSION, kind=ADDRESS_RECHECK_KIND)
    if publication_clock_recheck:
        request.update(contract_version=PUBLICATION_CLOCK_RECHECK_VERSION, kind=PUBLICATION_CLOCK_RECHECK_KIND)
    if retained_originals is not None:
        from downloader.news_fulltext_replay import RETAINED_NONTEXT_OUTCOME_KIND
        request.update(contract_version=3,kind=RETAINED_NONTEXT_OUTCOME_KIND,
            originals={target['id']:retained_originals[target['url']] for target in targets})
    if empty_recheck:
        request.update(contract_version=EMPTY_RESPONSE_RECHECK_VERSION, kind=EMPTY_RESPONSE_RECHECK_KIND,
                       empty_responses=empty_responses)
    if native_recheck:
        request.update(contract_version=NATIVE_SUCCESS_VERSION, kind=NATIVE_SUCCESS_KIND,
            native_successes={target['id']: native_success_witnesses[target['url']] for target in targets})
    payload = _bytes(request)
    if len(payload) > MAX_REQUEST_BYTES:
        raise ValueError("repair_request_too_large")
    directory = _directory(root, "pending")
    if len(_pending(directory)) >= MAX_PENDING:
        raise ValueError("repair_inbox_full")
    digest = hashlib.sha256(payload).hexdigest()
    path = directory / (digest + ".json")
    _write_once(path, payload)
    return dict(result, submitted=True, request_id=digest, request_path=str(path))


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_repair_json_key")
        result[key] = value
    return result


def _read_private_request(path, *, include_payload=False):
    """Read a bounded, content-addressed mailbox item without following links.

    Snapshot consumers may retain the exact verified bytes alongside the
    decoded request; owner admission still validates the same request ABI.
    """
    with os.fdopen(os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)), "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_REQUEST_BYTES or info.st_mode & 0o077:
            raise ValueError("repair_input_not_bounded_private_file")
        payload = stream.read(MAX_REQUEST_BYTES + 1)
    if len(payload) > MAX_REQUEST_BYTES or hashlib.sha256(payload).hexdigest() != path.stem:
        raise ValueError("repair_input_digest_mismatch")
    request = json.loads(payload, object_pairs_hook=_unique)
    return (request, payload) if include_payload else request


def _unsafe_request_path(path, error):
    """Recognize a proven unsafe inode without treating transient I/O as invalid."""
    if error.errno not in (errno.ELOOP, errno.EISDIR):
        return False
    try:
        mode = path.lstat().st_mode
    except OSError:
        return False
    return ((error.errno == errno.ELOOP and stat.S_ISLNK(mode))
            or (error.errno == errno.EISDIR and stat.S_ISDIR(mode)))


def _load(path):
    return _validate_request(_read_private_request(path))


def _validate_request(request):
    from downloader.news_fulltext_moneyweekly_outcomes import VERSION as PAID_VERSION, validate_request as validate_paid_request
    if isinstance(request, dict) and request.get('contract_version') == PAID_VERSION:
        return validate_paid_request(request)
    from downloader.news_fulltext_route_rechecks import VERSION as ROUTE_VERSION, validate_request as validate_route_request
    if isinstance(request,dict) and request.get('contract_version')==ROUTE_VERSION:
        return validate_route_request(request)
    from downloader.news_fulltext_stored_quality import VERSION as QUALITY_VERSION, validate_request
    if isinstance(request, dict) and request.get('contract_version') == QUALITY_VERSION:
        return validate_request(request)
    retained=isinstance(request,dict) and request.get('contract_version')==3
    empty = isinstance(request, dict) and request.get('contract_version') == EMPTY_RESPONSE_RECHECK_VERSION
    native = isinstance(request, dict) and request.get('contract_version') == NATIVE_SUCCESS_VERSION
    address = isinstance(request, dict) and request.get('contract_version') == ADDRESS_RECHECK_VERSION
    clock = isinstance(request, dict) and request.get('contract_version') == PUBLICATION_CLOCK_RECHECK_VERSION
    fields=({"contract_version", "kind", "created_at_utc", "reason", "sources", "targets"}
            | ({'originals'} if retained else {'empty_responses'} if empty else {'native_successes'} if native else set()))
    from downloader.news_fulltext_replay import RETAINED_NONTEXT_OUTCOME_KIND,validate_nontext_original_reference
    if (not isinstance(request, dict) or set(request) != fields
            or type(request["contract_version"]) is not int or request["contract_version"] not in {1, CONTRACT_VERSION,3,EMPTY_RESPONSE_RECHECK_VERSION,NATIVE_SUCCESS_VERSION,ADDRESS_RECHECK_VERSION,PUBLICATION_CLOCK_RECHECK_VERSION}
            or request["kind"] != (RETAINED_NONTEXT_OUTCOME_KIND if retained else
                                   EMPTY_RESPONSE_RECHECK_KIND if empty else NATIVE_SUCCESS_KIND if native else
                                   ADDRESS_RECHECK_KIND if address else PUBLICATION_CLOCK_RECHECK_KIND if clock
                                   else "reviewed_missing_body_repair")
            or not isinstance(request["reason"], str) or not request["reason"].strip() or len(request["reason"]) > 240
            or _clock(request["created_at_utc"]) > time.time()
            or not isinstance(request["targets"], list) or not 1 <= len(request["targets"]) <= MAX_TARGETS
            or not isinstance(request["sources"], dict)):
        raise ValueError("invalid_repair_request")
    seen = set()
    # ABI 1 requests retain their original semantic-only state contract.
    states = PUBLICATION_CLOCK_RECHECK_STATES if clock else ADDRESS_RECHECK_STATES if address else REPAIRABLE | (
        {_REVIEWED_SCOPE_FAILURE} if request["contract_version"] == 2 else set())
    for target in request["targets"]:
        if (not isinstance(target, dict) or set(target) != set(HEAD_FIELDS)
                or not isinstance(target["url"], str) or normalize_url(target["url"]) != target["url"]
                or target["id"] != hashlib.sha256(target["url"].encode()).hexdigest()
                or target["id"] in seen or not isinstance(target["provider"], str)
                or not isinstance(target["state"], str) or target["state"] not in states
                or target["error_code"] != target["state"]
                or type(target["attempts"]) is not int or target["attempts"] < 0
                or type(target["due"]) not in (int, float) or not math.isfinite(target["due"]) or target["due"] < 0
                or target["due"] > _clock(request["created_at_utc"])
                or _clock(target["last_checked_at_utc"]) > _clock(request["created_at_utc"])):
            raise ValueError("invalid_repair_target")
        seen.add(target["id"])
    if set(request["sources"]) != {target["provider"] for target in request["targets"]}:
        raise ValueError("invalid_repair_sources")
    for identity in request["sources"].values():
        if (not isinstance(identity, dict) or set(identity) != SOURCE_FIELDS
                or type(identity["contract_version"]) is not int or identity["contract_version"] != 1
                or any(not isinstance(identity[key], str) or not re.fullmatch(r"[0-9a-f]{64}", identity[key])
                       for key in SOURCE_FIELDS - {"contract_version"})):
            raise ValueError("invalid_repair_generation")
    if retained:
        if (not isinstance(request['originals'],dict) or set(request['originals'])!=seen
                or any(target['provider']!='globalvoices' or target['state']!='body_not_found' for target in request['targets'])):
            raise ValueError('invalid_retained_outcome_targets')
        for reference in request['originals'].values():
            validate_nontext_original_reference(reference)
    if empty:
        if (not isinstance(request['empty_responses'], dict) or set(request['empty_responses']) != seen
                or any(target['provider'] != 'cnyes' or target['state'] not in {'body_not_found', 'article_identity_not_verified'}
                       for target in request['targets'])):
            raise ValueError('invalid_empty_response_targets')
        for witness in request['empty_responses'].values():
            if (not isinstance(witness, dict) or set(witness) != {'reference', 'stored_head'}
                    or not isinstance(witness['stored_head'], dict) or set(witness['stored_head']) != set(STORED_HEAD_FIELDS)):
                raise ValueError('invalid_empty_response_head')
            _empty_response_reference(witness['reference'])
    if native:
        if (not isinstance(request['native_successes'], dict) or set(request['native_successes']) != seen
                or any(t['provider'] != 'upmedia' or t['state'] not in {
                    'article_identity_not_verified', 'article_attribution_not_verified'} for t in request['targets'])):
            raise ValueError('invalid_native_success_targets')
        for reference in request['native_successes'].values():
            validate_native_success_reference(reference)
    return request


def _load_for_admission(queue, path):
    """Reuse shape validation, never private-file or current-head admission.

    Every slice still opens the actual bounded private file without following
    symlinks, verifies its content-addressed name, and rejects duplicate keys.
    Equal decoded content can reuse owner-local validation. All dynamic source,
    lease, deadline, retained-original and once-only checks stay in _admit.
    """
    request = _read_private_request(path)
    cache = getattr(queue, 'reviewed_repair_validated_requests', None)
    if cache is None:
        cache = queue.reviewed_repair_validated_requests = OrderedDict()
    previous = cache.pop(path.name, None)
    now = time.time()
    if previous is not None and previous[0] == request and now >= previous[2]:
        cache[path.name] = previous
        return previous[0]
    request = _validate_request(request)
    cache[path.name] = (request, len(_bytes(request)), time.time())
    while (len(cache) > MAX_VALIDATED_REQUESTS
           or sum(item[1] for item in cache.values()) > MAX_VALIDATED_REQUEST_BYTES):
        cache.popitem(last=False)
    return request


def _schema(queue):
    if getattr(queue, "reviewed_repair_schema_ready", False):
        return
    with queue.db:
        queue.db.execute("CREATE TABLE IF NOT EXISTS reviewed_repair_jobs("
                         "request_id TEXT PRIMARY KEY,targets INTEGER NOT NULL,next_index INTEGER NOT NULL,"
                         "created_at_utc TEXT NOT NULL,completed_at_utc TEXT,owner_id TEXT NOT NULL)")
        queue.db.execute("CREATE TABLE IF NOT EXISTS reviewed_repair_items("
                         "request_id TEXT NOT NULL,target_index INTEGER NOT NULL,article_id BLOB NOT NULL,"
                         "provider TEXT NOT NULL,parser_fingerprint TEXT NOT NULL,outcome TEXT NOT NULL,"
                         "recorded_at_utc TEXT NOT NULL,owner_id TEXT NOT NULL,PRIMARY KEY(request_id,target_index))")
        queue.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS reviewed_repair_once ON reviewed_repair_items("
                         "provider,parser_fingerprint,article_id) WHERE outcome='enqueued'")
        queue.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS reviewed_empty_response_once ON reviewed_repair_items("
                         "provider,parser_fingerprint,article_id) WHERE outcome='revalidation_enqueued'")
        queue.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS reviewed_native_success_once ON reviewed_repair_items("
                         "provider,parser_fingerprint,article_id) WHERE outcome='native_success_recheck_enqueued'")
        queue.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS reviewed_address_validation_once ON reviewed_repair_items("
                         "provider,article_id) WHERE outcome='address_validation_recheck_enqueued'")
        queue.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS reviewed_publication_clock_once ON reviewed_repair_items("
                         "provider,parser_fingerprint,article_id) WHERE outcome='publication_clock_recheck_enqueued'")
        queue.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS reviewed_native_route_once ON reviewed_repair_items("
                         "provider,parser_fingerprint,article_id) WHERE outcome='native_article_route_recheck_enqueued'")
    queue.reviewed_repair_schema_ready = True


def _admit(queue, request, target):
    from downloader.news_fulltext_moneyweekly_outcomes import VERSION as PAID_VERSION, admit as admit_paid_preview
    if request['contract_version'] == PAID_VERSION:
        return admit_paid_preview(queue, request, target)
    from downloader.news_fulltext_route_rechecks import VERSION as ROUTE_VERSION, admit as admit_route
    if request['contract_version']==ROUTE_VERSION:
        return admit_route(queue,request,target)
    from downloader.news_fulltext_stored_quality import VERSION as QUALITY_VERSION, admit as admit_quality
    if request['contract_version'] == QUALITY_VERSION:
        return admit_quality(queue, request, target)
    row = queue.db.execute("SELECT * FROM articles WHERE id=?", (bytes.fromhex(target["id"]),)).fetchone()
    empty = request['contract_version'] == EMPTY_RESPONSE_RECHECK_VERSION
    native = request['contract_version'] == NATIVE_SUCCESS_VERSION
    address = request['contract_version'] == ADDRESS_RECHECK_VERSION
    clock = request['contract_version'] == PUBLICATION_CLOCK_RECHECK_VERSION
    problem = _head_problem(row, time.time(), registry=None if address or clock else queue.registry,
        allowed_failure_states=(PUBLICATION_CLOCK_RECHECK_STATES if clock
                                else ADDRESS_RECHECK_STATES if address else REPAIRABLE), stored_recheck=empty)
    if problem:
        return problem
    if _head(row) != target:
        return "head_changed_since_review"
    try:
        source, identity = _source(queue.db, queue.registry, target["url"])
    except (NewsError, ValueError) as error:
        return error.code if isinstance(error, NewsError) else str(error)
    if source.provider != target["provider"] or identity != request["sources"][source.provider]:
        return "source_identity_changed"
    outcome = (PUBLICATION_CLOCK_RECHECK_OUTCOME if clock else ADDRESS_RECHECK_OUTCOME if address else NATIVE_SUCCESS_OUTCOME if native
               else 'revalidation_enqueued' if empty else 'enqueued')
    if native:
        try:
            verify_native_success(queue.db, source, row, request['native_successes'][target['id']])
        except (ValueError, KeyError, TypeError, OverflowError, RecursionError):
            return 'native_success_evidence_invalid'
    if empty:
        witness = request['empty_responses'][target['id']]
        if {key: row[key] for key in STORED_HEAD_FIELDS} != witness['stored_head']:
            return 'stored_head_changed_since_review'
        try:
            _verify_empty_response(queue.root, source, row, witness['reference'])
        except (ValueError, KeyError, TypeError, OverflowError, RecursionError, pa.ArrowException):
            return 'empty_response_evidence_invalid'
    if request['contract_version']==3:
        from downloader.news_fulltext_replay import classify_retained_nontext_outcome
        try:
            return classify_retained_nontext_outcome(queue,source,target,request['originals'][target['id']])
        except (NewsError,ValueError,TypeError,KeyError,OverflowError,RecursionError,pa.ArrowException):
            return 'retained_outcome_evidence_invalid'
    if _already_admitted(queue.db, source.provider, identity['parser_fingerprint'], row['id'], outcome):
        return 'already_admitted_for_address_validation' if address else 'already_admitted_for_parser'
    # No attempts/deadline/error/check reset, no lease or saved-body mutation.
    queue.db.execute("UPDATE articles SET state='retry' WHERE id=?", (row["id"],))
    return outcome


def _archive(queue, path, request=None):
    job = dict(queue.db.execute("SELECT * FROM reviewed_repair_jobs WHERE request_id=?", (path.stem,)).fetchone())
    outcomes = [dict(row) for row in queue.db.execute(
        "SELECT target_index,lower(hex(article_id)) AS article_id,provider,outcome,recorded_at_utc,owner_id "
        "FROM reviewed_repair_items WHERE request_id=? ORDER BY target_index", (path.stem,))]
    receipt = {"contract_version": CONTRACT_VERSION, **job, "outcomes": outcomes,
               "admitted_targets": sum(row["outcome"] in ADMITTED_OUTCOMES for row in outcomes),
               "accepted_as_bodies": False, "request_sha256": path.stem}
    from downloader.news_fulltext_moneyweekly_outcomes import VERSION as PAID_VERSION, KIND as PAID_KIND, OUTCOME as PAID_OUTCOME
    if request is not None and request['contract_version'] == PAID_VERSION:
        receipt.update(contract_version=PAID_VERSION, kind=PAID_KIND, witnesses=request['witnesses'],
            reclassified_targets=sum(row['outcome'] == PAID_OUTCOME for row in outcomes),
            publisher_requests=0, body_acceptances=0, source_freshness_verified=False,
            source_check_and_publication_clocks_changed=False, immutable_bodies_preserved=True,
            review_clock_basis='reviewed_repair_items.recorded_at_utc')
    from downloader.news_fulltext_route_rechecks import VERSION as ROUTE_VERSION, KIND as ROUTE_KIND
    if request is not None and request['contract_version'] == ROUTE_VERSION:
        receipt.update(contract_version=ROUTE_VERSION, kind=ROUTE_KIND, witnesses=request['witnesses'],
            publisher_requests=0, body_acceptances=0,
            attempts_deadlines_errors_checks_and_saved_bodies_changed=False,
            full_body_still_requires_normal_capture=True, source_freshness_verified=False)
    from downloader.news_fulltext_stored_quality import VERSION as QUALITY_VERSION, KIND as QUALITY_KIND
    if request is not None and request['contract_version'] == QUALITY_VERSION:
        receipt.update(contract_version=QUALITY_VERSION, kind=QUALITY_KIND, originals=request['originals'],
            expected_error=request['expected_error'], publisher_requests=0, body_acceptances=0,
            reclassified_targets=sum(row['outcome'] == 'stored_quality_reclassified' for row in outcomes),
            immutable_bodies_preserved=True, source_observation_clocks_changed=False,
            review_clock_basis='reviewed_repair_items.recorded_at_utc',
            source_freshness_verified=False)
    if request is not None and request['contract_version']==3:
        receipt.update(contract_version=3,kind=request['kind'],originals=request['originals'],
            reclassified_targets=sum(row['outcome'] in CLASSIFIED_NONTEXT_OUTCOMES for row in outcomes),
            publisher_requests=0,body_acceptances=0,check_and_publication_clocks_changed=False)
    if request is not None and request['contract_version'] == EMPTY_RESPONSE_RECHECK_VERSION:
        receipt.update(contract_version=EMPTY_RESPONSE_RECHECK_VERSION, kind=request['kind'],
            empty_responses=request['empty_responses'], publisher_requests=0, body_acceptances=0,
            stored_heads_attempts_deadlines_and_clocks_changed=False, source_freshness_verified=False)
    if request is not None and request['contract_version'] == NATIVE_SUCCESS_VERSION:
        receipt.update(contract_version=NATIVE_SUCCESS_VERSION, kind=NATIVE_SUCCESS_KIND,
            native_successes=request['native_successes'], publisher_requests=0, body_acceptances=0,
            attempts_deadlines_errors_checks_and_saved_bodies_changed=False,
            previous_parser_repair_marker_preserved=True, full_body_still_requires_normal_capture=True)
    if request is not None and request['contract_version'] == ADDRESS_RECHECK_VERSION:
        receipt.update(contract_version=ADDRESS_RECHECK_VERSION, kind=ADDRESS_RECHECK_KIND,
            publisher_requests=0, body_acceptances=0, permission_granted=False,
            attempts_deadlines_errors_checks_and_saved_bodies_changed=False,
            full_body_still_requires_normal_capture=True, address_and_robots_checks_still_required=True)
    if request is not None and request['contract_version'] == PUBLICATION_CLOCK_RECHECK_VERSION:
        receipt.update(contract_version=PUBLICATION_CLOCK_RECHECK_VERSION, kind=PUBLICATION_CLOCK_RECHECK_KIND,
            publisher_requests=0, body_acceptances=0,
            attempts_deadlines_errors_checks_and_saved_bodies_changed=False,
            publication_clocks_rewritten=False, full_body_still_requires_normal_capture=True)
    directory = _directory(queue.root, "processed")
    _write_once(directory / (path.stem + ".outcome.json"), _bytes(receipt))
    archived = directory / path.name
    if archived.is_symlink() or archived.exists() and hashlib.sha256(archived.read_bytes()).hexdigest() != path.stem:
        raise ValueError("repair_archive_conflict")
    os.replace(path, archived)
    for parent in (directory, path.parent):
        descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _quarantine(queue, path, error):
    # Preserve the input inode, including an unsafe symlink, without following
    # it. Bad local control files must not starve later valid work or HTTP.
    directory = _directory(queue.root, "rejected")
    info = path.lstat()
    identity = {"name": path.name, "inode": info.st_ino, "mtime_ns": info.st_mtime_ns,
                "size": info.st_size, "mode": info.st_mode}
    key = hashlib.sha256(_bytes(identity)).hexdigest()
    archived = directory / (key + ".input")
    if archived.exists() or archived.is_symlink():
        raise ValueError("repair_rejection_conflict")
    _write_once(directory / (key + ".outcome.json"), _bytes({
        "contract_version": CONTRACT_VERSION, "input_file_identity": identity,
        "error": type(error).__name__, "accepted_as_bodies": False}))
    # Deterministic receipt bytes let a crash between receipt and rename resume;
    # a later bad file with the same name gets a distinct retained inode proof.
    os.replace(path, archived)
    for parent in (directory, path.parent):
        descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def consume_repairs(queue, *, limit=32, max_seconds=0.05, stop=None):
    """Rotate bounded admission slices; normal dispatch owns every request."""
    if type(limit) is not int or not 1 <= limit <= MAX_TARGETS or not math.isfinite(max_seconds) or not 0 < max_seconds <= 0.1:
        raise ValueError("invalid_bounded_repair_slice")
    started, counts, processed = time.monotonic(), Counter(), 0
    queue.reviewed_repair_pending = False
    queue.reviewed_repair_continuation_pending = False
    progressed, considered = False, 0
    base = queue.root / "reviewed_repairs"
    from downloader.news_fulltext_wake import clear_mailbox_scan_retry, defer_mailbox_scan_retry
    try:
        try:
            base.lstat()
        except FileNotFoundError:
            clear_mailbox_scan_retry(queue, 'repair')
            return {"processed": 0, "outcomes": {}}
        directory = _directory(queue.root, "pending")
        paths = _rotation_order(queue, _pending(directory))
    except (OSError, ValueError, sqlite3.OperationalError):
        defer_mailbox_scan_retry(queue, 'repair')
        queue.metrics.count("local_repairs", "inbox_unavailable")
        return {"processed": 0, "outcomes": {}, "inbox_unavailable": True}
    clear_mailbox_scan_retry(queue, 'repair')
    for path in paths:
        # Discovery or validation can itself exceed the soft slice. Permit one
        # bounded request/target to make progress, then yield; the persisted
        # turn keeps different requests fair across slow slices and restarts.
        if (processed >= limit or stop is not None and stop.is_set()
                or (considered > 0 and time.monotonic() - started >= max_seconds)):
            break
        try:
            request = _load_for_admission(queue, path)
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError, NewsError) as error:
            if stop is not None and stop.is_set():
                break
            if isinstance(error, OSError) and not _unsafe_request_path(path, error):
                # Transient I/O is not proof that a sealed request is bad.
                # Retain it and spend this request's turn so others can run.
                counts['request_io_deferred'] += 1
                processed += 1
                with queue.db:
                    _record_turn(queue, path, stop)
                considered += 1
                continue
            try:
                _quarantine(queue, path, error)
                progressed = True
            except (OSError, ValueError):
                queue.metrics.count("local_repairs", "quarantine_deferred")
            counts["invalid_request"] += 1
            processed += 1
            with queue.db:
                _record_turn(queue, path, stop)
            considered += 1
            continue
        if stop is not None and stop.is_set():
            break
        _schema(queue)
        with queue.db:
            queue.db.execute("INSERT OR IGNORE INTO reviewed_repair_jobs VALUES (?,?,0,?,NULL,?)",
                             (path.stem, len(request["targets"]), request["created_at_utc"], queue.owner))
            job = queue.db.execute("SELECT * FROM reviewed_repair_jobs WHERE request_id=?", (path.stem,)).fetchone()
            cursor = job["next_index"]
            previous_cursor = cursor
            while cursor < len(request["targets"]) and processed < limit:
                if (stop is not None and stop.is_set()
                        or ((processed > 0 or considered > 0) and time.monotonic() - started >= max_seconds)):
                    break
                target = request["targets"][cursor]
                try:
                    outcome = _admit(queue, request, target)
                except OSError:
                    from downloader.news_fulltext_stored_quality import VERSION as QUALITY_VERSION
                    if request['contract_version'] not in {3, EMPTY_RESPONSE_RECHECK_VERSION, QUALITY_VERSION}:
                        raise
                    # A temporarily unavailable retained file must not stop
                    # the resident HTTP owner or consume the local outcome.
                    # Keep this target/cursor intact; try other mail next.
                    counts['retained_outcome_io_deferred']+=1
                    processed+=1
                    break
                queue.db.execute("INSERT INTO reviewed_repair_items VALUES (?,?,?,?,?,?,?,?)", (
                    path.stem, cursor, bytes.fromhex(target["id"]), target["provider"],
                    request["sources"][target["provider"]]["parser_fingerprint"], outcome, utc_now(), queue.owner))
                counts[outcome] += 1
                processed += 1
                cursor += 1
                queue.counter("reviewed_repair_targets_processed", 1)
                if outcome in ADMITTED_OUTCOMES:
                    queue.counter("reviewed_repair_targets_admitted", 1)
                    if outcome == 'revalidation_enqueued':
                        queue.counter('reviewed_empty_response_rechecks_admitted', 1)
                    elif outcome == NATIVE_SUCCESS_OUTCOME:
                        queue.counter('reviewed_native_success_rechecks_admitted', 1)
                    elif outcome == ADDRESS_RECHECK_OUTCOME:
                        queue.counter('reviewed_address_validation_rechecks_admitted', 1)
                    elif outcome == PUBLICATION_CLOCK_RECHECK_OUTCOME:
                        queue.counter('reviewed_publication_clock_rechecks_admitted', 1)
                elif outcome in CLASSIFIED_SOURCE_OUTCOMES or outcome == 'stored_quality_reclassified':
                    queue.counter('reviewed_retained_outcomes_classified',1)
            complete = cursor == len(request["targets"])
            queue.db.execute("UPDATE reviewed_repair_jobs SET next_index=?,completed_at_utc=coalesce(completed_at_utc,?) "
                             "WHERE request_id=?", (cursor, utc_now() if complete else None, path.stem))
            _record_turn(queue, path, stop)
        # Hints follow the successful commit. A rollback, unavailable retained
        # original or completed-but-unarchived file cannot cause a fast loop.
        progressed |= cursor > previous_cursor
        queue.reviewed_repair_pending |= not complete
        considered += 1
        if complete:
            try:
                from downloader.news_fulltext_stored_quality import VERSION as QUALITY_VERSION
                from downloader.news_fulltext_route_rechecks import VERSION as ROUTE_VERSION
                from downloader.news_fulltext_moneyweekly_outcomes import VERSION as PAID_VERSION
                if request['contract_version'] in {3, EMPTY_RESPONSE_RECHECK_VERSION, NATIVE_SUCCESS_VERSION, ADDRESS_RECHECK_VERSION, PUBLICATION_CLOCK_RECHECK_VERSION, QUALITY_VERSION, ROUTE_VERSION, PAID_VERSION}:
                    _archive(queue,path,request=request)
                else:
                    _archive(queue,path)
            except (OSError, ValueError):
                queue.metrics.count("local_repairs", "archive_deferred")
    for outcome, count in counts.items():
        queue.metrics.count("local_repairs", "reviewed_" + outcome, count)
    queue.reviewed_repair_continuation_pending = progressed and (
        queue.reviewed_repair_pending or considered < len(paths))
    return {"processed": processed, "outcomes": dict(counts)}
