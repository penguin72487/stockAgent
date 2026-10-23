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
"""
from __future__ import annotations

from collections import Counter
from contextlib import closing
from datetime import datetime
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
STORED_HEAD_FIELDS = ('body_sha256', 'body_bytes', 'parquet_path', 'captured_at_utc',
                      'etag', 'last_modified', 'validator_url')
MAX_TARGETS = 512
MAX_PENDING = 64
MAX_REQUEST_BYTES = 1 << 20
PUMP_INTERVAL_SECONDS = 5
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


def submit_repairs(root, registry, urls, *, reason, retained_originals=None, empty_response_witnesses=None,
                   native_success_witnesses=None):
    """Seal explicit eligible failures while the canonical owner stays alive."""
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 240:
        raise ValueError("repair_requires_bounded_review_reason")
    selected = list(islice(urls, MAX_TARGETS + 1))
    if not 1 <= len(selected) <= MAX_TARGETS:
        raise ValueError("repair_requires_one_to_512_urls")
    selected = list(dict.fromkeys(normalize_url(url) for url in selected))
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
            problem = _head_problem(row, now, registry=registry, stored_recheck=empty_recheck)
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
                    outcome = NATIVE_SUCCESS_OUTCOME if native_recheck else 'revalidation_enqueued' if empty_recheck else 'enqueued'
                    if retained_originals is None and has_markers and db.execute(
                            "SELECT 1 FROM reviewed_repair_items WHERE provider=? AND parser_fingerprint=? "
                            "AND article_id=? AND outcome=?", (source.provider, identity["parser_fingerprint"], row["id"], outcome)).fetchone():
                        raise ValueError("already_admitted_for_parser")
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
    result = {"contract_version": NATIVE_SUCCESS_VERSION if native_recheck else CONTRACT_VERSION,
              "submitted_targets": len(targets), "excluded": excluded,
              "publisher_requests": 0, "queue_mutations": False, "accepted_as_bodies": False}
    if not targets:
        return dict(result, submitted=False, request_id=None)
    request = {"contract_version": CONTRACT_VERSION, "kind": "reviewed_missing_body_repair",
               "created_at_utc": utc_now(), "reason": reason.strip(), "sources": sources, "targets": targets}
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


def _read_private_request(path):
    """Common bounded local mailbox reader; callers validate their own ABI."""
    with os.fdopen(os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)), "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_REQUEST_BYTES or info.st_mode & 0o077:
            raise ValueError("repair_input_not_bounded_private_file")
        payload = stream.read(MAX_REQUEST_BYTES + 1)
    if len(payload) > MAX_REQUEST_BYTES or hashlib.sha256(payload).hexdigest() != path.stem:
        raise ValueError("repair_input_digest_mismatch")
    return json.loads(payload, object_pairs_hook=_unique)


def _load(path):
    request = _read_private_request(path)
    retained=isinstance(request,dict) and request.get('contract_version')==3
    empty = isinstance(request, dict) and request.get('contract_version') == EMPTY_RESPONSE_RECHECK_VERSION
    native = isinstance(request, dict) and request.get('contract_version') == NATIVE_SUCCESS_VERSION
    fields=({"contract_version", "kind", "created_at_utc", "reason", "sources", "targets"}
            | ({'originals'} if retained else {'empty_responses'} if empty else {'native_successes'} if native else set()))
    from downloader.news_fulltext_replay import RETAINED_NONTEXT_OUTCOME_KIND,validate_nontext_original_reference
    if (not isinstance(request, dict) or set(request) != fields
            or type(request["contract_version"]) is not int or request["contract_version"] not in {1, CONTRACT_VERSION,3,EMPTY_RESPONSE_RECHECK_VERSION,NATIVE_SUCCESS_VERSION}
            or request["kind"] != (RETAINED_NONTEXT_OUTCOME_KIND if retained else
                                   EMPTY_RESPONSE_RECHECK_KIND if empty else NATIVE_SUCCESS_KIND if native else "reviewed_missing_body_repair")
            or not isinstance(request["reason"], str) or not request["reason"].strip() or len(request["reason"]) > 240
            or _clock(request["created_at_utc"]) > time.time()
            or not isinstance(request["targets"], list) or not 1 <= len(request["targets"]) <= MAX_TARGETS
            or not isinstance(request["sources"], dict)):
        raise ValueError("invalid_repair_request")
    seen = set()
    # ABI 1 requests retain their original semantic-only state contract.
    states = REPAIRABLE | ({_REVIEWED_SCOPE_FAILURE} if request["contract_version"] == 2 else set())
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
    queue.reviewed_repair_schema_ready = True


def _admit(queue, request, target):
    row = queue.db.execute("SELECT * FROM articles WHERE id=?", (bytes.fromhex(target["id"]),)).fetchone()
    empty = request['contract_version'] == EMPTY_RESPONSE_RECHECK_VERSION
    native = request['contract_version'] == NATIVE_SUCCESS_VERSION
    problem = _head_problem(row, time.time(), registry=queue.registry, stored_recheck=empty)
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
    outcome = NATIVE_SUCCESS_OUTCOME if native else 'revalidation_enqueued' if empty else 'enqueued'
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
    if queue.db.execute("SELECT 1 FROM reviewed_repair_items WHERE provider=? AND parser_fingerprint=? "
                        "AND article_id=? AND outcome=?",
                        (source.provider, identity["parser_fingerprint"], row["id"], outcome)).fetchone():
        return "already_admitted_for_parser"
    # No attempts/deadline/error/check reset, no lease or saved-body mutation.
    queue.db.execute("UPDATE articles SET state='retry' WHERE id=?", (row["id"],))
    return outcome


def _archive(queue, path, request=None):
    job = dict(queue.db.execute("SELECT * FROM reviewed_repair_jobs WHERE request_id=?", (path.stem,)).fetchone())
    outcomes = [dict(row) for row in queue.db.execute(
        "SELECT target_index,lower(hex(article_id)) AS article_id,provider,outcome,recorded_at_utc,owner_id "
        "FROM reviewed_repair_items WHERE request_id=? ORDER BY target_index", (path.stem,))]
    receipt = {"contract_version": CONTRACT_VERSION, **job, "outcomes": outcomes,
               "admitted_targets": sum(row["outcome"] in {"enqueued", 'revalidation_enqueued', NATIVE_SUCCESS_OUTCOME} for row in outcomes),
               "accepted_as_bodies": False, "request_sha256": path.stem}
    if request is not None and request['contract_version']==3:
        receipt.update(contract_version=3,kind=request['kind'],originals=request['originals'],
            reclassified_targets=sum(row['outcome']=='classified_source_body_image_only' for row in outcomes),
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
    """Bound local admission work; normal dispatch still owns every request."""
    if type(limit) is not int or not 1 <= limit <= MAX_TARGETS or not math.isfinite(max_seconds) or not 0 < max_seconds <= 0.1:
        raise ValueError("invalid_bounded_repair_slice")
    started, counts, processed = time.monotonic(), Counter(), 0
    base = queue.root / "reviewed_repairs"
    if not base.exists() and not base.is_symlink():
        return {"processed": 0, "outcomes": {}}
    try:
        directory = _directory(queue.root, "pending")
        paths = _pending(directory)
    except (OSError, ValueError):
        queue.metrics.count("local_repairs", "inbox_unavailable")
        return {"processed": 0, "outcomes": {}, "inbox_unavailable": True}
    for path in paths:
        if processed >= limit or time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
            break
        try:
            request = _load(path)
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError, NewsError) as error:
            try:
                _quarantine(queue, path, error)
            except (OSError, ValueError):
                queue.metrics.count("local_repairs", "quarantine_deferred")
            counts["invalid_request"] += 1
            processed += 1
            continue
        _schema(queue)
        with queue.db:
            queue.db.execute("INSERT OR IGNORE INTO reviewed_repair_jobs VALUES (?,?,0,?,NULL,?)",
                             (path.stem, len(request["targets"]), request["created_at_utc"], queue.owner))
            job = queue.db.execute("SELECT * FROM reviewed_repair_jobs WHERE request_id=?", (path.stem,)).fetchone()
            cursor = job["next_index"]
            while cursor < len(request["targets"]) and processed < limit:
                if time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
                    break
                target = request["targets"][cursor]
                try:
                    outcome = _admit(queue, request, target)
                except OSError:
                    if request['contract_version'] not in {3, EMPTY_RESPONSE_RECHECK_VERSION}:
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
                if outcome in {"enqueued", 'revalidation_enqueued', NATIVE_SUCCESS_OUTCOME}:
                    queue.counter("reviewed_repair_targets_admitted", 1)
                    if outcome == 'revalidation_enqueued':
                        queue.counter('reviewed_empty_response_rechecks_admitted', 1)
                    elif outcome == NATIVE_SUCCESS_OUTCOME:
                        queue.counter('reviewed_native_success_rechecks_admitted', 1)
                elif outcome=='classified_source_body_image_only':
                    queue.counter('reviewed_retained_outcomes_classified',1)
            complete = cursor == len(request["targets"])
            queue.db.execute("UPDATE reviewed_repair_jobs SET next_index=?,completed_at_utc=coalesce(completed_at_utc,?) "
                             "WHERE request_id=?", (cursor, utc_now() if complete else None, path.stem))
        if complete:
            try:
                if request['contract_version'] in {3, EMPTY_RESPONSE_RECHECK_VERSION, NATIVE_SUCCESS_VERSION}:
                    _archive(queue,path,request=request)
                else:
                    _archive(queue,path)
            except (OSError, ValueError):
                queue.metrics.count("local_repairs", "archive_deferred")
    for outcome, count in counts.items():
        queue.metrics.count("local_repairs", "reviewed_" + outcome, count)
    return {"processed": processed, "outcomes": dict(counts)}
