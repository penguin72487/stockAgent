"""Explicit, bounded reuse of reviewed public native diagnostic originals.

This is a local acceptance path on the canonical queue, not another crawler.
Original observation/authentication evidence stays immutable; current source
scope and native validation are checked again. Old evidence cannot overwrite
a newer head, refusal, lease, or completed body during receipt recovery.
"""
from __future__ import annotations

from datetime import datetime, UTC
import hashlib
import json
from pathlib import Path
import time

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, normalize_url


RETAINED_NATIVE_REPLAY_VERSION = 1
_KIND = 'reviewed_native_diagnostic_materialization_v1'
_MAX_RAW = 16 << 20


def _reviewed_source_scope(source):
    """Existing known originals, plus the already-reviewed CMNews SSR route.

    A discoverable provider is not a generic retained-response grant. CMNews
    keeps its exact current native container, byline and private-reading scope;
    explicit URL/frontier, immutable approval and source checks still apply.
    """
    cmnews = bool(source and source.provider == 'cmnews'
        and source.domains == ('cmnews.com.tw',)
        and source.embedded_article_layout == 'cmnews_visible_article_v1'
        and source.selectors == ('.articleContent__textContent',)
        and source.require_configured_container and source.require_author_attribution)
    return bool(source and source.allowed and (source.known_url_only or cmnews)
        and source.acquisition_mode == 'continuous' and source.embedded_article_layout
        and source.authorization.get('kind') == 'personal_research')


def _json(path, limit):
    if path.stat().st_size > limit:
        raise ValueError('reviewed native JSON exceeds bound')
    with path.open('rb') as stream:
        payload = stream.read(limit + 1)
    if len(payload) > limit:
        raise ValueError('reviewed native JSON changed beyond bound')
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError('reviewed native JSON object required')
    return value, payload


def _clock(value):
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() is None or parsed > datetime.now(UTC):
            raise ValueError('clock is naive or future')
        return parsed
    except (TypeError, ValueError):
        raise ValueError('reviewed native observation clock invalid') from None


def _immutable(root, relative, payload):
    path = root/relative
    if path.exists():
        if path.stat().st_size != len(payload):
            raise ValueError('reviewed native immutable evidence changed')
        with path.open('rb') as stream:
            previous = stream.read(len(payload) + 1)
        if previous != payload:
            raise ValueError('reviewed native immutable evidence changed')
    else:
        atomic_write_bytes(path, payload, durable=True, mode=0o600)


def materialize_reviewed_native(queue, urls, manifest_path, *, lease_seconds):
    """Operator-selected originals, zero HTTP and no retry/permission reset.

    Validate the entire batch before writing evidence or claiming any URL.
    Only the wording of a personal-reading scope may differ from its captured
    diagnostic policy, with both exact policies explicitly approved/preserved.
    Other authorization changes require fresh review, not this import path.
    """
    from downloader.news_fulltext import article_response_record, extract_article_response
    from downloader.news_fulltext_queue import utc_now
    urls = sorted(set(normalize_url(url) for url in urls))
    if not 1 <= len(urls) <= 16 or type(lease_seconds) is not int or lease_seconds <= 0:
        raise ValueError('bounded explicit reviewed native selection required')
    manifest_path = Path(manifest_path).resolve()
    manifest, manifest_bytes = _json(manifest_path, 65536)
    if (type(manifest.get('contract_version')) is not int
            or manifest.get('contract_version') != RETAINED_NATIVE_REPLAY_VERSION
            or manifest.get('scope_kind') != _KIND or manifest.get('publish') is not False
            or manifest.get('remote_training_export') is not False
            or manifest.get('diagnostic_scope_rewording_reviewed') is not True
            or not isinstance(manifest.get('entries'), list)
            or len(manifest['entries']) != len(urls)
            or any(not isinstance(entry, dict) for entry in manifest['entries'])
            or [entry.get('url') for entry in manifest['entries']] != urls):
        raise ValueError('reviewed native approval scope invalid')
    approved = _clock(manifest.get('approved_at_utc'))
    provider = manifest.get('provider')
    source = queue.registry.sources.get(provider) if isinstance(provider, str) else None
    if (not _reviewed_source_scope(source)
            or manifest.get('data_fingerprint') != source.data_fingerprint
            or manifest.get('authorization_fingerprint') != source.authorization_fingerprint):
        raise ValueError('reviewed native current source scope invalid')
    if not isinstance(manifest.get('observation_report'), str):
        raise ValueError('reviewed native report path required')
    report_path = (manifest_path.parent/manifest['observation_report']).resolve()
    if not report_path.is_relative_to(manifest_path.parent):
        raise ValueError('reviewed native report outside approved scope')
    report, report_bytes = _json(report_path, 8 << 20)
    if hashlib.sha256(report_bytes).hexdigest() != manifest.get('observation_report_sha256'):
        raise ValueError('reviewed native observation report changed')
    probes = report.get('probes', report.get('results'))
    if (not isinstance(probes, list) or not 1 <= len(probes) <= 1024
            or any(not isinstance(probe, dict) for probe in probes)):
        raise ValueError('reviewed native observation report invalid')
    diagnostic_root = manifest_path.parent/'private-source-diagnostics'
    prepared, total_bytes, encoded_bytes, decoded_bytes = [], 0, 0, 0
    started = time.monotonic()
    for item in manifest['entries']:
        url = item['url']
        if queue.registry.require(url).provider != provider:
            raise ValueError('reviewed native source differs from approved provider')
        matching = [p for p in probes if p.get('url') == url]
        if len(matching) != 1:
            raise ValueError('reviewed native original observation missing or duplicated')
        probe = matching[0]
        if (not isinstance(item.get('diagnostic_path'), str)
                or not isinstance(probe.get('private_diagnostic_path'), str)):
            raise ValueError('reviewed native diagnostic path required')
        path = (manifest_path.parent/item['diagnostic_path']).resolve()
        if not path.is_relative_to(diagnostic_root.resolve()):
            raise ValueError('reviewed native diagnostic outside approved scope')
        if (probe.get('provider') != provider or probe.get('status') != 200
                or probe.get('response_sha256') != item.get('response_sha256')
                or Path(probe.get('private_diagnostic_path', '')).resolve() != path):
            raise ValueError('reviewed native original status or identity invalid')
        if path.stat().st_size > 10 << 20:
            raise ValueError('reviewed native diagnostic exceeds bound')
        with path.open('rb') as stream:
            payload = stream.read((10 << 20) + 1)
        if len(payload) > 10 << 20:
            raise ValueError('reviewed native diagnostic changed beyond bound')
        encoded_bytes += len(payload)
        if encoded_bytes > _MAX_RAW:
            raise ValueError('reviewed native aggregate encoded bound exceeded')
        receipt, receipt_bytes = _json(path.with_suffix('.receipt.json'), 65536)
        digest = hashlib.sha256(payload).hexdigest()
        if (digest != item.get('diagnostic_sha256') or digest != receipt.get('sha256')
                or hashlib.sha256(receipt_bytes).hexdigest() != item.get('diagnostic_receipt_sha256')
                or receipt.get('response_sha256') != item.get('response_sha256')
                or receipt.get('diagnostic_only') is not True or receipt.get('accepted_as_article') is not False
                or receipt.get('publish') is not False):
            raise ValueError('reviewed native diagnostic receipt invalid')
        parquet = pq.ParquetFile(pa.BufferReader(payload))
        if (parquet.metadata.num_rows != 1 or parquet.metadata.num_row_groups != 1
                or parquet.metadata.row_group(0).total_byte_size > _MAX_RAW):
            raise ValueError('reviewed native diagnostic grain or memory bound invalid')
        decoded_bytes += parquet.metadata.row_group(0).total_byte_size
        if decoded_bytes > _MAX_RAW:
            raise ValueError('reviewed native aggregate decoded bound exceeded')
        raw = parquet.read(use_threads=False).to_pylist()[0]
        body = raw.get('raw_response_body')
        if not isinstance(body, bytes):
            raise ValueError('reviewed native original bytes missing')
        total_bytes += len(body)
        if total_bytes > _MAX_RAW:
            raise ValueError('reviewed native aggregate response bound exceeded')
        if (raw.get('provider') != provider or raw.get('request_url') != url or raw.get('final_url') != url
                or raw.get('response_sha256') != item['response_sha256']
                or hashlib.sha256(body).hexdigest() != item['response_sha256']
                or raw.get('diagnostic_only') is not True or raw.get('accepted_as_article') is not False
                or raw.get('publish') is not False):
            raise ValueError('reviewed native original response identity invalid')
        observed = _clock(raw.get('captured_at_utc'))
        headers = json.loads(raw['headers_json'])
        original_auth = json.loads(raw['authorization_json'])
        if (raw['captured_at_utc'] != item.get('captured_at_utc') or observed > approved
                or not isinstance(headers, dict)
                or any(not isinstance(k, str) or not isinstance(v, str) for k, v in headers.items())
                or not isinstance(original_auth, dict) or original_auth != item.get('original_authorization')
                or {k: v for k, v in original_auth.items() if k != 'scope'} != {
                    k: v for k, v in source.authorization.items() if k != 'scope'}):
            raise ValueError('reviewed native original authorization or HTTP clock invalid')
        response = Response(url, 200, {k.lower(): v for k, v in headers.items()}, body)
        parse_started = time.monotonic()
        with queue.metrics.measure('article_extraction', provider):
            article = extract_article_response(response, source)
        if any(article.get(key) != item.get(key) for key in (
                'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw')):
            raise ValueError('reviewed native approved extraction changed')
        head = queue.db.execute('SELECT * FROM articles WHERE url=? AND provider=?', (url, provider)).fetchone()
        if head is None:
            raise ValueError('reviewed native original absent from canonical frontier')
        head = dict(head)
        record = article_response_record(source, head, response, article,
            captured_at_utc=raw['captured_at_utc'], http_duration_seconds=None,
            parse_duration_seconds=time.monotonic()-parse_started, duration_seconds=time.monotonic()-parse_started,
            resolution_basis='reviewed_native_diagnostic_original; literal_request_and_final_URL_unchanged')
        private = 'reviewed_native_sources/'+digest+'.parquet'
        record.update(acquisition_transport='retained_raw_reparse',
            source_observed_at_utc=raw['captured_at_utc'], reparsed_at_utc=utc_now(),
            replay_source_parquet=private, reviewed_native_replay_contract_version=RETAINED_NATIVE_REPLAY_VERSION,
            original_diagnostic_authorization_json=json.dumps(original_auth, ensure_ascii=False, sort_keys=True),
            metadata_title_raw=head.get('title'), historical_point_in_time=False)
        prepared.append((url, head, record, private, payload, receipt_bytes))
    # Store the explicit approval and originals by content hash. The ordinary
    # body receipt/recovery path references them; no second queue or writer.
    scope_sha = hashlib.sha256(manifest_bytes).hexdigest()
    scope_relative = 'reviewed_native_sources/approvals/'+scope_sha+'.json'
    _immutable(queue.root, scope_relative, manifest_bytes)
    report_relative = 'reviewed_native_sources/observations/'+manifest['observation_report_sha256']+'.json'
    _immutable(queue.root, report_relative, report_bytes)
    records, skipped = [], []
    for url, head, record, private, payload, receipt_bytes in prepared:
        if head['body_sha256'] or head['state'] not in {'pending', 'retry', 'retry_cooldown'}:
            skipped.append(url)
            continue
        if head.get('last_checked_at_utc'):
            try:
                latest = _clock(head['last_checked_at_utc'])
            except ValueError:
                skipped.append(url)
                continue
            if latest > _clock(record['source_observed_at_utc']):
                skipped.append(url)
                continue
        _immutable(queue.root, private, payload)
        _immutable(queue.root, private+'.diagnostic-receipt.json', receipt_bytes)
        claimed = queue.claim_url(url, lease_seconds, expected_missing_head={
            field: head[field] for field in RETAINED_SUPPLEMENT_HEAD_FIELDS})
        if claimed is None:
            skipped.append(url)
            continue
        witness = queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
        record.update(reviewed_native_approval_path=scope_relative, reviewed_native_scope_sha256=scope_sha,
            reviewed_native_observation_report_path=report_relative,
            reviewed_native_head_json=json.dumps({field: witness[field] for field in RETAINED_SUPPLEMENT_HEAD_FIELDS}, sort_keys=True))
        records.append(record)
    queue.accept(records)
    materialized = sum(queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?',
        (record['requested_url'],)).fetchone()[0] == record['body_sha256'] for record in records)
    queue.metrics.count(provider, 'reviewed_native_materialized_heads', materialized)
    return {'contract_version': RETAINED_NATIVE_REPLAY_VERSION, 'selected_urls': len(urls),
        'materialized_bodies': materialized, 'skipped_urls': skipped, 'publisher_requests_this_command': 0,
        'original_response_bytes': total_bytes, 'scope_sha256': scope_sha,
        'original_observation_clocks_preserved': True, 'wall_seconds': time.monotonic()-started,
        'all_publisher_history_complete': False, 'publish': False, 'remote_training_export': False}


def reviewed_native_head_matches(queue, record):
    """Recheck the immutable review and exact missing-head witness on commit."""
    from downloader.news_fulltext_queue import source_path
    if (type(record.get('reviewed_native_replay_contract_version')) is not int
            or record.get('reviewed_native_replay_contract_version') != RETAINED_NATIVE_REPLAY_VERSION
            or record.get('acquisition_transport') != 'retained_raw_reparse'):
        raise ValueError('invalid reviewed native acceptance contract')
    expected = json.loads(record['reviewed_native_head_json'])
    if (set(expected) != set(RETAINED_SUPPLEMENT_HEAD_FIELDS) or expected['state'] != 'inflight'
            or expected['body_sha256'] is not None or not expected['lease_owner']
            or expected['url'] != record['requested_url'] or expected['provider'] != record['provider']):
        raise ValueError('invalid reviewed native missing-head witness')
    try:
        source = queue.registry.require(record['requested_url'])
    except NewsError:
        return False
    if (not _reviewed_source_scope(source)
            or source.data_fingerprint != record['data_fingerprint']
            or source.authorization_fingerprint != record['authorization_fingerprint']):
        return False
    scope, payload = _json(source_path(queue.root, record['reviewed_native_approval_path']), 65536)
    if (hashlib.sha256(payload).hexdigest() != record['reviewed_native_scope_sha256']
            or scope.get('scope_kind') != _KIND or type(scope.get('contract_version')) is not int
            or scope.get('contract_version') != RETAINED_NATIVE_REPLAY_VERSION
            or scope.get('provider') != source.provider or scope.get('data_fingerprint') != source.data_fingerprint
            or scope.get('authorization_fingerprint') != source.authorization_fingerprint
            or scope.get('publish') is not False or scope.get('remote_training_export') is not False
            or scope.get('diagnostic_scope_rewording_reviewed') is not True):
        raise ValueError('reviewed native immutable approval invalid')
    entry = [item for item in scope['entries'] if item.get('url') == record['requested_url']]
    if (len(entry) != 1 or any(record.get(key) != entry[0].get(key) for key in (
            'response_sha256', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw', 'captured_at_utc'))
            or json.loads(record['original_diagnostic_authorization_json']) != entry[0].get('original_authorization')):
        raise ValueError('reviewed native record differs from approved original')
    head = queue.db.execute('SELECT * FROM articles WHERE id=?', (bytes.fromhex(record['article_id']),)).fetchone()
    if head is None:
        return False
    if all(head[key] == expected[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS):
        return True
    restarted = {'state', 'due', 'error_code', 'lease_owner', 'lease_until'}
    return (head['state'] == 'retry' and head['due'] == 0 and head['error_code'] == 'owner_restart'
        and head['lease_owner'] is None and head['lease_until'] is None
        and all(head[key] == expected[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS if key not in restarted))
