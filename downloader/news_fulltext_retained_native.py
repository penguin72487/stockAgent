"""Explicit, bounded reuse of reviewed public native diagnostic originals.

This is a local acceptance path on the canonical queue, not another crawler.
Original observation/authentication evidence stays immutable; current source
scope and native validation are checked again. Old evidence cannot overwrite
a newer head, refusal, lease, or completed body during receipt recovery.
"""
from __future__ import annotations

from datetime import datetime, UTC
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re
import time

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_policy import NewsError, normalize_url


RETAINED_NATIVE_REPLAY_VERSION = 1
_KIND = 'reviewed_native_diagnostic_materialization_v1'
RESIDENT_NATIVE_REPLAY_VERSION = 2
_RESIDENT_KIND = 'reviewed_resident_native_materialization_v2'
FINITE_NATIVE_REPLAY_VERSION = 3
_FINITE_KIND = 'reviewed_finite_native_materialization_v3'
_MAX_RAW = 16 << 20


def _approval_kind(version):
    return {RETAINED_NATIVE_REPLAY_VERSION: _KIND, RESIDENT_NATIVE_REPLAY_VERSION: _RESIDENT_KIND,
        FINITE_NATIVE_REPLAY_VERSION: _FINITE_KIND}.get(version)


def _scope_reviewed(scope, version):
    if version == FINITE_NATIVE_REPLAY_VERSION:
        return (scope.get('diagnostic_scope_rewording_reviewed') is True
            and scope.get('original_author_scope_evidence_addition_reviewed') is True)
    return (scope.get('diagnostic_scope_rewording_reviewed') is True if version == 1 else
        scope.get('diagnostic_scope_unchanged') is True and scope.get('parser_transition_reviewed') is True)


def _article_digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def _resident_original(queue, source, item, probe, approved):
    """Read the canonical evidence17 original; never synthesize HTTP or auth.

    This closed source transition does not generalize old diagnostic policies
    into new permissions. The writer's observation clock is retained as such;
    the original did not store a receive clock or authorization JSON.
    """
    from downloader.news_fulltext_queue import source_path
    from downloader.news_fulltext_taiwannews import (
        TAIWANNEWS_INLINE_ORIGINAL_QUESTION, TAIWANNEWS_V2_LAYOUT, TAIWANNEWS_V3_HTTP_BINDING,
        private_taiwannews_inline_original_permit, taiwannews_resident_source,
    )
    identifier = item.get('request_id')
    if (not taiwannews_resident_source(source) or not isinstance(identifier, str)
            or not re.fullmatch('[0-9a-f]{64}', identifier)):
        raise ValueError('reviewed resident source transition invalid')
    relative = 'reviewed_repairs/candidate_diagnostics/originals/'+identifier+'.parquet'
    receipt_relative = relative.removesuffix('.parquet')+'.receipt.json'
    reference = {'path': relative, 'sha256': item.get('diagnostic_sha256'), 'receipt_path': receipt_relative,
        'response_sha256': item.get('response_sha256'), 'response_bytes': item.get('response_bytes')}
    if (item.get('diagnostic_path') != relative or item.get('diagnostic_receipt_path') != receipt_relative
            or probe.get('request_id') != identifier or probe.get('url') != item['url']
            or probe.get('reference') != reference or probe.get('accepted_as_body') is not False
            or probe.get('receipt_sha256') != item.get('diagnostic_receipt_sha256')):
        raise ValueError('reviewed resident original reference invalid')
    job = queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (identifier,)).fetchone()
    if (job is None or job['state'] != 'observed' or job['provider'] != source.provider
            or _article_digest(dict(job)) != item.get('candidate_job_sha256')
            or hashlib.sha256(job['request_json'].encode()).hexdigest() != identifier):
        raise ValueError('reviewed resident completed candidate changed')
    request, result = json.loads(job['request_json']), json.loads(job['result_json'])
    if (request.get('evidence_contract_version') != 17 or request.get('candidate_layout') != TAIWANNEWS_V2_LAYOUT
            or request.get('candidate_fingerprint') != TAIWANNEWS_V3_HTTP_BINDING[0]
            or request.get('target', {}).get('url') != item['url']
            or request.get('source', {}).get('data_fingerprint') != TAIWANNEWS_V3_HTTP_BINDING[0]
            or request.get('source', {}).get('authorization_fingerprint') != source.authorization_fingerprint
            or result.get('private_original') != reference or result.get('accepted_as_article') is not False
            or result.get('raw_or_names_or_body_retained') is not True):
        raise ValueError('reviewed resident candidate witness invalid')
    path = source_path(queue.root, relative)
    if path.stat().st_size > 10 << 20:
        raise ValueError('reviewed resident diagnostic exceeds bound')
    with path.open('rb') as stream:
        payload = stream.read((10 << 20)+1)
    if len(payload) > 10 << 20 or hashlib.sha256(payload).hexdigest() != reference['sha256']:
        raise ValueError('reviewed resident diagnostic changed')
    receipt, receipt_bytes = _json(source_path(queue.root, receipt_relative), 65536)
    if (hashlib.sha256(receipt_bytes).hexdigest() != item.get('diagnostic_receipt_sha256')
            or receipt.get('contract_version') != 1 or receipt.get('request_id') != identifier
            or receipt.get('question') != TAIWANNEWS_INLINE_ORIGINAL_QUESTION or receipt.get('original') != reference):
        raise ValueError('reviewed resident diagnostic receipt invalid')
    parquet = pq.ParquetFile(pa.BufferReader(payload))
    if (parquet.metadata.num_rows != 1 or parquet.metadata.num_row_groups != 1
            or parquet.metadata.row_group(0).total_byte_size > _MAX_RAW):
        raise ValueError('reviewed resident diagnostic grain or memory bound invalid')
    raw, = parquet.read(use_threads=False).to_pylist()
    body = raw.get('raw_response_body')
    if (not isinstance(body, bytes) or not 0 < len(body) <= 2 << 20 or b'\x00' in body
            or len(body) != item.get('response_bytes') or raw.get('request_id') != identifier
            or raw.get('provider') != source.provider or raw.get('request_url') != item['url']
            or raw.get('final_url') != probe.get('final_url') or raw.get('source_document_url') != raw.get('final_url')
            or raw.get('response_sha256') != item.get('response_sha256')
            or hashlib.sha256(body).hexdigest() != item.get('response_sha256')
            or raw.get('data_fingerprint') != TAIWANNEWS_V3_HTTP_BINDING[0]
            or raw.get('authorization_fingerprint') != source.authorization_fingerprint
            or any(raw.get(k) is not v or receipt.get(k) is not v for k, v in {
                'diagnostic_only': True, 'accepted_as_article': False, 'publish': False,
                'historical_point_in_time': False, 'per_article_license_verified': False,
                'training_injection_enabled': False, 'remote_training_export': False}.items())):
        raise ValueError('reviewed resident original bytes or scope invalid')
    observed = _clock(raw.get('observed_at_utc'))
    if raw['observed_at_utc'] != item.get('captured_at_utc') or observed > approved:
        raise ValueError('reviewed resident writer observation clock invalid')
    headers = json.loads(raw['headers_json'])
    hops = json.loads(raw['transport_redirects_json'])
    if (not isinstance(headers, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in headers.items())
            or not isinstance(hops, list) or len(hops) > 1):
        raise ValueError('reviewed resident transport evidence invalid')
    try:
        redirects = tuple(RedirectHop(**hop) for hop in hops)
    except (TypeError, ValueError):
        raise ValueError('reviewed resident redirect shape invalid') from None
    for hop in redirects:
        if (type(hop.status) is not int or not isinstance(hop.response_sha256, str)
                or not re.fullmatch('[0-9a-f]{64}', hop.response_sha256)
                or type(hop.response_bytes) is not int or not 0 <= hop.response_bytes <= 2 << 20
                or _clock(hop.observed_at_utc) > observed):
            raise ValueError('reviewed resident redirect evidence invalid')
    response = Response(raw['final_url'], 200, {k.lower(): v for k, v in headers.items()}, body, redirects)
    captured_source = replace(source, embedded_article_layout=TAIWANNEWS_V2_LAYOUT,
        data_fingerprint=TAIWANNEWS_V3_HTTP_BINDING[0])
    expected = private_taiwannews_inline_original_permit(response, captured_source, item['url'])
    if receipt.get('permit') != expected:
        raise ValueError('reviewed resident original source permit invalid')
    return raw, response, payload, receipt_bytes, parquet.metadata.row_group(0).total_byte_size


def _reviewed_source_scope(source, version=RETAINED_NATIVE_REPLAY_VERSION):
    """Existing known originals, plus the already-reviewed CMNews SSR route.

    A discoverable provider is not a generic retained-response grant. CMNews
    keeps its exact current native container, byline and private-reading scope;
    explicit URL/frontier, immutable approval and source checks still apply.
    """
    if version == FINITE_NATIVE_REPLAY_VERSION:
        # These are individually reviewed native contracts, not a generic
        # permission for every finite provider or related publisher subsite.
        namespace = {
            'gvm': ('www.gvm.com.tw', 'gvm_public_native_article_v1', 'https://www.gvm.com.tw/copyright'),
            'cw': ('www.cw.com.tw', 'cw_public_editor_compilation_v1', 'https://www.cw.com.tw/copyright'),
            'businessweekly_wealth': ('wealth.businessweekly.com.tw', 'businessweekly_wealth_public_written_v1',
                'https://www.businessweekly.com.tw/CopyRight'),
            'knews': ('www.knews.com.tw', 'knews_public_native_article_v1', 'https://www.knews.com.tw/'),
            'setn': ('www.setn.com', 'setn_public_native_article_v1', 'https://www.setn.com/'),
        }.get(source.provider if source else None)
        return bool(source and source.allowed and namespace
            and source.domains == (namespace[0],) and source.acquisition_mode == 'finite'
            and source.finite_article_urls and not source.allow_reviewed_supplements
            and source.embedded_article_layout == namespace[1]
            and source.authorization.get('kind') == 'personal_research'
            and source.authorization.get('evidence_url') == namespace[2]
            and source.authorization.get('original_author_scope_evidence_urls') == list(source.finite_article_urls))
    cmnews = bool(source and source.provider == 'cmnews'
        and source.domains == ('cmnews.com.tw',)
        and source.embedded_article_layout == 'cmnews_visible_article_v1'
        and source.selectors == ('.articleContent__textContent',)
        and source.require_configured_container and source.require_author_attribution)
    return bool(source and source.allowed and (source.known_url_only or cmnews)
        and source.acquisition_mode == 'continuous' and source.embedded_article_layout
        and source.authorization.get('kind') == 'personal_research')


def _finite_membership(queue, source, urls):
    """The explicit approval and frozen canonical cohort must be the same set."""
    if tuple(urls) != source.finite_article_urls:
        raise ValueError('reviewed finite selection differs from literal source scope')
    members = tuple(row[0] for row in queue.db.execute(
        'SELECT url FROM finite_cohort_members WHERE provider=? ORDER BY url', (source.provider,)))
    if members != source.finite_article_urls:
        raise ValueError('reviewed finite selection differs from immutable cohort')
    from downloader.news_fulltext_queue import source_path
    header = queue.db.execute('SELECT * FROM finite_cohorts WHERE provider=?', (source.provider,)).fetchone()
    if header is None:
        raise ValueError('reviewed finite cohort receipt missing')
    receipt, payload = _json(source_path(queue.root, header['receipt_path']), 65536)
    digest = hashlib.sha256(''.join(url+'\n' for url in members).encode()).hexdigest()
    if (hashlib.sha256(payload).hexdigest() != header['receipt_sha256']
            or receipt.get('contract_version') != 1 or receipt.get('provider') != source.provider
            or receipt.get('urls') != len(members) or header['urls'] != len(members)
            or receipt.get('urls_sha256') != digest or header['urls_sha256'] != digest):
        raise ValueError('reviewed finite cohort receipt invalid')


def _original_authorization_matches(original, source, version):
    ignored = {'scope'}
    if version == FINITE_NATIVE_REPLAY_VERSION:
        # Only the reviewed addition of these same original URLs is allowed;
        # an old explicit, different author grant cannot be erased/replaced.
        key = 'original_author_scope_evidence_urls'
        if key in original and original[key] != list(source.finite_article_urls):
            return False
        ignored.add(key)
    return ({k: v for k, v in original.items() if k not in ignored}
        == {k: v for k, v in source.authorization.items() if k not in ignored})


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
    ABI1 permits only an explicitly reviewed personal-reading scope rewording,
    preserving both exact policies. ABI2 reuses canonical resident originals
    only for the attested same-authorization native parser transition; it
    preserves the actual redirect, writer clock and absent auth-JSON facts.
    Other authorization changes require fresh review, not this import path.
    """
    from downloader.news_fulltext import article_response_record, extract_article_response
    from downloader.news_fulltext_queue import utc_now
    urls = sorted(set(normalize_url(url) for url in urls))
    if not 1 <= len(urls) <= 16 or type(lease_seconds) is not int or lease_seconds <= 0:
        raise ValueError('bounded explicit reviewed native selection required')
    manifest_path = Path(manifest_path).resolve()
    manifest, manifest_bytes = _json(manifest_path, 65536)
    version = manifest.get('contract_version')
    if (type(version) is not int or _approval_kind(version) is None
            or manifest.get('scope_kind') != _approval_kind(version) or manifest.get('publish') is not False
            or manifest.get('remote_training_export') is not False
            or not _scope_reviewed(manifest, version)
            or not isinstance(manifest.get('entries'), list)
            or len(manifest['entries']) != len(urls)
            or any(not isinstance(entry, dict) for entry in manifest['entries'])
            or [entry.get('url') for entry in manifest['entries']] != urls):
        raise ValueError('reviewed native approval scope invalid')
    approved = _clock(manifest.get('approved_at_utc'))
    provider = manifest.get('provider')
    source = queue.registry.sources.get(provider) if isinstance(provider, str) else None
    if (not _reviewed_source_scope(source, version)
            or manifest.get('data_fingerprint') != source.data_fingerprint
            or manifest.get('authorization_fingerprint') != source.authorization_fingerprint):
        raise ValueError('reviewed native current source scope invalid')
    if version == FINITE_NATIVE_REPLAY_VERSION:
        _finite_membership(queue, source, urls)
    if not isinstance(manifest.get('observation_report'), str):
        raise ValueError('reviewed native report path required')
    report_path = (manifest_path.parent/manifest['observation_report']).resolve()
    if not report_path.is_relative_to(manifest_path.parent):
        raise ValueError('reviewed native report outside approved scope')
    report, report_bytes = _json(report_path, 8 << 20)
    if hashlib.sha256(report_bytes).hexdigest() != manifest.get('observation_report_sha256'):
        raise ValueError('reviewed native observation report changed')
    probes = (report.get('private_originals') if version == RESIDENT_NATIVE_REPLAY_VERSION
              else report.get('probes', report.get('results')))
    if (not isinstance(probes, list) or not 1 <= len(probes) <= 1024
            or any(not isinstance(probe, dict) for probe in probes)):
        raise ValueError('reviewed native observation report invalid')
    diagnostic_root = manifest_path.parent/'private-source-diagnostics'
    explicit_directory = 'diagnostic_directory' in manifest
    if version == FINITE_NATIVE_REPLAY_VERSION or explicit_directory:
        directory = manifest.get('diagnostic_directory')
        if (not isinstance(directory, str) or directory in {'', '.', '..'}
                or Path(directory).name != directory or version == RESIDENT_NATIVE_REPLAY_VERSION
                or version == RETAINED_NATIVE_REPLAY_VERSION
                and (type(manifest.get('diagnostic_directory_contract_version')) is not int
                     or manifest['diagnostic_directory_contract_version'] != 1)):
            raise ValueError('reviewed native diagnostic directory invalid')
        diagnostic_root = manifest_path.parent/directory
        if not diagnostic_root.resolve().is_relative_to(manifest_path.parent):
            raise ValueError('reviewed native diagnostic directory outside approved scope')
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
        if version == RESIDENT_NATIVE_REPLAY_VERSION:
            raw, response, payload, receipt_bytes, decoded = _resident_original(queue, source, item, probe, approved)
            encoded_bytes += len(payload)
            decoded_bytes += decoded
            total_bytes += len(response.body)
            if max(encoded_bytes, decoded_bytes, total_bytes) > _MAX_RAW:
                raise ValueError('reviewed resident aggregate memory bound exceeded')
            parse_started = time.monotonic()
            with queue.metrics.measure('article_extraction', provider):
                article = extract_article_response(response, source)
            if (sorted(article) != item.get('article_keys') or _article_digest(article) != item.get('article_fields_sha256')
                    or any(article.get(key) != item.get(key) for key in (
                        'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw'))
                    or article.get('attribution_status') != 'source_declared_corporate_author_not_verified_person'):
                raise ValueError('reviewed resident approved extraction changed')
            head = queue.db.execute('SELECT * FROM articles WHERE url=? AND provider=?', (url, provider)).fetchone()
            if head is None:
                raise ValueError('reviewed native original absent from canonical frontier')
            head = dict(head)
            record = article_response_record(source, head, response, article,
                captured_at_utc=raw['observed_at_utc'], http_duration_seconds=None,
                parse_duration_seconds=time.monotonic()-parse_started, duration_seconds=time.monotonic()-parse_started,
                resolution_basis='reviewed_resident_original; actual_same_ID_transport_preserved_not_alias_inference')
            record.update(acquisition_transport='retained_raw_reparse', source_observed_at_utc=raw['observed_at_utc'],
                source_observation_clock_basis='candidate_private_original_writer_observation_not_HTTP_receive_or_historical_publication',
                reparsed_at_utc=utc_now(), replay_source_parquet=item['diagnostic_path'],
                reviewed_native_replay_contract_version=version, original_diagnostic_authorization_json=None,
                original_diagnostic_authorization_fingerprint=raw['authorization_fingerprint'],
                original_diagnostic_authorization_basis='captured_fingerprint; original_authorization_JSON_not_stored',
                metadata_title_raw=head.get('title'), historical_point_in_time=False)
            _resident_entry(source, manifest, record)
            # The original already belongs to this canonical root. The shared
            # durable acceptance writer must reference it, not create another
            # raw archive or diagnostics/HTTP owner.
            prepared.append((url, head, record, item['diagnostic_path'], None, None))
            continue
        if (not isinstance(item.get('diagnostic_path'), str)
                or not isinstance(probe.get('private_diagnostic_path'), str)):
            raise ValueError('reviewed native diagnostic path required')
        path = (manifest_path.parent/item['diagnostic_path']).resolve()
        if not path.is_relative_to(diagnostic_root.resolve()):
            raise ValueError('reviewed native diagnostic outside approved scope')
        if (version == FINITE_NATIVE_REPLAY_VERSION or explicit_directory) and any(
                target.stat().st_mode & 0o077 for target in (diagnostic_root, path, path.with_suffix('.receipt.json'))):
            raise ValueError('reviewed native original is not private')
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
                or not _original_authorization_matches(original_auth, source, version)):
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
            replay_source_parquet=private, reviewed_native_replay_contract_version=version,
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
        item = next(item for item in manifest['entries'] if item['url'] == url)
        resident = version == RESIDENT_NATIVE_REPLAY_VERSION
        if (head['body_sha256'] or head.get('lease_owner') is not None or head.get('lease_until') is not None
                or (not resident and head['state'] not in {'pending', 'retry', 'retry_cooldown'})
                or resident and any(head[field] != item.get('expected_missing_head', {}).get(field)
                    for field in RETAINED_SUPPLEMENT_HEAD_FIELDS)):
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
        if payload is not None:
            _immutable(queue.root, private, payload)
            _immutable(queue.root, private+'.diagnostic-receipt.json', receipt_bytes)
        record.update(reviewed_native_approval_path=scope_relative, reviewed_native_scope_sha256=scope_sha,
            reviewed_native_observation_report_path=report_relative)
        kwargs = {'reviewed_native_record': record} if resident else {}
        claimed = queue.claim_url(url, lease_seconds, expected_missing_head={
            field: head[field] for field in RETAINED_SUPPLEMENT_HEAD_FIELDS}, **kwargs)
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
    return {'contract_version': version, 'selected_urls': len(urls),
        'materialized_bodies': materialized, 'skipped_urls': skipped, 'publisher_requests_this_command': 0,
        'original_response_bytes': total_bytes, 'scope_sha256': scope_sha,
        'original_observation_clocks_preserved': True, 'wall_seconds': time.monotonic()-started,
        'all_publisher_history_complete': False, 'publish': False, 'remote_training_export': False}


def _resident_entry(source, scope, record):
    from downloader.news_fulltext_taiwannews import taiwannews_resident_source
    if not taiwannews_resident_source(source):
        raise ValueError('reviewed resident current source invalid')
    entries = scope.get('entries')
    if not isinstance(entries, list) or not 1 <= len(entries) <= 16:
        raise ValueError('reviewed resident approval entries invalid')
    matching = [item for item in entries if isinstance(item, dict) and item.get('url') == record['requested_url']]
    if len(matching) != 1:
        raise ValueError('reviewed resident approval entry missing or duplicated')
    item = matching[0]
    keys = item.get('article_keys')
    before = item.get('expected_missing_head')
    if (not isinstance(keys, list) or not 1 <= len(keys) <= 128
            or any(not isinstance(key, str) or key not in record for key in keys) or keys != sorted(set(keys))
            or _article_digest({key: record[key] for key in keys}) != item.get('article_fields_sha256')
            or not isinstance(before, dict) or set(before) != set(RETAINED_SUPPLEMENT_HEAD_FIELDS)
            or before['url'] != record['requested_url'] or before['provider'] != source.provider
            or before['state'] != 'article_title_not_verified' or before['error_code'] != 'article_title_not_verified'
            or before['body_sha256'] is not None or before['lease_owner'] is not None or before['lease_until'] is not None
            or record.get('original_diagnostic_authorization_json') is not None
            or record.get('original_diagnostic_authorization_fingerprint') != source.authorization_fingerprint
            or record.get('replay_source_parquet') != item.get('diagnostic_path')
            or any(record.get(key) != item.get(key) for key in (
                'response_sha256', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw', 'captured_at_utc'))):
        raise ValueError('reviewed resident record differs from approved original')
    return item


def reviewed_native_missing_head_allowed(queue, record, expected):
    """Only a fully approved local v2 repair can fence a terminal title error.

    Refusals, leases, future deadlines and every other source remain governed
    by the ordinary exact claim. This does not reset any failure in advance.
    """
    from downloader.news_fulltext_queue import source_path
    if (record.get('reviewed_native_replay_contract_version') != RESIDENT_NATIVE_REPLAY_VERSION
            or record.get('acquisition_transport') != 'retained_raw_reparse'):
        return False
    source = queue.registry.require(record['requested_url'])
    scope, payload = _json(source_path(queue.root, record['reviewed_native_approval_path']), 65536)
    if (hashlib.sha256(payload).hexdigest() != record['reviewed_native_scope_sha256']
            or scope.get('contract_version') != RESIDENT_NATIVE_REPLAY_VERSION or scope.get('scope_kind') != _RESIDENT_KIND
            or scope.get('provider') != source.provider or scope.get('data_fingerprint') != source.data_fingerprint
            or scope.get('authorization_fingerprint') != source.authorization_fingerprint
            or record.get('data_fingerprint') != source.data_fingerprint
            or record.get('authorization_fingerprint') != source.authorization_fingerprint
            or scope.get('publish') is not False or scope.get('remote_training_export') is not False
            or not _scope_reviewed(scope, RESIDENT_NATIVE_REPLAY_VERSION)):
        raise ValueError('reviewed resident missing-head approval invalid')
    entry = _resident_entry(source, scope, record)
    return expected == entry['expected_missing_head']


def reviewed_native_head_matches(queue, record):
    """Recheck the immutable review and exact missing-head witness on commit."""
    from downloader.news_fulltext_queue import source_path
    version = record.get('reviewed_native_replay_contract_version')
    if (type(version) is not int or _approval_kind(version) is None
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
    if (not _reviewed_source_scope(source, version)
            or source.data_fingerprint != record['data_fingerprint']
            or source.authorization_fingerprint != record['authorization_fingerprint']):
        return False
    scope, payload = _json(source_path(queue.root, record['reviewed_native_approval_path']), 65536)
    if (hashlib.sha256(payload).hexdigest() != record['reviewed_native_scope_sha256']
            or scope.get('scope_kind') != _approval_kind(version) or type(scope.get('contract_version')) is not int
            or scope.get('contract_version') != version
            or scope.get('provider') != source.provider or scope.get('data_fingerprint') != source.data_fingerprint
            or scope.get('authorization_fingerprint') != source.authorization_fingerprint
            or scope.get('publish') is not False or scope.get('remote_training_export') is not False
            or not _scope_reviewed(scope, version)):
        raise ValueError('reviewed native immutable approval invalid')
    if version == FINITE_NATIVE_REPLAY_VERSION:
        _finite_membership(queue, source, [item.get('url') for item in scope['entries']])
        if record['requested_url'] not in source.finite_article_urls:
            raise ValueError('reviewed finite record outside literal scope')
    entry = [item for item in scope['entries'] if item.get('url') == record['requested_url']]
    if version == RESIDENT_NATIVE_REPLAY_VERSION:
        item = _resident_entry(source, scope, record)
        before = item['expected_missing_head']
        changed = {'state', 'attempts', 'lease_owner', 'lease_until'}
        if (expected['attempts'] != before['attempts']+1
                or any(expected[key] != before[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS if key not in changed)):
            raise ValueError('reviewed resident inflight witness differs from approved head')
    elif (len(entry) != 1 or any(record.get(key) != entry[0].get(key) for key in (
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
