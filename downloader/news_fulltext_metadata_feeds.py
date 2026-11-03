"""Explicit publisher RSS information, not permission to acquire article bodies.

The existing HTTP/queue owner is authoritative. Metadata envelopes have their
own scope, durable receipts and clocks; article body heads are never accepted.
"""
from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import re
import time
from itertools import islice
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records

VERSION = 2
FAILURE_DIAGNOSTIC_VERSION = 1
XML_VALUE_PRESERVATION = 'literal_ascii_controls_in_declared_item_values_v1'
_XML_CONTROLS = re.compile(rb'[\x00-\x08\x0b\x0c\x0e-\x1f]')
_MAXIMUM_XML_CONTROLS = 256
LTN_FEEDS = tuple('https://news.ltn.com.tw/rss/' + name + '.xml' for name in
    ('all', 'politics', 'society', 'life', 'opinion', 'world', 'business', 'sports',
     'entertainment', 'art', 'def', 'local', 'people', 'novelty'))


def reviewed_policy(entry, config, *, data_version=VERSION):
    """No inference from an enabled article adapter or a user purpose label."""
    from downloader.news_fulltext_policy import normalize_url
    raw = entry.get('metadata_feed_policy')
    if raw is None:
        return (), {}, '', 0
    if not isinstance(raw, dict) or set(raw) != {'enabled', 'layout', 'urls', 'interval_seconds', 'authorization'}:
        raise ValueError('invalid metadata feed policy fields')
    auth, urls, delay = raw['authorization'], raw['urls'], raw['interval_seconds']
    if (type(raw['enabled']) is not bool or raw['layout'] != 'ltn_rss_information_v1'
            or entry['id'] != 'ltn' or 'ltn.com.tw' not in entry['domains']
            or not isinstance(urls, list) or not urls or len(urls) > len(LTN_FEEDS)
            or any(not isinstance(url, str) or url not in LTN_FEEDS or normalize_url(url) != url for url in urls)
            or len(set(urls)) != len(urls) or type(delay) is not int or not 60 <= delay <= 86400
            or not isinstance(auth, dict) or auth.get('kind') != 'publisher_noncommercial_rss_information'
            or auth.get('evidence_url') != 'https://service.ltn.com.tw/RSS'
            or auth.get('scope') != 'declared_rss_title_description_link_pubDate_only'
            or auth.get('publisher_credit') != '自由時報／The Liberty Times'
            or not isinstance(auth.get('reviewed_on'), str) or not auth['reviewed_on']
            or config.get('collection_purpose') != 'personal_research'):
        raise ValueError('metadata feed requires exact reviewed publisher information scope')
    if auth.get('valid_until'):
        expiry = datetime.fromisoformat(auth['valid_until'].replace('Z', '+00:00'))
        if expiry.tzinfo is None:
            raise ValueError('metadata authorization expiry must include timezone')
    if data_version not in {1, VERSION}:
        raise ValueError('unsupported metadata feed data contract')
    payload = {'version': data_version, 'layout': raw['layout'], 'urls': urls, 'authorization': auth}
    if data_version == 2:
        payload['xml_value_preservation'] = XML_VALUE_PRESERVATION
    fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return tuple(urls) if raw['enabled'] else (), dict(auth), fingerprint, delay


def compatible_fingerprints(source):
    """Only this exact policy's stricter v1; never retag its native receipts."""
    current = source.metadata_feed_fingerprint
    if not source.metadata_feeds or not source.metadata_feed_authorization:
        return (current,)
    payload = {'version': VERSION, 'layout': 'ltn_rss_information_v1',
               'urls': list(source.metadata_feeds), 'authorization': source.metadata_feed_authorization,
               'xml_value_preservation': XML_VALUE_PRESERVATION}
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    if current != expected:
        return (current,)
    payload.pop('xml_value_preservation')
    payload['version'] = 1
    return current, hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def access_policy_fingerprint(source):
    # Parser representation changes cannot erase an identical source policy's
    # measured pacing/refusals. The original key is the strict-v1 policy hash.
    return compatible_fingerprints(source)[-1]


def _xml_tree(body):
    """Strict fast path; lossless fallback ONLY in declared item value text.

    No recover mode, deletion, URL/date repair, entity expansion or changed
    source bytes. Unique tokens allow strict XML parsing then exact restoration;
    every token must appear exactly once in an item title/description text.
    """
    try:
        return ET.fromstring(body), []
    except ET.ParseError as original:
        failure = original
    controls = list(islice(_XML_CONTROLS.finditer(body), _MAXIMUM_XML_CONTROLS + 1))
    if not controls or len(controls) > _MAXIMUM_XML_CONTROLS:
        raise failure
    try:
        body.decode('utf-8')
    except UnicodeDecodeError:
        raise failure from None
    declaration_body = body[3:] if body.startswith(b'\xef\xbb\xbf') else body
    declaration = re.match(br'\s*<\?xml\s+[^?]*encoding\s*=\s*[\'"]([^\'"]+)', declaration_body, re.I)
    if declaration and declaration[1].lower() not in {b'utf-8', b'utf8'}:
        raise failure
    prefix = 'SA_XML_C0_' + hashlib.sha256(body).hexdigest() + '_'
    if prefix.encode() in body:
        raise failure
    codes = {}
    def token(match):
        identifier = prefix + str(len(codes)) + '__'
        codes[identifier] = match[0][0]
        return identifier.encode()
    try:
        tree = ET.fromstring(_XML_CONTROLS.sub(token, body))
    except ET.ParseError:
        raise failure from None
    permitted = {id(node): (number, node.tag) for number, item in enumerate(tree.findall('./channel/item'))
                 for node in item if node.tag in {'title', 'description'}}
    pattern = re.compile(re.escape(prefix) + r'[0-9]+__')
    restored = set()
    detail = []
    for node in tree.iter():
        if (prefix in node.tag or prefix in (node.tail or '')
                or any(prefix in k or prefix in v for k, v in node.attrib.items())):
            raise failure
        if prefix not in (node.text or ''):
            continue
        if id(node) not in permitted:
            raise failure
        number, field = permitted[id(node)]
        def restore(match):
            identifier = match[0]
            if identifier not in codes or identifier in restored:
                raise failure
            restored.add(identifier)
            detail.append({'item_index': number, 'field': field, 'codepoint': codes[identifier]})
            return chr(codes[identifier])
        node.text = pattern.sub(restore, node.text)
    if len(restored) != len(codes):
        # Controls in ignored comments/PI/markup must not silently disappear.
        raise failure
    return tree, detail


def require_request(registry, url, feed_url):
    from downloader.news_fulltext_policy import NewsError, normalize_url
    feed_url, url = normalize_url(feed_url), normalize_url(url)
    source = registry.match(feed_url)
    if source is None or feed_url not in source.metadata_feeds:
        raise NewsError('unregistered_metadata_feed')
    # No query/path/host redirects, home-page fetch or article fallback. Robots
    # redirects outside this exact initial origin's /robots.txt are refused.
    origin = urlsplit(feed_url)
    robots = origin.scheme + '://' + origin.netloc + '/robots.txt'
    if url not in {feed_url, robots}:
        raise NewsError('request_outside_metadata_feed_scope')
    expiry = source.metadata_feed_authorization.get('valid_until')
    if expiry and datetime.fromisoformat(expiry.replace('Z', '+00:00')) <= datetime.now(timezone.utc):
        raise NewsError('authorization_expired')
    return source


def _representation_error(response, source, feed_url, stage, *, xml_error=None, root=None, channel_count=None):
    """Typed failure facts, never response/header/article text or permission."""
    from downloader.news_fulltext_diagnostics import FailureResponse
    from downloader.news_fulltext_policy import NewsError
    from downloader.news_fulltext_queue import utc_now
    detail = {
        'stage': stage,
        'exact_feed_url': response.url == feed_url and feed_url in source.metadata_feeds,
        'http_200': response.status == 200,
        'xml_media_type': response.headers.get('content-type', '').split(';', 1)[0].lower().strip()
            in {'application/rss+xml', 'application/xml', 'text/xml'},
        'unsafe_markup': bool(re.search(br'<!\s*(?:DOCTYPE|ENTITY)\b', response.body, re.I)),
    }
    if xml_error is not None:
        detail.update(xml_error_code=xml_error.code, xml_error_position=list(xml_error.position),
                      xml10_forbidden_ascii_control_count=sum(1 for _ in _XML_CONTROLS.finditer(response.body)))
    if root is not None:
        detail.update(rss_root=root.tag == 'rss', rss_version_2=root.get('version') == '2.0')
    if channel_count is not None:
        detail['channel_count'] = channel_count
    error = NewsError('invalid_metadata_feed_representation',
                      retry=xml_error is not None, delay=300 if xml_error is not None else 0)
    error.metadata_feed_failure_details = detail
    error.failure_response = FailureResponse(response, source.provider, source.metadata_feed_fingerprint,
                                             feed_url, utc_now())
    return error


def _parse(response, source, feed_url):
    from downloader.news_fulltext_policy import NewsError, normalize_url
    if (response.url != feed_url or feed_url not in source.metadata_feeds or response.status != 200
            or response.headers.get('content-type', '').split(';', 1)[0].lower().strip()
                not in {'application/rss+xml', 'application/xml', 'text/xml'}
            or re.search(br'<!\s*(?:DOCTYPE|ENTITY)\b', response.body, re.I)):
        raise _representation_error(response, source, feed_url, 'response_envelope')
    try:
        rss, restored_controls = _xml_tree(response.body)
    except ET.ParseError as error:
        raise _representation_error(response, source, feed_url, 'xml_syntax', xml_error=error) from None
    if rss.tag != 'rss' or rss.get('version') != '2.0':
        raise _representation_error(response, source, feed_url, 'rss_root_or_version', root=rss)
    channels = rss.findall('channel')
    if len(channels) != 1:
        raise _representation_error(response, source, feed_url, 'channel_cardinality', channel_count=len(channels))
    items = channels[0].findall('item')
    if len(items) > 1000:
        raise NewsError('metadata_feed_item_bound_exceeded')
    rows = []
    for item in items:
        children = list(item)
        names = [node.tag for node in children]
        if sorted(names) != ['description', 'link', 'pubDate', 'title'] or any(len(node) for node in children):
            raise NewsError('unreviewed_metadata_feed_item_fields')
        fields = {node.tag: node.text or '' for node in children}
        url = normalize_url(fields['link'])
        host = urlsplit(url).hostname or ''
        if not (host == 'ltn.com.tw' or host.endswith('.ltn.com.tw')):
            raise NewsError('metadata_feed_item_outside_publisher')
        published = None
        try:
            clock = parsedate_to_datetime(fields['pubDate'])
            if clock.tzinfo is not None:
                published = clock.isoformat()
        except (ValueError, TypeError, OverflowError):
            pass  # Keep the source's raw clock; never invent an offset/time.
        rows.append({'url': url, 'title': fields['title'], 'description_html': fields['description'],
                     'publication_raw': fields['pubDate'], 'publication_aware': published,
                     'publisher_credit': source.metadata_feed_authorization['publisher_credit'],
                     'author': None, 'content_kind': 'rss_information_not_fulltext'})
    preservation = {'contract_version': 1, 'control_count': len(restored_controls),
                    'controls': restored_controls, 'original_values_restored': True,
                    'source_raw_bytes_modified': False} if restored_controls else None
    return rows, preservation


def parse(response, source, feed_url):
    return _parse(response, source, feed_url)[0]


def failure_key(source, feed_url):
    return 'metadata_feed_failure_observation:v1:' + source.metadata_feed_fingerprint + ':' + hashlib.sha256(feed_url.encode()).hexdigest()


def record_failure_observation(queue, task, error):
    """Latest bounded private witness on the existing canonical queue writer.

    Raw responses stay absent. Registration bounds current-fingerprint keys to
    the fourteen declared feeds. This does not reset source clocks/deadlines,
    charge/add raw reservations, grant body access or archive failure history.
    """
    from downloader.news_fulltext_diagnostics import FailureResponse
    from downloader.news_fulltext_policy import NewsError
    context = getattr(error, 'failure_response', None)
    if (task['kind'] != 'metadata_feed' or error.code != 'invalid_metadata_feed_representation'
            or not isinstance(context, FailureResponse)):
        return False
    try:
        source = require_request(queue.registry, task['url'], task['url'])
        if (task['provider'] != source.provider or context.provider != source.provider
                or context.source_fingerprint != source.metadata_feed_fingerprint
                or context.requested_url != task['url'] or context.response.url != task['url']
                or context.response.status != 200 or len(context.response.body) > 8 << 20):
            return False
        observed = datetime.fromisoformat(context.received_at_utc)
        job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()
        if (observed.utcoffset() is None or job is None or job['provider'] != source.provider
                or job['kind'] != 'metadata_feed' or job['error_code'] != error.code
                or job['attempts'] != task['attempts'] + 1
                or observed > datetime.fromisoformat(job['last_checked_at_utc'])):
            return False
        try:
            parse(context.response, source, task['url'])
        except NewsError as proved:
            if (proved.code != error.code or proved.retry != error.retry or proved.delay != error.delay
                    or getattr(proved, 'metadata_feed_failure_details', None)
                       != getattr(error, 'metadata_feed_failure_details', None)):
                return False
        else:
            return False
        value = {
            'contract_version': FAILURE_DIAGNOSTIC_VERSION, 'provider': source.provider,
            'feed_url': task['url'], 'metadata_fingerprint': source.metadata_feed_fingerprint,
            'response_sha256': hashlib.sha256(context.response.body).hexdigest(),
            'response_bytes': len(context.response.body), 'http_status': context.response.status,
            'validation_observed_at_utc': context.received_at_utc,
            'checked_at_utc': job['last_checked_at_utc'], 'attempts': job['attempts'],
            'retry_due_epoch': job['due'], 'error_code': error.code,
            'failure_details': error.metadata_feed_failure_details,
            'raw_response_retained': False, 'accepted_body_count': 0,
            'permission_granted': False, 'publish': False, 'remote_training_export': False,
            'clock_basis': 'local validator-failure observation, not HTTP-arrival or publication time',
            'observation_scope': 'latest typed failure only; not raw-response or complete failure-history archive',
        }
        encoded = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
        if len(encoded.encode()) > 4096:
            return False
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?) ON CONFLICT(key) DO UPDATE '
                         'SET value=excluded.value WHERE queue_settings.value IS NOT excluded.value',
                         (failure_key(source, task['url']), encoded))
        queue.metrics.count(source.provider, 'metadata_feed_failure_observations')
        return True
    except Exception:
        queue.metrics.count(task['provider'], 'metadata_feed_failure_diagnostic_errors')
        return False  # Best-effort diagnostics cannot undo the source failure.


def accept(queue, task, response, rows, records, errors, *, _checkpoint_id=None, _checked=None):
    """Commit durable metadata before scheduling success; safe to replay a crash."""
    from downloader.news_fulltext import (_article_links_started_after_heads, _discovery_head_fields,
                                          _locked_discovery_head_matches, _missing_article_links)
    from downloader.news_fulltext_policy import NewsError
    from downloader.news_fulltext_queue import utc_now
    from downloader.news_fulltext_metadata_checkpoint import MAX_BODY_BYTES, _MISSING_RECEIPT, _original
    import pyarrow as pa

    def verified_original(value, **options):
        try:
            return _original(queue, value, source, **options)
        except FileNotFoundError as error:
            raise NewsError('metadata_feed_receipt_mismatch', retry=True, delay=60) from error
        except (ValueError, TypeError, KeyError, OverflowError, NewsError, pa.ArrowException) as error:
            raise NewsError('metadata_feed_receipt_mismatch', retry=True, delay=60) from error

    source = require_request(queue.registry, task['url'], task['url'])
    if task['provider'] != source.provider or records or errors or response.url != task['url']:
        raise NewsError('metadata_feed_cannot_accept_article_bodies')
    fingerprint = source.metadata_feed_fingerprint
    checked = utc_now() if _checked is None else _checked
    previous = queue.db.execute('SELECT * FROM metadata_feed_heads WHERE provider=? AND feed_url=?',
                                 (source.provider, task['url'])).fetchone()
    if response.status == 304:
        if (rows or previous is None or task.get('parser_fingerprint') not in compatible_fingerprints(source)
                or previous['parser_fingerprint'] != task.get('parser_fingerprint')
                or task.get('response_sha256') != previous['response_sha256']):
            raise NewsError('unverifiable_metadata_feed_not_modified', retry=True, delay=60)
        fingerprint = previous['parser_fingerprint']  # Never relabel strict-v1 evidence on 304.
        digest = previous['response_sha256']
        relative = (f"metadata_feeds/{fingerprint}/{hashlib.sha256(task['url'].encode()).hexdigest()}/"
                    f'{digest}.parquet')
        if (not isinstance(digest, str) or re.fullmatch(r'[0-9a-f]{64}', digest) is None
                or previous['receipt_path'] != relative + '.receipt.json'
                or type(previous['rows']) is not int or not 0 <= previous['rows'] <= 1000
                or type(previous['response_bytes']) is not int
                or not 0 <= previous['response_bytes'] <= MAX_BODY_BYTES):
            raise NewsError('metadata_feed_receipt_mismatch', retry=True, delay=60)
        witness = {'head': {'url': task['url']}, 'response_sha256': digest,
                   'metadata_fingerprint': fingerprint, 'response_bytes': previous['response_bytes'],
                   'headers': {'content-type': 'application/rss+xml'}}
        _, _, retained_rows, receipt = verified_original(witness, legacy=True, details=True)
        clocks = [datetime.fromisoformat(row['publication_aware']) for row in retained_rows if row['publication_aware']]
        if (len(retained_rows) != previous['rows']
                or (min(clocks).isoformat() if clocks else None) != previous['first_publication_aware']
                or (max(clocks).isoformat() if clocks else None) != previous['last_publication_aware']):
            raise NewsError('metadata_feed_receipt_mismatch', retry=True, delay=60)
    elif response.status == 200:
        if rows:
            raise NewsError('metadata_feed_cannot_accept_preparsed_or_invented_rows')
        rows, preservation = _parse(response, source, task['url'])  # One healthy XML parse, canonical writer.
        if _checkpoint_id is None:
            from downloader.news_fulltext_metadata_checkpoint import register
            _checkpoint_id = register(queue, task, response, source, checked)
        digest = hashlib.sha256(response.body).hexdigest()
        feed_id = hashlib.sha256(task['url'].encode()).hexdigest()
        relative = f'metadata_feeds/{fingerprint}/{feed_id}/{digest}.parquet'
        target = queue.root / relative
        receipt_path = relative + '.receipt.json'
        receipt_file = queue.root / receipt_path
        target_present = target.is_file()
        if not target_present:
            # Never replace orphan evidence or recreate an original underneath
            # an existing receipt with a newly invented observation clock.
            if target.exists() or target.is_symlink() or receipt_file.exists() or receipt_file.is_symlink():
                raise NewsError('metadata_feed_receipt_mismatch', retry=True, delay=60)
            atomic_write_parquet(target, table_from_records([{
                'feed_url': task['url'], 'provider': source.provider, 'raw_response_body': response.body,
                'items_json': json.dumps(rows, ensure_ascii=False, sort_keys=True),
                'response_sha256': digest, 'metadata_fingerprint': fingerprint,
                'authorization_json': json.dumps(source.metadata_feed_authorization, ensure_ascii=False, sort_keys=True),
                'observed_at_utc': checked, 'content_kind': 'rss_information_not_fulltext',
                'article_bodies_accepted': 0, 'publish': False, 'remote_training_export': False,
            }]), compression='zstd', compression_level=9, durable=True)
        witness = {'head': {'url': task['url']}, 'response_sha256': digest,
                   'metadata_fingerprint': fingerprint, 'response_bytes': len(response.body),
                   'headers': {'content-type': response.headers.get('content-type')}}
        parsed = (response.body, rows, preservation)
        # An orphan can be sealed only after validating the source bytes,
        # identity, authorization, timestamps and parsed rows of the original.
        receipt = _MISSING_RECEIPT
        if target_present:
            _, _, _, receipt = verified_original(witness, parsed=parsed, details=True)
        if receipt is _MISSING_RECEIPT:
            atomic_write_json(receipt_file, {
                'contract_version': VERSION, 'parquet_path': relative, 'sha256': sha256_file(target),
                'response_sha256': digest, 'feed_url': task['url'], 'provider': source.provider,
                'metadata_fingerprint': fingerprint, 'rows': len(rows), 'response_bytes': len(response.body),
                'xml_value_preservation': preservation,
                'article_bodies_accepted': 0, 'publish': False, 'remote_training_export': False,
            }, durable=True)
            # Read back both files after publishing the receipt, including the
            # original's content, instead of trusting a partial write.
            _, _, _, receipt = verified_original(witness, parsed=parsed, details=True)
        target.chmod(0o600)
        receipt_file.chmod(0o600)
        clocks = [datetime.fromisoformat(row['publication_aware']) for row in rows if row['publication_aware']]
    else:
        raise NewsError('metadata_feed_invalid_acceptance_status')
    with queue.db:
        if response.status == 200:
            queue.db.execute('INSERT OR IGNORE INTO metadata_feed_observations VALUES (?,?,?,?,?,?,?)',
                (source.provider, task['url'], fingerprint, digest, receipt_path, len(rows), checked))
        if not _locked_discovery_head_matches(queue, task, _discovery_head_fields(task)):
            # The raw page and its receipt remain indexed observations, but an
            # older request cannot replace a later source/head outcome.
            if response.status == 200:
                links = [(row['url'], row['title'], row['publication_raw']) for row in rows]
                links = _missing_article_links(queue, links)
                queue.add_articles([{'url': url, 'title': title, 'date': date} for url, title, date in links])
                from downloader.news_fulltext_metadata_checkpoint import finish
                finish(queue, _checkpoint_id)
            queue.metrics.count(source.provider, 'metadata_feed_stale_source_head')
            return 0
        if response.status == 200:
            links = [(row['url'], row['title'], row['publication_raw']) for row in rows]
            links = _article_links_started_after_heads(queue, links, task.get('_discovery_request_started_at_utc'))
            queue.add_articles([{'url': url, 'title': title, 'date': date} for url, title, date in links])
            queue.db.execute('INSERT OR REPLACE INTO metadata_feed_heads VALUES (?,?,?,?,?,?,?,?,?,?)',
                (source.provider, task['url'], fingerprint, digest, receipt_path, len(rows), len(response.body), checked,
                 min(clocks).isoformat() if clocks else None, max(clocks).isoformat() if clocks else None))
        else:
            queue.db.execute('UPDATE metadata_feed_heads SET checked_at_utc=? WHERE provider=? AND feed_url=?',
                             (checked, source.provider, task['url']))
        queue.db.execute('UPDATE discovery SET due=?,etag=coalesce(?,etag),last_modified=coalesce(?,last_modified),'
            'response_sha256=?,parser_fingerprint=?,last_checked_at_utc=?,attempts=0,error_code=NULL WHERE url=?',
            ((time.time() if _checked is None else datetime.fromisoformat(checked.replace('Z', '+00:00')).timestamp())
             + source.metadata_feed_interval_seconds, response.headers.get('etag'),
             response.headers.get('last-modified'), receipt['response_sha256'], fingerprint, checked, task['url']))
        if response.status == 200:
            from downloader.news_fulltext_metadata_checkpoint import finish
            finish(queue, _checkpoint_id)
    if response.status == 200:
        queue.metrics.count(source.provider, 'durably_accepted_metadata_feed_pages')
        queue.metrics.count(source.provider, 'metadata_feed_item_observations', len(rows))
    return 0  # Never increment a body/version acceptance counter.


def progress(db, source):
    """Project feed heads and indexed recovery counts without publisher calls."""
    from downloader.news_fulltext_metadata_legacy import progress as legacy_progress
    jobs, rows = [], []
    fingerprints = compatible_fingerprints(source)
    placeholders = ','.join('?' for _ in source.metadata_feeds)
    pending = {row['request_url']: (row['pending'], row['not_before']) for row in db.execute(
        "SELECT request_url,count(*) AS pending,min(not_before) AS not_before "
        "FROM metadata_feed_interrupted_checkpoints WHERE state='pending' AND request_url IN (" + placeholders + ") "
        "GROUP BY request_url", source.metadata_feeds)} if source.metadata_feeds else {}
    for url in source.metadata_feeds:
        row = db.execute('SELECT * FROM metadata_feed_heads WHERE provider=? AND feed_url=?', (source.provider, url)).fetchone()
        job = db.execute('SELECT due,error_code,last_checked_at_utc FROM discovery WHERE url=? AND kind=?',
                         (url, 'metadata_feed')).fetchone()
        if row is not None and row['parser_fingerprint'] in fingerprints:
            rows.append(dict(row))
        if job is not None:
            jobs.append((url, dict(job)))
    eligible = [job for url, job in jobs if url not in pending]
    next_recovery = min((value[1] for value in pending.values()), default=None)
    return {'contract_version': VERSION, 'failure_diagnostic_contract_version': FAILURE_DIAGNOSTIC_VERSION,
        'enabled': True, 'registered_feeds': len(source.metadata_feeds),
        'verified_feeds': len(rows), 'latest_feed_item_observations': sum(row['rows'] for row in rows),
        'latest_feed_response_bytes': sum(row['response_bytes'] for row in rows),
        'feeds_with_errors': sum(bool(job['error_code']) for _, job in jobs),
        'poll_interval_seconds': source.metadata_feed_interval_seconds,
        'next_check_at_utc': datetime.fromtimestamp(min(job['due'] for job in eligible), timezone.utc).isoformat() if eligible else None,
        'last_check_at_utc': max((job['last_checked_at_utc'] for _, job in jobs if job['last_checked_at_utc']), default=None),
        'checkpoint_contract_version': 1, 'pending_recovery_feeds': len(pending),
        'pending_recovery_originals': sum(value[0] for value in pending.values()),
        'local_recovery_ready': next_recovery is not None and next_recovery <= time.time(),
        'next_local_recovery_at_utc': datetime.fromtimestamp(next_recovery, timezone.utc).isoformat() if next_recovery is not None else None,
        'legacy_original_index': legacy_progress(db, source),
        'article_bodies_accepted': 0, 'overlapping_feed_items_not_unique_articles': True,
        'all_history_complete': False, 'finish_eta_utc': None}
