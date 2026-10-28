"""Native paid-preview outcomes from immutable, closed source observations.

The existing repair inbox owns the transaction and review receipt. No HTTP,
body, publication clock, source retry or implicit article alias is created.
"""
from contextlib import closing
from dataclasses import replace
import hashlib
from itertools import islice
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError, _plain_text_node, normalize_url

CONTRACT_VERSION = 1
FINGERPRINT_KEY = 'verified_native_paid_preview_outcome_contract_version'
VERSION = 10
KIND = 'reviewed_native_paid_preview_outcome'
OUTCOME = 'classified_paywall_or_login'
MAX_TARGETS = 32
NOTICE = '本文為付費文章，欲閱覽請至商城作數位訂閱\n訂閱服務'
NOTICE_SHA256 = hashlib.sha256(NOTICE.encode()).hexdigest()
_DIGEST = re.compile('[0-9a-f]{64}')


def native_paid_preview(container, source, *, page_url):
    """Recognize the witnessed native footer, never a quotation/navigation."""
    from bs4 import Tag
    from downloader.news_fulltext_moneyweekly import moneyweekly_body_geometry_source, moneyweekly_body_geometry_route
    from downloader.news_fulltext_public_originals import _visible
    if (CONTRACT_VERSION != 1 or not moneyweekly_body_geometry_source(source)
            or not moneyweekly_body_geometry_route(page_url)
            or not urlsplit(page_url).path.startswith('/Magazine/Info/')):
        return False
    children = [n for n in container.children if isinstance(n, Tag)]
    if (len(children) != 2 or any(n.name != 'div' for n in children)
            or any(str(n).strip() for n in container.children if not isinstance(n, Tag))):
        return False
    footer = children[-1]
    links = footer.select('a')
    if (len(links) != 1 or links[0].get_text(' ', strip=True) != '訂閱服務'
            or any(n.name not in {'div', 'a', 'br', 'span'} for n in footer.find_all(True))):
        return False
    try:
        for node in (container, footer, *footer.find_all(True)):
            _visible(node)
        return _plain_text_node(footer, source.exclude_selectors, parser=source.html_parser) == NOTICE
    except NewsError:
        return False


def reference(value):
    if (not isinstance(value, dict) or set(value) != {'request_id', 'job_sha256'}
            or any(not isinstance(v, str) or not _DIGEST.fullmatch(v) for v in value.values())):
        raise ValueError('invalid_paid_preview_witness')


def _witness(db, source, target, value):
    from downloader.news_fulltext_moneyweekly import (
        BODY_GEOMETRY_LAYOUT, moneyweekly_body_geometry_result, moneyweekly_body_geometry_source,
    )
    from downloader.news_fulltext_repairs import _bytes, _clock, _source_contract
    reference(value)
    try:
        row = db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (value['request_id'],)).fetchone()
        if (CONTRACT_VERSION != 1 or not moneyweekly_body_geometry_source(source) or row is None
                or row['provider'] != 'moneyweekly' or row['state'] != 'observed'
                or hashlib.sha256(_bytes(dict(row))).hexdigest() != value['job_sha256']
                or len(row['request_json']) > 1 << 20 or len(row['result_json']) > 1 << 20):
            raise ValueError
        request, result = json.loads(row['request_json']), json.loads(row['result_json'])
        old_capture = request['source']['data_fingerprint']
        historical = {source.parser_repair_prior_capture_fingerprint, *source.parser_repair_capture_history}
        old_source = replace(source, data_fingerprint=old_capture, parser_repair_capture_fingerprint=old_capture)
        if (old_capture == source.data_fingerprint or old_capture not in historical
                or request['source'] != _source_contract(db, old_source, target['url'])[1]
                or hashlib.sha256(_bytes(request)).hexdigest() != row['request_id']
                or request['contract_version'] != 65 or request['evidence_contract_version'] != 65
                or request['candidate_layout'] != BODY_GEOMETRY_LAYOUT
                or request['candidate_fingerprint'] != old_capture or request['target'] != target
                or not urlsplit(target['url']).path.startswith('/Magazine/Info/')
                or not moneyweekly_body_geometry_result(result, target['url'])
                or result['status'] != 200 or result['redirect_count'] != 0
                or not _clock(target['last_checked_at_utc']) <= _clock(row['reserved_at_utc'])
                    <= _clock(row['completed_at_utc']) <= time.time()):
            raise ValueError
        html = result['html']
        bodies = html['bodies']
        if html['configured_body_count'] != 1 or len(bodies) != 1:
            raise ValueError
        body = bodies[0]
        children = body['direct_children']
        if (body['direct_children_count'] != 2 or len(children) != 2 or body['children_truncated']
                or body['inline_hidden_hint'] or body['paragraph_count'] or body['image_count']
                or body['iframe_count'] or body['script_count']
                or any(n['tag'] != 'div' or n['inline_hidden_hint'] for n in children)
                or children[-1]['text_sha256'] != NOTICE_SHA256
                or children[-1]['text_characters'] != len(NOTICE)):
            raise ValueError
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError, RecursionError, sqlite3.OperationalError):
        raise ValueError('closed_paid_preview_evidence_required') from None


def submit(root, registry, urls, *, reason, witnesses):
    from downloader import news_fulltext_repairs as shared
    from downloader.news_fulltext_queue import utc_now
    if not isinstance(reason, str) or not 0 < len(reason.strip()) <= 240:
        raise ValueError('paid_preview_requires_bounded_reason')
    selected = list(islice(urls, MAX_TARGETS + 1))
    if not 1 <= len(selected) <= MAX_TARGETS or not isinstance(witnesses, dict) or set(witnesses) != set(selected):
        raise ValueError('paid_preview_requires_one_to_32_witnessed_urls')
    if any(not isinstance(url, str) or normalize_url(url) != url for url in selected):
        raise ValueError('literal_paid_preview_url_required')
    root = Path(root).resolve()
    targets, sources, references, excluded = [], {}, {}, []
    with closing(sqlite3.connect((root/'queue.sqlite3').as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        for url in dict.fromkeys(selected):
            row = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
            problem = shared._head_problem(row, time.time(), registry=registry, allowed_failure_states={'body_not_found'})
            if problem is not None:
                excluded.append(dict(url=url, reason=problem))
                continue
            source, identity = shared._source(db, registry, url)
            target = shared._head(row)
            _witness(db, source, target, witnesses[url])
            targets.append(target)
            references[target['id']] = witnesses[url]
            sources[source.provider] = identity
        assert db.total_changes == 0
    result = dict(contract_version=VERSION, submitted_targets=len(targets), excluded=excluded,
        publisher_requests=0, queue_mutations=False, accepted_as_bodies=False)
    if not targets:
        return dict(result, submitted=False, request_id=None)
    request = dict(contract_version=VERSION, kind=KIND, created_at_utc=utc_now(), reason=reason.strip(),
        targets=targets, sources=sources, witnesses=references)
    validate_request(request)
    payload = shared._bytes(request)
    if len(payload) > shared.MAX_REQUEST_BYTES:
        raise ValueError('paid_preview_request_unbounded')
    directory = shared._directory(root, 'pending')
    if len(shared._pending(directory)) >= shared.MAX_PENDING:
        raise ValueError('repair_inbox_full')
    digest = hashlib.sha256(payload).hexdigest()
    path = directory/(digest+'.json')
    shared._write_once(path, payload)
    return dict(result, submitted=True, request_id=digest, request_path=str(path))


def validate_request(request):
    from downloader import news_fulltext_repairs as shared
    if (not isinstance(request, dict) or set(request) != {'contract_version','kind','created_at_utc','reason','targets','sources','witnesses'}
            or type(request['contract_version']) is not int or request['contract_version'] != VERSION or request['kind'] != KIND
            or not isinstance(request['reason'], str) or not 0 < len(request['reason'].strip()) <= 240
            or shared._clock(request['created_at_utc']) > time.time()
            or not isinstance(request['targets'], list) or not 1 <= len(request['targets']) <= MAX_TARGETS
            or not isinstance(request['sources'], dict) or set(request['sources']) != {'moneyweekly'}
            or not isinstance(request['witnesses'], dict)):
        raise ValueError('invalid_paid_preview_request')
    identity = request['sources']['moneyweekly']
    if (not isinstance(identity, dict) or set(identity) != shared.SOURCE_FIELDS
            or type(identity['contract_version']) is not int or identity['contract_version'] != 1
            or any(not isinstance(v,str) or not _DIGEST.fullmatch(v) for k,v in identity.items() if k!='contract_version')):
        raise ValueError('invalid_paid_preview_source')
    seen = set()
    for target in request['targets']:
        if (not isinstance(target,dict) or set(target)!=set(shared.HEAD_FIELDS)
                or not isinstance(target['id'],str) or not _DIGEST.fullmatch(target['id']) or target['id'] in seen
                or not isinstance(target['url'],str) or normalize_url(target['url'])!=target['url']
                or hashlib.sha256(target['url'].encode()).hexdigest()!=target['id']
                or target['provider']!='moneyweekly' or target['state']!='body_not_found' or target['error_code']!='body_not_found'
                or type(target['attempts']) is not int or target['attempts']<0
                or type(target['due']) not in (int,float) or not math.isfinite(target['due']) or target['due']<0
                or shared._clock(target['last_checked_at_utc'])>time.time()):
            raise ValueError('invalid_paid_preview_target')
        seen.add(target['id'])
    if set(request['witnesses']) != seen:
        raise ValueError('invalid_paid_preview_references')
    for value in request['witnesses'].values():
        reference(value)
    return request


def admit(queue, request, target):
    from downloader import news_fulltext_repairs as shared
    row = queue.db.execute('SELECT * FROM articles WHERE id=?', (bytes.fromhex(target['id']),)).fetchone()
    problem = shared._head_problem(row, time.time(), registry=queue.registry, allowed_failure_states={'body_not_found'})
    if problem:
        return problem
    if shared._head(row) != target:
        return 'head_changed_since_review'
    try:
        source, identity = shared._source(queue.db, queue.registry, target['url'])
        if identity != request['sources'][source.provider]:
            return 'source_identity_changed'
        _witness(queue.db, source, target, request['witnesses'][target['id']])
    except (NewsError, ValueError, KeyError, TypeError, OverflowError, RecursionError):
        return 'paid_preview_evidence_invalid'
    queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='paywall_or_login' WHERE id=?", (row['id'],))
    return OUTCOME
