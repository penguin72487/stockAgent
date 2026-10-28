"""Reviewed acquisition of newly supported known routes, not body acceptance.

Use the common repair mailbox, transaction, once-only journal and HTTP owner.
Source-observed route geometry allows another acquisition attempt only; every
normal author, whole-body, publication, robots and rate check still applies.
"""
from contextlib import closing
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from itertools import islice
from urllib.parse import urlsplit

from downloader.news_fulltext_moneyweekly import (
    NATIVE_ARTICLE_PATTERN, ROUTE_METADATA_LAYOUT, ROUTE_METADATA_QUESTION,
    moneyweekly_article_id, moneyweekly_literal_unhandled_route, moneyweekly_route_source,
)
from downloader.news_fulltext_policy import NewsError, normalize_url
from downloader.news_fulltext_queue import utc_now
from downloader import news_fulltext_repairs as shared

VERSION = 9
KIND = 'reviewed_native_article_route_recheck'
OUTCOME = 'native_article_route_recheck_enqueued'
STATES = frozenset({'unsupported_article_path'})


def _family(url):
    if not moneyweekly_literal_unhandled_route(url):
        raise ValueError('reviewed_native_moneyweekly_route_required')
    return 'article_redirect' if urlsplit(url).path == '/_Article' else 'magazine'


def _source(source):
    return bool(moneyweekly_route_source(source) and source.article_path_pattern == NATIVE_ARTICLE_PATTERN
        and source.parser_repair_prior_capture_fingerprint
        and source.parser_repair_prior_capture_fingerprint != source.data_fingerprint)


def _reference(value):
    if (not isinstance(value, dict) or set(value) != {'request_id','job_sha256'}
            or any(not isinstance(v,str) or not re.fullmatch('[0-9a-f]{64}',v) for v in value.values())):
        raise ValueError('invalid_native_route_witness_reference')


def _witness(db, source, family, reference):
    _reference(reference)
    error = 'closed_source_native_route_witness_required'
    try:
        row = db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
            (reference['request_id'],)).fetchone()
        if (not _source(source) or row is None or row['provider'] != 'moneyweekly' or row['state'] != 'observed'
                or hashlib.sha256(shared._bytes(dict(row))).hexdigest() != reference['job_sha256']):
            raise ValueError(error)
        request,result=json.loads(row['request_json']),json.loads(row['result_json'])
        target=request['target'];url=target['url']
        if (hashlib.sha256(shared._bytes(request)).hexdigest() != row['request_id']
                or request['contract_version'] != 64 or request['evidence_contract_version'] != 64
                or request['candidate_layout'] != ROUTE_METADATA_LAYOUT or _family(url) != family
                or request['candidate_fingerprint'] != source.parser_repair_prior_capture_fingerprint
                or request['source']['data_fingerprint'] != source.parser_repair_prior_capture_fingerprint
                or request['source']['authorization_fingerprint'] != source.authorization_fingerprint
                or target['provider'] != 'moneyweekly' or target['state'] != 'unsupported_article_path'
                or target['error_code'] != 'unsupported_article_path'
                or result['provider'] != 'moneyweekly' or result['question'] != ROUTE_METADATA_QUESTION
                or result['evidence_contract_version'] != 64 or result['requested_url'] != url or result['final_url'] != url
                or result['redirect_count'] != 0 or result['current_source_not_candidate'] is not True
                or any(result[k] is not False for k in ('accepted_as_article','raw_or_names_or_body_retained',
                    'raw_or_body_retained','destination_requested','identity_or_alias_accepted','body_completeness_verified'))
                or not (shared._clock(row['created_at_utc']) <= shared._clock(row['reserved_at_utc'])
                    <= shared._clock(row['completed_at_utc']) <= time.time())):
            raise ValueError(error)
        if family == 'magazine':
            if (result['status'] != 200 or result['canonical_urls'] != [url] or result['og_urls'] != [url]
                    or result['configured_body_count'] != 1 or result['heading_count'] != 1
                    or result['native_member_mask_present'] is not False):
                raise ValueError(error)
        elif (result['status'] != 302 or not isinstance(result['location'],str)
                or moneyweekly_article_id(result['location']) != moneyweekly_article_id(url,version=4,requested=True)):
            raise ValueError(error)
    except (KeyError,TypeError,ValueError,sqlite3.OperationalError,RecursionError):
        raise ValueError(error) from None


def _head_problem(row, now):
    problem=shared._head_problem(row,now,allowed_failure_states=STATES)
    if problem == 'missing_or_invalid_check' and row['last_checked_at_utc'] is None:
        return None  # Preserve unknown time for path-only exclusions; never invent a check.
    return problem


def submit(root, registry, urls, *, reason, witnesses):
    if not isinstance(reason,str) or not reason.strip() or len(reason)>240:
        raise ValueError('repair_requires_bounded_review_reason')
    selected=list(islice(urls,shared.MAX_TARGETS+1))
    if not 1<=len(selected)<=shared.MAX_TARGETS:
        raise ValueError('repair_requires_one_to_512_urls')
    if not isinstance(witnesses,dict) or not set(witnesses)<= {'magazine','article_redirect'}:
        raise ValueError('invalid_native_route_witnesses')
    targets,sources,excluded=[],{},[]
    root=Path(root).resolve()
    with closing(sqlite3.connect((root/'queue.sqlite3').as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row;db.execute('BEGIN')
        has_items=db.execute("SELECT 1 FROM sqlite_master WHERE name='reviewed_repair_items'").fetchone()
        for url in dict.fromkeys(selected):
            try:
                if not isinstance(url,str) or normalize_url(url)!=url:
                    raise ValueError('literal_native_route_required')
                family=_family(url)
                row=db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone()
                problem=_head_problem(row,time.time())
                if problem:raise ValueError(problem)
                source,identity=shared._source(db,registry,url)
                if row['provider']!=source.provider:raise ValueError('provider_changed')
                _witness(db,source,family,witnesses[family])
                if has_items and shared._already_admitted(db,source.provider,identity['parser_fingerprint'],row['id'],OUTCOME):
                    raise ValueError('already_admitted_for_native_route')
                targets.append(shared._head(row));sources[source.provider]=identity
            except (ValueError,NewsError,KeyError) as error:
                excluded.append(dict(url=url,reason=error.code if isinstance(error,NewsError) else str(error)))
    result=dict(contract_version=VERSION,submitted_targets=len(targets),excluded=excluded,publisher_requests=0,
        queue_mutations=False,accepted_as_bodies=False)
    if not targets:return dict(result,submitted=False,request_id=None)
    needed={_family(t['url']) for t in targets}
    request=dict(contract_version=VERSION,kind=KIND,created_at_utc=utc_now(),reason=reason.strip(),
        sources=sources,targets=targets,witnesses={k:witnesses[k] for k in sorted(needed)})
    payload=shared._bytes(validate_request(request))
    if len(payload)>shared.MAX_REQUEST_BYTES:raise ValueError('repair_request_too_large')
    directory=shared._directory(root,'pending')
    if len(shared._pending(directory))>=shared.MAX_PENDING:raise ValueError('repair_inbox_full')
    digest=hashlib.sha256(payload).hexdigest()
    path = directory / (digest + '.json')
    shared._write_once(path,payload)
    return dict(result,submitted=True,request_id=digest,request_path=str(path))


def validate_request(request):
    fields={'contract_version','kind','created_at_utc','reason','sources','targets','witnesses'}
    if (not isinstance(request,dict) or set(request)!=fields or type(request['contract_version']) is not int
            or request['contract_version']!=VERSION or request['kind']!=KIND
            or not isinstance(request['reason'],str) or not 0<len(request['reason'].strip())<=240
            or shared._clock(request['created_at_utc'])>time.time()
            or not isinstance(request['targets'],list) or not 1<=len(request['targets'])<=shared.MAX_TARGETS
            or not isinstance(request['sources'],dict) or set(request['sources'])!={'moneyweekly'}
            or not isinstance(request['witnesses'],dict)):
        raise ValueError('invalid_native_route_request')
    identity=request['sources']['moneyweekly']
    if (not isinstance(identity,dict) or set(identity)!=shared.SOURCE_FIELDS
            or type(identity['contract_version']) is not int or identity['contract_version']!=1
            or any(not isinstance(v,str) or not re.fullmatch('[0-9a-f]{64}',v)
                for k,v in identity.items() if k!='contract_version')):
        raise ValueError('invalid_native_route_source')
    seen,families=set(),set()
    for target in request['targets']:
        if (not isinstance(target,dict) or set(target)!=set(shared.HEAD_FIELDS)
                or target['provider']!='moneyweekly' or target['state']!='unsupported_article_path'
                or target['error_code']!=target['state'] or not isinstance(target['url'],str)
                or normalize_url(target['url'])!=target['url']
                or target['id']!=hashlib.sha256(target['url'].encode()).hexdigest() or target['id'] in seen
                or type(target['attempts']) is not int or target['attempts']<0
                or type(target['due']) not in (int,float) or not math.isfinite(target['due']) or target['due']<0
                or target['due']>shared._clock(request['created_at_utc'])
                or target['last_checked_at_utc'] is not None
                    and shared._clock(target['last_checked_at_utc'])>shared._clock(request['created_at_utc'])):
            raise ValueError('invalid_native_route_target')
        seen.add(target['id']);families.add(_family(target['url']))
    if set(request['witnesses'])!=families:raise ValueError('invalid_native_route_witness_families')
    for reference in request['witnesses'].values():_reference(reference)
    return request


def admit(queue,request,target):
    row=queue.db.execute('SELECT * FROM articles WHERE id=?',(bytes.fromhex(target['id']),)).fetchone()
    problem=_head_problem(row,time.time())
    if problem:return problem
    if shared._head(row)!=target:return 'head_changed_since_review'
    try:
        source,identity=shared._source(queue.db,queue.registry,target['url'])
        if identity!=request['sources'][source.provider]:return 'source_identity_changed'
        family=_family(target['url'])
        _witness(queue.db,source,family,request['witnesses'][family])
    except (ValueError,NewsError,KeyError):return 'native_route_evidence_invalid'
    if shared._already_admitted(queue.db,source.provider,identity['parser_fingerprint'],row['id'],OUTCOME):
        return 'already_admitted_for_native_route'
    queue.db.execute("UPDATE articles SET state='retry' WHERE id=?",(row['id'],))
    return OUTCOME
