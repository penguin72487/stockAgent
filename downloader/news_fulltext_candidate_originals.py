"""Private exact failure witnesses, never article admission or a second HTTP owner.

The resident candidate lane reserves its once-only request before HTTP. A
worker can return an immutable response with a positive, source-bound permit;
only the queue owner durably writes the private witness and its receipt.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re

from downloader.news_fulltext_policy import NewsError, _objects, public_article_document
from downloader.news_fulltext_pocket import (
    POCKET_AUTHOR, POCKET_ORIGIN, _native_visible, _unique_object, pocket_article_id,
)
from downloader.news_fulltext_public_originals import _one

MAX_ORIGINAL_BYTES = 2 << 20
QUESTION = 'pocket_private_native_original_boundary_v1'


@dataclass(frozen=True)
class PendingOriginal:
    response: object
    facts: dict
    permit: dict


def private_original_permit(response, source):
    """Independently verify own/public native scope, not the failing body grammar.

    This does not grant copyright, publish/training use or infer historical
    availability. Unknown author, alternative body or access-gate evidence
    prevents retention; it is not relaxed just because the parser failed.
    """
    url, payload = response.url, response.body
    if (source.provider != 'pocket' or not source.allowed or not source.request_enabled
            or source.authorization.get('kind') != 'personal_research'
            or not source.known_url_only or pocket_article_id(url) is None
            or response.status != 200 or response.redirects
            or not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ORIGINAL_BYTES):
        raise NewsError('private_original_scope_not_verified')
    soup = public_article_document(payload, source, page_url=url)
    headings = [n for n in _one(soup, 'main').select('h1') if n.find_parent('article') is None]
    if len(headings) != 1:
        raise NewsError('private_original_scope_not_verified')
    heading = headings[0]
    title = heading.get_text(' ', strip=True)
    if (not title or _one(soup, 'head > link[rel=canonical]').get('href') != url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != url
            or _one(soup, 'head > meta[property="og:title"]').get('content') != title
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'):
        raise NewsError('private_original_scope_not_verified')
    header = heading.find_next_sibling()
    if header is None or header.name != 'div':
        raise NewsError('private_original_scope_not_verified')
    credit = _one(header, ':scope > div > span.font-bold.text-slate-600.mr-2')
    native = _one(heading.parent, ':scope > article.cms-body > .cms-article-scope')
    for node in (heading, header, credit, native):
        _native_visible(node)
    if credit.get_text(' ', strip=True) != POCKET_AUTHOR:
        raise NewsError('private_original_scope_not_verified')
    scripts = soup.select('script[type="application/ld+json"]')
    if not 1 <= len(scripts) <= 16:
        raise NewsError('private_original_scope_not_verified')
    declarations = []
    try:
        for script in scripts:
            text = script.get_text()
            if len(text) > 128 << 10:
                raise ValueError('unbounded declaration')
            declarations.extend(a for a in _objects(json.loads(text, object_pairs_hook=_unique_object))
                                if a.get('@type') == 'Article')
    except (TypeError, ValueError, RecursionError):
        raise NewsError('private_original_scope_not_verified') from None
    if len(declarations) != 1:
        raise NewsError('private_original_scope_not_verified')
    article = declarations[0]
    author = article.get('author')
    if (article.get('headline') != title or article.get('url') != url
            or article.get('mainEntityOfPage') != {'@type':'WebPage', '@id':url}
            or ('isAccessibleForFree' in article and article['isAccessibleForFree'] is not True
                and article['isAccessibleForFree'] != 'true') or article.get('articleBody')
            or not isinstance(author, dict) or author.get('@type') != 'Organization'
            or author.get('name') != POCKET_AUTHOR or author.get('url') != POCKET_ORIGIN+'/school/'
            or not isinstance(article.get('publisher'),dict)
            or article['publisher'].get('@type') != 'Organization' or article['publisher'].get('name') != '口袋學堂'):
        raise NewsError('private_original_scope_not_verified')
    first = next((n for n in native.find_all(['p','h2','h3'])
                  if n.find_parent(class_='article-meta') is None), None)
    if first and re.match(r'^(?:轉載|轉自|本文來源|新聞來源|來源[：:])', first.get_text(' ',strip=True)):
        raise NewsError('private_original_scope_not_verified')
    return {'contract_version':1,'question':QUESTION,'provider':'pocket','url':url,
        'response_sha256':hashlib.sha256(payload).hexdigest(),
        'data_fingerprint':source.data_fingerprint,'authorization_fingerprint':source.authorization_fingerprint,
        # Guest-visible native text plus the reviewed personal-research scope
        # is not an invented schema free declaration or a copyright licence.
        'scope':'reviewed_current_public_native_own_organization_private_original_only'}


def retain_original(queue, job, observation):
    """Main-writer durable sidecar; no accepted_batches or article-head writes."""
    from downloader.artifact_io import atomic_write_parquet, table_from_records, sha256_file
    from downloader.news_fulltext_repairs import _bytes, _directory, _write_once
    from downloader.news_fulltext_queue import utc_now
    import pyarrow.parquet as pq

    source, response, permit = job['source'], observation.response, observation.permit
    if (job.get('evidence_contract_version') != 4 or source.provider != 'pocket'
            or not re.fullmatch(r'[0-9a-f]{64}', job['request_id'])
            or response.url != job['url'] or response.status != 200 or response.redirects
            or not 0 < len(response.body) <= MAX_ORIGINAL_BYTES
            or permit != {'contract_version':1,'question':QUESTION,'provider':'pocket','url':response.url,
                'response_sha256':hashlib.sha256(response.body).hexdigest(),
                'data_fingerprint':source.data_fingerprint,'authorization_fingerprint':source.authorization_fingerprint,
                'scope':'reviewed_current_public_native_own_organization_private_original_only'}):
        raise ValueError('private_original_permit_mismatch')
    directory = _directory(queue.root,'candidate_diagnostics','originals')
    path = directory/(job['request_id']+'.parquet')
    receipt_path = directory/(job['request_id']+'.receipt.json')
    if path.exists() or path.is_symlink() or receipt_path.exists() or receipt_path.is_symlink():
        raise ValueError('private_original_evidence_already_exists')
    flags = {'diagnostic_only':True,'accepted_as_article':False,'publish':False,
             'historical_point_in_time':False,'per_article_license_verified':False,
             'training_injection_enabled':False,'remote_training_export':False}
    record = {'request_id':job['request_id'],'provider':'pocket','request_url':job['url'],
        'final_url':response.url,'observed_at_utc':utc_now(),'raw_response_body':response.body,
        'response_sha256':permit['response_sha256'],'data_fingerprint':source.data_fingerprint,
        'authorization_fingerprint':source.authorization_fingerprint,
        'headers_json':json.dumps({k:v for k,v in response.headers.items()
            if k.lower() in {'date','content-type','last-modified'}},ensure_ascii=False),**flags}
    atomic_write_parquet(path,table_from_records([record]),compression='zstd',compression_level=9,durable=True)
    path.chmod(0o600)
    restored = pq.ParquetFile(path).read().to_pylist()
    if restored != [record]:
        raise ValueError('private_original_readback_mismatch')
    reference = {'path':str(path.relative_to(queue.root)),'sha256':sha256_file(path),
        'receipt_path':str(receipt_path.relative_to(queue.root)),
        'response_sha256':permit['response_sha256'],'response_bytes':len(response.body)}
    _write_once(receipt_path,_bytes({'contract_version':1,'request_id':job['request_id'],
        'question':QUESTION,'permit':permit,'original':reference,**flags}))
    return {**observation.facts,'raw_or_names_or_body_retained':True,
        'raw_retention_state':'retained_verified','private_original':reference,
        'article_heads_mutated':False,'publish':False,'remote_training_export':False}


def retention_flags(request, result, state):
    """Uncertain outcomes must not falsely promise that no raw witness exists."""
    if request.get('evidence_contract_version',1) != 4:
        return {'raw_or_names_or_body_retained':False}
    if result is not None:
        return {'raw_or_names_or_body_retained':result.get('raw_or_names_or_body_retained',False),
                'raw_retention_state':result.get('raw_retention_state','not_retained')}
    uncertain = state in {'reserved','uncertain_outcome'}
    return {'raw_or_names_or_body_retained':None if uncertain else False,
            'raw_retention_state':'unknown' if uncertain else 'not_retained'}
