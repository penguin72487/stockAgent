"""Finite private source excerpts and public navigation declarations.

The question requires closed v66 evidence. It may retain at most one short
native header and a sub-100-character template; this is not a body-free probe.
No article, author, clock, alias, media or destination is admitted here.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from urllib.parse import parse_qsl, urljoin, urlsplit

from downloader.news_fulltext_moneyweekly import moneyweekly_body_geometry_route
from downloader.news_fulltext_moneyweekly_metadata import QUESTION as PRIOR_QUESTION, require_prior_template, template_result
from downloader.news_fulltext_policy import NewsError, _plain_text_node, normalize_url

LAYOUT = 'moneyweekly_short_template_and_query_declaration_v1'
QUESTION = 'moneyweekly_literal_short_template_and_public_navigation_query_v1'
_HASH = re.compile('[0-9a-f]{64}')
_SECRET_KEY = re.compile(r'token|session|auth|pass|cookie|secret|credential|email|user|api.?key|signature|access|code', re.I)


def _sha(value):
    return hashlib.sha256(value.encode() if isinstance(value,str) else value).hexdigest()


def require_prior_excerpt(db,source,url):
    """A new field requires an exact, closed successful v66 prerequisite."""
    from downloader.news_fulltext_repairs import _bytes,_head,_source_contract
    try:
        require_prior_template(db,source,url)
        key=_sha(_bytes([66,source.provider,_sha(url),PRIOR_QUESTION]))
        row=db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',(key,)).fetchone()
        if row is None or row['state']!='observed':
            raise ValueError
        request,result=json.loads(row['request_json']),json.loads(row['result_json'])
        head=db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone()
        if (not template_result(result,url) or request['target']!=_head(head)
                or request['source']!=_source_contract(db,source,url)[1]
                or _sha(_bytes(request))!=row['request_id']
                or request['candidate_fingerprint']!=source.data_fingerprint):
            raise ValueError
        shape=result['structure']
        if urlsplit(url).path.startswith('/Magazine/Info/'):
            if (result['status']!=200 or shape['heading_count']!=1 or shape['body_count']!=1
                    or not 0<shape['bodies'][0]['text_characters']<100
                    or shape['native_leaf_metadata']):
                raise ValueError
        elif (result['status']!=302 or shape['literal_url'] is not None
                or len(shape['query_fields'])!=1 or shape['query_fields'][0]['key']!='other'
                or shape['query_fields'][0]['value_characters']!=2):
            raise ValueError
        return source
    except (KeyError,TypeError,ValueError,AttributeError,RecursionError,sqlite3.OperationalError):
        raise ValueError('closed_unresolved_moneyweekly_excerpt_required') from None


def _public_location(value,base):
    """Keep only a native numeric Article route and one tiny non-secret flag.

    A literal publisher query declaration is not an alias or permission to
    request it. Unknown long values and credential-like names remain hashed.
    """
    if not isinstance(value,str) or not 0<len(value)<=2048:
        return None
    try:
        resolved=urljoin(base,value)
        parts=urlsplit(resolved)
        if (normalize_url(resolved)!=resolved or parts.scheme!='https'
                or parts.netloc!='www.moneyweekly.com.tw' or parts.fragment
                or not re.fullmatch(r'/ArticleData/Info/Article/[1-9][0-9]*',parts.path)):
            return None
        fields=parse_qsl(parts.query,keep_blank_values=True,strict_parsing=True)
        if len(fields)!=1:
            return None
        key,flag=fields[0]
        if (not re.fullmatch('[A-Za-z][A-Za-z0-9_]{0,31}',key) or _SECRET_KEY.search(key)
                or not re.fullmatch('[A-Za-z0-9]{1,2}',flag)):
            return None
        return dict(raw_reference=value,literal_url=resolved,key=key,value=flag)
    except (ValueError,NewsError):
        return None


def _snippet(node,source,maximum):
    from downloader.news_fulltext_public_originals import _visible
    _visible(node)
    for descendant in node.find_all(True):
        _visible(descendant)
    text=_plain_text_node(node,source.exclude_selectors,parser=source.html_parser)
    dom=node.get_text(' ',strip=True)
    if len(text)>maximum or len(dom)>maximum:
        raise NewsError('moneyweekly_excerpt_exceeds_bound')
    return dict(text=text,text_characters=len(text),text_sha256=_sha(text),dom_text_sha256=_sha(dom))


def excerpt_metadata(response,source,url):
    if (not moneyweekly_body_geometry_route(url) or response.url!=url or response.redirects
            or response.status not in {200,301,302,303,307,308}):
        raise NewsError('http_404' if response.status==404 else 'literal_moneyweekly_excerpt_required')
    common=dict(contract_version=1,evidence_contract_version=67,provider='moneyweekly',question=QUESTION,
        requested_url=url,final_url=response.url,status=response.status,redirect_count=0,
        response_bytes=len(response.body),response_sha256=_sha(response.body),current_source_not_candidate=True,
        accepted_as_article=False,body_completeness_verified=False,identity_or_alias_accepted=False,
        author_or_publication_accepted=False,raw_response_retained=False,full_body_retained=False,
        destination_requested=False)
    if urlsplit(url).path.startswith('/Home/ChangePage/FN/'):
        if response.status!=302:
            raise NewsError('literal_moneyweekly_excerpt_required')
        raw=response.headers.get('location','')
        common.update(kind='public_navigation_declaration',raw_or_names_or_body_retained=False,
            raw_or_body_retained=False,diagnostic_excerpt_retained=False,
            declaration=_public_location(raw,url),location_raw_sha256=_sha(raw))
        return common
    if (response.status!=200 or not urlsplit(url).path.startswith('/Magazine/Info/')
            or response.headers.get('content-type','').split(';',1)[0].strip().lower()
                not in {'text/html','application/xhtml+xml'} or len(response.body)>2<<20):
        raise NewsError('literal_moneyweekly_excerpt_required')
    from bs4 import BeautifulSoup,Tag
    soup=BeautifulSoup(response.body,source.html_parser)
    headings,bodies=soup.select('h1'),soup.select(source.selectors[0])
    if (len(headings)!=1 or len(bodies)!=1 or not 0<len(bodies[0].get_text(' ',strip=True))<100
            or bodies[0].find(['img','script','style','iframe','object','embed','video','audio','form'])):
        raise NewsError('short_moneyweekly_template_required')
    body=bodies[0]
    children=[n for n in body.children if isinstance(n,Tag)]
    parent=headings[0].parent.parent
    siblings=[n for n in parent.children if isinstance(n,Tag)]
    if len(children)!=2 or len(siblings)>12 or len(siblings)<3 or siblings[2].name!='div':
        raise NewsError('short_moneyweekly_template_required')
    common.update(kind='private_short_template_excerpt',raw_or_names_or_body_retained=True,
        raw_or_body_retained=True,diagnostic_excerpt_retained=True,possibly_prose=True,
        header=_snippet(siblings[2],source,128),body=_snippet(body,source,99),
        children=[_snippet(n,source,99) for n in children])
    return common


def excerpt_result(result,url):
    """Validate a closed schema, literal bounds, hashes and truthful retention."""
    def keys(value,fields):
        return isinstance(value,dict) and set(value)==set(fields.split())
    def sha(value):
        return isinstance(value,str) and bool(_HASH.fullmatch(value))
    def snippet(value,maximum):
        return (keys(value,'text text_characters text_sha256 dom_text_sha256')
            and isinstance(value['text'],str) and len(value['text'])<=maximum
            and type(value['text_characters']) is int and value['text_characters']==len(value['text'])
            and value['text_sha256']==_sha(value['text']) and sha(value['dom_text_sha256']))
    try:
        base='contract_version evidence_contract_version provider question requested_url final_url status redirect_count response_bytes response_sha256 current_source_not_candidate accepted_as_article body_completeness_verified identity_or_alias_accepted author_or_publication_accepted raw_response_retained full_body_retained destination_requested kind raw_or_names_or_body_retained raw_or_body_retained diagnostic_excerpt_retained'
        fields=(base+' possibly_prose header body children' if result['kind']=='private_short_template_excerpt'
            else base+' declaration location_raw_sha256')
        if (not keys({k:v for k,v in result.items() if k!='timing'},fields)
                or type(result['contract_version']) is not int or result['contract_version']!=1
                or type(result['evidence_contract_version']) is not int or result['evidence_contract_version']!=67
                or result['provider']!='moneyweekly' or result['question']!=QUESTION
                or not moneyweekly_body_geometry_route(url) or result['requested_url']!=url or result['final_url']!=url
                or type(result['redirect_count']) is not int or result['redirect_count']!=0
                or result['current_source_not_candidate'] is not True
                or any(result[k] is not False for k in ('accepted_as_article','body_completeness_verified','identity_or_alias_accepted',
                    'author_or_publication_accepted','raw_response_retained','full_body_retained','destination_requested'))
                or type(result['response_bytes']) is not int or not 0<=result['response_bytes']<=2<<20
                or not sha(result['response_sha256'])):
            return False
        if 'timing' in result and (not keys(result['timing'],'http_seconds parse_seconds scope_seconds total_seconds')
                or any(type(v) not in (int,float) or not math.isfinite(v) or not 0<=v<=3600 for v in result['timing'].values())):
            return False
        if result['kind']=='private_short_template_excerpt':
            return (urlsplit(url).path.startswith('/Magazine/Info/') and result['status']==200
                and all(result[k] is True for k in ('raw_or_names_or_body_retained','raw_or_body_retained','diagnostic_excerpt_retained','possibly_prose'))
                and snippet(result['header'],128) and snippet(result['body'],99) and result['body']['text_characters']>0
                and isinstance(result['children'],list) and len(result['children'])==2 and all(snippet(v,99) for v in result['children']))
        declaration=result['declaration']
        return (result['kind']=='public_navigation_declaration' and urlsplit(url).path.startswith('/Home/ChangePage/FN/')
            and result['status']==302 and sha(result['location_raw_sha256'])
            and all(result[k] is False for k in ('raw_or_names_or_body_retained','raw_or_body_retained','diagnostic_excerpt_retained'))
            and (declaration is None or keys(declaration,'raw_reference literal_url key value')
                and _public_location(declaration['raw_reference'],url)==declaration
                and _sha(declaration['raw_reference'])==result['location_raw_sha256']))
    except (KeyError,ValueError,TypeError,AttributeError,RecursionError):
        return False
