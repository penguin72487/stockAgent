"""Manager's reviewed public written article; no HTTP, archive or rendering.

Use the native article, never its longer JSON-LD articleBody (which can contain
script text). Native inert table declarations stay explicitly unrendered; this
adapter does not execute JavaScript, visit an iframe or establish reuse rights.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from itertools import islice
import json
import re
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Comment

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


MANAGER_ORIGIN = 'https://www.managertoday.com.tw'
MANAGER_PATH = r'^/articles/view/[1-9][0-9]*$'
MANAGER_BODY = ':scope > .manager-native-body'
MANAGER_LAYOUTS = frozenset({'manager_public_written_article_v1', 'manager_public_written_article_v2',
    'manager_public_written_article_v3', 'manager_public_written_article_v4', 'manager_public_written_article_v5'})
MANAGER_V4_VERIFICATION = 'native_paired_literal_byline_roles_desks_and_writing_punctuation_v4_20261026'
MANAGER_V5_VERIFICATION = 'native_static_embedded_document_metadata_and_literal_financial_cards_v5_20261026'
MANAGER_V5_HTTP_BINDING = (
    '246daae62d73bfd882008a65d9e78d13abe7ce15e4af2ff484bc0b33fd69ae83',
    'a29f390f5c48fda7f0a9c6e9d439758a944a4c31aa84d68222c66994131e8337',
    'e51f5608c5125f199713708b887dbbb00a507730b8fe9ebd7d1fafc4386874f3',
    'c21c2354a560e98fc582b91ff6810a24fd944edc01d84ad339e9208c33b20dd2',
    'e51f5608c5125f199713708b887dbbb00a507730b8fe9ebd7d1fafc4386874f3',
)
# Exact installed v3 -> reviewed v4 capture, authorization, effective HTTP and
# HTTP authorization identities. Parsing must not renew pacing/refusal state.
MANAGER_V4_HTTP_BINDING = (
    'c21c2354a560e98fc582b91ff6810a24fd944edc01d84ad339e9208c33b20dd2',
    '246daae62d73bfd882008a65d9e78d13abe7ce15e4af2ff484bc0b33fd69ae83',
    'e51f5608c5125f199713708b887dbbb00a507730b8fe9ebd7d1fafc4386874f3',
    'c21c2354a560e98fc582b91ff6810a24fd944edc01d84ad339e9208c33b20dd2',
    'e51f5608c5125f199713708b887dbbb00a507730b8fe9ebd7d1fafc4386874f3',
)
MANAGER_V1_SCOPE_CUTOFF = '2026-10-25T05:00:00+00:00'
MANAGER_METADATA_EVIDENCE = 46
MANAGER_METADATA_QUESTION = 'manager_current_native_header_credit_title_clock_shape_v1'
MANAGER_PLAIN_CREDIT_QUESTION = 'manager_current_plain_credit_header_hierarchy_v1'
MANAGER_BODY_ORIGINAL_QUESTION = 'manager_current_header_verified_failed_embedded_document_original_v1'
MANAGER_MULTIPLE_ORIGINAL_QUESTION = 'manager_private_unresolved_multiple_article_roots_header_bound_original_v1'
# The linked publisher profile declares this editor's role. It is not an
# independently verified human identity, blanket ownership or a publisher grant.
_WRITERS = {'支琬清': MANAGER_ORIGIN + '/columnist/view/5738'}
_TAGS = frozenset({'p', 'br', 'h2', 'h3', 'h4', 'h5', 'h6', 'a', 'div', 'span',
    'b', 'strong', 'em', 'i', 'u', 's', 'del', 'sup', 'sub', 'blockquote', 'pre',
    'code', 'ul', 'ol', 'li', 'hr', 'figure', 'figcaption', 'picture', 'source',
    'img', 'section', 'table', 'thead', 'tbody', 'tfoot', 'tr', 'td', 'th',
    'caption', 'colgroup', 'col', 'script', 'style', 'template'})
_REPRINT = re.compile(r'^[（(]?(?:本文轉載|本文轉自|本文由.{0,64}(?:授權|提供)|轉載自|轉自)')
_BYLINE_ROLES = frozenset({'撰文', '採訪·撰文', '採訪‧撰文', '採訪．撰文', '採訪・撰文',
    '整理‧撰文', '整理 · 撰文', '整理·撰文', '經理人用戶成長中心'})
_UPSTREAM_BRANDS = ('經濟日報', '商業周刊', '今周刊', 'MoneyDJ', '遠見', '數位時代', '鉅亨網', 'Smart 智富')
_CARD_EVENTS = {'onmouseout': "this.style.transform='none';this.style.boxShadow='none';",
    'onmouseover': "this.style.transform='translateY(-3px)';this.style.boxShadow='0 8px 20px rgba(0,0,0,.10)';"}


def manager_metadata_source(source):
    """Observe only the current private source; never substitute a candidate."""
    return (source.provider == 'managertoday' and source.allowed and source.request_enabled
        and source.domains == ('www.managertoday.com.tw',)
        and source.embedded_article_layout in MANAGER_LAYOUTS
        and source.authorization.get('kind') == 'personal_research'
        and source.authorization.get('evidence_url') == MANAGER_ORIGIN + '/')


def manager_native_metadata(soup, *, plain_credit_header=False):
    """Retain bounded header declarations, not body text or author verification."""
    from downloader.news_fulltext_document_metadata import _scalar
    headers = soup.select('div.main_box section.article_top')[:3]
    result = {'header_count': len(headers), 'headers': [],
        'body_container_count': len(soup.select('section.view_box main.htmlview[data-cat=article]')),
        'main_container_count': len(soup.select('main.htmlview')),
        'metadata_not_verified_authorship': True, 'body_text_exported': False}
    for header in headers[:2]:
        credits = header.select('a[href*="/columnist/"]')[:17]
        result['headers'].append({
            'headings': [_scalar(h.get_text(' ', strip=True)) for h in header.select('h1')[:3]],
            'credits': [{key: _scalar(value) for key, value in {
                'href': n.get('href'), 'data_dl_text': n.get('data-dl_text'),
                'label': ' '.join(islice(n.stripped_strings, 16))}.items()}
                for n in credits[:16]],
            'credit_limit_reached': len(credits) > 16,
            'dates': [s for s in islice(header.stripped_strings, 256)
                      if re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', s)]})
    shapes = []
    for body in soup.select('section.view_box main.htmlview[data-cat=article]')[:2]:
        nodes = body.find_all(True, limit=16385)
        unknown = {}
        for node in nodes[:16384]:
            if node.name not in _TAGS:
                unknown[node.name] = unknown.get(node.name, 0) + 1
        shapes.append({'node_limit_reached': len(nodes) > 16384,
            'nodes_examined': min(len(nodes), 16384),
            'unknown_element_counts': dict(islice(sorted(unknown.items()), 16)),
            'templates': [{'x_ref': _scalar(n.get('x-ref')),
                'parent_tag': n.parent.name, 'parent_x_data': _scalar(n.parent.get('x-data')),
                'parent_classes': [_scalar(v) for v in n.parent.get('class', [])[:8]],
                'has_table_cells': bool(n.select('table td'))}
                for n in body.select('template')[:8]]})
    result['body_structure_without_prose'] = shapes
    if plain_credit_header:
        # A separate, fixed follow-up to the closed no-profile question.
        # Direct header text only, not body/script/template text or a
        # recursively copied DOM. One O(N) header traversal.
        items, used_bytes, index = [], 0, {}
        header = headers[0] if headers else None
        nodes = header.find_all(True, limit=49) if header else []
        for node in nodes[:48]:
            index[id(node)] = len(items)
            direct = ' '.join(str(t).strip() for t in node.contents
                if isinstance(t, NavigableString) and not isinstance(t, Comment)).strip()
            if node.name in {'script','style','template'}:
                direct = ''
            record = {'tag':node.name,'parent_index':index.get(id(node.parent)),
                'classes':[str(v)[:64] for v in node.get('class',[])[:3]],
                'attributes':{k:str(node[k])[:128] for k in ('itemprop','data-cat','data-dl_text') if k in node.attrs},
                'direct_text': direct if len(direct)<=256 else {'omitted_characters':len(direct)}}
            size = len(json.dumps(record,ensure_ascii=False).encode())
            if used_bytes+size>12000:
                break
            used_bytes+=size
            items.append(record)
        result['plain_credit_header_hierarchy'] = {'nodes':items,
            'node_or_byte_limit_reached':len(items)<len(nodes),
            'sampling_limits':{'headers':1,'nodes':48,'bytes':12000,'direct_text_characters':256},
            'raw_dom_or_body_retained':False,'authorship_independently_verified':False}
    return result


def _key(value):
    return re.sub(r'\s+', ' ', value).strip() if isinstance(value, str) else ''


def _clock(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00', value):
        raise NewsError('article_identity_not_verified')
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('article_identity_not_verified') from None


def _plain_writing_credits(header, name):
    """Observed plain byline, paired with its adjacent native date, not a caption.

    The desktop variant has one intervening BR; the mobile variant does not.
    Neither the JSON-LD Person alone nor an arbitrary span containing a name is
    attribution. All observed responsive copies must agree on both name/date.
    """
    candidates = [node for node in header.select('span')
        if re.match(r'^(?:採訪·撰文|撰文)\s+', _key(node.get_text(' ', strip=True)))]
    date_nodes = [node for node in header.select('span')
        if re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', node.get_text(strip=True))]
    if not 1 <= len(candidates) <= 2 or len(date_nodes) != len(candidates):
        raise NewsError('article_attribution_not_verified')
    labels, dates = [], []
    for credit in candidates:
        _visible(credit)
        label = _key(credit.get_text(' ', strip=True))
        if (credit.find(True) is not None or credit.parent.name != 'div'
                or label not in {'撰文 ' + name, '採訪·撰文 ' + name}):
            raise NewsError('article_attribution_not_verified')
        previous = credit.find_previous_sibling()
        if previous is not None and previous.name == 'br' and not previous.attrs:
            previous = previous.find_previous_sibling()
        if (previous is None or previous.name != 'span' or previous.find(True) is not None
                or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', previous.get_text(strip=True))):
            raise NewsError('article_attribution_not_verified')
        _visible(previous)
        labels.append(label)
        dates.append(previous.get_text(strip=True))
    return labels, dates


def _native_plain_bylines(header, name):
    """Literal native credit/date pairs, including desks, not inferred people.

    Witnessed desktop/mobile groups contain date, optional empty BR, and one
    plain byline span. Match complete groups and both copies; the JSON-LD name
    alone never supplies a missing byline. Keep punctuation and composite names.
    """
    spans = header.find_all('span', limit=257)
    if len(spans) > 256:
        raise NewsError('article_attribution_not_verified')
    dates = [n for n in spans if re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', n.get_text(strip=True))]
    allowed_labels = {name, *(role + ' ' + name for role in _BYLINE_ROLES)}
    declared_credits = [n for n in spans if n.find(True) is None
        and _key(n.get_text(' ', strip=True)) in allowed_labels]
    if not 1 <= len(dates) <= 2 or len(declared_credits) != len(dates):
        raise NewsError('article_attribution_not_verified')
    labels, raw_dates = [], []
    for date in dates:
        group = date.parent
        children = group.find_all(True, recursive=False)
        if (group.name != 'div' or len(children) not in {2, 3} or children[0] is not date
                or len(children) == 3 and (children[1].name != 'br' or children[1].attrs)
                or any(isinstance(n, NavigableString) and not isinstance(n, Comment) and str(n).strip()
                    for n in group.contents)):
            raise NewsError('article_attribution_not_verified')
        credit = children[-1]
        if date.find(True) is not None or credit.name != 'span' or credit.find(True) is not None:
            raise NewsError('article_attribution_not_verified')
        _visible(date)
        _visible(credit)
        label = _key(credit.get_text(' ', strip=True))
        if any(brand in value for brand in _UPSTREAM_BRANDS for value in (name, label)):
            raise NewsError('article_rights_or_scope_excluded')
        if label not in allowed_labels or all(credit is not n for n in declared_credits):
            raise NewsError('article_attribution_not_verified')
        labels.append(label)
        raw_dates.append(date.get_text(strip=True))
    if len(set(labels)) != 1 or len(set(raw_dates)) != 1:
        raise NewsError('article_attribution_not_verified')
    return labels, raw_dates


def private_manager_body_original_permit(response, source, requested_url):
    """One received failed-body witness, after the existing header/clock gates.

    A successful parse needs no diagnostic copy. Only the native adapter's
    body_not_found boundary can retain this private original; a bad identity,
    byline, release, access or explicit reprint cannot become a body permit.
    """
    import hashlib
    from downloader.news_fulltext_policy import public_article_document
    if (not manager_metadata_source(source)
            or source.embedded_article_layout != 'manager_public_written_article_v4'
            or response.status != 200 or response.url != requested_url or response.redirects
            or not 0 < len(response.body) <= 2 << 20
            or not re.search(r'(?:text/html|application/xhtml[+]xml)',
                response.headers.get('content-type',''),re.I)):
        raise NewsError('private_manager_original_scope_not_verified')
    soup = public_article_document(response.body,source,page_url=response.url)
    try:
        manager_public_written_article(soup,page_url=requested_url,adapter_version=4)
    except NewsError as error:
        if error.code != 'body_not_found':
            raise
    else:
        raise NewsError('private_manager_original_requires_failed_body')
    return dict(contract_version=1,question=MANAGER_BODY_ORIGINAL_QUESTION,provider=source.provider,
        url=response.url,response_sha256=hashlib.sha256(response.body).hexdigest(),
        data_fingerprint=source.data_fingerprint,authorization_fingerprint=source.authorization_fingerprint,
        scope='current_header_clock_verified_private_failed_embedded_document_original_only',
        literal_header_and_clock_verified=True,body_geometry_accepted=False,article_accepted=False)


def private_manager_multiple_original_permit(response, source, requested_url):
    """Retain one multiple-root failure; validate its header, not a body match.

    One outer NewsArticle and one matching native headline/date/byline bind
    this private observation. The relationship between its two bodies remains
    deliberately unaccepted until the exact original can be reviewed.
    """
    import hashlib
    from downloader.news_fulltext_policy import public_article_document
    parts=urlsplit(requested_url or '')
    if (not manager_metadata_source(source) or source.embedded_article_layout!='manager_public_written_article_v5'
            or parts.scheme!='https' or parts.netloc!='www.managertoday.com.tw' or parts.query or parts.fragment
            or not re.fullmatch(MANAGER_PATH,parts.path) or response.status!=200
            or response.url!=requested_url or response.redirects or not isinstance(response.body,bytes)
            or not 0<len(response.body)<=2<<20 or b'\0' in response.body
            or response.headers.get('content-type','').split(';',1)[0].strip().lower() not in {'text/html','application/xhtml+xml'}):
        raise NewsError('private_manager_multiple_original_scope_not_verified')
    soup=public_article_document(response.body,source,page_url=requested_url)
    metadata=soup.head
    if metadata is None or any(_one(metadata,selector).get(attr)!=requested_url for selector,attr in (
            ('link[rel=canonical]','href'),('meta[property="og:url"]','content'))):
        raise NewsError('article_identity_not_verified')
    scripts=soup.select('script[type="application/ld+json"]');declarations=[]
    if len(scripts)>8:raise NewsError('article_identity_not_verified')
    try:
        for script in scripts:
            nodes=list(islice(_objects(json.loads(script.get_text())),257))
            if len(nodes)>256:raise NewsError('article_identity_not_verified')
            declarations.extend(n for n in nodes if n.get('@type')=='NewsArticle')
    except (ValueError,TypeError,RecursionError):
        raise NewsError('article_identity_not_verified') from None
    if len(declarations)!=1:raise NewsError('article_identity_not_verified')
    declared=declarations[0];title=_key(declared.get('headline'));publisher=declared.get('publisher')
    if declared.get('isAccessibleForFree') in (False,'false'):raise NewsError('paywall_or_login')
    if (not title or declared.get('url')!=requested_url or declared.get('mainEntityOfPage')!=requested_url
            or not isinstance(publisher,dict) or publisher.get('name')!='經理人 Managertoday'
            or publisher.get('url')!=MANAGER_ORIGIN
            or _key(_one(metadata,'meta[property="og:title"]').get('content'))!=title+'|經理人'
            or not _key(_one(metadata,'title').get_text(' ',strip=True)).endswith('|經理人')):
        raise NewsError('article_identity_not_verified')
    headers=soup.select('div.main_box section.article_top')
    if (len(headers)!=2 or len(soup.select('h1'))!=2 or len(soup.select('main.htmlview'))!=2
            or len(soup.select('section.view_box main.htmlview[data-cat=article]'))!=2):
        raise NewsError('private_manager_multiple_root_geometry_not_verified')
    matches=[(i,h) for i,h in enumerate(headers) if _key(_one(h,'h1').get_text(' ',strip=True))==title]
    if len(matches)!=1:raise NewsError('article_identity_not_verified')
    index,header=matches[0];_visible(header);_visible(_one(header,'h1'))
    if any(_REPRINT.match(_key(n.get_text(' ',strip=True)))
            for body in soup.select('main.htmlview') for n in body.find_all(['p','blockquote'])):
        raise NewsError('article_rights_or_scope_excluded')
    author=declared.get('author')
    if (not isinstance(author,dict) or author.get('@type')!='Person' or not isinstance(author.get('name'),str)
            or not 1<=len(author['name'])<=128 or header.select('a[href*="/columnist/"]')):
        raise NewsError('article_attribution_not_verified')
    _,dates=_native_plain_bylines(header,author['name'])
    published,modified=declared.get('datePublished'),declared.get('dateModified')
    _clock(published);_clock(modified)
    if (set(dates)!={published[:10]}
            or _one(metadata,'meta[property="article:published_time"]').get('content')!=published):
        raise NewsError('article_identity_not_verified')
    try:manager_public_written_article(soup,page_url=requested_url,adapter_version=5)
    except NewsError as error:
        if error.code!='article_identity_not_verified':raise
    else:raise NewsError('private_manager_multiple_original_requires_current_failure')
    return dict(contract_version=1,question=MANAGER_MULTIPLE_ORIGINAL_QUESTION,provider=source.provider,
        url=requested_url,response_sha256=hashlib.sha256(response.body).hexdigest(),
        data_fingerprint=source.data_fingerprint,authorization_fingerprint=source.authorization_fingerprint,
        scope='private_failed_two_root_original; matching_header_only_not_body_association_or_article',
        matched_native_header_index=index,header_count=2,main_count=2,
        matched_native_header_and_clock_verified=True,article_roots_bound=False,
        body_geometry_accepted=False,article_accepted=False)


def _static_card_document(template):
    """Observed inert HTML document; metadata is separate from written cards.

    Page parsers keep different HTML/head/body wrappers inside TEMPLATE. Parse
    only this already-received fragment with one native document parser. This
    executes no event handler and visits no iframe, stylesheet or other URL.
    """
    fragment = BeautifulSoup(template.decode_contents(),'lxml')
    if fragment.head is None or fragment.body is None:
        raise NewsError('body_not_found')
    head = fragment.head.find_all(True,recursive=False)
    roots = fragment.body.find_all(True,recursive=False)
    if (len(head)!=3 or [n.name for n in head]!=['meta','meta','title']
            or dict(head[0].attrs)!={'charset':'utf-8'}
            or dict(head[1].attrs)!={'name':'viewport','content':'width=device-width, initial-scale=1'}
            or head[2].attrs or head[2].find(True) is not None or not head[2].get_text(strip=True)
            or len(roots)!=1 or roots[0].name!='div'
            or any(isinstance(n,NavigableString) and not isinstance(n,Comment) and n.strip()
                for scope in (fragment.head,fragment.body) for n in scope.contents)):
        raise NewsError('body_not_found')
    nodes = fragment.body.find_all(True,limit=1025)
    if len(nodes)>1024 or any(n.name not in {'div','span'} for n in nodes):
        raise NewsError('body_not_found')
    events = set()
    for node in nodes:
        if any(key not in {'style',*_CARD_EVENTS} for key in node.attrs):
            raise NewsError('body_not_found')
        _visible(node)
        for key,value in node.attrs.items():
            if key in _CARD_EVENTS:
                if value!=_CARD_EVENTS[key]:
                    raise NewsError('body_not_found')
                events.add(key)
    # Empty/placeholder frames cannot replace missing article text. Source
    # strings stay literal: no ratios, numbers, dates or card labels inferred.
    if not any(text.strip() for text in roots[0].stripped_strings):
        raise NewsError('body_not_found')
    return fragment,dict(document_title=str(head[2].string),
        metadata=[dict(n.attrs) for n in head[:2]],
        written_container_tags=['div','span'],event_attributes_present=sorted(events),
        rendered_or_executed=False,external_resources_requested=False)


def manager_public_written_article(soup, *, page_url, adapter_version=1):
    if type(adapter_version) is not int or adapter_version not in {1, 2, 3, 4, 5}:
        raise NewsError('article_identity_not_verified')
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'www.managertoday.com.tw'
            or parts.query or parts.fragment or not re.fullmatch(MANAGER_PATH, parts.path)):
        raise NewsError('article_identity_not_verified')
    # A static document has its own HEAD. Only the outer publisher HEAD may
    # establish the article identity, title or release clock.
    metadata = soup.head if adapter_version>=5 else soup
    if metadata is None:
        raise NewsError('article_identity_not_verified')
    def header_one(selector):
        return _one(metadata,selector.removeprefix('head > ') if adapter_version>=5 else selector)
    for selector, attr in (('head > link[rel=canonical]', 'href'),
                           ('head > meta[property="og:url"]', 'content')):
        if header_one(selector).get(attr) != page_url:
            raise NewsError('article_identity_not_verified')
    scope = _one(soup, 'div.main_box')
    header = _one(scope, 'section.article_top')
    heading = _one(header, 'h1')
    body = _one(scope, 'section.view_box main.htmlview[data-cat=article]')
    if len(soup.select('h1')) != 1 or len(soup.select('main.htmlview')) != 1:
        raise NewsError('article_identity_not_verified')
    for node in (scope, header, heading, body):
        _visible(node)
    title = _key(heading.get_text(' ', strip=True))
    seo_title = _key(header_one('head > title').get_text(' ', strip=True))
    og_title = _key(header_one('head > meta[property="og:title"]').get('content'))
    if not title or (adapter_version == 1 and any(_key(_one(soup, selector).get(attr) if attr else
            _one(soup, selector).get_text(' ', strip=True)) != title + '|經理人'
            for selector, attr in (('head > title', None), ('head > meta[property="og:title"]', 'content')))):
        raise NewsError('article_title_not_verified')
    if adapter_version == 2 and (not seo_title or seo_title != og_title or not seo_title.endswith('|經理人')):
        raise NewsError('article_title_not_verified')
    if adapter_version >= 3 and (not seo_title.endswith('|經理人')
            or seo_title == '|經理人' or og_title not in {seo_title, title + '|經理人'}):
        raise NewsError('article_title_not_verified')

    declarations = []
    scripts = soup.select('script[type="application/ld+json"]')
    if len(scripts) > 8:
        raise NewsError('article_identity_not_verified')
    try:
        for script in scripts:
            nodes = list(islice(_objects(json.loads(script.get_text())), 257))
            if len(nodes) > 256:
                raise NewsError('article_identity_not_verified')
            declarations.extend(node for node in nodes if node.get('@type') == 'NewsArticle')
    except (ValueError, TypeError, RecursionError):
        raise NewsError('article_identity_not_verified') from None
    if len(declarations) != 1:
        raise NewsError('article_identity_not_verified')
    declared = declarations[0]
    if declared.get('isAccessibleForFree') in (False, 'false'):
        raise NewsError('paywall_or_login')
    publisher = declared.get('publisher')
    if (declared.get('url') != page_url or declared.get('mainEntityOfPage') != page_url
            or _key(declared.get('headline')) != title or not isinstance(publisher, dict)
            or publisher.get('name') != '經理人 Managertoday' or publisher.get('url') != MANAGER_ORIGIN):
        raise NewsError('article_identity_not_verified')
    author = declared.get('author')
    if (not isinstance(author, dict) or author.get('@type') != 'Person'
            or not isinstance(author.get('name'), str) or not 1 <= len(author['name']) <= 128
            or adapter_version == 1 and author.get('name') not in _WRITERS):
        raise NewsError('article_rights_or_scope_excluded')
    name = author['name']
    if adapter_version >= 4 and any(brand in name for brand in _UPSTREAM_BRANDS):
        raise NewsError('article_rights_or_scope_excluded')
    credits = header.select('a[href*="/columnist/"]')
    if len(credits) > 8 or not credits and adapter_version < 3:
        raise NewsError('article_attribution_not_verified')
    labels = []
    credit_dates = []
    profile = _WRITERS.get(name)
    if adapter_version >= 3 and not credits:
        if og_title != title + '|經理人':
            raise NewsError('article_title_not_verified')
        profile = None
        labels, credit_dates = (_native_plain_bylines(header, name) if adapter_version >= 4
            else _plain_writing_credits(header, name))
    elif adapter_version >= 2:
        profiles = {credit.get('href') for credit in credits}
        if len(profiles) != 1:
            raise NewsError('article_attribution_not_verified')
        profile = profiles.pop()
        if not isinstance(profile, str) or not re.fullmatch(re.escape(MANAGER_ORIGIN) + r'/columnist/view/[1-9][0-9]*', profile):
            raise NewsError('article_attribution_not_verified')
    for credit in credits:
        _visible(credit)
        if credit.get('href') != profile or credit.get('data-dl_text') != name:
            raise NewsError('article_attribution_not_verified')
        label = _key(credit.get_text(' ', strip=True))
        if label:
            if label != '撰文 ' + name and not ((adapter_version >= 4 or adapter_version >= 2
                    and name == '支琬清' and profile == _WRITERS[name])
                    and label == '經理人用戶成長中心 ' + name):
                raise NewsError('article_attribution_not_verified')
            labels.append(label)
    if not labels:
        raise NewsError('article_attribution_not_verified')
    published, modified = declared.get('datePublished'), declared.get('dateModified')
    release, revision = _clock(published), _clock(modified)
    if (adapter_version == 1 and revision < release
            or header_one('head > meta[property="article:published_time"]').get('content') != published):
        raise NewsError('article_identity_not_verified')
    displayed = {value.strip() for value in header.stripped_strings
                 if re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value.strip())}
    if displayed != {published[:10]}:
        raise NewsError('article_identity_not_verified')
    if credit_dates and set(credit_dates) != {published[:10]}:
        raise NewsError('article_identity_not_verified')

    if any(_REPRINT.match(_key(node.get_text(' ', strip=True)))
           for node in body.find_all(['p', 'blockquote'])):
        raise NewsError('article_rights_or_scope_excluded')
    nodes = body.find_all(True, limit=16385)
    if len(nodes) > 16384:
        raise NewsError('body_not_found')
    templates = body.select('template')
    static_documents, document_nodes, static_fragments = [],set(),{}
    for index,node in enumerate(templates):
        # A specific native static table declaration, not an iframe response,
        # hidden article body, script-generated prose or arbitrary template.
        if (dict(node.attrs) != {'x-ref': 'src'} or node.parent.name != 'div'
                or node.parent.get('x-data') != 'iframe'
                or set(node.parent.get('class', [])) != {'widget', 'w-full', 'iframe'}
                or len(node.parent.select('template')) != 1):
            raise NewsError('body_not_found')
        if adapter_version>=5 and not node.select('table td'):
            fragment,declaration = _static_card_document(node)
            static_fragments[index] = fragment
            static_documents.append(declaration)
            document_nodes.update(id(t) for t in node.find_all(True))
        elif (not node.select('table td')
                or any(t.name not in _TAGS-{'template'} for t in node.find_all(True))):
            raise NewsError('body_not_found')
    if any(node.name not in _TAGS and not (id(node) in document_nodes
            and node.name in {'html','head','body','meta','title'}) for node in nodes):
        raise NewsError('body_not_found')
    for node in nodes:
        if node.name not in {'script', 'style', 'template'}:
            # Ancestors outside the body were checked above. Check each body
            # node once, rather than walking its ancestors for every text node.
            style = re.sub(r'\s+', '', str(node.get('style', ''))).lower()
            if (node.has_attr('hidden') or str(node.get('aria-hidden', '')).lower() == 'true'
                    or 'display:none' in style or 'visibility:hidden' in style):
                raise NewsError('article_identity_not_verified')
    normalized = deepcopy(body)
    for index,template in enumerate(normalized.select('template')):
        fragment = (static_fragments[index] if index in static_fragments
            else BeautifulSoup(template.decode_contents(), 'lxml'))
        replacement = soup.new_tag('div')
        for child in list(fragment.body.contents if fragment.body else fragment.contents):
            replacement.append(child.extract())
        template.replace_with(replacement)
    for node in normalized.select('script, style'):
        node.decompose()
    normalized.name = 'div'
    normalized.attrs = {'class': ['manager-native-body']}
    output_scope = soup.new_tag('div')
    output_scope.append(normalized)
    notices = [_key(n.get_text(' ', strip=True)) for n in body.select('.widget.textblock')]
    notice_fields = {'native_textblock_notices': notices,
        'inert_table_declarations': len(templates)-len(static_documents), 'inert_tables_rendered_or_executed': False,
        'native_visible_date': sorted(displayed), 'reuse_rights_granted': False}
    if adapter_version >= 2:
        notice_fields.update(native_headline=title, declared_headline=declared.get('headline'),
            html_title=seo_title, og_title=og_title,
            headline_source='native_h1; SEO_declarations_preserved_separately',
            modified_before_published_declaration=revision < release,
            clock_order_independently_verified=False)
    if adapter_version>=5:
        notice_fields.update(inert_static_document_declarations=static_documents,
            inert_document_metadata_not_article_title_or_release=True)
    return {
        'headline': title, 'author': [{'name': name}],
        'datePublished': published, 'dateModified': modified,
        'article_identity_basis': {
            1: 'literal_final_canonical_OG_URL_native_heading_JSONLD_publisher_and_paired_editor_profile_credit_v1',
            2: 'literal_final_canonical_OG_URL_native_heading_JSONLD_publisher_and_paired_native_writer_profile_credit_v2',
            3: 'literal_final_canonical_OG_URL_native_OG_JSONLD_headline_publisher_and_paired_native_writing_credit_v3',
            4: 'literal_final_canonical_OG_URL_native_OG_JSONLD_headline_publisher_and_matching_native_literal_byline_v4',
            5: 'literal_final_canonical_OG_URL_native_OG_JSONLD_headline_publisher_literal_byline_and_static_document_v5',
        }[adapter_version],
        'author_attribution_detail_json': json.dumps({'native_labels': labels,
            'profile_url': profile, 'profile_role': ('publisher_declared_editor' if adapter_version == 1
                else 'publisher_declared_editorial_center_credit' if adapter_version >= 4 and profile
                    and labels[0] == '經理人用戶成長中心 ' + name
                else 'publisher_declared_writer' if profile else 'publisher_declared_plain_byline' if adapter_version >= 4
                else 'publisher_declared_plain_writing_credit'),
            **({'native_credit_dates': credit_dates,
                ('byline_basis' if adapter_version >= 4 else 'writing_credit_basis'):
                    ('adjacent_native_date_literal_byline_and_matching_JSONLD_declaration' if adapter_version >= 4
                        else 'adjacent_native_date_plain_writing_label_and_matching_JSONLD_Person')}
                if credit_dates else {}),
            **({'literal_name_kind': ('editorial_desk' if name in {'經理人編輯部', '經理人用戶成長中心'}
                    else 'unparsed_composite' if '、' in name else 'source_declared_literal_name'),
                'declared_schema_author_type': author['@type'],
                'name_split_or_natural_person_inferred': False} if adapter_version >= 4 else {}),
            'person_identity_or_ownership_independently_verified': False}, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notice_fields, ensure_ascii=False),
        'published_time_basis': 'publisher_current_JSONLD_release_and_revision_with_explicit_offset; '
            'matching_native_header_date; not_observed_historical_PIT',
        'body_completeness': ('current_native_main_written_prose_tables_and_static_inert_table_and_card_documents_once; '
            if adapter_version>=5 and static_documents else 'current_native_main_written_prose_tables_and_static_inert_table_declarations_once; ') +
            'inert_templates_not_executed_or_render_verified; script_style_and_neighboring_stories_excluded; '
            'media_unfetched; not_original_historical_version',
    }, output_scope
