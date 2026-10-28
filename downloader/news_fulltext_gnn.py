"""GNN's native public article, including explicitly labelled supplier prose.

No HTTP, RSS, media traversal, rendering, alias synthesis or reuse-rights grant.
The GNN reporter credit is not proof that the reporter wrote supplier content.
"""
from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from itertools import islice
import json
import re
from urllib.parse import urljoin, urlsplit, urlunsplit

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


GNN_ORIGIN = 'https://gnn.gamer.com.tw'
GNN_PATH = r'^/detail[.]php$'
GNN_BODY = ':scope > div.GN-lbox3B'
GNN_LAYOUT = 'gnn_native_reporter_article_v1'
GNN_CREDITED_LAYOUT = 'gnn_native_credited_article_v2'
GNN_WRITTEN_LAYOUT = 'gnn_native_written_article_v3'
GNN_LEGACY_LAYOUT = 'gnn_native_legacy_article_v4'
GNN_LAYOUTS = frozenset({GNN_LAYOUT, GNN_CREDITED_LAYOUT, GNN_WRITTEN_LAYOUT, GNN_LEGACY_LAYOUT})
GNN_METADATA_EVIDENCE = 48
GNN_METADATA_QUESTION = 'gnn_current_native_credit_schema_and_body_structure_v1'
GNN_BOUNDARY_ORIGINAL_QUESTION = 'gnn_private_current_document_boundary_original_v1'
GNN_ROUTE_EVIDENCE = 50
GNN_ROUTE_QUESTION = 'gnn_current_literal_first_hop_route_and_status_v1'
_TAGS = frozenset({'div','p','br','h2','h3','h4','h5','h6','a','span','b','strong',
    'em','i','u','s','del','sup','sub','blockquote','pre','code','ul','ol','li',
    'hr','figure','figcaption','picture','source','img','section','table',
    'thead','tbody','tfoot','tr','td','th','caption','colgroup','col'})
_TEMPLATE_TAGS = _TAGS | {'html','head','body'}
_SUPPLIER_NOTICE = '【以下內容為廠商提供資料原文】'
_FOLLOW_CARD_TEMPLATE = '''<div class="gamecard__background"></div>
<div class="gamecard__game"><div class="gamecard-img"><a href="javascript:;"></a></div>
<div class="gamecard-name"><p class="gamecard-label-name"></p>
<p class="gamecard-label-people"><span></span> 人已追蹤，快追蹤取得最新情報！</p></div></div>
<div class="gamecard__data"><div class="gamecard__info">
<a href="javascript:;" class="gamecard__follow" data-want2playsn="__ids__"></a></div></div>'''
_SLICK_GALLERY_SCRIPT = '''jQuery(function() {
jQuery('.__gallery__ .banner-horizontal').slick({
slidesToShow: 1,
slidesToScroll: 1,
arrows: true,
prevArrow: '<div class="slick-prev" aria-label="Previous" type="button"><div></div><div></div></div>',
nextArrow: '<div class="slick-next" aria-label="Next" type="button"><div></div><div></div></div>',
asNavFor: '.__gallery__ .horizontal-nav'
});
jQuery('.__gallery__ .horizontal-nav').slick({
slidesToShow: 5,
slidesToScroll: 1,
asNavFor: '.__gallery__ .banner-horizontal',
arrows: false,
dots: false,
centerMode: false,
centerPadding: '8px',
focusOnSelect: true
});
let imgLength = jQuery('.__gallery__ .slider-count').data('length');
jQuery('.__gallery__ .banner-horizontal').on('afterChange', function() {
let currentIndex = jQuery('.__gallery__ .banner-horizontal').slick('slickCurrentSlide') + 1;
jQuery('.__gallery__ .slider-count span').text(currentIndex + '/' + imgLength);
});
});'''


def _template_shape(node):
    """Compare all template text/attributes/children, apart from whitespace."""
    if not getattr(node, 'name', None):
        return _key(str(node))
    attrs = tuple(sorted((k, tuple(v) if isinstance(v, list) else v)
        for k, v in node.attrs.items()))
    children = tuple(shape for child in node.children if (shape := _template_shape(child)))
    return node.name, attrs, children


@lru_cache(maxsize=1)
def _follow_card_shape():
    from bs4 import BeautifulSoup
    return _template_shape(BeautifulSoup(_FOLLOW_CARD_TEMPLATE, 'lxml'))


def _follow_card_embed(node, *, allow_empty_ids=False):
    """Recognize only the witnessed inert follow UI, never arbitrary prose."""
    if (node.name != 'script' or node.attrs != {
            'id': 'wallFanspageCardTemplate', 'type': 'text/template'}):
        return None
    from bs4 import BeautifulSoup
    import hashlib
    value = node.get_text()
    if len(value) > 32768:
        return None
    fragment = BeautifulSoup(value, 'lxml')
    if len(fragment.find_all(True, limit=65)) > 64:
        return None
    links = fragment.select('a.gamecard__follow[data-want2playsn]')
    if len(links) != 1:
        return None
    try:
        ids = json.loads(links[0]['data-want2playsn'])
    except (ValueError, TypeError, RecursionError):
        return None
    if (not isinstance(ids, list) or not (0 if allow_empty_ids else 1) <= len(ids) <= 128
            or any(type(i) is not int or not 0 < i <= 2**53 for i in ids)):
        return None
    links[0]['data-want2playsn'] = '__ids__'
    if _template_shape(fragment) != _follow_card_shape():
        return None
    return {'kind': 'inert_publisher_follow_card_ui', 'written_article_text': False,
        'characters': len(value), 'sha256': hashlib.sha256(value.encode()).hexdigest(),
        'linked_game_count': len(ids), 'media_or_links_fetched': False}


def _legacy_youtube_embed(node):
    """Attest an empty legacy wrapper; never load Flash, video or scripts."""
    if (node.name != 'object' or node.attrs != {'width':'600','height':'400'}
            or _key(node.get_text(' ',strip=True))):
        return None
    children=node.find_all(True,limit=5)
    if len(children)!=4 or [n.name for n in children]!=['param','param','param','embed']:
        return None
    src=children[0].get('value','')
    if not isinstance(src,str) or not re.fullmatch(r'https://www[.]youtube[.]com/embed/[A-Za-z0-9_-]{11}',src):
        return None
    expected=[{'name':'movie','value':src},{'name':'allowFullScreen','value':'true'},
        {'name':'allowscriptaccess','value':'always'},
        {'src':src,'type':'application/x-shockwave-flash','allowscriptaccess':'always',
            'allowfullscreen':'true','width':'600','height':'400'}]
    if any(child.attrs!=attrs for child,attrs in zip(children,expected)):
        return None
    return {'kind':'unfetched_legacy_youtube_wrapper','src':src,'executed':False,
        'written_article_text':False}


def _gallery_embed(node, body):
    """Exact witnessed carousel program and text-free media DOM, without JS."""
    if node.name!='script' or node.attrs:
        return None
    import hashlib
    value=node.get_text()
    if len(value)>4096:
        return None
    refs=re.findall(r'gnn-slick-banner-horizontal-[0-9]{13}',value)
    if not refs or len(set(refs))!=1:
        return None
    gallery=refs[0]
    if _key(value.replace(gallery,'__gallery__'))!=_key(_SLICK_GALLERY_SCRIPT):
        return None
    roots=body.select('div.'+gallery)
    if len(roots)!=1 or roots[0].attrs!={'class':[gallery,'header-banner']}:
        return None
    root=roots[0]
    counts=root.select(':scope > div.slider-count')
    main=root.select('.banner-horizontal');nav=root.select('.horizontal-nav')
    if len(counts)!=1 or len(main)!=1 or len(nav)!=1:
        return None
    counter=counts[0];length=counter.get('data-length','')
    if (not re.fullmatch(r'[1-9][0-9]{0,2}',str(length))
            or counter.attrs!={'class':['slider-count'],'data-length':length}
            or len(main[0].select('img'))!=int(length) or len(nav[0].select('img'))!=int(length)):
        return None
    children=counter.find_all(True,limit=2)
    indicator='1/'+length
    if (len(children)!=1 or children[0].name!='span'
            or children[0].attrs!={'class':['slider-count-span']}
            or _key(root.get_text(' ',strip=True))!=indicator):
        return None
    return {'kind':'unexecuted_publisher_image_gallery_ui','gallery_class':gallery,
        'ui_counter':indicator,'media_count':int(length),'written_article_text':False,
        'script_sha256':hashlib.sha256(value.encode()).hexdigest(),'executed':False,
        'media_or_links_fetched':False}


def gnn_metadata_source(source):
    """Diagnose the current reviewed source; this grants no new source scope."""
    return (source.provider == 'gnn' and source.allowed and source.request_enabled
        and source.domains == ('gnn.gamer.com.tw',)
        and source.embedded_article_layout in GNN_LAYOUTS
        and source.authorization.get('kind') == 'personal_research'
        and source.authorization.get('evidence_url') == GNN_ORIGIN + '/')


def gnn_literal_route(url):
    """The recorded literal address, including HTTP; never infer an alias."""
    return isinstance(url, str) and re.fullmatch(
        r'https?://gnn[.]gamer[.]com[.]tw/detail[.]php[?]sn=[1-9][0-9]*', url) is not None


def gnn_route_metadata(response, source, requested_url):
    """Observe the authorized first hop only, without following its Location.

    Other destinations retain only origin/path and query key names. Tokens,
    response prose and arbitrary headers never enter this finite observation.
    Only a literal GNN article Location can be retained in full; it still
    grants no alias, destination fetch or article acceptance.
    """
    import hashlib
    if (not gnn_metadata_source(source) or source.embedded_article_layout != GNN_LEGACY_LAYOUT
            or not gnn_literal_route(requested_url) or response.url != requested_url or response.redirects
            or type(response.status) is not int or not 200 <= response.status < 500
            or response.status in {401, 403, 429, 451}
            or not isinstance(response.body, bytes) or len(response.body) > 2 << 20):
        raise NewsError('gnn_first_hop_scope_not_verified')
    mime = response.headers.get('content-type', '').split(';', 1)[0].strip().lower()
    result = {'contract_version': 1, 'evidence_contract_version': GNN_ROUTE_EVIDENCE,
        'provider': source.provider, 'question': GNN_ROUTE_QUESTION,
        'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
        'status': response.status, 'content_type': mime if re.fullmatch(r'[a-z0-9.+_-]+/[a-z0-9.+_-]+', mime) else None,
        'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'current_source_not_candidate': True, 'destination_requested': False,
        'accepted_as_article': False, 'body_completeness_verified': False,
        'identity_or_alias_accepted': False, 'raw_or_body_retained': False,
        'raw_or_names_or_body_retained': False}
    if response.status not in {301, 302, 303, 307, 308}:
        return result
    location = response.headers.get('location')
    result['location_present'] = isinstance(location, str) and bool(location)
    if not result['location_present']:
        return result
    result['location_sha256'] = hashlib.sha256(location.encode()).hexdigest()
    result['location_valid_http_route'] = False
    if len(location) > 2048 or any(ord(char) <= 32 or ord(char) == 127 or char == '\\' for char in location):
        return result
    try:
        target = urljoin(requested_url, location)
        parts = urlsplit(target)
        if (parts.scheme not in {'http', 'https'} or not parts.hostname or parts.username or parts.password
                or parts.port is not None or len(parts.path) > 512 or len(parts.netloc) > 253):
            return result
        keys = [parameter.split('=', 1)[0] for parameter in parts.query.split('&')] if parts.query else []
        if len(keys) > 16 or any(not re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', key) for key in keys):
            return result
        result.update(location_valid_http_route=True,
            target_origin_path=urlunsplit((parts.scheme, parts.netloc, parts.path or '/', '', '')),
            target_query_keys=keys, target_fragment_present=bool(parts.fragment))
        if gnn_literal_route(target):
            result.update(literal_gnn_article_target=target, literal_location=location)
    except (ValueError, UnicodeError):
        pass
    return result


def private_gnn_boundary_original_permit(response, source, requested_url):
    """Retain a current private diagnostic, never accept unknown article text.

    A native byline/body failure is exactly what needs offline investigation.
    Attest the literal document and access scope; do not invent a verified
    reporter, body boundary, historical version, source alias or reuse right.
    """
    import hashlib
    from downloader.news_fulltext_policy import public_article_document
    from downloader.news_fulltext_candidate_originals import MAX_ORIGINAL_BYTES
    parts=urlsplit(requested_url or '')
    if (not gnn_metadata_source(source) or parts.scheme!='https' or parts.netloc!='gnn.gamer.com.tw'
            or parts.path!='/detail.php' or not re.fullmatch(r'sn=[1-9][0-9]*',parts.query) or parts.fragment
            or response.status!=200 or response.redirects or response.url!=requested_url
            or not isinstance(response.body,bytes) or not 0<len(response.body)<=MAX_ORIGINAL_BYTES
            or b'\x00' in response.body
            or response.headers.get('content-type','').split(';',1)[0].strip().lower()
                not in {'text/html','application/xhtml+xml'}):
        raise NewsError('private_gnn_original_scope_not_verified')
    soup=public_article_document(response.body,source,page_url=requested_url)
    scope=_one(soup,'div.BH-lbox.GN-lbox3.gnn-detail-cont')
    heading=_one(scope,':scope > div:not([class]) h1')
    body=_one(scope,GNN_BODY)
    for node in (scope,heading,body):_visible(node)
    title=_key(heading.get_text(' ',strip=True))
    if (_one(soup,'head > meta[property="og:url"]').get('content')!=requested_url
            or _key(_one(soup,'head > meta[property="og:title"]').get('content'))!=title
            or len(soup.select('h1'))!=1 or len(soup.select('div.GN-lbox3B'))!=1):
        raise NewsError('private_gnn_original_scope_not_verified')
    declarations=[]
    scripts=soup.select('script[type="application/ld+json"]')
    if len(scripts)>8:raise NewsError('private_gnn_original_scope_not_verified')
    try:
        for script in scripts:
            nodes=list(islice(_objects(json.loads(script.get_text())),257))
            if len(nodes)>256:raise NewsError('private_gnn_original_scope_not_verified')
            declarations.extend(n for n in nodes if n.get('@type')=='NewsArticle')
    except (ValueError,TypeError,RecursionError):
        raise NewsError('private_gnn_original_scope_not_verified') from None
    if len(declarations)!=1:raise NewsError('private_gnn_original_scope_not_verified')
    article=declarations[0]
    if article.get('isAccessibleForFree') in (False,'false'):raise NewsError('paywall_or_login')
    if (article.get('mainEntityOfPage')!={'@type':'WebPage','@id':requested_url}
            or article.get('url',requested_url)!=requested_url or _key(article.get('headline'))!=title
            or not isinstance(article.get('publisher'),dict)
            or article['publisher'].get('name')!='巴哈姆特電玩資訊站'):
        raise NewsError('private_gnn_original_scope_not_verified')
    return {'contract_version':1,'question':GNN_BOUNDARY_ORIGINAL_QUESTION,'provider':'gnn','url':requested_url,
        'response_sha256':hashlib.sha256(response.body).hexdigest(),'data_fingerprint':source.data_fingerprint,
        'authorization_fingerprint':source.authorization_fingerprint,
        'scope':'known_literal_current_public_GNN_document_private_diagnostic_only',
        'native_permit':{'literal_native_document_declarations_agree':True,
            'byline_verified':False,'body_boundary_verified':False,'historical_identity_verified':False},
        'body_completeness_verified':False}


def gnn_native_metadata(soup):
    """Bounded public header declarations and element shapes, never prose."""
    from collections import Counter
    from downloader.news_fulltext_document_metadata import _scalar
    scopes = soup.select('div.BH-lbox.GN-lbox3.gnn-detail-cont')
    bodies = soup.select('div.GN-lbox3B')
    result = {'scope_count': len(scopes), 'body_container_count': len(bodies),
        'headers': [], 'body_structure_without_prose': [],
        'metadata_not_verified_authorship': True, 'body_text_exported': False}
    for scope in scopes[:2]:
        headers = scope.select(':scope > div:not([class])')
        result['headers'].append({'header_count': len(headers), 'declarations': [
            {'headings': [_scalar(n.get_text(' ', strip=True)) for n in h.select('h1')[:3]],
             'credits': [_scalar(n.get_text(' ', strip=True))
                for n in h.select('p.GN-lbox3A > span.GN-lbox3C')[:4]]}
            for h in headers[:2]]})
    for body in bodies[:2]:
        nodes = body.find_all(True, limit=16385)
        unknown = Counter(n.name for n in nodes[:16384] if n.name not in _TAGS)
        result['body_structure_without_prose'].append({
            'nodes_examined': min(len(nodes), 16384), 'node_limit_reached': len(nodes) > 16384,
            'unknown_element_counts': dict(islice(sorted(unknown.items()), 16)),
            'embeds': [{key: _scalar(value) for key, value in {
                'tag': n.name, 'src': n.get('src'), 'type': n.get('type'),
                'classes': ' '.join(n.get('class', []))}.items()}
                for n in islice((n for n in nodes[:16384] if n.name not in _TAGS), 8)]})
    return result


def _key(value):
    return re.sub(r'\s+', ' ', value).strip() if isinstance(value, str) else ''


def _clock(value):
    if not isinstance(value,str) or not re.fullmatch(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00',value):
        raise NewsError('article_identity_not_verified')
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('article_identity_not_verified') from None


def _nonprose_embed(node):
    """Prove a local empty media wrapper, without executing or fetching it."""
    if node.name == 'iframe':
        src = node.get('src', '')
        if (isinstance(src,str) and re.fullmatch(r'https://www[.]youtube[.]com/embed/[A-Za-z0-9_-]{11}(?:[?]wmode=transparent)?', src)
                and not _key(node.get_text(' ', strip=True)) and not node.find(True)):
            return {'kind':'unfetched_youtube_video','src':src}
    if node.name == 'script' and node.get('type') == 'text/template' and not node.has_attr('src'):
        from bs4 import BeautifulSoup
        import hashlib
        value = node.get_text()
        if len(value) > 32768:
            return None
        fragment = BeautifulSoup(value, 'lxml')
        nodes = fragment.find_all(True, limit=1025)
        if (len(nodes) <= 1024 and not _key(fragment.get_text(' ', strip=True))
                and all(n.name in _TEMPLATE_TAGS for n in nodes)):
            return {'kind':'inert_template_without_written_text',
                'characters':len(value),'sha256':hashlib.sha256(value.encode()).hexdigest()}
    return None


def gnn_native_article(soup, *, page_url, layout=GNN_LAYOUT):
    if layout not in GNN_LAYOUTS:
        raise NewsError('article_identity_not_verified')
    credited = layout != GNN_LAYOUT
    written = layout in {GNN_WRITTEN_LAYOUT,GNN_LEGACY_LAYOUT}
    legacy_body = layout == GNN_LEGACY_LAYOUT
    parts = urlsplit(page_url or '')
    if (parts.scheme!='https' or parts.netloc!='gnn.gamer.com.tw'
            or parts.path!='/detail.php' or not re.fullmatch(r'sn=[1-9][0-9]*',parts.query)
            or parts.fragment):
        raise NewsError('article_identity_not_verified')
    if _one(soup,'head > meta[property="og:url"]').get('content') != page_url:
        raise NewsError('article_identity_not_verified')
    canonicals=soup.select('head > link[rel=canonical]')
    if len(canonicals)>1 or canonicals and canonicals[0].get('href')!=page_url:
        raise NewsError('article_identity_not_verified')
    scope=_one(soup,'div.BH-lbox.GN-lbox3.gnn-detail-cont')
    header=_one(scope,':scope > div:not([class])')
    heading=_one(header,'h1')
    credits=header.select('p.GN-lbox3A > span.GN-lbox3C')
    legacy = header.select('p.GN-lbox3A > span.GN-lbox3CA') if written else []
    if legacy and (len(legacy) != 1 or credits or len(header.select('p.GN-lbox3A')) != 1):
        raise NewsError('article_identity_not_verified')
    credit=(_one(header,'p.GN-lbox3A') if credited and not credits
        else _one(header,'p.GN-lbox3A > span.GN-lbox3C'))
    if legacy:
        credit = legacy[0]
    body=_one(scope,GNN_BODY)
    if len(soup.select('h1'))!=1 or len(soup.select('div.GN-lbox3B'))!=1:
        raise NewsError('article_identity_not_verified')
    for node in (scope,header,heading,credit,body):
        _visible(node)
    if credited:
        credit_nodes = credit.find_all(True,limit=65)
        credit_tags = {'a','span','time','br'} | ({'font'} if legacy else set())
        if len(credit_nodes)>64 or any(n.name not in credit_tags for n in credit_nodes):
            raise NewsError('article_identity_not_verified')
        for node in credit_nodes:
            _visible(node)
    title=_key(heading.get_text(' ',strip=True))
    html_title = _key(_one(soup,'head > title').get_text(' ',strip=True))
    title_agrees = (bool(re.fullmatch(re.escape(title)+r'(?:《[^<>]{1,200}》)? - 巴哈姆特', html_title))
        if credited else html_title==title+' - 巴哈姆特')
    if (not title or _key(_one(soup,'head > meta[property="og:title"]').get('content'))!=title or not title_agrees):
        raise NewsError('article_title_not_verified')
    label=_key(credit.get_text(' ',strip=True))
    native=re.fullmatch(r'（GNN 記者 (.{1,80}) 報導） ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8})',label)
    supplier = None
    if credited and native is None:
        supplier = re.fullmatch(r'（廠商 (.{1,80}) 提供） ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8})',label)
        native = supplier
    editorial = re.fullmatch(r'（編輯部 報導） ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8})', label) if written else None
    press = re.fullmatch(r'（新聞稿） ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8}) (.{1,80})', label) if legacy else None
    contributing = re.fullmatch(r'（特約編輯 (.{1,80})） ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8})',label) if legacy_body else None
    if legacy and press is None:
        raise NewsError('article_attribution_not_verified')
    if native is None and editorial is None and press is None and contributing is None:
        raise NewsError('article_attribution_not_verified')
    auxiliary_credit = None
    if press:
        fonts = credit.select(':scope > font')
        if (len(fonts) != 1 or len(credit.find_all(True)) != 1
                or fonts[0].attrs != {'color': 'white', 'size': '2'}
                or _key(fonts[0].get_text(' ', strip=True)) != press[2]):
            raise NewsError('article_attribution_not_verified')
        native_name, native_release, declared_name = '新聞稿', press[1], press[2]
        role = 'publisher_declared_press_release'
        auxiliary_credit = press[2]
    elif contributing:
        native_name,native_release=contributing[1],contributing[2]
        declared_name='特約編輯 '+native_name
        role='publisher_declared_contributing_editor'
    elif editorial:
        native_name, native_release, declared_name = '編輯部', editorial[1], '編輯部-'
        role = 'publisher_declared_editorial_desk'
    else:
        native_name, native_release = native[1], native[2]
        declared_name = '廠商-' + native_name if supplier else native_name
        role = 'publisher_declared_supplier_credit' if supplier else 'publisher_declared_GNN_reporter'
    scripts=soup.select('script[type="application/ld+json"]')
    if len(scripts)>8:
        raise NewsError('article_identity_not_verified')
    declarations=[]
    try:
        for script in scripts:
            nodes=list(islice(_objects(json.loads(script.get_text())),257))
            if len(nodes)>256:
                raise NewsError('article_identity_not_verified')
            declarations.extend(n for n in nodes if n.get('@type')=='NewsArticle')
    except (ValueError,TypeError,RecursionError):
        raise NewsError('article_identity_not_verified') from None
    if len(declarations)!=1:
        raise NewsError('article_identity_not_verified')
    declared=declarations[0]
    if declared.get('isAccessibleForFree') in (False,'false'):
        raise NewsError('paywall_or_login')
    if (declared.get('mainEntityOfPage')!={'@type':'WebPage','@id':page_url}
            or declared.get('url',page_url)!=page_url
            or _key(declared.get('headline'))!=title
            or not isinstance(declared.get('publisher'),dict)
            or declared['publisher'].get('name')!='巴哈姆特電玩資訊站'):
        raise NewsError('article_identity_not_verified')
    if declared.get('author')!={'@type':'Person','name':declared_name}:
        raise NewsError('article_attribution_not_verified')
    published,modified=declared.get('datePublished'),declared.get('dateModified')
    release,revision=_clock(published),_clock(modified)
    if native_release.replace(' ','T')+'+08:00'!=published:
        raise NewsError('article_identity_not_verified')
    nodes=body.find_all(True,limit=16385)
    if len(nodes)>16384:
        raise NewsError('body_not_found')
    embeds = []
    approved_media_children=set()
    for node in nodes:
        if (node.name in _TAGS or written and node.name == 'font'
                or legacy_body and node.name in {'center','wbr'} or id(node) in approved_media_children):
            continue
        embed = _nonprose_embed(node) if credited else None
        if embed is None and written:
            embed = _follow_card_embed(node,allow_empty_ids=legacy_body)
        if embed is None and legacy_body:
            embed = _legacy_youtube_embed(node)
            if embed is not None:
                approved_media_children.update(id(child) for child in node.find_all(True))
        if embed is None and legacy_body:
            embed = _gallery_embed(node,body)
        if embed is None:
            raise NewsError('body_not_found')
        embeds.append(embed)
        if len(embeds)>32:
            raise NewsError('body_not_found')
    for node in nodes:
        style=re.sub(r'\s+','',str(node.get('style',''))).lower()
        if (node.has_attr('hidden') or str(node.get('aria-hidden','')).lower()=='true'
                or 'display:none' in style or 'visibility:hidden' in style):
            raise NewsError('article_identity_not_verified')
    notices=[_key(n.get_text(' ',strip=True)) for n in body.select('p.caution')]
    galleries=[embed for embed in embeds if embed['kind']=='unexecuted_publisher_image_gallery_ui']
    if galleries:
        from copy import deepcopy
        scope=deepcopy(scope)
        for gallery in galleries:
            _one(scope,'div.'+gallery['gallery_class']+' > div.slider-count').decompose()
    return {'headline':title,'author':[{'name':native_name}],
        'datePublished':published,'dateModified':modified,
        'article_identity_basis':('literal_final_OG_URL_native_heading_header_role_release_and_matching_JSONLD_GNN_publisher_v4'
            if legacy_body else 'literal_final_OG_URL_native_heading_header_role_release_and_matching_JSONLD_GNN_publisher_v3'
            if written else 'literal_final_OG_URL_native_heading_header_credit_release_and_matching_JSONLD_GNN_publisher_v2'
            if credited else 'literal_final_OG_URL_native_heading_header_reporter_release_and_matching_JSONLD_GNN_publisher_v1'),
        'author_attribution_detail_json':json.dumps({'native_label':label,
            'role':role,'native_name':native_name,
            'all_body_written_or_owned_by_reporter_verified':False,
            'person_identity_independently_verified':False,
            **({'publisher_JSONLD_author':declared['author'],
                'publisher_declared_Person_type_independently_verified':False} if credited else {}),
            **({'legacy_auxiliary_credit_name':auxiliary_credit,
                'legacy_auxiliary_credit_visually_attributed':False,
                'authors_field_is_press_release_label_not_individual_author':True} if press else {})},ensure_ascii=False),
        'verified_article_notices_json':json.dumps({'native_content_notices':notices,
            'supplier_content_declared':bool(supplier) or _SUPPLIER_NOTICE in notices,
            'publisher_may_include_supplier_provided_prose':True,
            'reuse_rights_granted':False,'rss_used':False,
            'modified_before_published_declaration':revision<release,
            'clock_order_independently_verified':False,
            **({'native_headline':title,'html_title':html_title,
                **({'native_embedded_nonarticle_content':embeds} if written else {
                    'unfetched_or_verified_text_empty_embeds':embeds})} if credited else {})},ensure_ascii=False),
        'published_time_basis':'current_publisher_JSONLD_explicit_UTC08_release_and_revision; exact_native_header_release; not_observed_historical_PIT',
        'body_completeness':'current_native_GN-lbox3B_written_prose_and_tables; supplier_notices_preserved; '
            'media_unfetched; neighboring_stories_excluded; not_original_historical_version'
            + ('; witnessed_gallery_UI_counter_excluded_and_raw_indicator_retained' if galleries else ''),
    },scope
