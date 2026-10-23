"""SETN originals and iNEWS native microdata with raw source-clock declarations.

This is a pure native validator. It neither follows legacy URLs nor interprets
the schema's UTC label as a verified local/publication or historical PIT clock.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urljoin, urlsplit

from bs4 import Comment, NavigableString, Tag

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


SETN_LAYOUT = 'setn_public_native_article_v1'
SETN_PATH_PATTERN = r'^/news/[1-9][0-9]*$'
SETN_BODY_SELECTOR = (':scope > .article_content_wrap > #article_content > #newsContent > p, '
                      ':scope > .article_content_wrap > #article_content > #newsContent > figure > figcaption')
SETN_AUTHOR_SELECTOR = '.article_time_wrap .author_wrap > a, #newsContent > p:first-child'
SETN_REVIEWED_URLS = ('https://www.setn.com/news/1911453', 'https://www.setn.com/news/431945')
INEWS_PROVIDER = 'setn_inews_scope'
INEWS_LAYOUT = 'setn_inews_native_article_v1'
INEWS_LAYOUT_VERSIONS = {INEWS_LAYOUT: 1, 'setn_inews_native_article_v2': 2,
                        'setn_inews_native_article_v3': 3, 'setn_inews_native_article_v4': 4,
                        'setn_inews_native_article_v5': 5, 'setn_inews_native_article_v6': 6,
                        'setn_inews_native_article_v7': 7, 'setn_inews_native_article_v8': 8}
INEWS_BOUNDARY_QUESTION = 'inews_native_editor_credit_shapes_v1'
INEWS_ORIGIN = 'https://inews.setn.com'
INEWS_SCOPE_SELECTOR = '#newsArea.stockNewArea > .leftCon'
INEWS_EDITOR_SELECTOR = ':scope > .flexbox > #ckuse.pageText.printdiv > article > .newsCon'
INEWS_BODY_SELECTOR = (INEWS_EDITOR_SELECTOR + ' > [itemprop=articleBody] > p, '
                       + INEWS_EDITOR_SELECTOR + ' > [itemprop=articleBody] > figure > figcaption')
INEWS_V8_BODY_SELECTOR = (INEWS_BODY_SELECTOR + ', '
    + INEWS_EDITOR_SELECTOR + ' > [itemprop=articleBody] > ul > li > p, '
    + INEWS_EDITOR_SELECTOR + ' > [itemprop=articleBody] > ol > li > p')
INEWS_AUTHOR_SELECTOR = INEWS_EDITOR_SELECTOR + ' > .article_remark_wrap > .author_wrap > a[itemprop=author]'
INEWS_V4_AUTHOR_SELECTOR = INEWS_AUTHOR_SELECTOR + ', ' + INEWS_EDITOR_SELECTOR + ' > [itemprop=articleBody] > p'
INEWS_TITLE_SELECTOR = ':scope > .newsTitle.printdiv[itemprop=headline]'
# Filled from the exact offline-reviewed diagnostic -> canonical transition.
# Same host/provider controls, not a wildcard or new refusal/rate allowance.
INEWS_HTTP_BINDING = (
    'fa61396fabb366ef7f4b529da191fe7218702016c4611f780a30b9b7b98b06d4',
    'de47711105012d5657e9ca6ad9110572b198f48eb2daf38b16dfdec291fe3001',
    '7d236a885de97188ce100e2b1da4580cabd1bfa4a22c369e5214f491f88a2900',
    'fbd919ae2d6fd1cf932b81f8509dade514194b74e76e461aa2dda9b60f6a49d7',
)
INEWS_V2_HTTP_BINDING = (
    'e50aed5f1ed607cdab9fb16ad124f78f56156e183bb96849d0fecf82f8a6e322',
    'de47711105012d5657e9ca6ad9110572b198f48eb2daf38b16dfdec291fe3001',
    '7d236a885de97188ce100e2b1da4580cabd1bfa4a22c369e5214f491f88a2900',
    'fbd919ae2d6fd1cf932b81f8509dade514194b74e76e461aa2dda9b60f6a49d7',
)
INEWS_V3_HTTP_BINDING = (
    'f0c96744de7248bf63c5218a640272cd769ce747446f1b50434a69e6b1413786',
    'de47711105012d5657e9ca6ad9110572b198f48eb2daf38b16dfdec291fe3001',
    '7d236a885de97188ce100e2b1da4580cabd1bfa4a22c369e5214f491f88a2900',
    'fbd919ae2d6fd1cf932b81f8509dade514194b74e76e461aa2dda9b60f6a49d7',
)
# Filled only after the reviewed printed-credit capture is prepared offline.
# Changing a credit representation must not reset the host's access controls.
INEWS_V4_HTTP_BINDING = (
    'eaf79008449d44f96b53e29bab6c3a12d159586ead042768745070b65c9b719e',
    'f5cf56a73dd9d488b067f1a76756f7b5c0546a6fe5e51f2ce3e383fd7f1f0ab7',
    '7d236a885de97188ce100e2b1da4580cabd1bfa4a22c369e5214f491f88a2900',
    'fbd919ae2d6fd1cf932b81f8509dade514194b74e76e461aa2dda9b60f6a49d7',
)
# Populated only from the exact reviewed first-paragraph reporter capture.
INEWS_V5_HTTP_BINDING = (
    '916dd2a5b65dbe9414e86eb270f296577357a9139fc53cf125f5fb8f92327f56',
    'd6c298a90faf0139dd28b293631512f1450c163a5f4dfa158452b471c7b01656',
    '7d236a885de97188ce100e2b1da4580cabd1bfa4a22c369e5214f491f88a2900',
    'fbd919ae2d6fd1cf932b81f8509dade514194b74e76e461aa2dda9b60f6a49d7',
)
# Filled from the exact reviewed inline-font transition; never a host wildcard.
INEWS_V6_HTTP_BINDING = (
    '3d350eef66a63c16e6eeb2c2965b9d31fd67685aea65eddfbb8118eafa8f27e0',
    'd6c298a90faf0139dd28b293631512f1450c163a5f4dfa158452b471c7b01656',
    '7d236a885de97188ce100e2b1da4580cabd1bfa4a22c369e5214f491f88a2900',
    'fbd919ae2d6fd1cf932b81f8509dade514194b74e76e461aa2dda9b60f6a49d7',
)
# Set only from the exact reviewed empty-heading transition below.
INEWS_V7_HTTP_BINDING = (
    '393f716f2b6326e8931419750b858068299b82dd36bdcc2c8b32600f25ba1894',
    'd6c298a90faf0139dd28b293631512f1450c163a5f4dfa158452b471c7b01656',
    '7d236a885de97188ce100e2b1da4580cabd1bfa4a22c369e5214f491f88a2900',
    'fbd919ae2d6fd1cf932b81f8509dade514194b74e76e461aa2dda9b60f6a49d7',
)
# Exact reviewed inline-heading/list transition; no new HTTP budget or rights.
INEWS_V8_HTTP_BINDING = (
    '35b6b287cf05644ca5188fcbcc689094ff9901b6c8a222cbf78cd18a45fec6ea',
    'd6c298a90faf0139dd28b293631512f1450c163a5f4dfa158452b471c7b01656',
    '7d236a885de97188ce100e2b1da4580cabd1bfa4a22c369e5214f491f88a2900',
    'fbd919ae2d6fd1cf932b81f8509dade514194b74e76e461aa2dda9b60f6a49d7',
)
# One witnessed five-character native outline label. Hashing the exact label
# prevents an arbitrary short paragraph from silently becoming a navigation
# exclusion. Links must additionally match unique, ordered local headings.
INEWS_OUTLINE_LABEL_SHA256 = '6e0b442a2b150a529451b4857600c8a0b8ef1afc3bfb61c57b171e67455f33b0'
# Only a matching reviewed source/auth pair may reuse the diagnostic HTTP
# identity. Filled from the accepted offline binding, not a generic alias rule.
SETN_FINITE_HTTP_BINDING = (
    'a85c0ddf0669230ef0696286e683aa915c53d59c7f2433b6934945b491ed573f',
    '3d5eba5a1b80e0ec8b619fc95b87c34e68c138e175dd23f0c63cad58d372d1cc',
    '035970df91dfe2b86db90a9b5dd9baeb508c0fe24e19dec70570a741c94deaba',
    'cfe2792e852f425fd529cc0be03c3a5a9a00d989071469e0c611ffed8c687d6d',
)
_BYLINES = {'施郁韻', '蔡佩蓉'}
_INLINE = frozenset({'span', 'strong', 'b', 'em', 'i', 'u', 's', 'a', 'br', 'sub', 'sup', 'img'})


def _children(node):
    for child in node.children:
        if isinstance(child, Tag):
            yield child
        elif isinstance(child, Comment):
            continue
        elif str(child).strip():
            raise NewsError('article_identity_not_verified')


def _schema_clock(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2} [+]00:00', value):
        raise NewsError('invalid_bulk_publication_clock')
    try:
        return datetime.strptime(value, '%Y-%m-%d %H:%M %z')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None


def _visible_clock(node):
    _visible(node)
    value = node.get_text(' ', strip=True)
    if not re.fullmatch(r'[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', value):
        raise NewsError('invalid_bulk_publication_clock')
    try:
        return value, datetime.strptime(value, '%Y/%m/%d %H:%M:%S')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None


def _whole_body(scope, body):
    """Preserve written captions once; reject new prose outside the known editor."""
    area, wrapper = body.parent, body.parent.parent
    if (area.get('id') != 'article_content' or wrapper.get('class') != ['article_content_wrap']
            or list(_children(area)) != [body]):
        raise NewsError('article_identity_not_verified')
    for node in _children(wrapper):
        if node is area:
            continue
        if node.name != 'div' or node.get('class') not in (['article_tool_wrap'], ['more_news_list_wrap']):
            raise NewsError('article_identity_not_verified')
    allowed_sections = {'article_breadcrumb_wrap', 'article_time_wrap', 'textAD_vote_wrap',
                        'ai_article_wrap', 'article_content_wrap'}
    for node in _children(scope):
        if node.name == 'h1' and node.get('class') == ['article_title_wrap']:
            continue
        if node.name != 'div' or len(node.get('class', [])) != 1 or node['class'][0] not in allowed_sections:
            raise NewsError('article_identity_not_verified')
    _visible(body)
    captions = []
    for node in _children(body):
        _visible(node)
        if node.name == 'figure':
            picture, caption = _one(node, ':scope > picture'), _one(node, ':scope > figcaption')
            if list(_children(node)) != [picture, caption]:
                raise NewsError('article_identity_not_verified')
            if (len(picture.select(':scope > img')) != 1 or picture.get_text(strip=True)
                    or any(child.name not in {'picture', 'img', 'source'} for child in picture.find_all())):
                raise NewsError('article_identity_not_verified')
            captions.append(caption.get_text(' ', strip=True))
            containers = (picture, caption)
        elif node.name == 'p':
            containers = (node,)
        else:
            raise NewsError('article_identity_not_verified')
        for container in containers:
            _visible(container)
            for child in container.find_all():
                if child.name not in _INLINE | {'picture', 'source'}:
                    raise NewsError('article_identity_not_verified')
                _visible(child)
    if sum(len(n.get_text(' ', strip=True)) for n in body.select(':scope > p')) < 100:
        raise NewsError('body_not_found')
    return captions


def setn_public_native_article(soup, *, page_url):
    if page_url not in SETN_REVIEWED_URLS:
        raise NewsError('article_identity_not_verified')
    identifier = page_url.rsplit('/', 1)[1]
    if (_one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'
            or _one(soup, 'input#hfNewsID').get('value') != identifier):
        raise NewsError('article_identity_not_verified')
    articles = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            articles.extend(o for o in _objects(json.loads(script.get_text())) if o.get('@type') == 'NewsArticle')
        except (ValueError, TypeError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
    if len(articles) != 1:
        raise NewsError('article_identity_not_verified')
    declared = articles[0]
    if 'isAccessibleForFree' in declared and declared['isAccessibleForFree'] is not True:
        raise NewsError('paywall_or_login')
    publisher = declared.get('publisher')
    if (declared.get('url') != page_url
            or declared.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or not isinstance(publisher, dict) or publisher.get('@type') != 'Organization'
            or publisher.get('name') != '三立新聞網'
            or _one(soup, 'head > meta[name=author]').get('content') != '三立新聞網'):
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, 'article.article_wrap > section.article_part01')
    heading = _one(scope, ':scope > h1.article_title_wrap')
    _visible(scope)
    _visible(heading)
    headline = heading.get_text(' ', strip=True)
    og_title = _one(soup, 'head > meta[property="og:title"]').get('content', '')
    if (not headline or declared.get('headline') != headline
            or not og_title.startswith(headline + ' | ') or not og_title.endswith(' | 三立新聞網  SETN.COM')):
        raise NewsError('article_identity_not_verified')
    body = _one(scope, ':scope > .article_content_wrap > #article_content > #newsContent')
    captions = _whole_body(scope, body)
    declared_author = declared.get('author')
    declared_authors = declared_author if isinstance(declared_author, list) else [declared_author]
    if (len(declared_authors) != 1 or not isinstance(declared_authors[0], dict)
            or declared_authors[0].get('@type') != 'Person' or declared_authors[0].get('name') not in _BYLINES):
        raise NewsError('article_attribution_not_verified')
    name, profile = declared_authors[0]['name'], declared_authors[0].get('url')
    byline = _one(scope, '.article_time_wrap .author_wrap')
    _visible(byline)
    byline_raw = byline.get_text(' ', strip=True)
    if byline_raw:
        link = _one(byline, ':scope > a', 'article_attribution_not_verified')
        _visible(link)
        if (byline_raw != '編輯 ' + name + ' 綜合報導' or link.get_text(strip=True) != name
                or not isinstance(profile, str) or not profile.startswith('https://www.setn.com/author/')
                or profile != 'https://www.setn.com' + str(link.get('href', ''))):
            raise NewsError('article_attribution_not_verified')
        source_role = 'source_visible_header_editor_signature'
    else:
        byline = _one(body, ':scope > p:first-child', 'article_attribution_not_verified')
        byline_raw = byline.get_text(' ', strip=True)
        if profile != '' or not re.fullmatch('記者' + re.escape(name) + r'／[^／\s]{1,30}報導', byline_raw):
            raise NewsError('article_attribution_not_verified')
        source_role = 'source_visible_printed_reporter_signature'
    clock_box = _one(scope, '.article_time_wrap .article_time_box')
    items = clock_box.select(':scope > .time_item')
    if len(items) != 2:
        raise NewsError('invalid_bulk_publication_clock')
    published_raw, published_visible = _visible_clock(_one(items[0], ':scope > span'))
    updated_nodes = items[1].select(':scope > span')
    if len(updated_nodes) != 2 or updated_nodes[1].get_text(strip=True) != '更新':
        raise NewsError('invalid_bulk_publication_clock')
    modified_raw, modified_visible = _visible_clock(updated_nodes[0])
    published, modified = declared.get('datePublished'), declared.get('dateModified')
    published_schema, modified_schema = _schema_clock(published), _schema_clock(modified)
    # Compare only the declared wall minute. Do not fabricate +08:00, discard
    # the source UTC label, or turn the visible update seconds into schema data.
    if (published_schema.strftime('%Y/%m/%d %H:%M') != published_raw[:16]
            or modified_schema.strftime('%Y/%m/%d %H:%M') != modified_raw[:16]
            or modified_visible < published_visible or modified_schema < published_schema):
        raise NewsError('invalid_bulk_publication_clock')
    detail = {'contract_version': 1, 'source_visible_signature_raw': byline_raw,
        'source_credit_role': source_role, 'source_declared_person_name': name,
        'source_declared_profile_url': profile, 'meta_author_is_publisher_not_person': True,
        'original_ownership_verified': False, 'human_reporter_identity_verified': False}
    notices = {'contract_version': 1, 'visible_publication_clock_raw': published_raw,
        'visible_modified_clock_raw': modified_raw, 'visible_clock_precision': 'second',
        'visible_timezone_declared': False, 'schema_publication_clock_raw': published,
        'schema_modified_clock_raw': modified, 'schema_clock_precision': 'minute',
        'schema_timezone_offset_raw': '+00:00', 'same_wall_minute_is_same_instant_proof': False,
        'source_display_timezone_offset_verified': False, 'publication_offset_resolution': 'unresolved',
        'historical_first_release_verified': False, 'exact_PIT_eligible': False,
        'AI_summary_is_article_body': False, 'media_or_scripts_requested_or_executed': False,
        'written_figure_captions': captions}
    return {'headline': headline, 'author': [{'name': name}], 'datePublished': published, 'dateModified': modified,
        'published_time_basis': 'source_schema_declared_minute_UTC_label_retained; visible_second_clock_timezone_unstated; offset_unresolved_not_exact_PIT',
        'article_identity_basis': 'literal_reviewed_URL_canonical_og_schema_hidden_newsID_heading_and_whole_native_editor_v1',
        'author_attribution_detail_json': json.dumps(detail, ensure_ascii=False, sort_keys=True),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False, sort_keys=True),
        'body_completeness': 'whole_visible_native_paragraphs_and_written_figcaptions_in_source_order_once; source_risk_notice_kept; AI_summary_tools_ads_related_media_excluded'}, scope


def _inews_outline(node, body, *, adapter_contract_version=1):
    """Prove a native table of contents, not a list of related articles.

    Check the whole small subtree, then one page-wide ID count. No DOM edits,
    copies, media/profile requests or repeated full-page searches per link.
    """
    children = list(_children(node))
    if (node.name!='div' or node.get('id')!='nav-directory' or node.parent is not body
            or len(children)!=2 or [n.name for n in children]!=['p','ul']):
        raise NewsError('article_identity_not_verified')
    label, links = children
    strong = _one(label,':scope > strong')
    if (list(_children(label))!=[strong] or strong.find_all()
            or hashlib.sha256(label.get_text(' ',strip=True).encode()).hexdigest()!=INEWS_OUTLINE_LABEL_SHA256):
        raise NewsError('article_identity_not_verified')
    entries = list(_children(links))
    if not 1<=len(entries)<=32 or any(entry.name!='li' for entry in entries):
        raise NewsError('article_identity_not_verified')
    targets, labels = [], []
    for entry in entries:
        anchor = _one(entry,':scope > a')
        identifier = str(anchor.get('href',''))
        text = anchor.get_text(' ',strip=True)
        if (list(_children(entry))!=[anchor] or anchor.find_all()
                or not re.fullmatch(r'#nav-title-[1-9][0-9]*',identifier) or not 1<=len(text)<=200):
            raise NewsError('article_identity_not_verified')
        targets.append(identifier[1:]); labels.append(text)
    root = body
    while root.parent is not None: root = root.parent
    counts = Counter(str(element['id']) for element in root.find_all(id=True))
    headings = body.find_all(id=re.compile(r'^nav-title-[1-9][0-9]*$'))
    carriers = []
    for heading in headings:
        if heading.name=='p' and heading.parent is body and not heading.find_all():
            carriers.append(heading)
        elif (adapter_contract_version>=8 and heading.name=='strong' and not heading.find_all()
                and heading.parent.name=='p' and heading.parent.parent is body
                and not any(str(sibling).strip() for sibling in heading.previous_siblings)):
            carriers.append(heading.parent)
        else:
            raise NewsError('article_identity_not_verified')
    if (len(set(targets))!=len(targets) or any(counts[identifier]!=1 for identifier in targets)
            or [n.get('id') for n in headings]!=targets
            or len({id(n) for n in carriers})!=len(carriers)
            or [n.get_text(' ',strip=True) for n in headings]!=labels):
        raise NewsError('article_identity_not_verified')
    positions = {id(child):index for index,child in enumerate(_children(body))}
    if any(positions[id(node)]>=positions[id(paragraph)] for paragraph in carriers):
        raise NewsError('article_identity_not_verified')
    result = {'contract_version':1,'source_container_id':'nav-directory',
        'exact_label_sha256':INEWS_OUTLINE_LABEL_SHA256,'ordered_local_heading_ids':targets,
        'link_labels_match_unique_visible_native_heading_text':True,
        'links_requested_or_accepted_as_URL_aliases':False,'heading_text_preserved_once':True}
    if any(n.name=='strong' for n in headings):
        result.update(contract_version=2,ordered_target_tags=[n.name for n in headings],
            inline_heading_and_following_prose_preserved_in_one_native_paragraph=True)
    return result


def _inews_prose_list(node):
    """Witnessed direct ul/ol, each li owning one written paragraph.

    This is article text, not another navigation exclusion. No nested list,
    foreign block, arbitrary numbering rule or media execution is inferred.
    """
    entries = list(_children(node))
    if not 1<=len(entries)<=256 or any(n.name!='li' for n in entries):
        raise NewsError('article_identity_not_verified')
    paragraphs = []
    for entry in entries:
        children = list(_children(entry))
        if len(children)!=1 or children[0].name!='p':
            raise NewsError('article_identity_not_verified')
        paragraph = children[0]
        if not paragraph.get_text(' ',strip=True) or any(n.name not in _INLINE for n in paragraph.find_all()):
            raise NewsError('article_identity_not_verified')
        paragraphs.append(paragraph)
    for element in (node,*entries,*paragraphs):
        if any(key not in {'data-start','data-end'} or not isinstance(value,str)
                or not re.fullmatch(r'[0-9]{1,12}',value) for key,value in element.attrs.items()):
            raise NewsError('article_identity_not_verified')
    return paragraphs, {'source_list_type':node.name,'source_entry_count':len(entries),
        'source_order_preserved':True,'written_values_preserved_once':True,
        'list_number_labels_invented':False,'navigation_or_related_content_excluded':False}


def _inews_whole_editor(scope, *, adapter_contract_version=1):
    """One native editor; linear own-node checks, no DOM copy or media fetch."""
    editor = _one(scope, INEWS_EDITOR_SELECTOR)
    wrapper, article = editor.parent.parent, editor.parent
    if list(_children(wrapper)) != [article] or list(_children(article)) != [editor]:
        raise NewsError('article_identity_not_verified')
    remark = _one(editor, ':scope > .article_remark_wrap')
    body = _one(editor, ':scope > div[itemprop=articleBody]')
    if list(_children(editor)) != [remark, body]:
        raise NewsError('article_identity_not_verified')
    parts = list(_children(scope))
    if len(parts) != 2 or parts[0].get('class') != ['newsTitle', 'printdiv'] or parts[1].get('class') != ['flexbox']:
        raise NewsError('article_identity_not_verified')
    # Adjacent tools/advertisements/recommendations are not editor paragraphs.
    # Only the witnessed structural siblings are outside the body. New prose
    # cannot become an exclusion just by falling outside our CSS selector.
    sibling_classes = {('shareBox','hidden-print'), ('printBox','hidden-print'), ('google_btn',),
        ('ad300x250Box','hidden-print'), ('alert-message','alert-message-warning','hidden-print'),
        ('extend','hidden-print'), ('hotNewsSection','hidden-print')}
    for node in _children(parts[1]):
        if node is wrapper:
            continue
        if node.name != 'div' or tuple(node.get('class', ())) not in sibling_classes:
            raise NewsError('article_identity_not_verified')
    captions, outlines, outline_nodes, empty_heading_nodes = [], [], set(), set()
    prose_lists, prose_list_nodes, written_paragraphs = [], set(), []
    for node in _children(body):
        if node.name == 'figure':
            caption = _one(node, ':scope > figcaption')
            children = list(_children(node))
            direct_image = (adapter_contract_version >= 2 and len(children) == 2
                and children[0].name == 'img' and not list(_children(children[0])))
            if direct_image:
                picture = children[0]
            else:
                picture = _one(node, ':scope > picture')
            if (children != [picture,caption] or picture.get_text(strip=True)
                    or not direct_image and (len(picture.select(':scope > img')) != 1
                        or any(child.name not in {'img','source'} for child in picture.find_all()))):
                raise NewsError('article_identity_not_verified')
            captions.append(caption.get_text(' ',strip=True))
        elif adapter_contract_version>=3 and node.name=='div' and node.get('id')=='nav-directory':
            if outlines:
                raise NewsError('article_identity_not_verified')
            outlines.append(_inews_outline(node,body,adapter_contract_version=adapter_contract_version))
            outline_nodes.update(id(element) for element in (node,*node.find_all()))
        elif adapter_contract_version>=8 and node.name in {'ul','ol'}:
            paragraphs, description = _inews_prose_list(node)
            prose_lists.append(description)
            prose_list_nodes.update(id(n) for n in (node,*node.find_all()))
            written_paragraphs.extend(paragraphs)
        elif adapter_contract_version>=7 and node.name=='h1':
            # Witnessed native whitespace-only placeholder after a proven
            # local outline. Never discard a written heading, an attribute,
            # hidden subtree or an arbitrary block elsewhere in the editor.
            previous = node.find_previous_sibling()
            if (empty_heading_nodes or node.attrs or not outlines or previous is None
                    or previous.name!='div' or previous.get('id')!='nav-directory'
                    or id(previous) not in outline_nodes
                    or any(type(child) is not NavigableString or str(child).strip() for child in node.contents)):
                raise NewsError('article_identity_not_verified')
            empty_heading_nodes.add(id(node))
        elif node.name != 'p':
            raise NewsError('article_identity_not_verified')
        else:
            written_paragraphs.append(node)
    _visible(body)
    allowed = _INLINE | {'div','p','figure','picture','source','figcaption'}
    if adapter_contract_version >= 6:
        allowed |= {'font'}
    for node in [body,*body.find_all()]:
        style = re.sub(r'\s+','',str(node.get('style',''))).lower()
        if ((node.name not in allowed and not (id(node) in outline_nodes and node.name in {'ul','li'})
                and id(node) not in empty_heading_nodes and id(node) not in prose_list_nodes)
                or node.has_attr('hidden') or str(node.get('aria-hidden','')).lower() == 'true'
                or 'display:none' in style or 'visibility:hidden' in style):
            raise NewsError('article_identity_not_verified')
        if node is not body and node.name == 'div' and id(node) not in outline_nodes:
            raise NewsError('article_identity_not_verified')
        if node.name == 'font':
            # Witnessed legacy typeface, not another content container. Keep
            # every written value and the existing visibility/subtree checks.
            # No font/media is requested, rendered or executed by extraction.
            written = node.find_parent(['p','figcaption'])
            if (node.attrs != {'face':'Arial, Helvetica, sans-serif'} or written is None
                    or not (written.name == 'p' and written.parent is body
                        or written.name == 'figcaption' and written.parent.name == 'figure'
                        and written.parent.parent is body)):
                raise NewsError('article_identity_not_verified')
    if sum(len(n.get_text(' ',strip=True)) for n in written_paragraphs) < 100:
        raise NewsError('body_not_found')
    return editor, remark, body, captions, outlines, len(empty_heading_nodes), prose_lists


def _inews_native_identity(soup, *, page_url):
    """Shared literal publisher identity; does not validate or accept prose."""
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'inews.setn.com' or parts.query or parts.fragment
            or not re.fullmatch(SETN_PATH_PATTERN, parts.path)):
        raise NewsError('article_identity_not_verified')
    canonical = _one(soup,'head > link[rel=canonical]').get('href')
    og_url = _one(soup,'head > meta[property="og:url"]').get('content')
    if (not isinstance(canonical,str) or not isinstance(og_url,str)
            or canonical not in {parts.path,page_url} or og_url not in {parts.path,page_url}
            or urljoin(page_url,canonical) != page_url or urljoin(page_url,og_url) != page_url
            or _one(soup,'head > meta[property="og:type"]').get('content') != 'article'
            or _one(soup,'head > meta[name=author]').get('content') != '三立iNEWS'
            or _one(soup,'head > meta[name=source]').get('content') != '三立iNEWS'
            or soup.select('script[type="application/ld+json"]')):
        raise NewsError('article_identity_not_verified')
    scope = _one(soup,INEWS_SCOPE_SELECTOR)
    if not scope.has_attr('itemscope') or scope.get('itemtype') != 'https://schema.org/NewsArticle':
        raise NewsError('article_identity_not_verified')
    _visible(scope)
    heading = _one(scope,INEWS_TITLE_SELECTOR)
    _visible(heading)
    title = heading.get_text(' ',strip=True)
    declared_title = title+' | 三立iNEWS | 三立新聞網 SETN.COM'
    if (not title or _one(soup,'head > meta[property="og:title"]').get('content') != declared_title
            or _one(soup,'head > meta[name=Title]').get('content') != declared_title
            or _one(soup,'head > title').get_text(' ',strip=True) != declared_title):
        raise NewsError('article_identity_not_verified')
    return scope, title, canonical, og_url


def setn_inews_native_article(soup, *, page_url, adapter_contract_version=1):
    """Native microdata, not main-site JSON-LD or an inferred +08:00 clock."""
    if type(adapter_contract_version) is not int or adapter_contract_version not in INEWS_LAYOUT_VERSIONS.values():
        raise NewsError('article_identity_not_verified')
    scope, title, canonical, og_url = _inews_native_identity(soup, page_url=page_url)
    _, remark, body, captions, outlines, empty_headings, prose_lists = _inews_whole_editor(scope, adapter_contract_version=adapter_contract_version)
    byline = _one(remark,':scope > .author_wrap','article_attribution_not_verified')
    _visible(byline)
    signature = byline.get_text(' ',strip=True)
    printed_credit = adapter_contract_version >= 4 and not signature and not byline.find_all()
    printed_location = None
    if printed_credit:
        # Witnessed native first-written paragraph, not a publisher/desk name
        # promoted to a person. Its complete signature remains in the body;
        # there is no source profile to reconstruct, request or invent.
        first = next((p for p in body.find_all('p',recursive=False) if p.get_text(' ',strip=True)),None)
        if first is None or first.find_all():
            raise NewsError('article_attribution_not_verified')
        _visible(first)
        signature = first.get_text(' ',strip=True)
        match = re.fullmatch(r'財經中心／([\u3400-\u9fff·]{2,20})報導',signature)
        if match is None and adapter_contract_version >= 5:
            # A separate witnessed role: the first paragraph explicitly says
            # reporter/name/location. Do not invent a desk, profile, historical
            # employment or ownership from this printed publisher attribution.
            match = re.fullmatch(r'記者([\u3400-\u9fff·]{2,20})／([\u3400-\u9fff]{1,20})報導',signature)
            if match is not None:
                printed_location = match[2]
        if match is None or match[1] in {'綜合','整理','編輯','記者'}:
            raise NewsError('article_attribution_not_verified')
        if any(re.match(r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])',p.get_text(' ',strip=True))
                for p in body.find_all('p')):
            raise NewsError('article_rights_or_scope_excluded')
        name, profile = match[1], None
    else:
        link = _one(byline,':scope > a[itemprop=author]','article_attribution_not_verified')
        _visible(link)
        name, profile = link.get_text(' ',strip=True), link.get('href','')
        location = r'[\u3400-\u9fff]{1,20}' if adapter_contract_version == 1 else r'(?:[\u3400-\u9fff]{1,20})?'
        if (byline.find_all(recursive=False) != [link] or link.find_all()
                or not re.fullmatch(r'[\u3400-\u9fff·]{2,20}',name)
                or not re.fullmatch(r'https://www\.setn\.com/author/[a-z0-9-]+',profile)
                or not re.fullmatch('記者 '+re.escape(name)+' '+location+'報導',signature)):
            raise NewsError('article_attribution_not_verified')
    clock = _one(remark,':scope > time.pageDate','invalid_bulk_publication_clock')
    if list(_children(remark)) != [byline,clock] or clock.find_all() or clock.has_attr('datetime'):
        raise NewsError('invalid_bulk_publication_clock')
    _visible(clock)
    raw_clock = clock.get_text(' ',strip=True)
    try:
        if not re.fullmatch(r'[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}',raw_clock):
            raise ValueError
        datetime.strptime(raw_clock,'%Y/%m/%d %H:%M')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    attribution = {'contract_version':1,'source_visible_signature_raw':signature,
        'source_credit_role':'native_visible_named_reporter_signature',
        'source_declared_person_name':name,'source_declared_profile_url':profile,
        'profile_requested':False,'meta_author_is_publisher_not_person':True,
        'human_reporter_identity_verified':False,'original_ownership_verified':False}
    if printed_credit:
        attribution.update(contract_version=2,
            source_credit_role='native_visible_printed_financial_desk_named_reporter_signature',
            source_declared_desk='財經中心', source_profile_absent=True,
            printed_signature_preserved_in_article_body=True)
        if printed_location is not None:
            attribution.pop('source_declared_desk')
            attribution.update(source_credit_role='native_visible_printed_named_reporter_signature',
                source_declared_report_location=printed_location)
    notices = {'contract_version':1,'canonical_raw':canonical,'og_url_raw':og_url,
        'source_current_article_clock_raw':raw_clock,'source_clock_precision':'minute',
        'source_timezone_declared':False,'source_clock_release_or_revision_role_labelled':False,
        'JSON_LD_present':False,'revision_clock_present':False,'historical_first_release_verified':False,
        'exact_PIT_eligible':False,'written_figure_captions':captions,
        'media_or_scripts_requested_or_executed':False}
    if outlines:
        notices['matched_native_in_page_outlines'] = outlines
    if empty_headings:
        notices['verified_empty_outline_heading_placeholders'] = {
            'count':empty_headings,'tag':'h1','directly_after_verified_native_outline':True,
            'attributes_or_descendants_present':False,'written_values_removed':False,
            'raw_source_tree_modified':False}
    if prose_lists:
        notices['verified_native_written_prose_lists'] = prose_lists
    return {'headline':title,'author':[{'name':name}],'datePublished':raw_clock,
        'published_time_basis':'source_visible_current_article_header_minute_label; timezone_and_release_or_revision_role_unstated; not_exact_historical_PIT',
        'article_identity_basis':'literal_known_iNEWS_URL_relative_or_absolute_canonical_OG_native_NewsArticle_headline_whole_editor_v'+str(adapter_contract_version),
        'author_attribution_detail_json':json.dumps(attribution,ensure_ascii=False,sort_keys=True),
        'verified_article_notices_json':json.dumps(notices,ensure_ascii=False,sort_keys=True),
        'body_completeness':'whole_native_editor_paragraphs_and_written_figcaptions_once; raw_values_risk_notice_and_photo_credit_kept; header_tools_ads_recommendations_media_excluded'
            + ('; witnessed_native_outline_navigation_excluded_only_after_exact_local_heading_match' if outlines else '')
            + ('; written_native_list_paragraphs_preserved_once_in_source_order' if prose_lists else '')},scope


def inews_boundary_metadata(soup, *, page_url):
    """Same received failure, finite shapes only; no names, body or new GET."""
    import hashlib
    editors = soup.select(INEWS_SCOPE_SELECTOR+' > .flexbox > #ckuse > article > .newsCon')
    clocks = soup.select('#newsArea .article_remark_wrap > time.pageDate')
    credits = soup.select('#newsArea .article_remark_wrap > .author_wrap')
    result = {'contract_version':1,'native_editors':len(editors),'native_clocks':len(clocks),
        'native_credits':len(credits),'JSON_LD_count':len(soup.select('script[type="application/ld+json"]')),
        'accepted_as_article':False,'raw_body_or_names_retained':False,'publisher_requests':0}
    for key,nodes in (('clock',clocks),('credit',credits)):
        result[key+'_shapes'] = [{'tag':node.name,'characters':len(node.get_text(' ',strip=True)),
            'text_sha256':hashlib.sha256(node.get_text(' ',strip=True).encode()).hexdigest(),
            'child_tags':[child.name for child in node.find_all(recursive=False)][:16]} for node in nodes[:4]]
    result['editor_shapes'] = [[{'tag':child.name,'id':child.get('id'),
        'class':child.get('class'),'characters':len(child.get_text(' ',strip=True))}
        for child in node.find_all(recursive=False)[:16]] for node in editors[:2]]
    return result


def inews_boundary_source(source):
    """The current reviewed native scope, not a candidate or a new permission."""
    return bool(source.provider == INEWS_PROVIDER and source.allowed and source.request_enabled
        and source.embedded_article_layout in INEWS_LAYOUT_VERSIONS
        and source.known_url_only and source.acquisition_mode == 'continuous'
        and source.domains == ('inews.setn.com',) and source.html_parser == 'lxml'
        and source.authorization.get('kind') == 'personal_research'
        and source.article_path_pattern == SETN_PATH_PATTERN
        and source.selectors == ((INEWS_V8_BODY_SELECTOR,) if INEWS_LAYOUT_VERSIONS.get(source.embedded_article_layout,0)>=8
            else (INEWS_BODY_SELECTOR,))
        and source.author_selectors == ((INEWS_V4_AUTHOR_SELECTOR,)
            if INEWS_LAYOUT_VERSIONS.get(source.embedded_article_layout,0)>=4 else (INEWS_AUTHOR_SELECTOR,))
        and source.title_selector == INEWS_TITLE_SELECTOR
        and source.require_author_attribution and source.require_configured_container)


def inews_literal_article_url(url):
    """No legacy IDs, alternative hosts, aliases or query normalization."""
    if not isinstance(url, str):
        return False
    parts = urlsplit(url)
    return (parts.scheme == 'https' and parts.netloc == 'inews.setn.com'
        and not parts.query and not parts.fragment and bool(re.fullmatch(SETN_PATH_PATTERN, parts.path))
        and url == INEWS_ORIGIN + parts.path)


def inews_resident_boundary_metadata(soup, *, page_url):
    """Bounded shapes for the live owner; no prose, names, links or acceptance.

    This question is distinct from the richer explicit validation context. It
    can identify unsupported editor children and missing native author links
    without retaining public names under the resident queue's no-names ABI.
    Neither missing links nor a credit-prefix candidate proves authorship.
    """
    from itertools import islice

    shapes_remaining = 48
    shape_limit_reached = False

    def shape(node):
        nonlocal shapes_remaining, shape_limit_reached
        if not shapes_remaining:
            shape_limit_reached = True
            return {'omitted': 'global_shape_limit'}
        shapes_remaining -= 1
        text = node.get_text(' ', strip=True)
        children = list(islice((n for n in node.children if isinstance(n, Tag)), 9))
        # Attributes themselves can contain names/prose/URLs. Only their
        # presence and fixed native vocabulary are relevant to this question.
        return {'tag': node.name[:32], 'tag_truncated': len(node.name) > 32, 'characters': len(text),
            'text_sha256': hashlib.sha256(text.encode()).hexdigest(),
            'attribute_names': [key[:24] for key in sorted(node.attrs)[:8]],
            'attributes_truncated': len(node.attrs) > 8 or any(len(key) > 24 for key in node.attrs),
            'native_article_body': node.get('itemprop') == 'articleBody',
            'native_author': node.get('itemprop') == 'author',
            'hidden': node.has_attr('hidden'), 'child_tags': [n.name[:32] for n in children[:8]],
            'children_truncated': len(children) > 8 or any(len(n.name) > 32 for n in children)}

    result = {'contract_version': 1, 'native_boundary': inews_boundary_metadata(soup, page_url=page_url),
        'accepted_as_article': False, 'raw_body_or_names_retained': False, 'publisher_requests': 0,
        'identity_or_alias_accepted': False, 'publication_offset_or_release_role_inferred': False}
    # The older boundary carries editor id/class values. Keep its counts,
    # hashes and lengths, but redact those arbitrary attribute values here.
    for editor in result['native_boundary']['editor_shapes']:
        for child in editor:
            child.pop('id', None)
            child.pop('class', None)
            child['tag'] = child['tag'][:32]
    for key in ('clock_shapes', 'credit_shapes'):
        for node in result['native_boundary'][key]:
            node['tag'] = node['tag'][:32]
            node['child_tags'] = [tag[:32] for tag in node['child_tags']]
    editors = soup.select(INEWS_SCOPE_SELECTOR + ' > .flexbox > #ckuse > article > .newsCon')
    credits = soup.select('#newsArea .article_remark_wrap > .author_wrap')
    result['credits'] = []
    for credit in credits[:4]:
        text = credit.get_text(' ', strip=True)
        role = next((role for role in ('記者', '編輯', '文／', '文/', '來源：', '來源:')
                     if text.startswith(role)), 'empty' if not text else 'other')
        links = credit.find_all('a', limit=9)
        result['credits'].append({'element': shape(credit), 'prefix_candidate': role,
            'links': [shape(link) for link in links[:8]], 'links_truncated': len(links) > 8,
            'verified_as_credit': False})
    result['editors'] = []
    for editor in editors[:2]:
        bodies = editor.select(':scope > [itemprop=articleBody]')
        contexts = []
        for body in bodies[:2]:
            children = list(islice((n for n in body.children if isinstance(n, Tag)), 49))
            contexts.append({'element': shape(body), 'children': [shape(child) for child in children[:48]],
                'children_truncated': len(children) > 48})
        result['editors'].append({'element': shape(editor), 'body_count': len(bodies),
            'bodies': contexts, 'bodies_truncated': len(bodies) > 2})
    result['context_truncated'] = len(editors) > 2 or len(credits) > 4
    result['global_shape_limit'] = 48
    result['global_shape_limit_reached'] = shape_limit_reached
    return result


def inews_validation_context(soup, *, page_url):
    """Bounded public credit/clock and editor-shape question, never acceptance.

    V1 failure facts contain only lengths/hashes. They cannot explain a real
    desk signature, a printed byline or an unsupported editor child. This
    V4 also observes bounded nested lists and native in-page outline targets;
    V3 direct-list/printed-credit observations remain valid. No candidate is
    promoted to prose,
    attribution or a related-news exclusion, nor grants a new request budget.
    No raw HTML copy, second GET, accepting-parser retry or inferred clock.
    """
    from itertools import islice
    import hashlib

    def bounded(value, maximum=200):
        value = str(value or '')
        return {'value': value[:maximum], 'characters': len(value), 'truncated': len(value) > maximum}

    def shape(node):
        children = list(islice((n for n in node.children if isinstance(n, Tag)), 17))
        return {'tag': node.name, 'id': bounded(node.get('id'), 80),
            'classes': [bounded(v, 80) for v in node.get('class', [])[:8]],
            'classes_truncated': len(node.get('class', [])) > 8,
            'itemprop': bounded(node.get('itemprop'), 80),
            'style': bounded(node.get('style'), 256), 'hidden': node.has_attr('hidden'),
            'aria_hidden': bounded(node.get('aria-hidden'), 16),
            'child_tags': [n.name for n in children[:16]], 'children_truncated': len(children) > 16}

    def link(node, *, resolve_relative=False):
        raw = str(node.get('href', ''))
        try:
            parts = urlsplit(urljoin(page_url, raw) if resolve_relative else raw)
            public = (parts.scheme + '://' + parts.netloc + parts.path
                if parts.scheme in {'https', 'http'} and parts.netloc
                and not parts.username and not parts.password else None)
        except ValueError:
            public = None
        fragment = (raw[1:] if resolve_relative and re.fullmatch(r'#nav-title-[1-9][0-9]*',raw) else None)
        return {'text': bounded(node.get_text(' ', strip=True)), 'href_without_query_or_fragment': bounded(public, 256),
            'href_raw_sha256': hashlib.sha256(raw.encode()).hexdigest(),
            'native_local_outline_fragment_candidate': bounded(fragment,96),
            'itemprop': bounded(node.get('itemprop'), 80), 'element': shape(node)}

    scopes = soup.select(INEWS_SCOPE_SELECTOR)
    editors = soup.select(INEWS_SCOPE_SELECTOR+' > .flexbox > #ckuse > article > .newsCon')
    credits = soup.select('#newsArea .article_remark_wrap > .author_wrap')
    clocks = soup.select('#newsArea .article_remark_wrap > time.pageDate')
    headings = soup.select(INEWS_SCOPE_SELECTOR+' > .newsTitle[itemprop=headline]')
    title = headings[0].get_text(' ', strip=True) if len(headings) == 1 else None
    expected_title = title+' | 三立iNEWS | 三立新聞網 SETN.COM' if title else None
    result = {'contract_version': 4, 'scope_count': len(scopes), 'editor_count': len(editors),
        'credit_count': len(credits), 'clock_count': len(clocks), 'heading_count': len(headings),
        'JSON_LD_count': len(soup.select('script[type="application/ld+json"]')),
        'public_credits_are_candidates_not_verified_authors': True, 'accepted_as_article': False,
        'raw_response_retained': False, 'full_body_retained': False, 'publisher_requests': 0,
        'publication_offset_or_release_role_inferred': False}
    result['head_identity'] = {}
    def matches_literal(name, value, expected):
        try:
            return urljoin(page_url,value) == expected if name in {'canonical','og_url'} else value == expected
        except ValueError:
            return False  # A bad declaration is evidence, not a diagnostic crash.
    for name, selector, attribute, expected in (
        ('canonical', 'head > link[rel=canonical]', 'href', page_url),
        ('og_url', 'head > meta[property="og:url"]', 'content', page_url),
        ('og_title', 'head > meta[property="og:title"]', 'content', expected_title),
        ('title_meta', 'head > meta[name=Title]', 'content', expected_title),
        ('publisher_author', 'head > meta[name=author]', 'content', '三立iNEWS'),
        ('publisher_source', 'head > meta[name=source]', 'content', '三立iNEWS')):
        nodes = soup.select(selector)
        values = [str(n.get(attribute, '')) for n in nodes[:4]]
        result['head_identity'][name] = {'node_count': len(nodes), 'truncated': len(nodes) > 4,
            'matches_literal_native_identity': [matches_literal(name,v,expected) for v in values]}
    result['credits'] = [{'element': shape(n), 'signature_raw': bounded(n.get_text(' ', strip=True)),
        'links': [link(a) for a in n.find_all('a', limit=9)[:8]],
        'links_truncated': len(n.find_all('a', limit=9)) > 8} for n in credits[:4]]
    result['clocks'] = [{'element': shape(n), 'raw': bounded(n.get_text(' ', strip=True)),
        'datetime_attribute': bounded(n.get('datetime'))} for n in clocks[:4]]
    result['scopes'] = [{'element': shape(n),
        'children': [shape(c) for c in islice((c for c in n.children if isinstance(c, Tag)), 16)],
        'flexbox_children': [shape(c) for c in n.select(':scope > .flexbox > *')[:24]]} for n in scopes[:2]]
    result['editors'] = []
    def adjacent_context(node):
        if not isinstance(node, Tag):
            return None
        text = node.get_text(' ', strip=True)
        label = (text if len(text) <= 80 and re.match(
            r'^[【\[\s]*(?:更多|延伸閱讀|相關新聞|相關報導|推薦閱讀|看更多)', text) else None)
        outline_label = (text if len(text) <= 80 and re.fullmatch(
            r'[【\[\s]*(?:文章目錄|目錄|本文導覽)[】\]\s：:]*',text) else None)
        return {'element': shape(node), 'characters': len(text),
            'text_sha256': hashlib.sha256(text.encode()).hexdigest(),
            'short_related_label_candidate': label, 'short_outline_label_candidate': outline_label,
            'verified_as_related_exclusion': False}

    for editor in editors[:2]:
        nodes = editor.find_all(limit=129)
        bodies = editor.select(':scope > [itemprop=articleBody]')
        printed, scans, lists, nested_lists, outline_targets = [], [], [], [], []
        for body_index, body in enumerate(bodies[:2]):
            paragraphs = body.find_all('p', recursive=False, limit=17)
            first_nonempty = None
            candidates = 0
            for paragraph_index, paragraph in enumerate(paragraphs[:16]):
                text = paragraph.get_text(' ', strip=True)
                if text and first_nonempty is None:
                    first_nonempty = paragraph_index
                if re.match(r'^(?:記者|編輯|[^\s／/]{2,8}中心[／/]|文[／/]|中央社|來源[：:])', text):
                    if candidates < 4:
                        printed.append({'body_index': body_index, 'paragraph_index': paragraph_index,
                            'characters': len(text), 'text_sha256': hashlib.sha256(text.encode()).hexdigest(),
                            'short_printed_credit_candidate': text if len(text) <= 200 else None,
                            'truncated': len(text) > 200, 'verified_as_credit': False})
                    candidates += 1
            scans.append({'body_index': body_index, 'direct_paragraphs_considered': min(16,len(paragraphs)),
                'paragraph_scan_truncated': len(paragraphs)>16, 'first_nonempty_paragraph_index': first_nonempty,
                'credit_candidates_limit': 4, 'credit_candidates_truncated': candidates>4,
                'absence_of_candidate_proves_no_credit': False})
            targets = body.find_all(id=re.compile(r'^nav-title-[1-9][0-9]*$'),limit=33)
            for target in targets[:32]:
                outline_targets.append({'body_index':body_index,'element':shape(target),
                    'label':bounded(target.get_text(' ',strip=True),200),
                    'scan_truncated':len(targets)>32,'verified_as_outline_target':False})
            body_lists = body.find_all(['ul','ol'],limit=5)
            for list_index, node in enumerate(body_lists[:4]):
                anchors = node.find_all('a', limit=9)
                chain = []
                parent = node.parent
                while isinstance(parent,Tag) and parent is not body and len(chain)<3:
                    chain.append(shape(parent))
                    parent = parent.parent
                context = {'body_index': body_index, 'list_index': list_index,
                    'element': shape(node), 'previous_sibling': adjacent_context(node.find_previous_sibling()),
                    'next_sibling': adjacent_context(node.find_next_sibling()),
                    'links': [link(anchor,resolve_relative=True) for anchor in anchors[:8]],
                    'links_truncated': len(anchors)>8,
                    'list_scan_truncated': len(body_lists)>4,
                    'ancestor_shapes_until_native_body':chain,'ancestor_scan_truncated':parent is not body,
                    'resolved_links_are_context_not_URL_aliases': True,
                    'accepted_as_body_or_related_exclusion': False}
                (lists if node.parent is body else nested_lists).append(context)
        result['editors'].append({'element': shape(editor), 'nodes': [shape(n) for n in nodes[:128]],
            'nodes_truncated': len(nodes) > 128, 'body_count': len(bodies), 'printed_credit_candidates': printed,
            'body_details_truncated': len(bodies)>2, 'printed_credit_scans': scans, 'direct_link_list_contexts': lists,
            'nested_link_list_contexts':nested_lists,'native_outline_target_candidates':outline_targets})
    result['context_truncated'] = len(scopes) > 2 or len(editors) > 2 or len(credits) > 4 or len(clocks) > 4
    return result
