"""MAX's public native posts; identity, credit and revision are separate facts.

No HTTP or alias generation. A current rewritten company guide is not the
original historical article, even when the publisher retains an old release
clock. Source numbers, tables, caveats and literal headline variants survive.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
from itertools import islice
import json
import re
from urllib.parse import unquote, urlsplit

from downloader.news_fulltext_policy import MAX_LAYOUT_VERSIONS, NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


MAX_ORIGIN = 'https://maxfinanciallife.com'
MAX_BODY_SELECTOR = ':scope > .post-entry > #penci-post-entry-inner'
MAX_AUTHOR_SELECTOR = ':scope > .single-header > .post-box-meta-single > .byline > .author > a.author-url'
MAX_PATH_PATTERN = r'^/(?!about/?$|editorial-policy/?$|privacy-policy/?$|sitemap_index[.]xml$|post-sitemap[0-9]*[.]xml$)[^/]+/?$'
MAX_SITEMAP = MAX_ORIGIN + '/sitemap_index.xml'
MAX_AUTHORS = ('MAX', 'MAX 投資研究團隊')
# Reviewed same-owner failed-response metadata, 2026-10-20: one native table
# stylesheet (including the publisher's literal <br /> corruption) and one
# wb-batch text/table/bar presentation sheet. Never a generic style bypass;
# unknown or changed sheets remain unverified, and raw responses stay intact.
MAX_PRESENTATION_CSS_SHA256 = frozenset({
    'dcefb0c9a60b5cd3a95e9eeaa90ffe38a22ec1bee97dafbc0460f592a6bfed54',
    '0eafb0df78ccaa3a1fb439c20d4b4c7ac7b495bc042c139406704ba4551617fb',
})
# v7: the complete 31-head metadata diagnosis found seven more native
# presentation sheets. Exact bytes are attested, not an arbitrary CSS skip.
# The two scoped sheets carry a native empty boolean attribute; all values,
# malformed literal <br /> CSS and original HTML remain in the raw envelope.
MAX_V7_PRESENTATION_CSS_SHA256 = frozenset({
    '34e2db88a3dd0062053dd6b0b71e0fe7ac7ef7d1e3df9f9db95c64de2efd25bf',
    '8f7532e34f3a2f98fc9f7d046d2792f96a2c1cfd3f997a729cc6910d5305e16a',
    '83c52dbd2168187ba7b216ac2a10b761626eb5780c97fd6307e34afc1856828c',
    '9c11c518504a320bbddf46a125bffbc0a431db1a642545e4df40a0cca2d14257',
    'b35d3249102f8f14e12067d1f9c9ce711028a44f54647a9e9ab187786ea1b6c0',
})
MAX_V7_SCOPED_PRESENTATION_CSS_SHA256 = frozenset({
    '08e516a3e3880eef70515df0db42a7a53becadd5520b97808b3481de3aca8abb',
    '73a4e87bc4dc8fefea72bbd4af06543d31606fb1cf7f12028cd4f743860b2ede',
})
# v7/v8 presentation repair retry scope, not a parser exclusion or absence.
# These exact observed heads still need an embedded-media adapter. Replaying
# the CSS/title-only repair cannot fix them; preserve their attempts/deadlines.
MAX_V7_DEFERRED_EMBED_PATHS = frozenset({
    '/futures-txf-intro-guide/', '/00958b-bond-etf-info/',
    '/brent-wti-crude-bz-cl-chart/', '/crude-oil-futures-guide-cl/',
    '/oil-investing-channels-uso/', '/futures-margin-maintenance-txf/',
    '/copper-hg-investing-guide/', '/tradingview-crude-oil-cl/',
})
# Same-owner metadata diagnosis of all eight unresolved native posts. These
# literal hints are connection setup, not prose or permission to fetch them.
MAX_NATIVE_RESOURCE_HINTS = frozenset({
    ('preconnect', 'https://fonts.googleapis.com'),
    ('preconnect', 'https://fonts.gstatic.com'),
    ('dns-prefetch', '//fonts.googleapis.com'),
    ('dns-prefetch', '//fonts.gstatic.com'),
    ('dns-prefetch', '//s.gravatar.com'),
    ('dns-prefetch', '//www.google-analytics.com'),
})
MAX_CHART_SCRIPT = 'https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js'
MAX_CHART_OPTIONS_SHA256 = frozenset({
    '5038fc5934914bedfda2620508deb90154fec98b3ae953cc38b33f4765c4a9d1',
    '9dbde1c169565b92cf5e42d888b67f94cc394148d42a03dd8dd55b34dce1872d',
})
_CLOCK = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00')
_TAGS = frozenset({
    'a', 'b', 'blockquote', 'br', 'caption', 'code', 'col', 'colgroup', 'del',
    'div', 'em', 'figcaption', 'figure', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
    'hr', 'i', 'img', 'li', 'ol', 'p', 'pre', 's', 'section', 'small', 'span',
    'strong', 'sub', 'sup', 'table', 'tbody', 'td', 'tfoot', 'th', 'thead',
    'tr', 'u', 'ul',
})


def _reference(value):
    return value.get('@id') if isinstance(value, dict) else value


def _native_clock(raw, visible):
    if not isinstance(raw, str) or not _CLOCK.fullmatch(raw):
        raise NewsError('invalid_bulk_publication_clock')
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    if (visible.get('datetime') != raw or visible.get_text(' ', strip=True)
            != f'{parsed.day} {parsed.month} 月, {parsed.year}'):
        raise NewsError('invalid_bulk_publication_clock')
    return parsed


def max_native_clock_metadata(soup, source):
    """Observe bounded native clock shapes without bodies, raw HTML or repair grants."""
    if source.provider != 'maxfinanciallife':
        raise ValueError('max_native_clock_metadata_scope_required')

    def shape(node):
        return {'tag': node.name, 'id': str(node.get('id', ''))[:100],
                'classes': [str(value)[:80] for value in node.get('class', [])[:8]]}

    def scalar(value):
        return value[:160] if isinstance(value, str) else None

    times = soup.select('article[id^="post-"] > .single-header time')
    heads = soup.select('head > meta[property="article:published_time"], '
                        'head > meta[property="article:modified_time"]')
    declarations, declaration_count = [], 0
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            objects = _objects(json.loads(script.get_text()))
        except (ValueError, TypeError):
            continue
        for node in objects:
            if node.get('@type') not in ('WebPage', 'BlogPosting'):
                continue
            declaration_count += 1
            if len(declarations) < 8:
                declarations.append({'type': node['@type'],
                    'datePublished': scalar(node.get('datePublished')),
                    'dateModified': scalar(node.get('dateModified'))})
    return {'contract_version': 2, 'head_clock_count': len(heads), 'header_time_count': len(times),
        'schema_clock_count': declaration_count,
        'head_clocks': [{'field': n.get('property'), 'content': scalar(n.get('content'))} for n in heads[:8]],
        'header_times': [{**shape(n), 'datetime': scalar(n.get('datetime')),
            'source_clock_text': n.get_text(' ', strip=True)[:100],
            'in_native_hidden_update_template': n.find_parent(class_='penci-hide-tagupdated') is not None,
            'native_template_direct_label': ([str(s)[:200] for s in n.parent.find_all(string=True, recursive=False)[:8]]
                if n.parent.get('class') == ['penci-hide-tagupdated'] else None),
            'ancestors': [shape(p) for p in islice(n.parents, 6) if p.name]} for n in times[:8]],
        'schema_clocks': declarations, 'body_text_exported': False, 'raw_response_retained': False,
        'source_clock_inferred': False, 'public_visibility_inferred': False,
        'queue_mutation_possible': False, 'article_durably_accepted': False}


def _separate_furniture(body):
    """Remove only verified direct page furniture; preserve its source text.

    Table/diagram figures are prose, not photos. Unknown pagination or content
    mixed into a tag/navigation block is an explicit incomplete layout, never
    silently truncated. The immutable response is not modified.
    """
    notices, captions = [], []
    for child in list(body.find_all(recursive=False)):
        classes = set(child.get('class', []))
        if child.get('id') == 'toc_container':
            links = child.select('ul.toc_list a[href]')
            if (classes != {'no_bullets'} or len(child.select(':scope > p.toc_title')) != 1
                    or child.select_one(':scope > p.toc_title').get_text(' ', strip=True) != '內容目錄'
                    or len(child.select(':scope > ul.toc_list')) != 1 or not links
                    or any(not link['href'].startswith('#') for link in links)
                    or child.find(['table', 'figure', 'form']) is not None):
                raise NewsError('article_identity_not_verified')
            notices.append({'kind': 'native_table_of_contents', 'text': child.get_text(' ', strip=True)})
            child.decompose()
        elif classes == {'penci-single-link-pages'}:
            if child.get_text(' ', strip=True) or child.find(['a', 'iframe']):
                raise NewsError('body_not_found')  # Further source pages are not a complete body.
            child.decompose()
        elif classes == {'post-tags'}:
            if (child.name != 'div' or any(node.name != 'a' or node.get('rel') != ['tag']
                                         for node in child.find_all(True))
                    or any(str(s).strip() for s in child.find_all(string=True, recursive=False))):
                raise NewsError('article_identity_not_verified')
            notices.append({'kind': 'native_post_tags', 'text': child.get_text(' ', strip=True)})
            child.decompose()
    # The reviewed recommendation is a final heading/list pair, not an
    # arbitrary paragraph containing a link or a cited external publication.
    children = body.find_all(recursive=False)
    if len(children) >= 2 and children[-2].name == 'h2' and children[-2].get_text(' ', strip=True) == '延伸推薦閱讀':
        heading, links = children[-2:]
        if (links.name != 'ul' or not links.select(':scope > li > a[href]')
                or any(n.name not in {'li', 'a'} for n in links.find_all(True))):
            raise NewsError('article_identity_not_verified')
        notices.append({'kind': 'native_related_reading', 'text': links.get_text(' ', strip=True),
                        'links': [{'text': a.get_text(' ', strip=True), 'url': a['href']} for a in links.select('a[href]')]})
        heading.decompose()
        links.decompose()
    for figure in list(body.find_all('figure')):
        if figure.find('img') is None or figure.find('table') is not None:
            # The shared text extractor intentionally excludes media figures.
            # These reviewed native figures contain real tables/text; unwrap
            # their layout wrapper so no financial cells are discarded.
            figure.unwrap()
            continue  # Keep every source table and literal text diagram.
        if (len(figure.select(':scope > img')) != 1 or len(figure.select(':scope > figcaption')) != 1
                or any(n.name not in {'img', 'figcaption'} for n in figure.find_all(True))
                or any(str(s).strip() for s in figure.find_all(string=True, recursive=False))):
            raise NewsError('article_identity_not_verified')
        captions.append({'kind': 'separate_native_photo_caption',
                         'text': figure.figcaption.get_text(' ', strip=True),
                         'image_url': figure.img.get('src')})
        figure.decompose()
    return notices, captions


def _separate_native_presentation(body, adapter_contract_version=6):
    """Keep source-attested CSS out of prose, without interpreting or executing it."""
    styles, notices = body.find_all('style'), []
    if len(styles) > 8:
        raise NewsError('article_identity_not_verified')
    for style in styles:
        _visible(style)
        css = style.get_text()
        digest = hashlib.sha256(css.encode()).hexdigest()
        normal = (style.attrs in ({}, {'type': 'text/css'})
                  and (digest in MAX_PRESENTATION_CSS_SHA256
                       or adapter_contract_version >= 7 and digest in MAX_V7_PRESENTATION_CSS_SHA256))
        scoped = (adapter_contract_version >= 7 and style.attrs == {'scoped': ''}
                  and digest in MAX_V7_SCOPED_PRESENTATION_CSS_SHA256)
        if (style.find(True) is not None or not (normal or scoped)):
            raise NewsError('article_identity_not_verified')
        notices.append({'kind': 'native_presentation_stylesheet_not_article_prose',
            'source_css_sha256': digest, 'source_css_characters': len(css),
            'original_response_unchanged': True, 'style_executed': False,
            'style_external_resource_requests': 0})
        style.decompose()
    return notices


def _separate_native_resource_references(body):
    """Preserve reviewed reference metadata, never execute or fetch rich media.

    A page's chart configuration is not the chart's market series, and an iframe
    title is not a video transcript. Unknown active content remains a failure.
    Only this already verified native body DOM is modified; raw bytes survive.
    """
    nodes = body.find_all(['link', 'meta', 'script', 'iframe'])
    if len(nodes) > 12:
        raise NewsError('article_identity_not_verified')
    notices, seen = [], set()
    for node in nodes:
        _visible(node)
        if node.find(True) is not None:
            raise NewsError('article_identity_not_verified')
        if node.name == 'link':
            rel = node.get('rel', [])
            key = (rel[0], node.get('href')) if len(rel) == 1 else None
            if (node.parent is not body or key not in MAX_NATIVE_RESOURCE_HINTS
                    or node.attrs != {'rel': [key[0]], 'href': key[1]} or node.get_text().strip()):
                raise NewsError('article_identity_not_verified')
            notice = {'kind': 'native_connection_hint_not_article_prose', 'source_attributes': dict(node.attrs)}
        elif node.name == 'meta':
            key = ('meta', 'x-dns-prefetch-control')
            if (node.parent is not body or node.attrs != {'http-equiv': key[1], 'content': 'on'}
                    or node.get_text().strip()):
                raise NewsError('article_identity_not_verified')
            notice = {'kind': 'native_connection_hint_not_article_prose', 'source_attributes': dict(node.attrs)}
        elif node.name == 'script':
            options = node.get_text()
            digest = hashlib.sha256(options.encode()).hexdigest()
            key = ('chart', digest)
            if (node.parent.name != 'p' or digest not in MAX_CHART_OPTIONS_SHA256
                    or node.attrs != {'type': 'text/javascript', 'src': MAX_CHART_SCRIPT, 'async': ''}):
                raise NewsError('article_identity_not_verified')
            try:
                config = json.loads(options)
            except (ValueError, TypeError):
                raise NewsError('article_identity_not_verified') from None
            if not isinstance(config, dict):
                raise NewsError('article_identity_not_verified')
            notice = {'kind': 'native_external_chart_reference_not_article_prose',
                'source_attributes': dict(node.attrs), 'source_options_sha256': digest,
                'source_widget_options': config, 'market_series_observed': False}
        else:
            key = ('video', node.get('src'))
            expected = {'title', 'width', 'height', 'src', 'frameborder', 'allow', 'referrerpolicy', 'allowfullscreen'}
            if (node.parent.name != 'p' or set(node.attrs) != expected or node.get_text().strip()
                    or not re.fullmatch(r'https://www[.]youtube[.]com/embed/[A-Za-z0-9_-]{11}[?]feature=oembed', node.get('src', ''))
                    or not node.get('title', '').strip() or len(node['title']) > 500
                    or any(not re.fullmatch(r'[1-9][0-9]{0,3}', node.get(k, '')) for k in ('width', 'height'))
                    or node['frameborder'] != '0' or node['allowfullscreen'] != ''
                    or node['referrerpolicy'] != 'strict-origin-when-cross-origin'
                    or node['allow'] != 'accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share'):
                raise NewsError('article_identity_not_verified')
            notice = {'kind': 'native_external_video_reference_not_article_prose',
                'source_attributes': dict(node.attrs), 'video_or_transcript_captured': False}
        if key in seen:
            raise NewsError('article_identity_not_verified')
        seen.add(key)
        notices.append({**notice, 'original_response_unchanged': True,
                        'external_resource_requests': 0, 'resource_executed': False})
        parent = node.parent
        node.decompose()
        if parent.name == 'p' and not parent.find(True) and not parent.get_text().strip():
            parent.decompose()  # Only an empty media wrapper, never surrounding prose.
    return notices


def max_visible_post(soup, *, page_url, adapter_contract_version=1):
    if adapter_contract_version not in MAX_LAYOUT_VERSIONS.values():
        raise ValueError('unsupported MAX native-post adapter')
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'maxfinanciallife.com' or parts.query or parts.fragment
            or not re.fullmatch(MAX_PATH_PATTERN, parts.path)
            or '/' in unquote(parts.path.strip('/')) or '\\' in unquote(parts.path)):
        raise NewsError('article_identity_not_verified')
    body = _one(soup, '#penci-post-entry-inner')
    scope = body.find_parent('article')
    if (scope is None or not re.fullmatch(r'post-[1-9][0-9]*', scope.get('id', ''))
            or not {'post', 'type-post', 'status-publish'}.issubset(scope.get('class', []))
            or _one(scope, MAX_BODY_SELECTOR) is not body):
        raise NewsError('article_identity_not_verified')
    header = _one(scope, ':scope > .single-header')
    title_node = _one(header, ':scope > h1.post-title.single-post-title.entry-title')
    title = title_node.get_text(' ', strip=True)
    html_title = _one(soup, 'head > title').get_text(' ', strip=True)
    og_title = _one(soup, 'head > meta[property="og:title"]').get('content')
    if (not title or not html_title or og_title != html_title
            or _one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url):
        raise NewsError('article_identity_not_verified')
    objects = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            objects.extend(_objects(json.loads(script.get_text())))
        except (ValueError, TypeError):
            raise NewsError('article_identity_not_verified') from None
    pages = [n for n in objects if n.get('@type') == 'WebPage' and n.get('@id') == page_url + '#webpage']
    posts = [n for n in objects if n.get('@type') == 'BlogPosting']
    if len(pages) != 1 or len(posts) != 1:
        raise NewsError('article_identity_not_verified')
    page, post = pages[0], posts[0]
    schema_headlines = (page.get('name'), post.get('headline'))
    native_title_variants = adapter_contract_version >= 7 and all(
        isinstance(value, str) and value.strip() and len(value) <= 2000 for value in schema_headlines)
    if (page.get('url') != page_url
            or not native_title_variants and any(value != html_title for value in schema_headlines)
            or post.get('@id') != page_url + '#richSnippet'
            or _reference(post.get('mainEntityOfPage')) != page['@id']
            or _reference(post.get('isPartOf')) != page['@id']):
        raise NewsError('article_identity_not_verified')
    if any(str(n.get('isAccessibleForFree', 'true')).lower() == 'false' for n in (post, page)):
        raise NewsError('paywall_or_login')
    byline = _one(scope, MAX_AUTHOR_SELECTOR, 'article_attribution_not_verified')
    author, profile = byline.get_text(' ', strip=True), byline.get('href')
    declared = post.get('author')
    if (author not in MAX_AUTHORS or profile not in {MAX_ORIGIN + '/author/ping/', MAX_ORIGIN + '/author/max-research-team/'}
            or not isinstance(declared, dict) or declared.get('name') != author
            or declared.get('@id') != profile):
        raise NewsError('article_attribution_not_verified')
    clocks = _one(header, ':scope > .post-box-meta-single')
    published_node = _one(clocks, 'time.entry-date.published')
    published = _one(soup, 'head > meta[property="article:published_time"]').get('content')
    modified_meta = soup.select('head > meta[property="article:modified_time"]')
    modified_nodes = clocks.select('time.entry-date.modified')
    schema_only_revision = adapter_contract_version >= 3 and not modified_meta and not modified_nodes
    hidden_update_notice = None
    if schema_only_revision:
        # Observed native current posts omit the head revision tag altogether.
        # Both JSON-LD declarations still carry the exact published/revised
        # seconds. The hidden theme update clock may be OLDER than publication;
        # it is source furniture, never a release date or historical witness.
        hidden = _one(header, ':scope > div.penci-hide-tagupdated')
        hidden_time = _one(hidden, ':scope > time.entry-date' if adapter_contract_version >= 5 else
                           ':scope > time.entry-date.updated')
        hidden_role = set(hidden_time.get('class', []))
        hidden_roles = ({'entry-date', 'updated'}, {'entry-date', 'published'}) if adapter_contract_version >= 5 else ({'entry-date', 'updated'},)
        if (hidden.get('class') != ['penci-hide-tagupdated']
                or hidden_role not in hidden_roles
                or adapter_contract_version < 4 and (len(hidden.find_all(True)) != 1
                    or any(str(s).strip() for s in hidden.find_all(string=True, recursive=False)))):
            raise NewsError('article_identity_not_verified')
        modified = post.get('dateModified')
        if modified != published:
            raise NewsError('invalid_bulk_publication_clock')
        modified_node = published_node
        hidden_update_notice = {'kind': 'native_hidden_update_template_not_a_publication_clock',
            'datetime_attribute': str(hidden_time.get('datetime', ''))[:160],
            'source_clock_text': hidden_time.get_text(' ', strip=True)[:100],
            'public_visibility_verified': False, 'used_as_published_or_modified_time': False}
        if hidden_role == {'entry-date', 'published'}:
            # Native unchanged posts repeat the same published clock in this
            # header-only template. Do not require a fictional update label,
            # or use a hidden copy instead of the real visible publication.
            _native_clock(published, hidden_time)
            hidden_update_notice.update(native_time_classes=['entry-date', 'published'],
                                        duplicate_publication_not_a_separate_update=True)
        if adapter_contract_version >= 4 and len(hidden.find_all(True)) > 1:
            # This header-only theme wrapper also has auxiliary markup. None
            # of it belongs to #penci-post-entry-inner or proves availability;
            # preserve the raw, count the layout, do not drop body paragraphs.
            hidden_update_notice['auxiliary_header_elements'] = len(hidden.find_all(True)) - 1
    else:
        modified = _one(soup, 'head > meta[property="article:modified_time"]').get('content')
    # Native new posts omit the update label when release and revision are
    # equal. Missing visible metadata is not a reason to invent an update or
    # discard the actual public article. All three machine declarations and
    # the real visible publication must still agree exactly.
    unpublished_revision_label = (adapter_contract_version >= 2 and not schema_only_revision and not modified_nodes
                                  and modified == published)
    if not schema_only_revision:
        modified_node = (published_node if unpublished_revision_label else
                         _one(clocks, 'time.entry-date.modified'))
    if (any(n.get('datePublished') != published or n.get('dateModified') != modified for n in (post, page))
            or _native_clock(modified, modified_node) < _native_clock(published, published_node)):
        raise NewsError('invalid_bulk_publication_clock')
    for node in (scope, header, title_node, clocks, byline, published_node, modified_node, body):
        _visible(node)
    presentation = _separate_native_presentation(body, adapter_contract_version) if adapter_contract_version >= 6 else []
    references = _separate_native_resource_references(body) if adapter_contract_version >= 9 else []
    # Inspect the scoped text, not comments/footer/related-story bylines.
    for node in body.find_all(True):
        _visible(node)
        # A native article may place its own introduction and h1 inside an
        # HTML5 header, below the already verified body container. It is prose,
        # not the outer site header. All descendants/visibility/identity and
        # external-widget refusals still apply; old capture ABIs stay exact.
        if node.name not in _TAGS and not (adapter_contract_version >= 8 and node.name == 'header'):
            raise NewsError('article_identity_not_verified')
    if adapter_contract_version >= 8:
        # The shared prose walker deliberately excludes SITE headers. These
        # already scoped and fully inspected article introductions must lose
        # only that wrapper, not their h1/prose/financial values. Never change
        # the shared exclusion set or unwrap uninspected active descendants.
        for introduction in list(body.find_all('header')):
            introduction.unwrap()
    notices, captions = _separate_furniture(body)
    notices.extend(presentation)
    notices.extend(references)
    first = body.find('p')
    if first and re.match(r'^(?:轉載|轉自|本文來源|新聞來源|來源[：:])', first.get_text(' ', strip=True)):
        raise NewsError('article_rights_or_scope_excluded')
    notices.insert(0, {'kind': 'native_headline_variants', 'visible_header': title,
                       'html_and_og_title': html_title, 'schema_headline': post['headline'],
                       'inline_headings': [n.get_text(' ', strip=True) for n in body.find_all('h1')]})
    if native_title_variants and any(value != html_title for value in schema_headlines):
        # All literal URL/graph/post-ID/author/clock bindings above still
        # apply. An independently emitted SEO/schema title is not an alias
        # witness or a reason to rewrite any of the source's actual titles.
        notices[0].update(schema_webpage_name=page['name'],
            machine_headline_variants_present=True, semantic_title_equivalence_inferred=False)
    if unpublished_revision_label:
        notices.append({'kind': 'native_no_separate_revision_label',
                        'published_at_raw': published, 'modified_at_raw': modified,
                        'basis': 'three_equal_source_machine_declarations_and_visible_publication; no_update_label_or_time_invented'})
    if schema_only_revision:
        notices.extend([{'kind': 'native_revision_from_two_equal_schema_clocks',
            'published_at_raw': published, 'modified_at_raw': modified,
            'head_modified_meta_present': False, 'visible_modified_label_present': False,
            'basis': 'literal_BlogPosting_and_WebPage_dateModified_equal_head_and_visible_publication; no_missing_tag_or_old_publication_invented'},
            hidden_update_notice])
    return {
        'headline': title, 'author': [{'name': author}],
        'author_attribution_detail_json': json.dumps({'native_credit': author,
            'native_profile_url': profile, 'role': 'publisher_declared_pseudonym' if author == 'MAX' else 'publisher_declared_research_team',
            'named_reporter_independently_verified': False,
            'operator_is_not_inferred_author': True,
            'editorial_policy_ai_assistance_possible_not_article_specific_proof': True}, ensure_ascii=False),
        'datePublished': published, 'dateModified': modified,
        'published_time_basis': 'publisher_declared_machine_seconds_and_offset_match_visible_date; current_revision_not_original_historical_body',
        'article_identity_basis': 'literal_native_canonical_OG_WebPage_BlogPosting_post_ID_scoped_header_and_author_profile_v1',
        'body_completeness': ('current_native_post_prose_tables_numbers_and_caveats; native_TOC_tags_related_links_and_separate_photo_captions_preserved_as_metadata; not_original_historical_version'
            + ('; native_external_chart_and_video_references_preserved_as_metadata; no_media_payload_transcript_or_market_series_captured'
               if any(n['kind'] in {'native_external_chart_reference_not_article_prose', 'native_external_video_reference_not_article_prose'} for n in references) else '')),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'verified_image_captions_json': json.dumps(captions, ensure_ascii=False) if captions else None,
    }, scope
