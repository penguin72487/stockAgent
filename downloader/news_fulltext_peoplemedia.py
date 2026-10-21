"""Observed PeopleMedia native written articles; no HTTP or discovery.

Exact current declarations bind the literal UUID, title, credit and body.
Malformed revision clocks remain raw evidence, not reconstructed timestamps.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


PEOPLEMEDIA_ORIGIN = 'https://www.peoplemedia.tw'
PEOPLEMEDIA_PATH_PATTERN = r'^/news/[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
PEOPLEMEDIA_BODY_SELECTOR = ':scope > .peoplemedia-native-body'
_CREDITS = frozenset({'陳文蔚／綜合報導', '陳文蔚／台北報導', '唐詩／台北報導',
                      '財經中心／綜合報導', '政治中心／綜合報導'})
_V2_CREDITS = frozenset({'記者陳文蔚/綜合報導', '生活中心／台北報導', '記者何豪毅／綜合報導',
    '國際中心／綜合報導', '記者林冠妙／台北報導', '財經、社會中心／綜合報導', '政治中心/綜合報導',
    '社會中心／綜合報導', '財經中心/綜合報導'})
_V3_CREDITS = frozenset({'文／陳永興', '陳俊廷/高雄報導'})
# Observed, missing v1 heads repairable by these exact credit/title changes.
# This is a one-time migration boundary, not a source URL discovery list.
_V2_REPAIR_IDS = frozenset({
    '02a3ad54-17af-4f7d-932b-8f529638165e', '03cf29b1-d77e-4295-80b1-a2906f1c384e',
    '31ec7e55-16ca-409e-8cff-fb4c1edf9a14', '444778e5-4cb7-480e-9a71-352bb563c19d',
    '5449f6a9-3a03-4a97-b65d-4c0ad2f2b1c3', '6c49aec6-4862-41e3-9822-6f0bad9637da',
    'dff088b7-4e70-4aec-bb14-88a1562f7cc7', '1a25c346-5a6e-4c61-b1be-caf2414c40a8',
    '8ba2121d-51e4-4ae9-be3b-1ba04c880ff7'})
_V3_REPAIR_IDS = frozenset({'52e01b6b-3b65-479c-bd44-1b2d348772e1',
    'fb949cfe-df32-467d-be9f-c163f26a3887', '8ac3cee5-4d74-421b-b6dd-dbc58793d9df',
    'e2d32f8b-5fea-49a0-a24d-06fb14bec3cd'})
_BODY_TAGS = frozenset({'p', 'br', 'a', 'img', 'span', 'strong', 'b', 'em', 'i', 'u',
    'h2', 'h3', 'h4', 'ul', 'ol', 'li', 'blockquote', 'table', 'caption', 'thead',
    'tbody', 'tfoot', 'tr', 'th', 'td', 'div'})
_WIDGET = 'dablewidget_37Jvj47N'
_WIDGET_SCRIPT = "dable('renderWidget', 'dablewidget_37Jvj47N');"
_EMAIL_DECODER = '/cdn-cgi/scripts/5c5dd728/cloudflare-static/email-decode.min.js'


def peoplemedia_article_id(url, *, requested=False):
    if not isinstance(url, str) or type(requested) is not bool:
        return None
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if (parts.scheme != 'https' or parts.netloc != 'www.peoplemedia.tw'
            or parts.query or parts.fragment or not re.fullmatch(PEOPLEMEDIA_PATH_PATTERN, parts.path)):
        return None
    return parts.path.removeprefix('/news/')


def peoplemedia_v2_repair_article_id(url, *, requested=False):
    """Only the nine evidenced missing predecessors, not every v1 failure."""
    article_id = peoplemedia_article_id(url, requested=requested)
    return article_id if article_id in _V2_REPAIR_IDS else None


def peoplemedia_v3_repair_article_id(url, *, requested=False):
    article_id = peoplemedia_article_id(url, requested=requested)
    return article_id if article_id in _V3_REPAIR_IDS else None


def _native_visible(node):
    _visible(node)
    for parent in [node, *node.parents]:
        if getattr(parent, 'attrs', None) and set(parent.get('class', [])).intersection(
                {'hidden', 'd-none', 'sr-only', 'screen-reader-text'}):
            raise NewsError('article_identity_not_verified')


def peoplemedia_validation_metadata(soup, *, page_url):
    """Observe bounded native declarations, not prose, permission or acceptance.

    Use the already received, unmodified DOM. Unknown signatures, clocks and
    body shapes must remain diagnosable without another request or a raw-body
    exception. This function does not call the accepting adapter.
    """
    def text(value):
        if not isinstance(value, str):
            return {'type': type(value).__name__}
        return {'value': value[:512], 'characters': len(value),
                'sha256': hashlib.sha256(value.encode()).hexdigest(), 'truncated': len(value) > 512}

    def public_url(value):
        if not isinstance(value, str):
            return {'type': type(value).__name__}
        try:
            parts = urlsplit(value)
            display = (parts._replace(query='', fragment='').geturl()[:512]
                       if parts.scheme == 'https' and parts.netloc == 'www.peoplemedia.tw'
                       and not parts.username and not parts.password else None)
        except ValueError:
            display = None
        return {'display_url': display, 'sha256': hashlib.sha256(value.encode()).hexdigest()}

    def values(selector, attr=None, *, urls=False):
        nodes = soup.select(selector)
        convert = public_url if urls else text
        return {'node_count': len(nodes), 'truncated': len(nodes) > 8,
                'values': [convert(n.get(attr) if attr else n.get_text(' ', strip=True)) for n in nodes[:8]]}

    def reference(value):
        """Unfetched DOM reference, not a request or an admitted media origin."""
        if not isinstance(value, str):
            return {'type': type(value).__name__}
        try:
            parts = urlsplit(value)
            display = (parts._replace(query='', fragment='').geturl()[:512]
                       if parts.scheme in {'', 'http', 'https'} and parts.netloc
                       and not parts.username and not parts.password else None)
        except ValueError:
            display = None
        return {'display_url': display, 'characters': len(value),
                'sha256': hashlib.sha256(value.encode()).hexdigest(), 'requested': False}

    scopes = soup.select('#news[itemscope][itemtype="http://schema.org/NewsArticle"]')
    native = soup.select('#news > #news_content > #newscontent[itemprop=articleBody]')
    facts = {
        'contract_version': 2, 'current_native_url_recognized': peoplemedia_article_id(page_url) is not None,
        'article_scope_count': len(scopes), 'global_h1_count': len(soup.select('h1')),
        'headings': values('#news > h1.news_title[itemprop=headline]'), 'head_titles': values('head > title'),
        'og_titles': values('head > meta[property="og:title"]', 'content'),
        'og_types': values('head > meta[property="og:type"]', 'content'),
        'canonical_urls': values('head > link[rel=canonical]', 'href', urls=True),
        'og_urls': values('head > meta[property="og:url"]', 'content', urls=True),
        'main_entity_urls': values('#news > meta[itemprop=mainEntityOfPage]', 'itemid', urls=True),
        'publisher_names': values('#news > span[itemprop=publisher] > meta[itemprop=name]', 'content'),
        'publisher_urls': values('#news > span[itemprop=publisher] > meta[itemprop=url]', 'content', urls=True),
        'native_credits': values('#news > #news_author > span.author[itemprop=author] > span[itemprop=name]'),
        'visible_clocks': values('#news > #news_author > span.date'),
        'native_published': values('#news > #news_author > meta[itemprop=datePublished]', 'content'),
        'head_published': values('head > meta[property="article:published_time"]', 'content'),
        'native_modified': values('#news > #news_author > meta[itemprop=dateModified]', 'content'),
        'head_modified': values('head > meta[property="article:modified_time"]', 'content'),
        'content_id_count': len(soup.select('#newscontent')), 'native_body_count': len(native),
        'native_body_sample_truncated': len(native) > 8, 'native_body_structures': [],
        'labels_are_observations_not_verified_attribution': True,
        'body_text_retained': False, 'raw_response_retained': False,
        'article_identity_or_scope_granted': False, 'additional_requests': 0,
    }
    for node in native[:8]:
        elements = node.find_all(True, limit=4097)
        names = {}; hidden = 0
        for element in [node, *elements[:4096]]:
            if element is not node:
                names[element.name] = names.get(element.name, 0) + 1
            try:
                _native_visible(element)
            except NewsError:
                hidden += 1
        scripts = node.find_all('script', limit=9)
        widgets = node.select('div[id="' + _WIDGET + '"]')
        paragraphs = node.find_all(['p', 'blockquote'], limit=33)
        embeds = node.find_all(['iframe', 'object', 'embed'], limit=9)
        separators = node.find_all('hr', limit=9)
        facts['native_body_structures'].append({
            'sampled_element_count': min(len(elements), 4096), 'elements_truncated': len(elements) > 4096,
            'tag_counts': dict(sorted(names.items())[:64]), 'tag_names_truncated': len(names) > 64,
            'unrecognized_tag_names': sorted(set(names) - _BODY_TAGS - {'script'})[:64],
            'sampled_invisible_nodes': hidden, 'scripts_truncated': len(scripts) > 8,
            'scripts': [{'attributes_present': bool(n.attrs),
                'attribute_names': sorted(n.attrs)[:16], 'attribute_names_truncated': len(n.attrs) > 16,
                'src': reference(n.get('src')), 'type': text(n.get('type')), 'charset': text(n.get('charset')),
                'parent_tag': n.parent.name if n.parent else None,
                'parent_is_native_ad_hook': bool(n.parent and n.parent.get('id') == _WIDGET),
                'child_tag_count': len(n.find_all(True, limit=17)),
                'script_characters': len(n.string or ''),
                'exact_reviewed_ad_hook': not n.attrs and (n.string or '').strip() == _WIDGET_SCRIPT,
                'script_sha256': hashlib.sha256((n.string or '').encode()).hexdigest()} for n in scripts[:8]],
            'embeds_truncated': len(embeds) > 8,
            'embeds': [{'tag': n.name, 'attribute_names': sorted(n.attrs)[:16],
                'attribute_names_truncated': len(n.attrs) > 16, 'src': reference(n.get('src')),
                'child_tag_count': len(n.find_all(True, limit=17)),
                'fallback_characters': len(n.get_text(' ', strip=True)),
                'parent_tag': n.parent.name if n.parent else None} for n in embeds[:8]],
            'separators_truncated': len(separators) > 8,
            'separators': [{'tag': n.name, 'attribute_names': sorted(n.attrs)[:16],
                'previous_sibling_tag': getattr(n.find_previous_sibling(), 'name', None),
                'next_sibling_tag': getattr(n.find_next_sibling(), 'name', None)} for n in separators[:8]],
            'ad_widget_count': len(widgets),
            'sampled_populated_ad_widgets': sum(bool(n.find(True) or n.get_text(strip=True)) for n in widgets[:8]),
            'ad_widgets_observed_before_any_removals': True,
            'ad_widget_sample_truncated': len(widgets) > 8,
            'sampled_reprint_prefix_present': any(re.match(
                r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])', p.get_text(' ', strip=True))
                for p in paragraphs[:32]),
            'reprint_prefix_sample_truncated': len(paragraphs) > 32,
            'diagnostic_only_not_body_acceptance': True,
        })
    return facts


def peoplemedia_visible_article(soup, *, page_url, adapter_contract_version=1):
    """One scoped prose region, never related stories, photos or ad text."""
    if type(adapter_contract_version) is not int or adapter_contract_version not in {1, 2, 3}:
        raise NewsError('article_identity_not_verified')
    if peoplemedia_article_id(page_url) is None:
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, '#news[itemscope][itemtype="http://schema.org/NewsArticle"]')
    heading = _one(scope, ':scope > h1.news_title[itemprop=headline]')
    title = heading.get_text(' ', strip=True)
    canonicals = soup.select('head > link[rel=canonical]')
    og_title = _one(soup, 'head > meta[property="og:title"]').get('content')
    head_title = _one(soup, 'head > title').get_text(' ', strip=True)
    strict_title = og_title == '【民報】' + title and head_title == title + ' | 民報 PeopleNews'
    # The observed declarations contain one trailing ASCII space before their
    # fixed wrappers. No interior whitespace, Unicode, fuzzy or substring match.
    title_suffix = ' | 民報 PeopleNews'
    v2_title = (adapter_contract_version >= 2 and isinstance(og_title, str)
        and og_title.startswith('【民報】') and og_title[4:].strip(' ') == title
        and head_title.endswith(title_suffix) and head_title[:-len(title_suffix)].strip(' ') == title)
    if (not title or len(soup.select('h1')) != 1 or not 1 <= len(canonicals) <= 2
            or any(node.get('href') != page_url for node in canonicals)
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'
            or not (strict_title or v2_title)
            or _one(scope, ':scope > meta[itemprop=mainEntityOfPage]').get('itemid') != page_url
            or _one(scope, ':scope > span[itemprop=publisher] > meta[itemprop=name]').get('content') != '民報'
            or _one(scope, ':scope > span[itemprop=publisher] > meta[itemprop=url]').get('content') != page_url):
        raise NewsError('article_identity_not_verified')
    header = _one(scope, ':scope > #news_author')
    author = _one(header, ':scope > span.author[itemprop=author] > span[itemprop=name]',
                  'article_attribution_not_verified')
    credit = author.get_text(' ', strip=True)
    credits = _CREDITS | (_V2_CREDITS if adapter_contract_version >= 2 else frozenset())
    if adapter_contract_version >= 3:
        credits |= _V3_CREDITS
    if credit not in credits:
        raise NewsError('article_rights_or_scope_excluded' if credit else 'article_attribution_not_verified')
    published = _one(header, ':scope > meta[itemprop=datePublished]').get('content')
    visible_clock = _one(header, ':scope > span.date')
    try:
        if not isinstance(published, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+08:00', published):
            raise ValueError('unreviewed release clock')
        datetime.fromisoformat(published)
        if (visible_clock.get_text(' ', strip=True) != published[:16]
                or _one(soup, 'head > meta[property="article:published_time"]').get('content') != published):
            raise ValueError('conflicting release declarations')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    modified = _one(header, ':scope > meta[itemprop=dateModified]').get('content')
    if (not isinstance(modified, str) or not modified
            or _one(soup, 'head > meta[property="article:modified_time"]').get('content') != modified):
        raise NewsError('article_identity_not_verified')
    try:
        parsed_modified = datetime.fromisoformat(modified)
        modified_valid = (bool(re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+08:00', modified))
                          and parsed_modified.tzinfo is not None)
    except ValueError:
        modified_valid = False
    native = _one(scope, ':scope > #news_content > #newscontent[itemprop=articleBody]', 'body_not_found')
    if len(soup.select('#newscontent')) != 1:
        raise NewsError('article_identity_not_verified')
    for node in (scope, heading, header, author, visible_clock, native):
        _native_visible(node)
    notices = [{'kind': 'native_release_and_revision_clocks', 'published_raw': published,
        'modified_raw': modified, 'visible_release_raw': visible_clock.get_text(' ', strip=True),
        'modified_clock_valid': modified_valid, 'invalid_revision_not_coerced': not modified_valid,
        'observed_historical_PIT': False}]
    if not strict_title:
        notices.append({'kind': 'native_title_outer_ASCII_space_variation', 'head_title_raw': head_title,
                        'og_title_raw': og_title, 'visible_title': title,
                        'interior_whitespace_or_fuzzy_matching': False})
    # These observed empty ad hooks contain no prose; unknown scripts or
    # populated widgets fail rather than being silently stripped as furniture.
    for script in list(native.select('script')):
        ad_hook = not script.attrs and (script.string or '').strip() == _WIDGET_SCRIPT
        email_hook = (adapter_contract_version >= 3
            and script.attrs == {'src': _EMAIL_DECODER, 'data-cfasync': 'false'}
            and not script.find(True) and not (script.string or '').strip()
            and script.parent.name == 'div' and script.parent.get('id') == _WIDGET)
        if not (ad_hook or email_hook):
            raise NewsError('article_identity_not_verified')
        if email_hook:
            _native_visible(script.parent)
            notices.append({'kind': 'unfetched_source_email_decoder_inside_ad_hook',
                'source_reference_raw': _EMAIL_DECODER, 'script_requested': False, 'script_executed': False,
                'obfuscated_addresses_decoded': False, 'access_controls_bypassed': False})
        script.decompose()
    for widget in list(native.select('div[id="' + _WIDGET + '"]')):
        if widget.find(True) or widget.get_text(strip=True):
            raise NewsError('article_identity_not_verified')
        widget.decompose()
    # These are empty, native video frames, not alternate article bodies.
    # Retain references as notices, do not visit/execute/transcribe them or
    # silently turn a multimedia article into a claim of complete video data.
    videos = native.find_all('iframe') if adapter_contract_version >= 3 else []
    if len(videos) > 8:
        raise NewsError('article_identity_not_verified')
    for video in videos:
        _native_visible(video)
        def dimension(value, *, percent=False):
            return isinstance(value, str) and (percent and value == '100%' or
                bool(re.fullmatch(r'[1-9][0-9]{0,3}', value)) and int(value) <= 8192)
        if (set(video.attrs) != {'src', 'width', 'height', 'frameborder'}
                or not re.fullmatch(r'https://www[.]youtube[.]com/embed/[A-Za-z0-9_-]{11}', str(video.get('src', '')))
                or video.get('frameborder') != '0' or not dimension(video.get('width'), percent=True)
                or not dimension(video.get('height')) or video.find(True) or video.get_text(strip=True)):
            raise NewsError('article_identity_not_verified')
        notices.append({'kind': 'unfetched_native_youtube_video', 'source_reference_raw': video['src'],
            'media_requested': False, 'media_transcribed': False, 'media_ownership_inferred': False,
            'native_written_prose_only': True, 'multimedia_complete': False})
        video.decompose()
    for node in native.find_all(True):
        if (node.name not in _BODY_TAGS and not (adapter_contract_version >= 3 and node.name == 'hr' and not node.attrs)):
            raise NewsError('article_identity_not_verified')
        _native_visible(node)
        if node.name in {'p', 'blockquote'} and re.match(
                r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])', node.get_text(' ', strip=True)):
            raise NewsError('article_rights_or_scope_excluded')
    for image in list(native.select('img')):
        notices.append({'kind': 'unfetched_inline_image', 'media_requested': False,
                        'image_ownership_inferred': False})
        image.decompose()
    normalized = soup.new_tag('div', attrs={'class': 'peoplemedia-native-body'})
    while native.contents:
        normalized.append(native.contents[0].extract())
    body_scope = soup.new_tag('div')
    body_scope.append(normalized)
    return {
        'headline': title, 'author': [{'name': credit}], 'datePublished': published, 'dateModified': modified,
        'author_attribution_detail_json': json.dumps({'native_credit': credit,
            'role': 'publisher_declared_native_credit_not_verified_person',
            'editorial_center_label': credit.startswith(('財經中心', '政治中心', '生活中心',
                '國際中心', '社會中心', '財經、社會中心')),
            'photo_credit_is_not_text_author': True}, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'published_time_basis': 'matching_native_and_head_release_seconds_offset_+08:00; '
            'visible_minute_matches; current_source_declaration_not_observed_historical_PIT',
        'article_identity_basis': 'literal_UUID_same_canonical_OG_mainEntity_publisher_URL; '
            + ('single_native_title_head_and_exact_reviewed_credit_v1' if strict_title and credit in _CREDITS
               else 'native_title_outer_ASCII_spaces_only_and_exact_observed_credit_v2' if credit not in _V3_CREDITS
               else 'native_title_and_exact_observed_named_credit_v3_not_generic_contributor_scope'),
        'body_completeness': 'current_native_articleBody_ordered_prose_and_tables_once; '
            'empty_verified_ad_hook_removed; hero_photo_and_related_regions_excluded; '
            'inline_media_unfetched; not_original_historical_version'
            + ('; native_written_prose_only; referenced_youtube_videos_unfetched_not_transcribed; multimedia_not_complete'
               if videos else ''),
    }, body_scope
