"""Observed IDN legacy articles; pure DOM validation, no discovery or HTTP.

The header calendar is the current day, not the release clock. Only the
publisher's trailing, date-only signature is retained as publication evidence.
The literal response URL remains authoritative: no category, host or ID alias
is inferred from a similar headline, byline or date embedded in a filename.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
import re
from urllib.parse import urlsplit

from bs4.element import Comment, NavigableString

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


IDN_ORIGIN = 'https://www.idn.com.tw'
IDN_PATH_PATTERN = r'^/news/news_content[.]aspx$'
IDN_BODY_SELECTOR = ':scope > .idn-native-body'
_CREDIT = '【記者柯安聰台北報導】'
_REVIEWED_CREDITS = {_CREDIT: '柯安聰', '【記者郭襄陽台北報導】': '郭襄陽',
                     '【記者郭宜均台北報導】': '郭宜均', '【記者謝政儒台北報導】': '謝政儒'}
_COPYRIGHT = 'Copyright © 2012 自立晚報. All rights reserved. 版權所有，禁止擅自轉貼節錄'
_CLOSING = re.compile(r'(?:\((自立電子報|自立晚報)([0-9]{4}/[0-9]{1,2}/[0-9]{1,2})\)'
                      r'|（(自立電子報|自立晚報)([0-9]{4}/[0-9]{1,2}/[0-9]{1,2})）)$')
_TAGS = frozenset({'br', 'div', 'img'})
_NATIVE_DATE_V2 = r'(?:[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}|[0-9]{8})'
_CLOSING_V2 = re.compile(r'\((自立電子報|自立晚報)('+_NATIVE_DATE_V2+r')\)'
                        r'|（(自立電子報|自立晚報)('+_NATIVE_DATE_V2+r')）')
# Filled only for the fully reviewed old/new capture pair. A parser change
# must not create fresh HTTP pacing, REP/refusal or diagnostic allowances.
IDN_V2_SCOPE_REPAIR_IDENTITY = (
    '57c06704c790b0ba06ea571fef391c7c4151570b317b128ed688f931327cd171',
    '0797540609346dd6ef946b0828d22839ec7aa869d4c2532b91016ec552b4ce9b',
)
IDN_V2_PRIOR_HTTP_MEASUREMENT_IDENTITY = (
    '57c06704c790b0ba06ea571fef391c7c4151570b317b128ed688f931327cd171',
    'c8e737d8dd7035444dab71105e6bbec4d85e462b1a6d0b0ccd8954a63704e134',
)
IDN_V3_SCOPE_REPAIR_IDENTITY = (
    '0797540609346dd6ef946b0828d22839ec7aa869d4c2532b91016ec552b4ce9b',
    '0e6cc8feb882d30336c318b3ad5987cd123bf01c5fb766bf4ccaf40b6b62cd57',
)
IDN_V3_PRIOR_HTTP_MEASUREMENT_IDENTITY = (
    '57c06704c790b0ba06ea571fef391c7c4151570b317b128ed688f931327cd171',
    'c8e737d8dd7035444dab71105e6bbec4d85e462b1a6d0b0ccd8954a63704e134',
)


def idn_article_id(url, *, requested=False):
    """Recognize literal observed routes; do not generate or normalize aliases."""
    if not isinstance(url, str) or type(requested) is not bool:
        return None
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if (parts.scheme != 'https' or parts.netloc != 'www.idn.com.tw' or parts.fragment
            or not re.fullmatch(IDN_PATH_PATTERN, parts.path)):
        return None
    pairs = parts.query.split('&')
    fields = {}
    for pair in pairs:
        key, separator, value = pair.partition('=')
        if not separator or key in fields or key not in {'artid', 'catid', 'catsid', 'catdid'}:
            return None
        if key == 'artid':
            if not re.fullmatch(r'[0-9]{8}[a-z]{2,16}[0-9]{3}', value):
                return None
        elif not re.fullmatch(r'[0-9]{1,4}', value):
            return None
        fields[key] = value
    return fields.get('artid')


def _native_visible(node):
    _visible(node)
    for parent in [node, *node.parents]:
        if getattr(parent, 'attrs', None) and set(parent.get('class', [])).intersection(
                {'hidden', 'd-none', 'sr-only', 'screen-reader-text'}):
            raise NewsError('article_identity_not_verified')


def _image_prefaced_credit(native):
    """Describe one native image/short-line/byline structure, never drop prose.

    This is not arbitrary searching for a reporter somewhere in the article.
    Only the first native image and one direct-text line may precede the exact
    reporter at the start of the immediately following native text node.
    """
    nodes = [node for node in native.contents if not isinstance(node, Comment)
             and not (isinstance(node, NavigableString) and not node.strip())]
    if len(nodes) < 5:
        return None
    image, break1, prefix, break2, paragraph = nodes[:5]
    if (getattr(image, 'name', None) != 'div' or image.get('align') != 'center'
            or image.get_text(strip=True) or len(image.find_all('img')) != 1
            or any(node.name != 'img' for node in image.find_all(True))
            or getattr(break1, 'name', None) != 'br' or getattr(break2, 'name', None) != 'br'
            or not isinstance(prefix, NavigableString) or not isinstance(paragraph, NavigableString)):
        return None
    text, following = prefix.strip(), paragraph.strip()
    if (not 1 <= len(text) <= 160 or '\n' in text or '\r' in text
            or any(marker in text for marker in ('【', '】', '記者', '轉載', '轉自', '本文由', '新聞來源'))):
        return None
    credit = next((value for value in _REVIEWED_CREDITS if following.startswith(value)), None)
    if credit is None:
        return None
    return {'native_credit': credit, 'prefix_characters': len(text),
        'prefix_sha256_utf8': sha256(text.encode()).hexdigest(),
        'structure': 'first_centered_single_image_BR_short_direct_text_BR_reviewed_reporter',
        'prefix_text_retained': False, 'article_admission_granted': False}


def idn_validation_metadata(soup, *, page_url):
    """Bounded declarations only, from the received DOM; no article acceptance."""
    bodies = soup.select('td.newsa')
    # Diagnostic recognition is deliberately broader than article admission.
    # Only declaration-shaped strings from bounded edges are retained, never
    # arbitrary first/last paragraphs or a guessed URL-derived release date.
    credit = re.compile(r'(?:【|\[|（|\()(?:記者|本報記者)[\u3400-\u9fff]{2,24}(?:報導|報道)(?:】|\]|）|\))')
    clock = re.compile(r'[（(]?自立(?:電子報|晚報)[\s0-9０-９/／.\-年月日:：()（）]{1,48}')
    result = {'contract_version': 4, 'native_request_recognized': idn_article_id(page_url) is not None,
        'heading_count': len(soup.select('td.headnewsd')), 'body_count': len(bodies),
        'head_titles': [n.get_text(' ', strip=True)[:512] for n in soup.select('head > title')[:4]],
        'native_headings': [n.get_text(' ', strip=True)[:512] for n in soup.select('td.headnewsd')[:4]],
        'body_structures': [], 'body_text_retained': False, 'article_identity_or_scope_granted': False,
        'additional_requests': 0}
    if not bodies:
        # A 200 response without the article can be an origin error. Recognize
        # fixed operational labels only; unknown text is not copied or called
        # a deletion, challenge or transient error without an actual witness.
        visible = soup.get_text(' ', strip=True)
        labels = ('Service Unavailable', 'Internal Server Error', 'Server Error',
            'HTTP Error', 'The service is unavailable.', 'Too Many Requests',
            'Access Denied', 'Microsoft OLE DB', '頁面不存在', '找不到網頁', '網頁不存在')
        result['non_article_document'] = {'visible_characters': len(visible),
            'recognized_operational_labels': [label for label in labels if label in visible],
            'element_count': len(soup.find_all(True)), 'text_retained': False,
            'source_outcome_classified': False}
        # Inspect literal static scripts only; never execute or follow them.
        # Fixed labels and lengths/hashes avoid retaining arbitrary script text,
        # credentials, source prose or inferred redirects/identity aliases.
        notices = ('查無資料', '查無此新聞', '查無此筆資料', '查無此文章', '此新聞不存在',
            '新聞不存在', '資料不存在', '文章不存在', '無此新聞', '無此資料',
            '新聞已移除', '資料已刪除', '已被刪除', '找不到資料', '本新聞已下架')
        scripts = soup.find_all('script', limit=9)
        result['non_article_document'].update(
            tag_names=[node.name for node in soup.find_all(True, limit=32)],
            scripts_truncated=len(scripts) > 8, static_scripts=[])
        for script in scripts[:8]:
            code = script.get_text()
            sample = code[:4096]
            alerts = re.findall(r"\balert\s*\(\s*(['\"])([^'\"\\\r\n]{1,96})\1\s*\)", sample)
            # Only a short CJK operational notice in this non-article/script
            # context; never copy Latin tokens, URLs, arbitrary JS or prose.
            short_notices = [value for _, value in alerts if re.fullmatch(
                r'[\u3400-\u9fff\s，。！？、：；（）「」『』,.!?…！]{1,96}', value)]
            result['non_article_document']['static_scripts'].append({
                'characters': len(code), 'sha256_utf8': sha256(code.encode()).hexdigest(),
                'scan_truncated': len(code) > 4096,
                'recognized_operational_labels': [label for label in notices if label in sample],
                'short_CJK_alert_notices': short_notices[:4],
                'short_notice_is_not_article_or_permanent_absence_proof': True,
                'has_alert_call': bool(re.search(r'\balert\s*\(', sample)),
                'has_history_back_call': bool(re.search(r'\bhistory\s*\.\s*back\s*\(', sample)),
                'has_history_negative_one_call': bool(re.search(r'\bhistory\s*\.\s*go\s*\(\s*-1\s*\)', sample)),
                'has_location_reference': bool(re.search(r'\blocation\s*(?:[.=\[])', sample)),
                'executed_or_followed': False, 'text_retained': False})
    for body in bodies[:4]:
        text = body.get_text(' ', strip=True)
        signature = re.match(r'^【([^】]{1,96})】', text)
        closing = _CLOSING.search(text)
        tags = body.find_all(True, limit=4097)
        leading = next(credit.finditer(text[:256]), None)
        prefix = text[:leading.start()] if leading else None
        markers = {'\ufeff', '\u200b', '\u2060'}
        result['body_structures'].append({
            'native_credit': signature[0] if signature else None,
            'trailing_publisher_clock': closing[0] if closing else None,
            'leading_credit_declarations': [match[0] for match in list(credit.finditer(text[:256]))[:4]],
            'first_credit_offset': leading.start() if leading else None,
            'precredit_characters': len(prefix) if prefix is not None else None,
            'precredit_only_whitespace_or_noncontent_markers':
                all(c.isspace() or c in markers for c in prefix) if prefix is not None else None,
            'precredit_noncontent_codepoints':
                sorted({f'U+{ord(c):04X}' for c in prefix if c in markers}) if prefix is not None else [],
            'precredit_photo_caption_marker':
                any(marker in prefix for marker in ('(圖)', '（圖）', '圖說')) if prefix is not None else None,
            'first_child_structures': [{'kind': child.name or 'text',
                'text_characters': len(child.get_text() if child.name else str(child))}
                for child in body.contents[:12]],
            'image_prefaced_credit_structure': _image_prefaced_credit(body),
            'closing_date_declarations': [match[0].strip() for match in list(clock.finditer(text[-256:]))[:4]],
            'lexical_declarations_are_not_verified_credit_or_release_clocks': True,
            'tag_names': sorted({node.name for node in tags[:4096]})[:64],
            'elements_truncated': len(tags) > 4096,
            'labels_not_verified_person_identity_or_originality': True})
    return result


def idn_visible_article(soup, *, page_url, adapter_contract_version=1):
    if type(adapter_contract_version) is not int or adapter_contract_version not in {1, 2, 3}:
        raise ValueError('unreviewed IDN native adapter')
    if idn_article_id(page_url) is None:
        raise NewsError('article_identity_not_verified')
    heading = _one(soup, 'td.headnewsd')
    native = _one(soup, 'td.newsa', 'body_not_found')
    title = heading.get_text(' ', strip=True)
    if (not title or heading.find(True) or not heading.find_parent('table')
            or not native.find_parent('table')
            or _one(soup, 'head > title').get_text(' ', strip=True) != '自立晚報 - ' + title):
        raise NewsError('article_identity_not_verified')
    # There is no canonical or OG URL in the observed legacy pages. Do not
    # manufacture one; if the publisher adds a declaration it must match.
    for selector, attribute in (('head > link[rel=canonical]', 'href'),
                                ('head > meta[property="og:url"]', 'content')):
        declarations = soup.select(selector)
        if declarations and (len(declarations) != 1 or declarations[0].get(attribute) != page_url):
            raise NewsError('article_identity_not_verified')
    for node in (heading, native, *native.find_all(True)):
        _native_visible(node)
    for node in native.find_all(True):
        if node.name not in _TAGS:
            raise NewsError('article_identity_not_verified')
    text = native.get_text(' ', strip=True)
    credits = _REVIEWED_CREDITS if adapter_contract_version >= 2 else {_CREDIT: '柯安聰'}
    credit = next((value for value in credits if text.startswith(value)), None)
    prefix_evidence = None
    if credit is None and adapter_contract_version == 3:
        prefix_evidence = _image_prefaced_credit(native)
        credit = prefix_evidence['native_credit'] if prefix_evidence else None
    if credit is None:
        raise NewsError('article_rights_or_scope_excluded' if re.match(r'^【[^】]+】', text)
                        else 'article_attribution_not_verified')
    if re.search(r'(?:^|[\r\n])\s*[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])',
                 native.get_text('\n', strip=True)):
        raise NewsError('article_rights_or_scope_excluded')
    if adapter_contract_version == 1:
        closing = _CLOSING.search(text)
        if not closing:
            raise NewsError('invalid_bulk_publication_clock')
    else:
        # Observed closing signatures may precede a photo caption. Preserve
        # every caption and the literal compact date; do not take story-event,
        # header-current-day, index, filename or capture clocks as publication.
        declarations = list(_CLOSING_V2.finditer(text[-512:]))
        if len(declarations) > 1:
            raise NewsError('invalid_bulk_publication_clock')
        closing = declarations[0] if declarations else None
        if closing is None and re.search(r'自立(?:電子報|晚報)\s*[0-9]', text[-512:]):
            raise NewsError('invalid_bulk_publication_clock')
    published = (closing[2] or closing[4]) if closing else None
    if published is not None:
        try:
            datetime.strptime(published, '%Y%m%d' if '/' not in published else '%Y/%m/%d')
        except ValueError:
            raise NewsError('invalid_bulk_publication_clock') from None
    footers = [node for node in soup.select('td.body_9g')
               if 'Copyright' in node.get_text(' ', strip=True)]
    if (len(footers) != 1
            or re.sub(r'\s+', ' ', footers[0].get_text(' ', strip=True)) != _COPYRIGHT):
        raise NewsError('article_rights_not_verified')
    _native_visible(footers[0])
    release_notice = ({'kind': 'publisher_trailing_release_date', 'text': closing[0],
         'clock_precision': 'date_only', 'intraday_time_or_timezone_inferred': False} if closing else
        {'kind': 'publisher_release_date_unknown', 'text': None,
         'clock_precision': 'unknown', 'release_clock_or_training_eligibility_inferred': False})
    notices = [release_notice,
        {'kind': 'publisher_copyright', 'text': footers[0].get_text(' ', strip=True),
         'open_license_or_bulk_permission_inferred': False},
    ]
    if prefix_evidence:
        notices.append({'kind': 'publisher_native_precredit_image_and_short_line',
            'structure': prefix_evidence['structure'],
            'prefix_characters': prefix_evidence['prefix_characters'],
            'prefix_sha256_utf8': prefix_evidence['prefix_sha256_utf8'],
            'prefix_prose_preserved_in_body': True,
            'image_or_caption_ownership_inferred': False})
    for image in list(native.select('img')):
        notices.append({'kind': 'unfetched_inline_image', 'media_requested': False,
                        'image_ownership_inferred': False})
        image.decompose()
    # Comments are not visible prose. BR-delimited text (including the literal
    # reporter, source date and captions) is moved once, never summarized.
    normalized = soup.new_tag('div', attrs={'class': 'idn-native-body'})
    while native.contents:
        normalized.append(native.contents[0].extract())
    scope = soup.new_tag('div')
    scope.append(normalized)
    return {
        'headline': title, 'author': [{'name': credits[credit]}], 'datePublished': published,
        'author_attribution_detail_json': json.dumps({'native_credit': credit,
            'role': 'publisher_visible_reporter_credit',
            'reporter_real_identity_independently_verified': False,
            'article_id_filename_is_not_attribution_evidence': True}, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'published_time_basis': ('publisher_trailing_native_date_only; no_intraday_time_or_timezone_inferred; '
            if published else 'unknown_release_clock; body_acquisition_not_release_clock_or_training_acceptance; ')
            + 'header_current_calendar_and_URL_filename_not_release_evidence; not_observed_historical_PIT',
        'article_identity_basis': 'literal_HTTPS_request_response_no_redirect_or_alias; '
            'native_single_legacy_heading_matches_document_title; '
            + ('visible_reporter_and_trailing_publisher_date; ' if adapter_contract_version == 1
               else 'visible_reviewed_reporter; ')
            + ('first_native_image_and_short_precredit_line_preserved; ' if prefix_evidence else '')
            +
            'no_embedded_ID_or_original_historical_version_claimed',
        'body_completeness': 'whole_native_td_newsa_ordered_visible_prose_once; '
            'reporter_closing_date_and_text_captions_preserved; navigation_and_header_calendar_excluded; '
            'inline_media_unfetched_not_transcribed; current_source_response_not_original_historical_version',
    }, scope
