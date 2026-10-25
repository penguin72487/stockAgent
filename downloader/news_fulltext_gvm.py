"""Reviewed GVM public native articles; no HTTP, hidden text or script execution.

V1 preserves the finite pair; V2 handles known public URLs privately; V3 also
recognizes empty, unrequested native audio/video players; V4 retains written
table cells and their visible presentation wrappers; V5 also retains static
class-only inline-text wrappers at the native body root; V6 also verifies the
public periodical header against its same-page publication issue. V7 also accepts
a verified empty Firstory story player without a title attribute. V8 recognizes
written bodies without a lead, semantic table headers and an empty, unrequested
Flourish chart mount. V9 retains static editorial blocks, table footers and
presentation attributes, and recognizes witnessed empty embed variants. Native
reporter and advertorial credits remain different roles, not verified people
or ownership. Neither adapter discovers URLs or grants publication/training.
"""
from __future__ import annotations

from datetime import datetime
from itertools import chain
import json
import re
from urllib.parse import parse_qs, urlsplit

from bs4 import NavigableString, Tag

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


GVM_LAYOUT = 'gvm_public_native_article_v1'
GVM_KNOWN_URL_LAYOUT = 'gvm_public_native_article_v2'
GVM_MEDIA_LAYOUT = 'gvm_public_native_article_v3'
GVM_TABLE_LAYOUT = 'gvm_public_native_article_v4'
GVM_INLINE_LAYOUT = 'gvm_public_native_article_v5'
GVM_MAGAZINE_LAYOUT = 'gvm_public_native_article_v6'
GVM_UNTITLED_MEDIA_LAYOUT = 'gvm_public_native_article_v7'
GVM_WRITTEN_FORMAT_LAYOUT = 'gvm_public_native_article_v8'
GVM_STATIC_EDITOR_LAYOUT = 'gvm_public_native_article_v9'
GVM_PRESENTATION_LAYOUT = 'gvm_public_native_article_v10'
GVM_ORDERED_CREDIT_LAYOUT = 'gvm_public_native_article_v11'
GVM_V2_VERIFICATION = 'native_profile_header_schema_agreement_known_URL_v2_20261023'
GVM_V3_VERIFICATION = 'native_empty_Firstory_YouTube_written_prose_v3_20261024'
GVM_V4_VERIFICATION = 'native_visible_written_tables_and_captions_v4_20261024'
GVM_V5_VERIFICATION = 'native_static_inline_text_wrappers_retained_v5_20261024'
GVM_V6_VERIFICATION = 'native_public_periodical_issue_header_and_clocks_v6_20261024'
GVM_V7_VERIFICATION = 'native_empty_untitled_Firstory_story_player_v7_20261024'
GVM_V8_VERIFICATION = 'native_optional_lead_scoped_th_empty_Flourish_v8_20261024'
GVM_V9_VERIFICATION = 'native_static_blocks_table_footers_empty_embeds_v9_20261024'
GVM_V10_VERIFICATION = 'native_trailing_ASCII_title_space_special_issue_v10_20261024'
GVM_V11_VERIFICATION = 'native_two_ordered_comma_joined_profile_schema_credits_v11_20261024'
GVM_HEADER_QUESTION = 'gvm_remaining_native_header_title_credit_issue_relations_v1'
GVM_MULTIPLE_CREDIT_QUESTION = 'gvm_ordered_visible_profile_schema_two_credit_relations_v1'
GVM_CANDIDATE_LAYOUTS = {
    34:GVM_KNOWN_URL_LAYOUT,35:GVM_KNOWN_URL_LAYOUT,36:GVM_MEDIA_LAYOUT,
    37:GVM_TABLE_LAYOUT,40:GVM_WRITTEN_FORMAT_LAYOUT,41:GVM_STATIC_EDITOR_LAYOUT,42:GVM_PRESENTATION_LAYOUT,
}
# The resident ABI2 replay accepts only originals captured by this exact V8
# source. The current policy must derive that same predecessor, including all
# endpoints, pacing and authorization; a new layout alone grants no access.
GVM_V8_ORIGINAL_BINDING = (
    '4f2c6922d0eba72ca01be45f7dcefd260daf44a71cffb6a39047bbf16d0fc03c',
    '53f752b4a6a5c4b138930b9edfb2bb1738320e1d12a78db122c6cba78538b4c4',
)
GVM_V9_ORIGINAL_BINDING = (
    'aeeffbf948129581f2c65e2b479fe16a46bbd502653fd8be3ba93e1b4de68beb',
    '53f752b4a6a5c4b138930b9edfb2bb1738320e1d12a78db122c6cba78538b4c4',
)
GVM_V10_ORIGINAL_BINDING = (
    '9fdda08405ae02e1e704a59b90f441cb19f721cd332012ba3cc3a4d14330482a',
    '53f752b4a6a5c4b138930b9edfb2bb1738320e1d12a78db122c6cba78538b4c4',
)
GVM_BOUNDARY_QUESTION = 'gvm_current_native_head_credit_editor_structure_v1'
GVM_EDITOR_QUESTION = 'gvm_current_native_unrecognized_editor_nodes_v1'
GVM_REMAINING_QUESTION = 'gvm_v3_remaining_native_head_and_editor_grammar_v1'
GVM_V4_FAILURE_QUESTION = 'gvm_v4_received_native_failure_context_v1'
GVM_PATH_PATTERN = r'^/article/[1-9][0-9]*$'
GVM_BODY_SELECTOR = ':scope > .article-content.article-content_level-0'
GVM_AUTHOR_SELECTOR = ':scope > .article-head .article-head_author'
GVM_EXCLUSIONS = ('.newsletter', '.article-head_marketing', '#article_float_article',
                  '#ad_article_video', '[id^="div-gpt-ad-"]')
GVM_REVIEWED_AUTHORS = ('盧佳柔', '遠見雜誌整合傳播部企劃製作')
# Exact finite capture/auth -> the actual earlier two-original HTTP owner.
# A different URL set, policy, endpoint or pacing must not inherit calibration.
GVM_FINITE_HTTP_BINDING = (
    '5cd98e438146ec9b72d8e02b726492e977baa8266adb6b18c4d6f2dffd3fcae9',
    '27372f5fea2a6c3e76d716b0b225f8f85ce1944100dc961332b86d166a271ace',
    'd053e657612ea17db48c41971030b3554e51357bbd33b1674a7f4dca1a877a7f',
    'eddf93f47e81758f1908611db46dd82274f0542d432967665bd1840376e6661a',
)
_ORIGIN = 'https://www.gvm.com.tw'
_EXTERNAL_CREDIT = re.compile(
    r'中央社|中央通訊社|CNA|聯合新聞網|聯合報|經濟日報|工商時報|中國時報|'
    r'自由時報|鉅亨|MoneyDJ|路透|Reuters|法新社|AFP|美聯社|Associated Press', re.I)
_REPRINT = re.compile(
    r'(?:本文|本篇文章|本篇|文章|新聞稿)(?:為|由|係)?(?:轉載|轉自)|'
    r'(?:全文|原文)轉載|(?:轉載|轉自|授權轉載)(?:自|於|：|:)|'
    r'（中央社記者[^）]{1,100}）')
_AD = re.compile(r'div-gpt-ad-[0-9]{13}-[0-9]+-([1-9][0-9]*)')
_AD_SCRIPT = re.compile(r"\s*googletag[.]cmd[.]push\(function \(\) \{ googletag[.]display\('([^']+)'\); \}\);\s*")
_PROSE = frozenset({'p', 'blockquote', 'h2', 'h3', 'h4', 'ul', 'ol', 'li', 'br',
    'strong', 'b', 'em', 'i', 'u', 's', 'a', 'span', 'img', 'figcaption', 'sub', 'sup'})
_INLINE_PROSE = frozenset({'br', 'strong', 'b', 'em', 'i', 'u', 's', 'a', 'span', 'sub', 'sup'})


def _known_url_source(source, layout):
    return bool(source.provider == 'gvm' and source.allowed and source.request_enabled
        and source.embedded_article_layout == layout
        and source.domains == ('www.gvm.com.tw',) and source.known_url_only
        and source.acquisition_mode == 'continuous' and not source.finite_article_urls
        and source.authorization.get('kind') == 'personal_research'
        and source.selectors == (GVM_BODY_SELECTOR,)
        and source.author_selectors == (GVM_AUTHOR_SELECTOR,)
        and source.exclude_selectors == GVM_EXCLUSIONS
        and source.require_configured_container and source.require_author_attribution
        and not any((source.feeds, source.sitemaps, source.wordpress_apis,
            source.fulltext_feeds, source.index_pages, source.metadata_feeds,
            source.incremental_index_pages, source.reviewed_article_subsites)))


def gvm_known_url_source(source):
    """The V2 private source bound to closed questions 34/35."""
    return _known_url_source(source, GVM_KNOWN_URL_LAYOUT)


def gvm_media_source(source):
    """The V3 private source; it cannot reinterpret older observations."""
    return _known_url_source(source, GVM_MEDIA_LAYOUT)


def gvm_table_source(source):
    """The exact current V4 source for a distinct received-failure question."""
    return _known_url_source(source, GVM_TABLE_LAYOUT)


def gvm_original_source(source):
    """The reviewed V8 envelope for once-only missing-original retention."""
    return _known_url_source(source, GVM_WRITTEN_FORMAT_LAYOUT)


def gvm_static_editor_source(source):
    """Exact successor scope for locally replaying the sealed V8 originals."""
    return _known_url_source(source, GVM_STATIC_EDITOR_LAYOUT)


def gvm_presentation_source(source):
    """The same private source with evidenced title/issue presentation fixes."""
    return _known_url_source(source, GVM_PRESENTATION_LAYOUT)


def gvm_ordered_credit_source(source):
    """The same private source with the witnessed two-author header grammar."""
    return _known_url_source(source, GVM_ORDERED_CREDIT_LAYOUT)


def gvm_header_relations(soup, *, page_url, version=9):
    """Finite remaining-header evidence, never names, prose or acceptance.

    The strict V9 reader still determines its actual failure. Independent
    native header/profile/schema/clock agreement may retain a private original
    for offline repair even when its title or periodical issue declarations
    conflict. It cannot accept that conflict as an article or invent a credit.
    """
    from downloader.news_fulltext_candidates import extraction_failure_location
    if type(version) is not int or version not in {9, 10}:
        raise ValueError('unreviewed GVM header observation version')
    facts = {'contract_version':1, 'native_parser_version':version,
        'private_header_profile_schema_clocks_verified':False}
    try:
        gvm_public_native_article(soup,page_url=page_url,version=version)
    except NewsError as error:
        facts.update(current_native_error=error.code, failure_location=extraction_failure_location(error),
            gvm_received_failure_context=gvm_failure_context(error))
    else:
        facts.update(current_native_error=None, failure_location=None, gvm_received_failure_context=None)
    parts = urlsplit(page_url or '')
    if (parts.scheme!='https' or parts.netloc!='www.gvm.com.tw' or parts.query or parts.fragment
            or not re.fullmatch(GVM_PATH_PATTERN,parts.path)):
        return facts
    identifier = parts.path.rsplit('/',1)[1]
    online = soup.select('.article-left > .article-block > .article-block > .article-head')
    magazine = soup.select('main.main-wrapper > section > .container.container-article > .article-block > .article-head')
    facts.update(online_header_count=len(online),periodical_header_count=len(magazine))
    if len(online)+len(magazine)!=1: return facts
    head = (magazine or online)[0]
    bodies = head.parent.select(GVM_BODY_SELECTOR)
    facts['native_body_count'] = len(bodies)
    if len(bodies)!=1: return facts
    body = bodies[0]
    try:
        _visible(head); _visible(body)
        canonical = _one(soup,'head > link[rel=canonical]').get('href')
        og_url = _one(soup,'head > meta[property="og:url"]').get('content')
    except NewsError: return facts
    identity = canonical==og_url==page_url and head.get('aid')==identifier and head.get('id')=='article_'+identifier
    access = (head.get('status')=='不限制' and body.get('class')==['article-content','article-content_level-0']
        and (head.get('data-is-accessible') in (None,'1') if magazine else head.get('data-is-accessible')=='1'))
    if magazine:
        markers = head.parent.parent.select(':scope > div#article_end_'+identifier)
        identity = bool(identity and len(markers)==1 and markers[0].attrs=={'id':'article_end_'+identifier}
            and not markers[0].find() and not markers[0].get_text(strip=True)
            and head.parent.find_all(recursive=False)==[head,body] and head.get('article_type')=='期刊')
    else:
        markers = head.parent.parent.select(':scope > input#article_id')
        identity = bool(identity and len(markers)==1 and markers[0].get('type')=='hidden' and markers[0].get('value')==identifier)
    declarations = []
    try:
        for script in soup.select('head script[type="application/ld+json"]'):
            declarations.extend(v for v in _objects(json.loads(script.get_text())) if v.get('@type')=='NewsArticle')
    except (ValueError,TypeError,RecursionError): return facts
    facts.update(literal_native_identity_agrees=identity, native_public_access=access, schema_article_count=len(declarations))
    if len(declarations)!=1: return facts
    declared = declarations[0]
    publisher = (declared.get('publisher')=={'@id':_ORIGIN+'/#publisher'}
        and declared.get('mainEntityOfPage')=={'@type':'WebPage','@id':page_url})
    facts.update(schema_publisher_and_literal_URL_agree=publisher, schema_public_access=declared.get('isAccessibleForFree') is True)
    titles = head.select(':scope > h1.article-head_h1')
    og_titles = soup.select('head > meta[property="og:title"]')
    facts['visible_title_count'] = len(titles)
    if len(titles)==1 and len(og_titles)==1:
        text = titles[0].get_text(' ',strip=True)
        facts['title_relations'] = {'visible_matches_header':text==head.get('title'),
            'visible_matches_schema':text==declared.get('headline'),
            'visible_matches_OG':og_titles[0].get('content')==text+' | 遠見雜誌',
            'header_matches_schema':head.get('title')==declared.get('headline')}
    direct = head.select('.article-head_inner > a.article-head_author')
    anchors = head.select('a.article-head_author')
    credits = head.select('.article-head_author')
    facts.update(prior_credit_selector_count=len(direct),native_credit_anchor_count=len(anchors),
        native_credit_node_count=len(credits),native_credit_tag_counts={tag:sum(n.name==tag for n in credits) for tag in sorted({n.name for n in credits})})
    authors = declared.get('author')
    facts['schema_author_count'] = len(authors) if isinstance(authors,list) else None
    credit_agreement = False
    external = bool(_EXTERNAL_CREDIT.search(str(head.get('author',''))))
    if isinstance(authors,list): external |= any(_EXTERNAL_CREDIT.search(str(a.get('name',''))) for a in authors if isinstance(a,dict))
    if len(anchors)==1 and len(credits)==1:
        credit = anchors[0]
        name, profile = credit.get_text(' ',strip=True), credit.get('href')
        role = ('publisher_declared_periodical_author' if magazine else
            {'網路文章':'publisher_declared_reporter','廣編':'publisher_declared_advertorial'}.get(head.get('article_type')))
        route = 'blog' if role=='publisher_declared_advertorial' else 'author'
        author_id = head.get('author_id','')
        facts['single_credit_relations'] = {'name_matches_header':name==head.get('author'),
            'schema_matches_visible_profile':authors==[{'@type':'Person','name':name,'url':profile}],
            'profile_matches_native_ID':bool(re.fullmatch(r'[1-9][0-9]*',author_id)) and profile==f'{_ORIGIN}/{route}/{author_id}',
            'native_role_recognized':role is not None,
            'reviewed_advertorial_role_agrees':(role=='publisher_declared_advertorial')==(name==GVM_REVIEWED_AUTHORS[1])}
        try: _visible(credit)
        except NewsError: facts['single_credit_visible'] = False
        else: facts['single_credit_visible'] = True
        credit_agreement = bool(name and len(name)<=100 and facts['single_credit_visible']
            and all(facts['single_credit_relations'].values()))
        external |= bool(_EXTERNAL_CREDIT.search(name))
    reprint = bool(_REPRINT.search(body.get_text(' ',strip=True)))
    facts.update(external_media_credit_marker=bool(external),explicit_reprint_marker=reprint,
        single_native_profile_schema_credit_agrees=credit_agreement)
    clocks = False
    try:
        published = _clock(declared.get('datePublished'))
        modified = _clock(declared.get('dateModified'))
        modified_meta = _clock(_one(soup,'head > meta[property="article:modified_time"]').get('content'))
        visible_date = _one(head,'.article-head_box > p.article-head_grey:not(.js-article-click)')
        _visible(visible_date)
        clocks = (published==_one(soup,'head > meta[property="article:published_time"]').get('content')
            and head.get('pub_date')==published[:10] and visible_date.get_text(strip=True)==published[:10]
            and datetime.fromisoformat(modified)>=datetime.fromisoformat(published)
            and datetime.fromisoformat(modified_meta)>=datetime.fromisoformat(published))
    except NewsError: pass
    facts['native_publication_clocks_agree'] = clocks
    if magazine:
        links = head.select(':scope > a.article-head_magazine')
        issue = declared.get('isPartOf')
        facts['periodical_issue_relations'] = {'link_count':len(links),'schema_issue_is_object':isinstance(issue,dict)}
        if len(links)==1 and isinstance(issue,dict):
            link = links[0]; url = link.get('href','')
            facts['periodical_issue_relations'].update({'native_issue_URL':bool(re.fullmatch(re.escape(_ORIGIN)+r'/magazine/published/[1-9][0-9]*',url)),
                'schema_URL_matches_link':issue.get('url')==url,'schema_ID_matches_link':issue.get('@id')==url+'#issue',
                'name_matches_link_title':issue.get('name')==link.get('title'),
                'issue_number_present':bool(re.fullmatch(r'[1-9][0-9]*',str(issue.get('issueNumber',''))))})
    facts['private_header_profile_schema_clocks_verified'] = bool(identity and access and publisher
        and declared.get('isAccessibleForFree') is True and credit_agreement and clocks and not external and not reprint)
    return facts


def _ordered_credit_attribute_relation(value, values):
    """Report literal aggregate structure without returning names or IDs."""
    facts = {'ordered_pair':False,'primary_only':False,'absent':value is None}
    if not isinstance(value,str) or len(values)!=2:
        return facts
    facts['primary_only'] = value==values[0]
    if value.startswith(values[0]) and value.endswith(values[1]):
        separator=value[len(values[0]):len(value)-len(values[1])]
        if 0<len(separator)<=8 and all(c in ',，、/／|｜;；&＆ \t' for c in separator):
            facts.update(ordered_pair=True,separator_codepoints=[ord(c) for c in separator])
    return facts


def gvm_multiple_credit_relations(soup, *, page_url):
    """Ordered visible/schema profiles for the twenty witnessed dual credits.

    This is private-original evidence only. Native aggregate attributes are
    described independently, never guessed or accepted as article attribution.
    The production V10 reader remains unchanged and may still reject the body.
    """
    base=gvm_header_relations(soup,page_url=page_url,version=10)
    facts={'contract_version':1,'native_parser_version':10,'single_credit_parser_context':base,
        'ordered_visible_profile_schema_clocks_verified':False}
    if (base.get('online_header_count')!=1 or base.get('periodical_header_count')!=0
            or base.get('native_credit_anchor_count')!=2 or base.get('prior_credit_selector_count')!=2
            or base.get('native_credit_node_count')!=2 or base.get('schema_author_count')!=2):
        return facts
    head=_one(soup,'.article-left > .article-block > .article-block > .article-head')
    credits=head.select('.article-head_inner > a.article-head_author')
    declarations=[]
    for script in soup.select('head script[type="application/ld+json"]'):
        try: declarations.extend(v for v in _objects(json.loads(script.get_text())) if v.get('@type')=='NewsArticle')
        except (ValueError,TypeError,RecursionError):return facts
    if len(declarations)!=1:return facts
    names=[n.get_text(' ',strip=True) for n in credits]
    profiles=[n.get('href') for n in credits]
    identifiers=[]
    for profile in profiles:
        match=re.fullmatch(re.escape(_ORIGIN)+r'/author/([1-9][0-9]*)',profile or '')
        if match is None:return facts
        identifiers.append(match[1])
    try:
        for credit in credits:_visible(credit)
    except NewsError:return facts
    agrees=declarations[0].get('author')==[{'@type':'Person','name':name,'url':profile} for name,profile in zip(names,profiles)]
    facts.update(visible_profiles_distinct=len(set(profiles))==2,visible_names_distinct=len(set(names))==2,
        schema_profiles_match_visible_order=agrees,native_reporter_role=head.get('article_type')=='網路文章',
        native_aggregate_name_relation=_ordered_credit_attribute_relation(head.get('author'),names),
        native_aggregate_ID_relation=_ordered_credit_attribute_relation(head.get('author_id'),identifiers))
    facts['ordered_visible_profile_schema_clocks_verified']=bool(agrees and len(set(profiles))==len(set(names))==2
        and all(name and len(name)<=100 and not _EXTERNAL_CREDIT.search(name) and name!=GVM_REVIEWED_AUTHORS[1] for name in names)
        and facts['native_reporter_role'] and all(base.get(k) is True for k in (
            'literal_native_identity_agrees','native_public_access','schema_publisher_and_literal_URL_agree',
            'schema_public_access','native_publication_clocks_agree'))
        and base.get('external_media_credit_marker') is False and base.get('explicit_reprint_marker') is False)
    return facts


def gvm_boundary_metadata(soup, *, page_url, version=2):
    """Bounded native relations only; no names, prose, raw HTML or acceptance.

    One replay of the current native parser locates the actual refusing site.
    This observer cannot follow links, change heads, or widen the source scope.
    """
    from collections import Counter
    from downloader.news_fulltext_candidates import extraction_failure_location

    if type(version) is not int or version not in (2, 3):
        raise ValueError('unreviewed GVM boundary version')
    heads = soup.select('.article-left > .article-block > .article-block > .article-head')
    result = {'contract_version': 1, 'head_count': len(heads)}
    if version == 3:
        result.update(contract_version=2, native_parser_version=3)
    if len(heads) == 1:
        head = heads[0]
        bodies = head.parent.select(GVM_BODY_SELECTOR)
        credit = head.select('.article-head_inner > a.article-head_author')
        article_type = head.get('article_type', '')
        result.update(body_count=len(bodies), credit_count=len(credit),
            article_type_raw=article_type if isinstance(article_type, str) and len(article_type) <= 40 else None,
            accessible=head.get('data-is-accessible') == '1', unrestricted=head.get('status') == '不限制')
        if len(credit) == 1:
            name = credit[0].get_text(' ', strip=True)
            profile = urlsplit(credit[0].get('href', ''))
            result['credit_relations'] = {'header_name_agrees': name == head.get('author'),
                'external_media_marker': bool(_EXTERNAL_CREDIT.search(name)),
                'reviewed_advertorial_team': name == GVM_REVIEWED_AUTHORS[1],
                'profile_same_origin': profile.scheme == 'https' and profile.netloc == 'www.gvm.com.tw',
                'author_route': bool(re.fullmatch(r'/author/[1-9][0-9]*', profile.path)),
                'blog_route': bool(re.fullmatch(r'/blog/[1-9][0-9]*', profile.path))}
        if len(bodies) == 1:
            body = bodies[0]
            counts = Counter(node.name for node in body.find_all())
            unsupported = []
            for node in body.find_all():
                if node.name in _PROSE:
                    continue
                classes = [value for value in node.get('class', [])
                    if isinstance(value, str) and re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', value)]
                if len(unsupported) < 16:
                    unsupported.append({'tag': node.name, 'classes': classes[:6],
                        'parent_tag': node.parent.name, 'direct_body_child': node.parent is body,
                        'child_tags': dict(Counter(n.name for n in node.find_all(recursive=False)).most_common(12)),
                        'text_characters': len(node.get_text(' ', strip=True)),
                        'hidden': node.has_attr('hidden') or str(node.get('aria-hidden', '')).lower() == 'true'})
            result['editor'] = {'tag_counts': dict(counts.most_common(32)),
                'unsupported_nodes': unsupported, 'unsupported_truncated': sum(
                    count for tag, count in counts.items() if tag not in _PROSE) > 16,
                'explicit_reprint_marker': bool(_REPRINT.search(body.get_text(' ', strip=True))),
                'lead_count': len(body.select(':scope > blockquote.article-head_blockquote'))}
    try:
        gvm_public_native_article(soup, page_url=page_url, version=version)
    except NewsError as error:
        result.update(current_native_error=error.code, failure_location=extraction_failure_location(error))
    else:
        result.update(current_native_error=None, failure_location=None)
    return result


def _clock(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00', value):
        raise NewsError('invalid_bulk_publication_clock')
    try:
        datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    return value  # Retain declared precision/offset; never start_time or fetch time.


def _editor_exclusions(body, article_id, *, version=2):
    """Validate the same direct nonprose mounts for parsing and diagnostics."""
    charts = _empty_chart_exclusions(body, version=version) if version >= 8 else set()
    excluded = set(charts)
    if version >= 9:
        for node in body.find_all('div', id='article_further[]', recursive=False):
            if node.attrs != {'id': 'article_further[]'} or not _empty_player(node):
                raise NewsError('article_identity_not_verified')
            _visible(node)
            excluded.add(id(node))  # Empty native recommendation placeholder.
    for selector in GVM_EXCLUSIONS:
        for node in body.select(selector):
            if node.parent is not body:
                raise NewsError('article_identity_not_verified')
            identifier = node.get('id', '')
            if identifier.startswith('div-gpt-ad-'):
                if (not _AD.fullmatch(identifier) or not identifier.endswith('-' + article_id)
                        or node.get_text(strip=True)):
                    raise NewsError('article_identity_not_verified')
            elif identifier in {'article_float_article', 'ad_article_video'} and node.get_text(strip=True):
                raise NewsError('article_identity_not_verified')
            excluded.add(id(node))
    for script in body.find_all('script'):
        if id(script.parent) in charts:
            continue  # Exact empty external chart loader; never requested/executed.
        match = _AD_SCRIPT.fullmatch(script.get_text()) if not script.attrs else None
        slot = script.parent
        if (match is None or not _AD.fullmatch(match[1]) or not match[1].endswith('-' + article_id)
                or slot.parent is not body or slot.name != 'div' or slot.get('id') != match[1]
                or id(slot) not in excluded
                or any(text.strip() for text in slot.find_all(string=True) if text.parent is not script)):
            raise NewsError('article_identity_not_verified')
    return excluded


def _empty_player(node):
    # Comments, fallback prose and child elements are not an empty player.
    return all(type(child) is NavigableString and not child.strip() for child in node.children)


def _empty_chart_exclusions(body, *, version=8):
    """Only the witnessed empty Flourish mount, never fallback text or data.

    Source HTML supplies no chart observations. Preserve the complete written
    article and record this missing nontext channel; do not request the loader,
    visualization, thumbnail or another origin, or synthesize its values.
    """
    excluded = set()
    kinds = {'flourish-chart', 'flourish-table', 'flourish-cards'} if version >= 9 else {'flourish-chart'}
    for node in body.find_all('div', recursive=False):
        if (set(node.attrs) != {'class', 'data-src'}
                or node.get('class') not in [['flourish-embed', kind] for kind in kinds]
                or not re.fullmatch(r'visualisation/[1-9][0-9]*', node.get('data-src', ''))):
            continue
        children = [child for child in node.children
            if not (type(child) is NavigableString and not child.strip())]
        if len(children) != 1 or not isinstance(children[0], Tag):
            continue
        script = children[0]
        if (script.name != 'script'
                or script.attrs != {'src': 'https://public.flourish.studio/resources/embed.js'}
                or not _empty_player(script)):
            continue
        _visible(node)
        excluded.add(id(node))
    return excluded


def _editor_media_exclusions(body, *, version=3):
    """V3's observed empty player shapes, never an external media request.

    Keep wrappers containing any written/fallback content in the rejecting
    tree. In particular, a generic div/iframe is not a prose exclusion.
    """
    excluded = set()
    firstory_keys = {'src', 'title', 'frameborder', 'height', 'scrolling', 'width', 'style'}
    youtube_keys = {'src', 'allowfullscreen', 'alt', 'class', 'frameborder'}
    for node in body.find_all(recursive=False):
        if node.name == 'iframe' and _empty_player(node):
            try:
                parts = urlsplit(node.get('src', ''))
            except ValueError:
                continue
            if (set(node.attrs) <= firstory_keys
                    and (node.get('title') == 'Firstory' or version >= 7 and 'title' not in node.attrs)
                    and parts.scheme == 'https' and parts.netloc in {'open.firstory.me', 'open.firstory.fm'}
                    and re.fullmatch(r'/embed/story/[a-z0-9]+', parts.path)
                    and not parts.query and not parts.fragment):
                _visible(node)
                excluded.add(id(node))
            elif version >= 9 and _empty_facebook_post(node, parts):
                _visible(node)
                excluded.add(id(node))
        elif node.name == 'div' and set(node.attrs) == {'class'}:
            children = [child for child in node.children
                if not (type(child) is NavigableString and not child.strip())]
            if len(children) != 1 or getattr(children[0], 'name', None) != 'iframe':
                continue
            frame = children[0]
            try:
                parts = urlsplit(frame.get('src', ''))
            except ValueError:
                continue
            if (_empty_player(frame) and set(frame.attrs) <= youtube_keys
                    and (parts.scheme == 'https' or version >= 9 and not parts.scheme
                         and frame.get('src', '').startswith('//'))
                    and parts.netloc == 'www.youtube.com'
                    and re.fullmatch(r'/embed/[a-zA-Z0-9_-]{1,128}', parts.path)
                    and not parts.fragment):
                # Query parameters remain opaque and unrequested. No player,
                # transcript, script, API or alternate article is fetched.
                _visible(node)
                _visible(frame)
                excluded.add(id(node))
    return excluded


def _empty_facebook_post(node, parts):
    """One witnessed empty post player; no social post or transcript request."""
    keys = {'src', 'width', 'height', 'style', 'scrolling', 'frameborder', 'allowfullscreen', 'allow'}
    if (set(node.attrs) != keys or parts.scheme != 'https' or parts.netloc != 'www.facebook.com'
            or parts.path != '/plugins/post.php' or parts.fragment
            or node.get('style') != 'border:none;overflow:hidden' or node.get('scrolling') != 'no'
            or node.get('frameborder') != '0' or node.get('allowfullscreen') != 'true'
            or node.get('allow') != 'autoplay; clipboard-write; encrypted-media; picture-in-picture; web-share'
            or not re.fullmatch(r'[1-9][0-9]{0,3}%?', node.get('width', ''))
            or not re.fullmatch(r'[1-9][0-9]{0,3}', node.get('height', ''))):
        return False
    query = parse_qs(parts.query, keep_blank_values=True)
    if (set(query) != {'href', 'show_text', 'width'} or any(len(v) != 1 for v in query.values())
            or query['show_text'] != ['true'] or not re.fullmatch(r'[1-9][0-9]{0,3}', query['width'][0])):
        return False
    try:
        post = urlsplit(query['href'][0])
    except ValueError:
        return False
    return bool(post.scheme == 'https' and post.netloc == 'www.facebook.com'
        and re.fullmatch(r'/[a-zA-Z0-9._-]+/posts/[a-zA-Z0-9]+', post.path)
        and not post.query and not post.fragment)


_STATIC_STYLE_PROPERTIES = frozenset({
    'background', 'background-color', 'border', 'border-top', 'border-bottom', 'border-left', 'border-right',
    'border-radius', 'box-shadow', 'box-sizing', 'color', 'display', 'flex', 'flex-wrap', 'align-items',
    'justify-content', 'gap', 'font-family', 'font-size', 'font-weight', 'letter-spacing', 'line-height',
    'margin', 'margin-top', 'margin-bottom', 'margin-left', 'margin-right',
    'padding', 'padding-left', 'padding-right', 'padding-top', 'padding-bottom',
    'width', 'max-width', 'min-width', 'overflow', 'overflow-x', 'overflow-wrap', '-webkit-overflow-scrolling',
    'position', 'top', 'left', 'right', 'bottom', 'text-align', 'white-space', 'word-break', 'word-wrap',
})


def _static_presentation(node):
    """Nonexecuting presentation only; preserve all descendant writing.

    This is a closed HTML/CSS vocabulary, not a browser visibility oracle.
    Reject loaders, event attributes, CSS escapes, hidden/offscreen styles and
    unknown properties rather than skipping a wrapper's contents.
    """
    if set(node.attrs) - {'class', 'style'}:
        return False
    if any(not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', value) for value in node.get('class', [])):
        return False
    style = node.get('style', '')
    if not isinstance(style, str) or len(style) > 4096:
        return False
    for declaration in style.split(';'):
        if not declaration.strip():
            continue
        key, separator, value = declaration.partition(':')
        key, value = key.strip().lower(), value.strip().lower()
        if (not separator or key not in _STATIC_STYLE_PROPERTIES or not value
                or not re.fullmatch(r"[a-z0-9#.,%() '!-]+", value)
                or re.search(r'url|expression|var\s*\(|calc\s*\(|(?:^|[ (])-\d', value)):
            return False
        if key == 'display' and value not in {'block', 'inline', 'inline-block', 'flex'}:
            return False
        if key == 'position' and value not in {'relative', 'absolute'}:
            return False
        if key in {'width', 'max-width', 'font-size', 'line-height'} and re.fullmatch(r'0(?:[a-z%]+)?', value):
            return False
    _visible(node)
    return True


def _static_written_nodes(body):
    """Rooted static blocks; no new node here is a text exclusion."""
    allowed = set()
    for root in body.find_all(['div', 'section'], recursive=False):
        if root.name == 'div':
            if not root.has_attr('style') or not _static_presentation(root):
                continue
        elif root.attrs:
            keys = {'data-testid', 'data-turn', 'data-turn-id', 'data-turn-id-container', 'dir'}
            identifier = root.get('data-turn-id', '')
            if (set(root.attrs) != keys or root.get('dir') != 'auto' or root.get('data-turn') != 'assistant'
                    or not re.fullmatch(r'conversation-turn-[0-9]+', root.get('data-testid', ''))
                    or not re.fullmatch(r'request-WEB:[0-9a-f-]{36}-[0-9]+', identifier)
                    or root.get('data-turn-id-container') != identifier):
                continue
            # Source-provided editor markup, not an instruction, author or
            # model provenance assertion. Retain its actual written text.
        allowed.add(id(root))
        for node in root.find_all('div'):
            if _static_presentation(node) or _empty_timeline_decoration(node):
                allowed.add(id(node))
    return allowed


def _empty_timeline_decoration(node):
    """The witnessed empty timeline dot, never offscreen prose."""
    if set(node.attrs) != {'style'} or node.get_text(strip=True):
        return False
    children = [child for child in node.children
        if not (type(child) is NavigableString and not child.strip())]
    if len(children) != 1 or not isinstance(children[0], Tag) or str(children[0]) != '<br/>':
        return False
    if not re.fullmatch(
            r'position:absolute;left:-31px;top:(?:20|22)px;width:15px;height:15px;'
            r'border-radius:50%;background:#(?:2563eb|facc15);border:4px solid #(?:dbeafe|fef3c7);',
            node['style']):
        return False
    _visible(node)
    return True


def _written_table_nodes(body, *, version=4, static_blocks=frozenset()):
    """Recognize visible static table structure without excluding its text.

    Only tables at the body root or in presentation-only div ancestors anchor
    this grammar. Their prose/captions are still visited by the normal text
    reader. A div elsewhere, nested table, loader or unknown node still fails.
    """
    allowed, roots, tables = set(), {}, set()
    presentation = {'class', 'style'}
    table_keys = presentation | ({'border', 'cellpadding', 'cellspacing', 'width', 'dir',
        'data-sheets-baot', 'data-sheets-root', 'data-path-to-node'} if version >= 9 else set())
    for table in body.find_all('table'):
        if (set(table.attrs) - table_keys or table.find('table') is not None
                or table.find('tr') is None or table.find(['td', 'th']) is None):
            continue
        if version >= 9:
            numeric = ('border', 'cellpadding', 'cellspacing', 'data-path-to-node')
            if (any(not re.fullmatch(r'[0-9]{1,4}', table[key]) for key in numeric if key in table.attrs)
                    or 'width' in table.attrs and not re.fullmatch(r'[1-9][0-9]{0,3}%?', table['width'])
                    or 'dir' in table.attrs and table['dir'] != 'ltr'
                    or any(table.get(key, '1') != '1' for key in ('data-sheets-baot', 'data-sheets-root'))):
                continue
        root, parent = table, table.parent
        while parent is not body and ((parent.name == 'div' and not (set(parent.attrs) - presentation))
                or version >= 9 and id(parent) in static_blocks):
            root, parent = parent, parent.parent
        if parent is body:
            roots[id(root)] = root
            tables.add(id(table))
    parents = {'thead': {'table'}, 'tbody': {'table'}, 'tr': {'thead', 'tbody', 'table'},
        'td': {'tr'}, 'th': {'tr'}}
    if version >= 9:
        parents['tfoot'] = {'table'}
        parents['tr'].add('tfoot')
    for root in roots.values():
        for node in chain((root,), root.find_all()):
            if node.name == 'div' and not (set(node.attrs) - presentation):
                allowed.add(id(node))
            elif node.name == 'table' and id(node) in tables:
                allowed.add(id(node))
            elif node.name in parents and node.parent.name in parents[node.name]:
                table = node.find_parent('table')
                keys = presentation | ({'colspan', 'rowspan'} if node.name in {'td', 'th'} else set())
                if version >= 9:
                    if node.name in {'tr', 'td', 'th'}:
                        keys.add('height')
                    if node.name in {'td', 'th'}:
                        keys.update({'width', 'align'})
                if version >= 11 and node.name == 'td' and node.get('valign') == 'top':
                    # The remaining dual-credit original uses this literal
                    # presentation attribute. Retain every cell's text; an
                    # unknown value, hidden cell or event handler still fails.
                    keys.add('valign')
                if version >= 8 and node.name == 'th' and node.get('scope') in {'row', 'col', 'rowgroup', 'colgroup'}:
                    keys.add('scope')  # Header relationship, not a loader or omitted cell.
                if table is None or id(table) not in tables or set(node.attrs) - keys:
                    continue
                if version >= 9 and (any(not re.fullmatch(r'[1-9][0-9]{0,3}', node[key])
                        for key in ('height', 'width') if key in node.attrs)
                        or 'align' in node.attrs and node['align'] not in {'left', 'center', 'right'}):
                    continue
                spans = [str(node[key]) for key in ('colspan', 'rowspan') if key in node.attrs]
                if all(re.fullmatch(r'[1-9][0-9]{0,2}', value) for value in spans):
                    allowed.add(id(node))
    return allowed


def _written_inline_wrapper(node, body):
    """Retain the observed text/link wrappers, never discard unknown prose.

    Only a direct, class-only div containing nonempty static inline writing
    qualifies. Nested blocks, media, loaders, event handlers and hidden content
    stay in the refusing tree. This recognizes HTML, without executing it or
    inferring author, access, clocks or a complete article from a short wrapper.
    """
    if (node.name != 'div' or node.parent is not body or set(node.attrs) != {'class'}
            or not node.get('class') or not node.get_text(strip=True)):
        return False
    allowed_attributes = {'class', 'href', 'target', 'rel', 'title', 'style'}
    if any(child.name not in _INLINE_PROSE or set(child.attrs) - allowed_attributes for child in node.find_all()):
        return False
    _visible(node)
    return True


def _unsupported_editor_nodes(body, article_id, *, version=2):
    """Yield only actually unsupported nodes, after native widget exclusions.

    Production stops at the first yielded node. The bounded observer may look
    farther, but cannot turn this iterator into an accepted article.
    """
    excluded = _editor_exclusions(body, article_id, version=version)
    if version >= 3:
        excluded.update(_editor_media_exclusions(body, version=version))
    static_blocks = _static_written_nodes(body) if version >= 9 else set()
    written_tables = _written_table_nodes(body, version=version, static_blocks=static_blocks) if version >= 4 else set()
    for node in body.find_all():
        if id(node) in excluded or any(id(parent) in excluded for parent in node.parents):
            continue
        if node.name == 'input' and node.parent is body:
            if (node.get('type') == 'hidden' and node.get('id') == 'tracker[]'
                    and node.get('key') == 'banners' and str(node.get('value', '')).isdigit()):
                continue  # Native ad telemetry, not an observation or written text.
        if version < 3 and node.name == 'iframe':
            parts = urlsplit(node.get('src', ''))
            if (node.parent is body and node.get('title') == 'Firstory'
                    and parts.scheme == 'https' and parts.netloc == 'open.firstory.me'
                    and re.fullmatch(r'/embed/story/[a-z0-9]+', parts.path)
                    and not parts.query and not parts.fragment and not node.get_text(strip=True)):
                _visible(node)
                continue  # Unrequested external audio is not captured prose.
        if (node.name not in _PROSE and id(node) not in written_tables and id(node) not in static_blocks
                and not (version >= 5 and _written_inline_wrapper(node, body))):
            yield node
        else:
            _visible(node)


def _body_structure(body, article_id, *, version=2):
    """Check the native written tree; no loader execution or media fetch."""
    unsupported = next(_unsupported_editor_nodes(body, article_id, version=version), None)
    if unsupported is not None:
        raise NewsError('article_identity_not_verified')
    leads = body.select(':scope > blockquote.article-head_blockquote')
    if version < 8 or leads:
        lead = _one(body, ':scope > blockquote.article-head_blockquote')
        _visible(lead)
    paragraphs = body.find_all('p', recursive=False)
    if not paragraphs or sum(len(n.get_text(' ', strip=True)) for n in paragraphs) < 100:
        raise NewsError('body_not_found')  # A lead/summary alone is never complete.
    return not leads


def gvm_editor_grammar_metadata(soup, *, page_url, version=2):
    """Explain the current parser's real rejecting nodes, without any prose.

    ABI34 counted all nonprose tags, including valid excluded widgets. This is
    a distinct once-only question using the shared classifier. Literal URLs,
    attribute values, names and source text are never returned.
    """
    from itertools import islice
    from downloader.news_fulltext_candidates import extraction_failure_location

    if type(version) is not int or version not in (2, 3):
        raise ValueError('unreviewed GVM editor metadata version')
    result = {'contract_version': 1, 'observed': False, 'accepted_as_article': False}
    if version == 3:
        result.update(contract_version=2, native_parser_version=3)
    try:
        gvm_public_native_article(soup, page_url=page_url, version=version)
    except NewsError as error:
        location = extraction_failure_location(error)
        result.update(current_native_error=error.code, failure_location=location)
        if not any(site['module']=='news_fulltext_gvm.py' and site['function']=='_body_structure'
                   for site in location['sites']):
            return result  # Do not inspect a body that failed the prior identity/access/credit gates.
    else:
        result.update(current_native_error=None, failure_location=None)
        return result
    body = _one(_one(soup, '.article-left > .article-block > .article-block > .article-head').parent,
                GVM_BODY_SELECTOR)
    try:
        nodes = list(islice(_unsupported_editor_nodes(body, page_url.rsplit('/',1)[1], version=version), 17))
    except NewsError as error:
        result['classification_error'] = error.code
        return result
    facts = [_editor_node_metadata(node, body) for node in nodes[:16]]
    result.update(observed=True,unsupported_nodes=facts,unsupported_truncated=len(nodes)>16,
                  classifier_shared_with_production=True,raw_or_names_or_body_retained=False)
    return result


def _editor_node_metadata(node, body):
    """Bounded structure only; shared with the frozen ABI35/36 observers."""
    from collections import Counter

    keys = [key for key in node.attrs if isinstance(key, str) and re.fullmatch(r'[a-zA-Z0-9:_-]{1,64}', key)]
    value = {'tag': node.name, 'direct_body_child': node.parent is body,
        'attribute_keys': sorted(keys)[:16], 'attribute_keys_truncated': len(keys) > 16,
        'child_tags': dict(Counter(n.name for n in node.find_all(recursive=False)).most_common(16)),
        'text_characters': len(node.get_text(' ', strip=True)),
        'empty_static_leaf': not node.find() and not node.get_text(strip=True)}
    try:
        _visible(node)
    except NewsError:
        value['visible_under_current_rules'] = False
    else:
        value['visible_under_current_rules'] = True
    if node.name == 'iframe':
        try:
            parts = urlsplit(node.get('src', ''))
            host = parts.hostname or ''
            origin = (parts.scheme + '://' + host if parts.scheme == 'https'
                and re.fullmatch(r'[a-z0-9.-]{1,253}', host) and '.' in host
                and not parts.username and not parts.password and parts.port in (None, 443) else None)
            value.update(src_https_origin=origin, src_has_query=bool(parts.query),
                src_embed_path=bool(re.fullmatch(r'/embed/[a-zA-Z0-9_-]+', parts.path)),
                src_firstory_story=bool(re.fullmatch(r'/embed/story/[a-z0-9]+', parts.path)),
                declared_firstory=node.get('title') == 'Firstory')
        except ValueError:
            value['invalid_src'] = True
    return value


def gvm_failure_context(error):
    """Observe the actual failed parser frame without replay, HTTP or raw text.

    The first refusing node is already computed by production. Earlier failures
    never trigger body classification; absence of a body frame means only that
    the earlier gates did not all finish, not that body data is absent.
    """
    result = {'contract_version': 1, 'native_frame_observed': False,
        'prior_header_identity_access_credit_clocks_passed': False,
        'first_refusing_editor_node': None, 'current_native_error': error.code,
        'parser_replayed': False, 'additional_requests': 0,
        'raw_or_names_or_body_retained': False, 'accepted_as_article': False}
    frame, examined = error.__traceback__, 0
    while frame is not None and examined < 32:
        code, values = frame.tb_frame.f_code, frame.tb_frame.f_locals
        if code is gvm_public_native_article.__code__:
            result['native_frame_observed'] = True
            result['native_parser_version'] = values.get('version')
            head, body = values.get('head'), values.get('body')
            result.update(native_head_found=isinstance(head, Tag), native_body_found=isinstance(body, Tag))
            if isinstance(head, Tag):
                result.update(native_accessible=head.get('data-is-accessible') == '1',
                    native_unrestricted=head.get('status') == '不限制')
        elif code is _body_structure.__code__:
            result['prior_header_identity_access_credit_clocks_passed'] = True
            node, body = values.get('unsupported'), values.get('body')
            if isinstance(node, Tag) and isinstance(body, Tag):
                result['first_refusing_editor_node'] = _editor_node_metadata(node, body)
        frame = frame.tb_next
        examined += 1
    return result


def _magazine_issue(head, declared, *, version=6):
    """Same-page periodical identity, not a guessed legacy-page fallback."""
    link = _one(head, ':scope > a.article-head_magazine')
    _visible(link)
    issue = declared.get('isPartOf')
    if not isinstance(issue, dict):
        raise NewsError('article_identity_not_verified')
    url = link.get('href', '')
    periodical = {'@type': 'Periodical', '@id': _ORIGIN + '/magazine#periodical',
        'name': '遠見雜誌', 'url': _ORIGIN + '/magazine',
        'publisher': {'@id': _ORIGIN + '/#publisher'}}
    special = bool(version >= 10 and re.fullmatch(re.escape(_ORIGIN) + r'/magazine/special/[1-9][0-9]*', url))
    if (not (special or re.fullmatch(re.escape(_ORIGIN) + r'/magazine/published/[1-9][0-9]*', url))
            or issue.get('@type') != 'PublicationIssue' or issue.get('@id') != url + '#issue'
            or issue.get('url') != url or issue.get('isPartOf') != periodical
            or not isinstance(issue.get('name'), str) or not issue['name']
            or link.get('title') != issue['name']
            or (issue.get('issueNumber') is not None if special
                else not re.fullmatch(r'[1-9][0-9]*', str(issue.get('issueNumber', ''))))):
        raise NewsError('article_identity_not_verified')
    result = {'source_issue_url': url, 'source_issue_number': issue.get('issueNumber'),
        'source_issue_title': issue['name'], 'source_visible_issue_label': link.get_text(' ', strip=True),
        'source_issue_alternate_name': issue.get('alternateName'), 'issue_requested': False}
    if special:
        result.update(source_issue_kind='special', issue_number_not_declared=True,
            native_special_ID_is_not_issue_number=True)
    return result


def _two_native_credits(head, declared):
    """Two visible native profiles, exact comma attributes and ordered schema.

    Evidence42 witnessed twenty online headers with this literal grammar.
    Do not split a byline, infer another author, or relax periodical/advertorial
    scope. Unknown aggregate separators and extra credit nodes still refuse.
    """
    credits = head.select('.article-head_inner > a.article-head_author')
    if (head.get('article_type') != '網路文章' or len(credits) != 2
            or len(head.select('.article-head_author')) != 2):
        raise NewsError('article_rights_or_scope_excluded')
    names, profiles, identifiers = [], [], []
    for credit in credits:
        _visible(credit)
        name, profile = credit.get_text(' ', strip=True), credit.get('href')
        match = re.fullmatch(re.escape(_ORIGIN) + r'/author/([1-9][0-9]*)', profile or '')
        if (not name or len(name) > 100 or _EXTERNAL_CREDIT.search(name)
                or name == GVM_REVIEWED_AUTHORS[1] or match is None):
            raise NewsError('article_rights_or_scope_excluded')
        names.append(name); profiles.append(profile); identifiers.append(match[1])
    if (len(set(names)) != 2 or len(set(profiles)) != 2
            or head.get('author') != ','.join(names) or head.get('author_id') != ','.join(identifiers)
            or declared.get('author') != [{'@type': 'Person', 'name': name, 'url': profile}
                for name, profile in zip(names, profiles)]):
        raise NewsError('article_rights_or_scope_excluded')
    return names, profiles


def gvm_public_native_article(soup, *, page_url, version=1):
    if version not in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11):
        raise ValueError('unreviewed GVM native version')
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'www.gvm.com.tw'
            or parts.query or parts.fragment or not re.fullmatch(GVM_PATH_PATTERN, parts.path)):
        raise NewsError('article_identity_not_verified')
    article_id = parts.path.rsplit('/', 1)[1]
    if (_one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url):
        raise NewsError('article_identity_not_verified')
    online_heads = soup.select('.article-left > .article-block > .article-block > .article-head')
    magazine_heads = (soup.select('main.main-wrapper > section > .container.container-article > .article-block > .article-head')
        if version >= 6 else [])
    magazine = bool(magazine_heads)
    if len(online_heads) + len(magazine_heads) != 1:
        raise NewsError('article_identity_not_verified')
    head = (magazine_heads if magazine else online_heads)[0]
    scope = head.parent
    body = _one(scope, GVM_BODY_SELECTOR)
    _visible(head)
    _visible(body)
    if magazine:
        marker = _one(scope.parent, ':scope > div#article_end_' + article_id)
        if (marker.attrs != {'id': 'article_end_' + article_id} or marker.find() or marker.get_text(strip=True)
                or scope.find_all(recursive=False) != [head, body]
                or any(type(node) is NavigableString and str(node).strip() for node in scope.children)
                or head.get('article_type') != '期刊'):
            raise NewsError('article_identity_not_verified')
    else:
        marker = _one(scope.parent, ':scope > input#article_id')
        if marker.get('type') != 'hidden' or marker.get('value') != article_id:
            raise NewsError('article_identity_not_verified')
    if head.get('aid') != article_id or head.get('id') != 'article_' + article_id:
        raise NewsError('article_identity_not_verified')
    # The witnessed periodical header omits the online-only attribute. Its
    # unrestricted status, level-0 body and free-access schema must all agree;
    # an explicit denial still wins, even on an otherwise matching template.
    if ((head.get('data-is-accessible') not in (None, '1') if magazine else head.get('data-is-accessible') != '1')
            or head.get('status') != '不限制'
            or body.get('class') != ['article-content', 'article-content_level-0']):
        raise NewsError('paywall_or_login')
    articles = []
    for script in soup.select('head script[type="application/ld+json"]'):
        try:
            articles.extend(value for value in _objects(json.loads(script.get_text()))
                            if value.get('@type') == 'NewsArticle')
        except (ValueError, TypeError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
    if len(articles) != 1:
        raise NewsError('article_identity_not_verified')
    declared = articles[0]
    if declared.get('isAccessibleForFree') is not True:
        raise NewsError('paywall_or_login')
    if (declared.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or declared.get('publisher') != {'@id': _ORIGIN + '/#publisher'}):
        raise NewsError('article_identity_not_verified')
    issue = _magazine_issue(head, declared, version=version) if magazine else None
    title = _one(head, ':scope > h1.article-head_h1')
    _visible(title)
    headline = title.get_text(' ', strip=True)
    native_title, schema_title = head.get('title'), declared.get('headline')
    title_presentation = bool(version >= 10 and isinstance(native_title, str) and native_title == schema_title
        and native_title != headline and native_title.rstrip(' ') == headline)
    if (not headline or (headline != native_title or headline != schema_title) and not title_presentation
            or _one(soup, 'head > meta[property="og:title"]').get('content') != headline + ' | 遠見雜誌'):
        raise NewsError('article_identity_not_verified')
    role = ('publisher_declared_periodical_author' if magazine else
        {'網路文章': 'publisher_declared_reporter', '廣編': 'publisher_declared_advertorial'}.get(head.get('article_type')))
    multiple = bool(version >= 11 and not magazine and len(head.select('.article-head_inner > a.article-head_author')) == 2)
    if multiple:
        names, profiles = _two_native_credits(head, declared)
        name, profile = head['author'], None
    else:
        credit = _one(head, '.article-head_inner > a.article-head_author')
        _visible(credit)
        name, profile = credit.get_text(' ', strip=True), credit.get('href')
        route = 'blog' if role == 'publisher_declared_advertorial' else 'author'
        author_id = head.get('author_id', '')
        if (role is None or not name or len(name) > 100 or head.get('author') != name
                or not re.fullmatch(r'[1-9][0-9]*', author_id) or profile != f'{_ORIGIN}/{route}/{author_id}'
                or declared.get('author') != [{'@type': 'Person', 'name': name, 'url': profile}]):
            raise NewsError('article_rights_or_scope_excluded')
        names = [name]
    if version == 1:
        if (name not in GVM_REVIEWED_AUTHORS
                or (role == 'publisher_declared_advertorial') != (name == GVM_REVIEWED_AUTHORS[1])):
            raise NewsError('article_rights_or_scope_excluded')
    elif (_EXTERNAL_CREDIT.search(name) or _REPRINT.search(body.get_text(' ', strip=True))
            or (role == 'publisher_declared_advertorial') != (name == GVM_REVIEWED_AUTHORS[1])):
        # Native profile/ID/header/schema agreement establishes attribution,
        # not employment or ownership. Keep declared reposts and guest sponsors
        # outside this publisher-original private-reading scope, including CNA.
        raise NewsError('article_rights_or_scope_excluded')
    published = _clock(declared.get('datePublished'))
    modified_schema = _clock(declared.get('dateModified'))
    modified_meta = _clock(_one(soup, 'head > meta[property="article:modified_time"]').get('content'))
    visible_date = _one(head, '.article-head_box > p.article-head_grey:not(.js-article-click)')
    _visible(visible_date)
    if (published != _one(soup, 'head > meta[property="article:published_time"]').get('content')
            or head.get('pub_date') != published[:10] or visible_date.get_text(strip=True) != published[:10]
            or datetime.fromisoformat(modified_meta) < datetime.fromisoformat(published)
            or datetime.fromisoformat(modified_schema) < datetime.fromisoformat(published)):
        raise NewsError('invalid_bulk_publication_clock')
    summary_absent = _body_structure(body, article_id, version=version)
    detail = {'contract_version': min(version, 6) if magazine else min(version, 5), 'source_visible_credit': name, 'literal_profile_url': profile,
        'publisher_declared_article_type': head['article_type'], 'source_credit_role': role,
        'schema_author_type_raw': 'Person', 'human_reporter_identity_verified': False,
        'original_ownership_verified': False, 'sponsored_content_is_independent_editorial': False}
    if multiple:
        detail = {k: v for k, v in detail.items() if k not in {'source_visible_credit', 'literal_profile_url'}}
        detail.update(contract_version=7, source_visible_credits=names, literal_profile_urls=profiles,
            source_native_author_raw=head['author'], source_native_author_ID_raw=head['author_id'],
            ordered_visible_native_and_schema_credits_agree=True)
    notices = {'contract_version': 1, 'source_publication_meta_raw': published,
        'source_schema_modified_raw': modified_schema, 'source_og_modified_raw': modified_meta,
        'modified_declarations_agree': modified_schema == modified_meta,
        'visible_publication_date_raw': visible_date.get_text(strip=True),
        'native_start_time_is_publication': False,
        'media_or_scripts_requested_or_executed': False, 'historical_first_release_verified': False}
    if title_presentation:
        notices.update(native_title_trailing_ASCII_space_presentation=True,
            source_native_title_raw=native_title, source_schema_headline_raw=schema_title,
            source_visible_title_raw=headline, semantic_title_conflict_accepted=False)
    if multiple:
        notices['native_two_ordered_comma_joined_credit_header_schema_verified'] = True
    if version >= 3:
        notices.update(contract_version=2, native_editor_media_grammar_version=3)
    if version >= 4:
        notices.update(contract_version=3, native_editor_table_prose_grammar_version=1)
    if version >= 5:
        notices.update(contract_version=4, native_editor_inline_prose_grammar_version=1)
    if magazine:
        notices.update(contract_version=5, native_public_periodical_layout=True,
            source_declared_periodical_issue=issue, native_online_access_attribute_absent=head.get('data-is-accessible') is None,
            metadata_midnight_is_verified_first_release_time=False)
    if version >= 7 and any(isinstance(node, Tag) and node.name == 'iframe' and 'title' not in node.attrs
            for node in body.children):
        # The prior whole-tree check already proved an empty, visible,
        # allowlisted Firstory story player. This records absence, not a
        # guessed title, audio transcript or another source request.
        notices['native_empty_untitled_firstory_story_player_verified'] = True
    empty_chart = bool(version >= 8 and body.select(':scope > div.flourish-chart[data-src]'))
    if version >= 8:
        if summary_absent:
            notices['native_full_written_body_has_no_summary'] = True
        if body.find('th', scope=True) is not None:
            notices['native_visible_table_header_scope_verified'] = True
        if empty_chart:
            notices.update(native_empty_flourish_chart_loader_verified=True,
                dynamic_chart_values_captured=False)
    extra_embeds = bool(version >= 9 and body.select(':scope > div.flourish-table, :scope > div.flourish-cards'))
    facebook_post = bool(version >= 9 and body.select(':scope > iframe[src^="https://www.facebook.com/plugins/post.php?"]'))
    if version >= 9:
        if extra_embeds:
            notices.update(native_empty_flourish_table_or_cards_loader_verified=True,
                dynamic_embed_values_captured=False)
        if facebook_post:
            notices.update(native_empty_facebook_post_player_verified=True, external_social_post_text_captured=False)
        if body.find('tfoot') is not None:
            notices['native_visible_table_footer_text_retained'] = True
        if any(node.get('src', '').startswith('//') for node in body.find_all('iframe')):
            notices['native_empty_protocol_relative_youtube_player_verified'] = True
    if version >= 11 and body.find('td', valign='top') is not None:
        notices['native_visible_table_cell_top_alignment_verified'] = True
    return {'headline': headline, 'author': [{'name': name} for name in names], 'datePublished': published,
        'dateModified': modified_meta,
        'published_time_basis': 'publisher_declared_metadata_seconds_and_offset; visible_date_only; no_historical_first_release_proof',
        'article_identity_basis': f'literal_canonical_og_URL_native_ID_header_schema_credit_public_level0_v{version}',
        'author_attribution_detail_json': json.dumps(detail, ensure_ascii=False, sort_keys=True),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False, sort_keys=True),
        'body_completeness': ('whole_public_native_editor_written_text_with_visible_captions; summary_not_promoted; newsletter_ads_excluded; audio_images_not_transcribed'
            + ('; empty_Flourish_chart_values_not_captured' if empty_chart else '')
            + ('; empty_Flourish_table_or_cards_values_not_captured' if extra_embeds else '')
            + ('; external_social_post_text_not_captured' if facebook_post else ''))}, scope
