"""iNEWS native microdata contracts; synthetic text, no downloaded news fixture."""
from copy import deepcopy
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_setn import (
    INEWS_AUTHOR_SELECTOR, INEWS_V4_AUTHOR_SELECTOR, INEWS_BODY_SELECTOR, INEWS_V8_BODY_SELECTOR,
    INEWS_LAYOUT, INEWS_LAYOUT_VERSIONS, INEWS_PROVIDER,
    INEWS_TITLE_SELECTOR, SETN_PATH_PATTERN, inews_boundary_metadata, inews_validation_context,
)

URL = 'https://inews.setn.com/news/123456'
TITLE = '合成iNEWS測試'
NAME = '合成記者'
CLOCK = '2020/03/02 09:01'
CAPTION = '合成圖說（圖／測試機構提供）'
TEXT = '自行編寫的測試段落：交易量1234股、價格12.34、負值-5、比率2.3%，全部保留原值。' * 5


def entry():
    return {'id':INEWS_PROVIDER,'domains':['inews.setn.com'],'enabled':True,
        'acquisition_mode':'continuous','private_known_url_scope':'personal_noncommercial_originals_v1',
        'html_parser':'lxml','interval_seconds':10,'parallelism':1,
        'embedded_article_layout':INEWS_LAYOUT,'article_path_pattern':SETN_PATH_PATTERN,
        'article_selectors':[INEWS_BODY_SELECTOR],'title_selector':INEWS_TITLE_SELECTOR,
        'require_configured_container':True,'require_author_attribution':True,
        'author_selectors':[INEWS_AUTHOR_SELECTOR],
        'authorization':{'kind':'personal_research','evidence_url':'https://inews.setn.com/',
            'additional_evidence_urls':['https://blogs.iset.com.tw/setn/archives/9'],
            'reviewed_on':'2026-10-23','scope':'Private known public native originals; not an archive or reuse licence.',
            'automated_access_review':'no_explicit_prohibition_identified'}}


def policy(value=None):
    return PolicyRegistry({'contract_version':1,'collection_purpose':'personal_research',
        'publish':False,'remote_training_export':False,'providers':[value or entry()]},
        base=Path('.')).sources[INEWS_PROVIDER]


def v2_entry():
    value = entry(); value['embedded_article_layout'] = 'setn_inews_native_article_v2'
    return value


def extract_v2(soup=None):
    return extract_article(str(soup or page()).encode(),policy(v2_entry()),page_url=URL)


def v3_entry():
    value = entry(); value['embedded_article_layout'] = 'setn_inews_native_article_v3'
    return value


def v4_entry():
    value = entry(); value['embedded_article_layout'] = 'setn_inews_native_article_v4'
    value['author_selectors'] = [INEWS_V4_AUTHOR_SELECTOR]
    return value


def v5_entry():
    value=v4_entry(); value['embedded_article_layout']='setn_inews_native_article_v5'
    return value


def v6_entry():
    value=v5_entry(); value['embedded_article_layout']='setn_inews_native_article_v6'
    return value


def v7_entry():
    value=v6_entry(); value['embedded_article_layout']='setn_inews_native_article_v7'
    return value


def v8_entry():
    value=v7_entry(); value['embedded_article_layout']='setn_inews_native_article_v8'
    value['article_selectors']=[INEWS_V8_BODY_SELECTOR]
    return value


def extract_v3(soup=None):
    return extract_article(str(soup or page()).encode(),policy(v3_entry()),page_url=URL)


def page_with_outline(monkeypatch):
    import hashlib
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_OUTLINE_LABEL_SHA256',
        hashlib.sha256('合成本文目錄'.encode()).hexdigest())
    soup = page(); body = soup.select_one('[itemprop=articleBody]')
    outline = BeautifulSoup('<div id="nav-directory"><p><strong>合成本文目錄</strong></p><ul>'
        '<li><a href="#nav-title-338">合成第一小標</a></li><li><a href="#nav-title-339">合成第二小標</a></li>'
        '</ul></div><p id="nav-title-338">合成第一小標</p><p id="nav-title-339">合成第二小標</p>','lxml')
    for element in list(outline.body.children):
        body.append(element)
    return soup


def page():
    full = TITLE+' | 三立iNEWS | 三立新聞網 SETN.COM'
    return BeautifulSoup(f'''<html><head><title>{full}</title>
      <link rel="canonical" href="/news/123456"><meta property="og:url" content="/news/123456">
      <meta property="og:type" content="article"><meta property="og:title" content="{full}">
      <meta name="Title" content="{full}"><meta name="author" content="三立iNEWS">
      <meta name="source" content="三立iNEWS"><meta name="description" content="NOT AN ARTICLE BODY">
      </head><body><div id="newsArea" class="stockNewArea">
      <div class="leftCon" itemscope itemtype="https://schema.org/NewsArticle">
       <div class="newsTitle printdiv" itemprop="headline">{TITLE}</div><div class="flexbox">
        <div class="shareBox hidden-print">SHARING NOT BODY</div>
        <div id="ckuse" class="pageText printdiv"><article><div class="newsCon">
         <div class="article_remark_wrap"><div class="author_wrap">記者
          <a href="https://www.setn.com/author/test-reporter" itemprop="author">{NAME}</a> 台北報導</div>
          <time class="pageDate">{CLOCK}</time></div>
         <div itemprop="articleBody"><p></p><figure><picture><img src="https://attach.setn.com/test.jpg"
          alt="ALT IS NOT A SECOND CAPTION"></picture><figcaption contenteditable="false">{CAPTION}</figcaption></figure>
          <p>{TEXT}</p><p><span style="color:#ff0000; font-size:18px">合成風險提示，不能刪除。</span></p></div>
        </div></article></div><div class="extend hidden-print">RELATED NOT BODY</div>
       </div></div><div class="rightCon hidden-print">NEIGHBOUR NOT BODY</div></div></body></html>''','lxml')


def extract(soup=None, **kwargs):
    return extract_article(str(soup or page()).encode(),policy(),page_url=kwargs.get('url',URL))


def test_native_whole_body_credit_and_unresolved_minute_clock_are_preserved():
    row = extract()
    assert row['title'] == TITLE and json.loads(row['authors_json']) == [NAME]
    assert TEXT in row['body'] and row['body'].count(CAPTION) == 1
    assert '合成風險提示，不能刪除。' in row['body']
    assert all(text not in row['body'] for text in ('NOT BODY','NOT AN ARTICLE BODY','ALT IS NOT','記者 '+NAME,CLOCK))
    assert row['published_at_raw'] == CLOCK and row['modified_at_raw'] is None
    assert row['declared_canonical_url'] == '/news/123456'
    assert not row['historical_point_in_time']
    detail, notices = map(json.loads,(row['author_attribution_detail_json'],row['verified_article_notices_json']))
    assert detail['source_visible_signature_raw'] == '記者 '+NAME+' 台北報導'
    assert not detail['profile_requested'] and not detail['original_ownership_verified']
    assert not detail['human_reporter_identity_verified']
    assert notices['source_current_article_clock_raw'] == CLOCK and notices['source_clock_precision'] == 'minute'
    assert not notices['source_timezone_declared'] and not notices['source_clock_release_or_revision_role_labelled']
    assert not notices['exact_PIT_eligible'] and not notices['revision_clock_present']
    assert 'timezone_and_release_or_revision_role_unstated' in row['published_time_basis']


def test_absolute_declared_same_url_is_supported_without_alias_or_clock_rewrite():
    soup = page()
    soup.select_one('link[rel=canonical]')['href'] = URL
    soup.select_one('meta[property="og:url"]')['content'] = URL
    assert extract(soup)['body_sha256'] == extract()['body_sha256']
    assert extract(soup)['declared_canonical_url'] == URL


def test_another_native_reporter_does_not_require_a_current_staff_or_name_whitelist():
    soup = page()
    soup.select_one('a[itemprop=author]').string = '另一記者'
    soup.select_one('a[itemprop=author]')['href'] = 'https://www.setn.com/author/another-person'
    assert json.loads(extract(soup)['authors_json']) == ['另一記者']


@pytest.mark.parametrize('target,attribute,value', [
    ('link[rel=canonical]','href','https://www.setn.com/news/123456'),
    ('link[rel=canonical]','href','/news/654321'),
    ('meta[property="og:url"]','content','/news/654321'),
    ('meta[property="og:type"]','content','website'),
    ('meta[property="og:title"]','content','DIFFERENT'),
    ('meta[name=Title]','content','DIFFERENT'),
    ('meta[name=author]','content','Unverified publisher'),
    ('meta[name=source]','content','Unverified publisher'),
    ('.leftCon','itemtype','https://schema.org/WebPage'),
])
def test_identity_declarations_cannot_be_borrowed_from_related_or_main_site(target,attribute,value):
    soup = page(); soup.select_one(target)[attribute] = value
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('url', [URL+'?page=2',URL+'#body',URL+'/',URL.replace('inews.','www.'),
    'http://inews.setn.com/news/123456','https://inews.setn.com/News.aspx?NewsID=123456'])
def test_unsupported_url_is_never_a_guessed_original_or_alias(url):
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract(url=url)


@pytest.mark.parametrize('change', [
    lambda s:s.select_one('.newsTitle').__setattr__('string','Another title'),
    lambda s:s.select_one('.leftCon').attrs.pop('itemscope'),
    lambda s:s.head.append(BeautifulSoup('<script type="application/ld+json">{}</script>','lxml').script),
    lambda s:s.select_one('.newsCon').append(BeautifulSoup('<p>EXTRA EDITOR TEXT</p>','lxml').p),
    lambda s:s.select_one('[itemprop=articleBody]').append(BeautifulSoup('<div>EXTRA BLOCK</div>','lxml').div),
    lambda s:s.select_one('[itemprop=articleBody]').append('EXTRA DIRECT EDITOR TEXT'),
    lambda s:s.select_one('[itemprop=articleBody]').append(BeautifulSoup('<script>PRIVATE SCRIPT</script>','lxml').script),
    lambda s:s.select_one('[itemprop=articleBody]').append(BeautifulSoup('<iframe></iframe>','lxml').iframe),
    lambda s:s.select_one('[itemprop=articleBody] > p').__setitem__('hidden',''),
    lambda s:s.select_one('.newsCon').__setitem__('style','display: none'),
    lambda s:s.select_one('figcaption').decompose(),
    lambda s:s.select_one('figure').append(BeautifulSoup('<p>LOST FIGURE PARAGRAPH</p>','lxml').p),
    lambda s:s.select_one('.flexbox').append(BeautifulSoup('<p>OUTSIDE EDITOR TEXT</p>','lxml').p),
])
def test_full_editor_visibility_and_container_boundaries_cannot_silently_drop_new_content(change):
    soup = page(); change(soup)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('change', [
    lambda s:s.select_one('.author_wrap').__setattr__('string','中央社'),
    lambda s:s.select_one('a[itemprop=author]').__setitem__('href','https://other.test/author/person'),
    lambda s:s.select_one('a[itemprop=author]').__setitem__('hidden',''),
    lambda s:s.select_one('.author_wrap').append(BeautifulSoup('<span>轉載</span>','lxml').span),
    lambda s:s.select_one('.author_wrap').append(BeautifulSoup('<a itemprop="author">另一人</a>','lxml').a),
])
def test_third_party_missing_hidden_or_ambiguous_credit_cannot_be_a_reporter(change):
    soup = page(); change(soup)
    with pytest.raises(NewsError,match='article_(attribution|identity)_not_verified'):
        extract(soup)


@pytest.mark.parametrize('clock', ['2021/02/29 09:01','2020/03/02','2020/03/02 09:01:30',
    '2020/03/02 09:01 +08:00','PRIVATE BODY','2020/03/02 25:01'])
def test_unknown_precision_invalid_dates_or_prose_cannot_turn_into_publication_clock(clock):
    soup = page(); soup.select_one('time').string = clock
    with pytest.raises(NewsError,match='invalid_bulk_publication_clock'):
        extract(soup)


def test_additional_machine_clock_cannot_be_silently_ignored():
    soup = page(); soup.select_one('time')['datetime'] = '2020-03-02T09:01:00+08:00'
    with pytest.raises(NewsError,match='invalid_bulk_publication_clock'):
        extract(soup)


def test_image_credit_and_meta_description_cannot_substitute_for_original_prose():
    soup = page()
    for p in soup.select('[itemprop=articleBody] > p'):
        p.decompose()
    soup.select_one('figcaption').string = TEXT
    with pytest.raises(NewsError,match='body_not_found'):
        extract(soup)


@pytest.mark.parametrize('changes', [
    {'domains':['www.setn.com']},{'acquisition_mode':'finite'},
    {'private_known_url_scope':''},{'require_author_attribution':False},
    {'require_configured_container':False},{'title_selector':'h1'},
    {'article_selectors':['article']},{'author_selectors':['meta[name=author]']},
    {'exclude_selectors':['p']},{'allow_short_configured_body':True},
    {'feeds':['https://inews.setn.com/rss']},
])
def test_source_scope_is_known_url_only_and_exact_native_contract(changes):
    value = entry(); value.update(changes)
    with pytest.raises(ValueError):
        policy(value)


def test_exact_reviewed_http_binding_preserves_original_diagnostic_controls(monkeypatch):
    source = policy()
    binding = (source.data_fingerprint,source.authorization_fingerprint,'a'*64,'b'*64)
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_HTTP_BINDING',binding)
    bound = policy()
    assert bound.http_capacity_source_fingerprint == 'a'*64
    assert bound.http_capacity_authorization_fingerprint == 'b'*64
    value = entry(); value['authorization']['scope'] += ' changed'
    changed = policy(value)
    assert not changed.http_capacity_source_fingerprint
    assert not changed.http_capacity_authorization_fingerprint


def test_same_response_failure_metadata_never_retains_body_names_or_accepts_article():
    soup = page(); before = str(soup)
    result = inews_boundary_metadata(soup,page_url=URL)
    assert str(soup) == before
    assert result['native_editors'] == result['native_clocks'] == result['native_credits'] == 1
    assert result['publisher_requests'] == 0 and not result['accepted_as_article']
    assert not result['raw_body_or_names_retained']
    encoded = json.dumps(result,ensure_ascii=False)
    assert NAME not in encoded and TEXT not in encoded and CLOCK not in encoded


def test_missing_validation_context_preserves_public_credit_and_clock_without_body_or_acceptance():
    soup = page(); before = str(soup)
    result = inews_validation_context(soup, page_url=URL)
    assert str(soup) == before
    assert result['contract_version'] == 4 and result['publisher_requests'] == 0
    assert not result['accepted_as_article'] and not result['raw_response_retained']
    assert not result['full_body_retained'] and not result['publication_offset_or_release_role_inferred']
    assert result['credits'][0]['signature_raw']['value'] == '記者 '+NAME+' 台北報導'
    assert result['clocks'][0]['raw']['value'] == CLOCK
    assert all(v['matches_literal_native_identity'] == [True] for v in result['head_identity'].values())
    assert TEXT not in json.dumps(result, ensure_ascii=False) and CAPTION not in json.dumps(result, ensure_ascii=False)


def test_validation_context_bounds_unreviewed_structures_and_redacts_profile_secrets():
    soup = page()
    soup.select_one('a[itemprop=author]')['href'] = 'https://www.setn.com/author/test?token=SECRET#FRAGMENT'
    soup.select_one('.author_wrap').append('X'*1000)
    body = soup.select_one('[itemprop=articleBody]')
    for _ in range(160):
        body.append(BeautifulSoup('<blockquote><p>UNREVIEWED PROSE</p></blockquote>', 'lxml').blockquote)
    result = inews_validation_context(soup, page_url=URL)
    assert result['credits'][0]['signature_raw']['truncated']
    assert result['editors'][0]['nodes_truncated'] and len(result['editors'][0]['nodes']) == 128
    encoded = json.dumps(result, ensure_ascii=False)
    assert 'SECRET' not in encoded and 'FRAGMENT' not in encoded and 'UNREVIEWED PROSE' not in encoded


def test_printed_credit_candidate_is_explicit_and_cannot_invent_a_verified_author():
    soup = page(); soup.select_one('[itemprop=articleBody] > p').string = '記者合成記者／台北報導'
    result = inews_validation_context(soup, page_url=URL)
    credit, = result['editors'][0]['printed_credit_candidates']
    assert credit['short_printed_credit_candidate'] == '記者合成記者／台北報導'
    assert not credit['verified_as_credit'] and not result['accepted_as_article']


def test_context_finds_printed_credit_after_empty_paragraph_without_retaining_other_prose():
    soup = page()
    body = soup.select_one('[itemprop=articleBody]')
    body.insert(0, BeautifulSoup('<p> </p>', 'lxml').p)
    body.insert(1, BeautifulSoup('<p>記者合成記者／台北報導</p>', 'lxml').p)
    before = str(soup)
    result = inews_validation_context(soup, page_url=URL)
    credit, = result['editors'][0]['printed_credit_candidates']
    scan, = result['editors'][0]['printed_credit_scans']
    assert credit['paragraph_index'] == scan['first_nonempty_paragraph_index'] == 1
    assert credit['short_printed_credit_candidate'] == '記者合成記者／台北報導'
    assert not credit['verified_as_credit'] and not scan['absence_of_candidate_proves_no_credit']
    assert TEXT not in json.dumps(result,ensure_ascii=False) and str(soup) == before


def test_context_bounds_paragraph_scan_and_credit_candidates_without_claiming_credit_absence():
    soup = page(); body = soup.select_one('[itemprop=articleBody]')
    for _ in range(20):
        body.insert(0, BeautifulSoup('<p>記者合成記者／台北報導</p>', 'lxml').p)
    context = inews_validation_context(soup,page_url=URL)['editors'][0]
    assert len(context['printed_credit_candidates']) == 4
    assert context['printed_credit_scans'][0]['direct_paragraphs_considered'] == 16
    assert context['printed_credit_scans'][0]['paragraph_scan_truncated']
    assert context['printed_credit_scans'][0]['credit_candidates_truncated']
    assert not context['printed_credit_scans'][0]['absence_of_candidate_proves_no_credit']


def test_context_observes_direct_list_links_and_labels_without_acceptance_alias_or_secret():
    soup = page(); body = soup.select_one('[itemprop=articleBody]')
    body.append(BeautifulSoup('<p>更多新聞：</p>', 'lxml').p)
    body.append(BeautifulSoup('<ul><li><a href="/news/1920001?token=SECRET#FRAGMENT">合成標題</a></li>'
        '<li><a href="https://user:SECRET@example.com/no">另一合成標題</a></li></ul>', 'lxml').ul)
    before = str(soup)
    result = inews_validation_context(soup,page_url=URL)
    context, = result['editors'][0]['direct_link_list_contexts']
    assert context['previous_sibling']['short_related_label_candidate'] == '更多新聞：'
    assert not context['previous_sibling']['verified_as_related_exclusion']
    assert context['links'][0]['href_without_query_or_fragment']['value'] == 'https://inews.setn.com/news/1920001'
    assert context['links'][1]['href_without_query_or_fragment']['value'] == ''
    assert context['resolved_links_are_context_not_URL_aliases'] and not context['accepted_as_body_or_related_exclusion']
    encoded = json.dumps(result,ensure_ascii=False)
    assert 'SECRET' not in encoded and 'FRAGMENT' not in encoded and TEXT not in encoded
    assert str(soup) == before and not result['accepted_as_article']


def test_context_list_and_anchor_limits_do_not_copy_nested_or_arbitrary_article_prose():
    soup = page(); body = soup.select_one('[itemprop=articleBody]')
    for _ in range(8):
        body.append(BeautifulSoup('<ul>'+''.join('<li><a href="/news/1920001">合成連結</a></li>' for _ in range(12))+'</ul>', 'lxml').ul)
    context = inews_validation_context(soup,page_url=URL)['editors'][0]
    assert len(context['direct_link_list_contexts']) == 4
    assert all(len(row['links']) == 8 and row['links_truncated'] and row['list_scan_truncated'] for row in context['direct_link_list_contexts'])


def test_nested_outline_context_preserves_fragment_target_question_without_a_related_exclusion():
    soup = page(); body = soup.select_one('[itemprop=articleBody]')
    outline = BeautifulSoup('<div id="nav-directory"><p><strong>目錄</strong></p>'
        '<ul><li><a href="#nav-title-338">合成小標</a></li></ul></div>'
        '<p id="nav-title-338">合成小標</p>','lxml')
    body.append(outline.select_one('#nav-directory'))
    body.append(outline.select_one('#nav-title-338'))
    before = str(soup)
    result = inews_validation_context(soup,page_url=URL)
    editor, = result['editors']
    assert not editor['direct_link_list_contexts']
    context, = editor['nested_link_list_contexts']
    target, = editor['native_outline_target_candidates']
    assert context['ancestor_shapes_until_native_body'][0]['id']['value'] == 'nav-directory'
    assert context['previous_sibling']['short_outline_label_candidate'] == '目錄'
    assert not context['previous_sibling']['short_related_label_candidate']
    assert context['links'][0]['native_local_outline_fragment_candidate']['value'] == 'nav-title-338'
    assert target['element']['id']['value'] == 'nav-title-338' and target['label']['value'] == '合成小標'
    assert not target['verified_as_outline_target'] and not context['accepted_as_body_or_related_exclusion']
    assert not result['accepted_as_article'] and str(soup) == before
    assert TEXT not in json.dumps(result,ensure_ascii=False)


def test_outline_context_does_not_turn_foreign_secret_or_arbitrary_fragments_into_local_targets():
    soup = page(); body = soup.select_one('[itemprop=articleBody]')
    body.append(BeautifulSoup('<ul><li><a href="https://other.test/a#nav-title-338">外站</a></li>'
        '<li><a href="#token-SECRET">未知</a></li><li><a href="?token=SECRET#nav-title-338">查詢</a></li>'
        '<li><a href="#nav-title-0">零值</a></li></ul>','lxml').ul)
    result = inews_validation_context(soup,page_url=URL)
    context, = result['editors'][0]['direct_link_list_contexts']
    assert all(not row['native_local_outline_fragment_candidate']['value'] for row in context['links'])
    assert 'SECRET' not in json.dumps(result,ensure_ascii=False)


def test_outline_context_caps_targets_and_reports_duplicate_ids_instead_of_verifying_them():
    soup = page(); body = soup.select_one('[itemprop=articleBody]')
    for _ in range(40):
        body.append(BeautifulSoup('<p id="nav-title-338">合成小標</p>','lxml').p)
    targets = inews_validation_context(soup,page_url=URL)['editors'][0]['native_outline_target_candidates']
    assert len(targets) == 32 and all(row['scan_truncated'] for row in targets)
    assert all(not row['verified_as_outline_target'] for row in targets)


def test_nested_list_context_has_a_shared_total_cap_and_bounded_ancestor_chain():
    soup = page(); body = soup.select_one('[itemprop=articleBody]')
    body.append(BeautifulSoup('<div><div><div><div>'+''.join('<ul><li><a href="#nav-title-338">合成</a></li></ul>'
        for _ in range(8))+'</div></div></div></div>','lxml').div)
    context = inews_validation_context(soup,page_url=URL)['editors'][0]
    assert not context['direct_link_list_contexts'] and len(context['nested_link_list_contexts']) == 4
    assert all(row['list_scan_truncated'] and row['ancestor_scan_truncated']
        and len(row['ancestor_shapes_until_native_body'])==3 for row in context['nested_link_list_contexts'])


@pytest.mark.parametrize('entry_factory', [entry, v2_entry, v3_entry, v4_entry, v5_entry, v6_entry, v7_entry, v8_entry])
@pytest.mark.parametrize('metadata_only', [False, True])
def test_explicit_probe_uses_the_same_envelope_for_missing_credit_context(metadata_only, entry_factory):
    from types import SimpleNamespace
    from downloader.news_fulltext_http import Response
    from scripts.probe_news_sources import probe_provider
    calls = []
    response = Response(URL, 200, {'content-type':'text/html'}, str(page()).encode())
    def get(url):
        calls.append(url)
        return response
    source = policy(entry_factory())
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda _url:source), get=get,
        provider_not_before={}, robot_delays={})
    item, = probe_provider(http, [URL], metadata_only=metadata_only)
    assert calls == [URL] and not item['article_durably_accepted']
    assert item['inews_native_validation_context']['credits'][0]['signature_raw']['value'] == '記者 '+NAME+' 台北報導'
    assert 'private_diagnostic_path' not in item and 'body' not in item
    assert ('extracted' in item) is (not metadata_only)


@pytest.mark.parametrize('entry_factory', [entry, v2_entry, v3_entry, v4_entry, v5_entry, v6_entry, v7_entry, v8_entry])
def test_received_failure_keeps_new_context_in_the_old_daily_budget_without_accepting_or_parsing(tmp_path, monkeypatch, entry_factory):
    from contextlib import closing
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_failure_diagnostics import fail, reports
    from test_news_failure_identity_metadata import evidence
    registry = PolicyRegistry({'contract_version':1,'collection_purpose':'personal_research',
        'publish':False,'remote_training_export':False,'providers':[entry_factory()]}, base=tmp_path)
    monkeypatch.setattr('downloader.news_fulltext_setn.setn_inews_native_article',
        lambda *_a, **_k:pytest.fail('Metadata must not run the accepting parser again'))
    response = Response(URL, 200, {'content-type':'text/html'}, str(page()).encode())
    with closing(NewsQueue(tmp_path/'store', registry)) as queue:
        for _ in range(3):
            fail(queue, response, code='article_attribution_not_verified', received_at_utc='2026-10-23T01:00:00+00:00')
        paths, rows = evidence(queue.root)
        # Receipt names include UUIDs, not an ordering guarantee when two
        # reservations share an observation clock. Verify the exact multiset.
        assert len(rows) == 2 and sorted(row['budget_attempt'] for row in rows) == [1,2]
        for row, path in zip(rows, paths):
            facts = row['facts']
            assert facts['inews_native_boundary']['contract_version'] == 1
            assert facts['inews_native_validation_context']['contract_version'] == 4
            assert not row['raw_response_retained'] and not row['accepted_as_article']
            assert TEXT not in path.read_text() and CAPTION not in path.read_text()
        assert not reports(queue.root)[0]
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_diagnostic_regression_cases_cover_every_registered_native_layout():
    assert {factory()['embedded_article_layout'] for factory in (entry,v2_entry,v3_entry,v4_entry,v5_entry,v6_entry,v7_entry,v8_entry)} == set(INEWS_LAYOUT_VERSIONS)


def test_native_diagnostic_upgrade_does_not_refill_the_same_day_budget(tmp_path, monkeypatch):
    from contextlib import closing
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_queue import NewsQueue, utc_now
    from test_news_failure_diagnostics import fail, reports
    from test_news_failure_identity_metadata import evidence
    monkeypatch.setattr('downloader.news_fulltext_setn.setn_inews_native_article',
        lambda *_a, **_k:pytest.fail('Do not run the accepting parser in a failure diagnostic'))
    response = Response(URL,200,{'content-type':'text/html'},str(page()).encode())
    root, clock = tmp_path/'store', utc_now()
    original = {}
    for index, factory in enumerate((entry,v2_entry,v3_entry,v4_entry,v5_entry,v6_entry,v7_entry,v8_entry)):
        registry = PolicyRegistry({'contract_version':1,'collection_purpose':'personal_research',
            'publish':False,'remote_training_export':False,'providers':[factory()]},base=tmp_path)
        with closing(NewsQueue(root,registry)) as queue:
            fail(queue,response,code='article_attribution_not_verified',received_at_utc=clock)
            paths, rows = evidence(root)
            assert len(paths)==min(index+1,2)
            assert sorted(row['budget_attempt'] for row in rows)==list(range(1,min(index+1,2)+1))
            assert all(row['facts']['inews_native_validation_context']['contract_version']==4 for row in rows)
            assert all(path.read_bytes()==original[path] for path in original)
            original.update({path:path.read_bytes() for path in paths})
            assert not reports(root)[0]
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0]==0
    assert len(original)==2


@pytest.mark.parametrize('container', ['paragraph','caption'])
def test_v6_witnessed_legacy_typeface_preserves_written_values_and_raw_tree(container):
    soup=page()
    node=soup.select_one('[itemprop=articleBody] > p:nth-of-type(2)' if container=='paragraph' else 'figcaption')
    font=soup.new_tag('font',face='Arial, Helvetica, sans-serif')
    for child in list(node.contents): font.append(child.extract())
    node.append(font)
    before=str(soup)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(before.encode(),policy(v5_entry()),page_url=URL)
    row=extract_article(before.encode(),policy(v6_entry()),page_url=URL)
    old=extract_article(str(page()).encode(),policy(v5_entry()),page_url=URL)
    assert {k:v for k,v in row.items() if k!='article_identity_basis'}=={
        k:v for k,v in old.items() if k!='article_identity_basis'}
    assert str(soup)==before and row['article_identity_basis'].endswith('_v6')


@pytest.mark.parametrize('mutation', ['face','color','style','hidden','aria_hidden',
    'outside_written','unknown_child','hidden_parent'])
def test_v6_does_not_treat_unknown_or_hidden_font_subtrees_as_visible_prose(mutation):
    soup=page(); paragraph=soup.select_one('[itemprop=articleBody] > p:nth-of-type(2)')
    font=soup.new_tag('font',face='Arial, Helvetica, sans-serif');font.string='合成原始值1234股'
    paragraph.append(font)
    if mutation=='face': font['face']='Unreviewed Font'
    elif mutation=='color': font['color']='white'
    elif mutation=='style': font['style']='display:none'
    elif mutation=='hidden': font['hidden']=''
    elif mutation=='aria_hidden': font['aria-hidden']='true'
    elif mutation=='outside_written': soup.select_one('[itemprop=articleBody]').append(font.extract())
    elif mutation=='unknown_child': font.append(soup.new_tag('iframe',src='https://example.invalid/'))
    else: paragraph['hidden']=''
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(str(soup).encode(),policy(v6_entry()),page_url=URL)


def test_v6_http_control_identity_requires_exact_source_authorization_and_rate(monkeypatch):
    source=policy(v6_entry())
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V6_HTTP_BINDING',
        (source.data_fingerprint,source.authorization_fingerprint,'a'*64,'b'*64))
    assert policy(v6_entry()).http_capacity_source_fingerprint=='a'*64
    for change in ('scope','rate'):
        value=v6_entry()
        if change=='scope': value['authorization']['scope']+=' changed'
        else: value['parallelism']=2
        assert not policy(value).http_capacity_source_fingerprint


@pytest.mark.parametrize('previous_entry,next_entry', [(v5_entry,v6_entry),(v6_entry,v7_entry),(v7_entry,v8_entry)])
def test_native_upgrade_preserves_saved_bodies_and_failed_head_state(tmp_path,previous_entry,next_entry):
    from contextlib import closing
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_queue import NewsQueue, utc_now
    from test_news_failure_diagnostics import fail
    def registry(value):
        return PolicyRegistry({'contract_version':1,'collection_purpose':'personal_research',
            'publish':False,'remote_training_export':False,'providers':[value]},base=tmp_path)
    root=tmp_path/'store';saved=URL+'1'
    with closing(NewsQueue(root,registry(previous_entry()))) as queue:
        fail(queue,Response(URL,200,{'content-type':'text/html'},str(page()).encode()),
            code='article_identity_not_verified',received_at_utc=utc_now())
        queue.add_articles([{'url':saved}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,body_sha256=?,body_bytes=?,parquet_path=?,captured_at_utc=?,due=? WHERE url=?',
                ('complete','a'*64,123,'batches/existing.parquet',utc_now(),10000000000,saved))
        before={r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(root,registry(next_entry()))) as queue:
        assert before=={r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}


@pytest.mark.parametrize('whitespace', ['', '\u00a0', ' \n\u00a0\t'])
def test_v7_empty_native_outline_placeholder_keeps_every_written_value_and_tree(monkeypatch,whitespace):
    soup=page_with_outline(monkeypatch);before=extract_article(str(soup).encode(),policy(v6_entry()),page_url=URL)
    heading=soup.new_tag('h1')
    if whitespace: heading.string=whitespace
    soup.select_one('#nav-directory').insert_after(heading);raw=str(soup)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(raw.encode(),policy(v6_entry()),page_url=URL)
    result=extract_article(raw.encode(),policy(v7_entry()),page_url=URL)
    notices=json.loads(result['verified_article_notices_json'])
    proof=notices.pop('verified_empty_outline_heading_placeholders')
    assert proof['count']==1 and not proof['written_values_removed'] and not proof['raw_source_tree_modified']
    assert notices==json.loads(before['verified_article_notices_json'])
    assert {k:v for k,v in result.items() if k not in {'article_identity_basis','verified_article_notices_json'}}=={
        k:v for k,v in before.items() if k not in {'article_identity_basis','verified_article_notices_json'}}
    assert str(soup)==raw and result['article_identity_basis'].endswith('_v7')


@pytest.mark.parametrize('mutation', ['text','nested','comment','style','hidden','aria_hidden','id',
    'duplicate','before_outline','elsewhere','no_outline','unverified_outline','h2','zero_width'])
def test_v7_placeholder_does_not_discard_headings_unknown_subtrees_or_unverified_navigation(monkeypatch,mutation):
    from bs4 import Comment
    soup=page_with_outline(monkeypatch);heading=soup.new_tag('h1');heading.string='\u00a0'
    outline=soup.select_one('#nav-directory');outline.insert_after(heading)
    if mutation=='text': heading.string='合成有意義的小標題'
    elif mutation=='nested': heading.append(soup.new_tag('span'))
    elif mutation=='comment': heading.append(Comment('not a blank text node'))
    elif mutation in {'style','hidden','aria_hidden','id'}:
        heading[{'aria_hidden':'aria-hidden'}.get(mutation,mutation)]={'style':'display:none','hidden':'','aria_hidden':'true','id':'unknown'}[mutation]
    elif mutation=='duplicate': heading.insert_after(soup.new_tag('h1'))
    elif mutation=='before_outline': outline.insert_before(heading.extract())
    elif mutation=='elsewhere': soup.select_one('[itemprop=articleBody]').append(heading.extract())
    elif mutation=='no_outline': outline.decompose()
    elif mutation=='unverified_outline': outline.select_one('a')['href']='#unknown'
    elif mutation=='h2': heading.name='h2'
    else: heading.string='\u200b'
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(str(soup).encode(),policy(v7_entry()),page_url=URL)


def test_v7_without_placeholder_preserves_v6_values_and_http_controls(monkeypatch):
    for soup in (page(),page_with_outline(monkeypatch)):
        old=extract_article(str(soup).encode(),policy(v6_entry()),page_url=URL)
        result=extract_article(str(soup).encode(),policy(v7_entry()),page_url=URL)
        assert {k:v for k,v in result.items() if k!='article_identity_basis'}=={
            k:v for k,v in old.items() if k!='article_identity_basis'}
    source=policy(v7_entry())
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V7_HTTP_BINDING',
        (source.data_fingerprint,source.authorization_fingerprint,'a'*64,'b'*64))
    assert policy(v7_entry()).http_capacity_source_fingerprint=='a'*64
    for change in ('scope','rate'):
        value=v7_entry()
        if change=='scope': value['authorization']['scope']+=' changed'
        else: value['parallelism']=2
        assert not policy(value).http_capacity_source_fingerprint


def inline_outline_page(monkeypatch):
    soup=page_with_outline(monkeypatch)
    # Deliberately not numeric ID order: source order is authoritative.
    first,second=soup.select('#nav-directory a')
    first['href'],second['href']='#nav-title-339','#nav-title-338'
    for paragraph,identifier,heading,prose in zip(
            soup.select('[itemprop=articleBody] > p[id]'),('nav-title-339','nav-title-338'),
            ('合成第一小標','合成第二小標'),('合成負值-5股，比率2.3%。','合成另一段1234股。')):
        paragraph.attrs={'data-path-to-node':'6'};paragraph.clear()
        strong=soup.new_tag('strong',id=identifier,style='color:rgb(0, 0, 255); font-weight:bold')
        strong.string=heading;paragraph.append(strong)
        paragraph.append(soup.new_tag('br'));paragraph.append(soup.new_tag('br'));paragraph.append(prose)
    return soup


def test_v8_inline_outline_preserves_the_whole_carrier_paragraph_once_in_source_order(monkeypatch):
    soup=inline_outline_page(monkeypatch);raw=str(soup)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(raw.encode(),policy(v7_entry()),page_url=URL)
    result=extract_article(raw.encode(),policy(v8_entry()),page_url=URL)
    proof,=json.loads(result['verified_article_notices_json'])['matched_native_in_page_outlines']
    assert proof['contract_version']==2 and proof['ordered_local_heading_ids']==['nav-title-339','nav-title-338']
    assert proof['ordered_target_tags']==['strong','strong']
    assert proof['inline_heading_and_following_prose_preserved_in_one_native_paragraph']
    assert '合成第一小標\n合成負值-5股，比率2.3%。\n\n合成第二小標\n合成另一段1234股。' in result['body']
    assert result['body'].count('合成第一小標')==result['body'].count('合成第二小標')==1
    assert '合成本文目錄' not in result['body'] and result['body'].count(CAPTION)==1
    assert str(soup)==raw and not result['historical_point_in_time']


@pytest.mark.parametrize('mutation', ['label','duplicate_id','outside','late','nested',
    'hidden','aria_hidden','hidden_parent','same_carrier','missing_target'])
def test_v8_inline_heading_still_requires_unique_visible_ordered_local_targets(monkeypatch,mutation):
    soup=inline_outline_page(monkeypatch);heading=soup.select_one('strong[id]')
    if mutation=='label': heading.string='UNMATCHED HEADING'
    elif mutation=='duplicate_id': soup.body.append(soup.new_tag('div',id=heading['id']))
    elif mutation=='outside': soup.body.append(heading.extract())
    elif mutation=='late': heading.insert_before('UNVERIFIED PREFIX')
    elif mutation=='nested': heading.wrap(soup.new_tag('span'))
    elif mutation=='hidden': heading['hidden']=''
    elif mutation=='aria_hidden': heading['aria-hidden']='true'
    elif mutation=='hidden_parent': heading.parent['style']='display:none'
    elif mutation=='same_carrier': heading.parent.append(soup.select('strong[id]')[1].extract())
    else: heading.attrs.pop('id')
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(str(soup).encode(),policy(v8_entry()),page_url=URL)


def append_written_list(soup,kind='ul',count=3):
    node=soup.new_tag(kind,attrs={'data-start':'1','data-end':'999'})
    for index in range(count):
        item=soup.new_tag('li',attrs={'data-start':str(index),'data-end':'999'})
        paragraph=soup.new_tag('p',attrs={'data-start':str(index),'data-end':'999'})
        paragraph.string=f'合成清單原始值{index}：-5股、12.34、比率2.3%，不取log。'
        item.append(paragraph);node.append(item)
    soup.select_one('[itemprop=articleBody]').append(node)
    return node


@pytest.mark.parametrize('kind,count', [('ul',3),('ol',22)])
def test_v8_written_lists_preserve_all_values_once_without_invented_numbering(kind,count):
    soup=page();before=extract_article(str(soup).encode(),policy(v7_entry()),page_url=URL)
    node=append_written_list(soup,kind,count);raw=str(soup)
    paragraphs=[p.get_text() for p in node.select('li > p')]
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(raw.encode(),policy(v7_entry()),page_url=URL)
    result=extract_article(raw.encode(),policy(v8_entry()),page_url=URL)
    assert result['body']==before['body']+'\n\n'+'\n\n'.join(paragraphs)
    assert all(result['body'].count(p)==1 for p in paragraphs) and result['body'].count(CAPTION)==1
    proof,=json.loads(result['verified_article_notices_json'])['verified_native_written_prose_lists']
    assert proof['source_list_type']==kind and proof['source_entry_count']==count
    assert proof['source_order_preserved'] and not proof['list_number_labels_invented']
    assert not proof['navigation_or_related_content_excluded']
    assert str(soup)==raw and not result['historical_point_in_time']


def test_v8_mixed_lists_paragraphs_and_figcaption_follow_DOM_order_not_selector_order():
    soup=page();body=soup.select_one('[itemprop=articleBody]')
    first=append_written_list(soup,'ol',3);body.insert(1,first.extract())
    last=append_written_list(soup,'ul',4)
    last.select_one('p').string='合成最後清單原始值-789股。'
    result=extract_article(str(soup).encode(),policy(v8_entry()),page_url=URL)
    assert (result['body'].index('合成清單原始值0')<result['body'].index(CAPTION)
        <result['body'].index(TEXT)<result['body'].index('合成最後清單原始值-789股。'))
    assert result['body'].count(CAPTION)==1
    assert [r['source_list_type'] for r in json.loads(result['verified_article_notices_json'])[
        'verified_native_written_prose_lists']]==['ol','ul']


def test_v8_nonempty_list_paragraphs_count_as_written_body_not_just_captions():
    soup=page();body=soup.select_one('[itemprop=articleBody]')
    for node in list(body.children): node.extract()
    node=append_written_list(soup,'ul',5)
    result=extract_article(str(soup).encode(),policy(v8_entry()),page_url=URL)
    assert result['body']=='\n\n'.join(p.get_text() for p in node.select('li > p'))


@pytest.mark.parametrize('mutation', ['raw_list_text','raw_item_text','nested_list','two_paragraphs',
    'related_link','foreign_block','script','hidden_item','hidden_inline','empty_list','empty_paragraph',
    'start','reversed','type','class','invalid_data','style','too_many'])
def test_v8_unknown_or_hidden_list_structures_are_not_dropped_or_relaxed(mutation):
    soup=page();node=append_written_list(soup);item=node.li;paragraph=item.p
    if mutation=='raw_list_text': node.append('UNOWNED TEXT')
    elif mutation=='raw_item_text': item.append('UNOWNED TEXT')
    elif mutation=='nested_list': paragraph.append(soup.new_tag('ul'))
    elif mutation=='two_paragraphs': item.append(soup.new_tag('p'))
    elif mutation=='related_link':
        paragraph.decompose();anchor=soup.new_tag('a',href='/news/654321');anchor.string='RELATED';item.append(anchor)
    elif mutation=='foreign_block': paragraph.wrap(soup.new_tag('div'))
    elif mutation=='script': paragraph.append(soup.new_tag('script'))
    elif mutation=='hidden_item': item['hidden']=''
    elif mutation=='hidden_inline':
        hidden=soup.new_tag('strong',attrs={'aria-hidden':'true'});hidden.string='INVISIBLE';paragraph.append(hidden)
    elif mutation=='empty_list': node.clear()
    elif mutation=='empty_paragraph': paragraph.clear()
    elif mutation=='invalid_data': item['data-start']='not-a-number'
    elif mutation=='too_many':
        for _ in range(254): node.append(deepcopy(item))
    else: node[mutation]={'start':'2','reversed':'','type':'a','class':'unknown','style':'display:none'}[mutation]
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(str(soup).encode(),policy(v8_entry()),page_url=URL)


def test_v8_unchanged_representations_preserve_v7_data_and_exact_HTTP_controls(monkeypatch):
    for soup in (page(),page_with_outline(monkeypatch)):
        old=extract_article(str(soup).encode(),policy(v7_entry()),page_url=URL)
        result=extract_article(str(soup).encode(),policy(v8_entry()),page_url=URL)
        assert old['extraction_method']=='css:'+INEWS_BODY_SELECTOR
        assert result['extraction_method']=='css:'+INEWS_V8_BODY_SELECTOR
        assert {k:v for k,v in result.items() if k not in {'article_identity_basis','extraction_method'}}=={
            k:v for k,v in old.items() if k not in {'article_identity_basis','extraction_method'}}
    source=policy(v8_entry())
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V8_HTTP_BINDING',
        (source.data_fingerprint,source.authorization_fingerprint,'a'*64,'b'*64))
    assert policy(v8_entry()).http_capacity_source_fingerprint=='a'*64
    for change in ('scope','rate'):
        value=v8_entry()
        if change=='scope': value['authorization']['scope']+=' changed'
        else: value['parallelism']=2
        assert not policy(value).http_capacity_source_fingerprint
    wrong=v8_entry();wrong['article_selectors']=[INEWS_BODY_SELECTOR]
    with pytest.raises(ValueError): policy(wrong)


def test_v2_source_omitted_location_is_not_a_missing_author_or_a_guessed_location():
    soup = page(); byline = soup.select_one('.author_wrap')
    link = byline.a.extract(); byline.clear(); byline.append('記者 '); byline.append(link); byline.append(' 報導')
    with pytest.raises(NewsError,match='article_attribution_not_verified'):
        extract(soup)
    row = extract_v2(soup)
    assert json.loads(row['authors_json']) == [NAME]
    detail = json.loads(row['author_attribution_detail_json'])
    assert detail['source_visible_signature_raw'] == '記者 '+NAME+' 報導'
    assert not detail['human_reporter_identity_verified'] and not row['historical_point_in_time']
    assert row['body_sha256'] == extract()['body_sha256']


def test_v2_direct_native_image_preserves_written_caption_once_without_fetching_media():
    soup = page(); image = soup.select_one('picture > img').extract(); soup.select_one('picture').replace_with(image)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract(soup)
    row = extract_v2(soup)
    assert row['body_sha256'] == extract()['body_sha256'] and row['body'].count(CAPTION) == 1
    assert not json.loads(row['verified_article_notices_json'])['media_or_scripts_requested_or_executed']


@pytest.mark.parametrize('change',[
    lambda s:s.select_one('.author_wrap').__setattr__('string',''),
    lambda s:s.select_one('.author_wrap').__setattr__('string','中央社'),
    lambda s:s.select_one('a[itemprop=author]').__setitem__('href','https://foreign.test/author/other'),
    lambda s:s.select_one('figcaption').decompose(),
    lambda s:s.select_one('figure').append(s.new_tag('img')),
    lambda s:s.select_one('[itemprop=articleBody]').append(BeautifulSoup('<ul><li><a>UNREVIEWED LINKED LIST</a></li></ul>','lxml').ul),
])
def test_v2_does_not_invent_authors_drop_captions_or_silently_accept_unreviewed_lists(change):
    soup = page(); change(soup)
    with pytest.raises(NewsError,match='article_(identity|attribution)_not_verified'):
        extract_v2(soup)


def test_v2_preserves_all_v1_source_values_and_separates_parser_identity():
    old, new = extract(), extract_v2()
    for key in ('title','body','body_sha256','authors_json','published_at_raw','modified_at_raw',
        'verified_article_notices_json','author_attribution_detail_json','body_completeness'):
        assert old[key] == new[key]
    assert old['article_identity_basis'].endswith('_v1') and new['article_identity_basis'].endswith('_v2')
    assert policy().data_fingerprint != policy(v2_entry()).data_fingerprint


def test_v2_http_binding_is_exact_and_cannot_grant_a_changed_authorization(monkeypatch):
    source = policy(v2_entry())
    binding = (source.data_fingerprint,source.authorization_fingerprint,'a'*64,'b'*64)
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V2_HTTP_BINDING',binding)
    assert policy(v2_entry()).http_capacity_source_fingerprint == 'a'*64
    value = v2_entry(); value['authorization']['scope'] += ' changed'
    assert not policy(value).http_capacity_source_fingerprint


def test_v3_matches_outline_to_unique_ordered_headings_and_keeps_body_values_once(monkeypatch):
    soup = page_with_outline(monkeypatch); before = str(soup)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_v2(soup)
    row = extract_v3(soup)
    assert str(soup) == before and row['body'].count('合成第一小標') == row['body'].count('合成第二小標') == 1
    assert '合成本文目錄' not in row['body'] and row['body'].count(CAPTION) == 1
    soup.select_one('#nav-directory').decompose()
    reference = extract_v2(soup)
    for key in ('title','body','body_sha256','authors_json','published_at_raw','modified_at_raw','author_attribution_detail_json'):
        assert row[key] == reference[key]
    notices = json.loads(row['verified_article_notices_json'])
    outline, = notices.pop('matched_native_in_page_outlines')
    assert notices == json.loads(reference['verified_article_notices_json'])
    assert outline['ordered_local_heading_ids'] == ['nav-title-338','nav-title-339']
    assert outline['link_labels_match_unique_visible_native_heading_text'] and outline['heading_text_preserved_once']
    assert not outline['links_requested_or_accepted_as_URL_aliases']
    assert not row['historical_point_in_time'] and not notices['exact_PIT_eligible']
    assert row['article_identity_basis'].endswith('_v3')


@pytest.mark.parametrize('change',[
    lambda s:s.select_one('#nav-directory > p > strong').__setattr__('string','UNKNOWN LABEL'),
    lambda s:s.select_one('#nav-directory > p').append('UNKNOWN PROSE'),
    lambda s:s.select_one('#nav-directory').append(s.new_tag('p')),
    lambda s:s.select_one('#nav-directory ul').append(s.new_tag('script')),
    lambda s:s.select_one('#nav-directory li').append('UNKNOWN PROSE'),
    lambda s:s.select_one('#nav-directory li a').append(s.new_tag('b')),
    lambda s:s.select_one('#nav-directory li a').__setitem__('href','https://foreign.test/a#nav-title-338'),
    lambda s:s.select_one('#nav-directory li a').__setitem__('href',URL+'#nav-title-338'),
    lambda s:s.select_one('#nav-directory li a').__setitem__('href','#nav-title-338?token=secret'),
    lambda s:s.select_one('#nav-directory li a').__setitem__('href','#nav-title-0'),
    lambda s:s.select_one('#nav-directory li a').__setattr__('string','DIFFERENT HEADING'),
    lambda s:s.select('#nav-directory li a')[1].__setitem__('href','#nav-title-338'),
    lambda s:s.select_one('#nav-title-338').__setitem__('id','unmatched-target'),
    lambda s:s.select_one('#nav-title-338').__setitem__('hidden',''),
    lambda s:s.select_one('#nav-directory').__setitem__('style','display:none'),
    lambda s:s.select_one('#nav-title-338').append(s.new_tag('strong')),
    lambda s:s.select_one('#nav-title-338').__setattr__('name','span'),
    lambda s:s.select_one('[itemprop=articleBody]').append(s.select_one('#nav-directory').extract()),
    lambda s:s.body.append(BeautifulSoup('<p id="nav-title-338">OTHER</p>','lxml').p),
    lambda s:s.select_one('[itemprop=articleBody]').append(BeautifulSoup('<p id="nav-title-340">EXTRA HEADING</p>','lxml').p),
    lambda s:s.select_one('[itemprop=articleBody]').append(BeautifulSoup('<ul><li>UNREVIEWED LIST</li></ul>','lxml').ul),
])
def test_v3_does_not_drop_unmatched_prose_links_ids_or_unknown_lists(monkeypatch,change):
    soup = page_with_outline(monkeypatch); change(soup)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_v3(soup)


def test_v3_without_outline_preserves_all_v2_values_except_parser_identity():
    old, new = extract_v2(), extract_v3()
    for key in old:
        if key!='article_identity_basis': assert old[key] == new[key]
    assert new['article_identity_basis'].endswith('_v3')
    assert policy(v2_entry()).data_fingerprint!=policy(v3_entry()).data_fingerprint


def test_v3_retains_v2_omitted_location_and_direct_image_but_never_invents_author():
    soup = page(); byline = soup.select_one('.author_wrap')
    link = byline.a.extract(); byline.clear(); byline.append('記者 '); byline.append(link); byline.append(' 報導')
    image = soup.select_one('picture > img').extract(); soup.select_one('picture').replace_with(image)
    assert extract_v3(soup)['body_sha256']==extract_v2(soup)['body_sha256']
    byline.clear()
    with pytest.raises(NewsError,match='article_attribution_not_verified'):
        extract_v3(soup)


def test_v3_http_binding_is_exact_and_cannot_grant_another_source_or_authorization(monkeypatch):
    source = policy(v3_entry())
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V3_HTTP_BINDING',
        (source.data_fingerprint,source.authorization_fingerprint,'a'*64,'b'*64))
    assert policy(v3_entry()).http_capacity_source_fingerprint=='a'*64
    for key in ('scope','reviewed_on'):
        value = v3_entry(); value['authorization'][key]+=' changed'
        assert not policy(value).http_capacity_source_fingerprint


@pytest.mark.parametrize('value',['http://[','https://[bad','//[invalid'])
def test_bad_source_url_declaration_is_a_diagnostic_mismatch_not_a_context_crash(value):
    soup = page(); soup.select_one('link[rel=canonical]')['href'] = value
    result = inews_validation_context(soup,page_url=URL)
    assert result['head_identity']['canonical']['matches_literal_native_identity'] == [False]
    assert not result['accepted_as_article']


@pytest.mark.parametrize('new_entry',[v2_entry,v3_entry,v4_entry])
def test_parser_only_policy_reconcile_preserves_failed_attempts_deadlines_and_requires_explicit_repair(tmp_path,new_entry):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue, utc_now
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_repairs import submit_repairs
    from test_news_failure_diagnostics import fail
    def registry(value):
        return PolicyRegistry({'contract_version':1,'collection_purpose':'personal_research',
            'publish':False,'remote_training_export':False,'providers':[value]},base=tmp_path)
    root = tmp_path/'store'
    with closing(NewsQueue(root, registry(entry()))) as queue:
        fail(queue,Response(URL,200,{'content-type':'text/html'},str(page()).encode()),
            code='article_attribution_not_verified',received_at_utc=utc_now())
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    current = registry(new_entry())
    with closing(NewsQueue(root, current)) as queue:
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
        assert before == after
    result = submit_repairs(root,current,[URL],reason='Witnessed missing-place signature, normal once-per-parser native recheck')
    assert result['submitted'] and result['submitted_targets'] == 1 and not result['queue_mutations']


def test_v4_printed_named_reporter_is_not_a_desk_person_or_invented_profile():
    soup = page(); soup.select_one('.author_wrap').clear()
    signature = '財經中心／合成記者報導'
    soup.select_one('[itemprop=articleBody] > p').string = signature
    before = str(soup)
    with pytest.raises(NewsError,match='article_attribution_not_verified'):
        extract_v3(soup)
    row = extract_article(before.encode(),policy(v4_entry()),page_url=URL)
    detail, notices = map(json.loads,(row['author_attribution_detail_json'],row['verified_article_notices_json']))
    assert json.loads(row['authors_json']) == [NAME]
    assert row['body'].startswith(signature) and row['body'].count(signature) == 1
    assert TEXT in row['body'] and row['body'].count(CAPTION) == 1
    assert detail['source_declared_person_name'] == NAME and detail['source_declared_desk'] == '財經中心'
    assert detail['source_declared_profile_url'] is None and detail['source_profile_absent']
    assert detail['printed_signature_preserved_in_article_body'] and not detail['profile_requested']
    assert not detail['human_reporter_identity_verified'] and not detail['original_ownership_verified']
    assert row['published_at_raw'] == CLOCK and row['modified_at_raw'] is None
    assert not notices['exact_PIT_eligible'] and not notices['source_timezone_declared']
    assert str(soup) == before and row['article_identity_basis'].endswith('_v4')


@pytest.mark.parametrize('signature', ['財經中心／綜合報導', '財經中心／整理報導', '財經中心／編輯報導',
    '財經中心／記者報導','中央社／合成記者報導','國際中心／合成記者報導','財經中心/合成記者報導',
    '合成記者報導','財經中心／合成記者報導 trailing prose','財經中心／合成記者'])
def test_v4_does_not_promote_unnamed_desk_third_party_or_unknown_signatures(signature):
    soup = page(); soup.select_one('.author_wrap').clear()
    soup.select_one('[itemprop=articleBody] > p').string = signature
    with pytest.raises(NewsError,match='article_attribution_not_verified'):
        extract_article(str(soup).encode(),policy(v4_entry()),page_url=URL)


@pytest.mark.parametrize('mutation', ['header','late','nested','hidden','reprint','unknown_body'])
def test_v4_does_not_fallback_past_ambiguous_header_first_prose_or_whole_editor(mutation):
    soup = page(); soup.select_one('.author_wrap').clear()
    first = soup.select_one('[itemprop=articleBody] > p'); first.string = '財經中心／合成記者報導'
    if mutation == 'header':
        soup.select_one('.author_wrap').string = 'unverified header credit'
    elif mutation == 'late':
        node = soup.new_tag('p'); node.string = '正文先於署名，不得從後面找一個來補'
        first.insert_before(node)
    elif mutation == 'nested':
        first.string = ''; node = soup.new_tag('span'); node.string = '財經中心／合成記者報導'; first.append(node)
    elif mutation == 'hidden':
        first['hidden'] = ''
    elif mutation == 'reprint':
        node = soup.new_tag('p'); node.string = '本文轉載自未驗證來源'; first.insert_after(node)
    else:
        node = soup.new_tag('ul'); first.insert_after(node)
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(),policy(v4_entry()),page_url=URL)


def test_v4_preserves_accepted_v3_header_fields_and_captions_except_version_identity(monkeypatch):
    for soup in (page(),page_with_outline(monkeypatch)):
        old = extract_v3(soup)
        new = extract_article(str(soup).encode(),policy(v4_entry()),page_url=URL)
        assert {k:v for k,v in old.items() if k!='article_identity_basis'} == {
            k:v for k,v in new.items() if k!='article_identity_basis'}
    assert policy(v4_entry()).data_fingerprint != policy(v3_entry()).data_fingerprint


def test_v4_http_binding_requires_the_exact_reviewed_capture_and_authorization(monkeypatch):
    source = policy(v4_entry())
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V4_HTTP_BINDING',
        (source.data_fingerprint,source.authorization_fingerprint,'a'*64,'b'*64))
    bound = policy(v4_entry())
    assert bound.http_capacity_source_fingerprint == 'a'*64
    assert bound.http_capacity_authorization_fingerprint == 'b'*64
    for key in ('scope','reviewed_on'):
        value = v4_entry(); value['authorization'][key] += ' changed'
        assert not policy(value).http_capacity_source_fingerprint
    value = v4_entry(); value['author_selectors'] = [INEWS_AUTHOR_SELECTOR]
    with pytest.raises(ValueError):
        policy(value)


def test_v4_reviewed_printed_credit_description_does_not_requeue_saved_bodies_or_reset_failures(tmp_path):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue, utc_now
    from downloader.news_fulltext_http import Response
    from test_news_failure_diagnostics import fail
    def registry(value):
        return PolicyRegistry({'contract_version':1,'collection_purpose':'personal_research',
            'publish':False,'remote_training_export':False,'providers':[value]},base=tmp_path)
    root=tmp_path/'store'; saved=URL+'1'
    with closing(NewsQueue(root,registry(v3_entry()))) as queue:
        fail(queue,Response(URL,200,{'content-type':'text/html'},str(page()).encode()),
            code='article_attribution_not_verified',received_at_utc=utc_now())
        queue.add_articles([{'url':saved}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,body_sha256=?,body_bytes=?,parquet_path=?,captured_at_utc=?,due=? WHERE url=?',
                ('complete','a'*64,123,'batches/existing.parquet',utc_now(),time_value:=10000000000,saved))
        before={r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    value=v4_entry(); value['authorization']['scope']+=' Reviewed printed named reporter; absent profile stays absent.'
    with closing(NewsQueue(root,registry(value))) as queue:
        after={r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert before==after and after[saved]['due']==time_value
