"""FuHouse native contracts, with original synthetic prose and no news fixture."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_setn import (
    FUHOUSE_AUTHOR_SELECTOR, FUHOUSE_BODY_SELECTOR, FUHOUSE_LAYOUT, FUHOUSE_PROVIDER,
    FUHOUSE_TITLE_SELECTOR, FUHOUSE_VERIFICATION, SETN_PATH_PATTERN,
    FUHOUSE_V2_LAYOUT, FUHOUSE_V2_BODY_SELECTOR, FUHOUSE_V2_AUTHOR_SELECTOR, FUHOUSE_V2_VERIFICATION,
    FUHOUSE_V3_LAYOUT, FUHOUSE_V3_BODY_SELECTOR, FUHOUSE_V3_VERIFICATION,
    FUHOUSE_V4_LAYOUT, FUHOUSE_V4_BODY_SELECTOR, FUHOUSE_V4_VERIFICATION,
)

URL = 'https://fuhouse.setn.com/news/123456'
TITLE = '自行編寫的房產測試標題'
TEXT = '合成段落：成交量1000股、價格12.34、負數-5、比率2.3%，全部原值保留。' * 5


def entry():
    return {'id': FUHOUSE_PROVIDER, 'domains': ['fuhouse.setn.com'], 'enabled': True,
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'embedded_article_layout': FUHOUSE_LAYOUT, 'adapter_verification': FUHOUSE_VERIFICATION,
        'html_parser': 'lxml', 'interval_seconds': 10, 'parallelism': 1,
        'article_path_pattern': SETN_PATH_PATTERN, 'article_selectors': [FUHOUSE_BODY_SELECTOR],
        'title_selector': FUHOUSE_TITLE_SELECTOR, 'author_selectors': [FUHOUSE_AUTHOR_SELECTOR],
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research', 'evidence_url': 'https://fuhouse.setn.com/',
            'reviewed_on': '2026-10-24', 'automated_access_review': 'no_explicit_prohibition_identified',
            'scope': 'Private known public originals only; not a publishing, training or export licence.'}}


def policy(value=None):
    return PolicyRegistry({'contract_version': 1, 'publish': False, 'remote_training_export': False,
        'collection_purpose': 'personal_research',
        'providers': [value or entry()]}, base=Path('.')).sources[FUHOUSE_PROVIDER]


def page():
    full = TITLE + ' | FuHouse.SETN富房網'
    return BeautifulSoup(f'''<html><head><title>{full}</title>
      <link rel="canonical" href="{URL}"><meta property="og:url" content="{URL}">
      <meta property="og:type" content="article"><meta property="og:title" content="{full}">
      <meta name="Title" content="{full}"><meta name="author" content="三立新聞網">
      <meta name="source" content="三立新聞網"><meta property="dable:item_id" content="123456">
      <meta property="article:published_time" content="2020-03-02T09:01:00Z">
      <meta name="Description" content="NOT_ARTICLE_SUMMARY"></head><body>
      <nav>NOT_ARTICLE_NAV</nav><div class="newspage_mainbox contLeft" itemscope
       itemtype="https://schema.org/NewsArticle"><h1 itemprop="headline">{TITLE}</h1>
       <div class="date-box"><div class="article_remark_wrap"><div class="author_wrap"></div>
        <div class="page-date">2020/03/02 09:01</div></div>
        <div class="magicword hidden-print">NOT_ARTICLE_TOOLS</div></div>
       <article><div class="ckuse" id="ckuse"><div itemprop="articleBody">
        <p>財經中心／綜合報導</p><p style="text-align:center"><img src="https://example.org/photo.jpg"
         alt="ALT_IS_NOT_A_CAPTION"></p><p>合成圖說（圖／測試提供）</p><p>{TEXT}</p>
        <p><strong>合成結尾與風險提示</strong><br>不能遺漏的第二行。</p>
       </div><div class="ad-area dfpAD" id="gad_div-gpt-ad-artcle_bottom" style="text-align:center"></div>
       </div></article><div class="newspage_newslist">NOT_ARTICLE_RELATED</div></div></body></html>''', 'lxml')


def extract(soup=None, url=URL):
    return extract_article(str(soup or page()).encode(), policy(), page_url=url)


def test_whole_native_paragraphs_preserve_desk_missing_person_source_clocks_and_values():
    result = extract()
    assert result['body'].count(TEXT) == 1
    assert result['body'].startswith('財經中心／綜合報導')
    assert '合成圖說（圖／測試提供）' in result['body']
    assert result['body'].endswith('合成結尾與風險提示\n不能遺漏的第二行。')
    assert 'NOT_ARTICLE_' not in result['body'] and 'ALT_IS_NOT_A_CAPTION' not in result['body']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['source_declared_desk'] == '財經中心'
    assert detail['source_declared_person_name'] is None and detail['source_declared_profile_url'] is None
    assert not detail['human_reporter_identity_verified'] and detail['meta_author_is_publisher_not_person']
    notices = json.loads(result['verified_article_notices_json'])
    assert result['published_at_raw'] == '2020-03-02T09:01:00Z' and result['modified_at_raw'] is None
    assert notices['visible_current_article_clock_raw'] == '2020/03/02 09:01'
    assert notices['visible_clock_precision'] == 'minute' and notices['meta_clock_precision'] == 'second'
    assert notices['meta_timezone_offset_raw'] == 'Z' and not notices['visible_timezone_declared']
    assert not notices['exact_PIT_eligible'] and not notices['same_wall_minute_is_same_instant_proof']


@pytest.mark.parametrize('url', [URL + '?page=2', URL + '#body', URL + '/',
    URL.replace('https:', 'http:'), URL.replace('fuhouse.', 'inews.'), URL.replace('fuhouse.', 'www.')])
def test_no_unwitnessed_article_or_subsite_alias(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(url=url)


@pytest.mark.parametrize('selector,attribute,value', [
    ('link[rel=canonical]', 'href', URL + '7'), ('meta[property="og:url"]', 'content', URL + '7'),
    ('meta[property="dable:item_id"]', 'content', '999999'),
    ('meta[name=author]', 'content', '合成人名'), ('meta[name=source]', 'content', '另一來源'),
    ('meta[property="og:title"]', 'content', '另一標題'),
    ('.newspage_mainbox', 'itemtype', 'https://schema.org/WebPage'),
    ('[itemprop=articleBody]', 'hidden', ''), ('[itemprop=articleBody] > p', 'onclick', 'bad()'),
    ('[itemprop=articleBody] > p', 'style', 'display: none'),
])
def test_conflicting_identity_hidden_or_executable_body_fails_closed(selector, attribute, value):
    soup = page(); soup.select_one(selector)[attribute] = value
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('markup', ['<div>有遺漏的未知正文容器</div>', '<table><tr><td>123</td></tr></table>',
    '<p><iframe src="https://example.org/embed"></iframe></p>', '<p><script>bad()</script></p>',
    '<p><a href="javascript:bad()">合成連結</a></p>'])
def test_unknown_written_structure_is_not_silently_dropped(markup):
    soup = page(); soup.select_one('[itemprop=articleBody]').append(BeautifulSoup(markup, 'lxml').body.contents[0])
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('signature', ['記者合成人名／台北報導', '中央社記者合成人名', '綜合報導', '財經中心／合成人名報導'])
def test_unwitnessed_credit_shape_does_not_invent_person(signature):
    soup = page(); soup.select_one('[itemprop=articleBody] > p').string = signature
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(soup)


def test_explicit_reprint_marker_is_not_promoted_to_local_original():
    soup = page(); soup.select_one('[itemprop=articleBody]').append(BeautifulSoup('<p>新聞來源：另一媒體</p>', 'lxml').p)
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup)


@pytest.mark.parametrize('selector,value', [('.page-date', '2020/03/02 09:02'),
    ('.page-date', '2020/02/30 09:01'), ('.page-date', '2020/03/02 09:01:00'),
    ('meta[property="article:published_time"]', '2020-03-02T09:01:00+08:00'),
    ('meta[property="article:published_time"]', '2020-02-30T09:01:00Z')])
def test_actual_clock_declarations_must_be_valid_and_preserved(selector, value):
    soup = page(); node = soup.select_one(selector)
    if node.name == 'meta': node['content'] = value
    else: node.string = value
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract(soup)


@pytest.mark.parametrize('change', [
    {'article_selectors': ['body']}, {'author_selectors': ['meta[name=author]']},
    {'exclude_selectors': ['p:last-child']}, {'allow_short_configured_body': True},
    {'domains': ['inews.setn.com']}, {'adapter_verification': 'unreviewed'},
    {'require_author_attribution': False}, {'feeds': ['https://fuhouse.setn.com/rss']},
])
def test_only_reviewed_native_known_url_recipe_can_be_registered(change):
    value = deepcopy(entry()); value.update(change)
    with pytest.raises(ValueError):
        policy(value)


def test_only_exact_reviewed_source_and_authorization_can_inherit_http_identity(monkeypatch):
    value = entry(); unbound = policy(value)
    source_fp, auth_fp = '1' * 64, '2' * 64
    monkeypatch.setattr('downloader.news_fulltext_setn.FUHOUSE_HTTP_BINDING',
        (unbound.data_fingerprint, unbound.authorization_fingerprint, source_fp, auth_fp))
    approved = policy(value)
    assert approved.http_capacity_source_fingerprint == source_fp
    assert approved.http_capacity_authorization_fingerprint == auth_fp
    changed = deepcopy(value); changed['authorization']['scope'] += ' Changed scope.'
    assert policy(changed).http_capacity_authorization_fingerprint != auth_fp
    changed = deepcopy(value); changed['interval_seconds'] = 5
    assert policy(changed).http_capacity_source_fingerprint != source_fp


def test_installed_catalog_passes_full_loader_and_keeps_original_http_identity():
    from downloader.news_fulltext import load_config
    from downloader.news_fulltext_setn import FUHOUSE_HTTP_BINDING
    config, registry, _ = load_config(Path('configs/news_fulltext.json'))
    source = registry.sources[FUHOUSE_PROVIDER]
    configured = next(p for p in config['providers'] if p['id'] == FUHOUSE_PROVIDER)
    assert configured['note'] and source.known_url_only
    assert source.authorization_fingerprint == FUHOUSE_HTTP_BINDING[1]
    assert (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint) == FUHOUSE_HTTP_BINDING[2:]
    if source.embedded_article_layout == FUHOUSE_LAYOUT:
        assert source.data_fingerprint == FUHOUSE_HTTP_BINDING[0]
    else:
        assert source.embedded_article_layout in {FUHOUSE_V2_LAYOUT, FUHOUSE_V3_LAYOUT, FUHOUSE_V4_LAYOUT}
        assert source.parser_repair_capture_history[-1] == FUHOUSE_HTTP_BINDING[0]
        assert len(source.parser_repair_capture_history) == int(source.embedded_article_layout.rsplit('_v',1)[1])-1


def v2_entry(value=None):
    result = deepcopy(value or entry())
    result.update(embedded_article_layout=FUHOUSE_V2_LAYOUT, adapter_verification=FUHOUSE_V2_VERIFICATION,
        article_selectors=[FUHOUSE_V2_BODY_SELECTOR], author_selectors=[FUHOUSE_V2_AUTHOR_SELECTOR])
    return result


def v2_extract(soup=None):
    return extract_article(str(soup or page()).encode(), policy(v2_entry()), page_url=URL)


def header_figure_page():
    soup = page(); soup.select_one('[itemprop=articleBody] > p').string = ' '
    soup.select_one('.author_wrap').append(BeautifulSoup(
        '記者 <a href="https://www.setn.com/author/test-reporter" itemprop="author" target="_blank">測試記者</a> 綜合報導',
        'lxml').body)
    # Keep the source's direct text / anchor structure, without a body wrapper.
    soup.select_one('.author_wrap > body').unwrap()
    image = soup.select_one('[itemprop=articleBody] > p:nth-child(2)')
    image.replace_with(BeautifulSoup('<figure><picture><img src="https://example.org/image.jpg" '
        'alt="ALT_DOES_NOT_DUPLICATE_THE_WRITTEN_CAPTION"></picture>'
        '<figcaption contenteditable="false">合成圖說：-123股、3.2%（圖／測試提供）</figcaption></figure>', 'lxml').figure)
    return soup


def test_v2_header_reporter_profile_caption_and_original_values_are_preserved_once():
    result = v2_extract(header_figure_page())
    assert result['body'].count('合成圖說：-123股、3.2%（圖／測試提供）') == 1
    assert 'ALT_DOES_NOT' not in result['body'] and result['body'].count(TEXT) == 1
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['source_declared_person_name'] == '測試記者'
    assert detail['source_declared_person_role'] == '記者' and detail['source_declared_desk'] is None
    assert detail['source_declared_profile_url'] == 'https://www.setn.com/author/test-reporter'
    assert not detail['printed_signature_preserved_in_article_body'] and not detail['profile_requested']
    assert not detail['human_reporter_identity_verified'] and not detail['original_ownership_verified']
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_written_caption_count'] == 1 and not notices['exact_PIT_eligible']


def test_v2_printed_desk_name_does_not_invent_reporter_role_or_profile():
    soup = page(); soup.select_one('[itemprop=articleBody] > p').string = '財經中心／合成姓名'
    result = v2_extract(soup); detail = json.loads(result['author_attribution_detail_json'])
    assert result['body'].startswith('財經中心／合成姓名')
    assert detail['source_declared_desk'] == '財經中心'
    assert detail['source_declared_person_name'] == '合成姓名'
    assert detail['source_declared_person_role'] is None and detail['source_declared_profile_url'] is None
    assert not detail['human_reporter_identity_verified'] and detail['source_header_credit_absent']
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(soup)


def test_v2_preserves_the_whole_v1_original_representation():
    old, new = extract(), v2_extract()
    for key in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw',
            'author_attribution_detail_json', 'verified_article_notices_json', 'body_completeness'):
        assert old[key] == new[key]


@pytest.mark.parametrize('selector,attr,value', [
    ('.author_wrap a', 'href', 'https://example.org/person'),
    ('.author_wrap a', 'itemprop', 'publisher'),
    ('.author_wrap a', 'onclick', 'run()'),
    ('figure', 'hidden', ''), ('figcaption', 'style', 'display:none')])
def test_v2_rejects_unverified_header_or_hidden_caption(selector, attr, value):
    soup = header_figure_page(); soup.select_one(selector)[attr] = value
    with pytest.raises(NewsError):
        v2_extract(soup)


@pytest.mark.parametrize('markup', [
    '<figure><picture><img></picture></figure>',
    '<figure>UNKNOWN_WRITTEN_VALUE<picture><img></picture><figcaption>合成圖說</figcaption></figure>',
    '<figure><picture><img></picture><figcaption><figure>未解析</figure></figcaption></figure>',
    '<figure><picture><img></picture><figcaption><script>run()</script>合成圖說</figcaption></figure>',
    '<figure><picture><img></picture><figcaption>合成圖說</figcaption><p>不能遺漏</p></figure>'])
def test_v2_rejects_unaccounted_figure_text_or_subtree(markup):
    soup = header_figure_page()
    soup.select_one('figure').replace_with(BeautifulSoup(markup, 'lxml').figure)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        v2_extract(soup)


def test_v2_retains_prior_capture_and_exact_http_binding_without_broadening_authorization(monkeypatch):
    old = policy(); original_source_fp, original_auth_fp = '3' * 64, '4' * 64
    monkeypatch.setattr('downloader.news_fulltext_setn.FUHOUSE_HTTP_BINDING',
        (old.data_fingerprint, old.authorization_fingerprint, original_source_fp, original_auth_fp))
    current = policy(v2_entry())
    assert current.parser_repair_capture_history == (old.data_fingerprint,)
    assert current.http_capacity_source_fingerprint == original_source_fp
    assert current.http_capacity_authorization_fingerprint == original_auth_fp
    changed = v2_entry(); changed['authorization']['scope'] += ' Changed authority.'
    assert policy(changed).http_capacity_source_fingerprint != original_source_fp
    changed = v2_entry(); changed['interval_seconds'] = 5
    assert policy(changed).http_capacity_source_fingerprint != original_source_fp


def test_v2_queue_upgrade_preserves_failure_attempts_deadlines_and_scope_exclusions(tmp_path):
    from downloader.news_fulltext_queue import NewsQueue, native_body_only_parser_repair
    old, new = policy(), policy(v2_entry())
    assert native_body_only_parser_repair(new, old.data_fingerprint)
    registry_config = {'contract_version':1,'collection_purpose':'personal_research',
        'publish':False,'remote_training_export':False}
    old_registry = PolicyRegistry({**registry_config,'providers':[entry()]},base=Path('.'))
    new_registry = PolicyRegistry({**registry_config,'providers':[v2_entry()]},base=Path('.'))
    q = NewsQueue(tmp_path, old_registry)
    try:
        with q.db:
            for suffix, state in [('111111','article_identity_not_verified'),
                    ('222222','article_attribution_not_verified'),('333333','article_rights_or_scope_excluded')]:
                url = 'https://fuhouse.setn.com/news/' + suffix
                q.add_articles([{'url':url}])
                q.db.execute('UPDATE articles SET state=?,attempts=3,due=12345,error_code=? WHERE url=?',
                    (state,state,url))
        fields = 'id,url,state,attempts,due,error_code,body_sha256,lease_owner,lease_until'
        prior = [tuple(r) for r in q.db.execute('SELECT '+fields+' FROM articles ORDER BY url')]
    finally: q.close()
    q = NewsQueue(tmp_path, new_registry)
    try:
        assert [tuple(r) for r in q.db.execute('SELECT '+fields+' FROM articles ORDER BY url')] == prior
    finally: q.close()


def v3_entry(value=None):
    result = v2_entry(value)
    result.update(embedded_article_layout=FUHOUSE_V3_LAYOUT, adapter_verification=FUHOUSE_V3_VERIFICATION,
        article_selectors=[FUHOUSE_V3_BODY_SELECTOR])
    return result


def v3_extract(soup=None):
    return extract_article(str(soup or page()).encode(), policy(v3_entry()), page_url=URL)


@pytest.mark.parametrize('signature,name,role,desk', [
    ('記者合成姓名／台北報導','合成姓名','記者',None),
    ('財經中心／合成姓名報導','合成姓名',None,'財經中心')])
def test_v3_literal_printed_roles_preserve_source_signature(signature,name,role,desk):
    soup = page(); soup.select_one('[itemprop=articleBody] > p').string = signature
    result = v3_extract(soup); detail = json.loads(result['author_attribution_detail_json'])
    assert result['body'].startswith(signature) and result['body'].count(TEXT)==1
    assert detail['source_visible_printed_signature_raw']==signature
    assert detail['source_declared_person_name']==name and detail['source_declared_person_role']==role
    assert detail['source_declared_desk']==desk and detail['source_declared_profile_url'] is None
    assert not detail['human_reporter_identity_verified'] and not detail['profile_requested']
    with pytest.raises(NewsError,match='article_attribution_not_verified'): v2_extract(soup)


def hybrid_page():
    soup = header_figure_page()
    soup.select_one('.author_wrap').contents[-1].replace_with(' 報導')
    soup.select_one('[itemprop=articleBody] > p').string = '記者測試記者／台北報導'
    return soup


def test_v3_header_profile_and_printed_signature_must_agree_and_both_remain_raw():
    result = v3_extract(hybrid_page()); detail = json.loads(result['author_attribution_detail_json'])
    assert detail['source_visible_header_signature_raw']=='記者 測試記者 報導'
    assert detail['source_visible_printed_signature_raw']=='記者測試記者／台北報導'
    assert detail['source_declared_printed_location_raw']=='台北'
    assert detail['source_declared_person_role']=='記者' and detail['printed_signature_preserved_in_article_body']
    assert result['body'].startswith('記者測試記者／台北報導')
    with pytest.raises(NewsError,match='article_attribution_not_verified'): v2_extract(hybrid_page())


@pytest.mark.parametrize('mismatch', ['name','location','no_printed','nested_signature','foreign_profile'])
def test_v3_rejects_inconsistent_hybrid_or_unwitnessed_blank_location(mismatch):
    soup = hybrid_page(); first=soup.select_one('[itemprop=articleBody] > p')
    if mismatch=='name': first.string='記者另一姓名／台北報導'
    elif mismatch=='location': soup.select_one('.author_wrap').contents[-1].replace_with(' 台南報導')
    elif mismatch=='no_printed': first.string=' '
    elif mismatch=='nested_signature': first.append(soup.new_tag('strong'))
    else: soup.select_one('.author_wrap a')['href']='https://example.org/author/test-reporter'
    with pytest.raises(NewsError,match='article_attribution_not_verified'): v3_extract(soup)


def outline_page():
    soup=page(); body=soup.select_one('[itemprop=articleBody]')
    markup=BeautifulSoup('<div id="nav-directory"><p><strong>導覽目錄：</strong></p><ul>'
        '<li><a href="#nav-title-123">合成第一節</a></li><li><a href="#nav-title-456">合成第二節</a></li>'
        '</ul></div><p id="nav-title-123">合成第一節</p><h3 data-start="1" data-end="2">合成小標</h3>'
        '<p id="nav-title-456">合成第二節</p>','lxml')
    for node in list(markup.body.contents): body.append(node)
    return soup


def test_v3_written_headings_and_matched_outline_navigation_are_distinct():
    result=v3_extract(outline_page())
    for text in ('合成第一節','合成第二節','合成小標'): assert result['body'].count(text)==1
    assert '導覽目錄' not in result['body']
    notices=json.loads(result['verified_article_notices_json'])
    assert notices['native_written_heading_count']==1
    outline,=notices['matched_native_in_page_outlines']
    assert outline['ordered_local_heading_ids']==['nav-title-123','nav-title-456']
    assert not outline['links_requested_or_accepted_as_URL_aliases']
    with pytest.raises(NewsError,match='article_identity_not_verified'): v2_extract(outline_page())


@pytest.mark.parametrize('mutation', ['label','target','duplicate','order','remote','missing_heading',
    'hidden_heading','hidden_outline','script','unique_outline_prose','nested_heading','empty_h3'])
def test_v3_no_silent_loss_or_invented_native_outline(mutation):
    soup=outline_page()
    if mutation=='label': soup.select_one('#nav-directory strong').string='相關文章：'
    elif mutation=='target': soup.select_one('#nav-directory a').string='不相同的標題'
    elif mutation=='duplicate': soup.select_one('[itemprop=articleBody]').append(BeautifulSoup('<p id="nav-title-123">重複</p>','lxml').p)
    elif mutation=='order': soup.select_one('#nav-directory a')['href']='#nav-title-456'
    elif mutation=='remote': soup.select_one('#nav-directory a')['href']='https://example.org/#nav-title-123'
    elif mutation=='missing_heading': soup.select_one('#nav-title-123').decompose()
    elif mutation=='hidden_heading': soup.select_one('#nav-title-123')['hidden']=''
    elif mutation=='hidden_outline': soup.select_one('#nav-directory')['style']='display:none'
    elif mutation=='script': soup.select_one('h3')['onclick']='bad()'
    elif mutation=='unique_outline_prose': soup.select_one('#nav-directory').append('不能遺漏的獨有正文')
    elif mutation=='nested_heading': soup.select_one('h3').append(soup.new_tag('section'))
    else: soup.select_one('h3').string=' '
    with pytest.raises(NewsError,match='article_identity_not_verified'): v3_extract(soup)


@pytest.mark.parametrize('factory', [page,header_figure_page])
def test_v3_prior_accepted_representations_have_identical_values(factory):
    old,new=v2_extract(factory()),v3_extract(factory())
    for key in ('body','body_sha256','authors_json','published_at_raw','modified_at_raw',
            'author_attribution_detail_json','verified_article_notices_json','body_completeness','article_identity_basis'):
        assert old[key]==new[key]


def test_v3_http_bucket_inherits_exact_v2_v1_ancestry_without_authority_changes(monkeypatch):
    old=policy(); v2=policy(v2_entry())
    monkeypatch.setattr('downloader.news_fulltext_setn.FUHOUSE_HTTP_BINDING',
        (old.data_fingerprint,old.authorization_fingerprint,'3'*64,'4'*64))
    current=policy(v3_entry())
    assert current.parser_repair_capture_history==(v2.data_fingerprint,old.data_fingerprint)
    assert current.parser_repair_prior_capture_fingerprint==v2.data_fingerprint
    assert (current.http_capacity_source_fingerprint,current.http_capacity_authorization_fingerprint)==('3'*64,'4'*64)
    altered=v3_entry(); altered['authorization']['scope']+=' changed'
    assert policy(altered).http_capacity_source_fingerprint!='3'*64


@pytest.mark.parametrize('old_entry', [v2_entry])
def test_v3_upgrade_preserves_original_failures_attempts_due_and_exclusions(tmp_path,old_entry):
    from downloader.news_fulltext_queue import NewsQueue, native_body_only_parser_repair
    settings={'contract_version':1,'collection_purpose':'personal_research','publish':False,'remote_training_export':False}
    old=PolicyRegistry({**settings,'providers':[old_entry()]},base=Path('.'))
    new=PolicyRegistry({**settings,'providers':[v3_entry()]},base=Path('.'))
    # Only the immediately preceding version is an implicit body-only repair.
    assert native_body_only_parser_repair(new.sources[FUHOUSE_PROVIDER],old.sources[FUHOUSE_PROVIDER].data_fingerprint)==(old_entry is v2_entry)
    q=NewsQueue(tmp_path,old)
    try:
        with q.db:
            for i,state in enumerate(('article_identity_not_verified','article_attribution_not_verified','article_rights_or_scope_excluded','not_found'),1):
                url='https://fuhouse.setn.com/news/'+str(i)
                q.add_articles([{'url':url}])
                q.db.execute('UPDATE articles SET state=?,attempts=7,due=12345,error_code=? WHERE url=?',(state,state,url))
        fields='id,url,state,attempts,due,error_code,body_sha256,lease_owner,lease_until'
        before=[tuple(r) for r in q.db.execute('SELECT '+fields+' FROM articles ORDER BY url')]
    finally: q.close()
    q=NewsQueue(tmp_path,new)
    try: assert [tuple(r) for r in q.db.execute('SELECT '+fields+' FROM articles ORDER BY url')]==before
    finally: q.close()


def v4_entry(value=None):
    result=v3_entry(value)
    result.update(embedded_article_layout=FUHOUSE_V4_LAYOUT,adapter_verification=FUHOUSE_V4_VERIFICATION,
        article_selectors=[FUHOUSE_V4_BODY_SELECTOR])
    return result


def v4_extract(soup=None):
    return extract_article(str(soup or page()).encode(),policy(v4_entry()),page_url=URL)


def strong_credit_page():
    soup=page(); first=soup.select_one('[itemprop=articleBody] > p')
    first.clear(); first.append(BeautifulSoup('<strong>財經中心／合成姓名報導</strong>','lxml').strong)
    return soup


def test_v4_plain_strong_printed_credit_preserves_literal_name_and_missing_role():
    result=v4_extract(strong_credit_page()); detail=json.loads(result['author_attribution_detail_json'])
    assert result['body'].startswith('財經中心／合成姓名報導')
    assert detail['source_visible_printed_signature_raw']=='財經中心／合成姓名報導'
    assert detail['source_printed_signature_markup']=='single_plain_strong_whole_first_paragraph'
    assert detail['source_declared_person_name']=='合成姓名' and detail['source_declared_person_role'] is None
    assert detail['source_declared_profile_url'] is None and not detail['human_reporter_identity_verified']
    with pytest.raises(NewsError,match='article_attribution_not_verified'): v3_extract(strong_credit_page())


@pytest.mark.parametrize('mutation',['partial','multiple','nested','attributes','hidden','script','third_party'])
def test_v4_strong_credit_does_not_hide_conflicts_or_invent_a_signature(mutation):
    soup=strong_credit_page(); first=soup.select_one('[itemprop=articleBody] > p'); strong=first.strong
    if mutation=='partial': first.append('另一人名')
    elif mutation=='multiple': first.append(soup.new_tag('strong'))
    elif mutation=='nested': strong.string.wrap(soup.new_tag('span'))
    elif mutation=='attributes': strong['data-unreviewed']='value'
    elif mutation=='hidden': strong['hidden']=''
    elif mutation=='script': strong['onclick']='bad()'
    else: strong.string='中央社記者合成姓名報導'
    with pytest.raises(NewsError): v4_extract(soup)


def written_list_page():
    soup=page(); body=soup.select_one('[itemprop=articleBody]')
    body.append(BeautifulSoup('<h2>合成原生小標</h2><ul><li><strong>合成第一項</strong>：-123股、1.25%。</li>'
        '<li><strong>合成第二項</strong>：原始數值0。<br>第二項內不能遺漏的後段文字。</li></ul>','lxml').body)
    body.find('body',recursive=False).unwrap()
    return soup


def test_v4_h2_and_inline_list_preserve_every_value_in_source_order_without_invented_bullets():
    result=v4_extract(written_list_page())
    assert result['body'].endswith('合成原生小標\n\n合成第一項\n：-123股、1.25%。\n\n合成第二項\n：原始數值0。\n第二項內不能遺漏的後段文字。')
    for text in (TEXT,'合成原生小標','第二項內不能遺漏的後段文字。'): assert result['body'].count(text)==1
    notices=json.loads(result['verified_article_notices_json'])
    description,=notices['native_written_prose_lists']
    assert description['source_entry_count']==2 and not description['list_number_labels_invented']
    assert not description['navigation_or_related_content_excluded']
    assert not notices['exact_PIT_eligible'] and notices['native_written_heading_count']==1
    with pytest.raises(NewsError,match='article_identity_not_verified'): v3_extract(written_list_page())


@pytest.mark.parametrize('mutation',['unknown_block','nested_list','empty_list','empty_li','list_attributes',
    'li_attributes','outside_list_prose','hidden_li','unknown_inline','empty_h2','hidden_h2','nested_h2','li_outside_list'])
def test_v4_written_blocks_cannot_be_silently_dropped_or_promoted_to_navigation(mutation):
    soup=written_list_page(); body=soup.select_one('[itemprop=articleBody]'); listing=body.ul
    if mutation=='unknown_block': listing.li.append(soup.new_tag('div'))
    elif mutation=='nested_list': listing.li.append(BeautifulSoup('<ul><li>巢狀清單</li></ul>','lxml').ul)
    elif mutation=='empty_list': listing.clear()
    elif mutation=='empty_li': listing.li.clear()
    elif mutation=='list_attributes': listing['class']=['related']
    elif mutation=='li_attributes': listing.li['id']='unreviewed'
    elif mutation=='outside_list_prose': listing.append('不能遺漏的清單外文字')
    elif mutation=='hidden_li': listing.li['hidden']=''
    elif mutation=='unknown_inline': listing.li.append(soup.new_tag('iframe'))
    elif mutation=='empty_h2': body.h2.clear()
    elif mutation=='hidden_h2': body.h2['hidden']=''
    elif mutation=='nested_h2': body.h2.append(soup.new_tag('section'))
    else: body.append(soup.new_tag('li'))
    with pytest.raises(NewsError,match='article_identity_not_verified'): v4_extract(soup)


def inline_outline_page():
    soup=outline_page()
    for key in ('123','456'):
        heading=soup.select_one('#nav-title-'+key); heading.name='strong'; heading.wrap(soup.new_tag('p'))
    # The matched heading stays in its paragraph with any following prose.
    soup.select_one('#nav-title-456').parent.append('標題後的原始敘述。')
    return soup


def test_v4_inline_outline_reuses_existing_ordered_local_identity_without_losing_prose():
    result=v4_extract(inline_outline_page()); outline,=json.loads(result['verified_article_notices_json'])['matched_native_in_page_outlines']
    assert outline['ordered_target_tags']==['strong','strong']
    assert result['body'].count('合成第一節')==1 and result['body'].count('合成第二節')==1
    assert '標題後的原始敘述。' in result['body'] and '導覽目錄' not in result['body']
    with pytest.raises(NewsError,match='article_identity_not_verified'): v3_extract(inline_outline_page())


@pytest.mark.parametrize('mutation',['different_label','duplicate_id','nested','preceding_prose','foreign_target','wrong_order','hidden'])
def test_v4_inline_outline_keeps_existing_failure_boundaries(mutation):
    soup=inline_outline_page(); heading=soup.select_one('#nav-title-123')
    if mutation=='different_label': heading.string='不符的標題'
    elif mutation=='duplicate_id': soup.select_one('[itemprop=articleBody]').append(BeautifulSoup('<p id="nav-title-123">重複</p>','lxml').p)
    elif mutation=='nested': heading.string.wrap(soup.new_tag('span'))
    elif mutation=='preceding_prose': heading.insert_before('不是首個可見節點')
    elif mutation=='foreign_target': soup.select_one('#nav-directory a')['href']='https://example.org/#nav-title-123'
    elif mutation=='wrong_order': soup.select_one('#nav-directory a')['href']='#nav-title-456'
    else: heading['style']='display:none'
    with pytest.raises(NewsError,match='article_identity_not_verified'): v4_extract(soup)


@pytest.mark.parametrize('factory',[page,header_figure_page,hybrid_page,outline_page])
def test_v4_all_previously_accepted_representations_remain_identical(factory):
    old,new=v3_extract(factory()),v4_extract(factory())
    for key in ('body','body_sha256','authors_json','published_at_raw','modified_at_raw',
            'author_attribution_detail_json','verified_article_notices_json','body_completeness','article_identity_basis'):
        assert old[key]==new[key]


def test_v4_http_bucket_preserves_v3_v2_v1_ancestry_and_original_controls(monkeypatch):
    original=policy()
    monkeypatch.setattr('downloader.news_fulltext_setn.FUHOUSE_HTTP_BINDING',
        (original.data_fingerprint,original.authorization_fingerprint,'3'*64,'4'*64))
    previous=policy(v3_entry()); current=policy(v4_entry())
    assert current.parser_repair_capture_history==(previous.data_fingerprint,policy(v2_entry()).data_fingerprint,original.data_fingerprint)
    assert current.parser_repair_prior_capture_fingerprint==previous.data_fingerprint
    assert (current.http_capacity_source_fingerprint,current.http_capacity_authorization_fingerprint)==('3'*64,'4'*64)
    changed=v4_entry(); changed['authorization']['scope']+=' changed'
    assert policy(changed).http_capacity_source_fingerprint!='3'*64


@pytest.mark.parametrize('old_entry',[entry,v2_entry,v3_entry])
def test_v4_upgrade_preserves_failures_attempts_deadlines_and_exclusions(tmp_path,old_entry):
    from downloader.news_fulltext_queue import NewsQueue,native_body_only_parser_repair
    settings={'contract_version':1,'collection_purpose':'personal_research','publish':False,'remote_training_export':False}
    old=PolicyRegistry({**settings,'providers':[old_entry()]},base=Path('.'))
    new=PolicyRegistry({**settings,'providers':[v4_entry()]},base=Path('.'))
    assert native_body_only_parser_repair(new.sources[FUHOUSE_PROVIDER],old.sources[FUHOUSE_PROVIDER].data_fingerprint)==(old_entry is v3_entry)
    q=NewsQueue(tmp_path,old)
    try:
        with q.db:
            for i,state in enumerate(('article_identity_not_verified','article_attribution_not_verified','article_rights_or_scope_excluded','not_found'),1):
                url='https://fuhouse.setn.com/news/'+str(i); q.add_articles([{'url':url}])
                q.db.execute('UPDATE articles SET state=?,attempts=7,due=12345,error_code=? WHERE url=?',(state,state,url))
        fields='id,url,state,attempts,due,error_code,body_sha256,lease_owner,lease_until'
        before=[tuple(r) for r in q.db.execute('SELECT '+fields+' FROM articles ORDER BY url')]
    finally: q.close()
    q=NewsQueue(tmp_path,new)
    try: assert [tuple(r) for r in q.db.execute('SELECT '+fields+' FROM articles ORDER BY url')]==before
    finally: q.close()
