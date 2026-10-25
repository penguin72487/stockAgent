from bs4 import BeautifulSoup
import pytest
from types import SimpleNamespace

from downloader.news_source_ibmi import ibmi_credit_declarations, ibmi_validation_metadata
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry
from scripts import probe_news_sources as probes
from test_news_fulltext import config


URL = ('https://ibmi.taiwan-healthcare.org/zh/news_detail.php?'
       'REFDOCTYPID=0o4dd9ctwhtyumw0&REFDOCID=0lo73vhjeznm3et1&PageNO=954')


def document(*, day='2011-07-12', credit='[工商時報 記者 林燦澤] 2011/07/12',
             declared=URL, extra='', body='原始數值 -2.5，未做任何轉換。'):
    return BeautifulSoup(f'''<html><head>
        <meta property="og:url" content="{declared}">
        <meta property="og:title" content="原始標題｜社團法人國家生技醫療產業策進會">
        </head><body><div class="center-left">
        <div class="center-left-title">原始標題<div class="center-left-date">{day}</div></div>
        <div class="center-left-value"><p>{credit}</p><p>{body}</p></div>
        </div>{extra}</body></html>''', 'lxml')


def test_native_day_and_credit_without_body_admission_or_source_mutation():
    soup = document()
    before = str(soup)
    result = ibmi_validation_metadata(soup, page_url=URL)
    assert result['identity_status'] == 'literal_source_declaration_matches'
    assert result['title_raw'] == '原始標題'
    assert result['publication_date_raw'] == '2011-07-12'
    assert result['publication_precision'] == 'day'
    assert result['declared_media_names'] == ['工商時報']
    assert result['credit_candidates'] == ['[工商時報 記者 林燦澤] 2011/07/12']
    assert not result['article_durably_accepted']
    assert not result['body_text_retained']
    assert not result['historical_first_publication_verified']
    assert '-2.5' not in str(result)
    assert str(soup) == before


@pytest.mark.parametrize('declared', [URL.replace('PageNO=954', 'PageNO=953'),
    URL.replace('0lo73vhjeznm3et1', 'different'), URL.replace('/zh/', '/zh//'),
    URL + '#fragment', 'https://example.org/article'])
def test_equal_title_or_document_id_does_not_grant_an_alias(declared):
    result = ibmi_validation_metadata(document(declared=declared), page_url=URL)
    assert result['identity_status'] == 'unverified'
    assert not result['publication_day_valid']


@pytest.mark.parametrize('url', [URL+'&PageNO=954', URL+'&other=1', URL+'#x',
    URL.replace('PageNO=954', 'PageNO='), URL.replace('https:', 'http:'),
    URL.replace('ibmi.', 'user@ibmi.')])
def test_no_open_query_template_or_foreign_identity(url):
    assert ibmi_validation_metadata(document(declared=url), page_url=url)['failure'] == 'unsupported_literal_article_url'


@pytest.mark.parametrize('day', ['2011-02-30', '2011/07/12', '2026-09-29 21:54:47', '', '發布日不詳'])
def test_invalid_or_missing_day_remains_raw_without_metadata_clock_fallback(day):
    result = ibmi_validation_metadata(document(day=day), page_url=URL)
    assert result['publication_date_raw'] == day
    assert not result['publication_day_valid']
    assert result['publication_precision'] is None


def test_footer_credit_and_prose_mentions_do_not_invent_a_byline():
    result = ibmi_validation_metadata(document(credit='', body='企業接受工商時報採訪。',
        extra='<footer><p>[工商時報 記者 王某]</p></footer>'), page_url=URL)
    assert result['credit_candidates'] == []
    assert result['declared_media_names'] == []
    assert result['syndication_status'] == 'unresolved'


@pytest.mark.parametrize('extra', ['<div class="center-left-value">另一個正文</div>',
    '<div class="center-left"></div>', '<meta property="og:url" content="'+URL+'">'])
def test_ambiguous_native_containers_and_declarations_fail(extra):
    assert ibmi_validation_metadata(document(extra=extra), page_url=URL)['identity_status'] == 'unverified'


def test_double_slash_view_requires_its_own_exact_source_declaration():
    url = URL.replace('/zh/', '/zh//')
    assert ibmi_validation_metadata(document(declared=url), page_url=url)['identity_status'] == 'literal_source_declaration_matches'


def test_empty_visible_title_cannot_use_site_brand_or_metadata_as_fallback():
    soup = document()
    soup.select_one('.center-left-title').contents[0].extract()
    assert ibmi_validation_metadata(soup, page_url=URL)['identity_status'] == 'unverified'


@pytest.mark.parametrize('status', [200, 403])
def test_shared_probe_reads_native_fields_in_one_request_without_extraction(config, tmp_path, monkeypatch, status):
    source = config['providers'][0]
    source.update(id='ibmi', domains=['ibmi.taiwan-healthcare.org'],
        acquisition_mode='finite', finite_article_urls=[URL],
        finite_identity_query_keys=['PageNO', 'REFDOCID', 'REFDOCTYPID'],
        article_path_pattern=r'^/zh/news_detail[.]php$', allow_reviewed_supplements=False)
    for name in ('feeds', 'sitemaps', 'wordpress_apis', 'cnyes_apis', 'fulltext_feeds',
                 'historical_fulltext_feeds', 'index_pages', 'incremental_index_pages', 'metadata_feeds'):
        source[name] = []
    source.pop('cnyes_public_index', None)
    registry = PolicyRegistry(config, base=tmp_path)
    calls = []
    def get(url):
        calls.append(url)
        return Response(url, status, {'content-type': 'text/html'}, str(document()).encode())
    monkeypatch.setattr(probes, 'extract_article', lambda *_args, **_kwargs: pytest.fail('metadata cannot extract a body'))
    http = SimpleNamespace(registry=registry, provider_not_before={}, robot_delays={}, get=get)
    result = probes.probe_provider(http, [URL], metadata_only=True)[0]
    assert calls == [URL]
    assert not result['article_durably_accepted']
    assert result['article_extraction'] == 'not_run'
    assert 'extracted' not in result and 'private_diagnostic_path' not in result
    assert ('ibmi_native_validation' in result) is (status == 200)
    if status == 200:
        assert result['ibmi_native_validation']['publication_date_raw'] == '2011-07-12'


@pytest.mark.parametrize('markup', [
    '<span>【工商時報 記者 林某】</span><br>正文',
    '【工商時報 記者 林某】<br>正文',
    '<span>【工商時報 <b>記者</b> 林某】正文</span>',
    '<p>2011-07-12 【工商時報 記者 林某】正文</p>',
    '<span>【2011-07-12 工商時報 記者 林某】正文</span>'])
def test_inline_signature_prefix_is_visible_metadata_without_following_prose(markup):
    soup = document(credit='')
    body = soup.select_one('.center-left-value')
    body.clear()
    for node in list(BeautifulSoup(markup, 'lxml').body.contents):
        body.append(node)
    result = ibmi_validation_metadata(soup, page_url=URL)
    assert result['metadata_contract_version'] == 4
    assert result['native_credit_structure']['direct_child_shapes'][0]['signature_prefix_found']
    assert '工商時報' in result['declared_media_names']
    assert all('正文' not in credit for credit in result['credit_candidates'])
    assert not result['body_text_retained'] and not result['article_durably_accepted']


def test_description_or_script_signature_cannot_supply_visible_authorship():
    soup = document(credit='', body='普通正文')
    meta = soup.new_tag('meta', property='og:description', content='【工商時報 記者 林某】正文')
    soup.head.append(meta)
    script = soup.new_tag('script')
    script.string = '【工商時報 記者 林某】'
    soup.select_one('.center-left-value').insert(0, script)
    result = ibmi_validation_metadata(soup, page_url=URL)
    assert result['credit_candidates'] == []
    assert result['syndication_status'] == 'unresolved'
    assert result['native_credit_structure']['description_signature_prefix'] == '【工商時報 記者 林某】'
    assert not result['native_credit_structure']['description_signature_is_visible_byline']


def test_unclosed_or_ordinary_quoted_lead_is_not_a_signature_or_body_export():
    for body in ('【工商時報 記者 林某 正文沒有結尾', '【企業展望】工商時報報導', '【企業引用工商時報】正文'):
        soup = document(credit='')
        soup.select_one('.center-left-value').string = body
        result = ibmi_validation_metadata(soup, page_url=URL)
        assert result['credit_candidates'] == []
        assert result['syndication_status'] == 'unresolved'


@pytest.mark.parametrize('markup', [
    '<!--【工商時報 記者 林某】-->',
    '<p hidden>【工商時報 記者 林某】</p>',
    '<span aria-hidden="true">【工商時報 記者 林某】</span>',
    '<div style="display:none !important"><p>【工商時報 記者 林某】</p></div>',
    '<span style="visibility:hidden">【工商時報 記者 林某】</span>'])
def test_hidden_or_comment_signatures_are_not_native_visible_credit(markup):
    soup = document(credit='', body='普通正文')
    body = soup.select_one('.center-left-value')
    for node in reversed(list(BeautifulSoup('<section>'+markup+'</section>', 'lxml').section.contents)):
        body.insert(0, node)
    result = ibmi_validation_metadata(soup, page_url=URL)
    assert result['credit_candidates'] == []
    assert result['syndication_status'] == 'unresolved'


def test_inline_native_characters_are_not_separated_by_invented_spaces():
    soup = document(credit='')
    body = soup.select_one('.center-left-value')
    body.clear()
    markup = '<p>[<span>經濟</span><span>日報</span> 記者<span>甲</span><span>乙</span>] 2011/05/24</p>'
    body.append(BeautifulSoup(markup, 'lxml').p)
    result = ibmi_validation_metadata(soup, page_url=URL)
    assert result['credit_candidates'] == ['[經濟日報 記者甲乙] 2011/05/24']
    assert result['declared_media_names'] == ['經濟日報']


def test_hidden_inline_names_do_not_turn_a_description_into_a_visible_signature():
    soup = document(credit='')
    body = soup.select_one('.center-left-value')
    body.clear()
    body.append(BeautifulSoup('<p>[<span hidden>工商時報</span> 記者甲乙]</p>', 'lxml').p)
    result = ibmi_validation_metadata(soup, page_url=URL)
    assert result['declared_media_names'] == []
    assert result['syndication_status'] == 'unresolved'


def test_inline_reporter_name_keeps_exact_characters_without_a_second_spaced_variant():
    soup = document(credit='')
    body = soup.select_one('.center-left-value')
    body.clear()
    body.append(BeautifulSoup('<p>[工商時報 記者<span>甲</span><span>乙</span>]</p>', 'lxml').p)
    assert ibmi_validation_metadata(soup, page_url=URL)['credit_candidates'] == ['[工商時報 記者甲乙]']


def test_unrecognized_header_is_bounded_evidence_without_person_or_media_inference():
    soup = document(credit='')
    body = soup.select_one('.center-left-value')
    body.clear()
    body.append(BeautifulSoup('<p>【未核實通訊社 記者甲乙】普通正文</p>', 'lxml').p)
    result = ibmi_validation_metadata(soup, page_url=URL)
    assert result['credit_candidates'] == []
    assert result['declared_media_names'] == []
    assert result['native_credit_structure']['unrecognized_native_header_candidates'] == ['【未核實通訊社 記者甲乙】']


@pytest.mark.parametrize('media', ['中時電子報', '自立電子報', '時報資訊', '太平洋日報', '自由時報'])
def test_saved_native_headers_can_be_classified_offline_without_a_rights_or_alias_grant(media):
    raw = f'【{media} 記者甲乙】2011/05/24'
    result = ibmi_credit_declarations([raw, raw])
    assert result['credit_candidates'] == [raw]
    assert result['declared_media_names'] == [media]
    assert result['syndication_status'] == 'declared_third_party_credit'
    assert not result['original_authorship_verified'] and not result['body_authorization_granted']


@pytest.mark.parametrize('candidates', ['工商時報', [''] , ['a' * 241], ['來源:甲'] * 25])
def test_credit_replay_requires_bounded_native_observations(candidates):
    with pytest.raises(ValueError, match='bounded_native_credit_candidates_required'):
        ibmi_credit_declarations(candidates)
