"""Synthetic legacy DOM only; publisher prose and credentials are not fixtures."""
from copy import deepcopy
from contextlib import closing
import json
from pathlib import Path
import re
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import extract_article_response, fetch_article, load_config
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_idn import (
    IDN_BODY_SELECTOR, IDN_ORIGIN, IDN_PATH_PATTERN, idn_article_id, idn_validation_metadata,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


URL = IDN_ORIGIN + '/news/news_content.aspx?catid=3&catsid=1&catdid=0&artid=20110124kofang013'
TITLE = '合成股數 -12.50 與比率 0% 原值不變'
COPYRIGHT = 'Copyright © 2012 自立晚報. All rights reserved.  版權所有，禁止擅自轉貼節錄'


@pytest.fixture
def settings():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [{
        'id': 'idn', 'domains': ['www.idn.com.tw'], 'enabled': True,
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'interval_seconds': 10, 'parallelism': 1, 'html_parser': 'lxml',
        'embedded_article_layout': 'idn_visible_article_v1',
        'article_path_pattern': IDN_PATH_PATTERN, 'article_selectors': [IDN_BODY_SELECTOR],
        'exclude_selectors': [], 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-21',
            'evidence_url': IDN_ORIGIN + '/',
            'scope': 'Private known reporter originals only; no discovery, media, license, export or reuse grant.',
            'automated_access_review': 'no_explicit_prohibition_identified'},
    }]
    return config


def page():
    prose = ''.join(f'第{i}段合成原始值。' + '保留股數負值零值及比率。' * 5 + '<br><br>' for i in range(3))
    return f'''<html><head><title>自立晚報 - {TITLE}</title></head><body>
      <table><tr><td class="body_9gc">2026-10-21-星期三</td></tr></table>
      <table><tr><td class="headnewsd">{TITLE}</td></tr></table>
      <table><tr><td class="newsa" colspan="2"><!-- table width="1">不是可見正文 -->
        【記者柯安聰台北報導】{prose}<div align="center"><img src="../productimage_s/synthetic.jpg"></div>
        (圖)圖片旁的合成文字仍是正文。原值 -12.50 和 0%。（自立電子報2011/01/24）<br>
      </td></tr></table>
      <table><tr><td class="body_9g"></td></tr><tr><td class="body_9g">{COPYRIGHT}</td></tr></table>
      <form action="/news/article_search.aspx"><input name="q"></form>
      <p>推薦另一篇、網站聯絡與搜尋不是正文。</p></body></html>'''


def parse(settings, html=None, *, url=URL):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['idn']
    return extract_article((page() if html is None else html).encode(), source, page_url=url)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_whole_native_br_prose_attribution_values_not_header_calendar(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    result = parse(settings)
    assert result['title'] == TITLE and json.loads(result['authors_json']) == ['柯安聰']
    assert result['published_at_raw'] == '2011/01/24' and result['modified_at_raw'] is None
    assert 'date_only' in result['published_time_basis'] and 'no_intraday_time' in result['published_time_basis']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert result['declared_canonical_url'] is None and result['extraction_method'] == 'css:' + IDN_BODY_SELECTOR
    for i in range(3):
        assert result['body'].count(f'第{i}段') == 1
    for value in ('【記者柯安聰台北報導】', '-12.50', '0%', '(圖)圖片旁的合成文字仍是正文。', '（自立電子報2011/01/24）'):
        assert value in result['body']
    for value in ('星期三', '2026-10-21', '不是可見正文', 'table width', '推薦', '網站聯絡', '版權所有'):
        assert value not in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    assert notices[0]['clock_precision'] == 'date_only' and not notices[0]['intraday_time_or_timezone_inferred']
    assert notices[1]['text'] == COPYRIGHT and not notices[1]['open_license_or_bulk_permission_inferred']
    assert notices[2]['kind'] == 'unfetched_inline_image' and not notices[2]['media_requested']
    assert not json.loads(result['author_attribution_detail_json'])['reporter_real_identity_independently_verified']


@pytest.mark.parametrize('clock', ['(自立電子報2011/01/24)', '（自立電子報2026/2/9）',
    '（自立電子報2026/10/20）', '(自立晚報2014/4/17)'])
def test_native_trailing_date_variants_are_literal_not_url_inference(settings, clock):
    result = parse(settings, page().replace('（自立電子報2011/01/24）', clock))
    assert result['published_at_raw'] == re.search(r'[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}', clock)[0]
    assert clock in result['body']


@pytest.mark.parametrize('old,new,code', [
    ('<title>自立晚報 - ', '<title>其他來源 - ', 'article_identity_not_verified'),
    ('class="headnewsd"', 'class="headnewsd" hidden', 'article_identity_not_verified'),
    ('class="newsa"', 'class="newsa" style="display:none"', 'article_identity_not_verified'),
    ('第1段', '<div aria-hidden="true">第1段</div>', 'article_identity_not_verified'),
    ('第1段', '<div class="sr-only">第1段</div>', 'article_identity_not_verified'),
    ('第1段', '<script>unknown()</script>第1段', 'article_identity_not_verified'),
    ('第1段', '<iframe src="/other"></iframe>第1段', 'article_identity_not_verified'),
    ('第1段', '<form>其他查詢</form>第1段', 'article_identity_not_verified'),
    ('【記者柯安聰台北報導】', '【中央社台北報導】', 'article_rights_or_scope_excluded'),
    ('【記者柯安聰台北報導】', '【記者另一人台北報導】', 'article_rights_or_scope_excluded'),
    ('【記者柯安聰台北報導】', '', 'article_attribution_not_verified'),
    ('第1段', '轉載自其他媒體。第1段', 'article_rights_or_scope_excluded'),
    ('（自立電子報2011/01/24）', '(自立電子報2011/02/30)', 'invalid_bulk_publication_clock'),
    ('（自立電子報2011/01/24）', '(自立電子報2011/01/24）', 'invalid_bulk_publication_clock'),
    ('（自立電子報2011/01/24）', '（自立電子報2011/01/24 09:00）', 'invalid_bulk_publication_clock'),
    ('（自立電子報2011/01/24）', '', 'invalid_bulk_publication_clock'),
    ('（自立電子報2011/01/24）', '（自立電子報2011/01/24）後面還有別的內容', 'invalid_bulk_publication_clock'),
    (COPYRIGHT, '全部免費開放授權', 'article_rights_not_verified'),
    ('class="body_9g">Copyright', 'hidden class="body_9g">Copyright', 'article_identity_not_verified'),
])
@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_unreviewed_identity_byline_hidden_prose_and_clock_rejected(settings, old, new, code, parser):
    settings['providers'][0]['html_parser'] = parser
    assert old in page()
    with pytest.raises(NewsError) as caught:
        parse(settings, page().replace(old, new))
    assert caught.value.code == code


@pytest.mark.parametrize('selector', ['headnewsd', 'newsa'])
def test_ambiguous_native_regions_are_not_concatenated(settings, selector):
    html = page().replace('</body>', f'<table><tr><td class="{selector}">其他內容</td></tr></table></body>')
    with pytest.raises(NewsError):
        parse(settings, html)


@pytest.mark.parametrize('update', [
    {'domains': ['idn.com.tw', 'www.idn.com.tw']}, {'article_selectors': ['td.newsa']},
    {'article_path_pattern': '^/.*'}, {'require_author_attribution': False},
    {'require_configured_container': False}, {'exclude_selectors': ['div']},
    {'title_selector': 'td.headnewsd'}, {'feeds': [IDN_ORIGIN + '/rss']},
])
def test_native_scope_cannot_be_silently_broadened(settings, update):
    settings['providers'][0].update(update)
    with pytest.raises(ValueError):
        PolicyRegistry(settings, base=Path.cwd())


@pytest.mark.parametrize('url', [URL + '&artid=20260101kofang999', URL + '&other=1', URL + '#part',
    URL.replace('https:', 'http:'), URL.replace('www.', ''), URL.replace('catid=3', 'catid=%33'),
    URL.replace('kofang', '../kofang'), URL.replace('artid=', 'ARTID='), URL.replace('20110124', 'abc')])
@pytest.mark.parametrize('version', [1, 2, 3])
def test_unreviewed_literal_routes_never_reach_http(settings, url, version):
    settings = versioned(settings, version)
    assert idn_article_id(url, requested=True) is None
    def forbidden(*args, **kwargs):
        raise AssertionError('must not make an unreviewed request')
    with pytest.raises(NewsError):
        fetch_article(SimpleNamespace(registry=PolicyRegistry(settings, base=Path.cwd()), get=forbidden), {'url': url})


@pytest.mark.parametrize('version', [1, 2, 3])
def test_only_literal_response_is_bound_no_category_alias_or_redirect(settings, version):
    settings = versioned(settings, version)
    registry = PolicyRegistry(settings, base=Path.cwd())
    response = Response(URL, 200, {'content-type': 'text/html'}, page().encode())
    http = SimpleNamespace(registry=registry, get=lambda *args, **kwargs: response)
    result = fetch_article(http, {'url': URL, 'id': b'\x01' * 16})
    assert result['requested_url'] == result['final_url'] == URL
    assert result['url_resolution_basis'] == 'requested_public_url' and result['raw_response_body'] == response.body
    for different in (URL.replace('catid=3', 'catid=4'), URL.replace('kofang013', 'kofang014')):
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            fetch_article(http, {'url': different, 'id': b'\x02' * 16})
    hop = RedirectHop(URL, 302, URL, URL, '2026-10-21T00:21:00+00:00', 'd'*64, 0)
    http.get = lambda *args, **kwargs: Response(URL, 200, response.headers, response.body, (hop,))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(http, {'url': URL, 'id': b'\x01' * 16})


def test_optional_publisher_canonical_must_be_exact(settings):
    html = page().replace('</head>', f'<link rel="canonical" href="{URL}"></head>')
    assert parse(settings, html)['title'] == TITLE
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, html.replace('href="'+URL, 'href="'+URL+'&catid=9'))


def test_failure_diagnosis_keeps_only_bounded_native_declarations(settings):
    soup = BeautifulSoup(page().replace('柯安聰', '另一人'), 'lxml')
    before = str(soup)
    facts = idn_validation_metadata(soup, page_url=URL)
    assert str(soup) == before and facts['body_count'] == 1
    assert facts['body_structures'][0]['native_credit'] == '【記者另一人台北報導】'
    assert facts['body_structures'][0]['trailing_publisher_clock'] == '（自立電子報2011/01/24）'
    assert not facts['body_text_retained'] and not facts['article_identity_or_scope_granted']
    assert facts['additional_requests'] == 0 and '第1段' not in json.dumps(facts)


def test_changed_attribution_does_not_become_a_complete_article(settings):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['idn']
    response = Response(URL, 200, {'content-type': 'text/html'}, page().replace('柯安聰', '其他人').encode())
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article_response(response, source)


def test_diagnostic_lexical_variants_do_not_broaden_article_admission(settings):
    html = page().replace('【記者柯安聰台北報導】', '（記者柯安聰台北報導）').replace(
        '（自立電子報2011/01/24）', '(自立電子報 2011/01/24）')
    facts = idn_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    structure = facts['body_structures'][0]
    assert structure['native_credit'] is None and structure['trailing_publisher_clock'] is None
    assert structure['leading_credit_declarations'] == ['（記者柯安聰台北報導）']
    assert structure['closing_date_declarations'] == ['(自立電子報 2011/01/24）']
    assert structure['lexical_declarations_are_not_verified_credit_or_release_clocks']
    assert '第1段' not in json.dumps(facts, ensure_ascii=False)
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        parse(settings, html)


def test_non_article_diagnostic_records_fixed_label_not_arbitrary_response(settings):
    html = '<html><body>Service Unavailable private-token arbitrary origin message</body></html>'
    facts = idn_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    diagnostic = facts['non_article_document']
    assert diagnostic['recognized_operational_labels'] == ['Service Unavailable']
    assert diagnostic['visible_characters'] > 20 and not diagnostic['text_retained']
    assert not diagnostic['source_outcome_classified']
    assert 'private-token' not in json.dumps(facts) and 'arbitrary' not in json.dumps(facts)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, html)


@pytest.mark.parametrize('prefix,only_markers,codepoints,caption', [
    ('\ufeff\u200b\u2060', True, ['U+200B', 'U+2060', 'U+FEFF'], False),
    ('(圖)私有圖說', False, [], True), ('私有前導文字', False, [], False)])
def test_diagnostic_precredit_shape_does_not_retain_or_admit_prefix(
        settings, prefix, only_markers, codepoints, caption):
    html = page().replace('【記者柯安聰台北報導】', prefix+'【記者柯安聰台北報導】')
    soup = BeautifulSoup(html, 'lxml')
    before = str(soup)
    facts = idn_validation_metadata(soup, page_url=URL)
    structure = facts['body_structures'][0]
    assert facts['contract_version'] == 4 and str(soup) == before
    assert structure['first_credit_offset'] == structure['precredit_characters'] == len(prefix)
    assert structure['precredit_only_whitespace_or_noncontent_markers'] is only_markers
    assert structure['precredit_noncontent_codepoints'] == codepoints
    assert structure['precredit_photo_caption_marker'] is caption
    assert '私有' not in json.dumps(facts, ensure_ascii=False)
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        parse(versioned(settings, 2), html)


def test_static_error_diagnostic_never_executes_follows_or_retains_script(settings):
    script = "alert('查無此新聞 private-token');history.go(-1);location.href='https://secret.invalid/token';"
    html = '<html><script>'+script+'</script></html>'
    soup = BeautifulSoup(html, 'lxml')
    before = str(soup)
    facts = idn_validation_metadata(soup, page_url=URL)
    record = facts['non_article_document']['static_scripts'][0]
    assert record['characters'] == len(script) and len(record['sha256_utf8']) == 64
    assert '查無此新聞' in record['recognized_operational_labels']
    assert record['has_alert_call'] and record['has_history_negative_one_call']
    assert record['has_location_reference'] and not record['has_history_back_call']
    assert not record['executed_or_followed'] and not record['text_retained']
    assert str(soup) == before and facts['additional_requests'] == 0
    assert all(value not in json.dumps(facts) for value in ('private-token', 'secret.invalid', 'location.href'))


def test_static_diagnostics_bound_script_scans_and_results():
    html = '<html>'+''.join('<script>'+('x'*4100)+"alert('查無此新聞');</script>" for _ in range(10))+'</html>'
    facts = idn_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    document = facts['non_article_document']
    assert document['scripts_truncated'] and len(document['static_scripts']) == 8
    assert all(row['scan_truncated'] and not row['recognized_operational_labels']
               and not row['has_alert_call'] for row in document['static_scripts'])


def test_precredit_image_structure_is_metadata_not_prose_or_parser_permission(settings):
    prefix = '<div align="center"><img src="/not-requested.jpg"></div><br>私有原始短行<br>'
    html = page().replace('【記者柯安聰台北報導】', prefix+'【記者柯安聰台北報導】')
    facts = idn_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    structure = facts['body_structures'][0]['image_prefaced_credit_structure']
    assert structure['native_credit'] == '【記者柯安聰台北報導】'
    assert structure['prefix_characters'] == len('私有原始短行') and len(structure['prefix_sha256_utf8']) == 64
    assert not structure['article_admission_granted'] and not structure['prefix_text_retained']
    assert '私有原始短行' not in json.dumps(facts, ensure_ascii=False)
    assert not facts['body_text_retained'] and facts['additional_requests'] == 0
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        parse(versioned(settings, 2), html)


@pytest.mark.parametrize('prefix', ['正文前導', '【其他作者】', '記者其他人', '新聞來源：第三人', '轉載其他內容'])
def test_precredit_metadata_does_not_generalize_unreviewed_prefix(settings, prefix):
    fragment = '<div align="center"><img src="/not-requested.jpg"></div><br>'+prefix+'<br>'
    if prefix == '正文前導':
        fragment = fragment.replace('<div align="center"><img src="/not-requested.jpg"></div><br>', '')
    html = page().replace('【記者柯安聰台北報導】', fragment+'【記者柯安聰台北報導】')
    facts = idn_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    assert facts['body_structures'][0]['image_prefaced_credit_structure'] is None


def test_static_nonarticle_CJK_notice_is_literal_and_never_a_retrieval_permission():
    html = '<script>alert("查無您要找的資料！");location.href="/secret";alert("private-token");</script>'
    facts = idn_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    record = facts['non_article_document']['static_scripts'][0]
    assert record['short_CJK_alert_notices'] == ['查無您要找的資料！']
    assert record['short_notice_is_not_article_or_permanent_absence_proof']
    assert not record['executed_or_followed'] and facts['additional_requests'] == 0
    assert all(value not in json.dumps(facts) for value in ('private-token', '/secret'))


def test_candidate_obeys_shared_installed_source_contract(settings, monkeypatch):
    import test_news_fulltext as shared
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config)
    candidate['providers'] = [entry for entry in config['providers'] if entry['id'] != 'idn'] + settings['providers']
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


def test_unknown_credit_diagnostic_reuses_shared_budget_and_keeps_no_raw_body(settings, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_failure_diagnostics import fail, reports
    from test_news_failure_identity_metadata import evidence
    registry = PolicyRegistry(settings, base=Path.cwd())
    response = Response(URL, 200, {'content-type': 'text/html', 'set-cookie': 'private-cookie'},
        page().replace('柯安聰', '其他人').encode())
    with closing(NewsQueue(tmp_path, registry)) as queue:
        for _ in range(3):
            fail(queue, response, code='article_rights_or_scope_excluded',
                 received_at_utc='2026-10-21T00:21:00+00:00')
        paths, rows = evidence(tmp_path)
        assert len(rows) == 2
        assert all(row['facts']['idn_native_validation']['body_structures'][0]['native_credit']
                   == '【記者其他人台北報導】' for row in rows)
        assert all(not row['accepted_as_article'] and not row['raw_response_retained'] for row in rows)
        assert all('第1段' not in p.read_text() and 'private-cookie' not in p.read_text() for p in paths)
        assert not reports(tmp_path)[0]
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_validation_metadata_attempts:%'").fetchone()[0] == '2'


def test_operator_probe_observes_unreviewed_credit_without_body_or_refetch(settings, tmp_path, capsys):
    from scripts.probe_news_sources import probe_provider
    registry = PolicyRegistry(settings, base=Path.cwd())
    requests = []
    def get(url):
        requests.append(url)
        return Response(url, 200, {'content-type': 'text/html'}, page().replace('柯安聰', '其他人').encode())
    http = SimpleNamespace(registry=registry, provider_not_before={}, robot_delays={}, get=get)
    result = probe_provider(http, [URL], output_root=tmp_path)
    assert requests == [URL] and result[0]['extraction_error'] == 'article_rights_or_scope_excluded'
    assert result[0]['idn_native_validation']['body_structures'][0]['native_credit'] == '【記者其他人台北報導】'
    assert '第1段' not in json.dumps(result, ensure_ascii=False) and '第1段' not in capsys.readouterr().out
    assert not list(tmp_path.iterdir())


def versioned(settings, version):
    result = deepcopy(settings)
    result['providers'][0]['embedded_article_layout'] = f'idn_visible_article_v{version}'
    return result


@pytest.mark.parametrize('name', ['柯安聰', '郭襄陽', '郭宜均', '謝政儒'])
def test_v2_native_reviewed_reporters_not_filename_or_verified_person(settings, name):
    html = page().replace('柯安聰', name)
    result = parse(versioned(settings, 2), html)
    assert json.loads(result['authors_json']) == [name] and f'【記者{name}台北報導】' in result['body']
    assert not json.loads(result['author_attribution_detail_json'])['reporter_real_identity_independently_verified']
    if name != '柯安聰':
        with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
            parse(settings, html)


@pytest.mark.parametrize('clock', ['(自立晚報20181205)', '(自立電子報2015/12/23)'])
def test_v2_actual_date_declaration_can_precede_unmodified_caption(settings, clock):
    html = page().replace('（自立電子報2011/01/24）', clock+' (圖)來源原有的圖說 -8.00 股')
    result = parse(versioned(settings, 2), html)
    assert result['published_at_raw'] == re.search(r'[0-9][0-9/]+', clock)[0]
    assert clock+' (圖)來源原有的圖說 -8.00 股' in result['body']
    assert not result['historical_point_in_time'] and 'date_only' in result['published_time_basis']


def test_v2_missing_release_date_is_null_not_discarded_body_or_invented_clock(settings):
    html = page().replace('（自立電子報2011/01/24）', '')
    result = parse(versioned(settings, 2), html)
    assert result['published_at_raw'] is None and 'unknown_release_clock' in result['published_time_basis']
    assert 'body_acquisition_not_release_clock_or_training_acceptance' in result['published_time_basis']
    assert not result['historical_point_in_time']
    assert json.loads(result['verified_article_notices_json'])[0]['clock_precision'] == 'unknown'
    assert '第0段' in result['body'] and '(圖)圖片旁的合成文字仍是正文。' in result['body']
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        parse(settings, html)


@pytest.mark.parametrize('clock', ['(自立晚報20180230)', '(自立電子報2011/02/30)',
    '(自立電子報2011/01/24）', '(自立電子報2011/01/24 09:00)',
    '(自立晚報20181205) (自立晚報20181206)'])
def test_v2_malformed_or_ambiguous_declared_date_does_not_become_unknown(settings, clock):
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        parse(versioned(settings, 2), page().replace('（自立電子報2011/01/24）', clock))


@pytest.mark.parametrize('credit', ['【中央社台北報導】', '【記者另一人台北報導】',
    '【記者柯安聰、其他作者台北報導】', '圖說【記者柯安聰台北報導】'])
def test_v2_does_not_guess_other_credits_or_precredit_content(settings, credit):
    html = page().replace('【記者柯安聰台北報導】', credit)
    with pytest.raises(NewsError):
        parse(versioned(settings, 2), html)


@pytest.mark.parametrize('name', ['柯安聰', '郭襄陽', '郭宜均', '謝政儒'])
def test_v3_first_image_short_line_and_credit_preserve_whole_native_body(settings, name):
    preface = '<div align="center"><img src="/unfetched-prefix.jpg"></div><br>原始短行 -12.5 股 0%<br>'
    html = page().replace('【記者柯安聰台北報導】', preface+f'【記者{name}台北報導】')
    result = parse(versioned(settings, 3), html)
    assert '原始短行 -12.5 股 0%' in result['body'] and f'【記者{name}台北報導】' in result['body']
    assert result['body'].index('原始短行') < result['body'].index('【記者')
    assert '第0段' in result['body'] and '第2段' in result['body']
    assert result['published_at_raw'] == '2011/01/24' and json.loads(result['authors_json']) == [name]
    notices = json.loads(result['verified_article_notices_json'])
    witness = next(row for row in notices if row['kind'] == 'publisher_native_precredit_image_and_short_line')
    assert witness['prefix_prose_preserved_in_body'] and not witness['image_or_caption_ownership_inferred']
    assert sum(row['kind'] == 'unfetched_inline_image' for row in notices) == 2


@pytest.mark.parametrize('preface', ['任意前導<br>',
    '<div align="center"><img src="x"></div><br>【其他作者】<br>',
    '<div align="center"><img src="x"></div><br>轉載自他處<br>',
    '<div align="center"><img src="x"><img src="y"></div><br>短行<br>',
    '<div align="center">其他正文<img src="x"></div><br>短行<br>',
    '<div align="center"><img src="x"></div><br>短行<br>多一行<br>',
    '<div align="center"><img src="x"></div><br>'+('很'*161)+'<br>'])
def test_v3_never_turns_arbitrary_precredit_prose_into_attribution(settings, preface):
    html = page().replace('【記者柯安聰台北報導】', preface+'【記者柯安聰台北報導】')
    with pytest.raises(NewsError):
        parse(versioned(settings, 3), html)


@pytest.mark.parametrize('obstruction', [None, 'body_sha256', 'parquet_path', 'lease_owner', 'lease_until',
    'missing_observation', 'future_observation', 'unknown_prior', 'unreviewed_scope'])
@pytest.mark.parametrize('version', [2, 3])
def test_pinned_native_repair_only_version_specific_failures_preserves_all_evidence(settings, tmp_path, monkeypatch, obstruction, version):
    from downloader import news_fulltext_idn as native
    from downloader.news_fulltext_queue import NewsQueue, utc_now
    old = PolicyRegistry(versioned(settings, version-1), base=Path.cwd())
    upgraded = versioned(settings, version)
    old_source = old.sources['idn']
    unpinned = PolicyRegistry(upgraded, base=Path.cwd()).sources['idn']
    monkeypatch.setattr(native, f'IDN_V{version}_SCOPE_REPAIR_IDENTITY', (old_source.data_fingerprint, unpinned.data_fingerprint))
    monkeypatch.setattr(native, f'IDN_V{version}_PRIOR_HTTP_MEASUREMENT_IDENTITY',
        (old_source.http_capacity_source_fingerprint or old_source.data_fingerprint,
         old_source.http_capacity_authorization_fingerprint or old_source.authorization_fingerprint))
    if obstruction == 'unreviewed_scope':
        upgraded['providers'][0]['authorization']['scope'] += '; unreviewed change'
    new = PolicyRegistry(upgraded, base=Path.cwd())
    if obstruction != 'unreviewed_scope':
        assert new.sources['idn'].http_capacity_source_fingerprint == old_source.data_fingerprint
        assert new.sources['idn'].http_capacity_authorization_fingerprint == old_source.authorization_fingerprint
    before = {}
    budget = 'failure_validation_metadata_attempts:v1:idn:invalid_bulk_publication_clock:20261021'
    with closing(NewsQueue(tmp_path, old)) as queue:
        for i, state in enumerate(('invalid_bulk_publication_clock', 'article_rights_or_scope_excluded',
            'article_attribution_not_verified', 'article_identity_not_verified', 'forbidden', 'complete')):
            url = URL.replace('kofang013', f'kofang{i:03d}')
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,'
                'last_checked_at_utc=? WHERE url=?', (state, state, utc_now(), url))
            if obstruction in {'body_sha256', 'parquet_path', 'lease_owner', 'lease_until'}:
                queue.db.execute(f'UPDATE articles SET {obstruction}=? WHERE url=?',
                    (9000000001 if obstruction == 'lease_until' else 'protected', url))
            elif obstruction in {'missing_observation', 'future_observation'}:
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?',
                    (None if obstruction == 'missing_observation' else '2099-01-01T00:00:00+00:00', url))
            before[url] = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        if obstruction == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:idn'", ('f'*64,))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (budget, '2'))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        for url, row in before.items():
            repairable = ({'invalid_bulk_publication_clock', 'article_rights_or_scope_excluded'}
                          if version == 2 else {'article_attribution_not_verified'})
            changed = obstruction is None and row['state'] in repairable
            expected = {**row, 'state': 'retry'} if changed else row
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == expected
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (budget,)).fetchone()[0] == '2'
        for url, row in before.items():
            queue.db.execute('UPDATE articles SET state=? WHERE url=?', (row['state'], url))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert all(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == row
                   for url, row in before.items())
