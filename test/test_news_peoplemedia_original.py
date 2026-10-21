"""Synthetic PeopleMedia DOM; never commit source prose as a test fixture."""
from copy import deepcopy
from contextlib import closing
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_peoplemedia import (
    PEOPLEMEDIA_BODY_SELECTOR, PEOPLEMEDIA_ORIGIN, PEOPLEMEDIA_PATH_PATTERN, peoplemedia_article_id,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, native_text_parser_repair, utc_now

URL = PEOPLEMEDIA_ORIGIN + '/news/01234567-89ab-cdef-0123-456789abcdef'
TITLE = '合成原值 -12.50 與股數 0%'
PUBLISHED = '2014-10-21 21:03:32+08:00'
MODIFIED = '2014-12-01T07:30:26Z+08:00'


@pytest.fixture
def settings():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [{
        'id': 'peoplemedia', 'domains': ['www.peoplemedia.tw'], 'enabled': True,
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'interval_seconds': 2, 'parallelism': 1, 'html_parser': 'lxml',
        'embedded_article_layout': 'peoplemedia_visible_article_v1',
        'article_path_pattern': PEOPLEMEDIA_PATH_PATTERN, 'article_selectors': [PEOPLEMEDIA_BODY_SELECTOR],
        'exclude_selectors': [], 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-21',
            'evidence_url': PEOPLEMEDIA_ORIGIN + '/copyright',
            'scope': 'Private known public native text only, not third-party, bulk, media or export rights.',
            'automated_access_review': 'no_explicit_prohibition_identified'},
    }]
    return config


def page(*, credit='陳文蔚／綜合報導', modified=MODIFIED):
    prose = ''.join(f'<p>第{i}段合成正文 -12.50 與 0%。' + '保留原數字股數與比率。' * 5 + '</p>' for i in range(3))
    return f'''<html><head><title>{TITLE} | 民報 PeopleNews</title>
      <link rel="canonical" href="{URL}"><link rel="canonical" href="{URL}">
      <meta property="og:title" content="【民報】{TITLE}"><meta property="og:type" content="article">
      <meta property="og:url" content="{URL}"><meta property="article:published_time" content="{PUBLISHED}">
      <meta property="article:modified_time" content="{modified}"></head><body>
      <div id="news" itemscope itemtype="http://schema.org/NewsArticle">
        <meta itemprop="mainEntityOfPage" itemid="{URL}">
        <span itemprop="publisher"><meta itemprop="name" content="民報"><meta itemprop="url" content="{URL}"></span>
        <h1 class="news_title" itemprop="headline">{TITLE}</h1>
        <div id="news_author"><span class="author" itemprop="author"><span itemprop="name">{credit}</span></span>
          <span class="date">2014-10-21 21:03</span><meta itemprop="datePublished" content="{PUBLISHED}">
          <meta itemprop="dateModified" content="{modified}"></div>
        <div id="news_content"><div id="newsphoto"><img src="/hero.jpg"><div id="newsphoto_title">圖文不同權利人</div></div>
          <div id="newscontent" itemprop="articleBody">{prose}<p><img src="/inline.jpg">圖片旁的字保留</p>
            <div id="dablewidget_37Jvj47N"></div><script>dable('renderWidget', 'dablewidget_37Jvj47N');</script>
          </div></div><div id="news_neighbors">相關文章不是正文</div></div></body></html>'''


def parse(settings, html=None, *, url=URL):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['peoplemedia']
    return extract_article((page() if html is None else html).encode(), source, page_url=url)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_native_prose_raw_values_and_invalid_revision_are_preserved(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    result = parse(settings)
    assert result['title'] == TITLE and json.loads(result['authors_json']) == ['陳文蔚／綜合報導']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    clocks = json.loads(result['verified_article_notices_json'])[0]
    assert not clocks['modified_clock_valid'] and clocks['invalid_revision_not_coerced']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    for i in range(3):
        assert result['body'].count(f'第{i}段') == 1
    assert '-12.50' in result['body'] and '0%' in result['body'] and '圖片旁的字保留' in result['body']
    for value in ('dable', '相關文章', '圖文不同權利人'):
        assert value not in result['body']


def test_editorial_label_is_not_coerced_to_a_person(settings):
    result = parse(settings, page(credit='財經中心／綜合報導', modified=PUBLISHED))
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['editorial_center_label'] and detail['role'].endswith('not_verified_person')
    assert json.loads(result['verified_article_notices_json'])[0]['modified_clock_valid']


@pytest.mark.parametrize('edit,code', [
    (lambda s: s.replace('href="' + URL, 'href="' + URL + '0', 1), 'article_identity_not_verified'),
    (lambda s: s.replace('【民報】' + TITLE, '另一篇文章'), 'article_identity_not_verified'),
    (lambda s: s.replace('2014-10-21 21:03</span>', '2014-10-21 21:04</span>'), 'invalid_bulk_publication_clock'),
    (lambda s: s.replace('陳文蔚／綜合報導', '中央社／轉載'), 'article_rights_or_scope_excluded'),
    (lambda s: s.replace('id="newscontent"', 'id="newscontent" hidden'), 'article_identity_not_verified'),
    (lambda s: s.replace('<p>第0段', '<p style="display:none">第0段'), 'article_identity_not_verified'),
    (lambda s: s.replace("dable('renderWidget', 'dablewidget_37Jvj47N');", 'unknown()'), 'article_identity_not_verified'),
    (lambda s: s.replace('id="dablewidget_37Jvj47N"></div>', 'id="dablewidget_37Jvj47N">新增推薦</div>'), 'article_identity_not_verified'),
    (lambda s: s.replace('<p>第0段', '<p>轉載自其他網站 第0段'), 'article_rights_or_scope_excluded'),
    (lambda s: s.replace('<p>第0段', '<iframe></iframe><p>第0段'), 'article_identity_not_verified'),
    (lambda s: s.replace('<p>第0段', '<p class="d-none">第0段'), 'article_identity_not_verified'),
])
def test_unproved_native_identity_visibility_and_rights_fail(settings, edit, code):
    with pytest.raises(NewsError, match='^' + code + '$'):
        parse(settings, edit(page()))


@pytest.mark.parametrize('url', [URL.replace('www.', ''), URL + '?x=1', URL + '/extra',
    URL.replace('/news/', '/preview/'), URL.replace('https:', 'http:')])
def test_no_guessed_alias_namespace_or_query(url):
    assert peoplemedia_article_id(url) is None


@pytest.mark.parametrize('field,value', [('domains', ['peoplemedia.tw']), ('article_selectors', ['body']),
    ('exclude_selectors', ['p']), ('feeds', [PEOPLEMEDIA_ORIGIN + '/feed']), ('require_author_attribution', False)])
def test_configuration_cannot_broaden_reviewed_scope(settings, field, value):
    settings['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(settings, base=Path.cwd())


def test_fetch_rejects_a_different_final_uuid(settings):
    registry = PolicyRegistry(settings, base=Path.cwd())
    http = SimpleNamespace(registry=registry,
        get=lambda url, **kwargs: Response(URL[:-1] + '0', 200, {'content-type': 'text/html'}, page().encode()))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(http, {'url': URL})


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_metadata_observes_native_credit_and_clock_without_dom_mutation_or_prose(settings, parser):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_peoplemedia import peoplemedia_validation_metadata
    html = page(credit='未審核記者／合成署名')
    soup = BeautifulSoup(html, parser)
    before = str(soup)
    facts = peoplemedia_validation_metadata(soup, page_url=URL)
    assert str(soup) == before
    assert facts['article_scope_count'] == facts['native_body_count'] == 1
    assert facts['native_credits']['values'][0]['value'] == '未審核記者／合成署名'
    assert facts['native_published']['values'][0]['value'] == PUBLISHED
    assert facts['native_modified']['values'][0]['value'] == MODIFIED
    assert facts['canonical_urls']['node_count'] == 2
    assert facts['canonical_urls']['values'][0]['display_url'] == URL
    structure = facts['native_body_structures'][0]
    assert structure['scripts'][0]['exact_reviewed_ad_hook']
    assert structure['sampled_invisible_nodes'] == structure['sampled_populated_ad_widgets'] == 0
    assert not facts['body_text_retained'] and not facts['raw_response_retained']
    assert not facts['article_identity_or_scope_granted'] and facts['additional_requests'] == 0
    encoded = json.dumps(facts, ensure_ascii=False)
    for forbidden in ('第0段', '圖片旁的字', '圖文不同權利人', '相關文章不是正文', 'renderWidget'):
        assert forbidden not in encoded
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        parse(settings, html)


def test_metadata_bounds_and_redacts_but_exposes_structural_failure_not_body():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_peoplemedia import peoplemedia_validation_metadata
    html = page().replace('<p>第0段', '<p hidden>本文轉載不可保留 <iframe>保密</iframe>第0段')
    html = html.replace('<div id="dablewidget_37Jvj47N"></div>', '<div id="dablewidget_37Jvj47N">不留廣告</div>')
    html = html.replace("dable('renderWidget', 'dablewidget_37Jvj47N');", 'secret_script_value()')
    html = html.replace('第1段', '<span>不可保留細節</span>' * 5000 + '第1段')
    soup = BeautifulSoup(html, 'lxml')
    soup.h1.string = '長' * 5000
    soup.select_one('meta[property="og:url"]')['content'] = URL + '?token=private-token#private-fragment'
    soup.select_one('link[rel=canonical]')['href'] = 'https://user:private-password@www.peoplemedia.tw/news/123'
    facts = peoplemedia_validation_metadata(soup, page_url=URL)
    structure = facts['native_body_structures'][0]
    assert structure['sampled_reprint_prefix_present'] and structure['unrecognized_tag_names'] == ['iframe']
    assert structure['sampled_invisible_nodes'] > 0 and structure['sampled_populated_ad_widgets'] == 1
    assert not structure['scripts'][0]['exact_reviewed_ad_hook']
    assert structure['elements_truncated'] and structure['sampled_element_count'] == 4096
    assert facts['headings']['values'][0]['truncated'] and len(facts['headings']['values'][0]['value']) == 512
    assert facts['og_urls']['values'][0]['display_url'] == URL
    assert facts['canonical_urls']['values'][0]['display_url'] is None
    encoded = json.dumps(facts, ensure_ascii=False)
    for forbidden in ('不可保留', '保密', '不留廣告', 'secret_script_value', 'private-token', 'private-password', 'private-fragment'):
        assert forbidden not in encoded
    assert len(encoded.encode()) < 65536


def test_metadata_observes_unfetched_embed_and_script_attributes_without_secret_values():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_peoplemedia import peoplemedia_validation_metadata
    html = page().replace('<p>第0段', '<iframe src="//media.test/embed/123?token=private-token#fragment" '
        'srcdoc="private-html">private-fallback</iframe><hr><p>第0段')
    html = html.replace('<p>第1段', '<script async src="https://script.test/widget.js?token=private-token" '
        'onload="private-javascript" charset="utf-8"></script><p>第1段')
    soup = BeautifulSoup(html, 'lxml')
    before = str(soup)
    facts = peoplemedia_validation_metadata(soup, page_url=URL)
    assert facts['contract_version'] == 2 and str(soup) == before
    structure = facts['native_body_structures'][0]
    iframe = structure['embeds'][0]
    assert iframe['src']['display_url'] == '//media.test/embed/123' and not iframe['src']['requested']
    assert iframe['fallback_characters'] == len('private-fallback') and 'srcdoc' in iframe['attribute_names']
    script = structure['scripts'][0]
    assert script['src']['display_url'] == 'https://script.test/widget.js'
    assert script['charset']['value'] == 'utf-8' and script['script_characters'] == 0
    assert 'onload' in script['attribute_names'] and not script['src']['requested']
    assert structure['separators'][0]['next_sibling_tag'] == 'p'
    assert structure['ad_widgets_observed_before_any_removals']
    encoded = json.dumps(facts, ensure_ascii=False)
    for forbidden in ('private-token', 'private-html', 'private-fallback', 'private-javascript', '#fragment', '第0段'):
        assert forbidden not in encoded


def test_metadata_does_not_call_a_script_only_ad_widget_written_prose(settings):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_peoplemedia import peoplemedia_validation_metadata
    html = page().replace('<div id="dablewidget_37Jvj47N"></div><script>',
        '<div id="dablewidget_37Jvj47N"><script>').replace('</script>', '</script></div>')
    facts = peoplemedia_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    structure = facts['native_body_structures'][0]
    assert structure['sampled_populated_ad_widgets'] == 1 and structure['ad_widgets_observed_before_any_removals']
    assert structure['scripts'][0]['parent_is_native_ad_hook'] and structure['scripts'][0]['exact_reviewed_ad_hook']
    assert parse(settings, html) == parse(settings)


def test_metadata_does_not_change_current_accepted_values(settings):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_peoplemedia import peoplemedia_validation_metadata
    source = PolicyRegistry(settings, base=Path.cwd()).sources['peoplemedia']
    soup = BeautifulSoup(page(), source.html_parser)
    peoplemedia_validation_metadata(soup, page_url=URL)
    assert extract_article(str(soup).encode(), source, page_url=URL) == parse(settings)


def test_canonical_metadata_reuses_existing_persistent_budget_without_raw(settings, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_failure_diagnostics import fail, reports
    from test_news_failure_identity_metadata import evidence
    response = Response(URL, 200, {'content-type': 'text/html', 'set-cookie': 'private-cookie'},
        page(credit='未審核記者／合成署名').encode())
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=Path.cwd()))
    try:
        for _ in range(3):
            fail(queue, response, code='article_rights_or_scope_excluded',
                 received_at_utc='2026-10-20T20:00:00+00:00')
        paths, rows = evidence(tmp_path)
        assert len(rows) == 2
        assert all(row['facts']['peoplemedia_native_validation']['native_credits']['values'][0]['value']
                   == '未審核記者／合成署名' for row in rows)
        assert all(not row['accepted_as_article'] and not row['raw_response_retained'] for row in rows)
        assert all('第0段' not in path.read_text() and 'private-cookie' not in path.read_text() for path in paths)
        assert not reports(tmp_path)[0]
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        key = 'failure_validation_metadata_attempts:v1:peoplemedia:article_rights_or_scope_excluded:20261020'
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == '2'
    finally:
        queue.close()


def test_operator_metadata_probe_observes_without_retention_or_additional_request(settings, tmp_path, capsys):
    from scripts.probe_news_sources import probe_provider
    registry = PolicyRegistry(settings, base=Path.cwd())
    requests = []
    def get(url):
        requests.append(url)
        return Response(url, 200, {'content-type': 'text/html'}, page(credit='未審核記者／合成署名').encode())
    http = SimpleNamespace(registry=registry, provider_not_before={}, robot_delays={}, get=get)
    result = probe_provider(http, [URL], output_root=tmp_path)
    assert requests == [URL]
    assert result[0]['extraction_error'] == 'article_rights_or_scope_excluded'
    assert result[0]['peoplemedia_native_validation']['native_credits']['values'][0]['value'] == '未審核記者／合成署名'
    assert '第0段' not in json.dumps(result, ensure_ascii=False) and '第0段' not in capsys.readouterr().out
    assert not list(tmp_path.iterdir())


def versioned(settings, version):
    result = deepcopy(settings)
    result['providers'][0]['embedded_article_layout'] = f'peoplemedia_visible_article_v{version}'
    return result


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('credit,editorial', [
    ('記者陳文蔚/綜合報導', False), ('生活中心／台北報導', True), ('記者何豪毅／綜合報導', False),
    ('國際中心／綜合報導', True), ('記者林冠妙／台北報導', False), ('財經、社會中心／綜合報導', True),
    ('政治中心/綜合報導', True), ('社會中心／綜合報導', True), ('財經中心/綜合報導', True)])
def test_v2_only_adds_exact_observed_credits_preserving_native_labels(settings, parser, credit, editorial):
    settings['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        parse(settings, page(credit=credit))
    current = parse(versioned(settings, 2), page(credit=credit))
    assert current['body'] == parse(settings)['body']
    assert current['published_at_raw'] == PUBLISHED and current['modified_at_raw'] == MODIFIED
    assert json.loads(current['authors_json']) == [credit]
    detail = json.loads(current['author_attribution_detail_json'])
    assert detail['native_credit'] == credit and detail['editorial_center_label'] == editorial
    assert detail['role'].endswith('not_verified_person')


@pytest.mark.parametrize('spaces', [1, 2])
def test_v2_matches_only_outer_ascii_spaces_in_native_title_declarations(settings, spaces):
    html = page().replace('【民報】' + TITLE, '【民報】' + TITLE + ' ' * spaces)
    html = html.replace(TITLE + ' | 民報', TITLE + ' ' * spaces + ' | 民報')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, html)
    result = parse(versioned(settings, 2), html)
    assert result['title'] == TITLE and result['body'] == parse(settings)['body']
    notice = json.loads(result['verified_article_notices_json'])[-2]
    assert notice['kind'] == 'native_title_outer_ASCII_space_variation'
    assert notice['og_title_raw'].endswith(' ' * spaces)
    assert not notice['interior_whitespace_or_fuzzy_matching']


@pytest.mark.parametrize('edit', [
    lambda s: s.replace('【民報】' + TITLE, '【民報】' + TITLE + '另一篇'),
    lambda s: s.replace('【民報】' + TITLE, '【民報】' + TITLE.replace('原值 ', '原值  ')),
    lambda s: s.replace('【民報】' + TITLE, '【民報】' + TITLE + '\t'),
    lambda s: s.replace('<p>第0段', '<iframe></iframe><p>第0段'),
    lambda s: s.replace('<p>第0段', '<script src="https://other.test/embed.js"></script><p>第0段'),
    lambda s: s.replace('<p>第0段', '<p class="hidden">第0段'),
])
def test_v2_does_not_weaken_identity_visibility_or_unreviewed_embedded_content(settings, edit):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 2), edit(page()))


@pytest.mark.parametrize('credit', ['文／陳永興', '中央社／轉載', '陳俊廷/高雄報導',
    '記者何豪毅／綜合報導、其他人', '財經中心／未知報導', '記者合成人／綜合報導'])
def test_v2_is_not_a_generic_reporter_or_opinion_scope_grant(settings, credit):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        parse(versioned(settings, 2), page(credit=credit))


def test_v2_preserves_accepted_v1_values_http_identity_and_authorization(settings):
    old = PolicyRegistry(settings, base=Path.cwd()).sources['peoplemedia']
    new = PolicyRegistry(versioned(settings, 2), base=Path.cwd()).sources['peoplemedia']
    assert parse(settings) == parse(versioned(settings, 2))
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == (old.http_capacity_source_fingerprint or old.data_fingerprint)
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert native_text_parser_repair(new, old.data_fingerprint)
    assert not native_text_parser_repair(new, new.data_fingerprint)
    changed = versioned(settings, 2)
    changed['providers'][0]['authorization']['scope'] += '; different source scope'
    assert not native_text_parser_repair(PolicyRegistry(changed, base=Path.cwd()).sources['peoplemedia'], old.data_fingerprint)


@pytest.mark.parametrize('version', [2, 3])
@pytest.mark.parametrize('obstruction', [None, 'body_sha256', 'parquet_path', 'lease_owner', 'lease_until',
    'missing_observation', 'future_observation', 'naive_observation', 'different_error', 'unknown_prior'])
def test_migration_only_requeues_exact_evidenced_missing_heads_once_preserving_evidence(settings, tmp_path, obstruction, version):
    from downloader.news_fulltext_peoplemedia import _V2_REPAIR_IDS, _V3_REPAIR_IDS
    old = PolicyRegistry(versioned(settings, version - 1), base=tmp_path)
    new = PolicyRegistry(versioned(settings, version), base=tmp_path)
    repair_ids = _V2_REPAIR_IDS if version == 2 else _V3_REPAIR_IDS
    excluded = (_V3_REPAIR_IDS if version == 2 else _V2_REPAIR_IDS) | {'01234567-89ab-cdef-0123-456789abcdef'}
    before = {}
    key = 'failure_identity_metadata_attempts:v1:peoplemedia:20261020'
    with closing(NewsQueue(tmp_path, old)) as queue:
        for number, article_id in enumerate(sorted(repair_ids | excluded)):
            url = PEOPLEMEDIA_ORIGIN + '/news/' + article_id
            queue.add_articles([{'url': url}])
            state = 'article_identity_not_verified' if number % 2 else 'article_rights_or_scope_excluded'
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,'
                'last_checked_at_utc=? WHERE url=?', (state, state, utc_now(), url))
            if obstruction in {'body_sha256', 'parquet_path', 'lease_owner', 'lease_until'}:
                queue.db.execute(f'UPDATE articles SET {obstruction}=? WHERE url=?',
                    (9000000001 if obstruction == 'lease_until' else 'saved-or-owned', url))
            elif obstruction in {'missing_observation', 'future_observation', 'naive_observation'}:
                observed = {'missing_observation': None, 'future_observation': '2099-01-01T00:00:00+00:00',
                            'naive_observation': '2026-01-01T00:00:00'}[obstruction]
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (observed, url))
            elif obstruction == 'different_error':
                queue.db.execute("UPDATE articles SET error_code='different' WHERE url=?", (url,))
            before[url] = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        if obstruction == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:peoplemedia'", ('f' * 64,))
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (key, '2'))
        queue.db.commit()
    marker = f'peoplemedia_native_parser_recheck_v{version}:' + new.sources['peoplemedia'].data_fingerprint
    with closing(NewsQueue(tmp_path, new)) as queue:
        for url, row in before.items():
            expected = {**row, 'state': 'retry'} if obstruction is None and url.rsplit('/', 1)[1] in repair_ids else row
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == expected
        record = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if obstruction == 'unknown_prior':
            assert record is None
        else:
            evidence = json.loads(record[0])
            assert len(evidence['rows']) == (len(repair_ids) if obstruction is None else 0)
            assert evidence['attempts_due_error_and_observation_preserved'] and not evidence['accepted_as_bodies']
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == '2'
        for url, row in before.items():
            queue.db.execute('UPDATE articles SET state=? WHERE url=?', (row['state'], url))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert all(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == row
                   for url, row in before.items())


@pytest.mark.parametrize('version', [1, 2, 3])
def test_candidate_obeys_installed_source_contract(settings, monkeypatch, version):
    import test_news_fulltext as shared
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config)
    candidate['providers'] = [entry for entry in config['providers'] if entry['id'] != 'peoplemedia'] + versioned(settings, version)['providers']
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


@pytest.mark.parametrize('credit', ['文／陳永興', '陳俊廷/高雄報導'])
def test_v3_named_credit_is_literal_not_generic_person_opinion_or_ownership(settings, credit):
    html = page(credit=credit).replace('圖片旁的字保留', '圖片旁的字保留<hr><strong>分隔線後 -3.50 股 0%</strong>')
    with pytest.raises(NewsError):
        parse(versioned(settings, 2), html)
    result = parse(versioned(settings, 3), html)
    assert json.loads(result['authors_json']) == [credit] and '-3.50 股 0%' in result['body']
    assert json.loads(result['author_attribution_detail_json'])['role'].endswith('not_verified_person')
    assert not result['per_article_license_verified'] and not result['historical_point_in_time']
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        parse(versioned(settings, 3), page(credit=credit + '、其他人'))


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('count', [1, 5])
def test_v3_empty_native_youtube_frames_preserve_prose_and_explicit_unfetched_notices(settings, parser, count):
    settings['providers'][0]['html_parser'] = parser
    video = '<iframe src="https://www.youtube.com/embed/SL7pY9qnfgs" frameborder="0" width="100%" height="315"></iframe>'
    html = page().replace('圖片旁的字保留', '圖片旁的字保留' + video * count + '影片後的 -5.0% 文字')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 2), html)
    result = parse(versioned(settings, 3), html)
    assert '圖片旁的字保留' in result['body'] and '影片後的 -5.0% 文字' in result['body']
    assert 'youtube' not in result['body'] and 'multimedia_not_complete' in result['body_completeness']
    notices = [row for row in json.loads(result['verified_article_notices_json']) if row['kind'] == 'unfetched_native_youtube_video']
    assert len(notices) == count
    assert all(not row['media_requested'] and not row['media_transcribed'] and not row['multimedia_complete'] for row in notices)


@pytest.mark.parametrize('edit', [
    lambda v: v.replace('www.youtube.com', 'youtube.com.evil.test'),
    lambda v: v.replace('SL7pY9qnfgs', 'SL7pY9qnfgs?key=x'),
    lambda v: v.replace('SL7pY9qnfgs', 'SL7pY9qnfgs#fragment'),
    lambda v: v.replace('/embed/', '/watch/'),
    lambda v: v.replace('https:', 'http:'),
    lambda v: v.replace('frameborder="0"', 'srcdoc="fallback" frameborder="0"'),
    lambda v: v.replace('frameborder="0"', 'onload="unknown()" frameborder="0"'),
    lambda v: v.replace('width="560"', 'width="0"'),
    lambda v: v.replace('height="315"', 'height="9999"'),
    lambda v: v.replace('></iframe>', '>不可丟棄的fallback</iframe>'),
    lambda v: v * 9,
])
def test_v3_does_not_silently_drop_unreviewed_embed_variants_or_fallback_prose(settings, edit):
    video = '<iframe src="https://www.youtube.com/embed/SL7pY9qnfgs" frameborder="0" width="560" height="315"></iframe>'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 3), page().replace('圖片旁的字保留', edit(video) + '圖片旁的字保留'))


def test_v3_source_email_shim_inside_empty_ad_hook_is_not_executed_or_decoded(settings):
    from downloader.news_fulltext_peoplemedia import _EMAIL_DECODER
    shim = f'<script data-cfasync="false" src="{_EMAIL_DECODER}"></script>'
    html = page().replace('<div id="dablewidget_37Jvj47N"></div>', '<div id="dablewidget_37Jvj47N">' + shim + '</div>')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 2), html)
    result = parse(versioned(settings, 3), html)
    assert result['body'] == parse(settings)['body']
    notice = next(row for row in json.loads(result['verified_article_notices_json']) if row['kind'] == 'unfetched_source_email_decoder_inside_ad_hook')
    assert not notice['script_requested'] and not notice['script_executed'] and not notice['obfuscated_addresses_decoded']
    assert not notice['access_controls_bypassed']


@pytest.mark.parametrize('edit', [
    lambda s: s.replace('data-cfasync="false"', 'data-cfasync="true"'),
    lambda s: s.replace('email-decode.min.js', 'different.js'),
    lambda s: s.replace('email-decode.min.js', 'email-decode.min.js?key=x'),
    lambda s: s.replace('></script>', '>unknown()</script>'),
    lambda s: s.replace('data-cfasync="false"', 'data-cfasync="false" onload="unknown()"'),
])
def test_v3_still_refuses_unknown_script_shapes(settings, edit):
    from downloader.news_fulltext_peoplemedia import _EMAIL_DECODER
    shim = f'<script data-cfasync="false" src="{_EMAIL_DECODER}"></script>'
    html = page().replace('<div id="dablewidget_37Jvj47N"></div>', '<div id="dablewidget_37Jvj47N">' + edit(shim) + '</div>')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 3), html)


def test_v3_shim_cannot_hide_outside_the_ad_widget_or_excuse_ad_text(settings):
    from downloader.news_fulltext_peoplemedia import _EMAIL_DECODER
    shim = f'<script data-cfasync="false" src="{_EMAIL_DECODER}"></script>'
    for html in (page().replace('圖片旁的字保留', shim + '圖片旁的字保留'),
                 page().replace('<div id="dablewidget_37Jvj47N"></div>', '<div id="dablewidget_37Jvj47N">不准隱藏的廣告正文' + shim + '</div>')):
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            parse(versioned(settings, 3), html)


def test_v3_keeps_v2_extracted_values_and_exact_predecessor_and_http_identity(settings):
    old = PolicyRegistry(versioned(settings, 2), base=Path.cwd()).sources['peoplemedia']
    new = PolicyRegistry(versioned(settings, 3), base=Path.cwd()).sources['peoplemedia']
    assert parse(versioned(settings, 2)) == parse(versioned(settings, 3))
    for credit in ('記者陳文蔚/綜合報導', '財經中心/綜合報導'):
        assert parse(versioned(settings, 2), page(credit=credit)) == parse(versioned(settings, 3), page(credit=credit))
    assert new.data_fingerprint != old.data_fingerprint and new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert native_text_parser_repair(new, old.data_fingerprint)
    v1 = PolicyRegistry(settings, base=Path.cwd()).sources['peoplemedia']
    assert not native_text_parser_repair(new, v1.data_fingerprint)
