"""Native two-original parsing must not relax CMNews or source access rules."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.news_fulltext_cmoneylearning import (
    CMONEY_LEARNING_BODY_SELECTOR, CMONEY_LEARNING_ORIGIN,
    CMONEY_LEARNING_PATH_PATTERN, CMONEY_LEARNING_PATHS,
    CMONEY_LEARNING_V2_PATH_PATTERN,CMONEY_LEARNING_V2_PATHS,CMONEY_LEARNING_SUPPLEMENT_PATHS,
    CMONEY_LEARNING_V2_AUTH_SCOPE, CMONEY_LEARNING_V2_NOTE, CMONEY_LEARNING_V2_HTTP_BINDING,
    CMONEY_LEARNING_NAVIGATION_PATHS, CMONEY_LEARNING_V3_PATHS, CMONEY_LEARNING_V3_PATH_PATTERN,
    CMONEY_LEARNING_V3_AUTH_SCOPE, CMONEY_LEARNING_V3_NOTE, CMONEY_LEARNING_V3_HTTP_BINDING,
    CMONEY_LEARNING_DECLARED_20261023_PATH, CMONEY_LEARNING_V4_PATHS,
    CMONEY_LEARNING_V4_PATH_PATTERN, CMONEY_LEARNING_V4_AUTH_SCOPE,
    CMONEY_LEARNING_V4_NOTE, CMONEY_LEARNING_V4_HTTP_BINDING,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_news_fulltext import config


@pytest.fixture
def settings(config):
    config = deepcopy(config)
    config['providers'] = [{
        'id': 'cmoneylearning', 'name': 'Synthetic public originals',
        'domains': ['cmoneylearning.com.tw'], 'enabled': True, 'acquisition_mode': 'finite',
        'interval_seconds': 10, 'parallelism': 1,
        'embedded_article_layout': 'cmoneylearning_visible_article_v1',
        'html_parser': 'lxml', 'article_path_pattern': CMONEY_LEARNING_PATH_PATTERN,
        'article_selectors': [CMONEY_LEARNING_BODY_SELECTOR], 'exclude_selectors': [],
        'title_selector': 'h1.articleContent__title',
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research',
            'evidence_url': 'https://www.cmoney.tw/member/tos.aspx',
            'reviewed_on': '2026-10-20', 'scope': 'Two private originals only; no aliases, discovery or export.',
            'automated_access_review': 'no_explicit_prohibition_identified'},
    }]
    return config


def test_two_original_candidate_keeps_the_closed_installed_source_contract(settings, monkeypatch):
    """Exercise admission before deployment, even while the recipe is disabled."""
    import test_news_fulltext as shared
    from downloader.news_fulltext import load_config

    installed, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(installed)
    candidate['providers'] = [p for p in candidate['providers'] if p['id'] != 'cmoneylearning'] + settings['providers']
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


def page(path=CMONEY_LEARNING_PATHS[0]):
    url = CMONEY_LEARNING_ORIGIN + path
    origin = CMONEY_LEARNING_ORIGIN
    person, organization = origin + '/author/rowan_lin/#person', origin + '/#organization'
    declarations = [
        {'@type': 'BlogPosting', '@id': url + '#article',
         'mainEntityOfPage': {'@type': 'WebPage', '@id': url}, 'headline': '合成財經報導',
         'datePublished': '2026-10-19T17:11:20+00:00', 'dateModified': '2026-10-19T17:11:44+00:00',
         'isAccessibleForFree': True, 'author': {'@id': person},
         'publisher': {'@id': organization}, 'copyrightHolder': {'@id': organization}},
        {'@type': 'Person', '@id': person, 'name': '理財內容編輯林序安',
         'url': origin + '/author/rowan_lin/', 'worksFor': {'@id': organization}},
        {'@type': 'Organization', '@id': organization, 'name': '實習大人', 'url': origin + '/'},
    ]
    scripts = ''.join('<script type="application/ld+json">' + json.dumps(v, ensure_ascii=False) + '</script>'
                      for v in declarations)
    prose = ''.join(f'<p>第{i}段' + '合成原始數值<strong>不取對數</strong>。' * 8 + '</p>' for i in range(4))
    return f'''<html><head><title>合成財經報導 | 實習大人</title>
      <link rel="canonical" href="{url}"><meta property="og:type" content="article">
      <meta property="og:title" content="合成財經報導 | 實習大人">{scripts}</head>
      <body><nav>不是正文的導航</nav><main class="site-main"><article class="articleContent">
      <div class="articleContent__body"><div class="articleContent__content--main">
      <div class="articleContent__content--titleRow"><h1 class="articleContent__title">合成財經報導</h1></div>
      <div class="articleContent__meta"><div class="articleContent__metaLine">
      <span class="articleContent__author">理財內容編輯林序安</span>
      <time class="articleContent__date" datetime="2026-10-20T01:11:20+08:00">2026年10月20日</time>
      </div></div><figure>不是正文的主圖</figure>
      <div class="articleContent__textContent"><div>{prose}
      <table><thead><tr><th>欄名</th></tr></thead><tbody><tr><td>-12.50</td></tr><tr><td>0</td></tr></tbody></table>
      <article id="readmo-ai-article-shortcode-1" class="readmo-ai-article-shortcode"></article>
      <script>throw new Error('不得執行的輪詢程式');</script></div></div>
      <footer>不是正文的頁尾</footer></div></div></article></main>
      <aside><h1>另一篇新聞</h1><span class="articleContent__author">別篇作者</span></aside></body></html>'''


def parse(settings, html, path=CMONEY_LEARNING_PATHS[0], *, url=None):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['cmoneylearning']
    return extract_article(html.encode(), source, page_url=url or CMONEY_LEARNING_ORIGIN + path)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('path', CMONEY_LEARNING_PATHS)
def test_native_body_table_and_equivalent_declared_clocks(settings, parser, path):
    settings['providers'][0]['html_parser'] = parser
    result = parse(settings, page(path), path)
    assert result['published_at_raw'] == '2026-10-20T01:11:20+08:00'
    assert result['modified_at_raw'] == '2026-10-19T17:11:44+00:00'
    assert json.loads(result['authors_json']) == ['理財內容編輯林序安']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert json.loads(result['author_attribution_detail_json'])['declared_editor_role_not_inferred_reporter']
    for i in range(4):
        assert f'第{i}段' in result['body']
    for value in ['欄名', '-12.50', '0']:
        assert value in result['body']
    assert result['body'].count('不取對數') == 32
    for value in ['合成財經報導', '導航', '主圖', '頁尾', '另一篇', '別篇作者', '輪詢程式', '理財內容編輯']:
        assert value not in result['body']
    assert 'not_CMNews_alias_equivalence' in result['article_identity_basis']


@pytest.mark.parametrize('old,new,code', [
    ('rel="canonical"', 'rel="alternate"', 'article_identity_not_verified'),
    ('property="og:type" content="article"', 'property="og:type" content="website"', 'article_identity_not_verified'),
    ('class="articleContent__title"', 'class="articleContent__title" hidden', 'article_identity_not_verified'),
    ('class="articleContent__body"', 'class="articleContent__body" aria-hidden="true"', 'article_identity_not_verified'),
    ('第2段', '<span class="hidden">第2段</span>', 'article_identity_not_verified'),
    ('理財內容編輯林序安</span>', '其他作者</span>', 'article_attribution_not_verified'),
    ('"name": "理財內容編輯林序安"', '"name": "另一位作者"', 'article_rights_or_scope_excluded'),
    ('"copyrightHolder":', '"unverifiedHolder":', 'article_rights_or_scope_excluded'),
    ('"isAccessibleForFree": true', '"isAccessibleForFree": false', 'paywall_or_login'),
    ('2026年10月20日', '2026年10月19日', 'invalid_bulk_publication_clock'),
    ('2026-10-20T01:11:20+08:00', '2026-10-20T01:11:20', 'invalid_bulk_publication_clock'),
    ('2026-10-19T17:11:20+00:00', '2026-10-19T17:11:21+00:00', 'invalid_bulk_publication_clock'),
    ('2026-10-19T17:11:44+00:00', '2026-10-19T17:11:19+00:00', 'invalid_bulk_publication_clock'),
    ('2026-10-19T17:11:44+00:00', '2026-02-30T17:11:44+00:00', 'invalid_bulk_publication_clock'),
    ('class="readmo-ai-article-shortcode"></article>', 'class="readmo-ai-article-shortcode">生成內容</article>', 'article_identity_not_verified'),
    ('<table>', '<iframe src="/unreviewed"></iframe><table>', 'paywall_or_login'),
])
def test_drift_and_access_never_fall_back(settings, old, new, code):
    with pytest.raises(NewsError, match=code):
        parse(settings, page().replace(old, new))


@pytest.mark.parametrize('selector', ['canonical', 'scope', 'date', 'schema'])
def test_ambiguous_current_article_is_not_chosen_by_first_match(settings, selector):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(page(), 'lxml')
    node = soup.select_one({'canonical': 'link[rel=canonical]', 'scope': 'article.articleContent',
                           'date': 'time', 'schema': 'script[type="application/ld+json"]'}[selector])
    node.insert_after(deepcopy(node))
    with pytest.raises(NewsError, match='article_identity_not_verified|invalid_bulk_publication_clock'):
        parse(settings, str(soup))


@pytest.mark.parametrize('url', [
    'https://cmoneylearning.com.tw/not-reviewed/', 'http://cmoneylearning.com.tw/',
    'https://cmnews.com.tw/article/other',
    CMONEY_LEARNING_ORIGIN + CMONEY_LEARNING_PATHS[0] + '?other=1',
    CMONEY_LEARNING_ORIGIN + CMONEY_LEARNING_PATHS[0] + '#other',
    CMONEY_LEARNING_ORIGIN + CMONEY_LEARNING_PATHS[0].upper(),
])
def test_no_guessed_alias_or_percent_case_expansion(settings, url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, page(), url=url)


@pytest.mark.parametrize('field,value', [
    ('acquisition_mode', 'continuous'), ('domains', ['cmnews.com.tw']),
    ('article_path_pattern', '^/.*$'), ('article_selectors', ['article']),
    ('exclude_selectors', ['p']), ('require_author_attribution', False),
    ('require_configured_container', False), ('title_selector', 'h1'),
    ('feeds', [CMONEY_LEARNING_ORIGIN + '/feed/']),
    ('sitemaps', [CMONEY_LEARNING_ORIGIN + '/sitemap.xml']),
    ('index_pages', [CMONEY_LEARNING_ORIGIN + '/']),
])
def test_two_private_originals_do_not_grant_sitewide_capture(settings, field, value):
    settings['providers'][0][field] = value
    with pytest.raises(ValueError, match='unreviewed embedded article parser'):
        PolicyRegistry(settings, base=Path.cwd())


@pytest.fixture
def supplemental_settings(settings):
    settings=deepcopy(settings);entry=settings['providers'][0]
    entry.update(embedded_article_layout='cmoneylearning_visible_article_v2',
        article_path_pattern=CMONEY_LEARNING_V2_PATH_PATTERN,allow_reviewed_supplements=True)
    entry['authorization']['scope']=CMONEY_LEARNING_V2_AUTH_SCOPE
    return settings


@pytest.mark.parametrize('parser',['lxml','html.parser'])
@pytest.mark.parametrize('path',CMONEY_LEARNING_V2_PATHS)
def test_v2_only_adds_the_two_literal_declared_originals(supplemental_settings,parser,path):
    supplemental_settings['providers'][0]['html_parser']=parser
    article=parse(supplemental_settings,page(path),path)
    assert article['body'].count('不取對數')==32
    assert 'BlogPosting_v2; not_CMNews_alias_equivalence' in article['article_identity_basis']
    assert article['published_at_raw']=='2026-10-20T01:11:20+08:00'
    assert not article['historical_point_in_time'] and not article['per_article_license_verified']


@pytest.mark.parametrize('path',CMONEY_LEARNING_SUPPLEMENT_PATHS)
def test_old_v1_does_not_inherit_the_new_original_scope(settings,path):
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(settings,page(path),path)


@pytest.mark.parametrize('settings_fixture', ['supplemental_settings', 'navigation_settings'])
@pytest.mark.parametrize('old,new,code',[
    ('rel="canonical"','rel="alternate"','article_identity_not_verified'),
    ('理財內容編輯林序安</span>','另一個人</span>','article_attribution_not_verified'),
    ('"copyrightHolder":','"unknownHolder":','article_rights_or_scope_excluded'),
    ('"isAccessibleForFree": true','"isAccessibleForFree": false','paywall_or_login'),
    ('2026-10-19T17:11:20+00:00','2026-10-19T17:11:21+00:00','invalid_bulk_publication_clock'),
    ('class="articleContent__body"','class="articleContent__body" hidden','article_identity_not_verified'),
])
def test_versioned_original_keeps_source_identity_access_credit_and_clock_gates(request,settings_fixture,old,new,code):
    path=CMONEY_LEARNING_SUPPLEMENT_PATHS[0]
    with pytest.raises(NewsError,match=code):
        parse(request.getfixturevalue(settings_fixture),page(path).replace(old,new),path)


@pytest.mark.parametrize('field,value',[
    ('article_path_pattern','^/.*$'),('allow_reviewed_supplements',False),
    ('feeds',[CMONEY_LEARNING_ORIGIN+'/feed/']),('acquisition_mode','continuous'),
])
def test_v2_is_not_sitewide_or_unreviewed_supplement_permission(supplemental_settings,field,value):
    supplemental_settings['providers'][0][field]=value
    with pytest.raises(ValueError,match='unreviewed embedded article parser'):
        PolicyRegistry(supplemental_settings,base=Path.cwd())


def test_v2_does_not_inherit_http_identity_for_arbitrary_config_changes():
    from downloader.news_fulltext import load_config
    config,_,repo=load_config(Path('configs/news_fulltext.json'))
    proposal=deepcopy(config);entry=next(e for e in proposal['providers'] if e['id']=='cmoneylearning')
    entry.update(embedded_article_layout='cmoneylearning_visible_article_v2',
        article_path_pattern=CMONEY_LEARNING_V2_PATH_PATTERN,allow_reviewed_supplements=True,
        adapter_verification='native_canonical_BlogPosting_editor_clock_v2_closed_declared_originals_20261022',
        note=CMONEY_LEARNING_V2_NOTE)
    entry['authorization'].update(reviewed_on='2026-10-22',scope=CMONEY_LEARNING_V2_AUTH_SCOPE)
    candidate=PolicyRegistry(proposal,base=repo).sources['cmoneylearning']
    assert (candidate.data_fingerprint,candidate.authorization_fingerprint)==CMONEY_LEARNING_V2_HTTP_BINDING[:2]
    assert (candidate.http_capacity_source_fingerprint,candidate.http_capacity_authorization_fingerprint)==CMONEY_LEARNING_V2_HTTP_BINDING[2:]
    entry['interval_seconds']=11
    changed=PolicyRegistry(proposal,base=repo).sources['cmoneylearning']
    assert not changed.http_capacity_source_fingerprint and not changed.http_capacity_authorization_fingerprint


def test_v2_reconciliation_preserves_completed_initial_heads_and_frozen_membership(settings,supplemental_settings,tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    root=tmp_path/'news'
    queue=NewsQueue(root,PolicyRegistry(settings,base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url':CMONEY_LEARNING_ORIGIN+path} for path in CMONEY_LEARNING_PATHS])
        queue.sync_finite_cohorts()
        # Synthetic completed-head facts only; this test does not certify bodies.
        with queue.db:
            queue.db.execute("UPDATE articles SET state='complete',body_sha256=?",('a'*64,))
        before=[dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')]
        cohort=dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone())
        receipt=(root/cohort['receipt_path']).read_bytes()
    finally:
        queue.close()
    queue=NewsQueue(root,PolicyRegistry(supplemental_settings,base=tmp_path))
    try:
        assert [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')]==before
        with queue.db:
            queue.add_articles([{'url':CMONEY_LEARNING_ORIGIN+path} for path in CMONEY_LEARNING_SUPPLEMENT_PATHS])
        queue.sync_finite_cohorts()
        assert dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone())==cohort
        assert (root/cohort['receipt_path']).read_bytes()==receipt
        for path in CMONEY_LEARNING_SUPPLEMENT_PATHS:
            url=CMONEY_LEARNING_ORIGIN+path
            assert queue.db.execute('SELECT state FROM articles WHERE url=?',(url,)).fetchone()[0]=='waiting_cohort'
            assert queue.claim_url(url,300) is None
        assert queue.db.execute('SELECT count(*) FROM finite_capture_supplements').fetchone()[0]==0
    finally:
        queue.close()


def title_variant_page(path=CMONEY_LEARNING_SUPPLEMENT_PATHS[0]):
    """Synthetic different SEO heading; never copy a private response fixture."""
    from bs4 import BeautifulSoup
    soup=BeautifulSoup(page(path),'lxml')
    soup.select_one('h1.articleContent__title').string='目前可見的財經標題'
    breadcrumb={'@type':'BreadcrumbList','itemListElement':[
        {'@type':'ListItem','position':1,'name':'首頁','item':CMONEY_LEARNING_ORIGIN+'/'},
        {'@type':'ListItem','position':2,'name':'目前可見的財經標題'},
    ]}
    script=soup.new_tag('script',type='application/ld+json')
    script.string=json.dumps(breadcrumb,ensure_ascii=False);soup.head.append(script)
    return str(soup)


@pytest.mark.parametrize('parser',['lxml','html.parser'])
def test_v2_preserves_visible_and_SEO_headings_with_independent_breadcrumb_witness(supplemental_settings,parser):
    supplemental_settings['providers'][0]['html_parser']=parser
    path=CMONEY_LEARNING_SUPPLEMENT_PATHS[0]
    result=parse(supplemental_settings,title_variant_page(path),path)
    assert result['title']=='目前可見的財經標題'
    declarations=json.loads(result['source_title_declarations_json'])
    assert declarations['BlogPosting_headline_raw']=='合成財經報導'
    assert declarations['head_title_raw']==declarations['og_title_raw']=='合成財經報導 | 實習大人'
    assert declarations['visible_title_raw']==result['title']
    assert declarations['title_variants_present'] and declarations['current_breadcrumb_heading_witness_required']
    assert not declarations['historical_title_chronology_inferred']
    assert result['body'].count('不取對數')==32


@pytest.mark.parametrize('mutation',[
    'missing_breadcrumb','different_current_heading','different_home','bool_position',
    'duplicate_breadcrumb','different_last_URL','different_seo','different_BlogPosting_headline',
])
def test_v2_never_accepts_arbitrary_title_disagreement(supplemental_settings,mutation):
    from bs4 import BeautifulSoup
    path=CMONEY_LEARNING_SUPPLEMENT_PATHS[0];soup=BeautifulSoup(title_variant_page(path),'lxml')
    scripts=soup.select('script[type="application/ld+json"]')
    crumb=scripts[-1];obj=json.loads(crumb.get_text())
    if mutation=='missing_breadcrumb':
        crumb.decompose()
    elif mutation=='duplicate_breadcrumb':
        crumb.insert_after(deepcopy(crumb))
    elif mutation=='different_seo':
        soup.select_one('meta[property="og:title"]')['content']='不相符的SEO標題 | 實習大人'
    elif mutation=='different_BlogPosting_headline':
        obj=json.loads(scripts[0].get_text());obj['headline']='別篇結構標題'
        scripts[0].string=json.dumps(obj,ensure_ascii=False)
    else:
        if mutation=='different_current_heading':
            obj['itemListElement'][-1]['name']='別篇正文'
        elif mutation=='different_home':
            obj['itemListElement'][0]['item']='https://different.test/'
        elif mutation=='bool_position':
            obj['itemListElement'][0]['position']=True
        elif mutation=='different_last_URL':
            obj['itemListElement'][-1]['item']='https://cmoneylearning.com.tw/not-this-page/'
        crumb.string=json.dumps(obj,ensure_ascii=False)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(supplemental_settings,str(soup),path)


def test_v1_does_not_inherit_title_variant_semantics(settings):
    path=CMONEY_LEARNING_PATHS[0]
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(settings,title_variant_page(path),path)


@pytest.mark.parametrize('tamper',[False,True])
def test_durable_title_variant_provenance_is_replayed_not_just_self_consistent(supplemental_settings,tmp_path,tamper):
    from downloader.news_fulltext import fetch_article
    from downloader.news_fulltext_queue import NewsQueue
    from scripts.audit_news_selected_articles import audit
    from test_news_fulltext import FakeResponse,http_fixture
    url=CMONEY_LEARNING_ORIGIN+CMONEY_LEARNING_SUPPLEMENT_PATHS[0]
    http,_=http_fixture(supplemental_settings,tmp_path,{
        CMONEY_LEARNING_ORIGIN+'/robots.txt':FakeResponse(b'User-agent: *\nAllow: /'),
        url:FakeResponse(title_variant_page().encode()),
    })
    queue=NewsQueue(tmp_path,http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url':url}])
        queue.sync_finite_cohorts()
        record=fetch_article(http,queue.claim_url(url,300))
        if tamper:
            evidence=json.loads(record['source_title_declarations_json'])
            evidence['BlogPosting_headline_raw']='錯誤的來源標題'
            record['source_title_declarations_json']=json.dumps(evidence,ensure_ascii=False,sort_keys=True)
        queue.accept([record])
        proof=audit(tmp_path,[url],registry=http.registry)
        assert proof['contract_version']==11
        assert proof['accepted'] is (not tamper)
        if tamper:
            assert proof['errors'][0]['error']=='current_source_replay_changed_body_title_author_or_clock'
    finally:
        queue.close()
        http.close()


@pytest.fixture
def navigation_settings(supplemental_settings):
    result = deepcopy(supplemental_settings)
    entry = result['providers'][0]
    entry.update(embedded_article_layout='cmoneylearning_visible_article_v3',
                 article_path_pattern=CMONEY_LEARNING_V3_PATH_PATTERN)
    entry['authorization']['scope'] = CMONEY_LEARNING_V3_AUTH_SCOPE
    return result


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('path', CMONEY_LEARNING_V3_PATHS)
def test_v3_joins_only_literal_reviewed_navigation_articles(navigation_settings, parser, path):
    navigation_settings['providers'][0]['html_parser'] = parser
    article = parse(navigation_settings, title_variant_page(path), path)
    assert article['body'].count('不取對數') == 32
    assert article['title'] == '目前可見的財經標題'
    assert 'BlogPosting_v3; not_CMNews_alias_equivalence' in article['article_identity_basis']
    declarations = json.loads(article['source_title_declarations_json'])
    assert declarations['title_variants_present'] and not declarations['historical_title_chronology_inferred']
    assert article['published_at_raw'] == '2026-10-20T01:11:20+08:00'
    assert not article['historical_point_in_time'] and not article['per_article_license_verified']


@pytest.mark.parametrize('path', CMONEY_LEARNING_NAVIGATION_PATHS)
def test_v2_does_not_silently_inherit_navigation_scope(supplemental_settings, path):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(supplemental_settings, page(path), path)


@pytest.mark.parametrize('field,value', [
    ('article_path_pattern', '^/.*$'), ('allow_reviewed_supplements', False),
    ('feeds', [CMONEY_LEARNING_ORIGIN + '/feed/']), ('acquisition_mode', 'continuous'),
])
def test_v3_never_enables_recursive_or_unreviewed_capture(navigation_settings, field, value):
    navigation_settings['providers'][0][field] = value
    with pytest.raises(ValueError, match='unreviewed embedded article parser'):
        PolicyRegistry(navigation_settings, base=Path.cwd())


def test_v3_literal_navigation_scope_is_unique_and_disjoint():
    assert len(set(CMONEY_LEARNING_NAVIGATION_PATHS)) == 15
    assert len(set(CMONEY_LEARNING_V3_PATHS)) == 19
    assert not set(CMONEY_LEARNING_NAVIGATION_PATHS).intersection(CMONEY_LEARNING_V2_PATHS)


def test_v3_closed_config_alone_inherits_the_prior_http_refusals():
    from downloader.news_fulltext import load_config
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    proposal = deepcopy(config)
    entry = next(e for e in proposal['providers'] if e['id'] == 'cmoneylearning')
    entry.update(embedded_article_layout='cmoneylearning_visible_article_v3',
                 article_path_pattern=CMONEY_LEARNING_V3_PATH_PATTERN, allow_reviewed_supplements=True,
                 adapter_verification='native_canonical_BlogPosting_editor_clock_v3_retained_navigation_20261022',
                 note=CMONEY_LEARNING_V3_NOTE)
    entry['authorization'].update(reviewed_on='2026-10-22', scope=CMONEY_LEARNING_V3_AUTH_SCOPE)
    candidate = PolicyRegistry(proposal, base=repo).sources['cmoneylearning']
    assert (candidate.data_fingerprint, candidate.authorization_fingerprint) == CMONEY_LEARNING_V3_HTTP_BINDING[:2]
    assert (candidate.http_capacity_source_fingerprint, candidate.http_capacity_authorization_fingerprint) == CMONEY_LEARNING_V3_HTTP_BINDING[2:]
    entry['interval_seconds'] = 11
    changed = PolicyRegistry(proposal, base=repo).sources['cmoneylearning']
    assert not changed.http_capacity_source_fingerprint and not changed.http_capacity_authorization_fingerprint


def test_v3_preserves_completed_v2_heads_and_requires_new_supplement_review(supplemental_settings, navigation_settings, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    root = tmp_path / 'news'
    queue = NewsQueue(root, PolicyRegistry(supplemental_settings, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': CMONEY_LEARNING_ORIGIN + p} for p in CMONEY_LEARNING_V2_PATHS])
        queue.sync_finite_cohorts()
        # Synthetic references test reconciliation, not actual body integrity.
        with queue.db:
            queue.db.execute("UPDATE articles SET state='complete',body_sha256=?", ('a' * 64,))
        before = [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')]
        cohort = dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone())
        receipt = (root / cohort['receipt_path']).read_bytes()
    finally:
        queue.close()


    queue = NewsQueue(root, PolicyRegistry(navigation_settings, base=tmp_path))
    try:
        assert [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before
        with queue.db:
            queue.add_articles([{'url': CMONEY_LEARNING_ORIGIN + p} for p in CMONEY_LEARNING_NAVIGATION_PATHS])
        queue.sync_finite_cohorts()
        assert dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone()) == cohort
        assert (root / cohort['receipt_path']).read_bytes() == receipt
        for path in CMONEY_LEARNING_NAVIGATION_PATHS:
            url = CMONEY_LEARNING_ORIGIN + path
            assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'waiting_cohort'
            assert queue.claim_url(url, 300) is None
        assert queue.db.execute('SELECT count(*) FROM finite_capture_supplements').fetchone()[0] == 0
    finally:
        queue.close()


def person_credit_page(path=CMONEY_LEARNING_NAVIGATION_PATHS[0], *, credit='合成 署名', slug='synthetic_author'):
    """A different declared Person is not the previously hard-coded editor."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(title_variant_page(path), 'lxml')
    soup.select_one('.articleContent__author').string = credit
    person_id = CMONEY_LEARNING_ORIGIN + '/author/' + slug + '/#person'
    for script in soup.select('script[type="application/ld+json"]'):
        item = json.loads(script.get_text())
        if item.get('@type') == 'BlogPosting':
            item['author'] = {'@id': person_id}
        elif item.get('@type') == 'Person':
            item.update(name=credit, url=person_id.removesuffix('#person'))
            item['@id'] = person_id
        script.string = json.dumps(item, ensure_ascii=False)
    return str(soup)


@pytest.mark.parametrize('credit,slug', [('合成 署名', 'synthetic_author'), ('合成筆名', 'synthetic_alias')])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v3_joins_visible_source_Person_without_inventing_editor_or_reporter(navigation_settings, credit, slug, parser):
    navigation_settings['providers'][0]['html_parser'] = parser
    path = CMONEY_LEARNING_NAVIGATION_PATHS[0]
    result = parse(navigation_settings, person_credit_page(path, credit=credit, slug=slug), path)
    assert json.loads(result['authors_json']) == [credit]
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['visible_credit_raw'] == credit
    assert detail['source_person_id'] == CMONEY_LEARNING_ORIGIN + '/author/' + slug + '/#person'
    assert detail['same_response_visible_Person_credit_not_inferred_role']
    assert detail['declared_editor_role_not_inferred_reporter'] is False
    assert not result['per_article_license_verified']


@pytest.mark.parametrize('mutation,error', [
    ('different_visible_credit', 'article_attribution_not_verified'),
    ('different_Person_name', 'article_attribution_not_verified'),
    ('different_Person_URL', 'article_attribution_not_verified'),
    ('foreign_Person_id', 'article_attribution_not_verified'),
    ('multiple_author_references', 'article_attribution_not_verified'),
    ('different_worksFor', 'article_rights_or_scope_excluded'),
    ('different_holder', 'article_rights_or_scope_excluded'),
    ('duplicate_Person', 'article_identity_not_verified'),
])
def test_v3_Person_credit_still_needs_same_response_identity_and_publisher_scope(navigation_settings, mutation, error):
    from bs4 import BeautifulSoup
    path = CMONEY_LEARNING_NAVIGATION_PATHS[0]
    soup = BeautifulSoup(person_credit_page(path), 'lxml')
    if mutation == 'different_visible_credit':
        soup.select_one('.articleContent__author').string = '其他署名'
    for script in soup.select('script[type="application/ld+json"]'):
        item = json.loads(script.get_text())
        if item.get('@type') == 'Person':
            if mutation == 'different_Person_name':
                item['name'] = '其他署名'
            elif mutation == 'different_Person_URL':
                item['url'] += '?other=1'
            elif mutation == 'foreign_Person_id':
                item['@id'] = item['@id'].replace('cmoneylearning.com.tw', 'foreign.test')
            elif mutation == 'different_worksFor':
                item['worksFor'] = {'@id': 'https://foreign.test/#organization'}
            elif mutation == 'duplicate_Person':
                duplicate = soup.new_tag('script', type='application/ld+json')
                duplicate.string = json.dumps(item, ensure_ascii=False)
                soup.head.append(duplicate)
        elif item.get('@type') == 'BlogPosting':
            if mutation == 'multiple_author_references':
                item['author'] = [item['author'], item['author']]
            elif mutation == 'different_holder':
                item['copyrightHolder'] = {'@id': 'https://foreign.test/#organization'}
        script.string = json.dumps(item, ensure_ascii=False)
    with pytest.raises(NewsError, match=error):
        parse(navigation_settings, str(soup), path)


def test_v2_preserves_its_original_single_editor_boundary(supplemental_settings):
    path = CMONEY_LEARNING_SUPPLEMENT_PATHS[0]
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        parse(supplemental_settings, person_credit_page(path), path)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('human_date', ['2026年09月02日', '2026年9月2日'])
def test_v3_accepts_native_human_date_padding_without_changing_machine_clock(navigation_settings, parser, human_date):
    navigation_settings['providers'][0]['html_parser'] = parser
    path = CMONEY_LEARNING_NAVIGATION_PATHS[0]
    value = person_credit_page(path).replace('2026年10月20日', human_date)
    value = value.replace('2026-10-20T01:11:20+08:00', '2026-09-02T01:11:20+08:00')
    value = value.replace('2026-10-19T17:11:20+00:00', '2026-09-01T17:11:20+00:00')
    value = value.replace('2026-10-19T17:11:44+00:00', '2026-09-01T17:11:44+00:00')
    result = parse(navigation_settings, value, path)
    assert result['published_at_raw'] == '2026-09-02T01:11:20+08:00'
    assert result['modified_at_raw'] == '2026-09-01T17:11:44+00:00'
    assert not result['historical_point_in_time']
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        parse(navigation_settings, value.replace(human_date, '2026年9月3日'), path)


def decorative_play_page(path=CMONEY_LEARNING_NAVIGATION_PATHS[0]):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(person_credit_page(path), 'lxml')
    icon = BeautifulSoup('<span class="yt-facade__play"><svg viewBox="0 0 68 48" aria-hidden="true" '
        'focusable="false"><path class="yt-facade__playBg" d="M1 2"/><path fill="#fff" d="M3 4"/></svg></span>', 'html.parser')
    soup.select_one('.articleContent__textContent').append(icon)
    return str(soup)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v3_empty_native_decorative_play_vectors_do_not_hide_source_prose(navigation_settings, parser):
    navigation_settings['providers'][0]['html_parser'] = parser
    path = CMONEY_LEARNING_NAVIGATION_PATHS[0]
    previous = parse(navigation_settings, person_credit_page(path), path)
    current = parse(navigation_settings, decorative_play_page(path), path)
    assert current == previous


@pytest.mark.parametrize('mutation', ['hidden_parent', 'vector_title', 'vector_text', 'vector_script',
    'hidden_prose', 'unreviewed_svg', 'different_vector', 'vector_event'])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v3_play_icon_exception_never_accepts_hidden_prose_or_arbitrary_svg(navigation_settings, mutation, parser):
    from bs4 import BeautifulSoup
    navigation_settings['providers'][0]['html_parser'] = parser
    path = CMONEY_LEARNING_NAVIGATION_PATHS[0]
    soup = BeautifulSoup(decorative_play_page(path), 'html.parser')
    svg = soup.select_one('svg')
    if mutation == 'hidden_parent':
        svg.parent['hidden'] = ''
    elif mutation in ('vector_title', 'vector_text', 'vector_script'):
        node = soup.new_tag({'vector_title': 'title', 'vector_text': 'text', 'vector_script': 'script'}[mutation])
        node.string = '不得接受的隱藏內容'
        svg.append(node)
    elif mutation == 'hidden_prose':
        node = soup.new_tag('p', attrs={'hidden': ''})
        node.string = '不得接受的隱藏內容'
        soup.select_one('.articleContent__textContent').append(node)
    elif mutation == 'unreviewed_svg':
        svg.parent['class'] = ['arbitrary-icon']
    elif mutation == 'different_vector':
        svg.select_one('path').name = 'use'
    elif mutation == 'vector_event':
        svg.select_one('path')['onload'] = 'untrusted()'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(navigation_settings, str(soup), path)


@pytest.fixture
def declared_original_settings(navigation_settings):
    result = deepcopy(navigation_settings)
    entry = result['providers'][0]
    entry.update(embedded_article_layout='cmoneylearning_visible_article_v4',
                 article_path_pattern=CMONEY_LEARNING_V4_PATH_PATTERN)
    entry['authorization']['scope'] = CMONEY_LEARNING_V4_AUTH_SCOPE
    return result


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v4_admits_only_the_new_exact_declared_original(declared_original_settings, navigation_settings, parser):
    path = CMONEY_LEARNING_DECLARED_20261023_PATH
    declared_original_settings['providers'][0]['html_parser'] = parser
    article = parse(declared_original_settings, title_variant_page(path), path)
    assert article['body'].count('不取對數') == 32
    assert 'BlogPosting_v4; not_CMNews_alias_equivalence' in article['article_identity_basis']
    assert not article['historical_point_in_time'] and not article['per_article_license_verified']
    with pytest.raises(NewsError):
        parse(navigation_settings, title_variant_page(path), path)
    assert len(set(CMONEY_LEARNING_V4_PATHS)) == 20
    assert set(CMONEY_LEARNING_V4_PATHS) - set(CMONEY_LEARNING_V3_PATHS) == {path}
    for unreviewed in (path.replace('00958b', '00959b'), path.upper(), path + 'extra/'):
        with pytest.raises(NewsError):
            parse(declared_original_settings, title_variant_page(unreviewed), unreviewed)


@pytest.mark.parametrize('path', CMONEY_LEARNING_V3_PATHS)
def test_v4_does_not_change_completed_v3_body_semantics(navigation_settings, declared_original_settings, path):
    raw = title_variant_page(path)
    old = parse(navigation_settings, raw, path)
    new = parse(declared_original_settings, raw, path)
    assert old.pop('article_identity_basis') != new.pop('article_identity_basis')
    assert old == new


def test_v4_preserves_completed_v3_heads_and_frozen_membership(navigation_settings, declared_original_settings, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    root = tmp_path/'news'
    queue = NewsQueue(root, PolicyRegistry(navigation_settings, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': CMONEY_LEARNING_ORIGIN+p} for p in CMONEY_LEARNING_V3_PATHS])
        queue.sync_finite_cohorts()
        with queue.db:
            queue.db.execute("UPDATE articles SET state='complete',body_sha256=?", ('a'*64,))
        before = [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
        cohort = dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone())
        receipt = (root/cohort['receipt_path']).read_bytes()
    finally:
        queue.close()
    queue = NewsQueue(root, PolicyRegistry(declared_original_settings, base=tmp_path))
    try:
        assert [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before
        assert dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone()) == cohort
        assert (root/cohort['receipt_path']).read_bytes() == receipt
        url = CMONEY_LEARNING_ORIGIN+CMONEY_LEARNING_DECLARED_20261023_PATH
        with queue.db:
            queue.add_articles([{'url': url}])
        queue.sync_finite_cohorts()
        assert queue.db.execute('SELECT state FROM articles WHERE url=?',(url,)).fetchone()[0] == 'waiting_cohort'
        assert queue.claim_url(url, 300) is None
    finally:
        queue.close()


def test_v4_only_the_closed_candidate_inherits_prior_transport_controls():
    from downloader.news_fulltext import load_config
    installed, _, repo = load_config(Path('configs/news_fulltext.json'))
    entry = next(row for row in installed['providers'] if row['id'] == 'cmoneylearning')
    entry.update(embedded_article_layout='cmoneylearning_visible_article_v4',
        article_path_pattern=CMONEY_LEARNING_V4_PATH_PATTERN,
        adapter_verification='native_canonical_BlogPosting_editor_clock_v4_declared_00958B_20261023',
        note=CMONEY_LEARNING_V4_NOTE)
    entry['authorization'].update(reviewed_on='2026-10-23', scope=CMONEY_LEARNING_V4_AUTH_SCOPE)
    source = PolicyRegistry(installed, base=repo).sources['cmoneylearning']
    assert (source.data_fingerprint, source.authorization_fingerprint,
        source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint) == CMONEY_LEARNING_V4_HTTP_BINDING
    entry['parallelism'] += 1
    altered = PolicyRegistry(installed, base=repo).sources['cmoneylearning']
    assert not altered.http_capacity_source_fingerprint and not altered.http_capacity_authorization_fingerprint
