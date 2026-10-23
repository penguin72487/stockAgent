"""Synthetic native DOM only: no copyrighted source prose in the repository."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_bastillepost import (
    BASTILLE_BODY_SELECTOR, BASTILLE_ORIGIN, BASTILLE_PATH_PATTERN, BASTILLE_TITLE_SELECTOR,
    bastillepost_article_id, bastillepost_transport_verified,
)
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article

ID_URL = BASTILLE_ORIGIN + '/hongkong/article/10123210/'
URL = BASTILLE_ORIGIN + '/hongkong/article/10123210-%e5%90%88%e6%88%90'
TITLE = '合成原值 -12.50 與 0%'
PUBLISHED = '2022-02-08T11:56:00+08:00'
MODIFIED = '2022-02-08T12:02:42+08:00'


@pytest.fixture
def settings():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [{
        'id':'bastillepost', 'domains':['www.bastillepost.com'], 'enabled':True,
        'acquisition_mode':'continuous', 'private_known_url_scope':'personal_noncommercial_originals_v1',
        'interval_seconds':2, 'parallelism':1, 'html_parser':'lxml',
        'embedded_article_layout':'bastillepost_visible_article_v1',
        'article_path_pattern':BASTILLE_PATH_PATTERN, 'article_selectors':[BASTILLE_BODY_SELECTOR],
        'title_selector':BASTILLE_TITLE_SELECTOR, 'exclude_selectors':[],
        'require_configured_container':True, 'require_author_attribution':True,
        'authorization':{'kind':'personal_research', 'reviewed_on':'2026-10-21',
            'evidence_url':BASTILLE_ORIGIN + '/hongkong/terms',
            'scope':'Private locally known public native text; not publication or training/export.',
            'automated_access_review':'no_explicit_prohibition_identified'},
    }]
    return config


def page(*, schema_edit=None, body=None):
    obj = {'@context':'http://schema.org', '@type':'NewsArticle', 'mainEntityOfPage':URL,
        'headline':TITLE, 'name':TITLE, 'description':'合成摘要不可代替正文',
        'author':{'@type':'Person', 'name':'巴士的報'},
        'publisher':{'@type':'Organization', 'name':'Bastillepost 巴士的報', 'url':BASTILLE_ORIGIN},
        'datePublished':PUBLISHED, 'dateModified':MODIFIED}
    if schema_edit:
        schema_edit(obj)
    if body is None:
        body = ('<p class="font-size-scaler-control">' + '原始數字 -12.50、0%、一千股都保留。' * 8
            + '<br><br>第二段保留(ms.)</p>'
            '<a class="image-lightbox" href="https://image.example.test/x.jpg"><img src="/x.jpg" alt="不是可見說明"></a>'
            '<figure><img src="/figure.jpg"><figcaption>可見圖片說明與攝影信用</figcaption></figure>'
            '<table><tr><td>0</td><td>-12.50</td></tr></table>')
    return f'''<html><head><link rel="canonical" href="{URL}">
      <meta property="og:url" content="{ID_URL}"><meta property="og:type" content="website">
      <meta property="og:title" content="{TITLE}">
      <meta property="article:published_time" content="{PUBLISHED}">
      <script type="application/ld+json">{json.dumps(obj,ensure_ascii=False)}</script></head><body>
      <main id="main"><div class="article-page__column __main">
      <div class="banner-image">正文外廣告不可收</div><div class="news-large">
        <div class="news-large__title"><div class="news-large__title-main">
          <h2 class="h2 bpcategory-special-color"><strong>{TITLE}</strong></h2>
          <span class="news-large__data date">2022年02月08日 11:56 最後更新：12:02</span>
        </div></div><div class="news-large__socials" style="display:none">社群工具</div>
      </div><div class="article-body">{body}</div>
      <div class="fb-comments">評論不可收</div></div>
      <aside><h2>另一篇文章</h2><p>鄰文不可收</p></aside></main></body></html>'''


def parse(settings, html=None, *, url=URL):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['bastillepost']
    return extract_article((page() if html is None else html).encode(), source, page_url=url)


@pytest.mark.parametrize('parser',['lxml','html.parser'])
def test_native_body_complete_values_captions_and_declarations(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    result = parse(settings)
    assert result['title'] == TITLE
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert json.loads(result['authors_json']) == ['巴士的報']
    assert result['attribution_status'] == 'source_declared_corporate_author_not_verified_person'
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert result['body'].count('原始數字') == 8
    assert result['body'].count('第二段保留(ms.)') == 1
    assert result['body'].count('可見圖片說明與攝影信用') == 1
    for bad in ('合成摘要','正文外廣告','評論不可收','鄰文不可收','不是可見說明','社群工具'):
        assert bad not in result['body']
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['schema_author_raw'] == {'@type':'Person','name':'巴士的報'}
    assert not detail['native_human_byline_verified'] and 'not_verified_person' in detail['role']
    notices = json.loads(result['verified_article_notices_json'])
    assert not notices[0]['visible_revision_has_date']
    assert sum(n['kind']=='unfetched_inline_image' for n in notices) == 2
    assert all(not n['media_requested'] for n in notices[1:])


@pytest.mark.parametrize('edit,code',[
    (lambda s:s.replace('rel="canonical" href="'+URL,'rel="canonical" href="'+URL+'0',1),None),
    (lambda s:s.replace('/article/10123210/','/article/99999/',1),'article_identity_not_verified'),
    (lambda s:s.replace('<h2 class="h2 bpcategory-special-color">','<h2 hidden class="h2 bpcategory-special-color">'),'article_identity_not_verified'),
    (lambda s:s.replace('<strong>','<strong class="visually-hidden">'),'article_identity_not_verified'),
    (lambda s:s.replace('class="article-body"','class="article-body d-none"'),'article_identity_not_verified'),
    (lambda s:s.replace('<br><br>','<br><span aria-hidden="true">隱藏內容</span><br>'),'article_identity_not_verified'),
    (lambda s:s.replace('<br><br>','<br><script>unknown()</script><br>'),'article_identity_not_verified'),
    (lambda s:s.replace('<br><br>','<br><iframe src="/video"></iframe><br>'),'article_identity_not_verified'),
    (lambda s:s.replace('<br><br>','<br><p>轉載自其他網站</p><br>'),'article_rights_or_scope_excluded'),
    (lambda s:s.replace('<table>','<table onclick="unknown()">'),'article_identity_not_verified'),
    (lambda s:s.replace('2022年02月08日 11:56','2022年02月09日 11:56'),'invalid_bulk_publication_clock'),
    (lambda s:s.replace('最後更新：12:02','最後更新：12:03'),'invalid_bulk_publication_clock'),
    (lambda s:s.replace('"巴士的報"','"另一個作者"'),'article_attribution_not_verified'),
    (lambda s:s.replace('</main>','<div class="article-body">其他正文</div></main>'),'article_identity_not_verified'),
])
def test_conflicts_hidden_unknown_and_third_party_are_not_silently_accepted(settings,edit,code):
    # A same-ID slug difference is admitted only by the actual source
    # declaration, never by a generated request, global alias or title match.
    if code is None:
        assert parse(settings,edit(page()))['title']==TITLE
    else:
        with pytest.raises(NewsError,match='^'+code+'$'):
            parse(settings,edit(page()))


@pytest.mark.parametrize('schema_edit,code',[
    (lambda x:x.update(articleBody='合成摘要'),'article_identity_not_verified'),
    (lambda x:x.update(mainEntityOfPage=URL.replace('10123210','99999')),'article_identity_not_verified'),
    (lambda x:x.update(headline='另一標題'),'article_identity_not_verified'),
    (lambda x:x.update(name='另一標題'),'article_identity_not_verified'),
    (lambda x:x['publisher'].update(name='另一個來源'),'article_identity_not_verified'),
    (lambda x:x.update(author={'@type':'Organization','name':'巴士的報'}),'article_attribution_not_verified'),
    (lambda x:x.update(datePublished='2022-02-30T11:56:00+08:00'),'invalid_bulk_publication_clock'),
    (lambda x:x.update(dateModified='2022-02-07T12:02:42+08:00'),'invalid_bulk_publication_clock'),
    (lambda x:x.update(dateModified='2022-02-08T04:02:42Z'),'invalid_bulk_publication_clock'),
])
def test_no_metadata_coercion_or_summary_substitution(settings,schema_edit,code):
    with pytest.raises(NewsError,match='^'+code+'$'):
        parse(settings,page(schema_edit=schema_edit))


@pytest.mark.parametrize('url',[
    URL.replace('www.',''), URL+'?a=1', URL+'#part', URL.replace('https:','http:'),
    URL.replace('/hongkong/','/macau/'), URL+'%2fpreview', URL+'%3fpaid', URL+'%zz',
    URL.replace('10123210','0010123210'), URL.replace('www.','user:pass@www.'),
])
def test_no_unreviewed_routes_or_encoded_query_aliases(url):
    assert bastillepost_article_id(url) is None


def hop(from_url,to_url):
    return RedirectHop(from_url,302,to_url,to_url,'2026-10-21T14:41:42+00:00','a'*64,0)


def test_actual_same_id_redirect_and_http_upgrade_require_witness():
    http_url=ID_URL.replace('https:','http:')
    response=Response(URL,200,{},b'',(hop(http_url,ID_URL),hop(ID_URL,URL)))
    assert bastillepost_article_id(http_url,requested=True)=='10123210'
    assert bastillepost_transport_verified(response,http_url)
    assert not bastillepost_transport_verified(replace(response,redirects=()),http_url)


@pytest.mark.parametrize('response,request_url',[
    (Response(URL,200,{},b''),ID_URL),
    (Response(URL,200,{},b'',(hop(ID_URL,URL.replace('10123210','99999')),)),ID_URL),
    (Response(URL,200,{},b'',(hop(ID_URL,BASTILLE_ORIGIN+'/'),hop(BASTILLE_ORIGIN+'/',URL))),ID_URL),
    (Response(URL,200,{},b'',(hop(ID_URL,'https://other.test/article/10123210'),hop('https://other.test/article/10123210',URL))),ID_URL),
    (Response(URL,200,{},b'',(replace(hop(ID_URL,URL),location='/hongkong/article/99999'),)),ID_URL),
    (Response(URL,200,{},b'',(replace(hop(ID_URL,URL),observed_at_utc='not-a-clock'),)),ID_URL),
    (Response(URL,200,{},b'',(replace(hop(ID_URL,URL),response_sha256='missing'),)),ID_URL),
    (Response(URL,200,{},b'',(replace(hop(ID_URL,URL),response_bytes=-1),)),ID_URL),
    (Response(URL,200,{},b'',(hop(ID_URL,ID_URL),hop(ID_URL,URL))),ID_URL),
    (Response(URL,200,{},b'',(hop(ID_URL,ID_URL.replace('https:','http:')),hop(ID_URL.replace('https:','http:'),URL))),ID_URL),
])
def test_transport_cannot_invent_or_omit_redirect_identity(response,request_url):
    assert not bastillepost_transport_verified(response,request_url)


@pytest.mark.parametrize('version', [1, 2, 3, 4, 5, 6, 7])
def test_fetch_keeps_literal_frontier_and_same_id_actual_chain(settings, version):
    settings['providers'][0]['embedded_article_layout'] = f'bastillepost_visible_article_v{version}'
    registry=PolicyRegistry(settings,base=Path.cwd())
    response=Response(URL,200,{'content-type':'text/html'},page().encode(),(hop(ID_URL,URL),))
    http=SimpleNamespace(registry=registry,get=lambda *a,**kw:response)
    result=fetch_article(http,{'url':ID_URL,'id':bytes.fromhex('12'*16)})
    assert result['requested_url']==ID_URL and result['final_url']==URL
    assert 'literal_same_id_source_redirect' in result['url_resolution_basis']


@pytest.mark.parametrize('version', [1, 2, 3, 4, 5, 6, 7])
def test_fetch_rejects_different_story_and_attach_failure_witness(settings, version):
    settings['providers'][0]['embedded_article_layout'] = f'bastillepost_visible_article_v{version}'
    registry=PolicyRegistry(settings,base=Path.cwd())
    response=Response(URL.replace('10123210','99999'),200,{'content-type':'text/html'},page().encode())
    http=SimpleNamespace(registry=registry,get=lambda *a,**kw:response)
    with pytest.raises(NewsError,match='article_identity_not_verified') as exc:
        fetch_article(http,{'url':ID_URL})
    assert exc.value.failure_response.requested_url==ID_URL


@pytest.mark.parametrize('version', [1, 2, 3, 4, 5, 6, 7])
@pytest.mark.parametrize('chain', [(), (hop(URL, ID_URL),), (hop(ID_URL, ID_URL),)])
def test_fetch_each_native_version_rejects_missing_or_broken_actual_chain(settings, version, chain):
    settings['providers'][0]['embedded_article_layout'] = f'bastillepost_visible_article_v{version}'
    response = Response(URL, 200, {'content-type':'text/html'}, page().encode(), chain)
    http = SimpleNamespace(registry=PolicyRegistry(settings, base=Path.cwd()), get=lambda *a, **kw:response)
    with pytest.raises(NewsError, match='article_identity_not_verified') as exc:
        fetch_article(http, {'url':ID_URL, 'id':bytes.fromhex('12'*16)})
    assert exc.value.failure_response.requested_url == ID_URL


@pytest.mark.parametrize('field,value',[
    ('domains',['bastillepost.com']), ('article_selectors',['body']), ('exclude_selectors',['figcaption']),
    ('feeds',[BASTILLE_ORIGIN+'/feed']), ('sitemaps',[BASTILLE_ORIGIN+'/sitemap.xml']),
    ('require_author_attribution',False), ('require_configured_container',False),
    ('article_path_pattern','.*'), ('title_selector','h1'), ('allow_short_configured_body',True),
    ('author_selectors',['.footer']), ('acquisition_mode','finite'),
])
def test_config_cannot_broaden_body_rights_or_discovery(settings,field,value):
    settings['providers'][0][field]=value
    with pytest.raises(ValueError):
        PolicyRegistry(settings,base=Path.cwd())


def test_empty_or_short_body_does_not_fall_back_to_schema_or_neighbors(settings):
    for body in ('','<p>圖片說明</p><img src="/a.jpg">'):
        with pytest.raises(NewsError,match='body_not_found'):
            parse(settings,page(body=body))


def test_pure_adapter_does_not_mutate_unrelated_regions(settings):
    from downloader.news_fulltext_bastillepost import bastillepost_visible_article
    soup=BeautifulSoup(page(),'lxml')
    aside=str(soup.aside)
    _,scope=bastillepost_visible_article(soup,page_url=URL)
    assert str(soup.aside)==aside
    assert len(scope.select(BASTILLE_BODY_SELECTOR))==1


@pytest.mark.parametrize('parser',['lxml','html.parser'])
def test_failure_metadata_preserves_dom_and_unknown_credit_without_body_or_permission(parser):
    from downloader.news_fulltext_bastillepost import bastillepost_validation_metadata
    soup=BeautifulSoup(page(schema_edit=lambda x:x['author'].update(name='合成未知作者')),parser)
    before=str(soup)
    facts=bastillepost_validation_metadata(soup,page_url=URL)
    assert before==str(soup)
    assert facts['native_body_count']==facts['native_scope_count']==1
    assert facts['schema']['author']['name']['value']=='合成未知作者'
    assert not facts['article_identity_or_scope_granted'] and facts['additional_requests']==0
    assert not facts['body_prose_retained'] and not facts['raw_response_retained']
    assert '原始數字' not in json.dumps(facts,ensure_ascii=False)
    assert facts['native_body_shapes'][0]['tag_counts']['figcaption']==1


def test_bounded_native_tree_rejects_instead_of_partial_truncation(settings):
    body='<p>'+'原始完整正文。'*20+'</p>'+'<br>'*4097
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(settings,page(body=body))


@pytest.mark.parametrize('version', [1, 2, 3, 4])
def test_owner_retains_received_native_failure_metadata_in_original_budget(settings,tmp_path,version):
    settings = versioned(settings, version)
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_failure_diagnostics import fail
    raw=Response(URL,200,{'content-type':'text/html'},page(
        schema_edit=lambda x:x['author'].update(name='合成未知作者')).encode())
    policy=PolicyRegistry(settings,base=tmp_path)
    with closing(NewsQueue(tmp_path/'store',policy)) as queue:
        for _ in range(3):
            fail(queue,raw,code='article_attribution_not_verified',received_at_utc='2026-10-21T15:00:00+00:00')
        paths=sorted((queue.root/'identity_metadata').rglob('*.json'))
        assert len(paths)==2
        for path in paths:
            r=json.loads(path.read_bytes())
            facts=r['facts']['bastillepost_native_validation']
            assert facts['schema']['author']['name']['value']=='合成未知作者'
            assert facts['native_body_count']==1
            assert not facts['body_prose_retained'] and not r['raw_response_retained']
            assert not r['accepted_as_article'] and '原始數字' not in path.read_text()
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0]==0


def versioned(settings, version=2):
    result=deepcopy(settings)
    result['providers'][0]['embedded_article_layout']=f'bastillepost_visible_article_v{version}'
    return result


@pytest.fixture
def boundary_settings(settings, tmp_path):
    result = versioned(settings)
    result.update(root=str(tmp_path), min_free_disk_bytes=1, exclusive_http_owner=False, workers=2)
    result['adaptive_rate'] = {'enabled': False}
    return result


def boundary_failure(queue, **overrides):
    import time
    values = {'state': 'body_not_found', 'error_code': 'body_not_found', 'attempts': 7,
        'due': time.time()-30, 'last_checked_at_utc': '2026-01-01T00:00:00+00:00'}
    values.update(overrides)
    with queue.db:
        queue.add_articles([{'url': URL}])
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in values)+' WHERE url=?',
            (*values.values(), URL))
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


def boundary_submit(queue, config):
    from downloader.news_fulltext_candidate_diagnostics import submit_candidate
    return submit_candidate(queue.root, config, queue.registry, URL, 'bastillepost_visible_article_v2',
        reason='Review nested written container and active-node signatures only', evidence_contract_version=8)


def boundary_http(queue, html=None):
    def get(url):
        http.calls.append(url)
        return Response(url, 200, {'content-type': 'text/html'}, (page() if html is None else html).encode())
    http = SimpleNamespace(registry=queue.registry, get=get, calls=[], provider_not_before={},
        blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())
    return http


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
def test_resident_written_boundary_distinguishes_nested_short_and_active_nodes_without_text(engine):
    from downloader.news_fulltext_bastillepost import bastillepost_written_boundary_metadata
    raw = page(body='<p class="font-size-scaler-control">不可輸出的短正文 -12.50。</p>'
        '<script>secret_script_value()</script><iframe src="https://media.example.test/v?token=private"></iframe>')
    soup = BeautifulSoup(raw, engine)
    body = soup.select_one('.article-body')
    wrapper = soup.new_tag('section', attrs={'class': 'native-written-wrapper'})
    body.wrap(wrapper)
    before = str(soup)
    facts = bastillepost_written_boundary_metadata(soup, page_url=URL)
    native = facts['scopes'][0]['bodies'][0]
    assert not native['direct_native_child'] and native['parent']['tag'] == 'section'
    assert native['paragraphs_sampled'][0]['sentence_ending']
    assert native['tag_counts']['script'] == native['tag_counts']['iframe'] == 1
    assert native['active_nodes'][1]['external_origin'] == 'https://media.example.test'
    assert len(native['active_nodes'][0]['inline_sha256']) == 64
    exported = json.dumps(facts, ensure_ascii=False)
    assert all(value not in exported for value in ('不可輸出的短正文', 'secret_script_value', 'token=', TITLE))
    assert not facts['article_identity_or_scope_granted'] and not facts['body_or_names_or_script_values_exported']
    assert str(soup) == before


def test_written_boundary_element_and_paragraph_output_are_bounded():
    from downloader.news_fulltext_bastillepost import bastillepost_written_boundary_metadata
    soup = BeautifulSoup(page(body='<p>不得輸出</p>'*2100), 'lxml')
    facts = bastillepost_written_boundary_metadata(soup, page_url=URL)
    scope = facts['scopes'][0]
    assert scope['elements_truncated'] and scope['elements_sampled'] == 2048
    body = scope['bodies'][0]
    assert body['elements_truncated'] and body['paragraphs_truncated']
    assert len(body['paragraphs_sampled']) == 16 and len(json.dumps(facts)) < 10000


def test_resident_bastille_current_source_only_and_no_current_parser_replay(boundary_settings, tmp_path, monkeypatch):
    from downloader import news_fulltext_candidates as candidates
    registry = PolicyRegistry(boundary_settings, base=tmp_path)
    source = registry.sources['bastillepost']
    assert candidates.native_candidate_source(boundary_settings, registry, tmp_path, source.embedded_article_layout) is source
    monkeypatch.setattr(candidates, 'extract_article', lambda *a, **k: pytest.fail('current parser is not a candidate'))
    raw = Response(URL, 200, {'content-type': 'text/html'}, page().encode())
    facts = candidates.candidate_response_metadata(raw, source, source, evidence_contract_version=8, requested_url=URL)
    assert not facts['current_parser_replayed'] and not facts['identity_or_alias_accepted']
    assert not facts['raw_or_names_or_body_retained'] and not facts['accepted_as_article']
    for evidence in (1, 6, 7, 11, True):
        assert not candidates.candidate_evidence_allowed(source, source, evidence)
    for raw in (Response(URL, 403, {'content-type': 'text/html'}, b'no'),
                Response(URL, 200, {'content-type': 'text/html'}, b'x'*((2 << 20)+1))):
        with pytest.raises(NewsError):
            candidates.candidate_response_metadata(raw, source, source, evidence_contract_version=8, requested_url=URL)


def test_resident_bastille_once_only_head_unchanged_and_restart_safe(boundary_settings, tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as jobs
    from downloader.news_fulltext_queue import NewsQueue
    queue = NewsQueue(tmp_path, PolicyRegistry(boundary_settings, base=tmp_path))
    try:
        before = boundary_failure(queue)
        request = boundary_submit(queue, boundary_settings)
        assert request['submitted'] and request['publisher_requests'] == 0
        jobs.pump_candidates(queue)
        http = boundary_http(queue)
        job = jobs.claim_candidate(queue, http, boundary_settings, 'bastillepost')
        result = jobs.fetch_candidate(http, job)
        jobs.finish_candidate(queue, job, result=result)
        assert http.calls == [URL]
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert boundary_submit(queue, boundary_settings)['state'] == 'observed'
    finally:
        queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(boundary_settings, base=tmp_path))
    try:
        assert not boundary_submit(queue, boundary_settings)['submitted']
        assert jobs.claim_candidate(queue, boundary_http(queue), boundary_settings, 'bastillepost') is None
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    finally:
        queue.close()


@pytest.mark.parametrize('override', [
    {'due': 4102444800.0}, {'state': 'complete'}, {'body_sha256': 'a'*64},
    {'lease_owner': 'other', 'lease_until': 4102444800.0},
    {'state': 'paywall_or_login', 'error_code': 'paywall_or_login'},
    {'state': 'article_rights_or_scope_excluded', 'error_code': 'article_rights_or_scope_excluded'},
])
def test_resident_bastille_never_rearms_saved_future_leased_or_scope_failures(boundary_settings, tmp_path, override):
    from downloader.news_fulltext_queue import NewsQueue
    queue = NewsQueue(tmp_path, PolicyRegistry(boundary_settings, base=tmp_path))
    try:
        boundary_failure(queue, **override)
        with pytest.raises(ValueError):
            boundary_submit(queue, boundary_settings)
    finally:
        queue.close()


@pytest.mark.parametrize('pause', ['origin', 'provider'])
def test_resident_bastille_existing_refusal_is_not_circumvented(boundary_settings, tmp_path, pause):
    from downloader import news_fulltext_candidate_diagnostics as jobs
    from downloader.news_fulltext_queue import NewsQueue
    queue = NewsQueue(tmp_path, PolicyRegistry(boundary_settings, base=tmp_path))
    try:
        boundary_failure(queue)
        boundary_submit(queue, boundary_settings)
        jobs.pump_candidates(queue)
        http = boundary_http(queue)
        if pause == 'origin':
            http.blocked_robots_origins = lambda p: {BASTILLE_ORIGIN}
        else:
            http.provider_not_before['bastillepost'] = 4102444800.0
        assert jobs.claim_candidate(queue, http, boundary_settings, 'bastillepost') is None
        assert http.calls == []
    finally:
        queue.close()


def test_resident_bastille_unknown_result_is_not_retried(boundary_settings, tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as jobs
    from downloader.news_fulltext_queue import NewsQueue
    queue = NewsQueue(tmp_path, PolicyRegistry(boundary_settings, base=tmp_path))
    try:
        boundary_failure(queue)
        boundary_submit(queue, boundary_settings)
        jobs.pump_candidates(queue)
        assert jobs.claim_candidate(queue, boundary_http(queue), boundary_settings, 'bastillepost')
    finally:
        queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(boundary_settings, base=tmp_path))
    try:
        jobs.pump_candidates(queue)  # Recovery belongs to the mailbox owner pump.
        assert boundary_submit(queue, boundary_settings)['state'] == 'uncertain_outcome'
        assert jobs.claim_candidate(queue, boundary_http(queue), boundary_settings, 'bastillepost') is None
    finally:
        queue.close()


def test_resident_bastille_fixed_key_not_parser_hash_and_cross_provider_or_query_rejected(boundary_settings, tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as jobs
    from downloader.news_fulltext_queue import NewsQueue
    queue = NewsQueue(tmp_path, PolicyRegistry(boundary_settings, base=tmp_path))
    try:
        boundary_failure(queue)
        target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert jobs._key(target, 'a'*64, evidence_contract_version=8) == jobs._key(target, 'b'*64, evidence_contract_version=8)
        with pytest.raises(ValueError):
            jobs._key({**target, 'provider': 'pocket'}, 'a'*64, evidence_contract_version=8)
        with pytest.raises(ValueError, match='literal_current_bastillepost_route_required'):
            jobs.submit_candidate(tmp_path, boundary_settings, queue.registry, URL+'?current_cat=138496',
                'bastillepost_visible_article_v2', reason='not a reviewed literal route', evidence_contract_version=8)
    finally:
        queue.close()


def test_resident_bastille_cli_uses_mailbox_not_another_http_owner(boundary_settings, tmp_path, monkeypatch):
    from scripts import probe_news_sources as cli
    from downloader.news_fulltext_queue import NewsQueue
    queue = NewsQueue(tmp_path, PolicyRegistry(boundary_settings, base=tmp_path))
    try:
        before = boundary_failure(queue)
        monkeypatch.setattr(cli, 'load_config', lambda path: (boundary_settings, queue.registry, tmp_path))
        monkeypatch.setattr(cli, 'NewsHTTP', lambda *a, **k: pytest.fail('submitter must not own HTTP'))
        output = tmp_path/'submitted.json'
        assert cli.main(['--probe', URL, '--candidate-embedded-layout', 'bastillepost_visible_article_v2',
            '--require-current-missing-body-failure', '--failure-code', 'body_not_found',
            '--enqueue-candidate', '--candidate-evidence-contract', '8', '--output', str(output)]) == 0
        assert json.loads(output.read_text())['submitted']
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    finally:
        queue.close()


def test_resident_bastille_actual_same_id_redirect_required(boundary_settings, tmp_path):
    from downloader.news_fulltext_candidates import candidate_response_metadata
    source = PolicyRegistry(boundary_settings, base=tmp_path).sources['bastillepost']
    hop = RedirectHop(from_url=ID_URL, status=301, location=URL, to_url=URL,
        observed_at_utc='2026-01-01T00:00:00+00:00', response_sha256='a'*64, response_bytes=0)
    response = Response(URL, 200, {'content-type': 'text/html'}, page().encode(), redirects=(hop,))
    facts = candidate_response_metadata(response, source, source, evidence_contract_version=8, requested_url=ID_URL)
    assert facts['redirect_count'] == 1 and not facts['identity_or_alias_accepted']
    for altered in (Response(URL, 200, response.headers, response.body),
                    Response(URL, 200, response.headers, response.body, redirects=(replace(hop, from_url=URL),))):
        with pytest.raises(NewsError, match='candidate_literal_native_transport_not_verified'):
            candidate_response_metadata(altered, source, source, evidence_contract_version=8, requested_url=ID_URL)


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_v2_complete_native_short_prose_is_not_a_100_character_proxy(settings,engine):
    config=versioned(settings);config['providers'][0]['html_parser']=engine
    prose='合成企業公布，融資金額為10億元，期限5年，原始負值 -12.50 與比率0%不變。'
    raw=page(body='<p class="font-size-scaler-control">'+prose+'<br><br></p>'
        '<a class="image-lightbox" href="https://image.example.test/x.jpg"><img src="/x.jpg"></a>')
    result=parse(config,raw)
    assert result['body']==prose and result['body'].count('-12.50')==1
    assert result['attribution_status']=='source_declared_corporate_author_not_verified_person'
    assert 'complete_short_native_primary_prose' in result['body_completeness']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    notices=json.loads(result['verified_article_notices_json'])
    assert any(n['kind']=='unfetched_inline_image' and not n['media_requested'] for n in notices)
    with pytest.raises(NewsError,match='body_not_found'):
        parse(settings,raw)


@pytest.mark.parametrize('body',[
    '', '<p>只有未經核對的簡短說明。</p>',
    '<figcaption>圖片說明。</figcaption><img src="/x.jpg">',
    '<div class="font-size-scaler-control">未經核對的區塊。</div>',
    '<p class="font-size-scaler-control">未完成的截斷內容……</p>',
    '<p class="font-size-scaler-control">尚未完成的截斷內容...</p>',
    '<p class="font-size-scaler-control">只有圖片說明沒有完整句子</p>',
    '<p class="font-size-scaler-control">原生短段落。</p><div>未知額外短區域。</div>',
])
def test_v2_short_prose_cannot_be_an_empty_caption_fragment_or_generic_flag(settings,body):
    with pytest.raises(NewsError,match='body_not_found'):
        parse(versioned(settings),page(body=body))


def test_v2_leaves_previously_accepted_native_output_equal(settings):
    assert parse(versioned(settings))==parse(settings)


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_v2_release_only_label_preserves_earlier_raw_revision_without_backdating(settings,engine):
    config=versioned(settings);config['providers'][0]['html_parser']=engine
    earlier='2022-02-08T01:00:17+08:00'
    raw=page(schema_edit=lambda x:x.update(dateModified=earlier)).replace(' 最後更新：12:02','')
    result=parse(config,raw)
    assert result['published_at_raw']==PUBLISHED and result['modified_at_raw']==earlier
    assert not result['historical_point_in_time']
    notice=json.loads(result['verified_article_notices_json'])[0]
    assert notice['modified_precedes_release'] and not notice['visible_revision_label_present']
    assert not notice['earlier_modified_promoted_to_publication']
    with pytest.raises(NewsError,match='invalid_bulk_publication_clock'):
        parse(settings,raw)


@pytest.mark.parametrize('mutation',[
    lambda s:s.replace('11:56</span>','11:57</span>'),
    lambda s:s.replace('"2022-02-08T01:00:17+08:00"','"2022-02-08T01:00:17"'),
    lambda s:s.replace('</head>','<meta property="article:modified_time" content="2022-02-08T01:01:17+08:00"></head>'),
])
def test_v2_release_only_is_not_an_exception_for_missing_or_conflicting_clocks(settings,mutation):
    raw=page(schema_edit=lambda x:x.update(dateModified='2022-02-08T01:00:17+08:00')).replace(' 最後更新：12:02','')
    with pytest.raises(NewsError,match='invalid_bulk_publication_clock'):
        parse(versioned(settings),mutation(raw))


def test_v2_only_literal_reviewed_current_cat_query_with_actual_transport_witness(settings):
    request=URL+'?current_cat=5'
    assert bastillepost_article_id(request,requested=True) is None
    assert bastillepost_article_id(request,requested=True,adapter_contract_version=2)=='10123210'
    response=Response(URL,200,{'content-type':'text/html'},page().encode(),(hop(request,URL),))
    assert bastillepost_transport_verified(response,request,adapter_contract_version=2)
    assert not bastillepost_transport_verified(replace(response,redirects=()),request,adapter_contract_version=2)
    registry=PolicyRegistry(versioned(settings),base=Path.cwd())
    calls=[]
    http=SimpleNamespace(registry=registry,get=lambda url,**kwargs:(calls.append(url) or response))
    result=fetch_article(http,{'url':request,'id':bytes.fromhex('12'*16)})
    assert calls==[request] and result['requested_url']==request and result['final_url']==URL
    assert 'same_id_source_redirect' in result['url_resolution_basis']


@pytest.mark.parametrize('query',[
    'current_cat=6','current_cat=05','current_cat=5&current_cat=5','current_cat=5&other=1',
    'CURRENT_CAT=5','current_cat=%35','current_cat=5#fragment',
])
def test_v2_query_scope_is_not_an_arbitrary_tracking_or_alias_exception(query):
    assert bastillepost_article_id(URL+'?'+query,requested=True,adapter_contract_version=2) is None


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_v2_explicit_other_publisher_credit_cannot_be_overwritten_by_corporate_schema(settings,engine):
    config=versioned(settings);config['providers'][0]['html_parser']=engine
    raw=page(body='<p class="font-size-scaler-control">（星島日報報道）'+'合成完整原始報道。'*20+'</p>')
    with pytest.raises(NewsError,match='article_rights_or_scope_excluded'):
        parse(config,raw)


def test_v2_repair_binds_complete_predecessor_and_keeps_exact_http_and_authorization_identity(settings):
    from downloader.news_fulltext_queue import native_text_parser_repair
    old=PolicyRegistry(settings,base=Path.cwd()).sources['bastillepost']
    new=PolicyRegistry(versioned(settings),base=Path.cwd()).sources['bastillepost']
    assert new.data_fingerprint!=old.data_fingerprint
    assert new.parser_repair_fingerprint!=old.parser_repair_fingerprint
    assert new.http_capacity_source_fingerprint==new.parser_repair_prior_capture_fingerprint==old.data_fingerprint
    assert new.authorization_fingerprint==old.authorization_fingerprint
    assert native_text_parser_repair(new,old.data_fingerprint)
    for field,value in [('interval_seconds',3),('name','changed source description')]:
        config=versioned(settings);config['providers'][0][field]=value
        changed=PolicyRegistry(config,base=Path.cwd()).sources['bastillepost']
        assert changed.http_capacity_source_fingerprint!=old.data_fingerprint
        assert not native_text_parser_repair(changed,old.data_fingerprint)
    config=versioned(settings);config['providers'][0]['authorization']['scope']+=' Changed scope.'
    changed=PolicyRegistry(config,base=Path.cwd()).sources['bastillepost']
    assert changed.authorization_fingerprint!=old.authorization_fingerprint
    assert not native_text_parser_repair(changed,old.data_fingerprint)


@pytest.mark.parametrize('state,suffix,changes,expected',[
    ('body_not_found','',{},'retry'),
    ('invalid_bulk_publication_clock','',{},'retry'),
    ('unsupported_article_path','?current_cat=5',{},'retry'),
    ('unsupported_article_path','?current_cat=6',{},'unsupported_article_path'),
    ('article_identity_not_verified','',{},'article_identity_not_verified'),
    ('article_rights_or_scope_excluded','',{},'article_rights_or_scope_excluded'),
    ('robots_denied','',{},'robots_denied'),
    ('http_429','',{},'http_429'),
    ('http_404','',{},'http_404'),
    ('complete','',{},'complete'),
    ('body_not_found','',{'lease_owner':'other'},'body_not_found'),
    ('body_not_found','',{'lease_until':1},'body_not_found'),
    ('body_not_found','',{'body_bytes':1},'body_not_found'),
    ('body_not_found','',{'body_sha256':'a'*64},'body_not_found'),
    ('body_not_found','',{'parquet_path':'retained.parquet'},'body_not_found'),
    ('body_not_found','',{'captured_at_utc':'2026-01-01T00:00:00+00:00'},'body_not_found'),
    ('body_not_found','',{'last_checked_at_utc':None},'body_not_found'),
    ('body_not_found','',{'last_checked_at_utc':'invalid'},'body_not_found'),
    ('body_not_found','',{'due':lambda:__import__('time').time()+86400},'body_not_found'),
])
def test_v2_automatic_repair_changes_only_due_empty_attested_failures_once(settings,tmp_path,state,suffix,changes,expected):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_reviewed_repairs import failure,head
    changes={key:value() if callable(value) else value for key,value in changes.items()}
    root=tmp_path/'store';url=URL+suffix
    with closing(NewsQueue(root,PolicyRegistry(settings,base=tmp_path))) as queue:
        before=failure(queue,url,state=state,error_code=state,**changes)
    registry=PolicyRegistry(versioned(settings),base=tmp_path)
    with closing(NewsQueue(root,registry)) as queue:
        assert head(queue,url)==dict(before,state=expected)
        marker='bastillepost_native_parser_recheck_v2:'+registry.sources['bastillepost'].data_fingerprint
        receipt=json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',(marker,)).fetchone()[0])
        assert not receipt['accepted_as_bodies']
        assert not receipt['stored_heads_leases_refusals_or_unreviewed_paths_changed']
        assert len(receipt['rows'])==int(expected=='retry')
        if expected=='retry':
            after=failure(queue,url,state=state,error_code=state,attempts=before['attempts']+1)
            queue.reconcile_policy()
            assert head(queue,url)==after


def test_v2_changed_scope_does_not_get_generic_missing_body_retry(settings,tmp_path):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_reviewed_repairs import failure,head
    root=tmp_path/'store'
    with closing(NewsQueue(root,PolicyRegistry(settings,base=tmp_path))) as queue:
        before=failure(queue,URL,state='body_not_found',error_code='body_not_found')
    config=versioned(settings);config['providers'][0]['authorization']['scope']+=' New source scope.'
    with closing(NewsQueue(root,PolicyRegistry(config,base=tmp_path))) as queue:
        assert head(queue,URL)==before


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('marker', ['(ky)', '(sl)', '(sl.)', '(jl)', '(ms.)', '(jh)'])
def test_v3_complete_native_short_prose_keeps_observed_opaque_suffix(settings, engine, marker):
    config = versioned(settings, 3); config['providers'][0]['html_parser'] = engine
    text = '完整原生數值 -12.50、0%、一千股 '+marker
    html = page(body='<p class="font-size-scaler-control">'+text+'</p>')
    with pytest.raises(NewsError, match='body_not_found'):
        parse(versioned(settings, 2), html)
    result = parse(config, html)
    assert result['body'] == text
    assert result['attribution_status'] == 'source_declared_corporate_author_not_verified_person'
    assert json.loads(result['authors_json']) == ['巴士的報']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('ending', ['', '(more)', '(draft)', '(MS.)', '(ms)', '(sl..)', '(zh)', '…', '(ky)省略'])
def test_v3_unknown_annotation_is_not_a_short_body_completion_proxy(settings, ending):
    with pytest.raises(NewsError, match='body_not_found'):
        parse(versioned(settings, 3), page(body='<p class="font-size-scaler-control">短段原值 -12.50'+ending+'</p>'))


def test_v3_preserves_old_native_long_and_sentence_ending_outputs(settings):
    for html in (page(), page(body='<p class="font-size-scaler-control">已驗證原生短文 -12.50。</p>')):
        assert parse(versioned(settings, 3), html) == parse(versioned(settings, 2), html)


def test_v3_immediate_predecessor_and_http_authorization_identity(settings):
    first = PolicyRegistry(settings, base=Path.cwd()).sources['bastillepost']
    old = PolicyRegistry(versioned(settings, 2), base=Path.cwd()).sources['bastillepost']
    new = PolicyRegistry(versioned(settings, 3), base=Path.cwd()).sources['bastillepost']
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint == first.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint


@pytest.mark.parametrize('state', ['body_not_found', 'article_identity_not_verified',
    'article_rights_or_scope_excluded', 'invalid_bulk_publication_clock', 'unsupported_article_path', 'robots_disallow'])
def test_v3_never_automatically_rearms_unproved_failures_or_resets_attempts(settings, tmp_path, state):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_reviewed_repairs import failure, head
    root = tmp_path/'store'
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 2), base=tmp_path))) as queue:
        before = failure(queue, URL, state=state, error_code=state, attempts=19)
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 3), base=tmp_path))) as queue:
        assert head(queue, URL) == before
        queue.reconcile_policy()
        assert head(queue, URL) == before


def test_v3_exact_witness_cohort_uses_existing_reviewed_repair_mailbox(settings, tmp_path):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue
    from downloader.news_fulltext_repairs import consume_repairs, submit_repairs
    from test_news_reviewed_repairs import failure, head
    root = tmp_path/'store'
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 2), base=tmp_path))) as queue:
        before = failure(queue, URL, state='body_not_found', error_code='body_not_found', attempts=19)
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 3), base=tmp_path))) as queue:
        assert head(queue, URL) == before
        request = submit_repairs(root, queue.registry, [URL], reason='Independent exact native witness compared; only this missing head')
        assert request['submitted_targets'] == 1 and request['submitted']
        consume_repairs(queue, max_seconds=.1)
        assert head(queue, URL) == dict(before, state='retry')


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
def test_v4_two_complete_primary_paragraphs_preserve_all_raw_values(settings, engine):
    first, second = '甲'*45+'。', '乙'*46+'。'
    body = f'<p class="font-size-scaler-control">{first}</p><p class="font-size-scaler-control">{second}</p>'
    with pytest.raises(NewsError, match='body_not_found'):
        parse(versioned(settings, 3), page(body=body))
    config = versioned(settings, 4); config['providers'][0]['html_parser'] = engine
    result = parse(config, page(body=body))
    assert result['body'] == first+'\n'+second and len(result['body']) == 94
    numeric = parse(config, page(body='<p class="font-size-scaler-control">原值 -12.50、0%。</p>'
        '<p class="font-size-scaler-control">成交量1000股，不補零。</p>'))
    assert numeric['body'] == '原值 -12.50、0%。\n成交量1000股，不補零。'
    assert 'complete_short_native_primary_prose' in result['body_completeness']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('body', [
    '<p class="font-size-scaler-control">未完</p><p class="font-size-scaler-control">完整。</p>',
    '<p class="font-size-scaler-control">完整。</p><p class="font-size-scaler-control">未完</p>',
    '<p class="font-size-scaler-control">完整。</p><p class="font-size-scaler-control">不透明。(sl)</p>',
    '<p class="font-size-scaler-control">完整。</p><p class="font-size-scaler-control caption">圖說。</p>',
    '<p class="font-size-scaler-control">完整。</p><p>未驗證主段。</p>',
    '<p class="font-size-scaler-control">完整。</p><div><p class="font-size-scaler-control">非直接段落。</p></div>',
    '<p class="font-size-scaler-control">完整。</p><p class="font-size-scaler-control">完整。</p>外部字串',
    '<p class="font-size-scaler-control">完整。</p><p class="font-size-scaler-control">完整。</p><span>外部文字</span>',
    '<p class="font-size-scaler-control">甲。</p><p class="font-size-scaler-control">乙。</p><p class="font-size-scaler-control">丙。</p>',
    '<p class="font-size-scaler-control">甲。</p><p class="font-size-scaler-control">乙。</p><a class="image-lightbox"><img src="/a.jpg"></a>',
])
def test_v4_shortness_never_discards_other_native_content_or_accepts_incomplete_paragraphs(settings, body):
    with pytest.raises(NewsError, match='body_not_found'):
        parse(versioned(settings, 4), page(body=body))


@pytest.mark.parametrize('extra,code', [
    ('<script>unknown()</script>', 'article_identity_not_verified'),
    ('<iframe src="/unknown"></iframe>', 'article_identity_not_verified'),
    ('<p hidden>隱藏。</p>', 'article_identity_not_verified'),
    ('<p class="font-size-scaler-control">（星島日報報道）轉載。</p>', 'article_rights_or_scope_excluded'),
])
def test_v4_two_paragraph_repair_keeps_active_visibility_and_credit_guards(settings, extra, code):
    body = '<p class="font-size-scaler-control">甲。</p><p class="font-size-scaler-control">乙。</p>'+extra
    with pytest.raises(NewsError, match=code):
        parse(versioned(settings, 4), page(body=body))


def test_v4_immediate_predecessor_and_old_outputs_keep_source_auth_http_identity(settings):
    old = PolicyRegistry(versioned(settings, 3), base=Path.cwd()).sources['bastillepost']
    new = PolicyRegistry(versioned(settings, 4), base=Path.cwd()).sources['bastillepost']
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint != new.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    for html in (page(), page(body='<p class="font-size-scaler-control">完整。(sl)</p>')):
        assert parse(versioned(settings, 4), html) == parse(versioned(settings, 3), html)


@pytest.mark.parametrize('version', [1, 2, 3, 4])
@pytest.mark.parametrize('suffix', ['?unknown=1', '?current_cat=0', '?current_cat=-1',
    '?current_cat=1&current_cat=2', '?current_cat=%35', '?current_cat=1234567890123'])
def test_all_native_versions_reject_unreviewed_query_before_http(settings, version, suffix):
    requests = []
    http = SimpleNamespace(registry=PolicyRegistry(versioned(settings, version), base=Path.cwd()),
        get=lambda *a, **kw: requests.append(a))
    with pytest.raises(NewsError, match='unsupported_article_path'):
        fetch_article(http, {'url':ID_URL+suffix, 'id':bytes.fromhex('12'*16)})
    assert not requests


def test_v4_literal_category_request_keeps_query_and_requires_actual_same_id_chain(settings):
    request_url = ID_URL+'?current_cat=138496'
    assert bastillepost_article_id(request_url, requested=True, adapter_contract_version=3) is None
    response = Response(URL, 200, {'content-type':'text/html'}, page().encode(), (hop(request_url, URL),))
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return response
    http = SimpleNamespace(registry=PolicyRegistry(versioned(settings, 4), base=Path.cwd()), get=get)
    result = fetch_article(http, {'url':request_url, 'id':bytes.fromhex('12'*16)})
    assert calls == [request_url] and result['requested_url'] == request_url
    assert result['final_url'] == URL and 'literal_same_id_source_redirect' in result['url_resolution_basis']
    response = Response(URL, 200, {'content-type':'text/html'}, page().encode())
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(http, {'url':request_url, 'id':bytes.fromhex('12'*16)})


@pytest.mark.parametrize('state', ['body_not_found', 'article_identity_not_verified',
    'article_rights_or_scope_excluded', 'invalid_bulk_publication_clock', 'unsupported_article_path', 'robots_disallow'])
def test_v4_leaves_old_recognized_heads_and_nonroute_failures_untouched(settings, tmp_path, state):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_reviewed_repairs import failure, head
    root = tmp_path/'store'
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 3), base=tmp_path))) as queue:
        before = failure(queue, URL, state=state, error_code=state, attempts=19)
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 4), base=tmp_path))) as queue:
        assert head(queue, URL) == before
        queue.reconcile_policy()
        assert head(queue, URL) == before


def test_v4_only_new_due_literal_category_view_retries_once_without_reset(settings, tmp_path):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_reviewed_repairs import failure, head
    url = URL+'?current_cat=138496'
    root = tmp_path/'store'
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 3), base=tmp_path))) as queue:
        before = failure(queue, url, state='unsupported_article_path', error_code='unsupported_article_path', attempts=19)
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 4), base=tmp_path))) as queue:
        assert head(queue, url) == dict(before, state='retry')
        queue.reconcile_policy()
        assert head(queue, url) == dict(before, state='retry')
        with queue.db:
            queue.db.execute("UPDATE articles SET state='unsupported_article_path',attempts=20 WHERE url=?", (url,))
        retried = head(queue, url)
        queue.reconcile_policy()
        assert head(queue, url) == retried
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 4), base=tmp_path))) as queue:
        assert head(queue, url) == retried


@pytest.mark.parametrize('changes', [
    {'due':lambda:__import__('time').time()+86400}, {'lease_owner':'other'}, {'lease_until':1},
    {'body_bytes':1}, {'body_sha256':'a'*64}, {'parquet_path':'already.parquet'},
    {'captured_at_utc':'2026-01-01T00:00:00+00:00'}, {'last_checked_at_utc':None},
    {'last_checked_at_utc':'invalid'}, {'last_checked_at_utc':'2099-01-01T00:00:00+00:00'},
    {'error_code':'some_other_error'},
])
def test_v4_new_literal_route_never_erases_lease_partial_saved_future_or_different_failure(settings, tmp_path, changes):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_reviewed_repairs import failure, head
    changes = {k:v() if callable(v) else v for k,v in changes.items()}
    url, root = URL+'?current_cat=138496', tmp_path/'store'
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 3), base=tmp_path))) as queue:
        values = dict(state='unsupported_article_path', error_code='unsupported_article_path')
        values.update(changes)
        before = failure(queue, url, **values)
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 4), base=tmp_path))) as queue:
        assert head(queue, url) == before


@pytest.mark.parametrize('state', ['body_not_found', 'article_identity_not_verified',
    'article_rights_or_scope_excluded', 'http_429', 'http_404'])
def test_v4_literal_route_does_not_rearm_other_query_failures(settings, tmp_path, state):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_reviewed_repairs import failure, head
    url, root = URL+'?current_cat=138496', tmp_path/'store'
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 3), base=tmp_path))) as queue:
        before = failure(queue, url, state=state, error_code=state)
    with closing(NewsQueue(root, PolicyRegistry(versioned(settings, 4), base=tmp_path))) as queue:
        assert head(queue, url) == before


def test_native_version_registry_is_closed_and_shared_with_live_and_diagnostic_entrypoints(settings):
    from downloader.news_fulltext_policy import BASTILLE_LAYOUT_VERSIONS
    assert BASTILLE_LAYOUT_VERSIONS == {f'bastillepost_visible_article_v{n}':n for n in (1,2,3,4,5,6,7)}
    with pytest.raises(ValueError):
        PolicyRegistry(versioned(settings, 8), base=Path.cwd())
