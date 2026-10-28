"""GNN native identity, clocks, supplier provenance and canonical recovery."""
from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import time
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import load_config, run_cycle
from downloader.news_fulltext_gnn import GNN_ORIGIN, GNN_PATH, GNN_BODY, GNN_LAYOUT, GNN_CREDITED_LAYOUT, GNN_WRITTEN_LAYOUT, GNN_LEGACY_LAYOUT
from downloader.news_fulltext_policy import PolicyRegistry, NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import FakeResponse, http_fixture


URL=GNN_ORIGIN+'/detail.php?sn=237675'
TITLE='公司營收與原生表格'


@pytest.fixture
def recipe():
    config,_,_=load_config(Path('configs/news_fulltext.json'))
    entry={'id':'gnn','domains':['gnn.gamer.com.tw'],'enabled':True,
        'acquisition_mode':'continuous','private_known_url_scope':'personal_noncommercial_originals_v1',
        'interval_seconds':2,'parallelism':1,'html_parser':'lxml',
        'embedded_article_layout':GNN_LAYOUT,'article_path_pattern':GNN_PATH,
        'article_selectors':[GNN_BODY],'exclude_selectors':[],'title_selector':'h1',
        'require_configured_container':True,'require_author_attribution':True,
        'feeds':[],'sitemaps':[],'wordpress_apis':[],'fulltext_feeds':[],
        'authorization':{'kind':'personal_research','reviewed_on':'2026-10-25',
            'evidence_url':GNN_ORIGIN+'/','automated_access_review':'no_explicit_prohibition_identified',
            'scope':'Known public HTML only for requested private reading; no RSS, redistribution or training/export'}}
    return {**config,'providers':[entry]}


@pytest.fixture
def source(recipe):
    return PolicyRegistry(recipe,base=Path.cwd()).sources['gnn']


@pytest.fixture
def v2source(recipe):
    config=deepcopy(recipe)
    config['providers'][0]['embedded_article_layout']=GNN_CREDITED_LAYOUT
    return PolicyRegistry(config,base=Path.cwd()).sources['gnn']


@pytest.fixture
def v3source(recipe):
    config=deepcopy(recipe)
    config['providers'][0]['embedded_article_layout']=GNN_WRITTEN_LAYOUT
    return PolicyRegistry(config,base=Path.cwd()).sources['gnn']


@pytest.fixture
def v4source(recipe):
    config=deepcopy(recipe)
    config['providers'][0]['embedded_article_layout']=GNN_LEGACY_LAYOUT
    return PolicyRegistry(config,base=Path.cwd()).sources['gnn']


def document():
    data={'@type':'NewsArticle','mainEntityOfPage':{'@type':'WebPage','@id':URL},
        'headline':TITLE,'author':{'@type':'Person','name':'Jessica'},
        'publisher':{'@type':'Organization','name':'巴哈姆特電玩資訊站'},
        'datePublished':'2022-09-12T14:54:58+08:00',
        'dateModified':'2022-09-12T14:54:58+08:00',
        'articleBody':'DECLARED_SUMMARY_NOT_NATIVE_BODY'}
    return BeautifulSoup(f'''<html><head><title>{TITLE} - 巴哈姆特</title>
      <meta property="og:url" content="{URL}"><meta property="og:title" content="{TITLE}">
      <script type="application/ld+json">{json.dumps(data,ensure_ascii=False)}</script></head><body>
      <div class="BH-lbox GN-lbox3 gnn-detail-cont"><div><h1>{TITLE}</h1>
      <p class="GN-lbox3A"><span class="GN-lbox3C">（GNN 記者 Jessica 報導） 2022-09-12 14:54:58</span></p></div>
      <div class="GN-lbox3B"><div><p>{'原生營收數值與營運展望，保留供稿來源及原始數值。'*10}</p>
      <p class="caution">【以下內容為廠商提供資料原文】</p>
      <table><tr><th>本月營收</th><td>438,000</td></tr></table>
      <figure>MEDIA_CAPTION_IN_RAW_NOT_PROSE<img src="https://media.test/image.jpg"></figure></div></div>
      <div class="gnn-label">NEIGHBOR_TAGS_NOT_BODY</div></div>
      <footer>NEIGHBOR_COPYRIGHT_NOT_BODY</footer></body></html>''','lxml')


def extract(soup,source,*,url=URL):
    return extract_article(str(soup).encode(),source,page_url=url)


def test_native_body_not_metadata_neighbors_or_media(source):
    soup=document();before=str(soup)
    row=extract(soup,source)
    assert '438,000' in row['body'] and '原生營收' in row['body']
    assert '廠商提供資料原文' in row['body']
    assert not any(x in row['body'] for x in ('NEIGHBOR','MEDIA_CAPTION','DECLARED_SUMMARY'))
    assert row['body_sha256']==hashlib.sha256(row['body'].encode()).hexdigest()
    assert row['extraction_method']=='css:'+GNN_BODY
    assert row['published_at_raw']=='2022-09-12T14:54:58+08:00'
    assert row['modified_at_raw']==row['published_at_raw']
    assert json.loads(row['authors_json'])==['Jessica'] and row['historical_point_in_time'] is False
    detail=json.loads(row['author_attribution_detail_json'])
    assert detail['role']=='publisher_declared_GNN_reporter'
    assert not detail['all_body_written_or_owned_by_reporter_verified']
    notices=json.loads(row['verified_article_notices_json'])
    assert notices['supplier_content_declared'] and not notices['rss_used'] and not notices['reuse_rights_granted']
    assert str(soup)==before


def test_native_editorial_without_supplier_notice_keeps_reporter_role(source):
    soup=document();soup.select_one('.caution').decompose()
    row=extract(soup,source)
    assert not json.loads(row['verified_article_notices_json'])['supplier_content_declared']


def test_v2_supplier_credit_is_not_promoted_to_reporter_or_person_identity(source,v2source):
    soup=document()
    soup.select_one('.GN-lbox3C').string='（廠商 範例科技 提供） 2022-09-12 14:54:58'
    script=soup.select_one('script[type="application/ld+json"]')
    data=json.loads(script.string);data['author']['name']='廠商-範例科技';script.string=json.dumps(data)
    with pytest.raises(NewsError,match='article_attribution_not_verified'):extract(soup,source)
    row=extract(soup,v2source)
    detail=json.loads(row['author_attribution_detail_json'])
    assert detail['role']=='publisher_declared_supplier_credit' and detail['native_name']=='範例科技'
    assert json.loads(row['authors_json'])==['範例科技']
    assert not detail['person_identity_independently_verified']
    assert not detail['publisher_declared_Person_type_independently_verified']
    assert not detail['all_body_written_or_owned_by_reporter_verified']
    soup.select_one('.caution').decompose()
    assert json.loads(extract(soup,v2source)['verified_article_notices_json'])['supplier_content_declared']
    data['author']['name']='另一家';script.string=json.dumps(data)
    with pytest.raises(NewsError,match='article_attribution_not_verified'):extract(soup,v2source)


def test_v2_native_legacy_credit_container_and_movie_suffix_preserve_v1(source,v2source):
    soup=document();soup.select_one('.GN-lbox3C').attrs.pop('class')
    soup.title.string=TITLE+'《すずめの戸締まり》 - 巴哈姆特'
    with pytest.raises(NewsError):extract(soup,source)
    row=extract(soup,v2source)
    assert row['title']==TITLE and json.loads(row['authors_json'])==['Jessica']
    assert json.loads(row['verified_article_notices_json'])['html_title']==soup.title.string
    soup.select_one('.GN-lbox3A').append(' OTHER_CREDIT')
    with pytest.raises(NewsError):extract(soup,v2source)


def test_v2_credit_fallback_cannot_take_a_hidden_descendant(v2source):
    soup=document();credit=soup.select_one('.GN-lbox3C');credit.attrs.pop('class')
    credit['hidden']=''
    with pytest.raises(NewsError,match='article_identity_not_verified'):extract(soup,v2source)


@pytest.mark.parametrize('kind',['video','empty_template'])
def test_v2_empty_media_is_not_executed_and_native_written_body_is_unchanged(source,v2source,kind):
    soup=document();expected=extract(soup,source)['body']
    if kind=='video':
        node=soup.new_tag('iframe',src='https://www.youtube.com/embed/nhgCSHMLO2c?wmode=transparent')
    else:
        node=soup.new_tag('script',type='text/template');node.string='<div><img src="https://media.test/x"></div>'
    soup.select_one('.GN-lbox3B').append(node)
    before=str(soup)
    with pytest.raises(NewsError):extract(soup,source)
    row=extract(soup,v2source)
    assert row['body']==expected and str(soup)==before
    assert len(json.loads(row['verified_article_notices_json'])['unfetched_or_verified_text_empty_embeds'])==1


@pytest.mark.parametrize('kind',['template_prose','template_table','template_script','external_template',
    'large_template','unknown_video','video_text','video_null_src','active_script','unknown_element'])
def test_v2_unknown_or_written_embedded_content_is_never_silently_dropped(v2source,kind):
    soup=document()
    if kind.startswith('template') or kind in {'external_template','large_template'}:
        node=soup.new_tag('script',type='text/template')
        node.string={'template_prose':'<p>ACTUAL_ARTICLE_PROSE</p>',
            'template_table':'<table><td>1234</td></table>',
            'template_script':'<script>EXECUTION_NOT_ALLOWED</script>',
            'large_template':'<img>'*6600}.get(kind,'')
        if kind=='external_template':node['src']='https://other.test/not-fetched'
    elif kind.startswith('video') or kind=='unknown_video':
        node=soup.new_tag('iframe',src='https://www.youtube.com/embed/nhgCSHMLO2c')
        if kind=='unknown_video':node['src']='https://unknown.test/embedded'
        elif kind=='video_null_src':node['src']=None
        else:node.string='FALLBACK_ARTICLE_PROSE'
    else:
        node=soup.new_tag('script' if kind=='active_script' else 'unknown-prose')
        node.string='ARTICLE_TEXT_OR_CODE'
    soup.select_one('.GN-lbox3B').append(node)
    with pytest.raises(NewsError,match='body_not_found'):extract(soup,v2source)


def test_v2_capture_abi_preserves_exact_v1_source_auth_capacity_and_diagnostic_key(source,v2source):
    assert v2source.data_fingerprint!=source.data_fingerprint
    assert v2source.authorization_fingerprint==source.authorization_fingerprint
    assert v2source.http_capacity_source_fingerprint==(source.http_capacity_source_fingerprint or source.data_fingerprint)
    assert v2source.parser_repair_prior_capture_fingerprint==source.data_fingerprint
    from downloader.news_fulltext_candidate_diagnostics import _key
    target={'provider':'gnn','id':'1'*64}
    assert _key(target,source.data_fingerprint,evidence_contract_version=48)==_key(target,v2source.data_fingerprint,evidence_contract_version=48)


def test_resident_diagnostic_keeps_public_credit_and_structure_but_no_prose(recipe, tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.news_fulltext_http import Response
    settings = {**deepcopy(recipe), 'root': str(tmp_path)}
    registry = PolicyRegistry(settings, base=Path.cwd())
    soup = document()
    iframe = soup.new_tag('iframe', src='https://video.test/not-fetched')
    soup.select_one('.GN-lbox3B').append(iframe)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                ('body_not_found', 'body_not_found', time.time()-1, '2026-01-01T00:00:00Z', URL))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        budget = list(queue.db.execute('SELECT * FROM queue_settings'))
        first = diagnostic.submit_candidate(tmp_path, settings, registry, URL, GNN_LAYOUT,
            reason='Unresolved native GNN declaration', evidence_contract_version=48)
        diagnostic.pump_candidates(queue)
        calls = []
        def get(url):
            calls.append(url)
            return Response(url, 200, {'content-type': 'text/html'}, str(soup).encode())
        http = SimpleNamespace(registry=registry, get=get, provider_not_before={},
            blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())
        http.provider_not_before['gnn'] = time.time()+3600
        assert diagnostic.claim_candidate(queue, http, settings, 'gnn') is None
        http.provider_not_before.clear()
        job = diagnostic.claim_candidate(queue, http, settings, 'gnn')
        assert job is not None
        result = diagnostic.fetch_candidate(http, job)
        encoded = json.dumps(result, ensure_ascii=False)
        assert '原生營收數值' not in encoded and '438,000' not in encoded
        assert 'DECLARED_SUMMARY' not in encoded and 'MEDIA_CAPTION' not in encoded
        assert result['requested_url'] == URL and result['raw_or_body_retained'] is False
        assert result['raw_or_names_or_body_retained'] is True
        assert result['public_header_declarations_retained'] is True
        assert result['public_document_identity']['declared_articles'][0]['author']['name'] == 'Jessica'
        native = result['gnn_native_header']
        assert native['headers'][0]['declarations'][0]['credits'] == ['（GNN 記者 Jessica 報導） 2022-09-12 14:54:58']
        assert native['body_structure_without_prose'][0]['unknown_element_counts'] == {'iframe': 1}
        diagnostic.finish_candidate(queue, job, result=result)
        diagnostic.pump_candidates(queue)
        assert diagnostic.candidate_status(tmp_path, first['request_id'])['state'] == 'observed'
        again = diagnostic.submit_candidate(tmp_path, settings, registry, URL, GNN_LAYOUT,
            reason='Another reason cannot renew a closed question', evidence_contract_version=48)
        assert not again['submitted'] and calls == [URL]
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
        assert list(queue.db.execute('SELECT * FROM queue_settings')) == budget
        assert not (tmp_path/'bodies').exists()


@pytest.mark.parametrize('evidence',[48,49])
@pytest.mark.parametrize('changes', [
    {'state': 'http_403', 'error_code': 'http_403'},
    {'state': 'http_429', 'error_code': 'http_429'},
    {'state': 'robots_denied', 'error_code': 'robots_denied'},
    {'state': 'awaiting_authorization', 'error_code': 'awaiting_authorization'},
    {'due': 4102444800}, {'lease_owner': 'other'}, {'body_bytes': 1},
])
def test_diagnostic_never_rearms_refusal_future_lease_or_body(recipe, tmp_path, changes, evidence):
    from downloader.news_fulltext_candidate_diagnostics import submit_candidate
    settings = {**deepcopy(recipe), 'root': str(tmp_path)}
    registry = PolicyRegistry(settings, base=Path.cwd())
    with closing(NewsQueue(tmp_path, registry)) as queue:
        values = dict(state='article_attribution_not_verified', error_code='article_attribution_not_verified',
            due=time.time()-1, last_checked_at_utc='2026-01-01T00:00:00Z')
        values.update(changes)
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in values)+' WHERE url=?',
                (*values.values(), URL))
        with pytest.raises((ValueError, NewsError)):
            submit_candidate(tmp_path, settings, registry, URL, GNN_LAYOUT,
                reason='No bypass of source refusal', evidence_contract_version=evidence)


def test_diagnostic_key_does_not_renew_on_parser_change_and_scope_cannot_expand(source):
    from downloader.news_fulltext_candidate_diagnostics import _key
    from downloader.news_fulltext_candidates import candidate_evidence_allowed
    target = {'provider': 'gnn', 'id': '1'*64}
    assert _key(target, 'a'*64, evidence_contract_version=48) == _key(target, 'b'*64, evidence_contract_version=48)
    with pytest.raises(ValueError):
        _key({**target, 'provider': 'managertoday'}, 'a'*64, evidence_contract_version=48)
    assert candidate_evidence_allowed(source, source, 48)
    assert not candidate_evidence_allowed(source, source, 47)


def test_private_boundary_question_retains_one_original_and_never_accepts_article(recipe,tmp_path):
    import pyarrow.parquet as pq
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.news_fulltext_candidate_originals import PendingOriginal
    from downloader.news_fulltext_http import Response
    from downloader.artifact_io import sha256_file
    config={**deepcopy(recipe),'root':str(tmp_path)}
    registry=PolicyRegistry(config,base=Path.cwd())
    soup=document();node=soup.new_tag('script',type='text/template')
    node.string='<p>WRITTEN_EMBEDDED_CONTENT_MUST_BE_REVIEWED</p>'
    soup.select_one('.GN-lbox3B').append(node);raw=str(soup).encode()
    with closing(NewsQueue(tmp_path,registry)) as queue:
        with queue.db:
            queue.add_articles([{'url':URL}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                ('body_not_found','body_not_found',time.time()-1,'2026-01-01T00:00:00Z',URL))
        before=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
        submitted=diagnostic.submit_candidate(tmp_path,config,registry,URL,GNN_LAYOUT,
            reason='One current native document for offline body-boundary repair',evidence_contract_version=49)
        diagnostic.pump_candidates(queue);calls=[]
        def get(url):
            calls.append(url);return Response(url,200,{'content-type':'text/html'},raw)
        http=SimpleNamespace(registry=registry,get=get,provider_not_before={},
            blocked_gateway_hosts=lambda p:(),blocked_robots_origins=lambda p:())
        job=diagnostic.claim_candidate(queue,http,config,'gnn');observation=diagnostic.fetch_candidate(http,job)
        assert isinstance(observation,PendingOriginal)
        assert observation.permit['native_permit']['byline_verified'] is False
        assert observation.permit['body_completeness_verified'] is False
        diagnostic.finish_candidate(queue,job,result=observation);diagnostic.pump_candidates(queue)
        result=diagnostic.candidate_status(tmp_path,submitted['request_id'])
        assert result['state']=='observed'
        record=result['result_json']
        assert record['accepted_as_article'] is False and record['raw_or_names_or_body_retained'] is True
        original=record['private_original'];path=tmp_path/original['path']
        assert path.stat().st_mode&0o777==0o600 and sha256_file(path)==original['sha256']
        rows=pq.ParquetFile(path).read().to_pylist()
        assert len(rows)==1 and rows[0]['raw_response_body']==raw
        assert rows[0]['diagnostic_only'] and not any(rows[0][k] for k in ('accepted_as_article','publish','training_injection_enabled','remote_training_export','historical_point_in_time'))
        receipt=json.loads((tmp_path/original['receipt_path']).read_bytes())
        assert receipt['question']=='gnn_private_current_document_boundary_original_v1'
        assert receipt['original']==original and receipt['permit']==observation.permit
        again=diagnostic.submit_candidate(tmp_path,config,registry,URL,GNN_LAYOUT,
            reason='No renewed question',evidence_contract_version=49)
        assert not again['submitted'] and calls==[URL]
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==before
        assert not (tmp_path/'bodies').exists() and not (tmp_path/'receipts').exists()
    with closing(NewsQueue(tmp_path,registry)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==before
        assert diagnostic.candidate_status(tmp_path,submitted['request_id'])['state']=='observed'


@pytest.mark.parametrize('mutation',['byline_missing','byline_conflict'])
def test_boundary_original_retention_does_not_assert_unknown_credit(source,mutation):
    from downloader.news_fulltext_gnn import private_gnn_boundary_original_permit
    from downloader.news_fulltext_http import Response
    soup=document()
    if mutation=='byline_missing':soup.select_one('.GN-lbox3A').decompose()
    else:soup.select_one('.GN-lbox3C').string='Unknown disputed native credit'
    permit=private_gnn_boundary_original_permit(Response(URL,200,{'content-type':'text/html'},str(soup).encode()),source,URL)
    assert not permit['native_permit']['byline_verified'] and not permit['body_completeness_verified']


@pytest.mark.parametrize('mutation',['status','mime','url','og','ld_url','publisher','paywall','free_false','hidden','nul'])
def test_boundary_original_cannot_expand_access_or_document_scope(source,mutation):
    from downloader.news_fulltext_gnn import private_gnn_boundary_original_permit
    from downloader.news_fulltext_http import Response
    soup=document();status=200;url=URL;mime='text/html'
    if mutation=='status':status=403
    elif mutation=='mime':mime='application/json'
    elif mutation=='url':url=URL+'1'
    elif mutation=='og':soup.select_one('meta[property="og:url"]')['content']=URL+'1'
    elif mutation in {'ld_url','publisher','free_false'}:
        node=soup.select_one('script[type="application/ld+json"]');data=json.loads(node.string)
        if mutation=='ld_url':data['mainEntityOfPage']['@id']=URL+'1'
        elif mutation=='publisher':data['publisher']['name']='Unreviewed publisher'
        else:data['isAccessibleForFree']=False
        node.string=json.dumps(data)
    elif mutation=='paywall':soup.body.append(soup.new_tag('form',action='/login'))
    elif mutation=='hidden':soup.select_one('.GN-lbox3B')['hidden']=''
    raw=str(soup).encode()
    if mutation=='nul':raw+=b'\0'
    with pytest.raises(NewsError):private_gnn_boundary_original_permit(Response(url,status,{'content-type':mime},raw),source,URL)


def test_private_boundary_question_key_cannot_be_renewed_by_parser_version():
    from downloader.news_fulltext_candidate_diagnostics import _key
    target={'provider':'gnn','id':'1'*64}
    assert _key(target,'a'*64,evidence_contract_version=49)==_key(target,'b'*64,evidence_contract_version=49)
    assert _key(target,'a'*64,evidence_contract_version=49)!=_key(target,'a'*64,evidence_contract_version=48)
    with pytest.raises(ValueError):_key({**target,'provider':'cnyes'},'a'*64,evidence_contract_version=49)


def test_private_original_written_before_crash_stays_uncertain_without_duplicate_get(recipe,tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.news_fulltext_candidate_originals import retain_original
    from downloader.news_fulltext_http import Response
    config={**deepcopy(recipe),'root':str(tmp_path)};registry=PolicyRegistry(config,base=Path.cwd())
    with closing(NewsQueue(tmp_path,registry)) as queue:
        with queue.db:
            queue.add_articles([{'url':URL}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                ('body_not_found','body_not_found',time.time()-1,'2026-01-01T00:00:00Z',URL))
        first=diagnostic.submit_candidate(tmp_path,config,registry,URL,GNN_LAYOUT,
            reason='Retained original survives a completion-journal crash',evidence_contract_version=49)
        diagnostic.pump_candidates(queue);calls=[]
        def get(url):
            calls.append(url);return Response(url,200,{'content-type':'text/html'},str(document()).encode())
        http=SimpleNamespace(registry=registry,get=get,provider_not_before={},
            blocked_gateway_hosts=lambda p:(),blocked_robots_origins=lambda p:())
        job=diagnostic.claim_candidate(queue,http,config,'gnn')
        observation=diagnostic.fetch_candidate(http,job)
        result=retain_original(queue,job,observation)
        original=tmp_path/result['private_original']['path'];saved=original.read_bytes()
        # The durable raw witness exists; simulate loss before finish_candidate.
    with closing(NewsQueue(tmp_path,registry)) as queue:
        diagnostic.pump_candidates(queue)
        status=diagnostic.candidate_status(tmp_path,first['request_id'])
        assert status['state']=='uncertain_outcome'
        again=diagnostic.submit_candidate(tmp_path,config,registry,URL,GNN_LAYOUT,
            reason='A changed reason cannot retry an uncertain original',evidence_contract_version=49)
        assert not again['submitted'] and diagnostic.claim_candidate(queue,http,config,'gnn') is None
        assert calls==[URL] and original.read_bytes()==saved
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?',(URL,)).fetchone()[0] is None


@pytest.fixture
def route_recipe(recipe):
    config = deepcopy(recipe)
    config['providers'][0]['embedded_article_layout'] = GNN_LEGACY_LAYOUT
    return config


@pytest.mark.parametrize('url', [URL, URL.replace('https:', 'http:')])
@pytest.mark.parametrize('state', ['awaiting_authorization', 'body_not_found'])
def test_first_hop_diagnostic_uses_resident_http_and_preserves_head(route_recipe, tmp_path, url, state):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    config = {**deepcopy(route_recipe), 'root': str(tmp_path)}
    origin = url.split('/detail.php')[0]
    secret = 'AUTH_QUERY_VALUE_MUST_NOT_BE_RETAINED'
    body = b'ORIGINAL_RESPONSE_PROSE_MUST_NOT_BE_RETAINED'
    http, session = http_fixture(config, tmp_path, {
        origin+'/robots.txt': FakeResponse(body=b'User-agent: *\nAllow: /'),
        url: FakeResponse(body=body, status=302, headers={'location': 'https://unregistered.test/login?token='+secret})})
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                (state, state, time.time()-1, '2026-01-01T00:00:00Z', url))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        settings = list(queue.db.execute('SELECT * FROM queue_settings'))
        first = diagnostic.submit_candidate(tmp_path, config, http.registry, url, GNN_LEGACY_LAYOUT,
            reason='Inspect only the original authorized first hop', evidence_contract_version=50)
        diagnostic.pump_candidates(queue)
        http.provider_not_before['gnn'] = time.time()+3600
        assert diagnostic.claim_candidate(queue, http, config, 'gnn') is None
        http.provider_not_before.clear()
        job = diagnostic.claim_candidate(queue, http, config, 'gnn')
        assert job is not None
        facts = diagnostic.fetch_candidate(http, job)
        encoded = json.dumps(facts)
        assert secret not in encoded and body.decode() not in encoded
        assert facts['status'] == 302 and facts['target_origin_path'] == 'https://unregistered.test/login'
        assert facts['target_query_keys'] == ['token']
        assert facts['response_sha256'] == hashlib.sha256(body).hexdigest()
        assert facts['requested_url'] == url and facts['redirect_count'] == 0
        assert all(facts[key] is False for key in ('destination_requested', 'accepted_as_article',
            'body_completeness_verified', 'identity_or_alias_accepted', 'raw_or_names_or_body_retained'))
        diagnostic.finish_candidate(queue, job, result=facts)
        diagnostic.pump_candidates(queue)
        assert diagnostic.candidate_status(tmp_path, first['request_id'])['state'] == 'observed'
        again = diagnostic.submit_candidate(tmp_path, config, http.registry, url, GNN_LEGACY_LAYOUT,
            reason='A different reason cannot repeat this route observation', evidence_contract_version=50)
        assert not again['submitted']
        assert [call[0] for call in session.calls] == [origin+'/robots.txt', url]
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before
        assert list(queue.db.execute('SELECT * FROM queue_settings')) == settings


@pytest.mark.parametrize('changes', [
    {'state': 'http_403', 'error_code': 'http_403'},
    {'state': 'http_429', 'error_code': 'http_429'},
    {'state': 'robots_disallowed', 'error_code': 'robots_disallowed'},
    {'due': 4102444800}, {'lease_owner': 'other'}, {'lease_until': 4102444800},
    {'body_bytes': 1}, {'captured_at_utc': '2026-01-01T00:00:00Z'},
    {'body_sha256': 'a'*64}, {'parquet_path': 'retained.parquet'},
    {'last_checked_at_utc': '2099-01-01T00:00:00Z'}, {'last_checked_at_utc': None},
])
def test_first_hop_diagnostic_cannot_rearm_refusals_or_protected_heads(route_recipe, tmp_path, changes):
    from downloader.news_fulltext_candidate_diagnostics import submit_candidate
    config = {**deepcopy(route_recipe), 'root': str(tmp_path)}
    registry = PolicyRegistry(config, base=Path.cwd())
    with closing(NewsQueue(tmp_path, registry)) as queue:
        values = dict(state='awaiting_authorization', error_code='awaiting_authorization',
            due=time.time()-1, last_checked_at_utc='2026-01-01T00:00:00Z')
        values.update(changes)
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET '+','.join(key+'=?' for key in values)+' WHERE url=?',
                (*values.values(), URL))
        with pytest.raises((ValueError, NewsError)):
            submit_candidate(tmp_path, config, registry, URL, GNN_LEGACY_LAYOUT,
                reason='No authorization, source refusal or deadline bypass', evidence_contract_version=50)


@pytest.mark.parametrize('location', [
    'javascript:alert(1)', 'https://user:password@other.test/a', '/a\nLocation: bad',
    'https://other.test:8443/a', 'https://other.test/'+('a'*513),
    '/?'+('&'.join('key'+str(i)+'=VALUE' for i in range(17))),
])
def test_first_hop_untrusted_location_has_only_hash(v4source, location):
    from downloader.news_fulltext_gnn import gnn_route_metadata
    from downloader.news_fulltext_http import Response
    facts = gnn_route_metadata(Response(URL, 302, {'location': location}, b''), v4source, URL)
    assert facts['location_valid_http_route'] is False
    assert 'target_origin_path' not in facts and 'literal_location' not in facts
    assert facts['location_sha256'] == hashlib.sha256(location.encode()).hexdigest()


def test_first_hop_literal_GNN_location_is_observed_without_alias_or_target_fetch(v4source):
    from downloader.news_fulltext_gnn import gnn_route_metadata
    from downloader.news_fulltext_http import Response
    url = URL.replace('https:', 'http:')
    facts = gnn_route_metadata(Response(url, 301, {'location': URL}, b''), v4source, url)
    assert facts['literal_gnn_article_target'] == facts['literal_location'] == URL
    assert facts['identity_or_alias_accepted'] is False and facts['destination_requested'] is False
    success = gnn_route_metadata(Response(url, 200, {'location': URL}, b'no redirect'), v4source, url)
    assert 'literal_location' not in success and 'target_origin_path' not in success


@pytest.mark.parametrize('code', [403, 429])
def test_first_hop_refusal_keeps_canonical_HTTP_backoff_and_closed_question(route_recipe, tmp_path, code):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    config = {**deepcopy(route_recipe), 'root': str(tmp_path)}
    http, session = http_fixture(config, tmp_path, {
        GNN_ORIGIN+'/robots.txt': FakeResponse(body=b'User-agent: *\nAllow: /'),
        URL: FakeResponse(body=b'refused', status=code, headers={'retry-after': '600'})})
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                ('body_not_found', 'body_not_found', time.time()-1, '2026-01-01T00:00:00Z', URL))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        first = diagnostic.submit_candidate(tmp_path, config, http.registry, URL, GNN_LEGACY_LAYOUT,
            reason='A refusal cannot trigger another GET', evidence_contract_version=50)
        diagnostic.pump_candidates(queue)
        job = diagnostic.claim_candidate(queue, http, config, 'gnn')
        with pytest.raises(NewsError, match='source_access_denied' if code == 403 else 'http_429') as failure:
            diagnostic.fetch_candidate(http, job)
        diagnostic.finish_candidate(queue, job, error=failure.value)
        assert diagnostic.candidate_status(tmp_path, first['request_id'])['state'] == 'observed_error'
        again = diagnostic.submit_candidate(tmp_path, config, http.registry, URL, GNN_LEGACY_LAYOUT,
            reason='No retry even after refusal', evidence_contract_version=50)
        assert not again['submitted'] and http.provider_not_before['gnn'] > time.time()
        assert [call[0] for call in session.calls] == [GNN_ORIGIN+'/robots.txt', URL]
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


def test_first_hop_robots_denial_cannot_request_the_article(route_recipe, tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    config = {**deepcopy(route_recipe), 'root': str(tmp_path)}
    http, session = http_fixture(config, tmp_path, {
        GNN_ORIGIN+'/robots.txt': FakeResponse(body=b'User-agent: *\nDisallow: /')})
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                ('body_not_found', 'body_not_found', time.time()-1, '2026-01-01T00:00:00Z', URL))
        diagnostic.submit_candidate(tmp_path, config, http.registry, URL, GNN_LEGACY_LAYOUT,
            reason='Robots policy still precedes the exact literal article', evidence_contract_version=50)
        diagnostic.pump_candidates(queue)
        job = diagnostic.claim_candidate(queue, http, config, 'gnn')
        with pytest.raises(NewsError, match='robots_disallowed') as failure:
            diagnostic.fetch_candidate(http, job)
        diagnostic.finish_candidate(queue, job, error=failure.value)
        assert [call[0] for call in session.calls] == [GNN_ORIGIN+'/robots.txt']


@pytest.mark.parametrize('change', ['disabled', 'expired_authorization'])
def test_first_hop_cannot_override_current_source_permission(route_recipe, tmp_path, change):
    from downloader.news_fulltext_candidate_diagnostics import submit_candidate
    config = {**deepcopy(route_recipe), 'root': str(tmp_path)}
    registry = PolicyRegistry(config, base=Path.cwd())
    with closing(NewsQueue(tmp_path, registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                ('awaiting_authorization', 'awaiting_authorization', time.time()-1, '2026-01-01T00:00:00Z', URL))
        if change == 'disabled':
            config['providers'][0]['enabled'] = False
        else:
            config['providers'][0]['authorization']['valid_until'] = '2020-01-01T00:00:00Z'
        denied = PolicyRegistry(config, base=Path.cwd())
        with pytest.raises((ValueError, NewsError)):
            submit_candidate(tmp_path, config, denied, URL, GNN_LEGACY_LAYOUT,
                reason='Old failure is not authority to change current source access', evidence_contract_version=50)


def test_first_hop_question_stays_uncertain_after_reserved_worker_loss(route_recipe, tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.news_fulltext_http import Response
    config = {**deepcopy(route_recipe), 'root': str(tmp_path)}
    registry = PolicyRegistry(config, base=Path.cwd())
    calls = []
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response(url, 302, {'location': 'https://other.test/'}, b'')
    http = SimpleNamespace(registry=registry, get=get, provider_not_before={},
        blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())
    with closing(NewsQueue(tmp_path, registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                ('body_not_found', 'body_not_found', time.time()-1, '2026-01-01T00:00:00Z', URL))
        first = diagnostic.submit_candidate(tmp_path, config, registry, URL, GNN_LEGACY_LAYOUT,
            reason='Reserve durably before first-hop observation', evidence_contract_version=50)
        diagnostic.pump_candidates(queue)
        job = diagnostic.claim_candidate(queue, http, config, 'gnn')
        diagnostic.fetch_candidate(http, job)  # Simulate loss before completion.
    with closing(NewsQueue(tmp_path, registry)) as queue:
        diagnostic.pump_candidates(queue)
        assert diagnostic.candidate_status(tmp_path, first['request_id'])['state'] == 'uncertain_outcome'
        again = diagnostic.submit_candidate(tmp_path, config, registry, URL, GNN_LEGACY_LAYOUT,
            reason='A crash does not authorize a duplicate GET', evidence_contract_version=50)
        assert not again['submitted'] and diagnostic.claim_candidate(queue, http, config, 'gnn') is None
        assert calls == [(URL, {'follow_redirects': False})]


def test_first_hop_question_is_current_source_only_and_cannot_renew(v4source, source):
    from downloader.news_fulltext_candidate_diagnostics import _key, _failure_states
    from downloader.news_fulltext_candidates import candidate_evidence_allowed
    target = {'provider': 'gnn', 'id': 'a'*64}
    assert candidate_evidence_allowed(v4source, v4source, 50)
    assert not candidate_evidence_allowed(source, source, 50)
    assert not candidate_evidence_allowed(v4source, source, 50)
    assert _failure_states(GNN_LAYOUT, 50) == set()
    assert _key(target, 'a'*64, evidence_contract_version=50) == _key(target, 'b'*64, evidence_contract_version=50)
    assert _key(target, 'a'*64, evidence_contract_version=50) != _key(target, 'a'*64, evidence_contract_version=49)
    with pytest.raises(ValueError):
        _key({**target, 'provider': 'managertoday'}, 'a'*64, evidence_contract_version=50)


@pytest.mark.parametrize('url',[URL+'#body',URL+'&utm_source=other',URL+'&sn=237675',
    URL.replace('sn=237675','sn=0237675'),URL.replace('https:','http:'),
    URL.replace('gnn.gamer.com.tw','www.gamer.com.tw'),URL.replace('detail.php','rss.php'),
    URL.replace('gnn.gamer.com.tw','gnn.gamer.com.tw.attacker.test')])
def test_literal_native_query_identity_is_not_guessed_or_stripped(source,url):
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract(document(),source,url=url)


@pytest.mark.parametrize('field,value,error',[
    ('author',{'@type':'Person','name':'Another'},'article_attribution_not_verified'),
    ('author',{'@type':'Organization','name':'Jessica'},'article_attribution_not_verified'),
    ('headline','Another','article_identity_not_verified'),
    ('mainEntityOfPage',{'@type':'WebPage','@id':URL+'1'},'article_identity_not_verified'),
    ('publisher',{'@type':'Organization','name':'Another'},'article_identity_not_verified'),
    ('datePublished','2022-09-12T14:55:58+08:00','article_identity_not_verified'),
    ('dateModified','2022-13-12T14:54:58+08:00','article_identity_not_verified'),
    ('datePublished','2022-09-12T14:54:58','article_identity_not_verified'),
    ('isAccessibleForFree',False,'paywall_or_login')])
def test_native_declarations_must_match_observed_credit_identity_clock(source,field,value,error):
    soup=document();node=soup.select_one('script');data=json.loads(node.string)
    data[field]=value;node.string=json.dumps(data)
    with pytest.raises(NewsError,match=error):extract(soup,source)


@pytest.mark.parametrize('change',['duplicate_body','duplicate_header','duplicate_root','duplicate_ld',
    'missing_credit','other_credit','hidden_credit','hidden_body','hidden_prose',
    'iframe','script_in_body','unknown_element','title_mismatch','og_mismatch','canonical_conflict',
    'paywall_form','nul'])
def test_ambiguous_hidden_embedded_or_refused_content_never_becomes_partial_success(source,change):
    soup=document();body=soup.select_one('.GN-lbox3B')
    if change=='duplicate_body':body.parent.append(deepcopy(body))
    elif change=='duplicate_header':soup.select_one('.gnn-detail-cont').append(deepcopy(soup.h1.parent))
    elif change=='duplicate_root':soup.body.append(deepcopy(body.parent))
    elif change=='duplicate_ld':soup.head.append(deepcopy(soup.select_one('script')))
    elif change=='missing_credit':soup.select_one('.GN-lbox3A').decompose()
    elif change=='other_credit':soup.select_one('.GN-lbox3C').string='（廠商提供） 2022-09-12 14:54:58'
    elif change=='hidden_credit':soup.select_one('.GN-lbox3A')['aria-hidden']='true'
    elif change=='hidden_body':body['hidden']=''
    elif change=='hidden_prose':body.select_one('p')['style']='display: none'
    elif change in {'iframe','script_in_body','unknown_element'}:
        body.append(soup.new_tag({'iframe':'iframe','script_in_body':'script','unknown_element':'custom-prose'}[change]))
    elif change=='title_mismatch':soup.title.string='Another - 巴哈姆特'
    elif change=='og_mismatch':soup.select_one('meta[property="og:url"]')['content']=URL+'1'
    elif change=='canonical_conflict':soup.head.append(soup.new_tag('link',rel='canonical',href=URL+'1'))
    elif change=='paywall_form':soup.body.append(soup.new_tag('form',action='/login'))
    else:
        with pytest.raises(NewsError):extract_article(str(soup).replace('Jessica','Jess\0ica').encode(),source,page_url=URL)
        return
    with pytest.raises(NewsError):extract(soup,source)


@pytest.mark.parametrize('field,value',[('id','other'),('domains',['gamer.com.tw']),
    ('article_path_pattern','.*'),('article_selectors',['body']),('title_selector','title'),
    ('require_author_attribution',False),('require_configured_container',False),
    ('feeds',[GNN_ORIGIN+'/rss.xml']),('private_known_url_scope',''),('html_parser','html.parser')])
def test_native_recipe_cannot_expand_domains_scope_rss_or_body_fallback(recipe,field,value):
    config=deepcopy(recipe);config['providers'][0][field]=value
    with pytest.raises(ValueError):PolicyRegistry(config,base=Path.cwd())


def test_canonical_unknown_source_activation_and_durable_recovery(recipe,tmp_path):
    empty={**recipe,'providers':[]}
    with closing(NewsQueue(tmp_path,PolicyRegistry(empty,base=Path.cwd()))) as queue:
        with queue.db:
            queue.add_articles([{'url':URL}])
        original=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
        assert original['state']=='awaiting_authorization' and original['attempts']==0
    registry=PolicyRegistry(recipe,base=Path.cwd())
    with closing(NewsQueue(tmp_path,registry)) as queue:
        head=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
        assert head['provider']=='gnn' and head['state']=='pending' and head['attempts']==0
        http,_=http_fixture(recipe,tmp_path,{
            GNN_ORIGIN+'/robots.txt':FakeResponse(b'User-agent: *\nAllow: /\n'),
            URL:FakeResponse(str(document()).encode())})
        try:
            result=run_cycle(queue,http,recipe,max_articles=1,max_discovery_requests=0)
            assert result['states']=={'complete':1}
        finally:http.close()
        stored=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    with closing(NewsQueue(tmp_path,registry)) as reopened:
        assert dict(reopened.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==stored
        assert len(list((tmp_path/'receipts').glob('*.json')))==1


def test_policy_restart_does_not_replay_source_refusal_or_clear_lease(recipe,tmp_path):
    registry=PolicyRegistry(recipe,base=Path.cwd())
    with closing(NewsQueue(tmp_path,registry)) as queue:
        queue.add_articles([{'url':URL}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=?,lease_owner=?,lease_until=? WHERE url=?',
                ('retry','http_429',time.time()+3600,'old_owner',time.time()+3600,URL))
        protected=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    with closing(NewsQueue(tmp_path,registry)) as reopened:
        assert dict(reopened.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==protected


@pytest.mark.parametrize('prior,next_layout',[(GNN_LAYOUT,GNN_CREDITED_LAYOUT),
    (GNN_LAYOUT,GNN_WRITTEN_LAYOUT),(GNN_CREDITED_LAYOUT,GNN_WRITTEN_LAYOUT),
    (GNN_LAYOUT,GNN_LEGACY_LAYOUT),(GNN_WRITTEN_LAYOUT,GNN_LEGACY_LAYOUT)])
def test_body_parser_adoption_does_not_rearm_failed_auth_refusal_or_stored_heads(recipe,tmp_path,prior,next_layout):
    settings={**deepcopy(recipe),'root':str(tmp_path)}
    settings['providers'][0]['embedded_article_layout']=prior
    registry=PolicyRegistry(settings,base=Path.cwd())
    with closing(NewsQueue(tmp_path,registry)) as queue:
        with queue.db:queue.add_articles([{'url':URL}])
        http,_=http_fixture(settings,tmp_path,{
            GNN_ORIGIN+'/robots.txt':FakeResponse(b'User-agent: *\nAllow: /\n'),
            URL:FakeResponse(str(document()).encode())})
        try:run_cycle(queue,http,settings,max_articles=1,max_discovery_requests=0)
        finally:http.close()
        rows=[(GNN_ORIGIN+'/detail.php?sn='+str(300000+i),state,error,due,lease)
            for i,(state,error,due,lease) in enumerate([
                ('body_not_found','body_not_found',time.time()-1,None),
                ('article_attribution_not_verified','article_attribution_not_verified',time.time()-1,None),
                ('awaiting_authorization','awaiting_authorization',0,None),
                ('retry','http_429',time.time()+3600,'other-owner')])]
        with queue.db:
            queue.add_articles([{'url':r[0]} for r in rows])
            for url,state,error,due,lease in rows:
                queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,lease_owner=?,lease_until=? WHERE url=?',
                    (state,error,due,lease,time.time()+3600 if lease else None,url))
        before={r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    next_settings=deepcopy(settings);next_settings['providers'][0]['embedded_article_layout']=next_layout
    next_registry=PolicyRegistry(next_settings,base=Path.cwd())
    with closing(NewsQueue(tmp_path,next_registry)) as queue:
        assert {r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}==before
        assert len(list((tmp_path/'receipts').glob('*.json')))==1


def follow_card(soup):
    node=soup.new_tag('script',id='wallFanspageCardTemplate',type='text/template')
    node.string='''<div class="gamecard__background"></div><div class="gamecard__game">
    <div class="gamecard-img"><a href="javascript:;"></a></div><div class="gamecard-name">
    <p class="gamecard-label-name"></p><p class="gamecard-label-people"><span></span>
    人已追蹤，快追蹤取得最新情報！</p></div></div><div class="gamecard__data">
    <div class="gamecard__info"><a href="javascript:;" class="gamecard__follow"
    data-want2playsn="[86669,87020]"></a></div></div>'''
    soup.select_one('.GN-lbox3B').append(node)
    return node


def test_v3_follow_ui_is_not_article_prose_or_a_rendered_gamecard(v2source,v3source):
    soup=document();expected=extract(soup,v2source)['body'];node=follow_card(soup)
    before=str(soup)
    with pytest.raises(NewsError,match='body_not_found'):extract(soup,v2source)
    row=extract(soup,v3source)
    assert row['body']==expected and str(soup)==before
    notice=json.loads(row['verified_article_notices_json'])
    embed=notice['native_embedded_nonarticle_content'][0]
    assert embed['kind']=='inert_publisher_follow_card_ui'
    assert not embed['written_article_text'] and not embed['media_or_links_fetched']
    assert embed['sha256']==hashlib.sha256(node.string.encode()).hexdigest()
    assert not any(x in row['body'] for x in ('快追蹤','86669','gamecard'))


@pytest.mark.parametrize('change',['id','src','active','new_text','changed_ui','new_node',
    'url','attribute','unknown_class','noninteger_id','empty_ids','too_many_ids','hidden'])
def test_v3_unknown_follow_template_variants_do_not_disappear(v3source,change):
    soup=document();node=follow_card(soup)
    if change=='id':node['id']='unknown_template'
    elif change=='src':node['src']='https://other.test/not-fetched'
    elif change=='active':node['type']='text/javascript'
    elif change=='new_text':node.string=node.string+'<p>ACTUAL_ARTICLE_TEXT</p>'
    elif change=='changed_ui':node.string=node.string.replace('人已追蹤，快追蹤取得最新情報！','ACTUAL_ARTICLE_TEXT')
    elif change=='new_node':node.string=node.string.replace('</span>','</span><img>')
    elif change=='url':node.string=node.string.replace('javascript:;','https://other.test/article')
    elif change=='attribute':node.string=node.string.replace('class="gamecard-name"','class="gamecard-name" data-text="ARTICLE"')
    elif change=='unknown_class':node.string=node.string.replace('gamecard-name','article-name')
    elif change=='noninteger_id':node.string=node.string.replace('[86669,87020]','[true]')
    elif change=='empty_ids':node.string=node.string.replace('[86669,87020]','[]')
    elif change=='too_many_ids':node.string=node.string.replace('[86669,87020]',json.dumps(list(range(1,131))))
    else:node['hidden']=''
    with pytest.raises(NewsError):extract(soup,v3source)


def press_release(soup):
    credit=soup.select_one('.GN-lbox3C');credit['class']=['GN-lbox3CA']
    credit.string='（新聞稿） 2022-09-12 14:54:58 '
    font=soup.new_tag('font',color='white',size='2');font.string='Jessica';credit.append(font)


def test_v3_legacy_press_credit_preserves_label_and_typographic_values(v2source,v3source):
    soup=document();press_release(soup)
    value=soup.select_one('td');value.string=''
    font=soup.new_tag('font',face='Calibri, serif');font.string='-4.88%';value.append(font)
    with pytest.raises(NewsError):extract(soup,v2source)
    row=extract(soup,v3source)
    assert '-4.88%' in row['body'] and '廠商提供資料原文' in row['body']
    assert json.loads(row['authors_json'])==['新聞稿']
    detail=json.loads(row['author_attribution_detail_json'])
    assert detail['role']=='publisher_declared_press_release'
    assert detail['legacy_auxiliary_credit_name']=='Jessica'
    assert detail['publisher_JSONLD_author']=={'@type':'Person','name':'Jessica'}
    assert not detail['legacy_auxiliary_credit_visually_attributed']
    assert not detail['all_body_written_or_owned_by_reporter_verified']
    assert detail['authors_field_is_press_release_label_not_individual_author']
    assert row['published_at_raw']=='2022-09-12T14:54:58+08:00'


@pytest.mark.parametrize('change',['wrong_name','wrong_clock','other_font','nested_font','hidden_font','duplicate_credit'])
def test_v3_press_release_auxiliary_credit_never_guesses_authorship(v3source,change):
    soup=document();press_release(soup)
    font=soup.select_one('.GN-lbox3CA font')
    if change=='wrong_name':font.string='Another'
    elif change=='wrong_clock':soup.select_one('.GN-lbox3CA').contents[0].replace_with('（新聞稿） 2022-09-12 14:55:58 ')
    elif change=='other_font':font['color']='red'
    elif change=='nested_font':font.append(soup.new_tag('span'))
    elif change=='hidden_font':font['hidden']=''
    else:soup.select_one('.GN-lbox3A').append(deepcopy(soup.select_one('.GN-lbox3CA')))
    with pytest.raises(NewsError):extract(soup,v3source)


def test_v3_editorial_desk_is_not_an_individual_reporter(v2source,v3source):
    soup=document();soup.select_one('.GN-lbox3C').string='（編輯部  報導） 2022-09-12 14:54:58'
    script=soup.select_one('script');data=json.loads(script.string)
    data['author']['name']='編輯部-';script.string=json.dumps(data)
    with pytest.raises(NewsError,match='article_attribution_not_verified'):extract(soup,v2source)
    row=extract(soup,v3source)
    assert json.loads(row['authors_json'])==['編輯部']
    detail=json.loads(row['author_attribution_detail_json'])
    assert detail['role']=='publisher_declared_editorial_desk'
    assert not detail['person_identity_independently_verified']
    data['author']['name']='Jessica';script.string=json.dumps(data)
    with pytest.raises(NewsError,match='article_attribution_not_verified'):extract(soup,v3source)


def test_v3_retains_prior_capture_auth_capacity_and_once_only_question_keys(source,v2source,v3source):
    assert len({s.data_fingerprint for s in (source,v2source,v3source)})==3
    assert len({s.authorization_fingerprint for s in (source,v2source,v3source)})==1
    assert v3source.parser_repair_capture_history==(source.data_fingerprint,v2source.data_fingerprint)
    assert v3source.parser_repair_prior_capture_fingerprint==v2source.data_fingerprint
    assert v3source.http_capacity_source_fingerprint==source.data_fingerprint
    from downloader.news_fulltext_candidate_diagnostics import _key
    for evidence in (48,49):
        assert len({_key({'provider':'gnn','id':'1'*64},s.data_fingerprint,evidence_contract_version=evidence)
            for s in (source,v2source,v3source)})==1


def test_v4_legacy_center_wbr_and_empty_follow_list_keep_all_financial_text(v3source,v4source):
    soup=document();center=soup.new_tag('center');table=soup.select_one('table')
    table.wrap(center);table.select_one('td').string='-4.88%'
    soup.select_one('.GN-lbox3B p').append(soup.new_tag('wbr'))
    node=follow_card(soup);node.string=node.string.replace('[86669,87020]','[]')
    with pytest.raises(NewsError):extract(soup,v3source)
    row=extract(soup,v4source)
    assert '-4.88%' in row['body'] and '本月營收' in row['body'] and '快追蹤' not in row['body']
    notice=json.loads(row['verified_article_notices_json'])
    assert notice['native_embedded_nonarticle_content'][0]['linked_game_count']==0


def test_v4_contributing_editor_keeps_native_role_and_declared_name(v3source,v4source):
    soup=document();soup.select_one('.GN-lbox3C').string='（特約編輯 Wei） 2022-09-12 14:54:58'
    script=soup.select_one('script');data=json.loads(script.string)
    data['author']['name']='特約編輯 Wei';script.string=json.dumps(data)
    with pytest.raises(NewsError):extract(soup,v3source)
    row=extract(soup,v4source)
    detail=json.loads(row['author_attribution_detail_json'])
    assert json.loads(row['authors_json'])==['Wei']
    assert detail['role']=='publisher_declared_contributing_editor'
    assert detail['publisher_JSONLD_author']['name']=='特約編輯 Wei'
    data['author']['name']='Wei';script.string=json.dumps(data)
    with pytest.raises(NewsError,match='article_attribution_not_verified'):extract(soup,v4source)


def legacy_video(soup):
    node=BeautifulSoup('''<object width="600" height="400">
    <param name="movie" value="https://www.youtube.com/embed/gLHvRw5Ok3M">
    <param name="allowFullScreen" value="true"><param name="allowscriptaccess" value="always">
    <embed src="https://www.youtube.com/embed/gLHvRw5Ok3M" type="application/x-shockwave-flash"
    allowscriptaccess="always" allowfullscreen="true" width="600" height="400"></object>''','lxml').object
    soup.select_one('.GN-lbox3B').append(node)
    return node


def test_v4_legacy_video_is_unfetched_and_does_not_change_written_body(v3source,v4source):
    soup=document();expected=extract(soup,v3source)['body'];legacy_video(soup)
    with pytest.raises(NewsError):extract(soup,v3source)
    row=extract(soup,v4source)
    assert row['body']==expected
    embed=json.loads(row['verified_article_notices_json'])['native_embedded_nonarticle_content'][0]
    assert embed['kind']=='unfetched_legacy_youtube_wrapper' and not embed['executed']


@pytest.mark.parametrize('change',['written_fallback','other_origin','mismatched_source','extra_parameter','nested_prose','hidden'])
def test_v4_unknown_video_or_fallback_prose_never_disappears(v4source,change):
    soup=document();node=legacy_video(soup)
    if change=='written_fallback':node.append('ACTUAL_ARTICLE_PROSE')
    elif change=='other_origin':node.param['value']='https://other.test/video'
    elif change=='mismatched_source':node.embed['src']='https://www.youtube.com/embed/CnCcWboQ3gM'
    elif change=='extra_parameter':node.append(soup.new_tag('param',value='other'))
    elif change=='nested_prose':node.embed.append(soup.new_tag('p'))
    else:node['hidden']=''
    with pytest.raises(NewsError):extract(soup,v4source)


def gallery(soup):
    from downloader.news_fulltext_gnn import _SLICK_GALLERY_SCRIPT
    name='gnn-slick-banner-horizontal-1678086572949'
    node=BeautifulSoup(f'''<div class="{name} header-banner">
    <div class="banner-horizontal"><img src="https://media.test/a"><img src="https://media.test/b"></div>
    <div class="horizontal-nav"><img src="https://media.test/a"><img src="https://media.test/b"></div>
    <div class="slider-count" data-length="2"><span class="slider-count-span">1/2</span></div></div>''','lxml').div
    script=soup.new_tag('script');script.string=_SLICK_GALLERY_SCRIPT.replace('__gallery__',name)
    body=soup.select_one('.GN-lbox3B');body.append(node);body.append(script)
    return node,script


def test_v4_static_gallery_never_executes_and_excludes_only_verified_counter(v3source,v4source):
    soup=document();expected=extract(soup,v3source)['body'];gallery(soup);before=str(soup)
    with pytest.raises(NewsError,match='body_not_found'):extract(soup,v3source)
    row=extract(soup,v4source)
    assert row['body']==expected and str(soup)==before and '1/2' not in row['body']
    embed=json.loads(row['verified_article_notices_json'])['native_embedded_nonarticle_content'][0]
    assert embed['ui_counter']=='1/2' and not embed['executed'] and not embed['media_or_links_fetched']


@pytest.mark.parametrize('change',['extra_js','different_reference','src','extra_text','counter_text',
    'duplicate_counter','count_mismatch','main_image_missing','hidden'])
def test_v4_unknown_gallery_script_or_written_content_never_drops(v4source,change):
    soup=document();node,script=gallery(soup)
    if change=='extra_js':script.string=script.string+';OTHER_ARTICLE_OR_CODE()'
    elif change=='different_reference':script.string=script.string.replace('1678086572949','1678086572948',1)
    elif change=='src':script['src']='https://other.test/loader'
    elif change=='extra_text':node.append('ACTUAL_WRITTEN_GALLERY_PROSE')
    elif change=='counter_text':node.select_one('.slider-count-span').string='ARTICLE_TEXT'
    elif change=='duplicate_counter':node.append(deepcopy(node.select_one('.slider-count')))
    elif change=='count_mismatch':node.select_one('.slider-count')['data-length']='3'
    elif change=='main_image_missing':node.select_one('.banner-horizontal img').decompose()
    else:node.select_one('.slider-count')['hidden']=''
    with pytest.raises(NewsError):extract(soup,v4source)


def test_v4_retains_all_three_prior_capture_versions_and_once_only_keys(source,v2source,v3source,v4source):
    assert v4source.parser_repair_capture_history==tuple(s.data_fingerprint for s in (source,v2source,v3source))
    assert v4source.http_capacity_source_fingerprint==source.data_fingerprint
    assert v4source.authorization_fingerprint==source.authorization_fingerprint
    from downloader.news_fulltext_candidate_diagnostics import _key
    for evidence in (48,49):
        assert _key({'provider':'gnn','id':'1'*64},v4source.data_fingerprint,evidence_contract_version=evidence)==_key(
            {'provider':'gnn','id':'1'*64},source.data_fingerprint,evidence_contract_version=evidence)
