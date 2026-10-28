"""Source identity, native text and refusal semantics, without publisher calls."""
from contextlib import closing
from copy import deepcopy
import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
import time

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


def test_resident_header_question_is_once_only_metadata_not_a_body_or_new_owner(recipe, tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_queue import NewsQueue
    settings = deepcopy(recipe)
    settings['root'] = str(tmp_path)
    registry = PolicyRegistry(settings, base=Path.cwd())
    with closing(NewsQueue(tmp_path, registry)) as queue:
        queue.add_articles([{'url': URL}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                ('article_rights_or_scope_excluded', 'article_rights_or_scope_excluded',
                 time.time()-1, '2026-01-01T00:00:00Z', URL))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        budget = list(queue.db.execute('SELECT * FROM queue_settings'))
        submitted = diagnostic.submit_candidate(tmp_path, settings, registry, URL,
            'manager_public_written_article_v1', reason='Unresolved native header declarations', evidence_contract_version=46)
        diagnostic.pump_candidates(queue)
        calls = []
        def get(url):
            calls.append(url)
            return Response(url, 200, {'content-type': 'text/html'}, str(document()).encode())
        http = SimpleNamespace(registry=registry, get=get, provider_not_before={},
            blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())
        job = diagnostic.claim_candidate(queue, http, settings, 'managertoday')
        assert job is not None
        result = diagnostic.fetch_candidate(http, job)
        encoded = json.dumps(result, ensure_ascii=False)
        assert 'JSONLD_SUMMARY_AND_SCRIPT_POLLUTION' not in encoded
        assert '原生段落' not in encoded and not result['raw_or_body_retained']
        assert result['manager_native_header']['headers'][0]['credits'][0]['href'] == PROFILE
        assert result['public_document_identity']['declared_articles'][0]['author']['name'] == '支琬清'
        shape = result['manager_native_header']['body_structure_without_prose'][0]
        assert shape['unknown_element_counts'] == {} and not shape['node_limit_reached']
        assert shape['templates'][0]['has_table_cells']
        diagnostic.finish_candidate(queue, job, result=result)
        diagnostic.pump_candidates(queue)
        assert diagnostic.candidate_status(tmp_path, submitted['request_id'])['state'] == 'observed'
        again = diagnostic.submit_candidate(tmp_path, settings, registry, URL,
            'manager_public_written_article_v1', reason='A new reason cannot renew the question', evidence_contract_version=46)
        assert not again['submitted'] and diagnostic.claim_candidate(queue, http, settings, 'managertoday') is None
        assert calls == [URL]
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
        assert list(queue.db.execute('SELECT * FROM queue_settings')) == budget
        assert not (tmp_path/'bodies').exists()


@pytest.mark.parametrize('changes', [
    {'state': 'http_403', 'error_code': 'http_403'},
    {'state': 'robots_denied', 'error_code': 'robots_denied'},
    {'due': 4102444800}, {'lease_owner': 'other'}, {'body_bytes': 1},
])
def test_header_question_preserves_refusal_future_lease_and_body_gates(recipe, tmp_path, changes):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.news_fulltext_queue import NewsQueue
    settings = deepcopy(recipe)
    settings['root'] = str(tmp_path)
    registry = PolicyRegistry(settings, base=Path.cwd())
    with closing(NewsQueue(tmp_path, registry)) as queue:
        queue.add_articles([{'url': URL}])
        values = dict(state='article_title_not_verified', error_code='article_title_not_verified',
            due=time.time()-1, last_checked_at_utc='2026-01-01T00:00:00Z')
        values.update(changes)
        with queue.db:
            queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in values)+' WHERE url=?',
                (*values.values(), URL))
        with pytest.raises((ValueError, NewsError)):
            diagnostic.submit_candidate(tmp_path, settings, registry, URL,
                'manager_public_written_article_v1', reason='Cannot change source refusal', evidence_contract_version=46)


def test_header_observation_key_survives_parser_and_reason_changes():
    from downloader.news_fulltext_candidate_diagnostics import _key
    target = {'provider': 'managertoday', 'id': '1'*64}
    assert _key(target, 'a'*64, evidence_contract_version=46) == _key(target, 'b'*64, evidence_contract_version=46)
    with pytest.raises(ValueError):
        _key({**target, 'provider': 'anue'}, 'a'*64, evidence_contract_version=46)


def test_plain_credit_followup_requires_closed_no_profile_question_and_never_rearms_it(recipe,tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_queue import NewsQueue
    settings=deepcopy(recipe);settings['root']=str(tmp_path)
    registry=PolicyRegistry(settings,base=Path.cwd())
    soup=document()
    for node in soup.select('section.article_top a'):node.decompose()
    header=soup.select_one('section.article_top')
    label=soup.new_tag('span',attrs={'class':['author-label']});label.string='撰文'
    name=soup.new_tag('span',attrs={'class':['author-name']});name.string='簡鈺璇'
    header.append(label);header.append(name)
    calls=[]
    def get(url):
        calls.append(url)
        return Response(url,200,{'content-type':'text/html'},str(soup).encode())
    with closing(NewsQueue(tmp_path,registry)) as queue:
        queue.add_articles([{'url':URL}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                ('article_title_not_verified','article_title_not_verified',time.time()-1,'2026-01-01T00:00:00Z',URL))
        before=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
        http=SimpleNamespace(registry=registry,get=get,provider_not_before={},
            blocked_gateway_hosts=lambda p:(),blocked_robots_origins=lambda p:())
        def submit(version):
            return diagnostic.submit_candidate(tmp_path,settings,registry,URL,
                'manager_public_written_article_v1',reason='Native plain credit hierarchy',evidence_contract_version=version)
        with pytest.raises(ValueError,match='closed_manager_no_profile_header_required'):
            submit(47)
        first=submit(46)
        diagnostic.pump_candidates(queue)
        job=diagnostic.claim_candidate(queue,http,settings,'managertoday')
        facts=diagnostic.fetch_candidate(http,job)
        assert 'plain_credit_header_hierarchy' not in facts['manager_native_header']
        diagnostic.finish_candidate(queue,job,result=facts);diagnostic.pump_candidates(queue)
        followup=submit(47)
        diagnostic.pump_candidates(queue)
        job=diagnostic.claim_candidate(queue,http,settings,'managertoday')
        facts=diagnostic.fetch_candidate(http,job)
        tree=facts['manager_native_header']['plain_credit_header_hierarchy']
        assert any(n['direct_text']=='簡鈺璇' for n in tree['nodes'])
        assert not tree['raw_dom_or_body_retained']
        encoded=json.dumps(facts,ensure_ascii=False)
        assert '原生段落' not in encoded and 'JSONLD_SUMMARY_AND_SCRIPT_POLLUTION' not in encoded
        diagnostic.finish_candidate(queue,job,result=facts);diagnostic.pump_candidates(queue)
        assert diagnostic.candidate_status(tmp_path,first['request_id'])['state']=='observed'
        assert diagnostic.candidate_status(tmp_path,followup['request_id'])['state']=='observed'
        assert not submit(46)['submitted'] and not submit(47)['submitted']
        assert calls==[URL,URL]
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==before


def test_plain_credit_metadata_is_bounded_and_does_not_modify_the_source_dom():
    from downloader.news_fulltext_managertoday import manager_native_metadata
    soup=document();header=soup.select_one('section.article_top')
    for _ in range(100):
        node=soup.new_tag('span');node.string='大'*1000;header.append(node)
    before=str(soup)
    result=manager_native_metadata(soup,plain_credit_header=True)
    tree=result['plain_credit_header_hierarchy']
    assert len(tree['nodes'])<=48 and tree['node_or_byte_limit_reached']
    assert len(json.dumps(tree,ensure_ascii=False).encode())<14000
    assert str(soup)==before


@pytest.mark.parametrize('change', ['pending','uncertain','redirected','raw','profile','other_question'])
def test_plain_credit_followup_rejects_incomplete_or_different_prior_evidence(source,change):
    from downloader.news_fulltext_candidate_diagnostics import _prior_manager_no_profile_header
    from downloader.news_fulltext_managertoday import MANAGER_METADATA_QUESTION
    state='observed'
    result={'question':MANAGER_METADATA_QUESTION,'requested_url':URL,'final_url':URL,
        'status':200,'raw_or_body_retained':False,
        'manager_native_header':{'header_count':1,'headers':[{'credits':[]}]}}
    if change=='pending':state='ready'
    elif change=='uncertain':state='uncertain_outcome'
    elif change=='redirected':result['final_url']=URL+'0'
    elif change=='raw':result['raw_or_body_retained']=True
    elif change=='profile':result['manager_native_header']['headers'][0]['credits']=[{'href':PROFILE}]
    else:result['question']='different_question'
    with sqlite3.connect(':memory:') as db:
        db.row_factory=sqlite3.Row
        db.execute('CREATE TABLE native_candidate_jobs(provider TEXT,state TEXT,result_json TEXT,request_json TEXT)')
        db.execute('INSERT INTO native_candidate_jobs VALUES(?,?,?,?)',('managertoday',state,
            json.dumps(result),json.dumps({'evidence_contract_version':46,'target':{'url':URL}})))
        with pytest.raises(ValueError,match='closed_manager_no_profile_header_required'):
            _prior_manager_no_profile_header(db,source,URL)


URL = 'https://www.managertoday.com.tw/articles/view/73138'
PROFILE = 'https://www.managertoday.com.tw/columnist/view/5738'
TITLE = '示例基金資料與公開資訊'


@pytest.fixture(scope='module')
def recipe():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    entry = deepcopy(next(p for p in config['providers'] if p['id'] == 'managertoday'))
    # Keep acceptance of the four preserved v1 originals independently testable
    # after the live catalog adopts v2.
    entry['embedded_article_layout'] = 'manager_public_written_article_v1'
    entry['adapter_verification'] = 'native_header_metadata_questions46_47_plain_writing_date_pairs_and_distinct_SEO_OG_headline_v3_contracts_20261025'
    return {**config, 'providers': [entry]}


@pytest.fixture(scope='module')
def source(recipe):
    return PolicyRegistry(recipe, base=Path.cwd()).sources['managertoday']


@pytest.fixture(scope='module')
def v2source(recipe):
    config = deepcopy(recipe)
    config['providers'][0]['embedded_article_layout'] = 'manager_public_written_article_v2'
    return PolicyRegistry(config, base=Path.cwd()).sources['managertoday']


@pytest.fixture(scope='module')
def v3source(recipe):
    config = deepcopy(recipe)
    config['providers'][0]['embedded_article_layout'] = 'manager_public_written_article_v3'
    return PolicyRegistry(config, base=Path.cwd()).sources['managertoday']


@pytest.fixture(scope='module')
def v4source(recipe):
    from downloader.news_fulltext_managertoday import MANAGER_V4_VERIFICATION
    config = deepcopy(recipe)
    config['providers'][0].update(embedded_article_layout='manager_public_written_article_v4',
        adapter_verification=MANAGER_V4_VERIFICATION)
    return PolicyRegistry(config, base=Path.cwd()).sources['managertoday']


@pytest.fixture(scope='module')
def v5source(recipe):
    from downloader.news_fulltext_managertoday import MANAGER_V5_VERIFICATION
    config=deepcopy(recipe)
    config['providers'][0].update(embedded_article_layout='manager_public_written_article_v5',
        adapter_verification=MANAGER_V5_VERIFICATION)
    return PolicyRegistry(config,base=Path.cwd()).sources['managertoday']


def test_v5_changes_capture_only_and_reuses_exact_predecessor_http_state(v4source,v5source):
    from downloader.news_fulltext_managertoday import MANAGER_V5_HTTP_BINDING
    from downloader.news_fulltext_retained_native import _resident_transition
    assert v5source.allowed
    assert (v4source.data_fingerprint,v5source.data_fingerprint,v5source.authorization_fingerprint,
        v5source.http_capacity_source_fingerprint,v5source.http_capacity_authorization_fingerprint)==MANAGER_V5_HTTP_BINDING
    assert v5source.authorization_fingerprint==v4source.authorization_fingerprint
    assert v5source.parser_repair_prior_capture_fingerprint==v4source.data_fingerprint
    assert (v5source.http_capacity_source_fingerprint,v5source.http_capacity_authorization_fingerprint)==(
        v4source.http_capacity_source_fingerprint,v4source.http_capacity_authorization_fingerprint)
    transition=_resident_transition(v5source)
    assert transition[0:3]==(58,'manager_public_written_article_v4',v4source.data_fingerprint)
    assert transition[5]==frozenset({'body_not_found'})


@pytest.mark.parametrize('field,value',[('interval_seconds',3),('note','Unreviewed source scope'),
    ('capacity_max_parallelism',3)])
def test_v5_other_scope_or_pacing_cannot_reuse_capacity_or_original(recipe,field,value):
    from downloader.news_fulltext_managertoday import MANAGER_V5_VERIFICATION
    from downloader.news_fulltext_retained_native import _resident_transition
    config=deepcopy(recipe)
    config['providers'][0].update(embedded_article_layout='manager_public_written_article_v5',
        adapter_verification=MANAGER_V5_VERIFICATION)
    config['providers'][0][field]=value
    source=PolicyRegistry(config,base=Path.cwd()).sources['managertoday']
    assert source.allowed and not source.http_capacity_source_fingerprint
    assert not source.parser_repair_prior_capture_fingerprint
    with pytest.raises(ValueError,match='reviewed resident current source invalid'):
        _resident_transition(source)


def static_document_soup():
    from downloader.news_fulltext_managertoday import _CARD_EVENTS
    soup=document()
    template=soup.select_one('main.htmlview template');template.clear()
    fragment=BeautifulSoup('''<!DOCTYPE html><html><head><meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>INERT_CARD_METADATA_TITLE</title></head><body><div style="width:100%">
      <div style="display:flex"><span>00929 ETF</span><div>-1,234.50</div><span>0.50%</span></div>
      <div>2020/03/02</div><div>1:5</div><div>每月配息</div></div></body></html>''','html.parser')
    card=fragment.body.div.div
    for key,value in _CARD_EVENTS.items():card[key]=value
    for child in list(fragment.contents):template.append(child.extract())
    return soup


def test_v5_preserves_all_static_card_values_but_separates_document_metadata(v4source,v5source,monkeypatch):
    from dataclasses import replace
    from downloader import news_fulltext_managertoday as native
    soup=static_document_soup();raw=str(soup).encode()
    with pytest.raises(NewsError,match='body_not_found'):
        extract_article(raw,v4source,page_url=URL)
    # Validation and extraction reuse one parsed document per page parse.
    calls=[];parse=native.BeautifulSoup
    def observed(*args,**kwargs):
        calls.append(1);return parse(*args,**kwargs)
    monkeypatch.setattr(native,'BeautifulSoup',observed)
    article=extract_article(raw,v5source,page_url=URL)
    assert len(calls)==1
    assert article==extract_article(raw,replace(v5source,html_parser='html.parser'),page_url=URL)
    assert len(calls)==2
    assert article['title']==TITLE
    for text in ('00929 ETF','-1,234.50','0.50%','2020/03/02','1:5','每月配息'):
        assert text in article['body']
    assert article['body'].count('00929 ETF')==1
    assert not any(text in article['body'] for text in ('INERT_CARD_METADATA_TITLE','NEIGHBOR','POLLUTION','boxShadow'))
    facts=json.loads(article['verified_article_notices_json'])
    assert facts['inert_table_declarations']==0
    card,=facts['inert_static_document_declarations']
    assert card['document_title']=='INERT_CARD_METADATA_TITLE'
    assert card['metadata']==[{'charset':'utf-8'},{'name':'viewport','content':'width=device-width, initial-scale=1'}]
    assert not card['rendered_or_executed'] and not card['external_resources_requested']
    assert card['event_attributes_present']==['onmouseout','onmouseover']
    assert facts['inert_document_metadata_not_article_title_or_release']
    assert article['published_at_raw']=='2026-10-20T13:49:00+08:00'
    assert json.loads(article['authors_json'])==['支琬清']


@pytest.mark.parametrize('change',['viewport','extra_metadata','blank_title','script','resource','unknown_tag',
    'unknown_event','changed_event','binding','hidden','hidden_style','empty','stray_text','extra_root',
    'outside_metadata','wrong_parent','wrong_ref'])
def test_v5_cannot_drop_unreviewed_document_content_or_promote_hidden_text(v5source,change):
    soup=static_document_soup();template=soup.select_one('main.htmlview template')
    fragment=BeautifulSoup(template.decode_contents(),'html.parser');root=fragment.body.div
    if change=='viewport':fragment.select_one('meta[name=viewport]')['content']='OTHER'
    elif change=='extra_metadata':fragment.head.append(soup.new_tag('meta',attrs={'name':'author','content':'OTHER'}))
    elif change=='blank_title':fragment.title.string=' '
    elif change=='script':
        node=soup.new_tag('script');node.string='WRITE_MORE_TEXT()';root.append(node)
    elif change=='resource':root.append(soup.new_tag('iframe',src='https://other.example/'))
    elif change=='unknown_tag':root.append(soup.new_tag('p'))
    elif change=='unknown_event':root['onclick']='FETCH_MORE()'
    elif change=='changed_event':root['onmouseover']='FETCH_MORE()'
    elif change=='binding':root['x-text']='remoteProse'
    elif change=='hidden':root['aria-hidden']='true'
    elif change=='hidden_style':root['style']='display: none'
    elif change=='empty':root.clear()
    elif change=='stray_text':fragment.body.append('UNBOUND_TEXT')
    elif change=='extra_root':fragment.body.append(soup.new_tag('div'))
    elif change=='outside_metadata':soup.select_one('main.htmlview').append(soup.new_tag('title'))
    elif change=='wrong_parent':template.parent['x-data']='unreviewed'
    else:template['x-ref']='other'
    template.clear()
    for child in list(fragment.contents):template.append(child.extract())
    with pytest.raises(NewsError):extract(soup,v5source)


def test_v5_retains_v4_plain_byline_and_legacy_table_values(v4source,v5source):
    for soup in (document(),native_byline_document('經理人編輯部'),native_byline_document('齊立文、周頌宜','採訪‧撰文')):
        before,after=extract(soup,v4source),extract(soup,v5source)
        assert all(after[k]==before[k] for k in ('body','body_sha256','title','authors_json',
            'author_attribution_detail_json','published_at_raw','modified_at_raw','published_time_basis'))


def test_v4_capture_changes_preserve_exact_effective_http_identity_and_authorization(v3source,v4source):
    from downloader.news_fulltext_managertoday import MANAGER_V4_HTTP_BINDING
    assert v4source.allowed
    assert v3source.data_fingerprint == MANAGER_V4_HTTP_BINDING[0]
    assert v4source.data_fingerprint == MANAGER_V4_HTTP_BINDING[1]
    assert v4source.authorization_fingerprint == v3source.authorization_fingerprint == MANAGER_V4_HTTP_BINDING[2]
    assert v4source.parser_repair_prior_capture_fingerprint == v3source.data_fingerprint
    assert v4source.http_capacity_source_fingerprint == (v3source.http_capacity_source_fingerprint or v3source.data_fingerprint)
    assert v4source.http_capacity_authorization_fingerprint == (v3source.http_capacity_authorization_fingerprint or v3source.authorization_fingerprint)


@pytest.mark.parametrize('field,value', [('interval_seconds',3),('note','Different unreviewed scope'),
    ('capacity_max_parallelism',3)])
def test_v4_unreviewed_config_cannot_inherit_old_http_state(recipe,field,value):
    from downloader.news_fulltext_managertoday import MANAGER_V4_VERIFICATION
    config=deepcopy(recipe)
    config['providers'][0].update(embedded_article_layout='manager_public_written_article_v4',
        adapter_verification=MANAGER_V4_VERIFICATION)
    config['providers'][0][field]=value
    source=PolicyRegistry(config,base=Path.cwd()).sources['managertoday']
    assert source.allowed and not source.http_capacity_source_fingerprint
    assert not source.http_capacity_authorization_fingerprint and not source.parser_repair_prior_capture_fingerprint


def test_v4_requires_reviewed_verification_and_never_accepts_nul(v4source,recipe):
    from downloader.news_fulltext_managertoday import MANAGER_V4_VERIFICATION
    config=deepcopy(recipe);config['providers'][0].update(embedded_article_layout='manager_public_written_article_v4',
        adapter_verification=MANAGER_V4_VERIFICATION+'different')
    with pytest.raises(ValueError,match='unreviewed embedded article parser'):
        PolicyRegistry(config,base=Path.cwd())
    with pytest.raises(NewsError):
        extract_article(str(native_byline_document('Y Chen')).encode()+b'\0',v4source,page_url=URL)


@pytest.mark.parametrize('evidence',[46,47])
def test_v4_never_rearms_closed_header_questions_via_layout_or_fingerprint(v3source,v4source,evidence):
    from downloader.news_fulltext_candidate_diagnostics import _key
    from downloader.news_fulltext_candidates import candidate_evidence_allowed
    old = {'provider':'managertoday','id':'literal-existing-head','source_fingerprint':v3source.data_fingerprint}
    current = {**old,'source_fingerprint':v4source.data_fingerprint}
    assert _key(old,v3source.embedded_article_layout,evidence_contract_version=evidence) == _key(
        current,v4source.embedded_article_layout,evidence_contract_version=evidence)
    assert candidate_evidence_allowed(v4source,v4source,evidence)


def embedded_document_response():
    from downloader.news_fulltext_http import Response
    soup = document()
    template = soup.select_one('main.htmlview template')
    template.clear()
    template.append(soup.new_tag('meta',charset='utf-8'))
    title=soup.new_tag('title');title.string='UNREVIEWED_EMBEDDED_DOCUMENT_TITLE';template.append(title)
    prose=soup.new_tag('p');prose.string='未知靜態嵌入書寫內容。'*20;template.append(prose)
    return Response(URL,200,{'content-type':'text/html','set-cookie':'SECRET'},str(soup).encode())


def test_failed_embedded_original_never_accepts_body_or_invents_article(v4source):
    from downloader.news_fulltext_managertoday import private_manager_body_original_permit
    from downloader.news_fulltext_candidates import candidate_response_metadata
    raw=embedded_document_response()
    with pytest.raises(NewsError,match='body_not_found'):
        extract_article(raw.body,v4source,page_url=URL)
    permit=private_manager_body_original_permit(raw,v4source,URL)
    assert permit['literal_header_and_clock_verified'] and not permit['article_accepted']
    assert not permit['body_geometry_accepted']
    facts=candidate_response_metadata(raw,v4source,v4source,evidence_contract_version=58,requested_url=URL)
    assert facts['private_original_permit']==permit
    assert not facts['raw_or_names_or_body_retained'] and not facts['accepted_as_article']
    assert 'UNREVIEWED' not in json.dumps(facts) and '未知靜態' not in json.dumps(facts,ensure_ascii=False)


@pytest.mark.parametrize('mutation',['complete','canonical','author','date','reprint','access','redirect','size'])
def test_failed_embedded_original_requires_existing_identity_credit_clock_and_refusal_gates(v4source,mutation):
    from downloader.news_fulltext_managertoday import private_manager_body_original_permit
    from downloader.news_fulltext_http import Response
    raw=embedded_document_response()
    soup=BeautifulSoup(raw.body,'lxml')
    if mutation=='complete':soup=document()
    elif mutation=='canonical':soup.select_one('link[rel=canonical]')['href']=URL+'0'
    elif mutation=='author':soup.select_one('section.article_top a')['data-dl_text']='其他署名'
    elif mutation=='date':soup.select_one('meta[property="article:published_time"]')['content']='2026-10-19T13:49:00+08:00'
    elif mutation=='reprint':soup.select_one('main.htmlview p').string='本文轉載自其他媒體'
    elif mutation=='access':
        node=soup.select_one('script[type="application/ld+json"]');value=json.loads(node.get_text())
        value['isAccessibleForFree']=False;node.string=json.dumps(value,ensure_ascii=False)
    body=b'x'*((2<<20)+1) if mutation=='size' else str(soup).encode()
    raw=Response(URL+'0' if mutation=='redirect' else URL,200,{'content-type':'text/html'},body)
    with pytest.raises(NewsError):private_manager_body_original_permit(raw,v4source,URL)


@pytest.fixture
def embedded_original_owner(recipe,tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.news_fulltext_managertoday import MANAGER_V4_VERIFICATION
    from downloader.news_fulltext_queue import NewsQueue
    settings=deepcopy(recipe);settings['root']=str(tmp_path)
    settings['providers'][0].update(embedded_article_layout='manager_public_written_article_v4',adapter_verification=MANAGER_V4_VERIFICATION)
    calls=[]
    def get(url,**kwargs):
        calls.append((url,kwargs));return embedded_document_response()
    with closing(NewsQueue(tmp_path,PolicyRegistry(settings,base=Path.cwd()))) as queue:
        queue.add_articles([{'url':URL}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',attempts=6,due=0,"
                "last_checked_at_utc='2026-10-24T10:00:00+00:00' WHERE url=?",(URL,))
        source=queue.registry.sources['managertoday']
        submit=lambda evidence:diagnostic.submit_candidate(tmp_path,settings,queue.registry,URL,
            source.embedded_article_layout,reason='Fixed source-native embedded-document question',evidence_contract_version=evidence)
        with pytest.raises(ValueError,match='closed_manager_failed_embedded_document_shape_required'):submit(58)
        http=SimpleNamespace(get=get,provider_not_before={},blocked_gateway_hosts=lambda _:(),blocked_robots_origins=lambda _:())
        previous=submit(46);diagnostic.pump_candidates(queue)
        job=diagnostic.claim_candidate(queue,http,settings,'managertoday')
        diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job))
        assert diagnostic.candidate_status(tmp_path,previous['request_id'])['state']=='observed'
        calls.clear()
        yield queue,settings,submit,http,calls


def test_embedded_original_uses_once_only_owner_and_private_receipt_without_head_mutation(embedded_original_owner,monkeypatch):
    import pyarrow.parquet as pq
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    queue,settings,submit,http,calls=embedded_original_owner
    before=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    budgets=list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))
    old_jobs=[dict(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**kw:pytest.fail('no second HTTP owner'))
    requested=submit(58);diagnostic.pump_candidates(queue)
    job=diagnostic.claim_candidate(queue,http,settings,'managertoday')
    diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job))
    status=diagnostic.candidate_status(queue.root,requested['request_id'])
    assert status['state']=='observed' and calls==[(URL,{'follow_redirects':False})]
    result=status['result_json'];assert result['raw_or_names_or_body_retained'] and not result['accepted_as_article']
    ref=result['private_original'];path=queue.root/ref['path']
    record,=pq.ParquetFile(path).read().to_pylist()
    assert record['raw_response_body']==embedded_document_response().body
    assert hashlib.sha256(record['raw_response_body']).hexdigest()==ref['response_sha256']
    assert not record['publish'] and not record['training_injection_enabled'] and not record['remote_training_export']
    assert 'SECRET' not in record['headers_json'] and path.stat().st_mode&0o077==0
    receipt=json.loads((queue.root/ref['receipt_path']).read_bytes())
    assert receipt['original']==ref and not receipt['accepted_as_article']
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==before
    assert list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))==budgets
    assert [dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(r['request_id'],)).fetchone()) for r in old_jobs]==old_jobs
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    assert not submit(58)['submitted'] and not submit(46)['submitted']
    target=json.loads(queue.db.execute('SELECT request_json FROM native_candidate_jobs WHERE request_id=?',
        (requested['request_id'],)).fetchone()[0])['target']
    assert diagnostic._key(target,'a'*64,evidence_contract_version=58)==diagnostic._key(target,'b'*64,evidence_contract_version=58)


@pytest.mark.parametrize('gate',['cooldown','gateway','robots','uncertain','stored','lease'])
def test_embedded_original_keeps_existing_wait_and_refusal_gates(embedded_original_owner,gate):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    queue,settings,submit,http,calls=embedded_original_owner
    requested=submit(58);diagnostic.pump_candidates(queue)
    if gate=='cooldown':http.provider_not_before['managertoday']=time.time()+600
    elif gate=='gateway':http.blocked_gateway_hosts=lambda _:('www.managertoday.com.tw',)
    elif gate=='robots':http.blocked_robots_origins=lambda _:('https://www.managertoday.com.tw',)
    else:
        with queue.db:
            if gate=='uncertain':queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(requested['request_id'],))
            elif gate=='stored':queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?',('f'*64,URL))
            else:queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=? WHERE url=?",(time.time()+60,URL))
        diagnostic.pump_candidates(queue)
    assert diagnostic.claim_candidate(queue,http,settings,'managertoday') is None and not calls


def multiple_article_document():
    soup=native_byline_document('支琬清')
    other=deepcopy(soup.select_one('div.main_box'))
    other.select_one('h1').string='OTHER_ARTICLE_TITLE'
    other.select_one('main.htmlview p').string='OTHER_ARTICLE_WRITTEN_TEXT'
    soup.body.append(other)
    return soup


def test_multiple_original_is_header_bound_but_never_a_body_or_root_association(v5source):
    from dataclasses import replace
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_managertoday import MANAGER_MULTIPLE_ORIGINAL_QUESTION,private_manager_multiple_original_permit
    soup=multiple_article_document();response=Response(URL,200,{'content-type':'text/html'},str(soup).encode())
    with pytest.raises(NewsError,match='article_identity_not_verified'):extract(soup,v5source)
    permit=private_manager_multiple_original_permit(response,v5source,URL)
    assert permit==private_manager_multiple_original_permit(response,replace(v5source,html_parser='html.parser'),URL)
    assert permit['question']==MANAGER_MULTIPLE_ORIGINAL_QUESTION and permit['matched_native_header_index']==0
    assert permit['matched_native_header_and_clock_verified']
    assert not any(permit[k] for k in ('article_roots_bound','body_geometry_accepted','article_accepted'))
    assert 'OTHER_ARTICLE' not in json.dumps(permit) and '原生段落' not in json.dumps(permit,ensure_ascii=False)


@pytest.mark.parametrize('change',['canonical','OG','schema_URL','author','missing_date','reprint','access',
    'hidden','duplicate_match','one_root','three_roots','redirect','hop','nul','size','old_layout'])
def test_multiple_original_keeps_literal_header_clock_scope_and_transport_gates(v5source,change):
    from dataclasses import replace
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_managertoday import private_manager_multiple_original_permit
    soup=multiple_article_document();script=soup.select_one('script[type="application/ld+json"]')
    declared=json.loads(script.string)
    if change=='canonical':soup.select_one('link[rel=canonical]')['href']=URL+'0'
    elif change=='OG':soup.select_one('meta[property="og:title"]')['content']='OTHER|經理人'
    elif change=='schema_URL':declared['url']=URL+'0'
    elif change=='author':declared['author']['name']='OTHER_AUTHOR'
    elif change=='missing_date':soup.select_one('section.article_top span').decompose()
    elif change=='reprint':soup.select_one('main.htmlview p').string='本文轉載自其他媒體'
    elif change=='access':declared['isAccessibleForFree']=False
    elif change=='hidden':soup.select_one('section.article_top')['aria-hidden']='true'
    elif change=='duplicate_match':soup.select('h1')[1].string=TITLE
    elif change=='one_root':soup.select('div.main_box')[1].decompose()
    elif change=='three_roots':soup.body.append(deepcopy(soup.select('div.main_box')[1]))
    elif change=='old_layout':v5source=replace(v5source,embedded_article_layout='manager_public_written_article_v4')
    script.string=json.dumps(declared,ensure_ascii=False)
    raw=str(soup).encode()+ (b'\0' if change=='nul' else b'')
    if change=='size':raw=b'x'*((2<<20)+1)
    response=Response(URL+'0' if change=='redirect' else URL,200,{'content-type':'text/html'},raw,
        ('unfollowed-hop',) if change=='hop' else ())
    with pytest.raises(NewsError):private_manager_multiple_original_permit(response,v5source,URL)


@pytest.fixture
def multiple_original_owner(recipe,tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_managertoday import MANAGER_V5_VERIFICATION
    from downloader.news_fulltext_queue import NewsQueue
    settings=deepcopy(recipe);settings['root']=str(tmp_path)
    settings['providers'][0].update(embedded_article_layout='manager_public_written_article_v5',adapter_verification=MANAGER_V5_VERIFICATION)
    calls=[];raw=Response(URL,200,{'content-type':'text/html'},str(multiple_article_document()).encode())
    def get(url,**kwargs):calls.append((url,kwargs));return raw
    with closing(NewsQueue(tmp_path,PolicyRegistry(settings,base=Path.cwd()))) as queue:
        queue.add_articles([{'url':URL}])
        with queue.db:queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            "attempts=6,due=0,last_checked_at_utc='2026-10-24T10:00:00+00:00' WHERE url=?",(URL,))
        source=queue.registry.sources['managertoday']
        submit=lambda evidence:diagnostic.submit_candidate(tmp_path,settings,queue.registry,URL,source.embedded_article_layout,
            reason='One previously unretained multiple-root original; never repeat old header question',evidence_contract_version=evidence)
        with pytest.raises(ValueError,match='closed_manager_two_root_shape_required'):submit(59)
        http=SimpleNamespace(get=get,provider_not_before={},blocked_gateway_hosts=lambda _:(),blocked_robots_origins=lambda _:())
        old=submit(46);diagnostic.pump_candidates(queue)
        job=diagnostic.claim_candidate(queue,http,settings,'managertoday')
        diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job))
        assert diagnostic.candidate_status(tmp_path,old['request_id'])['state']=='observed'
        calls.clear()
        yield queue,settings,submit,http,calls,raw


def test_multiple_original_runs_once_via_owner_and_preserves_heads_budgets_and_closed_question(multiple_original_owner,monkeypatch):
    import pyarrow.parquet as pq
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    queue,settings,submit,http,calls,response=multiple_original_owner
    oldhead=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    oldjob=dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone())
    budget=list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('No second HTTP owner'))
    requested=submit(59);diagnostic.pump_candidates(queue)
    job=diagnostic.claim_candidate(queue,http,settings,'managertoday')
    diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job))
    facts=diagnostic.candidate_status(queue.root,requested['request_id'])['result_json']
    assert facts['raw_retention_state']=='retained_verified' and not facts['accepted_as_article']
    ref=facts['private_original'];path=queue.root/ref['path']
    record,=pq.ParquetFile(path).read().to_pylist()
    assert record['raw_response_body']==response.body and path.stat().st_mode&0o077==0
    assert hashlib.sha256(response.body).hexdigest()==ref['response_sha256']
    assert calls==[(URL,{'follow_redirects':False})]
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==oldhead
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(oldjob['request_id'],)).fetchone())==oldjob
    assert list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))==budget
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    assert not submit(59)['submitted'] and not submit(46)['submitted']
    target=json.loads(queue.db.execute('SELECT request_json FROM native_candidate_jobs WHERE request_id=?',(requested['request_id'],)).fetchone()[0])['target']
    assert diagnostic._key(target,'a'*64,evidence_contract_version=59)==diagnostic._key(target,'b'*64,evidence_contract_version=59)


@pytest.mark.parametrize('gate',['cooldown','gateway','robots','lease','stored','uncertain'])
def test_multiple_original_respects_existing_owner_gates(multiple_original_owner,gate):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    queue,settings,submit,http,calls,_=multiple_original_owner
    requested=submit(59);diagnostic.pump_candidates(queue)
    if gate=='cooldown':http.provider_not_before['managertoday']=time.time()+600
    elif gate=='gateway':http.blocked_gateway_hosts=lambda _:('www.managertoday.com.tw',)
    elif gate=='robots':http.blocked_robots_origins=lambda _:('https://www.managertoday.com.tw',)
    else:
        with queue.db:
            if gate=='lease':queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=? WHERE url=?",(time.time()+60,URL))
            elif gate=='stored':queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?',('f'*64,URL))
            else:queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(requested['request_id'],))
    assert diagnostic.claim_candidate(queue,http,settings,'managertoday') is None and not calls


@pytest.fixture
def static_document_approval(embedded_original_owner,tmp_path):
    import pyarrow.parquet as pq
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.artifact_io import atomic_write_json,sha256_file
    from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_managertoday import MANAGER_V5_VERIFICATION
    from downloader.news_fulltext_queue import NewsQueue,utc_now
    from downloader.news_fulltext_retained_native import _article_digest
    queue,settings,submit,http,_=embedded_original_owner
    response=Response(URL,200,{'content-type':'text/html'},str(static_document_soup()).encode())
    http.get=lambda *a,**kw:response
    requested=submit(58);diagnostic.pump_candidates(queue)
    job=diagnostic.claim_candidate(queue,http,settings,'managertoday')
    diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job))
    completed=dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (requested['request_id'],)).fetchone())
    reference=json.loads(completed['result_json'])['private_original']
    raw,=pq.ParquetFile(queue.root/reference['path']).read().to_pylist()
    before=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    queue.close()
    settings['providers'][0].update(embedded_article_layout='manager_public_written_article_v5',
        adapter_verification=MANAGER_V5_VERIFICATION)
    with closing(NewsQueue(tmp_path,PolicyRegistry(settings,base=Path.cwd()))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==before
        source=queue.registry.sources['managertoday'];article=extract_article(raw['raw_response_body'],source,page_url=URL)
        item=dict(url=URL,request_id=requested['request_id'],diagnostic_path=reference['path'],
            diagnostic_receipt_path=reference['receipt_path'],diagnostic_sha256=reference['sha256'],
            diagnostic_receipt_sha256=sha256_file(queue.root/reference['receipt_path']),
            response_sha256=reference['response_sha256'],response_bytes=reference['response_bytes'],
            captured_at_utc=raw['observed_at_utc'],candidate_job_sha256=_article_digest(completed),
            article_keys=sorted(article),article_fields_sha256=_article_digest(article),
            expected_missing_head={k:before[k] for k in RETAINED_SUPPLEMENT_HEAD_FIELDS},
            **{k:article[k] for k in ('body_sha256','authors_json','published_at_raw','modified_at_raw')})
        report=tmp_path/'review'/'observed.json'
        atomic_write_json(report,dict(private_originals=[dict(request_id=requested['request_id'],url=URL,final_url=URL,
            reference=reference,receipt_sha256=item['diagnostic_receipt_sha256'],accepted_as_body=False)]),durable=True)
        manifest=dict(contract_version=2,scope_kind='reviewed_resident_native_materialization_v2',provider='managertoday',
            data_fingerprint=source.data_fingerprint,authorization_fingerprint=source.authorization_fingerprint,
            approved_at_utc=utc_now(),diagnostic_scope_unchanged=True,parser_transition_reviewed=True,
            publish=False,remote_training_export=False,observation_report=report.name,
            observation_report_sha256=sha256_file(report),entries=[item])
        path=report.parent/'approved.json';atomic_write_json(path,manifest,durable=True)
        yield queue,path,manifest,reference,raw,completed


def test_v5_canonical_reuse_preserves_original_clock_closed_questions_and_raw(static_document_approval,monkeypatch):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    queue,path,manifest,reference,raw,closed=static_document_approval
    original=(queue.root/reference['path']).read_bytes()
    budgets=list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a,**k:pytest.fail('retained original needs no second HTTP owner'))
    result=materialize_reviewed_native(queue,[URL],path,lease_seconds=300)
    assert result['materialized_bodies']==1 and result['publisher_requests_this_command']==0
    head=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    record,=pq.ParquetFile(queue.root/head['parquet_path']).read().to_pylist()
    assert record['raw_response_body']==raw['raw_response_body']
    assert record['captured_at_utc']==record['source_observed_at_utc']==raw['observed_at_utc']
    assert record['body_sha256']==manifest['entries'][0]['body_sha256']
    assert '00929 ETF' in record['body'] and 'INERT_CARD_METADATA_TITLE' not in record['body']
    assert not record['historical_point_in_time'] and not record['remote_training_export']
    assert (queue.root/reference['path']).read_bytes()==original
    assert list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))==budgets
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(closed['request_id'],)).fetchone())==closed
    assert materialize_reviewed_native(queue,[URL],path,lease_seconds=300)['materialized_bodies']==0
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==head


@pytest.mark.parametrize('case',['head','receipt','source','closed_question','article_digest'])
def test_v5_canonical_reuse_rejects_changed_original_approval_or_head(static_document_approval,case):
    from dataclasses import replace
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    queue,path,manifest,reference,_,closed=static_document_approval
    if case=='head':
        with queue.db:queue.db.execute("UPDATE articles SET lease_owner='other' WHERE url=?",(URL,))
    elif case=='receipt':(queue.root/reference['receipt_path']).write_bytes(b'{}')
    elif case=='source':
        queue.registry.sources['managertoday']=replace(queue.registry.sources['managertoday'],authorization_fingerprint='a'*64)
    elif case=='closed_question':
        with queue.db:queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(closed['request_id'],))
    else:
        manifest['entries'][0]['article_fields_sha256']='a'*64;atomic_write_json(path,manifest,durable=True)
    if case=='head':
        assert materialize_reviewed_native(queue,[URL],path,lease_seconds=300)['materialized_bodies']==0
    else:
        with pytest.raises(ValueError):materialize_reviewed_native(queue,[URL],path,lease_seconds=300)
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()


def native_byline_document(name, role=''):
    soup = plain_credit_document()
    declaration = soup.select_one('script[type="application/ld+json"]')
    value = json.loads(declaration.get_text())
    value['author']['name'] = name
    declaration.string = json.dumps(value, ensure_ascii=False)
    for group in soup.select('section.article_top > div'):
        group.select('span')[-1].string = (role + ' ' if role else '') + name
    return soup


@pytest.mark.parametrize('name,role,kind', [
    ('周頌宜','採訪‧撰文','source_declared_literal_name'),
    ('Y Chen','整理‧撰文','source_declared_literal_name'),
    ('Y Chen','','source_declared_literal_name'),
    ('小資女艾蜜莉','','source_declared_literal_name'),
    ('大江生活','','source_declared_literal_name'),
    ('齊立文、周頌宜','採訪‧撰文','unparsed_composite'),
    ('盧廷羲','採訪．撰文','source_declared_literal_name'),
    ('吳美欣','採訪・撰文','source_declared_literal_name'),
    ('吳美欣','整理 · 撰文','source_declared_literal_name'),
    ('吳美欣','整理·撰文','source_declared_literal_name'),
    ('經理人編輯部','','editorial_desk'),
    ('經理人用戶成長中心','','editorial_desk'),
    ('支琬清','經理人用戶成長中心','source_declared_literal_name'),
])
def test_v4_native_literal_byline_preserves_names_roles_and_not_human_inference(v3source,v4source,name,role,kind):
    from dataclasses import replace
    soup = native_byline_document(name,role)
    with pytest.raises(NewsError):
        extract(soup,v3source)
    result = extract(soup,v4source)
    assert result == extract(soup,replace(v4source,html_parser='html.parser'))
    assert json.loads(result['authors_json']) == [name]
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['native_labels'] == [(role+' ' if role else '')+name]*2
    assert detail['profile_role'] == 'publisher_declared_plain_byline'
    assert detail['profile_url'] is None and detail['literal_name_kind'] == kind
    assert detail['native_credit_dates'] == ['2026-10-20']*2
    assert detail['declared_schema_author_type'] == 'Person'
    assert not detail['name_split_or_natural_person_inferred']
    assert not detail['person_identity_or_ownership_independently_verified']
    assert '0.50%' in result['body'] and result['body'].count('12345') == 1


def test_v4_new_linked_editorial_center_credit_uses_literal_profile_not_known_person(v3source,v4source):
    soup = document()
    declaration = soup.select_one('script[type="application/ld+json"]')
    value = json.loads(declaration.get_text());value['author']['name'] = '王宥筑'
    declaration.string = json.dumps(value,ensure_ascii=False)
    profile = 'https://www.managertoday.com.tw/columnist/view/5737'
    for credit in soup.select('section.article_top a'):
        credit['href'] = profile;credit['data-dl_text'] = '王宥筑'
        credit.string = '經理人用戶成長中心 王宥筑'
    with pytest.raises(NewsError):
        extract(soup,v3source)
    result = extract(soup,v4source)
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['profile_url'] == profile
    assert detail['profile_role'] == 'publisher_declared_editorial_center_credit'
    assert not detail['person_identity_or_ownership_independently_verified']


@pytest.mark.parametrize('change', ['no_native_name','foreign_credit','foreign_schema','unknown_role',
    'photo_only','conflicting_copy','nested_credit','nested_date','extra_group_text','extra_tag',
    'bad_br','missing_date','conflicting_date','third_copy','hidden_credit','hidden_date','missing_og'])
def test_v4_never_promotes_empty_or_foreign_byline_metadata_or_broken_date_pairs(v4source,change):
    soup = native_byline_document('Y Chen')
    groups = soup.select('section.article_top > div')
    date,credit = groups[0].select('span')
    if change=='no_native_name':
        for group in groups:group.select('span')[-1].string=''
    elif change=='foreign_credit':
        for group in groups:group.select('span')[-1].string='鉅亨網 Y Chen'
    elif change=='foreign_schema':
        soup=native_byline_document('鉅亨網 Y Chen')
    elif change=='unknown_role':
        for group in groups:group.select('span')[-1].string='未知角色 Y Chen'
    elif change=='photo_only':
        for group in groups:group.select('span')[-1].string='攝影 Y Chen'
    elif change=='conflicting_copy':credit.string='整理‧撰文 Y Chen'
    elif change in {'nested_credit','nested_date'}:
        node = credit if change=='nested_credit' else date
        child=soup.new_tag('b');child.string=node.get_text();node.clear();node.append(child)
    elif change=='extra_group_text':credit.insert_before('未核對署名')
    elif change=='extra_tag':credit.insert_before(soup.new_tag('div'))
    elif change=='bad_br':groups[0].select_one('br')['hidden']=''
    elif change=='missing_date':date.decompose()
    elif change=='conflicting_date':date.string='2026-10-19'
    elif change=='third_copy':soup.select_one('section.article_top').append(deepcopy(groups[0]))
    elif change=='hidden_credit':credit['hidden']=''
    elif change=='hidden_date':date['style']='display:none'
    else:soup.select_one('meta[property="og:title"]')['content']=str(soup.title.string)
    with pytest.raises(NewsError):
        extract(soup,v4source)


def plain_credit_document():
    soup = document()
    for node in soup.select('section.article_top a, section.article_top > span'):
        node.decompose()
    for br in (True, False):
        group = soup.new_tag('div')
        date = soup.new_tag('span')
        date.string = '2026-10-20'
        credit = soup.new_tag('span')
        credit.string = '採訪·撰文\n    支琬清'
        group.append(date)
        if br:
            group.append(soup.new_tag('br'))
        group.append(credit)
        soup.select_one('section.article_top').append(group)
    soup.title.string = '同篇文章的另一 SEO 標題|經理人'
    return soup


def test_v3_observed_plain_writing_credit_and_distinct_seo_title(source, v2source, v3source):
    soup = plain_credit_document()
    for old in (source, v2source):
        with pytest.raises(NewsError):
            extract(soup, old)
    result = extract(soup, v3source)
    assert result['title'] == TITLE
    assert result['body_sha256'] == extract(document(), source)['body_sha256']
    credit = json.loads(result['author_attribution_detail_json'])
    assert credit['profile_url'] is None
    assert credit['profile_role'] == 'publisher_declared_plain_writing_credit'
    assert credit['native_labels'] == ['採訪·撰文 支琬清'] * 2
    assert credit['native_credit_dates'] == ['2026-10-20'] * 2
    assert credit['person_identity_or_ownership_independently_verified'] is False
    facts = json.loads(result['verified_article_notices_json'])
    assert facts['html_title'] != facts['og_title']
    assert facts['og_title'] == TITLE + '|經理人'
    assert v3source.data_fingerprint not in {source.data_fingerprint, v2source.data_fingerprint}


@pytest.mark.parametrize('change', ['unknown_role', 'conflicting_role', 'other_name', 'other_date', 'missing_date',
    'intervening_tag', 'nested_credit', 'third_credit', 'caption_only', 'missing_credit',
    'missing_seo_brand', 'other_og', 'unpaired_plain_title', 'hidden_credit', 'hidden_date'])
def test_v3_plain_credit_cannot_promote_metadata_caption_reprint_or_unpaired_date(v3source, change):
    soup = plain_credit_document()
    groups = soup.select('section.article_top > div')
    credit = groups[0].select('span')[-1]
    date = groups[0].select_one('span')
    if change == 'unknown_role':
        credit.string = '鉅亨網 支琬清'
        groups[1].select('span')[-1].string = '鉅亨網 支琬清'
    elif change == 'conflicting_role':
        credit.string = '鉅亨網 支琬清'
    elif change == 'other_name':
        credit.string = '採訪·撰文 另一作者'
    elif change == 'other_date':
        date.string = '2026-10-19'
    elif change == 'missing_date':
        date.decompose()
    elif change == 'intervening_tag':
        credit.insert_before(soup.new_tag('div'))
    elif change == 'nested_credit':
        span = soup.new_tag('b')
        span.string = '採訪·撰文 支琬清'
        credit.clear()
        credit.append(span)
    elif change == 'third_credit':
        soup.select_one('section.article_top').append(deepcopy(groups[0]))
    elif change == 'caption_only':
        for group in groups:
            group.select('span')[-1].string = '攝影 支琬清'
    elif change == 'missing_credit':
        for group in groups:
            group.select('span')[-1].decompose()
    elif change == 'missing_seo_brand':
        soup.title.string = '其他網站'
    elif change == 'other_og':
        soup.select_one('meta[property="og:title"]')['content'] = '第三個無法核對的標題|經理人'
    elif change == 'unpaired_plain_title':
        soup.select_one('meta[property="og:title"]')['content'] = str(soup.title.string)
    elif change == 'hidden_credit':
        credit['hidden'] = ''
    else:
        date['style'] = 'display:none'
    with pytest.raises(NewsError):
        extract(soup, v3source)


def test_v3_retains_reviewed_v2_profile_title_role_and_clock_semantics(source, v3source):
    test_v2_keeps_native_title_and_unreordered_source_clock_declarations(source, v3source)
    test_v2_paired_source_writer_does_not_need_a_growing_name_whitelist(source, v3source)
    test_v2_observed_user_growth_credit_keeps_literal_source_role(v3source)


@pytest.mark.parametrize('change', ['duplicate_body','duplicate_heading','duplicate_schema',
    'missing_credit','conflicting_credit','external_frame','unknown_template','missing_template_table',
    'hidden_template_text','explicit_reprint','paywall_form','nul'])
def test_v3_preserves_v1_body_and_identity_refusals(v3source, change):
    test_no_partial_body_or_false_access_acceptance(v3source, change)


def test_v2_paired_source_writer_does_not_need_a_growing_name_whitelist(source, v2source):
    soup = document()
    data = json.loads(soup.select_one('script[type="application/ld+json"]').string)
    data['author']['name'] = '張玉琦'
    soup.select_one('script[type="application/ld+json"]').string = json.dumps(data)
    for node in soup.select('section.article_top a'):
        node['href'] = 'https://www.managertoday.com.tw/columnist/view/640'
        node['data-dl_text'] = '張玉琦'
        node.string = '撰文 張玉琦'
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup, source)
    result = extract(soup, v2source)
    assert json.loads(result['authors_json']) == ['張玉琦']
    assert 'publisher_declared_writer' in result['author_attribution_detail_json']
    assert v2source.data_fingerprint != source.data_fingerprint


def test_v2_keeps_native_title_and_unreordered_source_clock_declarations(source, v2source):
    soup = document()
    soup.title.string = '另一個 SEO 標題|經理人'
    soup.select_one('meta[property="og:title"]')['content'] = soup.title.string
    data = json.loads(soup.select_one('script[type="application/ld+json"]').string)
    data['dateModified'] = '2026-10-20T11:27:26+08:00'
    soup.select_one('script[type="application/ld+json"]').string = json.dumps(data)
    with pytest.raises(NewsError):
        extract(soup, source)
    result = extract(soup, v2source)
    assert result['title'] == TITLE
    assert result['modified_at_raw'] == data['dateModified']
    facts = json.loads(result['verified_article_notices_json'])
    assert facts['html_title'] == str(soup.title.string)
    assert facts['modified_before_published_declaration']
    assert not facts['clock_order_independently_verified']
    assert result['body_sha256'] == extract(document(), source)['body_sha256']


def test_v2_observed_user_growth_credit_keeps_literal_source_role(v2source):
    soup = document()
    for node in soup.select('section.article_top a'):
        node.string = '經理人用戶成長中心 支琬清'
    result = extract(soup, v2source)
    assert '經理人用戶成長中心 支琬清' in result['author_attribution_detail_json']


@pytest.mark.parametrize('change', ['duplicate_body','duplicate_heading','duplicate_schema',
    'missing_credit','conflicting_credit','external_frame','unknown_template','missing_template_table',
    'hidden_template_text','explicit_reprint','paywall_form','nul'])
def test_v2_preserves_v1_body_and_identity_refusals(v2source, change):
    test_no_partial_body_or_false_access_acceptance(v2source, change)


def test_v2_retry_only_reconsiders_old_missing_whitelist_failures(recipe, v2source, source):
    from downloader.news_fulltext_repairs import _head_problem
    registry = SimpleNamespace(sources={'managertoday': v2source})
    row = dict(provider='managertoday', url=URL, state='article_rights_or_scope_excluded',
        error_code='article_rights_or_scope_excluded', body_sha256=None, parquet_path=None,
        body_bytes=0, captured_at_utc=None, lease_owner=None, lease_until=None, attempts=1,
        due=time.time()-1, last_checked_at_utc='2026-10-25T04:55:00Z')
    assert _head_problem(row, time.time(), registry=registry) is None
    assert _head_problem(row, time.time(), registry=SimpleNamespace(sources={'managertoday':source}))
    for changes in ({'last_checked_at_utc':'2026-10-25T05:01:00Z'}, {'body_bytes':1},
            {'lease_owner':'other'}, {'due':4102444800},
            {'state':'http_403','error_code':'http_403'}, {'url':URL+'?other=1'}):
        assert _head_problem({**row, **changes}, time.time(), registry=registry)


@pytest.mark.parametrize('version', [2, 3, 4, 5])
def test_v2_policy_reload_preserves_failed_heads_and_uses_explicit_mailbox(recipe, tmp_path, version):
    from downloader.news_fulltext_queue import NewsQueue
    from downloader.news_fulltext_repairs import submit_repairs, consume_repairs
    v1 = deepcopy(recipe)
    v1['root'] = str(tmp_path)
    registry1 = PolicyRegistry(v1, base=Path.cwd())
    with closing(NewsQueue(tmp_path, registry1)) as queue:
        queue.add_articles([{'url': URL}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,attempts=?,last_checked_at_utc=? WHERE url=?',
                ('article_rights_or_scope_excluded','article_rights_or_scope_excluded',
                 time.time()-30,7,'2026-10-25T04:55:00Z',URL))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    v2 = deepcopy(v1)
    v2['providers'][0]['embedded_article_layout'] = f'manager_public_written_article_v{version}'
    if version == 4:
        from downloader.news_fulltext_managertoday import MANAGER_V4_VERIFICATION
        v2['providers'][0]['adapter_verification'] = MANAGER_V4_VERIFICATION
    elif version == 5:
        from downloader.news_fulltext_managertoday import MANAGER_V5_VERIFICATION
        v2['providers'][0]['adapter_verification'] = MANAGER_V5_VERIFICATION
    registry2 = PolicyRegistry(v2, base=Path.cwd())
    with closing(NewsQueue(tmp_path, registry2)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone()) == before
        request = submit_repairs(tmp_path, registry2, [URL], reason='Pinned old author-whitelist correction')
        assert request['submitted']
        # Admission is a cooperative 50ms slice; a busy filesystem may yield
        # before the first item. Drive the owner again, without widening it.
        deadline=time.monotonic()+5
        while True:
            consume_repairs(queue)
            after=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
            if after['state']=='retry' or time.monotonic()>=deadline:break
        assert after['state'] == 'retry'
        assert all(after[k] == before[k] for k in ('due','attempts','error_code','last_checked_at_utc'))


def document():
    declared = {'@type': 'NewsArticle', 'url': URL, 'mainEntityOfPage': URL,
        'headline': TITLE, 'articleBody': 'JSONLD_SUMMARY_AND_SCRIPT_POLLUTION',
        'datePublished': '2026-10-20T13:49:00+08:00',
        'dateModified': '2026-10-20T17:27:55+08:00',
        'author': {'@type': 'Person', 'name': '支琬清', 'url': URL},
        'publisher': {'@type': 'Organization', 'name': '經理人 Managertoday',
                      'url': 'https://www.managertoday.com.tw'}}
    return BeautifulSoup(f'''<html><head><title>{TITLE}|經理人</title>
      <link rel="canonical" href="{URL}"><meta property="og:url" content="{URL}">
      <meta property="og:title" content="{TITLE}|經理人">
      <meta property="article:published_time" content="2026-10-20T13:49:00+08:00">
      <script type="application/ld+json">{json.dumps(declared, ensure_ascii=False)}</script></head>
      <body><div class="main_box"><section class="article_top"><h1>{TITLE}</h1>
      <a href="{PROFILE}" data-dl_text="支琬清">撰文 支琬清</a><span>2026-10-20</span>
      <a href="{PROFILE}" data-dl_text="支琬清">撰文 支琬清</a></section>
      <section class="view_box"><div><main class="htmlview" data-cat="article">
      <p>{'原生段落，保留公開原值及來源聲明。' * 10}</p>
      <table><tr><th>費率</th><td>0.50%</td></tr></table>
      <div x-data="iframe" class="widget w-full iframe"><template x-ref="src">
      <table><tr><th>靜態原生表格</th><td>12345</td></tr></table>
      <script>INERT_TABLE_SCRIPT_NOT_PROSE()</script></template></div>
      <div class="widget textblock">本篇不提供合作夥伴轉載使用</div>
      </main><div>NEIGHBOR_STORY_NOT_BODY</div></div></section></div>
      <footer>NEIGHBOR_FOOTER_NOT_BODY</footer></body></html>''', 'lxml')


def extract(soup, source, *, url=URL):
    return extract_article(str(soup).encode(), source, page_url=url)


def test_native_body_not_jsonld_neighbor_or_inert_script(source):
    article = extract(document(), source)
    assert '原生段落' in article['body']
    assert article['body'].count('靜態原生表格') == 1
    assert '12345' in article['body'] and '0.50%' in article['body']
    assert not any(s in article['body'] for s in ('POLLUTION', 'NOT_PROSE', 'NEIGHBOR'))
    assert article['body_sha256'] == hashlib.sha256(article['body'].encode()).hexdigest()
    assert article['extraction_method'] == 'css::scope > .manager-native-body'
    assert article['published_at_raw'] == '2026-10-20T13:49:00+08:00'
    assert article['modified_at_raw'] == '2026-10-20T17:27:55+08:00'
    assert article['historical_point_in_time'] is False
    notices = json.loads(article['verified_article_notices_json'])
    assert notices['inert_table_declarations'] == 1
    assert notices['inert_tables_rendered_or_executed'] is False
    assert notices['reuse_rights_granted'] is False


def test_plain_native_article_does_not_need_inert_template(source):
    soup = document()
    soup.select_one('[x-data=iframe]').decompose()
    article = extract(soup, source)
    assert '原生段落' in article['body']
    assert json.loads(article['verified_article_notices_json'])['inert_table_declarations'] == 0


@pytest.mark.parametrize('url', [URL+'?member=1', URL+'#body', URL.replace('www.', ''),
    URL.replace('https:', 'http:'), URL.replace('/73138', '/073138'),
    URL.replace('managertoday.com.tw', 'managertoday.com.tw.attacker.test')])
def test_literal_final_url_namespace_not_alias(source, url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(document(), source, url=url)


@pytest.mark.parametrize('selector,attribute,value', [
    ('link[rel=canonical]', 'href', URL+'0'), ('meta[property="og:url"]', 'content', URL+'0'),
    ('meta[property="og:title"]', 'content', '另一篇文章|經理人'),
    ('meta[property="article:published_time"]', 'content', '2026-10-20T13:50:00+08:00'),
    ('section.article_top a', 'href', PROFILE+'0'),
    ('section.article_top a', 'data-dl_text', '另一作者'),
    ('main.htmlview', 'hidden', ''), ('main.htmlview p', 'style', 'display: none'),
    ('section.article_top', 'aria-hidden', 'true')])
def test_conflicting_identity_credit_clock_or_visibility_rejects(source, selector, attribute, value):
    soup = document()
    soup.select_one(selector)[attribute] = value
    with pytest.raises(NewsError):
        extract(soup, source)


@pytest.mark.parametrize('field,value,error', [
    ('author', {'@type':'Person','name':'未知作者'}, 'article_rights_or_scope_excluded'),
    ('publisher', {'name':'另一媒體','url':'https://example.com'}, 'article_identity_not_verified'),
    ('url', URL+'0', 'article_identity_not_verified'),
    ('headline', '另一標題', 'article_identity_not_verified'),
    ('isAccessibleForFree', False, 'paywall_or_login'),
    ('datePublished', '2026-10-20T13:49:00', 'article_identity_not_verified'),
    ('dateModified', '2026-10-19T13:49:00+08:00', 'article_identity_not_verified'),
    ('dateModified', '2026-13-20T13:49:00+08:00', 'article_identity_not_verified')])
def test_native_declarations_are_independent_not_metadata_fallback(source, field, value, error):
    soup = document()
    node = soup.select_one('script[type="application/ld+json"]')
    data = json.loads(node.string)
    data[field] = value
    node.string = json.dumps(data)
    with pytest.raises(NewsError, match=error):
        extract(soup, source)


@pytest.mark.parametrize('change', ['duplicate_body','duplicate_heading','duplicate_schema',
    'missing_credit','conflicting_credit','external_frame','unknown_template','missing_template_table',
    'hidden_template_text','explicit_reprint','paywall_form','nul'])
def test_no_partial_body_or_false_access_acceptance(source, change):
    soup = document()
    if change == 'duplicate_body':
        soup.select_one('section.view_box').append(deepcopy(soup.select_one('main.htmlview')))
    elif change == 'duplicate_heading':
        soup.body.append(deepcopy(soup.h1))
    elif change == 'duplicate_schema':
        soup.head.append(deepcopy(soup.select_one('script[type="application/ld+json"]')))
    elif change == 'missing_credit':
        for node in soup.select('section.article_top a'): node.decompose()
    elif change == 'conflicting_credit':
        soup.select_one('section.article_top a').string = '轉載 支琬清'
    elif change == 'external_frame':
        soup.select_one('main').append(soup.new_tag('iframe', src='https://example.com/'))
    elif change == 'unknown_template':
        soup.select_one('template')['x-ref'] = 'paid'
    elif change == 'missing_template_table':
        soup.select_one('template table').decompose()
    elif change == 'hidden_template_text':
        soup.select_one('template td')['style'] = 'visibility: hidden'
    elif change == 'explicit_reprint':
        soup.select_one('main p').string = '本文轉載自另一出版社'
    elif change == 'paywall_form':
        soup.body.append(soup.new_tag('form', action='/login'))
    else:
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            extract_article(str(soup).replace(TITLE, TITLE+'\0').encode(), source, page_url=URL)
        return
    with pytest.raises(NewsError):
        extract(soup, source)


@pytest.mark.parametrize('field,value', [('domains',['example.com']),('id','other'),
    ('article_selectors',['main']),('require_author_attribution',False),
    ('private_known_url_scope',''),('article_path_pattern','.*'),
    ('feeds',['https://www.managertoday.com.tw/']),('html_parser','html.parser')])
def test_native_adapter_cannot_expand_source_or_endpoint_scope(recipe, field, value):
    config = deepcopy(recipe)
    config['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(config, base=Path.cwd())


def test_canonical_recipe_uses_existing_budget_and_local_urls_only(recipe, source):
    entry = recipe['providers'][0]
    assert source.allowed and source.known_url_only
    assert source.interval_seconds == 2 and source.parallelism == 1
    assert not any(entry.get(k) for k in ('feeds','sitemaps','wordpress_apis','index_pages'))
    assert source.authorization['kind'] == 'personal_research'
