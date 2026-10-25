"""Ordered dual-credit evidence through the existing owner, without source GETs."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import time

from bs4 import BeautifulSoup
import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import PendingOriginal, private_gvm_multiple_credit_original_permit, retain_original
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_gvm import GVM_MULTIPLE_CREDIT_QUESTION, GVM_PRESENTATION_LAYOUT, gvm_multiple_credit_relations
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from test_news_fulltext import config
from test_news_gvm_header_evidence import header_owner, submit as submit_header
from test_news_gvm_media import media_page
from test_news_gvm_native import ORIGIN, URL, schema
from test_news_gvm_originals import head, http_stub, owner, response
from test_news_gvm_presentation import presentation_entry


def multiple_page():
    soup=media_page(''); head_node=soup.select_one('.article-head')
    first=soup.select_one('.article-head_author'); second=deepcopy(first)
    second.string='合成第二作者';second['href']=ORIGIN+'/author/777'
    first.parent.append(second)
    head_node['author']=first.get_text(strip=True)+'、'+second.get_text(strip=True)
    head_node['author_id']='6813,777'
    schema(soup,lambda a:a['author'].append({'@type':'Person','name':second.get_text(strip=True),'url':second['href']}))
    return BeautifulSoup(str(soup),'lxml')


def multiple_source():
    return PolicyRegistry({'contract_version':1,'collection_purpose':'personal_research','publish':False,
        'remote_training_export':False,'providers':[presentation_entry()]},base='.').sources['gvm']


def submit(queue,settings,reason='Ordered visible schema and native aggregate relations for two credits'):
    return diagnostic.submit_candidate(queue.root,settings,queue.registry,URL,GVM_PRESENTATION_LAYOUT,
        reason=reason,evidence_contract_version=42)


@pytest.fixture
def multiple_owner(header_owner,monkeypatch):
    queue,settings,_,_=header_owner; soup=multiple_page()
    prior=submit_header(queue,settings);diagnostic.pump_candidates(queue)
    http=http_stub(response(soup));job=diagnostic.claim_candidate(queue,http,settings,'gvm')
    result=diagnostic.fetch_candidate(http,job)
    assert not isinstance(result,PendingOriginal) and result['gvm_header_relations']['schema_author_count']==2
    diagnostic.finish_candidate(queue,job,result=result)
    old=queue.registry.sources['gvm']
    monkeypatch.setattr('downloader.news_fulltext_gvm.GVM_V9_ORIGINAL_BINDING',(old.data_fingerprint,old.authorization_fingerprint))
    settings['providers']=[presentation_entry()];queue.registry=PolicyRegistry(settings,base=queue.root.parent)
    assert queue.registry.sources['gvm'].parser_repair_prior_capture_fingerprint==old.data_fingerprint
    return queue,settings,soup,prior


def test_two_profiles_and_schema_order_are_private_evidence_not_body_acceptance():
    soup=multiple_page();before=str(soup);facts=gvm_multiple_credit_relations(soup,page_url=URL)
    assert facts['ordered_visible_profile_schema_clocks_verified'] and facts['schema_profiles_match_visible_order']
    assert facts['native_aggregate_name_relation']['separator_codepoints']==[ord('、')]
    assert facts['native_aggregate_ID_relation']['separator_codepoints']==[ord(',')]
    assert '合成第二作者' not in json.dumps(facts,ensure_ascii=False) and str(soup)==before
    result=candidate_response_metadata(response(soup),multiple_source(),multiple_source(),evidence_contract_version=42,requested_url=URL)
    permit=result['private_original_permit']
    assert result['question']==GVM_MULTIPLE_CREDIT_QUESTION and not result['accepted_as_article']
    assert not permit['native_aggregate_author_attributes_accepted'] and not permit['body_completeness_verified']
    assert result['current_native_error']=='article_identity_not_verified' and not result['raw_or_names_or_body_retained']


def test_aggregate_unknowns_are_recorded_without_inventing_attribution():
    soup=multiple_page();soup.select_one('.article-head')['author_id']='unverified aggregate'
    facts=gvm_multiple_credit_relations(soup,page_url=URL)
    assert facts['ordered_visible_profile_schema_clocks_verified']
    assert not facts['native_aggregate_ID_relation']['ordered_pair']
    assert not private_gvm_multiple_credit_original_permit(response(soup),multiple_source(),URL)['native_aggregate_author_attributes_accepted']


@pytest.mark.parametrize('change',['missing','third','hidden','hidden_parent','profile','duplicate_profile','duplicate_name',
    'schema_order','schema_name','clock','nonfree','canonical','publisher','reprint','external','advertorial'])
def test_unverified_or_denied_pair_cannot_retain_private_raw(change):
    soup=multiple_page();credits=soup.select('.article-head_author')
    if change=='missing':credits[1].decompose()
    elif change=='third':credits[1].parent.append(deepcopy(credits[1]))
    elif change=='hidden':credits[1]['hidden']=''
    elif change=='hidden_parent':credits[1].parent['style']='display:none'
    elif change=='profile':credits[1]['href']='https://example.invalid/author/777'
    elif change=='duplicate_profile':credits[1]['href']=credits[0]['href']
    elif change=='duplicate_name':credits[1].string=credits[0].get_text(strip=True)
    elif change=='schema_order':schema(soup,lambda a:a['author'].reverse())
    elif change=='schema_name':schema(soup,lambda a:a['author'][1].update(name='PRIVATE_SENTINEL'))
    elif change=='clock':soup.select_one('meta[property="article:published_time"]')['content']='invalid'
    elif change=='nonfree':schema(soup,lambda a:a.update(isAccessibleForFree=False))
    elif change=='canonical':soup.select_one('link[rel=canonical]')['href']+='9'
    elif change=='publisher':schema(soup,lambda a:a.update(publisher={'@id':'https://example.invalid/'}))
    elif change=='reprint':soup.select_one('.article-content > p').string='本文轉載自未核對來源'
    elif change=='external':soup.select_one('.article-head')['author']='中央社'
    else:soup.select_one('.article-head')['article_type']='廣編'
    with pytest.raises(NewsError):private_gvm_multiple_credit_original_permit(response(soup),multiple_source(),URL)
    facts=candidate_response_metadata(response(soup),multiple_source(),multiple_source(),evidence_contract_version=42,requested_url=URL)
    assert 'private_original_permit' not in facts and not facts['raw_or_names_or_body_retained']
    assert 'PRIVATE_SENTINEL' not in json.dumps(facts,ensure_ascii=False)


@pytest.mark.parametrize('change',[{'status':403},{'url':URL+'9'},{'body':b'x'*(2**21+1)},
    {'headers':{'content-type':'application/json'}}])
def test_private_transport_envelope_is_literal_and_bounded(change):
    with pytest.raises(NewsError):private_gvm_multiple_credit_original_permit(response(multiple_page(),**change),multiple_source(),URL)


@pytest.mark.parametrize('version',[1,34,40,41,42,True])
def test_question_is_bound_to_current_exact_source(version):
    source=multiple_source()
    assert candidate_evidence_allowed(source,source,version)==(type(version) is int and version==42)


def test_owner_reserves_once_then_writer_independently_revalidates_private_original(multiple_owner,monkeypatch):
    queue,settings,soup,prior=multiple_owner;before=head(queue)
    old_jobs=[tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('no second owner'))
    requested=submit(queue,settings);assert submit(queue,settings,'A different reason')['request_id']==requested['request_id']
    diagnostic.pump_candidates(queue);http=http_stub(response(soup))
    job=diagnostic.claim_candidate(queue,http,settings,'gvm');assert job and not http.calls
    result=diagnostic.fetch_candidate(http,job);assert isinstance(result,PendingOriginal) and http.calls==[URL]
    with pytest.raises(ValueError,match='private_original_permit_mismatch'):
        retain_original(queue,job,replace(result,response=replace(result.response,body=result.response.body+b' ')))
    diagnostic.finish_candidate(queue,job,result=result)
    status=diagnostic.candidate_status(queue.root,requested['request_id']);ref=status['result_json']['private_original']
    path=queue.root/ref['path'];raw,=pq.ParquetFile(path).read(use_threads=False).to_pylist()
    assert raw['raw_response_body']==result.response.body and hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256']
    assert path.stat().st_mode&0o077==0 and not raw['accepted_as_article'] and not raw['remote_training_export']
    assert head(queue)==before and [tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id<>?',(requested['request_id'],))]==old_jobs
    assert not submit(queue,settings)['submitted'] and diagnostic.claim_candidate(queue,http,settings,'gvm') is None


@pytest.mark.parametrize('change',['unknown','retained','head','request_digest','future','count','access'])
def test_old_closed_refusal_must_still_match_at_submission(multiple_owner,change):
    queue,settings,_,prior=multiple_owner
    row=queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(prior['request_id'],)).fetchone()
    with queue.db:
        if change=='unknown':queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(prior['request_id'],))
        elif change=='head':queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?',(URL,))
        elif change=='request_digest':queue.db.execute("UPDATE native_candidate_jobs SET request_json=replace(request_json,'Missing native','Changed native') WHERE request_id=?",(prior['request_id'],))
        elif change=='future':queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?",(prior['request_id'],))
        else:
            result=json.loads(row['result_json'])
            if change=='retained':result['private_original']={}
            elif change=='count':result['gvm_header_relations']['schema_author_count']=1
            else:result['gvm_header_relations']['schema_public_access']=False
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?',(json.dumps(result),prior['request_id']))
    with pytest.raises(ValueError,match='completed_gvm_two_credit_header_refusal_required'):submit(queue,settings)


@pytest.mark.parametrize('blocked',['deadline','gateway','robots','prior_changed','unknown'])
def test_owner_rechecks_deadlines_and_prior_before_network(multiple_owner,blocked):
    queue,settings,soup,prior=multiple_owner;requested=submit(queue,settings);diagnostic.pump_candidates(queue)
    http=http_stub(response(soup))
    if blocked=='deadline':http.provider_not_before['gvm']=time.time()+3600
    elif blocked=='gateway':http.blocked_gateway_hosts=lambda _:('www.gvm.com.tw',)
    elif blocked=='robots':http.blocked_robots_origins=lambda _:('https://www.gvm.com.tw',)
    elif blocked=='prior_changed':
        with queue.db:queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(prior['request_id'],))
    else:
        with queue.db:queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(requested['request_id'],))
    assert diagnostic.claim_candidate(queue,http,settings,'gvm') is None and not http.calls


def test_cli_is_resident_only(config,tmp_path,monkeypatch):
    from scripts import probe_news_sources as cli
    settings=deepcopy(config);settings.update(root='news',providers=[presentation_entry()])
    registry=PolicyRegistry(settings,base=tmp_path)
    monkeypatch.setattr(cli,'load_config',lambda _:(settings,registry,tmp_path))
    monkeypatch.setattr(cli,'require_current_missing_body_failures',lambda *a,**k:{})
    calls=[]
    monkeypatch.setattr(diagnostic,'submit_candidate',lambda *a,**k:calls.append(k) or {'submitted':True,'request_id':'a'*64})
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('no CLI GET'))
    args=['--probe',URL,'--candidate-evidence-contract','42','--candidate-embedded-layout',GVM_PRESENTATION_LAYOUT,
        '--require-current-missing-body-failure','--failure-code','article_identity_not_verified','--output',str(tmp_path/'result.json')]
    with pytest.raises(SystemExit):cli.main(args)
    assert not calls and cli.main([*args,'--enqueue-candidate'])==0 and calls[0]['evidence_contract_version']==42
