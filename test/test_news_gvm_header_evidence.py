"""Only unfinished native header questions; synthetic HTML, no source access."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import time

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import PendingOriginal, private_gvm_header_original_permit, retain_original
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_gvm import GVM_HEADER_QUESTION, GVM_STATIC_EDITOR_LAYOUT, gvm_header_relations
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from test_news_fulltext import config
from test_news_gvm_magazine import magazine_page
from test_news_gvm_media import media_page
from test_news_gvm_native import TEXT, URL, schema
from test_news_gvm_originals import head, http_stub, original_source, owner, response, submit as submit_original
from test_news_gvm_static_editor import static_entry


def header_page(kind='credit'):
    soup = magazine_page() if kind=='issue' else media_page('')
    if kind=='credit':
        soup.select_one('.article-head_box').append(soup.select_one('.article-head_author').extract())
    elif kind=='title':
        soup.select_one('.article-head')['title'] = '不同的來源標題宣告'
    elif kind=='issue':
        schema(soup, lambda a: a['isPartOf'].update(name='不同的期刊標題宣告'))
    else:
        raise AssertionError(kind)
    return soup


def current_source():
    return PolicyRegistry({'contract_version':1,'collection_purpose':'personal_research','publish':False,'remote_training_export':False,
        'providers':[static_entry()]},base='.').sources['gvm']


def submit(queue, settings, reason='Missing native title credit and issue relations'):
    return diagnostic.submit_candidate(queue.root,settings,queue.registry,URL,GVM_STATIC_EDITOR_LAYOUT,
        reason=reason,evidence_contract_version=41)


@pytest.fixture
def header_owner(owner, monkeypatch, request):
    queue, settings = owner
    soup = header_page(getattr(request,'param','credit'))
    old = queue.registry.sources['gvm']
    prior = submit_original(queue,settings)
    diagnostic.pump_candidates(queue)
    job = diagnostic.claim_candidate(queue,http_stub(response(soup)),settings,'gvm')
    observation = diagnostic.fetch_candidate(http_stub(response(soup)),job)
    assert not isinstance(observation,PendingOriginal) and not observation['raw_or_names_or_body_retained']
    diagnostic.finish_candidate(queue,job,result=observation)
    monkeypatch.setattr('downloader.news_fulltext_gvm.GVM_V8_ORIGINAL_BINDING',
        (old.data_fingerprint,old.authorization_fingerprint))
    settings['providers'] = [static_entry()]
    queue.registry = PolicyRegistry(settings,base=queue.root.parent)
    assert queue.registry.sources['gvm'].parser_repair_prior_capture_fingerprint==old.data_fingerprint
    return queue,settings,soup,prior


@pytest.mark.parametrize('kind',['credit','title','issue'])
def test_relation_witness_never_accepts_a_conflict_or_exports_names(kind):
    soup = header_page(kind)
    source = current_source()
    facts = candidate_response_metadata(response(soup),source,source,evidence_contract_version=41,requested_url=URL)
    assert facts['question']==GVM_HEADER_QUESTION
    assert not facts['accepted_as_article'] and not facts['identity_or_alias_accepted']
    assert facts['gvm_header_relations']['private_header_profile_schema_clocks_verified']
    assert not facts['private_original_permit']['article_identity_or_title_or_issue_conflict_accepted']
    encoded = json.dumps(facts,ensure_ascii=False)
    assert all(s not in encoded for s in (TEXT,'合成測試標題','不同的來源標題宣告','不同的期刊標題宣告','PRIVATE_','盧佳柔'))
    if kind=='credit':
        assert facts['gvm_header_relations']['prior_credit_selector_count']==0
        assert facts['gvm_header_relations']['native_credit_anchor_count']==1
    elif kind=='title':
        assert facts['gvm_header_relations']['title_relations']['visible_matches_header'] is False
    else:
        assert facts['gvm_header_relations']['periodical_issue_relations']['name_matches_link_title'] is False


@pytest.mark.parametrize('change',['missing_credit','ambiguous_credit','schema_credit','profile','clock','nonfree',
    'hidden_header','hidden_credit','canonical','publisher','reprint','external_credit'])
def test_missing_unverified_or_denied_evidence_does_not_retain_raw(change):
    soup = header_page()
    if change=='missing_credit': soup.select_one('.article-head_author').decompose()
    elif change=='ambiguous_credit': soup.select_one('.article-head_box').append(deepcopy(soup.select_one('.article-head_author')))
    elif change=='schema_credit': schema(soup,lambda a:a['author'][0].update(name='UNVERIFIED_PRIVATE_SENTINEL'))
    elif change=='profile': soup.select_one('.article-head_author')['href'] += '9'
    elif change=='clock': soup.select_one('meta[property="article:published_time"]')['content']='invalid'
    elif change=='nonfree': schema(soup,lambda a:a.update(isAccessibleForFree=False))
    elif change=='hidden_header': soup.select_one('.article-head')['hidden']=''
    elif change=='hidden_credit': soup.select_one('.article-head_author')['hidden']=''
    elif change=='canonical': soup.select_one('link[rel=canonical]')['href'] += '9'
    elif change=='publisher': schema(soup,lambda a:a.update(publisher={'@id':'https://example.invalid/'}))
    elif change=='reprint': soup.select_one('.article-content > p').string='本文轉載自未核對來源'
    else: soup.select_one('.article-head')['author']='中央社'
    source = current_source()
    with pytest.raises(NewsError): private_gvm_header_original_permit(response(soup),source,URL)
    facts = candidate_response_metadata(response(soup),source,source,evidence_contract_version=41,requested_url=URL)
    assert 'private_original_permit' not in facts and not facts['raw_or_names_or_body_retained']
    assert 'PRIVATE_SENTINEL' not in json.dumps(facts,ensure_ascii=False)


@pytest.mark.parametrize('version',[1,34,35,36,37,40,41,True])
def test_exact_source_and_question_pair(version):
    source = current_source()
    assert candidate_evidence_allowed(source,source,version)==(type(version) is int and version==41)
    assert not candidate_evidence_allowed(original_source(),original_source(),41)
    assert not candidate_evidence_allowed(source,original_source(),41)


@pytest.mark.parametrize('header_owner',['credit','title','issue'],indirect=True)
def test_once_only_owner_reservation_exact_raw_readback_and_no_article_writes(header_owner,monkeypatch):
    queue,settings,soup,prior = header_owner
    before = head(queue)
    prior_row = tuple(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(prior['request_id'],)).fetchone())
    budgets = [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')]
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('no second HTTP owner'))
    request = submit(queue,settings)
    assert submit(queue,settings,reason='Other words')['request_id']==request['request_id']
    diagnostic.pump_candidates(queue)
    http = http_stub(response(soup))
    job = diagnostic.claim_candidate(queue,http,settings,'gvm')
    assert job and not http.calls
    assert queue.db.execute('SELECT state FROM native_candidate_jobs WHERE request_id=?',(request['request_id'],)).fetchone()[0]=='reserved'
    result = diagnostic.fetch_candidate(http,job)
    assert isinstance(result,PendingOriginal) and http.calls==[URL]
    diagnostic.finish_candidate(queue,job,result=result)
    status = diagnostic.candidate_status(queue.root,request['request_id'])
    reference = status['result_json']['private_original']
    path = queue.root/reference['path']
    row, = pq.ParquetFile(path).read(use_threads=False).to_pylist()
    assert row['raw_response_body']==result.response.body
    assert hashlib.sha256(path.read_bytes()).hexdigest()==reference['sha256']
    receipt=json.loads((queue.root/reference['receipt_path']).read_bytes())
    assert receipt['permit']==result.permit and path.stat().st_mode & 0o077==0
    assert not row['accepted_as_article'] and not row['remote_training_export']
    assert not submit(queue,settings)['submitted'] and diagnostic.claim_candidate(queue,http,settings,'gvm') is None
    assert head(queue)==before and [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')]==budgets
    assert tuple(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(prior['request_id'],)).fetchone())==prior_row


def test_absent_closed_refusal_cannot_start_another_question(owner,monkeypatch):
    queue,settings = owner
    old = queue.registry.sources['gvm']
    settings['providers']=[static_entry()]; queue.registry=PolicyRegistry(settings,base=queue.root.parent)
    monkeypatch.setattr('downloader.news_fulltext_gvm.GVM_V8_ORIGINAL_BINDING',(old.data_fingerprint,old.authorization_fingerprint))
    with pytest.raises(ValueError,match='completed_gvm_missing_header_original_refusal_required'): submit(queue,settings)


@pytest.mark.parametrize('change',['unknown','retained','head_changed','transport','future','access','request_digest'])
def test_invalid_or_no_longer_matching_prior_is_not_an_eligibility_grant(header_owner,change):
    queue,settings,_,prior = header_owner
    row = queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(prior['request_id'],)).fetchone()
    with queue.db:
        if change=='unknown': queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(prior['request_id'],))
        elif change=='head_changed': queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?',(URL,))
        elif change=='future': queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?",(prior['request_id'],))
        elif change=='request_digest': queue.db.execute("UPDATE native_candidate_jobs SET request_json=replace(request_json,'offline','changed') WHERE request_id=?",(prior['request_id'],))
        else:
            result=json.loads(row['result_json'])
            if change=='retained': result['private_original']={}
            elif change=='transport': result['status']=403
            else: result['gvm_received_failure_context']['native_accessible']=False
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?',(json.dumps(result),prior['request_id']))
    with pytest.raises(ValueError,match='completed_gvm_missing_header_original_refusal_required'): submit(queue,settings)


@pytest.mark.parametrize('blocked',['deadline','gateway','robots','unknown','changed_prior'])
def test_claim_rechecks_prior_deadlines_and_unknown_outcome(header_owner,blocked):
    queue,settings,soup,prior = header_owner
    request=submit(queue,settings); diagnostic.pump_candidates(queue)
    http=http_stub(response(soup))
    if blocked=='deadline': http.provider_not_before['gvm']=time.time()+3600
    elif blocked=='gateway': http.blocked_gateway_hosts=lambda _:('www.gvm.com.tw',)
    elif blocked=='robots': http.blocked_robots_origins=lambda _:('https://www.gvm.com.tw',)
    elif blocked=='unknown':
        with queue.db: queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(request['request_id'],))
    else:
        with queue.db: queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(prior['request_id'],))
    assert diagnostic.claim_candidate(queue,http,settings,'gvm') is None and not http.calls


def test_stable_question_key_does_not_rearm_after_parser_or_counter_changes(header_owner):
    queue,_,_,_=header_owner
    target=diagnostic._head(head(queue))
    key=diagnostic._key(target,'a'*64,evidence_contract_version=41)
    assert diagnostic._key({**target,'attempts':99},'b'*64,evidence_contract_version=41)==key
    assert diagnostic._key(target,'a'*64,evidence_contract_version=40)!=key


@pytest.mark.parametrize('change',[{'status':403},{'status':429},{'url':URL+'?other=1'},
    {'redirects':(object(),)},{'headers':{'content-type':'application/json'}},
    {'body':b'x'*(2**21+1)}])
def test_transport_refusal_cannot_become_a_private_original(change):
    with pytest.raises(NewsError):
        private_gvm_header_original_permit(response(header_page(),**change),current_source(),URL)


def test_writer_independently_rechecks_bytes_and_permit(header_owner):
    queue,settings,soup,_=header_owner
    submit(queue,settings); diagnostic.pump_candidates(queue)
    http=http_stub(response(soup)); job=diagnostic.claim_candidate(queue,http,settings,'gvm')
    observation=diagnostic.fetch_candidate(http,job)
    assert isinstance(observation,PendingOriginal)
    changed=replace(observation,response=replace(observation.response,body=observation.response.body+b' '))
    with pytest.raises(ValueError,match='private_original_permit_mismatch'): retain_original(queue,job,changed)
    assert not (queue.root/'reviewed_repairs/candidate_diagnostics/originals'/f"{job['request_id']}.parquet").exists()


def test_header_observation_is_bounded_metadata_not_an_article_fallback():
    soup=header_page()
    before=str(soup)
    facts=gvm_header_relations(soup,page_url=URL)
    assert len(json.dumps(facts))<8192 and facts['current_native_error']=='article_identity_not_verified'
    assert str(soup)==before


def test_cli_uses_only_resident_submit_and_rejects_nonresident_header_diagnosis(config,tmp_path,monkeypatch):
    from scripts import probe_news_sources as cli
    settings=deepcopy(config); settings.update(root='news',providers=[static_entry()])
    registry=PolicyRegistry(settings,base=tmp_path)
    monkeypatch.setattr(cli,'load_config',lambda _:(settings,registry,tmp_path))
    monkeypatch.setattr(cli,'require_current_missing_body_failures',lambda *a,**k:{})
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('no CLI HTTP owner'))
    calls=[]
    def submitter(*a,**k): calls.append((a,k)); return {'submitted':True,'request_id':'a'*64}
    monkeypatch.setattr(diagnostic,'submit_candidate',submitter)
    args=['--probe',URL,'--candidate-evidence-contract','41','--candidate-embedded-layout',GVM_STATIC_EDITOR_LAYOUT,
        '--require-current-missing-body-failure','--failure-code','article_identity_not_verified','--output',str(tmp_path/'result.json')]
    with pytest.raises(SystemExit) as failure: cli.main(args)
    assert failure.value.code==2 and not calls
    assert cli.main([*args,'--enqueue-candidate'])==0 and len(calls)==1
    assert calls[0][1]['evidence_contract_version']==41
