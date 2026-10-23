"""One legacy witness, actual redirects/declarations, never alias admission."""
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext_candidate_originals import (
    LEGACY_QUESTION, PendingOriginal, legacy_transport_verified, private_legacy_original_permit,
)
from downloader.news_fulltext_candidates import candidate_response_metadata
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_pocket_original import settings, versioned, page, URL, OLD


LAYOUT = 'pocket_visible_article_v8'


@pytest.fixture
def config(settings, tmp_path):
    value = versioned(settings, 8)
    value.update(root=str(tmp_path), min_free_disk_bytes=1, workers=2, exclusive_http_owner=False,
                 adaptive_rate={'enabled':False})
    return value


@pytest.fixture
def queue(config, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(config,base=tmp_path))
    value.add_articles([{'url':OLD},{'url':URL}])
    with value.db:
        value.db.execute("UPDATE articles SET state='article_identity_not_verified',"
            "error_code='article_identity_not_verified',attempts=2,due=0,"
            "last_checked_at_utc='2026-01-01T00:00:00+00:00'")
    yield value
    value.close()


def heads(queue):
    return [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')]


def response(*, redirected=False, broken=True):
    body = page()
    if broken:
        body = body.replace('第2段','<unreviewed>第2段</unreviewed>')
    hops = (RedirectHop(OLD,301,URL,URL,'2026-01-01T00:00:00Z','a'*64,10),) if redirected else ()
    return Response(URL if redirected else OLD,200,{'content-type':'text/html'},body.encode(),hops)


def client(queue, received):
    calls=[]
    def get(url):
        calls.append(url)
        return received
    return SimpleNamespace(registry=queue.registry,get=get,calls=calls,provider_not_before={},
        blocked_gateway_hosts=lambda p:(),blocked_robots_origins=lambda p:())


def submit(queue,config,**changes):
    args={'reason':'One literal legacy route only; no alias/body/clock admission',
          'evidence_contract_version':5}
    args.update(changes)
    return diagnostic.submit_candidate(queue.root,config,queue.registry,OLD,LAYOUT,**args)


def claim(queue,config,http):
    request=submit(queue,config)
    with patch.object(diagnostic.time,'monotonic',return_value=0.0):
        diagnostic.pump_candidates(queue)
    job=diagnostic.claim_candidate(queue,http,config,'pocket')
    assert job['request_id']==request['request_id']
    return request,job


@pytest.mark.parametrize('redirected',[False,True])
def test_exact_private_legacy_original_preserves_actual_URL_chain_and_declared_document(queue,config,redirected):
    before=heads(queue);received=response(redirected=redirected);http=client(queue,received)
    request,job=claim(queue,config,http)
    pending=diagnostic.fetch_candidate(http,job)
    assert isinstance(pending,PendingOriginal)
    diagnostic.finish_candidate(queue,job,result=pending)
    with patch.object(diagnostic.time,'monotonic',return_value=0.0):
        diagnostic.pump_candidates(queue)
    state=diagnostic.candidate_status(queue.root,request['request_id'])
    assert state['state']=='observed'and state['raw_retention_state']=='retained_verified'
    assert state['raw_or_names_or_body_retained']and not state['accepted_as_article']
    facts=state['result_json'];reference=facts['private_original'];path=queue.root/reference['path']
    record=pq.ParquetFile(path).read().to_pylist()[0]
    assert record['request_url']==OLD and record['final_url']==received.url and record['source_document_url']==URL
    assert record['raw_response_body']==received.body
    assert len(json.loads(record['transport_redirects_json']))==int(redirected)
    assert facts['question']==LEGACY_QUESTION and facts['legacy_transport']['same_ID_transport_verified']
    assert facts['legacy_transport']['actual_redirects']==json.loads(record['transport_redirects_json'])
    receipt=json.loads((queue.root/reference['receipt_path']).read_bytes())
    assert receipt['question']==LEGACY_QUESTION and receipt['permit']==pending.permit
    assert hashlib.sha256(record['raw_response_body']).hexdigest()==record['response_sha256']
    assert hashlib.sha256(path.read_bytes()).hexdigest()==reference['sha256']
    assert not path.stat().st_mode&0o077 and not path.parent.stat().st_mode&0o077
    assert all(not record[k]for k in ('accepted_as_article','publish','historical_point_in_time',
        'per_article_license_verified','training_injection_enabled','remote_training_export'))
    assert heads(queue)==before and http.calls==[OLD]and not(queue.root/'bodies').exists()
    assert not submit(queue,config)['submitted']
    archive=queue.root/'reviewed_repairs/candidate_diagnostics/processed'/(job['request_id']+'.outcome.json')
    assert json.loads(archive.read_bytes())['raw_or_names_or_body_retained']


def test_valid_current_canonical_parser_never_retains_an_extra_original(queue,config):
    http=client(queue,response(redirected=True,broken=False));before=heads(queue)
    request,job=claim(queue,config,http);result=diagnostic.fetch_candidate(http,job)
    assert isinstance(result,dict)and result['current_error']is None
    diagnostic.finish_candidate(queue,job,result=result)
    assert not diagnostic.candidate_status(queue.root,request['request_id'])['raw_or_names_or_body_retained']
    assert heads(queue)==before and http.calls==[OLD]
    assert not(queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()


@pytest.mark.parametrize('kind',['other_ID','home','HTTP404','paywall','external_credit','bad_canonical'])
def test_refused_or_different_original_is_metadata_only_not_a_body_or_alias(queue,config,kind):
    received=response(redirected=True)
    if kind=='other_ID':
        other=URL.replace('12345','99999')
        received=replace(received,url=other,body=received.body.replace(URL.encode(),other.encode()),
            redirects=(replace(received.redirects[0],to_url=other,location=other),))
    elif kind=='home':
        home='https://www.pocket.tw/'
        received=replace(received,url=home,body=b'<html><body>home</body></html>',
            redirects=(replace(received.redirects[0],to_url=home,location=home),))
    elif kind=='HTTP404':received=replace(received,status=404)
    elif kind=='paywall':received=replace(received,body=received.body.replace(b'"isAccessibleForFree": true',b'"isAccessibleForFree": false'))
    elif kind=='external_credit':received=replace(received,body=received.body.replace('口袋學堂編輯團隊'.encode(),'外部供稿'.encode()))
    else:received=replace(received,body=received.body.replace(URL.encode(),URL.replace('12345','99999').encode()))
    before=heads(queue);http=client(queue,received);request,job=claim(queue,config,http)
    result=diagnostic.fetch_candidate(http,job)
    assert isinstance(result,dict)and not result['raw_or_names_or_body_retained']
    if kind=='other_ID':
        assert result['legacy_transport']['requested_article_id']=='12345'
        assert result['legacy_transport']['final_article_id']=='99999'
        assert not result['legacy_transport']['same_ID_transport_verified']
    diagnostic.finish_candidate(queue,job,result=result)
    assert diagnostic.candidate_status(queue.root,request['request_id'])['state']=='observed'
    assert heads(queue)==before and http.calls==[OLD]
    assert not(queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()


@pytest.mark.parametrize('kind',['missing','broken_chain','wrong_status','location','other_ID','query',
    'host','hash','negative_bytes','naive_clock','invalid_clock','nonhop'])
def test_legacy_transport_does_not_invent_or_repair_a_redirect(kind):
    value=response(redirected=True);hop=value.redirects[0]
    if kind=='missing':value=replace(value,redirects=())
    elif kind=='broken_chain':value=replace(value,redirects=(replace(hop,from_url=URL),))
    elif kind=='wrong_status':value=replace(value,redirects=(replace(hop,status=200),))
    elif kind=='location':value=replace(value,redirects=(replace(hop,location='/other/'),))
    elif kind=='other_ID':value=replace(value,redirects=(replace(hop,to_url=URL.replace('12345','99999')),))
    elif kind=='query':value=replace(value,url=URL+'?x=1')
    elif kind=='host':value=replace(value,url=URL.replace('www.pocket.tw','example.test'))
    elif kind=='hash':value=replace(value,redirects=(replace(hop,response_sha256='z'*64),))
    elif kind=='negative_bytes':value=replace(value,redirects=(replace(hop,response_bytes=-1),))
    elif kind=='naive_clock':value=replace(value,redirects=(replace(hop,observed_at_utc='2026-01-01'),))
    elif kind=='invalid_clock':value=replace(value,redirects=(replace(hop,observed_at_utc='bad'),))
    else:value=replace(value,redirects=('not a source hop',))
    assert not legacy_transport_verified(value,OLD)


@pytest.mark.parametrize('change',['canonical','query','host','http','slug'])
def test_new_question_rejects_canonical_or_unreviewed_requests_before_mailbox(queue,config,change):
    url={'canonical':URL,'query':OLD+'?x=1','host':OLD.replace('www.pocket.tw','example.test'),
         'http':OLD.replace('https:','http:'),'slug':OLD.replace('12345','article-slug')}[change]
    with pytest.raises(ValueError,match='literal_current_pocket_legacy_route_required'):
        diagnostic.submit_candidate(queue.root,config,queue.registry,url,LAYOUT,reason='No scope expansion',evidence_contract_version=5)
    assert not(queue.root/'reviewed_repairs').exists()


def test_canonical_v4_route_guard_and_keys_are_not_rearmed_by_new_question(queue,config):
    with pytest.raises(ValueError,match='literal_current_pocket_canonical_route_required'):
        submit(queue,config,evidence_contract_version=4)
    target={'provider':'pocket','id':'a'*64}
    assert diagnostic._key(target,'b'*64,evidence_contract_version=4)==diagnostic._key(target,'c'*64,evidence_contract_version=4)
    assert diagnostic._key(target,'b'*64,evidence_contract_version=5)==diagnostic._key(target,'c'*64,evidence_contract_version=5)
    assert diagnostic._key(target,'b'*64,evidence_contract_version=4)!=diagnostic._key(target,'b'*64,evidence_contract_version=5)


def test_legacy_question_requires_literal_target_even_for_direct_metadata_calls(queue):
    source=queue.registry.sources['pocket']
    with pytest.raises(ValueError,match='literal_current_pocket_legacy_route_required'):
        candidate_response_metadata(response(),source,source,evidence_contract_version=5,requested_url=URL)
    with pytest.raises(NewsError,match='candidate_response_exceeds_bound'):
        candidate_response_metadata(replace(response(),body=b' '*((2<<20)+1)),source,source,
                                    evidence_contract_version=5,requested_url=OLD)


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_independent_legacy_scope_guard_agrees_on_original_document_and_actual_URL(queue,engine):
    source=replace(queue.registry.sources['pocket'],html_parser=engine)
    received=response()
    permit=private_legacy_original_permit(received,source,OLD)
    assert permit['request_url']==permit['url']==OLD and permit['document_url']==URL
    assert permit['question']==LEGACY_QUESTION


@pytest.mark.parametrize('change',['canonical_target','canonical_contract','old_layout'])
def test_rehashed_legacy_request_cannot_downgrade_or_expand_scope(queue,config,change):
    submitted=submit(queue,config)
    directory=queue.root/'reviewed_repairs/candidate_diagnostics/pending'
    request=json.loads((directory/(submitted['request_id']+'.json')).read_bytes())
    if change=='canonical_target':
        request['target']['url']=URL
        request['target']['id']=hashlib.sha256(URL.encode()).hexdigest()
    elif change=='canonical_contract':
        request.update(contract_version=4,evidence_contract_version=4)
    else:request['candidate_layout']='pocket_visible_article_v3'
    request['observation_key']=diagnostic._key(request['target'],request['candidate_fingerprint'],
        evidence_contract_version=request['evidence_contract_version'])
    payload=diagnostic._bytes(request);path=directory/(hashlib.sha256(payload).hexdigest()+'.json')
    atomic_write_bytes(path,payload,mode=0o600)
    with pytest.raises(ValueError):diagnostic._load(path)


def test_unknown_legacy_outcome_is_not_replayed_after_restart(queue,config):
    http=client(queue,response());request,job=claim(queue,config,http);queue.close()
    restarted=NewsQueue(queue.root,queue.registry)
    try:
        with patch.object(diagnostic.time,'monotonic',return_value=0.0):
            diagnostic.pump_candidates(restarted)
        state=diagnostic.candidate_status(restarted.root,request['request_id'])
        assert state['state']=='uncertain_outcome'and state['raw_retention_state']=='unknown'
        assert state['raw_or_names_or_body_retained']is None
        assert diagnostic.claim_candidate(restarted,http,config,'pocket')is None
        assert not submit(restarted,config)['submitted']and http.calls==[]
    finally:restarted.close()


def test_failed_legacy_sidecar_write_stays_unknown_without_killing_owner(queue,config,monkeypatch):
    http=client(queue,response());request,job=claim(queue,config,http);pending=diagnostic.fetch_candidate(http,job)
    monkeypatch.setattr(diagnostic,'retain_original',lambda *a:(_ for _ in ()).throw(OSError('private detail')))
    diagnostic.finish_candidate(queue,job,result=pending)
    state=diagnostic.candidate_status(queue.root,request['request_id'])
    assert state['state']=='observed_error'and state['raw_retention_state']=='unknown'
    assert state['raw_or_names_or_body_retained']is None and 'private detail'not in json.dumps(state)
    assert queue.db.execute('SELECT 1').fetchone()[0]==1 and not submit(queue,config)['submitted']


@pytest.mark.parametrize('change',['document','raw','question','request','permit_type'])
def test_legacy_original_permit_cannot_be_forged_at_writer_boundary(queue,config,change):
    http=client(queue,response());request,job=claim(queue,config,http);pending=diagnostic.fetch_candidate(http,job)
    permit=dict(pending.permit)
    if change=='document':permit['document_url']=URL.replace('12345','99999')
    elif change=='raw':permit['response_sha256']='f'*64
    elif change=='question':permit['question']='different'
    elif change=='request':permit['request_url']=URL
    else:permit=None
    diagnostic.finish_candidate(queue,job,result=replace(pending,permit=permit))
    state=diagnostic.candidate_status(queue.root,request['request_id'])
    assert state['state']=='observed_error'and state['raw_retention_state']=='unknown'
    assert not(queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()
