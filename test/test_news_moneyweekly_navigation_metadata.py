"""Continue witnessed FN declarations without decoding, aliasing or re-fetching bodies."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader import news_fulltext_moneyweekly_outcomes as paid
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_moneyweekly import (
    ROUTE_METADATA_LAYOUT, moneyweekly_literal_navigation_route,
    moneyweekly_route_metadata, require_prior_moneyweekly_navigation,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_moneyweekly_body_geometry import FN, claim, heads, pump, submit as geometry, transport
from test_news_moneyweekly_native_routes import native_config

DECLARATION='https://www.moneyweekly.com.tw/ArticleData/Info/Article/90351?_view=-1'


@pytest.fixture(params=[0,1])
def retained(tmp_path,monkeypatch,request):
    settings=native_config()
    settings.update(root=str(tmp_path),exclusive_http_owner=False,min_free_disk_bytes=1)
    with monkeypatch.context() as generation:
        generation.setattr(paid,'CONTRACT_VERSION',request.param)
        registry=PolicyRegistry(settings,base=Path.cwd())
    previous=NewsQueue(tmp_path,registry)
    try:
        previous.add_articles([{'url':FN}])
        with previous.db:
            previous.db.execute("UPDATE articles SET state='unsupported_article_path',error_code='unsupported_article_path',"
                "attempts=1,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00'")
        submitted=geometry(previous,settings,FN)
        client=transport(previous,response=Response(FN,302,{'location':DECLARATION},b'UNRETAINED RESPONSE'))
        job=claim(previous,settings,client)
        result=jobs.fetch_candidate(client,job)
        assert result['location'] is None and result['status']==302
        jobs.finish_candidate(previous,job,result=result)
        closed=dict(previous.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
            (submitted['request_id'],)).fetchone())
        before=heads(previous)
    finally:
        previous.close()
    current=NewsQueue(tmp_path,PolicyRegistry(settings,base=Path.cwd()))
    try:
        assert heads(current)==before
        yield current,settings,closed,before
    finally:
        current.close()


def submit(queue,settings,reason='Observe the still-withheld literal first-hop declaration'):
    return jobs.submit_candidate(queue.root,settings,queue.registry,FN,ROUTE_METADATA_LAYOUT,
        reason=reason,evidence_contract_version=64)


def test_prior_current_and_capture_only_predecessor_continue_one_distinct_question(retained):
    queue,settings,closed,before=retained
    source=queue.registry.sources['moneyweekly']
    assert require_prior_moneyweekly_navigation(queue.db,source,FN)==source
    request=submit(queue,settings)
    assert request['submitted'] and request['request_id']!=closed['request_id']
    client=transport(queue,response=Response(FN,302,{'location':DECLARATION},b'UNRETAINED RESPONSE'))
    job=claim(queue,settings,client)
    result=jobs.fetch_candidate(client,job)
    assert client.calls==[(FN,False)]
    assert result['location']==DECLARATION
    assert result['location_reference_private'] is True and result['opaque_query_semantics_accepted'] is False
    assert result['identity_or_alias_accepted'] is result['body_completeness_verified'] is result['destination_requested'] is False
    assert 'UNRETAINED' not in json.dumps(result) and 'private_original' not in result
    jobs.finish_candidate(queue,job,result=result)
    assert jobs.candidate_status(queue.root,request['request_id'])['state']=='observed'
    assert not submit(queue,settings,reason='A different reason must not rearm a closed question')['submitted']
    assert not geometry(queue,settings,FN)['submitted']
    assert heads(queue)==before and not list(queue.root.rglob('*.parquet'))
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (closed['request_id'],)).fetchone())==closed


@pytest.mark.parametrize('change',['error','known_location','wrong_status','source','target','layout'])
def test_missing_or_changed_closed_geometry_is_not_a_new_request_allowance(retained,change):
    queue,settings,closed,before=retained
    request,result=json.loads(closed['request_json']),json.loads(closed['result_json'])
    with queue.db:
        if change=='error':
            queue.db.execute("UPDATE native_candidate_jobs SET state='observed_error' WHERE request_id=?",(closed['request_id'],))
        else:
            if change=='known_location':result['location']=DECLARATION.split('?')[0]
            elif change=='wrong_status':result['status']=301
            elif change=='source':request['source']['data_fingerprint']='a'*64
            elif change=='target':request['target']['attempts']+=1
            else:request['candidate_layout']='other'
            queue.db.execute('UPDATE native_candidate_jobs SET request_json=?,result_json=? WHERE request_id=?',
                (json.dumps(request),json.dumps(result),closed['request_id']))
    with pytest.raises(ValueError,match='closed_withheld_moneyweekly_navigation_required'):
        submit(queue,settings)
    assert heads(queue)==before


@pytest.mark.parametrize('change',['authorization','http','body'])
def test_other_source_changes_do_not_inherit_historical_geometry(retained,change):
    queue,settings,_,_=retained
    edited=deepcopy(settings)
    source=next(row for row in edited['providers'] if row['id']=='moneyweekly')
    if change=='authorization':source['permitted_author_names'].append('unreviewed author')
    elif change=='http':source['interval_seconds']+=1
    else:source['selectors']=['main']
    registry=PolicyRegistry(edited,base=Path.cwd())
    with pytest.raises(ValueError):
        jobs.submit_candidate(queue.root,edited,registry,FN,ROUTE_METADATA_LAYOUT,reason='changed source',evidence_contract_version=64)


@pytest.mark.parametrize('change',['lease','body','deadline','state'])
def test_current_head_protections_still_precede_navigation_dispatch(retained,change):
    queue,settings,_,_=retained
    with queue.db:
        if change=='lease':queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=9999999999")
        elif change=='body':queue.db.execute("UPDATE articles SET body_sha256=?,body_bytes=1,parquet_path='kept.parquet'",('b'*64,))
        elif change=='deadline':queue.db.execute('UPDATE articles SET due=9999999999')
        else:queue.db.execute("UPDATE articles SET state='complete',error_code=NULL")
    with pytest.raises(ValueError):submit(queue,settings)


def test_new_head_after_admission_prevents_a_source_request(retained):
    queue,settings,_,_=retained
    submitted=submit(queue,settings)
    pump(queue)
    with queue.db:queue.db.execute('UPDATE articles SET attempts=attempts+1')
    client=transport(queue)
    assert jobs.claim_candidate(queue,client,settings,'moneyweekly') is None
    assert client.calls==[]
    assert jobs.candidate_status(queue.root,submitted['request_id'])['state']=='skipped'


@pytest.mark.parametrize('declaration',[
    'https://other.test/ArticleData/Info/Article/90351?opaque=-1',
    'http://www.moneyweekly.com.tw/ArticleData/Info/Article/90351?opaque=-1',
    'https://user:pass@www.moneyweekly.com.tw/ArticleData/Info/Article/90351?opaque=-1',
    'https://www.moneyweekly.com.tw/ArticleData/Info/Article/90351#fragment',
    'https://www.moneyweekly.com.tw/Login?return=ArticleData',
])
def test_navigation_does_not_admit_other_origins_credentials_or_unreviewed_paths(retained,declaration):
    queue,_,_,_=retained
    source=queue.registry.sources['moneyweekly']
    result=moneyweekly_route_metadata(Response(FN,302,{'location':declaration},b''),source,FN)
    assert result['location'] is None and result['destination_requested'] is False


def test_unexpected_200_keeps_only_first_hop_facts_without_article_prose(retained):
    queue,_,_,_=retained
    result=moneyweekly_route_metadata(Response(FN,200,{},b'<p>UNRETAINED PROSE</p>'),queue.registry.sources['moneyweekly'],FN)
    assert result['status']==200 and result['location'] is None
    assert 'UNRETAINED' not in json.dumps(result) and 'configured_body_count' not in result


def test_new_literal_404_is_recorded_as_http_404_without_reopening_old_questions(retained):
    queue,settings,_,before=retained
    submitted=submit(queue,settings)
    client=transport(queue,response=Response(FN,404,{},b'UNRETAINED'))
    job=claim(queue,settings,client)
    with pytest.raises(NewsError,match='^http_404$') as failure:
        jobs.fetch_candidate(client,job)
    jobs.finish_candidate(queue,job,error=failure.value)
    saved=jobs.candidate_status(queue.root,submitted['request_id'])
    assert saved['state']=='observed_error' and saved['result_json']['error']=='http_404'
    assert heads(queue)==before and not submit(queue,settings)['submitted']


@pytest.mark.parametrize('field,value',[
    ('opaque_query_semantics_accepted',True),('identity_or_alias_accepted',True),
    ('location_reference_private',False),('response_bytes',True),('response_sha256','text'),
    ('evidence_contract_version',True),('redirect_count',True),('status',True),
    ('location','https://other.test/ArticleData/Info/Article/90351'),('extra','UNRETAINED PROSE'),
    ('timing',{'http_seconds':float('nan'),'parse_seconds':0,'scope_seconds':0,'total_seconds':0}),
])
def test_navigation_result_is_validated_again_before_durable_completion(retained,field,value):
    queue,settings,_,before=retained
    submitted=submit(queue,settings)
    client=transport(queue,response=Response(FN,302,{'location':DECLARATION},b''))
    job=claim(queue,settings,client)
    result=jobs.fetch_candidate(client,job)
    result[field]=value
    jobs.finish_candidate(queue,job,result=result)
    saved=jobs.candidate_status(queue.root,submitted['request_id'])
    assert saved['state']=='observed_error' and saved['result_json']['error']=='invalid_candidate_result'
    assert heads(queue)==before


@pytest.mark.parametrize('url',[FN.replace('Type=0','Type=1'),FN.replace('Page=','page='),FN+'00#fragment',
    FN.replace('www.moneyweekly.com.tw','other.test'),FN.replace('/FN/','/Article/'),FN.replace('Page=','Token=')])
def test_navigation_scope_preserves_the_original_query_without_decoding_it(url):
    assert not moneyweekly_literal_navigation_route(url)
