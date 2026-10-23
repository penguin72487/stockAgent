"""One current identity question, not another downloader or body admission."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
import json

import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader import news_fulltext_candidates as candidates
from downloader import news_fulltext_policy as policy
from downloader.news_fulltext import load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_candidate_diagnostics import http_stub
from test_news_cmnews_scope import URL, page, TEXT, AUTHOR

LAYOUT='cmnews_visible_article_v1'


@pytest.fixture
def settings(tmp_path):
    config=deepcopy(load_config(Path('configs/news_fulltext.json'))[0])
    config['providers']=[p for p in config['providers'] if p['id']=='cmnews']
    config.pop('provider_catalog',None)
    config.update(root=str(tmp_path),min_free_disk_bytes=1,workers=2,exclusive_http_owner=False,
        adaptive_rate={'enabled':False})
    return config


@pytest.fixture
def queue(settings,tmp_path):
    queue=NewsQueue(tmp_path,PolicyRegistry(settings,base=tmp_path))
    queue.add_articles([{'url':URL}])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            "attempts=1,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?",(URL,))
    yield queue
    queue.close()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())


def submit(queue,settings,**options):
    return jobs.submit_candidate(queue.root,settings,queue.registry,URL,LAYOUT,
        reason=options.pop('reason','Only localize the unanswered native identity check'),**options)


def pump(queue):
    with patch.object(jobs.time,'monotonic',return_value=0.0):
        jobs.pump_candidates(queue)


def response(raw=None,**changes):
    return replace(Response(URL,200,{'content-type':'text/html; charset=UTF-8'},raw or page()),**changes)


def test_current_source_factory_does_not_create_a_new_capture_or_candidate(queue,settings):
    source=queue.registry.sources['cmnews']
    assert candidates.native_candidate_source(settings,queue.registry,queue.registry.base,LAYOUT) is source
    assert candidates.candidate_evidence_allowed(source,source,1)
    assert all(not candidates.candidate_evidence_allowed(source,source,v) for v in [0,True,2,3,4,5,6,7,8,9,10,11,12,13,14])
    assert jobs._failure_states(LAYOUT,1)=={'article_identity_not_verified'}
    assert jobs._failure_states(LAYOUT,2)==set()


@pytest.mark.parametrize('changes',[
    {'allowed':False}, {'authorization':{'kind':'unreviewed'}}, {'domains':('elsewhere.test',)},
    {'embedded_article_layout':'unknown'}, {'data_fingerprint':'f'*64}, {'authorization_fingerprint':'e'*64},
])
def test_source_substitution_or_candidate_upgrade_is_not_allowed(queue,changes):
    source=queue.registry.sources['cmnews']
    assert not candidates.candidate_evidence_allowed(source,replace(source,**changes),1)


def test_same_owner_observes_metadata_without_touching_heads_or_storing_raw(queue,settings,monkeypatch):
    before=head(queue);settings_before=list(queue.db.execute('SELECT * FROM queue_settings'))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('no other HTTP owner'))
    submitted=submit(queue,settings);pump(queue)
    client=http_stub(queue,response=response(page(canonical='https://cmnews.com.tw/article/another-identity')))
    job=jobs.claim_candidate(queue,client,settings,'cmnews');result=jobs.fetch_candidate(client,job)
    assert result['question']=='cmnews_current_identity_boundary_v1'
    assert result['current_native_error']=='article_identity_not_verified'
    assert result['native_identity']['matching_canonical_count']==0
    assert result['native_identity']['matching_native_scope_count']==1
    assert not result['body_completeness_verified'] and not result['raw_or_names_or_body_retained']
    jobs.finish_candidate(queue,job,result=result);pump(queue)
    observed=jobs.candidate_status(queue.root,submitted['request_id'])
    assert observed['state']=='observed' and not observed.get('raw_retention_state')
    assert observed['result_json']==result and client.calls==[URL]
    assert head(queue)==before and list(queue.db.execute('SELECT * FROM queue_settings'))==settings_before
    assert not (queue.root/'bodies').exists() and not (queue.root/'diagnostics').exists()
    assert not (queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()
    encoded=json.dumps(result,ensure_ascii=False)
    assert AUTHOR not in encoded and TEXT not in encoded and '本篇標題' not in encoded
    assert not submit(queue,settings,reason='Another reason cannot rearm the completed question')['submitted']
    assert jobs.claim_candidate(queue,client,settings,'cmnews') is None


def test_native_identity_diagnosis_parses_once_and_does_not_run_full_body_parser(queue,monkeypatch):
    source=queue.registry.sources['cmnews'];calls=[];original=policy.public_article_document
    def document(*args,**kwargs):
        calls.append(1);return original(*args,**kwargs)
    monkeypatch.setattr(policy,'public_article_document',document)
    monkeypatch.setattr(candidates,'extract_article',lambda *a,**k:pytest.fail('not a self candidate/full-body replay'))
    result=candidates.candidate_response_metadata(response(),source,source)
    assert result['current_native_error'] is None and not result['current_parser_replayed']
    assert result['native_identity']['native_title_count']==1 and len(calls)==1
    assert not result['accepted_as_article'] and not result['body_completeness_verified']


@pytest.mark.parametrize('raw,expected',[
    (page(canonical='https://cmnews.com.tw/article/another'),'article_identity_not_verified'),
    (page().replace(b'ArticleContainer-',b'UnknownContainer-'),'article_identity_not_verified'),
    (page().replace('本篇標題</h1>'.encode(),b'</h1>'),'article_identity_not_verified'),
    (b'<html><head><title>Verify you are human</title></head><body>private challenge</body></html>','access_challenge'),
    (page(paid=True),'paywall_or_login'),
])
def test_counts_never_bypass_actual_native_or_access_verdict(queue,raw,expected):
    source=queue.registry.sources['cmnews'];before=head(queue)
    result=candidates.candidate_response_metadata(response(raw),source,source)
    assert result['current_native_error']==expected and not result['accepted_as_article']
    assert head(queue)==before


@pytest.mark.parametrize('state',[
    'body_not_found','article_attribution_not_verified','source_body_image_only','paywall_or_login',
    'http_403','http_429','pending','retry','retry_cooldown','inflight','source_not_found','awaiting_authorization',
])
def test_only_reviewed_identity_failure_can_be_submitted(queue,settings,state):
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?',(state,state,URL))
    before=head(queue)
    with pytest.raises(ValueError):
        submit(queue,settings)
    assert head(queue)==before and not (queue.root/'reviewed_repairs/candidate_diagnostics/pending').exists()


def test_fixed_question_key_does_not_rearm_with_capture_attempt_clock_or_reason(queue):
    target=jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    altered=dict(target,attempts=9,due=100,last_checked_at_utc='2026-02-01T00:00:00+00:00')
    assert jobs._key(target,'a'*64)==jobs._key(altered,'b'*64)


def test_literal_redirect_is_not_followed_by_a_second_candidate_get(queue,settings):
    request=submit(queue,settings);pump(queue)
    client=http_stub(queue,response=response(url='https://cmnews.com.tw/article/other'))
    job=jobs.claim_candidate(queue,client,settings,'cmnews')
    with pytest.raises(NewsError,match='candidate_literal_response_not_verified'):
        jobs.fetch_candidate(client,job)
    assert client.calls==[URL] and not (queue.root/'bodies').exists()


def test_unknown_reservation_survives_restart_without_resending(queue,settings):
    submitted=submit(queue,settings);pump(queue);client=http_stub(queue,response=response())
    assert jobs.claim_candidate(queue,client,settings,'cmnews')
    root=queue.root;registry=queue.registry;queue.close()
    replacement=NewsQueue(root,registry)
    try:
        pump(replacement)
        assert not submit(replacement,settings)['submitted']
        assert jobs.candidate_status(root,submitted['request_id'])['state']=='uncertain_outcome'
        assert jobs.claim_candidate(replacement,client,settings,'cmnews') is None and not client.calls
    finally:
        replacement.close()
