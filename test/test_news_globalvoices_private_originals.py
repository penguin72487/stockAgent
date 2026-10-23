"""Synthetic native CC failure originals, not publisher prose or media."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader.news_fulltext import load_config
from downloader.news_fulltext_candidate_originals import (
    GLOBALVOICES_ORIGINAL_LAYOUT as LAYOUT, GLOBALVOICES_ORIGINAL_QUESTION as QUESTION,
    PendingOriginal, globalvoices_original_source, private_globalvoices_original_permit, retain_original,
)
from downloader.news_fulltext_candidates import candidate_evidence_allowed, native_candidate_source
from downloader.news_fulltext_http import Response, RedirectHop
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_candidate_diagnostics import http_stub

URL='https://globalvoices.org/2005/11/06/synthetic-photo-post/'
RAW=('''<html><head><link rel="canonical" href="'''+URL+'''">
<meta property="article:published_time" content="2026-01-01T00:00:00+00:00"></head>
<body><div class="post-header"><h1 class="post-title">Synthetic photo post</h1>
<div id="post-header-credit"><a class="user-link" href="/author/synthetic-writer/">Synthetic Writer</a></div></div>
<div class="post-container"><div class="entry"><p><img src="https://media.test/photo.jpg"></p>
<p>Photo by <a href="https://photo.test/author/">Synthetic Photographer</a></p></div>
<div class="postfooter-credits"><span class="contributor-name"><a class="user-link" href="/author/synthetic-writer/">Synthetic Writer</a></span></div>
<div class="post-credit-container"><a rel="license" href="https://creativecommons.org/licenses/by/3.0/">CC</a></div>
</div></body></html>''').encode()


@pytest.fixture
def settings(tmp_path):
    config=deepcopy(load_config(Path('configs/news_fulltext.json'))[0])
    config['providers']=[p for p in config['providers'] if p['id']=='globalvoices']
    config.pop('provider_catalog',None)
    config.update(root=str(tmp_path),min_free_disk_bytes=1,workers=2,exclusive_http_owner=False,
        adaptive_rate={'enabled':False})
    return config


@pytest.fixture
def queue(settings,tmp_path):
    value=NewsQueue(tmp_path,PolicyRegistry(settings,base=tmp_path))
    value.add_articles([{'url':URL}])
    with value.db:
        value.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',"
            "attempts=1,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?",(URL,))
    yield value
    value.close()


def response(**changes):
    return replace(Response(URL,200,{'content-type':'text/html; charset=UTF-8'},RAW),**changes)


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())


def submit(queue,settings,**options):
    return jobs.submit_candidate(queue.root,settings,queue.registry,URL,LAYOUT,
        reason=options.pop('reason','Registered native missing-body original only; no body admission'),
        evidence_contract_version=options.pop('evidence',13),**options)


def pump(queue):
    with patch.object(jobs.time,'monotonic',return_value=0.0):
        jobs.pump_candidates(queue)


def claimed(queue,settings):
    client=http_stub(queue,response=response())
    requested=submit(queue,settings);pump(queue)
    return client,requested,jobs.claim_candidate(queue,client,settings,'globalvoices')


def test_current_native_source_has_no_new_capture_or_author_api_permission(settings,queue):
    source=queue.registry.sources['globalvoices']
    assert native_candidate_source(settings,queue.registry,queue.registry.base,LAYOUT) is source
    assert candidate_evidence_allowed(source,source,13)
    assert all(not candidate_evidence_allowed(source,source,v) for v in [*range(1,13),14,0,True,None])
    assert jobs._failure_states(LAYOUT,13)=={'body_not_found'}
    assert jobs._failure_states('fed_board_private_pdf_original_v1',13)==set()
    with pytest.raises(ValueError,match='reviewed_candidate_evidence_contract_required'):
        submit(queue,settings,evidence=1)


def test_same_owner_keeps_raw_bytes_private_without_body_replay_or_head_writes(settings,queue,monkeypatch):
    before=head(queue);settings_before=list(queue.db.execute('SELECT * FROM queue_settings'))
    monkeypatch.setattr('downloader.news_fulltext_candidates.extract_article',lambda *a,**k:pytest.fail('not a body replay'))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('no second HTTP owner'))
    client,request,job=claimed(queue,settings)
    assert jobs.candidate_status(queue.root,request['request_id'])['raw_retention_state']=='unknown'
    result=jobs.fetch_candidate(client,job)
    assert isinstance(result,PendingOriginal) and result.permit['question']==QUESTION
    assert not result.facts['current_parser_replayed'] and not result.facts['body_completeness_verified']
    assert result.facts['native_CC_authors_clock_scope_verified'] and not result.facts['third_party_media_requested']
    jobs.finish_candidate(queue,job,result=result);pump(queue)
    status=jobs.candidate_status(queue.root,request['request_id'])
    assert status['state']=='observed' and status['raw_retention_state']=='retained_verified'
    assert not status['accepted_as_article']
    ref=status['result_json']['private_original'];path=queue.root/ref['path']
    record,=pq.ParquetFile(path).read().to_pylist()
    assert record['raw_response_body']==RAW and record['request_url']==record['final_url']==URL
    assert record['response_sha256']==hashlib.sha256(RAW).hexdigest()
    assert json.loads(record['transport_redirects_json'])==[]
    for key in ('accepted_as_article','publish','historical_point_in_time','per_article_license_verified',
                'training_injection_enabled','remote_training_export'):
        assert record[key] is False
    assert record['diagnostic_only'] and not any(k in record for k in ('body','published_at_raw'))
    receipt=queue.root/ref['receipt_path']
    assert json.loads(receipt.read_bytes())['permit']==result.permit
    assert all(not p.stat().st_mode&0o077 for p in (path,receipt,path.parent))
    assert head(queue)==before and list(queue.db.execute('SELECT * FROM queue_settings'))==settings_before
    assert client.calls==[URL] and not (queue.root/'bodies').exists()
    assert not submit(queue,settings,reason='Different prose cannot rearm this question')['submitted']
    assert jobs.claim_candidate(queue,client,settings,'globalvoices') is None


@pytest.mark.parametrize('change',[
    {'provider':'publisher'},{'allowed':False},{'domains':('other.test',)},
    {'authorization':{'kind':'personal_research'}},{'title_selector':'h1'},
    {'required_license_url_prefix':''},{'required_license_url_prefix':'https://creativecommons.org/licenses/by/4.0/'},
])
def test_source_gate_is_registered_native_scope_not_a_general_html_capture(queue,change):
    source=replace(queue.registry.sources['globalvoices'],**change)
    assert not globalvoices_original_source(source)
    with pytest.raises(NewsError,match='private_globalvoices_original_scope_not_verified'):
        private_globalvoices_original_permit(response(),source,URL)


@pytest.mark.parametrize('literal',[
    URL.replace('https:','http:'),URL+'?page=1',URL+'#photo',URL.replace('globalvoices.org','evil.test'),
    URL.replace('globalvoices.org','zh.globalvoices.org'),URL.replace('globalvoices.org','user@globalvoices.org'),
    URL.replace('globalvoices.org','globalvoices.org:443'),'https://globalvoices.org/wp-json/wp/v2/posts',
    'https://globalvoices.org/author/synthetic-writer/',None,42,
])
def test_only_literal_dated_registered_article_url_not_profiles_or_wp_api(queue,literal):
    with pytest.raises(NewsError,match='private_globalvoices_original_scope_not_verified'):
        private_globalvoices_original_permit(response(),queue.registry.sources['globalvoices'],literal)


@pytest.mark.parametrize('changes',[
    {'status':304},{'status':403},{'status':429},{'status':404},
    {'headers':{'content-type':'application/pdf'}},{'headers':{'content-type':'application/json'}},
    {'url':URL+'other/'},{'body':b''},{'body':b'x'*((2<<20)+1)},
    {'redirects':(RedirectHop(URL,301,URL,URL,'2026-01-01T00:00:00Z','a'*64,0),)},
])
def test_actual_direct_html_transport_and_byte_budget_required(queue,changes):
    with pytest.raises(NewsError,match='private_globalvoices_original_scope_not_verified'):
        private_globalvoices_original_permit(response(**changes),queue.registry.sources['globalvoices'],URL)


@pytest.mark.parametrize('old,new',[
    (b'href="'+URL.encode()+b'"',b'href="https://globalvoices.org/2005/11/06/other/"'),
    (b'2026-01-01T00:00:00+00:00',b'2026-01-01T00:00:00'),
    (b'id="post-header-credit"',b'id="post-header-credit" hidden'),
    (b'class="entry"',b'class="entry" style="display:none"'),
    (b'https://creativecommons.org/licenses/by/3.0/',b'https://example.test/license'),
    (b'>Synthetic Writer</a></span>',b'>Someone Else</a></span>'),
    (b'</head>',b'<script type="application/ld+json">{"@type":"NewsArticle","isAccessibleForFree":false}</script></head>'),
    (b'</head>',b'<script type="application/ld+json">broken declaration</script></head>'),
])
def test_native_identity_article_license_matching_authors_clock_and_access_stay_strict(queue,old,new):
    assert old in RAW
    with pytest.raises(NewsError,match='private_globalvoices_original_scope_not_verified'):
        private_globalvoices_original_permit(response(body=RAW.replace(old,new)),queue.registry.sources['globalvoices'],URL)


def test_once_only_key_ignores_parser_upgrade_reason_attempts_or_clock(settings,queue):
    _,_,job=claimed(queue,settings)
    target=json.loads(queue.db.execute('SELECT request_json FROM native_candidate_jobs').fetchone()[0])['target']
    first=jobs._key(target,'a'*64,evidence_contract_version=13)
    changed={**target,'attempts':99,'last_checked_at_utc':'2026-01-02T00:00:00+00:00'}
    assert jobs._key(changed,'b'*64,evidence_contract_version=13)==first
    assert not submit(queue,settings)['submitted']
    assert job and jobs.claim_candidate(queue,http_stub(queue),settings,'globalvoices') is None


def test_worker_loss_does_not_rearm_unknown_reserved_original(settings,queue):
    _,request,_=claimed(queue,settings)
    with queue.db:
        queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(request['request_id'],))
    assert not submit(queue,settings,reason='Worker loss is not permission for another GET')['submitted']
    assert jobs.claim_candidate(queue,http_stub(queue),settings,'globalvoices') is None


def test_real_owner_recovery_preserves_unknown_reservation_without_another_get(settings,queue):
    _,request,_=claimed(queue,settings)
    queue.native_candidate_schema_ready=False;queue.owner='recovered-owner'
    pump(queue)
    status=jobs.candidate_status(queue.root,request['request_id'])
    assert status['state']=='uncertain_outcome' and status['raw_retention_state']=='unknown'
    assert status['raw_or_names_or_body_retained'] is None and not submit(queue,settings)['submitted']
    assert jobs.claim_candidate(queue,http_stub(queue),settings,'globalvoices') is None


def test_source_gate_pause_leaves_the_original_request_ready_without_http(settings,queue):
    request=submit(queue,settings);pump(queue)
    client=http_stub(queue,response=response())
    client.blocked_robots_origins=lambda p:('https://globalvoices.org',)
    assert jobs.claim_candidate(queue,client,settings,'globalvoices') is None
    assert jobs.candidate_status(queue.root,request['request_id'])['state']=='ready' and client.calls==[]


def test_private_write_failure_preserves_unknown_evidence_and_once_only_reservation(settings,queue,monkeypatch):
    client,request,job=claimed(queue,settings);result=jobs.fetch_candidate(client,job)
    monkeypatch.setattr(jobs,'retain_original',lambda *a:(_ for _ in ()).throw(OSError('synthetic write failure')))
    jobs.finish_candidate(queue,job,result=result)
    status=jobs.candidate_status(queue.root,request['request_id'])
    assert status['state']=='observed_error' and status['raw_retention_state']=='unknown'
    assert status['raw_or_names_or_body_retained'] is None and not submit(queue,settings)['submitted']


def test_owner_rechecks_scope_permit_before_any_private_write(settings,queue):
    client,_,job=claimed(queue,settings);original=jobs.fetch_candidate(client,job)
    forged=replace(original,permit={**original.permit,'response_sha256':'0'*64})
    with pytest.raises(ValueError,match='private_original_permit_mismatch'):
        retain_original(queue,job,forged)
    assert not (queue.root/'reviewed_repairs'/'candidate_diagnostics'/'originals').exists()


@pytest.mark.parametrize('change',[
    {'last_checked_at_utc':None},{'last_checked_at_utc':'2099-01-01T00:00:00+00:00'},
    {'state':'retry'},{'state':'source_body_image_only'},{'state':'request_denied'},
    {'due':999999999999.0},{'body_sha256':'a'*64},
])
def test_nonlegacy_null_clocks_leases_refusals_and_existing_bodies_are_not_relaxed(settings,queue,change):
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in change)+' WHERE url=?',(*change.values(),URL))
    with pytest.raises(ValueError):
        submit(queue,settings)
