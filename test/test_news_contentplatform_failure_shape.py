"""Diagnose only missing heads through the resident owner, without body admission."""
from copy import deepcopy
from dataclasses import replace
import json
import time
from types import SimpleNamespace

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_WRITTEN_HTML_LAYOUT
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_contentplatform import URL, RAW, page, source
from test_news_contentplatform_written_geometry import written_html_entry
from test_news_fulltext import config


def metadata(raw):
    policy = source(written_html_entry())
    return candidate_response_metadata(Response(URL,200,{'content-type':'text/html'},raw),
        policy,policy,evidence_contract_version=55,requested_url=URL)


def test_shape_keeps_failure_location_and_structural_facts_without_source_text():
    facts=metadata(page(extra='<div><iframe src="/SECRET_URL"></iframe><custom-secret>SECRET_PROSE</custom-secret><span hidden></span></div>'))
    shape=facts['native_failure_shape']
    assert shape['native_frame_verified'] and shape['current_native_error']=='article_identity_not_verified'
    assert shape['tag_counts']['iframe']==shape['tag_counts']['#other']==1
    assert any(not n['visible'] and n['text_characters']==0 for n in shape['exceptional_nodes'])
    assert not facts['raw_or_names_or_body_retained'] and not facts['accepted_as_article']
    assert not shape['body_geometry_accepted'] and not shape['source_text_exported']
    assert RAW not in json.dumps(facts,ensure_ascii=False)
    assert 'SECRET' not in json.dumps(facts) and '許' not in json.dumps(facts,ensure_ascii=False)


def test_shape_reports_frame_refusal_without_traversing_unverified_body():
    facts=metadata(page(canonical=URL.replace('533649','429527')))
    shape=facts['native_failure_shape']
    assert not shape['native_frame_verified'] and shape['current_native_error']=='article_identity_not_verified'
    assert shape['frame_node_counts']['canonical']==1 and 'tag_counts' not in shape
    assert shape['literal_native_link_declarations']['canonical']==[URL.replace('533649','429527')]
    assert shape['declared_links_are_not_accepted_aliases']


def test_paywall_precedes_all_geometry_observation():
    facts=metadata(page(extra='<div class="paywall">SECRET_MEMBER</div>'))
    assert facts['current_native_error']=='paywall_or_login' and 'native_failure_shape' not in facts
    assert 'SECRET' not in json.dumps(facts)


def test_shape_is_bounded_and_never_exports_unknown_tag_names():
    facts=metadata(page(extra='<unknown-secret>SECRET_TEXT</unknown-secret>'*5000))
    shape=facts['native_failure_shape']
    assert shape['walk_limit_reached'] and shape['walked_nodes']==4096
    assert len(shape['exceptional_nodes'])==24
    assert 'SECRET' not in json.dumps(shape) and 'unknown-secret' not in json.dumps(shape)


@pytest.mark.parametrize('change', [dict(allowed=False),dict(known_url_only=False),dict(domains=('example.org',)),
    dict(embedded_article_layout='contentplatform_native_article_v6'),dict(selectors=('body',))])
def test_question_cannot_expand_current_source_scope(change):
    policy=source(written_html_entry())
    assert candidate_evidence_allowed(policy,policy,55)
    changed=replace(policy,**change)
    assert not candidate_evidence_allowed(changed,changed,55)
    assert not candidate_evidence_allowed(policy,replace(policy,data_fingerprint='a'*64),55)


@pytest.fixture
def owner(config,tmp_path):
    settings=deepcopy(config)
    settings.update(root=str(tmp_path),providers=[written_html_entry()])
    queue=NewsQueue(tmp_path,PolicyRegistry(settings,base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url':URL}])
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
                "attempts=6,due=0,last_checked_at_utc='2026-10-26T10:57:44+00:00' WHERE url=?",(URL,))
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',('failure_diagnostic_attempts:v1:contentplatform:20261026','2'))
        yield queue,settings
    finally:
        queue.close()


def submit(queue,settings,reason='Inspect only the remaining current native failure structure'):
    return diagnostic.submit_candidate(queue.root,settings,queue.registry,URL,CONTENTPLATFORM_WRITTEN_HTML_LAYOUT,
        reason=reason,evidence_contract_version=55)


def http_stub():
    calls=[]
    def get(url,*,follow_redirects):
        calls.append((url,follow_redirects))
        return Response(URL,200,{'content-type':'text/html'},page(extra='<div><iframe src="/NEVER_FOLLOWED"></iframe></div>'))
    return SimpleNamespace(get=get,calls=calls,provider_not_before={},
        blocked_gateway_hosts=lambda _:(),blocked_robots_origins=lambda _:())


def test_resident_once_question_preserves_heads_failures_budgets_and_body_admission(owner,monkeypatch):
    queue,settings=owner
    before=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    charges=list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('no extra HTTP owner'))
    requested=submit(queue,settings)
    diagnostic.pump_candidates(queue)
    http=http_stub()
    job=diagnostic.claim_candidate(queue,http,settings,'contentplatform')
    assert job is not None and not http.calls
    facts=diagnostic.fetch_candidate(http,job)
    assert isinstance(facts,dict) and not facts['raw_or_names_or_body_retained']
    diagnostic.finish_candidate(queue,job,result=facts)
    result=diagnostic.candidate_status(queue.root,requested['request_id'])
    assert result['state']=='observed' and http.calls==[(URL,False)]
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==before
    assert list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))==charges
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    duplicate=submit(queue,settings,reason='Different words cannot buy another request')
    assert duplicate['request_id']==requested['request_id'] and not duplicate['submitted']
    target=json.loads(queue.db.execute('SELECT request_json FROM native_candidate_jobs').fetchone()[0])['target']
    assert diagnostic._key(target,'a'*64,evidence_contract_version=55)==diagnostic._key(target,'b'*64,evidence_contract_version=55)


@pytest.mark.parametrize('blocked',['deadline','gateway','robots','uncertain'])
def test_source_cooldown_refusal_and_unknown_outcome_are_preserved(owner,blocked):
    queue,settings=owner
    request=submit(queue,settings)
    diagnostic.pump_candidates(queue)
    http=http_stub()
    if blocked=='deadline':
        http.provider_not_before['contentplatform']=time.time()+600
    elif blocked=='gateway':
        http.blocked_gateway_hosts=lambda _:('www.contentplatform.info',)
    elif blocked=='robots':
        http.blocked_robots_origins=lambda _:('https://www.contentplatform.info',)
    else:
        with queue.db:
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(request['request_id'],))
        diagnostic.pump_candidates(queue)
    assert diagnostic.claim_candidate(queue,http,settings,'contentplatform') is None and not http.calls


def test_redirect_and_oversize_response_do_not_become_metadata_permission():
    policy=source(written_html_entry())
    with pytest.raises(NewsError):
        candidate_response_metadata(Response(URL.replace('533649','429527'),200,{'content-type':'text/html'},page()),
            policy,policy,evidence_contract_version=55,requested_url=URL)
    with pytest.raises(NewsError):
        metadata(b' '*(8<<20)+b'x')
