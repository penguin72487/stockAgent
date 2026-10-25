"""Once-only replayable GVM originals; synthetic HTML, no article admission."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import time
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import (
    GVM_ORIGINAL_QUESTION, PendingOriginal, private_gvm_original_permit, retain_original,
)
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_gvm import GVM_WRITTEN_FORMAT_LAYOUT
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_gvm_known_urls import current_source
from test_news_gvm_media import media_page
from test_news_gvm_native import TEXT, URL, schema
from test_news_gvm_written_formats import written_entry


def original_source():
    return PolicyRegistry({'contract_version':1, 'collection_purpose':'personal_research',
        'publish':False, 'remote_training_export':False, 'providers':[written_entry()]}, base='.').sources['gvm']


def unknown_page():
    soup = media_page('')
    node = soup.new_tag('hr', id='PRIVATE_VALUE_SENTINEL')
    soup.select_one('.article-content').append(node)
    return soup


def response(soup=None, **changes):
    return replace(Response(URL, 200, {'content-type':'text/html; charset=utf-8',
        'set-cookie':'PRIVATE_COOKIE_SENTINEL'}, str(unknown_page() if soup is None else soup).encode()), **changes)


@pytest.fixture
def owner(config, tmp_path):
    settings = deepcopy(config)
    settings['providers'] = [written_entry()]
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    queue.add_articles([{'url':URL}])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified', "
            "error_code='article_identity_not_verified',attempts=4,due=?,last_checked_at_utc=? WHERE url=?",
            (time.time()-60, '2026-01-01T00:00:00+00:00', URL))
    yield queue, settings
    queue.close()


def head(queue): return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
def submit(queue, settings, reason='One missing original for offline editor diagnosis'):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, GVM_WRITTEN_FORMAT_LAYOUT,
        reason=reason, evidence_contract_version=40)


def http_stub(raw, **overrides):
    calls = []
    def get(url): calls.append(url); return raw
    return SimpleNamespace(get=get, calls=calls, provider_not_before={},
        blocked_gateway_hosts=lambda _: (), blocked_robots_origins=lambda _: (), **overrides)


@pytest.mark.parametrize('version', [1, 34, 35, 36, 37, 38, 39, 40, True])
def test_original_question_is_bound_to_the_exact_current_source(version):
    source = original_source()
    assert candidate_evidence_allowed(source, source, version) == (type(version) is int and version==40)
    assert not candidate_evidence_allowed(current_source(), current_source(), 40)
    assert not candidate_evidence_allowed(source, current_source(), 40)


@pytest.mark.parametrize('change', [{'allowed':False}, {'known_url_only':False}, {'domains':('example.org',)},
    {'require_author_attribution':False}, {'selectors':('body',)}, {'feeds':('https://www.gvm.com.tw/feed',)}])
def test_private_original_cannot_change_the_registered_scope(change):
    source = replace(original_source(), **change)
    assert not candidate_evidence_allowed(source, source, 40)


@pytest.mark.parametrize('complete', [False, True])
def test_header_verified_original_is_not_body_acceptance(complete):
    source = original_source()
    raw = response(media_page('') if complete else unknown_page())
    permit = private_gvm_original_permit(raw, source, URL)
    assert permit['question']==GVM_ORIGINAL_QUESTION
    assert permit['native_header_access_credit_clocks_verified'] and not permit['body_completeness_verified']
    assert permit['current_native_error'] == (None if complete else 'article_identity_not_verified')
    facts = candidate_response_metadata(raw, source, source, evidence_contract_version=40, requested_url=URL)
    assert facts['private_original_permit']==permit and not facts['accepted_as_article']
    encoded = json.dumps(facts, ensure_ascii=False)
    assert all(value not in encoded for value in (TEXT, '合成測試標題', '合成作者', 'PRIVATE_'))


@pytest.mark.parametrize('change', ['canonical','credit','clock','nonfree','hidden_header','reprint'])
def test_failed_public_header_access_credit_or_clock_never_retains_raw(change):
    soup = unknown_page()
    if change=='canonical': soup.select_one('link[rel=canonical]')['href'] += '9'
    elif change=='credit': schema(soup, lambda a: a['author'][0].update(name='不同署名'))
    elif change=='clock': soup.select_one('meta[property="article:published_time"]')['content']='bad'
    elif change=='nonfree': schema(soup, lambda a: a.update(isAccessibleForFree=False))
    elif change=='hidden_header': soup.select_one('.article-head')['hidden']=''
    else: soup.select_one('.article-content > p').string='本文轉載自未驗證來源'
    source = original_source()
    with pytest.raises(NewsError): private_gvm_original_permit(response(soup), source, URL)
    facts = candidate_response_metadata(response(soup), source, source, evidence_contract_version=40, requested_url=URL)
    assert 'private_original_permit' not in facts and not facts['raw_or_names_or_body_retained']
    assert not facts['gvm_received_failure_context']['prior_header_identity_access_credit_clocks_passed']
    assert 'PRIVATE_' not in json.dumps(facts, ensure_ascii=False)


@pytest.mark.parametrize('change', [{'status':403}, {'status':429}, {'body':b''}, {'body':b'x'*(2**21+1)},
    {'url':URL+'?other=1'}, {'headers':{'content-type':'application/json'}}])
def test_denied_or_changed_transport_is_not_an_original_permit(change):
    with pytest.raises(NewsError): private_gvm_original_permit(response(**change), original_source(), URL)


def test_same_owner_reserves_once_and_independently_writes_exact_original(owner, monkeypatch):
    queue, settings = owner
    before = head(queue)
    budgets = [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')]
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no second HTTP owner'))
    request = submit(queue, settings)
    assert submit(queue, settings, reason='Other words')['request_id']==request['request_id']
    diagnostic.pump_candidates(queue)
    http = http_stub(response())
    job = diagnostic.claim_candidate(queue, http, settings, 'gvm')
    reserved = queue.db.execute('SELECT state,owner_id FROM native_candidate_jobs WHERE request_id=?',
        (request['request_id'],)).fetchone()
    assert tuple(reserved)==('reserved', queue.owner) and not http.calls
    original = diagnostic.fetch_candidate(http, job)
    assert isinstance(original, PendingOriginal) and http.calls==[URL]
    diagnostic.finish_candidate(queue, job, result=original)
    result = diagnostic.candidate_status(queue.root, request['request_id'])
    assert result['state']=='observed' and result['raw_or_names_or_body_retained']
    reference = result['result_json']['private_original']
    path = queue.root/reference['path']
    row, = pq.ParquetFile(path).read(use_threads=False).to_pylist()
    receipt = json.loads((queue.root/reference['receipt_path']).read_bytes())
    assert hashlib.sha256(path.read_bytes()).hexdigest()==reference['sha256']
    assert row['raw_response_body']==original.response.body
    assert row['response_sha256']==hashlib.sha256(row['raw_response_body']).hexdigest()
    assert 'PRIVATE_COOKIE_SENTINEL' not in row['headers_json'] and path.stat().st_mode & 0o077==0
    assert receipt['question']==GVM_ORIGINAL_QUESTION and not row['accepted_as_article'] and not row['remote_training_export']
    assert head(queue)==before and [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')]==budgets
    assert not submit(queue, settings)['submitted'] and diagnostic.claim_candidate(queue, http, settings, 'gvm') is None


def test_original_key_does_not_rearm_after_a_parser_hash_or_counter_change(owner):
    queue, _ = owner
    target = diagnostic._head(head(queue))
    original = diagnostic._key(target, 'a'*64, evidence_contract_version=40)
    changed = {**target, 'attempts':100, 'last_checked_at_utc':'2026-10-01T00:00:00+00:00'}
    assert diagnostic._key(changed, 'b'*64, evidence_contract_version=40)==original


@pytest.mark.parametrize('state', ['complete','body_not_found','paywall_or_login','article_rights_or_scope_excluded','inflight'])
def test_completed_or_other_failures_cannot_enter_this_missing_original_lane(owner, state):
    queue, settings = owner
    with queue.db: queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (state,state,URL))
    with pytest.raises(ValueError): submit(queue, settings)


@pytest.mark.parametrize('blocked', ['deadline','gateway','robots','unknown'])
def test_refusal_deadlines_and_unknown_prior_never_dispatch_another_request(owner, blocked):
    queue, settings = owner
    request = submit(queue, settings); diagnostic.pump_candidates(queue)
    http = http_stub(response())
    if blocked=='deadline': http.provider_not_before['gvm']=time.time()+3600
    elif blocked=='gateway': http.blocked_gateway_hosts=lambda _: ('www.gvm.com.tw',)
    elif blocked=='robots': http.blocked_robots_origins=lambda _: ('https://www.gvm.com.tw',)
    else:
        with queue.db: queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (request['request_id'],))
    assert diagnostic.claim_candidate(queue, http, settings, 'gvm') is None and not http.calls
    assert not submit(queue, settings)['submitted']


def test_writer_rejects_changed_bytes_and_false_scope_before_writing(owner):
    queue, settings = owner
    submit(queue, settings); diagnostic.pump_candidates(queue)
    http = http_stub(response()); job = diagnostic.claim_candidate(queue, http, settings, 'gvm')
    original = diagnostic.fetch_candidate(http, job)
    changed = unknown_page(); changed.select_one('.article-content > p').string=TEXT+' changed'
    with pytest.raises(ValueError, match='private_original_permit_mismatch'):
        retain_original(queue, job, replace(original, response=response(changed)))
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))


def test_owner_handles_local_retention_failure_without_retrying_http_or_changing_head(owner, monkeypatch):
    queue, settings = owner
    before = head(queue); request = submit(queue, settings); diagnostic.pump_candidates(queue)
    http = http_stub(response()); job = diagnostic.claim_candidate(queue, http, settings, 'gvm')
    original = diagnostic.fetch_candidate(http, job)
    def failure(*a, **k): raise OSError('synthetic local write failure')
    monkeypatch.setattr(diagnostic, 'retain_original', failure)
    diagnostic.finish_candidate(queue, job, result=original)
    result = diagnostic.candidate_status(queue.root, request['request_id'])
    assert result['state']=='observed_error' and result['raw_retention_state']=='unknown'
    assert result['raw_or_names_or_body_retained'] is None and head(queue)==before
    assert not submit(queue, settings)['submitted'] and http.calls==[URL]


def test_original_cli_only_submits_to_the_resident_owner(config, tmp_path, monkeypatch):
    from scripts import probe_news_sources as cli
    settings = deepcopy(config); settings.update(root='news', providers=[written_entry()])
    registry = PolicyRegistry(settings, base=tmp_path)
    monkeypatch.setattr(cli, 'load_config', lambda _: (settings, registry, tmp_path))
    monkeypatch.setattr(cli, 'require_current_missing_body_failures', lambda *a, **k: {})
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('CLI cannot own HTTP'))
    calls = []
    def submitter(*args, **kwargs): calls.append((args, kwargs)); return {'submitted':True,'request_id':'a'*64}
    monkeypatch.setattr(diagnostic, 'submit_candidate', submitter)
    args = ['--probe',URL,'--candidate-evidence-contract','40','--candidate-embedded-layout',GVM_WRITTEN_FORMAT_LAYOUT,
        '--require-current-missing-body-failure','--failure-code','article_identity_not_verified','--output',str(tmp_path/'result.json')]
    with pytest.raises(SystemExit) as failure: cli.main(args)
    assert failure.value.code==2 and not calls
    assert cli.main([*args,'--enqueue-candidate'])==0
    assert len(calls)==1 and calls[0][1]['evidence_contract_version']==40
