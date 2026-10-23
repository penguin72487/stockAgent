"""Current ETtoday questions use the resident owner, not a stale v38 candidate."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace
import time

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidates import (
    candidate_evidence_allowed, candidate_response_metadata, native_candidate_source)
from downloader.news_fulltext_ettoday import (
    ETTODAY_BOUNDARY_QUESTION, ettoday_literal_diagnostic_url, ettoday_resident_source,
    ettoday_resident_boundary_metadata, ettoday_validation_metadata)
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
import test_news_ettoday as native

URL, LAYOUT = native.URLS[0], 'ettoday_visible_article_v39'


@pytest.fixture
def settings(tmp_path):
    entry = native.recipe()
    entry['embedded_article_layout'] = LAYOUT
    return {'contract_version':1, 'root':str(tmp_path), 'collection_purpose':'personal_research',
        'publish':False, 'remote_training_export':False, 'providers':[entry],
        'min_free_disk_bytes':1, 'workers':2, 'exclusive_http_owner':False,
        'adaptive_rate':{'enabled':False}}


@pytest.fixture
def queue(settings, tmp_path):
    owner = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    owner.add_articles([{'url':URL}])
    with owner.db:
        owner.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=? WHERE url=?',
            ('article_attribution_not_verified','article_attribution_not_verified',time.time()-60,
             '2026-01-01T00:00:00+00:00',URL))
    yield owner
    owner.close()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())


def submit(queue, settings, **kwargs):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, LAYOUT,
        reason=kwargs.pop('reason','Previously unasked current editor and credit relations'),
        evidence_contract_version=27, **kwargs)


def http_stub(queue, response=None):
    calls = []
    def get(url):
        calls.append(url)
        return response or Response(url,200,{'content-type':'text/html'},str(native.page()).encode())
    return SimpleNamespace(registry=queue.registry,get=get,calls=calls,provider_not_before={},
        blocked_gateway_hosts=lambda p:(),blocked_robots_origins=lambda p:())


def test_current_source_is_supported_without_rearming_old_v1(settings, tmp_path):
    registry = PolicyRegistry(settings,base=tmp_path)
    source = registry.sources['ettoday']
    assert ettoday_resident_source(source)
    assert native_candidate_source(settings,registry,tmp_path,LAYOUT) is source
    assert candidate_evidence_allowed(source,source,27)
    assert all(not candidate_evidence_allowed(source,source,v) for v in range(1,27))
    assert not candidate_evidence_allowed(source,source,True)
    assert diagnostic._failure_states(LAYOUT,27) == {
        'article_identity_not_verified','article_attribution_not_verified'}
    assert diagnostic._failure_states('cmnews_visible_article_v1',27) == set()
    old = deepcopy(settings)
    old['providers'][0]['embedded_article_layout'] = 'ettoday_visible_article_v38'
    predecessor = PolicyRegistry(old,base=tmp_path)
    candidate = native_candidate_source(old,predecessor,tmp_path,LAYOUT)
    assert candidate_evidence_allowed(predecessor.sources['ettoday'],candidate,1)
    assert not candidate_evidence_allowed(predecessor.sources['ettoday'],candidate,27)


@pytest.mark.parametrize('change',[
    {'allowed':False},{'acquisition_mode':'local_only'},{'known_url_only':False},
    {'domains':('www.ettoday.net',)},{'html_parser':'html.parser'}, {'selectors':('body',)},
    {'exclude_selectors':('aside',)}, {'title_selector':'h2'}, {'require_author_attribution':False},
    {'require_configured_container':False},{'authorization':{'kind':'licensed_api'}},
    {'reviewed_article_subsites':('unreviewed.ettoday.net',)},
    {'embedded_article_layout':'ettoday_visible_article_v38'},{'article_path_pattern':'.*'},
])
def test_current_diagnostic_cannot_expand_reviewed_scope(settings,tmp_path,change):
    source = replace(PolicyRegistry(settings,base=tmp_path).sources['ettoday'],**change)
    assert not ettoday_resident_source(source)
    assert not candidate_evidence_allowed(source,source,27)


@pytest.mark.parametrize('url',[
    URL+'?redirect=1',URL+'?',URL+'#',URL+'#x','http://finance.ettoday.net/news/3254747',
    'https://finance.ettoday.net:443/news/3254747','https://finance.ettoday.net/news/03254747',
    'https://esg.ettoday.net/news/3254747','https://user@finance.ettoday.net/news/3254747',None,
])
def test_only_exact_registered_literal_routes(url):
    assert not ettoday_literal_diagnostic_url(url,allow_house=True)
    assert all(ettoday_literal_diagnostic_url(u) for u in native.URLS)
    assert not ettoday_literal_diagnostic_url('https://house.ettoday.net/news/3254747')
    assert ettoday_literal_diagnostic_url('https://house.ettoday.net/news/3254747',allow_house=True)


def test_once_only_resident_question_does_not_reparse_accept_or_mutate_heads(queue,settings,monkeypatch):
    before, config_before = head(queue), deepcopy(settings)
    budgets = list(queue.db.execute('SELECT * FROM queue_settings'))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a,**k:pytest.fail('No second HTTP owner'))
    monkeypatch.setattr('downloader.news_fulltext_candidates.extract_article',
        lambda *a,**k:pytest.fail('No accepting candidate parse'))
    request = submit(queue,settings)
    assert request['submitted'] and request['publisher_requests'] == 0
    assert submit(queue,settings,reason='Another reason is not another question')['request_id'] == request['request_id']
    diagnostic.pump_candidates(queue)
    http = http_stub(queue)
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    assert job and diagnostic.candidate_status(queue.root,request['request_id'])['state'] == 'reserved'
    facts = diagnostic.fetch_candidate(http,job)
    assert http.calls == [URL] and facts['question'] == ETTODAY_BOUNDARY_QUESTION
    assert not facts['accepted_as_article'] and not facts['raw_or_names_or_body_retained']
    assert facts['current_source_not_candidate'] and not facts['current_parser_replayed']
    assert not facts['identity_or_alias_accepted'] and not facts['body_completeness_verified']
    text = json.dumps(facts,ensure_ascii=False)
    assert all(s not in text for s in (native.TITLE,native.PROSE,native.NOTICE,'研究記者','test.jpg'))
    assert len(text.encode()) < 60000
    assert facts['response_sha256'] == hashlib.sha256(str(native.page()).encode()).hexdigest()
    diagnostic.finish_candidate(queue,job,result=facts)
    diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root,request['request_id'])['state'] == 'observed'
    assert head(queue) == before and settings == config_before
    assert list(queue.db.execute('SELECT * FROM queue_settings')) == budgets
    assert not submit(queue,settings)['submitted']
    assert diagnostic.claim_candidate(queue,http,settings,'ettoday') is None
    assert not (queue.root/'batches').exists() and not (queue.root/'candidate_originals').exists()


def test_geometry_is_bounded_not_source_strings_or_attribution():
    page = native.page()
    old = ettoday_validation_metadata(page,page_url=URL,adapter_contract_version=39)
    story = page.select_one('.story')
    for _ in range(70):
        node = page.new_tag('arbitrary-source-name')
        node.string = '未確認正文-123.45'
        story.append(node)
    result = ettoday_resident_boundary_metadata(page,page_url=URL)
    assert {k:result[k] for k in old} == ettoday_validation_metadata(page,page_url=URL,adapter_contract_version=39)
    assert result['native_editor_shapes'][0]['scan_truncated']
    assert len(result['native_editor_shapes'][0]['blocks']) == 64
    assert result['native_editor_shapes'][0]['blocks'][-1]['tag'] == 'other'
    assert 'arbitrary-source-name' not in json.dumps(result) and '未確認正文' not in json.dumps(result,ensure_ascii=False)
    assert not result['attribution_verified_by_diagnostic'] and not result['whole_body_verified_by_diagnostic']
    assert result['canonical_equals_literal_request'] and result['og_url_equals_literal_request']
    assert result['schema_identity_relations'][0]['headline_equals_single_native_heading']


@pytest.mark.parametrize('change',['deadline','body','lease','state','source'])
def test_owner_rechecks_current_eligibility_after_submission(queue,settings,change):
    request = submit(queue,settings)
    diagnostic.pump_candidates(queue)
    with queue.db:
        if change == 'deadline': queue.db.execute('UPDATE articles SET due=?',(time.time()+3600,))
        elif change == 'body': queue.db.execute("UPDATE articles SET body_sha256='stored'")
        elif change == 'lease': queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=?",(time.time()+3600,))
        elif change == 'state': queue.db.execute("UPDATE articles SET state='pending',error_code=NULL")
    if change == 'source':
        changed = deepcopy(settings)
        changed['providers'][0]['authorization']['reviewed_on'] = '2026-10-23'
        queue.registry = PolicyRegistry(changed,base=queue.root)
    http = http_stub(queue)
    assert diagnostic.claim_candidate(queue,http,settings,'ettoday') is None and not http.calls
    diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root,request['request_id'])['state'] == 'skipped'


def test_deadline_and_robots_controls_defer_without_reserving_or_http(queue,settings):
    request = submit(queue,settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue)
    http.provider_not_before['ettoday'] = time.time()+3600
    assert diagnostic.claim_candidate(queue,http,settings,'ettoday') is None
    http.provider_not_before.clear()
    http.blocked_robots_origins = lambda p:('https://finance.ettoday.net',)
    assert diagnostic.claim_candidate(queue,http,settings,'ettoday') is None and not http.calls
    assert diagnostic.candidate_status(queue.root,request['request_id'])['state'] == 'ready'


@pytest.mark.parametrize('mutation',['other_provider','other_layout','query','bool_version'])
def test_serialized_requests_cannot_forge_evidence_scope(queue,settings,mutation):
    request = submit(queue,settings)
    path = diagnostic._inbox(queue.root)/(request['request_id']+'.json')
    value = json.loads(path.read_text())
    if mutation == 'other_provider': value['target']['provider'] = 'cmnews'
    elif mutation == 'other_layout': value['candidate_layout'] = 'cmnews_visible_article_v1'
    elif mutation == 'query':
        value['target']['url'] += '?redirect=1'
        value['target']['id'] = hashlib.sha256(value['target']['url'].encode()).hexdigest()
        value['observation_key'] = diagnostic._key(value['target'],value['candidate_fingerprint'],evidence_contract_version=27)
    else: value['contract_version'] = value['evidence_contract_version'] = True
    from downloader.artifact_io import atomic_write_bytes
    payload = diagnostic._bytes(value)
    sealed = path.with_name(hashlib.sha256(payload).hexdigest()+'.json')
    atomic_write_bytes(sealed,payload,mode=0o600)
    with pytest.raises(ValueError,match='invalid_native_candidate|literal_current_ettoday_route'):
        diagnostic._load(sealed)


def test_question_key_ignores_code_fingerprint_and_failure_counters(queue):
    target = diagnostic._head(head(queue))
    first = diagnostic._key(target,'a'*64,evidence_contract_version=27)
    target.update(attempts=99,last_checked_at_utc='2026-01-02T00:00:00+00:00')
    assert diagnostic._key(target,'b'*64,evidence_contract_version=27) == first


def test_reserved_crash_is_not_automatically_retried(settings,tmp_path):
    owner = NewsQueue(tmp_path,PolicyRegistry(settings,base=tmp_path))
    try:
        owner.add_articles([{'url':URL}])
        with owner.db:
            owner.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=?',
                ('article_identity_not_verified','article_identity_not_verified',time.time()-60,
                 '2026-01-01T00:00:00+00:00'))
        request = submit(owner,settings)
        diagnostic.pump_candidates(owner)
        assert diagnostic.claim_candidate(owner,http_stub(owner),settings,'ettoday')
    finally:
        owner.close()
    recovered = NewsQueue(tmp_path,PolicyRegistry(settings,base=tmp_path))
    try:
        diagnostic.pump_candidates(recovered)
        http = http_stub(recovered)
        assert diagnostic.claim_candidate(recovered,http,settings,'ettoday') is None
        assert diagnostic.candidate_status(tmp_path,request['request_id'])['state'] == 'uncertain_outcome'
        assert not submit(recovered,settings)['submitted'] and not http.calls
    finally:
        recovered.close()


def test_cli_submission_and_status_are_mailbox_only(queue,settings,monkeypatch,tmp_path):
    from scripts import probe_news_sources as cli
    monkeypatch.setattr(cli,'load_config',lambda p:(settings,queue.registry,tmp_path))
    monkeypatch.setattr(cli,'NewsHTTP',lambda *a,**k:pytest.fail('CLI must not create another owner'))
    output = tmp_path/'private/submitted.json'
    args = ['--output',str(output),'--probe',URL,'--require-current-missing-body-failure',
        '--failure-code','article_attribution_not_verified','--candidate-embedded-layout',LAYOUT,
        '--enqueue-candidate','--candidate-evidence-contract','27']
    before = head(queue)
    assert cli.main(args) == 0
    result = json.loads(output.read_bytes())
    assert result['submitted'] and result['publisher_requests'] == 0 and head(queue) == before
    assert cli.main(['--output',str(output),'--candidate-request-status',result['request_id']]) == 0
    assert json.loads(output.read_bytes())['state'] == 'pending' and head(queue) == before


@pytest.mark.parametrize('layout,enqueue',[(LAYOUT,False),('videoland_visible_article_v4',True)])
def test_cli_rejects_standalone_or_other_layout_for_v27(queue,settings,monkeypatch,tmp_path,layout,enqueue):
    from scripts import probe_news_sources as cli
    monkeypatch.setattr(cli,'load_config',lambda p:(settings,queue.registry,tmp_path))
    monkeypatch.setattr(cli,'NewsHTTP',lambda *a,**k:pytest.fail('Invalid scope must not issue HTTP'))
    args = ['--output',str(tmp_path/'private/error.json'),'--probe',URL,
        '--candidate-embedded-layout',layout,'--candidate-evidence-contract','27']
    if enqueue: args.append('--enqueue-candidate')
    with pytest.raises(SystemExit): cli.main(args)


@pytest.mark.parametrize('change',['redirect','wrong_url','empty','too_large','paid'])
def test_response_must_be_exact_public_envelope(queue,change):
    source = queue.registry.sources['ettoday']
    body = str(native.page()).encode()
    url, redirects = URL, ()
    if change == 'redirect': redirects = ('unreviewed',)
    elif change == 'wrong_url': url = native.URLS[1]
    elif change == 'empty': body = b''
    elif change == 'too_large': body = b'x' * ((2<<20)+1)
    elif change == 'paid': body = body.replace(b'"@type": "NewsArticle"',b'"isAccessibleForFree": false, "@type": "NewsArticle"')
    response = Response(url,200,{'content-type':'text/html'},body,redirects=redirects)
    with pytest.raises(NewsError):
        candidate_response_metadata(response,source,source,evidence_contract_version=27,requested_url=URL)
